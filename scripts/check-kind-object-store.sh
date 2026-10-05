#!/usr/bin/env bash
# Offline guard for the kind warehouse object-store selector (#6332).
#
# kind installs one of two stores and then installs the chart against it. The
# failure this guards is the two halves disagreeing: a round that brings up
# Garage and installs the chart with MinIO's endpoint and credentials comes up,
# ingests, and produces a full evidence directory for the store nobody asked
# for. Both deployment stages therefore read ONE resolved answer from
# scripts/kind-common.bash, and an unknown selector stops before a cluster
# exists rather than falling back to the MinIO defaults.
#
# What runs here: the pure resolver, the real scripts/kind-up.sh driven end to
# end against recording stand-ins for kind, kubectl, helm and docker, the two
# refusal paths of scripts/kind-round.sh, and the agreement between the Garage
# manifest, its values overlay, its server configuration and the compose
# service it is kept in step with. No cluster, no container, no network.

set -euo pipefail

cd "$(dirname "$0")/.."
ROOT=$PWD

UP=scripts/kind-up.sh
ROUND=scripts/kind-round.sh
COMMON=scripts/kind-common.bash
BASE_VALUES=deploy/kind/values.kind.yaml
GARAGE_VALUES=deploy/kind/values.kind.garage.yaml
MINIO_MANIFEST=deploy/kind/manifests/minio.yaml
GARAGE_MANIFEST=deploy/kind/manifests/garage.yaml
GARAGE_TOML=deploy/garage/garage.toml
COMPOSE=deploy/docker-compose.yml
FIRST_ROUND_LINE='log "bring up the base kind deployment"'

fail() { echo "FAIL $*" >&2; exit 1; }

# Fixed-string containment without a pipe: `printf | grep -q` can lose the
# write race and die of SIGPIPE, which pipefail reports as a missing line.
contains() { case "$1" in *"$2"*) ;; *) return 1 ;; esac; }

for file in "$UP" "$ROUND" "$COMMON" "$BASE_VALUES" "$GARAGE_VALUES" \
  "$MINIO_MANIFEST" "$GARAGE_MANIFEST" "$GARAGE_TOML" "$COMPOSE"; do
  [[ -f "$file" ]] || fail "$file does not exist"
done
bash -n "$UP"
bash -n "$ROUND"
bash -n "$COMMON"

# --- the pure resolver -------------------------------------------------------
#
# Driven in this shell: the functions are pure, so they need no sandbox.
# shellcheck source=scripts/kind-common.bash
source "$COMMON"

for accepted in minio garage; do
  answer=$(siglake_kind_object_store "$accepted") ||
    fail "the resolver rejected the supported store $accepted"
  [[ "$answer" == "$accepted" ]] ||
    fail "the resolver turned $accepted into $answer"
done
# Unset and empty both mean the shipping default, as they do for compose.
for defaulted in "" " "; do
  [[ "$defaulted" == " " ]] && continue
  answer=$(siglake_kind_object_store "$defaulted") ||
    fail "the resolver rejected an empty selector instead of defaulting it"
  [[ "$answer" == minio ]] ||
    fail "an empty selector resolved to $answer, not minio"
done
[[ "$(siglake_kind_object_store)" == minio ]] ||
  fail "an absent selector resolved to something other than minio"

# A near miss must be refused, not rounded to the default: the whole point of
# the selector is that the arm an evidence directory claims is the arm that ran.
for refused in minioo MinIO GARAGE s3 "minio garage" "garage " "" ; do
  [[ -n "$refused" ]] || continue
  if answer=$(siglake_kind_object_store "$refused" 2>/dev/null); then
    fail "the resolver accepted the unknown store '$refused' as $answer"
  fi
  if siglake_kind_store_settings "$refused" >/dev/null 2>&1; then
    fail "the settings resolver accepted the unknown store '$refused'"
  fi
  if siglake_kind_store_values "$refused" >/dev/null 2>&1; then
    fail "the values resolver accepted the unknown store '$refused'"
  fi
done

expected_minio_values=$BASE_VALUES
expected_garage_values="$BASE_VALUES"$'\n'"$GARAGE_VALUES"
[[ "$(siglake_kind_store_values minio)" == "$expected_minio_values" ]] ||
  fail "the minio arm does not install exactly $BASE_VALUES"
[[ "$(siglake_kind_store_values garage)" == "$expected_garage_values" ]] ||
  fail "the garage arm does not layer $GARAGE_VALUES over $BASE_VALUES, in that order"

while IFS= read -r store; do
  IFS=$'\t' read -r manifest app init_job config overlay \
    < <(siglake_kind_store_settings "$store")
  [[ -f "$manifest" ]] || fail "$store names a manifest that does not exist: $manifest"
  [[ -n "$app" ]] || fail "$store names no pod label to wait for"
  [[ -n "$init_job" ]] || fail "$store names no bucket Job to wait for"
  [[ -z "$config" || -f "$config" ]] ||
    fail "$store names a server configuration that does not exist: $config"
  [[ -z "$overlay" || -f "$overlay" ]] ||
    fail "$store names a values overlay that does not exist: $overlay"
  grep -q "name: $init_job" "$manifest" ||
    fail "$manifest declares no Job named $init_job"
  grep -q "app: $app" "$manifest" ||
    fail "$manifest carries no app: $app label"
done <<'STORES'
minio
garage
STORES

echo "ok (kind object-store resolver: minio default, garage opt-in, typos refused)"

# --- the Garage files agree with each other ----------------------------------

python3 - "$GARAGE_MANIFEST" "$GARAGE_VALUES" "$GARAGE_TOML" "$COMPOSE" <<'PY' ||
import re
import sys

manifest_path, values_path, toml_path, compose_path = sys.argv[1:]
manifest = open(manifest_path, encoding="utf-8").read()
values = open(values_path, encoding="utf-8").read()
toml = open(toml_path, encoding="utf-8").read()
compose = open(compose_path, encoding="utf-8").read()
problems = []


def manifest_env(name):
    """The value of a container env var, from the `- name:`/`value:` pair."""
    found = re.findall(
        rf"^\s*- name: {re.escape(name)}\n\s*value: (?P<value>.+)$",
        manifest,
        re.MULTILINE,
    )
    if len(found) != 1:
        problems.append(
            f"{manifest_path}: expected one {name} env declaration, found {len(found)}"
        )
        return None
    return found[0].strip().strip('"')


def values_scalar(section, key):
    """A second-level key of the overlay's two-level YAML."""
    block = re.search(rf"^{section}:\n(?P<body>(?:  .*\n|\n)*)", values, re.MULTILINE)
    if not block:
        problems.append(f"{values_path}: no `{section}:` block")
        return None
    found = re.findall(rf"^  {re.escape(key)}: (?P<value>.+)$", block.group("body"), re.MULTILINE)
    if len(found) != 1:
        problems.append(
            f"{values_path}: expected one {section}.{key}, found {len(found)}"
        )
        return None
    return found[0].strip().strip('"')


def values_env(name):
    found = re.findall(
        rf"^  - name: {re.escape(name)}\n\s*value: (?P<value>.+)$", values, re.MULTILINE
    )
    if len(found) != 1:
        problems.append(
            f"{values_path}: expected one {name} entry under extraEnv, found {len(found)}"
        )
        return None
    return found[0].strip().strip('"')


# The credentials and bucket the server is started with are the ones the chart
# is handed. A mismatch is an arm that comes up and then cannot write.
for server_name, chart_name in (
    ("GARAGE_DEFAULT_ACCESS_KEY", "AWS_ACCESS_KEY_ID"),
    ("GARAGE_DEFAULT_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"),
):
    served, charted = manifest_env(server_name), values_env(chart_name)
    if served is not None and charted is not None and served != charted:
        problems.append(
            f"{server_name}={served} in {manifest_path} but {chart_name}={charted} "
            f"in {values_path}"
        )

bucket = manifest_env("GARAGE_DEFAULT_BUCKET")
charted_bucket = values_scalar("s3", "bucket")
if bucket != "siglake-warehouse":
    problems.append(f"{manifest_path}: the Garage bucket is {bucket}, not siglake-warehouse")
if charted_bucket != "siglake-warehouse":
    problems.append(f"{values_path}: s3.bucket is {charted_bucket}, not siglake-warehouse")
if bucket is not None and bucket != charted_bucket:
    problems.append(f"bucket {bucket} in {manifest_path} but {charted_bucket} in {values_path}")

# The readiness Job must wait on the same bucket at the same endpoint, or it
# gates on nothing.
if bucket and f"garage/{bucket}" not in manifest:
    problems.append(f"{manifest_path}: the readiness Job does not list garage/{bucket}")

# AWS_REGION and AWS_ENDPOINT_URL are rendered from these two keys by the
# chart's _helpers.tpl; the card's four variables are these plus the two above.
endpoint = values_scalar("s3", "endpoint")
region = values_scalar("s3", "region")
if endpoint != "http://garage:3900":
    problems.append(f"{values_path}: s3.endpoint is {endpoint}, not http://garage:3900")
if region != "us-east-1":
    problems.append(f"{values_path}: s3.region is {region}, not us-east-1")

served_region = re.findall(r'^s3_region = "(?P<value>[^"]+)"$', toml, re.MULTILINE)
if len(served_region) != 1:
    problems.append(f"{toml_path}: expected one s3_region, found {len(served_region)}")
elif served_region[0] != region:
    problems.append(
        f"{toml_path} serves region {served_region[0]} but {values_path} signs for {region}"
    )

# The endpoint has to name a Service this manifest declares, on a port it
# exposes -- a chart pointed at a hostname nothing serves fails at the first
# write, deep inside a round.
if endpoint:
    host, _, port = endpoint.removeprefix("http://").partition(":")
    if not re.search(rf"^kind: Service\nmetadata:\n  name: {re.escape(host)}$", manifest, re.MULTILINE):
        problems.append(f"{manifest_path}: no Service named {host} for endpoint {endpoint}")
    if not re.search(rf"^\s*- name: s3\n\s*port: {re.escape(port)}$", manifest, re.MULTILINE):
        problems.append(f"{manifest_path}: the Service exposes no s3 port {port}")
    if not re.search(rf"^\s*- containerPort: {re.escape(port)}$", manifest, re.MULTILINE):
        problems.append(f"{manifest_path}: the container exposes no port {port}")

# One Garage version across the two stacks. The compose arm is where the
# conditional-write evidence was taken; a kind arm on a different build would
# not be the same store.
kind_image = re.findall(r"^\s*image: (?P<value>dxflrs/garage:\S+)$", manifest, re.MULTILINE)
compose_image = re.findall(r"^\s*image: (?P<value>dxflrs/garage:\S+)$", compose, re.MULTILINE)
if len(kind_image) != 1:
    problems.append(f"{manifest_path}: expected one dxflrs/garage image, found {len(kind_image)}")
if len(compose_image) != 1:
    problems.append(f"{compose_path}: expected one dxflrs/garage image, found {len(compose_image)}")
if len(kind_image) == 1 and len(compose_image) == 1 and kind_image[0] != compose_image[0]:
    problems.append(
        f"kind runs {kind_image[0]} but compose runs {compose_image[0]}"
    )

# The server configuration is installed from the one tracked file, never
# copied into the manifest.
if "metadata_dir" in manifest or "rpc_secret" in manifest:
    problems.append(f"{manifest_path}: inlines the Garage configuration instead of mounting the ConfigMap")
for required in (
    "name: garage-config",
    "mountPath: /etc/garage.toml",
    "subPath: garage.toml",
):
    if required not in manifest:
        problems.append(f"{manifest_path}: missing `{required}`")

for problem in problems:
    print(f"FAIL {problem}", file=sys.stderr)
sys.exit(1 if problems else 0)
PY
  fail "the Garage manifest, values overlay, server configuration and compose service disagree"

echo "ok (Garage manifest, values overlay, garage.toml and compose agree)"

# --- kind-up.sh, end to end, against stand-ins -------------------------------

sandbox=$(mktemp -d "${TMPDIR:-/tmp}/siglake-kind-object-store.XXXXXX")
trap 'rm -rf -- "$sandbox"' EXIT
mkdir -p "$sandbox/bin"

cat >"$sandbox/bin/kind" <<'EOF'
#!/usr/bin/env bash
printf 'kind %s\n' "$*" >>"$CALLS"
# No clusters exist, so kind-up.sh takes its create path.
EOF
cat >"$sandbox/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf 'docker %s\n' "$*" >>"$CALLS"
# `ps -a` answering nothing is a box with no abandoned control plane.
EOF
cat >"$sandbox/bin/helm" <<'EOF'
#!/usr/bin/env bash
printf 'helm %s\n' "$*" >>"$CALLS"
EOF
cat >"$sandbox/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
printf 'kubectl %s\n' "$*" >>"$CALLS"
case "$*" in
*"apply -f -")
  # The ConfigMap body arrives on stdin; read all of it, or the writer dies of
  # SIGPIPE and the pipeline's failure is blamed on the wrong half.
  cat >>"$CONFIGMAP_APPLIED"
  ;;
*"-o name"*)
  # `get pods -l app=<store> -o name`: answer with one pod so the readiness
  # wait does not spin for its whole timeout.
  printf 'pod/stand-in-0\n'
  ;;
*"create configmap"*)
  printf 'STAND-IN CONFIGMAP %s\n' "$*"
  ;;
esac
EOF
chmod +x "$sandbox/bin"/*

run_kind_up() {
  local arm=$1
  shift
  local calls="$sandbox/$arm.calls" applied="$sandbox/$arm.configmap"
  : >"$calls"
  : >"$applied"
  env PATH="$sandbox/bin:$PATH" CALLS="$calls" CONFIGMAP_APPLIED="$applied" \
    "$@" "$ROOT/$UP" >"$sandbox/$arm.out" 2>"$sandbox/$arm.err"
}

# The default arm: no selector at all, byte for byte the deployment that
# shipped before the selector existed.
run_kind_up default ||
  fail "kind-up.sh failed with no selector; see $sandbox/default.err"
default_calls=$(<"$sandbox/default.calls")
contains "$default_calls" "kubectl apply -f $ROOT/deploy/kind/manifests/postgres.yaml" ||
  fail "the default arm did not apply the postgres manifest"
contains "$default_calls" "kubectl apply -f $ROOT/$MINIO_MANIFEST" ||
  fail "the default arm did not apply $MINIO_MANIFEST"
contains "$default_calls" "kubectl wait --for=condition=complete job/minio-bucket-init --timeout=120s" ||
  fail "the default arm did not wait for minio-bucket-init"
contains "$default_calls" "kubectl get pods -l app=minio -o name" ||
  fail "the default arm did not wait for a minio pod"
contains "$default_calls" "helm upgrade --install siglake $ROOT/deploy/helm/siglake --values $ROOT/$BASE_VALUES --wait --timeout 5m" ||
  fail "the default arm did not install the chart with exactly $BASE_VALUES"
! contains "$default_calls" garage ||
  fail "the default arm reached Garage"
[[ ! -s "$sandbox/default.configmap" ]] ||
  fail "the default arm installed a store ConfigMap"

# The explicit minio arm must be the same deployment as the default one.
run_kind_up minio SIGLAKE_OBJECT_STORE=minio ||
  fail "kind-up.sh failed with SIGLAKE_OBJECT_STORE=minio; see $sandbox/minio.err"
[[ "$(<"$sandbox/minio.calls")" == "$default_calls" ]] ||
  fail "SIGLAKE_OBJECT_STORE=minio deployed something other than the default arm"

run_kind_up garage SIGLAKE_OBJECT_STORE=garage ||
  fail "kind-up.sh failed with SIGLAKE_OBJECT_STORE=garage; see $sandbox/garage.err"
garage_calls=$(<"$sandbox/garage.calls")
contains "$garage_calls" "kubectl apply -f $ROOT/deploy/kind/manifests/postgres.yaml" ||
  fail "the garage arm did not apply the postgres manifest"
contains "$garage_calls" "kubectl create configmap garage-config --from-file=garage.toml=$ROOT/$GARAGE_TOML --dry-run=client -o yaml" ||
  fail "the garage arm did not install $GARAGE_TOML as the garage-config ConfigMap"
contains "$(<"$sandbox/garage.configmap")" "STAND-IN CONFIGMAP create configmap garage-config" ||
  fail "the generated ConfigMap never reached kubectl apply"
contains "$garage_calls" "kubectl apply -f $ROOT/$GARAGE_MANIFEST" ||
  fail "the garage arm did not apply $GARAGE_MANIFEST"
! contains "$garage_calls" "$MINIO_MANIFEST" ||
  fail "the garage arm also applied the MinIO manifest"
contains "$garage_calls" "kubectl get pods -l app=garage -o name" ||
  fail "the garage arm did not wait for a garage pod"
contains "$garage_calls" "kubectl wait --for=condition=complete job/garage-bucket-check --timeout=120s" ||
  fail "the garage arm did not wait for the bucket readiness Job"
contains "$garage_calls" "helm upgrade --install siglake $ROOT/deploy/helm/siglake --values $ROOT/$BASE_VALUES --values $ROOT/$GARAGE_VALUES --wait --timeout 5m" ||
  fail "the garage arm did not install the chart with the base values then the Garage overlay"

# An unknown selector stops before anything is created. Were the refusal
# missing, the stand-ins would record the whole MinIO deployment under a name
# that asked for neither store.
for refused in minioo MinIO "garage "; do
  if run_kind_up refused SIGLAKE_OBJECT_STORE="$refused"; then
    fail "kind-up.sh accepted SIGLAKE_OBJECT_STORE='$refused'"
  fi
  contains "$(<"$sandbox/refused.err")" \
    "SIGLAKE_OBJECT_STORE must be 'minio' or 'garage' (got '$refused')" ||
    fail "kind-up.sh did not name the rejected selector '$refused'"
  [[ ! -s "$sandbox/refused.calls" ]] ||
    fail "kind-up.sh reached kind, kubectl, helm or docker before refusing '$refused'"
done

echo "ok (kind-up.sh: minio default, garage manifest + ConfigMap + overlay, typos refused before creating anything)"

# --- kind-round.sh: one resolution, both deployment stages -------------------

round_body=$(grep -vE '^[[:space:]]*(#|$)' "$ROUND")
contains "$round_body" 'OBJECT_STORE="$(siglake_kind_object_store "${SIGLAKE_OBJECT_STORE:-}")"' ||
  fail "$ROUND does not resolve the selector through the shared resolver"
contains "$round_body" 'SIGLAKE_OBJECT_STORE="$OBJECT_STORE" \' ||
  fail "$ROUND does not forward the resolved store to the kind-up.sh bootstrap"
contains "$round_body" 'for object_store_values_file in "${OBJECT_STORE_VALUES[@]}"; do' ||
  fail "$ROUND does not build the monitoring upgrade's values list from the resolved store"
contains "$round_body" 'SIGLAKE_HELM_ARGS+=(--values "$ROOT/$object_store_values_file")' ||
  fail "$ROUND does not pass the resolved values files to the monitoring upgrade"
! contains "$round_body" '--values "$ROOT/deploy/kind/values.kind.yaml"' ||
  fail "$ROUND still names the MinIO values file directly in a deployment stage"

# Both stages have to read the SAME variable: a second resolution could answer
# differently if the environment changed between them.
resolutions=$(printf '%s\n' "$round_body" | grep -c 'siglake_kind_object_store' || true)
[[ "$resolutions" == 1 ]] ||
  fail "$ROUND resolves the selector $resolutions times, expected exactly 1"

# The two opt-ins that address MinIO by name must refuse another store rather
# than measure it through MinIO's endpoint.
contains "$round_body" 'the mirror-reclaim qualification addresses MinIO directly and cannot run with SIGLAKE_OBJECT_STORE=%s' ||
  fail "$ROUND does not refuse the mirror-reclaim arm on a non-MinIO store"
contains "$round_body" 'the compactor wake-up capture bootstraps its own MinIO cluster and cannot run with SIGLAKE_OBJECT_STORE=%s' ||
  fail "$ROUND does not refuse the wake-up capture on a non-MinIO store"

# Drive the real script's refusals. PATH holds stand-ins that record and fail,
# so a refusal that did not happen shows up as a recorded call rather than as
# a cluster.
mkdir -p "$sandbox/refuse-bin"
for tool in kind kubectl helm docker; do
  cat >"$sandbox/refuse-bin/$tool" <<EOF
#!/usr/bin/env bash
printf '%s %s\n' "$tool" "\$*" >>"\$CALLS"
exit 1
EOF
  chmod +x "$sandbox/refuse-bin/$tool"
done

run_round_refusal() {
  local arm=$1
  shift
  local calls="$sandbox/round-$arm.calls"
  : >"$calls"
  if env PATH="$sandbox/refuse-bin:$PATH" CALLS="$calls" KEEP=0 "$@" \
    "$ROOT/$ROUND" >"$sandbox/round-$arm.out" 2>"$sandbox/round-$arm.err"; then
    fail "kind-round.sh accepted the $arm combination"
  fi
  [[ ! -s "$calls" ]] ||
    fail "kind-round.sh reached a cluster tool before refusing the $arm combination: $(<"$calls")"
}

run_round_refusal unknown-store SIGLAKE_OBJECT_STORE=minioo
contains "$(<"$sandbox/round-unknown-store.err")" \
  "SIGLAKE_OBJECT_STORE must be 'minio' or 'garage' (got 'minioo')" ||
  fail "kind-round.sh did not name the rejected selector"

run_round_refusal mirror-reclaim \
  SIGLAKE_OBJECT_STORE=garage \
  KIND_ROUND_MIRROR_RECLAIM_ARM=off \
  KIND_ROUND_CATALOG_CLAIM_ENABLED=false \
  KIND_ROUND_WAL_MIRROR_ENABLED=true \
  KIND_ROUND_WAL_MIRROR_ACTIVE_INTERVAL_SECS=0 \
  KIND_ROUND_COMMITTED_RETENTION_SECS=901 \
  KIND_ROUND_MIRROR_LEDGER_RECLAIM=false \
  KIND_ROUND_LOAD_SECONDS=3600
contains "$(<"$sandbox/round-mirror-reclaim.err")" \
  "the mirror-reclaim qualification addresses MinIO directly and cannot run with SIGLAKE_OBJECT_STORE=garage" ||
  fail "kind-round.sh refused the mirror-reclaim arm for some other reason"

run_round_refusal wakeup \
  SIGLAKE_OBJECT_STORE=garage \
  COMPACTOR_WAKEUP_CAPTURE=1 \
  COMPACTOR_WAKEUP_CLUSTER=wakeup
contains "$(<"$sandbox/round-wakeup.err")" \
  "the compactor wake-up capture bootstraps its own MinIO cluster and cannot run with SIGLAKE_OBJECT_STORE=garage" ||
  fail "kind-round.sh refused the wake-up capture for some other reason"

# The positive path: source the round's prelude -- everything up to the first
# deployment stage -- and read what both stages will use.
mkdir -p "$sandbox/round/scripts"
sed -n "1,/^${FIRST_ROUND_LINE}\$/p" "$ROUND" | sed '$d' >"$sandbox/round/scripts/prelude.bash"
cp "$COMMON" "$sandbox/round/scripts/kind-common.bash"
cat >"$sandbox/round/scripts/print-store.bash" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/prelude.bash"
trap - EXIT INT TERM
printf '%s\n' "$OBJECT_STORE"
printf '%s\n' "${OBJECT_STORE_VALUES[@]}"
rm -rf -- "$TMP_DIR"
EOF
chmod +x "$sandbox/round/scripts/print-store.bash"

for arm in "minio:minio" "garage:garage" ":minio"; do
  selector=${arm%%:*}
  expected=${arm##*:}
  if [[ -n "$selector" ]]; then
    resolved=$(env TMPDIR="$sandbox" SIGLAKE_OBJECT_STORE="$selector" \
      "$sandbox/round/scripts/print-store.bash" 2>/dev/null)
  else
    resolved=$(env TMPDIR="$sandbox" "$sandbox/round/scripts/print-store.bash" 2>/dev/null)
  fi
  want="$expected"$'\n'"$(siglake_kind_store_values "$expected")"
  [[ "$resolved" == "$want" ]] ||
    fail "the round resolved selector '${selector:-<unset>}' to:"$'\n'"$resolved"$'\n'"expected:"$'\n'"$want"
done

echo "ok (kind-round.sh: one resolution, forwarded to both deployment stages, MinIO-only opt-ins refused)"
