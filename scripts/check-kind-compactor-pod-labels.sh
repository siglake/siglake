#!/usr/bin/env bash
# Offline guard for the kind round's two-compactor shared-queue capture (#4151).

set -euo pipefail

cd "$(dirname "$0")/.."

ROUND=scripts/kind-round.sh
GRADER=scripts/grade-kind-compactor-pod-labels.py
FIXTURE=scripts/testdata/kind-compactor-pod-labels-verified.json
PROM_SOURCE=crates/siglake-operator/src/prom.rs
HELPERS=deploy/helm/siglake/templates/_helpers.tpl
FIRST_ROUND_LINE='log "bring up the base kind deployment"'

fail() { echo "FAIL $*" >&2; exit 1; }
contains() { case "$1" in *"$2"*) ;; *) return 1 ;; esac; }

# #5556: the SHA the stand-in `git` answers with, and the different SHA a
# launcher injects. The two must not be the same string, or the injected arm
# would pass on the checkout's answer.
STANDIN_COMMIT=0123456789abcdef0123456789abcdef01234567
INJECTED_COMMIT=5f3a51489c46f0bd38b3c28da1c1e0c8f5a7de21

for file in "$ROUND" "$GRADER" "$FIXTURE" "$PROM_SOURCE" "$HELPERS"; do
  [[ -f "$file" ]] || fail "$file does not exist"
done
bash -n "$ROUND"
python3 -m py_compile "$GRADER"

round_body=$(grep -vE '^[[:space:]]*(#|$)' "$ROUND")
contains "$round_body" 'COMPACTOR_POD_LABEL_CAPTURE="${COMPACTOR_POD_LABEL_CAPTURE:-0}"' ||
  fail "$ROUND does not default COMPACTOR_POD_LABEL_CAPTURE off"
target=$(printf '%s\n' "$round_body" | sed -n 's/^COMPACTOR_SCALE_TARGET=\([0-9][0-9]*\)$/\1/p')
[[ "$target" =~ ^[0-9]+$ ]] && ((target >= 2)) ||
  fail "$ROUND does not select at least two compactors"
for setting in \
  'WAL_MIRROR_ENABLED="${KIND_ROUND_WAL_MIRROR_ENABLED:-true}"' \
  'CATALOG_CLAIM_ENABLED="${KIND_ROUND_CATALOG_CLAIM_ENABLED:-true}"' \
  '--set wal.mirror.enabled="$WAL_MIRROR_ENABLED"' \
  '--set compactor.catalogClaim.enabled="$CATALOG_CLAIM_ENABLED"' \
  '--set compactor.replicas="$COMPACTOR_SCALE_TARGET"' \
  '--set compactor.commitBatch.targetMb="$COMPACTOR_CAPTURE_BATCH_TARGET_MB"' \
  '--set compactor.commitBatch.maxAgeSecs="$COMPACTOR_CAPTURE_BATCH_MAX_AGE_SECONDS"'; do
  contains "$round_body" "$setting" || fail "$ROUND lacks $setting"
done
contains "$round_body" '--reuse-values' ||
  fail "$ROUND does not test the chart upgrade path for two compactors"
contains "$round_body" 'timestamp(siglake_compactor_sealed_pending{%s})' ||
  fail "$ROUND does not retain source scrape timestamps"
contains "$round_body" 'compactor_capture_settled "$COMPACTOR_RAW_JSON" "$COMPACTOR_SAMPLE_TIMES_JSON"' ||
  fail "$ROUND does not require two settled scrape generations"

# Keep both chart refusals intact: multiple compactors still need the claim,
# and the claim still needs the mirror. This capture supplies those inputs; it
# does not relax either guard or enable the refused Pods custom metric.
helpers=$(<"$HELPERS")
contains "$helpers" 'gt $max 1' || fail "$HELPERS lost the multi-compactor threshold"
contains "$helpers" 'not .Values.compactor.catalogClaim.enabled' ||
  fail "$HELPERS no longer requires a catalog claim"
contains "$helpers" '.Values.compactor.catalogClaim.enabled (not .Values.wal.mirror.enabled)' ||
  fail "$HELPERS no longer requires mirroring for the claim"
if sed -n '/capture_compactor_pod_labels()/,/^}/p' "$ROUND" |
    grep -q 'autoscaling.compactor.customMetric'; then
  fail "the capture changes the refused claim-mode custom metric"
fi

python3 - "$ROUND" <<'PY' || fail "the opt-in phase is not last or its verdict is not deferred"
import sys

lines = open(sys.argv[1], encoding="utf-8").read().splitlines()
guard = [i for i, line in enumerate(lines)
         if line == 'if [[ "$COMPACTOR_POD_LABEL_CAPTURE" == 1 ]]; then']
if len(guard) != 1:
    raise SystemExit(f"expected one opt-in block, found {guard}")
call = next(i for i, line in enumerate(lines)
            if line.strip().startswith('capture_compactor_pod_labels "$next_event"'))
if not lines[call].endswith("|| true"):
    raise SystemExit("capture failure is not deferred")
schema = next(i for i, line in enumerate(lines)
              if line == 'if [[ "$SCHEMA_ROLLBACK_PROBE" == 1 ]]; then')
verdict = next(i for i, line in enumerate(lines)
               if line.startswith('[[ "$COMPACTOR_POD_LABEL_FAILURE" -eq 0 ]]'))
if not schema < guard[0] < call < verdict:
    raise SystemExit(f"expected schema arm, capture, verdict; got {schema}, {guard[0]}, {call}, {verdict}")
PY

sandbox=$(mktemp -d "${TMPDIR:-/tmp}/siglake-kind-compactor-pod-labels.XXXXXX")
trap 'rm -rf -- "$sandbox"' EXIT
mkdir -p "$sandbox/scripts" "$sandbox/tmp"
sed -n "1,/^${FIRST_ROUND_LINE}\$/p" "$ROUND" | sed '$d' >"$sandbox/scripts/prelude.bash"
cp scripts/kind-common.bash "$sandbox/scripts/kind-common.bash"
grep -q '^capture_compactor_pod_labels()' "$sandbox/scripts/prelude.bash" ||
  fail "the sourceable prefix does not define the compactor capture"

# Pin the round's expression to the operator source, rather than restating the
# expected query in this check.
python3 - "$PROM_SOURCE" "$sandbox/operator-expression" <<'PY'
import pathlib
import sys

source, out = sys.argv[1:]
matches = [
    line.strip() for line in pathlib.Path(source).read_text(encoding="utf-8").splitlines()
    if line.strip().startswith('"avg(sum by (pod) (siglake_compactor_sealed_pending')
    and "{namespace}" in line
]
if len(matches) != 1:
    raise SystemExit(f"expected one compactor format literal, found {len(matches)}")
literal = matches[0].rstrip(",").strip('"')
expression = (
    literal.replace('\\"', '"')
    .replace("{namespace}", "\0ns\0")
    .replace("{release}", "\0rel\0")
    .replace("{{", "{").replace("}}", "}")
    .replace("\0ns\0", "default").replace("\0rel\0", "siglake")
)
pathlib.Path(out).write_text(expression + "\n", encoding="utf-8")
PY
cat >"$sandbox/scripts/print-expression.bash" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/prelude.bash"
trap - EXIT INT TERM
compactor_operator_expression
printf '\n'
rm -rf -- "$TMP_DIR"
EOF
chmod +x "$sandbox/scripts/print-expression.bash"
expression=$(TMPDIR="$sandbox/tmp" "$sandbox/scripts/print-expression.bash" 2>/dev/null)
[[ "$expression" == "$(<"$sandbox/operator-expression")" ]] ||
  fail "the round's compactor expression differs from $PROM_SOURCE"

# Drive the real settling helper with the fixture's final raw/timestamp
# responses. The second generation advances every scrape timestamp; a replay
# of the first one must remain unsettled.
python3 - "$FIXTURE" "$sandbox/raw.json" "$sandbox/times.json" "$sandbox/expected" <<'PY'
import json
import sys

source, raw, times, expected = sys.argv[1:]
document = json.load(open(source, encoding="utf-8"))
json.dump(document["queries"]["raw"]["response"], open(raw, "w", encoding="utf-8"))
json.dump(document["queries"]["sample_times"]["response"], open(times, "w", encoding="utf-8"))
open(expected, "w", encoding="utf-8").write("\n".join(document["expected_pods"]) + "\n")
PY
cat >"$sandbox/scripts/settle.bash" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/prelude.bash"
trap - EXIT INT TERM
set +e
compactor_capture_settled "$RAW" "$TIMES" "$EXPECTED" "$CANDIDATE" "$SETTLED"
rc=$?
set -e
printf '%s\n' "$rc"
rm -rf -- "$TMP_DIR"
EOF
chmod +x "$sandbox/scripts/settle.bash"
common=(TMPDIR="$sandbox/tmp" RAW="$sandbox/raw.json" TIMES="$sandbox/times.json"
  EXPECTED="$sandbox/expected" CANDIDATE="$sandbox/candidate.json" SETTLED="$sandbox/settled.json")
first=$(env "${common[@]}" "$sandbox/scripts/settle.bash" 2>/dev/null)
[[ "$first" == 1 ]] || fail "one scrape generation settled the capture"
replayed=$(env "${common[@]}" "$sandbox/scripts/settle.bash" 2>/dev/null)
[[ "$replayed" == 1 ]] || fail "an unadvanced scrape timestamp settled the capture"
python3 - "$sandbox/times.json" <<'PY'
import json
import sys

path = sys.argv[1]
document = json.load(open(path, encoding="utf-8"))
for row in document["data"]["result"]:
    row["value"][1] = str(float(row["value"][1]) + 15)
json.dump(document, open(path, "w", encoding="utf-8"))
PY
settled=$(env "${common[@]}" "$sandbox/scripts/settle.bash" 2>/dev/null)
[[ "$settled" == 0 && -s "$sandbox/settled.json" ]] ||
  fail "two advancing equal scrape generations did not settle"

# --- the enabled capture, end to end against stand-ins ----------------------
#
# #5556: the retained `repository_commit` has to be the revision the source
# came from. Under the aws-runner the checkout on the box is an rsynced
# snapshot committed there by a throwaway `git init`, so its HEAD resolves in
# no repository; a launcher that knows the real revision injects it. Both paths
# are driven here against a stand-in `git` answering a different SHA.
mkdir -p "$sandbox/bin"
cp "$GRADER" "$sandbox/scripts/"
cat >"$sandbox/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
printf 'kubectl %s\n' "$*" >>"$CALLS"
case "$*" in
*"get pods"*) cat "$FIXTURE_COMPACTOR_PODS" ;;
esac
EOF
cat >"$sandbox/bin/git" <<EOF
#!/usr/bin/env bash
printf 'git %s\n' "\$*" >>"\$CALLS"
printf '%s\n' "$STANDIN_COMMIT"
EOF
cat >"$sandbox/bin/curl" <<'EOF'
#!/usr/bin/env bash
url=
query=
at=
body=
while [ "$#" -gt 0 ]; do
  case "$1" in
  --data-urlencode)
    case "$2" in
    query=*) query="${2#query=}" ;;
    time=*) at="${2#time=}" ;;
    esac
    shift 2
    ;;
  --data-binary) body="${2#@}"; shift 2 ;;
  -H | -X | --connect-timeout | --max-time) shift 2 ;;
  -*) shift ;;
  *) url=$1; shift ;;
  esac
done
printf 'curl %s query=%s time=%s body=%s\n' "$url" "$query" "$at" "$body" >>"$CALLS"
case "$url" in
*/api/v1/query) python3 "$SANDBOX/prom-answer.py" "$query" "$at" ;;
*/v1/logs)
  [ -s "$body" ] || { echo "empty ingest payload" >&2; exit 1; }
  python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$body" ||
    { echo "malformed ingest payload" >&2; exit 1; }
  printf '{"partialSuccess":{}}'
  ;;
*) printf 'STUB curl %s\n' "$url" ;;
esac
EOF
for stub in helm kind; do
  printf '#!/usr/bin/env bash\nprintf "%s %%s\\n" "$*" >>"$CALLS"\n' "$stub" >"$sandbox/bin/$stub"
done
chmod +x "$sandbox"/bin/*

cat >"$sandbox/prom-answer.py" <<'EOF'
"""Answer one compactor Prometheus instant query from the passing fixture."""

import json
import os
import sys

query, at = sys.argv[1], float(sys.argv[2] or 0)
queries = json.load(open(os.environ["FIXTURE_CAPTURE"], encoding="utf-8"))["queries"]
for name in ("raw", "sample_times", "per_pod", "operator_expression"):
    if queries[name]["expression"] != query:
        continue
    response = json.loads(json.dumps(queries[name]["response"]))
    shift = 0
    if name == "sample_times":
        # Every scrape generation the round polls carries later source sample
        # times, as a live Prometheus would; the round settles on the second.
        state = os.environ["GENERATION_STATE"]
        try:
            generation = int(open(state, encoding="utf-8").read())
        except (OSError, ValueError):
            generation = 0
        open(state, "w", encoding="utf-8").write(str(generation + 1))
        shift = 15 * generation
    for row in response["data"]["result"]:
        row["value"] = [at, f"{float(row['value'][1]) + shift:.0f}"]
    print(json.dumps(response))
    raise SystemExit(0)
print(f"unexpected query: {query}", file=sys.stderr)
raise SystemExit(1)
EOF

python3 - "$FIXTURE" "$sandbox/compactor-pods.json" <<'PY'
import json
import sys

source, out = sys.argv[1:]
document = json.load(open(source, encoding="utf-8"))
items = [
    {
        "metadata": {"name": row["pod"]},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "containerStatuses": [
                {"name": row["container"], "image": row["image"], "imageID": row["image_id"]}
            ],
        },
    }
    for row in document["revisions"]["compactor_pods"]
]
json.dump({"items": items}, open(out, "w", encoding="utf-8"))
PY

python3 - "$ROUND" "$sandbox/scripts/drive.bash" <<'PY'
import pathlib
import sys

source, output = map(pathlib.Path, sys.argv[1:])
lines = source.read_text(encoding="utf-8").splitlines()
opens = next(i for i, line in enumerate(lines)
             if line == 'if [[ "$COMPACTOR_POD_LABEL_CAPTURE" == 1 ]]; then')
closes = next(i for i, line in enumerate(lines[opens + 1:], opens + 1) if line == "fi")
script = [
    "#!/usr/bin/env bash",
    "set -euo pipefail",
    "# shellcheck source=/dev/null",
    'source "$(dirname "${BASH_SOURCE[0]}")/prelude.bash"',
    "trap - EXIT INT TERM",
    "COMPACTOR_POD_LABEL_LOAD_SECONDS=1",
    "COMPACTOR_POD_LABEL_GRACE_SECONDS=30",
    "next_event=900000",
    *lines[opens:closes + 1],
    "printf 'DRIVE_FAILURE=%s\\n' \"$COMPACTOR_POD_LABEL_FAILURE\"",
    'rm -rf -- "$TMP_DIR"',
]
output.write_text("\n".join(script) + "\n", encoding="utf-8")
PY
chmod +x "$sandbox/scripts/drive.bash"

drive_capture() {
  local name=$1 results=$2
  shift 2
  : >"$sandbox/$name.calls"
  rm -f "$sandbox/$name.generation"
  env -i PATH="$sandbox/bin:/usr/bin:/bin" HOME="$HOME" \
    TMPDIR="$sandbox/tmp" SANDBOX="$sandbox" CALLS="$sandbox/$name.calls" \
    GENERATION_STATE="$sandbox/$name.generation" \
    COMPACTOR_POD_LABEL_CAPTURE=1 RESULTS_DIR="$results" \
    FIXTURE_CAPTURE="$PWD/$FIXTURE" FIXTURE_COMPACTOR_PODS="$sandbox/compactor-pods.json" \
    "$@" "$sandbox/scripts/drive.bash" >"$sandbox/$name.out" 2>&1 ||
    fail "the $name arm exited nonzero: $(<"$sandbox/$name.out")"
  local output
  output=$(<"$sandbox/$name.out")
  contains "$output" 'DRIVE_FAILURE=0' ||
    fail "the $name arm recorded a capture failure: $output"
  contains "$output" 'grade=verified' ||
    fail "the $name arm did not grade verified: $output"
  [[ -s "$results/compactor-pod-labels.json" ]] ||
    fail "the $name arm wrote no results/compactor-pod-labels.json"
}

expect_revision() {
  local file=$1 want_commit=$2 want_source=$3
  python3 - "$file" "$want_commit" "$want_source" <<'PY' || fail "$file does not pin the source revision the capture was given"
import json
import sys

path, want_commit, want_source = sys.argv[1:]
revisions = json.load(open(path, encoding="utf-8"))["revisions"]
if revisions.get("repository_commit") != want_commit:
    raise SystemExit(
        f"repository_commit is {revisions.get('repository_commit')!r}, expected {want_commit!r}"
    )
if revisions.get("repository_commit_source") != want_source:
    raise SystemExit(
        f"repository_commit_source is {revisions.get('repository_commit_source')!r}, "
        f"expected {want_source!r}"
    )
PY
}

# Nothing injected: the checkout's HEAD, recorded as such.
drive_capture checkout-commit "$sandbox/results-checkout-commit"
expect_revision "$sandbox/results-checkout-commit/compactor-pod-labels.json" \
  "$STANDIN_COMMIT" git_rev_parse_head
# Injected: the launcher's revision wins over the box's own commit.
drive_capture injected-commit "$sandbox/results-injected-commit" \
  SIGLAKE_SOURCE_COMMIT="$INJECTED_COMMIT"
expect_revision "$sandbox/results-injected-commit/compactor-pod-labels.json" \
  "$INJECTED_COMMIT" siglake_source_commit_env

# The default path stays off: no capture, no evidence, no stand-in reached.
: >"$sandbox/default-off.calls"
env -i PATH="$sandbox/bin:/usr/bin:/bin" HOME="$HOME" \
  TMPDIR="$sandbox/tmp" SANDBOX="$sandbox" CALLS="$sandbox/default-off.calls" \
  GENERATION_STATE="$sandbox/default-off.generation" \
  RESULTS_DIR="$sandbox/results-default-off" \
  FIXTURE_CAPTURE="$PWD/$FIXTURE" FIXTURE_COMPACTOR_PODS="$sandbox/compactor-pods.json" \
  "$sandbox/scripts/drive.bash" >"$sandbox/default-off.out" 2>&1 ||
  fail "the default-off arm exited nonzero: $(<"$sandbox/default-off.out")"
contains "$(<"$sandbox/default-off.out")" 'DRIVE_FAILURE=0' ||
  fail "the default-off arm recorded a capture failure: $(<"$sandbox/default-off.out")"
[[ ! -s "$sandbox/default-off.calls" ]] ||
  fail "the default-off arm reached the capture stand-ins: $(<"$sandbox/default-off.calls")"
[[ ! -e "$sandbox/results-default-off/compactor-pod-labels.json" ]] ||
  fail "the default-off arm wrote shared-queue evidence"

verified="$sandbox/verified.json"
python3 "$GRADER" "$FIXTURE" --output "$verified" 2>"$sandbox/verified.log" ||
  fail "the verified fixture failed: $(<"$sandbox/verified.log")"
python3 - "$verified" <<'PY'
import json
import sys

evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
assert evidence["grade"] == "verified", evidence
assert evidence["summary"]["pod_count"] == 2, evidence
assert evidence["summary"]["shared_queue"] == 7.0, evidence
assert evidence["summary"]["operator_value"] == 7.0, evidence
PY

fixtures=1
expect_unverified() {
  local mutation=$1 want=$2 input="$sandbox/$1.input.json" output="$sandbox/$1.output.json"
  python3 - "$FIXTURE" "$input" "$mutation" <<'PY'
import json
import sys

source, out, mutation = sys.argv[1:]
document = json.load(open(source, encoding="utf-8"))
raw = document["queries"]["raw"]["response"]["data"]["result"]
if mutation == "missing-pod":
    raw[0]["metric"].pop("pod")
elif mutation == "unequal-copies":
    raw[1]["value"][1] = "3"
elif mutation == "wrong-tenant":
    raw[0]["metric"]["tenant"] = "acme"
elif mutation == "stale-spread":
    document["queries"]["sample_times"]["response"]["data"]["result"][1]["value"][1] = "1789862050"
elif mutation == "one-generation":
    document["settling_samples"] = document["settling_samples"][:1]
elif mutation == "timestamps-did-not-advance":
    document["settling_samples"][1] = document["settling_samples"][0]
elif mutation == "operator-doubled":
    document["queries"]["operator_expression"]["response"]["data"]["result"][0]["value"][1] = "14"
elif mutation == "zero-queue":
    for row in raw:
        row["value"][1] = "0"
else:
    raise SystemExit(mutation)
json.dump(document, open(out, "w", encoding="utf-8"), indent=2)
PY
  if python3 "$GRADER" "$input" --output "$output" 2>"$sandbox/$mutation.log"; then
    fail "$mutation graded verified"
  fi
  grep -Fq "$want" "$sandbox/$mutation.log" ||
    fail "$mutation failed for the wrong reason: $(<"$sandbox/$mutation.log")"
  fixtures=$((fixtures + 1))
}

expect_unverified missing-pod 'carries no nonempty pod label'
expect_unverified unequal-copies 'did not publish the same shared queue'
expect_unverified wrong-tenant 'is not tenant=default'
expect_unverified stale-spread 'beyond one 15s scrape interval'
expect_unverified one-generation 'lacks two settled scrape generations'
expect_unverified timestamps-did-not-advance 'scrape timestamp did not advance'
expect_unverified operator-doubled 'not the shared queue'
expect_unverified zero-queue 'shared queue was not positive'

echo "ok ($fixtures offline compactor shared-queue fixtures and 3 driven capture arms; live install needs a kind round)"
