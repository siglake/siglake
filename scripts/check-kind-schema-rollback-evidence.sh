#!/usr/bin/env bash
# Offline fixtures and PATH-stand-in run for the schema-rollback probe.

set -euo pipefail

cd "$(dirname "$0")/.."

GRADER=scripts/grade-kind-schema-rollback.py
FIXTURE=scripts/testdata/kind-schema-rollback-verified.json
PROBE=scripts/kind-schema-rollback-probe.sh
ROUND=scripts/kind-round.sh
STANDIN=scripts/testdata/kind-schema-rollback-standin.py

fail() { echo "FAIL $*" >&2; exit 1; }
contains() { case "$1" in *"$2"*) ;; *) return 1 ;; esac; }

for file in "$GRADER" "$FIXTURE" "$PROBE" "$ROUND" "$STANDIN"; do
  [[ -f "$file" ]] || fail "$file does not exist"
done
command -v setsid >/dev/null 2>&1 || fail "missing required tool: setsid"

round_body=$(grep -vE '^[[:space:]]*(#|$)' "$ROUND")
contains "$round_body" 'SCHEMA_ROLLBACK_PROBE="${SCHEMA_ROLLBACK_PROBE:-0}"' ||
  fail "$ROUND does not default SCHEMA_ROLLBACK_PROBE off"
contains "$round_body" 'scripts/kind-schema-rollback-probe.sh' ||
  fail "$ROUND never invokes the schema-rollback probe"
contains "$(<crates/siglake-core/Cargo.toml)" 'experimental-schema-rollback-probe = []' ||
  fail "siglake-core does not declare the probe-only feature"
contains "$(<deploy/Dockerfile)" 'ARG SIGLAKE_CARGO_FEATURES=""' ||
  fail "the release Dockerfile does not default the probe feature off"
contains "$(<"$PROBE")" 'PROBE_FEATURE=siglake-core/experimental-schema-rollback-probe' ||
  fail "$PROBE does not build image B with the declared feature"

# The operator arm's Prometheus address, end to end: the round builds it from
# the release and namespace it installed Prometheus under, the probe passes it
# to the chart, and the chart renders it onto the binary's flag. Left to the
# chart default the operator reaches a Service the round never installed, holds
# every replica count (reconciler.rs: "prometheus query failed; HOLDING") and
# the arm meets that as a rollout timeout with no log to explain it (#3468).
if ! python3 - "$ROUND" "$PROBE" <<'PY'
import pathlib
import re
import sys

round_text, probe_text = (
    pathlib.Path(path).read_text(encoding="utf-8") for path in sys.argv[1:]
)

keda = re.search(r"--set-string keda\.prometheusServerAddress=\"([^\"]+)\"", round_text)
if not keda:
    raise SystemExit("the round no longer passes keda.prometheusServerAddress")
passed = re.search(
    r"SCHEMA_ROLLBACK_OPERATOR_PROMETHEUS_URL=\"([^\"]+)\"", round_text
)
if not passed:
    raise SystemExit("the round does not pass SCHEMA_ROLLBACK_OPERATOR_PROMETHEUS_URL")
# Both are the same address written the same way: release and namespace come
# from the round's own variables, so moving Prometheus moves both at once.
if passed.group(1) != (
    "http://${PROM_RELEASE}-prometheus.${PROM_NAMESPACE}.svc.cluster.local:9090"
):
    raise SystemExit(f"the round's operator Prometheus URL is {passed.group(1)!r}")
if keda.group(1).rsplit(":", 1)[0] != passed.group(1).rsplit(":", 1)[0]:
    raise SystemExit(
        f"the round addresses Prometheus two ways: {keda.group(1)!r} for KEDA, "
        f"{passed.group(1)!r} for the operator"
    )
if "SCHEMA_ROLLBACK_OPERATOR_PROMETHEUS_URL" not in probe_text:
    raise SystemExit("the probe does not read the knob the round passes")
if '--set-string prometheus.url="$OPERATOR_PROMETHEUS_URL"' not in probe_text:
    raise SystemExit("the probe's operator install does not set prometheus.url")
PY
then
  fail "the round and probe do not agree on the operator's Prometheus address"
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/siglake-schema-rollback-evidence.XXXXXX")
probe_pgid=
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [[ -n "$probe_pgid" ]]; then
    kill -TERM -- "-$probe_pgid" 2>/dev/null || true
    for _ in $(seq 1 50); do
      kill -0 -- "-$probe_pgid" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL -- "-$probe_pgid" 2>/dev/null || true
    wait "$probe_pgid" 2>/dev/null || true
  fi
  rm -rf -- "$work"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# What `helm template` would put on the command line, without helm: the args
# block is two literal lines, so the render is a substitution. Also holds the
# binary default, the operator chart default and the siglake chart's KEDA
# default to one Prometheus address, and checks the chart comment says whose
# Service it is.
check_prometheus_defaults() {
  python3 - "$@" <<'PY'
import pathlib
import re
import sys

deployment_path, operator_values, siglake_values, operator_main = sys.argv[1:]
lines = pathlib.Path(deployment_path).read_text(encoding="utf-8").splitlines()
flag = [i for i, line in enumerate(lines) if line.strip() == "- --prometheus-url"]
if len(flag) != 1:
    raise SystemExit(f"expected one --prometheus-url arg, found {len(flag)}")
value = lines[flag[0] + 1].strip()
if value != "- {{ .Values.prometheus.url | quote }}":
    raise SystemExit(f"--prometheus-url does not render the value: {value!r}")

url = "http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090"
rendered = [lines[flag[0]].strip(), value.replace("{{ .Values.prometheus.url | quote }}", f'"{url}"')]
if rendered != ["- --prometheus-url", f'- "{url}"']:
    raise SystemExit(f"the probe's flags do not render onto the binary: {rendered}")


def default(path, pattern):
    found = re.search(pattern, pathlib.Path(path).read_text(encoding="utf-8"), re.M)
    if not found:
        raise SystemExit(f"{path} no longer declares {pattern!r}")
    return found.group(1)


operator_default = default(operator_values, r"^  url: (\S+)$")
keda_default = default(siglake_values, r"^  prometheusServerAddress: (\S+)$")
binary_default = default(
    operator_main,
    r'''(?s)#\[arg\(
\s*long,
\s*env = "SIGLAKE_PROMETHEUS_URL",
\s*default_value = "([^"]+)"
\s*\)\]
\s*prometheus_url: String''',
)
operator_source = pathlib.Path(operator_main).read_text(encoding="utf-8")
if binary_default != operator_default:
    raise SystemExit(
        f"{operator_main} defaults to {binary_default!r}, while "
        f"{operator_values} defaults to {operator_default!r}"
    )
if f"/// `{binary_default}`." not in operator_source:
    raise SystemExit(
        f"{operator_main} does not document its --prometheus-url default "
        f"as {binary_default!r}"
    )
if (
    "prometheus-community/prometheus" not in operator_source
    or "kube-prometheus-stack" not in operator_source
):
    raise SystemExit(
        f"{operator_main} does not identify the default Service owner and override case"
    )
if operator_default != keda_default:
    raise SystemExit(
        f"{operator_values} defaults to {operator_default!r}, while "
        f"{siglake_values} defaults to {keda_default!r}"
    )
comment = pathlib.Path(operator_values).read_text(encoding="utf-8").split("prometheus:")[0]
service = operator_default.split("//", 1)[1].split(".", 1)[0]
if service not in comment or "prometheus-community" not in comment:
    raise SystemExit(
        f"the comment above prometheus.url does not name {service!r} and the chart it belongs to"
    )
PY
}

deployment=deploy/helm/siglake-operator/templates/deployment.yaml
operator_values=deploy/helm/siglake-operator/values.yaml
siglake_values=deploy/helm/siglake/values.yaml
operator_main=crates/siglake-operator/src/main.rs
if ! parity_error=$(check_prometheus_defaults \
  "$deployment" "$operator_values" "$siglake_values" "$operator_main" 2>&1)
then
  fail "the operator --prometheus-url render or default is wrong: $parity_error"
fi

prometheus_drift_case() {
  local name=$1 drifted=$2
  local case_dir="$work/prometheus-$name"
  local case_main="$case_dir/main.rs"
  local case_operator_values="$case_dir/operator-values.yaml"
  local case_siglake_values="$case_dir/siglake-values.yaml"
  local output rc=0

  mkdir -p "$case_dir"
  cp "$operator_main" "$case_main"
  cp "$operator_values" "$case_operator_values"
  cp "$siglake_values" "$case_siglake_values"
  python3 - "$drifted" "$case_main" "$case_operator_values" "$case_siglake_values" <<'PY'
import pathlib
import re
import sys

drifted, main_path, operator_values, siglake_values = sys.argv[1:]
paths = {
    "main.rs": (
        main_path,
        r'(env = "SIGLAKE_PROMETHEUS_URL",\n\s*default_value = ")([^"]+)(")',
    ),
    "operator-values.yaml": (
        operator_values,
        r"^(  url: )(\S+)$",
    ),
    "siglake-values.yaml": (
        siglake_values,
        r"^(  prometheusServerAddress: )(\S+)$",
    ),
}
path, pattern = paths[drifted]
source = pathlib.Path(path).read_text(encoding="utf-8")
drifted_source, count = re.subn(
    pattern,
    lambda match: (
        f"{match.group(1)}{match.group(2)}/drift"
        f"{match.group(3) if match.lastindex == 3 else ''}"
    ),
    source,
    flags=re.M,
)
if count != 1:
    raise SystemExit(f"expected one default to drift in {path}, found {count}")
pathlib.Path(path).write_text(drifted_source, encoding="utf-8")
PY
  output=$(check_prometheus_defaults \
    "$deployment" "$case_operator_values" "$case_siglake_values" "$case_main" 2>&1) || rc=$?
  [[ "$rc" -ne 0 ]] || fail "$name drift was accepted"
  contains "$output" "$case_dir/$drifted" ||
    fail "$name drift did not name $case_dir/$drifted: $output"
}

prometheus_drift_case binary main.rs
prometheus_drift_case operator-chart operator-values.yaml
prometheus_drift_case siglake-chart siglake-values.yaml

grade_case() {
  local name=$1 input=$2 expected_rc=$3 want=$4 rc=0
  local output="$work/$name.json" log="$work/$name.log"
  python3 "$GRADER" "$input" --output "$output" 2>"$log" || rc=$?
  [[ "$rc" -eq "$expected_rc" ]] || fail "$name exited $rc, expected $expected_rc: $(<"$log")"
  local grade problems
  grade=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["evidence"]["grade"])' "$output")
  problems=$(python3 -c 'import json,sys; print("\n".join(json.load(open(sys.argv[1]))["evidence"]["problems"]))' "$output")
  if [[ "$expected_rc" -eq 0 ]]; then
    [[ "$grade" == verified ]] || fail "$name was not verified: $problems"
  else
    [[ "$grade" == unverified ]] || fail "$name was not unverified"
    contains "$problems" "$want" || fail "$name was caught for the wrong reason: $problems"
  fi
}

grade_case passing "$FIXTURE" 0 ''

python3 - "$FIXTURE" "$work/missing-operator.input.json" <<'PY'
import json, sys
document = json.load(open(sys.argv[1], encoding="utf-8"))
del document["operator_arm"]
json.dump(document, open(sys.argv[2], "w", encoding="utf-8"))
PY
grade_case missing-operator "$work/missing-operator.input.json" 1 'missing operator arm'

python3 - "$FIXTURE" "$work/rollback-migrated.input.json" <<'PY'
import json, sys
document = json.load(open(sys.argv[1], encoding="utf-8"))
step = next(row for row in document["chart_arm"]["steps"] if row["name"] == "rollback_to_a")
step["migration_jobs"].append({
    "name": f"siglake-migrate-schema-{step['revision']}",
    "uid": "rollback-job",
    "created_at": "2026-09-09T00:00:00Z",
    "completed_at": "2026-09-09T00:00:01Z",
    "succeeded": 1,
    "failed": 0,
    "image": "siglake:kind",
})
json.dump(document, open(sys.argv[2], "w", encoding="utf-8"))
PY
grade_case rollback-migrated "$work/rollback-migrated.input.json" 1 'rollback ran a migration Job'

python3 - "$FIXTURE" "$work/missing-rollout.input.json" <<'PY'
import json, sys
document = json.load(open(sys.argv[1], encoding="utf-8"))
step = next(row for row in document["operator_arm"]["steps"] if row["name"] == "upgrade_to_b")
del step["pods"]
json.dump(document, open(sys.argv[2], "w", encoding="utf-8"))
PY
grade_case missing-rollout "$work/missing-rollout.input.json" 1 'observed no operator-managed pods'

python3 - "$FIXTURE" "$work/stale-rollout.input.json" <<'PY'
import json, sys
document = json.load(open(sys.argv[1], encoding="utf-8"))
step = next(
    row for row in document["operator_arm"]["steps"]
    if row["name"] == "revert_to_a_retained"
)
for pod in step["pods"]:
    pod["image"] = "siglake:kind-rollback-probe"
json.dump(document, open(sys.argv[2], "w", encoding="utf-8"))
PY
grade_case stale-rollout "$work/stale-rollout.input.json" 1 'still runs image'

# The arm is ADDITIVE: with SCHEMA_ROLLBACK_PROBE unset, the round runs what it
# ran before the arm existed.
#
# This was checked when the arm landed by stripping its two blocks out of
# kind-round.sh and diffing the remainder against `origin/main`'s copy. That
# comparison inverted the moment the arm merged: origin/main now HAS the blocks
# (3ae47b5), so the stripped file can never equal it again and the check failed
# on every branch cut afterwards -- first seen on task #2311's gate run,
# 2026-09-09, on a branch whose kind-round.sh is byte-identical to main's.
# Pinning the baseline to the pre-arm commit instead would have gone red on the
# next legitimate edit to the round, which is the same trap one commit later.
#
# So the property the diff stood in for is checked directly, which does not
# rot: everything the arm adds is inside a block conditioned on
# SCHEMA_ROLLBACK_PROBE, so the default path reaches none of it, and that block
# sits after the panel and scaling evidence and before the round's verdict.
if ! python3 - "$ROUND" <<'PY'
import pathlib
import sys

lines = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()

# Where the knob is declared and validated. Every mention of it in this span is
# settings, not a command the round runs.
declared = next(i for i, line in enumerate(lines)
                if line == 'SCHEMA_ROLLBACK_PROBE="${SCHEMA_ROLLBACK_PROBE:-0}"')
settings_end = next(i for i, line in enumerate(lines[declared:], declared)
                    if line == "}")

# Another opt-in may refuse being combined with this arm. That reference is a
# launch-time validation, not a second path into the schema probe.
qualification_settings = next(i for i, line in enumerate(lines)
                              if line == 'case "$MIRROR_RECLAIM_ARM" in')
qualification_settings_end = next(
    i for i, line in enumerate(lines[qualification_settings:], qualification_settings)
    if line == "esac"
)

# The opt-in block, and where it ends: a `fi` in the first column, so an `if`
# nested inside the block cannot close it early.
guard = [i for i, line in enumerate(lines)
         if line == 'if [[ "$SCHEMA_ROLLBACK_PROBE" == 1 ]]; then']
if len(guard) != 1:
    raise SystemExit(f"expected one opt-in block, found {len(guard)}: {guard}")
opens = guard[0]
closes = next(i for i, line in enumerate(lines[opens + 1:], opens + 1) if line == "fi")

# Nothing the arm brings may run outside that block.
tokens = ("SCHEMA_ROLLBACK", "kind-schema-rollback-probe.sh",
          "grade-kind-schema-rollback.py")
stray = [
    (i + 1, line) for i, line in enumerate(lines)
    if any(token in line for token in tokens)
    and not line.lstrip().startswith("#")
    and not declared <= i <= settings_end
    and not qualification_settings <= i <= qualification_settings_end
    and not opens <= i <= closes
]
if stray:
    raise SystemExit(f"the arm reaches the default path at {stray}")

body = "\n".join(lines[opens + 1:closes])
if "kind-schema-rollback-probe.sh" not in body:
    raise SystemExit("the opt-in block does not invoke the probe")
if not (declared < opens):
    raise SystemExit("the knob is declared after the block that reads it")

# Ordering, which is why the arm is where it is: it takes the release off the
# round's own image, so everything measured about that image is collected
# before it runs, and the round's verdict is taken after.
verdict = next(i for i, line in enumerate(lines)
               if line.startswith('[[ "$PANEL_FAILURES" -eq 0 ]]'))
evidence = max(i for i, line in enumerate(lines) if "SCALE_FAILURES=" in line)
if not (evidence < opens < closes < verdict):
    raise SystemExit(
        f"the arm is not between the round's evidence ({evidence + 1}) and its "
        f"verdict ({verdict + 1}): block at {opens + 1}-{closes + 1}"
    )
PY
then
  fail "SCHEMA_ROLLBACK_PROBE is not the only way to reach the arm"
fi

# A permanent aggregate mismatch must exhaust the bounded poll and retain both
# raw responses. A missing probe bucket must do the same even when its grouped
# sum equals count(*).
failed_probe_case() {
  local name=$1 behavior=$2 label=$3 want=$4
  local case_dir="$work/$name" rc=0
  mkdir -p "$case_dir/bin" "$case_dir/state" "$case_dir/results"
  for tool in curl docker helm kind kubectl; do
    ln -s "$PWD/$STANDIN" "$case_dir/bin/$tool"
  done
  : >"$case_dir/state/calls.log"
  PATH="$case_dir/bin:$PATH" \
  SCHEMA_ROLLBACK_STANDIN_STATE="$case_dir/state" \
  SCHEMA_ROLLBACK_STANDIN_GROUP_BEHAVIOR="$behavior" \
  KUBE_CONTEXT=kind-siglake \
  RESULTS_DIR="$case_dir/results" \
  SCHEMA_ROLLBACK_CONVERGE_TIMEOUT_SECONDS=2 \
  SCHEMA_ROLLBACK_CONVERGE_INTERVAL_SECONDS=1 \
  SCHEMA_ROLLBACK_RECONCILE_TIMEOUT_SECONDS=2 \
  SCHEMA_ROLLBACK_INGEST_BATCH=2 \
    setsid "$PROBE" >"$case_dir/probe.stdout" 2>"$case_dir/probe.stderr" &
  probe_pgid=$!
  wait "$probe_pgid" || rc=$?
  probe_pgid=
  [[ "$rc" -ne 0 ]] || fail "$name unexpectedly converged"
  contains "$(<"$case_dir/probe.stderr")" "$want" ||
    fail "$name failed without the expected diagnostic: $(<"$case_dir/probe.stderr")"
  [[ -s "$case_dir/results/schema-rollback-${label}-count.json" ]] ||
    fail "$name did not retain its count response"
  [[ -s "$case_dir/results/schema-rollback-${label}-group-by.json" ]] ||
    fail "$name did not retain its GROUP BY response"
  python3 - "$behavior" \
    "$case_dir/results/schema-rollback-${label}-count.json" \
    "$case_dir/results/schema-rollback-${label}-group-by.json" <<'PY'
import json
import sys

behavior, count_path, group_path = sys.argv[1:]
count = int(json.load(open(count_path, encoding="utf-8"))["rows"][0]["n"])
groups = json.load(open(group_path, encoding="utf-8"))["rows"]
grouped = sum(int(row["n"]) for row in groups)
if behavior == "mismatch":
    assert grouped != count, (grouped, count)
elif behavior == "missing-probe":
    assert grouped == count, (grouped, count)
    assert not any(str(row.get("rollback_probe")) == "1" for row in groups), groups
PY
}

failed_probe_case permanent-mismatch mismatch baseline 'grouped=9'
failed_probe_case missing-probe-bucket missing-probe ingest-under-b 'missing_buckets=1'

# A completed retained A Job and converged CR generation do not prove that the
# B pods rolled back. The probe must exhaust its rollout wait and retain the
# CR, workloads, pods and Jobs that explain the timeout.
stuck_rollout_case() {
  local case_dir="$work/stuck-rollout" rc=0 diagnostic
  mkdir -p "$case_dir/bin" "$case_dir/state" "$case_dir/results"
  for tool in curl docker helm kind kubectl; do
    ln -s "$PWD/$STANDIN" "$case_dir/bin/$tool"
  done
  : >"$case_dir/state/calls.log"
  PATH="$case_dir/bin:$PATH" \
  SCHEMA_ROLLBACK_STANDIN_STATE="$case_dir/state" \
  SCHEMA_ROLLBACK_STANDIN_ROLLOUT_BEHAVIOR=stuck-revert \
  KUBE_CONTEXT=kind-siglake \
  RESULTS_DIR="$case_dir/results" \
  SCHEMA_ROLLBACK_CONVERGE_TIMEOUT_SECONDS=2 \
  SCHEMA_ROLLBACK_CONVERGE_INTERVAL_SECONDS=1 \
  SCHEMA_ROLLBACK_RECONCILE_TIMEOUT_SECONDS=2 \
  SCHEMA_ROLLBACK_INGEST_BATCH=2 \
    setsid "$PROBE" >"$case_dir/probe.stdout" 2>"$case_dir/probe.stderr" &
  probe_pgid=$!
  wait "$probe_pgid" || rc=$?
  probe_pgid=
  [[ "$rc" -ne 0 ]] || fail "stuck rollout unexpectedly converged"
  contains "$(<"$case_dir/probe.stderr")" 'operator workloads did not roll out image siglake:kind' ||
    fail "stuck rollout failed without the timeout diagnostic: $(<"$case_dir/probe.stderr")"
  diagnostic="$case_dir/results/schema-rollback-operator-rollout-revert_to_a_retained-timeout.json"
  [[ -s "$diagnostic" ]] || fail "stuck rollout did not retain $diagnostic"
  # The timeout is the case where the operator's own log is the only thing that
  # can say why nothing rolled, so it is retained on that path too.
  contains "$(<"$case_dir/results/schema-rollback-operator.log")" \
    'revert_to_a_retained timed out waiting for siglake:kind' ||
    fail "stuck rollout did not retain the operator log for the timed-out step"
  python3 - "$diagnostic" <<'PY'
import json
import sys

document = json.load(open(sys.argv[1], encoding="utf-8"))
assert document["step"] == "revert_to_a_retained", document
assert document["cr"]["metadata"]["generation"] == document["cr"]["status"]["observedGeneration"]
jobs = document["jobs"]["items"]
old_a = next(job for job in jobs if job["spec"]["template"]["spec"]["containers"][0]["image"] == "siglake:kind")
assert old_a["status"]["succeeded"] == 1, old_a
workload_pods = [
    pod for pod in document["pods"]["items"]
    if pod["metadata"]["labels"]["app.kubernetes.io/component"]
    in {"ingester", "compactor", "query"}
]
assert all(
    pod["spec"]["containers"][0]["image"] == "siglake:kind-rollback-probe"
    for pod in workload_pods
), workload_pods
PY
}

stuck_rollout_case

# Run the real probe end to end with each GROUP BY observation trailing its
# count observation for two polls. These four names resolve only to the fixture
# dispatcher; git and python remain the host's read-only tools.
mkdir -p "$work/bin" "$work/state" "$work/results"
for tool in curl docker helm kind kubectl; do
  ln -s "$PWD/$STANDIN" "$work/bin/$tool"
done
cat >"$work/bin/git" <<'STANDIN'
#!/usr/bin/env bash
set -euo pipefail
printf 'called\n' >>"$SCHEMA_ROLLBACK_STANDIN_STATE/git-calls"
[[ "$*" == *'rev-parse HEAD' ]] || exit 64
printf '%s\n' '1111111111111111111111111111111111111111'
STANDIN
chmod +x "$work/bin/git"
: >"$work/state/calls.log"
PATH="$work/bin:$PATH" \
SCHEMA_ROLLBACK_STANDIN_STATE="$work/state" \
SCHEMA_ROLLBACK_STANDIN_GROUP_BEHAVIOR=delay:2 \
SCHEMA_ROLLBACK_STANDIN_ROLLOUT_BEHAVIOR=delay:2 \
KUBE_CONTEXT=kind-siglake \
RESULTS_DIR="$work/results" \
SCHEMA_ROLLBACK_CONVERGE_TIMEOUT_SECONDS=5 \
SCHEMA_ROLLBACK_CONVERGE_INTERVAL_SECONDS=1 \
SCHEMA_ROLLBACK_RECONCILE_TIMEOUT_SECONDS=5 \
SCHEMA_ROLLBACK_INGEST_BATCH=2 \
  setsid "$PROBE" >"$work/probe.stdout" 2>"$work/probe.stderr" &
probe_pgid=$!
wait "$probe_pgid" ||
  fail "stand-in probe failed: $(<"$work/probe.stderr")"
probe_pgid=

while IFS= read -r pid; do
  if kill -0 "$pid" 2>/dev/null; then
    fail "port-forward stand-in $pid survived the probe"
  fi
done <"$work/state/port-forward-pids"

if ! python3 - "$work/results/schema-rollback.json" "$work/state/calls.log" \
  "$work/state/group-lag.json" "$work/results/schema-rollback-operator.log" <<'PY'
import json, sys
evidence = json.load(open(sys.argv[1], encoding="utf-8"))
assert evidence["evidence"]["grade"] == "verified", evidence["evidence"]["problems"]
revisions = evidence["revisions"]
assert revisions["repository_commit"] == "1" * 40, revisions
assert revisions["repository_commit_source"] == "git_rev_parse_head", revisions
assert revisions["image_a"]["source_revision"] == "1" * 40, revisions
assert revisions["image_b"]["source_revision"] == "1" * 40, revisions
for step in evidence["operator_arm"]["steps"]:
    assert {pod["component"] for pod in step["pods"]} == {"ingester", "compactor", "query"}, step
calls = [json.loads(line) for line in open(sys.argv[2], encoding="utf-8") if line.strip()]
lag = json.load(open(sys.argv[3], encoding="utf-8"))
assert lag["delayed_responses"] == 6, lag

def find(after, tool, word):
    return next(i for i, call in enumerate(calls[after + 1 :], after + 1) if call[0] == tool and word in call[1:])

build_b = find(-1, "docker", "build")
load_b = find(build_b, "kind", "load")
upgrade_b = find(load_b, "helm", "upgrade")
rollback_a = find(upgrade_b, "helm", "rollback")
forward_b = find(rollback_a, "helm", "upgrade")
operator_install = next(
    i for i, call in enumerate(calls[forward_b + 1 :], forward_b + 1)
    if call[0] == "helm" and "--install" in call
)
operator_applies = [
    i for i, call in enumerate(calls[operator_install + 1 :], operator_install + 1)
    if call[0] == "kubectl" and "apply" in call and any(arg.endswith("cluster.yaml") for arg in call)
]
assert len(operator_applies) == 3, operator_applies
delete_a = find(operator_applies[-1], "kubectl", "delete")
reconcile_a = find(delete_a, "kubectl", "annotate")
assert build_b < load_b < upgrade_b < rollback_a < forward_b < operator_install
assert operator_applies[-1] < delete_a < reconcile_a

# The operator install carries the Prometheus address, and every operator step
# retains the operator's own log under a header naming it. The stand-in serves
# the reconciler's holding warning for any other address, so a log without it
# is the operator reading the Prometheus the round installed.
url = "http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090"
assert f"prometheus.url={url}" in calls[operator_install], calls[operator_install]
operator_log = open(sys.argv[4], encoding="utf-8").read()
for step in ("install_at_a", "upgrade_to_b", "revert_to_a_retained", "revert_to_a_recreated"):
    assert f" {step}\n" in operator_log, (step, operator_log)
assert operator_log.count(f"started with --prometheus-url {url}") == 4, operator_log
assert "HOLDING" not in operator_log, operator_log
PY
then
  fail "stand-in probe command sequence or evidence is wrong"
fi

# Drive the complete writer again with an injected source revision different
# from the stand-in git SHA. All three schema-rollback revision fields must
# stay equal, and the injected path must not execute git.
injected="$work/injected"
mkdir -p "$injected/bin" "$injected/state" "$injected/results"
for tool in curl docker helm kind kubectl; do
  ln -s "$PWD/$STANDIN" "$injected/bin/$tool"
done
ln -s "$work/bin/git" "$injected/bin/git"
: >"$injected/state/calls.log"
PATH="$injected/bin:$PATH" \
SCHEMA_ROLLBACK_STANDIN_STATE="$injected/state" \
SCHEMA_ROLLBACK_STANDIN_GROUP_BEHAVIOR=delay:2 \
SCHEMA_ROLLBACK_STANDIN_ROLLOUT_BEHAVIOR=delay:2 \
SIGLAKE_SOURCE_COMMIT=2222222222222222222222222222222222222222 \
KUBE_CONTEXT=kind-siglake \
RESULTS_DIR="$injected/results" \
SCHEMA_ROLLBACK_CONVERGE_TIMEOUT_SECONDS=5 \
SCHEMA_ROLLBACK_CONVERGE_INTERVAL_SECONDS=1 \
SCHEMA_ROLLBACK_RECONCILE_TIMEOUT_SECONDS=5 \
SCHEMA_ROLLBACK_INGEST_BATCH=2 \
  setsid "$PROBE" >"$injected/probe.stdout" 2>"$injected/probe.stderr" &
probe_pgid=$!
wait "$probe_pgid" ||
  fail "injected-revision probe failed: $(<"$injected/probe.stderr")"
probe_pgid=
python3 - "$injected/results/schema-rollback.json" <<'PY' ||
import json
import sys

evidence = json.load(open(sys.argv[1], encoding="utf-8"))
assert evidence["evidence"]["grade"] == "verified", evidence["evidence"]
revisions = evidence["revisions"]
assert revisions["repository_commit"] == "2" * 40, revisions
assert revisions["repository_commit_source"] == "siglake_source_commit_env", revisions
assert revisions["image_a"]["source_revision"] == "2" * 40, revisions
assert revisions["image_b"]["source_revision"] == "2" * 40, revisions
PY
  fail "the schema-rollback artifact writer did not retain the injected revision origin"
[[ $(wc -l <"$work/state/git-calls") -eq 1 ]] ||
  fail "the fallback schema-rollback run did not call git exactly once"
[[ ! -e "$injected/state/git-calls" ]] ||
  fail "the injected schema-rollback revision still called git"

echo "ok (5 grader fixtures; delayed GROUP BY and operator reconcile converged;" \
  "permanent mismatch, missing bucket and stuck rollout failed with retained diagnostics; probe passed without" \
  "port-forward leaks; the operator binary and both charts agree on the built-in Prometheus address;" \
  "the round, the probe and the chart agree on the round's operator" \
  "Prometheus address and every operator step retained the operator log; the arm is reachable only through" \
  "SCHEMA_ROLLBACK_PROBE, between the round's evidence and its verdict)"
