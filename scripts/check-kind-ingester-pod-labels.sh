#!/usr/bin/env bash
# Offline guard for the kind round's ingester per-pod label capture (#3647).
#
# Three things can silently make that capture worthless, and none of them shows
# up as a failed round:
#
#   1. The expression the round evaluates drifts from the one the operator runs
#      (crates/siglake-operator/src/prom.rs:177). The round would then retain a
#      perfectly good answer to a question nothing asks.
#   2. The capture is taken with one ingester pod, or with one series per pod.
#      Then the fleet total, the per-series average and the per-pod mean are the
#      same number and the reading proves nothing about which one the operator
#      computes.
#   3. The grader treats a missing `pod` label as a group of its own, so the
#      exact collapse the card is about -- `sum by (pod)` over unlabelled series
#      returning ONE group holding the fleet total -- grades verified.
#
# So this file pins the expression against the Rust source, drives the round's
# own capture function against stand-in `kubectl`, `curl` and `git` (no cluster,
# no container runtime, no network), and runs the grader over one passing
# fixture and seven mutations of it, each of which must be caught for its own
# stated reason.

set -euo pipefail

cd "$(dirname "$0")/.."

ROUND=scripts/kind-round.sh
GRADER=scripts/grade-kind-ingester-pod-labels.py
FIXTURE=scripts/testdata/kind-ingester-pod-labels-verified.json
PROM_SOURCE=crates/siglake-operator/src/prom.rs
CHART_VALUES=deploy/helm/siglake/values.yaml
# The line the sourceable prefix of the round stops before: everything below it
# builds a cluster.
FIRST_ROUND_LINE='log "bring up the base kind deployment"'

fail() { echo "FAIL $*" >&2; exit 1; }
contains() { case "$1" in *"$2"*) ;; *) return 1 ;; esac; }
lines() { printf '%s' "$1" | grep -c . || true; }

for file in "$ROUND" "$GRADER" "$FIXTURE" "$PROM_SOURCE" "$CHART_VALUES"; do
  [[ -f "$file" ]] || fail "$file does not exist"
done

# Full-line comments and blanks removed: the round's prose explains the very
# lines this reads, and a commented-out line is not a line it runs.
round_body=$(grep -vE '^[[:space:]]*(#|$)' "$ROUND")

# --- the round's constants and installed range -------------------------------
base=$(printf '%s\n' "$round_body" | sed -n 's/^INGESTER_SCALE_BASE=\([0-9][0-9]*\)$/\1/p')
target=$(printf '%s\n' "$round_body" | sed -n 's/^INGESTER_SCALE_TARGET=\([0-9][0-9]*\)$/\1/p')
[[ "$(lines "$base")" == 1 ]] ||
  fail "$ROUND has no single \`INGESTER_SCALE_BASE=<integer>\` assignment"
[[ "$(lines "$target")" == 1 ]] ||
  fail "$ROUND has no single \`INGESTER_SCALE_TARGET=<integer>\` assignment"
((target >= 2)) ||
  fail "$ROUND drives the ingester tier to $target replicas; #3647 needs at least two scraped pods"
((target > base)) ||
  fail "$ROUND: INGESTER_SCALE_TARGET=$target is not above INGESTER_SCALE_BASE=$base"

for setting in \
  '--set keda.ingester.minReplicas="$INGESTER_SCALE_BASE"' \
  '--set keda.ingester.maxReplicas="$INGESTER_SCALE_TARGET"'; do
  count=$(printf '%s\n' "$round_body" | grep -cF -- "$setting" || true)
  [[ "$count" == 1 ]] ||
    fail "$ROUND has $count \`$setting\` lines, expected exactly 1 -- a second --set silently wins"
done

# The floor goes up for the phase and comes back down. Without the second call
# the round leaves a tier pinned above its chart floor for everything after it,
# including the teardown dump a reader takes restart counts from.
contains "$round_body" 'patch_ingester_floor "$INGESTER_SCALE_TARGET"' ||
  fail "$ROUND never raises the ingester floor to \$INGESTER_SCALE_TARGET"
contains "$round_body" 'patch_ingester_floor "$INGESTER_SCALE_BASE"' ||
  fail "$ROUND never lowers the ingester floor back to \$INGESTER_SCALE_BASE"
contains "$round_body" 'capture_ingester_pod_labels "$next_event"' ||
  fail "$ROUND never runs the capture phase"
# One `time=` per query, and the same one: four answers taken at four instants
# are four unrelated numbers, and the arithmetic over them means nothing.
contains "$round_body" '--data-urlencode "time=$at"' ||
  fail "$ROUND does not pin the capture's evaluation timestamp"
capture_calls=$(printf '%s\n' "$round_body" | grep -c 'prometheus_capture "' || true)
[[ "$capture_calls" == 4 ]] ||
  fail "$ROUND makes $capture_calls retained Prometheus captures, expected the raw, per-series, per-pod and operator four"

# The verdict is deferred like the panel and outage ones: a capture that did not
# hold must not cost the round the evidence collected after it.
python3 - "$ROUND" <<'PY' || fail "$ROUND does not defer the capture verdict until after the remaining evidence"
import sys

lines = open(sys.argv[1], encoding="utf-8").read().splitlines()
call = next(i for i, line in enumerate(lines)
            if line.startswith('capture_ingester_pod_labels "$next_event"'))
if not lines[call].endswith("|| true"):
    raise SystemExit("the capture call is not deferred; a failure would exit the round early")
panel = next(i for i, line in enumerate(lines) if line == 'log "dashboard panel evidence"')
scaledobject = next(i for i, line in enumerate(lines) if line == 'log "ScaledObject evidence"')
verdict = next(i for i, line in enumerate(lines)
               if line.startswith('[[ "$INGESTER_POD_LABEL_FAILURE" -eq 0 ]]'))
if not (call < panel < scaledobject < verdict):
    raise SystemExit(
        "expected the capture, panel evidence, ScaledObject evidence and verdict in order; "
        f"got {call + 1}, {panel + 1}, {scaledobject + 1}, {verdict + 1}"
    )
PY

# --- production defaults are untouched ---------------------------------------
# The round narrows the ingester's KEDA range with --set. The chart a customer
# installs must still ship the wide one; a round-only setting that leaked into
# values.yaml would cap every deployment at two ingesters.
python3 - "$CHART_VALUES" <<'PY' || fail "$CHART_VALUES no longer ships the production ingester autoscaling defaults"
import sys

import yaml

values = yaml.safe_load(open(sys.argv[1], encoding="utf-8"))
keda = values["keda"]
expected = {"enabled": False}
for key, want in expected.items():
    if keda[key] != want:
        raise SystemExit(f"keda.{key} is {keda[key]!r}, expected {want!r}")
ingester = keda["ingester"]
for key, want in {
    "enabled": True,
    "minReplicas": 1,
    "maxReplicas": 10,
    "requestsPerSecondTarget": "800",
}.items():
    if ingester[key] != want:
        raise SystemExit(f"keda.ingester.{key} is {ingester[key]!r}, expected {want!r}")
PY

sandbox=$(mktemp -d "${TMPDIR:-/tmp}/siglake-kind-ingester-pod-labels.XXXXXX")
trap 'rm -rf -- "$sandbox"' EXIT
mkdir -p "$sandbox/bin" "$sandbox/scripts" "$sandbox/tmp" "$sandbox/results"

# The prefix of the round that defines constants and functions without running
# any of them.
sed -n "1,/^${FIRST_ROUND_LINE}\$/p" "$ROUND" | sed '$d' >"$sandbox/scripts/prelude.bash"
grep -q '^capture_ingester_pod_labels()' "$sandbox/scripts/prelude.bash" ||
  fail "the sourceable prefix of $ROUND does not define capture_ingester_pod_labels"
cp scripts/kind-common.bash "$sandbox/scripts/kind-common.bash"
cp "$GRADER" "$sandbox/scripts/"

# --- the expression, against the operator's own source -----------------------
# prom.rs's format literal with its placeholders filled and its doubled braces
# collapsed is exactly what KEDA-less operator deployments evaluate. The round
# builds the same string from $NAMESPACE and $RELEASE; both are read here rather
# than restated, so a change on either side is a failure of this guard and not
# of a round that already spent an hour.
python3 - "$PROM_SOURCE" "$sandbox/operator-expression" <<'PY' || fail "could not read the operator's ingester expression out of $PROM_SOURCE"
import pathlib
import re
import sys

source, out = sys.argv[1:]
text = pathlib.Path(source).read_text(encoding="utf-8")
matches = [
    line.strip()
    for line in text.splitlines()
    if line.strip().startswith('"avg(sum by (pod) (rate(siglake_ingest_requests_total')
    # The format string, not prom.rs's own unit-test copy with the placeholders
    # already filled in.
    and "{namespace}" in line
]
if len(matches) != 1:
    raise SystemExit(f"expected one ingester_rps format literal in {source}, found {len(matches)}")
literal = matches[0].rstrip(",").strip('"')
expression = (
    literal.replace('\\"', '"')
    .replace("{namespace}", "\x00ns\x00")
    .replace("{release}", "\x00rel\x00")
    .replace("{{", "{")
    .replace("}}", "}")
    .replace("\x00ns\x00", "default")
    .replace("\x00rel\x00", "siglake")
)
if re.search(r"[{][a-z_]+[}]", expression):
    raise SystemExit(f"unresolved format placeholder in {expression}")
pathlib.Path(out).write_text(expression + "\n", encoding="utf-8")
PY

cat >"$sandbox/scripts/print-expressions.bash" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/prelude.bash"
trap - EXIT INT TERM
ingester_operator_expression
printf '\n'
EOF
chmod +x "$sandbox/scripts/print-expressions.bash"

# Stand-ins. Every one of them records what it was asked for; none of them
# reaches a cluster, a registry or the network.
cat >"$sandbox/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
printf 'kubectl %s\n' "$*" >>"$CALLS"
case "$*" in
*"patch scaledobject/"*) exit "${KUBECTL_PATCH_RC:-0}" ;;
*"get pods"*) cat "$FIXTURE_INGESTER_PODS" ;;
esac
EOF
cat >"$sandbox/bin/git" <<'EOF'
#!/usr/bin/env bash
printf 'git %s\n' "$*" >>"$CALLS"
printf '%s\n' "0123456789abcdef0123456789abcdef01234567"
EOF
# The one stand-in with logic: it answers the Prometheus API out of the passing
# fixture, re-stamped to whatever `time=` it was given, and accepts the ingest
# posts. Anything else is a request the capture was not supposed to make.
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
*/api/v1/query)
  [ "${PROM_RC:-0}" = 0 ] || exit "$PROM_RC"
  python3 "$SANDBOX/prom-answer.py" "$query" "$at"
  ;;
*/v1/logs | */v1/traces)
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
"""Answer one Prometheus instant query from the passing fixture."""

import json
import os
import sys

query, at = sys.argv[1], float(sys.argv[2] or 0)
fixture = json.load(open(os.environ["FIXTURE_CAPTURE"], encoding="utf-8"))
queries = fixture["queries"]
if query.startswith("count("):
    print(json.dumps({
        "status": "success",
        "data": {"resultType": "vector", "result": [
            {"metric": {}, "value": [at, os.environ.get("ACTIVE_PODS", "2")]}
        ]},
    }))
    raise SystemExit(0)
for name in ("operator_expression", "per_pod_rate", "per_series_rate", "raw"):
    if queries[name]["expression"] == query:
        response = json.loads(json.dumps(queries[name]["response"]))
        for row in response["data"]["result"]:
            row["value"] = [at, row["value"][1]]
        print(json.dumps(response))
        raise SystemExit(0)
print(f"unexpected query: {query}", file=sys.stderr)
raise SystemExit(1)
EOF

python3 - "$FIXTURE" "$sandbox/ingester-pods.json" <<'PY'
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
    for row in document["revisions"]["ingester_pods"]
]
json.dump({"items": items}, open(out, "w", encoding="utf-8"))
PY

cat >"$sandbox/scripts/drive.bash" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/prelude.bash"
# The round's own cleanup would tear down a cluster this sandbox never built.
trap - EXIT INT TERM
INGESTER_POD_LABEL_SECONDS=${DRIVE_LOAD_SECONDS:-1}
INGESTER_POD_LABEL_GRACE_SECONDS=${DRIVE_GRACE_SECONDS:-5}
capture_ingester_pod_labels 900000 || true
printf 'DRIVE_FAILURE=%s\n' "$INGESTER_POD_LABEL_FAILURE"
rm -rf -- "$TMP_DIR"
EOF
chmod +x "$sandbox/scripts/drive.bash"

expression=$("$sandbox/scripts/print-expressions.bash" 2>/dev/null | head -1)
want_expression=$(head -1 "$sandbox/operator-expression")
[[ "$expression" == "$want_expression" ]] ||
  fail "$ROUND evaluates
  $expression
but $PROM_SOURCE runs
  $want_expression"

fixtures=0

# --- arm 1: the capture, end to end, against the stand-ins -------------------
calls="$sandbox/calls.log"
: >"$calls"
env -i PATH="$sandbox/bin:/usr/bin:/bin" HOME="$HOME" \
  TMPDIR="$sandbox/tmp" SANDBOX="$sandbox" CALLS="$calls" \
  RESULTS_DIR="$sandbox/results" \
  FIXTURE_CAPTURE="$PWD/$FIXTURE" FIXTURE_INGESTER_PODS="$sandbox/ingester-pods.json" \
  "$sandbox/scripts/drive.bash" >"$sandbox/drive.out" 2>&1 ||
  fail "the capture arm exited nonzero: $(<"$sandbox/drive.out")"
drive=$(<"$sandbox/drive.out")
contains "$drive" 'DRIVE_FAILURE=0' ||
  fail "the capture arm recorded a failure against a passing cluster: $drive"
contains "$drive" 'grade=verified' || fail "the capture arm did not grade verified: $drive"
call_log=$(<"$calls")
contains "$call_log" 'minReplicaCount":2' || fail "the capture never raised the ingester floor"
contains "$call_log" 'minReplicaCount":1' || fail "the capture never lowered the ingester floor"
contains "$call_log" '/v1/logs' || fail "the capture drove no log ingest"
contains "$call_log" '/v1/traces' || fail "the capture drove no trace ingest"
for produced in ingester-pod-labels.json ingester-pod-labels-raw.json \
  ingester-pod-labels-per-series.json ingester-pod-labels-per-pod.json \
  ingester-pod-labels-expression.json; do
  [[ -s "$sandbox/results/$produced" ]] || fail "the capture did not write results/$produced"
done
# Every retained answer carries its label sets, and all four name one instant.
python3 - "$sandbox/results/ingester-pod-labels.json" "$sandbox/results/ingester-pod-labels-raw.json" <<'PY' || fail "the retained capture is summarized, not raw"
import json
import sys

graded = json.load(open(sys.argv[1], encoding="utf-8"))
raw = json.load(open(sys.argv[2], encoding="utf-8"))
stamps = {query["time"] for query in graded["queries"].values()}
if len(stamps) != 1 or stamps != {graded["evaluated_at"]}:
    raise SystemExit(f"the four queries name {stamps}, not one evaluation timestamp")
series = raw["data"]["result"]
pods = {row["metric"].get("pod") for row in series}
if len(series) < 2 or len(pods) < 2 or not all(pods):
    raise SystemExit(f"the raw response kept {len(series)} series over pods {pods}")
if graded["evidence"]["grade"] != "verified":
    raise SystemExit(f"graded {graded['evidence']}")
PY
fixtures=$((fixtures + 1))

# --- arm 2: one ready ingester pod -------------------------------------------
# The capture must refuse rather than retain a one-pod reading, in which the
# fleet total and the per-pod mean are the same number.
python3 - "$sandbox/ingester-pods.json" "$sandbox/one-ingester-pod.json" <<'PY'
import json
import sys

source, out = sys.argv[1:]
document = json.load(open(source, encoding="utf-8"))
document["items"] = document["items"][:1]
json.dump(document, open(out, "w", encoding="utf-8"))
PY
: >"$calls"
env -i PATH="$sandbox/bin:/usr/bin:/bin" HOME="$HOME" \
  TMPDIR="$sandbox/tmp" SANDBOX="$sandbox" CALLS="$calls" \
  DRIVE_GRACE_SECONDS=1 RESULTS_DIR="$sandbox/results-one-pod" \
  FIXTURE_CAPTURE="$PWD/$FIXTURE" FIXTURE_INGESTER_PODS="$sandbox/one-ingester-pod.json" \
  "$sandbox/scripts/drive.bash" >"$sandbox/one-pod.out" 2>&1 ||
  fail "the one-pod arm exited nonzero: $(<"$sandbox/one-pod.out")"
one_pod=$(<"$sandbox/one-pod.out")
contains "$one_pod" 'DRIVE_FAILURE=1' ||
  fail "a one-pod tier left the capture green: $one_pod"
contains "$one_pod" 'only 1 ready ingester pods' ||
  fail "the one-pod arm failed for the wrong reason: $one_pod"
contains "$(<"$calls")" 'minReplicaCount":1' ||
  fail "the one-pod arm left the ingester floor raised"
fixtures=$((fixtures + 1))

# --- the grader, on the passing fixture and seven mutations ------------------
verified="$sandbox/verified.json"
python3 "$GRADER" "$FIXTURE" --output "$verified" 2>"$sandbox/verified.log" ||
  fail "the verified fixture did not pass: $(<"$sandbox/verified.log")"
python3 - "$verified" <<'PY' || fail "the verified fixture summary is wrong"
import json
import sys

evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
summary = evidence["summary"]
assert evidence["grade"] == "verified", evidence
assert summary["pod_count"] == 2, summary
assert summary["series_count"] == 4, summary
assert summary["active_pod_count"] == 2, summary
# The three readings the capture exists to tell apart.
assert summary["fleet_total"] == 9.0, summary
assert summary["per_pod_mean"] == 4.5, summary
assert summary["per_series_average"] == 2.25, summary
assert summary["operator_value"] == 4.5, summary
PY
fixtures=$((fixtures + 1))

# Each mutation is a plausible capture, and `want` is the reason it must be
# caught for -- so a mutation caught by the wrong check fails too.
expect_unverified() {
  local mutation=$1 want=$2
  local input="$sandbox/${mutation}.input.json"
  local output="$sandbox/${mutation}.output.json"
  local log="$sandbox/${mutation}.log" rc=0
  python3 - "$FIXTURE" "$input" "$mutation" <<'PY'
import json
import sys

source, destination, mutation = sys.argv[1:]
document = json.load(open(source, encoding="utf-8"))
queries = document["queries"]


def rows(name):
    return queries[name]["response"]["data"]["result"]


def value(row):
    return float(row["value"][1])


if mutation == "missing-pod-label":
    del rows("raw")[1]["metric"]["pod"]
elif mutation == "empty-pod-label":
    rows("per_series_rate")[2]["metric"]["pod"] = ""
elif mutation == "collapsed-group":
    # What `sum by (pod)` returns when the label is gone: one group, holding
    # the fleet total, with no pod on it.
    total = sum(value(row) for row in rows("per_pod_rate"))
    queries["per_pod_rate"]["response"]["data"]["result"] = [
        {"metric": {}, "value": [document["evaluated_at"], str(total)]}
    ]
elif mutation == "single-active-pod":
    quiet = rows("per_pod_rate")[1]["metric"]["pod"]
    for row in rows("per_series_rate"):
        if row["metric"].get("pod") == quiet:
            row["value"][1] = "0"
    rows("per_pod_rate")[1]["value"][1] = "0"
    mean = sum(value(row) for row in rows("per_pod_rate")) / len(rows("per_pod_rate"))
    rows("operator_expression")[0]["value"][1] = str(mean)
elif mutation == "arithmetic-mismatch":
    # The pre-#3620 reading: the fleet total where the per-pod mean belongs.
    rows("operator_expression")[0]["value"][1] = str(
        sum(value(row) for row in rows("per_pod_rate"))
    )
elif mutation == "mixed-timestamps":
    queries["operator_expression"]["time"] = document["evaluated_at"] + 30
    rows("operator_expression")[0]["value"][0] = document["evaluated_at"] + 30
elif mutation == "single-series-per-pod":
    for name in ("raw", "per_series_rate"):
        queries[name]["response"]["data"]["result"] = [
            row for row in rows(name) if row["metric"].get("endpoint") != "otlp_traces"
        ]
    totals = {}
    for row in rows("per_series_rate"):
        totals[row["metric"]["pod"]] = totals.get(row["metric"]["pod"], 0.0) + value(row)
    for row in rows("per_pod_rate"):
        row["value"][1] = str(totals[row["metric"]["pod"]])
    rows("operator_expression")[0]["value"][1] = str(sum(totals.values()) / len(totals))
elif mutation == "expression-drift":
    queries["operator_expression"]["expression"] = queries["per_pod_rate"]["expression"]
else:
    raise SystemExit(f"unknown mutation {mutation}")
json.dump(document, open(destination, "w", encoding="utf-8"))
PY
  python3 "$GRADER" "$input" --output "$output" 2>"$log" || rc=$?
  [[ "$rc" -eq 1 ]] || fail "$mutation exited $rc, expected the unverified exit 1"
  local grade problem
  grade=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["evidence"]["grade"])' "$output")
  [[ "$grade" == unverified ]] || fail "$mutation was not graded unverified"
  problem=$(python3 -c 'import json,sys; print("\n".join(json.load(open(sys.argv[1]))["evidence"]["problems"]))' "$output")
  contains "$problem" "$want" || fail "$mutation was caught for the wrong reason: $problem"
  fixtures=$((fixtures + 1))
}

expect_unverified missing-pod-label 'raw: series 1 carries no pod label'
expect_unverified empty-pod-label 'per_series_rate: series 2 carries an empty pod label'
expect_unverified collapsed-group 'per_pod_rate: series 0 carries no pod label'
expect_unverified single-active-pod 'carried a nonzero request rate at the evaluation timestamp, not two or more'
expect_unverified arithmetic-mismatch 'the mean of the per-pod sums is'
expect_unverified mixed-timestamps "not at the capture's"
expect_unverified single-series-per-pod 'every pod published one series'
expect_unverified expression-drift 'not the one this capture claims to have evaluated'

echo "ok ($fixtures offline ingester per-pod label fixtures; the live capture needs a kind round)"
