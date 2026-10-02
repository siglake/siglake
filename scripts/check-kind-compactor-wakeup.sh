#!/usr/bin/env bash
# Offline guard for the kind round's zero-replica compactor wake-up capture
# (#6011 prepares it; #6012 runs it and retains the acceptance).
#
# The live arm needs an operator-managed cluster, so nothing here proves a
# wake-up. What it does prove is that the arm cannot produce plausible-looking
# evidence for something else: it is off by default, it refuses to share a
# round with the captures that move the same tier, the expression it evaluates
# is character for character the one the reconciler runs, and the grader
# refuses every way the capture can be hollow.

set -euo pipefail

cd "$(dirname "$0")/.."

ROUND=scripts/kind-round.sh
GRADER=scripts/grade-kind-compactor-wakeup.py
FIXTURE=scripts/testdata/kind-compactor-wakeup-verified.json
PROM_SOURCE=crates/siglake-operator/src/prom.rs
PROM_TEST_MARKER='fn the_activation_expression_is_the_one_the_kind_capture_evaluates'

fail() { echo "FAIL $*" >&2; exit 1; }
contains() { case "$1" in *"$2"*) ;; *) return 1 ;; esac; }

for file in "$ROUND" "$GRADER" "$FIXTURE" "$PROM_SOURCE"; do
  [[ -f "$file" ]] || fail "$file does not exist"
done
bash -n "$ROUND"
python3 -m py_compile "$GRADER"

round_body=$(grep -vE '^[[:space:]]*(#|$)' "$ROUND")
contains "$round_body" 'COMPACTOR_WAKEUP_CAPTURE="${COMPACTOR_WAKEUP_CAPTURE:-0}"' ||
  fail "$ROUND does not default COMPACTOR_WAKEUP_CAPTURE off"
contains "$round_body" '[[ "$COMPACTOR_WAKEUP_CAPTURE" == 0 || "$COMPACTOR_WAKEUP_CAPTURE" == 1 ]]' ||
  fail "$ROUND does not constrain COMPACTOR_WAKEUP_CAPTURE to 0 or 1"
contains "$round_body" '[[ -n "$COMPACTOR_WAKEUP_CLUSTER" ]]' ||
  fail "$ROUND does not require the SiglakeCluster the capture watches to be named"
# The arm reads an operator-managed cluster. Every other opt-in either moves
# the same compactor tier or interrupts the ingest that has to wake it.
for incompatible in COMPACTOR_POD_LABEL_CAPTURE INGESTER_POD_LABEL_CAPTURE \
  POSTGRES_OUTAGE_PROBE SCHEMA_ROLLBACK_PROBE; do
  contains "$round_body" "$incompatible" ||
    fail "$ROUND does not name $incompatible among the wake-up capture's incompatibilities"
done
python3 - "$ROUND" <<'PY' || fail "the wake-up capture's incompatibility list is not enforced"
import re
import sys

source = open(sys.argv[1], encoding="utf-8").read()
block = re.search(
    r'if \[\[ "\$COMPACTOR_WAKEUP_CAPTURE" == 1 \]\]; then(.*?)\nfi\n',
    source,
    re.DOTALL,
)
if not block:
    raise SystemExit("no COMPACTOR_WAKEUP_CAPTURE=1 validation block")
body = block.group(1)
for name in (
    "COMPACTOR_POD_LABEL_CAPTURE",
    "INGESTER_POD_LABEL_CAPTURE",
    "POSTGRES_OUTAGE_PROBE",
    "SCHEMA_ROLLBACK_PROBE",
):
    if name not in body:
        raise SystemExit(f"{name} is not refused alongside the wake-up capture")
if "exit 1" not in body:
    raise SystemExit("the validation block does not exit nonzero")
# And the reverse direction: the mirror-reclaim arm refuses this capture too.
if "COMPACTOR_WAKEUP_CAPTURE" not in re.search(
    r"case \"\$MIRROR_RECLAIM_ARM\" in(.*?)\nesac\n", source, re.DOTALL
).group(1):
    raise SystemExit("the mirror-reclaim arm does not refuse the wake-up capture")
PY

# The phases the grader reads have to be the ones the round writes, or a
# capture that never parked would grade on phases nobody recorded.
for phase in before-park parking parked ingested waking woken; do
  contains "$round_body" "compactor_wakeup_sample $phase" ||
    fail "$ROUND does not record the $phase phase"
done
contains "$round_body" 'grade-kind-compactor-wakeup.py' ||
  fail "$ROUND does not grade the capture"
contains "$round_body" 'compactor_wakeup_signal_is_positive' ||
  fail "$ROUND does not wait for the publisher and scrape before retaining the signal"

python3 - "$ROUND" <<'PY' || fail "the opt-in phase is not last or its verdict is not deferred"
import sys

lines = open(sys.argv[1], encoding="utf-8").read().splitlines()
guard = [i for i, line in enumerate(lines)
         if line == 'if [[ "$COMPACTOR_WAKEUP_CAPTURE" == 1 ]]; then']
if len(guard) != 2:  # the validation block and the capture block
    raise SystemExit(f"expected two opt-in blocks (validate, run), found {guard}")
call = next(i for i, line in enumerate(lines)
            if line.strip().startswith('capture_compactor_wakeup "$next_event"'))
if not lines[call].endswith("|| true"):
    raise SystemExit("capture failure is not deferred")
pod_labels = next(i for i, line in enumerate(lines)
                  if line.strip().startswith('capture_compactor_pod_labels "$next_event"'))
verdict = next(i for i, line in enumerate(lines)
               if line.startswith('[[ "$COMPACTOR_WAKEUP_FAILURE" -eq 0 ]]'))
if not pod_labels < call < verdict:
    raise SystemExit(f"expected the ordinary captures, then this one, then the verdict; "
                     f"got {pod_labels}, {call}, {verdict}")
PY

python3 - "$ROUND" <<'PY' || fail "the wake-up capture does not retain a zero control then wait for a positive signal"
import sys

lines = open(sys.argv[1], encoding="utf-8").read().splitlines()
baseline = next(i for i, line in enumerate(lines)
                if '"$COMPACTOR_WAKEUP_PARKED_EXPRESSION_JSON" ||' in line)
ingest = next(i for i, line in enumerate(lines)
              if 'ingest_events "$next" "$COMPACTOR_WAKEUP_BATCH"' in line)
positive = next(i for i, line in enumerate(lines)
                if line.strip().startswith("if compactor_wakeup_signal_is_positive"))
sample = next(i for i, line in enumerate(lines)
              if line.strip() == "compactor_wakeup_sample ingested")
wake = next(i for i, line in enumerate(lines)
            if 'log "wait for the operator to bring the compactor back' in line)
if not baseline < ingest < positive < sample < wake:
    raise SystemExit(
        f"expected parked control, ingest, positive-signal wait, retained sample, wake; "
        f"got {baseline}, {ingest}, {positive}, {sample}, {wake}"
    )
PY

# --- the expression the capture evaluates ----------------------------------
#
# Pinned to the operator source through the unit test that asserts the exact
# string for one (release, namespace), so a change to
# `Queries::compactor_activation` that this script does not follow fails here
# rather than producing evidence for a query the reconciler does not run.
sandbox=$(mktemp -d "${TMPDIR:-/tmp}/siglake-kind-compactor-wakeup.XXXXXX")
trap 'rm -rf -- "$sandbox"' EXIT
mkdir -p "$sandbox/scripts" "$sandbox/tmp"

python3 - "$PROM_SOURCE" "$PROM_TEST_MARKER" "$sandbox/operator-expression" <<'PY'
import pathlib
import sys

source, marker, out = sys.argv[1:]
lines = pathlib.Path(source).read_text(encoding="utf-8").splitlines()
start = next(i for i, line in enumerate(lines) if marker in line)
literals = [
    line.strip().rstrip(",").strip('"')
    for line in lines[start : start + 40]
    if line.strip().startswith('"avg(sum by (pod) (siglake_wal_segments_sealed')
]
if len(literals) != 1:
    raise SystemExit(f"expected one expected-expression literal under {marker}, found {len(literals)}")
pathlib.Path(out).write_text(literals[0].replace('\\"', '"') + "\n", encoding="utf-8")
PY

FIRST_ROUND_LINE='log "bring up the base kind deployment"'
sed -n "1,/^${FIRST_ROUND_LINE}\$/p" "$ROUND" | sed '$d' >"$sandbox/scripts/prelude.bash"
cp scripts/kind-common.bash "$sandbox/scripts/kind-common.bash"
grep -q '^capture_compactor_wakeup()' "$sandbox/scripts/prelude.bash" ||
  fail "the sourceable prefix does not define the wake-up capture"

cat >"$sandbox/scripts/print-expression.bash" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/prelude.bash"
trap - EXIT INT TERM
COMPACTOR_WAKEUP_CLUSTER=siglake
NAMESPACE=default
compactor_wakeup_operator_expression
printf '\n'
rm -rf -- "$TMP_DIR"
EOF
chmod +x "$sandbox/scripts/print-expression.bash"
expression=$(TMPDIR="$sandbox/tmp" "$sandbox/scripts/print-expression.bash" 2>/dev/null)
[[ "$expression" == "$(<"$sandbox/operator-expression")" ]] || {
  printf 'round:    %s\noperator: %s\n' "$expression" "$(<"$sandbox/operator-expression")" >&2
  fail "the round's activation expression differs from $PROM_SOURCE"
}
# The fixture is graded against the same string, so a fixture written for an
# older expression cannot keep passing.
python3 - "$FIXTURE" "$sandbox/operator-expression" <<'PY' || fail "the fixture evaluates another expression"
import json
import pathlib
import sys

fixture, expected = sys.argv[1:]
document = json.loads(pathlib.Path(fixture).read_text(encoding="utf-8"))
want = pathlib.Path(expected).read_text(encoding="utf-8").strip()
for name in ("parked_operator_expression", "operator_expression"):
    got = document["queries"][name]["expression"]
    if got != want:
        raise SystemExit(
            f"fixture {name} expression\n  {got}\ndiffers from the operator's\n  {want}"
        )
PY

# --- the grader, on a verified capture and on every hollow one --------------
verified="$sandbox/verified.json"
python3 "$GRADER" "$FIXTURE" --output "$verified" 2>"$sandbox/verified.log" ||
  fail "the verified fixture failed: $(<"$sandbox/verified.log")"
python3 - "$verified" <<'PY'
import json
import sys

evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
assert evidence["grade"] == "verified", evidence
assert evidence["summary"]["parked_replicas"] == 0, evidence
assert evidence["summary"]["parked_operator_value"] == 0, evidence
assert evidence["summary"]["woken_replicas"] == 1, evidence
assert evidence["summary"]["published_depth"] == 6.0, evidence
assert evidence["summary"]["operator_value"] == 6.0, evidence
assert evidence["summary"]["fresh_publishers"] == 2, evidence
PY

fixtures=1
expect_unverified() {
  local mutation=$1 want=$2 input="$sandbox/$1.input.json" output="$sandbox/$1.output.json"
  python3 - "$FIXTURE" "$input" "$mutation" <<'PY'
import json
import sys

source, out, mutation = sys.argv[1:]
document = json.load(open(source, encoding="utf-8"))
history = document["replica_history"]
depth = document["queries"]["published_depth"]["response"]["data"]["result"]
age = document["queries"]["sample_age"]["response"]["data"]["result"]
operator = document["queries"]["operator_expression"]["response"]["data"]["result"]
parked_operator = document["queries"]["parked_operator_expression"]["response"]["data"]["result"]
if mutation == "pod-still-terminating":
    history[2]["pods"] = ["siglake-compactor-6d4c9b9f8c-h2xq7"]
elif mutation == "never-parked":
    history[2]["replicas"] = 1
elif mutation == "never-woken":
    history[5]["replicas"] = 0
elif mutation == "ingest-after-the-wake":
    history[3], history[5] = history[5], history[3]
elif mutation == "compactor-running-during-ingest":
    history[3]["pods"] = ["siglake-compactor-6d4c9b9f8c-nm4bd"]
elif mutation == "every-publisher-stale":
    for row in age:
        row["value"][1] = "600"
elif mutation == "fresh-but-empty-queue":
    for row in depth:
        row["value"][1] = "0"
    operator[0]["value"][1] = "0"
elif mutation == "missing-pod-label":
    depth[0]["metric"].pop("pod")
elif mutation == "operator-reads-nothing":
    document["queries"]["operator_expression"]["response"]["data"]["result"] = []
elif mutation == "operator-disagrees":
    operator[0]["value"][1] = "3"
elif mutation == "unpinned-commit":
    document["revisions"]["repository_commit_source"] = "unknown"
elif mutation == "no-ingest":
    document["settings"]["ingest_batch"] = 0
elif mutation == "baseline-already-positive":
    parked_operator[0]["value"][1] = "6"
else:
    raise SystemExit(mutation)
json.dump(document, open(out, "w", encoding="utf-8"), indent=2)
PY
  if python3 "$GRADER" "$input" --output "$output" 2>"$sandbox/$mutation.log"; then
    fail "$mutation graded verified"
  fi
  grep -Fq "$want" "$sandbox/$mutation.log" ||
    fail "$mutation failed for the wrong reason: $(<"$sandbox/$mutation.log")"
  python3 - "$output" <<'PY' || fail "$mutation wrote no unverified evidence"
import json
import sys

evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
assert evidence["grade"] == "unverified", evidence
assert evidence["reason"], evidence
PY
  fixtures=$((fixtures + 1))
}

expect_unverified pod-still-terminating 'still existed while the tier read parked'
expect_unverified never-parked 'at the parked phase were 1, not 0'
expect_unverified never-woken 'the tier did not come back'
expect_unverified ingest-after-the-wake 'the phases are out of order'
expect_unverified compactor-running-during-ingest 'did not happen with the tier parked'
expect_unverified every-publisher-stale 'frozen gauge, not a reading'
expect_unverified fresh-but-empty-queue 'not a reason to start a worker'
expect_unverified missing-pod-label 'carries no nonempty pod label'
expect_unverified operator-reads-nothing 'returned 0 series, not one scalar'
expect_unverified operator-disagrees 'not reading the depth this capture saw'
expect_unverified unpinned-commit "provenance is unknown"
expect_unverified no-ingest 'drove no ingest while the tier was parked'
expect_unverified baseline-already-positive 'already read 6 before ingest'

echo "ok ($fixtures offline compactor wake-up fixtures; the live wake-up needs an operator-managed kind round, #6012)"
