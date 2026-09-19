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
  '--set wal.mirror.enabled=true' \
  '--set compactor.catalogClaim.enabled=true' \
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

echo "ok ($fixtures offline compactor shared-queue fixtures; live install needs a kind round)"
