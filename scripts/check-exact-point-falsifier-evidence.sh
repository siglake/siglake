#!/usr/bin/env bash
# Offline fixtures for the exact-point falsifier grader. No cluster or AWS.

set -euo pipefail

cd "$(dirname "$0")/.."

GRADER=scripts/grade-exact-point-falsifiers.py
FIXTURE=scripts/testdata/exact-point-falsifiers-complete

fail() { echo "FAIL $*" >&2; exit 1; }
contains() { case "$1" in *"$2"*) ;; *) return 1 ;; esac; }

[[ -x "$GRADER" ]] || fail "$GRADER is missing or not executable"
[[ -d "$FIXTURE" ]] || fail "$FIXTURE does not exist"

fixture_dir=$(mktemp -d "${TMPDIR:-/tmp}/siglake-exact-point-evidence.XXXXXX")
trap 'rm -rf -- "$fixture_dir"' EXIT

grade_dir() {
  local directory=$1 output=$2 log=$3
  python3 "$GRADER" "$directory" --output "$output" 2>"$log"
}

complete_output="$fixture_dir/complete.json"
grade_dir "$FIXTURE" "$complete_output" "$fixture_dir/complete.log" ||
  fail "complete fixture did not verify: $(cat "$fixture_dir/complete.log")"
python3 - "$complete_output" <<'PY' || fail "complete fixture summary is wrong"
import json, sys
evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
assert evidence["grade"] == "verified"
assert evidence["evidence_kind"] == "synthetic"
assert evidence["hypothesis_outcome"] == "passed"
assert evidence["falsifiers"] == {"f1": "passed", "f2": "passed", "f3": "passed", "f4": "passed"}
PY

fixtures=1

mutate_fixture() {
  local name=$1
  local destination="$fixture_dir/$name"
  cp -R "$FIXTURE" "$destination"
  python3 - "$destination" "$name" <<'PY'
import json, pathlib, sys
directory = pathlib.Path(sys.argv[1])
mutation = sys.argv[2]

def update(name, change):
    path = directory / name
    document = json.loads(path.read_text(encoding="utf-8"))
    change(document)
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")

if mutation == "complete-falsification":
    def falsify(document):
        document["encoded_bytes"] = 800000
        document["bytes_per_point"] = 800000 / document["points"]
        document["verdict"] = "falsified"
    update("encoding.json", falsify)
elif mutation == "missing-candidate-arm":
    update("boundary-arms.json", lambda document: document.pop("candidate"))
elif mutation == "truncated-export":
    update("export.json", lambda document: document.update(truncated=True, status="truncated"))
elif mutation == "inconsistent-revision":
    update("encoding.json", lambda document: document["revisions"].update(repository_commit="other-revision"))
else:
    raise SystemExit(f"unknown mutation {mutation}")
PY
  printf '%s\n' "$destination"
}

falsified_dir=$(mutate_fixture complete-falsification)
falsified_output="$fixture_dir/complete-falsification.json"
grade_dir "$falsified_dir" "$falsified_output" "$fixture_dir/complete-falsification.log" ||
  fail "complete falsification was not verified: $(cat "$fixture_dir/complete-falsification.log")"
python3 - "$falsified_output" <<'PY' || fail "complete falsification summary is wrong"
import json, sys
evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
assert evidence["grade"] == "verified"
assert evidence["hypothesis_outcome"] == "falsified"
assert evidence["falsifiers"]["f2"] == "falsified"
PY
fixtures=$((fixtures + 1))

expect_unverified() {
  local mutation=$1 want=$2
  local directory output log rc=0
  directory=$(mutate_fixture "$mutation")
  output="$fixture_dir/$mutation.json"
  log="$fixture_dir/$mutation.log"
  grade_dir "$directory" "$output" "$log" || rc=$?
  [[ "$rc" -eq 1 ]] || fail "$mutation exited $rc, expected unverified exit 1"
  local grade problems
  grade=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["evidence"]["grade"])' "$output")
  [[ "$grade" == unverified ]] || fail "$mutation was not graded unverified"
  problems=$(python3 -c 'import json,sys; print("\n".join(json.load(open(sys.argv[1]))["evidence"]["problems"]))' "$output")
  contains "$problems" "$want" || fail "$mutation was caught for the wrong reason: $problems"
  fixtures=$((fixtures + 1))
}

expect_unverified missing-candidate-arm 'missing the candidate arm'
expect_unverified truncated-export 'truncated or unchecked export'
expect_unverified inconsistent-revision 'repository_commit is inconsistent'

echo "ok ($fixtures synthetic exact-point evidence fixtures; live collection is outside this guard)"
