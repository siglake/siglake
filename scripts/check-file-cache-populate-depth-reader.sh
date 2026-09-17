#!/usr/bin/env bash
# Offline fixtures for the decoded-file-cache population-depth reader (#4890).
# It runs from exposition snapshots on disk, so it needs neither a cluster nor
# a compiled binary. The arms are the readings the 0.2.0 cache decision turns
# on, plus the malformed inputs that must be refused rather than reported.

set -euo pipefail

cd "$(dirname "$0")/.."

READER=scripts/read-file-cache-populate-depth.py
FIXTURES=scripts/testdata/file-cache-populate-depth

fail() { echo "FAIL $*" >&2; exit 1; }
contains() { case "$1" in *"$2"*) ;; *) return 1 ;; esac; }

[[ -x "$READER" ]] || fail "$READER is missing or not executable"
[[ -d "$FIXTURES" ]] || fail "$FIXTURES does not exist"

work=$(mktemp -d "${TMPDIR:-/tmp}/siglake-populate-depth.XXXXXX")
trap 'rm -rf -- "$work"' EXIT

# Arm 1: a shape whose every task bypasses population (a converted predicate,
# #4891). No samples must read as INELIGIBLE, never as zero decode depth.
out=$(python3 "$READER" --shape label_filter="$FIXTURES/bypassed-shape.txt") ||
  fail "the bypassed shape did not read"
contains "$out" "This shape is INELIGIBLE, not zero-depth" ||
  fail "a fully bypassed shape must not read as zero decode depth: $out"
contains "$out" "192 of 192 cache requests bypassed population" ||
  fail "the bypassed shape must report its denominator: $out"

# Arm 2: predicate-free clipped browses, every one far below the floor — the
# regime in which row-group population buys a footer read and nothing else.
python3 "$READER" --shape browse="$FIXTURES/clipped-below-floor.txt" \
  --output "$work/browse.json" >"$work/browse.txt" ||
  fail "the below-floor shape did not read"
python3 - "$work/browse.json" <<'PY' || fail "below-floor report is wrong"
import json, sys
shape = json.load(open(sys.argv[1], encoding="utf-8"))["shapes"][0]
clipped = shape["outcomes"]["clipped"]
assert clipped["samples"] == 16, clipped
assert clipped["below_floor"] == 16, clipped
assert clipped["at_or_above_floor"] == 0, clipped
assert shape["qualifying_at_or_above_floor"] == 0, shape
# Unpolled tasks are present and counted, and stay out of the fraction.
assert shape["outcomes"]["unpolled"]["samples"] == 2, shape
assert shape["qualifying_samples"] == 16, shape
assert any("never polled" in note for note in shape["notes"]), shape
assert "No footer geometry supplied" in shape["statement"], shape
PY
contains "$(cat "$work/browse.txt")" "16 below floor, 0 at or above" ||
  fail "the below-floor table must show the split: $(cat "$work/browse.txt")"

# Arm 3: a delta against a baseline from the same process, with recorded footer
# geometry. Errors and unpolled tasks stay out of the fraction, and geometry
# larger than the floor must downgrade the claim rather than assert completion.
python3 "$READER" --shape deep="$FIXTURES/deep-browse-after.txt" \
  --baseline deep="$FIXTURES/deep-browse-before.txt" \
  --geometry deep="$FIXTURES/deep-browse-geometry.json" \
  --stats deep="$FIXTURES/deep-browse-stats.json" \
  --output "$work/deep.json" >/dev/null ||
  fail "the deep shape did not read"
python3 - "$work/deep.json" <<'PY' || fail "deep-browse report is wrong"
import json, sys
shape = json.load(open(sys.argv[1], encoding="utf-8"))["shapes"][0]
outcomes = shape["outcomes"]
# The baseline's five clipped samples and its completed one are subtracted.
assert outcomes["clipped"]["samples"] == 6, outcomes["clipped"]
assert outcomes["clipped"]["at_or_above_floor"] == 4, outcomes["clipped"]
assert outcomes["completed"]["samples"] == 1, outcomes["completed"]
assert outcomes["error"]["samples"] == 1, outcomes["error"]
assert outcomes["unpolled"]["samples"] == 1, outcomes["unpolled"]
# The exact threshold: a sample of exactly 131,072 rows qualifies.
assert shape["qualifying_samples"] == 7, shape
assert shape["qualifying_at_or_above_floor"] == 5, shape
assert shape["row_group_floor_rows"] == 131072, shape
# Counter deltas, not process totals.
assert shape["requests"]["miss"] == 9, shape["requests"]
assert shape["requests"]["bypass"] == 4, shape["requests"]
assert shape["per_request_stats"]["file_cache_populate_rows"] == 2629648, shape
assert "upper bound" in shape["statement"], shape["statement"]
assert "900000-1048576 rows per group" in shape["statement"], shape["statement"]
PY

# Arm 4: refusals. Each must exit 2 with a reason, not report a number.
refuse() {
  local why=$1
  shift
  local log="$work/refuse.log" rc=0
  python3 "$READER" "$@" >"$work/refuse.out" 2>"$log" || rc=$?
  [[ $rc -eq 2 ]] || fail "$why: expected exit 2, got $rc"
  contains "$(cat "$log")" "FAIL" || fail "$why: no reason printed: $(cat "$log")"
}

# A recorder built without the bucket layout renders the histogram as a
# summary; quantiles cannot answer the floor question.
refuse "summary-form export" --shape s="$FIXTURES/summary-form.txt"
contains "$(cat "$work/refuse.log")" "exposed as a summary" ||
  fail "the summary refusal must name its cause: $(cat "$work/refuse.log")"

# An export re-bucketed without the floor edge cannot be read exactly.
refuse "missing floor edge" --shape s="$FIXTURES/missing-floor-edge.txt"
contains "$(cat "$work/refuse.log")" "131071" ||
  fail "the missing-edge refusal must name the edge: $(cat "$work/refuse.log")"

# An outcome label the populate path does not record means the reader and the
# binary disagree; refuse rather than silently drop the samples.
refuse "unknown outcome label" --shape s="$FIXTURES/unknown-outcome.txt"
contains "$(cat "$work/refuse.log")" "does not record" ||
  fail "the unknown-outcome refusal must name its cause: $(cat "$work/refuse.log")"

# A baseline from a restarted process: counts go backwards.
refuse "baseline after the snapshot" \
  --shape s="$FIXTURES/deep-browse-before.txt" \
  --baseline s="$FIXTURES/deep-browse-after.txt"
contains "$(cat "$work/refuse.log")" "went backwards" ||
  fail "the backwards-baseline refusal must name its cause: $(cat "$work/refuse.log")"

refuse "baseline naming an unknown shape" \
  --shape s="$FIXTURES/clipped-below-floor.txt" \
  --baseline other="$FIXTURES/clipped-below-floor.txt"

echo "ok check-file-cache-populate-depth-reader"
