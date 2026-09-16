#!/usr/bin/env bash
# Verify that scripts/kind-round.sh installs the query tier with KEDA headroom
# above the floor it then drives, and that the scale evidence file is named from
# the same two constants.
#
# The failure this guards is the one task #968 spent six rounds on: with
# `keda.query.maxReplicas` installed at the BASE replica count, the scale step
# still runs -- `advance_query_scale` patches the ScaledObject's
# `minReplicaCount` up to the target, KEDA clamps the tier to its maximum, the
# target is never reached, and the step fails on its grace timeout. The round
# then reports a transition that timed out, which sends the next one looking at
# discovery, readiness and load rather than at the one `--set` that made the
# target unreachable.
#
# The second half is the same shape one step later: the evidence the manager's
# read_results run greps for is `results/scale-<base>-<target>-<base>.json`,
# which the script DERIVES from the constants. Pinning the derivation and the
# documented name to those constants means moving them cannot leave the round
# writing a file under a name nobody looks for.
#
# Static: a few file reads, no cluster, no kubectl, no helm.

set -euo pipefail

cd "$(dirname "$0")/.."

SELF=scripts/check-kind-round-scale.sh
ROUND_SCRIPT=scripts/kind-round.sh
# The shipping documents that quote the evidence file by name, relative to the
# doc root passed to check_round_script.
DOC_FILES=(README.md deploy/kind/README.md)

# `scale-<n>-<n>-<n>.json` anywhere in the tree. Every occurrence must be the
# name the constants produce. This file is exempt: like scripts/check-public-
# tree.py it necessarily contains the pattern it searches for.
NAME_PATTERN='scale-[0-9]+-[0-9]+-[0-9]+\.json'

fail() {
  echo "FAIL $*" >&2
  exit 1
}

lines() { printf '%s' "$1" | grep -c . || true; }

# Does $1 contain the fixed string $2? Deliberately not `printf | grep -qF`:
# `grep -q` exits at its first match, so the write into the pipe can lose the
# race and die of SIGPIPE, which `pipefail` reports as a missing line. That made
# this guard red on a tree it should pass in ~7% of runs, on a fixture chosen by
# the scheduler -- including the pristine copy.
contains() { case "$1" in *"$2"*) ;; *) return 1 ;; esac; }

# Files naming a scale evidence file. $1 = doc root: `.` is the tracked tree
# (the real check), anything else a fixture copy of the documents.
name_bearing_files() {
  if [ "$1" = "." ]; then
    git grep -lE "$NAME_PATTERN" -- . || true
  else
    grep -rlE "$NAME_PATTERN" "$1" || true
  fi
}

# $1 = kind-round.sh (the real one, or a mutated copy), $2 = doc root. Prints
# nothing on success; the first problem is fatal, so a message names one thing
# to fix.
check_round_script() {
  local script=$1 docroot=$2
  local body base target min_sets max_sets json_line literal rel stale doc
  [ -f "$script" ] || fail "$script does not exist"

  # Full-line comments and blanks removed: the round's prose explains the very
  # `--set` lines this reads, and a commented-out line is not a line it runs.
  body=$(grep -vE '^[[:space:]]*(#|$)' "$script" || true)
  [ -n "$body" ] || fail "$script is empty once comments are stripped"

  # --- the constants -------------------------------------------------------
  base=$(printf '%s\n' "$body" | sed -n 's/^QUERY_SCALE_BASE=\([0-9][0-9]*\)$/\1/p')
  target=$(printf '%s\n' "$body" | sed -n 's/^QUERY_SCALE_TARGET=\([0-9][0-9]*\)$/\1/p')
  [ "$(lines "$base")" = 1 ] ||
    fail "$script has no single \`QUERY_SCALE_BASE=<integer>\` assignment"
  [ "$(lines "$target")" = 1 ] ||
    fail "$script has no single \`QUERY_SCALE_TARGET=<integer>\` assignment"
  ((target > base)) ||
    fail "$script: QUERY_SCALE_TARGET=$target is not above QUERY_SCALE_BASE=$base -- the scale step would drive the query tier nowhere"

  # --- the installed KEDA range --------------------------------------------
  # Exactly one `--set` per bound, expressed with the constants: a second one
  # later in the same `helm upgrade` silently wins, and a literal drifts from
  # the floor the scale step patches.
  min_sets=$(printf '%s\n' "$body" | grep -cF -- '--set keda.query.minReplicas' || true)
  max_sets=$(printf '%s\n' "$body" | grep -cF -- '--set keda.query.maxReplicas' || true)
  [ "$min_sets" = 1 ] ||
    fail "$script sets keda.query.minReplicas on $min_sets lines, expected exactly 1"
  [ "$max_sets" = 1 ] ||
    fail "$script sets keda.query.maxReplicas on $max_sets lines, expected exactly 1"

  contains "$body" '--set keda.query.maxReplicas="$QUERY_SCALE_TARGET"' ||
    fail "$script does not install keda.query.maxReplicas from \$QUERY_SCALE_TARGET -- KEDA would clamp the tier below the target and the scale step would fail on its grace timeout instead of naming the range"
  contains "$body" '--set keda.query.minReplicas="$QUERY_SCALE_BASE"' ||
    fail "$script does not install keda.query.minReplicas from \$QUERY_SCALE_BASE"
  contains "$body" '--set query.replicas="$QUERY_SCALE_BASE"' ||
    fail "$script does not install query.replicas from \$QUERY_SCALE_BASE"

  # --- the evidence file name ----------------------------------------------
  json_line=$(printf '%s\n' "$body" | grep -E '^SCALE_JSON=' || true)
  [ "$(lines "$json_line")" = 1 ] ||
    fail "$script has no single \`SCALE_JSON=\` assignment"
  case "$json_line" in
  *'scale-${QUERY_SCALE_BASE}-${QUERY_SCALE_TARGET}-${QUERY_SCALE_BASE}.json'*) ;;
  *) fail "$script does not derive SCALE_JSON from the scale constants: $json_line" ;;
  esac

  # The name a read_results run greps for. Every occurrence in the tree must be
  # the one these constants produce -- a stale name in a document is a reader
  # looking for a file the round never wrote.
  literal="scale-${base}-${target}-${base}.json"
  while IFS= read -r rel; do
    [ -n "$rel" ] || continue
    [ "$rel" != "$SELF" ] || continue
    stale=$(grep -ohE "$NAME_PATTERN" "$rel" | grep -vxF "$literal" | head -1 || true)
    [ -z "$stale" ] ||
      fail "$rel names \`$stale\`, but QUERY_SCALE_BASE=$base / QUERY_SCALE_TARGET=$target make the round write \`$literal\`"
  done < <(name_bearing_files "$docroot")

  for doc in "${DOC_FILES[@]}"; do
    grep -qF "$literal" "$docroot/$doc" ||
      fail "$docroot/$doc does not name \`$literal\` -- the file the round writes and the document that points a reader at it have drifted"
  done
}

check_round_script "$ROUND_SCRIPT" .

# --- fixtures ----------------------------------------------------------------
# A guard whose failure mode is a green run is worse than no guard, so every
# assertion above is shown going red on a mutated copy of the real files. Each
# mutation is a plausible edit, and `want` is a substring the message must
# carry, so a mutation caught by the WRONG assertion fails too.
fixture_dir=$(mktemp -d "${TMPDIR:-/tmp}/siglake-kind-round-scale.XXXXXX")
trap 'rm -rf -- "$fixture_dir"' EXIT

fixtures=0

# An unmutated copy passes, so what the mutations below prove is the mutation
# and not the copying.
cp "$ROUND_SCRIPT" "$fixture_dir/pristine.sh"
check_round_script "$fixture_dir/pristine.sh" .

# $1 = case name, $2 = substring the failure message must contain, $3 = doc
# root, $4.. = `sed` scripts applied to a copy of scripts/kind-round.sh. With no
# `sed` script the script is copied verbatim and the mutation is the doc root.
mutate() {
  local name=$1 want=$2 docroot=$3
  shift 3
  local copy="$fixture_dir/$name.sh" out rc=0 s
  if [ "$#" -eq 0 ]; then
    cp "$ROUND_SCRIPT" "$copy"
  else
    local seds=()
    for s in "$@"; do seds+=(-e "$s"); done
    sed "${seds[@]}" "$ROUND_SCRIPT" >"$copy"
    if cmp -s "$copy" "$ROUND_SCRIPT"; then
      echo "FAIL fixture $name changed nothing -- the mutation no longer matches $ROUND_SCRIPT" >&2
      exit 1
    fi
  fi
  out=$(check_round_script "$copy" "$docroot" 2>&1) || rc=$?
  if [ "$rc" -eq 0 ]; then
    echo "FAIL fixture $name was not caught: the guard passes with it applied" >&2
    exit 1
  fi
  case "$out" in
  *"$want"*) ;;
  *)
    echo "FAIL fixture $name was caught by the wrong check; wanted a message about '$want':" >&2
    printf '  %s\n' "$out" >&2
    exit 1
    ;;
  esac
  fixtures=$((fixtures + 1))
}

# Task #968's exact regression: the ceiling put back at the base.
mutate ceiling-at-base 'keda.query.maxReplicas from $QUERY_SCALE_TARGET' . \
  's/--set keda.query.maxReplicas="\$QUERY_SCALE_TARGET"/--set keda.query.maxReplicas="$QUERY_SCALE_BASE"/'
# The ceiling hardcoded: right today, silently wrong the day a constant moves.
mutate ceiling-literal 'keda.query.maxReplicas from $QUERY_SCALE_TARGET' . \
  's/--set keda.query.maxReplicas="\$QUERY_SCALE_TARGET"/--set keda.query.maxReplicas=4/'
# A second `--set` later in the same command wins over the first.
mutate ceiling-overridden 'sets keda.query.maxReplicas on 2 lines' . \
  's|^\(  --set keda.query.maxReplicas="\$QUERY_SCALE_TARGET" \\\)$|\1\n  --set keda.query.maxReplicas=2 \\|'
# The floor dropped: the chart default applies, not the base.
mutate floor-dropped 'sets keda.query.minReplicas on 0 lines' . \
  '/--set keda.query.minReplicas=/d'
mutate floor-literal 'keda.query.minReplicas from $QUERY_SCALE_BASE' . \
  's/--set keda.query.minReplicas="\$QUERY_SCALE_BASE"/--set keda.query.minReplicas=2/'
mutate replicas-literal 'query.replicas from $QUERY_SCALE_BASE' . \
  's/--set query.replicas="\$QUERY_SCALE_BASE"/--set query.replicas=2/'
# No headroom at all, and headroom the wrong way round.
mutate target-equals-base 'is not above QUERY_SCALE_BASE' . \
  's/^QUERY_SCALE_TARGET=[0-9][0-9]*$/QUERY_SCALE_TARGET=2/'
mutate target-below-base 'is not above QUERY_SCALE_BASE' . \
  's/^QUERY_SCALE_TARGET=[0-9][0-9]*$/QUERY_SCALE_TARGET=1/'
# A constant that is no longer a constant: nothing static can compare it.
mutate target-not-literal 'QUERY_SCALE_TARGET=<integer>' . \
  's/^QUERY_SCALE_TARGET=[0-9][0-9]*$/QUERY_SCALE_TARGET="${QUERY_SCALE_TARGET:-4}"/'
# The evidence name frozen: it stops following the constants.
mutate evidence-name-hardcoded 'does not derive SCALE_JSON' . \
  's|scale-${QUERY_SCALE_BASE}-${QUERY_SCALE_TARGET}-${QUERY_SCALE_BASE}.json|scale-2-4-2.json|'
mutate evidence-name-gone 'no single `SCALE_JSON=` assignment' . \
  '/^SCALE_JSON=/d'
# The constants moved while the documents still point at the old name: the
# round writes one file and the read_results run greps for another.
mutate base-moved-docs-stale 'make the round write' . \
  's/^QUERY_SCALE_BASE=[0-9][0-9]*$/QUERY_SCALE_BASE=3/'
mutate target-moved-docs-stale 'make the round write' . \
  's/^QUERY_SCALE_TARGET=[0-9][0-9]*$/QUERY_SCALE_TARGET=5/'

# The other direction of the same drift: documents that stop naming the file at
# all. The script is untouched, so this runs against copies of the documents --
# `sed` on the tracked tree is not something a guard may do.
docroot="$fixture_dir/docs"
mkdir -p "$docroot/deploy/kind"
for doc in "${DOC_FILES[@]}"; do
  mkdir -p "$docroot/$(dirname "$doc")"
  sed -E "s/$NAME_PATTERN//g" "$doc" >"$docroot/$doc"
done
mutate docs-silent 'have drifted' "$docroot"

base_now=$(sed -n 's/^QUERY_SCALE_BASE=\([0-9][0-9]*\)$/\1/p' "$ROUND_SCRIPT")
target_now=$(sed -n 's/^QUERY_SCALE_TARGET=\([0-9][0-9]*\)$/\1/p' "$ROUND_SCRIPT")
echo "ok ($ROUND_SCRIPT: KEDA range ${base_now}-${target_now} installed from the constants, \
evidence scale-${base_now}-${target_now}-${base_now}.json documented; $fixtures fixtures)"
