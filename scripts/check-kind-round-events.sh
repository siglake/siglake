#!/usr/bin/env bash
# Verify the kind round's configurable initial batch without starting a
# cluster. The check sources only the setup prelude before the cleanup trap.

set -euo pipefail

cd "$(dirname "$0")/.."

ROUND_SCRIPT=scripts/kind-round.sh
TRAP_LINE='trap cleanup EXIT INT TERM'

fail() {
  echo "FAIL $*" >&2
  exit 1
}

check_round_script() {
  local script=$1 check_dir prelude actual expected value invalid_dir out rc first
  check_dir=$(mktemp -d "$fixture_dir/check.XXXXXX")
  mkdir -p "$check_dir/scripts" "$check_dir/tmp"
  prelude="$check_dir/scripts/prelude.bash"

  grep -qxF "$TRAP_LINE" "$script" ||
    fail "$script no longer has a \`$TRAP_LINE\` line -- this check cannot isolate setup"
  sed -n "1,/^${TRAP_LINE}\$/p" "$script" | sed '$d' >"$prelude"
  cp scripts/kind-common.bash "$check_dir/scripts/kind-common.bash"

  run_valid() {
    local mode=$1
    (
      TMPDIR="$check_dir/tmp/$mode"
      mkdir -p "$TMPDIR"
      export TMPDIR
      case "$mode" in
      omitted) unset KIND_ROUND_EVENTS ;;
      empty)
        KIND_ROUND_EVENTS=
        export KIND_ROUND_EVENTS
        ;;
      hundred)
        KIND_ROUND_EVENTS=100
        export KIND_ROUND_EVENTS
        ;;
      *) fail "unknown valid fixture $mode" ;;
      esac
      # shellcheck disable=SC1090
      source "$prelude"
      printf '%s\n' "$LOAD_EVENTS"
      initial_load_description
      rm -rf -- "$TMP_DIR"
    )
  }

  expected=$'6000\ningest 6000 events with 6000 distinct hosts (above the 4096 inline group-count ceiling)'
  actual=$(run_valid omitted)
  [[ "$actual" == "$expected" ]] ||
    fail "$script: omitted KIND_ROUND_EVENTS produced \`$actual\`, expected \`$expected\`"

  actual=$(run_valid empty)
  [[ "$actual" == "$expected" ]] ||
    fail "$script: empty KIND_ROUND_EVENTS produced \`$actual\`, expected the 6000 default"

  expected=$'100\ningest 100 events with 100 distinct hosts (at or below the 4096 inline group-count ceiling)'
  actual=$(run_valid hundred)
  [[ "$actual" == "$expected" ]] ||
    fail "$script: KIND_ROUND_EVENTS=100 produced \`$actual\`, expected \`$expected\`"

  for value in 0 -1 1.5 abc 01; do
    invalid_dir="$check_dir/tmp/invalid-${value//[^A-Za-z0-9]/_}"
    mkdir -p "$invalid_dir"
    rc=0
    out=$(KIND_ROUND_EVENTS="$value" TMPDIR="$invalid_dir" \
      bash -c 'source "$1"' _ "$prelude" 2>&1) || rc=$?
    [[ "$rc" -ne 0 ]] ||
      fail "$script: KIND_ROUND_EVENTS=$value was accepted"
    case "$out" in
    *'ERROR: KIND_ROUND_EVENTS must be a positive integer'*) ;;
    *) fail "$script: KIND_ROUND_EVENTS=$value failed without the validation error: $out" ;;
    esac
    first=$(find "$invalid_dir" -mindepth 1 -print -quit)
    [[ -z "$first" ]] ||
      fail "$script: KIND_ROUND_EVENTS=$value reached temporary setup before rejection: $first"
  done
}

fixture_dir=$(mktemp -d "${TMPDIR:-/tmp}/siglake-kind-round-events.XXXXXX")
trap 'rm -rf -- "$fixture_dir"' EXIT

check_round_script "$ROUND_SCRIPT"

fixtures=0
mutate() {
  local name=$1 want=$2 expression=$3 copy out rc=0
  copy="$fixture_dir/$name.sh"
  sed "$expression" "$ROUND_SCRIPT" >"$copy"
  cmp -s "$copy" "$ROUND_SCRIPT" &&
    fail "fixture $name changed nothing -- the mutation no longer matches $ROUND_SCRIPT"
  out=$(check_round_script "$copy" 2>&1) || rc=$?
  [[ "$rc" -ne 0 ]] ||
    fail "fixture $name was not caught: the check passes with it applied"
  case "$out" in
  *"$want"*) ;;
  *)
    printf "FAIL fixture %s was caught by the wrong check; wanted a message about '%s':\n  %s\n" \
      "$name" "$want" "$out" >&2
    exit 1
    ;;
  esac
  fixtures=$((fixtures + 1))
}

mutate hardcoded-load 'KIND_ROUND_EVENTS=100 produced' \
  's/^LOAD_EVENTS=.*/LOAD_EVENTS=6000/'
mutate empty-is-not-default 'empty KIND_ROUND_EVENTS' \
  's/${KIND_ROUND_EVENTS:-6000}/${KIND_ROUND_EVENTS-6000}/'
mutate validation-removed 'KIND_ROUND_EVENTS=0 was accepted' \
  '/^\[\[ "$LOAD_EVENTS" =~ /,/^}/d'
mutate stale-host-message 'omitted KIND_ROUND_EVENTS produced' \
  's/with %s distinct hosts/with >4096 distinct hosts/'
mutate wrong-ceiling 'omitted KIND_ROUND_EVENTS produced' \
  's/inline_group_count_ceiling=4096/inline_group_count_ceiling=8192/'

echo "ok ($ROUND_SCRIPT: KIND_ROUND_EVENTS defaults to 6000, accepts positive integers, rejects invalid values before setup, and reports host cardinality; $fixtures fixtures)"
