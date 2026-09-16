#!/usr/bin/env bash
# Helpers for detecting another checkout relinking workspace test executables.
# Sourced by ci-local.sh and exercised without compiling by
# check-ci-local-test-guard.sh.

record_test_executables() {
  local build_messages=$1 identities=$2 executable identity
  : >"$identities"
  while IFS= read -r executable; do
    if ! identity=$(stat -c '%i %Y' -- "$executable"); then
      return 1
    fi
    printf '%s %s\n' "$identity" "$executable" >>"$identities"
  done < <(
    python3 - "$build_messages" <<'PY'
import json
import sys

executables = set()
with open(sys.argv[1], encoding="utf-8") as messages:
    for line in messages:
        try:
            executable = json.loads(line).get("executable")
        except json.JSONDecodeError:
            continue
        if executable is not None:
            executables.add(executable)
for executable in sorted(executables):
    print(executable)
PY
  )
  [ -s "$identities" ]
}

append_cargo_diagnostics() {
  python3 - "$1" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as messages:
    for line in messages:
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        rendered = message.get("message", {}).get("rendered")
        if message.get("reason") == "compiler-message" and rendered is not None:
            print(rendered, end="" if rendered.endswith("\n") else "\n")
PY
}

changed_test_executables() {
  local identities=$1 changed=$2 old_inode old_mtime executable identity
  : >"$changed"
  while read -r old_inode old_mtime executable; do
    if ! identity=$(stat -c '%i %Y' -- "$executable" 2>/dev/null) \
      || [ "$identity" != "$old_inode $old_mtime" ]; then
      printf '%s\n' "$executable" >>"$changed"
    fi
  done <"$identities"
  [ -s "$changed" ]
}

test_pass_contaminated() {
  local test_log=$1 identities=$2 changed=$3
  changed_test_executables "$identities" "$changed" || true
  grep -Fq '(never executed)' "$test_log" || [ -s "$changed" ]
}

print_test_failure_summary() {
  awk '
    /^test .* FAILED$/ || /^error/ || /^Caused by:/ || /^  (could not execute process|No such file or directory)/ { print }
    /^failures:$/ { collecting = 1; names = ""; next }
    collecting && /^    / { names = names $0 "\n"; next }
    collecting { if (names != "") printf "failures:\n%s", names; collecting = 0 }
    END { if (collecting && names != "") printf "failures:\n%s", names }
  ' "$1"
}

find_concurrent_test_run() {
  local log_root=$1 current_log=$2 current_checkout=$3 attempt_started=$4 attempt_ended=$5
  local candidate_mtime candidate checkout
  while read -r candidate_mtime candidate; do
    [ "$candidate" != "$current_log" ] || continue
    candidate_mtime=${candidate_mtime%%.*}
    if [ "$candidate_mtime" -lt "$attempt_started" ] \
      || [ "$candidate_mtime" -gt "$attempt_ended" ]; then
      continue
    fi
    checkout=$(
      sed -nE \
        -e 's@^checkout: (/.*)$@\1@p' \
        -e 's@.*\((/.*)/(crates|third_party)/[^)]*\).*@\1@p' "$candidate" \
        | awk '!seen[$0]++' \
        | while IFS= read -r path; do
            [ "$path" = "$current_checkout" ] || { printf '%s\n' "$path"; break; }
          done
    )
    if [ -n "$checkout" ]; then
      printf '%s (%s)\n' "$checkout" "$candidate"
      return 0
    fi
  done < <(
    find "$log_root" -mindepth 2 -maxdepth 2 -type f -name test.log \
      -printf '%T@ %p\n' 2>/dev/null | sort -nr
  )
  return 1
}
