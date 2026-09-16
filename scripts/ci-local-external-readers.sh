#!/usr/bin/env bash
# Reporting for ci-local.sh's `external-readers` job: how the checker is
# invoked, and how one run of it becomes the job's status string.
#
# Sourced by ci-local.sh and exercised without engines (stub commands, a stub
# SIGLAKE_BIN) by check-external-readers-report.sh.
#
# Until 2026-09-07 the job invoked check-external-timestamp-contract.sh
# permissively in every mode and reported `ok (0 engine(s) agreed, 3 skipped)`
# when no engine was installed: a green line for a contract nothing had
# verified, which is exactly what heavy run #37 accepted as launch evidence.
# The checker has its own strict mode for this (`--require-engines`, see its
# header), so:
#
#   --strict     the checker is given --require-engines, and an engine this box
#                cannot run becomes a `skipped (...)` status. report() turns
#                that into a FAIL line naming the engines, counts it as a job
#                NOT RUN rather than a red one — a missing duckdb is no
#                evidence about main — and the summary can no longer say
#                ALL CHECKED JOBS GREEN.
#   default      still permissive, but the status says INCOMPLETE and names the
#                engines instead of reading as a plain `ok`.
#
# A reader that ran and disagreed is red in both modes.

# Extra arguments for the checker: its own strict mode, under ours only.
external_readers_args() {
  if [ "${1:-0}" = 1 ]; then
    printf '%s\n' --require-engines
  fi
}

# The engines that did not run, in the order the checker reached them: a
# permissive run prints `SKIP: <engine> not installed`, a --require-engines run
# turns the same condition into a FAIL line.
external_readers_unavailable() {
  sed -n \
    -e 's/^SKIP: \([^ ]*\) not installed.*/\1/p' \
    -e 's/^FAIL: \([^ ]*\) not installed and --require-engines was given.*/\1/p' \
    -- "$1" | awk '!seen[$0]++'
}

# `<strict> <checker log> <checker exit code>` -> the status string for
# report(). Everything is read back out of the retained job log, so the line
# and the log agree on which engines are missing.
external_readers_status() {
  local strict=${1:-0} log=${2:-} rc=${3:-1}
  if [ ! -f "$log" ]; then
    printf 'FAIL (no external-readers log at %s)\n' "$log"
    return 0
  fi

  local agreed engines missing hard list
  agreed=$(grep -c '^  ok: ' -- "$log" || true)
  engines=$(external_readers_unavailable "$log")
  missing=$(printf '%s' "$engines" | grep -c . || true)
  # A FAIL line that is neither a strict-mode skip nor the checker's own tally
  # of them is a reader that ran and disagreed, or the siglake half of the
  # contract. The two exclusions quote check-external-timestamp-contract.sh's
  # wording; check-external-readers-report.sh asserts exact status strings
  # against a real run of it, so a reworded line shows up as a red arm there.
  hard=$(grep '^FAIL: ' -- "$log" |
    grep -vc \
      -e ' not installed and --require-engines was given$' \
      -e '^FAIL: [0-9]* external-reader check(s) failed$' || true)
  list=$(printf '%s\n' "$engines" | awk 'NF{ if (n++) printf ", "; printf "%s", $0 } END { print "" }')

  if [ "$hard" -gt 0 ]; then
    printf 'FAIL (%s check(s) failed, %s reader(s) agreed%s)\n' \
      "$hard" "$agreed" "${list:+, $missing not installed: $list}"
    return 0
  fi
  if [ "$rc" != 0 ] && [ "$missing" -eq 0 ]; then
    # Nonzero with nothing to attribute it to: the fixture half died before the
    # engines (no binary, a build failure, a crash).
    printf 'FAIL (checker exited %s with no FAIL line to attribute it to)\n' "$rc"
    return 0
  fi
  if [ "$missing" -gt 0 ]; then
    if [ "$strict" = 1 ]; then
      printf 'skipped (external reader(s) not installed: %s)\n' "$list"
    else
      printf 'ok (INCOMPLETE: %s reader(s) agreed, %s not installed: %s)\n' \
        "$agreed" "$missing" "$list"
    fi
    return 0
  fi
  if [ "$agreed" -eq 0 ]; then
    # Every engine present, none of them answered: a silent pass otherwise.
    printf 'FAIL (no external reader result in the log)\n'
    return 0
  fi
  printf 'ok (%s reader(s) agreed, none skipped)\n' "$agreed"
}
