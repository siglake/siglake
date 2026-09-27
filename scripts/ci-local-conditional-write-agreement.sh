#!/usr/bin/env bash
# Compare the raw S3 conditional-write probe with the application's guard.

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <combined-probe-and-guard-log>" >&2
  exit 2
fi

log=$1
raw_line=$(grep -E 'cleanup_status=.* verdict=' "$log" | tail -n 1 || true)
guard_line=$(grep -E '^CONDITIONAL_WRITE_GUARD ' "$log" | tail -n 1 || true)
raw_verdict=$(sed -n 's/.* verdict=\([^ ]*\).*/\1/p' <<<"$raw_line")
guard_values=${guard_line#CONDITIONAL_WRITE_GUARD }

case "$raw_verdict|$guard_values" in
  'preconditions-rejected|if_match=verified if_not_exists=verified refusal=no'\
  |'silently-accepted|if_match=ignored if_not_exists=ignored refusal=yes')
    echo "CONDITIONAL_WRITE_AGREEMENT ok raw=$raw_verdict $guard_values"
    ;;
  *)
    echo "CONDITIONAL_WRITE_AGREEMENT mismatch raw=${raw_verdict:-missing} guard=${guard_values:-missing}"
    exit 1
    ;;
esac
