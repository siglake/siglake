#!/usr/bin/env bash
# Grade raw S3 conditional-write capability separately from application safety.

set -euo pipefail

if [ "$#" -ne 2 ] || [[ ! $2 =~ ^[0-9]+$ ]]; then
  echo "usage: $0 <combined-probe-and-guard-log> <raw-probe-exit-code>" >&2
  exit 2
fi

log=$1
raw_rc=$2
capability=incomplete
application_safety=unverified
raw_verdict=missing
store=missing
guard_values=missing
production_guard_values=missing
cas_evidence=missing

report() { # <ok|fail> <reason>
  local outcome=$1 reason=$2
  echo "CONDITIONAL_WRITE_CAPABILITY verdict=$capability store=$store raw=$raw_verdict"
  echo "CONDITIONAL_WRITE_APPLICATION_SAFETY verdict=$application_safety guard=$guard_values production_guard=$production_guard_values cas=$cas_evidence"
  echo "CONDITIONAL_WRITE_QUALIFICATION $outcome reason=$reason"
  [ "$outcome" = ok ]
}

field() { # <line> <name>
  sed -n "s/.* $2=\([^ ]*\).*/\1/p" <<<"$1"
}

raw_precondition() { # <observation line> <field prefix>
  local line=$1 prefix=$2 status error_code
  status=$(field "$line" "${prefix}_status")
  error_code=$(field "$line" "${prefix}_error_code")
  if [[ $status =~ ^2 ]]; then
    printf ignored
  elif [ "$status" = 412 ]; then
    printf verified
  elif [ "$status" = 501 ] || [[ $error_code =~ ^(NotImplemented|NotSupported)$ ]]; then
    printf unsupported
  else
    printf incomplete
  fi
}

mapfile -t raw_headers < <(grep -E '^conditional-write probe: store=' "$log" || true)
mapfile -t raw_summaries < <(grep -E 'cleanup_status=.* verdict=' "$log" || true)
mapfile -t if_none_lines < <(grep -E '^  if_none_match_status=' "$log" || true)
mapfile -t if_match_lines < <(grep -E '^  stale_if_match_status=' "$log" || true)
mapfile -t guard_lines < <(grep -E '^CONDITIONAL_WRITE_GUARD ' "$log" || true)
mapfile -t production_guard_lines < <(grep -E '^CONDITIONAL_WRITE_PRODUCTION_GUARD ' "$log" || true)
mapfile -t cas_lines < <(grep -E '^CAS_RACE ' "$log" || true)

if [ "${#raw_headers[@]}" -ne 1 ] || [ "${#raw_summaries[@]}" -ne 1 ] \
  || [ "${#if_none_lines[@]}" -ne 1 ] || [ "${#if_match_lines[@]}" -ne 1 ]; then
  report fail incomplete-raw-evidence
  exit 1
fi

store=${raw_headers[0]##*=}
raw_verdict=$(field "${raw_summaries[0]}" verdict)
cleanup=$(field "${raw_summaries[0]}" cleanup)
if_none=$(raw_precondition "${if_none_lines[0]}" if_none_match)
if_match=$(raw_precondition "${if_match_lines[0]}" stale_if_match)

case "$if_match|$if_none" in
  verified\|verified) capability=safe ;;
  ignored\|ignored|ignored\|verified|verified\|ignored|ignored\|unsupported|unsupported\|ignored)
    capability=unsafe-ignored
    ;;
  unsupported\|unsupported|unsupported\|verified|verified\|unsupported)
    capability=unsupported
    ;;
  *) capability=incomplete ;;
esac
case "$raw_verdict" in
  incomplete|unexpected-response|etag-missing) capability=incomplete ;;
esac

expected_raw_rc=1
expected_raw_verdict=
case "$capability" in
  safe)
    expected_raw_rc=0
    expected_raw_verdict=preconditions-rejected
    ;;
  unsafe-ignored) expected_raw_verdict=silently-accepted ;;
  unsupported) expected_raw_verdict=unsupported ;;
esac

if [ "$cleanup" != deleted ] || [ "$capability" = incomplete ] \
  || [ "$raw_verdict" != "$expected_raw_verdict" ] \
  || [ "$raw_rc" -ne "$expected_raw_rc" ]; then
  report fail incomplete-raw-evidence
  exit 1
fi

if [ "${#guard_lines[@]}" -ne 1 ]; then
  report fail incomplete-application-evidence
  exit 1
fi
guard_values=${guard_lines[0]#CONDITIONAL_WRITE_GUARD }
guard_if_match=$(field " ${guard_lines[0]}" if_match)
guard_if_none=$(field " ${guard_lines[0]}" if_not_exists)
guard_refusal=$(field " ${guard_lines[0]}" refusal)

if [ "${#production_guard_lines[@]}" -gt 1 ]; then
  application_safety=incomplete
  report fail incomplete-production-builder-evidence
  exit 1
fi
if [ "${#production_guard_lines[@]}" -eq 1 ]; then
  production_if_match=$(field " ${production_guard_lines[0]}" if_match)
  production_if_none=$(field " ${production_guard_lines[0]}" if_not_exists)
  production_refusal=$(field " ${production_guard_lines[0]}" refusal)
  production_guard_values=${production_guard_lines[0]#CONDITIONAL_WRITE_PRODUCTION_GUARD }
  if [ "$production_if_match" != "$guard_if_match" ] \
    || [ "$production_if_none" != "$guard_if_none" ] \
    || [ "$production_refusal" != "$guard_refusal" ]; then
    application_safety=disagreement
    report fail production-builder-guard-disagreement
    exit 1
  fi
fi

if [ "$guard_if_match" != "$if_match" ] || [ "$guard_if_none" != "$if_none" ]; then
  application_safety=disagreement
  report fail raw-guard-disagreement
  exit 1
fi

if [ "$capability" = safe ]; then
  if [ "$guard_refusal" != no ]; then
    application_safety=disagreement
    report fail safe-endpoint-refused
    exit 1
  fi
  if [ "${#cas_lines[@]}" -ne 1 ] \
    || [ "${cas_lines[0]}" != 'CAS_RACE rounds=20 winners=1 losers=1' ]; then
    application_safety=incomplete
    [ "${#cas_lines[@]}" -eq 1 ] && cas_evidence=${cas_lines[0]#CAS_RACE }
    report fail incomplete-cas-evidence
    exit 1
  fi
  cas_evidence='rounds=20,winners=1,losers=1'
  application_safety=accepted-safe
  report ok positive-safety
  exit 0
fi

if [ "$guard_refusal" != yes ]; then
  application_safety=accepted-unsafe
  report fail unsafe-endpoint-accepted
  exit 1
fi
if [ "${#cas_lines[@]}" -ne 1 ] \
  || [ "${cas_lines[0]}" != 'CAS_RACE skipped=guard-refuses-store' ]; then
  application_safety=incomplete
  [ "${#cas_lines[@]}" -eq 1 ] && cas_evidence=${cas_lines[0]#CAS_RACE }
  report fail incomplete-refusal-evidence
  exit 1
fi
cas_evidence=skipped-guard-refuses-store
application_safety=refused-unsafe

if [ "$store" != garage ]; then
  report fail negative-safety-not-valid-for-store
  exit 1
fi
report ok negative-safety-only
