#!/usr/bin/env bash
# Exercise the generated-artifact retry without compiling Rust.

set -euo pipefail

cd "$(dirname "$0")/.."
. scripts/ci-local-generated-guard.sh

check_dir=$(mktemp -d "${TMPDIR:-/tmp}/siglake-generated-guard.XXXXXX")
trap 'rm -rf -- "$check_dir"' EXIT

run_fixture() (
  local scenario=$1 expected_attempts=$2 expected_refreshes=$3 expected_reasons=$4
  local attempts=0 refreshes=0
  local glog="$check_dir/$scenario.log"
  local -a gen_why=()

  run_generators() {
    attempts=$((attempts + 1))
    gen_why=()
    printf 'fixture %s attempt %s diagnostic\n' "$scenario" "$attempts" >>"$glog"
    case "$scenario:$attempts" in
      recovers:1|persistent-failure:1|persistent-failure:2|refresh-fails:1)
        gen_why+=("siglake-openapi failed to run")
        ;;
      persistent-drift:1|persistent-drift:2)
        gen_why+=("docs/api is stale (differs from HEAD):")
        ;;
    esac
  }

  refresh_workspace_sources() {
    refreshes=$((refreshes + 1))
    [ "$scenario" != refresh-fails ]
  }

  run_generators_with_retry

  if [ "$attempts" -ne "$expected_attempts" ]; then
    echo "FAIL $scenario ran $attempts generator attempts, expected $expected_attempts" >&2
    exit 1
  fi
  if [ "$refreshes" -ne "$expected_refreshes" ]; then
    echo "FAIL $scenario ran $refreshes refreshes, expected $expected_refreshes" >&2
    exit 1
  fi
  if [ "${#gen_why[@]}" -ne "$expected_reasons" ]; then
    echo "FAIL $scenario retained ${#gen_why[@]} reasons, expected $expected_reasons" >&2
    printf '  %s\n' "${gen_why[@]}" >&2
    exit 1
  fi

  local attempt_count diagnostic_count first_reason_count retry_count
  attempt_count=$(grep -c '^generator attempt [12]$' "$glog")
  if [ "$attempt_count" -ne "$expected_attempts" ]; then
    echo "FAIL $scenario did not label every generator attempt" >&2
    exit 1
  fi
  diagnostic_count=$(grep -c "^fixture $scenario attempt [12] diagnostic$" "$glog")
  if [ "$diagnostic_count" -ne "$expected_attempts" ]; then
    echo "FAIL $scenario did not preserve every attempt's diagnostic" >&2
    exit 1
  fi
  first_reason_count=$(grep -c '^first generator pass reason:' "$glog" || true)
  if [ "$expected_refreshes" -eq 1 ] && [ "$first_reason_count" -ne 1 ]; then
    echo "FAIL $scenario did not preserve its first-pass reason" >&2
    exit 1
  fi
  if [ "$expected_refreshes" -eq 0 ] && [ "$first_reason_count" -ne 0 ]; then
    echo "FAIL $scenario recorded a reason for a clean pass" >&2
    exit 1
  fi
  retry_count=$(grep -cF \
    "first generator pass reported failure or drift; refreshed this checkout's sources and retried once" \
    "$glog" || true)
  if [ "$expected_attempts" -eq 2 ] && [ "$retry_count" -ne 1 ]; then
    echo "FAIL $scenario did not record its single retry" >&2
    exit 1
  fi
  if [ "$expected_attempts" -eq 1 ] && [ "$retry_count" -ne 0 ]; then
    echo "FAIL $scenario recorded a retry that did not happen" >&2
    exit 1
  fi
)

# A stale compiled unit can recover. Genuine generator failures and artifact
# drift remain red after exactly one retry. A failed refresh preserves the
# original failure and adds its own reason. A clean pass does no extra work.
run_fixture recovers 2 1 0
run_fixture persistent-failure 2 1 1
run_fixture persistent-drift 2 1 1
run_fixture refresh-fails 1 1 2
run_fixture clean 1 0 0

echo "ok (generated recovery, persistent failure, persistent drift, failed refresh and clean fixtures)"
