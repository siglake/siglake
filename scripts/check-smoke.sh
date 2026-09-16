#!/usr/bin/env bash
# Exercise the compose smoke's host-count formatter and pipeline exit status
# without starting the compose stack.

set -euo pipefail

cd "$(dirname "$0")/.."

smoke_check_dir=$(mktemp -d "${TMPDIR:-/tmp}/siglake-smoke-check.XXXXXX")
trap 'rm -rf -- "$smoke_check_dir"' EXIT
stub_bin="$smoke_check_dir/bin"
mkdir "$stub_bin"

cat >"$stub_bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

case "$*" in
  *'/v1/logs'*)
    ;;
  *'SELECT count(*) AS n FROM events'*)
    if [ -n "${SMOKE_TOTAL_RESPONSE:-}" ]; then
      printf '%s\n' "$SMOKE_TOTAL_RESPONSE"
    else
      printf '%s\n' '{"rows":[{"n":100}]}'
    fi
    ;;
  *'GROUP BY host'*)
    printf '%s\n' "${SMOKE_HOST_RESPONSE:?missing host response}"
    exit "${SMOKE_HOST_CURL_STATUS:-0}"
    ;;
  *)
    echo "unexpected curl invocation: $*" >&2
    exit 2
    ;;
esac
EOF

cat >"$stub_bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' '2026-09-05 00:00:00 123 data/events.parquet'
EOF

cat >"$stub_bin/sleep" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF

chmod +x "$stub_bin/curl" "$stub_bin/docker" "$stub_bin/sleep"

host_response='{"rows":[{"host":"host-0","n":25},{"host":"host-1","n":25},{"host":"host-2","n":25},{"host":"host-3","n":25}]}'
if ! output=$(PATH="$stub_bin:$PATH" SMOKE_HOST_RESPONSE="$host_response" \
    scripts/smoke.sh 100 2>&1); then
  echo "FAIL smoke fixture did not complete" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

for expected in '  host-0: 25' '  host-1: 25' '  host-2: 25' '  host-3: 25'; do
  if ! grep -Fqx "$expected" <<<"$output"; then
    echo "FAIL missing formatted host count: $expected" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
done

uneven_host_response='{"rows":[{"host":"host-0","n":26},{"host":"host-1","n":25},{"host":"host-2","n":25},{"host":"host-3","n":24}]}'
if output=$(PATH="$stub_bin:$PATH" SMOKE_HOST_RESPONSE="$uneven_host_response" \
    scripts/smoke.sh 100 2>&1); then
  echo "FAIL smoke returned success for uneven host groups" >&2
  printf '%s\n' "$output" >&2
  exit 1
else
  status=$?
fi
if [ "$status" -ne 1 ]; then
  echo "FAIL uneven host groups exited $status, expected 1" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
for expected in '  host-0: 26' '  host-1: 25' '  host-2: 25' '  host-3: 24' \
    '  FAIL: expected exactly four host groups of 25 rows'; do
  if ! grep -Fqx "$expected" <<<"$output"; then
    echo "FAIL assertion failure did not include: $expected" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
done

if output=$(PATH="$stub_bin:$PATH" SMOKE_HOST_RESPONSE="$host_response" \
    SMOKE_TOTAL_RESPONSE='{"rows":[{"n":101}]}' scripts/smoke.sh 100 2>&1); then
  echo "FAIL smoke returned success when grouped counts did not sum to total" >&2
  printf '%s\n' "$output" >&2
  exit 1
else
  status=$?
fi
if [ "$status" -ne 1 ]; then
  echo "FAIL grouped-total mismatch exited $status, expected 1" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
for expected in '  host-0: 25' '  host-1: 25' '  host-2: 25' '  host-3: 25' \
    '  FAIL: grouped counts sum to 100, total count is 101'; do
  if ! grep -Fqx "$expected" <<<"$output"; then
    echo "FAIL grouped-total failure did not include: $expected" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
done

if PATH="$stub_bin:$PATH" SMOKE_HOST_RESPONSE='not-json' \
    scripts/smoke.sh 100 >/dev/null 2>&1; then
  echo "FAIL smoke returned success after its host-count formatter failed" >&2
  exit 1
fi

# The formatter succeeds here, so only pipefail can preserve curl's failure.
if PATH="$stub_bin:$PATH" SMOKE_HOST_RESPONSE="$host_response" \
    SMOKE_HOST_CURL_STATUS=22 scripts/smoke.sh 100 >/dev/null 2>&1; then
  echo "FAIL smoke returned success after its host-count request failed" >&2
  exit 1
fi

echo "ok (four equal host groups sum to total; assertion, formatter, and pipeline failures propagate)"
