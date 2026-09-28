#!/usr/bin/env bash
# Daemon-free fixtures for ci-local-conditional-write-probe.sh.

set -euo pipefail

cd "$(dirname "$0")/.."

check_dir=$(mktemp -d "${TMPDIR:-/tmp}/siglake-conditional-write-check.XXXXXX")
trap 'rm -rf -- "$check_dir"' EXIT
mkdir "$check_dir/bin"

cat >"$check_dir/bin/curl" <<'STUB'
#!/usr/bin/env bash
set -u

method=GET
request_header=
headers_file=
body_file=
url=
has_body=0
sigv4=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --request) method=$2; shift 2 ;;
    --head) method=HEAD; shift ;;
    --header)
      case "$2" in
        If-*) request_header=$2 ;;
      esac
      shift 2
      ;;
    --data-binary) has_body=1; shift 2 ;;
    --aws-sigv4) sigv4=$2; shift 2 ;;
    --user)
      [ "$2" = "$EXPECTED_ACCESS_KEY:$EXPECTED_SECRET_KEY" ] || exit 91
      shift 2
      ;;
    --dump-header) headers_file=$2; shift 2 ;;
    --output) body_file=$2; shift 2 ;;
    --write-out|--connect-timeout|--max-time) shift 2 ;;
    --silent|--show-error) shift ;;
    --*) echo "unexpected curl option: $1" >&2; exit 92 ;;
    *) url=$1; shift ;;
  esac
done

printf '%s|%s|header=%s|body=%s|sigv4=%s\n' \
  "$method" "$url" "$request_header" "$has_body" "$sigv4" >>"$CURL_CALLS"
call=$(wc -l <"$CURL_CALLS")

status=200
error_code=
case "$FIXTURE_SCENARIO:$call" in
  rejected:2|rejected:3|cleanup:2|cleanup:3) status=412; error_code=PreconditionFailed ;;
  accepted:2|accepted:3) status=200 ;;
  unsupported:2|unsupported:3) status=501; error_code=NotImplemented ;;
  setup:1) status=403; error_code=AccessDenied ;;
  authentication:2|authentication:3) status=403; error_code=AccessDenied ;;
  unexpected:2|unexpected:3) status=409; error_code=Conflict ;;
  transport:2)
    : >"$headers_file"
    : >"$body_file"
    printf 000
    exit 7
    ;;
esac
if [ "$call" -eq 5 ]; then
  if [ "$FIXTURE_SCENARIO" = cleanup ]; then
    status=500
    error_code=InternalError
  else
    status=204
  fi
fi

{
  printf 'HTTP/1.1 %s fixture\r\n' "$status"
  if [ "$call" -eq 1 ] || [ "$method" = HEAD ]; then
    printf 'ETag: "fixture-etag"\r\n'
  fi
  printf '\r\n'
} >"$headers_file"
if [ -n "$error_code" ]; then
  printf '<Error><Code>%s</Code></Error>\n' "$error_code" >"$body_file"
else
  : >"$body_file"
fi
printf '%s' "$status"
STUB
chmod +x "$check_dir/bin/curl"

fail() {
  echo "FAIL $*" >&2
  exit 1
}

run_fixture() { # <store> <scenario>
  local store=$1 scenario=$2
  fixture_rc=0
  : >"$check_dir/curl.calls"
  fixture_output=$(env -u SIGLAKE_S3_ENDPOINT -u SIGLAKE_S3_HOST_ENDPOINT \
    -u SIGLAKE_S3_ACCESS_KEY -u SIGLAKE_S3_SECRET_KEY \
    PATH="$check_dir/bin:$PATH" CURL_CALLS="$check_dir/curl.calls" \
    FIXTURE_SCENARIO="$scenario" SIGLAKE_OBJECT_STORE="$store" \
    SIGLAKE_MINIO_HOST_PORT=29000 SIGLAKE_GARAGE_HOST_PORT=23900 \
    SIGLAKE_GARAGE_ACCESS_KEY=fixture-garage-access \
    SIGLAKE_GARAGE_SECRET_KEY=fixture-garage-secret \
    EXPECTED_ACCESS_KEY=$([ "$store" = garage ] && printf fixture-garage-access || printf minioadmin) \
    EXPECTED_SECRET_KEY=$([ "$store" = garage ] && printf fixture-garage-secret || printf minioadmin) \
    scripts/ci-local-conditional-write-probe.sh 2>&1) || fixture_rc=$?
}

run_fixture minio rejected
[ "$fixture_rc" -eq 0 ] || fail "MinIO rejected fixture returned $fixture_rc: $fixture_output"
grep -Fq 'store=minio' <<<"$fixture_output" || fail "MinIO arm was not reported"
grep -Fq 'http://localhost:29000/siglake-warehouse/' "$check_dir/curl.calls" \
  || fail "MinIO arm did not use its selected host endpoint"

run_fixture garage rejected
[ "$fixture_rc" -eq 0 ] || fail "Garage rejected fixture returned $fixture_rc: $fixture_output"
grep -Fq 'store=garage' <<<"$fixture_output" || fail "Garage arm was not reported"
grep -Fq 'http://localhost:23900/siglake-warehouse/' "$check_dir/curl.calls" \
  || fail "Garage arm did not use its selected host endpoint"
grep -Fq 'PUT|'"$(head -n 1 "$check_dir/curl.calls" | cut -d'|' -f2)"'|header=If-None-Match: *|body=1' \
  "$check_dir/curl.calls" || fail "If-None-Match request header or body is missing"
grep -Fq 'header=If-Match: "00000000000000000000000000000000"|body=1' \
  "$check_dir/curl.calls" || fail "stale If-Match request header or body is missing"
grep -Fq 'sigv4=aws:amz:us-east-1:s3' "$check_dir/curl.calls" \
  || fail "requests were not signed for the configured region and S3 service"
grep -Fq 'verdict=preconditions-rejected' <<<"$fixture_output" \
  || fail "412 responses were not classified as precondition rejection"
if grep -Fq 'fixture-garage-secret' <<<"$fixture_output" \
  || grep -Fq 'fixture-garage-secret' "$check_dir/curl.calls"; then
  fail "the probe logged its secret key"
fi

run_fixture garage accepted
[ "$fixture_rc" -ne 0 ] || fail "silently accepted conditions passed the probe"
grep -Fq 'verdict=silently-accepted' <<<"$fixture_output" \
  || fail "2xx conditional writes were not classified as silently accepted"

run_fixture garage unsupported
[ "$fixture_rc" -ne 0 ] || fail "unsupported conditions passed the probe"
grep -Fq 'if_none_match_status=501 if_none_match_error_code=NotImplemented' <<<"$fixture_output" \
  || fail "unsupported response observations were not retained"
grep -Fq 'verdict=unsupported' <<<"$fixture_output" \
  || fail "501/NotImplemented responses were not classified as unsupported"

run_fixture garage transport
[ "$fixture_rc" -ne 0 ] || fail "incomplete transport evidence passed the probe"
[ "$(wc -l <"$check_dir/curl.calls")" -eq 5 ] \
  || fail "a transport failure stopped later observations or cleanup"
grep -Fq 'if_none_match_status=000' <<<"$fixture_output" \
  || fail "the missing transport observation was not retained"
grep -Fq 'verdict=incomplete' <<<"$fixture_output" \
  || fail "transport failure was not classified as incomplete evidence"
tail -n 1 "$check_dir/curl.calls" | grep -Fq 'DELETE|' \
  || fail "failure cleanup did not delete the probe key"

run_fixture garage setup
[ "$fixture_rc" -ne 0 ] || fail "failed setup passed the probe"
grep -Fq 'initial_put_status=403 initial_put_error_code=AccessDenied' <<<"$fixture_output" \
  || fail "setup failure observations were not retained"
grep -Fq 'verdict=incomplete verdict_detail=setup' <<<"$fixture_output" \
  || fail "setup failure was not classified as incomplete evidence"

run_fixture garage authentication
[ "$fixture_rc" -ne 0 ] || fail "failed authentication passed the probe"
grep -Fq 'verdict=incomplete verdict_detail=authentication' <<<"$fixture_output" \
  || fail "conditional-request authentication failure was not classified"

run_fixture garage cleanup
[ "$fixture_rc" -ne 0 ] || fail "failed cleanup passed the probe"
grep -Fq 'cleanup_status=500 cleanup_error_code=InternalError cleanup=failed' \
  <<<"$fixture_output" || fail "cleanup failure observations were not retained"

run_fixture garage unexpected
[ "$fixture_rc" -ne 0 ] || fail "unexpected responses passed the probe"
grep -Fq 'verdict=unexpected-response' <<<"$fixture_output" \
  || fail "unexpected responses were not retained"

run_qualification_fixture() { # <name> <store> <scenario> <guard> <cas> <pass|fail> <capability> <safety> <reason>
  local name=$1 store=$2 scenario=$3 guard=$4 cas=$5 expected=$6
  local expected_capability=$7 expected_safety=$8 expected_reason=$9 rc=0
  local log="$check_dir/qualification-$name.log" output
  run_fixture "$store" "$scenario"
  printf '%s\n' "$fixture_output" >"$log"
  [ -z "$guard" ] || printf '%s\n' "$guard" >>"$log"
  [ -z "$cas" ] || printf '%s\n' "$cas" >>"$log"
  output=$(scripts/ci-local-conditional-write-agreement.sh "$log" "$fixture_rc" 2>&1) || rc=$?
  case "$expected:$rc" in
    pass:0)
      grep -Fq 'CONDITIONAL_WRITE_QUALIFICATION ok' <<<"$output" \
        || fail "$name did not print a passing qualification: $output"
      ;;
    fail:0) fail "$name unexpectedly passed: $output" ;;
    fail:*)
      grep -Fq 'CONDITIONAL_WRITE_QUALIFICATION fail' <<<"$output" \
        || fail "$name did not print a failing qualification: $output"
      ;;
    *) fail "$name returned $rc: $output" ;;
  esac
  grep -Fq "CONDITIONAL_WRITE_CAPABILITY verdict=$expected_capability" <<<"$output" \
    || fail "$name capability verdict differs: $output"
  grep -Fq "CONDITIONAL_WRITE_APPLICATION_SAFETY verdict=$expected_safety" <<<"$output" \
    || fail "$name application-safety verdict differs: $output"
  grep -Fq "reason=$expected_reason" <<<"$output" \
    || fail "$name qualification reason differs: $output"
}

verified_guard='CONDITIONAL_WRITE_GUARD if_match=verified if_not_exists=verified refusal=no'
refused_ignored_guard='CONDITIONAL_WRITE_GUARD if_match=ignored if_not_exists=ignored refusal=yes'
accepted_ignored_guard='CONDITIONAL_WRITE_GUARD if_match=ignored if_not_exists=ignored refusal=no'
refused_unsupported_guard='CONDITIONAL_WRITE_GUARD if_match=unsupported if_not_exists=unsupported refusal=yes'
cas_complete='CAS_RACE rounds=20 winners=1 losers=1'
cas_skipped='CAS_RACE skipped=guard-refuses-store'

run_qualification_fixture minio-positive minio rejected "$verified_guard" "$cas_complete" \
  pass safe accepted-safe positive-safety
run_qualification_fixture garage-refused garage accepted "$refused_ignored_guard" "$cas_skipped" \
  pass unsafe-ignored refused-unsafe negative-safety-only
run_qualification_fixture garage-unsupported garage unsupported "$refused_unsupported_guard" "$cas_skipped" \
  pass unsupported refused-unsafe negative-safety-only
run_qualification_fixture minio-negative-is-not-enough minio accepted "$refused_ignored_guard" "$cas_skipped" \
  fail unsafe-ignored refused-unsafe negative-safety-not-valid-for-store
run_qualification_fixture unsafe-accepted garage accepted "$accepted_ignored_guard" "$cas_skipped" \
  fail unsafe-ignored accepted-unsafe unsafe-endpoint-accepted
run_qualification_fixture raw-guard-disagreement garage accepted "$verified_guard" "$cas_skipped" \
  fail unsafe-ignored disagreement raw-guard-disagreement
run_qualification_fixture safe-refused garage rejected \
  'CONDITIONAL_WRITE_GUARD if_match=verified if_not_exists=verified refusal=yes' "$cas_complete" \
  fail safe disagreement safe-endpoint-refused
run_qualification_fixture missing-guard garage accepted '' "$cas_skipped" \
  fail unsafe-ignored unverified incomplete-application-evidence
run_qualification_fixture missing-positive-cas minio rejected "$verified_guard" '' \
  fail safe incomplete incomplete-cas-evidence
run_qualification_fixture skipped-positive-cas minio rejected "$verified_guard" "$cas_skipped" \
  fail safe incomplete incomplete-cas-evidence
run_qualification_fixture missing-refusal-cas garage accepted "$refused_ignored_guard" '' \
  fail unsafe-ignored incomplete incomplete-refusal-evidence
run_qualification_fixture raced-unsafe garage accepted "$refused_ignored_guard" "$cas_complete" \
  fail unsafe-ignored incomplete incomplete-refusal-evidence
run_qualification_fixture transport-incomplete garage transport "$refused_ignored_guard" "$cas_skipped" \
  fail incomplete unverified incomplete-raw-evidence
run_qualification_fixture setup-incomplete garage setup "$refused_ignored_guard" "$cas_skipped" \
  fail incomplete unverified incomplete-raw-evidence
run_qualification_fixture auth-incomplete garage authentication "$refused_ignored_guard" "$cas_skipped" \
  fail incomplete unverified incomplete-raw-evidence
run_qualification_fixture cleanup-incomplete garage cleanup "$verified_guard" "$cas_complete" \
  fail safe unverified incomplete-raw-evidence
run_qualification_fixture unexpected-incomplete garage unexpected "$refused_ignored_guard" "$cas_skipped" \
  fail incomplete unverified incomplete-raw-evidence

grep -Fq '|| conditional_probe_rc=$?' scripts/ci-local.sh \
  || fail "ci-local does not retain the standalone raw-probe failure"
grep -Fq 'cargo test -p siglake-storage --lib conditional_write_live --' \
  scripts/ci-local.sh || fail "ci-local's live docker job does not run the application guard"
grep -Fq 'timeout --signal=TERM --kill-after=10s 180s' \
  scripts/ci-local.sh || fail "ci-local's conditional-write live tests have no process deadline"
grep -Fq 'scripts/ci-local-conditional-write-agreement.sh "$dlog" "$conditional_probe_rc"' \
  scripts/ci-local.sh || fail "ci-local's live docker job does not compare the two verdicts"
grep -Fq 'cargo test -p siglake-storage --lib conditional_write_live --' \
  .github/workflows/ci.yml || fail "hosted Docker CI does not run the application guard"
grep -Fq 'scripts/ci-local-conditional-write-agreement.sh "$conditional_log" "$conditional_probe_rc"' \
  .github/workflows/ci.yml || fail "hosted Docker CI does not compare the two verdicts"

echo "ok (raw capability, application safety, refusal and incomplete-evidence grading)"
