#!/usr/bin/env bash
# Record how the selected compose object store handles S3 conditional writes.

set -uo pipefail

cd "$(dirname "$0")/.."

# Resolve the selected arm exactly as scripts/up.sh does.
# shellcheck source=scripts/compose-common.bash
source scripts/compose-common.bash

bucket=siglake-warehouse
probe_key="_ci/conditional-write-probe/${SIGLAKE_OBJECT_STORE}-$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
probe_url="${SIGLAKE_S3_HOST_ENDPOINT%/}/$bucket/$probe_key"
probe_tmp=$(mktemp -d "${TMPDIR:-/tmp}/siglake-conditional-write-probe.XXXXXX") || exit 1
payload="$probe_tmp/payload"
printf 'siglake conditional-write probe\n' >"$payload"

declare -A statuses error_codes etags curl_results
cleanup_attempted=0

response_etag() {
  tr -d '\r' <"$1" |
    sed -n 's/^[Ee][Tt][Aa][Gg]:[[:space:]]*//p' |
    tail -n 1
}

response_error_code() {
  tr '\n' ' ' <"$1" |
    sed -n 's:.*<Code>\([^<]*\)</Code>.*:\1:p'
}

run_request() { # <label> <method> [header]
  local label=$1 method=$2 request_header=${3:-}
  local headers="$probe_tmp/$label.headers" body="$probe_tmp/$label.body"
  local status curl_rc=0 etag error_code
  local -a method_args header_args=()

  : >"$headers"
  : >"$body"
  if [ -n "$request_header" ]; then
    header_args=(--header "$request_header")
  fi
  case "$method" in
    PUT)
      method_args=(--request PUT --header 'Content-Type: application/octet-stream' \
        --data-binary "@$payload")
      ;;
    HEAD) method_args=(--head) ;;
    DELETE) method_args=(--request DELETE) ;;
    *) return 2 ;;
  esac

  status=$(curl --silent --show-error --connect-timeout 5 --max-time 15 \
    --aws-sigv4 "aws:amz:$SIGLAKE_S3_REGION:s3" \
    --user "$SIGLAKE_S3_ACCESS_KEY:$SIGLAKE_S3_SECRET_KEY" \
    --dump-header "$headers" --output "$body" --write-out '%{http_code}' \
    "${method_args[@]}" "${header_args[@]}" "$probe_url") || curl_rc=$?
  if [[ ! $status =~ ^[0-9]{3}$ ]] || [ "$curl_rc" -ne 0 ]; then
    status=000
  fi

  etag=$(response_etag "$headers")
  error_code=$(response_error_code "$body")
  statuses[$label]=$status
  error_codes[$label]=${error_code:-none}
  etags[$label]=$([ -n "$etag" ] && printf present || printf absent)
  curl_results[$label]=$curl_rc
}

cleanup_probe() {
  if [ "$cleanup_attempted" -eq 0 ]; then
    cleanup_attempted=1
    run_request cleanup DELETE || true
  fi
  rm -rf -- "$probe_tmp"
}
trap cleanup_probe EXIT

# Do not stop after one failed request: the retained line must say which
# observations were available and which transport failed.
run_request initial_put PUT
run_request if_none_match PUT 'If-None-Match: *'
run_request stale_if_match PUT 'If-Match: "00000000000000000000000000000000"'
run_request head HEAD
run_request cleanup DELETE
cleanup_attempted=1

verdict=unexpected-response
verdict_detail=none
probe_ok=0
if [ "${curl_results[initial_put]}" -ne 0 ] \
  || [ "${curl_results[if_none_match]}" -ne 0 ] \
  || [ "${curl_results[stale_if_match]}" -ne 0 ] \
  || [ "${curl_results[head]}" -ne 0 ]; then
  verdict=incomplete
  verdict_detail=transport
elif [[ ! ${statuses[initial_put]} =~ ^2 ]] || [[ ! ${statuses[head]} =~ ^2 ]]; then
  verdict=incomplete
  verdict_detail=setup
elif [[ ${statuses[if_none_match]} =~ ^(401|403)$ ]] \
  || [[ ${statuses[stale_if_match]} =~ ^(401|403)$ ]]; then
  verdict=incomplete
  verdict_detail=authentication
elif [[ ${statuses[if_none_match]} =~ ^2 || ${statuses[stale_if_match]} =~ ^2 ]]; then
  verdict=silently-accepted
elif [ "${statuses[if_none_match]}" = 501 ] \
  || [ "${statuses[stale_if_match]}" = 501 ] \
  || [[ ${error_codes[if_none_match]} =~ ^(NotImplemented|NotSupported)$ ]] \
  || [[ ${error_codes[stale_if_match]} =~ ^(NotImplemented|NotSupported)$ ]]; then
  verdict=unsupported
elif [ "${statuses[if_none_match]}" = 412 ] \
  && [ "${statuses[stale_if_match]}" = 412 ]; then
  if [ "${etags[initial_put]}" = present ] && [ "${etags[head]}" = present ]; then
    verdict=preconditions-rejected
    probe_ok=1
  else
    verdict=etag-missing
  fi
fi

if [ "${curl_results[cleanup]}" -ne 0 ] || [[ ! ${statuses[cleanup]} =~ ^2 ]]; then
  cleanup_result=failed
  probe_ok=0
else
  cleanup_result=deleted
fi

printf '%s\n' \
  "conditional-write probe: store=$SIGLAKE_OBJECT_STORE" \
  "  initial_put_status=${statuses[initial_put]} initial_put_error_code=${error_codes[initial_put]} initial_put_etag=${etags[initial_put]}" \
  "  if_none_match_status=${statuses[if_none_match]} if_none_match_error_code=${error_codes[if_none_match]} if_none_match_etag=${etags[if_none_match]}" \
  "  stale_if_match_status=${statuses[stale_if_match]} stale_if_match_error_code=${error_codes[stale_if_match]} stale_if_match_etag=${etags[stale_if_match]}" \
  "  head_status=${statuses[head]} head_error_code=${error_codes[head]} head_etag=${etags[head]}" \
  "  cleanup_status=${statuses[cleanup]} cleanup_error_code=${error_codes[cleanup]} cleanup=$cleanup_result verdict=$verdict verdict_detail=$verdict_detail"

exit "$((1 - probe_ok))"
