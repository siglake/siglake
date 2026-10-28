#!/usr/bin/env bash
# Refuse a profiling publication unless its immutable GHCR tag is absent.

set -euo pipefail

build_source=${BUILD_SOURCE_SHA:-}
if [[ ! $build_source =~ ^[0-9a-f]{40}$ ]]; then
  echo "BUILD_SOURCE_SHA must be exactly 40 lowercase hexadecimal characters" >&2
  exit 1
fi

tag="prof-$build_source"
token_url='https://ghcr.io/token?scope=repository:siglake/siglake:pull'
manifest_url="https://ghcr.io/v2/siglake/siglake/manifests/$tag"
scratch=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/siglake-prof-tag.XXXXXX")
trap 'rm -rf "$scratch"' EXIT

curl_status() {
  local output=$1
  shift
  curl --silent --show-error --location --max-time 15 \
    --output "$output" --write-out '%{http_code}' "$@"
}

if ! token_status=$(curl_status "$scratch/token.json" "$token_url"); then
  echo "GHCR tag state is undetermined: anonymous token request failed or timed out" >&2
  exit 1
fi
case "$token_status" in
  200) ;;
  401)
    echo "GHCR tag state is undetermined: anonymous token request was unauthorized (401)" >&2
    exit 1
    ;;
  429)
    echo "GHCR tag state is undetermined: anonymous token request was rate limited (429)" >&2
    exit 1
    ;;
  *)
    echo "GHCR tag state is undetermined: anonymous token request returned HTTP $token_status" >&2
    exit 1
    ;;
esac

if ! token=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["token"])' "$scratch/token.json") || [[ -z $token ]]; then
  echo "GHCR tag state is undetermined: anonymous token response had no token" >&2
  exit 1
fi

if ! manifest_status=$(curl_status /dev/null \
  --head \
  --header "Authorization: Bearer $token" \
  --header 'Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.docker.distribution.manifest.list.v2+json' \
  --dump-header "$scratch/headers" \
  "$manifest_url"); then
  echo "GHCR tag state is undetermined: manifest request failed or timed out" >&2
  exit 1
fi

case "$manifest_status" in
  404)
    echo "GHCR tag $tag is absent; publication may proceed"
    ;;
  200)
    digest=$(awk 'BEGIN { IGNORECASE=1 } /^Docker-Content-Digest:/ { gsub("\r", "", $2); print $2; exit }' "$scratch/headers")
    digest=${digest:-unknown}
    echo "refusing to overwrite existing GHCR tag $tag (digest $digest)" >&2
    exit 1
    ;;
  401)
    echo "GHCR tag state is undetermined: manifest request was unauthorized (401)" >&2
    exit 1
    ;;
  429)
    echo "GHCR tag state is undetermined: manifest request was rate limited (429)" >&2
    exit 1
    ;;
  *)
    echo "GHCR tag state is undetermined: manifest request returned HTTP $manifest_status" >&2
    exit 1
    ;;
esac
