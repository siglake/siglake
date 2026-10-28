#!/usr/bin/env bash
# Validate and resolve the immutable source of a profiling image publication.

set -euo pipefail

mode=${1:-}
build_source=${BUILD_SOURCE:-}

if [[ ! $build_source =~ ^[0-9a-f]{40}$ ]]; then
  echo "BUILD_SOURCE must be exactly 40 lowercase hexadecimal characters" >&2
  exit 1
fi

case "$mode" in
  validate)
    ;;
  resolve)
    checkout_dir=${SOURCE_DIR:?SOURCE_DIR is required in resolve mode}
    github_env=${GITHUB_ENV:?GITHUB_ENV is required in resolve mode}
    actual=$(git -C "$checkout_dir" rev-parse --verify 'HEAD^{commit}')
    if [[ $actual != "$build_source" ]]; then
      echo "checked-out revision $actual does not match requested BUILD_SOURCE $build_source" >&2
      exit 1
    fi
    revision=$(git -C "$checkout_dir" rev-parse --short=12 HEAD)
    {
      printf 'BUILD_SOURCE_SHA=%s\n' "$actual"
      printf 'REVISION=%s\n' "$revision"
      printf 'PROFILE_TAG=prof-%s\n' "$actual"
    } >> "$github_env"
    ;;
  *)
    echo "usage: $0 {validate|resolve}" >&2
    exit 2
    ;;
esac
