#!/usr/bin/env bash

# POST a JSON file without expanding its contents into curl's argument list.
post_json_file() {
  local url=$1 body_file=$2
  shift 2
  curl -fsS -X POST "$url" \
    -H 'Content-Type: application/json' \
    "$@" \
    --data-binary @"$body_file"
}
