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

# Which object store backs the kind warehouse (task #6332). `minio` is the
# default and the only store a shipping default selects; `garage` is the opt-in
# comparison arm and brings its own manifest, server configuration and chart
# values overlay. The name matches the compose selector of the same name
# (scripts/compose-common.bash).
#
# The three functions below are pure -- the store name is their only input --
# so scripts/check-kind-object-store.sh can drive them without a cluster, and
# kind-up.sh and kind-round.sh resolve the selector exactly once each.

# Print `manifest<TAB>app_label<TAB>init_job<TAB>config_source<TAB>values_overlay`
# for a store, every path repo-relative. `config_source` is the file kind-up.sh
# installs as the `<store>-config` ConfigMap, empty when the store needs none.
# Returns non-zero for anything else, which is what rejects a typo.
siglake_kind_store_settings() {
  case "$1" in
    minio)
      printf '%s\t%s\t%s\t%s\t%s\n' \
        deploy/kind/manifests/minio.yaml minio minio-bucket-init '' ''
      ;;
    garage)
      printf '%s\t%s\t%s\t%s\t%s\n' \
        deploy/kind/manifests/garage.yaml garage garage-bucket-check \
        deploy/garage/garage.toml deploy/kind/values.kind.garage.yaml
      ;;
    *) return 1 ;;
  esac
}

# Resolve the selector's value, defaulting an unset or empty one to minio the
# way scripts/compose-common.bash does. Fails on an unknown store so the caller
# can name it before anything is created.
siglake_kind_object_store() {
  local store=${1:-minio}
  [ -n "$store" ] || store=minio
  siglake_kind_store_settings "$store" >/dev/null || return 1
  printf '%s\n' "$store"
}

# The chart values files for a store, repo-relative, base first. The garage
# overlay is layered on values.kind.yaml rather than copied from it, so one
# arm cannot quietly keep an old copy of the other's sizes and toggles.
siglake_kind_store_values() {
  local settings overlay
  settings=$(siglake_kind_store_settings "$1") || return 1
  overlay=${settings##*$'\t'}
  printf '%s\n' deploy/kind/values.kind.yaml
  [ -z "$overlay" ] || printf '%s\n' "$overlay"
}
