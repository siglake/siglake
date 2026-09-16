#!/usr/bin/env bash
#
# scripts/down.sh — tear down the local siglake stress-test environment.
#
# By default removes volumes too (postgres data, minio bucket, WAL segments).
# Pass `--keep-volumes` to preserve state across restarts.
#
# Every profile is activated for the teardown regardless of which object store
# the stack was brought up with, so a garage arm is removed by a plain
# `scripts/down.sh` rather than left behind holding ports 3900/3903.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$ROOT/deploy/docker-compose.yml"
# shellcheck source=scripts/compose-common.bash
source "$ROOT/scripts/compose-common.bash"

COMPOSE_PROFILES=garage
export COMPOSE_PROFILES

KEEP_VOLUMES=0
for arg in "$@"; do
  case "$arg" in
    --keep-volumes) KEEP_VOLUMES=1 ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown arg: $arg" >&2
      exit 1
      ;;
  esac
done

if [ "$KEEP_VOLUMES" -eq 1 ]; then
  echo "==> docker compose down (keeping volumes)"
  docker compose -p "$SIGLAKE_COMPOSE_PROJECT" -f "$COMPOSE" down --remove-orphans
else
  echo "==> docker compose down -v (removing volumes)"
  docker compose -p "$SIGLAKE_COMPOSE_PROJECT" -f "$COMPOSE" down -v --remove-orphans
fi
