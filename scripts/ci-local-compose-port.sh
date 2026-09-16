#!/usr/bin/env bash
# Host-port selection for ci-local.sh's Docker job.
#
# Sourced by ci-local.sh and exercised without containers by
# check-compose-preflight.sh. The kernel's ephemeral allocator can claim a
# port returned by a bind-to-zero probe before compose starts, so candidates
# come from below ip_local_port_range instead.

ci_local_choose_compose_ingest_port() { # <log>
  local log=$1 port

  port=$(python3 - \
    "${SIGLAKE_PG_HOST_PORT:-}" \
    "${SIGLAKE_MINIO_HOST_PORT:-}" \
    "${SIGLAKE_MINIO_CONSOLE_HOST_PORT:-}" \
    "${SIGLAKE_OTLP_GRPC_HOST_PORT:-}" \
    "${SIGLAKE_INGEST_METRICS_HOST_PORT:-}" \
    "${SIGLAKE_COMPACTOR_METRICS_HOST_PORT:-}" \
    "${SIGLAKE_QUERY_HOST_PORT:-}" \
    "${SIGLAKE_QUERY_METRICS_HOST_PORT:-}" \
    "${SIGLAKE_PROMETHEUS_HOST_PORT:-}" \
    "${SIGLAKE_GARAGE_HOST_PORT:-}" \
    "${SIGLAKE_GARAGE_ADMIN_HOST_PORT:-}" <<'PY'
import random
import socket
import sys

try:
    with open("/proc/sys/net/ipv4/ip_local_port_range") as fh:
        ephemeral_low = int(fh.read().split()[0])
except (OSError, ValueError, IndexError):
    ephemeral_low = 32768

excluded = {int(value) for value in sys.argv[1:] if value.isdigit()}
start = 20000 if ephemeral_low > 20000 else 1024
candidates = [
    port for port in range(start, ephemeral_low) if port not in excluded
]
random.shuffle(candidates)

for candidate in candidates:
    try:
        with socket.socket() as probe:
            probe.bind(("0.0.0.0", candidate))
    except OSError:
        continue
    print(candidate)
    break
else:
    raise SystemExit("no free host port below the ephemeral range")
PY
  ) || {
    echo "docker compose port selection failed: no free ingest host port below ip_local_port_range" >>"$log"
    return 1
  }

  export SIGLAKE_INGEST_HOST_PORT=$port
  echo "==> docker compose selected SIGLAKE_INGEST_HOST_PORT=$port" >>"$log"
}
