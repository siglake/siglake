# Shared host-side settings for deploy/docker-compose.yml.
# shellcheck shell=bash

: "${SIGLAKE_COMPOSE_PROJECT:=siglake-dev}"
: "${SIGLAKE_PG_HOST_PORT:=5433}"
: "${SIGLAKE_MINIO_HOST_PORT:=9000}"
: "${SIGLAKE_MINIO_CONSOLE_HOST_PORT:=9001}"
: "${SIGLAKE_INGEST_HOST_PORT:=8088}"
: "${SIGLAKE_OTLP_GRPC_HOST_PORT:=4317}"
: "${SIGLAKE_INGEST_METRICS_HOST_PORT:=9100}"
: "${SIGLAKE_COMPACTOR_METRICS_HOST_PORT:=9101}"
: "${SIGLAKE_QUERY_HOST_PORT:=8089}"
: "${SIGLAKE_QUERY_METRICS_HOST_PORT:=9105}"
: "${SIGLAKE_PROMETHEUS_HOST_PORT:=9090}"

# Which object store the siglake services talk to. `minio` is the default and
# the only store any shipping default selects; `garage` is the opt-in arm of the
# 0.2.0 comparison (task #2958) and lives behind compose profile `garage`.
# MinIO still starts in the garage arm — the siglake services declare a static
# `depends_on` on it — so a matched throughput slice stops it after the stack is
# healthy.
: "${SIGLAKE_OBJECT_STORE:=minio}"
: "${SIGLAKE_GARAGE_HOST_PORT:=3900}"
: "${SIGLAKE_GARAGE_ADMIN_HOST_PORT:=3903}"

# Throwaway loopback credentials, the same posture as minioadmin/minioadmin.
# The key id follows the `GK` + 32 hex shape Garage's own quick start uses.
: "${SIGLAKE_GARAGE_ACCESS_KEY:=GK0123456789abcdef0123456789abcdef}"
: "${SIGLAKE_GARAGE_SECRET_KEY:=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef}"

# Print the compose profiles a store needs, one per line, or nothing. Pure: the
# store name is the only input, so the preflight regression can drive it.
siglake_compose_store_profiles() {
  case "$1" in
    garage) printf 'garage\n' ;;
    minio) ;;
    *) return 1 ;;
  esac
}

# Print `endpoint<TAB>host_endpoint<TAB>access_key<TAB>secret_key` for a store.
# Pure but for the host port and credential overrides read from the
# environment, which are themselves resolved above.
siglake_compose_store_settings() {
  case "$1" in
    garage)
      printf '%s\t%s\t%s\t%s\n' \
        "http://garage:3900" \
        "http://localhost:$SIGLAKE_GARAGE_HOST_PORT" \
        "$SIGLAKE_GARAGE_ACCESS_KEY" \
        "$SIGLAKE_GARAGE_SECRET_KEY"
      ;;
    minio)
      printf '%s\t%s\t%s\t%s\n' \
        "http://minio:9000" \
        "http://localhost:$SIGLAKE_MINIO_HOST_PORT" \
        minioadmin minioadmin
      ;;
    *) return 1 ;;
  esac
}

if siglake_compose_store_settings "$SIGLAKE_OBJECT_STORE" >/dev/null 2>&1; then
  IFS=$'\t' read -r _siglake_s3_endpoint _siglake_s3_host_endpoint \
    _siglake_s3_access_key _siglake_s3_secret_key \
    < <(siglake_compose_store_settings "$SIGLAKE_OBJECT_STORE")
  : "${SIGLAKE_S3_ENDPOINT:=$_siglake_s3_endpoint}"
  : "${SIGLAKE_S3_HOST_ENDPOINT:=$_siglake_s3_host_endpoint}"
  : "${SIGLAKE_S3_ACCESS_KEY:=$_siglake_s3_access_key}"
  : "${SIGLAKE_S3_SECRET_KEY:=$_siglake_s3_secret_key}"
  unset _siglake_s3_endpoint _siglake_s3_host_endpoint \
    _siglake_s3_access_key _siglake_s3_secret_key
  COMPOSE_PROFILES="${COMPOSE_PROFILES:-$(siglake_compose_store_profiles "$SIGLAKE_OBJECT_STORE")}"
  export COMPOSE_PROFILES
fi
# An unknown store leaves the S3 variables unset on purpose: the preflight
# below names it, and compose's own `${VAR:-default}` keeps MinIO's values, so
# a typo cannot silently point the stack somewhere else.
: "${SIGLAKE_S3_REGION:=us-east-1}"

export SIGLAKE_OBJECT_STORE
export SIGLAKE_GARAGE_HOST_PORT
export SIGLAKE_GARAGE_ADMIN_HOST_PORT
export SIGLAKE_GARAGE_ACCESS_KEY
export SIGLAKE_GARAGE_SECRET_KEY
export SIGLAKE_S3_ENDPOINT
export SIGLAKE_S3_HOST_ENDPOINT
export SIGLAKE_S3_ACCESS_KEY
export SIGLAKE_S3_SECRET_KEY
export SIGLAKE_S3_REGION

export SIGLAKE_COMPOSE_PROJECT
export SIGLAKE_PG_HOST_PORT
export SIGLAKE_MINIO_HOST_PORT
export SIGLAKE_MINIO_CONSOLE_HOST_PORT
export SIGLAKE_INGEST_HOST_PORT
export SIGLAKE_OTLP_GRPC_HOST_PORT
export SIGLAKE_INGEST_METRICS_HOST_PORT
export SIGLAKE_COMPACTOR_METRICS_HOST_PORT
export SIGLAKE_QUERY_HOST_PORT
export SIGLAKE_QUERY_METRICS_HOST_PORT
export SIGLAKE_PROMETHEUS_HOST_PORT

siglake_compose_port_is_listening() {
  local port=$1 hex
  local -a tables=()

  if [ -r /proc/net/tcp ]; then
    tables+=(/proc/net/tcp)
    [ ! -r /proc/net/tcp6 ] || tables+=(/proc/net/tcp6)
    printf -v hex '%04X' "$((10#$port))"
    awk -v port="$hex" '
      $4 == "0A" {
        split($2, address, ":")
        if (toupper(address[2]) == port) found = 1
      }
      END { exit !found }
    ' "${tables[@]}"
    return
  fi

  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
  elif command -v ss >/dev/null 2>&1; then
    ss -H -ltn "sport = :$port" 2>/dev/null | grep -q .
  else
    # Bash's /dev/tcp is the last-resort portable probe. It briefly connects
    # to a listener, but avoids letting a compose build run for minutes before
    # Docker reports the collision.
    (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null
  fi
}

siglake_compose_port_owned_by_project() {
  local port=$1 published
  published=$(docker ps \
    --filter "label=com.docker.compose.project=$SIGLAKE_COMPOSE_PROJECT" \
    --format '{{.Ports}}' 2>/dev/null) || return 1
  grep -Fq ":${port}->" <<<"$published"
}

siglake_compose_port_holder() {
  local port=$1 holder

  holder=$(docker ps --format '{{.Names}}|{{.Ports}}' 2>/dev/null \
    | awk -F '|' -v needle=":${port}->" \
      'index($2, needle) { print "container " $1 " (" $2 ")"; exit }') || true
  if [ -n "$holder" ]; then
    printf '%s\n' "$holder"
    return
  fi

  if command -v lsof >/dev/null 2>&1; then
    holder=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null \
      | awk 'NR == 2 { printf "process %s (pid %s, user %s)", $1, $2, $3 }') || true
  fi
  if [ -z "$holder" ] && command -v ss >/dev/null 2>&1; then
    holder=$(ss -H -ltnp "sport = :$port" 2>/dev/null | head -1) || true
    [ -z "$holder" ] || holder="process $holder"
  fi

  printf '%s\n' "${holder:-an unknown process}"
}

siglake_compose_preflight() {
  local spec variable port holder
  local -a ports=(
    "SIGLAKE_PG_HOST_PORT:$SIGLAKE_PG_HOST_PORT"
    "SIGLAKE_MINIO_HOST_PORT:$SIGLAKE_MINIO_HOST_PORT"
    "SIGLAKE_MINIO_CONSOLE_HOST_PORT:$SIGLAKE_MINIO_CONSOLE_HOST_PORT"
    "SIGLAKE_INGEST_HOST_PORT:$SIGLAKE_INGEST_HOST_PORT"
    "SIGLAKE_OTLP_GRPC_HOST_PORT:$SIGLAKE_OTLP_GRPC_HOST_PORT"
    "SIGLAKE_INGEST_METRICS_HOST_PORT:$SIGLAKE_INGEST_METRICS_HOST_PORT"
    "SIGLAKE_COMPACTOR_METRICS_HOST_PORT:$SIGLAKE_COMPACTOR_METRICS_HOST_PORT"
    "SIGLAKE_QUERY_HOST_PORT:$SIGLAKE_QUERY_HOST_PORT"
    "SIGLAKE_QUERY_METRICS_HOST_PORT:$SIGLAKE_QUERY_METRICS_HOST_PORT"
    "SIGLAKE_PROMETHEUS_HOST_PORT:$SIGLAKE_PROMETHEUS_HOST_PORT"
  )

  if ! siglake_compose_store_profiles "$SIGLAKE_OBJECT_STORE" >/dev/null 2>&1; then
    echo "compose port preflight failed: SIGLAKE_OBJECT_STORE must be 'minio' or 'garage' (got '$SIGLAKE_OBJECT_STORE')" >&2
    return 1
  fi
  if [ "$SIGLAKE_OBJECT_STORE" = garage ]; then
    ports+=(
      "SIGLAKE_GARAGE_HOST_PORT:$SIGLAKE_GARAGE_HOST_PORT"
      "SIGLAKE_GARAGE_ADMIN_HOST_PORT:$SIGLAKE_GARAGE_ADMIN_HOST_PORT"
    )
  fi

  # Every port number is validated before any of them is probed, so a typo is
  # reported as a typo rather than as whatever the earlier ports happen to
  # collide with on this host.
  for spec in "${ports[@]}"; do
    variable=${spec%%:*}
    port=${spec#*:}
    if [[ ! $port =~ ^[1-9][0-9]{0,4}$ ]] || [ "$port" -gt 65535 ]; then
      echo "compose port preflight failed: $variable must be a port from 1 to 65535 (got '$port')" >&2
      return 1
    fi
  done

  for spec in "${ports[@]}"; do
    variable=${spec%%:*}
    port=${spec#*:}
    if siglake_compose_port_is_listening "$port" \
      && ! siglake_compose_port_owned_by_project "$port"; then
      holder=$(siglake_compose_port_holder "$port")
      echo "compose port preflight failed: host port $port ($variable) is already in use by $holder" >&2
      echo "  set $variable to an unused port, or stop the holder before retrying" >&2
      return 1
    fi
  done

  echo "==> compose host-port preflight ok (object store: $SIGLAKE_OBJECT_STORE)"
}
