#!/usr/bin/env bash
# Verify that the compose preflight rejects a live host port without Docker.

set -euo pipefail

cd "$(dirname "$0")/.."

check_dir=$(mktemp -d "${TMPDIR:-/tmp}/siglake-compose-preflight.XXXXXX")
listener_pid=
cleanup() {
  if [ -n "$listener_pid" ]; then
    kill "$listener_pid" 2>/dev/null || true
    wait "$listener_pid" 2>/dev/null || true
  fi
  rm -rf -- "$check_dir"
}
trap cleanup EXIT

# Garage refuses to start unless rpc_secret is exactly 32 bytes encoded as
# hexadecimal. Keep this check daemon-free so the shell gate catches malformed
# development fixtures before the opt-in Docker arm reaches Garage itself.
garage_rpc_secret_is_valid() {
  [[ $1 =~ ^[0-9a-fA-F]{64}$ ]]
}

malformed_garage_rpc_secret=00000000000000000000000000000000000000000000000000000000000dev01
if garage_rpc_secret_is_valid "$malformed_garage_rpc_secret"; then
  echo "FAIL garage RPC-secret check accepted the non-hex regression fixture" >&2
  exit 1
fi

mapfile -t garage_rpc_secrets < <(
  sed -n 's/^[[:space:]]*rpc_secret[[:space:]]*=[[:space:]]*"\([^"]*\)"[[:space:]]*$/\1/p' \
    deploy/garage/garage.toml
)
if [ "${#garage_rpc_secrets[@]}" -ne 1 ]; then
  echo "FAIL deploy/garage/garage.toml must contain exactly one quoted rpc_secret" >&2
  exit 1
fi
if ! garage_rpc_secret_is_valid "${garage_rpc_secrets[0]}"; then
  echo "FAIL deploy/garage/garage.toml rpc_secret must be 64 hexadecimal characters" >&2
  exit 1
fi

echo "ok (garage RPC secret is 32 bytes encoded as hexadecimal)"

mkdir "$check_dir/bin"
cat >"$check_dir/bin/docker" <<'EOF'
#!/usr/bin/env bash
# The regression test must not inspect or modify the machine's Docker state.
if [ -n "${TEST_DOCKER_CALLS:-}" ]; then
  printf '%s\n' "$*" >>"$TEST_DOCKER_CALLS"
fi
case "$*" in
  *label=com.docker.compose.project=*)
    [ -z "${TEST_PROJECT_PORTS:-}" ] || printf '%s\n' "$TEST_PROJECT_PORTS"
    ;;
  *'{{.Names}}|{{.Ports}}'*)
    [ -z "${TEST_PORT:-}" ] \
      || printf 'foreign-postgres|127.0.0.1:%s->5432/tcp\n' "$TEST_PORT"
    ;;
  *'up -d garage'*) exit "${TEST_DOCKER_GARAGE_UP_RC:-0}" ;;
  *'run --rm garage-init'*)
    echo "fake garage-init attached output"
    exit "${TEST_DOCKER_GARAGE_INIT_RC:-0}"
    ;;
  *'up --build -d'*) exit "${TEST_DOCKER_UP_RC:-0}" ;;
  *'logs --tail 50 garage'*)
    echo "fake garage diagnostics"
    exit "${TEST_DOCKER_GARAGE_LOGS_RC:-0}"
    ;;
  *'logs --tail 50'*)
    echo "fake compose diagnostics"
    exit "${TEST_DOCKER_LOGS_RC:-0}"
    ;;
esac
EOF
chmod +x "$check_dir/bin/docker"

cat >"$check_dir/bin/curl" <<'EOF'
#!/usr/bin/env bash
# A successful startup reaches one health probe, which must stay hermetic too.
exit 0
EOF
chmod +x "$check_dir/bin/curl"

python3 - <<'PY' >"$check_dir/port" &
import socket

with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    print(listener.getsockname()[1], flush=True)
    while True:
        connection, _ = listener.accept()
        connection.close()
PY
listener_pid=$!

for _ in $(seq 1 50); do
  [ ! -s "$check_dir/port" ] || break
  kill -0 "$listener_pid" 2>/dev/null || {
    echo "FAIL throwaway listener exited before reporting its port" >&2
    exit 1
  }
  sleep 0.1
done
[ -s "$check_dir/port" ] || {
  echo "FAIL throwaway listener did not report its port" >&2
  exit 1
}
port=$(<"$check_dir/port")

if output=$(PATH="$check_dir/bin:$PATH" TEST_PORT="$port" SIGLAKE_PG_HOST_PORT="$port" \
    scripts/up.sh --preflight-only 2>&1); then
  echo "FAIL compose preflight accepted listening host port $port" >&2
  exit 1
fi

expected="compose port preflight failed: host port $port (SIGLAKE_PG_HOST_PORT) is already in use by container foreign-postgres"
if [[ $output != *"$expected"* ]]; then
  echo "FAIL compose preflight did not name the occupied port and variable" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

echo "ok (occupied host port $port rejected before compose)"

# The object-store selector (task #2958) decides which credentials and endpoint
# the whole stack gets, so a typo in it must stop at the preflight rather than
# silently leaving the MinIO defaults in place.
if output=$(PATH="$check_dir/bin:$PATH" SIGLAKE_OBJECT_STORE=minioo \
    scripts/up.sh --preflight-only 2>&1); then
  echo "FAIL compose preflight accepted SIGLAKE_OBJECT_STORE=minioo" >&2
  exit 1
fi
expected="compose port preflight failed: SIGLAKE_OBJECT_STORE must be 'minio' or 'garage' (got 'minioo')"
if [[ $output != *"$expected"* ]]; then
  echo "FAIL compose preflight did not name the unknown object store" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

# And the garage arm's own published ports join the checked set only when that
# arm is selected: they are not published in the default stack, so checking
# them there would reject a host that has something unrelated on 3900. Both
# arms are driven with an out-of-range value, which the number pass catches
# before anything is probed — so the assertion holds on a box where some other
# default port is already taken.
if output=$(PATH="$check_dir/bin:$PATH" \
    SIGLAKE_OBJECT_STORE=garage SIGLAKE_GARAGE_HOST_PORT=99999 \
    scripts/up.sh --preflight-only 2>&1); then
  echo "FAIL compose preflight accepted an out-of-range garage S3 port" >&2
  exit 1
fi
expected="compose port preflight failed: SIGLAKE_GARAGE_HOST_PORT must be a port from 1 to 65535 (got '99999')"
if [[ $output != *"$expected"* ]]; then
  echo "FAIL compose preflight did not name the out-of-range garage port" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
output=$(PATH="$check_dir/bin:$PATH" SIGLAKE_OBJECT_STORE=minio \
  SIGLAKE_GARAGE_HOST_PORT=99999 \
  scripts/up.sh --preflight-only 2>&1) || true
if [[ $output == *SIGLAKE_GARAGE_HOST_PORT* ]]; then
  echo "FAIL compose preflight checked the garage ports in the default minio arm" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

echo "ok (object-store selector validated; garage ports checked only in the garage arm)"

# ci-local's Docker job must not inherit the developer port or pin the old
# 18088 assignment. Drive its selector directly, then stand in the preflight's
# listener probe so the old port is occupied and every candidate port is free.
# No container or machine Docker state is reached.
# shellcheck source=scripts/ci-local-compose-port.sh
source scripts/ci-local-compose-port.sh
# shellcheck source=scripts/compose-common.bash
source scripts/compose-common.bash
selection_log="$check_dir/selection.log"
SIGLAKE_PG_HOST_PORT=15433
SIGLAKE_MINIO_HOST_PORT=19000
SIGLAKE_MINIO_CONSOLE_HOST_PORT=19001
SIGLAKE_OTLP_GRPC_HOST_PORT=14317
SIGLAKE_INGEST_METRICS_HOST_PORT=19100
SIGLAKE_COMPACTOR_METRICS_HOST_PORT=19101
SIGLAKE_QUERY_HOST_PORT=18089
SIGLAKE_QUERY_METRICS_HOST_PORT=19105
SIGLAKE_PROMETHEUS_HOST_PORT=19090
SIGLAKE_GARAGE_HOST_PORT=13900
SIGLAKE_GARAGE_ADMIN_HOST_PORT=13903
export SIGLAKE_PG_HOST_PORT SIGLAKE_MINIO_HOST_PORT SIGLAKE_MINIO_CONSOLE_HOST_PORT
export SIGLAKE_OTLP_GRPC_HOST_PORT SIGLAKE_INGEST_METRICS_HOST_PORT
export SIGLAKE_COMPACTOR_METRICS_HOST_PORT SIGLAKE_QUERY_HOST_PORT
export SIGLAKE_QUERY_METRICS_HOST_PORT SIGLAKE_PROMETHEUS_HOST_PORT
export SIGLAKE_GARAGE_HOST_PORT SIGLAKE_GARAGE_ADMIN_HOST_PORT
ci_local_choose_compose_ingest_port "$selection_log"
selected_port=$SIGLAKE_INGEST_HOST_PORT

if [ "$selected_port" = 18088 ]; then
  echo "FAIL ci-local selected its old fixed ingest port" >&2
  exit 1
fi
ephemeral_low=$(awk '{print $1}' /proc/sys/net/ipv4/ip_local_port_range 2>/dev/null) \
  || ephemeral_low=32768
if [ "$selected_port" -ge "$ephemeral_low" ]; then
  echo "FAIL ci-local selected ephemeral-range host port $selected_port" >&2
  exit 1
fi
case " 15433 19000 19001 14317 19100 19101 18089 19105 19090 13900 13903 " in
  *" $selected_port "*)
    echo "FAIL ci-local selected another compose service's host port $selected_port" >&2
    exit 1
    ;;
esac
if ! grep -Fxq "==> docker compose selected SIGLAKE_INGEST_HOST_PORT=$selected_port" \
    "$selection_log"; then
  echo "FAIL ci-local did not log its selected ingest port" >&2
  exit 1
fi
if siglake_compose_port_is_listening "$selected_port"; then
  echo "FAIL ci-local selected listening host port $selected_port" >&2
  exit 1
fi

siglake_compose_port_is_listening() {
  [ "$1" = 18088 ]
}
if ! output=$(siglake_compose_preflight 2>&1); then
  echo "FAIL compose preflight rejected selected port $selected_port while 18088 was occupied" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

if ! grep -Fq 'ci_local_choose_compose_ingest_port "$dlog"' scripts/ci-local.sh; then
  echo "FAIL ci-local Docker job does not call the tested port selector" >&2
  exit 1
fi

echo "ok (ci-local selected and logged free ingest host port $selected_port; occupied 18088 ignored)"

# A compose startup error used to leave before the health-timeout logger could
# run. Drive the whole up script with fake Docker and curl commands so startup
# diagnostics and status propagation stay covered without a daemon.
run_up_scenario() {
  env PATH="$check_dir/bin:$PATH" \
    TEST_DOCKER_CALLS="$check_dir/docker.calls" \
    TEST_DOCKER_UP_RC="$1" TEST_DOCKER_LOGS_RC="$2" \
    TEST_PROJECT_PORTS='0.0.0.0:25331->5432/tcp, 0.0.0.0:25332->9000/tcp, 0.0.0.0:25333->9001/tcp, 0.0.0.0:25334->8088/tcp, 0.0.0.0:25335->4317/tcp, 0.0.0.0:25336->9100/tcp, 0.0.0.0:25337->9101/tcp, 0.0.0.0:25338->8089/tcp, 0.0.0.0:25339->9105/tcp, 0.0.0.0:25340->9090/tcp' \
    SIGLAKE_PG_HOST_PORT=25331 \
    SIGLAKE_MINIO_HOST_PORT=25332 \
    SIGLAKE_MINIO_CONSOLE_HOST_PORT=25333 \
    SIGLAKE_INGEST_HOST_PORT=25334 \
    SIGLAKE_OTLP_GRPC_HOST_PORT=25335 \
    SIGLAKE_INGEST_METRICS_HOST_PORT=25336 \
    SIGLAKE_COMPACTOR_METRICS_HOST_PORT=25337 \
    SIGLAKE_QUERY_HOST_PORT=25338 \
    SIGLAKE_QUERY_METRICS_HOST_PORT=25339 \
    SIGLAKE_PROMETHEUS_HOST_PORT=25340 \
    scripts/up.sh 2>&1
}

: >"$check_dir/docker.calls"
startup_rc=0
output=$(run_up_scenario 23 0) || startup_rc=$?
if [ "$startup_rc" -ne 23 ]; then
  echo "FAIL compose startup failure returned $startup_rc instead of 23" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
if [[ $output != *"docker compose startup failed; recent service logs:"* ]] \
  || [[ $output != *"fake compose diagnostics"* ]]; then
  echo "FAIL compose startup failure did not print service diagnostics" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
if ! grep -Fq 'logs --tail 50' "$check_dir/docker.calls"; then
  echo "FAIL compose startup failure did not request service logs" >&2
  exit 1
fi

: >"$check_dir/docker.calls"
startup_rc=0
output=$(run_up_scenario 24 42) || startup_rc=$?
if [ "$startup_rc" -ne 24 ]; then
  echo "FAIL diagnostic collection failure masked startup status 24 as $startup_rc" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
if ! grep -Fq 'logs --tail 50' "$check_dir/docker.calls"; then
  echo "FAIL failed diagnostic collection was not attempted" >&2
  exit 1
fi

: >"$check_dir/docker.calls"
if ! output=$(run_up_scenario 0 0); then
  echo "FAIL successful compose startup no longer completes" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
if [[ $output != *"siglake stress-test environment is up."* ]]; then
  echo "FAIL successful compose startup lost its connection summary" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
if grep -Fq 'logs --tail 50' "$check_dir/docker.calls"; then
  echo "FAIL successful compose startup requested failure diagnostics" >&2
  exit 1
fi

echo "ok (compose startup failures retain status and print best-effort service logs)"

# Garage starts in two steps before the main compose command. Cover each one
# independently because `garage-init` is an attached, removed-on-exit container:
# its own output is the only reliable initialization diagnostic after failure.
run_garage_scenario() {
  env PATH="$check_dir/bin:$PATH" \
    TEST_DOCKER_CALLS="$check_dir/docker.calls" \
    TEST_DOCKER_GARAGE_UP_RC="$1" TEST_DOCKER_GARAGE_INIT_RC="$2" \
    TEST_DOCKER_GARAGE_LOGS_RC="$3" \
    TEST_PROJECT_PORTS='0.0.0.0:25431->5432/tcp, 0.0.0.0:25432->9000/tcp, 0.0.0.0:25433->9001/tcp, 0.0.0.0:25434->8088/tcp, 0.0.0.0:25435->4317/tcp, 0.0.0.0:25436->9100/tcp, 0.0.0.0:25437->9101/tcp, 0.0.0.0:25438->8089/tcp, 0.0.0.0:25439->9105/tcp, 0.0.0.0:25440->9090/tcp, 0.0.0.0:25441->3900/tcp, 0.0.0.0:25442->3903/tcp' \
    SIGLAKE_OBJECT_STORE=garage \
    SIGLAKE_PG_HOST_PORT=25431 \
    SIGLAKE_MINIO_HOST_PORT=25432 \
    SIGLAKE_MINIO_CONSOLE_HOST_PORT=25433 \
    SIGLAKE_INGEST_HOST_PORT=25434 \
    SIGLAKE_OTLP_GRPC_HOST_PORT=25435 \
    SIGLAKE_INGEST_METRICS_HOST_PORT=25436 \
    SIGLAKE_COMPACTOR_METRICS_HOST_PORT=25437 \
    SIGLAKE_QUERY_HOST_PORT=25438 \
    SIGLAKE_QUERY_METRICS_HOST_PORT=25439 \
    SIGLAKE_PROMETHEUS_HOST_PORT=25440 \
    SIGLAKE_GARAGE_HOST_PORT=25441 \
    SIGLAKE_GARAGE_ADMIN_HOST_PORT=25442 \
    scripts/up.sh 2>&1
}

: >"$check_dir/docker.calls"
garage_rc=0
output=$(run_garage_scenario 31 0 0) || garage_rc=$?
if [ "$garage_rc" -ne 31 ]; then
  echo "FAIL garage startup failure returned $garage_rc instead of 31" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
if [[ $output != *"garage startup failed; recent garage logs:"* ]] \
  || [[ $output != *"fake garage diagnostics"* ]]; then
  echo "FAIL garage startup failure did not print garage diagnostics" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
if ! grep -Fq 'logs --tail 50 garage' "$check_dir/docker.calls"; then
  echo "FAIL garage startup failure did not request garage logs" >&2
  exit 1
fi
if grep -Fq 'run --rm garage-init' "$check_dir/docker.calls"; then
  echo "FAIL garage startup failure continued to garage-init" >&2
  exit 1
fi

: >"$check_dir/docker.calls"
garage_rc=0
output=$(run_garage_scenario 0 32 0) || garage_rc=$?
if [ "$garage_rc" -ne 32 ]; then
  echo "FAIL garage-init failure returned $garage_rc instead of 32" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
if [[ $output != *"fake garage-init attached output"* ]] \
  || [[ $output != *"garage initialization failed; recent garage logs:"* ]] \
  || [[ $output != *"fake garage diagnostics"* ]]; then
  echo "FAIL garage-init failure did not retain attached output and garage diagnostics" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

: >"$check_dir/docker.calls"
garage_rc=0
output=$(run_garage_scenario 0 33 47) || garage_rc=$?
if [ "$garage_rc" -ne 33 ]; then
  echo "FAIL garage diagnostic collection failure masked garage-init status 33 as $garage_rc" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
if ! grep -Fq 'logs --tail 50 garage' "$check_dir/docker.calls"; then
  echo "FAIL failed garage diagnostic collection was not attempted" >&2
  exit 1
fi

: >"$check_dir/docker.calls"
if ! output=$(run_garage_scenario 0 0 0); then
  echo "FAIL successful garage startup no longer completes" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
if [[ $output != *"siglake stress-test environment is up."* ]]; then
  echo "FAIL successful garage startup lost its connection summary" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
if grep -Fq 'logs --tail 50 garage' "$check_dir/docker.calls"; then
  echo "FAIL successful garage startup requested failure diagnostics" >&2
  exit 1
fi

echo "ok (garage startup failures retain status and print best-effort garage logs)"
