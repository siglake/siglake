#!/usr/bin/env bash
# Verify that the loopback bench scripts honour their port knobs and report a
# server that dies during startup, without running a benchmark.
#
# The failure this guards is expensive rather than loud: a bind collision on
# 8088/8089/9100/9105 (an unrelated engine on the host, a second bench run)
# lands in the server's own log while the script polls /healthz for two or three
# minutes and then blames readiness -- after a multi-GB corpus has already been
# generated and ingested.
#
# Stand-in binaries under a throwaway CARGO_TARGET_DIR record their argv and
# serve 200 on every address they were told to bind, so this runs the real
# scripts, on free ports, with no corpus, no cargo build and no container.

set -euo pipefail

cd "$(dirname "$0")/.."

SCRIPTS=(
  bench/local-ab.sh
  bench/pool-residual-repro.sh
  bench/admission-fanout-repro.sh
  bench/decode-cliff-sweep.sh
)

# Checked for their port DEFAULTS only, not driven through the runtime arms
# below. jaeger-name-poll-probe.sh (#2283) writes its fixture over OTLP HTTP
# instead of through `siglake-bench ingest`, and starts REPLICAS query servers
# on QUERY_PORT+i, so it fits neither the stop point the arms use nor their
# single-query-server assertions. Its defaults are still the ones a second bench
# run would collide with, which is what this loop is for.
PORT_DEFAULT_ONLY=(
  bench/jaeger-name-poll-probe.sh
)

# Ingest-only scripts: same defaults check, minus the query knobs they do not
# have. wal-mirror-ab.sh (#3758) starts one ingest server per arm and no query
# server, so the runtime arms below -- which assert on a siglake-query-server
# argv -- do not apply to it either.
INGEST_PORT_DEFAULT_ONLY=(
  bench/wal-mirror-ab.sh
)

# bench/ is one of the paths scripts/make-public-tree.sh removes, so this guard
# names files that do not exist in the published tree. Like
# scripts/check-claude-md.sh, it is the one shipping file allowed to name them
# (EXPECTED in check-public-tree.py) and it skips rather than failing when they
# are gone -- there is nothing to guard there.
for script in "${SCRIPTS[@]}" "${PORT_DEFAULT_ONLY[@]}" "${INGEST_PORT_DEFAULT_ONLY[@]}"; do
  if [ ! -f "$script" ]; then
    echo "skipped (no $script in this tree)"
    exit 0
  fi
done

# The defaults are checked in the source, not at runtime: every runtime arm
# below MOVES the ports, so a changed default would go unnoticed there. They are
# the ports every existing bench note and runbook assumes.
check_defaults() {
  local script=$1
  shift
  local pair name want
  for pair in "$@"; do
    name=${pair%%:*}
    want=${pair##*:}
    if ! grep -qF "${name}=\"\${${name}:-${want}}\"" "$script"; then
      echo "FAIL $script does not default $name to $want" >&2
      exit 1
    fi
  done
}

for script in "${SCRIPTS[@]}" "${PORT_DEFAULT_ONLY[@]}"; do
  check_defaults "$script" \
    INGEST_PORT:8088 INGEST_METRICS_PORT:9100 QUERY_PORT:8089 QUERY_METRICS_PORT:9105
done

for script in "${INGEST_PORT_DEFAULT_ONLY[@]}"; do
  check_defaults "$script" INGEST_PORT:8088 INGEST_METRICS_PORT:9100
done

check_dir=$(mktemp -d "${TMPDIR:-/tmp}/siglake-bench-ports.XXXXXX")
quickwit_pid_file="$check_dir/quickwit.pid"
cleanup() {
  if [ -s "$quickwit_pid_file" ]; then
    kill "$(<"$quickwit_pid_file")" 2>/dev/null || true
  fi
  rm -rf -- "$check_dir"
}
trap cleanup EXIT

mkdir -p "$check_dir/bin" "$check_dir/target/release"

# Records its argv, then dies on demand (STUB_DIE) or serves every --bind and
# --metrics-bind address until killed. `siglake-bench` is not a server: it
# exits 0, except for the subcommand in STUB_BENCH_FAIL, which is how an arm
# stops the script once startup has been proven.
cat >"$check_dir/bin/stub.py" <<'PY'
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

name = os.environ.get("STUB_NAME", "?")
argv = sys.argv[1:]
with open(os.environ["STUB_RECORD"], "a") as fh:
    fh.write(name + " " + " ".join(argv) + "\n")

if name in os.environ.get("STUB_DIE", "").split(","):
    print(f"{name}: error binding: Address already in use (os error 98)", file=sys.stderr)
    sys.exit(1)

if name == "siglake-bench":
    sub = argv[0] if argv else ""
    sys.exit(1 if sub == os.environ.get("STUB_BENCH_FAIL") else 0)


def flag(opt):
    return next((argv[i + 1] for i, a in enumerate(argv)
                 if a == opt and i + 1 < len(argv)), None)


# The real servers create <data-dir>/<warehouse> on startup, and
# decode-cliff-sweep.sh's file census walks it BEFORE it starts the query
# server whose ports this guard is checking -- a missing directory would stop
# the script one step too early.
data_dir, warehouse = flag("--data-dir"), flag("--warehouse")
if data_dir and warehouse:
    os.makedirs(os.path.join(data_dir, warehouse), exist_ok=True)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok\n")

    def do_POST(self):
        # Nothing here executes a query. decode-cliff-sweep.sh's probe request
        # is where that script is meant to stop, once both of its servers have
        # started on the moved ports.
        self.send_response(501)
        self.end_headers()
        self.wfile.write(b"stub: no query execution\n")

    def log_message(self, *_args):
        pass


binds = [argv[i + 1] for i, a in enumerate(argv)
         if a in ("--bind", "--metrics-bind") and i + 1 < len(argv)]
if not binds:
    sys.exit(f"{name}: no --bind in {argv}")
servers = []
for bind in binds:
    host, _, port = bind.rpartition(":")
    servers.append(ThreadingHTTPServer((host or "127.0.0.1", int(port)), Handler))
for server in servers[1:]:
    threading.Thread(target=server.serve_forever, daemon=True).start()
servers[0].serve_forever()
PY

for binary in siglake siglake-query-server siglake-bench; do
  cat >"$check_dir/target/release/$binary" <<EOF
#!/usr/bin/env bash
STUB_NAME=$binary exec python3 "$check_dir/bin/stub.py" "\$@"
EOF
  chmod +x "$check_dir/target/release/$binary"
done

# The scripts build their binaries first; the stand-ins are already there.
cat >"$check_dir/bin/cargo" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
# local-ab.sh's only container use is `sg docker -c "<cmd>"`. Its `run -d`
# starts a stand-in on Quickwit's port instead so the script gets past the
# Quickwit health wait; every other command is a no-op. Nothing here touches the
# machine's container state.
cat >"$check_dir/bin/sg" <<EOF
#!/usr/bin/env bash
if [[ "\${3:-}" == *"run -d --name"* ]]; then
  STUB_NAME=quickwit setsid python3 "$check_dir/bin/stub.py" \
    --bind "127.0.0.1:\${QUICKWIT_PORT:?}" >/dev/null 2>&1 &
  echo "\$!" >"$quickwit_pid_file"
  echo stub-container-id
fi
exit 0
EOF
chmod +x "$check_dir/bin/cargo" "$check_dir/bin/sg"

# One contiguous block of eight free ports: the peer knobs default to
# QUERY_PORT+1 / QUERY_METRICS_PORT+1, so the block cannot have holes.
#
# BELOW the ephemeral range, not inside it. The block is probed here and bound
# by the stand-ins seconds later, and every port the kernel hands out to an
# outgoing connection comes from `ip_local_port_range` -- so on a box running
# anything else (another lane's gate, cargo, a curl in one of these very
# scripts) a probed port can be taken in between. The stand-in then dies with
# EADDRINUSE the moment it binds, and the script under test correctly reports
# `exited during startup` -- which reaches this guard as an unexplained arm
# failure on a machine-dependent port. Below the range the kernel never assigns
# the port itself, so only a long-lived listener can hold one, and the probe
# sees that. The scan order is randomised so two of these guards running
# concurrently (two lanes, same box) do not pick the same block.
port_base=$(python3 -c '
import random
import socket

try:
    with open("/proc/sys/net/ipv4/ip_local_port_range") as fh:
        ephemeral_low = int(fh.read().split()[0])
except (OSError, ValueError, IndexError):
    ephemeral_low = 32768

candidates = list(range(20000, ephemeral_low - 8, 8))
if not candidates:
    # A box whose ephemeral range starts below 20008 leaves nowhere safe; fall
    # back to the old behaviour (an arbitrary free block, race and all) rather
    # than refusing to run the guard at all.
    candidates = list(range(20000, 28000, 8))
random.shuffle(candidates)
for base in candidates:
    held = []
    try:
        for i in range(8):
            s = socket.socket()
            s.bind(("127.0.0.1", base + i))
            held.append(s)
    except OSError:
        continue
    finally:
        for s in held:
            s.close()
    print(base)
    break
else:
    raise SystemExit("no block of eight free ports below the ephemeral range")
')
peer_port=$((port_base + 2))
peer_metrics_port=$((port_base + 5))
ports=(
  "INGEST_PORT=$port_base"
  "QUERY_PORT=$((port_base + 1))"
  "INGEST_METRICS_PORT=$((port_base + 3))"
  "QUERY_METRICS_PORT=$((port_base + 4))"
  "QUICKWIT_PORT=$((port_base + 6))"
  "QUICKWIT_GRPC_PORT=$((port_base + 7))"
)

# $1 = case name, $2 = script, rest = extra env. Prints nothing; leaves
# $check_dir/<case>.{log,rec}. Never fails the run itself: every arm below
# stops its script deliberately, so the exit code is checked by the caller.
run_arm() {
  local case=$1 script=$2
  shift 2
  : >"$check_dir/$case.rec"
  local rc=0
  env PATH="$check_dir/bin:$PATH" \
    CARGO_TARGET_DIR="$check_dir/target" \
    STUB_RECORD="$check_dir/$case.rec" \
    OUT_ROOT="$check_dir/$case-data" \
    "${ports[@]}" "$@" \
    timeout 60 bash "$script" >"$check_dir/$case.log" 2>&1 || rc=$?
  if [ -s "$quickwit_pid_file" ]; then
    kill "$(<"$quickwit_pid_file")" 2>/dev/null || true
    : >"$quickwit_pid_file"
  fi
  echo "$rc"
}

want() {
  # $1 = case, $2 = file suffix, $3 = grep -E pattern, $4 = what it proves
  if ! grep -qE -- "$3" "$check_dir/$1.$2"; then
    echo "FAIL $1: $4" >&2
    sed 's/^/  /' "$check_dir/$1.$2" >&2
    exit 1
  fi
}

for script in "${SCRIPTS[@]}"; do
  case=$(basename "$script" .sh)

  # What each script HAS, rather than a test on its name:
  #   peer   starts a second query server on PEER_PORT/PEER_METRICS_PORT.
  #   stop   how arm 1 is stopped once startup is proven. For three of the
  #          scripts both servers are up before `siglake-bench ingest`, the
  #          first command carrying the client URLs, so failing that subcommand
  #          stops the script exactly there. decode-cliff-sweep.sh instead
  #          ingests, kills the ingest server, takes a file census and only
  #          THEN starts the query server for its probe, so the same stop point
  #          would never reach query startup: it runs the ingest phase through
  #          (its bench `ingest` ends in `|| true` anyway) and stops at the
  #          probe's own POST /api/v1/sql, which the stand-in refuses.
  #   extra  env both arms need. The sweep's 60s compactor settle would
  #          otherwise eat the 60s timeout.
  peer=1
  stop=(STUB_BENCH_FAIL=ingest)
  extra=()
  case $script in
  bench/local-ab.sh)
    peer=0
    ;;
  bench/decode-cliff-sweep.sh)
    peer=0
    stop=()
    extra=(COMPACT_SETTLE_SECS=0)
    ;;
  esac

  # Arm 1: moved ports reach every bind AND every client URL. DISTRIBUTED=1 is
  # pool-residual-repro.sh's two-server topology and is ignored by the others.
  rc=$(run_arm "$case-moved" "$script" DISTRIBUTED=1 "${extra[@]}" "${stop[@]}")
  if [ "$rc" = 124 ]; then
    echo "FAIL $case: timed out with the ports moved (a health wait never resolved)" >&2
    sed 's/^/  /' "$check_dir/$case-moved.log" >&2
    exit 1
  fi
  want "$case-moved" rec \
    "siglake .*--bind 127\.0\.0\.1:$port_base --metrics-bind 127\.0\.0\.1:$((port_base + 3))" \
    "the ingest server did not take INGEST_PORT/INGEST_METRICS_PORT"
  want "$case-moved" rec \
    "siglake-query-server .*--bind 127\.0\.0\.1:$((port_base + 1)) --metrics-bind 127\.0\.0\.1:$((port_base + 4))" \
    "the query server did not take QUERY_PORT/QUERY_METRICS_PORT"
  want "$case-moved" rec \
    "siglake-bench ingest .*--siglake http://127\.0\.0\.1:$port_base --siglake-query http://127\.0\.0\.1:$((port_base + 1))" \
    "the bench client URLs did not follow the moved ports"
  if grep -qE '127\.0\.0\.1:(8088|8089|8090|9100|9105|9106)\b' "$check_dir/$case-moved.rec"; then
    echo "FAIL $case: a default port survived the override" >&2
    grep -nE '127\.0\.0\.1:(8088|8089|8090|9100|9105|9106)\b' "$check_dir/$case-moved.rec" >&2
    exit 1
  fi
  if [ "$peer" = 1 ]; then
    want "$case-moved" rec \
      "siglake-query-server .*--bind 127\.0\.0\.1:$peer_port --metrics-bind 127\.0\.0\.1:$peer_metrics_port" \
      "the peer query server did not follow QUERY_PORT"
    want "$case-moved" rec \
      "--query-peers http://127\.0\.0\.1:$((port_base + 1)),http://127\.0\.0\.1:$peer_port" \
      "the peer list did not follow QUERY_PORT"
  fi

  # Arm 2: a query server that exits during startup is reported at once, by a
  # script whose OTHER services are answering /healthz normally.
  rc=$(run_arm "$case-dead" "$script" "${extra[@]}" STUB_DIE=siglake-query-server)
  if [ "$rc" = 124 ]; then
    echo "FAIL $case: waited out the health poll for a query server that had exited" >&2
    exit 1
  fi
  want "$case-dead" log 'siglake-query exited during startup' \
    "a dead query server was not reported as a startup exit"
  want "$case-dead" log 'Address already in use' \
    "the dead query server's log was not tailed into the error"

  # Arm 3: the same for the INGEST server, which every script waits on with its
  # own pid and log. In the sweep it is a separate wait from the query server's
  # (they never run at the same time), and in the other three it is the wait
  # that comes first, so a shared helper regressing on one of them would not
  # show up in arm 2.
  rc=$(run_arm "$case-dead-ingest" "$script" "${extra[@]}" STUB_DIE=siglake)
  if [ "$rc" = 124 ]; then
    echo "FAIL $case: waited out the health poll for an ingest server that had exited" >&2
    exit 1
  fi
  want "$case-dead-ingest" log 'siglake-ingest exited during startup' \
    "a dead ingest server was not reported as a startup exit"
  want "$case-dead-ingest" log 'Address already in use' \
    "the dead ingest server's log was not tailed into the error"
done

echo "ok (${#SCRIPTS[@]} bench scripts: ports moved to ${port_base}.., startup death reported;" \
  "$((${#PORT_DEFAULT_ONLY[@]} + ${#INGEST_PORT_DEFAULT_ONLY[@]})) more checked for port defaults only)"
