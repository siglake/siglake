#!/usr/bin/env bash
# Offline checks for the Jaeger UI recording workflow (task #2311).
#
# The workflow itself needs a container runtime and a browser, so it cannot run
# here. Two of its three parts can, and do:
#
#   1. the extractor, over the synthetic fixtures in
#      bench/jaeger-ui-recording/testdata/ -- a periodic recording yields a
#      cadence, a bursty one yields none, a sparse one yields no verdict, and a
#      source that cannot attribute sessions never yields a CADENCE_SOURCE;
#   2. the recording proxy end to end, against stand-in upstreams on free
#      loopback ports -- path rewriting, both operations shapes, session
#      attribution across requests, marks, and a refusal recorded as a refusal.
#
# What is NOT exercised: the pinned UI image and the browser. This guard only
# checks that the driver and the README agree on which image is pinned.
#
# bench/ is one of the paths scripts/make-public-tree.sh removes, so this guard
# names files that do not exist in the published tree. Like
# scripts/check-bench-ports.sh it is exempted for that (EXPECTED in
# check-public-tree.py) and skips rather than failing when they are gone.

set -euo pipefail

cd "$(dirname "$0")/.."

DIR=bench/jaeger-ui-recording
DRIVER="$DIR/record-jaeger-ui.sh"
PROXY="$DIR/recording_proxy.py"
EXTRACT="$DIR/poll_report.py"
README="$DIR/README.md"
TESTDATA="$DIR/testdata"

fail() { echo "FAIL $*" >&2; exit 1; }
contains() { case "$1" in *"$2"*) ;; *) return 1 ;; esac; }

for file in "$DRIVER" "$PROXY" "$EXTRACT" "$README" \
  "$DIR/jaeger_ui_routes.py" \
  "$TESTDATA/synthetic-periodic.jsonl" \
  "$TESTDATA/synthetic-bursty.jsonl" \
  "$TESTDATA/synthetic-sparse.jsonl" \
  "$TESTDATA/synthetic-session.har" \
  "$TESTDATA/synthetic-tracelayer.log"; do
  if [ ! -f "$file" ]; then
    echo "skipped (no $file in this tree)"
    exit 0
  fi
done

# The pinned image is the one thing a recording cannot be re-read without, and
# it is written down twice. A tag bumped in one place only would produce a
# recording whose UI version is a guess.
image=$(sed -n 's/^UI_IMAGE="${UI_IMAGE:-\(.*\)}"$/\1/p' "$DRIVER")
digest=$(sed -n 's/^UI_IMAGE_DIGEST="${UI_IMAGE_DIGEST:-\(.*\)}"$/\1/p' "$DRIVER")
[ -n "$image" ] || fail "$DRIVER does not pin UI_IMAGE"
[ -n "$digest" ] || fail "$DRIVER does not pin UI_IMAGE_DIGEST"
contains "$(<"$README")" "$image" || fail "$README does not name the pinned image $image"
contains "$(<"$README")" "$digest" || fail "$README does not name the pinned digest"
contains "$image" ":" || fail "UI_IMAGE has no tag: $image"
case $image in
*:latest) fail "UI_IMAGE is a moving tag: $image" ;;
esac

work=$(mktemp -d "${TMPDIR:-/tmp}/siglake-jaeger-ui-recording.XXXXXX")
trap 'rm -rf -- "$work"' EXIT

# --- 1. the extractor over the synthetic fixtures ---------------------------
extract() {
  local case=$1
  shift
  local rc=0
  python3 "$EXTRACT" "$@" --json-out "$work/$case.json" \
    >"$work/$case.out" 2>&1 || rc=$?
  echo "$rc" >"$work/$case.rc"
}

extract periodic --records "$TESTDATA/synthetic-periodic.jsonl"
extract bursty --records "$TESTDATA/synthetic-bursty.jsonl"
extract sparse --records "$TESTDATA/synthetic-sparse.jsonl"
extract har --har "$TESTDATA/synthetic-session.har" --label synthetic-har
extract tracelog --trace-log "$TESTDATA/synthetic-tracelayer.log"
extract periodic-required --records "$TESTDATA/synthetic-periodic.jsonl" --require-cadence
extract bursty-required --records "$TESTDATA/synthetic-bursty.jsonl" --require-cadence
extract tracelog-required --trace-log "$TESTDATA/synthetic-tracelayer.log" --require-cadence

if ! python3 - "$work" <<'PY'
import json
import pathlib
import sys

work = pathlib.Path(sys.argv[1])


def report(case):
    return json.loads((work / f"{case}.json").read_text(encoding="utf-8"))


def rc(case):
    return int((work / f"{case}.rc").read_text().strip())


def check(condition, message):
    if not condition:
        raise SystemExit(f"{message}\n  {(work / 'last.out')}")


periodic = report("periodic")
check(periodic["verdict"] == "periodic", f"periodic fixture: {periodic['verdict']}")
check(4900 <= periodic["cadence_ms"] <= 5100,
      f"periodic fixture cadence {periodic['cadence_ms']} ms is not the fixture's 5,000")
check(periodic["cadence_source"].startswith("recording:synthetic-periodic:"),
      f"periodic fixture CADENCE_SOURCE: {periodic['cadence_source']}")
check(periodic["source"]["sha256_12"] in periodic["cadence_source"],
      "CADENCE_SOURCE does not name the recording's digest")
check(str(periodic["cadence_ms"]) in periodic["probe_command"]
      and "jaeger-name-poll-probe.sh" in periodic["probe_command"],
      f"periodic fixture probe command: {periodic['probe_command']}")
check(periodic["pollers"]["sessions"] == 2,
      f"periodic fixture sessions: {periodic['pollers']}")
check(sorted(periodic["sessions"]) == ["synthetic-tab-a", "synthetic-tab-b"],
      f"periodic fixture session ids: {periodic['sessions']}")
check(rc("periodic") == 0 and rc("periodic-required") == 0,
      "a periodic recording did not satisfy --require-cadence")

bursty = report("bursty")
check(bursty["verdict"] == "bursty", f"bursty fixture: {bursty['verdict']}")
check(bursty["cadence_ms"] is None and bursty["cadence_source"] is None,
      "the bursty fixture was given a cadence")
check(rc("bursty-required") == 1,
      "--require-cadence passed on a bursty recording")
services = next(s for s in bursty["series"] if s["route"] == "services")
check(services["bursts"]["n"] == 4,
      f"bursty fixture bursts: {services['bursts']}")
check(services["bursts"]["max_requests"] == 3,
      f"bursty fixture burst size: {services['bursts']}")
check([m["label"] for m in bursty["marks"]][0] == "synthetic-open",
      f"bursty fixture marks: {bursty['marks']}")

sparse = report("sparse")
check(sparse["verdict"] == "insufficient", f"sparse fixture: {sparse['verdict']}")
check(sparse["cadence_ms"] is None, "the sparse fixture was given a cadence")

har = report("har")
check(har["verdict"] == "periodic", f"HAR fixture: {har['verdict']}")
check(har["source"]["session_attribution"] == "har_file",
      f"HAR attribution: {har['source']['session_attribution']}")
check(har["routes"]["operations"]["variants"] == ["query"],
      f"HAR operations variant: {har['routes']['operations']}")
check(set(har["routes"]) >= {"services", "operations", "traces_search",
                             "trace_by_id", "unmapped_api"},
      f"HAR routes: {sorted(har['routes'])}")
check(har["unmapped_paths"] == ["/api/dependencies"],
      f"HAR unmapped: {har['unmapped_paths']}")
check("ui_asset" not in har["routes"],
      "UI asset requests were counted without --include-assets")
check(har["pollers"]["connections_per_session"] == {
    "synthetic-page-a": 1, "synthetic-page-b": 1},
    f"HAR connections: {har['pollers']}")

# The log times the polls and cannot count the pollers, so it must not produce
# a label -- that is the whole reason the proxy exists.
log = report("tracelog")
check(log["verdict"] == "periodic", f"trace-log fixture: {log['verdict']}")
check(log["cadence_ms"] is not None, "the trace log yielded no cadence at all")
check(log["cadence_source"] is None,
      f"a trace log yielded a CADENCE_SOURCE: {log['cadence_source']}")
check(log["pollers"]["sessions"] is None,
      f"a trace log claimed a poller count: {log['pollers']}")
check(any("cannot attribute sessions" in p for p in log["problems"]),
      f"trace log problems: {log['problems']}")
check(rc("tracelog") == 1 and rc("tracelog-required") == 1,
      "the trace log's missing attribution was not an error")
check(log["routes"]["services"]["requests"] == 6
      and "ui_asset" not in log["routes"],
      f"trace log routes: {log['routes']}")
PY
then
  fail "the extractor's answer on the synthetic fixtures is wrong"
fi

# --- 2. the recording proxy end to end -------------------------------------
# Three free ports below the ephemeral range, in randomised order: a port
# probed here and bound a second later can otherwise be taken by any outgoing
# connection on the box (scripts/check-bench-ports.sh has the same comment and
# the same reason).
port_base=$(python3 -c '
import random
import socket

try:
    with open("/proc/sys/net/ipv4/ip_local_port_range") as fh:
        ephemeral_low = int(fh.read().split()[0])
except (OSError, ValueError, IndexError):
    ephemeral_low = 32768

candidates = list(range(20000, max(ephemeral_low - 4, 20004), 4))
random.shuffle(candidates)
for base in candidates:
    held = []
    try:
        for i in range(4):
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
    raise SystemExit("no block of four free ports below the ephemeral range")
')
query_port=$port_base
ui_port=$((port_base + 1))
record_port=$((port_base + 2))
driver_record_port=$((port_base + 3))

# Stand-in upstreams. The query server answers siglake's Jaeger paths only, so
# a rewrite that got the path wrong shows up as a 404 in the recording; it
# records every path it was asked for, which is what proves the rewrite.
cat >"$work/upstreams.py" <<'PY'
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

record = sys.argv[1]
query_port, ui_port = int(sys.argv[2]), int(sys.argv[3])
lock = threading.Lock()


def note(line):
    with lock, open(record, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


class Query(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        note("query " + self.path)
        path = self.path.split("?")[0]
        prefix = "/api/v1/jaeger/synthetic-index/api"
        body, status = b'{"error":"no such route"}', 404
        if path == "/healthz":
            body, status = b"ok\n", 200
        elif path == f"{prefix}/services":
            body, status = json.dumps({"data": ["svc one"], "total": 1}).encode(), 200
        elif path == f"{prefix}/services/svc%20missing/operations":
            body, status = b'{"error":"no such service"}', 404
        elif path.startswith(f"{prefix}/services/") and path.endswith("/operations"):
            body, status = json.dumps({"data": ["op-a", "op-b"], "total": 2}).encode(), 200
        elif path == f"{prefix}/traces":
            body, status = json.dumps({"data": [], "total": 0}).encode(), 200
        elif path.startswith(f"{prefix}/traces/"):
            body, status = json.dumps({"data": [], "total": 0}).encode(), 200
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


class Ui(Query):
    def do_GET(self):
        note("ui " + self.path)
        body = b"<html>synthetic jaeger ui</html>"
        self.send_response(200)
        self.send_header("content-type", "text/html")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


ui = ThreadingHTTPServer(("127.0.0.1", ui_port), Ui)
threading.Thread(target=ui.serve_forever, daemon=True).start()
ThreadingHTTPServer(("127.0.0.1", query_port), Query).serve_forever()
PY

python3 "$work/upstreams.py" "$work/upstream.log" "$query_port" "$ui_port" \
  >"$work/upstreams.out" 2>&1 &
upstreams_pid=$!
python3 "$PROXY" \
  --listen "127.0.0.1:$record_port" \
  --query-url "http://127.0.0.1:$query_port" \
  --index synthetic-index \
  --ui-origin "http://127.0.0.1:$ui_port" \
  --ui-version "$image" \
  --label offline-guard \
  --out "$work/records.jsonl" \
  >"$work/proxy.out" 2>&1 &
proxy_pid=$!
trap 'kill "$upstreams_pid" "$proxy_pid" 2>/dev/null || true; rm -rf -- "$work"' EXIT

ready=0
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:$record_port/_rec/health" >/dev/null 2>&1 &&
    curl -fsS "http://127.0.0.1:$query_port/healthz" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 0.5
done
[ "$ready" = 1 ] || fail "the proxy or the stand-in upstreams did not start: $(cat "$work/proxy.out" "$work/upstreams.out")"

# A scripted client, not a browser: one cookie jar per session, so the cookie
# the proxy sets is sent back and session continuity is exercised rather than
# assumed. The sequence covers every route the workflow's interaction steps
# reach, both operations shapes, and a service the query server refuses.
cat >"$work/client.py" <<'PY'
import http.cookiejar
import sys
import time
import urllib.request

base = f"http://127.0.0.1:{sys.argv[1]}"


def opener():
    return urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())
    )


def get(session, path, headers=None):
    request = urllib.request.Request(base + path, headers=headers or {})
    try:
        with session.open(request, timeout=30) as resp:
            resp.read()
            return resp.status
    except urllib.error.HTTPError as e:
        e.read()
        return e.code


# Tab A names its own session; the cookie carries the label onwards.
tab_a = opener()
get(tab_a, "/search?rec_session=tab-a")
for _ in range(4):
    get(tab_a, "/api/services")
    get(tab_a, "/api/services/svc%20one/operations")
    time.sleep(0.05)
get(tab_a, "/api/operations?service=svc%20one&spanKind=server")
get(tab_a, "/api/traces?service=svc+one&limit=20&lookback=1h")
get(tab_a, "/api/traces/0123456789abcdef")
get(tab_a, "/api/dependencies?endTs=1&lookback=2")
get(tab_a, "/api/services/svc%20missing/operations")

# Tab B labels itself with the header instead.
tab_b = opener()
get(tab_b, "/api/services", {"x-rec-session": "tab-b"})
get(tab_b, "/api/services")

# An unlabelled visitor: the proxy assigns an id and the cookie keeps it.
anonymous = opener()
get(anonymous, "/api/services")
get(anonymous, "/api/services")

urllib.request.urlopen(
    urllib.request.Request(base + "/_rec/mark?label=guard-step", method="POST"),
    timeout=30,
).read()
PY
python3 "$work/client.py" "$record_port" >"$work/client.out" 2>&1 ||
  fail "the scripted client failed: $(<"$work/client.out")"

# The proxy's stop line is written on SIGTERM, and the arm below reads it, so
# the recorder is stopped the way the driver stops it. The stand-in upstreams
# stay up: the driver arm needs them.
kill "$proxy_pid" 2>/dev/null || true
wait "$proxy_pid" 2>/dev/null || true

python3 "$EXTRACT" --records "$work/records.jsonl" \
  --json-out "$work/live.json" >"$work/live.out" 2>&1 || true

if ! python3 - "$work" <<'PY'
import json
import pathlib
import sys

work = pathlib.Path(sys.argv[1])
records = [json.loads(line) for line in
           (work / "records.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
upstream = (work / "upstream.log").read_text(encoding="utf-8").splitlines()
report = json.loads((work / "live.json").read_text(encoding="utf-8"))


def check(condition, message):
    if not condition:
        raise SystemExit(message)


requests = [r for r in records if r["record"] == "request"]
meta = [r for r in records if r["record"] == "meta"]
marks = [r for r in records if r["record"] == "mark"]

check(len(meta) == 2, f"the recording has {len(meta)} meta line(s), expected a start and a stop")
check(meta[0]["ui_version"].startswith("jaegertracing/"),
      f"the meta line does not record the pinned image: {meta[0]['ui_version']}")
check(meta[1]["requests"] == len(requests),
      f"the stop line counted {meta[1]['requests']} of {len(requests)} requests")
check([m["label"] for m in marks] == ["guard-step"], f"marks: {marks}")

# The rewrite: both operations shapes must reach ONE siglake path, and the
# service name must survive re-quoting.
ops = [p for p in upstream
       if p == "query /api/v1/jaeger/synthetic-index/api/services/svc%20one/operations"]
check(len(ops) == 5,
      f"the operations rewrite reached siglake {len(ops)} times as a bare path, "
      f"expected 5 (four path-shape, one query-shape): {upstream}")
check(any(p == "query /api/v1/jaeger/synthetic-index/api/traces?service=svc+one&limit=20&lookback=1h"
          for p in upstream),
      f"the trace search did not forward its query string verbatim: {upstream}")
check(any(p.startswith("query /api/v1/jaeger/synthetic-index/api/traces/0123456789abcdef")
          for p in upstream),
      "the single-trace route did not forward")
check(not any("/api/dependencies" in p for p in upstream),
      "an unmapped /api/ request was forwarded to siglake")
check(any(p.startswith("ui /search") for p in upstream),
      f"the UI asset request did not reach the UI origin: {upstream}")
check(not any("rec_session" in p for p in upstream),
      f"the recorder's own query parameter was forwarded upstream: {upstream}")

by_route = {}
for r in requests:
    by_route.setdefault(r["route"], []).append(r)
check(set(by_route) == {"services", "operations", "traces_search", "trace_by_id",
                        "unmapped_api", "ui_asset"},
      f"routes recorded: {sorted(by_route)}")
check({r["variant"] for r in by_route["operations"]} == {"path", "query"},
      "both operations shapes were not classified separately")
check(all(r["upstream"] == "proxy" for r in by_route["unmapped_api"]),
      "an unmapped request was attributed to siglake")
check(any(r["status"] == 404 for r in by_route["operations"]),
      "a refused operations request was not recorded with its status")
check(any(p for p in report["problems"] if "404" in p),
      f"the report did not flag the refusal: {report['problems']}")

sessions = {r["session"] for r in requests}
check({"tab-a", "tab-b"} <= sessions, f"labelled sessions missing: {sessions}")
check(len(sessions) == 3, f"expected three sessions (two labelled, one assigned): {sessions}")
generated = sorted(sessions - {"tab-a", "tab-b"})[0]
assigned = [r for r in requests if r["session"] == generated]
check(len(assigned) == 2 and {r["session_source"] for r in assigned} == {"new", "cookie"},
      f"the assigned session did not carry over the cookie: {assigned}")
check({r["session_source"] for r in requests if r["session"] == "tab-a"} == {"param", "cookie"},
      "the labelled session did not carry over the cookie")
check(all(r["client_port"] for r in requests), "no client port was recorded")
check(report["pollers"]["sessions"] == 3, f"report sessions: {report['pollers']}")
PY
then
  fail "the recording proxy's records or the report over them are wrong"
fi

# --- 3. the driver, without the container half -----------------------------
# START_UI=0 against the same stand-in upstreams: the preflight, the recorder's
# start and stop, the marks and the report all run. A `sg` on PATH that refuses
# proves the container command is not reached on this path -- the one thing
# START_UI=0 is supposed to guarantee.
mkdir -p "$work/bin"
cat >"$work/bin/sg" <<EOF
#!/usr/bin/env bash
echo "\$@" >>"$work/container-calls.log"
exit 1
EOF
chmod +x "$work/bin/sg"

driver_rc=0
env PATH="$work/bin:$PATH" \
  START_UI=0 \
  RECORD_SECS=8 \
  QUERY_URL="http://127.0.0.1:$query_port" \
  UI_ORIGIN="http://127.0.0.1:$ui_port" \
  RECORD_PORT="$driver_record_port" \
  INDEX=synthetic-index \
  OUT_ROOT="$work/driver" \
  LABEL=offline-guard-driver \
  timeout 120 bash "$DRIVER" >"$work/driver.out" 2>&1 &
driver_pid=$!

# Poll the recorder the driver started, inside its own window: the arm then
# covers the whole path -- preflight, recorder, marks, report -- with the
# container and the browser as the only parts left out.
driver_ready=0
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:$driver_record_port/_rec/health" >/dev/null 2>&1; then
    driver_ready=1
    break
  fi
  sleep 0.5
done
[ "$driver_ready" = 1 ] ||
  fail "the driver's recorder did not start: $(<"$work/driver.out")"
python3 "$work/client.py" "$driver_record_port" >"$work/driver-client.out" 2>&1 ||
  fail "the scripted client failed against the driver: $(<"$work/driver-client.out")"

wait "$driver_pid" || driver_rc=$?
[ "$driver_rc" -eq 0 ] ||
  fail "the driver failed with START_UI=0 (rc $driver_rc): $(<"$work/driver.out")"
[ -f "$work/container-calls.log" ] &&
  fail "START_UI=0 still ran a container command: $(<"$work/container-calls.log")"

kill "$upstreams_pid" 2>/dev/null || true

if ! python3 - "$work" <<'PY'
import json
import pathlib
import sys

work = pathlib.Path(sys.argv[1])
run = work / "driver" / "offline-guard-driver"
records = [json.loads(line) for line in
           (run / "records.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
report = json.loads((run / "report.json").read_text(encoding="utf-8"))
stdout = (work / "driver.out").read_text(encoding="utf-8")


def check(condition, message):
    if not condition:
        raise SystemExit(message)


marks = [r["label"] for r in records if r["record"] == "mark"]
# The client's own mark can land before the driver's opening one: the driver
# polls its recorder's health once a second, so this guard can see it ready
# first. Only the sitting's two marks are ordered.
check(set(marks) == {"unscripted-start", "unscripted-end", "guard-step"},
      f"the unscripted sitting's marks: {marks}")
check(marks.index("unscripted-start") < marks.index("unscripted-end"),
      f"the sitting's marks are out of order: {marks}")
requests = [r for r in records if r["record"] == "request"]
check(len(requests) >= 15, f"the driver's recorder captured {len(requests)} requests")
check({r["route"] for r in requests} >= {"services", "operations", "traces_search"},
      f"routes through the driver's recorder: {sorted({r['route'] for r in requests})}")
meta = [r for r in records if r["record"] == "meta"]
check(meta[0]["label"] == "offline-guard-driver" and meta[0]["index"] == "synthetic-index",
      f"the driver's meta line: {meta[0]}")
check("service(s) listed" in stdout, f"the driver skipped its preflight: {stdout}")
check(report["source"]["label"] == "offline-guard-driver",
      f"the driver's report label: {report['source']['label']}")
check(report["requests"] == len([r for r in requests if r["route"] != "ui_asset"]),
      f"the report counted {report['requests']} of the recorder's requests")
# The verdict is NOT asserted here: this client's intervals are whatever the
# box was doing, and a guard that required them to be bursty would be a flake.
# What must hold either way is that a label appears only alongside a periodic
# verdict and only naming this file.
if report["cadence_source"] is not None:
    check(report["verdict"] == "periodic"
          and report["source"]["sha256_12"] in report["cadence_source"],
          f"the driver's report labelled a {report['verdict']} recording: "
          f"{report['cadence_source']}")
PY
then
  fail "the driver's own recording or report is wrong"
fi

echo "ok (5 extractor fixtures over 4 readers; proxy driven end to end and the" \
  "driver run with START_UI=0 on 127.0.0.1:$port_base..; pinned image $image)"
