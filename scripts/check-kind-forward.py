#!/usr/bin/env python3
"""Exercise the kind round's real forwarding helpers without a cluster."""
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
ROUND = (ROOT / "scripts/kind-round.sh").read_text()
HELPERS = "\n".join(
    re.search(rf"^{name}\(\) \{{\n.*?^\}}", ROUND, re.M | re.S).group(0)
    for name in ("wait_for_forward", "start_query_pod_forward", "stop_active_forward")
)


class ForwardTests(unittest.TestCase):
    def run_shell(self, body, **extra_env):
        with tempfile.TemporaryDirectory() as directory:
            env = dict(os.environ, TMP_DIR=directory, **extra_env)
            return subprocess.run(
                ["bash", "-c", "set -euo pipefail\n" + HELPERS + "\n" + body],
                env=env, text=True, capture_output=True, timeout=10,
            )

    def test_dynamic_ipv4_listener_is_the_probe_endpoint(self):
        # A fake kubectl binds a real socket and answers a real HTTP probe. The
        # shell must pass IPv4-only/dynamic arguments and publish its actual port.
        body = r'''
KUBE_CONTEXT=test NAMESPACE=default ACTIVE_PF_PID= QUERY_LOCAL_PORT=$OCCUPIED_PORT
cat > "$TMP_DIR/kubectl" <<'STUB'
#!/usr/bin/env python3
import http.server
import sys
assert sys.argv[1:] == ['--context', 'test', '-n', 'default', 'port-forward',
                       '--address', '127.0.0.1', 'pod/test-pod', ':8089'], sys.argv
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'correct-pod')
    def log_message(self, *args):
        pass
server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
print(f'Forwarding from 127.0.0.1:{server.server_port} -> 8089', flush=True)
server.serve_forever()
STUB
chmod +x "$TMP_DIR/kubectl"
export PATH="$TMP_DIR:$PATH"
die() { echo "$*" >&2; exit 1; }
trap stop_active_forward EXIT
start_query_pod_forward test-pod QUERY_LOCAL_PORT 8089 query
[[ "$QUERY_LOCAL_PORT" != "$OCCUPIED_PORT" ]]
[[ "$(curl -fsS --max-time 2 "http://127.0.0.1:$QUERY_LOCAL_PORT/")" == correct-pod ]]
'''
        with socket.socket() as occupied:
            occupied.bind(("127.0.0.1", 0))
            occupied.listen()
            result = self.run_shell(body, OCCUPIED_PORT=str(occupied.getsockname()[1]))
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_ipv6_only_readiness_is_rejected(self):
        result = self.run_shell(r'''
die() { echo "$*" >&2; exit 1; }
sleep() { :; }
printf 'Forwarding from [::1]:18089 -> 8089\n' > "$TMP_DIR/forward.log"
wait_for_forward $$ "$TMP_DIR/forward.log"
''')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("did not become ready", result.stderr)

    def test_dead_forward_cannot_reuse_stale_ready_log(self):
        result = self.run_shell(r'''
die() { echo "$*" >&2; exit 1; }
printf 'Forwarding from 127.0.0.1:18089 -> 8089\n' > "$TMP_DIR/forward.log"
true &
pid=$!
wait "$pid"
wait_for_forward "$pid" "$TMP_DIR/forward.log"
''')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exited before becoming ready", result.stderr)

    def test_every_forward_is_ipv4_only_and_dynamic(self):
        calls = re.findall(r'^kubectl.*?(?<!\\)\n|^  kubectl.*?(?<!\\)\n', ROUND, re.M | re.S)
        forwards = [call for call in calls if ' port-forward ' in call]
        self.assertEqual(len(forwards), 3)
        for call in forwards:
            self.assertIn('port-forward --address 127.0.0.1', call)
            self.assertRegex(call, r'"\:(?:\$\{remote_port\}|9090)"')


if __name__ == '__main__':
    unittest.main()
