import asyncio
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
from mcp import Client, StdioServerParameters

from siglake_mcp.client import Settings, SiglakeClient
from siglake_mcp.server import create_server


async def test_tools_resources_and_structured_errors():
    settings = Settings()
    api = SiglakeClient(settings, httpx.MockTransport(lambda r: httpx.Response(200, json={
        "columns": ["n"], "rows": [{"n": 3}], "row_count": 1})))
    async with Client(create_server(settings, api)) as client:
        tools = (await client.list_tools()).tools
        assert {t.name for t in tools} == {"list_tables", "describe_table", "query_sql", "explain_query"}
        assert all(t.annotations.read_only_hint and not t.annotations.destructive_hint for t in tools)
        assert all(t.output_schema for t in tools)
        result = await client.call_tool("query_sql", {"query": "SELECT count(*) AS n FROM events"})
        assert not result.is_error
        assert result.structured_content["rows"] == [{"n": 3}]
        rejected = await client.call_tool("query_sql", {"query": "DELETE FROM events"})
        assert rejected.is_error
        invalid = await client.call_tool("query_sql", {"query": "SELECT 1", "max_rows": 0})
        assert invalid.is_error
        resource = await client.read_resource("siglake://query-guide")
        assert "DataFusion" in resource.contents[0].text


@pytest.fixture
def backend():
    calls = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append((self.path, body))
            response = json.dumps({"columns": ["n"], "rows": [{"n": 36000}], "row_count": 1}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


async def test_real_stdio_subprocess_and_http_round_trip(backend):
    endpoint, calls = backend
    params = StdioServerParameters(command=sys.executable,
        args=["-m", "siglake_mcp.server", "--endpoint", endpoint],
        env={"PATH": os.environ.get("PATH", "")})
    async with Client(params, read_timeout_seconds=10) as client:
        assert len((await client.list_tools()).tools) == 4
        result = await client.call_tool("query_sql", {"query": "SELECT count(*) AS n FROM events"})
        assert not result.is_error
        assert result.structured_content["rows"] == [{"n": 36000}]
    assert len(calls) == 1 and calls[0][0] == "/api/v1/sql"


@pytest.mark.parametrize("protocol", ["2024-11-05", "2025-03-26", "2025-06-18"])
async def test_legacy_client_handshake_and_tool_call(protocol, backend):
    endpoint, calls = backend
    process = await asyncio.create_subprocess_exec(sys.executable, "-m", "siglake_mcp.server", "--endpoint", endpoint,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        env={"PATH": os.environ.get("PATH", "")})
    async def send(message):
        process.stdin.write(json.dumps(message).encode() + b"\n")
        await process.stdin.drain()

    async def receive():
        return json.loads(await asyncio.wait_for(process.stdout.readline(), timeout=10))

    try:
        await send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": protocol, "capabilities": {}, "clientInfo": {"name": "compat-test", "version": "1"}}})
        initialized = await receive()
        assert initialized["result"]["protocolVersion"] == protocol
        assert initialized["result"]["serverInfo"]["name"] == "Siglake"
        await send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        await send({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        assert len((await receive())["result"]["tools"]) == 4
        await send({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
            "name": "query_sql", "arguments": {"query": "SELECT count(*) AS n FROM events"}}})
        result = (await receive())["result"]
        assert not result.get("isError")
        assert json.loads(result["content"][0]["text"])["rows"] == [{"n": 36000}]
        assert len(calls) == 1
    finally:
        process.stdin.close()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except TimeoutError:
            process.kill()
            await process.wait()
