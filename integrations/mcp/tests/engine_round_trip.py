"""Owned real-engine proof, invoked only by the signed PostgreSQL Rust fixture.

No network default: requires explicit synthetic loopback URL/token. Uses actual
supported STDIO tools plus direct HTTP, never mock tool return values.
"""
import asyncio
import json
import os
import sys

import httpx
from mcp import Client, StdioServerParameters


async def main():
    endpoint = os.environ["SIGLAKE_SYNTHETIC_ENGINE"]
    token = os.environ["SIGLAKE_SYNTHETIC_TOKEN"]
    assert endpoint.startswith("http://127.0.0.1:")
    async with httpx.AsyncClient() as http:
        raw = await http.post(endpoint + "/api/v1/sql", headers={"Authorization": "Bearer " + token},
                              json={"query": "SELECT 1 AS n", "default_order": False})
        raw.raise_for_status()
        value = raw.json()
        assert value["rows"] == [{"n": 1}]
        assert value["controls"]["version"] == 1
        assert value["controls"]["mandatory_timeout"]
        assert value["controls"]["operator_generation"] == 9007199254740993
        assert "gen=9007199254740993;" in raw.headers["x-siglake-query-controls"]
        assert "x-siglake-query-controls" in raw.headers
    params = StdioServerParameters(command=sys.executable,
        args=["-m", "siglake_mcp.server", "--endpoint", endpoint],
        env={"PATH": os.environ.get("PATH", ""), "PYTHONPATH": os.environ["PYTHONPATH"],
             "SIGLAKE_TOKEN": token, "HOME": os.environ["HOME"],
             "AWS_EC2_METADATA_DISABLED": "true", "PYTHONDONTWRITEBYTECODE": "1"})
    async with Client(params, read_timeout_seconds=15) as client:
        assert len((await client.list_tools()).tools) == 4
        result = await client.call_tool("query_sql", {"query": "SELECT 1 AS n", "max_rows": 1})
        assert not result.is_error, result
        data = result.structured_content
        assert data["rows"] == [{"n": 1}]
        controls = data["execution"]["controls"]
        assert controls["policy_revision"] == value["controls"]["policy_revision"]
        assert controls["operator_generation"] == 9007199254740993
        assert controls["effective_timeout_ms"] == value["controls"]["effective_timeout_ms"]
        assert controls["warnings"] == value["controls"]["warnings"]
        result = await client.call_tool("explain_query", {"query": "SELECT 1 AS n"})
        assert not result.is_error, result
        assert result.structured_content["result"]["controls"]["mode"] == "planning"
        assert result.structured_content["result"]["estimated_bytes_scanned"] == 0
        rejected = await client.call_tool("query_sql", {"query": "SELECT 1", "max_rows": 10001})
        assert rejected.is_error
    print(json.dumps({"actual_stdio": True, "direct_http": True, "tools": 4,
                      "revision": controls["policy_revision"], "mandatory_timeout": True,
                      "exact_integer_generation": controls["operator_generation"]}))


if __name__ == "__main__":
    asyncio.run(main())
