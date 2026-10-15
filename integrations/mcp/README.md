# Siglake MCP server

Connect Claude Code, Claude Desktop or Codex to a running Siglake query server.
Ask questions about your logs using schema discovery and DataFusion SQL. This
adapter runs locally over MCP stdio and sends requests to Siglake's HTTP API.
It does not require a new Siglake engine build.

## Install

Requires Python 3.11 or newer. From a checkout of this repository:

```sh
python3 -m venv .venv-mcp
.venv-mcp/bin/python -m pip install ./integrations/mcp
.venv-mcp/bin/siglake-mcp --help
```

Use the **absolute path** to the installed `siglake-mcp` executable in your
client configuration. The examples below use `/path/to/.venv-mcp/bin/siglake-mcp`;
replace that placeholder with your installation path. A registry release is
not required; install directly from the checkout.

## Connect Codex

```sh
codex mcp add siglake -- /path/to/.venv-mcp/bin/siglake-mcp \
  --endpoint http://127.0.0.1:8089
codex mcp list
```

Alternatively, add to your Codex configuration:

```toml
[mcp_servers.siglake]
command = "/path/to/.venv-mcp/bin/siglake-mcp"
args = ["--endpoint", "http://127.0.0.1:8089"]
env_vars = ["SIGLAKE_TOKEN", "SIGLAKE_TENANT"]
tool_timeout_sec = 35
```

## Connect Claude Code

```sh
claude mcp add --transport stdio siglake -- \
  /path/to/.venv-mcp/bin/siglake-mcp --endpoint http://127.0.0.1:8089
claude mcp get siglake
```

Restart the client session after adding the server. Use `/mcp` to inspect its
connection and available tools. For a team configuration, Claude Code also
supports the following entry in a project's `.mcp.json`.

## Connect Claude Desktop

Add this entry to the desktop app's MCP configuration and restart the app:

```json
{
  "mcpServers": {
    "siglake": {
      "command": "/path/to/.venv-mcp/bin/siglake-mcp",
      "args": ["--endpoint", "http://127.0.0.1:8089"]
    }
  }
}
```

The executable and endpoint must be reachable from the computer running the
client. A port-forward on another computer is not your local `127.0.0.1`.
This version uses stdio; a hosted web connector or remote OAuth MCP endpoint
is not part of this adapter.

## Tools

| Tool | Purpose |
| --- | --- |
| `list_tables` | List this tenant's managed indexes and event-time fields; supports paging. |
| `describe_table` | Return the declared mapping, types, timestamp field and quoted SQL identifier without scanning logs. |
| `query_sql` | Execute one read-only SELECT or SELECT-based CTE query and return rows, execution metadata and truncation status. |
| `explain_query` | Use the SQL API's `dry_run` option to estimate a query without executing it. |

The `siglake://query-guide` resource provides short SQL and observability
guidance. Discovery describes declared index mappings; automatically promoted
columns may also exist. It does not enumerate SQL system tables.

Try:

> What log tables are available, and which fields can I use to investigate errors?

> For checkout between 14:05 and 14:15 UTC on October 13, compare error rates
> and p95 latency before and after 14:10. Include request counts and identify
> which hosts and versions account for the change.

Use times, service names and fields that exist in your dataset. If using
synthetic demo data, ask the client to label it as synthetic.

## Configuration and behavior

- `--endpoint` or `SIGLAKE_ENDPOINT`: query API base URL; default
  `http://127.0.0.1:8089`. HTTPS, including a reverse-proxy path prefix, is supported.
- `SIGLAKE_TOKEN`: optional bearer token, passed only to the configured API.
  Supply it through your client's secret/environment mechanism. The server
  does not accept a token command-line flag or print it in help.
- `--tenant` or `SIGLAKE_TENANT`: optional `X-Scope-OrgID` header. Server-side
  authentication and tenant authorization remain authoritative; this is not
  an impersonation mechanism or an additional authorization layer.
- `--timeout-seconds`: 25 by default, maximum 120. Each query asks Siglake for
  this deadline, and the adapter also applies an overall HTTP deadline.
- `--max-rows`: maximum tool-request row cap, default 1,000. Each query defaults
  to 100 rows. Truncated results are explicitly marked. Row caps do not bound
  scanned data; include a time predicate and use `explain_query` when useful.
- `--max-response-bytes`: bounded decoded HTTP response, default 2 MiB. A larger
  response is refused with an instruction to narrow the query, never partially
  parsed or silently treated as complete.

The adapter validates the SQL syntax tree and refuses multiple statements,
writes, DDL, COPY, commands, locking clauses and writes inside CTEs. It forwards
accepted SQL unchanged and asks for exact answers with implicit ordering off.
Use `ORDER BY` explicitly for top-N and recent-log queries. SQL dialect support
is limited to statements the bundled parser can validate; unsupported syntax
is refused rather than bypassing validation. Use server credentials scoped
for the intended read access: MCP tool annotations alone are not authorization.

There are no automatic retries and no response cache. Siglake's own caching
and freshness rules still apply. Authentication failures, unreachable endpoints,
timeouts and oversized responses return tool errors. Redirects are not followed.
Logs and protocol diagnostics use stderr; stdout is reserved for MCP.

## Development

```sh
.venv-mcp/bin/python -m pip install -e './integrations/mcp[test]'
.venv-mcp/bin/python -m pytest integrations/mcp/tests
```

Tests cover the wire protocol, HTTP mapping, authentication/tenant forwarding,
SQL restrictions, limits and error handling. They use a local fake HTTP server;
they do not require cloud credentials or a live Siglake cluster.

Client setup follows the official [Codex MCP documentation](https://developers.openai.com/codex/mcp)
and [Claude Code MCP documentation](https://code.claude.com/docs/en/mcp).
The server uses the official [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk).
