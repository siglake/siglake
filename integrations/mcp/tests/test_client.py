import json

import httpx
import pytest

from siglake_mcp.client import Settings, SiglakeClient, SiglakeError, validate_sql


@pytest.mark.parametrize("query", [
    "SELECT * FROM events", "WITH x AS (SELECT 1) SELECT * FROM x",
    "SELECT 1 UNION ALL SELECT 2", "SELECT '; DROP TABLE x' AS text",
    'SELECT count(*) FROM "a-b" WHERE timestamp >= TIMESTAMP \'2026-10-13 14:00:00\'',
    "/* not a command */ SELECT approx_percentile_cont(duration_ms, 0.95) FROM events;",
])
def test_read_only_sql_is_forwarded_verbatim(query):
    assert validate_sql(query) == query


@pytest.mark.parametrize("query", [
    "", "-- comment", "SELECT 1; DELETE FROM events", "DROP TABLE events",
    "DELETE FROM events", "UPDATE events SET raw = 'x'", "CREATE TABLE x (a int)",
    "COPY events TO '/tmp/data'", "CALL admin()", "SET x = 1",
    "WITH x AS (DELETE FROM events RETURNING *) SELECT * FROM x",
    "WITH x AS (INSERT INTO events VALUES (1) RETURNING *) SELECT * FROM x",
    "SELECT * INTO backup FROM events", "SELECT * FROM events FOR UPDATE",
    "SELECT * FROM", "SELECT '" + "x" * 65536 + "'",
])
def test_mutations_and_unvalidated_sql_are_refused(query):
    with pytest.raises(SiglakeError):
        validate_sql(query)


@pytest.mark.parametrize("endpoint", ["file:///tmp/x", "https://user:password@example.com", "https://example.com?q=x", "https://example.com/#x", "http://", "http://example.com:bad"])
def test_invalid_endpoint_refused_without_echoing_secret(endpoint):
    with pytest.raises(ValueError) as err:
        Settings(endpoint=endpoint)
    assert "password" not in str(err.value)


def test_credentials_are_not_in_settings_repr():
    assert "secret-value" not in repr(Settings(token="secret-value"))


async def test_limits_auth_tenant_and_sql_are_forwarded_without_rewriting():
    seen = []
    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"columns": ["n"], "rows": [{"n": 36}], "row_count": 1,
                                         "stats": {"rows_scanned": 36}, "approximate": False})
    api = SiglakeClient(Settings(endpoint="https://example.com/prefix", token="secret", tenant="team_a"), httpx.MockTransport(handler))
    result = await api.query("SELECT count(*) AS n FROM events", 10)
    request = seen[0]
    assert str(request.url) == "https://example.com/prefix/api/v1/sql"
    assert request.headers["authorization"] == "Bearer secret"
    assert request.headers["x-scope-orgid"] == "team_a"
    body = json.loads(request.content)
    assert body["query"] == result["query"] == "SELECT count(*) AS n FROM events"
    assert body["limits"] == {"max_rows_returned": 10, "timeout_seconds": 25, "circuit_breakers": True}
    assert body["format"] == "records"
    assert body["exact"] is True and body["default_order"] is False and body["dry_run"] is False
    assert result["execution"]["stats"]["rows_scanned"] == 36
    assert result["execution"]["approximate"] is False
    assert result["truncated"] is False


async def test_invalid_query_never_reaches_backend():
    def handler(request):
        pytest.fail("mutation reached backend")
    api = SiglakeClient(Settings(), httpx.MockTransport(handler))
    with pytest.raises(SiglakeError):
        await api.query("SELECT 1; DROP TABLE events")


async def test_legacy_unbounded_response_is_clipped_and_marked():
    api = SiglakeClient(Settings(), httpx.MockTransport(lambda r: httpx.Response(200, json={
        "columns": ["n"], "rows": [{"n": n} for n in range(4)], "row_count": 4})))
    result = await api.query("SELECT n FROM events", 2)
    assert result["row_count"] == 2 and result["server_row_count"] == 4
    assert result["truncated"] is True


async def test_server_truncation_is_preserved():
    api = SiglakeClient(Settings(), httpx.MockTransport(lambda r: httpx.Response(200, json={
        "columns": [], "rows": [], "row_count": 0, "truncated": True})))
    assert (await api.query("SELECT * FROM events"))["truncated"] is True


@pytest.mark.parametrize("status", [301, 302, 400, 401, 403, 404, 413, 429, 500, 504])
async def test_http_errors_never_echo_body_or_follow_redirect(status):
    calls = []
    def handler(request):
        calls.append(request)
        return httpx.Response(status, text="sensitive upstream detail", headers={"Location": "https://other.example"})
    api = SiglakeClient(Settings(token="secret"), httpx.MockTransport(handler))
    with pytest.raises(SiglakeError) as err:
        await api.query("SELECT 1")
    assert "sensitive" not in str(err.value) and "secret" not in str(err.value)
    assert len(calls) == 1


@pytest.mark.parametrize("error", [httpx.ConnectError("secret URL"), httpx.ReadTimeout("secret URL")])
async def test_network_failures_are_actionable_and_redacted(error):
    def handler(request):
        raise error
    api = SiglakeClient(Settings(), httpx.MockTransport(handler))
    with pytest.raises(SiglakeError) as err:
        await api.query("SELECT 1")
    assert "secret URL" not in str(err.value)


async def test_oversized_response_is_refused():
    api = SiglakeClient(Settings(max_response_bytes=1024), httpx.MockTransport(lambda r: httpx.Response(200, content=b"x" * 1025)))
    with pytest.raises(SiglakeError, match="byte limit"):
        await api.query("SELECT 1")


async def test_invalid_json_is_not_a_success():
    api = SiglakeClient(Settings(), httpx.MockTransport(lambda r: httpx.Response(200, content=b"not json")))
    with pytest.raises(SiglakeError, match="invalid JSON"):
        await api.query("SELECT 1")


async def test_discovery_mapping_and_paging():
    config = {"index_id": "a-b", "doc_mapping": {"timestamp_field": "timestamp", "field_mappings": [{"name": "status", "type": "long"}]}}
    def handler(request):
        if request.url.path == "/api/v1/indexes":
            return httpx.Response(200, json=[{"index_id": "z", "doc_mapping": {}}, config])
        assert request.url.path == "/api/v1/indexes/a-b"
        return httpx.Response(200, json=config)
    api = SiglakeClient(Settings(), httpx.MockTransport(handler))
    first = await api.list_tables(limit=1)
    assert first["tables"][0]["name"] == "a-b" and first["next_offset"] == 1
    assert (await api.list_tables(offset=1, limit=1))["next_offset"] is None
    described = await api.describe_table("a-b")
    assert described["sql_identifier"] == '"a-b"'
    assert described["mapping"] == config["doc_mapping"]


async def test_dry_run_is_requested():
    def handler(request):
        assert json.loads(request.content)["dry_run"] is True
        return httpx.Response(200, json={"estimated_bytes_scanned": 1024})
    api = SiglakeClient(Settings(), httpx.MockTransport(handler))
    result = await api.query("SELECT * FROM events", dry_run=True)
    assert result["dry_run"] is True and result["result"]["estimated_bytes_scanned"] == 1024
