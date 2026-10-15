"""Bounded HTTP access and SELECT-only validation; no ingestion/admin routes."""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote, urlsplit

import httpx
import sqlglot
from sqlglot import exp


class SiglakeError(Exception):
    """An actionable error safe to return through a tool."""


@dataclass(frozen=True)
class Settings:
    endpoint: str = "http://127.0.0.1:8089"
    token: str | None = field(default=None, repr=False)
    tenant: str | None = None
    timeout_seconds: int = 25
    max_rows: int = 1000
    max_response_bytes: int = 2 * 1024 * 1024

    def __post_init__(self) -> None:
        try:
            url = urlsplit(self.endpoint)
            valid = (url.scheme in {"http", "https"} and url.hostname and not
                     (url.username or url.password or url.query or url.fragment))
            _ = url.port
        except ValueError:
            valid = False
        if not valid:
            raise ValueError("SIGLAKE_ENDPOINT must be an HTTP(S) base URL without credentials, query, or fragment")
        if self.tenant is not None and not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", self.tenant):
            raise ValueError("SIGLAKE_TENANT must contain 1–128 letters, digits, underscores or hyphens")
        if self.token is not None and (not self.token or any(ord(c) < 32 or ord(c) > 126 for c in self.token)):
            raise ValueError("SIGLAKE_TOKEN must be a nonempty printable ASCII token")
        if not 1 <= self.timeout_seconds <= 120:
            raise ValueError("timeout_seconds must be between 1 and 120")
        if not 1 <= self.max_rows <= 10000:
            raise ValueError("max_rows must be between 1 and 10000")
        if not 1024 <= self.max_response_bytes <= 16 * 1024 * 1024:
            raise ValueError("max_response_bytes must be between 1024 and 16777216")


def validate_sql(query: str) -> str:
    """Parse for validation only: forward the original DataFusion SQL verbatim."""
    if not query.strip() or len(query.encode("utf-8")) > 65536:
        raise SiglakeError("Supply a nonempty SELECT query of at most 64 KiB")
    try:
        statements = [s for s in sqlglot.parse(query, error_level=sqlglot.ErrorLevel.RAISE) if s is not None]
    except (sqlglot.errors.SqlglotError, RecursionError):
        raise SiglakeError("Could not validate SQL; use one SELECT query (a read-only WITH query is allowed)") from None
    if len(statements) != 1 or not isinstance(statements[0], exp.Query):
        raise SiglakeError("Only one read-only SELECT query is allowed; DDL, DML, COPY and commands are disabled")
    forbidden = (exp.DDL, exp.DML, exp.Into, exp.Lock, exp.Command, exp.Copy, exp.Transaction)
    if any(isinstance(node, forbidden) for node in statements[0].walk()):
        raise SiglakeError("Query contains a write, command or locking clause; only read-only SELECT is allowed")
    return query


class SiglakeClient:
    def __init__(self, settings: Settings, transport: httpx.AsyncBaseTransport | None = None):
        self.settings = settings
        self.transport = transport

    async def request(self, method: str, path: str, body: dict | None = None) -> Any:
        headers = {"Accept": "application/json"}
        if self.settings.token:
            headers["Authorization"] = "Bearer " + self.settings.token
        if self.settings.tenant:
            headers["X-Scope-OrgID"] = self.settings.tenant
        # No redirects: credentials must stay on the operator-configured origin.
        # The total deadline also bounds a server that trickles response bytes.
        try:
            async with asyncio.timeout(self.settings.timeout_seconds + 2):
                async with httpx.AsyncClient(
                    headers=headers, timeout=self.settings.timeout_seconds,
                    follow_redirects=False, transport=self.transport,
                ) as client:
                    async with client.stream(method, self.settings.endpoint.rstrip("/") + path, json=body) as response:
                        if response.is_redirect:
                            raise SiglakeError("Siglake returned a redirect; configure its final API base URL")
                        if not response.is_success:
                            # Never reflect an arbitrary upstream error page, SQL text, or credentials.
                            hints = {401: "check SIGLAKE_TOKEN", 403: "check token permissions and SIGLAKE_TENANT",
                                     404: "check the endpoint and table name", 422: "check compatibility with the query API request format", 400: "check DataFusion SQL syntax and schema",
                                     413: "narrow the time range or returned columns", 429: "server is busy; try later",
                                     504: "narrow the time range or simplify the query"}
                            raise SiglakeError(f"Siglake HTTP {response.status_code}: " + hints.get(response.status_code, "check query-server logs"))
                        data = bytearray()
                        async for chunk in response.aiter_bytes():
                            if len(data) + len(chunk) > self.settings.max_response_bytes:
                                raise SiglakeError("Response exceeds the MCP byte limit; select fewer columns, smaller fields or fewer rows")
                            data.extend(chunk)
                        try:
                            return json.loads(data)
                        except (ValueError, UnicodeError):
                            raise SiglakeError("Siglake returned an invalid JSON response") from None
        except (TimeoutError, httpx.TimeoutException):
            raise SiglakeError("Siglake request timed out; check connectivity or narrow the query. No automatic retry was made") from None
        except httpx.RequestError:
            raise SiglakeError("Could not reach Siglake; check the configured endpoint and any local tunnel") from None

    async def list_tables(self, offset: int = 0, limit: int = 100) -> dict:
        if offset < 0 or not 1 <= limit <= 1000:
            raise SiglakeError("offset must be nonnegative and limit must be 1–1000")
        data = await self.request("GET", "/api/v1/indexes")
        indexes = data.get("indexes") if isinstance(data, dict) else data
        if not isinstance(indexes, list) or any(not isinstance(i, dict) or not isinstance(i.get("index_id"), str) for i in indexes):
            raise SiglakeError("Unexpected index-list response from Siglake")
        indexes.sort(key=lambda i: i["index_id"])
        tables = [{"name": i["index_id"], "timestamp_field": i.get("doc_mapping", {}).get("timestamp_field")}
                  for i in indexes[offset:offset + limit]]
        return {"tables": tables, "total": len(indexes),
                "next_offset": offset + limit if offset + limit < len(indexes) else None,
                "scope": "Managed indexes returned by this tenant's API; not an inventory of every SQL system table"}

    async def describe_table(self, table: str) -> dict:
        if not table or len(table) > 512 or any(ord(c) < 32 for c in table) or table in {".", ".."}:
            raise SiglakeError("Supply a table name returned by list_tables")
        data = await self.request("GET", "/api/v1/indexes/" + quote(table, safe=""))
        if not isinstance(data, dict) or data.get("index_id") != table or not isinstance(data.get("doc_mapping"), dict):
            raise SiglakeError("Unexpected index-description response from Siglake")
        return {"table": table, "sql_identifier": '"' + table.replace('"', '""') + '"',
                "mapping": data["doc_mapping"], "retention": data.get("retention"),
                "schema_source": "Declared index mapping; dynamically promoted columns may also exist"}

    async def query(self, query: str, max_rows: int = 100, dry_run: bool = False) -> dict:
        validate_sql(query)
        if not 1 <= max_rows <= self.settings.max_rows:
            raise SiglakeError(f"max_rows must be between 1 and {self.settings.max_rows}")
        body = {"query": query, "format": "records", "priority": "interactive", "default_order": False,
                "dry_run": dry_run, "exact": True,
                "limits": {"max_rows_returned": max_rows, "timeout_seconds": self.settings.timeout_seconds,
                           "circuit_breakers": True}}
        data = await self.request("POST", "/api/v1/sql", body)
        if not isinstance(data, dict):
            raise SiglakeError("Unexpected SQL response from Siglake")
        if dry_run:
            return {"query": query, "dry_run": True, "result": data}
        if not isinstance(data.get("rows"), list) or not isinstance(data.get("columns"), list):
            raise SiglakeError("SQL response is missing rows or columns")
        # Older servers may ignore request limits. Bound the returned tool result
        # too, and report truncation without silently presenting a complete answer.
        rows = data["rows"][:max_rows]
        truncated = bool(data.get("truncated")) or len(data["rows"]) > max_rows
        return {"query": query, "columns": data["columns"], "rows": rows,
                "row_count": len(rows), "server_row_count": data.get("row_count"),
                "truncated": truncated, "max_rows": max_rows,
                "execution": {k: v for k, v in data.items() if k not in {"columns", "rows", "row_count", "truncated", "max_rows"}}}
