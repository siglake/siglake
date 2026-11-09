"""No extra test dependencies; engine/STDIO integration lives in engine_round_trip.py."""
import copy
import json
import unittest

import httpx

from siglake_mcp.client import Settings, SiglakeClient, SiglakeError
from siglake_mcp.controls import ControlsError, from_headers, validate


def sample():
    return {"version": 1, "state": "applied", "policy_revision": 9007199254740993,
            "operator_generation": 9223372036854775807, "priority": "interactive", "mode": "request",
            "scope": "request", "mandatory_timeout": True, "effective_timeout_ms": 2000,
            "effective_output_rows": 1, "output_cap_enforced": True, "scan_bytes_warning": 1,
            "estimate_status": "complete", "estimate_exact": False, "estimated_bytes": 2,
            "warnings": ["scan_warning_exceeded"]}


class ControlsValidation(unittest.TestCase):
    def test_integer_precision_fixed_schema_bounds_and_warnings(self):
        c = sample()
        self.assertEqual(validate(json.loads(json.dumps(c))), c)
        self.assertEqual(c["policy_revision"], 9007199254740993)
        for key, value in [("policy_revision", 1.0), ("policy_revision", True),
                           ("estimated_bytes", 9223372036854775808), ("warnings", ["SECRET_CANARY"]),
                           ("warnings", ["scan_warning_exceeded"] * 7), ("warnings", []), ("warnings", ["scan_warning_exceeded"] * 2),
                           ("extra", "SQL_SECRET_CANARY"), ("state", "disabled"), ("effective_timeout_ms", -1)]:
            invalid = copy.deepcopy(c)
            invalid[key] = value
            with self.assertRaises(ControlsError):
                validate(invalid)

    def test_headers_strict_ascii_framing_duplicates_and_limits(self):
        h = "v=1;state=applied;rev=9007199254740993;gen=9223372036854775807;priority=interactive;mode=request;scope=request;mandatory=1;timeout_ms=2000;output_rows=1;output_enforced=1;scan_warning=1;estimate=complete;exact=0;estimated_bytes=2"
        headers = httpx.Headers({"x-siglake-query-controls": h, "x-siglake-query-warnings": "scan_warning_exceeded"})
        self.assertEqual(from_headers(headers), sample())
        for value in [h + ";extra=SQL_SECRET_CANARY", h.replace("v=1;", "v=01;"), h + "x" * 1024, h.replace("mandatory=1", "mandatory=true"), h.replace("rev=9007199254740993", "rev=9223372036854775808")]:
            with self.assertRaises(ControlsError):
                from_headers(httpx.Headers({"x-siglake-query-controls": value}))
        with self.assertRaises(ControlsError):
            from_headers(httpx.Headers([("x-siglake-query-controls", h), ("x-siglake-query-controls", h)]))


class ControlsClient(unittest.IsolatedAsyncioTestCase):
    async def test_query_and_planning_passthrough_without_integer_rounding(self):
        for dry in [False, True]:
            native = {"columns": ["n"], "rows": [{"n": 9223372036854775807}], "row_count": 1,
                      "controls": sample()}
            client = SiglakeClient(Settings(), httpx.MockTransport(lambda r: httpx.Response(200, json=native)))
            value = await client.query("SELECT 1", 1, dry)
            controls = value["result"]["controls"] if dry else value["execution"]["controls"]
            self.assertEqual(controls, sample())
            self.assertEqual(value["result"]["rows"][0]["n"] if dry else value["rows"][0]["n"], 9223372036854775807)

    async def test_invalid_required_metadata_fails_not_silent_success(self):
        for controls in [{}, {**sample(), "warnings": ["RAW_ERROR_SECRET_CANARY"]}, {**sample(), "extra": "x" * 4097}]:
            client = SiglakeClient(Settings(), httpx.MockTransport(lambda r: httpx.Response(200, json={"columns": [], "rows": [], "controls": controls})))
            with self.assertRaises(SiglakeError) as error:
                await client.query("SELECT 1")
            self.assertNotIn("CANARY", str(error.exception))
        client = SiglakeClient(Settings(), httpx.MockTransport(lambda r: httpx.Response(200, headers={"x-siglake-query-warnings": "scan_warning_exceeded"}, json={"columns": [], "rows": []})))
        with self.assertRaises(SiglakeError):
            await client.query("SELECT 1")


if __name__ == "__main__":
    unittest.main()
