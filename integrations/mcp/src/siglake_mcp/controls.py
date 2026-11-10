"""Strict bounded engine metadata, not a client-side policy resolver.

Python's JSON integers are arbitrary precision: keep signed-64 transport values
as integers, never coerce them through a float/JavaScript Number.
"""
from __future__ import annotations

import json
import re


class ControlsError(ValueError):
    pass


WARNINGS = ("timeout_opt_out_ignored", "scan_warning_exceeded", "scan_estimate_incomplete",
            "scan_estimate_unavailable", "scan_threshold_not_evaluated", "output_cap_unavailable")
ENUMS = {
    "state": {"applied", "inherited", "unavailable"},
    "priority": {"interactive", "batch"},
    "mode": {"request", "planning", "batch_job", "unknown"},
    "scope": {"request", "handoff", "queue_run", "unknown"},
    "estimate_status": {"complete", "incomplete", "unavailable"},
}
NUMBERS = ("policy_revision", "operator_generation", "effective_timeout_ms", "effective_output_rows",
           "scan_bytes_warning", "estimated_bytes")
BOOLS = ("mandatory_timeout", "output_cap_enforced")
HEADER_FIELDS = ("v", "state", "rev", "gen", "priority", "mode", "scope", "mandatory", "timeout_ms",
                 "output_rows", "output_enforced", "scan_warning", "estimate", "exact", "estimated_bytes")
KEYS = {"version", "warnings", "estimate_exact", *ENUMS, *NUMBERS, *BOOLS}


def validate(value: object) -> dict:
    if not isinstance(value, dict) or set(value) != KEYS:
        raise ControlsError()
    if type(value["version"]) is not int or value["version"] != 1:
        raise ControlsError()
    for key, allowed in ENUMS.items():
        if not isinstance(value[key], str) or value[key] not in allowed:
            raise ControlsError()
    for key in NUMBERS:
        n = value[key]
        if n is None and key != "operator_generation":
            continue
        if type(n) is not int or not 0 <= n <= 9223372036854775807:
            raise ControlsError()
        if key in {"operator_generation", "policy_revision", "scan_bytes_warning"} and n == 0:
            raise ControlsError()
    for key in BOOLS:
        if type(value[key]) is not bool:
            raise ControlsError()
    if value["estimate_exact"] is not None and type(value["estimate_exact"]) is not bool:
        raise ControlsError()
    warnings = value["warnings"]
    if not isinstance(warnings, list) or len(warnings) > 6 or any(not isinstance(w, str) or w not in WARNINGS for w in warnings):
        raise ControlsError()
    if warnings != [w for w in WARNINGS if w in warnings]:
        raise ControlsError()
    if value["state"] == "unavailable":
        if any(value[k] is not None for k in ("policy_revision", "effective_timeout_ms", "effective_output_rows", "scan_bytes_warning")) or any(value[k] for k in BOOLS):
            raise ControlsError()
    elif not value["mandatory_timeout"] or value["effective_timeout_ms"] is None:
        raise ControlsError()
    if value["state"] == "applied" and value["policy_revision"] is None:
        raise ControlsError()
    if value["output_cap_enforced"] and value["effective_output_rows"] is None:
        raise ControlsError()
    if value["estimate_status"] == "unavailable" and (value["estimate_exact"] is not None or value["estimated_bytes"] is not None):
        raise ControlsError()
    if value["estimate_status"] == "complete" and value["estimated_bytes"] is None:
        raise ControlsError()
    required = set()
    if value["estimate_status"] == "unavailable":
        required.add("scan_estimate_unavailable")
    elif value["estimate_status"] == "incomplete":
        required.add("scan_estimate_incomplete")
    threshold = value["scan_bytes_warning"]
    if threshold is not None:
        if value["estimate_status"] != "complete":
            required.add("scan_threshold_not_evaluated")
        elif value["estimated_bytes"] > threshold:
            required.add("scan_warning_exceeded")
    if value["mode"] != "planning" and not value["output_cap_enforced"]:
        required.add("output_cap_unavailable")
    if not required.issubset(warnings):
        raise ControlsError()
    if len(json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode()) > 4096:
        raise ControlsError()
    return value


def from_headers(headers) -> dict | None:
    controls = headers.get_list("x-siglake-query-controls")
    warnings = headers.get_list("x-siglake-query-warnings")
    if not controls and not warnings:
        return None
    if len(controls) != 1 or len(warnings) > 1:
        raise ControlsError()
    c, w = controls[0], warnings[0] if warnings else ""
    if not c.isascii() or not w.isascii() or len(c) > 1024 or len(w) > 512 or len(c) + len(w) + 60 > 2048:
        raise ControlsError()
    parts = c.split(";")
    if len(parts) != len(HEADER_FIELDS) or any(not p.startswith(k + "=") for p, k in zip(parts, HEADER_FIELDS)):
        raise ControlsError()
    fields = dict(p.split("=", 1) for p in parts)
    if fields["v"] != "1":
        raise ControlsError()

    def number(key):
        value = fields[key]
        if value == "na":
            return None
        if not re.fullmatch(r"[0-9]{1,19}", value):
            raise ControlsError()
        return int(value)

    def boolean(key, nullable=False):
        if nullable and fields[key] == "na":
            return None
        if fields[key] not in {"0", "1"}:
            raise ControlsError()
        return fields[key] == "1"

    return validate({"version": number("v"), "state": fields["state"], "policy_revision": number("rev"),
                     "operator_generation": number("gen"), "priority": fields["priority"], "mode": fields["mode"],
                     "scope": fields["scope"], "mandatory_timeout": boolean("mandatory"),
                     "effective_timeout_ms": number("timeout_ms"), "effective_output_rows": number("output_rows"),
                     "output_cap_enforced": boolean("output_enforced"), "scan_bytes_warning": number("scan_warning"),
                     "estimate_status": fields["estimate"], "estimate_exact": boolean("exact", True),
                     "estimated_bytes": number("estimated_bytes"), "warnings": w.split(",") if w else []})
