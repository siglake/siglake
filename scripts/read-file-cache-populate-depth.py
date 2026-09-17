#!/usr/bin/env python3
"""Read clipped-browse decode depth for the decoded-file cache (#4890).

Input is one Prometheus exposition snapshot per query shape, optionally with a
baseline snapshot taken before that shape ran, so the shape's samples are a
delta and not the process total. The numbers come from two shipped series:

  siglake_query_scan_file_cache_populate_rows{outcome}   histogram, per task
  siglake_query_scan_file_cache_requests_total{outcome}  counter, per task

`populate_rows` is recorded once per population of the SHIPPED whole-file path,
when the stream is dropped: rows the reader handed it, counted before the
residual filter above the scan drops any of them, cumulative across the stream
and unaffected by the candidate being inserted or discarded. It is decode depth
offered to population, not total physical decoder work, and it exists for tasks
that populate — a task carrying a predicate or a prune spec bypasses population
(#4891) and produces no sample at all. That is why `bypass` is reported beside
the samples: a shape with no samples has either decoded nothing or been
ineligible throughout, and those have opposite readings.

The threshold is exact. The write path's MIN_ROW_GROUP_ROWS is 131,072, so a
population was handed at least one row group's worth of rows exactly when its
sample is >= 131,072 — the `le="131071"` bucket edge, which this reader
requires. Being handed that many rows is necessary, not sufficient: a file
whose groups are larger closes none of them at that depth, and the rows are
only a whole group when the read also started on a group boundary. Recorded
footer geometry (--geometry) is what turns the fraction into a statement about
group completion; without it the reader says so rather than guessing.
"""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import re
import sys
from typing import Any

DEPTH_METRIC = "siglake_query_scan_file_cache_populate_rows"
REQUESTS_METRIC = "siglake_query_scan_file_cache_requests_total"

# The write path's MIN_ROW_GROUP_ROWS. A population handed fewer rows than this
# cannot have closed a row group at the shipped floor.
ROW_GROUP_FLOOR_ROWS = 131_072
# Inclusive `le` edge for "strictly below the floor".
FLOOR_BELOW_EDGE = float(ROW_GROUP_FLOOR_ROWS - 1)

# Every outcome `CachePopulateStream` records, and how each reads.
OUTCOMES = ("completed", "clipped", "unpolled", "error")
# Populations that measure how deep a browse read before it stopped. `error` is
# a failed read and `unpolled` a task that never started, so neither belongs in
# the qualifying fraction.
QUALIFYING_OUTCOMES = ("clipped", "completed")

REQUEST_OUTCOMES = (
    "hit",
    "miss",
    "bypass",
    "insert",
    "insert_skipped_contended",
    "skip_oversized",
    "abandoned",
    "evict",
)

SAMPLE_RE = re.compile(r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?P<labels>\{.*\})?\s+(?P<value>\S+)$")
LABEL_RE = re.compile(r'(?P<key>[a-zA-Z_][a-zA-Z0-9_]*)="(?P<value>(?:[^"\\]|\\.)*)"')


class InputError(Exception):
    """The evidence cannot be read as asked."""


def parse_exposition(path: pathlib.Path) -> list[tuple[str, dict[str, str], float]]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as error:
        raise InputError(f"cannot read {path}: {error}") from error
    samples: list[tuple[str, dict[str, str], float]] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = SAMPLE_RE.match(line)
        if not match:
            raise InputError(f"{path}:{lineno}: not an exposition sample: {raw!r}")
        labels = {}
        if match.group("labels"):
            labels = {
                m.group("key"): m.group("value")
                for m in LABEL_RE.finditer(match.group("labels"))
            }
        try:
            value = float(match.group("value"))
        except ValueError as error:
            raise InputError(f"{path}:{lineno}: value is not a number: {raw!r}") from error
        samples.append((match.group("name"), labels, value))
    return samples


def depth_series(
    samples: list[tuple[str, dict[str, str], float]], path: pathlib.Path
) -> dict[str, dict[str, Any]]:
    """Per-outcome buckets/count/sum, summed over every other label (pods)."""
    out: dict[str, dict[str, Any]] = {}

    def slot(outcome: str) -> dict[str, Any]:
        return out.setdefault(outcome, {"buckets": {}, "count": 0.0, "sum": 0.0})

    for name, labels, value in samples:
        if not name.startswith(DEPTH_METRIC):
            continue
        suffix = name[len(DEPTH_METRIC) :]
        outcome = labels.get("outcome")
        if outcome is None:
            raise InputError(f"{path}: {name} carries no `outcome` label")
        if outcome not in OUTCOMES:
            raise InputError(
                f"{path}: {name} has outcome={outcome!r}, which the populate path "
                f"does not record (expected one of {', '.join(OUTCOMES)})"
            )
        entry = slot(outcome)
        if suffix == "_bucket":
            le = labels.get("le")
            if le is None:
                raise InputError(f"{path}: {name} carries no `le` label")
            edge = math.inf if le in ("+Inf", "Inf") else float(le)
            entry["buckets"][edge] = entry["buckets"].get(edge, 0.0) + value
        elif suffix == "_count":
            entry["count"] += value
        elif suffix == "_sum":
            entry["sum"] += value
        elif suffix == "":
            raise InputError(
                f"{path}: {DEPTH_METRIC} is exposed as a summary (no `_bucket` "
                "series). The recorder was built without its bucket layout, and "
                "the row-group floor cannot be read from quantiles."
            )
    return out


def request_series(samples: list[tuple[str, dict[str, str], float]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for name, labels, value in samples:
        if name != REQUESTS_METRIC:
            continue
        outcome = labels.get("outcome", "")
        out[outcome] = out.get(outcome, 0.0) + value
    return out


def subtract_depth(
    after: dict[str, dict[str, Any]], before: dict[str, dict[str, Any]], shape: str
) -> dict[str, dict[str, Any]]:
    delta: dict[str, dict[str, Any]] = {}
    for outcome, entry in after.items():
        base = before.get(outcome, {"buckets": {}, "count": 0.0, "sum": 0.0})
        buckets = {}
        for edge, value in entry["buckets"].items():
            got = value - base["buckets"].get(edge, 0.0)
            if got < 0:
                raise InputError(
                    f"{shape}: bucket le={edge} went backwards between baseline and "
                    "snapshot; the two were not taken from the same process"
                )
            buckets[edge] = got
        count = entry["count"] - base["count"]
        if count < 0:
            raise InputError(
                f"{shape}: {DEPTH_METRIC}_count for outcome={outcome} went backwards "
                "between baseline and snapshot; the two were not taken from the "
                "same process"
            )
        delta[outcome] = {
            "buckets": buckets,
            "count": count,
            "sum": entry["sum"] - base["sum"],
        }
    return delta


def subtract_requests(after: dict[str, float], before: dict[str, float], shape: str) -> dict[str, float]:
    delta = {}
    for outcome, value in after.items():
        got = value - before.get(outcome, 0.0)
        if got < 0:
            raise InputError(
                f"{shape}: {REQUESTS_METRIC}{{outcome={outcome}}} went backwards "
                "between baseline and snapshot; the two were not taken from the "
                "same process"
            )
        delta[outcome] = got
    return delta


def floor_split(entry: dict[str, Any], shape: str, outcome: str) -> tuple[float, float]:
    """(below the floor, at or above it) for one outcome's samples."""
    buckets = entry["buckets"]
    if not buckets:
        return (0.0, 0.0)
    if FLOOR_BELOW_EDGE not in buckets:
        raise InputError(
            f"{shape}: {DEPTH_METRIC}{{outcome={outcome}}} has no le=\"{int(FLOOR_BELOW_EDGE)}\" "
            "bucket, so the row-group floor cannot be read exactly. The export "
            "predates POPULATE_ROW_BUCKETS or was re-bucketed."
        )
    total = buckets.get(math.inf, entry["count"])
    below = buckets[FLOOR_BELOW_EDGE]
    return (below, max(total - below, 0.0))


def distribution(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """Per-bucket sample counts, as non-cumulative bands in edge order."""
    edges = sorted(entry["buckets"])
    bands = []
    previous = 0.0
    for edge in edges:
        cumulative = entry["buckets"][edge]
        bands.append(
            {
                "le": "+Inf" if edge == math.inf else edge,
                "cumulative": cumulative,
                "in_band": cumulative - previous,
            }
        )
        previous = cumulative
    return bands


def load_geometry(path: pathlib.Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise InputError(f"cannot read {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise InputError(f"{path} is not JSON: {error}") from error
    if isinstance(document, list):
        document = {"rows_per_group": document}
    if not isinstance(document, dict):
        raise InputError(f"{path}: geometry must be an object or a list of group row counts")
    rows = document.get("rows_per_group")
    out: dict[str, Any] = {"source": str(path)}
    if isinstance(rows, list) and rows:
        if not all(isinstance(value, int) and value > 0 for value in rows):
            raise InputError(f"{path}: rows_per_group must be positive integers")
        out["row_groups"] = len(rows)
        out["min_rows_per_group"] = min(rows)
        out["max_rows_per_group"] = max(rows)
    else:
        for key in ("min_rows_per_group", "max_rows_per_group", "row_groups", "files"):
            if key in document:
                out[key] = document[key]
        if "min_rows_per_group" not in out:
            raise InputError(
                f"{path}: geometry needs rows_per_group or min_rows_per_group — "
                "the floor alone does not say whether a group was completed"
            )
    for key in ("files", "row_groups"):
        if key in document:
            out[key] = document[key]
    return out


def geometry_statement(geometry: dict[str, Any] | None, at_or_above: float, total: float) -> str:
    if total == 0:
        return "no qualifying samples, so no statement about row-group completion"
    fraction = at_or_above / total
    if geometry is None:
        return (
            f"{at_or_above:.0f} of {total:.0f} ({fraction:.1%}) were handed at least "
            f"{ROW_GROUP_FLOOR_ROWS} rows. No footer geometry supplied: this is an "
            "upper bound on what row-group population could have closed, because a "
            "file whose groups are larger than the floor needs a deeper read, and a "
            "read that does not start on a group boundary closes nothing at any depth"
        )
    smallest = geometry["min_rows_per_group"]
    largest = geometry.get("max_rows_per_group", smallest)
    if smallest >= ROW_GROUP_FLOOR_ROWS and largest > ROW_GROUP_FLOOR_ROWS:
        return (
            f"{at_or_above:.0f} of {total:.0f} ({fraction:.1%}) reached the "
            f"{ROW_GROUP_FLOOR_ROWS}-row floor, but the measured files hold "
            f"{smallest}-{largest} rows per group, so the floor fraction is an "
            "upper bound and the group-completing fraction is at most the share "
            f"reaching {largest} rows"
        )
    return (
        f"{at_or_above:.0f} of {total:.0f} ({fraction:.1%}) reached the "
        f"{ROW_GROUP_FLOOR_ROWS}-row floor, and the measured files hold "
        f"{smallest}-{largest} rows per group"
    )


def load_stats(path: pathlib.Path) -> dict[str, Any]:
    """Per-request scan denominators from recorded /api/v1/sql responses."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise InputError(f"cannot read {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise InputError(f"{path} is not JSON: {error}") from error
    responses = document if isinstance(document, list) else [document]
    totals = {
        "responses": 0,
        "file_cache_hits": 0,
        "file_cache_misses": 0,
        "file_cache_bypasses": 0,
        "file_cache_populate_rows": 0,
    }
    for response in responses:
        if not isinstance(response, dict):
            raise InputError(f"{path}: every recorded response must be an object")
        scan = response
        for key in ("stats", "scan"):
            if isinstance(scan, dict) and key in scan and isinstance(scan[key], dict):
                scan = scan[key]
        if not isinstance(scan, dict):
            raise InputError(f"{path}: a recorded response has no stats.scan object")
        totals["responses"] += 1
        for key in list(totals):
            if key == "responses":
                continue
            value = scan.get(key, 0)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise InputError(f"{path}: stats.scan.{key} must be a non-negative integer")
            totals[key] += value
    if totals["responses"] == 0:
        raise InputError(f"{path}: no recorded responses")
    return totals


def pairs(values: list[str], flag: str) -> dict[str, pathlib.Path]:
    out: dict[str, pathlib.Path] = {}
    for value in values:
        if "=" not in value:
            raise InputError(f"{flag} takes SHAPE=PATH, got {value!r}")
        shape, path = value.split("=", 1)
        if not shape or not path:
            raise InputError(f"{flag} takes SHAPE=PATH, got {value!r}")
        if shape in out:
            raise InputError(f"{flag} names shape {shape!r} twice")
        out[shape] = pathlib.Path(path)
    return out


def read_shape(
    shape: str,
    snapshot: pathlib.Path,
    baseline: pathlib.Path | None,
    geometry: dict[str, Any] | None,
    stats: dict[str, Any] | None,
) -> dict[str, Any]:
    after_samples = parse_exposition(snapshot)
    depth = depth_series(after_samples, snapshot)
    requests = request_series(after_samples)
    if baseline is not None:
        before_samples = parse_exposition(baseline)
        depth = subtract_depth(depth, depth_series(before_samples, baseline), shape)
        requests = subtract_requests(requests, request_series(before_samples), shape)

    outcomes: dict[str, Any] = {}
    for outcome in OUTCOMES:
        entry = depth.get(outcome)
        if entry is None:
            outcomes[outcome] = {"samples": 0}
            continue
        below, at_or_above = floor_split(entry, shape, outcome)
        outcomes[outcome] = {
            "samples": entry["count"],
            "rows_total": entry["sum"],
            "mean_rows": entry["sum"] / entry["count"] if entry["count"] else 0.0,
            "below_floor": below,
            "at_or_above_floor": at_or_above,
            "distribution": distribution(entry),
        }

    qualifying = sum(outcomes[outcome]["samples"] for outcome in QUALIFYING_OUTCOMES)
    qualifying_at_floor = sum(
        outcomes[outcome].get("at_or_above_floor", 0.0) for outcome in QUALIFYING_OUTCOMES
    )
    denominators = {
        outcome: requests.get(outcome, 0.0) for outcome in REQUEST_OUTCOMES
    }
    populations = sum(outcomes[outcome]["samples"] for outcome in OUTCOMES)
    notes = []
    if populations == 0:
        if denominators["bypass"] > 0:
            notes.append(
                f"no population opened: {denominators['bypass']:.0f} of "
                f"{denominators['hit'] + denominators['miss'] + denominators['bypass']:.0f} "
                "cache requests bypassed population (predicate or prune spec, #4891). "
                "This shape is INELIGIBLE, not zero-depth: nothing here says how deep "
                "its browses read"
            )
        elif denominators["hit"] > 0 and denominators["miss"] == 0:
            notes.append(
                f"no population opened: every one of {denominators['hit']:.0f} cache "
                "requests hit, so no task read a file"
            )
        else:
            notes.append(
                "no population samples and no cache requests — the shape did not run, "
                "or the cache is disabled on this install"
            )
    if outcomes["unpolled"]["samples"]:
        notes.append(
            f"{outcomes['unpolled']['samples']:.0f} population(s) were never polled "
            "(the plan's LIMIT was satisfied before the task started); they are zero "
            "by construction and excluded from the floor fraction"
        )
    if outcomes["error"]["samples"]:
        notes.append(
            f"{outcomes['error']['samples']:.0f} population(s) ended in a read error "
            "and are excluded from the floor fraction"
        )
    if outcomes["clipped"]["samples"]:
        notes.append(
            "`clipped` counts a population dropped before end-of-stream: a LIMIT "
            "satisfied early AND a cancelled or failed-elsewhere query. Read it "
            "against the shape's own query outcomes"
        )

    return {
        "shape": shape,
        "snapshot": str(snapshot),
        "baseline": str(baseline) if baseline else None,
        "outcomes": outcomes,
        "requests": denominators,
        "populations": populations,
        "qualifying_samples": qualifying,
        "qualifying_at_or_above_floor": qualifying_at_floor,
        "row_group_floor_rows": ROW_GROUP_FLOOR_ROWS,
        "geometry": geometry,
        "per_request_stats": stats,
        "statement": geometry_statement(geometry, qualifying_at_floor, qualifying),
        "notes": notes,
    }


def render(report: dict[str, Any]) -> str:
    lines = [
        f"decoded-file-cache population depth, floor {ROW_GROUP_FLOOR_ROWS} rows",
        "",
    ]
    for shape in report["shapes"]:
        lines.append(f"shape {shape['shape']}")
        counts = ", ".join(
            f"{outcome}={shape['outcomes'][outcome]['samples']:.0f}" for outcome in OUTCOMES
        )
        lines.append(f"  populations: {counts}")
        requests = ", ".join(
            f"{outcome}={value:.0f}"
            for outcome, value in shape["requests"].items()
            if value
        )
        lines.append(f"  cache requests: {requests or 'none'}")
        for outcome in QUALIFYING_OUTCOMES:
            entry = shape["outcomes"][outcome]
            if not entry["samples"]:
                continue
            lines.append(
                f"  {outcome}: {entry['samples']:.0f} samples, mean "
                f"{entry['mean_rows']:.0f} rows, {entry['below_floor']:.0f} below "
                f"floor, {entry['at_or_above_floor']:.0f} at or above"
            )
            for band in entry["distribution"]:
                if band["in_band"]:
                    lines.append(f"      <= {band['le']}: {band['in_band']:.0f}")
        if shape["per_request_stats"]:
            stats = shape["per_request_stats"]
            lines.append(
                f"  per-request: {stats['responses']} responses, "
                f"hits={stats['file_cache_hits']}, misses={stats['file_cache_misses']}, "
                f"bypasses={stats['file_cache_bypasses']}, "
                f"populate_rows={stats['file_cache_populate_rows']}"
            )
        lines.append(f"  {shape['statement']}")
        for note in shape["notes"]:
            lines.append(f"  note: {note}")
        lines.append("")
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--shape",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="Prometheus exposition snapshot taken after this query shape ran",
    )
    parser.add_argument(
        "--baseline",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="snapshot taken before the shape ran; its counts are subtracted",
    )
    parser.add_argument(
        "--geometry",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="recorded footer geometry for the files the shape scanned",
    )
    parser.add_argument(
        "--stats",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="recorded /api/v1/sql responses for the shape (per-request denominators)",
    )
    parser.add_argument("--output", metavar="PATH", help="write the report as JSON")
    args = parser.parse_args(argv)

    try:
        snapshots = pairs(args.shape, "--shape")
        if not snapshots:
            raise InputError("at least one --shape NAME=PATH is required")
        baselines = pairs(args.baseline, "--baseline")
        geometries = pairs(args.geometry, "--geometry")
        stats_paths = pairs(args.stats, "--stats")
        for flag, named in (
            ("--baseline", baselines),
            ("--geometry", geometries),
            ("--stats", stats_paths),
        ):
            unknown = sorted(set(named) - set(snapshots))
            if unknown:
                raise InputError(f"{flag} names shapes with no --shape: {', '.join(unknown)}")
        shapes = [
            read_shape(
                shape,
                path,
                baselines.get(shape),
                load_geometry(geometries[shape]) if shape in geometries else None,
                load_stats(stats_paths[shape]) if shape in stats_paths else None,
            )
            for shape, path in snapshots.items()
        ]
    except InputError as error:
        print(f"FAIL {error}", file=sys.stderr)
        return 2

    report = {"row_group_floor_rows": ROW_GROUP_FLOOR_ROWS, "shapes": shapes}
    print(render(report))
    if args.output:
        try:
            pathlib.Path(args.output).write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
        except OSError as error:
            print(f"FAIL cannot write {args.output}: {error}", file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
