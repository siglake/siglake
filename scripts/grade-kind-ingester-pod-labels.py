#!/usr/bin/env python3
"""Grade a retained kind ingester per-pod label capture without a cluster.

The capture (scripts/kind-round.sh's `capture_ingester_pod_labels`) retains four
Prometheus answers taken at ONE evaluation timestamp: the raw
`siglake_ingest_requests_total` series with their label sets, the per-series 1m
rate, the same rate summed by `pod`, and the operator's own expression from
crates/siglake-operator/src/prom.rs:177.

What this grader is for (#3647): `pod` is attached by Prometheus Operator's
target relabeling, so nothing in the repository renders it and no offline
fixture can prove it is there. If it went missing, `sum by (pod)` would collapse
the tier into one group and the operator would read the fleet total as one pod's
rate. So every contributing series must carry a nonempty `pod`, at least two
pods must carry a nonzero rate at the same instant, and the operator's value
must be the MEAN OF THE PER-POD SUMS -- a number that is only distinguishable
from the fleet total and the per-series average when the capture has more than
one pod and more series than pods.
"""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import sys
from typing import Any

METRIC = "siglake_ingest_requests_total"
REQUIRED_QUERIES = ("raw", "per_series_rate", "per_pod_rate", "operator_expression")
# Prometheus renders float64 as text and the two sides of every comparison below
# are the same sum in a different order, so the gap that matters is far above
# this and far below a per-series/per-pod confusion.
REL_TOLERANCE = 1e-6
ABS_TOLERANCE = 1e-9


def close(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=REL_TOLERANCE, abs_tol=ABS_TOLERANCE)


def selector(namespace: str, release: str) -> str:
    return (
        f'namespace="{namespace}",app_kubernetes_io_instance="{release}",'
        'app_kubernetes_io_component="ingester"'
    )


def expected_expressions(namespace: str, release: str) -> dict[str, str]:
    matcher = selector(namespace, release)
    return {
        "raw": f"{METRIC}{{{matcher}}}",
        "per_series_rate": f"rate({METRIC}{{{matcher}}}[1m])",
        "per_pod_rate": f"sum by (pod) (rate({METRIC}{{{matcher}}}[1m]))",
        "operator_expression": (
            f"avg(sum by (pod) (rate({METRIC}{{{matcher}}}[1m])))"
        ),
    }


def samples(
    query: dict[str, Any], name: str, evaluated_at: int | None, problems: list[str]
) -> list[tuple[dict[str, str], float]] | None:
    """The (labels, value) pairs of one instant vector, or None if unusable."""
    response = query.get("response")
    if not isinstance(response, dict):
        problems.append(f"{name}: no retained Prometheus response")
        return None
    if response.get("status") != "success":
        problems.append(f"{name}: Prometheus answered status={response.get('status')!r}")
        return None
    data = response.get("data")
    if not isinstance(data, dict) or data.get("resultType") != "vector":
        problems.append(f"{name}: response is not an instant vector")
        return None
    result = data.get("result")
    if not isinstance(result, list) or not result:
        problems.append(f"{name}: the response carries no series")
        return None
    parsed: list[tuple[dict[str, str], float]] = []
    for index, item in enumerate(result):
        if not isinstance(item, dict) or not isinstance(item.get("metric"), dict):
            problems.append(f"{name}: series {index} has no label set")
            return None
        value = item.get("value")
        if not isinstance(value, list) or len(value) != 2:
            problems.append(f"{name}: series {index} has no instant sample")
            return None
        try:
            stamp, number = float(value[0]), float(value[1])
        except (TypeError, ValueError):
            problems.append(f"{name}: series {index} has a non-numeric sample")
            return None
        if not math.isfinite(number):
            problems.append(f"{name}: series {index} sampled a non-finite value")
            return None
        if evaluated_at is not None and not close(stamp, float(evaluated_at)):
            problems.append(
                f"{name}: series {index} was evaluated at {stamp}, not at the capture's {evaluated_at}"
            )
            return None
        labels = {
            str(key): str(item["metric"][key]) for key in item["metric"]
        }
        parsed.append((labels, number))
    return parsed


def pod_labels(
    rows: list[tuple[dict[str, str], float]], name: str, problems: list[str]
) -> dict[str, float] | None:
    """Sum `rows` by their `pod` label, refusing a missing or empty one."""
    by_pod: dict[str, float] = {}
    usable = True
    for index, (labels, value) in enumerate(rows):
        if "pod" not in labels:
            problems.append(f"{name}: series {index} carries no pod label")
            usable = False
            continue
        if not labels["pod"].strip():
            problems.append(f"{name}: series {index} carries an empty pod label")
            usable = False
            continue
        by_pod[labels["pod"]] = by_pod.get(labels["pod"], 0.0) + value
    return by_pod if usable else None


def grade(document: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    if document.get("schema_version") != 1:
        problems.append("unsupported or missing schema_version")

    revisions = document.get("revisions")
    if not isinstance(revisions, dict) or not revisions.get("repository_commit"):
        problems.append("missing pinned repository revision")
    else:
        pods = revisions.get("ingester_pods")
        if not isinstance(pods, list) or not pods:
            problems.append("missing pinned ingester-pod images")
        elif any(
            not isinstance(row, dict)
            or not row.get("pod")
            or not row.get("image")
            or not row.get("image_id")
            for row in pods
        ):
            problems.append("one or more ingester-pod image revisions are incomplete")

    settings = document.get("settings")
    required_settings = (
        "namespace",
        "release",
        "scale_path",
        "ingester_min_replicas",
        "ingester_max_replicas",
        "ingester_floor_during_capture",
        "load_seconds",
        "rate_window",
    )
    namespace = release = ""
    if not isinstance(settings, dict):
        problems.append("missing effective capture settings")
        settings = {}
    else:
        missing = [key for key in required_settings if key not in settings]
        if missing:
            problems.append("missing effective settings: " + ", ".join(missing))
        namespace = str(settings.get("namespace") or "")
        release = str(settings.get("release") or "")
        if not namespace or not release:
            problems.append("the capture does not name the namespace and release it selected on")
        if settings.get("rate_window") != "1m":
            problems.append(
                f"the operator reads a 1m rate, the capture recorded {settings.get('rate_window')!r}"
            )
        floor = settings.get("ingester_floor_during_capture")
        if not isinstance(floor, int) or isinstance(floor, bool) or floor < 2:
            problems.append("the capture did not hold the ingester at two or more replicas")
        load_seconds = settings.get("load_seconds")
        if not isinstance(load_seconds, int) or isinstance(load_seconds, bool) or load_seconds <= 0:
            problems.append("the capture recorded no load window")

    evaluated_at = document.get("evaluated_at")
    if not isinstance(evaluated_at, int) or isinstance(evaluated_at, bool) or evaluated_at <= 0:
        problems.append("missing evaluation timestamp")
        evaluated_at = None

    expected = document.get("expected_pods")
    if (
        not isinstance(expected, list)
        or len(expected) < 2
        or not all(isinstance(pod, str) and pod.strip() for pod in expected)
    ):
        problems.append("fewer than two ingester pods were expected in the capture")
        expected = []
    expected_set = set(expected)
    if len(expected_set) != len(expected):
        problems.append("the expected ingester-pod set contains duplicates")

    queries = document.get("queries")
    if not isinstance(queries, dict):
        problems.append("missing retained Prometheus queries")
        queries = {}
    wanted = expected_expressions(namespace, release) if namespace and release else {}
    parsed: dict[str, list[tuple[dict[str, str], float]]] = {}
    for name in REQUIRED_QUERIES:
        query = queries.get(name)
        if not isinstance(query, dict):
            problems.append(f"missing the {name} query")
            continue
        if evaluated_at is not None and query.get("time") != evaluated_at:
            problems.append(
                f"{name}: evaluated at time={query.get('time')!r}, not at the capture's {evaluated_at}"
            )
        if name in wanted and query.get("expression") != wanted[name]:
            problems.append(
                f"{name}: the retained expression is not the one this capture claims to have evaluated"
            )
        rows = samples(query, name, evaluated_at, problems)
        if rows is not None:
            parsed[name] = rows

    summary: dict[str, Any] = {
        "pod_count": 0,
        "series_count": 0,
        "active_pod_count": 0,
        "per_pod_rate": {},
        "fleet_total": None,
        "per_series_average": None,
        "per_pod_mean": None,
        "operator_value": None,
    }

    raw_pods = None
    if "raw" in parsed:
        raw_pods = pod_labels(parsed["raw"], "raw", problems)
        for index, (labels, _) in enumerate(parsed["raw"]):
            if labels.get("__name__", METRIC) != METRIC:
                problems.append(f"raw: series {index} is not {METRIC}")
        if raw_pods is not None:
            summary["pod_count"] = len(raw_pods)
            summary["series_count"] = len(parsed["raw"])
            if len(raw_pods) < 2:
                problems.append(
                    f"the raw capture covers {len(raw_pods)} ingester pod(s), not two or more"
                )
            if len(parsed["raw"]) <= len(raw_pods):
                problems.append(
                    "every pod published one series, so the per-series average and the "
                    "per-pod mean are the same number and the capture proves neither"
                )
            if expected_set and not expected_set <= set(raw_pods):
                problems.append(
                    "the raw capture is missing expected pods: "
                    + ", ".join(sorted(expected_set - set(raw_pods)))
                )

    per_pod_from_series = None
    if "per_series_rate" in parsed:
        per_pod_from_series = pod_labels(parsed["per_series_rate"], "per_series_rate", problems)

    per_pod = None
    if "per_pod_rate" in parsed:
        per_pod = pod_labels(parsed["per_pod_rate"], "per_pod_rate", problems)
        if per_pod is not None and len(per_pod) != len(parsed["per_pod_rate"]):
            problems.append("per_pod_rate: the grouped answer repeats a pod")

    if per_pod is not None and per_pod_from_series is not None:
        if set(per_pod) != set(per_pod_from_series):
            problems.append(
                "the grouped answer and the per-series answer cover different pods: "
                f"{sorted(per_pod)} vs {sorted(per_pod_from_series)}"
            )
        else:
            for pod in sorted(per_pod):
                if not close(per_pod[pod], per_pod_from_series[pod]):
                    problems.append(
                        f"pod {pod}: sum by (pod) reads {per_pod[pod]}, its series sum to "
                        f"{per_pod_from_series[pod]}"
                    )

    if per_pod is not None:
        summary["per_pod_rate"] = {pod: per_pod[pod] for pod in sorted(per_pod)}
        active = [pod for pod, value in per_pod.items() if value > 0]
        summary["active_pod_count"] = len(active)
        if len(active) < 2:
            problems.append(
                f"{len(active)} ingester pod(s) carried a nonzero request rate at the "
                "evaluation timestamp, not two or more"
            )
        if expected_set and not expected_set <= set(per_pod):
            problems.append(
                "the per-pod rate is missing expected pods: "
                + ", ".join(sorted(expected_set - set(per_pod)))
            )
        fleet_total = sum(per_pod.values())
        mean = fleet_total / len(per_pod) if per_pod else 0.0
        summary["fleet_total"] = fleet_total
        summary["per_pod_mean"] = mean
        series_count = len(parsed.get("per_series_rate", []))
        if series_count:
            summary["per_series_average"] = fleet_total / series_count

        operator = parsed.get("operator_expression")
        if operator is not None:
            if len(operator) != 1:
                problems.append(
                    f"the operator expression answered {len(operator)} series, expected one scalar group"
                )
            else:
                value = operator[0][1]
                summary["operator_value"] = value
                if not close(value, mean):
                    problems.append(
                        f"the operator expression reads {value}, the mean of the per-pod sums is {mean}"
                    )
                if close(value, fleet_total):
                    problems.append(
                        "the operator's reading is indistinguishable from the fleet total in this capture"
                    )
                if summary["per_series_average"] is not None and close(
                    value, summary["per_series_average"]
                ):
                    problems.append(
                        "the operator's reading is indistinguishable from the per-series average "
                        "in this capture"
                    )

    return {
        "grade": "verified" if not problems else "unverified",
        "problems": problems,
        "summary": summary,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    try:
        document = json.loads(args.input.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"ERROR: cannot read the ingester label capture: {error}", file=sys.stderr)
        return 2
    if not isinstance(document, dict):
        print("ERROR: the ingester label capture root is not an object", file=sys.stderr)
        return 2

    result = grade(document)
    graded = dict(document)
    graded["evidence"] = result
    rendered = json.dumps(graded, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    summary = result["summary"]
    print(
        "INGESTER_POD_LABEL_EVIDENCE "
        f"grade={result['grade']} pods={summary['pod_count']} "
        f"active_pods={summary['active_pod_count']} series={summary['series_count']} "
        f"operator={summary['operator_value']} per_pod_mean={summary['per_pod_mean']} "
        f"fleet_total={summary['fleet_total']} per_series_average={summary['per_series_average']}",
        file=sys.stderr,
    )
    for problem in result["problems"]:
        print(f"  {problem}", file=sys.stderr)
    return 0 if result["grade"] == "verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
