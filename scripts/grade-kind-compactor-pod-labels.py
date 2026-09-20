#!/usr/bin/env python3
"""Grade a retained two-compactor shared-queue capture without a cluster."""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import sys
from typing import Any

METRIC = "siglake_compactor_sealed_pending"
REQUIRED_QUERIES = ("raw", "sample_times", "per_pod", "operator_expression")
REL_TOLERANCE = 1e-9
ABS_TOLERANCE = 1e-9


def close(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=REL_TOLERANCE, abs_tol=ABS_TOLERANCE)


def selector(namespace: str, release: str) -> str:
    return (
        f'namespace="{namespace}",app_kubernetes_io_instance="{release}",'
        'app_kubernetes_io_component="compactor"'
    )


def expected_expressions(namespace: str, release: str) -> dict[str, str]:
    matcher = selector(namespace, release)
    raw = f"{METRIC}{{{matcher}}}"
    return {
        "raw": raw,
        "sample_times": f"timestamp({raw})",
        "per_pod": f"sum by (pod) ({raw})",
        "operator_expression": f"avg(sum by (pod) ({raw}))",
    }


def samples(
    query: dict[str, Any], name: str, evaluated_at: int | None, problems: list[str]
) -> list[tuple[dict[str, str], float]] | None:
    response = query.get("response")
    if not isinstance(response, dict) or response.get("status") != "success":
        problems.append(f"{name}: no successful retained Prometheus response")
        return None
    data = response.get("data")
    if not isinstance(data, dict) or data.get("resultType") != "vector":
        problems.append(f"{name}: response is not an instant vector")
        return None
    result = data.get("result")
    if not isinstance(result, list) or not result:
        problems.append(f"{name}: the response carries no series")
        return None
    parsed = []
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
                f"{name}: series {index} was evaluated at {stamp}, not {evaluated_at}"
            )
            return None
        parsed.append(({str(k): str(v) for k, v in item["metric"].items()}, number))
    return parsed


def by_pod(
    rows: list[tuple[dict[str, str], float]], name: str, problems: list[str]
) -> dict[str, float] | None:
    totals: dict[str, float] = {}
    usable = True
    for index, (labels, value) in enumerate(rows):
        pod = labels.get("pod", "").strip()
        if not pod:
            problems.append(f"{name}: series {index} carries no nonempty pod label")
            usable = False
            continue
        totals[pod] = totals.get(pod, 0.0) + value
    return totals if usable else None


def grade(document: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    if document.get("schema_version") != 1:
        problems.append("unsupported or missing schema_version")

    revisions = document.get("revisions")
    if not isinstance(revisions, dict) or not revisions.get("repository_commit"):
        problems.append("missing pinned repository revision")
    else:
        pods = revisions.get("compactor_pods")
        if not isinstance(pods, list) or len(pods) < 2:
            problems.append("fewer than two pinned compactor-pod images")
        elif any(
            not isinstance(row, dict)
            or not row.get("pod")
            or not row.get("image")
            or not row.get("image_id")
            for row in pods
        ):
            problems.append("one or more compactor-pod image revisions are incomplete")

    settings = document.get("settings")
    namespace = release = ""
    required_settings = (
        "namespace",
        "release",
        "scale_path",
        "compactor_replicas",
        "load_seconds",
        "compactor_interval_seconds",
        "scrape_interval_seconds",
        "commit_batch_target_mb",
        "commit_batch_max_age_seconds",
    )
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
            problems.append("the capture does not name its namespace and release")
        if settings.get("scale_path") != "helm_upgrade_reuse_values":
            problems.append("the capture did not install the two-compactor chart path")
        replicas = settings.get("compactor_replicas")
        if not isinstance(replicas, int) or isinstance(replicas, bool) or replicas < 2:
            problems.append("the capture did not request two or more compactor replicas")
        for key in ("load_seconds", "compactor_interval_seconds", "scrape_interval_seconds"):
            value = settings.get(key)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                problems.append(f"{key} is not a positive integer")
        max_age = settings.get("commit_batch_max_age_seconds")
        if isinstance(max_age, int) and isinstance(settings.get("load_seconds"), int):
            if max_age <= settings["load_seconds"]:
                problems.append("the commit-batch hold does not outlive the load window")

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
        problems.append("fewer than two compactor pods were expected")
        expected = []
    expected_set = set(expected)
    if len(expected_set) != len(expected):
        problems.append("the expected compactor-pod set contains duplicates")

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
            problems.append(f"{name}: query time differs from the capture timestamp")
        if name in wanted and query.get("expression") != wanted[name]:
            problems.append(f"{name}: retained expression differs from the capture contract")
        rows = samples(query, name, evaluated_at, problems)
        if rows is not None:
            parsed[name] = rows

    summary: dict[str, Any] = {
        "pod_count": 0,
        "series_count": 0,
        "per_pod": {},
        "sample_times": {},
        "shared_queue": None,
        "operator_value": None,
    }
    raw_totals = None
    if "raw" in parsed:
        raw_totals = by_pod(parsed["raw"], "raw", problems)
        for index, (labels, _) in enumerate(parsed["raw"]):
            if labels.get("__name__", METRIC) != METRIC:
                problems.append(f"raw: series {index} is not {METRIC}")
            if labels.get("tenant") != "default":
                problems.append(f"raw: series {index} is not tenant=default")
        if raw_totals is not None:
            summary["pod_count"] = len(raw_totals)
            summary["series_count"] = len(parsed["raw"])
            summary["per_pod"] = {pod: raw_totals[pod] for pod in sorted(raw_totals)}
            if set(raw_totals) != expected_set:
                problems.append("the raw series do not cover exactly the expected compactor pods")
            if any(value <= 0 for value in raw_totals.values()):
                problems.append("the shared queue was not positive on every compactor")
            values = list(raw_totals.values())
            if values and not all(close(value, values[0]) for value in values[1:]):
                problems.append("the compactor replicas did not publish the same shared queue")
            elif values:
                summary["shared_queue"] = values[0]

    source_times = None
    if "sample_times" in parsed:
        source_times = by_pod(parsed["sample_times"], "sample_times", problems)
        if source_times is not None:
            if len(source_times) != len(parsed["sample_times"]):
                problems.append("sample_times: more than one timestamp series per pod")
            summary["sample_times"] = {pod: source_times[pod] for pod in sorted(source_times)}
            if set(source_times) != expected_set:
                problems.append("sample timestamps do not cover exactly the expected pods")
            interval = settings.get("scrape_interval_seconds")
            if isinstance(interval, int) and source_times:
                spread = max(source_times.values()) - min(source_times.values())
                if spread > interval:
                    problems.append(
                        f"the per-pod samples are {spread}s apart, beyond one {interval}s scrape interval"
                    )

    grouped = by_pod(parsed["per_pod"], "per_pod", problems) if "per_pod" in parsed else None
    if grouped is not None:
        if len(grouped) != len(parsed["per_pod"]):
            problems.append("per_pod: the grouped answer repeats a pod")
        if raw_totals is not None and (
            set(grouped) != set(raw_totals)
            or any(not close(grouped[pod], raw_totals[pod]) for pod in grouped if pod in raw_totals)
        ):
            problems.append("sum by (pod) does not match the raw per-pod totals")

    operator = parsed.get("operator_expression")
    if operator is not None:
        if len(operator) != 1:
            problems.append("the operator expression did not return one scalar group")
        else:
            value = operator[0][1]
            summary["operator_value"] = value
            if summary["shared_queue"] is not None and not close(value, summary["shared_queue"]):
                problems.append(
                    f"the operator reads {value}, not the shared queue {summary['shared_queue']}"
                )

    settling = document.get("settling_samples")
    if not isinstance(settling, list) or len(settling) != 2:
        problems.append("the capture lacks two settled scrape generations")
    else:
        generations = []
        for generation, snapshot in enumerate(settling):
            rows = snapshot.get("pods") if isinstance(snapshot, dict) else None
            if not isinstance(rows, list):
                problems.append(f"settling generation {generation} has no pod samples")
                continue
            try:
                mapped = {
                    row["pod"]: (float(row["value"]), float(row["sample_time"])) for row in rows
                }
            except (KeyError, TypeError, ValueError):
                problems.append(f"settling generation {generation} has malformed samples")
                continue
            if set(mapped) != expected_set:
                problems.append(f"settling generation {generation} does not cover expected pods")
            values = [pair[0] for pair in mapped.values()]
            if values and (values[0] <= 0 or not all(close(v, values[0]) for v in values[1:])):
                problems.append(f"settling generation {generation} does not carry one positive queue")
            interval = settings.get("scrape_interval_seconds")
            sample_stamps = [pair[1] for pair in mapped.values()]
            if isinstance(interval, int) and sample_stamps:
                spread = max(sample_stamps) - min(sample_stamps)
                if spread > interval:
                    problems.append(
                        f"settling generation {generation} spans {spread}s, beyond one "
                        f"{interval}s scrape interval"
                    )
            generations.append(mapped)
        if len(generations) == 2 and set(generations[0]) == set(generations[1]):
            for pod in generations[1]:
                old_value, old_time = generations[0][pod]
                new_value, new_time = generations[1][pod]
                if not close(old_value, new_value):
                    problems.append(f"pod {pod}: queue changed between settled generations")
                if new_time <= old_time:
                    problems.append(f"pod {pod}: scrape timestamp did not advance")
                if raw_totals is not None and pod in raw_totals and not close(new_value, raw_totals[pod]):
                    problems.append(f"pod {pod}: final raw value differs from the settled value")
                if source_times is not None and pod in source_times and not close(new_time, source_times[pod]):
                    problems.append(f"pod {pod}: final timestamp differs from the settled timestamp")

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
        print(f"ERROR: cannot read the compactor label capture: {error}", file=sys.stderr)
        return 2
    if not isinstance(document, dict):
        print("ERROR: the compactor label capture root is not an object", file=sys.stderr)
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
        "COMPACTOR_POD_LABEL_EVIDENCE "
        f"grade={result['grade']} pods={summary['pod_count']} "
        f"series={summary['series_count']} shared_queue={summary['shared_queue']} "
        f"operator={summary['operator_value']}",
        file=sys.stderr,
    )
    for problem in result["problems"]:
        print(f"  {problem}", file=sys.stderr)
    return 0 if result["grade"] == "verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
