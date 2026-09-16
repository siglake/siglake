#!/usr/bin/env python3
"""Grade a retained kind Postgres-outage trace without touching a cluster."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import sys
from typing import Any


def parse_stamp(value: Any, field: str, problems: list[str]) -> dt.datetime | None:
    if not isinstance(value, str) or not value:
        problems.append(f"missing {field}")
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        problems.append(f"invalid {field}: {value!r}")
        return None
    if parsed.tzinfo is None:
        problems.append(f"{field} has no timezone: {value!r}")
        return None
    return parsed


def numeric_series(sample: dict[str, Any], field: str) -> dict[str, float] | None:
    rows = sample.get(field)
    if not isinstance(rows, list) or not rows:
        return None
    result: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("pod"), str):
            return None
        value = row.get("value")
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
            return None
        if row["pod"] in result:
            return None
        result[row["pod"]] = float(value)
    return result


def grade(document: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    if document.get("schema_version") != 1:
        problems.append("unsupported or missing schema_version")
    revisions = document.get("revisions")
    if not isinstance(revisions, dict) or not revisions.get("repository_commit"):
        problems.append("missing pinned repository revision")
    else:
        query_images = revisions.get("query_pods")
        postgres_image = revisions.get("postgres")
        if not isinstance(query_images, list) or not query_images:
            problems.append("missing pinned query-pod images")
        elif any(
            not isinstance(row, dict)
            or not row.get("pod")
            or not row.get("image")
            or not row.get("image_id")
            for row in query_images
        ):
            problems.append("one or more query-pod image revisions are incomplete")
        if not isinstance(postgres_image, dict) or not postgres_image.get("image_id"):
            problems.append("missing pinned Postgres image")
        elif not postgres_image.get("pod") or not postgres_image.get("image"):
            problems.append("Postgres image revision is incomplete")

    settings = document.get("settings")
    required_settings = (
        "persistent_job_store",
        "reconcile_interval_seconds",
        "reconcile_write_timeout_seconds",
        "max_unreconciled_per_pod",
        "sample_interval_seconds",
        "outage_seconds",
        "drain_timeout_seconds",
    )
    if not isinstance(settings, dict):
        problems.append("missing effective reconciliation settings")
    else:
        missing = [name for name in required_settings if name not in settings]
        if missing:
            problems.append("missing effective settings: " + ", ".join(missing))
        if settings.get("persistent_job_store") is not True:
            problems.append("persistent batch-job store was not enabled")

    expected = document.get("expected_pods")
    if not isinstance(expected, list) or not expected or not all(
        isinstance(pod, str) and pod for pod in expected
    ):
        problems.append("missing expected query-pod set")
        expected = []
    expected_set = set(expected)
    if len(expected_set) != len(expected):
        problems.append("expected query-pod set contains duplicates")
    if isinstance(revisions, dict) and isinstance(revisions.get("query_pods"), list) and expected_set:
        revision_pods = {
            row.get("pod") for row in revisions["query_pods"] if isinstance(row, dict)
        }
        if revision_pods != expected_set:
            problems.append("pinned query-pod revisions do not match the expected pod set")

    timestamps = document.get("timestamps")
    if not isinstance(timestamps, dict):
        timestamps = {}
        problems.append("missing outage/restoration timestamps")
    outage_at = parse_stamp(timestamps.get("outage_started_at"), "outage_started_at", problems)
    restored_at = parse_stamp(
        timestamps.get("restoration_started_at"), "restoration_started_at", problems
    )
    ready_at = parse_stamp(timestamps.get("postgres_ready_at"), "postgres_ready_at", problems)
    if outage_at and restored_at and restored_at <= outage_at:
        problems.append("restoration did not follow the outage")
    if restored_at and ready_at and ready_at < restored_at:
        problems.append("Postgres-ready timestamp precedes restoration")

    submissions = document.get("submissions")
    if not isinstance(submissions, list):
        submissions = []
        problems.append("missing submission observations")
    accepted = [row for row in submissions if isinstance(row, dict) and row.get("http_status") == 202]
    if not accepted:
        problems.append("no batch submission was observed accepted")
    submission_times = [
        parsed
        for i, row in enumerate(accepted)
        if (parsed := parse_stamp(row.get("submitted_at"), f"submissions[{i}].submitted_at", problems))
    ]

    samples = document.get("samples")
    if not isinstance(samples, list):
        samples = []
        problems.append("missing Prometheus samples")

    parsed_samples: list[tuple[dt.datetime, str, dict[str, float], dict[str, float]]] = []
    for index, sample in enumerate(samples):
        if not isinstance(sample, dict):
            problems.append(f"sample {index} is not an object")
            continue
        at = parse_stamp(sample.get("at"), f"samples[{index}].at", problems)
        phase = sample.get("phase")
        backlog = numeric_series(sample, "backlog")
        completions = numeric_series(sample, "completions")
        if phase not in {"baseline", "outage", "recovery"}:
            problems.append(f"sample {index} has invalid phase {phase!r}")
        if backlog is None:
            problems.append(f"sample {index} has no usable backlog observation")
        if completions is None:
            problems.append(f"sample {index} has no usable completion observation")
        if at and phase in {"baseline", "outage", "recovery"} and backlog is not None and completions is not None:
            parsed_samples.append((at, phase, backlog, completions))

    parsed_samples.sort(key=lambda row: row[0])
    for at, phase, _, _ in parsed_samples:
        if phase == "baseline" and outage_at and at > outage_at:
            problems.append(f"baseline sample at {at.isoformat()} follows the outage")
        if phase == "outage" and outage_at and at < outage_at:
            problems.append(f"outage sample at {at.isoformat()} precedes the outage")
        if phase == "outage" and restored_at and at > restored_at:
            problems.append(f"outage sample at {at.isoformat()} follows restoration")
        if phase == "recovery" and restored_at and at < restored_at:
            problems.append(f"recovery sample at {at.isoformat()} precedes restoration")
    if expected_set:
        for at, _, backlog, completions in parsed_samples:
            missing_backlog = expected_set - backlog.keys()
            missing_completions = expected_set - completions.keys()
            if missing_backlog:
                problems.append(
                    f"backlog sample at {at.isoformat()} missed pods: {', '.join(sorted(missing_backlog))}"
                )
            if missing_completions:
                problems.append(
                    f"completion sample at {at.isoformat()} missed pods: {', '.join(sorted(missing_completions))}"
                )

    baseline = [row for row in parsed_samples if row[1] == "baseline"]
    outage = [row for row in parsed_samples if row[1] == "outage"]
    recovery = [row for row in parsed_samples if row[1] == "recovery"]
    if not baseline:
        problems.append("no baseline sample")
    if not outage:
        problems.append("no outage sample")
    if not recovery:
        problems.append("no recovery sample")
    if baseline and expected_set and any(baseline[-1][2].get(pod) != 0 for pod in expected_set):
        problems.append("baseline backlog was not zero on every query pod")

    totals = [(at, phase, sum(backlog.values())) for at, phase, backlog, _ in parsed_samples]
    positive = [(at, total) for at, _, total in totals if total > 0]
    peak = max((total for _, _, total in totals), default=0.0)
    if not positive:
        problems.append("the outage produced no observed unreconciled backlog")

    drained_at: dt.datetime | None = None
    if positive and restored_at:
        first_positive_at = positive[0][0]
        for at, phase, backlog, _ in parsed_samples:
            if at >= max(first_positive_at, restored_at) and phase == "recovery" and expected_set:
                if all(backlog.get(pod) == 0 for pod in expected_set):
                    drained_at = at
                    break
        if drained_at is None:
            problems.append("the observed backlog did not drain after restoration")

    completion_delta: float | None = None
    completion_seconds: float | None = None
    if len(parsed_samples) >= 2:
        first, last = parsed_samples[0], parsed_samples[-1]
        first_total = sum(first[3].values())
        last_total = sum(last[3].values())
        completion_delta = last_total - first_total
        completion_seconds = (last[0] - first[0]).total_seconds()
        if completion_delta < 0:
            problems.append("completion counter decreased during the observation")
        if completion_seconds <= 0:
            problems.append("completion observation window was not positive")
    else:
        problems.append("not enough samples to calculate completion rate")

    submission_seconds = 0.0
    if len(submission_times) >= 2:
        submission_seconds = (max(submission_times) - min(submission_times)).total_seconds()

    peak_by_pod = {
        pod: max((backlog.get(pod, 0.0) for _, _, backlog, _ in parsed_samples), default=0.0)
        for pod in sorted(expected_set)
    }
    summary = {
        "accepted_jobs": len(accepted),
        "submission_window_seconds": submission_seconds,
        "submission_rate_per_second": len(accepted) / max(submission_seconds, 1.0),
        "completion_delta": completion_delta,
        "completion_observation_seconds": completion_seconds,
        "completion_rate_per_second": (
            completion_delta / completion_seconds
            if completion_delta is not None and completion_seconds and completion_seconds > 0
            else None
        ),
        "peak_backlog_total": peak,
        "peak_backlog_by_pod": peak_by_pod,
        "first_backlog_at": positive[0][0].isoformat().replace("+00:00", "Z") if positive else None,
        "drained_at": drained_at.isoformat().replace("+00:00", "Z") if drained_at else None,
        "time_to_drain_seconds": (
            (drained_at - restored_at).total_seconds() if drained_at and restored_at else None
        ),
    }
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
        print(f"ERROR: cannot read outage evidence: {error}", file=sys.stderr)
        return 2
    if not isinstance(document, dict):
        print("ERROR: outage evidence root is not an object", file=sys.stderr)
        return 2

    result = grade(document)
    graded = dict(document)
    graded["evidence"] = result
    rendered = json.dumps(graded, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    print(
        "POSTGRES_OUTAGE_EVIDENCE "
        f"grade={result['grade']} peak={result['summary']['peak_backlog_total']} "
        f"drain_seconds={result['summary']['time_to_drain_seconds']}",
        file=sys.stderr,
    )
    for problem in result["problems"]:
        print(f"  {problem}", file=sys.stderr)
    return 0 if result["grade"] == "verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
