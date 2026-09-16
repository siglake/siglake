#!/usr/bin/env python3
"""Grade a retained kind Postgres-outage trace without touching a cluster."""

from __future__ import annotations

import argparse
import dataclasses
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


STOPPED_STATES = {"T", "t"}


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


def scrape_time(sample: dict[str, Any]) -> dt.datetime | None:
    """The oldest Prometheus scrape any value in this sample came from.

    The instant query's own evaluation timestamp is the clock at query time and
    says nothing about how old the scraped counters are, so it cannot separate a
    counter that moved during the pause from one scraped before it.
    """
    stamps: list[float] = []
    for field in ("backlog", "completions"):
        rows = sample.get(field)
        if not isinstance(rows, list):
            return None
        for row in rows:
            if not isinstance(row, dict):
                return None
            stamp = row.get("sample_time")
            if not isinstance(stamp, (int, float)) or isinstance(stamp, bool):
                return None
            stamps.append(float(stamp))
    if not stamps:
        return None
    return dt.datetime.fromtimestamp(min(stamps), dt.timezone.utc)


def process_states(sample: dict[str, Any]) -> list[dict[str, str]] | None:
    """The `pid/state/starttime` rows the pause window observed, if usable."""
    observation = sample.get("postgres")
    if not isinstance(observation, dict) or observation.get("exec_status") != 0:
        return None
    rows = observation.get("processes")
    if not isinstance(rows, list) or not rows:
        return None
    parsed: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            return None
        if not all(isinstance(row.get(key), str) and row[key] for key in ("pid", "state", "starttime")):
            return None
        parsed.append({key: row[key] for key in ("pid", "state", "starttime")})
    return parsed


@dataclasses.dataclass
class Observation:
    at: dt.datetime
    phase: str
    backlog: dict[str, float]
    completions: dict[str, float]
    scraped_at: dt.datetime | None
    processes: list[dict[str, str]] | None


def grade_pause(
    observations: list[Observation], problems: list[str]
) -> tuple[int | None, int]:
    """Check that the selected Postgres process set stayed stopped and unchanged.

    Returns the size of the first observed stopped set and how many outage
    samples carried a usable observation.
    """
    first: list[dict[str, str]] | None = None
    observed = 0
    outage_samples = [row for row in observations if row.phase == "outage"]
    unobserved = [row.at for row in outage_samples if row.processes is None]
    if unobserved:
        problems.append(
            f"no usable Postgres process-state observation in {len(unobserved)} of "
            f"{len(outage_samples)} outage samples (first at {unobserved[0].isoformat()})"
        )
    for observation in outage_samples:
        stamp = observation.at.isoformat()
        if observation.processes is None:
            continue
        observed += 1
        running = [row for row in observation.processes if row["state"] not in STOPPED_STATES]
        if running:
            detail = ", ".join(f"pid {row['pid']} state {row['state']}" for row in running)
            problems.append(
                f"outage sample at {stamp} observed Postgres processes that were not stopped: {detail}"
            )
        postmaster = next((row for row in observation.processes if row["pid"] == "1"), None)
        if postmaster is None:
            problems.append(f"outage sample at {stamp} did not observe the paused postmaster")
        if first is None:
            first = observation.processes
            continue
        earlier = next((row for row in first if row["pid"] == "1"), None)
        if postmaster and earlier and postmaster["starttime"] != earlier["starttime"]:
            problems.append(
                f"the postmaster was replaced during the pause: start time {earlier['starttime']} "
                f"became {postmaster['starttime']} by {stamp}"
            )
        if len(observation.processes) < len(first):
            problems.append(
                f"the stopped Postgres process set shrank during the pause: {len(first)} "
                f"processes became {len(observation.processes)} by {stamp}"
            )
    return (len(first) if first is not None else None), observed


def grade_write_probes(
    document: dict[str, Any], problems: list[str]
) -> dict[str, list[str]]:
    """Check the bounded writes taken around and inside the pause window."""
    probes = document.get("write_probes")
    if not isinstance(probes, list) or not probes:
        problems.append("missing bounded write-block observations")
        probes = []
    outcomes: dict[str, list[str]] = {"baseline": [], "outage": [], "recovery": []}
    for index, probe in enumerate(probes):
        if not isinstance(probe, dict) or probe.get("phase") not in outcomes:
            problems.append(f"write probe {index} does not name a phase of the probe")
            continue
        if probe.get("outcome") not in {"completed", "blocked", "error"}:
            problems.append(f"write probe {index} has no usable outcome")
            continue
        at = parse_stamp(probe.get("at"), f"write_probes[{index}].at", problems)
        if at is None:
            continue
        outcome = probe["outcome"]
        outcomes[probe["phase"]].append(outcome)
        if probe["phase"] == "outage" and outcome == "completed":
            problems.append(
                f"a bounded write completed at {at.isoformat()} while Postgres was paused, "
                "so the pause did not block writes"
            )
        if probe["phase"] == "outage" and outcome == "error":
            problems.append(
                f"the bounded write at {at.isoformat()} during the pause neither blocked nor "
                f"completed: {str(probe.get('detail', ''))[:120]!r}"
            )
    if not outcomes["outage"]:
        problems.append("no bounded write was attempted while Postgres was paused")
    elif "blocked" not in outcomes["outage"]:
        problems.append("no bounded write was observed blocked by the pause")
    for phase, description in (("baseline", "before the pause"), ("recovery", "after restoration")):
        if not outcomes[phase]:
            problems.append(f"no bounded write was attempted {description}")
        elif "completed" not in outcomes[phase]:
            problems.append(
                f"no bounded write completed {description}, so a blocked write proves nothing"
            )
    return outcomes


def grade_container(document: dict[str, Any], problems: list[str]) -> None:
    """A restart would explain a drained backlog without any write landing."""
    container = document.get("postgres_container")
    before = container.get("before") if isinstance(container, dict) else None
    after = container.get("after") if isinstance(container, dict) else None
    usable = all(
        isinstance(row, dict)
        and isinstance(row.get("uid"), str)
        and row["uid"]
        and isinstance(row.get("restart_count"), int)
        and not isinstance(row.get("restart_count"), bool)
        for row in (before, after)
    )
    if not usable:
        problems.append("missing Postgres container identity and restart observations")
        return
    assert isinstance(before, dict) and isinstance(after, dict)
    if before["uid"] != after["uid"]:
        problems.append("the Postgres pod was replaced during the probe")
    elif after["restart_count"] != before["restart_count"]:
        problems.append(
            "the Postgres container restarted during the probe: restart count "
            f"{before['restart_count']} became {after['restart_count']}"
        )
    elif before.get("started_at") != after.get("started_at"):
        problems.append(
            "the Postgres container start time changed during the probe: "
            f"{before.get('started_at')} became {after.get('started_at')}"
        )


def grade(document: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    if document.get("schema_version") not in {1, 2}:
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

    parsed_samples: list[Observation] = []
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
            parsed_samples.append(
                Observation(
                    at=at,
                    phase=phase,
                    backlog=backlog,
                    completions=completions,
                    scraped_at=scrape_time(sample),
                    processes=process_states(sample),
                )
            )

    parsed_samples.sort(key=lambda row: row.at)
    for observation in parsed_samples:
        at, phase = observation.at, observation.phase
        if phase == "baseline" and outage_at and at > outage_at:
            problems.append(f"baseline sample at {at.isoformat()} follows the outage")
        if phase == "outage" and outage_at and at < outage_at:
            problems.append(f"outage sample at {at.isoformat()} precedes the outage")
        if phase == "outage" and restored_at and at > restored_at:
            problems.append(f"outage sample at {at.isoformat()} follows restoration")
        if phase == "recovery" and restored_at and at < restored_at:
            problems.append(f"recovery sample at {at.isoformat()} precedes restoration")
    untimed = [row.at for row in parsed_samples if row.scraped_at is None]
    if untimed:
        problems.append(
            f"no Prometheus scrape timestamp in {len(untimed)} of {len(parsed_samples)} "
            f"samples (first at {untimed[0].isoformat()}), so their values cannot be "
            "placed against the pause"
        )
    if expected_set:
        for observation in parsed_samples:
            at = observation.at
            missing_backlog = expected_set - observation.backlog.keys()
            missing_completions = expected_set - observation.completions.keys()
            if missing_backlog:
                problems.append(
                    f"backlog sample at {at.isoformat()} missed pods: {', '.join(sorted(missing_backlog))}"
                )
            if missing_completions:
                problems.append(
                    f"completion sample at {at.isoformat()} missed pods: {', '.join(sorted(missing_completions))}"
                )

    baseline = [row for row in parsed_samples if row.phase == "baseline"]
    outage = [row for row in parsed_samples if row.phase == "outage"]
    recovery = [row for row in parsed_samples if row.phase == "recovery"]
    if not baseline:
        problems.append("no baseline sample")
    if not outage:
        problems.append("no outage sample")
    if not recovery:
        problems.append("no recovery sample")
    if baseline and expected_set and any(baseline[-1].backlog.get(pod) != 0 for pod in expected_set):
        problems.append("baseline backlog was not zero on every query pod")

    stopped_processes, observed_outage_states = grade_pause(parsed_samples, problems)
    write_probe_outcomes = grade_write_probes(document, problems)
    grade_container(document, problems)

    totals = [(row.at, row.phase, sum(row.backlog.values())) for row in parsed_samples]
    positive = [(at, total) for at, _, total in totals if total > 0]
    peak = max((total for _, _, total in totals), default=0.0)
    if not positive:
        problems.append("the outage produced no observed unreconciled backlog")

    # The blind spot #3504 and run #76 hit: a backlog that reaches zero while
    # completions rise, before restoration, was graded as a clean drain because
    # `drained_at` only looked at recovery samples. Counters say what was
    # counted, not when the row was written, so this is reported as an
    # unexplained observation, and separately when the scrape it came from
    # predates the pause, which makes it delayed observation of earlier work.
    pre_restoration_drain: dict[str, Any] | None = None
    previous_completions: float | None = None
    for observation in parsed_samples:
        total_completions = sum(observation.completions.values())
        delta = (
            total_completions - previous_completions
            if previous_completions is not None
            else 0.0
        )
        previous_completions = total_completions
        if observation.phase != "outage" or delta <= 0:
            continue
        if restored_at and observation.at >= restored_at:
            continue
        if sum(observation.backlog.values()) != 0:
            continue
        stamp = observation.at.isoformat()
        scraped = observation.scraped_at
        if scraped is not None and outage_at and scraped < outage_at:
            problems.append(
                f"outage sample at {stamp} shows zero backlog with completions up by {delta:g}, "
                f"from a Prometheus scrape at {scraped.isoformat()} taken before the pause: "
                "delayed observation of pre-pause work, not a write during the pause"
            )
        else:
            problems.append(
                f"outage sample at {stamp} shows zero backlog with completions up by {delta:g} "
                f"before restoration at {restored_at.isoformat() if restored_at else 'unknown'}"
                + (f", scraped at {scraped.isoformat()}" if scraped else "")
                + "; the pause window is unexplained"
            )
        if pre_restoration_drain is None:
            pre_restoration_drain = {
                "at": stamp.replace("+00:00", "Z"),
                "completion_increase": delta,
                "scraped_at": (
                    scraped.isoformat().replace("+00:00", "Z") if scraped else None
                ),
                "scrape_precedes_pause": bool(
                    scraped is not None and outage_at and scraped < outage_at
                ),
            }

    drained_at: dt.datetime | None = None
    if positive and restored_at:
        first_positive_at = positive[0][0]
        for observation in parsed_samples:
            if observation.at >= max(first_positive_at, restored_at) and observation.phase == "recovery" and expected_set:
                if all(observation.backlog.get(pod) == 0 for pod in expected_set):
                    drained_at = observation.at
                    break
        if drained_at is None:
            problems.append("the observed backlog did not drain after restoration")

    completion_delta: float | None = None
    completion_seconds: float | None = None
    if len(parsed_samples) >= 2:
        first, last = parsed_samples[0], parsed_samples[-1]
        first_total = sum(first.completions.values())
        last_total = sum(last.completions.values())
        completion_delta = last_total - first_total
        completion_seconds = (last.at - first.at).total_seconds()
        if completion_delta < 0:
            problems.append("completion counter decreased during the observation")
        if completion_seconds <= 0:
            problems.append("completion observation window was not positive")
    else:
        problems.append("not enough samples to calculate completion rate")

    observation_lags = [
        (row.at - row.scraped_at).total_seconds()
        for row in parsed_samples
        if row.scraped_at is not None
    ]

    submission_seconds = 0.0
    if len(submission_times) >= 2:
        submission_seconds = (max(submission_times) - min(submission_times)).total_seconds()

    peak_by_pod = {
        pod: max((row.backlog.get(pod, 0.0) for row in parsed_samples), default=0.0)
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
        # Undefined when the backlog reached zero before restoration: there is
        # then no restoration-to-drain interval to report.
        "time_to_drain_seconds": (
            (drained_at - restored_at).total_seconds()
            if drained_at and restored_at and pre_restoration_drain is None
            else None
        ),
        "pre_restoration_drain": pre_restoration_drain,
        "outage_samples_with_process_state": observed_outage_states,
        "stopped_postgres_processes": stopped_processes,
        "write_probe_outcomes": write_probe_outcomes,
        "max_observation_lag_seconds": max(observation_lags) if observation_lags else None,
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
