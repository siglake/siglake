#!/usr/bin/env python3
"""Grade a retained kind Postgres-outage trace without touching a cluster."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import pathlib
import re
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
TERMINAL_STATUSES = {"succeeded", "failed", "cancelled", "timeout"}
STAMP_FRACTION = re.compile(r"[T ]\d{2}:\d{2}:\d{2}(?:\.(\d+))?")


@dataclasses.dataclass(frozen=True)
class RecordedStamp:
    """The interval represented by a timestamp at its recorded precision."""

    at: dt.datetime
    resolution: dt.timedelta

    @property
    def until(self) -> dt.datetime:
        return self.at + self.resolution


def recorded_stamp(value: Any, parsed: dt.datetime | None) -> RecordedStamp | None:
    if parsed is None or not isinstance(value, str):
        return None
    match = STAMP_FRACTION.search(value)
    digits = len(match.group(1)) if match and match.group(1) else 0
    if digits == 0:
        resolution = dt.timedelta(seconds=1)
    else:
        # datetime has microsecond resolution. More input digits cannot make
        # the parsed interval narrower than that.
        resolution = dt.timedelta(microseconds=max(1, 10 ** max(0, 6 - digits)))
    return RecordedStamp(parsed, resolution)


def resolution_label(stamp: RecordedStamp) -> str:
    seconds = stamp.resolution.total_seconds()
    if seconds >= 1:
        return f"{seconds:g}s"
    return f"{seconds * 1000:g}ms"


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


def series_scrape_times(
    sample: dict[str, Any], field: str
) -> dict[str, dt.datetime] | None:
    """Prometheus scrape generation for each pod in one numeric series."""
    rows = sample.get(field)
    if not isinstance(rows, list) or not rows:
        return None
    result: dict[str, dt.datetime] = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("pod"), str):
            return None
        stamp = row.get("sample_time")
        if not isinstance(stamp, (int, float)) or isinstance(stamp, bool):
            return None
        if row["pod"] in result:
            return None
        result[row["pod"]] = dt.datetime.fromtimestamp(float(stamp), dt.timezone.utc)
    return result


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
    backlog_scraped_at: dict[str, dt.datetime] | None
    processes: list[dict[str, str]] | None


def recovery_drain_interval(
    observations: list[Observation],
    expected_pods: set[str],
    restoration_applied: RecordedStamp | None,
) -> tuple[dt.datetime | None, dict[str, Any] | None]:
    """Bracket a positive-to-zero recovery episode by scrape generations.

    Poll timestamps cannot date the state Prometheus returned. A drain is
    supported only after every expected pod has an advancing all-zero scrape
    newer than the latest positive scrape in the episode. Repeated instant
    queries against one scrape generation therefore neither narrow nor invent
    an interval.
    """
    if not expected_pods or restoration_applied is None:
        return None, None

    latest_positive_scrape: dt.datetime | None = None
    for observation in observations:
        scrape_times = observation.backlog_scraped_at
        if scrape_times is None or expected_pods - scrape_times.keys():
            continue

        for pod in expected_pods:
            if observation.backlog.get(pod, 0) > 0:
                stamp = scrape_times[pod]
                if latest_positive_scrape is None or stamp > latest_positive_scrape:
                    latest_positive_scrape = stamp

        if (
            observation.phase != "recovery"
            or latest_positive_scrape is None
            or any(observation.backlog.get(pod) != 0 for pod in expected_pods)
        ):
            continue

        zero_scrapes = [scrape_times[pod] for pod in expected_pods]
        if any(stamp <= latest_positive_scrape for stamp in zero_scrapes):
            continue
        if any(stamp < restoration_applied.until for stamp in zero_scrapes):
            continue

        last_zero_scrape = max(zero_scrapes)
        lower_seconds = max(
            0.0,
            (latest_positive_scrape - restoration_applied.until).total_seconds(),
        )
        upper_seconds = (
            last_zero_scrape - restoration_applied.at
        ).total_seconds()
        return observation.at, {
            "lower_bound": lower_seconds,
            "upper_bound": upper_seconds,
            "restoration_boundary": "restoration_applied_at",
            "last_positive_scrape_at": latest_positive_scrape.isoformat().replace(
                "+00:00", "Z"
            ),
            "all_pods_zero_by_scrape_at": last_zero_scrape.isoformat().replace(
                "+00:00", "Z"
            ),
        }

    return None, None


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


def parse_pg_stamp(value: Any) -> dt.datetime | None:
    """A `timestamptz` as psql prints it, or None for a NULL or unusable one."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace(" ", "T")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    elif len(text) > 3 and text[-3] in "+-":
        text += ":00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def grade_commit_times(
    document: dict[str, Any],
    accepted: list[dict[str, Any]],
    outage_started: RecordedStamp | None,
    pause_applied: RecordedStamp | None,
    restoration_started: RecordedStamp | None,
    restoration_applied: RecordedStamp | None,
    problems: list[str],
) -> dict[str, Any]:
    """Date each accepted job's row version by its Postgres commit timestamp.

    `pg_xact_commit_timestamp(xmin)` dates the row version that is visible now,
    which is not the same thing as every status transition the job made: the
    amendment at `crates/siglake-query-server/src/jobs.rs:2313` rewrites an
    already-terminal row, so a recovered job's latest commit can postdate a
    terminal write that landed earlier. Anything the reading cannot place -- a
    missing row, a NULL timestamp, a job still short of a terminal status, a
    recovered row, or a commit whose recorded interval overlaps a signal
    transition -- is kept as a gap, and a gap is what stops this reading from
    settling the pause question either way. Timestamp intervals come from the
    precision retained in each field, preserving a one-second interval for
    historical traces while using the probe's current millisecond precision.
    """
    reading: dict[str, Any] = {
        "collected_at": None,
        "track_commit_timestamp": None,
        "rows_returned": None,
        "correlated_jobs": 0,
        "uncorrelated_rows": None,
        "committed_before_pause": 0,
        "committed_in_pause": [],
        "committed_after_restoration": 0,
        "unplaceable_commits": [],
        "signal_boundaries": None,
        "gaps": [],
        "settles_pause": False,
    }
    observation = document.get("job_commit_times")
    if not isinstance(observation, dict):
        problems.append("missing job-row commit-time observations")
        return reading
    rows = observation.get("rows")
    if not isinstance(rows, list):
        rows = []
    reading["collected_at"] = observation.get("at")
    reading["track_commit_timestamp"] = observation.get("track_commit_timestamp")
    reading["rows_returned"] = len(rows)
    gaps: list[str] = reading["gaps"]

    if observation.get("exec_status") != 0 or observation.get("query_status") != 0:
        problems.append(
            "the job-row commit-time query did not run: "
            f"{str(observation.get('detail', ''))[:160]!r}"
        )
        gaps.append("the commit-time query did not run")
    if reading["track_commit_timestamp"] != "on":
        problems.append(
            "Postgres did not track commit timestamps "
            f"(track_commit_timestamp={reading['track_commit_timestamp']!r}), so no job row can be dated"
        )
        gaps.append("commit timestamps were not tracked")

    by_id: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("job_id"), str) or not row["job_id"]:
            gaps.append("a retained commit-time row has no job id")
            continue
        by_id.setdefault(row["job_id"], []).append(row)

    boundaries = (outage_started, pause_applied, restoration_started, restoration_applied)
    if any(stamp is None for stamp in boundaries):
        gaps.append(
            "the trace has no complete pause and restoration bounds to place commit timestamps against"
        )
    else:
        assert all(stamp is not None for stamp in boundaries)
        reading["signal_boundaries"] = {
            "pause_transition": {
                "earliest": outage_started.at.isoformat(),
                "latest": pause_applied.until.isoformat(),
            },
            "proven_stopped": {
                "earliest": pause_applied.until.isoformat(),
                "latest": restoration_started.at.isoformat(),
            },
            "restoration_transition": {
                "earliest": restoration_started.at.isoformat(),
                "latest": restoration_applied.until.isoformat(),
            },
        }

    seen: set[str] = set()
    for index, submission in enumerate(accepted):
        job_id = submission.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            gaps.append(f"accepted submission {index} has no job id to correlate")
            continue
        seen.add(job_id)
        matches = by_id.get(job_id, [])
        if not matches:
            gaps.append(f"no job row for accepted job {job_id}")
            continue
        if len(matches) > 1:
            gaps.append(f"job {job_id} has {len(matches)} rows")
            continue
        row = matches[0]
        reading["correlated_jobs"] += 1
        status = row.get("status")
        if status not in TERMINAL_STATUSES:
            gaps.append(
                f"job {job_id} is {status!r} after the recovery window, so its row is not the "
                "terminal write"
            )
        if row.get("recovered_at"):
            gaps.append(
                f"job {job_id} was recovered at {row['recovered_at']}, so its visible row version "
                "may be an amendment of an earlier terminal write"
            )
        if parse_pg_stamp(row.get("committed_at")) is None:
            gaps.append(f"job {job_id} has no usable commit timestamp")

    reading["uncorrelated_rows"] = len([job_id for job_id in by_id if job_id not in seen])
    # Every row in the table is dated, not only the ones this burst submitted: a
    # job row from earlier in the round that committed while the processes were
    # stopped is the same finding, and the completion counter the samples read
    # does not distinguish them either.
    for job_id, matches in sorted(by_id.items()):
        for row in matches:
            committed_at = parse_pg_stamp(row.get("committed_at"))
            committed = recorded_stamp(row.get("committed_at"), committed_at)
            if committed is None or any(stamp is None for stamp in boundaries):
                continue
            assert outage_started is not None
            assert pause_applied is not None
            assert restoration_started is not None
            assert restoration_applied is not None
            stamp = committed.at.isoformat()
            if committed.until <= outage_started.at:
                reading["committed_before_pause"] += 1
            elif (
                committed.at >= pause_applied.until
                and committed.until <= restoration_started.at
            ):
                reading["committed_in_pause"].append(job_id)
                problems.append(
                    f"job {job_id} committed at {stamp}, inside the proven stopped window from "
                    f"{pause_applied.until.isoformat()} through "
                    f"{restoration_started.at.isoformat()}, so the pause did not block writes"
                )
            elif committed.at >= restoration_applied.until:
                reading["committed_after_restoration"] += 1
            else:
                reading["unplaceable_commits"].append(job_id)
                if committed.until > outage_started.at and committed.at < pause_applied.until:
                    transition = "pause"
                    started = outage_started
                    applied = pause_applied
                elif (
                    committed.until > restoration_started.at
                    and committed.at < restoration_applied.until
                ):
                    transition = "restoration"
                    started = restoration_started
                    applied = restoration_applied
                else:
                    transition = "signal-boundary"
                    started = outage_started
                    applied = restoration_applied
                problems.append(
                    f"job {job_id} committed at {stamp}, overlapping the {transition} transition "
                    f"bounded by {started.at.isoformat()} ({resolution_label(started)} precision) "
                    f"and {applied.until.isoformat()} ({resolution_label(applied)} precision), so "
                    "the commit cannot be placed inside or outside the proven stopped window"
                )

    if not accepted:
        gaps.append("no accepted submission to correlate commit times with")
    reading["settles_pause"] = (
        not gaps
        and not reading["committed_in_pause"]
        and not reading["unplaceable_commits"]
        and reading["correlated_jobs"] > 0
    )
    if gaps:
        problems.append("job-row commit times are incomplete: " + "; ".join(gaps))
    return reading


def grade(document: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    if document.get("schema_version") not in {1, 2, 3}:
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
    # Read after the exec that carries each signal, so they bound the interval a
    # commit timestamp has to fall in to be a write taken while the processes
    # were stopped.
    pause_applied_at = parse_stamp(
        timestamps.get("pause_applied_at"), "pause_applied_at", problems
    )
    restoration_applied_at = parse_stamp(
        timestamps.get("restoration_applied_at"), "restoration_applied_at", problems
    )
    outage_stamp = recorded_stamp(timestamps.get("outage_started_at"), outage_at)
    pause_applied_stamp = recorded_stamp(
        timestamps.get("pause_applied_at"), pause_applied_at
    )
    restoration_started_stamp = recorded_stamp(
        timestamps.get("restoration_started_at"), restored_at
    )
    restoration_applied_stamp = recorded_stamp(
        timestamps.get("restoration_applied_at"), restoration_applied_at
    )
    if outage_at and restored_at and restored_at <= outage_at:
        problems.append("restoration did not follow the outage")
    if restored_at and ready_at and ready_at < restored_at:
        problems.append("Postgres-ready timestamp precedes restoration")
    if outage_at and pause_applied_at and pause_applied_at < outage_at:
        problems.append("the pause was applied before the outage window opened")
    if restored_at and pause_applied_at and pause_applied_at > restored_at:
        problems.append("the pause was applied after restoration")
    if restored_at and restoration_applied_at and restoration_applied_at < restored_at:
        problems.append("restoration completed before it started")

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
                    backlog_scraped_at=series_scrape_times(sample, "backlog"),
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
    commit_times = grade_commit_times(
        document,
        accepted,
        outage_stamp,
        pause_applied_stamp,
        restoration_started_stamp,
        restoration_applied_stamp,
        problems,
    )

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
    # The commit-time reading is what can answer it: the messages collected here
    # are only raised when that reading cannot place every accepted job's write
    # outside the pause.
    pre_restoration_drain: dict[str, Any] | None = None
    drain_problems: list[str] = []
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
            drain_problems.append(
                f"outage sample at {stamp} shows zero backlog with completions up by {delta:g}, "
                f"from a Prometheus scrape at {scraped.isoformat()} taken before the pause: "
                "delayed observation of pre-pause work, not a write during the pause"
            )
        else:
            drain_problems.append(
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
                "resolution": None,
            }

    if pre_restoration_drain is not None:
        if commit_times["settles_pause"]:
            # Every accepted job's row version is dated, terminal, unamended and
            # committed outside the pause, so the completion the counter showed
            # was observed work rather than a write that landed while Postgres
            # was stopped. This is the only reading that clears the observation;
            # a counter cannot.
            pre_restoration_drain["resolution"] = {
                "state": "resolved",
                "by": "job_commit_times",
                "detail": (
                    f"{commit_times['correlated_jobs']} accepted job rows are dated by commit "
                    f"timestamp, {commit_times['committed_after_restoration']} after restoration "
                    f"and {commit_times['committed_before_pause']} before the pause, none inside "
                    "it: the completion was observed work, not a write that landed during the pause"
                ),
            }
        else:
            pre_restoration_drain["resolution"] = {
                "state": "unresolved",
                "by": "job_commit_times",
                "detail": (
                    "the commit-time reading cannot place every accepted job's write outside the "
                    "pause"
                    + (
                        "; commits inside the pause: " + ", ".join(commit_times["committed_in_pause"])
                        if commit_times["committed_in_pause"]
                        else ""
                    )
                    + (
                        "; gaps: " + "; ".join(commit_times["gaps"])
                        if commit_times["gaps"]
                        else ""
                    )
                    + (
                        "; unplaceable commits: " + ", ".join(commit_times["unplaceable_commits"])
                        if commit_times["unplaceable_commits"]
                        else ""
                    )
                ),
            }
            problems.extend(drain_problems)

    drained_at: dt.datetime | None = None
    time_to_drain_seconds: dict[str, Any] | None = None
    if positive and restored_at:
        drained_at, time_to_drain_seconds = recovery_drain_interval(
            parsed_samples, expected_set, restoration_applied_stamp
        )
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
        "time_to_drain_seconds": time_to_drain_seconds,
        "pre_restoration_drain": pre_restoration_drain,
        "job_commit_times": commit_times,
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
    commits = result["summary"]["job_commit_times"]
    drain = result["summary"]["pre_restoration_drain"]
    print(
        "POSTGRES_OUTAGE_EVIDENCE "
        f"grade={result['grade']} peak={result['summary']['peak_backlog_total']} "
        f"drain_seconds={result['summary']['time_to_drain_seconds']} "
        f"job_rows_committed_in_pause={len(commits['committed_in_pause'])} "
        f"job_rows_dated={commits['correlated_jobs']} "
        f"pre_restoration_drain="
        f"{(drain['resolution'] or {}).get('state', 'unresolved') if drain else 'none'}",
        file=sys.stderr,
    )
    for problem in result["problems"]:
        print(f"  {problem}", file=sys.stderr)
    return 0 if result["grade"] == "verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
