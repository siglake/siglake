#!/usr/bin/env python3
"""Grade a retained kind schema-rollback trace without touching a cluster.

`verified` means every arm of task #2364 has evidence in the trace:

  * two images, from one pinned repository revision, with DIFFERENT ids and the
    probe feature recorded against the second one;
  * the chart arm's six steps, in order, each running the image its step
    expects: ingest under A, upgrade to B (one migration Job for that
    revision, completed, on image B, and the table widened), ingest under B,
    `helm rollback` to A (no migration Job for the rollback revision, the
    column still present), ingest after the rollback, upgrade forward (a
    migration Job under the NEW revision's name, so the immutable-Job 422 did
    not happen);
  * the rows written under B keep their value across the rollback while the
    rows written after it read null, with the null bucket the only one that
    grows;
  * every row count read through the distributed endpoint, with a cross-shard
    `GROUP BY` whose per-key counts sum to it;
  * the operator arm's four steps: install at A, `spec.image` to B, revert to A
    while A's Job is retained (same Job, same uid: reuse, not recreation), and
    revert again after deleting it (same Job NAME, a new uid: recreation), with
    `spec.schemaVersion` constant throughout and
    `siglake_operator_rollout_held_total` recorded at every step and rising
    when the operator held a rollout.

Anything missing is `unverified`, with the reason named. Nothing here is a
threshold: the grade is about whether the observations exist and agree, not
about how fast anything was.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any

CHART_STEPS = (
    "ingest_under_a",
    "upgrade_to_b",
    "ingest_under_b",
    "rollback_to_a",
    "ingest_after_rollback",
    "upgrade_forward_to_b",
)
OPERATOR_STEPS = (
    "install_at_a",
    "upgrade_to_b",
    "revert_to_a_retained",
    "revert_to_a_recreated",
)
DISTRIBUTED_MODES = {"aggregate", "ordered_aggregate"}


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def named_steps(
    container: Any, expected: tuple[str, ...], arm: str, problems: list[str]
) -> dict[str, dict[str, Any]]:
    """The arm's steps by name, requiring exactly the expected names in order."""
    if not isinstance(container, dict):
        problems.append(f"missing {arm} arm")
        return {}
    steps = container.get("steps")
    if not isinstance(steps, list) or not steps:
        problems.append(f"the {arm} arm recorded no steps")
        return {}
    names = [step.get("name") if isinstance(step, dict) else None for step in steps]
    if names != list(expected):
        problems.append(
            f"the {arm} arm's steps are {names!r}, expected {list(expected)!r} in that order"
        )
    return {
        step["name"]: step
        for step in steps
        if isinstance(step, dict) and isinstance(step.get("name"), str)
    }


def grade_rows(step: dict[str, Any], label: str, problems: list[str]) -> dict[str, float]:
    """Check one step's row observation and return its per-key counts.

    Keys are the rendered group values, with `null` for the SQL null. An
    unusable observation is a problem AND an empty result, so a caller comparing
    two steps cannot read a missing bucket as a zero one.
    """
    rows = step.get("rows")
    if not isinstance(rows, dict):
        problems.append(f"{label} has no row observation")
        return {}
    endpoint = rows.get("endpoint")
    if not isinstance(endpoint, str) or not endpoint.endswith("/api/v1/sql"):
        problems.append(
            f"{label} did not read its rows through the distributed /api/v1/sql "
            f"endpoint (endpoint={endpoint!r})"
        )
    total = rows.get("total")
    if not is_number(total) or total < 0:
        problems.append(f"{label} has no usable total row count")
        total = None
    mode = rows.get("distributed_mode")
    if mode not in DISTRIBUTED_MODES:
        problems.append(
            f"{label}'s GROUP BY did not fan out across shards (distributed mode={mode!r})"
        )
    groups = rows.get("groups")
    if not isinstance(groups, list) or not groups:
        problems.append(f"{label} recorded no GROUP BY buckets")
        return {}
    counts: dict[str, float] = {}
    for index, row in enumerate(groups):
        if not isinstance(row, dict) or not is_number(row.get("n")):
            problems.append(f"{label}'s GROUP BY bucket {index} has no usable count")
            return {}
        key = row.get("key")
        if key is not None and not isinstance(key, str):
            problems.append(f"{label}'s GROUP BY bucket {index} has a non-string key {key!r}")
            return {}
        name = "null" if key is None else key
        if name in counts:
            problems.append(f"{label}'s GROUP BY repeats the key {name!r}")
            return {}
        counts[name] = float(row["n"])
    if total is not None and sum(counts.values()) != total:
        problems.append(
            f"{label}'s per-key counts sum to {sum(counts.values())}, "
            f"not the {total} rows count(*) reported"
        )
    return counts


def step_image(step: dict[str, Any], label: str, problems: list[str]) -> None:
    """Every workload pod in the step runs the image the step expects."""
    expected = step.get("expected_image")
    pods = step.get("pods")
    if not isinstance(expected, str) or not expected:
        problems.append(f"{label} does not name the image it expected")
        return
    if not isinstance(pods, list) or not pods:
        problems.append(f"{label} recorded no pod images")
        return
    wrong = sorted(
        f"{row.get('pod')}={row.get('image')}"
        for row in pods
        if isinstance(row, dict)
        and row.get("component") in {"ingester", "compactor", "query"}
        and row.get("image") != expected
    )
    if wrong:
        problems.append(f"{label} expected {expected} but pods ran: {', '.join(wrong)}")
    if not any(
        isinstance(row, dict) and row.get("component") in {"ingester", "compactor", "query"}
        for row in pods
    ):
        problems.append(f"{label} recorded no ingester, compactor or query pod")


def jobs_for_revision(step: dict[str, Any]) -> list[dict[str, Any]]:
    revision = step.get("revision")
    jobs = step.get("migration_jobs")
    if not isinstance(jobs, list) or not isinstance(revision, int):
        return []
    suffix = f"-{revision}"
    return [
        job
        for job in jobs
        if isinstance(job, dict)
        and isinstance(job.get("name"), str)
        and job["name"].endswith(suffix)
    ]


def grade_chart(document: dict[str, Any], problems: list[str]) -> dict[str, Any]:
    settings = document.get("settings")
    settings = settings if isinstance(settings, dict) else {}
    probe_value = settings.get("probe_value")
    probe_key = str(probe_value) if probe_value is not None else "1"
    image_a = ((document.get("revisions") or {}).get("image_a") or {}).get("tag")
    image_b = ((document.get("revisions") or {}).get("image_b") or {}).get("tag")

    steps = named_steps(document.get("chart_arm"), CHART_STEPS, "chart", problems)
    counts: dict[str, dict[str, float]] = {}
    for name in CHART_STEPS:
        step = steps.get(name)
        if step is None:
            continue
        label = f"chart step {name}"
        counts[name] = grade_rows(step, label, problems)
        step_image(step, label, problems)
        wanted = image_a if name in {"ingest_under_a", "rollback_to_a", "ingest_after_rollback"} else image_b
        if isinstance(wanted, str) and step.get("expected_image") != wanted:
            problems.append(
                f"{label} expected image {step.get('expected_image')!r}, "
                f"but this step of the round runs {wanted!r}"
            )

    # The widen: the upgrade to B ran ONE migration Job under that revision's
    # name, it completed on image B, and the probe column answers a GROUP BY
    # from then on.
    upgrade = steps.get("upgrade_to_b")
    upgrade_job: dict[str, Any] | None = None
    if upgrade is not None:
        matching = jobs_for_revision(upgrade)
        if len(matching) != 1:
            problems.append(
                f"the upgrade to image B ran {len(matching)} migration Jobs named for "
                f"revision {upgrade.get('revision')!r}, expected exactly 1"
            )
        else:
            upgrade_job = matching[0]
            if not is_number(upgrade_job.get("succeeded")) or upgrade_job["succeeded"] < 1:
                problems.append(
                    f"the migration Job {upgrade_job.get('name')!r} did not report a success"
                )
            if not upgrade_job.get("completed_at"):
                problems.append(
                    f"the migration Job {upgrade_job.get('name')!r} has no completion time"
                )
            if upgrade_job.get("image") != image_b:
                problems.append(
                    f"the migration Job ran {upgrade_job.get('image')!r}; the pre-upgrade hook "
                    f"has to run the NEW image ({image_b!r}), which is what declares the new schema"
                )
        if upgrade.get("probe_column_present") is not True:
            problems.append(
                "the table did not widen: the probe column did not answer a GROUP BY after "
                "the upgrade to image B"
            )

    # The rollback: no migration Job for its revision, the column still there.
    rollback = steps.get("rollback_to_a")
    if rollback is not None:
        ran = jobs_for_revision(rollback)
        if ran:
            problems.append(
                "the rollback ran a migration Job ("
                + ", ".join(sorted(str(job.get("name")) for job in ran))
                + "); a rollback must not migrate -- the chart's hook is pre-upgrade only"
            )
        if rollback.get("probe_column_present") is not True:
            problems.append(
                "the widened column was gone after the rollback: an additive widen is not undone "
                "by rolling the image back"
            )

    # Forward again: a Job under the new revision's name. A reused name would
    # have 422'd on the immutable pod template instead.
    forward = steps.get("upgrade_forward_to_b")
    if forward is not None:
        matching = jobs_for_revision(forward)
        if len(matching) != 1:
            problems.append(
                f"the forward upgrade ran {len(matching)} migration Jobs named for revision "
                f"{forward.get('revision')!r}, expected exactly 1"
            )
        elif upgrade_job is not None and matching[0].get("name") == upgrade_job.get("name"):
            problems.append(
                "the forward upgrade reused the earlier migration Job name "
                f"{upgrade_job.get('name')!r}; the Job name has to carry the release revision"
            )
        if isinstance(forward.get("revision"), int) and isinstance(
            upgrade.get("revision") if upgrade else None, int
        ):
            if forward["revision"] <= upgrade["revision"]:
                problems.append(
                    "the forward upgrade did not advance the release revision past the first "
                    "upgrade to image B"
                )
        if forward.get("probe_column_present") is not True:
            problems.append("the probe column was gone after the forward upgrade")

    # The values: B's rows keep theirs, A's rows read null, and only the null
    # bucket grows.
    summary: dict[str, Any] = {}
    under_b = counts.get("ingest_under_b") or {}
    after_rollback = counts.get("ingest_after_rollback") or {}
    if under_b and after_rollback:
        b_before = under_b.get(probe_key)
        b_after = after_rollback.get(probe_key)
        null_before = under_b.get("null")
        null_after = after_rollback.get("null")
        if b_before is None or b_after is None:
            problems.append(
                f"no {probe_key!r} bucket on both sides of the rollback, so nothing shows the "
                "rows image B wrote keeping their value"
            )
        elif b_before <= 0:
            problems.append(
                f"image B wrote no rows carrying {probe_key!r}: the probe column was present but "
                "never populated, so the rollback has nothing to preserve"
            )
        elif b_after != b_before:
            problems.append(
                f"the rows image B wrote changed across the rollback: {b_before} -> {b_after} "
                f"rows with {probe_key!r}"
            )
        if null_before is None or null_after is None:
            problems.append("no null bucket on both sides of the rollback")
        elif null_after <= null_before:
            problems.append(
                f"the rolled-back image A wrote no rows reading null: null bucket "
                f"{null_before} -> {null_after}"
            )
        else:
            summary["null_rows_added_after_rollback"] = null_after - null_before
        if is_number(b_before):
            summary["rows_written_by_image_b"] = b_before
    elif steps:
        problems.append("the rollback's before/after row observations are not both usable")

    if upgrade_job is not None:
        summary["upgrade_migration_job"] = upgrade_job.get("name")
    if rollback is not None:
        summary["rollback_revision"] = rollback.get("revision")
    return summary


def grade_operator(document: dict[str, Any], problems: list[str]) -> dict[str, Any]:
    image_a = ((document.get("revisions") or {}).get("image_a") or {}).get("tag")
    image_b = ((document.get("revisions") or {}).get("image_b") or {}).get("tag")
    steps = named_steps(document.get("operator_arm"), OPERATOR_STEPS, "operator", problems)

    versions: set[Any] = set()
    held: list[tuple[str, float | None]] = []
    jobs: dict[str, dict[str, Any]] = {}
    cr_generations: dict[str, int] = {}
    for name in OPERATOR_STEPS:
        step = steps.get(name)
        if step is None:
            continue
        label = f"operator step {name}"
        wanted = image_b if name == "upgrade_to_b" else image_a
        if isinstance(wanted, str) and step.get("requested_image") != wanted:
            problems.append(
                f"{label} has requested_image={step.get('requested_image')!r}, expected {wanted!r}"
            )
        if isinstance(wanted, str) and step.get("spec_image") != wanted:
            problems.append(
                f"{label} has spec.image={step.get('spec_image')!r}, expected {wanted!r}"
            )
        generation = step.get("generation")
        observed_generation = step.get("observed_generation")
        if not isinstance(generation, int) or generation < 1:
            problems.append(f"{label} did not record a positive CR generation")
        elif observed_generation != generation:
            problems.append(
                f"{label} has stale operator status: generation {generation}, "
                f"observedGeneration {observed_generation!r}"
            )
        else:
            cr_generations[name] = generation

        deployments = step.get("deployments")
        statefulsets = step.get("statefulsets")
        pods = step.get("pods")
        if not isinstance(deployments, list) or not deployments:
            problems.append(f"{label} observed no operator-managed Deployments")
            deployments = []
        if not isinstance(statefulsets, list) or not statefulsets:
            problems.append(f"{label} observed no operator-managed StatefulSets")
            statefulsets = []
        if not isinstance(pods, list) or not pods:
            problems.append(f"{label} observed no operator-managed pods")
            pods = []

        desired_by_component: dict[str, int] = {}
        workload_components: set[str] = set()
        components_by_kind: dict[str, set[str]] = {"Deployment": set(), "StatefulSet": set()}
        for kind, workloads in (("Deployment", deployments), ("StatefulSet", statefulsets)):
            for workload in workloads:
                if not isinstance(workload, dict):
                    problems.append(f"{label} has a malformed {kind} observation")
                    continue
                workload_name = workload.get("name")
                component = workload.get("component")
                prefix = f"{label} {kind} {workload_name!r}"
                if not workload_name or not isinstance(component, str) or not component:
                    problems.append(f"{prefix} has no name or component label")
                    continue
                workload_components.add(component)
                components_by_kind[kind].add(component)
                desired = workload.get("desired_replicas")
                if not isinstance(desired, int) or desired < 1:
                    problems.append(f"{prefix} has invalid desired replicas {desired!r}")
                    continue
                if component in desired_by_component:
                    problems.append(f"{label} observed more than one workload for {component!r}")
                desired_by_component[component] = desired
                if workload.get("image") != wanted:
                    problems.append(
                        f"{prefix} still has image {workload.get('image')!r}, expected {wanted!r}"
                    )
                workload_generation = workload.get("generation")
                if not isinstance(workload_generation, int) or workload_generation < 1:
                    problems.append(f"{prefix} has no positive generation")
                elif workload.get("observed_generation") != workload_generation:
                    problems.append(
                        f"{prefix} rollout is stale: generation {workload_generation}, "
                        f"observedGeneration {workload.get('observed_generation')!r}"
                    )
                for field in ("updated_replicas", "ready_replicas"):
                    if workload.get(field) != desired:
                        problems.append(
                            f"{prefix} has {field}={workload.get(field)!r}, expected {desired}"
                        )
                if kind == "Deployment":
                    if workload.get("available_replicas") != desired:
                        problems.append(
                            f"{prefix} has available_replicas="
                            f"{workload.get('available_replicas')!r}, expected {desired}"
                        )
                else:
                    if workload.get("current_replicas") != desired:
                        problems.append(
                            f"{prefix} has current_replicas="
                            f"{workload.get('current_replicas')!r}, expected {desired}"
                        )
                    current_revision = workload.get("current_revision")
                    if not current_revision or current_revision != workload.get("update_revision"):
                        problems.append(
                            f"{prefix} has not reached one revision: current={current_revision!r}, "
                            f"update={workload.get('update_revision')!r}"
                        )

        missing_components = {"ingester", "compactor", "query"} - workload_components
        if missing_components:
            problems.append(
                f"{label} is missing operator-managed workloads for "
                + ", ".join(sorted(missing_components))
            )
        missing_deployments = {"ingester", "compactor"} - components_by_kind["Deployment"]
        if missing_deployments:
            problems.append(
                f"{label} is missing operator-managed Deployments for "
                + ", ".join(sorted(missing_deployments))
            )
        if "query" not in components_by_kind["StatefulSet"]:
            problems.append(f"{label} is missing the operator-managed query StatefulSet")
        pod_counts: dict[str, int] = {}
        for pod in pods:
            if not isinstance(pod, dict):
                problems.append(f"{label} has a malformed pod observation")
                continue
            pod_name = pod.get("name")
            component = pod.get("component")
            prefix = f"{label} pod {pod_name!r}"
            if not pod_name or component not in desired_by_component:
                problems.append(f"{prefix} has no matching operator-managed workload")
                continue
            pod_counts[component] = pod_counts.get(component, 0) + 1
            if pod.get("image") != wanted:
                problems.append(
                    f"{prefix} still runs image {pod.get('image')!r}, expected {wanted!r}"
                )
            if pod.get("phase") != "Running" or pod.get("ready") is not True:
                problems.append(
                    f"{prefix} is not ready and running: phase={pod.get('phase')!r}, "
                    f"ready={pod.get('ready')!r}"
                )
        for component, desired in desired_by_component.items():
            if pod_counts.get(component, 0) != desired:
                problems.append(
                    f"{label} observed {pod_counts.get(component, 0)} pods for {component!r}, "
                    f"expected {desired}"
                )
        versions.add(step.get("spec_schema_version"))
        job = step.get("job")
        if not isinstance(job, dict) or not job.get("name") or not job.get("uid"):
            problems.append(f"{label} observed no migration Job for its image")
        else:
            jobs[name] = job
            if not is_number(job.get("succeeded")) or job["succeeded"] < 1:
                problems.append(
                    f"{label}'s migration Job {job.get('name')!r} did not report a success"
                )
            if not job.get("created_at"):
                problems.append(f"{label}'s migration Job has no creation time")
            if not job.get("completed_at"):
                problems.append(f"{label}'s migration Job has no completion time")
        counter = step.get("rollout_held_total")
        if not isinstance(counter, dict) or "present" not in counter:
            problems.append(f"{label} did not record siglake_operator_rollout_held_total")
            held.append((name, None))
        elif counter.get("present") is True and is_number(counter.get("value")):
            held.append((name, float(counter["value"])))
        elif counter.get("present") is False:
            # Absent: the counter is not pre-registered, so it does
            # not exist before the first hold. Read as zero only here.
            held.append((name, 0.0))
        else:
            problems.append(
                f"{label} recorded siglake_operator_rollout_held_total as present with no value"
            )
            held.append((name, None))

    install_generation = cr_generations.get("install_at_a")
    upgrade_generation = cr_generations.get("upgrade_to_b")
    retained_generation = cr_generations.get("revert_to_a_retained")
    recreated_generation = cr_generations.get("revert_to_a_recreated")
    if (
        install_generation is not None
        and upgrade_generation is not None
        and upgrade_generation <= install_generation
    ):
        problems.append(
            "the image B operator step did not advance the CR generation: "
            f"{install_generation} -> {upgrade_generation}"
        )
    if (
        upgrade_generation is not None
        and retained_generation is not None
        and retained_generation <= upgrade_generation
    ):
        problems.append(
            "the retained-Job image A revert did not advance the CR generation: "
            f"{upgrade_generation} -> {retained_generation}"
        )
    if (
        retained_generation is not None
        and recreated_generation is not None
        and recreated_generation != retained_generation
    ):
        problems.append(
            "deleting the retained Job and annotating the CR unexpectedly changed its generation: "
            f"{retained_generation} -> {recreated_generation}"
        )

    if len(versions) > 1:
        problems.append(
            "spec.schemaVersion changed during the operator arm ("
            + ", ".join(sorted(repr(v) for v in versions))
            + "); the revert has to be image-only"
        )
    if versions and next(iter(versions)) in (None, 0):
        problems.append(
            "the CR did not set spec.schemaVersion, so the operator renders no migration Job at all"
        )

    install = jobs.get("install_at_a")
    upgrade = jobs.get("upgrade_to_b")
    retained = jobs.get("revert_to_a_retained")
    recreated = jobs.get("revert_to_a_recreated")

    if install and upgrade and install.get("name") == upgrade.get("name"):
        problems.append(
            f"the operator rendered the same Job name {install.get('name')!r} for both images; "
            "the name has to carry the template digest or a revert could never re-run it"
        )
    if install and retained:
        if retained.get("name") != install.get("name"):
            problems.append(
                f"the revert rendered Job {retained.get('name')!r}, not the {install.get('name')!r} "
                "it rendered for the same image before"
            )
        if retained.get("uid") != install.get("uid"):
            problems.append(
                "the retained-Job revert did not reuse the completed Job: uid "
                f"{install.get('uid')!r} -> {retained.get('uid')!r}. Either the Job was reaped "
                "before this step or the operator recreated it, so this step shows recreation "
                "rather than reuse"
            )
    if retained and recreated:
        if recreated.get("name") != retained.get("name"):
            problems.append(
                f"the recreated Job is named {recreated.get('name')!r}, not {retained.get('name')!r}: "
                "the same image and schema version have to render the same name"
            )
        if recreated.get("uid") == retained.get("uid"):
            problems.append(
                "the deleted Job was not recreated: the same uid "
                f"{retained.get('uid')!r} is still there, so this step shows nothing about an "
                "absent Job"
            )
    recreated_step = steps.get("revert_to_a_recreated")
    if recreated_step is not None and not recreated_step.get("deleted_job_uid"):
        problems.append(
            "the recreation step does not record the uid of the Job it deleted, so 'a new uid' "
            "cannot be told from 'a different Job'"
        )

    values = [value for _, value in held]
    if any(value is None for value in values):
        pass  # already reported
    else:
        for (before_name, before), (after_name, after) in zip(held, held[1:]):
            if after < before:
                problems.append(
                    f"siglake_operator_rollout_held_total fell from {before} at {before_name} to "
                    f"{after} at {after_name}; a counter cannot decrease"
                )
        first = dict(held).get("install_at_a")
        upgrade_held = dict(held).get("upgrade_to_b")
        if first is not None and upgrade_held is not None and upgrade_held <= first:
            problems.append(
                "the operator never held a rollout while the new image's migration Job ran "
                f"(siglake_operator_rollout_held_total {first} -> {upgrade_held}); the rollout gate "
                "is what stops the new pods writing before the table is widened"
            )

    return {
        "job_names": {name: job.get("name") for name, job in jobs.items()},
        "job_uids": {name: job.get("uid") for name, job in jobs.items()},
        "generations": {
            name: {
                "generation": step.get("generation"),
                "observed_generation": step.get("observed_generation"),
            }
            for name, step in steps.items()
        },
        "rollout_held_total": dict(held),
        "spec_schema_version": next(iter(versions)) if len(versions) == 1 else None,
    }


def grade(document: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    if document.get("schema_version") != 1:
        problems.append("unsupported or missing schema_version")

    revisions = document.get("revisions")
    if not isinstance(revisions, dict):
        revisions = {}
        problems.append("missing pinned revisions")
    if not revisions.get("repository_commit"):
        problems.append("missing pinned repository revision")
    image_a = revisions.get("image_a")
    image_b = revisions.get("image_b")
    for name, image in (("image_a", image_a), ("image_b", image_b)):
        if not isinstance(image, dict) or not image.get("tag") or not image.get("id"):
            problems.append(f"missing pinned {name} tag and id")
        elif image.get("source_revision") != revisions.get("repository_commit"):
            problems.append(
                f"{name} source revision {image.get('source_revision')!r} does not match "
                f"the pinned repository revision {revisions.get('repository_commit')!r}"
            )
    if isinstance(image_a, dict) and isinstance(image_b, dict):
        if image_a.get("id") and image_a.get("id") == image_b.get("id"):
            problems.append(
                "both images have the same id: the round measured one image against itself"
            )
        if not image_b.get("cargo_features"):
            problems.append(
                "image B records no cargo features, so nothing says it declares a different schema"
            )
        if image_a.get("cargo_features"):
            problems.append(
                "image A was built with cargo features "
                f"({image_a.get('cargo_features')!r}); the older image has to be the ordinary build"
            )
    operator_image = revisions.get("operator_image")
    if not isinstance(operator_image, dict) or not operator_image.get("id"):
        problems.append("missing pinned operator image")

    settings = document.get("settings")
    required = (
        "namespace",
        "release",
        "operator_namespace",
        "cr_name",
        "cr_schema_version",
        "chart_start_revision",
        "ingest_batch",
        "probe_column",
        "probe_value",
    )
    if not isinstance(settings, dict):
        problems.append("missing effective settings")
    else:
        missing = [name for name in required if name not in settings]
        if missing:
            problems.append("missing effective settings: " + ", ".join(missing))

    chart = grade_chart(document, problems)
    operator = grade_operator(document, problems)
    return {
        "grade": "verified" if not problems else "unverified",
        "problems": problems,
        "summary": {"chart_arm": chart, "operator_arm": operator},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    try:
        document = json.loads(args.input.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"ERROR: cannot read schema-rollback evidence: {error}", file=sys.stderr)
        return 2
    if not isinstance(document, dict):
        print("ERROR: schema-rollback evidence root is not an object", file=sys.stderr)
        return 2

    result = grade(document)
    graded = dict(document)
    graded["evidence"] = result
    rendered = json.dumps(graded, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    chart = result["summary"]["chart_arm"]
    print(
        "SCHEMA_ROLLBACK_EVIDENCE "
        f"grade={result['grade']} "
        f"image_b_rows={chart.get('rows_written_by_image_b')} "
        f"null_rows_after_rollback={chart.get('null_rows_added_after_rollback')} "
        f"operator_jobs={len(result['summary']['operator_arm'].get('job_names') or {})}",
        file=sys.stderr,
    )
    for problem in result["problems"]:
        print(f"  {problem}", file=sys.stderr)
    return 0 if result["grade"] == "verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
