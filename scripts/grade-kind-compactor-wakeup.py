#!/usr/bin/env python3
"""Grade the kind round's zero-replica compactor wake-up capture (#6011/#6012).

The capture answers one question: did a compactor tier the operator had PARKED
AT ZERO come back because the ingesters said there was work? Three things have
to hold, and each has a way of being absent that looks like success if nobody
checks for it:

1. **The tier really was at zero, with no compactor process.** A Deployment
   whose `spec.replicas` is 0 while a pod is still terminating is not a parked
   tier: that pod holds a claim and publishes the compactor's own gauge, so
   anything read in that window proves nothing about an independent signal.
2. **The reading advanced from zero to a positive, fresh published depth while
   nothing was running.** The parked control prevents an unrelated pre-existing
   backlog from being credited to the ingest this arm drives.
   The depth gauge keeps answering with its last value for the whole scrape
   staleness window after its publisher stops, so a depth without a sample age
   under the operator's allowance is not a current reading. A depth of zero,
   however fresh, is not a reason to start a worker.
3. **The tier came back afterwards**, in that order: parked, then the ingest,
   then a positive replica count.

The operator's own activation expression is evaluated in the same window and
has to agree with the depth the ingesters published. If it reads nothing, the
query the reconciler actually runs did not see what the capture saw, and the
capture is evidence about a query nobody runs.

This grades ONE capture. It is not the acceptance for the feature: a verified
document says a live cluster did this once, in this round.
"""

from __future__ import annotations

import argparse
import datetime
import re
import json
import math
import pathlib
import sys

COMMIT_SOURCES = {"git_rev_parse_head", "siglake_source_commit_env"}


class Unverified(Exception):
    """The capture does not establish the wake-up."""


def vector(queries: dict, name: str, what: str) -> list:
    query = queries.get(name) or {}
    response = query.get("response") or {}
    if response.get("status") != "success":
        raise Unverified(f"the {what} query did not succeed: {response.get('status')!r}")
    data = response.get("data") or {}
    if data.get("resultType") != "vector":
        raise Unverified(f"the {what} query returned {data.get('resultType')!r}, not a vector")
    return data.get("result") or []


def per_pod(rows: list, what: str, combine: str) -> dict[str, float]:
    """Fold a vector into one number per pod, summed or maximised."""
    out: dict[str, float] = {}
    for row in rows:
        pod = (row.get("metric") or {}).get("pod", "").strip()
        if not pod:
            raise Unverified(f"a {what} series carries no nonempty pod label")
        try:
            value = float(row["value"][1])
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise Unverified(f"a {what} series carries no parseable sample: {exc}") from exc
        if not math.isfinite(value):
            raise Unverified(f"a {what} series carries a non-finite sample ({value})")
        if combine == "sum":
            out[pod] = out.get(pod, 0.0) + value
        else:
            out[pod] = max(out.get(pod, value), value)
    if not out:
        raise Unverified(f"the {what} query matched no series at all")
    return out


def scalar(queries: dict, name: str, what: str) -> float:
    rows = vector(queries, name, what)
    if len(rows) != 1:
        raise Unverified(
            f"the {what} returned {len(rows)} series, not one scalar — "
            "an empty result is refused as a reading by the reconciler"
        )
    try:
        value = float(rows[0]["value"][1])
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise Unverified(f"the {what} carries no parseable sample: {exc}") from exc
    if not math.isfinite(value):
        raise Unverified(f"the {what} carries a non-finite sample ({value})")
    return value


def grade(document: dict) -> dict:
    if document.get("schema_version") != 2:
        raise Unverified(
            f"schema_version is {document.get('schema_version')!r}, not 2; "
            "the capture has no required pre-ingest control"
        )
    settings = document.get("settings") or {}
    revisions = document.get("revisions") or {}
    if not revisions.get("repository_commit"):
        raise Unverified("the capture pins no repository_commit")
    if revisions.get("repository_commit_source") not in COMMIT_SOURCES:
        raise Unverified(
            f"repository_commit_source is {revisions.get('repository_commit_source')!r}, "
            "so the commit's provenance is unknown"
        )

    history = document.get("replica_history") or []
    phases = [row.get("phase") for row in history]
    for required in ("parked", "ingested", "woken"):
        if required not in phases:
            raise Unverified(f"the replica history has no {required} phase")
    if not phases.index("parked") < phases.index("ingested") < phases.index("woken"):
        raise Unverified(
            f"the phases are out of order ({phases}): the ingest has to follow the park "
            "and precede the wake, or the tier was not woken by it"
        )

    parked = history[phases.index("parked")]
    if parked.get("replicas") != 0:
        raise Unverified(
            f"replicas at the parked phase were {parked.get('replicas')!r}, not 0"
        )
    if parked.get("pods"):
        raise Unverified(
            f"a compactor pod still existed while the tier read parked: {parked.get('pods')} — "
            "a terminating pod still holds its claim and publishes its own gauge"
        )
    if settings.get("pods_while_parked"):
        raise Unverified(
            f"the capture recorded compactor pods while parked: {settings['pods_while_parked']}"
        )

    ingested = history[phases.index("ingested")]
    if ingested.get("pods") or ingested.get("replicas") not in (0, None):
        raise Unverified(
            "the ingest did not happen with the tier parked: replicas "
            f"{ingested.get('replicas')!r}, pods {ingested.get('pods')}"
        )
    if not int(settings.get("ingest_batch") or 0) > 0:
        raise Unverified("the capture drove no ingest while the tier was parked")

    woken = history[phases.index("woken")]
    if not isinstance(woken.get("replicas"), int) or woken["replicas"] < 1:
        raise Unverified(
            f"the tier did not come back: replicas at the woken phase were "
            f"{woken.get('replicas')!r}"
        )

    queries = document.get("queries") or {}
    parked_operator_value = scalar(
        queries,
        "parked_operator_expression",
        "parked operator activation expression",
    )
    if parked_operator_value != 0.0:
        raise Unverified(
            f"the operator's expression already read {parked_operator_value:g} before ingest: "
            "the capture does not show this arm advancing the activation signal"
        )

    depth = per_pod(vector(queries, "published_depth", "published depth"), "published depth", "sum")
    age = per_pod(vector(queries, "sample_age", "sample age"), "sample age", "max")
    allowance = float(settings.get("sample_max_age_seconds") or 0)
    if allowance <= 0:
        raise Unverified("the capture records no sample-age allowance to judge freshness by")
    fresh = {pod: value for pod, value in depth.items() if age.get(pod, allowance + 1) <= allowance}
    if not fresh:
        raise Unverified(
            f"no publisher's sample was within {allowance:g}s of its last successful catalog "
            f"read (ages {age}), so the depth is a frozen gauge, not a reading"
        )
    if min(fresh.values()) <= 0.0:
        raise Unverified(
            f"a fresh publisher reported a queue depth of {min(fresh.values()):g}: an empty "
            "queue is not a reason to start a worker"
        )

    operator_value = scalar(queries, "operator_expression", "operator activation expression")
    expected = sum(fresh.values()) / len(fresh)
    if not math.isclose(operator_value, expected, rel_tol=1e-9):
        raise Unverified(
            f"the operator's expression read {operator_value:g} where the fresh publishers "
            f"average {expected:g}: the reconciler is not reading the depth this capture saw"
        )
    if operator_value <= 0.0:
        raise Unverified(
            f"the operator's expression read {operator_value:g}, which asks for no worker"
        )

    return {
        "grade": "verified",
        "summary": {
            "parked_replicas": 0,
            "parked_operator_value": parked_operator_value,
            "woken_replicas": woken["replicas"],
            "publishers": len(depth),
            "fresh_publishers": len(fresh),
            "published_depth": expected,
            "operator_value": operator_value,
            "sample_max_age_seconds": allowance,
        },
        "revisions": revisions,
        "settings": settings,
    }


def grade_acceptance(document: dict) -> dict:
    """Full #6012 proof; schema-2 captures remain historical activation-only evidence."""
    if document.get("schema_version") != 4:
        raise Unverified("full acceptance requires schema_version 4")
    activation = dict(document, schema_version=2)
    evidence = grade(activation)
    revisions = document.get("revisions") or {}
    if not re.fullmatch(r"[0-9a-f]{40}", revisions.get("repository_commit", "")):
        raise Unverified("acceptance requires a full source commit")
    settings = document["settings"]
    batch = settings["ingest_batch"]
    acceptance = document.get("acceptance") or {}

    def require(condition, why):
        if not condition:
            raise Unverified(why)

    observer = acceptance.get("positive_observer") or {}
    pre_trigger = observer.get("pre_trigger") or {}
    positive = observer.get("positive") or {}

    def direct_values(observation, name):
        direct = observation.get("direct") or observation
        rows = direct.get(name)
        require(isinstance(rows, list) and bool(rows), f"direct observer is missing {name}")
        require(all(isinstance(row, dict) and isinstance(row.get("labels"), str) and
                    isinstance(row.get("value"), (int, float)) and not isinstance(row.get("value"), bool) and
                    math.isfinite(row["value"]) and row["value"] >= 0 for row in rows),
                f"direct observer has malformed {name}")
        require(len({row["labels"] for row in rows}) == len(rows),
                f"direct observer has duplicate {name} label sets")
        return {row["labels"]: row["value"] for row in rows}

    def direct_reading(observation):
        depth = direct_values(observation, "published_depth")
        age = direct_values(observation, "sample_age")
        require(set(depth) == set(age), "direct observer depth/age label sets differ")
        require(max(age.values()) <= settings["sample_max_age_seconds"],
                "direct observer sample is stale")
        return sum(depth.values())

    pre_state = pre_trigger.get("state") or {}
    pre_direct = pre_trigger.get("direct") or {}
    require(pre_state.get("replicas") == 0 and pre_state.get("pods") == [],
            "direct observer did not start with the compactor parked")
    require(direct_reading(pre_trigger) == 0, "direct observer baseline was already positive")
    positive_before = positive.get("before") or {}
    positive_after = positive.get("after") or {}
    positive_direct = positive.get("direct") or {}
    require(positive_before.get("replicas") == 0 and positive_before.get("pods") == [],
            "direct positive signal has no preceding parked replica observation")
    require(isinstance(positive_after.get("replicas"), int) and
            isinstance(positive_after.get("pods"), list),
            "direct positive signal has no following replica observation")
    try:
        bracket = positive_after["at"] - positive_before["at"]
        direct_at = positive_direct["time"]
        pre_at = pre_direct["time"]
    except (KeyError, TypeError) as exc:
        raise Unverified(f"direct observer timestamps are missing: {exc}") from exc
    require(0 <= bracket <= settings.get("observer_max_bracket_seconds", 0),
            "direct positive signal has no bounded replica bracket")
    require(positive_before["at"] <= direct_at <= positive_after["at"],
            "direct positive signal falls outside its replica bracket")
    require(pre_at <= pre_state.get("at", 0) <= acceptance.get("positive_started_at", 0) <= direct_at,
            "direct observer did not start before the ingest trigger")
    direct_depth = direct_reading(positive_direct)
    require(direct_depth > 0, "direct observer retained no positive queue depth")

    def ledger(name):
        rows = acceptance.get(name)
        require(isinstance(rows, list) and bool(rows), f"missing {name}")
        require(all(isinstance(r, dict) and isinstance(r.get("id"), str) and r["id"] and
                    isinstance(r.get("rows"), int) and r["rows"] > 0 for r in rows),
                f"malformed {name}")
        require(len({r["id"] for r in rows}) == len(rows), f"duplicate IDs in {name}")
        return {r["id"]: r for r in rows}

    sha = revisions["repository_commit"]
    images = document.get("images") or {}
    versions = document.get("build_versions") or {}
    expected_images = {f"{name}:{sha[:12]}" for name in ("siglake-wakeup", "siglake-wakeup-operator")}
    require(set(images) == expected_images and set(versions) == expected_images and
            all(re.fullmatch(r"sha256:[0-9a-f]{64}", digest) for digest in images.values()) and
            all(sha in version for version in versions.values()), "image build provenance does not match pinned source")
    initial = ledger("initial_ledger")
    require(sum(r["rows"] for r in initial.values()) == batch and
            all(r.get("status") == "committed" for r in initial.values()),
            "initial load was not completely committed before parking")
    sealed = {key: row for key, row in ledger("sealed_ledger").items() if key not in initial}
    committed = ledger("committed_ledger")
    require(bool(sealed) and all(r.get("status") == "sealed" for r in sealed.values()),
            "new segments were not observed sealed while parked")
    require(set(sealed) == set(committed), "committed segment IDs differ from the parked ingest")
    require(sum(r["rows"] for r in sealed.values()) == batch, "sealed rows do not equal the sent batch")
    require(all(r.get("status") == "committed" and isinstance(r.get("committed_at_ms"), int)
                and r["committed_at_ms"] > 0 and r["rows"] == sealed[key]["rows"]
                for key, r in committed.items()), "new segments lack durable committed certificates")
    history = document["replica_history"]
    woken_at = next(r["at"] for r in history if r["phase"] == "woken")
    ingested_row = next(r for r in history if r["phase"] == "ingested")
    ingested_at = ingested_row["at"]
    require(ingested_at == positive_before["at"] and ingested_row.get("replicas") == 0 and
            ingested_row.get("pods") == [],
            "replica history does not retain the direct observer's last parked sample")
    positive_start = acceptance.get("positive_started_at", 0)
    require(0 < positive_start <= ingested_at <= direct_at <= woken_at <= acceptance.get("committed_at", 0),
            "committed evidence is outside the positive wake window")
    require(all(direct_at <= (document["queries"].get(name) or {}).get("time", 0) <= woken_at
                for name in ("published_depth", "sample_age", "operator_expression")),
            "Prometheus evidence is outside the direct-signal/wake window")
    require(all(r["committed_at_ms"] / 1000 >= positive_start for r in committed.values()),
            "committed certificates predate this ingest")
    response = acceptance.get("query_response") or {}
    require(not response.get("truncated") and
            [r.get("host") for r in response.get("rows", [])] == [f"wake-{i:04d}" for i in range(batch)],
            "query does not return exactly the new row IDs")

    absent = acceptance.get("absent_compactor_series") or {}
    negative = acceptance.get("missing_activation") or {}
    for name, query in (("absent compactor series", absent), ("missing activation", negative)):
        response = query.get("response") or {}
        data = response.get("data") or {}
        require(response.get("status") == "success" and data.get("resultType") == "vector" and
                data.get("result") == [], f"{name} must be a successful empty vector")
    parked = acceptance.get("negative_parked") or {}
    woken = acceptance.get("negative_woken") or {}
    require(parked.get("replicas") == 0 and parked.get("pods") == [], "negative control did not begin parked")
    require(woken.get("replicas") == 1, "missing publisher did not restore exactly one compactor")
    disabled = acceptance.get("publisher_disabled") or {}
    env = (disabled.get("spec") or {}).get("extraEnv") or []
    require([r.get("value") for r in env if r.get("name") == "SIGLAKE_WAL_SEALED_PUBLISH_SECS"] == ["0"],
            "negative control has no explicit disabled publisher")
    started = acceptance.get("negative_started_at", 0)
    require(0 < parked.get("at", 0) <= started <= negative.get("time", 0) <= woken.get("at", 0),
            "negative control readings are out of order")
    require(woken["at"] - started < 600, "fallback observation overlaps the scheduled maintenance wake")

    # Parse the real operator's timestamped decision, not an agent-authored boolean.
    # Both other tiers must have usable samples during the no-compactor-series window.
    logs = re.sub(r"\x1b\[[0-9;]*m", "", acceptance.get("operator_log", ""))
    decisions = []
    timestamped = []
    for line in logs.splitlines():
        try:
            at = datetime.datetime.fromisoformat(line.split()[0].replace("Z", "+00:00")).timestamp()
        except (ValueError, IndexError):
            continue
        if f"name={settings['cluster']}" not in line:
            continue
        timestamped.append((at, line))
        if "reconcile decision" in line:
            decisions.append((at, line))
    require(any(absent.get("time", 0) <= at <= acceptance.get("unaffected_until", 0) and
                "ingester_rps_per_pod: Some(" in line and "query_in_flight_per_pod: Some(" in line
                for at, line in decisions), "no ordinary ingester/query decisions while compactor series absent")
    require(any(started <= at <= woken["at"] + 5 and "compactor_backlog: None" in line and
                "comp 0→1" in line for at, line in decisions),
            "no operator missing-signal decision restoring compactor 0 to 1")
    require(any(direct_at <= at <= woken_at + 5 and "compactor_backlog: Some(" in line and
                "comp 0→1" in line for at, line in decisions),
            "no operator positive-signal decision restoring compactor 0 to 1")
    require(not any(started <= at <= woken["at"] and "waking the parked compactor for maintenance" in line for at, line in timestamped),
            "negative control was a maintenance wake")
    workloads = (acceptance.get("workloads") or {}).get("items") or []
    for component, kind in (("ingester", "Deployment"), ("query", "StatefulSet")):
        matches = [w for w in workloads if w.get("kind") == kind and
                   (w.get("metadata") or {}).get("name") == settings["cluster"] + "-" + component]
        require(len(matches) == 1 and matches[0]["spec"]["replicas"] == 1 and
                (matches[0].get("status") or {}).get("readyReplicas", 0) == 1,
                f"{component} did not retain its ready floor")
    evidence["summary"].update({"committed_rows": batch, "exact_row_ids": batch,
                                "unaffected_tiers": ["ingester", "query"], "missing_publisher_woken_replicas": 1})
    evidence["scope"] = "full wake-up acceptance; cleanup is recorded separately"
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=pathlib.Path, help="the capture document to grade")
    parser.add_argument("--require-acceptance", action="store_true", help="require all #6012 controls and committed-row proof")
    parser.add_argument("--output", type=pathlib.Path, help="where to write the graded evidence")
    args = parser.parse_args()

    try:
        document = json.loads(args.capture.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"unverified: the capture is unreadable: {exc}", file=sys.stderr)
        return 1

    try:
        evidence = grade_acceptance(document) if args.require_acceptance else grade(document)
    except (Unverified, TypeError, KeyError, ValueError, StopIteration) as exc:
        print(f"unverified: {exc}", file=sys.stderr)
        if args.output:
            args.output.write_text(
                json.dumps(
                    {"capture": document, "evidence": {"grade": "unverified", "reason": str(exc)}},
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
        return 1

    print(
        "grade=verified "
        f"depth={evidence['summary']['published_depth']:g} "
        f"operator={evidence['summary']['operator_value']:g} "
        f"woken_replicas={evidence['summary']['woken_replicas']}"
    )
    if args.output:
        args.output.write_text(
            json.dumps({"capture": document, "evidence": evidence}, indent=2) + "\n",
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
