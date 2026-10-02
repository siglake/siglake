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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=pathlib.Path, help="the capture document to grade")
    parser.add_argument("--output", type=pathlib.Path, help="where to write the graded evidence")
    args = parser.parse_args()

    try:
        document = json.loads(args.capture.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"unverified: the capture is unreadable: {exc}", file=sys.stderr)
        return 1

    try:
        evidence = grade(document)
    except Unverified as exc:
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
