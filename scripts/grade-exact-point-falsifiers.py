#!/usr/bin/env python3
"""Grade retained exact-point falsifier artifacts without touching a cluster."""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import re
import sys
from typing import Any


EXPECTED_ROWS = 98_466_115
POINT_LIMIT = 27_300
BYTE_LIMIT = 757_764
CPU_LIMIT_SECONDS = 2.2
JSON_ARTIFACTS = (
    "cardinality.json",
    "export.json",
    "encoding.json",
    "build-cpu.json",
    "boundary-arms.json",
)


def is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def nonnegative_int(value: Any) -> bool:
    return is_int(value) and value >= 0


def positive_int(value: Any) -> bool:
    return is_int(value) and value > 0


def load_artifacts(directory: pathlib.Path, problems: list[str]) -> dict[str, dict[str, Any]]:
    artifacts: dict[str, dict[str, Any]] = {}
    for name in JSON_ARTIFACTS:
        path = directory / name
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            problems.append(f"missing artifact {name}")
            continue
        except (OSError, json.JSONDecodeError) as error:
            problems.append(f"cannot read {name}: {error}")
            continue
        if not isinstance(document, dict):
            problems.append(f"{name} root is not an object")
            continue
        artifacts[name] = document
    return artifacts


def pinned_scalar(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return bool(value) and all(pinned_scalar(item) for item in value)
    if isinstance(value, dict):
        return bool(value) and all(
            isinstance(key, str) and key and pinned_scalar(item) for key, item in value.items()
        )
    return False


def check_envelopes(
    artifacts: dict[str, dict[str, Any]], problems: list[str]
) -> tuple[str | None, str | None]:
    evidence_kinds: set[str] = set()
    revision_values: dict[str, set[str]] = {}
    repository_commit: str | None = None
    for name, document in artifacts.items():
        if document.get("schema_version") != 1:
            problems.append(f"{name} has unsupported or missing schema_version")
        kind = document.get("evidence_kind")
        if kind not in {"live", "synthetic"}:
            problems.append(f"{name} has invalid or missing evidence_kind")
        else:
            evidence_kinds.add(kind)
        revisions = document.get("revisions")
        if not isinstance(revisions, dict) or not pinned_scalar(revisions):
            problems.append(f"{name} has missing or empty pinned revisions")
            continue
        commit = revisions.get("repository_commit")
        if not isinstance(commit, str) or not commit.strip():
            problems.append(f"{name} has no pinned repository_commit")
        elif repository_commit is None:
            repository_commit = commit
        for key, value in revisions.items():
            rendered = json.dumps(value, sort_keys=True)
            revision_values.setdefault(key, set()).add(rendered)
    if len(evidence_kinds) > 1:
        problems.append("artifacts mix live and synthetic evidence")
    for key, values in sorted(revision_values.items()):
        if len(values) > 1:
            problems.append(f"pinned revision {key} is inconsistent across artifacts")
    return next(iter(evidence_kinds), None), repository_commit


def require_verdict(
    name: str, document: dict[str, Any], expected: str, problems: list[str]
) -> None:
    actual = document.get("verdict")
    if actual != expected:
        problems.append(f"{name} verdict {actual!r} does not match measured {expected!r}")


def grade_cardinality(document: dict[str, Any], problems: list[str]) -> str | None:
    fields = ("row_count", "distinct_ts_ns", "distinct_ts_us", "distinct_ts_ns_level")
    for field in fields:
        if not nonnegative_int(document.get(field)):
            problems.append(f"cardinality.json has invalid {field}")
    if any(not nonnegative_int(document.get(field)) for field in fields):
        return None
    if document["row_count"] != EXPECTED_ROWS:
        problems.append(f"cardinality.json row_count is not frozen-corpus {EXPECTED_ROWS}")
    queries = document.get("queries")
    if not isinstance(queries, dict):
        problems.append("cardinality.json has no query-path observations")
        return None
    for path in ("transparent", "local"):
        observation = queries.get(path)
        if not isinstance(observation, dict):
            problems.append(f"cardinality.json is missing the {path} query path")
            continue
        if observation.get("status") != "complete" or observation.get("eligible") is not True:
            problems.append(f"cardinality.json {path} query was incomplete or ineligible")
        if observation.get("counter_reset") is not False:
            problems.append(f"cardinality.json {path} counters reset or were not checked")
        for cache_field in ("cache_hits_delta", "cache_misses_delta"):
            if observation.get(cache_field) != 0:
                problems.append(f"cardinality.json {path} {cache_field} is not zero")
        for field in fields:
            if observation.get(field) != document[field]:
                problems.append(f"cardinality.json {path} {field} disagrees with the retained value")
    expected = "passed" if document["distinct_ts_ns"] <= POINT_LIMIT else "falsified"
    require_verdict("cardinality.json", document, expected, problems)
    return expected


def grade_export(
    document: dict[str, Any], cardinality: dict[str, Any], problems: list[str]
) -> None:
    if document.get("status") != "complete":
        problems.append("export.json does not record a complete export")
    if document.get("truncated") is not False:
        problems.append("export.json records a truncated or unchecked export")
    if document.get("error") is not None:
        problems.append("export.json records an export error")
    if document.get("resource_limited") is not False:
        problems.append("export.json records a resource-limited or unchecked export")
    sha256 = document.get("sha256")
    if not isinstance(sha256, str) or re.fullmatch(r"[0-9a-f]{64}", sha256) is None:
        problems.append("export.json has no valid SHA-256")
    if document.get("exported_pair_rows") != cardinality.get("distinct_ts_ns_level"):
        problems.append("export.json pair-row count disagrees with cardinality.json")
    if document.get("row_count") != cardinality.get("row_count"):
        problems.append("export.json sum(n) disagrees with cardinality.json")
    require_verdict("export.json", document, "complete", problems)


def grade_encoding(
    document: dict[str, Any], cardinality: dict[str, Any], export: dict[str, Any], problems: list[str]
) -> str | None:
    numeric_fields = (
        "points",
        "exported_pair_rows",
        "row_count",
        "dictionary_values",
        "encoded_bytes",
        "serde_json_bytes",
    )
    for field in numeric_fields:
        if not positive_int(document.get(field)):
            problems.append(f"encoding.json has invalid {field}")
    bytes_per_point = document.get("bytes_per_point")
    if not isinstance(bytes_per_point, (int, float)) or isinstance(bytes_per_point, bool) or bytes_per_point <= 0:
        problems.append("encoding.json has invalid bytes_per_point")
    if document.get("decode_round_trip") is not True:
        problems.append("encoding.json does not record a successful decoding round trip")
    if document.get("points") != cardinality.get("distinct_ts_ns"):
        problems.append("encoding.json timestamp points disagree with cardinality.json")
    if document.get("exported_pair_rows") != cardinality.get("distinct_ts_ns_level"):
        problems.append("encoding.json pair rows disagree with cardinality.json")
    if document.get("exported_pair_rows") != export.get("exported_pair_rows"):
        problems.append("encoding.json pair rows disagree with export.json")
    if document.get("row_count") != cardinality.get("row_count"):
        problems.append("encoding.json sum(n) disagrees with cardinality.json")
    if positive_int(document.get("points")) and positive_int(document.get("encoded_bytes")) and isinstance(bytes_per_point, (int, float)):
        expected_bpp = document["encoded_bytes"] / document["points"]
        if not math.isclose(float(bytes_per_point), expected_bpp, rel_tol=1e-12, abs_tol=1e-12):
            problems.append("encoding.json bytes_per_point is arithmetically inconsistent")
    if not positive_int(document.get("encoded_bytes")):
        return None
    expected = "passed" if document["encoded_bytes"] <= BYTE_LIMIT else "falsified"
    require_verdict("encoding.json", document, expected, problems)
    return expected


def grade_build_cpu(document: dict[str, Any], problems: list[str]) -> str | None:
    if document.get("status") != "complete":
        problems.append("build-cpu.json is not complete")
    if document.get("source") != "candidate_arm_counter":
        problems.append("build-cpu.json is not from the candidate-arm counter")
    if document.get("clock") != "monotonic_elapsed" or document.get("unit") != "nanoseconds":
        problems.append("build-cpu.json has the wrong clock or unit")
    if document.get("counter_reset") is not False:
        problems.append("build-cpu.json counters reset or were not checked")
    roles = document.get("role_nanoseconds")
    pods = document.get("per_pod")
    if not isinstance(roles, dict) or any(not positive_int(roles.get(role)) for role in ("drain_append", "compaction_rewrite")):
        problems.append("build-cpu.json lacks positive writer-role subtotals")
        return None
    if not isinstance(pods, list) or not pods:
        problems.append("build-cpu.json has no per-pod observations")
        return None
    sums = {"drain_append": 0, "compaction_rewrite": 0}
    seen: set[str] = set()
    for index, row in enumerate(pods):
        if not isinstance(row, dict) or not isinstance(row.get("pod"), str) or not row["pod"]:
            problems.append(f"build-cpu.json per_pod[{index}] has no pod")
            continue
        if row["pod"] in seen:
            problems.append(f"build-cpu.json repeats pod {row['pod']}")
        seen.add(row["pod"])
        for role in sums:
            value = row.get(role)
            if not nonnegative_int(value):
                problems.append(f"build-cpu.json per_pod[{index}] has invalid {role}")
            else:
                sums[role] += value
    for role, value in sums.items():
        if roles.get(role) != value:
            problems.append(f"build-cpu.json {role} subtotal does not equal per-pod sum")
    aggregate = document.get("aggregate_seconds")
    total_ns = sum(roles[role] for role in sums)
    if not isinstance(aggregate, (int, float)) or isinstance(aggregate, bool) or aggregate < 0:
        problems.append("build-cpu.json has invalid aggregate_seconds")
        return None
    if not math.isclose(float(aggregate), total_ns / 1e9, rel_tol=1e-12, abs_tol=1e-12):
        problems.append("build-cpu.json aggregate_seconds does not equal role subtotals")
    expected = "passed" if aggregate <= CPU_LIMIT_SECONDS else "falsified"
    require_verdict("build-cpu.json", document, expected, problems)
    return expected


def count_map(value: Any) -> bool:
    return isinstance(value, dict) and bool(value) and all(
        isinstance(key, str) and key and nonnegative_int(count) for key, count in value.items()
    )


def grade_boundary(document: dict[str, Any], problems: list[str]) -> str | None:
    control = document.get("control")
    candidate = document.get("candidate")
    distributed = document.get("distributed_correctness")
    if not isinstance(control, dict):
        problems.append("boundary-arms.json is missing the control arm")
        return None
    if not isinstance(candidate, dict):
        problems.append("boundary-arms.json is missing the candidate arm")
        return None
    eligible = True
    for name, arm in (("control", control), ("candidate", candidate)):
        if arm.get("status") != "complete" or arm.get("query_path") != "local_full_table":
            problems.append(f"boundary-arms.json {name} arm is incomplete or ineligible")
            eligible = False
        if arm.get("counter_reset") is not False:
            problems.append(f"boundary-arms.json {name} counters reset or were not checked")
            eligible = False
        if arm.get("execution_count") != 2 or arm.get("max_rows_returned") != [10, 9]:
            problems.append(f"boundary-arms.json {name} did not execute the intended two calls")
            eligible = False
        if arm.get("cache_hits_delta") != 0 or arm.get("cache_misses_delta") != 0:
            problems.append(f"boundary-arms.json {name} used or populated a result cache")
            eligible = False
        if not count_map(arm.get("counts")):
            problems.append(f"boundary-arms.json {name} has no exact per-level counts")
            eligible = False
        for field in ("boundary_scan_delta", "physical_read_delta"):
            if not nonnegative_int(arm.get(field)):
                problems.append(f"boundary-arms.json {name} has invalid {field}")
                eligible = False
    if control.get("files_per_call") != [4, 4]:
        problems.append("boundary-arms.json control does not establish exactly four files per call")
        eligible = False
    if not positive_int(control.get("boundary_scan_delta")) or not positive_int(control.get("physical_read_delta")):
        problems.append("boundary-arms.json control did not read boundary files")
        eligible = False
    if not positive_int(candidate.get("candidate_served_delta")):
        problems.append("boundary-arms.json has no positive candidate execution")
        eligible = False
    if not isinstance(distributed, dict) or distributed.get("label") != "correctness_only" or distributed.get("query_path") != "transparent":
        problems.append("boundary-arms.json lacks the labelled transparent correctness check")
        eligible = False
    elif not count_map(distributed.get("control_counts")) or not count_map(distributed.get("candidate_counts")):
        problems.append("boundary-arms.json transparent correctness counts are missing")
        eligible = False
    sql_sha256 = document.get("sql_sha256")
    if not isinstance(sql_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", sql_sha256) is None:
        problems.append("boundary-arms.json does not pin the byte-identical SQL")
        eligible = False
    if not eligible:
        return None
    exact = control["counts"] == candidate["counts"]
    distributed_exact = (
        distributed["control_counts"] == control["counts"]
        and distributed["candidate_counts"] == candidate["counts"]
    )
    eliminated = candidate["boundary_scan_delta"] == 0 and candidate["physical_read_delta"] == 0
    expected = "passed" if exact and distributed_exact and eliminated else "falsified"
    require_verdict("boundary-arms.json", document, expected, problems)
    return expected


def check_report(
    directory: pathlib.Path, repository_commit: str | None, problems: list[str]
) -> None:
    reports = sorted(directory.glob("EXACT_POINT_FALSIFIERS_*.md"))
    if len(reports) != 1:
        problems.append("expected exactly one EXACT_POINT_FALSIFIERS_*.md report")
        return
    try:
        text = reports[0].read_text(encoding="utf-8")
    except OSError as error:
        problems.append(f"cannot read {reports[0].name}: {error}")
        return
    if re.search(r"(?im)^run:\s*\S+", text) is None:
        problems.append(f"{reports[0].name} does not name the run")
    commit = re.search(r"(?im)^repository_commit:\s*(\S+)", text)
    if commit is None:
        problems.append(f"{reports[0].name} does not name repository_commit")
    elif repository_commit is not None and commit.group(1) != repository_commit:
        problems.append(f"{reports[0].name} repository_commit disagrees with artifacts")


def grade(directory: pathlib.Path) -> dict[str, Any]:
    problems: list[str] = []
    artifacts = load_artifacts(directory, problems)
    evidence_kind, repository_commit = check_envelopes(artifacts, problems)
    outcomes: dict[str, str | None] = {name: None for name in ("f1", "f2", "f3", "f4")}
    cardinality = artifacts.get("cardinality.json")
    export = artifacts.get("export.json")
    encoding = artifacts.get("encoding.json")
    build_cpu = artifacts.get("build-cpu.json")
    boundary = artifacts.get("boundary-arms.json")
    if cardinality is not None:
        outcomes["f1"] = grade_cardinality(cardinality, problems)
    if cardinality is not None and export is not None:
        grade_export(export, cardinality, problems)
    if cardinality is not None and export is not None and encoding is not None:
        outcomes["f2"] = grade_encoding(encoding, cardinality, export, problems)
    if build_cpu is not None:
        outcomes["f3"] = grade_build_cpu(build_cpu, problems)
    if boundary is not None:
        outcomes["f4"] = grade_boundary(boundary, problems)
    check_report(directory, repository_commit, problems)
    if any(outcome is None for outcome in outcomes.values()):
        problems.append("one or more falsifiers has no explicit eligible outcome")
    grade_value = "verified" if not problems else "unverified"
    hypothesis = None
    if grade_value == "verified":
        hypothesis = "falsified" if "falsified" in outcomes.values() else "passed"
    return {
        "grade": grade_value,
        "evidence_kind": evidence_kind,
        "hypothesis_outcome": hypothesis,
        "falsifiers": outcomes,
        "problems": problems,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    if not args.directory.is_dir():
        print(f"ERROR: not an artifact directory: {args.directory}", file=sys.stderr)
        return 2
    evidence = grade(args.directory)
    rendered = json.dumps({"evidence": evidence}, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    print(
        "EXACT_POINT_FALSIFIERS "
        f"grade={evidence['grade']} outcome={evidence['hypothesis_outcome']}",
        file=sys.stderr,
    )
    for problem in evidence["problems"]:
        print(f"  {problem}", file=sys.stderr)
    return 0 if evidence["grade"] == "verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
