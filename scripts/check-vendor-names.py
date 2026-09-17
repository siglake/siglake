#!/usr/bin/env python3
"""Refuse proprietary observability vendors' names in the tree that ships.

The sweep rule, settled 2026-09-16: the public tree does not discuss non-OSS
competitors. Task #4569 applied it by hand across sixteen files -- a
`BLOOM_FILTER_COLUMNS` doc comment that described its columns as "equivalent to
<vendor>'s primary indexed fields", an ingest auth scheme named after one
vendor, eleven comments named after that vendor's event-collector protocol, and
a private middleware whose name from the same protocol reached three published
OpenAPI 429 descriptions. The sweep that found them was a one-off grep, so the
next comment written with a competitor's product page open has nothing to stop
it.

scripts/check-public-tree.py, the other guard over the published tree,
deliberately does not check names: this project publishes head-to-head
benchmarks on purpose, so a named ENGINE is normal there. This gate draws the
line the sweep rule draws. An open-source engine anyone can install and
reproduce a number against -- Quickwit, Elasticsearch/OpenSearch, ClickHouse,
DuckDB, Loki, VictoriaLogs, Grafana, Jaeger, Prometheus, Trino, Spark, Iceberg,
DataFusion -- is fair game, and none of them are below. Naming a proprietary
vendor is either a comparison no reader can run or a compatibility promise the
code does not keep, and #4569 found one of each.

Scope is the files that ship, taken from check-public-tree.py so there is one
definition of that and not two: the competitive corpus and the notes that reason
about it live in paths the launch squash removes, and the rule is about the
published tree. `third_party/` is upstream code we do not rewrite, and
`Cargo.lock` is generated.

Matching is case-insensitive, after a non-alphanumeric boundary. Most of these
names are distinctive enough to match as a prefix, which is the point: the hits
worth catching are `splunkd`, `hec_token`, `api.datadoghq.com` and
`newrelic_exporter`, not tidy prose. The names whose tail is an ordinary English
word need a closing boundary too -- `devops` is not Devo and "we observe
increased latency" is not Observe Inc. -- and are listed separately below. A
multi-word name matches with or without its separator, since `sumologic` and
`observeinc` are how the same vendors write themselves in a URL.

A legitimate mention is allow-listed with a marker AND a reason on the same
line:

    // ... as <vendor> does  (vendor-name-ok: named in the removal record)

which puts the exception in the diff, where review can see it. A marker without
a reason is itself a failure: the whole value of the exception is the sentence
that justifies it.

Runs in the `shell` CI job (it compiles nothing) and inside the `shell` line of
scripts/ci-local.sh. A fixture suite runs first on every invocation, because a
denylist whose regexes match nothing is a gate that passes silently forever.
"""

from __future__ import annotations

import importlib.util
import os
import pathlib
import re
import sys

# Importing the checker writes scripts/__pycache__/ into the checkout
# otherwise. scripts/make-public-tree.sh sets PYTHONDONTWRITEBYTECODE for the
# same import; here the flag belongs in the file that does the importing.
sys.dont_write_bytecode = True

PUBLIC_TREE_CHECKER = "check-public-tree.py"

# Proprietary vendors, matched as a prefix after a non-alphanumeric boundary.
# Keep this list in one place and keep it alphabetical. Adding a name is cheap;
# removing one is a decision about the sweep rule, not about this file.
VENDORS = (
    "appdynamics",
    "axiom.co",
    "chronosphere",
    "coralogix",
    "datadog",
    "dynatrace",
    "elastic cloud",
    "exabeam",
    "graylog",
    "honeycomb",
    "humio",
    "hyperdx",
    "instana",
    "lightstep",
    "logdna",
    "loggly",
    "logscale",
    "logz.io",
    "mezmo",
    "new relic",
    "panther labs",
    "papertrail",
    "scalyr",
    "sematext",
    "sentinelone",
    "signalfx",
    "solarwinds",
    "splunk",
    "sumo logic",
    "wavefront",
)

# Names that need a closing boundary as well, because a longer ordinary word
# starts with them. Without it this gate reports every `devops` in the tree and
# every sentence that observes an increase, which is how a guard gets deleted.
WORD_BOUNDED_VENDORS = (
    "better stack",
    "devo",
    "observe inc",
)

# `datadog` must match `api.datadoghq.com`; `devo` must not match `devops`. The
# leading boundary is alphanumeric-only on purpose, so `_splunk` and `-splunk`
# are still hits. A multi-word name's separator is optional: `sumologic`.
#
# One alternation behind one lookbehind, not thirty-three patterns searched in
# turn: the whole shipping tree is 11 MB and this gate runs in the `shell` job
# on every push.
TRAILING = r"(?![A-Za-z0-9])"


def vendor_pattern(name: str, word_bounded: bool) -> str:
    body = r"[\s_.-]*".join(re.escape(word) for word in name.split())
    return body + (TRAILING if word_bounded else "")


VENDOR_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    + "|".join(
        [vendor_pattern(name, False) for name in VENDORS]
        + [vendor_pattern(name, True) for name in WORD_BOUNDED_VENDORS]
    )
    + ")",
    re.IGNORECASE,
)

# The exception, and the reason it exists. Both halves are required.
MARKER = re.compile(r"vendor-name-ok(?::[ \t]*(?P<reason>\S[^\n]*))?", re.IGNORECASE)

# This file necessarily contains every name it searches for -- the denylist IS
# the pattern. Exempted by exact path, deliberately not by directory or glob:
# `scripts/` ships, and a hole shaped like "anything under scripts/" is how a
# real mention would get through. ci-local.sh and ci.yml name no vendor either;
# they run this script, which is why neither needs an exemption.
SELF = "scripts/check-vendor-names.py"

# Generated, and not prose anyone writes. `third_party/` is already dropped by
# check-public-tree.py's own skip list.
SKIP_FILES = {"Cargo.lock"}


def mentions(line: str) -> list[str]:
    """Vendor names on a line, as matched."""
    return [m.group(0) for m in VENDOR_RE.finditer(line)]


def allow_reason(line: str) -> str | None:
    """The reason on an allow-list marker; `""` when the marker carries none."""
    m = MARKER.search(line)
    if not m:
        return None
    return m.group("reason") or ""


def file_problems(rel: str, text: str) -> tuple[list[tuple[int, str]], int]:
    """One file's reportable lines, and how many mentions its markers keep.

    A marker is only read on a line that mentions a vendor, so prose about the
    marker itself -- the failure message below, the step that runs this script --
    is not a reasonless exception.

    The line walk runs only for a file the whole-text scan already hit, which is
    almost none of them.
    """
    problems: list[tuple[int, str]] = []
    allowed = 0
    if not VENDOR_RE.search(text) and not VENDOR_RE.search(rel):
        return problems, allowed
    if names := mentions(rel):
        problems.append(
            (
                0,
                f"the path names `{names[0]}`, a proprietary vendor -- the "
                "published tree does not discuss non-OSS competitors",
            )
        )
    for lineno, line in enumerate(text.splitlines(), 1):
        names = mentions(line)
        if not names:
            continue
        reason = allow_reason(line)
        if reason:
            allowed += 1
            continue
        if reason == "":
            problems.append(
                (
                    lineno,
                    "carries `vendor-name-ok` with no reason after it -- write "
                    "`vendor-name-ok: <why this mention stays>`: "
                    f"{line.strip()[:110]}",
                )
            )
            continue
        for name in names:
            problems.append(
                (
                    lineno,
                    f"names `{name}`, a proprietary vendor -- the published tree "
                    "does not discuss non-OSS competitors: "
                    f"{line.strip()[:110]}",
                )
            )
    return problems, allowed


def shipping_files(root: pathlib.Path) -> list[pathlib.PurePath]:
    """The published tree's files, from the checker that defines that set.

    Not a second copy of the exclusion list: scripts/make-public-tree.sh reads
    the same module to decide what the squash removes, so all three agree about
    what ships by construction.
    """
    path = root / "scripts" / PUBLIC_TREE_CHECKER
    spec = importlib.util.spec_from_file_location("check_public_tree", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return [
        rel for rel in module.shipping_files(root) if rel.as_posix() not in SKIP_FILES
    ]


# Negative fixtures. The red ones are the shapes #4569 removed plus the forms a
# name takes in a URL or an identifier; the green ones are the words this gate
# must not touch, several of which are the reason a name is word-bounded above.
RED = [
    "the columns equivalent to Splunk's primary indexed fields",
    "# a leftover from the HEC ingest surface -- Splunk-compatible",
    'let endpoint = "https://http-intake.logs.datadoghq.com/api/v2/logs";',
    'newrelic_exporter = { version = "0.1" }',
    "shipped to sumologic collectors",
    "compared against Dynatrace",
    "the logz.io shipper",
    "Humio, now LogScale",
    "https://observeinc.com/pricing",
    "Better Stack's included volume",
    "Elastic Cloud's hot tier",
    "a SentinelOne deployment",
    "signalfx_dimensions",
    "the wavefront proxy",
    "Devo ingests it as",
    "hyperdx-otel-collector",
    "New Relic and AppDynamics both",
    "coralogix, chronosphere, mezmo, logdna",
    "papertrail, loggly, solarwinds, sematext, scalyr",
    "graylog, exabeam, panther labs, lightstep, instana",
    "honeycomb's trace sampling",
    "axiom.co/docs",
    "Observe Inc. charges per",
]
GREEN = [
    "the devops runbook lives in deploy/",
    "a devoted reader of the changelog",
    "we observe increased latency at the tail",
    "a better stacked bar than the last one",
    "Quickwit, ClickHouse and Elasticsearch on the same corpus",
    "Elasticsearch-compatible NDJSON bulk ingest",
    "OpenSearch's bulk API",
    "elastic scaling of the query tier",
    "VictoriaLogs, Loki and Grafana render it",
    "Jaeger, Prometheus, Trino, Spark, Iceberg, DataFusion",
    "the instance count per shard",
    "instantaneous rate over the window",
    "gray logging of the audit trail",
    "an axiom. Consistency follows from it",
]

# A whole file, so the line walk, the marker and the reason requirement are
# exercised together and not only the line matcher.
FIXTURE_FILE = """\
// The ingest handlers accept exactly one authorization scheme.
let scheme = "Splunk";
// as Datadog does (vendor-name-ok: named once in the removal record)
let other = "Datadog";  // vendor-name-ok
let fine = "Bearer";
"""


def run_fixtures() -> int:
    """Prove the denylist goes red on real shapes, and stays quiet on ours."""
    for line in RED:
        if not mentions(line):
            raise AssertionError(f"fixture: a vendor name passed: {line!r}")
    for line in GREEN:
        if found := mentions(line):
            raise AssertionError(f"fixture: reported {found} in: {line!r}")

    # Every name on the list is exercised by a red fixture of its own. Without
    # this, a typo in an entry -- or a name the separator rule cannot spell --
    # is an entry that never matches anything, and the count in the ok line
    # says 33 either way.
    for name, word_bounded in [(n, False) for n in VENDORS] + [
        (n, True) for n in WORD_BOUNDED_VENDORS
    ]:
        alone = re.compile(
            r"(?<![A-Za-z0-9])(?:" + vendor_pattern(name, word_bounded) + ")",
            re.IGNORECASE,
        )
        if not any(alone.search(line) for line in RED):
            raise AssertionError(f"fixture: no red fixture exercises `{name}`")

    problems, allowed = file_problems("crates/siglake-ingest/src/lib.rs", FIXTURE_FILE)
    if sorted(lineno for lineno, _ in problems) != [2, 4]:
        raise AssertionError(
            f"fixture: expected the unmarked mention and the reasonless marker "
            f"(lines 2 and 4), reported {sorted(n for n, _ in problems)}"
        )
    if "vendor-name-ok" not in problems[-1][1]:
        raise AssertionError("fixture: the reasonless marker was not named as such")
    if allowed != 1:
        raise AssertionError("fixture: the allow-listed mention was not counted")

    # A path that names a vendor, with no such line inside the file.
    named_path, _ = file_problems("docs/datadog-comparison.md", "nothing in here\n")
    if not named_path:
        raise AssertionError("fixture: a vendor-named path was not reported")

    # The self-exemption is one path, not a directory.
    if not SELF.startswith("scripts/") or SELF.count("/") != 1:
        raise AssertionError("fixture: SELF is not a single path under scripts/")

    return len(RED) + len(GREEN) + len(VENDORS) + len(WORD_BOUNDED_VENDORS) + 4


def annotate(rel: str, lineno: int, message: str) -> None:
    """A GitHub annotation on the offending line, when running under Actions."""
    if not os.environ.get("GITHUB_ACTIONS"):
        return
    where = f"file={rel}" + (f",line={lineno}" if lineno else "")
    print(f"::error {where}::{message}")


def main() -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    try:
        fixtures = run_fixtures()
    except AssertionError as e:
        print(f"FAIL {e}", file=sys.stderr)
        return 1

    files = shipping_files(root)
    # ABSENCE IS NEVER SILENCE: an empty list means the checkout or the import
    # broke, and a gate that checked nothing has not passed.
    if not files:
        print(
            "FAIL no shipping files found -- the gate checked nothing, which is "
            "not the same as passing",
            file=sys.stderr,
        )
        return 1

    problems: list[str] = []
    allowed = 0
    checked = 0
    for rel in files:
        path = root / rel
        name = rel.as_posix()
        if name == SELF or not path.is_file():
            continue
        try:
            text = path.read_text()
        except (OSError, UnicodeDecodeError):
            continue
        checked += 1
        found, kept = file_problems(name, text)
        allowed += kept
        for lineno, message in found:
            problems.append(f"{name}:{lineno} {message}")
            annotate(name, lineno, message)

    if problems:
        for p in problems:
            print(f"FAIL {p}", file=sys.stderr)
        print(
            "FAIL the public tree does not discuss non-OSS competitors (the "
            "2026-09-16 sweep rule). Describe the thing by its own property, as "
            "task #4569 did, or keep the mention with "
            "`vendor-name-ok: <why>` on the line.",
            file=sys.stderr,
        )
        print(
            f"\n{len(problems)} vendor mention(s) in the tree that ships "
            f"({checked} files checked).",
            file=sys.stderr,
        )
        return 1

    # `ok   <N> ...` -- the shape check-set-var.py and check-public-tree.py print.
    print(
        f"ok   {checked} shipping files name none of "
        f"{len(VENDORS) + len(WORD_BOUNDED_VENDORS)} proprietary vendors "
        f"({allowed} allow-listed mention(s)); {fixtures} fixtures"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
