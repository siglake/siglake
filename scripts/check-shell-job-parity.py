#!/usr/bin/env python3
"""Keep CI's `shell` guards and the local gate in sync.

`.github/workflows/ci.yml`'s `shell` job and `scripts/ci-local.sh`'s `shell`
block are two hand-maintained copies of the same list, and the whole point of
the local gate is that it answers the same question CI will. Nothing compared
them, so a guard added to one side stayed missing from the other for as long as
nobody looked: the image-size guard (2026-09-07, task #1638) landed in ci-local
and had to be picked up here separately, and task #1718 the same day wired three
more into ci.yml by hand -- three guards that had been local-only, invisibly,
since the day each was written.

Every `scripts/check-*` the local shell block runs must be a `run:` in ci.yml's
`shell` job. In the other direction, every guard in CI's shell job must be
invoked somewhere in ci-local.sh, though it can have its own local job and
status line: `check-claude-md.sh` and `check-set-var.py` do. A guard that can
only run on a CI runner must be named in `RUNNER_ONLY_GUARDS`; otherwise either
direction of drift fails the gate.

Stdlib only, deliberately: this runs in the `shell` job, which installs nothing
(the `helm` job is the one that `pip install`s pyyaml). Both sides are parsed
statically -- ci-local's `# --- shell ---` banner block for local-to-CI parity,
all of ci-local for CI-to-local parity, and ci.yml's `run:` scalars under
`shell:` -- with full-line comments stripped first, so prose that names a
script cannot stand in for running it.

Runs as part of the `shell` line of scripts/ci-local.sh and as a step of
ci.yml's `shell` job, which means this gate is inside the list it checks.

A parser whose failure mode is a green run is worse than no parser, so a fixture
suite runs first, on every invocation: it deletes each real ci.yml step in turn
and requires the mutation to be caught, and it feeds the extractors shapes whose
answer must be an error rather than an empty set. It costs milliseconds and
needs no fixtures on disk.
"""

from __future__ import annotations

import os
import pathlib
import re
import sys

CI_YML = pathlib.PurePath(".github/workflows/ci.yml")
CI_LOCAL = pathlib.PurePath("scripts/ci-local.sh")
CHECKER = pathlib.PurePath("scripts/check-shell-job-parity.py")

# Guards that fundamentally require the CI runner and therefore cannot be
# invoked by ci-local.sh. Keep this explicit even while it is empty: adding a
# CI-only guard requires a deliberate exception here rather than silent drift.
RUNNER_ONLY_GUARDS: frozenset[str] = frozenset()

# ci-local.sh delimits its jobs with `# --- <name> ------...` banners.
BANNER = re.compile(r"^# --- (?P<name>[a-z0-9-]+) -{3,}\s*$")

# `# --- shell ---`'s guard invocations, and ci.yml's `run:` scalars, name their
# scripts by repo-relative path. Both extensions: check-set-var.py shows a
# python guard is a guard.
GUARD = re.compile(r"\bscripts/(check-[A-Za-z0-9._-]+\.(?:sh|py))\b")

# A whole line of comment -- YAML above a step, or bash inside one. Not a
# trailing comment: stripping one would mean deciding whether the `#` is inside
# a quote, and a guard invoked on a line that also carries a comment is still
# invoked.
FULL_LINE_COMMENT = re.compile(r"^\s*#")

# A 2-space-indented mapping key: the indent GitHub Actions job names sit at.
JOB_KEY = re.compile(r"^  (?P<name>[A-Za-z0-9_-]+):\s*(?:#.*)?$")

# `run: ...` as a step key or as the first key of a step.
RUN_KEY = re.compile(r"^(?P<indent>\s*)(?:- )?run:(?P<value>.*)$")

BLOCK_SCALAR = re.compile(r"^[|>][+-]?\d*$")


class ExtractionError(Exception):
    """The file did not have the shape this gate parses.

    Never an empty result: a gate that compared nothing to nothing has not
    passed, and both shapes here are load-bearing enough that losing one is a
    change someone must see.
    """


def local_shell_guards(text: str) -> set[str]:
    """Every `scripts/check-*` invoked in ci-local.sh's `# --- shell ---` block."""
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        m = BANNER.match(line)
        if m and m.group("name") == "shell":
            start = i + 1
            break
    if start is None:
        raise ExtractionError(f"{CI_LOCAL} has no `# --- shell ---` job banner")

    guards: set[str] = set()
    for line in lines[start:]:
        if BANNER.match(line):
            break
        if FULL_LINE_COMMENT.match(line):
            continue
        guards.update(GUARD.findall(line))
    if not guards:
        raise ExtractionError(
            f"{CI_LOCAL}'s shell block invokes no scripts/check-* guard -- "
            "either the block moved or the invocations changed shape"
        )
    return guards


def local_guards(text: str) -> set[str]:
    """Every `scripts/check-*` invoked anywhere in ci-local.sh."""
    guards: set[str] = set()
    for line in text.splitlines():
        if not FULL_LINE_COMMENT.match(line):
            guards.update(GUARD.findall(line))
    if not guards:
        raise ExtractionError(
            f"{CI_LOCAL} invokes no scripts/check-* guard -- either the "
            "invocations moved or their shape changed"
        )
    return guards


def locally_uncovered_ci_guards(
    ci: set[str], local: set[str], runner_only: frozenset[str] = RUNNER_ONLY_GUARDS
) -> set[str]:
    """CI shell guards that neither run locally nor have a runner-only exception."""
    return ci - local - runner_only


def ci_shell_guards(text: str) -> set[str]:
    """Every `scripts/check-*` a `run:` in ci.yml's `shell` job executes.

    Block scalars (`run: |`) count: the `bash -n` sweep is one, and a guard
    invoked inside a multi-line step is invoked. Comment lines do not, at either
    indent -- the steps in this job are documented by paragraphs that name the
    script they are about.
    """
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        m = JOB_KEY.match(line)
        if m and m.group("name") == "shell":
            start = i + 1
            break
    if start is None:
        raise ExtractionError(f"{CI_YML} has no `shell:` job")

    guards: set[str] = set()
    i = start
    while i < len(lines):
        line = lines[i]
        if JOB_KEY.match(line):
            break
        i += 1
        if FULL_LINE_COMMENT.match(line):
            continue
        m = RUN_KEY.match(line)
        if not m:
            continue
        value = m.group("value").strip()
        if not BLOCK_SCALAR.match(value):
            guards.update(GUARD.findall(value))
            continue
        # A block scalar: every following line indented deeper than the `run:`
        # key itself, blanks included.
        key_indent = len(m.group("indent"))
        while i < len(lines):
            body = lines[i]
            if body.strip() and len(body) - len(body.lstrip()) <= key_indent:
                break
            i += 1
            if not FULL_LINE_COMMENT.match(body):
                guards.update(GUARD.findall(body))

    if not guards:
        raise ExtractionError(
            f"{CI_YML}'s `shell` job runs no scripts/check-* guard -- "
            "either the job was renamed or its steps changed shape"
        )
    return guards


def annotate(message: str, path: pathlib.PurePath = CI_YML) -> None:
    """Attach a failure to the file that needs attention under GitHub Actions."""
    if os.environ.get("GITHUB_ACTIONS"):
        print(f"::error file={path}::{message}")


def drop_guard_lines(text: str, guard: str) -> str:
    """Text with every line that invokes `guard` deleted -- a fixture mutation.

    Any line, not just a `run:` one, so a guard invoked from inside a block
    scalar is mutated too. Comment lines are left alone: deleting the prose is
    not the mutation this fixture is about.
    """
    pattern = re.compile(r"\bscripts/" + re.escape(guard) + r"\b")
    return "\n".join(
        line
        for line in text.splitlines()
        if FULL_LINE_COMMENT.match(line) or not pattern.search(line)
    )


def run_fixtures(ci_text: str, local_text: str) -> int:
    """Prove the extractors can go red. Returns the number of fixtures checked.

    Two kinds: mutations of the real files -- delete each step CI is required to
    have and require the deletion to be reported -- and synthetic shapes whose
    only correct answer is an error.
    """
    checked = 0
    local = local_shell_guards(local_text)
    local_all = local_guards(local_text)
    ci = ci_shell_guards(ci_text)

    for guard in sorted(local):
        mutated = drop_guard_lines(ci_text, guard)
        try:
            after = ci_shell_guards(mutated)
        except ExtractionError as e:  # only if `guard` was the job's last step
            raise AssertionError(f"fixture: deleting {guard} broke extraction: {e}") from e
        if guard in after:
            raise AssertionError(
                f"fixture: deleting {guard}'s ci.yml step left it extracted -- "
                "the parser is reading something other than the step"
            )
        if not (local - after):
            raise AssertionError(f"fixture: deleting {guard}'s ci.yml step was not reported")
        checked += 1

    # Delete every real CI guard that is run by a separate ci-local job and
    # require the inverse comparison to catch it. This includes set-var today.
    for guard in sorted((ci & local_all) - local - RUNNER_ONLY_GUARDS):
        mutated = drop_guard_lines(local_text, guard)
        after = local_guards(mutated)
        if guard in after:
            raise AssertionError(
                f"fixture: deleting {guard}'s ci-local invocation left it "
                "extracted -- the parser is reading something other than the invocation"
            )
        if guard not in locally_uncovered_ci_guards(ci, after):
            raise AssertionError(
                f"fixture: deleting {guard}'s ci-local invocation was not reported"
            )
        checked += 1

    # A guard CI runs but ci-local never invokes is inverse drift, even when
    # ci-local's shell block and CI otherwise agree.
    inverse_ci = """
jobs:
  shell:
    steps:
      - run: scripts/check-smoke.sh
      - run: scripts/check-ci-only.sh
"""
    inverse_local = """
# --- shell -------------------------------------------------------------------
scripts/check-smoke.sh
# --- other -------------------------------------------------------------------
true
"""
    inverse_ci_guards = ci_shell_guards(inverse_ci)
    inverse_local_guards = local_guards(inverse_local)
    uncovered = locally_uncovered_ci_guards(inverse_ci_guards, inverse_local_guards)
    if uncovered != {"check-ci-only.sh"}:
        raise AssertionError("fixture: a CI-only guard was not reported")
    checked += 1

    if locally_uncovered_ci_guards(
        inverse_ci_guards,
        inverse_local_guards,
        frozenset({"check-ci-only.sh"}),
    ):
        raise AssertionError("fixture: a runner-only guard was not allow-listed")
    checked += 1

    # A guard that appears only in the prose above a step is not run by it.
    commented = """
jobs:
  shell:
    steps:
      # scripts/check-imaginary.sh explains itself here
      - name: a step
        run: scripts/check-smoke.sh
"""
    if ci_shell_guards(commented) != {"check-smoke.sh"}:
        raise AssertionError("fixture: a commented-out guard counted as a step")
    checked += 1

    # A guard inside a block scalar is run; a bash comment inside one is not.
    scalar = """
jobs:
  shell:
    steps:
      - run: |
          set -e
          # scripts/check-imaginary.sh
          scripts/check-smoke.sh
  other:
    steps:
      - run: scripts/check-elsewhere.sh
"""
    if ci_shell_guards(scalar) != {"check-smoke.sh"}:
        raise AssertionError("fixture: block scalar or job boundary misparsed")
    checked += 1

    # Empty and missing extractions are errors, not empty sets.
    for name, text, extract in (
        ("ci.yml without a shell job", "jobs:\n  fmt:\n    steps: []\n", ci_shell_guards),
        (
            "a shell job with no guard",
            "jobs:\n  shell:\n    steps:\n      - run: true\n",
            ci_shell_guards,
        ),
        ("ci-local without a shell banner", "# --- fmt ---\ntrue\n", local_shell_guards),
        (
            "a shell block with no guard",
            "# --- shell ---\nbash -n f\n# --- claude-md ---\nscripts/check-smoke.sh\n",
            local_shell_guards,
        ),
        ("ci-local with no guard", "# --- shell ---\nbash -n f\n", local_guards),
    ):
        try:
            extract(text)
        except ExtractionError:
            checked += 1
        else:
            raise AssertionError(f"fixture: {name} extracted without an error")

    return checked


def main(argv: list[str]) -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    if "--print-lists" in argv:
        for label, guards in (
            ("ci-local shell", local_shell_guards((root / CI_LOCAL).read_text())),
            ("ci-local all", local_guards((root / CI_LOCAL).read_text())),
            ("ci.yml shell", ci_shell_guards((root / CI_YML).read_text())),
        ):
            print(f"{label}: {' '.join(sorted(guards))}")
        return 0

    ci_text = (root / CI_YML).read_text()
    local_text = (root / CI_LOCAL).read_text()

    try:
        fixtures = run_fixtures(ci_text, local_text)
        local = local_shell_guards(local_text)
        local_all = local_guards(local_text)
        ci = ci_shell_guards(ci_text)
    except (ExtractionError, AssertionError) as e:
        print(f"FAIL {e}", file=sys.stderr)
        annotate(str(e))
        return 1

    missing = sorted(local - ci)
    if missing:
        for guard in missing:
            msg = (
                f"scripts/{guard} runs in {CI_LOCAL}'s shell block but is not a "
                f"step of {CI_YML}'s `shell` job -- add "
                f"`- run: scripts/{guard}` there (python guards run as "
                "`python3 scripts/...`)"
            )
            print(f"FAIL {msg}", file=sys.stderr)
            annotate(msg)
        print(
            f"\n{len(missing)} local-only guard(s); the local gate and CI must "
            "ask the same question.",
            file=sys.stderr,
        )
        return 1

    stale_runner_only = sorted(RUNNER_ONLY_GUARDS - ci)
    if stale_runner_only:
        msg = (
            "RUNNER_ONLY_GUARDS names guard(s) not run by ci.yml's `shell` job: "
            f"{' '.join(stale_runner_only)}"
        )
        print(f"FAIL {msg}", file=sys.stderr)
        annotate(msg, CHECKER)
        return 1

    local_runner_only = sorted(RUNNER_ONLY_GUARDS & local_all)
    if local_runner_only:
        msg = (
            "RUNNER_ONLY_GUARDS names guard(s) that ci-local.sh invokes: "
            f"{' '.join(local_runner_only)}"
        )
        print(f"FAIL {msg}", file=sys.stderr)
        annotate(msg, CHECKER)
        return 1

    missing_local = sorted(locally_uncovered_ci_guards(ci, local_all))
    if missing_local:
        for guard in missing_local:
            msg = (
                f"scripts/{guard} runs in {CI_YML}'s `shell` job but is never "
                f"invoked by {CI_LOCAL} -- add it to an appropriate local job, "
                "or allow-list it in RUNNER_ONLY_GUARDS if it can only run on "
                "the CI runner"
            )
            print(f"FAIL {msg}", file=sys.stderr)
            annotate(msg, CI_LOCAL)
        print(
            f"\n{len(missing_local)} CI-only guard(s); the local gate and CI "
            "must ask the same question.",
            file=sys.stderr,
        )
        return 1

    separate = sorted((ci - local) & local_all)
    runner_only = sorted(ci & RUNNER_ONLY_GUARDS)
    tails = []
    if separate:
        tails.append(f"{len(separate)} in separate local jobs ({' '.join(separate)})")
    if runner_only:
        tails.append(f"{len(runner_only)} runner-only ({' '.join(runner_only)})")
    tail = f", {', '.join(tails)}" if tails else ""
    # `ok   <N> ...` -- the same shape check-set-var.py prints.
    print(f"ok   {len(local)} shell guards in CI and the local gate{tail}; {fixtures} fixtures")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
