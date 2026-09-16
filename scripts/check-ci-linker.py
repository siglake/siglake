#!/usr/bin/env python3
"""A workflow job that asks for the mold linker must install it.

`-C link-arg=-fuse-ld=mold` is a promise that mold is on the runner. No runner
image ships it, so every job that wants it apt-gets it first. The flag used to
sit in ci.yml's workflow-wide `env:`, where jobs inherit it whether or not they
ever install anything -- and one did. `docker` builds two cargo suites (the
MinIO mirror-pagination tests and the Postgres ownership tests) and has no
install step, so from the day the flag landed every one of its runs died on the
first build script it linked:

    = note: "cc" ... "-nodefaultlibs" "-fuse-ld=mold"
    = note: collect2: fatal error: cannot find 'ld'
    error: could not compile `libc` (build script) due to 1 previous error

The job is named after the tests, so the red run reads as a MinIO or Postgres
failure. It was not one: cargo never reached a test, the compose stack came up
and was torn straight back down, and the runner spent its whole budget failing
to link `serde`.

This gate reads every workflow under .github/workflows and requires, in each:

  * nothing asks for mold above `jobs:` -- a workflow-level flag is inherited
    by jobs that install nothing, which is exactly the shape above;
  * a job that asks for mold installs it;
  * a job that installs mold asks for it -- the other direction catches a
    dropped flag leaving a minute of apt behind in every run.

A job that does neither is fine: it links with the default linker and is
slower, which is the entire cost of forgetting.

Stdlib only, deliberately: this runs in ci.yml's `shell` job, which installs
nothing. The parse is line-based over comment-stripped text -- job names at the
two-space indent below `jobs:`, and two disjoint patterns, the flag and the
install -- so prose that mentions mold (this file's own docstring included,
were it ever pasted into a workflow) cannot stand in for either one.

Runs as part of the `shell` line of scripts/ci-local.sh and as a step of
ci.yml's `shell` job.

A parser whose failure mode is a green run is worse than no parser, so a
fixture suite runs first, on every invocation: it deletes the real install step
of each real job in turn, and hoists each real flag to the workflow level, and
requires both mutations to be reported. It costs milliseconds and needs no
fixtures on disk.
"""

from __future__ import annotations

import os
import pathlib
import re
import sys

WORKFLOWS = pathlib.PurePath(".github/workflows")
CHECKER = pathlib.PurePath("scripts/check-ci-linker.py")

# The ask. Only ever written as a RUSTFLAGS link-arg, but matched on the linker
# selection itself so a differently spelled RUSTFLAGS still counts.
FLAG = re.compile(r"-fuse-ld=mold")

# The install. Deliberately narrow: a package manager installing `mold`, or the
# marketplace action for it. A new mechanism makes this gate RED with the
# message below rather than quietly passing -- teach it the new shape then.
INSTALL = re.compile(r"\b(?:apt-get|apt|dnf|yum|brew|pacman)\b.*\binstall\b.*\bmold\b|setup-mold")

# A whole line of comment. Not a trailing one: deciding whether a `#` is inside
# a quoted RUSTFLAGS value is the parser this file is trying not to be.
FULL_LINE_COMMENT = re.compile(r"^\s*#")

JOBS_KEY = re.compile(r"^jobs:\s*(?:#.*)?$")
JOB_KEY = re.compile(r"^  (?P<name>[A-Za-z0-9_-]+):\s*(?:#.*)?$")
TOP_KEY = re.compile(r"^[A-Za-z0-9_-]+:")


class ExtractionError(Exception):
    """The file did not have the shape this gate parses.

    Never an empty result: a workflow with no jobs is a parse that failed, not
    a workflow that passed.
    """


def split_jobs(text: str) -> tuple[list[str], dict[str, list[str]]]:
    """(lines above `jobs:`, {job name: its lines}) -- comment lines dropped.

    Everything above `jobs:` is the workflow preamble, where an `env:` reaches
    every job. A job owns every line until the next two-space key or the next
    top-level key.
    """
    lines = [line for line in text.splitlines() if not FULL_LINE_COMMENT.match(line)]
    start = next((i for i, line in enumerate(lines) if JOBS_KEY.match(line)), None)
    if start is None:
        raise ExtractionError("no `jobs:` key")

    preamble = lines[:start]
    jobs: dict[str, list[str]] = {}
    current: str | None = None
    for line in lines[start + 1 :]:
        m = JOB_KEY.match(line)
        if m:
            current = m.group("name")
            jobs[current] = []
            continue
        if TOP_KEY.match(line):  # a key back at column 0 ends the jobs block
            current = None
            continue
        if current is not None:
            jobs[current].append(line)

    if not jobs:
        raise ExtractionError("`jobs:` declares no job -- the indent this gate reads changed")
    return preamble, jobs


def asks_for_mold(lines: list[str]) -> bool:
    return any(FLAG.search(line) for line in lines)


def installs_mold(lines: list[str]) -> bool:
    return any(INSTALL.search(line) for line in lines)


def audit(name: str, text: str) -> list[str]:
    """Every way `text` breaks the flag/install pairing, as reader-facing lines."""
    preamble, jobs = split_jobs(text)
    problems: list[str] = []

    if asks_for_mold(preamble):
        problems.append(
            f"{name}: `-fuse-ld=mold` is set above `jobs:`, so every job inherits it "
            "-- including any that never installs mold, whose first link then fails "
            "with `collect2: fatal error: cannot find 'ld'`. Put it in the `env:` of "
            "each job that installs mold."
        )

    for job, lines in jobs.items():
        flag, install = asks_for_mold(lines), installs_mold(lines)
        if flag and not install:
            problems.append(
                f"{name}: job `{job}` sets `-fuse-ld=mold` but never installs mold "
                "-- add `- name: Install mold linker` / `run: sudo apt-get install -y mold`, "
                "or drop the flag and link with the default linker."
            )
        elif install and not flag:
            problems.append(
                f"{name}: job `{job}` installs mold but never sets `-fuse-ld=mold` "
                "-- either the flag was dropped (the install is now a minute of apt "
                "for nothing) or this gate does not recognise how the flag is set."
            )

    return problems


def annotate(message: str, path: pathlib.PurePath) -> None:
    """Attach a failure to the file that needs attention under GitHub Actions."""
    if os.environ.get("GITHUB_ACTIONS"):
        print(f"::error file={path}::{message}")


def drop_matching(text: str, pattern: re.Pattern[str]) -> str:
    """Text with every non-comment line matching `pattern` deleted."""
    return "\n".join(
        line
        for line in text.splitlines()
        if FULL_LINE_COMMENT.match(line) or not pattern.search(line)
    )


def hoist_flag(text: str) -> str:
    """The real workflow with the flag moved to a workflow-level `env:`.

    The mutation the `docker` failure actually was: the flag reaching a job
    that installs nothing, by inheritance rather than by its own `env:`.
    """
    return re.sub(
        r"^jobs:",
        'env:\n  RUSTFLAGS: "-C link-arg=-fuse-ld=mold"\njobs:',
        drop_matching(text, FLAG),
        count=1,
        flags=re.M,
    )


def run_fixtures(real: list[tuple[str, str]]) -> int:
    """Prove the gate can go red. Returns the number of fixtures checked."""
    checked = 0

    for name, text in real:
        _, jobs = split_jobs(text)
        paired = sorted(j for j, lines in jobs.items() if asks_for_mold(lines))
        for job in paired:
            # Delete this job's install step only: the flag stays, so the job
            # becomes exactly the broken `docker` job.
            mutated_lines = []
            in_job = False
            for line in text.splitlines():
                m = JOB_KEY.match(line)
                if m:
                    in_job = m.group("name") == job
                if in_job and not FULL_LINE_COMMENT.match(line) and INSTALL.search(line):
                    continue
                mutated_lines.append(line)
            mutated = "\n".join(mutated_lines)
            if installs_mold(split_jobs(mutated)[1][job]):
                raise AssertionError(
                    f"fixture: deleting {name} job `{job}`'s install left it extracted "
                    "-- the parser is reading something other than the step"
                )
            if not any(f"job `{job}`" in p for p in audit(name, mutated)):
                raise AssertionError(
                    f"fixture: {name} job `{job}` without its install was not reported"
                )
            checked += 1

        if paired:
            if not any("above `jobs:`" in p for p in audit(name, hoist_flag(text))):
                raise AssertionError(f"fixture: {name} with a workflow-level flag was not reported")
            checked += 1

    # A job that installs mold and asks for nothing is the inverse drift.
    stale = """
jobs:
  test:
    steps:
      - run: sudo apt-get install -y mold
      - run: cargo test
"""
    if not any("installs mold but never sets" in p for p in audit("fixture", stale)):
        raise AssertionError("fixture: an install with no flag was not reported")
    checked += 1

    # Neither half is not a finding: the default linker is a valid choice.
    plain = """
jobs:
  test:
    steps:
      - run: cargo test
"""
    if audit("fixture", plain):
        raise AssertionError("fixture: a job using the default linker was reported")
    checked += 1

    # A paired job passes, and a comment naming either half is prose, not a step.
    paired_ok = """
env:
  CARGO_TERM_COLOR: always
jobs:
  test:
    env:
      RUSTFLAGS: "-C link-arg=-fuse-ld=mold"
    steps:
      - run: sudo apt-get install -y mold
      - run: cargo test
  # sudo apt-get install -y mold is explained here, and -fuse-ld=mold too
  deny:
    steps:
      - uses: EmbarkStudios/cargo-deny-action@v2
"""
    if audit("fixture", paired_ok):
        raise AssertionError("fixture: a correctly paired workflow was reported")
    checked += 1

    # Shapes whose only correct answer is an error.
    for name, text in (
        ("a file with no jobs key", "on:\n  push:\n"),
        ("a jobs key with no job", "jobs:\n"),
    ):
        try:
            split_jobs(text)
        except ExtractionError:
            checked += 1
        else:
            raise AssertionError(f"fixture: {name} extracted without an error")

    return checked


def workflows(root: pathlib.Path) -> list[tuple[str, str]]:
    """Every workflow file, as (name, text), newest name order."""
    directory = root / WORKFLOWS
    found = sorted(p for p in directory.glob("*.y*ml") if p.suffix in (".yml", ".yaml"))
    if not found:
        raise ExtractionError(f"{WORKFLOWS} holds no workflow -- the gate checked nothing")
    return [(p.name, p.read_text()) for p in found]


def main(argv: list[str]) -> int:
    root = pathlib.Path(__file__).resolve().parent.parent

    try:
        real = workflows(root)
        fixtures = run_fixtures(real)
    except (ExtractionError, AssertionError) as e:
        print(f"FAIL {e}", file=sys.stderr)
        annotate(str(e), CHECKER)
        return 1

    problems: list[str] = []
    paired = 0
    for name, text in real:
        try:
            problems.extend(audit(name, text))
        except ExtractionError as e:
            print(f"FAIL {name}: {e}", file=sys.stderr)
            annotate(f"{name}: {e}", WORKFLOWS / name)
            return 1
        paired += sum(1 for lines in split_jobs(text)[1].values() if asks_for_mold(lines))

    if problems:
        for problem in problems:
            print(f"FAIL {problem}", file=sys.stderr)
            annotate(problem, WORKFLOWS / problem.split(":", 1)[0])
        return 1

    if "--print-lists" in argv:
        for name, text in real:
            for job, lines in split_jobs(text)[1].items():
                state = "mold" if asks_for_mold(lines) else "default linker"
                print(f"{name} {job}: {state}")

    # `ok   <N> ...` -- the same shape check-shell-job-parity.py prints.
    print(
        f"ok   {paired} job(s) pair -fuse-ld=mold with an install across "
        f"{len(real)} workflow(s); {fixtures} fixtures"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
