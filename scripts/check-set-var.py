#!/usr/bin/env python3
"""Refuse any `std::env::set_var` / `remove_var` call in tracked Rust crates.

The test harness runs a binary's tests on parallel threads and the process
environment is shared between them, so two tests that `set_var` the same knob
race: one reads the other's value. That flake shipped more than once: the
compactor's idle-backoff bounds on 2026-08-14 (3c6d88a) and the storage
attributes-compression test on 2026-09-02 (7f4e938), which failed one run in
several with a sibling's zstd level. The fix, decided that day, is a convention
rather than a mutex: every env-reading knob gets a pure resolver twin --
`fn x_from(Option<&str>)` -- and tests drive the pure function. Nothing enforced
it, and a convention nobody enforces is a wish.

This gate rejects every `set_var(` or `remove_var(` call in tracked
`crates/**/*.rs`, with the file:line of every site.

Line comments are stripped first: the pure resolvers' doc comments explain
themselves by naming the `set_var` they replaced, and a gate that fires on its
own justification gets deleted.

Runs in the `shell` CI job (no compilation) and as the `set-var` line of
scripts/ci-local.sh.
"""

from __future__ import annotations

import os
import pathlib
import re
import subprocess
import sys

# A call site. `\b` keeps `unset_var(` or `reset_var(` from matching, if such a
# helper is ever written; `\s*` allows the formatting rustfmt never produces.
CALL = re.compile(r"\b(?:set_var|remove_var)\s*\(")

# Everything after `//` on a line. Good enough for a guard: a string literal
# holding `//` followed by a real `set_var(` on the same line is not a shape this
# tree has, and the failure mode is a missed site, not a false red.
LINE_COMMENT = re.compile(r"//.*")


def tracked_rust_files(root: pathlib.Path) -> list[pathlib.PurePath]:
    """Every `.rs` file git tracks under crates/ -- src, tests, benches, examples.

    `git ls-files`, not a walk: `crates/*/target` or an untracked scratch file
    must not be able to turn the gate red, and third_party/ is outside crates/
    by construction.
    """
    out = subprocess.run(
        ["git", "ls-files", "-z", "--", "crates/*.rs"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [pathlib.PurePath(name) for name in out.split("\0") if name]


def call_sites(path: pathlib.Path) -> list[tuple[int, str]]:
    sites = []
    for lineno, line in enumerate(path.read_text().splitlines(), 1):
        code = LINE_COMMENT.sub("", line)
        if CALL.search(code):
            sites.append((lineno, line.strip()))
    return sites


def annotate(rel: pathlib.PurePath, lineno: int | None, message: str) -> None:
    """A GitHub annotation on the offending line, when running under Actions."""
    if not os.environ.get("GITHUB_ACTIONS"):
        return
    where = f"file={rel}" + (f",line={lineno}" if lineno else "")
    print(f"::error {where}::{message}")


def main() -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    files = tracked_rust_files(root)
    # ABSENCE IS NEVER SILENCE: an empty file list means the pathspec or the
    # checkout broke, and a gate that checked nothing has not passed.
    if not files:
        print("FAIL no tracked .rs files under crates/ -- the gate checked nothing", file=sys.stderr)
        return 1

    offenders: dict[pathlib.PurePath, list[tuple[int, str]]] = {}
    for rel in files:
        sites = call_sites(root / rel)
        if sites:
            offenders[rel] = sites

    problems: list[str] = []
    for rel, sites in sorted(offenders.items()):
        for lineno, text in sites:
            msg = f"{rel}:{lineno} mutates the process environment: {text[:100]}"
            problems.append(msg)
            annotate(rel, lineno, msg)

    if problems:
        for p in problems:
            print(f"FAIL {p}", file=sys.stderr)
        print(
            "FAIL tests must not mutate the process environment: give the knob "
            "a pure resolver twin (`fn x_from(Option<&str>)`) and drive that "
            "from the test instead.",
            file=sys.stderr,
        )
        print(
            f"\n{len(problems)} forbidden mutation site(s); "
            f"{len(files)} Rust files checked.",
            file=sys.stderr,
        )
        return 1

    n_sites = sum(len(s) for s in offenders.values())
    # `ok   <N> ...` -- ci-local.sh reads the second field for its status line.
    print(
        f"ok   {n_sites} process-environment mutation sites in "
        f"{len(files)} tracked Rust files under crates/"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
