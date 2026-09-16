#!/usr/bin/env python3
"""The local gate builds under the settings CI builds under.

`.github/workflows/ci.yml` gives every cargo job a workflow-wide `env:` block
and, in each job that installs mold, `RUSTFLAGS: -C link-arg=-fuse-ld=mold`.
Until 2026-09-11 `scripts/ci-local.sh` applied none of it: `grep -n
'CARGO_PROFILE\\|RUSTFLAGS\\|CARGO_INCREMENTAL\\|fuse-ld' scripts/ci-local.sh`
returned nothing, so the local gate built with full debuginfo, built
incrementally, and linked with the default linker. Three differences from the
run it claims to predict, and the third is the one difference a tree that
registers types through link sections (`inventory`, via `typetag`, via
third_party/iceberg) can actually change behaviour on -- which is why #2931's
runner-only test failure could not be reproduced locally at all.

The settings now live in scripts/ci-local-build-env.sh, and this gate holds the
two files together:

  * every key in ci.yml's workflow `env:` is either mirrored by `CI_ENV` with
    the same value, or named in NOT_MIRRORED below with a reason;
  * `CI_ENV` invents nothing ci.yml does not set, and NOT_MIRRORED names
    nothing ci.yml has stopped setting;
  * every RUSTFLAGS a ci.yml job sets is the flag `CI_MOLD_FLAG` spells;
  * the helper's two arms behave: with mold, the flag is exported and the
    status is `ok`; without it, RUSTFLAGS is NOT set to some other linker and
    the status is a `skipped (...)` naming mold -- which report() turns into a
    job NOT RUN under --strict;
  * ci-local.sh actually calls both halves.

Stdlib only, deliberately: this runs in ci.yml's `shell` job, which installs
nothing. `bash` is the one external program, and it is what is being checked.

Runs as part of the `shell` line of scripts/ci-local.sh and as a step of
ci.yml's `shell` job.

A parser whose failure mode is a green run is worse than no parser, so a
fixture suite runs first, on every invocation: it drops, retitles and rewrites
each real setting in turn and requires the mutation to be reported, and it
feeds the extractors shapes whose only correct answer is an error.
"""

from __future__ import annotations

import os
import pathlib
import re
import subprocess
import sys
import tempfile

CI_YML = pathlib.PurePath(".github/workflows/ci.yml")
CI_LOCAL = pathlib.PurePath("scripts/ci-local.sh")
HELPER = pathlib.PurePath("scripts/ci-local-build-env.sh")
CHECKER = pathlib.PurePath("scripts/check-ci-build-env.py")

# Keys ci.yml sets for every cargo job that the local gate deliberately does
# NOT set, and why. An entry here is an exception a reader can weigh, not a
# silent omission; the key must still be in ci.yml's block, and the helper must
# name it, or this gate fails.
NOT_MIRRORED = {
    "CARGO_TERM_COLOR": (
        "`always` puts SGR escapes in front of every diagnostic, and this gate "
        "reads its own job logs with anchored patterns -- `grep -E '^error'` "
        "over clippy.log, `/^test result: ok\\./` over test.log -- that a "
        "leading \\e[1m\\e[31m stops matching. CI has a terminal reading the "
        "output; the local gate has awk."
    ),
}

# `  KEY: value` under a top-level `env:`. GitHub Actions env values are plain
# scalars; the quotes ci.yml writes around `"0"` are YAML's, not the value's.
ENV_ENTRY = re.compile(r"^  (?P<key>[A-Za-z_][A-Za-z0-9_]*):\s*(?P<value>\S.*?)\s*$")
RUSTFLAGS_ENTRY = re.compile(r"^\s+RUSTFLAGS:\s*(?P<value>\S.*?)\s*$")
TOP_KEY = re.compile(r"^[A-Za-z0-9_-]+:")
ENV_KEY = re.compile(r"^env:\s*(?:#.*)?$")
JOBS_KEY = re.compile(r"^jobs:\s*(?:#.*)?$")
JOB_KEY = re.compile(r"^  (?P<name>[A-Za-z0-9_-]+):\s*(?:#.*)?$")
FULL_LINE_COMMENT = re.compile(r"^\s*#")

# `CI_ENV=(` ... `)` in the helper, one `NAME=value` per line.
CI_ENV_BLOCK = re.compile(r"^CI_ENV=\(\s*$(?P<body>.*?)^\)\s*$", re.M | re.S)
CI_ENV_ITEM = re.compile(r"^\s*(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>\S*)\s*$")
MOLD_FLAG = re.compile(r"^CI_MOLD_FLAG='(?P<value>[^']*)'\s*$", re.M)


class ExtractionError(Exception):
    """The file did not have the shape this gate parses.

    Never an empty result: a gate that compared nothing to nothing has not
    passed, and an empty `CI_ENV` is exactly the state before #2947.
    """


def unquote(value: str) -> str:
    """A YAML scalar as the runner would see it. Comments are not stripped:
    deciding whether a `#` is inside a quote is the parser this is not."""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def workflow_env(text: str) -> dict[str, str]:
    """ci.yml's workflow-wide `env:` block -- the one every job inherits."""
    lines = [line for line in text.splitlines() if not FULL_LINE_COMMENT.match(line)]
    start = None
    for i, line in enumerate(lines):
        if JOBS_KEY.match(line):
            break
        if ENV_KEY.match(line):
            start = i + 1
            break
    if start is None:
        raise ExtractionError(f"{CI_YML} has no workflow-level `env:` block above `jobs:`")

    env: dict[str, str] = {}
    for line in lines[start:]:
        if not line.strip():
            continue
        m = ENV_ENTRY.match(line)
        if not m:
            break
        env[m.group("key")] = unquote(m.group("value"))
    if not env:
        raise ExtractionError(f"{CI_YML}'s workflow `env:` block declares no key")
    return env


def job_rustflags(text: str) -> dict[str, str]:
    """{job name: its RUSTFLAGS} for every job in ci.yml that sets one.

    Job-level, not workflow-level: check-ci-linker.py owns the rule that the
    flag may not sit above `jobs:`, and this gate only reads what the jobs say.
    """
    lines = [line for line in text.splitlines() if not FULL_LINE_COMMENT.match(line)]
    start = next((i for i, line in enumerate(lines) if JOBS_KEY.match(line)), None)
    if start is None:
        raise ExtractionError(f"{CI_YML} has no `jobs:` key")

    flags: dict[str, str] = {}
    current: str | None = None
    for line in lines[start + 1 :]:
        m = JOB_KEY.match(line)
        if m:
            current = m.group("name")
            continue
        if TOP_KEY.match(line):  # a key back at column 0 ends the jobs block
            current = None
            continue
        f = RUSTFLAGS_ENTRY.match(line)
        if f and current is not None:
            flags[current] = unquote(f.group("value"))
    if not flags:
        raise ExtractionError(
            f"{CI_YML} has no job setting RUSTFLAGS -- either the cargo jobs stopped "
            "choosing a linker or this gate no longer reads how they set it"
        )
    return flags


def helper_env(text: str) -> dict[str, str]:
    """`CI_ENV` from scripts/ci-local-build-env.sh, as {key: value}."""
    m = CI_ENV_BLOCK.search(text)
    if not m:
        raise ExtractionError(f"{HELPER} has no `CI_ENV=(` ... `)` array")
    env: dict[str, str] = {}
    for line in m.group("body").splitlines():
        if not line.strip() or FULL_LINE_COMMENT.match(line):
            continue
        item = CI_ENV_ITEM.match(line)
        if not item:
            raise ExtractionError(f"{HELPER}: `CI_ENV` entry is not NAME=value: {line.strip()!r}")
        env[item.group("key")] = item.group("value")
    if not env:
        raise ExtractionError(
            f"{HELPER}'s `CI_ENV` is empty -- the local gate would apply none of "
            "ci.yml's build settings, which is the state task #2947 fixed"
        )
    return env


def helper_mold_flag(text: str) -> str:
    m = MOLD_FLAG.search(text)
    if not m or not m.group("value"):
        raise ExtractionError(f"{HELPER} has no single-quoted `CI_MOLD_FLAG=` assignment")
    return m.group("value")


def parity_problems(
    ci_env: dict[str, str],
    ci_flags: dict[str, str],
    local_env: dict[str, str],
    local_flag: str,
    helper_text: str,
    not_mirrored: dict[str, str] = NOT_MIRRORED,
) -> list[str]:
    """Every way the two files disagree, as reader-facing lines."""
    problems: list[str] = []

    for key, value in sorted(ci_env.items()):
        if key in not_mirrored:
            continue
        if key not in local_env:
            problems.append(
                f"{CI_YML} sets `{key}: {value}` for every cargo job and {HELPER}'s "
                f"CI_ENV does not -- add `{key}={value}` there, or add {key} to "
                f"NOT_MIRRORED in {CHECKER} with the reason it is left out."
            )
        elif local_env[key] != value:
            problems.append(
                f"{CI_YML} sets `{key}: {value}`, {HELPER} sets "
                f"`{key}={local_env[key]}` -- the local gate would build under a "
                "setting no CI job uses."
            )

    for key in sorted(local_env):
        if key not in ci_env:
            problems.append(
                f"{HELPER}'s CI_ENV sets `{key}` and {CI_YML} does not -- this file "
                "mirrors CI, so a setting only the local gate uses belongs in "
                "ci-local.sh with its own reason."
            )

    for key in sorted(not_mirrored):
        if key not in ci_env:
            problems.append(
                f"NOT_MIRRORED in {CHECKER} excuses `{key}`, which {CI_YML} no longer "
                "sets -- drop the entry."
            )
        elif key in local_env:
            problems.append(
                f"`{key}` is both in NOT_MIRRORED and in {HELPER}'s CI_ENV -- one of "
                "the two is wrong about whether the local gate applies it."
            )
        elif key not in helper_text:
            problems.append(
                f"NOT_MIRRORED excuses `{key}` and {HELPER} never names it -- a reader "
                "of the helper cannot see which of ci.yml's keys it drops."
            )

    wrong = sorted(job for job, value in ci_flags.items() if value != local_flag)
    if wrong:
        problems.append(
            f"{CI_YML} job(s) {', '.join(wrong)} set RUSTFLAGS to "
            + ", ".join(sorted({ci_flags[j] for j in wrong}))
            + f" and {HELPER}'s CI_MOLD_FLAG is `{local_flag}` -- the local gate would "
            "link with flags no CI job uses."
        )

    return problems


def status_problems(
    ok_status: str, skipped_status: str, rustflags_without_mold: str | None
) -> list[str]:
    """Every way the helper's two arms fail the not-exercised rule."""
    problems: list[str] = []
    if not ok_status.startswith("ok"):
        problems.append(
            f"{HELPER}: with mold present the build-env status is {ok_status!r}, "
            "which report() does not read as a passing job."
        )
    if not skipped_status.startswith("skipped ("):
        problems.append(
            f"{HELPER}: with mold missing the build-env status is {skipped_status!r}. "
            "It must be a `skipped (...)`, which is what report() turns into a FAIL "
            "line under --strict; anything else lets a run that never linked with "
            "mold report as one that did."
        )
    if "mold" not in skipped_status:
        problems.append(
            f"{HELPER}: the missing-mold status {skipped_status!r} does not name mold, "
            "so the strict FAIL line would not say what is missing."
        )
    if rustflags_without_mold is not None:
        problems.append(
            f"{HELPER}: apply_build_env exported RUSTFLAGS="
            f"{rustflags_without_mold!r} on a box without mold -- a silent fallback "
            "to another linker is what this job exists to refuse."
        )
    return problems


def bash(root: pathlib.Path, script: str) -> str:
    """Run `script` from the repo root with a clean build environment."""
    proc = subprocess.run(
        ["bash", "-c", script],
        cwd=root,
        env={
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("CARGO_PROFILE", "RUSTFLAGS")) and k != "CARGO_INCREMENTAL"
        },
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise ExtractionError(
            f"driving {HELPER} failed (exit {proc.returncode}): {proc.stderr.strip()}"
        )
    return proc.stdout


# @-delimited placeholders rather than str.format: these are bash, and bash is
# made of braces.
DRIVE = """
set -u
. @HELPER@
build_env_status 1
build_env_status 0
( apply_build_env 0; printf 'RUSTFLAGS=%s\\n' "${RUSTFLAGS-<unset>}" )
( apply_build_env 1; printf 'MOLD_RUSTFLAGS=%s\\n' "${RUSTFLAGS-<unset>}" )
( apply_build_env 0
  for entry in "${CI_ENV[@]}"; do
    name=${entry%%=*}
    printf 'APPLIED %s=%s\\n' "$name" "${!name-<unset>}"
  done )
"""


def drive_helper(root: pathlib.Path) -> tuple[str, str, str | None, dict[str, str]]:
    """(ok status, skipped status, RUSTFLAGS without mold or None, applied env)."""
    out = bash(root, DRIVE.replace("@HELPER@", str(HELPER)))
    lines = out.splitlines()
    if len(lines) < 4:
        raise ExtractionError(f"{HELPER} answered {out!r}, which is not two statuses and two arms")
    ok_status, skipped_status = lines[0], lines[1]
    without = lines[2].split("=", 1)[1]
    with_mold = lines[3].split("=", 1)[1]
    applied = dict(
        line.split(" ", 1)[1].split("=", 1) for line in lines[4:] if line.startswith("APPLIED ")
    )
    if with_mold == "<unset>":
        raise ExtractionError(
            f"{HELPER}: apply_build_env 1 left RUSTFLAGS unset, so a box WITH mold "
            "would still link with the default linker"
        )
    return ok_status, skipped_status, (None if without == "<unset>" else without), applied


ACCOUNT = r"""
set -u
awk '/^report\(\) \{$/,/^\}$/' @CI_LOCAL@ >@WORK@/report.sh
[ -s @WORK@/report.sh ] || { echo 'no report() in @CI_LOCAL@' >&2; exit 1; }
for strict in 0 1; do (
  . @WORK@/report.sh
  STRICT=$strict LOG_DIR=@WORK@ job_started=$SECONDS fail=0 red=0 strict_skips=0
  # A subshell, not a command substitution: report()'s counters are half the
  # claim, and $(...) would throw them away with the subshell it runs in.
  report build-env "@STATUS@" >@WORK@/line.$strict 2>/dev/null
  printf '%s|%s|fail=%s red=%s skips=%s\n' "$strict" \
    "$(sed -e 's/ ([0-9]*s)$//' -e 's/ *$//' @WORK@/line.$strict)" \
    "$fail" "$red" "$strict_skips"
); done
"""


def report_accounting(root: pathlib.Path, status: str) -> dict[str, str]:
    """What ci-local.sh's own report() does with `status`, strict and not.

    report() lives inline in ci-local.sh, so it is lifted by name and driven --
    the same technique check-external-readers-report.sh uses. What is pinned
    here is the accounting, not the wording: a strict run must set fail=1 and
    count a job NOT RUN (red=0), so the summary says neither ALL CHECKED JOBS
    GREEN nor that main would be red.
    """
    if any(c in status for c in '"$`\\'):
        raise ExtractionError(
            f"the build-env status {status!r} carries a character this driver cannot "
            "hand to bash unchanged"
        )
    with tempfile.TemporaryDirectory() as work:
        script = (
            ACCOUNT.replace("@WORK@", work)
            .replace("@CI_LOCAL@", str(CI_LOCAL))
            .replace("@STATUS@", status)
        )
        out = bash(root, script)
        return dict(line.split("|", 1) for line in out.splitlines())


def annotate(message: str, path: pathlib.PurePath = CI_YML) -> None:
    """Attach a failure to the file that needs attention under GitHub Actions."""
    if os.environ.get("GITHUB_ACTIONS"):
        print(f"::error file={path}::{message}")


def drop_lines(text: str, pattern: str) -> str:
    rx = re.compile(pattern)
    return "\n".join(
        line for line in text.splitlines() if FULL_LINE_COMMENT.match(line) or not rx.search(line)
    )


def run_fixtures(ci_text: str, helper_text: str) -> int:
    """Prove the gate can go red. Returns the number of fixtures checked."""
    checked = 0
    ci_env = workflow_env(ci_text)
    ci_flags = job_rustflags(ci_text)
    local_env = helper_env(helper_text)
    local_flag = helper_mold_flag(helper_text)

    # The mutations below are all "the real files, minus one thing", so they
    # only mean anything if the real files pass. A real drift surfaces here
    # first; carry it, or the fixture hides the finding it is standing on.
    real = parity_problems(ci_env, ci_flags, local_env, local_flag, helper_text)
    if real:
        raise AssertionError("the real files do not agree: " + "; ".join(real))
    checked += 1

    # Each mirrored key, dropped from the helper and then given a wrong value.
    for key in sorted(local_env):
        dropped = {k: v for k, v in local_env.items() if k != key}
        if not any(
            f"`{key}: " in p
            for p in parity_problems(ci_env, ci_flags, dropped, local_flag, helper_text)
        ):
            raise AssertionError(f"fixture: dropping {key} from CI_ENV was not reported")
        wrong = dict(local_env, **{key: local_env[key] + "-drifted"})
        if not any(
            "no CI job uses" in p
            for p in parity_problems(ci_env, ci_flags, wrong, local_flag, helper_text)
        ):
            raise AssertionError(f"fixture: a drifted {key} value was not reported")
        checked += 2

    # The same dropped from the helper FILE, so the extractor is exercised too.
    for key in sorted(local_env):
        mutated = drop_lines(helper_text, rf"^\s*{key}=")
        if key in helper_env(mutated):
            raise AssertionError(
                f"fixture: deleting {key}'s CI_ENV line left it extracted -- the "
                "parser is reading something other than the array"
            )
        checked += 1

    # A local-only invention, a stale exception, and an exception the helper
    # does not name.
    invented = dict(local_env, SIGLAKE_LOCAL_ONLY="1")
    if not any(
        "mirrors CI" in p
        for p in parity_problems(ci_env, ci_flags, invented, local_flag, helper_text)
    ):
        raise AssertionError("fixture: a CI_ENV key ci.yml does not set was not reported")
    checked += 1

    if not any(
        "drop the entry" in p
        for p in parity_problems(
            ci_env, ci_flags, local_env, local_flag, helper_text, {"CARGO_GONE": "reason"}
        )
    ):
        raise AssertionError("fixture: a stale NOT_MIRRORED entry was not reported")
    checked += 1

    # The exception is named in the helper's prose, so this fixture edits the
    # text rather than dropping lines: a comment is exactly where it lives.
    excused = next(iter(NOT_MIRRORED))
    if not any(
        "never names it" in p
        for p in parity_problems(
            ci_env,
            ci_flags,
            local_env,
            local_flag,
            helper_text.replace(excused, "SOME_OTHER_KEY"),
            NOT_MIRRORED,
        )
    ):
        raise AssertionError("fixture: an exception the helper never names was not reported")
    checked += 1

    if not any(
        "one of" in p
        for p in parity_problems(
            ci_env,
            ci_flags,
            dict(local_env, **{excused: "always"}),
            local_flag,
            helper_text,
            NOT_MIRRORED,
        )
    ):
        raise AssertionError("fixture: a key both excused and mirrored was not reported")
    checked += 1

    # A job linking with something else, and a helper flag that drifted.
    if not any(
        "link with flags no CI job uses" in p
        for p in parity_problems(
            ci_env, ci_flags, local_env, "-C link-arg=-fuse-ld=lld", helper_text
        )
    ):
        raise AssertionError("fixture: a drifted CI_MOLD_FLAG was not reported")
    checked += 1

    # The behavioural rule, over statuses the helper could have returned.
    if status_problems("ok (3 ci.yml env keys, mold)", "skipped (mold not installed)", None):
        raise AssertionError("fixture: the correct pair of statuses was reported")
    checked += 1
    for label, args in (
        ("a green status with no mold", ("ok", "ok (mold not installed)", None)),
        ("a status that does not name mold", ("ok", "skipped (a linker is missing)", None)),
        ("a fallback linker", ("ok", "skipped (mold not installed)", "-C link-arg=-fuse-ld=lld")),
        ("a non-ok mold arm", ("skipped (x)", "skipped (mold not installed)", None)),
    ):
        if not status_problems(*args):
            raise AssertionError(f"fixture: {label} was not reported")
        checked += 1

    # Shapes whose only correct answer is an error.
    for name, text, extract in (
        ("ci.yml with no workflow env", "on:\n  push:\njobs:\n  fmt:\n", workflow_env),
        ("an env block above jobs with no key", "env:\njobs:\n  fmt:\n", workflow_env),
        ("ci.yml with no jobs key", "env:\n  A: b\n", job_rustflags),
        ("ci.yml with no job RUSTFLAGS", "jobs:\n  fmt:\n    steps: []\n", job_rustflags),
        ("a helper with no CI_ENV array", "CI_MOLD_FLAG='x'\n", helper_env),
        ("an empty CI_ENV array", "CI_ENV=(\n)\n", helper_env),
        ("a helper with no mold flag", "CI_ENV=(\n  A=b\n)\n", helper_mold_flag),
        ("an empty mold flag", "CI_MOLD_FLAG=''\n", helper_mold_flag),
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
    ci_text = (root / CI_YML).read_text()
    helper_text = (root / HELPER).read_text()
    local_text = (root / CI_LOCAL).read_text()

    try:
        fixtures = run_fixtures(ci_text, helper_text)
        ci_env = workflow_env(ci_text)
        ci_flags = job_rustflags(ci_text)
        local_env = helper_env(helper_text)
        local_flag = helper_mold_flag(helper_text)
        ok_status, skipped_status, fallback, applied = drive_helper(root)
        accounting = report_accounting(root, skipped_status)
    except (ExtractionError, AssertionError) as e:
        print(f"FAIL {e}", file=sys.stderr)
        annotate(str(e), CHECKER)
        return 1

    if "--print-lists" in argv:
        for key, value in sorted(ci_env.items()):
            where = "NOT MIRRORED" if key in NOT_MIRRORED else f"CI_ENV={local_env.get(key)}"
            print(f"ci.yml env {key}={value} ({where})")
        for job, value in sorted(ci_flags.items()):
            print(f"ci.yml job {job} RUSTFLAGS={value}")
        print(f"helper mold flag: {local_flag}")
        print(f"helper status with mold: {ok_status}")
        print(f"helper status without:   {skipped_status}")
        for strict, line in sorted(accounting.items()):
            print(f"report(strict={strict}): {line}")
        return 0

    problems = parity_problems(ci_env, ci_flags, local_env, local_flag, helper_text)
    problems += status_problems(ok_status, skipped_status, fallback)

    # What apply_build_env actually put in the environment, not what the array
    # says: `export "${CI_ENV[@]}"` is one line away from exporting nothing.
    for key, value in sorted(local_env.items()):
        if applied.get(key) != value:
            problems.append(
                f"{HELPER}: apply_build_env left {key}={applied.get(key)!r} in the "
                f"environment, not {value!r} -- CI_ENV is not being exported."
            )

    # The accounting report() gives the missing-mold status. `red=0` matters as
    # much as `fail=1`: a box without mold has not shown main would be red.
    for strict, want in (
        ("0", f"{'build-env':<18} skipped|fail=0 red=0 skips=0"),
        ("1", f"{'build-env':<18} FAIL|fail=1 red=0 skips=1"),
    ):
        got = accounting.get(strict, "")
        # The status text after `skipped` is the helper's business, not this
        # assertion's; compare the verdict and the counters.
        got_head = re.sub(r"(skipped) \(.*?\)\|", r"\1|", got)
        if got_head != want:
            problems.append(
                f"{CI_LOCAL}: report() of the missing-mold status under STRICT={strict} "
                f"accounted it as {got!r}, expected {want!r}."
            )

    # The defect this gate is really about was never in a function but in the
    # job forgetting to call one.
    for wiring in ("apply_build_env ", "build_env_status ", f". {HELPER}"):
        if wiring not in local_text:
            problems.append(
                f"{CI_LOCAL} no longer contains `{wiring.strip()}` -- the build "
                "settings are mirrored in a file nothing applies."
            )

    if problems:
        for problem in problems:
            print(f"FAIL {problem}", file=sys.stderr)
            annotate(problem, HELPER)
        return 1

    mirrored = ", ".join(sorted(local_env))
    excused = ", ".join(sorted(NOT_MIRRORED))
    print(
        f"ok   {len(local_env)} ci.yml env key(s) mirrored ({mirrored}), "
        f"{len(NOT_MIRRORED)} excused ({excused}), "
        f"{len(ci_flags)} job(s) linking with `{local_flag}`; {fixtures} fixtures"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
