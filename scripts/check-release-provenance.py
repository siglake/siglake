#!/usr/bin/env python3
"""Both published images are built with the commit the release checkout selected.

`crates/siglake-core/build.rs` stamps `SIGLAKE_GIT_SHA` into `--version` and
`siglake_build_info`, falling back to `git rev-parse` and then to the literal
`unknown`. Neither `deploy/Dockerfile` nor `deploy/Dockerfile.operator` copies
Git metadata into the builder, so inside an image the fallback cannot work: the
build argument is the only source. CI and the profiling workflow passed it;
`.github/workflows/publish.yml` -- the workflow that produces the images users
run -- did not, so every released binary said `unknown` and nothing told a
release apart from a later fix built over it.

The value has to be the commit the release checkout resolved to, and that is not
`github.sha`. A tag push makes the two the same, which is exactly why a workflow
that reads `github.sha` looks correct until someone dispatches the workflow by
hand: a manual dispatch runs the workflow from its own revision -- whatever
`main` is that day -- and publishes the revision named by its `tag` input. The
two paths disagree only on the path nobody rehearses.

So this gate executes the workflow's revision step rather than matching its
text, against a synthetic repository built here: one commit tagged `v0.1.0`, one
later commit on the branch, and a detached checkout of the tag, which is the
state `actions/checkout@v4` with a `ref:` leaves behind. The step must produce
the tagged commit under both triggers -- the tag push, where `github.sha` is the
release commit, and a dispatch where `github.sha` is the later one. Four more
things are read off the workflow statically: both Dockerfiles that declare the
argument are built, each build gets the argument, the value traces back to a
single `$GITHUB_ENV` assignment, and that assignment runs after the checkout.

Stdlib only and no registry: this runs in the `shell` job, which installs
nothing, and in the local gate's shell block. Tag normalization is a separate
question, answered by scripts/check-release-tags.py.

A parser whose failure mode is a green run is worse than no parser, so fixtures
run first on every invocation, including the workflow as it shipped before this
gate -- no build arguments at all -- and the plausible wrong answer, `github.sha`,
which must be reported for the dispatch path and only for it.
"""

from __future__ import annotations

import os
import pathlib
import re
import subprocess
import sys
import tempfile

PUBLISH_YML = pathlib.PurePath(".github/workflows/publish.yml")
DOCKERFILES = (
    pathlib.PurePath("deploy/Dockerfile"),
    pathlib.PurePath("deploy/Dockerfile.operator"),
)
BUILD_ARG = "SIGLAKE_GIT_SHA"

# The release tag the synthetic checkout carries. Any tag works; this gate never
# compares it with the workspace version.
FIXTURE_TAG = "v0.1.0"

# `steps:` and the list items under it.
STEPS_KEY = re.compile(r"^(?P<indent>\s*)steps:\s*$")
LIST_ITEM = re.compile(r"^(?P<indent>\s*)- ")

# `run: ...` / `build-args: ...` as a step key or as the first key of a step.
RUN_KEY = re.compile(r"^(?P<indent>\s*)(?:- )?run:(?P<value>.*)$")
BUILD_ARGS_KEY = re.compile(r"^(?P<indent>\s*)(?:- )?build-args:(?P<value>.*)$")
BLOCK_SCALAR = re.compile(r"^[|>][+-]?\d*$")
FILE_KEY = re.compile(r"^\s*file:\s*(?P<value>\S+)\s*$", re.MULTILINE)

# A GitHub Actions expression. Ours never nest a `}`.
GH_EXPR = re.compile(r"\$\{\{(?P<expr>[^}]*)\}\}")
# `${{ env.REVISION }}` and nothing else around it.
ENV_EXPR = re.compile(r"^\$\{\{\s*env\.(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\}\}$")

ARG_DECLARATION = re.compile(rf"^ARG\s+{BUILD_ARG}\b", re.MULTILINE)
HEX = re.compile(r"^[0-9a-f]+$")

# Short enough to be ambiguous is not provenance.
MIN_REVISION_CHARS = 7


class ExtractionError(Exception):
    """The file did not have the shape this gate parses.

    Never an empty result: a gate that compared nothing to nothing has not
    passed, and every shape here is one whose loss someone must see.
    """


# --- extraction --------------------------------------------------------------


def step_blocks(text: str) -> list[str]:
    """Every step of every job, each as its own block of lines, in file order."""
    lines = text.splitlines()
    blocks: list[str] = []
    i = 0
    while i < len(lines):
        m = STEPS_KEY.match(lines[i])
        i += 1
        if not m:
            continue
        key_indent = len(m.group("indent"))
        region: list[str] = []
        while i < len(lines):
            line = lines[i]
            if line.strip() and len(line) - len(line.lstrip()) <= key_indent:
                break
            region.append(line)
            i += 1
        item_indent: int | None = None
        current: list[str] = []
        for line in region:
            m2 = LIST_ITEM.match(line)
            if m2 and (item_indent is None or len(m2.group("indent")) == item_indent):
                item_indent = len(m2.group("indent"))
                if current:
                    blocks.append("\n".join(current))
                current = []
            current.append(line)
        if current:
            blocks.append("\n".join(current))
    if not blocks:
        raise ExtractionError(f"{PUBLISH_YML} has no steps")
    return blocks


def scalar(block: str, key: re.Pattern[str]) -> str | None:
    """A step's value for `key`, inline or block scalar; None when absent."""
    lines = block.splitlines()
    for i, line in enumerate(lines):
        m = key.match(line)
        if not m:
            continue
        value = m.group("value").strip()
        if not BLOCK_SCALAR.match(value):
            return value
        key_indent = len(m.group("indent"))
        body: list[str] = []
        for b in lines[i + 1 :]:
            if b.strip() and len(b) - len(b.lstrip()) <= key_indent:
                break
            body.append(b)
        return "\n".join(body)
    return None


def build_args(block: str) -> dict[str, str] | None:
    """A build step's `build-args`; None when the step passes none at all."""
    body = scalar(block, BUILD_ARGS_KEY)
    if body is None:
        return None
    args: dict[str, str] = {}
    for entry in (line.strip() for line in body.splitlines()):
        if not entry:
            continue
        if "=" not in entry:
            raise ExtractionError(
                f"{PUBLISH_YML}: build argument `{entry}` is not `NAME=value`"
            )
        name, value = entry.split("=", 1)
        args[name.strip()] = value.strip()
    return args


def image_builds(text: str) -> list[tuple[str, dict[str, str] | None]]:
    """(Dockerfile, build arguments) for every image the workflow builds."""
    builds = []
    for block in step_blocks(text):
        if "docker/build-push-action" not in block:
            continue
        m = FILE_KEY.search(block)
        if not m:
            raise ExtractionError(
                f"{PUBLISH_YML} builds an image with no `file:` -- this gate "
                "identifies a build by its Dockerfile"
            )
        builds.append((m.group("value"), build_args(block)))
    if not builds:
        raise ExtractionError(f"{PUBLISH_YML} builds no image")
    return builds


def assigns(block: str, name: str) -> bool:
    """This step writes `name=` into `$GITHUB_ENV`."""
    body = scalar(block, RUN_KEY)
    return body is not None and f"{name}=" in body and "GITHUB_ENV" in body


def assignment_step(text: str, name: str) -> str:
    """The one `run:` body that assigns `name` in `$GITHUB_ENV`."""
    bodies = [scalar(b, RUN_KEY) for b in step_blocks(text) if assigns(b, name)]
    if len(bodies) != 1:
        raise ExtractionError(
            f"{PUBLISH_YML} has {len(bodies)} `run:` steps that assign `{name}` "
            "to $GITHUB_ENV; this gate evaluates exactly one"
        )
    return bodies[0] or ""


def declared_build_arg(text: str) -> bool:
    """This Dockerfile takes the provenance argument."""
    return bool(ARG_DECLARATION.search(text))


# --- the synthetic release checkout -------------------------------------------


def git(repo: pathlib.Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
        env=git_env(repo),
    )
    return proc.stdout.strip()


def git_env(repo: pathlib.Path) -> dict[str, str]:
    """A git environment that reads no user or system configuration."""
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(repo),
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_AUTHOR_NAME": "gate",
        "GIT_AUTHOR_EMAIL": "gate@example.invalid",
        "GIT_COMMITTER_NAME": "gate",
        "GIT_COMMITTER_EMAIL": "gate@example.invalid",
    }


def release_checkout(root: pathlib.Path) -> tuple[pathlib.Path, str, str]:
    """A repository in the state the workflow's checkout leaves behind.

    One commit tagged `v0.1.0`, one later commit on the branch, and HEAD
    detached at the tag. Returns (repo, release commit, later commit): the
    second is what `github.sha` is for a dispatch run, and the value the images
    must NOT be stamped with.
    """
    repo = root / "release"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    (repo / "VERSION").write_text("0.1.0\n")
    git(repo, "add", "VERSION")
    git(repo, "commit", "-qm", "release")
    git(repo, "tag", FIXTURE_TAG)
    release = git(repo, "rev-parse", "HEAD")
    (repo / "VERSION").write_text("0.1.1-dev\n")
    git(repo, "commit", "-qam", "work after the release")
    later = git(repo, "rev-parse", "HEAD")
    git(repo, "checkout", "-q", "--detach", FIXTURE_TAG)
    return repo, release, later


def evaluate_revision(step: str, name: str, repo: pathlib.Path, github_sha: str) -> str:
    """Run the workflow's revision step in the checkout; return what it assigned."""

    def substitute(m: re.Match[str]) -> str:
        expr = m.group("expr")
        if "github.sha" in expr:
            return github_sha
        if "ref_name" in expr or "inputs.tag" in expr:
            return FIXTURE_TAG
        raise ExtractionError(
            f"{PUBLISH_YML}'s `{name}` step reads `{expr.strip()}`, which this "
            "gate cannot supply -- it supplies the event's commit and tag only"
        )

    script = "set -euo pipefail\n" + GH_EXPR.sub(substitute, step)
    with tempfile.TemporaryDirectory() as d:
        env_file = pathlib.Path(d) / "github_env"
        env_file.write_text("")
        env = git_env(repo)
        env["GITHUB_ENV"] = str(env_file)
        proc = subprocess.run(
            ["bash", "-c", script],
            cwd=repo,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise ExtractionError(
                f"{PUBLISH_YML}'s `{name}` step exited {proc.returncode} under "
                f"bash: {proc.stderr.strip() or '(no stderr)'}"
            )
        written = [
            line.split("=", 1)[1]
            for line in env_file.read_text().splitlines()
            if line.startswith(f"{name}=")
        ]
    if len(written) != 1:
        raise ExtractionError(
            f"{PUBLISH_YML}'s `{name}` step wrote {len(written)} `{name}` values "
            "to $GITHUB_ENV; this gate evaluates exactly one"
        )
    return written[0]


def is_revision_of(value: str, commit: str) -> bool:
    """`value` names `commit`: its full hash or an unambiguously long prefix."""
    return (
        len(value) >= MIN_REVISION_CHARS
        and bool(HEX.match(value))
        and commit.startswith(value)
    )


# --- checks -------------------------------------------------------------------


def coverage_problems(text: str, declaring: list[str]) -> tuple[list[str], dict[str, list[str]]]:
    """Every image that takes the argument is built, and gets it.

    Returns the problems and, for the builds that do get it, the argument value
    mapped to the Dockerfiles passing it -- one value is evaluated once however
    many images share it.
    """
    problems: list[str] = []
    values: dict[str, list[str]] = {}
    builds = image_builds(text)
    built = [dockerfile for dockerfile, _ in builds]
    for dockerfile in declaring:
        if dockerfile not in built:
            problems.append(
                f"{PUBLISH_YML}: `{dockerfile}` declares `ARG {BUILD_ARG}` but no "
                "step here builds it -- the released image would come from "
                "somewhere this gate cannot check"
            )
    for dockerfile, args in builds:
        if dockerfile not in declaring:
            continue
        value = (args or {}).get(BUILD_ARG)
        if not value:
            problems.append(
                f"{PUBLISH_YML}: the image built from `{dockerfile}` gets no "
                f"{BUILD_ARG} build argument, and the build copies no Git "
                "metadata -- its binaries would report revision `unknown`"
            )
            continue
        values.setdefault(value, []).append(dockerfile)
    return problems, values


def order_problems(text: str, name: str) -> list[str]:
    """The revision is resolved from the checkout, so it comes after it."""
    blocks = step_blocks(text)
    checkouts = [i for i, b in enumerate(blocks) if "actions/checkout" in b]
    if not checkouts:
        raise ExtractionError(f"{PUBLISH_YML} checks out nothing")
    assignments = [i for i, b in enumerate(blocks) if assigns(b, name)]
    if not assignments:
        raise ExtractionError(f"{PUBLISH_YML} has no step assigning `{name}`")
    if min(assignments) < max(checkouts):
        return [
            f"{PUBLISH_YML}: `{name}` is resolved before the release checkout "
            "completes, so it cannot be the commit that gets published"
        ]
    return []


def value_problems(text: str, value: str, images: list[str]) -> list[str]:
    """The value passed to the build is the checked-out release commit.

    Both triggers are evaluated. They differ only in `github.sha`: it is the
    release commit for a tag push and the workflow's own revision for a manual
    dispatch, which is the case a text match cannot separate.
    """
    named = ", ".join(f"`{image}`" for image in sorted(images))
    m = ENV_EXPR.match(value)
    if not m:
        return [
            f"{PUBLISH_YML}: {named} passes {BUILD_ARG}={value}; this gate "
            "resolves `${{ env.NAME }}` only, so the published provenance "
            "cannot be evaluated offline -- assign it in a step and pass the env"
        ]
    name = m.group("name")
    problems = order_problems(text, name)
    step = assignment_step(text, name)
    with tempfile.TemporaryDirectory() as d:
        repo, release, later = release_checkout(pathlib.Path(d))
        triggers = (
            (f"a `{FIXTURE_TAG}` tag push", release),
            ("a manual dispatch run from a later revision", later),
        )
        for trigger, github_sha in triggers:
            got = evaluate_revision(step, name, repo, github_sha)
            if is_revision_of(got, release):
                continue
            problems.append(
                f"{PUBLISH_YML}: under {trigger}, {named} would be built with "
                f"{BUILD_ARG}=`{got}`, but the checked-out release commit is "
                f"`{release}` -- resolve the revision from the checkout, not "
                "from github.sha or the tag"
            )
    return problems


def provenance_problems(text: str, declaring: list[str]) -> list[str]:
    problems, values = coverage_problems(text, declaring)
    for value, images in values.items():
        problems += value_problems(text, value, images)
    return problems


# --- fixtures -----------------------------------------------------------------

DECLARING = [str(path) for path in DOCKERFILES]

CHECKOUT = """
jobs:
  publish:
    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ github.event.inputs.tag || github.ref }}
"""

REVISION_STEP = """      - name: Resolve release revision
        run: echo "REVISION=$(git rev-parse --short HEAD)" >> "$GITHUB_ENV"
"""

BUILDS = """      - name: Build and push siglake
        uses: docker/build-push-action@v7
        with:
          context: .
          file: deploy/Dockerfile
          build-args: SIGLAKE_GIT_SHA=${{ env.REVISION }}
      - name: Build and push siglake-operator
        uses: docker/build-push-action@v7
        with:
          context: .
          file: deploy/Dockerfile.operator
          build-args: |
            SIGLAKE_GIT_SHA=${{ env.REVISION }}
"""

WORKFLOW = CHECKOUT + REVISION_STEP + BUILDS

# The workflow as it shipped before this gate: both Dockerfiles already took the
# argument, and neither build passed one.
PRE_FIX = CHECKOUT + re.sub(r" *build-args:(?: \|\n)?[^\n]*\n(?: +SIGLAKE[^\n]*\n)?", "", BUILDS)


def run_fixtures() -> int:
    """Prove every check can go red. Returns the number of fixtures checked."""
    checked = 0

    if provenance_problems(WORKFLOW, DECLARING):
        raise AssertionError("fixture: a workflow that stamps the release commit was reported")
    checked += 1

    if len(provenance_problems(PRE_FIX, DECLARING)) != 2:
        raise AssertionError("fixture: the pre-fix workflow's two unstamped images were not both reported")
    checked += 1

    one_image = WORKFLOW.replace(
        "          build-args: |\n            SIGLAKE_GIT_SHA=${{ env.REVISION }}\n", ""
    )
    problems = provenance_problems(one_image, DECLARING)
    if len(problems) != 1 or "Dockerfile.operator" not in problems[0]:
        raise AssertionError("fixture: an operator image built without the argument passed")
    checked += 1

    # The plausible wrong answer. A tag push cannot tell it from the right one;
    # a dispatch from a later revision can, and must.
    from_sha = WORKFLOW.replace(
        '$(git rev-parse --short HEAD)', "${{ github.sha }}"
    )
    problems = provenance_problems(from_sha, DECLARING)
    if len(problems) != 1 or "dispatch" not in problems[0]:
        raise AssertionError(
            "fixture: github.sha was not reported exactly once, for the dispatch path"
        )
    checked += 1

    # The other plausible wrong answer: the version string the image is named
    # after, which says nothing about what was built.
    from_tag = WORKFLOW.replace(
        '$(git rev-parse --short HEAD)', "${{ github.event.inputs.tag || github.ref_name }}"
    )
    if len(provenance_problems(from_tag, DECLARING)) != 2:
        raise AssertionError("fixture: the release tag as provenance passed a trigger")
    checked += 1

    # A revision short enough to be ambiguous is not provenance.
    truncated = WORKFLOW.replace(
        "$(git rev-parse --short HEAD)", "$(git rev-parse HEAD | cut -c1-4)"
    )
    if len(provenance_problems(truncated, DECLARING)) != 2:
        raise AssertionError("fixture: a four-character revision passed")
    checked += 1

    full = WORKFLOW.replace("rev-parse --short HEAD", "rev-parse HEAD")
    if provenance_problems(full, DECLARING):
        raise AssertionError("fixture: a full-length revision was reported")
    checked += 1

    before_checkout = CHECKOUT.replace(
        "      - uses: actions/checkout@v4", REVISION_STEP + "      - uses: actions/checkout@v4"
    ) + BUILDS
    problems = provenance_problems(before_checkout, DECLARING)
    if not any("before the release checkout" in p for p in problems):
        raise AssertionError("fixture: a revision resolved before the checkout passed")
    checked += 1

    direct = WORKFLOW.replace("${{ env.REVISION }}", "${{ github.sha }}")
    if len(provenance_problems(direct, DECLARING)) != 1:
        raise AssertionError("fixture: a build argument this gate cannot trace passed")
    checked += 1

    if provenance_problems(WORKFLOW, DECLARING + ["deploy/Dockerfile.nothing"]) == []:
        raise AssertionError("fixture: a Dockerfile taking the argument but never built passed")
    checked += 1

    if not declared_build_arg(f"FROM rust\nARG {BUILD_ARG}\n"):
        raise AssertionError("fixture: a declared build argument was not seen")
    checked += 1
    if declared_build_arg(f"FROM rust\nENV {BUILD_ARG}=x\n"):
        raise AssertionError("fixture: an env assignment counted as a declaration")
    checked += 1

    for name, text in (
        ("a workflow with no steps", "jobs:\n  publish:\n    name: p\n"),
        ("a workflow that builds no image", CHECKOUT),
        (
            "a build step with no Dockerfile",
            CHECKOUT + "      - uses: docker/build-push-action@v7\n        with:\n          context: .\n",
        ),
    ):
        try:
            image_builds(text)
        except ExtractionError:
            checked += 1
        else:
            raise AssertionError(f"fixture: {name} extracted without an error")

    two_steps = CHECKOUT + REVISION_STEP + REVISION_STEP + BUILDS
    for name, text in (
        ("a workflow with two revision steps", two_steps),
        ("a workflow with no revision step", CHECKOUT + BUILDS),
    ):
        try:
            assignment_step(text, "REVISION")
        except ExtractionError:
            checked += 1
        else:
            raise AssertionError(f"fixture: {name} extracted without an error")

    with tempfile.TemporaryDirectory() as d:
        repo, release, later = release_checkout(pathlib.Path(d))
        if release == later:
            raise AssertionError("fixture: the synthetic release and later commits are the same")
        if git(repo, "rev-parse", "HEAD") != release:
            raise AssertionError("fixture: the synthetic checkout is not detached at the tag")
        checked += 1
        if is_revision_of("0.1.0", release) or is_revision_of(later, release):
            raise AssertionError("fixture: a non-revision or the wrong commit was accepted")
        checked += 1
        try:
            evaluate_revision(
                'echo "R=${{ env.TAG }}" >> "$GITHUB_ENV"', "R", repo, release
            )
        except ExtractionError:
            checked += 1
        else:
            raise AssertionError("fixture: an expression this gate cannot supply was evaluated anyway")
        try:
            evaluate_revision('echo "R=a" >> "$GITHUB_ENV"; false', "R", repo, release)
        except ExtractionError:
            checked += 1
        else:
            raise AssertionError("fixture: a failing revision step was read as an answer")

    return checked


# --- driver -------------------------------------------------------------------


def annotate(message: str, path: pathlib.PurePath = PUBLISH_YML) -> None:
    """Attach a failure to the file that needs attention under GitHub Actions."""
    if os.environ.get("GITHUB_ACTIONS"):
        print(f"::error file={path}::{message}")


def main(argv: list[str]) -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    problems: list[str] = []
    try:
        fixtures = run_fixtures()
        declaring = [
            str(path)
            for path in DOCKERFILES
            if declared_build_arg((root / path).read_text())
        ]
        if not declaring:
            raise ExtractionError(
                f"no Dockerfile in {', '.join(str(p) for p in DOCKERFILES)} declares "
                f"`ARG {BUILD_ARG}` -- the build stamps its revision some other way now"
            )
        publish = (root / PUBLISH_YML).read_text()
        problems += provenance_problems(publish, declaring)
    except (ExtractionError, AssertionError, OSError, subprocess.SubprocessError) as e:
        print(f"FAIL {e}", file=sys.stderr)
        annotate(str(e))
        return 1

    if problems:
        for problem in problems:
            print(f"FAIL {problem}", file=sys.stderr)
            annotate(problem)
        print(
            f"\n{len(problems)} published image(s) without the release commit they "
            "were built from.",
            file=sys.stderr,
        )
        return 1

    if "--print-facts" in argv:
        for dockerfile, args in image_builds(publish):
            print(f"{dockerfile}: {BUILD_ARG}={(args or {}).get(BUILD_ARG, '(none)')}")
    print(
        f"ok   {len(declaring)} published images carry the checked-out release "
        f"commit under both triggers; {fixtures} fixtures"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
