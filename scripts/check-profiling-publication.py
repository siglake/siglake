#!/usr/bin/env python3
"""Hold the immutable, engine-only GHCR profiling publication contract.

The executable fixtures drive source validation against a temporary Git
checkout and the registry preflight against a stand-in curl. Static checks keep
the manual workflow's permissions, ordering, tags, labels and build settings
from drifting. No registry, image build or third-party Python package is used.
"""

from __future__ import annotations

import os
import pathlib
import re
import subprocess
import sys
import tempfile


WORKFLOW = pathlib.PurePath(".github/workflows/profiling-image-ghcr.yml")
PREPARE = pathlib.PurePath("scripts/prepare-profiling-publication.sh")
AVAILABLE = pathlib.PurePath("scripts/check-profiling-tag-available.sh")
SHA = re.compile(r"^[0-9a-f]{40}$")
JOB = re.compile(r"^  (?P<name>[a-z][a-z0-9_-]*):\s*$", re.MULTILINE)
RUN = re.compile(r"^(?P<indent>\s*)(?:- )?run:(?P<value>.*)$")
BLOCK = re.compile(r"^[|>][+-]?\d*$")


class CheckError(Exception):
    """The checker or a fixture could not establish an answer."""


def run(
    args: list[str],
    *,
    cwd: pathlib.Path,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def git(cwd: pathlib.Path, *args: str) -> str:
    result = run(["git", *args], cwd=cwd)
    if result.returncode:
        raise CheckError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result.stdout.strip()


def run_scalars(text: str) -> list[str]:
    """Extract each workflow run body, failing closed on an empty result."""
    lines = text.splitlines()
    values: list[str] = []
    for i, line in enumerate(lines):
        match = RUN.match(line)
        if not match:
            continue
        value = match.group("value").strip()
        if not BLOCK.match(value):
            values.append(value)
            continue
        indent = len(match.group("indent"))
        body: list[str] = []
        for following in lines[i + 1 :]:
            if following.strip() and len(following) - len(following.lstrip()) <= indent:
                break
            body.append(following)
        values.append("\n".join(body))
    if not values:
        raise CheckError(f"{WORKFLOW} has no run steps")
    return values


def job_blocks(text: str) -> dict[str, str]:
    jobs_marker = re.search(r"^jobs:\s*$", text, re.MULTILINE)
    if not jobs_marker:
        raise CheckError(f"{WORKFLOW} has no jobs mapping")
    jobs_text = text[jobs_marker.end() :]
    matches = list(JOB.finditer(jobs_text))
    if not matches:
        raise CheckError(f"{WORKFLOW} has no jobs")
    return {
        match.group("name"): jobs_text[
            match.start() : matches[i + 1].start() if i + 1 < len(matches) else len(jobs_text)
        ]
        for i, match in enumerate(matches)
    }


def permission_map(block: str) -> dict[str, str]:
    lines = block.splitlines()
    for i, line in enumerate(lines):
        if line == "    permissions:":
            answer: dict[str, str] = {}
            for following in lines[i + 1 :]:
                match = re.match(r"^      ([a-z-]+):\s*(\S+)\s*$", following)
                if not match:
                    break
                answer[match.group(1)] = match.group(2)
            return answer
    raise CheckError("publish job has no explicit permissions map")


def ordered(block: str, needles: list[str]) -> bool:
    position = -1
    for needle in needles:
        position = block.find(needle, position + 1)
        if position < 0:
            return False
    return True


def workflow_problems(text: str) -> list[str]:
    problems: list[str] = []
    try:
        blocks = job_blocks(text)
        runs = run_scalars(text)
    except CheckError as error:
        return [str(error)]

    on_match = re.search(r"^on:\s*$\n(?P<body>.*?)(?=^concurrency:)", text, re.MULTILINE | re.DOTALL)
    if not on_match or "  workflow_dispatch:" not in on_match.group("body") or re.search(
        r"^  push:", on_match.group("body"), re.MULTILINE
    ):
        problems.append("workflow trigger must be manual-only")
    if not re.search(
        r"^concurrency:\s*$\n  group: profiling-image-ghcr\n  cancel-in-progress: false$",
        text,
        re.MULTILINE,
    ):
        problems.append("profiling publication must serialize without cancelling in-progress work")
    if any("inputs." in body for body in runs):
        problems.append("an untrusted workflow input is interpolated into a shell run body")
    for fact in (
        "      build_source:\n",
        "        required: true\n",
        "        type: string\n",
        "  BUILD_SOURCE: ${{ inputs.build_source }}\n",
    ):
        if fact not in text:
            problems.append(f"workflow lost its required full-source input contract: {fact.strip()}")

    if set(blocks) != {"check", "publish"}:
        problems.append("workflow must contain only check and publish jobs")
        return problems
    if permission_map(blocks["check"]) != {"contents": "read"}:
        problems.append("check job permissions must be exactly contents: read")
    if permission_map(blocks["publish"]) != {"contents": "read", "packages": "write"}:
        problems.append("publish job permissions must be exactly contents: read and packages: write")
    if "    needs: check" not in blocks["publish"]:
        problems.append("publication does not depend on the full profiling feature check")

    source_order = [
        "path: workflow",
        "prepare-profiling-publication.sh validate",
        "ref: ${{ env.BUILD_SOURCE }}",
        "path: source",
        "prepare-profiling-publication.sh resolve",
    ]
    for name in ("check", "publish"):
        if not ordered(blocks[name], source_order):
            problems.append(f"{name} job does not validate, check out and verify the source in order")
        if blocks[name].count("ref: ${{ github.sha }}") != 1 or blocks[name].count(
            "persist-credentials: false"
        ) != 2:
            problems.append(f"{name} job does not pin its workflow checkout or disable checkout credentials")
    for fact in (
        "dtolnay/rust-toolchain@1.95.0",
        "working-directory: source",
        "run: scripts/check-profiling-feature.sh",
    ):
        if fact not in blocks["check"]:
            problems.append(f"check job lost required feature gate setting: {fact}")
    if not ordered(
        blocks["publish"],
        [
            "check-profiling-tag-available.sh",
            "docker/login-action@v3",
            "docker/build-push-action@v7",
        ],
    ):
        problems.append("tag availability must be established before login and publication")

    required = (
        "runs-on: ubuntu-24.04",
        "timeout-minutes: 360",
        "push: true",
        "SIGLAKE_GIT_SHA=${{ env.REVISION }}",
        "PROFILING=1",
        "BUILD_CACHE_ID=profiling-ghcr-${{ env.BUILD_SOURCE_SHA }}",
        "tags: ghcr.io/siglake/siglake:${{ env.PROFILE_TAG }}",
        "org.opencontainers.image.revision=${{ env.BUILD_SOURCE_SHA }}",
        "dev.siglake.image.variant=profiling",
        "dev.siglake.workflow.revision=${{ github.sha }}",
        "cache-from: type=gha,scope=siglake-profiling-ghcr",
        "cache-to: type=gha,scope=siglake-profiling-ghcr,mode=max",
        "Free disk on the hosted runner",
        "CARGO_BUILD_JOBS=2",
        "context: source",
        "file: source/deploy/Dockerfile",
    )
    for fact in required:
        if fact not in blocks["publish"]:
            problems.append(f"publish job lost required setting: {fact}")
    if "siglake-operator" in text or re.search(r"tags:.*(?:latest|prof-\$\{\{ env\.REVISION)", text):
        problems.append("workflow names an operator or mutable/non-full profiling tag")

    allowed_uses = {
        "actions/checkout@v4",
        "dtolnay/rust-toolchain@1.95.0",
        "Swatinem/rust-cache@v2",
        "docker/setup-buildx-action@v3",
        "docker/login-action@v3",
        "docker/build-push-action@v7",
    }
    uses = set(re.findall(r"^\s+- (?:name:.*\n\s+)?uses:\s*(\S+)\s*$", text, re.MULTILINE))
    if uses != allowed_uses:
        problems.append(
            "workflow action providers differ from the reviewed set: "
            f"missing={sorted(allowed_uses - uses)}, extra={sorted(uses - allowed_uses)}"
        )
    return problems


def script_problems(prepare: str, available: str) -> list[str]:
    problems: list[str] = []
    for fact in ("^[0-9a-f]{40}$", "rev-parse --verify 'HEAD^{commit}'", "PROFILE_TAG=prof-%s"):
        if fact not in prepare:
            problems.append(f"source resolver lost required check: {fact}")
    for fact in (
        "https://ghcr.io/token?scope=repository:siglake/siglake:pull",
        "https://ghcr.io/v2/siglake/siglake/manifests/$tag",
        "--max-time 15",
        "--request HEAD",
        "Docker-Content-Digest:",
    ):
        if fact not in available:
            problems.append(f"registry preflight lost required check: {fact}")
    return problems


def prepare_fixtures(root: pathlib.Path, prepare: pathlib.Path) -> int:
    checked = 0
    with tempfile.TemporaryDirectory() as raw:
        temporary = pathlib.Path(raw)
        repo = temporary / "repo"
        repo.mkdir()
        git(repo, "init", "-q")
        git(repo, "config", "user.name", "fixture")
        git(repo, "config", "user.email", "fixture@example.invalid")
        (repo / "source").write_text("fixture\n")
        git(repo, "add", "source")
        git(repo, "commit", "-q", "-m", "fixture")
        revision = git(repo, "rev-parse", "HEAD")
        if not SHA.fullmatch(revision):
            raise CheckError("temporary repository did not produce a full SHA-1 commit id")

        sentinel = temporary / "executed"
        hostile = (
            "main",
            revision[:12],
            "a" * 39,
            f"$(touch {sentinel})",
            f"`touch {sentinel}`",
            "a" * 20 + "\n" + "b" * 20,
        )
        for value in hostile:
            github_env = temporary / "github-env"
            github_env.write_text("")
            env = os.environ | {"BUILD_SOURCE": value, "GITHUB_ENV": str(github_env)}
            result = run([str(prepare), "validate"], cwd=root, env=env)
            if result.returncode == 0 or github_env.read_text() or sentinel.exists():
                raise CheckError(f"invalid or adversarial source passed validation: {value!r}")
            checked += 1

        github_env = temporary / "github-env"
        github_env.write_text("")
        wrong = "0" * 40 if revision != "0" * 40 else "1" * 40
        env = os.environ | {
            "BUILD_SOURCE": wrong,
            "SOURCE_DIR": str(repo),
            "GITHUB_ENV": str(github_env),
        }
        result = run([str(prepare), "resolve"], cwd=root, env=env)
        if result.returncode == 0 or github_env.read_text():
            raise CheckError("a full SHA different from the checkout passed resolution")
        checked += 1

        env["BUILD_SOURCE"] = revision
        result = run([str(prepare), "resolve"], cwd=root, env=env)
        expected = (
            f"BUILD_SOURCE_SHA={revision}\n"
            f"REVISION={git(repo, 'rev-parse', '--short=12', 'HEAD')}\n"
            f"PROFILE_TAG=prof-{revision}\n"
        )
        if result.returncode or github_env.read_text() != expected or expected.count("prof-") != 1:
            raise CheckError("matching full SHA did not write exactly one immutable profiling tag")
        checked += 1
    return checked


def registry_fixtures(root: pathlib.Path, available: pathlib.Path) -> int:
    checked = 0
    with tempfile.TemporaryDirectory() as raw:
        temporary = pathlib.Path(raw)
        shim = temporary / "curl"
        shim.write_text(
            """#!/usr/bin/env bash
set -eu
output=/dev/null
headers=
url=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output) output=$2; shift 2 ;;
    --dump-header) headers=$2; shift 2 ;;
    --write-out|--header|--request|--max-time) shift 2 ;;
    --silent|--show-error|--location) shift ;;
    *) url=$1; shift ;;
  esac
done
case "$url" in
  *'/token?'*) printf '{"token":"fixture-token"}' > "$output"; printf 200 ;;
  *'/manifests/'*)
    if [ "${MANIFEST_RESULT}" = timeout ]; then exit 28; fi
    if [ "${MANIFEST_RESULT}" = 200 ]; then
      printf 'Docker-Content-Digest: sha256:fixture\\r\\n' > "$headers"
    else
      : > "$headers"
    fi
    printf '%s' "$MANIFEST_RESULT"
    ;;
  *) exit 2 ;;
esac
"""
        )
        shim.chmod(0o755)
        base = os.environ | {
            "PATH": f"{temporary}:{os.environ['PATH']}",
            "RUNNER_TEMP": str(temporary),
            "BUILD_SOURCE_SHA": "a" * 40,
        }
        for result_name, should_pass, evidence in (
            ("404", True, "is absent"),
            ("200", False, "sha256:fixture"),
            ("401", False, "unauthorized (401)"),
            ("429", False, "rate limited (429)"),
            ("timeout", False, "undetermined"),
        ):
            result = run(
                [str(available)],
                cwd=root,
                env=base | {"MANIFEST_RESULT": result_name},
            )
            combined = result.stdout + result.stderr
            if (result.returncode == 0) != should_pass or evidence not in combined:
                raise CheckError(f"registry fixture {result_name} was misclassified: {combined.strip()}")
            checked += 1
    return checked


def static_fixtures(workflow: str) -> int:
    if workflow_problems(workflow):
        raise CheckError("known-good workflow failed before static mutations")
    mutations = (
        workflow.replace("cancel-in-progress: false", "cancel-in-progress: true", 1),
        workflow.replace("      packages: write\n", "", 1),
        workflow.replace(
            "run: workflow/scripts/prepare-profiling-publication.sh validate",
            'run: echo "${{ inputs.build_source }}"',
            1,
        ),
        workflow.replace("tags: ghcr.io/siglake/siglake:${{ env.PROFILE_TAG }}", "tags: ghcr.io/siglake/siglake:latest", 1),
        workflow.replace("    needs: check\n", "", 1),
        workflow.replace("run: workflow/scripts/check-profiling-tag-available.sh", "run: true", 1),
    )
    for index, mutation in enumerate(mutations, 1):
        if not workflow_problems(mutation):
            raise CheckError(f"static mutation {index} escaped the workflow gate")
    return len(mutations)


def main() -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    prepare = root / PREPARE
    available = root / AVAILABLE
    try:
        workflow = (root / WORKFLOW).read_text()
        prepare_text = prepare.read_text()
        available_text = available.read_text()
        dynamic = prepare_fixtures(root, prepare) + registry_fixtures(root, available)
        static = static_fixtures(workflow)
        problems = workflow_problems(workflow) + script_problems(prepare_text, available_text)
    except (CheckError, OSError, subprocess.SubprocessError) as error:
        print(f"FAIL {error}", file=sys.stderr)
        return 1
    if problems:
        for problem in problems:
            print(f"FAIL {problem}", file=sys.stderr)
        return 1
    print(
        f"ok   pinned profiling publication: {dynamic} executable fixtures, "
        f"{static} static mutations"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
