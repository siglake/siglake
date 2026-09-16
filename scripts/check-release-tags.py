#!/usr/bin/env python3
"""The published image tag, the chart defaults and the documented install agree.

`.github/workflows/publish.yml` derives the image tag from the git tag, and the
git tag is `v0.1.0`. Both charts ask for their numeric `appVersion` when
`image.tag` is left empty, and the chart README documents `--set
image.tag=0.1.0`. Nothing compared the two sides, so `helm install` from the
published chart would have pulled `ghcr.io/example-invalid/siglake:0.1.0` against a
registry holding only `v0.1.0` -- an ImagePullBackOff on the first release. The
kind scripts always pass `image.tag` explicitly, so the default was never
exercised against a registry.

Four questions, all answered from tracked files with no registry and no helm:

1. The workflow's tag-resolution step, evaluated over `v<workspace version>`,
   produces the workspace version. The step's shell body is EXECUTED, not
   pattern-matched: `appVersion == workspace version` is true today and would
   have passed the broken workflow unchanged, so a guard that only compared
   version strings would have caught nothing.
2. Every image the workflow pushes is tagged from that resolved value (or
   `latest`), so the normalization is the tag that reaches the registry, and the
   checkout still uses the original `v` ref -- the release commit is the tag's.
3. Both charts' default rendered image tag -- `image.tag` when set, otherwise
   `appVersion` through the chart's image helper -- is the workspace version.
4. Every version-shaped image tag pinned in `deploy/` (the operator sample, the
   install examples) is that same version, with no `v` prefix.

Stdlib only and no helm: this runs in the `shell` job, which installs nothing,
and in the local gate's shell block, which must answer the same question. The
chart side is a two-rule mini-render (`image.tag` or `appVersion`) rather than a
`helm template` call; the rule is checked against the helper source, so a helper
that stops defaulting to `appVersion` is an extraction error rather than a
silently wrong answer.

A parser whose failure mode is a green run is worse than no parser, so fixtures
run first on every invocation, including the pre-fix workflow body itself: the
guard must call the shipped-today-broken `echo "TAG=${{ ... }}"` a failure.
"""

from __future__ import annotations

import os
import pathlib
import re
import subprocess
import sys
import tempfile

CARGO_TOML = pathlib.PurePath("Cargo.toml")
PUBLISH_YML = pathlib.PurePath(".github/workflows/publish.yml")
CHARTS = (
    pathlib.PurePath("deploy/helm/siglake"),
    pathlib.PurePath("deploy/helm/siglake-operator"),
)
# Tracked trees whose literal image tags ship to a user: the operator sample
# manifest and the install examples in the deployment READMEs.
PINNED_TAG_ROOT = pathlib.PurePath("deploy")

# `version = "0.1.0"` under `[workspace.package]`.
TOML_SECTION = re.compile(r"^\[(?P<name>[^\]]+)\]\s*$")
TOML_VERSION = re.compile(r'^version\s*=\s*"(?P<value>[^"]+)"\s*$')

# `run: ...` as a step key or as the first key of a step, and the block-scalar
# marker that means the body is on the following lines.
RUN_KEY = re.compile(r"^(?P<indent>\s*)(?:- )?run:(?P<value>.*)$")
BLOCK_SCALAR = re.compile(r"^[|>][+-]?\d*$")
REF_KEY = re.compile(r"^\s*ref:\s*(?P<value>.+?)\s*$", re.MULTILINE)

# A GitHub Actions expression. Ours never nest a `}`.
GH_EXPR = re.compile(r"\$\{\{(?P<expr>[^}]*)\}\}")

# `ghcr.io/example-invalid/siglake:0.1.0`, `...:${{ env.TAG }}`, `...:latest`.
IMAGE_REF = re.compile(
    r"(?P<image>ghcr\.io/[A-Za-z0-9._/-]+):(?P<tag>\$\{\{[^}]*\}\}|[A-Za-z0-9._-]+)"
)
# `--set image.tag=0.1.0`, `--set "image.tag=$IMAGE_TAG"`.
SET_IMAGE_TAG = re.compile(r"image\.tag=(?P<tag>[^\s\"']+)")

# A release version inside a tag: `0.1.0`, `0.1.0-rc.1`. Prefixed and suffixed
# tags (`operator-0.1.0`) carry one too and are held to the same version.
SEMVER = re.compile(r"\d+\.\d+\.\d+(?:-[A-Za-z0-9.]+)?")
V_PREFIXED = re.compile(r"(?<![A-Za-z0-9])v\d")

# `{{- define "siglake.image" -}}` ... `{{- end -}}`.
DEFINE = re.compile(r'\{\{-?\s*define\s+"(?P<name>[^"]+)"\s*-?\}\}')
END = re.compile(r"\{\{-?\s*end\s*-?\}\}")

APP_VERSION = re.compile(r'^appVersion:\s*"?(?P<value>[^"\s]+)"?\s*$')


class ExtractionError(Exception):
    """The file did not have the shape this gate parses.

    Never an empty result: a gate that compared nothing to nothing has not
    passed, and every shape here is one whose loss someone must see.
    """


# --- extraction --------------------------------------------------------------


def workspace_version(text: str) -> str:
    """`version` from Cargo.toml's `[workspace.package]`."""
    section = None
    for line in text.splitlines():
        m = TOML_SECTION.match(line)
        if m:
            section = m.group("name")
            continue
        if section != "workspace.package":
            continue
        m = TOML_VERSION.match(line)
        if m:
            return m.group("value")
    raise ExtractionError(f"{CARGO_TOML} has no `version` under `[workspace.package]`")


def run_scalars(text: str) -> list[str]:
    """Every `run:` body in a workflow, inline or block scalar."""
    lines = text.splitlines()
    out: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        i += 1
        m = RUN_KEY.match(line)
        if not m:
            continue
        value = m.group("value").strip()
        if not BLOCK_SCALAR.match(value):
            out.append(value)
            continue
        key_indent = len(m.group("indent"))
        body: list[str] = []
        while i < len(lines):
            b = lines[i]
            if b.strip() and len(b) - len(b.lstrip()) <= key_indent:
                break
            body.append(b)
            i += 1
        out.append("\n".join(body))
    return out


def tag_resolution_step(text: str) -> str:
    """The one `run:` body that assigns TAG."""
    steps = [s for s in run_scalars(text) if "TAG=" in s]
    if len(steps) != 1:
        raise ExtractionError(
            f"{PUBLISH_YML} has {len(steps)} `run:` steps that assign TAG; "
            "this gate evaluates exactly one"
        )
    return steps[0]


def evaluate_tag(step: str, ref: str) -> str:
    """Run the tag-resolution step with `ref` as the pushed tag; return TAG.

    Executing the shipped step is the point: the bug this gate exists for is a
    missing `${TAG#v}`, which no comparison of version strings can see.
    """

    def substitute(m: re.Match[str]) -> str:
        expr = m.group("expr")
        if "ref_name" not in expr and "inputs.tag" not in expr:
            raise ExtractionError(
                f"{PUBLISH_YML}'s tag-resolution step reads `{expr.strip()}`, "
                "which this gate cannot supply -- it substitutes the release tag only"
            )
        return ref

    script = "set -euo pipefail\n" + GH_EXPR.sub(substitute, step)
    with tempfile.TemporaryDirectory() as d:
        env_file = pathlib.Path(d) / "github_env"
        env_file.write_text("")
        proc = subprocess.run(
            ["bash", "-c", script],
            env={
                "GITHUB_ENV": str(env_file),
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            },
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise ExtractionError(
                f"{PUBLISH_YML}'s tag-resolution step exited {proc.returncode} "
                f"under bash: {proc.stderr.strip() or '(no stderr)'}"
            )
        written = [
            line.split("=", 1)[1]
            for line in env_file.read_text().splitlines()
            if line.startswith("TAG=")
        ]
    if len(written) != 1:
        raise ExtractionError(
            f"{PUBLISH_YML}'s tag-resolution step wrote {len(written)} TAG values "
            "to $GITHUB_ENV; this gate evaluates exactly one"
        )
    return written[0]


def define_bodies(text: str) -> dict[str, str]:
    """Every `{{ define "name" }}` body in a helper template."""
    bodies: dict[str, str] = {}
    for m in DEFINE.finditer(text):
        end = END.search(text, m.end())
        bodies[m.group("name")] = text[m.end() : end.start() if end else len(text)]
    return bodies


def chart_app_version(text: str) -> str:
    for line in text.splitlines():
        m = APP_VERSION.match(line)
        if m:
            return m.group("value")
    raise ExtractionError("Chart.yaml has no `appVersion`")


def values_image_tag(text: str) -> str:
    """`image.tag` from a chart's values.yaml; empty string when unset."""
    in_image = False
    for line in text.splitlines():
        if re.match(r"^image:\s*$", line):
            in_image = True
            continue
        if in_image:
            if line and not line[0].isspace():
                break
            m = re.match(r"^\s+tag:\s*(?P<value>.*?)\s*$", line)
            if m:
                return m.group("value").strip("\"'")
    raise ExtractionError("values.yaml has no `image.tag`")


def chart_default_tag(chart_text: str, values_text: str, helpers_text: str, label: str) -> str:
    """The image tag a default `helm install` of this chart would request.

    Two rules, both read off the source: an explicit `image.tag` wins, otherwise
    the helper's fallback to `appVersion`. A helper that stopped falling back is
    an extraction error -- the answer would be a guess.
    """
    image_helpers = {
        name: body for name, body in define_bodies(helpers_text).items() if "image" in name.lower()
    }
    if not image_helpers:
        raise ExtractionError(f"{label}: no image helper in _helpers.tpl")
    for name, body in sorted(image_helpers.items()):
        if ".Values.image.tag" in body and "Chart.AppVersion" not in body:
            raise ExtractionError(
                f"{label}: helper `{name}` reads image.tag but no longer falls back "
                "to appVersion -- the default install tag is no longer appVersion"
            )
    tag = values_image_tag(values_text)
    return tag if tag else chart_app_version(chart_text)


# --- checks ------------------------------------------------------------------


def normalization_problems(step: str, version: str) -> list[str]:
    """The workflow turns the release tag into the tag the charts ask for."""
    problems = []
    for ref in (f"v{version}", version):
        got = evaluate_tag(step, ref)
        if got != version:
            problems.append(
                f"{PUBLISH_YML}: publishing git tag `{ref}` would push image tag "
                f"`{got}`, but the charts request `{version}` -- strip the leading "
                "v when resolving TAG"
            )
    return problems


def trigger_problems(text: str) -> list[str]:
    """Both ways of publishing go through the step that was evaluated.

    The tag push and the manual dispatch share one expression today, so
    evaluating it covers both -- but only while it still reads both operands. A
    step that stopped reading `inputs.tag` would leave the dispatch path
    publishing something this gate never looked at.
    """
    step = tag_resolution_step(text)
    reads = " ".join(m.group("expr") for m in GH_EXPR.finditer(step))
    problems = []
    if re.search(r"^\s*tags:\s*\[?\"?v", text, re.MULTILINE) and "ref_name" not in reads:
        problems.append(
            f"{PUBLISH_YML}: a `v*` tag push publishes, but the tag-resolution step "
            "does not read `github.ref_name` -- that path is unnormalized"
        )
    if re.search(r"^\s*workflow_dispatch:", text, re.MULTILINE) and "inputs.tag" not in reads:
        problems.append(
            f"{PUBLISH_YML}: the manual dispatch publishes, but the tag-resolution "
            "step does not read its `tag` input -- that path is unnormalized"
        )
    return problems


def publish_reference_problems(text: str) -> list[str]:
    """Every pushed image is tagged from the resolved TAG, and only from it."""
    refs = IMAGE_REF.findall(text)
    if not refs:
        raise ExtractionError(f"{PUBLISH_YML} pushes no ghcr.io image")
    problems = []
    for image, tag in refs:
        flat = re.sub(r"\s+", " ", tag)
        if flat not in ("${{ env.TAG }}", "latest"):
            problems.append(
                f"{PUBLISH_YML}: `{image}` is tagged `{flat}`, which bypasses the "
                "resolved TAG -- use `${{ env.TAG }}` so the normalization applies"
            )
    for m in REF_KEY.finditer(text):
        if "env.TAG" in m.group("value"):
            problems.append(
                f"{PUBLISH_YML}: the checkout `ref:` reads the normalized TAG "
                f"(`{m.group('value')}`) -- it must stay the original `v` ref, "
                "which is the git ref that exists"
            )
    return problems


def chart_problems(label: str, default_tag: str, version: str) -> list[str]:
    if default_tag == version:
        return []
    return [
        f"{label}: a default install requests image tag `{default_tag}`, but the "
        f"published tag is `{version}` -- appVersion and the workspace version must match"
    ]


def pinned_tag_problems(path: str, text: str, version: str) -> list[str]:
    """Version-shaped image tags written out in shipped files.

    An install example or sample manifest that pins a tag is a promise about the
    registry, and it has to be the same promise the workflow keeps: the
    workspace version, spelled without the release tag's `v`.
    """
    problems = []
    candidates = [(m.group("tag"), m.group(0)) for m in IMAGE_REF.finditer(text)]
    candidates += [(m.group("tag"), m.group(0)) for m in SET_IMAGE_TAG.finditer(text)]
    for tag, whole in candidates:
        if "$" in tag or tag == "latest":
            continue
        found = SEMVER.findall(tag)
        if not found:
            continue
        for got in found:
            if got != version:
                problems.append(
                    f"{path}: `{whole}` pins version {got}, but the published tag "
                    f"is {version}"
                )
        if V_PREFIXED.search(tag):
            problems.append(
                f"{path}: `{whole}` carries the release tag's `v` prefix; published "
                "image tags are numeric"
            )
    return problems


# --- fixtures ----------------------------------------------------------------

# The workflow as it shipped before this gate: the git tag straight through.
BROKEN_STEP = 'echo "TAG=${{ github.event.inputs.tag || github.ref_name }}" >> "$GITHUB_ENV"'
FIXED_STEP = 'REF="${{ github.event.inputs.tag || github.ref_name }}"\necho "TAG=${REF#v}" >> "$GITHUB_ENV"'


def run_fixtures() -> int:
    """Prove every check can go red. Returns the number of fixtures checked."""
    checked = 0

    # The bug this gate was written for: equality of appVersion and the
    # workspace version is true on both sides of it, so only evaluating the step
    # separates them.
    if not normalization_problems(BROKEN_STEP, "0.1.0"):
        raise AssertionError("fixture: the pre-fix tag-resolution step passed normalization")
    checked += 1
    if normalization_problems(FIXED_STEP, "0.1.0"):
        raise AssertionError("fixture: a correct tag-resolution step was reported")
    checked += 1
    # Exactly one `v`, so a tag that is somehow `vv0.1.0` is still wrong.
    if evaluate_tag(FIXED_STEP, "vv0.1.0") != "v0.1.0":
        raise AssertionError("fixture: the strip is not a single leading v")
    checked += 1

    triggers = """
on:
  push:
    tags: ["v*"]
  workflow_dispatch:
    inputs:
      tag:
        required: true
jobs:
  publish:
    steps:
      - run: |
          REF="${{ github.event.inputs.tag || github.ref_name }}"
          echo "TAG=${REF#v}" >> "$GITHUB_ENV"
"""
    if trigger_problems(triggers):
        raise AssertionError("fixture: a workflow that normalizes both paths was reported")
    checked += 1
    for dropped, name in (
        ("github.event.inputs.tag || ", "the dispatch input"),
        (" || github.ref_name", "the pushed ref"),
    ):
        if not trigger_problems(triggers.replace(dropped, "")):
            raise AssertionError(f"fixture: a step that stopped reading {name} passed")
        checked += 1

    workflow = """
jobs:
  publish:
    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ github.ref }}
      - run: echo "TAG=x" >> "$GITHUB_ENV"
      - with:
          tags: |
            ghcr.io/example-invalid/siglake:${{ env.TAG }}
            ghcr.io/example-invalid/siglake:latest
"""
    if publish_reference_problems(workflow):
        raise AssertionError("fixture: a correct publish workflow was reported")
    checked += 1
    if not publish_reference_problems(
        workflow.replace("siglake:${{ env.TAG }}", "siglake:${{ github.ref_name }}")
    ):
        raise AssertionError("fixture: an image tagged straight from the git ref passed")
    checked += 1
    if not publish_reference_problems(workflow.replace("ref: ${{ github.ref }}", "ref: ${{ env.TAG }}")):
        raise AssertionError("fixture: a checkout of the normalized tag passed")
    checked += 1

    chart = 'name: c\nversion: 0.1.0\nappVersion: "0.1.0"\n'
    values = "image:\n  repository: ghcr.io/example-invalid/siglake\n  tag: \"\"\n  pullPolicy: IfNotPresent\n"
    helpers = '{{- define "c.image" -}}\n{{- $tag := default .Chart.AppVersion .Values.image.tag -}}\n{{- end -}}\n'
    if chart_default_tag(chart, values, helpers, "fixture") != "0.1.0":
        raise AssertionError("fixture: an empty image.tag did not render appVersion")
    checked += 1
    if chart_default_tag(chart, values.replace('tag: ""', "tag: 9.9.9"), helpers, "fixture") != "9.9.9":
        raise AssertionError("fixture: an explicit image.tag did not win")
    checked += 1
    if not chart_problems("fixture", chart_default_tag(chart, values, helpers, "fixture"), "0.1.1"):
        raise AssertionError("fixture: an appVersion behind the workspace version passed")
    checked += 1
    for name, args in (
        ("a helper that dropped its appVersion fallback", (chart, values, helpers.replace("default .Chart.AppVersion ", ""), "fixture")),
        ("a template with no image helper", (chart, values, '{{- define "c.name" -}}x{{- end -}}', "fixture")),
        ("values.yaml with no image.tag", (chart, "image:\n  repository: r\n", helpers, "fixture")),
        ("a Chart.yaml with no appVersion", ("name: c\nversion: 0.1.0\n", values, helpers, "fixture")),
    ):
        try:
            chart_default_tag(*args)
        except ExtractionError:
            checked += 1
        else:
            raise AssertionError(f"fixture: {name} rendered a tag without an error")

    pinned = "image: ghcr.io/example-invalid/siglake:0.1.0\n  --set image.tag=operator-0.1.0\n  --set image.tag=$IMAGE_TAG\n"
    if pinned_tag_problems("f", pinned, "0.1.0"):
        raise AssertionError("fixture: correctly pinned tags were reported")
    checked += 1
    if len(pinned_tag_problems("f", pinned, "0.1.1")) != 2:
        raise AssertionError("fixture: stale pinned tags were not both reported")
    checked += 1
    if not pinned_tag_problems("f", "image: ghcr.io/example-invalid/siglake:v0.1.0\n", "0.1.0"):
        raise AssertionError("fixture: a v-prefixed pinned tag passed")
    checked += 1

    for name, text, extract in (
        ("Cargo.toml without a workspace version", "[package]\nversion = \"9.9.9\"\n", workspace_version),
        ("a workflow with no TAG step", "jobs:\n  p:\n    steps:\n      - run: true\n", tag_resolution_step),
        ("a workflow with two TAG steps", 'jobs:\n  p:\n    steps:\n      - run: TAG=a\n      - run: TAG=b\n', tag_resolution_step),
        ("a workflow that pushes no image", "jobs:\n  p:\n    steps:\n      - run: true\n", publish_reference_problems),
    ):
        try:
            extract(text)
        except ExtractionError:
            checked += 1
        else:
            raise AssertionError(f"fixture: {name} extracted without an error")

    try:
        evaluate_tag('echo "TAG=${{ github.sha }}" >> "$GITHUB_ENV"', "v0.1.0")
    except ExtractionError:
        checked += 1
    else:
        raise AssertionError("fixture: an expression this gate cannot supply was evaluated anyway")

    return checked


# --- driver ------------------------------------------------------------------


def annotate(message: str, path: pathlib.PurePath = PUBLISH_YML) -> None:
    """Attach a failure to the file that needs attention under GitHub Actions."""
    if os.environ.get("GITHUB_ACTIONS"):
        print(f"::error file={path}::{message}")


def main(argv: list[str]) -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    problems: list[str] = []
    try:
        fixtures = run_fixtures()
        version = workspace_version((root / CARGO_TOML).read_text())
        publish = (root / PUBLISH_YML).read_text()
        problems += normalization_problems(tag_resolution_step(publish), version)
        problems += trigger_problems(publish)
        problems += publish_reference_problems(publish)
        for chart in CHARTS:
            default_tag = chart_default_tag(
                (root / chart / "Chart.yaml").read_text(),
                (root / chart / "values.yaml").read_text(),
                (root / chart / "templates/_helpers.tpl").read_text(),
                str(chart),
            )
            problems += chart_problems(str(chart), default_tag, version)
        pinned = subprocess.run(
            ["git", "ls-files", "-z", str(PINNED_TAG_ROOT)],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split("\0")
        pinned_files = [p for p in pinned if p]
        if not pinned_files:
            raise ExtractionError(f"no tracked files under {PINNED_TAG_ROOT}/")
        for rel in pinned_files:
            try:
                text = (root / rel).read_text()
            except (UnicodeDecodeError, FileNotFoundError):
                continue
            problems += pinned_tag_problems(rel, text, version)
    except (ExtractionError, AssertionError, OSError) as e:
        print(f"FAIL {e}", file=sys.stderr)
        annotate(str(e))
        return 1

    if problems:
        for problem in problems:
            print(f"FAIL {problem}", file=sys.stderr)
            annotate(problem)
        print(
            f"\n{len(problems)} mismatch(es) between the published image tag and "
            "what an install asks for.",
            file=sys.stderr,
        )
        return 1

    if "--print-facts" in argv:
        print(f"workspace version: {version}")
        print(f"publish tag for v{version}: {evaluate_tag(tag_resolution_step(publish), f'v{version}')}")
    print(
        f"ok   {len(CHARTS)} charts and {len(pinned_files)} deploy files agree on "
        f"image tag {version}; {fixtures} fixtures"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
