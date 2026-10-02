#!/usr/bin/env python3
"""The published image repositories and tags agree with the install defaults.

`.github/workflows/publish.yml` derives the image tag from the git tag, and the
git tag is `v0.1.0`. Both charts ask for their numeric `appVersion` when
`image.tag` is left empty, and the chart README documents `--set
image.tag=0.1.0`. Nothing compared the two sides, so `helm install` from the
published chart would have pulled `ghcr.io/example-invalid/siglake:0.1.0` against a
registry holding only `v0.1.0` -- an ImagePullBackOff on the first release. The
kind scripts always pass `image.tag` explicitly, so the default was never
exercised against a registry.

Seven questions, all answered from tracked files with no registry and no helm:

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
4. The first numbered release in `CHANGELOG.md` is the workspace version.
5. Each chart's default image repository, the operator sample and Terraform's
   generated Helm values use the repository that publishes that product.
   Explicit chart repository overrides remain supported and are outside this
   default-parity check.
6. Every version-shaped image tag pinned in `deploy/` (the operator sample, the
   install examples and the AWS image defaults) is that same version, with no
   `v` prefix.
7. Compose, kind and their helper scripts pin the same Bitnami Legacy MinIO
   server and client snapshots by readable tag and immutable digest.

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
CHANGELOG = pathlib.PurePath("CHANGELOG.md")
PUBLISH_YML = pathlib.PurePath(".github/workflows/publish.yml")
CHARTS = (
    pathlib.PurePath("deploy/helm/siglake"),
    pathlib.PurePath("deploy/helm/siglake-operator"),
)
PRODUCT_CHARTS = {
    "siglake": CHARTS[0],
    "siglake-operator": CHARTS[1],
}
OPERATOR_SAMPLE = pathlib.PurePath("deploy/operator/sample-cluster.yaml")
TERRAFORM_OUTPUTS = pathlib.PurePath("deploy/terraform/aws/outputs.tf")
# Tracked trees whose literal image tags ship to a user: operator samples,
# launchers and install examples.
PINNED_TAG_ROOT = pathlib.PurePath("deploy")
COMPOSE_YML = pathlib.PurePath("deploy/docker-compose.yml")
KIND_MINIO_YML = pathlib.PurePath("deploy/kind/manifests/minio.yaml")
SMOKE_SH = pathlib.PurePath("scripts/smoke.sh")
KIND_ROUND_SH = pathlib.PurePath("scripts/kind-round.sh")

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
# AWS examples use placeholders because the registry is supplied by the
# operator. Keep these exact so ordinary host:version prose is not treated as
# a pinned image.
PLACEHOLDER_IMAGE_REF = re.compile(
    r"(?P<image>__ECR_REPO_URL__|<ECR>):(?P<tag>[A-Za-z0-9._-]+)"
)
# The AWS launcher and its documentation spell the same default three ways.
SIGLAKE_IMAGE_TAG_DEFAULTS = (
    re.compile(r"SIGLAKE_IMAGE_TAG:-(?P<tag>[A-Za-z0-9._-]+)"),
    re.compile(r"SIGLAKE_IMAGE_TAG[^\n]*?\bdefault:\s*`?(?P<tag>[A-Za-z0-9._-]+)"),
    re.compile(r"SIGLAKE_IMAGE_TAG`?\s*\|\s*`?(?P<tag>[A-Za-z0-9._-]+)"),
)

# These roles are intentionally named rather than inferred from whatever
# MinIO references remain in the files. Deleting or replacing an image field
# must fail instead of shrinking the set the parity check compares.
COMPOSE_MINIO_ROLES = {
    "minio": "minio",
    "minio-init": "minio-client",
    "garage-init": "minio-client",
}
KIND_MINIO_ROLES = {
    "minio": "minio",
    "mc": "minio-client",
}
BITNAMI_LEGACY_MINIO_REF = re.compile(
    r"^docker\.io/bitnamilegacy/(?P<image>minio|minio-client):"
    r"(?P<tag>[A-Za-z0-9._-]+)@sha256:(?P<digest>[0-9a-f]{64})$"
)
MINIO_IMAGE_LITERAL = re.compile(
    r"(?:[A-Za-z0-9.-]+/)+(?:minio|minio-client|mc):[A-Za-z0-9._-]+"
    r"(?:@sha256:[0-9a-f]{64})?"
)

# A release version inside a tag: `0.1.0`, `0.1.0-rc.1`. Prefixed and suffixed
# tags (`operator-0.1.0`) carry one too and are held to the same version.
SEMVER = re.compile(r"\d+\.\d+\.\d+(?:-[A-Za-z0-9.]+)?")
V_PREFIXED = re.compile(r"(?<![A-Za-z0-9])v\d")

# `{{- define "siglake.image" -}}` ... `{{- end -}}`.
DEFINE = re.compile(r'\{\{-?\s*define\s+"(?P<name>[^"]+)"\s*-?\}\}')
END = re.compile(r"\{\{-?\s*end\s*-?\}\}")

APP_VERSION = re.compile(r'^appVersion:\s*"?(?P<value>[^"\s]+)"?\s*$')
VALUES_REPOSITORY = re.compile(r"^\s+repository:\s*(?P<value>\S.*?)\s*$")
TERRAFORM_OUTPUT = re.compile(
    r'^output\s+"(?P<name>[^"]+)"\s*\{\s*$', re.MULTILINE
)
TERRAFORM_REPOSITORY = re.compile(
    r'^\s+repository\s*=\s*"(?P<value>[^"]+)"\s*$', re.MULTILINE
)
CHANGELOG_RELEASE = re.compile(
    r"^##\s+(?P<version>\d+\.\d+\.\d+(?:-[A-Za-z0-9.]+)?)(?:\s|$)",
    re.MULTILINE,
)


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


def changelog_version(text: str) -> str:
    """Leading version token from the first numbered changelog section."""
    match = CHANGELOG_RELEASE.search(text)
    if not match:
        raise ExtractionError(f"{CHANGELOG} has no numbered `## <version>` section")
    return match.group("version")


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


def published_repositories(text: str) -> dict[str, str]:
    """Canonical repository for each product pushed by the workflow."""
    refs = [
        (image, re.sub(r"\s+", " ", tag))
        for image, tag in IMAGE_REF.findall(text)
    ]
    repositories: dict[str, str] = {}
    for product in PRODUCT_CHARTS:
        tagged = [
            image
            for image, tag in refs
            if image.rsplit("/", 1)[-1] == product and tag == "${{ env.TAG }}"
        ]
        if len(tagged) != 1:
            raise ExtractionError(
                f"{PUBLISH_YML} has {len(tagged)} resolved-TAG repositories for "
                f"`{product}`; expected one"
            )
        product_repositories = {
            image
            for image, tag in refs
            if image.rsplit("/", 1)[-1] == product
            and tag in ("${{ env.TAG }}", "latest")
        }
        if product_repositories != {tagged[0]}:
            rendered = ", ".join(sorted(product_repositories)) or "(none)"
            raise ExtractionError(
                f"{PUBLISH_YML} sends `{product}` release tags to different "
                f"repositories: {rendered}"
            )
        repositories[product] = tagged[0]
    return repositories


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


def values_image_repository(text: str) -> str:
    """Top-level `image.repository` from a chart's values.yaml."""
    in_image = False
    for line in text.splitlines():
        if re.match(r"^image:\s*$", line):
            in_image = True
            continue
        if in_image:
            if line and not line[0].isspace():
                break
            match = VALUES_REPOSITORY.match(line)
            if match:
                return match.group("value").strip("\"'")
    raise ExtractionError("values.yaml has no `image.repository`")


def chart_default_repository(
    values_text: str,
    helpers_text: str,
    label: str,
    override: str | None = None,
) -> str:
    """Repository rendered by the chart, with an optional install override."""
    helpers = define_bodies(helpers_text)
    image_helpers = {
        name: body
        for name, body in helpers.items()
        if "image" in name.lower()
        and ".Values.image.repository" in body
        and ".Values.image.tag" in body
    }
    if not image_helpers:
        raise ExtractionError(
            f"{label}: no image helper renders the top-level image.repository "
            "and image.tag values"
        )
    default = values_image_repository(values_text)
    return override if override is not None else default


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


def operator_sample_repository(text: str) -> str:
    """Repository named by the canonical SiglakeCluster example."""
    refs = [
        image
        for image, tag in IMAGE_REF.findall(text)
        if SEMVER.fullmatch(tag)
    ]
    if len(refs) != 1:
        raise ExtractionError(
            f"{OPERATOR_SAMPLE} has {len(refs)} versioned GHCR images; expected one"
        )
    return refs[0]


def terraform_helm_repository(text: str) -> str:
    """image.repository in Terraform's generated `helm_values` output."""
    outputs = list(TERRAFORM_OUTPUT.finditer(text))
    for index, match in enumerate(outputs):
        if match.group("name") != "helm_values":
            continue
        end = outputs[index + 1].start() if index + 1 < len(outputs) else len(text)
        repositories = TERRAFORM_REPOSITORY.findall(text[match.end() : end])
        if len(repositories) != 1:
            raise ExtractionError(
                f"{TERRAFORM_OUTPUTS}'s helm_values output has "
                f"{len(repositories)} image repositories; expected one"
            )
        return repositories[0]
    raise ExtractionError(f"{TERRAFORM_OUTPUTS} has no `helm_values` output")


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


def repository_problems(label: str, actual: str, published: str) -> list[str]:
    """A shipped default or example names the product's publish repository."""
    if actual == published:
        return []
    return [
        f"{label}: image repository `{actual}` differs from the publisher's "
        f"`{published}`"
    ]


def chart_problems(label: str, default_tag: str, version: str) -> list[str]:
    if default_tag == version:
        return []
    return [
        f"{label}: a default install requests image tag `{default_tag}`, but the "
        f"published tag is `{version}` -- appVersion and the workspace version must match"
    ]


def changelog_problems(changelog: str, version: str) -> list[str]:
    recorded = changelog_version(changelog)
    if recorded == version:
        return []
    return [
        f"{CHANGELOG}: first numbered release is `{recorded}`, but the workspace "
        f"version is `{version}` -- land the version bump and release section together"
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
    candidates += [
        (m.group("tag"), m.group(0)) for m in PLACEHOLDER_IMAGE_REF.finditer(text)
    ]
    for pattern in SIGLAKE_IMAGE_TAG_DEFAULTS:
        candidates += [(m.group("tag"), m.group(0)) for m in pattern.finditer(text)]
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


def compose_service_images(text: str) -> dict[str, list[str]]:
    """Literal `image` values keyed by compose service name."""
    images: dict[str, list[str]] = {}
    in_services = False
    service: str | None = None
    for line in text.splitlines():
        if line == "services:":
            in_services = True
            service = None
            continue
        if in_services and line and not line[0].isspace():
            break
        service_match = re.match(r"^  (?P<name>[A-Za-z0-9_-]+):\s*$", line)
        if service_match:
            service = service_match.group("name")
            continue
        image_match = re.match(r"^    image:\s*(?P<value>\S.*?)\s*$", line)
        if service is not None and image_match:
            images.setdefault(service, []).append(
                image_match.group("value").strip("\"'")
            )
    return images


def named_list_item_images(text: str) -> dict[str, list[str]]:
    """Literal `image` values keyed by a YAML list item's `name`."""
    lines = text.splitlines()
    images: dict[str, list[str]] = {}
    for i, line in enumerate(lines):
        item = re.match(r"^(?P<indent>\s*)- name:\s*(?P<name>\S+)\s*$", line)
        if not item:
            continue
        item_indent = len(item.group("indent"))
        for nested in lines[i + 1 :]:
            if nested.strip() and len(nested) - len(nested.lstrip()) <= item_indent:
                break
            image = re.match(r"^\s+image:\s*(?P<value>\S.*?)\s*$", nested)
            if image:
                images.setdefault(item.group("name"), []).append(
                    image.group("value").strip("\"'")
                )
                break
    return images


def minio_image_problems(
    compose: str, kind: str, smoke: str, kind_round: str
) -> list[str]:
    """MinIO roles use matching Bitnami Legacy tag-and-digest references."""
    sources = (
        (str(COMPOSE_YML), compose_service_images(compose), COMPOSE_MINIO_ROLES),
        (str(KIND_MINIO_YML), named_list_item_images(kind), KIND_MINIO_ROLES),
        (
            str(SMOKE_SH),
            {"client": MINIO_IMAGE_LITERAL.findall(smoke)},
            {"client": "minio-client"},
        ),
        (
            str(KIND_ROUND_SH),
            {"client": MINIO_IMAGE_LITERAL.findall(kind_round)},
            {"client": "minio-client"},
        ),
    )
    problems: list[str] = []
    references: dict[str, list[tuple[str, str, str]]] = {
        "minio": [],
        "minio-client": [],
    }
    for path, images, roles in sources:
        for role, expected_image in roles.items():
            declarations = images.get(role, [])
            if not declarations:
                problems.append(f"{path}: `{role}` has no image declaration")
                continue
            if len(declarations) != 1:
                problems.append(
                    f"{path}: `{role}` has {len(declarations)} image declarations; expected one"
                )
                continue
            reference = declarations[0]
            match = BITNAMI_LEGACY_MINIO_REF.fullmatch(reference)
            if not match or match.group("image") != expected_image:
                problems.append(
                    f"{path}: `{role}` image `{reference}` must be "
                    f"`docker.io/bitnamilegacy/{expected_image}:<tag>@sha256:<digest>`"
                )
                continue
            references[expected_image].append((path, role, reference))

    for image, pins in references.items():
        distinct = {reference for _, _, reference in pins}
        if len(distinct) > 1:
            rendered = ", ".join(
                f"{path} `{role}`={reference}" for path, role, reference in pins
            )
            problems.append(
                f"MinIO `{image}` references differ between deployment roles: {rendered}"
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

    repository_workflow = """
jobs:
  publish:
    steps:
      - with:
          tags: |
            ghcr.io/siglake/siglake:${{ env.TAG }}
            ghcr.io/siglake/siglake:latest
      - with:
          tags: |
            ghcr.io/siglake/siglake-operator:${{ env.TAG }}
            ghcr.io/siglake/siglake-operator:latest
"""
    published = published_repositories(repository_workflow)
    if published != {
        "siglake": "ghcr.io/siglake/siglake",
        "siglake-operator": "ghcr.io/siglake/siglake-operator",
    }:
        raise AssertionError("fixture: publish repositories were extracted incorrectly")
    checked += 1
    try:
        published_repositories(
            repository_workflow.replace(
                "ghcr.io/siglake/siglake-operator:latest",
                "ghcr.io/other/siglake-operator:latest",
            )
        )
    except ExtractionError:
        checked += 1
    else:
        raise AssertionError("fixture: split release and latest repositories passed")

    chart = 'name: c\nversion: 0.1.0\nappVersion: "0.1.0"\n'
    values = "image:\n  repository: ghcr.io/siglake/siglake\n  tag: \"\"\n  pullPolicy: IfNotPresent\n"
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

    repository_helpers = '''{{- define "c.image" -}}
{{ .Values.image.repository }}:{{ default .Chart.AppVersion .Values.image.tag }}
{{- end -}}
'''
    if chart_default_repository(values, repository_helpers, "fixture") != published["siglake"]:
        raise AssertionError("fixture: the matching chart repository was not extracted")
    checked += 1
    old_values = values.replace("ghcr.io/siglake/siglake", "ghcr.io/example-invalid/siglake")
    if not repository_problems(
        "fixture",
        chart_default_repository(old_values, repository_helpers, "fixture"),
        published["siglake"],
    ):
        raise AssertionError(
            "fixture: equal numeric tags hid a chart/publisher repository mismatch"
        )
    checked += 1
    override = "registry.example/customer/siglake"
    if chart_default_repository(values, repository_helpers, "fixture", override) != override:
        raise AssertionError("fixture: an explicit chart repository override did not win")
    checked += 1
    try:
        chart_default_repository(values, '{{- define "c.image" -}}fixed{{- end -}}', "fixture")
    except ExtractionError:
        checked += 1
    else:
        raise AssertionError("fixture: a helper that ignores repository overrides passed")

    operator_sample = "spec:\n  image: ghcr.io/siglake/siglake:0.1.0\n"
    if repository_problems(
        str(OPERATOR_SAMPLE),
        operator_sample_repository(operator_sample),
        published["siglake"],
    ):
        raise AssertionError("fixture: a matching operator example was reported")
    checked += 1
    if not repository_problems(
        str(OPERATOR_SAMPLE),
        operator_sample_repository(operator_sample.replace("ghcr.io/siglake", "ghcr.io/example-invalid")),
        published["siglake"],
    ):
        raise AssertionError("fixture: an operator example repository mismatch passed")
    checked += 1

    terraform = '''output "helm_values" {
  value = yamlencode({
    image = {
      repository = "ghcr.io/siglake/siglake"
    }
  })
}
'''
    if repository_problems(
        str(TERRAFORM_OUTPUTS),
        terraform_helm_repository(terraform),
        published["siglake"],
    ):
        raise AssertionError("fixture: matching Terraform Helm values were reported")
    checked += 1
    if not repository_problems(
        str(TERRAFORM_OUTPUTS),
        terraform_helm_repository(terraform.replace("ghcr.io/siglake", "ghcr.io/example-invalid")),
        published["siglake"],
    ):
        raise AssertionError("fixture: a Terraform repository mismatch passed")
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

    changelog = "# Changelog\n\n## Unreleased\n\n## 0.1.0\n"
    if changelog_problems(changelog, "0.1.0"):
        raise AssertionError("fixture: a plain current changelog section was reported")
    checked += 1
    if changelog_problems(changelog.replace("## 0.1.0", "## 0.1.0 — first release"), "0.1.0"):
        raise AssertionError("fixture: a subtitled current changelog section was reported")
    checked += 1
    stale_changelog = changelog.replace("## 0.1.0", "## 0.0.9\n\n## 0.1.0")
    if not changelog_problems(stale_changelog, "0.1.0"):
        raise AssertionError("fixture: a stale first release section passed")
    checked += 1
    try:
        changelog_problems("# Changelog\n\n## Unreleased\n", "0.1.0")
    except ExtractionError:
        checked += 1
    else:
        raise AssertionError("fixture: a changelog with no release section passed")

    aws_pins = """\
image: __ECR_REPO_URL__:0.1.0
pushed `<ECR>:operator-0.1.0` and `<ECR>:0.1.0`
SIGLAKE_IMAGE_TAG image tag to deploy (default: 0.1.0)
| `SIGLAKE_IMAGE_TAG` | `0.1.0` | Image tag to deploy. |
IMAGE_TAG="${SIGLAKE_IMAGE_TAG:-0.1.0}"
"""
    if pinned_tag_problems("f", aws_pins, "0.1.0"):
        raise AssertionError("fixture: current AWS image pins were reported")
    checked += 1
    if len(pinned_tag_problems("f", aws_pins, "0.1.1")) != 6:
        raise AssertionError("fixture: stale AWS image pins were not all reported")
    checked += 1
    prose = "pre-0.1.0; tagged v0.1.0; rollback boundary 0.1.0; example.invalid:0.1.0\n"
    if pinned_tag_problems("f", prose, "0.1.1"):
        raise AssertionError("fixture: release prose was treated as a pinned image")
    checked += 1

    server_digest = "1" * 64
    client_digest = "2" * 64
    server_ref = (
        "docker.io/bitnamilegacy/minio:SERVER-1@sha256:" + server_digest
    )
    client_ref = (
        "docker.io/bitnamilegacy/minio-client:CLIENT-1@sha256:" + client_digest
    )
    compose_minio = f"""\
services:
  minio:
    image: {server_ref}
  minio-init:
    image: {client_ref}
  garage-init:
    image: {client_ref}
  unrelated:
    image: example.invalid/other:latest
"""
    kind_minio = f"""\
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: minio
          image: {server_ref}
---
apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      containers:
        - name: mc
          image: {client_ref}
"""
    smoke_minio = f"docker run {client_ref}\n"
    kind_round_minio = f"kubectl run --image={client_ref}\n"
    if minio_image_problems(
        compose_minio, kind_minio, smoke_minio, kind_round_minio
    ):
        raise AssertionError("fixture: matching MinIO pins were reported")
    checked += 1
    if not minio_image_problems(
        compose_minio,
        kind_minio.replace("minio:SERVER-1", "minio:SERVER-2"),
        smoke_minio,
        kind_round_minio,
    ):
        raise AssertionError("fixture: a kind MinIO server tag mismatch passed")
    checked += 1
    if not minio_image_problems(
        compose_minio,
        kind_minio.replace("minio-client:CLIENT-1", "minio-client:CLIENT-2"),
        smoke_minio,
        kind_round_minio,
    ):
        raise AssertionError("fixture: a kind MinIO client tag mismatch passed")
    checked += 1
    if not minio_image_problems(
        compose_minio.replace(
            f"garage-init:\n    image: {client_ref}",
            f"garage-init:\n    image: {client_ref.replace('CLIENT-1', 'CLIENT-2')}",
        ),
        kind_minio,
        smoke_minio,
        kind_round_minio,
    ):
        raise AssertionError("fixture: compose's Garage client tag mismatch passed")
    checked += 1
    for role, declaration in (
        ("compose minio", f"    image: {server_ref}\n"),
        ("compose minio-init", f"    image: {client_ref}\n"),
        (
            "compose garage-init",
            f"  garage-init:\n    image: {client_ref}\n",
        ),
        ("kind minio", f"          image: {server_ref}\n"),
        ("kind mc", f"          image: {client_ref}\n"),
    ):
        compose_fixture = compose_minio
        kind_fixture = kind_minio
        if role.startswith("compose"):
            compose_fixture = compose_fixture.replace(declaration, "", 1)
        else:
            kind_fixture = kind_fixture.replace(declaration, "", 1)
        if not minio_image_problems(
            compose_fixture, kind_fixture, smoke_minio, kind_round_minio
        ):
            raise AssertionError(f"fixture: missing {role} image declaration passed")
        checked += 1
    for refused in ("quay.io/minio/minio", "docker.io/minio/minio"):
        if not minio_image_problems(
            compose_minio.replace("docker.io/bitnamilegacy/minio", refused, 1),
            kind_minio,
            smoke_minio,
            kind_round_minio,
        ):
            raise AssertionError(f"fixture: `{refused}` MinIO server reference passed")
        checked += 1
    if not minio_image_problems(
        compose_minio.replace(f"@sha256:{server_digest}", "", 1),
        kind_minio,
        smoke_minio,
        kind_round_minio,
    ):
        raise AssertionError("fixture: a MinIO server reference without a digest passed")
    checked += 1
    if not minio_image_problems(compose_minio, kind_minio, "", kind_round_minio):
        raise AssertionError("fixture: a missing smoke client reference passed")
    checked += 1
    if not minio_image_problems(compose_minio, kind_minio, smoke_minio, ""):
        raise AssertionError("fixture: a missing kind-round client reference passed")
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
        problems += changelog_problems((root / CHANGELOG).read_text(), version)
        publish = (root / PUBLISH_YML).read_text()
        problems += normalization_problems(tag_resolution_step(publish), version)
        problems += trigger_problems(publish)
        problems += publish_reference_problems(publish)
        repositories = published_repositories(publish)
        for product, chart in PRODUCT_CHARTS.items():
            values_text = (root / chart / "values.yaml").read_text()
            helpers_text = (root / chart / "templates/_helpers.tpl").read_text()
            default_tag = chart_default_tag(
                (root / chart / "Chart.yaml").read_text(),
                values_text,
                helpers_text,
                str(chart),
            )
            problems += chart_problems(str(chart), default_tag, version)
            problems += repository_problems(
                str(chart),
                chart_default_repository(values_text, helpers_text, str(chart)),
                repositories[product],
            )
        problems += repository_problems(
            str(OPERATOR_SAMPLE),
            operator_sample_repository((root / OPERATOR_SAMPLE).read_text()),
            repositories["siglake"],
        )
        problems += repository_problems(
            str(TERRAFORM_OUTPUTS),
            terraform_helm_repository((root / TERRAFORM_OUTPUTS).read_text()),
            repositories["siglake"],
        )
        problems += minio_image_problems(
            (root / COMPOSE_YML).read_text(),
            (root / KIND_MINIO_YML).read_text(),
            (root / SMOKE_SH).read_text(),
            (root / KIND_ROUND_SH).read_text(),
        )
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
            f"\n{len(problems)} release mismatch(es).",
            file=sys.stderr,
        )
        return 1

    if "--print-facts" in argv:
        print(f"workspace version: {version}")
        print(f"publish tag for v{version}: {evaluate_tag(tag_resolution_step(publish), f'v{version}')}")
        for product, repository in repositories.items():
            print(f"publish repository for {product}: {repository}")
    print(
        f"ok   changelog, {len(CHARTS)} charts and {len(pinned_files)} deploy files "
        f"agree on release repositories and version {version}; MinIO pins agree; "
        f"{fixtures} fixtures"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
