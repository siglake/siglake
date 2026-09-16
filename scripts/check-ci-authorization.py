#!/usr/bin/env python3
"""A fork's pull request runs only the exact commit a maintainer reviewed.

Two jobs in one file, for the same reason check-release-tags.py evaluates
publish.yml's tag step: the rule and the test of the rule must not be two
copies.

  * `--decide` reads the `pull_request_target` event at $GITHUB_EVENT_PATH and
    prints `approve=`, `revoke=`, `sha=`, `pr=`, `head_repo=` and `reason=`
    for ci-authorize.yml's `$GITHUB_OUTPUT`. This is the decision itself.
  * with no argument it runs the decision table over synthetic events and the
    static checks below over the real workflows. That is the gate; it runs in
    ci.yml's `shell` job and in scripts/ci-local.sh's shell block.

WHY IT IS SHAPED THIS WAY. A fork's pull request runs with a read-only token,
no secrets, and an Actions cache scoped to `refs/pull/N/merge`; GitHub enforces
all three, and a run awaiting approval has executed nothing. `ci:run` therefore
does not *carry* the authorization, it *releases* one specific queued run:
ci-authorize.yml runs on `pull_request_target` (trusted base code, write-capable
token, and NO checkout of the fork), reads the label event, re-reads the pull
request's head, and approves the queued `pull_request` run whose `head_sha` is
that exact commit.

The alternative -- `pull_request_target` with `ref: head.sha` and the jobs
gated on the label -- is the recipe GitHub's own hardening guidance rules out:
"Workflows that use these triggers must not explicitly check out untrusted
code", because "these workflows are privileged, which means they share the same
cache of the main branch with other privileged workflow triggers, and may have
repository write access and access to referenced secrets"
(docs.github.com/en/actions/reference/security/secure-use). Approving a queued
run instead keeps the executed code inside the restricted context GitHub built
for it, and keeps the decision in base-branch code the fork cannot edit. Which
is why the first static check below is that ci.yml is NOT reachable from a
privileged trigger: if `pull_request_target` or `workflow_call` ever appears in
its `on:` block, every sentence in this paragraph stops being true.

WHAT A STALE LABEL CANNOT DO. Approval is granted to a run id, and a run id is
pinned to a commit. A new commit produces a new queued run, which no earlier
approval touches, and `synchronize` removes the label so the next `labeled`
event -- a deliberate act by a trusted login, after reading the new diff -- is
what releases it. An unrelated label event decides nothing; a `ci:run` label
from a login outside the trusted list decides nothing.

THE WINDOW THAT REMAINS. Between the labeling event and the approval call the
fork can push again; scripts/ci-approve-run.sh closes it by re-reading the head
and refusing when it has moved, and approving by `head_sha` rather than by pull
request number means a later commit cannot inherit the approval.

Stdlib only: this runs in ci.yml's `shell` job, which installs nothing.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import sys

WORKFLOWS = pathlib.PurePath(".github/workflows")
CI_YML = WORKFLOWS / "ci.yml"
AUTHORIZE_YML = WORKFLOWS / "ci-authorize.yml"
APPROVE_SH = pathlib.PurePath("scripts/ci-approve-run.sh")
CHECKER = pathlib.PurePath("scripts/check-ci-authorization.py")

# The label. One spelling, read by the workflow from this file's output rather
# than written out again there.
LABEL = "ci:run"

# Triggers that run with the base repository's privileges. ci.yml must not be
# reachable from any of them -- see the module docstring.
PRIVILEGED_TRIGGERS = ("pull_request_target", "workflow_run", "workflow_call")

# The complete permission maps allowed in ci.yml. `docker` needs the one extra
# grant, `id-token: write`, to assume the ECR role on a push to main. Naming
# the whole map instead of allow-listing the job keeps another write scope from
# hitching a ride on that exception.
WORKFLOW_PERMISSIONS = {"contents": "read"}
JOB_PERMISSIONS = {"docker": {"contents": "read", "id-token": "write"}}

ECR_PUSH = "${{ github.event_name == 'push' && secrets.AWS_ROLE_ARN != '' }}"
ECR_STEP_IF = "if: env.ECR_PUSH == 'true'"
ECR_IMAGE_PUSH = "push: ${{ env.ECR_PUSH == 'true' }}"

SHA = re.compile(r"^[0-9a-f]{40}$")

FULL_LINE_COMMENT = re.compile(r"^\s*#")
TOP_KEY = re.compile(r"^(?P<name>[A-Za-z0-9_-]+):")
KEY_AT = re.compile(r"^(?P<indent> *)(?P<name>[A-Za-z0-9_-]+):")


class ExtractionError(Exception):
    """The file did not have the shape this gate parses.

    Never an empty result: a workflow with no `on:` block is a parse that
    failed, not a workflow that passed.
    """


# --- the decision ------------------------------------------------------------


class Decision:
    """What one event authorizes. `approve` and `revoke` are never both true."""

    def __init__(
        self,
        reason: str,
        *,
        approve: bool = False,
        revoke: bool = False,
        sha: str = "",
        pr: str = "",
        head_repo: str = "",
        level: str = "notice",
    ) -> None:
        self.reason = reason
        self.approve = approve
        self.revoke = revoke
        self.sha = sha
        self.pr = pr
        self.head_repo = head_repo
        # `warning` for a maintainer's action that had no effect -- a ci:run
        # label that released nothing is the one refusal somebody is waiting
        # on, and a green run with a quiet summary line is how it gets missed.
        self.level = level

    def outputs(self) -> list[str]:
        return [
            f"approve={'true' if self.approve else 'false'}",
            f"revoke={'true' if self.revoke else 'false'}",
            f"level={self.level}",
            f"sha={self.sha}",
            f"pr={self.pr}",
            f"head_repo={self.head_repo}",
            # One line: GITHUB_OUTPUT takes key=value, and a reason worth
            # reading is worth reading on the summary line of the job.
            f"reason={' '.join(self.reason.split())}",
        ]


def trusted_logins(raw: str | None) -> frozenset[str]:
    """The configured logins, lowercased. Commas or whitespace, either way.

    This matches the workflow expression's `variable || 'gianarb'`: GitHub
    treats both an unset variable and an empty variable as false, so both use
    the human-confirmed default. A non-empty typo still fails closed.
    """
    resolved = raw or "gianarb"
    return frozenset(part.lower() for part in re.split(r"[\s,]+", resolved) if part)


def decide(event_name: str, event: dict, trusted: frozenset[str]) -> Decision:
    """The whole rule. Pure: same event, same answer, no API, no clock."""
    if event_name != "pull_request_target":
        return Decision(
            f"{event_name} decides nothing here: authorization is a "
            "pull_request_target label event on base-branch code"
        )

    action = event.get("action") or ""
    pr = event.get("pull_request") or {}
    number = pr.get("number")
    head = pr.get("head") or {}
    sha = head.get("sha") or ""
    head_repo = ((head.get("repo") or {}).get("full_name")) or ""
    base_repo = (((pr.get("base") or {}).get("repo") or {}).get("full_name")) or ""
    sender = ((event.get("sender") or {}).get("login")) or ""
    labels = {(label.get("name") or "") for label in (pr.get("labels") or [])}

    if action == "synchronize":
        # The reviewed commit is no longer the proposed commit. The queued run
        # for the new head is a different run and carries no approval; drop the
        # label so releasing it is a deliberate act again.
        if LABEL in labels:
            return Decision(
                f"a new head ({sha[:12]}) revokes the reviewed authorization: "
                f"removing {LABEL}",
                revoke=True,
            )
        return Decision(f"a new head ({sha[:12]}) and no {LABEL} label: nothing to revoke")

    if action != "labeled":
        return Decision(f"action `{action}` is not a labeling event: nothing to decide")

    name = ((event.get("label") or {}).get("name")) or ""
    if name != LABEL:
        return Decision(f"label `{name}` is not {LABEL}: nothing to decide")

    if sender.lower() not in trusted:
        return Decision(
            f"`{sender}` is not a trusted login, so the {LABEL} label releases "
            "nothing -- set the SIGLAKE_CI_TRUSTED_LOGINS repository variable "
            "to the logins allowed to authorize a run",
            level="warning",
        )

    if not number:
        return Decision("the event carries no pull request number", level="warning")

    if head_repo and head_repo == base_repo:
        # A branch in this repository already runs on `pull_request` with
        # nothing held back. Labeling it is harmless and releases nothing.
        return Decision(
            f"#{number}'s head is a branch in {base_repo}, which already runs: "
            "nothing to release"
        )

    if not SHA.match(sha):
        return Decision(f"#{number} has no reviewable head commit (`{sha}`)", level="warning")

    return Decision(
        f"{sender} authorized {head_repo}@{sha[:12]} for #{number}",
        approve=True,
        sha=sha,
        pr=str(number),
        head_repo=head_repo,
    )


def read_event(path: str | None) -> tuple[str, dict]:
    event_name = os.environ.get("GITHUB_EVENT_NAME", "")
    if not path:
        raise ExtractionError("GITHUB_EVENT_PATH is unset: there is no event to decide on")
    return event_name, json.loads(pathlib.Path(path).read_text())


# --- the static checks -------------------------------------------------------


def strip_comments(text: str) -> list[str]:
    return [line for line in text.splitlines() if not FULL_LINE_COMMENT.match(line)]


def block(text: str, key: str) -> list[str]:
    """The lines under a top-level `key:`, comments dropped.

    `on:` and `permissions:` are both read this way. A missing key is an
    error: every check below is about what a block does or does not contain,
    and an absent block would answer every one of them with silence.
    """
    lines = strip_comments(text)
    start = next(
        (i for i, line in enumerate(lines) if TOP_KEY.match(line) and line.startswith(f"{key}:")),
        None,
    )
    if start is None:
        raise ExtractionError(f"no top-level `{key}:` key")
    out: list[str] = []
    for line in lines[start + 1 :]:
        if TOP_KEY.match(line):
            break
        out.append(line)
    if not out and not lines[start].strip().endswith(":"):
        raise ExtractionError(f"`{key}:` is empty")
    return out


def jobs(text: str) -> dict[str, list[str]]:
    """{job name: its lines}, comments dropped. Same parse as check-ci-linker."""
    lines = strip_comments(text)
    start = next((i for i, line in enumerate(lines) if line.startswith("jobs:")), None)
    if start is None:
        raise ExtractionError("no `jobs:` key")
    out: dict[str, list[str]] = {}
    current: str | None = None
    for line in lines[start + 1 :]:
        m = KEY_AT.match(line)
        if m and len(m.group("indent")) == 2:
            current = m.group("name")
            out[current] = []
            continue
        if TOP_KEY.match(line):
            current = None
            continue
        if current is not None:
            out[current].append(line)
    if not out:
        raise ExtractionError("`jobs:` declares no job -- the indent this gate reads changed")
    return out


def steps(lines: list[str]) -> list[list[str]]:
    """A job's steps, as lists of lines. A step starts at a `- ` list item."""
    out: list[list[str]] = []
    for line in lines:
        if re.match(r"^\s+- ", line):
            out.append([line])
        elif out:
            out[-1].append(line)
    return out


def checkout_steps(text: str) -> list[list[str]]:
    """Every `uses: actions/checkout` step, as its own lines.

    A step owns the lines from its `- uses:` until the next list item at the
    same indent or anything shallower.
    """
    lines = strip_comments(text)
    steps: list[list[str]] = []
    for i, line in enumerate(lines):
        if "uses: actions/checkout" not in line:
            continue
        indent = len(line) - len(line.lstrip())
        step = [line]
        for follow in lines[i + 1 :]:
            if not follow.strip():
                continue
            follow_indent = len(follow) - len(follow.lstrip())
            if follow_indent < indent or (follow_indent == indent and follow.lstrip().startswith("- ")):
                break
            step.append(follow)
        steps.append(step)
    if not steps:
        raise ExtractionError("no `uses: actions/checkout` step")
    return steps


def indented_mapping(lines: list[str], key: str, indent: int) -> dict[str, str] | None:
    """Read one simple YAML mapping at a known indentation.

    Workflow permissions and job permissions contain scalar values only. If
    that changes, returning the literal value makes the exact-map audit fail.
    """
    prefix = " " * indent
    start = next((i for i, line in enumerate(lines) if line == f"{prefix}{key}:"), None)
    if start is None:
        return None
    out: dict[str, str] = {}
    for line in lines[start + 1 :]:
        if not line.strip():
            continue
        line_indent = len(line) - len(line.lstrip())
        if line_indent <= indent:
            break
        match = re.match(rf"^ {{{indent + 2}}}(?P<name>[A-Za-z0-9_-]+):\s*(?P<value>.+?)\s*$", line)
        if not match:
            out[f"<unparsed:{line.strip()}>"] = ""
            continue
        name = match.group("name")
        if name in out:
            out[f"<duplicate:{name}>"] = match.group("value")
        else:
            out[name] = match.group("value")
    return out


def audit_ci(text: str) -> list[str]:
    """Every way ci.yml stops being the restricted half of this arrangement."""
    problems: list[str] = []

    # The `on:` line itself as well as the block under it: `on: [push,
    # pull_request_target]` is the same workflow as the indented spelling, and
    # a gate that only reads one of the two shapes reads whichever the next
    # edit does not use.
    triggers = [
        line
        for line in strip_comments(text)
        if TOP_KEY.match(line) and line.startswith("on:")
    ] + block(text, "on")
    for trigger in PRIVILEGED_TRIGGERS:
        if any(re.search(rf"\b{re.escape(trigger)}\b", line) for line in triggers):
            problems.append(
                f"{CI_YML}: `{trigger}` in the `on:` block. This workflow runs "
                "contributor code, and it may only do so from `pull_request`, "
                "where the token is read-only, no secret is readable and the "
                "Actions cache is scoped to the pull request instead of to "
                f"main. A `{trigger}` run of these jobs has all three, which "
                "is the arrangement scripts/check-ci-authorization.py exists "
                "to keep from coming back."
            )

    permissions = indented_mapping(strip_comments(text), "permissions", 0)
    if permissions != WORKFLOW_PERMISSIONS:
        problems.append(
            f"{CI_YML}: workflow permissions are {permissions}; the exact "
            f"allowed map is {WORKFLOW_PERMISSIONS}. An omitted scope can "
            "inherit the repository default, and another scope expands what "
            "contributor code can do."
        )

    # A pull request may not reach anything privileged in here. Three ways
    # that can stop being true, each checked where it would break:
    #
    #   * a job asking for a write permission that is not the one job this
    #     workflow has a reason to grant (OIDC for the ECR push on main);
    #   * a secret readable outside a step the event gates -- a fork's run
    #     reads no secret at all, but the development repository's own pull
    #     requests do run here, and `ECR_PUSH` is what keeps them from
    #     assuming the deployment role;
    #   * an AWS step that lost its condition.
    for job, lines in jobs(text).items():
        declared_permissions = indented_mapping(lines, "permissions", 4)
        expected_permissions = JOB_PERMISSIONS.get(job)
        if declared_permissions != expected_permissions:
            problems.append(
                f"{CI_YML}: job `{job}` permissions are {declared_permissions}; "
                f"the exact allowed map is {expected_permissions}. Only "
                "`docker` may widen the workflow grant, and only with "
                "`id-token: write` plus `contents: read`."
            )

        if job == "docker":
            ecr_push_lines = [
                line.strip() for line in lines if line.strip().startswith("ECR_PUSH:")
            ]
            expected = f"ECR_PUSH: {ECR_PUSH}"
            if ecr_push_lines != [expected]:
                problems.append(
                    f"{CI_YML}: docker must define exactly `{expected}` once; "
                    f"found {ecr_push_lines}. ECR publication is push-only."
                )
        for step in steps(lines):
            body = "\n".join(step)
            condition = next((line.strip() for line in step if re.match(r"^\s+if:", line)), "")
            gated = condition == ECR_STEP_IF
            if re.search(r"\baws-actions/", body) and not gated:
                problems.append(
                    f"{CI_YML}: job `{job}` runs `{step[0].strip()}` without "
                    f"the exact `{ECR_STEP_IF}` condition (found "
                    f"`{condition or 'nothing'}`). That condition is the only "
                    "thing standing between a pull request and the deployment role."
                )
            if "uses: docker/build-push-action@" in body:
                push_inputs = [line.strip() for line in step if line.strip().startswith("push:")]
                if push_inputs != [ECR_IMAGE_PUSH]:
                    problems.append(
                        f"{CI_YML}: `{step[0].strip()}` image push inputs are "
                        f"{push_inputs}; expected exactly `{ECR_IMAGE_PUSH}`."
                    )
            for line in step:
                if "secrets." in line and not gated:
                    problems.append(
                        f"{CI_YML}: `{line.strip()}` reads a secret in a step "
                        "the event does not gate. Every secret in this "
                        "workflow has to be behind `ECR_PUSH`, which is "
                        "`github.event_name == 'push'` and the secret being "
                        "set at all."
                    )
        # Everything above the job's `steps:` key is its own header: `env:`,
        # `permissions:`, `runs-on:`.
        header = lines[: next(
            (i for i, line in enumerate(lines) if re.match(r"^\s+steps:", line)), len(lines)
        )]
        for line in header:
            if "secrets." in line and "github.event_name == 'push'" not in line:
                problems.append(
                    f"{CI_YML}: `{line.strip()}` resolves a secret in job "
                    f"`{job}`'s own `env:` without testing the event. A pull "
                    "request must not be able to read it."
                )

    for step in checkout_steps(text):
        head = step[0].strip()
        if not any(re.match(r"^\s*persist-credentials:\s*false\s*$", line) for line in step[1:]):
            problems.append(
                f"{CI_YML}: `{head}` does not set `persist-credentials: false`. "
                "Contributor code runs in this job; leaving a credential in "
                "`.git/config` hands it to every build script."
            )
        if any(re.match(r"^\s*ref:", line) for line in step[1:]):
            problems.append(
                f"{CI_YML}: `{head}` pins a `ref:`. The `pull_request` event's "
                "own merge ref is the reviewed code here; an explicit ref is "
                "how a privileged rewrite of this file would start."
            )

    return problems


def audit_authorize(text: str) -> list[str]:
    """Every way the controller stops being base-branch-only."""
    problems: list[str] = []
    lines = strip_comments(text)

    triggers = block(text, "on")
    trigger_keys = {
        m.group("name")
        for m in (KEY_AT.match(line) for line in triggers)
        if m and len(m.group("indent")) == 2
    }
    if trigger_keys != {"pull_request_target"}:
        problems.append(
            f"{AUTHORIZE_YML}: triggers are {sorted(trigger_keys)}; this "
            "workflow holds a write-capable token and must answer to nothing "
            "but `pull_request_target`."
        )
    types = next((line for line in triggers if re.match(r"^\s+types:", line)), "")
    if "labeled" not in types or "synchronize" not in types:
        problems.append(
            f"{AUTHORIZE_YML}: `types:` must be `[labeled, synchronize]` -- "
            "`labeled` is the authorization and `synchronize` is what revokes "
            f"it. Found: `{types.strip() or 'nothing'}`."
        )

    for step in checkout_steps(text):
        if any(re.match(r"^\s*ref:", line) for line in step[1:]):
            problems.append(
                f"{AUTHORIZE_YML}: the checkout pins a `ref:`. A "
                "pull_request_target run must take the base branch and only "
                "the base branch; a ref that can name the fork's head is the "
                "pwn-request shape."
            )

    for pattern, why in (
        (r"\bcargo\b", "builds nothing"),
        (r"\bnpm\b|\bpip install\b", "installs nothing"),
        (r"github\.event\.pull_request\.(title|body|head\.ref)", "interpolates no fork-controlled text"),
    ):
        hit = next((line for line in lines if re.search(pattern, line)), "")
        if hit:
            problems.append(
                f"{AUTHORIZE_YML}: `{hit.strip()}` -- this job {why}. It reads "
                "the event, calls the API, and exits."
            )

    if not any(f"{CHECKER}" in line for line in lines):
        problems.append(f"{AUTHORIZE_YML}: does not run {CHECKER} --decide; the decision is there.")
    if not any(f"{APPROVE_SH}" in line for line in lines):
        problems.append(f"{AUTHORIZE_YML}: does not run {APPROVE_SH}; the head recheck is there.")

    trusted_expression = "CI_TRUSTED_LOGINS: ${{ vars.SIGLAKE_CI_TRUSTED_LOGINS || 'gianarb' }}"
    trusted_lines = [
        line.strip() for line in lines if line.strip().startswith("CI_TRUSTED_LOGINS:")
    ]
    if trusted_lines != [trusted_expression]:
        problems.append(
            f"{AUTHORIZE_YML}: trusted-logins resolution must be exactly "
            f"`{trusted_expression}` so unset and empty repository variables "
            "both retain the human-confirmed gianarb default."
        )

    return problems


# --- fixtures ----------------------------------------------------------------

TRUSTED = frozenset({"gianarb", "toddpersen"})
FORK_SHA = "a" * 40
NEW_SHA = "b" * 40


def event(
    action: str,
    *,
    head_repo: str = "contributor/siglake",
    sha: str = FORK_SHA,
    sender: str = "gianarb",
    label: str | None = LABEL,
    labels: tuple[str, ...] = (),
    number: int = 77,
) -> dict:
    payload = {
        "action": action,
        "sender": {"login": sender},
        "pull_request": {
            "number": number,
            "head": {"sha": sha, "repo": {"full_name": head_repo}},
            "base": {"repo": {"full_name": "limnion-ai/siglake"}},
            "labels": [{"name": name} for name in labels],
        },
    }
    if label is not None:
        payload["label"] = {"name": label}
    return payload


def run_fixtures(ci_text: str, authorize_text: str) -> int:
    """Prove the decision and the static checks can both go red."""
    checked = 0

    # The decision table. Each row is a state this has to get right, and the
    # first six are the states the card names.
    table = [
        (
            "an unlabeled fork pull request",
            "pull_request",
            event("opened"),
            (False, False),
        ),
        (
            "a trusted login labels the reviewed head",
            "pull_request_target",
            event("labeled"),
            (True, False),
        ),
        (
            "a new commit under a retained label",
            "pull_request_target",
            event("synchronize", sha=NEW_SHA, labels=(LABEL,)),
            (False, True),
        ),
        (
            "a new commit with no label",
            "pull_request_target",
            event("synchronize", sha=NEW_SHA),
            (False, False),
        ),
        (
            "an untrusted login adds the label",
            "pull_request_target",
            event("labeled", sender="drive-by"),
            (False, False),
        ),
        (
            "an unrelated label on a labeled-and-pushed pull request",
            "pull_request_target",
            event("labeled", label="area:docs", labels=(LABEL,)),
            (False, False),
        ),
        (
            "a trusted-team branch in this repository",
            "pull_request_target",
            event("labeled", head_repo="limnion-ai/siglake"),
            (False, False),
        ),
        (
            "a push to main",
            "push",
            {"ref": "refs/heads/main"},
            (False, False),
        ),
        (
            "the label removed again",
            "pull_request_target",
            event("unlabeled"),
            (False, False),
        ),
        (
            "a head that is not a commit",
            "pull_request_target",
            event("labeled", sha="main"),
            (False, False),
        ),
    ]
    for name, event_name, payload, (approve, revoke) in table:
        got = decide(event_name, payload, TRUSTED)
        if (got.approve, got.revoke) != (approve, revoke):
            raise AssertionError(
                f"fixture: {name} -> approve={got.approve} revoke={got.revoke}, "
                f"expected approve={approve} revoke={revoke} ({got.reason})"
            )
        if got.approve and got.sha != payload["pull_request"]["head"]["sha"]:
            raise AssertionError(f"fixture: {name} approved a commit other than the head")
        if not got.approve and got.sha:
            raise AssertionError(f"fixture: {name} emitted a sha without approving it")
        if not got.reason.strip():
            raise AssertionError(f"fixture: {name} decided without saying why")
        checked += 1

    # GitHub's expression fallback applies to unset and empty variables. A
    # non-empty typo must narrow the decision rather than widen it.
    for raw, expected in (
        (None, True),
        ("", True),
        ("gianarb", True),
        ("GianArb", True),
        ("toddpersen, gianarb", True),
        ("toddpersen", False),
        ("gianarbo", False),
    ):
        got = decide("pull_request_target", event("labeled"), trusted_logins(raw))
        if got.approve is not expected:
            raise AssertionError(
                f"fixture: trusted list {raw!r} -> approve={got.approve}, expected {expected}"
            )
        checked += 1

    # A ci:run label that released nothing is the refusal somebody is waiting
    # on, so it is annotated as a warning rather than left in the log.
    if decide("pull_request_target", event("labeled", sender="drive-by"), TRUSTED).level != "warning":
        raise AssertionError("fixture: a refused ci:run label was not raised as a warning")
    if decide("pull_request_target", event("labeled"), TRUSTED).level != "notice":
        raise AssertionError("fixture: a released run was annotated as a warning")
    checked += 2

    # Every output is one line, or the job's $GITHUB_OUTPUT stops parsing.
    for _, event_name, payload, _ in table:
        for line in decide(event_name, payload, TRUSTED).outputs():
            if "\n" in line or not re.match(r"^[a-z_]+=", line):
                raise AssertionError(f"fixture: {line!r} is not a `key=value` output line")
    checked += 1

    # Mutations of the real workflows. Each is a way this arrangement has been
    # broken in other repositories.
    if audit_ci(ci_text):
        raise AssertionError(f"the real {CI_YML} fails its own check: {audit_ci(ci_text)}")
    if audit_authorize(authorize_text):
        raise AssertionError(
            f"the real {AUTHORIZE_YML} fails its own check: {audit_authorize(authorize_text)}"
        )
    checked += 2

    for trigger in PRIVILEGED_TRIGGERS:
        mutated = ci_text.replace("  pull_request:\n", f"  pull_request:\n  {trigger}:\n", 1)
        if not any(trigger in p for p in audit_ci(mutated)):
            raise AssertionError(f"fixture: `{trigger}` added to {CI_YML}'s triggers was not reported")
        checked += 1
        # The same workflow written the other way round.
        flow = re.sub(
            r"^on:\n  push:\n    branches: \[main\]\n  pull_request:\n",
            f"on: [push, pull_request, {trigger}]\n",
            ci_text,
            count=1,
            flags=re.M,
        )
        if flow == ci_text:
            raise AssertionError("fixture: the `on:` block no longer has the shape this mutates")
        if not any(trigger in p for p in audit_ci(flow)):
            raise AssertionError(f"fixture: `{trigger}` in a flow-style `on:` was not reported")
        checked += 1

    mutated = re.sub(r"^permissions:\n  contents: read\n", "", ci_text, count=1, flags=re.M)
    try:
        problems = audit_ci(mutated)
    except ExtractionError:
        problems = ["no permissions block"]
    if not problems:
        raise AssertionError(f"fixture: {CI_YML} without `contents: read` was not reported")
    checked += 1

    mutated = ci_text.replace("          persist-credentials: false\n", "", 1)
    if not any("persist-credentials" in p for p in audit_ci(mutated)):
        raise AssertionError(f"fixture: a checkout without persist-credentials was not reported")
    checked += 1

    mutated = ci_text.replace(
        "          persist-credentials: false",
        "          persist-credentials: false\n          ref: ${{ github.event.pull_request.head.sha }}",
        1,
    )
    if not any("pins a `ref:`" in p for p in audit_ci(mutated)):
        raise AssertionError(f"fixture: a checkout pinning a ref was not reported")
    checked += 1

    # A second job asking for a write permission.
    mutated = ci_text.replace(
        "  fmt:\n", "  fmt:\n    permissions:\n      id-token: write\n", 1
    )
    if not any("job `fmt` permissions" in p for p in audit_ci(mutated)):
        raise AssertionError("fixture: a second privileged job was not reported")
    checked += 1

    # The privileged job may carry exactly one write grant, not any grant its
    # name happens to shelter.
    mutated = ci_text.replace(
        "      id-token: write\n      contents: read\n",
        "      id-token: write\n      contents: write\n",
        1,
    )
    if not any("job `docker` permissions" in p for p in audit_ci(mutated)):
        raise AssertionError("fixture: broadened docker permissions were not reported")
    checked += 1

    mutated = ci_text.replace(
        "      id-token: write\n      contents: read\n",
        "      id-token: write\n      contents: read\n      actions: write\n",
        1,
    )
    if not any("job `docker` permissions" in p for p in audit_ci(mutated)):
        raise AssertionError("fixture: an extra docker write scope was not reported")
    checked += 1

    # The event test dropped from the one line that resolves a secret.
    mutated = ci_text.replace(
        "github.event_name == 'push' && secrets.AWS_ROLE_ARN",
        "secrets.AWS_ROLE_ARN",
        1,
    )
    if not any("ECR publication is push-only" in p for p in audit_ci(mutated)):
        raise AssertionError("fixture: an ungated secret in a job header was not reported")
    checked += 1

    mutated = ci_text.replace(
        "github.event_name == 'push' && secrets.AWS_ROLE_ARN != ''",
        "github.event_name != 'pull_request' && secrets.AWS_ROLE_ARN != ''",
        1,
    )
    if not any("ECR publication is push-only" in p for p in audit_ci(mutated)):
        raise AssertionError("fixture: a broadened ECR event condition was not reported")
    checked += 1

    # The condition dropped from the step that assumes the role.
    mutated = ci_text.replace(
        "      - uses: aws-actions/configure-aws-credentials@v6\n        if: env.ECR_PUSH == 'true'\n",
        "      - uses: aws-actions/configure-aws-credentials@v6\n",
        1,
    )
    ci_problems = audit_ci(mutated)
    if not any("aws-actions/configure-aws-credentials" in p for p in ci_problems):
        raise AssertionError("fixture: an ungated role assume was not reported")
    if not any("reads a secret in a step" in p for p in ci_problems):
        raise AssertionError("fixture: the ungated role-to-assume secret was not reported")
    checked += 2

    # Mentioning ECR_PUSH is insufficient: inverted and bypassed conditions
    # must both fail, as must an image action that pushes unconditionally.
    for replacement in ("if: env.ECR_PUSH != 'true'", "if: always()"):
        mutated = ci_text.replace("if: env.ECR_PUSH == 'true'", replacement, 1)
        if not any("exact `if: env.ECR_PUSH == 'true'`" in p for p in audit_ci(mutated)):
            raise AssertionError(f"fixture: `{replacement}` on an AWS step was not reported")
        checked += 1

    for replacement in ("push: ${{ env.ECR_PUSH != 'true' }}", "push: true"):
        mutated = ci_text.replace(ECR_IMAGE_PUSH, replacement, 1)
        if not any("image push inputs" in p for p in audit_ci(mutated)):
            raise AssertionError(f"fixture: `{replacement}` on an image build was not reported")
        checked += 1

    mutated = authorize_text.replace("    types: [labeled, synchronize]\n", "")
    if not any("types:" in p for p in audit_authorize(mutated)):
        raise AssertionError("fixture: the controller without its `types:` filter was not reported")
    checked += 1

    mutated = authorize_text.replace(
        "  pull_request_target:", "  pull_request:\n  pull_request_target:", 1
    )
    if not any("triggers are" in p for p in audit_authorize(mutated)):
        raise AssertionError("fixture: a second trigger on the controller was not reported")
    checked += 1

    mutated = authorize_text.replace(
        "          persist-credentials: false",
        "          persist-credentials: false\n          ref: ${{ github.event.pull_request.head.sha }}",
        1,
    )
    if not any("pins a `ref:`" in p for p in audit_authorize(mutated)):
        raise AssertionError("fixture: the controller checking out the fork head was not reported")
    checked += 1

    mutated = authorize_text.replace(
        "vars.SIGLAKE_CI_TRUSTED_LOGINS || 'gianarb'",
        "vars.SIGLAKE_CI_TRUSTED_LOGINS",
    )
    if not any("trusted-logins resolution" in p for p in audit_authorize(mutated)):
        raise AssertionError("fixture: removing the trusted-login fallback was not reported")
    checked += 1

    mutated = authorize_text.replace("        run: |", "        run: cargo build\n        run: |", 1)
    if not any("builds nothing" in p for p in audit_authorize(mutated)):
        raise AssertionError("fixture: a cargo invocation in the controller was not reported")
    checked += 1

    for script in (CHECKER, APPROVE_SH):
        mutated = "\n".join(
            line for line in authorize_text.splitlines() if str(script) not in line
        )
        if not any(str(script) in p for p in audit_authorize(mutated)):
            raise AssertionError(f"fixture: the controller not running {script} was not reported")
        checked += 1

    # Absence is never silence.
    for name, text, extract in (
        ("a workflow with no `on:` block", "jobs:\n  a:\n    steps: []\n", lambda t: block(t, "on")),
        ("a workflow with no checkout", "on:\n  push:\njobs:\n  a:\n", checkout_steps),
    ):
        try:
            extract(text)
        except ExtractionError:
            checked += 1
        else:
            raise AssertionError(f"fixture: {name} extracted without an error")

    return checked


# --- entry point -------------------------------------------------------------


def annotate(message: str, path: pathlib.PurePath) -> None:
    if os.environ.get("GITHUB_ACTIONS"):
        print(f"::error file={path}::{message}")


def main(argv: list[str]) -> int:
    root = pathlib.Path(__file__).resolve().parent.parent

    if "--decide" in argv:
        try:
            event_name, payload = read_event(os.environ.get("GITHUB_EVENT_PATH"))
        except (ExtractionError, OSError, json.JSONDecodeError) as e:
            print(f"FAIL {e}", file=sys.stderr)
            return 1
        decision = decide(event_name, payload, trusted_logins(os.environ.get("CI_TRUSTED_LOGINS")))
        for line in decision.outputs():
            print(line)
        # The reason goes to stderr so it is readable whether or not stdout is
        # being collected into $GITHUB_OUTPUT.
        print(decision.reason, file=sys.stderr)
        return 0

    ci_text = (root / CI_YML).read_text()
    authorize_text = (root / AUTHORIZE_YML).read_text()

    try:
        fixtures = run_fixtures(ci_text, authorize_text)
        problems = audit_ci(ci_text) + audit_authorize(authorize_text)
    except (ExtractionError, AssertionError) as e:
        print(f"FAIL {e}", file=sys.stderr)
        annotate(str(e), CHECKER)
        return 1

    if problems:
        for problem in problems:
            print(f"FAIL {problem}", file=sys.stderr)
            annotate(problem, CI_YML if str(CI_YML) in problem else AUTHORIZE_YML)
        return 1

    print(f"ok   fork pull requests run one reviewed commit; {fixtures} fixtures")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
