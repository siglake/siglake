#!/usr/bin/env python3
"""Check the tree that will actually be published.

The launch squash drops a fixed set of paths (see EXCLUDED below, which must
match the orphan-branch recipe in docs/internal/LAUNCH.md). Everything else
ships. Five ways a file that ships can carry something it should not, all of
which are invisible in normal development because the excluded paths are present
locally:

  1. A local absolute path — `/home/<user>/...`, `~/workspace/...`. It leaks a
     filesystem layout and, worse, the existence and location of private repos.

  2. A citation to a private repo. `~/workspace/compare` is the competitive
     analysis corpus; two public design docs cited it by local path, one of them
     reproducing its survey.

  3. A reference to a closed-source repository. Naming one sends a reader to a
     404 and advertises a commercial component as part of the open core.

  4. A reference from a public file INTO an excluded path — docs/internal/,
     docs/archived/, bench/, CLAUDE.md — or to an excluded Markdown file by
     basename. Those files do not exist after the squash, so the reference is
     dead on arrival. One public design doc pointed at a plan that lives in
     docs/internal/.

  5. A docs/<name>.md path that is not among the files that ship. A moved note
     can otherwise leave a plausible-looking path which is a 404 in the public
     tree.

  6. Source metadata naming a repository other than the published one. The
     manifest's `repository` and the charts' `home`/`sources` shipped with the
     pre-launch organization, which sends crates.io and Artifact Hub readers to
     an archived repo (and, for the full-history mirror, a private one).

Competitor NAMES are deliberately not checked. This project publishes
head-to-head benchmarks on purpose — an eight-engine board, an intentional
CHANGELOG reference — so a named engine is normal. What is not publishable is a
private-source citation or a refuted premise, which is a judgment no grep makes.

Run before the squash, and in CI so drift is caught while it is one line.
"""

from __future__ import annotations

import pathlib
import re
import sys

# The one place the squash's exclusion list lives: scripts/make-public-tree.sh
# imports this module and removes exactly these paths, so what it publishes and
# what this checker vets cannot drift apart. Edit here, and only here.
EXCLUDED = [
    "docs/internal",
    "docs/archived",
    "CLAUDE.md",
    "bench",
    ".claude",
    "crates/siglake-bench",
]

# Vendored upstream code: we carry deliberate divergence there and do not
# rewrite it for style. Keep one prefix per crate so third_party/README.md and
# any future non-vendored files under third_party/ are still vetted.
SKIP_PREFIXES = [
    "third_party/iceberg",
    "third_party/iceberg-catalog-sql",
    "third_party/iceberg-storage-opendal",
]

LOCAL_PATH = re.compile(r"(/home/[a-z][a-z0-9_-]*/|~/workspace/|~/\.siglake)")

# PATH-shaped references only. An earlier version matched bare words and
# reported "a bench node" in prose and "CLAUDE.md policy" in a vendored
# competitor checkout — noise that would have got the whole check ignored. A
# directory only counts with its trailing slash. The filename rule built below
# is similarly narrow: it matches excluded Markdown filenames, not bare words
# such as "bench". The leading boundary matters: without it `bench/` matched
# the index name in `wal-mirror/acme/logs-bench/`.
DEAD_REF = re.compile(
    r"(?<![\w/-])(docs/internal/|docs/archived/|bench/|\.claude/|crates/siglake-bench|CLAUDE\.md)"
)

DOCS_MD_REF = re.compile(r"(?<![\w-])(docs/[A-Za-z0-9][A-Za-z0-9_./-]*\.md)(?!\w)")
DATED_MD_STEM = re.compile(r"_20\d\d-\d\d-\d\d")

# Repositories that stay closed. Naming one from the open tree sends a reader to
# a 404 and, worse, advertises a commercial component as though it were part of
# the open core.
#
# `siglake-detection` is deliberately and permanently closed — running the
# detection layer is part of the commercial offering. The open core exposes the
# INTERFACE it consumes (`siglake_wal::consumer`), which is the thing an OSS
# user needs; the pipeline itself is not theirs to fetch.
#
# Deliberately narrow. `siglake-benchmarks` is also private today but is
# intended to go public, so it is NOT listed — a guard that fires on something
# about to become correct is a guard people delete.
CLOSED_SOURCE = re.compile(r"siglake-detection")

# The engine's own repository, settled by the public push to
# github.com/siglake/siglake. Source metadata that names any other owner of a
# repo called `siglake` points at the pre-launch organization's archived copy or
# at the private full-history mirror.
#
# Narrow on purpose, because the pre-launch organization legitimately owns plenty
# of other things this must not touch:
#
#   * `github.com/<org>/siglake-benchmarks`, `.../siglake.dev` — different
#     repositories, public under the old owner. The closing boundary below stops
#     the match at `siglake`, so a longer repo name never matches.
#   * `ghcr.io/<org>/siglake` — package publication authority, not source. Only
#     `github.com` host matches, so image references are untouched.
#   * `github.com/<org>` with no repo path — a maintainer's profile, which is an
#     identity and not an engine-source link.
#   * `siglake.limnion.ai` — the CRD API group. Not a URL, never matched.
CANONICAL_ENGINE_REPO = "siglake/siglake"
ENGINE_REPO_URL = re.compile(
    r"github\.com[/:]([A-Za-z0-9][A-Za-z0-9._-]*/siglake)(?:\.git)?(?![\w.-])"
)

# deploy/helm/siglake-operator/Chart.yaml carries the same `home`/`sources`
# defect. Correcting it belongs to the operator chart's own metadata task
# (#4856), so it is exempt here rather than fixed under a data-plane card. The
# exemption is checked for being still necessary: once that chart names the
# published repository, this set has to shrink, so the hole cannot outlive the
# defect it was opened for.
ENGINE_REPO_EXEMPT = {"deploy/helm/siglake-operator/Chart.yaml"}

# The squash deletes crates/siglake-bench AND `sed -i`s its workspace-members
# line out of Cargo.toml, so that one reference is handled by construction.
#
# scripts/check-claude-md.sh caps the session brief, which the squash also
# removes. It exists to name that file and the status log its entries move to,
# and it skips (exit 0) when the brief is absent, so in the published tree
# neither reference is ever followed. Exempted per (file, token), not by file:
# a local path or a closed-source name in that script still fails. ci-local.sh
# and ci.yml -- long, prose-heavy, and shipped -- call the script rather than
# naming the brief, so neither earns an exemption a future comment could hide in.
# make-public-tree.sh likewise names LAUNCH.md only to explain where its own
# executable exclusion list replaced the old prose recipe.
#
# scripts/check-bench-ports.sh is the same shape: it guards the port knobs of
# the four loopback bench scripts, which the squash removes with the rest of
# bench/, and skips (exit 0) when they are absent. Exempted per (file, token)
# for the same reason -- ci-local.sh and ci.yml call it rather than naming
# bench/ themselves.
#
# scripts/check-jaeger-ui-recording.sh (#2311) is the third of that shape: it
# drives bench/jaeger-ui-recording's extractor and proxy offline, and skips
# (exit 0) when that directory is absent.
EXPECTED = {
    ("Cargo.toml", "crates/siglake-bench"),
    ("scripts/check-bench-ports.sh", "bench/"),
    ("scripts/check-jaeger-ui-recording.sh", "bench/"),
    ("scripts/check-claude-md.sh", "CLAUDE.md"),
    ("scripts/check-claude-md.sh", "docs/internal/"),
    ("scripts/check-claude-md.sh", "STATUS_LOG_2026-06_to_09.md"),
    ("scripts/make-public-tree.sh", "LAUNCH.md"),
}

# This file necessarily contains every pattern it searches for — the exclusion
# list and the regexes ARE the patterns. Exempted by exact path, deliberately
# not by directory or glob: `scripts/` holds shipping code, and a hole shaped
# like "anything under scripts/" is how a real leak would get through.
SELF = "scripts/check-public-tree.py"

TEXTUAL = {
    ".rs",
    ".md",
    ".toml",
    ".tf",
    ".yaml",
    ".yml",
    ".json",
    ".sh",
    ".py",
    ".tpl",
    ".txt",
}


def foreign_engine_repos(line: str) -> list[str]:
    """Owners other than the published one for a repository called `siglake`."""
    return [
        m.group(1)
        for m in ENGINE_REPO_URL.finditer(line)
        if m.group(1) != CANONICAL_ENGINE_REPO
    ]


# Negative fixtures for rule 6, kept to engine source metadata: the three field
# shapes that shipped wrong, and the neighbouring URLs which are correct as they
# stand and must not be swept up with them.
ENGINE_REPO_RED = [
    'repository = "https://github.com/example-invalid/siglake"',
    "home: https://github.com/example-invalid/siglake",
    "  - git@github.com:example-invalid/siglake.git",
]
ENGINE_REPO_GREEN = [
    'repository = "https://github.com/siglake/siglake"',
    "  - https://github.com/siglake/siglake",
    "benchmarks live at https://github.com/limnion-ai/siglake-benchmarks",
    "the site is https://github.com/limnion-ai/siglake.dev",
    "  - name: limnion-ai",
    "    url: https://github.com/limnion-ai",
    "  repository: ghcr.io/example-invalid/siglake",
    'image: "ghcr.io/example-invalid/siglake:0.1.0"',
    '    group = "siglake.limnion.ai",',
    "2026-07-24: chose the limnion-ai organization; the launch superseded it",
]


def run_fixtures() -> int:
    """Prove rule 6 goes red on the fields that shipped wrong, and only those."""
    for line in ENGINE_REPO_RED:
        if not foreign_engine_repos(line):
            raise AssertionError(f"fixture: source metadata passed: {line!r}")
    for line in ENGINE_REPO_GREEN:
        if found := foreign_engine_repos(line):
            raise AssertionError(f"fixture: reported {found} in: {line!r}")
    return len(ENGINE_REPO_RED) + len(ENGINE_REPO_GREEN)


def shipping_files(root: pathlib.Path) -> list[pathlib.PurePath]:
    """The files git tracks, minus what the squash removes.

    `git ls-files`, not a filesystem walk: the published tree is derived from
    git, and the working directory holds untracked and ignored things that are
    never published — a 92 MB `compare/` checkout of another engine, ignored via
    .gitignore, which a filesystem walk dutifully reported on.
    """
    import subprocess

    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=root, capture_output=True, text=True, check=True
    ).stdout
    files = []
    for name in out.split("\0"):
        if not name:
            continue
        rel = pathlib.PurePath(name)
        if any(
            rel.parts[: len(pathlib.PurePath(prefix).parts)]
            == pathlib.PurePath(prefix).parts
            for prefix in SKIP_PREFIXES
        ):
            continue
        if any(
            rel.parts[: len(pathlib.PurePath(ex).parts)] == pathlib.PurePath(ex).parts
            for ex in EXCLUDED
        ):
            continue
        files.append(rel)
    return files


def excluded_markdown_names(
    root: pathlib.Path, shipping: list[pathlib.PurePath]
) -> re.Pattern[str] | None:
    """Return exact excluded Markdown names which do not also ship.

    README.md exists below two excluded directories and throughout the public
    tree; treating that basename as private would make every normal README
    citation fail. Dated document stems are also distinctive references (for
    example MORNING_QUEUE_2026-08-31), even when prose omits the .md suffix.
    """
    import subprocess

    out = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    shipping_basenames = {rel.name for rel in shipping if rel.suffix == ".md"}
    names: set[str] = set()
    for name in out.split("\0"):
        if not name:
            continue
        rel = pathlib.PurePath(name)
        if rel.suffix != ".md" or rel.name in shipping_basenames:
            continue
        if not any(
            len(rel.parts) > len(pathlib.PurePath(ex).parts)
            and rel.parts[: len(pathlib.PurePath(ex).parts)]
            == pathlib.PurePath(ex).parts
            for ex in EXCLUDED
        ):
            continue
        names.add(rel.name)
        if DATED_MD_STEM.search(rel.stem):
            names.add(rel.stem)

    if not names:
        return None
    alternatives = "|".join(
        re.escape(name) for name in sorted(names, key=len, reverse=True)
    )
    return re.compile(rf"(?<!\w)({alternatives})(?!\w)")


def main() -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    problems: list[str] = []
    checked = 0
    fixtures = run_fixtures()
    shipping = shipping_files(root)
    shipping_names = {rel.as_posix() for rel in shipping}
    excluded_md_ref = excluded_markdown_names(root, shipping)
    engine_repo_exempt_used: set[str] = set()

    for rel in shipping:
        path = root / rel
        if path.suffix not in TEXTUAL or not path.is_file():
            continue
        if str(rel) == SELF:
            continue
        checked += 1
        try:
            text = path.read_text()
        except (OSError, UnicodeDecodeError):
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            if m := LOCAL_PATH.search(line):
                problems.append(
                    f"{rel}:{lineno} local path `{m.group(1)}` — leaks a filesystem "
                    f"layout, and often a private repo's location: {line.strip()[:110]}"
                )
            if m := CLOSED_SOURCE.search(line):
                problems.append(
                    f"{rel}:{lineno} names `{m.group(0)}`, a closed-source repository — "
                    f"the open tree must not link or name it: {line.strip()[:110]}"
                )
            for owner in foreign_engine_repos(line):
                if rel.as_posix() in ENGINE_REPO_EXEMPT:
                    engine_repo_exempt_used.add(rel.as_posix())
                    continue
                problems.append(
                    f"{rel}:{lineno} source metadata names `{owner}`, not the "
                    f"published repository `{CANONICAL_ENGINE_REPO}` — crates.io "
                    f"and Artifact Hub readers follow this: {line.strip()[:110]}"
                )
            dead_ref_reported = False
            for m in DEAD_REF.finditer(line):
                if (str(rel), m.group(1)) in EXPECTED:
                    continue
                problems.append(
                    f"{rel}:{lineno} references `{m.group(1)}`, which the squash "
                    f"removes — the reference is dead in the published tree: "
                    f"{line.strip()[:110]}"
                )
                dead_ref_reported = True
                break
            if dead_ref_reported:
                continue
            docs_ref_reported = False
            for m in DOCS_MD_REF.finditer(line):
                token = m.group(1)
                basename = pathlib.PurePath(token).name
                if token in shipping_names or (str(rel), basename) in EXPECTED:
                    continue
                problems.append(
                    f"{rel}:{lineno} references `{token}`, which is not a file "
                    f"in the published tree: {line.strip()[:110]}"
                )
                docs_ref_reported = True
                break
            if docs_ref_reported:
                continue
            if excluded_md_ref:
                for m in excluded_md_ref.finditer(line):
                    token = m.group(1)
                    if (str(rel), token) in EXPECTED:
                        continue
                    problems.append(
                        f"{rel}:{lineno} references excluded document `{token}` — "
                        f"the reference is dead in the published tree: "
                        f"{line.strip()[:110]}"
                    )
                    break

    for stale in sorted(ENGINE_REPO_EXEMPT - engine_repo_exempt_used):
        problems.append(
            f"{stale} no longer names another owner of `siglake`, so its entry in "
            f"ENGINE_REPO_EXEMPT is a hole with nothing behind it — drop it"
        )

    if problems:
        for p in problems:
            print(f"FAIL {p}", file=sys.stderr)
        print(
            f"\n{len(problems)} problem(s) in the tree that ships "
            f"({checked} files checked).",
            file=sys.stderr,
        )
        return 1
    print(
        f"ok   {checked} shipping files carry no local paths, dead references or "
        f"foreign source metadata; {fixtures} fixtures"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
