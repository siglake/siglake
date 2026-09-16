#!/usr/bin/env bash
#
# Build the tree that gets published: one orphan commit with the private paths
# removed, in a throwaway worktree, never on main. Prints the source commit, the
# exclusions applied and the full tracked-file list, then runs
# scripts/check-public-tree.py on the result.
#
#   scripts/make-public-tree.sh                     # from HEAD into $(mktemp -d)
#   scripts/make-public-tree.sh --ref main --gate   # what launch runs
#
# Options:
#   --ref REF      commit to publish (default HEAD). Only committed content is
#                  read; uncommitted changes in this checkout never ship.
#   --out DIR      where to create the worktree (default: a fresh mktemp -d).
#                  Must be outside this repository and empty or absent.
#   --branch NAME  orphan branch to create (default public-main). An existing
#                  branch of that name is refused unless --force, because a
#                  branch left over from an earlier run is exactly the stale
#                  tree that gets published by accident.
#   --force        delete an existing --branch first.
#   --gate         after building, run scripts/ci-local.sh --strict in the
#                  output tree. This checks the same non-heavy jobs as CI and
#                  treats a missing tool or otherwise skipped job as a failure.
#
# This script never pushes. Publishing is a separate, deliberate act:
#   git push <public-remote> public-main:main   # then tag
#
# Why a worktree and not `git checkout --orphan` in the development checkout:
# the orphan checkout replaces the index in place, and the working tree is then
# one `git add -A` away from committing the deletion of every private path onto
# whatever branch is checked out next. A worktree shares objects and refs with
# this repository (so the branch is visible here for the push) but has its own
# index and working tree, and is deleted when done.
#
# The exclusion list is read from scripts/check-public-tree.py, the checker CI
# runs on every commit. The squash and the checker therefore cannot disagree
# about what ships, which is the only thing that makes the checker's "the tree
# that ships carries no dead references" claim hold. Until 2026-09 the recipe
# lived in prose in LAUNCH.md (internal) with the list duplicated there.
#
# Removing a workspace member leaves its entry, and any dependency only it
# pulled, in Cargo.lock. `cargo update --workspace` drops them without moving
# any other version. This step matters: deploy/Dockerfile and the OpenAPI
# freshness job build with --locked, which refuses a lock that needs updating,
# so a public tree with the stale lock fails its first image build.

set -euo pipefail
cd "$(dirname "$0")/.."
repo=$(pwd -P)

die() { echo "make-public-tree: $*" >&2; exit 1; }

ref=HEAD out="" branch=public-main force=0 gate=0
while [ $# -gt 0 ]; do
  case "$1" in
    --ref) ref=${2:?--ref needs a value}; shift 2 ;;
    --out) out=${2:?--out needs a value}; shift 2 ;;
    --branch) branch=${2:?--branch needs a value}; shift 2 ;;
    --force) force=1; shift ;;
    --gate) gate=1; shift ;;
    -h|--help) sed -n '3,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

command -v python3 >/dev/null || die "python3 is required (the exclusion list is read from the checker)"
command -v cargo >/dev/null || die "cargo is required (Cargo.lock is re-synced after the member removal)"

sha=$(git rev-parse --verify --quiet "${ref}^{commit}") || die "not a commit: $ref"

# --- the exclusion list, from the checker ----------------------------------
# Importing the checker as a module would write scripts/__pycache__/ into the
# development checkout; the env var stops that.
mapfile -t excluded < <(PYTHONDONTWRITEBYTECODE=1 python3 - "$repo/scripts/check-public-tree.py" <<'PY'
import importlib.util, sys
spec = importlib.util.spec_from_file_location("check_public_tree", sys.argv[1])
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
print("\n".join(mod.EXCLUDED))
PY
)
[ "${#excluded[@]}" -gt 0 ] || die "read an empty exclusion list from scripts/check-public-tree.py"
for p in "${excluded[@]}"; do
  case "$p" in
    ''|/*|*..*|*/) die "refusing exclusion entry '$p': must be a relative path with no '..' and no trailing slash" ;;
  esac
done

# --- where the tree is built ------------------------------------------------
if [ -z "$out" ]; then
  out=$(mktemp -d -t siglake-public.XXXXXX)
else
  out=$(realpath -m -- "$out")
  if [ -e "$out" ] && [ -n "$(ls -A -- "$out" 2>/dev/null)" ]; then
    die "--out $out exists and is not empty"
  fi
fi
case "$out" in
  "$repo"|"$repo"/*) die "--out must be outside the repository ($repo): $out" ;;
esac

if git show-ref --verify --quiet "refs/heads/$branch"; then
  if [ "$force" = 1 ]; then
    git branch -q -D "$branch"
  else
    die "branch '$branch' already exists, cut from an earlier run ($(git rev-parse --short "$branch")). \
Pass --force to rebuild it; do not publish the old one."
  fi
fi

# --- build ------------------------------------------------------------------
git worktree add -q --detach -- "$out" "$sha"
g() { git -C "$out" "$@"; }

on_fail() {
  echo "make-public-tree: FAILED; the partial worktree is left at $out for inspection." >&2
  echo "  clean up with: git worktree remove --force $out; git branch -D $branch 2>/dev/null" >&2
}
trap on_fail ERR

g checkout -q --orphan "$branch"
for p in "${excluded[@]}"; do
  g rm -r -q --cached --ignore-unmatch -- "$p"
  rm -rf -- "$out/$p"
  case "$p" in
    crates/*)
      # The member line, and nothing else: a workspace.dependencies entry or a
      # path dep would mean the public workspace still needs the crate, which is
      # a real problem to fix in the source tree, not paper over here.
      sed -i "\|^[[:space:]]*\"$p\",[[:space:]]*\$|d" "$out/Cargo.toml"
      if grep -q -- "$p" "$out/Cargo.toml"; then
        die "Cargo.toml still references $p after its members line was removed; the public workspace depends on it"
      fi
      ;;
  esac
done

# Re-sync Cargo.lock to the smaller workspace. --offline first (no version can
# move, and no network on the launch machine); fall back to online only if the
# local registry cache is missing.
(cd "$out" && { cargo update -q --workspace --offline 2>/dev/null || cargo update -q --workspace; })
# Prove the lock is exactly what --locked builds will accept.
(cd "$out" && cargo metadata --locked --offline --format-version 1 >/dev/null) \
  || die "Cargo.lock is not --locked-clean after the member removal"

version=$(sed -n 's/^version = "\(.*\)"$/\1/p' "$out/Cargo.toml" | head -1)
[ -n "$version" ] || die "could not read workspace.package.version from Cargo.toml"
g add -A
g commit -q -m "siglake v$version"

# --- verify -----------------------------------------------------------------
[ "$(g rev-list --count HEAD)" = 1 ] || die "expected exactly one commit on $branch"
for p in "${excluded[@]}"; do
  [ ! -e "$out/$p" ] || die "$p still exists on disk in the public tree"
  [ -z "$(g ls-files -- "$p")" ] || die "$p is still tracked in the public tree"
done
[ -z "$(g status --porcelain)" ] || die "the public worktree is not clean after the commit"

trap - ERR

# --- report -----------------------------------------------------------------
echo "source:   $ref = $sha  $(git log -1 --format=%s "$sha")"
if ! git merge-base --is-ancestor "$sha" main 2>/dev/null; then
  echo "note:     $ref is NOT on main; a launch build must come from main" >&2
fi
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "note:     this checkout has uncommitted changes; they are not in the public tree" >&2
fi
echo "branch:   $branch = $(g rev-parse --short HEAD)  \"siglake v$version\""
echo "worktree: $out"
echo "excluded: ${excluded[*]}"
echo "tracked files: $(g ls-files | wc -l)"
g ls-files | sed 's/^/  /'
echo
(cd "$out" && python3 scripts/check-public-tree.py)

# --- gate -------------------------------------------------------------------
if [ "$gate" = 1 ]; then
  echo
  echo "gate: scripts/ci-local.sh --strict in $out (all non-heavy CI jobs)"
  (cd "$out" && scripts/ci-local.sh --strict)
  echo "gate: green"
else
  echo
  echo "gate not run; either re-run with --gate or, in $out:"
  echo "  scripts/ci-local.sh --strict"
fi

echo
echo "when done with the tree:"
echo "  git worktree remove --force $out && git branch -D $branch"
