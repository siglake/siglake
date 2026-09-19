#!/usr/bin/env bash
#
# Run the vendored forks' own `#[cfg(test)]` unit tests, and the doctests of the
# two forks that have any.
#
# The three forks under third_party/ reach the build only through the root
# manifest's `[patch.crates-io]`, so `cargo test --workspace` compiles them as
# ordinary dependencies -- without `--cfg test`, and without ever touching
# their test modules. Task #2651 found what that costs: the
# iceberg fork's lib-test target had not compiled for some time (a missing
# `ParquetReadOptions` import), one test asserted something false about the read
# path, and three asserted on wall-clock ordering, two of which failed. CI saw
# none of it. third_party/README.md's "Running the fork's own unit tests" wrote
# the recipe down; this script is the recipe, so that the rot cannot come back
# with the next fork edit.
#
#   scripts/check-fork-tests.sh                  # all three; scripts/ci-local.sh's `fork-tests` job
#   scripts/check-fork-tests.sh --fork iceberg   # one of them
#   scripts/check-fork-tests.sh --fork-src iceberg=DIR   # test DIR instead of the fork's src/
#   scripts/check-fork-tests.sh --scratch-base DIR       # build the mirror under DIR
#   scripts/check-fork-tests.sh --keep           # leave the mirror behind and name it
#
# One `ok <fork> <n> tests` line per fork on stdout (plus `, <n> doctests` for a
# fork whose doctests are gated) and a final `ok   <forks> forks, <n> unit
# tests`. A fork that fails prints `FAIL ...` on stderr followed by all of
# cargo's output, so a caller that redirects both keeps the failing test's name;
# a green fork's output is summarised by its count rather than reprinted.
#
# The forks are not workspace members and cannot be tested in place (cargo
# refuses `--manifest-path` into a patched path dependency), so each is tested
# from a disposable mirror of symlinks. Two properties of that mirror cost an
# hour each to rediscover in #2651; both are enforced below rather than left as
# prose:
#
#   * The mirror must not sit inside a cargo workspace, which rules out
#     anywhere under this checkout, `target/` included. Cargo then resolves
#     `crates/siglake-bloom` twice, once through the mirror's symlink and once
#     through the enclosing checkout, and the run dies with a package collision
#     in the lockfile. scratch_base_ok() below refuses such a base, which is why
#     $TMPDIR is only used when it is safe (ci-local.sh points TMPDIR at its own
#     run directory under CARGO_TARGET_DIR, i.e. inside a checkout).
#   * The mirror needs the repo root's Cargo.toml, crates/, third_party/ and
#     rust-toolchain.toml symlinked, and each fork copy two directory levels
#     down, because the forks' path dependencies on `../../crates/siglake-{bloom,
#     index}` inherit `workspace.package` from that root manifest.
#
# Only the manifest is copied, never edited in place. `[workspace]` is appended
# so the copy is its own workspace root, and the copy then needs back whatever
# the enclosing workspace used to supply:
#
#   * iceberg-catalog-sql and iceberg-storage-opendal both depend on
#     `iceberg = "0.10.0"`, which outside the root manifest's patch comes from
#     crates.io -- the job would then test upstream storage or upstream's
#     catalog and prove nothing. Their copies get a `[patch.crates-io]` entry
#     pointing `iceberg` at the owned fork, and the resolved lockfile is
#     checked afterwards to make sure the patch took.
#   * iceberg-storage-opendal takes `metrics` with `workspace = true` (the
#     siglake write-path gauges), which a standalone manifest cannot resolve at
#     all. Its copy gets a `[workspace.dependencies]` block carrying the root
#     manifest's own line for it.
#
# `--doc` runs for the forks in DOC_MIN_TESTS below: iceberg and, since #2784,
# iceberg-catalog-sql. Neither could run standalone at first, and for the same
# reason. Five of the iceberg fork's doctests need `#[tokio::main]`, whose
# default flavour wants `rt-multi-thread`, and its manifest asked tokio only for
# `sync` and `rt` (#2671); iceberg-catalog-sql's single crate-root example is
# also `#[tokio::main]`, and its dev-dependency on tokio named no features at
# all, so the example compiled only where something else in the graph turned
# them on. Both manifests now carry an explicit `[dev-dependencies.tokio]`.
# The doctests cost ~5 s every run between them: rustdoc rebuilds the merged
# doctest binary, there is no fingerprint to keep warm.
#
# iceberg-storage-opendal has no doctests -- not one fenced example anywhere
# under its src/, measured in #2784 -- so it stays `--lib` only and gets no
# entry rather than a floor of 0, which would assert nothing. A doc example
# added to it wants a DOC_MIN_TESTS entry with it.
#
# Both targets run with each fork's default features -- what siglake ships.
# Nothing here runs rustfmt over the forks: they carry upstream's formatting
# settings, which are not vendored (third_party/README.md).
#
# Build artifacts go to the caller's CARGO_TARGET_DIR, which is where the warm
# ones already are; when the caller has none, this points cargo at the
# checkout's own target/ so a disposable mirror never becomes a from-scratch
# build that is thrown away.

set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)

# Fork -> the minimum number of tests that must run. A filter, feature or
# mirror mistake that selects nothing exits 0 and prints "0 passed", which
# reads like success; the adopted 0.10.1 forks count 1410, 37 and 8.
declare -A MIN_TESTS=(
  [iceberg]=1000
  [iceberg-catalog-sql]=37
  # Resolver, upload-permit and memory-operator tests. src/azdls.rs's module
  # is behind `opendal-azdls`, which is not in `default`, so it is not in this
  # count and this job does not compile it.
  [iceberg-storage-opendal]=8
)
# Fork -> the minimum number of doctests that must pass, for the forks whose
# doctest target is gated. A fork absent from this table is `--lib` only. The
# floor is the same guard as MIN_TESTS: `cargo test --doc` on a crate with no
# doctests prints "0 passed" and exits 0, so a mirror that stopped selecting
# any of them would read as success. #2671 measured 85 passed and 9 ignored.
declare -A DOC_MIN_TESTS=(
  [iceberg]=80
  # The one example in src/lib.rs's crate-level docs, `no_run`, so rustdoc
  # compiles it and reports it as passed without connecting to anything. There
  # is no margin to leave: #2784 measured 1 passed, 0 ignored.
  [iceberg-catalog-sql]=1
)
# Fork -> crates.io packages the mirror must resolve to the owned fork instead.
declare -A PATCHED=(
  [iceberg-catalog-sql]=iceberg
  [iceberg-storage-opendal]=iceberg
)
# Fork -> dependencies its manifest inherits with `workspace = true`. The
# mirror's manifest copy is its own workspace root, so it has to carry a
# [workspace.dependencies] entry for each of them or cargo refuses to read it.
# The entry is lifted from this checkout's root manifest below rather than
# written out here, so the mirror pins what the workspace pins.
declare -A INHERITED=(
  [iceberg-storage-opendal]=metrics
)
ALL_FORKS=(iceberg iceberg-catalog-sql iceberg-storage-opendal)

declare -A SRC_OVERRIDE=()
FORKS=()
SCRATCH_BASE=
KEEP=0

usage() { sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
  case "$1" in
    --fork)
      [ $# -ge 2 ] || { echo "--fork needs a fork name" >&2; exit 2; }
      FORKS+=("$2"); shift ;;
    --fork-src)
      [ $# -ge 2 ] || { echo "--fork-src needs NAME=DIR" >&2; exit 2; }
      case "$2" in
        *=*) SRC_OVERRIDE[${2%%=*}]=${2#*=} ;;
        *) echo "--fork-src wants NAME=DIR, got: $2" >&2; exit 2 ;;
      esac
      shift ;;
    --scratch-base)
      [ $# -ge 2 ] || { echo "--scratch-base needs a directory" >&2; exit 2; }
      SCRATCH_BASE=$2; shift ;;
    --keep) KEEP=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

[ "${#FORKS[@]}" -gt 0 ] || FORKS=("${ALL_FORKS[@]}")
for fork in "${FORKS[@]}" "${!SRC_OVERRIDE[@]}"; do
  [ -n "${MIN_TESTS[$fork]+set}" ] || { echo "not a vendored fork: $fork" >&2; exit 2; }
done

# A base is usable only if nothing between it and / is a cargo package or
# workspace: see the package-collision note above.
scratch_base_ok() { # <dir>
  local dir=$1 real
  [ -d "$dir" ] && [ -w "$dir" ] || return 1
  real=$(cd "$dir" && pwd -P) || return 1
  while :; do
    [ -e "$real/Cargo.toml" ] && return 1
    [ "$real" = / ] && break
    real=$(dirname "$real")
  done
  return 0
}

if [ -n "$SCRATCH_BASE" ]; then
  if ! scratch_base_ok "$SCRATCH_BASE"; then
    echo "FAIL --scratch-base $SCRATCH_BASE is missing, unwritable, or inside a cargo workspace" >&2
    exit 1
  fi
  base=$SCRATCH_BASE
else
  base=
  for candidate in "${TMPDIR:-}" /tmp; do
    [ -n "$candidate" ] || continue
    if scratch_base_ok "$candidate"; then base=$candidate; break; fi
  done
  if [ -z "$base" ]; then
    echo "FAIL no scratch directory outside a cargo workspace (tried \$TMPDIR and /tmp);" \
      "name one with --scratch-base" >&2
    exit 1
  fi
fi

# The mirror is removed on exit but its PATH is derived from what is being
# tested, and that is the difference between a 46-second job and a 3-second
# one. A path dependency's absolute path goes into cargo's unit hash, so a
# mktemp mirror is a from-scratch build of the fork and everything under it,
# every run. Recreating the same path with the same file mtimes -- hence the
# `touch -r` on each copied manifest below -- leaves the fingerprints in
# CARGO_TARGET_DIR fresh, and a re-run that changed nothing recompiles nothing.
# The `--fork-src` overrides go into the name too, so a failure-path fixture
# does not invalidate the gate's warm artifacts.
mirror_key() {
  local fork
  {
    printf '%s\n' "$ROOT"
    for fork in $(printf '%s\n' "${!SRC_OVERRIDE[@]}" | sort); do
      printf '%s=%s\n' "$fork" "${SRC_OVERRIDE[$fork]}"
    done
  } | sha256sum | cut -c1-12
}
MIRROR="$base/siglake-fork-tests-$(mirror_key)"

# Two runs from the same checkout would otherwise share that path, and the one
# that finished first would delete the mirror out from under the other. flock
# serialises them; without flock the path has to be unique instead, at the cost
# of a full rebuild.
if command -v flock >/dev/null 2>&1; then
  exec 9>"$MIRROR.lock" || exit 1
  if ! flock -w 1800 9; then
    echo "FAIL another check-fork-tests.sh has held $MIRROR.lock for 30 minutes" >&2
    exit 1
  fi
else
  MIRROR=$(mktemp -d "$base/siglake-fork-tests.XXXXXXXX") || exit 1
fi

cleanup() {
  if [ "$KEEP" = 1 ]; then
    echo "  mirror kept: $MIRROR" >&2
  else
    # Every directory entry below the fork copies is a symlink, so this removes
    # the mirror and nothing it points at. The build artifacts are in
    # CARGO_TARGET_DIR and survive, as does the empty .lock file.
    rm -rf -- "$MIRROR"
  fi
}
trap cleanup EXIT
# A mirror left behind by a --keep run or a killed one is this checkout's own,
# by construction of the name, and is rebuilt from scratch below.
rm -rf -- "$MIRROR"
# `mkdir` and `ln` report what went wrong themselves (a full tmpfs, most
# likely); the FAIL line is here so the gate's excerpt names the mirror too.
mirror_setup_failed() {
  echo "FAIL could not build the mirror at $MIRROR; see the error above" >&2
  exit 1
}
mkdir -p "$MIRROR" || mirror_setup_failed

export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$ROOT/target}

ln -s "$ROOT/Cargo.toml" "$MIRROR/Cargo.toml" || mirror_setup_failed
ln -s "$ROOT/crates" "$MIRROR/crates" || mirror_setup_failed
ln -s "$ROOT/third_party" "$MIRROR/third_party" || mirror_setup_failed
ln -s "$ROOT/rust-toolchain.toml" "$MIRROR/rust-toolchain.toml" || mirror_setup_failed

# The resolved `iceberg` must be the fork, not the registry's 0.10 release: a package
# resolved from a registry carries a `source =` line, a path dependency does not.
patch_took() { # <lockfile> <package>
  awk -v want="$2" '
    /^\[\[package\]\]/ { pkg = ""; next }
    /^name = / { gsub(/[",]/, ""); pkg = $3; if (pkg == want) seen = 1; next }
    /^source = / { if (pkg == want) registry = 1 }
    END { exit(seen && !registry ? 0 : 1) }
  ' "$1"
}

# The root manifest's [workspace.dependencies] line for a package, verbatim, so
# a fork that inherits it in the workspace inherits the same version in the
# mirror. Exits non-zero when the root no longer pins it, which is a real
# failure: the fork's own manifest names no version of its own.
root_workspace_dep() { # <package>
  awk -v want="$1" '
    /^\[/ { in_deps = ($0 == "[workspace.dependencies]"); next }
    in_deps && $1 == want && $2 == "=" { print; found = 1; exit }
    END { exit(found ? 0 : 1) }
  ' "$ROOT/Cargo.toml"
}

rc=0
total_tests=0
total_doctests=0
checked=0
for fork in "${FORKS[@]}"; do
  started=$SECONDS
  src=${SRC_OVERRIDE[$fork]:-$ROOT/third_party/$fork/src}
  if [ ! -d "$src" ]; then
    echo "FAIL $fork: no source directory at $src" >&2
    rc=1
    continue
  fi
  # Two levels down, so the manifest's `../../crates/...` lands on the mirror's
  # symlink and inherits the root workspace's package metadata.
  dir="$MIRROR/forks/$fork"
  mkdir -p "$dir" || { rc=1; continue; }
  ln -s "$(cd "$src" && pwd -P)" "$dir/src" || { rc=1; continue; }
  [ -d "$ROOT/third_party/$fork/testdata" ] &&
    ln -s "$ROOT/third_party/$fork/testdata" "$dir/testdata"
  cp "$ROOT/third_party/$fork/Cargo.toml" "$dir/Cargo.toml" || { rc=1; continue; }
  printf '\n[workspace]\n' >>"$dir/Cargo.toml"
  if [ -n "${INHERITED[$fork]+set}" ]; then
    printf '\n[workspace.dependencies]\n' >>"$dir/Cargo.toml"
    # shellcheck disable=SC2086 # the table's values are space-separated lists
    for dep in ${INHERITED[$fork]}; do
      if ! line=$(root_workspace_dep "$dep"); then
        echo "FAIL $fork: $dep is inherited with \`workspace = true\` but the" \
          "root manifest's [workspace.dependencies] no longer pins it" >&2
        rc=1
        continue 2
      fi
      printf '%s\n' "$line" >>"$dir/Cargo.toml"
    done
  fi
  if [ -n "${PATCHED[$fork]+set}" ]; then
    printf '\n[patch.crates-io]\n%s = { path = "../../third_party/%s" }\n' \
      "${PATCHED[$fork]}" "${PATCHED[$fork]}" >>"$dir/Cargo.toml"
  fi
  # Same content, same mtime as the fork's own manifest: cargo compares mtimes
  # to decide freshness, so a manifest that is merely rewritten is a rebuild.
  touch -r "$ROOT/third_party/$fork/Cargo.toml" "$dir/Cargo.toml"

  out="$MIRROR/$fork.log"
  echo "  [$fork] cargo test --lib in $dir" >&2
  (cd "$dir" && cargo test --lib) >"$out" 2>&1
  fork_rc=$?
  elapsed=$((SECONDS - started))

  if [ "$fork_rc" -ne 0 ]; then
    echo "FAIL $fork: cargo test --lib exited $fork_rc (${elapsed}s)" >&2
    sed "s/^/  [$fork] /" "$out" >&2
    rc=1
    continue
  fi
  if [ -n "${PATCHED[$fork]+set}" ] && ! patch_took "$dir/Cargo.lock" "${PATCHED[$fork]}"; then
    echo "FAIL $fork: ${PATCHED[$fork]} resolved from the registry, not third_party/${PATCHED[$fork]};" \
      "the mirror's [patch.crates-io] did not take and this run proved nothing" >&2
    rc=1
    continue
  fi
  n=$(awk '/^test result: ok\./ {s += $4} END {print s+0}' "$out")
  if [ "$n" -lt "${MIN_TESTS[$fork]}" ]; then
    echo "FAIL $fork: only $n tests ran; expected ${MIN_TESTS[$fork]}+" >&2
    rc=1
    continue
  fi

  # The doctest target is a separate cargo invocation and a separate floor: it
  # compiles the crate as a downstream consumer would, so it is the only target
  # that can catch a doc example the fork's own dev-dependencies cannot build.
  doc_n=
  if [ -n "${DOC_MIN_TESTS[$fork]+set}" ]; then
    doc_out="$MIRROR/$fork.doc.log"
    echo "  [$fork] cargo test --doc in $dir" >&2
    (cd "$dir" && cargo test --doc) >"$doc_out" 2>&1
    doc_rc=$?
    elapsed=$((SECONDS - started))
    if [ "$doc_rc" -ne 0 ]; then
      echo "FAIL $fork: cargo test --doc exited $doc_rc (${elapsed}s)" >&2
      sed "s/^/  [$fork] /" "$doc_out" >&2
      rc=1
      continue
    fi
    doc_n=$(awk '/^test result: ok\./ {s += $4} END {print s+0}' "$doc_out")
    if [ "$doc_n" -lt "${DOC_MIN_TESTS[$fork]}" ]; then
      echo "FAIL $fork: only $doc_n doctests ran; expected ${DOC_MIN_TESTS[$fork]}+" >&2
      rc=1
      continue
    fi
    total_doctests=$((total_doctests + doc_n))
  fi

  if [ -n "$doc_n" ]; then
    printf 'ok   %s %s tests, %s doctests (%ss)\n' "$fork" "$n" "$doc_n" "$elapsed"
  else
    printf 'ok   %s %s tests (%ss)\n' "$fork" "$n" "$elapsed"
  fi
  total_tests=$((total_tests + n))
  checked=$((checked + 1))
done

if [ "$rc" -eq 0 ]; then
  # scripts/ci-local.sh reports whatever follows "forks, " on this line.
  printf 'ok   %s forks, %s unit tests' "$checked" "$total_tests"
  [ "$total_doctests" -gt 0 ] && printf ', %s doctests' "$total_doctests"
  printf '\n'
fi
exit "$rc"
