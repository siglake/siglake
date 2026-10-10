#!/usr/bin/env bash
# Exercise the ci-local test-binary identity guard without compiling Rust.

set -euo pipefail

cd "$(dirname "$0")/.."
. scripts/ci-local-test-guard.sh

check_dir=$(mktemp -d "${TMPDIR:-/tmp}/siglake-test-guard.XXXXXX")
trap 'rm -rf -- "$check_dir"' EXIT

workspace="$check_dir/workspace"
fixture_doc_extension=md
fixture_doc="$workspace/docs/x.${fixture_doc_extension}"
mkdir -p \
  "$workspace/crates/a/src" \
  "$workspace/crates/b/src" \
  "$workspace/third_party/c/src" \
  "$workspace/docs"
touch \
  "$workspace/Cargo.toml" \
  "$workspace/Cargo.lock" \
  "$workspace/crates/a/Cargo.toml" \
  "$workspace/crates/a/src/lib.rs" \
  "$workspace/crates/b/Cargo.toml" \
  "$workspace/crates/b/src/main.rs" \
  "$workspace/third_party/c/Cargo.toml" \
  "$workspace/third_party/c/src/lib.rs" \
  "$fixture_doc"
git -C "$workspace" init -q
git -C "$workspace" add .
git -C "$workspace" \
  -c user.name='CI fixture' -c user.email='ci-fixture@example.invalid' \
  commit -qm 'fixture'
touch -d '2 hours ago' \
  "$workspace/Cargo.toml" \
  "$workspace/Cargo.lock" \
  "$workspace/crates/a/Cargo.toml" \
  "$workspace/crates/a/src/lib.rs" \
  "$workspace/crates/b/Cargo.toml" \
  "$workspace/crates/b/src/main.rs" \
  "$workspace/third_party/c/Cargo.toml" \
  "$workspace/third_party/c/src/lib.rs" \
  "$fixture_doc"
marker="$check_dir/marker"
touch -d '1 hour ago' "$marker"
docs_mtime=$(stat -c '%Y' -- "$fixture_doc")
(
  cd "$workspace"
  refreshed_workspace_source_count=$(refresh_workspace_sources)
  if [ "$refreshed_workspace_source_count" -ne 5 ]; then
    echo "FAIL refreshed $refreshed_workspace_source_count workspace sources, expected 5" >&2
    exit 1
  fi
  for source in \
    Cargo.toml \
    Cargo.lock \
    crates/a/src/lib.rs \
    crates/b/src/main.rs \
    third_party/c/src/lib.rs; do
    if [ ! "$source" -nt "$marker" ]; then
      echo "FAIL workspace source was not refreshed: $source" >&2
      exit 1
    fi
  done
  if [ "$(stat -c '%Y' -- "$fixture_doc")" != "$docs_mtime" ]; then
    echo "FAIL refresh changed the documentation fixture" >&2
    exit 1
  fi
  if [ -n "$(git status --porcelain)" ]; then
    echo "FAIL refresh made the git worktree dirty" >&2
    git status --short >&2
    exit 1
  fi
)

executable="$check_dir/query server"
printf 'first\n' >"$executable"
build_messages="$check_dir/build.json"
python3 - "$executable" >"$build_messages" <<'PY'
import json
import sys

print(json.dumps({"reason": "compiler-artifact", "executable": sys.argv[1]}))
PY

identities="$check_dir/identities"
changed="$check_dir/changed"
test_log="$check_dir/test.log"
record_test_executables "$build_messages" "$identities"
printf 'test result: ok. 1 passed; 0 failed\n' >"$test_log"
if test_pass_contaminated "$test_log" "$identities" "$changed"; then
  echo "FAIL unchanged executable was reported as contaminated" >&2
  exit 1
fi

# Atomic replacement gives the same path a new inode even on filesystems whose
# timestamp granularity would make an mtime-only check miss the relink.
printf 'second\n' >"$check_dir/replacement"
mv "$check_dir/replacement" "$executable"
if ! test_pass_contaminated "$test_log" "$identities" "$changed" \
  || ! grep -Fqx "$executable" "$changed"; then
  echo "FAIL replaced executable was not reported as contaminated" >&2
  exit 1
fi

record_test_executables "$build_messages" "$identities"
cat >"$test_log" <<'EOF'
Caused by:
  could not execute process `/tmp/query_server` (never executed)
Caused by:
  No such file or directory (os error 2)
EOF
if ! test_pass_contaminated "$test_log" "$identities" "$changed"; then
  echo "FAIL never-executed log was not reported as contaminated" >&2
  exit 1
fi
failure_summary=$(print_test_failure_summary "$test_log")
for expected in 'Caused by:' '  could not execute process `/tmp/query_server` (never executed)' \
  '  No such file or directory (os error 2)'; do
  if ! grep -Fqx "$expected" <<<"$failure_summary"; then
    echo "FAIL failure summary dropped: $expected" >&2
    exit 1
  fi
done

log_root="$check_dir/ci-local"
mkdir -p "$log_root/older" "$log_root/newer"
printf 'Compiling old (/work/old/crates/core)\n' >"$log_root/older/test.log"
printf 'checkout: /work/new\n' >"$log_root/newer/test.log"
attempt_epoch=$(date +%s)
touch -d "@$((attempt_epoch - 2))" "$log_root/older/test.log"
touch -d "@$attempt_epoch" "$log_root/newer/test.log"
attribution=$(find_concurrent_test_run \
  "$log_root" "$test_log" /work/current "$attempt_epoch" "$attempt_epoch")
if [[ $attribution != "/work/new ($log_root/newer/test.log)" ]]; then
  echo "FAIL newest concurrent checkout was not attributed: $attribution" >&2
  exit 1
fi

stale_log_root="$check_dir/stale-ci-local"
mkdir -p "$stale_log_root/stale"
printf 'checkout: /work/stale\n' >"$stale_log_root/stale/test.log"
touch -d "@$((attempt_epoch - 2))" "$stale_log_root/stale/test.log"
if attribution=$(find_concurrent_test_run \
  "$stale_log_root" "$test_log" /work/current "$attempt_epoch" "$attempt_epoch"); then
  echo "FAIL stale checkout was attributed: $attribution" >&2
  exit 1
fi

named_log_root="$check_dir/named-ci-local"
named_log_dir="$check_dir/caller named logs"
mkdir -p "$named_log_root" "$named_log_dir"
printf 'checkout: /work/caller-named\n' >"$named_log_dir/test.log"
touch -d "@$attempt_epoch" "$named_log_dir/test.log"
named_pointer="$named_log_root/.run-$$-$attempt_epoch.pointer"
write_ci_local_run_pointer "$named_pointer" "$named_log_dir" "$$" "$attempt_epoch"
attribution=$(find_concurrent_test_run \
  "$named_log_root" "$test_log" /work/current "$attempt_epoch" "$attempt_epoch")
if [[ $attribution != "/work/caller-named ($named_log_dir/test.log)" ]]; then
  echo "FAIL caller-named log directory was not attributed: $attribution" >&2
  exit 1
fi

stale_pointer_dir="$check_dir/stale pointer logs"
mkdir -p "$stale_pointer_dir"
printf 'checkout: /work/stale-pointer\n' >"$stale_pointer_dir/test.log"
touch -d "@$attempt_epoch" "$stale_pointer_dir/test.log"
stale_pointer="$named_log_root/.run-999999999-$attempt_epoch.pointer"
write_ci_local_run_pointer \
  "$stale_pointer" "$stale_pointer_dir" 999999999 "$attempt_epoch"
rm -f -- "$named_pointer"
if attribution=$(find_concurrent_test_run \
  "$named_log_root" "$test_log" /work/current "$attempt_epoch" "$attempt_epoch"); then
  echo "FAIL stale run pointer was attributed: $attribution" >&2
  exit 1
fi

self_pointer="$named_log_root/.run-$$-$attempt_epoch.pointer"
write_ci_local_run_pointer "$self_pointer" "$check_dir" "$$" "$attempt_epoch"
touch -d "@$attempt_epoch" "$test_log"
if attribution=$(find_concurrent_test_run \
  "$named_log_root" "$test_log" /work/current "$attempt_epoch" "$attempt_epoch"); then
  echo "FAIL current run pointer was attributed to itself: $attribution" >&2
  exit 1
fi

echo "ok (source refresh, replacement, never-executed summary, physical, caller-named, stale and self-pointer attribution fixtures)"
