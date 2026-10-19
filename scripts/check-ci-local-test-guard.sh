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

# A rustc killed by a signal is retried once; a tree that does not compile is
# not. The two look alike in a build log -- both end in `error:` lines and a
# red cargo exit -- so each spelling of a dead compiler and each spelling of an
# ordinary failure is held against the matcher here.
crash_log="$check_dir/crash.log"
cat >"$crash_log" <<'EOF'
   Compiling siglake-storage v0.3.0 (/work/crates/siglake-storage)
error: rustc interrupted by SIGSEGV, printing backtrace
note: rustc unexpectedly overflowed its stack! this is a bug
error: could not compile `siglake-storage` (lib test)
EOF
if ! cargo_compiler_crashed "$crash_log"; then
  echo "FAIL rustc's own signal report was not reported as a compiler crash" >&2
  exit 1
fi

cat >"$crash_log" <<'EOF'
error: could not compile `siglake-storage` (lib test)

Caused by:
  process didn't exit successfully: `/usr/local/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/rustc --crate-name siglake_storage --test` (signal: 11, SIGSEGV: invalid memory reference)
EOF
if ! cargo_compiler_crashed "$crash_log"; then
  echo "FAIL cargo's signal wait-status was not reported as a compiler crash" >&2
  exit 1
fi

# The doctest arm of the fork-tests job compiles through rustdoc, which carries
# the same signal handler and the same wait status.
cat >"$crash_log" <<'EOF'
error: doctest failed, to rerun pass `--doc`

Caused by:
  process didn't exit successfully: `/usr/local/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/rustdoc --edition=2021 --crate-type lib` (signal: 11, SIGSEGV: invalid memory reference)
EOF
if ! cargo_compiler_crashed "$crash_log"; then
  echo "FAIL rustdoc's signal wait-status was not reported as a compiler crash" >&2
  exit 1
fi

cat >"$crash_log" <<'EOF'
error[E0308]: mismatched types
 --> crates/siglake-storage/src/iceberg.rs:1:1
error: could not compile `siglake-storage` (lib test) due to 1 previous error
EOF
if cargo_compiler_crashed "$crash_log"; then
  echo "FAIL an ordinary compile error was reported as a compiler crash" >&2
  exit 1
fi

# The renderer indents the source it quotes, so a crate that writes this text
# in a string literal cannot buy itself a retry.
cat >"$crash_log" <<'EOF'
error[E0599]: no method named `signal`
   |
13 |     let note = "error: rustc interrupted by SIGSEGV, printing backtrace";
   |                 ^^^^^
EOF
if cargo_compiler_crashed "$crash_log"; then
  echo "FAIL a quoted diagnostic was reported as a compiler crash" >&2
  exit 1
fi

# The jobs below the test job read logs that mix compiling with running, so the
# two other processes that print this wait status have to stay red. A test
# binary the kernel killed is the regression the profiling and docker jobs
# exist to catch; a build script the kernel killed is a dependency that cannot
# be built here.
cat >"$crash_log" <<'EOF'
   Compiling siglake-core v0.3.0 (/work/crates/siglake-core)
    Finished `test` profile [unoptimized + debuginfo] target(s)
     Running unittests src/lib.rs (/work/target/debug/deps/siglake_core-9f2c1a7b)
error: test failed, to rerun pass `-p siglake-core --lib`

Caused by:
  process didn't exit successfully: `/work/target/debug/deps/siglake_core-9f2c1a7b --nocapture` (signal: 11, SIGSEGV: invalid memory reference)
EOF
if cargo_compiler_crashed "$crash_log"; then
  echo "FAIL a test binary killed by a signal was reported as a compiler crash" >&2
  exit 1
fi

cat >"$crash_log" <<'EOF'
error: failed to run custom build command for `ring v0.17.8`

Caused by:
  process didn't exit successfully: `/work/target/debug/build/ring-4a1f/build-script-build` (signal: 9, SIGKILL: kill)
EOF
if cargo_compiler_crashed "$crash_log"; then
  echo "FAIL a build script killed by a signal was reported as a compiler crash" >&2
  exit 1
fi

# check-fork-tests.sh prefixes each fork's cargo output with `  [<fork>] `, so
# that job passes the prefix in. The anchors still hold: without the prefix the
# same log matches nothing.
fork_prefix='  \[[a-z0-9-]+\] '
cat >"$crash_log" <<'EOF'
FAIL iceberg: cargo test --lib exited 101 (12s)
  [iceberg] error: could not compile `iceberg` (lib test)
  [iceberg]
  [iceberg] Caused by:
  [iceberg]   process didn't exit successfully: `/usr/local/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/rustc --crate-name iceberg --test` (signal: 11, SIGSEGV: invalid memory reference)
EOF
if ! cargo_compiler_crashed "$crash_log" "$fork_prefix"; then
  echo "FAIL a prefixed fork crash was not reported as a compiler crash" >&2
  exit 1
fi
if cargo_compiler_crashed "$crash_log"; then
  echo "FAIL prefixed output matched without the prefix the job passes in" >&2
  exit 1
fi
cat >"$crash_log" <<'EOF'
FAIL iceberg: cargo test --lib exited 101 (12s)
  [iceberg] error[E0308]: mismatched types
  [iceberg] error: could not compile `iceberg` (lib test) due to 1 previous error
EOF
if cargo_compiler_crashed "$crash_log" "$fork_prefix"; then
  echo "FAIL a prefixed compile error was reported as a compiler crash" >&2
  exit 1
fi

# The retry itself: one replay, no more, and only for a crash. Each attempt
# keeps its own log and the job log keeps both, which is what a red line's
# excerpt and the morning-after reader respectively need.
crash_output=$(cat <<'EOF'
error: could not compile `siglake-storage` (lib test)

Caused by:
  process didn't exit successfully: `/toolchains/stable/bin/rustc --crate-name siglake_storage --test` (signal: 11, SIGSEGV: invalid memory reference)
EOF
)
retry_attempts=0
# `<attempt that stops crashing, or 0 for never> <exit code>`: a stand-in cargo
# job that writes a crash log until that attempt, then succeeds.
crashing_job() {
  local recovers_on=$1
  retry_attempts=$((retry_attempts + 1))
  if [ "$recovers_on" -ne 0 ] && [ "$retry_attempts" -ge "$recovers_on" ]; then
    printf 'test result: ok. 7 passed; 0 failed\n'
    return 0
  fi
  printf '%s\n' "$crash_output"
  return 101
}
failing_job() {
  retry_attempts=$((retry_attempts + 1))
  printf 'error[E0308]: mismatched types\n'
  return 101
}
passing_job() {
  retry_attempts=$((retry_attempts + 1))
  printf 'test result: ok. 7 passed; 0 failed\n'
  return 0
}

job_log="$check_dir/job.log"
stem="$check_dir/job-attempt"

retry_attempts=0
rc=0
cargo_job_with_crash_retry "$job_log" "$stem" "" passing_job || rc=$?
if [ "$rc" -ne 0 ] || [ "$retry_attempts" -ne 1 ] || [ "$cargo_job_attempts" -ne 1 ] \
  || [ "$cargo_job_compiler_crashed" -ne 0 ]; then
  echo "FAIL a job that passed was retried or misreported (rc $rc, $retry_attempts runs)" >&2
  exit 1
fi
if [ "$(cargo_job_status ok)" != "ok" ] \
  || [ "$(cargo_job_status ok '940 tests')" != "ok (940 tests)" ]; then
  echo "FAIL an unretried status named a retry" >&2
  exit 1
fi

retry_attempts=0
rc=0
cargo_job_with_crash_retry "$job_log" "$stem" "" failing_job || rc=$?
if [ "$rc" -ne 101 ] || [ "$retry_attempts" -ne 1 ] || [ "$cargo_job_compiler_crashed" -ne 0 ]; then
  echo "FAIL an ordinary compile failure was retried (rc $rc, $retry_attempts runs)" >&2
  exit 1
fi

retry_attempts=0
rc=0
cargo_job_with_crash_retry "$job_log" "$stem" "" crashing_job 2 || rc=$?
if [ "$rc" -ne 0 ] || [ "$retry_attempts" -ne 2 ] || [ "$cargo_job_attempts" -ne 2 ] \
  || [ "$cargo_job_compiler_crashed" -ne 0 ]; then
  echo "FAIL a crash the replay cleared was not reported green (rc $rc, $retry_attempts runs)" >&2
  exit 1
fi
if [ "$(cargo_job_status ok)" != "ok (retried after a compiler crash)" ] \
  || [ "$(cargo_job_status ok '940 tests')" \
    != "ok (940 tests; retried after a compiler crash)" ]; then
  echo "FAIL a recovered run did not name its retry: $(cargo_job_status ok '940 tests')" >&2
  exit 1
fi
if [ "$cargo_job_attempt_log" != "$stem-2.log" ] \
  || ! grep -Fq 'test result: ok.' "$stem-2.log" \
  || ! grep -Fq 'SIGSEGV' "$stem-1.log"; then
  echo "FAIL the retry did not preserve both attempt logs" >&2
  exit 1
fi
if [ "$(grep -c '^===== attempt [12] =====$' "$job_log")" -ne 2 ] \
  || ! grep -Fq 'COMPILER CRASHED' "$job_log"; then
  echo "FAIL the job log did not keep both attempts and name the crash" >&2
  exit 1
fi

retry_attempts=0
rc=0
cargo_job_with_crash_retry "$job_log" "$stem" "" crashing_job 0 || rc=$?
if [ "$rc" -ne 101 ] || [ "$retry_attempts" -ne 2 ] || [ "$cargo_job_attempts" -ne 2 ] \
  || [ "$cargo_job_compiler_crashed" -ne 1 ]; then
  echo "FAIL an unrecovered crash was not exhausted after one retry (rc $rc, $retry_attempts runs)" >&2
  exit 1
fi
if [ "$(cargo_job_status FAIL 'rustc killed by a signal')" \
  != "FAIL (rustc killed by a signal; retried after a compiler crash)" ]; then
  echo "FAIL an unrecovered crash did not report a specific FAIL status" >&2
  exit 1
fi

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

echo "ok (source refresh, replacement, never-executed summary, compiler-crash, test-binary and build-script signals, prefixed fork output, crash retry, physical, caller-named, stale and self-pointer attribution fixtures)"
