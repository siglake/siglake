#!/usr/bin/env bash
#
# Run every job in .github/workflows/ci.yml locally, and report one line each.
#
# This exists because CI was RED ON MAIN for two days and nobody noticed: the
# OpenAPI freshness gate had been failing since 2026-08-29, when a commit added
# a field without regenerating docs/api/. The gates in this repo have a strong
# record of catching real defects — the chart validator found seven, the
# public-tree checker found fourteen — but only when someone looks at them. The
# cheap fix is to make the whole suite runnable in one command before pushing.
#
#   scripts/ci-local.sh              # build-env, fmt, shell, claude-md, set-var, dashboard, test, clippy, profiling, helm, public-tree, generated, deny, fork-tests
#   scripts/ci-local.sh --all        # + operator-cluster (kind), docker, external-readers
#   scripts/ci-local.sh --strict     # a job this box cannot run (no mold, helm, promtool, cargo-deny, kind, docker, external reader) is FAIL, not skipped
#   scripts/ci-local.sh --log-dir D  # keep this run's job logs in D (also CI_LOCAL_LOG_DIR)
#
# `--all` is the nightly / pre-release run, not the per-merge gate (decided
# 2026-09-03): a kind cluster plus two image builds add ~20 min, and the
# operator-cluster and docker jobs in ci.yml already run on every push and PR.
# external-readers has no job in ci.yml; this script is the only place it runs.
# Pair it with --strict, so a box without kind reports operator-cluster red
# instead of a skip that reads as green in the summary line.
#
# A job whose tool is missing (mold, helm, promtool, cargo-deny, kind, docker,
# an external Iceberg reader) reports `skipped (...)` and does not affect the exit
# code, so a laptop without kind still gets a meaningful green. An automated
# caller that judges by exit code alone reads that same green as "deny passed"
# when deny never ran. `--strict` is for that caller — the manager's playbook
# passes it — and turns every skipped job into a FAIL line that names what was
# missing. `external-readers` used to report a bare `ok` with every engine
# skipped, which is how heavy run #37 admitted an unverified external-reader
# contract as launch evidence; see ci-local-external-readers.sh.
#
# Every job's full output is kept in a directory this invocation alone owns,
# ${CARGO_TARGET_DIR:-target}/ci-local/<UTC stamp>-<pid>/<job>.log unless the
# caller names one with --log-dir or CI_LOCAL_LOG_DIR, and SURVIVES EXIT, red
# or green: the summary names the directory, a red line prints its log path,
# and ${CARGO_TARGET_DIR:-target}/ci-local/latest points at the newest run. The
# logs used to live in a mktemp dir removed on EXIT, and a failing test job
# showed the first five FAILED lines: the 2026-08-30 flake's name was lost that
# way, and a four-minute suite had to be rerun to learn it. They then lived
# under fixed names directly in ci-local/, one set for every checkout sharing
# the target dir, serialised by a lock: the first nightly --all run
# (2026-09-05) lost its red operator-cluster log to the per-merge gate that had
# waited fourteen minutes behind it and emptied the directory the instant the
# lock was released. Run directories this script named are pruned after a
# week; a --log-dir the caller named is never touched.
#
# Temporary files are different: /tmp on the manager host is a quota'd tmpfs
# shared by parallel agents, so TMPDIR points at <run>/tmp on the roomier target
# filesystem. That scratch directory is removed on EXIT, while the job logs
# above survive for diagnosis.
#
# CI's helm job also runs check-public-tree.py; here that step is reported as
# its own `public-tree` line, so a dead reference is not blamed on the chart.
# Likewise the shell job's set_var guard is its own `set-var` line: a test that
# mutates the process environment is not a shell script that fails to parse.
# And check-chart.py's helm-free half (deployment env names, dashboard metric
# names, README alert count) is its own `dashboard` line, so a box without helm
# still runs it instead of skipping it along with the renders.
#
# Exits nonzero if any job would fail.

set -uo pipefail
cd "$(dirname "$0")/.."
. scripts/ci-local-test-guard.sh
. scripts/ci-local-external-readers.sh
. scripts/ci-local-image-sizes.sh
. scripts/ci-local-build-env.sh
. scripts/ci-local-compose-port.sh

WITH_HEAVY=0
STRICT=0
# The environment form exists for callers that drive checkouts of mixed age:
# a script from before --log-dir existed exits 2 on the flag and ignores the
# variable. The flag wins when both are given.
LOG_DIR=${CI_LOCAL_LOG_DIR:-}
while [ $# -gt 0 ]; do
  case "$1" in
    --all) WITH_HEAVY=1 ;;
    --strict) STRICT=1 ;;
    --log-dir=*) LOG_DIR=${1#--log-dir=} ;;
    --log-dir)
      [ $# -ge 2 ] || { echo "--log-dir needs a directory" >&2; exit 2; }
      LOG_DIR=$2; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

caller_named_log_dir=0
[ -n "$LOG_DIR" ] && caller_named_log_dir=1

total_started=$SECONDS

# One directory per invocation, shared with nobody and emptied by no one.
# LOG_ROOT is shared by every checkout that shares CARGO_TARGET_DIR (on the
# manager box: every lane, gate and external run). The job logs used to sit
# in it under fixed names, invocations serialised by a flock on it, and each
# run began with `rm -f *.log`: a per-merge gate waited fourteen minutes
# behind the nightly --all run and, the instant the lock was released,
# deleted the evidence of that run's red operator-cluster job. A fresh
# <stamp>-<pid> directory needs neither the lock nor the rm, so two runs
# neither wait on nor overwrite each other; a caller that wants the logs kept
# with its own record (the manager, next to the run's log) names the
# directory, and nothing in a directory the caller named is deleted here, not
# even a stale log from a job this run did not reach.
LOG_ROOT="${CARGO_TARGET_DIR:-target}/ci-local"
case "$LOG_ROOT" in /*) ;; *) LOG_ROOT="$PWD/$LOG_ROOT" ;; esac
[ -n "$LOG_DIR" ] || LOG_DIR="$LOG_ROOT/$(date -u +%Y%m%dT%H%M%SZ)-$$"
case "$LOG_DIR" in /*) ;; *) LOG_DIR="$PWD/$LOG_DIR" ;; esac
mkdir -p "$LOG_ROOT" "$LOG_DIR"
export TMPDIR="$LOG_DIR/tmp"
mkdir -p "$TMPDIR"
run_pointer=
if [ "$caller_named_log_dir" -eq 1 ]; then
  # Caller-owned logs can live outside LOG_ROOT, where the contamination guard
  # cannot discover them by walking for test.log. Publish this run while it is
  # active; the pointer is diagnostic only and never changes the guard verdict.
  run_started=$(date +%s)
  run_pointer="$LOG_ROOT/.run-$$-$run_started.pointer"
  if ! write_ci_local_run_pointer "$run_pointer" "$LOG_DIR" "$$" "$run_started"; then
    echo "  warning: could not publish ci-local run pointer: $run_pointer" >&2
    run_pointer=
  fi
fi
cleanup_ci_local() {
  rm -rf -- "$LOG_DIR/tmp"
  [ -z "$run_pointer" ] || rm -f -- "$run_pointer"
}
trap cleanup_ci_local EXIT
# `latest` is for whoever tails the newest run by hand; with two runs in
# flight it names the one that started last.
ln -sfn "$LOG_DIR" "$LOG_ROOT/latest" 2>/dev/null || true
# Bound the accumulation: only directories this script named, by their
# <stamp>-<pid> shape, and only once untouched for a week, so the evidence of
# a red run outlives any morning-after look at it.
find "$LOG_ROOT" -mindepth 1 -maxdepth 1 -type d -name '[0-9]*T[0-9]*Z-[0-9]*' \
  -mtime +7 -exec rm -rf {} + 2>/dev/null || true
echo "  per-job logs: $LOG_DIR" >&2

fail=0         # the exit code: any FAIL line, including a strict-mode skip
red=0          # a job that RAN and failed; what "main would be red too" means
strict_skips=0 # skipped jobs turned FAIL by --strict
contamination_red=0 # a shared-target race is not evidence about main

# One line per job on stdout — `<job> <status> (<seconds>s)`, status one of
# `ok`, `ok (...)`, `FAIL`, `FAIL (...)`, `skipped (...)` — is the shape the manager parses;
# keep it. Everything else (excerpts, log paths) goes to stderr, indented two
# spaces. Under --strict a `skipped (...)` status is reported as FAIL and the
# original reason follows on stderr, so the line still names the job and the
# excerpt still names the missing tool.
report() {
  local elapsed=$((SECONDS - job_started)) status=$2 why=
  case "$status" in
    skipped*)
      if [ "$STRICT" = 1 ]; then
        why=$status; status=FAIL; strict_skips=$((strict_skips + 1))
      fi ;;
  esac
  printf '%-18s %s (%ss)\n' "$1" "$status" "$elapsed"
  if [[ "$status" == FAIL* ]]; then
    fail=1
    if [ -n "$why" ]; then
      echo "  $why; --strict counts a job this box cannot run as red" >&2
    else
      red=1
    fi
    [ -s "$LOG_DIR/$1.log" ] && echo "  log: $LOG_DIR/$1.log" >&2
  fi
  return 0
}

# --- build-env ---------------------------------------------------------------
job_started=$SECONDS
# The settings CI builds under, applied before the first cargo job below and
# reported as a line of its own so the summary says which ones this run got.
# They live in scripts/ci-local-build-env.sh; scripts/check-ci-build-env.py
# diffs that file against ci.yml's workflow `env:` and its jobs' RUSTFLAGS and
# fails when they drift.
#
# Two costs, both expected. Flipping the debug profile invalidates every unit
# in CARGO_TARGET_DIR once, so the first run after this landed rebuilt the
# workspace; and while the target dir is shared with plain `cargo test` in the
# same tree (#1696 gives ci-local its own), the two profiles evict each other,
# so alternating between them pays that rebuild each way.
#
# mold is a prerequisite, not a preference. A box without it links with the
# default linker, which is the one difference `cargo test --workspace` green
# locally and red on the runner (#2931) could not be reproduced against here.
# That run reports `skipped`, so --strict calls it what it is -- not exercised
# -- rather than letting it read as a linker this gate checked.
have_mold=0
command -v mold >/dev/null 2>&1 && have_mold=1
apply_build_env "$have_mold"
report build-env "$(build_env_status "$have_mold")"

# --- fmt ---------------------------------------------------------------------
job_started=$SECONDS
if cargo fmt --all -- --check >"$LOG_DIR/fmt.log" 2>&1; then
  report fmt ok
else
  report fmt FAIL; grep -E '^Diff in' "$LOG_DIR/fmt.log" | head -5 | sed 's/^/  /' >&2
fi

# --- shell -------------------------------------------------------------------
job_started=$SECONDS
# Mirrors the `shell` job. Counts what it CHECKED, for the same reason the test
# job does: an empty file list exits 0 from a bare loop and reads like success.
sh_files=$(git ls-files '*.sh' '*.sh.tpl' '*.bash' | wc -l)
if [ "$sh_files" -eq 0 ]; then
  report shell FAIL
  echo "  no shell scripts found; the gate would have checked nothing" >&2
else
  sh_rc=0
  : >"$LOG_DIR/shell.log"
  while IFS= read -r f; do
    bash -n "$f" 2>>"$LOG_DIR/shell.log" || sh_rc=1
  done < <(git ls-files '*.sh' '*.sh.tpl' '*.bash')
  scripts/check-smoke.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-aws-up-kubeconfig.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-aws-down-destroy.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-loadgen.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-compose-preflight.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-ci-local-test-guard.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-bench-ports.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-jaeger-ui-recording.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  # The mold linker flag and the apt install of mold have to live in the same
  # job: set workflow-wide, the flag reached `docker`, which installs nothing,
  # and every run of that job died at its first link with `cannot find 'ld'`
  # under the name of the MinIO and Postgres suites it never reached.
  python3 scripts/check-ci-linker.py >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  # And the other half of the same drift: ci.yml's build settings reaching this
  # script at all. Until #2947 none of them did, so every local gate built with
  # full debuginfo, incrementally, and linked with cc -- three differences from
  # the run it claims to predict, silently. This compares
  # scripts/ci-local-build-env.sh with ci.yml's workflow `env:` and its jobs'
  # RUSTFLAGS, and drives both arms of the mold prerequisite.
  python3 scripts/check-ci-build-env.py >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  # A fork's pull request runs the one commit a maintainer labelled `ci:run`,
  # and nothing else. The first gate holds the two properties that make that
  # true -- ci.yml reachable only from `pull_request`, ci-authorize.yml never
  # checking out the fork -- and drives the decision table over synthetic label
  # events. The second drives the approval's head recheck against a stand-in
  # `gh`: a push landing between the label and the approval must release
  # nothing.
  python3 scripts/check-ci-authorization.py >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-ci-approve-run.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-external-readers-report.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-image-sizes-report.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-kind-round-scale.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-kind-round-events.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-kind-mirror-reclaim-qualification.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-kind-postgres-outage-evidence.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-kind-schema-rollback-evidence.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-exact-point-falsifier-evidence.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-file-cache-populate-depth-reader.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-kind-ingester-pod-labels.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-kind-compactor-pod-labels.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  scripts/check-kind-round-diagnostics.sh >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  # The release tag is `v0.1.0` and both charts ask for the numeric appVersion,
  # so the first published chart would have pulled a tag the registry does not
  # have. This evaluates publish.yml's tag-resolution step and compares the
  # answer with both charts' default rendered image tag and the pinned tags in
  # deploy/ -- no registry, no helm.
  python3 scripts/check-release-tags.py >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  # What is inside the released image, as opposed to what it is called. The
  # Dockerfiles copy no Git metadata, so SIGLAKE_GIT_SHA is the only source of
  # the revision in `--version` and siglake_build_info, and publish.yml passed
  # none. This executes its revision step against a synthetic tagged checkout
  # under both triggers -- no registry, no image build.
  python3 scripts/check-release-provenance.py >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  # The 2026-09-16 sweep rule: the published tree does not discuss non-OSS
  # competitors. #4569 cleared one vendor's name and its event-collector
  # protocol out of sixteen files by hand -- one of them a middleware whose
  # name reached three published OpenAPI descriptions -- and the sweep that
  # found them was a grep nobody would think to run again. This reads the files
  # that ship (the set check-public-tree.py defines) against a maintained
  # denylist, and takes `vendor-name-ok: <why>` on the line as the exception.
  python3 scripts/check-vendor-names.py >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  # This list and ci.yml's `shell` job are two copies of the same list, and a
  # guard added to only one of them ran nowhere the other looked -- which is how
  # the three guards above it were local-only until 2026-09-07. The check reads
  # both files statically, requires CI to run everything this block runs, and
  # requires every CI shell guard to run somewhere in this file; it is
  # therefore inside the list it checks.
  python3 scripts/check-shell-job-parity.py >>"$LOG_DIR/shell.log" 2>&1 || sh_rc=1
  if [ "$sh_rc" -eq 0 ]; then
    report shell "ok ($sh_files scripts)"
  else
    report shell FAIL; head -5 "$LOG_DIR/shell.log" | sed 's/^/  /' >&2
  fi
fi

# --- claude-md ---------------------------------------------------------------
job_started=$SECONDS
# The shell job's second step: the session brief stays under 200 lines, so the
# status log cannot regrow in it. The script owns the threshold, the message
# and the file name -- it is the one shipping file allowed to name the brief
# (EXPECTED in check-public-tree.py) -- and prints the status reported here:
# `ok (N lines)`, or `ok (public tree: ...)` when both private paths are absent
# from a published tree and there is nothing to cap. A private-shaped tree
# missing only the brief still reports `skipped`, so --strict rejects it.
if claude_status=$(scripts/check-claude-md.sh 2>"$LOG_DIR/claude-md.log"); then
  report claude-md "$claude_status"
else
  report claude-md FAIL; sed 's/^/  /' "$LOG_DIR/claude-md.log" >&2
fi

# --- set-var -----------------------------------------------------------------
job_started=$SECONDS
# The shell job's third step: no std::env::set_var / remove_var in any tracked
# Rust file under crates/. The ok line carries the mutation-site count.
if python3 scripts/check-set-var.py >"$LOG_DIR/set-var.log" 2>&1; then
  report set-var "ok ($(awk '/^ok/ {print $2; exit}' "$LOG_DIR/set-var.log") mutation sites)"
else
  report set-var FAIL; grep -E '^FAIL' "$LOG_DIR/set-var.log" | head -5 | sed 's/^/  /' >&2
fi

# --- dashboard ---------------------------------------------------------------
job_started=$SECONDS
# check-chart.py's checks that need no render: every deployed SIGLAKE_* env name
# in compose and the operator is named by non-test Rust source; every
# `siglake_*` series a deploy/grafana panel queries is one crates/ or an owned
# fork emits, in the form the exporter renders it; the README's alert count matches the
# PrometheusRule template source; and every counter the template reads through
# `increase()` is in a pre-registration list in siglake-core's metrics.rs. CI
# runs them inside the helm job's full check-chart.py; here they are
# their own line so a box without helm still runs them, and so a renamed
# metric is not blamed on the chart. The helm job below still runs the full
# script, render matrix included, when the binary is present. Runs before the
# four-minute test job for the same reason set-var does: a wrong answer here is
# red in a second.
#
# Two of these checks evaluate a panel's PromQL with promtool (the drain backlog
# and the text-index startup stages), which the helm job installs in CI but
# which is also worth using here whenever it happens to be on PATH: the panels'
# arithmetic is what those checks are about, and skipping it silently on a box
# that could have run it is the worse default.
dashboard_args=(--source-only)
if command -v promtool >/dev/null 2>&1; then
  dashboard_args+=(--require-promtool)
fi
if python3 scripts/check-chart.py "${dashboard_args[@]}" >"$LOG_DIR/dashboard.log" 2>&1; then
  report dashboard "ok ($(awk '/^ok +\[dashboard\]/ {print $3; exit}' "$LOG_DIR/dashboard.log") dashboards)"
else
  report dashboard FAIL; grep -E '^FAIL' "$LOG_DIR/dashboard.log" | head -5 | sed 's/^/  /' >&2
fi

# --- test --------------------------------------------------------------------
job_started=$SECONDS
# Counts what RAN, not just the exit code: a filter or feature mistake that
# selects nothing exits 0 and prints "0 passed", which reads like success.
# Do not stop at the first red test binary: one failure must not hide the rest
# of the workspace from the gate report. A red run therefore still pays the
# full ~6-7 minute workspace-test cost.
: >"$LOG_DIR/test.log"
test_attempt=1
test_contamination=0
test_contaminated_twice=0
test_attribution=
while :; do
  test_pass_log="$LOG_DIR/test-pass-$test_attempt.log"
  test_build_messages="$LOG_DIR/test-build-$test_attempt.json"
  test_identities="$LOG_DIR/test-identities-$test_attempt"
  test_changed="$LOG_DIR/test-changed-$test_attempt"
  : >"$test_pass_log"
  # Publish this checkout before waiting for Cargo's shared artifact lock, so
  # a colliding run can attribute us even before this attempt finishes.
  test_attempt_started=$(date +%s)
  printf '===== test attempt %s =====\ncheckout: %s\n' "$test_attempt" "$PWD" \
    >>"$LOG_DIR/test.log"

  # Cargo's artifact lock ends with this build. Record inode and mtime as soon
  # as its JSON stream closes; there is necessarily a small window between the
  # build finishing and this stat pass, which `(never executed)` also covers.
  if cargo test --workspace --no-run --message-format=json \
      >"$test_build_messages" 2>>"$test_pass_log"; then
    append_cargo_diagnostics "$test_build_messages" >>"$test_pass_log"
    if record_test_executables "$test_build_messages" "$test_identities"; then
      # promtool is one of the tools --strict names. siglake-operator's
      # prom_fixture test hands the ingester load query it generates to
      # `promtool test rules`; without the binary it skips, and this variable
      # (set by CI's test job too) makes that skip a failure under --strict
      # instead of a green line for a query nothing evaluated.
      SIGLAKE_OPERATOR_REQUIRE_PROMTOOL="$STRICT" \
        cargo test --workspace --no-fail-fast >>"$test_pass_log" 2>&1
      test_rc=$?
      if test_pass_contaminated "$test_pass_log" "$test_identities" "$test_changed"; then
        test_attempt_ended=$(date +%s)
        test_attribution=$(find_concurrent_test_run \
          "$LOG_ROOT" "$LOG_DIR/test.log" "$PWD" \
          "$test_attempt_started" "$test_attempt_ended" || true)
        [ -n "$test_attribution" ] || test_attribution="unknown concurrent checkout (no other run log named it)"
        {
          printf '\nCONTAMINATED: test binaries relinked during the run by a concurrent build; see %s\n' \
            "$test_attribution"
          if [ -s "$test_changed" ]; then
            echo "changed executables:"
            sed 's/^/  /' "$test_changed"
          fi
        } >>"$test_pass_log"
        test_contamination=1
      else
        test_contamination=0
      fi
    else
      echo "error: failed to record test executable identities" >>"$test_pass_log"
      test_rc=1
      test_contamination=0
    fi
  else
    test_rc=$?
    append_cargo_diagnostics "$test_build_messages" >>"$test_pass_log"
    test_contamination=0
  fi
  cat "$test_pass_log" >>"$LOG_DIR/test.log"

  if [ "$test_contamination" -eq 0 ]; then
    break
  elif [ "$test_attempt" -eq 1 ]; then
    echo "  test CONTAMINATED: binaries relinked during the run by $test_attribution; retrying once" >&2
    print_test_failure_summary "$test_pass_log" | sed 's/^/  /' >&2
    test_attempt=2
  else
    test_contaminated_twice=1
    break
  fi
done

if [ "$test_contaminated_twice" -eq 1 ]; then
  contamination_red=1
  report test "FAIL (contaminated twice by concurrent builds)"
  echo "  test binaries relinked during both attempts; latest concurrent run: $test_attribution" >&2
  print_test_failure_summary "$test_pass_log" | sed 's/^/  /' >&2
elif [ "$test_rc" -eq 0 ]; then
  n=$(awk '/^test result: ok\./ {s+=$4} END {print s+0}' "$test_pass_log")
  if [ "$n" -lt 900 ]; then
    if [ "$test_attempt" -eq 2 ]; then
      report test "FAIL (only $n tests after contamination retry)"
    else
      report test "FAIL"
    fi
    echo "  only $n tests ran; expected 900+" >&2
  elif [ "$test_attempt" -eq 2 ]; then
    report test "ok ($n tests; retried after contamination)"
  else
    report test "ok ($n tests)"
  fi
else
  read -r n binaries < <(
    awk '/^test result: (ok|FAILED)\./ {n += $4 + $6; b++} END {print n+0, b+0}' \
      "$test_pass_log"
  )
  if [ "$test_attempt" -eq 2 ]; then
    report test "FAIL ($n tests across $binaries binaries after contamination retry)"
  else
    report test "FAIL ($n tests across $binaries binaries)"
  fi
  # EVERY failed test by name, every cargo/compiler `error` line, and each test
  # binary's `failures:` name list — not the first five. The `failures:` marker
  # appears twice per binary: first before the captured-stdout blocks, then
  # before the four-space-indented name list; only the list is echoed here.
  print_test_failure_summary "$test_pass_log" | sed 's/^/  /' >&2
fi

# --- clippy ------------------------------------------------------------------
job_started=$SECONDS
if cargo clippy --workspace --all-targets -- -D warnings >"$LOG_DIR/clippy.log" 2>&1; then
  report clippy ok
else
  report clippy FAIL; grep -E '^error' "$LOG_DIR/clippy.log" | head -5 | sed 's/^/  /' >&2
fi

# --- profiling ---------------------------------------------------------------
job_started=$SECONDS
# A step of ci.yml's `clippy` job, reported as its own line because it answers a
# different question: the clippy above builds every member with DEFAULT
# features, so `siglake-core`'s off-by-default `profiling` feature -- the
# `/debug/pprof/*` endpoints, and the only thing a PROFILING=1 image adds to the
# code -- is compiled by neither it nor the test job. The script runs clippy and
# the crate's tests in both `tokio_unstable` configurations and is the same one
# ci.yml and .github/workflows/profiling-image.yml run.
#
# Two extra dependency builds, since each RUSTFLAGS change invalidates the
# graph; they cache separately after the first run. That is the price of the
# gate building what the diagnostic image ships.
if scripts/check-profiling-feature.sh >"$LOG_DIR/profiling.log" 2>&1; then
  report profiling "ok ($(awk '/^test result: ok\./ {s+=$4} END {print s+0}' \
    "$LOG_DIR/profiling.log") tests, both cfgs)"
else
  report profiling FAIL
  grep -E '^(error|test result: FAILED)' "$LOG_DIR/profiling.log" | head -5 | sed 's/^/  /' >&2
fi

# --- helm --------------------------------------------------------------------
job_started=$SECONDS
# Both charts lint, the validator (including `promtool check rules` and
# `promtool test rules`) passes, and every install guard still REFUSES the
# configuration it exists for. A
# guard that stopped firing stops protecting anyone, so it is checked rather
# than assumed. CI installs promtool in its helm job; a local full chart check
# reports that one check as skipped when promtool is absent, while still running
# the render matrix. Under --strict, an otherwise-green helm job becomes a skip
# (and therefore FAIL) when that happens; when promtool is present, the validator
# is told to require it. check-chart.py --source-only needs neither tool.
#
# A missing helm is `skipped`, like a missing cargo-deny, not FAIL. Every step
# below needs the binary — check-chart.py's render matrix goes through `helm
# template`; its helm-free checks already ran as the `dashboard` job above —
# so without it the lints fail with command-not-found and the guards pass
# vacuously: a red line that says nothing about the chart. In sandboxes that
# block helm that line was red on every run regardless of the chart, and
# `--strict` already turns the skip into a FAIL that names what was missing
# for callers that must not mistake "unchecked" for green. CI installs helm
# itself (azure/setup-helm), so it never takes this branch. A helm that is
# present but refuses to run is still FAIL: that is a real result.
if command -v helm >/dev/null 2>&1; then
  helm_ok=1
  promtool_missing=0
  check_chart_args=()
  if [ "$STRICT" = 1 ]; then
    if command -v promtool >/dev/null 2>&1; then
      check_chart_args=(--require-promtool)
    else
      promtool_missing=1
    fi
  fi
  hlog="$LOG_DIR/helm.log"
  : >"$hlog"
  helm lint deploy/helm/siglake >>"$hlog" 2>&1 || helm_ok=0
  helm lint deploy/helm/siglake-operator >>"$hlog" 2>&1 || helm_ok=0
  python3 scripts/check-chart.py "${check_chart_args[@]}" >>"$hlog" 2>&1 || helm_ok=0
  base=(--set s3.region=us-west-2 --set s3.bucket=ci)
  # guard <what> <--set ...>: the render must FAIL. Its refusal lands in the log;
  # a render that succeeded is the defect, and is named there rather than dumped.
  guard() {
    local what=$1; shift
    if helm template t deploy/helm/siglake "${base[@]}" "$@" >/dev/null 2>>"$hlog"; then
      echo "FAIL: $what was allowed" >>"$hlog"; helm_ok=0
    fi
  }
  guard "auth + fan-out with no coordinator token" --set query.tokens.list='{tok}'
  guard "raised bin concurrency without a memory limit" --set compactor.binConcurrency=4
  guard "a second compactor pod without the catalog claim" --set compactor.replicas=2
  guard "a compactor HPA ceiling above one pod without the catalog claim" \
    --set autoscaling.compactor.enabled=true
  guard "the catalog claim with no WAL mirror to claim from" \
    --set compactor.catalogClaim.enabled=true --set wal.mirror.enabled=false
  # #3718: under the claim every compactor publishes the whole shared sealed
  # queue, and the HPA can only ask for that gauge per pod — so the same
  # backlog asks for a count proportional to the count already running. Both
  # doors, including the tier this chart does not render.
  backlog_metric=(--set autoscaling.compactor.enabled=true
    --set autoscaling.compactor.customMetric.enabled=true
    --set compactor.catalogClaim.enabled=true)
  guard "the compactor backlog metric under the catalog claim" "${backlog_metric[@]}"
  guard "the compactor backlog metric under a claim whose tier is not rendered" \
    "${backlog_metric[@]}" --set compactor.enabled=false
  # #3075: the same contention reached through `ingester.extraArgs`. An embedded
  # compactor takes no claim and the chart renders none for it, so the refusal is
  # unconditional — at one replica the rolling update still overlaps two pods.
  embedded=(--set 'ingester.extraArgs[0]=--with-compactor')
  guard "an embedded compactor on the ingester" "${embedded[@]}"
  guard "an embedded compactor under a KEDA ingester ceiling" "${embedded[@]}" \
    --set keda.enabled=true --set ingester.replicas=1
  guard "an embedded compactor waived by the catalog claim" "${embedded[@]}" \
    --set compactor.catalogClaim.enabled=true
  # renders <what> <--set ...>: the render must SUCCEED. A guard with no
  # must-render case is how a refusal grows past its intent — here, that an
  # ordinary ingester still scales, and that an autoscaler which is OFF does not
  # contribute its unused ceiling to any count.
  renders() {
    local what=$1; shift
    if ! helm template t deploy/helm/siglake "${base[@]}" "$@" >/dev/null 2>>"$hlog"; then
      echo "FAIL: $what was refused" >>"$hlog"; helm_ok=0
    fi
  }
  renders "an ordinary ingester tier above one pod" \
    --set ingester.replicas=3 --set autoscaling.ingester.enabled=true \
    --set keda.enabled=true
  # The three neighbours of #3718's refusal. check-chart.py holds the metrics
  # each of these HPAs actually carries; here they only have to render.
  renders "claim-mode compactor scaling on CPU alone" \
    --set autoscaling.compactor.enabled=true --set compactor.catalogClaim.enabled=true
  renders "the compactor backlog metric in filesystem mode at one pod" \
    --set autoscaling.compactor.enabled=true --set autoscaling.compactor.maxReplicas=1 \
    --set autoscaling.compactor.customMetric.enabled=true
  renders "the ingester's per-pod custom metric under the catalog claim" \
    --set autoscaling.ingester.enabled=true \
    --set autoscaling.ingester.customMetric.enabled=true \
    --set compactor.catalogClaim.enabled=true
  # The operator's Prometheus address reaches its command line. The kind round's
  # schema-rollback arm installs this chart with exactly these flags, and an
  # operator that queries the wrong address holds every replica count while
  # staying Ready (#3489). The offline half of the same property is in
  # check-kind-schema-rollback-evidence.sh, which runs without helm.
  op_url=http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090
  if helm template t deploy/helm/siglake-operator \
    --set-string prometheus.url="$op_url" >"$LOG_DIR/operator-render.yaml" 2>>"$hlog"; then
    op_rendered=$(grep -A1 -F -- '- --prometheus-url' "$LOG_DIR/operator-render.yaml")
    case "$op_rendered" in
      *"$op_url"*) ;;
      *) echo "FAIL: the operator chart rendered no --prometheus-url $op_url" >>"$hlog"
         helm_ok=0 ;;
    esac
  else
    echo "FAIL: the operator chart did not render with the rollback arm's flags" >>"$hlog"
    helm_ok=0
  fi
  # There is deliberately no third guard here any more. The KEDA query ceiling
  # (maxReplicas above query.replicas) was refused only because --query-peers
  # was rendered from the replica count; since #967 the pods discover each
  # other from the headless Service's SRV record, so the range is SUPPORTED and
  # that render must now succeed. check-chart.py above carries the replacement
  # coverage: its `query-scale-out` scenario renders exactly the configuration
  # this used to reject, and check_query_peer_discovery holds the four
  # conditions that make it safe (no static list, SRV name matching a rendered
  # headless Service, Ready-only endpoints, pod name from the downward API).
  if [ "$helm_ok" = 1 ] && [ "$promtool_missing" = 1 ]; then
    report helm "skipped (promtool not installed; lint, renders, guards ok)"
  elif [ "$helm_ok" = 1 ]; then
    report helm ok
  else
    report helm FAIL; grep -E '^(FAIL|Error|\[ERROR\])' "$hlog" | head -5 | sed 's/^/  /' >&2
  fi
else
  report helm "skipped (helm not installed)"
fi

# --- public-tree -------------------------------------------------------------
job_started=$SECONDS
# CI runs this as a step of the helm job (ci.yml: "The tree that ships carries
# no dead references or local paths"). It gets its own line here because its
# failures are dead links and local paths, not chart defects.
if python3 scripts/check-public-tree.py >"$LOG_DIR/public-tree.log" 2>&1; then
  report public-tree ok
else
  report public-tree FAIL; head -5 "$LOG_DIR/public-tree.log" | sed 's/^/  /' >&2
fi

# --- generated ---------------------------------------------------------------
job_started=$SECONDS
# Regenerates in place, then compares against HEAD: the point is that the
# COMMITTED artifacts match what the code produces. CI does `git add -A` on
# docs/api and diffs the index, so a spec file the generator newly writes fails
# there; the plain `git diff -- docs/api` this used to run never sees untracked
# files and let it pass. `git status --porcelain` reports the same set —
# modified, deleted and untracked — without staging anything, so the index is
# exactly as it was when the script exits.
gen_why=()
glog="$LOG_DIR/generated.log"
: >"$glog"
gen_differed=0
run_generators() {
  local api_status f
  gen_why=()
  gen_differed=0
  cargo run -q --locked -p siglake-openapi -- --out docs/api >>"$glog" 2>&1 \
    || gen_why+=("siglake-openapi failed to run")
  api_status=$(git status --porcelain --untracked-files=all -- docs/api)
  if [ -n "$api_status" ]; then
    gen_differed=1
    gen_why+=("docs/api is stale (differs from HEAD):")
    while IFS= read -r line; do gen_why+=("  $line"); done < <(printf '%s\n' "$api_status" | head -5)
    { printf '%s\n' "$api_status"; git --no-pager diff --stat HEAD -- docs/api; } >>"$glog"
  fi
  cargo run -q -p siglake-operator -- --print-crd >"$LOG_DIR/crd.yaml" 2>>"$glog" \
    || gen_why+=("siglake-operator --print-crd failed")
  for f in deploy/operator/crd.yaml deploy/helm/siglake-operator/crds/siglakecluster.yaml; do
    if ! diff -u "$f" "$LOG_DIR/crd.yaml" >>"$glog" 2>&1; then
      gen_differed=1
      gen_why+=("$f is stale")
    fi
  done
}

run_generators
if [ "$gen_differed" -eq 1 ]; then
  workspace_sources=()
  for manifest in crates/*/Cargo.toml third_party/*/Cargo.toml; do
    crate_dir=${manifest%/Cargo.toml}
    if [ -f "$crate_dir/src/lib.rs" ]; then
      workspace_sources+=("$crate_dir/src/lib.rs")
    elif [ -f "$crate_dir/src/main.rs" ]; then
      workspace_sources+=("$crate_dir/src/main.rs")
    fi
  done
  # This confirmation still writes this checkout's units into the shared
  # target with newer mtimes, so another checkout can lose the same race in
  # reverse. Splitting manager target dirs is the complete fix; test binaries
  # are outside this generated-artifact check's scope.
  if touch "${workspace_sources[@]}" >>"$glog" 2>&1; then
    run_generators
    echo "first pass differed; rebuilt from this checkout and re-diffed (shared CARGO_TARGET_DIR race)" \
      >>"$glog"
  else
    gen_why=("failed to dirty workspace sources before regenerating")
  fi
fi
if [ "${#gen_why[@]}" -eq 0 ]; then
  report generated ok
else
  report generated FAIL; printf '  %s\n' "${gen_why[@]}" >&2
fi

# --- deny --------------------------------------------------------------------
job_started=$SECONDS
if command -v cargo-deny >/dev/null 2>&1; then
  dlog="$LOG_DIR/deny.log"
  # The advisory check refreshes the RustSec database over the network. Probe
  # that fetch separately so an offline laptop gets one parseable skip line,
  # while a fetched advisory or any license/source violation remains a FAIL.
  if ! cargo deny fetch db >"$dlog" 2>&1; then
    report deny "skipped (advisory db unreachable)"
  elif cargo deny check advisories licenses sources >>"$dlog" 2>&1; then
    report deny ok
  else
    report deny FAIL
  fi
else
  report deny "skipped (cargo-deny not installed)"
fi

# --- fork-tests --------------------------------------------------------------
job_started=$SECONDS
# The vendored forks reach the build only through the root manifest's
# [patch.crates-io], so the `test` job above compiles them as ordinary
# dependencies -- without --cfg test, never touching their #[cfg(test)]
# modules. Task #2651 found what that had cost: third_party/iceberg's lib-test
# target did not compile at all, one test asserted something false about the
# read path, and three asserted on wall-clock ordering, two of which failed.
# No gate could see any of it. This job is third_party/README.md's standalone
# recipe, run for iceberg, iceberg-catalog-sql and iceberg-storage-opendal with
# each fork's default features. --doc runs for iceberg (#2757) and
# iceberg-catalog-sql (#2784); iceberg-storage-opendal has no doc examples at
# all, so it has no doctest arm to gate. Its src/azdls.rs module stays out
# either way, behind the non-default `opendal-azdls` feature. The job has no
# counterpart in ci.yml yet: the public runner's disk budget is #180.
#
# ~75 s the first time on a box (iceberg-storage-opendal is ~30 s of that, it
# builds opendal's services from scratch), ~9 s after that, of which the
# doctests are ~5 s (~4 s iceberg, ~1 s iceberg-catalog-sql): rustdoc
# recompiles the merged doctest binary every run, there is nothing to keep
# warm. The mirror it builds is disposable but its path is not, so cargo's
# fingerprints for the fork's test artifacts stay fresh in CARGO_TARGET_DIR
# between runs.
#
# To watch this job go red without committing a broken module -- any of the
# three forks, same shape, with $f one of iceberg, iceberg-catalog-sql,
# iceberg-storage-opendal:
#   f=iceberg; d=$(mktemp -d); cp -r "third_party/$f/src" "$d/src"
#   printf '\n#[cfg(test)]\nmod x { fn b() -> u32 { "s" } }\n' >>"$d/src/lib.rs"
#   scripts/check-fork-tests.sh --fork "$f" --fork-src "$f=$d/src"
# The doctest arm, likewise, with a failing example instead:
#   printf '\n/// d\n///\n/// ```\n/// assert_eq!(1, 2);\n/// ```\npub fn f() {}\n' \
#     >>"$d/src/lib.rs"
if scripts/check-fork-tests.sh >"$LOG_DIR/fork-tests.log" 2>&1; then
  report fork-tests \
    "ok ($(awk '/^ok +[0-9]+ forks, / {sub(/^ok +[0-9]+ forks, /, ""); print; exit}' \
      "$LOG_DIR/fork-tests.log"))"
else
  report fork-tests FAIL
  grep -E '^(FAIL|  \[[a-z-]+\] error)' "$LOG_DIR/fork-tests.log" | head -5 | sed 's/^/  /' >&2
fi

# --- heavy jobs --------------------------------------------------------------
if [ "$WITH_HEAVY" = 1 ]; then
  # operator-cluster: the operator's actual job is reconciling a CR into
  # workloads, and `cargo test --workspace` cannot reach it — those tests are
  # #[ignore]d because they need a real API server.
  job_started=$SECONDS
  if command -v kind >/dev/null 2>&1 && command -v kubectl >/dev/null 2>&1; then
    olog="$LOG_DIR/operator-cluster.log"
    export KUBECONFIG="$LOG_DIR/kubeconfig"
    if ! kind create cluster --name siglake-ci-local --wait 120s \
      --kubeconfig "$KUBECONFIG" >"$olog" 2>&1; then
      report operator-cluster FAIL
      tail -20 "$olog" | sed 's/^/  /' >&2
    elif ! kubectl cluster-info >>"$olog" 2>&1 \
      || ! kubectl get nodes >>"$olog" 2>&1; then
      kind delete cluster --name siglake-ci-local --kubeconfig "$KUBECONFIG" >>"$olog" 2>&1
      report operator-cluster FAIL
      tail -20 "$olog" | sed 's/^/  /' >&2
    elif ! kubectl apply -f deploy/operator/crd.yaml >>"$olog" 2>&1; then
      kind delete cluster --name siglake-ci-local --kubeconfig "$KUBECONFIG" >>"$olog" 2>&1
      report operator-cluster FAIL
      tail -20 "$olog" | sed 's/^/  /' >&2
    else
      SIGLAKE_OPERATOR_REQUIRE_CLUSTER=1 \
        cargo test -p siglake-operator -- --ignored --test-threads=1 >>"$olog" 2>&1
      test_rc=$?
      read -r passed failed < <(
        awk '/^test result: (ok|FAILED)\./ {p+=$4; f+=$6} END {print p+0, f+0}' "$olog"
      )
      kind delete cluster --name siglake-ci-local --kubeconfig "$KUBECONFIG" >>"$olog" 2>&1
      if [ "$test_rc" -eq 0 ] && [ "$passed" -ge 4 ] && [ "$failed" -eq 0 ]; then
        report operator-cluster "ok ($passed tests)"
      else
        report operator-cluster "FAIL ($passed passed, $failed failed)"
        if [ "$passed" -lt 4 ] && [ "$failed" -eq 0 ]; then
          echo "  only $passed tests ran; expected 4+" >&2
        fi
        grep -E '^test .* \.\.\. FAILED$' "$olog" | sed 's/^/  /' >&2
      fi
    fi
  else
    report operator-cluster "skipped (needs kind + kubectl)"
  fi

  # docker: the images CI publishes plus native S3 pagination against MinIO.
  # The push needs cloud creds and remains out of scope here.
  job_started=$SECONDS
  if docker version >/dev/null 2>&1; then
    dk_ok=1
    preflight_ok=1
    image_sizes=
    dlog="$LOG_DIR/docker.log"
    export SIGLAKE_COMPOSE_PROJECT=siglake-ci-local
    export SIGLAKE_PG_HOST_PORT=15433
    export SIGLAKE_MINIO_HOST_PORT=19000
    export SIGLAKE_MINIO_CONSOLE_HOST_PORT=19001
    export SIGLAKE_OTLP_GRPC_HOST_PORT=14317
    export SIGLAKE_INGEST_METRICS_HOST_PORT=19100
    export SIGLAKE_COMPACTOR_METRICS_HOST_PORT=19101
    export SIGLAKE_QUERY_HOST_PORT=18089
    export SIGLAKE_QUERY_METRICS_HOST_PORT=19105
    export SIGLAKE_PROMETHEUS_HOST_PORT=19090
    export SIGLAKE_GARAGE_HOST_PORT=13900
    export SIGLAKE_GARAGE_ADMIN_HOST_PORT=13903
    : >"$dlog"
    if ci_local_choose_compose_ingest_port "$dlog"; then
      # Resolve the store the stack will run on, so the S3 test below points at
      # whatever scripts/up.sh brought up. SIGLAKE_OBJECT_STORE=minio unless the
      # caller selected garage (task #2958); CI always takes the default.
      # shellcheck source=scripts/compose-common.bash
      source scripts/compose-common.bash
    else
      dk_ok=0
      preflight_ok=0
    fi
    if [ "$preflight_ok" = 1 ] && scripts/up.sh --preflight-only >>"$dlog" 2>&1; then
      docker build -q -f deploy/Dockerfile -t siglake:ci-local . >>"$dlog" 2>&1 || dk_ok=0
      docker build -q -f deploy/Dockerfile.operator -t siglake-operator:ci-local . \
        >>"$dlog" 2>&1 || dk_ok=0
      # Measured before compose runs, so a red s3_mirror_pagination still
      # leaves the sizes of the images this run built in the log and the line.
      # See ci-local-image-sizes.sh: the measurement never decides the verdict,
      # and a half of it the daemon did not answer is named UNRECORDED rather
      # than dropped.
      if [ "$dk_ok" = 1 ]; then
        image_sizes=$(docker_image_sizes "$dlog")
      fi
      if scripts/up.sh >>"$dlog" 2>&1; then
        SIGLAKE_TEST_S3_ENDPOINT="$SIGLAKE_S3_HOST_ENDPOINT" \
        SIGLAKE_TEST_S3_ACCESS_KEY="$SIGLAKE_S3_ACCESS_KEY" \
        SIGLAKE_TEST_S3_SECRET_KEY="$SIGLAKE_S3_SECRET_KEY" \
          cargo test -p siglake-compactor --test s3_mirror_pagination -- \
            --ignored --nocapture >>"$dlog" 2>&1 || dk_ok=0
        # The shared-store job ownership rules (#1845) are the only thing
        # standing between a query scale-out and a destroyed batch result,
        # and no hermetic test can reach them: they are SQL. compose's
        # Postgres is the only one this repo starts, so the two-store
        # regression runs here or nowhere. The compose stack has no
        # query-server, so the sweep sees only this test's own rows. The
        # suite also covers cancellation propagation (#1846) and graceful
        # owner release (#1852). Its fleet-wide sweeps share this database,
        # so run its cases serially.
        SIGLAKE_TEST_JOBS_POSTGRES_URI="postgres://siglake:siglake@localhost:$SIGLAKE_PG_HOST_PORT/siglake" \
          cargo test -p siglake-query-server --test jobs_postgres_ownership -- \
            --ignored --nocapture --test-threads=1 >>"$dlog" 2>&1 || dk_ok=0
        # Same argument for the local drain's mirror-reclamation mark (#4913,
        # #4956): it decides which mirror objects retention may delete, the
        # deployed catalog is Postgres, and its hermetic cases run on SQLite.
        # `ON CONFLICT(id) DO NOTHING`'s rows_affected and the preserved
        # `COALESCE(committed_at_ms, $1)` stamp are backend behaviour the
        # parse-gate cannot establish. Each case gets its own Postgres schema,
        # so its claim never takes a row compose's ingest registered.
        SIGLAKE_TEST_JOBS_POSTGRES_URI="postgres://siglake:siglake@localhost:$SIGLAKE_PG_HOST_PORT/siglake" \
          cargo test -p siglake-storage --lib local_commit_mark_postgres -- \
            --ignored --nocapture >>"$dlog" 2>&1 || dk_ok=0
        # wal-recover's catalog reader uses the same deployed backend and has
        # three properties a parser or SQLite cannot establish: all 256 `$N`
        # binds work, the server refuses a write on the reader's own fenced
        # connection, and a missing table differs from an empty ledger.
        SIGLAKE_TEST_JOBS_POSTGRES_URI="postgres://siglake:siglake@localhost:$SIGLAKE_PG_HOST_PORT/siglake" \
          cargo test -p siglake-storage --lib wal_ledger_postgres -- \
            --ignored --nocapture >>"$dlog" 2>&1 || dk_ok=0
        # The candidate-local claim gate (#5189) is Postgres-only by
        # construction — a CTE, FOR UPDATE SKIP LOCKED and an UPDATE ... FROM
        # agg — and returns an empty vec before the statement on SQLite, so no
        # hermetic case runs a line of it. Same binary and same scratch-schema
        # isolation as the local mark above; same order as ci.yml, so the two
        # logs read alike.
        SIGLAKE_TEST_JOBS_POSTGRES_URI="postgres://siglake:siglake@localhost:$SIGLAKE_PG_HOST_PORT/siglake" \
          cargo test -p siglake-storage --lib eligible_claim_postgres -- \
            --ignored --nocapture >>"$dlog" 2>&1 || dk_ok=0
        # Multi-shard Postgres claims transition rows before applying the Rust
        # ownership filter. Prove a foreign row is released and its owner can
        # claim it; SQLite filters before its per-row UPDATE and cannot cover
        # that ordering.
        SIGLAKE_TEST_JOBS_POSTGRES_URI="postgres://siglake:siglake@localhost:$SIGLAKE_PG_HOST_PORT/siglake" \
          cargo test -p siglake-storage --lib sharded_claim_postgres -- \
            --ignored --nocapture >>"$dlog" 2>&1 || dk_ok=0
        # The consumed-proof watermark advances in the same transaction as the
        # segment transition. Exercise both its successful commit and rollback
        # after a Postgres statement failure; SQLite does not poison the open
        # transaction in the same way.
        SIGLAKE_TEST_JOBS_POSTGRES_URI="postgres://siglake:siglake@localhost:$SIGLAKE_PG_HOST_PORT/siglake" \
          cargo test -p siglake-storage --lib watermark_transaction_postgres -- \
            --ignored --nocapture >>"$dlog" 2>&1 || dk_ok=0
      else
        dk_ok=0
      fi
      scripts/down.sh >>"$dlog" 2>&1 || dk_ok=0
    else
      dk_ok=0
      preflight_ok=0
    fi
    report docker "$(docker_job_status "$dk_ok" "$preflight_ok" "$image_sizes")"
    if [ "$preflight_ok" = 0 ]; then
      sed 's/^/  /' "$dlog" >&2
    fi
  else
    report docker "skipped (no docker daemon; this environment needs sg docker -c)"
  fi

  # external-readers: the 2026-09-06 timestamp contract exists so that Spark,
  # DuckDB and PyIceberg can read a siglake warehouse. The siglake half of the
  # check (format version 2, the Iceberg field types, the `timestamp_ns`
  # round-trip, the total order) runs anywhere; each engine is skipped when it
  # is not installed, which is why this is a heavy job rather than a gate job.
  # Under --strict the engines are prerequisites like kind or promtool: the
  # checker is given its own --require-engines and a missing reader is a job
  # NOT RUN, not a green one. See ci-local-external-readers.sh.
  job_started=$SECONDS
  xlog="$LOG_DIR/external-readers.log"
  mapfile -t xopts < <(external_readers_args "$STRICT")
  xrc=0
  scripts/check-external-timestamp-contract.sh \
    ${xopts[@]+"${xopts[@]}"} >"$xlog" 2>&1 || xrc=$?
  xstatus=$(external_readers_status "$STRICT" "$xlog" "$xrc")
  report external-readers "$xstatus"
  if [[ "$xstatus" == FAIL* ]]; then
    tail -20 "$xlog" | sed 's/^/  /' >&2
  fi
fi

echo "---"
printf 'total %ss\n' "$((SECONDS - total_started))"
if [ "$fail" = 0 ]; then
  echo "ALL CHECKED JOBS GREEN"
elif [ "$red" = 0 ]; then
  # Only strict-mode skips: nothing that ran was red, but the jobs this box
  # could not run were not checked, and --strict refuses to call that green.
  echo "NO JOB RED, BUT $strict_skips NOT RUN — --strict does not call that green"
elif [ "$contamination_red" = 1 ]; then
  echo "SOME JOBS RED — test contamination does not show main would be red"
else
  echo "SOME JOBS RED — main would be red too"
fi
# Named on every run, not only a red one: a green summary read the next
# morning still has to say where a `skipped` job's reason or a slow job's
# output went, and the manager keeps this line with the run.
echo "  per-job logs: $LOG_DIR" >&2
exit "$fail"
