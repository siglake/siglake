#!/usr/bin/env bash
# The build settings ci.yml gives its cargo jobs, for scripts/ci-local.sh.
#
# Until 2026-09-11 (task #2947) the local gate applied none of them: every run
# built with full debuginfo, built incrementally, and linked with the default
# linker, so "the local gate passes" answered a question CI was not asking.
# `grep -n 'CARGO_PROFILE\|RUSTFLAGS\|CARGO_INCREMENTAL\|fuse-ld' scripts/ci-local.sh`
# returned nothing.
#
# Sourced by ci-local.sh and driven, both arms, by check-ci-build-env.py, which
# also diffs this file against .github/workflows/ci.yml and fails when the two
# drift.

# ci.yml's workflow-wide `env:` block (.github/workflows/ci.yml, above `jobs:`),
# minus the keys deliberately not mirrored -- CARGO_TERM_COLOR is the only one
# today, and the reason is in check-ci-build-env.py's NOT_MIRRORED. `NAME=value`
# entries so `export "${CI_ENV[@]}"` applies them and the checker reads them
# without running anything.
CI_ENV=(
  CARGO_INCREMENTAL=0
  CARGO_PROFILE_DEV_DEBUG=line-tables-only
  CARGO_PROFILE_TEST_DEBUG=line-tables-only
)

# The RUSTFLAGS every cargo job in ci.yml carries, in the same `env:` as its
# `apt-get install -y mold` (check-ci-linker.py keeps those two together). There
# is no second spelling for a box without mold: a fallback linker is what made
# the difference invisible, so the job reports itself unexercised instead.
CI_MOLD_FLAG='-C link-arg=-fuse-ld=mold'

# `<1 if mold is on PATH, else 0>` -> the `build-env` job's status string.
# Without mold the status is a `skipped (...)`, which report() prints as a job
# NOT RUN under --strict and which therefore keeps a strict run off ALL CHECKED
# JOBS GREEN.
build_env_status() {
  if [ "${1:-0}" = 1 ]; then
    printf 'ok (%s ci.yml env keys, mold)\n' "${#CI_ENV[@]}"
  else
    printf 'skipped (mold not installed; the cargo jobs are not exercised with %s)\n' \
      "ci.yml's linker"
  fi
}

# Same argument. Exports the mirrored keys unconditionally and the mold flag
# only when mold is there to honour it -- a RUSTFLAGS naming a linker that is
# not installed fails the first link with `collect2: fatal error: cannot find
# 'ld'`, which is the ci.yml `docker` failure check-ci-linker.py exists for.
apply_build_env() {
  export "${CI_ENV[@]}"
  if [ "${1:-0}" = 1 ]; then
    export RUSTFLAGS="$CI_MOLD_FLAG"
  fi
}
