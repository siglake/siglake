#!/usr/bin/env bash
#
# Build and test what the PROFILING=1 image ships: siglake-core's off-by-default
# `profiling` feature, in BOTH `tokio_unstable` configurations.
#
# `cargo clippy --workspace --all-targets` — the clippy job's one command —
# builds every member with DEFAULT features, so it never compiles
# `crates/siglake-core/src/profiling.rs` at all. Nothing else does either: the
# feature is off in every release build by design. Without this gate the module
# rots unseen and the next profiling round discovers it at image-build time,
# after the round has been scheduled.
#
# Both configurations, because tokio-metrics gates most of its counters behind
# `#[cfg(tokio_unstable)]` and the runtime route compiles a different field set
# for each. The profiling image sets the flag (deploy/Dockerfile's PROFILING=1
# arm); a developer running `cargo test -p siglake-core --features profiling`
# does not. A gate that checked one of them would pass while the other broke.
#
# One script rather than two copies of the commands, so ci.yml's `clippy` job
# and scripts/ci-local.sh's `profiling` job cannot drift: that drift is what
# check-shell-job-parity.py and check-ci-build-env.py exist to catch elsewhere.
#
# RUSTFLAGS is APPENDED to, never replaced. CI's cargo jobs carry
# `-C link-arg=-fuse-ld=mold` in their job env and check-ci-build-env.py
# requires every job-level RUSTFLAGS to be exactly that flag, so a second
# spelling here would either drop the linker or fail that gate.
#
# Usage: scripts/check-profiling-feature.sh
# Exits nonzero on the first configuration that fails.

set -euo pipefail
cd "$(dirname "$0")/.."

CRATE=siglake-core
FEATURE=profiling

# `<label>` `<extra RUSTFLAGS>` -> clippy and the crate's tests under it.
run_configuration() {
    local label=$1 extra=$2
    local flags="${RUSTFLAGS:-}${extra:+ $extra}"

    echo "== ${FEATURE} feature, ${label}"
    # --all-targets so the module's own test target is linted too, matching the
    # workspace clippy command it is invisible to.
    RUSTFLAGS="$flags" cargo clippy -p "$CRATE" --features "$FEATURE" \
        --all-targets -- -D warnings
    # The tests, not just the build: the cancellation and overlap regressions in
    # profiling.rs are the reason the admission ticket is a guard, and a build
    # check would not notice either coming back.
    RUSTFLAGS="$flags" cargo test -p "$CRATE" --features "$FEATURE"
}

run_configuration "default cfg" ""
run_configuration "--cfg tokio_unstable" "--cfg tokio_unstable"

echo "ok   ${CRATE}'s ${FEATURE} feature builds and tests in both tokio_unstable configurations"
