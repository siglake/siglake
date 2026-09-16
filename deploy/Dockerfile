# Multi-stage build: rust workspace → minimal runtime.
#
# We produce ONE image with three binaries: `siglake`, `siglake-query-server`,
# and `siglake-loadgen`. The Helm chart picks the entrypoint per pod role:
# - ingester:      siglake ingest-server --bind 0.0.0.0:8088
# - compactor:     siglake compactor
# - query-server:  siglake-query-server --bind 0.0.0.0:8089
# `siglake-loadgen` ships so fleet rounds can run in-cluster load jobs from
# the same image.
#
# Image is debian-slim rather than distroless because we need
# /etc/ssl/certs/ca-certificates.crt (for HTTPS to S3) and a working
# `hostname` binary (read by the ingest server to derive ingester_id).

# ---------- builder ----------
# `rust:1.95-slim` currently tracks Debian Trixie (glibc 2.39). The runtime
# below is pinned to the same Debian release so the binary's glibc deps
# resolve. If you bump the rust tag and the runtime FROM at the same time,
# keep them on the same Debian codename or you'll see GLIBC_2.39-not-found.
FROM rust:1.95-slim AS builder

# System deps for cargo build:
# - pkg-config + libssl: rustls is statically linked, but some transitive deps
#   prefer system openssl headers during build.
# - libsqlite3-dev: sqlx prefers a system sqlite; we statically link via the
#   `sqlite` feature, but headers are useful for compile.
# - clang/cmake/make: zstd-sys, blake3, snap, etc. all build C code.
# - binutils: `strip` for the debug-info strip below. The rust image's gcc
#   already depends on it; naming it keeps the strip from riding on that.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        libsqlite3-dev \
        clang \
        cmake \
        make \
        binutils \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
# Copy the manifest and lockfile first to maximize Docker layer caching:
# changes to source files only re-run the final cargo build, not the deps fetch.
COPY Cargo.toml Cargo.lock ./
COPY rust-toolchain.toml ./
COPY crates ./crates
# Vendored iceberg-rust read path ([patch.crates-io] points here).
COPY third_party ./third_party

# Build both binaries in release mode. `--locked` ensures we use exactly
# the versions in Cargo.lock (no opportunistic patch-version drift).
# The target cache is scoped by BUILD_CACHE_ID. Docker shares an unscoped cache
# mount across EVERY build on the host, so two source trees at different commits
# (a git worktree, say) write to the same /build/target -- and on 2026-08-16 that
# silently linked stale objects into a "freshly built" image. The registry cache
# is content-addressed and safe to share; this one is not.
#
# The binaries are copied out and then stripped of DWARF. The workspace release
# profile keeps `debug = 1` (Cargo.toml) so local perf work has line tables,
# which leaves each binary 600-740 MB and made the published image 1.95 GB.
# `--strip-debug` drops only the .debug_* sections and keeps .symtab, so panic
# backtraces and flat `perf` profiles in the container still name functions;
# what is lost is file:line resolution and DWARF-unwound call graphs (decided
# 2026-09-03: profiling rounds that need those build their own image). The
# copies are stripped rather than target/, so the cached cargo state is left
# alone. Measured 2026-09-03 on a local release build: siglake 600 -> 115 MB,
# siglake-query-server 630 -> 123 MB.
#
# SIGLAKE_CARGO_FEATURES is empty for every published image and for every
# ordinary build: the default below and `--features` being omitted entirely
# when it is empty keep the release binaries at the workspace's default
# features. It exists for the schema-rollback round
# (scripts/kind-schema-rollback-probe.sh), which needs a SECOND image built
# from this same checkout that declares one more `events` column — the only way
# to exercise a rollback between two images that differ in their declared
# schema. Comma-separated, `<crate>/<feature>` form, and it must be paired with
# its own BUILD_CACHE_ID: the target cache is not keyed by feature set, so
# reusing `default` would leave feature-built objects in the cache the release
# image links from.
ARG BUILD_CACHE_ID=default
ARG SIGLAKE_GIT_SHA
ARG SIGLAKE_CARGO_FEATURES=""
ENV SIGLAKE_GIT_SHA=${SIGLAKE_GIT_SHA}
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target,id=siglake-target-${BUILD_CACHE_ID} \
    cargo build --release --locked \
        ${SIGLAKE_CARGO_FEATURES:+--features ${SIGLAKE_CARGO_FEATURES}} \
        --bin siglake --bin siglake-query-server --bin siglake-loadgen \
 && cp target/release/siglake /usr/local/bin/siglake \
 && cp target/release/siglake-query-server /usr/local/bin/siglake-query-server \
 && cp target/release/siglake-loadgen /usr/local/bin/siglake-loadgen \
 && strip --strip-debug \
        /usr/local/bin/siglake \
        /usr/local/bin/siglake-query-server \
        /usr/local/bin/siglake-loadgen

# ---------- runtime ----------
FROM debian:trixie-slim AS runtime

# Minimum runtime deps: TLS roots for S3, hostname for the ingester id default.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        hostname \
        curl \
 && rm -rf /var/lib/apt/lists/*

# Non-root user. UID 65532 matches Google Distroless's `nonroot` so we're
# compatible with downstream pod-security policies that expect that UID.
RUN groupadd -r -g 65532 siglake \
 && useradd -r -u 65532 -g siglake -m -d /home/siglake -s /usr/sbin/nologin siglake

COPY --from=builder /usr/local/bin/siglake /usr/local/bin/siglake
COPY --from=builder /usr/local/bin/siglake-query-server /usr/local/bin/siglake-query-server
COPY --from=builder /usr/local/bin/siglake-loadgen /usr/local/bin/siglake-loadgen

# Pre-create the WAL directory with siglake ownership. Docker named volumes
# inherit the mount-point's ownership from the image on first creation
# when the volume is empty, so this avoids the "Permission denied" the
# non-root user would otherwise hit when the wal/ volume is mounted in.
RUN mkdir -p /var/lib/siglake/wal /var/lib/siglake/data \
 && chown -R siglake:siglake /var/lib/siglake

USER siglake
WORKDIR /home/siglake

# Default to printing help; the K8s pod spec sets the actual subcommand.
ENTRYPOINT ["/usr/local/bin/siglake"]
CMD ["--help"]

# Document the ingester (OTLP) + query-server ports; not required but helpful for
# `docker run -p`.
EXPOSE 8088
EXPOSE 8089
