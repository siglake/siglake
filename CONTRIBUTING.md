# Contributing to siglake

Thanks for your interest in siglake. This document covers the mechanics of
building, testing, and submitting changes.

## Building

siglake is a Rust workspace. The pinned toolchain is in `rust-toolchain.toml`
(currently rustc 1.95.0); `rustup` picks it up automatically.
To bump it, update the channel in `rust-toolchain.toml`, then run
`scripts/ci-local.sh` with the new toolchain before submitting the change.

```sh
cargo build --workspace
```

## Testing

Every change must pass the full gate before review. One command runs every
hosted CI job plus the local-only checks:

```sh
scripts/ci-local.sh          # build-env, fmt, shell, claude-md, set-var, dashboard, test, clippy, profiling, helm, public-tree, generated, deny, fork-tests
scripts/ci-local.sh --all    # + operator-cluster (needs kind), docker, external-readers
scripts/ci-local.sh --strict # a job this box cannot run (no helm, promtool, cargo-deny, kind, docker) is red, not skipped
scripts/ci-local.sh --log-dir DIR # keep this run's job logs in DIR (or set CI_LOCAL_LOG_DIR)
```

`--all` is a nightly or pre-release run, not the per-change gate: a kind
cluster plus two image builds add about twenty minutes, and CI runs both heavy
jobs on every push and pull request anyway. When you do run it, pair it with
`--strict`, so a machine without kind reports `operator-cluster` red instead of
skipping it under a green summary.

`fork-tests` runs only in the default local gate, and `external-readers` runs
only in the local `--all` gate. Hosted CI does not yet run either job.

Each job's full output is kept in a directory the run owns,
`${CARGO_TARGET_DIR:-target}/ci-local/<UTC stamp>-<pid>/<job>.log` unless you
pass `--log-dir`, and survives the run: the summary names the directory,
`${CARGO_TARGET_DIR:-target}/ci-local/latest` points at the newest run, a red
line prints its log path, and a red `test` line names every failed test rather
than the first few. Two runs that share a target directory neither wait on nor
overwrite each other. Run directories the script named are pruned after a week;
one you named is never touched.

The three you will reach for most, individually:

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

The other jobs matter more than they look. `generated` checks that the committed
OpenAPI specs and the two committed copies of the CRD match what the code
produces — that gate was failing on main for two days because a commit added a
field without regenerating, and nobody looked. `helm` renders both charts across
four value sets and asserts the install guards still refuse the configurations
they exist for; it has caught seven real defects, two of which were visible in
plain `helm template` output. `dashboard` is the half of that validator which
needs no render — every metric a Grafana panel queries is one the code emits,
in the form the exporter renders it, and the README's alert count matches the
PrometheusRule template — so a box without helm still runs it
(`scripts/check-chart.py --source-only`).

Most tests are hermetic (no network, no cloud). Tests that need object
storage spin up their infrastructure via `deploy/docker-compose.yml` or use
in-memory/`file://` backends — a plain `cargo test --workspace` needs no
external services.

## Local stack

A full local deployment (Postgres catalog + MinIO warehouse + siglake) is one
command:

```sh
docker compose -f deploy/docker-compose.yml up --build
```

See the README **Quickstart** for sending logs and querying.

`SIGLAKE_OBJECT_STORE=garage scripts/up.sh` runs the same stack with a Garage
warehouse instead of MinIO, behind compose profile `garage`. It is an
evaluation arm, not a supported configuration: MinIO is the default everywhere,
and nothing has been measured against Garage yet.

## Code conventions

- Ship complete states: each PR compiles, is clippy-clean, and is tested.
  Don't merge half-states behind long-lived feature branches.
- New user-visible limitations go in [`docs/LIMITATIONS.md`](docs/LIMITATIONS.md)
  — deferring something is fine, hiding it is not.
- Result caches must be snapshot-keyed, never TTL-expired: a cache entry is
  a pure function of `(table, snapshot, query)` and invalidates on commit.
  TTL'd result caches silently serve stale leading-edge answers.
- Storage owns physical ordering: data files are written ordered by the
  table's declared Iceberg `SortOrder`. Don't add per-component re-sorts.
- Env-var tuning knobs are named `SIGLAKE_*` and documented where they're
  read.

## Vendored forks

`third_party/iceberg`, `third_party/iceberg-catalog-sql`, and
`third_party/iceberg-storage-opendal` are first-class forks, not submodules —
see `third_party/README.md` for what diverged and why. Changes to fork code
follow the same test/clippy gate as the rest of the workspace.

## Submitting changes

1. Fork and branch from `main`.
2. Make your change with tests.
3. Run the full gate above.
4. Sign off every commit (`git commit -s`; see below).
5. Open a PR describing what changed and why. Small, focused PRs review
   faster than large ones.

### How CI starts on a fork's pull request

When the repository is configured to require approval for all outside
collaborators, your run sits at *waiting for approval* until a maintainer has
read the diff and added the `ci:run` label. Once the hosted approval path has
been qualified, that label releases the queued run for the exact commit; until
then a maintainer may release it from the Actions tab. Running
`scripts/ci-local.sh` before you open the pull request is therefore worth the
time: it covers every hosted job plus the local-only checks described above.

Push again and the label comes off. That is deliberate — an approval belongs to
the commit it was given for, not to the pull request — so a new commit needs a
new review and a new label. Nothing is wrong with your branch when this
happens.

### Licensing and sign-off

Contributions are licensed under the [Apache License 2.0](LICENSE), the same
license the project ships under: inbound is outbound. There is no Contributor
License Agreement (CLA) to sign.

Instead, every commit carries a `Signed-off-by:` trailer certifying the
[Developer Certificate of Origin 1.1](https://developercertificate.org/): that
you wrote the change, or otherwise have the right to submit it under the
project's license. `git commit -s` adds the trailer from your configured name
and email. A forgotten trailer is added with `git commit --amend -s` for one
commit, or `git rebase --signoff main` for a whole branch. Every commit in a
pull request must carry it.

## Reporting bugs

Use the issue templates. For security issues, **do not open a public
issue** — see `SECURITY.md`.
