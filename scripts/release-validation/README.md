# Public release validation

Customer-runnable release tests live in **siglake**. Comparative performance
benchmarks belong to **siglake/siglake-benchmarks**. Release acceptance, install
checks, cancellation soaks and burn-ins must not be dependencies of that public
comparison site. Source CI and a successful performance run do not certify the
published release image.

## Run a released artifact

Requirements: Linux, Docker Compose v2, Python 3.10+, git release tags, at least
16 GiB available RAM and 20 GiB free disk for `smoke`. Duration profiles need a
dedicated Docker backing filesystem sized for their rewrite volume. Concurrent
duration runs share that filesystem even though their Compose projects and
volumes are isolated, so budget their aggregate use and monitor free space. This
baseline allocates a 4 GiB, two-CPU limit to each product component, 1 GiB to
each dependency, and no swap. It is a single-host validation configuration, not
a claim to test stock Helm defaults.

```sh
python3 scripts/release-validation/run.py --version v0.2.1 --profile smoke \
  --out "$HOME/siglake-validation/v0.2.1-smoke-$(date -u +%Y%m%dT%H%M%SZ)"
```

Use `sg docker -c '...'` if your current session lacks the Docker group. Supported
profiles are `smoke` (at least 120 seconds of workload plus setup/assertions),
`24h` (86,400 seconds), and `72h` (259,200 seconds). Each starts from new volumes.
The 72h profile records an intermediate 24h checkpoint; it is not a separately
cleaned-up 24h run. Setup and final restart/cleanup time are outside the duration.
A directory must be new: a failure or retry never overwrites earlier evidence.

The harness resolves the released engine and operator GHCR manifests
**anonymously**, verifies their content digests and pulls those digests. Each
binary's `--version` must name the selected release and the commit behind its
git tag. Dependencies are also pulled with an empty Docker client auth
configuration and pinned by digest. It extracts Compose
settings from the selected release's git tag, removes builds and globally bound
ports, and exposes only randomly assigned loopback API/metrics ports. It disables
query result caching and the WAL query overlay so row assertions require committed
object-store data. Temporary known fixture credentials are local to this test.

`--dependency-policy cached-diagnostic` is an explicit escape for investigating
runtime behavior when a dependency registry is unavailable. It uses existing
MinIO/Postgres images and retains their digests; the Siglake image still pulls
anonymously. **It never qualifies anonymous clean installation.** The default
public policy fails rather than substituting a cache, mirror or locally built
product image.

## Duration and interruption

For 24h/72h runs, commit the harness first and detach it from your terminal:

```sh
python3 scripts/release-validation/start.py --version v0.2.1 --profile 72h \
  --results-root "$HOME/siglake-validation"
```

This creates an immutable detached worktree and a bounded systemd user service.
The command prints its unit, log and results paths. Inspect `summary.json` and
`events.jsonl`; stop the printed unit with `systemctl --user stop UNIT`.
`ExecStopPost` repeats idempotent cleanup if the runner exits abnormally. A user
manager configured with lingering survives logout. A tmux crash does not kill
this service. A host reboot interrupts, rather than resumes, qualification:

```sh
python3 scripts/release-validation/recover.py /absolute/path/to/interrupted-run
```

Run recovery before repeating after a reboot. It verifies the recorded project
identity and configuration hash, removes only that project's containers, network
and volumes, and records remaining resources. Never use global `docker prune`.
A cleanup failure makes the result fail. Evidence, harness snapshots and shared
image layers remain for review; remove these deliberately after retention ends.

The manual `Public release validation` GitHub workflow runs a two-version smoke
matrix on hosted runners and duration profiles on a dedicated runner labeled
`siglake-validation`. It uploads results even on failure and invokes recovery.
A hard runner loss requires the recovery command above; an `always()` step cannot
run on a host that is gone. Long profiles do not fit hosted runner time limits.

## What is checked

Every cycle writes 100 deterministic OTLP records, including out-of-order event
times, and waits at most 120 seconds for their **exact IDs** in committed data.
Three simultaneous queries verify total, four grouped counts and filtered count;
no `count >= expected` shortcut accepts loss plus duplicates. No ingest retry is
hidden: an uncertain acknowledgment fails this profile. A batch job must return
the same oracle answer. Missing bearer credentials must be refused.

Before setup, the run records the available bytes on the filesystem backing
Docker's root directory. Each minute it captures that reading with container
state, enforced limits, Docker statistics and query metrics, refusing OOM,
unexpected restarts or stopped services. If a cohort's 120-second deadline
contains a MinIO HTTP 507 `XMinioStorageFull` response from the same Compose
project, the failure is reported as backing-store exhaustion instead of an
unexplained visibility mismatch. No capacity threshold is inferred from that
reading.
Query and compactor restart hourly; all three product components restart at the
end, with exact counts checked again. Load is bounded (100 events/cycle, three
simultaneous queries), not a saturation or large-working-set benchmark. There is
no implied memory-leak proof from staying below a hard limit.

The following remain separate required qualification tracks: Kubernetes/Helm
and operator install; OIDC tenant isolation; distributed replicas; upgrades;
retention/deletion and schema evolution; catalog/object-store outages; sustained
large-working-set traffic; expensive-query cancellation. A baseline profile
`passed` is **not** a full release gate. Backfill tracking includes all these
tracks for v0.1.0/v0.2.0/v0.2.1; do not mark them passed without retained evidence.

`cancellation-soak.sh` migrated from the benchmarks repository. It targets a
separately provisioned dedicated endpoint and writes into a new `OUT`. Its slow
query preflight must prove work is still in flight when clients disconnect;
small-data runs that cannot meet that condition are unqualified. Provisioning
and cleanup are the caller's responsibility; use the run-owned deployment
lifecycle above when integrating it into a larger qualification profile.

## Evidence and publication

`summary.json` carries version, source/harness identity, exact images, profile,
configuration checksum, checks, omissions, progress, final outcome and cleanup.
`events.jsonl`, `resources.jsonl`, `metrics.prom`, final service logs and the
rendered Compose configuration support review. `sha256.json` checksums final
files. SIGTERM/SIGINT records `interrupted`; failure preserves its cause. A
running summary is never a pass. Hashes are integrity checks, not signatures.

Commit small reviewed summaries under `results/release-validation/`; keep bulk
logs in durable artifact storage (workflow artifacts default to 90 days). Review
logs/configuration before publication and retain a durable URL plus checksum in
the public docs. Record failures and retries separately. Never commit real
credentials, production logs or customer data. The public docs ledger records
actual elapsed results and all remaining gaps.
