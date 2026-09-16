use std::io::BufRead;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use datafusion::prelude::SessionContext;
use object_store::path::Path as ObjectPath;
use tokio::sync::Mutex;
use uuid::Uuid;

use siglake_compactor::Compactor;
use siglake_core::{events_schema, events_to_record_batch, Event};
use siglake_ingest::{AppState, TenantRouting};
use siglake_storage::iceberg::{GroupCountRebuildOptions, IcebergContext};
use siglake_storage::subscribe::IcebergSubscription;
use siglake_storage::{local_store, register_parquet_dir, session_context, write_batch_as_parquet};
use siglake_wal::WalWriter;

mod sql_client;

const DEFAULT_OTLP_GRPC_LISTEN: &str = "0.0.0.0:4317";

#[derive(Parser, Debug)]
#[command(
    name = "siglake",
    about = "siglake command-line interface: servers, maintenance jobs and a SQL client",
    version = siglake_core::BUILD_VERSION
)]
struct Cli {
    /// Root directory used as the local object store.
    #[arg(long, default_value = "./data", global = true)]
    data_dir: PathBuf,

    #[command(subcommand)]
    command: Command,
}

// A clap subcommand enum holds one variant per command, each carrying that
// command's whole flag set, so the variants are inherently uneven — the
// ingest-server has dozens of knobs and `gen` has three. Boxing a variant to
// even them out would buy nothing: exactly one is ever constructed, once, at
// startup.
#[allow(clippy::large_enum_variant)]
#[derive(Subcommand, Debug)]
enum Command {
    /// Interactive SQL client for a RUNNING siglake query server
    /// (`/api/v1/sql`): one-shot with a query argument, or a REPL without.
    /// Table output includes the per-query scan stats + server time; use
    /// `--dry-run` for the cost estimate without executing.
    Sql {
        /// SQL to run once; omit for an interactive session.
        query: Option<String>,
        /// Query server base URL.
        #[arg(
            long,
            env = "SIGLAKE_ENDPOINT",
            default_value = "http://localhost:8089"
        )]
        endpoint: String,
        /// Bearer token (when the server runs with --auth-tokens / OIDC).
        #[arg(long, env = "SIGLAKE_TOKEN")]
        token: Option<String>,
        /// Output format.
        #[arg(long, value_enum, default_value = "table")]
        format: sql_client::SqlOutput,
        /// Cost estimate only — plan the query without executing it.
        #[arg(long)]
        dry_run: bool,
        /// Suppress the stats footer.
        #[arg(long)]
        quiet: bool,
    },
    /// Ingest newline-delimited JSON events.
    Ingest {
        /// NDJSON input file. If omitted, reads from stdin.
        #[arg(long)]
        input: Option<PathBuf>,
    },
    /// Run a SQL query against the ingested events. Table name: `events`.
    Query {
        /// SQL query string.
        #[arg(long)]
        sql: String,
    },
    /// Generate `n` synthetic events to stdout (NDJSON) for local testing
    /// and demos.
    Gen {
        /// Number of events to generate.
        #[arg(long, default_value_t = 100)]
        n: usize,
    },
    /// Iceberg demo: append `n` synthetic events through the SQLite-backed
    /// `IcebergContext`, then run a few canned SQL queries against the
    /// resulting snapshot.
    ///
    /// The catalog (SQLite) and data (Parquet + manifests) persist between
    /// runs, so a second invocation appends to the same table rather than
    /// recreating it. Use `--reset` to wipe the warehouse first.
    IcebergDemo {
        /// Number of events to ingest.
        #[arg(long, default_value_t = 1000)]
        n: usize,
        /// Warehouse subdirectory under `--data-dir`.
        #[arg(long, default_value = "warehouse")]
        warehouse: String,
        /// Wipe the warehouse before running.
        #[arg(long, default_value_t = false)]
        reset: bool,
    },

    /// Run the OTLP ingest server.
    ///
    /// HTTP endpoints:
    /// - POST /v1/logs        — OTLP/HTTP logs (JSON or protobuf)
    /// - GET  /api/v1/stream  — SSE tail, teed ahead of the WAL append
    /// - GET  /healthz        — liveness/readiness
    ///
    /// Tenancy is SINGLE-TENANT by default: every request routes to the
    /// `default` tenant, and `X-Scope-OrgID` naming another one is refused.
    /// Multi-tenant routing is explicit — `--oidc-tenant-claim` takes the
    /// tenant from a verified JWT, `--trust-scope-header` takes the client's
    /// word. Each tenant gets its own WAL subtree + Iceberg namespace.
    ///
    /// Without `--with-compactor`, this only writes WAL segments — you'll
    /// need to run `siglake compactor` separately to commit them to Iceberg.
    /// With `--with-compactor`, runs an in-process compactor every 1s for
    /// single-process demos.
    IngestServer {
        /// Address to bind. The OTLP/HTTP ingest surface (`POST /v1/logs`)
        /// serves here; 8088 is the long-standing default and is kept so
        /// existing deployments do not have to move.
        #[arg(long, default_value = "0.0.0.0:8088")]
        bind: SocketAddr,
        /// Address to bind for the Prometheus `/metrics` endpoint.
        #[arg(long, default_value = "0.0.0.0:9100")]
        metrics_bind: SocketAddr,
        /// Subdirectory under `--data-dir` (or absolute path) for WAL segments.
        #[arg(long, default_value = "wal")]
        wal: String,
        /// Subdirectory under `--data-dir` for the Iceberg warehouse,
        /// when running locally with no `--warehouse-url`.
        #[arg(long, default_value = "warehouse")]
        warehouse: String,
        /// Full warehouse URL (e.g. `s3://bucket/prefix`). Overrides
        /// `--warehouse`. Reads `SIGLAKE_WAREHOUSE_URL` if not given.
        #[arg(long, env = "SIGLAKE_WAREHOUSE_URL")]
        warehouse_url: Option<String>,
        /// Iceberg catalog URI (e.g. `postgres://user:pass@host/db`,
        /// `sqlite://path?mode=rwc`). Reads `SIGLAKE_CATALOG_URI` if not
        /// given. Defaults to a SQLite db under the warehouse dir.
        #[arg(long, env = "SIGLAKE_CATALOG_URI")]
        catalog_uri: Option<String>,
        /// Roll the WAL segment after this many events.
        #[arg(long, default_value_t = 4096)]
        wal_max_events: usize,
        /// Roll the WAL segment after this many seconds.
        #[arg(long, default_value_t = 5)]
        wal_max_age_secs: u64,
        /// Also run a polling compactor in-process (every 1s).
        #[arg(long, default_value_t = false)]
        with_compactor: bool,
        /// OTLP/gRPC listen address. Use --disable-otlp-grpc to turn it off.
        #[arg(
            long,
            env = "SIGLAKE_OTLP_GRPC_LISTEN",
            default_value = DEFAULT_OTLP_GRPC_LISTEN,
            conflicts_with = "disable_otlp_grpc"
        )]
        otlp_grpc_listen: Option<std::net::SocketAddr>,
        /// Disable the default OTLP/gRPC logs and traces listener.
        #[arg(long, default_value_t = false)]
        disable_otlp_grpc: bool,
        /// WAL → object-store mirror at `<warehouse-url>/<prefix>/`. Unset
        /// mirrors to `wal-mirror/` whenever `--warehouse-url` is set (the
        /// default since 2026-09-11), and is off without one. Pass an EMPTY
        /// value to turn the mirror off.
        #[arg(long, env = "SIGLAKE_WAL_MIRROR_PREFIX")]
        wal_mirror_prefix: Option<String>,
        /// When > 0 and `--wal-mirror-prefix` is set, also mirror the
        /// currently-active segment every N seconds to
        /// `<prefix>/_active/<filename>`. N is the upload window for the
        /// in-flight segment, and it becomes an N-second data-loss bound
        /// on ONE recovery path: a successful upload is recovered when an
        /// operator runs `siglake wal-recover` onto the WAL root and the
        /// filesystem drain commits what it finds. Nothing reads an
        /// active snapshot on its own, and the catalog-claim drain
        /// reconciles sealed objects only — it never reads `_active/` and
        /// gets no N-second target.
        #[arg(
            long,
            env = "SIGLAKE_WAL_ACTIVE_MIRROR_INTERVAL_SECS",
            default_value_t = 0
        )]
        wal_active_mirror_interval_secs: u64,
        /// Comma-separated bearer tokens. When set, every request must
        /// carry `Authorization: Bearer <token>` where `<token>` matches one
        /// of these. These tokens say who may write, not what they may write
        /// as: binding tenancy to the caller is `--oidc-tenant-claim`.
        /// Empty/unset = no auth (the v0 behavior; only safe inside a trusted
        /// network).
        #[arg(long, env = "SIGLAKE_AUTH_TOKENS")]
        auth_tokens: Option<String>,
        /// OIDC issuer URL for JWT verification (e.g.
        /// `https://cognito-idp.us-east-1.amazonaws.com/<pool-id>`).
        /// When set together with `--oidc-audience`, every request must
        /// carry a valid `Authorization: Bearer <jwt>`. Takes precedence
        /// over `--auth-tokens`.
        #[arg(long, env = "SIGLAKE_OIDC_ISSUER")]
        oidc_issuer: Option<String>,
        /// OIDC audience (client ID) to validate in the JWT `aud` claim.
        #[arg(long, env = "SIGLAKE_OIDC_AUDIENCE")]
        oidc_audience: Option<String>,
        /// JWT claim that carries the tenant. When set, the tenant comes from
        /// the VERIFIED token rather than the `X-Scope-OrgID` header, and a
        /// header naming a different tenant is refused.
        ///
        /// Requires `--oidc-issuer` and `--oidc-audience`: the tenant comes
        /// from a verified token, so with no verifier there is nothing to take
        /// it from. Setting it alone — with static `--auth-tokens`, with open
        /// auth, or with `--trust-scope-header` still routing on the header —
        /// is refused at startup rather than accepted and ignored.
        ///
        /// Setting it also makes the claim MANDATORY: a verified token whose
        /// claim is missing, blank, not a string, longer than 128 chars, or
        /// outside `[A-Za-z0-9_-]` is refused with `403` before the batch is
        /// routed anywhere — token validity is not tenant authorization. The
        /// value is validated, never repaired, so an unusable claim never
        /// falls back to the `default` tenant. Refusals are counted by
        /// `siglake_ingest_tenant_denied_total`
        /// (`reason="claim_missing"` / `"claim_invalid"` /
        /// `"header_mismatch"`).
        ///
        /// Set this on any shared deployment: it is the only way to route
        /// tenants that does not take a client header at its word. Without it
        /// this ingester is single-tenant unless `--trust-scope-header` says
        /// otherwise.
        #[arg(long, env = "SIGLAKE_OIDC_TENANT_CLAIM")]
        oidc_tenant_claim: Option<String>,
        /// Let the unverified `X-Scope-OrgID` header select the tenant.
        ///
        /// OFF by default since 2026-09-11: this ingester is single-tenant
        /// unless told otherwise, and a header naming any tenant but `default`
        /// is refused with `403` rather than honoured or ignored. Prefer
        /// `--oidc-tenant-claim`, which binds the tenant to a verified
        /// identity; reach for this only where a gateway in front of the
        /// ingester sets the header itself and strips the client's.
        ///
        /// Accepts `1`/`true`/`yes`/`on` (or the bare flag). Anything else,
        /// including an unset or empty value, leaves the header untrusted: a
        /// typo must not silently open multi-tenant routing.
        #[arg(
            long,
            env = "SIGLAKE_TRUST_SCOPE_HEADER",
            num_args = 0..=1,
            default_missing_value = "true",
        )]
        trust_scope_header: Option<String>,
        /// Steady-state request rate per bearer token (or per
        /// `X-Forwarded-For` IP in open mode). 0 disables the limiter.
        #[arg(long, env = "SIGLAKE_INGEST_RATE_PER_SEC", default_value_t = 0.0)]
        ingest_rate_per_sec: f64,
        /// Burst capacity for the token-bucket rate limiter. Ignored
        /// when `--ingest-rate-per-sec=0`.
        #[arg(long, env = "SIGLAKE_INGEST_RATE_BURST", default_value_t = 0.0)]
        ingest_rate_burst: f64,
        /// Redis URL for a cross-replica shared rate budget.
        /// Empty/unset = per-replica in-memory limiter
        /// (the default). When set, the rate-per-sec / burst values
        /// apply to a single token-bucket shared across every
        /// ingester replica via a Redis Lua script. Example:
        /// `redis://redis.siglake-system:6379/0`.
        #[arg(long, env = "SIGLAKE_INGEST_RATE_REDIS_URL")]
        ingest_rate_redis_url: Option<String>,
        /// Hash-key prefix the Redis rate budget uses. Defaults to
        /// `siglake:rb`. Use a unique prefix per siglake deployment
        /// that shares a Redis with other tenants.
        #[arg(long, env = "SIGLAKE_INGEST_RATE_REDIS_PREFIX")]
        ingest_rate_redis_prefix: Option<String>,
        /// Per-tenant capacity of the mpsc-fed writer queue.
        /// Default 1024 enables the BackpressureRouter
        /// (non-blocking mpsc path); set 0 to opt out and keep the
        /// legacy mutex-serialized path. A full lane returns 503 +
        /// `Retry-After` instead of blocking on `Mutex<WalWriter>`.
        /// Tune up for higher per-tenant burst tolerance.
        #[arg(
            long,
            env = "SIGLAKE_INGEST_BACKPRESSURE_CAPACITY",
            default_value_t = 1024
        )]
        ingest_backpressure_capacity: usize,
        /// Group commit. When >0, the per-tenant writer
        /// task waits this many ms after the first command for more
        /// commands to accumulate before flushing. Trades p50 ack
        /// latency for amortized fsync cost across many batches.
        /// Requires `--ingest-backpressure-capacity > 0`.
        #[arg(long, env = "SIGLAKE_INGEST_GROUP_COMMIT_MS", default_value_t = 0)]
        ingest_group_commit_ms: u64,
        /// Per-tenant write parallelism. N writer tasks
        /// per tenant; ingest handlers round-robin across them. 1
        /// (default) preserves the single-writer-per-tenant
        /// behavior. Requires `--ingest-backpressure-capacity > 0`.
        #[arg(long, env = "SIGLAKE_INGEST_BACKPRESSURE_SHARDS", default_value_t = 1)]
        ingest_backpressure_shards: usize,
        /// RSS memory circuit breaker (MiB). When >0, a background
        /// task samples this process's resident set every
        /// `--ingest-mem-sample-secs`; ingest handlers shed load with
        /// `503` + `Retry-After` while RSS is at or above this ceiling.
        /// 0 (default) disables the breaker.
        #[arg(long, env = "SIGLAKE_INGEST_MEM_LIMIT_MIB", default_value_t = 0)]
        ingest_mem_limit_mib: u64,
        /// RSS sampling interval (seconds) for the memory circuit breaker.
        /// Only meaningful when `--ingest-mem-limit-mib > 0`.
        #[arg(long, env = "SIGLAKE_INGEST_MEM_SAMPLE_SECS", default_value_t = 60)]
        ingest_mem_sample_secs: u64,
        /// Tenants this ingester accepts, comma-separated. Empty = any.
        /// Checked against the tenant actually resolved, so it bounds a JWT
        /// claim as well as a trusted header.
        ///
        /// Each novel tenant mints a backpressure lane holding an open file, a
        /// fresh set of metric label values, and an Iceberg namespace with
        /// seven tables downstream. Set this on any deployment whose tenants
        /// are known.
        #[arg(long, value_delimiter = ',', env = "SIGLAKE_ALLOWED_TENANTS")]
        allowed_tenants: Vec<String>,
        /// Distinct tenants to mint before refusing new ones. 0 = unbounded.
        ///
        /// The backstop for when the tenant set is not known ahead of time.
        /// Refusing an unexpected tenant is a bad day; exhausting file
        /// descriptors is an outage for every tenant that was real.
        ///
        /// Counts tenants, not `(tenant, index)` lanes — see
        /// `--ingest-max-lanes` for those. At the cap the tenants already
        /// writing are unaffected and a novel one is refused with `403`
        /// (`siglake_ingest_tenant_denied_total{reason="at_capacity"}`). The
        /// count is this process's: it starts empty on restart, and each pod
        /// holds its own.
        #[arg(long, env = "SIGLAKE_MAX_TENANTS", default_value_t = 0)]
        max_tenants: usize,
        /// Distinct `(tenant, index)` backpressure lanes to create before
        /// refusing new ones. 0 = unbounded.
        ///
        /// Each lane holds an open file per shard. Both halves of the key are
        /// client headers, so the cross product is client-controlled.
        #[arg(long, env = "SIGLAKE_INGEST_MAX_LANES", default_value_t = 0)]
        ingest_max_lanes: usize,
    },

    /// Disaster-recovery: pull every WAL segment under
    /// `<warehouse-url>/<prefix>/` back onto a local WAL root, rebuilding the
    /// `<tenant>[/<index>]/sealed/` layout so the ordinary drain commits each
    /// segment to the namespace and table it came from. Skips segments already
    /// present locally, and recovers the active mirror too (preferring the
    /// sealed copy of any segment present as both).
    WalRecover {
        /// Full source URL (e.g. `s3://bucket/wal-mirror`).
        #[arg(long, env = "SIGLAKE_WAL_MIRROR_URL")]
        from: String,
        /// Local WAL ROOT to populate — the same path the ingester and
        /// compactor are pointed at (e.g. `/var/lib/siglake/wal`), NOT a
        /// `sealed/` subdirectory. The tenant/index layout is rebuilt beneath
        /// it.
        #[arg(long)]
        to: PathBuf,
    },

    /// Return segments the drain set aside under `<wal>/poison/` to `sealed/`,
    /// so the next drain cycle claims them again.
    ///
    /// A segment lands there after failing to read on every attempt the drain
    /// gave it (`SIGLAKE_COMPACTOR_POISON_ATTEMPTS`), which is why nothing
    /// requeues it automatically: the retry that would change the answer is
    /// the one an operator does first — restoring the file from the mirror or
    /// a backup, or upgrading to a build that knows its frame version.
    /// Requeueing a segment unchanged simply spends the attempts again.
    ///
    /// Prints one line per set-aside segment with the recorded reason. Refuses
    /// to move a segment whose name is already back in `sealed/`.
    WalRequeue {
        /// The WAL ROOT (e.g. `/var/lib/siglake/wal`), not a `poison/`
        /// directory. Its own `poison/`, every tenant's and every index's are
        /// all visited.
        #[arg(long)]
        wal: PathBuf,
        /// Requeue only this segment file name. Default: every set-aside
        /// segment under the root.
        #[arg(long)]
        segment: Option<String>,
        /// Report what would move without moving anything.
        #[arg(long)]
        dry_run: bool,
    },

    /// Bound the `query_audit` Iceberg table's growth.
    ///
    /// Two modes:
    ///   * default — **drop + recreate** (a coarse row-TTL: deletes ALL
    ///     historical audit rows; iceberg-rust 0.9 has no public row-level
    ///     delete). The recreated table keeps the same schema/partition/sort.
    ///   * `--max-age-secs N` — **non-destructive snapshot-age sweep**: keep
    ///     the rows, but expire `query_audit` snapshots older than N seconds
    ///     (the per-flush commit churn) so the metadata.json stays bounded,
    ///     then reclaim the now-unreferenced files. Uses the in-fork
    ///     `expire_snapshots` age sweep + the same orphan GC as `gc-orphans`.
    ///
    /// Schedule on a CronJob; the running query-server picks up the table on
    /// its next audit write (`ensure_query_audit_table` is idempotent).
    /// `--dry-run` reports what would happen without touching the catalog.
    AuditRotate {
        /// Iceberg warehouse URL (`s3://bucket/prefix`,
        /// `file:///abs/path`). Reads `SIGLAKE_WAREHOUSE_URL` if not
        /// given.
        #[arg(long, env = "SIGLAKE_WAREHOUSE_URL")]
        warehouse_url: Option<String>,
        /// Iceberg catalog URI (`postgres://...`, `sqlite://...`).
        /// Reads `SIGLAKE_CATALOG_URI` if not given.
        #[arg(long, env = "SIGLAKE_CATALOG_URI")]
        catalog_uri: Option<String>,
        /// Per-deployment Iceberg namespace. Defaults to the
        /// chart's `tenant.namespace` value (`siglake`).
        #[arg(long, env = "SIGLAKE_TENANT_NAMESPACE", default_value = "siglake")]
        namespace: String,
        /// Table to rotate. Defaults to `query_audit`. Any table works with
        /// `--max-age-secs` (the non-destructive snapshot-age sweep — including
        /// any user index, e.g. a consumer's own output tables). The
        /// destructive drop-and-recreate path (no `--max-age-secs`) is
        /// restricted to `query_audit`.
        #[arg(long, default_value = "query_audit")]
        table: String,
        /// Non-destructive mode: expire snapshots older than this many seconds
        /// (keeping rows) + reclaim orphans, instead of drop-and-recreate.
        #[arg(long)]
        max_age_secs: Option<u64>,
        /// Print what would happen and exit 0; don't touch the
        /// catalog. Useful for verifying the catalog URI + warehouse
        /// URL before scheduling the rotate.
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },

    /// Reclaim orphan files: physically delete data/manifest/
    /// manifest-list files under a table's location that no retained
    /// snapshot references — the storage left behind by re-clustering
    /// overwrites + snapshot expiry.
    ///
    /// **Dry-run by default** — reports the orphan count + bytes and deletes
    /// nothing. Pass `--apply` to actually delete. Files modified within
    /// `--min-age-secs` are skipped (guards the in-flight-write race). Run on
    /// a periodic cadence (CronJob) per table. file/s3 warehouses only.
    GcOrphans {
        /// Iceberg warehouse URL. Reads `SIGLAKE_WAREHOUSE_URL` if not given.
        #[arg(long, env = "SIGLAKE_WAREHOUSE_URL")]
        warehouse_url: Option<String>,
        /// Iceberg catalog URI. Reads `SIGLAKE_CATALOG_URI` if not given.
        #[arg(long, env = "SIGLAKE_CATALOG_URI")]
        catalog_uri: Option<String>,
        /// Per-deployment Iceberg namespace.
        #[arg(long, env = "SIGLAKE_TENANT_NAMESPACE", default_value = "siglake")]
        namespace: String,
        /// Table to GC (`events`, `query_audit`, or any index id).
        #[arg(long, default_value = "events")]
        table: String,
        /// Skip files modified within this many seconds (safety window
        /// against deleting a concurrently-written, not-yet-referenced file).
        #[arg(long, default_value_t = 86400)]
        min_age_secs: u64,
        /// Actually delete. Without this, the command is a dry-run.
        #[arg(long, default_value_t = false)]
        apply: bool,
    },

    /// Enforce per-index retention policies by dropping whole data files whose
    /// manifest max timestamp is older than the configured horizon.
    ///
    /// Dry-run by default: prints the files/bytes/rows that would be removed
    /// and leaves the snapshot untouched. This command only performs the file
    /// drop rewrite; compose it with the existing snapshot-expiry + orphan-GC
    /// sweeps to reclaim superseded files physically. A Helm CronJob would
    /// mirror `audit-rotate`: same image/env, different subcommand.
    RetentionSweep {
        /// Iceberg warehouse URL. Reads `SIGLAKE_WAREHOUSE_URL` if not given.
        #[arg(long, env = "SIGLAKE_WAREHOUSE_URL")]
        warehouse_url: Option<String>,
        /// Iceberg catalog URI. Reads `SIGLAKE_CATALOG_URI` if not given.
        #[arg(long, env = "SIGLAKE_CATALOG_URI")]
        catalog_uri: Option<String>,
        /// Per-deployment Iceberg namespace.
        #[arg(long, env = "SIGLAKE_TENANT_NAMESPACE", default_value = "siglake")]
        namespace: String,
        /// Optional managed index id. Absent => sweep every managed index in the
        /// resolved tenant namespace.
        #[arg(long)]
        index: Option<String>,
        /// Actually apply the retention rewrite. Without this, the command is a
        /// dry-run.
        #[arg(long, default_value_t = false)]
        apply: bool,
    },

    /// Execute pending GDPR/delete tasks for one managed index.
    ///
    /// Dry-run by default: evaluates the pending tasks and reports the files and
    /// rows they would rewrite without touching the snapshot or ledger.
    DeleteSweep {
        /// Iceberg warehouse URL. Reads `SIGLAKE_WAREHOUSE_URL` if not given.
        #[arg(long, env = "SIGLAKE_WAREHOUSE_URL")]
        warehouse_url: Option<String>,
        /// Iceberg catalog URI. Reads `SIGLAKE_CATALOG_URI` if not given.
        #[arg(long, env = "SIGLAKE_CATALOG_URI")]
        catalog_uri: Option<String>,
        /// Per-deployment Iceberg namespace.
        #[arg(long, env = "SIGLAKE_TENANT_NAMESPACE", default_value = "siglake")]
        namespace: String,
        /// Managed index id whose pending delete tasks should execute.
        #[arg(long)]
        index: String,
        /// Actually apply the rewrites and advance the ledger. Without this, the
        /// command is a dry-run.
        #[arg(long, default_value_t = false)]
        apply: bool,
    },

    /// Rebuild a table's group-count aggregate from the committed data files.
    ///
    /// The operator fallback for a LOST group-count delta. A delta write that
    /// exhausts its retries leaves a durable marker, and the maintenance
    /// compactor normally rebuilds the aggregate on its next fold. If that
    /// automatic rebuild fails or remains incomplete, the query guard refuses
    /// the cheap Tier-1 path for the affected columns — correctly, because a
    /// short aggregate answers wrongly. Symptom: a `GROUP BY` on a
    /// high-cardinality column that used to answer in milliseconds now takes
    /// seconds and reports `served_by: "materialized"`, while
    /// `siglake_group_count_delta_write_failures_total` is non-zero.
    ///
    /// Reads the same tiers a query would — each file's group-count footer where
    /// it has one, a raw-page decode where it does not — so it is correct for
    /// columns too wide for the per-file footers, which is exactly the case that
    /// needs repairing. Cost is one expensive scan per column, once.
    ///
    /// Safe to run live: the object is written under an optimistic lock and the
    /// rebuild refuses rather than merging if a concurrent writer wins. Deltas
    /// at or below the scanned snapshot become redundant and are cleaned up by
    /// the compactor; later ones fold on top as usual.
    RebuildGroupCounts {
        /// Iceberg warehouse URL. Reads `SIGLAKE_WAREHOUSE_URL` if not given.
        #[arg(long, env = "SIGLAKE_WAREHOUSE_URL")]
        warehouse_url: Option<String>,
        /// Iceberg catalog URI. Reads `SIGLAKE_CATALOG_URI` if not given.
        #[arg(long, env = "SIGLAKE_CATALOG_URI")]
        catalog_uri: Option<String>,
        /// Per-deployment Iceberg namespace.
        #[arg(long, env = "SIGLAKE_TENANT_NAMESPACE", default_value = "siglake")]
        namespace: String,
        /// Table whose aggregate to rebuild (`events`, or a managed index id).
        #[arg(long, default_value = "events")]
        table: String,
        /// Also add the typed (long/double/bool) columns the table's schema
        /// carries but its aggregate never did.
        ///
        /// For a table created before typed columns joined the side aggregates:
        /// `status` is in every file's footer and in no aggregate, so `GROUP BY
        /// status` reports `served_by: "materialized"` for the life of the table,
        /// and a plain rebuild — which repairs only what the aggregate already
        /// holds — cannot change that. This admits such columns, computing each
        /// exact full-table total from the files; no rewrite is needed. Each is
        /// held to `SIGLAKE_TYPED_GROUP_COUNT_CARDINALITY` on its whole-table
        /// distinct count, and is left absent (reported, never partial) if some
        /// live file cannot serve it. Without the flag the report still names
        /// the columns it would add.
        #[arg(long)]
        admit_typed_columns: bool,
    },
    /// Republish a table's inline time aggregates with a provable coverage
    /// chain, recomputing them from committed files.
    ///
    /// For a table whose inline aggregate object predates the snapshot-coverage
    /// chain. Such an object cannot prove which equal-row-count snapshot it
    /// describes, so every query refuses it and takes the exact per-file tier:
    /// answers stay right, `date_histogram` and windowed `GROUP BY` stop being
    /// served from warm metadata. Symptom:
    /// `siglake_query_side_aggs_cache_total{result="unproven_coverage"}` climbing
    /// on a table whose windowed shapes report `served_by: "materialized"`.
    ///
    /// Nothing repairs this on its own — a coverage chain with no head cannot be
    /// rejoined by later appends, and a compaction has no edge to walk back to —
    /// so this command is the only way back. After it runs, the appends that
    /// follow join the chain normally.
    ///
    /// What it costs: the time buckets are one footer read per live file, but
    /// the 2-D time x group rollup has no footer to read and decodes two columns
    /// of every live file whose time range spans more than one bucket. On a
    /// large table that is a full pass, once.
    ///
    /// NOT safe to run against a table being ingested: the pass reads the files
    /// of one snapshot and cannot merge a commit that lands under it, so it
    /// retries and then gives up without writing. Run it in a window with no
    /// ingest to the table. Re-running after success is a reported no-op.
    ///
    /// The inline whole-table group counts are dropped rather than republished:
    /// one coverage edge governs the object, and they cannot be proven. They
    /// were already refused before this ran, so nothing readable is lost;
    /// `GROUP BY` without a time window is served by the wide aggregate or the
    /// per-file tier, exactly as it was.
    RebuildTimeAggregates {
        /// Iceberg warehouse URL. Reads `SIGLAKE_WAREHOUSE_URL` if not given.
        #[arg(long, env = "SIGLAKE_WAREHOUSE_URL")]
        warehouse_url: Option<String>,
        /// Iceberg catalog URI. Reads `SIGLAKE_CATALOG_URI` if not given.
        #[arg(long, env = "SIGLAKE_CATALOG_URI")]
        catalog_uri: Option<String>,
        /// Per-deployment Iceberg namespace.
        #[arg(long, env = "SIGLAKE_TENANT_NAMESPACE", default_value = "siglake")]
        namespace: String,
        /// Table whose time aggregates to rebuild (`events`, or a managed index id).
        #[arg(long, default_value = "events")]
        table: String,
    },
    /// Additively reconcile a table's stored schema toward the schema the
    /// running build declares for it.
    ///
    /// Adds any column the code declares but the table lacks, as an optional
    /// (nullable) column — never drops, renames, reorders, or retypes. Existing
    /// data files stay readable, with the new columns reading back null for
    /// rows written before the migration. Idempotent: re-running once the table
    /// is up to date adds nothing. Safe to run live against a table being
    /// written — the commit is guarded by the schema-id optimistic lock. Until
    /// migration, the write path refuses writes that populate a column absent
    /// from the table, naming the column and remedy. Reconcile every declared
    /// table in every namespace with `siglake migrate-schema --all-tables
    /// --all-namespaces`.
    ///
    /// Run as a one-shot Job before/at rollout. `--dry-run` reports the columns
    /// that *would* be added without touching the catalog.
    MigrateSchema {
        /// Iceberg warehouse URL. Reads `SIGLAKE_WAREHOUSE_URL` if not given.
        #[arg(long, env = "SIGLAKE_WAREHOUSE_URL")]
        warehouse_url: Option<String>,
        /// Iceberg catalog URI. Reads `SIGLAKE_CATALOG_URI` if not given.
        #[arg(long, env = "SIGLAKE_CATALOG_URI")]
        catalog_uri: Option<String>,
        /// Per-deployment Iceberg namespace.
        #[arg(long, env = "SIGLAKE_TENANT_NAMESPACE", default_value = "siglake")]
        namespace: String,
        /// Table to migrate (`events` or `query_audit` — the tables siglake
        /// declares; a managed index's schema belongs to whoever declared it).
        /// Ignored when `--all-tables` is set.
        #[arg(long, default_value = "events")]
        table: String,
        /// Migrate every known table to its declared schema in one pass.
        #[arg(long, default_value_t = false)]
        all_tables: bool,
        /// Migrate every NAMESPACE in the warehouse, not just `--namespace`.
        ///
        /// Tenancy is header-based, so `events` exists once per tenant
        /// namespace. Without this a migration reports success having left
        /// every other tenant's table narrow. Namespaces with no such table
        /// are skipped, never created.
        #[arg(long, default_value_t = false)]
        all_namespaces: bool,
        /// Report the columns that would be added and exit 0; don't commit.
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Include these promoted typed columns in the
        /// `events` declared schema so the migration adds them. Repeatable;
        /// same `attr_key:type[:column]` format as `compactor --promote-attr`.
        /// Pass the same set the compactor runs with.
        #[arg(long = "promote-attr")]
        promote_attr: Vec<String>,
    },

    /// Run a SQL query DIRECTLY against a warehouse via DataFusion — no
    /// query server involved (offline/ops tool; `siglake sql` is the
    /// client for a running server).
    ///
    /// `events` plus every managed index are pre-registered. Any
    /// DataFusion-supported SQL works (joins, window functions, etc.).
    SqlDirect {
        /// SQL query, e.g. `"SELECT count(*) FROM events"`.
        #[arg(long)]
        query: String,
        /// Subdirectory under `--data-dir` for the Iceberg warehouse,
        /// when running locally with no `--warehouse-url`.
        #[arg(long, default_value = "warehouse")]
        warehouse: String,
        /// Full warehouse URL. See `ingest-server --warehouse-url`.
        #[arg(long, env = "SIGLAKE_WAREHOUSE_URL")]
        warehouse_url: Option<String>,
        /// Iceberg catalog URI. See `ingest-server --catalog-uri`.
        #[arg(long, env = "SIGLAKE_CATALOG_URI")]
        catalog_uri: Option<String>,
    },

    /// Tail an Iceberg-backed table by time-column. Prints each new
    /// row batch as it commits. The tailing primitive for a consumer of a
    /// siglake TABLE; to consume the write-ahead log itself — earlier, and
    /// with retention that waits for you — see `siglake_wal::consumer` and
    /// `docs/CONSUMING_SEGMENTS.md`.
    Subscribe {
        /// Table to tail: `events`, or any index id.
        #[arg(long)]
        table: String,
        /// Time column to cursor on. Defaults to `events.timestamp`, or an
        /// index's own declared `timestamp_field`.
        #[arg(long)]
        time_column: Option<String>,
        /// Subscription cursor start (RFC3339). Defaults to "1 minute ago".
        #[arg(long)]
        since: Option<String>,
        /// Polling interval (seconds).
        #[arg(long, default_value_t = 1)]
        interval_secs: u64,
        /// Process current snapshot once and exit.
        #[arg(long, default_value_t = false)]
        once: bool,
        /// Subdirectory under `--data-dir` for the Iceberg warehouse,
        /// when running locally with no `--warehouse-url`.
        #[arg(long, default_value = "warehouse")]
        warehouse: String,
        /// Full warehouse URL. See `ingest-server --warehouse-url`.
        #[arg(long, env = "SIGLAKE_WAREHOUSE_URL")]
        warehouse_url: Option<String>,
        /// Iceberg catalog URI. See `ingest-server --catalog-uri`.
        #[arg(long, env = "SIGLAKE_CATALOG_URI")]
        catalog_uri: Option<String>,
    },

    /// Run the compactor: drain sealed WAL segments into Iceberg.
    Compactor {
        /// Address to bind for the Prometheus `/metrics` endpoint.
        #[arg(long, default_value = "0.0.0.0:9101")]
        metrics_bind: SocketAddr,
        /// Subdirectory under `--data-dir` (or absolute path) for WAL segments.
        #[arg(long, default_value = "wal")]
        wal: String,
        /// Subdirectory under `--data-dir` for the Iceberg warehouse,
        /// when running locally with no `--warehouse-url`.
        #[arg(long, default_value = "warehouse")]
        warehouse: String,
        /// Full warehouse URL. See `ingest-server --warehouse-url`.
        #[arg(long, env = "SIGLAKE_WAREHOUSE_URL")]
        warehouse_url: Option<String>,
        /// Iceberg catalog URI. See `ingest-server --catalog-uri`.
        #[arg(long, env = "SIGLAKE_CATALOG_URI")]
        catalog_uri: Option<String>,
        /// Process the currently-sealed segments and exit.
        #[arg(long, default_value_t = false)]
        once: bool,
        /// Polling interval (seconds) when running as a daemon.
        #[arg(long, default_value_t = 1)]
        interval_secs: u64,
        /// Switch to multi-pod-safe catalog-claim coordination. When
        /// set, the compactor no longer reads `<wal>/sealed/`; it
        /// claims segments out of the `wal_segments` SQL table and
        /// fetches their bytes from the WAL mirror at
        /// `<warehouse-url>/<mirror-prefix>/<id>.arrow`. Requires
        /// `--warehouse-url` (object-store mirror root) and
        /// `--catalog-uri` (Postgres / SQLite for the claim table).
        #[arg(long, env = "SIGLAKE_COMPACTOR_CATALOG_CLAIM", default_value_t = false)]
        catalog_claim: bool,
        /// Mirror prefix under `--warehouse-url` to scan for
        /// catalog-claim mode. Defaults to `wal-mirror` — the same constant the
        /// ingester writes under, the chart renders and the operator sets, so
        /// the reader and the writer cannot drift apart.
        #[arg(
            long,
            default_value = DEFAULT_WAL_MIRROR_PREFIX,
            env = "SIGLAKE_WAL_MIRROR_PREFIX"
        )]
        mirror_prefix: String,
        /// Max segments per `try_claim` cycle in catalog-claim mode.
        /// Sized to drain an accumulated commit-batch in one
        /// commit; aligns with the local-FS 64-segment cap.
        #[arg(
            long,
            env = "SIGLAKE_COMPACTOR_CATALOG_CLAIM_BATCH",
            default_value_t = 64
        )]
        catalog_claim_batch: usize,
        /// Which half of the work this process performs:
        /// `drain` (WAL -> Iceberg only), `maintenance` (reclustering, snapshot
        /// expiry, gauge sampling, aggregate folding, delete tasks), or
        /// `combined` (both, the default).
        ///
        /// Setting the reclustering interval to 0 does NOT make a process
        /// drain-only -- expiry, gauge sampling and folding all still run.
        #[arg(long, env = "SIGLAKE_COMPACTOR_ROLE", default_value = "combined")]
        role: String,
        /// Max sealed segments to claim per cycle on the local-FS path.
        /// `0` disables the limit.
        #[arg(
            long,
            env = "SIGLAKE_COMPACTOR_FS_CLAIM_MAX_SEGMENTS",
            default_value_t = 64
        )]
        fs_claim_max_segments: usize,
        /// Max total on-disk bytes to claim per cycle on the local-FS path.
        /// `0` disables the limit.
        #[arg(
            long,
            env = "SIGLAKE_COMPACTOR_FS_CLAIM_MAX_BYTES",
            default_value_t = 67_108_864
        )]
        fs_claim_max_bytes: u64,
        /// Promote a declared OTLP attribute out of the
        /// `attributes` JSON into its own typed column on write. Repeatable.
        /// Format `attr_key:type[:column]`, type ∈ string|int|float|bool, column
        /// defaults to the attr key with `.`/`-`→`_`. E.g.
        /// `--promote-attr http.status_code:int --promote-attr k8s.namespace:string`.
        /// The events table is widened additively at startup.
        #[arg(long = "promote-attr")]
        promote_attr: Vec<String>,
    },
}

/// jemalloc as the global allocator: the drain's large transient allocations
/// (256 MiB batch -> concat -> sort -> split -> encode copies) fragment glibc
/// malloc arenas, which retain freed memory indefinitely — a 200G round crept
/// to a 64 GB OOM kill with a ~1 GiB instantaneous working set. Capping arenas
/// (MALLOC_ARENA_MAX=2) held RSS flat but serialized allocation and tanked the
/// ingest accept path ~4x. jemalloc returns memory and scales across threads.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    // Log lines go to stderr so the `println!` reports of the maintenance
    // subcommands (rebuild-group-counts, migrate-schema --dry-run, gc-orphans,
    // ...) stay pipe-clean on stdout under the default filter. Container
    // runtimes capture both streams, so the servers lose nothing.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,siglake=debug".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    std::fs::create_dir_all(&cli.data_dir)?;

    match cli.command {
        Command::Ingest { input } => ingest(&cli.data_dir, input).await,
        Command::Sql {
            query: q,
            endpoint,
            token,
            format,
            dry_run,
            quiet,
        } => {
            sql_client::run(
                &sql_client::SqlClientOpts {
                    endpoint,
                    token,
                    format,
                    dry_run,
                    quiet,
                },
                q,
            )
            .await
        }
        Command::Query { sql } => query(&cli.data_dir, &sql).await,
        Command::Gen { n } => gen(n),
        Command::IcebergDemo {
            n,
            warehouse,
            reset,
        } => iceberg_demo(&cli.data_dir, n, &warehouse, reset).await,
        Command::IngestServer {
            oidc_tenant_claim,
            trust_scope_header,
            allowed_tenants,
            max_tenants,
            ingest_max_lanes,
            bind,
            metrics_bind,
            wal,
            warehouse,
            warehouse_url,
            catalog_uri,
            wal_max_events,
            wal_max_age_secs,
            with_compactor,
            otlp_grpc_listen,
            disable_otlp_grpc,
            wal_mirror_prefix,
            wal_active_mirror_interval_secs,
            auth_tokens,
            oidc_issuer,
            oidc_audience,
            ingest_rate_per_sec,
            ingest_rate_burst,
            ingest_rate_redis_url,
            ingest_rate_redis_prefix,
            ingest_backpressure_capacity,
            ingest_group_commit_ms,
            ingest_backpressure_shards,
            ingest_mem_limit_mib,
            ingest_mem_sample_secs,
        } => {
            run_ingest_server(
                &cli.data_dir,
                bind,
                metrics_bind,
                &wal,
                &warehouse,
                warehouse_url.as_deref(),
                catalog_uri.as_deref(),
                wal_max_events,
                Duration::from_secs(wal_max_age_secs),
                with_compactor,
                otlp_grpc_listen_from(otlp_grpc_listen, disable_otlp_grpc),
                wal_mirror_prefix.as_deref(),
                wal_active_mirror_interval_secs,
                auth_tokens.as_deref(),
                oidc_issuer.as_deref(),
                oidc_audience.as_deref(),
                ingest_rate_per_sec,
                ingest_rate_burst,
                ingest_rate_redis_url,
                ingest_rate_redis_prefix,
                ingest_backpressure_capacity,
                ingest_group_commit_ms,
                ingest_backpressure_shards,
                ingest_mem_limit_mib,
                ingest_mem_sample_secs,
                allowed_tenants,
                max_tenants,
                ingest_max_lanes,
                oidc_tenant_claim.as_deref(),
                tenant_routing_from(trust_scope_header.as_deref()),
            )
            .await
        }
        Command::WalRecover { from, to } => run_wal_recover(&from, &to).await,
        Command::WalRequeue {
            wal,
            segment,
            dry_run,
        } => run_wal_requeue(&wal, segment.as_deref(), dry_run),
        Command::AuditRotate {
            warehouse_url,
            catalog_uri,
            namespace,
            table,
            max_age_secs,
            dry_run,
        } => {
            run_audit_rotate(
                &cli.data_dir,
                warehouse_url.as_deref(),
                catalog_uri.as_deref(),
                &namespace,
                &table,
                max_age_secs,
                dry_run,
            )
            .await
        }
        Command::GcOrphans {
            warehouse_url,
            catalog_uri,
            namespace,
            table,
            min_age_secs,
            apply,
        } => {
            run_gc_orphans(
                &cli.data_dir,
                warehouse_url.as_deref(),
                catalog_uri.as_deref(),
                &namespace,
                &table,
                min_age_secs,
                apply,
            )
            .await
        }
        Command::RetentionSweep {
            warehouse_url,
            catalog_uri,
            namespace,
            index,
            apply,
        } => {
            run_retention_sweep(
                &cli.data_dir,
                warehouse_url.as_deref(),
                catalog_uri.as_deref(),
                &namespace,
                index.as_deref(),
                apply,
            )
            .await
        }
        Command::DeleteSweep {
            warehouse_url,
            catalog_uri,
            namespace,
            index,
            apply,
        } => {
            run_delete_sweep(
                &cli.data_dir,
                warehouse_url.as_deref(),
                catalog_uri.as_deref(),
                &namespace,
                &index,
                apply,
            )
            .await
        }
        Command::MigrateSchema {
            warehouse_url,
            catalog_uri,
            namespace,
            table,
            all_tables,
            all_namespaces,
            dry_run,
            promote_attr,
        } => {
            run_migrate_schema(
                &cli.data_dir,
                warehouse_url.as_deref(),
                catalog_uri.as_deref(),
                &namespace,
                &table,
                all_tables,
                all_namespaces,
                dry_run,
                parse_promoted(&promote_attr)?,
            )
            .await
        }
        Command::RebuildGroupCounts {
            warehouse_url,
            catalog_uri,
            namespace,
            table,
            admit_typed_columns,
        } => {
            run_rebuild_group_counts(
                &cli.data_dir,
                warehouse_url.as_deref(),
                catalog_uri.as_deref(),
                &namespace,
                &table,
                admit_typed_columns,
            )
            .await
        }
        Command::RebuildTimeAggregates {
            warehouse_url,
            catalog_uri,
            namespace,
            table,
        } => {
            run_rebuild_time_aggregates(
                &cli.data_dir,
                warehouse_url.as_deref(),
                catalog_uri.as_deref(),
                &namespace,
                &table,
            )
            .await
        }
        Command::SqlDirect {
            query,
            warehouse,
            warehouse_url,
            catalog_uri,
        } => {
            run_sql(
                &cli.data_dir,
                &warehouse,
                warehouse_url.as_deref(),
                catalog_uri.as_deref(),
                &query,
            )
            .await
        }
        Command::Subscribe {
            table,
            time_column,
            since,
            interval_secs,
            once,
            warehouse,
            warehouse_url,
            catalog_uri,
        } => {
            run_subscribe(RunSubscribeArgs {
                data_dir: cli.data_dir.clone(),
                table,
                time_column,
                since,
                interval: Duration::from_secs(interval_secs),
                once,
                warehouse_sub: warehouse,
                warehouse_url,
                catalog_uri,
            })
            .await
        }
        Command::Compactor {
            metrics_bind,
            wal,
            warehouse,
            warehouse_url,
            catalog_uri,
            once,
            interval_secs,
            catalog_claim,
            mirror_prefix,
            catalog_claim_batch,
            role,
            fs_claim_max_segments,
            fs_claim_max_bytes,
            promote_attr,
        } => {
            run_compactor(
                &cli.data_dir,
                metrics_bind,
                &wal,
                &warehouse,
                warehouse_url.as_deref(),
                catalog_uri.as_deref(),
                once,
                Duration::from_secs(interval_secs),
                catalog_claim,
                &mirror_prefix,
                catalog_claim_batch,
                siglake_compactor::CompactorRole::parse(&role).map_err(|e| anyhow::anyhow!(e))?,
                fs_claim_max_segments,
                fs_claim_max_bytes,
                parse_promoted(&promote_attr)?,
            )
            .await
        }
    }
}

/// Parse `--promote-attr attr_key:type[:column]` specs (WS-7 dense extraction)
/// into [`PromotedColumn`]s. Errors on a malformed spec.
/// Register one mirrored segment, retrying with exponential backoff and jitter.
///
/// This used to be fire-and-forget: a failed register logged "drain sweep will
/// recover" and moved on. That sweep is precisely what was disabled as Phase 1
/// item 1 of the remediation plan (it cost a full-prefix re-list on every drain
/// and measured no throughput benefit). So on the current configuration a
/// dropped registration means the segment is ACCEPTED, DURABLE IN THE MIRROR,
/// AND UNQUERYABLE FOREVER -- with nothing but a warn-level line to say so.
///
/// Retries bound the common case (a brief catalog blip). Jitter is derived from
/// the segment id so a fleet of ingesters whose catalog just came back does not
/// retry in lockstep. A permanent failure increments a counter meant for
/// alerting: it is silent data loss from the query layer's point of view, and it
/// must not look like an ordinary warning.
/// Reclaim an ingester's local WAL disk once its segments are safely committed.
///
/// THE DEFECT THIS CLOSES. An ingester keeps every sealed segment on its own
/// PVC. In catalog-claim mode nothing removed them: the drain reads the MIRROR,
/// and `sweep_committed_coordinated` -- the sweep that deletes local files --
/// runs only on the filesystem path, because `run_once` returns through
/// `run_once_catalog` before reaching it. So the disk fills and ingest starts
/// 500ing. Not a risk but a schedule: the only variable is how long the PVC
/// lasts, and it is the topology the chart recommends.
///
/// Deletion is gated on the CATALOG, not on the upload: a segment is removed
/// only once its row says `committed`, which means its rows are in Iceberg and
/// the local copy is redundant. Uploaded-but-not-committed is exactly the state
/// the WAL exists to survive.
///
/// The settle floor is a correctness requirement, not tuning. A query pod
/// serving un-committed rows reads these same sealed files, and a commit is not
/// instantly visible to it; the standing invariant is that the committed-sweep
/// floor exceeds the table-cache stale ceiling. Deleting the instant the
/// catalog says `committed` would make rows disappear for the width of that
/// window -- the transition race already fixed once for the FS path.
async fn local_wal_sweep_loop(
    claim: siglake_storage::catalog_claim::SqlSegmentClaim,
    root: std::path::PathBuf,
) {
    let Some((interval, settled)) = local_wal_sweep_config_from(
        std::env::var("SIGLAKE_WAL_LOCAL_SWEEP_SECS")
            .ok()
            .as_deref(),
        std::env::var("SIGLAKE_WAL_LOCAL_SWEEP_SETTLE_SECS")
            .ok()
            .as_deref(),
    ) else {
        tracing::info!("local WAL sweep disabled (SIGLAKE_WAL_LOCAL_SWEEP_SECS=0)");
        return;
    };
    tracing::info!(
        interval_secs = interval.as_secs(),
        settle_secs = settled.as_secs(),
        "local WAL sweep enabled (reclaims committed segments from this ingester's disk)"
    );
    loop {
        tokio::time::sleep(interval).await;
        match local_wal_sweep_once(&claim, &root, settled).await {
            Ok((deleted, remaining)) => {
                if deleted > 0 {
                    tracing::info!(deleted, remaining, "local WAL sweep reclaimed segments");
                }
                metrics::counter!("siglake_wal_local_sweep_deleted_total").increment(deleted);
                // The gauge is the point: an ingester whose segments are not
                // being committed now shows a rising number instead of simply
                // running out of disk one day.
                metrics::gauge!("siglake_wal_local_sealed_segments").set(remaining as f64);
            }
            Err(e) => {
                metrics::counter!("siglake_wal_local_sweep_errors_total").increment(1);
                tracing::warn!(error = ?e, "local WAL sweep failed");
            }
        }
    }
}

const DEFAULT_LOCAL_WAL_SWEEP_INTERVAL: Duration = Duration::from_secs(300);
const DEFAULT_LOCAL_WAL_SWEEP_SETTLE: Duration = Duration::from_secs(600);

/// Pure resolver for the local cleanup windows. Besides avoiding process-env
/// races in tests, keeping these defaults named lets the committed-retention
/// safety floor be checked against the values the ingester actually uses.
fn local_wal_sweep_config_from(
    interval_secs: Option<&str>,
    settle_secs: Option<&str>,
) -> Option<(Duration, Duration)> {
    let interval = Duration::from_secs(
        interval_secs
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_LOCAL_WAL_SWEEP_INTERVAL.as_secs()),
    );
    if interval.is_zero() {
        return None;
    }
    let settled = Duration::from_secs(
        settle_secs
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_LOCAL_WAL_SWEEP_SETTLE.as_secs()),
    );
    Some((interval, settled))
}

/// One sweep pass. Returns `(deleted, still sealed)`.
async fn local_wal_sweep_once(
    claim: &siglake_storage::catalog_claim::SqlSegmentClaim,
    root: &std::path::Path,
    settled: Duration,
) -> anyhow::Result<(u64, usize)> {
    // Tenant subdirs AND the legacy top level, for the same reason the drain
    // sweeps both: a deployment that toggled backpressure off leaves segments
    // at the top level while `default/` lingers.
    let mut dirs: Vec<std::path::PathBuf> = siglake_wal::list_tenant_dirs(root)
        .unwrap_or_default()
        .into_iter()
        .map(|(_, d)| d)
        .collect();
    dirs.push(root.to_path_buf());

    let mut deleted = 0u64;
    let mut remaining = 0usize;
    for dir in dirs {
        let sealed = match siglake_wal::list_sealed(&dir) {
            Ok(s) => s,
            Err(_) => continue,
        };
        remaining += sealed.len();
        // Chunked so the `IN (...)` list stays a sane size on any dialect.
        for chunk in sealed.chunks(256) {
            let by_id: std::collections::HashMap<String, &std::path::PathBuf> = chunk
                .iter()
                .filter_map(|p| {
                    p.file_name()
                        .and_then(|f| f.to_str())
                        .map(|f| (f.trim_end_matches(".arrow").to_string(), p))
                })
                .collect();
            let ids: Vec<String> = by_id.keys().cloned().collect();
            let done = claim.committed_and_settled(&ids, settled).await?;
            for id in done {
                if let Some(path) = by_id.get(&id) {
                    match siglake_wal::delete_segment(path) {
                        Ok(()) => {
                            deleted += 1;
                            remaining = remaining.saturating_sub(1);
                        }
                        Err(e) => {
                            tracing::warn!(path = %path.display(), error = ?e,
                                "local WAL sweep: delete failed");
                        }
                    }
                }
            }
        }
    }
    Ok((deleted, remaining))
}

#[cfg(test)]
mod local_wal_sweep_tests {
    use super::*;

    #[test]
    fn committed_retention_floor_outlives_default_local_cleanup() {
        assert!(
            siglake_compactor::MIN_COMMITTED_RETENTION_SECS
                > (DEFAULT_LOCAL_WAL_SWEEP_INTERVAL + DEFAULT_LOCAL_WAL_SWEEP_SETTLE).as_secs(),
            "remote retention must leave a strict window for local cleanup"
        );
        assert_eq!(
            local_wal_sweep_config_from(None, None),
            Some((
                DEFAULT_LOCAL_WAL_SWEEP_INTERVAL,
                DEFAULT_LOCAL_WAL_SWEEP_SETTLE
            ))
        );
        assert_eq!(local_wal_sweep_config_from(Some("0"), None), None);
    }

    #[tokio::test]
    async fn local_copy_is_removed_while_the_committed_row_still_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writer = siglake_wal::WalWriter::with_thresholds(
            tmp.path(),
            "ingester",
            1,
            Duration::from_secs(60),
        )
        .unwrap();
        let segment = writer
            .append_events(&[siglake_core::Event::now("retained")])
            .unwrap()
            .unwrap();
        let id = segment
            .path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let uri = format!(
            "sqlite://{}?mode=rwc",
            tmp.path().join("claim.db").display()
        );
        let claim = siglake_storage::catalog_claim::SqlSegmentClaim::connect(&uri, "drain")
            .await
            .unwrap();
        claim
            .register(&id, "default", "", "wal-mirror/segment.arrow", 1, 1)
            .await
            .unwrap();
        assert_eq!(claim.try_claim(1).await.unwrap().len(), 1);
        claim.mark_committed(&id).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;

        let (deleted, remaining) = local_wal_sweep_once(&claim, tmp.path(), Duration::ZERO)
            .await
            .unwrap();
        assert_eq!((deleted, remaining), (1, 0));
        assert!(!segment.path.exists(), "the local sealed copy was retained");
        assert_eq!(
            claim
                .purgeable_committed(Duration::ZERO, 10)
                .await
                .unwrap()
                .len(),
            1,
            "local cleanup must not remove the row remote retention still needs"
        );
    }
}

async fn register_mirrored_with_retry(
    claim: &siglake_storage::catalog_claim::SqlSegmentClaim,
    seg: &siglake_wal::mirror::MirroredSegment,
) {
    const MAX_ATTEMPTS: u32 = 6;
    let started = std::time::Instant::now();
    let mut delay = Duration::from_millis(100);
    for attempt in 1..=MAX_ATTEMPTS {
        match claim
            .register(
                &seg.id,
                &seg.tenant,
                &seg.index_id,
                &seg.url,
                seg.bytes as i64,
                seg.rows as i64,
            )
            .await
        {
            Ok(_) => {
                metrics::counter!("siglake_wal_mirror_register_total", "outcome" => "ok")
                    .increment(1);
                metrics::histogram!("siglake_wal_mirror_register_lag_seconds")
                    .record(started.elapsed().as_secs_f64());
                if attempt > 1 {
                    tracing::info!(id = %seg.id, attempt, "mirror registrar: register recovered");
                }
                return;
            }
            Err(e) if attempt == MAX_ATTEMPTS => {
                metrics::counter!("siglake_wal_mirror_register_total", "outcome" => "err")
                    .increment(1);
                metrics::counter!("siglake_wal_mirror_register_abandoned_total").increment(1);
                tracing::error!(id = %seg.id, url = %seg.url, attempts = attempt, error = ?e,
                    "mirror registrar: PERMANENTLY FAILED — segment is durable in the \
                     mirror but will never be drained or queried until it is registered");
                return;
            }
            Err(e) => {
                metrics::counter!("siglake_wal_mirror_register_total", "outcome" => "retry")
                    .increment(1);
                tracing::warn!(id = %seg.id, attempt, error = ?e,
                    "mirror registrar: register failed; retrying");
                tokio::time::sleep(delay + register_retry_jitter(&seg.id, attempt)).await;
                delay = (delay * 2).min(Duration::from_secs(15));
            }
        }
    }
}

#[derive(Default)]
struct CatchUpSweepState {
    claim: Option<siglake_storage::catalog_claim::SqlSegmentClaim>,
    pending_registration: Vec<siglake_wal::mirror::MirroredSegment>,
}

async fn wal_mirror_catch_up_pass<Connect, ConnectFuture, Sweep, SweepFuture>(
    state: &mut CatchUpSweepState,
    mut claim_factory: Option<&mut Connect>,
    sweep: &mut Sweep,
) where
    Connect: FnMut() -> ConnectFuture,
    ConnectFuture:
        std::future::Future<Output = Result<siglake_storage::catalog_claim::SqlSegmentClaim>>,
    Sweep: FnMut() -> SweepFuture,
    SweepFuture: std::future::Future<Output = Result<Vec<siglake_wal::mirror::MirroredSegment>>>,
{
    // Do not sweep another batch while registrations from the preceding pass
    // are pending. This bounds retained work to one pass and preserves the
    // only retry path after catch_up_sweep has removed its local mirror pins.
    let recovered = if state.pending_registration.is_empty() {
        match sweep().await {
            Ok(segs) => segs,
            Err(e) => {
                tracing::warn!(error = ?e, "WAL mirror catch-up sweep failed");
                return;
            }
        }
    } else {
        Vec::new()
    };

    let recovered_this_pass = !recovered.is_empty();
    if recovered_this_pass {
        tracing::info!(
            uploaded = recovered.len(),
            "WAL mirror catch-up sweep recovered stranded segments"
        );
        state.pending_registration = recovered;
    }

    if state.pending_registration.is_empty() {
        return;
    }

    if state.claim.is_none() {
        if let Some(factory) = claim_factory.as_mut() {
            match factory().await {
                Ok(claim) => state.claim = Some(claim),
                Err(e) => {
                    tracing::warn!(
                        error = ?e,
                        pending = state.pending_registration.len(),
                        "catch-up sweep: catalog connect failed; registration will retry next sweep"
                    );
                    if recovered_this_pass {
                        metrics::counter!("siglake_wal_mirror_sweep_unregistered_total")
                            .increment(state.pending_registration.len() as u64);
                    }
                    return;
                }
            }
        } else {
            // Catalog registration was intentionally not configured. Preserve
            // the existing upload-only mode without retaining work forever.
            metrics::counter!("siglake_wal_mirror_sweep_unregistered_total")
                .increment(state.pending_registration.len() as u64);
            state.pending_registration.clear();
            return;
        }
    }

    let pending = std::mem::take(&mut state.pending_registration);
    let claim = state
        .claim
        .as_ref()
        .expect("successful claim initialization stores a claim");
    for seg in &pending {
        register_mirrored_with_retry(claim, seg).await;
    }
}

#[cfg(test)]
mod catch_up_sweep_tests {
    use super::*;

    #[tokio::test]
    async fn failed_initialization_retries_pending_registration_next_pass() {
        let tmp = tempfile::tempdir().unwrap();
        let claim_uri = format!(
            "sqlite://{}?mode=rwc",
            tmp.path().join("claim.db").display()
        );
        let connect_calls = std::cell::Cell::new(0);
        let mut claim_factory = || {
            let attempt = connect_calls.get() + 1;
            connect_calls.set(attempt);
            let claim_uri = claim_uri.clone();
            async move {
                if attempt == 1 {
                    anyhow::bail!("injected catalog initialization failure");
                }
                siglake_storage::catalog_claim::SqlSegmentClaim::connect(&claim_uri, "test-sweep")
                    .await
            }
        };

        let sweep_calls = std::cell::Cell::new(0);
        let mut sweep = || {
            let call = sweep_calls.get() + 1;
            sweep_calls.set(call);
            let segments = if call == 1 {
                vec![siglake_wal::mirror::MirroredSegment {
                    id: "recovered-segment".to_string(),
                    tenant: "tenant-a".to_string(),
                    index_id: "index-a".to_string(),
                    url: "wal-mirror/tenant-a/index-a/recovered-segment.arrow".to_string(),
                    bytes: 123,
                    rows: 7,
                }]
            } else {
                Vec::new()
            };
            std::future::ready(Ok(segments))
        };
        let mut state = CatchUpSweepState::default();

        wal_mirror_catch_up_pass(&mut state, Some(&mut claim_factory), &mut sweep).await;
        assert_eq!(connect_calls.get(), 1);
        assert_eq!(sweep_calls.get(), 1);
        assert_eq!(state.pending_registration.len(), 1);
        assert!(state.claim.is_none());

        wal_mirror_catch_up_pass(&mut state, Some(&mut claim_factory), &mut sweep).await;
        assert_eq!(connect_calls.get(), 2);
        assert_eq!(
            sweep_calls.get(),
            1,
            "pending registration must recover without a newly sealed segment"
        );
        assert!(state.pending_registration.is_empty());
        let claimed = state.claim.as_ref().unwrap().try_claim(10).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, "recovered-segment");
        assert_eq!(claimed[0].tenant, "tenant-a");
        assert_eq!(claimed[0].index_id, "index-a");
    }
}

/// Per-segment retry jitter, so a fleet whose catalog just recovered does not
/// stampede it in lockstep. Derived from the segment id rather than a clock:
/// every ingester that failed at the same instant would otherwise pick the same
/// delay.
fn register_retry_jitter(id: &str, attempt: u32) -> Duration {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut h);
    attempt.hash(&mut h);
    // Up to 250ms of spread; enough to decorrelate, small next to the backoff.
    Duration::from_millis(h.finish() % 250)
}

fn parse_promoted(specs: &[String]) -> Result<Vec<siglake_core::PromotedColumn>> {
    specs
        .iter()
        .map(|s| {
            siglake_core::PromotedColumn::parse_spec(s).ok_or_else(|| {
                anyhow::anyhow!(
                    "invalid --promote-attr {s:?}; expected `attr_key:type[:column]` \
                     where type is string|int|float|bool"
                )
            })
        })
        .collect()
}

/// Resolve the Iceberg namespace a subcommand lands in.
///
/// `explicit` is a subcommand's own `--namespace` value: the six maintenance
/// paths (audit-rotate, gc-orphans, retention-sweep, delete-sweep,
/// migrate-schema, rebuild-group-counts) each take one, and it wins outright.
/// `env` is the raw `SIGLAKE_TENANT_NAMESPACE` the long-running subcommands
/// (ingest-server, compactor, query-server, subscribe, sql) inherit from Helm's
/// `tenant.namespace` + commonEnv, so they share a namespace without
/// per-subcommand flag churn. Neither set falls back to the storage default.
///
/// Pure so the precedence can be tested without mutating the process
/// environment: the maintenance paths used to write their flag into
/// `SIGLAKE_TENANT_NAMESPACE` for `open_iceberg` to read back, which a
/// parallel test in this binary could observe mid-flight.
fn tenant_namespace_from(explicit: Option<&str>, env: Option<&str>) -> String {
    explicit
        .or(env)
        .unwrap_or(siglake_storage::iceberg::NAMESPACE)
        .to_string()
}

/// Open an [`IcebergContext`] from CLI flag/env-var inputs.
/// - If `warehouse_url` is provided, uses `open_with(catalog_uri, warehouse_url)`.
///   `catalog_uri` defaults to a SQLite DB at `{data_dir}/{warehouse_sub}/_catalog.db`.
/// - Otherwise falls back to the local-path constructor `IcebergContext::open(...)`.
/// - `namespace` is a subcommand's explicit `--namespace`; `None` defers to
///   `SIGLAKE_TENANT_NAMESPACE` (see [`tenant_namespace_from`]).
async fn open_iceberg(
    data_dir: &std::path::Path,
    warehouse_sub: &str,
    warehouse_url: Option<&str>,
    catalog_uri: Option<&str>,
    namespace: Option<&str>,
) -> Result<IcebergContext> {
    let default_ns = siglake_storage::iceberg::NAMESPACE.to_string();
    let tenant = tenant_namespace_from(
        namespace,
        std::env::var("SIGLAKE_TENANT_NAMESPACE").ok().as_deref(),
    );

    if let Some(url) = warehouse_url {
        let catalog_uri = match catalog_uri {
            Some(c) => c.to_string(),
            None => {
                // Default: SQLite next to the local warehouse dir, even when
                // the warehouse itself is remote. Useful for one-host minio dev.
                let local = data_dir.join(warehouse_sub);
                std::fs::create_dir_all(&local)
                    .with_context(|| format!("creating {}", local.display()))?;
                let abs = std::fs::canonicalize(&local)?;
                format!("sqlite://{}/_catalog.db?mode=rwc", abs.display())
            }
        };
        IcebergContext::open_with_namespace(&catalog_uri, url, &tenant).await
    } else if tenant == default_ns {
        // Pure local path mode: SQLite + local FS, both under the warehouse dir.
        let warehouse_dir = data_dir.join(warehouse_sub);
        IcebergContext::open(&warehouse_dir).await
    } else {
        // Local-fs mode with a non-default tenant: route through
        // open_with_namespace explicitly.
        let warehouse_dir = data_dir.join(warehouse_sub);
        std::fs::create_dir_all(&warehouse_dir)?;
        let abs = std::fs::canonicalize(&warehouse_dir)?;
        let url = format!("file://{}", abs.display());
        let cat_uri = format!("sqlite://{}/_catalog.db?mode=rwc", abs.display());
        IcebergContext::open_with_namespace(&cat_uri, &url, &tenant).await
    }
}

/// #2693: the ingest binary's [`siglake_ingest::WalTableIdentity`] — the WAL
/// writer's link to the catalog, which `siglake-ingest` itself does not have.
///
/// A per-index WAL directory is keyed by the index NAME, and a `DELETE`+`POST`
/// of the same id points it at a different Iceberg table. Resolving the table
/// here is what lets each sealed segment name the incarnation its rows were
/// written for, so a writer that outlives the drop cannot have its rows folded
/// into the replacement.
struct CatalogTableIdentity {
    /// The default-namespace context; also the source of per-tenant ones.
    ice: Arc<IcebergContext>,
    /// `tenant_<t>` namespace contexts, opened once each.
    per_tenant: Mutex<std::collections::HashMap<String, Arc<IcebergContext>>>,
}

impl CatalogTableIdentity {
    fn new(ice: Arc<IcebergContext>) -> Self {
        Self {
            ice,
            per_tenant: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// The context a tenant's managed indexes live in. `default` IS the main
    /// namespace — the same mapping the drain's `ice_for_tenant` applies, and
    /// resolving it any other way would compare a segment against a table
    /// nothing ever commits to.
    async fn for_tenant(&self, tenant: &str) -> Result<Arc<IcebergContext>> {
        if tenant.is_empty() || tenant == "default" {
            return Ok(self.ice.clone());
        }
        if let Some(ice) = self.per_tenant.lock().await.get(tenant) {
            return Ok(ice.clone());
        }
        let ice = Arc::new(
            self.ice
                .for_namespace(&format!("tenant_{tenant}"))
                .await
                .with_context(|| format!("open tenant namespace tenant_{tenant}"))?,
        );
        self.per_tenant
            .lock()
            .await
            .insert(tenant.to_string(), ice.clone());
        Ok(ice)
    }
}

#[async_trait::async_trait]
impl siglake_ingest::WalTableIdentity for CatalogTableIdentity {
    async fn table_uuid(&self, tenant: &str, index: &str) -> Result<Option<Uuid>> {
        // `events` is the tenant's WAL ROOT, not a per-index directory: no API
        // deletes and recreates it, so there is no incarnation to distinguish
        // and every reader passes no identity for it either.
        if index == siglake_ingest::EVENTS_INDEX_ID {
            return Ok(None);
        }
        let ice = self.for_tenant(tenant).await?;
        let Some(raw) = ice
            .index_table_uuid(index)
            .await
            .with_context(|| format!("index_table_uuid {index}"))?
        else {
            // The index has no table yet — the drain auto-creates it on first
            // commit. Unstamped segments are "no opinion", which is right: an
            // identity does not exist to be wrong about.
            return Ok(None);
        };
        Ok(Some(Uuid::parse_str(&raw).with_context(|| {
            format!("table uuid {raw} for index {index} is not a UUID")
        })?))
    }
}

fn otlp_grpc_listen_from(configured: Option<SocketAddr>, disabled: bool) -> Option<SocketAddr> {
    if disabled {
        None
    } else {
        configured
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_ingest_server(
    data_dir: &std::path::Path,
    bind: SocketAddr,
    metrics_bind: SocketAddr,
    wal_sub: &str,
    warehouse_sub: &str,
    warehouse_url: Option<&str>,
    catalog_uri: Option<&str>,
    wal_max_events: usize,
    wal_max_age: Duration,
    with_compactor: bool,
    otlp_grpc_listen: Option<SocketAddr>,
    wal_mirror_prefix: Option<&str>,
    wal_active_mirror_interval_secs: u64,
    auth_tokens: Option<&str>,
    oidc_issuer: Option<&str>,
    oidc_audience: Option<&str>,
    ingest_rate_per_sec: f64,
    ingest_rate_burst: f64,
    ingest_rate_redis_url: Option<String>,
    ingest_rate_redis_prefix: Option<String>,
    ingest_backpressure_capacity: usize,
    ingest_group_commit_ms: u64,
    ingest_backpressure_shards: usize,
    ingest_mem_limit_mib: u64,
    ingest_mem_sample_secs: u64,
    allowed_tenants_arg: Vec<String>,
    max_tenants: usize,
    ingest_max_lanes: usize,
    oidc_tenant_claim: Option<&str>,
    tenant_routing: TenantRouting,
) -> Result<()> {
    // Refuse a tenancy configuration the ingester cannot honour before the
    // first startup side effect — the metrics listener, the WAL directory, the
    // catalog — rather than in the verifier branch a few hundred lines down,
    // which never ran for the case that mattered.
    if let Some(err) = ingest_oidc_config_error(oidc_issuer, oidc_audience, oidc_tenant_claim) {
        bail!("{err}");
    }
    let oidc_tenant_claim = oidc_tenant_claim_from(oidc_tenant_claim);
    // Spawn metrics server first so it's serving when the ingest server
    // accepts its first request.
    let _metrics_handle = siglake_core::metrics::init(metrics_bind).await?;
    // Alerted counters exist at 0 from the first scrape, so the first refusal
    // or abandonment on this pod is a delta `increase()` can see.
    siglake_core::metrics::preregister(siglake_core::metrics::INGESTER_ALERTED_COUNTERS);
    let build = siglake_core::build_info();
    metrics::gauge!(
        "siglake_build_info",
        "version" => build.version,
        "commit" => build.commit
    )
    .set(1.0);
    // WAL: respect absolute paths, otherwise join under data_dir.
    let wal_dir = if std::path::Path::new(wal_sub).is_absolute() {
        std::path::PathBuf::from(wal_sub)
    } else {
        data_dir.join(wal_sub)
    };
    std::fs::create_dir_all(&wal_dir)?;

    let ingester_id = std::env::var("SIGLAKE_INGESTER_ID")
        .or_else(|_| hostname_lossy())
        .unwrap_or_else(|_| format!("ing-{}", &Uuid::new_v4().to_string()[..8]));

    let mut writer =
        WalWriter::with_thresholds(&wal_dir, ingester_id.clone(), wal_max_events, wal_max_age)
            .with_context(|| format!("opening WAL at {}", wal_dir.display()))?;

    // WAL → object-store mirror, ON by default wherever a warehouse URL
    // says there is an object store to mirror to. Every sealed segment is
    // enqueued for background upload to `<warehouse_url>/<prefix>/`.
    // Failures don't block ingest.
    let wal_mirror_prefix = wal_mirror_prefix_from(wal_mirror_prefix, warehouse_url);
    let mut mirror_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut active_mirror_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut active_mirror_store: Option<opendal::Operator> = None;
    let mut active_mirror_prefix: Option<String> = None;
    let mut mirror_handle: Option<siglake_wal::mirror::WalMirrorHandle> = None;
    if let Some(prefix) = wal_mirror_prefix.as_deref() {
        let url = warehouse_url.ok_or_else(|| {
            anyhow::anyhow!(
                "--wal-mirror-prefix {prefix:?} requires --warehouse-url (s3://… etc.); \
                 pass an empty --wal-mirror-prefix to run without the mirror"
            )
        })?;
        let store = build_opendal_operator(url)?;
        let (mut mirror, handle) =
            siglake_wal::mirror::WalMirror::new(store.clone(), prefix.to_string());
        // Fleet mode: the ingester registers its own uploads in the shared
        // `wal_segments` catalog, so drains claim them IMMEDIATELY and their
        // `sync_mirror_to_catalog` stays a rare recovery sweep — re-listing +
        // re-inserting the whole prefix every cycle on every drain collapsed
        // the claim path once a backlog accumulated (round 2, 2026-07-14).
        // Registration failures are non-fatal: the drain-side sweep is the
        // safety net, exactly as for an ingester crash before registering.
        if let Some(claim_uri) = catalog_uri {
            let (uploaded_tx, mut uploaded_rx) =
                tokio::sync::mpsc::unbounded_channel::<siglake_wal::mirror::MirroredSegment>();
            mirror = mirror.with_uploaded_tx(uploaded_tx);
            let claim_uri = claim_uri.to_string();
            let registrar_id = format!("{ingester_id}-registrar");
            let wal_dir_for_sweep = wal_dir.clone();
            tokio::spawn(async move {
                let claim = loop {
                    match siglake_storage::catalog_claim::SqlSegmentClaim::connect(
                        &claim_uri,
                        registrar_id.clone(),
                    )
                    .await
                    {
                        Ok(c) => break c,
                        Err(e) => {
                            tracing::warn!(error = ?e, "mirror registrar: catalog connect failed; retrying");
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        }
                    }
                };
                let sweeper = claim.clone();
                let sweep_root = wal_dir_for_sweep.clone();
                tokio::spawn(async move {
                    local_wal_sweep_loop(sweeper, sweep_root).await;
                });
                while let Some(seg) = uploaded_rx.recv().await {
                    register_mirrored_with_retry(&claim, &seg).await;
                }
            });
            tracing::info!("WAL mirror registrar enabled (ingester-side catalog registration)");
        }
        writer.set_mirror_handle(Some(handle.clone()));
        mirror_handle = Some(handle);
        mirror_task = Some(tokio::spawn(mirror.run()));
        tracing::info!(prefix, "WAL mirror enabled");
        // #68 catch-up sweep: seal-time uploads are the only other mirror
        // path, so segments sealed while the object store was unreachable
        // would otherwise stay stranded locally forever. Sweep once at
        // startup (the outage-recovery moment) and then periodically.
        // `SIGLAKE_WAL_MIRROR_SWEEP_SECS` (default 300; 0 disables).
        let sweep_secs: u64 = std::env::var("SIGLAKE_WAL_MIRROR_SWEEP_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);
        if sweep_secs > 0 {
            let op = store.clone();
            let sweep_prefix = prefix.to_string();
            let sweep_root = wal_dir.clone();
            let sweep_claim_uri = catalog_uri.map(|s| s.to_string());
            tokio::spawn(async move {
                // Register what the sweep recovers. Uploading to the mirror is
                // only half the repair: an unregistered segment is never
                // claimed, so its rows stay unqueryable -- the exact condition
                // this sweep exists to fix. Without this the recovery path
                // silently recreated the failure it was written to repair.
                let sweep_claimer = format!("{}-sweep", hostname_lossy().unwrap_or_default());
                let mut claim_factory = || {
                    let sweep_claim_uri = sweep_claim_uri.clone();
                    let sweep_claimer = sweep_claimer.clone();
                    async move {
                        let uri = sweep_claim_uri.as_deref().ok_or_else(|| {
                            anyhow::anyhow!("catalog registration is not configured")
                        })?;
                        siglake_storage::catalog_claim::SqlSegmentClaim::connect(uri, sweep_claimer)
                            .await
                    }
                };
                let mut state = CatchUpSweepState::default();
                loop {
                    let mut sweep =
                        || siglake_wal::mirror::catch_up_sweep(&op, &sweep_prefix, &sweep_root);
                    wal_mirror_catch_up_pass(
                        &mut state,
                        sweep_claim_uri.as_ref().map(|_| &mut claim_factory),
                        &mut sweep,
                    )
                    .await;
                    tokio::time::sleep(Duration::from_secs(sweep_secs)).await;
                }
            });
        }
        if wal_active_mirror_interval_secs > 0 {
            active_mirror_store = Some(store);
            active_mirror_prefix = Some(prefix.to_string());
        }
    } else if wal_active_mirror_interval_secs > 0 {
        anyhow::bail!("--wal-active-mirror-interval-secs requires --wal-mirror-prefix");
    }

    let writer = Arc::new(Mutex::new(writer));

    // Periodic age-roll task.
    let tick_writer = writer.clone();
    let tick_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            let mut w = tick_writer.lock().await;
            if let Err(e) = w.tick() {
                tracing::warn!(error = %e, "WAL tick failed");
            }
        }
    });

    // Periodic active-segment mirror. Disabled unless both
    // --wal-mirror-prefix and --wal-active-mirror-interval-secs are
    // set.
    if let (Some(store), Some(prefix)) = (active_mirror_store, active_mirror_prefix) {
        let interval_secs = wal_active_mirror_interval_secs;
        tracing::info!(
            interval_secs,
            prefix = prefix.as_str(),
            "WAL active-segment mirror enabled"
        );
        let task_writer = writer.clone();
        active_mirror_task = Some(tokio::spawn(async move {
            siglake_wal::mirror::active_mirror_loop(
                task_writer,
                store,
                prefix,
                Duration::from_secs(interval_secs),
            )
            .await;
        }));
    }

    // Always open the Iceberg context once on startup. Two reasons:
    //   1. Validate catalog connectivity early (ingest starts unhealthy if
    //      we can't reach the catalog, which is the fail-fast we want).
    //   2. Serialize first-time table creation. Subsequent compactor /
    //      dependent pods can `depends_on` the ingester being healthy and
    //      will skip the `CREATE TABLE IF NOT EXISTS` altogether — which
    //      is what `iceberg-catalog-sql 0.9` doesn't currently handle
    //      concurrently against Postgres (system-catalog DDL races).
    let ice =
        Arc::new(open_iceberg(data_dir, warehouse_sub, warehouse_url, catalog_uri, None).await?);
    let compactor_handle = if with_compactor {
        let mut compactor = Compactor::new(&wal_dir, ice.clone());
        // Honor the claim-cap envs here too (the standalone `compactor`
        // subcommand exposes them as flags). Each claim batch is one commit,
        // so these set the commit SIZE — the lever for trading commit count
        // (catalog CAS churn) against per-commit memory.
        let env_u64 = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<u64>().ok());
        let max_segs = env_u64("SIGLAKE_COMPACTOR_FS_CLAIM_MAX_SEGMENTS")
            .map(|v| v as usize)
            .unwrap_or(64);
        let max_bytes = env_u64("SIGLAKE_COMPACTOR_FS_CLAIM_MAX_BYTES").unwrap_or(67_108_864);
        compactor = compactor.with_fs_batch_limits(max_segs, max_bytes);
        if let Some(cfg) = recluster_cfg_from_env() {
            compactor = compactor.with_reclustering(cfg);
        }
        if let Some(cfg) = expire_cfg_from_env() {
            compactor = compactor.with_snapshot_expiry(cfg);
        }
        compactor = compactor.with_delete_tasks(delete_tasks_enabled_from_env());
        if let Some(cfg) = commit_batch_cfg_from_env() {
            compactor = compactor.with_commit_batching(cfg);
        }
        Some(tokio::spawn(async move {
            std::sync::Arc::new(compactor)
                .run_loop(Duration::from_secs(1))
                .await
        }))
    } else {
        // Hold a reference so the catalog handle stays warm; this is
        // free since IcebergContext is just a couple of Arcs.
        let _ = ice;
        None
    };

    tracing::info!(
        addr = %bind,
        wal = %wal_dir.display(),
        warehouse_url = warehouse_url.unwrap_or("(local)"),
        // Mirroring is the difference between "a lost WAL volume loses
        // acknowledged data" and "it does not", so the startup line says which
        // one this process is, default or not.
        wal_mirror_prefix = wal_mirror_prefix.as_deref().unwrap_or("(off)"),
        // Which tenant a request can reach is the other thing an operator must
        // be able to read off the startup line rather than infer from flags.
        tenant_routing = tenant_routing.label(),
        oidc_tenant_claim = oidc_tenant_claim.unwrap_or("(none)"),
        ingester_id,
        with_compactor,
        "siglake-ingest starting"
    );
    // The claim here is the resolved one, so only a claim that actually binds
    // tenancy silences this; an empty value used to silence it too.
    if tenant_routing == TenantRouting::TrustHeader && oidc_tenant_claim.is_none() {
        tracing::warn!(
            "--trust-scope-header is set with no --oidc-tenant-claim: any accepted credential \
             can write as any tenant. Only safe behind a gateway that sets X-Scope-OrgID itself \
             and strips the client's."
        );
    }

    // Wrap the writer with a broadcast channel so the new
    // /api/v1/stream SSE endpoint can tee events to live subscribers.
    let (events_tx, _) = tokio::sync::broadcast::channel(siglake_ingest::STREAM_BROADCAST_CAPACITY);
    let tokens = auth_tokens
        .map(siglake_ingest::AuthTokens::from_csv)
        .filter(|t| !t.is_empty());
    if ingest_auth_open_from(auth_tokens, oidc_issuer, oidc_audience) {
        tracing::warn!(
            "no `--auth-tokens` / SIGLAKE_AUTH_TOKENS supplied — ingest is open; \
             only safe inside a trusted network"
        );
    }
    // Every request resolves to its own `<root>/<tenant>/` subtree, so the
    // per-tenant WAL router is always on — a single-tenant ingester simply
    // resolves every request to `default`. The
    // backpressure path (default) keys its lanes by the same resolved
    // tenant; this legacy non-backpressure router covers the opt-out case.
    let tenant_router = Some(std::sync::Arc::new(siglake_ingest::TenantWalRouter::new(
        wal_dir.clone(),
        ingester_id.clone(),
        wal_max_events,
        wal_max_age,
    )));
    let rate_limiter: Option<Arc<dyn siglake_ingest::rate_limit::RateBudget>> =
        if ingest_rate_per_sec > 0.0 {
            let burst = if ingest_rate_burst > 0.0 {
                ingest_rate_burst
            } else {
                ingest_rate_per_sec.max(1.0)
            };
            if let Some(url) = ingest_rate_redis_url.as_deref() {
                tracing::info!(
                    rate_per_sec = ingest_rate_per_sec,
                    burst,
                    redis_url_redacted = redact_redis_url(url),
                    "ingest rate limiter enabled (Redis backend, cross-replica shared budget)"
                );
                let rb = siglake_ingest::rate_limit::RedisRateBudget::connect(
                    url,
                    ingest_rate_per_sec,
                    burst,
                    ingest_rate_redis_prefix.as_deref(),
                )
                .await
                .with_context(|| {
                    format!("connecting Redis rate budget at {}", redact_redis_url(url))
                })?;
                Some(Arc::new(rb) as Arc<dyn siglake_ingest::rate_limit::RateBudget>)
            } else {
                tracing::info!(
                    rate_per_sec = ingest_rate_per_sec,
                    burst,
                    "ingest rate limiter enabled (in-memory, per-replica)"
                );
                Some(Arc::new(siglake_ingest::rate_limit::RateLimiter::new(
                    ingest_rate_per_sec,
                    burst,
                ))
                    as Arc<dyn siglake_ingest::rate_limit::RateBudget>)
            }
        } else {
            None
        };
    // Pre-warm and wire the mirror handle onto every tenant writer
    // so per-tenant seals also feed the WAL mirror.
    if let (Some(router), Some(handle)) = (&tenant_router, mirror_handle.as_ref()) {
        router.set_mirror_handle(Some(handle.clone())).await?;
    }

    // Periodic age-roll for per-tenant writers. The legacy `writer`'s
    // tick (above) is unused once tenants is set, but a quiet tenant's
    // active segment still needs to age-roll on its own threshold.
    let tenant_tick_handle = if let Some(router) = tenant_router.clone() {
        let r = router.clone();
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                interval.tick().await;
                r.tick_all().await;
            }
        }))
    } else {
        None
    };

    // Backpressure path (Phase 4.12.16): when the operator passes a
    // non-zero capacity, every ingest handler routes through this
    // `BackpressureRouter` instead of the legacy
    // `Mutex<WalWriter>`. A full lane returns 503 +
    // `Retry-After` rather than blocking handlers behind a per-tenant
    // mutex. The router owns its own writer tasks; the legacy
    // single-`writer` + per-tenant `TenantWalRouter` paths are
    // ignored when this is `Some`.
    let backpressure_router = if ingest_backpressure_capacity > 0 {
        tracing::info!(
            capacity = ingest_backpressure_capacity,
            group_commit_ms = ingest_group_commit_ms,
            shards = ingest_backpressure_shards,
            "ingest backpressure router enabled"
        );
        let bp = siglake_ingest::backpressure::BackpressureRouter::new(
            wal_dir.clone(),
            ingester_id.clone(),
            wal_max_events,
            wal_max_age,
            ingest_backpressure_capacity,
        )
        .with_group_commit_ms(ingest_group_commit_ms)
        .with_shards_per_tenant(ingest_backpressure_shards)
        // Both halves of a lane key are client headers; without a cap a client
        // that varies them exhausts this process's file descriptors.
        .with_max_lanes(ingest_max_lanes);
        if let Some(handle) = mirror_handle.as_ref() {
            bp.set_mirror_handle(Some(handle.clone())).await;
        }
        Some(Arc::new(bp))
    } else {
        None
    };

    // #2693: give both routers the catalog, so every WAL segment they seal
    // names the Iceberg table its rows are destined for. Installed before the
    // listener binds — a lane created without it stamps nothing, and an
    // unstamped segment is "no opinion" to every reader.
    let identity: Arc<dyn siglake_ingest::WalTableIdentity> =
        Arc::new(CatalogTableIdentity::new(ice.clone()));
    if let Some(router) = &tenant_router {
        router.set_identity_resolver(Some(identity.clone())).await;
    }
    if let Some(bp) = &backpressure_router {
        bp.set_identity_resolver(Some(identity.clone())).await;
    }
    // A lane keeps its binding for as long as the process lives unless
    // something re-asks; this is what notices a `DELETE`+`POST` of an index.
    // One catalog metadata read per lane per
    // `SIGLAKE_WAL_IDENTITY_REFRESH_SECS` (30s default).
    let identity_refresh_handle = {
        let tenants = tenant_router.clone();
        let bp = backpressure_router.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                if let Some(r) = &tenants {
                    r.refresh_table_identities().await;
                }
                if let Some(r) = &bp {
                    r.refresh_table_identities().await;
                }
            }
        })
    };

    // Keep Arc clones for seal-on-shutdown (the non-backpressure path).
    // `state` is consumed by axum::serve, so capture handles first.
    let writer_for_seal = writer.clone();
    let tenant_router_for_seal = tenant_router.clone();
    let commit_force_timeout = Duration::from_secs(
        std::env::var("SIGLAKE_COMMIT_FORCE_TIMEOUT_SECS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(30),
    );
    let remote_wal_drain =
        remote_wal_drain_from(std::env::var("SIGLAKE_REMOTE_WAL_DRAIN").ok().as_deref());
    let oidc_verifier = match (oidc_issuer, oidc_audience) {
        (Some(issuer), Some(audience)) => {
            tracing::info!(issuer, audience, "ingest OIDC auth enabled");
            let verifier = siglake_core::oidc::OidcVerifier::from_issuer(
                issuer.to_string(),
                audience.to_string(),
            )
            .await
            .with_context(|| format!("OIDC discovery failed for issuer {issuer}"))?;
            let verifier = match oidc_tenant_claim {
                Some(claim) => {
                    tracing::info!(claim, "ingest tenancy bound to a verified JWT claim");
                    verifier.with_tenant_claim(claim)
                }
                None => {
                    tracing::warn!(
                        "ingest tenancy comes from the {} header only — any accepted credential \
                         can write as any tenant. Set --oidc-tenant-claim on a shared cluster.",
                        siglake_ingest::TENANT_HEADER
                    );
                    verifier
                }
            };
            Some(verifier)
        }
        // Both unreachable: `ingest_oidc_config_error` refused a half-set pair
        // at entry, and a claim with no verifier with it. They stay as the
        // exhaustive arms, not as the check.
        (Some(_), None) | (None, Some(_)) => {
            anyhow::bail!("--oidc-issuer and --oidc-audience must both be set (or neither)");
        }
        (None, None) => None,
    };
    // A tenant id is a client header, and each novel value mints a lane with an
    // open file, metric label values, and an Iceberg namespace downstream.
    // `--allowed-tenants` is the bound when you know your tenants;
    // `--max-tenants` is the backstop when you do not.
    let allowed_tenants: Option<std::sync::Arc<std::collections::HashSet<String>>> =
        if allowed_tenants_arg.is_empty() {
            None
        } else {
            Some(std::sync::Arc::new(
                allowed_tenants_arg.iter().cloned().collect(),
            ))
        };
    let mut state = AppState {
        writer,
        tenants: tenant_router.clone(),
        backpressure: None,
        events_tx: Some(events_tx),
        tokens: tokens.map(std::sync::Arc::new),
        oidc_verifier: oidc_verifier.map(std::sync::Arc::new),
        rate_limiter,
        mem_guard: None,
        commit_force_timeout,
        remote_wal_drain,
        allowed_tenants: allowed_tenants.clone(),
        tenant_routing,
        max_tenants,
        tenant_admission: Default::default(),
    };
    if let Some(bp) = backpressure_router {
        state = state.with_backpressure_arc(bp);
    }
    // WS-8 RSS memory circuit breaker: opt-in via --ingest-mem-limit-mib.
    if ingest_mem_limit_mib > 0 {
        let limit_bytes = ingest_mem_limit_mib.saturating_mul(1024 * 1024);
        let guard = std::sync::Arc::new(siglake_ingest::mem_guard::MemoryGuard::new(limit_bytes));
        let interval = Duration::from_secs(ingest_mem_sample_secs.max(1));
        siglake_ingest::mem_guard::spawn_sampler(guard.clone(), interval);
        tracing::info!(
            limit_mib = ingest_mem_limit_mib,
            sample_secs = ingest_mem_sample_secs,
            "RSS memory circuit breaker enabled"
        );
        state = state.with_mem_guard(guard);
    }
    // Keep a separate Arc clone of the backpressure router so we
    // can drain it AFTER `serve` returns. The state is consumed by
    // axum::serve; if we don't keep our own handle here, the only
    // remaining Arc lives inside axum's State extractor and gets
    // dropped without a graceful shutdown — which is precisely
    // what round 11 caught: ingest handlers returned 200 to events
    // still queued in the writer task's mpsc, then SIGTERM killed
    // the runtime before the queue drained.
    let bp_for_drain = state.backpressure.clone();
    // Backpressure lanes also need periodic age-roll so an idle
    // active segment seals without waiting for process shutdown or a
    // new write.
    let backpressure_tick_handle = state.backpressure.clone().map(|router| {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                interval.tick().await;
                router.tick_all().await;
            }
        })
    });
    let serve_result = siglake_ingest::serve_with_otlp_grpc(bind, otlp_grpc_listen, state).await;

    if let Some(bp) = bp_for_drain {
        tracing::info!("draining BackpressureRouter on shutdown");
        bp.shutdown().await;
        tracing::info!("BackpressureRouter drained");
    }

    // Force-seal active WAL segments on shutdown so buffered events land in
    // `sealed/` for the compactor rather than stranding in `active/` when the
    // pod is scaled down. The backpressure router (above) already seals its own
    // lanes; these cover the opt-out (capacity 0) path and are a no-op when
    // backpressure handled the traffic (those writers saw none).
    if let Some(router) = tenant_router_for_seal.as_ref() {
        router.seal_all().await;
    }
    if let Err(e) = writer_for_seal.lock().await.seal() {
        tracing::warn!(error = %e, "default-writer seal-on-shutdown failed");
    }

    tick_handle.abort();
    identity_refresh_handle.abort();
    if let Some(h) = tenant_tick_handle {
        h.abort();
    }
    if let Some(h) = backpressure_tick_handle {
        h.abort();
    }
    if let Some(h) = compactor_handle {
        h.abort();
    }
    if let Some(h) = mirror_task {
        h.abort();
    }
    if let Some(h) = active_mirror_task {
        h.abort();
    }
    serve_result
}

async fn run_wal_recover(from: &str, to: &std::path::Path) -> Result<()> {
    let url = url::Url::parse(from).with_context(|| format!("parse --from URL: {from}"))?;
    let prefix = url.path().trim_start_matches('/').trim_end_matches('/');
    // `--to` used to mean `<wal>/sealed` (recovery flattened everything into
    // one directory). It now means the WAL ROOT, and the layout is rebuilt
    // beneath it. Refuse the old spelling rather than silently building
    // `<wal>/sealed/<tenant>/sealed/...`, which nothing would ever drain.
    if to.file_name().and_then(|n| n.to_str()) == Some(siglake_wal::SEALED_DIR) {
        anyhow::bail!(
            "--to must be the WAL ROOT, not a `{}` directory: recovery now rebuilds the \
             <tenant>[/<index>]/{}/ layout beneath it so each segment is committed to the \
             namespace it came from. Pass {} instead.",
            siglake_wal::SEALED_DIR,
            siglake_wal::SEALED_DIR,
            to.parent().unwrap_or(to).display()
        );
    }
    let store = build_opendal_operator(from)?;
    tracing::info!(from, to = %to.display(), prefix, "wal-recover starting");
    let pulled = siglake_wal::mirror::recover_from_object_store(store, prefix, to).await?;
    println!("pulled {pulled} segments into {}", to.display());
    Ok(())
}

/// Every WAL directory under `root` that can hold a `poison/`: the root
/// itself (legacy single-tenant layout), each tenant, and each tenant's
/// per-index directories. The same walk the drain does each cycle.
fn wal_dirs_under(root: &std::path::Path) -> Result<Vec<PathBuf>> {
    let mut dirs = vec![root.to_path_buf()];
    for (_tenant, dir) in siglake_wal::list_tenant_dirs(root)? {
        for (_index, index_dir) in siglake_wal::list_index_dirs(&dir)? {
            dirs.push(index_dir);
        }
        dirs.push(dir);
    }
    dirs.sort();
    Ok(dirs)
}

/// Implementation of the `siglake wal-requeue` subcommand. See
/// `Command::WalRequeue` docs for the user-facing semantics.
fn run_wal_requeue(wal: &std::path::Path, segment: Option<&str>, dry_run: bool) -> Result<()> {
    if wal.file_name().and_then(|n| n.to_str()) == Some(siglake_wal::POISON_DIR) {
        anyhow::bail!(
            "--wal must be the WAL ROOT, not a `{}` directory: the requeue visits every \
             tenant and index directory beneath it. Pass {} instead.",
            siglake_wal::POISON_DIR,
            wal.parent().unwrap_or(wal).display()
        );
    }
    let mut seen = 0usize;
    let mut moved = 0usize;
    for dir in wal_dirs_under(wal)? {
        for held in siglake_wal::list_poisoned(&dir)? {
            let name = held
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_string();
            if segment.is_some_and(|wanted| wanted != name) {
                continue;
            }
            seen += 1;
            let reason = siglake_wal::read_poison_note(&held)
                .map(|note| format!("{} after {} attempt(s)", note.reason, note.attempts))
                .unwrap_or_else(|| "no note recorded".to_string());
            if dry_run {
                println!("WOULD REQUEUE {} — {reason}", held.display());
                continue;
            }
            let back = siglake_wal::requeue_poisoned_segment(&held)?;
            moved += 1;
            println!(
                "REQUEUED {} -> {} — {reason}",
                held.display(),
                back.display()
            );
        }
    }
    if seen == 0 {
        match segment {
            Some(name) => println!(
                "no segment named {name} is set aside under {}",
                wal.display()
            ),
            None => println!("nothing is set aside under {}", wal.display()),
        }
        return Ok(());
    }
    if dry_run {
        println!("{seen} segment(s) would be requeued; nothing was moved");
    } else {
        println!("requeued {moved} of {seen} segment(s) into sealed/");
    }
    Ok(())
}

/// Implementation of the `siglake audit-rotate` subcommand. See
/// `Command::AuditRotate` docs for the user-facing semantics.
async fn run_audit_rotate(
    data_dir: &std::path::Path,
    warehouse_url: Option<&str>,
    catalog_uri: Option<&str>,
    namespace: &str,
    table: &str,
    max_age_secs: Option<u64>,
    dry_run: bool,
) -> Result<()> {
    let ice = open_iceberg(
        data_dir,
        "warehouse",
        warehouse_url,
        catalog_uri,
        Some(namespace),
    )
    .await?;
    // Any table is fair game for the non-destructive snapshot-age sweep; the
    // destructive drop-and-recreate path stays restricted to `query_audit`.
    if max_age_secs.is_none() && table != "query_audit" {
        anyhow::bail!(
            "audit-rotate --table {table} requires --max-age-secs (the non-destructive \
             snapshot sweep); drop-and-recreate is only allowed for query_audit"
        );
    }
    let ident = iceberg::TableIdent::new(ice.namespace().clone(), table.to_string());

    // Non-destructive snapshot-age sweep: keep the audit rows, just expire old
    // snapshots + reclaim their orphan files.
    if let Some(secs) = max_age_secs {
        // Always keep at least this many recent snapshots regardless of age.
        const RETAIN_LAST: usize = 100;
        /// Floor for the orphan-GC safety window, independent of the snapshot
        /// retention the caller asked for.
        ///
        /// These are two different quantities that happened to be passed the
        /// same number. Snapshot retention is a policy choice — "how much audit
        /// history do I want" — and can legitimately be minutes. `min_age`
        /// guards a RACE: a writer creates data files and only then commits
        /// them, so any file younger than the longest write-then-commit gap
        /// must be presumed live. A measured recluster bin of ~45M rows takes
        /// ~285s, so `--max-age-secs 300` — an entirely reasonable choice for
        /// bounding a hot audit table — left a window in which an
        /// about-to-be-committed data file was treated as an orphan and
        /// DELETED, after which the commit landed referencing a missing object.
        ///
        /// 24h: comfortably beyond any commit gap the system produces, and the
        /// value the operator-rendered CronJobs already got by accident (their
        /// interval is in whole days). Orphans are a space concern, not a
        /// correctness one, so erring long costs nothing but disk.
        const GC_MIN_AGE_FLOOR: std::time::Duration = std::time::Duration::from_secs(86_400);
        let max_age = std::time::Duration::from_secs(secs);
        let gc_min_age = max_age.max(GC_MIN_AGE_FLOOR);
        if dry_run {
            println!(
                "dry-run: would expire {ident} snapshots older than {secs}s \
                 (keeping the most-recent {RETAIN_LAST}), then GC orphans older than {}s \
                 — rows preserved",
                gc_min_age.as_secs()
            );
            return Ok(());
        }
        let expired = ice
            .expire_snapshots_older_than(&ident, RETAIN_LAST, max_age)
            .await?;
        if gc_min_age > max_age {
            tracing::info!(
                requested_secs = secs,
                gc_min_age_secs = gc_min_age.as_secs(),
                "audit-rotate: orphan GC uses its own safety floor, not the retention window — \
                 a file younger than the longest write-then-commit gap may be about to be \
                 committed"
            );
        }
        let gc = ice
            .gc_orphans(
                &ident,
                siglake_storage::iceberg::GcOptions {
                    min_age: gc_min_age,
                    apply: true,
                },
            )
            .await?;
        println!(
            "age sweep: expired {expired} snapshot(s) older than {secs}s on {ident}; \
             reclaimed {} orphan file(s) ({:.1} MiB) older than {}s",
            gc.deleted,
            gc.orphan_bytes as f64 / (1024.0 * 1024.0),
            gc_min_age.as_secs(),
        );
        return Ok(());
    }

    if dry_run {
        let exists = ice.catalog().table_exists(&ident).await?;
        println!(
            "dry-run: would {} {}",
            if exists {
                "drop + recreate"
            } else {
                "create fresh"
            },
            ident
        );
        return Ok(());
    }
    let outcome = ice.rotate_query_audit_table().await?;
    match outcome {
        siglake_storage::iceberg::RotateAuditOutcome::Recreated => {
            println!("rotated: dropped + recreated {ident}");
        }
        siglake_storage::iceberg::RotateAuditOutcome::CreatedFresh => {
            println!("created fresh: {ident} (no prior table to drop)");
        }
    }
    Ok(())
}

/// Implementation of the `siglake gc-orphans` subcommand (BIG-3). Dry-run
/// unless `apply`. See `Command::GcOrphans` docs.
async fn run_gc_orphans(
    data_dir: &std::path::Path,
    warehouse_url: Option<&str>,
    catalog_uri: Option<&str>,
    namespace: &str,
    table: &str,
    min_age_secs: u64,
    apply: bool,
) -> Result<()> {
    use siglake_storage::iceberg::GcOptions;

    let ice = open_iceberg(
        data_dir,
        "warehouse",
        warehouse_url,
        catalog_uri,
        Some(namespace),
    )
    .await?;
    let ident = iceberg::TableIdent::new(ice.namespace().clone(), table.to_string());

    let report = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: std::time::Duration::from_secs(min_age_secs),
                apply,
            },
        )
        .await?;

    let mode = if apply { "APPLY" } else { "dry-run" };
    println!(
        "[{mode}] {ident}: scanned={} reachable={} orphans={} ({:.1} MiB) skipped_recent={} deleted={}",
        report.scanned,
        report.reachable,
        report.orphans,
        report.orphan_bytes as f64 / (1024.0 * 1024.0),
        report.skipped_recent,
        report.deleted,
    );
    if !apply && report.orphans > 0 {
        println!(
            "re-run with --apply to reclaim {} orphan file(s)",
            report.orphans
        );
    }
    Ok(())
}

async fn run_retention_sweep(
    data_dir: &std::path::Path,
    warehouse_url: Option<&str>,
    catalog_uri: Option<&str>,
    namespace: &str,
    index: Option<&str>,
    apply: bool,
) -> Result<()> {
    let ice = open_iceberg(
        data_dir,
        "warehouse",
        warehouse_url,
        catalog_uri,
        Some(namespace),
    )
    .await?;
    let outcomes = if let Some(index_id) = index {
        vec![if apply {
            ice.enforce_index_retention(index_id).await?
        } else {
            ice.preview_index_retention(index_id).await?
        }]
    } else if apply {
        ice.enforce_all_index_retention().await?
    } else {
        ice.preview_all_index_retention().await?
    };

    let mode = if apply { "APPLY" } else { "dry-run" };
    for outcome in outcomes {
        if !outcome.retention_enabled {
            println!("[{mode}] {}: retention disabled", outcome.index_id);
            continue;
        }
        println!(
            "[{mode}] {}: cutoff={} files_dropped={} rows_dropped={} bytes_dropped={:.1} MiB straddling_kept={}",
            outcome.index_id,
            outcome
                .cutoff
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_else(|| "-".to_string()),
            outcome.files_dropped,
            outcome.rows_dropped,
            outcome.bytes_dropped as f64 / (1024.0 * 1024.0),
            outcome.straddling_files_kept,
        );
    }
    if !apply {
        println!(
            "re-run with --apply to commit the file-drop rewrite; snapshot expiry + orphan GC stay separate sweeps"
        );
    }
    Ok(())
}

async fn run_delete_sweep(
    data_dir: &std::path::Path,
    warehouse_url: Option<&str>,
    catalog_uri: Option<&str>,
    namespace: &str,
    index: &str,
    apply: bool,
) -> Result<()> {
    let ice = open_iceberg(
        data_dir,
        "warehouse",
        warehouse_url,
        catalog_uri,
        Some(namespace),
    )
    .await?;
    let outcome = if apply {
        ice.execute_delete_tasks(index).await?
    } else {
        ice.preview_delete_tasks(index).await?
    };
    let mode = if apply { "APPLY" } else { "dry-run" };
    println!(
        "[{mode}] {}: tasks_examined={} tasks_completed={} tasks_failed={} tasks_already_claimed={} files_rewritten={} rows_deleted={}",
        outcome.index_id,
        outcome.tasks_examined,
        outcome.tasks_completed,
        outcome.tasks_failed,
        outcome.tasks_already_claimed,
        outcome.files_rewritten,
        outcome.rows_deleted,
    );
    if outcome.tasks_already_claimed > 0 {
        println!(
            "{} pending task(s) are claimed by another executor and were not re-run",
            outcome.tasks_already_claimed
        );
    }
    if !apply {
        println!(
            "re-run with --apply to execute the rewrites; deleted rows remain in older snapshots until expiry + orphan GC"
        );
    }
    Ok(())
}

/// The schema the running build declares for `table`, or `None` for an
/// unknown table name. Used by `migrate-schema` to reconcile the catalog's
/// stored schema toward the code's.
fn declared_schema_for(
    table: &str,
    promoted: &[siglake_core::PromotedColumn],
) -> Option<datafusion::arrow::datatypes::SchemaRef> {
    use siglake_core::audit_schema::query_audit_schema;
    Some(match table {
        // WS-7: the declared events schema includes any promoted typed columns.
        "events" => siglake_core::events_schema_with(promoted),
        "query_audit" => query_audit_schema(),
        // Managed indexes carry their own declared schema in the table
        // property; `migrate-schema` reconciles the tables siglake DECLARES,
        // and a consumer's index is declared by the consumer.
        _ => return None,
    })
}

/// Every table `migrate-schema --all-tables` reconciles. The tables siglake
/// itself declares; a managed index's schema belongs to whoever declared it.
const MIGRATABLE_TABLES: &[&str] = &["events", "query_audit"];

/// Additively reconcile one or all tables toward their declared schemas. See
/// `Command::MigrateSchema` docs for the user-facing semantics.
#[allow(clippy::too_many_arguments)]
async fn run_migrate_schema(
    data_dir: &std::path::Path,
    warehouse_url: Option<&str>,
    catalog_uri: Option<&str>,
    namespace: &str,
    table: &str,
    all_tables: bool,
    all_namespaces: bool,
    dry_run: bool,
    promoted: Vec<siglake_core::PromotedColumn>,
) -> Result<()> {
    let ice = open_iceberg(
        data_dir,
        "warehouse",
        warehouse_url,
        catalog_uri,
        Some(namespace),
    )
    .await?;

    // Tenancy is header-based, so `events` exists once per tenant namespace.
    // Migrating only the namespace named on the command line reports success
    // having left every other tenant's table narrow — after which the newer
    // binary's writes to those tenants are refused by the write path (which is
    // the safe failure, but it is still an outage per unmigrated tenant).
    let namespaces: Vec<String> = if all_namespaces {
        let mut found: Vec<String> = ice
            .catalog()
            .list_namespaces(None)
            .await
            .context("list namespaces")?
            .into_iter()
            .map(|ns| ns.join("."))
            .collect();
        found.sort();
        if found.is_empty() {
            println!("no namespaces in this warehouse");
            return Ok(());
        }
        println!(
            "migrating {} namespace(s): {}",
            found.len(),
            found.join(", ")
        );
        found
    } else {
        vec![namespace.to_string()]
    };

    let mode = if dry_run { "dry-run" } else { "APPLY" };
    let mut total_added = 0usize;
    for ns in &namespaces {
        let ns_ident = iceberg::NamespaceIdent::from_strs(ns.split('.'))
            .with_context(|| format!("namespace ident {ns}"))?;
        total_added +=
            migrate_one_namespace(&ice, &ns_ident, table, all_tables, dry_run, &promoted, mode)
                .await
                .with_context(|| format!("migrate namespace {ns}"))?;
    }
    let verb = if dry_run { "pending" } else { "added" };
    println!("migrate-schema complete: {total_added} column(s) {verb}");
    Ok(())
}

// Barrier installed by [`rendezvous_after_observation`]'s tests. Thread-local,
// so a test that installs one is not seen by the tests running beside it.
#[cfg(test)]
thread_local! {
    static AFTER_OBSERVATION: std::cell::RefCell<Option<Arc<tokio::sync::Barrier>>> =
        const { std::cell::RefCell::new(None) };
}

/// Test-only seam between reading the events table's recorded version and
/// everything [`migrate_one_namespace`] does afterwards: a concurrent
/// migrator's higher stamp landing in that window is the #2553 regression, and
/// the function has nowhere else to be held open. Compiles away outside tests.
///
/// Two-phase: the first wait releases the competing writer, the second waits
/// for it to finish, so the migration continues with the observation it already
/// made and a table that has moved underneath it.
#[cfg(test)]
async fn rendezvous_after_observation() {
    let gate = AFTER_OBSERVATION.with(|hook| hook.borrow().clone());
    if let Some(gate) = gate {
        gate.wait().await;
        gate.wait().await;
    }
}

#[cfg(not(test))]
async fn rendezvous_after_observation() {}

/// [`run_migrate_schema`] for one namespace's context.
#[allow(clippy::too_many_arguments)]
async fn migrate_one_namespace(
    ice: &siglake_storage::iceberg::IcebergContext,
    ns: &iceberg::NamespaceIdent,
    table: &str,
    all_tables: bool,
    dry_run: bool,
    promoted: &[siglake_core::PromotedColumn],
    mode: &str,
) -> Result<usize> {
    let tables: Vec<&str> = if all_tables {
        MIGRATABLE_TABLES.to_vec()
    } else {
        vec![table]
    };
    let mut total_added = 0usize;
    for t in tables {
        let Some(schema) = declared_schema_for(t, promoted) else {
            anyhow::bail!(
                "unknown table `{t}` — known: {}",
                MIGRATABLE_TABLES.join(", ")
            );
        };
        let ident = iceberg::TableIdent::new(ns.clone(), t.to_string());
        // Skip rather than provision: a warehouse can hold namespaces siglake
        // did not create, and a migration tool must not invent tables.
        if !ice
            .catalog()
            .table_exists(&ident)
            .await
            .with_context(|| format!("table_exists {ident}"))?
        {
            println!("[{mode}] {ident}: absent, skipped");
            continue;
        }
        // Report the version the TABLE records, not the one this binary
        // declares — the two differing is the whole point, and until now
        // nothing in the product could tell them apart.
        let mut observed = None;
        if t == "events" {
            // This is a refusal boundary, not an additive migration: Iceberg
            // cannot retype the old nanosecond timestamp in place, and adding
            // timestamp_ns would leave all historical rows null. Validate
            // before both the dry-run diff and the apply path.
            ice.assert_events_timestamp_contract(&ident).await?;
            let at = ice.observed_schema_version(&ident).await?;
            let want = siglake_core::EVENTS_SCHEMA_VERSION;
            if at == want {
                println!("[{mode}] {ident}: schema version {at}");
            } else {
                println!("[{mode}] {ident}: schema version {at}, this binary declares {want}");
            }
            observed = Some(at);
            // Everything after this point may run while another migration job
            // (a Helm hook, the operator's Job, this same Job retried) is
            // migrating the same table, so `observed` is a report and never an
            // input to the stamp below.
            rendezvous_after_observation().await;
        }
        if dry_run {
            let pending = ice
                .pending_schema_additions(&ident, schema.as_ref())
                .await?;
            if pending.is_empty() {
                println!("[{mode}] {ident}: up to date");
            } else {
                println!(
                    "[{mode}] {ident}: would add {} column(s): {}",
                    pending.len(),
                    pending.join(", ")
                );
                total_added += pending.len();
            }
        } else {
            let added = ice
                .migrate_table_schema_additive(&ident, schema.as_ref())
                .await?;
            if added == 0 {
                println!("[{mode}] {ident}: up to date");
            } else {
                println!("[{mode}] {ident}: added {added} column(s)");
                total_added += added;
            }
            // Stamp AFTER the widen, so the recorded version is evidence the
            // columns exist rather than a promise that they will. Only the
            // events table carries the events schema version.
            //
            // The stamp is MONOTONE: a table already recording a higher
            // version than this binary declares keeps it. An older binary
            // meeting an already-widened table (a Helm roll-forward after a
            // rollback, or an operator `spec.image` revert) adds no column, so
            // stamping its own lower constant would make the table report a
            // shape it does not have — and `--dry-run --all-namespaces` is how
            // an operator asks what shape a table is at.
            //
            // The maximum is taken by the storage operation against the base it
            // commits onto, not against `observed` here: a concurrent migrator
            // can publish a higher version after this run read the table, and
            // again between a lost CAS and its retry.
            if t == "events" {
                let recorded = ice
                    .stamp_schema_version_at_least(&ident, siglake_core::EVENTS_SCHEMA_VERSION)
                    .await?;
                if observed.is_some_and(|at| at != recorded) {
                    println!("[{mode}] {ident}: schema version now {recorded}");
                }
            }
        }
    }
    Ok(total_added)
}

#[cfg(test)]
mod migrate_schema_tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use iceberg::arrow::arrow_schema_to_schema;
    use iceberg::spec::{FormatVersion, PrimitiveType, Type};
    use iceberg::TableCreation;

    async fn old_contract_events_table() -> (tempfile::TempDir, IcebergContext) {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        ice.catalog().drop_table(ice.table_ident()).await.unwrap();

        let old_fields = siglake_core::events_schema()
            .fields()
            .iter()
            .filter(|field| field.name() != siglake_core::TIMESTAMP_NS_COLUMN)
            .map(|field| {
                let field = field.as_ref().clone();
                if field.name() == "timestamp" {
                    field.with_data_type(DataType::Timestamp(
                        TimeUnit::Nanosecond,
                        Some("+00:00".into()),
                    ))
                } else {
                    field
                }
            })
            .collect::<Vec<_>>();
        let old_arrow = datafusion::arrow::datatypes::Schema::new(old_fields);
        let old_iceberg = arrow_schema_to_schema(&old_arrow).unwrap();
        let creation = TableCreation::builder()
            .name("events".to_string())
            .schema(old_iceberg)
            .format_version(FormatVersion::V3)
            .properties(std::collections::HashMap::from([(
                siglake_core::SCHEMA_VERSION_PROPERTY_KEY.to_string(),
                "2".to_string(),
            )]))
            .build();
        ice.catalog()
            .create_table(ice.namespace(), creation)
            .await
            .unwrap();
        (tmp, ice)
    }

    async fn assert_old_contract_unchanged(ice: &IcebergContext) {
        let table = ice.catalog().load_table(ice.table_ident()).await.unwrap();
        assert_eq!(table.metadata().format_version(), FormatVersion::V3);
        let schema = table.metadata().current_schema();
        let timestamp_id = schema.field_id_by_name("timestamp").unwrap();
        assert_eq!(
            schema
                .field_by_id(timestamp_id)
                .unwrap()
                .field_type
                .as_ref(),
            &Type::Primitive(PrimitiveType::TimestamptzNs)
        );
        assert!(
            schema
                .field_id_by_name(siglake_core::TIMESTAMP_NS_COLUMN)
                .is_none(),
            "refusal must not add timestamp_ns"
        );
        assert_eq!(
            table
                .metadata()
                .properties()
                .get(siglake_core::SCHEMA_VERSION_PROPERTY_KEY)
                .map(String::as_str),
            Some("2"),
            "refusal must not stamp the current schema version"
        );
    }

    #[tokio::test]
    async fn old_timestamp_contract_refuses_dry_run_and_apply_without_mutation() {
        for dry_run in [true, false] {
            let (_tmp, ice) = old_contract_events_table().await;
            let err = migrate_one_namespace(
                &ice,
                ice.namespace(),
                "events",
                false,
                dry_run,
                &[],
                if dry_run { "dry-run" } else { "APPLY" },
            )
            .await
            .expect_err("a pre-contract events table must be refused");
            let message = format!("{err:#}");
            assert!(message.contains("Timestamp contract"), "{message}");
            assert!(
                message.contains("docs/DESIGN_time_ordered_storage.md"),
                "{message}"
            );
            assert_old_contract_unchanged(&ice).await;
        }
    }

    #[tokio::test]
    async fn pending_additions_reports_an_existing_column_type_mismatch() {
        let (_tmp, ice) = old_contract_events_table().await;
        let err = ice
            .pending_schema_additions(ice.table_ident(), siglake_core::events_schema().as_ref())
            .await
            .expect_err("same-name timestamp types must not compare as up to date");
        let message = format!("{err:#}");
        assert!(message.contains("schema type mismatch"), "{message}");
        assert!(message.contains("timestamp"), "{message}");
    }

    #[tokio::test]
    async fn compatible_events_table_still_accepts_additive_migration() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        let promoted = [siglake_core::PromotedColumn {
            attr_key: "service.name".to_string(),
            name: "service_name".to_string(),
            ty: siglake_core::PromotedType::Utf8,
        }];

        let added = migrate_one_namespace(
            &ice,
            ice.namespace(),
            "events",
            false,
            false,
            &promoted,
            "APPLY",
        )
        .await
        .unwrap();
        assert_eq!(added, 1);
        let table = ice.catalog().load_table(ice.table_ident()).await.unwrap();
        assert_eq!(table.metadata().format_version(), FormatVersion::V2);
        assert!(table
            .metadata()
            .current_schema()
            .field_id_by_name("service_name")
            .is_some());
    }

    /// Widen the events table past what this binary declares, and record a
    /// version above `EVENTS_SCHEMA_VERSION` — what a newer binary leaves
    /// behind, met by an older one on a Helm roll-forward after a rollback or
    /// an operator `spec.image` revert.
    async fn widened_future_version_table() -> (tempfile::TempDir, IcebergContext, u32) {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        let promoted = [siglake_core::PromotedColumn {
            attr_key: "service.name".to_string(),
            name: "service_name".to_string(),
            ty: siglake_core::PromotedType::Utf8,
        }];
        let added = migrate_one_namespace(
            &ice,
            ice.namespace(),
            "events",
            false,
            false,
            &promoted,
            "APPLY",
        )
        .await
        .unwrap();
        assert_eq!(added, 1);
        let future = siglake_core::EVENTS_SCHEMA_VERSION + 1;
        // Fixture setup: this table has to record a version this binary would
        // never write, which is what the explicit writer is kept for.
        #[allow(clippy::disallowed_methods)]
        ice.stamp_schema_version(ice.table_ident(), future)
            .await
            .unwrap();
        (tmp, ice, future)
    }

    async fn recorded_version(ice: &IcebergContext) -> Option<u32> {
        let table = ice.catalog().load_table(ice.table_ident()).await.unwrap();
        table
            .metadata()
            .properties()
            .get(siglake_core::SCHEMA_VERSION_PROPERTY_KEY)
            .and_then(|raw| raw.parse::<u32>().ok())
    }

    /// An older binary's migration run on an already-widened table must not
    /// stamp the recorded version DOWN: it added nothing, so it has no evidence
    /// the table narrowed, and `--dry-run` is the documented way to ask a table
    /// what shape it is at.
    #[tokio::test]
    async fn an_older_binarys_migration_does_not_lower_the_recorded_version() {
        let (_tmp, ice, future) = widened_future_version_table().await;

        // The older binary: no promoted columns, so a narrower declared schema.
        let added =
            migrate_one_namespace(&ice, ice.namespace(), "events", false, false, &[], "APPLY")
                .await
                .unwrap();
        assert_eq!(added, 0, "a narrower declared schema adds no column");
        assert_eq!(
            recorded_version(&ice).await,
            Some(future),
            "the apply path must preserve a higher recorded version"
        );
        assert!(
            ice.catalog()
                .load_table(ice.table_ident())
                .await
                .unwrap()
                .metadata()
                .current_schema()
                .field_id_by_name("service_name")
                .is_some(),
            "the widened column survives the older binary's run"
        );

        // And the dry-run that reports it neither mutates nor disagrees.
        let pending =
            migrate_one_namespace(&ice, ice.namespace(), "events", false, true, &[], "dry-run")
                .await
                .unwrap();
        assert_eq!(pending, 0);
        assert_eq!(
            ice.observed_schema_version(ice.table_ident())
                .await
                .unwrap(),
            future
        );
        assert_eq!(recorded_version(&ice).await, Some(future));
    }

    /// Monotone is not "never stamp": a table behind this binary is still
    /// advanced once the widen commits.
    #[tokio::test]
    async fn a_forward_migration_still_advances_the_recorded_version() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        ice.ensure_events_table().await.unwrap();
        let behind = siglake_core::EVENTS_SCHEMA_VERSION - 1;
        // Fixture setup: start the table below this binary's constant.
        #[allow(clippy::disallowed_methods)]
        ice.stamp_schema_version(ice.table_ident(), behind)
            .await
            .unwrap();

        let promoted = [siglake_core::PromotedColumn {
            attr_key: "service.name".to_string(),
            name: "service_name".to_string(),
            ty: siglake_core::PromotedType::Utf8,
        }];
        let added = migrate_one_namespace(
            &ice,
            ice.namespace(),
            "events",
            false,
            false,
            &promoted,
            "APPLY",
        )
        .await
        .unwrap();
        assert_eq!(added, 1);
        assert_eq!(
            recorded_version(&ice).await,
            Some(siglake_core::EVENTS_SCHEMA_VERSION)
        );
    }

    /// #2553: two migration jobs overlap. This one reads the recorded version
    /// first; the other publishes a higher one before this one stamps. The
    /// version it reported is stale from that moment on, and stamping it — even
    /// as the maximum of the report and this binary's constant, which is what
    /// the sequential rollback fix computed — takes the other job's version
    /// away.
    ///
    /// The barrier holds this run open at the observation seam, so the
    /// interleaving is chosen rather than raced. Against the precomputed stamp
    /// (`max(observed, EVENTS_SCHEMA_VERSION)`, written through
    /// `stamp_schema_version`) it fails with `left: Some(3), right: Some(5)`.
    #[tokio::test]
    async fn a_version_published_after_the_observation_is_not_stamped_down() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        ice.ensure_events_table().await.unwrap();
        // Behind this binary, so the run below has a real stamp to make.
        #[allow(clippy::disallowed_methods)]
        ice.stamp_schema_version(ice.table_ident(), siglake_core::EVENTS_SCHEMA_VERSION - 1)
            .await
            .unwrap();
        let competing = siglake_core::EVENTS_SCHEMA_VERSION + 2;

        let gate = Arc::new(tokio::sync::Barrier::new(2));
        AFTER_OBSERVATION.with(|hook| *hook.borrow_mut() = Some(gate.clone()));

        let competitor = {
            let ice = ice.clone();
            let gate = gate.clone();
            async move {
                // Wait for the other run to have made its observation, publish
                // a higher version, and only then let it continue.
                gate.wait().await;
                // The competing migrator publishes an exact version.
                #[allow(clippy::disallowed_methods)]
                ice.stamp_schema_version(ice.table_ident(), competing)
                    .await
                    .unwrap();
                gate.wait().await;
            }
        };
        let migrator =
            migrate_one_namespace(&ice, ice.namespace(), "events", false, false, &[], "APPLY");
        let (_, added) = tokio::join!(competitor, migrator);

        assert_eq!(added.unwrap(), 0, "a narrower declared schema adds nothing");
        assert_eq!(
            recorded_version(&ice).await,
            Some(competing),
            "the apply path stamped a version the observation had already lost"
        );
        AFTER_OBSERVATION.with(|hook| *hook.borrow_mut() = None);
    }
}

/// Build an opendal `Operator` from a URL. Supports `s3://`,
/// `file://`, and `memory://`. Credential resolution follows the full
/// AWS SDK chain via [`siglake_storage::aws_credential::SiglakeAwsLoader`]:
/// static keys → IRSA → ECS/Fargate task role → EC2 IMDSv2.
pub fn build_opendal_operator(url: &str) -> Result<opendal::Operator> {
    let parsed = url::Url::parse(url).with_context(|| format!("parse URL: {url}"))?;
    let scheme = parsed.scheme();
    match scheme {
        "s3" | "s3a" => {
            let bucket = parsed
                .host_str()
                .ok_or_else(|| anyhow::anyhow!("S3 URL missing bucket: {url}"))?;
            let root = parsed.path().trim_end_matches('/');
            let region = std::env::var("AWS_REGION")
                .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                .unwrap_or_else(|_| "us-east-1".to_string());
            let mut builder = opendal::services::S3::default()
                .bucket(bucket)
                .region(&region)
                .customized_credential_load(Box::new(
                    siglake_storage::aws_credential::SiglakeAwsLoader::new(),
                ));
            if !root.is_empty() {
                builder = builder.root(root);
            }
            if let Ok(e) = std::env::var("AWS_ENDPOINT_URL") {
                builder = builder.endpoint(&e);
            }
            Ok(opendal::Operator::new(builder)?.finish())
        }
        "file" => {
            let root = parsed.path();
            let builder = opendal::services::Fs::default().root(root);
            Ok(opendal::Operator::new(builder)?.finish())
        }
        "memory" => {
            let builder = opendal::services::Memory::default();
            Ok(opendal::Operator::new(builder)?.finish())
        }
        other => anyhow::bail!("unsupported scheme `{other}` for WAL mirror URL: {url}"),
    }
}

fn hostname_lossy() -> Result<String> {
    use std::process::Command;
    let out = Command::new("hostname").output()?;
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Strip any `user:pass@` from a `redis://...` URL before logging.
/// Production Redis URLs frequently carry an inline password
/// (`redis://:hunter2@host:6379/0`) and we don't want it in
/// startup logs.
fn redact_redis_url(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(mut u) => {
            if !u.password().unwrap_or_default().is_empty() {
                let _ = u.set_password(Some("***"));
            }
            if !u.username().is_empty() {
                let _ = u.set_username("***");
            }
            u.to_string()
        }
        Err(_) => "<unparseable redis url>".to_string(),
    }
}

struct RunSubscribeArgs {
    data_dir: PathBuf,
    table: String,
    time_column: Option<String>,
    since: Option<String>,
    interval: Duration,
    once: bool,
    warehouse_sub: String,
    warehouse_url: Option<String>,
    catalog_uri: Option<String>,
}

async fn run_subscribe(args: RunSubscribeArgs) -> Result<()> {
    let ice = Arc::new(
        open_iceberg(
            &args.data_dir,
            &args.warehouse_sub,
            args.warehouse_url.as_deref(),
            args.catalog_uri.as_deref(),
            None,
        )
        .await?,
    );
    let time_column = match args.time_column.clone() {
        Some(c) => c,
        None => default_time_column(&ice, &args.table).await,
    };
    let since = parse_since(args.since.as_deref(), chrono::Duration::minutes(1))?;
    let mut sub = IcebergSubscription::new(ice, args.table.clone(), time_column, since);

    loop {
        let batches = sub.poll().await?;
        if !batches.is_empty() {
            print_batches(&batches)?;
        }
        if args.once {
            return Ok(());
        }
        tokio::time::sleep(args.interval).await;
    }
}

/// Which column to tail by, when the caller did not say.
///
/// Asks the INDEX what its event-time field is, rather than consulting a table
/// of known names. That list held five detection tables back when siglake owned
/// them; an index declares its own `timestamp_field`, so any index — including
/// a consumer's own output tables — gets the right answer without being known
/// here.
async fn default_time_column(ice: &IcebergContext, table: &str) -> String {
    if table == "events" {
        return "timestamp".to_string();
    }
    match ice.get_index(table).await {
        Ok(Some(config)) => config.doc_mapping.timestamp_field,
        // Not an index, or the catalog is unhappy: `timestamp` is the
        // convention and a wrong guess surfaces as a clear planning error.
        _ => "timestamp".to_string(),
    }
}

fn parse_since(
    since: Option<&str>,
    default_back: chrono::Duration,
) -> Result<chrono::DateTime<chrono::Utc>> {
    match since {
        Some(s) => s
            .parse::<chrono::DateTime<chrono::Utc>>()
            .with_context(|| format!("parse --since as RFC3339: {s}")),
        None => Ok(chrono::Utc::now() - default_back),
    }
}

/// Registers `events` plus every managed index with DataFusion and runs the
/// supplied SQL.
async fn run_sql(
    data_dir: &std::path::Path,
    warehouse_sub: &str,
    warehouse_url: Option<&str>,
    catalog_uri: Option<&str>,
    sql: &str,
) -> Result<()> {
    let ice = open_iceberg(data_dir, warehouse_sub, warehouse_url, catalog_uri, None).await?;
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await?;
    // Every managed index, rather than a hardcoded pair -- so a consumer's own
    // output tables are queryable here the same as anyone's.
    ice.register_indexes_with_datafusion(&ctx).await?;
    let df = ctx
        .sql(sql)
        .await
        .with_context(|| format!("executing SQL: {sql}"))?;
    let batches = df.collect().await.context("collect SQL results")?;
    print_batches(&batches)
}

fn print_batches(batches: &[datafusion::arrow::array::RecordBatch]) -> Result<()> {
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        println!("(no rows)");
        return Ok(());
    }
    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(batches)?;
    println!("{formatted}");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_compactor(
    data_dir: &std::path::Path,
    metrics_bind: SocketAddr,
    wal_sub: &str,
    warehouse_sub: &str,
    warehouse_url: Option<&str>,
    catalog_uri: Option<&str>,
    once: bool,
    interval: Duration,
    catalog_claim: bool,
    mirror_prefix: &str,
    catalog_claim_batch: usize,
    role: siglake_compactor::CompactorRole,
    fs_claim_max_segments: usize,
    fs_claim_max_bytes: u64,
    promoted: Vec<siglake_core::PromotedColumn>,
) -> Result<()> {
    let wal_dir = if std::path::Path::new(wal_sub).is_absolute() {
        std::path::PathBuf::from(wal_sub)
    } else {
        data_dir.join(wal_sub)
    };
    let ice = open_iceberg(data_dir, warehouse_sub, warehouse_url, catalog_uri, None)
        .await?
        .with_promoted_columns(promoted);
    // WS-7: widen the (default-tenant) events table to carry the declared typed
    // columns before any append. Per-tenant tables widen on first use.
    ice.ensure_promoted_columns().await?;
    let ice = Arc::new(ice);
    let mut compactor = if catalog_claim {
        let claim_uri = catalog_uri.ok_or_else(|| {
            anyhow::anyhow!("--catalog-claim requires --catalog-uri (Postgres or SQLite)")
        })?;
        let url = warehouse_url.ok_or_else(|| {
            anyhow::anyhow!("--catalog-claim requires --warehouse-url (object-store mirror root)")
        })?;
        // The ingester's mirror opt-out is an EMPTY `SIGLAKE_WAL_MIRROR_PREFIX`,
        // and that variable reaches this flag too — a deployment that sets it
        // cluster-wide (the operator's `spec.extraEnv` goes to every tier) would
        // otherwise claim from the bucket ROOT and list the whole warehouse.
        if mirror_prefix.trim().is_empty() {
            anyhow::bail!(
                "--catalog-claim requires a non-empty --mirror-prefix \
                 (SIGLAKE_WAL_MIRROR_PREFIX is empty, which is the ingester's mirror opt-out: \
                 catalog-claim drains read the mirror, so there would be nothing to claim)"
            );
        }
        let store = build_opendal_operator(url)?;
        let claimer = std::env::var("SIGLAKE_COMPACTOR_ID")
            .or_else(|_| hostname_lossy())
            .unwrap_or_else(|_| format!("comp-{}", &Uuid::new_v4().to_string()[..8]));
        tracing::info!(
            mirror_prefix,
            batch_size = catalog_claim_batch,
            claimer,
            "compactor catalog-claim mode enabled"
        );
        let claim = siglake_storage::catalog_claim::SqlSegmentClaim::connect(claim_uri, claimer)
            .await
            .with_context(|| format!("connect claim DB at {claim_uri}"))?;
        Compactor::new(&wal_dir, ice).with_catalog_claim(siglake_compactor::CatalogClaimConfig {
            claim,
            store,
            prefix: mirror_prefix.to_string(),
            batch_size: catalog_claim_batch,
            last_mirror_sync: Default::default(),

            last_reclaim: std::sync::Arc::new(std::sync::Mutex::new(None)),
        })
    } else {
        Compactor::new(&wal_dir, ice)
            .with_fs_batch_limits(fs_claim_max_segments, fs_claim_max_bytes)
    };
    if let Some(cfg) = recluster_cfg_from_env() {
        compactor = compactor.with_reclustering(cfg);
    }
    if let Some(cfg) = expire_cfg_from_env() {
        compactor = compactor.with_snapshot_expiry(cfg);
    }
    compactor = compactor.with_delete_tasks(delete_tasks_enabled_from_env());
    compactor = compactor.with_role(role);
    tracing::info!(?role, "compactor role");
    if let Some(cfg) = commit_batch_cfg_from_env() {
        compactor = compactor.with_commit_batching(cfg);
    }
    if once {
        // For `--once` runs we skip the metrics server: scrapers wouldn't
        // get a chance to read it before the process exits.
        let _ = metrics_bind;
        let n = compactor.run_once().await?;
        tracing::info!(committed = n, "compactor --once complete");
        println!("committed {n} segment(s)");
        Ok(())
    } else {
        let _metrics_handle = siglake_core::metrics::init(metrics_bind).await?;
        // Alerted counters exist at 0 from the first scrape, so the first
        // watchdog trip or unprovable reclaim is a delta `increase()` can see.
        siglake_core::metrics::preregister(siglake_core::metrics::COMPACTOR_ALERTED_COUNTERS);
        let build = siglake_core::build_info();
        metrics::gauge!(
            "siglake_build_info",
            "version" => build.version,
            "commit" => build.commit
        )
        .set(1.0);
        tracing::info!(
            wal = %wal_dir.display(),
            warehouse_url = warehouse_url.unwrap_or("(local)"),
            interval_ms = interval.as_millis() as u64,
            "compactor daemon starting"
        );
        std::sync::Arc::new(compactor).run_loop(interval).await
    }
}

fn synth_event(i: usize) -> Event {
    Event {
        timestamp: chrono::Utc::now(),
        host: format!("host-{}", i % 4),
        source: "/var/log/app.log".into(),
        sourcetype: "app:json".into(),
        index: "main".into(),
        raw: format!("event {i} status={} latency_ms={}", i % 5, (i * 7) % 250),
        attributes: None,
    }
}

/// First nanosecond the fixture writes: 2026-01-01T00:00:00.000000000Z.
const FIXTURE_BASE_NANOS: i64 = 1_767_225_600_000_000_000;

/// One fixture row. Deterministic, unlike [`synth_event`]'s `Utc::now()`, so an
/// external engine can assert exact values — and spaced 1 ns apart so that
/// every microsecond of `timestamp` holds a thousand rows. That makes the
/// fixture a real test of the 2026-09-06 timestamp contract: only
/// `timestamp_ns` distinguishes rows inside one microsecond, and only the
/// `(timestamp, timestamp_ns)` sort order is total.
fn fixture_event(i: usize) -> Event {
    let mut e = synth_event(i);
    e.timestamp = chrono::DateTime::from_timestamp_nanos(FIXTURE_BASE_NANOS + i as i64);
    e
}

async fn iceberg_demo(
    data_dir: &std::path::Path,
    n: usize,
    warehouse: &str,
    reset: bool,
) -> Result<()> {
    let warehouse_dir = data_dir.join(warehouse);
    if reset && warehouse_dir.exists() {
        std::fs::remove_dir_all(&warehouse_dir)
            .with_context(|| format!("resetting warehouse {}", warehouse_dir.display()))?;
    }
    tracing::info!(warehouse = %warehouse_dir.display(), n, reset, "iceberg demo start");

    let ice = IcebergContext::open(&warehouse_dir).await?;

    // Append in two batches to exercise the multi-snapshot path.
    let half = n / 2;
    let batch_a: Vec<Event> = (0..half).map(fixture_event).collect();
    let batch_b: Vec<Event> = (half..n).map(fixture_event).collect();
    let wrote_a = ice.append_events(&batch_a).await?;
    let wrote_b = ice.append_events(&batch_b).await?;
    tracing::info!(rows_a = wrote_a, rows_b = wrote_b, "appends committed");

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await?;

    println!("=== count ===");
    ctx.sql("SELECT count(*) AS rows FROM events")
        .await?
        .show()
        .await?;
    println!("=== per-host ===");
    ctx.sql("SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY host")
        .await?
        .show()
        .await?;
    println!("=== time bounds ===");
    ctx.sql("SELECT min(timestamp) AS t_min, max(timestamp) AS t_max FROM events")
        .await?
        .show()
        .await?;
    println!("=== status=3 ===");
    ctx.sql("SELECT count(*) AS n FROM events WHERE raw LIKE '%status=3%'")
        .await?
        .show()
        .await?;

    println!("=== timestamp contract ===");
    ctx.sql(
        "SELECT timestamp, timestamp_ns FROM events \
         ORDER BY timestamp ASC, timestamp_ns ASC LIMIT 5",
    )
    .await?
    .show()
    .await?;
    iceberg_demo_assert_timestamp_contract(&ice, &ctx, n).await?;

    Ok(())
}

/// Assert, in-process, everything an external engine is then asked to confirm
/// about the 2026-09-06 timestamp contract, and print the expected values so a
/// DuckDB/Spark/PyIceberg harness can diff against them.
///
/// This is the siglake half of the external-engine regression check driven by
/// `scripts/check-external-timestamp-contract.sh`.
async fn iceberg_demo_assert_timestamp_contract(
    ice: &IcebergContext,
    ctx: &SessionContext,
    n: usize,
) -> Result<()> {
    ice.assert_events_timestamp_contract(ice.table_ident())
        .await?;

    // The nanosecond round-trip: `timestamp_ns` is the value handed in
    // verbatim, and `timestamp` is that value floored to the microsecond. The
    // microsecond bounds are computed here rather than divided out of the
    // nanosecond ones by the harness, so the external engines are held to a
    // value siglake itself read back out of the column.
    let rows = ctx
        .sql(
            // `arrow_cast(timestamp, 'Int64')` is the stored microsecond value.
            // Integer division truncates toward zero, which equals the
            // contract's floor for the fixture's post-epoch instants.
            "SELECT count(*), min(timestamp_ns), max(timestamp_ns), \
             count(*) FILTER (WHERE timestamp_ns / 1000 \
               != arrow_cast(timestamp, 'Int64')), \
             min(arrow_cast(timestamp, 'Int64')), \
             max(arrow_cast(timestamp, 'Int64')), \
             count(*) FILTER (WHERE timestamp IS NULL OR timestamp_ns IS NULL) \
             FROM events",
        )
        .await?
        .collect()
        .await?;
    let batch = rows
        .first()
        .filter(|b| b.num_rows() == 1)
        .context("contract query returned no row")?;
    let int = |col: usize| -> Result<i64> {
        batch
            .column(col)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .map(|a| a.value(0))
            .with_context(|| format!("column {col} is not Int64"))
    };
    let (count, min_ns, max_ns, disagreements) = (int(0)?, int(1)?, int(2)?, int(3)?);
    let (min_us, max_us, null_rows) = (int(4)?, int(5)?, int(6)?);

    if count != n as i64 {
        bail!("committed {count} rows, expected {n}");
    }
    if min_ns != FIXTURE_BASE_NANOS || max_ns != FIXTURE_BASE_NANOS + n as i64 - 1 {
        bail!(
            "timestamp_ns spans [{min_ns}, {max_ns}], expected [{}, {}]",
            FIXTURE_BASE_NANOS,
            FIXTURE_BASE_NANOS + n as i64 - 1
        );
    }
    if disagreements != 0 {
        bail!(
            "{disagreements} rows where timestamp is not timestamp_ns floored to the microsecond"
        );
    }
    if null_rows != 0 {
        bail!("{null_rows} rows with a null timestamp or timestamp_ns; both columns are required");
    }
    // The external engines diff their own decoded microseconds against these,
    // so a wrong value here would be handed to them as the expectation.
    if min_us != min_ns / 1000 || max_us != max_ns / 1000 {
        bail!(
            "timestamp spans [{min_us}, {max_us}] us, expected [{}, {}] (timestamp_ns floored)",
            min_ns / 1000,
            max_ns / 1000
        );
    }

    // Total order: rows one nanosecond apart share microseconds, so ordering by
    // `timestamp` alone cannot be total while the pair is.
    let distinct = ctx
        .sql("SELECT count(DISTINCT timestamp), count(DISTINCT timestamp_ns) FROM events")
        .await?
        .collect()
        .await?;
    let distinct = distinct.first().context("distinct query returned no row")?;
    let distinct_at = |col: usize| -> Result<i64> {
        distinct
            .column(col)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .map(|a| a.value(0))
            .with_context(|| format!("column {col} is not Int64"))
    };
    let (distinct_us, distinct_ns) = (distinct_at(0)?, distinct_at(1)?);
    if distinct_ns != count {
        bail!("{distinct_ns} distinct timestamp_ns values over {count} rows: not a total order");
    }

    println!(
        "contract ok: format_version=2 timestamp=timestamptz(us) timestamp_ns=long\n\
         external assertions: rows={count} min_timestamp_ns={min_ns} max_timestamp_ns={max_ns} \
         min_timestamp_us={min_us} max_timestamp_us={max_us} \
         distinct_timestamp={distinct_us} distinct_timestamp_ns={distinct_ns}"
    );
    Ok(())
}

fn gen(n: usize) -> Result<()> {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for i in 0..n {
        let e = synth_event(i);
        serde_json::to_writer(&mut out, &e)?;
        writeln!(out)?;
    }
    Ok(())
}

async fn ingest(data_dir: &std::path::Path, input: Option<PathBuf>) -> Result<()> {
    let reader: Box<dyn BufRead> = match input {
        Some(p) => Box::new(std::io::BufReader::new(
            std::fs::File::open(&p).with_context(|| format!("opening {}", p.display()))?,
        )),
        None => Box::new(std::io::BufReader::new(std::io::stdin().lock())),
    };

    let mut events: Vec<Event> = Vec::new();
    for (line_no, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let event: Event = serde_json::from_str(&line)
            .with_context(|| format!("parsing NDJSON line {}", line_no + 1))?;
        events.push(event);
    }

    if events.is_empty() {
        bail!("no events read");
    }

    let batch = events_to_record_batch(&events)?;
    let store = local_store(data_dir)?;

    // Phase 1: partition by the hour of the first event in the batch.
    // The compactor in phase 2 will own real partition assignment, sorting,
    // and row-group sizing.
    let pk = events[0].timestamp.format("events/%Y/%m/%d/%H").to_string();
    let file_name = format!("{}.parquet", Uuid::now_v7());
    let path = ObjectPath::from(format!("{pk}/{file_name}"));

    let bytes_written = write_batch_as_parquet(store.as_ref(), &path, &batch).await?;
    tracing::info!(
        rows = events.len(),
        bytes = bytes_written,
        path = %path,
        "wrote parquet file"
    );
    println!(
        "wrote {} events ({} bytes) to {}",
        events.len(),
        bytes_written,
        path
    );
    Ok(())
}

async fn query(data_dir: &std::path::Path, sql: &str) -> Result<()> {
    let ctx = session_context();
    let abs = std::fs::canonicalize(data_dir)
        .with_context(|| format!("canonicalize {}", data_dir.display()))?;
    let url = format!("file://{}/events/", abs.display());
    register_parquet_dir(&ctx, &url, "events", Some(events_schema())).await?;

    let df = ctx.sql(sql).await?;
    df.show().await?;
    Ok(())
}

/// Async delete-task execution for the compactor daemon — **default ON since
/// 2026-09-11**; `SIGLAKE_DELETE_TASKS=0` (or `off`/`false`/`no`) disables the
/// idle-cycle sweep.
///
/// This was opt-in and OFF, which made `POST /api/v1/delete-tasks` answer `201`
/// for a task that nothing would ever run: the submitter is told the deletion
/// was accepted, the record sits `pending` forever, and the only signal is a
/// gauge nobody reads on a fresh install. An accepted GDPR deletion that never
/// executes is the worse failure of the two, so the sweep runs unless an
/// operator turns it off. Flipped by Todd's 2026-09-10 defaults review.
///
/// Nothing about the default weakens what executes: the sweep still takes the
/// create-only claim per task, and execution still refuses any task whose
/// recorded `table_uuid` is not the table it is about to rewrite, including the
/// pre-#2837 records that carry no identity at all.
fn delete_tasks_enabled_from_env() -> bool {
    delete_tasks_enabled_from(std::env::var("SIGLAKE_DELETE_TASKS").ok().as_deref())
}

/// Resolve delete-task execution from the raw environment value.
///
/// Pure so the default and its opt-out are testable without mutating
/// process-global state observed by parallel tests in this binary. An
/// unrecognized value keeps the default rather than silently disabling
/// deletions — a typo must not strand an accepted GDPR request.
fn delete_tasks_enabled_from(raw: Option<&str>) -> bool {
    !matches!(
        raw.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("0" | "off" | "false" | "no")
    )
}

/// Resolve whether mirrored segments are consumed by a remote catalog-claim
/// drain. Unknown or malformed values refuse `commit=force` conservatively.
fn remote_wal_drain_from(raw: Option<&str>) -> bool {
    !matches!(
        raw.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("0" | "off" | "false" | "no")
    )
}

/// Resolve where the ingester takes the tenant from — **single-tenant by
/// default since 2026-09-11**; `--trust-scope-header` (or
/// `SIGLAKE_TRUST_SCOPE_HEADER=1`) opts into routing on the header.
///
/// `X-Scope-OrgID` was trusted in every configuration without
/// `--oidc-tenant-claim`, and it picks both the WAL subtree a request lands in
/// and the Iceberg namespace the drain commits it to — so the shipped default
/// let any caller write as, and then be read as, any tenant. Flipped by Todd's
/// 2026-09-10 defaults review.
///
/// The polarity is the opposite of [`delete_tasks_enabled_from`]'s on purpose:
/// there an unrecognized value keeps the feature ON because stranding a GDPR
/// deletion is the worse failure; here an unrecognized value keeps the header
/// UNTRUSTED, because a typo that quietly opens cross-tenant writes is.
///
/// Pure so the default and its opt-in are testable without mutating
/// process-global state observed by parallel tests in this binary.
fn tenant_routing_from(raw: Option<&str>) -> TenantRouting {
    match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        Some("1" | "true" | "yes" | "on") => TenantRouting::TrustHeader,
        _ => TenantRouting::SingleTenant,
    }
}

/// The JWT claim the ingester binds tenancy to, or `None` when the option is
/// unset or carries nothing.
///
/// Empty is not a claim: `SIGLAKE_OIDC_TENANT_CLAIM=` is how an `extraEnv`
/// says "off", and the verifier has always read it that way. Trimmed, because
/// the value arrives from a ConfigMap as often as from a flag. Resolving it
/// once is also what keeps the trusted-header warning honest — an empty value
/// used to suppress it while binding nothing.
fn oidc_tenant_claim_from(raw: Option<&str>) -> Option<&str> {
    raw.map(str::trim).filter(|claim| !claim.is_empty())
}

/// Whether the ingester will accept requests without authenticating them.
///
/// Static token input is resolved through the same parser installed in
/// [`siglake_ingest::AppState`], so blank and comma-only CSV values do not
/// suppress the open-ingest warning. A complete OIDC pair installs a verifier
/// later in startup and therefore closes ingest even when no static tokens are
/// configured. Half-set OIDC pairs are refused by
/// [`ingest_oidc_config_error`] before this predicate is reached.
fn ingest_auth_open_from(
    tokens: Option<&str>,
    oidc_issuer: Option<&str>,
    oidc_audience: Option<&str>,
) -> bool {
    let has_static_tokens = tokens
        .map(siglake_ingest::AuthTokens::from_csv)
        .is_some_and(|tokens| !tokens.is_empty());
    let has_oidc = matches!((oidc_issuer, oidc_audience), (Some(_), Some(_)));

    !has_static_tokens && !has_oidc
}

/// Why the ingester refuses to start with this OIDC configuration, if it does.
///
/// `--oidc-tenant-claim` says the tenant comes from a VERIFIED token. Without
/// an issuer and an audience there is no verifier and so no token to take it
/// from: the option was accepted and ignored, leaving ingest on static-token
/// or open auth and — under `--trust-scope-header` — leaving the client's
/// header to pick the tenant, the configuration the claim was set to replace.
/// The query server already refuses the same combination, so both boundaries
/// fail at startup rather than each promise a binding neither enforces.
///
/// Pure, and called before the first startup side effect, so the refusal is
/// testable across the matrix (claim alone, claim with static tokens, claim
/// with a trusted header, a complete configuration, no configuration) without
/// an identity provider or a mutated environment.
fn ingest_oidc_config_error(
    oidc_issuer: Option<&str>,
    oidc_audience: Option<&str>,
    oidc_tenant_claim: Option<&str>,
) -> Option<&'static str> {
    match (oidc_issuer, oidc_audience) {
        (Some(_), Some(_)) => None,
        (Some(_), None) | (None, Some(_)) => {
            Some("--oidc-issuer and --oidc-audience must both be set (or neither)")
        }
        (None, None) if oidc_tenant_claim_from(oidc_tenant_claim).is_some() => Some(
            "--oidc-tenant-claim requires --oidc-issuer and --oidc-audience: the tenant comes \
             from a verified token, and with no OIDC verifier there is no token to take it \
             from. Configure both, or unset the claim",
        ),
        (None, None) => None,
    }
}

/// Prefix the WAL mirror writes under when nothing names one. The same string
/// the chart, the operator and the compactor's `--mirror-prefix` default use,
/// because the writer and the reader have to agree on it.
const DEFAULT_WAL_MIRROR_PREFIX: &str = "wal-mirror";

/// Resolve the ingester's WAL-mirror prefix — **default ON since 2026-09-11**,
/// wherever a warehouse URL says there is an object store to mirror to.
///
/// Mirroring was opt-in, so the shipped default lost acknowledged data on a lost
/// WAL volume: a segment is durable in the WAL and not in Iceberg until the
/// compactor commits it, and without the mirror the WAL volume was the only
/// copy. Flipped by Todd's 2026-09-10 defaults review.
///
/// The rules, in the order they apply:
/// - An explicit prefix wins, and still fails fast if no warehouse URL is set.
/// - An explicit EMPTY prefix (`--wal-mirror-prefix ''`, or
///   `SIGLAKE_WAL_MIRROR_PREFIX=` in the environment, which is what the chart
///   renders for `wal.mirror.enabled: false`) is the opt-out.
/// - Unset with a warehouse URL is [`DEFAULT_WAL_MIRROR_PREFIX`].
/// - Unset with no warehouse URL is off: the local-directory warehouse of
///   `siglake ingest-server` with no `--warehouse-url` is the same disk the WAL
///   is on, so "mirroring" there would copy bytes onto the volume whose loss it
///   is supposed to survive, and nothing would reveal that it protects nothing.
///
/// Pure so the default and its opt-out are testable without mutating
/// process-global state observed by parallel tests in this binary.
fn wal_mirror_prefix_from(raw: Option<&str>, warehouse_url: Option<&str>) -> Option<String> {
    match raw.map(str::trim) {
        Some("") => None,
        Some(prefix) => Some(prefix.to_string()),
        None => warehouse_url.map(|_| DEFAULT_WAL_MIRROR_PREFIX.to_string()),
    }
}

/// Default re-clustering cadence, seconds. The compactor heals the layout every
/// N idle seconds; the pass itself is bounded, and the run loop drains
/// back-to-back while a fragmented layout is consolidating, so this is a floor
/// on how often it *starts*, not on how much it does.
const DEFAULT_RECLUSTER_INTERVAL_SECS: u64 = 15;

/// Tier-2 re-clustering for the compactor daemon — **default ON since
/// 2026-08-06**; `SIGLAKE_RECLUSTER_INTERVAL_SECS=0` disables it.
///
/// This was opt-in and OFF, and neither the Helm chart nor the operator set the
/// variable — only the bench harness did. So a default install drained WAL into
/// Iceberg and then **never compacted**: small files accumulated without bound,
/// overlap depth grew, ordered scans lost their early-stop, and every
/// compaction feature in the codebase (leveled passes, the depth trigger, bin
/// concurrency, the multi-level planner) was unreachable. Nothing surfaced it
/// because every benchmark round set the variable explicitly.
///
/// A storage engine that does not compact by default is not a defensible
/// default; the compaction work is only worth anything if it runs.
/// Established by the 2026-08-06 defaults review (F0).
fn recluster_cfg_from_env() -> Option<siglake_compactor::ReclusterConfig> {
    let secs: u64 = std::env::var("SIGLAKE_RECLUSTER_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_RECLUSTER_INTERVAL_SECS);
    if secs == 0 {
        return None;
    }
    let mut cfg = siglake_compactor::ReclusterConfig::new(std::time::Duration::from_secs(secs));
    // Per-pod tuning of the pass bounds. The defaults are conservative; pods with
    // more memory can raise the row / byte caps to converge in fewer passes.
    let env_u64 = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<u64>().ok());
    if let Some(v) = env_u64("SIGLAKE_RECLUSTER_MAX_PASS_ROWS") {
        cfg.policy.max_pass_rows = v;
    }
    if let Some(v) = env_u64("SIGLAKE_RECLUSTER_MAX_PASS_MB") {
        cfg.policy.max_pass_bytes = v.saturating_mul(1024 * 1024);
    }
    if let Some(v) = env_u64("SIGLAKE_RECLUSTER_MAX_FILES") {
        cfg.policy.max_files_per_pass = v as usize;
    }
    if let Some(v) = env_u64("SIGLAKE_RECLUSTER_TARGET_MB") {
        cfg.policy.target_file_bytes = v.saturating_mul(1024 * 1024);
    }
    if let Some(v) = env_u64("SIGLAKE_RECLUSTER_COLD_TARGET_MB") {
        // Cold-partition output target (capped at max_pass_bytes until the
        // streaming merge lands). Raise it together with the pass byte cap.
        cfg.policy.cold_target_file_bytes = v.saturating_mul(1024 * 1024);
    }
    if let Some(v) = env_u64("SIGLAKE_RECLUSTER_COLD_AGE_SECS") {
        cfg.policy.cold_age_secs = v;
    }
    if let Some(v) = env_u64("SIGLAKE_RECLUSTER_MAX_BINS") {
        // Output bins (independent merges) per partition per pass. Higher values
        // heal a fragmented partition in fewer passes at the cost of more work
        // per pass; each bin is still memory-bounded by the pass row/byte caps.
        cfg.policy.max_bins_per_pass = (v as usize).max(1);
    }
    if let Some(v) = env_u64("SIGLAKE_RECLUSTER_MAX_WINDOW_SECS") {
        // Optional window sealing (off by default): bins seal at time gaps once
        // their span exceeds this width, so merged files tend toward
        // window-bounded ranges (finer retention granularity). Best-effort —
        // disjointness always wins over width.
        if v > 0 {
            cfg.policy.max_window_ns = Some((v as i64).saturating_mul(1_000_000_000));
        }
    }
    // A.4.2: LSM-style leveled compaction — **default ON since 2026-08-06**.
    //
    // The bounded per-level pass keeps compaction seconds–minutes and keeps up
    // under sustained writes; the flat whole-partition alternative forms the
    // 1–2 h giant merge this was built to replace. It is also the ONLY mode the
    // benchmark arc ever measured — every AWS round set
    // `SIGLAKE_COMPACTOR_LEVELED=1`, so the multi-level-per-pass planner, bin
    // concurrency and the depth trigger are all leveled-path features. Shipping
    // flat as the default meant shipping the slower mode AND the unmeasured one
    // (2026-08-06 defaults review, F1).
    //
    // The flat path is retained behind `SIGLAKE_COMPACTOR_LEVELED=0` because it
    // has no depth trigger and no per-level cadence — strictly simpler, and the
    // right escape hatch if leveled ever misbehaves on a shape we have not seen.
    //
    // The level ladder, fan-in, and per-level file trigger are individually
    // tunable; unset fields keep `LevelPolicy`'s defaults (128 MiB / 1 GiB /
    // 8 GiB ceilings, trigger 8, fan-in 64).
    if compactor_leveled_enabled() {
        let mut levels = siglake_storage::iceberg::LevelPolicy::default();
        // Comma-separated ascending MiB ceilings, e.g. "128,1024,8192".
        if let Ok(csv) = std::env::var("SIGLAKE_COMPACTOR_LEVEL_CEILINGS_MB") {
            let parsed: Vec<u64> = csv
                .split(',')
                .filter_map(|s| s.trim().parse::<u64>().ok())
                .map(|mb| mb.saturating_mul(1024 * 1024))
                .collect();
            if !parsed.is_empty() {
                levels.level_ceilings = parsed;
            }
        }
        if let Some(v) = env_u64("SIGLAKE_COMPACTOR_LEVEL_TRIGGER_FILES") {
            levels.trigger_files = (v as usize).max(2);
        }
        if let Some(v) = env_u64("SIGLAKE_COMPACTOR_LEVEL_MAX_FANIN") {
            levels.max_fanin = (v as usize).max(2);
        }
        if let Some(v) = env_u64("SIGLAKE_COMPACTOR_LEVEL_MAX_GEN") {
            // Write-amplification bound: a file rewritten this many times is
            // mature and exits compaction (unless it still overlaps a neighbor —
            // disjointness always wins). 0 disables the cap.
            levels.max_merge_gen = v as u32;
        }
        if let Some(v) = env_u64("SIGLAKE_COMPACTOR_MAX_OVERLAP_DEPTH") {
            // Depth trigger: merge the deepest overlap stack once a partition's
            // time-overlap depth exceeds this, independent of level counts —
            // keeps the layout's ordered-scan merge fan-in at/under the query
            // side's default caps. 0 disables (default 12).
            levels.max_overlap_depth = v as usize;
        }
        // Per-level pass cadence, comma-separated seconds (e.g. "10,60,3600" for
        // L0/L1/L2+; levels beyond the list inherit the last entry). The leading
        // edge rescans fast; cold levels rescan rarely (each pass re-lists the
        // table's live files, so needless cold scans are pure catalog load).
        if let Ok(csv) = std::env::var("SIGLAKE_COMPACTOR_LEVEL_INTERVALS_SECS") {
            let parsed: Vec<std::time::Duration> = csv
                .split(',')
                .filter_map(|s| s.trim().parse::<u64>().ok())
                .map(std::time::Duration::from_secs)
                .collect();
            if !parsed.is_empty() {
                cfg.level_intervals = parsed;
            }
        }
        cfg.levels = Some(levels);
    }
    Some(cfg)
}

/// Whether the compactor runs LSM-style leveled compaction. Default ON;
/// `SIGLAKE_COMPACTOR_LEVELED=0` (or `off`/`false`) selects the legacy flat
/// whole-partition pass. Pure so the default is unit-testable.
fn compactor_leveled_from_env(raw: Option<&str>) -> bool {
    !matches!(raw.map(str::trim), Some("0" | "off" | "false"))
}

fn compactor_leveled_enabled() -> bool {
    compactor_leveled_from_env(std::env::var("SIGLAKE_COMPACTOR_LEVELED").ok().as_deref())
}

/// Commit-accumulation batching (BIG-4 `#4b`), opt-in via
/// `SIGLAKE_COMMIT_BATCH_TARGET_MB`. When set (> 0) the compactor defers
/// a commit until the sealed queue reaches the byte target **or** its
/// oldest segment reaches `SIGLAKE_COMMIT_BATCH_MAX_AGE_SECS` (default
/// 10s, the freshness floor). `0` / unset ⇒ legacy commit-every-cycle.
/// Commit-accumulation batching policy (BIG-4 `#4b`). **Default-on** since
/// the 2026-06-02 round-60 soak (1h @ 10k EPS: ~124k rows/commit,
/// exact row conservation, no backlog drift).
/// Defers a commit until the sealed queue reaches
/// `SIGLAKE_COMMIT_BATCH_TARGET_MB` (default 32) *or* its oldest segment
/// hits `SIGLAKE_COMMIT_BATCH_MAX_AGE_SECS` (default 10) — so ingest-to-
/// queryable latency lags by up to `max_age`. Set `*_TARGET_MB=0` to opt
/// out to the legacy commit-per-segment path.
fn commit_batch_cfg_from_env() -> Option<siglake_compactor::CommitBatchPolicy> {
    commit_batch_cfg_from(
        std::env::var("SIGLAKE_COMMIT_BATCH_TARGET_MB")
            .ok()
            .as_deref(),
        std::env::var("SIGLAKE_COMMIT_BATCH_MAX_AGE_SECS")
            .ok()
            .as_deref(),
    )
}

/// Resolve commit batching from the two raw environment values.
///
/// Pure so policy behavior can be tested without mutating process-global state
/// observed by parallel tests in this binary.
fn commit_batch_cfg_from(
    target_mb: Option<&str>,
    max_age_secs: Option<&str>,
) -> Option<siglake_compactor::CommitBatchPolicy> {
    const DEFAULT_TARGET_MB: u64 = 32;
    const DEFAULT_MAX_AGE_SECS: u64 = 10;
    let target_mb: u64 = target_mb
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_TARGET_MB);
    // Explicit 0 opts out to the legacy commit-per-segment path.
    if target_mb == 0 {
        return None;
    }
    let max_age_secs: u64 = max_age_secs
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_AGE_SECS);
    Some(siglake_compactor::CommitBatchPolicy {
        target_bytes: target_mb.saturating_mul(1024 * 1024),
        max_age: std::time::Duration::from_secs(max_age_secs.max(1)),
    })
}

/// Snapshot-metadata expiry (BIG-4 `#4c`). **Default-on** since the round-60
/// soak (expiry kept the snapshots array bounded across 1h of sustained
/// commits). On a `SIGLAKE_SNAPSHOT_EXPIRE_INTERVAL_SECS` cadence (default
/// 60) the compactor drops all but the most-recent
/// `SIGLAKE_SNAPSHOT_RETAIN_LAST` (default 100) events-table snapshots,
/// always retaining the current snapshot + ref targets. Non-destructive
/// (metadata only — expired-snapshot data/manifest files remain as orphans
/// until the BIG-3 sweep); the tradeoff is losing time-travel beyond the
/// retained N. Set `*_INTERVAL_SECS=0` to opt out (snapshots never trimmed).
fn expire_cfg_from_env() -> Option<siglake_compactor::ExpireConfig> {
    expire_cfg_from(
        std::env::var("SIGLAKE_SNAPSHOT_EXPIRE_INTERVAL_SECS")
            .ok()
            .as_deref(),
        std::env::var("SIGLAKE_SNAPSHOT_RETAIN_LAST")
            .ok()
            .as_deref(),
    )
}

/// Resolve snapshot expiry from the two raw environment values.
///
/// Pure so policy behavior can be tested without mutating process-global state
/// observed by parallel tests in this binary.
fn expire_cfg_from(
    interval_secs: Option<&str>,
    retain_last: Option<&str>,
) -> Option<siglake_compactor::ExpireConfig> {
    const DEFAULT_INTERVAL_SECS: u64 = 60;
    const DEFAULT_RETAIN_LAST: usize = 100;
    let secs: u64 = interval_secs
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_INTERVAL_SECS);
    // Explicit 0 opts out — snapshots are never trimmed.
    if secs == 0 {
        return None;
    }
    let retain_last: usize = retain_last
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_RETAIN_LAST);
    Some(siglake_compactor::ExpireConfig::new(
        std::time::Duration::from_secs(secs),
        retain_last,
    ))
}

#[cfg(test)]
mod default_policy_tests {
    use super::*;

    /// Leveled compaction is default-ON since 2026-08-06. Before that the
    /// shipped default was the flat whole-partition pass — the slower mode by
    /// the flag's own documentation, and the one no benchmark ever exercised
    /// (every AWS round set `=1`). This pins the default and its opt-out.
    #[test]
    fn leveled_compaction_defaults_on() {
        // Unset ⇒ on.
        assert!(compactor_leveled_from_env(None));
        // Historical explicit opt-in still works.
        assert!(compactor_leveled_from_env(Some("1")));
        assert!(compactor_leveled_from_env(Some("on")));
        // The escape hatch back to the flat pass.
        assert!(!compactor_leveled_from_env(Some("0")));
        assert!(!compactor_leveled_from_env(Some("off")));
        assert!(!compactor_leveled_from_env(Some("false")));
        assert!(!compactor_leveled_from_env(Some(" 0 ")));
        // Anything unrecognized keeps the default rather than silently
        // selecting the legacy path — a typo must not downgrade compaction.
        assert!(compactor_leveled_from_env(Some("yes")));
        assert!(compactor_leveled_from_env(Some("")));
    }

    // BIG-4 #4b/#4c are default-on (round-60 soak). These guard that unset
    // configuration still gets batching + expiry, that explicit values win,
    // and that `0` is the opt-out.

    #[test]
    fn commit_batch_defaults_on_with_opt_out() {
        let cfg = commit_batch_cfg_from(None, None).expect("batching is default-on when env unset");
        assert_eq!(cfg.target_bytes, 32 * 1024 * 1024);
        assert_eq!(cfg.max_age, std::time::Duration::from_secs(10));

        let cfg =
            commit_batch_cfg_from(Some("8"), Some("3")).expect("explicit values enable batching");
        assert_eq!(cfg.target_bytes, 8 * 1024 * 1024);
        assert_eq!(cfg.max_age, std::time::Duration::from_secs(3));

        assert!(
            commit_batch_cfg_from(Some("0"), None).is_none(),
            "TARGET_MB=0 must opt out to commit-per-segment"
        );
    }

    #[test]
    fn snapshot_expire_defaults_on_with_opt_out() {
        let cfg = expire_cfg_from(None, None).expect("expiry is default-on when env unset");
        assert_eq!(cfg.interval, std::time::Duration::from_secs(60));
        assert_eq!(cfg.retain_last, 100);

        let cfg = expire_cfg_from(Some("30"), Some("50")).expect("explicit values enable expiry");
        assert_eq!(cfg.interval, std::time::Duration::from_secs(30));
        assert_eq!(cfg.retain_last, 50);

        assert!(
            expire_cfg_from(Some("0"), None).is_none(),
            "INTERVAL_SECS=0 must opt out (snapshots never trimmed)"
        );
    }

    /// Delete-task execution is default-ON since 2026-09-11. Before that the
    /// shipped default was OFF, so `POST /api/v1/delete-tasks` answered `201`
    /// for a deletion no sweep would ever run. This pins the default and the
    /// opt-out that replaces it.
    #[test]
    fn delete_tasks_default_on_with_opt_out() {
        // Unset ⇒ the sweep runs.
        assert!(delete_tasks_enabled_from(None));
        // The historical explicit opt-in values still mean on.
        assert!(delete_tasks_enabled_from(Some("1")));
        assert!(delete_tasks_enabled_from(Some("true")));
        assert!(delete_tasks_enabled_from(Some("TRUE")));
        assert!(delete_tasks_enabled_from(Some("yes")));
        // The opt-out, in the spellings the other compactor toggles take, in
        // any case, and tolerant of the whitespace a chart value can carry.
        assert!(!delete_tasks_enabled_from(Some("0")));
        assert!(!delete_tasks_enabled_from(Some("off")));
        assert!(!delete_tasks_enabled_from(Some("false")));
        assert!(!delete_tasks_enabled_from(Some("FALSE")));
        assert!(!delete_tasks_enabled_from(Some("no")));
        assert!(!delete_tasks_enabled_from(Some(" 0 ")));
        // Anything unrecognized keeps the default: a typo must not strand an
        // accepted deletion.
        assert!(delete_tasks_enabled_from(Some("maybe")));
        assert!(delete_tasks_enabled_from(Some("")));
    }

    #[test]
    fn remote_wal_drain_defaults_conservative_with_local_opt_out() {
        assert!(remote_wal_drain_from(None));
        assert!(remote_wal_drain_from(Some("1")));
        assert!(remote_wal_drain_from(Some("invalid")));
        assert!(!remote_wal_drain_from(Some("0")));
        assert!(!remote_wal_drain_from(Some(" false ")));
    }

    /// Tenant routing is single-tenant by default since 2026-09-11. Before
    /// that, `X-Scope-OrgID` selected the tenant in every configuration without
    /// `--oidc-tenant-claim` — an unauthenticated client header choosing the
    /// WAL subtree and the Iceberg namespace a write landed in. This pins the
    /// default and the one spelling of the opt-in.
    #[test]
    fn tenant_routing_defaults_to_single_tenant() {
        assert_eq!(tenant_routing_from(None), TenantRouting::SingleTenant);

        // The opt-in, in the spellings a flag, an env var and a chart value
        // arrive in, in any case, tolerant of whitespace.
        for on in ["1", "true", "TRUE", "yes", "on", " true "] {
            assert_eq!(
                tenant_routing_from(Some(on)),
                TenantRouting::TrustHeader,
                "{on:?} must opt into header routing"
            );
        }

        // Everything else stays single-tenant. Unlike the compactor toggles,
        // an unrecognized value does NOT keep some previous behaviour: a typo
        // that quietly opened cross-tenant writes is the worse failure here.
        for off in ["0", "off", "false", "no", "", "  ", "maybe", "tenant"] {
            assert_eq!(
                tenant_routing_from(Some(off)),
                TenantRouting::SingleTenant,
                "{off:?} must not open header routing"
            );
        }
    }

    /// OTLP/gRPC is default-ON on its standard port. The separate disable flag
    /// is required because omitting `--otlp-grpc-listen` no longer turns the
    /// listener off.
    #[test]
    fn otlp_grpc_defaults_on_and_has_an_explicit_opt_out() {
        let default_addr: SocketAddr = DEFAULT_OTLP_GRPC_LISTEN.parse().unwrap();
        let default = Cli::try_parse_from(["siglake", "ingest-server"]).unwrap();
        let Command::IngestServer {
            otlp_grpc_listen,
            disable_otlp_grpc,
            ..
        } = default.command
        else {
            panic!("parsed the wrong command")
        };
        assert_eq!(otlp_grpc_listen, Some(default_addr));
        assert!(!disable_otlp_grpc);
        assert_eq!(
            otlp_grpc_listen_from(otlp_grpc_listen, disable_otlp_grpc),
            Some(default_addr)
        );

        let disabled =
            Cli::try_parse_from(["siglake", "ingest-server", "--disable-otlp-grpc"]).unwrap();
        let Command::IngestServer {
            otlp_grpc_listen,
            disable_otlp_grpc,
            ..
        } = disabled.command
        else {
            panic!("parsed the wrong command")
        };
        assert!(disable_otlp_grpc);
        assert_eq!(
            otlp_grpc_listen_from(otlp_grpc_listen, disable_otlp_grpc),
            None
        );

        let custom = Cli::try_parse_from([
            "siglake",
            "ingest-server",
            "--otlp-grpc-listen",
            "127.0.0.1:14317",
        ])
        .unwrap();
        let Command::IngestServer {
            otlp_grpc_listen, ..
        } = custom.command
        else {
            panic!("parsed the wrong command")
        };
        assert_eq!(otlp_grpc_listen, Some("127.0.0.1:14317".parse().unwrap()));

        assert!(Cli::try_parse_from([
            "siglake",
            "ingest-server",
            "--otlp-grpc-listen",
            "127.0.0.1:14317",
            "--disable-otlp-grpc",
        ])
        .is_err());
    }

    /// WAL mirroring is default-ON since 2026-09-11 wherever a warehouse URL
    /// gives it somewhere to write. Before that it was opt-in, so the shipped
    /// default lost acknowledged data with the WAL volume. This pins the
    /// default, the opt-out that replaces it, and the one case that stays off.
    #[test]
    fn wal_mirror_default_on_with_an_object_store_and_an_empty_opt_out() {
        let s3 = Some("s3://bucket/warehouse");

        // Unset with an object store ⇒ the shared default prefix.
        assert_eq!(
            wal_mirror_prefix_from(None, s3).as_deref(),
            Some(DEFAULT_WAL_MIRROR_PREFIX)
        );
        // Unset with no object store ⇒ off. The local warehouse directory is
        // the disk the WAL is already on; a copy there protects nothing.
        assert_eq!(wal_mirror_prefix_from(None, None), None);

        // The opt-out: an empty value, which is what the chart renders for
        // `wal.mirror.enabled: false` and what an operator puts in extraEnv.
        assert_eq!(wal_mirror_prefix_from(Some(""), s3), None);
        assert_eq!(wal_mirror_prefix_from(Some("   "), s3), None);

        // An explicit prefix wins, trimmed, with or without a warehouse URL —
        // the no-URL case is the startup error, not a silent disable.
        assert_eq!(
            wal_mirror_prefix_from(Some("wal-dr"), s3).as_deref(),
            Some("wal-dr")
        );
        assert_eq!(
            wal_mirror_prefix_from(Some(" wal-dr "), s3).as_deref(),
            Some("wal-dr")
        );
        assert_eq!(
            wal_mirror_prefix_from(Some("wal-dr"), None).as_deref(),
            Some("wal-dr")
        );
    }

    /// The writer and the reader have to name the SAME prefix: the ingester
    /// mirrors to it and a catalog-claim drain reads only from it. Both
    /// defaults come from one constant so they cannot drift. (The chart's
    /// `wal.mirror.prefix` is the third copy; `scripts/check-chart.py` holds
    /// that one, where the rendered manifests are.)
    #[test]
    fn the_mirror_prefix_default_is_one_constant_for_writer_and_reader() {
        use clap::CommandFactory as _;

        let compactor_default = Cli::command()
            .get_subcommands()
            .find(|c| c.get_name() == "compactor")
            .expect("compactor subcommand")
            .get_arguments()
            .find(|a| a.get_id() == "mirror_prefix")
            .expect("--mirror-prefix")
            .get_default_values()
            .to_vec();
        assert_eq!(
            compactor_default,
            vec![std::ffi::OsString::from(DEFAULT_WAL_MIRROR_PREFIX)],
            "the drain must claim from the prefix the ingester writes to"
        );
        assert_eq!(
            wal_mirror_prefix_from(None, Some("s3://bucket/warehouse")).as_deref(),
            Some(DEFAULT_WAL_MIRROR_PREFIX)
        );
    }

    /// The six maintenance subcommands used to write their `--namespace` into
    /// the process environment for `open_iceberg` to read back. The
    /// value now travels as an argument; this pins the precedence that write
    /// used to produce: an explicit namespace wins over the environment, the
    /// environment wins over the storage default, and only both-unset lands
    /// on the default.
    #[test]
    fn tenant_namespace_prefers_explicit_then_env_then_default() {
        let default_ns = siglake_storage::iceberg::NAMESPACE;

        // Long-running subcommands pass None: env-derived, default when unset.
        assert_eq!(tenant_namespace_from(None, None), default_ns);
        assert_eq!(tenant_namespace_from(None, Some("acme")), "acme");

        // Maintenance subcommands pass their flag: it wins even when the env
        // names a different tenant, exactly as the old env write overwrote it.
        assert_eq!(tenant_namespace_from(Some("acme"), None), "acme");
        assert_eq!(tenant_namespace_from(Some("acme"), Some("other")), "acme");

        // Passing the default explicitly still selects the pure local-path
        // branch in open_iceberg, which compares against the same constant.
        assert_eq!(
            tenant_namespace_from(Some(default_ns), Some("other")),
            default_ns
        );
    }
}

/// `--oidc-tenant-claim` says tenancy comes from a verified token, and the
/// ingester could not keep that promise without a verifier: the option was
/// parsed, passed in and dropped, leaving static-token or open auth — and,
/// under `--trust-scope-header`, the client's header still choosing the
/// tenant. The query server has refused the combination all along; these pin
/// the ingester's matching refusal, through the parser and the pure validator
/// so no identity provider and no environment write is involved.
#[cfg(test)]
mod ingest_oidc_prerequisite_tests {
    use super::*;

    /// What the startup check reads out of a parsed `ingest-server`
    /// invocation: the three OIDC options and the resolved tenant routing.
    struct Parsed {
        issuer: Option<String>,
        audience: Option<String>,
        claim: Option<String>,
        routing: TenantRouting,
    }

    fn parse(args: &[&str]) -> Parsed {
        let cli = Cli::try_parse_from(
            ["siglake", "ingest-server"]
                .into_iter()
                .chain(args.iter().copied()),
        )
        .expect("ingest-server arguments parse");
        let Command::IngestServer {
            oidc_issuer,
            oidc_audience,
            oidc_tenant_claim,
            trust_scope_header,
            ..
        } = cli.command
        else {
            panic!("parsed the wrong command")
        };
        Parsed {
            routing: tenant_routing_from(trust_scope_header.as_deref()),
            issuer: oidc_issuer,
            audience: oidc_audience,
            claim: oidc_tenant_claim,
        }
    }

    fn refusal(args: &[&str]) -> &'static str {
        let p = parse(args);
        ingest_oidc_config_error(
            p.issuer.as_deref(),
            p.audience.as_deref(),
            p.claim.as_deref(),
        )
        .unwrap_or_else(|| panic!("{args:?} must be refused before ingest starts"))
    }

    fn accepted(args: &[&str]) {
        let p = parse(args);
        assert_eq!(
            ingest_oidc_config_error(
                p.issuer.as_deref(),
                p.audience.as_deref(),
                p.claim.as_deref()
            ),
            None,
            "{args:?} must still start"
        );
    }

    const IDP: &str = "https://idp.example.com";

    #[test]
    fn a_tenant_claim_without_a_verifier_is_refused() {
        // Claim alone: auth is open. Nothing verifies the caller, so nothing
        // can carry the claim the tenant was supposed to come from.
        assert!(refusal(&["--oidc-tenant-claim", "org"]).contains("requires --oidc-issuer"));

        // Claim with static tokens. The tokens say who may write, never as
        // whom, so they do not stand in for the verifier.
        assert!(
            refusal(&["--oidc-tenant-claim", "org", "--auth-tokens", "t1,t2"])
                .contains("requires --oidc-issuer")
        );

        // Claim with header routing: the configuration reads as "tenancy is
        // bound to a verified identity" while the client's header picks the
        // tenant, which is the case this refusal exists for.
        let p = parse(&["--oidc-tenant-claim", "org", "--trust-scope-header", "1"]);
        assert_eq!(p.routing, TenantRouting::TrustHeader);
        assert!(ingest_oidc_config_error(None, None, p.claim.as_deref()).is_some());
    }

    #[test]
    fn a_complete_or_an_absent_oidc_configuration_still_starts() {
        // Nothing configured at all — the local single-node default.
        accepted(&[]);
        accepted(&["--auth-tokens", "t1"]);
        // Header routing with no claim keeps working: it is a documented
        // (and warned-about) configuration, not a contradiction.
        accepted(&["--trust-scope-header", "1"]);
        // OIDC without a claim: verified callers, single-tenant routing.
        accepted(&["--oidc-issuer", IDP, "--oidc-audience", "siglake"]);
        // The configuration the option is for.
        accepted(&[
            "--oidc-issuer",
            IDP,
            "--oidc-audience",
            "siglake",
            "--oidc-tenant-claim",
            "org",
        ]);
    }

    #[test]
    fn half_an_issuer_audience_pair_keeps_its_own_refusal() {
        assert!(refusal(&["--oidc-issuer", IDP]).contains("must both be set"));
        assert!(refusal(&["--oidc-audience", "siglake"]).contains("must both be set"));
        // With a claim as well the pair error still wins: it names the option
        // actually missing, and it now fires before the metrics listener and
        // the WAL directory rather than after them.
        assert!(
            refusal(&["--oidc-issuer", IDP, "--oidc-tenant-claim", "org"])
                .contains("must both be set")
        );
    }

    #[test]
    fn an_empty_claim_is_off_rather_than_a_refusal() {
        assert_eq!(oidc_tenant_claim_from(None), None);
        assert_eq!(oidc_tenant_claim_from(Some("")), None);
        assert_eq!(oidc_tenant_claim_from(Some("   ")), None);
        assert_eq!(oidc_tenant_claim_from(Some(" org ")), Some("org"));

        // `SIGLAKE_OIDC_TENANT_CLAIM=` is how an extraEnv says "off", so it
        // starts...
        assert_eq!(ingest_oidc_config_error(None, None, Some("")), None);
        assert_eq!(ingest_oidc_config_error(None, None, Some("  ")), None);
        // ...and, because the resolved claim is what the startup path now
        // reads, it no longer silences the trusted-header warning either: the
        // warning fires when nothing binds the tenant.
        assert!(oidc_tenant_claim_from(Some("")).is_none());
    }
}

#[cfg(test)]
mod ingest_auth_open_tests {
    use super::ingest_auth_open_from;

    const IDP: &str = "https://idp.example.com";
    const AUDIENCE: &str = "siglake";

    #[test]
    fn no_auth_configuration_is_open() {
        assert!(ingest_auth_open_from(None, None, None));
        assert!(ingest_auth_open_from(Some(""), None, None));
        assert!(ingest_auth_open_from(Some("   "), None, None));
        assert!(ingest_auth_open_from(Some(" , , "), None, None));
    }

    #[test]
    fn static_tokens_close_ingest() {
        assert!(!ingest_auth_open_from(Some("token"), None, None));
        assert!(!ingest_auth_open_from(Some(" , token, "), None, None));
    }

    #[test]
    fn oidc_closes_ingest_without_static_tokens() {
        assert!(!ingest_auth_open_from(None, Some(IDP), Some(AUDIENCE)));
        assert!(!ingest_auth_open_from(
            Some(" , "),
            Some(IDP),
            Some(AUDIENCE)
        ));
    }

    #[test]
    fn combined_static_and_oidc_auth_closes_ingest() {
        assert!(!ingest_auth_open_from(
            Some("token"),
            Some(IDP),
            Some(AUDIENCE)
        ));
    }
}

/// The default-configuration delete path, end to end through the compactor the
/// daemon builds: submit a task the way `POST /api/v1/delete-tasks` does, run
/// the sweep with `SIGLAKE_DELETE_TASKS` unset, and read the rows.
///
/// The unit test above pins the resolver; these pin what the resolver's value
/// does when it reaches a real sweep — including the two safeguards the flip
/// must not weaken (#2837): the incarnation binding taken at submission, and
/// the refusal of a record that carries no identity.
#[cfg(test)]
mod delete_task_default_execution_tests {
    use super::*;

    use siglake_core::index_config::IndexConfig;
    use siglake_storage::iceberg::DeleteTaskState;

    fn event_at(host: &str, raw: &str) -> Event {
        Event {
            timestamp: chrono::Utc::now(),
            host: host.to_string(),
            source: "/var/log/app.log".to_string(),
            sourcetype: "app:json".to_string(),
            index: "main".to_string(),
            raw: raw.to_string(),
            attributes: None,
        }
    }

    /// A warehouse holding one managed index with a `victim` row and a `keep`
    /// row, plus the WAL root the compactor scans for tenant namespaces.
    async fn index_with_two_rows() -> (tempfile::TempDir, std::path::PathBuf, Arc<IcebergContext>) {
        let tmp = tempfile::tempdir().unwrap();
        let wal = tmp.path().join("wal");
        std::fs::create_dir_all(&wal).unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        let mut config = IndexConfig::builtin_events();
        config.index_id = "logs".to_string();
        ice.create_index(&config).await.unwrap();
        let batch = events_to_record_batch(&[event_at("victim", "gdpr"), event_at("keep", "kept")])
            .unwrap();
        ice.append_to_table(&ice.index_table_ident("logs"), batch, &[])
            .await
            .unwrap();
        (tmp, wal, ice)
    }

    async fn row_count(ice: &IcebergContext, where_sql: &str) -> i64 {
        let ctx = SessionContext::new();
        ice.register_table_with_datafusion(&ctx, &ice.index_table_ident("logs"), "logs")
            .await
            .unwrap();
        let batches = ctx
            .sql(&format!(
                "SELECT count(*) AS n FROM \"logs\" WHERE {where_sql}"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0)
    }

    /// The acceptance case for the flip: an accepted deletion executes on a
    /// compactor configured exactly as an unset environment configures it.
    #[tokio::test]
    async fn a_submitted_task_executes_under_the_default_configuration() {
        let (_tmp, wal, ice) = index_with_two_rows().await;
        let task = ice
            .create_delete_task("logs", "host = 'victim'", None, None)
            .await
            .unwrap();

        let compactor =
            Compactor::new(&wal, ice.clone()).with_delete_tasks(delete_tasks_enabled_from(None));
        assert_eq!(
            compactor.run_delete_tasks_once().await.unwrap(),
            1,
            "an unset SIGLAKE_DELETE_TASKS must execute the pending task"
        );

        let executed = ice.get_delete_task(task.task_id).await.unwrap().unwrap();
        assert_eq!(executed.state, DeleteTaskState::Done, "{executed:?}");
        assert_eq!(executed.rows_deleted, 1, "{executed:?}");
        assert_eq!(row_count(&ice, "host = 'victim'").await, 0);
        assert_eq!(
            row_count(&ice, "host = 'keep'").await,
            1,
            "the rewrite must keep everything the predicate is not TRUE for"
        );
    }

    /// The opt-out has to be real: with it set, the task stays `pending` and
    /// the rows stay.
    #[tokio::test]
    async fn the_opt_out_leaves_the_task_pending_and_the_rows_intact() {
        let (_tmp, wal, ice) = index_with_two_rows().await;
        let task = ice
            .create_delete_task("logs", "host = 'victim'", None, None)
            .await
            .unwrap();

        let compactor = Compactor::new(&wal, ice.clone())
            .with_delete_tasks(delete_tasks_enabled_from(Some("0")));
        assert_eq!(compactor.run_delete_tasks_once().await.unwrap(), 0);

        let untouched = ice.get_delete_task(task.task_id).await.unwrap().unwrap();
        assert_eq!(untouched.state, DeleteTaskState::Pending, "{untouched:?}");
        assert_eq!(row_count(&ice, "host = 'victim'").await, 1);
    }

    /// #2837's refusal survives the flip. A record with no `table_uuid` — every
    /// task written before the binding — is still refused rather than resolved
    /// through its index NAME, which is exactly what proves nothing there.
    #[tokio::test]
    async fn the_default_still_refuses_a_task_with_no_recorded_incarnation() {
        let (_tmp, wal, ice) = index_with_two_rows().await;
        let mut task = ice
            .create_delete_task("logs", "host = 'victim'", None, None)
            .await
            .unwrap();
        assert!(
            task.table_uuid.is_some(),
            "submission must record the incarnation it validated"
        );
        task.table_uuid = None;
        ice.write_delete_task_record_for_test(&task).await.unwrap();

        let compactor =
            Compactor::new(&wal, ice.clone()).with_delete_tasks(delete_tasks_enabled_from(None));
        assert_eq!(
            compactor.run_delete_tasks_once().await.unwrap(),
            0,
            "an unidentified legacy record must not count as executed"
        );

        let refused = ice.get_delete_task(task.task_id).await.unwrap().unwrap();
        assert_eq!(refused.state, DeleteTaskState::Failed, "{refused:?}");
        assert!(
            refused
                .error
                .clone()
                .unwrap_or_default()
                .contains("resubmit"),
            "the refusal must name the recovery: {refused:?}"
        );
        assert_eq!(
            row_count(&ice, "host = 'victim'").await,
            1,
            "a refused task must rewrite nothing"
        );
    }
}

#[cfg(test)]
mod registrar_retry_tests {
    use super::*;

    #[test]
    fn jitter_is_bounded_and_decorrelated() {
        // Bounded: jitter must stay small next to the backoff it perturbs.
        for id in ["seg-a", "seg-b", "seg-0000000001"] {
            for attempt in 1..=6 {
                assert!(register_retry_jitter(id, attempt) < Duration::from_millis(250));
            }
        }
        // Decorrelated: two ingesters that failed at the same instant must not
        // pick the same delay, which is the stampede this exists to prevent.
        let a: Vec<_> = (1..=6).map(|n| register_retry_jitter("seg-a", n)).collect();
        let b: Vec<_> = (1..=6).map(|n| register_retry_jitter("seg-b", n)).collect();
        assert_ne!(a, b, "different segments must not share a retry schedule");
    }

    #[test]
    fn jitter_is_deterministic_per_segment() {
        // Derived from the id, not a clock: the same segment retried on the same
        // attempt must be reproducible, so a stuck schedule is debuggable.
        assert_eq!(
            register_retry_jitter("seg-x", 3),
            register_retry_jitter("seg-x", 3)
        );
        assert_ne!(
            register_retry_jitter("seg-x", 3),
            register_retry_jitter("seg-x", 4),
            "successive attempts must not reuse one delay"
        );
    }
}

/// `siglake rebuild-group-counts` — the operator fallback when the automatic
/// repair for a lost group-count delta fails or remains incomplete.
///
/// Prints per column what it recovered and whether that column will now serve
/// Tier-1 again, because "the command exited 0" is not the same as "the fast
/// path is back": a column can be legitimately short (added mid-life, so older
/// files carry no footer for it) and a rebuild cannot invent history it never
/// had. Saying so beats implying a repair that did not happen.
///
/// Each line also says whether the column was REPAIRED (the aggregate already
/// carried it) or ADMITTED (`--admit-typed-columns` added it), and the closing
/// summary separates the case a flag fixes — a typed column in every file but
/// never in the aggregate — from the case only a rewrite fixes, a column some
/// live file cannot serve at all. The old message called both "only a table
/// rewrite recovers those", which sent operators to rewrite tables the tool
/// could have repaired.
async fn run_rebuild_group_counts(
    data_dir: &std::path::Path,
    warehouse_url: Option<&str>,
    catalog_uri: Option<&str>,
    namespace: &str,
    table: &str,
    admit_typed_columns: bool,
) -> Result<()> {
    let ice = open_iceberg(
        data_dir,
        "warehouse",
        warehouse_url,
        catalog_uri,
        Some(namespace),
    )
    .await?;
    let options = GroupCountRebuildOptions {
        admit_typed_columns,
    };
    let report = ice
        .rebuild_group_count_aggregate_with(table, options)
        .await?;

    // Typed columns the schema has and the aggregate does not — with the flag
    // off, this is the remedy; with it on, they are in the report as admitted.
    let admissible_hint = || {
        if !admit_typed_columns && !report.admissible_typed_columns.is_empty() {
            println!(
                "\nnot in the aggregate: {}. The schema carries these typed columns but the \
                 aggregate never did, so GROUP BY on them is served from the per-file path \
                 (`materialized`). No rewrite is needed while every live file carries them: \
                 re-run with --admit-typed-columns to add them.",
                report.admissible_typed_columns.join(", ")
            );
        }
    };

    if report.skipped_no_columns {
        println!(
            "{namespace}.{table}: the group-count aggregate holds no columns; nothing to rebuild"
        );
        admissible_hint();
        return Ok(());
    }
    println!(
        "{namespace}.{table}: rebuilt through sequence {} (table rows: {})",
        report.sequence_number,
        report
            .record_count
            .map(|n| n.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    let mut unreadable = Vec::new();
    let mut over_cap = Vec::new();
    let mut short = Vec::new();
    for c in &report.columns {
        let origin = if c.admitted { "admitted" } else { "repaired" };
        match (c.rows, c.over_cap) {
            (Some(rows), Some(cap)) => {
                println!(
                    "  {:<24} {origin:<9} rows={:<14} distinct={:<10} NOT written: {} distinct \
                     exceeds the typed cardinality cap ({cap})",
                    c.column, rows, c.distinct, c.distinct
                );
                over_cap.push(format!("{} ({} distinct)", c.column, c.distinct));
            }
            (Some(rows), None) => {
                println!(
                    "  {:<24} {origin:<9} rows={:<14} distinct={:<10} {}",
                    c.column,
                    rows,
                    c.distinct,
                    match (c.covers_table, c.admitted) {
                        (true, true) => "Tier-1 enabled for a column the aggregate never carried",
                        (true, false) => "Tier-1 restored",
                        (false, _) => "still short of the table row count",
                    }
                );
                if !c.covers_table {
                    short.push(c.column.clone());
                }
            }
            (None, _) => {
                println!(
                    "  {:<24} {origin:<9} NOT READABLE from any tier — left absent rather than \
                     written wrong",
                    c.column
                );
                unreadable.push(c.column.clone());
            }
        }
    }
    if !unreadable.is_empty() {
        println!(
            "\nnot readable: {}. Some live file can serve the column from neither its \
             group-count footer nor a raw-page decode — the column is missing from that file's \
             schema, or the file predates typed footers. No flag recovers that; only rewriting \
             those files does (compaction rewrites them with footers for the current column set).",
            unreadable.join(", ")
        );
    }
    if !over_cap.is_empty() {
        println!(
            "\nover the typed cap: {}. Counted exactly but not written. Raise \
             SIGLAKE_TYPED_GROUP_COUNT_CARDINALITY above the distinct count and re-run to admit \
             them — a measurement column is usually better left out.",
            over_cap.join(", ")
        );
    }
    if !short.is_empty() {
        println!(
            "\nstill short: {}. Every live file was read and the total still does not match the \
             table row count, so the files themselves disagree with the snapshot summary (a \
             footer over-claims, or the row count is unknown); a rewrite of those files is the \
             only recovery.",
            short.join(", ")
        );
    }
    admissible_hint();
    Ok(())
}

/// `siglake rebuild-time-aggregates` — republish the inline time aggregates of
/// a table whose coverage chain cannot be proven (#3082).
///
/// Prints per component whether it was restored, because "exited 0" is not the
/// same as "the fast path is back": a component short of the table's row count
/// is deliberately left absent, and a table with delete files or NULL
/// timestamps lands short for reasons no rebuild can change. Saying which
/// component came back beats implying both did.
async fn run_rebuild_time_aggregates(
    data_dir: &std::path::Path,
    warehouse_url: Option<&str>,
    catalog_uri: Option<&str>,
    namespace: &str,
    table: &str,
) -> Result<()> {
    let ice = open_iceberg(
        data_dir,
        "warehouse",
        warehouse_url,
        catalog_uri,
        Some(namespace),
    )
    .await?;
    let report = ice.rebuild_inline_time_aggregates(table).await?;

    if report.already_covered {
        println!(
            "{namespace}.{table}: the inline aggregate already proves coverage of snapshot {} \
             (sequence {}); nothing to rebuild",
            report.coverage.snapshot_id, report.coverage.sequence_number
        );
        return Ok(());
    }
    println!(
        "{namespace}.{table}: read snapshot {} (sequence {}, table rows: {})",
        report.coverage.snapshot_id, report.coverage.sequence_number, report.record_count
    );
    println!(
        "  {:<24} {}",
        "time_buckets",
        match (report.time_buckets_restored, report.time_buckets_rows) {
            (true, Some(rows)) =>
                format!("rows={rows:<14} restored (date_histogram, windowed count)"),
            (false, Some(rows)) => format!(
                "rows={rows:<14} NOT written: short of the table row count, so the read guard \
                 would refuse it"
            ),
            (_, None) => "not read".to_string(),
        }
    );
    let mut short = Vec::new();
    for column in &report.columns {
        match column.rows {
            Some(rows) if column.covers_table => println!(
                "  {:<24} rows={rows:<14} restored (windowed GROUP BY)",
                format!("time_group_counts.{}", column.column)
            ),
            Some(rows) => {
                println!(
                    "  {:<24} rows={rows:<14} NOT written: short of the table row count",
                    format!("time_group_counts.{}", column.column)
                );
                short.push(column.column.clone());
            }
            None => {
                println!(
                    "  {:<24} NOT READABLE from any tier — left absent rather than written wrong",
                    format!("time_group_counts.{}", column.column)
                );
                short.push(column.column.clone());
            }
        }
    }
    if report.columns.is_empty() {
        println!("  {:<24} the object maintained none", "time_group_counts");
    }
    if !report.published {
        println!(
            "\nNOTHING WAS WRITTEN. No component could be proven complete against the table's \
             {} rows, and the publication drops the inline group counts — so writing here would \
             have destroyed what the object still holds in exchange for nothing. The table keeps \
             answering exactly from the per-file tiers.",
            report.record_count
        );
        return Ok(());
    }
    if !short.is_empty() {
        println!(
            "\nleft absent: {}. Counted from every live file and still short of the table row \
             count — the column exceeds a rollup cap, or is missing from files older than it. A \
             windowed GROUP BY on these keeps using the exact per-file tier.",
            short.join(", ")
        );
    }
    println!(
        "\nThe inline whole-table group counts were dropped: one coverage edge governs the \
         object and they could not be proven. They were already refused before this ran, so \
         `GROUP BY` without a time window is served exactly as it was. Commits after this one \
         extend the coverage chain normally."
    );
    Ok(())
}
