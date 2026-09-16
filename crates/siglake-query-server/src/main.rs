//! `siglake-query-server` binary entry point.
//!
//! See `crates/siglake-query-server/src/lib.rs` for the HTTP API surface
//! and the BYOC framing that motivates it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;

use siglake_query_server::discovery::{self, PeerSource};
use siglake_query_server::{
    serve, serve_tls, AppState, AuditService, AuthConfig, JobStore, OidcVerifier, QueryScanConfig,
    ServerLimits, TlsConfig,
};
use siglake_storage::{configure_query_scan_tuning, iceberg::IcebergContext, QueryScanTuning};

#[derive(Parser, Debug)]
#[command(
    name = "siglake-query-server",
    about = "HTTP query API for the siglake Iceberg warehouse",
    version = siglake_core::BUILD_VERSION
)]
struct Cli {
    /// Address to bind the HTTP API.
    #[arg(long, default_value = "0.0.0.0:8089", env = "SIGLAKE_QUERY_BIND")]
    bind: SocketAddr,

    /// Address to bind the Prometheus `/metrics` endpoint.
    #[arg(
        long,
        default_value = "0.0.0.0:9105",
        env = "SIGLAKE_QUERY_METRICS_BIND"
    )]
    metrics_bind: SocketAddr,

    /// Root directory used as the local data dir. Only consulted when
    /// `--warehouse-url` is omitted (local-fs warehouse mode).
    #[arg(long, default_value = "./data", env = "SIGLAKE_DATA_DIR")]
    data_dir: PathBuf,

    /// Subdirectory under `--data-dir` for the warehouse, when running
    /// locally with no `--warehouse-url`.
    #[arg(long, default_value = "warehouse")]
    warehouse: String,

    /// Full warehouse URL (e.g. `s3://bucket/prefix`).
    #[arg(long, env = "SIGLAKE_WAREHOUSE_URL")]
    warehouse_url: Option<String>,

    /// Iceberg catalog URI (e.g.
    /// `postgres://user:pass@host/db`, `sqlite://path?mode=rwc`).
    #[arg(long, env = "SIGLAKE_CATALOG_URI")]
    catalog_uri: Option<String>,

    /// Comma-separated bearer tokens accepted on `/api/v1/*` routes.
    /// Empty or unset = no bearer mode. Ignored when `--oidc-issuer` is
    /// also set (OIDC wins). Reads `SIGLAKE_QUERY_TOKENS` if not given
    /// on the command line.
    #[arg(long, env = "SIGLAKE_QUERY_TOKENS")]
    tokens: Option<String>,

    /// OIDC issuer URL. When set, the server discovers the JWKS via
    /// `<issuer>/.well-known/openid-configuration` and verifies every
    /// `Authorization: Bearer <jwt>` against it. Requires
    /// `--oidc-audience`.
    #[arg(long, env = "SIGLAKE_OIDC_ISSUER")]
    oidc_issuer: Option<String>,

    /// Expected `aud` claim. Required when `--oidc-issuer` is set.
    #[arg(long, env = "SIGLAKE_OIDC_AUDIENCE")]
    oidc_audience: Option<String>,

    /// Name of the JWT claim that carries the per-request tenant
    /// identifier. Setting it enables per-request multi-tenancy and
    /// makes the claim MANDATORY: each verified caller is routed to
    /// its own Iceberg namespace (`tenant_<claim>`, created on first
    /// use), and a token whose claim is missing, blank, not a string,
    /// longer than 128 chars, or outside `[A-Za-z0-9_-]` is refused
    /// with `403` before the request is routed anywhere. The value is
    /// validated, never repaired, so an unusable claim never falls
    /// back to the default namespace. When unset, tenancy is not
    /// derived from a claim at all and every caller reads the default
    /// namespace (the `--tenant-namespace` value). Requires
    /// `--oidc-issuer`.
    #[arg(long, env = "SIGLAKE_OIDC_TENANT_CLAIM")]
    oidc_tenant_claim: Option<String>,

    /// Server-side cap on rows returned per query. NDJSON streams stop
    /// at the cap and emit a truncation marker; records responses
    /// truncate, flip the `truncated` flag, and return 413.
    #[arg(long, default_value_t = 1_000_000, env = "SIGLAKE_QUERY_MAX_ROWS")]
    max_rows: usize,

    /// Number of source partitions to expose from the custom siglake
    /// Iceberg scan. Unset keeps the DataFusion default.
    #[arg(long, env = "SIGLAKE_QUERY_SCAN_PARTITIONS")]
    query_scan_partitions: Option<usize>,

    /// Distributed query (#7): comma-separated worker base URLs, one per
    /// shard (e.g. `http://q0:8089,http://q1:8089`). Enables
    /// `/api/v1/sql/distributed`, which fans a query across these peers and
    /// merges. Each node also serves `/api/v1/sql/shard` as a worker.
    ///
    /// A FIXED list: a pod added beyond it receives no shard work. Kubernetes
    /// deployments should use `--query-peer-discovery-srv` instead; this
    /// remains the non-Kubernetes and test compatibility mode, and its
    /// documented contract is that the coordinator is peer zero. Setting both
    /// is refused.
    #[arg(long, value_delimiter = ',', env = "SIGLAKE_QUERY_PEERS")]
    query_peers: Vec<String>,

    /// Distributed query (#967): the SRV record naming the query tier's
    /// headless Service port, e.g.
    /// `_http._tcp.siglake-query-headless.default.svc.cluster.local`. A
    /// background task re-resolves it and publishes a membership snapshot, so
    /// every Ready replica becomes eligible for shard work without a rollout.
    /// Each query pins ONE snapshot, so membership never moves under a running
    /// query. Mutually exclusive with `--query-peers`.
    #[arg(long, env = "SIGLAKE_QUERY_PEER_DISCOVERY_SRV")]
    query_peer_discovery_srv: Option<String>,

    /// Scheme discovered peers are addressed with (`http` or `https`). SRV
    /// records carry a target and a port but no scheme, so this is explicit.
    /// Only meaningful with `--query-peer-discovery-srv`.
    #[arg(long, env = "SIGLAKE_QUERY_PEER_SCHEME")]
    query_peer_scheme: Option<String>,

    /// How often peer discovery re-resolves the SRV record, in seconds.
    /// CoreDNS remains the TTL authority; this bounds how quickly a scale
    /// event becomes visible to fan-out.
    #[arg(
        long,
        default_value_t = 5,
        env = "SIGLAKE_QUERY_PEER_DISCOVERY_INTERVAL_SECS"
    )]
    query_peer_discovery_interval_secs: u64,

    /// This pod's name, matched against the SRV answer to find the
    /// coordinator's OWN worker URL (the failover target). Defaults to the
    /// `HOSTNAME` the container runtime sets, which for a StatefulSet pod is
    /// its pod name.
    #[arg(long, env = "SIGLAKE_QUERY_PEER_SELF_NAME")]
    query_peer_self_name: Option<String>,

    /// Bearer token the coordinator presents to peer workers (when they
    /// enforce auth).
    #[arg(long, env = "SIGLAKE_QUERY_COORDINATOR_TOKEN")]
    query_coordinator_token: Option<String>,

    /// Aggregate object-store reader budget per query. When set, the
    /// custom scan derives per-partition reader concurrency from the
    /// planned source partition count so total reader fan-out stays
    /// bounded under wider scans.
    #[arg(long, env = "SIGLAKE_QUERY_SCAN_READER_BUDGET")]
    query_scan_reader_budget: Option<usize>,

    /// Per-source-partition object-store read concurrency inside the
    /// custom siglake Iceberg scan. Unset derives automatically.
    #[arg(long, env = "SIGLAKE_QUERY_SCAN_FILE_CONCURRENCY")]
    query_scan_file_concurrency: Option<usize>,

    /// Target Arrow batch size inside the custom siglake Iceberg scan.
    /// Unset keeps the iceberg reader default.
    #[arg(long, env = "SIGLAKE_QUERY_SCAN_BATCH_SIZE")]
    query_scan_batch_size: Option<usize>,

    /// Merge nearby object-store byte ranges into larger reads when
    /// the gap is smaller than this many bytes. Unset keeps the
    /// iceberg reader default.
    #[arg(long, env = "SIGLAKE_QUERY_SCAN_RANGE_COALESCE_BYTES")]
    query_scan_range_coalesce_bytes: Option<u64>,

    /// Maximum concurrent merged byte-range fetches per source
    /// partition. Unset keeps the iceberg reader default.
    #[arg(long, env = "SIGLAKE_QUERY_SCAN_RANGE_FETCH_CONCURRENCY")]
    query_scan_range_fetch_concurrency: Option<usize>,

    /// Only enable the merged-range reader path when the planned scan
    /// touches at least this many bytes. Unset applies the range
    /// settings to every scan.
    #[arg(long, env = "SIGLAKE_QUERY_SCAN_RANGE_ADAPTIVE_MIN_BYTES")]
    query_scan_range_adaptive_min_bytes: Option<u64>,

    /// Only enable the merged-range reader path when the planned scan
    /// touches at least this many files. Unset applies the range
    /// settings to every scan.
    #[arg(long, env = "SIGLAKE_QUERY_SCAN_RANGE_ADAPTIVE_MIN_FILES")]
    query_scan_range_adaptive_min_files: Option<usize>,

    /// Reduce source partition fan-out for smaller planned scans by
    /// targeting at least this many bytes per partition. Unset keeps
    /// the fixed `--query-scan-partitions` ceiling.
    #[arg(long, env = "SIGLAKE_QUERY_SCAN_ADAPTIVE_PARTITION_MIN_BYTES")]
    query_scan_adaptive_partition_min_bytes: Option<u64>,

    /// Reduce source partition fan-out for smaller planned scans by
    /// targeting at least this many files per partition. Unset keeps
    /// the fixed `--query-scan-partitions` ceiling.
    #[arg(long, env = "SIGLAKE_QUERY_SCAN_ADAPTIVE_PARTITION_MIN_FILES")]
    query_scan_adaptive_partition_min_files: Option<usize>,

    /// Total in-memory byte budget for the experimental source-file batch cache
    /// (process-lifetime, LRU). Disabled when unset or 0; enabling it requires
    /// positive byte and entry limits.
    #[arg(long, env = "SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_BYTES")]
    query_scan_file_cache_max_bytes: Option<u64>,

    /// Maximum number of source files to retain in the experimental source-file
    /// batch cache. Disabled when unset or 0; enabling it requires positive byte
    /// and entry limits.
    #[arg(long, env = "SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_ENTRIES")]
    query_scan_file_cache_max_entries: Option<usize>,

    /// Byte-range object cache budget (bytes) — the cold-S3 hot cache that caches
    /// Parquet footers, column chunks, and index sidecars so repeat/warm reads
    /// (incl. the FTS/bloom pruning path) don't re-hit S3.
    ///
    /// DEFAULT-ON, sized at **1/4 of the container memory limit** (64 MiB floor,
    /// 16 GiB cap); 1 GiB when there is no cgroup limit to read. Set to 0 to
    /// disable. A 64 GiB pod derives to the 16 GiB every published board used.
    #[arg(long, env = "SIGLAKE_OBJECT_CACHE_BYTES")]
    query_object_cache_max_bytes: Option<u64>,

    /// Budget (bytes) for parsed per-file inverted indexes, the form a warm text
    /// query is served from.
    ///
    /// DEFAULT-ON, sized at **1/16 of the container memory limit** (64 MiB
    /// floor, 1 GiB cap); 1 GiB when there is no cgroup limit to read. A parsed
    /// index costs about 40 bytes per indexed row. Set to 0 to deserialize per
    /// query, as before this cache existed.
    #[arg(long, env = "SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES")]
    query_parsed_index_cache_max_bytes: Option<u64>,

    /// Budget (bytes) for the serialized Puffin blobs those indexes are parsed
    /// from, which a parsed eviction falls back on.
    ///
    /// DEFAULT-ON, sized at **1/64 of the container memory limit** (16 MiB
    /// floor, 256 MiB cap); 256 MiB when there is no cgroup limit to read — a
    /// quarter of the parsed budget, which is about what a blob is of its
    /// parsed form, so the two cover the same files. Set to 0 to keep only the
    /// parsed form and re-fetch on a miss.
    #[arg(long, env = "SIGLAKE_PUFFIN_BLOB_CACHE_MAX_BYTES")]
    query_puffin_blob_cache_max_bytes: Option<u64>,

    /// WAL root the query node can read. When set, queries union the Iceberg
    /// snapshot with the uncommitted WAL segments (sealed/ + processing/) under
    /// this directory, so just-ingested rows are visible before the compaction
    /// commit. This covers `events` and every managed user index the query
    /// references. Requires this pod to see the ingester's WAL — a shared RWX
    /// volume (EFS) in the distributed deployment. Unset ⇒ disabled (historical
    /// commit-cycle visibility).
    #[arg(long, env = "SIGLAKE_QUERY_WAL_BUFFER_DIR")]
    query_wal_buffer_dir: Option<std::path::PathBuf>,

    /// Enable query-tier hot caches. When set (and `--query-wal-buffer-dir` is
    /// provided), a background task tails the WAL root and serves the
    /// `last_values()` and `distinct_values(<dim>)` UDTFs from per-tenant
    /// last-value + distinct caches. Unset ⇒ disabled.
    #[arg(long, default_value_t = false, env = "SIGLAKE_QUERY_HOT_CACHES")]
    query_hot_caches: bool,

    /// Disable best-effort audit-log emission. By default each completed query
    /// is submitted to the bounded `siglake.query_audit` writer; rows are
    /// dropped whole if that writer is over budget or unavailable.
    #[arg(long, default_value_t = false, env = "SIGLAKE_QUERY_AUDIT_DISABLED")]
    audit_disabled: bool,

    /// Postgres URI for the persistent batch-job store. When set, batch
    /// jobs survive pod restart. Every replica shares the store, so
    /// recovery is scoped to execution ownership: a starting replica
    /// leaves jobs owned by a still-heartbeating sibling alone, and marks
    /// a job `failed` only once its owner's lease has expired (see
    /// `--jobs-owner-lease-secs`). That includes this pod's own previous
    /// incarnation, so a restart surfaces a terminal state within one
    /// lease period rather than instantly. When unset or BLANK, batch state
    /// is in-memory and private to this process: pod restart drops every
    /// queued and running job, and a peer replica answers 404 for them. The
    /// chart sets this from the catalog URI by default
    /// (`query.jobs.persistent: true`).
    #[arg(long, env = "SIGLAKE_JOBS_POSTGRES_URI")]
    jobs_postgres_uri: Option<String>,

    /// How stale a query replica's job-owner heartbeat may get before its
    /// in-flight batch jobs are considered orphaned and failed. The owner
    /// heartbeats at a third of this interval, so three consecutive lost
    /// writes are survivable. Postgres-backed store only.
    #[arg(
        long,
        default_value_t = 120,
        env = "SIGLAKE_JOBS_OWNER_LEASE_SECS",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    jobs_owner_lease_secs: u64,

    /// Grace period for batch-job rows that carry no execution owner —
    /// rows written by a build older than ownership tracking. Their
    /// executor's liveness is unknowable, so they are failed only once
    /// they are this old. Postgres-backed store only.
    #[arg(
        long,
        default_value_t = 86_400,
        env = "SIGLAKE_JOBS_OWNERLESS_GRACE_SECS"
    )]
    jobs_ownerless_grace_secs: u64,

    /// How often an executing replica re-reads the status of the batch jobs
    /// it is running, so a cancellation persisted by *another* replica
    /// reaches the future that holds the admission reservation and the
    /// storage-scan cancel guard. This is the bound the `202` from
    /// `DELETE /api/v1/jobs/<id>` promises: after it, the work has stopped.
    /// The query is by primary key over this pod's in-flight jobs only.
    /// Postgres-backed store only — with the in-memory store the replica
    /// that accepts the DELETE is necessarily the executor and the abort is
    /// immediate.
    #[arg(
        long,
        default_value_t = 2,
        env = "SIGLAKE_JOBS_CANCEL_POLL_SECS",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    jobs_cancel_poll_secs: u64,

    /// Path to a PEM-encoded TLS cert chain. Pair with `--tls-key`
    /// to serve HTTPS instead of HTTP. When either is unset the
    /// binary stays HTTP-only and TLS is left to a fronting Ingress.
    #[arg(long, env = "SIGLAKE_QUERY_TLS_CERT")]
    tls_cert: Option<PathBuf>,

    /// Path to the matching PEM-encoded private key.
    #[arg(long, env = "SIGLAKE_QUERY_TLS_KEY")]
    tls_key: Option<PathBuf>,

    /// Iceberg namespace this query-server reads from. Defaults to
    /// `siglake`. Set per-tenant to run multiple isolated siglake
    /// deployments against one shared Iceberg catalog + warehouse;
    /// each tenant gets its own namespace. This is deployment-level
    /// tenancy: it is the namespace callers read while
    /// `--oidc-tenant-claim` is unset. With a claim configured, every
    /// request is routed by its own claim instead.
    #[arg(long, default_value = "siglake", env = "SIGLAKE_TENANT_NAMESPACE")]
    tenant_namespace: String,
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
    // Logs + traces via OTel (opt-in via OTEL_EXPORTER_OTLP_ENDPOINT); the fmt
    // console layer always stays on, and stays on stderr, matching the siglake
    // CLI and the rest of the workspace's binaries.
    siglake_core::telemetry::init(siglake_core::telemetry::TelemetryConfig::from_env("query"))?;
    // `run` owns every other exit from this process — graceful shutdown, a
    // startup error, a bad flag — so the flush happens once, here, on all of
    // them. The providers live in a `OnceLock` that never drops, so nothing
    // else would flush them. No-op when OTel is off.
    let result = run().await;
    siglake_core::telemetry::shutdown();
    result
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    let read_caches = siglake_storage::resolve_query_read_cache_config(
        siglake_storage::iceberg::cgroup_memory_limit_bytes(),
        cli.query_object_cache_max_bytes,
        cli.query_scan_file_cache_max_bytes,
        cli.query_scan_file_cache_max_entries,
    );
    siglake_storage::configure_query_read_caches(read_caches);

    // The two text-index caches live in the vendored fork, which cannot read
    // the cgroup limit (that lives in siglake-storage, which depends on it), so
    // their budgets are resolved here and pushed in — before the query memory
    // pool is built, since it subtracts what they hold.
    let text_index_caches = siglake_storage::resolve_text_index_cache_config(
        siglake_storage::iceberg::cgroup_memory_limit_bytes(),
        cli.query_parsed_index_cache_max_bytes,
        cli.query_puffin_blob_cache_max_bytes,
    );
    siglake_storage::configure_text_index_caches(text_index_caches);

    let ice = open_iceberg(
        &cli.data_dir,
        &cli.warehouse,
        cli.warehouse_url.as_deref(),
        cli.catalog_uri.as_deref(),
        &cli.tenant_namespace,
    )
    .await?;

    let auth = match (cli.oidc_issuer.as_deref(), cli.oidc_audience.as_deref()) {
        (Some(issuer), Some(audience)) => {
            let mut verifier = OidcVerifier::from_issuer(issuer.to_string(), audience.to_string())
                .await
                .with_context(|| format!("oidc discovery for {issuer}"))?;
            if let Some(claim) = cli.oidc_tenant_claim.as_deref() {
                verifier = verifier.with_tenant_claim(claim);
                tracing::info!(claim, "per-request tenant routing enabled");
            }
            AuthConfig::from_oidc(verifier)
        }
        (Some(_), None) | (None, Some(_)) => {
            anyhow::bail!("--oidc-issuer and --oidc-audience must both be set (or both unset)");
        }
        (None, None) => {
            if cli.oidc_tenant_claim.is_some() {
                anyhow::bail!("--oidc-tenant-claim requires --oidc-issuer + --oidc-audience");
            }
            match cli.tokens.as_deref() {
                Some(s) => AuthConfig::from_tokens(s.split(',')),
                None => AuthConfig::open(),
            }
        }
    };
    if auth.is_open() {
        tracing::warn!(
            "auth is open (no --tokens / SIGLAKE_QUERY_TOKENS, no OIDC issuer) — \
             do not run this configuration outside a trusted network"
        );
    }

    let limits = ServerLimits {
        max_rows: cli.max_rows,
        ..ServerLimits::default()
    };
    let query_scan = QueryScanConfig {
        target_partitions: cli.query_scan_partitions.filter(|n| *n > 0),
        reader_budget: cli.query_scan_reader_budget.filter(|n| *n > 0),
        file_concurrency_limit: cli.query_scan_file_concurrency.filter(|n| *n > 0),
        batch_size: cli.query_scan_batch_size.filter(|n| *n > 0),
        range_coalesce_bytes: cli.query_scan_range_coalesce_bytes.filter(|n| *n > 0),
        range_fetch_concurrency: cli.query_scan_range_fetch_concurrency.filter(|n| *n > 0),
        range_adaptive_min_bytes: cli.query_scan_range_adaptive_min_bytes.filter(|n| *n > 0),
        range_adaptive_min_files: cli.query_scan_range_adaptive_min_files.filter(|n| *n > 0),
        adaptive_partition_min_bytes: cli
            .query_scan_adaptive_partition_min_bytes
            .filter(|n| *n > 0),
        adaptive_partition_min_files: cli
            .query_scan_adaptive_partition_min_files
            .filter(|n| *n > 0),
        file_cache_max_bytes: read_caches.file_cache_max_bytes,
        file_cache_max_entries: read_caches.file_cache_max_entries,
        ordered_merge_max_fan_in: None,
        ordered_sort_cluster_max_rows: None,
        reversed_chunk_rows: None,
        bypass_reader_caches: false,
        ordered_drain_buffer_bytes: None,
    };
    configure_query_scan_tuning(QueryScanTuning {
        reader_budget: query_scan.reader_budget,
        file_concurrency_limit: query_scan.file_concurrency_limit,
        batch_size: query_scan.batch_size,
        range_coalesce_bytes: query_scan.range_coalesce_bytes,
        range_fetch_concurrency: query_scan.range_fetch_concurrency,
        range_adaptive_min_bytes: query_scan.range_adaptive_min_bytes,
        range_adaptive_min_files: query_scan.range_adaptive_min_files,
        adaptive_partition_min_bytes: query_scan.adaptive_partition_min_bytes,
        adaptive_partition_min_files: query_scan.adaptive_partition_min_files,
        file_cache_max_bytes: query_scan.file_cache_max_bytes,
        file_cache_max_entries: query_scan.file_cache_max_entries,
        ordered_drain_buffer_bytes: query_scan.ordered_drain_buffer_bytes,
    });
    tracing::info!(
        target_partitions = query_scan.target_partitions,
        reader_budget = query_scan.reader_budget,
        file_concurrency_limit = query_scan.file_concurrency_limit,
        batch_size = query_scan.batch_size,
        range_coalesce_bytes = query_scan.range_coalesce_bytes,
        range_fetch_concurrency = query_scan.range_fetch_concurrency,
        range_adaptive_min_bytes = query_scan.range_adaptive_min_bytes,
        range_adaptive_min_files = query_scan.range_adaptive_min_files,
        adaptive_partition_min_bytes = query_scan.adaptive_partition_min_bytes,
        adaptive_partition_min_files = query_scan.adaptive_partition_min_files,
        file_cache_max_bytes = query_scan.file_cache_max_bytes,
        file_cache_max_entries = query_scan.file_cache_max_entries,
        "query scan tuning"
    );
    let ice = Arc::new(ice);
    // #79 continuous pre-warm: table entries + live-file lists + footers for
    // the events table and every managed index, at startup AND on a periodic
    // cadence — an unqueried node otherwise ages past the staleness ceiling
    // and the next (possibly distributed) query pays the synchronous metadata
    // reload + manifest walk (~1–2.7s at 200G, scaling with file count).
    // `SIGLAKE_QUERY_PREWARM=0` disables warming entirely;
    // `SIGLAKE_QUERY_WARM_INTERVAL_SECS` tunes the cadence (0 = startup-only).
    if std::env::var("SIGLAKE_QUERY_WARM_INTERVAL_SECS").as_deref() == Ok("0") {
        if siglake_storage::iceberg::query_prewarm_enabled() {
            let warm_ice = ice.clone();
            tokio::spawn(async move {
                siglake_query_server::warm_all_query_caches(&warm_ice).await;
            });
        }
    } else {
        siglake_query_server::spawn_query_cache_warmer(ice.clone());
    }
    // OUTSIDE the warmer's branch. It was inside it, which coupled memory
    // reporting to the warmer's configuration — the exact thing the comment on
    // `spawn_memory_gauges` says not to do: reporting must survive the warmer
    // being disabled or stalling, because that is when it is most wanted. With
    // `SIGLAKE_QUERY_WARM_INTERVAL_SECS=0` there were no memory gauges at all.
    siglake_query_server::spawn_memory_gauges(ice.clone());
    let jobs = match jobs_store_uri_from(cli.jobs_postgres_uri.as_deref()) {
        Some(uri) => {
            let recovery = siglake_query_server::jobs::JobRecoveryPolicy {
                owner_lease: std::time::Duration::from_secs(cli.jobs_owner_lease_secs),
                ownerless_grace: std::time::Duration::from_secs(cli.jobs_ownerless_grace_secs),
                cancel_poll: std::time::Duration::from_secs(cli.jobs_cancel_poll_secs),
            };
            let store =
                JobStore::new_postgres(uri, 2, std::time::Duration::from_secs(86_400), recovery)
                    .await
                    .context("init Postgres JobStore")?;
            tracing::info!(
                owner = store.owner_id(),
                owner_lease_secs = cli.jobs_owner_lease_secs,
                cancel_poll_secs = cli.jobs_cancel_poll_secs,
                "jobs: using Postgres-backed JobStore"
            );
            store
        }
        None => {
            tracing::info!("jobs: using in-memory JobStore (state lost on restart)");
            JobStore::default()
        }
    };
    let peer_source = build_peer_source(&cli)?;
    let mut state = AppState::new(ice.clone(), auth)
        .with_limits(limits)
        .with_jobs(jobs)
        .with_query_scan(query_scan)
        .with_wal_buffer_dir(cli.query_wal_buffer_dir.clone())
        .with_peer_source(peer_source, cli.query_coordinator_token.clone());

    if let Some(dir) = cli.query_wal_buffer_dir.as_deref() {
        tracing::info!(dir = %dir.display(), "real-time WAL buffer enabled for `events`");
    }

    if cli.query_hot_caches {
        match cli.query_wal_buffer_dir.clone() {
            Some(dir) => {
                tracing::info!(dir = %dir.display(),
                    "query-tier hot caches enabled (last_values/distinct_values)");
                state = state.with_hot_caches(
                    Some(dir),
                    siglake_query_server::hot_cache::HotCacheConfig::default(),
                );
            }
            None => tracing::warn!(
                "--query-hot-caches set but --query-wal-buffer-dir is unset; hot caches disabled"
            ),
        }
    }

    if cli.oidc_tenant_claim.is_some() {
        state = state.with_tenants(siglake_query_server::TenantRegistry::new(ice.clone()));
    }

    if !cli.audit_disabled {
        let (service, writer) = AuditService::with_defaults(ice.clone());
        tokio::spawn(service.run());
        state = state.with_audit(writer);
    }

    let _metrics = siglake_core::metrics::init(cli.metrics_bind).await?;
    // Alerted counters exist at 0 from the first scrape, so the first
    // abandoned warm cycle or exec-pool task is a delta `increase()` can see.
    siglake_core::metrics::preregister(siglake_core::metrics::QUERY_SERVER_ALERTED_COUNTERS);
    siglake_query_server::jobs::initialize_metrics();
    let build = siglake_core::build_info();
    metrics::gauge!(
        "siglake_build_info",
        "version" => build.version,
        "commit" => build.commit
    )
    .set(1.0);

    match (cli.tls_cert.as_deref(), cli.tls_key.as_deref()) {
        (Some(cert), Some(key)) => {
            let tls = TlsConfig {
                cert_path: cert.into(),
                key_path: key.into(),
            };
            serve_tls(cli.bind, state, tls).await
        }
        (Some(_), None) | (None, Some(_)) => {
            anyhow::bail!("--tls-cert and --tls-key must both be set (or both unset)")
        }
        (None, None) => serve(cli.bind, state).await,
    }
}

/// Open an [`IcebergContext`] from CLI flag/env-var inputs. Same logic
/// as the siglake-cli `open_iceberg` helper — duplicated here to avoid
/// pulling the entire CLI crate in.
async fn open_iceberg(
    data_dir: &std::path::Path,
    warehouse_sub: &str,
    warehouse_url: Option<&str>,
    catalog_uri: Option<&str>,
    tenant_namespace: &str,
) -> Result<IcebergContext> {
    if let Some(url) = warehouse_url {
        let catalog_uri = match catalog_uri {
            Some(c) => c.to_string(),
            None => {
                let local = data_dir.join(warehouse_sub);
                std::fs::create_dir_all(&local)
                    .with_context(|| format!("creating {}", local.display()))?;
                let abs = std::fs::canonicalize(&local)?;
                format!("sqlite://{}/_catalog.db?mode=rwc", abs.display())
            }
        };
        IcebergContext::open_with_namespace(&catalog_uri, url, tenant_namespace).await
    } else {
        // Local-fs path: the default `open()` always uses the
        // hardcoded `siglake` namespace. We still respect a non-default
        // tenant_namespace by routing through the explicit form.
        let warehouse_dir = data_dir.join(warehouse_sub);
        if tenant_namespace == siglake_storage::iceberg::NAMESPACE {
            IcebergContext::open(&warehouse_dir).await
        } else {
            std::fs::create_dir_all(&warehouse_dir)?;
            let abs = std::fs::canonicalize(&warehouse_dir)?;
            let url = format!("file://{}", abs.display());
            let cat_uri = format!("sqlite://{}/_catalog.db?mode=rwc", abs.display());
            IcebergContext::open_with_namespace(&cat_uri, &url, tenant_namespace).await
        }
    }
}

/// Build this pod's distributed-query membership source and, under SRV
/// discovery, start the background resolver.
///
/// Must be called from inside the runtime: discovery is a spawned task.
fn build_peer_source(cli: &Cli) -> Result<Option<PeerSource>> {
    let scheme = discovery::peer_scheme_from(cli.query_peer_scheme.as_deref())
        .map_err(|e| anyhow::anyhow!(e))?;
    match discovery::peer_config_from(&cli.query_peers, cli.query_peer_discovery_srv.as_deref())
        .map_err(|e| anyhow::anyhow!(e))?
    {
        discovery::PeerConfig::None => Ok(None),
        discovery::PeerConfig::Static(peers) => {
            tracing::info!(
                ?peers,
                "distributed query enabled (coordinator, static peer list)"
            );
            Ok(PeerSource::from_static(peers))
        }
        discovery::PeerConfig::Srv(srv) => {
            // The pod identity is read from the environment exactly ONCE, here,
            // and everything downstream takes it as an argument — the matching
            // itself is a pure function tests drive directly.
            let hostname = cli
                .query_peer_self_name
                .clone()
                .or_else(|| std::env::var("HOSTNAME").ok())
                .unwrap_or_default();
            if hostname.trim().is_empty() {
                anyhow::bail!(
                    "--query-peer-discovery-srv needs this pod's own name to find its \
                     failover URL in the SRV answer, and neither \
                     --query-peer-self-name nor HOSTNAME is set"
                );
            }
            let interval =
                std::time::Duration::from_secs(cli.query_peer_discovery_interval_secs.max(1));
            tracing::info!(
                srv = %srv,
                scheme = scheme.as_str(),
                hostname = %hostname,
                interval_secs = interval.as_secs(),
                "distributed query enabled (coordinator, SRV peer discovery); queries run \
                 locally until the first membership is published"
            );
            let directory = std::sync::Arc::new(discovery::PeerDirectory::new(hostname));
            let resolver = std::sync::Arc::new(
                discovery::DnsSrvResolver::from_system().context("build the SRV resolver")?,
            );
            discovery::spawn_peer_discovery(directory.clone(), resolver, srv, scheme, interval);
            Ok(Some(PeerSource::from_directory(directory)))
        }
    }
}

/// The batch-job store URI this process runs with, from whatever
/// `--jobs-postgres-uri` / `SIGLAKE_JOBS_POSTGRES_URI` carries.
///
/// A BLANK value is the opt-out, not a URI. The chart turns the store off by
/// omitting the variable, but an operator-managed pod cannot omit an entry the
/// operator renders — `spec.extraEnv` can only give it another value — and a
/// `""` that reached `JobStore::new_postgres` would crash-loop the pod on
/// `connect Postgres `. So blank (and whitespace) reads as "in-memory store",
/// which is the state the chart's `query.jobs.persistent: false` produces.
fn jobs_store_uri_from(configured: Option<&str>) -> Option<&str> {
    let uri = configured?.trim();
    (!uri.is_empty()).then_some(uri)
}

#[cfg(test)]
mod tests {
    use super::{jobs_store_uri_from, Cli};
    use clap::CommandFactory;
    use siglake_storage::resolve_query_read_cache_config;

    #[test]
    fn cli_version_contains_the_embedded_provenance() {
        assert_eq!(
            Cli::command().get_version(),
            Some(siglake_core::BUILD_VERSION)
        );
        // Catches a duplicated flag name or environment variable among the
        // cache knobs, which clap otherwise only reports by panicking in the
        // deployed binary.
        Cli::command().debug_assert();
    }

    /// The text-index cache flags take the same path as the read-cache ones:
    /// resolved once here, then both applied to the caches and subtracted from
    /// the query memory pool. A flag that changed only one of those is the
    /// defect this shape exists to prevent.
    #[test]
    fn text_index_cache_flags_are_the_values_reserved_for_the_pool() {
        let derived = siglake_storage::resolve_text_index_cache_config(
            Some(8 * 1024 * 1024 * 1024),
            None,
            None,
        );
        assert_eq!(derived.parsed_index_max_bytes, 512 * 1024 * 1024);
        assert_eq!(derived.puffin_blob_max_bytes, 128 * 1024 * 1024);
        assert_eq!(derived.reserved_bytes(), 640 * 1024 * 1024);

        // The packaged 4Gi pod spends its whole remainder on the pool's
        // first-file decode reservation, so it derives no text-index caches.
        let floor = siglake_storage::resolve_text_index_cache_config(
            Some(4 * 1024 * 1024 * 1024),
            None,
            None,
        );
        assert_eq!(floor.reserved_bytes(), 0);

        let overridden = siglake_storage::resolve_text_index_cache_config(
            Some(4 * 1024 * 1024 * 1024),
            Some(2 * 1024 * 1024 * 1024),
            Some(0),
        );
        assert_eq!(overridden.reserved_bytes(), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn no_cgroup_uses_the_actual_object_cache_fallback_and_no_file_cache() {
        let config = resolve_query_read_cache_config(None, None, None, None);

        assert_eq!(config.object_cache_max_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.file_cache_max_bytes, None);
        assert_eq!(config.file_cache_max_entries, None);
        assert_eq!(config.reserved_bytes(), config.object_cache_max_bytes);
    }

    #[test]
    fn cli_cache_overrides_are_the_values_reserved_for_the_pool() {
        let config = resolve_query_read_cache_config(
            None,
            Some(3 * 1024 * 1024 * 1024),
            Some(536_870_912),
            Some(512),
        );

        assert_eq!(config.object_cache_max_bytes, 3 * 1024 * 1024 * 1024);
        assert_eq!(config.file_cache_max_bytes, Some(536_870_912));
        assert_eq!(config.file_cache_max_entries, Some(512));
        assert_eq!(config.reserved_bytes(), 3_758_096_384);
    }

    /// The persistent store is the chart default, so the only ways to ask for
    /// the in-memory one are an absent variable and a blank one — the latter
    /// being all `spec.extraEnv` can express against an operator-rendered
    /// entry.
    #[test]
    fn a_blank_jobs_store_uri_is_the_in_memory_opt_out() {
        assert_eq!(jobs_store_uri_from(None), None);
        assert_eq!(jobs_store_uri_from(Some("")), None);
        assert_eq!(jobs_store_uri_from(Some("   ")), None);
        assert_eq!(
            jobs_store_uri_from(Some("  postgres://siglake:pw@db:5432/siglake  ")),
            Some("postgres://siglake:pw@db:5432/siglake")
        );
    }

    #[test]
    fn source_file_cache_is_disabled_unless_both_limits_are_positive() {
        let missing_entries = resolve_query_read_cache_config(None, None, Some(1024), None);
        let zero_bytes = resolve_query_read_cache_config(None, None, Some(0), Some(8192));

        assert_eq!(missing_entries.file_cache_max_bytes, None);
        assert_eq!(missing_entries.file_cache_max_entries, None);
        assert_eq!(zero_bytes.file_cache_max_bytes, None);
        assert_eq!(zero_bytes.file_cache_max_entries, None);
    }
}
