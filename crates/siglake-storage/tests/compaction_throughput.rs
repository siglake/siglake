//! Phase-0 compaction bench: merge throughput per merge path, per time layout.
//!
//! The 2026-08-04 1TB round measured compaction at ~95K rows/s — about one
//! core's worth on a 16-vCPU box — and concluded compaction structurally cannot
//! keep up with ingest. That number was measured in aggregate from
//! `compactor.log`; nothing attributed it to a *path*. siglake has three merge
//! implementations and `recluster_files` picks between them by memory safety,
//! not by cost:
//!
//! ```text
//! recluster_files
//!  ├ bin ≤ INRAM caps ─────────→ in-RAM concat + full re-sort
//!  └ else merge_files_streaming
//!      ├ files ≤ fanin (64) ───→ merge_file_slice_streaming   (heap pop per ROW)
//!      └ files >  fanin ───────→ merge_files_page_bounded     (RLE run plan)
//! ```
//!
//! The bench deploy sets `SIGLAKE_RECLUSTER_MERGE_FANIN=16` (not the code
//! default of 64), so that round's 67 slow bins split 55 slice-streaming
//! (≤16 files) and 12 page-bounded (>16) — matching its 12 `page-bounded merge
//! planned` lines exactly. This bench measures the same bin through each path
//! so the difference is a number rather than an inference, and it runs locally
//! in minutes instead of an overnight AWS round.
//!
//! Run:
//! ```text
//! cargo test -p siglake-storage --test compaction_throughput -- --ignored --nocapture
//! ```
//!
//! Knobs (all optional): `BENCH_FILES` (12), `BENCH_ROWS_PER_FILE` (100_000),
//! `BENCH_LAYOUTS` (`disjoint,staggered,overlapping`), `BENCH_PATHS`
//! (`slice,page`; add `inram` for the third path). To change the Parquet
//! row-group target, set the writer's own `SIGLAKE_PARQUET_TARGET_ROW_GROUP_BYTES`
//! in the environment before launching (it is read per file).
//!
//! Separate test binary because it is a perf report, not a correctness test:
//! both arms are `#[ignore]` and run by hand. The merge path under test is
//! selected per arm through explicit `ReclusterMergeOptions`, never by writing
//! the process-global `SIGLAKE_RECLUSTER_*` knobs.

use chrono::{TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

use siglake_core::Event;
use siglake_storage::iceberg::{
    IcebergContext, LevelPolicy, LeveledPassOptions, MergePathKind, ReclusterMergeOptions,
    ReclusterPolicy, BLOOM_FILTER_COLUMNS,
};

// ---------------------------------------------------------------- metrics ---

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

/// Sum a counter across all series matching `name`, optionally filtered to one
/// label value. Counters are process-cumulative, so arms diff two snapshots.
fn counter_sum(snapshot: &SnapshotVec, name: &str, label: Option<(&str, &str)>) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && label
                    .is_none_or(|(k, v)| key.key().labels().any(|l| l.key() == k && l.value() == v))
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

// ---------------------------------------------------------------- corpus ----

/// Deterministic LCG (Numerical Recipes constants). Hand-rolled so the bench
/// has no dev-dependency on a RNG crate and so a given seed reproduces byte
/// for byte across runs — the same property the AWS corpus generator has.
struct Lcg(u64);

impl Lcg {
    fn next_u32(&mut self) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) as u32
    }

    fn below(&mut self, n: u32) -> u32 {
        self.next_u32() % n.max(1)
    }

    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len() as u32) as usize]
    }
}

const LEVELS: &[&str] = &[
    "debug", "info", "info", "info", "info", "warn", "warn", "error",
];
const REGIONS: &[&str] = &[
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-southeast-1",
    "ap-northeast-1",
];
const SERVICES: &[&str] = &[
    "checkout",
    "catalog",
    "auth",
    "payments",
    "search",
    "recommendations",
    "inventory",
    "shipping",
    "notifications",
    "gateway",
];
const METHODS: &[&str] = &["GET", "GET", "GET", "POST", "PUT", "DELETE"];
const PATHS: &[&str] = &[
    "/",
    "/api/v1/cart",
    "/api/v1/products",
    "/api/v1/checkout",
    "/api/v1/login",
    "/healthz",
    "/metrics",
    "/api/v1/search",
];
const AGENTS: &[&str] = &["filebeat", "vector", "fluentbit", "otel-collector"];
const PROVIDERS: &[&str] = &["aws", "gcp", "azure"];

/// One corpus-shaped event. Mirrors the AWS benchmark corpus
/// (`siglake-benchmarks/corpus/generate_corpus.py`): 2000 hosts, a templated
/// one-line message, and a residual `attributes` JSON carrying the nested
/// agent/cloud/http/trace keys. Both wide columns are Utf8, which is what makes
/// a per-row gather expensive — the cost this bench is trying to attribute.
fn gen_event(rng: &mut Lcg, ts_secs: i64) -> Event {
    let region = rng.pick(REGIONS);
    let service = rng.pick(SERVICES);
    let method = rng.pick(METHODS);
    let path = rng.pick(PATHS);
    let status = [200u32, 200, 200, 201, 204, 301, 400, 404, 500][rng.below(9) as usize];
    let ms = rng.below(4000) + 1;
    let host_id = rng.below(2000) + 1;

    let raw = format!("{method} {path} {status} in {ms}ms service={service}");
    // Trace ids only need to be wide and varied, not cryptographic — widen the
    // LCG's 32 bits with a fixed odd multiplier.
    let widen = |v: u32| (v as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let trace_hi = widen(rng.next_u32());
    let trace_lo = widen(rng.next_u32());
    let span_id = widen(rng.next_u32());
    let attributes = format!(
        r#"{{"agent":{{"name":"{}","version":"8.1{}.0","id":"agent-{:05}"}},"cloud":{{"provider":"{}","region":"{}","availability_zone":"{}{}","account_id":"{:012}"}},"host":{{"ip":"10.{}.{}.{}","os":"linux"}},"http":{{"method":"{}","path":"{}","status_code":{},"response_time_ms":{}}},"trace":{{"id":"{trace_hi:016x}{trace_lo:016x}","span_id":"{span_id:016x}"}}}}"#,
        rng.pick(AGENTS),
        rng.below(4),
        rng.below(5000) + 1,
        rng.pick(PROVIDERS),
        region,
        region,
        ["a", "b", "c"][rng.below(3) as usize],
        rng.below(999_999_999) as u64 * 1000,
        rng.below(256),
        rng.below(256),
        rng.below(254) + 1,
        method,
        path,
        status,
        ms,
    );

    Event {
        timestamp: Utc.timestamp_opt(ts_secs, 0).single().unwrap(),
        host: format!("host-{host_id:05}"),
        source: format!("/var/log/{service}.log"),
        sourcetype: (*rng.pick(LEVELS)).to_string(),
        index: "main".into(),
        raw,
        attributes: Some(attributes),
    }
}

// ---------------------------------------------------------------- layouts ---

#[derive(Clone, Copy, PartialEq, Eq)]
enum Layout {
    /// File `i` covers `[i·span, (i+1)·span)`. The mature/settled shape: the
    /// merge plan collapses to exactly one run per input.
    Disjoint,
    /// File `i` starts every `span/2`, so any instant is covered by ~2 files —
    /// a realistic leading edge with several ingesters.
    Staggered,
    /// Every file covers the same span. The pathological cluster: the plan
    /// degenerates to many short runs and both paths must interleave per row.
    Overlapping,
}

impl Layout {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "disjoint" => Some(Self::Disjoint),
            "staggered" => Some(Self::Staggered),
            "overlapping" => Some(Self::Overlapping),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Disjoint => "disjoint",
            Self::Staggered => "staggered",
            Self::Overlapping => "overlapping",
        }
    }

    /// Start offset (seconds) of file `i`'s time range.
    fn start_of(self, i: usize, span: i64) -> i64 {
        match self {
            Self::Disjoint => i as i64 * span,
            Self::Staggered => i as i64 * (span / 2),
            Self::Overlapping => 0,
        }
    }
}

// ------------------------------------------------------------------ paths ---

#[derive(Clone, Copy, PartialEq, Eq)]
enum MergePath {
    /// `merge_file_slice_streaming` — K open decoders, heap pop per row,
    /// `interleave_record_batch` per output batch. Today's default for any bin
    /// at or under the fan-in cap, i.e. essentially all of them.
    Slice,
    /// `merge_files_page_bounded` — RLE run plan, whole runs emitted as
    /// zero-copy slices. Today reached only above the fan-in cap.
    Page,
    /// Whole-bin in-RAM concat + full re-sort. Today's path for small bins.
    InRam,
}

impl MergePath {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "slice" => Some(Self::Slice),
            "page" => Some(Self::Page),
            "inram" => Some(Self::InRam),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Slice => "slice-streaming",
            Self::Page => "page-bounded",
            Self::InRam => "inram-sort",
        }
    }

    /// The dispatch outcome this arm is steering `recluster_files` toward. The
    /// arm asserts against it: an arm that silently fell back to a different
    /// implementation would report a difference of zero and read as "the paths
    /// cost the same", which is the exact shape of a measurement that looks
    /// like data and isn't.
    fn expected_kind(self) -> MergePathKind {
        match self {
            Self::Slice => MergePathKind::SliceStreaming,
            Self::Page => MergePathKind::PageBounded,
            Self::InRam => MergePathKind::InRamSort,
        }
    }

    /// Steer `recluster_files_with`'s dispatch to this path. Explicit options
    /// per arm, not the process-global `SIGLAKE_RECLUSTER_*` knobs: arms run
    /// back to back in one process, and a test binary that `set_var`s races
    /// its own parallel tests.
    fn merge_options(self, files: usize) -> ReclusterMergeOptions {
        match self {
            Self::Slice => ReclusterMergeOptions {
                force_streaming: Some(true),
                // Fan-in above the bin size keeps the bin on the slice path.
                merge_fanin: Some(files + 8),
                ..ReclusterMergeOptions::default()
            },
            Self::Page => ReclusterMergeOptions {
                force_streaming: Some(true),
                // Fan-in below the bin size routes to the page-bounded plan
                // merge (the tiered path stays off — it is opt-in).
                merge_fanin: Some(2),
                ..ReclusterMergeOptions::default()
            },
            Self::InRam => ReclusterMergeOptions {
                force_streaming: Some(false),
                inram_max_bytes: Some(65_536 * 1024 * 1024),
                inram_max_rows: Some(1_000_000_000),
                ..ReclusterMergeOptions::default()
            },
        }
    }
}

// ------------------------------------------------------------------ report --

struct ArmResult {
    layout: Layout,
    path: MergePath,
    files_in: usize,
    files_out: usize,
    rows: usize,
    bytes_in: u64,
    bytes_out: u64,
    row_groups_in: usize,
    wall_secs: f64,
    /// Nanos attributed to awaiting inputs / merge logic / encode+upload.
    stage_nanos: (u64, u64, u64),
    /// Row groups the plan would let a verbatim copy serve, and the total.
    /// Zero/zero on paths that build no plan.
    copyable_row_groups: u64,
    total_row_groups: u64,
    /// Total time spent fetching chunk inputs, and the part of it that actually
    /// stalled the merge. The gap is what chunk pipelining hid behind encode —
    /// the only direct measure of whether the prefetch earns its memory.
    /// Zero on paths that do not pipeline.
    fetch_gross: u64,
    fetch_stalled: u64,
    /// Peak process RSS observed WHILE THIS ARM's merge ran.
    ///
    /// Process-wide and monotonic within a process, so it is only trustworthy
    /// when the process runs ONE arm: with six arms in sequence, every arm after
    /// the first inherits the high-water mark of the ones before it and the
    /// column reads as flat. Restrict BENCH_LAYOUTS and BENCH_PATHS to a single
    /// arm to get a number that means anything — measuring memory was what
    /// single-sample readings of this got wrong on 2026-08-27, when the same
    /// build read 3,511 MB and 4,097 MB on two runs.
    peak_rss: u64,
}

impl ArmResult {
    fn rows_per_sec(&self) -> f64 {
        self.rows as f64 / self.wall_secs.max(f64::EPSILON)
    }

    /// Stage shares of the *attributed* time. This is deliberately not a share
    /// of `wall_secs`: the merge is only part of `recluster_files`, which also
    /// loads the table, commits, and rebuilds indexes.
    fn stage_shares(&self) -> (f64, f64, f64) {
        let (i, a, w) = self.stage_nanos;
        let total = (i + a + w) as f64;
        if total <= 0.0 {
            return (0.0, 0.0, 0.0);
        }
        (i as f64 / total, a as f64 / total, w as f64 / total)
    }

    fn attributed_secs(&self) -> f64 {
        let (i, a, w) = self.stage_nanos;
        (i + a + w) as f64 / 1e9
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

// ------------------------------------------------------------------- bench --

/// Build a table of `files` data files in `layout`, then merge them all in one
/// bin through `path`, timing only the merge.
/// Append `files` data files in `layout`. Each file spans one hour of event
/// time with rows spread evenly across it, so the timestamp column has
/// realistic run structure rather than a single repeated value. Seeded per file
/// so every arm sees byte-identical input.
async fn build_corpus(ice: &IcebergContext, layout: Layout, files: usize, rows_per_file: usize) {
    const SPAN_SECS: i64 = 3600;
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    for f in 0..files {
        let mut rng = Lcg(0x5EED_0000_u64 ^ (f as u64).wrapping_mul(0x9E37_79B9));
        let start = base + layout.start_of(f, SPAN_SECS);
        let batch: Vec<Event> = (0..rows_per_file)
            .map(|r| {
                let ts = start + (r as i64 * SPAN_SECS) / rows_per_file.max(1) as i64;
                gen_event(&mut rng, ts)
            })
            .collect();
        ice.append_events(&batch).await.expect("append events");
    }
}

/// Peak resident-set bytes sampled while `f` runs, alongside its value. Reads
/// `/proc/self/statm`, so Linux-only; returns a zero peak elsewhere. Peak RSS is
/// what decides a safe `SIGLAKE_COMPACTOR_BIN_CONCURRENCY` default, and it is
/// not something the wall-clock numbers can tell us.
async fn with_peak_rss<T, F: std::future::Future<Output = T>>(f: F) -> (T, u64) {
    let peak = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let peak = peak.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let page = 4096u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                if let Ok(s) = std::fs::read_to_string("/proc/self/statm") {
                    if let Some(rss) = s
                        .split_whitespace()
                        .nth(1)
                        .and_then(|v| v.parse::<u64>().ok())
                    {
                        peak.fetch_max(rss * page, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
    };
    let out = f.await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = sampler.await;
    (out, peak.load(std::sync::atomic::Ordering::Relaxed))
}

async fn run_arm(
    layout: Layout,
    path: MergePath,
    files: usize,
    rows_per_file: usize,
    snapshotter: &Snapshotter,
) -> ArmResult {
    let merge = path.merge_options(files);

    let tmp = tempfile::tempdir().expect("tempdir");
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .expect("open context");

    build_corpus(&ice, layout, files, rows_per_file).await;

    let ident = ice.events_table_ident().clone();
    let live = ice.live_data_files(&ident).await.expect("live files");
    let bytes_in: u64 = live.iter().map(|f| f.file_size_in_bytes()).sum();
    let expected_rows: u64 = live.iter().map(|f| f.record_count()).sum();
    let files_in = live.len();
    let row_groups_in = count_row_groups(&live);

    // Counters are cumulative for the process; diff around the merge so the
    // arm reports only its own work (the corpus appends emit metrics too).
    let before = snapshotter.snapshot().into_vec();
    let start = std::time::Instant::now();
    let (stats, peak_rss) =
        with_peak_rss(ice.recluster_files_with(&ident, live, BLOOM_FILTER_COLUMNS, &merge)).await;
    let stats = stats.expect("recluster");
    let wall_secs = start.elapsed().as_secs_f64();
    let after = snapshotter.snapshot().into_vec();

    let stage = |name: &str| {
        counter_sum(
            &after,
            "siglake_compactor_merge_stage_nanos_total",
            Some(("stage", name)),
        )
        .saturating_sub(counter_sum(
            &before,
            "siglake_compactor_merge_stage_nanos_total",
            Some(("stage", name)),
        ))
    };
    let delta = |name: &str| {
        counter_sum(&after, name, None).saturating_sub(counter_sum(&before, name, None))
    };
    let stage_nanos = (stage("input"), stage("assemble"), stage("write"));
    let fetch_gross = delta("siglake_compactor_merge_fetch_gross_nanos_total");
    let fetch_stalled = delta("siglake_compactor_merge_fetch_stalled_nanos_total");
    let copyable_row_groups = delta("siglake_compactor_merge_rowgroups_copyable_total");
    let total_row_groups = delta("siglake_compactor_merge_rowgroups_total");

    assert_eq!(
        stats.rows as u64,
        expected_rows,
        "{} / {}: merge must conserve rows",
        layout.label(),
        path.label()
    );
    assert_eq!(
        stats.merge_path,
        Some(path.expected_kind()),
        "{} / {}: arm did not take the merge path it is measuring",
        layout.label(),
        path.label()
    );
    assert_eq!(
        stats.bytes_in, bytes_in,
        "reported input bytes must match the bin's manifest sizes"
    );

    let out = ice.live_data_files(&ident).await.expect("live files after");
    let bytes_out = stats.bytes_out;

    // A fast path that emits mis-ordered rows would be worthless, and the
    // row-count guard inside `recluster_files` cannot see order. Check the
    // declared sort invariant actually holds on the output of every arm.
    assert_sorted_ascending(&out, layout, path);

    ArmResult {
        layout,
        path,
        files_in,
        files_out: out.len(),
        rows: stats.rows,
        bytes_in,
        bytes_out,
        row_groups_in,
        wall_secs,
        stage_nanos,
        copyable_row_groups,
        total_row_groups,
        fetch_gross,
        fetch_stalled,
        peak_rss,
    }
}

fn local_path(df: &iceberg::spec::DataFile) -> String {
    df.file_path().trim_start_matches("file://").to_string()
}

fn count_row_groups(files: &[iceberg::spec::DataFile]) -> usize {
    files
        .iter()
        .map(|df| {
            let bytes = std::fs::read(local_path(df)).expect("read data file");
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                bytes::Bytes::from(bytes),
            )
            .expect("parquet reader")
            .metadata()
            .num_row_groups()
        })
        .sum()
}

/// Every merge output must be globally ascending in `timestamp` (the declared
/// sort order for a freshly created table), across files as well as within one.
fn assert_sorted_ascending(files: &[iceberg::spec::DataFile], layout: Layout, path: MergePath) {
    let mut all: Vec<i64> = Vec::new();
    let mut sorted_paths: Vec<String> = files.iter().map(local_path).collect();
    sorted_paths.sort();
    for p in sorted_paths {
        let bytes = std::fs::read(&p).expect("read output file");
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(bytes),
        )
        .expect("parquet reader")
        .build()
        .expect("build reader");
        for rb in reader {
            let rb = rb.expect("batch");
            let idx = rb
                .schema()
                .index_of(siglake_core::nanos_source_column(
                    rb.schema().as_ref(),
                    "timestamp",
                ))
                .expect("timestamp column");
            let col = siglake_core::column_nanos(rb.column(idx)).unwrap();
            all.extend((0..rb.num_rows()).map(|r| col.value(r)));
        }
    }
    let violations = all.windows(2).filter(|w| w[0] > w[1]).count();
    assert_eq!(
        violations,
        0,
        "{} / {}: merge output must be ascending in timestamp",
        layout.label(),
        path.label()
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf report; run with --ignored --nocapture"]
async fn report_compaction_throughput() {
    let files = env_usize("BENCH_FILES", 12);
    let rows_per_file = env_usize("BENCH_ROWS_PER_FILE", 100_000);
    // Row-group target (cross-review F-6): the writer reads
    // `SIGLAKE_PARQUET_TARGET_ROW_GROUP_BYTES` itself, so set it in the shell
    // that launches the bench (this binary used to forward a `BENCH_ROW_GROUP_MB`
    // alias into it with `set_var`, which the harness's parallel test threads
    // race). Note `MIN_ROW_GROUP_ROWS` = 128Ki clamps it from below, so
    // `rows_per_file` must exceed a few times that for a file to hold more than
    // one row group — which is what production files (~2.3M rows) look like,
    // and what the copy census needs to be meaningful for partially-overlapping
    // layouts.
    let layouts: Vec<Layout> = std::env::var("BENCH_LAYOUTS")
        .unwrap_or_else(|_| "disjoint,staggered,overlapping".into())
        .split(',')
        .filter_map(Layout::parse)
        .collect();
    let paths: Vec<MergePath> = std::env::var("BENCH_PATHS")
        .unwrap_or_else(|_| "slice,page".into())
        .split(',')
        .filter_map(MergePath::parse)
        .collect();

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    // Ignore the error: a recorder may already be installed by another test in
    // this binary. There is only one test here, so this always succeeds today.
    let _ = metrics::set_global_recorder(recorder);

    println!(
        "\ncompaction bench: {files} files x {rows_per_file} rows = {} rows/arm\n",
        files * rows_per_file
    );

    let mut results: Vec<ArmResult> = Vec::new();
    for &layout in &layouts {
        for &path in &paths {
            results.push(run_arm(layout, path, files, rows_per_file, &snapshotter).await);
        }
    }

    println!(
        "{:<12} {:<16} {:>6} {:>4} {:>11} {:>9} {:>9} {:>8} {:>11} {:>7} {:>9}",
        "layout",
        "path",
        "rgs_in",
        "out",
        "rows",
        "in_MB",
        "out_MB",
        "wall_s",
        "rows/s",
        "vs",
        "peak_MB"
    );
    for r in &results {
        // Baseline for the ratio column is this layout's slice-streaming arm —
        // today's production path — so the column reads as "what changing the
        // dispatch would buy".
        let baseline = results
            .iter()
            .find(|b| b.layout == r.layout && b.path == MergePath::Slice)
            .map(|b| b.rows_per_sec());
        let vs = match baseline {
            Some(b) if r.path != MergePath::Slice => format!("{:.2}x", r.rows_per_sec() / b),
            _ => "-".into(),
        };
        println!(
            "{:<12} {:<16} {:>6} {:>4} {:>11} {:>9.1} {:>9.1} {:>8.2} {:>11.0} {:>7} {:>9.0}",
            r.layout.label(),
            r.path.label(),
            r.row_groups_in,
            r.files_out,
            r.rows,
            mb(r.bytes_in),
            mb(r.bytes_out),
            r.wall_secs,
            r.rows_per_sec(),
            vs,
            mb(r.peak_rss),
        );
    }
    println!(
        "\nfiles_in was {} for every arm",
        results.first().map(|r| r.files_in).unwrap_or(0)
    );

    // Where the merge time actually goes. `attrib_s` is the merge itself;
    // `wall_s` above additionally covers table load, commit and index rebuild,
    // so the gap between them is itself informative.
    println!("\nstage attribution (share of merge time):");
    println!(
        "{:<12} {:<16} {:>9} {:>8} {:>10} {:>8} {:>18}",
        "layout", "path", "attrib_s", "input%", "assemble%", "write%", "copyable_rgs"
    );
    for r in &results {
        let (input, assemble, write) = r.stage_shares();
        let copyable = if r.total_row_groups > 0 {
            format!(
                "{}/{} ({:.0}%)",
                r.copyable_row_groups,
                r.total_row_groups,
                100.0 * r.copyable_row_groups as f64 / r.total_row_groups as f64
            )
        } else {
            "n/a".into()
        };
        println!(
            "{:<12} {:<16} {:>9.2} {:>8.1} {:>10.1} {:>8.1} {:>18}",
            r.layout.label(),
            r.path.label(),
            r.attributed_secs(),
            input * 100.0,
            assemble * 100.0,
            write * 100.0,
            copyable,
        );
    }

    // Chunk-pipelining effectiveness. `hidden` is fetch time that overlapped
    // encode and therefore never cost wall-clock; at prefetch=1 it must be ~0.
    if results.iter().any(|r| r.fetch_gross > 0) {
        println!("\npipelining (page-bounded only):");
        println!(
            "{:<12} {:<16} {:>11} {:>11} {:>9}",
            "layout", "path", "gross_s", "stalled_s", "hidden%"
        );
        for r in results.iter().filter(|r| r.fetch_gross > 0) {
            println!(
                "{:<12} {:<16} {:>11.2} {:>11.2} {:>9.1}",
                r.layout.label(),
                r.path.label(),
                r.fetch_gross as f64 / 1e9,
                r.fetch_stalled as f64 / 1e9,
                100.0 * (1.0 - r.fetch_stalled as f64 / r.fetch_gross as f64),
            );
        }
    }
    println!();
}

// ------------------------------------------------------- bin parallelism ----

/// Bin-level parallelism inside one leveled pass.
///
/// A leveled pass merges its bins in a `for` loop. The 2026-08-05 attribution
/// found ~87% of a merge is CPU-bound Parquet encode, so a sequential pass uses
/// roughly one core regardless of how many the compactor has — the 1TB round ran
/// on a 16-vCPU box at ~47% duty cycle. Bins are pairwise file-disjoint and each
/// commits its own `rewrite_files`, so they can run concurrently;
/// `SIGLAKE_COMPACTOR_BIN_CONCURRENCY` bounds how many in production. Here each
/// arm pins it through `LeveledPassOptions::bin_concurrency` instead, so the
/// arms cannot race each other (or this binary's other test) on the env var.
///
/// This arm builds a corpus wide enough that the packer emits several bins, then
/// runs the *same* pass at each concurrency, reporting wall time and peak RSS —
/// the latter is what decides a safe default.
///
/// ```text
/// cargo test --release -p siglake-storage --test compaction_throughput -- \
///     --ignored --nocapture report_bin_parallelism
/// ```
/// Knobs: `BENCH_PAR_FILES` (48), `BENCH_PAR_ROWS_PER_FILE` (60_000),
/// `BENCH_CONCURRENCIES` (`1,2,4,8`).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf report; run with --ignored --nocapture"]
async fn report_bin_parallelism() {
    let files = env_usize("BENCH_PAR_FILES", 24);
    let rows_per_file = env_usize("BENCH_PAR_ROWS_PER_FILE", 300_000);
    let files_per_bin = env_usize("BENCH_FILES_PER_BIN", 3);
    let concurrencies: Vec<usize> = std::env::var("BENCH_CONCURRENCIES")
        .unwrap_or_else(|_| "1,2,4,8".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    println!(
        "\nbin parallelism: {files} files x {rows_per_file} rows = {} rows/arm\n",
        files * rows_per_file
    );
    println!(
        "{:>6} {:>6} {:>11} {:>9} {:>11} {:>7} {:>10}",
        "conc", "bins", "rows", "wall_s", "rows/s", "vs", "peak_MB"
    );

    let mut baseline: Option<f64> = None;
    for &conc in &concurrencies {
        let tmp = tempfile::tempdir().expect("tempdir");
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .expect("open context");
        // Disjoint files: the settled shape, and the one where the packer
        // produces many independent time-adjacent bins.
        build_corpus(&ice, Layout::Disjoint, files, rows_per_file).await;

        let ident = ice.events_table_ident().clone();
        let before = ice.live_data_files(&ident).await.expect("live files");
        let rows_before: u64 = before.iter().map(|f| f.record_count()).sum();

        // Bins seal at a time gap once they reach the level's byte target, so
        // the L0 target is what decides bin size. Derive it from the corpus's
        // actual file size (~`files_per_bin` files per bin) rather than a
        // constant: a target below one file's size makes every bin a singleton
        // and singletons are dropped, leaving nothing to parallelize.
        let mean_file_bytes =
            before.iter().map(|f| f.file_size_in_bytes()).sum::<u64>() / before.len().max(1) as u64;
        let l0_target = mean_file_bytes * files_per_bin as u64;
        // `max_total_files` for the pass is `max_fanin.min(max_files_per_pass)`,
        // so both must exceed the file count or the pass stops early.
        let levels = LevelPolicy {
            level_ceilings: vec![l0_target, l0_target * 8, l0_target * 64],
            trigger_files: 2,
            max_fanin: 256,
            ..LevelPolicy::default()
        };
        let policy = ReclusterPolicy {
            max_bins_per_pass: 64,
            max_files_per_pass: 256,
            max_pass_bytes: l0_target * 4,
            max_pass_rows: (rows_per_file * (files_per_bin + 1)) as u64,
            ..ReclusterPolicy::default()
        };

        // NOTE: CAS contention is deliberately NOT reported here. This fixture
        // cannot reproduce it — 7 bins over ~12s against a local sqlite catalog
        // barely overlap, and an A/B with the commit lock disabled measured
        // identical stale-base counts. The phenomenon is a property of scale
        // (53-bin passes, Postgres RDS, minutes-long commits), so it is
        // measured on the round via siglake_iceberg_commit_{attempts,stale_base}
        // rather than pretended at here.
        let start = std::time::Instant::now();
        let (stats, peak) = with_peak_rss(ice.recluster_pass_leveled(
            &ident,
            BLOOM_FILTER_COLUMNS,
            &levels,
            policy,
            &LeveledPassOptions {
                bin_concurrency: Some(conc),
                ..LeveledPassOptions::default()
            },
        ))
        .await;
        let wall = start.elapsed().as_secs_f64();
        let stats = stats.expect("leveled pass");

        // Every bin must still conserve rows, and the pass as a whole must
        // leave the table's row count untouched — a concurrency bug that
        // dropped or double-counted a bin would show up here, not in the timing.
        let after = ice.live_data_files(&ident).await.expect("live files after");
        let rows_after: u64 = after.iter().map(|f| f.record_count()).sum();
        assert_eq!(
            rows_before, rows_after,
            "concurrency {conc}: pass must conserve the table's rows"
        );
        let merged_rows: usize = stats.iter().map(|s| s.rows).sum();
        assert!(
            stats.len() >= 2,
            "concurrency {conc}: need multiple bins to measure parallelism, got {} \
             (live files before={} after={})",
            stats.len(),
            before.len(),
            after.len()
        );

        let rate = merged_rows as f64 / wall.max(f64::EPSILON);
        let vs = match baseline {
            Some(b) => format!("{:.2}x", rate / b),
            None => {
                baseline = Some(rate);
                "-".into()
            }
        };
        println!(
            "{:>6} {:>6} {:>11} {:>9.2} {:>11.0} {:>7} {:>10.0}",
            conc,
            stats.len(),
            merged_rows,
            wall,
            rate,
            vs,
            peak as f64 / (1024.0 * 1024.0),
        );
    }
    println!();
}
