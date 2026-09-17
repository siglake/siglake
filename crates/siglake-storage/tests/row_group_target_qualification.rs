//! What would a smaller compactor row-group target buy, and what would it cost?
//!
//! #4754 made `SIGLAKE_PARQUET_TARGET_ROW_GROUP_BYTES` /
//! `IcebergTuning::target_row_group_bytes` reach the merge, re-cluster and
//! delete-rewrite writers without changing the 256 MiB default
//! (`TARGET_ROW_GROUP_UNCOMPRESSED_BYTES`). #4772 asks whether the packaged
//! compactor — `memory: 1Gi` in `deploy/helm/siglake/values.yaml` — should ask
//! for less, and the only evidence on the table was net heap growth over one
//! delete fixture three orders of magnitude smaller than a cold-target file
//! (`delete_task_size_gate.rs::measure_peak_against_row_group_target`).
//!
//! This binary measures ONE arm of that question per process, over a
//! corpus-shaped bin taken through the compactor's own merge:
//!
//! * **what the target buys** — peak live heap and peak process RSS across the
//!   merge, which are different numbers (see below);
//! * **what it costs on write** — output row groups, bloom bytes per row group
//!   and as a share of the file, Parquet footer bytes, compression ratio,
//!   merge wall time;
//! * **what it costs (or buys) on query** — settled scan attribution for a
//!   needle that lives in one time window, a timestamp range, and a full-file
//!   predicate scan.
//!
//! THREE BYTE MEASURES, NOT ONE. The card asks for them kept apart, because
//! they differ by more than 2x on the same rows:
//!
//! * *sampled extent* — `ArrayData::get_slice_memory_size` over the rows a
//!   batch spans. This is what the writer's sizing heuristic prices a row with
//!   (`sampled_row_bytes`), so it is what `target / row_bytes` divides.
//! * *allocation slack* — `RecordBatch::get_array_memory_size`, the whole
//!   buffers behind those rows. A buffered row group holds buffers, not
//!   extents, so what a target of N bytes actually holds resident runs above N.
//! * *resident memory* — peak `/proc/self/statm` RSS across the merge. This is
//!   the number a 1Gi container limit is compared against, and it includes the
//!   allocator's retained arenas, the runtime, the compressed input chunks and
//!   the encoder's own buffers — none of which the row-group target moves.
//!
//! ONE ARM PER PROCESS. Peak RSS is process-wide and monotonic, so a second arm
//! in the same process inherits the first arm's high-water mark and reads flat
//! (the same trap `compaction_throughput.rs` documents). The arm is therefore
//! chosen by environment and the sweep is a shell loop:
//!
//! ```text
//! for mb in 0 128 64 32; do
//!   RG_TARGET_MB=$mb cargo test --release -p siglake-storage \
//!     --test row_group_target_qualification -- --ignored --nocapture
//! done
//! ```
//!
//! `RG_TARGET_MB=0` (the default) leaves `target_row_group_bytes` unset, which
//! is the shipped 256 MiB. `RG_HEAP_TRACKING=0` turns off the tracking
//! allocator: its per-allocation atomics are on the merge's hot path, so the
//! wall-clock and RSS columns want a second pass without it. `RG_FILES` (8) and
//! `RG_ROWS_PER_FILE` (250_000) size the bin.
//!
//! COMPACTOR-ONLY. The corpus is appended through the DEFAULT tuning and the
//! arm's target is applied to the context afterwards, so every arm merges a
//! byte-identical bin and only the compactor's writer changes. The flush
//! default is a separate decision and this measurement says nothing about it.
//!
//! The recorded readings and the decision they support are in
//! `docs/DESIGN_row_group_target_qualification.md`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{TimeZone, Utc};
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use siglake_core::Event;
use siglake_storage::iceberg::{
    IcebergContext, IcebergTuning, MergePathKind, ReclusterMergeOptions, BLOOM_FILTER_COLUMNS,
};
use siglake_storage::SiglakeIcebergTableScan;

// ------------------------------------------------------------- allocator ---

/// Peak live heap bytes between [`start_tracking`] and [`peak_tracked`], the
/// same shape `delete_task_size_gate.rs` uses — quoted here so this binary's
/// heap column is comparable with the 80.4/56.5 MB reading that opened #4772.
struct PeakTracking;

static TRACKING: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for PeakTracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && TRACKING.load(Ordering::Relaxed) {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if TRACKING.load(Ordering::Relaxed) {
            // Saturating: tracking starts mid-process, so some of what is freed
            // here was allocated before LIVE existed.
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some(live.saturating_sub(layout.size()))
            });
        }
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: PeakTracking = PeakTracking;

fn start_tracking(enabled: bool) {
    LIVE.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    TRACKING.store(enabled, Ordering::Relaxed);
}

fn peak_tracked() -> usize {
    TRACKING.store(false, Ordering::Relaxed);
    PEAK.load(Ordering::Relaxed)
}

// ------------------------------------------------------------------ corpus --

/// Deterministic LCG (Numerical Recipes constants), as in
/// `compaction_throughput.rs`: a given seed reproduces byte for byte, so every
/// arm merges the same bytes.
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
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-southeast-1",
];
const SERVICES: &[&str] = &[
    "checkout",
    "catalog",
    "auth",
    "payments",
    "search",
    "inventory",
    "shipping",
    "gateway",
];
const METHODS: &[&str] = &["GET", "GET", "GET", "POST", "PUT", "DELETE"];
const PATHS: &[&str] = &[
    "/",
    "/api/v1/cart",
    "/api/v1/products",
    "/api/v1/checkout",
    "/healthz",
    "/api/v1/search",
];

/// The needle host. It exists on `NEEDLE_ROWS` consecutive rows of ONE input
/// file, so after the timestamp-ordered merge every needle row is inside one
/// narrow time window — the shape where a smaller row group can prune more.
const NEEDLE_HOST: &str = "host-needle";
const NEEDLE_ROWS: usize = 256;
/// Which input file carries the needle, and where in it. Mid-file so the
/// needle's window is interior to the merged output, not at a file boundary.
const NEEDLE_FILE: usize = 3;
/// The needle's own time window. Each input file spans one hour from
/// 2026-06-01T00:00:00Z, and the needle sits at the midpoint of file 3, so
/// these bracket it whatever `RG_ROWS_PER_FILE` is: the range shape and the
/// host shape then ask about the same part of the merged output, one through
/// statistics and one through the row-group blooms.
const NEEDLE_WINDOW_START: &str = "2026-06-01 03:30:00";
const NEEDLE_WINDOW_END: &str = "2026-06-01 03:31:00";

/// One corpus-shaped event, mirroring the AWS benchmark corpus: 2000 hosts, a
/// templated message, and a residual `attributes` JSON with nested
/// agent/cloud/http/trace keys. The high-entropy trace ids are what keep the
/// compression ratio — and therefore the bytes a row group holds — realistic.
fn gen_event(rng: &mut Lcg, ts_secs: i64, host: Option<&str>) -> Event {
    let region = rng.pick(REGIONS);
    let service = rng.pick(SERVICES);
    let method = rng.pick(METHODS);
    let path = rng.pick(PATHS);
    let status = [200u32, 200, 200, 201, 204, 301, 400, 404, 500][rng.below(9) as usize];
    let ms = rng.below(4000) + 1;
    let host_id = rng.below(2000) + 1;
    let widen = |v: u32| (v as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let trace_hi = widen(rng.next_u32());
    let trace_lo = widen(rng.next_u32());
    let span_id = widen(rng.next_u32());

    Event {
        timestamp: Utc.timestamp_opt(ts_secs, 0).single().unwrap(),
        host: host
            .map(str::to_string)
            .unwrap_or(format!("host-{host_id:05}")),
        source: format!("/var/log/{service}.log"),
        sourcetype: (*rng.pick(LEVELS)).to_string(),
        index: "main".into(),
        raw: format!("{method} {path} {status} in {ms}ms service={service}"),
        attributes: Some(format!(
            r#"{{"agent":{{"name":"vector","version":"8.1{}.0","id":"agent-{:05}"}},"cloud":{{"provider":"aws","region":"{}","availability_zone":"{}{}","account_id":"{:012}"}},"http":{{"method":"{}","path":"{}","status_code":{},"response_time_ms":{}}},"trace":{{"id":"{trace_hi:016x}{trace_lo:016x}","span_id":"{span_id:016x}"}}}}"#,
            rng.below(4),
            rng.below(5000) + 1,
            region,
            region,
            ["a", "b", "c"][rng.below(3) as usize],
            rng.below(999_999_999) as u64 * 1000,
            method,
            path,
            status,
            ms,
        )),
    }
}

/// Append `files` disjoint hour-spans of `rows_per_file` rows each. Disjoint is
/// the settled shape: the merge plan collapses to one run per input, so the
/// arm measures the writer rather than the interleave.
async fn build_corpus(ice: &IcebergContext, files: usize, rows_per_file: usize) {
    const SPAN_SECS: i64 = 3600;
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();
    let needle_at = rows_per_file / 2;

    for f in 0..files {
        let mut rng = Lcg(0x5EED_0000_u64 ^ (f as u64).wrapping_mul(0x9E37_79B9));
        let start = base + f as i64 * SPAN_SECS;
        let batch: Vec<Event> = (0..rows_per_file)
            .map(|r| {
                let ts = start + (r as i64 * SPAN_SECS) / rows_per_file.max(1) as i64;
                let needle = f == NEEDLE_FILE && (needle_at..needle_at + NEEDLE_ROWS).contains(&r);
                gen_event(&mut rng, ts, needle.then_some(NEEDLE_HOST))
            })
            .collect();
        ice.append_events(&batch).await.expect("append events");
    }
}

// ----------------------------------------------------------- file anatomy ---

/// What one Parquet file is made of, read out of its own footer.
#[derive(Default, Debug, Clone)]
struct Anatomy {
    files: usize,
    row_groups: usize,
    rows: u64,
    file_bytes: u64,
    /// Sum of column-chunk compressed sizes: the payload.
    data_bytes: u64,
    /// Sum of column-chunk uncompressed sizes — Parquet's own "uncompressed",
    /// which is ENCODED (dictionary, RLE) and so is far below the Arrow
    /// decoded extent the target is denominated in. Quoting one for the other
    /// is the easiest way to misread this measurement.
    encoded_bytes: u64,
    bloom_bytes: u64,
    /// Thrift footer + the 8-byte length/magic trailer, per file.
    footer_bytes: u64,
    min_row_group_rows: u64,
    max_row_group_rows: u64,
    /// Bytes of each footer key-value entry, by key. The siglake footers —
    /// group counts, time buckets, the per-row-group raw trigram bloom — are
    /// stored here, and they are the per-row-group tax a smaller target pays.
    kv_bytes: std::collections::BTreeMap<String, u64>,
}

impl Anatomy {
    fn rows_per_group(&self) -> f64 {
        self.rows as f64 / self.row_groups.max(1) as f64
    }

    /// Everything that is not column-chunk payload: footers, blooms, page and
    /// column indexes. This is the per-row-group tax a smaller target pays.
    fn overhead_bytes(&self) -> u64 {
        self.file_bytes.saturating_sub(self.data_bytes)
    }
}

fn anatomy_of(paths: &[String]) -> Anatomy {
    let mut a = Anatomy {
        min_row_group_rows: u64::MAX,
        ..Default::default()
    };
    for path in paths {
        let bytes = std::fs::read(path).expect("read parquet");
        a.files += 1;
        a.file_bytes += bytes.len() as u64;
        // The trailer is `<4-byte metadata len><"PAR1">`; the metadata length
        // is the only exact read of the footer's encoded size.
        let len = bytes.len();
        let footer_len = u32::from_le_bytes(bytes[len - 8..len - 4].try_into().unwrap()) as u64;
        a.footer_bytes += footer_len + 8;
        let md = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .expect("parquet reader")
            .metadata()
            .clone();
        for kv in md
            .file_metadata()
            .key_value_metadata()
            .into_iter()
            .flatten()
        {
            *a.kv_bytes.entry(kv.key.clone()).or_default() +=
                kv.value.as_ref().map(|v| v.len() as u64).unwrap_or(0);
        }
        a.row_groups += md.num_row_groups();
        for rg in md.row_groups() {
            a.rows += rg.num_rows() as u64;
            a.min_row_group_rows = a.min_row_group_rows.min(rg.num_rows() as u64);
            a.max_row_group_rows = a.max_row_group_rows.max(rg.num_rows() as u64);
            for col in rg.columns() {
                a.data_bytes += col.compressed_size().max(0) as u64;
                a.encoded_bytes += col.uncompressed_size().max(0) as u64;
                a.bloom_bytes += col.bloom_filter_length().unwrap_or(0).max(0) as u64;
            }
        }
    }
    if a.min_row_group_rows == u64::MAX {
        a.min_row_group_rows = 0;
    }
    a
}

/// The two Arrow byte measures of the same rows, read off the merged output:
/// the extent the writer's sizing prices a row with, and the whole buffers a
/// buffered batch of those rows actually holds.
fn arrow_row_bytes(batch: &RecordBatch) -> (f64, f64) {
    let rows = batch.num_rows().max(1) as f64;
    let extent: usize = batch
        .columns()
        .iter()
        .map(|c| c.to_data().get_slice_memory_size().unwrap_or(0))
        .sum();
    (
        extent as f64 / rows,
        batch.get_array_memory_size() as f64 / rows,
    )
}

// ---------------------------------------------------------------- sampling --

/// Peak resident-set bytes sampled while `f` runs, alongside its value. Linux
/// only (`/proc/self/statm`); zero elsewhere. This is the number the 1Gi limit
/// is compared against — the heap column is not.
async fn with_peak_rss<T, F: std::future::Future<Output = T>>(f: F) -> (T, u64) {
    let peak = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let peak = peak.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(rss_bytes(), Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };
    let out = f.await;
    stop.store(true, Ordering::Relaxed);
    let _ = sampler.await;
    peak.fetch_max(rss_bytes(), Ordering::Relaxed);
    (out, peak.load(Ordering::Relaxed))
}

fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| {
            s.split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok())
        })
        .map(|pages| pages * 4096)
        .unwrap_or(0)
}

// -------------------------------------------------------------- query cost --

/// The scan-attribution counters a request carries, summed over partitions as
/// `summarize_plan_runtime` reads them.
#[derive(Debug, Default, Clone, Copy)]
struct ScanCost {
    files_read: usize,
    row_groups_considered: usize,
    row_groups_read: usize,
    row_groups_pruned_bloom: usize,
    row_groups_pruned_stats: usize,
    rows_pruned_selection: usize,
    object_store_reads: usize,
    decoded_bytes: usize,
    bytes_footer: usize,
    bytes_index: usize,
    bytes_data: usize,
    rows_out: usize,
    millis: f64,
}

fn scan_nodes(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut out = Vec::new();
    plan.apply(|node| {
        if node
            .as_any()
            .downcast_ref::<SiglakeIcebergTableScan>()
            .is_some()
        {
            out.push(node.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    out
}

/// Run `sql` and return its settled scan attribution. Settled, not raw: an
/// early-stopped partition folds its counters on a later poll, and reading
/// before that is the 2026-09-03 `label_filter` artifact (#274).
async fn query_cost(ctx: &datafusion::prelude::SessionContext, sql: &str) -> ScanCost {
    let plan = ctx
        .sql(sql)
        .await
        .expect("plan sql")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let start = Instant::now();
    let mut rows_out = 0usize;
    {
        #[allow(clippy::disallowed_methods)]
        let mut stream = execute_stream(plan.clone(), siglake_storage::bounded_task_context())
            .expect("execute stream");
        while let Some(batch) = stream.next().await {
            rows_out += batch.expect("batch").num_rows();
        }
    }
    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(30)).await;
    assert!(
        settle.complete,
        "scan did not settle for `{sql}`: {settle:?}"
    );
    let millis = start.elapsed().as_secs_f64() * 1000.0;

    let mut cost = ScanCost {
        rows_out,
        millis,
        ..Default::default()
    };
    for node in scan_nodes(&plan) {
        let metrics = node.metrics().expect("scan metrics").aggregate_by_name();
        let sum = |name: &str| {
            metrics
                .sum_by_name(name)
                .map(|m| m.as_usize())
                .unwrap_or_default()
        };
        cost.files_read += sum("files_read");
        cost.row_groups_considered += sum("row_groups_considered");
        cost.row_groups_read += sum("row_groups_read");
        cost.row_groups_pruned_bloom += sum("row_groups_pruned_bloom");
        cost.row_groups_pruned_stats += sum("row_groups_pruned_stats");
        cost.rows_pruned_selection += sum("rows_pruned_selection");
        cost.object_store_reads += sum("object_store_reads");
        cost.decoded_bytes += sum("decoded_bytes");
        cost.bytes_footer += sum("bytes_footer");
        cost.bytes_index += sum("bytes_index");
        cost.bytes_data += sum("bytes_data");
    }
    cost
}

// -------------------------------------------------------------------- arm ---

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// ONE ARM: build the bin at the default target, merge it at this arm's target,
/// then price the output on write and on query. See the module docs for the
/// sweep loop and for what each byte column means.
#[ignore = "measurement; run one arm per process with --ignored --nocapture"]
#[tokio::test(flavor = "multi_thread")]
async fn measure_row_group_target_arm() {
    let target_mb = env_usize("RG_TARGET_MB", 0);
    let files = env_usize("RG_FILES", 8);
    let rows_per_file = env_usize("RG_ROWS_PER_FILE", 250_000);
    let track_heap = env_usize("RG_HEAP_TRACKING", 1) == 1;
    // Parquet-native blooms are default-OFF (they were measured useless and the
    // writer keeps them behind `SIGLAKE_PARQUET_NATIVE_BLOOMS`). `RG_NATIVE_BLOOMS=1`
    // prices the arm as if that decision were reversed, because those filters
    // are per row group and are the one overhead that scales with the count
    // independently of the data.
    let native_blooms = env_usize("RG_NATIVE_BLOOMS", 0) == 1;
    let target_bytes = (target_mb > 0).then(|| target_mb * 1024 * 1024);
    assert!(
        files > NEEDLE_FILE,
        "RG_FILES must exceed {NEEDLE_FILE} or no input file carries the needle"
    );

    let tmp = tempfile::tempdir().expect("tempdir");
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .expect("open context");

    // The bin is written at the DEFAULT target: this arm changes the compactor,
    // not the flush path, so every arm merges byte-identical inputs.
    build_corpus(&ice, files, rows_per_file).await;

    let ident = ice.events_table_ident().clone();
    let live = ice.live_data_files(&ident).await.expect("live files");
    let input = anatomy_of(&file_paths(&live));
    let expected_rows: u64 = live.iter().map(|f| f.record_count()).sum();

    // Result caches off: each query below runs once against a cold context, and
    // a snapshot-keyed hit would price the second arm's query at zero.
    let ice = ice.with_tuning(IcebergTuning {
        target_row_group_bytes: target_bytes,
        native_blooms: Some(native_blooms),
        result_caches: Some(false),
        ..Default::default()
    });
    // The bin is one merge through the slice-streaming path — the compactor's
    // default for any bin at or under the fan-in cap, which is nearly all of
    // them.
    let merge = ReclusterMergeOptions {
        force_streaming: Some(true),
        merge_fanin: Some(files + 8),
        ..Default::default()
    };

    // RSS is monotonic and the corpus build already ran in this process, so the
    // merge's own resident cost is the peak ABOVE this baseline, not the peak.
    let rss_before = rss_bytes();
    start_tracking(track_heap);
    let start = Instant::now();
    let (stats, peak_rss) =
        with_peak_rss(ice.recluster_files_with(&ident, live, BLOOM_FILTER_COLUMNS, &merge)).await;
    let merge_secs = start.elapsed().as_secs_f64();
    let peak_heap = peak_tracked();
    let stats = stats.expect("recluster");

    assert_eq!(
        stats.merge_path,
        Some(MergePathKind::SliceStreaming),
        "the arm did not take the merge path it is measuring"
    );
    assert_eq!(stats.rows as u64, expected_rows, "merge must conserve rows");

    let out = ice.live_data_files(&ident).await.expect("live files after");
    let output = anatomy_of(&file_paths(&out));

    // The two Arrow measures of the SAME rows, off the merged output: the
    // extent the writer's heuristic divides the target by, and the buffers a
    // batch of those rows holds.
    let ctx = siglake_storage::session_context_with_target_partitions(Some(4));
    ice.register_with_datafusion(&ctx).await.expect("register");
    let sample = ctx
        .sql("SELECT * FROM events LIMIT 8192")
        .await
        .expect("sample plan")
        .collect()
        .await
        .expect("sample rows");
    let (extent_per_row, alloc_per_row) = arrow_row_bytes(&sample[0]);
    drop(sample);

    println!(
        "\n== arm target={} files_in={} rows={} heap_tracking={} native_blooms={native_blooms}",
        target_bytes
            .map(|b| format!("{} MiB", b / (1024 * 1024)))
            .unwrap_or_else(|| "default (256 MiB)".into()),
        input.files,
        output.rows,
        track_heap,
    );
    println!(
        "   input      files={} rgs={} rows/rg={:.0} bytes={:.1} MB",
        input.files,
        input.row_groups,
        input.rows_per_group(),
        mb(input.file_bytes),
    );
    println!(
        "   output     files={} rgs={} rows/rg={:.0} (min {} max {}) bytes={:.1} MB",
        output.files,
        output.row_groups,
        output.rows_per_group(),
        output.min_row_group_rows,
        output.max_row_group_rows,
        mb(output.file_bytes),
    );
    println!(
        "   per row    arrow_extent={extent_per_row:.0} B  arrow_alloc={alloc_per_row:.0} B  \
         parquet_encoded={:.0} B  compressed={:.0} B",
        output.encoded_bytes as f64 / output.rows.max(1) as f64,
        output.data_bytes as f64 / output.rows.max(1) as f64,
    );
    println!(
        "   per rg     extent={:.1} MB  encoded={:.1} MB  compressed={:.1} MB  \
         bloom={:.0} KB  (compression {:.2}x)",
        output.rows_per_group() * extent_per_row / (1024.0 * 1024.0),
        mb(output.encoded_bytes) / output.row_groups.max(1) as f64,
        mb(output.data_bytes) / output.row_groups.max(1) as f64,
        output.bloom_bytes as f64 / output.row_groups.max(1) as f64 / 1024.0,
        output.encoded_bytes as f64 / output.data_bytes.max(1) as f64,
    );
    println!(
        "   overhead   bloom={:.2} MB ({:.2}%)  footer={:.2} MB ({:.2}%)  \
         non-payload={:.2} MB ({:.2}%)",
        mb(output.bloom_bytes),
        100.0 * output.bloom_bytes as f64 / output.file_bytes.max(1) as f64,
        mb(output.footer_bytes),
        100.0 * output.footer_bytes as f64 / output.file_bytes.max(1) as f64,
        mb(output.overhead_bytes()),
        100.0 * output.overhead_bytes() as f64 / output.file_bytes.max(1) as f64,
    );
    for (key, bytes) in &output.kv_bytes {
        println!(
            "   footer kv  {key:<34} {:>9.1} KB  ({:>7.2} KB/row-group, {:.2}% of file)",
            *bytes as f64 / 1024.0,
            *bytes as f64 / output.row_groups.max(1) as f64 / 1024.0,
            100.0 * *bytes as f64 / output.file_bytes.max(1) as f64,
        );
    }
    println!(
        "   merge      wall={merge_secs:.1}s  rows/s={:.0}  peak_heap={:.1} MB  \
         rss {:.1} -> {:.1} MB (+{:.1})  bytes_in={:.1} MB bytes_out={:.1} MB",
        stats.rows as f64 / merge_secs.max(f64::EPSILON),
        peak_heap as f64 / (1024.0 * 1024.0),
        mb(rss_before),
        mb(peak_rss),
        mb(peak_rss.saturating_sub(rss_before)),
        mb(stats.bytes_in),
        mb(stats.bytes_out),
    );

    // Query side. The needle host lives in one narrow time window of the
    // ordered output, so it is the shape where a smaller row group prunes more;
    // the range is the same question through statistics instead of blooms; the
    // sourcetype predicate hits every row group and prices the per-row-group
    // tax on a scan that prunes nothing.
    //
    // `sum(length(raw))` rather than `count(*)`: a count with an equality on a
    // group-count column is answered from the footer aggregates without opening
    // a row group at all, which prices every arm at zero and measures nothing.
    let shapes: &[(&str, String)] = &[
        (
            "needle host",
            format!("SELECT sum(length(raw)) FROM events WHERE host = '{NEEDLE_HOST}'"),
        ),
        (
            "narrow range (stats)",
            format!(
                "SELECT sum(length(raw)) FROM events WHERE timestamp >= TIMESTAMP \
                 '{NEEDLE_WINDOW_START}' AND timestamp < TIMESTAMP '{NEEDLE_WINDOW_END}'"
            ),
        ),
        (
            "full predicate scan",
            "SELECT sum(length(raw)) FROM events WHERE sourcetype = 'error'".to_string(),
        ),
    ];
    for (label, sql) in shapes {
        let cost = query_cost(&ctx, sql).await;
        println!(
            "   query      {label:<21} rgs {}/{} read (bloom -{} stats -{})  \
             data={:.2} MB footer={:.0} KB index={:.0} KB  decoded={:.2} MB  \
             rows_pruned={}  reads={}  files={}  out_rows={}  {:.1} ms",
            cost.row_groups_read,
            cost.row_groups_considered,
            cost.row_groups_pruned_bloom,
            cost.row_groups_pruned_stats,
            cost.bytes_data as f64 / (1024.0 * 1024.0),
            cost.bytes_footer as f64 / 1024.0,
            cost.bytes_index as f64 / 1024.0,
            cost.decoded_bytes as f64 / (1024.0 * 1024.0),
            cost.rows_pruned_selection,
            cost.object_store_reads,
            cost.files_read,
            cost.rows_out,
            cost.millis,
        );
    }
    println!();
}

fn file_paths(files: &[iceberg::spec::DataFile]) -> Vec<String> {
    files
        .iter()
        .map(|f| f.file_path().trim_start_matches("file://").to_string())
        .collect()
}
