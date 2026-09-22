//! #4182: what a repeat text suite costs when its indexed files outgrow the
//! two text-index caches.
//!
//! The caches sit on both sides of one decode — serialized blob bytes and the
//! parsed index — and only the parsed side serves a warm query. The blob side
//! is therefore a re-parse source: it is read exactly when a parsed entry has
//! been evicted, and a blob whose parsed twin is still resident cannot be read
//! at all. Bounding the two independently let them fall out of step. On the
//! benchmarks' 50G round of 2026-09-14 a `keyword_and_label` plan over 14
//! indexed files read 4.60 GB over 183 index-phase reads where the same plan
//! had read 0.50 GB over 73: every blob was being evicted one execution before
//! the query that wanted it.
//!
//! This file measures that end to end, on a fixed file layout with explicit
//! finite budgets and a fixed query sequence. The fork unit test
//! `a_plan_larger_than_both_caches_stops_refetching_every_blob` carries the
//! same comparison against the pre-change eviction rule; here the arms are the
//! budgets, which is what an operator can actually set.
//!
//! Measured 2026-09-16 by `report_text_index_cache_budget_split` below — six
//! Puffin-indexed files of 400 rows, 31,319 B parsed and 5,577 B serialized
//! each, per steady pass of the same query:
//!
//! | budget | fetches | blob hits | parsed hits | index bytes | retained |
//! |---|---|---|---|---|---|
//! | 3 parsed, no blobs | 6.0 | 0 | 0 | 6,696 | 93,957 |
//! | 3 parsed, 3 blobs (the packaged shape) | 3.5 | 2.5 | 0 | 3,906 | 110,688 |
//! | 6 parsed, no blobs | 0 | 0 | 6.0 | 0 | 187,914 |
//! | 1 parsed, 6 blobs | 0 | 6.0 | 0 | 0 | 64,781 |
//!
//! Two readings. The eviction fix is the second row: the blobs the budget
//! holds are read from memory instead of re-fetched, where before this change
//! the same row fetched all six on every pass. And a budget that covers the
//! plan is what removes the fetches altogether — as serialized copies it costs
//! a third of what parsed indexes cost, at a decode per file per query. Which
//! way to spend the pair is #4102's (parsed sizing) and #4054's (retention),
//! with these numbers as their input; nothing here changes a default.
//!
//! The regression and the measurement read the same process state — one
//! metrics recorder, two process-wide cache budgets and what those caches hold
//! — so they take [`CACHE_TESTS`] and run one at a time, which is what lets the
//! binary run with `--include-ignored` (#5300).

use std::sync::{Mutex, MutexGuard, OnceLock};

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, IcebergTuning, ReclusterMergeOptions};

/// Both tests below own the same process state — the metrics recorder, the two
/// text-index cache budgets and what those caches hold — so they run one at a
/// time, and the recorder is installed once for the binary.
static CACHE_TESTS: Mutex<()> = Mutex::new(());

fn setup() -> (MutexGuard<'static, ()>, &'static Snapshotter) {
    static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();
    let guard = CACHE_TESTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let snapshotter = SNAPSHOTTER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().expect("install debugging recorder");
        snapshotter
    });
    reset_process_state();
    // Every read of a debugging recorder drains it, so this leaves each test
    // with an empty recorder to fill.
    let _ = snapshotter.snapshot();
    (guard, snapshotter)
}

/// Both budgets back to their default resolution and both caches empty.
///
/// Clearing the budgets is not enough on its own: they are enforced on insert,
/// so entries another test admitted stay resident, and the parsed footprint
/// these measurements divide by entry count is process-wide.
fn reset_process_state() {
    iceberg::arrow::clear_text_index_cache_max_bytes();
    iceberg::arrow::clear_text_index_caches();
}

/// Runs one test with this binary's process state to itself, and leaves that
/// state as it was found for the next one.
///
/// A `#[test]` with its own runtime rather than `#[tokio::test]`: the lock has
/// to span the whole measurement, and a std `MutexGuard` held across an
/// `.await` inside an async test is what `clippy::await_holding_lock` exists to
/// catch.
fn serialized<F>(body: impl FnOnce(&'static Snapshotter) -> F)
where
    F: std::future::Future<Output = ()>,
{
    let (_guard, snapshotter) = setup();
    // Current-thread, which is what `#[tokio::test]` gave these measurements
    // before they were serialized: a multi-thread runtime consults the six
    // files' indexes concurrently, and the fixed query sequence these passes
    // count is a sequential one.
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime")
        .block_on(body(snapshotter));
    reset_process_state();
}

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

/// `DebuggingRecorder` reports a counter as the delta since the last snapshot,
/// so every reading here is per phase of the measurement.
fn counter_sum(snapshot: &SnapshotVec, name: &str, phase: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "phase" && label.value() == phase)
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(value) => *value,
            _ => 0,
        })
        .sum()
}

/// Rows with a term on every file and a rarer one on every tenth row, so a
/// query consults every file's index rather than being pruned out of some.
fn rows(count: usize, chunk: usize) -> Vec<Event> {
    use chrono::{Duration, TimeZone, Utc};

    // One day, so every chunk lands in the same partition and a recluster of
    // one chunk is a single-partition rewrite (#4200).
    let base = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    (0..count)
        .map(|row| {
            let rare = if row.is_multiple_of(10) {
                " rareneedle"
            } else {
                ""
            };
            let mut event = Event::now(format!(
                "commonterm{rare} chunk-{chunk:02} row-{row:05} \
                 service-{} host-{}",
                row % 7,
                row % 23
            ));
            event.timestamp = base + Duration::milliseconds((chunk * count + row) as i64);
            event
        })
        .collect()
}

/// One Puffin-indexed file per chunk: append the chunk, then recluster the file
/// it produced on its own. `index_footer_max_bytes: Some(0)` keeps every index
/// out of the Parquet footer, so all of them are blobs this cache pair holds
/// both forms of. `indexed` off is the control — the same rows, no index.
async fn puffin_files_fixture(
    path: &std::path::Path,
    chunks: usize,
    rows_per_chunk: usize,
    indexed: bool,
) -> IcebergContext {
    let ice = IcebergContext::open(path)
        .await
        .unwrap()
        .with_inverted_index(indexed)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(IcebergTuning {
            index_rebuild: Some(indexed),
            index_footer_max_bytes: Some(0),
            ..Default::default()
        });
    let ident = ice.events_table_ident().clone();
    let blooms = ice.events_bloom_columns();
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    let mut reclustered: std::collections::HashSet<String> = std::collections::HashSet::new();
    for chunk in 0..chunks {
        ice.append_events(&rows(rows_per_chunk, chunk))
            .await
            .unwrap();
        if !indexed {
            continue;
        }
        let fresh: Vec<_> = ice
            .live_data_files(&ident)
            .await
            .unwrap()
            .into_iter()
            .filter(|file| !reclustered.contains(file.file_path()))
            .collect();
        for file in &fresh {
            reclustered.insert(file.file_path().to_string());
        }
        let outputs = ice
            .recluster_files_with(
                &ident,
                fresh,
                &bloom_refs,
                &ReclusterMergeOptions {
                    force_streaming: Some(true),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        for file in ice.live_data_files(&ident).await.unwrap() {
            reclustered.insert(file.file_path().to_string());
        }
        assert!(
            outputs.files_added > 0,
            "chunk {chunk} produced no reclustered file to index"
        );
    }
    ice
}

async fn needle_rows(ctx: &datafusion::prelude::SessionContext, term: &str) -> Vec<String> {
    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{term}%'");
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut out: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let raw = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| raw.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    out.sort();
    out
}

/// One pass of the fixed suite: what it paid for its files' indexes, and its
/// rows.
struct Pass {
    /// Blobs read from object storage.
    fetches: u64,
    /// Decodes handed bytes the blob cache still held.
    blob_hits: u64,
    /// Files whose parsed index was still cached, which cost neither.
    parsed_hits: u64,
    index_reads: u64,
    index_bytes: u64,
    elapsed: std::time::Duration,
    rows: Vec<String>,
}

async fn measure_pass(
    ctx: &datafusion::prelude::SessionContext,
    snapshotter: &Snapshotter,
    warehouse: &str,
    term: &str,
) -> Pass {
    let (fetches_before, hits_before) = iceberg::arrow::puffin_blob_fetch_counts();
    let (_, parsed_before) = iceberg::arrow::parsed_inverted_index_cache_stats(warehouse);
    let started = std::time::Instant::now();
    let rows = needle_rows(ctx, term).await;
    let elapsed = started.elapsed();
    let (fetches_after, hits_after) = iceberg::arrow::puffin_blob_fetch_counts();
    let (_, parsed_after) = iceberg::arrow::parsed_inverted_index_cache_stats(warehouse);
    let snapshot = snapshotter.snapshot().into_vec();
    Pass {
        fetches: fetches_after - fetches_before,
        blob_hits: hits_after - hits_before,
        parsed_hits: parsed_after - parsed_before,
        index_reads: counter_sum(&snapshot, "siglake_object_store_reads_total", "index"),
        index_bytes: counter_sum(&snapshot, "siglake_object_store_read_bytes_total", "index"),
        elapsed,
        rows,
    }
}

/// Per-file parsed and blob sizes for this fixture, learned from a warehouse
/// queried under budgets that hold all of it. The measurement warehouse needs
/// its budgets set BEFORE its first query: shrinking a budget does not evict
/// what a cache already holds (the bounds are enforced on insert), so a
/// warehouse that started warm never converges on a smaller one.
struct EntrySizes {
    parsed: usize,
    blob: usize,
}

async fn entry_sizes(tmp: &std::path::Path, chunks: usize, rows_per_chunk: usize) -> EntrySizes {
    // The per-file figures below come off a process-wide footprint, so the
    // sizing warehouse has to be the only thing in it.
    iceberg::arrow::clear_text_index_caches();
    siglake_storage::configure_text_index_caches(siglake_storage::TextIndexCacheConfig {
        parsed_index_max_bytes: u64::MAX / 2,
        puffin_blob_max_bytes: u64::MAX / 2,
    });
    let path = tmp.join("sizing");
    let sizing = puffin_files_fixture(&path, chunks, rows_per_chunk, true).await;
    let ctx = datafusion::prelude::SessionContext::new();
    sizing.register_with_datafusion(&ctx).await.unwrap();
    assert!(!needle_rows(&ctx, "rareneedle").await.is_empty());
    let warehouse = path.to_string_lossy().to_string();
    let (parsed_entries, _) = iceberg::arrow::parsed_inverted_index_cache_stats(&warehouse);
    let (blob_entries, blob_bytes, _) = iceberg::arrow::puffin_blob_cache_stats(&warehouse);
    let footprint = iceberg::arrow::parsed_inverted_index_cache_footprint();
    assert_eq!(
        (parsed_entries, blob_entries),
        (chunks, chunks),
        "the sizing warehouse must hold one parsed index and one blob per file"
    );
    EntrySizes {
        // The footprint is process-wide, and the clear above left the sizing
        // warehouse as the only thing in it, so its bytes are these files'.
        parsed: footprint.bytes / parsed_entries,
        blob: blob_bytes / blob_entries,
    }
}

/// THE DEFECT THIS GUARDS. Six Puffin-indexed files, caches budgeted for three
/// of each form, and the same text query six times. Before #4182 every
/// execution re-fetched all six blobs, because each blob was evicted by the
/// fetch of the file whose parsed entry displaced its own. Now a repeat
/// execution fetches only the files the blob budget cannot hold, and the rows
/// are the control's on every pass — eviction choice must not change an answer.
#[test]
fn a_repeat_text_suite_stops_refetching_the_blobs_it_still_holds() {
    serialized(repeat_text_suite_stops_refetching_the_blobs_it_still_holds);
}

async fn repeat_text_suite_stops_refetching_the_blobs_it_still_holds(snapshotter: &Snapshotter) {
    let chunks = 6;
    let rows_per_chunk = 400;
    let held = 3;
    let tmp = tempfile::tempdir().unwrap();
    let sizes = entry_sizes(tmp.path(), chunks, rows_per_chunk).await;

    // Explicit finite budgets, a fixed layout, a fixed query sequence.
    siglake_storage::configure_text_index_caches(siglake_storage::TextIndexCacheConfig {
        parsed_index_max_bytes: (sizes.parsed * held) as u64,
        puffin_blob_max_bytes: (sizes.blob * held) as u64,
    });

    let path = tmp.path().join("measured");
    let measured = puffin_files_fixture(&path, chunks, rows_per_chunk, true).await;
    let control =
        puffin_files_fixture(&tmp.path().join("control"), chunks, rows_per_chunk, false).await;
    let ctx = datafusion::prelude::SessionContext::new();
    measured.register_with_datafusion(&ctx).await.unwrap();
    let control_ctx = datafusion::prelude::SessionContext::new();
    control
        .register_with_datafusion(&control_ctx)
        .await
        .unwrap();
    let expected = needle_rows(&control_ctx, "rareneedle").await;
    assert_eq!(
        expected.len(),
        chunks * rows_per_chunk / 10,
        "the fixture's needle is on every tenth row of every chunk"
    );

    // Eight passes: the first is cold, the next displace the sizing
    // warehouse's entries — the budgets are process-wide — and the rest are
    // steady.
    let warehouse = path.to_string_lossy().to_string();
    let mut passes = Vec::new();
    for pass in 0..8 {
        let measurement = measure_pass(&ctx, snapshotter, &warehouse, "rareneedle").await;
        assert_eq!(
            measurement.rows, expected,
            "pass {pass}: the rows must be the unindexed control's, whatever the \
             caches dropped"
        );
        passes.push(measurement);
    }

    let (_, blob_bytes, _) = iceberg::arrow::puffin_blob_cache_stats(&warehouse);
    let footprint = iceberg::arrow::parsed_inverted_index_cache_footprint();
    assert!(
        blob_bytes <= sizes.blob * held && footprint.bytes <= sizes.parsed * held,
        "both caches must stay inside the budgets set for them ({blob_bytes} B of \
         blobs against {}, {} B parsed against {})",
        sizes.blob * held,
        footprint.bytes,
        sizes.parsed * held
    );
    assert!(
        footprint.evictions > 0,
        "the plan is meant to exceed the parsed budget; no eviction means the \
         measurement proved nothing"
    );

    // Measured 2026-09-16, per pass of the six-file plan: 6 fetches and 0 blob
    // hits cold, then 4/2 and 3/3 alternating with the protection window, and
    // 3,348–4,464 index-phase bytes against the cold pass's 26,082. Before
    // #4182 every pass of this layout paid 6 fetches, 0 blob hits and 6,696
    // bytes. The parsed cache serves nothing here: six files against a budget
    // for three is a working set its LRU cannot keep (#4102), which is why
    // every pass decodes six times in both arms.
    for (pass, measurement) in passes.iter().enumerate().skip(2) {
        let accounted = measurement.fetches + measurement.blob_hits + measurement.parsed_hits;
        assert_eq!(
            accounted, chunks as u64,
            "pass {pass}: every file's index is acquired exactly once, from a \
             fetch, the blob cache or the parsed cache ({} + {} + {})",
            measurement.fetches, measurement.blob_hits, measurement.parsed_hits
        );
        assert!(
            measurement.fetches < chunks as u64 && measurement.blob_hits + 1 >= held as u64,
            "pass {pass} fetched {} of {chunks} blobs and was served {} from \
             memory; the {held} the budget holds must not be re-fetched",
            measurement.fetches,
            measurement.blob_hits
        );
        assert!(
            measurement.index_reads >= measurement.fetches,
            "pass {pass}: index-phase reads ({}) cannot be fewer than the blob \
             fetches ({}) they include",
            measurement.index_reads,
            measurement.fetches
        );
        assert!(
            measurement.index_bytes < passes[1].index_bytes,
            "pass {pass} read {} index-phase bytes, no better than the \
             whole-plan re-fetch of pass 1 ({})",
            measurement.index_bytes,
            passes[1].index_bytes
        );
    }
}

/// What the pair's SPLIT is worth on the same layout, which the eviction fix
/// does not answer: at six indexed files, a budget for three parsed indexes and
/// three blobs against one that covers the plan, against one that spends the
/// same bytes on blobs alone. Blob bytes are about a fifth of their parsed form
/// here, so the same memory covers several times as many files as serialized
/// copies — at a decode per file per query.
///
/// Not a gate. It prints a table for #4102's parsed-budget sizing and #4054's
/// retention question and asserts only that the arms ran:
///
/// ```text
/// cargo test -p siglake-storage --test text_index_blob_refetch -- \
///   --ignored --nocapture
/// ```
#[ignore = "measurement, not a gate"]
#[test]
fn report_text_index_cache_budget_split() {
    serialized(text_index_cache_budget_split_report);
}

async fn text_index_cache_budget_split_report(snapshotter: &Snapshotter) {
    let chunks = 6;
    let rows_per_chunk = 400;
    let tmp = tempfile::tempdir().unwrap();
    let sizes = entry_sizes(tmp.path(), chunks, rows_per_chunk).await;
    println!(
        "{chunks} Puffin-indexed files of {rows_per_chunk} rows: {} B parsed, {} B \
         serialized per file ({}x)",
        sizes.parsed,
        sizes.blob,
        sizes.parsed / sizes.blob.max(1)
    );
    println!(
        "arm                       parsed_B  blob_B  fetch  blob_hit  parsed_hit  \
         idx_reads  idx_bytes  ms  retained_B"
    );

    for (arm, parsed_held, blob_held) in [
        ("blob cache off", 3, 0),
        ("packaged shape", 3, 3),
        ("parsed covers the plan", 6, 0),
        ("blob-heavy", 1, 6),
    ] {
        let parsed_budget = (sizes.parsed * parsed_held) as u64;
        let blob_budget = (sizes.blob * blob_held) as u64;
        // Empty caches per arm as well as fresh budgets: the sizing warehouse
        // and the previous arm are resident until something evicts them, and
        // the retained column below reads a process-wide footprint.
        iceberg::arrow::clear_text_index_caches();
        siglake_storage::configure_text_index_caches(siglake_storage::TextIndexCacheConfig {
            parsed_index_max_bytes: parsed_budget,
            puffin_blob_max_bytes: blob_budget,
        });
        // A fresh warehouse per arm: the caches are keyed by path, so a
        // previous arm's entries would otherwise start this one warm.
        let path = tmp
            .path()
            .join(format!("arm-{}-{}", parsed_held, blob_held));
        let fixture = puffin_files_fixture(&path, chunks, rows_per_chunk, true).await;
        let ctx = datafusion::prelude::SessionContext::new();
        fixture.register_with_datafusion(&ctx).await.unwrap();
        let warehouse = path.to_string_lossy().to_string();
        let mut passes = Vec::new();
        for _ in 0..8 {
            passes.push(measure_pass(&ctx, snapshotter, &warehouse, "rareneedle").await);
        }
        let steady = &passes[4..];
        let mean = |values: &[u64]| values.iter().sum::<u64>() as f64 / values.len() as f64;
        let fetches = mean(&steady.iter().map(|pass| pass.fetches).collect::<Vec<_>>());
        let blob_hits = mean(&steady.iter().map(|pass| pass.blob_hits).collect::<Vec<_>>());
        let parsed_hits = mean(
            &steady
                .iter()
                .map(|pass| pass.parsed_hits)
                .collect::<Vec<_>>(),
        );
        let reads = mean(
            &steady
                .iter()
                .map(|pass| pass.index_reads)
                .collect::<Vec<_>>(),
        );
        let bytes = mean(
            &steady
                .iter()
                .map(|pass| pass.index_bytes)
                .collect::<Vec<_>>(),
        );
        let ms = steady
            .iter()
            .map(|pass| pass.elapsed.as_secs_f64() * 1000.0)
            .sum::<f64>()
            / steady.len() as f64;
        let (_, blob_bytes, _) = iceberg::arrow::puffin_blob_cache_stats(&warehouse);
        // Process-global, and the clear above left this arm's warehouse as the
        // only one in it.
        let parsed_bytes = iceberg::arrow::parsed_inverted_index_cache_footprint().bytes;
        println!(
            "{arm:<24}  {parsed_budget:8}  {blob_budget:6}  {fetches:5.1}  {blob_hits:8.1}  \
             {parsed_hits:10.1}  {reads:9.1}  {bytes:9.0}  {ms:4.1}  {}",
            blob_bytes + parsed_bytes
        );
        assert_eq!(
            passes[0].rows.len(),
            chunks * rows_per_chunk / 10,
            "{arm}: the arm must return the fixture's rows"
        );
    }
}
