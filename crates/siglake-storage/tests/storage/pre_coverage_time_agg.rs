//! What a pre-coverage inline time aggregate costs, and why no commit repairs it.
//!
//! #2920 gave `siglake-aggregates.json` a snapshot-coverage chain: a reader
//! admits the object only when its `coverage` edge reaches the snapshot the
//! query is serving. An object written before that has no chain, so
//! `cached_side_aggregates` rejects it (`side_aggs_cache_total{unproven_coverage}`)
//! and every consumer takes the exact per-file tier. Answers stay right; the
//! acceleration is gone.
//!
//! Two questions #3082 has to answer before any rebuild command is written:
//! does the condition heal on its own, and what does it cost. The properties
//! below answer the first (it does not heal, for a structural reason), the
//! `#[ignore]`d report answers the second.
//!
//! Run the report:
//!   cargo test --release -p siglake-storage --test storage \
//!     pre_coverage_time_agg::the_fallback_cost_report -- --ignored --nocapture

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};

use chrono::{Duration, TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::index_config::IndexConfig;
use siglake_core::Event;
use siglake_storage::iceberg::{
    ColumnGroupCounts, IcebergContext, IcebergTuning, SnapshotAggregates, TimeBounds,
    SNAPSHOT_TIME_BUCKET_BASE_NS,
};

const INDEX: &str = "logs-pre-coverage";

/// Caches that would hide the difference, off: the windowed date histogram and
/// the windowed GROUP BY are both result-cached per (table, snapshot, spec),
/// so a second call of either measures the memo and not the path.
fn tuning() -> IcebergTuning {
    IcebergTuning {
        result_caches: Some(false),
        agg_result_cache_capacity: Some(0),
        ..Default::default()
    }
}

async fn open(dir: &std::path::Path) -> Arc<IcebergContext> {
    Arc::new(
        IcebergContext::open(&dir.join("warehouse"))
            .await
            .unwrap()
            .with_tuning(tuning()),
    )
}

fn index_config() -> IndexConfig {
    IndexConfig {
        index_id: INDEX.into(),
        ..IndexConfig::builtin_events()
    }
}

fn base_time() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()
}

/// Row `k` lands `k * STEP` seconds after the base. Chosen per seeding so the
/// whole span stays inside `TIME_BUCKET_CAP` hourly buckets (2048): past that
/// the inline aggregate coarsens its width, `date_histogram_from_snapshot_agg`
/// can no longer nest an hourly request in it, and BOTH arms of the report
/// would take the per-file path — a 1.0x ratio that means nothing.
const SPAN_SECS: i64 = 1_000 * 3_600;

fn step_secs(total_rows: i64) -> i64 {
    (SPAN_SECS / total_rows.max(1)).max(1)
}

/// One commit per call: `rows` rows from row `from`, one data file per append
/// at these sizes.
async fn append(ice: &IcebergContext, config: &IndexConfig, from: i64, rows: i64, step: i64) {
    let ident = ice.index_table_ident(INDEX);
    let levels = ["info", "warn", "debug", "error"];
    let mut events = Vec::with_capacity(rows as usize);
    for i in 0..rows {
        let k = from + i;
        let mut e = Event::now(format!("row {k}"));
        e.timestamp = base_time() + Duration::seconds(k * step);
        e.host = format!("host-{}", k % 3);
        e.sourcetype = levels[(k % 4) as usize].to_string();
        events.push(e);
    }
    let batch = siglake_core::events_to_record_batch(&events).unwrap();
    let mapped = siglake_core::mapping::map_carrier_batch(&batch, config).unwrap();
    let bloom_cols: Vec<String> = config.doc_mapping.tag_fields.to_vec();
    let bloom: Vec<&str> = bloom_cols.iter().map(String::as_str).collect();
    ice.append_to_table(&ident, mapped, &bloom).await.unwrap();
}

async fn seed(dir: &std::path::Path, appends: i64, per_append: i64) -> Arc<IcebergContext> {
    let ice = open(dir).await;
    let config = index_config();
    ice.create_index(&config).await.unwrap();
    let step = step_secs(appends * per_append);
    for a in 0..appends {
        append(&ice, &config, a * per_append, per_append, step).await;
    }
    ice
}

/// The one incarnation directory holding this table's aggregate artifacts,
/// found rather than hardcoded (the layout is `aggregate_prefix_rel_path`).
fn aggregate_dir(root: &std::path::Path) -> std::path::PathBuf {
    fn find(dir: &std::path::Path) -> Option<std::path::PathBuf> {
        for e in std::fs::read_dir(dir).ok()? {
            let p = e.ok()?.path();
            if !p.is_dir() {
                continue;
            }
            if p.join("siglake-aggregates.json").exists() {
                return Some(p);
            }
            if let Some(found) = find(&p) {
                return Some(found);
            }
        }
        None
    }
    find(root).expect("aggregate dir")
}

fn side_object_path(dir: &std::path::Path) -> std::path::PathBuf {
    aggregate_dir(&dir.join("warehouse")).join("siglake-aggregates.json")
}

fn read_side(dir: &std::path::Path) -> SnapshotAggregates {
    serde_json::from_slice(&std::fs::read(side_object_path(dir)).unwrap()).unwrap()
}

/// Rewrite the side object in the shape a pre-#2920 writer left: identical
/// counts, no `coverage` and no `coverage_links`. Both fields are
/// `skip_serializing_if`, so deleting the keys IS the legacy encoding rather
/// than an approximation of it. Returns the bytes that were there.
fn strip_coverage(dir: &std::path::Path) -> Vec<u8> {
    let path = side_object_path(dir);
    let before = std::fs::read(&path).unwrap();
    let mut doc: serde_json::Value = serde_json::from_slice(&before).unwrap();
    let obj = doc.as_object_mut().expect("side object is a JSON object");
    assert!(
        obj.remove("coverage").is_some(),
        "the seeded object must carry a coverage edge, else this test strips nothing"
    );
    obj.remove("coverage_links");
    std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();
    before
}

fn write_side(dir: &std::path::Path, bytes: &[u8]) {
    std::fs::write(side_object_path(dir), bytes).unwrap();
}

/// The trailing quarter of the seeded span — the `*_last25` shape the bench
/// board reports — snapped out to the aggregate's hourly bucket edges.
///
/// The snap is required, not cosmetic: `date_histogram_from_snapshot_agg`
/// declines a window whose edges straddle an aggregate bucket, so an
/// unsnapped window puts BOTH arms on the per-file path and the report
/// compares that path to itself.
fn last25_of(total_rows: i64, step: i64) -> TimeBounds {
    let hour = 3_600;
    let span = total_rows * step;
    let start = (span * 3 / 4) / hour * hour;
    let end = (span + hour - 1) / hour * hour;
    TimeBounds {
        start: Some(base_time() + Duration::seconds(start)),
        end: Some(base_time() + Duration::seconds(end)),
    }
}

fn last25(appends: i64, per_append: i64) -> TimeBounds {
    let total = appends * per_append;
    last25_of(total, step_secs(total))
}

fn counter_total(snap: metrics_util::debugging::Snapshot, name: &str) -> u64 {
    snap.into_vec()
        .into_iter()
        .filter(|(k, _, _, _)| k.key().name() == name)
        .map(|(_, _, _, v)| match v {
            DebugValue::Counter(c) => c,
            _ => 0,
        })
        .sum()
}

/// THE PROPERTY THE CARD RESTS ON: a pre-coverage object is refused, the
/// answers do not change, and the refusal is reported as such.
#[tokio::test]
async fn a_pre_coverage_object_is_refused_and_the_answers_stay_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path(), 6, 400).await;
    let window = last25(6, 400);

    let tier1_groups = ice
        .grouped_counts_with_summary(INDEX, "sourcetype", None, Some(window))
        .await
        .unwrap()
        .expect("windowed group counts");
    assert_eq!(
        tier1_groups.source_label(),
        "tier1_windowed_agg",
        "the rollup must serve this before the strip, or the comparison proves nothing"
    );
    let tier1_hist = ice
        .date_histogram_counts(INDEX, SNAPSHOT_TIME_BUCKET_BASE_NS, 0, None, Some(window))
        .await
        .unwrap()
        .expect("windowed histogram");
    let tier1_count = ice.windowed_count(INDEX, window, None).await.unwrap();
    assert!(
        tier1_count.is_some(),
        "the time buckets must serve the count"
    );
    drop(ice);

    strip_coverage(tmp.path());
    let ice = open(tmp.path()).await;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);
    let fallback_groups = ice
        .grouped_counts_with_summary(INDEX, "sourcetype", None, Some(window))
        .await
        .unwrap()
        .expect("windowed group counts");
    let refusals = counter_total(
        snapshotter.snapshot(),
        "siglake_query_side_aggs_cache_total",
    );
    drop(guard);

    assert_eq!(
        fallback_groups.source_label(),
        "materialized",
        "a coverage-less object must not serve a windowed GROUP BY"
    );
    assert!(
        refusals > 0,
        "the refusal must be counted, not silent (side_aggs_cache_total)"
    );
    // By key, not by emission order: neither tier promises one, and the
    // planner sorts.
    let sorted = |g: &siglake_storage::iceberg::GroupCounts| {
        let mut rows = g.to_rows();
        rows.sort();
        rows
    };
    assert_eq!(
        sorted(&fallback_groups),
        sorted(&tier1_groups),
        "the exact per-file tier must return the same answer the rollup did"
    );

    let fallback_hist = ice
        .date_histogram_counts(INDEX, SNAPSHOT_TIME_BUCKET_BASE_NS, 0, None, Some(window))
        .await
        .unwrap()
        .expect("windowed histogram");
    assert_eq!(
        fallback_hist, tier1_hist,
        "the per-file histogram must agree with the aggregate's"
    );
    assert_eq!(
        ice.windowed_count(INDEX, window, None).await.unwrap(),
        None,
        "windowed_count has no per-file tier of its own: without the time \
         buckets it declines and the caller scans"
    );
}

/// WHY AN OPERATOR PATH IS NEEDED AT ALL: no amount of further ingest repairs
/// it. The commit path publishes an edge `(parent, snapshot, seq)` and
/// `add_coverage_link` joins it only onto the chain's head. A legacy object has
/// no head, so the first edge after the strip has a parent nothing matches and
/// stays pending; every later edge chains onto that pending run instead of
/// joining. The object keeps accumulating correct counts it can never prove.
#[tokio::test]
async fn later_appends_do_not_restore_coverage() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path(), 4, 200).await;
    drop(ice);
    strip_coverage(tmp.path());

    let ice = open(tmp.path()).await;
    let config = index_config();
    for a in 4..8 {
        append(&ice, &config, a * 200, 200, step_secs(800)).await;
    }

    let side = read_side(tmp.path());
    assert_eq!(
        side.coverage, None,
        "four further commits joined the chain, so the strip is not the \
         permanent condition this card is about"
    );
    assert!(
        !side.coverage_links.is_empty(),
        "the appends must have published edges; an empty pending set would mean \
         the commits were not maintaining the object at all"
    );
    assert!(
        side.time_buckets.is_some() && side.time_group_counts.is_some(),
        "the counts keep accumulating — it is only the proof that is missing"
    );

    let window = last25_of(1600, step_secs(800));
    let got = ice
        .grouped_counts_with_summary(INDEX, "sourcetype", None, Some(window))
        .await
        .unwrap()
        .expect("windowed group counts");
    assert_eq!(
        got.source_label(),
        "materialized",
        "the table is still on the exact per-file tier after four more commits"
    );
}

/// A re-cluster does not repair it either — `aggregate_covers_current_snapshot`
/// walks row-conserving re-clusters back to a coverage edge, and there is none
/// to reach.
#[tokio::test]
async fn a_recluster_does_not_restore_coverage() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path(), 4, 200).await;
    drop(ice);
    strip_coverage(tmp.path());

    let ice = open(tmp.path()).await;
    let ident = ice.index_table_ident(INDEX);
    // One re-cluster commit is what this asks for. The seed spans about 42 day
    // partitions and `recluster_files` takes one partition value per call
    // (#4720), so the bin is one day's files, not the whole live set.
    let files = ice.live_data_files(&ident).await.unwrap();
    assert!(files.len() > 1, "need multiple files to compact");
    let mut groups: BTreeMap<String, Vec<_>> = BTreeMap::new();
    for file in files {
        groups
            .entry(format!("{:?}", file.partition()))
            .or_default()
            .push(file);
    }
    let bin = groups
        .into_values()
        .max_by_key(|group| group.len())
        .expect("a live partition to re-cluster");
    let bloom: Vec<&str> = vec!["host", "source", "sourcetype", "index"];
    ice.recluster_files(&ident, bin, &bloom).await.unwrap();

    assert_eq!(read_side(tmp.path()).coverage, None);
    let window = last25(4, 200);
    assert_eq!(
        ice.grouped_counts_with_summary(INDEX, "sourcetype", None, Some(window))
            .await
            .unwrap()
            .expect("windowed group counts")
            .source_label(),
        "materialized",
        "compaction is not a repair path for a coverage-less object"
    );
}

fn ms(d: StdDuration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// One arm's cost, kept apart because they answer different questions: the
/// first call on a fresh context (cold footer and file-list caches) is what an
/// operator notices, the calls after it are what the table pays forever.
#[derive(Default)]
struct Arm {
    cold: Vec<StdDuration>,
    warm: Vec<StdDuration>,
}

impl Arm {
    /// One round's samples: the leading call is that context's cold one.
    fn round(&mut self, mut samples: Vec<StdDuration>) {
        self.cold.push(samples.remove(0));
        self.warm.extend(samples);
    }

    fn p50(samples: &[StdDuration]) -> f64 {
        let mut s = samples.to_vec();
        s.sort();
        ms(s[s.len() / 2])
    }

    fn cold_p50(&self) -> f64 {
        Self::p50(&self.cold)
    }

    fn warm_p50(&self) -> f64 {
        Self::p50(&self.warm)
    }
}

/// The two shapes a pre-coverage inline time aggregate stops serving.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// `date_histogram_counts` at the aggregate's own width: Tier-1 reads the
    /// time buckets, the fallback reads a footer per live file.
    DateHistogram,
    /// Windowed `GROUP BY`: Tier-1 reads the 2-D time x group rollup, the
    /// fallback sums a per-file footer.
    WindowedGroupBy,
}

impl Shape {
    fn label(self) -> &'static str {
        match self {
            Shape::DateHistogram => "date_histogram",
            Shape::WindowedGroupBy => "windowed_group_by",
        }
    }
}

/// `calls` timings of one shape on ONE fresh context. Each shape gets its own
/// context: running both in one would let the histogram's per-file pass warm
/// the footer cache the GROUP BY's fallback then reads, which is how the first
/// cut of this report produced a fallback arm that beat Tier-1.
async fn sample(
    dir: &std::path::Path,
    shape: Shape,
    window: TimeBounds,
    calls: usize,
    tier1: bool,
) -> Vec<StdDuration> {
    let ice = open(dir).await;
    let mut out = Vec::with_capacity(calls);
    for _ in 0..calls {
        let t = Instant::now();
        match shape {
            Shape::DateHistogram => {
                let recorder = DebuggingRecorder::new();
                let snapshotter = recorder.snapshotter();
                let guard = metrics::set_default_local_recorder(&recorder);
                let got = ice
                    .date_histogram_counts(
                        INDEX,
                        SNAPSHOT_TIME_BUCKET_BASE_NS,
                        0,
                        None,
                        Some(window),
                    )
                    .await
                    .unwrap();
                out.push(t.elapsed());
                let served = counter_total(
                    snapshotter.snapshot(),
                    "siglake_query_histogram_snapshot_agg_total",
                );
                drop(guard);
                assert!(got.is_some(), "the histogram must answer in both arms");
                // WITHOUT this the report can compare the per-file path to
                // itself: over `TIME_BUCKET_CAP` buckets the aggregate widens
                // and Tier-1 silently declines, which read as a 1.0x ratio.
                assert_eq!(
                    served > 0,
                    tier1,
                    "the {} arm did not take the path it is named for",
                    if tier1 { "Tier-1" } else { "fallback" }
                );
            }
            Shape::WindowedGroupBy => {
                let got = ice
                    .grouped_counts_with_summary(INDEX, "sourcetype", None, Some(window))
                    .await
                    .unwrap()
                    .expect("windowed group counts");
                out.push(t.elapsed());
                let want = if tier1 {
                    "tier1_windowed_agg"
                } else {
                    "materialized"
                };
                assert_eq!(got.source_label(), want, "the arm took the wrong path");
            }
        }
    }
    out
}

/// THE MEASUREMENT. Per live-file count and query shape: the same table, once
/// with the side object as written and once with its coverage stripped,
/// interleaved per pair so a drifting machine moves both arms together.
///
/// Reported, not asserted. The ratio is the input to "does a rebuild command
/// earn its surface"; a threshold on it would be a latency assertion on a
/// shared box.
#[tokio::test]
#[ignore = "measurement report"]
async fn the_fallback_cost_report() {
    println!(
        "{:>6} {:>18} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "files", "shape", "t1 cold", "t1 warm", "fb cold", "fb warm", "cold x", "warm x"
    );
    for (appends, per_append) in [(8i64, 500i64), (32, 500), (128, 500)] {
        let tmp = tempfile::tempdir().unwrap();
        let ice = seed(tmp.path(), appends, per_append).await;
        let ident = ice.index_table_ident(INDEX);
        let files = ice.live_data_files(&ident).await.unwrap().len();
        drop(ice);
        let window = last25(appends, per_append);
        let covered = std::fs::read(side_object_path(tmp.path())).unwrap();
        strip_coverage(tmp.path());
        let stripped = std::fs::read(side_object_path(tmp.path())).unwrap();

        for shape in [Shape::DateHistogram, Shape::WindowedGroupBy] {
            let (mut t1, mut fb) = (Arm::default(), Arm::default());
            for _ in 0..5 {
                write_side(tmp.path(), &covered);
                t1.round(sample(tmp.path(), shape, window, 4, true).await);
                write_side(tmp.path(), &stripped);
                fb.round(sample(tmp.path(), shape, window, 4, false).await);
            }
            println!(
                "{files:>6} {:>18} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>8.1}x {:>8.1}x",
                shape.label(),
                t1.cold_p50(),
                t1.warm_p50(),
                fb.cold_p50(),
                fb.warm_p50(),
                fb.cold_p50() / t1.cold_p50().max(0.001),
                fb.warm_p50() / t1.warm_p50().max(0.001),
            );
        }
    }
}

/// THE REPAIR. After `rebuild_inline_time_aggregates` the same queries serve
/// from Tier-1 again with byte-identical answers, and the object carries a
/// coverage edge naming the snapshot the pass read.
#[tokio::test]
async fn the_rebuild_restores_tier_1_without_changing_an_answer() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path(), 6, 400).await;
    let window = last25(6, 400);

    let before_groups = ice
        .grouped_counts_with_summary(INDEX, "sourcetype", None, Some(window))
        .await
        .unwrap()
        .expect("windowed group counts");
    assert_eq!(before_groups.source_label(), "tier1_windowed_agg");
    let before_hist = ice
        .date_histogram_counts(INDEX, SNAPSHOT_TIME_BUCKET_BASE_NS, 0, None, Some(window))
        .await
        .unwrap()
        .expect("windowed histogram");
    let before_count = ice.windowed_count(INDEX, window, None).await.unwrap();
    let before_side = read_side(tmp.path());
    drop(ice);

    strip_coverage(tmp.path());
    let ice = open(tmp.path()).await;
    let report = ice.rebuild_inline_time_aggregates(INDEX).await.unwrap();

    assert!(
        report.published,
        "the rebuild published nothing: {report:?}"
    );
    assert!(!report.already_covered);
    assert!(
        report.time_buckets_restored,
        "time buckets short of {} rows: {:?}",
        report.record_count, report.time_buckets_rows
    );
    assert!(
        report.columns.iter().any(|c| c.column == "sourcetype"),
        "the pass must take its column set from the object it repairs: {report:?}"
    );
    assert!(
        report.columns.iter().all(|c| c.covers_table),
        "every maintained column should be readable from the files here: {report:?}"
    );

    let side = read_side(tmp.path());
    assert_eq!(
        side.coverage.map(|c| c.snapshot_id),
        Some(report.coverage.snapshot_id),
        "the published edge must name the snapshot the pass read"
    );
    assert!(side.coverage_links.is_empty());
    assert_eq!(
        side.group_counts, None,
        "the inline group counts are dropped, not certified (decision 2026-09-16)"
    );

    // The counts themselves are unchanged: this is a proof-of-provenance
    // repair, not a recount. A drifting map here would be the failure mode the
    // whole coverage mechanism exists to prevent.
    assert_eq!(
        side.time_buckets, before_side.time_buckets,
        "the rebuilt time buckets differ from what maintenance had accumulated"
    );
    assert_eq!(
        side.time_group_counts, before_side.time_group_counts,
        "the rebuilt 2-D rollup differs from what maintenance had accumulated"
    );

    let after_groups = ice
        .grouped_counts_with_summary(INDEX, "sourcetype", None, Some(window))
        .await
        .unwrap()
        .expect("windowed group counts");
    assert_eq!(
        after_groups.source_label(),
        "tier1_windowed_agg",
        "the windowed GROUP BY is still on the per-file tier after the repair"
    );
    let mut want = before_groups.to_rows();
    want.sort();
    let mut got = after_groups.to_rows();
    got.sort();
    assert_eq!(got, want, "the repair changed the answer");
    assert_eq!(
        ice.date_histogram_counts(INDEX, SNAPSHOT_TIME_BUCKET_BASE_NS, 0, None, Some(window))
            .await
            .unwrap(),
        Some(before_hist)
    );
    assert_eq!(
        ice.windowed_count(INDEX, window, None).await.unwrap(),
        before_count
    );
}

/// THE POINT OF RE-ROOTING THE CHAIN, and the property that makes this a repair
/// rather than a one-off: after the rebuild, the NEXT append's edge has the
/// published snapshot as its parent, so it joins and ordinary commit-path
/// maintenance carries coverage forward on its own. Without this the command
/// would buy one query's worth of Tier-1 and lose it at the next commit.
#[tokio::test]
async fn ordinary_maintenance_carries_coverage_forward_after_the_rebuild() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path(), 4, 200).await;
    drop(ice);
    strip_coverage(tmp.path());

    let ice = open(tmp.path()).await;
    let report = ice.rebuild_inline_time_aggregates(INDEX).await.unwrap();
    assert!(report.published);

    let config = index_config();
    for a in 4..7 {
        append(&ice, &config, a * 200, 200, step_secs(800)).await;
    }

    let side = read_side(tmp.path());
    assert!(
        side.coverage_links.is_empty(),
        "three appends after the repair left edges stranded: {:?}",
        side.coverage_links
    );
    let window = last25_of(1400, step_secs(800));
    assert_eq!(
        ice.grouped_counts_with_summary(INDEX, "sourcetype", None, Some(window))
            .await
            .unwrap()
            .expect("windowed group counts")
            .source_label(),
        "tier1_windowed_agg",
        "coverage did not advance with the appends that followed the repair"
    );
}

/// Re-running is free of consequence: the maps are replaced rather than merged,
/// so a second pass over an already-covered object reports that and writes
/// nothing. A merge-based repair would double every count here.
#[tokio::test]
async fn a_second_rebuild_is_a_reported_no_op() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path(), 4, 200).await;
    drop(ice);
    strip_coverage(tmp.path());

    let ice = open(tmp.path()).await;
    assert!(
        ice.rebuild_inline_time_aggregates(INDEX)
            .await
            .unwrap()
            .published
    );
    let after_first = read_side(tmp.path());

    let again = ice.rebuild_inline_time_aggregates(INDEX).await.unwrap();
    assert!(
        again.already_covered && !again.published,
        "a second pass must recognise its own work: {again:?}"
    );
    let after_second = read_side(tmp.path());
    assert_eq!(
        after_second.time_buckets, after_first.time_buckets,
        "the second pass changed the time buckets"
    );
    assert_eq!(
        after_second.time_group_counts, after_first.time_group_counts,
        "the second pass changed the 2-D rollup"
    );
}

/// The pass refuses to rebuild an object it cannot bind to a column set, rather
/// than inventing one. Same rule as the wide rebuild: a repair restores what a
/// table was maintaining.
#[tokio::test]
async fn a_table_with_no_inline_object_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path(), 2, 100).await;
    drop(ice);
    std::fs::remove_file(side_object_path(tmp.path())).unwrap();

    let ice = open(tmp.path()).await;
    let err = ice
        .rebuild_inline_time_aggregates(INDEX)
        .await
        .expect_err("a missing object must be refused, not created");
    assert!(
        err.to_string().contains("no inline aggregate object"),
        "unexpected refusal: {err}"
    );
}

/// THE FOOTER SHORTCUT. A file whose manifest `[min, max]` fits inside one
/// aggregate bucket contributes its group-count footer to that bucket instead
/// of being decoded. Worth its own test because it is a SECOND way to compute
/// the same map, and a shortcut that disagrees with the decode would publish a
/// wrong answer as proven.
#[tokio::test]
async fn bucket_contained_files_are_rebuilt_from_footers_and_agree() {
    let tmp = tempfile::tempdir().unwrap();
    // One second apart: every append's file lands inside the first hour, so
    // every file is provably contained in one bucket.
    let ice = open(tmp.path()).await;
    let config = index_config();
    ice.create_index(&config).await.unwrap();
    for a in 0..5 {
        // Repeated timestamps keep the sibling far below every cardinality cap.
        // Its absence therefore proves the semantic exclusion, not cap eviction.
        append(&ice, &config, a * 300, 300, 0).await;
    }
    let window = TimeBounds {
        start: Some(base_time()),
        end: Some(base_time() + Duration::hours(1)),
    };
    let aggregate_timestamp_ns = ice
        .grouped_counts_with_summary(INDEX, "timestamp_ns", None, None)
        .await
        .unwrap();
    assert!(
        aggregate_timestamp_ns.is_none(),
        "timestamp_ns must miss every aggregate tier so the SQL caller takes its exact scan"
    );

    let before = read_side(tmp.path());
    drop(ice);

    strip_coverage(tmp.path());
    let ice = open(tmp.path()).await;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);
    let report = ice.rebuild_inline_time_aggregates(INDEX).await.unwrap();
    let snap = snapshotter.snapshot().into_vec();
    drop(guard);

    let by_source = |want: &str| -> u64 {
        snap.iter()
            .filter(|(k, _, _, _)| {
                k.key().name() == "siglake_inline_time_group_rebuild_files_total"
                    && k.key()
                        .labels()
                        .any(|l| l.key() == "source" && l.value() == want)
            })
            .map(|(_, _, _, v)| match v {
                DebugValue::Counter(c) => *c,
                _ => 0,
            })
            .sum()
    };
    assert!(
        by_source("footer") > 0,
        "no file took the footer shortcut, so this test exercises the decode \
         path the other tests already cover"
    );
    assert_eq!(
        by_source("decode"),
        0,
        "a bucket-contained file fell to the decode"
    );

    assert!(report.published, "{report:?}");
    let after = read_side(tmp.path());
    // Per covering column, not map-for-map: this pins that the footer shortcut
    // and ordinary commit-path maintenance compute the same rollup.
    let before_tg = before.time_group_counts.expect("maintained rollup");
    let after_tg = after.time_group_counts.expect("rebuilt rollup");
    let covering: Vec<&str> = report
        .columns
        .iter()
        .filter(|c| c.covers_table)
        .map(|c| c.column.as_str())
        .collect();
    assert!(
        covering.contains(&"sourcetype"),
        "no covering column to compare: {report:?}"
    );
    for column in &covering {
        assert_eq!(
            after_tg.columns.get(*column),
            before_tg.columns.get(*column),
            "the footer shortcut disagrees with maintenance on `{column}`"
        );
    }
    assert!(
        report.columns.iter().all(|c| c.column != "timestamp_ns"),
        "new aggregate selection must not rebuild timestamp_ns: {report:?}"
    );
    assert!(
        !after_tg.columns.contains_key("timestamp_ns"),
        "a column short of the row count must not be published"
    );
    assert_eq!(
        ice.grouped_counts_with_summary(INDEX, "sourcetype", None, Some(window))
            .await
            .unwrap()
            .expect("windowed group counts")
            .source_label(),
        "tier1_windowed_agg"
    );
}

/// Existing side objects can still carry the pre-fix inferred column. Keep the
/// compatibility report pinned with explicit legacy metadata rather than
/// making new writes recreate the defect.
#[tokio::test]
async fn a_legacy_short_timestamp_ns_is_reported_and_removed() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = open(tmp.path()).await;
    let config = index_config();
    ice.create_index(&config).await.unwrap();
    for a in 0..5 {
        append(&ice, &config, a * 300, 300, 0).await;
    }

    let fixed_bytes = std::fs::read(side_object_path(tmp.path())).unwrap();
    let mut legacy = read_side(tmp.path());
    let legacy_tg = legacy
        .time_group_counts
        .as_mut()
        .expect("maintained rollup");
    legacy_tg.columns.insert(
        "timestamp_ns".to_string(),
        std::collections::BTreeMap::from([(
            base_time()
                .timestamp_nanos_opt()
                .expect("base time in nanos"),
            ColumnGroupCounts {
                values: [("legacy".to_string(), 300)].into_iter().collect(),
                nulls: 0,
            },
        )]),
    );
    let legacy_bytes = serde_json::to_vec(&legacy).unwrap();
    assert!(
        legacy_bytes.len() > fixed_bytes.len(),
        "the excluded column must reduce the maintained side object: legacy={} fixed={}",
        legacy_bytes.len(),
        fixed_bytes.len()
    );
    eprintln!(
        "inline aggregate bytes for 5x300 repeated timestamps: legacy={} fixed={} saved={}",
        legacy_bytes.len(),
        fixed_bytes.len(),
        legacy_bytes.len() - fixed_bytes.len()
    );
    write_side(tmp.path(), &legacy_bytes);
    drop(ice);

    strip_coverage(tmp.path());
    let ice = open(tmp.path()).await;
    let report = ice.rebuild_inline_time_aggregates(INDEX).await.unwrap();
    assert!(
        report
            .columns
            .iter()
            .any(|c| c.column == "timestamp_ns" && !c.covers_table),
        "the short legacy timestamp column must be reported: {report:?}"
    );
    assert!(
        !read_side(tmp.path())
            .time_group_counts
            .expect("rebuilt rollup")
            .columns
            .contains_key("timestamp_ns"),
        "a legacy column short of the row count must not be published"
    );
}
