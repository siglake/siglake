//! What happens to the inline side object's coverage chain when a commit the
//! chain cannot bridge lands, and how it comes back (#3800).
//!
//! #2920 admits `siglake-aggregates.json` only when its `coverage` edge is
//! reachable from the snapshot a query serves through nothing but row-conserving
//! re-clusters. Four commits produce a chain the reader cannot walk:
//!
//!   - **snapshot expiry** of the edge's own snapshot. The walk needs every
//!     snapshot between the edge and current to still be in `metadata.json`, so
//!     a re-cluster run longer than `retain_last` used to strand the object for
//!     the life of the table;
//!   - **a delete task** and **retention**, which remove rows, so the object is
//!     also short of `total-records` on its own terms;
//!   - **a foreign overwrite**, deliberately unbridgeable
//!     (`iceberg::aggregate_coverage_link_tests`).
//!
//! The properties below: expiry re-roots the edge rather than orphaning it, a
//! republication lands on a root the next append can join, and the two
//! row-removing commits are repaired by `rebuild-time-aggregates` rather than by
//! anything on the commit path.

use std::collections::BTreeMap;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

use chrono::{Duration, TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use siglake_core::index_config::{IndexConfig, RetentionPolicy};
use siglake_core::Event;
use siglake_storage::iceberg::{
    IcebergContext, IcebergTuning, InlineCoverageOutcome, SnapshotAggregates, TimeBounds,
    SNAPSHOT_TIME_BUCKET_BASE_NS,
};

use crate::fixture_clock::fixture_base;

const INDEX: &str = "logs-orphan-coverage";

/// Result caches off: both consumers memoize per (table, snapshot, spec), so a
/// second call would measure the memo instead of the tier under test.
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

fn hourly_retention_config() -> IndexConfig {
    hourly_retention_config_with_period(2 * 60 * 60)
}

fn hourly_retention_config_with_period(period_secs: u64) -> IndexConfig {
    let mut config = index_config();
    config.retention = Some(RetentionPolicy {
        period_secs,
        schedule: Some("0 * * * *".to_string()),
    });
    config
}

fn base_time() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()
}

/// The seeded span, kept inside the inline aggregate's hourly bucket cap so the
/// windowed request nests in it and Tier-1 is reachable at all.
const SPAN_SECS: i64 = 1_000 * 3_600;

fn step_secs(total_rows: i64) -> i64 {
    (SPAN_SECS / total_rows.max(1)).max(1)
}

const PER_APPEND: i64 = 200;
const APPENDS: i64 = 4;

async fn append(ice: &IcebergContext, config: &IndexConfig, from: i64, rows: i64) {
    let ident = ice.index_table_ident(INDEX);
    let step = step_secs(APPENDS * PER_APPEND);
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

async fn append_times(
    ice: &IcebergContext,
    config: &IndexConfig,
    times: &[chrono::DateTime<Utc>],
    prefix: &str,
) {
    let mut events = Vec::with_capacity(times.len());
    for (i, timestamp) in times.iter().enumerate() {
        let mut event = Event::now(format!("{prefix}-{i}"));
        event.timestamp = *timestamp;
        event.host = format!("{prefix}-host-{}", i % 2);
        event.sourcetype = ["info", "warn"][i % 2].to_string();
        events.push(event);
    }
    let batch = siglake_core::events_to_record_batch(&events).unwrap();
    let mapped = siglake_core::mapping::map_carrier_batch(&batch, config).unwrap();
    let bloom_cols: Vec<String> = config.doc_mapping.tag_fields.to_vec();
    let bloom: Vec<&str> = bloom_cols.iter().map(String::as_str).collect();
    ice.append_to_table(&ice.index_table_ident(INDEX), mapped, &bloom)
        .await
        .unwrap();
}

async fn seed(dir: &std::path::Path) -> Arc<IcebergContext> {
    let ice = open(dir).await;
    let config = index_config();
    ice.create_index(&config).await.unwrap();
    for a in 0..APPENDS {
        append(&ice, &config, a * PER_APPEND, PER_APPEND).await;
    }
    ice
}

/// One row-conserving re-cluster commit, which is all the properties below ask
/// of it. It rewrites ONE day partition, not the whole table: the seeded span is
/// `SPAN_SECS` (about 42 days), and `recluster_files` takes one partition value
/// per call (#4720). The largest live group is chosen so the rewrite merges
/// files where the fixture has more than one in a day; a group of one is still a
/// rewrite commit, which is what a repeated call gets.
async fn recluster(ice: &IcebergContext) {
    let ident = ice.index_table_ident(INDEX);
    let files = ice.live_data_files(&ident).await.unwrap();
    assert!(files.len() > 1, "need multiple files to re-cluster");
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
}

/// The one incarnation directory holding this table's aggregate artifacts.
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

fn read_side(dir: &std::path::Path) -> SnapshotAggregates {
    let path = aggregate_dir(&dir.join("warehouse")).join("siglake-aggregates.json");
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn strip_coverage(dir: &std::path::Path) {
    let path = aggregate_dir(&dir.join("warehouse")).join("siglake-aggregates.json");
    let mut side: SnapshotAggregates =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    side.coverage = None;
    side.coverage_links.clear();
    std::fs::write(path, serde_json::to_vec(&side).unwrap()).unwrap();
}

/// Rewrite one immutable fixture file without its time-bucket KV. The Iceberg
/// manifest still describes the same rows and bounds; the measurement opens no
/// writer after this point. This is the pre-footer shape the rebuild's validity
/// guard must demote to a timestamp decode.
fn remove_time_bucket_footer(path: &str) {
    let path = std::path::Path::new(path.strip_prefix("file://").unwrap_or(path));
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let schema = builder.schema().clone();
    let key_values = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|kv| kv.key != siglake_storage::iceberg::TIME_BUCKETS_KV_KEY)
        .collect();
    let batches: Vec<_> = builder
        .build()
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect();
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(key_values))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
    for batch in &batches {
        writer.write(batch).unwrap();
    }
    writer.close().unwrap();
}

fn time_bucket_footer(path: &str) -> BTreeMap<i64, u64> {
    let path = std::path::Path::new(path.strip_prefix("file://").unwrap_or(path));
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let json = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == siglake_storage::iceberg::TIME_BUCKETS_KV_KEY)
        })
        .and_then(|kv| kv.value.as_deref())
        .expect("live fixture file has a time-bucket footer");
    let parsed: serde_json::Value = serde_json::from_str(json).unwrap();
    assert_eq!(
        parsed.get("nulls").and_then(serde_json::Value::as_u64),
        Some(0),
        "fixture footer has no null timestamps"
    );
    parsed
        .get("buckets")
        .and_then(serde_json::Value::as_object)
        .expect("fixture footer has a buckets object")
        .iter()
        .map(|(start, count)| {
            (
                start.parse::<i64>().unwrap(),
                count.as_u64().expect("fixture bucket count is a u64"),
            )
        })
        .collect()
}

/// The trailing quarter of the span, snapped to the aggregate's hourly bucket
/// edges (an unsnapped window is declined by both arms).
fn last25(total_rows: i64) -> TimeBounds {
    let hour = 3_600;
    let span = total_rows * step_secs(APPENDS * PER_APPEND);
    let start = (span * 3 / 4) / hour * hour;
    let end = (span + hour - 1) / hour * hour;
    TimeBounds {
        start: Some(base_time() + Duration::seconds(start)),
        end: Some(base_time() + Duration::seconds(end)),
    }
}

/// Which tier answered the windowed `GROUP BY`, with its rows.
async fn windowed_groups(
    ice: &IcebergContext,
    window: TimeBounds,
) -> (String, Vec<(Option<String>, u64)>) {
    let got = ice
        .grouped_counts_with_summary(INDEX, "sourcetype", None, Some(window))
        .await
        .unwrap()
        .expect("windowed group counts");
    let mut rows = got.to_rows();
    rows.sort();
    (got.source_label().to_string(), rows)
}

/// Run one census pass under a local recorder and return what it made of
/// [`INDEX`] together with the single gauge sample it wrote for that table:
/// value, and the labels the chart alert selects and renders on.
///
/// No query runs here. That is the point of the census — the only other signal
/// for this state, `siglake_query_side_aggs_cache_total{result="unproven_coverage"}`,
/// needs a query to arrive and names no table.
async fn census_samples(
    ice: &IcebergContext,
) -> (InlineCoverageOutcome, Vec<(f64, BTreeMap<String, String>)>) {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);
    let outcomes = ice.census_inline_coverage().await;
    drop(guard);

    let outcome = outcomes
        .iter()
        .find(|(table, _)| table == INDEX)
        .map(|(_, outcome)| *outcome)
        .unwrap_or_else(|| panic!("the census skipped {INDEX}: {outcomes:?}"));

    let samples: Vec<(f64, BTreeMap<String, String>)> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(k, _, _, _)| k.key().name() == "siglake_inline_coverage_unproven")
        .filter_map(|(k, _, _, v)| {
            let labels: BTreeMap<String, String> = k
                .key()
                .labels()
                .map(|l| (l.key().to_string(), l.value().to_string()))
                .collect();
            (labels.get("table").map(String::as_str) == Some(INDEX)).then_some(match v {
                DebugValue::Gauge(g) => (*g, labels),
                other => panic!("{other:?} is not a gauge"),
            })
        })
        .collect();
    (outcome, samples)
}

/// The same pass, for the tables the census reaches a verdict on: exactly one
/// gauge sample, and its value and labels.
async fn census(ice: &IcebergContext) -> (InlineCoverageOutcome, f64, BTreeMap<String, String>) {
    let (outcome, mut samples) = census_samples(ice).await;
    assert_eq!(
        samples.len(),
        1,
        "one gauge sample per table per pass: {samples:?}"
    );
    let (value, labels) = samples.pop().unwrap();
    (outcome, value, labels)
}

/// #4674: the state every remaining orphaning trigger ends in has a name on it.
/// A delete task removes rows, so the object describes a generation that no
/// longer exists and no commit republishes its chain; the census reports the
/// table by name, and the census after the rebuild clears it.
#[tokio::test]
async fn the_census_names_a_table_a_delete_task_left_unprovable() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path()).await;

    // Healthy first, or a gauge stuck at 1 would pass the assertion below for
    // free.
    let (outcome, unproven, labels) = census(&ice).await;
    assert_eq!(outcome, InlineCoverageOutcome::Covered);
    assert_eq!(unproven, 0.0, "a covered table must be reported at 0");
    assert_eq!(
        labels,
        BTreeMap::from([
            ("iceberg_namespace".to_string(), "siglake".to_string()),
            ("table".to_string(), INDEX.to_string()),
        ]),
        "the alert selects on these two labels and renders both into its \
         `rebuild-time-aggregates` line"
    );

    ice.create_delete_task(INDEX, "host = 'host-0'", None, None)
        .await
        .unwrap();
    let outcome = ice.execute_delete_tasks(INDEX).await.unwrap();
    assert!(outcome.rows_deleted > 0, "the delete task removed nothing");

    let (outcome, unproven, _) = census(&ice).await;
    assert_eq!(
        outcome,
        InlineCoverageOutcome::Unproven,
        "the census did not report the orphaned object"
    );
    assert_eq!(unproven, 1.0);
    // Exact all the way through: the guard refuses the object, it does not
    // serve it.
    assert_eq!(
        windowed_groups(&ice, last25(APPENDS * PER_APPEND)).await.0,
        "materialized"
    );

    let report = ice.rebuild_inline_time_aggregates(INDEX).await.unwrap();
    assert!(
        report.published,
        "the rebuild published nothing: {report:?}"
    );

    let (outcome, unproven, _) = census(&ice).await;
    assert_eq!(
        outcome,
        InlineCoverageOutcome::Covered,
        "the census did not clear after the repair"
    );
    assert_eq!(
        unproven, 0.0,
        "the gauge stayed raised after the repair, so the alert would never resolve"
    );
}

/// A pass that could not READ the object has no evidence about its coverage,
/// and must not overwrite the last real reading with one. Writing a 0 here
/// would clear a standing alert on an object-store blip; writing a 1 would page
/// for a table that is fine.
#[tokio::test]
async fn a_census_that_cannot_read_the_object_reports_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path()).await;
    assert_eq!(census(&ice).await.0, InlineCoverageOutcome::Covered);
    drop(ice);

    // The object is THERE and unusable — the case `load_side_aggregates`
    // reports as `parse_error` and otherwise returns as an indistinguishable
    // `None`, exactly as it would for a table that never published one.
    let path = aggregate_dir(&tmp.path().join("warehouse")).join("siglake-aggregates.json");
    std::fs::write(&path, b"{not json").unwrap();

    let ice = open(tmp.path()).await;
    let (outcome, samples) = census_samples(&ice).await;
    assert_eq!(outcome, InlineCoverageOutcome::Undetermined);
    assert!(
        samples.is_empty(),
        "a failed read wrote a coverage verdict: {samples:?}"
    );
}

/// THE REPRODUCTION, and then the fix: expiring the snapshot the coverage edge
/// names used to strand the object permanently, because the reader's walk needs
/// every snapshot between the edge and current to still be in `metadata.json`.
/// A re-cluster run longer than `retain_last` is the production shape;
/// `retain_last = 1` is the same condition without 100 compaction commits.
#[tokio::test]
async fn expiry_re_roots_the_coverage_edge_instead_of_orphaning_it() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path()).await;
    let ident = ice.index_table_ident(INDEX);
    let window = last25(APPENDS * PER_APPEND);

    let (label, before) = windowed_groups(&ice, window).await;
    assert_eq!(
        label, "tier1_windowed_agg",
        "the rollup must serve this before the expiry, or the test proves nothing"
    );
    let before_hist = ice
        .date_histogram_counts(INDEX, SNAPSHOT_TIME_BUCKET_BASE_NS, 0, None, Some(window))
        .await
        .unwrap()
        .expect("windowed histogram");
    let covered = read_side(tmp.path()).coverage.expect("a coverage edge");

    // Two re-clusters: the reader bridges them, so coverage still names the
    // last append's snapshot and its provability depends on that snapshot
    // staying in the metadata.
    recluster(&ice).await;
    recluster(&ice).await;
    assert_eq!(
        read_side(tmp.path()).coverage,
        Some(covered),
        "a re-cluster must not move the edge; the bridge is what makes it readable"
    );

    let expired = ice.expire_snapshots(&ident, 1).await.unwrap();
    assert!(
        expired >= APPENDS as usize,
        "the expiry has to drop the edge's own snapshot to reproduce this, dropped {expired}"
    );

    let side = read_side(tmp.path());
    let rerooted = side
        .coverage
        .expect("the expiry must leave a coverage edge");
    assert_ne!(
        rerooted, covered,
        "the expired snapshot is still named by the edge, so the chain is orphaned"
    );
    assert_eq!(
        ice.snapshot_count_for(&ident).await.unwrap(),
        1,
        "only the current snapshot survives this expiry"
    );

    let (label, after) = windowed_groups(&ice, window).await;
    assert_eq!(
        label, "tier1_windowed_agg",
        "Tier-1 stopped serving after the expiry: the chain was orphaned"
    );
    assert_eq!(after, before, "the re-rooted edge changed an answer");
    assert_eq!(
        ice.date_histogram_counts(INDEX, SNAPSHOT_TIME_BUCKET_BASE_NS, 0, None, Some(window))
            .await
            .unwrap(),
        Some(before_hist)
    );

    // And it keeps serving: the re-rooted edge is a root the next append's link
    // can join, so ordinary commit-path maintenance carries it forward.
    let config = index_config();
    append(&ice, &config, APPENDS * PER_APPEND, PER_APPEND).await;
    let side = read_side(tmp.path());
    assert!(
        side.coverage_links.is_empty(),
        "the append after the expiry left its edge stranded: {:?}",
        side.coverage_links
    );
    let (label, _) = windowed_groups(&ice, last25((APPENDS + 1) * PER_APPEND)).await;
    assert_eq!(
        label, "tier1_windowed_agg",
        "coverage did not advance with the append that followed the re-root"
    );
}

/// An expiry that cannot prove the object at the current snapshot leaves it
/// alone. The rule is #2920's: ancestry that is not there is never bridged, and
/// matching row totals are not evidence.
#[tokio::test]
async fn an_expiry_does_not_certify_an_object_it_cannot_prove() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path()).await;
    let ident = ice.index_table_ident(INDEX);

    // A delete task removes rows, so the object describes a generation that no
    // longer exists. The expiry that follows must not re-root it.
    ice.create_delete_task(INDEX, "host = 'host-0'", None, None)
        .await
        .unwrap();
    let outcome = ice.execute_delete_tasks(INDEX).await.unwrap();
    assert!(outcome.rows_deleted > 0, "the delete task removed nothing");
    let orphaned = read_side(tmp.path()).coverage;

    recluster(&ice).await;
    let expired = ice.expire_snapshots(&ident, 1).await.unwrap();
    assert!(expired > 0, "nothing expired, so nothing is being tested");

    assert_eq!(
        read_side(tmp.path()).coverage,
        orphaned,
        "the expiry moved an edge whose object it could not prove"
    );
    let (label, _) = windowed_groups(&ice, last25(APPENDS * PER_APPEND)).await;
    assert_eq!(
        label, "materialized",
        "an object that lost rows must stay on the exact per-file tier"
    );
}

/// The row-removing triggers: after a delete task the object is stale on its own
/// terms (its counts include deleted rows) and no commit repairs it. The
/// maintenance pass does, and the answer it restores is the exact one.
#[tokio::test]
async fn the_rebuild_restores_tier_1_after_a_delete_task() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path()).await;
    let window = last25(APPENDS * PER_APPEND);
    assert_eq!(windowed_groups(&ice, window).await.0, "tier1_windowed_agg");

    ice.create_delete_task(INDEX, "host = 'host-0'", None, None)
        .await
        .unwrap();
    let outcome = ice.execute_delete_tasks(INDEX).await.unwrap();
    assert!(outcome.rows_deleted > 0, "the delete task removed nothing");

    let (label, exact) = windowed_groups(&ice, window).await;
    assert_eq!(
        label, "materialized",
        "a delete task must retire the object to the exact tier"
    );
    let exact_hist = ice
        .date_histogram_counts(INDEX, SNAPSHOT_TIME_BUCKET_BASE_NS, 0, None, Some(window))
        .await
        .unwrap()
        .expect("windowed histogram");

    let report = ice.rebuild_inline_time_aggregates(INDEX).await.unwrap();
    assert!(
        report.published,
        "the rebuild published nothing: {report:?}"
    );
    assert!(
        report.time_buckets_restored,
        "time buckets short of {} rows: {:?}",
        report.record_count, report.time_buckets_rows
    );

    let (label, rebuilt) = windowed_groups(&ice, window).await;
    assert_eq!(
        label, "tier1_windowed_agg",
        "the rebuild did not put the windowed GROUP BY back on Tier-1: {report:?}"
    );
    assert_eq!(rebuilt, exact, "the rebuild changed the answer");
    assert_eq!(
        ice.date_histogram_counts(INDEX, SNAPSHOT_TIME_BUCKET_BASE_NS, 0, None, Some(window))
            .await
            .unwrap(),
        Some(exact_hist),
        "the rebuilt histogram disagrees with the exact one"
    );
}

/// Retention is the other shipped row-removing commit. Its schedule is an
/// operator concern (the CLI does not consume this cron string), but an hourly
/// policy must leave the same unproven state as a delete task and the same
/// rebuild must restore exact Tier-1 answers.
#[tokio::test]
async fn the_rebuild_restores_tier_1_after_hourly_retention() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = open(tmp.path()).await;
    let config = hourly_retention_config();
    ice.create_index(&config).await.unwrap();

    let now = Utc::now();
    let hour = Utc
        .timestamp_opt(now.timestamp().div_euclid(3_600) * 3_600, 0)
        .unwrap();
    append_times(
        &ice,
        &config,
        &[hour - Duration::hours(6), hour - Duration::hours(5)],
        "expired",
    )
    .await;
    append_times(
        &ice,
        &config,
        &[hour - Duration::minutes(90), hour - Duration::minutes(30)],
        "straddling",
    )
    .await;
    append_times(
        &ice,
        &config,
        &[hour - Duration::minutes(20), hour - Duration::minutes(10)],
        "recent",
    )
    .await;

    let window = TimeBounds {
        start: Some(hour - Duration::hours(2)),
        end: Some(hour + Duration::hours(1)),
    };
    assert_eq!(windowed_groups(&ice, window).await.0, "tier1_windowed_agg");

    let retained = ice.enforce_index_retention(INDEX).await.unwrap();
    assert!(retained.files_dropped > 0, "{retained:?}");
    let (label, exact) = windowed_groups(&ice, window).await;
    assert_eq!(label, "materialized");
    assert_eq!(census(&ice).await.0, InlineCoverageOutcome::Unproven);

    let report = ice.rebuild_inline_time_aggregates(INDEX).await.unwrap();
    assert!(report.published, "{report:?}");
    assert!(report.time_buckets_restored, "{report:?}");
    assert_eq!(census(&ice).await.0, InlineCoverageOutcome::Covered);
    let (label, rebuilt) = windowed_groups(&ice, window).await;
    assert_eq!(label, "tier1_windowed_agg");
    assert_eq!(rebuilt, exact, "the retention repair changed the answer");
}

#[derive(Debug)]
struct RebuildCost {
    full_ms: f64,
    buckets_ms: f64,
    bucket_footer_files: u64,
    bucket_decode_files: u64,
    bucket_decoded_bytes: u64,
    group_footer_files: u64,
    group_decode_files: u64,
    group_decoded_bytes: u64,
}

type MetricSnapshot = (
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
);

fn metric_counter(snapshot: &[MetricSnapshot], name: &str, source: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "source" && label.value() == source)
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(value) => *value,
            _ => 0,
        })
        .sum()
}

fn metric_histogram(snapshot: &[MetricSnapshot], name: &str, component: &str) -> f64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "component" && label.value() == component)
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Histogram(samples) => samples
                .iter()
                .map(|sample| sample.into_inner())
                .sum::<f64>(),
            _ => 0.0,
        })
        .sum()
}

async fn sample_rebuild(dir: &std::path::Path) -> RebuildCost {
    strip_coverage(dir);
    let ice = open(dir).await;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);
    let started = Instant::now();
    let report = ice.rebuild_inline_time_aggregates(INDEX).await.unwrap();
    let full_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let snapshot = snapshotter.snapshot().into_vec();
    drop(guard);
    assert!(report.published, "{report:?}");

    RebuildCost {
        full_ms,
        buckets_ms: metric_histogram(
            &snapshot,
            "siglake_inline_time_rebuild_seconds",
            "time_buckets",
        ) * 1_000.0,
        bucket_footer_files: metric_counter(
            &snapshot,
            "siglake_inline_time_rebuild_files_total",
            "footer",
        ),
        bucket_decode_files: metric_counter(
            &snapshot,
            "siglake_inline_time_rebuild_files_total",
            "decode",
        ),
        bucket_decoded_bytes: metric_histogram(
            &snapshot,
            "siglake_inline_time_rebuild_decoded_bytes",
            "time_buckets",
        ) as u64,
        group_footer_files: metric_counter(
            &snapshot,
            "siglake_inline_time_group_rebuild_files_total",
            "footer",
        ),
        group_decode_files: metric_counter(
            &snapshot,
            "siglake_inline_time_group_rebuild_files_total",
            "decode",
        ),
        group_decoded_bytes: metric_histogram(
            &snapshot,
            "siglake_inline_time_rebuild_decoded_bytes",
            "time_group_counts",
        ) as u64,
    }
}

fn median(samples: &[RebuildCost], get: impl Fn(&RebuildCost) -> f64) -> f64 {
    let mut values: Vec<f64> = samples.iter().map(get).collect();
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn print_cost(label: &str, samples: &[RebuildCost]) {
    let first = &samples[0];
    assert!(samples.iter().all(|sample| {
        sample.bucket_footer_files == first.bucket_footer_files
            && sample.bucket_decode_files == first.bucket_decode_files
            && sample.bucket_decoded_bytes == first.bucket_decoded_bytes
            && sample.group_footer_files == first.group_footer_files
            && sample.group_decode_files == first.group_decode_files
            && sample.group_decoded_bytes == first.group_decoded_bytes
    }));
    println!(
        "{label}: bucket-only p50={:.2}ms files={}/{}(footer/decode) decoded={}B; \
         full p50={:.2}ms group-files={}/{}(footer/decode) group-decoded={}B",
        median(samples, |sample| sample.buckets_ms),
        first.bucket_footer_files,
        first.bucket_decode_files,
        first.bucket_decoded_bytes,
        median(samples, |sample| sample.full_ms),
        first.group_footer_files,
        first.group_decode_files,
        first.group_decoded_bytes,
    );
}

#[derive(Debug)]
struct HourlyRetentionRebuildFixture {
    hour: chrono::DateTime<Utc>,
    expired: Vec<Vec<chrono::DateTime<Utc>>>,
    live: Vec<Vec<chrono::DateTime<Utc>>>,
    period_secs: u64,
}

fn hourly_retention_rebuild_fixture(now: chrono::DateTime<Utc>) -> HourlyRetentionRebuildFixture {
    let hour = fixture_base();
    debug_assert_eq!(hour.timestamp().rem_euclid(3_600), 0);

    // This report intentionally reaches six hours back, past fixture_clock's
    // general three-hour single-partition margin. Noon still leaves every row
    // on the fixed day, and the assertions below lock that geometry down.
    let expired = (0..8)
        .map(|append_no| {
            (0..200)
                .map(|row| hour - Duration::hours(6) + Duration::seconds(append_no * 200 + row))
                .collect()
        })
        .collect();
    let live = (0..16)
        .map(|append_no| {
            (0..200)
                .map(|row| {
                    let bucket = if row % 2 == 0 {
                        hour - Duration::minutes(90)
                    } else {
                        hour - Duration::minutes(30)
                    };
                    bucket + Duration::seconds(append_no)
                })
                .collect()
        })
        .collect();
    let intended_cutoff = hour - Duration::hours(2);
    let period_secs = now
        .signed_duration_since(intended_cutoff)
        .num_seconds()
        .try_into()
        .expect("fixture base precedes the retention run");

    HourlyRetentionRebuildFixture {
        hour,
        expired,
        live,
        period_secs,
    }
}

#[test]
fn hourly_retention_rebuild_fixture_is_stable_at_utc_boundaries() {
    for now in [
        Utc.with_ymd_and_hms(2026, 9, 23, 1, 35, 0).unwrap(),
        Utc.with_ymd_and_hms(2026, 9, 23, 0, 1, 0).unwrap(),
        Utc.with_ymd_and_hms(2026, 9, 23, 23, 59, 0).unwrap(),
    ] {
        let fixture = hourly_retention_rebuild_fixture(now);
        let event_times: Vec<_> = fixture
            .expired
            .iter()
            .chain(&fixture.live)
            .flatten()
            .copied()
            .collect();
        assert!(
            event_times
                .iter()
                .all(|timestamp| timestamp.date_naive() == fixture.hour.date_naive()),
            "{now}: fixture events crossed a UTC day"
        );

        let cutoff = now - Duration::seconds(i64::try_from(fixture.period_secs).unwrap());
        let last_expired = fixture.expired.iter().flatten().max().unwrap();
        let first_live = fixture.live.iter().flatten().min().unwrap();
        assert!(
            *last_expired < cutoff && cutoff < *first_live,
            "{now}: cutoff {cutoff} did not separate {last_expired} from {first_live}"
        );
    }
}

/// Local decision input for #4675. The table models an operator invoking the
/// CLI hourly: one retention trigger drops old files and leaves 16 live files,
/// each spanning two aggregate buckets so the full 2-D pass must decode them.
/// Five fresh contexts provide cold-cache medians. The second arm removes the
/// time-bucket footer to measure the specified legacy/invalid-footer fallback.
#[tokio::test]
#[ignore = "measurement report"]
async fn hourly_retention_rebuild_cost_report() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = open(tmp.path()).await;
    let fixture = hourly_retention_rebuild_fixture(Utc::now());
    let config = hourly_retention_config_with_period(fixture.period_secs);
    ice.create_index(&config).await.unwrap();

    for times in &fixture.expired {
        append_times(&ice, &config, times, "expired").await;
    }
    for times in &fixture.live {
        append_times(&ice, &config, times, "live").await;
    }
    let retained = ice.enforce_index_retention(INDEX).await.unwrap();
    assert_eq!(retained.files_dropped, 8, "{retained:?}");
    assert_eq!(retained.rows_dropped, 1_600, "{retained:?}");
    assert_eq!(retained.straddling_files_kept, 0, "{retained:?}");
    let ident = ice.index_table_ident(INDEX);
    let files = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(files.len(), 16, "fixture drifted: {retained:?}");
    assert_eq!(
        files.iter().map(|file| file.record_count()).sum::<u64>(),
        3_200,
        "retention kept the wrong live-row population"
    );
    let expected_buckets = BTreeMap::from([
        (
            (fixture.hour - Duration::hours(2))
                .timestamp_nanos_opt()
                .unwrap(),
            100,
        ),
        (
            (fixture.hour - Duration::hours(1))
                .timestamp_nanos_opt()
                .unwrap(),
            100,
        ),
    ]);
    for file in &files {
        let footer = time_bucket_footer(file.file_path());
        assert_eq!(
            footer.len(),
            2,
            "live fixture file {} does not have exactly two footer buckets: {footer:?}",
            file.file_path()
        );
        assert!(
            footer.values().all(|count| *count == 100),
            "live fixture file {} does not split its rows 100/100: {footer:?}",
            file.file_path()
        );
        let hourly_buckets = footer.into_iter().fold(
            BTreeMap::<i64, u64>::new(),
            |mut buckets, (start, count)| {
                let hour =
                    start.div_euclid(SNAPSHOT_TIME_BUCKET_BASE_NS) * SNAPSHOT_TIME_BUCKET_BASE_NS;
                *buckets.entry(hour).or_default() += count;
                buckets
            },
        );
        assert_eq!(
            hourly_buckets,
            expected_buckets,
            "live fixture file {} does not span both intended hourly buckets",
            file.file_path()
        );
    }
    let paths: Vec<String> = files
        .iter()
        .map(|file| file.file_path().to_string())
        .collect();
    drop(ice);

    let mut footer = Vec::new();
    for _ in 0..5 {
        footer.push(sample_rebuild(tmp.path()).await);
    }
    print_cost("valid-footer", &footer);

    for path in &paths {
        remove_time_bucket_footer(path);
    }
    let mut missing = Vec::new();
    for _ in 0..5 {
        missing.push(sample_rebuild(tmp.path()).await);
    }
    print_cost("missing-footer", &missing);
}

/// A republication has to land on a root the next append can join. The pass
/// reads the files of the current snapshot, and on a compacted table that
/// snapshot is a re-cluster — whose descendants' links name the append BELOW
/// it, not the re-cluster. Publishing the raw snapshot bought one query's worth
/// of Tier-1 and lost it at the next commit.
#[tokio::test]
async fn a_rebuild_on_a_recluster_survives_the_next_append() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path()).await;
    recluster(&ice).await;
    let append_root = read_side(tmp.path()).coverage.unwrap();
    drop(ice);

    // The pre-#2920 shape: counts with no provable edge, on a table whose
    // newest snapshot is a re-cluster.
    let path = aggregate_dir(&tmp.path().join("warehouse")).join("siglake-aggregates.json");
    let mut doc: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let obj = doc.as_object_mut().unwrap();
    assert!(obj.remove("coverage").is_some());
    obj.remove("coverage_links");
    std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();

    let ice = open(tmp.path()).await;
    let report = ice.rebuild_inline_time_aggregates(INDEX).await.unwrap();
    assert!(report.published, "{report:?}");
    let published = read_side(tmp.path()).coverage.expect("an edge");
    assert_eq!(
        published, append_root,
        "the repair must publish the same root the commit path uses, so the \
         next append joins it"
    );

    let config = index_config();
    append(&ice, &config, APPENDS * PER_APPEND, PER_APPEND).await;

    let side = read_side(tmp.path());
    assert!(
        side.coverage_links.is_empty(),
        "the append after the repair left its edge stranded: {:?}",
        side.coverage_links
    );
    let (label, _) = windowed_groups(&ice, last25((APPENDS + 1) * PER_APPEND)).await;
    assert_eq!(
        label, "tier1_windowed_agg",
        "the repair lost Tier-1 at the first commit after it"
    );
}
