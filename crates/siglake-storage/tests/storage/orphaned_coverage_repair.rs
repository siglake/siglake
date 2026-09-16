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

use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};
use siglake_core::index_config::IndexConfig;
use siglake_core::Event;
use siglake_storage::iceberg::{
    IcebergContext, IcebergTuning, SnapshotAggregates, TimeBounds, SNAPSHOT_TIME_BUCKET_BASE_NS,
};

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

async fn seed(dir: &std::path::Path) -> Arc<IcebergContext> {
    let ice = open(dir).await;
    let config = index_config();
    ice.create_index(&config).await.unwrap();
    for a in 0..APPENDS {
        append(&ice, &config, a * PER_APPEND, PER_APPEND).await;
    }
    ice
}

async fn recluster(ice: &IcebergContext) {
    let ident = ice.index_table_ident(INDEX);
    let files = ice.live_data_files(&ident).await.unwrap();
    assert!(files.len() > 1, "need multiple files to re-cluster");
    let bloom: Vec<&str> = vec!["host", "source", "sourcetype", "index"];
    ice.recluster_files(&ident, files, &bloom).await.unwrap();
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
