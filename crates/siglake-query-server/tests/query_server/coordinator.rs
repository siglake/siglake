//! Differential tests for the distributed-query coordinator (#7 part 2).
//!
//! For every supported query shape, the coordinated result (fanned across N
//! in-process shards + merged) MUST equal the single-pod unsharded result.
//! Unsupported shapes must fall back to a whole-table run (also equal).

use std::sync::Arc;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use async_trait::async_trait;
use chrono::{Duration, TimeZone, Utc};
use siglake_core::Event;
use siglake_query_server::coordinator::{classify, coordinate, DistPlan, ShardRunner};
use siglake_query_server::udfs::register_udfs;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::{session_context_with, ScanShard};

/// In-process shard runner: runs the query against the local warehouse with
/// the given shard's `SessionConfig` extension. Stands in for an HTTP peer.
struct LocalRunner {
    ice: Arc<IcebergContext>,
}

#[async_trait]
impl ShardRunner for LocalRunner {
    async fn run(&self, sql: &str, shard: Option<ScanShard>) -> Result<Vec<RecordBatch>> {
        let ctx = session_context_with(None, shard);
        self.ice.register_with_datafusion(&ctx).await?;
        register_udfs(&ctx);
        Ok(ctx.sql(sql).await?.collect().await?)
    }
}

/// Stringify + sort all rows so results compare regardless of batch/row order.
fn normalize(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let opts = FormatOptions::default();
    let mut rows = Vec::new();
    for b in batches {
        let fmts: Vec<ArrayFormatter> = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c, &opts).unwrap())
            .collect();
        for r in 0..b.num_rows() {
            rows.push(
                fmts.iter()
                    .map(|f| f.value(r).to_string())
                    .collect::<Vec<_>>(),
            );
        }
    }
    rows.sort();
    rows
}

/// Like [`normalize`] but **preserves row order** — for verifying ordered-scan
/// results come back in the right sequence, not just the right set.
fn ordered(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let opts = FormatOptions::default();
    let mut rows = Vec::new();
    for b in batches {
        let fmts: Vec<ArrayFormatter> = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c, &opts).unwrap())
            .collect();
        for r in 0..b.num_rows() {
            rows.push(
                fmts.iter()
                    .map(|f| f.value(r).to_string())
                    .collect::<Vec<_>>(),
            );
        }
    }
    rows
}

async fn warehouse() -> (tempfile::TempDir, Arc<IcebergContext>) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    // Six appends ⇒ several files for the shards to split. Varied host + raw.
    for i in 0..6 {
        let batch: Vec<Event> = (0..((i + 1) * 5))
            .map(|j| {
                let mut e = Event::now(format!("req {j} status={} error", (i + j) % 3 * 100 + 200));
                e.host = format!("host-{}", j % 4);
                e.timestamp = base + Duration::seconds((i * 100 + j) as i64);
                e
            })
            .collect();
        ice.append_events(&batch).await.unwrap();
    }
    (tmp, ice)
}

/// Run `sql` both ways and assert the merged distributed result == unsharded.
async fn assert_same(ice: &Arc<IcebergContext>, sql: &str, shards: usize) {
    let planning = session_context_with(None, None);
    ice.register_with_datafusion(&planning).await.unwrap();
    register_udfs(&planning);
    let unsharded = planning.sql(sql).await.unwrap().collect().await.unwrap();

    let runner = LocalRunner { ice: ice.clone() };
    let coordinated = coordinate(&planning, &runner, sql, shards).await.unwrap();

    assert_eq!(
        normalize(&coordinated),
        normalize(&unsharded),
        "coordinated != unsharded for `{sql}`"
    );
}

/// Like [`assert_same`], but also assert the query genuinely **distributes**
/// (not the whole-table `Local` fallback). Correctness alone can't tell the two
/// apart — the fallback also returns the right answer — which is exactly how a
/// classifier regression (AWS smoke round 62) hid for so long.
async fn assert_distributes(ice: &Arc<IcebergContext>, sql: &str, shards: usize) {
    let planning = session_context_with(None, None);
    ice.register_with_datafusion(&planning).await.unwrap();
    register_udfs(&planning);
    let plan = planning.state().create_logical_plan(sql).await.unwrap();
    assert_ne!(
        classify(&plan),
        DistPlan::Local,
        "`{sql}` must distribute, not fall back"
    );
    assert_same(ice, sql, shards).await;
}

#[tokio::test]
async fn coordinated_aggregates_match_unsharded() {
    let (_tmp, ice) = warehouse().await;
    // Two-phase aggregate shapes (count→sum, sum→sum, min, max; ± GROUP BY).
    // `assert_distributes` proves each actually fans out (not the fallback).
    assert_distributes(&ice, "SELECT count(*) FROM events", 3).await;
    assert_distributes(&ice, "SELECT count(*) AS n FROM events", 3).await; // aliased
    assert_distributes(&ice, "SELECT count(*) FROM events WHERE host = 'host-1'", 3).await;
    assert_distributes(&ice, "SELECT host, count(*) FROM events GROUP BY host", 4).await;
    assert_distributes(
        &ice,
        "SELECT host, count(*) AS c FROM events GROUP BY host",
        4,
    )
    .await; // aliased
    assert_distributes(
        &ice,
        "SELECT host, count(*) FROM events WHERE raw LIKE '%error%' GROUP BY host",
        3,
    )
    .await;
    assert_distributes(
        &ice,
        "SELECT min(\"timestamp\"), max(\"timestamp\") FROM events",
        3,
    )
    .await;
    assert_distributes(
        &ice,
        "SELECT host, min(\"timestamp\"), max(\"timestamp\") FROM events GROUP BY host",
        5,
    )
    .await;
    assert_distributes(&ice, "SELECT sum(length(raw)) FROM events", 3).await;
}

#[tokio::test]
async fn coordinated_scans_match_unsharded() {
    let (_tmp, ice) = warehouse().await;
    assert_distributes(
        &ice,
        "SELECT host, raw FROM events WHERE host = 'host-2'",
        3,
    )
    .await;
    assert_distributes(
        &ice,
        "SELECT raw FROM events WHERE raw LIKE '%status=500%'",
        4,
    )
    .await;
    assert_distributes(
        &ice,
        "SELECT raw FROM events WHERE match_terms(raw, 'status 500')",
        4,
    )
    .await;
    // LIMIT: re-applied after the union. (count-equality is what matters; the
    // exact rows under a non-deterministic LIMIT may differ, so assert the row
    // COUNT matches the limit instead — done in the dedicated test below.)
}

#[tokio::test]
async fn limited_scan_returns_at_most_limit() {
    let (_tmp, ice) = warehouse().await;
    let planning = session_context_with(None, None);
    ice.register_with_datafusion(&planning).await.unwrap();
    let runner = LocalRunner { ice: ice.clone() };
    let got = coordinate(&planning, &runner, "SELECT raw FROM events LIMIT 7", 3)
        .await
        .unwrap();
    let rows: usize = got.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 7,
        "LIMIT must be re-applied after the cross-shard union"
    );
}

#[tokio::test]
async fn coordinated_ordered_scan_matches_unsharded_in_order() {
    let (_tmp, ice) = warehouse().await;
    let planning = session_context_with(None, None);
    ice.register_with_datafusion(&planning).await.unwrap();
    register_udfs(&planning);
    let runner = LocalRunner { ice: ice.clone() };

    // warehouse() timestamps are unique, so ORDER BY timestamp is a total order —
    // the distributed merge must reproduce the exact single-pod sequence (not just
    // the same set). Covers top-N oldest, top-N newest, and a full unbounded sort.
    for sql in [
        "SELECT * FROM events ORDER BY \"timestamp\" ASC LIMIT 10",
        "SELECT * FROM events ORDER BY \"timestamp\" DESC LIMIT 8",
        "SELECT * FROM events ORDER BY \"timestamp\" ASC",
    ] {
        let plan = planning.state().create_logical_plan(sql).await.unwrap();
        assert!(
            matches!(classify(&plan), DistPlan::OrderedScan { .. }),
            "`{sql}` must classify as OrderedScan, got {:?}",
            classify(&plan)
        );
        let dist = ordered(&coordinate(&planning, &runner, sql, 4).await.unwrap());
        let single = ordered(&planning.sql(sql).await.unwrap().collect().await.unwrap());
        assert_eq!(
            dist, single,
            "distributed ordered result must match single-pod IN ORDER for `{sql}`"
        );
    }
}

/// #82 ordered aggregates: GROUP BY under a top-level ORDER BY [LIMIT]. The
/// data is built ADVERSARIALLY for the naive ship-the-original-query approach:
/// each file has unique hosts with 20 rows and a `common` host with 8, so any
/// 1–2-file shard's local top-2 is uniques only — `common` (globally #1 with
/// 48 rows) appears in NO shard's local top-2. Only the #82 rewrite (workers
/// return complete groups, coordinator orders global totals) gets this right.
#[tokio::test]
async fn coordinated_ordered_aggregates_match_unsharded_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    for i in 0..6 {
        let mut batch = Vec::new();
        for j in 0..20 {
            let mut e = Event::now(format!("unique row {j}"));
            e.host = format!("unique-{i}");
            e.timestamp = base + Duration::seconds((i * 100 + j) as i64);
            batch.push(e);
        }
        for j in 0..8 {
            let mut e = Event::now(format!("common row {j}"));
            e.host = "common".to_string();
            e.timestamp = base + Duration::seconds((i * 100 + 50 + j) as i64);
            batch.push(e);
        }
        ice.append_events(&batch).await.unwrap();
    }

    let planning = session_context_with(None, None);
    ice.register_with_datafusion(&planning).await.unwrap();
    register_udfs(&planning);
    let runner = LocalRunner { ice: ice.clone() };

    // Host tie-breakers keep every ordering total, so the distributed result
    // must reproduce the exact single-pod sequence.
    for sql in [
        "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC, host ASC LIMIT 2",
        "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC, host ASC",
        "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY host ASC LIMIT 3",
        "SELECT host, min(\"timestamp\") AS lo FROM events GROUP BY host ORDER BY lo ASC, host ASC LIMIT 2",
        "SELECT host, count(*) AS n FROM events WHERE raw LIKE '%row%' GROUP BY host ORDER BY n DESC, host ASC LIMIT 4",
    ] {
        let plan = planning.state().create_logical_plan(sql).await.unwrap();
        match classify(&plan) {
            DistPlan::OrderedAggregate { worker_sql, .. } => {
                let lowered = worker_sql.to_lowercase();
                assert!(
                    !lowered.contains("order by") && !lowered.contains("limit"),
                    "worker SQL must be sort/limit-stripped, got `{worker_sql}`"
                );
            }
            other => panic!("`{sql}` must classify as OrderedAggregate, got {other:?}"),
        }
        for shards in [3, 4] {
            let dist = ordered(&coordinate(&planning, &runner, sql, shards).await.unwrap());
            let single = ordered(&planning.sql(sql).await.unwrap().collect().await.unwrap());
            assert_eq!(
                dist, single,
                "distributed ordered aggregate must match single-pod IN ORDER for `{sql}` ({shards} shards)"
            );
        }
    }

    // The adversarial property itself: the global winner tops the merged
    // result even though it is in no shard's local top-2.
    let sql =
        "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC, host ASC LIMIT 1";
    let top = ordered(&coordinate(&planning, &runner, sql, 3).await.unwrap());
    assert_eq!(
        top[0][0], "common",
        "global top group must survive the merge: {top:?}"
    );
    assert_eq!(top[0][1], "48");
}

#[tokio::test]
async fn nondistributable_shapes_fall_back_and_match() {
    let (_tmp, ice) = warehouse().await;
    // avg (no single-column merge), DISTINCT, computed projection over an
    // aggregate, and an ORDERED unmergeable aggregate (#82 distributes the
    // mergeable ones; avg under ORDER BY must still fall back).
    let planning = session_context_with(None, None);
    ice.register_with_datafusion(&planning).await.unwrap();
    for sql in [
        "SELECT avg(length(raw)) FROM events",
        "SELECT DISTINCT host FROM events",
        "SELECT count(*) * 2 FROM events", // computed projection ⇒ not pass-through
        "SELECT host, avg(length(raw)) AS a FROM events GROUP BY host ORDER BY a", // ordered + unmergeable
    ] {
        let plan = planning.state().create_logical_plan(sql).await.unwrap();
        assert_eq!(
            classify(&plan),
            DistPlan::Local,
            "expected fallback for `{sql}`"
        );
        assert_same(&ice, sql, 3).await; // fallback still returns the right answer
    }
}

/// #86: classify is STRUCTURAL — any single-relation query is a fan-out
/// candidate (user indexes distribute; the table-safety gate lives in
/// `distributed_inner`, which only routes `events`/managed indexes here).
/// No-table queries must still be Local (a `SELECT 1` fanned out would be
/// multiplied by the cross-shard union), as must multi-relation joins.
#[tokio::test]
async fn classify_is_structural_single_relation() {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["a"]))]).unwrap();
    ctx.register_table(
        "custom-idx",
        Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap()),
    )
    .unwrap();
    ctx.register_table(
        "other",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();

    // A single non-events relation is now structurally distributable.
    let p = ctx.sql("SELECT value FROM \"custom-idx\"").await.unwrap();
    assert!(
        matches!(classify(p.logical_plan()), DistPlan::Scan { .. }),
        "single-relation scan is a fan-out candidate"
    );

    // No-table and cross-relation queries stay Local.
    let p2 = ctx.sql("SELECT 1 AS x").await.unwrap();
    assert!(
        matches!(classify(p2.logical_plan()), DistPlan::Local),
        "no-table query must be Local"
    );
    let p3 = ctx
        .sql("SELECT a.value FROM \"custom-idx\" a JOIN other b ON a.value = b.value")
        .await
        .unwrap();
    assert!(
        matches!(classify(p3.logical_plan()), DistPlan::Local),
        "cross-relation join must be Local"
    );
}
