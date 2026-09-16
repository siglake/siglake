//! B.1.3 parallel drain: with `SIGLAKE_DRAIN_CONCURRENCY > 1` a single cycle
//! claims multiple bounded batches and commits them concurrently. The gates:
//! every row lands exactly once (no loss, no double-apply), the WAL lifecycle
//! stays clean (sealed/ and processing/ empty afterward), and — the subtle one —
//! the cumulative side-object aggregates survive concurrent commits (the
//! storage-side `side_agg_lock` serializes the read-modify-write; without it a
//! racing commit loses increments and the Tier-1 total==total-records guard
//! rejects the aggregate forever).

use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use siglake_compactor::Compactor;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_wal::{list_sealed, WalWriter, PROCESSING_DIR};

fn synth(n: usize, tag: &str) -> Vec<Event> {
    (0..n)
        .map(|i| {
            let mut e = Event::now(format!("{tag} row {i}"));
            e.sourcetype = if i % 2 == 0 { "app:json" } else { "syslog" }.into();
            e
        })
        .collect()
}

/// Continuous dispatch: one `run_once` drains MORE batches than the concurrency
/// target by topping up as commits land (9 segments at 2/batch = 5 batches
/// through a 3-wide pipeline) — the old barrier design did at most
/// `concurrency` batches per cycle.
#[tokio::test]
async fn continuous_dispatch_refills_past_the_concurrency_target() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    const SEGS: usize = 9;
    const ROWS: usize = 30;
    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-2", ROWS, Duration::from_secs(60)).unwrap();
        for s in 0..SEGS {
            assert!(w
                .append_events(&synth(ROWS, &format!("r{s}")))
                .unwrap()
                .is_some());
        }
    }
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let compactor = Compactor::new(&wal_dir, ice.clone())
        .with_fs_batch_limits(2, 0)
        .with_drain_concurrency(3);
    let n = compactor.run_once().await.unwrap();
    assert_eq!(
        n, SEGS,
        "continuous refill drains all {SEGS} segments in one pass"
    );
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 0);

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let batches = ctx
        .sql("SELECT count(*) AS n, count(DISTINCT raw) AS d FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let arr = |i: usize| {
        batches[0]
            .column(i)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0) as usize
    };
    assert_eq!(arr(0), SEGS * ROWS, "all rows committed");
    assert_eq!(arr(1), SEGS * ROWS, "no row committed twice");
}

#[tokio::test]
async fn parallel_drain_commits_all_batches_exactly_once() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    // Six sealed segments of 40 rows each (roll threshold = 40).
    const SEGS: usize = 6;
    const ROWS: usize = 40;
    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-1", ROWS, Duration::from_secs(60)).unwrap();
        for s in 0..SEGS {
            assert!(
                w.append_events(&synth(ROWS, &format!("seg{s}")))
                    .unwrap()
                    .is_some(),
                "each append should roll one sealed segment"
            );
        }
    }
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), SEGS);

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    // Cap each batch at 2 segments → one cycle claims 3 batches of 2 and
    // commits them concurrently.
    let compactor = Compactor::new(&wal_dir, ice.clone())
        .with_fs_batch_limits(2, 0)
        .with_drain_concurrency(3);
    let n = compactor.run_once().await.unwrap();
    assert_eq!(n, SEGS, "one parallel cycle drains all {SEGS} segments");

    // Lifecycle clean: nothing left sealed, nothing stuck in processing/.
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 0);
    let processing = std::fs::read_dir(wal_dir.join(PROCESSING_DIR))
        .unwrap()
        .count();
    assert_eq!(processing, 0, "processing/ must be empty after the cycle");

    // Exactly-once: every row present, none duplicated.
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let batches = ctx
        .sql("SELECT count(*) AS n, count(DISTINCT raw) AS d FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n_rows = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    let distinct = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n_rows as usize, SEGS * ROWS, "all rows committed");
    assert_eq!(distinct as usize, SEGS * ROWS, "no row committed twice");

    // Side-aggregate integrity under concurrency: the Tier-1 whole-table
    // group-count aggregate must still satisfy total == total-records (i.e. no
    // concurrent commit lost its increments) and serve the exact counts.
    let counts = {
        let rows = ice
            .grouped_counts_with_summary("events", "sourcetype", None, None)
            .await
            .unwrap()
            .expect("Tier-1 aggregate must survive concurrent commits");
        let mut v = rows.to_rows();
        v.sort();
        v
    };
    assert_eq!(
        counts,
        vec![
            (Some("app:json".into()), (SEGS * ROWS / 2) as u64),
            (Some("syslog".into()), (SEGS * ROWS / 2) as u64),
        ],
        "side-object group counts must account for every concurrent commit"
    );
}
