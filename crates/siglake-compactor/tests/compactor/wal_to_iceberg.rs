//! Compactor integration: WAL segments on disk → Iceberg snapshots queryable
//! via DataFusion.

use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use parquet::file::reader::FileReader;

use siglake_compactor::Compactor;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_wal::{
    list_sealed, recover_orphaned_partials, WalWriter, ACTIVE_DIR, COMMITTED_DIR, PROCESSING_DIR,
    SEALED_DIR,
};

fn synth(n: usize) -> Vec<Event> {
    (0..n).map(|i| Event::now(format!("e{i}"))).collect()
}

async fn count_events(ice: &IcebergContext) -> i64 {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn run_once_drains_single_segment() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    // Seed one sealed WAL segment.
    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-1", 100, Duration::from_secs(60)).unwrap();
        let seg = w.append_events(&synth(100)).unwrap();
        assert!(seg.is_some(), "should have rolled at threshold");
    }
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 1);

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let compactor = Compactor::new(&wal_dir, ice.clone());
    let n = compactor.run_once().await.unwrap();
    assert_eq!(n, 1);

    // Sealed should be empty; processing should be empty (file deleted on success).
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 0);
    let processing = wal_dir.join(PROCESSING_DIR);
    let proc_count = std::fs::read_dir(&processing).unwrap().count();
    assert_eq!(proc_count, 0, "processing/ should be empty after success");

    assert_eq!(count_events(&ice).await, 100);
}

/// An untyped commit failure keeps the existing bounded retry behavior: a
/// later attempt in the same cycle reaches the real commit path.
#[tokio::test]
async fn a_transient_commit_error_is_retried_in_the_same_cycle() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    {
        let mut writer =
            WalWriter::with_thresholds(&wal_dir, "ing-transient", 1, Duration::from_secs(60))
                .unwrap();
        writer
            .append_events(&[Event::now("repair me")])
            .unwrap()
            .expect("one row seals the segment");
    }

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let committed = {
        let _guard = metrics::set_default_local_recorder(&recorder);
        Compactor::new(&wal_dir, ice.clone())
            .with_drain_concurrency(1)
            .with_drain_cycle_budget(Duration::from_secs(2))
            .with_transient_fs_commit_failures_for_test(1)
            .run_once()
            .await
            .unwrap()
    };
    assert_eq!(committed, 1, "the retried segment commits in this cycle");
    let commit_errors: u64 = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == "siglake_compactor_cycles_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == "commit_error")
        })
        .map(|(key, _, _, value)| match value {
            DebugValue::Counter(n) => n,
            other => panic!("{} must be a counter, got {other:?}", key.key().name()),
        })
        .sum();
    assert_eq!(
        commit_errors, 1,
        "the successful commit must follow the one injected failed attempt"
    );
    assert_eq!(count_events(&ice).await, 1);
}

#[tokio::test]
async fn commit_batching_defers_below_target_then_commits() {
    use siglake_compactor::CommitBatchPolicy;

    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    // One small sealed segment, far below any realistic byte target.
    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-1", 100, Duration::from_secs(60)).unwrap();
        w.append_events(&synth(100)).unwrap();
    }
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 1);

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    // Target 1 GiB, age floor 1 h: the lone small segment clears neither
    // trigger, so the gate defers — nothing committed, segment retained.
    let deferring = Compactor::new(&wal_dir, ice.clone()).with_commit_batching(CommitBatchPolicy {
        target_bytes: 1024 * 1024 * 1024,
        max_age: Duration::from_secs(3600),
    });
    let n = deferring.run_once().await.unwrap();
    assert_eq!(n, 0, "below target + under age floor ⇒ deferred");
    assert_eq!(
        list_sealed(&wal_dir).unwrap().len(),
        1,
        "segment stays sealed"
    );
    assert_eq!(
        count_events(&ice).await,
        0,
        "nothing committed while deferring"
    );

    // A tiny target makes the same queue immediately commit-worthy.
    let committing =
        Compactor::new(&wal_dir, ice.clone()).with_commit_batching(CommitBatchPolicy {
            target_bytes: 1,
            max_age: Duration::from_secs(3600),
        });
    let n = committing.run_once().await.unwrap();
    assert_eq!(n, 1, "target reached ⇒ commit");
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 0);
    assert_eq!(count_events(&ice).await, 100);
}

#[tokio::test]
async fn commit_batching_age_floor_forces_commit() {
    use siglake_compactor::CommitBatchPolicy;

    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-1", 100, Duration::from_secs(60)).unwrap();
        w.append_events(&synth(100)).unwrap();
    }
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    // Unreachable byte target, but a zero age floor: the freshness floor
    // alone must force the commit so low-traffic tables stay visible.
    let c = Compactor::new(&wal_dir, ice.clone()).with_commit_batching(CommitBatchPolicy {
        target_bytes: 1024 * 1024 * 1024,
        max_age: Duration::ZERO,
    });
    let n = c.run_once().await.unwrap();
    assert_eq!(n, 1, "age floor reached ⇒ commit despite target unmet");
    assert_eq!(count_events(&ice).await, 100);
}

#[tokio::test]
async fn run_once_drains_multiple_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    // Three sealed segments of different sizes.
    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-1", 10, Duration::from_secs(60)).unwrap();
        // Each call to append_events with 10 rows hits the threshold and
        // rolls. Three calls = three sealed segments.
        for _ in 0..3 {
            w.append_events(&synth(10)).unwrap();
        }
    }
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 3);

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let compactor = Compactor::new(&wal_dir, ice.clone());
    let n = compactor.run_once().await.unwrap();
    assert_eq!(n, 3);

    assert_eq!(count_events(&ice).await, 30);
}

#[tokio::test]
async fn run_once_commits_a_torn_partials_prefix_with_healthy_siblings() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    const OWNER: &str = "ing-torn-drain";

    // Simulate a SIGKILL after the first append was acknowledged and six bytes
    // of the next message reached disk. The partial remains byte-for-byte as
    // the crash left it when recovery promotes it.
    let mut torn =
        WalWriter::with_thresholds(&wal_dir, OWNER, 1_000_000, Duration::from_secs(3600)).unwrap();
    torn.append_events(&synth(3)).unwrap();
    assert!(torn.sync_active().unwrap());
    let partial = std::fs::read_dir(wal_dir.join(ACTIVE_DIR))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("partial"))
        .expect("writer has one active partial");
    let prefix_len = std::fs::metadata(&partial).unwrap().len();
    torn.append_events(&synth(5)).unwrap();
    assert!(torn.sync_active().unwrap());
    std::mem::forget(torn);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&partial)
        .unwrap()
        .set_len(prefix_len + 6)
        .unwrap();
    assert_eq!(recover_orphaned_partials(&wal_dir, OWNER).unwrap(), 1);

    // A healthy sibling is claimed in the same local commit batch. One torn
    // tail must not release either segment back to sealed/ for another cycle.
    {
        let mut healthy =
            WalWriter::with_thresholds(&wal_dir, "ing-healthy", 2, Duration::from_secs(60))
                .unwrap();
        healthy
            .append_events(&synth(2))
            .unwrap()
            .expect("healthy sibling seals");
    }
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 2);

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let drained = Compactor::new(&wal_dir, ice.clone())
        .run_once()
        .await
        .unwrap();
    assert_eq!(drained, 2, "both claimed siblings commit in one cycle");
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 0);
    assert_eq!(count_events(&ice).await, 5, "3 recovered + 2 healthy");
    assert_eq!(
        std::fs::read_dir(wal_dir.join(COMMITTED_DIR))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("arrow"))
            .count(),
        2,
        "both source segments finish instead of being released for retry"
    );
}

#[tokio::test]
async fn run_once_respects_fs_batch_segment_cap() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-1", 10, Duration::from_secs(60)).unwrap();
        for _ in 0..3 {
            w.append_events(&synth(10)).unwrap();
        }
    }
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 3);

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let compactor = Compactor::new(&wal_dir, ice.clone()).with_fs_batch_limits(2, 0);
    // Continuous dispatch: the segment cap bounds each COMMIT batch (2+1 here),
    // while one pass keeps refilling until the queue is drained — the old
    // one-batch-per-cycle pacing left the remainder for the next cycle.
    let n = compactor.run_once().await.unwrap();
    assert_eq!(n, 3, "one pass drains the whole queue in capped batches");
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 0);
    assert_eq!(count_events(&ice).await, 30);
}

#[tokio::test]
async fn run_once_respects_fs_batch_byte_cap_but_always_claims_one_segment() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-1", 10, Duration::from_secs(60)).unwrap();
        for _ in 0..3 {
            w.append_events(&synth(10)).unwrap();
        }
    }
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 3);

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let compactor = Compactor::new(&wal_dir, ice.clone()).with_fs_batch_limits(0, 1);
    // A 1-byte cap forces one segment per COMMIT batch; the continuous pass
    // still drains all three (three single-segment commits).
    let n = compactor.run_once().await.unwrap();
    assert_eq!(n, 3, "three capped single-segment commits in one pass");
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 0);
    assert_eq!(count_events(&ice).await, 30);
}

#[tokio::test]
async fn run_once_with_no_segments_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    std::fs::create_dir_all(wal_dir.join(SEALED_DIR)).unwrap();

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let compactor = Compactor::new(&wal_dir, ice.clone());
    let n = compactor.run_once().await.unwrap();
    assert_eq!(n, 0);
    assert_eq!(count_events(&ice).await, 0);
}

/// Find every Parquet data file under the warehouse, recursing into the
/// `day_ts=YYYY-MM-DD/` partition subdirectories.
fn data_files(warehouse: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            for entry in rd.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    out.push(p);
                }
            }
        }
    }
    let dir = warehouse.join("siglake/events/data");
    let mut out = Vec::new();
    walk(&dir, &mut out);
    out.sort();
    out
}

#[tokio::test]
async fn rows_in_output_parquet_are_time_ordered_ascending() {
    use chrono::{TimeZone, Utc};
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    // Three segments with timestamps deliberately interleaved so the
    // compactor must do a real sort (not just preserve segment order).
    {
        let mut w =
            siglake_wal::WalWriter::with_thresholds(&wal_dir, "ing-1", 4, Duration::from_secs(60))
                .unwrap();
        let mk = |secs: i64, host: &str| Event {
            timestamp: Utc.timestamp_opt(secs, 0).unwrap(),
            host: host.into(),
            source: "src".into(),
            sourcetype: "st".into(),
            index: "main".into(),
            raw: format!("t={secs} h={host}"),
            attributes: None,
        };
        // Segment A: oldest two events of three different hosts
        w.append_events(&[
            mk(1000, "h2"),
            mk(1000, "h1"),
            mk(2000, "h3"),
            mk(3000, "h1"),
        ])
        .unwrap();
        // Segment B
        w.append_events(&[
            mk(1500, "h2"),
            mk(2500, "h2"),
            mk(3000, "h2"),
            mk(4000, "h1"),
        ])
        .unwrap();
    }
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 2);

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let n = Compactor::new(&wal_dir, ice.clone())
        .run_once()
        .await
        .unwrap();
    assert_eq!(n, 2);

    // Read the resulting Parquet file directly. With this little data
    // there'll be exactly one row group, so on-disk order is what we
    // wrote.
    let files = data_files(&warehouse);
    assert_eq!(files.len(), 1, "expected one parquet file: {files:?}");
    let bytes = std::fs::read(&files[0]).unwrap();
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    let reader = builder.build().unwrap();
    // `timestamp` is microsecond; `timestamp_ns` is the exact nanosecond and the
    // second sort key. Read both: only the pair is a total order.
    let mut all_ts: Vec<i64> = Vec::new();
    let mut all_ns: Vec<i64> = Vec::new();
    let mut all_host: Vec<String> = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let ts = siglake_core::column_nanos(batch.column_by_name("timestamp").unwrap()).unwrap();
        let ns = siglake_core::column_nanos(
            batch
                .column_by_name(siglake_core::TIMESTAMP_NS_COLUMN)
                .unwrap(),
        )
        .unwrap();
        let host = batch
            .column_by_name("host")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            all_ts.push(ts.value(i));
            all_ns.push(ns.value(i));
            all_host.push(host.value(i).to_string());
        }
    }
    // Time-series store: rows are physically ordered by the table's declared
    // sort order, `(timestamp ASC, timestamp_ns ASC)`.
    let _ = all_host;
    for w in all_ts.windows(2) {
        assert!(
            w[0] <= w[1],
            "rows must be ascending by timestamp, got {} then {}",
            w[0],
            w[1]
        );
    }
    for pair in all_ts
        .iter()
        .zip(all_ns.iter())
        .collect::<Vec<_>>()
        .windows(2)
    {
        assert!(
            pair[0] <= pair[1],
            "rows must be ascending by (timestamp, timestamp_ns), got {:?} then {:?}",
            pair[0],
            pair[1]
        );
    }
}

/// Parquet-native blooms are OFF by default since 2026-08-06 (they prune
/// nothing in a time-sorted layout — see `native_blooms_enabled`). This pins
/// the default; the storage integration suite pins that explicit configuration
/// still restores them.
#[tokio::test]
async fn output_parquet_has_no_native_bloom_filters_by_default() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    {
        let mut w =
            siglake_wal::WalWriter::with_thresholds(&wal_dir, "ing-1", 50, Duration::from_secs(60))
                .unwrap();
        w.append_events(&synth(50)).unwrap();
    }
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    Compactor::new(&wal_dir, ice.clone())
        .run_once()
        .await
        .unwrap();

    let files = data_files(&warehouse);
    assert_eq!(files.len(), 1);
    let bytes = std::fs::read(&files[0]).unwrap();
    let reader =
        parquet::file::reader::SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
    let metadata = reader.metadata();

    // For every row group, check that the four dimensional columns have a
    // bloom filter offset set (and timestamp + raw do not).
    for rg in 0..metadata.num_row_groups() {
        let rg_meta = metadata.row_group(rg);
        for col_idx in 0..rg_meta.num_columns() {
            let col_meta = rg_meta.column(col_idx);
            let name = col_meta.column_path().string();
            let has_bloom = col_meta.bloom_filter_offset().is_some();
            match name.as_str() {
                "host" | "source" | "sourcetype" | "index" => {
                    assert!(
                        !has_bloom,
                        "column {name} must have NO native bloom by default \
                         (SIGLAKE_PARQUET_NATIVE_BLOOMS=on restores it)"
                    );
                }
                // `raw` and the WS-7 residual `attributes` are high-cardinality
                // text — no native token bloom (substring uses the trigram path).
                // Neither timestamp column takes one: both are range-pruned.
                "timestamp" | "timestamp_ns" | "raw" | "attributes" => {
                    assert!(
                        !has_bloom,
                        "column {name} should NOT have a bloom filter (high-cardinality)"
                    );
                }
                other => panic!("unexpected column: {other}"),
            }
        }
    }
}

#[tokio::test]
async fn segments_committed_in_lex_order() {
    // uuidv7 sorts lexicographically by time, so segments are processed
    // oldest-first. We don't strictly need this for correctness (each
    // commit is independent), but it keeps query results time-ordered.
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-1", 1, Duration::from_secs(60)).unwrap();
        for i in 0..5 {
            // Distinguish segments by their raw payload.
            w.append_events(&[Event::now(format!("seg{i}"))]).unwrap();
        }
    }
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 5);

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let compactor = Compactor::new(&wal_dir, ice.clone());
    let n = compactor.run_once().await.unwrap();
    assert_eq!(n, 5);
    assert_eq!(count_events(&ice).await, 5);
}

/// Count `.arrow` segments in a WAL subdirectory, matching what the sweep
/// itself considers (sidecars and non-segment files are not segments).
fn arrow_files(dir: &std::path::Path) -> usize {
    match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("arrow"))
            .count(),
        Err(_) => 0,
    }
}

/// A segment enters `committed/` at age 0, so it is never sweepable on the
/// cycle that commits it. If the sweep only runs on cycles that also drain,
/// the trailing retention window is stranded the moment writes stop — which
/// is what left 666 MB pinned on a production EFS for eight weeks.
#[tokio::test]
async fn idle_cycle_sweeps_the_committed_tail() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-1", 100, Duration::from_secs(60)).unwrap();
        w.append_events(&synth(100)).unwrap();
    }
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 1);

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let committed = wal_dir.join(COMMITTED_DIR);

    // Drain under a floor nothing can clear in-cycle: the segment lands in
    // committed/ and stays there, which is the correct steady-state behaviour.
    let draining = Compactor::with_retention(&wal_dir, ice.clone(), Duration::from_secs(3600));
    assert_eq!(draining.run_once().await.unwrap(), 1);
    assert_eq!(
        arrow_files(&committed),
        1,
        "freshly committed segment is below the floor and must be retained"
    );

    // Writes stop. Every subsequent cycle for this directory is idle, so an
    // idle cycle is the only one that will ever see the tail come due.
    let sweeping = Compactor::with_retention(&wal_dir, ice.clone(), Duration::ZERO);
    assert_eq!(
        sweeping.run_once().await.unwrap(),
        0,
        "nothing sealed left to drain"
    );
    assert_eq!(
        arrow_files(&committed),
        0,
        "idle cycle must still sweep retention-expired segments"
    );
}

/// Batching defers the commit, not the sweep: a low-rate tenant can sit behind
/// the accumulation gate for many consecutive cycles, and `committed/`
/// retention is independent of when the next commit happens.
#[tokio::test]
async fn deferred_cycle_still_sweeps_the_committed_tail() {
    use siglake_compactor::CommitBatchPolicy;

    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-1", 100, Duration::from_secs(60)).unwrap();
        w.append_events(&synth(100)).unwrap();
    }
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let committed = wal_dir.join(COMMITTED_DIR);

    let draining = Compactor::with_retention(&wal_dir, ice.clone(), Duration::from_secs(3600));
    assert_eq!(draining.run_once().await.unwrap(), 1);
    assert_eq!(arrow_files(&committed), 1);

    // A new segment arrives but is far below the batch target, so this cycle
    // gets past the empty check and then defers at the accumulation gate.
    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-2", 100, Duration::from_secs(60)).unwrap();
        w.append_events(&synth(100)).unwrap();
    }
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 1);

    let deferring = Compactor::with_retention(&wal_dir, ice.clone(), Duration::ZERO)
        .with_commit_batching(CommitBatchPolicy {
            target_bytes: 1024 * 1024 * 1024,
            max_age: Duration::from_secs(3600),
        });
    assert_eq!(
        deferring.run_once().await.unwrap(),
        0,
        "below target + under age floor ⇒ deferred"
    );
    assert_eq!(
        list_sealed(&wal_dir).unwrap().len(),
        1,
        "deferred segment stays sealed"
    );
    assert_eq!(
        arrow_files(&committed),
        0,
        "deferring the commit must not defer the sweep"
    );
}

/// The FIFTH exit, which the sweep-on-every-cycle change missed: an index whose
/// `ensure_index` cannot resolve it `continue`s past the drain, and does so on
/// every later cycle too — so its `committed/` tail is stranded for exactly the
/// reason the other four were.
///
/// Lower stakes than those four (this path has pending work and means a
/// misconfigured index rather than a quiet one) but the same shape, and an index
/// stuck here is stuck permanently.
///
/// `ensure_index` returns None when the index was never created AND no template
/// resolves it, which is the default state for an unknown index. The branch
/// returns before anything parses a segment, so file CONTENT is irrelevant here
/// — only that `sealed/` is non-empty (to get past the idle-index branch) and
/// that `committed/` holds a tail.
#[tokio::test]
async fn unresolvable_index_still_sweeps_its_committed_tail() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    // wal_dir/<tenant>/<index>/{sealed,committed}. The tenant dir needs its own
    // `sealed/` to be discovered at all — `list_tenant_dirs` keys on that.
    std::fs::create_dir_all(wal_dir.join("t1").join("sealed")).unwrap();
    let index_dir = wal_dir.join("t1").join("never-created");
    std::fs::create_dir_all(index_dir.join("sealed")).unwrap();
    std::fs::create_dir_all(index_dir.join(COMMITTED_DIR)).unwrap();
    std::fs::write(
        index_dir.join("sealed").join("ing-1-000000000001.arrow"),
        b"x",
    )
    .unwrap();
    std::fs::write(
        index_dir
            .join(COMMITTED_DIR)
            .join("ing-1-000000000000.arrow"),
        b"x",
    )
    .unwrap();

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let committed = index_dir.join(COMMITTED_DIR);
    assert_eq!(arrow_files(&committed), 1, "a tail to strand");

    // Retention ZERO: the tail is due immediately, so anything that reaches the
    // sweep clears it. `ensure_index` cannot resolve `never-created`, so this
    // cycle takes the unresolved branch.
    let c = Compactor::with_retention(&wal_dir, ice.clone(), Duration::ZERO);
    c.run_once().await.unwrap();
    assert_eq!(
        arrow_files(&committed),
        0,
        "an index that cannot be resolved must still have its retention swept — \
         it will take this same branch on every future cycle"
    );
    assert_eq!(
        list_sealed(&index_dir).unwrap().len(),
        1,
        "and its pending segment must be left alone, not silently dropped"
    );
}

/// #3143: one segment whose bytes never decode used to fail every batch it was
/// claimed into, forever, with nothing naming the file. It is now charged for
/// its own failures, set aside under `poison/` once it has spent them, and its
/// batch siblings commit on the next cycle.
///
/// Drives the whole containment: repeated cycles, a restart, the automatic
/// orphan disposition that must not touch it, the consumed proof that must not
/// claim it, and the operator's deliberate way back.
#[tokio::test]
async fn an_undecodable_segment_is_set_aside_and_its_siblings_commit() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use siglake_wal::{list_poisoned, read_poison_note, requeue_poisoned_segment, POISON_DIR};

    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    // Three sealed segments, one row each, all claimable into one batch.
    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-poison", 1, Duration::from_secs(60)).unwrap();
        for i in 0..3 {
            w.append_events(&[Event::now(format!("row-{i}"))])
                .unwrap()
                .expect("one row seals the segment");
        }
    }
    let sealed = list_sealed(&wal_dir).unwrap();
    assert_eq!(sealed.len(), 3);

    // The middle one is truncated in place — a torn restore, a bad sector and
    // an unknown frame version all arrive at the same read error.
    let poisoned_name = sealed[1].file_name().unwrap().to_str().unwrap().to_string();
    let intact_bytes = std::fs::read(&sealed[1]).unwrap();
    std::fs::write(&sealed[1], &intact_bytes[..8]).unwrap();
    let corrupt_bytes = std::fs::read(&sealed[1]).unwrap();

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let compactor = Compactor::new(&wal_dir, ice.clone()).with_poison_attempts(2);
    {
        let _guard = metrics::set_default_local_recorder(&recorder);
        // A pass re-lists and re-claims whatever it released until its budget
        // runs out, so the two attempts and the commit of what is left happen
        // inside one cycle: batch of three fails, batch of three fails again
        // and sets the segment aside, batch of two commits.
        assert_eq!(compactor.run_once().await.unwrap(), 2);
    }
    assert_eq!(
        count_events(&ice).await,
        2,
        "the healthy siblings must not be held hostage"
    );
    assert!(list_sealed(&wal_dir).unwrap().is_empty());
    assert_eq!(
        std::fs::read_dir(wal_dir.join(PROCESSING_DIR))
            .unwrap()
            .count(),
        0
    );

    let held = wal_dir.join(POISON_DIR).join(&poisoned_name);
    assert_eq!(list_poisoned(&wal_dir).unwrap(), vec![held.clone()]);
    assert_eq!(
        std::fs::read(&held).unwrap(),
        corrupt_bytes,
        "the original bytes are preserved, not repaired and not deleted"
    );
    let note = read_poison_note(&held).expect("a set-aside segment carries its verdict");
    assert_eq!(note.segment, poisoned_name);
    assert_eq!(note.attempts, 2);
    assert!(!note.reason.is_empty(), "the read error is recorded");

    let snapshot = snapshotter.snapshot().into_vec();
    let counter: u64 = snapshot
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == "siglake_compactor_segments_poisoned_total")
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(n) => *n,
            other => panic!("segments_poisoned_total must be a counter, got {other:?}"),
        })
        .sum();
    assert_eq!(counter, 1, "one set-aside, counted once");
    let gauge: f64 = snapshot
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == "siglake_compactor_segments_poisoned")
        .map(|(_, _, _, value)| match value {
            DebugValue::Gauge(n) => n.into_inner(),
            other => panic!("segments_poisoned must be a gauge, got {other:?}"),
        })
        .fold(0.0, f64::max);
    assert_eq!(
        gauge, 1.0,
        "and reported as a level an operator can alert on"
    );

    // The consumed proof speaks only for what committed: a set-aside segment
    // is not claimed as consumed, so a requeue of it is still a fresh commit.
    let (consumed, _floor) = ice
        .consumed_segments_and_history_floor("events")
        .await
        .unwrap();
    assert!(
        !consumed.contains(&poisoned_name),
        "a segment that never committed must not be proven consumed"
    );
    for path in [&sealed[0], &sealed[2]] {
        let name = path.file_name().unwrap().to_str().unwrap();
        assert!(
            consumed.contains(name),
            "{name} committed and must be proven"
        );
    }

    // A restart re-runs startup recovery and every cycle re-runs the orphan
    // disposition. Neither may requeue or delete the set-aside.
    let restarted = Compactor::new(&wal_dir, ice.clone()).with_poison_attempts(2);
    assert_eq!(restarted.run_once().await.unwrap(), 0);
    assert_eq!(list_poisoned(&wal_dir).unwrap(), vec![held.clone()]);
    assert_eq!(std::fs::read(&held).unwrap(), corrupt_bytes);
    assert!(
        list_sealed(&wal_dir).unwrap().is_empty(),
        "never auto-requeued"
    );
    assert_eq!(count_events(&ice).await, 2);

    // The operator's way back: fix the file, then requeue it deliberately.
    std::fs::write(&held, &intact_bytes).unwrap();
    let back = requeue_poisoned_segment(&held).unwrap();
    assert_eq!(back, wal_dir.join("sealed").join(&poisoned_name));
    assert!(list_poisoned(&wal_dir).unwrap().is_empty());
    assert_eq!(restarted.run_once().await.unwrap(), 1);
    assert_eq!(
        count_events(&ice).await,
        3,
        "a repaired segment commits like any other"
    );
}

/// Count `siglake_compactor_cycles_total{outcome=…}` in a snapshot. Each failed
/// batch increments `commit_error` once, so over one pass that is the number of
/// claim attempts the pass spent on a failing set.
fn cycle_outcomes(snapshot: metrics_util::debugging::Snapshot, outcome: &str) -> u64 {
    snapshot
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == "siglake_compactor_cycles_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "outcome" && l.value() == outcome)
        })
        .map(|(_, _, _, value)| match value {
            metrics_util::debugging::DebugValue::Counter(n) => n,
            other => panic!("cycles_total must be a counter, got {other:?}"),
        })
        .sum()
}

/// #4651: a pass whose commits keep failing fast used to re-claim the same
/// segments — a rename and two fsyncs per segment each way — for its whole
/// cycle budget, because the released batch came straight back on the re-list
/// and the spin guard compared the fresh list against an exhausted snapshot.
/// The attempts one pass spends on a segment are now bounded and small, and the
/// next cycle starts it over.
#[tokio::test]
async fn a_persistently_failing_batch_is_not_re_claimed_for_the_whole_cycle() {
    use metrics_util::debugging::DebuggingRecorder;

    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-spin", 1, Duration::from_secs(60)).unwrap();
        for i in 0..2 {
            w.append_events(&[Event::now(format!("row-{i}"))])
                .unwrap()
                .expect("one row seals the segment");
        }
    }

    // Every attempt fails, fast and untyped, naming no segment: a catalog
    // conflict that keeps recurring, a store that is refusing writes. Before
    // the bound this pass made over ten thousand claims in its 30 s budget.
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let compactor = Compactor::new(&wal_dir, ice.clone())
        .with_drain_concurrency(1)
        .with_drain_cycle_budget(Duration::from_secs(30))
        .with_transient_fs_commit_failures_for_test(usize::MAX);
    let started = std::time::Instant::now();
    {
        let _guard = metrics::set_default_local_recorder(&recorder);
        assert!(
            compactor.run_once().await.is_err(),
            "nothing commits while every attempt fails"
        );
    }
    let elapsed = started.elapsed();
    let attempts = cycle_outcomes(snapshotter.snapshot(), "commit_error");
    assert!(
        (1..=3).contains(&attempts),
        "a pass must spend a small bounded number of claims on a set that keeps failing, got \
         {attempts}"
    );
    assert!(
        elapsed < Duration::from_secs(30),
        "and must not sit in its top-up loop until the cycle budget expires ({elapsed:?})"
    );
    // The segments are intact in sealed/, charged nothing durable: the cause
    // named no segment, so #3143's ledger holds nothing against them.
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 2);
    assert!(siglake_wal::list_poisoned(&wal_dir).unwrap().is_empty());
    assert_eq!(
        std::fs::read_dir(wal_dir.join(PROCESSING_DIR))
            .unwrap()
            .count(),
        0,
        "every withheld segment is back in sealed/, not stranded in processing/"
    );

    // The bound is per pass, not a durable verdict: the next cycle tries the
    // same segments again and commits them once the cause clears.
    let healthy = Compactor::new(&wal_dir, ice.clone()).with_drain_concurrency(1);
    assert_eq!(healthy.run_once().await.unwrap(), 2);
    assert_eq!(count_events(&ice).await, 2);
}

/// #4651 must not cost the same-cycle retry an ordinary failure gets.
/// `a_transient_commit_error_is_retried_in_the_same_cycle` pins one failure;
/// this pins the edge of the bound, two failures followed by the attempt that
/// commits, so a pass that keeps failing is stopped by the third attempt and
/// not by the second.
#[tokio::test]
async fn the_attempt_bound_leaves_room_for_two_same_cycle_retries() {
    use metrics_util::debugging::DebuggingRecorder;

    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    {
        let mut writer =
            WalWriter::with_thresholds(&wal_dir, "ing-retry", 1, Duration::from_secs(60)).unwrap();
        for i in 0..2 {
            writer
                .append_events(&[Event::now(format!("row-{i}"))])
                .unwrap()
                .expect("one row seals the segment");
        }
    }

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let compactor = Compactor::new(&wal_dir, ice.clone())
        .with_drain_concurrency(1)
        .with_drain_cycle_budget(Duration::from_secs(5))
        .with_transient_fs_commit_failures_for_test(2);
    let committed = {
        let _guard = metrics::set_default_local_recorder(&recorder);
        compactor.run_once().await.unwrap()
    };
    assert_eq!(
        committed, 2,
        "the batch commits on the third attempt, inside the same cycle"
    );
    assert_eq!(
        cycle_outcomes(snapshotter.snapshot(), "commit_error"),
        2,
        "after exactly the two injected failures"
    );
    assert_eq!(count_events(&ice).await, 2);
    assert!(list_sealed(&wal_dir).unwrap().is_empty());
}

/// #4651 against #3143's accounting, with the set-aside disabled — the card's
/// measured case. The unreadable segment still fails every batch it joins and
/// still never leaves `sealed/`, but the pass no longer re-claims it until the
/// budget runs out, and the failures it is charged are bounded per cycle rather
/// than by how fast the read fails.
#[tokio::test]
async fn a_bounded_pass_charges_an_unreadable_segment_a_bounded_number_of_reads() {
    use metrics_util::debugging::DebuggingRecorder;

    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    {
        let mut w = WalWriter::with_thresholds(&wal_dir, "ing-bounded", 1, Duration::from_secs(60))
            .unwrap();
        for i in 0..3 {
            w.append_events(&[Event::now(format!("row-{i}"))])
                .unwrap()
                .expect("one row seals the segment");
        }
    }
    let sealed = list_sealed(&wal_dir).unwrap();
    let intact = std::fs::read(&sealed[1]).unwrap();
    std::fs::write(&sealed[1], &intact[..8]).unwrap();

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let compactor = Compactor::new(&wal_dir, ice.clone())
        .with_poison_attempts(0)
        .with_drain_cycle_budget(Duration::from_secs(30));
    {
        let _guard = metrics::set_default_local_recorder(&recorder);
        assert!(compactor.run_once().await.is_err());
    }
    let attempts = cycle_outcomes(snapshotter.snapshot(), "commit_error");
    assert!(
        (1..=3).contains(&attempts),
        "the unreadable segment costs a bounded number of claims per cycle, got {attempts}"
    );
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 3);
    assert!(siglake_wal::list_poisoned(&wal_dir).unwrap().is_empty());
    assert_eq!(count_events(&ice).await, 0);

    // With the budget restored, the same directory converges: the segment
    // spends its attempts, is set aside, and its siblings commit — #3143's
    // property, which the bound must leave intact.
    let containing = Compactor::new(&wal_dir, ice.clone()).with_poison_attempts(3);
    assert_eq!(containing.run_once().await.unwrap(), 2);
    assert_eq!(siglake_wal::list_poisoned(&wal_dir).unwrap().len(), 1);
    assert_eq!(count_events(&ice).await, 2);
}

/// The negative control for the set-aside. With the attempt budget disabled
/// the queue behaves exactly as it did before #3143: the unreadable segment
/// takes every batch it joins down with it, cycle after cycle, and the rows of
/// its healthy siblings never become queryable.
#[tokio::test]
async fn without_the_set_aside_one_unreadable_segment_blocks_its_siblings() {
    use siglake_wal::list_poisoned;

    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    {
        let mut w = WalWriter::with_thresholds(&wal_dir, "ing-control", 1, Duration::from_secs(60))
            .unwrap();
        for i in 0..3 {
            w.append_events(&[Event::now(format!("row-{i}"))])
                .unwrap()
                .expect("one row seals the segment");
        }
    }
    let sealed = list_sealed(&wal_dir).unwrap();
    let intact = std::fs::read(&sealed[1]).unwrap();
    std::fs::write(&sealed[1], &intact[..8]).unwrap();

    let compactor = Compactor::new(&wal_dir, ice.clone())
        .with_poison_attempts(0)
        .with_drain_cycle_budget(Duration::from_millis(100));
    for cycle in 0..3 {
        assert!(
            compactor.run_once().await.is_err(),
            "cycle {cycle} commits nothing"
        );
        assert_eq!(list_sealed(&wal_dir).unwrap().len(), 3);
        assert!(list_poisoned(&wal_dir).unwrap().is_empty());
        assert_eq!(count_events(&ice).await, 0);
    }
}
