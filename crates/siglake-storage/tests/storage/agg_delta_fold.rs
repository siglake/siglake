//! Stage 3 of the incremental group-count aggregate
//! (`docs/DESIGN_incremental_group_count_aggregate.md`): the compactor folds
//! outstanding deltas into the wide base and GCs them.
//!
//! Three properties decide whether the fold is safe, and each has a specific
//! way of going wrong:
//!
//!   1. **No double counting.** A delta object outlives its absorption by a
//!      full cycle on purpose, so for that whole window the base contains a
//!      delta whose object is still listed. Counting it twice is the one
//!      correctness hazard in the design.
//!   2. **Order-independence.** Absorption is recorded by MEMBERSHIP, not by a
//!      high-water mark, because deltas cannot be totally ordered by arrival: a
//!      writer that stalls between its commit and its delta PUT lands one
//!      "behind" work already done. A watermark loses that delta permanently.
//!      This is not hypothetical — the first cut of this design used a
//!      watermark over Iceberg SNAPSHOT IDS, which are random.
//!   3. **Crash safety.** A crash between the fold and the deletes must leave
//!      the system correct, not merely recoverable.

use chrono::{TimeZone, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::{
    group_count_delta_rel_path, ColumnGroupCounts, FileGroupCounts, GroupCountDelta,
    IcebergContext, WideGroupCounts,
};

const DISTINCT_HOSTS: usize = 9_000;
const ROWS_PER_COMMIT: usize = 9_000;

fn ev(secs: i64, host: &str) -> Event {
    Event {
        timestamp: Utc.timestamp_opt(secs, 0).single().unwrap(),
        host: host.to_string(),
        source: "src".into(),
        sourcetype: "app:json".into(),
        index: "main".into(),
        raw: format!("request from {host}"),
        attributes: None,
    }
}

/// The one incarnation directory under the events table's `metadata/` — every
/// aggregate artifact of that incarnation lives directly under it (#2919).
/// Found rather than hardcoded, so the test keeps working if the layout moves.
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
    find(root).expect("events table aggregate dir")
}

fn delta_files(root: &std::path::Path) -> Vec<String> {
    let dir = aggregate_dir(root).join("siglake-agg-deltas");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out: Vec<String> = entries
        .filter_map(|e| Some(e.ok()?.file_name().to_str()?.to_string()))
        .collect();
    out.sort();
    out
}

fn wide(root: &std::path::Path) -> Option<WideGroupCounts> {
    let bytes = std::fs::read(aggregate_dir(root).join("siglake-agg-wide.json")).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Append one commit's worth of rows; returns the rows added.
async fn commit(ice: &IcebergContext, nth: usize) -> u64 {
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp()
        + (nth * ROWS_PER_COMMIT) as i64;
    let evs: Vec<Event> = (0..ROWS_PER_COMMIT)
        .map(|i| {
            ev(
                base + i as i64,
                &format!("host-{:06}", (i + nth) % DISTINCT_HOSTS),
            )
        })
        .collect();
    ice.append_events(&evs).await.unwrap();
    ROWS_PER_COMMIT as u64
}

/// The compactor folds with `min_backlog` 1 throughout this file: these tests
/// are about the fold's CORRECTNESS, and a threshold that made it decline would
/// only mean they silently tested nothing. The threshold's own behaviour —
/// declining a small backlog on a busy cycle — is exercised separately.
///
/// Exact `GROUP BY host` totals as served right now.
async fn served_total(ice: &IcebergContext) -> u64 {
    ice.grouped_counts_with_summary("events", "host", None, None)
        .await
        .unwrap()
        .expect("grouped counts served")
        .iter()
        .map(|(_, c)| c)
        .sum()
}

/// `host`'s total according to the AGGREGATE, or `None` when no aggregate
/// accounts for every row.
///
/// The end-to-end answer cannot stand in for this. Every way of getting the
/// aggregate wrong — a doubled delta, a skipped one — leaves a total that
/// disagrees with `record_count`, which the read guard rejects, which falls the
/// query through to a scan that returns the correct answer anyway. So a test
/// that only checks the answer passes while the aggregate is broken. (Verified
/// by breaking it: removing the double-count guard left every `served_total`
/// assertion green.) This is the assertion that fails.
async fn aggregate_total(ice: &IcebergContext) -> Option<u64> {
    ice.table_group_counts_summary("events")
        .await
        .unwrap()
        .and_then(|g| g.column_total("host"))
}

#[tokio::test]
async fn folding_absorbs_deltas_without_ever_double_counting_them() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&root).await.unwrap().with_tuning(
        siglake_storage::iceberg::IcebergTuning {
            table_group_count_cardinality: Some(2_000_000),
            ..Default::default()
        },
    );

    let mut rows = 0u64;
    for n in 0..3 {
        rows += commit(&ice, n).await;
    }
    assert_eq!(delta_files(&root).len(), 3, "three commits, three deltas");
    assert_eq!(served_total(&ice).await, rows, "exact before any fold");
    assert_eq!(
        aggregate_total(&ice).await,
        Some(rows),
        "and served BY THE AGGREGATE, from outstanding deltas alone"
    );

    // --- Cycle 1: fold. The objects must SURVIVE, absorbed but not deleted.
    let outcomes = ice.fold_group_count_deltas(1).await.unwrap();
    assert_eq!(outcomes.len(), 1, "only the events table did anything");
    let (_, o) = &outcomes[0];
    assert_eq!((o.folded, o.deleted, o.pruned), (3, 0, 0));

    let w = wide(&root).expect("wide base written");
    assert_eq!(w.absorbed.len(), 3, "all three recorded as absorbed");
    assert_eq!(
        w.decode_all().unwrap().column_total("host"),
        Some(rows),
        "the folded base accounts for every row"
    );
    assert_eq!(
        delta_files(&root).len(),
        3,
        "deltas are NOT deleted in the cycle that folds them — a reader holding \
         a pre-fold base still needs them"
    );

    // The hazard: base now contains these deltas AND the objects are still
    // listed. A reader that ignored `absorbed` would double every count.
    ice.invalidate_cached_table(ice.events_table_ident()).await;
    assert_eq!(
        aggregate_total(&ice).await,
        Some(rows),
        "an absorbed delta whose object still exists must not be counted twice"
    );
    assert_eq!(served_total(&ice).await, rows);

    // --- Cycle 2 with no new commits: idempotent, and now the GC runs.
    let outcomes = ice.fold_group_count_deltas(1).await.unwrap();
    let (_, o) = &outcomes[0];
    assert_eq!(
        (o.folded, o.deleted, o.pruned),
        (0, 3, 0),
        "delete, not refold"
    );
    assert!(delta_files(&root).is_empty(), "absorbed deltas GC'd");
    let w = wide(&root).expect("wide base");
    assert_eq!(
        w.decode_all().unwrap().column_total("host"),
        Some(rows),
        "refolding must not have added anything"
    );

    ice.invalidate_cached_table(ice.events_table_ident()).await;
    assert_eq!(
        aggregate_total(&ice).await,
        Some(rows),
        "exact after the GC"
    );
    assert_eq!(served_total(&ice).await, rows);

    // --- Cycle 3: the absorbed set drops ids whose objects are gone, so it
    //     stays bounded instead of growing for the table's whole life.
    let outcomes = ice.fold_group_count_deltas(1).await.unwrap();
    let (_, o) = &outcomes[0];
    assert_eq!((o.folded, o.deleted, o.pruned), (0, 0, 3));
    assert!(
        wide(&root).unwrap().absorbed.is_empty(),
        "absorbed set self-prunes once the objects are gone"
    );

    // --- And new commits keep working against a non-empty base.
    rows += commit(&ice, 3).await;
    ice.invalidate_cached_table(ice.events_table_ident()).await;
    assert_eq!(
        aggregate_total(&ice).await,
        Some(rows),
        "base + a fresh outstanding delta"
    );
    assert_eq!(served_total(&ice).await, rows);
    ice.fold_group_count_deltas(1).await.unwrap();
    ice.invalidate_cached_table(ice.events_table_ident()).await;
    assert_eq!(
        aggregate_total(&ice).await,
        Some(rows),
        "…and after folding it in"
    );
}

#[tokio::test]
async fn a_delta_that_lands_out_of_order_is_still_folded() {
    // Property 2. A stalled writer's delta can carry a sequence number BELOW
    // ones already absorbed. Membership folds it; a high-water mark would skip
    // it forever, and the column would silently fall back to a full scan for
    // the rest of the table's life.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&root).await.unwrap().with_tuning(
        siglake_storage::iceberg::IcebergTuning {
            table_group_count_cardinality: Some(2_000_000),
            ..Default::default()
        },
    );
    for n in 0..2 {
        commit(&ice, n).await;
    }
    ice.fold_group_count_deltas(1).await.unwrap();

    let absorbed = wide(&root).unwrap().absorbed;
    let lowest = *absorbed.iter().next().expect("something absorbed");
    let before = wide(&root)
        .unwrap()
        .decode_all()
        .unwrap()
        .column_total("host")
        .unwrap();

    // A delta numbered below everything already folded — what a writer that
    // stalled between its commit and its PUT leaves behind.
    let late_seq = lowest - 1;
    assert!(
        !absorbed.contains(&late_seq),
        "the late id must be new, else the test proves nothing"
    );
    let late = GroupCountDelta {
        sequence_number: late_seq,
        snapshot_id: None,
        coverage_link: None,
        group_counts: Some(FileGroupCounts {
            columns: [(
                "host".to_string(),
                ColumnGroupCounts {
                    values: [("late-host".to_string(), 7u64)].into_iter().collect(),
                    nulls: 0,
                },
            )]
            .into_iter()
            .collect(),
        }),
        sketches: None,
    };
    let path = aggregate_dir(&root).join(group_count_delta_rel_path(late_seq));
    std::fs::write(&path, serde_json::to_vec(&late).unwrap()).unwrap();

    let outcomes = ice.fold_group_count_deltas(1).await.unwrap();
    let (_, o) = &outcomes[0];
    assert_eq!(o.folded, 1, "the out-of-order delta is folded, not skipped");

    let w = wide(&root).unwrap();
    assert!(w.absorbed.contains(&late_seq));
    let counts = w.decode_all().unwrap();
    assert_eq!(
        counts.column_total("host"),
        Some(before + 7),
        "its contribution landed exactly once"
    );
}

#[tokio::test]
async fn a_crash_between_folding_and_deleting_leaves_the_counts_correct() {
    // Property 3. The fold's write records absorption; the deletes are a
    // separate, best-effort step. Simulate the crash by restoring the delta
    // objects a fold had deleted — indistinguishable, from the next cycle's
    // point of view, from a delete that never landed.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&root).await.unwrap().with_tuning(
        siglake_storage::iceberg::IcebergTuning {
            table_group_count_cardinality: Some(2_000_000),
            ..Default::default()
        },
    );

    let mut rows = 0u64;
    for n in 0..2 {
        rows += commit(&ice, n).await;
    }
    let dir = aggregate_dir(&root).join("siglake-agg-deltas");
    let saved: Vec<(std::path::PathBuf, Vec<u8>)> = delta_files(&root)
        .iter()
        .map(|f| {
            let p = dir.join(f);
            let b = std::fs::read(&p).unwrap();
            (p, b)
        })
        .collect();

    ice.fold_group_count_deltas(1).await.unwrap(); // absorb
    ice.fold_group_count_deltas(1).await.unwrap(); // delete
    assert!(delta_files(&root).is_empty());

    // The crash: the objects are back, still named in `absorbed`.
    for (p, b) in &saved {
        std::fs::write(p, b).unwrap();
    }
    let w = wide(&root).unwrap();
    assert_eq!(w.absorbed.len(), saved.len(), "still recorded as absorbed");

    ice.invalidate_cached_table(ice.events_table_ident()).await;
    assert_eq!(
        aggregate_total(&ice).await,
        Some(rows),
        "a resurrected absorbed delta must not be counted again"
    );
    assert_eq!(served_total(&ice).await, rows);

    let outcomes = ice.fold_group_count_deltas(1).await.unwrap();
    let (_, o) = &outcomes[0];
    assert_eq!(
        (o.folded, o.deleted),
        (0, saved.len()),
        "the next cycle re-deletes rather than re-folding"
    );
    let counts = wide(&root).unwrap().decode_all().unwrap();
    assert_eq!(counts.column_total("host"), Some(rows));
}

/// The busy-cycle threshold: a small backlog is left alone, a large one is
/// absorbed.
///
/// This is what stops the starvation fix from becoming its own problem. The
/// 2026-08-03 1TB round folded ONCE in 7h48m because every maintenance cycle was
/// skipped under backpressure, so deltas reached 865 — and a reader pays an S3
/// GET per unabsorbed delta. Moving the fold above those early-outs fixes that,
/// but folding on a pure timer would rewrite a 26.5MB base every minute for
/// eight hours to absorb a handful of commits. The threshold is what makes a
/// busy ingest take a few large folds instead of either extreme.
#[tokio::test]
async fn a_busy_cycle_waits_for_a_backlog_worth_rewriting_the_base_for() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&root).await.unwrap().with_tuning(
        siglake_storage::iceberg::IcebergTuning {
            table_group_count_cardinality: Some(2_000_000),
            ..Default::default()
        },
    );

    let mut rows = 0u64;
    for n in 0..3 {
        rows += commit(&ice, n).await;
    }
    let outstanding = delta_files(&root).len();
    assert_eq!(outstanding, 3, "three commits, three deltas");

    // A busy cycle whose threshold exceeds the backlog absorbs nothing — the
    // base is not worth rewriting yet.
    let outcomes = ice.fold_group_count_deltas(outstanding + 1).await.unwrap();
    assert!(
        outcomes.is_empty(),
        "a backlog under the threshold must not trigger a fold: {outcomes:?}"
    );
    assert_eq!(
        delta_files(&root).len(),
        outstanding,
        "and nothing may be deleted either — the deltas are still the only copy"
    );

    // Still exact meanwhile: the read path folds what the compactor declined,
    // so declining costs read amplification and never correctness.
    assert_eq!(aggregate_total(&ice).await, Some(rows));

    // Once the backlog reaches the threshold, it absorbs.
    let outcomes = ice.fold_group_count_deltas(outstanding).await.unwrap();
    assert_eq!(outcomes.len(), 1, "at the threshold the fold runs");
    assert_eq!(outcomes[0].1.folded, outstanding);
    assert_eq!(aggregate_total(&ice).await, Some(rows));
}
