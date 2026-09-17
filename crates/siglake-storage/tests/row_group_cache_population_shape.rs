//! Task #4847: what row-group-granular population does to the shape #4494
//! pinned, on a file with more than one row group.
//!
//! #4494's fixture (`file_cache_population_shape.rs`) runs on single-row-group
//! files, where option (c) has nothing to work with: it is the same file, so a
//! clipped read leaves the same nothing behind. This one gives the read a
//! boundary to reach. Every file here is written at the row-group floor
//! (`MIN_ROW_GROUP_ROWS` = 131,072 rows, reached with
//! `IcebergTuning::target_row_group_bytes = 1`) plus a short tail, so it holds
//! group 0 = 131,072 rows and group 1 = the tail, and the browse predicate
//! matches only at the HEAD of the tail. A `LIMIT 100` browse therefore decodes
//! group 0 whole and stops one batch into group 1.
//!
//! The browse's predicate is a residual one (`lower(host) = …`); see `BROWSE`
//! for why a converted predicate reaches no populate path since #4891.
//!
//! Phases, all on one table so the difference is the POLICY and nothing else:
//!
//! 1. shipped whole-file policy, clipped browse, twice: one miss and one
//!    abandoned population per attempt, no insert — #4494's reading, unchanged
//!    by the extra row group;
//! 2. the prototype, same browse, cold: the same one miss, and now group 0 is
//!    inserted while the partial group 1 is still abandoned;
//! 3. the prototype's repeat: group 0 is served from the cache and only group 1
//!    is read, with the SAME rows out as the cache-disabled control;
//! 4. one drained pass completes group 1 too, after which the browse needs no
//!    reader at all (`hit`);
//! 5. exactness under a partially populated file: the unclipped predicate
//!    answer matches the cache-disabled control row for row.
//!
//! Isolated in its own test binary: the scan tuning, the decoded cache, the
//! footer-layout cache and the metrics recorder are all process-wide.

use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, IcebergTuning};
use siglake_storage::QueryScanTuning;

/// The bench rounds' cache tuning, verbatim (#4494).
const CACHE_MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const CACHE_MAX_ENTRIES: usize = 16384;
/// `MIN_ROW_GROUP_ROWS`: the smallest row group the write path will produce, and
/// therefore the smallest unit option (c) could ever cache. 16 batches of 8,192.
const GROUP_ROWS: usize = 128 * 1024;
const BATCH_ROWS: usize = 8_192;
/// Group 1, three batches long: the browse's matches sit in its FIRST batch, so
/// the read stops with group 1 partial and group 0 whole.
const TAIL_ROWS: usize = 3 * BATCH_ROWS;
/// Rows the browse predicate matches, at the head of the tail (group 1).
const NEEDLES: usize = 200;

#[derive(Debug, Default, PartialEq, Eq)]
struct Outcomes {
    hit: u64,
    miss: u64,
    bypass: u64,
    insert: u64,
    insert_skipped_contended: u64,
    skip_oversized: u64,
    abandoned: u64,
    evict: u64,
}

fn counter_sum(
    snapshot: &[(
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    )],
    name: &str,
    outcome: &str,
) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == outcome)
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(count) => *count,
            _ => 0,
        })
        .sum()
}

/// One phase's outcome deltas. `snapshot()` drains the registry, so every read
/// is a delta against the previous one.
fn outcomes(snapshotter: &Snapshotter) -> (Outcomes, RowGroupOutcomes) {
    let snapshot = snapshotter.snapshot().into_vec();
    let shipped = "siglake_query_scan_file_cache_requests_total";
    let proto = "siglake_query_scan_file_cache_row_group_total";
    (
        Outcomes {
            hit: counter_sum(&snapshot, shipped, "hit"),
            miss: counter_sum(&snapshot, shipped, "miss"),
            bypass: counter_sum(&snapshot, shipped, "bypass"),
            insert: counter_sum(&snapshot, shipped, "insert"),
            insert_skipped_contended: counter_sum(&snapshot, shipped, "insert_skipped_contended"),
            skip_oversized: counter_sum(&snapshot, shipped, "skip_oversized"),
            abandoned: counter_sum(&snapshot, shipped, "abandoned"),
            evict: counter_sum(&snapshot, shipped, "evict"),
        },
        RowGroupOutcomes {
            group_inserted: counter_sum(&snapshot, proto, "group_inserted"),
            served_whole_task: counter_sum(&snapshot, proto, "served_whole_task"),
            partial_serve: counter_sum(&snapshot, proto, "partial_serve"),
            misaligned: counter_sum(&snapshot, proto, "misaligned"),
            refused: counter_sum(&snapshot, proto, "refused"),
            layout_read: counter_sum(&snapshot, proto, "layout_read"),
            layout_hit: counter_sum(&snapshot, proto, "layout_hit"),
            layout_error: counter_sum(&snapshot, proto, "layout_error"),
        },
    )
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RowGroupOutcomes {
    group_inserted: u64,
    served_whole_task: u64,
    partial_serve: u64,
    misaligned: u64,
    refused: u64,
    layout_read: u64,
    layout_hit: u64,
    layout_error: u64,
}

/// Accumulate deltas until `settled` holds or the deadline passes: `abandoned`
/// is charged from a populate stream's `Drop`, which a clipped plan runs on
/// whatever task last held the stream — not necessarily before `collect()`
/// returns (#4494).
async fn settling(
    snapshotter: &Snapshotter,
    settled: impl Fn(&Outcomes, &RowGroupOutcomes) -> bool,
) -> (Outcomes, RowGroupOutcomes) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut total = Outcomes::default();
    let mut proto = RowGroupOutcomes::default();
    loop {
        let (shipped, rg) = outcomes(snapshotter);
        total.hit += shipped.hit;
        total.miss += shipped.miss;
        total.bypass += shipped.bypass;
        total.insert += shipped.insert;
        total.insert_skipped_contended += shipped.insert_skipped_contended;
        total.skip_oversized += shipped.skip_oversized;
        total.abandoned += shipped.abandoned;
        total.evict += shipped.evict;
        proto.group_inserted += rg.group_inserted;
        proto.served_whole_task += rg.served_whole_task;
        proto.partial_serve += rg.partial_serve;
        proto.misaligned += rg.misaligned;
        proto.refused += rg.refused;
        proto.layout_read += rg.layout_read;
        proto.layout_hit += rg.layout_hit;
        proto.layout_error += rg.layout_error;
        if settled(&total, &proto) || std::time::Instant::now() >= deadline {
            return (total, proto);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// One file: `GROUP_ROWS` rows the browse predicate misses, then a `TAIL_ROWS`
/// tail whose last `NEEDLES` rows match it.
fn fixture_events() -> Vec<Event> {
    let mut events = Vec::with_capacity(GROUP_ROWS + TAIL_ROWS);
    for i in 0..GROUP_ROWS + TAIL_ROWS {
        let mut event = Event::now(format!("row-{i} checkout latency={} ms", i % 97));
        event.host = if (GROUP_ROWS..GROUP_ROWS + NEEDLES).contains(&i) {
            "needle".into()
        } else {
            "bulk".into()
        };
        events.push(event);
    }
    events
}

fn tuning(row_group_prototype: bool) -> QueryScanTuning {
    QueryScanTuning {
        file_cache_max_bytes: Some(CACHE_MAX_BYTES),
        file_cache_max_entries: Some(CACHE_MAX_ENTRIES),
        file_concurrency_limit: Some(1),
        batch_size: Some(BATCH_ROWS),
        file_cache_row_group_prototype: row_group_prototype,
        ..Default::default()
    }
}

async fn raws(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let column = batch
            .column_by_name("raw")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push(column.value(i).to_string());
        }
    }
    rows
}

/// The browse, under a RESIDUAL predicate: `lower(host)` does not convert to an
/// Iceberg predicate, so nothing reaches the reader and the clip lands where the
/// fixture puts it. Since #4891 a task carrying a CONVERTED predicate bypasses
/// the cache at either granularity (it would have to be read with the predicate
/// stripped, which costs more than not caching — see
/// `file_cache_predicate_bypass.rs`), so `host = 'needle'` would exercise no
/// population here at all. This is the `resid/*` regime of
/// `row_group_cache_measurement.rs`, and it is the one where the prototype's win
/// was measured.
const BROWSE: &str = "SELECT raw FROM events WHERE lower(host) = 'needle' LIMIT 100";
const UNCLIPPED: &str = "SELECT raw FROM events WHERE lower(host) = 'needle'";
/// A drain that projects what the browse projects (`raw` AND the `host` its
/// residual filter reads), so it addresses the same entries. Cache identity
/// includes the projection at either granularity, so a bare `SELECT raw` is a
/// different key and populates its own entries — see `DRAIN`.
const DRAIN_BROWSE_PROJECTION: &str = "SELECT raw FROM events WHERE lower(host) = 'bulk'";
const DRAIN: &str = "SELECT raw FROM events";

#[tokio::test(flavor = "multi_thread")]
async fn row_group_population_survives_a_clipped_browse() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    // `target_row_group_bytes = 1` rounds down to MIN_ROW_GROUP_ROWS, so the
    // file holds a floor-sized group 0 and a short group 1.
    let ice = IcebergContext::open(tmp.path())
        .await
        .unwrap()
        .with_tuning(IcebergTuning {
            target_row_group_bytes: Some(1),
            ..Default::default()
        });
    ice.append_events(&fixture_events()).await.unwrap();

    // Control, with the cache off entirely: the answers every arm below must
    // reproduce.
    siglake_storage::configure_query_scan_tuning(QueryScanTuning {
        file_concurrency_limit: Some(1),
        batch_size: Some(BATCH_ROWS),
        ..Default::default()
    });
    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let control_browse = raws(&ctx, BROWSE).await;
    let control_unclipped = raws(&ctx, UNCLIPPED).await;
    let control_drain = raws(&ctx, DRAIN).await;
    let control_bulk = raws(&ctx, DRAIN_BROWSE_PROJECTION).await;
    assert_eq!(control_browse.len(), 100);
    assert_eq!(control_unclipped.len(), NEEDLES);
    assert_eq!(control_drain.len(), GROUP_ROWS + TAIL_ROWS);
    assert_eq!(control_bulk.len(), GROUP_ROWS + TAIL_ROWS - NEEDLES);

    // Phase 1: the shipped whole-file policy on a two-group file. Twice, and
    // nothing accumulates — the extra boundary changes nothing, because the
    // entry is the file.
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::clear_row_group_layout_cache();
    siglake_storage::configure_query_scan_tuning(tuning(false));
    let whole_file_ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&whole_file_ctx).await.unwrap();
    let _ = outcomes(&snapshotter);
    for attempt in 1..=2 {
        assert_eq!(raws(&whole_file_ctx, BROWSE).await, control_browse);
        let (shipped, proto) = settling(&snapshotter, |o, _| o.abandoned >= 1).await;
        assert_eq!(
            shipped,
            Outcomes {
                miss: 1,
                abandoned: 1,
                ..Default::default()
            },
            "attempt {attempt}: whole-file policy must populate nothing from a clipped browse"
        );
        assert_eq!(
            proto,
            RowGroupOutcomes::default(),
            "attempt {attempt}: the prototype is off, so none of its arms may move"
        );
    }
    assert_eq!(
        siglake_storage::decoded_file_cache_footprint().entries,
        0,
        "two clipped browses under the shipped policy leave the cache empty"
    );

    // Phase 2: the prototype, same browse, cold. One miss, group 0 inserted at
    // its boundary, and the partial group 1 still abandoned.
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::clear_row_group_layout_cache();
    siglake_storage::configure_query_scan_tuning(tuning(true));
    let proto_ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&proto_ctx).await.unwrap();
    let _ = outcomes(&snapshotter);
    assert_eq!(raws(&proto_ctx, BROWSE).await, control_browse);
    let (shipped, proto) = settling(&snapshotter, |o, p| {
        o.abandoned >= 1 && p.group_inserted >= 1
    })
    .await;
    assert_eq!(
        shipped,
        Outcomes {
            miss: 1,
            insert: 1,
            abandoned: 1,
            ..Default::default()
        },
        "the clipped browse must insert the group it completed and abandon only the partial one"
    );
    assert_eq!(
        proto,
        RowGroupOutcomes {
            group_inserted: 1,
            layout_read: 1,
            ..Default::default()
        },
        "one footer read, one completed group"
    );
    let footprint = siglake_storage::decoded_file_cache_footprint();
    assert_eq!(footprint.entries, 1, "{footprint:?}");
    assert_eq!(footprint.rows, GROUP_ROWS, "the entry is a whole row group");

    // Phase 3: the repeat serves group 0 from the cache and reads only group 1,
    // with the control's rows.
    assert_eq!(raws(&proto_ctx, BROWSE).await, control_browse);
    let (shipped, proto) = settling(&snapshotter, |o, p| {
        p.partial_serve >= 1 && o.abandoned >= 1
    })
    .await;
    assert_eq!(
        shipped,
        Outcomes {
            miss: 1,
            abandoned: 1,
            ..Default::default()
        },
        "a partial serve still opens a reader for the groups it does not have"
    );
    assert_eq!(
        proto,
        RowGroupOutcomes {
            partial_serve: 1,
            layout_hit: 1,
            ..Default::default()
        },
        "group 0 served from cache, the layout read once and reused"
    );

    // Phase 4: one drained pass over the same projection completes group 1,
    // after which the browse needs no reader at all.
    assert_eq!(
        raws(&proto_ctx, DRAIN_BROWSE_PROJECTION).await,
        control_bulk
    );
    let (shipped, proto) = settling(&snapshotter, |_, p| p.group_inserted >= 1).await;
    assert_eq!(
        shipped,
        Outcomes {
            miss: 1,
            insert: 1,
            ..Default::default()
        },
        "the drained pass inserts the group the browse left partial"
    );
    assert_eq!(
        proto,
        RowGroupOutcomes {
            group_inserted: 1,
            partial_serve: 1,
            layout_hit: 1,
            ..Default::default()
        },
        "it served group 0 from cache and populated group 1"
    );
    assert_eq!(raws(&proto_ctx, BROWSE).await, control_browse);
    let (shipped, proto) = settling(&snapshotter, |o, _| o.hit >= 1).await;
    assert_eq!(
        shipped,
        Outcomes {
            hit: 1,
            ..Default::default()
        },
        "with both groups cached the task is served whole"
    );
    assert_eq!(
        proto,
        RowGroupOutcomes {
            served_whole_task: 1,
            layout_hit: 1,
            ..Default::default()
        }
    );

    // Phase 5: exact answers over a partially populated file. Drop group 1 by
    // clearing and re-populating only group 0, then ask the unclipped question:
    // the cached prefix and the freshly read remainder must add up to the
    // control's rows, in the control's order.
    siglake_storage::clear_decoded_file_cache();
    assert_eq!(raws(&proto_ctx, BROWSE).await, control_browse);
    let _ = settling(&snapshotter, |_, p| p.group_inserted >= 1).await;
    assert_eq!(raws(&proto_ctx, UNCLIPPED).await, control_unclipped);
    let (_, proto) = settling(&snapshotter, |_, p| p.partial_serve >= 1).await;
    assert_eq!(
        proto.partial_serve, 1,
        "the unclipped answer was assembled from the cached group 0 plus a read of group 1"
    );
    assert_eq!(raws(&proto_ctx, DRAIN).await, control_drain);

    siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::clear_row_group_layout_cache();
}
