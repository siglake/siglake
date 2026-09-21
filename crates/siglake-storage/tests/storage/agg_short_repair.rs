//! #3000: a group-count aggregate that is merely SHORT of `record_count`, with
//! no marker to explain it, used to stay short for the life of the table.
//!
//! The lost-delta marker covers one cause — a delta PUT that spent its four
//! attempts — and nothing else. A commit killed between its commit and its
//! delta PUT writes neither delta nor marker. The #2919 upgrade starts every
//! table's new `metadata/siglake-agg/<table-uuid>/` prefix empty at the first
//! commit after the upgrade, which leaves every pre-upgrade row uncounted. In
//! both states every answer stays exact and every `GROUP BY` pays the per-file
//! tiers, forever, until an operator runs `siglake rebuild-group-counts`.
//!
//! What makes the repair safe rather than a background scan on a hair trigger
//! is the discrimination in the middle: a commit's delta is written AFTER the
//! commit, so for the second or so between them the newest generation has no
//! contribution anywhere and every column reads short — indistinguishable, by
//! totals alone, from a delta whose writer was killed. The census refuses to
//! repair until the newest generation's own contribution is in the artifact,
//! and it must refuse a column a previous rebuild already proved it cannot
//! restore, or one unreadable column buys a full Tier-2 rebuild every pass.

use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use chrono::{TimeZone, Utc};
use siglake_core::index_config::{DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode};
use siglake_core::Event;
use siglake_storage::iceberg::{
    ColumnGroupCounts, ColumnSketch, FileGroupCounts, GroupCountDelta, GroupCountSketches,
    IcebergContext, IcebergTuning, ShortAggregateAttemptReason, ShortAggregateAttemptStart,
    ShortAggregateOutcome, ShortAggregateRepair, WideGroupCounts, GROUP_COUNT_SKETCH_VERSION,
};
use std::collections::BTreeMap;

const ROWS: i64 = 5_000;

fn index_config(id: &str) -> IndexConfig {
    IndexConfig {
        index_id: id.to_string(),
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                FieldMapping {
                    name: "timestamp".into(),
                    field_type: FieldType::Datetime,
                    required: true,
                },
                FieldMapping {
                    name: "host".into(),
                    field_type: FieldType::Text {
                        tokenizer: Some("raw".into()),
                    },
                    required: false,
                },
                FieldMapping {
                    name: "service".into(),
                    field_type: FieldType::Text {
                        tokenizer: Some("raw".into()),
                    },
                    required: false,
                },
            ],
            timestamp_field: "timestamp".to_string(),
            // `host` is a declared dimension with 5,000 distinct values per
            // batch, so it is over the 4,096-entry INLINE ceiling and the wide
            // object is its only Tier-1 carrier — which is what makes a short
            // wide object visible end to end here.
            tag_fields: vec!["host".to_string()],
            default_search_fields: vec![],
        },
        retention: None,
        index_at_flush: None,
    }
}

fn batch(cfg: &IndexConfig, nth: i64) -> RecordBatch {
    let base = 1_700_000_000_000_000i64 + nth * 100_000_000;
    RecordBatch::try_new(
        cfg.to_arrow_schema(),
        vec![
            std::sync::Arc::new(
                TimestampMicrosecondArray::from(
                    (0..ROWS).map(|i| Some(base + i)).collect::<Vec<_>>(),
                )
                .with_timezone("+00:00"),
            ),
            std::sync::Arc::new(StringArray::from(
                (0..ROWS)
                    .map(|i| format!("h-{:06}", nth * ROWS + i))
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(StringArray::from(
                (0..ROWS)
                    .map(|i| format!("svc-{:06}", nth * ROWS + i))
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(StringArray::from(vec![None::<&str>; ROWS as usize])),
        ],
    )
    .unwrap()
}

/// [`batch`] with a chosen row count, for the cost measurement: the repair
/// reads the FILES, so its cost is set by rows and distinct values per file.
fn wide_batch(cfg: &IndexConfig, nth: i64, rows: i64) -> RecordBatch {
    let base = 1_700_000_000_000_000i64 + nth * 100_000_000_000;
    RecordBatch::try_new(
        cfg.to_arrow_schema(),
        vec![
            std::sync::Arc::new(
                TimestampMicrosecondArray::from(
                    (0..rows).map(|i| Some(base + i)).collect::<Vec<_>>(),
                )
                .with_timezone("+00:00"),
            ),
            std::sync::Arc::new(StringArray::from(
                (0..rows)
                    .map(|i| format!("h-{:08}", nth * rows + i))
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(StringArray::from(
                (0..rows)
                    .map(|i| format!("svc-{:08}", nth * rows + i))
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(StringArray::from(vec![None::<&str>; rows as usize])),
        ],
    )
    .unwrap()
}

async fn open(root: &std::path::Path) -> IcebergContext {
    IcebergContext::open(root).await.unwrap().with_tuning(
        // Above the inline ceiling, which is what switches the incremental
        // base+delta path on at all.
        IcebergTuning {
            table_group_count_cardinality: Some(4_194_304),
            result_caches: Some(false),
            ..Default::default()
        },
    )
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}

/// Every aggregate artifact of every incarnation in the warehouse.
fn aggregate_artifacts(wh: &std::path::Path) -> Vec<std::path::PathBuf> {
    walk(wh)
        .into_iter()
        .filter(|p| p.to_string_lossy().contains("siglake-agg"))
        .collect()
}

fn wide_path(wh: &std::path::Path) -> std::path::PathBuf {
    let mut found: Vec<std::path::PathBuf> = walk(wh)
        .into_iter()
        .filter(|p| p.file_name().is_some_and(|n| n == "siglake-agg-wide.json"))
        .collect();
    assert_eq!(
        found.len(),
        1,
        "one wide object in this warehouse: {found:?}"
    );
    found.pop().unwrap()
}

fn read_wide(wh: &std::path::Path) -> WideGroupCounts {
    serde_json::from_slice(&std::fs::read(wide_path(wh)).unwrap()).unwrap()
}

fn write_wide(path: &std::path::Path, wide: &WideGroupCounts) {
    std::fs::write(path, serde_json::to_vec(wide).unwrap()).unwrap();
}

fn counts(column: &str, rows: u64) -> FileGroupCounts {
    let mut columns = BTreeMap::new();
    columns.insert(
        column.to_string(),
        ColumnGroupCounts {
            values: [("only-value".to_string(), rows)].into_iter().collect(),
            nulls: 0,
        },
    );
    FileGroupCounts { columns }
}

async fn make_short(ice: &IcebergContext, id: &str) {
    let cfg = index_config(id);
    ice.create_index(&cfg).await.unwrap();
    let ident = ice.index_table_ident(id);
    ice.append_to_table(&ident, batch(&cfg, 0), &["host"])
        .await
        .unwrap();
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let location = table.metadata().location().to_string();
    let dir = std::path::Path::new(location.strip_prefix("file://").unwrap_or(&location));
    let delta = walk(dir)
        .into_iter()
        .find(|path| path.to_string_lossy().contains("siglake-agg-deltas"))
        .expect("first delta");
    std::fs::remove_file(delta).unwrap();
    ice.append_to_table(&ident, batch(&cfg, 1), &["host"])
        .await
        .unwrap();
    ice.fold_group_count_deltas(1).await.unwrap();
    ice.invalidate_cached_table(&ident).await;
}

/// How `GROUP BY host` is being served right now, and its total.
async fn served(ice: &IcebergContext, index: &str) -> (String, u64) {
    let g = ice
        .grouped_counts_with_summary(index, "host", None, None)
        .await
        .unwrap()
        .unwrap();
    (g.source_label().to_string(), g.iter().map(|(_, c)| c).sum())
}

/// The motivating state: a table whose aggregate prefix starts empty mid-life
/// (#2919's upgrade), so the aggregate accounts for the delta era alone.
#[tokio::test]
async fn an_aggregate_short_since_an_empty_prefix_is_rebuilt_from_the_files() {
    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = open(&wh).await;
    let cfg = index_config("upgraded");
    ice.create_index(&cfg).await.unwrap();
    let ident = ice.index_table_ident("upgraded");

    for n in 0..2 {
        ice.append_to_table(&ident, batch(&cfg, n), &["host"])
            .await
            .unwrap();
    }
    // The compactor had folded the deltas into a base before the upgrade; the
    // base object is what the upgrade leaves behind at a path nothing adopts.
    ice.fold_group_count_deltas(1).await.unwrap();
    assert_eq!(
        served(&ice, "upgraded").await,
        ("tier1_wide".to_string(), (2 * ROWS) as u64),
        "precondition: Tier-1 serves the table before the upgrade"
    );

    // The upgrade: the artifacts this incarnation had been reading are at a
    // path nothing adopts any more, and the new prefix is empty.
    for artifact in aggregate_artifacts(&wh) {
        std::fs::remove_file(&artifact).unwrap();
    }
    ice.append_to_table(&ident, batch(&cfg, 2), &["host"])
        .await
        .unwrap();
    ice.fold_group_count_deltas(1).await.unwrap();
    let rows = (3 * ROWS) as u64;
    assert_eq!(
        served(&ice, "upgraded").await,
        ("materialized".to_string(), rows),
        "the aggregate now accounts for one commit of three: exact answers, \
         per-file tiers, and no marker anywhere to say so"
    );

    // Census only: it must SEE the deficit without reading a single file.
    let before = std::fs::read(wide_path(&wh)).unwrap();
    let outcomes = ice.repair_short_group_count_aggregates(0).await.unwrap();
    assert_eq!(
        outcomes,
        vec![(
            "upgraded".to_string(),
            ShortAggregateOutcome::Detected {
                columns: vec!["host".to_string()]
            }
        )],
        "detected, and left alone"
    );
    assert_eq!(
        std::fs::read(wide_path(&wh)).unwrap(),
        before,
        "a census with no repair budget must not write"
    );

    // With a budget, one Tier-2 pass per maintained column restores it.
    let outcomes = ice.repair_short_group_count_aggregates(1).await.unwrap();
    assert_eq!(
        outcomes,
        vec![(
            "upgraded".to_string(),
            ShortAggregateOutcome::Repaired {
                columns: vec!["host".to_string()],
                unrestored: Vec::new()
            }
        )]
    );
    assert_eq!(
        served(&ice, "upgraded").await,
        ("tier1_wide".to_string(), rows),
        "the cheap tier is back, and still exact"
    );
    assert!(
        !aggregate_artifacts(&wh)
            .iter()
            .any(|path| path.to_string_lossy().contains(".short-repair.")),
        "the successful aggregate CAS must clear its attempt marker"
    );

    // Repeated pass: nothing is short, so nothing is rebuilt. A repair that
    // re-fires every interval is worse than no repair — it is one Tier-2 query
    // per column per pass, forever.
    let after = std::fs::read(wide_path(&wh)).unwrap();
    assert!(
        ice.repair_short_group_count_aggregates(1)
            .await
            .unwrap()
            .is_empty(),
        "a repaired table is covered"
    );
    assert_eq!(
        std::fs::read(wide_path(&wh)).unwrap(),
        after,
        "and nothing rewrote the base"
    );

    // A later commit folds onto the rebuilt base exactly once.
    ice.append_to_table(&ident, batch(&cfg, 3), &["host"])
        .await
        .unwrap();
    assert_eq!(
        served(&ice, "upgraded").await,
        ("tier1_wide".to_string(), (4 * ROWS) as u64),
        "a post-rebuild commit adds its rows once"
    );
}

/// The discrimination that keeps this off a hair trigger: a contribution that
/// has not landed YET is short in exactly the same way as one that never will.
#[tokio::test]
async fn a_contribution_that_has_not_landed_yet_is_not_rebuilt_for() {
    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = open(&wh).await;
    let cfg = index_config("inflight");
    ice.create_index(&cfg).await.unwrap();
    let ident = ice.index_table_ident("inflight");

    for n in 0..2 {
        ice.append_to_table(&ident, batch(&cfg, n), &["host"])
            .await
            .unwrap();
    }
    // Stand in for a writer still between its commit and its delta PUT: the
    // newest commit's contribution is nowhere, and the aggregate is short by
    // exactly its rows.
    let mut deltas: Vec<std::path::PathBuf> = aggregate_artifacts(&wh)
        .into_iter()
        .filter(|p| p.to_string_lossy().contains("siglake-agg-deltas"))
        .collect();
    deltas.sort();
    let newest = deltas.pop().expect("the newest commit wrote a delta");
    std::fs::remove_file(&newest).unwrap();
    ice.fold_group_count_deltas(1).await.unwrap();
    assert_eq!(
        served(&ice, "inflight").await.0,
        "materialized",
        "precondition: the aggregate is short"
    );

    let before = std::fs::read(wide_path(&wh)).unwrap();
    assert_eq!(
        ice.repair_short_group_count_aggregates(1).await.unwrap(),
        vec![(
            "inflight".to_string(),
            ShortAggregateOutcome::Pending {
                columns: vec!["host".to_string()]
            }
        )],
        "the newest generation has published nothing, so the fold — or the PUT \
         still in flight — is what closes this gap"
    );
    assert_eq!(
        std::fs::read(wide_path(&wh)).unwrap(),
        before,
        "and no Tier-2 read was spent on it"
    );

    // Now a LATER commit lands its delta. The gap in the middle is the same
    // size, and nothing outstanding can close it any more.
    ice.append_to_table(&ident, batch(&cfg, 2), &["host"])
        .await
        .unwrap();
    assert_eq!(
        ice.repair_short_group_count_aggregates(0).await.unwrap(),
        vec![(
            "inflight".to_string(),
            ShortAggregateOutcome::Detected {
                columns: vec!["host".to_string()]
            }
        )],
        "the same deficit, now provably lost"
    );
    assert_eq!(
        ice.repair_short_group_count_aggregates(1)
            .await
            .unwrap()
            .first()
            .map(|(_, outcome)| outcome.clone()),
        Some(ShortAggregateOutcome::Repaired {
            columns: vec!["host".to_string()],
            unrestored: Vec::new()
        })
    );
    assert_eq!(
        served(&ice, "inflight").await,
        ("tier1_wide".to_string(), (3 * ROWS) as u64)
    );
}

/// The rebuild reads the EXACT half out of the files and nothing else, so its
/// watermark must not take the approximate half down with it: every delta at or
/// below `rebuilt_through` is deleted rather than folded, sketches included.
///
/// The carry is selective — it drops any column the rebuild just restored
/// exactly, because a column represented both ways is reconciled by demoting
/// the exact side, which would undo the repair. That arm is defence rather than
/// a state this test can reach: a delta that sketches a column the base holds
/// exactly demotes it at READ time, so by the time the census runs the column
/// is out of the exact map and is never selected for repair.
///
/// The delta is written by hand: it is the artifact a commit writes for a
/// column whose per-batch cardinality it could not count exactly, and getting
/// the write path to produce one alongside a short exact column needs a fixture
/// an order of magnitude larger than the state under test.
#[tokio::test]
async fn a_repair_carries_the_sketch_half_of_the_deltas_its_watermark_retires() {
    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = open(&wh).await;
    let cfg = index_config("sketched");
    ice.create_index(&cfg).await.unwrap();
    let ident = ice.index_table_ident("sketched");

    for n in 0..2 {
        ice.append_to_table(&ident, batch(&cfg, n), &["host"])
            .await
            .unwrap();
    }
    // The first commit's contribution is lost and the second's is folded in, so
    // the exact half is short with nothing outstanding to explain it.
    let mut deltas: Vec<std::path::PathBuf> = aggregate_artifacts(&wh)
        .into_iter()
        .filter(|p| p.to_string_lossy().contains("siglake-agg-deltas"))
        .collect();
    deltas.sort();
    let lost = deltas.remove(0);
    let lost_sequence: i64 = lost
        .file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse().ok())
        .expect("a delta object is named for its sequence number");
    std::fs::remove_file(&lost).unwrap();
    ice.fold_group_count_deltas(1).await.unwrap();

    // What that commit did publish: a sketch for a column it could not count
    // exactly. Unabsorbed, and below the sequence number the repair is about to
    // write as its watermark.
    const SKETCH_ROWS: u64 = 77;
    let mut columns = BTreeMap::new();
    columns.insert(
        "trace_id".to_string(),
        ColumnSketch::from_exact(
            64,
            [("trace-top".to_string(), SKETCH_ROWS)]
                .into_iter()
                .collect(),
            SKETCH_ROWS,
        ),
    );
    std::fs::write(
        &lost,
        serde_json::to_vec(&GroupCountDelta {
            sequence_number: lost_sequence,
            sketches: Some(GroupCountSketches {
                version: GROUP_COUNT_SKETCH_VERSION,
                columns,
            }),
            ..Default::default()
        })
        .unwrap(),
    )
    .unwrap();
    ice.invalidate_cached_table(&ident).await;

    assert_eq!(
        ice.repair_short_group_count_aggregates(1)
            .await
            .unwrap()
            .first()
            .map(|(_, outcome)| outcome.clone()),
        Some(ShortAggregateOutcome::Repaired {
            columns: vec!["host".to_string()],
            unrestored: vec!["trace_id".to_string()]
        }),
        "the exact half is rebuilt and the unavailable carried sketch is reported"
    );
    let wide = read_wide(&wh);
    let carried = wide.sketches.as_ref().expect("the sketch half is carried");
    assert_eq!(
        carried.columns.keys().collect::<Vec<_>>(),
        vec!["trace_id"],
        "the approximate-only column keeps its rows; the repaired column does \
         not come back as a sketch beside its own exact map"
    );
    assert_eq!(carried.columns["trace_id"].rows, SKETCH_ROWS);

    // The fold now deletes that delta as redundant. Whatever the base did not
    // carry is gone for good at this point, which is what makes the assertion
    // above worth more after this line than before it.
    ice.fold_group_count_deltas(1).await.unwrap();
    assert!(
        !aggregate_artifacts(&wh)
            .iter()
            .any(|p| p.to_string_lossy().contains("siglake-agg-deltas")),
        "precondition: the watermark retired every delta"
    );
    ice.invalidate_cached_table(&ident).await;
    assert_eq!(
        served(&ice, "sketched").await,
        ("tier1_wide".to_string(), (2 * ROWS) as u64),
        "the exact repair survives the fold"
    );
    let approximate = ice
        .approximate_top_group_counts("sketched", "trace_id", 10, None)
        .await
        .unwrap()
        .expect("the carried sketch answers after its delta is gone");
    assert_eq!(
        (approximate.rows_accounted, approximate.rows.len()),
        (SKETCH_ROWS, 1)
    );
}

/// A census repair may recover one already-short sketch without making an
/// unreadable sibling fatal. `timestamp_ns` is the production case: it is
/// admitted as a typed group dimension, then its per-row-unique values exceed
/// the footer cap, while the raw-page fallback deliberately supports only
/// dictionary-encoded UTF-8.
#[tokio::test]
async fn a_repair_restores_a_short_sketch_beside_unreadable_timestamp_ns() {
    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = open(&wh).await;

    for nth in 0..2i64 {
        let base = 1_700_000_000 + nth * ROWS;
        let events: Vec<Event> = (0..ROWS)
            .map(|row| Event {
                timestamp: Utc.timestamp_opt(base + row, 0).single().unwrap(),
                host: format!("h-{:06}", nth * ROWS + row),
                source: "src".into(),
                sourcetype: if row % 2 == 0 {
                    "app:json".into()
                } else {
                    "syslog".into()
                },
                index: "main".into(),
                raw: "request".into(),
                attributes: None,
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
    ice.fold_group_count_deltas(1).await.unwrap();
    let table_rows = (2 * ROWS) as u64;

    // Model a marker-era loss: one exact column and one sketch are already
    // short. A second sketch is complete but cannot be recomputed from these
    // files. The rebuild must correct what it can and retain that sibling.
    let path = wide_path(&wh);
    let mut wide = read_wide(&wh);
    let mut exact = wide.decode_all().expect("the folded base is exact");
    exact.columns.insert(
        "host".to_string(),
        counts("host", 1).columns.remove("host").unwrap(),
    );
    exact
        .columns
        .remove("sourcetype")
        .expect("sourcetype starts exact");
    wide.set_group_counts(Some(exact));
    let mut sketches = GroupCountSketches {
        version: GROUP_COUNT_SKETCH_VERSION,
        columns: BTreeMap::new(),
    };
    sketches.columns.insert(
        "sourcetype".to_string(),
        ColumnSketch::from_exact(64, [("app:json".to_string(), 1)].into_iter().collect(), 1),
    );
    sketches.columns.insert(
        "timestamp_ns".to_string(),
        ColumnSketch::from_exact(
            64,
            [("1700000000000000000".to_string(), table_rows)]
                .into_iter()
                .collect(),
            table_rows,
        ),
    );
    wide.sketches = Some(sketches);
    write_wide(&path, &wide);
    ice.invalidate_cached_table(ice.events_table_ident()).await;

    let outcomes = ice.repair_short_group_count_aggregates(1).await.unwrap();
    assert_eq!(
        outcomes,
        vec![(
            "events".to_string(),
            ShortAggregateOutcome::Repaired {
                columns: vec!["host".to_string()],
                unrestored: vec!["timestamp_ns".to_string()],
            }
        )],
        "the unreadable sketch is reported without failing the exact repair"
    );

    let repaired = read_wide(&wh);
    let repaired_sketches = repaired.sketches.expect("both sketches remain");
    assert_eq!(
        repaired_sketches.columns["sourcetype"].rows, table_rows,
        "the readable short sketch is rebuilt from every committed file"
    );
    assert_eq!(
        repaired_sketches.columns["timestamp_ns"].rows, table_rows,
        "the unavailable column keeps its carried state"
    );
    assert_eq!(
        served(&ice, "events").await,
        ("tier1_wide".to_string(), table_rows),
        "the exact deficit is repaired in the same publication"
    );
}

/// A column the rebuild cannot cover must be RECORDED, or the next commit's
/// delta re-adds it short and every later pass rebuilds for it again.
#[tokio::test]
async fn a_column_the_rebuild_cannot_cover_is_recorded_and_not_retried() {
    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = open(&wh).await;
    let cfg = index_config("residue");
    ice.create_index(&cfg).await.unwrap();
    let ident = ice.index_table_ident("residue");

    for n in 0..2 {
        ice.append_to_table(&ident, batch(&cfg, n), &["host"])
            .await
            .unwrap();
    }
    ice.fold_group_count_deltas(1).await.unwrap();
    let rows = (2 * ROWS) as u64;

    // A maintained column no live file can answer for. `absent_dim` is in the
    // folded base and in no schema, which is the shape of a column whose files
    // were all rewritten by something that did not stamp it: the rebuild reads
    // the files, gets nothing, and leaves the column out rather than writing a
    // partial total.
    let path = wide_path(&wh);
    let mut wide = read_wide(&wh);
    let mut all = wide.decode_all().expect("host is maintained");
    all.columns.insert(
        "absent_dim".to_string(),
        ColumnGroupCounts {
            values: [("x".to_string(), rows / 2)].into_iter().collect(),
            nulls: 0,
        },
    );
    wide.set_group_counts(Some(all));
    write_wide(&path, &wide);
    ice.invalidate_cached_table(&ident).await;

    let outcome = ice
        .repair_short_group_count_aggregates(1)
        .await
        .unwrap()
        .pop()
        .map(|(_, outcome)| outcome);
    assert_eq!(
        outcome,
        Some(ShortAggregateOutcome::Repaired {
            columns: vec!["absent_dim".to_string()],
            unrestored: vec!["absent_dim".to_string()],
        }),
        "the rebuild ran and could not restore the column"
    );
    assert_eq!(
        read_wide(&wh).short_repair.map(|r| r.unrestored),
        Some(["absent_dim".to_string()].into_iter().collect()),
        "recorded in the same write that published the rebuild"
    );
    assert_eq!(
        served(&ice, "residue").await,
        ("tier1_wide".to_string(), rows),
        "and the column that COULD be restored was"
    );

    // The next commit's delta puts the column back, short, the way a real
    // unreadable column comes back. The census must leave it alone.
    let mut wide = read_wide(&wh);
    let mut all = wide.decode_all().unwrap();
    all.columns.insert(
        "absent_dim".to_string(),
        ColumnGroupCounts {
            values: [("x".to_string(), 1)].into_iter().collect(),
            nulls: 0,
        },
    );
    wide.set_group_counts(Some(all));
    write_wide(&path, &wide);
    ice.invalidate_cached_table(&ident).await;

    let before = std::fs::read(&path).unwrap();
    assert_eq!(
        ice.repair_short_group_count_aggregates(1).await.unwrap(),
        vec![(
            "residue".to_string(),
            ShortAggregateOutcome::Unrestored {
                columns: vec!["absent_dim".to_string()]
            }
        )],
        "short only in a column a rebuild already proved it cannot restore"
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "so no second Tier-2 rebuild was spent"
    );

    // The operator's rebuild CLEARS the record: it has just re-read the same
    // files, and its outcome is the newer evidence.
    ice.rebuild_group_count_aggregate("residue").await.unwrap();
    assert_eq!(read_wide(&wh).short_repair, None);
}

/// A hand-written record must suppress even a column the rebuild would have
/// restored: the record is the census's memory, and trusting it is what bounds
/// the repair to one attempt per condition.
#[tokio::test]
async fn a_recorded_column_is_skipped_without_reading_any_file() {
    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = open(&wh).await;
    let cfg = index_config("recorded");
    ice.create_index(&cfg).await.unwrap();
    let ident = ice.index_table_ident("recorded");

    ice.append_to_table(&ident, batch(&cfg, 0), &["host"])
        .await
        .unwrap();
    ice.fold_group_count_deltas(1).await.unwrap();
    let path = wide_path(&wh);
    let mut wide = read_wide(&wh);
    wide.set_group_counts(Some(counts("host", 1)));
    wide.short_repair = Some(ShortAggregateRepair {
        sequence_number: 1,
        unrestored: ["host".to_string()].into_iter().collect(),
    });
    write_wide(&path, &wide);
    ice.invalidate_cached_table(&ident).await;

    let before = std::fs::read(&path).unwrap();
    assert_eq!(
        ice.repair_short_group_count_aggregates(1).await.unwrap(),
        vec![(
            "recorded".to_string(),
            ShortAggregateOutcome::Unrestored {
                columns: vec!["host".to_string()]
            }
        )]
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

/// What the two halves cost, plus the end-to-end cost of the existing durable
/// marker PUT used as the proxy for a pre-attempt marker (#4628). Run it:
///
/// ```text
/// cargo test --release -p siglake-storage --test storage \
///     agg_short_repair::measure_census_and_repair_cost -- --ignored --nocapture
/// ```
///
/// The census must be cheap enough to run on a timer over every table, and the
/// repair must be expensive enough to justify budgeting it. The retained A/B
/// uses matching warehouses: the control rebuild carries two sketches, while
/// the restoration arm recomputes the readable one and carries the unavailable
/// `timestamp_ns` sketch. Recorded 2026-09-20 on the dev box (release, local
/// filesystem warehouse, 8 commits, two high-cardinality dimensions):
///
/// ```text
/// rows=40000  carried_only=51.2ms  restoration=91.0ms   delta=39.7ms
/// rows=400000 carried_only=792.5ms restoration=1432.5ms delta=640.0ms
/// ```
///
/// Each restorable sketch adds one Tier-2 query. That keeps restoration inside
/// the existing opt-in, one-table repair budget; it does not add work to the
/// census-only pass or the CLI.
#[tokio::test]
#[ignore = "measurement, not an assertion"]
async fn measure_census_and_repair_cost() {
    for rows_per_commit in [5_000i64, 50_000] {
        measure_one(rows_per_commit).await;
    }
}

async fn measure_one(rows_per_commit: i64) {
    const COMMITS: i64 = 8;
    let tmp = tempfile::tempdir().unwrap();
    let carry_wh = tmp.path().join("carry");
    let restore_wh = tmp.path().join("restore");
    let carry = prepare_cost_fixture(&carry_wh, "carry", rows_per_commit).await;
    let ice = prepare_cost_fixture(&restore_wh, "restore", rows_per_commit).await;
    let ident = ice.index_table_ident("restore");

    let started = std::time::Instant::now();
    let census = ice.repair_short_group_count_aggregates(0).await.unwrap();
    let census_ms = started.elapsed().as_secs_f64() * 1e3;
    assert!(
        matches!(
            census.first().map(|(_, o)| o),
            Some(ShortAggregateOutcome::Detected { .. })
        ),
        "{census:?}"
    );

    // The selected design reuses this marker writer's incarnation-scoped
    // OpenDAL PUT and retry policy, with a different body and filename. The
    // test helper includes one catalog load, so this is a conservative local
    // measurement of the extra write rather than a raw filesystem syscall.
    let started = std::time::Instant::now();
    ice.write_group_count_rebuild_marker_for_test(&ident, COMMITS, &[("host", 4_194_304)], &[])
        .await
        .unwrap();
    let marker_ms = started.elapsed().as_secs_f64() * 1e3;
    let marker = aggregate_artifacts(&restore_wh)
        .into_iter()
        .find(|p| p.to_string_lossy().ends_with(".rebuild.json"))
        .expect("durable marker was written");
    let marker_bytes = std::fs::metadata(marker).unwrap().len();

    ice.invalidate_cached_table(&ident).await;
    let started = std::time::Instant::now();
    let carry_report = carry.rebuild_group_count_aggregate("carry").await.unwrap();
    let carried_only_ms = started.elapsed().as_secs_f64() * 1e3;
    assert!(!carry_report.skipped_no_columns);
    let carried = read_wide(&carry_wh).sketches.unwrap();
    assert_eq!(carried.columns["service"].rows, 1);

    let started = std::time::Instant::now();
    let repair = ice.repair_short_group_count_aggregates(1).await.unwrap();
    let restoration_ms = started.elapsed().as_secs_f64() * 1e3;
    assert!(
        matches!(
            repair.first().map(|(_, o)| o),
            Some(ShortAggregateOutcome::Repaired { unrestored, .. })
                if unrestored == &["timestamp_ns".to_string()]
        ),
        "{repair:?}"
    );
    let restored = read_wide(&restore_wh).sketches.unwrap();
    assert_eq!(
        restored.columns["service"].rows,
        (COMMITS * rows_per_commit) as u64
    );

    let rows = (COMMITS * rows_per_commit) as f64;
    println!(
        "rows={rows:.0} commits={COMMITS} census={census_ms:.1}ms \
         marker_put={marker_ms:.1}ms marker_bytes={marker_bytes} \
         carried_only={carried_only_ms:.1}ms restoration={restoration_ms:.1}ms \
         restoration_delta={:.1}ms restoration_per_100k_rows={:.0}ms",
        restoration_ms - carried_only_ms,
        restoration_ms / rows * 100_000.0
    );
}

async fn prepare_cost_fixture(
    wh: &std::path::Path,
    index: &str,
    rows_per_commit: i64,
) -> IcebergContext {
    const COMMITS: i64 = 8;
    let ice = open(wh).await;
    let cfg = index_config(index);
    ice.create_index(&cfg).await.unwrap();
    let ident = ice.index_table_ident(index);
    for n in 0..COMMITS {
        ice.append_to_table(
            &ident,
            wide_batch(&cfg, n, rows_per_commit),
            &["host", "service"],
        )
        .await
        .unwrap();
    }
    let mut deltas: Vec<std::path::PathBuf> = aggregate_artifacts(wh)
        .into_iter()
        .filter(|path| path.to_string_lossy().contains("siglake-agg-deltas"))
        .collect();
    deltas.sort();
    std::fs::remove_file(&deltas[0]).unwrap();
    ice.fold_group_count_deltas(1).await.unwrap();

    let path = wide_path(wh);
    let mut wide = read_wide(wh);
    let mut exact = wide.decode_all().unwrap();
    exact.columns.remove("service").unwrap();
    wide.set_group_counts(Some(exact));
    let table_rows = (COMMITS * rows_per_commit) as u64;
    wide.sketches = Some(GroupCountSketches {
        version: GROUP_COUNT_SKETCH_VERSION,
        columns: BTreeMap::from([
            (
                "service".to_string(),
                ColumnSketch::from_exact(
                    64,
                    [("svc-00000000".to_string(), 1)].into_iter().collect(),
                    1,
                ),
            ),
            (
                "timestamp_ns".to_string(),
                ColumnSketch::from_exact(
                    64,
                    [("1700000000000000000".to_string(), table_rows)]
                        .into_iter()
                        .collect(),
                    table_rows,
                ),
            ),
        ]),
    });
    write_wide(&path, &wide);
    ice.invalidate_cached_table(&ident).await;
    ice
}

/// Local reproduction for #4628: cancel the same census+repair future the
/// compactor watchdog wraps, then create a new context (the restart) and prove
/// both that the aggregate CAS published nothing and that the next pass sees
/// the same table as repairable again.
///
/// The timeout is derived from a census-only reading of this exact fixture. It
/// is longer than that metadata phase and shorter than its Tier-2 repair, so
/// the cancellation lands during the file scan rather than while discovering
/// the deficit.
#[tokio::test]
#[ignore = "local cancellation/restart reproduction, not a correctness gate"]
async fn reproduce_watchdog_cancelled_repair_restart() {
    const COMMITS: i64 = 8;
    const ROWS_PER_COMMIT: i64 = 50_000;
    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = open(&wh).await;
    let cfg = index_config("watchdog");
    ice.create_index(&cfg).await.unwrap();
    let ident = ice.index_table_ident("watchdog");

    for n in 0..COMMITS {
        ice.append_to_table(&ident, wide_batch(&cfg, n, ROWS_PER_COMMIT), &["host"])
            .await
            .unwrap();
    }
    let mut deltas: Vec<std::path::PathBuf> = aggregate_artifacts(&wh)
        .into_iter()
        .filter(|p| p.to_string_lossy().contains("siglake-agg-deltas"))
        .collect();
    deltas.sort();
    std::fs::remove_file(&deltas[0]).unwrap();
    ice.fold_group_count_deltas(1).await.unwrap();
    ice.invalidate_cached_table(&ident).await;

    let census_started = std::time::Instant::now();
    let census = ice.repair_short_group_count_aggregates(0).await.unwrap();
    let census_elapsed = census_started.elapsed();
    assert!(matches!(
        census.first().map(|(_, outcome)| outcome),
        Some(ShortAggregateOutcome::Detected { .. })
    ));
    let before = std::fs::read(wide_path(&wh)).unwrap();

    ice.invalidate_cached_table(&ident).await;
    let timeout = (census_elapsed * 2).max(std::time::Duration::from_millis(50));
    let repair_started = std::time::Instant::now();
    let cut = tokio::time::timeout(timeout, ice.repair_short_group_count_aggregates(1)).await;
    let cut_elapsed = repair_started.elapsed();
    assert!(cut.is_err(), "fixture repair completed inside {timeout:?}");
    assert_eq!(
        std::fs::read(wide_path(&wh)).unwrap(),
        before,
        "a cancelled repair must not publish a partial aggregate"
    );
    drop(ice);

    let restarted = open(&wh).await;
    let after_restart = restarted
        .repair_short_group_count_aggregates(0)
        .await
        .unwrap();
    assert!(matches!(
        after_restart.first().map(|(_, outcome)| outcome),
        Some(ShortAggregateOutcome::BackedOff {
            reason: ShortAggregateAttemptReason::Started,
            ..
        })
    ));
    println!(
        "rows={} census={:.1}ms timeout={:.1}ms cut_after={:.1}ms \
         aggregate_unchanged=true restart_outcome=backed_off_started",
        COMMITS * ROWS_PER_COMMIT,
        census_elapsed.as_secs_f64() * 1e3,
        timeout.as_secs_f64() * 1e3,
        cut_elapsed.as_secs_f64() * 1e3,
    );
}

#[tokio::test]
async fn a_started_attempt_blocks_a_second_compactor_past_the_maintenance_lease() {
    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = open(&wh).await;
    make_short(&ice, "concurrent").await;
    let item = ice
        .census_short_group_count_aggregates()
        .await
        .unwrap()
        .into_iter()
        .find(|item| item.table == "concurrent")
        .unwrap();
    let first = ice
        .prepare_short_group_count_repair(&item, std::time::Duration::from_secs(600))
        .await
        .unwrap();
    assert!(matches!(first, ShortAggregateAttemptStart::Ready(_)));

    // The catalog maintenance lease is 300 seconds. The durable attempt's
    // 600-second watchdog remains the exclusion record after that lease can be
    // acquired by another compactor.
    let second = ice
        .prepare_short_group_count_repair(&item, std::time::Duration::from_secs(600))
        .await
        .unwrap();
    assert!(matches!(
        second,
        ShortAggregateAttemptStart::Outcome(ShortAggregateOutcome::BackedOff {
            attempts: 1,
            reason: ShortAggregateAttemptReason::Started,
            ..
        })
    ));
}

#[tokio::test]
async fn restart_turns_an_expired_started_attempt_into_durable_interrupted_backoff() {
    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = open(&wh).await;
    make_short(&ice, "restart").await;
    let item = ice
        .census_short_group_count_aggregates()
        .await
        .unwrap()
        .into_iter()
        .find(|item| item.table == "restart")
        .unwrap();
    assert!(matches!(
        ice.prepare_short_group_count_repair(&item, std::time::Duration::ZERO)
            .await
            .unwrap(),
        ShortAggregateAttemptStart::Ready(_)
    ));
    drop(ice);

    let restarted = open(&wh).await;
    let outcome = restarted
        .census_short_group_count_aggregates()
        .await
        .unwrap()
        .into_iter()
        .find(|item| item.table == "restart")
        .map(|item| item.outcome)
        .unwrap();
    assert!(matches!(
        &outcome,
        ShortAggregateOutcome::BackedOff {
            attempts: 1,
            reason: ShortAggregateAttemptReason::Interrupted,
            ..
        }
    ));

    let cfg = index_config("restart");
    let ident = restarted.index_table_ident("restart");
    restarted
        .append_to_table(&ident, batch(&cfg, 2), &["host"])
        .await
        .unwrap();
    restarted.fold_group_count_deltas(1).await.unwrap();
    restarted.invalidate_cached_table(&ident).await;
    let after_snapshot_advance = restarted
        .census_short_group_count_aggregates()
        .await
        .unwrap()
        .into_iter()
        .find(|item| item.table == "restart")
        .map(|item| item.outcome)
        .unwrap();
    assert!(matches!(
        after_snapshot_advance,
        ShortAggregateOutcome::BackedOff {
            attempts: 1,
            reason: ShortAggregateAttemptReason::Interrupted,
            ..
        }
    ));
}

/// The per-pass budget. Every table upgraded across #2919 is short at once, and
/// a warehouse's indexes are all short together, so an unbudgeted pass is a
/// whole-warehouse Tier-2 scan.
#[tokio::test]
async fn one_pass_repairs_at_most_its_budget() {
    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = open(&wh).await;
    // Both tables lose their FIRST commit's contribution and then take a second
    // commit, whose delta lands — so the gap is in the middle, where nothing
    // outstanding can close it.
    for id in ["first", "second"] {
        let cfg = index_config(id);
        ice.create_index(&cfg).await.unwrap();
        let ident = ice.index_table_ident(id);
        ice.append_to_table(&ident, batch(&cfg, 0), &["host"])
            .await
            .unwrap();
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let location = table.metadata().location().to_string();
        let dir = std::path::Path::new(location.strip_prefix("file://").unwrap_or(&location));
        let mut deltas: Vec<std::path::PathBuf> = walk(dir)
            .into_iter()
            .filter(|p| p.to_string_lossy().contains("siglake-agg-deltas"))
            .collect();
        deltas.sort();
        assert_eq!(deltas.len(), 1, "one commit, one delta: {deltas:?}");
        std::fs::remove_file(&deltas[0]).unwrap();
        ice.append_to_table(&ident, batch(&cfg, 1), &["host"])
            .await
            .unwrap();
        ice.invalidate_cached_table(&ident).await;
    }
    ice.fold_group_count_deltas(1).await.unwrap();

    let outcomes = ice.repair_short_group_count_aggregates(1).await.unwrap();
    let repaired = outcomes
        .iter()
        .filter(|(_, o)| matches!(o, ShortAggregateOutcome::Repaired { .. }))
        .count();
    let detected = outcomes
        .iter()
        .filter(|(_, o)| matches!(o, ShortAggregateOutcome::Detected { .. }))
        .count();
    assert_eq!(
        (outcomes.len(), repaired, detected),
        (2, 1, 1),
        "one rebuilt, the other reported and left for the next pass: {outcomes:?}"
    );

    // The next pass takes the one that waited, and then there is nothing left.
    let outcomes = ice.repair_short_group_count_aggregates(1).await.unwrap();
    assert_eq!(
        outcomes
            .iter()
            .filter(|(_, o)| matches!(o, ShortAggregateOutcome::Repaired { .. }))
            .count(),
        1,
        "{outcomes:?}"
    );
    assert!(ice
        .repair_short_group_count_aggregates(1)
        .await
        .unwrap()
        .is_empty());
}

/// #4737: one compactor censuses the base namespace and every `tenant_*`
/// namespace, and each holds its own `events`. The counter used to carry the
/// bare table name, so both short tables incremented `table="events"` and
/// `SiglakeGroupCountAggregateShort` named a table an operator could not
/// locate — let alone pass to `rebuild-group-counts --namespace`.
#[tokio::test]
async fn a_short_events_table_in_two_namespaces_is_two_series() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    /// One commit of events with distinct hosts, above the inline ceiling so
    /// the wide object is `host`'s only Tier-1 carrier.
    async fn commit(ice: &IcebergContext, nth: i64) {
        let base = 1_700_000_000i64 + nth * 100_000;
        let events: Vec<Event> = (0..ROWS)
            .map(|i| Event {
                timestamp: Utc.timestamp_opt(base + i, 0).single().unwrap(),
                host: format!("h-{:06}", nth * ROWS + i),
                source: "src".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: "request".into(),
                attributes: None,
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }

    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let base = open(&wh).await;
    let tenant = base.for_namespace("tenant_acme").await.unwrap();

    for ice in [&base, &tenant] {
        for nth in 0..2 {
            commit(ice, nth).await;
        }
        ice.fold_group_count_deltas(1).await.unwrap();
    }

    // #2919's upgrade, in both namespaces at once: the artifacts each
    // incarnation had been reading sit at a path nothing adopts any more, so
    // the next commit starts a fresh aggregate that is short of every row
    // before it — with no marker anywhere to say so.
    for artifact in aggregate_artifacts(&wh) {
        std::fs::remove_file(&artifact).unwrap();
    }
    for ice in [&base, &tenant] {
        commit(ice, 2).await;
        ice.fold_group_count_deltas(1).await.unwrap();
    }

    // Census only, no repair budget: the counter is what a default install
    // records, and it must not depend on a rebuild having run.
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);
    for ice in [&base, &tenant] {
        let outcomes = ice.repair_short_group_count_aggregates(0).await.unwrap();
        assert!(
            outcomes.iter().any(|(table, outcome)| table == "events"
                && matches!(outcome, ShortAggregateOutcome::Detected { .. })),
            "{} censused nothing short: {outcomes:?}",
            ice.namespace()
        );
    }
    drop(guard);

    let mut series: Vec<(String, u64)> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| key.key().name() == "siglake_group_count_short_aggregates_total")
        .map(|(key, _, _, value)| {
            let label = |name: &str| {
                key.key()
                    .labels()
                    .find(|l| l.key() == name)
                    .map(|l| l.value().to_string())
                    .unwrap_or_else(|| panic!("no {name} label on {key:?}"))
            };
            assert_eq!(label("table"), "events", "{key:?}");
            assert_eq!(label("outcome"), "detected", "{key:?}");
            let DebugValue::Counter(count) = value else {
                panic!("{value:?} is not a counter");
            };
            (label("iceberg_namespace"), count)
        })
        .collect();
    series.sort();
    assert_eq!(
        series,
        vec![("siglake".to_string(), 1), ("tenant_acme".to_string(), 1),],
        "two namespaces' events tables must reach the alert as two series, \
         each at its own count"
    );
}
