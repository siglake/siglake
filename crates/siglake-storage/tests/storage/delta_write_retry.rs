//! A lost group-count delta degrades a table until it is rebuilt, so a transient
//! write failure must be retried and an exhausted write must schedule repair.
//!
//! Both halves come from the 2026-09-02 round. One delta write failed with an
//! error the object store itself labelled temporary:
//!
//!   Unexpected (temporary) at write, context: { called: reqsign::LoadCredential,
//!   written: 309809 } => loading credential to sign http request, source: error
//!   sending request for url (http://169.254.169.254/latest/api/token)
//!
//! an IMDS credential refresh that timed out on an instance writing fine either
//! side of it. There was no retry. `top_hosts` went 140ms -> 3,451ms and
//! `count_distinct_host` 116ms -> 3,194ms, until the table was rebuilt.

use std::cell::Cell;
use std::sync::atomic::{AtomicU32, Ordering};

/// Recovery: a write that fails twice and then succeeds must be reported as a
/// success, not lost.
#[tokio::test]
async fn a_transient_write_failure_is_retried_and_recovers() {
    let calls = AtomicU32::new(0);
    let out =
        siglake_storage::iceberg::retry_delta_write_for_test("events", "seq-1.json", 4, || {
            let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if n < 3 {
                    Err(anyhow::anyhow!(
                        "Unexpected (temporary) at write: credential refresh"
                    ))
                } else {
                    Ok(())
                }
            }
        })
        .await;
    assert!(out.is_ok(), "should have recovered: {out:?}");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "should stop at first success"
    );
}

/// A single attempt is not a retry policy — this is the shape of the defect.
#[tokio::test]
async fn one_attempt_loses_a_transient_failure() {
    let calls = AtomicU32::new(0);
    let out =
        siglake_storage::iceberg::retry_delta_write_for_test("events", "seq-2.json", 1, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Err(anyhow::anyhow!("temporary")) }
        })
        .await;
    assert!(out.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// Exhaustion: a sustained outage must surface a real error carrying the CAUSE,
/// not a bare path. The 09-02 log did carry its `Caused by:` chain — that part
/// was never broken, and this pins it so it stays that way.
#[tokio::test]
async fn exhausted_attempts_surface_the_underlying_cause() {
    let calls = AtomicU32::new(0);
    let out =
        siglake_storage::iceberg::retry_delta_write_for_test("events", "seq-3.json", 4, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Err(anyhow::anyhow!("IMDS token fetch timed out")) }
        })
        .await;
    let err = out.expect_err("a sustained outage must fail");
    assert_eq!(calls.load(Ordering::SeqCst), 4, "all attempts used");
    let chain = format!("{err:?}");
    assert!(
        chain.contains("IMDS token fetch timed out"),
        "the cause must survive to the log line, got: {chain}"
    );
    assert!(
        chain.contains("seq-3.json") && chain.contains("attempt 4/4"),
        "and it must say WHICH delta and that attempts were spent, got: {chain}"
    );
}

/// A serialization-shaped failure would fail identically every time; the policy
/// must not spin on one for longer than its budget.
#[tokio::test]
async fn the_attempt_budget_is_bounded() {
    let calls = Cell::new(0u32);
    let out =
        siglake_storage::iceberg::retry_delta_write_for_test("events", "seq-4.json", 3, || {
            calls.set(calls.get() + 1);
            async move { Err(anyhow::anyhow!("permanent")) }
        })
        .await;
    assert!(out.is_err());
    assert_eq!(calls.get(), 3, "exactly the budget, no more");
}

/// The claim in the operator-facing warning, verified: a LOST delta is not
/// repaired by a later one.
///
/// The old message said the fast path came back "until a later commit's delta
/// lands", which reads as self-healing and is false — a later delta adds its OWN
/// rows, so the running total stays short of `record_count` and the read guard
/// keeps refusing Tier-1. That message would have sent an operator to wait
/// instead of to rebuild. Asserting the corrected behaviour rather than trusting
/// the corrected wording.
#[tokio::test]
async fn a_lost_delta_is_not_healed_by_later_deltas() {
    use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
    use siglake_core::index_config::{
        DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode,
    };
    use siglake_storage::iceberg::IcebergContext;

    // Above BASE (4096) so the base+delta path is on and deltas exist at all.
    let cfg = IndexConfig {
        index_id: "lost".to_string(),
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
            ],
            timestamp_field: "timestamp".to_string(),
            tag_fields: vec!["host".to_string()],
            default_search_fields: vec![],
        },
        retention: None,
        index_at_flush: None,
    };

    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&wh).await.unwrap().with_tuning(
        siglake_storage::iceberg::IcebergTuning {
            table_group_count_cardinality: Some(4_194_304),
            result_caches: Some(false),
            ..Default::default()
        },
    );
    ice.create_index(&cfg).await.unwrap();
    let ident = ice.index_table_ident("lost");

    // Wide enough that `host` cannot fit the inline cap, so it lives only in the
    // wide aggregate the deltas feed.
    let rows = 3_000i64;
    let append = |b: i64| {
        let schema = cfg.to_arrow_schema();
        let base = 1_700_000_000_000_000i64 + b * 100_000_000;
        RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(
                    TimestampMicrosecondArray::from(
                        (0..rows).map(|i| Some(base + i)).collect::<Vec<_>>(),
                    )
                    .with_timezone("+00:00"),
                ),
                std::sync::Arc::new(StringArray::from(
                    (0..rows)
                        .map(|i| format!("h-{:06}", b * rows + i))
                        .collect::<Vec<_>>(),
                )),
                std::sync::Arc::new(StringArray::from(vec![None::<&str>; rows as usize])),
            ],
        )
        .unwrap()
    };

    for b in 0..4i64 {
        ice.append_to_table(&ident, append(b), &["host"])
            .await
            .unwrap();
    }
    let g = ice
        .grouped_counts_with_summary("lost", "host", None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        g.source_label(),
        "tier1_wide",
        "baseline: host is served by the wide aggregate"
    );

    // Lose ONE delta, exactly as a failed write does.
    let mut deltas: Vec<std::path::PathBuf> = walk(&wh)
        .into_iter()
        .filter(|p| p.to_string_lossy().contains("siglake-agg-deltas"))
        .collect();
    deltas.sort();
    assert!(
        !deltas.is_empty(),
        "the base+delta path should have written deltas"
    );
    let victim = deltas[deltas.len() / 2].clone();
    std::fs::remove_file(&victim).unwrap();

    // Later commits land their own deltas — the thing the old message said would
    // fix it.
    for b in 4..7i64 {
        ice.append_to_table(&ident, append(b), &["host"])
            .await
            .unwrap();
    }

    let g = ice
        .grouped_counts_with_summary("lost", "host", None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        g.source_label(),
        "materialized",
        "a lost delta must NOT be healed by later deltas — if this now says tier1_wide, \
         something repairs the aggregate and the operator-facing warning needs rewriting again"
    );
    // And the answer must still be RIGHT, just expensive: the footer path is exact.
    let total: u64 = g.iter().map(|(_, c)| c).sum();
    assert_eq!(total, (rows * 7) as u64, "the fallback is exact, only slow");
}

/// The maintenance compactor's automatic repair, on the exact state the
/// previous test leaves behind: a table whose wide aggregate is short because a
/// delta was lost. This is the 2026-09-02 field failure in miniature. Losing the
/// first delta also proves the marker carries the initial column census; there
/// is no later aggregate from which the rebuild could discover `host`.
///
/// `host` is the column that matters here and the reason the rebuild reads
/// through Tier-2 rather than footers alone: it exceeds the per-file footer cap
/// in most files, so a footer-only rebuild would compute a SHORT total and
/// persist it as authoritative — a repair that corrupts.
#[tokio::test]
async fn a_lost_delta_marker_automatically_rebuilds_from_files() {
    use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
    use siglake_core::index_config::{
        DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode,
    };
    use siglake_storage::iceberg::IcebergContext;

    let cfg = IndexConfig {
        index_id: "repair".to_string(),
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
            ],
            timestamp_field: "timestamp".to_string(),
            tag_fields: vec!["host".to_string()],
            default_search_fields: vec![],
        },
        retention: None,
        index_at_flush: None,
    };

    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&wh).await.unwrap().with_tuning(
        siglake_storage::iceberg::IcebergTuning {
            table_group_count_cardinality: Some(4_194_304),
            result_caches: Some(false),
            ..Default::default()
        },
    );
    ice.create_index(&cfg).await.unwrap();
    let ident = ice.index_table_ident("repair");

    let rows = 5_000i64;
    let mk = |b: i64| {
        let schema = cfg.to_arrow_schema();
        let base = 1_700_000_000_000_000i64 + b * 100_000_000;
        RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(
                    TimestampMicrosecondArray::from(
                        (0..rows).map(|i| Some(base + i)).collect::<Vec<_>>(),
                    )
                    .with_timezone("+00:00"),
                ),
                std::sync::Arc::new(StringArray::from(
                    (0..rows)
                        .map(|i| format!("h-{:06}", b * rows + i))
                        .collect::<Vec<_>>(),
                )),
                std::sync::Arc::new(StringArray::from(vec![None::<&str>; rows as usize])),
            ],
        )
        .unwrap()
    };

    ice.append_to_table(&ident, mk(0), &["host"]).await.unwrap();
    // Lose the table's first and only delta.
    let mut deltas: Vec<std::path::PathBuf> = walk(&wh)
        .into_iter()
        .filter(|p| p.to_string_lossy().contains("siglake-agg-deltas"))
        .collect();
    deltas.sort();
    assert!(!deltas.is_empty());
    assert_eq!(deltas.len(), 1, "precondition: this is the first delta");
    let victim = &deltas[0];
    let lost_sequence: i64 = victim
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    std::fs::remove_file(victim).unwrap();
    ice.write_group_count_rebuild_marker_for_test(
        &ident,
        lost_sequence,
        &[("host", 4_194_304)],
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        ice.grouped_counts_with_summary("repair", "host", None, None)
            .await
            .unwrap()
            .unwrap()
            .source_label(),
        "materialized",
        "precondition: the aggregate is short and Tier-1 is refused"
    );

    // The next aggregate-maintenance pass sees the durable marker and repairs
    // the table; no operator command is involved.
    let outcomes = ice.fold_group_count_deltas(1).await.unwrap();
    let repair = outcomes
        .iter()
        .find(|(table, _)| table == "repair")
        .map(|(_, outcome)| outcome)
        .expect("repair table has a maintenance outcome");
    assert!(
        repair.rebuilt,
        "the lost-delta marker must trigger a rebuild"
    );
    assert_eq!(
        repair.repair_markers_deleted, 1,
        "the covered marker is consumed"
    );

    // Tier-1 is back, and still exact.
    let g = ice
        .grouped_counts_with_summary("repair", "host", None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        g.source_label(),
        "tier1_wide",
        "the rebuild must restore the cheap tier"
    );
    let total: u64 = g.iter().map(|(_, c)| c).sum();
    assert_eq!(total, rows as u64);

    // And a later commit must not double-count: its delta is ABOVE the
    // watermark, so it folds normally on top of the rebuilt base.
    ice.append_to_table(&ident, mk(1), &["host"]).await.unwrap();
    let g = ice
        .grouped_counts_with_summary("repair", "host", None, None)
        .await
        .unwrap()
        .unwrap();
    let total: u64 = g.iter().map(|(_, c)| c).sum();
    assert_eq!(
        total,
        (rows * 2) as u64,
        "a post-rebuild commit must add its rows exactly once"
    );
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
