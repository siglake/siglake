//! A table born before typed columns joined the side aggregates (2c597f8) has
//! `status` in every file's group-count footer and in NO aggregate. The read
//! path serves it from the per-file tier for the life of the table, and the
//! rebuild — which takes its column set from the aggregate on purpose — could
//! not change that: it told the operator "only a table rewrite recovers those"
//! about a total it could have computed exactly.
//!
//! `admit_typed_columns` is the opt-in that lets it. This pins the three
//! outcomes an admitted column can have — written and serving Tier-1, counted
//! but held back by the typed cap, and left absent because some live file
//! cannot serve it — and that the default is unchanged.
//!
//! No env is set: the default table cap (4,194,304) is above the inline
//! ceiling, so the base+delta path is on, and every on-disk edit is followed by
//! `invalidate_cached_table`, which is what a commit does to the caches.

use std::path::{Path, PathBuf};

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use siglake_core::index_config::{DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode};
use siglake_storage::iceberg::{
    GroupCountDelta, GroupCountRebuildOptions, IcebergContext, SnapshotAggregates, WideGroupCounts,
};

const ROWS: i64 = 3_000;
const BATCHES: i64 = 4;
/// Distinct `size` values per batch: under the per-file footer cap and the
/// typed cap (both 1024), so every file carries a footer for it — but disjoint
/// across batches, so the WHOLE-TABLE count (2,000) is over the typed cap.
const SIZES_PER_BATCH: i64 = 500;
/// Distinct `bytes` values per batch: over the per-file cap, so no file has a
/// footer for it, and an Int64 column is not something the raw-page decode
/// reads. Nothing can serve it.
const BYTES_PER_BATCH: i64 = 1_500;

const FLOAT_ROWS: i64 = 60;
const FLOAT_BATCHES: i64 = 2;
const FLOAT_DISTINCT: usize = 4;

fn field(name: &str, field_type: FieldType) -> FieldMapping {
    FieldMapping {
        name: name.to_string(),
        field_type,
        required: name == "timestamp",
    }
}

/// http_logs in miniature: one declared text dim and three typed columns of
/// three different widths.
fn config() -> IndexConfig {
    IndexConfig {
        index_id: "prefix".to_string(),
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                field("timestamp", FieldType::Datetime),
                field(
                    "method",
                    FieldType::Text {
                        tokenizer: Some("raw".into()),
                    },
                ),
                field("status", FieldType::Long),
                field("size", FieldType::Long),
                field("bytes", FieldType::Long),
            ],
            timestamp_field: "timestamp".to_string(),
            tag_fields: vec!["method".to_string()],
            default_search_fields: vec![],
        },
        retention: None,
        index_at_flush: None,
    }
}

fn float_config(index_id: &str) -> IndexConfig {
    IndexConfig {
        index_id: index_id.to_string(),
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                field("timestamp", FieldType::Datetime),
                field("ratio", FieldType::Double),
            ],
            timestamp_field: "timestamp".to_string(),
            tag_fields: vec![],
            default_search_fields: vec![],
        },
        retention: None,
        index_at_flush: None,
    }
}

fn batch(config: &IndexConfig, b: i64) -> RecordBatch {
    let base = 1_700_000_000_000_000i64 + b * 100_000_000;
    RecordBatch::try_new(
        config.to_arrow_schema(),
        vec![
            std::sync::Arc::new(
                TimestampMicrosecondArray::from(
                    (0..ROWS).map(|i| Some(base + i)).collect::<Vec<_>>(),
                )
                .with_timezone("+00:00"),
            ),
            std::sync::Arc::new(StringArray::from(
                (0..ROWS)
                    .map(|i| if i % 2 == 0 { "GET" } else { "POST" })
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(Int64Array::from(
                (0..ROWS)
                    .map(|i| [200i64, 200, 304, 404, 500, 302][(i % 6) as usize])
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(Int64Array::from(
                (0..ROWS)
                    .map(|i| b * SIZES_PER_BATCH + (i % SIZES_PER_BATCH))
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(Int64Array::from(
                (0..ROWS)
                    .map(|i| b * BYTES_PER_BATCH + (i % BYTES_PER_BATCH))
                    .collect::<Vec<_>>(),
            )),
            // Dynamic mapping mode appends the WS-7 residual `attributes` column.
            std::sync::Arc::new(StringArray::from(vec![None::<&str>; ROWS as usize])),
        ],
    )
    .unwrap()
}

fn float_batch(config: &IndexConfig, b: i64) -> RecordBatch {
    let base = 1_700_000_000_000_000i64 + b * 100_000_000;
    RecordBatch::try_new(
        config.to_arrow_schema(),
        vec![
            std::sync::Arc::new(
                TimestampMicrosecondArray::from(
                    (0..FLOAT_ROWS).map(|i| Some(base + i)).collect::<Vec<_>>(),
                )
                .with_timezone("+00:00"),
            ),
            // Four values are deliberately far below the default typed cap
            // (1024), so this test needs no process-global environment knob.
            std::sync::Arc::new(Float64Array::from(
                (0..FLOAT_ROWS)
                    .map(|i| [0.25, 1.5, 2.75, 4.0][(i as usize) % FLOAT_DISTINCT])
                    .collect::<Vec<_>>(),
            )),
            // Dynamic mapping mode appends the WS-7 residual `attributes` column.
            std::sync::Arc::new(StringArray::from(vec![None::<&str>; FLOAT_ROWS as usize])),
        ],
    )
    .unwrap()
}

fn walk(dir: &Path) -> Vec<PathBuf> {
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

/// Put `table`'s aggregates back into the pre-2c597f8 shape: `columns` gone
/// from the inline object, the wide object and every un-absorbed delta, while
/// the data files — and their footers — are untouched. That is exactly the
/// state a table created before the fix is in. Returns how many objects it
/// edited, so a layout change cannot silently turn this into a no-op.
fn strip_columns_from_aggregates(warehouse: &Path, table: &str, columns: &[&str]) -> usize {
    let mut edited = 0;
    for path in walk(warehouse) {
        let s = path.to_string_lossy().to_string();
        if !s.contains(&format!("/{table}/")) {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let bytes = std::fs::read(&path).unwrap();
        let out = if name == "siglake-aggregates.json" {
            let mut aggs: SnapshotAggregates = serde_json::from_slice(&bytes).unwrap();
            if let Some(gc) = aggs.group_counts.as_mut() {
                for c in columns {
                    gc.columns.remove(*c);
                }
            }
            serde_json::to_vec(&aggs).unwrap()
        } else if name == "siglake-agg-wide.json" {
            let mut wide: WideGroupCounts = serde_json::from_slice(&bytes).unwrap();
            if let Some(mut all) = wide.decode_all() {
                for c in columns {
                    all.columns.remove(*c);
                }
                wide.set_group_counts(Some(all));
            }
            serde_json::to_vec(&wide).unwrap()
        } else if s.contains("/siglake-agg-deltas/") {
            let mut delta: GroupCountDelta = serde_json::from_slice(&bytes).unwrap();
            if let Some(gc) = delta.group_counts.as_mut() {
                for c in columns {
                    gc.columns.remove(*c);
                }
            }
            serde_json::to_vec(&delta).unwrap()
        } else {
            continue;
        };
        std::fs::write(&path, out).unwrap();
        edited += 1;
    }
    edited
}

async fn served_by(ice: &IcebergContext, table: &str, column: &str) -> Option<&'static str> {
    ice.grouped_counts_with_summary(table, column, None, None)
        .await
        .unwrap()
        .map(|g| g.source_label())
}

async fn rows_of(ice: &IcebergContext, table: &str, column: &str) -> Vec<(Option<String>, u64)> {
    let mut rows = ice
        .grouped_counts_with_summary(table, column, None, None)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("counts for {column}"))
        .to_rows();
    rows.sort();
    rows
}

async fn assert_tier1_total(
    ice: &IcebergContext,
    table: &str,
    column: &str,
    tier: &str,
    record_count: u64,
) {
    let counts = ice
        .grouped_counts_with_summary(table, column, None, None)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("counts for {column}"));
    assert_eq!(counts.source_label(), tier);
    assert_eq!(counts.len(), FLOAT_DISTINCT);
    assert_eq!(
        counts.iter().map(|(_, count)| count).sum::<u64>(),
        record_count,
        "per-key counts must sum to the table record count"
    );
}

#[tokio::test]
async fn float64_counts_use_tier1_after_write_and_typed_rebuild() {
    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&wh).await.unwrap();

    // The normal write path must cast Float64 keys into the Tier-1 aggregate.
    let write_config = float_config("floatwrite");
    ice.create_index(&write_config).await.unwrap();
    let write_ident = ice.index_table_ident(&write_config.index_id);
    for b in 0..FLOAT_BATCHES {
        ice.append_to_table(&write_ident, float_batch(&write_config, b), &[])
            .await
            .unwrap();
    }
    let write_record_count = ice
        .live_data_files(&write_ident)
        .await
        .unwrap()
        .iter()
        .map(|file| file.record_count())
        .sum();
    assert_tier1_total(
        &ice,
        &write_config.index_id,
        "ratio",
        "tier1_inline",
        write_record_count,
    )
    .await;

    // Emulate a table written before Float64 joined the side aggregate: keep
    // its data-file footers but remove the column from every aggregate object.
    let rebuild_config = float_config("floatrebuild");
    ice.create_index(&rebuild_config).await.unwrap();
    let rebuild_ident = ice.index_table_ident(&rebuild_config.index_id);
    for b in 0..FLOAT_BATCHES {
        ice.append_to_table(&rebuild_ident, float_batch(&rebuild_config, b), &[])
            .await
            .unwrap();
    }
    let rebuild_record_count = ice
        .live_data_files(&rebuild_ident)
        .await
        .unwrap()
        .iter()
        .map(|file| file.record_count())
        .sum();
    let edited = strip_columns_from_aggregates(&wh, rebuild_ident.name(), &["ratio"]);
    assert!(
        edited >= 2,
        "expected aggregate objects to edit, got {edited}"
    );
    ice.invalidate_cached_table(&rebuild_ident).await;
    assert_eq!(
        served_by(&ice, &rebuild_config.index_id, "ratio").await,
        Some("materialized"),
        "precondition: the legacy table has only per-file Float64 counts"
    );

    let report = ice
        .rebuild_group_count_aggregate_with(
            &rebuild_config.index_id,
            GroupCountRebuildOptions {
                admit_typed_columns: true,
            },
        )
        .await
        .unwrap();
    let ratio = report
        .columns
        .iter()
        .find(|column| column.column == "ratio")
        .expect("ratio in rebuild report");
    assert!(ratio.admitted);
    assert_eq!(ratio.rows, Some(rebuild_record_count));
    assert_eq!(ratio.distinct, FLOAT_DISTINCT);
    assert!(ratio.covers_table);
    assert_tier1_total(
        &ice,
        &rebuild_config.index_id,
        "ratio",
        "tier1_wide",
        rebuild_record_count,
    )
    .await;
}

#[tokio::test]
async fn admitting_typed_columns_restores_tier1_and_refuses_what_it_must() {
    let tmp = tempfile::tempdir().unwrap();
    let wh = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&wh).await.unwrap();
    let config = config();
    ice.create_index(&config).await.unwrap();
    let ident = ice.index_table_ident(&config.index_id);
    let table = config.index_id.as_str();

    for b in 0..BATCHES {
        ice.append_to_table(&ident, batch(&config, b), &["method"])
            .await
            .unwrap();
    }
    let total = (ROWS * BATCHES) as u64;

    // A table born AFTER the fix: `status` and `size` are in the aggregate.
    // `bytes` never is — every batch is over the per-file cap, so the write path
    // sketches it — and nothing can read it, since no file has a footer for it
    // and the raw-page decode does not do Int64.
    assert_eq!(served_by(&ice, table, "status").await, Some("tier1_inline"));
    assert_eq!(served_by(&ice, table, "method").await, Some("tier1_inline"));
    assert_eq!(served_by(&ice, table, "bytes").await, None);
    let status_rows = rows_of(&ice, table, "status").await;
    assert_eq!(status_rows.iter().map(|(_, c)| c).sum::<u64>(), total);

    // Now make it a table born BEFORE the fix.
    let edited = strip_columns_from_aggregates(&wh, ident.name(), &["status", "size"]);
    assert!(
        edited >= 2,
        "expected to edit the inline object and at least one delta, edited {edited}"
    );
    ice.invalidate_cached_table(&ident).await;
    assert_eq!(
        served_by(&ice, table, "status").await,
        Some("materialized"),
        "precondition: the aggregate no longer carries the typed column"
    );
    assert_eq!(served_by(&ice, table, "size").await, Some("materialized"));
    assert_eq!(
        served_by(&ice, table, "method").await,
        Some("tier1_inline"),
        "the declared text dim is untouched"
    );
    assert_eq!(
        rows_of(&ice, table, "status").await,
        status_rows,
        "the per-file path is exact; only the tier changed"
    );

    // DEFAULT behaviour is unchanged: the rebuild repairs what the aggregate
    // holds and does not invent columns — but it now SAYS which ones it could.
    let report = ice.rebuild_group_count_aggregate(table).await.unwrap();
    assert!(!report.skipped_no_columns);
    assert_eq!(
        report
            .columns
            .iter()
            .map(|c| c.column.as_str())
            .collect::<Vec<_>>(),
        vec!["method"],
        "without the flag only the maintained column is touched"
    );
    assert!(report.columns.iter().all(|c| !c.admitted));
    assert_eq!(
        report.admissible_typed_columns,
        vec![
            "bytes".to_string(),
            "size".to_string(),
            "status".to_string()
        ],
        "the typed columns the flag would add are reported even when it is off"
    );
    assert_eq!(
        served_by(&ice, table, "status").await,
        Some("materialized"),
        "a plain rebuild must not admit anything"
    );

    // THE FLAG. Three admitted columns, three outcomes.
    let report = ice
        .rebuild_group_count_aggregate_with(
            table,
            GroupCountRebuildOptions {
                admit_typed_columns: true,
            },
        )
        .await
        .unwrap();
    let col = |name: &str| {
        report
            .columns
            .iter()
            .find(|c| c.column == name)
            .unwrap_or_else(|| panic!("{name} in the report"))
    };

    // Maintained: repaired, and reported as such.
    let method = col("method");
    assert!(!method.admitted);
    assert!(method.covers_table);
    assert_eq!(method.over_cap, None);

    // Admitted and in every file's footer: written, exact, covers the table.
    let status = col("status");
    assert!(status.admitted, "status was not in the aggregate");
    assert_eq!(status.rows, Some(total));
    assert_eq!(status.distinct, 5);
    assert_eq!(status.over_cap, None);
    assert!(status.covers_table, "and must now pass the read guard");

    // Admitted, readable, but 2,000 distinct table-wide: counted exactly, then
    // held back by the typed cap rather than written.
    let size = col("size");
    assert!(size.admitted);
    assert_eq!(size.rows, Some(total), "the count itself is exact");
    assert_eq!(size.distinct, (SIZES_PER_BATCH * BATCHES) as usize);
    assert_eq!(
        size.over_cap,
        Some(1024),
        "the typed cap applies to an admitted column's whole-table width"
    );
    assert!(
        !size.covers_table,
        "nothing was written, so it cannot cover"
    );

    // Admitted but unreadable: left absent, never a partial total.
    let bytes = col("bytes");
    assert!(bytes.admitted);
    assert_eq!(
        bytes.rows, None,
        "no tier can serve it; must not be invented"
    );
    assert!(!bytes.covers_table);
    assert_eq!(bytes.over_cap, None);

    // The read path agrees with the report. The admitted column lands in the
    // WIDE object — the rebuild's only output — so it reads back `tier1_wide`,
    // which is the same warm-metadata cost as `tier1_inline`; `materialized`
    // is the per-file tier this exists to get off.
    assert_eq!(
        served_by(&ice, table, "status").await,
        Some("tier1_wide"),
        "the admitted column must be served by the table-level aggregate"
    );
    assert_eq!(rows_of(&ice, table, "status").await, status_rows);
    assert_eq!(
        served_by(&ice, table, "size").await,
        Some("materialized"),
        "an over-cap column must not have been written"
    );
    assert_eq!(served_by(&ice, table, "bytes").await, None);

    // And a later commit folds on top exactly once: its delta is above the
    // watermark, so the admitted column stays complete and stays Tier-1.
    ice.append_to_table(&ident, batch(&config, BATCHES), &["method"])
        .await
        .unwrap();
    let g = ice
        .grouped_counts_with_summary(table, "status", None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        g.iter().map(|(_, c)| c).sum::<u64>(),
        (ROWS * (BATCHES + 1)) as u64,
        "a post-rebuild commit must add its rows exactly once"
    );
    assert_eq!(g.source_label(), "tier1_wide");
}

/// The flag on a table whose aggregate already carries its typed columns is
/// not a rewrite of the aggregate: maintained columns are repaired as before,
/// and only what was genuinely missing is admitted.
#[tokio::test]
async fn the_flag_admits_only_what_the_aggregate_lacks() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let config = config();
    ice.create_index(&config).await.unwrap();
    let ident = ice.index_table_ident(&config.index_id);
    for b in 0..2 {
        ice.append_to_table(&ident, batch(&config, b), &["method"])
            .await
            .unwrap();
    }

    let report = ice
        .rebuild_group_count_aggregate_with(
            &config.index_id,
            GroupCountRebuildOptions {
                admit_typed_columns: true,
            },
        )
        .await
        .unwrap();
    // `bytes` is the only typed column the write path never counted exactly.
    assert_eq!(report.admissible_typed_columns, vec!["bytes".to_string()]);
    for c in &report.columns {
        match c.column.as_str() {
            "bytes" => {
                assert!(c.admitted);
                assert_eq!(c.rows, None);
            }
            other => {
                assert!(!c.admitted, "{other} was maintained, not admitted");
                assert!(c.covers_table, "{other} must be repaired in place");
            }
        }
    }
    assert_eq!(
        served_by(&ice, &config.index_id, "status").await,
        Some("tier1_inline"),
        "a column the inline object already covers keeps being served from it"
    );
}
