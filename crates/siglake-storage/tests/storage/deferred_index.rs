//! B.1.2 defer-index: with `SIGLAKE_INDEX_AT_FLUSH=0`, ingest-time writes skip
//! ALL raw-text index work (inverted-index footer keys, file-level trigram
//! bloom, row-group token blooms, Puffin sidecar) — the measured ~30 %-of-append
//! cost — while compaction (a rewrite, gen ≥ 1) builds them on the merged
//! output: consolidation is the materialization point. Queries stay correct
//! throughout (the funnel falls back to a scan for unindexed files).

use siglake_bloom::{RAW_TRIGRAM_BLOOM_KV_KEY, RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY};
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, BLOOM_FILTER_COLUMNS};

use crate::fixture_clock::{assert_one_partition, fixture_base};

/// One second apart off [`fixture_base`], so the four appends below re-cluster
/// as one `day(timestamp)` partition whatever time of day the suite runs
/// (#5678).
fn event_at(offset_secs: i64, raw: String) -> Event {
    Event {
        timestamp: fixture_base() + chrono::Duration::seconds(offset_secs),
        ..Event::now(raw)
    }
}

fn footer_keys(path: &std::path::Path) -> Vec<String> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let bytes = std::fs::read(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .map(|kv| kv.iter().map(|e| e.key.clone()).collect())
        .unwrap_or_default()
}

fn find_parquet(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(find_parquet(&p));
            } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                out.push(p);
            }
        }
    }
    out
}

fn has_index_keys(keys: &[String]) -> bool {
    keys.iter().any(|k| {
        k == RAW_TRIGRAM_BLOOM_KV_KEY
            || k == RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY
            || k.starts_with("siglake.inverted_index")
    })
}

#[tokio::test]
async fn deferred_table_skips_index_at_flush_and_materializes_at_compaction() {
    // Marker-bearing parent (#6126): the gen-1 filter below reads the file name,
    // so a directory called `siglake-g1-…` must not make every file look merged.
    let tmp = tempfile::Builder::new()
        .prefix("siglake-g1-parent-")
        .tempdir()
        .unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            index_at_flush: Some(false),
            ..Default::default()
        });

    // Four ingest-time appends (gen 0) — all index work must be skipped.
    for i in 0..4 {
        ice.append_events(&[event_at(i, format!("database timeout batch {i}"))])
            .await
            .unwrap();
    }
    let data_dir = warehouse.join("siglake/events/data");
    let flushed = find_parquet(&data_dir);
    assert_eq!(flushed.len(), 4);
    for f in &flushed {
        let keys = footer_keys(f);
        assert!(
            !has_index_keys(&keys),
            "deferred flush must carry no index keys, got {keys:?} in {f:?}"
        );
    }
    // No Puffin sidecars either (nothing to upload when nothing was built).
    let puffins: Vec<_> = {
        fn find_puffin(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            if let Ok(rd) = std::fs::read_dir(dir) {
                for e in rd.flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        find_puffin(&p, out);
                    } else if p.to_string_lossy().contains("siglake-index-") {
                        out.push(p);
                    }
                }
            }
        }
        let mut v = Vec::new();
        find_puffin(&warehouse, &mut v);
        v
    };
    assert!(
        puffins.is_empty(),
        "deferred flush must write no Puffin sidecars: {puffins:?}"
    );

    // Queries on unindexed files stay correct (funnel falls back to a scan).
    let ctx = datafusion::prelude::SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let batches = ctx
        .sql("SELECT count(*) AS n FROM events WHERE raw LIKE '%database timeout%'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 4, "LIKE over unindexed files must still match");

    // Compaction (gen ≥ 1) materializes the indexes on the merged output.
    let ident = ice.events_table_ident().clone();
    let files = ice.live_data_files(&ident).await.unwrap();
    assert_one_partition(&files, "deferred_table_skips_index_at_flush");
    ice.recluster_files(&ident, files, BLOOM_FILTER_COLUMNS)
        .await
        .unwrap();
    let merged: Vec<_> = find_parquet(&data_dir)
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("siglake-g1-"))
        })
        .collect();
    assert_eq!(merged.len(), 1, "one gen-1 merged output");
    let keys = footer_keys(&merged[0]);
    assert!(
        keys.iter().any(|k| k == RAW_TRIGRAM_BLOOM_KV_KEY),
        "merged output must carry the trigram bloom, got {keys:?}"
    );
    assert!(
        keys.iter().any(|k| k.starts_with("siglake.inverted_index")),
        "merged output must carry inverted-index footer keys, got {keys:?}"
    );

    // And the data is still all there, still searchable.
    let batches = ctx
        .sql("SELECT count(*) AS n FROM events WHERE raw LIKE '%database timeout%'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 4, "post-compaction LIKE must still match all rows");
}
