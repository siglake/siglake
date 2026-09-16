//! Regression (sortedness audit): the streaming k-way recluster merge must
//! honor the table's DECLARED sort direction. siglake's reverse-scan work made
//! DESC-declared `events` tables a real, runtime-reachable state
//! (`replace_sort_order().desc("timestamp")`), and the writer + footer + WS-3
//! gate all honor the declared direction — but the streaming merge was hardcoded
//! ascending. So a DESC table re-clustered via the streaming path came out
//! mis-ordered under a DESC `SortingColumn` footer the reader trusts (wrong rows
//! for `ORDER BY timestamp DESC LIMIT`). The merge is now direction-aware; this
//! proves it end-to-end on a DESC-declared table.
//!
//! The merge path is pinned per call so the fixture does not depend on
//! process-global configuration.

use chrono::{Duration, TimeZone, Utc};
use iceberg::spec::NullOrder;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::TableIdent;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, ReclusterMergeOptions, BLOOM_FILTER_COLUMNS};

async fn set_events_sort_descending(ice: &IcebergContext) {
    let ident = TableIdent::new(ice.namespace().clone(), "events".to_string());
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let tx = tx
        .replace_sort_order()
        .desc("timestamp", NullOrder::Last)
        .apply(tx)
        .unwrap();
    tx.commit(ice.catalog().as_ref()).await.unwrap();
}

#[tokio::test]
async fn streaming_recluster_desc_table_outputs_descending() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path())
        .await
        .unwrap()
        .with_table_cache_ttl(std::time::Duration::ZERO);
    // Flip events to DESC BEFORE writing, so the writer sorts each file DESC.
    set_events_sort_descending(&ice).await;

    // Three interleaved-range files; with a DESC declared order each is written
    // high→low internally.
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let mk = |offs: &[i64]| -> Vec<Event> {
        offs.iter()
            .map(|&s| {
                let mut e = Event::now(format!("row-{s}"));
                e.timestamp = base + Duration::seconds(s);
                e
            })
            .collect()
    };
    ice.append_events(&mk(&[10, 50, 90])).await.unwrap();
    ice.append_events(&mk(&[20, 60])).await.unwrap();
    ice.append_events(&mk(&[5, 70])).await.unwrap();

    let ident = ice.events_table_ident().clone();
    let files = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(files.len(), 3, "three input files");

    let merge = ReclusterMergeOptions {
        force_streaming: Some(true),
        ..ReclusterMergeOptions::default()
    };
    let stats = ice
        .recluster_files_with(&ident, files, BLOOM_FILTER_COLUMNS, &merge)
        .await
        .unwrap();
    assert!(stats.files_added >= 1);
    assert_eq!(stats.rows, 7, "all rows carried through the merge");

    // Output must be globally DESC with a DESC timestamp footer.
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let merged = ice.live_data_files(&ident).await.unwrap();
    let mut ts: Vec<i64> = Vec::new();
    for df in &merged {
        let bytes = table
            .file_io()
            .new_input(df.file_path())
            .unwrap()
            .read()
            .await
            .unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        for rg in builder.metadata().row_groups() {
            let sc = rg.sorting_columns().expect("sorting columns present");
            assert_eq!(sc.len(), 1, "merge stamps only the timestamp sort column");
            assert!(sc[0].descending, "DESC table → descending footer");
        }
        for rb in builder.build().unwrap() {
            let rb = rb.unwrap();
            let idx = rb
                .schema()
                .index_of(siglake_core::nanos_source_column(
                    rb.schema().as_ref(),
                    "timestamp",
                ))
                .unwrap();
            let col = siglake_core::column_nanos(rb.column(idx)).unwrap();
            for r in 0..rb.num_rows() {
                ts.push(col.value(r));
            }
        }
    }
    let expected: Vec<i64> = [90, 70, 60, 50, 20, 10, 5]
        .iter()
        .map(|s| {
            (base + Duration::seconds(*s))
                .timestamp_nanos_opt()
                .unwrap()
        })
        .collect();
    assert_eq!(
        ts, expected,
        "DESC recluster must output globally descending, no loss/dupe"
    );
}
