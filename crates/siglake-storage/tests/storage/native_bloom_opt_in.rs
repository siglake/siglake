//! `SIGLAKE_PARQUET_NATIVE_BLOOMS=on` still restores Parquet-native blooms.
//!
//! They are default-OFF since 2026-08-06 because they prune nothing in a
//! time-sorted layout: for a bloom to help, the value must be ABSENT from the
//! row group, and at >=128Ki rows per group with a ~2,000-value column that
//! probability is ~1e-28. The cost was measured at 146 KB/row-group, 4.07% of
//! an ingest-written file.
//!
//! The write path is KEPT rather than deleted, because that reasoning is
//! contingent on the layout — a dimension-clustered corpus would make blooms
//! selective again. This test is what makes "we can turn it back on" a checked
//! claim instead of a hopeful comment.
//!
use chrono::{TimeZone, Utc};
use parquet::file::reader::FileReader;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

#[tokio::test]
async fn native_blooms_come_back_when_the_knob_is_on() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            native_blooms: Some(true),
            ..Default::default()
        });
    let events: Vec<Event> = (0..500)
        .map(|i| Event {
            timestamp: Utc.timestamp_opt(1_800_000_000 + i, 0).single().unwrap(),
            host: format!("host-{}", i % 7),
            source: "src".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: format!("event {i}"),
            attributes: None,
        })
        .collect();
    ice.append_events(&events).await.unwrap();

    let ident = ice.events_table_ident().clone();
    let files = ice.live_data_files(&ident).await.unwrap();
    assert!(!files.is_empty(), "fixture must write a data file");

    let mut dim_blooms = 0usize;
    let mut text_blooms = 0usize;
    for df in &files {
        let path = df.file_path().trim_start_matches("file://").to_string();
        let bytes = std::fs::read(&path).unwrap();
        let reader =
            parquet::file::reader::SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        let meta = reader.metadata();
        for rg in 0..meta.num_row_groups() {
            let rgm = meta.row_group(rg);
            for c in 0..rgm.num_columns() {
                let col = rgm.column(c);
                let has = col.bloom_filter_offset().is_some();
                match col.column_path().string().as_str() {
                    "host" | "source" | "sourcetype" | "index" if has => dim_blooms += 1,
                    // High-cardinality text never gets a native bloom —
                    // substring search uses the trigram path instead.
                    "raw" | "attributes" | "timestamp" if has => text_blooms += 1,
                    _ => {}
                }
            }
        }
    }

    assert!(
        dim_blooms > 0,
        "the knob must restore native blooms on the dimensional columns"
    );
    assert_eq!(
        text_blooms, 0,
        "high-cardinality text columns must never get a native bloom"
    );
}
