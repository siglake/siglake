//! Tier-2 reports `rows_scanned: 0` whether it read a footer per file or decoded
//! raw pages per file, and those differ by orders of magnitude. This pins the
//! counters that tell them apart.
//!
//! Motivation is concrete: on the 2026-09-01 round `count_by_status` was served
//! by Tier-2 at 85ms against 2.6ms for the same shape on a text column, and
//! nothing in a 723-metric scrape could say whether that was a healthy
//! footer-served fold over many files or a degenerate fallback. Separating the
//! two took a local bisect against a month-old commit. These counters make it a
//! read.

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use chrono::Utc;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::index_config::{DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode};
use siglake_storage::iceberg::IcebergContext;

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

/// Counter value for `name` carrying label `key=value`.
fn counter_with_label(snapshot: &SnapshotVec, name: &str, key: &str, value: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _, _, _)| {
            k.key().name() == name
                && k.key()
                    .labels()
                    .any(|l| l.key() == key && l.value() == value)
        })
        .map(|(_, _, _, v)| match v {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

fn field(name: &str, field_type: FieldType, required: bool) -> FieldMapping {
    FieldMapping {
        name: name.to_string(),
        field_type,
        required,
    }
}

/// `method` is a tag field, so it is a bloom column and lands in the per-file
/// group-count footer. `region` is deliberately NOT a tag field and not typed,
/// so `group_count_columns_for` never includes it and no file carries a footer
/// for it — which is what forces the raw-page fallback.
fn config() -> IndexConfig {
    IndexConfig {
        index_id: "tier2".to_string(),
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                field("timestamp", FieldType::Datetime, true),
                field(
                    "method",
                    FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    false,
                ),
                field(
                    "region",
                    FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    false,
                ),
            ],
            timestamp_field: "timestamp".to_string(),
            tag_fields: vec!["method".to_string()],
            default_search_fields: vec![],
        },
        retention: None,
        index_at_flush: None,
    }
}

/// `timestamp` is a microsecond `timestamptz`, so the fixture's unit is
/// microseconds throughout: one row per microsecond, batches 1e6 apart.
fn batch(config: &IndexConfig, base_us: i64, n: i64) -> RecordBatch {
    let schema = config.to_arrow_schema();
    let ts: Vec<Option<i64>> = (0..n).map(|i| Some(base_us + i)).collect();
    let methods: Vec<Option<&str>> = (0..n)
        .map(|i| {
            if i % 2 == 0 {
                Some("GET")
            } else {
                Some("POST")
            }
        })
        .collect();
    let regions: Vec<Option<&str>> = (0..n)
        .map(|i| {
            if i % 3 == 0 {
                Some("us-east")
            } else {
                Some("us-west")
            }
        })
        .collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("+00:00")),
            Arc::new(StringArray::from(methods)),
            Arc::new(StringArray::from(regions)),
            // Dynamic mapping mode appends the WS-7 residual `attributes` column.
            Arc::new(StringArray::from(vec![None::<&str>; n as usize])),
        ],
    )
    .unwrap()
}

const FILES: u64 = 3;

#[tokio::test]
async fn tier2_says_whether_it_read_footers_or_decoded_raw_pages() {
    // Starve Tier-1 so both queries below reach Tier-2: the base cap applies to
    // the side aggregate, while the per-file FOOTER cap is a compiled-in
    // constant, so the footers survive.
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            table_group_count_cardinality: Some(1),
            ..Default::default()
        });
    let config = config();
    ice.create_index(&config).await.unwrap();
    let ident = ice.index_table_ident(&config.index_id);

    let base = Utc::now().timestamp_micros();
    for b in 0..FILES as i64 {
        ice.append_to_table(
            &ident,
            batch(&config, base + b * 1_000_000, 60),
            &["method"],
        )
        .await
        .unwrap();
    }
    assert_eq!(
        ice.live_data_files(&ident).await.unwrap().len(),
        FILES as usize
    );

    // (a) a footer-backed column: every file served from its footer.
    let g = ice
        .grouped_counts_with_summary("tier2", "method", None, None)
        .await
        .unwrap()
        .expect("counts for method");
    assert_eq!(
        g.source_label(),
        "materialized",
        "the cap should have forced Tier-2; the counters below only describe Tier-2"
    );
    let mut rows = g.to_rows();
    rows.sort();
    assert_eq!(
        rows,
        vec![(Some("GET".into()), 90), (Some("POST".into()), 90)]
    );

    // `snapshot()` DRAINS, so each snapshot is the delta for the query just run
    // — which is what lets the two outcomes be attributed separately below.
    let s = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_with_label(
            &s,
            "siglake_group_count_tier2_files_total",
            "outcome",
            "footer"
        ),
        FILES,
        "every file carries a footer for a bloom column"
    );
    assert_eq!(
        counter_with_label(
            &s,
            "siglake_group_count_tier2_files_total",
            "outcome",
            "raw_page"
        ),
        0,
        "nothing should have fallen back yet"
    );
    assert_eq!(
        counter_with_label(
            &s,
            "siglake_group_count_tier2_calls_total",
            "outcome",
            "footer_only"
        ),
        1
    );

    // (b) a column no footer covers: every file decodes raw pages instead. Same
    // `rows_scanned: 0`, same `served_by`, orders of magnitude more work.
    let g = ice
        .grouped_counts_with_summary("tier2", "region", None, None)
        .await
        .unwrap()
        .expect("counts for region");
    assert_eq!(g.source_label(), "materialized");
    let mut rows = g.to_rows();
    rows.sort();
    assert_eq!(
        rows,
        vec![(Some("us-east".into()), 60), (Some("us-west".into()), 120)],
        "the fallback must still be exact"
    );

    let s = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_with_label(
            &s,
            "siglake_group_count_tier2_files_total",
            "outcome",
            "raw_page"
        ),
        FILES,
        "no footer covers `region`, so every file decodes raw pages"
    );
    assert_eq!(
        counter_with_label(
            &s,
            "siglake_group_count_tier2_calls_total",
            "outcome",
            "fallback_only"
        ),
        1,
        "the call-level label is what distinguishes a degenerate Tier-2 at a glance"
    );
    // This snapshot is the region query ALONE, and it recorded no footer reads —
    // so a degenerate Tier-2 cannot hide behind a healthy one in the same scrape,
    // which is the whole point.
    assert_eq!(
        counter_with_label(
            &s,
            "siglake_group_count_tier2_files_total",
            "outcome",
            "footer"
        ),
        0
    );

    // (c) the WINDOWED sibling has its own fallback — a file straddling the
    // window edge takes a window-restricted scan instead of its footer — and it
    // was equally unlabelled. Covering only the unwindowed half would leave
    // exactly the gap this work exists to close.
    //
    // This window fully contains files 0 and 1 and CUTS file 2 in half.
    let window = siglake_storage::iceberg::TimeBounds {
        start: Some(chrono::DateTime::from_timestamp_micros(base).unwrap()),
        end: Some(chrono::DateTime::from_timestamp_micros(base + 2 * 1_000_000 + 30).unwrap()),
    };
    let g = ice
        .grouped_counts_with_summary("tier2", "method", None, Some(window))
        .await
        .unwrap()
        .expect("windowed counts for method");
    let mut total = 0u64;
    g.for_each(|_, count| total = total.saturating_add(count));
    assert_eq!(total, 150, "60 + 60 + the 30 rows inside the window");

    let s = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_with_label(
            &s,
            "siglake_group_count_tier2_files_total",
            "outcome",
            "footer"
        ),
        2,
        "the two fully-contained files are served from their footers"
    );
    assert_eq!(
        counter_with_label(
            &s,
            "siglake_group_count_tier2_files_total",
            "outcome",
            "boundary_scan"
        ),
        1,
        "the straddling file must be reported as a scan, not as a footer read"
    );
    assert_eq!(
        counter_with_label(
            &s,
            "siglake_group_count_tier2_calls_total",
            "outcome",
            "mixed"
        ),
        1
    );
}
