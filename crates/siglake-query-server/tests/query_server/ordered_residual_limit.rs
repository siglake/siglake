//! An ordered source LIMIT must not run before an unsupported residual filter.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use chrono::{Duration, TimeZone, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::{IcebergContext, IcebergTuning};
use tower::util::ServiceExt;

const ROW_GROUP_FLOOR: usize = 131_072;
const ROWS_PER_DAY: usize = ROW_GROUP_FLOOR + 64;

async fn query(app: &Router, sql: &str) -> serde_json::Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({ "query": sql })).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

fn timestamps(body: &serde_json::Value) -> Vec<i64> {
    assert_eq!(body["row_count"].as_u64(), Some(10), "{body}");
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            row["value"]
                .as_str()
                .unwrap()
                .parse::<chrono::DateTime<Utc>>()
                .unwrap()
                .timestamp_micros()
        })
        .collect()
}

fn is_match(row: usize) -> bool {
    row >= 20 && row + 20 < ROWS_PER_DAY && row % 1_000 == 500
}

#[tokio::test]
async fn residual_attr_get_filter_precedes_ordered_limit_in_both_directions() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap()
            // The minimum target produces one full group and a tail group in
            // each day-partitioned file.
            .with_tuning(IcebergTuning {
                target_row_group_bytes: Some(1),
                ..Default::default()
            }),
    );
    let base = Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap();
    let mut events = Vec::with_capacity(2 * ROWS_PER_DAY);
    let mut matching = Vec::new();
    for day in 0..2i64 {
        for row in 0..ROWS_PER_DAY {
            let timestamp = base + Duration::days(day) + Duration::milliseconds(row as i64);
            let matches = is_match(row);
            let mut event = Event::now(format!("day-{day}-row-{row}"));
            event.timestamp = timestamp;
            event.host = if matches { "kernel" } else { "other" }.into();
            let cutoff = if matches {
                timestamp - Duration::milliseconds(1)
            } else {
                timestamp + Duration::milliseconds(1)
            };
            event.attributes = Some(format!(
                r#"{{"process.name":"{}","cutoff":"{}"}}"#,
                if matches { "kernel" } else { "user" },
                cutoff.to_rfc3339(),
            ));
            if matches {
                matching.push(timestamp.timestamp_micros());
            }
            events.push(event);
        }
    }
    ice.append_events(&events).await.unwrap();

    let files = ice.live_data_files(ice.events_table_ident()).await.unwrap();
    assert_eq!(files.len(), 2, "the fixture must span two day partitions");
    for file in &files {
        let path = file.file_path().trim_start_matches("file://");
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap()).unwrap();
        assert_eq!(
            reader.metadata().num_row_groups(),
            2,
            "each persisted file must span the ordered reader's row-group boundary"
        );
    }

    let app = router(AppState::new(ice, AuthConfig::open()));
    let attr = "attr_get(attributes, 'process.name') = 'kernel'";
    let asc = query(
        &app,
        &format!(
            "SELECT timestamp AS value FROM events WHERE {attr} ORDER BY timestamp ASC LIMIT 10"
        ),
    )
    .await;
    assert_eq!(timestamps(&asc), matching[..10]);

    let desc = query(
        &app,
        &format!(
            "SELECT timestamp AS value FROM events WHERE {attr} ORDER BY timestamp DESC LIMIT 10"
        ),
    )
    .await;
    assert_eq!(
        timestamps(&desc),
        matching.iter().rev().take(10).copied().collect::<Vec<_>>()
    );

    // A literal time range reaches the reader while the row-dependent cutoff
    // remains residual. The source must not stop at ten rows before applying
    // that residual in either direction.
    let mixed = "timestamp >= '2026-10-01T00:00:00Z' AND \
                 timestamp > CAST(attr_get(attributes, 'cutoff') AS TIMESTAMP)";
    let mixed_asc = query(
        &app,
        &format!(
            "SELECT timestamp AS value FROM events WHERE {mixed} \
             ORDER BY timestamp ASC LIMIT 10"
        ),
    )
    .await;
    assert_eq!(timestamps(&mixed_asc), matching[..10]);
    let mixed_desc = query(
        &app,
        &format!(
            "SELECT timestamp AS value FROM events WHERE {mixed} \
             ORDER BY timestamp DESC LIMIT 10"
        ),
    )
    .await;
    assert_eq!(
        timestamps(&mixed_desc),
        matching.iter().rev().take(10).copied().collect::<Vec<_>>()
    );

    // Controls: source-capped unfiltered scans and reader-pushed predicates
    // retain the same exact top-N contract in both directions.
    for direction in ["ASC", "DESC"] {
        let unfiltered = query(
            &app,
            &format!(
                "SELECT timestamp AS value FROM events ORDER BY timestamp {direction} LIMIT 10"
            ),
        )
        .await;
        let pushed = query(
            &app,
            &format!(
                "SELECT timestamp AS value FROM events WHERE host = 'kernel' \
                 ORDER BY timestamp {direction} LIMIT 10"
            ),
        )
        .await;
        let time_bound = query(
            &app,
            &format!(
                "SELECT timestamp AS value FROM events \
                 WHERE timestamp >= '2026-10-01T00:00:00Z' \
                 ORDER BY timestamp {direction} LIMIT 10"
            ),
        )
        .await;
        let got_unfiltered = timestamps(&unfiltered);
        assert!(
            got_unfiltered.windows(2).all(|w| if direction == "ASC" {
                w[0] < w[1]
            } else {
                w[0] > w[1]
            }),
            "unfiltered {direction} control is not ordered: {got_unfiltered:?}"
        );
        let expected_pushed = if direction == "ASC" {
            matching.iter().take(10).copied().collect::<Vec<_>>()
        } else {
            matching.iter().rev().take(10).copied().collect::<Vec<_>>()
        };
        assert_eq!(timestamps(&pushed), expected_pushed);
        assert_eq!(timestamps(&time_bound), got_unfiltered);
    }
}
