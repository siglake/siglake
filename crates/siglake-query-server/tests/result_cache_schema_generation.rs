//! An additive schema migration must not be replayed around: the SQL result
//! cache separates entries across it (#2494).
//!
//! THE DEFECT THIS PINS. `IcebergContext::migrate_table_schema_additive`
//! commits an `UpdateSchemaAction` and NO data snapshot, so the served column
//! set widens while the snapshot id stands still. The table cache is
//! invalidated and its provider rebuilt (it keys on the schema id too), and the
//! provider serves `current_schema()` — so the query answer is wide
//! immediately. The result-cache key, however, carried only the snapshot, and a
//! warm entry therefore kept replaying the pre-migration body: a filtered
//! `SELECT *` short one column, until the next data commit moved the snapshot.
//! On a quiet table that is unbounded, and the standing invariant forbids
//! curing it with a TTL.
//!
//! The migration here runs with NO append after it, which is what makes the
//! test the defect and not a snapshot-invalidation test in disguise: the
//! snapshot id is read before and after and asserted equal, so the only thing
//! that moved is the schema.
//!
//! Read through the real router with the real `outcome` counter, so a green run
//! cannot be one where the cache was never consulted, and repeats inside each
//! generation must still HIT — this separates generations, it does not disable
//! caching.
//!
//! ONE test function in its own binary, on purpose: the result cache and the
//! metrics recorder are both process-wide, so a sibling test's queries would
//! read as this test's cache traffic.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

/// A column the current binary does not declare, standing in for the next
/// additive bump — the same device `siglake-storage`'s `schema_rollback.rs`
/// uses, and the only way to exercise a widen without a second image.
const FUTURE_COLUMN: &str = "severity_number";

/// `events_schema()` plus one nullable column: what a future binary declares
/// and `migrate-schema` additively adds.
fn widened_schema() -> SchemaRef {
    let base = siglake_core::events_schema();
    let mut fields: Vec<Field> = base.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(FUTURE_COLUMN, DataType::Int64, true));
    Arc::new(Schema::new(fields))
}

const HOST: &str = "schema-generation-host";

/// Cache-outcome counts SINCE THE LAST CALL (`snapshot()` drains), keyed by the
/// `outcome` label.
fn outcomes(snapshotter: &Snapshotter) -> HashMap<String, u64> {
    let mut by_outcome = HashMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        if key.key().name() != "siglake_query_sql_result_cache_requests_total" {
            continue;
        }
        let Some(outcome) = key
            .key()
            .labels()
            .find(|l| l.key() == "outcome")
            .map(|l| l.value().to_string())
        else {
            continue;
        };
        if let DebugValue::Counter(c) = value {
            *by_outcome.entry(outcome).or_insert(0) += c;
        }
    }
    by_outcome
}

async fn post_sql(app: &Router, body: serde_json::Value) -> serde_json::Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql/local")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{body}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}

/// The rows of a records response, as objects.
fn rows(body: &serde_json::Value) -> Vec<serde_json::Map<String, serde_json::Value>> {
    body["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows in {body}"))
        .iter()
        .map(|row| row.as_object().expect("an object row").clone())
        .collect()
}

/// The column list a records response declares.
///
/// This, not the row objects, is where the served column SET is observable: the
/// records renderer elides null fields from a row (`attributes` is absent from
/// every row of this fixture for the same reason), so a widened column reads as
/// "named in `columns`, absent from every row" — which is exactly "present and
/// null for rows written before the widen".
fn columns(body: &serde_json::Value) -> Vec<String> {
    body["columns"]
        .as_array()
        .unwrap_or_else(|| panic!("no columns in {body}"))
        .iter()
        .map(|c| c.as_str().expect("a column name").to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_additive_migration_is_never_answered_from_the_narrow_generation() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let events: Vec<Event> = (0..12i64)
        .map(|i| {
            let mut e = Event::now(format!("row {i}"));
            e.timestamp = base + ChronoDuration::milliseconds(i * 100);
            e.host = HOST.to_string();
            e
        })
        .collect();
    ice.append_events(&events).await.unwrap();
    let app = router(AppState::new(ice.clone(), AuthConfig::open()));

    // A filtered `SELECT *` — cacheable (it has a predicate) and, unlike a
    // count, its BODY names the columns, which is what the migration changes.
    let request = serde_json::json!({
        "query": format!("SELECT * FROM events WHERE host = '{HOST}'"),
        "limits": { "max_rows_returned": 1_000 },
    });

    // ===== The narrow generation ===========================================
    let _ = outcomes(&snapshotter);
    let narrow = post_sql(&app, request.clone()).await;
    assert_eq!(rows(&narrow).len(), 12, "{narrow}");
    assert!(
        !columns(&narrow).contains(&FUTURE_COLUMN.to_string()),
        "the precondition failed: the table already carries {FUTURE_COLUMN}, \
         so the migration below cannot widen anything: {narrow}"
    );
    let cold = outcomes(&snapshotter);
    assert_eq!(cold.get("miss").copied(), Some(1), "{cold:?}");
    assert_eq!(
        cold.get("insert").copied(),
        Some(1),
        "the body must be STORED, or the replay this test forecloses could not \
         happen: {cold:?}"
    );

    // A repeat inside the generation is a real hit on the real entry.
    let narrow_again = post_sql(&app, request.clone()).await;
    assert_eq!(narrow_again["rows"], narrow["rows"]);
    let warm = outcomes(&snapshotter);
    assert_eq!(
        warm.get("hit").copied(),
        Some(1),
        "an unchanged generation must still be served from the cache: {warm:?}"
    );

    // ===== The widen =======================================================
    let snapshot_before = ice.current_table_snapshot_id("events").await.unwrap();
    assert!(snapshot_before.is_some(), "the table has data");
    let added = ice
        .migrate_table_schema_additive(ice.events_table_ident(), widened_schema().as_ref())
        .await
        .unwrap();
    assert_eq!(added, 1, "the migration must add exactly {FUTURE_COLUMN}");
    // THE POINT OF THE TEST: no data snapshot was committed, so snapshot-only
    // identity cannot tell the two generations apart.
    assert_eq!(
        ice.current_table_snapshot_id("events").await.unwrap(),
        snapshot_before,
        "an additive migration must not move the snapshot; if it does, this \
         test is no longer about the defect it was written for"
    );

    // ===== The wide generation =============================================
    let _ = outcomes(&snapshotter);
    let wide = post_sql(&app, request.clone()).await;
    let wide_rows = rows(&wide);
    assert_eq!(wide_rows.len(), 12, "{wide}");
    assert!(
        columns(&wide).contains(&FUTURE_COLUMN.to_string()),
        "the post-migration answer must carry {FUTURE_COLUMN}, not the cached \
         narrow body: {wide}"
    );
    for row in &wide_rows {
        assert!(
            row.get(FUTURE_COLUMN)
                .unwrap_or(&serde_json::Value::Null)
                .is_null(),
            "a row written before the widen must read null in {FUTURE_COLUMN}, \
             never a value: {wide}"
        );
    }
    let after = outcomes(&snapshotter);
    assert_eq!(
        after.get("hit").copied().unwrap_or(0),
        0,
        "the post-migration request was served the pre-migration entry: {after:?}"
    );
    assert_eq!(
        after.get("miss").copied(),
        Some(1),
        "the post-migration request must reach the query: {after:?}"
    );

    // The wide generation caches on its own key, and the narrow entry is simply
    // unreachable — no request derives its key any more.
    let wide_again = post_sql(&app, request.clone()).await;
    assert_eq!(wide_again["rows"], wide["rows"]);
    let wide_warm = outcomes(&snapshotter);
    assert_eq!(
        wide_warm.get("hit").copied(),
        Some(1),
        "a repeat inside the wide generation must hit: {wide_warm:?}"
    );
}
