//! Task #3052: mixed-type keys, backfill convergence and query equivalence for
//! opt-in WS-7 attribute auto-promotion.
//!
//! The pass types a key from a bounded sample and then widens the schema for
//! good, so the two things that decide whether the feature is safe to leave
//! running are: it must refuse keys it cannot type soundly, and the answer to a
//! query on a promoted key must be the same at every point in the backfill —
//! before the promotion, between the declaration and the rewrite, and after the
//! gate flips.
//!
//! The threshold and column-ceiling off switches are in `siglake-storage`'s
//! `auto_promotion_bounds.rs` (they only need the table); the selection
//! arithmetic is unit-tested in `iceberg.rs`.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::{IcebergContext, LevelPolicy, LeveledPassOptions, ReclusterPolicy};
use tower::util::ServiceExt;

/// One pass over everything these fixtures write, so a verdict is about the
/// data and not about which files the sample reached.
const FILES: usize = 8;
const ROWS: usize = 4096;

async fn count(app: &Router, sql: &str) -> i64 {
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
    let status = response.status();
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(status, StatusCode::OK, "{sql}: {body}");
    body["rows"][0]["n"]
        .as_i64()
        .unwrap_or_else(|| panic!("{sql}: {body}"))
}

async fn app_for(warehouse: &std::path::Path) -> Router {
    router(AppState::new(
        Arc::new(IcebergContext::open(warehouse).await.unwrap()),
        AuthConfig::open(),
    ))
}

fn events(n: usize, attributes: impl Fn(usize) -> String) -> Vec<Event> {
    (0..n)
        .map(|i| Event::now(format!("row {i}")).with_attributes(Some(attributes(i))))
        .collect()
}

async fn schema_names(ice: &IcebergContext) -> Vec<String> {
    ice.catalog()
        .load_table(ice.events_table_ident())
        .await
        .unwrap()
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect()
}

/// A key whose sampled values disagree on type has no sound typed column —
/// `"200"` and `200` coerce differently, and an explicit JSON null is not a
/// scalar at all — so the pass must leave it in `attributes`, where `attr_get`
/// still answers on it in both spellings.
#[tokio::test]
async fn mixed_type_keys_stay_residual_and_queryable() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    // `clean` is a string in every row. `mixed` alternates string and number.
    // `nullable` is a string in half the rows and an explicit JSON null in the
    // rest. All three are in every row, so frequency is not what separates
    // them.
    ice.append_events(&events(200, |i| {
        let mixed = if i % 2 == 0 {
            format!(r#""{i}""#)
        } else {
            format!("{i}")
        };
        let nullable = if i % 2 == 0 { r#""set""# } else { "null" };
        format!(
            r#"{{"clean":"c{}","mixed":{mixed},"nullable":{nullable}}}"#,
            i % 4
        )
    }))
    .await
    .unwrap();

    let newly = ice
        .auto_promote_hot_keys(0.5, 16, FILES, ROWS)
        .await
        .unwrap();
    let keys: Vec<&str> = newly.iter().map(|c| c.attr_key.as_str()).collect();
    assert_eq!(keys, vec!["clean"], "{newly:?}");
    let names = schema_names(&ice).await;
    assert!(names.iter().any(|n| n == "clean"), "{names:?}");
    assert!(
        !names.iter().any(|n| n == "mixed" || n == "nullable"),
        "a key with no sound type must not reach the schema: {names:?}"
    );

    // Both residual keys still answer, from the JSON, in either spelling.
    let app = app_for(&warehouse).await;
    assert_eq!(
        count(
            &app,
            "SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'mixed') = '4'"
        )
        .await,
        1,
        "the numeric spelling of a mixed key"
    );
    assert_eq!(
        count(
            &app,
            "SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'mixed') = '3'"
        )
        .await,
        1,
        "the string spelling of a mixed key"
    );
    assert_eq!(
        count(
            &app,
            "SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'nullable') = 'set'"
        )
        .await,
        100
    );
    // And the promoted sibling on the same rows agrees with its own JSON.
    assert_eq!(
        count(
            &app,
            "SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'clean') = 'c2'"
        )
        .await,
        50
    );
}

/// Promotion, backfill and equivalence on one table: rows written before the
/// promotion carry the key only in their JSON, so until a rewrite covers them
/// the promoted column is short — while the `attr_get` answer is exact the
/// whole way through. After the rewrite the gate flips and both spellings
/// agree, per group and in total.
#[tokio::test]
async fn backfill_converges_and_both_spellings_agree() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let attrs = |i: usize| format!(r#"{{"k8s.namespace":"ns-{}"}}"#, i % 4);

    // Two pre-promotion files.
    ice.append_events(&events(100, attrs)).await.unwrap();
    ice.append_events(&events(100, attrs)).await.unwrap();
    let app = app_for(&warehouse).await;
    let pre_promotion = count(
        &app,
        "SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'k8s.namespace') = 'ns-1'",
    )
    .await;
    assert_eq!(pre_promotion, 50);

    let newly = ice
        .auto_promote_hot_keys(0.5, 16, FILES, ROWS)
        .await
        .unwrap();
    assert_eq!(newly.len(), 1, "{newly:?}");
    assert_eq!(newly[0].name, "k8s_namespace");
    // Nothing is backfilled yet, so the gate stays shut even though the
    // property is recorded.
    assert!(
        !ice.ensure_promotion_backfill_property().await.unwrap(),
        "the gate must not flip before a rewrite covers the old files"
    );

    // One post-promotion file, which the write path materializes directly.
    ice.append_events(&events(100, attrs)).await.unwrap();

    let app = app_for(&warehouse).await;
    // Mid-backfill the residual JSON is complete on every row, so `attr_get`
    // is already exact...
    assert_eq!(
        count(
            &app,
            "SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'k8s.namespace') = 'ns-1'"
        )
        .await,
        75,
        "attr_get must be exact at every point in the backfill"
    );
    // ...while the column itself is short by the two pre-promotion files.
    assert_eq!(
        count(
            &app,
            "SELECT count(*) AS n FROM events WHERE k8s_namespace = 'ns-1'"
        )
        .await,
        25,
        "the promoted column is populated only for post-promotion files"
    );

    // The backfill: a rewrite re-extracts the key from each old file's JSON.
    let ident = ice.events_table_ident().clone();
    ice.recluster_pass_leveled(
        &ident,
        &["host"],
        &LevelPolicy {
            trigger_files: 100,
            max_merge_gen: 0,
            ..Default::default()
        },
        ReclusterPolicy::default(),
        &LeveledPassOptions::default(),
    )
    .await
    .unwrap();
    assert!(
        ice.ensure_promotion_backfill_property().await.unwrap(),
        "every live file now carries the column, so the gate flips"
    );

    let app = app_for(&warehouse).await;
    for group in 0..4 {
        let json = count(
            &app,
            &format!(
                "SELECT count(*) AS n FROM events \
                 WHERE attr_get(attributes, 'k8s.namespace') = 'ns-{group}'"
            ),
        )
        .await;
        let column = count(
            &app,
            &format!("SELECT count(*) AS n FROM events WHERE k8s_namespace = 'ns-{group}'"),
        )
        .await;
        assert_eq!(
            json, column,
            "ns-{group}: attr_get {json} vs column {column}"
        );
        assert_eq!(json, 75, "ns-{group}");
    }
    assert_eq!(
        count(&app, "SELECT count(*) AS n FROM events").await,
        300,
        "the backfill rewrite must not lose or duplicate rows"
    );
    assert_eq!(
        count(
            &app,
            "SELECT count(*) AS n FROM events WHERE k8s_namespace IS NULL"
        )
        .await,
        0,
        "no row may be left unextracted after the gate flips"
    );
}
