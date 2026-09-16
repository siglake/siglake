//! WS-7 typed promotion: numeric/bool attribute keys auto-promote to TYPED
//! columns; post-backfill, `attr_get` comparisons rewrite to typed literals —
//! NUMERIC semantics (the decisive assertion: 99 < 100, where lexicographic
//! string compare says "99" >= "100") — and projections CAST back to VARCHAR.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::{IcebergContext, LevelPolicy, LeveledPassOptions, ReclusterPolicy};
use tower::util::ServiceExt;

async fn request_json(app: &Router, body: serde_json::Value) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    (status, body)
}

/// 40 rows status=99 + 60 rows status=500 (nested under "http" to exercise
/// dotted-key typing), plus a float, a bool, and a string key per row.
fn typed_events(offset: usize, n: usize) -> Vec<Event> {
    (0..n)
        .map(|i| {
            let idx = offset + i;
            let status = if idx % 100 < 40 { 99 } else { 500 };
            let hit = idx.is_multiple_of(2);
            Event::now(format!("typed row {idx}")).with_attributes(Some(format!(
                r#"{{"http":{{"status":{status}}},"req.duration":{}.5,"cache.hit":{hit},"cloud.provider":"gcp"}}"#,
                idx % 7
            )))
        })
        .collect()
}

#[tokio::test]
async fn typed_auto_promotion_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    // Two pre-promotion files.
    ice.append_events(&typed_events(0, 100)).await.unwrap();
    ice.append_events(&typed_events(100, 100)).await.unwrap();

    // Auto-promotion must TYPE the keys from the sampled JSON.
    let newly = ice.auto_promote_hot_keys(0.5, 16, 4, 4096).await.unwrap();
    let types: std::collections::HashMap<&str, siglake_core::PromotedType> =
        newly.iter().map(|c| (c.attr_key.as_str(), c.ty)).collect();
    assert_eq!(
        types.get("http.status"),
        Some(&siglake_core::PromotedType::Int64),
        "{newly:?}"
    );
    assert_eq!(
        types.get("req.duration"),
        Some(&siglake_core::PromotedType::Float64),
        "{newly:?}"
    );
    assert_eq!(
        types.get("cache.hit"),
        Some(&siglake_core::PromotedType::Boolean),
        "{newly:?}"
    );
    assert_eq!(
        types.get("cloud.provider"),
        Some(&siglake_core::PromotedType::Utf8),
        "{newly:?}"
    );

    // Post-promotion commit materializes typed columns; backfill flips the gate.
    ice.append_events(&typed_events(200, 100)).await.unwrap();
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
    assert!(ice.ensure_promotion_backfill_property().await.unwrap());

    let app = router(AppState::new(
        Arc::new(IcebergContext::open(&warehouse).await.unwrap()),
        AuthConfig::open(),
    ));

    // THE decisive test: numeric semantics. 300 rows total: 120 status=99,
    // 180 status=500. `>= '100'` numerically keeps only the 500s (180);
    // lexicographic ("99" >= "100") would return 300.
    let (status, body) = request_json(
        &app,
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'http.status') >= '100'"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["rows"][0]["n"].as_i64(),
        Some(180),
        "typed rewrite must give NUMERIC comparison semantics: {body}"
    );
    // http_logs gap class: the typed range count must ALSO be zero-scan —
    // the rewritten `>= Int64(100)` serves as an IntRange over the same
    // group-count footers the GROUP BY uses.
    assert_eq!(
        body["stats"]["rows_scanned"].as_u64(),
        Some(0),
        "typed range count must serve from footers: {body}"
    );

    // Typed equality + negation counts: zero-scan from the footers too.
    for (sql, expected) in [
        (
            "SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'http.status') = '500'",
            180,
        ),
        (
            "SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'http.status') != '500'",
            120,
        ),
    ] {
        let (status, body) = request_json(&app, serde_json::json!({ "query": sql })).await;
        assert_eq!(status, StatusCode::OK, "{sql}: {body}");
        assert_eq!(
            body["rows"][0]["n"].as_i64(),
            Some(expected),
            "{sql}: {body}"
        );
        assert_eq!(
            body["stats"]["rows_scanned"].as_u64(),
            Some(0),
            "typed dimensional count must serve from footers — {sql}: {body}"
        );
    }

    // Bool equality via typed literal.
    let (status, body) = request_json(
        &app,
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'cache.hit') = 'true'"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(150), "{body}");
    assert_eq!(
        body["stats"]["rows_scanned"].as_u64(),
        Some(0),
        "typed bool count must serve from footers: {body}"
    );

    // Projection/GROUP BY on a typed key: CAST back to VARCHAR, canonical
    // string rendering.
    let (status, body) = request_json(
        &app,
        serde_json::json!({
            "query": "SELECT attr_get(attributes, 'http.status') AS s, count(*) AS n \
                      FROM events GROUP BY s ORDER BY s"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let got: Vec<(String, i64)> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["s"].as_str().unwrap().to_string(),
                r["n"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![("500".into(), 180), ("99".into(), 120)],
        "typed GROUP BY must render canonical strings: {body}"
    );
    // Typed group-count footers: the CAST-wrapped GROUP BY serves from the
    // footer battery — Tier-1, zero data-file scan.
    assert_eq!(
        body["stats"]["rows_scanned"].as_u64(),
        Some(0),
        "typed GROUP BY must serve from footers: {}",
        body["stats"]
    );

    // The Utf8 key keeps the direct-substitution Tier-1 path.
    let (status, body) = request_json(
        &app,
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'cloud.provider') = 'gcp'"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(300), "{body}");
}
