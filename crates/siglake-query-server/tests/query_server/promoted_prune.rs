//! WS-7 prune spec e2e: `attr_get(attributes, key) = value` predicates prune
//! row groups via the promoted column's statistics — and NEVER prune files
//! written before the promotion was declared (their JSON still carries the
//! key). The query side needs no CLI state: the key→column map rides the
//! `siglake.promoted.v1` table property written by `ensure_promoted_columns`.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use serde_json::Value;
use tower::util::ServiceExt;

use siglake_core::{Event, PromotedColumn, PromotedType};
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::{IcebergContext, IcebergTuning};

async fn request_json(app: &Router, path: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(path)
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

fn pod_events(pod: &str, n: usize) -> Vec<Event> {
    (0..n)
        .map(|i| {
            Event::now(format!("row {i} on {pod}"))
                .with_attributes(Some(format!(r#"{{"k8s.pod":"{pod}"}}"#)))
        })
        .collect()
}

fn promotion() -> Vec<PromotedColumn> {
    vec![PromotedColumn {
        attr_key: "k8s.pod".into(),
        name: "k8s_pod".into(),
        ty: PromotedType::Utf8,
    }]
}

/// Promotion declared from day 0: a one-file equality prunes the other
/// files' row groups from the promoted column's min/max stats, and the
/// response's scan detail attributes the drop.
#[tokio::test]
async fn attr_get_equality_prunes_row_groups_via_promoted_stats() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_promoted_columns(promotion());
    ice.ensure_promoted_columns().await.unwrap();
    for i in 0..4 {
        ice.append_events(&pod_events(&format!("pod-{i}"), 25))
            .await
            .unwrap();
    }

    // The SERVER context has no promotion config — the property carries it.
    let server_ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let app = router(AppState::new(server_ice, AuthConfig::open()));
    let (status, body) = request_json(
        &app,
        "/api/v1/sql",
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'k8s.pod') = 'pod-3'"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(25), "{body}");
    let scan = body["stats"]["scan"]
        .as_object()
        .unwrap_or_else(|| panic!("scanning query must carry stats.scan: {body}"));
    assert_eq!(scan["files_planned"].as_u64(), Some(4), "{body}");
    assert_eq!(
        scan["row_groups_pruned_stats"].as_u64(),
        Some(3),
        "three files' stats exclude pod-3: {body}"
    );
    assert_eq!(scan["row_groups_read"].as_u64(), Some(1), "{body}");
    assert_eq!(
        body["stats"]["rows_scanned"].as_u64(),
        Some(25),
        "only the match decodes: {body}"
    );
}

/// Soundness: files written BEFORE the promotion was declared carry the key
/// only in their `attributes` JSON — they must never be pruned, while the
/// post-promotion file still is.
#[tokio::test]
async fn pre_promotion_files_are_never_pruned() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");

    // Two files ingested with NO promotion declared.
    let legacy = IcebergContext::open(&warehouse).await.unwrap();
    legacy
        .append_events(&pod_events("legacy-a", 25))
        .await
        .unwrap();
    legacy
        .append_events(&pod_events("legacy-b", 25))
        .await
        .unwrap();

    // Promotion declared later; one post-promotion file.
    let promoted = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_promoted_columns(promotion());
    promoted.ensure_promoted_columns().await.unwrap();
    promoted
        .append_events(&pod_events("new-c", 25))
        .await
        .unwrap();

    let server_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app = router(AppState::new(server_ice, AuthConfig::open()));
    let (status, body) = request_json(
        &app,
        "/api/v1/sql",
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'k8s.pod') = 'legacy-a'"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["rows"][0]["n"].as_i64(),
        Some(25),
        "pre-promotion rows must survive pruning: {body}"
    );
    let scan = body["stats"]["scan"]
        .as_object()
        .unwrap_or_else(|| panic!("scanning query must carry stats.scan: {body}"));
    assert_eq!(
        scan["row_groups_pruned_stats"].as_u64(),
        Some(1),
        "only the post-promotion file prunes: {body}"
    );
    assert_eq!(
        scan["row_groups_read"].as_u64(),
        Some(2),
        "both legacy files read (no column, no pruning): {body}"
    );
}

/// WS-7 rewrite: BEFORE backfill completes, `attr_get` aggregations scan
/// (correct, slow); AFTER the compactor's backfill flips the completion
/// property, the same SQL rewrites to the promoted column and the Tier-1
/// group-count fast path serves it — visible as a zero-scan response.
#[tokio::test]
async fn attr_get_rewrite_activates_after_backfill() {
    use siglake_storage::iceberg::{LevelPolicy, LeveledPassOptions, ReclusterPolicy};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");

    // Pre-promotion files: key only in the residual JSON.
    let legacy = IcebergContext::open(&warehouse).await.unwrap();
    legacy
        .append_events(&pod_events("pod-a", 25))
        .await
        .unwrap();
    legacy
        .append_events(&pod_events("pod-b", 25))
        .await
        .unwrap();

    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_promoted_columns(promotion());
    ice.ensure_promoted_columns().await.unwrap();

    // ORDER BY the group column: the Sort root used to defeat the Tier-1
    // group-count detection; the GroupCountSort::Group arm serves it now.
    let sql = "SELECT attr_get(attributes, 'k8s.pod') AS pod, count(*) AS n \
               FROM events GROUP BY pod ORDER BY pod";

    // Phase 1: gate closed (pre-promotion files live) — the query scans and
    // is CORRECT via the JSON path.
    {
        let server_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
        let app = router(AppState::new(server_ice, AuthConfig::open()));
        let (status, body) =
            request_json(&app, "/api/v1/sql", serde_json::json!({ "query": sql })).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let mut got: Vec<(String, i64)> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| (r["pod"].as_str().unwrap().into(), r["n"].as_i64().unwrap()))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![("pod-a".into(), 25), ("pod-b".into(), 25)],
            "{body}"
        );
        assert!(
            body["stats"]["scan"].is_object(),
            "pre-backfill attr_get must SCAN (gate closed): {body}"
        );
    }

    // Backfill: rewrite the pre-promotion files, flip the property.
    let ident = ice.events_table_ident().clone();
    ice.recluster_pass_leveled(
        &ident,
        &["host", "k8s_pod"],
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

    // Phase 2: gate open — same SQL rewrites to the promoted column and the
    // Tier-1 group-count fast path serves it with ZERO data-file scan.
    {
        let server_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
        let app = router(AppState::new(server_ice, AuthConfig::open()));
        let (status, body) =
            request_json(&app, "/api/v1/sql", serde_json::json!({ "query": sql })).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let mut got: Vec<(String, i64)> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| (r["pod"].as_str().unwrap().into(), r["n"].as_i64().unwrap()))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![("pod-a".into(), 25), ("pod-b".into(), 25)],
            "{body}"
        );
        assert!(
            body["stats"]["scan"].is_null(),
            "post-backfill attr_get must be a Tier-1 zero-scan answer: {body}"
        );
        assert_eq!(
            body["stats"]["rows_scanned"].as_u64(),
            Some(0),
            "zero rows scanned: {body}"
        );
    }
}

fn object_promotion() -> Vec<PromotedColumn> {
    vec![PromotedColumn {
        attr_key: "aws.cloudwatch".into(),
        name: "aws_cloudwatch".into(),
        ty: PromotedType::Utf8,
    }]
}

fn object_events() -> Vec<Event> {
    [
        (
            "object",
            r#"{"aws.cloudwatch":{"log_group":"group-a","log_stream":"stream-a"}}"#,
        ),
        (
            "array",
            r#"{"aws.cloudwatch":[{"log_stream":"array-stream"},2]}"#,
        ),
    ]
    .into_iter()
    .map(|(raw, attributes)| Event::now(raw).with_attributes(Some(attributes.to_string())))
    .collect()
}

async fn nested_object_answer(app: &Router, sql: &str) -> Value {
    let (status, body) = request_json(
        app,
        "/api/v1/sql",
        serde_json::json!({ "query": sql, "default_order": false }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["rows"].clone()
}

async fn nested_object_plan(app: &Router, sql: &str) -> String {
    nested_object_answer(app, &format!("EXPLAIN {sql}"))
        .await
        .to_string()
}

/// #6782: production SQL rewriting substitutes a qualified, object-valued
/// promotion only for the inner call of a nested `attr_get`. The unchanged
/// query must retain the outer call and the same answers as an unpromoted
/// table. Keeping the pre-backfill plan in the fixture proves the result is
/// not merely the JSON fallback succeeding on both sides.
#[tokio::test]
async fn nested_attr_get_rewrites_its_object_valued_inner_call() {
    const SQL: &str = "SELECT raw, \
        attr_get(attr_get(attributes, 'aws.cloudwatch'), 'log_stream') AS log_stream \
        FROM events ORDER BY raw";

    let tmp = tempfile::tempdir().unwrap();
    let promoted_warehouse = tmp.path().join("promoted");
    let promoted = IcebergContext::open(&promoted_warehouse)
        .await
        .unwrap()
        .with_promoted_columns(object_promotion());
    promoted.ensure_promoted_columns().await.unwrap();
    promoted.append_events(&object_events()).await.unwrap();

    let control_warehouse = tmp.path().join("control");
    let control = IcebergContext::open(&control_warehouse).await.unwrap();
    control.append_events(&object_events()).await.unwrap();

    let promoted_app = router(AppState::new(
        Arc::new(IcebergContext::open(&promoted_warehouse).await.unwrap()),
        AuthConfig::open(),
    ));
    let control_app = router(AppState::new(
        Arc::new(IcebergContext::open(&control_warehouse).await.unwrap()),
        AuthConfig::open(),
    ));

    // The promoted property exists, but the backfill/extraction-version gate
    // is still closed. The original nested expression must remain in the plan.
    assert!(promoted.events_attr_rewrite_map().await.unwrap().is_none());
    let closed_plan = nested_object_plan(&promoted_app, SQL).await;
    assert!(
        closed_plan.contains("attr_get(attr_get(attributes"),
        "gate-closed plan must retain the nested JSON walk:\n{closed_plan}"
    );
    let control_rows = nested_object_answer(&control_app, SQL).await;
    assert_eq!(
        nested_object_answer(&promoted_app, SQL).await,
        control_rows,
        "the gate-closed promoted table must use the JSON fallback"
    );

    assert!(promoted.ensure_promotion_backfill_property().await.unwrap());
    assert!(promoted.events_attr_rewrite_map().await.unwrap().is_some());
    let promoted_app = router(AppState::new(
        Arc::new(IcebergContext::open(&promoted_warehouse).await.unwrap()),
        AuthConfig::open(),
    ));

    let open_plan = nested_object_plan(&promoted_app, SQL).await;
    assert!(
        open_plan.contains("attr_get(aws_cloudwatch"),
        "outer attr_get must remain over the promoted column:\n{open_plan}"
    );
    assert!(
        !open_plan.contains("attr_get(attributes"),
        "inner attr_get must be absent after substitution:\n{open_plan}"
    );
    let promoted_rows = nested_object_answer(&promoted_app, SQL).await;
    assert_eq!(promoted_rows, control_rows);
    assert_eq!(
        promoted_rows,
        serde_json::json!([
            { "raw": "array" },
            { "raw": "object", "log_stream": "stream-a" }
        ]),
        "the fixture must exercise both array and object promoted values"
    );
}

/// The full autonomous loop, zero configuration: hot attribute keys promote
/// themselves (sampled from live files), the property-driven write path
/// materializes them on subsequent commits, backfill rewrites the old files,
/// the completion property flips — and `attr_get` aggregations on the key
/// serve at Tier-1 with zero data-file scan. No CLI flags anywhere.
#[tokio::test]
async fn auto_promotion_end_to_end_reaches_tier1() {
    use siglake_storage::iceberg::{LevelPolicy, LeveledPassOptions, ReclusterPolicy};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");

    // No promotions declared, ever. Two files with a hot key.
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    ice.append_events(&pod_events("pod-a", 25)).await.unwrap();
    ice.append_events(&pod_events("pod-b", 25)).await.unwrap();

    // Auto-promotion samples the files and promotes `k8s.pod` (present in
    // 100% of rows ≥ the 50% bar).
    let newly = ice.auto_promote_hot_keys(0.5, 16, 4, 4096).await.unwrap();
    assert_eq!(newly.len(), 1, "hot key must promote: {newly:?}");
    assert_eq!(newly[0].attr_key, "k8s.pod");
    assert_eq!(newly[0].name, "k8s_pod");
    // Idempotent: a second sampling run promotes nothing new.
    assert!(ice
        .auto_promote_hot_keys(0.5, 16, 4, 4096)
        .await
        .unwrap()
        .is_empty());

    // A post-promotion commit materializes the column WITHOUT any process
    // restart (property-driven write path).
    ice.append_events(&pod_events("pod-c", 25)).await.unwrap();

    // Backfill the two pre-promotion files; the completion property flips.
    let ident = ice.events_table_ident().clone();
    ice.recluster_pass_leveled(
        &ident,
        &["host", "k8s_pod"],
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

    // Tier-1 payoff through the whole stack.
    let server_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app = router(AppState::new(server_ice, AuthConfig::open()));
    let (status, body) = request_json(
        &app,
        "/api/v1/sql",
        serde_json::json!({
            "query": "SELECT attr_get(attributes, 'k8s.pod') AS pod, count(*) AS n \
                      FROM events GROUP BY pod ORDER BY pod"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let got: Vec<(String, i64)> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["pod"].as_str().unwrap().into(), r["n"].as_i64().unwrap()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("pod-a".into(), 25),
            ("pod-b".into(), 25),
            ("pod-c".into(), 25)
        ],
        "{body}"
    );
    assert!(
        body["stats"]["scan"].is_null(),
        "auto-promoted key must serve at Tier-1 zero-scan: {body}"
    );
}

/// Per-index promotion: the same autonomous loop on a USER INDEX — sample,
/// promote (property + additive widening on the index's own schema),
/// materialize on the next commit, backfill, gate flip, and the
/// attr_get→column rewrite serves the index query at Tier-1.
#[tokio::test]
async fn auto_promotion_loop_works_on_a_user_index() {
    use siglake_core::index_config::IndexConfig;
    use siglake_storage::iceberg::{LevelPolicy, LeveledPassOptions, ReclusterPolicy};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let config = IndexConfig {
        index_id: "svc-logs".into(),
        ..IndexConfig::builtin_events()
    };
    ice.create_index(&config).await.unwrap();
    let ident = ice.index_table_ident("svc-logs");

    // Two pre-promotion files on the index, hot key in the residual JSON.
    for tag in ["pod-a", "pod-b"] {
        let batch = siglake_core::events_to_record_batch(&pod_events(tag, 25)).unwrap();
        let mapped = siglake_core::mapping::map_carrier_batch(&batch, &config).unwrap();
        ice.append_to_table(&ident, mapped, &[]).await.unwrap();
    }

    // Auto-promotion over ALL tables (events has no rows; the index has the
    // hot key).
    let newly = ice.auto_promote_hot_keys(0.5, 16, 4, 4096).await.unwrap();
    assert_eq!(newly.len(), 1, "index hot key must promote: {newly:?}");
    assert_eq!(newly[0].attr_key, "k8s.pod");

    // Post-promotion commit materializes on the index (property-driven).
    let batch = siglake_core::events_to_record_batch(&pod_events("pod-c", 25)).unwrap();
    let mapped = siglake_core::mapping::map_carrier_batch(&batch, &config).unwrap();
    ice.append_to_table(&ident, mapped, &[]).await.unwrap();

    // Backfill the index's pre-promotion files; flip its gate.
    ice.recluster_pass_leveled(
        &ident,
        &["host", "k8s_pod"],
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

    // Tier-1 payoff on the INDEX query.
    let server_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app = router(AppState::new(server_ice, AuthConfig::open()));
    let (status, body) = request_json(
        &app,
        "/api/v1/sql",
        serde_json::json!({
            "query": "SELECT attr_get(attributes, 'k8s.pod') AS pod, count(*) AS n \
                      FROM \"svc-logs\" GROUP BY pod ORDER BY pod"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let got: Vec<(String, i64)> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["pod"].as_str().unwrap().into(), r["n"].as_i64().unwrap()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("pod-a".into(), 25),
            ("pod-b".into(), 25),
            ("pod-c".into(), 25)
        ],
        "{body}"
    );
    assert!(
        body["stats"]["scan"].is_null(),
        "index attr_get must serve at Tier-1 zero-scan: {body}"
    );
}

/// OTel-shaped residuals hold NESTED objects; their string leaves promote by
/// dotted key, extraction walks the path, and attr_get answers the same
/// dotted key both before (JSON walk) and after (promoted column) backfill.
#[tokio::test]
async fn nested_attribute_leaves_promote_by_dotted_key() {
    use chrono::Duration;
    use siglake_storage::iceberg::{LevelPolicy, LeveledPassOptions, ReclusterPolicy};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    for (i, provider) in ["aws", "gcp"].into_iter().enumerate() {
        let events: Vec<Event> = (0..25)
            .map(|j| {
                let mut e = Event::now(format!("req {j}")).with_attributes(Some(format!(
                    r#"{{"cloud":{{"provider":"{provider}","account_id":"a{i}"}},"http":{{"status_code":200}}}}"#
                )));
                e.timestamp = chrono::Utc::now() + Duration::seconds((i as i64) * 100 + j);
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }

    let newly = ice.auto_promote_hot_keys(0.5, 16, 4, 4096).await.unwrap();
    let keys: Vec<&str> = newly.iter().map(|c| c.attr_key.as_str()).collect();
    assert!(
        keys.contains(&"cloud.provider") && keys.contains(&"cloud.account_id"),
        "string leaves must promote by dotted key: {keys:?}"
    );
    assert!(
        newly
            .iter()
            .any(|c| c.attr_key == "http.status_code" && c.ty == siglake_core::PromotedType::Int64),
        "numeric leaves promote TYPED (Int64) since typed promotion: {newly:?}"
    );
    assert!(
        newly.iter().any(|c| c.name == "cloud_provider"),
        "dotted keys sanitize to identifiers: {newly:?}"
    );

    let ident = ice.events_table_ident().clone();
    ice.recluster_pass_leveled(
        &ident,
        &["host", "cloud_provider"],
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

    let server_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app = router(AppState::new(server_ice, AuthConfig::open()));
    let (status, body) = request_json(
        &app,
        "/api/v1/sql",
        serde_json::json!({
            "query": "SELECT attr_get(attributes, 'cloud.provider') AS p, count(*) AS n \
                      FROM events GROUP BY p ORDER BY p"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let got: Vec<(String, i64)> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["p"].as_str().unwrap().into(), r["n"].as_i64().unwrap()))
        .collect();
    assert_eq!(got, vec![("aws".into(), 25), ("gcp".into(), 25)], "{body}");
    assert!(
        body["stats"]["scan"].is_null(),
        "dotted-key attr_get must serve at Tier-1 post-backfill: {body}"
    );
}

/// The 07-16 round's failing shape: `count(*) WHERE attr_get(...) = 'v'` on a
/// low-cardinality value — nothing prunable, full-scanned into the breaker.
/// Post-backfill the rewrite turns it into `count(*) WHERE col = 'v'`, and
/// the dimensional-count fast path answers it from the group-count footers:
/// exact, zero data-file scan.
#[tokio::test]
async fn equality_count_on_attribute_serves_from_footers() {
    use siglake_storage::iceberg::{LevelPolicy, LeveledPassOptions, ReclusterPolicy};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    ice.append_events(&pod_events("pod-a", 30)).await.unwrap();
    ice.append_events(&pod_events("pod-b", 20)).await.unwrap();
    assert_eq!(
        ice.auto_promote_hot_keys(0.5, 16, 4, 4096)
            .await
            .unwrap()
            .len(),
        1
    );
    let ident = ice.events_table_ident().clone();
    ice.recluster_pass_leveled(
        &ident,
        &["host", "k8s_pod"],
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

    let server_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app = router(AppState::new(server_ice, AuthConfig::open()));
    for (sql, expect) in [
        ("SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'k8s.pod') = 'pod-a'", 30),
        ("SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'k8s.pod') IN ('pod-a', 'pod-b')", 50),
        ("SELECT count(*) AS n FROM events WHERE attr_get(attributes, 'k8s.pod') != 'pod-a'", 20),
    ] {
        let (status, body) =
            request_json(&app, "/api/v1/sql", serde_json::json!({ "query": sql })).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["rows"][0]["n"].as_i64(), Some(expect), "{sql}: {body}");
        assert!(
            body["stats"]["scan"].is_null(),
            "must serve from footers, zero scan: {sql}: {body}"
        );
        assert_eq!(body["stats"]["rows_scanned"].as_u64(), Some(0), "{sql}: {body}");
    }
}

// ---------------------------------------------------------------------------
// #6769: promotion and `attr_get` must extract the same value, and promoted
// values written under an older rule must not feed the rewrite or the prune.
// ---------------------------------------------------------------------------

fn dotted_promotion() -> Vec<PromotedColumn> {
    vec![PromotedColumn {
        attr_key: "a.b.c".into(),
        name: "a_b_c".into(),
        ty: PromotedType::Utf8,
    }]
}

fn attr_events(attributes: &str, n: usize) -> Vec<Event> {
    (0..n)
        .map(|i| Event::now(format!("row {i}")).with_attributes(Some(attributes.to_string())))
        .collect()
}

/// Every answer the divergence would have changed, as one readout: the
/// promoted column against the UDF row by row, the filtered count, and the
/// grouped counts.
async fn dotted_key_answers(app: &Router) -> (i64, i64, Vec<(String, i64)>) {
    let (status, body) = request_json(
        app,
        "/api/v1/sql",
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events \
                      WHERE a_b_c IS DISTINCT FROM attr_get(attributes, 'a.b.c')"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let disagreeing = body["rows"][0]["n"].as_i64().unwrap();

    let (status, body) = request_json(
        app,
        "/api/v1/sql",
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events \
                      WHERE attr_get(attributes, 'a.b.c') = 'inner-literal'"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let filtered = body["rows"][0]["n"].as_i64().unwrap();

    let (status, body) = request_json(
        app,
        "/api/v1/sql",
        serde_json::json!({
            "query": "SELECT attr_get(attributes, 'a.b.c') AS v, count(*) AS n \
                      FROM events GROUP BY v ORDER BY v"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let grouped = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["v"].as_str().unwrap_or("<null>").to_string(),
                r["n"].as_i64().unwrap(),
            )
        })
        .collect();
    (disagreeing, filtered, grouped)
}

/// #6769: a key `a.b.c` whose object under `a` carries a literal `"b.c"` key.
/// `attr_get` retries the literal at every level and resolves it; promotion's
/// strict segment walk wrote NULL, so the rewrite substituted a NULL column
/// and the prune hint dropped the row group. Promoted column and UDF must
/// agree, and the answers must not change when backfill opens the rewrite.
#[tokio::test]
async fn inner_literal_dotted_key_answers_match_before_and_after_backfill() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_promoted_columns(dotted_promotion());
    ice.ensure_promoted_columns().await.unwrap();
    // One file per shape: the inner literal dotted key, the nested walk it
    // must not break, and rows without the key at all.
    ice.append_events(&attr_events(r#"{"a":{"b.c":"inner-literal"}}"#, 10))
        .await
        .unwrap();
    ice.append_events(&attr_events(r#"{"a":{"b":{"c":"nested"}}}"#, 10))
        .await
        .unwrap();
    ice.append_events(&attr_events(r#"{"a":{"x":1}}"#, 5))
        .await
        .unwrap();

    let expected_groups = vec![
        ("inner-literal".to_string(), 10),
        ("nested".to_string(), 10),
        ("<null>".to_string(), 5),
    ];

    // Before backfill: the rewrite is closed, the prune hints are open (the
    // promotions were declared under the current rule), and the UDF reads the
    // residual JSON. Both extractors must land on the same value.
    let server_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app = router(AppState::new(server_ice, AuthConfig::open()));
    let before = dotted_key_answers(&app).await;
    assert_eq!(before.0, 0, "promoted column disagrees with attr_get");
    assert_eq!(before.1, 10, "filtered count on the inner literal key");
    assert_eq!(before.2, expected_groups);

    // After backfill: `attr_get` rewrites to the promoted column. Same answers.
    assert!(ice.ensure_promotion_backfill_property().await.unwrap());
    assert!(ice.events_attr_rewrite_map().await.unwrap().is_some());
    let server_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app = router(AppState::new(server_ice, AuthConfig::open()));
    let after = dotted_key_answers(&app).await;
    assert_eq!(after, before, "the rewrite changed the answer");
}

fn stale_dotted_batch(n: usize) -> arrow_array::RecordBatch {
    use arrow_array::{ArrayRef, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let batch =
        siglake_core::events_to_record_batch(&attr_events(r#"{"a":{"b.c":"inner-literal"}}"#, n))
            .unwrap();
    let mut fields = batch.schema().fields().to_vec();
    // Put the synthetic column first so storage aligns the batch to the table
    // schema and applies the promoted field's Iceberg id metadata.
    fields.insert(0, Arc::new(Field::new("a_b_c", DataType::Utf8, true)));
    let mut columns = batch.columns().to_vec();
    columns.insert(
        0,
        Arc::new(StringArray::from(vec![None::<&str>; n])) as ArrayRef,
    );
    arrow_array::RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// A pre-upgrade writer can commit AFTER the new reader adopts the table. Its
/// actual stale NULL must keep both rewrite and prune disabled even after a
/// later current-writer commit. Rewriting that file physically corrects the
/// promoted value and restores qualification.
#[tokio::test]
async fn promoted_values_from_an_older_extractor_neither_rewrite_nor_prune() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use siglake_storage::iceberg::{PROMOTION_BACKFILL_PROP, PROMOTION_EXTRACTION_PROP};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_promoted_columns(dotted_promotion());
    ice.ensure_promoted_columns().await.unwrap();
    ice.append_events(&attr_events(r#"{"a":{"b":{"c":"nested"}}}"#, 5))
        .await
        .unwrap();
    assert!(ice.ensure_promotion_backfill_property().await.unwrap());
    assert!(ice.events_attr_rewrite_map().await.unwrap().is_some());

    // Forge the state a 0.2.1 catalog is in: qualified under the OLD rule
    // (the bare promotion list as the completion value) and carrying no
    // extraction stamp.
    let ident = ice.events_table_ident().clone();
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set(
            PROMOTION_BACKFILL_PROP.to_string(),
            siglake_core::promoted_property_json(&dotted_promotion()).unwrap(),
        )
        .remove(PROMOTION_EXTRACTION_PROP.to_string())
        .apply(tx)
        .unwrap();
    tx.commit(ice.catalog().as_ref()).await.unwrap();
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_promoted_columns(dotted_promotion());
    assert!(
        ice.events_attr_rewrite_map().await.unwrap().is_none(),
        "values of an unknown extraction rule must not be substituted"
    );

    // The new reader adopts at sequence S. A still-running old writer then
    // commits an actual stale promoted NULL at S+1, and a current writer lands
    // after it. The latest sequence is therefore no proof about the old file.
    assert!(!ice.ensure_promotion_backfill_property().await.unwrap());
    ice.append_unprovenanced_promoted_batch_for_test(&ident, stale_dotted_batch(10))
        .await
        .unwrap();
    ice.append_events(&attr_events(r#"{"a":{"b":{"c":"current"}}}"#, 3))
        .await
        .unwrap();
    assert!(!ice.ensure_promotion_backfill_property().await.unwrap());
    assert!(ice.events_attr_rewrite_map().await.unwrap().is_none());

    // The query answers from residual JSON. The projected physical column
    // exposes the stale NULL, while the attr_get filter retains all ten rows
    // and no promoted statistic prunes their file.
    let server_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app = router(AppState::new(server_ice, AuthConfig::open()));
    let before = dotted_key_answers(&app).await;
    assert_eq!(before.0, 10, "fixture must persist stale promoted NULLs");
    assert_eq!(before.1, 10, "residual attr_get must retain the stale rows");
    let sql = "SELECT count(*) AS n FROM events \
               WHERE attr_get(attributes, 'a.b.c') = 'inner-literal'";
    let (status, body) =
        request_json(&app, "/api/v1/sql", serde_json::json!({ "query": sql })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(10), "{body}");
    let scan = body["stats"]["scan"]
        .as_object()
        .unwrap_or_else(|| panic!("unqualified table must scan: {body}"));
    assert_eq!(
        scan["row_groups_pruned_stats"].as_u64(),
        Some(0),
        "promoted-column pruning must be off for stale values: {body}"
    );
    assert_eq!(scan["row_groups_read"].as_u64(), Some(3), "{body}");

    // Rewriting the files re-extracts every row through the current rule, and
    // the next pass qualifies the table again.
    let files = ice.live_data_files(&ident).await.unwrap();
    ice.recluster_files(&ident, files, &["host", "a_b_c"])
        .await
        .unwrap();
    assert!(ice.ensure_promotion_backfill_property().await.unwrap());
    assert!(ice.events_attr_rewrite_map().await.unwrap().is_some());

    let server_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app = router(AppState::new(server_ice, AuthConfig::open()));
    let after = dotted_key_answers(&app).await;
    assert_eq!(after.0, 0, "rewrite must physically correct the stale NULL");
    assert_eq!(after.1, 10);
    let (status, body) =
        request_json(&app, "/api/v1/sql", serde_json::json!({ "query": sql })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(10), "{body}");
}

async fn partial_delete_keeps_stale_promotion_unqualified(streaming: bool) {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let tuning = IcebergTuning {
        delete_rewrite_inram_max_bytes: Some(if streaming { 0 } else { u64::MAX }),
        delete_rewrite_inram_max_rows: Some(if streaming { 0 } else { u64::MAX }),
        ..Default::default()
    };
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_tuning(tuning)
        .with_promoted_columns(dotted_promotion());
    ice.ensure_promoted_columns().await.unwrap();
    let ident = ice.events_table_ident().clone();
    ice.append_unprovenanced_promoted_batch_for_test(&ident, stale_dotted_batch(3))
        .await
        .unwrap();
    assert!(!ice.ensure_promotion_backfill_property().await.unwrap());
    assert!(ice.events_attr_rewrite_map().await.unwrap().is_none());

    ice.create_delete_task("events", "raw = 'row 0'", None, None)
        .await
        .unwrap();
    let outcome = ice.execute_delete_tasks("events").await.unwrap();
    assert_eq!(outcome.tasks_completed, 1, "{outcome:?}");
    assert_eq!(outcome.tasks_failed, 0, "{outcome:?}");
    assert_eq!(outcome.files_rewritten, 1, "{outcome:?}");
    assert_eq!(outcome.rows_deleted, 1, "{outcome:?}");

    // The survivor writer copied the stale promoted NULLs. It must preserve
    // unknown provenance instead of laundering the output as current-rule data.
    assert!(!ice.ensure_promotion_backfill_property().await.unwrap());
    assert!(ice.events_attr_rewrite_map().await.unwrap().is_none());
    let server_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app = router(AppState::new(server_ice, AuthConfig::open()));
    let (status, body) = request_json(
        &app,
        "/api/v1/sql",
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events \
                      WHERE attr_get(attributes, 'a.b.c') = 'inner-literal'"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(2), "{body}");
}

#[tokio::test]
async fn inram_partial_delete_does_not_launder_stale_promoted_values() {
    partial_delete_keeps_stale_promotion_unqualified(false).await;
}

#[tokio::test]
async fn streaming_partial_delete_does_not_launder_stale_promoted_values() {
    partial_delete_keeps_stale_promotion_unqualified(true).await;
}
