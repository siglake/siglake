use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use chrono::{Duration as ChronoDuration, Utc};
use datafusion::prelude::SessionContext;
use siglake_core::index_config::{FieldMapping, FieldType, IndexConfig};
use siglake_core::{events_to_record_batch, Event};
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::{DeleteTask, DeleteTaskState, IcebergContext};
use siglake_storage::index_manager::IndexTemplate;
use tower::util::ServiceExt;

struct Server {
    app: Router,
    ice: Arc<IcebergContext>,
    /// The warehouse this server's context is rooted at, so a test can open a
    /// second, independent context against it — a second replica.
    warehouse: std::path::PathBuf,
    _tmp: tempfile::TempDir,
}

async fn spawn(auth: AuthConfig) -> Server {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app = router(AppState::new(ice.clone(), auth));
    Server {
        app,
        ice,
        warehouse,
        _tmp: tmp,
    }
}

async fn request_json(
    app: &Router,
    method: Method,
    path: &str,
    body: Option<serde_json::Value>,
    bearer: Option<&str>,
) -> (StatusCode, Option<serde_json::Value>) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(token) = bearer {
        builder = builder.header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let body = if let Some(body) = body {
        builder = builder.header(axum::http::header::CONTENT_TYPE, "application/json");
        Body::from(serde_json::to_vec(&body).unwrap())
    } else {
        Body::empty()
    };

    let response = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    if bytes.is_empty() {
        (status, None)
    } else {
        (status, Some(serde_json::from_slice(&bytes).unwrap()))
    }
}

fn field(name: &str, field_type: FieldType, required: bool) -> FieldMapping {
    FieldMapping {
        name: name.to_string(),
        field_type,
        required,
    }
}

fn logs_config(index_id: &str) -> IndexConfig {
    IndexConfig {
        index_id: index_id.to_string(),
        doc_mapping: IndexConfig::builtin_events().doc_mapping,
        retention: None,
        index_at_flush: None,
    }
}

fn logs_template(template_id: &str, patterns: Vec<&str>) -> IndexTemplate {
    IndexTemplate {
        template_id: template_id.to_string(),
        index_id_patterns: patterns.into_iter().map(str::to_string).collect(),
        priority: 7,
        doc_mapping: logs_config("placeholder").doc_mapping,
        retention: None,
    }
}

fn log_event(ts: chrono::DateTime<Utc>, host: &str, raw: &str) -> Event {
    Event {
        timestamp: ts,
        host: host.to_string(),
        source: "/var/log/app.log".to_string(),
        sourcetype: "app:json".to_string(),
        index: "main".to_string(),
        raw: raw.to_string(),
        attributes: None,
    }
}

/// `attributes` is the canonical events schema's one nullable column: `None` is
/// what every non-structured source writes.
fn log_event_with_attributes(
    ts: chrono::DateTime<Utc>,
    host: &str,
    raw: &str,
    attributes: Option<&str>,
) -> Event {
    Event {
        attributes: attributes.map(str::to_string),
        ..log_event(ts, host, raw)
    }
}

fn bloom_columns(config: &IndexConfig) -> Vec<String> {
    config
        .doc_mapping
        .tag_fields
        .iter()
        .filter_map(|name| {
            config
                .doc_mapping
                .field_mappings
                .iter()
                .find(|field| field.name == *name)
                .and_then(|field| match &field.field_type {
                    FieldType::Text { .. } | FieldType::Json => Some(name.clone()),
                    _ => None,
                })
        })
        .collect()
}

async fn append_index_events(ice: &IcebergContext, config: &IndexConfig, events: &[Event]) {
    let batch = events_to_record_batch(events).unwrap();
    let blooms = bloom_columns(config);
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    ice.append_to_table(&ice.index_table_ident(&config.index_id), batch, &bloom_refs)
        .await
        .unwrap();
}

async fn count_index_rows(ice: &IcebergContext, index_id: &str, where_sql: Option<&str>) -> i64 {
    let ctx = SessionContext::new();
    ice.register_table_with_datafusion(&ctx, &ice.index_table_ident(index_id), index_id)
        .await
        .unwrap();
    let sql = match where_sql {
        Some(predicate) => format!("SELECT count(*) AS n FROM \"{index_id}\" WHERE {predicate}"),
        None => format!("SELECT count(*) AS n FROM \"{index_id}\""),
    };
    let batches = ctx
        .sql(sql.as_str())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

/// Round-76 regression: a custom index must be queryable through the SQL
/// endpoint. The original registration path filtered referenced table names
/// through a fixed system-table list, so a query against any user index was
/// dropped from registration and failed planning with "table not found" —
/// found live on the cluster, invisible to the per-layer tests (REST tested
/// CRUD, storage tested scans, nobody crossed POST /api/v1/sql with a
/// dynamic table name).
#[tokio::test]
async fn custom_index_is_queryable_through_sql_endpoint() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("r76-regress");
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    append_index_events(
        &srv.ice,
        &config,
        &[
            log_event(Utc::now(), "rh-1", "regression needle quokka"),
            log_event(Utc::now(), "rh-2", "ordinary line"),
        ],
    )
    .await;

    // Quoted (hyphenated) dynamic table name through the real SQL endpoint —
    // both a plain scan and an FTS UDF over it.
    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({
            "query": "SELECT host FROM \"r76-regress\" WHERE match_terms(raw, 'quokka')"
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    let body = body.unwrap();
    assert_eq!(body["row_count"], 1, "body: {body}");
    assert_eq!(body["rows"][0]["host"], "rh-1");

    // An unknown table still errors clearly (not a managed index).
    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({ "query": "SELECT 1 FROM nonexistent_table" })),
        None,
    )
    .await;
    assert_ne!(status, StatusCode::OK);
    let msg = body.unwrap()["error"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        msg.contains("nonexistent_table"),
        "error should name the table: {msg}"
    );
}

/// #4038: a bare `SELECT … FROM "<index>" LIMIT n` gets the same implicit
/// newest-first ordering `events` gets. The rewrite used to be gated on a
/// hard-coded name list (`events`, `query_audit`), so every managed index
/// browsed in file order — and the ordered scan path was unreachable from an
/// index query that did not spell `ORDER BY` out. Run #73's text-LIMIT shapes
/// are the measurement that surfaced it.
#[tokio::test]
async fn bare_index_select_is_ordered_newest_first() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("order-idx");
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Appended oldest-first, in ONE file: an unordered scan returns them in
    // exactly this order, so either assertion below is a real observation.
    let base = Utc::now() - chrono::Duration::hours(1);
    let events: Vec<Event> = (0..6)
        .map(|j| {
            log_event(
                base + chrono::Duration::seconds(j),
                "oh-1",
                &format!("line {j}"),
            )
        })
        .collect();
    append_index_events(&srv.ice, &config, &events).await;

    let rows = |body: &serde_json::Value| -> Vec<String> {
        body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["raw"].as_str().unwrap().to_string())
            .collect()
    };

    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({
            "query": "SELECT timestamp, raw FROM \"order-idx\" LIMIT 3"
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    let body = body.unwrap();
    assert_eq!(
        rows(&body),
        vec!["line 5", "line 4", "line 3"],
        "bare LIMIT must be newest-first: {body}"
    );

    // The caller's explicit LIMIT is preserved, not replaced by the
    // ceiling+1 the no-LIMIT form injects.
    assert_eq!(body["row_count"].as_u64(), Some(3), "{body}");

    // `default_order: false` is still the opt-out: file order, oldest first.
    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({
            "query": "SELECT timestamp, raw FROM \"order-idx\" LIMIT 3",
            "default_order": false
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    let body = body.unwrap();
    assert_eq!(
        rows(&body),
        vec!["line 0", "line 1", "line 2"],
        "default_order:false must leave the scan unordered: {body}"
    );

    // A name that is not a managed index of this tenant is not made eligible
    // by asking: it still fails planning, and does not order anything.
    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({ "query": "SELECT raw FROM \"no-such-index\" LIMIT 3" })),
        None,
    )
    .await;
    assert_ne!(status, StatusCode::OK, "body: {body:?}");
}

/// #4090: the implicit newest-first rewrite injects a bare `timestamp` into
/// ORDER BY, and SQL resolves a bare ORDER BY identifier against the OUTPUT
/// names first. A projection that aliases another column to `timestamp`
/// therefore captures the injected sort, and the browse silently comes back
/// ordered by that column instead — a different top-N, not a different
/// presentation of the same top-N.
///
/// The fixture makes the three candidate orders distinguishable: raw text order
/// opposes event-time order, and neither matches file order.
#[tokio::test]
async fn aliased_timestamp_projection_does_not_capture_implicit_order() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("order-idx");
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Appended oldest-first in ONE file, so file order is "b", "d", "f", "e",
    // "c", "a". Newest-first by event time is "a", "c"; raw text descending is
    // "f", "e". Every assertion below picks exactly one of those three.
    let base = Utc::now() - chrono::Duration::hours(1);
    let texts = ["b", "d", "f", "e", "c", "a"];
    let events: Vec<Event> = texts
        .iter()
        .enumerate()
        .map(|(j, text)| {
            log_event(
                base + chrono::Duration::seconds(j as i64),
                "oh-1",
                &format!("{text} line"),
            )
        })
        .collect();
    append_index_events(&srv.ice, &config, &events).await;

    // The projected column is NAMED `timestamp` but holds `raw`.
    let aliased = |body: &serde_json::Value| -> Vec<String> {
        body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["timestamp"].as_str().unwrap().to_string())
            .collect()
    };
    let ask = |payload: serde_json::Value| {
        let app = srv.app.clone();
        async move {
            let (status, body) =
                request_json(&app, Method::POST, "/api/v1/sql", Some(payload), None).await;
            assert_eq!(status, StatusCode::OK, "body: {body:?}");
            body.unwrap()
        }
    };

    // Why the rewrite declines instead of re-binding the sort to the source
    // column: DataFusion refuses a qualified reference under this projection,
    // with either the table name or a table alias. Injecting `ORDER BY
    // "order-idx".timestamp` would turn a working browse into a 400.
    for qualified in [
        "SELECT raw AS timestamp FROM \"order-idx\" ORDER BY \"order-idx\".timestamp DESC LIMIT 2",
        "SELECT t.raw AS timestamp FROM \"order-idx\" AS t ORDER BY t.timestamp DESC LIMIT 2",
    ] {
        let (status, body) = request_json(
            &srv.app,
            Method::POST,
            "/api/v1/sql",
            Some(serde_json::json!({ "query": qualified })),
            None,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "`{qualified}`: {:?}",
            body.as_ref()
        );
        let msg = body.unwrap()["error"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        assert!(
            msg.contains("ambiguous"),
            "`{qualified}` should fail as ambiguous, not otherwise: {msg}"
        );
    }

    // `default_order: false` — the shape's reference point: file order.
    let unordered = ask(serde_json::json!({
        "query": "SELECT raw AS timestamp FROM \"order-idx\" LIMIT 2",
        "default_order": false
    }))
    .await;
    assert_eq!(
        aliased(&unordered),
        vec!["b line", "d line"],
        "default_order:false must leave the scan unordered: {unordered}"
    );

    // THE REGRESSION. Before the fix this returned "f line", "e line": the
    // injected `ORDER BY timestamp DESC` bound to the aliased `raw`, so the
    // browse answered with the two lexicographically largest lines instead of
    // the two newest. The rewrite now declines the shape, leaving file order.
    let implicit = ask(serde_json::json!({
        "query": "SELECT raw AS timestamp FROM \"order-idx\" LIMIT 2"
    }))
    .await;
    assert_eq!(
        aliased(&implicit),
        aliased(&unordered),
        "the alias must not capture the injected sort: {implicit}"
    );

    // Same through a table alias, where the source column is only reachable as
    // `t.timestamp` and the injected identifier is still bare.
    let implicit_table_alias = ask(serde_json::json!({
        "query": "SELECT t.raw AS timestamp FROM \"order-idx\" AS t LIMIT 2"
    }))
    .await;
    assert_eq!(
        aliased(&implicit_table_alias),
        aliased(&unordered),
        "table-aliased relation must not be text-ordered either: {implicit_table_alias}"
    );

    // Scoped: an alias that does NOT take the name `timestamp` still gets the
    // implicit newest-first browse.
    let other_alias = ask(serde_json::json!({
        "query": "SELECT raw AS ts FROM \"order-idx\" LIMIT 2"
    }))
    .await;
    assert_eq!(
        other_alias["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["ts"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["a line", "c line"],
        "an unshadowed projection is still newest-first: {other_alias}"
    );

    // A client's OWN `ORDER BY timestamp` keeps standard SQL alias semantics:
    // it names the output column, so raw text descending.
    let client_order = ask(serde_json::json!({
        "query": "SELECT raw AS timestamp FROM \"order-idx\" ORDER BY timestamp DESC LIMIT 2"
    }))
    .await;
    assert_eq!(
        aliased(&client_order),
        vec!["f line", "e line"],
        "an explicit ORDER BY still resolves to the output alias: {client_order}"
    );
}

/// #4214: a managed mapping may declare `timestamp` and `Timestamp` side by
/// side — validation preserves case and rejects duplicates by exact name — and
/// a quoted identifier keeps the case it was written with. #4090's guard
/// compared identifiers case-blind, so `SELECT "Timestamp" AS timestamp` read
/// as a projection of the source timestamp column, the rewrite went ahead, and
/// the injected `ORDER BY timestamp DESC` bound to the aliased TEXT column: a
/// top-N by string, returned as the newest rows.
///
/// The fixture keeps the three orders apart: file order, event-time order and
/// `Timestamp` text order each pick a different pair.
#[tokio::test]
async fn quoted_mixed_case_timestamp_projection_does_not_capture_implicit_order() {
    let srv = spawn(AuthConfig::open()).await;
    let mut config = logs_config("qcase-idx");
    config.doc_mapping.field_mappings.push(field(
        "Timestamp",
        FieldType::Text { tokenizer: None },
        false,
    ));
    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "`timestamp` and `Timestamp` must both be accepted: {body:?}"
    );

    // One file, appended oldest-first: file order is "b", "d", "f", "e", "c",
    // "a"; newest-first by event time is "a", "c"; `Timestamp` text descending
    // is "f", "e".
    let base = Utc::now() - chrono::Duration::hours(1);
    let rows_in = [
        ("b", "T3"),
        ("d", "T5"),
        ("f", "T9"),
        ("e", "T8"),
        ("c", "T1"),
        ("a", "T0"),
    ];
    let events: Vec<Event> = rows_in
        .iter()
        .enumerate()
        .map(|(j, (text, label))| {
            let mut event = log_event(
                base + chrono::Duration::seconds(j as i64),
                "oh-1",
                &format!("{text} line"),
            );
            event.attributes = Some(format!(r#"{{"Timestamp":"{label}"}}"#));
            event
        })
        .collect();
    let carrier = events_to_record_batch(&events).unwrap();
    let mapped = siglake_core::mapping::map_carrier_batch(&carrier, &config).unwrap();
    let blooms = bloom_columns(&config);
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    srv.ice
        .append_to_table(&srv.ice.index_table_ident("qcase-idx"), mapped, &bloom_refs)
        .await
        .unwrap();

    let ask = |payload: serde_json::Value| {
        let app = srv.app.clone();
        async move {
            let (status, body) =
                request_json(&app, Method::POST, "/api/v1/sql", Some(payload), None).await;
            assert_eq!(status, StatusCode::OK, "body: {body:?}");
            body.unwrap()
        }
    };
    let column = |body: &serde_json::Value, name: &str| -> Vec<String> {
        body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row[name].as_str().unwrap().to_string())
            .collect()
    };

    // The shape's reference point: the scan left unordered, so file order.
    let unordered = ask(serde_json::json!({
        "query": "SELECT \"Timestamp\" AS timestamp, raw FROM \"qcase-idx\" LIMIT 2",
        "default_order": false
    }))
    .await;
    assert_eq!(
        column(&unordered, "raw"),
        vec!["b line", "d line"],
        "default_order:false must leave the scan unordered: {unordered}"
    );

    // THE REGRESSION. Before the fix this answered "f line", "e line" — the two
    // largest `Timestamp` strings, dressed as the two newest rows.
    let implicit = ask(serde_json::json!({
        "query": "SELECT \"Timestamp\" AS timestamp, raw FROM \"qcase-idx\" LIMIT 2"
    }))
    .await;
    assert_eq!(
        column(&implicit, "raw"),
        column(&unordered, "raw"),
        "the quoted mixed-case alias must not capture the injected sort: {implicit}"
    );

    // Scoped: the index is eligible, and an unshadowed projection of it still
    // gets the implicit newest-first browse.
    let newest = ask(serde_json::json!({
        "query": "SELECT timestamp, raw FROM \"qcase-idx\" LIMIT 2"
    }))
    .await;
    assert_eq!(
        column(&newest, "raw"),
        vec!["a line", "c line"],
        "an unshadowed projection is still newest-first: {newest}"
    );

    // An explicit sort on the quoted text column keeps the semantics it asked
    // for — string order, not event time.
    let by_text = ask(serde_json::json!({
        "query": "SELECT raw, \"Timestamp\" FROM \"qcase-idx\" ORDER BY \"Timestamp\" DESC LIMIT 2"
    }))
    .await;
    assert_eq!(
        column(&by_text, "raw"),
        vec!["f line", "e line"],
        "an explicit sort on the text column must sort by text: {by_text}"
    );
    assert_eq!(column(&by_text, "Timestamp"), vec!["T9", "T8"], "{by_text}");
}

/// Fix 1: `count(*) WHERE col != V` / `NOT IN (...)` must return the exact count
/// (the algebraic rewrite is total − excluded, with NULL groups excluded). A
/// negation can't be bloom-pruned, so before this it full-scanned (and timed out
/// at 1TB); here we assert correctness against a known distribution.
#[tokio::test]
async fn negation_count_returns_exact_count() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("neg-idx");
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    // 3 web, 2 db, 1 cache = 6 rows.
    let now = Utc::now();
    let events: Vec<Event> = [("web", 3), ("db", 2), ("cache", 1)]
        .iter()
        .flat_map(|(host, n)| (0..*n).map(move |_| log_event(now, host, "line")))
        .collect();
    append_index_events(&srv.ice, &config, &events).await;

    // Returns the count AND asserts the negation fast path FIRED (zero data-file
    // scan) — a fallthrough scan would also produce the right count, so checking
    // correctness alone wouldn't catch the path not engaging (the cost.exact
    // guard bug that 504'd at 1TB).
    let count = |sql: &'static str| {
        let app = srv.app.clone();
        async move {
            let (status, body) = request_json(
                &app,
                Method::POST,
                "/api/v1/sql",
                Some(serde_json::json!({ "query": sql })),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "sql {sql}: {body:?}");
            let body = body.unwrap();
            assert_eq!(
                body["stats"]["rows_scanned"].as_u64(),
                Some(0),
                "negation fast path must serve `{sql}` with zero scan: {body}"
            );
            body["rows"][0]["n"].as_i64().unwrap()
        }
    };

    // != excludes 'web' (3) → 3 remain
    assert_eq!(
        count("SELECT count(*) AS n FROM \"neg-idx\" WHERE host != 'web'").await,
        3
    );
    // <> is the same operator
    assert_eq!(
        count("SELECT count(*) AS n FROM \"neg-idx\" WHERE host <> 'web'").await,
        3
    );
    // NOT IN ('web','db') excludes 5 → 1 remains
    assert_eq!(
        count("SELECT count(*) AS n FROM \"neg-idx\" WHERE host NOT IN ('web', 'db')").await,
        1
    );
    // sanity: total is 6
    assert_eq!(count("SELECT count(*) AS n FROM \"neg-idx\"").await, 6);
}

/// http_logs gap class (2026-07-22 round): counts filtered on a TYPED (Long)
/// index column — equality, negation, and integer ranges — must serve
/// zero-scan from the group-count footers, exactly like GROUP BY on the same
/// column does. Pre-fix these full-scanned (247M rows live) with the answer
/// sitting in the footers.
#[tokio::test]
async fn typed_dimensional_counts_serve_zero_scan() {
    let srv = spawn(AuthConfig::open()).await;
    let mut config = logs_config("web-idx");
    config
        .doc_mapping
        .field_mappings
        .push(field("status", FieldType::Long, false));
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // 3×200, 2×404, 1×503 — status rides the attributes JSON through the
    // carrier mapping into the typed Long column, like the ES-bulk path.
    let now = Utc::now();
    let events: Vec<Event> = [(200i64, 3usize), (404, 2), (503, 1)]
        .iter()
        .flat_map(|(code, n)| {
            (0..*n).map(move |_| {
                let mut e = log_event(now, "web", "line");
                e.attributes = Some(format!(r#"{{"status":{code}}}"#));
                e
            })
        })
        .collect();
    let carrier = events_to_record_batch(&events).unwrap();
    let mapped = siglake_core::mapping::map_carrier_batch(&carrier, &config).unwrap();
    let blooms = bloom_columns(&config);
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    srv.ice
        .append_to_table(&srv.ice.index_table_ident("web-idx"), mapped, &bloom_refs)
        .await
        .unwrap();

    // Every count asserts the fast path FIRED (zero data-file scan) — a
    // fallthrough scan produces the same numbers, so correctness alone
    // wouldn't catch the path not engaging on typed columns.
    let count = |sql: &'static str| {
        let app = srv.app.clone();
        async move {
            let (status, body) = request_json(
                &app,
                Method::POST,
                "/api/v1/sql",
                Some(serde_json::json!({ "query": sql })),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "sql {sql}: {body:?}");
            let body = body.unwrap();
            assert_eq!(
                body["stats"]["rows_scanned"].as_u64(),
                Some(0),
                "typed dimensional count must serve `{sql}` with zero scan: {body}"
            );
            body["rows"][0]["n"].as_i64().unwrap()
        }
    };

    // Integer equality.
    assert_eq!(
        count("SELECT count(*) AS n FROM \"web-idx\" WHERE status = 404").await,
        2
    );
    // Integer negation (NULL-free here; NULLs would drop out per SQL `!=`).
    assert_eq!(
        count("SELECT count(*) AS n FROM \"web-idx\" WHERE status != 200").await,
        3
    );
    // Two-sided range (the 4xx/5xx shape).
    assert_eq!(
        count("SELECT count(*) AS n FROM \"web-idx\" WHERE status >= 400 AND status <= 599").await,
        3
    );
    // Strict bound + BETWEEN.
    assert_eq!(
        count("SELECT count(*) AS n FROM \"web-idx\" WHERE status > 404").await,
        1
    );
    assert_eq!(
        count("SELECT count(*) AS n FROM \"web-idx\" WHERE status BETWEEN 500 AND 599").await,
        1
    );
    // Integer IN list.
    assert_eq!(
        count("SELECT count(*) AS n FROM \"web-idx\" WHERE status IN (200, 503)").await,
        4
    );
}

/// The http_logs avg_size_by_status class: a numeric GROUP-BY aggregate on a
/// user index is served from the snapshot-keyed SQL result cache on repeat
/// (warm ≈ ms instead of a re-scan), and a commit invalidates it (new
/// snapshot ⇒ new key ⇒ fresh recompute).
#[tokio::test]
async fn index_aggregate_result_caches_and_invalidates_on_commit() {
    let srv = spawn(AuthConfig::open()).await;
    let mut config = logs_config("agg-idx");
    config
        .doc_mapping
        .field_mappings
        .push(field("status", FieldType::Long, false));
    config
        .doc_mapping
        .field_mappings
        .push(field("size", FieldType::Long, false));
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let append = |codes: Vec<(i64, i64)>| {
        let srv_ice = srv.ice.clone();
        let config = config.clone();
        async move {
            let now = Utc::now();
            let events: Vec<Event> = codes
                .into_iter()
                .map(|(code, size)| {
                    let mut e = log_event(now, "web", "line");
                    e.attributes = Some(format!(r#"{{"status":{code},"size":{size}}}"#));
                    e
                })
                .collect();
            let carrier = events_to_record_batch(&events).unwrap();
            let mapped = siglake_core::mapping::map_carrier_batch(&carrier, &config).unwrap();
            srv_ice
                .append_to_table(&srv_ice.index_table_ident("agg-idx"), mapped, &[])
                .await
                .unwrap();
        }
    };
    append(vec![(200, 10), (200, 30), (404, 100)]).await;

    let sql = "SELECT status, avg(size) AS avg_size, count(*) AS n FROM \"agg-idx\" \
               GROUP BY status ORDER BY n DESC LIMIT 20";
    let run = || {
        let app = srv.app.clone();
        async move {
            let (status, body) = request_json(
                &app,
                Method::POST,
                "/api/v1/sql",
                Some(serde_json::json!({ "query": sql })),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body:?}");
            body.unwrap()["rows"].clone()
        }
    };

    let first = run().await;
    assert_eq!(first[0]["status"].as_i64(), Some(200));
    assert_eq!(first[0]["avg_size"].as_f64(), Some(20.0));
    assert_eq!(first[0]["n"].as_i64(), Some(2));
    // Repeat at the same snapshot: identical (served from the result cache —
    // correctness assertion; the caching itself is observable via the
    // siglake_query_sql_result_cache_requests_total{outcome} counters).
    assert_eq!(run().await, first);

    // A commit moves the snapshot: the cached entry must NOT be replayed.
    // (Both groups tie at n=2 afterward, so assert by group, not position.)
    append(vec![(404, 300)]).await;
    let third = run().await;
    let by_status = |code: i64| {
        third
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["status"].as_i64() == Some(code))
            .unwrap_or_else(|| panic!("status {code} missing: {third}"))
            .clone()
    };
    assert_eq!(by_status(200)["n"].as_i64(), Some(2));
    assert_eq!(by_status(404)["n"].as_i64(), Some(2));
    assert_eq!(by_status(404)["avg_size"].as_f64(), Some(200.0));
}

/// #61 per-index buffer serving: rows sealed in a user index's WAL but not yet
/// committed to Iceberg must be visible to queries (union provider), counts
/// must be exact across committed + buffered, and the DYNAMIC fast-path guard
/// must (a) yield to the union while segments are in flight and (b) restore
/// the zero-scan fast paths once the buffer drains.
#[tokio::test]
async fn index_wal_buffer_serves_uncommitted_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let wal_root = tmp.path().join("wal");
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let state =
        AppState::new(ice.clone(), AuthConfig::open()).with_wal_buffer_dir(Some(wal_root.clone()));
    let delta_cache = state.buffer_delta_cache.clone();
    let app = router(state);

    let config = logs_config("buf-idx");
    let (status, _) = request_json(
        &app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // 2 committed rows…
    let now = Utc::now();
    append_index_events(
        &ice,
        &config,
        &[
            log_event(now, "web", "committed a"),
            log_event(now, "db", "committed b"),
        ],
    )
    .await;
    // …and 3 rows sealed in the index's WAL, NOT committed.
    let index_wal = wal_root.join("default").join("buf-idx");
    std::fs::create_dir_all(&index_wal).unwrap();
    let mut w = siglake_wal::WalWriter::with_thresholds(
        &index_wal,
        "ing-test",
        10_000,
        std::time::Duration::from_secs(600),
    )
    .unwrap();
    w.append_events(&[
        log_event(now, "web", "buffered a"),
        log_event(now, "web", "buffered b"),
        log_event(now, "cache", "buffered c"),
    ])
    .unwrap();
    w.seal().unwrap().expect("sealed segment");
    drop(w);

    let query = |sql: String| {
        let app = app.clone();
        async move {
            let (status, body) = request_json(
                &app,
                Method::POST,
                "/api/v1/sql",
                Some(serde_json::json!({ "query": sql })),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "sql {sql}: {body:?}");
            body.unwrap()
        }
    };

    // Buffered rows are visible; count is exact across committed + buffered —
    // AND still zero-scan (#61 hybrid: the fast path folds the buffered rows
    // as a delta instead of degrading to the union full scan).
    let body = query("SELECT count(*) AS n FROM \"buf-idx\"".into()).await;
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(5), "{body}");
    assert_eq!(
        body["stats"]["rows_scanned"].as_u64(),
        Some(0),
        "hybrid count must stay zero-scan with buffered rows: {body}"
    );
    // #78: the first request decoded the delta (miss); an identical repeat
    // with the buffer unchanged must serve it from the cache — same answer,
    // still zero-scan, no re-decode.
    assert_eq!(
        delta_cache.stats(),
        (0, 1),
        "first load must be a decode miss"
    );
    let body = query("SELECT count(*) AS n FROM \"buf-idx\"".into()).await;
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(5), "{body}");
    assert_eq!(body["stats"]["rows_scanned"].as_u64(), Some(0), "{body}");
    let (hits, misses) = delta_cache.stats();
    assert_eq!(misses, 1, "unchanged buffer must not re-decode");
    assert!(hits >= 1, "repeat load must hit the cache");
    // Grouped count sees the union too (2+1 committed, 2+1 buffered by host).
    let body =
        query("SELECT host, count(*) AS n FROM \"buf-idx\" GROUP BY host ORDER BY host".into())
            .await;
    let rows = body["rows"].as_array().unwrap();
    let by_host: std::collections::HashMap<&str, i64> = rows
        .iter()
        .map(|r| (r["host"].as_str().unwrap(), r["n"].as_i64().unwrap()))
        .collect();
    assert_eq!(by_host.get("web"), Some(&3), "{body}");
    assert_eq!(by_host.get("db"), Some(&1));
    assert_eq!(by_host.get("cache"), Some(&1));
    // Raw browse sees buffered rows.
    let body = query(
        "SELECT raw FROM \"buf-idx\" WHERE raw LIKE '%buffered%' ORDER BY raw LIMIT 10".into(),
    )
    .await;
    assert_eq!(body["row_count"].as_u64(), Some(3), "{body}");

    // #78 freshness: a NEWLY sealed segment changes the basename set — the
    // cache must miss and the count must include the new row immediately.
    let mut w2 = siglake_wal::WalWriter::with_thresholds(
        &index_wal,
        "ing-test-2",
        10_000,
        std::time::Duration::from_secs(600),
    )
    .unwrap();
    w2.append_events(&[log_event(now, "web", "buffered d")])
        .unwrap();
    w2.seal().unwrap().expect("sealed segment");
    drop(w2);
    let body = query("SELECT count(*) AS n FROM \"buf-idx\"".into()).await;
    assert_eq!(
        body["rows"][0]["n"].as_i64(),
        Some(6),
        "new sealed row must be visible: {body}"
    );
    assert!(
        delta_cache.stats().1 >= 2,
        "new segment must re-decode, not serve stale cache"
    );

    // Drain the buffer (segment leaves sealed/): counts drop back to the
    // committed 2 AND the zero-scan count fast path is eligible again.
    let sealed = index_wal.join("sealed");
    for entry in std::fs::read_dir(&sealed).unwrap().flatten() {
        std::fs::remove_file(entry.path()).unwrap();
    }
    let body = query("SELECT count(*) AS n FROM \"buf-idx\"".into()).await;
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(2), "{body}");
    assert_eq!(
        body["stats"]["rows_scanned"].as_u64(),
        Some(0),
        "drained buffer must restore the zero-scan fast path: {body}"
    );
}

/// #79: one warm cycle touches the events table and every managed index —
/// the same function the periodic warmer loops over so an unqueried
/// coordinator/worker never ages past the staleness ceiling.
#[tokio::test]
async fn warm_cycle_covers_events_and_indexes() {
    let server = spawn(AuthConfig::open()).await;
    let config = logs_config("warm-idx");
    let (status, _) = request_json(
        &server.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let now = Utc::now();
    append_index_events(&server.ice, &config, &[log_event(now, "web", "warm row")]).await;

    let warmed = siglake_query_server::warm_all_query_caches(&server.ice).await;
    assert_eq!(warmed, 2, "events + warm-idx must both warm");
    // Repeat cycle (the periodic path): all in-memory hits, same coverage.
    assert_eq!(
        siglake_query_server::warm_all_query_caches(&server.ice).await,
        2
    );
}

/// #61 transition race: when the compactor renames a segment into
/// `committed/`, a stale-while-revalidate table cache can still serve a
/// pre-commit snapshot — dropping the segment from the buffer at the rename
/// would make its rows VANISH until the refresh lands (observed live: LIKE
/// count 20000 → 0 → 20000). Recently-committed segments must stay buffered
/// until the serving snapshot's history names them (cumulative consumed set),
/// and drop out without double-counting once it does.
#[tokio::test]
async fn index_buffer_survives_commit_transition() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let wal_root = tmp.path().join("wal");
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app = router(
        AppState::new(ice.clone(), AuthConfig::open()).with_wal_buffer_dir(Some(wal_root.clone())),
    );
    let config = logs_config("trans-idx");
    let (status, _) = request_json(
        &app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Seal 3 rows into the index WAL.
    let now = Utc::now();
    let index_wal = wal_root.join("default").join("trans-idx");
    std::fs::create_dir_all(&index_wal).unwrap();
    let mut w = siglake_wal::WalWriter::with_thresholds(
        &index_wal,
        "ing-test",
        10_000,
        std::time::Duration::from_secs(600),
    )
    .unwrap();
    let events = vec![
        log_event(now, "web", "trans a"),
        log_event(now, "db", "trans b"),
        log_event(now, "web", "trans c"),
    ];
    w.append_events(&events).unwrap();
    let sealed = w.seal().unwrap().expect("sealed segment");
    drop(w);
    let basename = sealed
        .path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let count = |app: axum::Router| async move {
        let (status, body) = request_json(
            &app,
            Method::POST,
            "/api/v1/sql",
            Some(serde_json::json!({ "query": "SELECT count(*) AS n FROM \"trans-idx\"" })),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        body.unwrap()["rows"][0]["n"].as_i64().unwrap()
    };

    assert_eq!(count(app.clone()).await, 3, "sealed rows visible");

    // The compactor's rename: sealed/ → committed/, BEFORE the serving
    // snapshot knows the rows. The buffer must keep serving them.
    let committed_dir = index_wal.join("committed");
    std::fs::create_dir_all(&committed_dir).unwrap();
    std::fs::rename(&sealed.path, committed_dir.join(&basename)).unwrap();
    assert_eq!(
        count(app.clone()).await,
        3,
        "recently-committed rows must not vanish while the snapshot lags"
    );

    // The commit lands (rows in Iceberg + snapshot names the segment): the
    // segment drops out of the buffer — exact, no double count.
    let batch = siglake_core::events_to_record_batch(&events).unwrap();
    let mapped = siglake_core::map_carrier_batch(&batch, &config).unwrap();
    let mut props = std::collections::HashMap::new();
    props.insert(
        siglake_storage::iceberg::CONSUMED_SEGMENTS_PROP.to_string(),
        basename.clone(),
    );
    ice.append_to_table_with_props(&ice.index_table_ident("trans-idx"), mapped, &[], props)
        .await
        .unwrap();
    assert_eq!(
        count(app.clone()).await,
        3,
        "post-commit: rows served from Iceberg exactly once"
    );
}

/// Seal one segment of `events` into `<wal_root>/default/<index>/sealed/` and
/// return its path. `#2640` needs the basename, so this returns the sealed
/// segment rather than just dropping the writer.
fn seal_index_segment(
    wal_root: &std::path::Path,
    index: &str,
    writer_id: &str,
    events: &[Event],
) -> std::path::PathBuf {
    let index_wal = wal_root.join("default").join(index);
    std::fs::create_dir_all(&index_wal).unwrap();
    let mut w = siglake_wal::WalWriter::with_thresholds(
        &index_wal,
        writer_id,
        10_000,
        std::time::Duration::from_secs(600),
    )
    .unwrap();
    w.append_events(events).unwrap();
    let sealed = w.seal().unwrap().expect("sealed segment");
    sealed.path
}

/// Every `raw` `sql` returns, sorted — row IDENTITIES, not just a count.
async fn sql_raws(app: &Router, sql: &str) -> Vec<String> {
    let (status, body) = request_json(
        app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({ "query": sql })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "sql {sql}: {body:?}");
    let body = body.unwrap();
    let mut raws: Vec<String> = body["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("sql {sql}: {body}"))
        .iter()
        .map(|row| row["raw"].as_str().unwrap().to_string())
        .collect();
    raws.sort();
    raws
}

/// #2661 (was #2640's characterization): after an index is deleted and
/// recreated under the same name, a WAL-enabled query sees ONLY the
/// replacement's own rows, and a drain does not commit the dropped
/// incarnation's segments into it.
///
/// The WAL directory is keyed by tenant and index NAME
/// (`wal_buffer::resolve_index_wal_dir`), so deletion leaves it untouched, and
/// the buffer's exclusion set is read from the REPLACEMENT table, whose
/// snapshot history is empty. Two populations are exercised separately because
/// they resurfaced by different routes:
///
/// - `sealed/` — never committed anywhere. It read back through the buffer,
///   and a drain resolving the target by name made that permanent.
/// - `committed/` — already committed to the DROPPED table, and still inside
///   `RECENT_COMMITTED_WINDOW`. The old table's consumed set excluded it; the
///   replacement's does not, so it read back as a DOUBLE of rows whose only
///   surviving copy is in the dropped table's orphaned files.
///
/// A third population (#2835) shares the directory with them: segments an
/// ingest lane that had already re-resolved the index acknowledged for the
/// REPLACEMENT before any drain visited. The sweep that quarantines the first
/// two must leave those alone, including the partial their writer still holds
/// open, and the writer must go on appending and sealing afterward.
///
/// Both are now cut off by the directory's owner marker
/// (`siglake_wal::OWNER_FILE`), stamped by the drain that committed
/// population 2 and compared by every reader. The pre-deletion arms are the
/// other half of the regression: the buffer still serves un-committed rows and
/// the consumed-set exclusion still holds.
///
/// `dropping_committed_index_retains_files_and_recreation_is_a_new_table`
/// (siglake-storage) pins the Iceberg-only half of the same boundary.
#[tokio::test]
async fn a_recreated_index_serves_only_its_own_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let wal_root = tmp.path().join("wal");
    // `list_tenant_dirs` recognizes a tenant by its own `sealed/`; the drain
    // arm below needs `default` to be found that way.
    std::fs::create_dir_all(wal_root.join("default").join("sealed")).unwrap();

    let ice_old = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app_old = router(
        AppState::new(ice_old.clone(), AuthConfig::open())
            .with_wal_buffer_dir(Some(wal_root.clone())),
    );
    let config = logs_config("recr-idx");
    let (status, _) = request_json(
        &app_old,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let now = Utc::now();
    let index_wal = wal_root.join("default").join("recr-idx");

    // Population 2 FIRST, drained by the real compactor: it commits the rows
    // with the segment named in `CONSUMED_SEGMENTS_PROP`, renames the file into
    // `committed/` with a fresh mtime, and stamps the directory's owner marker
    // with the table it committed to. Nothing here is hand-rolled — the marker
    // under test is written by the shipped drain.
    let committed_events: Vec<Event> = (0..2)
        .map(|i| log_event(now, "committed-host", &format!("old committed {i}")))
        .collect();
    let committed_path =
        seal_index_segment(&wal_root, "recr-idx", "ing-committed", &committed_events);
    let committed_name = committed_path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(
        siglake_compactor::Compactor::new(&wal_root, ice_old.clone())
            .run_once()
            .await
            .unwrap(),
        1,
        "the drain must commit the first segment into the ORIGINAL table"
    );
    assert!(index_wal.join("committed").join(&committed_name).exists());
    let dropped_owner =
        siglake_wal::read_wal_owner(&index_wal).expect("the drain stamps the directory's owner");

    // Population 1: sealed after that drain, so never committed anywhere.
    let sealed_events: Vec<Event> = (0..3)
        .map(|i| log_event(now, "sealed-host", &format!("old sealed {i}")))
        .collect();
    let sealed_path = seal_index_segment(&wal_root, "recr-idx", "ing-sealed", &sealed_events);

    // BEFORE. The buffer serves the sealed rows and excludes the committed
    // segment, so each row appears exactly once.
    let browse = "SELECT raw FROM \"recr-idx\" ORDER BY raw";
    assert_eq!(
        sql_raws(&app_old, browse).await,
        vec![
            "old committed 0",
            "old committed 1",
            "old sealed 0",
            "old sealed 1",
            "old sealed 2",
        ],
        "pre-deletion: committed once (consumed-set exclusion) plus the sealed rows"
    );

    // Delete and recreate under the same name, through the shipped API.
    let (status, _) = request_json(
        &app_old,
        Method::DELETE,
        "/api/v1/indexes/recr-idx",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = request_json(
        &app_old,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        sealed_path.exists(),
        "deleting the index must not touch its WAL"
    );

    // Read through FRESH contexts, so nothing below is a stale provider cache
    // (#2639's boundary) — only the WAL routing and the exclusion set.
    let ice_new = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app_iceberg_only = router(AppState::new(ice_new.clone(), AuthConfig::open()));
    let app_buffered = router(
        AppState::new(ice_new.clone(), AuthConfig::open())
            .with_wal_buffer_dir(Some(wal_root.clone())),
    );

    // AFTER, Iceberg-only: the replacement is a new, empty table. This is the
    // answer `README.md`'s "recreation is a new table" bullet describes and
    // `index_manager.rs`'s committed-only regression pins.
    assert!(
        sql_raws(&app_iceberg_only, browse).await.is_empty(),
        "Iceberg-only: the replacement table must read empty"
    );

    // AFTER, WAL-enabled: the same answer. The directory's owner marker still
    // names the dropped table, so neither population is folded in — including
    // the committed pair, whose only surviving copy is in the dropped table's
    // orphaned files.
    assert!(
        sql_raws(&app_buffered, browse).await.is_empty(),
        "WAL-enabled: the replacement must expose only its own rows"
    );
    assert_eq!(
        siglake_wal::read_wal_owner(&index_wal).as_deref(),
        Some(dropped_owner.as_str()),
        "a read must not re-stamp the directory: the drain owns the marker"
    );

    // Population 3, #2835: an ingest lane that has already re-resolved the
    // index (`SIGLAKE_WAL_IDENTITY_REFRESH_SECS`) and bound to the
    // REPLACEMENT. Its rows are acknowledged into this same directory while
    // the marker still names the dropped table — one segment sealed, one
    // partial still open — so the drain below meets both incarnations at once.
    let live_uuid = ice_new
        .index_table_uuid("recr-idx")
        .await
        .unwrap()
        .expect("the replacement resolves to a table");
    assert_ne!(live_uuid, dropped_owner, "recreation is a new table");
    let mut replacement_writer = siglake_wal::WalWriter::with_thresholds(
        &index_wal,
        "ing-repl",
        10_000,
        std::time::Duration::from_secs(600),
    )
    .unwrap();
    replacement_writer
        .bind_table_uuid(Some(uuid::Uuid::parse_str(&live_uuid).unwrap()))
        .unwrap();
    replacement_writer
        .append_events(&[log_event(now, "new-host", "replacement sealed")])
        .unwrap();
    let replacement_sealed = replacement_writer.seal().unwrap().expect("sealed").path;
    replacement_writer
        .append_events(&[log_event(now, "new-host", "replacement open")])
        .unwrap();
    let open_partial = only_active_partial(&index_wal);

    // Until a drain re-stamps, the directory gate refuses the whole directory,
    // so even the replacement's own segment reads back empty. That is the
    // marker doing its job with the coarsest information it has; what must not
    // happen is the drain DESTROYING the distinction.
    assert!(
        sql_raws(&app_buffered, browse).await.is_empty(),
        "the directory still names the dropped table, so nothing under it is served"
    );

    // A drain resolves the target by NAME (`ensure_index`), which is how the
    // sealed population used to become a permanent snapshot of the
    // REPLACEMENT. It now finds the mismatch first and quarantines instead.
    let compactor = siglake_compactor::Compactor::new(&wal_root, ice_new.clone());
    assert_eq!(
        compactor.run_once().await.unwrap(),
        0,
        "the drain must not commit the dropped incarnation's sealed segment"
    );
    let ice_drained = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app_drained = router(AppState::new(ice_drained.clone(), AuthConfig::open()));
    assert!(
        sql_raws(&app_drained, browse).await.is_empty(),
        "after the drain the replacement table is still empty"
    );

    // Both populations are held, not deleted: a never-committed segment's rows
    // may exist nowhere else, and the already-committed one belongs to files
    // the dropped table still owns.
    let held = index_wal.join("stale").join(&dropped_owner);
    let sealed_name = sealed_path.file_name().unwrap();
    assert!(
        held.join(sealed_name).exists(),
        "the sealed segment is quarantined under stale/<dropped-uuid>/"
    );
    assert!(
        held.join(&committed_name).exists(),
        "so is the segment the DROPPED table had already committed"
    );
    assert!(!sealed_path.exists());
    assert!(!index_wal.join("committed").join(&committed_name).exists());

    // #2835: population 3 is untouched. Each of its segments names the LIVE
    // table in its own frame header, so the marker the sweep displaced never
    // spoke for them, and moving them would strand rows the ingester has
    // already acknowledged — with no concurrent drain in sight.
    assert!(
        replacement_sealed.exists(),
        "the replacement's sealed segment stays in sealed/, drainable"
    );
    assert!(
        !held.join(replacement_sealed.file_name().unwrap()).exists(),
        "and is not under stale/<dropped-uuid>/"
    );
    assert!(
        open_partial.exists(),
        "nor is the open partial pulled out from under its live writer"
    );

    // And the directory is now the replacement's: a segment sealed from here
    // on is ordinary in-flight data, served and drained as usual. So is the
    // one the sweep left in place.
    assert_eq!(
        siglake_wal::read_wal_owner(&index_wal),
        ice_drained.index_table_uuid("recr-idx").await.unwrap(),
        "the drain re-stamps the quarantined directory for the live table"
    );
    assert_eq!(
        sql_raws(&app_buffered, browse).await,
        vec!["replacement sealed"],
        "the segment quarantine spared is served as soon as the marker moves"
    );

    // The writer that was open across the re-stamp keeps writing into the same
    // directory: it appends and seals, and the partial it held becomes an
    // ordinary sealed segment.
    replacement_writer
        .append_events(&[log_event(now, "new-host", "replacement after re-stamp")])
        .unwrap();
    let after_restamp = replacement_writer.seal().unwrap().expect("sealed").path;
    assert!(after_restamp.exists());
    assert!(
        !open_partial.exists(),
        "the partial sealed rather than lingering"
    );

    let fresh_events: Vec<Event> = (0..2)
        .map(|i| log_event(now, "fresh-host", &format!("new sealed {i}")))
        .collect();
    seal_index_segment(&wal_root, "recr-idx", "ing-fresh", &fresh_events);
    assert_eq!(
        sql_raws(&app_buffered, browse).await,
        vec![
            "new sealed 0",
            "new sealed 1",
            "replacement after re-stamp",
            "replacement open",
            "replacement sealed",
        ],
        "the replacement's own in-flight rows are served"
    );
    assert_eq!(
        siglake_compactor::Compactor::new(&wal_root, ice_new.clone())
            .run_once()
            .await
            .unwrap(),
        3,
        "and drained"
    );
    assert_eq!(
        sql_raws(
            &router(AppState::new(
                Arc::new(IcebergContext::open(&warehouse).await.unwrap()),
                AuthConfig::open(),
            )),
            browse
        )
        .await,
        vec![
            "new sealed 0",
            "new sealed 1",
            "replacement after re-stamp",
            "replacement open",
            "replacement sealed",
        ],
        "committed into the replacement, and nothing of the dropped incarnation with them"
    );
}

/// The single `*.arrow.partial` under `<index_wal>/active/` — the file a live
/// writer is appending to. Panics unless there is exactly one.
fn only_active_partial(index_wal: &std::path::Path) -> std::path::PathBuf {
    let mut found: Vec<std::path::PathBuf> = std::fs::read_dir(index_wal.join("active"))
        .expect("active/ exists once a writer has opened a segment")
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("partial"))
        .collect();
    assert_eq!(found.len(), 1, "expected one open partial: {found:?}");
    found.pop().unwrap()
}

/// #2693: the residue #2661 left open — a writer HELD OPEN across the
/// `DELETE`+`POST`, sealing AFTER the drain has re-stamped the directory for
/// the replacement.
///
/// The directory marker cannot catch this one. It says "replacement" by the
/// time the stale writer seals, because the drain that quarantined the dropped
/// incarnation's segments re-stamped it in the same pass — that is the marker
/// working as designed. Every reader that resolves the target by name would
/// then fold the stale writer's rows into the replacement, permanently once a
/// drain committed them.
///
/// The identity is therefore bound to the SEGMENT, in its frame header, before
/// its first append. This test seals through a writer that learned the dropped
/// table's uuid and never learned the replacement's, and asserts the rows are
/// refused by the buffer and by the drain, and held under `stale/`.
#[tokio::test]
async fn a_writer_held_open_across_recreation_cannot_reach_the_replacement() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let wal_root = tmp.path().join("wal");
    std::fs::create_dir_all(wal_root.join("default").join("sealed")).unwrap();

    let ice_old = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let app_old = router(
        AppState::new(ice_old.clone(), AuthConfig::open())
            .with_wal_buffer_dir(Some(wal_root.clone())),
    );
    let config = logs_config("held-idx");
    let (status, _) = request_json(
        &app_old,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let now = Utc::now();
    let index_wal = wal_root.join("default").join("held-idx");
    std::fs::create_dir_all(&index_wal).unwrap();
    let dropped_uuid = ice_old
        .index_table_uuid("held-idx")
        .await
        .unwrap()
        .expect("the index resolves to a table");

    // An ingester lane, bound to the table the index resolves to right now —
    // exactly what `TenantWalRouter`/`BackpressureRouter` do at lane creation.
    let mut stale_writer = siglake_wal::WalWriter::with_thresholds(
        &index_wal,
        "ing-held",
        10_000,
        std::time::Duration::from_secs(600),
    )
    .unwrap();
    stale_writer
        .bind_table_uuid(Some(uuid::Uuid::parse_str(&dropped_uuid).unwrap()))
        .unwrap();
    stale_writer
        .append_events(&[log_event(now, "old-host", "before the drop")])
        .unwrap();
    let before_drop = stale_writer.seal().unwrap().expect("sealed").path;

    // Delete and recreate through the shipped API.
    let (status, _) = request_json(
        &app_old,
        Method::DELETE,
        "/api/v1/indexes/held-idx",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = request_json(
        &app_old,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let ice_new = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let live_uuid = ice_new
        .index_table_uuid("held-idx")
        .await
        .unwrap()
        .expect("the replacement resolves to a table");
    assert_ne!(live_uuid, dropped_uuid, "recreation is a new table");

    // The drain re-stamps the directory and holds the pre-drop segment. Note
    // WHICH gate catches it: no drain had visited this directory before the
    // recreation, so its marker is absent and #2661's directory check has no
    // opinion — the segment's own header is the only thing that knows. From
    // here on the DIRECTORY vouches for the replacement.
    let compactor = siglake_compactor::Compactor::new(&wal_root, ice_new.clone());
    assert_eq!(compactor.run_once().await.unwrap(), 0);
    assert_eq!(
        siglake_wal::read_wal_owner(&index_wal).as_deref(),
        Some(live_uuid.as_str()),
        "the drain has re-stamped the directory for the replacement"
    );
    assert!(!before_drop.exists(), "the pre-drop segment is quarantined");

    // THE RESIDUE: the ingester never learned about the recreation, so its
    // writer is still bound to the dropped table, and it seals into a
    // directory that now says otherwise.
    stale_writer
        .append_events(&[log_event(now, "old-host", "after the re-stamp")])
        .unwrap();
    let contaminant = stale_writer.seal().unwrap().expect("sealed").path;
    assert!(
        contaminant.exists(),
        "the stale writer seals into the LIVE directory"
    );
    assert_eq!(
        siglake_wal::classify_segment_owner(&contaminant, &live_uuid),
        siglake_wal::WalOwner::Stale(dropped_uuid.clone()),
        "the segment names the table it was opened for, whatever the directory says"
    );

    // The buffer refuses it: the directory gate passes, the per-segment gate
    // does not.
    let browse = "SELECT raw FROM \"held-idx\" ORDER BY raw";
    let app_buffered = router(
        AppState::new(
            Arc::new(IcebergContext::open(&warehouse).await.unwrap()),
            AuthConfig::open(),
        )
        .with_wal_buffer_dir(Some(wal_root.clone())),
    );
    assert!(
        sql_raws(&app_buffered, browse).await.is_empty(),
        "the buffer must not serve a dropped incarnation's rows"
    );

    // And the drain refuses it, holding it beside the pre-drop segment rather
    // than committing or deleting it.
    assert_eq!(
        siglake_compactor::Compactor::new(&wal_root, ice_new.clone())
            .run_once()
            .await
            .unwrap(),
        0,
        "the drain must not commit the stale writer's segment"
    );
    let held = index_wal.join("stale").join(&dropped_uuid);
    assert!(
        held.join(contaminant.file_name().unwrap()).exists(),
        "held under stale/<dropped-uuid>/, not deleted"
    );
    assert!(
        held.join(before_drop.file_name().unwrap()).exists(),
        "beside the segment sealed before the drop"
    );
    assert!(
        sql_raws(
            &router(AppState::new(
                Arc::new(IcebergContext::open(&warehouse).await.unwrap()),
                AuthConfig::open(),
            )),
            browse
        )
        .await
        .is_empty(),
        "after the drain the replacement table is still empty"
    );

    // The lane's own rows are unaffected: rebind the writer the way the
    // identity refresh does, and everything from there is ordinary in-flight
    // data — served, then drained.
    stale_writer
        .bind_table_uuid(Some(uuid::Uuid::parse_str(&live_uuid).unwrap()))
        .unwrap();
    stale_writer
        .append_events(&[log_event(now, "new-host", "after the rebind")])
        .unwrap();
    stale_writer.seal().unwrap().expect("sealed");
    assert_eq!(
        sql_raws(&app_buffered, browse).await,
        vec!["after the rebind"],
        "the replacement's own in-flight rows are served"
    );
    assert_eq!(
        siglake_compactor::Compactor::new(&wal_root, ice_new.clone())
            .run_once()
            .await
            .unwrap(),
        1,
        "and drained"
    );
}

/// Fix 4: `count(DISTINCT col)` is served exactly from the guarded per-value
/// counts (side object / footers) with ZERO data-file scan — inverted-index engines error
/// on this shape, siglake previously paid a full column scan+hash. A windowed
/// variant must NOT be claimed by the fast path (it falls through to the
/// planner and stays correct).
#[tokio::test]
async fn count_distinct_serves_exact_from_aggregates() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("cd-idx");
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    // 3 distinct hosts across 6 rows.
    let now = Utc::now();
    let events: Vec<Event> = [("web", 3), ("db", 2), ("cache", 1)]
        .iter()
        .flat_map(|(host, n)| (0..*n).map(move |_| log_event(now, host, "line")))
        .collect();
    append_index_events(&srv.ice, &config, &events).await;

    let query = |sql: &'static str| {
        let app = srv.app.clone();
        async move {
            let (status, body) = request_json(
                &app,
                Method::POST,
                "/api/v1/sql",
                Some(serde_json::json!({ "query": sql })),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "sql {sql}: {body:?}");
            body.unwrap()
        }
    };

    // Fast path: exact count, zero rows scanned.
    let body = query("SELECT count(DISTINCT host) AS n FROM \"cd-idx\"").await;
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(3), "{body}");
    assert_eq!(
        body["stats"]["rows_scanned"].as_u64(),
        Some(0),
        "count-distinct fast path must serve with zero scan: {body}"
    );

    // Windowed variant is NOT the fast path's shape — the planner answers it,
    // exactly, whatever it scans.
    let body =
        query("SELECT count(DISTINCT host) AS n FROM \"cd-idx\" WHERE host != 'cache'").await;
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(2), "{body}");
}

/// Bench/observability instrumentation: every SQL response carries the
/// `x-siglake-server-micros` header (engine time, the analogue of an
/// `elapsed_time_micros`), and a scanning query reports a `stats` block with
/// the leaf rows/bytes it actually read (the pruning-effectiveness metric).
#[tokio::test]
async fn sql_response_carries_scan_stats_and_server_micros_header() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("stats-idx");
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    append_index_events(
        &srv.ice,
        &config,
        &[
            log_event(Utc::now(), "sh-1", "alpha line"),
            log_event(Utc::now(), "sh-2", "beta line"),
        ],
    )
    .await;

    // Raw oneshot so we can inspect response headers (request_json drops them).
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/sql")
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({
                "query": "SELECT host FROM \"stats-idx\" ORDER BY host"
            }))
            .unwrap(),
        ))
        .unwrap();
    let resp = srv.app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let micros = resp
        .headers()
        .get("x-siglake-server-micros")
        .expect("server-micros header present")
        .to_str()
        .unwrap()
        .parse::<u64>()
        .expect("header parses as u64");
    assert!(micros > 0, "server micros should be positive");

    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let stats = &body["stats"];
    assert!(stats.is_object(), "scanning query must carry stats: {body}");
    assert_eq!(
        stats["rows_scanned"].as_u64(),
        Some(2),
        "two rows scanned: {body}"
    );
    assert!(
        stats["bytes_scanned"].as_u64().unwrap() > 0,
        "bytes_scanned should be positive on a real scan: {body}"
    );
}

#[tokio::test]
async fn indexes_http_lifecycle_round_trips() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("logs");

    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let created: IndexConfig = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(created, config);

    let (status, body) =
        request_json(&srv.app, Method::GET, "/api/v1/indexes/logs", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let fetched: IndexConfig = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(fetched, config);

    let (status, body) = request_json(&srv.app, Method::GET, "/api/v1/indexes", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let listed: Vec<IndexConfig> = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(listed, vec![IndexConfig::builtin_events(), config.clone()]);

    let mut updated = config.clone();
    updated.doc_mapping.field_mappings.push(field(
        "severity",
        FieldType::Text {
            tokenizer: Some("raw".to_string()),
        },
        false,
    ));
    updated.doc_mapping.tag_fields.push("severity".to_string());

    let (status, body) = request_json(
        &srv.app,
        Method::PUT,
        "/api/v1/indexes/logs",
        Some(serde_json::to_value(&updated).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let stored: IndexConfig = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(stored, updated);

    let (status, body) =
        request_json(&srv.app, Method::GET, "/api/v1/indexes/logs", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let fetched: IndexConfig = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(fetched, updated);

    let (status, body) =
        request_json(&srv.app, Method::DELETE, "/api/v1/indexes/logs", None, None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_none());

    let (status, _) = request_json(&srv.app, Method::GET, "/api/v1/indexes/logs", None, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn indexes_http_errors_map_to_expected_statuses() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("logs");

    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let invalid = serde_json::json!({
        "index_id": "bad-logs",
        "doc_mapping": {
            "mode": "dynamic",
            "field_mappings": [
                { "name": "ts", "type": "datetime", "required": true },
                { "name": "message", "type": "text", "tokenizer": "bogus", "required": false }
            ],
            "timestamp_field": "ts",
            "tag_fields": [],
            "default_search_fields": ["message"]
        },
        "retention": null
    });
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(invalid),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let mismatch = logs_config("other-logs");
    let (status, _) = request_json(
        &srv.app,
        Method::PUT,
        "/api/v1/indexes/logs",
        Some(serde_json::to_value(&mismatch).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let mut changed_type = config.clone();
    changed_type.doc_mapping.field_mappings[2].field_type = FieldType::Long;
    let (status, _) = request_json(
        &srv.app,
        Method::PUT,
        "/api/v1/indexes/logs",
        Some(serde_json::to_value(&changed_type).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) =
        request_json(&srv.app, Method::GET, "/api/v1/indexes/missing", None, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = request_json(
        &srv.app,
        Method::DELETE,
        "/api/v1/indexes/missing",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn legacy_events_index_is_exposed_and_deletable() {
    let srv = spawn(AuthConfig::open()).await;

    // The legacy `events` table qualifies because storage synthesizes
    // `IndexConfig::builtin_events()` when the table exists.
    srv.ice.ensure_events_table().await.unwrap();

    let (status, body) =
        request_json(&srv.app, Method::GET, "/api/v1/indexes/events", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let config: IndexConfig = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(config, IndexConfig::builtin_events());

    let (status, _) = request_json(
        &srv.app,
        Method::DELETE,
        "/api/v1/indexes/events",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) =
        request_json(&srv.app, Method::GET, "/api/v1/indexes/events", None, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn index_templates_round_trip_and_validate() {
    let srv = spawn(AuthConfig::open()).await;
    let template = logs_template("tenant-logs", vec!["tenant-*"]);

    let path = format!("/api/v1/index-templates/{}", template.template_id);
    let (status, body) = request_json(
        &srv.app,
        Method::PUT,
        &path,
        Some(serde_json::to_value(&template).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let stored: IndexTemplate = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(stored, template);

    let (status, body) =
        request_json(&srv.app, Method::GET, "/api/v1/index-templates", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let listed: Vec<IndexTemplate> = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(listed, vec![template.clone()]);

    let (status, _) = request_json(&srv.app, Method::DELETE, &path, None, None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) =
        request_json(&srv.app, Method::GET, "/api/v1/index-templates", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let listed: Vec<IndexTemplate> = serde_json::from_value(body.unwrap()).unwrap();
    assert!(listed.is_empty());

    let invalid = logs_template("broken-template", Vec::new());
    let path = format!("/api/v1/index-templates/{}", invalid.template_id);
    let (status, _) = request_json(
        &srv.app,
        Method::PUT,
        &path,
        Some(serde_json::to_value(&invalid).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// Template edits made over REST and edits made by a second replica on the same
/// warehouse must compose: neither surface's acknowledged change may carry away
/// the other's. Sequential rather than raced — the point is that a REST PUT and
/// DELETE write only their own template's key, so a peer's independent edit is
/// still there afterwards. (The raced form lives in siglake-storage's
/// `index_template_durability`.)
///
/// THE DEFECT THIS GUARDS. The packaged query deployment is a 2-replica
/// StatefulSet. With the v1 single-document template set, this replica's DELETE
/// replaced the whole document from a set read before the peer's PUT, so the
/// peer's template vanished after the API had already acknowledged it.
#[tokio::test]
async fn index_template_edits_do_not_clobber_a_second_replicas_edits() {
    let srv = spawn(AuthConfig::open()).await;
    let peer = IcebergContext::open(&srv.warehouse).await.unwrap();

    let over_rest = logs_template("rest-template", vec!["rest-*"]);
    let rest_path = format!("/api/v1/index-templates/{}", over_rest.template_id);
    let (status, _) = request_json(
        &srv.app,
        Method::PUT,
        &rest_path,
        Some(serde_json::to_value(&over_rest).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let on_the_peer = logs_template("peer-template", vec!["peer-*"]);
    peer.put_index_template(&on_the_peer).await.unwrap();
    let doomed = logs_template("doomed-template", vec!["doomed-*"]);
    peer.put_index_template(&doomed).await.unwrap();

    let (status, body) =
        request_json(&srv.app, Method::GET, "/api/v1/index-templates", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let listed: Vec<IndexTemplate> = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(
        listed,
        vec![doomed.clone(), on_the_peer.clone(), over_rest.clone()],
        "this replica must see the peer's templates alongside its own"
    );

    // A DELETE here must take exactly one template with it.
    let (status, _) = request_json(
        &srv.app,
        Method::DELETE,
        &format!("/api/v1/index-templates/{}", doomed.template_id),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        peer.list_index_templates().await.unwrap(),
        vec![on_the_peer, over_rest],
        "the peer must see the deletion and keep everything else"
    );
}

#[tokio::test]
async fn indexes_require_auth_when_bearer_mode_is_enabled() {
    let srv = spawn(AuthConfig::from_tokens(["secret-token"])).await;
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(logs_config("logs")).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn delete_tasks_rest_create_list_get_and_execute() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("logs");
    srv.ice.create_index(&config).await.unwrap();

    let now = Utc::now();
    append_index_events(
        &srv.ice,
        &config,
        &[
            log_event(
                now - ChronoDuration::days(3),
                "victim",
                "outside-bound victim",
            ),
            log_event(
                now - ChronoDuration::days(3) + ChronoDuration::minutes(1),
                "keep",
                "outside-bound keep",
            ),
        ],
    )
    .await;
    let outside_path = srv
        .ice
        .live_data_files(&srv.ice.index_table_ident("logs"))
        .await
        .unwrap()[0]
        .file_path()
        .to_string();
    append_index_events(
        &srv.ice,
        &config,
        &[
            log_event(
                now - ChronoDuration::hours(24),
                "victim",
                "inside-bound victim",
            ),
            log_event(
                now - ChronoDuration::hours(24) + ChronoDuration::minutes(1),
                "keep",
                "inside-bound keep",
            ),
        ],
    )
    .await;
    append_index_events(
        &srv.ice,
        &config,
        &[
            log_event(now - ChronoDuration::hours(23), "other", "no-match-1"),
            log_event(
                now - ChronoDuration::hours(23) + ChronoDuration::minutes(1),
                "other",
                "no-match-2",
            ),
        ],
    )
    .await;

    let body = serde_json::json!({
        "index_id": "logs",
        "predicate_sql": "host = 'victim'",
        "start_ts": (now - ChronoDuration::hours(36)).to_rfc3339(),
        "end_ts": (now - ChronoDuration::hours(12)).to_rfc3339(),
    });
    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/delete-tasks",
        Some(body),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let task: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(task.state, DeleteTaskState::Pending);

    let (status, body) = request_json(
        &srv.app,
        Method::GET,
        "/api/v1/delete-tasks?index_id=logs",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let listed: Vec<DeleteTask> = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].task_id, task.task_id);

    let outcome = srv.ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(outcome.tasks_completed, 1);
    assert_eq!(outcome.tasks_failed, 0);
    assert_eq!(outcome.files_rewritten, 1);
    assert_eq!(outcome.rows_deleted, 1);
    assert_eq!(count_index_rows(&srv.ice, "logs", None).await, 5);
    assert_eq!(
        count_index_rows(&srv.ice, "logs", Some("host = 'victim'")).await,
        1,
        "the bounded file should be rewritten, but the out-of-bounds file must survive",
    );
    let after_paths: Vec<String> = srv
        .ice
        .live_data_files(&srv.ice.index_table_ident("logs"))
        .await
        .unwrap()
        .into_iter()
        .map(|file| file.file_path().to_string())
        .collect();
    assert!(
        after_paths.iter().any(|path| path == &outside_path),
        "file outside the task bounds should not be rewritten"
    );

    let path = format!("/api/v1/delete-tasks/{}", task.task_id);
    let (status, body) = request_json(&srv.app, Method::GET, &path, None, None).await;
    assert_eq!(status, StatusCode::OK);
    let body = body.unwrap();
    assert!(
        body.get("claim").is_none(),
        "terminal tasks must not imply that a permanent claim describes current execution: {body}"
    );
    let stored: DeleteTask = serde_json::from_value(body).unwrap();
    assert_eq!(stored.state, DeleteTaskState::Done);
    assert_eq!(stored.files_rewritten, 1);
    assert_eq!(stored.rows_deleted, 1);
}

/// A pending task's GET response observes the permanent sibling claim without
/// changing the persisted task or the POST/list contracts. Presence is an
/// exclusion fact only; the claimant and elapsed wall time are diagnostics.
#[tokio::test]
async fn delete_task_get_reports_claimed_and_unclaimed_pending_tasks() {
    let srv = spawn(AuthConfig::open()).await;
    srv.ice.create_index(&logs_config("logs")).await.unwrap();
    let unclaimed = srv
        .ice
        .create_delete_task("logs", "host = 'queued'", None, None)
        .await
        .unwrap();
    let claimed = srv
        .ice
        .create_delete_task("logs", "host = 'stranded'", None, None)
        .await
        .unwrap();
    let claimant = uuid::Uuid::parse_str("018f1000-0000-7000-8000-000000000001").unwrap();
    let claimed_at = Utc::now() - ChronoDuration::seconds(90);
    persist_delete_task_claim(
        &srv.warehouse,
        &srv.ice.namespace().to_string(),
        claimed.task_id,
        serde_json::to_vec(&serde_json::json!({
            "task_id": claimed.task_id,
            "claimed_at": claimed_at,
            "claimant": claimant,
        }))
        .unwrap()
        .as_slice(),
    );

    let path = format!("/api/v1/delete-tasks/{}", unclaimed.task_id);
    let (status, body) = request_json(&srv.app, Method::GET, &path, None, None).await;
    assert_eq!(status, StatusCode::OK);
    let body = body.unwrap();
    assert_eq!(body["task_id"], unclaimed.task_id.to_string());
    assert_eq!(body["state"], "pending");
    assert_eq!(body["claim"]["present"], false);
    assert!(body["claim"]["observed_at"].is_string());
    assert!(body["claim"]["claimant"].is_null());
    assert!(body["claim"]["claimed_at"].is_null());
    assert!(body["claim"]["age_seconds"].is_null());

    let path = format!("/api/v1/delete-tasks/{}", claimed.task_id);
    let (status, body) = request_json(&srv.app, Method::GET, &path, None, None).await;
    assert_eq!(status, StatusCode::OK);
    let body = body.unwrap();
    assert_eq!(body["task_id"], claimed.task_id.to_string());
    assert_eq!(body["claim"]["present"], true);
    assert_eq!(body["claim"]["claimant"], claimant.to_string());
    assert_eq!(
        body["claim"]["claimed_at"],
        serde_json::to_value(claimed_at).unwrap()
    );
    let age = body["claim"]["age_seconds"].as_u64().unwrap();
    assert!((90..=92).contains(&age), "unexpected claim age: {body}");

    let listed: Vec<DeleteTask> = srv.ice.list_delete_tasks(Some("logs")).await.unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed
        .iter()
        .all(|task| task.state == DeleteTaskState::Pending));
}

/// THE #1471 ABANDONED-CLAIM FIXTURE. A dead executor may leave only `{}` (or
/// another unreadable diagnostic body) beside a still-pending record. The
/// create-only object still excludes every later executor, so GET must show
/// `present: true` while refusing to invent claimant, timestamp, or age.
#[tokio::test]
async fn delete_task_get_keeps_malformed_and_mismatched_claims_visible() {
    let srv = spawn(AuthConfig::open()).await;
    srv.ice.create_index(&logs_config("logs")).await.unwrap();
    let malformed_bodies = [
        Vec::new(),
        br#"{"task_id":"truncated"#.to_vec(),
        b"{}".to_vec(),
    ];

    for bytes in malformed_bodies {
        let task = srv
            .ice
            .create_delete_task("logs", "host = 'stranded'", None, None)
            .await
            .unwrap();
        persist_delete_task_claim(
            &srv.warehouse,
            &srv.ice.namespace().to_string(),
            task.task_id,
            &bytes,
        );
        assert_unreadable_claim_is_present(&srv.app, task.task_id).await;
    }

    let task = srv
        .ice
        .create_delete_task("logs", "host = 'mismatch'", None, None)
        .await
        .unwrap();
    let mismatched = serde_json::json!({
        "task_id": uuid::Uuid::now_v7(),
        "claimed_at": Utc::now(),
        "claimant": uuid::Uuid::now_v7(),
    });
    persist_delete_task_claim(
        &srv.warehouse,
        &srv.ice.namespace().to_string(),
        task.task_id,
        &serde_json::to_vec(&mismatched).unwrap(),
    );
    assert_unreadable_claim_is_present(&srv.app, task.task_id).await;
}

async fn assert_unreadable_claim_is_present(app: &Router, task_id: uuid::Uuid) {
    let path = format!("/api/v1/delete-tasks/{task_id}");
    let (status, body) = request_json(app, Method::GET, &path, None, None).await;
    assert_eq!(status, StatusCode::OK);
    let claim = &body.unwrap()["claim"];
    assert_eq!(claim["present"], true);
    assert!(claim["claimant"].is_null(), "{claim}");
    assert!(claim["claimed_at"].is_null(), "{claim}");
    assert!(claim["age_seconds"].is_null(), "{claim}");
}

/// An acknowledged deletion request whose predicate names a nullable column
/// must complete, not fail, when the candidate file also holds NULL-valued
/// rows.
///
/// THE DEFECT THIS GUARDS (task #2094). The executor counted the rows to
/// delete with `WHERE {predicate}` (TRUE only) but selected the survivors with
/// `WHERE NOT ({predicate})`, which is NULL wherever the predicate is. A row
/// with `attributes` null was in neither set, so the conservation guard fired
/// and the task went to `Failed` — permanently, since every later sweep saw
/// the same file. `attributes` is null for every non-structured source, so an
/// ordinary `attributes = '…'` GDPR request hit this on real data. The 201 is
/// answered before the executor runs, so the API had already acknowledged a
/// deletion that could never happen.
#[tokio::test]
async fn delete_tasks_rest_predicate_on_a_nullable_column_completes_and_keeps_null_rows() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("logs");
    srv.ice.create_index(&config).await.unwrap();

    let gone = r#"{"tenant":"gone"}"#;
    let stay = r#"{"tenant":"stay"}"#;
    let now = Utc::now();
    append_index_events(
        &srv.ice,
        &config,
        &[
            log_event_with_attributes(
                now - ChronoDuration::minutes(3),
                "web-01",
                "true row",
                Some(gone),
            ),
            log_event_with_attributes(
                now - ChronoDuration::minutes(2),
                "web-01",
                "false row",
                Some(stay),
            ),
            log_event_with_attributes(now - ChronoDuration::minutes(1), "web-01", "null row", None),
        ],
    )
    .await;

    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/delete-tasks",
        Some(serde_json::json!({
            "index_id": "logs",
            "predicate_sql": format!("attributes = '{gone}'"),
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let task: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();

    let outcome = srv.ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(outcome.tasks_completed, 1, "{outcome:?}");
    assert_eq!(
        outcome.tasks_failed, 0,
        "a NULL-valued nonmatch must not fail an acknowledged request: {outcome:?}"
    );
    assert_eq!(outcome.files_rewritten, 1, "{outcome:?}");
    assert_eq!(outcome.rows_deleted, 1, "{outcome:?}");

    let path = format!("/api/v1/delete-tasks/{}", task.task_id);
    let (status, body) = request_json(&srv.app, Method::GET, &path, None, None).await;
    assert_eq!(status, StatusCode::OK);
    let stored: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(stored.state, DeleteTaskState::Done, "{stored:?}");
    assert_eq!(stored.error, None, "{stored:?}");
    assert_eq!(stored.rows_deleted, 1, "{stored:?}");

    assert_eq!(count_index_rows(&srv.ice, "logs", None).await, 2);
    assert_eq!(
        count_index_rows(&srv.ice, "logs", Some(&format!("attributes = '{gone}'"))).await,
        0,
        "the matching row must be gone"
    );
    assert_eq!(
        count_index_rows(&srv.ice, "logs", Some(&format!("attributes = '{stay}'"))).await,
        1,
        "the FALSE-valued nonmatch must survive"
    );
    assert_eq!(
        count_index_rows(&srv.ice, "logs", Some("attributes IS NULL")).await,
        1,
        "the NULL-valued nonmatch must survive"
    );
}

/// The REST surface admits `attributes IS NULL` and the executor deletes
/// exactly the NULL rows, leaving the non-NULL ones.
#[tokio::test]
async fn delete_tasks_rest_is_null_predicate_deletes_only_the_null_rows() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("logs");
    srv.ice.create_index(&config).await.unwrap();

    let stay = r#"{"tenant":"stay"}"#;
    let now = Utc::now();
    append_index_events(
        &srv.ice,
        &config,
        &[
            log_event_with_attributes(
                now - ChronoDuration::minutes(3),
                "web-01",
                "null row 1",
                None,
            ),
            log_event_with_attributes(
                now - ChronoDuration::minutes(2),
                "web-01",
                "kept row",
                Some(stay),
            ),
            log_event_with_attributes(
                now - ChronoDuration::minutes(1),
                "web-01",
                "null row 2",
                None,
            ),
        ],
    )
    .await;

    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/delete-tasks",
        Some(serde_json::json!({
            "index_id": "logs",
            "predicate_sql": "attributes IS NULL",
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body:?}");
    let task: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();

    let outcome = srv.ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(outcome.tasks_failed, 0, "{outcome:?}");
    assert_eq!(outcome.rows_deleted, 2, "{outcome:?}");

    let stored = srv
        .ice
        .get_delete_task(task.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, DeleteTaskState::Done, "{stored:?}");
    assert_eq!(stored.rows_deleted, 2, "{stored:?}");
    assert_eq!(count_index_rows(&srv.ice, "logs", None).await, 1);
    assert_eq!(
        count_index_rows(&srv.ice, "logs", Some("attributes IS NULL")).await,
        0
    );
}

/// The dry run over the same NULL-bearing file reports the deletion it would
/// perform and leaves the task `Pending` with every row intact. Before the fix
/// the preview failed the row-count guard too, so an operator could not even
/// see what the request would do.
#[tokio::test]
async fn delete_tasks_dry_run_over_a_null_bearing_file_preserves_every_row() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("logs");
    srv.ice.create_index(&config).await.unwrap();

    let gone = r#"{"tenant":"gone"}"#;
    let now = Utc::now();
    append_index_events(
        &srv.ice,
        &config,
        &[
            log_event_with_attributes(
                now - ChronoDuration::minutes(2),
                "web-01",
                "true row",
                Some(gone),
            ),
            log_event_with_attributes(now - ChronoDuration::minutes(1), "web-01", "null row", None),
        ],
    )
    .await;

    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/delete-tasks",
        Some(serde_json::json!({
            "index_id": "logs",
            "predicate_sql": format!("attributes = '{gone}'"),
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let task: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();

    let outcome = srv.ice.preview_delete_tasks("logs").await.unwrap();
    assert!(outcome.dry_run, "{outcome:?}");
    assert_eq!(outcome.tasks_failed, 0, "{outcome:?}");
    assert_eq!(outcome.files_rewritten, 1, "{outcome:?}");
    assert_eq!(outcome.rows_deleted, 1, "{outcome:?}");

    let path = format!("/api/v1/delete-tasks/{}", task.task_id);
    let (status, body) = request_json(&srv.app, Method::GET, &path, None, None).await;
    assert_eq!(status, StatusCode::OK);
    let stored: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(
        stored.state,
        DeleteTaskState::Pending,
        "a dry run must not transition the task: {stored:?}"
    );
    assert_eq!(count_index_rows(&srv.ice, "logs", None).await, 2);
    assert_eq!(
        count_index_rows(&srv.ice, "logs", Some(&format!("attributes = '{gone}'"))).await,
        1,
        "a dry run deletes nothing"
    );
}

/// Persist a delete-task record straight into the warehouse, bypassing the
/// submission path. A `Failed` task cannot be produced through the API any
/// more (that was the #2094 defect), so the fixture stands in for a record a
/// pre-fix build left behind: same layout, same key, one JSON object at
/// `_siglake/config/delete_tasks/<namespace>/<task_id>.json`.
fn persist_delete_task_record(warehouse: &std::path::Path, namespace: &str, task: &DeleteTask) {
    let dir = warehouse
        .join("_siglake")
        .join("config")
        .join("delete_tasks")
        .join(namespace);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(format!("{}.json", task.task_id)),
        serde_json::to_vec(task).unwrap(),
    )
    .unwrap();
}

fn persist_delete_task_claim(
    warehouse: &std::path::Path,
    namespace: &str,
    task_id: uuid::Uuid,
    body: &[u8],
) {
    let dir = warehouse
        .join("_siglake")
        .join("config")
        .join("delete_tasks")
        .join(namespace);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{task_id}.claim")), body).unwrap();
}

/// The documented recovery for a `Failed` delete task: resubmit its request
/// fields and get a NEW task id. `Failed` is terminal — `execute_delete_tasks`
/// takes `Pending` only and nothing moves a task back — so a warehouse left
/// holding a task that failed under the pre-#2094 NULL complement holds an
/// acknowledged (201) deletion request that no sweep will ever execute again.
/// The recovery is an explicit POST of `index_id`, `predicate_sql`, `start_ts`
/// and `end_ts` under the same tenant identity, once the executor runs a build
/// where the original failure cause is remedied.
///
/// This pins what the README documents: the failed record and its error survive
/// the recovery unchanged (both ids belong in the operator's audit trail), the
/// new id is distinct and starts `Pending`, and the new task alone reports the
/// deletion — with the NULL-valued nonmatch preserved.
#[tokio::test]
async fn delete_tasks_rest_resubmitting_a_failed_task_completes_under_a_new_id() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("logs");
    srv.ice.create_index(&config).await.unwrap();

    let gone = r#"{"tenant":"gone"}"#;
    let stay = r#"{"tenant":"stay"}"#;
    let now = Utc::now();
    append_index_events(
        &srv.ice,
        &config,
        &[
            log_event_with_attributes(
                now - ChronoDuration::minutes(3),
                "web-01",
                "true row",
                Some(gone),
            ),
            log_event_with_attributes(
                now - ChronoDuration::minutes(2),
                "web-01",
                "false row",
                Some(stay),
            ),
            log_event_with_attributes(now - ChronoDuration::minutes(1), "web-01", "null row", None),
        ],
    )
    .await;

    // The error text a pre-fix executor wrote: the conservation guard fired
    // because the NULL-valued nonmatch was in neither the deleted nor the
    // survivor set. Naming the real candidate path keeps the fixture faithful.
    let candidate = srv
        .ice
        .live_data_files(&srv.ice.index_table_ident("logs"))
        .await
        .unwrap()[0]
        .file_path()
        .to_string();
    let failed = DeleteTask {
        task_id: uuid::Uuid::parse_str("00000000-0000-4000-8000-00000000dead").unwrap(),
        index_id: "logs".to_string(),
        // The pre-fix record carried no incarnation binding, and nothing
        // backfills one. Harmless here: the task is terminal, so no executor
        // reaches the fence.
        table_uuid: None,
        predicate_sql: format!("attributes = '{gone}'"),
        start_ts: None,
        end_ts: None,
        created_at: "2026-09-01T00:00:00Z".parse().unwrap(),
        state: DeleteTaskState::Failed,
        error: Some(format!(
            "delete-task row-count mismatch on {candidate}: output 1 + deleted 1 != input 3"
        )),
        files_rewritten: 0,
        rows_deleted: 0,
    };
    persist_delete_task_record(&srv.warehouse, &srv.ice.namespace().to_string(), &failed);

    let failed_path = format!("/api/v1/delete-tasks/{}", failed.task_id);
    let (status, body) = request_json(&srv.app, Method::GET, &failed_path, None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_value::<DeleteTask>(body.unwrap()).unwrap(),
        failed,
        "the fixture must read back through the API as written"
    );

    // Terminal means terminal: a sweep does not examine it, let alone retry it.
    let outcome = srv.ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(
        outcome.tasks_examined, 0,
        "a Failed task must not be re-executed: {outcome:?}"
    );
    assert_eq!(outcome.rows_deleted, 0, "{outcome:?}");
    assert_eq!(count_index_rows(&srv.ice, "logs", None).await, 3);

    // The recovery: the same request fields, submitted again.
    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/delete-tasks",
        Some(serde_json::json!({
            "index_id": failed.index_id,
            "predicate_sql": failed.predicate_sql,
            "start_ts": failed.start_ts,
            "end_ts": failed.end_ts,
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let resubmitted: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();
    assert_ne!(
        resubmitted.task_id, failed.task_id,
        "resubmission answers with a new id; it does not revive the old one"
    );
    assert_eq!(resubmitted.state, DeleteTaskState::Pending);

    let outcome = srv.ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(outcome.tasks_examined, 1, "{outcome:?}");
    assert_eq!(outcome.tasks_completed, 1, "{outcome:?}");
    assert_eq!(outcome.tasks_failed, 0, "{outcome:?}");
    assert_eq!(outcome.files_rewritten, 1, "{outcome:?}");
    assert_eq!(outcome.rows_deleted, 1, "{outcome:?}");

    let path = format!("/api/v1/delete-tasks/{}", resubmitted.task_id);
    let (status, body) = request_json(&srv.app, Method::GET, &path, None, None).await;
    assert_eq!(status, StatusCode::OK);
    let stored: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(stored.state, DeleteTaskState::Done, "{stored:?}");
    assert_eq!(stored.error, None, "{stored:?}");
    assert_eq!(stored.rows_deleted, 1, "{stored:?}");
    assert_eq!(stored.files_rewritten, 1, "{stored:?}");

    assert_eq!(count_index_rows(&srv.ice, "logs", None).await, 2);
    assert_eq!(
        count_index_rows(&srv.ice, "logs", Some(&format!("attributes = '{gone}'"))).await,
        0,
        "the resubmitted request must actually delete the matching row"
    );
    assert_eq!(
        count_index_rows(&srv.ice, "logs", Some(&format!("attributes = '{stay}'"))).await,
        1
    );
    assert_eq!(
        count_index_rows(&srv.ice, "logs", Some("attributes IS NULL")).await,
        1,
        "the NULL-valued nonmatch must survive the recovery too"
    );

    // The failure history is the audit trail: both ids stay, and the old record
    // is byte-identical to what a pre-fix build left — no retro-transition, no
    // error clearing, no accounting borrowed from the successful run.
    let reopened = IcebergContext::open(&srv.warehouse).await.unwrap();
    assert_eq!(
        reopened.get_delete_task(failed.task_id).await.unwrap(),
        Some(failed.clone()),
        "the Failed record and its error must survive the recovery unchanged"
    );
    let (status, body) = request_json(
        &srv.app,
        Method::GET,
        "/api/v1/delete-tasks?index_id=logs",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let listed: Vec<DeleteTask> = serde_json::from_value(body.unwrap()).unwrap();
    let mut ids: Vec<uuid::Uuid> = listed.iter().map(|task| task.task_id).collect();
    ids.sort();
    let mut expected = vec![failed.task_id, resubmitted.task_id];
    expected.sort();
    assert_eq!(ids, expected, "both ids must remain listed: {listed:?}");
}

/// Resubmission is recovery, not retry semantics: nothing about the second POST
/// is idempotent at the HTTP layer (it creates another task), but on an
/// UNCHANGED dataset with the same predicate and the same fixed bounds the
/// extra task is a no-op — no candidate matches, so no file is rewritten and no
/// rewrite snapshot is committed. Each task reports only its own execution: the
/// first still says one row, the second says zero. An operator who resubmits
/// because they cannot tell whether the first run landed therefore pays a scan,
/// not a second deletion and not a needless snapshot.
#[tokio::test]
async fn delete_tasks_rest_a_second_submission_over_an_unchanged_dataset_is_a_no_op() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("logs");
    srv.ice.create_index(&config).await.unwrap();

    let now = Utc::now();
    append_index_events(
        &srv.ice,
        &config,
        &[
            log_event(now - ChronoDuration::hours(24), "victim", "victim row"),
            log_event(now - ChronoDuration::hours(23), "keep", "kept row"),
        ],
    )
    .await;

    // Fixed bounds, evaluated once: a time-dependent predicate would make the
    // two submissions different requests.
    let request = serde_json::json!({
        "index_id": "logs",
        "predicate_sql": "host = 'victim'",
        "start_ts": (now - ChronoDuration::hours(36)).to_rfc3339(),
        "end_ts": (now - ChronoDuration::hours(12)).to_rfc3339(),
    });

    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/delete-tasks",
        Some(request.clone()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let first: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();
    let outcome = srv.ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(outcome.rows_deleted, 1, "{outcome:?}");
    assert_eq!(outcome.files_rewritten, 1, "{outcome:?}");

    let table_ident = srv.ice.index_table_ident("logs");
    let snapshots_after_first = srv.ice.snapshot_count_for(&table_ident).await.unwrap();
    let paths_after_first: Vec<String> = srv
        .ice
        .live_data_files(&table_ident)
        .await
        .unwrap()
        .into_iter()
        .map(|file| file.file_path().to_string())
        .collect();

    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/delete-tasks",
        Some(request),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let second: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();
    assert_ne!(second.task_id, first.task_id);

    let outcome = srv.ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(outcome.tasks_examined, 1, "{outcome:?}");
    assert_eq!(outcome.tasks_completed, 1, "{outcome:?}");
    assert_eq!(
        outcome.files_rewritten, 0,
        "nothing matches any more: {outcome:?}"
    );
    assert_eq!(outcome.rows_deleted, 0, "{outcome:?}");

    for (task_id, rows, files) in [
        (first.task_id, 1u64, 1usize),
        (second.task_id, 0u64, 0usize),
    ] {
        let path = format!("/api/v1/delete-tasks/{task_id}");
        let (status, body) = request_json(&srv.app, Method::GET, &path, None, None).await;
        assert_eq!(status, StatusCode::OK);
        let stored: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();
        assert_eq!(stored.state, DeleteTaskState::Done, "{stored:?}");
        assert_eq!(
            (stored.rows_deleted, stored.files_rewritten),
            (rows, files),
            "each task reports its own execution, not the request's cumulative effect: {stored:?}"
        );
    }

    assert_eq!(
        srv.ice.snapshot_count_for(&table_ident).await.unwrap(),
        snapshots_after_first,
        "a no-op delete task must not commit a rewrite snapshot"
    );
    let paths_after_second: Vec<String> = srv
        .ice
        .live_data_files(&table_ident)
        .await
        .unwrap()
        .into_iter()
        .map(|file| file.file_path().to_string())
        .collect();
    assert_eq!(paths_after_second, paths_after_first, "no file may move");
    assert_eq!(count_index_rows(&srv.ice, "logs", None).await, 1);
    assert_eq!(
        count_index_rows(&srv.ice, "logs", Some("host = 'keep'")).await,
        1
    );
}

/// Two replicas acknowledge a deletion request at the same instant: one over
/// the REST surface, one from an independent context on the same warehouse.
/// Every 201 the API returned must still be listed and fetchable afterwards.
///
/// THE DEFECT THIS GUARDS. The packaged query deployment is a 2-replica
/// StatefulSet. With the v1 single-document ledger both replicas read it,
/// appended their own task and replaced it, so two acknowledged GDPR requests
/// left one behind — and the API had already answered 201 for both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_tasks_rest_acknowledgement_survives_a_concurrent_replica() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("logs");
    srv.ice.create_index(&config).await.unwrap();

    let replica = IcebergContext::open(&srv.warehouse).await.unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let over_rest = {
        let barrier = barrier.clone();
        let app = srv.app.clone();
        async move {
            barrier.wait().await;
            request_json(
                &app,
                Method::POST,
                "/api/v1/delete-tasks",
                Some(serde_json::json!({
                    "index_id": "logs",
                    "predicate_sql": "host = 'rest'"
                })),
                None,
            )
            .await
        }
    };
    let on_the_peer = {
        let barrier = barrier.clone();
        async move {
            barrier.wait().await;
            replica
                .create_delete_task("logs", "host = 'peer'", None, None)
                .await
                .unwrap()
        }
    };
    let ((status, body), peer_task) = tokio::join!(over_rest, on_the_peer);
    assert_eq!(status, StatusCode::CREATED);
    let rest_task: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();

    let (status, body) = request_json(
        &srv.app,
        Method::GET,
        "/api/v1/delete-tasks?index_id=logs",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let listed: Vec<DeleteTask> = serde_json::from_value(body.unwrap()).unwrap();
    assert_eq!(
        listed.len(),
        2,
        "both acknowledged deletion requests must be listed: {listed:?}"
    );
    for task_id in [rest_task.task_id, peer_task.task_id] {
        let path = format!("/api/v1/delete-tasks/{task_id}");
        let (status, body) = request_json(&srv.app, Method::GET, &path, None, None).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "acknowledged task {task_id} is gone"
        );
        let stored: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();
        assert_eq!(stored.state, DeleteTaskState::Pending);
    }

    // Executing one of them must not take the other with it, and both stay
    // fetchable from a context opened after the fact (nothing is memoized).
    let outcome = srv.ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(outcome.tasks_examined, 2);
    assert_eq!(outcome.tasks_completed, 2);

    let reopened = IcebergContext::open(&srv.warehouse).await.unwrap();
    for task_id in [rest_task.task_id, peer_task.task_id] {
        let stored = reopened.get_delete_task(task_id).await.unwrap();
        assert_eq!(
            stored.map(|task| task.state),
            Some(DeleteTaskState::Done),
            "task {task_id} did not survive the sweep"
        );
    }
}

#[tokio::test]
async fn delete_tasks_validation_and_dry_run_reject_and_preserve_state() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("logs");
    srv.ice.create_index(&config).await.unwrap();
    let now = Utc::now();
    append_index_events(
        &srv.ice,
        &config,
        &[log_event(
            now - ChronoDuration::hours(1),
            "victim",
            "candidate",
        )],
    )
    .await;

    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/delete-tasks",
        Some(serde_json::json!({
            "index_id": "logs",
            "predicate_sql": "host IN (SELECT host FROM logs)"
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.unwrap()["error"]
        .as_str()
        .unwrap()
        .contains("must not contain subqueries"));

    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/delete-tasks",
        Some(serde_json::json!({
            "index_id": "logs",
            "predicate_sql": "host = 'victim'"
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let task: DeleteTask = serde_json::from_value(body.unwrap()).unwrap();

    let preview = srv.ice.preview_delete_tasks("logs").await.unwrap();
    assert_eq!(preview.tasks_completed, 1);
    assert_eq!(preview.rows_deleted, 1);
    assert_eq!(
        count_index_rows(&srv.ice, "logs", Some("host = 'victim'")).await,
        1
    );
    let stored = srv
        .ice
        .get_delete_task(task.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.state,
        DeleteTaskState::Pending,
        "dry-run must not advance the ledger"
    );
}

/// A typed group column must come back as its own type, and sort as its own
/// type, whichever path serves it.
///
/// THE DEFECT THIS GUARDS. Group-count footers store keys as the cast-to-Utf8
/// rendering of the column, and the group-count fast path handed those strings
/// straight to `serde_json` and sorted them with a string comparator. Two
/// consequences on any `type: "long"` index field or promoted Int64 column:
///
///   - the JSON changed type depending on which path served it -- `"404"` from
///     the fast path where the planner's Arrow writer emits `404`. The sibling
///     test above asserts `as_i64() == Some(200)` on the planner path, so the
///     two demonstrably disagreed about the same query's shape.
///   - `ORDER BY <group> LIMIT n` returned the LEXICOGRAPHICALLY smallest
///     values. With 99, 100, 200, 404, 500 present, a LIMIT 3 answered
///     100, 200, 404 -- wrong rows, not merely wrong order, because the LIMIT
///     cuts against the wrong ordering.
///
/// The date-histogram fast path had avoided this all along by round-tripping
/// through `batches_to_records` "so the JSON is byte-identical to the
/// non-fast-path result"; the group-count path hand-built its rows instead.
#[tokio::test]
async fn typed_group_column_is_rendered_and_ordered_as_its_type() {
    let srv = spawn(AuthConfig::open()).await;
    let mut config = logs_config("typed-idx");
    config
        .doc_mapping
        .field_mappings
        .push(field("status", FieldType::Long, false));
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Chosen so text order and numeric order disagree: as text the ascending
    // sequence is 100, 200, 404, 500, 99.
    let now = Utc::now();
    let events: Vec<Event> = [99i64, 100, 200, 404, 500]
        .into_iter()
        .map(|code| {
            let mut e = log_event(now, "web", "line");
            e.attributes = Some(format!(r#"{{"status":{code}}}"#));
            e
        })
        .collect();
    let carrier = events_to_record_batch(&events).unwrap();
    let mapped = siglake_core::mapping::map_carrier_batch(&carrier, &config).unwrap();
    srv.ice
        .append_to_table(&srv.ice.index_table_ident("typed-idx"), mapped, &[])
        .await
        .unwrap();

    let ask = |sql: &'static str| {
        let app = srv.app.clone();
        async move {
            let (status, body) = request_json(
                &app,
                Method::POST,
                "/api/v1/sql",
                Some(serde_json::json!({ "query": sql })),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{sql}: {body:?}");
            body.unwrap()["rows"].clone()
        }
    };

    // Rendered as a NUMBER, not a string.
    let rows = ask("SELECT status, count(*) AS n FROM \"typed-idx\" GROUP BY status").await;
    for row in rows.as_array().expect("rows array") {
        assert!(
            row["status"].is_i64(),
            "group key came back as {} rather than a number: {row}",
            if row["status"].is_string() {
                "a string"
            } else {
                "some other type"
            }
        );
    }

    // Ordered NUMERICALLY, so the LIMIT keeps the right rows.
    let rows = ask(
        "SELECT status, count(*) AS n FROM \"typed-idx\" GROUP BY status ORDER BY status LIMIT 3",
    )
    .await;
    let got: Vec<i64> = rows
        .as_array()
        .expect("rows array")
        .iter()
        .map(|r| r["status"].as_i64().expect("numeric group key"))
        .collect();
    assert_eq!(
        got,
        vec![99, 100, 200],
        "ORDER BY on a typed group column sorted the footer keys as text"
    );
}

/// A managed index reachable only through a PREDICATE subquery must still be
/// registered before planning.
///
/// THE DEFECT THIS PINS (task #1527). Registration registers exactly the tables
/// the request's SQL names, and discovery walked only FROM relations plus a
/// bare projection subquery. A second index used solely inside
/// `WHERE EXISTS (...)`, `WHERE ... IN (...)`, a JOIN condition or HAVING was
/// therefore never registered, and DataFusion refused the query at planning
/// with "table not found" — an ordinary, supported SQL shape failing before it
/// ever executed. Both spellings below name `events` in FROM and the index
/// nowhere else.
#[tokio::test]
async fn index_used_only_in_a_predicate_subquery_is_registered() {
    let srv = spawn(AuthConfig::open()).await;
    let config = logs_config("predicate-only");
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/indexes",
        Some(serde_json::to_value(&config).unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let now = Utc::now();
    append_index_events(
        &srv.ice,
        &config,
        &[
            log_event(now, "shared-host", "in the index"),
            log_event(now, "index-only-host", "in the index"),
        ],
    )
    .await;
    srv.ice
        .append_events(&[
            log_event(now, "shared-host", "in events"),
            log_event(now, "events-only-host", "in events"),
        ])
        .await
        .unwrap();

    // EXISTS: the index is named in the predicate and nowhere else.
    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({
            "query": "SELECT count(*) AS n FROM events \
                      WHERE EXISTS (SELECT 1 FROM \"predicate-only\")"
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert_eq!(body.unwrap()["rows"][0]["n"], 2);

    // IN: and the answer genuinely joins the two tables — only `shared-host`
    // appears in both, so a wrongly-scoped read would not produce 1.
    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({
            "query": "SELECT count(*) AS n FROM events \
                      WHERE host IN (SELECT host FROM \"predicate-only\")"
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert_eq!(body.unwrap()["rows"][0]["n"], 1);

    // HAVING, over the same pair.
    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({
            "query": "SELECT host, count(*) AS n FROM events GROUP BY host \
                      HAVING count(*) >= (SELECT count(*) FROM \"predicate-only\") - 1"
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert_eq!(body.unwrap()["row_count"], 2);

    // An unknown table inside a predicate subquery still fails closed, with the
    // registration path's clear message rather than a planner error.
    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({
            "query": "SELECT count(*) FROM events WHERE EXISTS (SELECT 1 FROM nonexistent_table)"
        })),
        None,
    )
    .await;
    assert_ne!(status, StatusCode::OK);
    let msg = body.unwrap()["error"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        msg.contains("nonexistent_table"),
        "error should name the missing table: {msg}"
    );
}
