//! Ordered-browse early-stop regression suite (from the 07-16 windowed-
//! browse 413 diagnosis): reversed windowed browses must stay page-bounded
//! across writer provenances (ingest, streaming compactor) and table kinds
//! (events, user index). Each test asserts a hard rows-scanned ceiling.
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use chrono::{Duration, TimeZone, Utc};
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig, QueryScanConfig};
use siglake_storage::iceberg::{IcebergContext, ReclusterMergeOptions};
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

#[tokio::test]
async fn interior_window_reversed_browse_attribution() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    // One converged file: 400k rows over 400k seconds (several row groups).
    let mut all = Vec::with_capacity(400_000);
    for i in 0..400_000i64 {
        let mut e = Event::now(format!("row {i}"));
        e.timestamp = base + Duration::seconds(i);
        all.push(e);
    }
    let batch = siglake_core::events_to_record_batch(&all).unwrap();
    ice.append_batch(batch).await.unwrap();

    let app = router(AppState::new(ice, AuthConfig::open()));
    // Interior window: rows 100k..200k, DESC LIMIT 100.
    let lo = (base + Duration::seconds(100_000)).to_rfc3339();
    let hi = (base + Duration::seconds(200_000)).to_rfc3339();
    let sql = format!(
        "SELECT \"timestamp\", raw FROM events WHERE \"timestamp\" >= '{lo}' AND \"timestamp\" < '{hi}' ORDER BY \"timestamp\" DESC LIMIT 100"
    );
    let (status, body) = request_json(&app, serde_json::json!({ "query": sql })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["row_count"].as_u64(), Some(100), "{body}");
    let rows_scanned = body["stats"]["rows_scanned"].as_u64().unwrap();
    let scan = &body["stats"]["scan"];
    eprintln!(
        "ATTRIBUTION rows_scanned={} rows_pruned_selection={} row_groups={}/{} decoded_bytes={}",
        rows_scanned,
        scan["rows_pruned_selection"],
        scan["row_groups_read"],
        scan["row_groups_considered"],
        scan["decoded_bytes"],
    );
    assert!(
        rows_scanned < 150_000,
        "interior-window reversed browse must not decode the whole file: scanned {rows_scanned}: {}",
        body["stats"]
    );
}

/// Same shape, but the file under the window is COMPACTOR-CONVERGED via the
/// streaming k-way merge (the live 200G giant straddlers' provenance): 8
/// fully-overlapping appends re-clustered into one file, interior window,
/// reversed browse.
#[tokio::test]
async fn interior_window_reversed_browse_on_converged_file() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 30, 0).unwrap();
    // 800k rows over ~80,000s (one day partition), written as 8 interleaved
    // appends so every file spans the whole range (max overlap).
    for j in 0..8i64 {
        let mut events = Vec::with_capacity(100_000);
        for i in 0..100_000i64 {
            let k = i * 8 + j;
            let mut e = Event::now(format!("row {k}"));
            e.timestamp = base + Duration::milliseconds(k * 100);
            events.push(e);
        }
        ice.append_events(&events).await.unwrap();
    }
    let ident = ice.events_table_ident().clone();
    let files = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(files.len(), 8);
    let merge = ReclusterMergeOptions {
        force_streaming: Some(true),
        ..ReclusterMergeOptions::default()
    };
    let stats = ice
        .recluster_files_with(
            &ident,
            files,
            siglake_storage::iceberg::BLOOM_FILTER_COLUMNS,
            &merge,
        )
        .await
        .unwrap();
    eprintln!(
        "converged: removed={} added={} rows={}",
        stats.files_removed, stats.files_added, stats.rows
    );

    let app = router(AppState::new(ice, AuthConfig::open()));
    // Interior window: global rows 300k..400k (t = 30,000s..40,000s).
    let lo = (base + Duration::seconds(30_000)).to_rfc3339();
    let hi = (base + Duration::seconds(40_000)).to_rfc3339();
    let sql = format!(
        "SELECT \"timestamp\", raw FROM events WHERE \"timestamp\" >= '{lo}' AND \"timestamp\" < '{hi}' ORDER BY \"timestamp\" DESC LIMIT 100"
    );
    let (status, body) = request_json(&app, serde_json::json!({ "query": sql })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["row_count"].as_u64(), Some(100), "{body}");
    let rows_scanned = body["stats"]["rows_scanned"].as_u64().unwrap();
    let scan = &body["stats"]["scan"];
    eprintln!(
        "ATTRIBUTION-CONVERGED rows_scanned={} rows_pruned_selection={} row_groups={}/{} pruned_stats={} decoded_bytes={}",
        rows_scanned,
        scan["rows_pruned_selection"],
        scan["row_groups_read"],
        scan["row_groups_considered"],
        scan["row_groups_pruned_stats"],
        scan["decoded_bytes"],
    );
    assert!(
        rows_scanned < 200_000,
        "interior-window reversed browse on a converged file must not decode the whole file: scanned {rows_scanned}: {}",
        body["stats"]
    );
}

/// The LIVE shape: a USER INDEX (nullable timestamp — the aacbc66 nulls-order
/// find), schema evolved by a WS-7 promotion mid-corpus, overlapping files,
/// trailing-window `ORDER BY ts DESC LIMIT 100` (windowed_browse_last25).
#[tokio::test]
async fn trailing_window_reversed_browse_on_user_index() {
    use siglake_core::index_config::IndexConfig;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let config = IndexConfig {
        index_id: "logs-bench".into(),
        ..IndexConfig::builtin_events()
    };
    ice.create_index(&config).await.unwrap();
    let ident = ice.index_table_ident("logs-bench");
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 30, 0).unwrap();
    // Eight interleaved appends x 20k rows over ~4.4h. Every file spans the
    // whole range, giving the ordered scan eight planned files and an
    // eight-stream overlapping tail. The 256-byte payload keeps the projection
    // wide enough to exercise the production merge's batch lifetime.
    let raw_pad = "x".repeat(256);
    for j in 0..8i64 {
        let mut events = Vec::with_capacity(20_000);
        for i in 0..20_000i64 {
            let k = i * 8 + j;
            let mut e = Event::now(format!("row {k} {raw_pad}"));
            e.timestamp = base + Duration::milliseconds(k * 100);
            events.push(e);
        }
        let batch = siglake_core::events_to_record_batch(&events).unwrap();
        let mapped = siglake_core::mapping::map_carrier_batch(&batch, &config).unwrap();
        ice.append_to_table(&ident, mapped, &[]).await.unwrap();
    }

    // #4353: pin `target_partitions` ABOVE the file count so the balanced
    // split gives every file its own partition on any host. That singleton
    // shape is what run 83 planned (5 files, 5 partitions) and what used to
    // skip the small-LIMIT coalesce: each partition was drained separately for
    // a 15,360-row total here, against 8,192 once the files share one
    // partition's k-way merge. On a 4-core runner the unpinned split already
    // packed two files together and entered the coalesce, so without the pin
    // this regression only reproduces on a big host.
    let app = router(
        AppState::new(ice, AuthConfig::open()).with_query_scan(QueryScanConfig {
            target_partitions: Some(8),
            ..QueryScanConfig::default()
        }),
    );
    // Last quarter of the corpus, hi at/past the newest row (ceil'd).
    let span_ms = 160_000i64 * 100;
    let lo = (base + Duration::milliseconds(span_ms * 3 / 4)).to_rfc3339();
    let hi = (base + Duration::milliseconds(span_ms) + Duration::seconds(1)).to_rfc3339();
    let sql = format!(
        "SELECT \"timestamp\", raw FROM \"logs-bench\" WHERE \"timestamp\" >= '{lo}' AND \"timestamp\" < '{hi}' ORDER BY \"timestamp\" DESC LIMIT 100"
    );
    let (status, body) = request_json(&app, serde_json::json!({ "query": &sql })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["row_count"].as_u64(), Some(100), "{body}");
    let rows_scanned = body["stats"]["rows_scanned"].as_u64().unwrap();
    let scan = &body["stats"]["scan"];
    eprintln!(
        "ATTRIBUTION-INDEX rows_scanned={} rows_pruned_selection={} row_groups={}/{} decoded_bytes={}",
        rows_scanned,
        scan["rows_pruned_selection"],
        scan["row_groups_read"],
        scan["row_groups_considered"],
        scan["decoded_bytes"],
    );
    assert_eq!(
        body["stats"]["scan"]["ordering"].as_str(),
        Some("advertised"),
        "{}",
        body["stats"]
    );
    // Exactly the newest 100 rows, newest first.
    let got: Vec<chrono::DateTime<Utc>> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["timestamp"].as_str().unwrap().parse().unwrap())
        .collect();
    let want: Vec<chrono::DateTime<Utc>> = (159_900i64..=159_999)
        .rev()
        .map(|k| base + Duration::milliseconds(k * 100))
        .collect();
    assert_eq!(got, want, "browse must return the newest 100 rows in order");
    assert_eq!(
        body["stats"]["scan"]["files_planned"].as_u64(),
        Some(8),
        "fixture must retain the eight-file plan: {}",
        body["stats"]
    );
    assert_eq!(
        rows_scanned, 100,
        "the ordered source must stop with the LIMIT batch instead of emitting post-limit batches: {}",
        body["stats"]
    );

    let mut warm_ms = Vec::with_capacity(11);
    for i in 0..11 {
        let started = std::time::Instant::now();
        let warm_sql = format!("{sql} /* source-cap-warm-{i} */");
        let (status, warm) = request_json(&app, serde_json::json!({ "query": warm_sql })).await;
        warm_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
        assert_eq!(status, StatusCode::OK, "{warm}");
        assert_eq!(warm["stats"]["rows_scanned"].as_u64(), Some(100), "{warm}");
    }
    warm_ms.sort_by(f64::total_cmp);
    eprintln!(
        "ORDERED-SOURCE-CAP warm_p50_ms={:.3}",
        warm_ms[warm_ms.len() / 2]
    );
}

/// Ordered-plan cache: the SECOND identical browse serves from the cached
/// arrangement (result-cache disabled via distinct limits) and must stay
/// exact + advertised.
#[tokio::test]
async fn ordered_plan_cache_hit_stays_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 30, 0).unwrap();
    for j in 0..4i64 {
        let mut events = Vec::with_capacity(20_000);
        for i in 0..20_000i64 {
            let k = i * 4 + j;
            let mut e = Event::now(format!("row {k}"));
            e.timestamp = base + Duration::milliseconds(k * 100);
            events.push(e);
        }
        ice.append_batch(siglake_core::events_to_record_batch(&events).unwrap())
            .await
            .unwrap();
    }
    let app = router(AppState::new(ice, AuthConfig::open()));
    // Same shape twice with different LIMITs (distinct SQL dodges the result
    // cache; the ordered-plan cache keys on the small-limit BUCKET, so both
    // hit the same cached arrangement after the first populates it).
    let mut last_first_ts = Vec::new();
    for limit in [100, 101, 102] {
        let (status, body) = request_json(
            &app,
            serde_json::json!({
                "query": format!(
                    "SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" DESC LIMIT {limit}"
                )
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["stats"]["scan"]["ordering"].as_str(),
            Some("advertised"),
            "{}",
            body["stats"]
        );
        let ts: Vec<chrono::DateTime<Utc>> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["timestamp"].as_str().unwrap().parse().unwrap())
            .collect();
        assert_eq!(ts.len(), limit as usize);
        assert!(
            ts.windows(2).all(|w| w[0] >= w[1]),
            "cached-plan browse must stay ordered"
        );
        last_first_ts.push(ts[0]);
    }
    assert!(
        last_first_ts.windows(2).all(|w| w[0] == w[1]),
        "all runs must agree on the newest row: {last_first_ts:?}"
    );
}

/// #6020 end to end: a managed index whose mapping declares `ts` as its event
/// time. A BARE interactive SELECT is rewritten to browse that field
/// newest-first, the scan advertises the order (no blocking sort), and the rows
/// are the newest event times across two overlapping files whose LIMIT boundary
/// falls inside a group of equal `ts` values.
#[tokio::test]
async fn bare_browse_of_a_custom_event_time_index_is_newest_first() {
    use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
    use siglake_core::index_config::{
        DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode,
    };

    let datetime_field = |name: &str| FieldMapping {
        name: name.to_string(),
        field_type: FieldType::Datetime,
        required: true,
    };
    let config = IndexConfig {
        index_id: "ts-logs".to_string(),
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                datetime_field("ts"),
                // Unrelated to the event time, and deliberately named
                // `timestamp`: ordering by it would not be a time order.
                datetime_field("timestamp"),
                FieldMapping {
                    name: "message".to_string(),
                    field_type: FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    required: true,
                },
            ],
            timestamp_field: "ts".to_string(),
            tag_fields: Vec::new(),
            default_search_fields: vec!["message".to_string()],
        },
        retention: None,
        index_at_flush: None,
    };

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let ident = ice.create_index(&config).await.unwrap();
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let at = |secs: i64| base + Duration::seconds(secs);
    let append = |offsets: Vec<i64>| {
        let config = config.clone();
        let ice = ice.clone();
        let ident = ident.clone();
        async move {
            let micros: Vec<Option<i64>> = offsets
                .iter()
                .map(|secs| Some(at(*secs).timestamp_micros()))
                .collect();
            let batch = RecordBatch::try_new(
                config.to_arrow_schema(),
                vec![
                    Arc::new(
                        TimestampMicrosecondArray::from(micros.clone())
                            .with_timezone(siglake_core::TIMESTAMP_TZ),
                    ),
                    // The unrelated column runs the OTHER way, so a browse
                    // that ordered by it would return different rows.
                    Arc::new(
                        TimestampMicrosecondArray::from(
                            micros.iter().map(|m| m.map(|m| -m)).collect::<Vec<_>>(),
                        )
                        .with_timezone(siglake_core::TIMESTAMP_TZ),
                    ),
                    Arc::new(StringArray::from(
                        offsets
                            .iter()
                            .map(|secs| format!("row {secs}"))
                            .collect::<Vec<_>>(),
                    )),
                    // The dynamic mapping's overflow column.
                    Arc::new(StringArray::from(vec![None::<&str>; offsets.len()])),
                ],
            )
            .unwrap();
            ice.append_to_table(&ident, batch, &[]).await.unwrap();
        }
    };
    // Two OVERLAPPING files (10..30 and 20..40) with three rows at +20.
    append(vec![30, 10, 20, 20]).await;
    append(vec![40, 20, 35]).await;

    let app = router(AppState::new(ice, AuthConfig::open()));
    let (status, body) = request_json(
        &app,
        serde_json::json!({ "query": "SELECT ts, message FROM \"ts-logs\" LIMIT 5" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["row_count"].as_u64(), Some(5), "{body}");
    assert_eq!(
        body["stats"]["scan"]["ordering"].as_str(),
        Some("advertised"),
        "a bare browse of a `ts`-mapped index must reach the ordered scan: {}",
        body["stats"]
    );
    let got: Vec<chrono::DateTime<Utc>> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["ts"].as_str().unwrap().parse().unwrap())
        .collect();
    // The equal +20 rows are interchangeable at the boundary — a custom event
    // time has no `timestamp_ns` tiebreak — but the VALUES are determined.
    assert_eq!(
        got,
        vec![at(40), at(35), at(30), at(20), at(20)],
        "{}",
        body["rows"]
    );
}
