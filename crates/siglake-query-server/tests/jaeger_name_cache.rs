//! The Jaeger name-list cache must never answer a question it was not asked
//! (#2268), and must never change what the two list routes are allowed to say.
//!
//! THE DEFECT THIS FORECLOSES. `/api/v1/jaeger/{index}/api/services` and
//! `.../services/{service}/operations` issue BYTE-IDENTICAL SQL for every index
//! of every tenant: `jaeger_routes.rs` registers whichever index was asked for
//! under the one shared `traces` alias, so the query text is
//! `SELECT DISTINCT service FROM traces …` no matter whose spans it reads. A
//! key derived from the text the way `/api/v1/sql`'s is would serve one
//! tenant's service list to another tenant and one index's to another index.
//! The key is therefore the route's own — resolved `namespace.index_id`, the
//! snapshot the registered provider serves, the list, the service, the
//! ceilings — and everything below varies exactly one of those at a time
//! through the REAL router and reads the `outcome` counter on every step, so a
//! green run cannot be one where the cache was simply never consulted.
//!
//! WHAT THE ISOLATION ARMS DO AND DO NOT PROVE. A/B'd against four mutations of
//! `jaeger_routes.rs`: dropping the snapshot from the key fails arm 5, dropping
//! the service fails arm 2, and disabling the cache outright fails arm 1. The
//! fourth — keying on the shared `traces` alias instead of the resolved table —
//! does NOT fail this file, and cannot: `generate_unique_snapshot_id` is
//! `abs(uuid.hi ^ uuid.lo)` (`third_party/iceberg/src/transaction/snapshot.rs`),
//! so two tables never share a snapshot id and the snapshot field masks the
//! missing one. Arms 3 and 4 are therefore behavioural regressions — no index
//! and no tenant is ever served another's list, by whatever mechanism — and the
//! resolved table's own contribution to the key is pinned unit-side, in
//! `jaeger_routes::tests::every_ingredient_of_a_name_list_key_separates_two_answers`.
//!
//! And what the cache must NOT change: a name list past what one entry may
//! retain (512 KiB of arena since #2302, 128 rows before it) is executed and
//! returned whole rather than stored or truncated, a 413 stays a 413 with no
//! data, a refused or abandoned request leaves no single-flight marker behind,
//! and a context with result caches off executes every poll.
//!
//! ONE test function, and its own binary, on purpose: the result cache, the
//! single-flight map and the metrics recorder are all process-wide, so a
//! sibling test's queries would read as this test's cache traffic. Nothing here
//! reads or writes the environment — result caches are switched off by INJECTED
//! `IcebergTuning` (project convention: tests never `set_var`).

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, HeaderMap, Method, Request, StatusCode};
use axum::Router;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    any_value::Value as ProtoValue, AnyValue as ProtoAnyValue, KeyValue as ProtoKeyValue,
};
use opentelemetry_proto::tonic::resource::v1::Resource as ProtoResource;
use opentelemetry_proto::tonic::trace::v1::{
    span, ResourceSpans as ProtoResourceSpans, ScopeSpans as ProtoScopeSpans, Span as ProtoSpan,
};
use siglake_core::index_config::IndexConfig;
use siglake_core::{events_to_record_batch, map_carrier_batch};
use siglake_ingest::otlp_traces::otlp_proto_traces_to_events;
use siglake_query_server::{router, AppState, AuthConfig, QueryScanConfig, ServerLimits};
use siglake_storage::iceberg::{IcebergContext, IcebergTuning};
use siglake_storage::index_manager::builtin_traces_template;
use tower::util::ServiceExt;

/// The packaged pod's reservation: a 64 MiB admission budget at the default
/// per-query share divisor resolves `min(16 MiB, 64 MiB / 4)` = 16 MiB, which
/// is a 10,237-name ceiling. Every arm that must ANSWER runs here.
const PACKAGED_BUDGET: u64 = 64 * 1024 * 1024;
/// 3 MiB of reservation: a 731-name ceiling, which the `MANY` corpus below
/// crosses and every other corpus here fits inside.
const SMALL_BUDGET: u64 = 12 * 1024 * 1024;
/// Eight simultaneous pollers need eight reservations, and 64 MiB admits four.
/// The reservation itself is CAPPED at 16 MiB, so this raises admission
/// capacity without moving the ceilings — the concurrent arm therefore shares
/// its cache key with `PACKAGED_BUDGET`, which is the point of the arm.
const CONCURRENT_BUDGET: u64 = 512 * 1024 * 1024;

/// Two namespaces over ONE warehouse, the shape `TenantRegistry` routes OIDC
/// tenants into.
const ACME: &str = "tenant_acme";
const WIDGETS: &str = "tenant_widgets";

/// The same index id in both namespaces: the collision the resolved table in
/// the key exists to separate, since the SQL and the `traces` alias are equal.
const SHARED_INDEX: &str = "traces-shared";
/// A second index in ONE namespace: the other half of the same separation.
const OTHER_INDEX: &str = "traces-other";
/// Distinct services, one span each: over the 731-name ceiling a 3 MiB
/// reservation resolves, and — since #2302 — well INSIDE the name-list entry's
/// 512 KiB byte allowance, so this corpus is the large-but-cacheable one.
const MANY_INDEX: &str = "traces-many";
const MANY_SERVICES: usize = 800;

/// Past the 512 KiB a name-list entry may retain (#2302), and the only way to
/// get there under a packaged pod's 10,237-name render ceiling is with LONG
/// names: 6,000 x (96 + 4) bytes is 585.9 KiB of arena, where the 800 short
/// names above are 9.4 KiB. The arithmetic is exact — an arena is one buffer
/// plus one `u32` per name — so this fixture is over the allowance by
/// construction rather than by a measurement that could drift.
const OVERSIZED_INDEX: &str = "traces-oversized";
const OVERSIZED_SERVICES: usize = 6_000;
const OVERSIZED_NAME_LEN: usize = 96;

/// One oversized service name, padded to [`OVERSIZED_NAME_LEN`] bytes. ASCII,
/// so bytes and characters agree and the arena arithmetic above is readable.
fn oversized_service(i: usize) -> String {
    format!(
        "{:x<width$}",
        format!("svc-{i:04}"),
        width = OVERSIZED_NAME_LEN
    )
}

/// Cache-outcome counts SINCE THE LAST CALL (`snapshot()` drains), keyed by the
/// `outcome` label: `hit`, `miss`, `insert`, `wait`, `wait_timeout`, `evict`.
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

fn count(outcomes: &HashMap<String, u64>, outcome: &str) -> u64 {
    outcomes.get(outcome).copied().unwrap_or(0)
}

/// No poll may ever park on a single-flight marker that nobody will fire. Every
/// arm asserts this, because a leaked marker is invisible in the ANSWERS — the
/// waiter eventually times out and executes, so the list is still right and the
/// only symptom is a ten-second stall.
fn assert_never_parked(label: &str, outcomes: &HashMap<String, u64>) {
    assert_eq!(
        count(outcomes, "wait_timeout"),
        0,
        "{label}: a poll parked on a leaked single-flight marker until the cap: {outcomes:?}"
    );
}

fn proto_str(key: &str, value: &str) -> ProtoKeyValue {
    ProtoKeyValue {
        key: key.to_string(),
        value: Some(ProtoAnyValue {
            value: Some(ProtoValue::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

/// One commit's worth of spans: for each `(service, operations)` pair, one
/// trace carrying one span per operation. `seed` keeps trace and span ids
/// distinct across commits to the same index.
fn proto_fixture(seed: u64, services: &[(&str, &[&str])]) -> ExportTraceServiceRequest {
    let resource_spans = services
        .iter()
        .enumerate()
        .map(|(t, (service, operations))| {
            let trace = seed * 1_000_000 + t as u64;
            let spans = operations
                .iter()
                .enumerate()
                .map(|(i, operation)| {
                    let k = trace * 1_000 + i as u64;
                    let start = 1_700_000_000_000_000_000 + k * 700_000;
                    ProtoSpan {
                        trace_id: trace.to_be_bytes().repeat(2),
                        span_id: (k ^ 0xa5a5_a5a5_a5a5_a5a5).to_be_bytes().to_vec(),
                        name: (*operation).to_string(),
                        kind: span::SpanKind::Server as i32,
                        start_time_unix_nano: start,
                        end_time_unix_nano: start + 50_000_000,
                        attributes: vec![proto_str("payload", "x")],
                        ..Default::default()
                    }
                })
                .collect();
            ProtoResourceSpans {
                resource: Some(ProtoResource {
                    attributes: vec![proto_str("service.name", service)],
                    ..Default::default()
                }),
                scope_spans: vec![ProtoScopeSpans {
                    spans,
                    ..Default::default()
                }],
                ..Default::default()
            }
        })
        .collect();
    ExportTraceServiceRequest { resource_spans }
}

fn trace_index(index: &str) -> IndexConfig {
    IndexConfig {
        index_id: index.to_string(),
        doc_mapping: builtin_traces_template().doc_mapping,
        retention: None,
        index_at_flush: None,
    }
}

/// ONE commit, so the snapshot moves exactly once per call and the arms below
/// can count snapshots.
async fn append(ice: &IcebergContext, index: &str, seed: u64, services: &[(&str, &[&str])]) {
    let config = trace_index(index);
    let bloom_refs = config
        .doc_mapping
        .tag_fields
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let events = otlp_proto_traces_to_events(proto_fixture(seed, services));
    let carrier = events_to_record_batch(&events).unwrap();
    let mapped = map_carrier_batch(&carrier, &config).unwrap();
    ice.append_to_table(&ice.index_table_ident(index), mapped, &bloom_refs)
        .await
        .unwrap();
}

async fn create_and_append(
    ice: &IcebergContext,
    index: &str,
    seed: u64,
    services: &[(&str, &[&str])],
) {
    ice.create_index(&trace_index(index)).await.unwrap();
    append(ice, index, seed, services).await;
}

fn app_with_budget(ice: &Arc<IcebergContext>, admission_budget_bytes: u64) -> Router {
    router(
        AppState::new(Arc::clone(ice), AuthConfig::open())
            .with_limits(ServerLimits {
                admission_budget_bytes,
                admission_wait_timeout: std::time::Duration::from_millis(50),
                ..Default::default()
            })
            // One partition, one file at a time: a deterministic batch sequence
            // keeps the mid-flight ceiling arms arithmetic rather than a race.
            .with_query_scan(QueryScanConfig {
                target_partitions: Some(1),
                file_concurrency_limit: Some(1),
                ..Default::default()
            }),
    )
}

fn services_path(index: &str) -> String {
    format!("/api/v1/jaeger/{index}/api/services")
}

fn operations_path(index: &str, service: &str) -> String {
    format!("/api/v1/jaeger/{index}/api/services/{service}/operations")
}

fn request(path: &str) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(path)
        .body(Body::empty())
        .unwrap()
}

async fn get(app: &Router, path: &str) -> (StatusCode, HeaderMap, serde_json::Value) {
    let response = app.clone().oneshot(request(path)).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or_else(
        |_| serde_json::json!({ "raw_body": String::from_utf8_lossy(&bytes).to_string() }),
    );
    (status, headers, body)
}

/// The `data` array of a 200, as names, with `total` checked against it — the
/// two are independently derived in the handler, so a cached body that lost
/// rows on the way in would show up here.
async fn names(app: &Router, path: &str) -> Vec<String> {
    let (status, _, body) = get(app, path).await;
    assert_eq!(status, StatusCode::OK, "{path}: {body}");
    let data: Vec<String> = body["data"]
        .as_array()
        .unwrap_or_else(|| panic!("{path}: no data array in {body}"))
        .iter()
        .map(|v| v.as_str().expect("a name").to_string())
        .collect();
    assert_eq!(
        body["total"].as_u64(),
        Some(data.len() as u64),
        "{path}: total disagrees with data: {body}"
    );
    data
}

fn strs(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| (*s).to_string()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_jaeger_name_cache_never_answers_a_question_it_was_not_asked() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let default_ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    // Two tenants' namespaces over one warehouse and one catalog, exactly as
    // `TenantRegistry` routes them.
    let acme = Arc::new(default_ice.for_namespace(ACME).await.unwrap());
    let widgets = Arc::new(default_ice.for_namespace(WIDGETS).await.unwrap());

    // `SHARED_INDEX` exists in BOTH namespaces with different contents: the two
    // requests that must not collide are byte-identical in SQL, in alias and in
    // index id, and differ only in the namespace the handler resolved.
    create_and_append(
        &acme,
        SHARED_INDEX,
        1,
        &[("checkout", &["op-a", "op-b"]), ("shipping", &["op-ship"])],
    )
    .await;
    create_and_append(&widgets, SHARED_INDEX, 1, &[("warehouse", &["op-pick"])]).await;
    create_and_append(&acme, OTHER_INDEX, 1, &[("billing", &["op-charge"])]).await;

    let many: Vec<String> = (0..MANY_SERVICES).map(|s| format!("svc-{s:04}")).collect();
    let many_ops: Vec<&[&str]> = vec![&["op-only"]; MANY_SERVICES];
    let many_fixture: Vec<(&str, &[&str])> = many
        .iter()
        .map(String::as_str)
        .zip(many_ops.iter().copied())
        .collect();
    create_and_append(&acme, MANY_INDEX, 1, &many_fixture).await;

    let oversized: Vec<String> = (0..OVERSIZED_SERVICES).map(oversized_service).collect();
    let oversized_ops: Vec<&[&str]> = vec![&["op-only"]; OVERSIZED_SERVICES];
    let oversized_fixture: Vec<(&str, &[&str])> = oversized
        .iter()
        .map(String::as_str)
        .zip(oversized_ops.iter().copied())
        .collect();
    create_and_append(&acme, OVERSIZED_INDEX, 1, &oversized_fixture).await;

    let acme_app = app_with_budget(&acme, PACKAGED_BUDGET);
    let widgets_app = app_with_budget(&widgets, PACKAGED_BUDGET);

    // `snapshot()` DRAINS, so every read below is the delta since the previous.
    let _ = outcomes(&snapshotter);

    // ---- 1. A repeat on a standing snapshot is served from the cache -------
    //
    // The whole point of the card: before it, every poll of these two routes
    // re-ran the unpredicated aggregate because they call `plan_client_sql` and
    // the mid-flight collector directly and never enter the SQL handler that
    // owns the cache.
    let cold = names(&acme_app, &services_path(SHARED_INDEX)).await;
    assert_eq!(cold, strs(&["checkout", "shipping"]));
    let first = outcomes(&snapshotter);
    assert_eq!(count(&first, "miss"), 1, "{first:?}");
    assert_eq!(count(&first, "insert"), 1, "{first:?}");
    assert_eq!(count(&first, "hit"), 0, "{first:?}");

    for poll in 2..=4 {
        let warm = names(&acme_app, &services_path(SHARED_INDEX)).await;
        assert_eq!(warm, cold, "poll {poll} disagreed with the cold answer");
    }
    let warm = outcomes(&snapshotter);
    assert_eq!(
        count(&warm, "hit"),
        3,
        "a repeated poll on a standing snapshot still re-executed the aggregate: {warm:?}"
    );
    assert_eq!(count(&warm, "miss"), 0, "{warm:?}");
    assert_never_parked("warm services", &warm);

    // ---- 2. The two lists, and two services, are different questions -------
    let checkout = names(&acme_app, &operations_path(SHARED_INDEX, "checkout")).await;
    assert_eq!(
        checkout,
        strs(&["op-a", "op-b"]),
        "the warmed SERVICE list was served as an operation list"
    );
    let shipping = names(&acme_app, &operations_path(SHARED_INDEX, "shipping")).await;
    assert_eq!(
        shipping,
        strs(&["op-ship"]),
        "one service's warmed operations were served for another service"
    );
    let lists = outcomes(&snapshotter);
    assert_eq!(
        count(&lists, "hit"),
        0,
        "an operation list was answered from another key's entry: {lists:?}"
    );
    assert_eq!(count(&lists, "miss"), 2, "{lists:?}");
    // …and each keeps its own entry, rather than one displacing the other.
    assert_eq!(
        names(&acme_app, &operations_path(SHARED_INDEX, "checkout")).await,
        checkout
    );
    assert_eq!(
        names(&acme_app, &operations_path(SHARED_INDEX, "shipping")).await,
        shipping
    );
    assert_eq!(names(&acme_app, &services_path(SHARED_INDEX)).await, cold);
    let separate = outcomes(&snapshotter);
    assert_eq!(
        count(&separate, "hit"),
        3,
        "the three name lists do not each hold their own entry: {separate:?}"
    );

    // ---- 3. Index isolation, inside one namespace --------------------------
    let other = names(&acme_app, &services_path(OTHER_INDEX)).await;
    assert_eq!(
        other,
        strs(&["billing"]),
        "another index's warmed service list was served: the key is not the \
         resolved table"
    );
    let other_ops = names(&acme_app, &operations_path(OTHER_INDEX, "billing")).await;
    assert_eq!(other_ops, strs(&["op-charge"]));
    let by_index = outcomes(&snapshotter);
    assert_eq!(count(&by_index, "hit"), 0, "{by_index:?}");
    assert_eq!(count(&by_index, "miss"), 2, "{by_index:?}");

    // ---- 4. Tenant isolation, same index id, byte-identical SQL ------------
    //
    // THE headline defect. `tenant_widgets` asks the same route for the same
    // index id and the handler issues the same string against the same `traces`
    // alias; only the namespace the identity resolved differs.
    let tenant = names(&widgets_app, &services_path(SHARED_INDEX)).await;
    assert_eq!(
        tenant,
        strs(&["warehouse"]),
        "one tenant was served another tenant's service list for the same \
         index id"
    );
    let tenant_ops = names(&widgets_app, &operations_path(SHARED_INDEX, "warehouse")).await;
    assert_eq!(tenant_ops, strs(&["op-pick"]));
    // The other tenant asking for a service it does not have gets an EMPTY
    // list, not the neighbour's operations.
    let absent = names(&widgets_app, &operations_path(SHARED_INDEX, "checkout")).await;
    assert!(
        absent.is_empty(),
        "a tenant with no `checkout` service was served the other tenant's \
         operations: {absent:?}"
    );
    let by_tenant = outcomes(&snapshotter);
    assert_eq!(
        count(&by_tenant, "hit"),
        0,
        "a cross-tenant hit: the key is not the resolved namespace: {by_tenant:?}"
    );
    assert_eq!(count(&by_tenant, "miss"), 3, "{by_tenant:?}");
    // And the first tenant's answers are unchanged by the second's traffic.
    assert_eq!(names(&acme_app, &services_path(SHARED_INDEX)).await, cold);
    assert_eq!(
        names(&acme_app, &operations_path(SHARED_INDEX, "checkout")).await,
        checkout
    );
    assert_eq!(count(&outcomes(&snapshotter), "hit"), 2);

    // ---- 5. A commit is what invalidates, and the only thing that does -----
    //
    // Snapshot-keyed, never TTL-expired (standing invariant): a new entry
    // appears because the snapshot moved, and the answer at the new snapshot is
    // complete on the FIRST poll after the commit. The defect this pins is the
    // one `register_table_returning_generation` closes — registering the
    // provider and then asking `current_table_generation` can return a
    // generation NEWER than the provider just registered (the entry is refreshed
    // on commit and in the background), which files this snapshot's answer under
    // the next one's key, where no commit will ever displace it.
    let mut expected = vec!["checkout".to_string(), "shipping".to_string()];
    for round in 0..3u64 {
        let fresh = format!("late-{round}");
        append(
            &acme,
            SHARED_INDEX,
            100 + round,
            &[(fresh.as_str(), &["op-late"])],
        )
        .await;
        expected.push(fresh.clone());
        expected.sort();

        // FIRST poll after the commit: a new key, executed, and complete.
        let after = names(&acme_app, &services_path(SHARED_INDEX)).await;
        assert_eq!(
            after, expected,
            "round {round}: the first poll after a commit did not see the new \
             service — an answer was filed under a snapshot it does not describe"
        );
        let commit = outcomes(&snapshotter);
        assert_eq!(
            count(&commit, "miss"),
            1,
            "round {round}: a commit did not produce a new key: {commit:?}"
        );
        assert_eq!(count(&commit, "hit"), 0, "round {round}: {commit:?}");

        // SECOND poll: the new snapshot's own entry, not the old one's.
        let again = names(&acme_app, &services_path(SHARED_INDEX)).await;
        assert_eq!(again, expected, "round {round}");
        let repeat = outcomes(&snapshotter);
        assert_eq!(
            count(&repeat, "hit"),
            1,
            "round {round}: the post-commit snapshot does not cache: {repeat:?}"
        );
        assert_never_parked(&format!("round {round}"), &repeat);
    }

    // ---- 6. Concurrent pollers execute the aggregate once ------------------
    //
    // The Jaeger UI polls both routes from every open tab. Single-flight is the
    // shared machinery's, and this is the evidence it reaches this caller: one
    // cold key, eight simultaneous pollers, at most one execution.
    append(&acme, SHARED_INDEX, 200, &[("burst", &["op-burst"])]).await;
    expected.push("burst".to_string());
    expected.sort();
    let burst_app = app_with_budget(&acme, CONCURRENT_BUDGET);
    let mut concurrent = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let app = burst_app.clone();
        let path = services_path(SHARED_INDEX);
        concurrent.spawn(async move { names(&app, &path).await });
    }
    while let Some(joined) = concurrent.join_next().await {
        assert_eq!(joined.unwrap(), expected, "a concurrent poller disagreed");
    }
    let burst = outcomes(&snapshotter);
    assert_eq!(
        count(&burst, "miss"),
        1,
        "eight simultaneous pollers of one cold key executed the aggregate more \
         than once: {burst:?}"
    );
    // The other seven are served from the leader's entry. A follower that
    // parked counts BOTH: `wait` when it registers on the leader's `Notify`,
    // then `hit` when it is woken and re-probes. How many park is a scheduling
    // detail (one that arrives after the insert goes straight to `hit`), so the
    // invariant is the hits — no follower may execute, and none may be handed
    // nothing.
    assert_eq!(
        count(&burst, "hit"),
        7,
        "the other seven pollers were not served the leader's answer: {burst:?}"
    );
    assert!(
        count(&burst, "wait") <= 7,
        "more waiters than pollers: {burst:?}"
    );
    assert_never_parked("concurrent pollers", &burst);

    // ---- 7. An oversized list is executed and returned WHOLE, never stored -
    //
    // 6,000 names of 96 bytes is 585.9 KiB of arena, past the 512 KiB one
    // name-list entry may retain (#2302). The contract is the one #2268 wrote
    // and #2302 kept, at the new threshold: the list bypasses the cache — it is
    // not truncated to fit, and neither the caller's allowance nor SQL's global
    // caps are raised to admit it.
    for poll in 1..=3 {
        let all = names(&acme_app, &services_path(OVERSIZED_INDEX)).await;
        assert_eq!(
            all.len(),
            OVERSIZED_SERVICES,
            "poll {poll}: an oversized name list was truncated to fit the cache"
        );
        assert_eq!(all.first(), Some(&oversized_service(0)));
        assert_eq!(all.last(), Some(&oversized_service(OVERSIZED_SERVICES - 1)));
    }
    let bypassed = outcomes(&snapshotter);
    assert_eq!(
        count(&bypassed, "insert"),
        0,
        "a name list over the 512 KiB entry allowance was stored: {bypassed:?}"
    );
    assert_eq!(
        count(&bypassed, "hit"),
        0,
        "an oversized list was served from the cache: {bypassed:?}"
    );
    assert_eq!(count(&bypassed, "miss"), 3, "{bypassed:?}");
    assert_never_parked("oversized bypass", &bypassed);

    // ---- 7b. …and the list that used to be oversized now caches ------------
    //
    // 800 names was refused entry until #2302, on a ROW cap that priced a
    // one-column list of short strings wrong by two orders of magnitude: the
    // list is 9.4 KiB of arena — 1.8% of this caller's allowance — and every
    // poll of it re-ran a 17–41 ms unpredicated aggregate on a snapshot that
    // never moved. Both routes, because operations was the more expensive of
    // the two at high cardinality.
    for (label, path) in [
        ("services", services_path(MANY_INDEX)),
        ("operations", operations_path(MANY_INDEX, "svc-0000")),
    ] {
        let cold = names(&acme_app, &path).await;
        let first = outcomes(&snapshotter);
        assert_eq!(count(&first, "miss"), 1, "{label}: {first:?}");
        assert_eq!(
            count(&first, "insert"),
            1,
            "{label}: a list inside the byte allowance was not stored: {first:?}"
        );
        let warm = names(&acme_app, &path).await;
        assert_eq!(
            warm, cold,
            "{label}: the cached list is not the list that was executed"
        );
        let repeat = outcomes(&snapshotter);
        assert_eq!(
            count(&repeat, "hit"),
            1,
            "{label}: a repeat poll of an {MANY_SERVICES}-name list still \
             re-executed the aggregate: {repeat:?}"
        );
        assert_eq!(count(&repeat, "miss"), 0, "{label}: {repeat:?}");
        assert_never_parked(label, &repeat);
    }
    // The service list itself, whole and in order, on both polls.
    let cached = names(&acme_app, &services_path(MANY_INDEX)).await;
    assert_eq!(cached, many, "the cached {MANY_SERVICES}-name list");
    assert_eq!(count(&outcomes(&snapshotter), "hit"), 1);

    // ---- 8. The whole-response 413 is unchanged, and leaves nothing behind -
    //
    // The oversized corpus, a 3 MiB reservation: 6,000 names against a
    // 731-name ceiling.
    // The refusal is mid-flight, so the leader returns through `?` without ever
    // reaching the insert — the path that used to leak a single-flight marker
    // and turn every later identical poll into a ten-second park.
    let small = app_with_budget(&acme, SMALL_BUDGET);
    for attempt in 1..=2 {
        let refused_at = std::time::Instant::now();
        let (status, headers, body) = get(&small, &services_path(OVERSIZED_INDEX)).await;
        assert_eq!(
            status,
            StatusCode::PAYLOAD_TOO_LARGE,
            "attempt {attempt}: {body}"
        );
        assert!(
            body.get("data").is_none(),
            "attempt {attempt}: a refusal returned partial data: {body}"
        );
        assert!(
            headers.get(header::RETRY_AFTER).is_none(),
            "attempt {attempt}: a deterministic refusal must advertise no retry"
        );
        assert!(
            body["error"]
                .as_str()
                .unwrap_or_default()
                .contains("731 names"),
            "attempt {attempt}: the name refusal lost its unit and ceiling: {body}"
        );
        assert!(
            refused_at.elapsed() < std::time::Duration::from_secs(5),
            "attempt {attempt}: the refusal parked on a marker the previous \
             refusal left behind"
        );
    }
    let refusals = outcomes(&snapshotter);
    assert_eq!(
        count(&refusals, "insert"),
        0,
        "a refused request cached something: {refusals:?}"
    );
    assert_eq!(
        count(&refusals, "hit"),
        0,
        "a refusal was answered from the cache: {refusals:?}"
    );
    assert_eq!(
        count(&refusals, "miss"),
        2,
        "the second refusal did not reach the query on its own: {refusals:?}"
    );
    assert_never_parked("repeated refusal", &refusals);
    // Nothing partial was left where a larger budget would find it. This
    // corpus is the one past the entry allowance, so the re-read is a fresh
    // execution rather than a hit on something warmed earlier.
    assert_eq!(
        names(&acme_app, &services_path(OVERSIZED_INDEX))
            .await
            .len(),
        OVERSIZED_SERVICES,
        "a refused render left a partial answer behind"
    );

    // ---- 9. A request that goes away mid-flight poisons nothing ------------
    //
    // A dropped client future runs no code at all on the way out, so cleanup can
    // only be `SqlResultCacheCtx::drop`. Driven by hand and abandoned rather
    // than raced against a timer: whether it got as far as becoming the leader
    // or not, the next identical poll must reach the query or the cache — never
    // park on it.
    append(&acme, SHARED_INDEX, 300, &[("dropped", &["op-drop"])]).await;
    expected.push("dropped".to_string());
    expected.sort();
    let _ = outcomes(&snapshotter);
    {
        let mut abandoned = std::pin::pin!(acme_app
            .clone()
            .oneshot(request(&services_path(SHARED_INDEX))));
        for _ in 0..8 {
            if futures::poll!(abandoned.as_mut()).is_ready() {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Dropped here, wherever it got to.
    }
    let resumed_at = std::time::Instant::now();
    let after_drop = names(&acme_app, &services_path(SHARED_INDEX)).await;
    assert_eq!(after_drop, expected, "the poll after an abandoned one");
    assert!(
        resumed_at.elapsed() < std::time::Duration::from_secs(5),
        "the poll after an abandoned one parked on its leaked marker"
    );
    let dropped = outcomes(&snapshotter);
    assert_never_parked("after an abandoned poll", &dropped);
    assert_eq!(count(&dropped, "wait_timeout"), 0, "{dropped:?}");

    // ---- 10. Result caches off: every poll executes ------------------------
    //
    // Injected tuning, not the environment. The route resolves the switch the
    // same way the storage caches do, so a deployment that turned result caching
    // off gets the pre-#2268 behaviour and no entries at all.
    let cold_tmp = tempfile::tempdir().unwrap();
    let no_cache = Arc::new(
        IcebergContext::open(&cold_tmp.path().join("warehouse"))
            .await
            .unwrap()
            .with_tuning(IcebergTuning {
                result_caches: Some(false),
                ..Default::default()
            }),
    );
    create_and_append(&no_cache, SHARED_INDEX, 1, &[("solo", &["op-solo"])]).await;
    let no_cache_app = app_with_budget(&no_cache, PACKAGED_BUDGET);
    let _ = outcomes(&snapshotter);
    for poll in 1..=3 {
        assert_eq!(
            names(&no_cache_app, &services_path(SHARED_INDEX)).await,
            strs(&["solo"]),
            "poll {poll} with result caches off"
        );
        assert_eq!(
            names(&no_cache_app, &operations_path(SHARED_INDEX, "solo")).await,
            strs(&["op-solo"]),
            "poll {poll} with result caches off"
        );
    }
    let off = outcomes(&snapshotter);
    for outcome in ["hit", "miss", "insert", "wait", "wait_timeout"] {
        assert_eq!(
            count(&off, outcome),
            0,
            "a context with result caches off still consulted the result cache \
             ({outcome}): {off:?}"
        );
    }
}
