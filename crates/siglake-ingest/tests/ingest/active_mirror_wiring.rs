//! The active-segment mirror has to cover the writers ingest writes to.
//!
//! THE DEFECT THIS GUARDS (#5055). `active_mirror_loop` was handed the ingest
//! server's ROOT `WalWriter`, and no ingest request appends to that writer once
//! a router is installed — the server always installs the per-tenant router,
//! and the default path installs the backpressure router over it. Every tick
//! flushed an empty writer, got `None`, and continued: with
//! `--wal-active-mirror-interval-secs 1` the process logged "WAL active-segment
//! mirror enabled" and never wrote one `_active/` object. The N-second
//! data-loss bound the flag advertises did not exist on any install, and
//! `wal-recover`'s `root confirmed` verdict — which keys off an `_active/`
//! object — was unreachable. The pre-#5055 wiring is the `root_writer_only`
//! negative control below: same server, same posts, same loop, no uploads.
//!
//! `mirror::active_mirror_uploads_partial_segment` drives the loop with a
//! writer it appends to itself, so it passed throughout.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use axum::Router;
use opendal::Operator;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tower::util::ServiceExt;

use siglake_ingest::backpressure::BackpressureRouter;
use siglake_ingest::{router, AppState, TenantRouting, TenantWalRouter};
use siglake_wal::mirror::ActiveMirrorSource;
use siglake_wal::WalWriter;

const PREFIX: &str = "wal-mirror";
/// Fast enough that the arms converge in well under a second, slow enough that
/// a tick is not competing with the post it is meant to follow.
const INTERVAL: Duration = Duration::from_millis(20);
/// Segments must stay OPEN for the whole of every arm: this is the active
/// mirror, and a sealed segment is the other uploader's.
const NO_ROLL: usize = 1_000_000;

/// The ingest server, its `file://` warehouse, and the active-mirror sources
/// the server derives from the state its handlers serve from — the same call
/// `siglake ingest-server` makes.
struct Server {
    app: Router,
    sources: Vec<Arc<dyn ActiveMirrorSource>>,
    root_writer: Arc<Mutex<WalWriter>>,
    op: Operator,
    wal: tempfile::TempDir,
    _warehouse: tempfile::TempDir,
}

/// `--warehouse-url file://<dir>`: the operator `build_opendal_operator`
/// builds for a `file://` URL, so the keys under test are the keys a real run
/// writes.
fn fs_op(root: &std::path::Path) -> Operator {
    Operator::new(opendal::services::Fs::default().root(root.to_str().unwrap()))
        .unwrap()
        .finish()
}

/// `backpressure` is `Some((capacity, shards))` for the default ingest path,
/// `None` for the `--ingest-backpressure-capacity 0` opt-out.
async fn server(routing: TenantRouting, backpressure: Option<(usize, usize)>) -> Server {
    let wal = tempfile::tempdir().unwrap();
    let warehouse = tempfile::tempdir().unwrap();
    let tenants = TenantWalRouter::new(wal.path(), "ing-test", NO_ROLL, Duration::from_secs(600));
    let root_writer = Arc::new(Mutex::new(
        WalWriter::with_thresholds(wal.path(), "ing-test", NO_ROLL, Duration::from_secs(600))
            .unwrap(),
    ));
    let mut state = AppState {
        writer: root_writer.clone(),
        tenants: Some(Arc::new(tenants)),
        backpressure: None,
        events_tx: None,
        tokens: None,
        oidc_verifier: None,
        rate_limiter: None,
        mem_guard: None,
        commit_force_timeout: Duration::from_secs(30),
        remote_wal_drain: true,
        allowed_tenants: None,
        tenant_routing: routing,
        max_tenants: 0,
        tenant_admission: Default::default(),
    };
    if let Some((capacity, shards)) = backpressure {
        let bp = BackpressureRouter::new(
            wal.path(),
            "ing-test",
            NO_ROLL,
            Duration::from_secs(600),
            capacity,
        )
        .with_shards_per_tenant(shards);
        state = state.with_backpressure_arc(Arc::new(bp));
    }
    let sources = state.active_mirror_sources();
    Server {
        app: router(state),
        sources,
        root_writer,
        op: fs_op(warehouse.path()),
        wal,
        _warehouse: warehouse,
    }
}

fn otlp_body(raw: &str) -> String {
    serde_json::json!({
        "resourceLogs": [{
            "resource": { "attributes": [
                { "key": "host.name", "value": { "stringValue": "h1" } }
            ]},
            "scopeLogs": [{
                "logRecords": [{
                    "timeUnixNano": "1700000000000000000",
                    "body": { "stringValue": raw },
                    "severityText": "INFO"
                }]
            }]
        }]
    })
    .to_string()
}

/// One OTLP log line, routed by the same two headers a client uses.
async fn post(app: &Router, tenant: Option<&str>, index: Option<&str>, raw: &str) {
    let mut req = HttpRequest::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/json");
    if let Some(tenant) = tenant {
        req = req.header("X-Scope-OrgID", tenant);
    }
    if let Some(index) = index {
        req = req.header("x-siglake-index", index);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(otlp_body(raw))).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "POST /v1/logs was refused");
}

fn spawn_mirror(sources: Vec<Arc<dyn ActiveMirrorSource>>, op: Operator) -> JoinHandle<()> {
    tokio::spawn(async move {
        siglake_wal::mirror::active_mirror_loop(sources, op, PREFIX.to_string(), INTERVAL).await;
    })
}

/// Every `_active/` object under the mirror prefix, keyed relative to it.
async fn active_keys(op: &Operator) -> Vec<String> {
    use futures::stream::StreamExt;
    let mut lister = op
        .lister_with(&format!("{PREFIX}/_active/"))
        .recursive(true)
        .await
        .unwrap()
        .fuse();
    let mut keys = Vec::new();
    while let Some(entry) = lister.next().await {
        let entry = entry.unwrap();
        if entry.metadata().is_file() {
            keys.push(entry.path().to_string());
        }
    }
    keys.sort();
    keys
}

/// Rows an active object reads back as, or 0 if it does not read back at all.
///
/// A listing is not enough to wait on. `build_opendal_operator` builds a
/// `file://` store with no `atomic_write_dir`, and opendal's fs writer then
/// creates the target file IN PLACE — so the key is listable, and stat-able at
/// zero bytes, from before the first byte of the body lands. Waiting on the
/// listing alone and then aborting the loop caught one object mid-PUT and left
/// it empty, which is the flake this test hit in CI once. What every arm
/// actually wants is "the object is recoverable", so that is what it waits for.
/// A prefix that stops after the schema message reads back as zero rows, which
/// is why this counts rows rather than testing for an `Ok`.
async fn active_object_rows(op: &Operator, key: &str) -> usize {
    let Ok(body) = op.read(key).await else {
        return 0;
    };
    siglake_wal::read_segment_from_bytes(&body.to_bytes())
        .map(|batches| batches.iter().map(|b| b.num_rows()).sum())
        .unwrap_or(0)
}

/// Poll until the mirror holds `want` active objects whose bodies read back as
/// segments with rows. Convergence, not a sleep: the assertion is on the end
/// state, and the loop's tick is what gets there.
async fn wait_for_active(op: &Operator, want: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let keys = active_keys(op).await;
        let mut readable = Vec::with_capacity(keys.len());
        for key in &keys {
            if active_object_rows(op, key).await > 0 {
                readable.push(key.clone());
            }
        }
        if readable.len() >= want {
            return readable;
        }
        if Instant::now() >= deadline {
            panic!(
                "mirror holds {} readable active objects of {} listed, wanted {want}: {keys:?}",
                readable.len(),
                keys.len()
            );
        }
        tokio::time::sleep(INTERVAL).await;
    }
}

/// Rows in every sealed segment under `dir`, with the `raw` column of each.
fn sealed_rows(dir: &std::path::Path) -> Vec<String> {
    let mut raws = Vec::new();
    for path in siglake_wal::list_sealed(dir).unwrap() {
        for batch in siglake_wal::read_segment(&path).unwrap() {
            let col = batch
                .column_by_name("raw")
                .expect("WAL batch has a raw column");
            let raw = col
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("raw is a StringArray");
            for i in 0..batch.num_rows() {
                raws.push(raw.value(i).to_string());
            }
        }
    }
    raws.sort();
    raws
}

/// The default ingest path: backpressure router on, tenant from the header,
/// one managed index alongside the tenant's events. Two tenants, two shards.
///
/// Asserts the whole chain the flag promises: an `_active/` object per open
/// writer, keyed by `<tenant>[/<index>]`, BEFORE anything seals — and a
/// `wal-recover` of that mirror onto a fresh WAL root that yields the rows
/// back, each under the tenant and index it was posted to.
#[tokio::test]
async fn backpressure_path_mirrors_every_tenant_and_index_before_sealing() {
    let server = server(TenantRouting::TrustHeader, Some((64, 2))).await;
    post(&server.app, Some("acme"), None, "acme-events-1").await;
    post(&server.app, Some("acme"), None, "acme-events-2").await;
    post(&server.app, Some("acme"), Some("orders"), "acme-orders-1").await;
    post(&server.app, Some("widgets"), None, "widgets-events-1").await;

    let mirror = spawn_mirror(server.sources.clone(), server.op.clone());
    // acme/events is two shards' worth (round-robin across a 2-shard group),
    // acme/orders and widgets/events one each.
    let keys = wait_for_active(&server.op, 4).await;
    mirror.abort();

    assert_eq!(keys.len(), 4, "one object per open writer: {keys:?}");
    // On the subdir exactly, not a prefix of it: `acme` and `acme/orders` are
    // different writers, and the whole point is that the key separates them.
    let count = |sub: &str| {
        keys.iter()
            .filter(|k| {
                let Some(rest) = k.strip_prefix(&format!("{PREFIX}/_active/{sub}/")) else {
                    return false;
                };
                !rest.contains('/') && rest.ends_with(".arrow.partial")
            })
            .count()
    };
    assert_eq!(count("acme"), 2, "acme's two event shards: {keys:?}");
    assert_eq!(count("acme/orders"), 1, "acme's managed index: {keys:?}");
    assert_eq!(count("widgets"), 1, "the second tenant: {keys:?}");

    // Nothing sealed: these are uploads of in-flight segments, which is the
    // window `activeIntervalSecs` exists to bound.
    assert!(
        siglake_wal::list_sealed(&server.wal.path().join("acme"))
            .unwrap()
            .is_empty(),
        "the arm sealed a segment, so it is not testing the active mirror"
    );

    // The PVC is gone; this is what is left of it.
    let recovered = tempfile::tempdir().unwrap();
    let root = recovered.path().join("wal");
    let summary = siglake_wal::mirror::recover_from_object_store(server.op.clone(), PREFIX, &root)
        .await
        .unwrap();
    assert_eq!(summary.pulled, 4, "every active object recovers");
    assert_eq!(
        sealed_rows(&root.join("acme")),
        vec!["acme-events-1", "acme-events-2"],
        "acme's events came back under acme"
    );
    assert_eq!(
        sealed_rows(&root.join("acme").join("orders")),
        vec!["acme-orders-1"],
        "the managed index's rows came back under their index, not events"
    );
    assert_eq!(
        sealed_rows(&root.join("widgets")),
        vec!["widgets-events-1"],
        "the second tenant's rows came back under widgets"
    );
}

/// The `--ingest-backpressure-capacity 0` opt-out, single-tenant: rows go to
/// the per-tenant router's `default` writer, and that is what must be mirrored.
#[tokio::test]
async fn tenant_router_path_mirrors_the_default_tenant() {
    let server = server(TenantRouting::SingleTenant, None).await;
    post(&server.app, None, None, "default-events-1").await;

    let mirror = spawn_mirror(server.sources.clone(), server.op.clone());
    let keys = wait_for_active(&server.op, 1).await;
    mirror.abort();

    assert_eq!(keys.len(), 1, "one object for the default tenant: {keys:?}");
    assert!(
        keys[0].starts_with(&format!("{PREFIX}/_active/default/")),
        "the key must carry the tenant recovery routes on: {keys:?}"
    );

    let recovered = tempfile::tempdir().unwrap();
    let root = recovered.path().join("wal");
    siglake_wal::mirror::recover_from_object_store(server.op.clone(), PREFIX, &root)
        .await
        .unwrap();
    assert_eq!(sealed_rows(&root.join("default")), vec!["default-events-1"]);
}

/// The pre-#5055 wiring, as a negative control: the same server, the same
/// posts, the same loop — driven by the root writer alone. It uploads nothing,
/// which is what every install with this flag on was getting.
#[tokio::test]
async fn root_writer_only_uploads_nothing() {
    let server = server(TenantRouting::TrustHeader, Some((64, 1))).await;
    post(&server.app, Some("acme"), None, "acme-events-1").await;
    post(&server.app, None, None, "default-events-1").await;

    let sources: Vec<Arc<dyn ActiveMirrorSource>> = vec![server.root_writer.clone()];
    let mirror = spawn_mirror(sources, server.op.clone());
    // Ten intervals is eight more than the first upload needs in the arms
    // above; the point is that no number of them produces one.
    tokio::time::sleep(INTERVAL * 10).await;
    mirror.abort();

    assert!(
        active_keys(&server.op).await.is_empty(),
        "the root writer holds no rows, so mirroring it mirrors nothing"
    );
    // ... while the sources the server derives do produce uploads, from the
    // very same writes.
    let mirror = spawn_mirror(server.sources.clone(), server.op.clone());
    let keys = wait_for_active(&server.op, 2).await;
    mirror.abort();
    assert_eq!(keys.len(), 2, "acme's and default's segments: {keys:?}");
}

/// `activeIntervalSecs: 0` is the default and the documented off switch: the
/// decision is one function, and nothing is uploaded when it says off.
#[tokio::test]
async fn interval_zero_is_off_and_mirrors_nothing() {
    assert!(
        siglake_wal::mirror::active_mirror_interval(0).is_none(),
        "0 must read as off — `tokio::time::interval` panics on it"
    );
    assert_eq!(
        siglake_wal::mirror::active_mirror_interval(5),
        Some(Duration::from_secs(5))
    );

    let server = server(TenantRouting::TrustHeader, Some((64, 1))).await;
    post(&server.app, Some("acme"), None, "acme-events-1").await;
    // What the server does with `None`: no loop, and therefore no objects.
    assert!(siglake_wal::mirror::active_mirror_interval(0).is_none());
    tokio::time::sleep(INTERVAL * 10).await;
    assert!(
        active_keys(&server.op).await.is_empty(),
        "an off interval must leave the mirror prefix alone"
    );
}

/// A segment whose bytes have not moved since the last upload is not re-PUT:
/// an idle tenant's open segment would otherwise cost one object-store write
/// per tick, forever.
#[tokio::test]
async fn an_unchanged_segment_is_not_uploaded_twice() {
    let server = server(TenantRouting::TrustHeader, Some((64, 1))).await;
    post(&server.app, Some("acme"), None, "acme-events-1").await;

    let mirror = spawn_mirror(server.sources.clone(), server.op.clone());
    let keys = wait_for_active(&server.op, 1).await;
    let first = server.op.stat(&keys[0]).await.unwrap();
    // Several more ticks with no writes in between.
    tokio::time::sleep(INTERVAL * 6).await;
    let idle = server.op.stat(&keys[0]).await.unwrap();
    assert_eq!(
        idle.last_modified(),
        first.last_modified(),
        "an idle segment was re-uploaded"
    );

    // One more write and the object moves again — the skip is byte-count
    // based, not a one-shot. Read back as rows, not as a length: a length can
    // be read mid-PUT, and "the second row is in the mirror" is the claim.
    post(&server.app, Some("acme"), None, "acme-events-2").await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if active_object_rows(&server.op, &keys[0]).await == 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the second write never reached the mirror"
        );
        tokio::time::sleep(INTERVAL).await;
    }
    mirror.abort();
}
