//! #2693: an ingest lane binds the Iceberg table its rows are destined for
//! before it appends anything, and re-binds when the index is recreated.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use siglake_core::Event;
use siglake_ingest::backpressure::BackpressureRouter;
use siglake_ingest::{
    identity_refresh_from, TenantWalRouter, WalTableIdentity, DEFAULT_IDENTITY_REFRESH,
};
use uuid::Uuid;

const DROPPED: &str = "22222222-2222-4222-8222-222222222222";
const LIVE: &str = "11111111-1111-4111-8111-111111111111";

/// A resolver whose answer an operator can change mid-test, standing in for a
/// `DELETE`+`POST` of the index behind the lane.
struct Swappable {
    uuid: std::sync::Mutex<Option<Uuid>>,
    fail: std::sync::atomic::AtomicBool,
    calls: AtomicUsize,
}

impl Swappable {
    fn new(uuid: &str) -> Arc<Self> {
        Arc::new(Self {
            uuid: std::sync::Mutex::new(Some(Uuid::parse_str(uuid).unwrap())),
            fail: std::sync::atomic::AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        })
    }
    fn set(&self, uuid: &str) {
        *self.uuid.lock().unwrap() = Some(Uuid::parse_str(uuid).unwrap());
    }
}

#[async_trait::async_trait]
impl WalTableIdentity for Swappable {
    async fn table_uuid(&self, _tenant: &str, _index: &str) -> anyhow::Result<Option<Uuid>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.fail.load(Ordering::Relaxed) {
            anyhow::bail!("catalog unreachable");
        }
        Ok(*self.uuid.lock().unwrap())
    }
}

fn ev(raw: &str) -> Event {
    Event::now(raw.to_string())
}

/// Every sealed segment under `dir/sealed/`, with the table it names.
fn sealed_owners(dir: &std::path::Path) -> Vec<Option<String>> {
    let mut out: Vec<(String, Option<String>)> = siglake_wal::list_sealed(dir)
        .unwrap()
        .into_iter()
        .map(|p| {
            (
                p.file_name().unwrap().to_string_lossy().to_string(),
                siglake_wal::segment_owner(&p).map(|u| u.to_string()),
            )
        })
        .collect();
    out.sort();
    out.into_iter().map(|(_, owner)| owner).collect()
}

/// The env knob's pure twin, exercised without touching the environment.
#[test]
fn identity_refresh_resolver() {
    assert_eq!(identity_refresh_from(None), DEFAULT_IDENTITY_REFRESH);
    assert_eq!(identity_refresh_from(Some(" 5 ")), Duration::from_secs(5));
    assert_eq!(identity_refresh_from(Some("0")), Duration::ZERO);
    assert_eq!(
        identity_refresh_from(Some("not-a-number")),
        DEFAULT_IDENTITY_REFRESH,
        "an unparseable override falls back rather than disabling the refresh"
    );
}

/// The legacy per-tenant router: the lane's first segment already names its
/// table, and a refresh after the index is recreated rebinds without
/// re-attributing the rows that were already accepted.
#[tokio::test]
async fn a_tenant_lane_binds_then_rebinds_its_table() {
    let tmp = tempfile::tempdir().unwrap();
    let router = TenantWalRouter::new(tmp.path(), "ing", 1_000, Duration::from_secs(3600));
    let resolver = Swappable::new(DROPPED);
    router
        .set_identity_resolver(Some(resolver.clone() as Arc<dyn WalTableIdentity>))
        .await;

    let writer = router.writer_for_index("acme", "orders").await.unwrap();
    let dir = router.index_dir("acme", "orders");
    writer.lock().await.append_events(&[ev("old")]).unwrap();

    // The index is dropped and recreated: the same name, a new table.
    resolver.set(LIVE);
    // Refreshes are TTL'd, so a re-resolve inside the window is skipped. This
    // is the window an operator shortens with SIGLAKE_WAL_IDENTITY_REFRESH_SECS.
    router.refresh_table_identities().await;
    assert!(
        sealed_owners(&dir).is_empty(),
        "nothing is re-resolved inside the refresh window"
    );

    // Past the window (simulated by re-installing the resolver, which clears
    // the per-lane timestamps the way a fresh process would).
    router
        .set_identity_resolver(Some(resolver.clone() as Arc<dyn WalTableIdentity>))
        .await;
    assert_eq!(
        sealed_owners(&dir),
        vec![Some(DROPPED.to_string())],
        "the rows accepted before the rebind keep the table they were written for"
    );

    writer.lock().await.append_events(&[ev("new")]).unwrap();
    writer.lock().await.seal().unwrap();
    assert_eq!(
        sealed_owners(&dir),
        vec![Some(DROPPED.to_string()), Some(LIVE.to_string())],
        "and everything after it names the replacement"
    );
}

/// The backpressure router (the shipped default) does the same through its
/// lane task, which owns the writer.
#[tokio::test]
async fn a_backpressure_lane_binds_then_rebinds_its_table() {
    let tmp = tempfile::tempdir().unwrap();
    let router = BackpressureRouter::new(tmp.path(), "ing", 1_000, Duration::from_secs(3600), 16);
    let resolver = Swappable::new(DROPPED);
    router
        .set_identity_resolver(Some(resolver.clone() as Arc<dyn WalTableIdentity>))
        .await;

    let dir = tmp.path().join("acme").join("orders");
    router
        .submit_for_index("acme", "orders", vec![ev("old")])
        .await;

    resolver.set(LIVE);
    // Re-installing the resolver clears the TTL bookkeeping, so this is the
    // first refresh past the window.
    router
        .set_identity_resolver(Some(resolver.clone() as Arc<dyn WalTableIdentity>))
        .await;
    // The rebind is a lane command: it lands after the writes already queued.
    router
        .submit_for_index("acme", "orders", vec![ev("new")])
        .await;
    router.shutdown().await;

    assert_eq!(
        sealed_owners(&dir),
        vec![Some(DROPPED.to_string()), Some(LIVE.to_string())],
        "the pre-recreation rows are sealed under the dropped table and the later \
         ones under the replacement"
    );
}

/// A catalog that cannot be reached leaves the binding alone. Dropping it to
/// "unstamped" would make the segments serve everywhere; guessing the new one
/// would attribute rows to a table nobody has confirmed.
#[tokio::test]
async fn an_unreachable_catalog_keeps_the_current_binding() {
    let tmp = tempfile::tempdir().unwrap();
    let router = TenantWalRouter::new(tmp.path(), "ing", 1_000, Duration::from_secs(3600));
    let resolver = Swappable::new(DROPPED);
    router
        .set_identity_resolver(Some(resolver.clone() as Arc<dyn WalTableIdentity>))
        .await;
    let writer = router.writer_for_index("acme", "orders").await.unwrap();
    let dir = router.index_dir("acme", "orders");

    resolver.fail.store(true, Ordering::Relaxed);
    router
        .set_identity_resolver(Some(resolver.clone() as Arc<dyn WalTableIdentity>))
        .await;

    writer.lock().await.append_events(&[ev("row")]).unwrap();
    writer.lock().await.seal().unwrap();
    assert_eq!(
        sealed_owners(&dir),
        vec![Some(DROPPED.to_string())],
        "a failed resolve must not blank the identity"
    );
}
