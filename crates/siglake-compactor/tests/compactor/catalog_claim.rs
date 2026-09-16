//! Multi-pod compactor path on top of `SqlSegmentClaim`.
//!
//! We seed two real WAL segments into an in-memory opendal Operator
//! (acting as the mirror bucket), wire two `Compactor` instances
//! against the same SQLite-backed claim, and run them concurrently.
//! Together they must commit each segment exactly once.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use datafusion::prelude::SessionContext;
use opendal::services::Memory;
use opendal::Operator;

use siglake_compactor::{CatalogClaimConfig, Compactor};
use siglake_core::Event;
use siglake_storage::catalog_claim::SqlSegmentClaim;
use siglake_storage::iceberg::IcebergContext;
use siglake_wal::WalWriter;

async fn count_events(ice: &IcebergContext) -> i64 {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

fn memory_op() -> Operator {
    Operator::new(Memory::default()).unwrap().finish()
}

fn synth(prefix: &str, n: usize) -> Vec<Event> {
    (0..n)
        .map(|i| Event::now(format!("{prefix}-{i}")))
        .collect()
}

/// Seal one segment locally, then upload its bytes verbatim to the
/// shared mirror bucket so the catalog-claim compactor can pick it up.
async fn seed_segment(
    tmp_root: &std::path::Path,
    name: &str,
    events: Vec<Event>,
    store: &Operator,
    prefix: &str,
) -> String {
    let wal_dir = tmp_root.join(name);
    let mut w =
        WalWriter::with_thresholds(&wal_dir, name, events.len(), Duration::from_secs(60)).unwrap();
    let seg = w
        .append_events(&events)
        .unwrap()
        .expect("should seal at threshold");
    let bytes = std::fs::read(&seg.path).unwrap();
    let id = seg
        .path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap()
        .to_string();
    let key = format!("{prefix}/{id}.arrow");
    store.write(&key, Bytes::from(bytes)).await.unwrap();
    id
}

/// Seed a segment under `<prefix>/<tenant>/<id>.arrow` (per-tenant
/// mirror layout). Used by the multi-tenant catalog-claim test.
async fn seed_tenant_segment(
    tmp_root: &std::path::Path,
    name: &str,
    tenant: &str,
    events: Vec<Event>,
    store: &Operator,
    prefix: &str,
) -> String {
    let wal_dir = tmp_root.join(name);
    let mut w =
        WalWriter::with_thresholds(&wal_dir, name, events.len(), Duration::from_secs(60)).unwrap();
    let seg = w
        .append_events(&events)
        .unwrap()
        .expect("should seal at threshold");
    let bytes = std::fs::read(&seg.path).unwrap();
    let id = seg
        .path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap()
        .to_string();
    let key = format!("{prefix}/{tenant}/{id}.arrow");
    store.write(&key, Bytes::from(bytes)).await.unwrap();
    id
}

async fn count_events_in(ice: &IcebergContext) -> i64 {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn catalog_claim_routes_per_tenant_segments_to_namespaces() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let prefix = "wal-mirror";

    let store = memory_op();
    seed_tenant_segment(tmp.path(), "ing-a", "acme", synth("a", 3), &store, prefix).await;
    seed_tenant_segment(
        tmp.path(),
        "ing-w",
        "widgets",
        synth("w", 5),
        &store,
        prefix,
    )
    .await;

    let claim_uri = format!(
        "sqlite://{}?mode=rwc",
        tmp.path().join("claim.db").display()
    );
    let claim = SqlSegmentClaim::connect(&claim_uri, "pod-1").await.unwrap();
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    let cfg = CatalogClaimConfig {
        claim,
        store: store.clone(),
        prefix: prefix.to_string(),
        batch_size: 10,
        last_mirror_sync: Default::default(),
        last_reclaim: Default::default(),
    };
    let c = Compactor::new(tmp.path().join("wal-pod-1"), ice.clone()).with_catalog_claim(cfg);
    let n = c.run_once().await.unwrap();
    assert_eq!(n, 2, "both tenants' segments committed");

    assert_eq!(count_events_in(&ice).await, 0);
    let acme = ice.for_namespace("tenant_acme").await.unwrap();
    assert_eq!(count_events_in(&acme).await, 3);
    let widgets = ice.for_namespace("tenant_widgets").await.unwrap();
    assert_eq!(count_events_in(&widgets).await, 5);
}

#[tokio::test]
async fn two_compactors_split_segments_without_double_commit() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let prefix = "wal-mirror";

    let store = memory_op();
    let id_a = seed_segment(tmp.path(), "ing-a", synth("a", 7), &store, prefix).await;
    let id_b = seed_segment(tmp.path(), "ing-b", synth("b", 7), &store, prefix).await;
    assert_ne!(id_a, id_b);

    let claim_db = tmp.path().join("claim.db");
    let claim_uri = format!("sqlite://{}?mode=rwc", claim_db.display());
    let claim1 = SqlSegmentClaim::connect(&claim_uri, "pod-1").await.unwrap();
    let claim2 = SqlSegmentClaim::connect(&claim_uri, "pod-2").await.unwrap();

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    let cfg1 = CatalogClaimConfig {
        claim: claim1,
        store: store.clone(),
        prefix: prefix.to_string(),
        batch_size: 4,
        last_mirror_sync: Default::default(),
        last_reclaim: Default::default(),
    };
    let cfg2 = CatalogClaimConfig {
        claim: claim2,
        store: store.clone(),
        prefix: prefix.to_string(),
        batch_size: 4,
        last_mirror_sync: Default::default(),
        last_reclaim: Default::default(),
    };
    let c1 = Compactor::new(tmp.path().join("wal-pod-1"), ice.clone()).with_catalog_claim(cfg1);
    let c2 = Compactor::new(tmp.path().join("wal-pod-2"), ice.clone()).with_catalog_claim(cfg2);

    let (r1, r2) = tokio::join!(c1.run_once(), c2.run_once());
    let n1 = r1.unwrap();
    let n2 = r2.unwrap();
    assert_eq!(n1 + n2, 2, "exactly 2 segments committed across both pods");

    assert_eq!(
        count_events(&ice).await,
        14,
        "must commit each segment exactly once"
    );
}

#[tokio::test]
async fn run_once_catalog_no_segments_is_empty_cycle() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let store = memory_op();
    let claim_db = tmp.path().join("claim.db");
    let claim_uri = format!("sqlite://{}?mode=rwc", claim_db.display());
    let claim = SqlSegmentClaim::connect(&claim_uri, "pod-1").await.unwrap();
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    let cfg = CatalogClaimConfig {
        claim,
        store,
        prefix: "wal-mirror".to_string(),
        batch_size: 4,
        last_mirror_sync: Default::default(),
        last_reclaim: Default::default(),
    };
    let c = Compactor::new(tmp.path().join("wal"), ice).with_catalog_claim(cfg);
    let n = c.run_once().await.unwrap();
    assert_eq!(n, 0);
}

/// Fleet prereq 2: a segment mirrored under `<prefix>/<tenant>/<index>/…`
/// must commit to that tenant's INDEX table — before the index dimension,
/// it collapsed into the default tenant's events table. The index resolves
/// through the same template auto-create path the FS drain uses.
#[tokio::test]
async fn catalog_claim_routes_per_index_segments_to_index_tables() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let prefix = "wal-mirror";

    let store = memory_op();
    // One events segment for tenant acme + one index segment for acme/app1.
    seed_tenant_segment(tmp.path(), "ing-ev", "acme", synth("ev", 3), &store, prefix).await;
    {
        // Per-index mirror layout: <prefix>/<tenant>/<index>/<id>.arrow.
        let wal_dir = tmp.path().join("ing-ix");
        let events = synth("ix", 4);
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "ing-ix", events.len(), Duration::from_secs(60))
                .unwrap();
        let seg = w.append_events(&events).unwrap().expect("seal");
        let bytes = std::fs::read(&seg.path).unwrap();
        let id = seg
            .path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap()
            .to_string();
        store
            .write(
                &format!("{prefix}/acme/app1/{id}.arrow"),
                Bytes::from(bytes),
            )
            .await
            .unwrap();
    }

    let claim_uri = format!(
        "sqlite://{}?mode=rwc",
        tmp.path().join("claim.db").display()
    );
    let claim = SqlSegmentClaim::connect(&claim_uri, "pod-1").await.unwrap();
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    // Install an events-shaped template so ensure_index("app1") auto-creates.
    {
        let tenant_ice = ice.for_namespace("tenant_acme").await.unwrap();
        let mut tpl = siglake_storage::index_manager::builtin_logs_template();
        tpl.template_id = "app1-template".into();
        tpl.index_id_patterns = vec!["app1".into()];
        tenant_ice.put_index_template(&tpl).await.unwrap();
    }

    let cfg = CatalogClaimConfig {
        claim,
        store,
        prefix: prefix.to_string(),
        batch_size: 8,
        last_mirror_sync: Default::default(),
        last_reclaim: Default::default(),
    };
    let c = Compactor::new(tmp.path().join("wal"), ice.clone()).with_catalog_claim(cfg);
    let n = c.run_once().await.unwrap();
    assert_eq!(n, 2, "both segments committed");

    // The events segment landed in tenant_acme.events…
    let tenant_ice = ice.for_namespace("tenant_acme").await.unwrap();
    assert_eq!(count_events_in(&tenant_ice).await, 3);
    // …and the index segment in tenant_acme's app1 table (mapped rows).
    let ctx = SessionContext::new();
    tenant_ice
        .register_table_with_datafusion(&ctx, &tenant_ice.index_table_ident("app1"), "app1")
        .await
        .unwrap();
    let batches = ctx
        .sql("SELECT count(*) AS n FROM app1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 4, "index segment rows committed to the index table");
}

/// Seed one segment under the per-index mirror layout
/// `<prefix>/<tenant>/<index>/<id>.arrow` and return its id.
async fn seed_index_segment(
    tmp_root: &std::path::Path,
    name: &str,
    tenant: &str,
    index: &str,
    events: Vec<Event>,
    store: &Operator,
    prefix: &str,
) -> String {
    seed_index_segment_owned(tmp_root, name, tenant, index, events, store, prefix, None).await
}

/// [`seed_index_segment`] through a writer bound to `owner` (#2693), so the
/// uploaded object carries that table's uuid in its frame header.
#[allow(clippy::too_many_arguments)]
async fn seed_index_segment_owned(
    tmp_root: &std::path::Path,
    name: &str,
    tenant: &str,
    index: &str,
    events: Vec<Event>,
    store: &Operator,
    prefix: &str,
    owner: Option<&str>,
) -> String {
    let wal_dir = tmp_root.join(name);
    let mut w =
        WalWriter::with_thresholds(&wal_dir, name, events.len(), Duration::from_secs(60)).unwrap();
    if let Some(owner) = owner {
        w.bind_table_uuid(Some(uuid::Uuid::parse_str(owner).unwrap()))
            .unwrap();
    }
    let seg = w.append_events(&events).unwrap().expect("seal");
    let bytes = std::fs::read(&seg.path).unwrap();
    let id = seg
        .path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap()
        .to_string();
    store
        .write(
            &format!("{prefix}/{tenant}/{index}/{id}.arrow"),
            Bytes::from(bytes),
        )
        .await
        .unwrap();
    id
}

async fn count_index_rows(ice: &IcebergContext, index: &str) -> i64 {
    let ctx = SessionContext::new();
    ice.register_table_with_datafusion(&ctx, &ice.index_table_ident(index), index)
        .await
        .unwrap();
    let batches = ctx
        .sql(&format!("SELECT count(*) AS n FROM {index}"))
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

/// Read the mirror owner marker as `(current, superseded)`.
async fn mirror_owner(
    store: &Operator,
    prefix: &str,
    tenant: &str,
    index: &str,
) -> (String, Vec<String>) {
    let key = siglake_wal::mirror::mirror_owner_key(prefix, tenant, index);
    let raw = String::from_utf8(store.read(&key).await.unwrap().to_vec()).unwrap();
    let mut lines = raw
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty());
    (lines.next().expect("a stamped marker"), lines.collect())
}

/// How many `.arrow` objects sit under an index's mirror prefix.
async fn mirrored_objects(store: &Operator, prefix: &str, tenant: &str, index: &str) -> usize {
    store
        .list(&format!("{prefix}/{tenant}/{index}/"))
        .await
        .unwrap()
        .iter()
        .filter(|e| e.path().ends_with(".arrow"))
        .count()
}

/// #2661, the catalog-claim half of
/// `query_server::indexes::a_recreated_index_serves_only_its_own_rows`, plus
/// #2729's recovery.
///
/// This drain never reads the ingester's filesystem — it claims rows and
/// fetches bytes from the mirror — so a per-WAL-directory marker cannot reach
/// it, and the mirror's `<prefix>/<tenant>/<index>/` keys are routed by index
/// NAME exactly like a directory. Without the mirror-side marker, deleting and
/// recreating the id commits the dropped incarnation's mirrored segments into
/// the replacement table.
///
/// The prefix marker alone cannot be the whole answer. Refusing the prefix
/// outright refused the REPLACEMENT's objects too, for good, because they are
/// under the same prefix (#2729: a `DELETE`+`POST` of an already-drained index
/// wedged its catalog-claim ingest permanently). So the marker moves to the
/// live table and each object is admitted on its own frame identity (#2693):
/// the replacement's commit, the dropped incarnation's are QUARANTINED — left
/// in the mirror, set aside in the claim store on the cycle that refuses them
/// (#2746: the verdict is terminal, so there is nothing for the release backoff
/// to retry), which `requeue_quarantined` is the deliberate way out of, the same
/// disposition `stale/` gives the filesystem path.
#[tokio::test]
async fn a_recreated_index_does_not_claim_the_dropped_incarnations_mirror() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let prefix = "wal-mirror";
    let store = memory_op();
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    let config = siglake_core::index_config::IndexConfig {
        index_id: "mirr_idx".to_string(),
        doc_mapping: siglake_core::index_config::IndexConfig::builtin_events().doc_mapping,
        retention: None,
        index_at_flush: None,
    };
    ice.create_index(&config).await.unwrap();
    let dropped_uuid = ice.index_table_uuid("mirr_idx").await.unwrap().unwrap();
    seed_index_segment_owned(
        tmp.path(),
        "ing-old",
        "default",
        "mirr_idx",
        synth("old", 4),
        &store,
        prefix,
        Some(&dropped_uuid),
    )
    .await;

    let claim_uri = format!(
        "sqlite://{}?mode=rwc",
        tmp.path().join("claim.db").display()
    );
    let claim = SqlSegmentClaim::connect(&claim_uri, "pod-1").await.unwrap();
    let cfg = |claim: SqlSegmentClaim| CatalogClaimConfig {
        claim,
        store: store.clone(),
        prefix: prefix.to_string(),
        batch_size: 8,
        last_mirror_sync: Default::default(),
        last_reclaim: Default::default(),
    };
    let c =
        Compactor::new(tmp.path().join("wal"), ice.clone()).with_catalog_claim(cfg(claim.clone()));
    assert_eq!(c.run_once().await.unwrap(), 1, "the original index drains");
    assert_eq!(count_index_rows(&ice, "mirr_idx").await, 4);

    // The drain stamped the group's prefix with the table it committed to.
    // THIS is the state #2729 wedged on: the marker now names a table that the
    // next step drops.
    assert_eq!(
        mirror_owner(&store, prefix, "default", "mirr_idx").await,
        (dropped_uuid.clone(), Vec::new()),
        "the marker names the table the segments were committed to"
    );

    // Delete and recreate the same id. An ingester that has not noticed keeps
    // mirroring the dropped incarnation's rows under the same key prefix,
    // while the replacement's own writer mirrors alongside it.
    assert!(ice.delete_index("mirr_idx").await.unwrap());
    ice.create_index(&config).await.unwrap();
    let ice_new = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let live_uuid = ice_new.index_table_uuid("mirr_idx").await.unwrap().unwrap();
    assert_ne!(live_uuid, dropped_uuid, "recreation is a new table");
    seed_index_segment_owned(
        tmp.path(),
        "ing-stale",
        "default",
        "mirr_idx",
        synth("stale", 3),
        &store,
        prefix,
        Some(&dropped_uuid),
    )
    .await;
    seed_index_segment_owned(
        tmp.path(),
        "ing-live",
        "default",
        "mirr_idx",
        synth("live", 5),
        &store,
        prefix,
        Some(&live_uuid),
    )
    .await;

    let c2 = Compactor::new(tmp.path().join("wal"), ice_new.clone())
        .with_catalog_claim(cfg(claim.clone()));
    assert_eq!(
        c2.run_once().await.unwrap(),
        1,
        "the replacement's own mirrored segment commits; the dropped \
         incarnation's does not"
    );
    assert_eq!(
        count_index_rows(&ice_new, "mirr_idx").await,
        5,
        "the replacement table exposes only its own rows"
    );
    assert_eq!(
        mirror_owner(&store, prefix, "default", "mirr_idx").await,
        (live_uuid.clone(), vec![dropped_uuid.clone()]),
        "the marker moves to the replacement and remembers what it displaced"
    );
    assert_eq!(
        mirrored_objects(&store, prefix, "default", "mirr_idx").await,
        3,
        "the refused object is left in the mirror, not deleted"
    );
    // #2746: on THIS cycle, not after twelve. The verdict cannot change — a
    // table uuid is never reused — so the twelve backoff cycles each paid a GET
    // of the object's bytes to reach the same answer.
    assert_eq!(
        claim.quarantined_count().await.unwrap(),
        1,
        "the refused object was released for retry instead of set aside at once"
    );

    // The refusal is not a one-cycle race the next cycle resolves the wrong
    // way, and the index keeps draining: a later segment of the replacement's
    // own commits, which is exactly what the permanent prefix refusal cost.
    assert_eq!(c2.run_once().await.unwrap(), 0);
    assert_eq!(count_index_rows(&ice_new, "mirr_idx").await, 5);
    seed_index_segment_owned(
        tmp.path(),
        "ing-live-2",
        "default",
        "mirr_idx",
        synth("live2", 2),
        &store,
        prefix,
        Some(&live_uuid),
    )
    .await;
    // A fresh config, so the recovery sweep that registers a newly mirrored
    // object is due: it is interval-gated per config
    // (`SIGLAKE_MIRROR_SYNC_INTERVAL_SECS`, default 60s), and tests do not
    // touch the environment.
    let c3 = Compactor::new(tmp.path().join("wal"), ice_new.clone())
        .with_catalog_claim(cfg(claim.clone()));
    assert_eq!(
        c3.run_once().await.unwrap(),
        1,
        "ingest for the recreated index keeps flowing on later cycles"
    );
    assert_eq!(count_index_rows(&ice_new, "mirr_idx").await, 7);
    assert_eq!(
        claim.quarantined_count().await.unwrap(),
        1,
        "a live object's commit disturbed the set-aside one"
    );
}

/// #2729: what the re-stamp must NOT do — vouch for an object that carries no
/// identity of its own.
///
/// A segment sealed by a writer with no table binding (a legacy object, or one
/// whose lane could not resolve its uuid) is [`siglake_wal::WalOwner::Unmarked`],
/// and unmarked is "no opinion" everywhere: the prefix marker decides. Under a
/// prefix that has been stamped for a dropped incarnation, letting it decide
/// re-opens #2661 — the marker's move to the live table would adopt exactly the
/// objects the old refusal was protecting the replacement from. So the marker
/// records the owner it displaced, and for as long as it does, an unstamped
/// object is refused too — on the cycle that re-stamps and on every cycle
/// after it.
#[tokio::test]
async fn an_unstamped_mirrored_segment_is_refused_under_a_re_stamped_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let prefix = "wal-mirror";
    let store = memory_op();
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    let config = siglake_core::index_config::IndexConfig {
        index_id: "mirr_legacy".to_string(),
        doc_mapping: siglake_core::index_config::IndexConfig::builtin_events().doc_mapping,
        retention: None,
        index_at_flush: None,
    };
    ice.create_index(&config).await.unwrap();
    let dropped_uuid = ice.index_table_uuid("mirr_legacy").await.unwrap().unwrap();
    // An unstamped object drains fine while the prefix has only ever named one
    // table: this is the pre-#2693 fleet, and refusing it would strand rows.
    seed_index_segment(
        tmp.path(),
        "ing-legacy-old",
        "default",
        "mirr_legacy",
        synth("old", 4),
        &store,
        prefix,
    )
    .await;

    let claim_uri = format!(
        "sqlite://{}?mode=rwc",
        tmp.path().join("claim.db").display()
    );
    let claim = SqlSegmentClaim::connect(&claim_uri, "pod-1").await.unwrap();
    let cfg = |claim: SqlSegmentClaim| CatalogClaimConfig {
        claim,
        store: store.clone(),
        prefix: prefix.to_string(),
        batch_size: 8,
        last_mirror_sync: Default::default(),
        last_reclaim: Default::default(),
    };
    let c =
        Compactor::new(tmp.path().join("wal"), ice.clone()).with_catalog_claim(cfg(claim.clone()));
    assert_eq!(c.run_once().await.unwrap(), 1);
    assert_eq!(count_index_rows(&ice, "mirr_legacy").await, 4);
    assert_eq!(
        mirror_owner(&store, prefix, "default", "mirr_legacy").await,
        (dropped_uuid.clone(), Vec::new())
    );

    assert!(ice.delete_index("mirr_legacy").await.unwrap());
    ice.create_index(&config).await.unwrap();
    let ice_new = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let live_uuid = ice_new
        .index_table_uuid("mirr_legacy")
        .await
        .unwrap()
        .unwrap();
    // Two more legacy objects, no identity in either, and one the replacement
    // stamped itself.
    seed_index_segment(
        tmp.path(),
        "ing-legacy-a",
        "default",
        "mirr_legacy",
        synth("legacy-a", 3),
        &store,
        prefix,
    )
    .await;
    seed_index_segment_owned(
        tmp.path(),
        "ing-live",
        "default",
        "mirr_legacy",
        synth("live", 5),
        &store,
        prefix,
        Some(&live_uuid),
    )
    .await;

    let c2 = Compactor::new(tmp.path().join("wal"), ice_new.clone())
        .with_catalog_claim(cfg(claim.clone()));
    assert_eq!(
        c2.run_once().await.unwrap(),
        1,
        "only the object that names the replacement itself commits"
    );
    assert_eq!(
        count_index_rows(&ice_new, "mirr_legacy").await,
        5,
        "the unstamped object's rows must not land in the replacement"
    );
    assert_eq!(
        mirror_owner(&store, prefix, "default", "mirr_legacy").await,
        (live_uuid.clone(), vec![dropped_uuid.clone()])
    );
    // #2746: the strict refusal is as terminal as a wrong-uuid one — the prefix
    // will name the displaced table for as long as the marker exists — so it is
    // set aside on the refusing cycle rather than retried twelve times.
    assert_eq!(
        claim.quarantined_count().await.unwrap(),
        1,
        "the unstamped object was released for retry instead of set aside at once"
    );

    // The refusal outlives the cycle that re-stamped: a claim batch covers
    // what it covers, so an unstamped object first seen two cycles later must
    // get the same answer, not be adopted because the marker now reads clean.
    // Each later cycle gets a fresh config so its recovery sweep is due — the
    // sweep that registers a newly mirrored object is interval-gated per
    // config, and tests do not touch the environment.
    seed_index_segment(
        tmp.path(),
        "ing-legacy-b",
        "default",
        "mirr_legacy",
        synth("legacy-b", 7),
        &store,
        prefix,
    )
    .await;
    let c3 = Compactor::new(tmp.path().join("wal"), ice_new.clone())
        .with_catalog_claim(cfg(claim.clone()));
    assert_eq!(c3.run_once().await.unwrap(), 0);
    assert_eq!(count_index_rows(&ice_new, "mirr_legacy").await, 5);
    assert_eq!(
        mirrored_objects(&store, prefix, "default", "mirr_legacy").await,
        4,
        "every refused object is still in the mirror for an operator"
    );
    assert_eq!(
        claim.quarantined_count().await.unwrap(),
        2,
        "an unstamped object first seen a later cycle takes the same path"
    );

    // And the replacement still drains its own.
    seed_index_segment_owned(
        tmp.path(),
        "ing-live-2",
        "default",
        "mirr_legacy",
        synth("live2", 2),
        &store,
        prefix,
        Some(&live_uuid),
    )
    .await;
    let c4 = Compactor::new(tmp.path().join("wal"), ice_new.clone())
        .with_catalog_claim(cfg(claim.clone()));
    assert_eq!(c4.run_once().await.unwrap(), 1);
    assert_eq!(count_index_rows(&ice_new, "mirr_legacy").await, 7);
}

/// #2693, the catalog-claim half of
/// `query_server::indexes::a_writer_held_open_across_recreation_cannot_reach_the_replacement`.
///
/// The mirror-side owner marker is per key PREFIX, and the prefix is routed by
/// index NAME. Once a drain has stamped it for the replacement, it vouches for
/// every object uploaded under it afterwards — including one a stale writer
/// mirrors while still bound to the dropped incarnation. The prefix marker
/// cannot tell those apart; the segment's own frame header can.
///
/// The refused segment's claim is QUARANTINED, not committed and not deleted,
/// and the rest of the group still commits: one contaminant does not strand the
/// replacement's own rows.
#[tokio::test]
async fn a_mirrored_segment_from_a_dropped_incarnation_is_refused_after_the_prefix_is_restamped() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let prefix = "wal-mirror";
    let store = memory_op();
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    let config = siglake_core::index_config::IndexConfig {
        index_id: "mirr_held".to_string(),
        doc_mapping: siglake_core::index_config::IndexConfig::builtin_events().doc_mapping,
        retention: None,
        index_at_flush: None,
    };
    ice.create_index(&config).await.unwrap();
    let dropped_uuid = ice.index_table_uuid("mirr_held").await.unwrap().unwrap();

    // Delete and recreate BEFORE any drain, so the prefix marker is absent and
    // #2661's prefix check has no opinion — exactly the filesystem test's
    // setup, and the case where only the per-segment header knows.
    assert!(ice.delete_index("mirr_held").await.unwrap());
    ice.create_index(&config).await.unwrap();
    let ice_new = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let live_uuid = ice_new
        .index_table_uuid("mirr_held")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(live_uuid, dropped_uuid, "recreation is a new table");

    // The stale writer's object and the replacement's own object land under the
    // same prefix, claimed in the same group.
    seed_index_segment_owned(
        tmp.path(),
        "ing-held",
        "default",
        "mirr_held",
        synth("stale", 3),
        &store,
        prefix,
        Some(&dropped_uuid),
    )
    .await;
    seed_index_segment_owned(
        tmp.path(),
        "ing-live",
        "default",
        "mirr_held",
        synth("live", 5),
        &store,
        prefix,
        Some(&live_uuid),
    )
    .await;

    let claim_uri = format!(
        "sqlite://{}?mode=rwc",
        tmp.path().join("claim.db").display()
    );
    let claim = SqlSegmentClaim::connect(&claim_uri, "pod-1").await.unwrap();
    let c = Compactor::new(tmp.path().join("wal"), ice_new.clone()).with_catalog_claim(
        CatalogClaimConfig {
            claim: claim.clone(),
            store: store.clone(),
            prefix: prefix.to_string(),
            batch_size: 8,
            last_mirror_sync: Default::default(),
            last_reclaim: Default::default(),
        },
    );

    assert_eq!(
        c.run_once().await.unwrap(),
        1,
        "the replacement's own segment commits; the contaminant does not"
    );
    let owner_key = siglake_wal::mirror::mirror_owner_key(prefix, "default", "mirr_held");
    assert_eq!(
        String::from_utf8(store.read(&owner_key).await.unwrap().to_vec())
            .unwrap()
            .trim(),
        live_uuid,
        "the prefix now vouches for the replacement, which is why the header must decide"
    );
    assert_eq!(
        count_index_rows(&ice_new, "mirr_held").await,
        5,
        "only the replacement's five rows; the dropped incarnation's three are refused"
    );

    // #2746: set aside on the cycle that refuses it. The verdict is terminal by
    // construction (the dropped table's uuid is never reused), so the twelve
    // release attempts only bought twelve more GETs of the same bytes.
    assert_eq!(
        claim.quarantined_count().await.unwrap(),
        1,
        "the contaminant was released for retry instead of set aside at once"
    );

    // Still refused on the next cycle, and its bytes are still in the mirror:
    // held for an operator, never silently dropped.
    assert_eq!(c.run_once().await.unwrap(), 0);
    assert_eq!(count_index_rows(&ice_new, "mirr_held").await, 5);
    let listed = store.list(&format!("{prefix}/default/mirr_held/")).await;
    assert_eq!(
        listed
            .unwrap()
            .iter()
            .filter(|e| e.path().ends_with(".arrow"))
            .count(),
        2,
        "both objects remain in the mirror"
    );
}

/// #2745: the refusal above is charted (`deploy/grafana/siglake-overview.json`
/// panel 156), and the panel breaks the counter down `by (tenant, index)`.
/// The mirror-path increment used to hardcode `tenant="mirror"`, which
/// collapsed every tenant and index into one series — a non-zero rate an
/// operator could read but not attribute — even though the group loop it sits
/// in is keyed by exactly those two. This asserts the label set, not just the
/// count.
#[tokio::test]
async fn a_refused_mirrored_segment_counts_under_the_groups_tenant_and_index() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let prefix = "wal-mirror";
    let store = memory_op();
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    let config = siglake_core::index_config::IndexConfig {
        index_id: "mirr_label".to_string(),
        doc_mapping: siglake_core::index_config::IndexConfig::builtin_events().doc_mapping,
        retention: None,
        index_at_flush: None,
    };
    ice.create_index(&config).await.unwrap();
    let dropped_uuid = ice.index_table_uuid("mirr_label").await.unwrap().unwrap();
    assert!(ice.delete_index("mirr_label").await.unwrap());
    ice.create_index(&config).await.unwrap();
    let ice_new = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    // One object, bound to the incarnation that no longer exists.
    seed_index_segment_owned(
        tmp.path(),
        "ing-stale",
        "default",
        "mirr_label",
        synth("stale", 3),
        &store,
        prefix,
        Some(&dropped_uuid),
    )
    .await;

    let claim_uri = format!(
        "sqlite://{}?mode=rwc",
        tmp.path().join("claim.db").display()
    );
    let claim = SqlSegmentClaim::connect(&claim_uri, "pod-1").await.unwrap();
    let c = Compactor::new(tmp.path().join("wal"), ice_new.clone()).with_catalog_claim(
        CatalogClaimConfig {
            claim,
            store: store.clone(),
            prefix: prefix.to_string(),
            batch_size: 8,
            last_mirror_sync: Default::default(),
            last_reclaim: Default::default(),
        },
    );

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    {
        let _guard = metrics::set_default_local_recorder(&recorder);
        assert_eq!(c.run_once().await.unwrap(), 0, "the only object is refused");
    }
    let refusals: Vec<(Vec<(String, String)>, u64)> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| key.key().name() == "siglake_compactor_wal_stale_segments_total")
        .map(|(key, _, _, value)| {
            let mut labels: Vec<(String, String)> = key
                .key()
                .labels()
                .map(|l| (l.key().to_string(), l.value().to_string()))
                .collect();
            labels.sort();
            let DebugValue::Counter(n) = value else {
                panic!("{} is a counter", key.key().name())
            };
            (labels, n)
        })
        .collect();
    assert_eq!(
        refusals,
        vec![(
            vec![
                ("index".to_string(), "mirr_label".to_string()),
                ("tenant".to_string(), "default".to_string()),
            ],
            1
        )],
        "one refusal, attributed to the group's own tenant and index"
    );
}

/// #2836, the mirrored-drain half: the index is recreated AFTER the owner
/// marker and the segment's own header have both placed the group in the table
/// this cycle verified, and BEFORE the append resolves the index name.
///
/// The per-segment gate (#2693) and the transaction's uuid fence guard
/// different intervals, and neither covers this one: the gate compares against
/// an identity that was true when it ran, and a replacement loaded before
/// `Transaction::new` is that transaction's own valid base. So the drain
/// carries the verified identity into the append, which refuses before it
/// writes a file.
///
/// The refusal releases the claim rather than committing it: the replacement
/// gets no rows, no snapshot and no consumed proof, and the ordinary path takes
/// the segment from there — the next cycle runs the real check, re-stamps the
/// prefix and quarantines it.
#[tokio::test]
async fn a_recreation_before_the_mirrored_append_loads_its_table_commits_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let prefix = "wal-mirror";
    let store = memory_op();
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    let config = siglake_core::index_config::IndexConfig {
        index_id: "mirr_race".to_string(),
        doc_mapping: siglake_core::index_config::IndexConfig::builtin_events().doc_mapping,
        retention: None,
        index_at_flush: None,
    };
    ice.create_index(&config).await.unwrap();
    let verified = ice.index_table_uuid("mirr_race").await.unwrap().unwrap();

    let claim_uri = format!(
        "sqlite://{}?mode=rwc",
        tmp.path().join("claim.db").display()
    );
    let claim = SqlSegmentClaim::connect(&claim_uri, "pod-1").await.unwrap();
    let cfg = |claim: SqlSegmentClaim| CatalogClaimConfig {
        claim,
        store: store.clone(),
        prefix: prefix.to_string(),
        batch_size: 8,
        last_mirror_sync: Default::default(),
        last_reclaim: Default::default(),
    };

    // The control: an ordinary cycle, where the name still resolves to the
    // table the check verified. It commits, which is the behaviour the fence
    // must not cost.
    let control = seed_index_segment_owned(
        tmp.path(),
        "ing-control",
        "default",
        "mirr_race",
        synth("control", 4),
        &store,
        prefix,
        Some(&verified),
    )
    .await;
    let c =
        Compactor::new(tmp.path().join("wal"), ice.clone()).with_catalog_claim(cfg(claim.clone()));
    assert_eq!(c.run_once().await.unwrap(), 1);
    assert_eq!(count_index_rows(&ice, "mirr_race").await, 4);

    // Same id, different table.
    assert!(ice.delete_index("mirr_race").await.unwrap());
    ice.create_index(&config).await.unwrap();
    let ice_new = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let live = ice_new
        .index_table_uuid("mirr_race")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(live, verified, "recreation is a new table");

    // A writer that has not noticed keeps mirroring the dropped incarnation's
    // rows under the same key prefix.
    seed_index_segment_owned(
        tmp.path(),
        "ing-stale",
        "default",
        "mirr_race",
        synth("stale", 3),
        &store,
        prefix,
        Some(&verified),
    )
    .await;

    // The window. Everything after the verdict is the shipped path: the
    // per-segment header gate admits the object (it names the table the verdict
    // names), the claim is taken, the bytes are fetched, and the append is the
    // first step that can see the recreation.
    let c2 = Compactor::new(tmp.path().join("wal"), ice_new.clone())
        .with_catalog_claim(cfg(claim.clone()))
        .with_verified_owner_for_test(&verified);
    let err = c2
        .run_once()
        .await
        .expect_err("the cycle must fail rather than commit into the replacement");
    let text = format!("{err:#}");
    assert!(
        text.contains(&verified) && text.contains("dropped and recreated"),
        "the error must name the incarnation boundary: {text}"
    );

    // The replacement received nothing at all.
    assert_eq!(count_index_rows(&ice_new, "mirr_race").await, 0);
    let table = ice_new
        .catalog()
        .load_table(&ice_new.index_table_ident("mirr_race"))
        .await
        .unwrap();
    assert_eq!(
        table.metadata().snapshots().count(),
        0,
        "the replacement must have no snapshot of its own"
    );
    assert!(
        table
            .metadata()
            .properties()
            .get(siglake_storage::consumed_proof::CONSUMED_PROOF_PROP)
            .is_none(),
        "and no consumed proof: a proof here retires the ingester's copy of rows \
         this table does not have"
    );

    // The segment is not marked committed. It is released for retry, its bytes
    // are still in the mirror, and nothing is quarantined yet — the refusal
    // happened before the check that decides that.
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        claim
            .purgeable_committed(Duration::ZERO, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        vec![control],
        "only the control segment is committed"
    );
    assert_eq!(claim.quarantined_count().await.unwrap(), 0);
    assert_eq!(claim.peek_pending().await.unwrap().segments, 1);
    assert_eq!(
        mirrored_objects(&store, prefix, "default", "mirr_race").await,
        2,
        "the refused object is left in the mirror, not deleted"
    );

    // And the ordinary path finishes the job: the next cycle's real check
    // re-stamps the prefix for the replacement and sets the segment aside.
    // (`release` backs the claim off for a second before it can be re-claimed.)
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let c3 = Compactor::new(tmp.path().join("wal"), ice_new.clone())
        .with_catalog_claim(cfg(claim.clone()));
    assert_eq!(c3.run_once().await.unwrap(), 0);
    assert_eq!(claim.quarantined_count().await.unwrap(), 1);
    assert_eq!(count_index_rows(&ice_new, "mirr_race").await, 0);
}

/// The durable acknowledgement watermark this table records, or `None` when it
/// has no consumed-proof property at all.
async fn acknowledged_through(ice: &IcebergContext, table: &str) -> Option<i64> {
    match ice.reclaim_proof_sources(table).await.unwrap().durable {
        siglake_storage::consumed_proof::ConsumedProofRead::Valid(proof) => {
            proof.acknowledged_through_ms
        }
        other => {
            assert!(
                matches!(
                    other,
                    siglake_storage::consumed_proof::ConsumedProofRead::Absent
                ),
                "proof must not be corrupt: {other:?}"
            );
            None
        }
    }
}

/// #2889: the claim store keys its acknowledgement watermark by
/// `(tenant, index NAME)` and the maintenance compaction resolves the same name
/// to a table. After a `DELETE` + `POST` of that id both sides still speak for
/// the dropped incarnation, and the boundary lands on a table that never held
/// the segments it describes — where `compact_through` also DROPS the
/// replacement's own retained entries at or below it, which are the evidence
/// reclaim reads before it dares requeue a claim.
///
/// This is the interval where the recreation has already COMPLETED before
/// maintenance starts, so neither side sees anything change under it. The
/// storage-side fence covers the other interval, between the watermark read and
/// the commit (`consumed_proof_maintenance_incarnation.rs`).
#[tokio::test]
async fn maintenance_does_not_acknowledge_a_replacement_for_the_dropped_incarnation() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let prefix = "wal-mirror";
    let store = memory_op();
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    let config = siglake_core::index_config::IndexConfig {
        index_id: "mirr_ack".to_string(),
        doc_mapping: siglake_core::index_config::IndexConfig::builtin_events().doc_mapping,
        retention: None,
        index_at_flush: None,
    };
    ice.create_index(&config).await.unwrap();
    let dropped_uuid = ice.index_table_uuid("mirr_ack").await.unwrap().unwrap();

    let claim_uri = format!(
        "sqlite://{}?mode=rwc",
        tmp.path().join("claim.db").display()
    );
    let claim = SqlSegmentClaim::connect(&claim_uri, "pod-1").await.unwrap();
    let cfg = |claim: SqlSegmentClaim| CatalogClaimConfig {
        claim,
        store: store.clone(),
        prefix: prefix.to_string(),
        batch_size: 8,
        last_mirror_sync: Default::default(),
        last_reclaim: Default::default(),
    };

    seed_index_segment_owned(
        tmp.path(),
        "ack-old",
        "default",
        "mirr_ack",
        synth("old", 4),
        &store,
        prefix,
        Some(&dropped_uuid),
    )
    .await;
    let c =
        Compactor::new(tmp.path().join("wal"), ice.clone()).with_catalog_claim(cfg(claim.clone()));
    assert_eq!(c.run_once().await.unwrap(), 1);
    let watermark = claim
        .consumed_proof_watermark("default", "mirr_ack")
        .await
        .unwrap()
        .expect("a terminal claim establishes the watermark");
    assert_eq!(
        watermark.table_uuid.as_deref(),
        Some(dropped_uuid.as_str()),
        "the watermark records the incarnation whose commit established it"
    );

    // DELETE + POST of the same id. Both the claim-store key and the ident the
    // maintenance pass builds are unchanged by this.
    assert!(ice.delete_index("mirr_ack").await.unwrap());
    ice.create_index(&config).await.unwrap();
    let ice_new = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let live_uuid = ice_new.index_table_uuid("mirr_ack").await.unwrap().unwrap();
    assert_ne!(live_uuid, dropped_uuid);
    assert_eq!(
        acknowledged_through(&ice_new, "mirr_ack").await,
        None,
        "a freshly created table acknowledges nothing"
    );

    let maint = Compactor::new(tmp.path().join("wal"), ice_new.clone())
        .with_catalog_claim(cfg(claim.clone()))
        .with_reclustering(siglake_compactor::ReclusterConfig::new(Duration::ZERO));
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    {
        let _guard = metrics::set_default_local_recorder(&recorder);
        maint.run_recluster_once().await.unwrap();
    }
    let skipped = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find(|(key, _, _, _)| {
            key.key().name() == "siglake_compactor_proof_watermark_skipped_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "reason" && label.value() == "incarnation_mismatch")
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "index" && label.value() == "mirr_ack")
        })
        .map(|(_, _, _, value)| value);
    assert_eq!(
        skipped,
        Some(DebugValue::Counter(1)),
        "the incarnation mismatch is exposed as one skipped proof watermark"
    );

    assert_eq!(
        acknowledged_through(&ice_new, "mirr_ack").await,
        None,
        "the replacement's proof property is untouched: the stored boundary was \
         established by a table this name no longer resolves to"
    );
    assert_eq!(
        claim
            .consumed_proof_watermark("default", "mirr_ack")
            .await
            .unwrap()
            .and_then(|w| w.table_uuid),
        Some(dropped_uuid.clone()),
        "and the refusal did not relabel the stored watermark with the live uuid"
    );

    // Fail-closed, not wedged: the replacement's own drain re-establishes the
    // watermark under its own incarnation, and maintenance then applies it.
    seed_index_segment_owned(
        tmp.path(),
        "ack-live",
        "default",
        "mirr_ack",
        synth("live", 2),
        &store,
        prefix,
        Some(&live_uuid),
    )
    .await;
    let c2 = Compactor::new(tmp.path().join("wal"), ice_new.clone())
        .with_catalog_claim(cfg(claim.clone()));
    assert_eq!(c2.run_once().await.unwrap(), 1);
    let watermark = claim
        .consumed_proof_watermark("default", "mirr_ack")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(watermark.table_uuid.as_deref(), Some(live_uuid.as_str()));
    let maint2 = Compactor::new(tmp.path().join("wal"), ice_new.clone())
        .with_catalog_claim(cfg(claim.clone()))
        .with_reclustering(siglake_compactor::ReclusterConfig::new(Duration::ZERO));
    maint2.run_recluster_once().await.unwrap();
    assert_eq!(
        acknowledged_through(&ice_new, "mirr_ack").await,
        Some(watermark.acknowledged_through_ms),
        "the live incarnation's own boundary is applied"
    );

    // Steady state: the drain's own append carries the boundary again, with no
    // maintenance pass between here and the assertion. The same comparison
    // gates both paths, so a fence too strict to admit an index's own boundary
    // would leave the table at the value `maint2` wrote. Two cycles, because a
    // cycle's append reads the boundary its own terminal claim then advances:
    // the first moves the stored boundary past that value, the second applies
    // it.
    for (n, name) in [(1, "ack-live-2"), (2, "ack-live-3")] {
        tokio::time::sleep(Duration::from_millis(5)).await;
        seed_index_segment_owned(
            tmp.path(),
            name,
            "default",
            "mirr_ack",
            synth(&format!("live{n}"), 1),
            &store,
            prefix,
            Some(&live_uuid),
        )
        .await;
        let c = Compactor::new(tmp.path().join("wal"), ice_new.clone())
            .with_catalog_claim(cfg(claim.clone()));
        assert_eq!(c.run_once().await.unwrap(), 1);
    }
    assert!(
        acknowledged_through(&ice_new, "mirr_ack").await > Some(watermark.acknowledged_through_ms),
        "the append applied the boundary it read for its own incarnation"
    );
}
