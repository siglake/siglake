//! Per-tenant compactor sweep: seed sealed segments under multiple
//! tenant subdirs, run the compactor once, verify each tenant's
//! data lands in its own Iceberg namespace.

use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;

use siglake_compactor::Compactor;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_wal::WalWriter;
use siglake_wal::{ORPHANS_DIR, PROCESSING_DIR};
use std::fs;

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

fn synth(host: &str, n: usize) -> Vec<Event> {
    (0..n).map(|i| Event::now(format!("{host}-{i}"))).collect()
}

#[tokio::test]
async fn per_tenant_sweep_routes_segments_to_tenant_namespaces() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    // Seal segments for two tenants under their own subdirs.
    {
        let tenant_dir = wal_root.join("acme");
        let mut w =
            WalWriter::with_thresholds(&tenant_dir, "ing", 3, Duration::from_secs(60)).unwrap();
        w.append_events(&synth("acme", 3))
            .unwrap()
            .expect("acme should seal at threshold");
    }
    {
        let tenant_dir = wal_root.join("widgets");
        let mut w =
            WalWriter::with_thresholds(&tenant_dir, "ing", 5, Duration::from_secs(60)).unwrap();
        w.append_events(&synth("widgets", 5))
            .unwrap()
            .expect("widgets should seal at threshold");
    }

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let compactor = Compactor::new(&wal_root, ice.clone());
    let n = compactor.run_once().await.unwrap();
    assert_eq!(n, 2, "one segment per tenant, both committed");

    // The default `siglake` namespace must stay empty.
    assert_eq!(
        count_events(&ice).await,
        0,
        "default namespace must not receive tenant data"
    );

    // Each tenant's namespace must hold exactly its events.
    let acme = ice.for_namespace("tenant_acme").await.unwrap();
    assert_eq!(count_events(&acme).await, 3);
    let widgets = ice.for_namespace("tenant_widgets").await.unwrap();
    assert_eq!(count_events(&widgets).await, 5);
}

/// Regression for bug #21 (Phase 4.12.18): legacy top-level `sealed/`
/// segments + a leftover tenant subdir must both get processed in a
/// single `run_once`. Without this fix the compactor would only sweep
/// the tenant subdirs and orphan every segment at the top level —
/// observed in AWS smoke round 10 after the backpressure router (which
/// always writes per-tenant) wrote `default/` segments, then was
/// disabled and the ingester reverted to top-level layout.
#[tokio::test]
async fn legacy_top_level_and_tenant_subdir_are_both_swept() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    // Seal a segment at the LEGACY top-level layout.
    {
        let mut w = WalWriter::with_thresholds(&wal_root, "ing-legacy", 4, Duration::from_secs(60))
            .unwrap();
        w.append_events(&synth("legacy", 4))
            .unwrap()
            .expect("top-level should seal at threshold");
    }
    // ALSO seal a segment under a tenant subdir, simulating the
    // backpressure-router-then-disabled scenario.
    {
        let tenant_dir = wal_root.join("default");
        let mut w =
            WalWriter::with_thresholds(&tenant_dir, "ing-tenant", 6, Duration::from_secs(60))
                .unwrap();
        w.append_events(&synth("tenant", 6))
            .unwrap()
            .expect("tenant should seal at threshold");
    }

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let compactor = Compactor::new(&wal_root, ice.clone());
    let n = compactor.run_once().await.unwrap();
    assert_eq!(n, 2, "BOTH segments must commit in one cycle");

    // BOTH land in the default namespace: tenant "default" IS the default
    // namespace (WI-8 finding — committing it to `tenant_default` made
    // header-less ingest invisible to no-claim readers, and diverged from
    // the catalog-claim path + delete-task sweep which already special-case
    // it). Legacy top-level and `default/` subdir are the same tenant.
    assert_eq!(count_events(&ice).await, 10);

    // And `tenant_default` is NOT created as a side effect.
    let stray = ice.for_namespace("tenant_default").await.unwrap();
    assert_eq!(count_events(&stray).await, 0);
}

/// Phase 4.13h: `Compactor::new` quarantines any segments left in
/// `processing/` by a prior process. Round-12 saw 749 segments
/// accumulate there; the recovery prevents the same accumulation
/// after a pod rollover.
#[tokio::test]
async fn compactor_new_quarantines_processing_orphans() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    fs::create_dir_all(wal_root.join(PROCESSING_DIR)).unwrap();
    fs::write(
        wal_root.join(PROCESSING_DIR).join("orphan-a.arrow"),
        b"fake-arrow-bytes",
    )
    .unwrap();
    fs::write(
        wal_root.join(PROCESSING_DIR).join("orphan-b.arrow"),
        b"fake-arrow-bytes",
    )
    .unwrap();

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    // The constructor scans processing/ and quarantines orphans.
    let _c = Compactor::new(&wal_root, ice);

    // processing/ is empty.
    let processing_count = fs::read_dir(wal_root.join(PROCESSING_DIR))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .count();
    assert_eq!(processing_count, 0);

    // orphans/ has both files.
    let orphans_count = fs::read_dir(wal_root.join(ORPHANS_DIR))
        .unwrap()
        .filter_map(|e| e.ok())
        .count();
    assert_eq!(orphans_count, 2);
}

#[tokio::test]
async fn compactor_new_quarantines_processing_orphans_in_index_dirs() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    fs::create_dir_all(wal_root.join("acme").join("sealed")).unwrap();
    fs::create_dir_all(wal_root.join("acme").join("app1").join("sealed")).unwrap();
    let index_processing = wal_root.join("acme").join("app1").join(PROCESSING_DIR);
    fs::create_dir_all(&index_processing).unwrap();
    fs::write(
        index_processing.join("orphan-index.arrow"),
        b"fake-arrow-bytes",
    )
    .unwrap();

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let _c = Compactor::new(&wal_root, ice);

    let processing_count = fs::read_dir(&index_processing)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .count();
    assert_eq!(processing_count, 0);

    let orphans_count = fs::read_dir(wal_root.join("acme").join("app1").join(ORPHANS_DIR))
        .unwrap()
        .filter_map(|e| e.ok())
        .count();
    assert_eq!(orphans_count, 1);
}

/// #81: orphan auto-disposition. A quarantined segment whose basename the
/// table's cumulative consumed set names is provably committed → deleted
/// (re-committing would duplicate). One absent from the set with the retained
/// history covering its life is provably NOT committed → requeued to sealed/
/// and re-committed the same cycle. One absent but older than the retained
/// floor is ambiguous → held for ops. Net: the exact manual recovery the
/// Phase-4 round performed, automated, with the count staying exact.
#[tokio::test]
async fn orphan_auto_disposition_deletes_committed_requeues_unconsumed() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    // Segment A: 3 rows, committed normally (its basename enters the
    // consumed set; its file lands in committed/).
    {
        let mut w =
            WalWriter::with_thresholds(&wal_root, "ing-a", 3, Duration::from_secs(60)).unwrap();
        w.append_events(&synth("a", 3)).unwrap().expect("A seals");
    }
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let compactor = Compactor::new(&wal_root, ice.clone());
    assert_eq!(compactor.run_once().await.unwrap(), 1);
    assert_eq!(count_events(&ice).await, 3);

    let orphans_dir = wal_root.join(ORPHANS_DIR);
    fs::create_dir_all(&orphans_dir).unwrap();

    // Crash simulation 1: A's committed file reappears quarantined (the
    // ambiguous commit_claimed-succeeded case) — consumed set proves it.
    let committed_a = fs::read_dir(wal_root.join("committed"))
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().and_then(|x| x.to_str()) == Some("arrow"))
        .expect("A in committed/")
        .path();
    let a_name = committed_a.file_name().unwrap().to_owned();
    fs::copy(&committed_a, orphans_dir.join(&a_name)).unwrap();

    // Crash simulation 2: B (2 rows) sealed but never committed, quarantined.
    // Its mtime is safely past the history floor + skew margin → requeue.
    {
        let mut w =
            WalWriter::with_thresholds(&wal_root, "ing-b", 2, Duration::from_secs(60)).unwrap();
        w.append_events(&synth("b", 2)).unwrap().expect("B seals");
    }
    let sealed_b = siglake_wal::list_sealed(&wal_root).unwrap().remove(0);
    let b_name = sealed_b.file_name().unwrap().to_owned();
    let orphan_b = orphans_dir.join(&b_name);
    fs::rename(&sealed_b, &orphan_b).unwrap();
    let f = fs::File::options().write(true).open(&orphan_b).unwrap();
    f.set_modified(std::time::SystemTime::now() + Duration::from_secs(10))
        .unwrap();
    drop(f);

    // Crash simulation 3: C (1 row) uncommitted but OLDER than the retained
    // floor — expiry could have dropped its consuming snapshot → held.
    {
        let mut w =
            WalWriter::with_thresholds(&wal_root, "ing-c", 1, Duration::from_secs(60)).unwrap();
        w.append_events(&synth("c", 1)).unwrap().expect("C seals");
    }
    let sealed_c = siglake_wal::list_sealed(&wal_root).unwrap().remove(0);
    let c_name = sealed_c.file_name().unwrap().to_owned();
    let orphan_c = orphans_dir.join(&c_name);
    fs::rename(&sealed_c, &orphan_c).unwrap();
    let f = fs::File::options().write(true).open(&orphan_c).unwrap();
    f.set_modified(std::time::SystemTime::now() - Duration::from_secs(3_600))
        .unwrap();
    drop(f);

    // One cycle: A deleted (proven committed), B requeued + committed, C held.
    assert_eq!(
        compactor.run_once().await.unwrap(),
        1,
        "exactly B re-commits"
    );
    assert_eq!(
        count_events(&ice).await,
        5,
        "3 (A, not duplicated) + 2 (B recovered)"
    );
    let remaining: Vec<_> = fs::read_dir(&orphans_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name())
        .collect();
    assert_eq!(
        remaining,
        vec![c_name],
        "only the ambiguous orphan stays held"
    );

    // Idempotent: a further cycle changes nothing.
    assert_eq!(compactor.run_once().await.unwrap(), 0);
    assert_eq!(count_events(&ice).await, 5);
}
