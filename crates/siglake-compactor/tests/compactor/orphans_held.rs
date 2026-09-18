//! #3267: `siglake_compactor_orphans_held{tenant}` is what
//! `SiglakeCompactorOrphansHeld` pages on, so it has to be a level a tenant's
//! whole WAL layout agrees on and one that comes back down.
//!
//! Two readings made it neither. Orphan disposition wrote the gauge once per
//! directory, so a tenant's events pass and its index passes overwrote each
//! other and one held orphan was hidden by any later directory that had none;
//! and a directory with no orphans returned before writing anything, so a
//! hold an operator had resolved kept its last non-zero reading for the life
//! of the process. The first would page on the wrong number, the second would
//! page forever.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

use siglake_compactor::Compactor;
use siglake_core::index_config::IndexConfig;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_wal::{WalWriter, ORPHANS_DIR};

const TENANT: &str = "acme";
const GAUGE: &str = "siglake_compactor_orphans_held";

/// Seal exactly `count` segments into `dir`, one event apiece.
fn seal_n(dir: &Path, ingester: &str, count: usize) {
    let mut writer = WalWriter::with_thresholds(dir, ingester, 1, Duration::from_secs(60)).unwrap();
    for i in 0..count {
        writer
            .append_events(&[Event::now(format!("{ingester}-{i}"))])
            .unwrap()
            .expect("one event meets the one-event threshold, so every append seals");
    }
}

/// Quarantine one freshly sealed segment under `dir`'s `orphans/` with an
/// mtime old enough that snapshot expiry could have dropped the snapshot that
/// would prove its commit status. That is the ambiguous disposition: the
/// compactor holds the file and an operator has to settle it.
///
/// The directory's table must already carry a snapshot, or the retained
/// history covers the segment's whole life and disposition requeues it as
/// provably uncommitted instead.
fn hold_one_orphan(dir: &Path, ingester: &str) -> PathBuf {
    seal_n(dir, ingester, 1);
    let mut sealed = siglake_wal::list_sealed(dir).unwrap();
    assert_eq!(sealed.len(), 1, "one segment to quarantine in {dir:?}");
    let sealed = sealed.remove(0);
    let orphans = dir.join(ORPHANS_DIR);
    std::fs::create_dir_all(&orphans).unwrap();
    let dest = orphans.join(sealed.file_name().unwrap());
    std::fs::rename(&sealed, &dest).unwrap();
    let f = std::fs::File::options().write(true).open(&dest).unwrap();
    f.set_modified(SystemTime::now() - Duration::from_secs(3_600))
        .unwrap();
    dest
}

/// The tenant's exported hold, or `None` when the cycle published nothing.
///
/// The debugging recorder keys by (name, labels), so a gauge written several
/// times in one cycle appears once holding the last value written — the shape
/// of the overwrite bug, and why this reads a single number.
fn published_held(snapshotter: &Snapshotter) -> Option<f64> {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find(|(key, _, _, _)| {
            key.key().name() == GAUGE
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "tenant" && label.value() == TENANT)
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Gauge(g) => g.into_inner(),
            other => panic!("{GAUGE} must be a gauge, got {other:?}"),
        })
}

async fn tenant_context(warehouse: &Path) -> (Arc<IcebergContext>, IcebergContext) {
    let ice = Arc::new(IcebergContext::open(warehouse).await.unwrap());
    let tenant_ice = ice
        .for_namespace(&format!("tenant_{TENANT}"))
        .await
        .unwrap();
    (ice, tenant_ice)
}

/// A tenant holds one ambiguous orphan in its events directory and one in a
/// managed index. The exported reading is the tenant's total, whichever
/// directory the sweep visits last.
#[tokio::test]
async fn a_tenants_held_orphans_sum_over_all_its_directories() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let tenant_dir = wal_root.join(TENANT);
    let index_dir = tenant_dir.join("idx_a");

    let (ice, tenant_ice) = tenant_context(&tmp.path().join("warehouse")).await;
    tenant_ice
        .create_index(&IndexConfig {
            index_id: "idx_a".to_string(),
            doc_mapping: IndexConfig::builtin_events().doc_mapping,
            retention: None,
            index_at_flush: None,
        })
        .await
        .unwrap();

    // Both tables need a snapshot before an orphan can be ambiguous: with no
    // history at all, disposition knows nothing was ever committed.
    let compactor = Compactor::new(&wal_root, ice.clone());
    seal_n(&tenant_dir, "ing-events", 1);
    seal_n(&index_dir, "ing-index", 1);
    assert_eq!(
        compactor.run_once().await.unwrap(),
        2,
        "one segment committed per directory"
    );

    hold_one_orphan(&tenant_dir, "orphan-events");
    hold_one_orphan(&index_dir, "orphan-index");

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    {
        let _guard = metrics::set_default_local_recorder(&recorder);
        compactor.run_once().await.unwrap();
    }
    // `idx_a` is visited after the events directory, and ahead of the fix its
    // `set` published 1 over the 1 the events pass had already reported.
    assert_eq!(
        published_held(&snapshotter),
        Some(2.0),
        "both held orphans are the tenant's reading"
    );
}

/// The other half: once an operator settles the hold, the gauge has to reach
/// zero without the pod restarting. Ahead of the fix the cycle that found an
/// empty `orphans/` returned before touching it, so the page never cleared.
///
/// One recorder across all three cycles, on purpose: a fresh one per cycle
/// would report the retained value as an absent series rather than as the
/// stale reading a running process would keep exporting.
#[tokio::test]
async fn a_resolved_hold_publishes_zero_on_the_next_cycle() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let tenant_dir = wal_root.join(TENANT);

    let (ice, _tenant_ice) = tenant_context(&tmp.path().join("warehouse")).await;
    let compactor = Compactor::new(&wal_root, ice);

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);

    seal_n(&tenant_dir, "ing-events", 1);
    assert_eq!(compactor.run_once().await.unwrap(), 1);
    assert_eq!(
        published_held(&snapshotter),
        Some(0.0),
        "a tenant with no orphans reports zero rather than no series"
    );

    let orphan = hold_one_orphan(&tenant_dir, "orphan-events");
    compactor.run_once().await.unwrap();
    assert_eq!(published_held(&snapshotter), Some(1.0), "the hold is paged");

    // The operator settles it: proof of commit status, then the file is gone.
    std::fs::remove_file(&orphan).unwrap();
    compactor.run_once().await.unwrap();
    assert_eq!(
        published_held(&snapshotter),
        Some(0.0),
        "a resolved hold clears on the next cycle"
    );
}

/// An index name that resolves to no table exits the sweep before disposition
/// runs, and stays that way on every later cycle. Its quarantine is
/// unclassified rather than resolved, so it must not read as zero.
#[tokio::test]
async fn an_unresolved_index_still_reports_the_orphans_nothing_classified() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let tenant_dir = wal_root.join(TENANT);

    let (ice, _tenant_ice) = tenant_context(&tmp.path().join("warehouse")).await;
    let compactor = Compactor::new(&wal_root, ice);
    seal_n(&tenant_dir, "ing-events", 1);
    assert_eq!(compactor.run_once().await.unwrap(), 1);

    // `ghost` is a WAL directory for an index no catalog entry names, so
    // `ensure_index` resolves nothing and the sweep skips the directory.
    hold_one_orphan(&tenant_dir.join("ghost"), "orphan-ghost");

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    {
        let _guard = metrics::set_default_local_recorder(&recorder);
        compactor.run_once().await.unwrap();
    }
    assert_eq!(
        published_held(&snapshotter),
        Some(1.0),
        "an orphan no disposition reached is still held for an operator"
    );
}
