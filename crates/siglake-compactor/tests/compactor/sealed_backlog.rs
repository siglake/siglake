//! #3691: `siglake_compactor_sealed_pending{tenant}` must describe a tenant's
//! whole filesystem queue, not the last directory the sweep happened to visit.
//!
//! A tenant's sealed segments live in its events directory and in one
//! directory per managed index. Every one of those used to `set` the same
//! tenant-labelled gauge in turn, so the exported value was whichever
//! directory came last in name order — a small, quiet index at the end of the
//! alphabet published a low backlog over whatever was queued ahead of it, and
//! the compactor HPA scaled on that.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

use siglake_compactor::Compactor;
use siglake_core::index_config::IndexConfig;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_wal::WalWriter;

const TENANT: &str = "acme";
const GAUGE: &str = "siglake_compactor_sealed_pending";
const PASS_CLAIM: &str = siglake_compactor::PASS_CLAIM_ATTEMPTS_EXHAUSTED;

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

/// The tenant's published backlog, or `None` when the cycle published nothing.
///
/// The debugging recorder keys by (name, labels), so a gauge written several
/// times in one cycle appears once holding the last value written — which is
/// exactly the shape of the bug, and why this reads a single number.
fn published_backlog(snapshotter: &Snapshotter) -> Option<f64> {
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

/// The tenant's pass-claim counter, or `None` when the cycle never registered
/// the series.
///
/// `Snapshotter::snapshot` swaps every counter back to 0 as it reads, so a
/// test that takes two snapshots reads deltas; each caller here takes one.
fn published_pass_claim(snapshotter: &Snapshotter, tenant: &str) -> Option<u64> {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find(|(key, _, _, _)| {
            key.key().name() == PASS_CLAIM
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "tenant" && label.value() == tenant)
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => c,
            other => panic!("{PASS_CLAIM} must be a counter, got {other:?}"),
        })
}

/// A tenant whose events directory is empty and whose queue sits in two
/// managed indexes of unequal size. Returns the backlog the cycle published.
async fn backlog_for(indexes: &[(&str, usize)]) -> f64 {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let tenant_dir = wal_root.join(TENANT);
    // `list_tenant_dirs` recognizes a tenant by its own `sealed/`. Leaving it
    // empty puts the tenant's entire queue in the index directories, where the
    // overwrite used to bury it.
    std::fs::create_dir_all(tenant_dir.join("sealed")).unwrap();

    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let tenant_ice = ice
        .for_namespace(&format!("tenant_{TENANT}"))
        .await
        .unwrap();
    for (index, count) in indexes {
        tenant_ice
            .create_index(&IndexConfig {
                index_id: (*index).to_string(),
                doc_mapping: IndexConfig::builtin_events().doc_mapping,
                retention: None,
                index_at_flush: None,
            })
            .await
            .unwrap();
        seal_n(&tenant_dir.join(index), index, *count);
    }

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    {
        let _guard = metrics::set_default_local_recorder(&recorder);
        Compactor::new(&wal_root, ice.clone())
            .run_once()
            .await
            .unwrap();
    }
    published_backlog(&snapshotter).expect("a completed sweep publishes the tenant's backlog")
}

/// Unequal backlogs in two indexes, and the same two counts with the names
/// swapped so the sweep meets them in the opposite order. One tenant, one
/// total, unchanged by visit order.
#[tokio::test]
async fn a_tenants_backlog_sums_its_indexes_whatever_order_they_are_visited_in() {
    // Indexes are walked in name order, so `idx_z` is the one whose `set` used
    // to win. Ahead of the fix this arm published 1 and the next published 3.
    assert_eq!(backlog_for(&[("idx_a", 3), ("idx_z", 1)]).await, 4.0);
    assert_eq!(backlog_for(&[("idx_a", 1), ("idx_z", 3)]).await, 4.0);
}

/// The other half of the claim: the total must fall back to zero once the
/// queue is actually empty, or the fix would trade a hidden backlog for a
/// gauge that never comes down.
#[tokio::test]
async fn a_swept_tenant_with_nothing_left_publishes_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let tenant_dir = wal_root.join(TENANT);
    std::fs::create_dir_all(tenant_dir.join("sealed")).unwrap();

    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let tenant_ice = ice
        .for_namespace(&format!("tenant_{TENANT}"))
        .await
        .unwrap();
    tenant_ice
        .create_index(&IndexConfig {
            index_id: "idx_a".to_string(),
            doc_mapping: IndexConfig::builtin_events().doc_mapping,
            retention: None,
            index_at_flush: None,
        })
        .await
        .unwrap();
    seal_n(&tenant_dir.join("idx_a"), "idx_a", 2);

    let compactor = Compactor::new(&wal_root, ice.clone());
    for (cycle, expected) in [(1, 2.0), (2, 0.0)] {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        {
            let _guard = metrics::set_default_local_recorder(&recorder);
            compactor.run_once().await.unwrap();
        }
        assert_eq!(
            published_backlog(&snapshotter),
            Some(expected),
            "cycle {cycle}"
        );
    }
}

/// Removing a tenant WAL directory retires its exported label on the next
/// complete sweep instead of leaving the last non-zero backlog visible.
#[tokio::test]
async fn a_tenant_that_disappears_publishes_zero_on_the_next_cycle() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let tenant_dir = wal_root.join(TENANT);
    seal_n(&tenant_dir, TENANT, 2);

    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let compactor = Compactor::new(&wal_root, ice);
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);

    compactor.run_once().await.unwrap();
    assert_eq!(published_backlog(&snapshotter), Some(2.0), "cycle 1");

    std::fs::remove_dir_all(&tenant_dir).unwrap();
    compactor.run_once().await.unwrap();
    assert_eq!(published_backlog(&snapshotter), Some(0.0), "cycle 2");
}

/// A sweep that does not finish has not observed the tenant's queue, and the
/// total it has so far is short by every directory it never reached. It
/// publishes nothing, leaving the last complete reading standing rather than
/// handing the autoscaler a drop it did not measure.
#[tokio::test]
async fn a_cycle_that_fails_partway_publishes_no_backlog_at_all() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let tenant_dir = wal_root.join(TENANT);
    std::fs::create_dir_all(tenant_dir.join("sealed")).unwrap();

    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let tenant_ice = ice
        .for_namespace(&format!("tenant_{TENANT}"))
        .await
        .unwrap();
    for index in ["idx_a", "idx_z"] {
        tenant_ice
            .create_index(&IndexConfig {
                index_id: index.to_string(),
                doc_mapping: IndexConfig::builtin_events().doc_mapping,
                retention: None,
                index_at_flush: None,
            })
            .await
            .unwrap();
    }
    seal_n(&tenant_dir.join("idx_a"), "idx_a", 2);
    // `idx_z` is visited second and its one "segment" cannot be read, so the
    // cycle returns an error after `idx_a` has already been counted. The
    // header carries no table identity, so the ownership gate keeps it and the
    // failure lands where it is wanted: the commit.
    let poison = tenant_dir.join("idx_z").join("sealed");
    std::fs::create_dir_all(&poison).unwrap();
    std::fs::write(poison.join("poison.arrow"), b"not an arrow stream").unwrap();

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let started = Instant::now();
    {
        let _guard = metrics::set_default_local_recorder(&recorder);
        Compactor::new(&wal_root, ice.clone())
            .with_drain_cycle_budget(Duration::from_secs(2))
            .run_once()
            .await
            .expect_err("an unreadable segment fails its directory's commit");
    }
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "the failing cycle must return inside its own budget"
    );
    assert_eq!(
        published_backlog(&snapshotter),
        None,
        "an incomplete sweep must publish neither a partial total nor a zero"
    );
}

/// #5014: the sweep registers `siglake_compactor_pass_claim_attempts_exhausted_total`
/// at 0 for every tenant it publishes gauges for.
///
/// The counter is otherwise written only when a pass gives up re-claiming a
/// segment, so on a healthy compactor the series did not exist at all — and
/// #4723's reading of it, "the bound was never hit", could not be told apart
/// from a drain that never ran.
#[tokio::test]
async fn a_swept_tenant_registers_its_pass_claim_counter_at_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let tenant_dir = wal_root.join(TENANT);
    seal_n(&tenant_dir, TENANT, 2);

    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    {
        let _guard = metrics::set_default_local_recorder(&recorder);
        Compactor::new(&wal_root, ice).run_once().await.unwrap();
    }
    assert_eq!(
        published_pass_claim(&snapshotter, TENANT),
        Some(0),
        "a completed sweep registers the tenant's pass-claim counter at zero"
    );
}

/// The zero is what an operator scrapes, so assert it in exposition form,
/// against a recorder built the way `metrics::init` builds the global one.
#[tokio::test]
async fn a_healthy_scrape_carries_the_pass_claim_counter_at_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    // No tenant subdirectory: the legacy top-level layout, which the sweep
    // labels `default`. One tenant, two segments, nothing that fails.
    seal_n(&wal_root, "default", 2);

    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let recorder = siglake_core::metrics::builder()
        .expect("builder")
        .build_recorder();
    {
        let _guard = metrics::set_default_local_recorder(&recorder);
        Compactor::new(&wal_root, ice).run_once().await.unwrap();
    }
    let scrape = recorder.handle().render();
    assert!(
        scrape.contains(&format!("{PASS_CLAIM}{{tenant=\"default\"}} 0")),
        "{scrape}"
    );
}

/// Registering the series must not clear it: a tenant that withheld segments
/// keeps its total across the sweeps that follow, or the counter would report
/// only whatever happened since the last cycle and every `increase()` over it
/// would read a reset as a drop.
#[tokio::test]
async fn a_later_sweep_preserves_a_tenants_pass_claim_count() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let tenant_dir = wal_root.join(TENANT);
    seal_n(&tenant_dir, TENANT, 2);

    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let compactor = Compactor::new(&wal_root, ice);
    // A Prometheus recorder rather than the debugging one: `render` leaves the
    // counter standing, so the second cycle is read against the first cycle's
    // accumulated total and not against a drained zero.
    let recorder = siglake_core::metrics::builder()
        .expect("builder")
        .build_recorder();
    let scrape = {
        let _guard = metrics::set_default_local_recorder(&recorder);
        compactor.run_once().await.unwrap();
        // Stands in for a pass that gave up on two segments. Reproducing that
        // needs a cause that fails every claim, which `withhold_spent_segment`
        // already has its own coverage for; what is under test here is the
        // sweep that runs afterwards.
        metrics::counter!(PASS_CLAIM, "tenant" => TENANT.to_string()).increment(2);
        // A second, empty sweep: the tenant is still published, so `publish`
        // registers its counter again.
        compactor.run_once().await.unwrap();
        recorder.handle().render()
    };
    assert!(
        scrape.contains(&format!("{PASS_CLAIM}{{tenant=\"{TENANT}\"}} 2")),
        "{scrape}"
    );
}
