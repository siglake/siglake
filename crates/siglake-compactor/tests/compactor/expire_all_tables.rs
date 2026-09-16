//! Snapshot expiry must cover EVERY table, not just `events`.
//!
//! `run_expire_once` used `events_table_ident()` alone, so user indexes never
//! had snapshots trimmed. All benchmark data lives in a user index, and so does
//! every user-created index in production.
//!
//! The 2026-08-10 1TB round measured `logs-bench` at **626 snapshots p50**
//! against a `retain_last` of 100, still climbing. `metadata.json` is re-read and
//! re-parsed on EVERY append, so its size multiplies straight into `load_table` —
//! the cost that grew ~240x across a round (40 ms early, 18,930 ms late) and was
//! the largest unexplained bucket in the write path.

use std::sync::Arc;
use std::time::Duration;

use siglake_compactor::{Compactor, ExpireConfig};
use siglake_core::index_config::IndexConfig;
use siglake_core::{events_to_record_batch, Event};
use siglake_storage::iceberg::IcebergContext;

#[tokio::test]
async fn expiry_trims_user_indexes_not_only_events() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );

    // A user index — the shape all benchmark and customer data actually lives in.
    let mut cfg = IndexConfig::builtin_events();
    cfg.index_id = "logs-user".to_string();
    ice.create_index(&cfg).await.expect("create index");
    let ident = ice.index_table_ident("logs-user");

    // Each append is one snapshot. Build well past any sane retention.
    const COMMITS: usize = 12;
    for i in 0..COMMITS {
        let batch = events_to_record_batch(&[Event::now(format!("row {i}"))]).unwrap();
        ice.append_to_table(&ident, batch, &[])
            .await
            .expect("append to index");
    }
    let before = ice
        .snapshot_count_for(&ident)
        .await
        .expect("snapshot count");
    assert!(
        before >= COMMITS,
        "fixture must accumulate snapshots (got {before})"
    );

    let compactor = Compactor::new(tmp.path().join("wal"), ice.clone())
        .with_snapshot_expiry(ExpireConfig::new(Duration::from_secs(0), 3));
    compactor.run_expire_once().await.expect("expire");

    let after = ice
        .snapshot_count_for(&ident)
        .await
        .expect("snapshot count");
    assert!(
        after < before,
        "user-index snapshots must be trimmed: {before} -> {after}"
    );
    assert!(
        after <= 5,
        "must converge near retain_last=3, got {after} (was {before})"
    );
}
