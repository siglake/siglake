//! A consumed-proof acknowledgement belongs to the table INCARNATION whose
//! commit established it, not to the name.
//!
//! THE DEFECT THIS GUARDS (#2889). `Compactor::compact_acknowledged_proofs`
//! reads the boundary from the claim store keyed by `(tenant, index NAME)` and
//! hands it to `compact_consumed_proof`, which resolves the same name to a
//! table. After a `DELETE` + `POST` of that id both sides still speak for the
//! dropped incarnation, so the replacement receives an acknowledgement
//! describing segments it never held — and `ConsumedProof::compact_through`
//! then drops its retained entries at or below the boundary, which are the
//! evidence the reclaim path reads before requeueing an abandoned claim.
//!
//! The compactor covers the interval where the recreation has already completed
//! before maintenance starts (`compactor/tests/compactor/catalog_claim.rs`).
//! This file covers the other one: the recreation lands BETWEEN the watermark
//! read and the commit, which no caller-side check can see. The identity is
//! compared on the handle the transaction is based on, so a name re-resolved
//! afterwards could not stand in for it.
//!
//! COMPATIBILITY. `None` is "no verified identity" and passes, the same wire
//! contract `AppendIncarnationMismatch` has: the events table has no
//! recreate-under-one-name path, and a pre-#2889 watermark row records no
//! incarnation. Callers refuse the latter for index tables rather than pass it
//! here, because a name is precisely what proves nothing.

use siglake_core::index_config::IndexConfig;
use siglake_core::{events_to_record_batch, Event};
use siglake_storage::consumed_proof::{ConsumedProofEntry, ConsumedProofRead};
use siglake_storage::iceberg::IcebergContext;

fn logs_index(index_id: &str) -> IndexConfig {
    let mut config = IndexConfig::builtin_events();
    config.index_id = index_id.to_string();
    config
}

/// The durable acknowledgement boundary, or `None` when the table records no
/// consumed-proof property at all.
async fn acknowledged_through(ice: &IcebergContext, table: &str) -> Option<i64> {
    match ice.reclaim_proof_sources(table).await.unwrap().durable {
        ConsumedProofRead::Valid(proof) => proof.acknowledged_through_ms,
        ConsumedProofRead::Absent => None,
        ConsumedProofRead::Corrupt(err) => panic!("proof must not be corrupt: {err}"),
    }
}

/// Seed one index table with a consumed-proof entry, so the maintenance
/// compaction has something to acknowledge and something to retire.
async fn seed_proof_entry(ice: &IcebergContext, index_id: &str, segment: &str, claimed_at_ms: i64) {
    let ident = ice.index_table_ident(index_id);
    let batch = events_to_record_batch(&[Event::now(format!("{segment}-row"))]).unwrap();
    ice.append_to_table_with_consumed_proof(
        &ident,
        batch,
        &[],
        &[ConsumedProofEntry {
            segment_id: format!("{segment}.arrow"),
            claimed_at_ms,
        }],
        None,
        None,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn maintenance_refuses_a_recreation_between_the_watermark_read_and_the_commit() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let config = logs_index("logs");
    ice.create_index(&config).await.unwrap();
    let dropped_uuid = ice.index_table_uuid("logs").await.unwrap().unwrap();
    seed_proof_entry(&ice, "logs", "seg-old", 1_000).await;

    // The window: maintenance has read the boundary for the incarnation it just
    // saw, and the id is dropped and re-created before it commits.
    assert!(ice.delete_index("logs").await.unwrap());
    ice.create_index(&config).await.unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let live_uuid = ice.index_table_uuid("logs").await.unwrap().unwrap();
    assert_ne!(live_uuid, dropped_uuid, "recreation is a new table");
    seed_proof_entry(&ice, "logs", "seg-live", 2_000).await;

    let ident = ice.index_table_ident("logs");
    let err = ice
        .compact_consumed_proof(&ident, 1_500, Some(&dropped_uuid))
        .await
        .expect_err("a boundary from the dropped incarnation must be refused");
    let message = err.to_string();
    assert!(
        message.contains(&dropped_uuid) && message.contains(&live_uuid),
        "the refusal names both incarnations: {message}"
    );

    assert_eq!(
        acknowledged_through(&ice, "logs").await,
        None,
        "the replacement's proof property is untouched"
    );
    let ConsumedProofRead::Valid(proof) = ice.reclaim_proof_sources("logs").await.unwrap().durable
    else {
        panic!("the replacement's own append recorded a proof")
    };
    assert!(
        proof.contains("seg-live.arrow"),
        "and its own entry survives: 1500 is above this entry's claim time, so \
         applying the boundary would have retired the evidence reclaim reads"
    );

    // The live incarnation's own boundary applies, so the fence refuses a
    // mislabelled boundary rather than disabling maintenance.
    assert!(ice
        .compact_consumed_proof(&ident, 2_500, Some(&live_uuid))
        .await
        .unwrap());
    assert_eq!(acknowledged_through(&ice, "logs").await, Some(2_500));
    let ConsumedProofRead::Valid(proof) = ice.reclaim_proof_sources("logs").await.unwrap().durable
    else {
        panic!("proof missing after compaction")
    };
    assert!(
        !proof.contains("seg-live.arrow"),
        "an acknowledged entry is retired, which is what the maintenance pass is for"
    );
}

/// `None` keeps the pre-#2889 behaviour for the callers that have no identity
/// to offer, and the events table is one of them by construction.
#[tokio::test]
async fn no_expected_incarnation_applies_the_boundary() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    ice.append_events(&[Event::now("events-row")])
        .await
        .unwrap();
    let ident = ice.events_table_ident().clone();
    assert!(ice
        .compact_consumed_proof(&ident, 4_000, None)
        .await
        .unwrap());
    assert_eq!(acknowledged_through(&ice, "events").await, Some(4_000));
    assert!(
        !ice.compact_consumed_proof(&ident, 4_000, None)
            .await
            .unwrap(),
        "a boundary the table already records is not re-committed"
    );
}
