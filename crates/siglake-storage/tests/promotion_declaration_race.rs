//! Task #1006: N query replicas declaring the same promotions on a tenant's
//! FIRST request.
//!
//! `IcebergContext::for_namespace` runs `ensure_namespace` (the #228 race, fixed
//! by tolerating `NamespaceAlreadyExists`), then `ensure_events_table`, then
//! `ensure_promoted_columns` -> `declare_promotions_for`. The last one issues
//! two more catalog transactions — the promoted-columns PROPERTY first, then the
//! additive schema WIDEN — and unlike the namespace create those are ordinary
//! optimistic-concurrency commits on the tenant's catalog row. Every replica
//! runs them for the same fresh tenant at the same instant.
//!
//! The unit test `concurrent_for_namespace_with_promotions_all_succeed` pins the
//! outcome at the 8 replicas the card asked for. This binary pins the part the
//! retry budget alone does NOT cover. It also bounds successful catalog CASes:
//! retry-aware idempotent actions must stop after the property and schema each
//! win once, rather than writing one redundant metadata version per replica.
//!
//! `Transaction::commit` retries a lost CAS — the SQL catalog returns
//! `CatalogCommitConflicts` with `retryable(true)` from its conditional
//! `UPDATE ... WHERE metadata_location = ?`, and the backoff loop re-loads the
//! base and re-applies the actions — but the budget is 4 retries
//! (`PROPERTY_COMMIT_NUM_RETRIES_DEFAULT`). Measured on this fixture before the
//! declaration was made race-tolerant: 8 replicas converged, 12 failed 2 runs in
//! 5, and 32 failed every run with
//! `commit promoted property: CatalogCommitConflicts` — i.e. a tenant's FIRST
//! query 500s because another replica declared the identical promotions first.
//!
//! `declare_promotions_for` now re-reads on a failed commit and continues when
//! the declaration is already there, the same doctrine as
//! `create_namespace_tolerating_existing` (#228). So this test runs the replica
//! count that used to fail deterministically, and asserts both halves: the
//! racers really collide (`siglake_catalog_cas_total{outcome="conflict"}` > 0,
//! so the test is not vacuous) and every one of them still comes back with the
//! widened schema, each promoted column present exactly once.
//!
//! Own test binary — it installs a metrics recorder, which is process-global.

use std::sync::Arc;

use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::{PromotedColumn, PromotedType};
use siglake_storage::iceberg::IcebergContext;

type Snapshot = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

/// Sum a counter, optionally restricted to one `key=value` label.
fn counter(snapshot: &Snapshot, name: &str, label: Option<(&str, &str)>) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _, _, _)| k.key().name() == name)
        .filter(|(k, _, _, _)| match label {
            None => true,
            Some((lk, lv)) => k.key().labels().any(|l| l.key() == lk && l.value() == lv),
        })
        .map(|(_, _, _, v)| match v {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

fn promotions() -> Vec<PromotedColumn> {
    vec![
        PromotedColumn {
            attr_key: "http.status_code".to_string(),
            name: "http_status_code".to_string(),
            ty: PromotedType::Int64,
        },
        PromotedColumn {
            attr_key: "service.name".to_string(),
            name: "service_name".to_string(),
            ty: PromotedType::Utf8,
        },
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_promotion_declarations_collide_and_converge() {
    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    let _ = metrics::set_global_recorder(recorder);

    let tmp = tempfile::tempdir().unwrap();
    let promoted = promotions();
    let plain = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    // Materialize the tenant namespace and its (un-widened) events table first,
    // so the replicas below contend on the PROMOTION commits rather than on the
    // namespace/table creation the earlier tests already cover. Without this the
    // creation work skews the replicas apart and one of them routinely finishes
    // the whole declaration before the others reach it.
    plain.for_namespace("tenant_promoted").await.unwrap();
    let ice = Arc::new(plain.with_promoted_columns(promoted.clone()));

    // 4x the fork's per-commit retry budget: the count that failed every run
    // before the declaration tolerated a lost race.
    const REPLICAS: usize = 32;
    let before = snap.snapshot().into_vec();

    let gate = Arc::new(tokio::sync::Barrier::new(REPLICAS));
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..REPLICAS {
        let ice = ice.clone();
        let gate = gate.clone();
        set.spawn(async move {
            gate.wait().await;
            ice.for_namespace("tenant_promoted").await
        });
    }
    let mut contexts = Vec::new();
    while let Some(r) = set.join_next().await {
        contexts
            .push(r.expect("replica task").unwrap_or_else(|e| {
                panic!("for_namespace failed under the promotion race: {e:#}")
            }));
    }
    let after = snap.snapshot().into_vec();

    let delta = |name: &str, label: Option<(&str, &str)>| {
        counter(&after, name, label).saturating_sub(counter(&before, name, label))
    };
    let conflicts = delta("siglake_catalog_cas_total", Some(("outcome", "conflict")));
    let won = delta("siglake_catalog_cas_total", Some(("outcome", "won")));
    let attempts = delta("siglake_iceberg_commit_attempts_total", None);
    eprintln!(
        "promotion race: {REPLICAS} replicas -> {attempts} commit attempts, \
         {won} CAS won, {conflicts} CAS conflicts"
    );

    assert_eq!(contexts.len(), REPLICAS, "every replica returned a context");
    // The point of the binary: the racers actually reach the CAS together.
    // Without a collision this test would prove nothing about tolerance.
    assert!(
        conflicts > 0,
        "no CAS conflict observed — the promotion declarations did not race \
         (attempts={attempts}, won={won}); the tolerance claim is unproven"
    );
    // Retries plus the tolerance converge rather than spin: each replica issues
    // at most the property + widen pair, each with 4 retries on top.
    assert!(
        attempts <= (REPLICAS * 2 * 5) as u64,
        "retries must converge, not spin (attempts={attempts}, conflicts={conflicts})"
    );
    assert!(
        won <= 3,
        "idempotent promotion retries wrote redundant metadata versions \
         (attempts={attempts}, won={won}, conflicts={conflicts})"
    );

    // Exactly one widened schema, visible to every replica, with the property
    // that gates the attr_get rewrite recorded against it.
    let expected = siglake_core::promoted_property_json(&promoted).unwrap();
    for ctx in &contexts {
        let table = ctx
            .catalog()
            .load_table(ctx.events_table_ident())
            .await
            .unwrap();
        let schema = table.metadata().current_schema();
        let names: Vec<&str> = schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        for col in &promoted {
            assert_eq!(
                names.iter().filter(|n| **n == col.name).count(),
                1,
                "promoted column {} landed {} times: {names:?}",
                col.name,
                names.iter().filter(|n| **n == col.name).count()
            );
        }
        assert_eq!(
            table
                .metadata()
                .properties()
                .get(siglake_core::PROMOTED_PROPERTY_KEY)
                .map(String::as_str),
            Some(expected.as_str()),
            "promotion property not recorded on the tenant table"
        );
    }
}
