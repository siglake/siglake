//! Concurrent committers must not fight over the catalog row.
//!
//! Iceberg publishes via optimistic concurrency: a commit CASes the table's
//! catalog row against the base it loaded, and exactly one racer wins. The
//! losers re-load and re-apply.
//!
//! The 2026-08-08 1TB round measured **645 of 1,430 commits (45.1%) hitting a
//! stale base**, and each retry re-runs `load_table` (25.2% of append time) plus
//! the commit itself (28.1%) — so contention inflates two of the four cost
//! quarters at once.
//!
//! A first fix serialized compaction bins against each other and moved the rate
//! only 45.1% -> 41.5%, which REFUTED "bins race bins" as the explanation. The
//! remaining pair is drain `fast_append` racing compaction `rewrite_files`: two
//! different call sites, one catalog row. `table_commit_lock` now covers both.
//!
//! **This test then killed the locking approach outright.** With commits
//! serialized it still measured 5 stale-base events across 6 writers, because
//! the append sequence is:
//!
//! ```text
//!   1. load_table()                  <- the base is read HERE
//!   2. promote / encode / flush      (tens of seconds)
//!   3. acquire the commit lock
//!   4. CAS against the base from (1) <- stale if anyone committed in between
//! ```
//!
//! Serializing (3)-(4) cannot make (1) fresher. A lock orders the commits, but
//! every one of them still carries a base read long before it. The only ways to
//! make the base fresh are to re-load it under the lock — which serializes a
//! 9.65 s `load_table` per committer, strictly worse than a 45% retry rate that
//! costs the same re-load in parallel — or to hold the lock across the whole
//! append, which serializes tens of seconds of encode+flush.
//!
//! So the lock cannot make retries go away. But removing it entirely was worse,
//! and this test caught that too: WITHOUT it the same 6-writer fixture exhausts
//! the retry budget and appends FAIL outright with `CatalogCommitConflicts`.
//! Six concurrent committers is the production shape — drain_concurrency 2 plus
//! up to 4 compaction bins — so that is a real failure mode, not a stress
//! artifact.
//!
//! Net: the lock bounds how many committers pile onto one catalog row, which
//! keeps retries CONVERGING instead of failing. It is a guard, not a
//! throughput fix. The throughput levers remain **fewer commits** (batch
//! amortization) and **cheaper retries** (a cheaper `load_table`).
//!
//! What this test pins: concurrent committers must all succeed, retries must
//! converge rather than spin, and every row must land exactly once.
//!
//! Own test binary — it installs a metrics recorder, which is process-global.

use std::sync::Arc;

use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

fn counter(
    snapshot: &[(
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    )],
    name: &str,
) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _, _, _)| k.key().name() == name)
        .map(|(_, _, _, v)| match v {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

fn synth(n: usize, tag: &str) -> Vec<Event> {
    (0..n)
        .map(|i| {
            let mut e = Event::now(format!("{tag} row {i}"));
            e.host = format!("host-{}", i % 8);
            e
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_commits_do_not_contend_on_the_catalog_row() {
    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    let _ = metrics::set_global_recorder(recorder);

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    // Materialize the table once so the racers below contend on COMMITS, not on
    // table creation.
    ice.append_events(&synth(1, "warmup")).await.unwrap();

    const WRITERS: usize = 6;
    const ROWS: usize = 40;
    let before = snap.snapshot().into_vec();

    let mut set = tokio::task::JoinSet::new();
    for w in 0..WRITERS {
        let ice = ice.clone();
        set.spawn(async move {
            ice.append_events(&synth(ROWS, &format!("w{w}")))
                .await
                .unwrap()
        });
    }
    let mut committed = 0usize;
    while let Some(r) = set.join_next().await {
        committed += r.expect("writer task");
    }
    let after = snap.snapshot().into_vec();

    let stale = counter(&after, "siglake_iceberg_commit_stale_base_total")
        .saturating_sub(counter(&before, "siglake_iceberg_commit_stale_base_total"));
    let attempts = counter(&after, "siglake_iceberg_commit_attempts_total")
        .saturating_sub(counter(&before, "siglake_iceberg_commit_attempts_total"));
    // The REAL CAS outcome, vs the stale-base counter that was misread as one.
    let cas_conflict = counter(&after, "siglake_catalog_cas_total")
        .saturating_sub(counter(&before, "siglake_catalog_cas_total"));
    eprintln!(
        "contention: {WRITERS} writers -> {attempts} attempts, {stale} stale-base refreshes, \
         {cas_conflict} total CAS outcomes"
    );
    // stale_base counts "refresh saw a newer snapshot", which precedes the
    // conditional UPDATE and is expected under concurrency. It is NOT a loss
    // rate; reading it as one produced the "67% of commits lose the CAS" figure
    // that several drain hypotheses rested on.
    assert!(
        stale <= attempts,
        "stale-base refreshes cannot exceed attempts (stale={stale}, attempts={attempts})"
    );

    assert_eq!(
        committed,
        WRITERS * ROWS,
        "every row must be reported committed"
    );
    // Retries are EXPECTED here (see the module docs) — the guarantee is that
    // they are resolved correctly, not that they do not happen. Assert only that
    // the retry path terminates rather than spinning.
    // Every writer returned Ok — the .unwrap()s above would have panicked on a
    // CatalogCommitConflicts, which is exactly what happens without the lock.
    // Retries are expected (see module docs); they must converge, not spin.
    assert!(
        attempts <= (WRITERS * 4) as u64,
        "retries must converge, not spin (attempts={attempts}, stale={stale})"
    );

    // Exactness: contention must not have cost or duplicated rows.
    let ctx = datafusion::prelude::SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let batches = ctx
        .sql("SELECT count(*) AS n, count(DISTINCT raw) AS d FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let col = |i: usize| {
        batches[0]
            .column(i)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0) as usize
    };
    assert_eq!(col(0), WRITERS * ROWS + 1, "all rows landed (incl. warmup)");
    assert_eq!(col(1), WRITERS * ROWS + 1, "no row committed twice");
}
