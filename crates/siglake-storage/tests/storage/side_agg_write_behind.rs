//! Write-behind side-aggregate maintenance (`SIGLAKE_SIDE_AGG_WRITE_BEHIND=1`):
//! the commit path enqueues its aggregate deltas in memory and a spawned
//! per-table flusher folds them into the side object off the commit path. The
//! gates: (a) the side object CONVERGES to exact (every commit's increments
//! land, none lost — the Tier-1 total==total-records guard passes once the
//! flusher catches up), and (b) staleness in the window is safe (queries fall
//! back, never wrong).

use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

fn synth(n: usize, tag: &str) -> Vec<Event> {
    (0..n)
        .map(|i| {
            let mut e = Event::now(format!("{tag} row {i}"));
            e.sourcetype = if i % 2 == 0 { "app:json" } else { "syslog" }.into();
            e
        })
        .collect()
}

#[tokio::test]
async fn write_behind_converges_to_exact_aggregates() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            side_agg_write_behind: Some(true),
            ..Default::default()
        });

    // Several appends so multiple deltas flow through the pending map (some
    // coalescing while a flush is in flight).
    const APPENDS: usize = 5;
    const ROWS: usize = 20;
    for a in 0..APPENDS {
        ice.append_events(&synth(ROWS, &format!("b{a}")))
            .await
            .unwrap();
    }

    // The flusher is async: poll until the Tier-1 aggregate serves (guard
    // passes) or time out. Convergence must land well within a second on
    // local FS; the generous bound just avoids CI flakes.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let expected = vec![
        (Some("app:json".to_string()), (APPENDS * ROWS / 2) as u64),
        (Some("syslog".to_string()), (APPENDS * ROWS / 2) as u64),
    ];
    loop {
        // Fresh context per poll: the writer context's caches could otherwise
        // serve a pre-convergence result; the reader path is what a separate
        // query process would see. `table_group_counts_summary` reads the side
        // OBJECT directly (no per-file fallback), so a total matching every
        // appended row strictly proves the flusher lost no delta.
        let reader = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        let side_total = reader
            .table_group_counts_summary("events")
            .await
            .unwrap()
            .and_then(|gc| gc.column_total("sourcetype"));
        if side_total == Some((APPENDS * ROWS) as u64) {
            // Converged. The guard now passes, so Tier-1 must serve exactly.
            let rows = reader
                .grouped_counts_with_summary("events", "sourcetype", None, None)
                .await
                .unwrap()
                .expect("Tier-1 must serve once the side object converges");
            let mut rows = rows.to_rows();
            rows.sort();
            assert_eq!(rows, expected, "converged aggregate must be exact");
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "write-behind flusher did not converge within 10s (side total: {side_total:?})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
