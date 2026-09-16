//! Write-behind side-aggregate maintenance, the 2D half
//! (`SIGLAKE_SIDE_AGG_WRITE_BEHIND=1`).
//!
//! Its sibling `side_agg_write_behind.rs` gates `group_counts` — the
//! whole-table column totals. It says NOTHING about `time_group_counts`, the
//! 2D time x group rollup that a windowed `GROUP BY` reads, and that is the
//! component the field reports wrong: measured 2026-08-27 at 1TB, a FRESH read
//! (no pinned copy involved — the pin fix had already landed and the cache read
//! `miss 892, zero hits`) gave `covered=1` against
//! `record_count=2,016,590,704`. A rollup frozen at one commit's worth while 2B
//! rows were committed around it.
//!
//! Two paths can maintain that object, and the split runs exactly along the
//! reproduction: every local test exercises the INLINE commit path, every bench
//! drain runs the WRITE-BEHIND flusher. That is why the local reproductions
//! kept passing while the field kept failing — this cell was never tested.
//!
//! SCOPE, stated because a green result here is easy to over-read: opendal's
//! Fs backend advertises `write_with_if_not_exists` but NOT
//! `write_with_if_match`, so a local-FS warehouse reports `conditional() ==
//! false` and the flusher takes its plain read-merge-write leg. What this
//! covers is write-behind folding and flushing the 2D delta with ONE writer and
//! no conditional write. The conditional-CAS retry under CONCURRENT writers —
//! what the S3 drain fleet actually runs — is a different cell, covered by
//! `side_agg_cas_loses_no_concurrent_2d_increments` in `iceberg.rs`.

use chrono::{DateTime, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, TimeBounds, SNAPSHOT_TIME_BUCKET_BASE_NS};

/// One append's worth of rows, all inside the hour bucket starting at `start`.
///
/// Spread across the hour rather than stacked on its first nanosecond: rows all
/// sharing one instant would land in the right bucket even if the bucketing
/// arithmetic were wrong, which is a way for this test to pass without testing.
fn synth(n: usize, tag: &str, start: DateTime<Utc>) -> Vec<Event> {
    (0..n)
        .map(|i| {
            let mut e = Event::now(format!("{tag} row {i}"));
            e.timestamp = start + chrono::Duration::minutes(i as i64);
            e.sourcetype = if i % 2 == 0 { "app:json" } else { "syslog" }.into();
            e
        })
        .collect()
}

/// A windowed `GROUP BY` must be served BY THE ROLLUP, and be exact.
///
/// Both halves are load-bearing. The fallback path returns the correct answer
/// too — it sums a footer per live file — so asserting only on the counts would
/// pass against a rollup that had been lost entirely. `source_label()` is what
/// separates them: `tier1_windowed_agg` means the 2D object answered,
/// `materialized` means the silent fallback did.
#[tokio::test]
async fn write_behind_converges_the_2d_rollup() {
    const HOUR: i64 = SNAPSHOT_TIME_BUCKET_BASE_NS;
    /// One append per hour bucket. Several, because the field symptom is a
    /// rollup that holds ONE commit's worth after many: a single-append test
    /// cannot tell a working flusher from one that keeps only the last delta.
    const APPENDS: usize = 6;
    const ROWS: usize = 20;

    let base: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
    let base_ns = base.timestamp_nanos_opt().unwrap();
    // Bucket starts are aligned to the width, so an aligned window has no
    // sub-width boundary range and the rollup answers the whole of it. An
    // unaligned window would send the edges to the per-file path and could fall
    // back for reasons that have nothing to do with what is being tested.
    assert_eq!(base_ns % HOUR, 0, "the fixture's base must be hour-aligned");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            side_agg_write_behind: Some(true),
            ..Default::default()
        });
    for a in 0..APPENDS {
        let start = DateTime::from_timestamp_nanos(base_ns + (a as i64) * HOUR);
        ice.append_events(&synth(ROWS, &format!("b{a}"), start))
            .await
            .unwrap();
    }

    let window = |from_hour: i64, to_hour: i64| TimeBounds {
        start: Some(DateTime::from_timestamp_nanos(base_ns + from_hour * HOUR)),
        end: Some(DateTime::from_timestamp_nanos(base_ns + to_hour * HOUR)),
    };
    let half = (ROWS / 2) as u64;

    // The flusher is async: poll until the rollup serves, or fail. Convergence
    // on local FS lands well inside a second; the generous bound only avoids CI
    // flakes. NOT a sleep-then-assert — a fixed sleep here would report a slow
    // flusher and a lost delta identically.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut last = String::from("<no answer at all>");
    loop {
        // Fresh context per poll: the writer's own caches could serve a
        // pre-convergence answer, and what matters is what a separate query
        // process reads off the object.
        let reader = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        let served = reader
            .grouped_counts_with_summary("events", "sourcetype", None, Some(window(0, 6)))
            .await
            .unwrap();
        if let Some(counts) = &served {
            last = counts.source_label().to_string();
            if last == "tier1_windowed_agg" {
                // Serving is not passing. The read guard only proves the
                // rollup's total matches `total-records`; it says nothing about
                // how those rows are spread across buckets and values, which is
                // the whole of what a windowed GROUP BY reads.
                let mut rows = counts.to_rows();
                rows.sort();
                assert_eq!(
                    rows,
                    vec![
                        (Some("app:json".to_string()), half * APPENDS as u64),
                        (Some("syslog".to_string()), half * APPENDS as u64),
                    ],
                    "the converged rollup must account for every appended row"
                );

                // A sub-window over two of the six buckets. The full window
                // would still match if every delta had been folded into one
                // bucket; this is what pins the TIME half of time x group.
                let sub = reader
                    .grouped_counts_with_summary("events", "sourcetype", None, Some(window(1, 3)))
                    .await
                    .unwrap()
                    .expect("a covered sub-window must answer");
                assert_eq!(
                    sub.source_label(),
                    "tier1_windowed_agg",
                    "an hour-aligned sub-window has no boundary range to fall back for"
                );
                let mut sub_rows = sub.to_rows();
                sub_rows.sort();
                assert_eq!(
                    sub_rows,
                    vec![
                        (Some("app:json".to_string()), half * 2),
                        (Some("syslog".to_string()), half * 2),
                    ],
                    "buckets 1..3 hold exactly two appends' rows"
                );
                return;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the 2D rollup never converged within 10s — last served by `{last}`. \
             `materialized` is the silent fallback: the windowed GROUP BY was \
             answered by summing a footer per live file because the rollup did \
             not account for every row, which is the 1TB `covered=1` symptom."
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
