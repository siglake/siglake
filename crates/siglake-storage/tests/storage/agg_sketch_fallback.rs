//! Stages 2-4 of `docs/DESIGN_approximate_group_counts.md`: a column too wide
//! for an exact aggregate is covered by a sketch instead of falling off a cliff.
//!
//! The cap used to be all-or-nothing. One distinct value over and the column
//! contributed nothing, so `GROUP BY` on it scanned every row. These tests pin
//! the three things that make the fallback worth having AND safe:
//!
//!   1. a column over the cap now has an answer at all;
//!   2. that answer never shadows an exact one — the sketch is consulted only
//!      where the exact path has already given up;
//!   3. it is labelled, with the error bound and the uncounted residual, so an
//!      approximate leaderboard can never be mistaken for an exact one.
//!

use chrono::{TimeZone, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

/// Deliberately straddling the cap: `host` is far over it, `sourcetype` sits at
/// 2 distinct values and must stay exact.
///
/// The cap must exceed the inline ceiling (4,096) or the incremental path — and
/// with it the sketch — is switched off entirely, which is how the first
/// version of this file silently tested nothing.
const CAP: usize = 8_192;
const HEAVY: usize = 100;
const TAIL: usize = 39_900;
const DISTINCT_HOSTS: usize = HEAVY + TAIL;

async fn open_tuned(path: &std::path::Path) -> IcebergContext {
    IcebergContext::open(path)
        .await
        .unwrap()
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            table_group_count_cardinality: Some(CAP),
            group_count_sketch_counters: Some(2048),
            ..Default::default()
        })
}

fn ev(secs: i64, host: &str) -> Event {
    Event {
        timestamp: Utc.timestamp_opt(secs, 0).single().unwrap(),
        host: host.to_string(),
        source: "src".into(),
        sourcetype: if secs % 2 == 0 {
            "app:json".into()
        } else {
            "syslog".into()
        },
        index: "main".into(),
        raw: "req".into(),
        attributes: None,
    }
}

/// An explicit plan rather than a hash: 100 heavy hitters with STRICTLY
/// DECREASING counts (200 down to 101) over a tail of 39,900 hosts at 2 rows
/// each.
///
/// Both properties are load-bearing. Distinct-value count has to be knowable so
/// the column is definitely over the cap — the first version used
/// `(i*i) % 40_000`, whose quadratic residues collapse to a few thousand
/// distinct values, so `host` stayed UNDER the cap and the test exercised the
/// exact path while claiming to exercise the sketch. And the counts have to be
/// tie-free, or the expected top-100 ordering is ambiguous and the assertion
/// tests the tiebreak rather than the sketch.
fn hosts() -> Vec<String> {
    let mut v = Vec::new();
    for r in 0..HEAVY {
        for _ in 0..(200 - r) {
            v.push(format!("h{r:06}"));
        }
    }
    for r in 0..TAIL {
        let name = format!("h{:06}", HEAVY + r);
        v.push(name.clone());
        v.push(name);
    }
    v
}

#[tokio::test]
async fn a_column_over_the_cap_is_served_by_a_labelled_sketch() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = open_tuned(&tmp.path().join("warehouse")).await;
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    let plan = hosts();
    let rows = plan.len() as u64;
    let mut truth: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    let evs: Vec<Event> = plan
        .iter()
        .enumerate()
        .map(|(i, h)| {
            *truth.entry(h.clone()).or_default() += 1;
            ev(base + i as i64, h)
        })
        .collect();
    assert_eq!(truth.len(), DISTINCT_HOSTS, "the plan must be over the cap");
    for chunk in evs.chunks(evs.len() / 3) {
        ice.append_events(chunk).await.unwrap();
    }

    // The exact top-100, to judge the sketch against.
    let mut exact_top: Vec<(String, u64)> = truth.iter().map(|(h, c)| (h.clone(), *c)).collect();
    exact_top.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    exact_top.truncate(100);

    // 1. `host` is over the cap, so no exact aggregate covers it — this is the
    //    precondition, and without it the rest of the test proves nothing.
    let summary = ice
        .table_group_counts_summary("events")
        .await
        .unwrap()
        .expect("an aggregate exists");
    // Not `None`: a column can be counted exactly by a SMALL commit and
    // sketched by a large one, so a partial exact tally can exist. What matters
    // is that it is incomplete, which is what makes the exact path decline and
    // the fallback the only thing that can answer.
    assert_ne!(
        summary.column_total("host"),
        Some(rows),
        "`host` must NOT be fully covered exactly, or this exercises the wrong path"
    );
    assert_eq!(
        summary.column_total("sourcetype"),
        Some(rows),
        "the low-cardinality control column must still be exact"
    );

    // 2. …and it now has an approximate answer instead of nothing.
    let approx = ice
        .approximate_top_group_counts("events", "host", 100, None)
        .await
        .unwrap()
        .expect("a column over the cap must be served by the sketch");

    assert!(!approx.rows.is_empty());
    assert!(
        approx.counters <= 2048,
        "the summary must stay within its counter budget, got {}",
        approx.counters
    );

    // 3. Every reported count understates the truth, by at most the bound.
    for (value, est) in &approx.rows {
        let t = truth.get(value).copied().unwrap_or(0);
        assert!(*est <= t, "{value}: sketch reported {est} > true {t}");
        assert!(
            t <= est + approx.error_upper_bound,
            "{value}: true {t} outside [{est}, {}]",
            est + approx.error_upper_bound
        );
    }

    // 4. The leaderboard itself is right — the property that actually matters
    //    for a top-K, and stronger than the worst-case bound implies.
    let got: Vec<&String> = approx.rows.iter().map(|(v, _)| v).collect();
    let want: Vec<&String> = exact_top.iter().map(|(v, _)| v).collect();
    assert_eq!(
        got, want,
        "the approximate top-100 must match the exact one"
    );

    // 5. COMPLETENESS. The summary must account for every row of the column —
    //    no more, no less. This is the assertion that catches a contribution
    //    lost or double-counted at the exact/sketch boundary: a column can be
    //    counted exactly by a small commit and sketched by a large one, and
    //    neither the top-K nor the error bound reveals a mismatch there.
    //    Without it the earlier version of this test stayed green while the
    //    reconciliation was disabled outright.
    assert_eq!(
        approx.rows_accounted, rows,
        "the sketch must account for every row of the column, exactly once"
    );

    // 6. The residual is reported — this is the number that exposed Quickwit
    //    discarding 96.4% of the corpus.
    let counted: u64 = approx.rows.iter().map(|(_, c)| *c).sum();
    assert!(
        approx.not_counted + counted <= rows,
        "the residual must not overstate what was skipped"
    );
}

/// A lost delta can carry approximate and exact columns together. Rebuilding
/// only the exact half would leave `rows_accounted` short forever, so the same
/// marker-driven pass must reconstruct the sketch from committed files too.
#[tokio::test]
async fn a_lost_delta_marker_rebuilds_its_sketch() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = open_tuned(&warehouse).await;
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();
    let plan = hosts();
    let rows = plan.len() as u64;
    let events: Vec<Event> = plan
        .iter()
        .enumerate()
        .map(|(i, host)| ev(base + i as i64, host))
        .collect();
    ice.append_events(&events).await.unwrap();

    let deltas: Vec<_> = walk_files(&warehouse)
        .into_iter()
        .filter(|path| path.to_string_lossy().contains("siglake-agg-deltas"))
        .collect();
    assert_eq!(deltas.len(), 1, "precondition: one commit, one delta");
    let lost_sequence = deltas[0]
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();
    std::fs::remove_file(&deltas[0]).unwrap();
    ice.write_group_count_rebuild_marker_for_test(
        ice.events_table_ident(),
        lost_sequence,
        &[("sourcetype", CAP)],
        &["host"],
    )
    .await
    .unwrap();

    assert!(
        ice.approximate_top_group_counts("events", "host", 10, None)
            .await
            .unwrap()
            .is_none(),
        "precondition: losing the only delta loses the sketch"
    );
    let outcomes = ice.fold_group_count_deltas(1).await.unwrap();
    assert!(outcomes.iter().any(|(_, outcome)| outcome.rebuilt));

    let rebuilt = ice
        .approximate_top_group_counts("events", "host", 10, None)
        .await
        .unwrap()
        .expect("the automatic rebuild restores the sketch");
    assert_eq!(
        rebuilt.rows_accounted, rows,
        "the rebuilt sketch covers every committed non-null host"
    );
}

#[tokio::test]
async fn the_sketch_never_shadows_an_exact_answer() {
    // A column the exact path can serve must never be answered approximately.
    //
    // Honest scope: this verifies the OUTCOME end to end, not the guard that
    // enforces it. Removing the guard leaves this green, because a
    // low-cardinality column is never sketched in the first place — there is
    // nothing for it to shadow. Under the current design a column is either
    // fully exact or has a sketch, never both complete, so the guard in
    // `approximate_top_group_counts` is defence in depth against a state that
    // should not arise rather than a live branch. Kept because it is nearly
    // free and the failure it prevents — a fast wrong answer displacing a
    // correct one — is the worst outcome in this feature.
    let tmp = tempfile::tempdir().unwrap();
    let ice = open_tuned(&tmp.path().join("warehouse")).await;
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();
    let plan = hosts();
    let rows = plan.len() as u64;
    let evs: Vec<Event> = plan
        .iter()
        .enumerate()
        .map(|(i, h)| ev(base + i as i64, h))
        .collect();
    for chunk in evs.chunks(evs.len() / 2) {
        ice.append_events(chunk).await.unwrap();
    }

    assert!(
        ice.approximate_top_group_counts("events", "sourcetype", 10, None)
            .await
            .unwrap()
            .is_none(),
        "a column the exact aggregate covers must not be served approximately"
    );

    // And the exact path still answers it, exactly.
    let exact = ice
        .grouped_counts_with_summary("events", "sourcetype", None, None)
        .await
        .unwrap()
        .expect("exact path serves the low-cardinality column");
    let mut total = 0u64;
    exact.for_each(|_, count| total = total.saturating_add(count));
    assert_eq!(total, rows);
}

/// The failure the 2026-08-01 round found: a column that is UNDER the cap in
/// every individual commit but over it once the commits are unioned.
///
/// This is the realistic shape and the one the per-batch degrade-in-place does
/// not catch. On http_logs a commit is ~5M rows and sees ~120K distinct hosts
/// against a 262,144 cap, so every commit counted `host` exactly; the overflow
/// only happened in the fold, where `merge`'s retain dropped the column
/// outright. No exact aggregate, no sketch, and a 6-second scan — with
/// `rows_scanned: 0` in the response, because the raw-page fallback bypasses
/// that counter.
#[tokio::test]
async fn a_column_that_only_overflows_once_commits_are_unioned_is_still_sketched() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = open_tuned(&tmp.path().join("warehouse")).await;
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    // Six commits, each with a DISJOINT block of 4,000 hosts — comfortably
    // under the 8,192 cap individually, 24,000 distinct once unioned.
    const PER_COMMIT: usize = 4_000;
    const COMMITS: usize = 6;
    // The whole point is per-commit UNDER the cap, unioned OVER it. Checked at
    // compile time so changing CAP cannot quietly turn this back into a test of
    // the per-batch path, which already worked.
    const _: () = assert!(PER_COMMIT < CAP && PER_COMMIT * COMMITS > CAP);
    let mut total = 0u64;
    for c in 0..COMMITS {
        let evs: Vec<Event> = (0..PER_COMMIT)
            .flat_map(|i| {
                let h = format!("h{:07}", c * PER_COMMIT + i);
                // Two rows each so counts are non-trivial.
                [h.clone(), h]
            })
            .enumerate()
            .map(|(i, h)| ev(base + (c * PER_COMMIT * 2 + i) as i64, &h))
            .collect();
        total += evs.len() as u64;
        ice.append_events(&evs).await.unwrap();
    }

    // The union is over the cap, so the exact path must decline…
    let summary = ice
        .table_group_counts_summary("events")
        .await
        .unwrap()
        .expect("an aggregate exists");
    assert_ne!(summary.column_total("host"), Some(total));

    // …and the sketch must have caught it, with every row accounted for.
    let approx = ice
        .approximate_top_group_counts("events", "host", 10, None)
        .await
        .unwrap()
        .expect("a column that overflows only in the fold must still be sketched");
    assert_eq!(
        approx.rows_accounted, total,
        "the demoted column must carry every row it was counted for"
    );
    for (_, count) in &approx.rows {
        assert!(
            *count <= 2,
            "each host has exactly 2 rows; MG never overcounts"
        );
    }
}

/// The sketch must preempt a Tier-1 MISS, not merely a refusal — which is the
/// distinction that decided whether any of this was reachable.
///
/// On the 2026-08-01 round `host` missed Tier-1 and fell to the raw-page tier,
/// which SUCCEEDED, exactly, in ~3.1s. A fallback that only replaces a refusal
/// was therefore never consulted and `top_hosts` was no faster than before the
/// feature existed. This pins the storage-side precondition for the fix: the
/// column is genuinely answerable by the slower exact path AND has a sketch, so
/// the query layer has a real choice to make.
#[tokio::test]
async fn an_over_cap_column_is_answerable_both_ways() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = open_tuned(&tmp.path().join("warehouse")).await;
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();
    let plan = hosts();
    let rows = plan.len() as u64;
    let evs: Vec<Event> = plan
        .iter()
        .enumerate()
        .map(|(i, h)| ev(base + i as i64, h))
        .collect();
    for chunk in evs.chunks(evs.len() / 3) {
        ice.append_events(chunk).await.unwrap();
    }

    // Tier-1 declines — this is the miss the sketch now preempts.
    assert!(
        ice.tier1_group_counts("events", "host")
            .await
            .unwrap()
            .is_none(),
        "`host` must miss Tier-1, or the query layer would never consult the sketch"
    );

    // The slower exact path still answers it, which is exactly why the old
    // "replace refusal only" policy never fired.
    let slow_exact = ice
        .grouped_counts_with_summary("events", "host", None, None)
        .await
        .unwrap()
        .expect("the raw-page path answers, slowly — that is the whole problem");
    let mut total = 0u64;
    slow_exact.for_each(|_, count| total = total.saturating_add(count));
    assert_eq!(total, rows);

    // And the sketch can answer it too, so the choice is real.
    assert!(
        ice.approximate_top_group_counts("events", "host", 10, None)
            .await
            .unwrap()
            .is_some(),
        "a sketch must exist for the column the exact path answers slowly"
    );

    // A low-cardinality column stays on Tier-1 and is unaffected by any of it.
    assert!(
        ice.tier1_group_counts("events", "sourcetype")
            .await
            .unwrap()
            .is_some(),
        "a cheap exact answer must still win"
    );
}

/// A column crossing the cap must be OBSERVABLE, not just handled.
///
/// This is the surprise the whole feature exists to defuse: a `GROUP BY` that
/// silently changes from exact to approximate. The query response carries an
/// `approximation` field, but that only helps whoever runs the query — an
/// operator watching a cluster needs to know the transition happened. Pinned
/// here by the state it leaves behind, since a `tracing` line is not
/// assertable from a test.
#[tokio::test]
async fn crossing_the_cap_is_visible_in_the_aggregate_state() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("warehouse");
    let ice = open_tuned(&root).await;
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    // Start UNDER the cap: `host` is exact, nothing is sketched.
    let small: Vec<Event> = (0..2_000)
        .map(|i| ev(base + i as i64, &format!("h{:07}", i % 1_000)))
        .collect();
    ice.append_events(&small).await.unwrap();
    let before = ice
        .approximate_top_group_counts("events", "host", 10, None)
        .await
        .unwrap();
    assert!(
        before.is_none(),
        "a column under the cap must not be sketched — otherwise the transition \
         below is not a transition"
    );
    assert!(ice
        .tier1_group_counts("events", "host")
        .await
        .unwrap()
        .is_some());

    // Now push it over.
    let big: Vec<Event> = (0..20_000)
        .map(|i| ev(base + 10_000 + i as i64, &format!("h{:07}", 1_000 + i)))
        .collect();
    ice.append_events(&big).await.unwrap();

    // The transition is durable and inspectable: the column has left the exact
    // aggregate and appears in the summary.
    ice.invalidate_cached_table(ice.events_table_ident()).await;
    assert!(
        ice.tier1_group_counts("events", "host")
            .await
            .unwrap()
            .is_none(),
        "the column must have left the exact aggregate"
    );
    let after = ice
        .approximate_top_group_counts("events", "host", 10, None)
        .await
        .unwrap()
        .expect("and arrived in the summary");
    assert!(after.counters > 0 && after.rows_accounted > 0);
}

/// `served_by` must distinguish a warm-metadata answer from a materialized one.
///
/// `rows_scanned` cannot: it counts DataFusion scan output, and the footer-sum
/// and raw-page paths bypass it, so a sub-millisecond Tier-1 answer and a
/// multi-second decode both report 0. That ambiguity cost two investigations —
/// it hid the original `top_hosts` regression, then hid a 6.5s `GROUP BY host`
/// on the 2026-08-03 1TB round, both times sending the diagnosis off in the
/// wrong direction.
#[tokio::test]
async fn served_by_separates_tier1_from_a_materialized_answer() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = open_tuned(&tmp.path().join("warehouse")).await;
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();
    let plan = hosts();
    let evs: Vec<Event> = plan
        .iter()
        .enumerate()
        .map(|(i, h)| ev(base + i as i64, h))
        .collect();
    for chunk in evs.chunks(evs.len() / 3) {
        ice.append_events(chunk).await.unwrap();
    }

    // Low-cardinality column: the inline object covers it.
    let low = ice
        .grouped_counts_with_summary("events", "sourcetype", None, None)
        .await
        .unwrap()
        .expect("covered");
    assert_eq!(
        low.source_label(),
        "tier1_inline",
        "a column the per-commit object covers is a warm-metadata answer"
    );

    // Over-cap column: no exact aggregate covers it, so whatever answers is
    // materialized — footers summed or raw pages decoded.
    let wide = ice
        .grouped_counts_with_summary("events", "host", None, None)
        .await
        .unwrap()
        .expect("still answered, just not cheaply");
    assert_eq!(
        wide.source_label(),
        "materialized",
        "the expensive path must not be indistinguishable from Tier-1"
    );
    assert_ne!(
        low.source_label(),
        wide.source_label(),
        "if these ever agree the field has stopped doing its job"
    );
}

fn walk_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk_files(&path));
        } else {
            out.push(path);
        }
    }
    out
}
