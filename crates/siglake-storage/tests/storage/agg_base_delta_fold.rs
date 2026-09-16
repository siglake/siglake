//! Stage 0 of the incremental group-count aggregate
//! (`docs/DESIGN_incremental_group_count_aggregate.md`): validate the base +
//! delta fold hermetically, and measure what it costs, BEFORE touching the
//! commit path.
//!
//! Why this comes first. Raising the cardinality cap so high-cardinality
//! `GROUP BY` could be served from a precomputed aggregate collapsed ingest at
//! scale: the aggregate is maintained by a CAS read-modify-write on every
//! commit, so at 1.1M keys each commit rewrote the whole map. The fix is to
//! make maintenance incremental — commits append a small delta, the compactor
//! folds deltas into the base — which keeps the aggregate CURRENT (the Tier-1
//! read guard requires `column_total == record_count`, so a merely
//! background-built aggregate would fail it) while removing the per-commit
//! whole-map rewrite.
//!
//! Two properties decide whether that design is safe, and both are pure
//! functions that need no S3, no Parquet and no cluster:
//!
//!   1. **Exactness** — folding base + unabsorbed deltas must equal aggregating
//!      every commit from scratch.
//!   2. **No double counting** — the absorbed set must exclude exactly the
//!      deltas already folded into the base. This is the one correctness hazard
//!      in the design: a reader assembling base + a delta the compactor just
//!      absorbed would count that delta twice.
//!
//! Absorption is recorded by MEMBERSHIP rather than by a high-water mark, and
//! these tests are written against membership because the watermark version is
//! not merely less general — it is wrong. Deltas cannot be totally ordered by
//! arrival: a writer that stalls between its commit and its delta write lands
//! one below a mark the compactor has already passed, and a watermark drops it
//! for good. (The first cut of this design keyed the mark on Iceberg snapshot
//! ids, which are random, so it would have dropped roughly half of them.)
//!
//! The cost measurement sets the compactor's fold cadence: read amplification
//! grows with the number of outstanding deltas, so the cadence has to keep that
//! count under whatever budget the numbers justify.
//!
//! Run the report:
//!   cargo test --release -p siglake-storage --test storage agg_base_delta_fold:: -- --ignored --nocapture

use std::collections::HashMap;
use std::time::Instant;

/// One commit's contribution: the keys that commit touched, and nothing else.
/// This is the whole point — cost proportional to the commit, not to the
/// table's total cardinality.
#[derive(Clone)]
struct Delta {
    /// The commit's Iceberg SEQUENCE number — monotonic, unlike its snapshot id.
    sequence_number: i64,
    counts: Vec<(String, u64)>,
}

/// The folded aggregate plus the ids already absorbed into it. The two live in
/// ONE object so a reader always sees them together — that atomicity is what
/// makes the double-count guard sound.
#[derive(Clone, Default)]
struct Base {
    absorbed: std::collections::BTreeSet<i64>,
    counts: HashMap<String, u64>,
}

fn host(i: usize) -> String {
    format!(
        "{}.{}.{}.{}",
        i % 256,
        (i / 256) % 256,
        (i / 65536) % 256,
        i % 97
    )
}

/// `commits` distinct commits, each touching `keys_per_commit` hosts drawn from
/// a shared pool so deltas overlap the way real ones do.
fn commits(n: usize, keys_per_commit: usize, pool: usize) -> Vec<Delta> {
    (0..n)
        .map(|c| Delta {
            sequence_number: c as i64 + 1,
            counts: (0..keys_per_commit)
                .map(|k| (host((c * 31 + k * 17) % pool), (k as u64 % 50) + 1))
                .collect(),
        })
        .collect()
}

/// Ground truth: aggregate every commit in one pass, with no base and no
/// watermark. Anything the incremental path produces must equal this.
fn single_pass(deltas: &[Delta]) -> HashMap<String, u64> {
    let mut out: HashMap<String, u64> = HashMap::new();
    for d in deltas {
        for (k, c) in &d.counts {
            *out.entry(k.clone()).or_default() += *c;
        }
    }
    out
}

/// The read path: base + every delta the base has NOT absorbed. Ones it has are
/// already in the base and must be skipped.
fn fold(base: &Base, deltas: &[Delta]) -> HashMap<String, u64> {
    let mut out = base.counts.clone();
    for d in deltas {
        if base.absorbed.contains(&d.sequence_number) {
            continue;
        }
        for (k, c) in &d.counts {
            *out.entry(k.clone()).or_default() += *c;
        }
    }
    out
}

/// The compactor: absorb every delta up to and including `through`, recording
/// each id in the same object it folded the counts into.
fn absorb(base: &mut Base, deltas: &[Delta], through: i64) {
    for d in deltas {
        if base.absorbed.contains(&d.sequence_number) || d.sequence_number > through {
            continue;
        }
        for (k, c) in &d.counts {
            *base.counts.entry(k.clone()).or_default() += *c;
        }
        base.absorbed.insert(d.sequence_number);
    }
}

#[test]
fn fold_of_base_and_deltas_equals_a_single_pass() {
    let deltas = commits(40, 500, 8_000);
    let truth = single_pass(&deltas);

    // Nothing absorbed yet: the base is empty and every delta is outstanding.
    assert_eq!(fold(&Base::default(), &deltas), truth, "cold base");

    // Absorb a prefix, then fold the remainder — the classic steady state.
    for through in [1i64, 7, 20, 39, 40] {
        let mut base = Base::default();
        absorb(&mut base, &deltas, through);
        assert_eq!(
            fold(&base, &deltas),
            truth,
            "absorbed through {through} must still fold to the same totals"
        );
    }
}

#[test]
fn absorbing_is_idempotent_so_a_retried_fold_cannot_double_count() {
    let deltas = commits(25, 300, 5_000);
    let truth = single_pass(&deltas);
    let mut base = Base::default();

    // The compactor crashing between fold and delta-delete, then retrying, is
    // the expected failure mode — absorbing the same range twice must be a
    // no-op, not a doubling.
    absorb(&mut base, &deltas, 10);
    absorb(&mut base, &deltas, 10);
    absorb(&mut base, &deltas, 5); // re-absorbing a lower range must not re-add
    assert_eq!(fold(&base, &deltas), truth);

    absorb(&mut base, &deltas, 25);
    absorb(&mut base, &deltas, 25);
    assert_eq!(fold(&base, &deltas), truth);
    assert_eq!(base.absorbed.len(), 25);
    // Fully absorbed: the fold must now be the base alone.
    assert_eq!(base.counts, truth);
}

#[test]
fn the_absorbed_set_is_what_prevents_double_counting() {
    // Demonstrate the hazard the design exists to avoid: if a reader ignores
    // the absorbed set and folds an already-absorbed delta, it over-counts.
    // This pins WHY the set must be read from the same object as the base.
    let deltas = commits(6, 100, 400);
    let truth = single_pass(&deltas);
    let mut base = Base::default();
    absorb(&mut base, &deltas, 6);

    let ignoring_the_set = {
        let mut out = base.counts.clone();
        for d in &deltas {
            for (k, c) in &d.counts {
                *out.entry(k.clone()).or_default() += *c;
            }
        }
        out
    };
    assert_ne!(
        ignoring_the_set, truth,
        "a reader that ignores the absorbed set MUST over-count — if this \
         passes, the test no longer proves the set is load-bearing"
    );
    // Every key is exactly doubled, which is the signature of the bug.
    for (k, v) in &truth {
        assert_eq!(ignoring_the_set[k], v * 2);
    }
    assert_eq!(fold(&base, &deltas), truth, "honouring it is correct");
}

#[test]
fn a_delta_missing_from_the_fold_is_detectable_by_the_row_count_guard() {
    // The read path only trusts the aggregate when `column_total ==
    // record_count`. A lost or not-yet-written delta must therefore be caught
    // rather than served: the folded total comes out short.
    let deltas = commits(12, 200, 3_000);
    let truth: u64 = single_pass(&deltas).values().sum();
    let mut base = Base::default();
    absorb(&mut base, &deltas, 4);

    let with_a_gap: Vec<Delta> = deltas
        .iter()
        .filter(|d| d.sequence_number != 9)
        .cloned()
        .collect();
    let short: u64 = fold(&base, &with_a_gap).values().sum();
    assert!(
        short < truth,
        "a missing delta must under-count so the record-count guard rejects it"
    );
}

#[test]
#[ignore]
fn report_fold_cost_vs_delta_count() {
    // Read amplification budget: how much does an unfolded delta backlog cost a
    // query? This sets the compactor's fold cadence.
    const POOL: usize = 1_100_000;
    const KEYS_PER_COMMIT: usize = 2_000;

    println!("pool={POOL} keys/commit={KEYS_PER_COMMIT}");
    println!("{:>7} {:>14} {:>14}", "deltas", "fold ms", "base-only ms");
    for n in [1usize, 10, 50, 200, 500] {
        let deltas = commits(n, KEYS_PER_COMMIT, POOL);
        // Steady state: everything absorbed, then `n` new commits arrive.
        let mut base = Base::default();
        absorb(&mut base, &commits(400, KEYS_PER_COMMIT, POOL), 400);

        let t = Instant::now();
        let folded = fold(&base, &deltas);
        let fold_ms = t.elapsed().as_secs_f64() * 1000.0;

        let t = Instant::now();
        let base_only = base.counts.clone();
        let base_ms = t.elapsed().as_secs_f64() * 1000.0;

        assert!(folded.len() >= base_only.len());
        println!("{n:>7} {fold_ms:>14.2} {base_ms:>14.2}");
    }
    println!(
        "\nfold cost is base-clone + O(delta entries); the compactor cadence has to\n\
         keep the outstanding delta count inside whatever budget this justifies."
    );
}

// ---------------------------------------------------------------------------
// Stage 1: the on-disk format for the base watermark and the delta objects.
// ---------------------------------------------------------------------------

/// The delta path must round-trip its sequence number, and sort
/// lexicographically in numeric order so a directory listing reads in commit
/// order.
#[test]
fn delta_paths_round_trip_and_sort_numerically() {
    use siglake_storage::iceberg::{group_count_delta_rel_path, group_count_delta_sequence_number};

    for id in [1i64, 9, 10, 99, 100, 1_000, 8_070_450_532_247_928_832] {
        let p = group_count_delta_rel_path(id);
        assert_eq!(
            group_count_delta_sequence_number(&p),
            Some(id),
            "round trip {id}"
        );
    }

    let mut paths: Vec<String> = [100i64, 2, 30, 4, 1_000]
        .iter()
        .map(|i| group_count_delta_rel_path(*i))
        .collect();
    paths.sort();
    let ids: Vec<i64> = paths
        .iter()
        .map(|p| group_count_delta_sequence_number(p).unwrap())
        .collect();
    assert_eq!(ids, vec![2, 4, 30, 100, 1_000], "lexicographic == numeric");

    // Anything that is not one of ours is ignored, not guessed at.
    for bad in ["metadata/siglake-agg-deltas/nope.json", "other/1.json", "x"] {
        assert_eq!(group_count_delta_sequence_number(bad), None, "{bad}");
    }
}

/// Deltas are keyed by the Iceberg SEQUENCE NUMBER, never the snapshot id.
///
/// Snapshot ids are `abs(uuid.hi ^ uuid.lo)` — random, so they carry no order
/// at all. Ordering the fold by them would let a delta land "below" work
/// already done and be skipped forever. This pins the property the key has to
/// have; it is the reason the field is not `snapshot_id`.
#[test]
fn iceberg_snapshot_ids_are_unordered_so_they_cannot_key_the_fold() {
    use siglake_storage::iceberg::{group_count_delta_rel_path, group_count_delta_sequence_number};

    // Two commits in order, ids as Iceberg would generate them: the second
    // commit's id is SMALLER. Sequence numbers are what preserve the order.
    let (first_id, second_id) = (8_070_450_532_247_928_832i64, 42i64);
    assert!(second_id < first_id, "later commit, smaller id");

    let (first_seq, second_seq) = (1i64, 2i64);
    let paths = [
        group_count_delta_rel_path(first_seq),
        group_count_delta_rel_path(second_seq),
    ];
    let mut ids: Vec<i64> = paths
        .iter()
        .map(|p| group_count_delta_sequence_number(p).unwrap())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![first_seq, second_seq], "commit order preserved");
}

/// A base object written before the aggregate was split must still parse — the
/// wide object simply does not exist yet, which reads as "no wide aggregate"
/// and leaves every column served exactly as it is today.
#[test]
fn a_legacy_side_object_still_parses() {
    use siglake_storage::iceberg::SnapshotAggregates;

    let legacy = r#"{"time_buckets":{"width_ns":3600000000000,"buckets":{}}}"#;
    let parsed: SnapshotAggregates = serde_json::from_str(legacy).expect("legacy object parses");
    assert!(parsed.group_counts.is_none());
    assert!(parsed.time_buckets.is_some());
}

/// The wide object round-trips its counts and its absorbed set, and an absent
/// set reads as "nothing absorbed" — the safe direction, since it can only
/// over-collect deltas (correct, marginally slower) and never skip one (wrong).
#[test]
fn the_wide_object_round_trips() {
    use siglake_storage::iceberg::WideGroupCounts;

    let empty: WideGroupCounts = serde_json::from_str("{}").expect("an empty object parses");
    assert!(empty.group_counts.is_none());
    assert!(empty.absorbed.is_empty());
    // An object written before the rebuild watermark existed must read as
    // "never rebuilt", not as "rebuilt through sequence 0" — which would make
    // the fold discard every delta at or below 0.
    assert!(empty.rebuilt_through.is_none());

    let mut wide = WideGroupCounts {
        group_counts: counts("host", &[("a", 3), ("b", 5)], 1).to_compact(),
        absorbed: [4i64, 9, 11].into_iter().collect(),
        sketches: None,
        rebuilt_through: None,
        coverage: None,
        coverage_links: Vec::new(),
    };
    let back: WideGroupCounts =
        serde_json::from_str(&serde_json::to_string(&wide).unwrap()).unwrap();
    assert_eq!(back.absorbed, wide.absorbed);
    assert_eq!(back.decode_all().unwrap().column_total("host"), Some(9));

    // The set is what the reader consults; membership, not comparison.
    wide.absorbed.insert(2);
    assert!(wide.absorbed.contains(&2) && !wide.absorbed.contains(&3));
}

/// A delta round-trips its counts through the compact encoding, and an empty
/// one is still a valid marker (a commit that touched no covered column).
#[test]
fn delta_objects_round_trip() {
    use siglake_storage::iceberg::GroupCountDelta;

    let d = GroupCountDelta {
        sequence_number: 7,
        snapshot_id: Some(8_070_450_532_247_928_832),
        coverage_link: None,
        group_counts: Some(counts("host", &[("a", 3), ("b", 5)], 1)),
        sketches: None,
    };
    let back: GroupCountDelta = serde_json::from_str(&serde_json::to_string(&d).unwrap()).unwrap();
    assert_eq!(back.sequence_number, 7);
    assert_eq!(back.snapshot_id, Some(8_070_450_532_247_928_832));
    let gc = back.group_counts.expect("counts survive");
    assert_eq!(gc.column_total("host"), Some(9));

    let empty = GroupCountDelta {
        sequence_number: 8,
        snapshot_id: None,
        coverage_link: None,
        group_counts: None,
        sketches: None,
    };
    let back: GroupCountDelta =
        serde_json::from_str(&serde_json::to_string(&empty).unwrap()).unwrap();
    assert_eq!(back.sequence_number, 8);
    assert!(back.group_counts.is_none());
}

fn counts(
    column: &str,
    values: &[(&str, u64)],
    nulls: u64,
) -> siglake_storage::iceberg::FileGroupCounts {
    use siglake_storage::iceberg::{ColumnGroupCounts, FileGroupCounts};
    use std::collections::BTreeMap;
    let mut columns = BTreeMap::new();
    columns.insert(
        column.to_string(),
        ColumnGroupCounts {
            values: values.iter().map(|(v, n)| ((*v).to_string(), *n)).collect(),
            nulls,
        },
    );
    FileGroupCounts { columns }
}
