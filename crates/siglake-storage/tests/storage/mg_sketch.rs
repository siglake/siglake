//! Stage 0 of `docs/DESIGN_approximate_group_counts.md`: validate the
//! Misra-Gries heavy-hitter sketch hermetically, and measure it, BEFORE any of
//! it touches production code.
//!
//! Why a sketch at all. `TABLE_GROUP_COUNT_CARDINALITY` is a cliff, not a dial:
//! a column one distinct value over the cap contributes NOTHING and its
//! `GROUP BY` falls back to a full scan of 247M rows, or is refused outright by
//! the row ceiling. This is the structure that gives those columns a bounded-size
//! answer instead of no answer.
//!
//! Four properties decide whether it is safe to build on, and each has a way of
//! being wrong that a latency check would never notice:
//!
//!   1. **The bound holds.** `count(x) <= true(x) <= count(x) + error_floor`.
//!      A sketch that quietly overcounts would report a leaderboard that is
//!      simply wrong, with no symptom.
//!   2. **Heavy hitters are never missed.** Anything above the error floor must
//!      be present, or a top-K can omit a genuine top-K member.
//!   3. **Merge is sound.** The whole delta -> wide-base pipeline merges these.
//!      The `(m+1)`-th-largest subtraction is easy to get subtly wrong and fails
//!      SILENTLY — counts just drift.
//!   4. **Degrading in place is sound.** The design starts a column exact and
//!      converts to a sketch mid-stream when it crosses the cap. That is only
//!      valid if an exact map is a legitimate summary with `error_floor = 0`.
//!
//! Run the report (accuracy vs. ground truth + cost, picks `m`):
//!   cargo test --release -p siglake-storage --test storage mg_sketch:: -- --ignored --nocapture

use std::collections::HashMap;
use std::time::Instant;

/// Misra-Gries with batch eviction.
///
/// Textbook MG decrements EVERY counter on a miss, which is O(m) per miss and
/// unusable at ingest rates. This grows to `2m` counters and then prunes back
/// to `m` by subtracting the `(m+1)`-th largest count — the same primitive the
/// merge uses, and O(m) work per m insertions, so O(1) amortized.
#[derive(Clone, Debug, Default)]
struct MisraGries {
    m: usize,
    counters: HashMap<String, u64>,
    /// Total subtracted by pruning. The error bound is reportable rather than
    /// theoretical because it is carried here.
    error_floor: u64,
}

impl MisraGries {
    fn new(m: usize) -> Self {
        assert!(m > 0);
        Self {
            m,
            counters: HashMap::new(),
            error_floor: 0,
        }
    }

    /// Seed from counts already tallied exactly — the degrade-in-place entry
    /// point. An exact map is a valid summary with nothing subtracted yet.
    fn from_exact(m: usize, exact: &HashMap<String, u64>) -> Self {
        let mut mg = Self {
            m,
            counters: exact.clone(),
            error_floor: 0,
        };
        mg.prune();
        mg
    }

    fn offer(&mut self, value: &str) {
        if let Some(c) = self.counters.get_mut(value) {
            *c += 1;
            return;
        }
        self.counters.insert(value.to_string(), 1);
        if self.counters.len() > 2 * self.m {
            self.prune();
        }
    }

    /// Shrink to at most `m` counters: subtract the `(m+1)`-th largest count
    /// from every counter and drop what reaches zero.
    fn prune(&mut self) {
        if self.counters.len() <= self.m {
            return;
        }
        let mut counts: Vec<u64> = self.counters.values().copied().collect();
        counts.select_nth_unstable_by(self.m, |a, b| b.cmp(a));
        let cut = counts[self.m];
        debug_assert!(cut >= 1, "counters are always positive, so the cut is too");
        self.counters.retain(|_, c| {
            *c = c.saturating_sub(cut);
            *c > 0
        });
        self.error_floor += cut;
    }

    fn merge(&mut self, other: &MisraGries) {
        for (k, c) in &other.counters {
            *self.counters.entry(k.clone()).or_insert(0) += *c;
        }
        self.error_floor += other.error_floor;
        self.prune();
    }

    fn estimate(&self, value: &str) -> u64 {
        self.counters.get(value).copied().unwrap_or(0)
    }

    /// Top `k` by estimated count, descending, ties broken by value so the
    /// result is deterministic.
    fn top(&self, k: usize) -> Vec<(String, u64)> {
        let mut v: Vec<(String, u64)> =
            self.counters.iter().map(|(s, c)| (s.clone(), *c)).collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.truncate(k);
        v
    }
}

/// Deterministic LCG — reproducible failures matter more here than entropy.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.0 >> 11
    }
}

/// A Zipf(s=1) stream over `distinct` values by inverse transform:
/// `rank = exp(u * ln(D+1)) - 1` draws rank `r` with probability proportional
/// to `1/(r+1)`.
///
/// The first version of this used min-of-two-uniform draws, which is barely
/// skewed at all — the top value got ~2/D of the mass, every count sat below
/// the error floor, and the heavy-hitter test could not observe a single heavy
/// hitter. A generator too flat to produce the phenomenon under test is worse
/// than no test.
///
/// Note this is MORE skewed than the real corpus, where the top host is 0.11%
/// of rows. That direction is fine for the property tests (it produces heavy
/// hitters to check) and is corrected for in the report, which sizes `m`
/// against the measured figure rather than against this.
fn zipf_stream(n: usize, distinct: usize, seed: u64) -> Vec<String> {
    let mut rng = Lcg(seed);
    let span = ((distinct + 1) as f64).ln();
    (0..n)
        .map(|_| {
            let u = (rng.next() % (1 << 40)) as f64 / (1u64 << 40) as f64;
            let rank = ((u * span).exp() - 1.0) as usize;
            format!("h{}", rank.min(distinct - 1))
        })
        .collect()
}

fn exact_counts(stream: &[String]) -> HashMap<String, u64> {
    let mut m: HashMap<String, u64> = HashMap::new();
    for s in stream {
        *m.entry(s.clone()).or_default() += 1;
    }
    m
}

fn sketch_of(stream: &[String], m: usize) -> MisraGries {
    let mut mg = MisraGries::new(m);
    for s in stream {
        mg.offer(s);
    }
    mg
}

#[test]
fn the_bound_holds_for_every_value() {
    // Property 1. Undercount only, and never by more than error_floor.
    let stream = zipf_stream(200_000, 20_000, 42);
    let truth = exact_counts(&stream);
    let mg = sketch_of(&stream, 500);

    assert!(
        mg.error_floor > 0,
        "the sketch must have actually pruned, else this proves nothing"
    );
    for (value, &true_count) in &truth {
        let est = mg.estimate(value);
        assert!(
            est <= true_count,
            "{value}: estimate {est} exceeds truth {true_count} — MG must never overcount"
        );
        assert!(
            true_count <= est + mg.error_floor,
            "{value}: truth {true_count} outside [{est}, {}]",
            est + mg.error_floor
        );
    }

    // The published bound on the floor itself.
    let n = stream.len() as u64;
    assert!(
        mg.error_floor <= n / (mg.m as u64 + 1),
        "error_floor {} exceeds N/(m+1) = {}",
        mg.error_floor,
        n / (mg.m as u64 + 1)
    );
}

#[test]
fn nothing_above_the_error_floor_is_ever_missed() {
    // Property 2. This is what makes a top-K trustworthy: a value frequent
    // enough to matter cannot be absent.
    let stream = zipf_stream(200_000, 20_000, 7);
    let truth = exact_counts(&stream);
    let mg = sketch_of(&stream, 500);

    let mut checked = 0;
    for (value, &true_count) in &truth {
        if true_count > mg.error_floor {
            assert!(
                mg.counters.contains_key(value),
                "{value} has {true_count} > floor {} but was dropped",
                mg.error_floor
            );
            checked += 1;
        }
    }
    assert!(
        checked > 50,
        "only {checked} values cleared the floor — weak test"
    );
}

#[test]
fn merging_summaries_is_sound_against_the_combined_stream() {
    // Property 3. The pipeline merges per-commit sketches into a wide base, so
    // a merged summary has to obey the same bound with respect to everything
    // both sides saw.
    let a_stream = zipf_stream(120_000, 15_000, 1);
    let b_stream = zipf_stream(150_000, 15_000, 2);
    let mut combined = a_stream.clone();
    combined.extend(b_stream.iter().cloned());
    let truth = exact_counts(&combined);

    let mut merged = sketch_of(&a_stream, 400);
    merged.merge(&sketch_of(&b_stream, 400));

    assert!(
        merged.counters.len() <= merged.m,
        "merge must respect the capacity"
    );
    for (value, &true_count) in &truth {
        let est = merged.estimate(value);
        assert!(est <= true_count, "{value}: merged estimate overcounts");
        assert!(
            true_count <= est + merged.error_floor,
            "{value}: truth {true_count} outside the merged bound"
        );
    }
}

#[test]
fn merge_is_order_independent() {
    // Union-and-sum is commutative and the prune is deterministic, so merging
    // in either order must produce the identical summary — not merely a valid
    // one. Two drains folding the same pair must not diverge.
    let a = sketch_of(&zipf_stream(80_000, 9_000, 11), 300);
    let b = sketch_of(&zipf_stream(90_000, 9_000, 12), 300);

    let mut ab = a.clone();
    ab.merge(&b);
    let mut ba = b.clone();
    ba.merge(&a);

    assert_eq!(ab.error_floor, ba.error_floor);
    assert_eq!(ab.counters, ba.counters, "merge must be commutative");
}

#[test]
fn an_exact_map_degrades_into_a_valid_sketch() {
    // Property 4. The design counts a column exactly until it crosses the cap,
    // then converts in place and keeps going. That is only sound if the
    // converted state is a real summary — otherwise every wide column starts
    // its sketch life with a silent lie.
    let head = zipf_stream(60_000, 12_000, 21);
    let tail = zipf_stream(60_000, 12_000, 22);
    let mut whole = head.clone();
    whole.extend(tail.iter().cloned());
    let truth = exact_counts(&whole);

    // Exact for the first half...
    let exact_head = exact_counts(&head);
    // ...then convert and continue in sketch mode.
    let mut mg = MisraGries::from_exact(400, &exact_head);
    for s in &tail {
        mg.offer(s);
    }

    for (value, &true_count) in &truth {
        let est = mg.estimate(value);
        assert!(est <= true_count, "{value}: degraded sketch overcounts");
        assert!(
            true_count <= est + mg.error_floor,
            "{value}: truth {true_count} outside the degraded bound"
        );
    }

    // And an exact map that FITS converts losslessly — the common case for a
    // column that never crosses the cap.
    let small = exact_counts(&zipf_stream(5_000, 50, 23));
    let kept = MisraGries::from_exact(400, &small);
    assert_eq!(kept.error_floor, 0, "a map within capacity loses nothing");
    assert_eq!(kept.counters, small);
}

#[test]
#[ignore]
fn report_accuracy_and_cost_to_choose_m() {
    // Ground truth from the 2026-07-31 round: the http_logs host column has
    // 1,149,520 distinct values over 247,249,096 rows, top host 277,634
    // (0.11%). Scaled down here by ~25x so the test is runnable; the SHAPE is
    // what picks `m`, and the worst-case bound N/(m+1) scales with it.
    const ROWS: usize = 10_000_000;
    const DISTINCT: usize = 1_150_000;
    const K: usize = 100;

    // Size `m` against the MEASURED corpus first — this is the number that
    // actually decides the default, and it needs no simulation. From the
    // 2026-07-31 round: N = 247,249,096 rows, top host = 277,634 (0.1123%).
    // A value survives pruning only while its count exceeds the error floor,
    // and the floor is bounded by N/(m+1).
    const REAL_N: u64 = 247_249_096;
    const REAL_TOP: u64 = 277_634;
    let m_to_retain_top = REAL_N / REAL_TOP; // floor < top  =>  m+1 > N/top
    let m_for_1pct = REAL_N / (REAL_TOP / 100); // floor < 1% of top
    println!(
        "http_logs host, measured: N={REAL_N}, top={REAL_TOP} ({:.4}% of rows)",
        REAL_TOP as f64 * 100.0 / REAL_N as f64
    );
    println!("  m to retain the top host at all : >= {m_to_retain_top}");
    println!("  m to bound its error under 1%   : >= {m_for_1pct}");
    println!("  (worst case; skew makes the realised floor smaller)\n");

    println!("stream: {ROWS} rows, {DISTINCT} distinct (http_logs host shape)");
    let stream = zipf_stream(ROWS, DISTINCT, 99);

    let t = Instant::now();
    let truth = exact_counts(&stream);
    let exact_ms = t.elapsed().as_secs_f64() * 1000.0;
    let mut top_truth: Vec<(String, u64)> = truth.iter().map(|(s, c)| (s.clone(), *c)).collect();
    top_truth.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    top_truth.truncate(K);
    let truth_set: std::collections::HashSet<&String> = top_truth.iter().map(|(s, _)| s).collect();

    println!(
        "exact: {exact_ms:.0} ms, {} distinct tracked\n",
        truth.len()
    );
    println!(
        "{:>9} {:>10} {:>12} {:>14} {:>12} {:>10}",
        "m", "build ms", "counters", "error_floor", "top100 hit", "max err%"
    );
    for m in [1_000usize, 10_000, 100_000] {
        let t = Instant::now();
        let mg = sketch_of(&stream, m);
        let build_ms = t.elapsed().as_secs_f64() * 1000.0;

        let top = mg.top(K);
        let hit = top.iter().filter(|(s, _)| truth_set.contains(s)).count();
        // Worst relative error among the reported top-K.
        let max_err = top
            .iter()
            .map(|(s, est)| {
                let t = truth.get(s).copied().unwrap_or(0);
                if t == 0 {
                    100.0
                } else {
                    (t - est) as f64 * 100.0 / t as f64
                }
            })
            .fold(0.0f64, f64::max);
        println!(
            "{m:>9} {build_ms:>10.0} {:>12} {:>14} {:>11}/{K} {max_err:>9.2}%",
            mg.counters.len(),
            mg.error_floor,
            hit
        );
    }
    println!(
        "\ntop100 hit = how many of the reported top-100 are genuinely top-100.\n\
         The bound is worst-case; log data is skewed, so realised error is what\n\
         picks the default. Compare build ms against the exact column: that is\n\
         the per-commit cost the cap exists to avoid paying."
    );
}
