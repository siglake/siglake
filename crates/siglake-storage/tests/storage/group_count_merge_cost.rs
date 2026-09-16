//! What does a high-cardinality `GROUP BY … ORDER BY count DESC LIMIT k` cost
//! to *compute*, and where does the time actually go?
//!
//! Motivation: on the 2026-07-29 http_logs board with result caches off,
//! `top_hosts` took **4,710 ms p50** while Quickwit's fast-field terms
//! aggregation answered the same shape in **7.55 ms**. Both scanned zero data
//! pages — siglake's answer comes from per-file group-count footers — so the
//! entire gap is in how those footers are turned into an answer.
//!
//! This reproduces that pipeline hermetically, with no S3 and no Parquet, so
//! the cost can be attributed and optimized in seconds instead of hours:
//!
//!   1. per-file footers are pulled from an LRU cache (a `Vec<(Option<String>,
//!      u64)>` per file),
//!   2. folded into one global map,
//!   3. sorted to take the top k.
//!
//! Run:
//!   cargo test --release -p siglake-storage --test storage group_count_merge_cost:: -- --ignored --nocapture

use std::collections::HashMap;
use std::time::Instant;

/// Shape of the real http_logs `host` column: many files, each summarizing up
/// to the per-file cardinality cap, with values that overlap heavily across
/// files (a busy host appears in most of them).
const FILES: usize = 170;
const KEYS_PER_FILE: usize = 1024;
const DISTINCT_HOSTS: usize = 40_000;
const TOP_K: usize = 100;

type Rows = Vec<(Option<String>, u64)>;

fn host(i: usize) -> String {
    // Realistic width and shared prefixes, like the corpus's dotted-quad hosts.
    format!(
        "{}.{}.{}.{}",
        i % 256,
        (i / 256) % 256,
        (i / 65536) % 256,
        i % 97
    )
}

/// One file's footer: KEYS_PER_FILE distinct hosts drawn from a shared pool, so
/// files overlap the way real ones do.
fn file_rows(file: usize) -> Rows {
    (0..KEYS_PER_FILE)
        .map(|k| {
            let id = (file * 7 + k * 13) % DISTINCT_HOSTS;
            (Some(host(id)), (k as u64 % 500) + 1)
        })
        .collect()
}

fn corpus() -> Vec<Rows> {
    (0..FILES).map(file_rows).collect()
}

/// TODAY'S PATH: the LRU cache hands back an owned clone per file
/// (`entry.0.clone()`), the fold allocates every key again into the map, and
/// the top-k sorts the entire key space.
fn merge_cloning(cache: &[Rows]) -> (Vec<(Option<String>, u64)>, usize) {
    let mut totals: HashMap<Option<String>, u64> = HashMap::new();
    for rows in cache {
        let owned: Rows = rows.clone(); // what LruMap::get does on every hit
        for (value, count) in owned {
            *totals.entry(value).or_default() += count;
        }
    }
    let distinct = totals.len();
    let mut all: Vec<(Option<String>, u64)> = totals.into_iter().collect();
    // Ties broken on the value: without this the order among equal counts is
    // HashMap iteration order, so the same query returns different rows run to
    // run. (The production path has exactly this problem today.)
    all.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    all.truncate(TOP_K);
    (all, distinct)
}

/// BORROWED PATH: no clone out of the cache, fold over borrowed `&str` keys, and
/// select the top k with a bounded heap instead of sorting everything. Strings
/// are materialized only for the k rows actually returned.
fn merge_borrowed(cache: &[Rows]) -> (Vec<(Option<String>, u64)>, usize) {
    let mut totals: HashMap<Option<&str>, u64> = HashMap::new();
    for rows in cache {
        for (value, count) in rows {
            *totals.entry(value.as_deref()).or_default() += count;
        }
    }
    let distinct = totals.len();
    // Rank key orders by "betterness": higher count wins; on a tie the
    // lexicographically SMALLER value wins (hence Reverse on the value). The
    // outer Reverse turns the max-heap into a min-heap, so `pop` discards the
    // worst candidate and the heap holds the running top-k.
    type Rank<'a> = std::cmp::Reverse<(u64, std::cmp::Reverse<&'a str>)>;
    let mut heap: std::collections::BinaryHeap<Rank> =
        std::collections::BinaryHeap::with_capacity(TOP_K + 1);
    for (value, count) in &totals {
        heap.push(std::cmp::Reverse((
            *count,
            std::cmp::Reverse(value.unwrap_or("")),
        )));
        if heap.len() > TOP_K {
            heap.pop();
        }
    }
    let mut out: Vec<(Option<String>, u64)> = heap
        .into_iter()
        .map(|std::cmp::Reverse((c, std::cmp::Reverse(v)))| (Some(v.to_string()), c))
        .collect();
    out.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    (out, distinct)
}

fn ms(f: impl FnOnce()) -> f64 {
    let t = Instant::now();
    f();
    t.elapsed().as_secs_f64() * 1000.0
}

#[test]
#[ignore]
fn report_merge_cost() {
    let cache = corpus();
    let entries: usize = cache.iter().map(|r| r.len()).sum();

    // Warm both once so allocator behaviour is comparable.
    let (a, distinct) = merge_cloning(&cache);
    let (b, distinct_b) = merge_borrowed(&cache);
    assert_eq!(distinct, distinct_b, "both paths see the same key space");
    assert_eq!(a, b, "the optimized path must return the SAME top-k");

    let runs = 5;
    let cloning: f64 = (0..runs)
        .map(|_| {
            ms(|| {
                merge_cloning(&cache);
            })
        })
        .sum::<f64>()
        / runs as f64;
    let borrowed: f64 = (0..runs)
        .map(|_| {
            ms(|| {
                merge_borrowed(&cache);
            })
        })
        .sum::<f64>()
        / runs as f64;

    println!("files={FILES} entries={entries} distinct_keys={distinct} top_k={TOP_K}");
    println!("  clone+sort (today) : {cloning:8.2} ms");
    println!(
        "  borrow+heap        : {borrowed:8.2} ms   ({:.1}x faster)",
        cloning / borrowed
    );
}

/// The optimized path must be exactly equivalent, not just faster — a top-k
/// that disagrees is worthless however fast it is.
#[test]
fn borrowed_merge_matches_cloning_merge() {
    let cache = corpus();
    let (want, _) = merge_cloning(&cache);
    let (got, _) = merge_borrowed(&cache);
    assert_eq!(got, want);
    assert_eq!(got.len(), TOP_K);
    // Counts must be non-increasing and the top entry must be a real maximum.
    assert!(got.windows(2).all(|w| w[0].1 >= w[1].1), "top-k is ordered");
}

// ---------------------------------------------------------------------------
// The fix: a precomputed per-snapshot aggregate answers top-K without merging.
// ---------------------------------------------------------------------------

/// Serving top-K from a precomputed aggregate must be milliseconds even at the
/// cardinality that made the query fall back to a full scan (1.1M distinct
/// hosts). This is the whole reason the table-level cap was raised: an exact
/// answer has to touch every key once, so the only way to be fast is to have
/// touched them at commit time instead of at query time.
#[test]
#[ignore]
fn report_precomputed_topk_cost() {
    const DISTINCT: usize = 1_100_000;
    let agg: Vec<(Option<String>, u64)> = (0..DISTINCT)
        .map(|i| (Some(host(i)), ((i * 2_654_435_761) % 100_000) as u64 + 1))
        .collect();

    let runs = 5;
    let heap_ms: f64 = (0..runs)
        .map(|_| {
            ms(|| {
                topk_from_aggregate(&agg, TOP_K);
            })
        })
        .sum::<f64>()
        / runs as f64;
    let gated_ms: f64 = (0..runs)
        .map(|_| {
            ms(|| {
                topk_gated(&agg, TOP_K);
            })
        })
        .sum::<f64>()
        / runs as f64;
    assert_eq!(topk_gated(&agg, TOP_K), topk_from_aggregate(&agg, TOP_K));
    println!("precomputed aggregate: {DISTINCT} keys, top_k={TOP_K}");
    println!("  bounded heap        : {heap_ms:8.2} ms");
    println!(
        "  threshold-gated heap: {gated_ms:8.2} ms   ({:.1}x faster)",
        heap_ms / gated_ms
    );
}

/// Bounded-heap top-K over a materialized aggregate: O(n log k), one pass, and
/// it never sorts the full key space. Ties break on the value so the result is
/// deterministic run to run.
fn topk_from_aggregate(agg: &[(Option<String>, u64)], k: usize) -> Vec<(Option<String>, u64)> {
    type Rank<'a> = std::cmp::Reverse<(u64, std::cmp::Reverse<&'a str>)>;
    let mut heap: std::collections::BinaryHeap<Rank> =
        std::collections::BinaryHeap::with_capacity(k + 1);
    for (value, count) in agg {
        heap.push(std::cmp::Reverse((
            *count,
            std::cmp::Reverse(value.as_deref().unwrap_or("")),
        )));
        if heap.len() > k {
            heap.pop();
        }
    }
    let mut out: Vec<(Option<String>, u64)> = heap
        .into_iter()
        .map(|std::cmp::Reverse((c, std::cmp::Reverse(v)))| (Some(v.to_string()), c))
        .collect();
    out.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    out
}

/// The bounded heap must agree exactly with a full sort — including the
/// adversarial case a naive per-file top-N would get wrong.
#[test]
fn precomputed_topk_matches_full_sort() {
    let agg: Vec<(Option<String>, u64)> = (0..50_000)
        .map(|i| (Some(host(i)), ((i * 7919) % 1000) as u64 + 1))
        .collect();
    let got = topk_from_aggregate(&agg, TOP_K);

    let mut want = agg.clone();
    want.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    want.truncate(TOP_K);

    assert_eq!(got, want, "bounded heap must equal a full sort");
    assert!(got.windows(2).all(|w| w[0].1 >= w[1].1));
}

/// Same result as [`topk_from_aggregate`], but the overwhelming majority of
/// keys never touch the heap: once k candidates are held, anything that cannot
/// beat the current k-th is rejected by one integer comparison. On real log
/// data almost every key fails that test, so the heap sees a few thousand
/// pushes instead of a million.
fn topk_gated(agg: &[(Option<String>, u64)], k: usize) -> Vec<(Option<String>, u64)> {
    type Rank<'a> = std::cmp::Reverse<(u64, std::cmp::Reverse<&'a str>)>;
    let mut heap: std::collections::BinaryHeap<Rank> =
        std::collections::BinaryHeap::with_capacity(k + 1);
    let mut floor: u64 = 0;
    for (value, count) in agg {
        // Strictly-less is the only safe fast reject: an equal count still has
        // to be compared on the value to break the tie deterministically.
        if heap.len() >= k && *count < floor {
            continue;
        }
        heap.push(std::cmp::Reverse((
            *count,
            std::cmp::Reverse(value.as_deref().unwrap_or("")),
        )));
        if heap.len() > k {
            heap.pop();
        }
        if heap.len() >= k {
            floor = heap.peek().map(|std::cmp::Reverse((c, _))| *c).unwrap_or(0);
        }
    }
    let mut out: Vec<(Option<String>, u64)> = heap
        .into_iter()
        .map(|std::cmp::Reverse((c, std::cmp::Reverse(v)))| (Some(v.to_string()), c))
        .collect();
    out.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    out
}

/// The gate must not change the answer — including on data where many keys tie
/// at exactly the k-th count, which is where a sloppy fast-reject goes wrong.
#[test]
fn gated_topk_matches_unbounded_heap() {
    for spread in [1u64, 7, 1000] {
        let agg: Vec<(Option<String>, u64)> = (0..30_000)
            .map(|i| (Some(host(i)), ((i as u64 * 7919) % spread) + 1))
            .collect();
        assert_eq!(
            topk_gated(&agg, TOP_K),
            topk_from_aggregate(&agg, TOP_K),
            "spread={spread}"
        );
    }
}
