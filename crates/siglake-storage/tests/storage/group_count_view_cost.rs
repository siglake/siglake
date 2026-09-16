//! What it costs to hand a 1.1M-key aggregate to a query.
//!
//! The 2026-07-30 http_logs round served `top_hosts` from the Tier-1 aggregate
//! — `rows_scanned: 0`, exact, 47.8× faster than the full scan it replaced —
//! and still cost 64.5ms, against a single-digit-ms acceptance bar. The fold was
//! not the problem and neither was the top-K, which already selects over
//! borrowed keys. The cost was between them: turning the decoded aggregate into
//! the `Vec<(Option<String>, u64)>` the query path consumed allocated one
//! `String` per key, 1.1M of them, on every query.
//!
//! It is normally invisible because the per-column result is memoized. That
//! memo is a RESULT cache, though, so any run that measures the engine rather
//! than the memo (`SIGLAKE_QUERY_RESULT_CACHE=off`, which is how every
//! published board must run) pays it per query.
//!
//! The board corroborated the diagnosis before this file existed:
//! `count_distinct_host` does almost nothing after this conversion and still
//! cost 51.8ms, while low-cardinality `count_by_status` cost 1.9ms — the cost
//! tracked key count, not query shape.
//!
//! Run the report:
//!   cargo test --release -p siglake-storage --test storage group_count_view_cost:: -- --ignored --nocapture

use siglake_storage::iceberg::{ColumnGroupCounts, FileGroupCounts, GroupCounts, WideGroupCounts};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

/// Host-shaped keys, as in the http_logs corpus (~1.1M distinct dotted quads).
fn host(i: usize) -> String {
    format!(
        "{}.{}.{}.{}",
        i % 256,
        (i / 256) % 256,
        (i / 65536) % 256,
        i % 97
    )
}

fn aggregate(keys: usize) -> Arc<FileGroupCounts> {
    let mut values = BTreeMap::new();
    for i in 0..keys {
        values.insert(host(i), (i as u64 % 50) + 1);
    }
    let mut columns = BTreeMap::new();
    columns.insert("host".to_string(), ColumnGroupCounts { values, nulls: 3 });
    Arc::new(FileGroupCounts { columns })
}

/// The borrowed view must agree with the materialized one, key for key. A
/// faster path that quietly drops the NULL group or reorders keys would sail
/// through a latency check.
#[test]
fn the_borrowed_view_matches_the_materialized_rows_exactly() {
    let agg = aggregate(5_000);
    let owned = agg.column_rows("host").expect("column covered");
    let view = GroupCounts::wide(agg.clone(), "host");

    let from_view: Vec<(Option<String>, u64)> = view
        .iter()
        .map(|(k, c)| (k.map(str::to_string), c))
        .collect();
    assert_eq!(from_view, owned, "same keys, same counts, same order");
    assert_eq!(view.len(), owned.len());

    // The NULL group is a real group and must survive.
    assert_eq!(
        view.iter()
            .filter(|(k, _)| k.is_none())
            .map(|(_, c)| c)
            .sum::<u64>(),
        3,
        "NULL count must be carried, not dropped"
    );
    assert_eq!(
        view.iter().map(|(_, c)| c).sum::<u64>(),
        owned.iter().map(|(_, c)| *c).sum::<u64>()
    );

    // An uncovered column is absent, not empty — the guard distinguishes them.
    assert!(GroupCounts::wide(agg, "nope").iter().next().is_none());
}

/// What the EXACT path costs at each candidate cardinality cap.
///
/// The cap decides where exactness ends: a column above it gets no exact
/// aggregate. So choosing it is choosing how much a served exact answer is
/// allowed to cost — which wants measuring, not extrapolating from one point.
/// `top_hosts` at 1.15M keys measured 57.85ms live; the "top-K" column here is
/// the same work (build the borrowed view, select the top 100).
///
/// Run:
///   cargo test --release -p siglake-storage --test storage \
///     group_count_view_cost::report_cost_by_cap -- --ignored --nocapture
#[test]
#[ignore]
fn report_cost_by_cap() {
    const K: usize = 100;
    println!(
        "{:>12} {:>12} {:>12} {:>14}",
        "cap (keys)", "scan ms", "top-K ms", "encoded KiB"
    );
    for cap in [131_072usize, 262_144, 524_288, 1_149_520, 2_097_152] {
        let agg = aggregate(cap);
        let view = GroupCounts::wide(agg.clone(), "host");

        let t = Instant::now();
        let scanned: u64 = view.iter().map(|(_, c)| c).sum();
        let scan_ms = t.elapsed().as_secs_f64() * 1000.0;

        // What top_hosts actually does: materialize the borrowed view, then
        // select the top K.
        let t = Instant::now();
        let mut v: Vec<(Option<&str>, u64)> = view.iter().collect();
        if K < v.len() {
            v.select_nth_unstable_by_key(K - 1, |e| std::cmp::Reverse(e.1));
            v.truncate(K);
        }
        v.sort_unstable_by_key(|e| std::cmp::Reverse(e.1));
        let topk_ms = t.elapsed().as_secs_f64() * 1000.0;

        let encoded = agg.to_compact().map(|b| b.len()).unwrap_or(0) / 1024;
        assert!(scanned > 0 && !v.is_empty());
        println!("{cap:>12} {scan_ms:>12.2} {topk_ms:>12.2} {encoded:>14}");
    }
    println!(
        "\nhost on the http_logs corpus is 1,149,520 distinct — a cap below that\n\
         routes it to the sketch, i.e. top_hosts stops being exact. encoded KiB\n\
         is the wide base object's contribution for one column at that width."
    );
}

#[test]
#[ignore]
fn report_view_vs_materialize_cost() {
    println!(
        "{:>10} {:>16} {:>16} {:>10} {:>16}",
        "keys", "materialize ms", "borrow+scan ms", "speedup", "flat-scan ms"
    );
    for keys in [1_000usize, 100_000, 1_100_000] {
        let agg = aggregate(keys);

        // What the query path used to do: build the owned Vec, then scan it.
        let t = Instant::now();
        let owned = agg.column_rows("host").unwrap();
        let sink: u64 = owned.iter().map(|(_, c)| *c).sum();
        let materialize_ms = t.elapsed().as_secs_f64() * 1000.0;

        // What it does now: borrow from the Arc'd aggregate and scan.
        let t = Instant::now();
        let view = GroupCounts::wide(agg.clone(), "host");
        let sink2: u64 = view.iter().map(|(_, c)| c).sum();
        let borrow_ms = t.elapsed().as_secs_f64() * 1000.0;

        // Headroom: the same counts in a FLAT sorted Vec rather than a
        // BTreeMap. Whatever the borrow column costs above this is pointer
        // chasing through the map's nodes, not work the query needs done.
        let flat: Vec<(&str, u64)> = agg.columns["host"]
            .values
            .iter()
            .map(|(k, v)| (k.as_str(), *v))
            .collect();
        let t = Instant::now();
        let sink3: u64 = flat.iter().map(|(_, c)| *c).sum::<u64>() + agg.columns["host"].nulls;
        let flat_ms = t.elapsed().as_secs_f64() * 1000.0;

        assert_eq!(sink, sink2, "both paths must total the same");
        assert_eq!(sink, sink3);
        println!(
            "{keys:>10} {materialize_ms:>16.2} {borrow_ms:>16.2} {:>9.1}x {flat_ms:>16.2}",
            materialize_ms / borrow_ms.max(f64::MIN_POSITIVE)
        );
    }
    println!(
        "\nmaterialize = what the query path did before (the ~50ms of top_hosts'\n\
         64.5ms in the 07-30 round); borrow = what replaces it; flat-scan = what\n\
         the same counts cost in a contiguous Vec, i.e. the headroom still left\n\
         in `FileGroupCounts` storing its values as a BTreeMap."
    );
}

/// What it costs to answer a question about ONE column of a wide aggregate.
///
/// The 2026-08-03 1TB round produced a 26.5MB base spanning 22 columns — WS-7
/// auto-promotion adds one per hot attribute — and the read path decoded every
/// one of them to check a single column's total. This measures the difference
/// between that and a targeted decode.
///
/// Run:
///   cargo test --release -p siglake-storage --test storage \
///     group_count_view_cost::report_targeted_decode_vs_whole_blob -- --ignored --nocapture
#[test]
#[ignore]
fn report_targeted_decode_vs_whole_blob() {
    use siglake_storage::iceberg::WideGroupCounts;
    // A wide aggregate shaped like the 1TB one: one big column plus many
    // smaller promoted ones.
    let mut columns = BTreeMap::new();
    columns.insert(
        "host".to_string(),
        aggregate(400_000).columns["host"].clone(),
    );
    for i in 0..20 {
        let mut values = BTreeMap::new();
        for k in 0..20_000 {
            values.insert(format!("c{i}v{k:07}"), (k as u64 % 17) + 1);
        }
        columns.insert(
            format!("promoted_{i}"),
            ColumnGroupCounts { values, nulls: 0 },
        );
    }
    let full = FileGroupCounts { columns };
    let mut wide = WideGroupCounts::default();
    wide.set_group_counts(Some(full));
    let blob_kib = wide.group_counts.as_ref().map(|b| b.len()).unwrap_or(0) / 1024;

    let t = Instant::now();
    let all = wide.decode_all().expect("decodes");
    let whole_ms = t.elapsed().as_secs_f64() * 1000.0;
    let whole_total = all.column_total("host");

    let t = Instant::now();
    let one = wide.decode_column("host").expect("decodes");
    let targeted_ms = t.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(
        Some(one.total()),
        whole_total,
        "the targeted decode must agree with the whole-blob one, or it is just faster and wrong"
    );
    // The read path used to turn that Vec back into a BTreeMap to get a
    // `ColumnGroupCounts`. Time that step on its own, since it is the part
    // `SortedColumnCounts` removed and the totals above hide it in noise.
    let t = Instant::now();
    let rebuilt: BTreeMap<String, u64> = one.values.iter().cloned().collect();
    let map_ms = t.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(rebuilt.len(), one.values.len());

    println!("blob {blob_kib} KiB, 21 columns, `host` = 400,000 values");
    println!("  decode whole blob : {whole_ms:>8.2} ms");
    println!("  decode one column : {targeted_ms:>8.2} ms");
    println!(
        "  speedup           : {:>8.1}x",
        whole_ms / targeted_ms.max(f64::MIN_POSITIVE)
    );
    println!("  (rebuilding the map on top: {map_ms:>8.2} ms — what the sorted Vec avoids)");
    println!(
        "\nthe read path pays this per snapshot generation, to check one column's\n\
         total against record_count."
    );
}

/// The targeted decode returns a `Vec` instead of a `BTreeMap`, so its ordering
/// is now the CODEC's promise rather than a container's guarantee. That promise
/// is load-bearing: [`GroupCounts::iter`] documents ascending values with NULL
/// last, and every consumer that pages, merges or diffs those rows would break
/// quietly if the compact encoding ever stored values unsorted.
///
/// So assert it against the map-backed path, which is ordered by construction.
/// `GroupCounts` equality is over the iterators, so this compares the full
/// sequence — order included — not just the totals.
#[test]
fn targeted_decode_iterates_in_the_same_order_as_the_map() {
    let mut values = BTreeMap::new();
    // Values whose byte order and insertion order disagree, so a codec that
    // returned them in write order rather than sorted order fails here.
    for k in 0..5_000u64 {
        let scrambled = (k * 2_654_435_761) % 100_000;
        values.insert(format!("host-{scrambled:06}"), k + 1);
    }
    let mut columns = BTreeMap::new();
    columns.insert("host".to_string(), ColumnGroupCounts { values, nulls: 7 });
    let full = FileGroupCounts { columns };

    let mut wide = WideGroupCounts::default();
    wide.set_group_counts(Some(full.clone()));

    let from_map = GroupCounts::wide(std::sync::Arc::new(full), "host");
    let from_blob = GroupCounts::Column(std::sync::Arc::new(
        wide.decode_column("host").expect("decodes"),
    ));

    assert_eq!(from_blob.len(), from_map.len());
    assert_eq!(
        from_blob.to_rows(),
        from_map.to_rows(),
        "the targeted decode must yield the same rows in the same order"
    );
    // Pin the documented contract directly, not only by agreement with a peer.
    let rows = from_blob.to_rows();
    assert!(
        rows.windows(2)
            .all(|w| w[0].0.is_some() && (w[1].0.is_none() || w[0].0 < w[1].0)),
        "values must be ascending with NULL last"
    );
    assert_eq!(rows.last().map(|(k, c)| (k.clone(), *c)), Some((None, 7)));
}
