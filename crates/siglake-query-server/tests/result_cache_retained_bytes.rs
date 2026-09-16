//! Calibration for [`siglake_query_server::format::retained_heap_bytes`]
//! against what the allocator actually hands out (#2304).
//!
//! The estimator exists because the SQL result cache used to charge
//! `serde_json::to_vec(body).len()` for an encoding it dropped, while retaining
//! the `RecordsResponse` — measured 29–35x larger on a one-column name list
//! (README, "Caches"). Nothing in `std`
//! reports a `BTreeMap`'s node count, so the retained figure is MODELLED, and a
//! model nobody checks is the same defect wearing a different number.
//!
//! This file checks it the only way that settles it: a tracking global
//! allocator, a live-heap delta taken while the body is held, and an assertion
//! against it on every shape the cache stores — one-column name lists at the
//! row cap and over it, mixed-type rows, rows wider than one `BTreeMap` node,
//! long strings, and the metadata (`cost`, `stats`) that rides on a real SQL
//! body.
//!
//! Two accuracy classes, because the model has exactly one approximate term.
//! A row of at most eleven columns is ONE leaf node and the estimate is exact
//! ([`Accuracy::Exact`], ±1%). A wider row is a tree whose node count `std`
//! does not report, so the leaf fill is fitted and the estimate is allowed to
//! run high ([`Accuracy::Conservative`]) — over-billing costs cache capacity,
//! under-billing is the defect this closes.
//!
//! The fit, measured 2026-09-08 with ascending `insert` of n keys (leaf 632 B,
//! internal 728 B, derived by subtracting the key allocations from the live
//! delta):
//!
//! | entries | 12 | 18 | 20 | 24 | 30 | 34 | 40 | 50 | 60 | 80 | 100 | 128 | 200 | 500 |
//! |---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
//! | leaves | 2 | 2 | 3 | 3 | 4 | 5 | 6 | 7 | 8 | 11 | 14 | 18 | 28 | 71 |
//! | pairs/leaf | 6.0 | 9.0 | 6.7 | 8.0 | 7.5 | 6.8 | 6.7 | 7.1 | 7.5 | 7.3 | 7.1 | 7.1 | 7.1 | 7.0 |
//!
//! THIS FILE HOLDS EXACTLY ONE `#[test]`, and must keep holding one. The
//! allocator is process-wide, so a second test in this binary would run
//! concurrently with this one under the default `cargo test` and each would
//! report the other's allocations in its live delta — a flake in the workspace
//! gate that `--test-threads=1` would hide locally.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Map, Value};
use siglake_query_server::cost::{ComplexityClass, CostReport};
use siglake_query_server::format::{
    retained_heap_bytes, DistPhaseStats, PhaseStats, RecordsResponse, ScanDetail, ScanStats,
};

static LIVE: AtomicUsize = AtomicUsize::new(0);

struct Tracking;

unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let out = unsafe { System.realloc(ptr, layout, new_size) };
        if !out.is_null() {
            if new_size >= layout.size() {
                LIVE.fetch_add(new_size - layout.size(), Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        out
    }
}

#[global_allocator]
static ALLOC: Tracking = Tracking;

/// Heap bytes still live while `build`'s value is held — the retained cost of
/// the representation, not the traffic that built it.
fn measured_retained<T>(build: impl FnOnce() -> T) -> usize {
    let before = LIVE.load(Ordering::Relaxed);
    let held = build();
    let retained = LIVE.load(Ordering::Relaxed).saturating_sub(before);
    drop(held);
    retained
}

/// What the estimate is held to for a shape.
#[derive(Clone, Copy)]
enum Accuracy {
    /// Rows of at most eleven columns: one leaf node each, nothing modelled.
    Exact,
    /// Wider rows: the leaf fill is fitted, so the estimate may run high but
    /// must not run low. Under-billing is the defect (#2304); over-billing only
    /// costs entries.
    Conservative,
}

impl Accuracy {
    fn bounds(self) -> std::ops::RangeInclusive<f64> {
        match self {
            Self::Exact => 0.99..=1.01,
            Self::Conservative => 0.95..=1.35,
        }
    }
}

fn body(rows: Vec<Value>, columns: Vec<String>) -> RecordsResponse {
    RecordsResponse {
        columns,
        row_count: rows.len(),
        rows: Value::Array(rows),
        truncated: false,
        max_rows: None,
        cost: None,
        stats: None,
        approximation: None,
    }
}

/// A one-column `SELECT DISTINCT` list: the Jaeger name-list shape, and the
/// shape §1.2 measured the 29–35x understatement on.
fn name_list(column: &str, count: usize, name_len: usize) -> RecordsResponse {
    let rows = (0..count)
        .map(|i| {
            let mut row = Map::new();
            row.insert(
                column.to_string(),
                Value::String(format!("{:0>width$}", i, width = name_len)),
            );
            Value::Object(row)
        })
        .collect();
    body(rows, vec![column.to_string()])
}

/// `columns` columns of alternating string / number / null / bool values —
/// what an ordinary SQL browse renders, and, past eleven columns, a row that
/// no longer fits one `BTreeMap` node.
fn wide_rows(columns: usize, count: usize, value_len: usize) -> RecordsResponse {
    let names: Vec<String> = (0..columns).map(|c| format!("column_{c}")).collect();
    let rows = (0..count)
        .map(|r| {
            let mut row = Map::new();
            for (c, name) in names.iter().enumerate() {
                let value = match c % 4 {
                    0 => Value::String("v".repeat(value_len)),
                    1 => Value::Number((r as u64 * 31 + c as u64).into()),
                    2 => Value::Bool(r % 2 == 0),
                    _ => Value::Null,
                };
                row.insert(name.clone(), value);
            }
            Value::Object(row)
        })
        .collect();
    body(rows, names)
}

fn check(label: &str, accuracy: Accuracy, build: impl Fn() -> RecordsResponse) {
    let measured = measured_retained(&build);
    let estimated = retained_heap_bytes(&build());
    let ratio = estimated as f64 / measured as f64;
    let bounds = accuracy.bounds();
    println!("{label:>34}  measured {measured:>9} B  estimated {estimated:>9} B  ratio {ratio:.3}");
    // An empty body allocates nothing at all, so there is no ratio to take:
    // the estimate has to be nothing too.
    if measured == 0 {
        assert_eq!(
            estimated, 0,
            "{label}: nothing allocated, {estimated} B billed"
        );
        return;
    }
    assert!(
        bounds.contains(&ratio),
        "{label}: retained_heap_bytes estimated {estimated} B against {measured} B measured \
         (ratio {ratio:.3}, allowed {:.2}..={:.2})",
        bounds.start(),
        bounds.end()
    );
}

/// The model tracks the allocator on every shape the result cache stores,
/// row tree and response metadata alike.
#[test]
fn retained_heap_bytes_tracks_the_allocator() {
    println!();
    // The row cap and the shapes either side of it, at the name lengths §1.2
    // and §1.3 measured.
    check("names x128 (len 8)", Accuracy::Exact, || {
        name_list("service", 128, 8)
    });
    check("names x128 (len 64)", Accuracy::Exact, || {
        name_list("service", 128, 64)
    });
    check("names x800 (len 8)", Accuracy::Exact, || {
        name_list("service", 800, 8)
    });
    check("names x1 (len 8)", Accuracy::Exact, || {
        name_list("service", 1, 8)
    });
    // Multi-column: 4 and 8 fit one 11-slot node, 12 and 40 do not.
    check("4 cols x128", Accuracy::Exact, || wide_rows(4, 128, 16));
    check("8 cols x128", Accuracy::Exact, || wide_rows(8, 128, 16));
    check("12 cols x128", Accuracy::Conservative, || {
        wide_rows(12, 128, 16)
    });
    check("20 cols x128", Accuracy::Conservative, || {
        wide_rows(20, 128, 16)
    });
    check("40 cols x128", Accuracy::Conservative, || {
        wide_rows(40, 128, 16)
    });
    // One fat row: the value bytes dominate the node overhead.
    check("4 cols x1 (4 KiB values)", Accuracy::Exact, || {
        wide_rows(4, 1, 4096)
    });
    // Empty: `batches_to_records` renders `rows: []` with no columns.
    check("empty", Accuracy::Exact, || body(Vec::new(), Vec::new()));

    // And the metadata a real SQL body carries — `cost`, `stats` and their
    // boxes — is charged too, not just the row tree.
    let with_metadata = || {
        let mut response = name_list("service", 16, 8);
        response.cost = Some(CostReport {
            files_to_scan: Some(12),
            files_considered: Some(40),
            estimated_bytes_scanned: 1 << 20,
            estimated_rows_processed: 4096,
            estimated_runtime_seconds: 0.25,
            complexity_class: ComplexityClass::Small,
            warnings: vec!["an estimate over incomplete statistics".to_string()],
            exact: false,
        });
        response.stats = Some(ScanStats {
            rows_scanned: 4096,
            bytes_scanned: 1 << 20,
            spill_bytes: 0,
            phases: Some(Box::new(PhaseStats {
                plan_micros: 900,
                buffer_delta_micros: 12,
                collect_micros: 4_000,
                render_micros: 300,
                distributed: Some(DistPhaseStats {
                    mode: "aggregate".to_string(),
                    shard_wall_micros: vec![1_200, 1_450],
                    merge_micros: 80,
                    peers: 2,
                    peer_generation: 7,
                }),
            })),
            scan: Some(Box::new(ScanDetail {
                files_planned: 12,
                files_read: 9,
                ordering: Some("advertised".to_string()),
                ..Default::default()
            })),
            served_by: Some("materialized".to_string()),
        });
        response
    };

    let bare = retained_heap_bytes(&name_list("service", 16, 8));
    check("16 names + cost + stats", Accuracy::Exact, with_metadata);
    assert!(
        retained_heap_bytes(&with_metadata()) > bare,
        "the metadata must be charged, not skipped"
    );
}
