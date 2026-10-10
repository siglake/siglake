//! Reproducible cost breakdown for reading the ~1.15M-group `host` aggregate.
//!
//! Run:
//!
//! ```text
//! cargo test --release -p siglake-storage --test group_count_streaming_profile \
//!   report_decode_materialization_cost -- --ignored --nocapture
//! ```
//!
//! The two arms consume the same encoded bytes in one process. `owned_decode`
//! is the preserved full decoder; `stream_prepare` validates the whole payload
//! and retains its decoded body; `stream_select` reconstructs every key in one
//! scratch buffer and owns only competitive top-100 keys. CPU is process CPU,
//! not an inference from wall time. Heap readings cover allocations made while
//! each result remains live.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use siglake_storage::iceberg::{ColumnGroupCounts, FileGroupCounts, WideGroupCounts};

struct TrackingAllocator;

static TRACKING: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

fn record_alloc(size: usize) {
    let live = LIVE.fetch_add(size as isize, Ordering::Relaxed) + size as isize;
    if TRACKING.load(Ordering::Relaxed) {
        PEAK.fetch_max(live, Ordering::Relaxed);
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(size, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(ptr, old, new_size) };
        if !new.is_null() {
            LIVE.fetch_sub(old.size() as isize, Ordering::Relaxed);
            record_alloc(new_size);
        }
        new
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

#[derive(Clone, Copy, Debug)]
struct Reading {
    wall: Duration,
    cpu: Duration,
    allocations: usize,
    allocated_bytes: usize,
    peak_heap_growth: usize,
}

struct Window {
    wall: Instant,
    cpu: Duration,
    baseline: isize,
}

fn process_cpu() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) };
    assert_eq!(rc, 0, "read process CPU clock");
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

fn start(track_allocations: bool) -> Window {
    let baseline = LIVE.load(Ordering::Relaxed);
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    TRACKING.store(track_allocations, Ordering::Relaxed);
    Window {
        wall: Instant::now(),
        cpu: process_cpu(),
        baseline,
    }
}

fn run<T>(track_allocations: bool, f: impl FnOnce() -> T) -> (T, Reading) {
    let window = start(track_allocations);
    let value = f();
    std::hint::black_box(&value);
    (value, window.finish())
}

fn combine(time: Reading, allocations: Reading) -> Reading {
    Reading {
        wall: time.wall,
        cpu: time.cpu,
        allocations: allocations.allocations,
        allocated_bytes: allocations.allocated_bytes,
        peak_heap_growth: allocations.peak_heap_growth,
    }
}

impl Window {
    fn finish(self) -> Reading {
        let cpu = process_cpu() - self.cpu;
        let wall = self.wall.elapsed();
        TRACKING.store(false, Ordering::Relaxed);
        Reading {
            wall,
            cpu,
            allocations: ALLOCATIONS.load(Ordering::Relaxed),
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
            peak_heap_growth: (PEAK.load(Ordering::Relaxed) - self.baseline).max(0) as usize,
        }
    }
}

fn host(i: usize) -> String {
    format!(
        "{}.{}.{}.{}",
        i % 256,
        (i / 256) % 256,
        (i / 65_536) % 256,
        i % 97
    )
}

fn wide(keys: usize) -> WideGroupCounts {
    let values = (0..keys).map(|i| (host(i), (i as u64 % 50) + 1)).collect();
    let mut columns = BTreeMap::new();
    columns.insert("host".to_string(), ColumnGroupCounts { values, nulls: 3 });
    let mut wide = WideGroupCounts::default();
    wide.set_group_counts(Some(FileGroupCounts { columns }));
    wide
}

fn print_reading(name: &str, reading: Reading) {
    println!(
        "{name:<16} wall_ms={:>8.2} cpu_ms={:>8.2} allocations={:>9} \
         allocated_mib={:>8.2} peak_heap_mib={:>8.2}",
        reading.wall.as_secs_f64() * 1_000.0,
        reading.cpu.as_secs_f64() * 1_000.0,
        reading.allocations,
        reading.allocated_bytes as f64 / 1_048_576.0,
        reading.peak_heap_growth as f64 / 1_048_576.0,
    );
}

fn better_borrowed(key: &str, count: u64, right: &(String, u64)) -> bool {
    count > right.1 || (count == right.1 && key < right.0.as_str())
}

fn select_top(
    streamed: &siglake_bloom::group_counts::StreamingColumnCounts,
    k: usize,
) -> (Vec<(String, u64)>, u64) {
    let mut selected: Vec<(String, u64)> = Vec::with_capacity(k * 2);
    let mut cutoff: Option<(String, u64)> = None;
    let mut total = 0u64;
    streamed.for_each(|key, count| {
        total = total.saturating_add(count);
        let Some(key) = key else {
            return;
        };
        if cutoff
            .as_ref()
            .is_some_and(|worst| !better_borrowed(key, count, worst))
        {
            return;
        }
        selected.push((key.to_string(), count));
        if selected.len() == k * 2 {
            selected
                .select_nth_unstable_by(k - 1, |a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            selected.truncate(k);
            cutoff = Some(selected[k - 1].clone());
        }
    });
    selected.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    selected.truncate(k);
    (selected, total)
}

#[test]
#[ignore]
fn report_decode_materialization_cost() {
    const GROUPS: usize = 1_149_520;
    const K: usize = 100;

    let wide = wide(GROUPS);
    let encoded_kib = wide.group_counts.as_ref().unwrap().len() / 1024;

    // Time without allocator atomics on the hot path, then repeat solely for
    // allocation counts/bytes. Combining those separate passes avoids making
    // 1.15M per-allocation atomics look like decoder CPU.
    let (mut owned, owned_time) = run(false, || wide.decode_column("host").unwrap());
    let (_, owned_allocations) = run(true, || wide.decode_column("host").unwrap());
    let owned_reading = combine(owned_time, owned_allocations);

    let (streamed, prepare_time) = run(false, || wide.streaming_column("host").unwrap());
    let (_, prepare_allocations) = run(true, || wide.streaming_column("host").unwrap());
    let prepare_reading = combine(prepare_time, prepare_allocations);

    let ((selected, streamed_total), select_time) = run(false, || select_top(&streamed, K));
    let (_, select_allocations) = run(true, || select_top(&streamed, K));
    let select_reading = combine(select_time, select_allocations);

    let owned_total = owned.total();
    owned
        .values
        .sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    assert_eq!(streamed_total, owned_total);
    assert_eq!(selected, owned.values[..K]);

    println!("groups={GROUPS} encoded_kib={encoded_kib} top_k={K}");
    print_reading("owned_decode", owned_reading);
    print_reading("stream_prepare", prepare_reading);
    print_reading("stream_select", select_reading);
    println!(
        "stream_total      wall_ms={:>8.2} cpu_ms={:>8.2} allocations={:>9} \
         allocated_mib={:>8.2} peak_heap_mib={:>8.2}",
        (prepare_reading.wall + select_reading.wall).as_secs_f64() * 1_000.0,
        (prepare_reading.cpu + select_reading.cpu).as_secs_f64() * 1_000.0,
        prepare_reading.allocations + select_reading.allocations,
        (prepare_reading.allocated_bytes + select_reading.allocated_bytes) as f64 / 1_048_576.0,
        prepare_reading
            .peak_heap_growth
            .max(select_reading.peak_heap_growth) as f64
            / 1_048_576.0,
    );
}
