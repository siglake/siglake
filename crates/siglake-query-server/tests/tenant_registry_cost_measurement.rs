//! Bounded local measurement of claim-driven tenant registry growth.
//!
//! This is ignored in the ordinary gate because it creates 100 namespaces and
//! their tables. Run it when changing the admission contract:
//!
//! ```text
//! cargo test -p siglake-query-server --test tenant_registry_cost_measurement \
//!   -- --ignored --nocapture
//! ```
//!
//! The tracking allocator reports live requested bytes rather than allocator
//! RSS. The filesystem figures include the SQLite catalog and Iceberg metadata;
//! all columns are deltas from one already-open default context.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use siglake_query_server::TenantRegistry;
use siglake_storage::iceberg::{IcebergContext, DEFAULT_CATALOG_FILE};

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

struct TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let out = unsafe { System.realloc(ptr, layout, new_size) };
        if !out.is_null() {
            if new_size >= layout.size() {
                LIVE_BYTES.fetch_add(new_size - layout.size(), Ordering::Relaxed);
            } else {
                LIVE_BYTES.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        out
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

fn tree_size_and_files(root: &Path) -> std::io::Result<(u64, u64)> {
    let mut bytes = 0;
    let mut files = 0;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            let (child_bytes, child_files) = tree_size_and_files(&entry.path())?;
            bytes += child_bytes;
            files += child_files;
        } else if metadata.is_file() {
            bytes += metadata.len();
            files += 1;
        }
    }
    Ok((bytes, files))
}

/// Characterization only: the retained cost is recorded in the query tenant
/// admission design, not asserted as a performance contract.
#[tokio::test]
#[ignore = "manual bounded measurement for query tenant admission"]
async fn measure_retained_cost_of_one_hundred_tenants() {
    let tmp = tempfile::tempdir().expect("temporary warehouse");
    let warehouse = tmp.path().join("warehouse");
    let default = Arc::new(
        IcebergContext::open(&warehouse)
            .await
            .expect("open default context"),
    );
    let registry = TenantRegistry::new(default);
    let baseline_heap = LIVE_BYTES.load(Ordering::Relaxed);
    let (baseline_disk, baseline_files) =
        tree_size_and_files(&warehouse).expect("measure baseline warehouse");
    let baseline_catalog = std::fs::metadata(warehouse.join(DEFAULT_CATALOG_FILE))
        .expect("catalog metadata")
        .len();
    let started = Instant::now();

    println!("tenants,elapsed_ms,live_heap_bytes,catalog_bytes,warehouse_bytes,files");
    for tenant_number in 1..=100 {
        registry
            .resolve(&format!("measure_{tenant_number:03}"))
            .await
            .expect("resolve tenant context");

        if [1, 10, 25, 50, 100].contains(&tenant_number) {
            let heap = LIVE_BYTES
                .load(Ordering::Relaxed)
                .saturating_sub(baseline_heap);
            let (disk, files) = tree_size_and_files(&warehouse).expect("measure tenant warehouse");
            let catalog = std::fs::metadata(warehouse.join(DEFAULT_CATALOG_FILE))
                .expect("catalog metadata")
                .len();
            println!(
                "{tenant_number},{},{heap},{},{},{}",
                started.elapsed().as_millis(),
                catalog.saturating_sub(baseline_catalog),
                disk.saturating_sub(baseline_disk),
                files.saturating_sub(baseline_files),
            );
        }
    }
}
