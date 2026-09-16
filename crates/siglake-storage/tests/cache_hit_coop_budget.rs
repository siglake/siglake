//! #1165: the vendored FileIO's byte-range object cache serves hits inside the
//! caller's poll, so a loop of hits — a warm manifest walk through
//! `load_manifest`, a footer sweep, a fully cached page scan — never returned
//! `Pending`, and a `tokio::time::timeout` wrapped around it could not fire
//! until the loop ended (a timeout only fires between polls). Each hit now
//! spends one unit of tokio's cooperative budget, so an all-hit loop yields
//! every 128 reads and the timeout above it becomes enforceable.
//!
//! The probe is a hand poll. A timeout can only fire between polls, so
//! `Pending` from one poll of the hit loop is the whole of the property, and
//! polling checks it without a clock.
//!
//! `timeout(Duration::ZERO, ..)` reads as the sharper probe. It is a race:
//! tokio rounds a deadline up to the end of the current millisecond
//! (`runtime/time/source.rs`, `deadline_to_tick`), so a zero timeout fires
//! somewhere in (0, 1] ms and the assertion turns into a race between that
//! timer and 1,000 hash-and-clone operations. Measured on the gate host
//! 2026-09-10: the loops take 1.7-2.5 ms against a timer that fired at
//! 0.69-1.22 ms, a margin of about 1.7x. CI runs on a faster core and lost the
//! race — this was the single failing target of `cargo test --workspace` in
//! run 34508646552. Do not reinstate the timer.
//!
//! Own test binary (one test) because the object cache is process-global and
//! toggling it must not race other tests.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use iceberg::io::{FileIOBuilder, MemoryStorageFactory};

const PATH: &str = "memory://warehouse/data/siglake-0000.parquet";

#[tokio::test]
async fn object_cache_hits_yield_so_a_timeout_above_the_loop_can_fire() {
    // Enable the byte-range object cache for this process (0 = disabled, the
    // default in a test binary).
    siglake_storage::configure_object_cache_max_bytes(64 * 1024 * 1024);

    let file_io = FileIOBuilder::new(Arc::new(MemoryStorageFactory)).build();
    let payload = Bytes::from_static(b"immutable data-file bytes");
    let len = payload.len() as u64;
    file_io
        .new_output(PATH)
        .unwrap()
        .write(payload.clone())
        .await
        .unwrap();
    let input = file_io.new_input(PATH).unwrap();
    let reader = input.reader().await.unwrap();

    // Fill both cache keys once: the whole-file read and the byte range.
    assert_eq!(input.read().await.unwrap(), payload);
    assert_eq!(reader.read(0..len).await.unwrap(), payload);

    // 1,000 whole-file hits (`InputFile::read`, the manifest and manifest-list
    // path): more than the budget left in this poll, so the loop has to park.
    let whole_file_hits = std::pin::pin!(async {
        for _ in 0..1000 {
            assert_eq!(input.read().await.unwrap(), payload);
        }
    });
    assert!(
        futures::poll!(whole_file_hits).is_pending(),
        "a loop of whole-file object-cache hits must return Pending once the \
         cooperative budget is spent, or no timeout above it can ever fire"
    );

    // Same for byte-range hits (`CachingFileRead::read`, the footer and page
    // path every Parquet read takes).
    let range_hits = std::pin::pin!(async {
        for _ in 0..1000 {
            assert_eq!(reader.read(0..len).await.unwrap(), payload);
        }
    });
    assert!(
        futures::poll!(range_hits).is_pending(),
        "a loop of byte-range object-cache hits must return Pending once the \
         cooperative budget is spent, or no timeout above it can ever fire"
    );

    // What gives those two assertions their meaning: lift the cooperative
    // budget and the identical loop finishes inside one poll. A hit that
    // started parking for some unrelated reason — a lock, a real read — would
    // satisfy both assertions above on its own, and #1165 could regress with
    // the test still green.
    let unbudgeted_hits = std::pin::pin!(tokio::task::unconstrained(async {
        for _ in 0..1000 {
            assert_eq!(input.read().await.unwrap(), payload);
        }
    }));
    assert!(
        futures::poll!(unbudgeted_hits).is_ready(),
        "an all-hit loop parks on nothing but the cooperative budget, so with \
         the budget lifted it must run to completion in a single poll"
    );

    // Yielding is not failing: with budget to spare the same loops complete and
    // every hit still returns the cached bytes.
    tokio::time::timeout(Duration::from_secs(30), async {
        for _ in 0..1000 {
            assert_eq!(input.read().await.unwrap(), payload);
            assert_eq!(reader.read(0..len).await.unwrap(), payload);
        }
    })
    .await
    .expect("the hit loops complete under a real budget");

    // Restore the default (disabled) so other test binaries are unaffected.
    siglake_storage::configure_object_cache_max_bytes(0);
}
