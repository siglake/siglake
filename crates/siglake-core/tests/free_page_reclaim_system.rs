//! Isolated Linux GNU System-allocator checks; no global allocator override.
//! Each parent owns one child with a 20-second deadline and kill/wait cleanup.
#![cfg(all(target_os = "linux", target_env = "gnu"))]

use siglake_core::{reclaim_allocator_free_pages, AllocatorFreePageReclaim};
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const CHILD_MODE: &str = "SIGLAKE_FREE_PAGE_RECLAIM_TEST_CHILD";

struct OwnedChild(Child);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

fn isolated(mode: &str) {
    let mut child = OwnedChild(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "system_allocator_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_MODE, mode)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            let mut output = String::new();
            child
                .0
                .stdout
                .take()
                .unwrap()
                .take(65_537)
                .read_to_string(&mut output)
                .unwrap();
            let mut errors = String::new();
            child
                .0
                .stderr
                .take()
                .unwrap()
                .take(65_537)
                .read_to_string(&mut errors)
                .unwrap();
            assert!(
                output.len() <= 65_536 && errors.len() <= 65_536,
                "child output bound"
            );
            // Prefix libtest summaries so whole-workspace parsers do not count
            // the isolated child again as an additional top-level test suite.
            for line in output.lines().chain(errors.lines()) {
                eprintln!("isolated_reclaim_child[{mode}] {line}");
            }
            assert!(status.success(), "isolated {mode} exited {status}");
            assert!(
                output.contains("test result: ok. 1 passed; 0 failed;"),
                "exact child test did not pass"
            );
            assert!(
                output.contains(&format!("reclaim_child_completed mode={mode}")),
                "child body did not complete"
            );
            break;
        }
        assert!(Instant::now() < deadline, "isolated {mode} timed out");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn resident_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
    let resident: u64 = statm.split_whitespace().nth(1).unwrap().parse().unwrap();
    // SAFETY: sysconf(_SC_PAGESIZE) has no pointer preconditions.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    assert!(page_size > 0);
    resident.checked_mul(page_size as u64).unwrap()
}

#[test]
fn touched_live_system_buffers_survive_reclaim() {
    isolated("live");
}

#[test]
fn freed_system_heap_pages_are_reclaimed_with_live_neighbors() {
    isolated("freed");
}

#[test]
fn system_allocator_child() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    assert!(matches!(mode.as_str(), "live" | "freed"));
    assert!(siglake_core::allocator_memory_readout().is_none());
    assert!(matches!(
        reclaim_allocator_free_pages().unwrap(),
        AllocatorFreePageReclaim::GlibcHeap { .. }
    ));
    // 32 MiB total: 512 touched 64-KiB heap allocations, below glibc's default
    // mmap threshold. Free alternating chunks, retaining live neighbors so this
    // exercises interior free heap pages rather than automatic mmap unmapping.
    // No mallopt, allocator environment, or global allocator override is used.
    let mut chunks: Vec<Option<Vec<u8>>> = (0..512)
        .map(|i| Some(vec![(i % 251 + 1) as u8; 64 * 1024]))
        .collect();
    std::hint::black_box(&chunks);
    if mode == "freed" {
        for i in (0..chunks.len()).step_by(2) {
            drop(chunks[i].take());
        }
    }
    let before = resident_bytes();
    let result = reclaim_allocator_free_pages().unwrap();
    let after = resident_bytes();
    println!("mode={mode} before_rss={before} after_rss={after} backend={result:?}");
    assert!(matches!(result, AllocatorFreePageReclaim::GlibcHeap { .. }));
    for (i, chunk) in chunks.iter().enumerate() {
        if let Some(bytes) = chunk {
            assert!(bytes.iter().all(|b| *b == (i % 251 + 1) as u8));
        }
    }
    if mode == "freed" {
        assert_eq!(
            result,
            AllocatorFreePageReclaim::GlibcHeap {
                released_pages: true
            }
        );
        // Of ~16 MiB freed, require a conservative 4 MiB actual RSS reduction.
        // This local allocator behavior test does not alter any workload limit.
        assert!(
            before.saturating_sub(after) >= 4 * 1024 * 1024,
            "free heap pages were not observably reclaimed: {before} -> {after}"
        );
    }
    std::hint::black_box(&chunks);
    println!("reclaim_child_completed mode={mode}");
}
