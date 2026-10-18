//! Explicit, best-effort release of allocator-owned free pages.

use crate::AllocatorPurgeReadout;

/// The backend actually invoked, rather than a claim about all process memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AllocatorFreePageReclaim {
    /// The existing positively identified jemalloc path, including its observations.
    Jemalloc(AllocatorPurgeReadout),
    /// Only the glibc heap was trimmed. This does not identify Rust's global allocator.
    GlibcHeap { released_pages: bool },
    /// No supported free-page operation was invoked on this platform.
    Unsupported,
}

/// Attempt to release already-free pages without changing allocator policy.
///
/// A positive existing jemalloc probe selects the existing purge API. Otherwise,
/// Linux GNU invokes `malloc_trim(0)` on the glibc heap only. A negative probe can
/// result from concurrent frees; it does **not** establish that System is global.
/// Other platforms return `Unsupported` on that path. No live buffer is freed,
/// no allocator or decay/mmap policy is changed, and no RSS reduction is promised.
///
/// glibc documents `malloc_trim` as MT-safe, returning 1 for released memory and
/// 0 for no release, with no errors. A zero result is successful, not an error.
/// See <https://www.man7.org/linux/man-pages/man3/malloc_trim.3.html>.
/// Jemalloc observation or purge errors are returned unchanged, without fallback.
pub fn reclaim_allocator_free_pages() -> std::io::Result<AllocatorFreePageReclaim> {
    reclaim_with(
        super::jemalloc_is_global_allocator(),
        super::purge_allocator_dirty_pages,
        trim_glibc_heap,
    )
}

fn reclaim_with(
    positive_jemalloc: bool,
    purge: impl FnOnce() -> std::io::Result<Option<AllocatorPurgeReadout>>,
    trim: impl FnOnce() -> Option<bool>,
) -> std::io::Result<AllocatorFreePageReclaim> {
    if positive_jemalloc {
        return purge()?
            .map(AllocatorFreePageReclaim::Jemalloc)
            .ok_or_else(|| std::io::Error::other("identified jemalloc purge unavailable"));
    }
    Ok(match trim() {
        Some(released_pages) => AllocatorFreePageReclaim::GlibcHeap { released_pages },
        None => AllocatorFreePageReclaim::Unsupported,
    })
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn trim_glibc_heap() -> Option<bool> {
    // SAFETY: malloc_trim has no pointer preconditions and is documented MT-safe.
    // pad=0 releases only free pages; glibc maintains all still-live allocations.
    Some(unsafe { libc::malloc_trim(0) } != 0)
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn trim_glibc_heap() -> Option<bool> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AllocatorMemoryReadout;

    fn sample() -> AllocatorPurgeReadout {
        let readout = AllocatorMemoryReadout {
            allocated_bytes: 1,
            active_bytes: 2,
            resident_bytes: 3,
            cgroup_memory_current_bytes: None,
        };
        AllocatorPurgeReadout {
            before: readout,
            after: readout,
        }
    }

    #[test]
    fn positive_jemalloc_uses_existing_observations_without_glibc() {
        let result = reclaim_with(true, || Ok(Some(sample())), || panic!("wrong backend"));
        assert_eq!(
            result.unwrap(),
            AllocatorFreePageReclaim::Jemalloc(sample())
        );
    }

    #[test]
    fn jemalloc_error_is_preserved_without_fallback() {
        let error = reclaim_with(
            true,
            || Err(std::io::Error::from_raw_os_error(5)),
            || panic!("must not fall back after jemalloc error"),
        )
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(5));
    }

    #[test]
    fn absent_positive_jemalloc_readout_is_not_invented() {
        let error = reclaim_with(true, || Ok(None), || panic!("wrong backend")).unwrap_err();
        assert!(error.to_string().contains("purge unavailable"));
    }

    #[test]
    fn glibc_zero_is_success_without_allocator_statistics() {
        let result = reclaim_with(false, || panic!("unidentified jemalloc"), || Some(false));
        assert_eq!(
            result.unwrap(),
            AllocatorFreePageReclaim::GlibcHeap {
                released_pages: false
            }
        );
    }

    #[test]
    fn glibc_release_is_reported_only_as_glibc_heap() {
        let result = reclaim_with(false, || panic!("unidentified jemalloc"), || Some(true));
        assert_eq!(
            result.unwrap(),
            AllocatorFreePageReclaim::GlibcHeap {
                released_pages: true
            }
        );
    }

    #[test]
    fn unsupported_is_an_explicit_noop() {
        let result = reclaim_with(false, || panic!("unidentified jemalloc"), || None);
        assert_eq!(result.unwrap(), AllocatorFreePageReclaim::Unsupported);
    }
}
