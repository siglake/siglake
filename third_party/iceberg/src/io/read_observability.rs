use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use futures::future::{BoxFuture, FutureExt, WeakShared};

use crate::{Error, ErrorKind, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// What a given object-store read was FOR. Labels both the global
/// `siglake_object_store_read_bytes_total` metric and, since the F-5
/// cross-review finding, the per-request byte classes in `stats.scan` — so a
/// cold aggregate can say whether it was footer-bound or column-bound, which
/// have opposite fixes.
pub enum ObjectStoreReadPhase {
    /// Iceberg manifest / manifest-list reads issued during planning. Counted
    /// separately from the reader's byte total, which covers scanning only.
    Manifest,
    /// Parquet footer + metadata.
    Footer,
    /// Page/offset/column index and siglake's own index blobs.
    Index,
    /// Column-chunk data pages.
    Data,
    /// Unclassified.
    Other,
}

impl ObjectStoreReadPhase {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Manifest => "manifest",
            Self::Footer => "footer",
            Self::Index => "index",
            Self::Data => "data",
            Self::Other => "other",
        }
    }
}

#[inline]
pub(crate) fn record_object_store_read(phase: ObjectStoreReadPhase, bytes: u64) {
    record_object_store_reads(phase, 1, bytes);
}

#[inline]
pub(crate) fn record_object_store_reads(
    phase: ObjectStoreReadPhase,
    reads: usize,
    bytes: u64,
) {
    metrics::counter!(
        "siglake_object_store_reads_total",
        "phase" => phase.label()
    )
    .increment(reads as u64);
    metrics::counter!(
        "siglake_object_store_read_bytes_total",
        "phase" => phase.label()
    )
    .increment(bytes);
}

type WeakSharedResult<T> = WeakShared<BoxFuture<'static, Arc<std::result::Result<T, String>>>>;

#[derive(Debug)]
pub(crate) struct ReadDebouncer<K, T> {
    inflight: Mutex<HashMap<K, WeakSharedResult<T>>>,
}

impl<K, T> Default for ReadDebouncer<K, T> {
    fn default() -> Self {
        Self {
            inflight: Mutex::new(HashMap::new()),
        }
    }
}

impl<K, T> ReadDebouncer<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone + Send + Sync + 'static,
{
    pub(crate) async fn run<F, Fut>(&self, key: K, seam: &'static str, op: F) -> Result<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let shared = {
            let mut inflight = self.inflight.lock().unwrap();
            if let Some(existing) = inflight.get(&key).and_then(WeakShared::upgrade) {
                metrics::counter!(
                    "siglake_object_store_debounced_total",
                    "seam" => seam
                )
                .increment(1);
                existing
            } else {
                let shared = async move { Arc::new(op().await.map_err(|err| err.to_string())) }
                    .boxed()
                    .shared();
                if let Some(weak) = shared.downgrade() {
                    inflight.insert(key, weak);
                }
                inflight.retain(|_, future| future.upgrade().is_some());
                shared
            }
        };

        let result = shared.await;
        match result.as_ref() {
            Ok(value) => Ok(value.clone()),
            Err(msg) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("debounced {seam} read failed: {msg}"),
            )),
        }
    }
}
