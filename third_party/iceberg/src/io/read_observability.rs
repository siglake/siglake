// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use futures::future::{BoxFuture, FutureExt, WeakShared};

use crate::{Error, ErrorKind, Result};

static OBJECT_STORE_BYTES_READ: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub(crate) fn object_store_bytes_read() -> u64 {
    OBJECT_STORE_BYTES_READ.load(std::sync::atomic::Ordering::Relaxed)
}

pub(crate) fn reset_object_store_bytes_read() -> u64 {
    OBJECT_STORE_BYTES_READ.swap(0, std::sync::atomic::Ordering::Relaxed)
}

/// What an object-store read was for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectStoreReadPhase {
    /// Iceberg manifest and manifest-list reads.
    Manifest,
    /// Parquet and Puffin footer metadata.
    Footer,
    /// Page, offset, column, and application index bytes.
    Index,
    /// Parquet column data pages.
    Data,
    /// Reads without a more specific classification.
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

pub(crate) fn record_object_store_reads(phase: ObjectStoreReadPhase, reads: usize, bytes: u64) {
    OBJECT_STORE_BYTES_READ.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    metrics::counter!("siglake_object_store_reads_total", "phase" => phase.label())
        .increment(reads as u64);
    metrics::counter!("siglake_object_store_read_bytes_total", "phase" => phase.label())
        .increment(bytes);
}

type WeakSharedResult<T> = WeakShared<BoxFuture<'static, Arc<std::result::Result<T, String>>>>;

/// Shares one immutable read among concurrent callers. The map holds only weak
/// futures, so completion, failure, or cancellation releases ownership and a
/// later caller can retry.
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
                metrics::counter!("siglake_object_store_debounced_total", "seam" => seam)
                    .increment(1);
                existing
            } else {
                let shared = async move { Arc::new(op().await.map_err(|error| error.to_string())) }
                    .boxed()
                    .shared();
                if let Some(weak) = shared.downgrade() {
                    inflight.insert(key, weak);
                }
                inflight.retain(|_, future| future.upgrade().is_some());
                shared
            }
        };

        match shared.await.as_ref() {
            Ok(value) => Ok(value.clone()),
            Err(message) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("debounced {seam} read failed: {message}"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::future;

    use super::ReadDebouncer;
    use crate::{Error, ErrorKind};

    #[tokio::test]
    async fn concurrent_identical_misses_run_one_population() {
        let debouncer = Arc::new(ReadDebouncer::<String, usize>::default());
        let populations = Arc::new(AtomicUsize::new(0));
        let run = |debouncer: Arc<ReadDebouncer<String, usize>>, populations: Arc<AtomicUsize>| async move {
            debouncer
                .run("same".to_string(), "test", move || async move {
                    populations.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                    Ok(17)
                })
                .await
        };
        let (left, right) = tokio::join!(
            run(Arc::clone(&debouncer), Arc::clone(&populations)),
            run(debouncer, Arc::clone(&populations)),
        );
        assert_eq!(left.unwrap(), 17);
        assert_eq!(right.unwrap(), 17);
        assert_eq!(populations.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn error_and_cancellation_release_ownership_for_retry() {
        let debouncer = ReadDebouncer::<String, usize>::default();
        let failed = debouncer
            .run("error".to_string(), "test", || async {
                Err(Error::new(ErrorKind::Unexpected, "injected"))
            })
            .await;
        assert!(failed.is_err());
        assert_eq!(
            debouncer
                .run("error".to_string(), "test", || async { Ok(23) })
                .await
                .unwrap(),
            23
        );

        {
            let cancelled = std::pin::pin!(debouncer.run("cancel".to_string(), "test", || async {
                future::pending::<crate::Result<usize>>().await
            }));
            assert!(futures::poll!(cancelled).is_pending());
        }
        assert_eq!(
            debouncer
                .run("cancel".to_string(), "test", || async { Ok(29) })
                .await
                .unwrap(),
            29
        );
    }
}
