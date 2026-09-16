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

//! OpenDAL-based storage implementation for Apache Iceberg.
//!
//! This crate provides [`OpenDalStorage`] and [`OpenDalStorageFactory`],
//! which implement the [`Storage`](iceberg::io::Storage) and
//! [`StorageFactory`](iceberg::io::StorageFactory) traits from the `iceberg` crate
//! using [OpenDAL](https://opendal.apache.org/) as the backend.

mod utils;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cfg_if::cfg_if;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::layers::RetryLayer;
use serde::{Deserialize, Serialize};
use utils::from_opendal_error;

cfg_if! {
    if #[cfg(feature = "opendal-azdls")] {
        mod azdls;
        use azdls::AzureStorageScheme;
        use azdls::*;
        use opendal::services::AzdlsConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-fs")] {
        mod fs;
        use fs::*;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-gcs")] {
        mod gcs;
        use gcs::*;
        use opendal::services::GcsConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-memory")] {
        mod memory;
        use memory::*;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-oss")] {
        mod oss;
        use opendal::services::OssConfig;
        use oss::*;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-s3")] {
        mod s3;
        use opendal::services::S3Config;
        pub use s3::*;
    }
}

/// OpenDAL-based storage factory.
///
/// Maps scheme to the corresponding OpenDalStorage storage variant.
/// Use this factory with `FileIOBuilder::new(factory)` to create FileIO instances.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpenDalStorageFactory {
    /// Memory storage factory.
    #[cfg(feature = "opendal-memory")]
    Memory,
    /// Local filesystem storage factory.
    #[cfg(feature = "opendal-fs")]
    Fs,
    /// S3 storage factory.
    #[cfg(feature = "opendal-s3")]
    S3 {
        /// s3 storage could have `s3://` and `s3a://`.
        /// Storing the scheme string here to return the correct path.
        configured_scheme: String,
        /// Custom AWS credential loader.
        #[serde(skip)]
        customized_credential_load: Option<s3::CustomAwsCredentialLoader>,
    },
    /// GCS storage factory.
    #[cfg(feature = "opendal-gcs")]
    Gcs,
    /// OSS storage factory.
    #[cfg(feature = "opendal-oss")]
    Oss,
    /// Azure Data Lake Storage factory.
    #[cfg(feature = "opendal-azdls")]
    Azdls {
        /// The configured Azure storage scheme.
        configured_scheme: AzureStorageScheme,
    },
}

#[typetag::serde(name = "OpenDalStorageFactory")]
impl StorageFactory for OpenDalStorageFactory {
    #[allow(unused_variables)]
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorageFactory::Memory => {
                Ok(Arc::new(OpenDalStorage::Memory(memory_config_build()?)))
            }
            #[cfg(feature = "opendal-fs")]
            OpenDalStorageFactory::Fs => Ok(Arc::new(OpenDalStorage::LocalFs)),
            #[cfg(feature = "opendal-s3")]
            OpenDalStorageFactory::S3 {
                configured_scheme,
                customized_credential_load,
            } => Ok(Arc::new(OpenDalStorage::S3 {
                configured_scheme: configured_scheme.clone(),
                config: s3_config_parse(config.props().clone())?.into(),
                customized_credential_load: customized_credential_load.clone(),
            })),
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorageFactory::Gcs => Ok(Arc::new(OpenDalStorage::Gcs {
                config: gcs_config_parse(config.props().clone())?.into(),
            })),
            #[cfg(feature = "opendal-oss")]
            OpenDalStorageFactory::Oss => Ok(Arc::new(OpenDalStorage::Oss {
                config: oss_config_parse(config.props().clone())?.into(),
            })),
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorageFactory::Azdls { configured_scheme } => {
                Ok(Arc::new(OpenDalStorage::Azdls {
                    configured_scheme: configured_scheme.clone(),
                    config: azdls_config_parse(config.props().clone())?.into(),
                }))
            }
            #[cfg(all(
                not(feature = "opendal-memory"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-s3"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
            ))]
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "No storage service has been enabled",
            )),
        }
    }
}

/// Default memory operator for serde deserialization.
#[cfg(feature = "opendal-memory")]
fn default_memory_operator() -> Operator {
    memory_config_build().expect("Failed to create default memory operator")
}

/// OpenDAL-based storage implementation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpenDalStorage {
    /// Memory storage variant.
    #[cfg(feature = "opendal-memory")]
    Memory(#[serde(skip, default = "self::default_memory_operator")] Operator),
    /// Local filesystem storage variant.
    #[cfg(feature = "opendal-fs")]
    LocalFs,
    /// S3 storage variant.
    #[cfg(feature = "opendal-s3")]
    S3 {
        /// s3 storage could have `s3://` and `s3a://`.
        /// Storing the scheme string here to return the correct path.
        configured_scheme: String,
        /// S3 configuration.
        config: Arc<S3Config>,
        /// Custom AWS credential loader.
        #[serde(skip)]
        customized_credential_load: Option<s3::CustomAwsCredentialLoader>,
    },
    /// GCS storage variant.
    #[cfg(feature = "opendal-gcs")]
    Gcs {
        /// GCS configuration.
        config: Arc<GcsConfig>,
    },
    /// OSS storage variant.
    #[cfg(feature = "opendal-oss")]
    Oss {
        /// OSS configuration.
        config: Arc<OssConfig>,
    },
    /// Azure Data Lake Storage variant.
    /// Expects paths of the form
    /// `abfs[s]://<filesystem>@<account>.dfs.<endpoint-suffix>/<path>` or
    /// `wasb[s]://<container>@<account>.blob.<endpoint-suffix>/<path>`.
    #[cfg(feature = "opendal-azdls")]
    #[allow(private_interfaces)]
    Azdls {
        /// The configured Azure storage scheme.
        /// Because Azdls accepts multiple possible schemes, we store the full
        /// passed scheme here to later validate schemes passed via paths.
        configured_scheme: AzureStorageScheme,
        /// Azure DLS configuration.
        config: Arc<AzdlsConfig>,
    },
}

impl OpenDalStorage {
    /// Creates operator from path.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`](iceberg::io::FileIO).
    ///
    /// # Returns
    ///
    /// The return value consists of two parts:
    ///
    /// * An [`opendal::Operator`] instance used to operate on file.
    /// * Relative path to the root uri of [`opendal::Operator`].
    #[allow(unreachable_code, unused_variables)]
    pub(crate) fn create_operator<'a>(
        &self,
        path: &'a impl AsRef<str>,
    ) -> Result<(Operator, &'a str)> {
        let path = path.as_ref();
        let (operator, relative_path): (Operator, &str) = match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorage::Memory(op) => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    (op.clone(), stripped)
                } else {
                    (op.clone(), &path[1..])
                }
            }
            #[cfg(feature = "opendal-fs")]
            OpenDalStorage::LocalFs => {
                let op = fs_config_build()?;
                if let Some(stripped) = path.strip_prefix("file:/") {
                    (op, stripped)
                } else {
                    (op, &path[1..])
                }
            }
            #[cfg(feature = "opendal-s3")]
            OpenDalStorage::S3 {
                configured_scheme,
                config,
                customized_credential_load,
            } => {
                let op = s3_config_build(config, customized_credential_load, path)?;
                let op_info = op.info();

                // Check prefix of s3 path.
                let prefix = format!("{}://{}/", configured_scheme, op_info.name());
                if path.starts_with(&prefix) {
                    (op, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorage::Gcs { config } => {
                let operator = gcs_config_build(config, path)?;
                let prefix = format!("gs://{}/", operator.info().name());
                if path.starts_with(&prefix) {
                    (operator, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-oss")]
            OpenDalStorage::Oss { config } => {
                let op = oss_config_build(config, path)?;
                let prefix = format!("oss://{}/", op.info().name());
                if path.starts_with(&prefix) {
                    (op, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorage::Azdls {
                configured_scheme,
                config,
            } => azdls_create_operator(path, config, configured_scheme)?,
            #[cfg(all(
                not(feature = "opendal-s3"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
            ))]
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "No storage service has been enabled",
                ));
            }
        };

        // Transient errors are common for object stores; however there's no
        // harm in retrying temporary failures for other storage backends as well.
        // siglake fork: jittered backoff.
        //
        // `RetryLayer::new()` takes backon's defaults: min_delay 1s, factor 2.0,
        // max_times 3, and **jitter false**. Without jitter every concurrent
        // retrier sleeps the same 1s/2s/4s and collides again in lockstep — the
        // textbook thundering herd, and siglake runs many concurrent S3 users
        // (drain workers, compaction bins, query scans).
        //
        // Deliberately NOT claimed as the fix for anything measured. The
        // 2026-08-08 round's load_table cost (9.65 s/call) was ~240x its
        // early-round value, and I theorised this ladder explained it; the logs
        // show 47 retries, all at the FIRST rung, i.e. ~0.5% of the time to
        // explain. This is a robustness fix on its own merits, not a
        // throughput one.
        let operator = operator.layer(
            RetryLayer::new()
                .with_jitter()
                .with_min_delay(std::time::Duration::from_millis(100))
                .with_max_times(5),
        );
        Ok((operator, relative_path))
    }
}

#[typetag::serde(name = "OpenDalStorage")]
#[async_trait]
impl Storage for OpenDalStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(op.exists(relative_path).await.map_err(from_opendal_error)?)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (op, relative_path) = self.create_operator(&path)?;
        let meta = op.stat(relative_path).await.map_err(from_opendal_error)?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(op
            .read(relative_path)
            .await
            .map_err(from_opendal_error)?
            .to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(Box::new(OpenDalReader(
            op.reader(relative_path).await.map_err(from_opendal_error)?,
        )))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (op, relative_path) = self.create_operator(&path)?;
        op.write(relative_path, bs)
            .await
            .map_err(from_opendal_error)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (op, relative_path) = self.create_operator(&path)?;
        // siglake fork: multipart upload concurrency (cross-review F10).
        //
        // Upstream is `op.writer(relative_path)`, and opendal's `WriteOptions`
        // derives Default with `concurrent: 0`, which its `MultipartWrite`
        // passes straight into `ConcurrentTasks::new(executor, concurrent, ..)`
        // — so every Parquet file siglake writes to object storage sends its
        // parts ONE AT A TIME. The 2026-08-08 1TB round measured flush (S3 PUT)
        // at 30.2% of append time, the largest single stage, with files of
        // ~534 MB going up in ~133 MB parts sequentially.
        //
        // Default 0 preserves upstream behaviour exactly; the knob exists so a
        // round can isolate the change instead of confounding it with whatever
        // else shipped in the same image.
        let concurrent = multipart_concurrency();
        let chunk = multipart_chunk_bytes();
        // EFFECTIVE CONFIG, published so a round can prove the setting applied.
        // Twice this session a benchmark measured a knob that was never in
        // effect: the whole catalog-drain tuning matrix varied filesystem-path
        // settings, and a --role drain round silently ran as `combined`. A gauge
        // nothing checks is decoration, so `bench/validate_round.py` gates on
        // these.
        metrics::gauge!("siglake_object_store_write_concurrency").set(concurrent as f64);
        metrics::gauge!("siglake_object_store_write_chunk_bytes")
            .set(chunk.unwrap_or(0) as f64);
        let w = if concurrent > 0 {
            // Memory per open writer is roughly `concurrent x chunk`, so the
            // two knobs must move together. opendal's S3 default part is
            // ~128 MiB (the 2026-08-08 round shows ~133 MB parts), so
            // concurrent=4 at that size would be ~532 MB per writer — and a
            // compactor has `bin_concurrency` merges open, each fanning out one
            // partition writer per day via try_join_all. Setting a SMALLER
            // chunk alongside a higher concurrency buys parallelism at the same
            // memory: 32 MiB x 4 is the same 128 MB, four ways.
            let mut b = op.writer_with(relative_path).concurrent(concurrent);
            if let Some(chunk) = chunk {
                b = b.chunk(chunk);
            }
            b.await.map_err(from_opendal_error)?
        } else {
            op.writer(relative_path).await.map_err(from_opendal_error)?
        };
        // Hold a class permit for the writer's LIFETIME, not just its creation:
        // the cost being shared is the upload itself, which happens on the
        // returned writer. Absent a task-local (the drain path and every
        // non-compaction caller) this is the Drain class.
        let class = UPLOAD_CLASS.try_with(|c| *c).unwrap_or(UploadClass::Drain);
        let permit = upload_permits(class)
            .acquire()
            .await
            .expect("upload semaphore is never closed");
        metrics::counter!("siglake_object_store_writer_opened_total",
            "class" => match class { UploadClass::Drain => "drain", UploadClass::Compaction => "compaction" })
        .increment(1);
        permit.forget();
        Ok(Box::new(OpenDalWriter {
            writer: w,
            _permit: ClassPermit(class),
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(op.delete(relative_path).await.map_err(from_opendal_error)?)
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(&path)?;
        let path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        Ok(op.remove_all(&path).await.map_err(from_opendal_error)?)
    }

    #[allow(unreachable_code, unused_variables)]
    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    #[allow(unreachable_code, unused_variables)]
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

// Newtype wrappers for opendal types to satisfy orphan rules.
// We can't implement iceberg's FileRead/FileWrite traits directly on opendal's
// Reader/Writer since neither trait nor type is defined in this crate.

/// Wrapper around `opendal::Reader` that implements `FileRead`.
pub(crate) struct OpenDalReader(pub(crate) opendal::Reader);

#[async_trait]
impl FileRead for OpenDalReader {
    async fn read(&self, range: std::ops::Range<u64>) -> Result<Bytes> {
        Ok(opendal::Reader::read(&self.0, range)
            .await
            .map_err(from_opendal_error)?
            .to_bytes())
    }
}

/// Wrapper around `opendal::Writer` that implements `FileWrite`.
/// Concurrent multipart part-uploads per file, from
/// `SIGLAKE_OBJECT_STORE_WRITE_CONCURRENCY`. `0` (default) keeps upstream's
/// sequential behaviour. Each in-flight part buffers its chunk, so raising this
/// raises writer memory by roughly `concurrent x chunk_size` per open file —
/// and a compactor already has `bin_concurrency` files open.
/// Which workload an upload belongs to, so the two cannot starve each other.
///
/// Quickwit hit this and fixed it the same way (#6376 separated index-upload and
/// merge-upload semaphores). siglake's 2026-08-15 1TB round shows the symptom
/// from the other side: 52 recluster watchdog trips, all of them DURING ingest,
/// on nodes running `--role combined`. A compaction bin that takes ~5 minutes
/// standalone must go 30 minutes without committing to trip a progress-aware
/// watchdog, and drain uploads monopolising the write path is the mechanism that
/// fits.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum UploadClass {
    /// WAL -> Iceberg commits. Latency-sensitive: it gates queryability.
    Drain,
    /// Compaction rewrites. Throughput-oriented and interruptible.
    Compaction,
}

tokio::task_local! {
    /// Set around compaction work; absent means the drain path.
    pub static UPLOAD_CLASS: UploadClass;
}

/// Run `fut` with every object-store upload inside it accounted to `class`.
pub async fn with_upload_class<F: std::future::Future>(class: UploadClass, fut: F) -> F::Output {
    UPLOAD_CLASS.scope(class, fut).await
}

/// Permits currently free for `class`. Public so the class pools are testable
/// and observable -- a pool silently draining to zero would throttle that
/// workload for the process's life with no other symptom.
pub fn available_upload_permits(class: UploadClass) -> usize {
    upload_permits(class).available_permits()
}

fn upload_permits(class: UploadClass) -> &'static tokio::sync::Semaphore {
    use std::sync::OnceLock;
    static DRAIN: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    static COMPACTION: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    match class {
        // Defaults are deliberately generous: this exists to stop one class
        // STARVING the other, not to throttle either. A too-small cap would
        // reintroduce the serialization it is meant to remove.
        UploadClass::Drain => DRAIN.get_or_init(|| {
            tokio::sync::Semaphore::new(write_permits("SIGLAKE_S3_WRITE_PERMITS_DRAIN", 64))
        }),
        UploadClass::Compaction => COMPACTION.get_or_init(|| {
            tokio::sync::Semaphore::new(write_permits(
                "SIGLAKE_S3_WRITE_PERMITS_COMPACTION",
                32,
            ))
        }),
    }
}

fn multipart_concurrency() -> usize {
    write_concurrency_from(
        std::env::var("SIGLAKE_OBJECT_STORE_WRITE_CONCURRENCY")
            .ok()
            .as_deref(),
    )
}

/// Resolve multipart concurrency from the raw configured value.
///
/// Unlike the permit resolver, `0` is meaningful: it selects opendal's
/// sequential upstream behaviour. Unset and unparseable values also select it.
fn write_concurrency_from(configured: Option<&str>) -> usize {
    configured
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0)
}

/// Multipart part size in bytes, from `SIGLAKE_OBJECT_STORE_WRITE_CHUNK_MB`.
/// `0`/unset keeps opendal's service default (~128 MiB on S3). Only consulted
/// when concurrency is enabled — see the memory note at the call site.
fn multipart_chunk_bytes() -> Option<usize> {
    write_chunk_bytes_from(
        std::env::var("SIGLAKE_OBJECT_STORE_WRITE_CHUNK_MB")
            .ok()
            .as_deref(),
    )
}

/// Resolve an explicit multipart chunk size from its raw MiB value.
fn write_chunk_bytes_from(configured: Option<&str>) -> Option<usize> {
    configured
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&mb| mb > 0)
        .map(|mb| mb * 1024 * 1024)
}

fn write_permits(variable: &str, default: usize) -> usize {
    write_permits_from(std::env::var(variable).ok().as_deref(), default)
}

/// Resolve an upload permit count from its raw configured value.
///
/// Unlike multipart concurrency, `0` is invalid and falls back to `default`:
/// the class pools must never be configured with no permits.
fn write_permits_from(configured: Option<&str>, default: usize) -> usize {
    configured
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&permits| permits > 0)
        .unwrap_or(default)
}

/// Writer plus the class permit it holds for its whole lifetime. The permit is
/// `forget()`-ed on acquire and returned to its semaphore when `_permit` drops.
pub(crate) struct OpenDalWriter {
    writer: opendal::Writer,
    /// Held only for its `Drop` impl; never read, hence the underscore.
    _permit: ClassPermit,
}

/// Returns one permit to `class`'s semaphore when dropped.
pub(crate) struct ClassPermit(UploadClass);

impl Drop for ClassPermit {
    fn drop(&mut self) {
        upload_permits(self.0).add_permits(1);
    }
}

#[async_trait]
impl FileWrite for OpenDalWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        Ok(opendal::Writer::write(&mut self.writer, bs)
            .await
            .map_err(from_opendal_error)?)
    }

    async fn close(&mut self) -> Result<()> {
        let _ = opendal::Writer::close(&mut self.writer)
            .await
            .map_err(from_opendal_error)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_concurrency_parses_configured_value() {
        assert_eq!(write_concurrency_from(None), 0);
        assert_eq!(write_concurrency_from(Some("0")), 0);
        assert_eq!(write_concurrency_from(Some("garbage")), 0);
        assert_eq!(write_concurrency_from(Some("4")), 4);
    }

    #[test]
    fn write_chunk_bytes_parses_positive_mebibytes() {
        assert_eq!(write_chunk_bytes_from(None), None);
        assert_eq!(write_chunk_bytes_from(Some("0")), None);
        assert_eq!(write_chunk_bytes_from(Some("garbage")), None);
        assert_eq!(write_chunk_bytes_from(Some("32")), Some(32 * 1024 * 1024));
    }

    #[test]
    fn write_permits_rejects_zero() {
        assert_eq!(write_permits_from(None, 64), 64);
        assert_eq!(write_permits_from(Some("0"), 64), 64);
        assert_eq!(write_permits_from(Some("garbage"), 64), 64);
        assert_eq!(write_permits_from(Some("8"), 64), 8);
    }

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_default_memory_operator() {
        let op = default_memory_operator();
        assert_eq!(op.info().scheme().to_string(), "memory");
    }
}
