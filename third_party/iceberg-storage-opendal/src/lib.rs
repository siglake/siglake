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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cfg_if::cfg_if;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::layers::{RetryLayer, TimeoutLayer};
use serde::{Deserialize, Serialize};
use utils::from_opendal_error;

cfg_if! {
    if #[cfg(feature = "opendal-azdls")] {
        mod azdls;
        use azdls::*;
        use opendal::services::AzdlsConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-hf")] {
        mod hf;
        use hf::*;
        use opendal::services::HfConfig;
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

mod resolving;
pub use resolving::{OpenDalResolvingStorage, OpenDalResolvingStorageFactory};

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
    Azdls,
    /// HuggingFace Hub storage factory.
    #[cfg(feature = "opendal-hf")]
    Hf,
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
                customized_credential_load,
            } => Ok(Arc::new(OpenDalStorage::S3 {
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
            OpenDalStorageFactory::Azdls => Ok(Arc::new(OpenDalStorage::Azdls {
                config: azdls_config_parse(config.props().clone())?.into(),
            })),
            #[cfg(feature = "opendal-hf")]
            OpenDalStorageFactory::Hf => Ok(Arc::new(OpenDalStorage::Hf {
                config: hf_config_parse(config.props().clone())?.into(),
            })),
            #[cfg(all(
                not(feature = "opendal-memory"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-s3"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-hf"),
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
    ///
    /// Accepts any S3-family URL (`s3://`, `s3a://`, `s3n://`); the scheme is
    /// derived from the path at call time.
    #[cfg(feature = "opendal-s3")]
    S3 {
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
    ///
    /// Accepts paths of the form
    /// `abfs[s]://<filesystem>@<account>.dfs.<endpoint-suffix>/<path>` or
    /// `wasb[s]://<container>@<account>.blob.<endpoint-suffix>/<path>`.
    /// The scheme is derived from the path at call time.
    #[cfg(feature = "opendal-azdls")]
    Azdls {
        /// Azure DLS configuration.
        config: Arc<AzdlsConfig>,
    },
    /// HuggingFace Hub storage variant.
    ///
    /// Accepts paths of the form
    /// `hf://<repo_type>/<owner>/<repo>[@<revision>]/<path_in_repo>`,
    /// where `<repo_type>` must be one of `models`, `datasets`, `spaces`, or `buckets`.
    #[cfg(feature = "opendal-hf")]
    Hf {
        /// HuggingFace Hub configuration (token + endpoint).
        config: Arc<HfConfig>,
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
                config,
                customized_credential_load,
            } => {
                let op = s3_config_build(config, customized_credential_load, path)?;
                let op_info = op.info();

                // Use the URL scheme in the path for prefix matching. This enables
                // use of S3-compatible storage backends using custom schemes (e.g., `minio://`, `r2://`).
                let url = url::Url::parse(path).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}: {e}"),
                    )
                })?;
                let prefix = format!("{}://{}/", url.scheme(), op_info.name());
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
            OpenDalStorage::Azdls { config } => azdls_create_operator(path, config)?,
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { config } => hf_config_build(config, path)?,
            #[cfg(all(
                not(feature = "opendal-s3"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-hf"),
            ))]
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "No storage service has been enabled",
                ));
            }
        };

        // Apply observability/resilience layers. TimeoutLayer must be
        // inside RetryLayer so each retry attempt is independently
        // bounded — without a per-attempt timeout, a future parked on a
        // silently dropped TCP connection never produces an `Err` and
        // RetryLayer cannot retry, leaving the caller hung indefinitely.
        // See: https://opendal.apache.org/docs/rust/opendal/layers/struct.TimeoutLayer.html
        //
        // Transient errors are common for object stores; we retry temporary
        // failures with exponential backoff. The retry behavior also
        // benefits non-object-store backends.
        let operator = operator.layer(TimeoutLayer::new()).layer(
            RetryLayer::new()
                .with_jitter()
                .with_min_delay(std::time::Duration::from_millis(100))
                .with_max_times(5),
        );
        Ok((operator, relative_path))
    }

    /// Returns a cache key used by `delete_stream` to group paths by storage operator.
    ///
    /// For most backends the URL host (bucket name) is sufficient. For HF the host
    /// encodes the repo type, not the repo identity, so a more specific key is used.
    fn batch_key_for_path(&self, path: &str) -> String {
        match self {
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { .. } => hf_batch_key(path),
            _ => url::Url::parse(path)
                .ok()
                .and_then(|u| u.host_str().map(|s| s.to_string()))
                .unwrap_or_default(),
        }
    }

    /// Extracts the relative path from an absolute path without building an operator.
    ///
    /// This is a lightweight alternative to [`create_operator`](Self::create_operator) for cases
    /// where only the relative path is needed (e.g. bulk deletes where the operator is already
    /// available).
    #[allow(unreachable_code, unused_variables)]
    pub(crate) fn relativize_path<'a>(&self, path: &'a str) -> Result<&'a str> {
        match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorage::Memory(_) => Ok(path.strip_prefix("memory:/").unwrap_or(&path[1..])),
            #[cfg(feature = "opendal-fs")]
            OpenDalStorage::LocalFs => Ok(path.strip_prefix("file:/").unwrap_or(&path[1..])),
            #[cfg(feature = "opendal-s3")]
            OpenDalStorage::S3 { .. } => {
                let url = url::Url::parse(path)?;
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, missing bucket"),
                    )
                })?;
                let prefix = format!("{}://{}/", url.scheme(), bucket);
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorage::Gcs { .. } => {
                let url = url::Url::parse(path)?;
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, missing bucket"),
                    )
                })?;
                let prefix = format!("gs://{}/", bucket);
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-oss")]
            OpenDalStorage::Oss { .. } => {
                let url = url::Url::parse(path)?;
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, missing bucket"),
                    )
                })?;
                let prefix = format!("oss://{}/", bucket);
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorage::Azdls { config } => {
                let azure_path = path.parse::<AzureStoragePath>()?;
                match_path_with_config(&azure_path, config)?;
                let relative_path_len = azure_path.path.len();
                Ok(&path[path.len() - relative_path_len..])
            }
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { .. } => {
                let parsed = hf::HfUri::parse(path).ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, format!("Invalid hf url: {path}"))
                })?;
                Ok(&path[path.len() - parsed.path.len()..])
            }
            #[cfg(all(
                not(feature = "opendal-s3"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-hf"),
            ))]
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "No storage service has been enabled",
            )),
        }
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
        let concurrent = multipart_concurrency();
        let chunk = multipart_chunk_bytes();
        metrics::gauge!("siglake_object_store_write_concurrency").set(concurrent as f64);
        metrics::gauge!("siglake_object_store_write_chunk_bytes").set(chunk.unwrap_or(0) as f64);

        let writer = if concurrent > 0 {
            let mut builder = op.writer_with(relative_path).concurrent(concurrent);
            if let Some(chunk) = chunk {
                builder = builder.chunk(chunk);
            }
            builder.await.map_err(from_opendal_error)?
        } else {
            op.writer(relative_path).await.map_err(from_opendal_error)?
        };

        let class = UPLOAD_CLASS.try_with(|class| *class).unwrap_or_default();
        let permit = upload_permits(class)
            .acquire()
            .await
            .expect("upload semaphore is never closed");
        permit.forget();
        metrics::counter!(
            "siglake_object_store_writer_opened_total",
            "class" => class.label()
        )
        .increment(1);
        Ok(Box::new(OpenDalWriter {
            writer,
            permit: Some(ClassPermit(class)),
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
        Ok(op
            .delete_with(&path)
            .recursive(true)
            .await
            .map_err(from_opendal_error)?)
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        let mut deleters: HashMap<String, opendal::Deleter> = HashMap::new();

        while let Some(path) = paths.next().await {
            let bucket = self.batch_key_for_path(&path);

            let (relative_path, deleter) = match deleters.entry(bucket) {
                Entry::Occupied(entry) => {
                    (self.relativize_path(&path)?.to_string(), entry.into_mut())
                }
                Entry::Vacant(entry) => {
                    let (op, rel) = self.create_operator(&path)?;
                    let rel = rel.to_string();
                    let deleter = op.deleter().await.map_err(from_opendal_error)?;
                    (rel, entry.insert(deleter))
                }
            };

            deleter
                .delete(relative_path)
                .await
                .map_err(from_opendal_error)?;
        }

        for (_, mut deleter) in deleters {
            deleter.close().await.map_err(from_opendal_error)?;
        }

        Ok(())
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

/// Workload class for independently bounded object-store uploads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UploadClass {
    /// WAL-to-Iceberg commits and every caller outside a compaction scope.
    #[default]
    Drain,
    /// Compaction rewrites.
    Compaction,
}

impl UploadClass {
    fn label(self) -> &'static str {
        match self {
            Self::Drain => "drain",
            Self::Compaction => "compaction",
        }
    }
}

tokio::task_local! {
    static UPLOAD_CLASS: UploadClass;
}

/// Account every object-store writer opened by `future` to `class`.
pub async fn with_upload_class<F: std::future::Future>(class: UploadClass, future: F) -> F::Output {
    UPLOAD_CLASS.scope(class, future).await
}

/// Return the number of immediately available permits for `class`.
pub fn available_upload_permits(class: UploadClass) -> usize {
    upload_permits(class).available_permits()
}

fn upload_permits(class: UploadClass) -> &'static tokio::sync::Semaphore {
    use std::sync::OnceLock;

    static DRAIN: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    static COMPACTION: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    match class {
        UploadClass::Drain => DRAIN.get_or_init(|| {
            tokio::sync::Semaphore::new(write_permits("SIGLAKE_S3_WRITE_PERMITS_DRAIN", 64))
        }),
        UploadClass::Compaction => COMPACTION.get_or_init(|| {
            tokio::sync::Semaphore::new(write_permits("SIGLAKE_S3_WRITE_PERMITS_COMPACTION", 32))
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

fn write_concurrency_from(configured: Option<&str>) -> usize {
    configured
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0)
}

fn multipart_chunk_bytes() -> Option<usize> {
    write_chunk_bytes_from(
        std::env::var("SIGLAKE_OBJECT_STORE_WRITE_CHUNK_MB")
            .ok()
            .as_deref(),
    )
}

fn write_chunk_bytes_from(configured: Option<&str>) -> Option<usize> {
    configured
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|mb| *mb > 0)
        .and_then(|mb| mb.checked_mul(1024 * 1024))
}

fn write_permits(variable: &str, default: usize) -> usize {
    write_permits_from(std::env::var(variable).ok().as_deref(), default)
}

fn write_permits_from(configured: Option<&str>, default: usize) -> usize {
    configured
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|permits| *permits > 0)
        .unwrap_or(default)
}

struct ClassPermit(UploadClass);

impl Drop for ClassPermit {
    fn drop(&mut self) {
        upload_permits(self.0).add_permits(1);
    }
}

/// Wrapper around `opendal::Writer` that holds one class permit until close,
/// failure, cancellation, or drop.
pub(crate) struct OpenDalWriter {
    writer: opendal::Writer,
    permit: Option<ClassPermit>,
}

impl OpenDalWriter {
    fn release_on_error<T>(&mut self, result: Result<T>) -> Result<T> {
        if result.is_err() {
            self.permit.take();
        }
        result
    }
}

#[async_trait]
impl FileWrite for OpenDalWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let result = opendal::Writer::write(&mut self.writer, bs)
            .await
            .map_err(from_opendal_error);
        self.release_on_error(result)
    }

    async fn close(&mut self) -> Result<()> {
        let result = opendal::Writer::close(&mut self.writer)
            .await
            .map(|_| ())
            .map_err(from_opendal_error);
        self.permit.take();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    #[test]
    fn upload_settings_keep_production_defaults_and_reject_invalid_values() {
        assert_eq!(write_concurrency_from(None), 0);
        assert_eq!(write_concurrency_from(Some("0")), 0);
        assert_eq!(write_concurrency_from(Some("invalid")), 0);
        assert_eq!(write_concurrency_from(Some("4")), 4);

        assert_eq!(write_chunk_bytes_from(None), None);
        assert_eq!(write_chunk_bytes_from(Some("0")), None);
        assert_eq!(write_chunk_bytes_from(Some("invalid")), None);
        assert_eq!(write_chunk_bytes_from(Some("32")), Some(32 * 1024 * 1024));
        assert_eq!(write_chunk_bytes_from(Some(&usize::MAX.to_string())), None);

        assert_eq!(write_permits_from(None, 64), 64);
        assert_eq!(write_permits_from(Some("0"), 64), 64);
        assert_eq!(write_permits_from(Some("invalid"), 64), 64);
        assert_eq!(write_permits_from(Some("8"), 64), 8);
    }

    #[cfg(feature = "opendal-memory")]
    #[tokio::test]
    async fn writers_publish_metrics_and_release_class_permits() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().expect("install debugging recorder");

        let storage = OpenDalStorage::Memory(default_memory_operator());
        let drain_before = available_upload_permits(UploadClass::Drain);
        let compaction_before = available_upload_permits(UploadClass::Compaction);

        let mut drain = Storage::writer(&storage, "memory:/drain")
            .await
            .expect("open drain writer");
        assert_eq!(
            available_upload_permits(UploadClass::Drain),
            drain_before - 1
        );
        assert_eq!(
            available_upload_permits(UploadClass::Compaction),
            compaction_before
        );
        drain.close().await.expect("close drain writer");
        assert_eq!(available_upload_permits(UploadClass::Drain), drain_before);

        let compaction = with_upload_class(
            UploadClass::Compaction,
            Storage::writer(&storage, "memory:/compaction"),
        )
        .await
        .expect("open compaction writer");
        assert_eq!(
            available_upload_permits(UploadClass::Compaction),
            compaction_before - 1
        );
        drop(compaction);
        assert_eq!(
            available_upload_permits(UploadClass::Compaction),
            compaction_before
        );

        let permit = upload_permits(UploadClass::Drain)
            .acquire()
            .await
            .expect("take failure fixture permit");
        permit.forget();
        let mut failing = OpenDalWriter {
            writer: default_memory_operator()
                .writer("failure")
                .await
                .expect("open failure fixture writer"),
            permit: Some(ClassPermit(UploadClass::Drain)),
        };
        assert_eq!(
            available_upload_permits(UploadClass::Drain),
            drain_before - 1
        );
        let failure: Result<()> = Err(Error::new(ErrorKind::Unexpected, "fixture failure"));
        assert!(failing.release_on_error(failure).is_err());
        assert_eq!(
            available_upload_permits(UploadClass::Drain),
            drain_before,
            "writer errors must release their permit"
        );

        let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();
        let cancelled_storage = storage.clone();
        let cancelled = tokio::spawn(async move {
            with_upload_class(UploadClass::Compaction, async move {
                let writer = Storage::writer(&cancelled_storage, "memory:/cancelled-open")
                    .await
                    .expect("open cancellation fixture writer");
                opened_tx.send(()).expect("signal open writer");
                std::future::pending::<()>().await;
                drop(writer);
            })
            .await;
        });
        opened_rx.await.expect("writer opened");
        assert_eq!(
            available_upload_permits(UploadClass::Compaction),
            compaction_before - 1
        );
        cancelled.abort();
        let _ = cancelled.await;
        assert_eq!(
            available_upload_permits(UploadClass::Compaction),
            compaction_before,
            "cancelling a task that owns a writer must release its permit"
        );

        let held = upload_permits(UploadClass::Compaction)
            .acquire_many(compaction_before as u32)
            .await
            .expect("hold compaction pool");
        let waiting_storage = storage.clone();
        let waiting = tokio::spawn(async move {
            with_upload_class(
                UploadClass::Compaction,
                Storage::writer(&waiting_storage, "memory:/cancelled"),
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(
            !waiting.is_finished(),
            "writer must wait for its class permit"
        );
        waiting.abort();
        let _ = waiting.await;
        drop(held);
        assert_eq!(
            available_upload_permits(UploadClass::Compaction),
            compaction_before,
            "cancelling a waiting writer must not consume a permit"
        );

        let snapshot = snapshotter.snapshot().into_vec();
        let value = |name: &str, class: Option<&str>| {
            snapshot.iter().find_map(|(key, _, _, value)| {
                let class_matches = class.is_none_or(|expected| {
                    key.key()
                        .labels()
                        .any(|label| label.key() == "class" && label.value() == expected)
                });
                (key.key().name() == name && class_matches).then_some(value)
            })
        };
        assert!(matches!(
            value("siglake_object_store_write_concurrency", None),
            Some(DebugValue::Gauge(_))
        ));
        assert!(matches!(
            value("siglake_object_store_write_chunk_bytes", None),
            Some(DebugValue::Gauge(_))
        ));
        assert_eq!(
            value("siglake_object_store_writer_opened_total", Some("drain")),
            Some(&DebugValue::Counter(1))
        );
        assert_eq!(
            value(
                "siglake_object_store_writer_opened_total",
                Some("compaction")
            ),
            Some(&DebugValue::Counter(2))
        );
    }

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_default_memory_operator() {
        let op = default_memory_operator();
        assert_eq!(op.info().scheme().to_string(), "memory");
    }

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_relativize_path_memory() {
        let storage = OpenDalStorage::Memory(default_memory_operator());

        assert_eq!(
            storage.relativize_path("memory:/path/to/file").unwrap(),
            "path/to/file"
        );
        // Without the scheme prefix, falls back to stripping the leading slash
        assert_eq!(
            storage.relativize_path("/path/to/file").unwrap(),
            "path/to/file"
        );
    }

    #[cfg(feature = "opendal-fs")]
    #[test]
    fn test_relativize_path_fs() {
        let storage = OpenDalStorage::LocalFs;

        assert_eq!(
            storage
                .relativize_path("file:/tmp/data/file.parquet")
                .unwrap(),
            "tmp/data/file.parquet"
        );
        assert_eq!(
            storage.relativize_path("/tmp/data/file.parquet").unwrap(),
            "tmp/data/file.parquet"
        );
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_relativize_path_s3() {
        let storage = OpenDalStorage::S3 {
            config: Arc::new(S3Config::default()),
            customized_credential_load: None,
        };

        // All S3-family schemes are accepted by the same storage instance.
        // Custom schemes for S3-compatible stores (e.g., `minio://`) are also
        // accepted because the path's scheme is used as-is for prefix matching.
        for scheme in ["s3", "s3a", "s3n", "minio"] {
            assert_eq!(
                storage
                    .relativize_path(&format!("{scheme}://my-bucket/path/to/file.parquet"))
                    .unwrap(),
                "path/to/file.parquet"
            );
        }
    }

    #[cfg(feature = "opendal-gcs")]
    #[test]
    fn test_relativize_path_gcs() {
        let storage = OpenDalStorage::Gcs {
            config: Arc::new(GcsConfig::default()),
        };

        assert_eq!(
            storage
                .relativize_path("gs://my-bucket/path/to/file.parquet")
                .unwrap(),
            "path/to/file.parquet"
        );
    }

    #[cfg(feature = "opendal-gcs")]
    #[test]
    fn test_relativize_path_gcs_invalid_scheme() {
        let storage = OpenDalStorage::Gcs {
            config: Arc::new(GcsConfig::default()),
        };

        assert!(
            storage
                .relativize_path("s3://my-bucket/path/to/file.parquet")
                .is_err()
        );
    }

    #[cfg(feature = "opendal-oss")]
    #[test]
    fn test_relativize_path_oss() {
        let storage = OpenDalStorage::Oss {
            config: Arc::new(OssConfig::default()),
        };

        assert_eq!(
            storage
                .relativize_path("oss://my-bucket/path/to/file.parquet")
                .unwrap(),
            "path/to/file.parquet"
        );
    }

    #[cfg(feature = "opendal-oss")]
    #[test]
    fn test_relativize_path_oss_invalid_scheme() {
        let storage = OpenDalStorage::Oss {
            config: Arc::new(OssConfig::default()),
        };

        assert!(
            storage
                .relativize_path("s3://my-bucket/path/to/file.parquet")
                .is_err()
        );
    }

    #[cfg(feature = "opendal-azdls")]
    #[test]
    fn test_relativize_path_azdls() {
        let storage = OpenDalStorage::Azdls {
            config: Arc::new(AzdlsConfig {
                account_name: Some("myaccount".to_string()),
                endpoint: Some("https://myaccount.dfs.core.windows.net".to_string()),
                ..Default::default()
            }),
        };

        assert_eq!(
            storage
                .relativize_path("abfss://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet")
                .unwrap(),
            "/path/to/file.parquet"
        );
    }
}
