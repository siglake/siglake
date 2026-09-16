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

use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;

use super::storage::{
    LocalFsStorageFactory, MemoryStorageFactory, Storage, StorageConfig, StorageFactory,
};
use crate::Result;

/// siglake byte-range object cache (the "cold-S3 read floor" hot cache).
///
/// `object_cache.rs` caches parsed manifests/manifest-lists; this caches the raw
/// *data-file* bytes — Parquet footers, column chunks, and index sidecars — that
/// every read otherwise re-fetches from object storage. Iceberg data/metadata
/// files are write-once (UUID/version-named, never mutated in place), so caching
/// by `(path, range)` is always correct. Process-global, LRU by total bytes,
/// gated by `SIGLAKE_OBJECT_CACHE_BYTES` (0/unset disables — so only processes
/// that opt in, e.g. the query server, pay the memory). The overwritten WAL
/// mirror is excluded out of caution.
fn object_cache_max_atomic() -> &'static std::sync::atomic::AtomicU64 {
    static MAX: OnceLock<std::sync::atomic::AtomicU64> = OnceLock::new();
    MAX.get_or_init(|| {
        // Default from the env so any process opts in by setting it; the query
        // server overrides via `set_object_cache_max_bytes`.
        let env = std::env::var("SIGLAKE_OBJECT_CACHE_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        std::sync::atomic::AtomicU64::new(env)
    })
}

fn object_cache_max_bytes() -> u64 {
    object_cache_max_atomic().load(std::sync::atomic::Ordering::Relaxed)
}

/// Set the byte-range object cache budget (bytes; 0 disables). Lets a process
/// (e.g. the query server) enable the hot cache without relying on the
/// `SIGLAKE_OBJECT_CACHE_BYTES` env. Process-global; takes effect for subsequent
/// reads.
pub fn set_object_cache_max_bytes(max_bytes: u64) {
    object_cache_max_atomic().store(max_bytes, std::sync::atomic::Ordering::Relaxed);
}

/// Immutable Iceberg data/metadata only — never cache the in-place-overwritten
/// WAL mirror / active segment.
fn object_cache_cacheable(path: &str) -> bool {
    !path.contains("/wal-mirror/") && !path.contains("/_active/")
}

#[derive(Default)]
struct ByteRangeCache {
    bytes: u64,
    order: VecDeque<String>,
    entries: HashMap<String, Bytes>,
}

impl ByteRangeCache {
    fn get(&self, key: &str) -> Option<Bytes> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: String, value: Bytes, max_bytes: u64) {
        let len = value.len() as u64;
        // Never let one entry exceed the whole budget (it would evict everything
        // then itself thrash); leave such reads uncached.
        if len > max_bytes {
            return;
        }
        if let Some(prev) = self.entries.insert(key.clone(), value) {
            self.bytes = self.bytes.saturating_sub(prev.len() as u64);
        }
        self.bytes = self.bytes.saturating_add(len);
        self.order.push_back(key);
        while self.bytes > max_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(evicted.len() as u64);
            }
        }
    }
}

fn byte_range_cache() -> &'static Mutex<ByteRangeCache> {
    static CACHE: OnceLock<Mutex<ByteRangeCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(ByteRangeCache::default()))
}

fn object_cache_get(key: &str) -> Option<Bytes> {
    let hit = byte_range_cache().lock().ok()?.get(key);
    metrics::counter!(
        "siglake_object_cache_requests_total",
        "outcome" => if hit.is_some() { "hit" } else { "miss" }
    )
    .increment(1);
    hit
}

/// siglake: a cache hit resolves inside the caller's poll, so a loop of hits
/// (a warm manifest walk, a footer sweep, a fully cached page scan) never
/// returns `Pending` and no `tokio::time::timeout` around it can fire until the
/// loop ends — a timeout only fires between polls. Spend one unit of tokio's
/// cooperative budget per hit instead: the task yields only once the budget
/// (128 units per poll) is gone, so an all-hit loop yields every 128 reads and a
/// timeout above it becomes enforceable at that granularity, while a read path
/// with a handful of hits pays a thread-local decrement. Outside a tokio
/// runtime the budget is unconstrained and this never yields.
///
/// Every hit path in this file goes through here; see the audit table in
/// siglake's `docs/DESIGN_continuous_compaction_and_ingest.md` (#1165).
async fn object_cache_hit(hit: Bytes) -> Bytes {
    tokio::task::coop::consume_budget().await;
    hit
}

fn object_cache_put(key: String, value: Bytes, max_bytes: u64) {
    if let Ok(mut cache) = byte_range_cache().lock() {
        cache.insert(key, value, max_bytes);
        metrics::gauge!("siglake_object_cache_bytes").set(cache.bytes as f64);
    }
}

/// A [`FileRead`] that serves byte ranges from the process-global byte-range
/// cache, filling on miss from the underlying reader.
struct CachingFileRead {
    inner: Box<dyn FileRead>,
    path: String,
    max_bytes: u64,
}

#[async_trait::async_trait]
impl FileRead for CachingFileRead {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
        let key = format!("{}@{}-{}", self.path, range.start, range.end);
        if let Some(hit) = object_cache_get(&key) {
            return Ok(object_cache_hit(hit).await);
        }
        let bytes = self.inner.read(range).await?;
        object_cache_put(key, bytes.clone(), self.max_bytes);
        Ok(bytes)
    }
}

/// FileIO implementation, used to manipulate files in underlying storage.
///
/// FileIO wraps a `dyn Storage` with lazy initialization via `StorageFactory`.
/// The storage is created on first use and cached for subsequent operations.
///
/// # Note
///
/// All paths passed to `FileIO` must be absolute paths starting with the scheme string
/// appropriate for the storage backend being used.
///
/// This crate provides native support for local filesystem (`file://`) and
/// memory (`memory://`) storage. For extensive storage backend support (S3, GCS,
/// OSS, Azure, etc.), use the
/// [`iceberg-storage-opendal`](https://crates.io/crates/iceberg-storage-opendal) crate.
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::io::{FileIO, FileIOBuilder};
/// use iceberg::io::{LocalFsStorageFactory, MemoryStorageFactory};
/// use std::sync::Arc;
///
/// // Create FileIO with memory storage for testing
/// let file_io = FileIO::new_with_memory();
///
/// // Create FileIO with local filesystem storage
/// let file_io = FileIO::new_with_fs();
///
/// // Create FileIO with custom factory
/// let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory))
///     .with_prop("key", "value")
///     .build();
/// ```
#[derive(Clone, Debug)]
pub struct FileIO {
    /// Storage configuration containing properties
    config: StorageConfig,
    /// Factory for creating storage instances
    factory: Arc<dyn StorageFactory>,
    /// Cached storage instance (lazily initialized)
    storage: Arc<OnceLock<Arc<dyn Storage>>>,
}

impl FileIO {
    /// Create a new FileIO backed by in-memory storage.
    ///
    /// This is useful for testing scenarios where persistent storage is not needed.
    pub fn new_with_memory() -> Self {
        Self {
            config: StorageConfig::new(),
            factory: Arc::new(MemoryStorageFactory),
            storage: Arc::new(OnceLock::new()),
        }
    }

    /// Create a new FileIO backed by local filesystem storage.
    ///
    /// This is useful for local development and testing with real files.
    pub fn new_with_fs() -> Self {
        Self {
            config: StorageConfig::new(),
            factory: Arc::new(LocalFsStorageFactory),
            storage: Arc::new(OnceLock::new()),
        }
    }

    /// Get the storage configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Get or create the storage instance.
    ///
    /// The factory is invoked on first access and the result is cached
    /// for all subsequent operations.
    fn get_storage(&self) -> Result<Arc<dyn Storage>> {
        // Check if already initialized
        if let Some(storage) = self.storage.get() {
            return Ok(storage.clone());
        }

        // Build the storage
        let storage = self.factory.build(&self.config)?;

        // Try to set it (another thread might have set it first)
        let _ = self.storage.set(storage.clone());

        // Return whatever is in the cell (either ours or another thread's)
        Ok(self.storage.get().unwrap().clone())
    }

    /// Deletes file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        self.get_storage()?.delete(path.as_ref()).await
    }

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    ///
    /// # Behavior
    ///
    /// - If the path is a file or not exist, this function will be no-op.
    /// - If the path is a empty directory, this function will remove the directory itself.
    /// - If the path is a non-empty directory, this function will remove the directory and all nested files and directories.
    pub async fn delete_prefix(&self, path: impl AsRef<str>) -> Result<()> {
        self.get_storage()?.delete_prefix(path.as_ref()).await
    }

    /// Check file exists.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn exists(&self, path: impl AsRef<str>) -> Result<bool> {
        self.get_storage()?.exists(path.as_ref()).await
    }

    /// Creates input file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile> {
        self.get_storage()?.new_input(path.as_ref())
    }

    /// Creates output file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile> {
        self.get_storage()?.new_output(path.as_ref())
    }
}

/// Builder for [`FileIO`].
///
/// The builder accepts an explicit `StorageFactory` and configuration properties.
/// Storage is lazily initialized on first use.
#[derive(Clone, Debug)]
pub struct FileIOBuilder {
    /// Factory for creating storage instances
    factory: Arc<dyn StorageFactory>,
    /// Storage configuration
    config: StorageConfig,
}

impl FileIOBuilder {
    /// Creates a new builder with the given storage factory.
    pub fn new(factory: Arc<dyn StorageFactory>) -> Self {
        Self {
            factory,
            config: StorageConfig::new(),
        }
    }

    /// Add a configuration property.
    pub fn with_prop(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.config = self.config.with_prop(key.to_string(), value.to_string());
        self
    }

    /// Add multiple configuration properties.
    pub fn with_props(
        mut self,
        args: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        self.config = self
            .config
            .with_props(args.into_iter().map(|e| (e.0.to_string(), e.1.to_string())));
        self
    }

    /// Get the storage configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Builds [`FileIO`].
    pub fn build(self) -> FileIO {
        FileIO {
            config: self.config,
            factory: self.factory,
            storage: Arc::new(OnceLock::new()),
        }
    }
}

/// The struct the represents the metadata of a file.
///
/// TODO: we can add last modified time, content type, etc. in the future.
pub struct FileMetadata {
    /// The size of the file.
    pub size: u64,
}

/// Trait for reading file.
///
/// # TODO
/// It's possible for us to remove the async_trait, but we need to figure
/// out how to handle the object safety.
#[async_trait::async_trait]
pub trait FileRead: Send + Sync + Unpin + 'static {
    /// Read file content with given range.
    ///
    /// TODO: we can support reading non-contiguous bytes in the future.
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes>;
}

/// Input file is used for reading from files.
#[derive(Clone, Debug)]
pub struct InputFile {
    storage: Arc<dyn Storage>,
    // Absolute path of file.
    path: String,
}

impl InputFile {
    /// Creates a new input file.
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self {
        Self { storage, path }
    }

    /// Absolute path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Check if file exists.
    pub async fn exists(&self) -> crate::Result<bool> {
        self.storage.exists(&self.path).await
    }

    /// Fetch and returns metadata of file.
    pub async fn metadata(&self) -> crate::Result<FileMetadata> {
        self.storage.metadata(&self.path).await
    }

    /// Read and returns whole content of file.
    ///
    /// For continuous reading, use [`Self::reader`] instead.
    pub async fn read(&self) -> crate::Result<Bytes> {
        let max_bytes = object_cache_max_bytes();
        if max_bytes > 0 && object_cache_cacheable(&self.path) {
            let key = format!("{}@whole", self.path);
            if let Some(hit) = object_cache_get(&key) {
                return Ok(object_cache_hit(hit).await);
            }
            let bytes = self.storage.read(&self.path).await?;
            object_cache_put(key, bytes.clone(), max_bytes);
            return Ok(bytes);
        }
        self.storage.read(&self.path).await
    }

    /// Creates [`FileRead`] for continuous reading.
    ///
    /// For one-time reading, use [`Self::read`] instead.
    pub async fn reader(&self) -> crate::Result<Box<dyn FileRead>> {
        let inner = self.storage.reader(&self.path).await?;
        let max_bytes = object_cache_max_bytes();
        if max_bytes > 0 && object_cache_cacheable(&self.path) {
            Ok(Box::new(CachingFileRead {
                inner,
                path: self.path.clone(),
                max_bytes,
            }))
        } else {
            Ok(inner)
        }
    }
}

/// Trait for writing file.
///
/// # TODO
///
/// It's possible for us to remove the async_trait, but we need to figure
/// out how to handle the object safety.
#[async_trait::async_trait]
pub trait FileWrite: Send + Unpin + 'static {
    /// Write bytes to file.
    ///
    /// TODO: we can support writing non-contiguous bytes in the future.
    async fn write(&mut self, bs: Bytes) -> crate::Result<()>;

    /// Close file.
    ///
    /// Calling close on closed file will generate an error.
    async fn close(&mut self) -> crate::Result<()>;
}

/// Output file is used for writing to files..
#[derive(Debug)]
pub struct OutputFile {
    storage: Arc<dyn Storage>,
    // Absolute path of file.
    path: String,
}

impl OutputFile {
    /// Creates a new output file.
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self {
        Self { storage, path }
    }

    /// Relative path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Checks if file exists.
    pub async fn exists(&self) -> Result<bool> {
        self.storage.exists(&self.path).await
    }

    /// Deletes file.
    ///
    /// If the file does not exist, it will not return error.
    pub async fn delete(&self) -> Result<()> {
        self.storage.delete(&self.path).await
    }

    /// Converts into [`InputFile`].
    pub fn to_input_file(self) -> InputFile {
        InputFile {
            storage: self.storage,
            path: self.path,
        }
    }

    /// Create a new output file with given bytes.
    ///
    /// # Notes
    ///
    /// Calling `write` will overwrite the file if it exists.
    /// For continuous writing, use [`Self::writer`].
    pub async fn write(&self, bs: Bytes) -> crate::Result<()> {
        self.storage.write(&self.path, bs).await
    }

    /// Creates output file for continuous writing.
    ///
    /// # Notes
    ///
    /// For one-time writing, use [`Self::write`] instead.
    pub async fn writer(&self) -> crate::Result<Box<dyn FileWrite>> {
        self.storage.writer(&self.path).await
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{File, create_dir_all};
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::AsyncReadExt;
    use futures::io::AllowStdIo;
    use tempfile::TempDir;

    use super::{FileIO, FileIOBuilder};
    use crate::io::{LocalFsStorageFactory, MemoryStorageFactory};

    fn create_local_file_io() -> FileIO {
        FileIO::new_with_fs()
    }

    fn write_to_file<P: AsRef<Path>>(s: &str, path: P) {
        create_dir_all(path.as_ref().parent().unwrap()).unwrap();
        let mut f = File::create(path).unwrap();
        write!(f, "{s}").unwrap();
    }

    async fn read_from_file<P: AsRef<Path>>(path: P) -> String {
        let mut f = AllowStdIo::new(File::open(path).unwrap());
        let mut s = String::new();
        f.read_to_string(&mut s).await.unwrap();
        s
    }

    #[tokio::test]
    async fn test_local_input_file() {
        let tmp_dir = TempDir::new().unwrap();

        let file_name = "a.txt";
        let content = "Iceberg loves rust.";

        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);
        write_to_file(content, &full_path);

        let file_io = create_local_file_io();
        let input_file = file_io.new_input(&full_path).unwrap();

        assert!(input_file.exists().await.unwrap());
        assert_eq!(&full_path, input_file.location());
        let read_content = read_from_file(full_path).await;

        assert_eq!(content, &read_content);
    }

    #[tokio::test]
    async fn test_delete_local_file() {
        let tmp_dir = TempDir::new().unwrap();

        let a_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), "a.txt");
        let sub_dir_path = format!("{}/sub", tmp_dir.path().to_str().unwrap());
        let b_path = format!("{}/{}", sub_dir_path, "b.txt");
        let c_path = format!("{}/{}", sub_dir_path, "c.txt");
        write_to_file("Iceberg loves rust.", &a_path);
        write_to_file("Iceberg loves rust.", &b_path);
        write_to_file("Iceberg loves rust.", &c_path);

        let file_io = create_local_file_io();
        assert!(file_io.exists(&a_path).await.unwrap());

        // Remove a file should be no-op.
        file_io.delete_prefix(&a_path).await.unwrap();
        assert!(file_io.exists(&a_path).await.unwrap());

        // Remove a not exist dir should be no-op.
        file_io.delete_prefix("not_exists/").await.unwrap();

        // Remove a dir should remove all files in it.
        file_io.delete_prefix(&sub_dir_path).await.unwrap();
        assert!(!file_io.exists(&b_path).await.unwrap());
        assert!(!file_io.exists(&c_path).await.unwrap());
        assert!(file_io.exists(&a_path).await.unwrap());

        file_io.delete(&a_path).await.unwrap();
        assert!(!file_io.exists(&a_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_non_exist_file() {
        let tmp_dir = TempDir::new().unwrap();

        let file_name = "a.txt";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let file_io = create_local_file_io();
        assert!(!file_io.exists(&full_path).await.unwrap());
        assert!(file_io.delete(&full_path).await.is_ok());
        assert!(file_io.delete_prefix(&full_path).await.is_ok());
    }

    #[tokio::test]
    async fn test_local_output_file() {
        let tmp_dir = TempDir::new().unwrap();

        let file_name = "a.txt";
        let content = "Iceberg loves rust.";

        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let file_io = create_local_file_io();
        let output_file = file_io.new_output(&full_path).unwrap();

        assert!(!output_file.exists().await.unwrap());
        {
            output_file.write(content.into()).await.unwrap();
        }

        assert_eq!(&full_path, output_file.location());

        let read_content = read_from_file(full_path).await;

        assert_eq!(content, &read_content);
    }

    #[tokio::test]
    async fn test_memory_io() {
        let io = FileIO::new_with_memory();

        let path = format!("{}/1.txt", TempDir::new().unwrap().path().to_str().unwrap());

        let output_file = io.new_output(&path).unwrap();
        output_file.write("test".into()).await.unwrap();

        assert!(io.exists(&path.clone()).await.unwrap());
        let input_file = io.new_input(&path).unwrap();
        let content = input_file.read().await.unwrap();
        assert_eq!(content, Bytes::from("test"));

        io.delete(&path).await.unwrap();
        assert!(!io.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_builder_with_props() {
        let factory = Arc::new(MemoryStorageFactory);
        let file_io = FileIOBuilder::new(factory)
            .with_prop("key1", "value1")
            .with_prop("key2", "value2")
            .build();

        assert_eq!(file_io.config().get("key1"), Some(&"value1".to_string()));
        assert_eq!(file_io.config().get("key2"), Some(&"value2".to_string()));
    }

    #[tokio::test]
    async fn test_file_io_builder_with_multiple_props() {
        let factory = Arc::new(LocalFsStorageFactory);
        let props = vec![("key1", "value1"), ("key2", "value2")];
        let file_io = FileIOBuilder::new(factory).with_props(props).build();

        assert_eq!(file_io.config().get("key1"), Some(&"value1".to_string()));
        assert_eq!(file_io.config().get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn byte_range_cache_serves_then_evicts_by_bytes() {
        let mut cache = super::ByteRangeCache::default();
        // Budget = 10 bytes. Insert three 4-byte entries -> the first is evicted.
        cache.insert("a".into(), Bytes::from_static(b"aaaa"), 10);
        cache.insert("b".into(), Bytes::from_static(b"bbbb"), 10);
        assert!(cache.get("a").is_some());
        assert!(cache.get("b").is_some());
        cache.insert("c".into(), Bytes::from_static(b"cccc"), 10); // 12 > 10 -> evict oldest
        assert!(cache.get("a").is_none(), "oldest evicted past the byte budget");
        assert!(cache.get("b").is_some());
        assert!(cache.get("c").is_some());
        assert!(cache.bytes <= 10);
    }

    #[test]
    fn byte_range_cache_skips_entries_larger_than_budget() {
        let mut cache = super::ByteRangeCache::default();
        cache.insert("big".into(), Bytes::from_static(b"xxxxxxxx"), 4); // 8 > 4 -> not cached
        assert!(cache.get("big").is_none());
        assert_eq!(cache.bytes, 0);
    }

    #[test]
    fn object_cache_excludes_the_overwritten_wal_mirror() {
        assert!(super::object_cache_cacheable(
            "s3://b/warehouse/siglake/events/data/abc.parquet"
        ));
        assert!(!super::object_cache_cacheable("s3://b/wal-mirror/seg-1.arrow"));
        assert!(!super::object_cache_cacheable("s3://b/wal-mirror/_active/seg.arrow"));
    }
}
