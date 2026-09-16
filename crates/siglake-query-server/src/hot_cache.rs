//! WS-6 query-tier hot caches: last-value + distinct, served in <10/<30 ms.
//!
//! Two in-memory structures maintained by tailing the WAL the query server
//! already mounts (RWX, read-only) for the real-time buffer:
//!
//! - **last-value** — the most-recent event per series key
//!   `(host, source, sourcetype, index)`. Answers "what's the latest line for
//!   each series?" dashboards without scanning.
//! - **distinct** — the set of distinct values seen per dimension column
//!   (`host`/`source`/`sourcetype`/`index`). Answers faceting / tag-value
//!   autocomplete.
//!
//! ## Semantics (InfluxDB last-value-cache style)
//!
//! The caches cover every series/value **written since the cache started
//! tracking** — they are populated from the WAL tail, not seeded from a full
//! historical scan, so a series that went silent before startup may be absent
//! until it next emits. Both folds are **idempotent and monotonic** (last-value
//! keeps the max-timestamp row; distinct is set-insert), so re-reading a segment
//! is harmless — the scanner only needs to read each segment *at least* once,
//! which lets it skip already-seen segments cheaply without exactly-once
//! bookkeeping. Freshness is bounded by the WAL seal threshold (`active/` is not
//! read — no IPC EOS), the same bound as the real-time buffer.
//!
//! Both maps are bounded ([`HotCacheConfig`]) so a high-cardinality dimension
//! can't grow them without limit; inserts past the cap are dropped and counted.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use arrow_array::builder::{StringBuilder, TimestampMicrosecondBuilder};
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

use siglake_wal::{COMMITTED_DIR, PROCESSING_DIR, SEALED_DIR};

/// The dimension columns the distinct cache tracks (and the series-key columns
/// for the last-value cache), in series-key order.
pub const DIMENSIONS: [&str; 4] = ["host", "source", "sourcetype", "index"];

/// A series identity: the tuple of dimension values, in [`DIMENSIONS`] order.
type SeriesKey = [String; 4];

/// The most-recent row recorded for a series.
#[derive(Clone, Debug)]
struct LastRow {
    ts_nanos: i64,
    raw: String,
}

/// Bounds + knobs for the hot caches.
#[derive(Clone, Copy, Debug)]
pub struct HotCacheConfig {
    /// Max series tracked by the last-value cache. Inserts of a *new* series
    /// past this are dropped (existing series keep updating).
    pub max_series: usize,
    /// Max distinct values tracked per dimension. Inserts past this are dropped.
    pub max_distinct_per_dim: usize,
}

impl Default for HotCacheConfig {
    fn default() -> Self {
        Self {
            max_series: 200_000,
            max_distinct_per_dim: 100_000,
        }
    }
}

#[derive(Default, Debug)]
struct HotState {
    last_value: HashMap<SeriesKey, LastRow>,
    /// `DIMENSIONS[i]` → its distinct values.
    distinct: [HashSet<String>; 4],
}

/// The query-tier hot caches for one tenant's WAL.
#[derive(Debug)]
pub struct HotCaches {
    cfg: HotCacheConfig,
    state: RwLock<HotState>,
    /// Basenames of WAL segments already folded in, so the scanner reads each
    /// segment at most once per process lifetime.
    seen: RwLock<HashSet<String>>,
}

impl HotCaches {
    pub fn new(cfg: HotCacheConfig) -> Self {
        Self {
            cfg,
            state: RwLock::new(HotState::default()),
            seen: RwLock::new(HashSet::new()),
        }
    }

    /// Fold one events `RecordBatch` into the caches. Unknown/short batches are
    /// skipped column-wise; only the dimension + timestamp + raw columns are
    /// read. Returns the number of rows folded.
    pub fn fold_batch(&self, batch: &RecordBatch) -> Result<usize> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(0);
        }
        let schema = batch.schema();
        let col_str = |name: &str| -> Result<&StringArray> {
            let idx = schema
                .index_of(name)
                .with_context(|| format!("hot-cache batch missing `{name}` column"))?;
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .with_context(|| format!("`{name}` is not Utf8"))
        };
        let dims: [&StringArray; 4] = [
            col_str(DIMENSIONS[0])?,
            col_str(DIMENSIONS[1])?,
            col_str(DIMENSIONS[2])?,
            col_str(DIMENSIONS[3])?,
        ];
        let raw = col_str("raw")?;
        // Exact nanoseconds: the last-value cache keeps the newest row per
        // series, and comparing microsecond-truncated instants would make the
        // winner among rows inside one microsecond arbitrary.
        let ts_idx = schema
            .index_of(siglake_core::nanos_source_column(
                schema.as_ref(),
                "timestamp",
            ))
            .context("missing timestamp")?;
        let ts = siglake_core::column_nanos(batch.column(ts_idx))
            .context("timestamp is not a timestamp or an ns long")?;

        let mut st = self.state.write().unwrap();
        for r in 0..n {
            let key: SeriesKey = [
                dims[0].value(r).to_string(),
                dims[1].value(r).to_string(),
                dims[2].value(r).to_string(),
                dims[3].value(r).to_string(),
            ];
            let ts_nanos = ts.value(r);
            // last-value: keep the max-timestamp row (ties keep the latest seen).
            match st.last_value.get_mut(&key) {
                Some(cur) => {
                    if ts_nanos >= cur.ts_nanos {
                        cur.ts_nanos = ts_nanos;
                        cur.raw = raw.value(r).to_string();
                    }
                }
                None => {
                    if st.last_value.len() < self.cfg.max_series {
                        st.last_value.insert(
                            key.clone(),
                            LastRow {
                                ts_nanos,
                                raw: raw.value(r).to_string(),
                            },
                        );
                    } else {
                        metrics::counter!("siglake_query_hot_cache_series_dropped_total")
                            .increment(1);
                    }
                }
            }
            // distinct: insert each dimension value (bounded).
            for d in 0..4 {
                let set = &mut st.distinct[d];
                if set.contains(key[d].as_str()) {
                    continue;
                }
                if set.len() < self.cfg.max_distinct_per_dim {
                    set.insert(key[d].clone());
                } else {
                    metrics::counter!("siglake_query_hot_cache_distinct_dropped_total",
                        "dim" => DIMENSIONS[d])
                    .increment(1);
                }
            }
        }
        Ok(n)
    }

    /// Scan `wal_dir` (`sealed/` + `processing/` + `committed/`) and fold every
    /// segment not yet seen. Idempotent across calls: only fresh segments are
    /// read. Unreadable segments are skipped (logged + counted). Returns the
    /// number of segments folded this call.
    pub fn scan_and_fold(&self, wal_dir: &Path) -> Result<usize> {
        let mut folded = 0usize;
        for sub in [SEALED_DIR, PROCESSING_DIR, COMMITTED_DIR] {
            let dir = wal_dir.join(sub);
            if !dir.is_dir() {
                continue;
            }
            for entry in
                std::fs::read_dir(&dir).with_context(|| format!("read_dir {}", dir.display()))?
            {
                let entry = entry?;
                let p = entry.path();
                if p.extension().and_then(|e| e.to_str()) != Some("arrow") {
                    continue;
                }
                let Some(name) = p.file_name().and_then(|f| f.to_str()).map(String::from) else {
                    continue;
                };
                if self.seen.read().unwrap().contains(&name) {
                    continue;
                }
                match siglake_wal::read_segment(&p) {
                    Ok(batches) => {
                        for b in &batches {
                            if let Err(e) = self.fold_batch(b) {
                                tracing::warn!(error = %e, segment = %p.display(),
                                    "hot-cache fold error");
                            }
                        }
                        self.seen.write().unwrap().insert(name);
                        folded += 1;
                    }
                    Err(e) => {
                        metrics::counter!("siglake_query_hot_cache_segment_errors_total")
                            .increment(1);
                        tracing::warn!(error = %e, segment = %p.display(),
                            "skipping unreadable hot-cache segment");
                    }
                }
            }
        }
        if folded > 0 {
            let st = self.state.read().unwrap();
            metrics::gauge!("siglake_query_hot_cache_series").set(st.last_value.len() as f64);
        }
        Ok(folded)
    }

    /// Snapshot the last-value cache as a `RecordBatch`: one row per series with
    /// columns `(host, source, sourcetype, index, timestamp, raw)`.
    pub fn last_values_batch(&self) -> Result<RecordBatch> {
        let st = self.state.read().unwrap();
        let mut dim_b: [StringBuilder; 4] = Default::default();
        let mut ts_b = TimestampMicrosecondBuilder::new().with_timezone(siglake_core::TIMESTAMP_TZ);
        let mut raw_b = StringBuilder::new();
        for (key, row) in st.last_value.iter() {
            for d in 0..4 {
                dim_b[d].append_value(&key[d]);
            }
            ts_b.append_value(siglake_core::micros_from_nanos(row.ts_nanos));
            raw_b.append_value(&row.raw);
        }
        let [mut h, mut s, mut t, mut i] = dim_b;
        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(h.finish()),
            Arc::new(s.finish()),
            Arc::new(t.finish()),
            Arc::new(i.finish()),
            Arc::new(ts_b.finish()),
            Arc::new(raw_b.finish()),
        ];
        RecordBatch::try_new(last_values_schema(), arrays).context("build last_values batch")
    }

    /// Snapshot the distinct cache for `dim` as a single-column `(value)` batch.
    /// Errors if `dim` is not a tracked dimension.
    pub fn distinct_batch(&self, dim: &str) -> Result<RecordBatch> {
        let d = DIMENSIONS.iter().position(|x| *x == dim).with_context(|| {
            format!("unknown dimension `{dim}` (expected one of {DIMENSIONS:?})")
        })?;
        let st = self.state.read().unwrap();
        let mut b = StringBuilder::new();
        for v in st.distinct[d].iter() {
            b.append_value(v);
        }
        RecordBatch::try_new(distinct_schema(), vec![Arc::new(b.finish())])
            .context("build distinct batch")
    }
}

/// Schema of [`HotCaches::last_values_batch`].
pub fn last_values_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("sourcetype", DataType::Utf8, false),
        Field::new("index", DataType::Utf8, false),
        Field::new("timestamp", siglake_core::timestamp_data_type(), false),
        Field::new("raw", DataType::Utf8, false),
    ]))
}

/// Schema of [`HotCaches::distinct_batch`].
pub fn distinct_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]))
}

/// Discover the per-tenant WAL directories under `root`: each entry is
/// `(tenant, dir)`. Supports both the flat single-tenant layout (`<root>/sealed`
/// ⇒ `("default", root)`) and the tenant-aware layout (`<root>/<tenant>/sealed`).
fn discover_tenant_dirs(root: &Path) -> Vec<(String, PathBuf)> {
    let mut out = Vec::new();
    if root.join(SEALED_DIR).is_dir() {
        out.push(("default".to_string(), root.to_path_buf()));
    }
    if let Ok(rd) = std::fs::read_dir(root) {
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() && p.join(SEALED_DIR).is_dir() {
                if let Some(name) = p.file_name().and_then(|f| f.to_str()) {
                    out.push((name.to_string(), p));
                }
            }
        }
    }
    out
}

/// Per-tenant registry of [`HotCaches`] over a shared WAL root. The query server
/// holds one; a background task calls [`refresh_once`](Self::refresh_once) on a
/// cadence, and each query registers the UDTFs bound to its tenant's cache.
#[derive(Debug)]
pub struct HotCacheRegistry {
    wal_root: PathBuf,
    cfg: HotCacheConfig,
    caches: RwLock<HashMap<String, Arc<HotCaches>>>,
}

impl HotCacheRegistry {
    pub fn new(wal_root: PathBuf, cfg: HotCacheConfig) -> Self {
        Self {
            wal_root,
            cfg,
            caches: RwLock::new(HashMap::new()),
        }
    }

    /// Get or lazily create the cache for `tenant`.
    pub fn for_tenant(&self, tenant: &str) -> Arc<HotCaches> {
        if let Some(c) = self.caches.read().unwrap().get(tenant) {
            return c.clone();
        }
        self.caches
            .write()
            .unwrap()
            .entry(tenant.to_string())
            .or_insert_with(|| Arc::new(HotCaches::new(self.cfg)))
            .clone()
    }

    /// One refresh pass over every discovered tenant WAL dir, folding fresh
    /// segments into each tenant's cache. Returns total segments folded.
    pub fn refresh_once(&self) -> usize {
        let mut total = 0;
        for (tenant, dir) in discover_tenant_dirs(&self.wal_root) {
            match self.for_tenant(&tenant).scan_and_fold(&dir) {
                Ok(n) => total += n,
                Err(e) => tracing::warn!(error = %e, tenant = %tenant,
                    "hot-cache refresh error"),
            }
        }
        total
    }

    /// Register the `last_values()` and `distinct_values(<dim>)` UDTFs into
    /// `ctx`, bound to `tenant`'s cache.
    pub fn register_udtfs(&self, ctx: &SessionContext, tenant: &str) {
        let cache = self.for_tenant(tenant);
        ctx.register_udtf(
            "last_values",
            Arc::new(LastValuesUdtf {
                cache: cache.clone(),
            }),
        );
        ctx.register_udtf("distinct_values", Arc::new(DistinctValuesUdtf { cache }));
    }
}

/// `last_values()` — one row per series, the most-recent event seen for it.
#[derive(Debug)]
struct LastValuesUdtf {
    cache: Arc<HotCaches>,
}

impl TableFunctionImpl for LastValuesUdtf {
    fn call(&self, _args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let batch = self
            .cache
            .last_values_batch()
            .map_err(|e| DataFusionError::External(e.into()))?;
        let schema = batch.schema();
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

/// `distinct_values('host'|'source'|'sourcetype'|'index')` — the distinct values
/// seen for that dimension since the cache started tracking.
#[derive(Debug)]
struct DistinctValuesUdtf {
    cache: Arc<HotCaches>,
}

impl TableFunctionImpl for DistinctValuesUdtf {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let dim = match args.first() {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => s.clone(),
            _ => {
                return Err(DataFusionError::Plan(
                    "distinct_values(dim) requires a single string-literal dimension".into(),
                ))
            }
        };
        let batch = self
            .cache
            .distinct_batch(&dim)
            .map_err(|e| DataFusionError::Plan(e.to_string()))?;
        let schema = batch.schema();
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use siglake_core::Event;
    use siglake_wal::WalWriter;
    use std::time::Duration;

    fn ev(host: &str, secs: i64, body: &str) -> Event {
        Event {
            timestamp: Utc.timestamp_opt(secs, 0).single().unwrap(),
            host: host.into(),
            source: "svc".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: body.into(),
            attributes: None,
        }
    }

    fn batch(evs: &[Event]) -> RecordBatch {
        siglake_core::events_to_record_batch(evs).unwrap()
    }

    /// The exposed `timestamp` is microsecond (the external contract); scale it
    /// back so the assertions below stay in the nanoseconds the cache folds on.
    fn last_for(b: &RecordBatch, host: &str) -> (i64, String) {
        let hosts = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let ts = siglake_core::column_nanos(b.column(4)).unwrap();
        let raw = b.column(5).as_any().downcast_ref::<StringArray>().unwrap();
        for r in 0..b.num_rows() {
            if hosts.value(r) == host {
                return (ts.value(r), raw.value(r).to_string());
            }
        }
        panic!("host {host} not in last_values");
    }

    #[test]
    fn last_value_keeps_the_max_timestamp_row() {
        let c = HotCaches::new(HotCacheConfig::default());
        // Out-of-order arrival: t=20 folded after t=30 must not overwrite.
        c.fold_batch(&batch(&[ev("h1", 10, "old")])).unwrap();
        c.fold_batch(&batch(&[ev("h1", 30, "newest")])).unwrap();
        c.fold_batch(&batch(&[ev("h1", 20, "middle")])).unwrap();
        let lv = c.last_values_batch().unwrap();
        assert_eq!(lv.num_rows(), 1, "one series");
        let (ts, raw) = last_for(&lv, "h1");
        assert_eq!(raw, "newest");
        assert_eq!(ts, 30_000_000_000, "ns of t=30s");
    }

    #[test]
    fn distinct_accumulates_per_dimension() {
        let c = HotCaches::new(HotCacheConfig::default());
        c.fold_batch(&batch(&[
            ev("h1", 1, "a"),
            ev("h2", 2, "b"),
            ev("h1", 3, "c"),
        ]))
        .unwrap();
        let hosts = c.distinct_batch("host").unwrap();
        let mut got: Vec<String> = {
            let a = hosts
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..a.len()).map(|i| a.value(i).to_string()).collect()
        };
        got.sort();
        assert_eq!(got, vec!["h1".to_string(), "h2".to_string()]);
        // sourcetype is constant across the rows ⇒ exactly one distinct value.
        assert_eq!(c.distinct_batch("sourcetype").unwrap().num_rows(), 1);
    }

    #[test]
    fn distinct_rejects_unknown_dimension() {
        let c = HotCaches::new(HotCacheConfig::default());
        assert!(c.distinct_batch("nope").is_err());
    }

    #[test]
    fn series_cap_drops_new_series_but_updates_existing() {
        let c = HotCaches::new(HotCacheConfig {
            max_series: 1,
            max_distinct_per_dim: 100,
        });
        c.fold_batch(&batch(&[ev("h1", 10, "first")])).unwrap();
        c.fold_batch(&batch(&[ev("h2", 10, "dropped")])).unwrap(); // new series past cap
        c.fold_batch(&batch(&[ev("h1", 20, "updated")])).unwrap(); // existing still updates
        let lv = c.last_values_batch().unwrap();
        assert_eq!(lv.num_rows(), 1, "capped at one series");
        assert_eq!(last_for(&lv, "h1").1, "updated");
    }

    #[tokio::test]
    async fn udtfs_serve_last_values_and_distinct() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w =
            WalWriter::with_thresholds(tmp.path(), "ing", 10_000, Duration::from_secs(60)).unwrap();
        w.append_events(&[ev("h1", 100, "x"), ev("h2", 200, "y"), ev("h1", 300, "z")])
            .unwrap();
        w.seal().unwrap().expect("sealed");

        let reg = HotCacheRegistry::new(tmp.path().to_path_buf(), HotCacheConfig::default());
        assert_eq!(
            reg.refresh_once(),
            1,
            "discovers the default-tenant WAL + folds it"
        );

        let ctx = SessionContext::new();
        reg.register_udtfs(&ctx, "default");

        // last_values(): one row per series, newest raw per host.
        let lv = ctx
            .sql("SELECT host, raw FROM last_values() ORDER BY host")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: usize = lv.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "two series");
        let host = lv[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let raw = lv[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            (host.value(0), raw.value(0)),
            ("h1", "z"),
            "h1 newest is t=300"
        );
        assert_eq!((host.value(1), raw.value(1)), ("h2", "y"));

        // distinct_values('host')
        let d = ctx
            .sql("SELECT value FROM distinct_values('host') ORDER BY value")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let vals: Vec<String> = d
            .iter()
            .flat_map(|b| {
                let a = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                (0..a.len())
                    .map(|i| a.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(vals, vec!["h1".to_string(), "h2".to_string()]);

        // a bad dimension is a planning error
        assert!(ctx
            .sql("SELECT * FROM distinct_values('nope')")
            .await
            .is_err());
    }

    #[test]
    fn scan_folds_each_segment_once() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w =
            WalWriter::with_thresholds(tmp.path(), "ing", 10_000, Duration::from_secs(60)).unwrap();
        w.append_events(&[ev("h1", 100, "x"), ev("h1", 200, "y")])
            .unwrap();
        w.seal().unwrap().expect("sealed");

        let c = HotCaches::new(HotCacheConfig::default());
        assert_eq!(c.scan_and_fold(tmp.path()).unwrap(), 1, "one fresh segment");
        assert_eq!(
            c.scan_and_fold(tmp.path()).unwrap(),
            0,
            "already seen ⇒ skipped"
        );
        assert_eq!(last_for(&c.last_values_batch().unwrap(), "h1").1, "y");
    }
}
