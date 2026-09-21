//! Task #4959: qualify per-shape file attribution without changing `stats.scan`.
//!
//! Two appends make one table heterogeneous: the larger, older file has a
//! floor-sized row group plus a tail, while the smaller, newer file has one
//! short group. The browse's time bound prunes the large file during planning.
//! The in-process-only attribution must name the short file at every stage,
//! demonstrating why sampling the largest warehouse object gives the wrong
//! geometry for this shape.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};

use chrono::{Duration, TimeZone, Utc};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, IcebergTuning};
use siglake_storage::{QueryScanTuning, SiglakeIcebergTableScan};

const LARGE_ROWS: usize = 131_072 + 8_192;
const SHORT_ROWS: usize = 8_192;
const ATTRIBUTION_METRIC: &str = "qualification_file_attribution";

fn events(rows: usize, day: u32, prefix: &str) -> Vec<Event> {
    let base = Utc
        .with_ymd_and_hms(2026, 1, day, 0, 0, 0)
        .single()
        .unwrap();
    (0..rows)
        .map(|row| {
            let mut event = Event::now(format!("{prefix}-{row}"));
            event.timestamp = base + Duration::microseconds(row as i64);
            event.host = if prefix == "new" { "selected" } else { "old" }.into();
            event
        })
        .collect()
}

fn parquet_files(root: &Path) -> Vec<PathBuf> {
    fn visit(path: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                out.push(path);
            }
        }
    }

    let mut out = Vec::new();
    visit(root, &mut out);
    out
}

fn geometry(path: &Path) -> Vec<u64> {
    ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap())
        .unwrap()
        .metadata()
        .row_groups()
        .iter()
        .map(|group| group.num_rows() as u64)
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttributedTask {
    file: String,
    start: u64,
    length: u64,
}

fn label(metric: &datafusion::physical_plan::metrics::Metric, name: &str) -> String {
    metric
        .labels()
        .iter()
        .find(|label| label.name() == name)
        .unwrap_or_else(|| panic!("metric has no {name} label"))
        .value()
        .to_string()
}

fn attribution(scan: &Arc<dyn ExecutionPlan>) -> BTreeMap<String, Vec<AttributedTask>> {
    let metrics = scan.metrics().unwrap();
    let mut stages = BTreeMap::<String, Vec<AttributedTask>>::new();
    for metric in metrics.iter() {
        if metric.value().name() != ATTRIBUTION_METRIC || metric.value().as_usize() == 0 {
            continue;
        }
        stages
            .entry(label(metric, "stage"))
            .or_default()
            .push(AttributedTask {
                file: label(metric, "file"),
                start: label(metric, "start").parse().unwrap(),
                length: label(metric, "length").parse().unwrap(),
            });
    }
    stages
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pruned_browse_names_the_file_whose_geometry_it_used() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path())
        .await
        .unwrap()
        .with_tuning(IcebergTuning {
            // The writer clamps this to the 131,072-row floor.
            target_row_group_bytes: Some(1),
            ..Default::default()
        });
    ice.append_events(&events(LARGE_ROWS, 1, "old"))
        .await
        .unwrap();
    ice.append_events(&events(SHORT_ROWS, 2, "new"))
        .await
        .unwrap();

    siglake_storage::configure_query_scan_tuning(QueryScanTuning {
        file_cache_max_bytes: Some(8 * 1024 * 1024 * 1024),
        file_cache_max_entries: Some(16_384),
        file_concurrency_limit: Some(1),
        file_attribution_prototype: true,
        ..Default::default()
    });
    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let plan = ctx
        .sql(
            "SELECT raw FROM events \
             WHERE timestamp >= TIMESTAMP '2026-01-02T00:00:00Z' \
             AND host = 'selected'",
        )
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    let batches = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        SHORT_ROWS
    );
    assert!(
        siglake_storage::settle_scan_partitions(&plan, StdDuration::from_secs(10))
            .await
            .complete
    );

    let mut scans = Vec::<Arc<dyn ExecutionPlan>>::new();
    plan.apply(|node| {
        if node
            .as_any()
            .downcast_ref::<SiglakeIcebergTableScan>()
            .is_some()
        {
            scans.push(node.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    assert_eq!(scans.len(), 1);

    let stages = attribution(&scans[0]);
    for stage in ["planned", "cache_candidate", "reader_attempt"] {
        assert_eq!(
            stages.get(stage).map(Vec::len),
            Some(1),
            "{stage} attribution: {stages:#?}"
        );
    }
    let named = stages
        .values()
        .flatten()
        .map(|task| task.file.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(named.len(), 1, "all stages must name the same file");
    let selected = Path::new(named.first().unwrap().strip_prefix("file://").unwrap());

    let mut files = parquet_files(tmp.path());
    files.sort_by_key(|path| std::cmp::Reverse(std::fs::metadata(path).unwrap().len()));
    assert_eq!(files.len(), 2);
    let sampled_largest = &files[0];
    assert_eq!(geometry(sampled_largest), vec![131_072, 8_192]);
    assert_eq!(geometry(selected), vec![8_192]);
    assert_ne!(
        sampled_largest, selected,
        "the fixture must reproduce the collector's largest-file mismatch"
    );

    // Compare the labelled capture with the bounded SQL shape proposed in the
    // design record. The byte count is retained as local evidence, not a wire
    // contract: paths vary with the temporary root.
    let task = &stages["reader_attempt"][0];
    let proposed = json!({
        "files": [{"id": task.file, "start": task.start, "length": task.length}],
        "omitted": 0
    });
    let serialized_bytes = serde_json::to_vec(&proposed).unwrap().len();
    let retained_identity_bytes = task.file.len() + size_of::<u64>() * 2;
    let metric_label_bytes = scans[0]
        .metrics()
        .unwrap()
        .iter()
        .filter(|metric| {
            matches!(
                metric.value().name(),
                "qualification_file_attribution" | "qualification_file_attribution_omitted"
            )
        })
        .flat_map(|metric| metric.labels())
        .map(|label| label.name().len() + label.value().len())
        .sum::<usize>();
    assert!(
        serialized_bytes < 512,
        "one local identity is unexpectedly large"
    );

    let started = Instant::now();
    for _ in 0..10_000 {
        std::hint::black_box(attribution(&scans[0]));
    }
    let collection_ns = started.elapsed().as_nanos() / 10_000;
    eprintln!(
        "file-attribution qualification: retained_identity_bytes={retained_identity_bytes} \
         metric_label_bytes={metric_label_bytes} serialized_bytes={serialized_bytes} \
         collection_ns={collection_ns}"
    );
}
