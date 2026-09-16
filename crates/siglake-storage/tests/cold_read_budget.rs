use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, RecordBatch, StringArray};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};

use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter_sum(snapshot: &SnapshotVec, name: &str, phase: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "phase" && label.value() == phase)
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct MatchTermsUdf {
    signature: Signature,
}

impl MatchTermsUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    datafusion::arrow::datatypes::DataType::Utf8,
                    datafusion::arrow::datatypes::DataType::Utf8,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MatchTermsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "match_terms"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[datafusion::arrow::datatypes::DataType],
    ) -> datafusion::error::Result<datafusion::arrow::datatypes::DataType> {
        Ok(datafusion::arrow::datatypes::DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let rows = args.number_rows;
        let cells = arg_as_str_array(&args.args[0], rows, "lhs")?;
        let query = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => s.clone(),
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "query must be a Utf8 literal".into(),
                ));
            }
        };
        let tokens: Vec<String> = query
            .split_whitespace()
            .map(|t| t.to_ascii_lowercase())
            .collect();
        let mut builder = BooleanBuilder::with_capacity(rows);
        for row in 0..rows {
            if cells.is_null(row) {
                builder.append_value(false);
                continue;
            }
            let haystack = cells.value(row).to_ascii_lowercase();
            builder.append_value(tokens.iter().all(|token| haystack.contains(token)));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

fn arg_as_str_array(
    arg: &ColumnarValue,
    rows: usize,
    label: &str,
) -> datafusion::error::Result<Arc<StringArray>> {
    let array = match arg {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(rows)?,
    };
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|array| Arc::new(array.clone()))
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!("{label} must be Utf8"))
        })
}

async fn collect(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

#[tokio::test]
async fn cold_match_terms_query_stays_within_object_store_read_budget() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path())
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO);
    let chunks: [&[&str]; 3] = [
        &["healthy heartbeat", "healthy row", "green steady"],
        &[
            "error timeout retry",
            "error timeout backoff",
            "timeout but recoverable",
        ],
        &[
            "database connection refused",
            "connection refused peer",
            "warning row",
        ],
    ];
    for chunk in chunks {
        let events: Vec<Event> = chunk
            .iter()
            .map(|raw| Event::now((*raw).to_string()))
            .collect();
        ice.append_events(&events).await.unwrap();
    }

    let ctx = SessionContext::new();
    let mut state = ctx.state();
    state
        .config_mut()
        .set_extension(Arc::new(siglake_storage::OrderedScanTuning {
            bypass_reader_caches: true,
            ..Default::default()
        }));
    let ctx = SessionContext::new_with_state(state);
    ctx.register_udf(ScalarUDF::from(MatchTermsUdf::new()));
    ice.register_with_datafusion(&ctx).await.unwrap();

    let batches = collect(
        &ctx,
        "SELECT raw FROM events WHERE match_terms(raw, 'error timeout')",
    )
    .await;
    let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(rows, 2, "fixture should prune to the single matching file");

    let snapshot = snapshotter.snapshot().into_vec();
    let manifest_reads = counter_sum(&snapshot, "siglake_object_store_reads_total", "manifest");
    let footer_reads = counter_sum(&snapshot, "siglake_object_store_reads_total", "footer");
    let index_reads = counter_sum(&snapshot, "siglake_object_store_reads_total", "index");
    let data_reads = counter_sum(&snapshot, "siglake_object_store_reads_total", "data");
    let candidate_files_post_manifest_prune = 3u64;

    assert!(
        manifest_reads > 0,
        "scan planning should read manifests on a cold query"
    );
    assert!(
        footer_reads <= candidate_files_post_manifest_prune,
        "cold footer reads should stay within the file set considered after manifest pruning"
    );
    assert!(
        index_reads <= candidate_files_post_manifest_prune,
        "puffin/page-index reads should stay within the candidate file set"
    );
    assert!(
        (1..=2).contains(&data_reads),
        "only the surviving file should issue data-phase reads; got {data_reads}"
    );

    // F-5: the READER byte classes must partition the reader's fetched total.
    // A split that does not add up is worse than no split — it invites
    // conclusions ("the footers are fat") the numbers do not support.
    //
    // `manifest` is deliberately NOT in that sum: planning-time manifest reads
    // are recorded by a different path and are not part of the reader's byte
    // total, so `stats.scan.fetched_bytes` does not include them either. That
    // asymmetry is easy to miss and is exactly what this pins.
    let reader_classes: u64 = ["footer", "index", "data", "other"]
        .iter()
        .map(|phase| counter_sum(&snapshot, "siglake_object_store_read_bytes_total", phase))
        .sum();
    let total_bytes = snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == "siglake_iceberg_object_store_bytes_read_total"
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum::<u64>();
    assert!(reader_classes > 0, "the scan must attribute some bytes");
    assert_eq!(
        reader_classes, total_bytes,
        "reader byte classes must sum to the reader total \
         (classes={reader_classes}, total={total_bytes})"
    );
    let manifest_bytes = counter_sum(
        &snapshot,
        "siglake_object_store_read_bytes_total",
        "manifest",
    );
    assert!(
        manifest_bytes > 0,
        "planning reads manifests, and they are counted separately from the reader total"
    );
}

/// F-5: the per-request byte classes must map each read phase to the right
/// class and conserve the bytes recorded.
///
/// This lives here, not beside the code, because the vendored `iceberg` crate
/// is NOT a workspace member — a `#[cfg(test)]` module inside it never runs.
/// An earlier version of this check went through the GLOBAL phase metrics and
/// passed even with the mapping's arms deliberately swapped, which is the
/// definition of a vacuous test; this exercises the mapping itself.
#[test]
fn f5_phase_bytes_land_in_the_matching_class() {
    use iceberg::arrow::ScanCounters;
    use iceberg::io::read_observability::ObjectStoreReadPhase;
    use std::sync::atomic::Ordering::Relaxed;

    let c = ScanCounters::default();
    c.add_phase_bytes(ObjectStoreReadPhase::Footer, 100);
    c.add_phase_bytes(ObjectStoreReadPhase::Index, 20);
    c.add_phase_bytes(ObjectStoreReadPhase::Data, 3);
    c.add_phase_bytes(ObjectStoreReadPhase::Manifest, 7);
    c.add_phase_bytes(ObjectStoreReadPhase::Other, 1);

    assert_eq!(c.bytes_footer.load(Relaxed), 100);
    assert_eq!(c.bytes_index.load(Relaxed), 20);
    assert_eq!(c.bytes_data.load(Relaxed), 3);
    // Manifest and Other share a class deliberately: neither is a scan
    // structure, and splitting them would imply precision the read path
    // does not have.
    assert_eq!(c.bytes_other.load(Relaxed), 8);

    let total = c.bytes_footer.load(Relaxed)
        + c.bytes_index.load(Relaxed)
        + c.bytes_data.load(Relaxed)
        + c.bytes_other.load(Relaxed);
    assert_eq!(total, 131, "byte classes must conserve the bytes recorded");
}
