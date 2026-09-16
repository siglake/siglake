//! Iceberg round-trip: append events through `IcebergContext`, then query the
//! resulting snapshot via the DataFusion `IcebergStaticTableProvider`.

use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, StringArray};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

use siglake_core::{Event, PromotedColumn, PromotedType};
use siglake_storage::iceberg::IcebergContext;

fn warehouse_dir(tmp: &tempfile::TempDir) -> std::path::PathBuf {
    tmp.path().to_path_buf()
}

/// Recursively collect every regular file under `dir`.
fn list_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            for entry in rd.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p.is_file() {
                    out.push(p);
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, &mut out);
    out
}

async fn explain_physical(df: &datafusion::dataframe::DataFrame) -> String {
    use datafusion::physical_plan::displayable;

    format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    )
}

async fn count_value(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

async fn grouped_string_counts(
    ctx: &SessionContext,
    sql: &str,
) -> std::collections::HashMap<String, i64> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut counts = std::collections::HashMap::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            counts.insert(keys.value(row).to_string(), values.value(row));
        }
    }
    counts
}

async fn grouped_i64_counts(
    ctx: &SessionContext,
    sql: &str,
) -> std::collections::HashMap<i64, i64> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut counts = std::collections::HashMap::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            counts.insert(keys.value(row), values.value(row));
        }
    }
    counts
}

fn collect_rows(batches: &[arrow_array::RecordBatch]) -> Vec<(i64, String, i64, String)> {
    batches
        .iter()
        .flat_map(|batch| {
            let ts = siglake_core::column_nanos(
                batch
                    .column_by_name(siglake_core::nanos_source_column(
                        batch.schema().as_ref(),
                        "timestamp",
                    ))
                    .unwrap(),
            )
            .unwrap();
            let host = batch
                .column_by_name("host")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let status = batch
                .column_by_name("status")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            let raw = batch
                .column_by_name("raw")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|i| {
                    (
                        ts.value(i),
                        host.value(i).to_string(),
                        status.value(i),
                        raw.value(i).to_string(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
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
        let cells = match &args.args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(rows)?,
        };
        let cells = cells
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("lhs must be Utf8".into())
            })?;
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
            .map(|token| token.to_ascii_lowercase())
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

#[tokio::test]
async fn query_cache_refreshes_on_schema_only_migration() {
    // A schema-only widen (e.g. enabling WS-7 promotion on an existing warehouse)
    // doesn't commit a data snapshot. The table-metadata cache must still rebuild
    // the provider on a schema-id change, or a separate reader (the query server)
    // keeps serving the old columns until the next data commit.
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);

    // Reader context (the "query server"): registers + queries, caching a provider
    // built from the initial 7-column schema.
    let reader = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_table_cache_ttl(std::time::Duration::ZERO);
    let ctx = SessionContext::new();
    reader.register_with_datafusion(&ctx).await.unwrap();
    ctx.sql("SELECT count(*) AS n FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // A *separate* context (the "compactor") widens the schema. This is a
    // schema-only change — no data snapshot is committed — so the reader's
    // cache key (snapshot id) is unchanged; only the schema id moves.
    let writer = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_promoted_columns(vec![PromotedColumn {
            attr_key: "http.status_code".into(),
            name: "status".into(),
            ty: PromotedType::Int64,
        }]);
    assert_eq!(
        writer.ensure_promoted_columns().await.unwrap(),
        1,
        "migration adds status"
    );

    // The reader must now see the new `status` column: with the snapshot id
    // unchanged, only the schema-id comparison forces the provider rebuild.
    let ctx2 = SessionContext::new();
    reader.register_with_datafusion(&ctx2).await.unwrap();
    // Planning + running a query that references `status` must succeed (not
    // "column not found").
    let n = ctx2
        .sql("SELECT count(*) AS n FROM events WHERE status IS NULL")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        n[0].column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
        0
    );
}

#[tokio::test]
async fn ws7_promoted_attributes_become_typed_columns() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);

    let promoted = vec![
        PromotedColumn {
            attr_key: "http.status_code".into(),
            name: "status".into(),
            ty: PromotedType::Int64,
        },
        PromotedColumn {
            attr_key: "k8s.namespace".into(),
            name: "ns".into(),
            ty: PromotedType::Utf8,
        },
    ];
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_promoted_columns(promoted);
    // Widen the freshly-created events table to carry the typed columns.
    ice.ensure_promoted_columns().await.unwrap();

    // 4 events with residual attributes; status 500 on the odd ones.
    let events: Vec<Event> = (0..4)
        .map(|i| {
            let code = if i % 2 == 0 { 200 } else { 500 };
            Event::now(format!("e{i}")).with_attributes(Some(format!(
                r#"{{"http.status_code":{code},"k8s.namespace":"prod"}}"#
            )))
        })
        .collect();
    ice.append_events(&events).await.unwrap();

    let ctx = siglake_storage::session_context();
    ctx.register_udf(ScalarUDF::from(MatchTermsUdf::new()));
    ice.register_with_datafusion(&ctx).await.unwrap();

    // The promoted columns are real typed columns: filter on the Int64 column.
    let df = ctx
        .sql("SELECT count(*) AS n FROM events WHERE status >= 500")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 2, "status is a typed Int64 column queryable directly");

    // GROUP BY the promoted Utf8 column.
    let df = ctx
        .sql("SELECT ns, count(*) AS n FROM events GROUP BY ns")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1, "one namespace group");
    let ns = batches[0]
        .column_by_name("ns")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(ns.value(0), "prod");

    // The promoted Utf8 column `ns` gets a row-group bloom (equality pruning);
    // the promoted Int64 column `status` relies on min/max statistics, no bloom.
    let parquet = list_files(&warehouse)
        .into_iter()
        .find(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
        .expect("a parquet data file");
    use parquet::file::reader::FileReader;
    let bytes = std::fs::read(&parquet).unwrap();
    let reader =
        parquet::file::reader::SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
    let meta = reader.metadata();
    let mut ns_has_bloom = false;
    let mut status_has_bloom = false;
    for rg in 0..meta.num_row_groups() {
        let rgm = meta.row_group(rg);
        for c in 0..rgm.num_columns() {
            let col = rgm.column(c);
            match col.column_path().string().as_str() {
                "ns" => ns_has_bloom |= col.bloom_filter_offset().is_some(),
                "status" => status_has_bloom |= col.bloom_filter_offset().is_some(),
                _ => {}
            }
        }
    }
    // Native blooms default OFF since 2026-08-06 — they prune nothing in a
    // time-sorted layout. What this still pins is that the promoted-column
    // WIRING picks Utf8 and not Int64: with the knob on, `ns` gets a bloom and
    // `status` does not (see native_bloom_opt_in.rs).
    assert!(
        !ns_has_bloom,
        "promoted Utf8 column `ns` should have no native bloom by default"
    );
    assert!(
        !status_has_bloom,
        "promoted Int64 column `status` should NOT have a bloom"
    );
}

#[tokio::test]
async fn exact_filter_pushdown_limit_removes_residual_filter_and_keeps_results_correct() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_promoted_columns(vec![PromotedColumn {
            attr_key: "http.status_code".into(),
            name: "status".into(),
            ty: PromotedType::Int64,
        }]);
    ice.ensure_promoted_columns().await.unwrap();

    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    let wide = "x".repeat(4096);
    let specs = [
        ("us-east-2", 500_i64, format!("error timeout east-0 {wide}")),
        ("us-west-1", 200_i64, format!("ok west-1 {wide}")),
        ("us-east-2", 503_i64, format!("error east-2 {wide}")),
        ("eu-central-1", 404_i64, format!("warn eu-3 {wide}")),
        ("us-east-2", 501_i64, format!("timeout east-4 {wide}")),
        ("us-west-1", 204_i64, format!("ok west-5 {wide}")),
        ("eu-central-1", 200_i64, format!("error eu-6 {wide}")),
        ("us-east-2", 418_i64, format!("teapot east-7 {wide}")),
        ("us-west-1", 502_i64, format!("timeout west-8 {wide}")),
        ("us-east-2", 200_i64, format!("ok east-9 {wide}")),
        (
            "eu-central-1",
            503_i64,
            format!("error timeout eu-10 {wide}"),
        ),
        ("us-east-2", 504_i64, format!("error east-11 {wide}")),
    ];

    let mut expected = Vec::new();
    for (file_idx, chunk) in specs.chunks(4).enumerate() {
        let batch: Vec<Event> = chunk
            .iter()
            .enumerate()
            .map(|(row_idx, (host, status, raw))| {
                let ordinal = (file_idx * 4 + row_idx) as i64;
                expected.push((
                    (base + Duration::seconds(ordinal))
                        .timestamp_nanos_opt()
                        .unwrap(),
                    (*host).to_string(),
                    *status,
                    raw.clone(),
                ));
                let mut event = Event::now(raw.clone())
                    .with_attributes(Some(format!(r#"{{"http.status_code":{status}}}"#)));
                event.timestamp = base + Duration::seconds(ordinal);
                event.host = (*host).to_string();
                event
            })
            .collect();
        ice.append_events(&batch).await.unwrap();
    }

    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(MatchTermsUdf::new()));
    ice.register_with_datafusion(&ctx).await.unwrap();

    let explain_df = ctx
        .sql("SELECT \"timestamp\", raw FROM events WHERE host = 'us-east-2' LIMIT 3")
        .await
        .unwrap();
    let explain = explain_physical(&explain_df).await;
    assert!(
        !explain.contains("FilterExec"),
        "exact host equality must remove the residual filter:\n{explain}"
    );
    assert!(
        explain.contains("fetch=3"),
        "the physical plan must carry the pushed fetch:\n{explain}"
    );
    assert!(
        explain.contains("limit:[3]"),
        "the scan display must carry the pushed limit:\n{explain}"
    );

    let gt_limit = ctx
        .sql("SELECT \"timestamp\", host, status, raw FROM events WHERE host = 'us-east-2' LIMIT 3")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let gt_limit_rows = collect_rows(&gt_limit);
    let want_host: std::collections::HashSet<_> = expected
        .iter()
        .filter(|(_, host, _, _)| host == "us-east-2")
        .cloned()
        .collect();
    assert_eq!(
        gt_limit_rows.len(),
        3,
        "LIMIT 3 must stop after any 3 exact matches"
    );
    assert!(
        gt_limit_rows.iter().all(|row| want_host.contains(row)),
        "LIMIT rows must all satisfy the exact predicate"
    );

    let lt_limit = ctx
        .sql("SELECT \"timestamp\", host, status, raw FROM events WHERE host = 'eu-central-1' LIMIT 10")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let lt_limit_rows = collect_rows(&lt_limit);
    let want_small: std::collections::HashSet<_> = expected
        .iter()
        .filter(|(_, host, _, _)| host == "eu-central-1")
        .cloned()
        .collect();
    assert_eq!(lt_limit_rows.len(), want_small.len());
    assert!(
        lt_limit_rows.iter().all(|row| want_small.contains(row)),
        "all rows must survive when matches stay below LIMIT"
    );
}

#[tokio::test]
async fn exact_filter_pushdown_matches_bruteforce_for_exact_safe_shapes() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_promoted_columns(vec![PromotedColumn {
            attr_key: "http.status_code".into(),
            name: "status".into(),
            ty: PromotedType::Int64,
        }]);
    ice.ensure_promoted_columns().await.unwrap();

    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    let specs = [
        ("us-east-2", 500_i64, "row-0"),
        ("us-west-1", 200_i64, "row-1"),
        ("us-east-2", 503_i64, "row-2"),
        ("eu-central-1", 404_i64, "row-3"),
        ("us-east-2", 501_i64, "row-4"),
        ("us-west-1", 204_i64, "row-5"),
        ("eu-central-1", 200_i64, "row-6"),
        ("us-east-2", 418_i64, "row-7"),
        ("us-west-1", 502_i64, "row-8"),
        ("us-east-2", 200_i64, "row-9"),
        ("eu-central-1", 503_i64, "row-10"),
        ("us-east-2", 504_i64, "row-11"),
    ];

    let mut expected = Vec::new();
    let batch: Vec<Event> = specs
        .iter()
        .enumerate()
        .map(|(i, (host, status, raw))| {
            let ts = (base + Duration::seconds(i as i64))
                .timestamp_nanos_opt()
                .unwrap();
            expected.push((ts, (*host).to_string(), *status, (*raw).to_string()));
            let mut event = Event::now(*raw)
                .with_attributes(Some(format!(r#"{{"http.status_code":{status}}}"#)));
            event.timestamp = base + Duration::seconds(i as i64);
            event.host = (*host).to_string();
            event
        })
        .collect();
    ice.append_events(&batch).await.unwrap();

    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(MatchTermsUdf::new()));
    ice.register_with_datafusion(&ctx).await.unwrap();

    let cases = [
        (
            "SELECT \"timestamp\", host, status, raw FROM events \
             WHERE host = 'us-east-2' ORDER BY \"timestamp\"",
            expected
                .iter()
                .filter(|(_, host, _, _)| host == "us-east-2")
                .cloned()
                .collect::<Vec<_>>(),
        ),
        (
            "SELECT \"timestamp\", host, status, raw FROM events \
             WHERE status >= 500 AND status < 600 ORDER BY \"timestamp\"",
            expected
                .iter()
                .filter(|(_, _, status, _)| *status >= 500 && *status < 600)
                .cloned()
                .collect::<Vec<_>>(),
        ),
        (
            "SELECT \"timestamp\", host, status, raw FROM events \
             WHERE host IN ('us-east-2', 'us-west-1') ORDER BY \"timestamp\"",
            expected
                .iter()
                .filter(|(_, host, _, _)| host == "us-east-2" || host == "us-west-1")
                .cloned()
                .collect::<Vec<_>>(),
        ),
        (
            "SELECT \"timestamp\", host, status, raw FROM events \
             WHERE host = 'us-east-2' AND status >= 500 ORDER BY \"timestamp\"",
            expected
                .iter()
                .filter(|(_, host, status, _)| host == "us-east-2" && *status >= 500)
                .cloned()
                .collect::<Vec<_>>(),
        ),
    ];

    for (sql, want) in cases {
        let got = collect_rows(&ctx.sql(sql).await.unwrap().collect().await.unwrap());
        assert_eq!(got, want, "exact pushdown query diverged for `{sql}`");
    }
}

#[tokio::test]
async fn like_and_match_terms_filters_stay_inexact() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let events = vec![
        Event::now("database error timeout"),
        Event::now("database success"),
        Event::now("network timeout"),
    ];
    ice.append_events(&events).await.unwrap();

    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(MatchTermsUdf::new()));
    ice.register_with_datafusion(&ctx).await.unwrap();

    let like_df = ctx
        .sql("SELECT raw FROM events WHERE raw LIKE '%database%' LIMIT 2")
        .await
        .unwrap();
    let like_plan = explain_physical(&like_df).await;
    assert!(
        like_plan.contains("FilterExec"),
        "LIKE must keep the residual filter:\n{like_plan}"
    );

    let match_df = ctx
        .sql("SELECT raw FROM events WHERE match_terms(raw, 'database timeout') LIMIT 2")
        .await
        .unwrap();
    let match_plan = explain_physical(&match_df).await;
    assert!(
        match_plan.contains("FilterExec"),
        "match_terms must stay inexact so the engine re-checks rows:\n{match_plan}"
    );
}

#[tokio::test]
async fn filtered_count_star_stays_correct_for_exact_and_inexact_shapes() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_promoted_columns(vec![
            PromotedColumn {
                attr_key: "region".into(),
                name: "region".into(),
                ty: PromotedType::Utf8,
            },
            PromotedColumn {
                attr_key: "http.status_code".into(),
                name: "status".into(),
                ty: PromotedType::Int64,
            },
        ]);
    ice.ensure_promoted_columns().await.unwrap();

    let specs = [
        ("eu-west-1", 200_i64),
        ("us-east-2", 200_i64),
        ("eu-west-1", 404_i64),
        ("us-west-1", 500_i64),
        ("eu-west-1", 503_i64),
        ("us-east-2", 503_i64),
        ("eu-west-1", 504_i64),
        ("ap-south-1", 200_i64),
    ];
    let events: Vec<Event> = specs
        .iter()
        .enumerate()
        .map(|(i, (region, status))| {
            let mut event = Event::now(format!("row-{i}")).with_attributes(Some(format!(
                r#"{{"region":"{region}","http.status_code":{status}}}"#
            )));
            event.host = format!("host-{i}");
            event
        })
        .collect();
    ice.append_events(&events).await.unwrap();

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();

    let total = count_value(&ctx, "SELECT count(*) AS n FROM events").await;
    assert_eq!(total, specs.len() as i64);
    let total_plan =
        explain_physical(&ctx.sql("SELECT count(*) AS n FROM events").await.unwrap()).await;
    assert!(
        total_plan.contains("PlaceholderRowExec"),
        "unfiltered count(*) should stay on the zero-scan stats path:\n{total_plan}"
    );
    assert!(
        !total_plan.contains("SiglakeIcebergTableScan"),
        "unfiltered count(*) must not scan:\n{total_plan}"
    );

    let group_by_region = grouped_string_counts(
        &ctx,
        "SELECT region, count(*) AS n FROM events GROUP BY region",
    )
    .await;
    let region_eq_sql = "SELECT count(*) AS n FROM events WHERE region = 'eu-west-1'";
    let region_eq_plan = explain_physical(&ctx.sql(region_eq_sql).await.unwrap()).await;
    assert!(
        region_eq_plan.contains("SiglakeIcebergTableScan"),
        "filtered count(*) must fall back to a scan:\n{region_eq_plan}"
    );
    assert!(
        !region_eq_plan.contains("PlaceholderRowExec"),
        "filtered count(*) must not be answered from unfiltered stats:\n{region_eq_plan}"
    );
    let region_eq_count = count_value(&ctx, region_eq_sql).await;
    assert_eq!(region_eq_count, *group_by_region.get("eu-west-1").unwrap());
    assert!(region_eq_count < total);

    let group_by_status = grouped_i64_counts(
        &ctx,
        "SELECT status, count(*) AS n FROM events GROUP BY status",
    )
    .await;
    let range_sql = "SELECT count(*) AS n FROM events WHERE status >= 500 AND status < 600";
    let range_count = count_value(&ctx, range_sql).await;
    let expected_range = group_by_status
        .iter()
        .filter(|(status, _)| **status >= 500 && **status < 600)
        .map(|(_, count)| *count)
        .sum::<i64>();
    assert_eq!(range_count, expected_range);
    assert!(range_count < total);

    let in_sql = "SELECT count(*) AS n FROM events WHERE region IN ('eu-west-1', 'us-east-2')";
    let in_count = count_value(&ctx, in_sql).await;
    let expected_in = ["eu-west-1", "us-east-2"]
        .into_iter()
        .map(|region| group_by_region.get(region).copied().unwrap_or_default())
        .sum::<i64>();
    assert_eq!(in_count, expected_in);
    assert!(in_count < total);
}

#[tokio::test]
async fn append_and_query_single_batch() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);

    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let events: Vec<Event> = (0..10).map(|i| Event::now(format!("e{i}"))).collect();
    let n = ice.append_events(&events).await.unwrap();
    assert_eq!(n, 10);

    // Verify the SQL catalog file and at least one Parquet data file plus a
    // metadata.json landed on disk under the warehouse.
    let files = list_files(&warehouse);
    assert!(
        files
            .iter()
            .any(|p| p.file_name().and_then(|n| n.to_str()) == Some("_catalog.db")),
        "expected SQLite catalog db: {files:#?}",
    );
    assert!(
        files
            .iter()
            .any(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet")),
        "expected at least one .parquet file: {files:#?}",
    );
    assert!(
        files
            .iter()
            .any(|p| p.extension().and_then(|e| e.to_str()) == Some("json")
                && p.to_string_lossy().contains("metadata")),
        "expected at least one Iceberg metadata.json file: {files:#?}",
    );

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 10);
}

/// The 2026-09-06 timestamp contract, asserted at the level external engines
/// actually fail at: the PHYSICAL Parquet types in the written data file.
///
/// Spark rejects a raw siglake Parquet file whose `timestamp` is INT64
/// TIMESTAMP(NANOS) — its `TimestampType` is microseconds and Iceberg 1.11 has
/// no nanosecond case for Spark 3.5/4.0/4.1. So the contract is only real if
/// the bytes on disk say MICROS with isAdjustedToUTC, and the exact nanosecond
/// rides alongside as a plain INT64 with no timestamp annotation (which every
/// engine reads as a number). Checking the Arrow or Iceberg schema alone would
/// not catch a writer that re-annotates on the way out.
#[tokio::test]
async fn written_parquet_uses_micros_utc_timestamp_and_a_plain_int64_sibling() {
    use parquet::basic::{
        ConvertedType, LogicalType, TimeUnit as ParquetTimeUnit, Type as PhysType,
    };
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);

    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let events: Vec<Event> = (0..8).map(|i| Event::now(format!("e{i}"))).collect();
    ice.append_events(&events).await.unwrap();

    let data_file = list_files(&warehouse)
        .into_iter()
        .find(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
        .expect("a parquet data file was written");
    let reader = SerializedFileReader::new(std::fs::File::open(&data_file).unwrap()).unwrap();
    let schema = reader.metadata().file_metadata().schema_descr();
    let column = |name: &str| {
        schema
            .columns()
            .iter()
            .find(|c| c.path().string() == name)
            .unwrap_or_else(|| panic!("data file has no `{name}` column"))
            .clone()
    };

    let ts = column("timestamp");
    assert_eq!(
        ts.physical_type(),
        PhysType::INT64,
        "timestamp must be an INT64"
    );
    match ts.logical_type_ref() {
        Some(LogicalType::Timestamp {
            is_adjusted_to_u_t_c,
            unit,
        }) => {
            assert!(
                matches!(unit, ParquetTimeUnit::MICROS),
                "timestamp must be TIMESTAMP(MICROS), got {unit:?} — Spark cannot map NANOS"
            );
            assert!(
                *is_adjusted_to_u_t_c,
                "timestamp must be isAdjustedToUTC=true, i.e. an Iceberg `timestamptz`"
            );
        }
        other => panic!("timestamp logical type is {other:?}, expected TIMESTAMP(MICROS, UTC)"),
    }

    // The sibling is a bare number, deliberately: annotating it as a timestamp
    // would reintroduce the nanosecond type the contract exists to avoid.
    let ns = column(siglake_core::TIMESTAMP_NS_COLUMN);
    assert_eq!(
        ns.physical_type(),
        PhysType::INT64,
        "timestamp_ns must be an INT64"
    );
    assert!(
        !matches!(ns.logical_type_ref(), Some(LogicalType::Timestamp { .. })),
        "timestamp_ns must carry no timestamp annotation, got {:?}",
        ns.logical_type_ref()
    );
    assert_eq!(
        ns.converted_type(),
        ConvertedType::NONE,
        "timestamp_ns must carry no converted type either"
    );

    // Declared order == on-disk order: the footer claims BOTH keys, so a reader
    // may trust that `(timestamp, timestamp_ns)` is total within the file.
    let sorting: Vec<i32> = reader
        .metadata()
        .row_group(0)
        .sorting_columns()
        .expect("row group stamps SortingColumn metadata")
        .iter()
        .map(|s| s.column_idx)
        .collect();
    let leaf_of = |name: &str| {
        schema
            .columns()
            .iter()
            .position(|c| c.path().string() == name)
            .unwrap() as i32
    };
    assert_eq!(
        sorting,
        vec![
            leaf_of("timestamp"),
            leaf_of(siglake_core::TIMESTAMP_NS_COLUMN)
        ],
        "footer must declare (timestamp, timestamp_ns) ascending"
    );
}

/// SPIKE (read-path ownership): now that the iceberg Parquet read path is
/// vendored in-tree, we can observe ACTUAL object-store bytes fetched by a scan
/// — impossible through stock iceberg-rust 0.9. A real column projection must
/// fetch the footer + the column chunk, so the in-reader counter must advance.
/// Uses a monotonic before/after delta (no reset) so it is robust to other
/// tests scanning concurrently in the same process.
#[tokio::test]
async fn vendored_reader_counts_object_store_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);

    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let events: Vec<Event> = (0..1000)
        .map(|i| {
            Event::now(format!(
                "event number {i} carrying some payload text to decode"
            ))
        })
        .collect();
    ice.append_events(&events).await.unwrap();

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();

    let before = iceberg::arrow::object_store_bytes_read();
    // Project a data column so the scan must read Parquet bytes (not answer
    // from metadata as a bare count(*) might).
    let df = ctx.sql("SELECT raw FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1000);
    let after = iceberg::arrow::object_store_bytes_read();

    assert!(
        after > before,
        "vendored reader counted no object-store bytes for the scan: before={before} after={after}"
    );
    eprintln!(
        "vendored reader counted {} object-store bytes for the raw-column scan",
        after - before
    );
}

/// CORRECTNESS: raw token-bloom file skipping must NEVER drop a matching row.
/// Two files with disjoint raw content get distinct blooms; a `raw LIKE` query
/// must return exactly the rows that match — a write/read format mismatch (which
/// would false-negative and skip a file that DOES contain the term) would make
/// the count too low and fail this test.
#[tokio::test]
async fn raw_token_bloom_skip_is_correct() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    // Two separate appends => two data files with disjoint raw tokens.
    let alpha: Vec<Event> = (0..10)
        .map(|i| Event::now(format!("alpha alpha entry {i}")))
        .collect();
    let bravo: Vec<Event> = (0..7)
        .map(|i| Event::now(format!("bravo bravo entry {i}")))
        .collect();
    ice.append_events(&alpha).await.unwrap();
    ice.append_events(&bravo).await.unwrap();

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();

    async fn count(ctx: &SessionContext, sql: &str) -> i64 {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0)
    }

    // 'alpha' only in file A: the B file's bloom must skip without dropping A's rows.
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%alpha%'"
        )
        .await,
        10,
        "bloom skip dropped matching 'alpha' rows (write/read mismatch?)"
    );
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%bravo%'"
        )
        .await,
        7,
    );
    // 'entry' is in both files — nothing may be skipped.
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%entry%'"
        )
        .await,
        17,
    );
    // A term in neither file — every file may be skipped; result is 0.
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%charlie%'"
        )
        .await,
        0,
    );
}

#[tokio::test]
async fn multiple_appends_visible_after_re_register() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);

    let ice = IcebergContext::open(&warehouse).await.unwrap();

    let batch_a: Vec<Event> = (0..5).map(|i| Event::now(format!("a{i}"))).collect();
    let batch_b: Vec<Event> = (0..7).map(|i| Event::now(format!("b{i}"))).collect();
    let batch_c: Vec<Event> = (0..3).map(|i| Event::now(format!("c{i}"))).collect();

    ice.append_events(&batch_a).await.unwrap();
    ice.append_events(&batch_b).await.unwrap();
    ice.append_events(&batch_c).await.unwrap();

    // Fresh SessionContext + register reads the latest snapshot.
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(
        arr.value(0),
        15,
        "expected 5+7+3=15 rows across three commits"
    );
}

/// BIG-4 lever-2: the commit path threads the already-loaded base table into
/// `Catalog::update_table_with_base`, so the vendored SQL catalog no longer
/// re-reads `metadata.json` from object storage on every commit. The base also
/// tightens the optimistic-concurrency lock: each commit's `WHERE
/// metadata_location = ?` is validated against the exact metadata its diff was
/// computed against.
///
/// This is the regression guard for that change. A correct base-threaded commit
/// must, on every sequential append: (a) advance the table to a fresh, distinct
/// `metadata_location` (the optimistic-lock UPDATE matched the base and a new
/// metadata.json was written), (b) add exactly one snapshot with a new current
/// id, and (c) conserve rows. A broken base (stale location, wrong table) would
/// make the lock miss → `rows_affected == 0` → a retryable conflict that
/// exhausts the backoff and fails the append.
#[tokio::test]
async fn base_threaded_commit_advances_metadata_each_append() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let ident = ice.events_table_ident().clone();

    // Baseline: the freshly-created table before any append.
    let base = ice.catalog().load_table(&ident).await.unwrap();
    let mut last_location = base
        .metadata_location()
        .expect("created table has a metadata location")
        .to_string();
    let mut last_snaps = base.metadata().snapshots().count();
    let mut last_current = base.metadata().current_snapshot_id();
    let mut seen_locations = std::collections::HashSet::new();
    seen_locations.insert(last_location.clone());

    let mut total = 0usize;
    for i in 0..5 {
        let batch: Vec<Event> = (0..=i).map(|j| Event::now(format!("c{i}-{j}"))).collect();
        total += batch.len();
        // append_events → append_batch → append_to_table → Transaction::commit
        // → do_commit → Catalog::update_table_with_base (the lever-2 path).
        ice.append_events(&batch).await.unwrap();

        let t = ice.catalog().load_table(&ident).await.unwrap();
        let loc = t.metadata_location().unwrap().to_string();
        let snaps = t.metadata().snapshots().count();
        let current = t.metadata().current_snapshot_id();

        assert_ne!(
            loc, last_location,
            "commit {i} must advance the metadata location (the optimistic-lock \
             UPDATE matched the threaded base and wrote a new metadata.json)"
        );
        assert!(
            seen_locations.insert(loc.clone()),
            "commit {i} reused a previous metadata location {loc}"
        );
        assert_eq!(
            snaps,
            last_snaps + 1,
            "commit {i} must add exactly one snapshot"
        );
        assert_ne!(
            current, last_current,
            "commit {i} must install a new current snapshot id"
        );

        last_location = loc;
        last_snaps = snaps;
        last_current = current;
    }

    // Every base-threaded commit landed; rows are conserved exactly.
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, total as i64, "1+2+3+4+5 = 15 rows across five commits");
}

/// BIG-4 `#4c`: snapshot-metadata expiry must bound the `snapshots`
/// array (the dominant per-commit catalog cost) WITHOUT dropping a row or
/// disturbing the current snapshot. Non-destructive: it only removes
/// snapshot metadata, never the live read path.
#[tokio::test]
async fn expire_snapshots_bounds_history_and_conserves_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let ident = ice.events_table_ident().clone();

    // Six appends ⇒ six snapshots, with a known total row count.
    let mut total = 0usize;
    for i in 0..6 {
        let batch: Vec<Event> = (0..=i).map(|j| Event::now(format!("s{i}-{j}"))).collect();
        total += batch.len();
        ice.append_events(&batch).await.unwrap();
    }

    let before = ice.catalog().load_table(&ident).await.unwrap();
    let before_snaps = before.metadata().snapshots().count();
    let before_current = before.metadata().current_snapshot_id();
    assert!(
        before_snaps >= 6,
        "expected >=6 snapshots, got {before_snaps}"
    );

    // Retain the 2 most-recent; the rest (minus current/refs) expire.
    let expired = ice.expire_snapshots(&ident, 2).await.unwrap();
    assert!(expired >= 1, "should have expired old snapshots");

    let after = ice.catalog().load_table(&ident).await.unwrap();
    let after_snaps = after.metadata().snapshots().count();
    assert_eq!(
        before_snaps - after_snaps,
        expired,
        "snapshot-count drop must equal the reported expired count"
    );
    assert_eq!(
        after.metadata().current_snapshot_id(),
        before_current,
        "current snapshot must be retained — live read path unaffected"
    );

    // Rows are conserved exactly — expiry touches metadata only.
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, total as i64, "expiry must not drop any rows");

    // Re-running at the same retention is a clean no-op (no commit).
    assert_eq!(
        ice.expire_snapshots(&ident, 2).await.unwrap(),
        0,
        "second pass at the same retention expires nothing"
    );
}

/// Multi-day batches must fan out across daily partitions: one Parquet
/// file per day, each in its own `day_ts=YYYY-MM-DD` directory under the
/// table's `data/` root. This is what gives time-range queries
/// manifest-level pruning.
#[tokio::test]
async fn multi_day_batch_fans_out_per_day() {
    use chrono::{Duration as ChronoDuration, TimeZone, Utc};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);

    let ice = IcebergContext::open(&warehouse).await.unwrap();

    // 3 events spread across 3 distinct days.
    let base = Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap();
    let events: Vec<Event> = (0..3)
        .map(|i| {
            let mut e = Event::now(format!("day-{i}"));
            e.timestamp = base + ChronoDuration::days(i as i64);
            e
        })
        .collect();
    ice.append_events(&events).await.unwrap();

    // Walk the data/ directory; expect three distinct partition subdirs.
    let data_root = warehouse.join("siglake/events/data");
    let mut partition_dirs: Vec<String> = Vec::new();
    let mut parquet_count = 0usize;
    for entry in list_files(&data_root) {
        if entry.extension().and_then(|e| e.to_str()) == Some("parquet") {
            parquet_count += 1;
            // Capture the parent directory name (the partition value).
            if let Some(parent) = entry
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str())
            {
                partition_dirs.push(parent.to_string());
            }
        }
    }
    assert_eq!(
        parquet_count, 3,
        "expected one parquet per day, got {parquet_count}"
    );
    partition_dirs.sort();
    partition_dirs.dedup();
    assert_eq!(
        partition_dirs.len(),
        3,
        "expected 3 distinct day partitions, got: {partition_dirs:?}"
    );
    for d in &partition_dirs {
        assert!(
            d.starts_with("day_ts="),
            "partition dir should be `day_ts=...`, got `{d}`"
        );
    }
}

/// Closing an [`IcebergContext`] and reopening it on the same warehouse must
/// return the data that the first instance committed. This is the regression
/// test that guards us against the in-memory `MemoryCatalog` failure mode
/// that bit us before the SQL catalog pivot — if someone swaps the catalog
/// impl back to anything that doesn't persist to disk, this test will fail.
#[tokio::test]
async fn data_persists_across_context_drop() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);

    {
        let ice = IcebergContext::open(&warehouse).await.unwrap();
        let events: Vec<Event> = (0..42).map(|i| Event::now(format!("e{i}"))).collect();
        ice.append_events(&events).await.unwrap();
    } // Drop the context — closes the sqlx pool.

    // Reopen against the same warehouse. The SQLite catalog file and the
    // committed snapshot must be visible to the new instance.
    let ice2 = IcebergContext::open(&warehouse).await.unwrap();
    let ctx = SessionContext::new();
    ice2.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 42, "data must persist across context drops");
}

#[tokio::test]
async fn group_by_host_aggregation() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);

    let ice = IcebergContext::open(&warehouse).await.unwrap();

    let events: Vec<Event> = (0..20)
        .map(|i| {
            let mut e = Event::now(format!("e{i}"));
            e.host = format!("host-{}", i % 4);
            e
        })
        .collect();
    ice.append_events(&events).await.unwrap();

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx
        .sql("SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY host")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 4, "expected 4 host groups");
    let n = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    for i in 0..4 {
        assert_eq!(n.value(i), 5, "host {i} should have 5 events");
    }
}

/// DATA-INTEGRITY GATE for the tier-2 re-clustering rewrite (vendored Iceberg
/// `Overwrite` action). Appends three data files whose time ranges INTERLEAVE
/// (a pessimal out-of-order layout), re-clusters all three into time-contiguous
/// file(s), and proves the overwrite conserves every row exactly once — no loss,
/// no duplication — while physically replacing the original files. Then proves
/// the safety guard: re-running against the now-stale file handles FAILS rather
/// than silently duplicating rows.
#[tokio::test]
async fn recluster_files_conserves_rows_and_replaces_files() {
    use std::collections::HashSet;

    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use siglake_storage::iceberg::BLOOM_FILTER_COLUMNS;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    // Noon today, so every offset below stays within one day partition (no
    // midnight crossing => deterministic single-partition re-cluster).
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    let mk = |offsets: &[i64], term: &str| -> Vec<Event> {
        offsets
            .iter()
            .enumerate()
            .map(|(i, &s)| {
                let mut e = Event::now(format!("{term} entry {i}"));
                e.timestamp = base + Duration::seconds(s);
                e
            })
            .collect()
    };
    // Three separate appends => three files. The time ranges interleave:
    // alpha=[10,30,50], bravo=[20,40], charlie=[5,60] — so file-level min/max
    // pruning is useless until they are re-clustered.
    ice.append_events(&mk(&[10, 30, 50], "alpha"))
        .await
        .unwrap();
    ice.append_events(&mk(&[20, 40], "bravo")).await.unwrap();
    ice.append_events(&mk(&[5, 60], "charlie")).await.unwrap();

    let ident = ice.events_table_ident().clone();
    let files = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(files.len(), 3, "expected one data file per append");
    let total_rows: u64 = files.iter().map(|f| f.record_count()).sum();
    assert_eq!(total_rows, 7);
    let old_paths: HashSet<String> = files.iter().map(|f| f.file_path().to_string()).collect();

    // Re-cluster all three into time-contiguous file(s).
    let stats = ice
        .recluster_files(&ident, files.clone(), BLOOM_FILTER_COLUMNS)
        .await
        .unwrap();
    assert_eq!(stats.files_removed, 3);
    assert_eq!(stats.rows, 7);
    assert!(
        stats.files_added >= 1 && stats.files_added < 3,
        "re-clustering should reduce the file count: {stats:?}"
    );

    // The live set is now exactly the new files; none of the originals survive.
    let after = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(after.len(), stats.files_added);
    assert_eq!(after.iter().map(|f| f.record_count()).sum::<u64>(), 7);
    for f in &after {
        assert!(
            !old_paths.contains(f.file_path()),
            "stale file still live after rewrite: {}",
            f.file_path()
        );
    }

    // No row lost or duplicated, content fully intact across the overwrite.
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    async fn count(ctx: &SessionContext, sql: &str) -> i64 {
        let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        b[0].column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0)
    }
    assert_eq!(count(&ctx, "SELECT count(*) AS n FROM events").await, 7);
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%alpha%'"
        )
        .await,
        3
    );
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%bravo%'"
        )
        .await,
        2
    );
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%charlie%'"
        )
        .await,
        2
    );

    // SAFETY GUARD: re-running with the now-stale handles must FAIL the live-file
    // check, never silently duplicate. The physical files still exist on disk, so
    // the failure must originate from the manifest rewrite's accounting guard.
    let stale = ice
        .recluster_files(&ident, files, BLOOM_FILTER_COLUMNS)
        .await;
    assert!(
        stale.is_err(),
        "re-clustering already-removed files must fail the live-file guard"
    );

    // The failed attempt left the committed table untouched.
    let ctx2 = SessionContext::new();
    ice.register_with_datafusion(&ctx2).await.unwrap();
    assert_eq!(count(&ctx2, "SELECT count(*) AS n FROM events").await, 7);
}

/// The bounded selection pass must heal only a capped slice per pass (the safety
/// valve against pessimal out-of-order ingest) while conserving every row.
#[tokio::test]
async fn recluster_pass_is_bounded_and_conserves_rows() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use siglake_storage::iceberg::{ReclusterPolicy, BLOOM_FILTER_COLUMNS};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    // Six small files in one day partition.
    for k in 0..6i64 {
        let mut e = Event::now(format!("payload {k}"));
        e.timestamp = base + Duration::seconds(k * 7);
        ice.append_events(std::slice::from_ref(&e)).await.unwrap();
    }
    let ident = ice.events_table_ident().clone();
    assert_eq!(ice.live_data_files(&ident).await.unwrap().len(), 6);

    // Cap the pass at 4 files; everything is small so the size/row caps won't
    // bind and all 4 pack into a single target-sized bin.
    let policy = ReclusterPolicy {
        min_files_per_partition: 4,
        target_file_bytes: 128 * 1024 * 1024,
        cold_target_file_bytes: 256 * 1024 * 1024,
        cold_age_secs: 7 * 24 * 3600,
        max_files_per_pass: 4,
        max_pass_bytes: 512 * 1024 * 1024,
        max_pass_rows: 2_000_000,
        max_bins_per_pass: 4,
        max_window_ns: None,
    };
    let stats = ice
        .recluster_pass(&ident, BLOOM_FILTER_COLUMNS, policy)
        .await
        .unwrap();

    // One bin produced, bounded to the 4-file per-pass cap -> 1 output.
    assert_eq!(stats.len(), 1, "one bin should be produced");
    assert_eq!(
        stats[0].files_removed, 4,
        "pass must respect the 4-file cap"
    );
    assert_eq!(stats[0].files_added, 1);
    // 6 original - 4 removed + 1 added = 3 live files; all 6 rows conserved.
    let after = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(after.len(), 3);
    assert_eq!(after.iter().map(|f| f.record_count()).sum::<u64>(), 6);

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let b = ctx
        .sql("SELECT count(*) AS n FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        b[0].column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
        6
    );
}

/// A.4.2 leveled compaction: the bounded per-level pass must (a) leave a level
/// alone while it's under the file trigger, (b) merge a level once it reaches the
/// trigger, bounded by the fan-in cap, and (c) conserve every row. Small tiny
/// files all land at L0 (well under the default 128 MiB ceiling), so this drives
/// the L0→L1 step with a low trigger + tiny fan-in.
#[tokio::test]
async fn leveled_pass_respects_trigger_and_fanin() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use siglake_storage::iceberg::{
        LevelPolicy, LeveledPassOptions, ReclusterPolicy, BLOOM_FILTER_COLUMNS,
    };

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    // Six tiny (KB-scale) files, all at L0 under any sane ceiling.
    for k in 0..6i64 {
        let mut e = Event::now(format!("payload {k}"));
        e.timestamp = base + Duration::seconds(k * 7);
        ice.append_events(std::slice::from_ref(&e)).await.unwrap();
    }
    let ident = ice.events_table_ident().clone();
    assert_eq!(ice.live_data_files(&ident).await.unwrap().len(), 6);

    let policy = ReclusterPolicy {
        max_files_per_pass: 64,
        max_pass_bytes: 512 * 1024 * 1024,
        max_pass_rows: 2_000_000,
        max_bins_per_pass: 4,
        ..Default::default()
    };

    // (a) Trigger 8 > 6 files at L0 ⇒ under the gate ⇒ no compaction.
    let levels_high = LevelPolicy {
        level_ceilings: vec![128 * 1024 * 1024, 1024 * 1024 * 1024],
        trigger_files: 8,
        max_fanin: 64,
        max_merge_gen: 0,
        max_overlap_depth: 0,
    };
    let stats = ice
        .recluster_pass_leveled(
            &ident,
            BLOOM_FILTER_COLUMNS,
            &levels_high,
            policy,
            &LeveledPassOptions::default(),
        )
        .await
        .unwrap();
    assert!(
        stats.is_empty(),
        "level under the trigger must not be compacted"
    );
    assert_eq!(ice.live_data_files(&ident).await.unwrap().len(), 6);

    // (b) Trigger 4 ≤ 6, fan-in 4 ⇒ merge exactly 4 of the L0 files into one.
    let levels_low = LevelPolicy {
        level_ceilings: vec![128 * 1024 * 1024, 1024 * 1024 * 1024],
        trigger_files: 4,
        max_fanin: 4,
        max_merge_gen: 0,
        max_overlap_depth: 0,
    };
    let stats = ice
        .recluster_pass_leveled(
            &ident,
            BLOOM_FILTER_COLUMNS,
            &levels_low,
            policy,
            &LeveledPassOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(stats.len(), 1, "one bounded level compaction");
    assert_eq!(
        stats[0].files_removed, 4,
        "fan-in cap bounds the merge to 4 files"
    );
    assert_eq!(stats[0].files_added, 1);

    // (c) 6 - 4 + 1 = 3 live files; all six rows conserved.
    let after = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(after.len(), 3);
    assert_eq!(after.iter().map(|f| f.record_count()).sum::<u64>(), 6);

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let b = ctx
        .sql("SELECT count(*) AS n FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        b[0].column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
        6
    );
}

/// A.4.3 `only_level`: the throttled backpressure slot restricts the leveled pass
/// to one level (L0) so under load it does only cheap small-file reduction and
/// defers the expensive higher levels. All tiny appended files are L0, so a pass
/// pinned to level 1 must be a no-op, while level 0 compacts — proving the
/// restriction actually gates on level, not just pressure.
#[tokio::test]
async fn leveled_only_level_restricts_the_pass() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use siglake_storage::iceberg::{
        LevelPolicy, LeveledPassOptions, ReclusterPolicy, BLOOM_FILTER_COLUMNS,
    };

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    for k in 0..6i64 {
        let mut e = Event::now(format!("payload {k}"));
        e.timestamp = base + Duration::seconds(k * 7);
        ice.append_events(std::slice::from_ref(&e)).await.unwrap();
    }
    let ident = ice.events_table_ident().clone();
    assert_eq!(ice.live_data_files(&ident).await.unwrap().len(), 6);

    let levels = LevelPolicy {
        level_ceilings: vec![128 * 1024 * 1024, 1024 * 1024 * 1024],
        trigger_files: 4,
        max_fanin: 64,
        max_merge_gen: 0,
        max_overlap_depth: 0,
    };
    let policy = ReclusterPolicy {
        max_bins_per_pass: 1,
        ..Default::default()
    };

    // Pinned to L1: no L1-sized files exist ⇒ no-op.
    let stats = ice
        .recluster_pass_leveled(
            &ident,
            BLOOM_FILTER_COLUMNS,
            &levels,
            policy,
            &LeveledPassOptions {
                allowed_levels: Some(vec![1]),
                max_total_bins: None,
                preempt: None,
                on_bin: None,
                bin_concurrency: None,
            },
        )
        .await
        .unwrap();
    assert!(
        stats.is_empty(),
        "only_level=1 must not touch the all-L0 layout"
    );
    assert_eq!(ice.live_data_files(&ident).await.unwrap().len(), 6);

    // Pinned to L0: compacts the cheap small files.
    let stats = ice
        .recluster_pass_leveled(
            &ident,
            BLOOM_FILTER_COLUMNS,
            &levels,
            policy,
            &LeveledPassOptions {
                allowed_levels: Some(vec![0]),
                max_total_bins: Some(1),
                preempt: None,
                on_bin: None,
                bin_concurrency: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(stats.len(), 1, "only_level=0 compacts the L0 files");
    assert!(stats[0].files_removed >= 2);
    assert_eq!(
        ice.live_data_files(&ident)
            .await
            .unwrap()
            .iter()
            .map(|f| f.record_count())
            .sum::<u64>(),
        6,
        "rows conserved"
    );
}

/// #63 lull freshness: a leveled pass with a `preempt` signal yields BETWEEN
/// BINS — a sealed segment arriving mid-pass waits for at most one bin's merge,
/// not the whole pass. Two day-partitions each holding an L0 cluster give the
/// pass two bins; a preempt that flips true after the first check must stop the
/// pass at exactly one merged bin, and a follow-up un-preempted pass finishes
/// the remaining bin (no work is lost, only deferred).
#[tokio::test]
async fn leveled_pass_preempts_between_bins() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use siglake_storage::iceberg::{
        LevelPolicy, LeveledPassOptions, ReclusterPolicy, BLOOM_FILTER_COLUMNS,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    // Two partitions (today + tomorrow), 4 tiny L0 files each ⇒ two bins.
    for day in 0..2i64 {
        for k in 0..4i64 {
            let mut e = Event::now(format!("payload d{day} k{k}"));
            e.timestamp = base + Duration::days(day) + Duration::seconds(k * 7);
            ice.append_events(std::slice::from_ref(&e)).await.unwrap();
        }
    }
    let ident = ice.events_table_ident().clone();
    assert_eq!(ice.live_data_files(&ident).await.unwrap().len(), 8);

    let levels = LevelPolicy {
        level_ceilings: vec![128 * 1024 * 1024, 1024 * 1024 * 1024],
        trigger_files: 4,
        max_fanin: 64,
        max_merge_gen: 0,
        max_overlap_depth: 0,
    };
    let policy = ReclusterPolicy::default();

    // Preempt flips true after the first between-bins check: bin 1 merges,
    // bin 2 is deferred to a later pass.
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in = calls.clone();
    let opts = LeveledPassOptions {
        allowed_levels: Some(vec![0]),
        max_total_bins: None,
        preempt: Some(Arc::new(move || {
            calls_in.fetch_add(1, Ordering::SeqCst) >= 1
        })),
        on_bin: None,
        bin_concurrency: None,
    };
    let stats = ice
        .recluster_pass_leveled(&ident, BLOOM_FILTER_COLUMNS, &levels, policy, &opts)
        .await
        .unwrap();
    assert_eq!(stats.len(), 1, "preempted pass merges exactly one bin");
    assert!(
        calls.load(Ordering::SeqCst) >= 2,
        "preempt checked before each bin"
    );
    let after_first = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(
        after_first.len(),
        5,
        "one 4-file bin merged to 1, the other deferred untouched"
    );

    // No preemption: the deferred bin merges; nothing was lost.
    let opts = LeveledPassOptions {
        allowed_levels: Some(vec![0]),
        max_total_bins: None,
        preempt: None,
        on_bin: None,
        bin_concurrency: None,
    };
    let stats = ice
        .recluster_pass_leveled(&ident, BLOOM_FILTER_COLUMNS, &levels, policy, &opts)
        .await
        .unwrap();
    assert_eq!(stats.len(), 1, "follow-up pass merges the deferred bin");
    let final_files = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(final_files.len(), 2, "both partitions consolidated");
    assert_eq!(
        final_files.iter().map(|f| f.record_count()).sum::<u64>(),
        8,
        "rows conserved across the preempted + resumed passes"
    );
}

/// Depth trigger (1TB shakeout): a converged layout can hold FEWER than
/// `trigger_files` files per level that all mutually overlap — count triggers
/// never fire, the stack persists, and ordered scans need env-raised fan-in
/// caps. With `max_overlap_depth` set, the leveled pass must merge the deepest
/// stab-point cluster anyway; with it 0 (disabled), the same layout must be
/// left alone (the pre-feature behavior).
#[tokio::test]
async fn depth_trigger_merges_deep_stack_below_count_trigger() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use siglake_storage::iceberg::{
        LevelPolicy, LeveledPassOptions, ReclusterPolicy, BLOOM_FILTER_COLUMNS,
    };

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    // 4 files, ALL spanning the same ~[0s, 90s] range → overlap depth 4, but
    // only 4 L0 files (< trigger_files = 8): the count trigger never fires.
    for k in 0..4i64 {
        let events: Vec<Event> = (0..4)
            .map(|j| {
                let mut e = Event::now(format!("stack {k} {j}"));
                e.timestamp = base + Duration::seconds(j * 30 + k);
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
    let ident = ice.events_table_ident().clone();
    assert_eq!(ice.live_data_files(&ident).await.unwrap().len(), 4);

    let policy = ReclusterPolicy::default();
    let mut levels = LevelPolicy {
        level_ceilings: vec![128 * 1024 * 1024, 1024 * 1024 * 1024],
        trigger_files: 8,
        max_fanin: 64,
        max_merge_gen: 0,
        max_overlap_depth: 0,
    };

    // Disabled: the deep stack is untouched (pre-feature behavior).
    let stats = ice
        .recluster_pass_leveled(
            &ident,
            BLOOM_FILTER_COLUMNS,
            &levels,
            policy,
            &LeveledPassOptions::default(),
        )
        .await
        .unwrap();
    assert!(stats.is_empty(), "depth trigger disabled ⇒ no bins");
    assert_eq!(ice.live_data_files(&ident).await.unwrap().len(), 4);

    // Enabled at 3: depth 4 > 3 ⇒ the stack merges even below the count trigger.
    levels.max_overlap_depth = 3;
    let stats = ice
        .recluster_pass_leveled(
            &ident,
            BLOOM_FILTER_COLUMNS,
            &levels,
            policy,
            &LeveledPassOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(stats.len(), 1, "depth trigger produces one bin");
    assert_eq!(stats[0].files_removed, 4);
    let after = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(after.len(), 1, "stack consolidated");
    assert_eq!(
        after.iter().map(|f| f.record_count()).sum::<u64>(),
        16,
        "rows conserved"
    );
}

/// Startup/refresh pre-warm: `warm_query_caches` walks the manifest + footers
/// without erroring, reports the live-file count, and leaves the query paths
/// exact (warming must be observationally free).
#[tokio::test]
async fn warm_query_caches_covers_live_files() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    for k in 0..3 {
        ice.append_events(&[Event::now(format!("warm {k}"))])
            .await
            .unwrap();
    }
    let n = ice.warm_query_caches("events").await.unwrap();
    assert_eq!(n, 3, "one live file per append");
    // Warmed caches serve the same exact result.
    let counts = ice
        .grouped_counts_with_summary("events", "sourcetype", None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counts.iter().map(|(_, c)| c).sum::<u64>(), 3);
    // Idempotent.
    assert_eq!(ice.warm_query_caches("events").await.unwrap(), 3);
}

/// Write-amplification bound: recluster outputs carry their rewrite generation in
/// the file NAME (`siglake-g<N>-…`), and a file at the generation cap that is
/// time-disjoint from its neighbors is mature — the leveled pass leaves it alone
/// instead of re-churning the same bytes forever.
#[tokio::test]
async fn rewrite_generation_is_stamped_and_caps_recompaction() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use siglake_storage::iceberg::{
        LevelPolicy, LeveledPassOptions, ReclusterPolicy, BLOOM_FILTER_COLUMNS,
    };

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    for k in 0..4i64 {
        let mut e = Event::now(format!("payload {k}"));
        e.timestamp = base + Duration::seconds(k * 7);
        ice.append_events(std::slice::from_ref(&e)).await.unwrap();
    }
    let ident = ice.events_table_ident().clone();
    let files = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(files.len(), 4);
    assert!(
        files.iter().all(|f| !f.file_path().contains("siglake-g")),
        "ingest-written files carry no generation marker"
    );

    // Rewrite of gen-0 inputs → gen-1 output, stamped in the name.
    ice.recluster_files(&ident, files, BLOOM_FILTER_COLUMNS)
        .await
        .unwrap();
    let after = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(after.len(), 1);
    assert!(
        after[0].file_path().contains("siglake-g1-"),
        "recluster output must carry generation 1, got {}",
        after[0].file_path()
    );

    // Append 3 more small files so L0 pressure exists, then run a leveled pass
    // with max_merge_gen=1: the g1 file is AT the cap and time-disjoint from
    // nothing (it overlaps nothing — the appends are later), so it must be left
    // out while the fresh gen-0 files merge among themselves.
    for k in 10..13i64 {
        let mut e = Event::now(format!("late {k}"));
        e.timestamp = base + Duration::seconds(k * 100);
        ice.append_events(std::slice::from_ref(&e)).await.unwrap();
    }
    let levels = LevelPolicy {
        level_ceilings: vec![128 * 1024 * 1024, 1024 * 1024 * 1024],
        trigger_files: 2,
        max_fanin: 64,
        max_merge_gen: 1,
        max_overlap_depth: 0,
    };
    let policy = ReclusterPolicy::default();
    let stats = ice
        .recluster_pass_leveled(
            &ident,
            BLOOM_FILTER_COLUMNS,
            &levels,
            policy,
            &LeveledPassOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(stats.len(), 1, "one merge of the fresh gen-0 files");
    assert_eq!(
        stats[0].files_removed, 3,
        "the gen-capped g1 file is excluded"
    );
    let survivors = ice.live_data_files(&ident).await.unwrap();
    assert!(
        survivors
            .iter()
            .any(|f| f.file_path().contains("siglake-g1-")),
        "the mature g1 file survives untouched"
    );
    assert_eq!(
        survivors.iter().map(|f| f.record_count()).sum::<u64>(),
        7,
        "all rows conserved across both rewrites"
    );
}

/// CORRECTNESS GATE for per-row-group token-bloom pruning (BIG-2). Writes one
/// file with two row groups of disjoint `raw` content (row group 0 = "alpha",
/// row group 1 = "bravo"). A `raw LIKE` query must prune the row group that
/// cannot contain the term WITHOUT dropping any matching row — a write/read
/// misalignment or a serialization bug would make a count too low and fail here.
#[tokio::test]
async fn rowgroup_token_bloom_prunes_without_dropping_rows() {
    // Force the row-group size to the floor (MIN_ROW_GROUP_ROWS) so the test data
    // spans two row groups. `=1` rounds down to the min clamp; harmless to other
    // tests, which already hit that clamp for their small batches.
    // Row group 0 must be entirely "alpha", so it must be exactly the clamp size.
    const RG0: usize = 128 * 1024; // == MIN_ROW_GROUP_ROWS
    const RG1: usize = 50_000;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap().with_tuning(
        siglake_storage::iceberg::IcebergTuning {
            target_row_group_bytes: Some(1),
            ..Default::default()
        },
    );

    let mut events: Vec<Event> = Vec::with_capacity(RG0 + RG1);
    for i in 0..RG0 {
        events.push(Event::now(format!("alpha alpha entry {i}")));
    }
    for i in 0..RG1 {
        events.push(Event::now(format!("bravo bravo entry {i}")));
    }
    // One append => one file. With the forced row-group floor it has two row
    // groups: [0..128K) all "alpha", [128K..178K) all "bravo".
    ice.append_events(&events).await.unwrap();

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    async fn count(ctx: &SessionContext, sql: &str) -> i64 {
        let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        b[0].column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0)
    }

    // "alpha" lives only in row group 0; the row-group bloom must skip row group 1
    // WITHOUT dropping any of the 128K matching rows.
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%alpha%'"
        )
        .await,
        RG0 as i64,
        "row-group pruning dropped matching 'alpha' rows"
    );
    // "bravo" only in row group 1.
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%bravo%'"
        )
        .await,
        RG1 as i64,
        "row-group pruning dropped matching 'bravo' rows"
    );
    // "entry" is in both row groups — neither may be pruned.
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%entry%'"
        )
        .await,
        (RG0 + RG1) as i64,
    );
    // A term in neither row group — both may be pruned; result is 0.
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%charlie%'"
        )
        .await,
        0,
    );
}

/// The decoded-memory guard (`max_pass_rows`) must cap a pass independently of
/// the file/byte caps — this is the bound that prevents the OOM round 55 found,
/// where 512 MB of compressed text expanded to multi-GB of Arrow on read.
#[tokio::test]
async fn recluster_pass_respects_row_cap() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use siglake_storage::iceberg::{ReclusterPolicy, BLOOM_FILTER_COLUMNS};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    // Six files of two rows each (12 rows total), one day partition.
    for k in 0..6i64 {
        let evs: Vec<Event> = (0..2)
            .map(|j| {
                let mut e = Event::now(format!("payload {k} {j}"));
                e.timestamp = base + Duration::seconds(k * 10 + j);
                e
            })
            .collect();
        ice.append_events(&evs).await.unwrap();
    }
    let ident = ice.events_table_ident().clone();

    // File/byte caps are generous; the 5-row PER-BIN cap is what must bind. Each
    // bin accumulates 2-row files until adding another would exceed 5 rows -> 2
    // files per bin. With 6 files of 2 rows that's 3 bins (the bin-packer heals
    // the whole partition in one pass via 3 independent, memory-bounded merges).
    let policy = ReclusterPolicy {
        min_files_per_partition: 4,
        target_file_bytes: 128 * 1024 * 1024,
        cold_target_file_bytes: 256 * 1024 * 1024,
        cold_age_secs: 7 * 24 * 3600,
        max_files_per_pass: 32,
        max_pass_bytes: 512 * 1024 * 1024,
        max_pass_rows: 5,
        max_bins_per_pass: 4,
        max_window_ns: None,
    };
    let stats = ice
        .recluster_pass(&ident, BLOOM_FILTER_COLUMNS, policy)
        .await
        .unwrap();
    assert_eq!(stats.len(), 3, "3 bins of 2 files each");
    for s in &stats {
        assert_eq!(s.rows, 4, "each bin's row cap must bind to <=5 rows");
        assert_eq!(s.files_removed, 2);
        assert_eq!(s.files_added, 1);
    }
    // 6 - 6 + 3 = 3 live files; all 12 rows conserved.
    let after = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(after.len(), 3);
    assert_eq!(after.iter().map(|f| f.record_count()).sum::<u64>(), 12);
}

/// BIG-3 orphan-GC foundation: the reachable-file set must (a) contain every
/// live data file + the manifest tree of the retained snapshots, and (b) after
/// a re-cluster + snapshot-expiry, EXCLUDE the files the rewrite superseded —
/// which remain physically on disk as the orphans the GC will reclaim. Getting
/// this set wrong means deleting live data, so it's tested exactly.
#[tokio::test]
async fn reachable_files_tracks_live_set_and_excludes_orphans() {
    use std::collections::HashSet;

    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use siglake_storage::iceberg::BLOOM_FILTER_COLUMNS;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let ident = ice.events_table_ident().clone();

    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    let mk = |offsets: &[i64], term: &str| -> Vec<Event> {
        offsets
            .iter()
            .enumerate()
            .map(|(i, &s)| {
                let mut e = Event::now(format!("{term} {i}"));
                e.timestamp = base + Duration::seconds(s);
                e
            })
            .collect()
    };
    ice.append_events(&mk(&[10, 30, 50], "alpha"))
        .await
        .unwrap();
    ice.append_events(&mk(&[20, 40], "bravo")).await.unwrap();
    ice.append_events(&mk(&[5, 60], "charlie")).await.unwrap();

    // (a) Every live data file is reachable, and the metadata tree (avro) too.
    let reachable_before = ice.reachable_files(&ident).await.unwrap();
    let live = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(live.len(), 3, "one data file per append");
    for df in &live {
        assert!(
            reachable_before.contains(df.file_path()),
            "live data file missing from reachable set: {}",
            df.file_path()
        );
    }
    assert!(
        reachable_before.iter().any(|p| p.ends_with(".avro")),
        "reachable set must include the manifest-list / manifest avro files"
    );

    // Re-cluster the three files into one, then expire down to the current
    // snapshot so the pre-rewrite snapshots (which referenced the old files)
    // drop out of metadata.
    let old_paths: Vec<String> = live.iter().map(|f| f.file_path().to_string()).collect();
    ice.recluster_files(&ident, live, BLOOM_FILTER_COLUMNS)
        .await
        .unwrap();
    ice.expire_snapshots(&ident, 1).await.unwrap();

    // (b) The superseded files are no longer reachable...
    let reachable_after = ice.reachable_files(&ident).await.unwrap();
    for p in &old_paths {
        assert!(
            !reachable_after.contains(p),
            "re-clustered-away file is still reachable (would never be GC'd): {p}"
        );
    }
    // ...but they are still physically on disk — the orphans BIG-3 reclaims.
    let on_disk: HashSet<String> = list_files(&warehouse)
        .iter()
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
        .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(str::to_string))
        .collect();
    let orphan_basenames: Vec<&str> = old_paths
        .iter()
        .filter_map(|p| p.rsplit('/').next())
        .collect();
    assert!(
        orphan_basenames.iter().all(|b| on_disk.contains(*b)),
        "re-clustered files should remain on disk as orphans: on_disk={on_disk:?} orphans={orphan_basenames:?}"
    );
    let reachable_basenames: HashSet<&str> = reachable_after
        .iter()
        .filter_map(|p| p.rsplit('/').next())
        .collect();
    for b in &orphan_basenames {
        assert!(
            !reachable_basenames.contains(*b),
            "orphan basename still in reachable set: {b}"
        );
    }

    // Rows conserved; every current data file is reachable.
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let n = ctx
        .sql("SELECT count(*) AS n FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 7, "re-cluster + expiry must conserve rows");
    for df in &ice.live_data_files(&ident).await.unwrap() {
        assert!(reachable_after.contains(df.file_path()));
    }
}

/// Build a warehouse with `events` data, re-cluster + expire so there are
/// known orphans, then return (ice, ident, warehouse, orphan_basenames).
async fn setup_orphans(
    tmp: &tempfile::TempDir,
) -> (
    siglake_storage::iceberg::IcebergContext,
    iceberg::TableIdent,
    std::path::PathBuf,
    Vec<String>,
) {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use siglake_storage::iceberg::BLOOM_FILTER_COLUMNS;

    let warehouse = warehouse_dir(tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let ident = ice.events_table_ident().clone();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    let mk = |offsets: &[i64], term: &str| -> Vec<Event> {
        offsets
            .iter()
            .enumerate()
            .map(|(i, &s)| {
                let mut e = Event::now(format!("{term} {i}"));
                e.timestamp = base + Duration::seconds(s);
                e
            })
            .collect()
    };
    ice.append_events(&mk(&[10, 30, 50], "alpha"))
        .await
        .unwrap();
    ice.append_events(&mk(&[20, 40], "bravo")).await.unwrap();
    ice.append_events(&mk(&[5, 60], "charlie")).await.unwrap();

    let files = ice.live_data_files(&ident).await.unwrap();
    let orphan_basenames: Vec<String> = files
        .iter()
        .filter_map(|f| f.file_path().rsplit('/').next().map(str::to_string))
        .collect();
    ice.recluster_files(&ident, files, BLOOM_FILTER_COLUMNS)
        .await
        .unwrap();
    ice.expire_snapshots(&ident, 1).await.unwrap();
    (ice, ident, warehouse, orphan_basenames)
}

fn parquet_basenames_on_disk(warehouse: &std::path::Path) -> std::collections::HashSet<String> {
    list_files(warehouse)
        .iter()
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
        .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(str::to_string))
        .collect()
}

/// BIG-3: dry-run reports orphans but deletes nothing; apply (min_age=0)
/// reclaims exactly the re-clustered-away files, conserves rows, leaves the
/// live files intact, and re-running is a clean no-op.
#[tokio::test]
async fn gc_orphans_dry_run_then_apply_conserves_rows() {
    use siglake_storage::iceberg::GcOptions;
    use std::time::Duration;

    let tmp = tempfile::tempdir().unwrap();
    let (ice, ident, warehouse, orphans) = setup_orphans(&tmp).await;

    // The re-clustered-away parquet are physically present pre-GC.
    let before = parquet_basenames_on_disk(&warehouse);
    assert!(
        orphans.iter().all(|b| before.contains(b)),
        "expected orphans on disk pre-GC: have={before:?} orphans={orphans:?}"
    );

    // Dry-run (default apply=false): finds orphans, deletes nothing.
    let dry = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: Duration::ZERO,
                apply: false,
            },
        )
        .await
        .unwrap();
    assert!(
        dry.orphans >= orphans.len(),
        "dry-run should find the orphans: {dry:?}"
    );
    assert!(dry.orphan_bytes > 0);
    assert_eq!(dry.deleted, 0, "dry-run must not delete");
    assert_eq!(
        parquet_basenames_on_disk(&warehouse),
        before,
        "dry-run must leave every file on disk"
    );

    // Apply: deletes exactly the orphans.
    let applied = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: Duration::ZERO,
                apply: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(applied.orphans, dry.orphans);
    assert_eq!(
        applied.deleted, applied.orphans,
        "apply deletes every orphan"
    );
    let after = parquet_basenames_on_disk(&warehouse);
    for b in &orphans {
        assert!(!after.contains(b), "orphan parquet survived GC: {b}");
    }

    // Live files survive + rows conserved + a fresh scan reads cleanly.
    for df in &ice.live_data_files(&ident).await.unwrap() {
        let bn = df.file_path().rsplit('/').next().unwrap();
        assert!(after.contains(bn), "GC deleted a LIVE file: {bn}");
    }
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let n = ctx
        .sql("SELECT count(*) AS n FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 7, "GC must conserve rows");

    // Idempotent: a second apply finds nothing.
    let again = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: Duration::ZERO,
                apply: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        again.orphans, 0,
        "re-run should be a clean no-op: {again:?}"
    );
    assert_eq!(again.deleted, 0);
}

/// BIG-3 safety window: orphans younger than `min_age` are skipped, not
/// deleted — guards the in-flight-write race.
#[tokio::test]
async fn gc_orphans_respects_min_age_safety_window() {
    use siglake_storage::iceberg::GcOptions;
    use std::time::Duration;

    let tmp = tempfile::tempdir().unwrap();
    let (ice, ident, warehouse, orphans) = setup_orphans(&tmp).await;
    let before = parquet_basenames_on_disk(&warehouse);

    // The orphans were just written, so a 1-hour window skips them all.
    let report = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: Duration::from_secs(3600),
                apply: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        report.orphans, 0,
        "fresh files must not be reclaimed under the window"
    );
    assert!(
        report.skipped_recent > 0,
        "fresh orphans should be counted as skipped: {report:?}"
    );
    assert_eq!(report.deleted, 0);
    assert_eq!(
        parquet_basenames_on_disk(&warehouse),
        before,
        "safety window must leave every file on disk"
    );
    let _ = orphans;
}

/// #8 substring search: the raw row-group bloom now indexes character TRIGRAMS,
/// so `raw LIKE '%substr%'` prunes row groups for arbitrary substrings — partial
/// tokens and cross-token (with spaces) — never dropping a matching row. This is
/// also a correctness fix: the old whole-token bloom would have MIS-pruned a
/// partial-token query like `%rror%`.
#[tokio::test]
async fn rowgroup_trigram_bloom_prunes_substrings_without_dropping_rows() {
    const RG0: usize = 128 * 1024; // MIN_ROW_GROUP_ROWS
    const RG1: usize = 50_000;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap().with_tuning(
        siglake_storage::iceberg::IcebergTuning {
            target_row_group_bytes: Some(1),
            ..Default::default()
        },
    );

    // RG0: "...error 500..."  RG1: "...warn 200..."  (disjoint substrings,
    // shared "host-" so we can also assert a both-groups substring).
    let mut events: Vec<Event> = Vec::with_capacity(RG0 + RG1);
    for i in 0..RG0 {
        events.push(Event::now(format!(
            "connection refused error 500 host-{}",
            i % 8
        )));
    }
    for i in 0..RG1 {
        events.push(Event::now(format!("request ok warn 200 host-{}", i % 8)));
    }
    ice.append_events(&events).await.unwrap();

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    async fn count(ctx: &SessionContext, sql: &str) -> i64 {
        ctx.sql(sql).await.unwrap().collect().await.unwrap()[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0)
    }

    // Cross-token substring (has a space) only in RG0 → RG1 pruned, no rows lost.
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%error 500%'"
        )
        .await,
        RG0 as i64,
        "cross-token substring pruning dropped matching rows"
    );
    // Partial-token substring ("rror" ⊂ "error"): the OLD token bloom would have
    // mis-pruned RG0 (no whole token "rror"); trigrams keep it.
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%rror%'"
        )
        .await,
        RG0 as i64,
        "partial-token substring dropped matching rows (the token-bloom bug)"
    );
    // Substring only in RG1.
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%warn 200%'"
        )
        .await,
        RG1 as i64,
    );
    // Substring present in neither row group → both pruned → 0.
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%zzx qqv%'"
        )
        .await,
        0,
    );
    // Substring in both row groups → neither pruned → all rows.
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%host-%'"
        )
        .await,
        (RG0 + RG1) as i64,
    );
}

/// #7 distributed query: a sharded scan (injected via the SessionConfig
/// ScanShard extension) sees only its slice of the table's files. The shards
/// partition rows disjointly and union to the full result, so a coordinator
/// can fan a query out across worker pods and merge. Uses a `WHERE` that forces
/// a real file scan (not a metadata count fast-path) so the file shard applies.
///
/// Shard ownership hashes the data-file path, and the path embeds the random
/// tempdir, so which shard gets which file differs per run. The expected
/// per-shard row count is therefore derived from the live-file list with the
/// same `ScanShard::owns` the provider uses, and asserted exactly — including
/// the (rare) run where every file lands in one shard, which used to trip a
/// probabilistic "at least two shards are non-empty" check.
#[tokio::test]
async fn sharded_scans_partition_rows_and_union_to_full() {
    use siglake_storage::{session_context_with, ScanShard};

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    // Several appends ⇒ several data files for the shards to split.
    let mut total = 0usize;
    for i in 0..6 {
        let batch: Vec<Event> = (0..(i + 1) * 4)
            .map(|j| Event::now(format!("s{i}-{j}")))
            .collect();
        total += batch.len();
        ice.append_events(&batch).await.unwrap();
    }
    let files = ice.live_data_files(ice.events_table_ident()).await.unwrap();
    assert_eq!(
        files.iter().map(|f| f.record_count()).sum::<u64>(),
        total as u64,
        "live files account for every appended row"
    );

    // Matches every row ("-" in each "sI-J") but forces a scan (no metadata
    // shortcut, no trigram prune since "-" is sub-trigram).
    async fn scan_count(ctx: &SessionContext) -> i64 {
        ctx.sql("SELECT count(*) AS n FROM events WHERE raw LIKE '%-%'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0)
    }

    let full_ctx = session_context_with(None, None);
    ice.register_with_datafusion(&full_ctx).await.unwrap();
    let full = scan_count(&full_ctx).await;
    assert_eq!(full, total as i64, "unsharded scan reads every row");

    const COUNT: usize = 3;
    let mut sum = 0i64;
    for index in 0..COUNT {
        let shard = ScanShard::new(index, COUNT).unwrap();
        // Exactly the rows of the files this shard owns — no more (the shard
        // filter applied) and no fewer (nothing owned was skipped).
        let expected: u64 = files
            .iter()
            .filter(|f| shard.owns(f.file_path()))
            .map(|f| f.record_count())
            .sum();
        let ctx = session_context_with(None, Some(shard));
        ice.register_with_datafusion(&ctx).await.unwrap();
        let c = scan_count(&ctx).await;
        assert_eq!(
            c, expected as i64,
            "shard {index}/{COUNT} must read exactly the files it owns"
        );
        sum += c;
    }
    assert_eq!(
        sum, full,
        "sharded scans must union to the full count (disjoint + covering)"
    );
}

/// In-fork `expire_snapshots` age sweep: `expire_snapshots_older_than` only
/// expires snapshots older than `max_age` — recent snapshots (and the current
/// one + ref targets) are always kept — and conserves every row.
#[tokio::test]
async fn expire_snapshots_age_sweep_keeps_recent_and_conserves_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let ident = ice.events_table_ident().clone();

    let mut total = 0usize;
    for i in 0..6 {
        let batch: Vec<Event> = (0..=i).map(|j| Event::now(format!("a{i}-{j}"))).collect();
        total += batch.len();
        ice.append_events(&batch).await.unwrap();
    }
    let before = ice.catalog().load_table(&ident).await.unwrap();
    let before_snaps = before.metadata().snapshots().count();
    assert!(before_snaps >= 6);

    // A 1-hour window: every snapshot was just committed, so none is "old" →
    // nothing expires even though retain_last=1 would otherwise drop most.
    let expired = ice
        .expire_snapshots_older_than(&ident, 1, std::time::Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(expired, 0, "recent snapshots must not be age-expired");
    assert_eq!(
        ice.catalog()
            .load_table(&ident)
            .await
            .unwrap()
            .metadata()
            .snapshots()
            .count(),
        before_snaps,
        "age sweep with a wide window is a no-op"
    );

    // A zero window: every existing snapshot is "older than now" → expires
    // everything beyond retain_last + the current snapshot.
    let expired = ice
        .expire_snapshots_older_than(&ident, 1, std::time::Duration::ZERO)
        .await
        .unwrap();
    assert!(expired >= 1, "max_age=0 should expire the old snapshots");
    let after = ice.catalog().load_table(&ident).await.unwrap();
    assert_eq!(
        after.metadata().current_snapshot_id(),
        before.metadata().current_snapshot_id(),
        "current snapshot must always survive the sweep"
    );

    // Rows are conserved exactly (metadata-only expiry).
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let n = ctx
        .sql("SELECT count(*) AS n FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, total as i64, "age sweep must not drop rows");
}

/// Time-ordering invariant of the storage write path: even a deliberately
/// out-of-order *direct* append (not via the compactor) lands physically
/// ordered by `timestamp` ascending on disk, and the Parquet row groups carry
/// the declared `SortingColumn` footer — so the on-disk order is a property
/// readers can trust, on every write path.
#[tokio::test]
async fn direct_append_is_time_ordered_and_declares_sorting_columns() {
    use chrono::{TimeZone, Utc};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&warehouse_dir(&tmp)).await.unwrap();

    // Shuffled timestamps within a single day (one partition file), some ties.
    let base = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
    let offsets = [7i64, 1, 5, 1, 9, 3, 0, 5, 2, 8, 4, 6];
    let events: Vec<Event> = offsets
        .iter()
        .enumerate()
        .map(|(i, off)| Event {
            timestamp: base + chrono::Duration::seconds(*off),
            host: format!("host-{}", i % 3),
            source: "src".into(),
            sourcetype: "app".into(),
            index: "main".into(),
            raw: format!("e{i}"),
            attributes: None,
        })
        .collect();
    ice.append_events(&events).await.unwrap();

    // Locate the written data file (under data/, .parquet).
    let parquet: Vec<_> = list_files(&warehouse_dir(&tmp))
        .into_iter()
        .filter(|p| {
            p.extension().is_some_and(|e| e == "parquet")
                && p.components().any(|c| c.as_os_str() == "data")
        })
        .collect();
    assert_eq!(parquet.len(), 1, "expected one data file: {parquet:?}");

    let bytes = bytes::Bytes::from(std::fs::read(&parquet[0]).unwrap());
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();

    // (1) Row groups declare the sort: `(timestamp, timestamp_ns)` ascending,
    // nulls last. Both keys, because `timestamp` is microsecond-precise since
    // the 2026-09-06 contract and the exact ns sibling is what breaks its ties.
    let ts_idx = builder
        .schema()
        .index_of("timestamp")
        .expect("timestamp column") as i32;
    let ts_ns_idx = builder
        .schema()
        .index_of(siglake_core::TIMESTAMP_NS_COLUMN)
        .expect("timestamp_ns column") as i32;
    let rg = builder.metadata().row_group(0);
    let sorting = rg
        .sorting_columns()
        .expect("row group must declare sorting columns");
    assert_eq!(sorting.len(), 2, "time-only sort order: {sorting:?}");
    assert_eq!(sorting[0].column_idx, ts_idx);
    assert_eq!(sorting[1].column_idx, ts_ns_idx);
    assert!(sorting.iter().all(|c| !c.descending), "must be ascending");
    assert!(sorting.iter().all(|c| !c.nulls_first));

    // (2) The actual rows are ascending by timestamp.
    let reader = builder.build().unwrap();
    let mut prev = i64::MIN;
    for batch in reader {
        let batch = batch.unwrap();
        let ts = siglake_core::column_nanos(
            batch
                .column_by_name(siglake_core::nanos_source_column(
                    batch.schema().as_ref(),
                    "timestamp",
                ))
                .unwrap(),
        )
        .unwrap();
        for i in 0..batch.num_rows() {
            assert!(ts.value(i) >= prev, "rows must be ascending by timestamp");
            prev = ts.value(i);
        }
    }
}

/// The `events` table is `day(timestamp)`-partitioned. A multi-day out-of-order
/// append must produce one file per day, **each internally ascending** — i.e.
/// the sort-before-partition-split preserves within-partition row order.
#[tokio::test]
async fn multi_day_append_orders_each_partition_file() {
    use chrono::{TimeZone, Utc};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&warehouse_dir(&tmp)).await.unwrap();

    // Rows interleaved across three days, shuffled within each day.
    let day0 = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
    let mut events = Vec::new();
    for (d, secs) in [(0i64, [50i64, 10, 30]), (2, [40, 5, 25]), (1, [60, 15, 35])] {
        for s in secs {
            events.push(Event {
                timestamp: day0 + chrono::Duration::days(d) + chrono::Duration::seconds(s),
                host: "h".into(),
                source: "src".into(),
                sourcetype: "app".into(),
                index: "main".into(),
                raw: "e".into(),
                attributes: None,
            });
        }
    }
    ice.append_events(&events).await.unwrap();

    let parquet: Vec<_> = list_files(&warehouse_dir(&tmp))
        .into_iter()
        .filter(|p| {
            p.extension().is_some_and(|e| e == "parquet")
                && p.components().any(|c| c.as_os_str() == "data")
        })
        .collect();
    assert_eq!(parquet.len(), 3, "expected one file per day: {parquet:?}");

    for f in parquet {
        let bytes = bytes::Bytes::from(std::fs::read(&f).unwrap());
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        let mut prev = i64::MIN;
        for batch in reader {
            let batch = batch.unwrap();
            let ts = siglake_core::column_nanos(
                batch
                    .column_by_name(siglake_core::nanos_source_column(
                        batch.schema().as_ref(),
                        "timestamp",
                    ))
                    .unwrap(),
            )
            .unwrap();
            for i in 0..batch.num_rows() {
                assert!(
                    ts.value(i) >= prev,
                    "each partition file must be ascending: {f:?}"
                );
                prev = ts.value(i);
            }
        }
    }
}

/// WS-6: `append_batch_with_consumed` records the consumed WAL segment
/// basenames in the new snapshot's summary, and `events_provider_with_consumed`
/// reads back the CUMULATIVE union across the retained history (#61
/// transition race, 851ce79) — a segment stays excluded once ANY snapshot
/// names it, so its rows can't double-count while it lingers in the buffer's
/// recent-committed window.
#[tokio::test]
async fn consumed_segments_round_trip_through_snapshot_summary() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    let evs: Vec<Event> = (0..3).map(|i| Event::now(format!("c{i}"))).collect();
    let batch = siglake_core::events_to_record_batch(&evs).unwrap();
    ice.append_batch_with_consumed(batch, &["ing-a.arrow".into(), "ing-b.arrow".into()])
        .await
        .unwrap();

    let (_p, consumed) = ice.events_provider_with_consumed().await.unwrap();
    let expected: std::collections::HashSet<String> =
        ["ing-a.arrow".to_string(), "ing-b.arrow".to_string()]
            .into_iter()
            .collect();
    assert_eq!(
        *consumed, expected,
        "consumed segments survive the snapshot summary"
    );

    // The set is cumulative: a subsequent plain append (no consumed property
    // on its own snapshot) must NOT clear it — clearing was the transition
    // race, where a buffered-but-committed segment's rows double-counted.
    let more: Vec<Event> = (0..2).map(|i| Event::now(format!("d{i}"))).collect();
    let batch2 = siglake_core::events_to_record_batch(&more).unwrap();
    ice.append_batch(batch2).await.unwrap();
    let (_p2, consumed2) = ice.events_provider_with_consumed().await.unwrap();
    assert_eq!(
        *consumed2, expected,
        "consumed set is cumulative — a plain append must not clear it"
    );
}

/// The durable reclaim proof is a table property, not a snapshot summary. A
/// row-preserving rewrite must carry it byte-for-byte while the legacy summary
/// remains available to the WS-6 buffer.
#[tokio::test]
async fn consumed_proof_survives_recluster_commit() {
    use siglake_storage::consumed_proof::{proof_from_table, CONSUMED_PROOF_PROP};
    use siglake_storage::iceberg::BLOOM_FILTER_COLUMNS;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&warehouse_dir(&tmp)).await.unwrap();

    for (n, segment) in ["proof-a.arrow", "proof-b.arrow"].into_iter().enumerate() {
        let events: Vec<Event> = (0..3)
            .map(|i| Event::now(format!("proof-{n}-{i}")))
            .collect();
        let batch = siglake_core::events_to_record_batch(&events).unwrap();
        ice.append_batch_with_consumed(batch, &[segment.to_string()])
            .await
            .unwrap();
    }

    let ident = ice.events_table_ident().clone();
    let before_table = ice.catalog().load_table(&ident).await.unwrap();
    let before_value = before_table
        .metadata()
        .properties()
        .get(CONSUMED_PROOF_PROP)
        .expect("append creates durable consumed proof")
        .clone();
    let before = proof_from_table(&before_table).unwrap().unwrap();
    assert!(before.contains("proof-a.arrow"));
    assert!(before.contains("proof-b.arrow"));

    let files = ice.live_data_files(&ident).await.unwrap();
    assert!(files.len() >= 2, "test needs a real multi-file rewrite");
    ice.recluster_files(&ident, files, BLOOM_FILTER_COLUMNS)
        .await
        .unwrap();

    let after_table = ice.catalog().load_table(&ident).await.unwrap();
    assert_eq!(
        after_table.metadata().properties().get(CONSUMED_PROOF_PROP),
        Some(&before_value),
        "recluster must preserve the durable property byte-for-byte"
    );
    let (_provider, legacy) = ice.events_provider_with_consumed().await.unwrap();
    assert!(legacy.contains("proof-a.arrow"));
    assert!(legacy.contains("proof-b.arrow"));
}

/// Two writers may load the same property base. The losing Iceberg CAS retry
/// must re-run the proof merge against the winner rather than commit its stale
/// precomputed value and erase the winner's segment ID.
#[tokio::test]
async fn concurrent_consumed_proof_cas_preserves_both_writers() {
    use iceberg::spec::TableProperties;
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use siglake_storage::consumed_proof::{
        proof_from_table, ConsumedProofEntry, MergeConsumedProofAction,
    };

    const WRITER_COUNT: usize = 6;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&warehouse_dir(&tmp)).await.unwrap();
    ice.append_events(&[Event::now("cas-base")]).await.unwrap();
    // Every same-base loser can consume one retry before the next writer wins.
    // Declare enough retries for the contention this test deliberately creates
    // instead of inheriting Iceberg's four-retry production default.
    let table = ice
        .catalog()
        .load_table(ice.events_table_ident())
        .await
        .unwrap();
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set(
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
            WRITER_COUNT.to_string(),
        )
        .apply(tx)
        .unwrap();
    tx.commit(ice.catalog().as_ref()).await.unwrap();

    let base = ice
        .catalog()
        .load_table(ice.events_table_ident())
        .await
        .unwrap();
    // Build every transaction against exactly the same metadata generation.
    // Even if the executor polls them one after another, all but the first must
    // refresh and re-apply their action against a newer property value.
    let mut commits = tokio::task::JoinSet::new();
    for writer in 0..WRITER_COUNT {
        let tx = Transaction::new(&base);
        let tx = MergeConsumedProofAction::new(
            vec![ConsumedProofEntry {
                segment_id: format!("cas-{writer}.arrow"),
                claimed_at_ms: writer as i64,
            }],
            None,
        )
        .apply(tx)
        .unwrap();
        let catalog = ice.catalog().clone();
        commits.spawn(async move { tx.commit(catalog.as_ref()).await });
    }
    while let Some(committed) = commits.join_next().await {
        committed.unwrap().unwrap();
    }

    let table = ice
        .catalog()
        .load_table(ice.events_table_ident())
        .await
        .unwrap();
    let proof = proof_from_table(&table).unwrap().unwrap();
    for writer in 0..WRITER_COUNT {
        assert!(proof.contains(&format!("cas-{writer}.arrow")));
    }
}

#[tokio::test]
async fn corrupt_consumed_proof_refuses_the_next_append() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use siglake_storage::consumed_proof::CONSUMED_PROOF_PROP;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&warehouse_dir(&tmp)).await.unwrap();
    ice.append_events(&[Event::now("before-corruption")])
        .await
        .unwrap();
    let ident = ice.events_table_ident().clone();
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set(CONSUMED_PROOF_PROP.to_string(), "not-json".to_string())
        .apply(tx)
        .unwrap();
    tx.commit(ice.catalog().as_ref()).await.unwrap();

    let before: u64 = ice
        .live_data_files(&ident)
        .await
        .unwrap()
        .iter()
        .map(|file| file.record_count())
        .sum();
    let err = ice
        .append_events(&[Event::now("must-not-commit")])
        .await
        .expect_err("corrupt durable proof must fail the table commit");
    assert!(format!("{err:#}").contains("cannot update siglake.consumed_proof.v1"));
    let after: u64 = ice
        .live_data_files(&ident)
        .await
        .unwrap()
        .iter()
        .map(|file| file.record_count())
        .sum();
    assert_eq!(after, before);
}

#[tokio::test]
async fn terminal_id_pruning_unwedges_a_near_cap_append_without_duplicate_rows() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use metrics_util::debugging::DebuggingRecorder;
    use siglake_storage::consumed_proof::{
        proof_from_table, ConsumedProof, ConsumedProofEntry, CONSUMED_PROOF_MAX_BYTES,
        CONSUMED_PROOF_PROP,
    };

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&warehouse_dir(&tmp)).await.unwrap();
    let survivor = ConsumedProofEntry {
        segment_id: "still-processing.arrow".to_string(),
        claimed_at_ms: 40,
    };
    let mut proof = ConsumedProof::empty(Some(10));
    proof.insert(survivor.clone()).unwrap();

    // Fill the v1 property to its largest valid size while keeping one
    // unrelated non-terminal entry that pruning must preserve.
    let mut low = 0usize;
    let mut high = CONSUMED_PROOF_MAX_BYTES;
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        let mut candidate = proof.clone();
        candidate
            .insert(ConsumedProofEntry {
                segment_id: format!("terminal-{}", "x".repeat(mid)),
                claimed_at_ms: 50,
            })
            .unwrap();
        if candidate.encode().is_ok() {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    let terminal_id = format!("terminal-{}", "x".repeat(low));
    proof
        .insert(ConsumedProofEntry {
            segment_id: terminal_id.clone(),
            claimed_at_ms: 50,
        })
        .unwrap();

    let ident = ice.events_table_ident().clone();
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set(CONSUMED_PROOF_PROP.to_string(), proof.encode().unwrap())
        .apply(tx)
        .unwrap();
    tx.commit(ice.catalog().as_ref()).await.unwrap();

    let later = ConsumedProofEntry {
        segment_id: "later-drain.arrow".to_string(),
        claimed_at_ms: 60,
    };
    let batch = siglake_core::events_to_record_batch(&[Event::now("exactly-once")]).unwrap();
    let refused_recorder = DebuggingRecorder::new();
    let refused_snapshot = refused_recorder.snapshotter();
    let err = {
        let _guard = metrics::set_default_local_recorder(&refused_recorder);
        ice.append_batch_with_consumed_proof(
            batch.clone(),
            std::slice::from_ref(&later),
            Some(30),
            None,
        )
        .await
        .expect_err("the unpruned candidate must cross the cap")
    };
    assert!(format!("{err:#}").contains("above the 1048576-byte cap"));
    let refused_metrics = refused_snapshot.snapshot().into_vec();
    assert!(refused_metrics.iter().any(|(key, _, _, _)| {
        key.key().name() == "siglake_consumed_proof_current_watermark_lag_seconds"
    }));
    assert!(!refused_metrics.iter().any(|(key, _, _, _)| {
        key.key().name() == "siglake_consumed_proof_watermark_lag_seconds"
    }));

    ice.append_batch_with_consumed_proof_pruning(
        batch,
        std::slice::from_ref(&later),
        Some(30),
        std::slice::from_ref(&terminal_id),
        None,
    )
    .await
    .unwrap();

    let table = ice.catalog().load_table(&ident).await.unwrap();
    let resumed = proof_from_table(&table).unwrap().unwrap();
    assert!(!resumed.contains(&terminal_id));
    assert!(resumed.contains(&survivor.segment_id));
    assert!(resumed.contains(&later.segment_id));
    let rows: u64 = ice
        .live_data_files(&ident)
        .await
        .unwrap()
        .iter()
        .map(|file| file.record_count())
        .sum();
    assert_eq!(rows, 1, "the refused attempt must not duplicate table rows");
}

/// WS-3 early-stop ordering. With one file per task partition the scan advertises
/// `timestamp ASC` output ordering, so an ordered `LIMIT` plans as a
/// `SortPreservingMergeExec` (streams just enough rows) rather than a blocking
/// `SortExec` — and, critically, still returns the correct rows. The differential
/// assertion is the safety net against over-claiming the order.
#[tokio::test]
async fn ordered_limit_uses_sort_preserving_merge_and_is_correct() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use datafusion::physical_plan::displayable;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    // Noon today so all offsets stay in one day partition. Four separate appends
    // ⇒ four files whose time ranges interleave (so the order only holds because
    // each file is internally sorted + merged across partitions).
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    let mk = |offs: &[i64]| -> Vec<Event> {
        offs.iter()
            .map(|&s| {
                let mut e = Event::now(format!("row @{s}"));
                e.timestamp = base + Duration::seconds(s);
                e
            })
            .collect()
    };
    ice.append_events(&mk(&[10, 50, 90])).await.unwrap();
    ice.append_events(&mk(&[20, 60])).await.unwrap();
    ice.append_events(&mk(&[5, 70, 95])).await.unwrap();
    ice.append_events(&mk(&[30, 40, 80])).await.unwrap();

    // target_partitions >= file count ⇒ one file per partition ⇒ ordering safe.
    let ctx = siglake_storage::session_context_with_target_partitions(Some(16));
    ice.register_with_datafusion(&ctx).await.unwrap();

    let sql = "SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" ASC LIMIT 5";
    let df = ctx.sql(sql).await.unwrap();
    let physical = df.clone().create_physical_plan().await.unwrap();
    let plan_str = format!("{}", displayable(physical.as_ref()).indent(true));
    assert!(
        plan_str.contains("SortPreservingMergeExec"),
        "expected early-stop merge of the sorted scan, got:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("SortExec"),
        "a blocking SortExec means the advertised ordering wasn't used:\n{plan_str}"
    );

    // Correctness: the five smallest timestamps, in order — proves the advertised
    // ordering didn't drop a needed sort and return mis-ordered rows.
    let batches = df.collect().await.unwrap();
    let ts: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let a = siglake_core::column_nanos(b.column(0)).unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    let want: Vec<i64> = [5, 10, 20, 30, 40]
        .iter()
        .map(|s| {
            (base + Duration::seconds(*s))
                .timestamp_nanos_opt()
                .unwrap()
        })
        .collect();
    assert_eq!(
        ts, want,
        "ordered LIMIT must return the 5 earliest rows in order"
    );
}

/// WS-3 k-way per-partition merge: when a task partition holds files whose
/// time ranges OVERLAP (here `target_partitions = 1` packs three interleaved
/// files into one partition), the scan still advertises the ordering and
/// merges the per-file streams at execute — no blocking `SortExec`, and the
/// rows come out globally interleaved-in-order, both as a full ordered scan
/// and under an early-stop `LIMIT`.
#[tokio::test]
async fn overlapping_multi_file_partition_merges_in_order() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use datafusion::physical_plan::displayable;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    let mk = |offs: &[i64]| -> Vec<Event> {
        offs.iter()
            .map(|&s| {
                let mut e = Event::now(format!("row @{s}"));
                e.timestamp = base + Duration::seconds(s);
                e
            })
            .collect()
    };
    // Interleaved time ranges ⇒ no disjoint run exists; only a merge can
    // stream this partition sorted.
    ice.append_events(&mk(&[10, 50, 90])).await.unwrap();
    ice.append_events(&mk(&[20, 60])).await.unwrap();
    ice.append_events(&mk(&[5, 70])).await.unwrap();

    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&ctx).await.unwrap();

    let collect_ts = |batches: Vec<arrow_array::RecordBatch>| -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                let a = siglake_core::column_nanos(b.column(0)).unwrap();
                (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
            })
            .collect()
    };
    let nanos = |offs: &[i64]| -> Vec<i64> {
        offs.iter()
            .map(|s| {
                (base + Duration::seconds(*s))
                    .timestamp_nanos_opt()
                    .unwrap()
            })
            .collect()
    };

    // Early-stop LIMIT: merged head, no blocking sort.
    let df = ctx
        .sql("SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" ASC LIMIT 3")
        .await
        .unwrap();
    let plan_str = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    assert!(
        !plan_str.contains("SortExec"),
        "overlapping partition must k-way merge, not blocking-sort:\n{plan_str}"
    );
    assert_eq!(collect_ts(df.collect().await.unwrap()), nanos(&[5, 10, 20]));

    // Full ordered scan: the merge must interleave ALL rows correctly.
    let df_full = ctx
        .sql("SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" ASC")
        .await
        .unwrap();
    assert_eq!(
        collect_ts(df_full.collect().await.unwrap()),
        nanos(&[5, 10, 20, 50, 60, 70, 90]),
        "k-way merge must produce the exact global interleaving"
    );
}

/// WS-3 per-partition merge: a MULTI-file partition advertises `timestamp ASC`
/// when its files are time-disjoint — the planner rearranges each partition
/// into an ascending run and execution drains tasks in order, so the
/// concatenated stream is sorted. Four disjoint files across two partitions:
/// the ordered `LIMIT` plans as a `SortPreservingMergeExec` with NO blocking
/// `SortExec`, and returns the exact global head.
#[tokio::test]
async fn disjoint_multi_file_partitions_advertise_ordering() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use datafusion::physical_plan::displayable;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    let mk = |offs: &[i64]| -> Vec<Event> {
        offs.iter()
            .map(|&s| {
                let mut e = Event::now(format!("row @{s}"));
                e.timestamp = base + Duration::seconds(s);
                e
            })
            .collect()
    };
    // Four time-DISJOINT files: [0..5], [10..15], [20..25], [30..35] seconds.
    for i in 0..4i64 {
        ice.append_events(&mk(&(0..6).map(|j| i * 10 + j).collect::<Vec<_>>()))
            .await
            .unwrap();
    }

    // Two partitions over four files ⇒ multi-file partitions; disjoint files ⇒
    // the gate rearranges each into an ASC run and advertises the ordering.
    let ctx = siglake_storage::session_context_with_target_partitions(Some(2));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let sql = "SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" ASC LIMIT 9";
    let df = ctx.sql(sql).await.unwrap();
    let plan_str = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    assert!(
        plan_str.contains("SortPreservingMergeExec"),
        "expected early-stop merge of disjoint-run partitions, got:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("SortExec"),
        "a blocking SortExec means the multi-file disjoint run wasn't advertised:\n{plan_str}"
    );

    let batches = df.collect().await.unwrap();
    let ts: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let a = siglake_core::column_nanos(b.column(0)).unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    let want: Vec<i64> = [0, 1, 2, 3, 4, 5, 10, 11, 12]
        .iter()
        .map(|s| {
            (base + Duration::seconds(*s))
                .timestamp_nanos_opt()
                .unwrap()
        })
        .collect();
    assert_eq!(
        ts, want,
        "global head must merge across multi-file partitions in order"
    );
}

/// WS-3 + round-73 finding: a WINDOWED ordered `LIMIT` (the canonical log-search
/// shape) must early-stop too. The residual `FilterExec` used to repartition
/// round-robin and destroy the scan's ordering; with
/// `prefer_existing_sort=true` in the session config the ordering survives and
/// the plan needs no blocking `SortExec`.
#[tokio::test]
async fn windowed_ordered_limit_early_stops() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use datafusion::physical_plan::displayable;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    let mk = |offs: &[i64]| -> Vec<Event> {
        offs.iter()
            .map(|&s| {
                let mut e = Event::now(format!("row @{s}"));
                e.timestamp = base + Duration::seconds(s);
                e
            })
            .collect()
    };
    for i in 0..4i64 {
        ice.append_events(&mk(&(0..6).map(|j| i * 10 + j).collect::<Vec<_>>()))
            .await
            .unwrap();
    }

    let ctx = siglake_storage::session_context_with_target_partitions(Some(2));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let lo = (base + Duration::seconds(10)).to_rfc3339();
    let hi = (base + Duration::seconds(31)).to_rfc3339();
    let sql = format!(
        "SELECT \"timestamp\" FROM events WHERE \"timestamp\" >= TIMESTAMP '{lo}' \
         AND \"timestamp\" < TIMESTAMP '{hi}' ORDER BY \"timestamp\" ASC LIMIT 4"
    );
    let df = ctx.sql(&sql).await.unwrap();
    let plan_str = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    assert!(
        !plan_str.contains("SortExec"),
        "windowed ordered LIMIT must not need a blocking sort:\n{plan_str}"
    );

    let batches = df.collect().await.unwrap();
    let ts: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let a = siglake_core::column_nanos(b.column(0)).unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    let want: Vec<i64> = [10, 11, 12, 13]
        .iter()
        .map(|s| {
            (base + Duration::seconds(*s))
                .timestamp_nanos_opt()
                .unwrap()
        })
        .collect();
    assert_eq!(
        ts, want,
        "windowed ordered LIMIT must return the window's head in order"
    );
}

/// WS-5 slice A: with `SIGLAKE_INVERTED_INDEX=1`, a written events data file
/// carries its inverted-index blob in the Parquet footer KV, and the blob
/// round-trips to correct term→row-ordinal postings (file-physical row order).
#[tokio::test]
async fn inverted_index_blob_lands_in_footer_kv() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_inverted_index(true);

    // Distinct raw tokens at known row positions. Single append ⇒ one file whose
    // physical order is timestamp ASC (Event::now is monotonic here).
    // All tokens >= MIN_TOKEN_LEN (3) so the tokenizer indexes them.
    let raws = [
        "error connecting database",
        "user login okay",
        "database error timeout",
    ];
    let evs: Vec<Event> = raws.iter().map(|r| Event::now((*r).to_string())).collect();
    ice.append_events(&evs).await.unwrap();

    let parquet: Vec<_> = list_files(&warehouse)
        .into_iter()
        .filter(|p| {
            p.extension().is_some_and(|e| e == "parquet")
                && p.components().any(|c| c.as_os_str() == "data")
        })
        .collect();
    assert_eq!(parquet.len(), 1, "one data file: {parquet:?}");

    let bytes = bytes::Bytes::from(std::fs::read(&parquet[0]).unwrap());
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let kv = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("footer KV present");
    let blob = kv
        .iter()
        .find(|e| e.key == siglake_index::INVERTED_INDEX_KV_KEY)
        .and_then(|e| e.value.clone())
        .expect("inverted-index blob in footer KV");

    let index = siglake_index::InvertedIndex::from_hex(&blob).expect("valid index blob");
    assert_eq!(index.n_rows(), 3);
    // Rows are written timestamp-ASC = append order, so "error" ∈ rows {0,2}.
    assert_eq!(index.postings("error"), Some([0u32, 2].as_slice()));
    assert_eq!(index.postings("login"), Some([1u32].as_slice()));
    assert_eq!(index.postings("missing"), None);
    // AND across tokens both in row 2.
    assert_eq!(index.matching_rows_all(&["database", "timeout"]), vec![2]);
}
// (The default-off gate is a one-line env early-return in
// `events_inverted_index_hex`; no absence test — it would race the shared
// `SIGLAKE_INVERTED_INDEX` env var with the positive test under parallel runs.)

/// Round-3 regression (2026-07-11): a USER-INDEX table carries a NULLABLE
/// timestamp, so DataFusion compares the advertised SortOptions STRUCTURALLY
/// — a NULLS LAST advertisement never satisfied an unadorned `ORDER BY
/// timestamp DESC` (whose default is NULLS FIRST) and every index browse fell
/// to a TopK full scan (413 at 200G). The gate must advertise the nulls order
/// the default ORDER BY requests, so the reversed browse plans as a
/// fetch-limited merge with no blocking sort.
#[tokio::test]
async fn reversed_browse_on_index_table_early_stops() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use datafusion::physical_plan::displayable;
    use siglake_core::index_config::IndexConfig;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let config = IndexConfig {
        index_id: "rev-idx".into(),
        ..IndexConfig::builtin_events()
    };
    ice.create_index(&config).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    for i in 0..3i64 {
        let events: Vec<Event> = (0..10)
            .map(|j| {
                let mut e = Event::now(format!("row {i}-{j}"));
                e.timestamp = base + Duration::seconds(i * 100 + j);
                e
            })
            .collect();
        let batch = siglake_core::events_to_record_batch(&events).unwrap();
        let mapped = siglake_core::mapping::map_carrier_batch(&batch, &config).unwrap();
        ice.append_to_table(&ice.index_table_ident("rev-idx"), mapped, &[])
            .await
            .unwrap();
    }

    // The reversed direction (DESC on an ASC-declared table), requested the
    // way the query server does it: PreferredScanOrder in the session config.
    let ctx = siglake_storage::session_context_with_order(
        Some(2),
        None,
        Some(siglake_storage::PreferredScanOrder { descending: true }),
    );
    ice.register_index_with_datafusion(&ctx, "rev-idx")
        .await
        .unwrap();
    let df = ctx
        .sql("SELECT \"timestamp\", raw FROM \"rev-idx\" ORDER BY \"timestamp\" DESC LIMIT 5")
        .await
        .unwrap();
    let plan_str = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    assert!(
        !plan_str.contains("SortExec"),
        "reversed index browse must not need a blocking sort:\n{plan_str}"
    );
    // And the rows are exactly the newest five, in order.
    let batches = df.collect().await.unwrap();
    let raws: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            (0..a.len())
                .map(|i| a.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        raws,
        vec!["row 2-9", "row 2-8", "row 2-7", "row 2-6", "row 2-5"]
    );
}

/// Scale repro for the round-4 residual: reversed browse over OVERLAPPING
/// files (the live leading-edge layout; the disjoint-file case is covered
/// above). Must plan without a blocking sort.
#[tokio::test]
async fn reversed_browse_over_overlapping_files_early_stops() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use datafusion::physical_plan::displayable;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    // Six mutually-overlapping files: [i, 100+i] seconds.
    let mk = |offs: &[i64]| -> Vec<Event> {
        offs.iter()
            .map(|&s| {
                let mut e = Event::now(format!("row @{s}"));
                e.timestamp = base + Duration::seconds(s);
                e
            })
            .collect()
    };
    for i in 0..6i64 {
        ice.append_events(&mk(&[i, 50 + i, 100 + i])).await.unwrap();
    }

    let ctx = siglake_storage::session_context_with_order(
        Some(3),
        None,
        Some(siglake_storage::PreferredScanOrder { descending: true }),
    );
    ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx
        .sql("SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" DESC LIMIT 4")
        .await
        .unwrap();
    let plan_str = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    assert!(
        !plan_str.contains("SortExec"),
        "reversed browse over overlapping files must not need a blocking sort:\n{plan_str}"
    );
    let batches = df.collect().await.unwrap();
    let ts: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let a = siglake_core::column_nanos(b.column(0)).unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    let want: Vec<i64> = [105i64, 104, 103, 102]
        .iter()
        .map(|s| {
            (base + Duration::seconds(*s))
                .timestamp_nanos_opt()
                .unwrap()
        })
        .collect();
    assert_eq!(ts, want, "newest four across overlapping files, in order");
}

/// #90 (fork-side unit test hosted here — the vendored crate isn't a
/// workspace member): a reversed read must reorder the RowSelection's
/// per-group segments to the reversed group order, splitting straddlers and
/// materializing omitted trailing skips. A misaligned selection corrupted
/// windowed reversed browses (hung with zero output at 200G).
#[test]
fn reverse_row_selection_reorders_per_group_segments() {
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
    // Three groups of 10 rows: group0 = select 10; group1 = skip 4, select 6;
    // group2 omitted (implicit trailing skip).
    let sel: RowSelection = vec![
        RowSelector::select(10),
        RowSelector::skip(4),
        RowSelector::select(6),
    ]
    .into();
    let rev = iceberg::arrow::reverse_row_selection(sel, &[10, 10, 10]);
    let got: Vec<(bool, usize)> = rev.iter().map(|s| (s.skip, s.row_count)).collect();
    // RowSelection normalizes adjacent same-kind runs: reversed order is
    // skip10(g2) + skip4,select6(g1) + select10(g0) = skip14, select16.
    assert_eq!(got, vec![(true, 14), (false, 16)], "{got:?}");

    // A selector straddling a group boundary must split.
    let sel: RowSelection = vec![RowSelector::select(15), RowSelector::skip(5)].into();
    let rev = iceberg::arrow::reverse_row_selection(sel, &[10, 10]);
    let got: Vec<(bool, usize)> = rev.iter().map(|s| (s.skip, s.row_count)).collect();
    // g1 was select5,skip5; g0 select10 — the trailing skip5+select10 stay
    // distinct (different kinds), select5 leads.
    assert_eq!(got, vec![(false, 5), (true, 5), (false, 10)], "{got:?}");
    assert_eq!(rev.iter().map(|s| s.row_count).sum::<usize>(), 20);
}

/// #93 regression: a deep overlap stack whose files are ALL at the TOP level
/// must still fire the depth trigger. The 2026-07-12 final round stalled at
/// depth 14: the converged layout's stack members were top-level files, which
/// the trigger's old eligibility excluded (correct for count triggers, wrong
/// for the depth trigger — the ordered scan reads every live file, and the
/// depth SLI sweeps them all).
#[tokio::test]
async fn depth_trigger_fires_on_top_level_stack() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use siglake_storage::iceberg::{
        LevelPolicy, LeveledPassOptions, ReclusterPolicy, BLOOM_FILTER_COLUMNS,
    };

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    // 4 mutually-overlapping files (depth 4).
    for k in 0..4i64 {
        let events: Vec<Event> = (0..4)
            .map(|j| {
                let mut e = Event::now(format!("top stack {k} {j}"));
                e.timestamp = base + Duration::seconds(j * 30 + k);
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
    let ident = ice.events_table_ident().clone();

    // TINY ceilings put every real file at the TOP level (size > all ceilings).
    let levels = LevelPolicy {
        level_ceilings: vec![16, 32],
        trigger_files: 100, // count triggers never fire
        max_fanin: 64,
        max_merge_gen: 0,
        max_overlap_depth: 3, // depth 4 > 3 ⇒ must fire despite top level
    };
    // PACED-cadence options: due levels exclude the top level (they always
    // do — level indices only cover the ceilings), max_total_bins None. The
    // second 1TB stall mode: the due-level gate suppressed depth bins.
    let paced = LeveledPassOptions {
        allowed_levels: Some(vec![0]),
        max_total_bins: None,
        ..Default::default()
    };
    let stats = ice
        .recluster_pass_leveled(
            &ident,
            BLOOM_FILTER_COLUMNS,
            &levels,
            ReclusterPolicy::default(),
            &paced,
        )
        .await
        .unwrap();
    assert_eq!(
        stats.len(),
        1,
        "depth trigger must fire on a top-level stack"
    );
    let after = ice.live_data_files(&ident).await.unwrap();
    assert!(after.len() < 4, "stack consolidated: {} files", after.len());
    assert_eq!(
        after.iter().map(|f| f.record_count()).sum::<u64>(),
        16,
        "rows conserved"
    );
}

/// Helper: simulate a legacy-DESC table (the long-lived production `events`
/// declaration) on a fresh warehouse by replacing the declared order.
async fn declare_desc_for_test(ice: &IcebergContext) {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let table = ice
        .catalog()
        .load_table(ice.events_table_ident())
        .await
        .unwrap();
    let tx = Transaction::new(&table);
    let tx = tx
        .replace_sort_order()
        .desc("timestamp", iceberg::spec::NullOrder::Last)
        .apply(tx)
        .unwrap();
    tx.commit(ice.catalog().as_ref()).await.unwrap();
    ice.invalidate_cached_table(ice.events_table_ident()).await;
}

fn desc_events(base: chrono::DateTime<chrono::Utc>, tag: &str, n: i64) -> Vec<Event> {
    use chrono::Duration;
    (0..n)
        .map(|j| {
            let mut e = Event::now(format!("row {tag}-{j}"));
            e.timestamp = base + Duration::seconds(j);
            e
        })
        .collect()
}

/// ASC convergence step 1: the migration op flips a legacy-DESC declaration
/// to ascending, records the outgoing order id, and is idempotent.
#[tokio::test]
async fn converge_sort_order_flips_desc_to_asc_and_records_legacy() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&warehouse_dir(&tmp)).await.unwrap();
    declare_desc_for_test(&ice).await;

    let flipped = ice
        .converge_sort_order_to_asc(ice.events_table_ident())
        .await
        .unwrap();
    assert!(flipped, "DESC table must converge");
    let table = ice
        .catalog()
        .load_table(ice.events_table_ident())
        .await
        .unwrap();
    let lead = table.metadata().default_sort_order().fields[0].clone();
    assert_eq!(lead.direction, iceberg::spec::SortDirection::Ascending);
    assert!(
        table
            .metadata()
            .properties()
            .contains_key(siglake_storage::iceberg::LEGACY_SORT_ORDER_PROP),
        "legacy order id recorded"
    );
    // Idempotent: already ascending ⇒ no-op.
    assert!(!ice
        .converge_sort_order_to_asc(ice.events_table_ident())
        .await
        .unwrap());
}

/// Mid-convergence: files written under BOTH directions coexist. The gate
/// must refuse the ordered advertisement (browses fall back to a blocking
/// TopK — slower, correct) rather than stream a DESC file as ASC.
#[tokio::test]
async fn ordered_browse_refuses_on_mixed_direction_files() {
    use chrono::{NaiveTime, TimeZone, Utc};
    use datafusion::physical_plan::displayable;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&warehouse_dir(&tmp)).await.unwrap();
    declare_desc_for_test(&ice).await;
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    // One DESC-stamped file, then converge, then one ASC-stamped file.
    ice.append_events(&desc_events(base, "old", 10))
        .await
        .unwrap();
    ice.converge_sort_order_to_asc(ice.events_table_ident())
        .await
        .unwrap();
    ice.append_events(&desc_events(
        base + chrono::Duration::seconds(100),
        "new",
        10,
    ))
    .await
    .unwrap();

    let ctx = siglake_storage::session_context_with_order(
        Some(2),
        None,
        Some(siglake_storage::PreferredScanOrder { descending: true }),
    );
    ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx
        .sql("SELECT \"timestamp\", raw FROM events ORDER BY \"timestamp\" DESC LIMIT 3")
        .await
        .unwrap();
    let plan_str = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    assert!(
        plan_str.contains("SortExec"),
        "mixed-direction files must refuse the ordered advertisement:\n{plan_str}"
    );
    // Correctness holds through the fallback.
    let batches = df.collect().await.unwrap();
    let raws: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            (0..a.len())
                .map(|i| a.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(raws, vec!["row new-9", "row new-8", "row new-7"]);
}

/// Post-convergence: once every live file was written under the (new)
/// ascending order — here, converged before any writes — the gate advertises
/// again on the multi-order table, attributing files by their stamped ids.
#[tokio::test]
async fn converged_table_advertises_once_files_agree() {
    use chrono::{NaiveTime, TimeZone, Utc};
    use datafusion::physical_plan::displayable;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&warehouse_dir(&tmp)).await.unwrap();
    declare_desc_for_test(&ice).await;
    ice.converge_sort_order_to_asc(ice.events_table_ident())
        .await
        .unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    ice.append_events(&desc_events(base, "a", 10))
        .await
        .unwrap();
    ice.append_events(&desc_events(base + chrono::Duration::seconds(100), "b", 10))
        .await
        .unwrap();

    let ctx = siglake_storage::session_context_with_order(
        Some(2),
        None,
        Some(siglake_storage::PreferredScanOrder { descending: false }),
    );
    ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx
        .sql("SELECT \"timestamp\", raw FROM events ORDER BY \"timestamp\" ASC LIMIT 3")
        .await
        .unwrap();
    let plan_str = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    assert!(
        !plan_str.contains("SortExec"),
        "all-ASC files on a converged table must advertise:\n{plan_str}"
    );
    let batches = df.collect().await.unwrap();
    let raws: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            (0..a.len())
                .map(|i| a.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(raws, vec!["row a-0", "row a-1", "row a-2"]);
}

/// WS-7 backfill: files written BEFORE a promotion get rewritten by the
/// backfill selector (even as single-file bins — converged files have no
/// merge partner), the promoted column materializes from the residual JSON,
/// and once every live file carries it the completion property flips —
/// enabling the query-side attr_get→column rewrite.
#[tokio::test]
async fn promotion_backfill_rewrites_old_files_and_flips_property() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use datafusion::prelude::SessionContext;
    use siglake_storage::iceberg::{
        LevelPolicy, LeveledPassOptions, ReclusterPolicy, PROMOTION_BACKFILL_PROP,
    };

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = warehouse_dir(&tmp);
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );

    // Two files ingested with NO promotion declared: the key lives only in
    // the residual attributes JSON.
    let legacy = IcebergContext::open(&warehouse).await.unwrap();
    for k in 0..2i64 {
        let events: Vec<Event> = (0..10)
            .map(|j| {
                let mut e = Event::now(format!("row {k}-{j}"))
                    .with_attributes(Some(format!(r#"{{"k8s.pod":"pod-{k}"}}"#)));
                e.timestamp = base + Duration::seconds(k * 100 + j);
                e
            })
            .collect();
        legacy.append_events(&events).await.unwrap();
    }

    // Promotion declared later; schema widens; backfill NOT complete.
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_promoted_columns(vec![PromotedColumn {
            attr_key: "k8s.pod".into(),
            name: "k8s_pod".into(),
            ty: PromotedType::Utf8,
        }]);
    ice.ensure_promoted_columns().await.unwrap();
    assert!(
        !ice.ensure_promotion_backfill_property().await.unwrap(),
        "pre-promotion files exist; property must not flip yet"
    );

    // A leveled pass with idle budget: the backfill selector must rewrite
    // the pre-promotion files even though no count/depth trigger fires.
    let ident = ice.events_table_ident().clone();
    let levels = LevelPolicy {
        trigger_files: 100, // count triggers never fire
        max_merge_gen: 0,
        ..Default::default()
    };
    let stats = ice
        .recluster_pass_leveled(
            &ident,
            &["host", "k8s_pod"],
            &levels,
            ReclusterPolicy::default(),
            &LeveledPassOptions::default(),
        )
        .await
        .unwrap();
    assert!(!stats.is_empty(), "backfill bin must fire");

    // Every live file now carries the promoted column ⇒ property flips.
    assert!(ice.ensure_promotion_backfill_property().await.unwrap());
    let table = ice.catalog().load_table(&ident).await.unwrap();
    assert!(
        table
            .metadata()
            .properties()
            .contains_key(PROMOTION_BACKFILL_PROP),
        "completion property recorded"
    );

    // Differential: the materialized column equals the JSON extraction.
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let batches = ctx
        .sql("SELECT k8s_pod, count(*) AS n FROM events GROUP BY k8s_pod ORDER BY k8s_pod")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows: Vec<(String, i64)> = Vec::new();
    for b in &batches {
        let pods = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let ns = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            rows.push((pods.value(i).to_string(), ns.value(i)));
        }
    }
    assert_eq!(rows, vec![("pod-0".into(), 10), ("pod-1".into(), 10)]);
}

/// Depth plateau (2026-07-18): the depth trigger must converge a deep stab
/// even when EVERY file is generation-mature (`max_merge_gen: 0` makes all
/// files mature immediately — the old eligible-set filter then never fired a
/// depth bin and layouts stalled at depth 29–36 live).
#[tokio::test]
async fn depth_trigger_overrides_generation_cap() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use siglake_storage::iceberg::{
        LevelPolicy, LeveledPassOptions, ReclusterPolicy, BLOOM_FILTER_COLUMNS,
    };

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&warehouse_dir(&tmp)).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(6, 0, 0).unwrap()),
    );
    // 8 fully-overlapping files (each spans the whole hour) => depth 8.
    for j in 0..8i64 {
        let events: Vec<Event> = (0..50)
            .map(|i| {
                let mut e = Event::now(format!("stab {j}/{i}"));
                e.timestamp = base + Duration::seconds(i * 64 + j);
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
    let ident = ice.events_table_ident().clone();
    let policy = LevelPolicy {
        trigger_files: 1000, // count triggers OFF — only the depth trigger acts
        max_merge_gen: 0,    // EVERYTHING is generation-mature
        max_overlap_depth: 3,
        ..Default::default()
    };
    // A few passes must converge the stab to <= the trigger depth.
    for _ in 0..6 {
        ice.recluster_pass_leveled(
            &ident,
            BLOOM_FILTER_COLUMNS,
            &policy,
            ReclusterPolicy::default(),
            &LeveledPassOptions::default(),
        )
        .await
        .unwrap();
    }
    let files = ice.live_data_files(&ident).await.unwrap();
    // All 8 inputs fully overlap, so converged-below-depth-3 means at most 3
    // live files remain (the old gen-mature filter left all 8 untouched).
    assert!(
        files.len() <= 3,
        "depth trigger must converge past generation-mature files: {} files remain",
        files.len()
    );
    let total: u64 = files.iter().map(|f| f.record_count()).sum();
    assert_eq!(total, 400, "row conservation through depth merges");
}

/// Merge-widening ping-pong discriminator (1TB flat-59): staggered spans
/// with a byte cap that forces tiny bins must still CONVERGE — the
/// time-adjacent + cap-exempt depth bins clear stabs in whole merges instead
/// of oscillating depth between neighboring stab points.
#[tokio::test]
async fn depth_trigger_converges_staggered_spans_under_byte_pressure() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use siglake_storage::iceberg::{
        LevelPolicy, LeveledPassOptions, ReclusterPolicy, BLOOM_FILTER_COLUMNS,
    };

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&warehouse_dir(&tmp)).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(2, 0, 0).unwrap()),
    );
    // 12 staggered files: file j spans [j*60, j*60 + 360]s — depth ~6 at the
    // middle, and any far-apart pairing widens its output substantially.
    for j in 0..12i64 {
        let events: Vec<Event> = (0..60)
            .map(|i| {
                let mut e = Event::now(format!("stagger {j}/{i}"));
                e.timestamp = base + Duration::seconds(j * 60 + i * 6);
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
    let ident = ice.events_table_ident().clone();
    let levels = LevelPolicy {
        trigger_files: 1000,
        max_merge_gen: 0,
        max_overlap_depth: 3,
        ..Default::default()
    };
    // Byte cap far below two files' size: the OLD packer truncated depth bins
    // to 2 arbitrary files; the fix exempts depth bins from this cap.
    let policy = ReclusterPolicy {
        max_pass_bytes: 1,
        ..Default::default()
    };
    for _ in 0..8 {
        ice.recluster_pass_leveled(
            &ident,
            BLOOM_FILTER_COLUMNS,
            &levels,
            policy,
            &LeveledPassOptions::default(),
        )
        .await
        .unwrap();
    }
    let files = ice.live_data_files(&ident).await.unwrap();
    assert!(
        files.len() <= 4,
        "staggered spans must converge under byte pressure: {} files remain",
        files.len()
    );
    let total: u64 = files.iter().map(|f| f.record_count()).sum();
    assert_eq!(total, 720, "rows conserved");
}
