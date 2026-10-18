//! Source regressions for #6585. These execute real DataFusion worker queries,
//! not canned SUM/COUNT responses; HTTP/persisted coverage lives in distributed_e2e.
use super::*;
use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Mutex;

fn input(rows: &[(Option<i64>, Option<i64>)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("status", DataType::Int64, true),
        Field::new("size", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn context(batches: Vec<RecordBatch>) -> SessionContext {
    let ctx = SessionContext::new();
    let schema = batches[0].schema();
    ctx.register_table(
        "httplogs",
        Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()),
    )
    .unwrap();
    ctx
}

struct Workers {
    inputs: Vec<RecordBatch>,
    calls: Mutex<Vec<(usize, String)>>,
}

#[async_trait]
impl ShardRunner for Workers {
    async fn run(&self, sql: &str, shard: Option<ScanShard>) -> Result<Vec<RecordBatch>> {
        let shard = shard.context("test must not silently use Local fallback")?;
        self.calls
            .lock()
            .unwrap()
            .push((shard.index, sql.to_owned()));
        context(vec![self.inputs[shard.index].clone()])
            .sql(sql)
            .await?
            .collect()
            .await
            .map_err(Into::into)
    }
}

async fn average_plan(ctx: &SessionContext, sql: &str) -> AveragePlan {
    let logical = ctx.state().create_logical_plan(sql).await.unwrap();
    match classify(&logical) {
        DistPlan::AverageAggregate(p) => p,
        other => panic!("AVG must really distribute: {other:?}; {logical:?}"),
    }
}

fn rows(batches: &[RecordBatch]) -> serde_json::Value {
    crate::format::batches_to_records(batches, None)
        .unwrap()
        .rows
}

const ORIGINAL: &str = "SELECT status, avg(size) AS avg_size, count(*) AS n FROM \"httplogs\" GROUP BY status ORDER BY n DESC LIMIT 20";

#[tokio::test]
async fn unequal_worker_populations_nulls_and_original_schema_are_exact() {
    let inputs = vec![
        input(&[
            (Some(200), Some(0)),
            (Some(200), None),
            (Some(200), None),
            (Some(404), None),
        ]),
        input(&[
            (Some(200), Some(100)),
            (Some(200), Some(100)),
            (Some(200), Some(100)),
            (Some(404), None),
            (None, Some(-5)),
            (None, Some(5)),
        ]),
        input(&[]),
    ];
    let planning = context(inputs.clone());
    let worker = Workers {
        inputs,
        calls: Mutex::new(Vec::new()),
    };
    // Unique tie ordering is separate from the exact original registered SQL.
    for sql in [ORIGINAL,
        "SELECT status, avg(size) AS a, avg(size) AS b, count(size) AS nn FROM httplogs GROUP BY status ORDER BY status NULLS LAST",
        "SELECT avg(size) AS a, count(*) AS n FROM httplogs",
        "SELECT avg(size) AS a FROM httplogs WHERE status=999",
        "SELECT status, avg(size) AS a FROM httplogs WHERE status=999 GROUP BY status",
        "SELECT avg(size) AS a, count(*) AS n FROM httplogs WHERE status=404",
    ] {
        let plan = average_plan(&planning, sql).await;
        assert!(!plan.worker_sql.to_lowercase().contains("avg("));
        assert!(!plan.worker_sql.to_lowercase().contains(" limit "));
        let local = planning.sql(sql).await.unwrap().collect().await.unwrap();
        let got = coordinate(&planning, &worker, sql, 3).await.unwrap();
        // Original has ties at n=2; compare complete rows as multisets there.
        let mut a = rows(&local).as_array().unwrap().clone();
        let mut b = rows(&got).as_array().unwrap().clone();
        if sql == ORIGINAL { a.sort_by_key(|r| r.to_string()); b.sort_by_key(|r| r.to_string()); }
        assert_eq!(a, b, "{sql}");
        let expected_schema = planning.state().create_logical_plan(sql).await.unwrap();
        assert_eq!(got[0].schema().as_ref(), expected_schema.schema().as_arrow(), "final schema: {sql}");
        let calls = worker.calls.lock().unwrap();
        assert_eq!(calls.len() % 3, 0);
        assert!(calls[calls.len()-3..].iter().all(|(_, sent)| sent == &plan.worker_sql));
    }
    let got = coordinate(&planning, &worker, ORIGINAL, 3).await.unwrap();
    let result = rows(&got);
    assert_eq!(result[0]["avg_size"], serde_json::json!(75.0));
    assert_eq!(result[0]["n"], serde_json::json!(6));
    assert_eq!(result[0]["status"], serde_json::json!(200));
}

#[tokio::test]
async fn global_avg_order_keeps_groups_that_lose_each_local_top_one() {
    let inputs = vec![
        input(&[
            (Some(0), Some(10)),
            (Some(1), Some(100)),
            (Some(2), Some(-100)),
        ]),
        input(&[
            (Some(0), Some(10)),
            (Some(1), Some(-100)),
            (Some(2), Some(100)),
        ]),
        input(&[]),
    ];
    let ctx = context(inputs.clone());
    let worker = Workers {
        inputs,
        calls: Mutex::new(Vec::new()),
    };
    let sql = "SELECT status, avg(size) AS a FROM httplogs GROUP BY status ORDER BY a DESC LIMIT 1";
    let got = coordinate(&ctx, &worker, sql, 3).await.unwrap();
    assert_eq!(rows(&got), serde_json::json!([{"status":0,"a":10.0}]));
    assert!(worker
        .calls
        .lock()
        .unwrap()
        .iter()
        .all(|(_, s)| !s.contains("LIMIT")));
}

#[tokio::test]
async fn wal_extra_partial_uses_identical_states_and_non_null_denominator() {
    let committed = input(&[(Some(200), Some(0)), (Some(200), None)]);
    let buffer = input(&[
        (Some(200), Some(100)),
        (Some(200), Some(100)),
        (Some(404), None),
    ]);
    let ctx = context(vec![committed.clone()]);
    let dist = classify(&ctx.state().create_logical_plan(ORIGINAL).await.unwrap());
    let partial_ctx = context(vec![buffer.clone()]);
    let extra = partial_ctx
        .sql(dist.worker_sql(ORIGINAL))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let workers = Workers {
        inputs: vec![committed.clone(), input(&[]), input(&[])],
        calls: Mutex::new(Vec::new()),
    };
    let got = coordinate_with_extra(&ctx, &workers, ORIGINAL, 3, extra)
        .await
        .unwrap();
    let wanted = context(vec![committed, buffer])
        .sql(ORIGINAL)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(rows(&got), rows(&wanted));
    assert!(dist.partial_cast_workspace(3).unwrap() > 3 * 27);
    assert!(
        dist.partial_schema(
            ctx.state()
                .create_logical_plan(ORIGINAL)
                .await
                .unwrap()
                .schema()
                .as_arrow()
        )
        .fields()
        .len()
            > 3
    );
}

#[tokio::test]
async fn original_integer_witness_refuses_rounded_and_overflowing_ranges() {
    for values in [vec![(1i64 << 53) + 1], vec![i64::MIN], vec![1i64 << 52; 3]] {
        let batch = input(
            &values
                .into_iter()
                .map(|v| (Some(1), Some(v)))
                .collect::<Vec<_>>(),
        );
        let ctx = context(vec![batch.clone()]);
        let workers = Workers {
            inputs: vec![batch, input(&[])],
            calls: Mutex::new(Vec::new()),
        };
        let error = coordinate(&ctx, &workers, ORIGINAL, 2).await.unwrap_err();
        assert!(format!("{error:#}").contains("integer"), "{error:#}");
    }
    let boundary = input(&[(Some(1), Some(1i64 << 53))]);
    let ctx = context(vec![boundary.clone()]);
    let workers = Workers {
        inputs: vec![boundary, input(&[])],
        calls: Mutex::new(Vec::new()),
    };
    assert_eq!(
        rows(&coordinate(&ctx, &workers, ORIGINAL, 2).await.unwrap())[0]["avg_size"],
        serde_json::json!((1u64 << 53) as f64)
    );
}

#[tokio::test]
async fn average_state_refuses_schema_nonfinite_and_inconsistent_nulls() {
    let ctx = context(vec![input(&[(Some(1), Some(10))])]);
    let plan = average_plan(&ctx, ORIGINAL).await;
    let valid = ctx
        .sql(&plan.worker_sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    plan.validate_states(&valid).unwrap();
    let base = &valid[0];
    let [sum_index, count_index, _, _] = plan.averages[0];
    for column in [
        Arc::new(Float64Array::from(vec![Some(f64::NAN)])) as ArrayRef,
        Arc::new(Float64Array::from(vec![Some(f64::INFINITY)])),
        Arc::new(Float64Array::from(vec![None])),
    ] {
        let mut columns = base.columns().to_vec();
        columns[sum_index] = column;
        assert!(plan
            .validate_states(&[RecordBatch::try_new(base.schema(), columns).unwrap()])
            .is_err());
    }
    let mut columns = base.columns().to_vec();
    columns[count_index] = Arc::new(Int64Array::from(vec![-1]));
    assert!(plan
        .validate_states(&[RecordBatch::try_new(base.schema(), columns).unwrap()])
        .is_err());
    assert!(plan
        .validate_schema(&[base.project(&[0]).unwrap()])
        .is_err());
    let mut fields = base.schema().fields().to_vec();
    fields[sum_index] = Arc::new(Field::new("wrong_alias", DataType::Float64, true));
    assert!(plan
        .validate_schema(&[RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            base.columns().to_vec()
        )
        .unwrap()])
        .is_err());
}

#[tokio::test]
async fn unsupported_average_types_expressions_and_modifiers_remain_local() {
    let ctx = context(vec![input(&[(Some(1), Some(10))])]);
    for sql in [
        "SELECT avg(CAST(size AS DOUBLE)) FROM httplogs",
        "SELECT avg(CAST(size AS DECIMAL(20,2))) FROM httplogs",
        "SELECT avg(size+1) FROM httplogs",
        "SELECT avg(DISTINCT size) FROM httplogs",
        "SELECT avg(size) FILTER (WHERE status=1) FROM httplogs",
        "SELECT avg(size)*2 FROM httplogs",
        "SELECT avg(size) FROM (SELECT * FROM httplogs LIMIT 1)",
    ] {
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        assert_eq!(classify(&plan), DistPlan::Local, "{sql}");
    }
}

#[tokio::test]
async fn avg_transport_failover_runs_the_same_state_query_on_the_same_shard() {
    struct Unreachable;
    #[async_trait]
    impl ShardRunner for Unreachable {
        async fn run(&self, _: &str, _: Option<ScanShard>) -> Result<Vec<RecordBatch>> {
            anyhow::bail!("connection refused");
        }
    }
    let inputs = vec![
        input(&[(Some(1), Some(0)), (Some(1), None)]),
        input(&[(Some(1), Some(100))]),
    ];
    let ctx = context(inputs.clone());
    let fallback = Workers {
        inputs,
        calls: Mutex::new(Vec::new()),
    };
    let (got, stats) =
        coordinate_with_failover(&ctx, &Unreachable, Some(&fallback), ORIGINAL, 2, vec![])
            .await
            .unwrap();
    assert_eq!(stats.mode, "average_aggregate");
    assert_eq!(
        rows(&got),
        serde_json::json!([{"status":1,"avg_size":50.0,"n":3}])
    );
    let plan = average_plan(&ctx, ORIGINAL).await;
    let mut calls = fallback.calls.lock().unwrap().clone();
    calls.sort();
    assert_eq!(
        calls,
        vec![(0, plan.worker_sql.clone()), (1, plan.worker_sql)]
    );
}

#[tokio::test]
async fn avg_state_count_overflow_and_nonrepresentable_denominator_refuse() {
    let ctx = context(vec![input(&[(Some(1), Some(0))])]);
    let plan = average_plan(&ctx, ORIGINAL).await;
    let valid = ctx
        .sql(&plan.worker_sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let base = &valid[0];
    let count_index = plan.averages[0][1];
    for count in [(1i64 << 53) + 1, i64::MAX] {
        let mut columns = base.columns().to_vec();
        columns[count_index] = Arc::new(Int64Array::from(vec![count]));
        let bad = RecordBatch::try_new(base.schema(), columns).unwrap();
        assert!(plan.validate_states(&[bad.clone(), bad]).is_err());
    }
    let mut fields = base.schema().fields().to_vec();
    fields[count_index] = Arc::new(Field::new(
        fields[count_index].name(),
        DataType::Int64,
        true,
    ));
    let mut columns = base.columns().to_vec();
    columns[count_index] = Arc::new(Int64Array::from(vec![None]));
    assert!(plan
        .validate_states(&[RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()])
        .is_err());
}
