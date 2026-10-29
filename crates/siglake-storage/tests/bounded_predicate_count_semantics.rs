//! #7787 — the bounded predicate-count lookup must carry NULL groups exactly
//! as the unbounded one does.
//!
//! `predicate_count_tier1` defers a large COLD wide fold so a selective
//! equality `COUNT` can fall back to the planner's filtered scan instead of
//! enumerating 43,042 delta names. What it must NOT change is what an answer
//! MEANS when it does serve one: a NULL-valued row is its own group, it is
//! counted towards the `total == record_count` guard, and it is never folded
//! into a value's count. Getting that wrong turns a refusal into a wrong
//! number, which is strictly worse than the timeout being fixed.
//!
//! The column here is deliberately above the inline per-commit cap, so only
//! the wide aggregate can answer it and the test exercises the new arm rather
//! than the untouched inline one.

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use chrono::Utc;
use siglake_core::index_config::{DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode};
use siglake_storage::iceberg::{IcebergContext, IcebergTuning};

/// Distinct `method` values, one row each.
const VALUES: usize = 4_100;
/// Rows whose `method` is NULL.
const NULLS: usize = 50;

fn field(name: &str, field_type: FieldType, required: bool) -> FieldMapping {
    FieldMapping {
        name: name.to_string(),
        field_type,
        required,
    }
}

/// `method` is a tag field, so it is a bloom column and lands in the
/// group-count aggregates. It is NOT required, so it can be NULL.
fn config() -> IndexConfig {
    IndexConfig {
        index_id: "bounded".to_string(),
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                field("timestamp", FieldType::Datetime, true),
                field(
                    "method",
                    FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    false,
                ),
            ],
            timestamp_field: "timestamp".to_string(),
            tag_fields: vec!["method".to_string()],
            default_search_fields: vec![],
        },
        retention: None,
        index_at_flush: None,
    }
}

fn batch(config: &IndexConfig, base_us: i64) -> RecordBatch {
    let rows = VALUES + NULLS;
    let schema = config.to_arrow_schema();
    let ts: Vec<Option<i64>> = (0..rows as i64).map(|i| Some(base_us + i)).collect();
    let methods: Vec<Option<String>> = (0..rows)
        .map(|i| (i < VALUES).then(|| format!("m-{i:05}")))
        .collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("+00:00")),
            Arc::new(StringArray::from(methods)),
            // Dynamic mapping mode appends the WS-7 residual `attributes` column.
            Arc::new(StringArray::from(vec![None::<&str>; rows])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn bounded_predicate_count_keeps_null_groups_separate_from_values() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_tuning(IcebergTuning {
            // Above the inline cap of 4096, so the incremental base+delta path
            // is on and `method` can only be served by the wide aggregate.
            table_group_count_cardinality: Some(10_000),
            result_caches: Some(false),
            ..Default::default()
        });
    let config = config();
    ice.create_index(&config).await.unwrap();
    let ident = ice.index_table_ident(&config.index_id);
    ice.append_to_table(
        &ident,
        batch(&config, Utc::now().timestamp_micros()),
        &["method"],
    )
    .await
    .unwrap();

    // Cold, so the wide aggregate is not folded and the footer tier answers.
    let cold = ice
        .predicate_count_tier1("bounded", "method")
        .await
        .unwrap()
        .expect("the footer tier answers a cold column");

    // Warm, so Tier-1 answers. Both must say the same thing about NULLs.
    ice.tier1_group_counts("bounded", "method")
        .await
        .unwrap()
        .expect("the wide aggregate covers `method` at this cap");
    let rows = ice
        .predicate_count_tier1("bounded", "method")
        .await
        .unwrap()
        .expect("a memoized complete wide aggregate must answer");
    assert_eq!(rows.source_label(), "tier1_wide");

    let mut total = 0u64;
    let mut nulls = 0u64;
    let mut one = 0u64;
    rows.for_each(|key, count| {
        total += count;
        match key {
            None => nulls += count,
            Some("m-00007") => one += count,
            Some(_) => {}
        }
    });
    assert_eq!(
        total,
        (VALUES + NULLS) as u64,
        "the Tier-1 guard is `total == record_count`, and NULL rows are part of it"
    );
    assert_eq!(nulls, NULLS as u64, "NULL is its own group, not a value");
    assert_eq!(
        one, 1,
        "the selective equality answer must not absorb the NULL group"
    );

    // The cold answer came from the footer tier and the warm one from the
    // wide aggregate. A NULL group that only one of them counts would make the
    // fold's removal a silent change of meaning.
    let mut cold_total = 0u64;
    let mut cold_nulls = 0u64;
    cold.for_each(|key, count| {
        cold_total += count;
        if key.is_none() {
            cold_nulls += count;
        }
    });
    assert_eq!((cold_total, cold_nulls), (total, nulls));
}

/// A DELETE commit is the staleness case the read guard exists for: it removes
/// rows without updating the aggregate, so every maintained column is now
/// short of `total-records`.
///
/// Returning the stale aggregate would be a count that still includes the
/// deleted row. The bounded lookup must reject it on the same
/// `total == record_count` guard the generic one uses and land on the per-file
/// footer tier, cold aggregate or warm.
#[tokio::test]
async fn a_delete_commit_leaves_the_bounded_answer_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_tuning(IcebergTuning {
            table_group_count_cardinality: Some(10_000),
            result_caches: Some(false),
            ..Default::default()
        });
    let config = config();
    ice.create_index(&config).await.unwrap();
    let ident = ice.index_table_ident(&config.index_id);
    ice.append_to_table(
        &ident,
        batch(&config, Utc::now().timestamp_micros()),
        &["method"],
    )
    .await
    .unwrap();

    // Warm the aggregate first, so the lookup below is deciding about a
    // decoded aggregate that USED to be complete.
    ice.tier1_group_counts("bounded", "method")
        .await
        .unwrap()
        .expect("complete before the delete");

    let outcome = ice.execute_delete_tasks("bounded").await.unwrap();
    assert_eq!(outcome.tasks_examined, 0, "no task yet: {outcome:?}");
    ice.create_delete_task("bounded", "method = 'm-00007'", None, None)
        .await
        .unwrap();
    let outcome = ice.execute_delete_tasks("bounded").await.unwrap();
    assert_eq!(outcome.rows_deleted, 1, "{outcome:?}");

    // The delete is a commit, so the entry is a fresh generation with a cold
    // memo: the bounded lookup takes the footer tier without folding.
    let cold = ice
        .predicate_count_tier1("bounded", "method")
        .await
        .unwrap()
        .expect("the footer tier answers after a delete");
    assert_eq!(counts(&cold), ((VALUES + NULLS - 1) as u64, 0));

    // And once the new generation HAS been folded, the aggregate is in hand
    // and found short of `total-records`, so the same tier answers again
    // rather than the stale aggregate.
    assert!(
        ice.tier1_group_counts("bounded", "method")
            .await
            .unwrap()
            .is_none(),
        "the delete left every maintained column short of total-records"
    );
    let warm = ice
        .predicate_count_tier1("bounded", "method")
        .await
        .unwrap()
        .expect("the footer tier answers a decoded-but-short aggregate too");
    assert_eq!(counts(&warm), ((VALUES + NULLS - 1) as u64, 0));

    // The generic lookup must agree, which is the equivalence that matters.
    let generic = ice
        .grouped_counts_with_summary("bounded", "method", None, None)
        .await
        .unwrap()
        .expect("the per-file footers answer");
    assert_eq!(counts(&generic), counts(&warm));
}

/// `(total rows, rows whose `method` is the deleted value)`.
fn counts(rows: &siglake_storage::iceberg::GroupCounts) -> (u64, u64) {
    let mut total = 0u64;
    let mut target = 0u64;
    rows.for_each(|key, count| {
        total += count;
        if key == Some("m-00007") {
            target = count;
        }
    });
    (total, target)
}
