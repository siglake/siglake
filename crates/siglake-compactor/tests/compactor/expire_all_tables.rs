//! Snapshot expiry must cover EVERY table, not just `events`.
//!
//! `run_expire_once` used `events_table_ident()` alone, so user indexes never
//! had snapshots trimmed. All benchmark data lives in a user index, and so does
//! every user-created index in production.
//!
//! The 2026-08-10 1TB round measured `logs-bench` at **626 snapshots p50**
//! against a `retain_last` of 100, still climbing. `metadata.json` is re-read and
//! re-parsed on EVERY append, so its size multiplies straight into `load_table` —
//! the cost that grew ~240x across a round (40 ms early, 18,930 ms late) and was
//! the largest unexplained bucket in the write path.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::prelude::SessionContext;
use siglake_compactor::{Compactor, ExpireConfig};
use siglake_core::audit_schema::query_audit_schema;
use siglake_core::index_config::IndexConfig;
use siglake_core::{events_to_record_batch, Event};
use siglake_storage::iceberg::test_catalog::TestCatalog;
use siglake_storage::iceberg::{IcebergContext, LevelPolicy, LeveledPassOptions, ReclusterPolicy};

fn audit_batch(commit: usize) -> RecordBatch {
    // Each commit deliberately carries two identical rows. Full-row equality
    // below must retain both copies. Even and odd commits alternate every
    // nullable field so both value and null arms survive maintenance.
    let base = 1_700_000_000_000_000i64 + commit as i64 * 10;
    let even = commit.is_multiple_of(2);
    let (email, format, complexity, estimated_bytes, estimated_rows, error) = if even {
        (
            Some("a@example.test"),
            Some("records"),
            Some("simple"),
            Some(100),
            Some(10),
            None,
        )
    } else {
        (None, None, None, None, None, Some("refused"))
    };
    let status = if even { "ok" } else { "rejected" };
    RecordBatch::try_new(
        query_audit_schema(),
        vec![
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(base), Some(base)])
                    .with_timezone("+00:00"),
            ),
            Arc::new(StringArray::from(vec!["subject-a", "subject-a"])),
            Arc::new(StringArray::from(vec![email, email])),
            Arc::new(StringArray::from(vec!["sql", "sql"])),
            Arc::new(StringArray::from(vec![
                format!("SELECT {commit}"),
                format!("SELECT {commit}"),
            ])),
            Arc::new(StringArray::from(vec![format, format])),
            Arc::new(StringArray::from(vec!["interactive", "interactive"])),
            Arc::new(Int64Array::from(vec![commit as i64, commit as i64])),
            Arc::new(StringArray::from(vec![status, status])),
            Arc::new(StringArray::from(vec![complexity, complexity])),
            Arc::new(Int64Array::from(vec![estimated_bytes, estimated_bytes])),
            Arc::new(Int64Array::from(vec![estimated_rows, estimated_rows])),
            Arc::new(BooleanArray::from(vec![!even; 2])),
            Arc::new(StringArray::from(vec![error, error])),
        ],
    )
    .expect("exact query_audit batch")
}

fn expected_audit_rows(commits: std::ops::RangeInclusive<usize>) -> RecordBatch {
    let batches: Vec<RecordBatch> = commits.map(audit_batch).collect();
    arrow::compute::concat_batches(&query_audit_schema(), &batches)
        .expect("concatenate expected audit rows")
}

async fn actual_audit_rows(ice: &IcebergContext) -> RecordBatch {
    let ctx = SessionContext::new();
    ice.register_query_audit_with_datafusion(&ctx)
        .await
        .expect("register query_audit");
    let batches = ctx
        .sql("SELECT * FROM query_audit ORDER BY timestamp, query, duration_ms, status")
        .await
        .expect("plan full audit read")
        .collect()
        .await
        .expect("read full audit rows");
    assert_eq!(
        batches[0].schema().fields(),
        query_audit_schema().fields(),
        "query_audit must expose the exact 14-column schema"
    );
    arrow::compute::concat_batches(&query_audit_schema(), &batches)
        .expect("concatenate actual audit rows")
}

async fn assert_audit_rows_equal(ice: &IcebergContext, expected: RecordBatch) {
    let actual = actual_audit_rows(ice).await;
    assert_eq!(actual.schema(), expected.schema(), "exact audit schema");
    assert_eq!(actual.num_rows(), expected.num_rows(), "audit row count");
    for (field, (actual, expected)) in actual
        .schema()
        .fields()
        .iter()
        .zip(actual.columns().iter().zip(expected.columns()))
    {
        assert_eq!(
            actual.to_data(),
            expected.to_data(),
            "full values, nulls and duplicate multiplicity for `{}`",
            field.name()
        );
    }
}

#[tokio::test]
async fn expiry_trims_user_indexes_not_only_events() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );

    // A user index — the shape all benchmark and customer data actually lives in.
    let mut cfg = IndexConfig::builtin_events();
    cfg.index_id = "logs-user".to_string();
    ice.create_index(&cfg).await.expect("create index");
    let ident = ice.index_table_ident("logs-user");

    // Each append is one snapshot. Build well past any sane retention.
    const COMMITS: usize = 12;
    for i in 0..COMMITS {
        let batch = events_to_record_batch(&[Event::now(format!("row {i}"))]).unwrap();
        ice.append_to_table(&ident, batch, &[])
            .await
            .expect("append to index");
    }
    let before = ice
        .snapshot_count_for(&ident)
        .await
        .expect("snapshot count");
    assert!(
        before >= COMMITS,
        "fixture must accumulate snapshots (got {before})"
    );

    // query_audit is optional. Its absence must not prevent the user index from
    // receiving the same expiry pass.
    assert!(ice
        .drop_query_audit_table()
        .await
        .expect("drop query_audit"));

    let compactor = Compactor::new(tmp.path().join("wal"), ice.clone())
        .with_snapshot_expiry(ExpireConfig::new(Duration::from_secs(0), 3));
    compactor.run_expire_once().await.expect("expire");

    let after = ice
        .snapshot_count_for(&ident)
        .await
        .expect("snapshot count");
    assert!(
        after < before,
        "user-index snapshots must be trimmed: {before} -> {after}"
    );
    assert!(
        after <= 5,
        "must converge near retain_last=3, got {after} (was {before})"
    );
}

#[tokio::test]
async fn audit_maintenance_preserves_exact_rows_aggregates_and_snapshot_bound() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let audit_ident = ice.query_audit_table_ident();

    const COMMITS: usize = 8;
    for commit in 0..COMMITS {
        ice.append_query_audit(audit_batch(commit))
            .await
            .expect("append query_audit");
    }
    let files_before = ice
        .live_data_files(&audit_ident)
        .await
        .expect("audit files before")
        .len();
    assert_eq!(files_before, COMMITS, "one small audit file per append");

    // Force an append into the rewrite's first catalog-CAS window. The rewrite
    // must retry from the newer base and retain those rows rather than replacing
    // the snapshot the append just committed.
    let concurrent_writer = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .expect("second audit writer");
    let catalog = TestCatalog::new(ice.catalog().clone())
        .before_first_update_with_base(move || {
            let concurrent_writer = concurrent_writer.clone();
            async move {
                concurrent_writer
                    .append_query_audit(audit_batch(COMMITS))
                    .await
                    .expect("concurrent audit append");
            }
        })
        .shared();
    let maintained = ice.as_ref().clone().with_catalog_for_test(catalog.clone());
    let stats = maintained
        .recluster_all_indexes(ReclusterPolicy::default())
        .await
        .expect("flat maintenance pass");
    assert!(catalog.fired(), "the concurrent-append hook must run");
    assert!(
        stats.iter().any(|stat| stat.files_removed > 0),
        "the all-table pass must compact query_audit"
    );
    let files_after = ice
        .live_data_files(&audit_ident)
        .await
        .expect("audit files after")
        .len();
    assert!(
        files_after < files_before,
        "audit small files must consolidate: {files_before} -> {files_after}"
    );

    let compactor = Compactor::new(tmp.path().join("wal"), ice.clone())
        .with_snapshot_expiry(ExpireConfig::new(Duration::ZERO, 3));
    compactor.run_expire_once().await.expect("expire audit");

    // A fresh append after both maintenance commits must remain writable and
    // covered by the exact aggregate, without losing or duplicating old rows.
    ice.append_query_audit(audit_batch(COMMITS + 1))
        .await
        .expect("fresh audit append");
    compactor
        .run_expire_once()
        .await
        .expect("expire after fresh append");

    let snapshots = ice
        .snapshot_count_for(&audit_ident)
        .await
        .expect("audit snapshot count");
    assert!(
        snapshots <= 5,
        "query_audit snapshots must converge near retain_last=3, got {snapshots}"
    );
    assert_audit_rows_equal(&ice, expected_audit_rows(0..=COMMITS + 1)).await;
    let mut status_counts = ice
        .grouped_counts_with_summary("query_audit", "status", None, None)
        .await
        .expect("audit status aggregate")
        .expect("status aggregate coverage")
        .to_rows();
    status_counts.sort();
    assert_eq!(
        status_counts,
        vec![
            (Some("ok".to_string()), (COMMITS + 2) as u64),
            (Some("rejected".to_string()), (COMMITS + 2) as u64),
        ],
        "query_audit aggregate coverage must include old and fresh rows"
    );
}

#[tokio::test]
async fn leveled_all_table_maintenance_compacts_query_audit() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let audit_ident = ice.query_audit_table_ident();
    for commit in 0..8 {
        ice.append_query_audit(audit_batch(commit))
            .await
            .expect("append query_audit");
    }
    let before = ice.live_data_files(&audit_ident).await.unwrap().len();
    ice.recluster_all_indexes_leveled(
        &LevelPolicy::default(),
        ReclusterPolicy::default(),
        &LeveledPassOptions::default(),
    )
    .await
    .expect("leveled maintenance pass");
    let after = ice.live_data_files(&audit_ident).await.unwrap().len();
    assert!(
        after < before,
        "leveled maintenance must consolidate query_audit: {before} -> {after}"
    );
    assert_audit_rows_equal(&ice, expected_audit_rows(0..=7)).await;
}
