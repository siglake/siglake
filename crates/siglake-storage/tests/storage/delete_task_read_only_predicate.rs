//! The compactor plans a delete task's `predicate_sql` READ-ONLY.
//!
//! WHY THIS EXISTS (task #1543, follow-up to #1523). The query server refuses
//! DDL/DML/statements on every request-derived SQL string since #1523, where
//! DataFusion's permissive default turned `COPY (SELECT …) TO '/abs/path'` into
//! an arbitrary filesystem write on the query pod. The compactor's delete-task
//! executor plans a client-supplied fragment too — twice, in
//! `SELECT count(*) … WHERE {fragment}` and
//! `SELECT * … WHERE ({fragment}) IS NOT TRUE`
//! — and did so under that same permissive default.
//!
//! No escape through those two templates has ever been demonstrated and none is
//! claimed here: `delete_tasks_routes::validate_delete_predicate_fragment`
//! requires the wrapped text to parse as exactly one `Statement::Query`. But the
//! fragment arrives at the compactor as a PERSISTED warehouse object, written by
//! a different crate in a different process, and "the validator is far away and
//! probably still correct" is not a boundary. The planner is.
//!
//! So there are two halves below:
//!
//!   * [`read_only_helper_refuses_the_shapes_the_permissive_default_executes`]
//!     A/Bs `siglake_storage::plan_read_only_sql` against plain `ctx.sql` on the
//!     exact statements #1523 measured, so the options are shown to be doing
//!     work rather than merely being spelled.
//!   * [`a_persisted_non_read_predicate_is_refused_with_nothing_written`] drives
//!     hostile fragments in as persisted delete-task records — bypassing the
//!     REST validator entirely, as a row written by an older or compromised
//!     submitter would — and asserts the sweep fails the task and writes nothing
//!     outside the task ledger itself.
//!
//! MEASURED, so nobody has to re-derive it: the persisted arm passes against the
//! pre-#1543 `ctx.sql` call sites as well. That is the finding, not a weakness of
//! the test — a fragment spliced into `… WHERE ({fragment})` cannot become a
//! DDL/DML statement, DataFusion refuses more than one statement per `sql()`
//! call, and every shape below therefore dies in the parser. The persisted arm
//! pins the OUTCOME (refused, nothing written) for the path that has no validator
//! in front of it; the helper arm above is the one that discriminates, and it
//! fails if either `with_allow_*` is dropped. Should a future template ever hand
//! the fragment a position where a statement can start, this arm turns
//! discriminating on its own.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::Utc;
use datafusion::prelude::SessionContext;

use siglake_core::index_config::{FieldType, IndexConfig};
use siglake_core::{events_to_record_batch, Event};
use siglake_storage::iceberg::{DeleteTaskState, IcebergContext};

use crate::fixture_clock::fixture_base;

fn logs_index(index_id: &str) -> IndexConfig {
    let mut config = IndexConfig::builtin_events();
    config.index_id = index_id.to_string();
    config
}

fn bloom_columns(config: &IndexConfig) -> Vec<String> {
    config
        .doc_mapping
        .tag_fields
        .iter()
        .filter_map(|name| {
            config
                .doc_mapping
                .field_mappings
                .iter()
                .find(|field| field.name == *name)
                .and_then(|field| match &field.field_type {
                    FieldType::Text { .. } | FieldType::Json => Some(name.clone()),
                    _ => None,
                })
        })
        .collect()
}

fn event_at(ts: chrono::DateTime<Utc>, host: &str, raw: &str) -> Event {
    Event {
        timestamp: ts,
        host: host.to_string(),
        source: "/var/log/app.log".to_string(),
        sourcetype: "app:json".to_string(),
        index: "main".to_string(),
        raw: raw.to_string(),
        attributes: None,
    }
}

async fn append_index_events(ice: &IcebergContext, config: &IndexConfig, events: &[Event]) {
    let batch = events_to_record_batch(events).unwrap();
    let blooms = bloom_columns(config);
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    ice.append_to_table(&ice.index_table_ident(&config.index_id), batch, &bloom_refs)
        .await
        .unwrap();
}

async fn count_index_rows(ice: &IcebergContext, index_id: &str) -> i64 {
    let ctx = SessionContext::new();
    ice.register_table_with_datafusion(&ctx, &ice.index_table_ident(index_id), index_id)
        .await
        .unwrap();
    let batches = ctx
        .sql(format!("SELECT count(*) AS n FROM \"{index_id}\"").as_str())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

/// Every regular file under `root`, as `path -> length`. A delete-task rewrite
/// writes NEW Parquet files under fresh UUID names and a new metadata/snapshot
/// document, so a path-and-length fingerprint catches any data movement; the
/// executor's own `Running`/`Failed` status writes land under
/// `_siglake/config/delete_tasks/` and are excluded by the caller.
fn fingerprint(root: &Path) -> BTreeMap<PathBuf, u64> {
    fn walk(dir: &Path, out: &mut BTreeMap<PathBuf, u64>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(kind) if kind.is_dir() => walk(&path, out),
                Ok(kind) if kind.is_file() => {
                    let len = entry.metadata().map(|meta| meta.len()).unwrap_or(0);
                    out.insert(path, len);
                }
                _ => {}
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, &mut out);
    out
}

/// Paths that appeared or changed size, other than delete-task status records.
fn unauthorized_writes(
    before: &BTreeMap<PathBuf, u64>,
    after: &BTreeMap<PathBuf, u64>,
) -> Vec<PathBuf> {
    after
        .iter()
        .filter(|(path, len)| before.get(*path) != Some(*len))
        .map(|(path, _)| path.clone())
        .filter(|path| {
            !path
                .to_string_lossy()
                .contains("_siglake/config/delete_tasks/")
        })
        .collect()
}

/// Persist a delete-task record straight into the warehouse, exactly where
/// [`IcebergContext::create_delete_task`] would put it, without going anywhere
/// near `siglake-query-server`'s predicate validator. This is the threat the
/// compactor actually faces: it reads tasks, it does not admit them.
fn persist_delete_task(
    warehouse: &Path,
    namespace: &str,
    index_id: &str,
    predicate_sql: &str,
) -> uuid::Uuid {
    let task_id = uuid::Uuid::now_v7();
    let record = serde_json::to_vec(&serde_json::json!({
        "task_id": task_id,
        "index_id": index_id,
        "predicate_sql": predicate_sql,
        "start_ts": null,
        "end_ts": null,
        "created_at": Utc::now(),
        "state": "pending",
        "error": null,
        "files_rewritten": 0,
        "rows_deleted": 0,
    }))
    .unwrap();
    let path = warehouse
        .join("_siglake/config/delete_tasks")
        .join(namespace)
        .join(format!("{task_id}.json"));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, record).unwrap();
    task_id
}

/// The helper itself, A/B'd against the default it replaces.
///
/// Each case is planned twice: once with plain `ctx.sql` (what the compactor
/// did before #1543) and once with `plan_read_only_sql`. The permissive arm has
/// to actually perform the side effect, or the read-only arm is proving nothing
/// — a test built on statements DataFusion rejects anyway would pass against the
/// pre-fix code.
#[tokio::test]
async fn read_only_helper_refuses_the_shapes_the_permissive_default_executes() {
    let tmp = tempfile::tempdir().unwrap();
    let permissive_out = tmp.path().join("permissive.csv");
    let refused_out = tmp.path().join("refused.csv");

    // 1. COPY — DML. The #1523 filesystem write.
    let ctx = SessionContext::new();
    ctx.sql(&format!(
        "COPY (SELECT 1 AS a) TO '{}'",
        permissive_out.display()
    ))
    .await
    .expect("the permissive default plans COPY")
    .collect()
    .await
    .expect("the permissive default executes COPY");
    assert!(
        permissive_out.exists(),
        "the A/B arm did not reproduce the defect, so the refusal below proves nothing"
    );

    let refused = siglake_storage::plan_read_only_sql(
        &ctx,
        &format!("COPY (SELECT 1 AS a) TO '{}'", refused_out.display()),
    )
    .await;
    assert!(
        refused.is_err(),
        "COPY must be refused at plan time: {refused:?}"
    );
    assert!(
        !refused_out.exists(),
        "a refusal that still writes the file is not a refusal"
    );

    // 2. CREATE EXTERNAL TABLE — DDL, which DataFusion runs EAGERLY inside
    //    `sql()`, i.e. the side effect lands before the caller sees a
    //    `DataFrame` at all.
    let external_dir = tmp.path().join("external");
    std::fs::create_dir_all(&external_dir).unwrap();
    let ddl = format!(
        "CREATE EXTERNAL TABLE outside (a INT) STORED AS PARQUET LOCATION '{}'",
        external_dir.display()
    );
    let ddl_ctx = SessionContext::new();
    ddl_ctx
        .sql(&ddl)
        .await
        .expect("the permissive default runs the DDL eagerly");
    assert!(
        ddl_ctx.table_exist("outside").unwrap(),
        "the A/B arm did not reproduce the defect"
    );

    let guarded = SessionContext::new();
    let refused = siglake_storage::plan_read_only_sql(&guarded, &ddl).await;
    assert!(
        refused.is_err(),
        "CREATE EXTERNAL TABLE must be refused: {refused:?}"
    );
    assert!(
        !guarded.table_exist("outside").unwrap(),
        "the refused DDL still registered its table — the eager path ran"
    );

    // 3. A session statement.
    let refused =
        siglake_storage::plan_read_only_sql(&guarded, "SET datafusion.execution.batch_size = 1")
            .await;
    assert!(
        refused.is_err(),
        "session statements must be refused: {refused:?}"
    );

    // 4. And the thing delete tasks actually need still plans.
    siglake_storage::plan_read_only_sql(
        &guarded,
        "SELECT count(*) AS n FROM (SELECT 1 AS a) WHERE a = 1",
    )
    .await
    .expect("an ordinary read must still plan");
}

/// A hostile fragment persisted as a delete-task record is refused by the
/// compactor, and the sweep leaves the warehouse byte-identical apart from the
/// task's own status record.
#[tokio::test]
async fn a_persisted_non_read_predicate_is_refused_with_nothing_written() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let target = tmp.path().join("exfiltrated.csv");
    let external_dir = tmp.path().join("external");
    std::fs::create_dir_all(&external_dir).unwrap();

    let config = logs_index("logs");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    ice.create_index(&config).await.unwrap();
    let now = fixture_base();
    append_index_events(
        &ice,
        &config,
        &[
            event_at(now, "victim", "victim row"),
            event_at(now, "keep", "kept row"),
        ],
    )
    .await;
    let namespace = ice.namespace().to_string();
    let rows_before = count_index_rows(&ice, "logs").await;
    assert_eq!(rows_before, 2);

    // Fragments are inserted into `SELECT … WHERE ({fragment})`, so each one
    // closes the parenthesis first and comments out the tail — the standard
    // injection shape. Whichever layer refuses (parser or read-only options),
    // the requirement is the same: refused, and nothing written.
    let hostile = [
        format!(
            "host = 'victim') TO '{}' STORED AS CSV --",
            target.display()
        ),
        format!(
            "host = 'victim'; COPY (SELECT raw FROM candidate_file) TO '{}'",
            target.display()
        ),
        format!(
            "host = 'victim'); CREATE EXTERNAL TABLE t (a INT) STORED AS PARQUET LOCATION '{}' --",
            external_dir.display()
        ),
        format!(
            "host = 'victim') UNION ALL SELECT 1; COPY (SELECT 1) TO '{}' --",
            target.display()
        ),
    ];

    for predicate in &hostile {
        let task_id = persist_delete_task(&warehouse, &namespace, "logs", predicate);
        let before = fingerprint(&warehouse);
        let outcome = ice.execute_delete_tasks("logs").await.unwrap();
        let after = fingerprint(&warehouse);

        assert_eq!(
            outcome.tasks_failed, 1,
            "predicate {predicate:?} was not refused: {outcome:?}"
        );
        assert_eq!(outcome.tasks_completed, 0, "{predicate:?}: {outcome:?}");
        assert_eq!(outcome.files_rewritten, 0, "{predicate:?}: {outcome:?}");
        assert_eq!(outcome.rows_deleted, 0, "{predicate:?}: {outcome:?}");

        let stored = ice.get_delete_task(task_id).await.unwrap().unwrap();
        assert_eq!(
            stored.state,
            DeleteTaskState::Failed,
            "{predicate:?} must be recorded as failed, not silently dropped"
        );

        assert!(!target.exists(), "{predicate:?} wrote {}", target.display());
        assert!(
            std::fs::read_dir(&external_dir).unwrap().next().is_none(),
            "{predicate:?} wrote into {}",
            external_dir.display()
        );
        let stray = unauthorized_writes(&before, &after);
        assert!(
            stray.is_empty(),
            "{predicate:?} left files behind outside the task ledger: {stray:?}"
        );
        assert_eq!(
            count_index_rows(&ice, "logs").await,
            rows_before,
            "{predicate:?} changed the table"
        );
    }
}

/// The control: read-only planning must not have broken ordinary deletion. The
/// same warehouse, the same executor, a legitimate predicate — the victim row
/// goes and the kept row stays.
#[tokio::test]
async fn an_ordinary_delete_task_still_rewrites_the_file() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let config = logs_index("logs");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    ice.create_index(&config).await.unwrap();
    let now = fixture_base();
    append_index_events(
        &ice,
        &config,
        &[
            event_at(now, "victim", "victim row"),
            event_at(now, "keep", "kept row"),
        ],
    )
    .await;

    let task = ice
        .create_delete_task("logs", "host = 'victim'", None, None)
        .await
        .unwrap();
    let outcome = ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(outcome.tasks_completed, 1, "{outcome:?}");
    assert_eq!(outcome.tasks_failed, 0, "{outcome:?}");
    assert_eq!(outcome.rows_deleted, 1, "{outcome:?}");
    assert_eq!(
        ice.get_delete_task(task.task_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        DeleteTaskState::Done
    );
    assert_eq!(count_index_rows(&ice, "logs").await, 1);
}
