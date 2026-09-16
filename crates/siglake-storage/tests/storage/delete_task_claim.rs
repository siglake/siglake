//! One pending delete task has one owner.
//!
//! THE DEFECT THIS GUARDS. Its sibling `delete_task_durability.rs` covers the
//! previous layer: each task is its own warehouse object, so writers cannot
//! erase *each other's* tasks. What that did not give a task was an owner. The
//! executor read the pending set, wrote `Running` into the task's record and
//! later the terminal state, with no CAS and no lease on the record — so two
//! processes sweeping the same index at the same time each executed the SAME
//! pending task and each wrote a terminal record, doubling the rewrite and the
//! `files_rewritten`/`rows_deleted` accounting. The compactor's cluster-wide
//! `maintenance_lease("delete_tasks")` was the only thing preventing it, and it
//! covers neither `siglake delete-tasks execute` run by hand nor a second
//! control plane, and returns `true` outright when no catalog is configured.
//!
//! A pending task is now claimed by a create-only write of a sibling
//! `{task_id}.claim` key before it executes. The loser of that write reports
//! the task as already claimed instead of re-running it.
//!
//! WHY NO CLAIM IS EVER COLLECTED, terminal ones included (#2129). The pending
//! set is a snapshot read before the claim, so a delayed executor can be
//! holding a `Pending` copy of a task another executor has already driven to
//! `done` or `failed` — and its loop never re-reads the record. Observing a
//! terminal record therefore does not make deleting its claim safe: the last
//! two tests here drive exactly that stale-view executor into a live claim and
//! pin that it skips, leaving the terminal record and its accounting alone.
//! The cost of retaining claims is one extra LIST entry per task; the record
//! reader takes `*.json` only, so no extra GET.

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};

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

/// Seed a warehouse with an index holding `victim`/`keep` rows spread over
/// several data files, and one pending delete task for the victim.
///
/// Four appends, four files: the events sit inside one day partition
/// ([`fixture_base`]), so the count is the number of appends and not a function
/// of what time the test ran.
async fn seed(warehouse: &std::path::Path) -> uuid::Uuid {
    seed_with_predicate(warehouse, "host = 'victim'").await
}

/// [`seed`] with the task's predicate chosen by the caller, so a test can seed
/// a task that fails at execution instead of completing.
async fn seed_with_predicate(warehouse: &std::path::Path, predicate: &str) -> uuid::Uuid {
    let config = logs_index("logs");
    let ice = IcebergContext::open(warehouse).await.unwrap();
    ice.create_index(&config).await.unwrap();
    let now = fixture_base();
    for batch in 0..4 {
        append_index_events(
            &ice,
            &config,
            &[
                event_at(
                    now - ChronoDuration::hours(2) + ChronoDuration::minutes(batch),
                    "victim",
                    "victim row",
                ),
                event_at(
                    now - ChronoDuration::hours(2) + ChronoDuration::minutes(batch + 1),
                    "keep",
                    "kept row",
                ),
            ],
        )
        .await;
    }
    let task = ice
        .create_delete_task("logs", predicate, None, None)
        .await
        .unwrap();
    task.task_id
}

async fn count_rows(ice: &IcebergContext, predicate: &str) -> i64 {
    let ctx = datafusion::prelude::SessionContext::new();
    ice.register_table_with_datafusion(&ctx, &ice.index_table_ident("logs"), "logs")
        .await
        .unwrap();
    let batches = ctx
        .sql(&format!(
            "SELECT count(*) AS n FROM \"logs\" WHERE {predicate}"
        ))
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

/// THE ACCEPTANCE CASE. Two executors, each with its own `IcebergContext` (its
/// own catalog handle and its own object-store operator, as two control planes
/// have), sweep the same index at the same instant. Exactly one executes the
/// task; the other either reports it already claimed or lists after completion
/// and sees no pending task. In both cases it rewrites nothing.
///
/// Real OS threads, each with its own current-thread runtime, rather than two
/// tasks on one shared runtime: the two sweeps then interleave the way two
/// processes do, with nothing about tokio's scheduling standing between them.
/// A/B'd against the pre-claim code — with the claim removed this fails 20 runs
/// out of 20 (both executors take the task; the loser of the Iceberg commit
/// reports `tasks_failed=1`).
#[test]
fn two_executors_racing_one_pending_task_produce_exactly_one_execution() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let task_id = rt.block_on(seed(&warehouse));
    drop(rt);

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let handles: Vec<_> = (0..2)
        .map(|_| {
            let warehouse = warehouse.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let ice = IcebergContext::open(&warehouse).await.unwrap();
                    barrier.wait();
                    ice.execute_delete_tasks("logs").await.unwrap()
                })
            })
        })
        .collect();
    let outcomes: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().expect("executor thread"))
        .collect();

    let executed: Vec<_> = outcomes
        .iter()
        .filter(|o| o.tasks_completed + o.tasks_failed > 0)
        .collect();
    assert_eq!(
        executed.len(),
        1,
        "exactly one executor may run the task: {outcomes:?}"
    );
    let winner = executed[0];
    assert_eq!(winner.tasks_completed, 1, "{outcomes:?}");
    assert_eq!(winner.tasks_failed, 0, "{outcomes:?}");
    assert_eq!(winner.tasks_already_claimed, 0, "{outcomes:?}");
    assert!(
        winner.files_rewritten > 0,
        "the winner must have done the rewrite: {outcomes:?}"
    );

    let loser = outcomes
        .iter()
        .find(|o| !std::ptr::eq(*o, winner))
        .expect("two outcomes");
    // The loser either listed while the task was still pending and lost its
    // claim, or listed after the winner's terminal write and saw an empty
    // pending set. Mixed accounting would describe neither valid ordering.
    assert!(
        matches!(
            (loser.tasks_examined, loser.tasks_already_claimed),
            (0, 0) | (1, 1)
        ),
        "the loser must lose the claim or observe an empty sweep: {outcomes:?}"
    );
    assert_eq!(loser.tasks_completed, 0, "{outcomes:?}");
    assert_eq!(loser.tasks_failed, 0, "{outcomes:?}");
    assert_eq!(loser.files_rewritten, 0, "{outcomes:?}");
    assert_eq!(loser.rows_deleted, 0, "{outcomes:?}");

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let reopened = IcebergContext::open(&warehouse).await.unwrap();
        let stored = reopened.get_delete_task(task_id).await.unwrap().unwrap();
        assert_eq!(stored.state, DeleteTaskState::Done, "{stored:?}");
        assert_eq!(stored.rows_deleted, winner.rows_deleted, "{stored:?}");
        assert_eq!(
            count_rows(&reopened, "host = 'victim'").await,
            0,
            "the deletion must have happened"
        );
        assert_eq!(
            count_rows(&reopened, "host = 'keep'").await,
            4,
            "a second execution would duplicate or drop the retained rows"
        );
    });
}

/// A sweep that starts after the winner's terminal write never sees the task:
/// pending-task filtering happens before sweep accounting is initialized.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_executor_listing_after_completion_reports_an_empty_sweep() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let task_id = seed(&warehouse).await;
    let winner = IcebergContext::open(&warehouse).await.unwrap();
    let late_executor = IcebergContext::open(&warehouse).await.unwrap();

    let completed = winner.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(completed.tasks_completed, 1, "{completed:?}");
    assert_eq!(
        winner
            .get_delete_task(task_id)
            .await
            .unwrap()
            .expect("record")
            .state,
        DeleteTaskState::Done
    );

    let late = late_executor.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(late.tasks_examined, 0, "{late:?}");
    assert_eq!(late.tasks_already_claimed, 0, "{late:?}");
    assert_eq!(late.tasks_completed, 0, "{late:?}");
    assert_eq!(late.tasks_failed, 0, "{late:?}");
    assert_eq!(late.files_rewritten, 0, "{late:?}");
    assert_eq!(late.rows_deleted, 0, "{late:?}");
}

/// A dry run claims nothing: `preview` must not poison the `execute` that
/// follows it, which is the ordinary operator sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dry_run_does_not_claim_the_task_it_previews() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let task_id = seed(&warehouse).await;
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    let preview = ice.preview_delete_tasks("logs").await.unwrap();
    assert!(preview.dry_run);
    assert_eq!(preview.tasks_examined, 1);
    assert_eq!(preview.tasks_already_claimed, 0);
    assert!(preview.files_rewritten > 0, "{preview:?}");
    assert_eq!(
        ice.get_delete_task(task_id).await.unwrap().unwrap().state,
        DeleteTaskState::Pending,
        "a dry run does not move the task"
    );
    // A second preview must also be free of the first one's leavings.
    let again = ice.preview_delete_tasks("logs").await.unwrap();
    assert_eq!(again, preview, "a dry run is repeatable");

    let applied = ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(applied.tasks_completed, 1, "{applied:?}");
    assert_eq!(applied.tasks_already_claimed, 0, "{applied:?}");
    assert_eq!(
        ice.get_delete_task(task_id).await.unwrap().unwrap().state,
        DeleteTaskState::Done
    );
}

/// Exclusion outlives the executor. A task left `Pending` with its claim
/// present — the shape a process crashed somewhere around its `rewrite_files`
/// commit leaves behind — is NOT picked up by the next sweep. Reclaiming it on
/// age alone, with no fencing token the Iceberg commit could check, would let
/// the evicted owner's rewrite land after its successor's; recovery is the
/// explicit resubmission a `failed` task already takes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_abandoned_under_its_claim_is_not_re_executed() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let task_id = seed(&warehouse).await;
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    // Stand in for the dead executor: its claim object, and nothing else.
    let claim = warehouse
        .join("_siglake/config/delete_tasks")
        .join(ice.namespace().to_string())
        .join(format!("{task_id}.claim"));
    std::fs::create_dir_all(claim.parent().unwrap()).unwrap();
    std::fs::write(&claim, b"{}").unwrap();

    let outcome = ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(outcome.tasks_examined, 1, "{outcome:?}");
    assert_eq!(outcome.tasks_already_claimed, 1, "{outcome:?}");
    assert_eq!(outcome.tasks_completed, 0, "{outcome:?}");
    assert_eq!(outcome.files_rewritten, 0, "{outcome:?}");
    assert_eq!(
        ice.get_delete_task(task_id).await.unwrap().unwrap().state,
        DeleteTaskState::Pending,
        "the abandoned task keeps its state; it does not become Running or Failed"
    );
    assert_eq!(
        count_rows(&ice, "host = 'victim'").await,
        4,
        "nothing was rewritten under someone else's claim"
    );
}

/// The claim objects share the record directory. They must not be mistaken for
/// task records, whose reader errors on anything it cannot parse as a
/// `DeleteTask` — that is deliberate, so an acknowledged request is never
/// silently skipped, and it is exactly what a `.json`-suffixed claim would trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claim_objects_do_not_break_listing_the_tasks() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let task_id = seed(&warehouse).await;
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    ice.execute_delete_tasks("logs").await.unwrap();

    let listed = ice.list_delete_tasks(Some("logs")).await.unwrap();
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].task_id, task_id);
    assert_eq!(listed[0].state, DeleteTaskState::Done);
    assert!(
        ice.get_delete_task(task_id).await.unwrap().is_some(),
        "the record is still readable beside its claim"
    );
}

/// Names of the objects in the namespace's delete-task record directory. The
/// claim is a sibling of the record, so this is the LIST-entry cost the
/// README's limitation quotes: two entries per task, one of them `*.json`.
fn record_dir_entries(warehouse: &std::path::Path, namespace: &str) -> Vec<String> {
    let dir = warehouse
        .join("_siglake/config/delete_tasks")
        .join(namespace);
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// A delayed executor holding a stale `Pending` view of a task that has since
/// completed. This is why a terminal task's claim is NOT collectable, however
/// safe that looks: the pending set is read before the claim, so executor B can
/// still be carrying `Pending` for a task executor A has already driven to
/// `done`, and nothing in B's loop re-reads the record. The surviving claim is
/// the whole of B's exclusion.
///
/// A/B'd against the sweep this argues against: with A's claim removed before B
/// resumes, B claims the task, runs it and reports `tasks_completed=1` — and
/// because A already deleted the rows, its terminal record replaces A's
/// `files_rewritten`/`rows_deleted` with its own zeroes. The audit trail of
/// what the deletion actually did is what a claim sweep costs.
///
/// Deterministic by construction rather than by racing: B's read of the pending
/// set and B's claim are separate calls here, with A's entire sweep in between.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_pending_view_of_a_done_task_is_refused_by_its_claim() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let task_id = seed(&warehouse).await;
    // Two contexts, as two control planes have.
    let executor_b = IcebergContext::open(&warehouse).await.unwrap();
    let executor_a = IcebergContext::open(&warehouse).await.unwrap();
    let namespace = executor_b.namespace().to_string();

    // B reads the pending set and then stalls (GC pause, slow store, a long
    // rewrite of an earlier task in the same sweep).
    let stale = executor_b
        .read_pending_delete_tasks_for_test("logs")
        .await
        .unwrap();
    assert_eq!(stale.len(), 1, "{stale:?}");
    assert_eq!(stale[0].task_id, task_id);
    assert_eq!(stale[0].state, DeleteTaskState::Pending, "{stale:?}");

    // A runs the task to completion while B is stalled.
    let a = executor_a.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(a.tasks_completed, 1, "{a:?}");
    assert!(a.files_rewritten > 0, "{a:?}");
    let after_a = executor_a
        .get_delete_task(task_id)
        .await
        .unwrap()
        .expect("record");
    assert_eq!(after_a.state, DeleteTaskState::Done, "{after_a:?}");

    // B resumes on its stale vector: the record says `done`, B's copy still
    // says `Pending`, and only the claim stands between them.
    let b = executor_b
        .execute_pending_delete_tasks_for_test("logs", stale)
        .await
        .unwrap();
    assert_eq!(b.tasks_examined, 1, "{b:?}");
    assert_eq!(b.tasks_already_claimed, 1, "{b:?}");
    assert_eq!(b.tasks_completed, 0, "{b:?}");
    assert_eq!(b.tasks_failed, 0, "{b:?}");
    assert_eq!(b.files_rewritten, 0, "{b:?}");
    assert_eq!(b.rows_deleted, 0, "{b:?}");

    // Terminal accounting is A's, unreplaced and unamended: B must not have
    // written `Running`, a second `Done`, or doubled counters.
    let after_b = executor_b
        .get_delete_task(task_id)
        .await
        .unwrap()
        .expect("record");
    assert_eq!(after_b, after_a, "B rewrote A's terminal record");
    assert_eq!(
        count_rows(&executor_b, "host = 'victim'").await,
        0,
        "{after_b:?}"
    );
    assert_eq!(
        count_rows(&executor_b, "host = 'keep'").await,
        4,
        "a second rewrite would duplicate or drop the retained rows"
    );
    // The claim is still there, in a terminal state, and is what did this.
    assert_eq!(
        record_dir_entries(&warehouse, &namespace),
        vec![format!("{task_id}.claim"), format!("{task_id}.json")],
        "claims are retained in every state: one extra LIST entry per task"
    );
}

/// The `failed` half of the same hazard. A terminal-by-failure task is the more
/// tempting collection target — nothing was rewritten, so the claim looks like
/// pure litter — but a delayed executor's stale `Pending` copy is identical, and
/// re-running the task would replace a recorded failure (and its `error`) with a
/// fresh verdict. Recovery from a `failed` task is an explicit resubmission
/// under a new task id, never a second run of this one.
///
/// Same A/B: remove A's claim before B resumes and B re-executes, reporting
/// `tasks_failed=1` a second time and rewriting the record's `error`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_pending_view_of_a_failed_task_is_refused_by_its_claim() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    // A predicate that cannot plan: the executor fails the task rather than
    // failing the sweep. `create_delete_task` does not validate predicates, so
    // this is a shape an accepted request really can take.
    let task_id = seed_with_predicate(&warehouse, "no_such_column = 'victim'").await;
    let executor_b = IcebergContext::open(&warehouse).await.unwrap();
    let executor_a = IcebergContext::open(&warehouse).await.unwrap();
    let namespace = executor_b.namespace().to_string();

    let stale = executor_b
        .read_pending_delete_tasks_for_test("logs")
        .await
        .unwrap();
    assert_eq!(stale.len(), 1, "{stale:?}");
    assert_eq!(stale[0].state, DeleteTaskState::Pending, "{stale:?}");

    let a = executor_a.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(a.tasks_failed, 1, "{a:?}");
    assert_eq!(a.tasks_completed, 0, "{a:?}");
    assert_eq!(a.files_rewritten, 0, "{a:?}");
    let after_a = executor_a
        .get_delete_task(task_id)
        .await
        .unwrap()
        .expect("record");
    assert_eq!(after_a.state, DeleteTaskState::Failed, "{after_a:?}");
    assert!(after_a.error.is_some(), "{after_a:?}");

    let b = executor_b
        .execute_pending_delete_tasks_for_test("logs", stale)
        .await
        .unwrap();
    assert_eq!(b.tasks_examined, 1, "{b:?}");
    assert_eq!(b.tasks_already_claimed, 1, "{b:?}");
    assert_eq!(b.tasks_completed, 0, "{b:?}");
    assert_eq!(
        b.tasks_failed, 0,
        "a claimed task is skipped, not re-failed: {b:?}"
    );

    let after_b = executor_b
        .get_delete_task(task_id)
        .await
        .unwrap()
        .expect("record");
    assert_eq!(
        after_b, after_a,
        "B replaced the recorded failure instead of skipping it"
    );
    assert_eq!(
        count_rows(&executor_b, "host = 'victim'").await,
        4,
        "the failed task deleted nothing, and neither did B"
    );
    assert_eq!(
        record_dir_entries(&warehouse, &namespace),
        vec![format!("{task_id}.claim"), format!("{task_id}.json")],
        "a failed task keeps its claim too"
    );
}
