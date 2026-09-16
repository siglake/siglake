//! The read-only observation of non-terminal delete tasks (#2114's design,
//! implemented by #2245).
//!
//! THE GAP THIS CLOSES. A delete task left `running` — by a process that died
//! between the `Running` write and the terminal write, by a terminal write that
//! failed after the rewrite committed, or by the sweep watchdog cancelling the
//! stage — is never looked at again: the sweep filters `state == Pending`.
//! Nothing counted or aged those records, so an acknowledged GDPR request could
//! sit unexecuted until an operator listed every task and eyeballed `state`.
//!
//! WHAT THIS LAYER IS ALLOWED TO DO: look. It writes no record, no claim and no
//! state, and it makes no lifecycle change — no `running -> pending`, no reset,
//! no retry, no claim release or takeover. Reclaiming on age alone, without a
//! fencing token the Iceberg commit could check, would let an evicted owner's
//! rewrite land after its successor's (`claim_delete_task`). Classification
//! against a bound lives in the compactor; this reports a state and an age.
//!
//! The claim's age is the OBJECT's `last_modified`, never the body's
//! `claimed_at`: a crashed executor is exactly the case that can leave the body
//! empty, truncated or malformed, and an age that vanishes there is an age that
//! fails on the case it exists for.

use chrono::{Duration as ChronoDuration, Utc};

use siglake_core::index_config::IndexConfig;
use siglake_storage::iceberg::{
    DeleteTask, DeleteTaskState, IcebergContext, NonTerminalDeleteTask, ObservedDeleteTaskClaim,
};

fn logs_index(index_id: &str) -> IndexConfig {
    let mut config = IndexConfig::builtin_events();
    config.index_id = index_id.to_string();
    config
}

/// A warehouse with one index and no data: the observer reads the task ledger
/// and the claim objects, never the table, so nothing here needs rows.
async fn open(warehouse: &std::path::Path) -> IcebergContext {
    let ice = IcebergContext::open(warehouse).await.unwrap();
    ice.create_index(&logs_index("logs")).await.unwrap();
    ice
}

async fn submit(ice: &IcebergContext, predicate: &str) -> DeleteTask {
    ice.create_delete_task("logs", predicate, None, None)
        .await
        .unwrap()
}

/// Move a task to `state` the way the executor would, without executing it.
async fn set_state(ice: &IcebergContext, task: &DeleteTask, state: DeleteTaskState) -> DeleteTask {
    let mut moved = task.clone();
    moved.state = state;
    ice.write_delete_task_record_for_test(&moved).await.unwrap();
    moved
}

fn claim_path(
    warehouse: &std::path::Path,
    ice: &IcebergContext,
    task: &DeleteTask,
) -> std::path::PathBuf {
    warehouse
        .join("_siglake/config/delete_tasks")
        .join(ice.namespace().to_string())
        .join(format!("{}.claim", task.task_id))
}

/// Stand in for an executor's claim, with the object's modification time under
/// the test's control. `body` is what the dead executor managed to write —
/// `b"{}"` is the shape the crashed-executor fixture in `delete_task_claim.rs`
/// uses, and the age must survive it.
fn plant_claim(
    warehouse: &std::path::Path,
    ice: &IcebergContext,
    task: &DeleteTask,
    body: &[u8],
    age: ChronoDuration,
) {
    let path = claim_path(warehouse, ice, task);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, body).unwrap();
    // No sleeping on wall time anywhere in this file: the claim is aged by
    // setting its mtime, which is the field `stat` reports.
    let when = std::time::SystemTime::now() - age.to_std().unwrap();
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(when))
        .unwrap();
}

fn only(tasks: &[NonTerminalDeleteTask]) -> &NonTerminalDeleteTask {
    assert_eq!(tasks.len(), 1, "{tasks:?}");
    &tasks[0]
}

fn claim_age(observed: &NonTerminalDeleteTask) -> ChronoDuration {
    match observed.claim {
        ObservedDeleteTaskClaim::Present {
            last_modified: Some(at),
        } => Utc::now() - at,
        other => panic!("expected an aged claim, got {other:?}"),
    }
}

/// The `running` stranding, in the shape all three of its causes leave behind:
/// `state: running`, a claim object, and no other temporal field. The observer
/// reports the state and the claim's age and does not touch the record — the
/// case where the terminal write failed AFTER the rewrite committed is
/// byte-identical to the crash, so the record must come back out unchanged.
#[tokio::test]
async fn a_running_task_under_an_aged_claim_is_observed_without_being_written() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = open(&warehouse).await;
    let task = submit(&ice, "host = 'victim'").await;
    let running = set_state(&ice, &task, DeleteTaskState::Running).await;
    plant_claim(
        &warehouse,
        &ice,
        &task,
        b"{}",
        ChronoDuration::seconds(4_210),
    );

    let observation = ice.observe_nonterminal_delete_tasks().await.unwrap();
    assert!(observation.complete, "{observation:?}");
    assert_eq!(observation.uninspected, 0);
    let observed = only(&observation.tasks);
    assert_eq!(observed.task_id, task.task_id);
    assert_eq!(observed.index_id, "logs");
    assert_eq!(observed.state, DeleteTaskState::Running);
    assert_eq!(observed.created_at, task.created_at);
    // An unreadable body does not cost the age: it comes from the object.
    let age = claim_age(observed);
    assert!(
        age >= ChronoDuration::seconds(4_210),
        "the claim must be aged from the object's last_modified, got {age}"
    );

    assert_eq!(
        ice.get_delete_task(task.task_id).await.unwrap().unwrap(),
        running,
        "the observer must leave the record exactly as it found it"
    );
    assert_eq!(
        std::fs::read(claim_path(&warehouse, &ice, &task)).unwrap(),
        b"{}",
        "the observer must not release, rewrite or take over the claim"
    );
}

/// An unclaimed `pending` task is observed with its claim absent. Absence means
/// only "not observed during this read"; it is the shape of a task waiting for
/// the next sweep, and nothing here classifies it.
#[tokio::test]
async fn a_pending_task_with_no_claim_is_observed_with_the_claim_absent() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = open(&warehouse).await;
    submit(&ice, "host = 'victim'").await;

    let observation = ice.observe_nonterminal_delete_tasks().await.unwrap();
    assert!(observation.complete);
    let observed = only(&observation.tasks);
    assert_eq!(observed.state, DeleteTaskState::Pending);
    assert_eq!(observed.claim, ObservedDeleteTaskClaim::Absent);
}

/// `pending` under a claim its executor never released — the process that died
/// between the claim and the `Running` write. Same observation shape, a
/// different state, and its claim is aged from the object too.
#[tokio::test]
async fn a_pending_task_under_a_claim_is_observed_with_its_claim_age() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = open(&warehouse).await;
    let task = submit(&ice, "host = 'victim'").await;
    plant_claim(&warehouse, &ice, &task, b"{}", ChronoDuration::seconds(90));

    let observation = ice.observe_nonterminal_delete_tasks().await.unwrap();
    assert!(observation.complete);
    let observed = only(&observation.tasks);
    assert_eq!(observed.state, DeleteTaskState::Pending);
    assert!(claim_age(observed) >= ChronoDuration::seconds(90));
}

/// Terminal tasks are not in the observable population, claim or no claim.
/// Claims are retained in every state (#2129), so a `done` task with a live
/// claim must not be reported as anything.
#[tokio::test]
async fn a_terminal_task_leaves_the_observation_even_though_its_claim_is_retained() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = open(&warehouse).await;
    let task = submit(&ice, "host = 'victim'").await;
    set_state(&ice, &task, DeleteTaskState::Running).await;
    plant_claim(
        &warehouse,
        &ice,
        &task,
        b"{}",
        ChronoDuration::seconds(4_210),
    );
    assert_eq!(
        ice.observe_nonterminal_delete_tasks()
            .await
            .unwrap()
            .tasks
            .len(),
        1
    );

    for terminal in [DeleteTaskState::Done, DeleteTaskState::Failed] {
        set_state(&ice, &task, terminal).await;
        let observation = ice.observe_nonterminal_delete_tasks().await.unwrap();
        assert!(observation.complete, "{observation:?}");
        assert!(
            observation.tasks.is_empty(),
            "a {terminal:?} task is terminal: {observation:?}"
        );
    }
}

/// The inspection cap does not silently truncate a count. Beyond it the
/// observation reports itself incomplete and says how many tasks it skipped,
/// which is what stops a caller publishing a low number that reads as recovery.
#[tokio::test]
async fn the_inspection_cap_reports_an_incomplete_observation() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = open(&warehouse).await;
    for _ in 0..3 {
        let task = submit(&ice, "host = 'victim'").await;
        set_state(&ice, &task, DeleteTaskState::Running).await;
    }

    let full = ice.observe_nonterminal_delete_tasks().await.unwrap();
    assert!(full.complete);
    assert_eq!(full.tasks.len(), 3);

    let capped = ice
        .observe_nonterminal_delete_tasks_capped(2)
        .await
        .unwrap();
    assert!(!capped.complete, "{capped:?}");
    assert_eq!(capped.tasks.len(), 2, "{capped:?}");
    assert_eq!(capped.uninspected, 1, "{capped:?}");
    // Oldest first, so the truncated tail is the newest tasks, not an
    // arbitrary subset.
    assert_eq!(capped.tasks[0].task_id, full.tasks[0].task_id);
    assert_eq!(capped.tasks[1].task_id, full.tasks[1].task_id);
}

/// A claim object is not a task record: it must not turn the listing into an
/// error, and the observation must not report the claim as a task of its own.
#[tokio::test]
async fn claim_objects_are_not_observed_as_tasks() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = open(&warehouse).await;
    let first = submit(&ice, "host = 'a'").await;
    let second = submit(&ice, "host = 'b'").await;
    plant_claim(&warehouse, &ice, &first, b"{}", ChronoDuration::seconds(5));
    plant_claim(&warehouse, &ice, &second, b"{}", ChronoDuration::seconds(5));

    let observation = ice.observe_nonterminal_delete_tasks().await.unwrap();
    assert!(observation.complete);
    assert_eq!(observation.tasks.len(), 2, "{observation:?}");
}
