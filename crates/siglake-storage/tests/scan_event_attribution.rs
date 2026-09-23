//! A scan's two log events name the execution and the scan node that produced
//! them, so nobody has to attribute them by their position in the log.
//!
//! WHY POSITION IS NOT ENOUGH, which is what this file pins. The scan emits
//! `siglake query scan reader tuning` once per scan node at PLANNING time and
//! `siglake query source partition profile` once per partition when that
//! partition's stream finishes or drops. The partition event is emitted from
//! a DataFusion pump, which carries no request span, and it can be emitted
//! long after the request that planned it has logged its own terminal line —
//! an early `LIMIT` closes the root stream and leaves the other partitions
//! unwinding, and an NDJSON body streams after its handler returns. A reader
//! that groups these events by their position between terminal lines
//! therefore hands a late partition to the NEXT execution. Run #108 of the
//! bench showed the effect: 15 of 133 executions had no tuning event in their
//! positional group, and for 14 of them the tuning event was the very next
//! line after the group's boundary.
//!
//! `query_execution_id` (minted per execution, injected through
//! `SessionConfig`) and `scan_id` (minted per scan node) are fields on both
//! events, so the join is exact whatever the interleaving.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use futures::StreamExt;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::{QueryExecutionId, SiglakeIcebergTableScan, UNATTRIBUTED_QUERY_EXECUTION_ID};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

const TUNING: &str = "siglake query scan reader tuning";
const PROFILE: &str = "siglake query source partition profile";
/// Stands in for the request's terminal line (`sql query profile` in the
/// query server, which this crate cannot emit). Its only job is to be the
/// boundary a positional reader would close a group on.
const TERMINAL: &str = "test execution terminal line";

const PARTITIONS: usize = 8;
const FILES: usize = 24;
/// The unwinding fixture is the one `scan_attribution_settle` established:
/// enough needle-free files per partition that each aborted pump has far more
/// work left than the polls it gets before the root stream closes. The other
/// tests here do not need the window open and use the cheaper `FILES`.
const FILES_STILL_UNWINDING: usize = 64;
const ROWS_PER_FILE: usize = 5_000;
/// The only `host` inside this range, carried by the first rows of file 0.
/// Every other file costs a footer read, a data read and a full decode before
/// it is known empty, so the other partitions are still unwinding when the
/// `LIMIT 1` closes the root stream.
const NEEDLE: &str = "host-mm";
const PREDICATE: &str = "host > 'host-ml' AND host < 'host-mn'";

// ---------------------------------------------------------------- capture

/// One captured event: its message and the integer fields this file asserts
/// on, plus the order it was logged in — which is the thing a positional
/// reader would use, so the tests have to be able to talk about it.
#[derive(Debug, Clone)]
struct Captured {
    seq: usize,
    message: String,
    fields: HashMap<&'static str, u64>,
}

impl Captured {
    fn execution(&self) -> u64 {
        *self.fields.get("query_execution_id").expect("no id field")
    }
    fn scan(&self) -> u64 {
        *self.fields.get("scan_id").expect("no scan_id field")
    }
}

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Captured>>>);

impl Recorder {
    fn events(&self, message: &str) -> Vec<Captured> {
        self.0
            .lock()
            .expect("recorder")
            .iter()
            .filter(|e| e.message == message)
            .cloned()
            .collect()
    }

    /// Every scan event (either kind) this scan node produced.
    fn for_scan(&self, scan_id: u64) -> Vec<Captured> {
        self.0
            .lock()
            .expect("recorder")
            .iter()
            .filter(|e| {
                (e.message == TUNING || e.message == PROFILE)
                    && e.fields.get("scan_id") == Some(&scan_id)
            })
            .cloned()
            .collect()
    }
}

#[derive(Default)]
struct Visitor {
    message: String,
    fields: HashMap<&'static str, u64>,
}

impl tracing::field::Visit for Visitor {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if let Some(name) = wanted(field.name()) {
            self.fields.insert(name, value);
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if let Some(name) = wanted(field.name()) {
            self.fields.insert(name, value as u64);
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }
}

/// Field names this file reads. Interning them keeps `Captured` cheap to
/// clone and makes a typo a compile-time lookup miss rather than a silent
/// absent key.
fn wanted(name: &str) -> Option<&'static str> {
    match name {
        "query_execution_id" => Some("query_execution_id"),
        "scan_id" => Some("scan_id"),
        "partition" => Some("partition"),
        _ => None,
    }
}

impl<S: tracing::Subscriber> Layer<S> for Recorder {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut visitor = Visitor::default();
        event.record(&mut visitor);
        if ![TUNING, PROFILE, TERMINAL].contains(&visitor.message.as_str()) {
            return;
        }
        self.0.lock().expect("recorder").push(Captured {
            seq: SEQ.fetch_add(1, Ordering::SeqCst),
            message: visitor.message,
            fields: visitor.fields,
        });
    }
}

/// The recorder every test in this binary shares. One global subscriber per
/// process is all `tracing` allows, and the scan's events come from spawned
/// pumps on other threads, so a thread-scoped default would miss them. Tests
/// select their own events by `scan_id`, which is process-unique, so sharing
/// the recorder across parallel tests cannot mix them up.
fn recorder() -> Recorder {
    static RECORDER: std::sync::OnceLock<Recorder> = std::sync::OnceLock::new();
    RECORDER
        .get_or_init(|| {
            let recorder = Recorder::default();
            tracing_subscriber::registry().with(recorder.clone()).init();
            recorder
        })
        .clone()
}

// ---------------------------------------------------------------- fixture

async fn needle_table(ice: &IcebergContext, files: usize) {
    for file in 0..files {
        let rows = if file == 0 {
            ROWS_PER_FILE + 1_000
        } else {
            ROWS_PER_FILE
        };
        let events: Vec<Event> = (0..rows)
            .map(|i| {
                let mut e = Event::now(format!("row {file}-{i} payload"));
                e.host = if file == 0 && i < 4 {
                    NEEDLE.to_string()
                } else {
                    format!("host-{}", (b'a' + (i % 26) as u8) as char)
                };
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
}

/// A session carrying one execution id, exactly as the query server builds it.
fn session_with(execution: QueryExecutionId) -> datafusion::prelude::SessionContext {
    let mut state =
        siglake_storage::session_context_with_target_partitions(Some(PARTITIONS)).state();
    state.config_mut().set_extension(Arc::new(execution));
    datafusion::prelude::SessionContext::new_with_state(state)
}

/// The plan's one scan leaf. Every shape here reads a single table, so more
/// than one leaf means the fixture changed under the test.
fn only_scan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let mut found = Vec::new();
    plan.apply(|node| {
        if node
            .as_any()
            .downcast_ref::<SiglakeIcebergTableScan>()
            .is_some()
        {
            found.push(node.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    assert_eq!(found.len(), 1, "one scan leaf expected in {plan:?}");
    found.remove(0)
}

fn scan_id_of(plan: &Arc<dyn ExecutionPlan>) -> u64 {
    only_scan(plan)
        .as_any()
        .downcast_ref::<SiglakeIcebergTableScan>()
        .unwrap()
        .scan_id()
}

fn execution_id_of(plan: &Arc<dyn ExecutionPlan>) -> u64 {
    only_scan(plan)
        .as_any()
        .downcast_ref::<SiglakeIcebergTableScan>()
        .unwrap()
        .query_execution_id()
}

fn live_partitions_of(plan: &Arc<dyn ExecutionPlan>) -> usize {
    only_scan(plan)
        .as_any()
        .downcast_ref::<SiglakeIcebergTableScan>()
        .unwrap()
        .live_partitions()
}

async fn plan_needle_scan(
    ctx: &datafusion::prelude::SessionContext,
    limit: &str,
) -> Arc<dyn ExecutionPlan> {
    ctx.sql(&format!(
        "SELECT host, raw FROM events WHERE {PREDICATE} {limit}"
    ))
    .await
    .unwrap()
    .create_physical_plan()
    .await
    .unwrap()
}

async fn drain(plan: &Arc<dyn ExecutionPlan>) -> usize {
    // The shared runtime and nothing else: these tests assert on the events,
    // not on any session option.
    #[allow(clippy::disallowed_methods)]
    let mut stream = execute_stream(plan.clone(), siglake_storage::bounded_task_context()).unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    rows
}

// ------------------------------------------------------------------ tests

/// Two executions of the SAME SQL, running at the same time. Every event
/// belongs to exactly one of them, and the two sets are disjoint — which is
/// the case a query-text join cannot decide and a positional join gets wrong.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_executions_of_identical_sql_keep_their_events_apart() {
    let events = recorder();
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    needle_table(&ice, FILES).await;

    let (left_id, right_id) = (QueryExecutionId::next(), QueryExecutionId::next());
    let left_ctx = session_with(left_id);
    let right_ctx = session_with(right_id);
    ice.register_with_datafusion(&left_ctx).await.unwrap();
    ice.register_with_datafusion(&right_ctx).await.unwrap();

    // BOTH plans are built before either runs, so the right execution's
    // tuning event is logged above every partition profile of the left one.
    // A positional reader would hand the left execution's partitions to the
    // right one; this ordering is fixed by construction, not by scheduling.
    let left_plan = plan_needle_scan(&left_ctx, "").await;
    let right_plan = plan_needle_scan(&right_ctx, "").await;
    let (left_scan, right_scan) = (scan_id_of(&left_plan), scan_id_of(&right_plan));
    assert_ne!(
        left_scan, right_scan,
        "two scan nodes of identical SQL must not share a scan id"
    );
    let right_tuning = events
        .for_scan(right_scan)
        .into_iter()
        .find(|e| e.message == TUNING)
        .expect("the right execution logged its tuning event")
        .seq;

    let (left_rows, right_rows) = futures::join!(drain(&left_plan), drain(&right_plan));
    assert_eq!((left_rows, right_rows), (4, 4), "the range filter is exact");
    siglake_storage::settle_scan_partitions(&left_plan, Duration::from_secs(10)).await;
    siglake_storage::settle_scan_partitions(&right_plan, Duration::from_secs(10)).await;

    for (scan_id, execution) in [(left_scan, left_id.0), (right_scan, right_id.0)] {
        let mine = events.for_scan(scan_id);
        let tuning: Vec<&Captured> = mine.iter().filter(|e| e.message == TUNING).collect();
        let profiles: Vec<&Captured> = mine.iter().filter(|e| e.message == PROFILE).collect();
        assert_eq!(tuning.len(), 1, "one tuning event per scan node: {mine:?}");
        assert!(
            !profiles.is_empty(),
            "the scan executed, so it profiled its partitions: {mine:?}"
        );
        for event in &mine {
            assert_eq!(
                event.execution(),
                execution,
                "an event of scan {scan_id} is attributed to another execution: {event:?}"
            );
        }
    }

    // What a positional reader would have got wrong: the left execution's
    // partition profiles sit below the right execution's first line.
    let left_after_boundary = events
        .for_scan(left_scan)
        .into_iter()
        .filter(|e| e.message == PROFILE && e.seq > right_tuning)
        .count();
    assert!(
        left_after_boundary > 0,
        "the left execution logged no profile below the right execution's tuning \
         event, so this run does not exercise the misattribution"
    );
}

/// The reported shape, reproduced: an early `LIMIT` drops partitions that
/// profile themselves AFTER their request's terminal line, and the next
/// execution's first line is already above that terminal line. A positional
/// reader gets both halves wrong — the late profiles fall into the next
/// group, and the next execution's group holds no tuning event. The ids do
/// not move.
#[tokio::test(flavor = "current_thread")]
async fn a_late_partition_profile_does_not_attach_to_the_next_execution() {
    let events = recorder();
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    needle_table(&ice, FILES_STILL_UNWINDING).await;

    let first_id = QueryExecutionId::next();
    let first_ctx = session_with(first_id);
    ice.register_with_datafusion(&first_ctx).await.unwrap();
    let first_plan = plan_needle_scan(&first_ctx, "LIMIT 1").await;
    let first_scan = scan_id_of(&first_plan);

    // The next execution is planned BEFORE the first one's terminal line, the
    // way a second in-flight request is: its tuning event is logged into the
    // first execution's positional group and is missing from its own.
    let second_id = QueryExecutionId::next();
    let second_ctx = session_with(second_id);
    ice.register_with_datafusion(&second_ctx).await.unwrap();
    let second_plan = plan_needle_scan(&second_ctx, "").await;
    let second_scan = scan_id_of(&second_plan);

    // The `LIMIT 1` is satisfied by partition 0; the root closes and the other
    // pumps are aborted, but each abort lands on that pump's next poll.
    assert_eq!(drain(&first_plan).await, 1);
    let live = live_partitions_of(&first_plan);
    assert!(
        live > 0,
        "no partition was still unwinding, so this run cannot produce a late \
         profile and proves nothing"
    );
    // The request's terminal line, which is where the reader closes the group.
    // Nothing is awaited between the drain returning and this, so on a
    // current-thread runtime no aborted pump can have run yet: the profiles
    // below are late by construction, not by timing.
    tracing::info!(query_execution_id = first_id.0, "{TERMINAL}");
    let boundary = events
        .events(TERMINAL)
        .into_iter()
        .filter(|e| e.execution() == first_id.0)
        .map(|e| e.seq)
        .next_back()
        .expect("the terminal line was captured");

    siglake_storage::settle_scan_partitions(&first_plan, Duration::from_secs(10)).await;

    let late: Vec<Captured> = events
        .for_scan(first_scan)
        .into_iter()
        .filter(|e| e.message == PROFILE && e.seq > boundary)
        .collect();
    assert!(
        !late.is_empty(),
        "no profile of the first execution landed below its terminal line, so \
         the fixture no longer reproduces the misattribution"
    );
    for event in &late {
        assert_eq!(
            event.execution(),
            first_id.0,
            "a late profile was attributed to the execution it merely follows: {event:?}"
        );
    }

    // The other half of the reported symptom: the second execution's tuning
    // event sits above the first one's terminal line, so its own positional
    // group holds none.
    let second_tuning = events
        .for_scan(second_scan)
        .into_iter()
        .find(|e| e.message == TUNING)
        .expect("the second execution logged its tuning event");
    assert!(
        second_tuning.seq < boundary,
        "the second execution's tuning event did not land in the first \
         execution's group, so the stranded-tuning half is not exercised"
    );
    assert_eq!(second_tuning.execution(), second_id.0);
}

/// One execution, two scan nodes — the shape `handle_local_inner` produces
/// when it plans the residual twin. Same execution id, different scan ids, so
/// a per-partition profile still resolves to ONE scan.
#[tokio::test(flavor = "multi_thread")]
async fn two_scan_nodes_of_one_execution_are_told_apart() {
    let events = recorder();
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    needle_table(&ice, FILES).await;

    let id = QueryExecutionId::next();
    let ctx = session_with(id);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let first = plan_needle_scan(&ctx, "").await;
    let second = plan_needle_scan(&ctx, "").await;
    let (first_scan, second_scan) = (scan_id_of(&first), scan_id_of(&second));
    assert_ne!(first_scan, second_scan);

    drain(&first).await;
    drain(&second).await;
    siglake_storage::settle_scan_partitions(&first, Duration::from_secs(10)).await;
    siglake_storage::settle_scan_partitions(&second, Duration::from_secs(10)).await;

    for scan_id in [first_scan, second_scan] {
        let mine = events.for_scan(scan_id);
        assert!(mine.iter().any(|e| e.message == PROFILE), "{mine:?}");
        assert!(
            mine.iter().all(|e| e.execution() == id.0),
            "both scan nodes belong to one execution: {mine:?}"
        );
    }
}

/// A session carrying no execution id — an internal scan, or any caller that
/// predates this — is marked unattributed rather than guessed at, and its
/// events still carry a unique scan id so they do not merge with anyone
/// else's.
#[tokio::test(flavor = "multi_thread")]
async fn a_scan_with_no_execution_id_is_reported_unattributed() {
    let events = recorder();
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    needle_table(&ice, FILES).await;

    let ctx = siglake_storage::session_context_with_target_partitions(Some(PARTITIONS));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let plan = plan_needle_scan(&ctx, "").await;
    let scan_id = scan_id_of(&plan);
    assert_eq!(execution_id_of(&plan), UNATTRIBUTED_QUERY_EXECUTION_ID);

    drain(&plan).await;
    siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(10)).await;

    let mine = events.for_scan(scan_id);
    assert!(mine.iter().any(|e| e.message == PROFILE), "{mine:?}");
    for event in &mine {
        assert_eq!(event.execution(), UNATTRIBUTED_QUERY_EXECUTION_ID);
        assert_eq!(event.scan(), scan_id);
    }
    // Nobody else's events were swept into this scan's set.
    assert_eq!(
        events
            .events(TUNING)
            .iter()
            .filter(|e| e.scan() == scan_id)
            .count(),
        1
    );
}
