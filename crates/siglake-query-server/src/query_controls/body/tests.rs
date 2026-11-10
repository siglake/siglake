use super::*;
use crate::{admission::AdmissionController, query_controls::runtime::test_controls};
use axum::http::header::CONTENT_TYPE;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

struct Source {
    inner: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>,
    live: Arc<AtomicUsize>,
    _cancel: siglake_storage::CancelOnDrop,
    _admission: crate::admission::AdmissionGuard,
    _exec: tokio::sync::OwnedSemaphorePermit,
}
impl Stream for Source {
    type Item = io::Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}
struct Resources {
    admission: AdmissionController,
    exec: Arc<tokio::sync::Semaphore>,
    live: Arc<AtomicUsize>,
    cancel: siglake_storage::QueryCancel,
}
impl Resources {
    fn new() -> Self {
        Self {
            admission: AdmissionController::new(1, 1, Duration::from_secs(1)),
            exec: Arc::new(tokio::sync::Semaphore::new(1)),
            live: Arc::new(AtomicUsize::new(0)),
            cancel: siglake_storage::QueryCancel::new(),
        }
    }
    async fn response(
        &self,
        stream: impl Stream<Item = io::Result<Bytes>> + Send + 'static,
    ) -> Response {
        self.live.fetch_add(1, Ordering::SeqCst);
        let source = Source {
            inner: Box::pin(stream),
            live: self.live.clone(),
            _cancel: siglake_storage::CancelOnDrop(self.cancel.clone()),
            _admission: self.admission.acquire(1).await.unwrap(),
            _exec: self.exec.clone().acquire_owned().await.unwrap(),
        };
        let mut response = Response::new(Body::from_stream(source));
        response
            .headers_mut()
            .insert(CONTENT_TYPE, "application/x-ndjson".parse().unwrap());
        response
    }
    async fn baseline(&self) {
        assert_eq!(self.live.load(Ordering::SeqCst), 0);
        assert!(self.cancel.is_cancelled());
        assert_eq!(self.exec.available_permits(), 1);
        assert!(futures::poll!(Box::pin(self.admission.acquire(1))).is_ready());
    }
}
async fn settle() {
    for _ in 0..12 {
        tokio::task::yield_now().await;
    }
}
fn registered(tasks: &BodyTasks) -> usize {
    tasks.state.lock().unwrap().tasks.len()
}
#[tokio::test(start_paused = true)]
async fn original_deadline_expires_idle_and_never_polled_tiny_eof_releasing_real_guards() {
    for tiny in [false, true] {
        let tasks = BodyTasks::default();
        let resources = Resources::new();
        let controls = test_controls(Duration::from_secs(1));
        // Original clock already spent in preparation: body gets remaining time.
        tokio::time::advance(Duration::from_millis(700)).await;
        let stream: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>> = if tiny {
            Box::pin(futures::stream::iter([Ok(Bytes::from_static(b"{}\n"))]))
        } else {
            Box::pin(futures::stream::pending())
        };
        let response = tasks
            .reserve()
            .unwrap()
            .start(resources.response(stream).await, controls, None)
            .unwrap();
        settle().await;
        assert_eq!(resources.live.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_millis(301)).await;
        settle().await;
        resources.baseline().await;
        // Source EOF is not delivery EOF: handle stays registered until joined.
        assert_eq!(registered(&tasks), 1);
        let bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(&bytes[..], b"{\"_meta\":\"timeout\"}\n");
        tasks.shutdown().await;
        assert_eq!(registered(&tasks), 0);
    }
}
#[tokio::test(start_paused = true)]
async fn full_channel_remains_selectable_and_never_exceeds_two_logical_chunks() {
    let tasks = BodyTasks::default();
    let resources = Resources::new();
    let response = resources
        .response(futures::stream::repeat_with(|| {
            Ok(Bytes::from(vec![b'x'; CHUNK_BYTES * 5]))
        }))
        .await;
    let mut body = tasks
        .reserve()
        .unwrap()
        .start(response, test_controls(Duration::from_secs(1)), None)
        .unwrap()
        .into_body()
        .into_data_stream();
    settle().await;
    // Producer retains at most two queued <=16KiB copies; an upstream chunk is
    // separately retained and is not claimed part of that logical byte bound.
    assert_eq!(tasks.state.lock().unwrap().tasks.len(), 1);
    let first = body.next().await.unwrap().unwrap();
    assert_eq!(first.len(), CHUNK_BYTES);
    tokio::time::advance(Duration::from_secs(1)).await;
    settle().await;
    resources.baseline().await;
    assert!(
        body.next().await.unwrap().is_err(),
        "partial record must terminate transport, not append a JSON marker"
    );
    assert!(body.next().await.is_none());
    tasks.shutdown().await;
}
#[tokio::test(start_paused = true)]
async fn body_drop_success_error_and_timeout_races_have_owned_join_and_one_release() {
    for outcome in [DELIVERED, ABANDONED, FAILED, TIMEOUT] {
        let tasks = BodyTasks::default();
        let resources = Resources::new();
        let stream = futures::stream::iter(if outcome == FAILED {
            vec![Err(io::Error::other("synthetic"))]
        } else {
            vec![Ok(Bytes::from_static(b"{}\n"))]
        });
        let response = tasks
            .reserve()
            .unwrap()
            .start(
                resources.response(stream).await,
                test_controls(Duration::from_secs(1)),
                None,
            )
            .unwrap();
        let body = response.into_body();
        match outcome {
            DELIVERED => {
                assert_eq!(&axum::body::to_bytes(body, 100).await.unwrap()[..], b"{}\n");
            }
            FAILED => {
                assert!(axum::body::to_bytes(body, 100).await.is_err());
            }
            TIMEOUT => {
                tokio::time::advance(Duration::from_secs(1)).await;
                settle().await;
                drop(body);
            }
            _ => {
                drop(body);
            }
        }
        tasks.shutdown().await;
        resources.baseline().await;
        assert_eq!(registered(&tasks), 0);
    }
}
#[tokio::test(start_paused = true)]
async fn cancelled_shutdown_retains_join_handles_closed_generation_and_no_reinsertion() {
    let tasks = BodyTasks::default();
    let resources = Resources::new();
    let response = tasks
        .reserve()
        .unwrap()
        .start(
            resources.response(futures::stream::pending()).await,
            test_controls(Duration::from_secs(10)),
            None,
        )
        .unwrap();
    settle().await;
    {
        let mut shutdown = Box::pin(tasks.shutdown());
        assert!(futures::poll!(&mut shutdown).is_pending());
    }
    assert_eq!(registered(&tasks), 1);
    assert!(tasks.reserve().is_err());
    tasks.shutdown().await;
    resources.baseline().await;
    assert_eq!(registered(&tasks), 0);
    drop(response);
}
#[tokio::test(start_paused = true)]
async fn final_owner_drop_is_abort_unconfirmed_but_still_running_executor_releases_guards() {
    let tasks = BodyTasks::default();
    let resources = Resources::new();
    let response = tasks
        .reserve()
        .unwrap()
        .start(
            resources.response(futures::stream::pending()).await,
            test_controls(Duration::from_secs(10)),
            None,
        )
        .unwrap();
    settle().await;
    drop(tasks); // Explicitly not join-confirmed.
    settle().await;
    resources.baseline().await;
    drop(response);
}
#[tokio::test(start_paused = true)]
async fn saturation_reserves_before_source_start_and_shutdown_rejects_late_slot_start() {
    let tasks = BodyTasks::default();
    let slots: Vec<_> = (0..TASKS).map(|_| tasks.reserve().unwrap()).collect();
    assert!(tasks.reserve().is_err());
    tasks.shutdown().await;
    for slot in slots {
        assert!(slot
            .start(
                Response::new(Body::empty()),
                test_controls(Duration::from_secs(1)),
                None
            )
            .is_err());
    }
    assert_eq!(registered(&tasks), 0);
    assert_eq!(tasks.state.lock().unwrap().reserved, 0);
}

#[tokio::test(start_paused = true)]
async fn concurrent_shutdown_waiters_observe_all_joins_without_detached_return_tasks() {
    let tasks = BodyTasks::default();
    let resources = Resources::new();
    let response = tasks
        .reserve()
        .unwrap()
        .start(
            resources.response(futures::stream::pending()).await,
            test_controls(Duration::from_secs(10)),
            None,
        )
        .unwrap();
    settle().await;
    tokio::join!(tasks.shutdown(), tasks.shutdown());
    assert_eq!(registered(&tasks), 0);
    resources.baseline().await;
    drop(response);
}
#[tokio::test(start_paused = true)]
async fn trickling_source_and_fragment_cancellation_never_refresh_original_deadline() {
    let tasks = BodyTasks::default();
    let resources = Resources::new();
    let stream = async_stream::stream! {
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            yield Ok(Bytes::from_static(b"{}\n"));
        }
    };
    let response = tasks
        .reserve()
        .unwrap()
        .start(
            resources.response(stream).await,
            test_controls(Duration::from_secs(1)),
            None,
        )
        .unwrap();
    for _ in 0..11 {
        tokio::time::advance(Duration::from_millis(100)).await;
        settle().await;
    }
    resources.baseline().await;
    tasks.shutdown().await;
    drop(response);
    let life = Arc::new(Life {
        outcome: AtomicU8::new(LIVE),
        changed: Notify::new(),
        history: None,
        deadline: Instant::now() + Duration::from_secs(10),
    });
    PRODUCTION.scope(life.clone(), async {
        assert!(crate::query_controls::runtime::render_check().is_ok());
        life.finish(ABANDONED);
        assert!(crate::query_controls::runtime::render_check().is_err(), "sync fragment checks observe cancellation even before the timer/select polls again");
    }).await;
}

#[tokio::test(start_paused = true)]
async fn tiny_completed_producers_still_hold_capacity_until_outer_eof_or_drop() {
    let tasks = BodyTasks::default();
    let mut responses = Vec::new();
    for _ in 0..TASKS {
        responses.push(
            tasks
                .reserve()
                .unwrap()
                .start(
                    Response::new(Body::empty()),
                    test_controls(Duration::from_secs(10)),
                    None,
                )
                .unwrap(),
        );
    }
    settle().await;
    assert_eq!(registered(&tasks), TASKS);
    assert!(tasks.reserve().is_err());
    drop(responses.pop());
    settle().await;
    let slot = tasks
        .reserve()
        .expect("body drop followed by observed join frees technical capacity");
    drop(slot);
    assert_eq!(registered(&tasks), TASKS - 1);
    tasks.shutdown().await;
    assert_eq!(registered(&tasks), 0);
    drop(responses);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn latest_distinct_shutdown_waiter_cancelled_cannot_steal_survivor_join_wakeup() {
    let tasks = Arc::new(BodyTasks::default());
    let resources = Resources::new();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let mut entered = Some(entered);
    let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let gate = release.clone();
    // Hold one native source poll open so both distinct shutdown task wakers
    // are deterministically registered BEFORE producer termination is possible.
    let stream = futures::stream::poll_fn(move |_| {
        if let Some(entered) = entered.take() {
            let _ = entered.send(());
            let (lock, changed) = &*gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
        }
        Poll::Pending
    });
    let response = tasks
        .reserve()
        .unwrap()
        .start(
            resources.response(stream).await,
            test_controls(Duration::from_secs(10)),
            None,
        )
        .unwrap();
    entered_rx.await.unwrap();
    let waiter = |tasks: Arc<BodyTasks>| {
        let (polled, ack) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut wait = Box::pin(tasks.shutdown());
            let mut polled = Some(polled);
            futures::future::poll_fn(|cx| {
                let result = wait.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(polled) = polled.take() {
                        let _ = polled.send(());
                    }
                }
                result
            })
            .await;
        });
        (task, ack)
    };
    let (mut survivor, a_polled) = waiter(tasks.clone());
    a_polled.await.unwrap();
    let (latest, b_polled) = waiter(tasks.clone());
    b_polled.await.unwrap();
    latest.abort();
    assert!(latest.await.unwrap_err().is_cancelled()); // observe test task join
    assert_eq!(
        registered(&tasks),
        1,
        "cancelled waiter must retain producer handle"
    );
    assert_eq!(resources.live.load(Ordering::SeqCst), 1);
    let (lock, changed) = &*release;
    *lock.lock().unwrap() = true;
    changed.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(2), &mut survivor).await;
    if result.is_err() {
        // Diagnostic failure must still clean up/observe every owned test task.
        tasks.shutdown().await;
        survivor.abort();
        let _ = survivor.await;
        panic!("latest waiter cancellation stranded the independently spawned survivor");
    }
    result.unwrap().unwrap();
    assert_eq!(registered(&tasks), 0);
    resources.baseline().await;
    drop(response);
}

#[tokio::test(start_paused = true)]
async fn timeout_winner_during_dequeue_discards_end_and_data_without_false_clean_eof() {
    for item_is_end in [false, true] {
        for at_boundary in [false, true] {
            let (tx, rx) = mpsc::channel(CHANNEL_ITEMS);
            tx.send(if item_is_end {
                Item::End
            } else {
                Item::Data(Bytes::from_static(b"queued-data\n"))
            })
            .await
            .unwrap();
            let life = Arc::new(Life {
                outcome: AtomicU8::new(LIVE),
                changed: Notify::new(),
                history: None,
                deadline: Instant::now() + Duration::from_secs(10),
            });
            let winner = life.clone();
            let mut body = ControlledBody {
                rx,
                life,
                ndjson: true,
                at_boundary,
                ended: false,
                // Exact race seam: initial state read was LIVE, then timeout
                // wins after dequeue but before delivery/data checkpoint.
                after_dequeue: Some(Box::new(move || {
                    assert_eq!(winner.finish(TIMEOUT), TIMEOUT);
                })),
            };
            let item = body.next().await.expect("timeout cannot become clean EOF");
            if at_boundary {
                assert!(item.is_ok_and(|bytes| bytes == b"{\"_meta\":\"timeout\"}\n"[..]));
            } else {
                assert!(
                    item.is_err(),
                    "partial record cannot gain a valid JSON marker"
                );
            }
            assert!(body.next().await.is_none());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn terminal_arbiter_checks_original_deadline_and_preserves_first_committed_winner() {
    let life = Life {
        outcome: AtomicU8::new(LIVE),
        changed: Notify::new(),
        history: None,
        deadline: Instant::now() + Duration::from_secs(1),
    };
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(life.finish(FAILED), TIMEOUT);
    assert_eq!(life.finish(DELIVERED), TIMEOUT);
    assert_eq!(life.finish(ABANDONED), TIMEOUT);
    let delivered = Life {
        outcome: AtomicU8::new(LIVE),
        changed: Notify::new(),
        history: None,
        deadline: Instant::now() + Duration::from_secs(1),
    };
    assert_eq!(delivered.finish(DELIVERED), DELIVERED);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(delivered.finish(TIMEOUT), DELIVERED);
}

#[tokio::test]
async fn buffered_arrow_never_polled_slow_and_dropped_bodies_release_backing_and_join_without_suffix(
) {
    struct Backing {
        bytes: Vec<u8>,
        live: Arc<AtomicUsize>,
    }
    impl AsRef<[u8]> for Backing {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Backing {
        fn drop(&mut self) {
            self.live.fetch_sub(1, Ordering::SeqCst);
        }
    }
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let payload = "x".repeat(CHUNK_BYTES * 12);
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec![payload.as_str()])) as ArrayRef],
    )
    .unwrap();
    let native = crate::format::batches_to_arrow_ipc(&[batch]).unwrap();
    assert!(native.len() > CHUNK_BYTES * 4);
    for mode in ["never", "slow", "drop", "success"] {
        let tasks = BodyTasks::default();
        let live = Arc::new(AtomicUsize::new(1));
        let budget = if mode == "success" {
            Duration::from_secs(2)
        } else {
            Duration::from_millis(100)
        };
        let controls = test_controls(budget);
        let started = std::time::Instant::now();
        // Preparation spends the same original clock before buffered delivery.
        if mode != "success" {
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        let mut response = Response::new(Body::from(Bytes::from_owner(Backing {
            bytes: native.clone(),
            live: live.clone(),
        })));
        response.headers_mut().insert(
            CONTENT_TYPE,
            "application/vnd.apache.arrow.stream".parse().unwrap(),
        );
        let response = tasks
            .reserve()
            .unwrap()
            .start(response, controls, None)
            .unwrap();
        assert_eq!(
            response.headers()[CONTENT_TYPE],
            "application/vnd.apache.arrow.stream"
        );
        let mut body = response.into_body().into_data_stream();
        if mode == "success" {
            let mut actual = Vec::new();
            while let Some(chunk) = body.next().await {
                actual.extend_from_slice(&chunk.unwrap());
            }
            assert_eq!(
                actual, native,
                "no Arrow JSON suffix/marker or batching change"
            );
        } else if mode == "drop" {
            drop(body);
            body = Body::empty().into_data_stream();
        } else {
            if mode == "slow" {
                assert_eq!(
                    &body.next().await.unwrap().unwrap()[..],
                    &native[..CHUNK_BYTES]
                );
            }
            tokio::time::timeout(Duration::from_millis(600), async {
                while live.load(Ordering::SeqCst) != 0 {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .unwrap();
            assert!(
                body.next().await.unwrap().is_err(),
                "Arrow expiry terminates transport, never appends JSON"
            );
            assert!(body.next().await.is_none());
        }
        // Shutdown is the confirmation boundary, not an assumed Drop join.
        tokio::time::timeout(Duration::from_millis(600), tasks.shutdown())
            .await
            .unwrap();
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert_eq!(registered(&tasks), 0);
        assert!(started.elapsed() < Duration::from_secs(1));
        eprintln!(
            "buffered Arrow {mode} backing release and join observed at {:?}",
            started.elapsed()
        );
        drop(body);
    }
}
