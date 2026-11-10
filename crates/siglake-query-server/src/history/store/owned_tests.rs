//! Owned disposable PG only; all diagnostic capture stays in memory.
use super::*;
use crate::history::postgres_tests::{fixture, options};
use crate::history::{HistoryService, WRITE_BUDGET};
use sqlx::ConnectOptions;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, timeout};

fn configured(socket: &std::path::Path) -> PgConnectOptions {
    options(socket, "qh_writer")
        .application_name("history-owned-regression")
        .disable_statement_logging()
}
async fn backend_count(admin: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE application_name='history-owned-regression'",
    )
    .fetch_one(admin)
    .await
    .unwrap()
}
async fn waiting(admin: &PgPool, pid: i32) {
    timeout(Duration::from_secs(3), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted AND locktype='advisory')")
                .bind(pid).fetch_one(admin).await.unwrap();
            if blocked { break; } sleep(Duration::from_millis(1)).await;
        }
    }).await.expect("synthetic backend did not reach advisory gate");
}

#[tokio::test]
#[ignore = "owned PG fixture: scripts/check-query-history-startup.sh"]
async fn owned_cap_reuse_cancel_discard_transactions_and_owner_close() {
    let fixture = fixture().await;
    let pool = OwnedPool::connect(configured(&fixture.socket))
        .await
        .unwrap();
    let mut first = pool.acquire().await.unwrap();
    let original: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *first)
        .await
        .unwrap();
    first.complete().await.unwrap();
    drop(first);
    let mut reused = pool.acquire().await.unwrap();
    let same: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *reused)
        .await
        .unwrap();
    assert_eq!(
        original, same,
        "completed operations should reuse, not reconnect"
    );
    let second = pool.acquire().await.unwrap();
    let third = pool.acquire().await.unwrap();
    let fourth = pool.acquire().await.unwrap();
    assert_eq!(backend_count(&fixture.admin).await, 4);
    assert!(timeout(WRITE_BUDGET, pool.acquire()).await.is_err());
    assert_eq!(
        backend_count(&fixture.admin).await,
        4,
        "cancelled wait must not create a fifth socket"
    );
    // A lease starts non-reusable even if its last SQL happened to complete.
    drop(reused);
    let mut replacement = pool.acquire().await.unwrap();
    let different: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *replacement)
        .await
        .unwrap();
    assert_ne!(original, different);
    let tx = replacement.begin().await.unwrap();
    drop(tx);
    drop(replacement); // queued rollback is discarded, not returned/pinged
    drop(second);
    drop(third);
    drop(fourth);
    let mut lease = pool.acquire().await.unwrap();
    sqlx::query("SELECT 1").execute(&mut *lease).await.unwrap();
    lease.complete().await.unwrap();
    drop(lease);
    assert_eq!(pool.idle.lock().unwrap().connections.len(), 1);
    let service = HistoryService::new(HistoryWriter {
        pool: WriterPool::Owned(pool.clone()),
    });
    let observer = service.observer();
    let mut held = pool.acquire().await.unwrap();
    drop(service);
    assert!(
        pool.acquire().await.is_err(),
        "surviving observers must not own pool admission"
    );
    sqlx::query("SELECT 1").execute(&mut *held).await.unwrap();
    held.complete().await.unwrap();
    drop(held);
    drop(observer);
    assert!(pool.idle.lock().unwrap().connections.is_empty());
    assert!(pool.permits.is_closed());
    // Explicit shutdown also closes the dedicated backend without requiring Drop.
    let shutdown_pool = OwnedPool::connect(configured(&fixture.socket))
        .await
        .unwrap();
    let mut service = HistoryService::new(HistoryWriter {
        pool: WriterPool::Owned(shutdown_pool.clone()),
    });
    let _ = service.shutdown().await;
    assert!(shutdown_pool.permits.is_closed());
    assert!(shutdown_pool.idle.lock().unwrap().connections.is_empty());
    assert!(shutdown_pool.acquire().await.is_err());
    // Cancellation at the real service's first shutdown await must close its
    // owned backend even while the service and a non-owning observer survive.
    let cancelled_pool = OwnedPool::connect(configured(&fixture.socket))
        .await
        .unwrap();
    let mut service = HistoryService::new(HistoryWriter {
        pool: WriterPool::Owned(cancelled_pool.clone()),
    });
    let observer = service.observer();
    {
        let shutdown = service.shutdown();
        tokio::pin!(shutdown);
        assert!(futures::poll!(&mut shutdown).is_pending());
    }
    assert!(cancelled_pool.permits.is_closed());
    assert!(cancelled_pool.idle.lock().unwrap().connections.is_empty());
    assert!(cancelled_pool.acquire().await.is_err());
    let _ = service.shutdown().await;
    drop(service);
    drop(observer);
}

#[tokio::test]
#[ignore = "owned PG fixture: scripts/check-query-history-startup.sh"]
async fn operational_role_and_schema_check_cancellation_is_unavailable_not_rejected() {
    let fixture = fixture().await;
    let mut connection = sqlx::PgConnection::connect_with(&configured(&fixture.socket))
        .await
        .unwrap();
    sqlx::query("SET statement_timeout='50ms'")
        .execute(&mut connection)
        .await
        .unwrap();
    for catalog in ["pg_auth_members", "pg_trigger"] {
        let mut lock = fixture.admin.begin().await.unwrap();
        // Fixed fixture-only catalog choices, never operator/user SQL.
        let sql = match catalog {
            "pg_auth_members" => "LOCK TABLE pg_catalog.pg_auth_members IN ACCESS EXCLUSIVE MODE",
            _ => "LOCK TABLE pg_catalog.pg_trigger IN ACCESS EXCLUSIVE MODE",
        };
        sqlx::query(sql).execute(&mut *lock).await.unwrap();
        assert_eq!(
            verify_collector(&mut connection).await,
            Err(HistoryError::Unavailable)
        );
        lock.rollback().await.unwrap();
        // Drain the known server cancellation in this owned test connection.
        connection.flush().await.unwrap();
    }
    sqlx::query("SET statement_timeout=0")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("ALTER ROLE qh_writer SUPERUSER")
        .execute(&fixture.admin)
        .await
        .unwrap();
    assert_eq!(
        verify_collector(&mut connection).await,
        Err(HistoryError::Privilege)
    );
    sqlx::query("ALTER ROLE qh_writer NOSUPERUSER")
        .execute(&fixture.admin)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE query_history.executions DISABLE TRIGGER immutable_effective_v3")
        .execute(&fixture.admin)
        .await
        .unwrap();
    assert_eq!(
        verify_collector(&mut connection).await,
        Err(HistoryError::Privilege)
    );
    sqlx::query("ALTER TABLE query_history.executions ENABLE TRIGGER immutable_effective_v3")
        .execute(&fixture.admin)
        .await
        .unwrap();
    assert_eq!(verify_collector(&mut connection).await, Ok(()));
}

#[tokio::test]
#[ignore = "owned PG fixture: scripts/check-query-history-startup.sh"]
async fn late_server_error_after_100ms_timeout_has_no_pool_release_or_close_log_escape() {
    let fixture = fixture().await;
    let admin = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(options(&fixture.socket, "fixture_admin").disable_statement_logging())
        .await
        .unwrap();
    let captured = Arc::new(Mutex::new(Vec::<u8>::new()));
    struct Sink(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let mut output = self.0.lock().unwrap();
            assert!(
                output.len() + bytes.len() <= 2 * 1024 * 1024,
                "bounded in-memory diagnostic capture exceeded"
            );
            output.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let output = captured.clone();
    // Global for this single selected test process: unlike a thread-local
    // subscriber this also observes any unexpected SQLx spawned return task.
    tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || Sink(output.clone()))
        .try_init()
        .expect("isolated regression logger");
    tracing::warn!("fixed history tracing probe");
    log::warn!("fixed history logging probe");
    const SENTINEL: &str = "synthetic_late_server_error_private_sentinel";
    sqlx::raw_sql("CREATE FUNCTION query_history.fixture_late_error() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(724918220195); RAISE EXCEPTION 'synthetic_late_server_error_private_sentinel'; END $$; REVOKE ALL ON FUNCTION query_history.fixture_late_error() FROM PUBLIC; CREATE TRIGGER fixture_late_error BEFORE INSERT ON query_history.owners FOR EACH ROW EXECUTE FUNCTION query_history.fixture_late_error();")
        .execute(&admin).await.unwrap();
    let mut gate = admin.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(724918220195)")
        .execute(&mut *gate)
        .await
        .unwrap();
    // Control: same real server trigger. Keep a raw pending connection after
    // client timeout, then read its LATE ErrorResponse as SQLx release ping did.
    // Verify the synthetic body without ever formatting/logging/persisting it.
    let mut control = sqlx::PgConnection::connect_with(&configured(&fixture.socket))
        .await
        .unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut control)
        .await
        .unwrap();
    let control_sql = sqlx::query(
        "INSERT INTO query_history.owners(owner_uuid,heartbeat_at) VALUES($1,clock_timestamp())",
    )
    .bind(uuid::Uuid::new_v4());
    let (timed, ()) = tokio::join!(
        timeout(WRITE_BUDGET, control_sql.execute(&mut control)),
        waiting(&admin, pid)
    );
    assert!(timed.is_err());
    sqlx::query("SELECT pg_advisory_unlock(724918220195)")
        .execute(&mut *gate)
        .await
        .unwrap();
    let late = timeout(Duration::from_secs(3), control.flush())
        .await
        .expect("bounded control ERROR read")
        .expect_err("control should receive late server ERROR");
    assert!(late
        .as_database_error()
        .is_some_and(|e| e.message() == SENTINEL));
    drop(late);
    drop(control);

    let pool = OwnedPool::connect(configured(&fixture.socket))
        .await
        .unwrap();
    let writer = HistoryWriter {
        pool: WriterPool::Owned(pool.clone()),
    };
    sqlx::query("SELECT pg_advisory_lock(724918220195)")
        .execute(&mut *gate)
        .await
        .unwrap();
    // Identify the one initial idle socket; only the internal test sees PIDs.
    let pid: i32 = {
        let mut lease = pool.acquire().await.unwrap();
        let pid = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *lease)
            .await
            .unwrap();
        lease.complete().await.unwrap();
        pid
    };
    let (timed, ()) = tokio::join!(
        timeout(WRITE_BUDGET, writer.heartbeat(uuid::Uuid::new_v4())),
        waiting(&admin, pid)
    );
    assert!(timed.is_err());
    assert!(
        pool.idle.lock().unwrap().connections.is_empty(),
        "timed-out lease must not be released/reused"
    );
    sqlx::query("SELECT pg_advisory_unlock(724918220195)")
        .execute(&mut *gate)
        .await
        .unwrap();
    timeout(Duration::from_secs(3), async {
        loop {
            let alive: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)")
                    .bind(pid)
                    .fetch_one(&admin)
                    .await
                    .unwrap();
            if !alive {
                break;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("discarded backend did not close");
    // A normally returned server ERROR is fixed too, and discards the lease.
    let error = timeout(WRITE_BUDGET, writer.heartbeat(uuid::Uuid::new_v4()))
        .await
        .expect("bounded normal ERROR return")
        .expect_err("expected fixed operation failure");
    assert_eq!(error, HistoryError::Unavailable);
    assert_eq!(pool.idle.lock().unwrap().connections.len(), 0);
    assert!(
        writer.receipt().await.is_ok(),
        "fresh connection recovery must remain possible"
    );
    writer.close_owned();
    drop(writer);
    drop(pool);
    sqlx::raw_sql("DROP TRIGGER fixture_late_error ON query_history.owners; DROP FUNCTION query_history.fixture_late_error();").execute(&admin).await.unwrap();
    let logs = captured.lock().unwrap();
    let text = String::from_utf8_lossy(&logs);
    assert!(
        text.contains("fixed history tracing probe")
            && text.contains("fixed history logging probe")
    );
    assert!(
        !text.contains(SENTINEL),
        "late upstream ERROR escaped dedicated operation/release/close boundary"
    );
    assert!(
        !text.contains("on-release") && !text.contains("closing the pool connection"),
        "dedicated backend must not run SQLx pool return/close callbacks"
    );
}
