//! Actual HTTP redirect regressions. Only synthetic counters are reported;
//! the downgrade target is the real legacy shard handler, not a runner mock.
use super::*;
use crate::coordinator::{coordinate_with_failover, HttpShardRunner};
use axum::{
    extract::Request,
    middleware::{self, Next},
    response::IntoResponse,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

const LEGACY: &str = "/api/v1/sql/shard";
const TOKEN: &str = "synthetic-redirect-coordinator";
const QUERY: &str = "SELECT raw FROM events";
#[derive(Clone, Copy)]
struct Redirect {
    status: u16,
    absolute: bool,
    v2_target: bool,
    location: bool,
}
struct Observations {
    mode: Mutex<Redirect>,
    legacy_executions: AtomicUsize,
    legacy_requests: AtomicUsize,
    forwarded_v2: AtomicUsize,
    synthetic_post_auth: AtomicUsize,
    fallback: AtomicUsize,
}
impl Observations {
    fn new() -> Self {
        Self {
            mode: Mutex::new(Redirect {
                status: 307,
                absolute: false,
                v2_target: false,
                location: true,
            }),
            legacy_executions: AtomicUsize::new(0),
            legacy_requests: AtomicUsize::new(0),
            forwarded_v2: AtomicUsize::new(0),
            synthetic_post_auth: AtomicUsize::new(0),
            fallback: AtomicUsize::new(0),
        }
    }
    fn reset(&self, mode: Redirect) {
        *self.mode.lock().unwrap() = mode;
        for counter in [
            &self.legacy_executions,
            &self.legacy_requests,
            &self.forwarded_v2,
            &self.synthetic_post_auth,
            &self.fallback,
        ] {
            counter.store(0, Ordering::SeqCst);
        }
    }
}
struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn shutdown(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
struct Fixture {
    worker: Server,
    fallback: Server,
    observations: Arc<Observations>,
    planning: datafusion::prelude::SessionContext,
    _scratch: tempfile::TempDir,
}
async fn fixture() -> Fixture {
    let scratch = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        siglake_storage::iceberg::IcebergContext::open(scratch.path())
            .await
            .unwrap(),
    );
    ice.append_events(&[siglake_core::Event::now("synthetic-redirect-event")])
        .await
        .unwrap();
    let planning = datafusion::prelude::SessionContext::new();
    ice.register_with_datafusion(&planning).await.unwrap();
    let state = crate::AppState::new(ice, crate::AuthConfig::from_tokens(["synthetic-external"]))
        .with_coordinator(vec![], Some(TOKEN.into()));
    let observations = Arc::new(Observations::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = crate::router(state.clone()).layer(middleware::from_fn({
        let observations = observations.clone();
        let origin = url.clone();
        move |request: Request, next: Next| {
            let observations = observations.clone();
            let origin = origin.clone();
            async move {
                let redirected = request.uri().query() == Some("redirected=1");
                let legacy = request.uri().path() == LEGACY;
                if !redirected {
                    let mode = *observations.mode.lock().unwrap();
                    let mut response = axum::http::StatusCode::from_u16(mode.status)
                        .unwrap()
                        .into_response();
                    if mode.location {
                        let path = if mode.v2_target { PATH } else { LEGACY };
                        let location = if mode.absolute {
                            format!("{origin}{path}?redirected=1")
                        } else {
                            format!("{path}?redirected=1")
                        };
                        response
                            .headers_mut()
                            .insert(axum::http::header::LOCATION, location.parse().unwrap());
                    }
                    return response;
                }
                if legacy {
                    observations.legacy_requests.fetch_add(1, Ordering::SeqCst);
                }
                if !legacy {
                    observations.forwarded_v2.fetch_add(1, Ordering::SeqCst);
                }
                let synthetic_post_auth = request.method() == axum::http::Method::POST
                    && request
                        .headers()
                        .get(axum::http::header::AUTHORIZATION)
                        .is_some_and(|h| h == format!("Bearer {TOKEN}").as_str());
                let response = next.run(request).await;
                // Increment only AFTER the actual native v1 handler succeeds.
                if legacy && response.status().is_success() {
                    observations
                        .legacy_executions
                        .fetch_add(1, Ordering::SeqCst);
                    if synthetic_post_auth {
                        observations
                            .synthetic_post_auth
                            .fetch_add(1, Ordering::SeqCst);
                    }
                }
                response
            }
        }
    }));
    let worker = Server {
        url,
        task: tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = crate::router(state).layer(middleware::from_fn({
        let observations = observations.clone();
        move |request: Request, next: Next| {
            observations.fallback.fetch_add(1, Ordering::SeqCst);
            next.run(request)
        }
    }));
    let fallback = Server {
        url,
        task: tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }),
    };
    Fixture {
        worker,
        fallback,
        observations,
        planning,
        _scratch: scratch,
    }
}
fn dispatch() -> Dispatch {
    let mut context = super::tests::context();
    context.binding = Binding::Operator { tenant: None };
    context.state = State::Inherited;
    context.revision = None;
    context.scan_warning = None;
    context.timeout_ms = 60_000;
    context.remaining_ms = 60_000;
    context.original_expires_unix_ms = unix_ms_floor(SystemTime::now()).unwrap() + 60_000;
    Dispatch {
        context,
        deadline: tokio::time::Instant::now() + Duration::from_secs(60),
    }
}
async fn coordinate(f: &Fixture, controlled: bool) -> anyhow::Result<()> {
    let controls = controlled.then(dispatch);
    let runner = HttpShardRunner::new(vec![f.worker.url.clone(); 2], Some(TOKEN.into()))
        .with_controls(controls.clone());
    let fallback = HttpShardRunner::new(vec![f.fallback.url.clone(); 2], Some(TOKEN.into()))
        .with_controls(controls);
    coordinate_with_failover(&f.planning, &runner, Some(&fallback), QUERY, 2, vec![])
        .await
        .map(|_| ())
}
fn unavailable_without_location(result: anyhow::Result<()>) {
    let error = crate::error::ApiError::from_query(
        result.unwrap_err(),
        crate::limits::Priority::Interactive,
    );
    assert_eq!(error.status, 503);
    assert!(!error
        .headers
        .iter()
        .any(|(name, _)| name == axum::http::header::LOCATION));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controlled_http_307_308_never_reach_legacy_v2_target_or_self_fallback() {
    let f = fixture().await;
    for status in [307, 308] {
        for absolute in [false, true] {
            for v2_target in [false, true] {
                f.observations.reset(Redirect {
                    status,
                    absolute,
                    v2_target,
                    location: true,
                });
                let result = coordinate(&f, true).await;
                let legacy = f.observations.legacy_executions.load(Ordering::SeqCst);
                let forwarded_v2 = f.observations.forwarded_v2.load(Ordering::SeqCst);
                let fallback = f.observations.fallback.load(Ordering::SeqCst);
                let synthetic_post_auth = f.observations.synthetic_post_auth.load(Ordering::SeqCst);
                eprintln!("controlled redirect status={status} absolute={absolute} v2_target={v2_target}: legacy_execution={legacy} forwarded_v2={forwarded_v2} self_fallback={fallback} parent_success={} synthetic_post_auth={synthetic_post_auth}",result.is_ok());
                assert_eq!(
                    legacy, 0,
                    "controlled redirect executed actual legacy shard"
                );
                assert_eq!(
                    forwarded_v2, 0,
                    "controlled redirect followed even a v2 target"
                );
                assert_eq!(fallback, 0, "controlled redirect retried on self");
                unavailable_without_location(result);
            }
        }
    }
    f.worker.shutdown().await;
    f.fallback.shutdown().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controlled_http_every_3xx_is_nonretryable_unavailable_without_location() {
    let f = fixture().await;
    for status in 300..400 {
        for location in [false, true] {
            f.observations.reset(Redirect {
                status,
                absolute: status % 2 == 0,
                v2_target: status % 2 != 0,
                location,
            });
            unavailable_without_location(coordinate(&f, true).await);
            assert_eq!(f.observations.legacy_requests.load(Ordering::SeqCst), 0);
            assert_eq!(f.observations.legacy_executions.load(Ordering::SeqCst), 0);
            assert_eq!(f.observations.forwarded_v2.load(Ordering::SeqCst), 0);
            assert_eq!(f.observations.fallback.load(Ordering::SeqCst), 0);
        }
    }
    eprintln!("all100 controlled3xx statuses with/without Location refused503; no caller Location, legacy request/execution, forwarded v2 or self fallback");
    f.worker.shutdown().await;
    f.fallback.shutdown().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_http_307_308_preserve_same_origin_post_body_and_auth_redirects() {
    let f = fixture().await;
    for status in [307, 308] {
        for absolute in [false, true] {
            f.observations.reset(Redirect {
                status,
                absolute,
                v2_target: false,
                location: true,
            });
            assert!(coordinate(&f, false).await.is_ok());
            assert_eq!(f.observations.legacy_executions.load(Ordering::SeqCst), 2);
            assert_eq!(f.observations.synthetic_post_auth.load(Ordering::SeqCst), 2);
            assert_eq!(f.observations.fallback.load(Ordering::SeqCst), 0);
        }
    }
    eprintln!("legacy307/308 relative/absolute same-origin redirects retain successful native POST/body/auth execution");
    f.worker.shutdown().await;
    f.fallback.shutdown().await;
}
