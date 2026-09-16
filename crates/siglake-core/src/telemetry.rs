//! Process-wide telemetry initialization: structured logs and distributed
//! traces via OpenTelemetry (OTLP/HTTP), plus the existing Prometheus metrics
//! scrape (kept unchanged in [`crate::metrics`]).
//!
//! ## What this wires
//!
//! - **Logs** — `opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge`
//!   forwards existing `tracing::info!/warn!/error!` events to an OTel
//!   `SdkLoggerProvider` as OTLP log records. **No log call-site changes** — every
//!   existing `tracing::!` call in the workspace becomes an OTel log automatically.
//! - **Traces** — `tracing_opentelemetry::layer()` forwards `tracing` spans
//!   (created via `#[tracing::instrument]` / `*_span!`) to an OTel
//!   `SdkTracerProvider` as OTLP spans. Spans are net-new instrumentation added at
//!   request/cycle boundaries; see the crate call-sites for where they're placed.
//!   W3C `traceparent` propagation is installed globally
//!   (`TraceContextPropagator`) so coordinator→worker HTTP fan-out stitches into a
//!   single trace tree.
//! - **Metrics** — *not* handled here. The `metrics` crate + Prometheus exporter
//!   ([`crate::metrics::init`]) stays the metrics path on the app side; an OTel
//!   Collector with a Prometheus receiver unifies them into OTLP downstream. None
//!   of the `metrics::` call sites change.
//!
//! ## Opt-in
//!
//! OTel emission is **off by default**. It turns on iff
//! `OTEL_EXPORTER_OTLP_ENDPOINT` is set (and `SIGLAKE_OTEL_DISABLED=1` is not).
//! Deployments that don't set it get a fmt-only subscriber equivalent to the
//! previous inlined `tracing_subscriber::fmt()…init()`, on stderr, which is where
//! every siglake binary's console output goes. Standard OTel env vars drive
//! behavior: `OTEL_SERVICE_NAME`, `OTEL_SERVICE_NAMESPACE`,
//! `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_EXPORTER_OTLP_HEADERS`,
//! `OTEL_TRACES_EXPORTER`, `OTEL_LOGS_EXPORTER`.
//!
//! ## Shutdown
//!
//! The providers live in the [`TELEMETRY`] `OnceLock`, which never drops, so
//! nothing flushes them by itself — a `Drop` guard on a process-lifetime static
//! is not a flush. Every binary calls [`shutdown`] explicitly on the one path
//! all of its exits funnel through (`main` wrapping a `run()`), which covers the
//! graceful-shutdown return, a startup error and a bad flag alike.
//!
//! ## Environment reads
//!
//! Every env read happens once, in [`TelemetryConfig::from_env`], which is a thin
//! wrapper over the pure [`TelemetryConfig::resolve`] (plus the per-knob
//! `*_from` twins). Tests drive the pure functions; nothing here mutates the
//! process environment.

use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result};
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

/// Configuration for [`init`]. Build with [`TelemetryConfig::from_env`] so the
/// standard `OTEL_*` env vars drive behavior and the `service.name` defaults to
/// `siglake-<component>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryConfig {
    /// OTLP/HTTP endpoint, e.g. `http://otel-collector:4318`. `None` ⇒ OTel
    /// emission off (fmt-only subscriber, identical to pre-OTel behavior).
    pub otlp_endpoint: Option<String>,
    /// `OTEL_EXPORTER_OTLP_HEADERS` ("k=v,k=v"), forwarded as exporter headers.
    pub otlp_headers: Option<String>,
    pub service_name: String,
    pub enable_traces: bool,
    pub enable_logs: bool,
    /// Resource attributes beyond `service.name`, already resolved from the
    /// environment (`service.namespace`, `host.name`, `service.instance.id`,
    /// `OTEL_RESOURCE_ATTRIBUTES`), so building the `Resource` reads no env.
    pub resource_attributes: Vec<(String, String)>,
    /// Fallback `RUST_LOG`-style filter when `RUST_LOG` is unset, e.g.
    /// `info,siglake=debug`.
    pub default_log_filter: String,
    /// Route the fmt (console) layer to stderr. Defaults to `true`: every
    /// siglake binary keeps stdout free for its reports (`--print-crd`,
    /// `migrate-schema --dry-run`, the SQL client's rows).
    pub fmt_to_stderr: bool,
}

impl TelemetryConfig {
    /// Read the standard `OTEL_*` env vars. `component` (e.g. `"ingest"`,
    /// `"query"`) seeds the default `OTEL_SERVICE_NAME` (`siglake-<component>`).
    pub fn from_env(component: &str) -> Self {
        Self::resolve(component, &|key| env::var(key).ok())
    }

    /// The pure twin of [`TelemetryConfig::from_env`]: every env read goes
    /// through `get`, so tests pass a fixed lookup instead of mutating the
    /// process environment.
    pub fn resolve(component: &str, get: &dyn Fn(&str) -> Option<String>) -> Self {
        let otlp_endpoint = otlp_endpoint_from(get("OTEL_EXPORTER_OTLP_ENDPOINT").as_deref());
        let otel_on =
            otlp_endpoint.is_some() && !otel_disabled_from(get("SIGLAKE_OTEL_DISABLED").as_deref());

        Self {
            otlp_endpoint,
            otlp_headers: get("OTEL_EXPORTER_OTLP_HEADERS"),
            service_name: service_name_from(get("OTEL_SERVICE_NAME").as_deref(), component),
            enable_traces: otel_on && exporter_enabled_from(get("OTEL_TRACES_EXPORTER").as_deref()),
            enable_logs: otel_on && exporter_enabled_from(get("OTEL_LOGS_EXPORTER").as_deref()),
            resource_attributes: resource_attributes_from(
                get("OTEL_SERVICE_NAMESPACE").as_deref(),
                get("HOSTNAME").or_else(|| get("HOST")).as_deref(),
                get("POD_NAME").as_deref(),
                get("OTEL_RESOURCE_ATTRIBUTES").as_deref(),
            ),
            default_log_filter: format!("info,siglake=debug,siglake_{component}=debug"),
            fmt_to_stderr: true,
        }
    }
}

/// An empty `OTEL_EXPORTER_OTLP_ENDPOINT` is "unset": a chart that renders the
/// variable with no value must not turn emission on.
fn otlp_endpoint_from(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// `SIGLAKE_OTEL_DISABLED=1|true` is the kill switch that overrides a set
/// endpoint (a deployment can turn emission off without editing the endpoint).
fn otel_disabled_from(raw: Option<&str>) -> bool {
    matches!(raw.map(str::trim), Some(v) if v == "1" || v.eq_ignore_ascii_case("true"))
}

/// `OTEL_{TRACES,LOGS}_EXPORTER`: `none`/`off` turns that signal off, anything
/// else (including unset) leaves it on, per the OTel env-var spec.
fn exporter_enabled_from(raw: Option<&str>) -> bool {
    match raw.map(str::trim) {
        Some(v) => !v.eq_ignore_ascii_case("none") && !v.eq_ignore_ascii_case("off"),
        None => true,
    }
}

fn service_name_from(raw: Option<&str>, component: &str) -> String {
    raw.map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("siglake-{component}"))
}

/// Resource attributes beyond `service.name`, in precedence order: the explicit
/// `OTEL_SERVICE_NAMESPACE`, the pod/host identity a Kubernetes deployment
/// injects, then the operator's free-form `OTEL_RESOURCE_ATTRIBUTES`.
fn resource_attributes_from(
    namespace: Option<&str>,
    host: Option<&str>,
    pod: Option<&str>,
    extra: Option<&str>,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut push = |key: &str, value: Option<&str>| {
        if let Some(v) = value.map(str::trim).filter(|s| !s.is_empty()) {
            out.push((key.to_string(), v.to_string()));
        }
    };
    push("service.namespace", namespace);
    push("host.name", host);
    push("service.instance.id", pod);
    if let Some(extra) = extra {
        let mut pairs: Vec<(String, String)> = parse_kv_pairs(extra).into_iter().collect();
        pairs.sort();
        out.extend(pairs);
    }
    out
}

/// Owns the OTel providers for the process lifetime. Stored in the process-wide
/// [`TELEMETRY`] static so [`shutdown`] can flush from the binaries' single exit
/// path. The static never drops, so `Drop` is not a flush — see [`shutdown`].
struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    shutdown: AtomicBool,
}

static TELEMETRY: OnceLock<TelemetryGuard> = OnceLock::new();

impl TelemetryGuard {
    /// Flush the batch span/log processors and shut the exporters down. Safe to
    /// call once; subsequent calls are no-ops.
    fn shutdown(&self) {
        if self.shutdown.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(p) = &self.logger_provider {
            if let Err(e) = p.shutdown() {
                eprintln!("otel logger provider shutdown error: {e:?}");
            }
        }
        if let Some(p) = &self.tracer_provider {
            if let Err(e) = p.shutdown() {
                eprintln!("otel tracer provider shutdown error: {e:?}");
            }
        }
    }
}

/// Flush the OTel providers and shut their exporters down. Call this from the
/// single path every exit of the process funnels through — `main` wrapping a
/// `run()` — so a graceful shutdown, a startup error and a bad flag all flush.
/// Nothing else flushes: [`TELEMETRY`] is a `OnceLock` that never drops. Safe to
/// call when OTel is off, before [`init`], or twice (no-op).
pub fn shutdown() {
    if let Some(g) = TELEMETRY.get() {
        g.shutdown();
    }
}

/// Initialize the global tracing subscriber with the fmt layer plus, when OTel
/// is enabled, OTel traces + logs layers exporting via OTLP/HTTP. Also installs
/// the W3C `TraceContextPropagator` globally so `opentelemetry_http`
/// `HeaderInjector`/`HeaderExtractor` carry `traceparent` across HTTP hops.
///
/// Stores the providers in a process-wide static; call [`shutdown`] before the
/// process exits to flush. When OTel is off (`otlp_endpoint` is `None`) this is
/// equivalent to the old `tracing_subscriber::fmt().with_env_filter(...)
/// .with_writer(std::io::stderr).init()`.
pub fn init(cfg: TelemetryConfig) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&cfg.default_log_filter));

    // One fmt layer, optionally to stderr, then per-layer-filtered. Boxing keeps
    // the stderr/stdout writer choice behind a single type so all OTel branches
    // below share it.
    let fmt_inner: Box<dyn Layer<Registry> + Send + Sync> = if cfg.fmt_to_stderr {
        Box::new(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_writer(std::io::stderr),
        )
    } else {
        Box::new(tracing_subscriber::fmt::layer().with_target(true))
    };
    let fmt_layer = fmt_inner.with_filter(filter.clone());

    // OTel off: fmt-only, identical to the pre-OTel inlined init.
    if cfg.otlp_endpoint.is_none() || (!cfg.enable_traces && !cfg.enable_logs) {
        tracing_subscriber::registry().with(fmt_layer).init();
        let _ = TELEMETRY.set(TelemetryGuard {
            tracer_provider: None,
            logger_provider: None,
            shutdown: AtomicBool::new(false),
        });
        return Ok(());
    }

    let resource = build_resource(&cfg);
    let endpoint = cfg.otlp_endpoint.as_deref().expect("endpoint present");
    let headers = cfg
        .otlp_headers
        .as_deref()
        .map(parse_kv_pairs)
        .unwrap_or_default();

    // W3C tracecontext propagation (used by the coordinator/worker HTTP fan-out).
    global::set_text_map_propagator(TraceContextPropagator::new());

    let mut tracer_provider: Option<SdkTracerProvider> = None;
    let mut tracer: Option<opentelemetry_sdk::trace::SdkTracer> = None;
    if cfg.enable_traces {
        let span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_protocol(Protocol::HttpBinary)
            .with_timeout(Duration::from_secs(10))
            .with_headers(headers.clone())
            .build()
            .context("build OTLP span exporter")?;
        let provider = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(span_exporter)
            .build();
        global::set_tracer_provider(provider.clone());
        tracer = Some(provider.tracer("siglake"));
        tracer_provider = Some(provider);
    }

    let mut logger_provider: Option<SdkLoggerProvider> = None;
    if cfg.enable_logs {
        let log_exporter = opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_protocol(Protocol::HttpBinary)
            .with_timeout(Duration::from_secs(10))
            .with_headers(headers)
            .build()
            .context("build OTLP log exporter")?;
        let provider = SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(log_exporter)
            .build();
        logger_provider = Some(provider);
    }

    // Suppress the telemetry-induced-telemetry loop: do not re-export logs from
    // the HTTP/gRPC transport crates or the OTel SDK itself.
    let otel_log_filter = filter
        .add_directive("hyper=off".parse().expect("directive"))
        .add_directive("tonic=off".parse().expect("directive"))
        .add_directive("h2=off".parse().expect("directive"))
        .add_directive("reqwest=off".parse().expect("directive"))
        .add_directive("opentelemetry=off".parse().expect("directive"));

    match (cfg.enable_traces, cfg.enable_logs) {
        (true, true) => {
            let traces = tracing_opentelemetry::layer().with_tracer(tracer.expect("traces on"));
            let logs = OpenTelemetryTracingBridge::new(logger_provider.as_ref().expect("logs on"))
                .with_filter(otel_log_filter);
            tracing_subscriber::registry()
                .with(fmt_layer)
                .with(traces)
                .with(logs)
                .init();
        }
        (true, false) => {
            let traces = tracing_opentelemetry::layer().with_tracer(tracer.expect("traces on"));
            tracing_subscriber::registry()
                .with(fmt_layer)
                .with(traces)
                .init();
        }
        (false, true) => {
            let logs = OpenTelemetryTracingBridge::new(logger_provider.as_ref().expect("logs on"))
                .with_filter(otel_log_filter);
            tracing_subscriber::registry()
                .with(fmt_layer)
                .with(logs)
                .init();
        }
        (false, false) => unreachable!("OTel-off branch returned early"),
    }

    tracing::info!(
        otel.endpoint = endpoint,
        otel.traces = cfg.enable_traces,
        otel.logs = cfg.enable_logs,
        service.name = %cfg.service_name,
        "opentelemetry emission enabled",
    );

    let _ = TELEMETRY.set(TelemetryGuard {
        tracer_provider,
        logger_provider,
        shutdown: AtomicBool::new(false),
    });
    Ok(())
}

fn build_resource(cfg: &TelemetryConfig) -> Resource {
    let mut builder = Resource::builder().with_service_name(cfg.service_name.clone());
    let attrs: Vec<KeyValue> = cfg
        .resource_attributes
        .iter()
        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
        .collect();
    if !attrs.is_empty() {
        builder = builder.with_attributes(attrs);
    }
    builder.build()
}

fn parse_kv_pairs(s: &str) -> HashMap<String, String> {
    s.split(',')
        .filter_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            let k = k.trim();
            if k.is_empty() {
                return None;
            }
            Some((k.to_string(), v.trim().to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    use opentelemetry::trace::Tracer;
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};

    /// Records what the batch processor hands the exporter, and how many times
    /// the provider shuts it down. The SDK's own `InMemorySpanExporter` clears
    /// its store on shutdown, which is exactly the moment under test here.
    #[derive(Debug, Default, Clone)]
    struct RecordingExporter {
        exported: Arc<Mutex<Vec<String>>>,
        shutdowns: Arc<Mutex<usize>>,
    }

    impl SpanExporter for RecordingExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            let mut exported = self.exported.lock().expect("exported");
            exported.extend(batch.into_iter().map(|s| s.name.to_string()));
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            *self.shutdowns.lock().expect("shutdowns") += 1;
            Ok(())
        }
    }

    /// A lookup over a fixed table, standing in for the process environment.
    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |key: &str| {
            pairs
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn parse_kv_pairs_basic() {
        let m = parse_kv_pairs("a=1, b=two ,c=,=x");
        assert_eq!(m.get("a"), Some(&"1".to_string()));
        assert_eq!(m.get("b"), Some(&"two".to_string()));
        assert_eq!(m.get("c"), Some(&"".to_string()));
        assert!(!m.contains_key(""));
    }

    #[test]
    fn resolve_defaults_service_name_and_leaves_otel_off() {
        let cfg = TelemetryConfig::resolve("query", &env_of(&[]));
        assert_eq!(cfg.service_name, "siglake-query");
        assert!(cfg.otlp_endpoint.is_none());
        assert!(!cfg.enable_traces);
        assert!(!cfg.enable_logs);
        assert!(cfg.fmt_to_stderr, "console output stays on stderr");
        assert!(cfg.resource_attributes.is_empty());
    }

    #[test]
    fn resolve_turns_both_signals_on_for_an_endpoint() {
        let cfg = TelemetryConfig::resolve(
            "ingest",
            &env_of(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")]),
        );
        assert_eq!(cfg.otlp_endpoint.as_deref(), Some("http://collector:4318"));
        assert!(cfg.enable_traces);
        assert!(cfg.enable_logs);
    }

    #[test]
    fn endpoint_and_disable_switch() {
        assert_eq!(otlp_endpoint_from(None), None);
        assert_eq!(otlp_endpoint_from(Some("")), None);
        assert_eq!(otlp_endpoint_from(Some("  ")), None);
        assert_eq!(
            otlp_endpoint_from(Some(" http://c:4318 ")).as_deref(),
            Some("http://c:4318")
        );

        assert!(!otel_disabled_from(None));
        assert!(!otel_disabled_from(Some("0")));
        assert!(otel_disabled_from(Some("1")));
        assert!(otel_disabled_from(Some("TRUE")));

        // The kill switch beats a set endpoint.
        let cfg = TelemetryConfig::resolve(
            "compactor",
            &env_of(&[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318"),
                ("SIGLAKE_OTEL_DISABLED", "1"),
            ]),
        );
        assert!(!cfg.enable_traces);
        assert!(!cfg.enable_logs);
    }

    #[test]
    fn per_signal_exporter_switch() {
        assert!(exporter_enabled_from(None));
        assert!(exporter_enabled_from(Some("otlp")));
        assert!(!exporter_enabled_from(Some("none")));
        assert!(!exporter_enabled_from(Some("OFF")));

        let cfg = TelemetryConfig::resolve(
            "query",
            &env_of(&[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318"),
                ("OTEL_LOGS_EXPORTER", "none"),
            ]),
        );
        assert!(cfg.enable_traces);
        assert!(!cfg.enable_logs, "logs off, traces still on");
    }

    #[test]
    fn service_name_override_wins() {
        assert_eq!(service_name_from(None, "query"), "siglake-query");
        assert_eq!(service_name_from(Some(""), "query"), "siglake-query");
        assert_eq!(service_name_from(Some("shard-a"), "query"), "shard-a");
    }

    #[test]
    fn resource_attributes_carry_pod_identity_and_extras() {
        let attrs = resource_attributes_from(
            Some("prod"),
            Some("node-7"),
            Some("siglake-query-1"),
            Some("region=eu-central-1,team=platform"),
        );
        assert_eq!(
            attrs,
            vec![
                ("service.namespace".to_string(), "prod".to_string()),
                ("host.name".to_string(), "node-7".to_string()),
                (
                    "service.instance.id".to_string(),
                    "siglake-query-1".to_string()
                ),
                ("region".to_string(), "eu-central-1".to_string()),
                ("team".to_string(), "platform".to_string()),
            ]
        );
        assert!(resource_attributes_from(None, Some(""), None, None).is_empty());
    }

    /// The claim [`shutdown`] makes: buffered spans reach the exporter because
    /// the provider is shut down explicitly, not because anything drops. The
    /// `OnceLock` in production never drops, so this is the whole flush.
    #[test]
    fn provider_shutdown_flushes_and_is_idempotent() {
        let exporter = RecordingExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        tracer.in_span("buffered", |_| {});

        let guard = TelemetryGuard {
            tracer_provider: Some(provider),
            logger_provider: None,
            shutdown: AtomicBool::new(false),
        };
        assert!(
            exporter.exported.lock().unwrap().is_empty(),
            "batch processor holds the span until shutdown"
        );

        guard.shutdown();
        assert_eq!(
            *exporter.exported.lock().unwrap(),
            vec!["buffered".to_string()],
            "shutdown flushed the batch processor"
        );
        assert_eq!(*exporter.shutdowns.lock().unwrap(), 1);

        // A second call must not export or shut down again: a binary may call
        // `shutdown` after a path that already did.
        guard.shutdown();
        assert_eq!(exporter.exported.lock().unwrap().len(), 1);
        assert_eq!(*exporter.shutdowns.lock().unwrap(), 1);
    }

    /// `shutdown()` before `init()` — the path a binary takes when clap rejects
    /// a flag — must not panic.
    #[test]
    fn shutdown_before_init_is_a_no_op() {
        super::shutdown();
    }
}
