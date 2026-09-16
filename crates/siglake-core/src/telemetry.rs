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
//!   of the 334 `metrics::` call sites change.
//!
//! ## Opt-in
//!
//! OTel emission is **off by default**. It turns on iff
//! `OTEL_EXPORTER_OTLP_ENDPOINT` is set (and `SIGLAKE_OTEL_DISABLED=1` is not).
//! Deployments that don't set it get a fmt-only subscriber byte-identical to the
//! previous inlined `tracing_subscriber::fmt()…init()` — protecting existing
//! smoke rounds. Standard OTel env vars drive behavior:
//! `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_EXPORTER_OTLP_HEADERS`,
//! `OTEL_TRACES_EXPORTER`, `OTEL_LOGS_EXPORTER`, `OTEL_BSP_*`/`OTEL_BLRP_*`.
//!
//! ## Shutdown
//!
//! [`TelemetryGuard`] owns the OTel providers. Hold it for the process lifetime
//! and call [`TelemetryGuard::shutdown`] on the SIGTERM path before exit so the
//! batch processors flush buffered OTLP. `Drop` also shuts down as a safety net.

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
pub struct TelemetryConfig {
    /// OTLP/HTTP endpoint, e.g. `http://otel-collector:4318`. `None` ⇒ OTel
    /// emission off (fmt-only subscriber, identical to pre-OTel behavior).
    pub otlp_endpoint: Option<String>,
    /// `OTEL_EXPORTER_OTLP_HEADERS` ("k=v,k=v"), forwarded as exporter headers.
    pub otlp_headers: Option<String>,
    pub service_name: String,
    pub service_namespace: Option<String>,
    pub enable_traces: bool,
    pub enable_logs: bool,
    /// Fallback `RUST_LOG`-style filter when `RUST_LOG` is unset, e.g.
    /// `info,siglake=debug`.
    pub default_log_filter: String,
    /// Route the fmt (console) layer to stderr instead of stdout.
    pub fmt_to_stderr: bool,
}

impl TelemetryConfig {
    /// Read the standard `OTEL_*` env vars. `component` (e.g. `"ingest"`,
    /// `"query"`) seeds the default `OTEL_SERVICE_NAME` (`siglake-<component>`).
    pub fn from_env(component: &str) -> Self {
        let otlp_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty());
        let disabled = env::var("SIGLAKE_OTEL_DISABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let otel_on = otlp_endpoint.is_some() && !disabled;

        let exporter_on = |name: &str| {
            env::var(name)
                .map(|v| !v.eq_ignore_ascii_case("none") && !v.eq_ignore_ascii_case("off"))
                .unwrap_or(true)
        };

        Self {
            otlp_endpoint,
            otlp_headers: env::var("OTEL_EXPORTER_OTLP_HEADERS").ok(),
            service_name: env::var("OTEL_SERVICE_NAME")
                .unwrap_or_else(|_| format!("siglake-{component}")),
            service_namespace: env::var("OTEL_SERVICE_NAMESPACE").ok(),
            enable_traces: otel_on && exporter_on("OTEL_TRACES_EXPORTER"),
            enable_logs: otel_on && exporter_on("OTEL_LOGS_EXPORTER"),
            default_log_filter: format!("info,siglake=debug,siglake_{component}=debug"),
            fmt_to_stderr: false,
        }
    }
}

/// Owns the OTel providers for the process lifetime. Stored in the process-wide
/// [`TELEMETRY`] static so [`shutdown`] can flush from the SIGTERM path; `Drop`
/// flushes as a safety net on normal exit (static drop). Not returned to callers.
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

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Flush the OTel providers and shut their exporters down. Call this from the
/// SIGTERM/shutdown path before `process::exit` so buffered OTLP ships. Safe to
/// call when OTel is off or already shut down (no-op). On normal return from
/// `main` the [`TELEMETRY`] static's `Drop` does this automatically.
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
/// Stores the providers in a process-wide static; call [`shutdown`] on the
/// SIGTERM path to flush. When OTel is off (`otlp_endpoint` is `None`) this is
/// equivalent to the old `tracing_subscriber::fmt().with_env_filter(...).init()`.
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
    if let Some(ns) = &cfg.service_namespace {
        builder = builder.with_attributes([KeyValue::new("service.namespace", ns.clone())]);
    }
    if let Ok(host) = env::var("HOSTNAME").or_else(|_| env::var("HOST")) {
        if !host.is_empty() {
            builder = builder.with_attributes([KeyValue::new("host.name", host)]);
        }
    }
    if let Ok(pod) = env::var("POD_NAME") {
        if !pod.is_empty() {
            builder = builder.with_attributes([KeyValue::new("service.instance.id", pod)]);
        }
    }
    // Merge user-supplied OTEL_RESOURCE_ATTRIBUTES (k=v,k=v).
    if let Ok(attrs) = env::var("OTEL_RESOURCE_ATTRIBUTES") {
        let kvs: Vec<KeyValue> = parse_kv_pairs(&attrs)
            .into_iter()
            .map(|(k, v)| KeyValue::new(k, v))
            .collect();
        if !kvs.is_empty() {
            builder = builder.with_attributes(kvs);
        }
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

    #[test]
    fn parse_kv_pairs_basic() {
        let m = parse_kv_pairs("a=1, b=two ,c=,=x");
        assert_eq!(m.get("a"), Some(&"1".to_string()));
        assert_eq!(m.get("b"), Some(&"two".to_string()));
        assert_eq!(m.get("c"), Some(&"".to_string()));
        assert!(!m.contains_key(""));
    }

    #[test]
    fn from_env_defaults_service_name() {
        env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        env::remove_var("OTEL_SERVICE_NAME");
        let cfg = TelemetryConfig::from_env("query");
        assert_eq!(cfg.service_name, "siglake-query");
        assert!(cfg.otlp_endpoint.is_none());
        assert!(!cfg.enable_traces);
        assert!(!cfg.enable_logs);
    }
}
