//! Tiny Prometheus instant-query client.
//!
//! The operator polls four queries per cycle (per-component load
//! signals) and folds the results into [`ObservedSamples`]. We don't
//! pull in `prometheus-http-query` for this — a single GET on
//! `/api/v1/query` is enough and keeps the operator binary small.

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::scaling::ObservedSamples;

pub struct PromClient {
    base_url: String,
    http: reqwest::Client,
}

impl PromClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .context("build prometheus http client")?,
        })
    }

    pub async fn instant_value(&self, query: &str) -> Result<Option<f64>> {
        let url = format!("{}/api/v1/query", self.base_url);
        let resp = self
            .http
            .get(&url)
            .query(&[("query", query)])
            .send()
            .await
            .with_context(|| format!("GET {url}"))?
            .error_for_status()
            .with_context(|| format!("GET {url} non-2xx"))?;
        let body: PromResponse = resp.json().await.context("parse prometheus json")?;
        if body.status != "success" {
            anyhow::bail!("prometheus returned status={}", body.status);
        }
        // `instant` queries return a vector result with one entry per
        // matching series. We average across pods on the *prom* side
        // by wrapping the query in `avg(...)` at the caller, so we
        // expect a single scalar here.
        // The prometheus instant-query response shape is
        // `value: [<unix_ts: f64>, "<sample: string>"]`. Pull the
        // string at index 1 out of the JSON array and parse it.
        let val = body
            .data
            .result
            .into_iter()
            .next()
            .and_then(|r| r.value.get(1).cloned())
            .and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok()));
        Ok(val)
    }

    /// Snapshot the three load signals.
    ///
    /// A query that matches NOTHING is an ERROR, not a zero. `0.0` on a
    /// saturation signal means "idle", so an empty result — a mistyped
    /// selector, a scrape that never landed, labels the series do not carry —
    /// reads as an idle fleet and scales it down. That is not hypothetical: the
    /// default queries select `namespace`, `app_kubernetes_io_instance`, and
    /// `app_kubernetes_io_component`. The binaries do not attach the latter
    /// two and Prometheus Operator does not add them unless `targetLabels` is
    /// set, so the defaults matched nothing at all and every reading was a
    /// silent zero.
    ///
    /// A NaN or infinite sample is an ERROR too. Prometheus writes `NaN`,
    /// `+Inf` and `-Inf` as sample strings — a division by a zero pod count, a
    /// quantile over no observations — and `f64::from_str` accepts all three.
    /// Downstream they are not readings but a floor or a ceiling: the deadband
    /// comparison is false for every NaN, and the saturating `as i32` cast
    /// turns `ceil()`'s NaN into 0 (clamped to `min`) and its `+Inf` into
    /// `i32::MAX` (clamped to `max`). A non-finite value blended into the EWMA
    /// also poisons every later reading, because `NaN` propagates through the
    /// blend forever.
    ///
    /// An unusable reading is absent FOR ITS OWN SIGNAL. It puts that
    /// component on the same path as a Prometheus outage — the reconciler
    /// holds its replica count and leaves its smoothing history untouched —
    /// and leaves the other two components deciding on their own readings.
    ///
    /// This used to take all three samples or fail the snapshot, so one stale
    /// series froze the whole fleet's sizing. That is the second defect behind
    /// the zero-floor refusal: a compactor stopped at zero publishes nothing,
    /// and its absence would have held ingest and query at their current size
    /// for as long as it stayed stopped (`docs/DESIGN_compactor_wakeup_signal.md`).
    /// All three absent is still today's whole-outage behaviour exactly.
    pub async fn observed(&self, queries: &Queries, namespace: &str) -> ObservedSamples {
        async fn sample(this: &PromClient, q: &str, what: &str, namespace: &str) -> Option<f64> {
            let read = async {
                let value = this
                    .instant_value(q)
                    .await
                    .with_context(|| format!("query `{q}`"))?;
                usable_sample(value, q, what)
            };
            match read.await {
                Ok(value) => Some(value),
                Err(e) => {
                    tracing::warn!(error = %e, component = what,
                        "prometheus reading unusable; HOLDING this component's replica count \
                         rather than reading the gap as idleness");
                    metrics::counter!("siglake_operator_prom_query_errors_total",
                        "namespace" => namespace.to_string(),
                        "component" => what.to_string())
                    .increment(1);
                    None
                }
            }
        }
        ObservedSamples {
            ingester_rps_per_pod: sample(self, &queries.ingester_rps, "ingester", namespace).await,
            compactor_backlog: sample(self, &queries.compactor_backlog, "compactor", namespace)
                .await,
            query_in_flight_per_pod: sample(self, &queries.query_in_flight, "query", namespace)
                .await,
        }
    }
}

/// The sample check behind [`PromClient::observed`], split out so tests drive
/// it without an HTTP server: `None` (an empty result vector) and a non-finite
/// value are both errors, and a finite value passes through unchanged.
fn usable_sample(value: Option<f64>, query: &str, what: &str) -> Result<f64> {
    match value {
        Some(v) if v.is_finite() => Ok(v),
        Some(v) => anyhow::bail!(
            "the {what} query returned a non-finite sample ({v}): `{query}`. Prometheus reports \
             NaN and ±Inf for undefined arithmetic — a division by an empty pod count, a \
             quantile over no observations — and neither is a load reading"
        ),
        None => anyhow::bail!(
            "the {what} query matched no series: `{query}`. An empty result is not zero \
             load — check the metric name and that the scrape attaches the labels the \
             selector uses (Prometheus Operator needs `targetLabels` for Service labels)"
        ),
    }
}

/// PromQL queries the reconciler runs each cycle. Defaults pull the
/// metrics emitted by [`siglake-ingest`], [`siglake-compactor`], and
/// [`siglake-query-server`] and reduce to one scalar that is already
/// per-pod, because that is what `spec.autoscaling.*.target` is compared
/// against.
///
/// PER-POD MEANS PER POD, NOT PER SERIES. `siglake_ingest_requests_total`
/// carries `endpoint`, `status`, `tenant` and `index`, so one ingester
/// publishes as many series as it has live label combinations. A bare
/// `avg(rate(...))` averages those series, which divides a pod's real
/// request rate by its label cardinality: the same traffic reads lower the
/// moment it spreads over logs and traces, a second index or a second
/// tenant, and the ingester tier scales *in* as its mix widens. The fix is
/// to sum within each pod first and average the pod totals, so a pod is one
/// reading however many series it publishes.
///
/// `siglake_compactor_sealed_pending` likewise carries `tenant`. Its per-pod
/// reading is the sum of those tenant queue depths, not their average, before
/// the replica totals are averaged across the fleet.
///
/// THAT AVERAGE IS ALSO WHAT DEDUPLICATES A SHARED QUEUE. Under the catalog
/// claim, `peek_pending` counts the whole sealed table with no worker filter,
/// so every compactor replica publishes the same total and the reading is that
/// total, not the fleet's sum of copies: `avg` over identical pod totals is the
/// total itself. Two workers reporting a backlog of 8 read 8 here, where
/// `sum` would read 16 and a second replica would appear to double the work.
/// [`crate::scaling::Load::Shared`] is the other half: the decision divides
/// this reading by `target` once and does not multiply it by the replica count
/// again (#3692). The filesystem drain caps at one replica, where a fleet of
/// one makes the average and the sum the same number.
///
/// `pod` is the grouping label because Prometheus Operator attaches it to
/// every ServiceMonitor target; the chart's `targetLabels` add only
/// `instance` and the component. If it ever went missing, `sum by (pod)`
/// would collapse the fleet into one group and the reading would be the
/// fleet total rather than a per-pod rate — high, not silently zero.
///
/// A selector that matches nothing still yields an empty vector through both
/// aggregations, which [`usable_sample`] refuses rather than reading as an
/// idle fleet.
#[derive(Clone, Debug)]
pub struct Queries {
    pub ingester_rps: String,
    pub compactor_backlog: String,
    pub query_in_flight: String,
}

/// How stale an ingester's catalog-depth sample may be and still count as a
/// reading, in seconds.
///
/// The gauge keeps answering with its last value for the whole scrape
/// staleness window after its publisher stops refreshing it, so the reader
/// needs the companion age series to tell a current depth from a frozen one.
/// Two publish intervals plus two scrapes is 60 s; the allowance is double
/// that, so an ingester that misses a read or two still counts.
pub const SEALED_SAMPLE_MAX_AGE_SECS: u64 = 120;

impl Queries {
    /// The queries for a cluster, given its compactor policy.
    ///
    /// A zero-floor compactor reads the depth the INGESTERS publish, because
    /// the tier it sizes may be stopped and `siglake_compactor_sealed_pending`
    /// is published by that tier. Every other policy keeps the compactor's own
    /// gauge, so no existing cluster's sizing moves and an upgrade that has not
    /// yet rolled its ingesters cannot lose its compactor signal.
    pub fn for_policy(
        release: &str,
        namespace: &str,
        compactor: &crate::crd::ComponentAutoscale,
    ) -> Self {
        let mut queries = Self::defaults(release, namespace);
        if compactor.min == 0 {
            queries.compactor_backlog = Self::compactor_activation(release, namespace);
        }
        queries
    }

    /// The activation reading for a zero-floor compactor: the ingester-published
    /// catalog depth, falling back to the compactor's own gauge.
    ///
    /// `and on (pod)` drops any ingester whose last successful catalog read is
    /// older than [`SEALED_SAMPLE_MAX_AGE_SECS`]; the depth is one shared
    /// queue that every ingester publishes in full, so `avg` over the pods
    /// that remain reads it once ([`crate::scaling::Load::Shared`]).
    ///
    /// THE `or` IS THE FAIL-SAFE'S OTHER HALF. With no usable reading a
    /// zero-floor tier is restored to one replica rather than held at zero
    /// ([`crate::scaling::replicas_without_signal`]), and that pod publishes
    /// `siglake_compactor_sealed_pending` itself. Without the fallback the
    /// operator would not be reading the series the pod it just started
    /// publishes, so a broken depth publisher would pin the tier at exactly
    /// one worker however deep the queue got. `or` returns the right side only
    /// when the left is an empty vector, so while the ingesters are publishing
    /// a fresh depth nothing else is consulted (#6011; the design's query
    /// selection and its fail-safe paragraph disagreed on this point, and the
    /// expression is where it is settled).
    pub fn compactor_activation(release: &str, namespace: &str) -> String {
        let ingester = format!(
            "namespace=\"{namespace}\",app_kubernetes_io_instance=\"{release}\",app_kubernetes_io_component=\"ingester\""
        );
        let compactor = format!(
            "namespace=\"{namespace}\",app_kubernetes_io_instance=\"{release}\",app_kubernetes_io_component=\"compactor\""
        );
        format!(
            "avg(sum by (pod) (siglake_wal_segments_sealed{{{ingester}}}) and on (pod) \
             (max by (pod) (siglake_wal_segments_sealed_sample_age_seconds{{{ingester}}}) <= \
             {SEALED_SAMPLE_MAX_AGE_SECS})) \
             or avg(sum by (pod) (siglake_compactor_sealed_pending{{{compactor}}}))"
        )
    }

    /// Defaults for a deployment where the chart's labels apply —
    /// matches by namespace, release, and component selectors.
    pub fn defaults(release: &str, namespace: &str) -> Self {
        Self {
            ingester_rps: format!(
                "avg(sum by (pod) (rate(siglake_ingest_requests_total{{namespace=\"{namespace}\",app_kubernetes_io_instance=\"{release}\",app_kubernetes_io_component=\"ingester\"}}[1m])))"
            ),
            compactor_backlog: format!(
                "avg(sum by (pod) (siglake_compactor_sealed_pending{{namespace=\"{namespace}\",app_kubernetes_io_instance=\"{release}\",app_kubernetes_io_component=\"compactor\"}}))"
            ),
            query_in_flight: format!(
                "avg(siglake_query_in_flight{{namespace=\"{namespace}\",app_kubernetes_io_instance=\"{release}\",app_kubernetes_io_component=\"query\"}})"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{usable_sample, PromClient, Queries, SEALED_SAMPLE_MAX_AGE_SECS};

    /// A stand-in Prometheus that answers each query with a canned body,
    /// chosen by the `query=` parameter in the request line. Returns the base
    /// URL to point a [`PromClient`] at.
    ///
    /// The per-signal split is about what happens to the OTHER two readings
    /// when one query fails, so the fixture has to fail exactly one of three
    /// live HTTP requests — a client pointed at a dead host fails all three
    /// and cannot tell the new behaviour from the old.
    async fn canned_prometheus(answers: Vec<(&str, String)>) -> String {
        let answers: Vec<(String, String)> = answers
            .into_iter()
            .map(|(name, body)| (name.to_string(), body))
            .collect();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let answers = answers.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = vec![0u8; 4096];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]).to_string();
                    let body = answers
                        .iter()
                        .find(|(name, _)| request.contains(&format!("query={name}")))
                        .map(|(_, body)| body.clone());
                    let response = match body {
                        Some(body) => format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        ),
                        // Every query this fixture was not given fails the way
                        // a Prometheus that is up but cannot serve does.
                        None => "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_string(),
                    };
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        format!("http://{addr}")
    }

    fn scalar(value: &str) -> String {
        format!(
            r#"{{"status":"success","data":{{"resultType":"vector","result":[{{"metric":{{}},"value":[1700000000,"{value}"]}}]}}}}"#
        )
    }

    fn named_queries() -> Queries {
        Queries {
            ingester_rps: "ing".into(),
            compactor_backlog: "cmp".into(),
            query_in_flight: "qry".into(),
        }
    }

    /// #6011 slice 1. One unusable reading is absent for ITS OWN signal and
    /// leaves the other two as readings. Before this, `observed` returned the
    /// first error and the reconciler held all three tiers.
    #[tokio::test]
    async fn one_failing_query_leaves_the_other_two_readings_intact() {
        // No answer for `cmp`: that request gets the 500.
        let base = canned_prometheus(vec![("ing", scalar("42.5")), ("qry", scalar("3"))]).await;
        let client = PromClient::new(base).expect("client");

        let samples = client.observed(&named_queries(), "logs").await;
        assert_eq!(samples.ingester_rps_per_pod, Some(42.5));
        assert_eq!(
            samples.compactor_backlog, None,
            "the failing query is absent for the compactor alone"
        );
        assert_eq!(samples.query_in_flight_per_pod, Some(3.0));
        assert!(!samples.all_absent(), "this is not a whole outage");
    }

    /// An empty result vector and a non-finite sample are unusable for their
    /// own signal too — the checks `usable_sample` already made, now applied
    /// per query rather than to the snapshot.
    #[tokio::test]
    async fn an_empty_vector_and_a_nan_are_absent_only_for_their_own_signal() {
        let empty = r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#;
        let base = canned_prometheus(vec![
            ("ing", empty.to_string()),
            ("cmp", scalar("NaN")),
            ("qry", scalar("7")),
        ])
        .await;
        let client = PromClient::new(base).expect("client");

        let samples = client.observed(&named_queries(), "logs").await;
        assert_eq!(samples.ingester_rps_per_pod, None, "an empty vector");
        assert_eq!(samples.compactor_backlog, None, "a NaN sample");
        assert_eq!(samples.query_in_flight_per_pod, Some(7.0));
    }

    /// Every query failing reproduces the outage reading exactly: all three
    /// absent, which the reconciler still turns into "hold every tier".
    #[tokio::test]
    async fn a_prometheus_that_answers_nothing_is_still_a_whole_outage() {
        let base = canned_prometheus(vec![]).await;
        let client = PromClient::new(base).expect("client");
        assert!(client.observed(&named_queries(), "logs").await.all_absent());
    }

    /// The sample strings Prometheus actually writes for undefined arithmetic
    /// parse to non-finite floats, so `usable_sample`'s rejection is on the
    /// live path, not a defensive branch: `instant_value` pulls the value out
    /// of the JSON array with the same `f64` parse.
    #[test]
    fn prometheus_non_finite_sample_strings_parse_to_non_finite_floats() {
        for text in ["NaN", "+Inf", "-Inf"] {
            let parsed = text
                .parse::<f64>()
                .unwrap_or_else(|e| panic!("`{text}` parses as f64: {e}"));
            assert!(!parsed.is_finite(), "`{text}` parsed to {parsed}");
            let err = usable_sample(Some(parsed), "avg(x)", "ingester")
                .expect_err("a non-finite sample is rejected")
                .to_string();
            assert!(err.contains("non-finite"), "got: {err}");
            assert!(err.contains("ingester"), "names the signal: {err}");
        }
    }

    #[test]
    fn empty_result_and_finite_samples_keep_their_existing_handling() {
        let err = usable_sample(None, "avg(x)", "compactor")
            .expect_err("an empty result vector is an error, not a zero")
            .to_string();
        assert!(err.contains("matched no series"), "got: {err}");

        // A legitimate zero is a reading, not a failure: idle load has to reach
        // the scaling decision for scale-to-zero to work.
        assert_eq!(usable_sample(Some(0.0), "avg(x)", "query").unwrap(), 0.0);
        assert_eq!(usable_sample(Some(12.5), "avg(x)", "query").unwrap(), 12.5);
    }

    #[test]
    fn default_queries_include_the_reconciled_namespace() {
        let queries = Queries::defaults("siglake", "tenant-a");

        assert_eq!(
            queries.ingester_rps,
            "avg(sum by (pod) (rate(siglake_ingest_requests_total{namespace=\"tenant-a\",app_kubernetes_io_instance=\"siglake\",app_kubernetes_io_component=\"ingester\"}[1m])))"
        );
        assert_eq!(
            queries.compactor_backlog,
            "avg(sum by (pod) (siglake_compactor_sealed_pending{namespace=\"tenant-a\",app_kubernetes_io_instance=\"siglake\",app_kubernetes_io_component=\"compactor\"}))"
        );
        assert_eq!(
            queries.query_in_flight,
            "avg(siglake_query_in_flight{namespace=\"tenant-a\",app_kubernetes_io_instance=\"siglake\",app_kubernetes_io_component=\"query\"})"
        );
    }

    /// Only a zero floor moves the compactor query. Every positive-floor
    /// policy keeps `siglake_compactor_sealed_pending`, so no existing
    /// cluster's sizing changes and an upgrade that has not yet rolled its
    /// ingesters cannot lose its compactor signal.
    #[test]
    fn only_a_zero_floor_selects_the_activation_query() {
        let policy = |min: i32| crate::crd::ComponentAutoscale {
            min,
            max: 4,
            target: 4.0,
        };
        let defaults = Queries::defaults("siglake", "logs");

        for min in [1, 2] {
            let queries = Queries::for_policy("siglake", "logs", &policy(min));
            assert_eq!(queries.compactor_backlog, defaults.compactor_backlog);
        }

        let parked = Queries::for_policy("siglake", "logs", &policy(0));
        assert_eq!(
            parked.compactor_backlog,
            Queries::compactor_activation("siglake", "logs")
        );
        assert!(
            parked
                .compactor_backlog
                .contains("siglake_wal_segments_sealed"),
            "the zero-floor reading is the ingester-published depth: {}",
            parked.compactor_backlog
        );
        assert!(
            parked
                .compactor_backlog
                .contains(&format!("<= {SEALED_SAMPLE_MAX_AGE_SECS}")),
            "with the staleness guard: {}",
            parked.compactor_backlog
        );
        assert!(
            parked
                .compactor_backlog
                .contains("or avg(sum by (pod) (siglake_compactor_sealed_pending"),
            "and the fail-safe fallback: {}",
            parked.compactor_backlog
        );
        // The other two signals are untouched by the compactor's policy.
        assert_eq!(parked.ingester_rps, defaults.ingester_rps);
        assert_eq!(parked.query_in_flight, defaults.query_in_flight);
    }

    /// The activation expression, written out for one release and namespace.
    ///
    /// `scripts/check-kind-compactor-wakeup.sh` lifts this literal out of the
    /// source and compares it to the string the kind capture evaluates, so the
    /// round cannot retain evidence for a query the reconciler does not run.
    /// Changing `compactor_activation` means changing this line, and then the
    /// capture's fixture — which is the point.
    #[test]
    fn the_activation_expression_is_the_one_the_kind_capture_evaluates() {
        assert_eq!(
            Queries::compactor_activation("siglake", "default"),
            "avg(sum by (pod) (siglake_wal_segments_sealed{namespace=\"default\",app_kubernetes_io_instance=\"siglake\",app_kubernetes_io_component=\"ingester\"}) and on (pod) (max by (pod) (siglake_wal_segments_sealed_sample_age_seconds{namespace=\"default\",app_kubernetes_io_instance=\"siglake\",app_kubernetes_io_component=\"ingester\"}) <= 120)) or avg(sum by (pod) (siglake_compactor_sealed_pending{namespace=\"default\",app_kubernetes_io_instance=\"siglake\",app_kubernetes_io_component=\"compactor\"}))"
        );
    }

    #[test]
    fn same_release_name_in_different_namespaces_has_disjoint_queries() {
        let tenant_a = Queries::defaults("siglake", "tenant-a");
        let tenant_b = Queries::defaults("siglake", "tenant-b");

        for (a, b) in [
            (&tenant_a.ingester_rps, &tenant_b.ingester_rps),
            (&tenant_a.compactor_backlog, &tenant_b.compactor_backlog),
            (&tenant_a.query_in_flight, &tenant_b.query_in_flight),
        ] {
            assert!(a.contains("namespace=\"tenant-a\""));
            assert!(b.contains("namespace=\"tenant-b\""));
            assert_ne!(a, b);
        }
    }
}

#[derive(Debug, Deserialize)]
struct PromResponse {
    status: String,
    data: PromData,
}

#[derive(Debug, Deserialize)]
struct PromData {
    #[serde(default)]
    result: Vec<PromResult>,
}

#[derive(Debug, Deserialize)]
struct PromResult {
    /// `[timestamp, "value"]`.
    value: Vec<serde_json::Value>,
}
