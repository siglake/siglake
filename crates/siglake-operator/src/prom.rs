//! Tiny Prometheus instant-query client.
//!
//! The operator polls four queries per cycle (per-component load
//! signals) and folds the results into [`ObservedMetrics`]. We don't
//! pull in `prometheus-http-query` for this — a single GET on
//! `/api/v1/query` is enough and keeps the operator binary small.

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::scaling::ObservedMetrics;

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
    /// Returning Err puts all of these on the same path as a Prometheus outage,
    /// where the reconciler holds the current replica counts and leaves the
    /// smoothing history untouched.
    pub async fn observed(&self, queries: &Queries) -> Result<ObservedMetrics> {
        async fn required(this: &PromClient, q: &str, what: &str) -> Result<f64> {
            let value = this
                .instant_value(q)
                .await
                .with_context(|| format!("query `{q}`"))?;
            usable_sample(value, q, what)
        }
        let ing = required(self, &queries.ingester_rps, "ingester").await?;
        let cmp = required(self, &queries.compactor_backlog, "compactor").await?;
        let qry = required(self, &queries.query_in_flight, "query").await?;
        Ok(ObservedMetrics {
            ingester_rps_per_pod: ing,
            compactor_backlog: cmp,
            query_in_flight_per_pod: qry,
        })
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

impl Queries {
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
    use super::{usable_sample, Queries};

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
