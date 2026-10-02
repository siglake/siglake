//! `siglake-operator` binary entry point.
//!
//! Watches `SiglakeCluster` CustomResources and scales the managed
//! Deployments based on the per-component load signals each binary
//! exports to Prometheus (`siglake_ingest_requests_total[1m]`,
//! `siglake_compactor_sealed_pending`, `siglake_query_in_flight`).
//!
//! v0 renders ingester and compactor Deployments plus the query StatefulSet,
//! reconciles on spec change, runs the schema-migration Job and scales those
//! workloads from Prometheus signals. The Helm chart remains the supported
//! install surface — see `docs/ARCHITECTURE.md` (Deployment) for what the
//! operator deliberately does not render.
//! Tenancy is header-based (`X-Scope-OrgID`),
//! so there's no per-tenant Secret to manage; per-tenant namespace
//! provisioning + multi-pod compactor orchestration is the next-step
//! extension once the scaffold has seen real traffic.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use clap::Parser;
use futures::stream::StreamExt;
use kube::{
    runtime::{controller::Controller, watcher},
    Api, Client,
};

use siglake_operator::{leader, prom, reconciler, SiglakeCluster};

#[derive(Parser, Debug)]
#[command(
    name = "siglake-operator",
    about = "Kubernetes operator for SiglakeCluster",
    version = siglake_core::BUILD_VERSION
)]
struct Cli {
    /// Prometheus base URL the reconciler queries for load signals.
    /// Defaults to the `prometheus-server` Service installed by the
    /// prometheus-community/prometheus chart:
    /// `http://prometheus-server.monitoring.svc.cluster.local:80`.
    /// kube-prometheus-stack uses a different Service; override this value.
    #[arg(
        long,
        env = "SIGLAKE_PROMETHEUS_URL",
        default_value = "http://prometheus-server.monitoring.svc.cluster.local:80"
    )]
    prometheus_url: String,

    /// Restrict the watch to one or more namespaces. Repeatable.
    /// When empty, the operator watches every namespace.
    #[arg(long = "watch-namespace", value_name = "NS")]
    watch_namespaces: Vec<String>,

    /// Namespace where the leader-election Lease lives. Defaults
    /// to the pod's namespace via `$POD_NAMESPACE`, falling back
    /// to `siglake-system`.
    #[arg(long, env = "POD_NAMESPACE", default_value = "siglake-system")]
    lease_namespace: String,

    /// Unique identity for this replica (typically the pod name).
    /// Reads `$POD_NAME` so the Helm Deployment pods carry their
    /// own names via the downward API.
    #[arg(long, env = "POD_NAME")]
    pod_name: Option<String>,

    /// Disable leader election. Safe for single-replica
    /// deployments; required to be off for `replicas > 1`.
    #[arg(long, default_value_t = false)]
    no_leader_election: bool,

    /// Bind address for the operator's own `/metrics` endpoint.
    /// Set to empty string to disable.
    #[arg(long, env = "SIGLAKE_METRICS_BIND", default_value = "0.0.0.0:9190")]
    metrics_bind: String,

    /// Print the rendered CRD YAML and exit. Used by the install
    /// flow: `siglake-operator --print-crd | kubectl apply -f -`.
    #[arg(long, default_value_t = false)]
    print_crd: bool,

    /// Helm-release adoption preflight (offline): synthesize a
    /// SiglakeCluster from the given chart VALUES file, run the parity
    /// checks, print the CR + handover runbook, and exit. See
    /// docs/DESIGN_operator_adoption.md.
    #[arg(long = "adopt-values", value_name = "VALUES_YAML")]
    adopt_values: Option<std::path::PathBuf>,

    /// CR / release name for adoption (must equal the helm release name
    /// for name+selector parity). Default "siglake".
    #[arg(long = "adopt-cluster-name", default_value = "siglake")]
    adopt_cluster_name: String,

    /// Namespace the helm release runs in. Lands on the synthesized
    /// resource's `metadata.namespace` and in every runbook command.
    /// Defaults to the release name, which is what the runbook assumed
    /// before this flag existed.
    #[arg(long = "adopt-namespace", value_name = "NS")]
    adopt_namespace: Option<String>,

    /// Catalog URI for adoption (cannot be inferred from chart values —
    /// the chart wires Postgres through a Secret).
    #[arg(long = "adopt-catalog-uri")]
    adopt_catalog_uri: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // `run` owns every exit from this process, so the OTel flush happens once,
    // here, on the reconcile loop's graceful stop and on every error return
    // alike. The providers live in a `OnceLock` that never drops, so nothing
    // else would flush them. No-op when OTel is off.
    let result = run().await;
    siglake_core::telemetry::shutdown();
    result
}

async fn run() -> Result<()> {
    // kube-rs's reqwest dependency uses rustls, which requires an
    // installed CryptoProvider before any TLS connection. The default
    // we use across the workspace is `aws-lc-rs` (already pulled in
    // transitively via sqlx). Without this call, the first kube API
    // request panics: "Could not automatically determine the
    // process-level CryptoProvider from Rustls crate features".
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Logs + traces via OTel (opt-in via OTEL_EXPORTER_OTLP_ENDPOINT). The fmt
    // console layer stays on stderr, matching the rest of the workspace's
    // binaries, so the `--print-crd` and `--adopt-values` reports below stay
    // pipe-clean on stdout (ci-local.sh diffs the CRD against the checked-in
    // copies). The container runtime captures both streams, so the controller
    // loses nothing.
    siglake_core::telemetry::init(siglake_core::telemetry::TelemetryConfig::from_env(
        "operator",
    ))?;

    let cli = Cli::parse();

    if cli.print_crd {
        return print_crd_yaml();
    }

    if let Some(values_path) = cli.adopt_values.as_ref() {
        let raw = std::fs::read_to_string(values_path)
            .with_context(|| format!("read {}", values_path.display()))?;
        let values: serde_yaml::Value = serde_yaml::from_str(&raw).context("parse values yaml")?;
        let namespace = siglake_operator::adopt::adoption_namespace_from(
            cli.adopt_namespace.as_deref(),
            &cli.adopt_cluster_name,
        );
        let report = siglake_operator::adopt::synthesize_from_values(
            &values,
            &cli.adopt_cluster_name,
            namespace,
            cli.adopt_catalog_uri.as_deref(),
        )?;
        // One YAML document, comments and all: the runbook's own step 5
        // applies this file.
        print!("{}", report.to_yaml()?);
        return Ok(());
    }

    tracing::info!("siglake-operator starting");

    // Bring up the `/metrics` endpoint up front so even the
    // leader-election wait window is scrapeable.
    if !cli.metrics_bind.is_empty() {
        let bind: std::net::SocketAddr = cli
            .metrics_bind
            .parse()
            .with_context(|| format!("parse --metrics-bind {}", cli.metrics_bind))?;
        let _h = siglake_core::metrics::init(bind)
            .await
            .context("metrics server")?;
        let build = siglake_core::build_info();
        metrics::gauge!(
            "siglake_build_info",
            "version" => build.version,
            "commit" => build.commit
        )
        .set(1.0);
        tracing::info!(addr = %bind, "operator /metrics listening");
    }

    let client = Client::try_default().await.context("kube client")?;

    // Acquire the leader-election lease before doing any work.
    // Single-replica deployments can skip via --no-leader-election.
    if !cli.no_leader_election {
        let holder = cli
            .pod_name
            .clone()
            .unwrap_or_else(|| format!("siglake-operator-{}", std::process::id()));
        let election = leader::Election::new(
            client.clone(),
            &cli.lease_namespace,
            "siglake-operator-leader",
            holder,
            Duration::from_secs(30),
        );
        tracing::info!(ns = %cli.lease_namespace, "acquiring leader lease");
        election.acquire().await?;
        let renew = election.clone();
        tokio::spawn(async move {
            if let Err(e) = renew.renew_loop().await {
                tracing::error!(error = %e, "lease renew loop ended; exiting");
                std::process::exit(1);
            }
        });
    }

    let prom = prom::PromClient::new(cli.prometheus_url.clone()).context("prometheus client")?;
    let ctx = Arc::new(reconciler::Context {
        client: client.clone(),
        prom,
        scaling_state: Default::default(),
        zero_floor_state: Default::default(),
    });

    if cli.watch_namespaces.is_empty() {
        let clusters: Api<SiglakeCluster> = Api::all(client.clone());
        Controller::new(clusters, watcher::Config::default())
            .shutdown_on_signal()
            .run(reconciler::reconcile, reconciler::error_policy, ctx)
            .for_each(|res| async move {
                match res {
                    Ok((obj, _action)) => tracing::debug!(?obj, "reconciled"),
                    Err(e) => tracing::warn!(error = %e, "reconcile error"),
                }
            })
            .await;
    } else {
        // One Controller per watched namespace. Each runs to its own
        // shutdown signal in a select_all; the process exits when any
        // controller terminates (Ctrl-C / SIGTERM propagates).
        let mut handles = Vec::new();
        for ns in cli.watch_namespaces {
            tracing::info!(namespace = %ns, "watching namespace");
            let clusters: Api<SiglakeCluster> = Api::namespaced(client.clone(), &ns);
            let ctx = ctx.clone();
            let h = tokio::spawn(async move {
                Controller::new(clusters, watcher::Config::default())
                    .shutdown_on_signal()
                    .run(reconciler::reconcile, reconciler::error_policy, ctx)
                    .for_each(|res| async move {
                        match res {
                            Ok((obj, _action)) => tracing::debug!(?obj, "reconciled"),
                            Err(e) => tracing::warn!(error = %e, "reconcile error"),
                        }
                    })
                    .await;
            });
            handles.push(h);
        }
        for h in handles {
            let _ = h.await;
        }
    }

    tracing::info!("siglake-operator stopping");
    // `main` flushes the OTel providers once this returns.
    Ok(())
}

fn print_crd_yaml() -> Result<()> {
    use kube::CustomResourceExt;
    let crd_doc = SiglakeCluster::crd();
    let y = serde_yaml::to_string(&crd_doc).context("serialize CRD to YAML")?;
    println!("{y}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::{CommandFactory, Parser};

    #[test]
    fn cli_version_contains_the_embedded_provenance() {
        assert_eq!(
            Cli::command().get_version(),
            Some(siglake_core::BUILD_VERSION)
        );
    }

    /// The help text is the source for the generated CLI reference, so every
    /// `siglake-operator …` example in it has to be an invocation the binary
    /// accepts. There are no subcommands: a bare word after the program name
    /// (`siglake-operator print-crd`) is refused by clap.
    #[test]
    fn help_examples_name_flags_not_subcommands() {
        let cmd = Cli::command();
        assert!(
            cmd.get_subcommands().next().is_none(),
            "a subcommand would change which help examples are valid"
        );
        for arg in cmd.get_arguments() {
            let help = arg
                .get_long_help()
                .or_else(|| arg.get_help())
                .map(|h| h.to_string())
                .unwrap_or_default();
            for tail in help.split("siglake-operator ").skip(1) {
                let word = tail.split_whitespace().next().unwrap_or_default();
                assert!(
                    word.starts_with('-'),
                    "help for `{}` shows `siglake-operator {word}`, which clap refuses",
                    arg.get_id()
                );
            }
        }
        assert!(Cli::try_parse_from(["siglake-operator", "print-crd"]).is_err());
    }

    /// The adoption namespace is optional on the command line and resolves to
    /// the release name when it is absent — the `-n {name}` every runbook step
    /// assumed before the flag existed.
    #[test]
    fn the_adoption_namespace_is_optional_and_falls_back_to_the_release_name() {
        use siglake_operator::adopt::adoption_namespace_from;

        let cli = Cli::try_parse_from([
            "siglake-operator",
            "--adopt-values",
            "values.yaml",
            "--adopt-cluster-name",
            "acme",
        ])
        .unwrap();
        assert_eq!(cli.adopt_namespace, None);
        assert_eq!(
            adoption_namespace_from(cli.adopt_namespace.as_deref(), &cli.adopt_cluster_name),
            "acme"
        );

        let cli = Cli::try_parse_from([
            "siglake-operator",
            "--adopt-values",
            "values.yaml",
            "--adopt-cluster-name",
            "acme",
            "--adopt-namespace",
            "obs-prod",
        ])
        .unwrap();
        assert_eq!(
            adoption_namespace_from(cli.adopt_namespace.as_deref(), &cli.adopt_cluster_name),
            "obs-prod"
        );
    }
}
