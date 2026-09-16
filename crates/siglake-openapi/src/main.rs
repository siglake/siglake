//! Emit the committed OpenAPI 3.1 specs for siglake's two HTTP servers.
//!
//! siglake runs two independent axum servers (ingest on one port, query on
//! another) with disjoint path sets and different auth token pools, so they get
//! **two** documents rather than one merged file with two `servers` entries —
//! a merged doc would falsely assert every path is reachable on both base URLs,
//! and `/healthz` exists on both with different bodies.
//!
//! There is deliberately no runtime `/openapi.json` endpoint; the specs are
//! generated from the same `#[utoipa::path]` annotations that register the axum
//! routes and committed under `docs/api/`. CI regenerates and `git diff`s them,
//! so an endpoint cannot ship undocumented. See `docs/api/README.md`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;

/// One spec document: the file it lands in and how to build it.
struct Spec {
    file: &'static str,
    build: fn() -> utoipa::openapi::OpenApi,
}

const SPECS: &[Spec] = &[
    Spec {
        file: "openapi-ingest.yaml",
        build: siglake_ingest::openapi,
    },
    Spec {
        file: "openapi-query.yaml",
        build: siglake_query_server::openapi,
    },
];

#[derive(Parser, Debug)]
#[command(
    name = "siglake-openapi",
    about = "Emit the committed OpenAPI 3.1 specs for the siglake HTTP servers."
)]
struct Cli {
    /// Directory to write the spec files into.
    #[arg(long, default_value = "docs/api")]
    out: PathBuf,

    /// Do not write; instead fail if any on-disk spec differs from what would
    /// be generated. Handy locally — CI uses the `git diff` form so a failure
    /// prints the actual drift.
    #[arg(long)]
    check: bool,
}

fn render(spec: &Spec) -> Result<String> {
    serde_yaml::to_string(&(spec.build)()).context("serialize OpenAPI document to YAML")
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.check {
        let mut stale = Vec::new();
        for spec in SPECS {
            let path = cli.out.join(spec.file);
            let generated = render(spec)?;
            let on_disk = std::fs::read_to_string(&path).unwrap_or_default();
            if on_disk != generated {
                stale.push(path);
            }
        }
        if !stale.is_empty() {
            anyhow::bail!(
                "OpenAPI specs are stale: {}\nRegenerate with: cargo run -p siglake-openapi -- --out {}",
                stale
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                cli.out.display(),
            );
        }
        eprintln!("OpenAPI specs are up to date.");
        return Ok(());
    }

    std::fs::create_dir_all(&cli.out).with_context(|| format!("create {}", cli.out.display()))?;
    for spec in SPECS {
        let path = cli.out.join(spec.file);
        write_if_changed(&path, &render(spec)?)?;
    }
    Ok(())
}

/// Write only when the content differs, so re-running does not needlessly bump
/// mtimes (keeps the CI `git diff` gate quiet on a no-op run).
fn write_if_changed(path: &Path, content: &str) -> Result<()> {
    if std::fs::read_to_string(path).ok().as_deref() == Some(content) {
        eprintln!("unchanged: {}", path.display());
        return Ok(());
    }
    std::fs::write(path, content).with_context(|| format!("write {}", path.display()))?;
    eprintln!("wrote:     {}", path.display());
    Ok(())
}
