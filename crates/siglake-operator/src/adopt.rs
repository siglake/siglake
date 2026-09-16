//! Helm-release adoption (design: `docs/DESIGN_operator_adoption.md`).
//!
//! v1 scope — the OFFLINE half of annotate-then-inherit: synthesize a
//! `SiglakeCluster` from a chart values file, run the naming/selector/kind
//! parity preflight, and print the manual handover runbook. The commit point
//! (annotation flip + helm release-secret delete + CR apply) stays a HUMAN
//! step in v1; nothing here talks to a cluster.
//!
//! Parity facts this leans on (verified 2026-07-22):
//! - Chart object names are `{release}-{role}`; the operator renders
//!   `{cr-name}-{component}` — naming the CR after the release makes every
//!   name identical.
//! - Both sides select on `app.kubernetes.io/{name,instance,component}`,
//!   with instance = release/CR name — identical selectors under the same
//!   name, so adopted workloads never hit the immutable-selector wall.
//! - Workload kinds already match the chart for the core tiers (ingester +
//!   compactor Deployments, query StatefulSet).

use anyhow::{Context, Result};

use crate::crd::{ExtraEnvVar, SiglakeCluster, SiglakeClusterSpec, TierResources};

/// Everything the offline adoption step produces.
pub struct AdoptionReport {
    pub cluster: SiglakeCluster,
    /// Divergences and required inputs the operator cannot infer from
    /// values alone. Non-empty ⇒ resolve before the handover.
    pub findings: Vec<String>,
    pub runbook: String,
}

impl AdoptionReport {
    /// The complete `--adopt-values` output, as ONE YAML document: the
    /// synthesized resource, then the preflight findings and the handover
    /// runbook as comments.
    ///
    /// The runbook's own step 5 is `kubectl apply -f cluster.yaml`, so the
    /// file the user saves has to still parse after the resource ends. That
    /// is the whole contract here — nothing below the resource may be
    /// anything but a comment, and a finding carrying a newline would break
    /// it, so they are flattened to one line each.
    pub fn to_yaml(&self) -> Result<String> {
        let mut out = String::new();
        out.push_str("# --- synthesized SiglakeCluster: this whole output is one manifest ---\n");
        out.push_str("# Save it (cluster.yaml) and apply it at step 5; every line below the\n");
        out.push_str("# resource is a comment, so `kubectl apply -f` reads only the resource.\n");
        out.push_str(&serde_yaml::to_string(&self.cluster).context("serialize SiglakeCluster")?);
        if self.findings.is_empty() {
            out.push_str("# preflight: no findings — proceed to the runbook\n");
        } else {
            out.push_str("# preflight FINDINGS (resolve before handover):\n");
            for finding in &self.findings {
                let flat = finding.split_whitespace().collect::<Vec<_>>().join(" ");
                out.push_str(&format!("#  - {flat}\n"));
            }
        }
        out.push_str(&self.runbook);
        out.push('\n');
        Ok(out)
    }
}

/// The namespace the handover targets: `--adopt-namespace` when it carries
/// one, otherwise the release name. The fallback is what every runbook step
/// assumed before the flag existed (`-n {name}`), so a release whose
/// namespace matches its name keeps the output it had.
pub fn adoption_namespace_from<'a>(flag: Option<&'a str>, cluster_name: &'a str) -> &'a str {
    match flag.map(str::trim) {
        Some(ns) if !ns.is_empty() => ns,
        _ => cluster_name,
    }
}

fn yaml_str(v: &serde_yaml::Value, path: &[&str]) -> Option<String> {
    let mut cur = v;
    for key in path {
        cur = cur.get(key)?;
    }
    match cur {
        serde_yaml::Value::String(s) if !s.is_empty() => Some(s.clone()),
        serde_yaml::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn yaml_i64(v: &serde_yaml::Value, path: &[&str]) -> Option<i64> {
    let mut cur = v;
    for key in path {
        cur = cur.get(key)?;
    }
    cur.as_i64()
}

fn yaml_bool(v: &serde_yaml::Value, path: &[&str]) -> Option<bool> {
    let mut cur = v;
    for key in path {
        cur = cur.get(key)?;
    }
    cur.as_bool()
}

/// A chart list of non-empty strings (`ingester.allowedTenants`,
/// `<tier>.tokens.list`). Absent and empty are the same answer.
fn yaml_strings(v: &serde_yaml::Value, path: &[&str]) -> Vec<String> {
    let mut cur = v;
    for key in path {
        let Some(next) = cur.get(key) else {
            return Vec::new();
        };
        cur = next;
    }
    cur.as_sequence()
        .map(|items| {
            items
                .iter()
                .filter_map(|i| i.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// A chart `<tier>.resources.{requests,limits}` map as quantity strings.
/// Helm values write CPU counts as numbers (`cpu: 2`); the CR carries the
/// same value as the string Kubernetes parses it to (`"2"`).
fn quantity_map(
    values: &serde_yaml::Value,
    path: &[&str],
) -> std::collections::BTreeMap<String, String> {
    let mut cur = values;
    for key in path {
        let Some(next) = cur.get(key) else {
            return Default::default();
        };
        cur = next;
    }
    cur.as_mapping()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| {
                    let k = k.as_str()?.to_string();
                    let v = match v {
                        serde_yaml::Value::String(s) if !s.is_empty() => s.clone(),
                        serde_yaml::Value::Number(n) => n.to_string(),
                        _ => return None,
                    };
                    Some((k, v))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// `<tier>.resources` from the values file, or `None` when the release never
/// set one. The operator's packaged defaults ARE the chart's, so an absent
/// block adopts to the same pod; a present block is carried key for key
/// (`spec.resources.<tier>` merges over the defaults the way helm's values
/// merge does), so a release that raised the query memory limit keeps it.
fn tier_resources(values: &serde_yaml::Value, tier: &str) -> Option<TierResources> {
    let requests = quantity_map(values, &[tier, "resources", "requests"]);
    let limits = quantity_map(values, &[tier, "resources", "limits"]);
    if requests.is_empty() && limits.is_empty() {
        return None;
    }
    Some(TierResources { requests, limits })
}

fn tier_extra_env(values: &serde_yaml::Value, tier: &str, out: &mut Vec<ExtraEnvVar>) {
    let Some(list) = values
        .get(tier)
        .and_then(|t| t.get("extraEnv"))
        .and_then(|e| e.as_sequence())
    else {
        return;
    };
    for item in list {
        if let (Some(name), Some(value)) = (
            item.get("name").and_then(|n| n.as_str()),
            item.get("value").and_then(|n| n.as_str()),
        ) {
            if !out.iter().any(|e| e.name == name) {
                out.push(ExtraEnvVar {
                    name: name.to_string(),
                    value: value.to_string(),
                });
            }
        }
    }
}

/// A tier's OIDC block as the chart renders it: issuer and audience together
/// or nothing at all — both templates guard on `{{- if and .issuer .audience }}`,
/// so half a block produces no env var and costs the adoption nothing.
#[derive(PartialEq, Eq)]
struct TierOidc {
    issuer: String,
    audience: String,
    tenant_claim: Option<String>,
}

fn tier_oidc(values: &serde_yaml::Value, tier: &str) -> Option<TierOidc> {
    Some(TierOidc {
        issuer: yaml_str(values, &[tier, "oidc", "issuer"])?,
        audience: yaml_str(values, &[tier, "oidc", "audience"])?,
        tenant_claim: yaml_str(values, &[tier, "oidc", "tenantClaim"]),
    })
}

/// Authentication and tenant controls, which the CR cannot express at all.
///
/// The chart turns these into container env — `SIGLAKE_QUERY_TOKENS` and
/// `SIGLAKE_OIDC_*` on the query StatefulSet, `SIGLAKE_OIDC_*`,
/// `SIGLAKE_TRUST_SCOPE_HEADER`, `SIGLAKE_ALLOWED_TENANTS`,
/// `SIGLAKE_MAX_TENANTS` and `SIGLAKE_INGEST_MAX_LANES` on the ingester
/// Deployment — and `render.rs` emits none of them. Dropped without a word,
/// a handover that reads clean leaves the query API answering anyone who can
/// reach the Service, unbinds the tenant from the verified token, and removes
/// the caps that bound what an unauthenticated client header can mint.
///
/// Every finding names the tier, what stops holding at cutover, and a
/// disposition that keeps the token bytes out of the CR: `spec.extraEnv` is
/// stored in plaintext, so it carries issuers, claims and caps but never a
/// credential.
fn auth_and_tenancy_findings(
    values: &serde_yaml::Value,
    cluster_name: &str,
    findings: &mut Vec<String>,
) {
    // Query bearer tokens. Either source renders SIGLAKE_QUERY_TOKENS from a
    // secretKeyRef; with neither, the query server logs its open-auth warning
    // and answers every caller — which is exactly what the adopted pod does.
    let query_key =
        yaml_str(values, &["query", "tokens", "secretKey"]).unwrap_or_else(|| "tokens".to_string());
    let query_tokens = match (
        yaml_str(values, &["query", "tokens", "existingSecret"]),
        yaml_strings(values, &["query", "tokens", "list"]).len(),
    ) {
        (Some(secret), _) => Some(format!(
            "query.tokens.existingSecret is {secret:?} (key {query_key:?})"
        )),
        (None, 0) => None,
        (None, n) => Some(format!(
            "query.tokens.list is set ({n} entries), which the chart renders into Secret {:?}",
            format!("{cluster_name}-query-tokens")
        )),
    };
    if let Some(source) = query_tokens {
        findings.push(format!(
            "{source}, but the CR has no field for query bearer tokens — the adopted QUERY tier \
             renders no SIGLAKE_QUERY_TOKENS and starts with auth OPEN, answering /api/v1/sql for \
             anything that can reach the Service. spec.extraEnv is stored in the CR in PLAINTEXT \
             and must not carry the tokens: keep the query tier on the chart until the CR can \
             reference a token Secret."
        ));
    }

    // Ingest tokens defined inline. `existingSecret` is mapped onto
    // spec.authTokensSecretRef above; a `list` is a chart-rendered Secret the
    // operator neither creates nor references.
    let ingest_inline = yaml_strings(values, &["ingester", "auth", "list"]).len();
    if ingest_inline > 0 && yaml_str(values, &["ingester", "auth", "existingSecret"]).is_none() {
        let key = yaml_str(values, &["ingester", "auth", "secretKey"])
            .unwrap_or_else(|| "tokens".to_string());
        let generated = format!("{cluster_name}-auth-tokens");
        findings.push(format!(
            "ingester.auth.list is set ({ingest_inline} entries), which the chart renders into \
             Secret {generated:?} — synthesis maps only ingester.auth.existingSecret, so the \
             adopted INGEST tier renders no SIGLAKE_AUTH_TOKENS and comes back UNAUTHENTICATED. \
             The handover leaves {generated:?} in the namespace with nothing owning it once the \
             release record is deleted: copy its {key:?} key into a customer-managed Secret, set \
             spec.authTokensSecretRef to that, and keep the token bytes out of spec.extraEnv."
        ));
    }

    // OIDC, on either tier. Issuer, audience and claim are not credentials, so
    // spec.extraEnv can carry them — but it folds cluster-wide and the ingest
    // server and the query server read the SAME three variables, so carrying
    // one tier's block turns OIDC on for the other one too.
    let oidc = [
        ("ingester", tier_oidc(values, "ingester")),
        ("query", tier_oidc(values, "query")),
    ];
    let both_tiers_agree = match (&oidc[0].1, &oidc[1].1) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    };
    for (tier, cfg) in &oidc {
        let Some(cfg) = cfg else { continue };
        let TierOidc {
            issuer,
            audience,
            tenant_claim,
        } = cfg;
        let lost = match (*tier, tenant_claim.as_deref()) {
            ("query", None) => "every caller is served without a verified token".to_string(),
            ("query", Some(claim)) => format!(
                "every caller is served without a verified token, and the tenant binding on \
                 {claim:?} is gone: queries that used to read `tenant_<claim>` all read the \
                 single default namespace"
            ),
            (_, None) => "ingest verifies nothing beyond whatever spec.authTokensSecretRef \
                 carries, and open when it carries none"
                .to_string(),
            (_, Some(claim)) => format!(
                "ingest verifies nothing beyond whatever spec.authTokensSecretRef carries, and \
                 the tenant binding on {claim:?} is gone: writes land in the `default` tenant and \
                 an X-Scope-OrgID naming any other one is refused with 403"
            ),
        };
        let disposition = if both_tiers_agree {
            "Both tiers ran this same block and none of it is a credential, so \
             SIGLAKE_OIDC_ISSUER / _AUDIENCE / _TENANT_CLAIM in spec.extraEnv reproduce it \
             cluster-wide."
        } else {
            "spec.extraEnv folds cluster-wide and both binaries read the same three variables, so \
             putting this block there also turns OIDC on for the tier that did not run it — keep \
             that tier on the chart, or make both sides deliberate before handover."
        };
        let adopted = if *tier == "query" { "QUERY" } else { "INGEST" };
        findings.push(format!(
            "{tier}.oidc is configured (issuer {issuer:?}, audience {audience:?}) but the CR has \
             no field for it — the adopted {adopted} tier renders no SIGLAKE_OIDC_* and {lost}. \
             {disposition}"
        ));
    }

    // Tenant routing. The chart renders the variable either way; the binary
    // leaves the header untrusted when it is absent, so only the `true` case
    // changes behaviour at cutover.
    if yaml_bool(values, &["ingester", "trustScopeHeader"]) == Some(true) {
        findings.push(
            "ingester.trustScopeHeader is true but the CR has no field for it — the adopted \
             INGEST tier renders no SIGLAKE_TRUST_SCOPE_HEADER and the binary leaves the header \
             untrusted, so it turns SINGLE-TENANT: an X-Scope-OrgID naming any tenant but \
             \"default\" is refused with 403 and those writers stop. Add \
             SIGLAKE_TRUST_SCOPE_HEADER=1 to spec.extraEnv to keep the routing (the other tiers \
             ignore it), or bind the tenant to a verified token with oidc.tenantClaim first."
                .to_string(),
        );
    }

    // Admission bounds. All three default to unbounded, which is what the
    // adopted ingester runs; a release that set one loses it silently.
    let allowed = yaml_strings(values, &["ingester", "allowedTenants"]);
    if !allowed.is_empty() {
        let joined = allowed.join(",");
        findings.push(format!(
            "ingester.allowedTenants bounds ingest to {} tenants ({joined}) but the CR has no \
             field for it — the adopted INGEST tier renders no SIGLAKE_ALLOWED_TENANTS and admits \
             ANY resolved tenant, including one a JWT claim names. Add \
             SIGLAKE_ALLOWED_TENANTS={joined} to spec.extraEnv to keep the bound.",
            allowed.len()
        ));
    }
    for (key, env, mints, cost) in [
        (
            "maxTenants",
            "SIGLAKE_MAX_TENANTS",
            "tenants",
            "each one holding a backpressure lane, a set of metric label values and an Iceberg \
             namespace of seven tables",
        ),
        (
            "maxLanes",
            "SIGLAKE_INGEST_MAX_LANES",
            "(tenant, index) lanes",
            "each one holding an open file per shard",
        ),
    ] {
        let Some(cap) = yaml_i64(values, &["ingester", key]).filter(|n| *n > 0) else {
            continue;
        };
        findings.push(format!(
            "ingester.{key} is {cap} but the CR has no field for it — the adopted INGEST tier \
             renders no {env} and mints {mints} without a bound, {cost}, on a key the client \
             supplies. Add {env}={cap} to spec.extraEnv to keep the cap."
        ));
    }
}

/// Synthesize a `SiglakeCluster` from chart values. `catalog_uri` must be
/// supplied by the caller: the chart wires Postgres through a Secret and
/// never materializes a URI, so it cannot be inferred from values.
///
/// `namespace` is the release's namespace (`adoption_namespace_from`). It
/// lands on `metadata.namespace` and in every runbook command: the CRD is
/// namespaced, so a resource without it is applied wherever the current
/// kubecontext points, which is not necessarily where the release runs.
pub fn synthesize_from_values(
    values: &serde_yaml::Value,
    cluster_name: &str,
    namespace: &str,
    catalog_uri: Option<&str>,
) -> Result<AdoptionReport> {
    let mut findings = Vec::new();

    let repo = yaml_str(values, &["image", "repository"])
        .context("values: image.repository is required")?;
    let tag = yaml_str(values, &["image", "tag"]).unwrap_or_default();
    if tag.is_empty() {
        findings.push(
            "image.tag is empty in values (chart falls back to appVersion) — set spec.image \
             explicitly before handover"
                .to_string(),
        );
    }
    let image = if tag.is_empty() {
        // The chart substitutes appVersion, which is not present in a values
        // file. An untagged image would instead ask Kubernetes for `latest`.
        // Leave the synthesized field invalid so a user who ignores the
        // preflight finding is still refused by the reconciler.
        String::new()
    } else {
        format!("{repo}:{tag}")
    };

    let bucket = yaml_str(values, &["s3", "bucket"]);
    // The chart's key is `warehousePrefix`, not `prefix`. Reading the wrong one
    // is silent in the worst way: both default to "warehouse", so a release that
    // never customized it adopts correctly and a release that DID points the CR
    // at the wrong warehouse path — an empty table, not an error.
    let prefix =
        yaml_str(values, &["s3", "warehousePrefix"]).unwrap_or_else(|| "warehouse".to_string());
    let warehouse_url = match bucket {
        Some(b) => format!("s3://{b}/{prefix}/"),
        None => {
            findings.push(
                "s3.bucket missing from values — supply the warehouse URL manually".to_string(),
            );
            String::new()
        }
    };

    let catalog_uri = match catalog_uri {
        Some(u) => u.to_string(),
        None => {
            findings.push(
                "catalog URI cannot be inferred (chart uses a Postgres Secret) — pass \
                 --adopt-catalog-uri"
                    .to_string(),
            );
            String::new()
        }
    };

    let mut spec = SiglakeClusterSpec {
        image,
        warehouse_url,
        catalog_uri,
        aws_region: yaml_str(values, &["s3", "region"]).unwrap_or_default(),
        ..Default::default()
    };

    // Ingest auth. Unmapped, the CR carries no token ref, the operator renders
    // no SIGLAKE_AUTH_TOKENS, and the adopted ingester comes back UNAUTHENTICATED
    // — a security regression that looks like a successful handover. The
    // secret-reference form is the one the CR can carry; the inline-token form
    // and every other auth or tenancy control is reported by
    // `auth_and_tenancy_findings` below.
    if let Some(name) =
        yaml_str(values, &["ingester", "auth", "existingSecret"]).filter(|s| !s.is_empty())
    {
        spec.auth_tokens_secret_ref = Some(crate::crd::SecretRef {
            name,
            key: yaml_str(values, &["ingester", "auth", "secretKey"])
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "tokens".to_string()),
        });
    }
    auth_and_tenancy_findings(values, cluster_name, &mut findings);

    // WAL PVC geometry. The operator SSA-applies the SAME-NAMED PVC before any
    // workload, so a mismatch is not a drift to fix later: shrinking a bound PVC
    // (the chart defaults to 50Gi, the CR to 5Gi) or changing its storageClass is
    // REJECTED by the API server, the reconcile returns Err, and nothing is
    // applied. Carrying the chart's values across is what keeps the apply a no-op.
    if let Some(size) = yaml_str(values, &["wal", "size"]).filter(|s| !s.is_empty()) {
        spec.storage.wal_size = size;
    }
    if let Some(mode) = yaml_str(values, &["wal", "accessMode"]).filter(|s| !s.is_empty()) {
        spec.storage.wal_access_mode = mode;
    }
    if let Some(sc) = yaml_str(values, &["wal", "storageClassName"]).filter(|s| !s.is_empty()) {
        spec.storage.wal_storage_class_name = sc;
    }
    // ...and the claim NAME, which the geometry above says nothing about. The
    // chart resolves `siglake.walClaimName` to `wal.existingClaim` when it is
    // set; the operator has no field for it and always renders and mounts
    // `{cr-name}-wal`. So this is the one WAL divergence that costs data rather
    // than a refused apply: the adopted pods come up on a different claim —
    // fresh and empty unless one of that name happens to exist — while every
    // segment acknowledged and not yet committed to Iceberg stays behind on the
    // release's claim, with nothing mounting it to finish the drain.
    if let Some(claim) = yaml_str(values, &["wal", "existingClaim"]).filter(|c| !c.is_empty()) {
        let operator_claim = format!("{cluster_name}-wal");
        if claim != operator_claim {
            findings.push(format!(
                "wal.existingClaim is {claim:?} but the CR has no field for it — the operator \
                 renders and mounts {operator_claim:?}, so the adopted ingester and compactor \
                 would start against a DIFFERENT, probably EMPTY WAL while every segment on \
                 {claim:?} that has not reached Iceberg stays there unread; drain the WAL to \
                 empty before handover, or keep the chart"
            ));
        }
    }

    // Tenancy + S3 endpoint are not expressible on the CR today. Silently
    // dropping them changes where data lands, so they are findings, not defaults.
    if let Some(ns) = yaml_str(values, &["tenant", "namespace"]).filter(|n| n != "siglake") {
        findings.push(format!(
            "tenant.namespace is {ns:?} but the CR has no field for it — operator-managed pods              would use the binary default \"siglake\"; keep the chart or add it to spec.extraEnv"
        ));
    }
    if let Some(ep) = yaml_str(values, &["s3", "endpoint"]).filter(|e| !e.is_empty()) {
        findings.push(format!(
            "s3.endpoint is {ep:?} (non-AWS object store) but the CR has no field for it —              set AWS_ENDPOINT_URL via spec.extraEnv before cutting over"
        ));
    }
    // The same rule, for the release that turned delete-task execution OFF.
    // Since 2026-09-11 the binary and the operator both run the sweep, so
    // dropping the chart's opt-out would start executing acknowledged
    // deletions the moment the CR takes over the compactor.
    if values
        .get("compactor")
        .and_then(|c| c.get("deleteTasks"))
        .and_then(|v| v.as_bool())
        == Some(false)
    {
        findings.push(
            "compactor.deleteTasks is false but the CR has no field for it — the adopted \
             compactor EXECUTES pending delete tasks by default; add SIGLAKE_DELETE_TASKS=0 to \
             spec.extraEnv to keep the opt-out"
                .to_string(),
        );
    }
    // The footer inverted index is also default-on in the binary and operator.
    // Preserve an adopted chart's explicit opt-out in the handover report.
    if values
        .get("compactor")
        .and_then(|c| c.get("invertedIndex"))
        .and_then(|i| i.get("enabled"))
        .and_then(|v| v.as_bool())
        == Some(false)
    {
        findings.push(
            "compactor.invertedIndex.enabled is false but the CR has no field for it — the \
             adopted compactor BUILDS footer inverted indexes by default; add \
             SIGLAKE_INVERTED_INDEX=0 to spec.extraEnv to keep the opt-out"
                .to_string(),
        );
    }
    // The post-rewrite Puffin rebuild runs the other way: the binary and the
    // operator leave it off since 2026-09-14, so a chart that turned it ON is
    // the arm that changes behaviour at handover — recluster outputs stop
    // gaining sidecars the moment the CR takes over the compactor.
    if values
        .get("compactor")
        .and_then(|c| c.get("indexRebuild"))
        .and_then(|v| v.as_bool())
        == Some(true)
    {
        findings.push(
            "compactor.indexRebuild is true but the CR has no field for it — the adopted \
             compactor does NOT rebuild missing Puffin indexes after a rewrite; add \
             SIGLAKE_INDEX_REBUILD=1 to spec.extraEnv to keep the opt-in (indexes already \
             registered stay readable either way)"
                .to_string(),
        );
    }
    // And for a release that kept the batch-job store in memory. The adopted
    // query tier points the store at the catalog Postgres, so cutting over
    // changes where batch state lives: jobs submitted before the handover are
    // not in the new store, and every job after it is shared by the replicas.
    if values
        .get("query")
        .and_then(|q| q.get("jobs"))
        .and_then(|j| j.get("persistent"))
        .and_then(|v| v.as_bool())
        == Some(false)
    {
        findings.push(
            "query.jobs.persistent is false but the CR has no field for it — the adopted query \
             tier stores batch jobs in the CATALOG Postgres, shared by every replica; set a \
             blank SIGLAKE_JOBS_POSTGRES_URI in spec.extraEnv to keep the per-pod in-memory \
             store only with spec.autoscaling.query.max=1 (a higher maximum is InvalidSpec)"
                .to_string(),
        );
    }
    // And for the release that turned WAL mirroring OFF. The adopted ingester
    // mirrors at any replica count since 2026-09-11, so cutting over starts
    // uploading every sealed segment — new PUTs, and new objects under a prefix
    // nothing reclaims unless the drain claims from it.
    if values
        .get("wal")
        .and_then(|w| w.get("mirror"))
        .and_then(|m| m.get("enabled"))
        .and_then(|v| v.as_bool())
        == Some(false)
    {
        findings.push(
            "wal.mirror.enabled is false but the CR has no field for it — the adopted ingester \
             MIRRORS sealed WAL segments to the object store by default; add an empty \
             SIGLAKE_WAL_MIRROR_PREFIX to spec.extraEnv to keep the opt-out"
                .to_string(),
        );
    }

    // Chart replicas → fixed-size autoscaling bands (min == max) so adoption
    // never changes pod counts.
    for (tier, slot) in [
        ("ingester", &mut spec.autoscaling.ingester),
        ("compactor", &mut spec.autoscaling.compactor),
        ("query", &mut spec.autoscaling.query),
    ] {
        let replicas = yaml_i64(values, &[tier, "replicas"]).unwrap_or(1) as i32;
        slot.min = replicas;
        slot.max = replicas;
    }

    // The chart chooses the drain explicitly, while the CR derives it from the
    // compactor policy ceiling. A fixed one-pod catalog-claim release cannot be
    // represented without making the policy scale-out-capable, so stop the
    // offline handover instead of producing a CR whose first reconcile must
    // refuse the existing templates.
    let chart_catalog_claim = values
        .get("compactor")
        .and_then(|compactor| compactor.get("catalogClaim"))
        .and_then(|claim| claim.get("enabled"))
        .and_then(|enabled| enabled.as_bool())
        .unwrap_or(false);
    if chart_catalog_claim != crate::render::uses_catalog_claim(&spec.autoscaling.compactor) {
        findings.push(format!(
            "compactor.catalogClaim.enabled is {chart_catalog_claim}, but the synthesized fixed policy selects the other drain mode — before handover, either align the chart to that mode and finish its drain, or set spec.autoscaling.compactor.max above 1 to retain catalog claims and accept a scale-out-capable policy"
        ));
    }

    // Per-tier resources. Memory is not a cosmetic field: the query server
    // sizes its caches and memory pool from the container limit, so adopting
    // a release at a different limit changes how every query runs, and a
    // pod-template change rolls the tier. Carrying the block across keeps the
    // first reconcile a no-op.
    spec.resources.ingester = tier_resources(values, "ingester");
    spec.resources.compactor = tier_resources(values, "compactor");
    spec.resources.query = tier_resources(values, "query");

    // Per-tier extraEnv folds into the cluster-wide list (union, first-wins).
    // The binaries ignore env vars they don't read, so over-broadcast is
    // harmless; a per-tier split is a CRD follow-on if a conflict ever
    // matters.
    let mut extra_env = Vec::new();
    for tier in ["ingester", "compactor", "query"] {
        tier_extra_env(values, tier, &mut extra_env);
    }
    if !extra_env.is_empty() {
        findings.push(format!(
            "per-tier extraEnv folded cluster-wide ({} vars) — verify none are tier-conflicting",
            extra_env.len()
        ));
    }
    spec.extra_env = extra_env;

    if let Some(sa) = yaml_str(values, &["serviceAccount", "name"]) {
        spec.service_account_name = sa;
    }

    // Detection tiers are NOT part of siglake any more — they moved out and
    // consume the WAL through the public interface. An older release may still
    // have them enabled in values, and adoption must say plainly that the
    // operator will not manage them rather than silently dropping them.
    for tier in ["detector", "correlator", "dispatcher"] {
        let enabled = values
            .get(tier)
            .and_then(|t| t.get("enabled"))
            .and_then(|e| e.as_bool())
            .unwrap_or(false);
        if enabled {
            findings.push(format!(
                "{tier} is enabled in values, but siglake no longer ships the detection tiers \
                 — they moved out of siglake and consume the WAL through \
                 `siglake_wal::consumer`. The operator will NOT manage this workload; deploy it \
                 separately before removing the old release, or its pods stop."
            ));
        }
    }

    let mut cluster = SiglakeCluster::new(cluster_name, spec);
    cluster.metadata.namespace = Some(namespace.to_string());
    let runbook = runbook_for(cluster_name, namespace);
    Ok(AdoptionReport {
        cluster,
        findings,
        runbook,
    })
}

fn runbook_for(name: &str, namespace: &str) -> String {
    format!(
        r#"# Adoption handover runbook (manual commit point — see DESIGN_operator_adoption.md)
# Release {name} in namespace {namespace}. Every command is a comment: step 5
# applies THIS file, so nothing below the resource may be YAML.
# 0. Preconditions: findings list above is EMPTY; operator deployed + CRD installed;
#    this output saved to cluster.yaml. Nothing before step 3 mutates the cluster.
# 1. Verify name parity (all objects must already exist with these names):
#   kubectl get deploy/{name}-ingester deploy/{name}-compactor sts/{name}-query -n {namespace}
# 2. Back up the helm release secrets BEFORE step 4 deletes them. This file is the
#    only way to give helm its release history back; without it an abort can only
#    re-install the release at revision 1.
#   kubectl get secret -n {namespace} -l owner=helm,name={name} -o yaml > helm-release-{name}.yaml
# 3. Flip ownership metadata (helm forgets WITHOUT cascading deletes):
#   for kind in deploy/{name}-ingester deploy/{name}-compactor sts/{name}-query; do
#     kubectl annotate $kind -n {namespace} meta.helm.sh/release-name- meta.helm.sh/release-namespace-
#     kubectl label $kind -n {namespace} app.kubernetes.io/managed-by=siglake-operator --overwrite
#   done
# 4. Delete the helm release secret (helm's memory of the release):
#   kubectl delete secret -n {namespace} -l owner=helm,name={name}
# 5. Apply the CR; the operator's first reconcile should be a NO-OP apply. The
#    resource carries metadata.namespace, so this lands in {namespace} whatever the
#    current context selects:
#   kubectl apply -f cluster.yaml
# 6. Watch: status must reach Ready WITHOUT pod restarts when specs match.
#   kubectl get siglakecluster {name} -n {namespace} -w
#
# Abort path, by the last step you ran:
# - through step 2: nothing changed; delete helm-release-{name}.yaml and stop.
# - after step 3, before step 4: the objects are unmanaged but running, and the
#   release secret still exists, so helm's history is intact. Restore BOTH the
#   annotations and the managed-by label — helm refuses to adopt an object that
#   carries only one of them:
#     for kind in deploy/{name}-ingester deploy/{name}-compactor sts/{name}-query; do
#       kubectl annotate $kind -n {namespace} --overwrite \
#         meta.helm.sh/release-name={name} meta.helm.sh/release-namespace={namespace}
#       kubectl label $kind -n {namespace} app.kubernetes.io/managed-by=Helm --overwrite
#     done
# - after step 4: do the same re-annotation, then put the release record back, or
#   `helm list` stays empty and upgrade/rollback/uninstall have nothing to act on:
#     kubectl apply -f helm-release-{name}.yaml
#   With no backup the fallback is `helm install {name} <chart> -n {namespace} -f <the
#   effective values you saved>` over the re-annotated objects: it takes ownership
#   again but starts history at revision 1, so every earlier revision is gone."#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::name_of;
    use serde::Deserialize;

    /// A release that CUSTOMIZED the keys the converter has to read.
    ///
    /// `standing_values` leaves them all at chart defaults, which is precisely
    /// why it could not catch reading `s3.prefix` instead of
    /// `s3.warehousePrefix`: both default to "warehouse", so the wrong key
    /// produced the right answer. A fixture that agrees with the code under test
    /// by coincidence is worse than no fixture — it reads as coverage.
    fn customized_values() -> serde_yaml::Value {
        serde_yaml::from_str(
            r#"
image:
  repository: registry.example.com/siglake
  tag: v1
s3:
  bucket: acme-logs
  region: eu-west-1
  warehousePrefix: lake/prod
  endpoint: https://minio.internal:9000
tenant:
  namespace: acme
wal:
  size: 50Gi
  accessMode: ReadWriteMany
  storageClassName: efs-sc
ingester:
  replicas: 3
  auth:
    existingSecret: acme-ingest-tokens
    secretKey: ingest-tokens
compactor:
  replicas: 2
  resources:
    limits:
      memory: 17Gi
      cpu: 4
query:
  replicas: 4
  resources:
    requests:
      memory: 2Gi
    limits:
      memory: 16Gi
"#,
        )
        .unwrap()
    }

    #[test]
    fn adoption_carries_the_customized_keys_across() {
        let report = synthesize_from_values(
            &customized_values(),
            "acme",
            "acme",
            Some("postgres://x@y/z"),
        )
        .unwrap();
        let spec = &report.cluster.spec;

        // The warehouse path decides which table the adopted cluster reads.
        assert_eq!(spec.warehouse_url, "s3://acme-logs/lake/prod/");

        // Losing this silently un-authenticates ingest.
        let auth = spec
            .auth_tokens_secret_ref
            .as_ref()
            .expect("ingester auth secret must survive adoption");
        assert_eq!(auth.name, "acme-ingest-tokens");
        assert_eq!(auth.key, "ingest-tokens");

        // The PVC applies FIRST and a shrink is rejected, failing the whole
        // reconcile — so these must match the chart, not the CR defaults.
        assert_eq!(spec.storage.wal_size, "50Gi");
        assert_eq!(spec.storage.wal_access_mode, "ReadWriteMany");
        assert_eq!(spec.storage.wal_storage_class_name, "efs-sc");

        // A raised memory limit decides how every query on the adopted tier
        // runs (caches and pool derive from it) and a changed pod template
        // rolls the tier — so `<tier>.resources` is carried key for key, with
        // helm's numeric cpu landing as the quantity string Kubernetes reads.
        let query = spec
            .resources
            .query
            .as_ref()
            .expect("query resources carried");
        assert_eq!(query.limits.get("memory").map(String::as_str), Some("16Gi"));
        assert_eq!(
            query.requests.get("memory").map(String::as_str),
            Some("2Gi")
        );
        let compactor = spec
            .resources
            .compactor
            .as_ref()
            .expect("compactor resources carried");
        assert_eq!(compactor.limits.get("cpu").map(String::as_str), Some("4"));
        assert_eq!(
            compactor.limits.get("memory").map(String::as_str),
            Some("17Gi")
        );
        assert!(
            spec.resources.ingester.is_none(),
            "a tier the release never sized adopts on the packaged defaults"
        );
        // And the render honours it: the adopted query pod is the 16Gi pod the
        // release ran, with the chart's untouched cpu limit still in place.
        let rendered = crate::render::query_statefulset(&report.cluster, 1)
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .remove(0)
            .resources
            .unwrap();
        let limits = rendered.limits.unwrap();
        assert_eq!(limits["memory"].0, "16Gi");
        assert_eq!(limits["cpu"].0, "2");

        // Not expressible on the CR — must be surfaced, never silently dropped.
        let findings = report.findings.join(" | ");
        assert!(
            findings.contains("tenant.namespace"),
            "unmapped tenant namespace must be reported: {findings}"
        );
        assert!(
            findings.contains("s3.endpoint"),
            "unmapped S3 endpoint must be reported: {findings}"
        );
    }

    fn standing_values() -> serde_yaml::Value {
        serde_yaml::from_str(
            r#"
image:
  repository: registry.example.com/siglake
  tag: bench-20260717-212139
s3:
  bucket: example-warehouse
  region: us-west-2
compactor:
  replicas: 1
  extraEnv:
  - name: SIGLAKE_PARQUET_TARGET_ROW_GROUP_BYTES
    value: "33554432"
  - name: SIGLAKE_AUTO_PROMOTE_MIN_PCT
    value: "50"
ingester:
  replicas: 1
query:
  replicas: 1
detector:
  enabled: false
correlator:
  enabled: false
dispatcher:
  enabled: false
"#,
        )
        .unwrap()
    }

    #[test]
    fn converter_maps_the_standing_cluster_values() {
        let report = synthesize_from_values(
            &standing_values(),
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap();
        let spec = &report.cluster.spec;
        assert_eq!(
            spec.image,
            "registry.example.com/siglake:bench-20260717-212139"
        );
        assert_eq!(spec.warehouse_url, "s3://example-warehouse/warehouse/");
        assert_eq!(spec.aws_region, "us-west-2");
        assert_eq!(spec.autoscaling.ingester.min, 1);
        assert_eq!(spec.autoscaling.ingester.max, 1);
        assert_eq!(spec.extra_env.len(), 2);
        assert_eq!(spec.extra_env[1].name, "SIGLAKE_AUTO_PROMOTE_MIN_PCT");
        // Only the folded-extraEnv advisory should remain.
        assert_eq!(report.findings.len(), 1, "{:?}", report.findings);

        // No `resources` in values ⇒ no override, and the operator's packaged
        // query pod is the chart's packaged query pod (4Gi). This is the
        // parity that #545 restored; a values file at chart defaults must adopt
        // onto the same pod template.
        assert!(spec.resources.query.is_none());
        let rendered = crate::render::query_statefulset(&report.cluster, 1)
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .remove(0)
            .resources
            .unwrap();
        assert_eq!(rendered.limits.unwrap()["memory"].0, "4Gi");
    }

    #[test]
    fn fixed_one_pod_catalog_claim_adoption_requires_a_policy_choice() {
        let mut values = standing_values();
        values["compactor"]["catalogClaim"]["enabled"] = serde_yaml::Value::Bool(true);
        let report = synthesize_from_values(
            &values,
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap();
        assert!(report.findings.iter().any(|finding| {
            finding.contains("compactor.catalogClaim.enabled is true")
                && finding.contains("spec.autoscaling.compactor.max above 1")
        }));
    }

    #[test]
    fn empty_chart_image_tag_cannot_become_latest_during_adoption() {
        let mut values = standing_values();
        values["image"]["tag"] = serde_yaml::Value::String(String::new());
        let report = synthesize_from_values(
            &values,
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap();

        assert!(
            report.cluster.spec.image.is_empty(),
            "an unknown chart appVersion must not silently become an untagged image"
        );
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.contains("image.tag is empty")),
            "the handover must tell the user to set spec.image"
        );
    }

    #[test]
    fn name_parity_holds_when_cr_named_after_release() {
        let report = synthesize_from_values(
            &standing_values(),
            "siglake",
            "siglake",
            Some("postgres://x@y/z"),
        )
        .unwrap();
        // Chart convention: {release}-{role}. Operator: name_of(cr, role).
        for role in ["ingester", "compactor", "query"] {
            assert_eq!(name_of(&report.cluster, role), format!("siglake-{role}"));
        }
    }

    /// A release that turned delete-task execution off is adopted onto a
    /// compactor that runs it. The CR has no field for the toggle, so the
    /// handover has to say so rather than let acknowledged deletions start
    /// executing at cutover.
    #[test]
    fn a_chart_delete_task_opt_out_is_reported_at_adoption() {
        let mut values = standing_values();
        values["compactor"]["deleteTasks"] = serde_yaml::Value::Bool(false);
        let report = synthesize_from_values(
            &values,
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap();
        assert!(
            report
                .findings
                .iter()
                .any(|f| f.contains("compactor.deleteTasks") && f.contains("SIGLAKE_DELETE_TASKS")),
            "the opt-out must be surfaced with its replacement: {:?}",
            report.findings
        );

        // The default, and an explicit `true`, are what the adopted compactor
        // already does — no finding to make.
        values["compactor"]["deleteTasks"] = serde_yaml::Value::Bool(true);
        let report = synthesize_from_values(
            &values,
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap();
        assert!(
            !report
                .findings
                .iter()
                .any(|f| f.contains("compactor.deleteTasks")),
            "{:?}",
            report.findings
        );
    }

    #[test]
    fn a_chart_inverted_index_opt_out_is_reported_at_adoption() {
        let mut values = standing_values();
        values["compactor"]["invertedIndex"]["enabled"] = serde_yaml::Value::Bool(false);
        let report = synthesize_from_values(
            &values,
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap();
        assert!(
            report.findings.iter().any(|finding| finding
                .contains("compactor.invertedIndex.enabled")
                && finding.contains("SIGLAKE_INVERTED_INDEX=0")),
            "the opt-out must be surfaced with its replacement: {:?}",
            report.findings
        );

        values["compactor"]["invertedIndex"]["enabled"] = serde_yaml::Value::Bool(true);
        let report = synthesize_from_values(
            &values,
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap();
        assert!(
            !report
                .findings
                .iter()
                .any(|finding| finding.contains("compactor.invertedIndex.enabled")),
            "the default-on chart needs no adoption warning: {:?}",
            report.findings
        );
    }

    /// The rebuild switch ships off, so its adoption risk is the opposite one:
    /// a chart that opted IN loses the rebuild at cutover, and an explicit
    /// `false` matches what the adopted compactor already does.
    #[test]
    fn a_chart_index_rebuild_opt_in_is_reported_at_adoption() {
        let mut values = standing_values();
        values["compactor"]["indexRebuild"] = serde_yaml::Value::Bool(true);
        let report = synthesize_from_values(
            &values,
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap();
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.contains("compactor.indexRebuild")
                    && finding.contains("SIGLAKE_INDEX_REBUILD=1")),
            "the opt-in must be surfaced with its replacement: {:?}",
            report.findings
        );

        values["compactor"]["indexRebuild"] = serde_yaml::Value::Bool(false);
        let report = synthesize_from_values(
            &values,
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap();
        assert!(
            !report
                .findings
                .iter()
                .any(|finding| finding.contains("compactor.indexRebuild")),
            "the default-off chart needs no adoption warning: {:?}",
            report.findings
        );
    }

    /// A release running the in-memory batch-job store is adopted onto a query
    /// tier that shares the catalog Postgres. The CR has no field for it, so
    /// the handover has to say where batch state moves to.
    #[test]
    fn a_chart_in_memory_job_store_is_reported_at_adoption() {
        let mut values = standing_values();
        values["query"]["jobs"]["persistent"] = serde_yaml::Value::Bool(false);
        let report = synthesize_from_values(
            &values,
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap();
        assert!(
            report
                .findings
                .iter()
                .any(|f| f.contains("query.jobs.persistent")
                    && f.contains("SIGLAKE_JOBS_POSTGRES_URI")),
            "the opt-out must be surfaced with its replacement: {:?}",
            report.findings
        );

        // The chart default, and an explicit `true`, are what the adopted
        // query tier already does — no finding to make.
        values["query"]["jobs"]["persistent"] = serde_yaml::Value::Bool(true);
        let report = synthesize_from_values(
            &values,
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap();
        assert!(
            !report
                .findings
                .iter()
                .any(|f| f.contains("query.jobs.persistent")),
            "{:?}",
            report.findings
        );
    }

    /// A release that runs without the WAL mirror is adopted onto an ingester
    /// that mirrors. The CR has no field for it, so the handover has to say so:
    /// the cutover otherwise starts writing an object per sealed segment into a
    /// bucket the operator never mentioned.
    #[test]
    fn a_chart_wal_mirror_opt_out_is_reported_at_adoption() {
        let mut values = standing_values();
        values["wal"]["mirror"]["enabled"] = serde_yaml::Value::Bool(false);
        let report = synthesize_from_values(
            &values,
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap();
        assert!(
            report.findings.iter().any(
                |f| f.contains("wal.mirror.enabled") && f.contains("SIGLAKE_WAL_MIRROR_PREFIX")
            ),
            "the opt-out must be surfaced with its replacement: {:?}",
            report.findings
        );

        // The chart default is what the adopted ingester already does.
        values["wal"]["mirror"]["enabled"] = serde_yaml::Value::Bool(true);
        let report = synthesize_from_values(
            &values,
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap();
        assert!(
            !report
                .findings
                .iter()
                .any(|f| f.contains("wal.mirror.enabled")),
            "{:?}",
            report.findings
        );
    }

    /// A release that pre-created its WAL claim is adopted onto an operator
    /// that mounts a claim of its own naming. The geometry keys say nothing
    /// about this, so without a finding the handover reads as clean and the
    /// adopted pods silently start on another volume.
    #[test]
    fn a_chart_wal_existing_claim_is_reported_at_adoption() {
        let uri = Some("postgres://siglake:pw@host:5432/db");
        let finding_of = |values: &serde_yaml::Value| {
            synthesize_from_values(values, "siglake", "siglake", uri)
                .unwrap()
                .findings
                .into_iter()
                .find(|f| f.contains("wal.existingClaim"))
        };

        // Absent (the standing release) and explicitly empty (the chart
        // default) both mean the chart's own `siglake-wal` — the claim the
        // operator renders. Nothing moves, nothing to report.
        let mut values = standing_values();
        assert_eq!(finding_of(&values), None, "no wal key at all");
        values["wal"]["existingClaim"] = serde_yaml::Value::String(String::new());
        assert_eq!(finding_of(&values), None, "chart default is empty");

        // A release whose pre-created claim happens to carry the name the
        // operator renders adopts onto the SAME volume.
        values["wal"]["existingClaim"] = serde_yaml::Value::String("siglake-wal".into());
        assert_eq!(finding_of(&values), None, "same claim, same WAL");

        // Any other name is a WAL switch at cutover.
        values["wal"]["existingClaim"] = serde_yaml::Value::String("acme-wal-efs".into());
        let finding = finding_of(&values).expect("a repointed WAL claim must be reported");
        assert!(
            finding.contains("\"acme-wal-efs\"") && finding.contains("\"siglake-wal\""),
            "the finding must name both claims: {finding}"
        );
        assert!(
            finding.contains("EMPTY WAL"),
            "the finding must say what the adopted pods start against: {finding}"
        );

        // The claim it names is the one the operator actually renders and
        // mounts, not a string the preflight guesses at.
        let report = synthesize_from_values(&values, "siglake", "siglake", uri).unwrap();
        assert_eq!(
            crate::render::wal_pvc(&report.cluster)
                .metadata
                .name
                .unwrap(),
            "siglake-wal"
        );

        // Carrying the geometry across is unchanged by any of it.
        values["wal"]["size"] = serde_yaml::Value::String("50Gi".into());
        let report = synthesize_from_values(&values, "siglake", "siglake", uri).unwrap();
        assert_eq!(report.cluster.spec.storage.wal_size, "50Gi");
    }

    /// The auth and tenancy keys at their values.yaml defaults — the shape
    /// `helm get values <release> --all` hands the preflight for a release
    /// that configured none of them. Every one of them must adopt silently;
    /// a preflight that cries over a default teaches the operator to skim.
    const CHART_AUTH_DEFAULTS: &str = r#"
ingester:
  auth:
    existingSecret: ""
    secretKey: tokens
    list: []
  trustScopeHeader: false
  allowedTenants: []
  maxTenants: 0
  maxLanes: 0
  oidc:
    issuer: ""
    audience: ""
    tenantClaim: ""
query:
  tokens:
    existingSecret: ""
    secretKey: tokens
    list: []
  oidc:
    issuer: ""
    audience: ""
    tenantClaim: ""
"#;

    fn merge(base: &mut serde_yaml::Value, patch: &serde_yaml::Value) {
        match (base, patch) {
            (serde_yaml::Value::Mapping(b), serde_yaml::Value::Mapping(p)) => {
                for (key, value) in p {
                    match b.get_mut(key) {
                        Some(slot) => merge(slot, value),
                        None => {
                            b.insert(key.clone(), value.clone());
                        }
                    }
                }
            }
            (base, patch) => *base = patch.clone(),
        }
    }

    /// `standing_values` + the chart's auth/tenancy defaults + `patch`.
    fn values_with(patch: &str) -> serde_yaml::Value {
        let mut values = standing_values();
        merge(
            &mut values,
            &serde_yaml::from_str(CHART_AUTH_DEFAULTS).unwrap(),
        );
        merge(&mut values, &serde_yaml::from_str(patch).unwrap());
        values
    }

    fn adopt(values: &serde_yaml::Value) -> AdoptionReport {
        synthesize_from_values(
            values,
            "siglake",
            "siglake",
            Some("postgres://siglake:pw@host:5432/db"),
        )
        .unwrap()
    }

    /// The container env the operator renders for a tier, flattened to
    /// `NAME` / `NAME=value` so a test can ask what the adopted pod starts with.
    fn rendered_env(report: &AdoptionReport, tier: &str) -> Vec<String> {
        let containers = match tier {
            "query" => {
                crate::render::query_statefulset(&report.cluster, 1)
                    .spec
                    .unwrap()
                    .template
                    .spec
                    .unwrap()
                    .containers
            }
            "ingester" => {
                crate::render::ingester_deployment(
                    &report.cluster,
                    1,
                    report.cluster.spec.auth_tokens_secret_ref.as_ref(),
                )
                .spec
                .unwrap()
                .template
                .spec
                .unwrap()
                .containers
            }
            other => panic!("no render for {other}"),
        };
        let mut out = Vec::new();
        for container in containers {
            for arg in container.args.unwrap_or_default() {
                out.push(arg);
            }
            for env in container.env.unwrap_or_default() {
                out.push(match env.value {
                    Some(value) => format!("{}={value}", env.name),
                    None => env.name,
                });
            }
        }
        out
    }

    #[test]
    fn chart_auth_defaults_adopt_without_an_auth_finding() {
        let report = adopt(&values_with("{}"));
        // Only the folded-extraEnv advisory `standing_values` earns.
        assert_eq!(report.findings.len(), 1, "{:?}", report.findings);
        assert!(
            report.findings[0].contains("extraEnv"),
            "{:?}",
            report.findings
        );
    }

    /// Every security-relevant chart control synthesis cannot carry, with the
    /// tier it belongs to, the behaviour that stops holding at cutover, and
    /// the disposition that keeps the handover secure.
    #[test]
    fn configured_auth_and_tenant_controls_are_reported_at_adoption() {
        // (case, values patch, substrings the finding must carry)
        let cases: &[(&str, &str, &[&str])] = &[
            (
                "query bearer tokens from a customer Secret",
                "query:\n  tokens:\n    existingSecret: acme-query-tokens\n    secretKey: sql\n",
                &[
                    "query.tokens.existingSecret is \"acme-query-tokens\"",
                    "\"sql\"",
                    "QUERY tier",
                    "SIGLAKE_QUERY_TOKENS",
                    "auth OPEN",
                    "keep the query tier on the chart",
                ],
            ),
            (
                "query bearer tokens defined inline",
                "query:\n  tokens:\n    list: [alpha-token, beta-token]\n",
                &[
                    "query.tokens.list is set (2 entries)",
                    "\"siglake-query-tokens\"",
                    "auth OPEN",
                    "PLAINTEXT",
                ],
            ),
            (
                "ingest tokens defined inline",
                "ingester:\n  auth:\n    list: [ingest-token]\n",
                &[
                    "ingester.auth.list is set (1 entries)",
                    "\"siglake-auth-tokens\"",
                    "UNAUTHENTICATED",
                    "spec.authTokensSecretRef",
                ],
            ),
            (
                "query OIDC with tenant binding",
                "query:\n  oidc:\n    issuer: https://idp.example.com\n    audience: siglake\n    tenantClaim: org\n",
                &[
                    "query.oidc is configured",
                    "https://idp.example.com",
                    "QUERY tier",
                    "SIGLAKE_OIDC_*",
                    "tenant binding on \"org\"",
                    "single default namespace",
                    "turns OIDC on for the tier that did not run it",
                ],
            ),
            (
                "ingest OIDC with tenant binding",
                "ingester:\n  oidc:\n    issuer: https://idp.example.com\n    audience: siglake\n    tenantClaim: org\n",
                &[
                    "ingester.oidc is configured",
                    "INGEST tier",
                    "tenant binding on \"org\"",
                    "refused with 403",
                ],
            ),
            (
                "half an OIDC block renders nothing, so it costs nothing",
                "query:\n  oidc:\n    issuer: https://idp.example.com\n",
                &[],
            ),
            (
                "trusted scope header",
                "ingester:\n  trustScopeHeader: true\n",
                &[
                    "ingester.trustScopeHeader is true",
                    "SIGLAKE_TRUST_SCOPE_HEADER=1",
                    "SINGLE-TENANT",
                    "403",
                ],
            ),
            (
                "explicit single-tenant routing is the adopted default",
                "ingester:\n  trustScopeHeader: false\n",
                &[],
            ),
            (
                "allowed tenants",
                "ingester:\n  allowedTenants: [acme, globex]\n",
                &[
                    "ingester.allowedTenants bounds ingest to 2 tenants",
                    "SIGLAKE_ALLOWED_TENANTS=acme,globex",
                    "admits ANY resolved tenant",
                ],
            ),
            (
                "tenant cap",
                "ingester:\n  maxTenants: 100\n",
                &[
                    "ingester.maxTenants is 100",
                    "SIGLAKE_MAX_TENANTS=100",
                    "without a bound",
                ],
            ),
            (
                "lane cap",
                "ingester:\n  maxLanes: 512\n",
                &[
                    "ingester.maxLanes is 512",
                    "SIGLAKE_INGEST_MAX_LANES=512",
                    "open file per shard",
                ],
            ),
            (
                "an unbounded cap is what the adopted ingester already runs",
                "ingester:\n  maxTenants: 0\n  maxLanes: 0\n",
                &[],
            ),
        ];

        for (case, patch, expected) in cases {
            let report = adopt(&values_with(patch));
            let new: Vec<&String> = report
                .findings
                .iter()
                .filter(|f| !f.contains("extraEnv folded cluster-wide"))
                .collect();
            if expected.is_empty() {
                assert!(new.is_empty(), "{case}: behaviour-equivalent, yet {new:?}");
                continue;
            }
            assert_eq!(new.len(), 1, "{case}: want one finding, got {new:?}");
            for want in *expected {
                assert!(
                    new[0].contains(want),
                    "{case}: want {want:?} in {:?}",
                    new[0]
                );
            }
        }
    }

    /// Both tiers on the same issuer CAN be reproduced through spec.extraEnv:
    /// the fold is cluster-wide, and here that is what the release ran.
    #[test]
    fn matching_oidc_on_both_tiers_has_an_extra_env_disposition() {
        let report = adopt(&values_with(
            "ingester:\n  oidc:\n    issuer: https://idp.example.com\n    audience: siglake\n    tenantClaim: org\nquery:\n  oidc:\n    issuer: https://idp.example.com\n    audience: siglake\n    tenantClaim: org\n",
        ));
        let oidc: Vec<&String> = report
            .findings
            .iter()
            .filter(|f| f.contains(".oidc is configured"))
            .collect();
        assert_eq!(oidc.len(), 2, "one finding per tier: {oidc:?}");
        for finding in oidc {
            assert!(
                finding
                    .contains("SIGLAKE_OIDC_ISSUER / _AUDIENCE / _TENANT_CLAIM in spec.extraEnv"),
                "{finding}"
            );
        }
    }

    /// The acceptance an empty findings list would violate: a chart whose
    /// every other key adopts cleanly still hands over a query pod that starts
    /// with `AuthConfig::Open`, because the render carries no auth at all.
    #[test]
    fn an_authenticated_chart_does_not_adopt_to_a_clean_handover() {
        let report = adopt(&values_with(
            "query:\n  tokens:\n    existingSecret: acme-query-tokens\n  oidc:\n    issuer: https://idp.example.com\n    audience: siglake\n    tenantClaim: org\n",
        ));
        assert!(
            report
                .findings
                .iter()
                .any(|f| f.contains("SIGLAKE_QUERY_TOKENS")),
            "{:?}",
            report.findings
        );

        // ...and the loss is what the render actually does, not a guess: the
        // adopted query container carries no token, no issuer and no flag that
        // would give it either, so `AuthConfig::open()` is the branch it takes.
        let env = rendered_env(&report, "query");
        for absent in [
            "SIGLAKE_QUERY_TOKENS",
            "SIGLAKE_OIDC_ISSUER",
            "SIGLAKE_OIDC_AUDIENCE",
            "SIGLAKE_OIDC_TENANT_CLAIM",
            "--tokens",
            "--oidc-issuer",
        ] {
            assert!(
                !env.iter().any(|e| e.starts_with(absent)),
                "the adopted query pod must not appear to keep {absent}: {env:?}"
            );
        }
    }

    /// The same, for the ingest tier's routing and admission bounds.
    #[test]
    fn ingest_routing_and_admission_bounds_leave_the_adopted_pod_unbounded() {
        let report = adopt(&values_with(
            "ingester:\n  trustScopeHeader: true\n  allowedTenants: [acme]\n  maxTenants: 100\n  maxLanes: 512\n",
        ));
        assert_eq!(
            report
                .findings
                .iter()
                .filter(|f| f.contains("spec.extraEnv"))
                .count(),
            4,
            "one finding per lost control: {:?}",
            report.findings
        );
        let env = rendered_env(&report, "ingester");
        for absent in [
            "SIGLAKE_TRUST_SCOPE_HEADER",
            "SIGLAKE_ALLOWED_TENANTS",
            "SIGLAKE_MAX_TENANTS",
            "SIGLAKE_INGEST_MAX_LANES",
        ] {
            assert!(
                !env.iter().any(|e| e.starts_with(absent)),
                "the adopted ingester must not appear to keep {absent}: {env:?}"
            );
        }
    }

    /// The secret-reference form is the one shape the CR can carry, and it
    /// still does — including against a values file that also lists tokens
    /// inline, which the chart itself ignores in favour of the reference.
    #[test]
    fn the_supported_ingest_secret_reference_still_maps() {
        let report = adopt(&values_with(
            "ingester:\n  auth:\n    existingSecret: acme-ingest-tokens\n    secretKey: ingest-tokens\n    list: [ignored-by-the-chart]\n",
        ));
        let auth = report
            .cluster
            .spec
            .auth_tokens_secret_ref
            .as_ref()
            .expect("the reference must survive adoption");
        assert_eq!(auth.name, "acme-ingest-tokens");
        assert_eq!(auth.key, "ingest-tokens");
        assert!(
            !report
                .findings
                .iter()
                .any(|f| f.contains("ingester.auth.list")),
            "the chart ignores the list when a reference is set: {:?}",
            report.findings
        );
        // And the rendered ingester reads the same Secret by reference, so the
        // operator never sees the bytes.
        let env = rendered_env(&report, "ingester");
        assert!(env.iter().any(|e| e == "SIGLAKE_AUTH_TOKENS"), "{env:?}");
    }

    /// Findings are read, pasted and filed. Token bytes belong in neither them
    /// nor `spec.extraEnv`, which the CR stores in plaintext.
    #[test]
    fn adoption_never_copies_token_bytes_out_of_their_secret() {
        let report = adopt(&values_with(
            "ingester:\n  auth:\n    list: [ingest-s3cr3t]\nquery:\n  tokens:\n    list: [query-s3cr3t]\n",
        ));
        let text = format!(
            "{} | {:?}",
            report.findings.join(" | "),
            report.cluster.spec.extra_env
        );
        for secret in ["ingest-s3cr3t", "query-s3cr3t"] {
            assert!(!text.contains(secret), "{secret} leaked into {text}");
        }
    }

    #[test]
    fn missing_catalog_uri_is_a_blocking_finding() {
        let report =
            synthesize_from_values(&standing_values(), "siglake", "siglake", None).unwrap();
        assert!(report
            .findings
            .iter()
            .any(|f| f.contains("--adopt-catalog-uri")));
    }

    /// The report's own step 5 is `kubectl apply -f cluster.yaml`, so the whole
    /// thing has to parse as the one resource it prescribes — findings,
    /// runbook and all. Before #4544 the runbook's command lines were bare, and
    /// the saved file stopped being YAML right after the resource.
    #[test]
    fn the_whole_report_is_the_manifest_its_runbook_applies() {
        let report = adopt(&values_with("{}"));
        let printed = report.to_yaml().unwrap();

        // What kubectl parses: one document, and it is the CR.
        let docs: Vec<serde_yaml::Value> = serde_yaml::Deserializer::from_str(&printed)
            .map(|d| serde_yaml::Value::deserialize(d).expect("the report must parse as YAML"))
            .collect();
        assert_eq!(docs.len(), 1, "one resource per report:\n{printed}");
        assert_eq!(docs[0]["kind"].as_str(), Some("SiglakeCluster"));
        assert_eq!(
            docs[0]["apiVersion"].as_str(),
            Some("siglake.limnion.ai/v1alpha1")
        );
        assert_eq!(docs[0]["metadata"]["name"].as_str(), Some("siglake"));
        assert_eq!(docs[0]["metadata"]["namespace"].as_str(), Some("siglake"));
        let parsed: SiglakeCluster =
            serde_yaml::from_str(&printed).expect("the report must deserialize into the CR");
        assert_eq!(parsed.spec, report.cluster.spec);

        // And the reason it parses: nothing below the resource is anything but
        // a comment. This is the line the docs' extraction leans on.
        for line in printed
            .lines()
            .skip_while(|l| !l.starts_with("# preflight"))
        {
            assert!(
                line.is_empty() || line.starts_with('#'),
                "a non-comment line below the resource breaks the apply: {line:?}\n{printed}"
            );
        }

        // The findings are comments too, one line each whatever they carry.
        let report = adopt(&values_with(
            "ingester:\n  trustScopeHeader: true\n  allowedTenants: [acme]\n",
        ));
        let printed = report.to_yaml().unwrap();
        assert!(printed.contains("# preflight FINDINGS"));
        assert_eq!(
            printed.matches("#  - ").count(),
            report.findings.len(),
            "one comment line per finding:\n{printed}"
        );
        serde_yaml::from_str::<SiglakeCluster>(&printed)
            .expect("findings must not break the manifest");
    }

    /// A release installed into a namespace that is not its name. Every command
    /// has to name the namespace the release runs in, and the resource has to
    /// carry it too — an apply without one lands wherever the current context
    /// points, and the CRD is namespaced.
    #[test]
    fn a_release_whose_namespace_differs_from_its_name_adopts_into_that_namespace() {
        let report = synthesize_from_values(
            &standing_values(),
            "acme",
            "obs-prod",
            Some("postgres://x@y/z"),
        )
        .unwrap();
        assert_eq!(
            report.cluster.metadata.namespace.as_deref(),
            Some("obs-prod")
        );
        assert_eq!(report.cluster.metadata.name.as_deref(), Some("acme"));
        assert_eq!(
            serde_yaml::from_str::<serde_yaml::Value>(&report.to_yaml().unwrap()).unwrap()
                ["metadata"]["namespace"]
                .as_str(),
            Some("obs-prod")
        );

        // Workload names still follow the RELEASE name; only -n moves.
        let runbook = &report.runbook;
        for command in [
            "kubectl get deploy/acme-ingester deploy/acme-compactor sts/acme-query -n obs-prod",
            "kubectl get secret -n obs-prod -l owner=helm,name=acme -o yaml > helm-release-acme.yaml",
            "kubectl delete secret -n obs-prod -l owner=helm,name=acme",
            "kubectl apply -f cluster.yaml",
            "kubectl get siglakecluster acme -n obs-prod -w",
        ] {
            assert!(runbook.contains(command), "missing {command:?}:\n{runbook}");
        }
        // ...including the abort path, which puts helm's own namespace
        // annotation back.
        let abort = runbook.split_once("# Abort path").unwrap().1;
        assert!(
            abort.contains("meta.helm.sh/release-name=acme")
                && abort.contains("meta.helm.sh/release-namespace=obs-prod"),
            "the abort path must restore the release's real namespace:\n{abort}"
        );
        assert!(
            abort.contains("kubectl label $kind -n obs-prod")
                && abort.contains("helm install acme <chart> -n obs-prod"),
            "every abort command must name the release namespace:\n{abort}"
        );
        assert!(
            !runbook.contains("-n acme"),
            "no command may fall back to the release name as a namespace:\n{runbook}"
        );
    }

    /// The flag's compatibility default: a release whose namespace was never
    /// given keeps the `-n {name}` the runbook always assumed.
    #[test]
    fn the_adoption_namespace_defaults_to_the_release_name() {
        assert_eq!(adoption_namespace_from(None, "acme"), "acme");
        assert_eq!(adoption_namespace_from(Some(""), "acme"), "acme");
        assert_eq!(adoption_namespace_from(Some("   "), "acme"), "acme");
        assert_eq!(
            adoption_namespace_from(Some("obs-prod"), "acme"),
            "obs-prod"
        );
        assert_eq!(
            adoption_namespace_from(Some(" obs-prod "), "acme"),
            "obs-prod"
        );
    }

    /// Step 4 deletes the helm release secret, which is helm's whole record of
    /// the release: once it is gone, re-annotating the workloads leaves
    /// `helm list` empty and upgrade/rollback/uninstall with nothing to act on.
    /// An abort path that only says "re-annotate" is therefore wrong after that
    /// step, and the backup that makes it right has to be taken BEFORE it.
    #[test]
    fn the_runbook_backs_up_the_release_history_before_deleting_it() {
        let runbook =
            synthesize_from_values(&standing_values(), "acme", "acme", Some("postgres://x@y/z"))
                .unwrap()
                .runbook;

        let backup = runbook
            .find("-o yaml > helm-release-acme.yaml")
            .expect("the runbook must save the release secrets to a file");
        let delete = runbook
            .find("kubectl delete secret -n acme -l owner=helm,name=acme")
            .expect("the runbook must still delete the release secret");
        assert!(
            backup < delete,
            "the backup has to be taken before the delete:\n{runbook}"
        );

        // The abort path must name the file as the way back, and restore the
        // managed-by label step 3 overwrote — helm's adoption check wants both
        // annotations AND `managed-by=Helm`.
        let abort = runbook
            .split_once("# Abort path")
            .expect("the runbook must carry an abort path")
            .1;
        assert!(
            abort.contains("kubectl apply -f helm-release-acme.yaml"),
            "the abort path must restore the release record from the backup:\n{abort}"
        );
        assert!(
            abort.contains("app.kubernetes.io/managed-by=Helm"),
            "the abort path must hand the managed-by label back to helm:\n{abort}"
        );
        assert!(
            abort.contains("meta.helm.sh/release-name=acme")
                && abort.contains("meta.helm.sh/release-namespace=acme"),
            "the abort path must restore both helm annotations:\n{abort}"
        );
        assert!(
            abort.contains("helm install acme"),
            "the abort path must name the no-backup fallback:\n{abort}"
        );
    }
}
