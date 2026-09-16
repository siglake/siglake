//! Structural invariants over both generated OpenAPI documents.
//!
//! These walk the serialized JSON rather than utoipa's typed model, so they
//! are independent of the crate's internal struct layout, and they are where
//! the "an endpoint cannot ship undocumented" guarantee actually lives —
//! `route_table_is_exact` in particular guards against the `routes!`
//! cross-product footgun (one `routes!` call with two paths silently registers
//! a method×path cross-product in axum while the doc shows only the real ops).

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

const METHODS: &[&str] = &[
    "get", "put", "post", "delete", "options", "head", "patch", "trace",
];

fn ingest() -> Value {
    serde_json::to_value(siglake_ingest::openapi()).unwrap()
}

fn query() -> Value {
    serde_json::to_value(siglake_query_server::openapi()).unwrap()
}

fn specs() -> [(&'static str, Value); 2] {
    [("ingest", ingest()), ("query", query())]
}

/// Collect every `path -> sorted methods` entry from a spec's `paths` map.
fn route_table(spec: &Value) -> BTreeMap<String, Vec<String>> {
    spec["paths"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(path, item)| {
            let mut methods: Vec<String> = item
                .as_object()
                .unwrap()
                .keys()
                .filter(|k| METHODS.contains(&k.as_str()))
                .cloned()
                .collect();
            methods.sort();
            (path.clone(), methods)
        })
        .collect()
}

/// Walk every `$ref` string anywhere in the document.
fn refs(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                if k == "$ref" {
                    if let Some(s) = v.as_str() {
                        out.push(s.to_string());
                    }
                } else {
                    refs(v, out);
                }
            }
        }
        Value::Array(items) => items.iter().for_each(|v| refs(v, out)),
        _ => {}
    }
}

/// Every `$ref: '#/components/schemas/Foo'` must resolve to a declared schema.
/// Catches a `body = Foo` / `value_type = Foo` whose schema was never collected.
#[test]
fn all_refs_resolve() {
    for (name, spec) in specs() {
        let declared: BTreeSet<String> = spec["components"]["schemas"]
            .as_object()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        let mut found = Vec::new();
        refs(&spec, &mut found);
        for r in found {
            let Some(schema) = r.strip_prefix("#/components/schemas/") else {
                panic!("{name}: unexpected $ref target `{r}`");
            };
            assert!(
                declared.contains(schema),
                "{name}: $ref `{r}` has no matching component (declared: {declared:?})"
            );
        }
    }
}

/// No axum catch-all spelling leaks into a path key. `{*tail}` is what
/// `utoipa-axum` derives a catch-all route from and is not legal OpenAPI path
/// templating, so a documented catch-all needs its key rewritten to `{tail}`
/// before serialization. No documented route is a catch-all today — the only
/// one, `_cat`, is registered outside the document — so a failure here means a
/// new one arrived and needs that rewrite.
#[test]
fn no_axum_wildcards_leak() {
    for (name, spec) in specs() {
        for path in spec["paths"].as_object().unwrap().keys() {
            assert!(
                !path.contains("{*"),
                "{name}: path `{path}` still carries an axum catch-all"
            );
        }
    }
}

/// Every `{param}` in a path template has a matching `in: path` parameter on
/// each operation. Catches a mistyped or missing `params((\"id\" = ..))`.
#[test]
fn path_params_are_declared() {
    for (name, spec) in specs() {
        for (path, item) in spec["paths"].as_object().unwrap() {
            let expected: BTreeSet<String> = path
                .split('/')
                .filter_map(|seg| seg.strip_prefix('{').and_then(|s| s.strip_suffix('}')))
                .map(|s| s.to_string())
                .collect();
            if expected.is_empty() {
                continue;
            }
            for (method, op) in item.as_object().unwrap() {
                if !METHODS.contains(&method.as_str()) {
                    continue;
                }
                let declared: BTreeSet<String> = op
                    .get("parameters")
                    .and_then(|p| p.as_array())
                    .map(|params| {
                        params
                            .iter()
                            .filter(|p| p["in"] == "path")
                            .filter_map(|p| p["name"].as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                assert!(
                    expected.is_subset(&declared),
                    "{name}: {method} {path} is missing path params: expected {expected:?}, \
                     declared {declared:?}"
                );
            }
        }
    }
}

/// Exactly the intended probe operations opt out of the global bearer security
/// requirement; everything else inherits it. An operation opts out by emitting
/// `security: [{}]` (an empty requirement).
#[test]
fn only_probes_are_unauthenticated() {
    // path -> methods that are allowed to be unauthenticated.
    let expected: BTreeMap<&str, BTreeMap<&str, &[&str]>> = BTreeMap::from([
        (
            "ingest",
            BTreeMap::from([("/healthz", &["get"][..]), ("/readyz", &["get"][..])]),
        ),
        (
            "query",
            BTreeMap::from([("/healthz", &["get"][..]), ("/readyz", &["get"][..])]),
        ),
    ]);

    for (name, spec) in specs() {
        for (path, item) in spec["paths"].as_object().unwrap() {
            for (method, op) in item.as_object().unwrap() {
                if !METHODS.contains(&method.as_str()) {
                    continue;
                }
                // An empty security requirement object means "no auth".
                let opts_out = op
                    .get("security")
                    .and_then(|s| s.as_array())
                    .map(|reqs| {
                        reqs.iter()
                            .any(|r| r.as_object().is_some_and(|o| o.is_empty()))
                    })
                    .unwrap_or(false);
                let allowed = expected[name]
                    .get(path.as_str())
                    .is_some_and(|ms| ms.contains(&method.as_str()));
                assert_eq!(
                    opts_out, allowed,
                    "{name}: {method} {path} unauthenticated={opts_out}, expected {allowed}"
                );
            }
        }
    }
}

/// `X-Scope-OrgID` is declared exactly on the operations whose handler resolves
/// a tenant from it, and nowhere else.
///
/// Every ingest route sits behind `ingest_auth_middleware`, which validates the
/// header, but only these five let it change the answer: four write into the
/// named tenant, and `GET /api/v1/stream` *reads* from it — an omitted header
/// there silently subscribes to `default`'s events, with a 200, keep-alives and
/// a stream that never completes to say otherwise. Nothing in the exchange
/// surfaces the mistake, so the spec is where a client has to learn it, which is
/// why this is pinned both ways: a new tenanted route must declare the header,
/// and a route that ignores the tenant must not claim to honour it.
///
/// Re-decided 2026-09-06 and left as it stands: the routes that ignore the
/// tenant still do not declare the parameter, even though the middleware can
/// refuse them over it. Declaring it there would say "send me this and I will
/// honour it", which is exactly false. The refusal is documented as a *response*
/// instead — see `middleware_refusals_are_declared`.
#[test]
fn tenant_header_declaration_is_exact() {
    // (spec, path, methods) that must declare the header. The query server is
    // tenant-scoped too, but never from a header — its tenant comes from the
    // verified JWT claim and a client cannot name one, so no query operation
    // belongs in this table. If a header-tenanted query route ever appears,
    // extend it.
    let expected: BTreeSet<(&str, &str, &str)> = BTreeSet::from([
        ("ingest", "/v1/logs", "post"),
        ("ingest", "/v1/traces", "post"),
        ("ingest", "/api/v1/_elastic/_bulk", "post"),
        ("ingest", "/api/v1/_elastic/{index}/_bulk", "post"),
        ("ingest", "/api/v1/stream", "get"),
    ]);

    let mut actual: BTreeSet<(&str, String, String)> = BTreeSet::new();
    for (name, spec) in specs() {
        for (path, item) in spec["paths"].as_object().unwrap() {
            for (method, op) in item.as_object().unwrap() {
                if !METHODS.contains(&method.as_str()) {
                    continue;
                }
                let Some(param) =
                    op.get("parameters")
                        .and_then(|p| p.as_array())
                        .and_then(|params| {
                            params
                                .iter()
                                .find(|p| p["in"] == "header" && p["name"] == "x-scope-orgid")
                        })
                else {
                    continue;
                };
                let where_ = format!("{name}: {method} {path}");
                // Optional: a required tenant header would break every existing
                // client, and the server does not enforce one.
                assert_ne!(
                    param["required"], true,
                    "{where_}: x-scope-orgid must stay optional"
                );
                let description = param["description"].as_str().unwrap_or_default();
                assert!(
                    description.contains("`default`"),
                    "{where_}: x-scope-orgid description must say what an absent header \
                     resolves to, got {description:?}"
                );
                if path == "/api/v1/stream" {
                    assert!(
                        description.contains("delivers"),
                        "{where_}: the stream's x-scope-orgid description must say the header \
                         selects whose events are delivered, got {description:?}"
                    );
                }
                actual.insert((name, path.clone(), method.clone()));
            }
        }
    }

    let actual: BTreeSet<(&str, &str, &str)> = actual
        .iter()
        .map(|(n, p, m)| (*n, p.as_str(), m.as_str()))
        .collect();
    assert_eq!(
        actual, expected,
        "the set of operations declaring x-scope-orgid drifted from the pinned set"
    );
}

/// Every ingest operation behind the two ingest middlewares declares all four
/// statuses they can answer with — `400`, `401`, `403`, `429` — in the
/// middleware's own envelopes.
///
/// `ingest_auth_middleware` validates credentials, `X-Scope-OrgID` and the JWT
/// tenant claim ahead of *every* route in `ingest_routes()`, not just the
/// tenant-consuming ones, and `hec_rate_limit_middleware` is a `route_layer`
/// over the same router that runs ahead of even the auth check. So a shipper
/// that only ever calls `GET /` or `/_cluster/health` can be answered `400` over
/// a header it never knew was inspected, `401` on a path that looks anonymous,
/// or `429` before it has sent a single event. Since those routes deliberately
/// do not declare the tenant parameter (`tenant_header_declaration_is_exact`),
/// the response list is the only place the spec can say so — hence pinned here.
///
/// Bodies are checked on `401`, `403` and `429`: those are always the
/// middleware's. A `400` can also come from a handler — the two `_bulk` routes
/// answer their own parse failures in the ES envelope — so only its presence is
/// pinned, and that divergence is stated in their `400` descriptions rather than
/// papered over with a second declared body.
///
/// `HEAD /` declares the statuses and the `retry-after` header but NO bodies, and
/// is held to that here: a `HEAD` response carries no content, so a declared
/// schema would promise something the caller cannot read. `GET /` documents the
/// envelopes for both.
///
/// `/healthz` and `/readyz` are the exemptions: they sit in `public_routes()`,
/// outside both layers, so they are neither authenticated, tenant-checked nor
/// rate-limited.
#[test]
fn middleware_refusals_are_declared() {
    const EXEMPT: &[&str] = &["/healthz", "/readyz"];
    const STATUSES: &[&str] = &["400", "401", "403", "429"];
    let spec = ingest();
    let mut checked = 0;
    for (path, item) in spec["paths"].as_object().unwrap() {
        if EXEMPT.contains(&path.as_str()) {
            continue;
        }
        for (method, op) in item.as_object().unwrap() {
            if !METHODS.contains(&method.as_str()) {
                continue;
            }
            let where_ = format!("ingest: {method} {path}");
            let responses = &op["responses"];
            for status in STATUSES {
                assert!(
                    responses.get(status).is_some(),
                    "{where_}: must declare {status}; the ingest middlewares can answer it \
                     before the handler runs"
                );
            }
            assert!(
                responses["429"]["headers"].get("retry-after").is_some(),
                "{where_}: the 429 must declare `retry-after`; the rate limiter always sets \
                 it, and it is the only part of the refusal a client can act on"
            );
            if method == "head" {
                for status in STATUSES {
                    assert!(
                        responses[status].get("content").is_none(),
                        "{where_}: a HEAD response carries no content, so {status} must \
                         declare a status and headers only — see `GET /` for the envelope"
                    );
                }
                checked += 1;
                continue;
            }
            for (status, schema) in [
                ("401", "SiglakeErrorBody"),
                ("403", "SiglakeErrorBody"),
                ("429", "RateLimitBody"),
            ] {
                assert_eq!(
                    responses[status]["content"]["application/json"]["schema"]["$ref"],
                    Value::String(format!("#/components/schemas/{schema}")),
                    "{where_}: the {status} comes from the middleware, so its body is \
                     `{schema}`"
                );
            }
            checked += 1;
        }
    }
    // Keep the exemption list from rotting into a name that no longer routes.
    for path in EXEMPT {
        assert!(
            spec["paths"].get(path).is_some(),
            "ingest: exempt path `{path}` is gone; drop it from EXEMPT"
        );
    }
    assert!(checked > 0, "ingest: no operations were checked");
}

/// Every query operation behind `auth::middleware` declares both statuses that
/// middleware can answer with — `401` and `403` — in the query envelope.
///
/// The `403` is the one worth pinning. It is new: a verified token whose
/// configured tenant claim is missing, blank, non-string or not a usable tenant
/// id used to be ACCEPTED and routed to the default namespace (or, after the
/// registry dropped the offending characters, to somebody else's). It is now
/// refused before any handler runs, on every route behind the layer — including
/// the ones that read no tenant data at all, like `/debug/memory-pool`, which is
/// exactly why a client cannot infer the refusal from the path and the spec has
/// to say so.
///
/// `/healthz` and `/readyz` are the exemptions: they sit in `public_routes()`,
/// outside the layer, so they are neither authenticated nor tenant-checked.
#[test]
fn query_middleware_refusals_are_declared() {
    const EXEMPT: &[&str] = &["/healthz", "/readyz"];
    let spec = query();
    let mut checked = 0;
    for (path, item) in spec["paths"].as_object().unwrap() {
        if EXEMPT.contains(&path.as_str()) {
            continue;
        }
        for (method, op) in item.as_object().unwrap() {
            if !METHODS.contains(&method.as_str()) {
                continue;
            }
            let where_ = format!("query: {method} {path}");
            let responses = &op["responses"];
            for status in ["401", "403"] {
                assert_eq!(
                    responses[status]["content"]["application/json"]["schema"]["$ref"],
                    Value::String("#/components/schemas/ApiErrorBody".into()),
                    "{where_}: the auth middleware can answer {status} before the handler \
                     runs, in the query error envelope"
                );
            }
            let description = responses["403"]["description"].as_str().unwrap_or_default();
            assert!(
                description.contains("claim"),
                "{where_}: the 403 description must name the JWT tenant claim as the reason, \
                 so a client knows re-authenticating will not help; got {description:?}"
            );
            checked += 1;
        }
    }
    // Keep the exemption list from rotting into a name that no longer routes.
    for path in EXEMPT {
        assert!(
            spec["paths"].get(path).is_some(),
            "query: exempt path `{path}` is gone; drop it from EXEMPT"
        );
    }
    assert!(checked > 0, "query: no operations were checked");
}

/// The ES read surface stays out of the ingest document, and the refusal stays
/// in the `elasticsearch-compat` tag.
///
/// The one deliberate exception to "a route that exists is documented"
/// (`docs/api/README.md`): the `_search` family routes and answers `501`, for
/// stray ES clients that deserve a definitive answer rather than a `404`, but no
/// ES query API is planned and fifteen documented `not implemented` operations
/// read as a surface under construction. Pinned in both directions — a path back
/// in the document fails here, and so does the sentence going missing from the
/// tag, which is the only place the document still says what those paths do.
#[test]
fn es_read_routes_stay_undocumented() {
    const READ_MARKERS: &[&str] = &["_search", "_msearch", "_field_caps", "_cat"];
    let spec = ingest();
    for path in spec["paths"].as_object().unwrap().keys() {
        for marker in READ_MARKERS {
            assert!(
                !path.contains(marker),
                "ingest: `{path}` documents an ES read route; they answer 501 and are \
                 registered with a plain `route` call in `es_read_stubs` so they stay out \
                 of the spec"
            );
        }
    }

    let tag = spec["tags"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "elasticsearch-compat")
        .expect("ingest: the elasticsearch-compat tag is gone");
    let description = tag["description"].as_str().unwrap_or_default();
    for expected in ["_search", "_msearch", "_field_caps", "_cat", "501", "sql"] {
        assert!(
            description.contains(expected),
            "ingest: the elasticsearch-compat tag is where the undocumented ES read routes \
             are accounted for, so its description must name `{expected}`; got \
             {description:?}"
        );
    }
}

/// The full route table of each server, pinned exactly. This is the check that
/// makes "undocumented endpoints impossible" real rather than aspirational:
/// adding, removing, or re-verbing a route is a deliberate edit here. It also
/// catches the `routes!` cross-product footgun, since a leaked method would
/// show up as an extra verb on some path.
#[test]
fn route_table_is_exact() {
    let ingest_expected: BTreeMap<&str, &[&str]> = BTreeMap::from([
        ("/", &["get", "head"][..]),
        ("/healthz", &["get"][..]),
        ("/readyz", &["get"][..]),
        ("/_cluster/health", &["get"][..]),
        ("/v1/logs", &["post"][..]),
        ("/v1/traces", &["post"][..]),
        ("/api/v1/stream", &["get"][..]),
        ("/api/v1/_elastic/_cluster/health", &["get"][..]),
        ("/api/v1/_elastic/_bulk", &["post"][..]),
        ("/api/v1/_elastic/{index}/_bulk", &["post"][..]),
        // The ES read paths route but are not documented — see
        // `es_read_routes_stay_undocumented`.
    ]);

    let query_expected: BTreeMap<&str, &[&str]> = BTreeMap::from([
        ("/healthz", &["get"][..]),
        ("/readyz", &["get"][..]),
        ("/debug/memory-pool", &["get"][..]),
        ("/api/v1/indexes", &["get", "post"][..]),
        ("/api/v1/indexes/{id}", &["delete", "get", "put"][..]),
        ("/api/v1/index-templates", &["get"][..]),
        ("/api/v1/index-templates/{id}", &["delete", "put"][..]),
        ("/api/v1/delete-tasks", &["get", "post"][..]),
        ("/api/v1/delete-tasks/{id}", &["get"][..]),
        ("/api/v1/sql", &["post"][..]),
        ("/api/v1/sql/explain", &["post"][..]),
        ("/api/v1/sql/local", &["post"][..]),
        ("/api/v1/sql/distributed", &["post"][..]),
        ("/api/v1/sql/shard", &["post"][..]),
        ("/api/v1/jobs/{id}", &["delete", "get"][..]),
        ("/api/v1/jobs/{id}/result", &["get"][..]),
        ("/api/v1/jaeger/{index}/api/services", &["get"][..]),
        (
            "/api/v1/jaeger/{index}/api/services/{service}/operations",
            &["get"][..],
        ),
        ("/api/v1/jaeger/{index}/api/traces", &["get"][..]),
        ("/api/v1/jaeger/{index}/api/traces/{trace_id}", &["get"][..]),
    ]);

    for (name, spec, expected) in [
        ("ingest", ingest(), ingest_expected),
        ("query", query(), query_expected),
    ] {
        let actual = route_table(&spec);
        let expected: BTreeMap<String, Vec<String>> = expected
            .into_iter()
            .map(|(p, ms)| (p.to_string(), ms.iter().map(|s| s.to_string()).collect()))
            .collect();
        assert_eq!(
            actual, expected,
            "{name}: route table drifted from the pinned set"
        );
    }
}
