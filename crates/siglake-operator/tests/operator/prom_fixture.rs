//! Evaluate the operator's generated per-pod load queries with Prometheus.
//!
//! The ingester request counter and compactor backlog gauge each carry labels
//! that let one pod publish several series, and a catalog-claim compactor fleet
//! publishes one shared queue depth once per worker. So the ingester query must
//! sum each pod's series before averaging pod totals, and the compactor query
//! must read the queue once whatever the replica count. Whether the queries
//! actually answer those contracts is a question about PromQL, not Rust: it
//! depends on how `avg`, `sum by` and `rate` treat those series. So it is
//! answered by Prometheus' own engine, the way `scripts/check-chart.py` hands
//! the rendered PrometheusRule to `promtool test rules`.
//!
//! The expression under test comes from [`Queries::defaults`], not from a copy
//! of it here — the point is to exercise what the operator sends. The pre-fix
//! formula is kept beside it as a falsifier: if a fixture cannot tell the two
//! apart, it is not evidence.
//!
//! `promtool` is not a build dependency of this workspace, so a box without it
//! SKIPS. CI's `test` job installs it and sets `SIGLAKE_OPERATOR_REQUIRE_PROMTOOL=1`,
//! which turns the skip into a failure there.

use std::process::Command;

use siglake_operator::prom::Queries;

const REQUIRE_PROMTOOL_ENV: &str = "SIGLAKE_OPERATOR_REQUIRE_PROMTOOL";

/// Whether a missing `promtool` must fail this test rather than skip it.
///
/// Pure so the environment policy is tested without mutating process-global
/// state shared by parallel tests.
fn require_promtool_from(configured: Option<&str>) -> bool {
    configured.map(str::trim) == Some("1")
}

fn require_promtool() -> bool {
    require_promtool_from(std::env::var(REQUIRE_PROMTOOL_ENV).ok().as_deref())
}

#[test]
fn require_promtool_is_opt_in() {
    assert!(!require_promtool_from(None));
    assert!(!require_promtool_from(Some("")));
    assert!(!require_promtool_from(Some("0")));
    assert!(!require_promtool_from(Some("true")));
    assert!(require_promtool_from(Some("1")));
    assert!(require_promtool_from(Some(" 1 ")));
}

/// The reconciled namespace and the release the fixture's series belong to.
const NAMESPACE: &str = "logs";
const RELEASE: &str = "siglake";

/// Seconds between fixture samples.
///
/// `rate(...[1m])` needs at least two samples inside the window, and its
/// extrapolation to the window edges is what turns the sampled delta into a
/// per-second value. At 20s the window holds three samples spanning 40s and the
/// factor is exactly 60/40, so every expected value below is an exact integer
/// rather than a float that misses promtool's exact comparison by 1e-14.
const STEP_SECS: u64 = 20;

/// Samples per series: 10 minutes of them, well past [`EVAL_TIME`].
const SAMPLES: u64 = 30;

/// When the expression is evaluated. Far enough in that every series has a
/// full `rate` window behind it.
const EVAL_TIME: &str = "5m";

/// One ingest counter series: the pod that publishes it, the label combination
/// that separates it from its siblings, and its steady request rate.
struct Series {
    pod: &'static str,
    endpoint: &'static str,
    status: &'static str,
    tenant: &'static str,
    index: &'static str,
    rps: u64,
}

struct Case {
    name: &'static str,
    /// The namespace the series carry; the selector's namespace is
    /// [`NAMESPACE`], so anything else must match nothing.
    namespace: &'static str,
    series: Vec<Series>,
    /// What the operator's query must read, or `None` when it must return an
    /// empty vector (which `usable_sample` refuses rather than reading as an
    /// idle fleet).
    expect: Option<u64>,
    /// What the pre-fix `avg(rate(...))` reads for the same traffic. Present
    /// only to show these fixtures distinguish the two formulas.
    expect_pre_fix: Option<u64>,
}

fn logs(pod: &'static str, tenant: &'static str, rps: u64) -> Series {
    Series {
        pod,
        endpoint: "/v1/logs",
        status: "200",
        tenant,
        index: "logs",
        rps,
    }
}

fn traces(pod: &'static str, tenant: &'static str, rps: u64) -> Series {
    Series {
        pod,
        endpoint: "/v1/traces",
        status: "200",
        tenant,
        index: "traces",
        rps,
    }
}

fn cases() -> Vec<Case> {
    vec![
        // One pod, two endpoints, 60 rps each. The pod is doing 120 requests
        // per second and that is the number the target is compared against.
        Case {
            name: "one pod's label combinations add up to its request rate",
            namespace: NAMESPACE,
            series: vec![
                logs("ingester-0", "acme", 60),
                traces("ingester-0", "acme", 60),
            ],
            expect: Some(120),
            expect_pre_fix: Some(60),
        },
        // The same 120 rps on the same single pod, spread over two tenants and
        // a second status. Cardinality is not load: the reading may not move.
        Case {
            name: "splitting the same pod load over more series does not move the reading",
            namespace: NAMESPACE,
            series: vec![
                logs("ingester-0", "acme", 30),
                traces("ingester-0", "acme", 30),
                logs("ingester-0", "globex", 30),
                Series {
                    pod: "ingester-0",
                    endpoint: "/v1/logs",
                    status: "429",
                    tenant: "globex",
                    index: "logs",
                    rps: 30,
                },
            ],
            expect: Some(120),
            expect_pre_fix: Some(30),
        },
        // Two pods with the same traffic shape but different label spreads:
        // 120 rps on one series, and 60 rps spread over four. Each pod is one
        // reading, so the fleet reads (120 + 60) / 2.
        Case {
            name: "pods are weighted equally however many series each publishes",
            namespace: NAMESPACE,
            series: vec![
                logs("ingester-0", "acme", 120),
                logs("ingester-1", "acme", 15),
                traces("ingester-1", "acme", 15),
                logs("ingester-1", "globex", 15),
                traces("ingester-1", "globex", 15),
            ],
            expect: Some(90),
            expect_pre_fix: Some(36),
        },
        // A fleet whose series the selector does not match reads as nothing at
        // all, not as zero load.
        Case {
            name: "a selector that matches no series yields an empty vector",
            namespace: "other-tenant",
            series: vec![logs("ingester-0", "acme", 60)],
            expect: None,
            expect_pre_fix: None,
        },
    ]
}

/// Render the promtool unit-test file for `expr` and its pre-fix control.
fn ingester_fixture(expr: &str, pre_fix: &str) -> String {
    let mut out = String::from("evaluation_interval: 1m\ntests:\n");
    for case in cases() {
        out.push_str(&format!(
            "  - name: {}\n    interval: {STEP_SECS}s\n    input_series:\n",
            case.name
        ));
        for s in &case.series {
            out.push_str(&format!(
                "      - series: 'siglake_ingest_requests_total{{namespace=\"{ns}\",\
                 app_kubernetes_io_instance=\"{RELEASE}\",app_kubernetes_io_component=\"ingester\",\
                 pod=\"{pod}\",endpoint=\"{endpoint}\",status=\"{status}\",tenant=\"{tenant}\",\
                 index=\"{index}\"}}'\n        values: '0+{delta}x{SAMPLES}'\n",
                ns = case.namespace,
                pod = s.pod,
                endpoint = s.endpoint,
                status = s.status,
                tenant = s.tenant,
                index = s.index,
                delta = s.rps * STEP_SECS,
            ));
        }
        out.push_str("    promql_expr_test:\n");
        out.push_str(&expr_test(expr, case.expect));
        out.push_str(&expr_test(pre_fix, case.expect_pre_fix));
    }
    out
}

/// One compactor backlog gauge series.
struct CompactorSeries {
    pod: &'static str,
    tenant: &'static str,
    pending: u64,
}

struct CompactorCase {
    name: &'static str,
    /// The namespace the series carry; see [`Case::namespace`].
    namespace: &'static str,
    series: Vec<CompactorSeries>,
    expect: Option<u64>,
    expect_pre_fix: Option<u64>,
    /// What a bare `sum` of the gauge reads for the same series — the fleet
    /// total. Present as the second control: under the catalog claim it counts
    /// one shared queue once per worker publishing it.
    expect_fleet_sum: Option<u64>,
}

fn pending(pod: &'static str, tenant: &'static str, pending: u64) -> CompactorSeries {
    CompactorSeries {
        pod,
        tenant,
        pending,
    }
}

fn compactor_cases() -> Vec<CompactorCase> {
    vec![
        CompactorCase {
            name: "one pod's tenant backlogs add up to its queue depth",
            namespace: NAMESPACE,
            series: vec![
                pending("compactor-0", "acme", 3),
                pending("compactor-0", "globex", 5),
            ],
            expect: Some(8),
            expect_pre_fix: Some(4),
            expect_fleet_sum: Some(8),
        },
        CompactorCase {
            name: "splitting the same queue depth over more tenants does not move the reading",
            namespace: NAMESPACE,
            series: vec![
                pending("compactor-0", "acme", 1),
                pending("compactor-0", "globex", 2),
                pending("compactor-0", "initech", 2),
                pending("compactor-0", "umbrella", 3),
            ],
            expect: Some(8),
            expect_pre_fix: Some(2),
            expect_fleet_sum: Some(8),
        },
        CompactorCase {
            name: "unequal pod queue depths are averaged across replicas",
            namespace: NAMESPACE,
            series: vec![
                pending("compactor-0", "acme", 3),
                pending("compactor-0", "globex", 5),
                pending("compactor-1", "acme", 10),
                pending("compactor-1", "globex", 20),
                pending("compactor-1", "initech", 2),
            ],
            expect: Some(20),
            expect_pre_fix: Some(8),
            expect_fleet_sum: Some(40),
        },
        // #3692. What a catalog-claim fleet actually publishes: `peek_pending`
        // counts the whole sealed table with no worker filter and labels it
        // `tenant="default"`, so every replica carries the same total. The
        // reading must be the queue, not the fleet's copies of it — the third
        // arm below is the 24 a `sum` would hand the decision, which at a target
        // of 4 asks for six workers instead of two.
        CompactorCase {
            name: "a shared claim queue is read once, not once per worker",
            namespace: NAMESPACE,
            series: vec![
                pending("compactor-0", "default", 8),
                pending("compactor-1", "default", 8),
                pending("compactor-2", "default", 8),
            ],
            expect: Some(8),
            expect_pre_fix: Some(8),
            expect_fleet_sum: Some(24),
        },
        // The same queue with one worker's scrape a cycle behind the others:
        // the average is between the copies, never their sum.
        CompactorCase {
            name: "a skewed shared claim queue stays within the copies it averages",
            namespace: NAMESPACE,
            series: vec![
                pending("compactor-0", "default", 8),
                pending("compactor-1", "default", 8),
                pending("compactor-2", "default", 5),
            ],
            expect: Some(7),
            expect_pre_fix: Some(7),
            expect_fleet_sum: Some(21),
        },
        CompactorCase {
            name: "an unmatched compactor selector yields an empty vector",
            namespace: "other-tenant",
            series: vec![pending("compactor-0", "acme", 8)],
            expect: None,
            expect_pre_fix: None,
            expect_fleet_sum: None,
        },
    ]
}

/// Render gauge cases for the generated compactor query and its two controls.
fn compactor_fixture(expr: &str, pre_fix: &str, fleet_sum: &str) -> String {
    let mut out = String::from("evaluation_interval: 1m\ntests:\n");
    for case in compactor_cases() {
        out.push_str(&format!(
            "  - name: {}\n    interval: 5m\n    input_series:\n",
            case.name
        ));
        for s in &case.series {
            out.push_str(&format!(
                "      - series: 'siglake_compactor_sealed_pending{{namespace=\"{ns}\",\
                 app_kubernetes_io_instance=\"{RELEASE}\",app_kubernetes_io_component=\"compactor\",\
                 pod=\"{pod}\",tenant=\"{tenant}\"}}'\n        values: '_ {pending}'\n",
                ns = case.namespace,
                pod = s.pod,
                tenant = s.tenant,
                pending = s.pending,
            ));
        }
        out.push_str("    promql_expr_test:\n");
        out.push_str(&expr_test(expr, case.expect));
        out.push_str(&expr_test(pre_fix, case.expect_pre_fix));
        out.push_str(&expr_test(fleet_sum, case.expect_fleet_sum));
    }
    out
}

fn expr_test(expr: &str, expect: Option<u64>) -> String {
    let samples = match expect {
        Some(v) => format!("\n          - labels: '{{}}'\n            value: {v}\n"),
        None => " []\n".to_string(),
    };
    format!("      - expr: '{expr}'\n        eval_time: {EVAL_TIME}\n        exp_samples:{samples}")
}

/// The query the operator built before #3620, kept as the control arm above.
fn pre_fix_expression() -> String {
    format!(
        "avg(rate(siglake_ingest_requests_total{{namespace=\"{NAMESPACE}\",\
         app_kubernetes_io_instance=\"{RELEASE}\",app_kubernetes_io_component=\"ingester\"}}[1m]))"
    )
}

/// The compactor query before #3620, kept as the gauge control arm.
fn compactor_pre_fix_expression() -> String {
    format!(
        "avg(siglake_compactor_sealed_pending{{namespace=\"{NAMESPACE}\",\
         app_kubernetes_io_instance=\"{RELEASE}\",app_kubernetes_io_component=\"compactor\"}})"
    )
}

/// The fleet total, as the second control arm: the reading the decision would
/// take if the fleet's copies of one shared queue were added up (#3692).
fn compactor_fleet_sum_expression() -> String {
    format!(
        "sum(siglake_compactor_sealed_pending{{namespace=\"{NAMESPACE}\",\
         app_kubernetes_io_instance=\"{RELEASE}\",app_kubernetes_io_component=\"compactor\"}})"
    )
}

fn assert_promtool_fixture(name: &str, expr: &str, contract: &str, fixture: String) {
    let dir = tempfile::tempdir().expect("temp dir for the promtool fixture");
    std::fs::write(dir.path().join(name), fixture).expect("write the promtool fixture");

    // Run from the fixture's own directory: promtool resolves the paths in a
    // unit-test file relative to the working directory.
    let out = match Command::new("promtool")
        .args(["test", "rules", name])
        .current_dir(dir.path())
        .output()
    {
        Ok(out) => out,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            assert!(
                !require_promtool(),
                "{REQUIRE_PROMTOOL_ENV}=1 but promtool is not installed, so the operator's \
                 per-pod query was never evaluated"
            );
            eprintln!("skipping: promtool is not installed, so `{expr}` was not evaluated");
            return;
        }
        Err(e) => panic!("run promtool: {e}"),
    };
    assert!(
        out.status.success(),
        "`{expr}` does not read {contract}:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[test]
fn the_ingester_query_reads_requests_per_second_per_pod() {
    let expr = Queries::defaults(RELEASE, NAMESPACE).ingester_rps;
    // A fixture both formulas satisfy proves nothing about either, so at least
    // one case has to read differently under the two.
    assert!(
        cases()
            .iter()
            .any(|c| c.expect.is_some() && c.expect != c.expect_pre_fix),
        "no case separates the per-pod query from the per-series one"
    );
    // Both expressions go into single-quoted YAML scalars, which carry the
    // double quotes of a PromQL selector but not a single quote.
    for e in [&expr, &pre_fix_expression()] {
        assert!(!e.contains('\''), "expression needs YAML escaping: {e}");
    }

    assert_promtool_fixture(
        "ingester-load.test.yaml",
        &expr,
        "requests/sec/pod",
        ingester_fixture(&expr, &pre_fix_expression()),
    );
}

#[test]
fn the_compactor_query_reads_the_sealed_queue_once() {
    let expr = Queries::defaults(RELEASE, NAMESPACE).compactor_backlog;
    assert!(
        compactor_cases()
            .iter()
            .any(|c| c.expect.is_some() && c.expect != c.expect_pre_fix),
        "no case separates the per-pod query from the per-series one"
    );
    assert!(
        compactor_cases()
            .iter()
            .any(|c| c.expect.is_some() && c.expect != c.expect_fleet_sum),
        "no case separates the per-pod query from the fleet total"
    );
    for e in [
        &expr,
        &compactor_pre_fix_expression(),
        &compactor_fleet_sum_expression(),
    ] {
        assert!(!e.contains('\''), "expression needs YAML escaping: {e}");
    }

    assert_promtool_fixture(
        "compactor-backlog.test.yaml",
        &expr,
        "the sealed queue depth, counted once however many workers publish it",
        compactor_fixture(
            &expr,
            &compactor_pre_fix_expression(),
            &compactor_fleet_sum_expression(),
        ),
    );
}

// --- #6011: the zero-floor compactor's activation reading -------------------

/// One ingester's published view of the shared catalog queue: the depth it
/// last read, and how long ago that read succeeded.
struct ActivationSeries {
    pod: &'static str,
    depth: u64,
    sample_age: u64,
}

struct ActivationCase {
    name: &'static str,
    ingesters: Vec<ActivationSeries>,
    /// What the compactor tier itself is publishing, if any pod is running.
    compactor: Vec<(&'static str, u64)>,
    expect: Option<u64>,
    /// The same reading without the `and on (pod)` staleness guard — the
    /// control arm, which reads a frozen gauge as a current one.
    expect_unguarded: Option<u64>,
}

fn activation_cases() -> Vec<ActivationCase> {
    vec![
        ActivationCase {
            name: "two ingesters publishing the same fresh depth read it once",
            ingesters: vec![
                ActivationSeries {
                    pod: "ingester-0",
                    depth: 6,
                    sample_age: 10,
                },
                ActivationSeries {
                    pod: "ingester-1",
                    depth: 6,
                    sample_age: 15,
                },
            ],
            compactor: vec![],
            expect: Some(6),
            expect_unguarded: Some(6),
        },
        // The case the guard exists for: an ingester whose catalog reads have
        // been failing keeps answering with the depth it last saw. Averaging
        // that frozen copy in halves the reading, and a frozen ZERO would read
        // as an empty queue and park the tier.
        ActivationCase {
            name: "an ingester whose catalog reads stopped is dropped, not averaged",
            ingesters: vec![
                ActivationSeries {
                    pod: "ingester-0",
                    depth: 6,
                    sample_age: 10,
                },
                ActivationSeries {
                    pod: "ingester-1",
                    depth: 0,
                    sample_age: 600,
                },
            ],
            compactor: vec![],
            expect: Some(6),
            expect_unguarded: Some(3),
        },
        // Every publisher stale and no compactor pod running: an empty vector,
        // which `usable_sample` refuses as a reading. The tier is then sized by
        // `replicas_without_signal`, which restores a zero-floor tier to one.
        ActivationCase {
            name: "every publisher stale and no compactor running is no reading at all",
            ingesters: vec![ActivationSeries {
                pod: "ingester-0",
                depth: 4,
                sample_age: 600,
            }],
            compactor: vec![],
            expect: None,
            expect_unguarded: Some(4),
        },
        // The fail-safe's other half: the tier restored to one replica
        // publishes `siglake_compactor_sealed_pending` itself, and the `or`
        // falls back to it, so the woken pod can still size its own tier while
        // the independent signal is missing.
        ActivationCase {
            name: "a stale publisher falls back to the compactor's own gauge",
            ingesters: vec![ActivationSeries {
                pod: "ingester-0",
                depth: 4,
                sample_age: 600,
            }],
            compactor: vec![("compactor-0", 9)],
            expect: Some(9),
            expect_unguarded: Some(4),
        },
        // And the fallback is consulted ONLY when the left side is empty: a
        // fresh depth wins over whatever a running compactor is publishing.
        ActivationCase {
            name: "a fresh published depth wins over the compactor's own gauge",
            ingesters: vec![ActivationSeries {
                pod: "ingester-0",
                depth: 6,
                sample_age: 10,
            }],
            compactor: vec![("compactor-0", 9)],
            expect: Some(6),
            expect_unguarded: Some(6),
        },
    ]
}

/// The activation expression without its staleness guard, as the control arm.
fn activation_unguarded_expression() -> String {
    format!(
        "avg(sum by (pod) (siglake_wal_segments_sealed{{namespace=\"{NAMESPACE}\",\
         app_kubernetes_io_instance=\"{RELEASE}\",app_kubernetes_io_component=\"ingester\"}}))"
    )
}

fn activation_fixture(expr: &str, unguarded: &str) -> String {
    let mut out = String::from("evaluation_interval: 1m\ntests:\n");
    for case in activation_cases() {
        out.push_str(&format!(
            "  - name: {}\n    interval: 5m\n    input_series:\n",
            case.name
        ));
        for s in &case.ingesters {
            out.push_str(&format!(
                "      - series: 'siglake_wal_segments_sealed{{namespace=\"{NAMESPACE}\",\
                 app_kubernetes_io_instance=\"{RELEASE}\",app_kubernetes_io_component=\"ingester\",\
                 pod=\"{pod}\",tenant=\"default\"}}'\n        values: '_ {depth}'\n",
                pod = s.pod,
                depth = s.depth,
            ));
            out.push_str(&format!(
                "      - series: 'siglake_wal_segments_sealed_sample_age_seconds{{namespace=\"{NAMESPACE}\",\
                 app_kubernetes_io_instance=\"{RELEASE}\",app_kubernetes_io_component=\"ingester\",\
                 pod=\"{pod}\",tenant=\"default\"}}'\n        values: '_ {age}'\n",
                pod = s.pod,
                age = s.sample_age,
            ));
        }
        for (pod, pending) in &case.compactor {
            out.push_str(&format!(
                "      - series: 'siglake_compactor_sealed_pending{{namespace=\"{NAMESPACE}\",\
                 app_kubernetes_io_instance=\"{RELEASE}\",app_kubernetes_io_component=\"compactor\",\
                 pod=\"{pod}\",tenant=\"default\"}}'\n        values: '_ {pending}'\n",
            ));
        }
        out.push_str("    promql_expr_test:\n");
        out.push_str(&expr_test(expr, case.expect));
        out.push_str(&expr_test(unguarded, case.expect_unguarded));
    }
    out
}

/// #6011 slice 3. The reading a `min: 0` compactor is sized by: the depth the
/// INGESTERS publish, with any publisher whose last catalog read is stale
/// dropped, falling back to the compactor's own gauge when none is left.
#[test]
fn the_activation_query_reads_the_fresh_published_depth() {
    let expr = Queries::compactor_activation(RELEASE, NAMESPACE);
    assert_eq!(
        Queries::for_policy(
            RELEASE,
            NAMESPACE,
            &siglake_operator::crd::ComponentAutoscale {
                min: 0,
                max: 4,
                target: 4.0,
            }
        )
        .compactor_backlog,
        expr,
        "a zero floor is what selects this query",
    );
    assert!(
        activation_cases()
            .iter()
            .any(|c| c.expect != c.expect_unguarded),
        "no case separates the guarded reading from the unguarded one"
    );
    for e in [&expr, &activation_unguarded_expression()] {
        assert!(!e.contains('\''), "expression needs YAML escaping: {e}");
    }

    assert_promtool_fixture(
        "compactor-activation.test.yaml",
        &expr,
        "the catalog depth the ingesters publish, ignoring stale publishers",
        activation_fixture(&expr, &activation_unguarded_expression()),
    );
}
