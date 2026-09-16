mod integration;
mod leader_chaos;
mod prom_fixture;

const REQUIRE_CLUSTER_ENV: &str = "SIGLAKE_OPERATOR_REQUIRE_CLUSTER";

/// Whether an unavailable Kubernetes client must fail the real-cluster tests.
///
/// Pure so the environment policy is tested without mutating process-global
/// state shared by parallel tests.
fn require_cluster_from(configured: Option<&str>) -> bool {
    configured.map(str::trim) == Some("1")
}

fn require_cluster() -> bool {
    require_cluster_from(std::env::var(REQUIRE_CLUSTER_ENV).ok().as_deref())
}

#[test]
fn require_cluster_is_opt_in() {
    assert!(!require_cluster_from(None));
    assert!(!require_cluster_from(Some("")));
    assert!(!require_cluster_from(Some("0")));
    assert!(!require_cluster_from(Some("true")));
    assert!(require_cluster_from(Some("1")));
    assert!(require_cluster_from(Some(" 1 ")));
}
