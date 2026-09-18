//! #4074: the query server's "is this name a managed index" gate reads the
//! bounded-staleness table cache, never `load_table`.
//!
//! The proof is a negative control rather than a cache-hit count: registration
//! already warms the same cache, so hits alone cannot tell a cached read from a
//! cached read PLUS a redundant uncached one. Here the table's metadata.json is
//! removed from the warehouse after the cache is warm, which leaves the catalog
//! row (what `table_exists` reads) intact and makes any real `load_table` fail.
//! `is_managed_index` must still answer; `get_index` — the gate this replaced —
//! must not.

use siglake_core::index_config::IndexConfig;
use siglake_storage::iceberg::IcebergContext;

fn index_config(index_id: &str) -> IndexConfig {
    IndexConfig {
        index_id: index_id.to_string(),
        doc_mapping: IndexConfig::builtin_events().doc_mapping,
        retention: None,
        index_at_flush: None,
    }
}

fn index_config_with_defaults(index_id: &str, fields: &[&str]) -> IndexConfig {
    let mut config = index_config(index_id);
    config.doc_mapping.default_search_fields = fields.iter().map(|field| (*field).into()).collect();
    config
}

/// Delete every metadata file of `index_id`'s table, so a `load_table` for it
/// can only fail. Returns how many files went.
fn strip_table_metadata(warehouse: &std::path::Path, index_id: &str) -> usize {
    let mut removed = 0;
    for namespace in std::fs::read_dir(warehouse).unwrap() {
        let table_metadata = namespace.unwrap().path().join(index_id).join("metadata");
        if !table_metadata.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(&table_metadata).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|ext| ext == "json") {
                std::fs::remove_file(&path).unwrap();
                removed += 1;
            }
        }
    }
    removed
}

#[tokio::test]
async fn managed_index_classification_answers_without_a_metadata_read() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        // Long enough that nothing in this test expires; the point is which
        // code path the answer comes from, not how long an entry lives.
        .with_table_cache_ttl(std::time::Duration::from_secs(3600));
    ice.create_index(&index_config("gate-idx")).await.unwrap();

    // Warm exactly as a first query would: this call is allowed to load.
    assert!(ice.is_managed_index("gate-idx").await.unwrap());

    let removed = strip_table_metadata(&warehouse, "gate-idx");
    assert!(
        removed > 0,
        "the fixture must actually remove the table's metadata, or it proves nothing"
    );

    // The old gate: an uncached `load_table` on the warm path, and it is now
    // reading a file that is not there. Its failure is what makes the assertion
    // below meaningful — `distributed_inner` swallowed this error into
    // "not distributable", so on the old gate a warm index query stopped
    // fanning out here.
    assert!(
        ice.get_index("gate-idx").await.is_err(),
        "get_index must reach storage; if it can answer from a cache this fixture no \
         longer separates the two paths"
    );

    // The new gate: catalog row + cached metadata, so it still answers.
    assert!(
        ice.is_managed_index("gate-idx").await.unwrap(),
        "warm classification must not need a metadata read"
    );
}

#[tokio::test]
async fn classification_matches_get_index_on_the_cases_that_are_not_indexes() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    ice.create_index(&index_config("present")).await.unwrap();

    // A name that is no table in this namespace.
    assert!(!ice.is_managed_index("absent").await.unwrap());
    assert!(ice.get_index("absent").await.unwrap().is_none());

    // The canonical events table classifies as distributable, as it did through
    // `get_index` (`index_config_from_table` hands back the builtin mapping).
    assert!(ice.is_managed_index("events").await.unwrap());

    // A dropped index stops classifying at once — the catalog row goes, so no
    // cache entry within its TTL can resurrect it.
    assert!(ice.delete_index("present").await.unwrap());
    assert!(!ice.is_managed_index("present").await.unwrap());
}

#[tokio::test]
async fn cached_config_is_tenant_scoped_and_tracks_mapping_commits() {
    let tmp = tempfile::tempdir().unwrap();
    let root = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_table_cache_ttl(std::time::Duration::from_secs(3600));
    let tenant_a = root.for_namespace("tenant_a").await.unwrap();
    let tenant_b = root.for_namespace("tenant_b").await.unwrap();
    tenant_a
        .create_index(&index_config_with_defaults("shared", &["raw"]))
        .await
        .unwrap();
    tenant_b
        .create_index(&index_config_with_defaults("shared", &["host"]))
        .await
        .unwrap();

    assert_eq!(
        tenant_a
            .cached_index_config("shared")
            .await
            .unwrap()
            .unwrap()
            .doc_mapping
            .default_search_fields,
        ["raw"]
    );
    assert_eq!(
        tenant_b
            .cached_index_config("shared")
            .await
            .unwrap()
            .unwrap()
            .doc_mapping
            .default_search_fields,
        ["host"]
    );

    let updated = index_config_with_defaults("shared", &["raw", "host"]);
    tenant_a.update_index(&updated).await.unwrap();
    assert_eq!(
        tenant_a
            .cached_index_config("shared")
            .await
            .unwrap()
            .unwrap()
            .doc_mapping
            .default_search_fields,
        ["raw", "host"],
        "the mapping commit must invalidate the cached config"
    );
    assert_eq!(
        tenant_b
            .cached_index_config("shared")
            .await
            .unwrap()
            .unwrap()
            .doc_mapping
            .default_search_fields,
        ["host"],
        "one tenant's invalidation must not change another tenant's mapping"
    );
}

#[tokio::test]
async fn cached_config_does_not_resurrect_a_dropped_incarnation() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_table_cache_ttl(std::time::Duration::from_secs(3600));
    ice.create_index(&index_config_with_defaults("replace-me", &["raw"]))
        .await
        .unwrap();
    assert!(ice
        .cached_index_config("replace-me")
        .await
        .unwrap()
        .is_some());

    assert!(ice.delete_index("replace-me").await.unwrap());
    assert!(ice
        .cached_index_config("replace-me")
        .await
        .unwrap()
        .is_none());

    ice.create_index(&index_config_with_defaults("replace-me", &["host"]))
        .await
        .unwrap();
    assert_eq!(
        ice.cached_index_config("replace-me")
            .await
            .unwrap()
            .unwrap()
            .doc_mapping
            .default_search_fields,
        ["host"],
        "the replacement must not reuse the dropped incarnation's cached mapping"
    );
}
