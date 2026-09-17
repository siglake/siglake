//! Task #3052: the opt-out and the column ceiling of opt-in WS-7 attribute
//! auto-promotion, at the table.
//!
//! Auto-promotion is the one path in the system that mutates a table's SCHEMA
//! with no operator in the loop: `auto_promote_hot_keys` decides from a bounded
//! sample and calls `declare_promotions_for`, which records the property and
//! widens the schema additively. Additive widening cannot be undone, so the
//! assertions that matter most here are the negative ones: when the pass
//! declines to promote, the table must come back identical in schema and
//! properties, not merely without a return value.
//!
//! The selection arithmetic is unit-tested in `iceberg.rs`
//! (`auto_promotion_sampling_tests`). Mixed-type keys, backfill convergence
//! and query equivalence need the `attr_get` UDF, which lives in the query
//! server: `siglake-query-server`'s `auto_promotion_bounds.rs`,
//! `promoted_prune.rs` and `typed_promotion.rs`.

use iceberg::table::Table;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

/// The sample bound every test here uses: one pass over everything these
/// fixtures write, so a verdict is about the threshold and not about which
/// files the sample happened to reach.
const FILES: usize = 8;
const ROWS: usize = 4096;

async fn events_table(ice: &IcebergContext) -> Table {
    ice.catalog()
        .load_table(ice.events_table_ident())
        .await
        .unwrap()
}

/// The table's schema column names and its promotion property — the whole of
/// what a promotion changes.
async fn schema_fingerprint(ice: &IcebergContext) -> (Vec<String>, Option<String>) {
    let table = events_table(ice).await;
    let names = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect();
    let property = table
        .metadata()
        .properties()
        .get(siglake_core::PROMOTED_PROPERTY_KEY)
        .cloned();
    (names, property)
}

fn events(n: usize, attributes: impl Fn(usize) -> String) -> Vec<Event> {
    (0..n)
        .map(|i| Event::now(format!("row {i}")).with_attributes(Some(attributes(i))))
        .collect()
}

/// The shipped configuration. A threshold of zero is the default, and it must
/// leave the table untouched — no promotion, no property, no widened schema —
/// even though the sample is full of keys that would clear any real bar.
#[tokio::test]
async fn a_zero_threshold_leaves_the_schema_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    ice.append_events(&events(200, |i| {
        format!(
            r#"{{"k8s.namespace":"prod","http.status":{}}}"#,
            200 + i % 3
        )
    }))
    .await
    .unwrap();

    let before = schema_fingerprint(&ice).await;
    assert_eq!(before.1, None, "a fresh table carries no promotions");

    let newly = ice
        .auto_promote_hot_keys(0.0, 16, FILES, ROWS)
        .await
        .unwrap();
    assert!(newly.is_empty(), "{newly:?}");
    assert_eq!(
        schema_fingerprint(&ice).await,
        before,
        "the default configuration must not widen a schema"
    );

    // Negative control: the same fixture with the feature ON does promote,
    // so the assertion above is about the threshold and not about the data.
    let newly = ice
        .auto_promote_hot_keys(0.5, 16, FILES, ROWS)
        .await
        .unwrap();
    assert_eq!(newly.len(), 2, "{newly:?}");
    assert_ne!(schema_fingerprint(&ice).await, before);
}

/// The second off switch: a zero column ceiling. An operator who wants the
/// sampling pass inert without unsetting the threshold has this, and it must
/// stop short of the declaration too.
#[tokio::test]
async fn a_zero_column_ceiling_leaves_the_schema_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    ice.append_events(&events(200, |_| r#"{"k8s.namespace":"prod"}"#.to_string()))
        .await
        .unwrap();

    let before = schema_fingerprint(&ice).await;
    let newly = ice
        .auto_promote_hot_keys(0.5, 0, FILES, ROWS)
        .await
        .unwrap();
    assert!(newly.is_empty(), "{newly:?}");
    assert_eq!(schema_fingerprint(&ice).await, before);
}

/// The ceiling is the blast radius: a sample full of hot keys promotes exactly
/// as many as the cap allows, the schema grows by exactly that many columns,
/// and a second pass on an unchanged table adds nothing.
#[tokio::test]
async fn the_column_ceiling_bounds_one_pass_and_the_next() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    // 20 keys, all present in every row: every one of them clears any
    // threshold, so only the cap decides.
    let attrs: String = format!(
        "{{{}}}",
        (0..20)
            .map(|k| format!(r#""key{k}":"v""#))
            .collect::<Vec<_>>()
            .join(",")
    );
    ice.append_events(&events(100, |_| attrs.clone()))
        .await
        .unwrap();

    let (before, _) = schema_fingerprint(&ice).await;
    let newly = ice
        .auto_promote_hot_keys(0.5, 5, FILES, ROWS)
        .await
        .unwrap();
    assert_eq!(newly.len(), 5, "{newly:?}");
    let (after, property) = schema_fingerprint(&ice).await;
    assert_eq!(
        after.len(),
        before.len() + 5,
        "the schema grew past the ceiling: {after:?}"
    );
    assert_eq!(
        siglake_core::promoted_columns_from_property(property.as_deref()).len(),
        5
    );

    // At the ceiling, the next pass is a no-op — it does not even re-declare
    // the list it already holds.
    let again = ice
        .auto_promote_hot_keys(0.5, 5, FILES, ROWS)
        .await
        .unwrap();
    assert!(again.is_empty(), "{again:?}");
    assert_eq!(schema_fingerprint(&ice).await.0, after);
}
