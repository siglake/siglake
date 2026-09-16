//! Acknowledged index-template edits must survive concurrent writers.
//!
//! THE DEFECT THESE GUARD. v1 kept every index template of the warehouse in one
//! JSON array document. `put_index_template` and `delete_index_template` each
//! read the whole document, edited the vector and replaced it, unconditionally.
//! That is read-modify-write on a key every writer shares, so two query
//! replicas editing two *different* templates each returned success and kept
//! only one, and a DELETE could resurrect a template another replica had just
//! written. Each template id now owns one namespace-scoped object, so no writer
//! holds another writer's key and no tenant reads another tenant's templates; a
//! delete leaves a tombstone so a legacy record cannot resurrect a deleted id.

use std::collections::BTreeMap;
use std::path::Path;

use siglake_core::index_config::{IndexConfig, RetentionPolicy};
use siglake_storage::iceberg::{IcebergContext, DEFAULT_CATALOG_FILE};
use siglake_storage::index_manager::IndexTemplate;

fn template(template_id: &str, priority: i32) -> IndexTemplate {
    IndexTemplate {
        template_id: template_id.to_string(),
        index_id_patterns: vec![format!("{template_id}-*")],
        priority,
        doc_mapping: IndexConfig::builtin_events().doc_mapping,
        retention: None,
    }
}

fn ids(templates: &[IndexTemplate]) -> Vec<String> {
    templates
        .iter()
        .map(|template| template.template_id.clone())
        .collect()
}

/// Every object under the warehouse config area, by relative path. Comparing
/// two of these is what proves an edit touched only its own key.
fn config_objects(warehouse: &Path) -> BTreeMap<String, Vec<u8>> {
    let root = warehouse.join("_siglake/config");
    let mut objects = BTreeMap::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let rel = path
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            objects.insert(rel, std::fs::read(&path).unwrap());
        }
    }
    objects
}

/// The deterministic core of the fix: a PUT and a DELETE each write exactly one
/// object, keyed by the id they were asked to change. v1 rewrote a single
/// shared document on every call, which is why an edit of one template could
/// carry away another writer's edit of a different one — no concurrency needed
/// to see it, only a stale read.
#[tokio::test]
async fn each_edit_writes_only_its_own_template_object() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    ice.put_index_template(&template("alpha", 1)).await.unwrap();
    ice.put_index_template(&template("beta", 2)).await.unwrap();

    let before = config_objects(&warehouse);
    ice.put_index_template(&template("gamma", 3)).await.unwrap();
    let after_put = config_objects(&warehouse);
    let added: Vec<&String> = after_put
        .keys()
        .filter(|k| !before.contains_key(*k))
        .collect();
    assert_eq!(
        added,
        vec!["index_templates/siglake/gamma.json"],
        "a PUT must create exactly its own record: {after_put:?}"
    );
    for (key, bytes) in &before {
        assert_eq!(
            after_put.get(key),
            Some(bytes),
            "PUT gamma rewrote an unrelated object {key}"
        );
    }

    let removed = ice.delete_index_template("alpha").await.unwrap();
    assert!(removed);
    let after_delete = config_objects(&warehouse);
    for key in [
        "index_templates/siglake/beta.json",
        "index_templates/siglake/gamma.json",
    ] {
        assert_eq!(
            after_delete.get(key),
            after_put.get(key),
            "DELETE alpha rewrote {key}"
        );
    }
    assert_eq!(
        after_delete.keys().collect::<Vec<_>>(),
        after_put.keys().collect::<Vec<_>>(),
        "DELETE must tombstone in place, not add or drop keys"
    );
    assert_eq!(
        ids(&ice.list_index_templates().await.unwrap()),
        ["beta", "gamma"]
    );
}

/// Rounds run by the concurrency tests below. A shared-document writer only
/// loses the interleaving it actually hits, and on a local filesystem that
/// window is a fraction of a millisecond; repeating the race widens the
/// exposure without making the suite slow.
const ROUNDS: i32 = 64;

/// One replica's operation for a round, as a future borrowing its context.
type Step<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>;

/// Run `left` and `right` on two OS threads, each with its own runtime and its
/// own `IcebergContext` — two query replicas, as far as this code can tell.
///
/// The threads matter. A `JoinSet` on a multi-thread runtime does NOT reproduce
/// this race: both tasks land on the spawning worker's queue and the template
/// path has no yield point inside it, so the second only starts once the first
/// has finished, and a shared-document writer serialises its way to a pass.
fn race_two_replicas<L, R>(warehouse: &Path, left: L, right: R)
where
    L: for<'a> Fn(&'a IcebergContext, i32) -> Step<'a> + Send + Sync + 'static,
    R: for<'a> Fn(&'a IcebergContext, i32) -> Step<'a> + Send + Sync + 'static,
{
    type Side = dyn for<'a> Fn(&'a IcebergContext, i32) -> Step<'a> + Send + Sync;
    let barrier = std::sync::Barrier::new(2);
    let sides: [&Side; 2] = [&left, &right];
    std::thread::scope(|scope| {
        for side in sides {
            let barrier = &barrier;
            scope.spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let ice = runtime.block_on(IcebergContext::open(warehouse)).unwrap();
                for round in 0..ROUNDS {
                    barrier.wait();
                    runtime.block_on(side(&ice, round));
                }
            });
        }
    });
}

/// Two independent replicas PUT two different templates at the same instant.
/// Both must be readable from a context opened afterwards.
#[test]
fn concurrent_puts_of_different_templates_both_survive_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    // Create the warehouse and its catalog up front; the race under test is
    // between template writes, not between two first-open bootstraps.
    reopen_and_list(&warehouse);
    race_two_replicas(
        &warehouse,
        |ice, round| {
            Box::pin(async move {
                ice.put_index_template(&template(&format!("left-{round}"), round))
                    .await
                    .unwrap()
            })
        },
        |ice, round| {
            Box::pin(async move {
                ice.put_index_template(&template(&format!("right-{round}"), round))
                    .await
                    .unwrap()
            })
        },
    );

    let listed = ids(&reopen_and_list(&warehouse));
    for round in 0..ROUNDS {
        for side in ["left", "right"] {
            let id = format!("{side}-{round}");
            assert!(
                listed.contains(&id),
                "acknowledged PUT of `{id}` was lost by the other replica: {listed:?}"
            );
        }
    }
}

/// One replica deletes a template while the other creates a different one. The
/// delete must not carry away the creation, and the creation must not resurrect
/// the deleted template.
#[test]
fn a_delete_cannot_erase_a_concurrent_put_of_another_template() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let seeder = IcebergContext::open(&warehouse).await.unwrap();
        for round in 0..ROUNDS {
            seeder
                .put_index_template(&template(&format!("doomed-{round}"), 0))
                .await
                .unwrap();
        }
        seeder
            .put_index_template(&template("bystander", 0))
            .await
            .unwrap();
    });
    drop(runtime);

    race_two_replicas(
        &warehouse,
        |ice, round| {
            Box::pin(async move {
                assert!(
                    ice.delete_index_template(&format!("doomed-{round}"))
                        .await
                        .unwrap(),
                    "seeded template doomed-{round} should have been there to delete"
                );
            })
        },
        |ice, round| {
            Box::pin(async move {
                ice.put_index_template(&template(&format!("fresh-{round}"), 0))
                    .await
                    .unwrap()
            })
        },
    );

    let listed = ids(&reopen_and_list(&warehouse));
    assert!(listed.contains(&"bystander".to_string()), "{listed:?}");
    for round in 0..ROUNDS {
        assert!(
            listed.contains(&format!("fresh-{round}")),
            "acknowledged PUT of `fresh-{round}` was lost by a concurrent DELETE: {listed:?}"
        );
        assert!(
            !listed.contains(&format!("doomed-{round}")),
            "acknowledged DELETE of `doomed-{round}` was undone by a concurrent PUT: {listed:?}"
        );
    }
}

fn reopen_and_list(warehouse: &Path) -> Vec<IndexTemplate> {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        IcebergContext::open(warehouse)
            .await
            .unwrap()
            .list_index_templates()
            .await
            .unwrap()
    })
}

/// A v1 document keeps being read, and is never rewritten: an edit lands in the
/// edited id's own record and the document stays byte-identical (no autonomous
/// migration). A deleted legacy template stays deleted — the tombstone is what
/// stops the untouched document from resurrecting it.
#[tokio::test]
async fn legacy_document_stays_readable_and_deleted_entries_do_not_reappear() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    let legacy = serde_json::to_vec(&vec![
        template("legacy-keep", 1),
        template("legacy-drop", 2),
    ])
    .unwrap();
    let legacy_path = warehouse.join("_siglake/config/index_templates.json");
    std::fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
    std::fs::write(&legacy_path, &legacy).unwrap();

    assert_eq!(
        ids(&ice.list_index_templates().await.unwrap()),
        ["legacy-drop", "legacy-keep"],
        "legacy document entries must stay readable"
    );

    ice.put_index_template(&template("fresh", 3)).await.unwrap();
    assert_eq!(
        ids(&ice.list_index_templates().await.unwrap()),
        ["fresh", "legacy-drop", "legacy-keep"]
    );

    assert!(ice.delete_index_template("legacy-drop").await.unwrap());
    assert!(
        !ice.delete_index_template("legacy-drop").await.unwrap(),
        "a second delete of the same id must report it gone"
    );
    // A record shadowing a legacy entry: same id, different body.
    ice.put_index_template(&template("legacy-keep", 99))
        .await
        .unwrap();

    let reopened = IcebergContext::open(&warehouse).await.unwrap();
    let listed = reopened.list_index_templates().await.unwrap();
    assert_eq!(ids(&listed), ["fresh", "legacy-keep"]);
    assert_eq!(
        listed
            .iter()
            .find(|t| t.template_id == "legacy-keep")
            .unwrap()
            .priority,
        99,
        "a record must shadow the legacy entry of the same id"
    );
    assert_eq!(
        std::fs::read(&legacy_path).unwrap(),
        legacy,
        "the legacy document must not be rewritten or migrated"
    );
}

/// Same-template conflict behaviour, stated explicitly: there is no CAS. Two
/// writers of the same id are last-write-wins, and a PUT after a DELETE of the
/// same id revives it (the PUT replaces the tombstone).
#[tokio::test]
async fn same_template_id_is_last_write_wins_and_a_put_revives_a_tombstone() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let first = IcebergContext::open(&warehouse).await.unwrap();
    let second = IcebergContext::open(&warehouse).await.unwrap();

    first
        .put_index_template(&template("shared", 1))
        .await
        .unwrap();
    second
        .put_index_template(&template("shared", 2))
        .await
        .unwrap();
    let reopened = IcebergContext::open(&warehouse).await.unwrap();
    assert_eq!(
        reopened.list_index_templates().await.unwrap()[0].priority,
        2,
        "the later writer of the same id wins"
    );

    assert!(first.delete_index_template("shared").await.unwrap());
    assert!(reopened.list_index_templates().await.unwrap().is_empty());
    second
        .put_index_template(&template("shared", 3))
        .await
        .unwrap();
    let listed = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .list_index_templates()
        .await
        .unwrap();
    assert_eq!(ids(&listed), ["shared"]);
    assert_eq!(listed[0].priority, 3, "a PUT must replace the tombstone");
}

/// Templates drive auto-create, so a resolved index must see the same set: the
/// tombstoned legacy template must not build an index after it was deleted.
#[tokio::test]
async fn resolution_honours_records_and_tombstones() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let legacy = serde_json::to_vec(&vec![template("gone", 5)]).unwrap();
    let legacy_path = warehouse.join("_siglake/config/index_templates.json");
    std::fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
    std::fs::write(&legacy_path, &legacy).unwrap();

    assert!(ice
        .resolve_index_template("gone-1")
        .await
        .unwrap()
        .is_some());
    assert!(ice.delete_index_template("gone").await.unwrap());
    assert!(
        ice.resolve_index_template("gone-1")
            .await
            .unwrap()
            .is_none(),
        "a deleted legacy template must stop auto-creating indexes"
    );
    assert!(
        ice.ensure_index("gone-1").await.unwrap().is_none(),
        "ensure_index must not build an index from a deleted template"
    );
}

async fn open_with_default_namespace(warehouse: &Path, namespace: &str) -> IcebergContext {
    std::fs::create_dir_all(warehouse).unwrap();
    let warehouse = std::fs::canonicalize(warehouse).unwrap();
    let catalog_uri = format!(
        "sqlite://{}?mode=rwc",
        warehouse.join(DEFAULT_CATALOG_FILE).display()
    );
    let warehouse_url = format!("file://{}", warehouse.display());
    IcebergContext::open_with_namespace(&catalog_uri, &warehouse_url, namespace)
        .await
        .unwrap()
}

/// A tenant context reads and writes only its own namespace segment. This is
/// the storage boundary behind both the template GET route and ingest-side
/// `ensure_index` auto-create.
#[tokio::test]
async fn tenant_templates_are_isolated_for_listing_resolution_and_auto_create() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let default = IcebergContext::open(&warehouse).await.unwrap();
    let tenant_a = default.for_namespace("tenant_a").await.unwrap();
    let tenant_b = default.for_namespace("tenant_b").await.unwrap();

    let mut a_template = template("shared", 10);
    a_template.index_id_patterns = vec!["customer-*".to_string()];
    a_template.retention = Some(RetentionPolicy {
        period_secs: 60,
        schedule: None,
    });
    tenant_a.put_index_template(&a_template).await.unwrap();

    assert_eq!(tenant_a.list_index_templates().await.unwrap(), [a_template]);
    assert!(tenant_b.list_index_templates().await.unwrap().is_empty());
    assert!(tenant_b
        .resolve_index_template("customer-1")
        .await
        .unwrap()
        .is_none());
    assert!(tenant_b.ensure_index("customer-1").await.unwrap().is_none());
    assert!(tenant_b
        .resolve_index_template("siglake-logs-builtin")
        .await
        .unwrap()
        .is_some());

    let mut b_template = template("shared", 20);
    b_template.index_id_patterns = vec!["customer-*".to_string()];
    b_template.retention = Some(RetentionPolicy {
        period_secs: 120,
        schedule: None,
    });
    tenant_b.put_index_template(&b_template).await.unwrap();
    assert_eq!(
        tenant_b
            .resolve_index_template("customer-1")
            .await
            .unwrap()
            .unwrap()
            .retention,
        b_template.retention
    );
    tenant_b.ensure_index("customer-1").await.unwrap().unwrap();
    assert_eq!(
        tenant_b
            .get_index("customer-1")
            .await
            .unwrap()
            .unwrap()
            .retention,
        b_template.retention,
        "tenant B auto-create must materialize only tenant B's template"
    );
}

/// Only the namespace configured at open time inherits the two warehouse-root
/// layouts. Named tenant edits stay in their own segment and cannot shadow or
/// tombstone a default-namespace entry.
#[tokio::test]
async fn only_the_configured_default_reads_legacy_layouts() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let default = open_with_default_namespace(&warehouse, "customer_default").await;

    let legacy =
        serde_json::to_vec(&vec![template("legacy-keep", 1), template("shadowed", 2)]).unwrap();
    let config_dir = warehouse.join("_siglake/config");
    std::fs::create_dir_all(config_dir.join("index_templates")).unwrap();
    std::fs::write(config_dir.join("index_templates.json"), legacy).unwrap();
    std::fs::write(
        config_dir.join("index_templates/root-live.json"),
        serde_json::to_vec(&serde_json::json!({
            "template_id": "root-live",
            "template": template("root-live", 3),
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(
        config_dir.join("index_templates/shadowed.json"),
        serde_json::to_vec(&serde_json::json!({
            "template_id": "shadowed",
            "template": null,
        }))
        .unwrap(),
    )
    .unwrap();

    assert_eq!(
        ids(&default.list_index_templates().await.unwrap()),
        ["legacy-keep", "root-live"]
    );

    let named = default.for_namespace("tenant_named").await.unwrap();
    assert!(named.list_index_templates().await.unwrap().is_empty());
    named
        .put_index_template(&template("legacy-keep", 99))
        .await
        .unwrap();
    assert!(named.delete_index_template("legacy-keep").await.unwrap());
    assert_eq!(
        ids(&default.list_index_templates().await.unwrap()),
        ["legacy-keep", "root-live"],
        "a named tenant's tombstone must not affect the default namespace"
    );
}
