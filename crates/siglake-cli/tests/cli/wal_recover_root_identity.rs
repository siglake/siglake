//! #4964: what `siglake wal-recover` can and cannot tell about the root it was
//! pointed at, measured against a `file://` mirror.
//!
//! #4928 made a restore that understood NOTHING exit nonzero. The near miss it
//! cannot see is `--from` exactly ONE component too high: the shallowest key
//! under a mirror root is `<tenant>/<segment>.arrow`, and one component up it
//! is `<prefix>/<tenant>/<segment>.arrow` — which
//! `siglake_wal::mirror::recovery_target` reads as the `<tenant>/<index>/`
//! layout. Real segments are then restored under a tenant named after the
//! mirror prefix, nothing is skipped on that key, and the command exits 0.
//!
//! These tests RECORD current behaviour; none of them asserts a fix. They are
//! the evidence behind `docs/DESIGN_wal_recovery_root_identity.md`, and the
//! thing that design has to keep true: cases 2-4 below are legitimate installs
//! whose keys are byte-identical to the misplacement in case 1, so no rule
//! reading only the key can separate them.

use std::path::{Path, PathBuf};

use siglake_wal::{list_sealed, SEALED_DIR};

use super::wal_recover_cli::{place, recover, seal_one};

/// Every `.arrow` a restore left on the WAL root, as paths relative to it,
/// sorted. This is the routing the FS drain then reads: the first component is
/// the tenant namespace, the second (when there are three) the index.
fn restored_layout(wal: &Path) -> Vec<String> {
    fn walk(dir: &Path, base: &Path, out: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, base, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("arrow") {
                out.push(
                    path.strip_prefix(base)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    let mut out = Vec::new();
    walk(wal, wal, &mut out);
    out.sort();
    out
}

/// A mirror at `<tmp>/<root>` holding one segment per key in `keys`, all copies
/// of the same sealed body. Returns the mirror directory.
fn mirror_with(tmp: &Path, root: &str, keys: &[&str]) -> PathBuf {
    let mirror = tmp.join(root);
    let src = seal_one(&tmp.join(format!("src-{}", root.replace('/', "-"))), "row");
    for key in keys {
        place(&mirror, key, &src);
    }
    mirror
}

/// The segment filename a `mirror_with` fixture used, so a caller can build the
/// key it expects on the WAL root.
fn segment_name(mirror: &Path) -> String {
    fn first(dir: &Path) -> Option<PathBuf> {
        for entry in std::fs::read_dir(dir).ok()?.flatten() {
            let path = entry.path();
            let found = if path.is_dir() {
                first(&path)
            } else {
                Some(path)
            };
            if found.is_some() {
                return found;
            }
        }
        None
    }
    first(mirror)
        .expect("a fixture object")
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

/// **Case 1, the defect.** `--from` exactly one component above the mirror root
/// restores the shallowest keys into a tenant named after the mirror prefix and
/// exits 0. The deeper keys — an index segment and the active mirror — are the
/// only ones refused, so the report carries a skip count that reads like a
/// mirror with stray objects in it (which
/// `a_mirror_with_unknown_keys_alongside_segments_restores_and_reports_both`
/// pins as a SUCCESS), not like a wrong `--from`.
#[test]
fn one_component_above_the_mirror_root_restores_into_an_invented_tenant() {
    let tmp = tempfile::tempdir().unwrap();
    let mirror = tmp.path().join("warehouse").join("wal-mirror");
    let events = seal_one(&tmp.path().join("src").join("acme"), "acme-events");
    let index = seal_one(&tmp.path().join("src").join("acme/orders"), "acme-orders");
    let active = seal_one(&tmp.path().join("src").join("widgets"), "widgets-active");
    let names = [
        events.file_name().unwrap().to_str().unwrap().to_string(),
        index.file_name().unwrap().to_str().unwrap().to_string(),
        active.file_name().unwrap().to_str().unwrap().to_string(),
    ];
    place(&mirror, &format!("acme/{}", names[0]), &events);
    place(&mirror, &format!("acme/orders/{}", names[1]), &index);
    place(
        &mirror,
        &format!("_active/widgets/{}.partial", names[2]),
        &active,
    );

    let wal = tmp.path().join("wal");
    let ancestor = tmp.path().join("warehouse");
    let (stdout, stderr, ok) = recover(&format!("file://{}", ancestor.display()), &wal);

    assert!(
        ok,
        "TODAY this succeeds — the near miss #4928 cannot see: {stdout}{stderr}"
    );
    assert!(
        stdout.contains("pulled 1 segments") && stdout.contains("2 keys skipped"),
        "{stdout}{stderr}"
    );
    assert_eq!(
        restored_layout(&wal),
        vec![format!("wal-mirror/acme/{}/{}", SEALED_DIR, names[0])],
        "acme's events segment lands under a tenant called `wal-mirror`, with \
         `acme` read as its index"
    );
    assert!(
        list_sealed(&wal.join("acme")).unwrap().is_empty(),
        "and nothing lands where the drain would route it to acme"
    );
}

/// **Case 2, why a prefix-name refusal cannot be the rule.** A tenant whose
/// name happens to equal the mirror prefix produces keys byte-identical to
/// case 1's restored key — `wal-mirror/acme/<segment>.arrow` — and restores to
/// the same place. Here that placement is CORRECT. Nothing in the key, the
/// object body or the listing separates the two; refusing a first component
/// named after a prefix would break this install and still not see a mirror
/// whose prefix is `mirror`, `wal` or a date.
#[test]
fn a_tenant_named_after_the_prefix_produces_the_same_keys_and_the_same_restore() {
    let tmp = tempfile::tempdir().unwrap();
    // The mirror root is `.../store`; the tenant under it is called
    // `wal-mirror`, with an index `acme`.
    let mirror = mirror_with(tmp.path(), "store", &["wal-mirror/acme/seg.arrow"]);
    let name = segment_name(&mirror);
    assert_eq!(name, "seg.arrow");

    let wal = tmp.path().join("wal");
    let (stdout, stderr, ok) = recover(&format!("file://{}", mirror.display()), &wal);
    assert!(ok, "{stdout}{stderr}");
    assert!(stdout.contains("pulled 1 segments"), "{stdout}{stderr}");
    assert_eq!(
        restored_layout(&wal),
        vec![format!("wal-mirror/acme/{SEALED_DIR}/seg.arrow")],
        "the legitimate restore of a tenant called `wal-mirror` is the SAME \
         layout case 1 produces by mistake"
    );
}

/// **Case 3, the silent one.** A legacy flat mirror — keys with no tenant
/// component at all, which `recovery_target` maps to the default tenant —
/// read from one component up produces NO skips whatsoever. The report is
/// `pulled N segments` with nothing after it: the exact line a correct restore
/// prints. Every segment lands under a tenant named after the mirror prefix
/// instead of `default`.
#[test]
fn a_legacy_flat_mirror_one_component_up_reports_a_clean_restore() {
    let tmp = tempfile::tempdir().unwrap();
    let mirror = mirror_with(tmp.path(), "warehouse/wal-mirror", &["seg.arrow"]);

    // The control: from the mirror root, the flat key is the default tenant.
    let right = tmp.path().join("wal-right");
    let (stdout, stderr, ok) = recover(&format!("file://{}", mirror.display()), &right);
    assert!(ok, "{stdout}{stderr}");
    assert_eq!(
        restored_layout(&right),
        vec![format!("default/{SEALED_DIR}/seg.arrow")]
    );

    // One component up: same exit status, same stdout shape, wrong namespace.
    let wrong = tmp.path().join("wal-wrong");
    let ancestor = tmp.path().join("warehouse");
    let (stdout, stderr, ok) = recover(&format!("file://{}", ancestor.display()), &wrong);
    assert!(ok, "{stdout}{stderr}");
    assert!(stdout.contains("pulled 1 segments"), "{stdout}{stderr}");
    assert!(
        !stdout.contains("skipped"),
        "nothing is refused, so the report is indistinguishable from a correct \
         restore: {stdout}{stderr}"
    );
    assert_eq!(
        restored_layout(&wrong),
        vec![format!("wal-mirror/{SEALED_DIR}/seg.arrow")],
        "the prefix component became the tenant"
    );
}

/// **Case 4, custom prefixes.** `--from` is the only thing the command is told;
/// it never learns the operator's configured `wal.mirror.prefix`. A mirror
/// rooted at `lake/m` misreads one component up exactly as `wal-mirror` does,
/// and the invented tenant is `m` — a name no blacklist would hold.
#[test]
fn a_custom_prefix_misreads_the_same_way_under_a_different_name() {
    let tmp = tempfile::tempdir().unwrap();
    let mirror = mirror_with(tmp.path(), "lake/m", &["acme/seg.arrow"]);

    let wal = tmp.path().join("wal");
    let (stdout, stderr, ok) = recover(
        &format!("file://{}", tmp.path().join("lake").display()),
        &wal,
    );
    assert!(ok, "{stdout}{stderr}");
    assert!(stdout.contains("pulled 1 segments"), "{stdout}{stderr}");
    assert_eq!(
        restored_layout(&wal),
        vec![format!("m/acme/{SEALED_DIR}/seg.arrow")],
        "the invented tenant is whatever the prefix's last component is"
    );
    assert!(!mirror.join("unused").exists());
}

/// **What the drain then does with it.** The card's framing said the drain
/// commits the misplaced segments into the invented namespace; the proxy's
/// scope note pushed back, because `Compactor::run_once` gates an INDEX
/// directory on `ensure_index` (`crates/siglake-compactor/src/lib.rs:2372`) and
/// `IndexManager::ensure_index` needs an existing index or a matching template
/// (`crates/siglake-storage/src/index_manager.rs:656`). Both are true, and they
/// split by layout:
///
/// - `<prefix>/<tenant>/<index>/<seg>` (case 1) misplaces to an INDEX dir, and
///   `ensure_index` stops it — the rows sit unresolved, counted by
///   `siglake_compactor_index_unresolved_total`, a stuck queue rather than a
///   wrong commit.
/// - `<prefix>/<seg>` — the legacy flat mirror of case 3 — misplaces to a
///   TENANT dir, and there is no equivalent gate: `ice_for_tenant` calls
///   `for_namespace`, which `ensure_namespace`s and `ensure_events_table`s on
///   the spot (`crates/siglake-storage/src/iceberg.rs:9936`). This test runs
///   that path end to end: the rows land in `tenant_wal-mirror`, and the
///   namespace they belonged to stays empty.
#[tokio::test]
async fn a_misplaced_flat_restore_commits_rows_into_an_invented_namespace() {
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use siglake_compactor::Compactor;
    use siglake_storage::iceberg::IcebergContext;

    async fn count_events(ice: &IcebergContext) -> i64 {
        let ctx = SessionContext::new();
        ice.register_with_datafusion(&ctx).await.unwrap();
        let batches = ctx
            .sql("SELECT count(*) AS n FROM events")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0)
    }

    let tmp = tempfile::tempdir().unwrap();
    let mirror = mirror_with(tmp.path(), "warehouse/wal-mirror", &["seg.arrow"]);
    assert!(mirror.join("seg.arrow").exists());

    // Recover one component too high, then drain exactly as the runbook says.
    let wal = tmp.path().join("wal");
    let ancestor = tmp.path().join("warehouse");
    let (stdout, stderr, ok) = recover(&format!("file://{}", ancestor.display()), &wal);
    assert!(ok, "{stdout}{stderr}");

    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("ice-wrong"))
            .await
            .unwrap(),
    );
    let committed = Compactor::new(&wal, ice.clone()).run_once().await.unwrap();
    assert_eq!(committed, 1, "the drain commits the misplaced segment");
    assert_eq!(
        count_events(&ice).await,
        0,
        "the namespace the rows belong to gets nothing"
    );
    let invented = ice.for_namespace("tenant_wal-mirror").await.unwrap();
    assert_eq!(
        count_events(&invented).await,
        1,
        "the row is committed into a namespace named after the mirror prefix"
    );

    // The control: the same mirror, recovered from its root, commits to the
    // default namespace and creates no tenant namespace at all.
    let right = tmp.path().join("wal-right");
    let (stdout, stderr, ok) = recover(&format!("file://{}", mirror.display()), &right);
    assert!(ok, "{stdout}{stderr}");
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("ice-right"))
            .await
            .unwrap(),
    );
    assert_eq!(
        Compactor::new(&right, ice.clone())
            .run_once()
            .await
            .unwrap(),
        1
    );
    assert_eq!(count_events(&ice).await, 1);
}

/// **The evidence that is actually in the store.** The catalog-claim drain
/// stamps `<tenant>/<index>/owner` under the mirror prefix
/// (`siglake_wal::mirror::mirror_owner_key`), and the active mirror writes
/// `_active/…`. Both sit at a KNOWN depth relative to the mirror root, so
/// either one present under `--from` pins the root — and one component up they
/// are one component deeper. Recovery skips both today (neither ends in
/// `.arrow`), and the skip is not distinguished from an unreadable layout.
///
/// The limit of this evidence is what the design has to weigh: a mirror with no
/// managed index and no active mirroring has neither key, and this test shows
/// that mirror restoring one component up with a clean report.
#[test]
fn the_markers_that_do_pin_the_root_sit_at_a_known_depth() {
    let tmp = tempfile::tempdir().unwrap();
    let owner_key = siglake_wal::mirror::mirror_owner_key("", "acme", "orders");
    assert_eq!(
        owner_key.trim_start_matches('/'),
        "acme/orders/owner",
        "the owner marker is exactly two components under the mirror root"
    );

    // A mirror that HAS the markers: from the root they are two and two
    // components deep; from one up, three.
    let mirror = mirror_with(tmp.path(), "warehouse/wal-mirror", &["acme/seg.arrow"]);
    std::fs::create_dir_all(mirror.join("acme").join("orders")).unwrap();
    std::fs::write(mirror.join("acme").join("orders").join("owner"), b"uuid\n").unwrap();

    let wal = tmp.path().join("wal");
    let (stdout, stderr, ok) = recover(&format!("file://{}", mirror.display()), &wal);
    assert!(ok, "{stdout}{stderr}");
    assert!(
        stdout.contains("pulled 1 segments") && stdout.contains("1 keys skipped"),
        "the owner marker is counted as an unrecognised key, the same as a \
         stray README: {stdout}{stderr}"
    );

    // A mirror WITHOUT them — no managed index, no active mirroring — is the
    // population that has no identity evidence at all beyond the segments.
    let bare = mirror_with(tmp.path(), "warehouse2/wal-mirror", &["acme/seg.arrow"]);
    assert!(!bare.join("_active").exists());
    let wal2 = tmp.path().join("wal2");
    let (stdout, stderr, ok) = recover(
        &format!("file://{}", tmp.path().join("warehouse2").display()),
        &wal2,
    );
    assert!(ok, "{stdout}{stderr}");
    assert!(
        stdout.contains("pulled 1 segments") && !stdout.contains("skipped"),
        "a bare mirror one component up restores clean: {stdout}{stderr}"
    );
    assert_eq!(
        restored_layout(&wal2),
        vec![format!("wal-mirror/acme/{SEALED_DIR}/seg.arrow")]
    );
}
