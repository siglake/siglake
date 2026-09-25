//! #4964 and #4973: what `siglake wal-recover` can and cannot tell about the
//! root it was pointed at, measured against a `file://` mirror.
//!
//! #4928 made a restore that understood NOTHING exit nonzero. The near miss it
//! cannot see is `--from` exactly ONE component too high: the shallowest key
//! under a mirror root is `<tenant>/<segment>.arrow`, and one component up it
//! is `<prefix>/<tenant>/<segment>.arrow` — which
//! `siglake_wal::mirror::recovery_target` reads as the `<tenant>/<index>/`
//! layout. Real segments were then restored under a tenant named after the
//! mirror prefix, nothing was skipped on that key, and the command exited 0.
//!
//! #4964 measured that and #4973 answered it with option C of
//! `docs/DESIGN_wal_recovery_root_identity.md`: the command PLANS unless it is
//! given `--apply`, and the same listing that produces the plan carries the
//! verdict. These cases now separate the three populations:
//!
//! - a mirror whose listing holds an `_active/` object or a
//!   `<tenant>/<index>/owner` marker ONE component too deep — refused, in both
//!   forms, naming the directory to pass instead;
//! - a mirror whose markers sit at their own depth — confirmed;
//! - a mirror with neither, which is the default install — unverified, and the
//!   plan is the whole checkpoint. The cases that follow the plan with
//!   `--apply` are that population, and they still misplace: no rule reading
//!   only the keys can separate them, which is what the legitimate installs in
//!   cases 2, 3 and 4 pin.

use std::path::{Path, PathBuf};

use siglake_wal::{list_sealed, SEALED_DIR};

use super::wal_recover_cli::{place, recover, recover_apply, seal_one};

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

/// Write a non-segment object into the mirror — an owner marker, a stray
/// README — at `key`.
fn place_marker(mirror: &Path, key: &str, body: &[u8]) {
    let dest = mirror.join(key);
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    std::fs::write(&dest, body).unwrap();
}

/// The segment filename a `mirror_with` fixture used, so a caller can build the
/// key it expects on the WAL root.
fn segment_name(mirror: &Path) -> String {
    fn first(dir: &Path) -> Option<PathBuf> {
        for entry in std::fs::read_dir(dir).ok()?.flatten() {
            let path = entry.path();
            let found = if path.is_dir() {
                first(&path)
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("arrow") {
                Some(path)
            } else {
                None
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

/// **Case 1, the defect, and #4973's answer to it.** `--from` exactly one
/// component above a mirror that has an active-mirror object used to restore
/// the shallowest keys into a tenant named after the mirror prefix and exit 0.
/// The `_active/` object it skipped on the way past is the evidence: under the
/// mirror root its first component IS `_active`, and here it is one component
/// deeper. Both forms of the command now refuse, name `wal-mirror` as the
/// directory to pass, and leave `--to` alone — the apply before the WAL root
/// is created at all.
///
/// Against the pre-#4973 binary the apply arm exits 0 and writes the invented
/// tenant.
#[test]
fn one_component_above_a_mirror_with_an_active_object_is_refused() {
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

    let ancestor = format!("file://{}", tmp.path().join("warehouse").display());
    for (arm, run) in [
        ("plan", recover as fn(&str, &Path) -> (String, String, bool)),
        ("apply", recover_apply),
    ] {
        let wal = tmp.path().join(format!("wal-{arm}"));
        let (stdout, stderr, ok) = run(&ancestor, &wal);
        assert!(!ok, "{arm}: the misplaced marker must refuse: {stdout}");
        assert!(
            stderr.contains("one component above the mirror root")
                && stderr.contains("`wal-mirror` directory"),
            "{arm}: the refusal names the directory to pass: {stderr}"
        );
        assert!(
            stderr.contains(&format!("_active/widgets/{}.partial", names[2])),
            "{arm}: and the key it read it from: {stderr}"
        );
        assert!(
            !wal.exists(),
            "{arm}: {} must not be created",
            wal.display()
        );
        // The plan is still printed, so the operator sees the tenant the
        // command would have invented next to the reason it will not.
        assert!(
            stdout.contains("tenant=wal-mirror index=acme"),
            "{arm}: {stdout}"
        );
    }

    // The control: from the mirror root the same objects restore, and the
    // verdict is the confirming one.
    let wal = tmp.path().join("wal-right");
    let (stdout, stderr, ok) = recover_apply(&format!("file://{}", mirror.display()), &wal);
    assert!(ok, "{stdout}{stderr}");
    assert!(stdout.contains("root confirmed"), "{stdout}");
    assert_eq!(
        restored_layout(&wal),
        vec![
            format!("acme/orders/{}/{}", SEALED_DIR, names[1]),
            format!("acme/{}/{}", SEALED_DIR, names[0]),
            format!("widgets/{}/{}", SEALED_DIR, names[2]),
        ],
        "every object lands where the drain routes it to its own tenant"
    );
    assert_eq!(list_sealed(&wal.join("acme")).unwrap().len(), 1);
}

/// The other marker, on its own. A fleet mirror with a managed index carries
/// `<tenant>/<index>/owner` (`siglake_wal::mirror::mirror_owner_key`) and may
/// carry no `_active/` object at all — `wal.mirror.activeIntervalSecs`
/// defaults to 0. One component up that marker is four components deep, which
/// the mirror layout never puts it at, and it refuses the same way.
#[test]
fn an_owner_marker_one_component_too_deep_refuses_the_apply() {
    let tmp = tempfile::tempdir().unwrap();
    let mirror = mirror_with(tmp.path(), "warehouse/wal-mirror", &["acme/seg.arrow"]);
    place_marker(
        &mirror,
        "acme/orders/owner",
        b"11111111-2222-3333-4444-555555555555\n",
    );
    assert!(!mirror.join("_active").exists(), "no active mirror here");

    let ancestor = format!("file://{}", tmp.path().join("warehouse").display());
    let wal = tmp.path().join("wal");
    let (stdout, stderr, ok) = recover_apply(&ancestor, &wal);
    assert!(!ok, "{stdout}{stderr}");
    assert!(
        stderr.contains("`wal-mirror` directory") && stderr.contains("acme/orders/owner"),
        "{stderr}"
    );
    assert!(!wal.exists(), "nothing was created: {stdout}{stderr}");

    // From the mirror root the same marker CONFIRMS, and the restore runs.
    let right = tmp.path().join("wal-right");
    let (stdout, stderr, ok) = recover_apply(&format!("file://{}", mirror.display()), &right);
    assert!(ok, "{stdout}{stderr}");
    assert!(stdout.contains("root confirmed"), "{stdout}");
    assert_eq!(
        restored_layout(&right),
        vec![format!("acme/{SEALED_DIR}/seg.arrow")]
    );
}

/// A confirming marker does not erase a contradicting one. A listing that
/// holds markers at BOTH depths is not a mirror root under either reading —
/// an ancestor holding two mirrors, or a mirror root with a stray copy of one
/// under it — so the refusal wins and says both keys.
#[test]
fn a_confirming_marker_does_not_erase_a_contradiction() {
    let tmp = tempfile::tempdir().unwrap();
    let ancestor = tmp.path().join("warehouse");
    let src = seal_one(&tmp.path().join("src"), "row");
    // At root depth: `_active/<tenant>/<seg>.arrow.partial`.
    place(&ancestor, "_active/acme/a.arrow.partial", &src);
    // One deeper, under a second directory: the contradiction.
    place(&ancestor, "wal-mirror/_active/acme/b.arrow.partial", &src);
    place(&ancestor, "wal-mirror/acme/seg.arrow", &src);

    let wal = tmp.path().join("wal");
    let (stdout, stderr, ok) = recover_apply(&format!("file://{}", ancestor.display()), &wal);
    assert!(!ok, "the contradiction wins: {stdout}{stderr}");
    assert!(
        stderr.contains("wal-mirror/_active/acme/b.arrow.partial"),
        "the misplaced marker is the evidence: {stderr}"
    );
    assert!(
        stderr.contains("_active/acme/a.arrow.partial") && stderr.contains("markers at two depths"),
        "and the confirming one is reported, not obeyed: {stderr}"
    );
    assert!(!wal.exists(), "nothing was created: {stdout}{stderr}");
}

/// **Why the one-component miss is the likely one.** `wal.mirror.prefix` is
/// relative to the warehouse URL, so the mirror root is
/// `s3://<bucket>/<warehousePrefix>/wal-mirror/`
/// (`deploy/helm/siglake/values.yaml:864-867`) and its parent is the warehouse
/// URL the operator already has in their config and their shell history. This
/// is that mistake against a warehouse-shaped ancestor. This mirror has no
/// marker of either kind — the default install — so nothing in the listing
/// contradicts the root and the verdict is `unverified`. What #4973 adds for
/// this population is the checkpoint: the plan names the tenant it would
/// invent, and writes nothing until the operator passes `--apply`.
#[test]
fn the_warehouse_url_an_operator_already_has_is_the_one_component_miss() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("bucket").join("warehouse");
    let src = seal_one(&tmp.path().join("src"), "row");
    place(&warehouse, "wal-mirror/acme/seg.arrow", &src);
    for key in [
        "siglake/events/metadata/v1.metadata.json",
        "siglake/events/data/part-0.parquet",
        "tenant_acme/events/data/part-1.parquet",
    ] {
        let dest = warehouse.join(key);
        std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
        std::fs::write(&dest, b"not a segment").unwrap();
    }

    let wal = tmp.path().join("wal");
    let from = format!("file://{}", warehouse.display());
    let (stdout, stderr, ok) = recover(&from, &wal);
    assert!(ok, "{stdout}{stderr}");
    assert!(
        stdout.contains("totals: 1 segments") && stdout.contains("3 keys skipped"),
        "{stdout}{stderr}"
    );
    assert!(
        stdout.contains("root unverified"),
        "no marker either way: {stdout}"
    );
    assert!(
        stdout.contains("tenant=wal-mirror index=acme"),
        "the plan names the tenant it would invent: {stdout}"
    );
    assert!(!wal.exists(), "and writes nothing: {stdout}{stderr}");

    // An operator who reads the plan and applies it anyway gets the old
    // outcome: nothing in this listing could have told the command otherwise.
    let (stdout, stderr, ok) = recover_apply(&from, &wal);
    assert!(ok, "{stdout}{stderr}");
    assert!(stdout.contains("pulled 1 segments"), "{stdout}{stderr}");
    assert_eq!(
        restored_layout(&wal),
        vec![format!("wal-mirror/acme/{SEALED_DIR}/seg.arrow")],
        "acme's segment lands under a tenant named after the mirror prefix"
    );
}

/// **Case 2, why a prefix-name refusal cannot be the rule.** A tenant whose
/// name happens to equal the mirror prefix produces keys byte-identical to
/// case 1's restored key — `wal-mirror/acme/<segment>.arrow` — and restores to
/// the same place. Here that placement is CORRECT. Nothing in the key, the
/// object body or the listing separates the two; refusing a first component
/// named after a prefix would break this install and still not see a mirror
/// whose prefix is `mirror`, `wal` or a date. #4973 leaves this install
/// working: the plan describes it, `--apply` restores it, and the marker
/// verdict never fires because nothing in the listing is misplaced. A re-run
/// of the apply is idempotent, as the runbook's second attempt has to be.
#[test]
fn a_tenant_named_after_the_prefix_produces_the_same_keys_and_the_same_restore() {
    let tmp = tempfile::tempdir().unwrap();
    // The mirror root is `.../store`; the tenant under it is called
    // `wal-mirror`, with an index `acme`. It carries the markers a healthy
    // fleet mirror carries, at their own depth.
    let mirror = mirror_with(tmp.path(), "store", &["wal-mirror/acme/seg.arrow"]);
    place_marker(&mirror, "wal-mirror/acme/owner", b"a-table-uuid\n");
    let name = segment_name(&mirror);
    assert_eq!(name, "seg.arrow");

    let from = format!("file://{}", mirror.display());
    let wal = tmp.path().join("wal");
    let (stdout, stderr, ok) = recover(&from, &wal);
    assert!(ok, "plan: {stdout}{stderr}");
    assert!(stdout.contains("root confirmed"), "{stdout}{stderr}");
    assert!(!wal.exists(), "the plan wrote nothing: {stdout}{stderr}");

    let (stdout, stderr, ok) = recover_apply(&from, &wal);
    assert!(ok, "{stdout}{stderr}");
    assert!(stdout.contains("pulled 1 segments"), "{stdout}{stderr}");
    let expected = vec![format!("wal-mirror/acme/{SEALED_DIR}/seg.arrow")];
    assert_eq!(
        restored_layout(&wal),
        expected,
        "the legitimate restore of a tenant called `wal-mirror` is the SAME \
         layout case 1 produces by mistake"
    );

    let (stdout, stderr, ok) = recover_apply(&from, &wal);
    assert!(ok, "the second apply is idempotent: {stdout}{stderr}");
    assert!(
        stdout.contains("pulled 0 segments") && stdout.contains("1 already present"),
        "{stdout}{stderr}"
    );
    assert_eq!(restored_layout(&wal), expected);
}

/// **Case 3, the silent one — and the case the plan exists for.** A legacy
/// flat mirror (keys with no tenant component at all, which `recovery_target`
/// maps to the default tenant) read from one component up produces NO skips
/// whatsoever, and no marker either. The two runs differ in exactly one place
/// an operator can see before anything is written: the destination on the plan
/// line, `default/` against `wal-mirror/`.
#[test]
fn a_legacy_flat_mirror_one_component_up_plans_a_different_destination() {
    let tmp = tempfile::tempdir().unwrap();
    let mirror = mirror_with(tmp.path(), "warehouse/wal-mirror", &["seg.arrow"]);

    // The control: from the mirror root, the flat key is the default tenant.
    let right = tmp.path().join("wal-right");
    let from = format!("file://{}", mirror.display());
    let (stdout, stderr, ok) = recover(&from, &right);
    assert!(ok, "{stdout}{stderr}");
    assert!(
        stdout.contains("tenant=default") && stdout.contains("root unverified"),
        "{stdout}{stderr}"
    );
    let (stdout, stderr, ok) = recover_apply(&from, &right);
    assert!(ok, "{stdout}{stderr}");
    assert_eq!(
        restored_layout(&right),
        vec![format!("default/{SEALED_DIR}/seg.arrow")]
    );

    // One component up: no marker contradicts it, so the plan is all the
    // operator gets — and the plan says `wal-mirror`, which is the tenant they
    // do not have. Nothing is written until they say so.
    let wrong = tmp.path().join("wal-wrong");
    let ancestor = format!("file://{}", tmp.path().join("warehouse").display());
    let (stdout, stderr, ok) = recover(&ancestor, &wrong);
    assert!(ok, "{stdout}{stderr}");
    assert!(
        stdout.contains("totals: 1 segments") && !stdout.contains("skipped"),
        "nothing is refused, so the counts are indistinguishable from a correct \
         restore: {stdout}{stderr}"
    );
    assert!(
        stdout.contains("tenant=wal-mirror"),
        "the destination is the difference: {stdout}"
    );
    assert!(!wrong.exists(), "and nothing is written: {stdout}{stderr}");

    let (stdout, stderr, ok) = recover_apply(&ancestor, &wrong);
    assert!(ok, "{stdout}{stderr}");
    assert!(stdout.contains("pulled 1 segments"), "{stdout}{stderr}");
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
    let from = format!("file://{}", tmp.path().join("lake").display());
    let (stdout, stderr, ok) = recover(&from, &wal);
    assert!(ok, "{stdout}{stderr}");
    assert!(
        stdout.contains("tenant=m index=acme") && stdout.contains("root unverified"),
        "the plan names the invented tenant: {stdout}{stderr}"
    );
    assert!(!wal.exists(), "and writes nothing: {stdout}{stderr}");

    let (stdout, stderr, ok) = recover_apply(&from, &wal);
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
/// (`crates/siglake-storage/src/index_manager.rs:656`). The outcome splits by
/// the DEPTH of the misplaced key, and only one of the three branches is the
/// one either of us named:
///
/// - `<prefix>/<seg>` — the legacy flat mirror of case 3 — misplaces to a
///   TENANT events dir, and there is no gate on that path at all:
///   `ice_for_tenant` calls `for_namespace`, which `ensure_namespace`s and
///   `ensure_events_table`s on the spot
///   (`crates/siglake-storage/src/iceberg.rs:9936`). This test runs it end to
///   end: the rows commit into `tenant_wal-mirror`, and the namespace they
///   belong to stays empty. The wrong-table commit is real, for this
///   population.
/// - `<prefix>/<tenant>/<seg>` and `<prefix>/<tenant>/<index>/<seg>` stop AT
///   `ensure_index`, with the real tenant name read as the index:
///   `a_misplaced_index_restore_is_walked_and_refused_at_the_index_gate` below.
///   Before #4972 they never reached it at all.
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
    let (stdout, stderr, ok) = recover_apply(&format!("file://{}", ancestor.display()), &wal);
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
    let (stdout, stderr, ok) = recover_apply(&format!("file://{}", mirror.display()), &right);
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

/// The other branch. A tenant-scoped mirror misplaced one component up
/// restores to `<wal>/<prefix>/<tenant>/{SEALED_DIR}/`, reading `<prefix>` as
/// the tenant and the real tenant name as an index.
///
/// Before #4972 that left `<wal>/<prefix>/` with no `sealed/` of its own, and
/// `list_layout_dirs` enumerates only a child that HAS one
/// (`crates/siglake-wal/src/lib.rs:2188`): the segments sat on the volume,
/// reported as restored, with no commit, no
/// `siglake_compactor_index_unresolved_total`, no backlog gauge and nothing in
/// `orphans/`. An operator who followed the runbook and watched the drain saw a
/// successful restore and an empty cluster.
///
/// The restore now rebuilds the discovery dir, so the misplacement is WALKED:
/// `tenant_<prefix>` is created and `ensure_index` refuses the invented index.
/// The rows still do not reach the namespace they belong to — that is #4964's
/// subject, and this test keeps pinning it — but the wrong `--from` now leaves
/// an artifact to notice.
#[tokio::test]
async fn a_misplaced_index_restore_is_walked_and_refused_at_the_index_gate() {
    use std::sync::Arc;

    use siglake_compactor::Compactor;
    use siglake_storage::iceberg::IcebergContext;

    let tmp = tempfile::tempdir().unwrap();
    mirror_with(tmp.path(), "warehouse/wal-mirror", &["acme/seg.arrow"]);

    let wal = tmp.path().join("wal");
    let ancestor = tmp.path().join("warehouse");
    let (stdout, stderr, ok) = recover_apply(&format!("file://{}", ancestor.display()), &wal);
    assert!(ok, "{stdout}{stderr}");
    assert_eq!(
        restored_layout(&wal),
        vec![format!("wal-mirror/acme/{SEALED_DIR}/seg.arrow")]
    );

    // `wal-mirror` is a tenant as far as the drain is concerned, and `acme` —
    // the real tenant — is read as an index under it.
    assert_eq!(
        siglake_wal::list_tenant_dirs(&wal)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>(),
        vec!["wal-mirror".to_string()]
    );

    let ice = Arc::new(IcebergContext::open(&tmp.path().join("ice")).await.unwrap());
    let compactor = Compactor::new(&wal, ice.clone());
    assert_eq!(compactor.run_once().await.unwrap(), 0);
    assert_eq!(
        compactor.run_once().await.unwrap(),
        0,
        "and every later cycle refuses it the same way"
    );
    assert_eq!(
        restored_layout(&wal),
        vec![format!("wal-mirror/acme/{SEALED_DIR}/seg.arrow")],
        "the segment is not committed, not quarantined and not deleted"
    );
    assert!(
        ice.catalog()
            .namespace_exists(&iceberg::NamespaceIdent::new("tenant_wal-mirror".into()))
            .await
            .unwrap(),
        "the namespace named after the mirror prefix is the artifact the wrong \
         --from now leaves behind"
    );
    assert_eq!(
        count_events_in(&ice, "tenant_acme").await,
        None,
        "and the namespace the rows belong to still gets nothing"
    );
}

/// `count(*)` over a namespace's `events`, or `None` when the namespace has
/// never been created.
async fn count_events_in(
    ice: &siglake_storage::iceberg::IcebergContext,
    namespace: &str,
) -> Option<i64> {
    use datafusion::prelude::SessionContext;

    if !ice
        .catalog()
        .namespace_exists(&iceberg::NamespaceIdent::new(namespace.into()))
        .await
        .unwrap()
    {
        return None;
    }
    let ns = ice.for_namespace(namespace).await.unwrap();
    let ctx = SessionContext::new();
    ns.register_with_datafusion(&ctx).await.unwrap();
    let batches = ctx
        .sql("SELECT count(*) AS n FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    Some(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
    )
}

/// A defect this qualification turned up that has nothing to do with `--from`,
/// fixed on its own card (#4972). The ingester creates `<tenant>/sealed/`
/// before it opens any per-index lane, and calls it the "tenant discovery dir"
/// (`crates/siglake-ingest/src/lib.rs:571-580`) — it exists so
/// `list_layout_dirs` enumerates the tenant. Recovery rebuilt
/// `<tenant>/<index>/sealed/` and not that, so a mirror holding only index
/// segments for a tenant — an Elasticsearch-bulk-only tenant whose events lane
/// never sealed — restored CORRECTLY, from the right `--from`, into a layout
/// the drain never walked.
///
/// The restore now rebuilds it, so the tenant is enumerated, the index
/// directory beneath it is reached, and the segment goes through the ordinary
/// index gates: resolved here, because the index exists. Against the pre-fix
/// code every assertion after the restore fails.
#[tokio::test]
async fn a_correct_restore_of_an_index_only_tenant_is_drained() {
    use std::sync::Arc;

    use siglake_compactor::Compactor;
    use siglake_core::index_config::IndexConfig;
    use siglake_storage::iceberg::IcebergContext;

    let tmp = tempfile::tempdir().unwrap();
    let mirror = mirror_with(
        tmp.path(),
        "warehouse/wal-mirror",
        &["acme/orders/seg.arrow"],
    );

    let wal = tmp.path().join("wal");
    let from = format!("file://{}", mirror.display());

    // The plan names the index destination and creates nothing — not the
    // segment's directory and not the tenant discovery dir #4972 added, which
    // is the one write a plan could most easily leak.
    let (stdout, stderr, ok) = recover(&from, &wal);
    assert!(ok, "plan: {stdout}{stderr}");
    assert!(stdout.contains("tenant=acme index=orders"), "{stdout}");
    assert!(!wal.exists(), "the plan created {}", wal.display());

    let (stdout, stderr, ok) = recover_apply(&from, &wal);
    assert!(ok, "{stdout}{stderr}");
    assert!(stdout.contains("pulled 1 segments"), "{stdout}{stderr}");
    assert_eq!(
        restored_layout(&wal),
        vec![format!("acme/orders/{SEALED_DIR}/seg.arrow")],
        "the layout is right: this is the tenant and index the segment came from"
    );
    assert!(
        wal.join("acme").join(SEALED_DIR).is_dir(),
        "and the restore rebuilt the discovery dir the enumeration reads"
    );

    // #4972's repair path is unchanged by the split: a re-run over a root
    // whose segments are present but whose discovery dir is missing recreates
    // it, under `--apply`.
    std::fs::remove_dir(wal.join("acme").join(SEALED_DIR)).unwrap();
    let (stdout, stderr, ok) = recover_apply(&from, &wal);
    assert!(ok, "{stdout}{stderr}");
    assert!(
        stdout.contains("1 already present"),
        "the segment is not re-pulled: {stdout}{stderr}"
    );
    assert!(
        wal.join("acme").join(SEALED_DIR).is_dir(),
        "and the discovery dir is repaired anyway"
    );

    assert_eq!(
        siglake_wal::list_tenant_dirs(&wal)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>(),
        vec!["acme".to_string()],
        "the tenant is enumerated"
    );
    assert_eq!(
        siglake_wal::list_index_dirs(&wal.join("acme"))
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>(),
        vec!["orders".to_string()],
        "and the walk reaches the index directory"
    );

    let ice = Arc::new(IcebergContext::open(&tmp.path().join("ice")).await.unwrap());
    let acme = ice.for_namespace("tenant_acme").await.unwrap();
    let mut config = IndexConfig::builtin_events();
    config.index_id = "orders".to_string();
    acme.create_index(&config).await.unwrap();

    assert_eq!(
        Compactor::new(&wal, ice).run_once().await.unwrap(),
        1,
        "the restored segment is committed to the index it came from"
    );
    assert!(
        restored_layout(&wal)
            .iter()
            .all(|p| !p.contains(&format!("/{SEALED_DIR}/"))),
        "and it leaves sealed/: {:?}",
        restored_layout(&wal)
    );
}

/// The same restore when the index does NOT resolve. The outcome the card
/// asked for is a VISIBLE one: the tenant is walked, `ensure_index` refuses,
/// and the segment is left in `sealed/` under a counted, exported backlog
/// instead of sitting on the volume with nothing to notice it.
#[tokio::test]
async fn an_index_only_restore_whose_index_does_not_resolve_reaches_the_index_gate() {
    use std::sync::Arc;

    use siglake_compactor::Compactor;
    use siglake_storage::iceberg::IcebergContext;

    let tmp = tempfile::tempdir().unwrap();
    let mirror = mirror_with(
        tmp.path(),
        "warehouse/wal-mirror",
        &["acme/orders/seg.arrow"],
    );

    let wal = tmp.path().join("wal");
    let (stdout, stderr, ok) = recover_apply(&format!("file://{}", mirror.display()), &wal);
    assert!(ok, "{stdout}{stderr}");

    let ice = Arc::new(IcebergContext::open(&tmp.path().join("ice")).await.unwrap());
    let compactor = Compactor::new(&wal, ice.clone());
    assert_eq!(
        compactor.run_once().await.unwrap(),
        0,
        "nothing is committed: no index `orders` and no template matches it"
    );
    assert_eq!(
        restored_layout(&wal),
        vec![format!("acme/orders/{SEALED_DIR}/seg.arrow")],
        "the segment is not committed, not quarantined and not deleted"
    );
    assert!(
        ice.catalog()
            .namespace_exists(&iceberg::NamespaceIdent::new("tenant_acme".into()))
            .await
            .unwrap(),
        "but the tenant walk reached this dir — the namespace it creates on the \
         way to `ensure_index` is the artifact an operator can see"
    );
}

/// **The evidence that is actually in the store, and the population that has
/// none of it.** The catalog-claim drain stamps `<tenant>/<index>/owner` under
/// the mirror prefix (`siglake_wal::mirror::mirror_owner_key`), and the active
/// mirror writes `_active/…`. Both sit at a KNOWN depth relative to the mirror
/// root, so either one present under `--from` pins it. Recovery still counts
/// them as unrecognised keys — they are not segments — and now also reads them
/// for the verdict, which is the one thing that distinguishes those skips from
/// a stray README.
///
/// The limit is the last arm: a mirror with no managed index and no active
/// mirroring has neither key, one component up reports no skip at all, and the
/// plan is the only thing between the operator and the invented tenant.
#[test]
fn the_markers_that_do_pin_the_root_sit_at_a_known_depth() {
    let tmp = tempfile::tempdir().unwrap();
    let owner_key = siglake_wal::mirror::mirror_owner_key("", "acme", "orders");
    assert_eq!(
        owner_key.trim_start_matches('/'),
        "acme/orders/owner",
        "the owner marker is exactly two components under the mirror root"
    );

    // A mirror that HAS the marker, read from its root: confirmed, and the
    // marker is still counted as a key recovery does not restore.
    let mirror = mirror_with(tmp.path(), "warehouse/wal-mirror", &["acme/seg.arrow"]);
    place_marker(&mirror, "acme/orders/owner", b"uuid\n");

    let wal = tmp.path().join("wal");
    let (stdout, stderr, ok) = recover_apply(&format!("file://{}", mirror.display()), &wal);
    assert!(ok, "{stdout}{stderr}");
    assert!(
        stdout.contains("pulled 1 segments") && stdout.contains("1 keys skipped"),
        "the owner marker is counted as an unrecognised key, the same as a \
         stray README: {stdout}{stderr}"
    );
    assert!(
        stdout.contains("root confirmed by `acme/orders/owner`"),
        "but unlike a README it also pins the root: {stdout}"
    );

    // A mirror WITHOUT them — no managed index, no active mirroring — is the
    // population that has no identity evidence at all beyond the segments.
    let bare = mirror_with(tmp.path(), "warehouse2/wal-mirror", &["acme/seg.arrow"]);
    assert!(!bare.join("_active").exists());
    let wal2 = tmp.path().join("wal2");
    let from = format!("file://{}", tmp.path().join("warehouse2").display());
    let (stdout, stderr, ok) = recover(&from, &wal2);
    assert!(ok, "{stdout}{stderr}");
    assert!(
        stdout.contains("totals: 1 segments")
            && !stdout.contains("skipped")
            && stdout.contains("root unverified"),
        "a bare mirror one component up plans clean: {stdout}{stderr}"
    );
    assert!(!wal2.exists(), "and writes nothing: {stdout}{stderr}");

    let (stdout, stderr, ok) = recover_apply(&from, &wal2);
    assert!(ok, "{stdout}{stderr}");
    assert_eq!(
        restored_layout(&wal2),
        vec![format!("wal-mirror/acme/{SEALED_DIR}/seg.arrow")]
    );
}
