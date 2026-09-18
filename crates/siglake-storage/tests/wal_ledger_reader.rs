//! Task #4997: the read-only `wal_segments` reader behind `siglake wal-recover
//! --catalog`, against hermetic SQLite catalogs it builds itself.
//!
//! These are the reader's cases: open modes, the no-DDL claim, the chunked
//! lookup's cost, and the composition with the plan `wal-recover` actually
//! ships. The VERDICT's cases — complete, partial, absent and conflicting id
//! matches, the two spellings, the precedence against the marker verdict — are
//! pure and live with the arithmetic in
//! `siglake-wal/src/mirror.rs::tests::ledger_identity`.
//!
//! They supersede #4974's `wal_ledger_identity_prototype.rs`, which made the
//! same claims against a private copy of the check before the flag existed.
//! `docs/DESIGN_wal_recovery_ledger_identity.md` records what each one
//! settled.

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use siglake_storage::catalog_claim::SqlSegmentClaim;
use siglake_storage::wal_ledger::{read_only_uri, WalLedgerReader, LOOKUP_CHUNK};
use siglake_wal::mirror::{ledger_verdict, plan_recovery, LedgerRow, LedgerVerdict, RootVerdict};
use sqlx::{AnyPool, Row};

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/// A database holding ONLY `wal_segments` — an older catalog, or one this
/// check has no business adding tables to — built without `ensure_schema` so
/// the read-only A/B has something to detect.
async fn ledger(path: &Path, rows: &[(&str, &str, &str, &str)]) {
    sqlx::any::install_default_drivers();
    let uri = format!("sqlite://{}?mode=rwc", path.display());
    let pool = AnyPool::connect(&uri).await.unwrap();
    sqlx::query(
        "CREATE TABLE wal_segments (
            id TEXT PRIMARY KEY, tenant TEXT NOT NULL DEFAULT 'default',
            index_id TEXT NOT NULL DEFAULT '', segment_url TEXT NOT NULL,
            bytes BIGINT NOT NULL, rows BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'sealed', attempts INTEGER NOT NULL DEFAULT 0,
            not_before_ms BIGINT, claimer TEXT, claimed_at_ms BIGINT,
            committed_at_ms BIGINT, registered_at_ms BIGINT NOT NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    for (id, tenant, index_id, url) in rows {
        sqlx::query(
            "INSERT INTO wal_segments (id, tenant, index_id, segment_url, bytes, rows, \
             registered_at_ms) VALUES (?, ?, ?, ?, 1, 1, 0)",
        )
        .bind(id)
        .bind(tenant)
        .bind(index_id)
        .bind(url)
        .execute(&pool)
        .await
        .unwrap();
    }
    // Release the `-wal`/`-shm` sidecars before anything reads the file: a
    // read-only open of a WAL-mode database with a live sidecar is its own
    // failure mode, measured in
    // `a_read_only_mount_reads_a_rollback_catalog_but_needs_immutable_for_a_wal_one`.
    pool.close().await;
}

/// Every byte of the ledger file and its sidecars, for the read-only A/B.
fn ledger_bytes(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter(|e| e.path().is_file())
        .map(|e| {
            (
                e.file_name().to_string_lossy().into_owned(),
                std::fs::read(e.path()).unwrap(),
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn ids(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|s| (*s).to_string()).collect()
}

// ---------------------------------------------------------------------------
// the reader
// ---------------------------------------------------------------------------

/// The read-only claim, A/B'd against the thing the card forbids. The reader
/// leaves the ledger file and its sidecars byte-identical;
/// `SqlSegmentClaim::connect` on the same file creates three tables and an
/// index, which is a schema migration inside recovery PLANNING.
#[tokio::test]
async fn a_read_only_lookup_leaves_the_ledger_file_byte_identical() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("catalog");
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("catalog.db");
    ledger(&db, &[("s1", "acme", "", "wal-mirror/acme/s1.arrow")]).await;
    let before = ledger_bytes(&dir);

    // The caller's own spelling asks for a WRITABLE open, and the reader
    // overrides it: the engine, not this code, is what refuses the write.
    let reader = WalLedgerReader::open(&format!("sqlite://{}?mode=rwc", db.display()))
        .await
        .unwrap();
    let rows = reader.lookup(&ids(&["s1", "s2"])).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows["s1"].tenant, "acme");
    reader.close().await;
    assert_eq!(
        ledger_bytes(&dir),
        before,
        "a read-only lookup must not change one byte of the catalog"
    );

    // The negative control: the connect path recovery would otherwise reuse.
    let claim = SqlSegmentClaim::connect(
        &format!("sqlite://{}?mode=rwc", db.display()),
        "wal-recover-test",
    )
    .await
    .unwrap();
    drop(claim);
    assert_ne!(
        ledger_bytes(&dir),
        before,
        "SqlSegmentClaim::connect runs ensure_schema, which is why the reader cannot use it"
    );
    // Name what it added, so the finding is not just "the bytes moved".
    let pool = AnyPool::connect(&read_only_uri(&format!("sqlite://{}", db.display())))
        .await
        .unwrap();
    let names: Vec<String> = sqlx::query("SELECT name FROM sqlite_master WHERE type = 'table'")
        .fetch_all(&pool)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();
    pool.close().await;
    for added in [
        "table_leases",
        "mirror_sync_cursors",
        "consumed_proof_watermarks",
    ] {
        assert!(
            names.iter().any(|n| n == added),
            "ensure_schema created {added}: {names:?}"
        );
    }
}

/// UNAVAILABLE, in the two shapes a DR run produces: the database is not there
/// at all (the second failure domain — a catalog that survived the PVC loss is
/// not guaranteed), and the file is there but is not a siglake catalog. Both
/// are errors from `open`, which the CLI turns into
/// [`LedgerVerdict::Unavailable`]; neither is silently downgraded to "no
/// evidence".
#[tokio::test]
async fn an_unreadable_ledger_is_reported_and_not_downgraded() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("gone.db");
    let err = WalLedgerReader::open(&format!("sqlite://{}", missing.display()))
        .await
        .expect_err("a missing SQLite file must not be created");
    let msg = format!("{err:#}");
    assert!(msg.contains("no such database file"), "{msg}");
    assert!(
        !msg.contains("immutable=1"),
        "SQLite reports a missing file as `unable to open`, the same words a read-only \
         mount uses, and an operator whose catalog did not survive the volume must not be \
         told to copy its sidecars somewhere writable: {msg}"
    );
    let verdict = LedgerVerdict::Unavailable { reason: msg };
    assert!(verdict.refuses(), "{}", verdict.line());
    assert!(
        verdict.line().contains("could not be read"),
        "{}",
        verdict.line()
    );

    // Present, readable, and not a catalog: `wal_segments` is missing. "The
    // catalog has no row for these ids" and "this is not a siglake catalog"
    // must not read the same.
    let foreign = tmp.path().join("foreign.db");
    sqlx::any::install_default_drivers();
    let pool = AnyPool::connect(&format!("sqlite://{}?mode=rwc", foreign.display()))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE something_else (x INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let err = WalLedgerReader::open(&format!("sqlite://{}", foreign.display()))
        .await
        .expect_err("a database with no wal_segments is not a ledger");
    let msg = format!("{err:#}");
    assert!(msg.contains("not a siglake catalog"), "{msg}");
}

/// A ledger a live deployment is still writing must be readable. The writer
/// here holds its pool open across the read, which is the state a DR run finds
/// when the catalog survived and the ingesters did not.
#[tokio::test]
async fn a_ledger_a_live_deployment_is_still_writing_opens_read_only() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("live.db");
    let writer = SqlSegmentClaim::connect(
        &format!("sqlite://{}?mode=rwc", db.display()),
        "live-compactor",
    )
    .await
    .unwrap();
    // Through the real registration path, so the columns and their spelling
    // are the uploader's own rather than this file's.
    writer
        .register(
            "s1",
            "acme",
            "orders",
            "wal-mirror/acme/orders/s1.arrow",
            1,
            1,
        )
        .await
        .unwrap();

    let reader = WalLedgerReader::open(&format!("sqlite://{}", db.display()))
        .await
        .expect("a live catalog must be readable read-only");
    let rows = reader.lookup(&ids(&["s1"])).await.unwrap();
    assert_eq!(
        rows["s1"],
        LedgerRow {
            tenant: "acme".to_string(),
            index_id: "orders".to_string(),
            segment_url: "wal-mirror/acme/orders/s1.arrow".to_string(),
        }
    );
    reader.close().await;
    drop(writer);
}

/// Journal mode from the database header, without opening the database —
/// opening a WAL-mode file is the thing being measured, and it has side
/// effects. Byte 18 is the write version: 1 legacy rollback journal, 2 WAL
/// (<https://sqlite.org/fileformat.html#the_database_header>).
fn header_journal(path: &Path) -> &'static str {
    let bytes = std::fs::read(path).unwrap();
    match bytes[18] {
        2 => "wal",
        _ => "rollback",
    }
}

/// Make `dir` unwritable and report whether the mode bits are enforced: root
/// ignores them, and so does a filesystem mounted without permission support.
fn deny_writes(dir: &Path) -> (bool, Restore) {
    let restore = Restore {
        dir: dir.to_path_buf(),
        perms: std::fs::metadata(dir).unwrap().permissions(),
    };
    let mut perms = restore.perms.clone();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o555);
    }
    std::fs::set_permissions(dir, perms).unwrap();
    (std::fs::write(dir.join("probe"), b"x").is_err(), restore)
}

/// Puts the mode bits back on drop, panic included: a `TempDir` whose
/// directory is still 0555 when the harness tears it down cannot be removed,
/// and the leftover needs a `chmod` by hand.
struct Restore {
    dir: std::path::PathBuf,
    perms: std::fs::Permissions,
}

impl Drop for Restore {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.dir, self.perms.clone());
    }
}

/// A rescued catalog on a READ-ONLY mount, which is the DR shape `mode=ro` is
/// for — and the one place the open mode has to be chosen on evidence rather
/// than by name.
///
/// Measured, on this SQLite: a rollback-journal database opens `mode=ro` with
/// the directory unwritable, and a WAL-mode one does NOT — SQLite has to
/// create a `-shm` beside it, so the open fails though every statement is a
/// SELECT. `immutable=1` reads it, and is unsafe against a LIVE catalog
/// because it ignores the `-wal` sidecar: it would return a stale snapshot and
/// the check would call it exact. Which one a siglake catalog is: sqlx does
/// not set `journal_mode` unless it is asked to and nothing in this workspace
/// asks, so `SqlSegmentClaim`'s own SQLite databases are rollback-journal and
/// `mode=ro` alone covers them. The reader's failure message names both
/// remedies rather than reaching for the second one itself.
#[tokio::test]
async fn a_read_only_mount_reads_a_rollback_catalog_but_needs_immutable_for_a_wal_one() {
    let tmp = tempfile::tempdir().unwrap();
    let row = ("s1", "acme", "", "wal-mirror/acme/s1.arrow");

    // Arm A: the journal mode this workspace's own catalogs are in.
    let dir_a = tmp.path().join("rollback");
    std::fs::create_dir_all(&dir_a).unwrap();
    let db_a = dir_a.join("catalog.db");
    ledger(&db_a, &[row]).await;
    assert_eq!(
        header_journal(&db_a),
        "rollback",
        "sqlx leaves journal_mode alone, so this is what a siglake SQLite catalog is"
    );
    let (enforced, _restore_a) = deny_writes(&dir_a);
    if !enforced {
        return;
    }
    let reader = WalLedgerReader::open(&format!("sqlite://{}", db_a.display()))
        .await
        .expect("a rollback-journal catalog reads off a read-only mount");
    assert_eq!(reader.lookup(&ids(&["s1"])).await.unwrap().len(), 1);
    reader.close().await;

    // Arm B: WAL mode, and no `-shm` left behind by an earlier writable open.
    let dir_b = tmp.path().join("wal-mode");
    std::fs::create_dir_all(&dir_b).unwrap();
    let db_b = dir_b.join("catalog.db");
    sqlx::any::install_default_drivers();
    let pool = AnyPool::connect(&format!("sqlite://{}?mode=rwc", db_b.display()))
        .await
        .unwrap();
    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE wal_segments (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, \
         index_id TEXT NOT NULL, segment_url TEXT NOT NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO wal_segments VALUES ('s1', 'acme', '', 'wal-mirror/acme/s1.arrow')")
        .execute(&pool)
        .await
        .unwrap();
    // Fold the sidecars back into the database before anything reads it. An
    // uncheckpointed `-wal` is a THIRD state — `immutable=1` ignores it and
    // would read a stale snapshot — and it is not what this arm measures.
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    assert_eq!(header_journal(&db_b), "wal");
    // A `-shm` left behind by the writable open is enough to let a read-only
    // open succeed, which is exactly the confound this arm has to remove.
    for sidecar in ["catalog.db-wal", "catalog.db-shm"] {
        let path = dir_b.join(sidecar);
        if std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0) == 0 {
            let _ = std::fs::remove_file(&path);
        }
    }
    assert!(
        !dir_b.join("catalog.db-wal").exists(),
        "the checkpoint must have emptied the -wal"
    );
    let (enforced, _restore_b) = deny_writes(&dir_b);
    if !enforced {
        return;
    }
    let err = WalLedgerReader::open(&format!("sqlite://{}", db_b.display()))
        .await
        .expect_err("mode=ro cannot open a WAL-mode database it cannot write beside");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("WAL-journal SQLite catalog"),
        "the failure has to name both remedies: {msg}"
    );
    assert!(msg.contains("immutable=1"), "{msg}");
    // And the remedy the message names does read it.
    let reader = WalLedgerReader::open(&format!("sqlite://{}?immutable=1", db_b.display()))
        .await
        .expect("immutable=1 reads a rescued WAL-mode catalog");
    assert_eq!(
        reader.lookup(&ids(&["s1"])).await.unwrap()["s1"].tenant,
        "acme"
    );
    reader.close().await;
}

/// COST. The card's constraint is that the listing may be far larger than the
/// retained ledger, so the lookup is keyed by the LISTING and its memory is
/// the MATCHED set: `ceil(listing / LOOKUP_CHUNK)` queries, and a map holding
/// only the rows that matched. The whole-ledger alternative is one query and a
/// map holding the whole ledger, which is the unbounded side of the pair — a
/// fleet's backlog is not bounded by the objects one restore happens to list.
#[tokio::test]
async fn a_listing_far_larger_than_the_ledger_costs_one_query_per_chunk() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("catalog.db");
    let retained = 200usize;
    let listed_count = 2_000usize;
    let rows: Vec<(String, String, String, String)> = (0..retained)
        .map(|i| {
            (
                format!("seg-{i:06}"),
                "acme".to_string(),
                String::new(),
                format!("wal-mirror/acme/seg-{i:06}.arrow"),
            )
        })
        .collect();
    let borrowed: Vec<(&str, &str, &str, &str)> = rows
        .iter()
        .map(|(a, b, c, d)| (a.as_str(), b.as_str(), c.as_str(), d.as_str()))
        .collect();
    ledger(&db, &borrowed).await;

    let listed: Vec<String> = (0..listed_count).map(|i| format!("seg-{i:06}")).collect();
    let reader = WalLedgerReader::open(&format!("sqlite://{}", db.display()))
        .await
        .unwrap();
    let t0 = Instant::now();
    let matched = reader.lookup(&listed).await.unwrap();
    let chunked_ms = t0.elapsed().as_secs_f64() * 1e3;
    assert_eq!(
        reader.queries(),
        listed_count.div_ceil(LOOKUP_CHUNK),
        "one query per chunk of the LISTING"
    );
    assert_eq!(matched.len(), retained, "only the retained rows match");
    assert_eq!(reader.rows_read(), retained);
    println!(
        "listing={listed_count} ledger={retained}: {} queries, {} rows read, {} B resident, \
         {chunked_ms:.1} ms",
        reader.queries(),
        reader.rows_read(),
        resident_bytes(&matched),
    );

    // The inverse shape, where the LEDGER is the larger side: this is the one
    // that decides the form. The chunked lookup still reads only the
    // intersection; a whole-ledger scan would hold every row.
    let big = tmp.path().join("big.db");
    let big_rows: Vec<(String, String, String, String)> = (0..listed_count)
        .map(|i| {
            (
                format!("seg-{i:06}"),
                "acme".to_string(),
                String::new(),
                format!("wal-mirror/acme/seg-{i:06}.arrow"),
            )
        })
        .collect();
    let big_borrowed: Vec<(&str, &str, &str, &str)> = big_rows
        .iter()
        .map(|(a, b, c, d)| (a.as_str(), b.as_str(), c.as_str(), d.as_str()))
        .collect();
    ledger(&big, &big_borrowed).await;
    let small = WalLedgerReader::open(&format!("sqlite://{}", big.display()))
        .await
        .unwrap();
    let matched_small = small.lookup(&listed[..retained]).await.unwrap();
    assert_eq!(small.queries(), 1);
    assert_eq!(
        small.rows_read(),
        retained,
        "the ledger is 10x the listing and the lookup reads the intersection"
    );
    println!(
        "listing={retained} ledger={listed_count}: {} queries, {} rows read, {} B resident",
        small.queries(),
        small.rows_read(),
        resident_bytes(&matched_small),
    );
    reader.close().await;
    small.close().await;
}

/// Heap the lookup table actually holds: every string byte plus the fixed
/// per-entry cost. Deterministic, unlike an RSS delta on a shared box.
fn resident_bytes(rows: &HashMap<String, LedgerRow>) -> usize {
    rows.iter()
        .map(|(id, row)| {
            id.len()
                + row.tenant.len()
                + row.index_id.len()
                + row.segment_url.len()
                + std::mem::size_of::<String>() * 4
                + std::mem::size_of::<LedgerRow>()
        })
        .sum()
}

// ---------------------------------------------------------------------------
// the composition
// ---------------------------------------------------------------------------

/// One real sealed segment under `dir`: the bodies matter, because
/// `plan_recovery` GETs and decodes every candidate it would write (#5077).
fn seal_one(dir: &Path, body: &str) -> std::path::PathBuf {
    let mut w = siglake_wal::WalWriter::with_thresholds(
        dir,
        "ing-1",
        1,
        std::time::Duration::from_secs(60),
    )
    .unwrap();
    w.append_events(&[siglake_core::Event::now(body.to_string())])
        .unwrap()
        .expect("one row seals the segment");
    siglake_wal::list_sealed(dir)
        .unwrap()
        .pop()
        .expect("a sealed segment")
}

fn fs_op(root: &Path) -> opendal::Operator {
    opendal::Operator::new(opendal::services::Fs::default().root(root.to_str().unwrap()))
        .unwrap()
        .finish()
}

/// The whole slice, end to end: the ledger settles a listing the shipped
/// marker verdict leaves `Unverified`, in BOTH directions.
///
/// This mirror has no `_active/` object and no `owner` marker — the default
/// install — so `RootVerdict` is `Unverified` from the right `--from` AND from
/// one component above it, which is the entire reason option D exists. The
/// rows come from `SqlSegmentClaim::register`, the uploader's own path, so the
/// `segment_url` spelling the normalization depends on is not this file's
/// invention.
#[tokio::test]
async fn the_ledger_settles_a_listing_the_shipped_marker_verdict_leaves_unverified() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let events = seal_one(&src.join("acme"), "row-a");
    let index = seal_one(&src.join("acme").join("orders"), "row-b");
    let events_name = events.file_name().unwrap().to_str().unwrap().to_string();
    let index_name = index.file_name().unwrap().to_str().unwrap().to_string();
    let warehouse = tmp.path().join("store").join("warehouse");
    let mirror = warehouse.join("wal-mirror");
    for (key, src) in [
        (format!("acme/{events_name}"), &events),
        (format!("acme/orders/{index_name}"), &index),
    ] {
        let dest = mirror.join(&key);
        std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
        std::fs::copy(src, &dest).unwrap();
    }
    let db = tmp.path().join("catalog.db");
    let uri = format!("sqlite://{}", db.display());
    let claim = SqlSegmentClaim::connect(&format!("{uri}?mode=rwc"), "uploader")
        .await
        .unwrap();
    for (name, index_id, key) in [
        (&events_name, "", format!("acme/{events_name}")),
        (&index_name, "orders", format!("acme/orders/{index_name}")),
    ] {
        claim
            .register(
                name.trim_end_matches(".arrow"),
                "acme",
                index_id,
                // The url `sync_mirror_to_catalog` composes: the mirror key
                // with its prefix, relative to the warehouse root.
                &format!("wal-mirror/{key}"),
                1,
                1,
            )
            .await
            .unwrap();
    }
    drop(claim);
    let wal = tmp.path().join("wal");

    // From the MIRROR ROOT. The plan routes correctly and cannot say so.
    let plan = plan_recovery(&fs_op(&mirror), "", &wal).await.unwrap();
    assert_eq!(plan.verdict, RootVerdict::Unverified);
    assert_eq!(plan.segments(), 2);
    let reader = WalLedgerReader::open(&uri).await.unwrap();
    let rows = reader.lookup(&plan.listed_ids()).await.unwrap();
    let verdict = ledger_verdict(&plan.listed, &rows);
    let LedgerVerdict::Confirmed {
        matched,
        uncertified,
        prefix,
        ..
    } = &verdict
    else {
        panic!("the catalog settles the default install: {verdict:?}");
    };
    assert_eq!((*matched, *uncertified), (2, 0));
    assert_eq!(prefix, "wal-mirror");
    reader.close().await;

    // From ONE COMPONENT UP — the warehouse URL, the string an operator has.
    let plan_up = plan_recovery(&fs_op(&warehouse), "", &wal).await.unwrap();
    assert_eq!(
        plan_up.verdict,
        RootVerdict::Unverified,
        "no marker: the shipped verdict cannot see this"
    );
    assert_eq!(
        plan_up
            .groups
            .iter()
            .map(|g| (g.tenant.as_str(), g.index.as_deref().unwrap_or("")))
            .collect::<Vec<_>>(),
        vec![("wal-mirror", "acme")],
        "and it would write a tenant named after the mirror prefix"
    );
    assert_eq!(plan_up.skipped, 1, "the deeper key is refused on its depth");
    let reader = WalLedgerReader::open(&uri).await.unwrap();
    let rows = reader.lookup(&plan_up.listed_ids()).await.unwrap();
    let verdict = ledger_verdict(&plan_up.listed, &rows);
    let LedgerVerdict::Contradicted {
        disagreements,
        directory,
        ..
    } = &verdict
    else {
        panic!("the catalog refuses the near miss: {verdict:?}");
    };
    assert_eq!(
        disagreements.len(),
        2,
        "including the key the plan skipped, which has no candidate to report"
    );
    assert_eq!(directory.as_deref(), Some("wal-mirror"));
    reader.close().await;

    // And the apply refuses it, having created nothing under the WAL root.
    let mut plan_up = plan_up;
    plan_up.attach_ledger(verdict);
    assert!(plan_up.refused());
    let err = siglake_wal::mirror::apply_plan(&fs_op(&warehouse), plan_up, &wal)
        .await
        .expect_err("a ledger-contradicted plan must refuse");
    assert!(format!("{err:#}").contains("root CONTRADICTED by the catalog"));
    assert!(!wal.exists(), "{} was created", wal.display());
}

/// A mirror with no catalog behaves exactly as it does today: no lookup, no
/// ledger verdict on the plan, and nothing refused on catalog grounds.
#[tokio::test]
async fn a_mirror_with_no_catalog_is_unchanged() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let seg = seal_one(&src.join("acme"), "row-a");
    let mirror = tmp.path().join("mirror");
    let dest = mirror.join("acme").join(seg.file_name().unwrap());
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    std::fs::copy(&seg, &dest).unwrap();

    let wal = tmp.path().join("wal");
    let plan = plan_recovery(&fs_op(&mirror), "", &wal).await.unwrap();
    assert_eq!(plan.verdict, RootVerdict::Unverified);
    assert!(plan.ledger().is_none());
    assert!(!plan.refused());
    assert!(plan.refusal_line().is_none());
    let summary = siglake_wal::mirror::apply_plan(&fs_op(&mirror), plan, &wal)
        .await
        .unwrap();
    assert_eq!(summary.pulled, 1);
}
