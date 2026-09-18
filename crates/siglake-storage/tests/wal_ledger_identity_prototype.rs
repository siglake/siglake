//! Task #4974: option D of `docs/DESIGN_wal_recovery_root_identity.md`
//! qualified against a hermetic SQLite ledger, before any `--catalog` flag
//! exists.
//!
//! #4973 shipped option C: `wal-recover` plans unless it is given `--apply`,
//! and the listing carries a root verdict read off the two store-side markers.
//! The population that has neither marker — no managed index, active mirroring
//! off — gets `Unverified` and the plan. Option D is the exact answer for the
//! part of that population that still has its catalog: `wal_segments` holds the
//! uploader's own `(tenant, index_id, segment_url)` for every object it
//! registered, so the listed ids can be looked up and the routing the KEY
//! implies compared against the routing the LEDGER recorded.
//!
//! What these cases settle, and what the design doc
//! `docs/DESIGN_wal_recovery_ledger_identity.md` records from them:
//!
//! - complete, partial, absent and conflicting id matches, each with a rule;
//! - a ledger that cannot be read at all, and one whose rows retention has
//!   purged (the same thing as an absent match, which is why it needs saying);
//! - the normalization between a full `--from` URL and a root-relative
//!   `segment_url`;
//! - disagreement REFUSES and never reroutes: no object is moved to the
//!   routing the ledger claims, and a partial match certifies only the objects
//!   it matched;
//! - the lookup's cost against a listing far larger than the retained ledger.
//!
//! Every SQL statement here is a SELECT, and the reader opens SQLite
//! `mode=ro`, because `SqlSegmentClaim::connect` runs `ensure_schema`
//! (`crates/siglake-storage/src/catalog_claim.rs:291`) — recovery planning
//! must not migrate the catalog it is inspecting. That is not a matter of
//! discipline here: `a_read_only_lookup_leaves_the_ledger_file_byte_identical`
//! A/Bs it against `SqlSegmentClaim::connect` on the same file.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use siglake_storage::catalog_claim::SqlSegmentClaim;
use sqlx::{AnyPool, Row};

/// Conservative `IN (...)` width, the same one `purge_committed_ids` uses
/// (`catalog_claim.rs:1509`): well under SQLite's variable limit and under
/// Postgres's 65535 bind cap.
const LOOKUP_CHUNK: usize = 256;

/// The identity columns of one `wal_segments` row. Write-once: no `UPDATE` in
/// `catalog_claim.rs` touches `tenant`, `index_id` or `segment_url` — every one
/// of them sets `status`, `claimer`, `attempts`, `not_before_ms` or a
/// timestamp — so a row's identity is whatever `register` (the uploader) or
/// `mark_committed_local` (the filesystem drain) inserted, whatever the
/// segment's lifecycle has done since.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LedgerRow {
    tenant: String,
    /// `""` for the built-in events table, as stored.
    index_id: String,
    /// Root-relative `<mirror prefix>/<tenant>/<index>/<id>.arrow`.
    segment_url: String,
}

/// The routing a mirror KEY implies, in the ledger's own spelling.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Route {
    tenant: String,
    index_id: String,
}

/// One listed object, as the ledger check sees it.
#[derive(Debug, Clone)]
struct Listed {
    /// Key relative to `--from`.
    key: String,
    /// Segment id: the basename with `.arrow` (and any `.partial`) removed.
    id: String,
    /// The routing `recovery_target` would give it, or `None` for a key whose
    /// layout recovery refuses — which is still looked up, because a refused
    /// key's id is as good as any other and the generic "restored nothing"
    /// bail is exactly the case the ledger can make definite.
    route: Option<Route>,
}

/// `<tenant>[/<index>]` from a key suffix, refusing what `recovery_target`
/// refuses.
///
/// Equivalent to `siglake_wal::mirror::parse_mirror_key_suffix` on the depths
/// that parser is defined for, and pinned against it in
/// `the_prototype_infers_the_same_routing_as_the_shipped_parser`. It is spelled
/// out here because the shipped parser folds "deeper than the layout allows"
/// into the default tenant, while `recovery_target` — the function the plan
/// actually routes on — refuses it, and this check has to agree with the plan.
fn infer_route(key: &str) -> Option<Route> {
    let trimmed = key.trim_matches('/');
    // A first component `_active` is the active mirror's own namespace, not a
    // tenant; anything else is the sealed layout verbatim.
    let body = trimmed.strip_prefix("_active/").unwrap_or(trimmed);
    let parts: Vec<&str> = body.split('/').filter(|p| !p.is_empty()).collect();
    let (filename, dirs) = parts.split_last()?;
    filename
        .strip_suffix(".arrow.partial")
        .or_else(|| filename.strip_suffix(".arrow"))?;
    let (tenant, index_id) = match dirs {
        [] => ("default", ""),
        [tenant] => (*tenant, ""),
        [tenant, index] => (*tenant, *index),
        _ => return None,
    };
    Some(Route {
        tenant: tenant.to_string(),
        index_id: index_id.to_string(),
    })
}

/// The SEALED form of a listed key: the shape the ledger's `segment_url` is
/// always written in. The active mirror's `_active/` component and `.partial`
/// tail are the uploader's staging spelling, and there is no ledger row for the
/// staged copy — `register` runs on the sealed upload — so a `.partial` key can
/// only be compared against a row in this form.
fn sealed_form(key: &str) -> String {
    let body = key.trim_matches('/');
    let body = body.strip_prefix("_active/").unwrap_or(body);
    body.strip_suffix(".partial").unwrap_or(body).to_string()
}

/// Segment id of a listed key, or `None` for a key that is not a segment at
/// all (`owner`, a stray `README.md`).
fn segment_id(key: &str) -> Option<String> {
    let name = key.trim_matches('/').rsplit('/').next()?;
    Some(
        name.strip_suffix(".arrow.partial")
            .or_else(|| name.strip_suffix(".arrow"))?
            .to_string(),
    )
}

/// The components of a root-relative `segment_url` that sit ABOVE the listed
/// key — the mirror prefix, as seen from `--from`.
///
/// This is the whole normalization between the two spellings. `--from` is a
/// full URL (`s3://bucket/warehouse/wal-mirror`) and the store is rooted at it,
/// so a listed key is relative to the mirror root; `segment_url` is relative to
/// the WAREHOUSE root and therefore carries the prefix. Neither string can be
/// compared with the other directly, and the bucket and host in `--from` must
/// not be compared at all: restoring from a COPY of the mirror in another
/// bucket is a legitimate DR shape, and a url match would refuse it.
///
/// Returns `Some(vec![])` when the url IS the key — which means `--from` is at
/// or above the warehouse root, i.e. one or more components too high — and
/// `None` when the url does not end in the key at all, which means the row and
/// the object disagree about where the object is.
fn prefix_above(segment_url: &str, key: &str) -> Option<Vec<String>> {
    let url = segment_url.trim_matches('/');
    let key = sealed_form(key);
    if url == key {
        return Some(Vec::new());
    }
    let head = url.strip_suffix(&key)?.strip_suffix('/')?;
    Some(head.split('/').map(str::to_string).collect())
}

/// One listed object whose ledger row contradicts its key.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Disagreement {
    key: String,
    /// What the key says.
    inferred: Option<Route>,
    /// What the ledger says.
    ledger: LedgerRow,
    reason: &'static str,
}

/// What the ledger says about `--from`, on top of #4973's marker verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LedgerVerdict {
    /// Every matched object's ledger row agrees with its key, and they agree
    /// with each other about the prefix. `uncertified` objects had no row and
    /// keep the routing their key implies — the verdict says nothing about
    /// them.
    Confirmed {
        matched: usize,
        uncertified: Vec<String>,
        prefix: String,
        evidence: String,
    },
    /// At least one matched object's row contradicts its key. The restore is
    /// refused WHOLE: the objects that did agree are not a licence to write the
    /// ones that did not, and nothing is rerouted onto what the ledger claims.
    Contradicted {
        disagreements: Vec<Disagreement>,
        agreed: usize,
        uncertified: Vec<String>,
        /// The directory under `--from` to pass instead, when the listing says
        /// so.
        directory: Option<String>,
    },
    /// No listed object has a row. A ledger that was reachable and had nothing
    /// to say is not evidence: this is the retention-purged mirror and the
    /// foreign-mirror mirror alike, and #4973's verdict stands unchanged.
    Silent { listed: usize },
    /// The ledger could not be read.
    Unavailable { reason: String },
}

impl LedgerVerdict {
    /// One line for the operator, in the register of `RootVerdict::line`.
    fn line(&self) -> String {
        match self {
            Self::Confirmed {
                matched,
                uncertified,
                prefix,
                evidence,
            } => format!(
                "root confirmed by the catalog: {matched} listed segment(s) match a \
                 wal_segments row and every one routes as its key does, under the mirror \
                 prefix `{prefix}` (e.g. `{evidence}`). {} listed segment(s) have no row \
                 and keep the routing their key implies",
                uncertified.len()
            ),
            Self::Contradicted {
                disagreements,
                agreed,
                directory,
                ..
            } => {
                let first = &disagreements[0];
                format!(
                    "root CONTRADICTED by the catalog: `{}` is registered as \
                     tenant={} index={} at `{}` ({}), so --from is not the mirror root. \
                     {agreed} other listed segment(s) did agree; the restore is refused whole \
                     and nothing is rerouted.{}",
                    first.key,
                    first.ledger.tenant,
                    if first.ledger.index_id.is_empty() {
                        "<events>"
                    } else {
                        &first.ledger.index_id
                    },
                    first.ledger.segment_url,
                    first.reason,
                    directory
                        .as_deref()
                        .map(|d| format!(" Pass the `{d}` directory under it instead."))
                        .unwrap_or_default()
                )
            }
            Self::Silent { listed } => format!(
                "catalog reachable and silent: none of the {listed} listed object(s) has a \
                 wal_segments row, so the catalog adds nothing. Read the plan"
            ),
            Self::Unavailable { reason } => {
                format!("catalog could not be read: {reason}")
            }
        }
    }
}

/// The ledger check itself: pure arithmetic over a listing and the rows its ids
/// matched.
fn ledger_verdict(listing: &[Listed], rows: &HashMap<String, LedgerRow>) -> LedgerVerdict {
    let mut disagreements: Vec<Disagreement> = Vec::new();
    let mut agreed: Vec<(String, Vec<String>)> = Vec::new();
    let mut uncertified: Vec<String> = Vec::new();
    for item in listing {
        let Some(row) = rows.get(&item.id) else {
            uncertified.push(item.key.clone());
            continue;
        };
        let prefix = prefix_above(&row.segment_url, &item.key);
        let route_disagrees = item
            .route
            .as_ref()
            .is_some_and(|r| r.tenant != row.tenant || r.index_id != row.index_id);
        let reason = match (&prefix, route_disagrees) {
            (_, true) => Some("the key routes somewhere else"),
            (None, _) => Some("the registered url does not end in the listed key"),
            (Some(p), _) if p.is_empty() => {
                Some("the listed key already carries the mirror prefix, so --from is above it")
            }
            _ => None,
        };
        match reason {
            Some(reason) => disagreements.push(Disagreement {
                key: item.key.clone(),
                inferred: item.route.clone(),
                ledger: row.clone(),
                reason,
            }),
            None => agreed.push((item.key.clone(), prefix.expect("checked above"))),
        }
    }
    if !disagreements.is_empty() {
        // The directory to pass instead is the first component of a
        // contradicting key: the ledger proved the object is one level deeper
        // than `--from` claimed.
        let directory = disagreements
            .iter()
            .find_map(|d| {
                let parts: Vec<&str> = d.key.trim_matches('/').split('/').collect();
                // A single-component key has no directory under `--from` to
                // name, and the refusal says so by omitting the sentence.
                (parts.len() > 1).then(|| parts[0].to_string())
            })
            .filter(|d| !d.is_empty());
        return LedgerVerdict::Contradicted {
            disagreements,
            agreed: agreed.len(),
            uncertified,
            directory,
        };
    }
    let Some((first_key, first_prefix)) = agreed.first().cloned() else {
        return LedgerVerdict::Silent {
            listed: listing.len(),
        };
    };
    // Two agreeing objects that disagree about the prefix are not one mirror.
    if let Some((key, other)) = agreed.iter().find(|(_, p)| *p != first_prefix) {
        return LedgerVerdict::Contradicted {
            disagreements: vec![Disagreement {
                key: key.clone(),
                inferred: None,
                ledger: LedgerRow {
                    tenant: String::new(),
                    index_id: String::new(),
                    segment_url: other.join("/"),
                },
                reason: "two listed segments are registered under different mirror prefixes",
            }],
            agreed: agreed.len(),
            uncertified,
            directory: None,
        };
    }
    LedgerVerdict::Confirmed {
        matched: agreed.len(),
        uncertified,
        prefix: first_prefix.join("/"),
        evidence: first_key,
    }
}

// ---------------------------------------------------------------------------
// the read-only reader
// ---------------------------------------------------------------------------

/// `SELECT` issued per chunk of ids. The only statement the check runs.
fn lookup_sql(chunk: usize) -> String {
    let placeholders = std::iter::repeat_n("?", chunk)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT id, tenant, index_id, segment_url FROM wal_segments WHERE id IN ({placeholders})"
    )
}

/// The whole-ledger alternative, measured against the chunked form in
/// `a_listing_far_larger_than_the_ledger_costs_one_query_per_chunk`.
const SCAN_SQL: &str = "SELECT id, tenant, index_id, segment_url FROM wal_segments";

/// Postgres's read-only fence. SQLite gets `mode=ro` in the URI; Postgres has
/// no URI equivalent, so the SELECTs run inside a transaction the server itself
/// refuses to write in.
const PG_READ_ONLY_BEGIN: &str = "START TRANSACTION READ ONLY";

/// Read-only ledger inspection. Counts its own queries so the cost can be
/// measured rather than asserted.
#[derive(Debug)]
struct LedgerReader {
    pool: AnyPool,
    queries: AtomicUsize,
    rows_read: AtomicUsize,
}

impl LedgerReader {
    /// Open `path` read-only and prove the table is there.
    ///
    /// The SQLite URI carries `mode=ro`, so the engine — not this code —
    /// refuses any write, `ensure_schema`'s DDL included. A missing
    /// `wal_segments` is reported as unavailable rather than as an empty
    /// ledger: "the catalog has no row for these ids" and "this is not a
    /// siglake catalog" must not read the same.
    async fn open_sqlite_read_only(path: &Path) -> Result<Self, String> {
        sqlx::any::install_default_drivers();
        let uri = format!("sqlite://{}?mode=ro", path.display());
        let pool = AnyPool::connect(&uri)
            .await
            .map_err(|e| format!("connect {uri}: {e}"))?;
        let this = Self {
            pool,
            queries: AtomicUsize::new(0),
            rows_read: AtomicUsize::new(0),
        };
        sqlx::query("SELECT id, tenant, index_id, segment_url FROM wal_segments LIMIT 1")
            .fetch_optional(&this.pool)
            .await
            .map_err(|e| format!("wal_segments is not readable: {e}"))?;
        this.queries.store(0, Ordering::Relaxed);
        Ok(this)
    }

    /// Look the listed ids up, `LOOKUP_CHUNK` at a time. Memory is the MATCHED
    /// set, not the ledger.
    async fn lookup(&self, ids: &[String]) -> Result<HashMap<String, LedgerRow>, String> {
        let mut out = HashMap::new();
        for chunk in ids.chunks(LOOKUP_CHUNK) {
            let sql = lookup_sql(chunk.len());
            let mut q = sqlx::query(&sql);
            for id in chunk {
                q = q.bind(id);
            }
            self.queries.fetch_add(1, Ordering::Relaxed);
            let rows = q
                .fetch_all(&self.pool)
                .await
                .map_err(|e| format!("lookup: {e}"))?;
            self.rows_read.fetch_add(rows.len(), Ordering::Relaxed);
            for row in rows {
                out.insert(
                    row.get::<String, _>("id"),
                    LedgerRow {
                        tenant: row.get("tenant"),
                        index_id: row.get("index_id"),
                        segment_url: row.get("segment_url"),
                    },
                );
            }
        }
        Ok(out)
    }

    /// The alternative: one query, and memory proportional to the LEDGER.
    async fn scan_all(&self) -> Result<HashMap<String, LedgerRow>, String> {
        self.queries.fetch_add(1, Ordering::Relaxed);
        let rows = sqlx::query(SCAN_SQL)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| format!("scan: {e}"))?;
        self.rows_read.fetch_add(rows.len(), Ordering::Relaxed);
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<String, _>("id"),
                    LedgerRow {
                        tenant: row.get("tenant"),
                        index_id: row.get("index_id"),
                        segment_url: row.get("segment_url"),
                    },
                )
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/// A ledger holding ONLY `wal_segments` — an older catalog, or one this check
/// has no business adding tables to — built without `ensure_schema` so the
/// read-only A/B has something to detect.
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
    // Checkpoint and release the `-wal`/`-shm` sidecars before anything reads
    // the file: a read-only open of a WAL-mode database with a live sidecar is
    // its own failure mode, pinned in
    // `a_ledger_whose_wal_sidecar_is_live_still_opens_read_only`.
    pool.close().await;
}

/// The listing a plan would hand the check.
fn listing(keys: &[&str]) -> Vec<Listed> {
    keys.iter()
        .filter_map(|key| {
            Some(Listed {
                key: (*key).to_string(),
                id: segment_id(key)?,
                route: infer_route(key),
            })
        })
        .collect()
}

fn ids(listing: &[Listed]) -> Vec<String> {
    listing.iter().map(|l| l.id.clone()).collect()
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

// ---------------------------------------------------------------------------
// the cases
// ---------------------------------------------------------------------------

/// COMPLETE match, correct root. Every listed segment has a row, every row
/// agrees with its key, and the prefix the rows carry is uniform.
#[tokio::test]
async fn a_complete_match_at_the_mirror_root_confirms_it() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("catalog.db");
    ledger(
        &db,
        &[
            ("s1", "acme", "", "wal-mirror/acme/s1.arrow"),
            ("s2", "acme", "orders", "wal-mirror/acme/orders/s2.arrow"),
            ("s3", "default", "", "wal-mirror/s3.arrow"),
        ],
    )
    .await;
    let listed = listing(&["acme/s1.arrow", "acme/orders/s2.arrow", "s3.arrow"]);
    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader.lookup(&ids(&listed)).await.unwrap();
    let verdict = ledger_verdict(&listed, &rows);
    let LedgerVerdict::Confirmed {
        matched,
        uncertified,
        prefix,
        ..
    } = &verdict
    else {
        panic!("a complete agreeing match must confirm: {verdict:?}");
    };
    assert_eq!(*matched, 3);
    assert!(uncertified.is_empty(), "{uncertified:?}");
    assert_eq!(prefix, "wal-mirror", "{}", verdict.line());
    // The legacy flat key is the one no store-side marker can vouch for, and
    // it is confirmed here on the same evidence as the other two.
    assert_eq!(reader.queries.load(Ordering::Relaxed), 1);
}

/// The headline refusal, and the worst line in #4964's table: a LEGACY FLAT
/// mirror one component up, whose report before #4973 was character-for-
/// character a correct restore and whose listing carries no marker at all.
#[tokio::test]
async fn a_flat_mirror_one_component_up_is_refused_by_the_ledger() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("catalog.db");
    ledger(
        &db,
        &[
            ("s1", "default", "", "wal-mirror/s1.arrow"),
            ("s2", "default", "", "wal-mirror/s2.arrow"),
        ],
    )
    .await;
    // `--from` is the warehouse, so every key already carries the prefix.
    let listed = listing(&["wal-mirror/s1.arrow", "wal-mirror/s2.arrow"]);
    // Without the ledger this reads as a per-tenant mirror at its root: a
    // tenant called `wal-mirror` with two events segments.
    assert_eq!(
        listed[0].route.as_ref().unwrap().tenant,
        "wal-mirror",
        "the key alone says the tenant is the prefix — that is the whole defect"
    );
    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader.lookup(&ids(&listed)).await.unwrap();
    let verdict = ledger_verdict(&listed, &rows);
    let LedgerVerdict::Contradicted {
        disagreements,
        agreed,
        directory,
        ..
    } = &verdict
    else {
        panic!("a ledger that routes these elsewhere must refuse: {verdict:?}");
    };
    assert_eq!(disagreements.len(), 2);
    assert_eq!(*agreed, 0);
    assert_eq!(
        directory.as_deref(),
        Some("wal-mirror"),
        "{}",
        verdict.line()
    );
    assert!(
        verdict.line().contains("nothing is rerouted"),
        "{}",
        verdict.line()
    );
}

/// The collision no key can resolve, resolved: a tenant legitimately CALLED
/// `wal-mirror`, whose keys and whose correct restore are byte-identical to the
/// case above.
#[tokio::test]
async fn a_tenant_named_after_the_prefix_is_confirmed_not_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("catalog.db");
    ledger(
        &db,
        &[
            ("s1", "wal-mirror", "", "wal-mirror/wal-mirror/s1.arrow"),
            ("s2", "wal-mirror", "", "wal-mirror/wal-mirror/s2.arrow"),
        ],
    )
    .await;
    let listed = listing(&["wal-mirror/s1.arrow", "wal-mirror/s2.arrow"]);
    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader.lookup(&ids(&listed)).await.unwrap();
    let verdict = ledger_verdict(&listed, &rows);
    let LedgerVerdict::Confirmed {
        matched, prefix, ..
    } = &verdict
    else {
        panic!("the legitimate install must be confirmed, not refused: {verdict:?}");
    };
    assert_eq!(*matched, 2);
    assert_eq!(prefix, "wal-mirror");
}

/// A mirror one component up whose keys are the DEEP layout. Recovery refuses
/// those keys on their depth and the command bails with a generic "restored
/// nothing, --from must name the mirror root" guess; the ledger makes it
/// definite and names the directory, from keys that are not candidates at all.
#[tokio::test]
async fn a_deep_mirror_one_component_up_is_refused_from_keys_the_plan_skipped() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("catalog.db");
    ledger(
        &db,
        &[("s1", "acme", "orders", "wal-mirror/acme/orders/s1.arrow")],
    )
    .await;
    let listed = listing(&["wal-mirror/acme/orders/s1.arrow"]);
    assert!(
        listed[0].route.is_none(),
        "the plan skips this key on its depth"
    );
    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader.lookup(&ids(&listed)).await.unwrap();
    let verdict = ledger_verdict(&listed, &rows);
    let LedgerVerdict::Contradicted { directory, .. } = &verdict else {
        panic!("the url alone settles this one: {verdict:?}");
    };
    assert_eq!(directory.as_deref(), Some("wal-mirror"));
}

/// PARTIAL match: retention purges a `committed` row as soon as its object is
/// gone (`purge_committed_ids`, `catalog_claim.rs:1509`), so a mirror routinely
/// holds objects with no row. The matched set confirms the ROOT — a property of
/// `--from`, not of the object — and the unmatched objects are named, keep the
/// routing their key implies, and are certified by nothing.
#[tokio::test]
async fn a_partial_match_confirms_the_root_and_certifies_only_what_it_matched() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("catalog.db");
    ledger(&db, &[("s1", "acme", "", "wal-mirror/acme/s1.arrow")]).await;
    let listed = listing(&["acme/s1.arrow", "acme/s2.arrow", "widgets/s3.arrow"]);
    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader.lookup(&ids(&listed)).await.unwrap();
    let verdict = ledger_verdict(&listed, &rows);
    let LedgerVerdict::Confirmed {
        matched,
        uncertified,
        ..
    } = &verdict
    else {
        panic!("one agreeing row pins the root: {verdict:?}");
    };
    assert_eq!(*matched, 1);
    assert_eq!(uncertified, &["acme/s2.arrow", "widgets/s3.arrow"]);
    // The routing of an uncertified object is the key's, unchanged: the ledger
    // matched nothing for it and therefore says nothing about it.
    assert_eq!(
        listed[2].route.as_ref().unwrap(),
        &Route {
            tenant: "widgets".to_string(),
            index_id: String::new(),
        }
    );
    assert!(
        verdict.line().contains("2 listed segment(s) have no row"),
        "{}",
        verdict.line()
    );
}

/// CONFLICTING match: some rows agree, one does not. Contradiction wins, the
/// same way a listing with markers at two depths refuses under #4973 — and the
/// agreeing rows are reported rather than used as a licence.
#[tokio::test]
async fn one_disagreeing_row_refuses_a_listing_the_rest_of_which_agrees() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("catalog.db");
    ledger(
        &db,
        &[
            ("s1", "acme", "", "wal-mirror/acme/s1.arrow"),
            ("s2", "acme", "", "wal-mirror/acme/s2.arrow"),
            // Registered under a DIFFERENT tenant than its key claims.
            ("s3", "widgets", "", "wal-mirror/widgets/s3.arrow"),
        ],
    )
    .await;
    let listed = listing(&["acme/s1.arrow", "acme/s2.arrow", "acme/s3.arrow"]);
    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader.lookup(&ids(&listed)).await.unwrap();
    let verdict = ledger_verdict(&listed, &rows);
    let LedgerVerdict::Contradicted {
        disagreements,
        agreed,
        ..
    } = &verdict
    else {
        panic!("a conflict must refuse whole: {verdict:?}");
    };
    assert_eq!(*agreed, 2, "the agreement is reported, not spent");
    assert_eq!(disagreements.len(), 1);
    assert_eq!(disagreements[0].key, "acme/s3.arrow");
    assert_eq!(
        disagreements[0].ledger.tenant, "widgets",
        "the refusal names the routing the ledger recorded, and does not apply it"
    );
    assert_eq!(
        disagreements[0].inferred.as_ref().unwrap().tenant,
        "acme",
        "and the routing the key implies, which stays in force for the plan"
    );
}

/// ABSENT match: the ledger is readable and has no row for anything listed.
/// This is a mirror whose rows retention has purged, and a mirror belonging to
/// another deployment, and it must read as "no evidence" rather than as either
/// a confirmation or a refusal.
#[tokio::test]
async fn a_reachable_ledger_with_no_matching_row_says_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("catalog.db");
    ledger(&db, &[("other", "acme", "", "wal-mirror/acme/other.arrow")]).await;
    let listed = listing(&["acme/s1.arrow", "wal-mirror/s2.arrow"]);
    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader.lookup(&ids(&listed)).await.unwrap();
    assert!(rows.is_empty());
    let verdict = ledger_verdict(&listed, &rows);
    assert_eq!(verdict, LedgerVerdict::Silent { listed: 2 });
    assert!(
        verdict.line().contains("Read the plan"),
        "{}",
        verdict.line()
    );
}

/// An EMPTY ledger — every row purged — is the same verdict, and specifically
/// not an error: a mirror whose backlog has fully drained is the healthy case.
#[tokio::test]
async fn an_empty_ledger_is_silent_not_unavailable() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("catalog.db");
    ledger(&db, &[]).await;
    let listed = listing(&["acme/s1.arrow"]);
    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader.lookup(&ids(&listed)).await.unwrap();
    assert_eq!(
        ledger_verdict(&listed, &rows),
        LedgerVerdict::Silent { listed: 1 }
    );
}

/// UNAVAILABLE ledger, in the two shapes a DR run produces: the DB is not
/// there at all (the second failure domain the card names), and the file is
/// there but is not a siglake catalog. Both are reported; neither is silently
/// downgraded to "no evidence", because an operator who asked for exact
/// evidence and got none must be told rather than handed a plan.
#[tokio::test]
async fn an_unreadable_ledger_is_reported_and_not_downgraded() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("gone.db");
    let err = LedgerReader::open_sqlite_read_only(&missing)
        .await
        .expect_err("a missing SQLite file must not open rwc");
    assert!(
        LedgerVerdict::Unavailable {
            reason: err.clone()
        }
        .line()
        .contains("could not be read"),
        "{err}"
    );

    // Present, readable, and not a catalog: `wal_segments` is missing.
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
    let err = LedgerReader::open_sqlite_read_only(&foreign)
        .await
        .expect_err("a database with no wal_segments is not a ledger");
    assert!(err.contains("wal_segments is not readable"), "{err}");
}

/// The normalization the two spellings need, and the DR shape a url STRING
/// comparison would refuse: the mirror has been copied into another bucket and
/// another path, and `--from` names the copy. The rows still carry the original
/// warehouse-relative url, the routing still agrees, and the root is confirmed.
#[tokio::test]
async fn a_relocated_mirror_copy_is_confirmed_because_only_the_tail_is_compared() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("catalog.db");
    ledger(
        &db,
        &[("s1", "acme", "orders", "lake/m/acme/orders/s1.arrow")],
    )
    .await;
    // `--from s3://dr-bucket/restore/2026-09-18/` — nothing in common with the
    // url's head but the tail.
    let listed = listing(&["acme/orders/s1.arrow"]);
    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader.lookup(&ids(&listed)).await.unwrap();
    let verdict = ledger_verdict(&listed, &rows);
    let LedgerVerdict::Confirmed { prefix, .. } = &verdict else {
        panic!("a copy of the mirror is a legitimate --from: {verdict:?}");
    };
    assert_eq!(
        prefix, "lake/m",
        "the prefix reported is the one the ledger recorded, multi-component and all"
    );
}

/// Two listed segments registered under DIFFERENT mirror prefixes: the routing
/// agrees for both and the listing is still not one mirror root. Refused.
#[tokio::test]
async fn two_prefixes_in_one_listing_are_refused_even_though_the_routing_agrees() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("catalog.db");
    ledger(
        &db,
        &[
            ("s1", "acme", "", "wal-mirror/acme/s1.arrow"),
            ("s2", "acme", "", "other-mirror/acme/s2.arrow"),
        ],
    )
    .await;
    let listed = listing(&["acme/s1.arrow", "acme/s2.arrow"]);
    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader.lookup(&ids(&listed)).await.unwrap();
    let verdict = ledger_verdict(&listed, &rows);
    assert!(
        matches!(verdict, LedgerVerdict::Contradicted { .. }),
        "{verdict:?}"
    );
    assert!(
        verdict.line().contains("different mirror prefixes"),
        "{}",
        verdict.line()
    );
}

/// An `_active/` object's key has no ledger row of its own — `register` runs on
/// the SEALED upload — so it is compared in its sealed form. Getting this wrong
/// would turn every active-mirror object into a disagreement and refuse the one
/// population #4973 can already confirm.
#[tokio::test]
async fn an_active_mirror_object_is_compared_in_its_sealed_form() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("catalog.db");
    ledger(&db, &[("s1", "acme", "", "wal-mirror/acme/s1.arrow")]).await;
    let listed = listing(&["_active/acme/s1.arrow.partial"]);
    assert_eq!(listed[0].id, "s1");
    assert_eq!(listed[0].route.as_ref().unwrap().tenant, "acme");
    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader.lookup(&ids(&listed)).await.unwrap();
    let verdict = ledger_verdict(&listed, &rows);
    let LedgerVerdict::Confirmed { prefix, .. } = &verdict else {
        panic!("the sealed row vouches for its own active prefix: {verdict:?}");
    };
    assert_eq!(prefix, "wal-mirror");
}

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

    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader
        .lookup(&["s1".to_string(), "s2".to_string()])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    reader.pool.close().await;
    assert_eq!(
        ledger_bytes(&dir),
        before,
        "a read-only lookup must not change one byte of the catalog"
    );

    // The negative control: the connect path recovery would otherwise reuse.
    let claim = SqlSegmentClaim::connect(
        &format!("sqlite://{}?mode=rwc", db.display()),
        "wal-recover-prototype",
    )
    .await
    .unwrap();
    drop(claim);
    let after = ledger_bytes(&dir);
    assert_ne!(
        after, before,
        "SqlSegmentClaim::connect runs ensure_schema, which is why the reader cannot use it"
    );
    // Name what it added, so the finding is not just "the bytes moved".
    let pool = AnyPool::connect(&format!("sqlite://{}?mode=ro", db.display()))
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

/// The composition, against the plan #4973 actually ships: the ledger check
/// runs on the same listing and covers exactly the gap the marker verdict
/// leaves. This mirror has no `_active/` object and no `owner` marker — the
/// default install — so `RootVerdict` is `Unverified` from the right `--from`
/// AND from one component above it, which is the whole reason option D exists.
#[tokio::test]
async fn the_ledger_settles_a_listing_the_shipped_marker_verdict_leaves_unverified() {
    use siglake_wal::mirror::{plan_recovery, RootVerdict};

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
    let registered: Vec<String> = [&events_name, &index_name]
        .iter()
        .map(|n| n.trim_end_matches(".arrow").to_string())
        .collect();
    let db = tmp.path().join("catalog.db");
    ledger(
        &db,
        &[
            (
                registered[0].as_str(),
                "acme",
                "",
                &format!("wal-mirror/acme/{events_name}"),
            ),
            (
                registered[1].as_str(),
                "acme",
                "orders",
                &format!("wal-mirror/acme/orders/{index_name}"),
            ),
        ],
    )
    .await;
    let wal = tmp.path().join("wal");

    // From the MIRROR ROOT. The plan routes correctly and cannot say so.
    let plan = plan_recovery(&fs_op(&mirror), "", &wal).await.unwrap();
    assert_eq!(plan.verdict, RootVerdict::Unverified);
    assert_eq!(plan.segments(), 2);
    let root_keys = [
        format!("acme/{events_name}"),
        format!("acme/orders/{index_name}"),
    ];
    // The prototype's inference is the plan's own routing, group for group.
    let mut plan_routes: Vec<(String, String)> = plan
        .groups
        .iter()
        .map(|g| (g.tenant.clone(), g.index.clone().unwrap_or_default()))
        .collect();
    plan_routes.sort();
    let listed = listing(&root_keys.iter().map(String::as_str).collect::<Vec<_>>());
    let mut inferred: Vec<(String, String)> = listed
        .iter()
        .map(|l| {
            let r = l.route.as_ref().unwrap();
            (r.tenant.clone(), r.index_id.clone())
        })
        .collect();
    inferred.sort();
    assert_eq!(
        plan_routes, inferred,
        "the check must route as the plan does"
    );
    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader.lookup(&ids(&listed)).await.unwrap();
    assert!(
        matches!(
            ledger_verdict(&listed, &rows),
            LedgerVerdict::Confirmed { matched: 2, .. }
        ),
        "{:?}",
        ledger_verdict(&listed, &rows)
    );
    reader.pool.close().await;

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
    let up_keys = [
        format!("wal-mirror/acme/{events_name}"),
        format!("wal-mirror/acme/orders/{index_name}"),
    ];
    let listed_up = listing(&up_keys.iter().map(String::as_str).collect::<Vec<_>>());
    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let rows = reader.lookup(&ids(&listed_up)).await.unwrap();
    let verdict = ledger_verdict(&listed_up, &rows);
    let LedgerVerdict::Contradicted {
        disagreements,
        directory,
        ..
    } = &verdict
    else {
        panic!("the ledger settles it: {verdict:?}");
    };
    assert_eq!(disagreements.len(), 2, "including the key the plan skipped");
    assert_eq!(directory.as_deref(), Some("wal-mirror"));
    reader.pool.close().await;
}

/// A ledger a live deployment is still writing must be readable. The writer
/// here holds its pool open across the read, which is the state a DR run finds
/// when the catalog survived and the ingesters did not.
#[tokio::test]
async fn a_ledger_a_live_deployment_is_still_writing_opens_read_only() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("live.db");
    sqlx::any::install_default_drivers();
    let writer = AnyPool::connect(&format!("sqlite://{}?mode=rwc", db.display()))
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE wal_segments (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, \
         index_id TEXT NOT NULL, segment_url TEXT NOT NULL)",
    )
    .execute(&writer)
    .await
    .unwrap();
    sqlx::query("INSERT INTO wal_segments VALUES ('s1', 'acme', '', 'wal-mirror/acme/s1.arrow')")
        .execute(&writer)
        .await
        .unwrap();

    let reader = LedgerReader::open_sqlite_read_only(&db)
        .await
        .expect("a live catalog must be readable read-only");
    let rows = reader.lookup(&["s1".to_string()]).await.unwrap();
    assert_eq!(rows["s1"].tenant, "acme");
    reader.pool.close().await;
    writer.close().await;
}

/// Journal mode from the database header, without opening the database —
/// opening a WAL-mode file is the thing being measured, and it has side
/// effects. Byte 18 is the write version: 1 legacy rollback journal, 2 WAL
/// (https://sqlite.org/fileformat.html#the_database_header).
fn header_journal(path: &Path) -> &'static str {
    let bytes = std::fs::read(path).unwrap();
    match bytes[18] {
        2 => "wal",
        _ => "rollback",
    }
}

/// Make `dir` unwritable and report whether the mode bits are enforced: root
/// ignores them, and so does a filesystem mounted without permission support.
fn deny_writes(dir: &Path) -> (bool, std::fs::Permissions) {
    let restore = std::fs::metadata(dir).unwrap().permissions();
    let mut perms = restore.clone();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o555);
    }
    std::fs::set_permissions(dir, perms).unwrap();
    (std::fs::write(dir.join("probe"), b"x").is_err(), restore)
}

/// A rescued catalog on a READ-ONLY mount, which is the DR shape `mode=ro` is
/// for — and the one place the open mode has to be chosen on evidence rather
/// than by name.
///
/// Measured, on this SQLite: a rollback-journal database opens `mode=ro` with
/// the directory unwritable, and a WAL-mode one does NOT — SQLite has to create
/// a `-shm` beside it, so the open fails `attempt to write a readonly database`
/// though every statement is a SELECT. `immutable=1` reads it, and is unsafe
/// against a LIVE catalog because it ignores the `-wal` sidecar: it would
/// return a stale snapshot and the check would call it exact.
///
/// Which one a siglake catalog is: sqlx does not set `journal_mode` unless it
/// is asked to (`sqlx-sqlite-0.8.6/src/options/mod.rs:177-181`), and nothing
/// in this workspace asks, so `SqlSegmentClaim`'s own SQLite databases are
/// rollback-journal and `mode=ro` alone covers them. The WAL arm is what a
/// catalog handed over by another tool costs.
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
    let (enforced, restore_a) = deny_writes(&dir_a);
    if !enforced {
        std::fs::set_permissions(&dir_a, restore_a).unwrap();
        return;
    }
    let reader = LedgerReader::open_sqlite_read_only(&db_a)
        .await
        .expect("a rollback-journal catalog reads off a read-only mount");
    assert_eq!(reader.lookup(&["s1".to_string()]).await.unwrap().len(), 1);
    reader.pool.close().await;
    std::fs::set_permissions(&dir_a, restore_a).unwrap();

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
    sqlx::query("CREATE TABLE wal_segments (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, index_id TEXT NOT NULL, segment_url TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO wal_segments VALUES ('s1', 'acme', '', 'wal-mirror/acme/s1.arrow')")
        .execute(&pool)
        .await
        .unwrap();
    // Fold the sidecars back into the database before anything reads it. An
    // uncheckpointed `-wal` is a THIRD state — `immutable=1` ignores it and
    // would read a stale snapshot — and it is not what this arm is measuring.
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
        "the checkpoint must have emptied the -wal: {:?}",
        std::fs::read_dir(&dir_b)
            .unwrap()
            .flatten()
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );
    let (enforced, restore_b) = deny_writes(&dir_b);
    if !enforced {
        std::fs::set_permissions(&dir_b, restore_b).unwrap();
        return;
    }
    let err = LedgerReader::open_sqlite_read_only(&db_b)
        .await
        .expect_err("mode=ro cannot open a WAL-mode database it cannot write beside");
    assert!(
        err.contains("readonly database") || err.contains("unable to open"),
        "{err}"
    );
    let pool = AnyPool::connect(&format!("sqlite://{}?mode=ro&immutable=1", db_b.display()))
        .await
        .expect("immutable=1 reads a rescued WAL-mode catalog");
    let got = sqlx::query("SELECT tenant FROM wal_segments WHERE id = 's1'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(got.get::<String, _>("tenant"), "acme");
    pool.close().await;
    std::fs::set_permissions(&dir_b, restore_b).unwrap();
}

/// The prototype's own routing inference must agree with the shipped parser
/// wherever that parser is defined, or this qualification is measuring
/// something the plan does not do.
#[test]
fn the_prototype_infers_the_same_routing_as_the_shipped_parser() {
    for key in [
        "s1.arrow",
        "acme/s1.arrow",
        "acme/orders/s1.arrow",
        "wal-mirror/s1.arrow",
        "wal-mirror/acme/s1.arrow",
    ] {
        let (tenant, index_id) = siglake_wal::mirror::parse_mirror_key_suffix(key);
        let mine = infer_route(key).expect("a depth the parser defines");
        assert_eq!(
            (mine.tenant.as_str(), mine.index_id.as_str()),
            (tenant.as_str(), index_id.as_str()),
            "{key}"
        );
    }
    // Deeper than the layout allows: the shipped parser folds it into the
    // default tenant, `recovery_target` refuses it, and this check follows
    // `recovery_target` because that is what the plan routes on.
    assert!(infer_route("a/b/c/s1.arrow").is_none());
    assert_eq!(
        siglake_wal::mirror::parse_mirror_key_suffix("a/b/c/s1.arrow"),
        ("default".to_string(), String::new())
    );
    // Not a segment at all.
    assert!(segment_id("acme/orders/owner").is_none());
    assert!(segment_id("README.md").is_none());
}

/// Both statements the check would run against Postgres, parsed in the dialect
/// that would run them. There is no Postgres in a lane, and a Postgres-only
/// syntax error on this path would be a runtime failure during a disaster
/// recovery — the worst possible time to find it.
#[test]
fn the_read_only_statements_parse_as_postgres() {
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    // `?` -> `$n`, the rewrite `Dialect::Postgres` performs.
    let mut n = 0;
    let rendered: String = lookup_sql(3)
        .chars()
        .map(|c| {
            if c == '?' {
                n += 1;
                format!("${n}")
            } else {
                c.to_string()
            }
        })
        .collect();
    for sql in [PG_READ_ONLY_BEGIN, SCAN_SQL, rendered.as_str(), "COMMIT"] {
        let parsed = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap_or_else(|e| panic!("does not parse as Postgres: {e}\n{sql}"));
        assert_eq!(parsed.len(), 1, "one statement per execute(): {sql}");
    }
}

/// COST. The card's constraint is that the listing may be far larger than the
/// retained ledger, so the lookup is keyed by the LISTING and its memory is the
/// MATCHED set: `ceil(listing / LOOKUP_CHUNK)` queries, and a map holding only
/// the rows that matched. The whole-ledger alternative is one query and a map
/// holding the whole ledger, which is the unbounded side of the pair.
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

    let keys: Vec<String> = (0..listed_count)
        .map(|i| format!("acme/seg-{i:06}.arrow"))
        .collect();
    let listed = listing(&keys.iter().map(String::as_str).collect::<Vec<_>>());
    assert_eq!(listed.len(), listed_count);

    let reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let t0 = Instant::now();
    let matched = reader.lookup(&ids(&listed)).await.unwrap();
    let chunked_ms = t0.elapsed().as_secs_f64() * 1e3;
    let chunked_queries = reader.queries.load(Ordering::Relaxed);
    assert_eq!(
        chunked_queries,
        listed_count.div_ceil(LOOKUP_CHUNK),
        "one query per chunk of the LISTING"
    );
    assert_eq!(matched.len(), retained, "only the retained rows match");
    let chunked_bytes = resident_bytes(&matched);

    let scan_reader = LedgerReader::open_sqlite_read_only(&db).await.unwrap();
    let t1 = Instant::now();
    let all = scan_reader.scan_all().await.unwrap();
    let scan_ms = t1.elapsed().as_secs_f64() * 1e3;
    assert_eq!(scan_reader.queries.load(Ordering::Relaxed), 1);
    let scan_bytes = resident_bytes(&all);

    let verdict = ledger_verdict(&listed, &matched);
    let LedgerVerdict::Confirmed {
        matched: m,
        uncertified,
        ..
    } = &verdict
    else {
        panic!("{verdict:?}");
    };
    assert_eq!(*m, retained);
    assert_eq!(uncertified.len(), listed_count - retained);

    println!(
        "listing={listed_count} ledger={retained}  chunked: {chunked_queries} queries, \
         {} rows read, {chunked_bytes} B resident, {chunked_ms:.1} ms  |  \
         whole-ledger scan: 1 query, {} rows read, {scan_bytes} B resident, {scan_ms:.1} ms",
        reader.rows_read.load(Ordering::Relaxed),
        scan_reader.rows_read.load(Ordering::Relaxed),
    );

    // The inverse shape, where the LEDGER is the larger side: this is the one
    // that decides the form, because a fleet's backlog is not bounded by the
    // objects one restore happens to list.
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
    let small_listing = listing(
        &keys[..retained]
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    );
    let r = LedgerReader::open_sqlite_read_only(&big).await.unwrap();
    let matched_small = r.lookup(&ids(&small_listing)).await.unwrap();
    let s = LedgerReader::open_sqlite_read_only(&big).await.unwrap();
    let all_big = s.scan_all().await.unwrap();
    println!(
        "listing={retained} ledger={listed_count}  chunked: {} queries, {} B resident  |  \
         whole-ledger scan: 1 query, {} B resident ({:.1}x)",
        r.queries.load(Ordering::Relaxed),
        resident_bytes(&matched_small),
        resident_bytes(&all_big),
        resident_bytes(&all_big) as f64 / resident_bytes(&matched_small) as f64,
    );
    assert!(
        resident_bytes(&all_big) > resident_bytes(&matched_small),
        "the scan form holds the whole ledger whatever the listing asked for"
    );
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
