//! Read-only `wal_segments` identity lookup for `siglake wal-recover
//! --catalog` (#4997).
//!
//! The uploader recorded where each mirrored object belongs before the volume
//! was lost: `register` writes `(id, tenant, index_id, segment_url)` for every
//! object it PUT ([`crate::catalog_claim::SqlSegmentClaim::register`]), and
//! `mark_committed_local` writes the same three columns for a segment the
//! filesystem drain committed with no row. Recovery can therefore look the
//! listed ids up and compare the routing the KEY implies against the routing
//! the LEDGER recorded. The rules, the partial-match policy and the
//! measurements are in `docs/DESIGN_wal_recovery_ledger_identity.md`; the
//! arithmetic is [`siglake_wal::mirror::ledger_verdict`], and this module is
//! only the reader — it returns that verdict's own [`LedgerRow`] rather than a
//! second spelling of the same three columns for a caller to copy across by
//! hand.
//!
//! Two constraints shape it, and neither is a matter of discipline:
//!
//! - **No DDL.** [`crate::catalog_claim::SqlSegmentClaim::connect`] runs
//!   `ensure_schema`, so reusing it would migrate the catalog a recovery PLAN
//!   is inspecting. SQLite is opened `mode=ro` and Postgres runs its SELECTs
//!   inside `START TRANSACTION READ ONLY`, so the ENGINE refuses a write
//!   rather than this code remembering not to issue one.
//! - **The listing bounds the cost, not the ledger.** The lookup is keyed by
//!   the listing, [`LOOKUP_CHUNK`] ids at a time, so its queries scale with
//!   the listing and its memory with the MATCHED set. A fleet's retained
//!   backlog is not bounded by the objects one `--from` happens to list, which
//!   is why the one-query whole-ledger scan is not the form shipped.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use sqlx::any::AnyPoolOptions;
use sqlx::pool::PoolConnection;
use sqlx::{Any, AnyPool, Row};
use tokio::sync::Mutex;

use siglake_wal::mirror::LedgerRow;

use crate::catalog_claim::Dialect;

/// Conservative `IN (...)` width, the same one `purge_committed_ids` uses:
/// well under SQLite's variable limit and under Postgres's 65535 bind cap.
pub const LOOKUP_CHUNK: usize = 256;

/// The `SELECT` issued per chunk of ids. The only statement shape the lookup
/// runs, and it reads no lifecycle column: a row's identity is write-once, so
/// `status` is irrelevant to it.
fn lookup_sql(chunk: usize) -> String {
    let placeholders = std::iter::repeat_n("?", chunk)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT id, tenant, index_id, segment_url FROM wal_segments WHERE id IN ({placeholders})"
    )
}

/// The existence probe, and the statement that decides
/// [`siglake_wal::mirror::LedgerVerdict::Unavailable`]: a database with no
/// `wal_segments` is not a siglake catalog, and must not read the same as a
/// catalog with no row for these ids.
const PROBE_SQL: &str = "SELECT id, tenant, index_id, segment_url FROM wal_segments LIMIT 1";

/// Postgres's read-only fence. SQLite gets `mode=ro` in the URI; Postgres has
/// no URI equivalent, so the SELECTs run inside a transaction the server
/// itself refuses to write in.
const PG_READ_ONLY_BEGIN: &str = "START TRANSACTION READ ONLY";

/// Rewrite a catalog URI into its read-only spelling.
///
/// Pure, so the rewrite is testable without a database. SQLite carries the
/// mode in the URI: any `mode=` the caller passed is replaced with `mode=ro`,
/// and every other parameter is preserved — including `immutable=1`, which is
/// the only way to read a WAL-journal catalog off a read-only mount and is
/// deliberately NOT a flag (it ignores the `-wal` sidecar, so against a live
/// database it returns a stale snapshot while the check claims exactness).
/// Postgres has no equivalent and is returned unchanged; its fence is
/// [`PG_READ_ONLY_BEGIN`].
pub fn read_only_uri(uri: &str) -> String {
    if Dialect::from_uri(uri) == Dialect::Postgres {
        return uri.to_string();
    }
    let (head, query) = match uri.split_once('?') {
        Some((head, query)) => (head, query),
        None => return format!("{uri}?mode=ro"),
    };
    let mut params: Vec<&str> = query
        .split('&')
        .filter(|p| !p.is_empty() && !p.starts_with("mode="))
        .collect();
    params.push("mode=ro");
    format!("{head}?{}", params.join("&"))
}

/// Read-only ledger inspection: open, probe, and look ids up in chunks.
///
/// Holds ONE connection for the life of the reader, because the Postgres fence
/// is a transaction and a transaction is per-connection.
#[derive(Debug)]
pub struct WalLedgerReader {
    pool: AnyPool,
    conn: Mutex<PoolConnection<Any>>,
    dialect: Dialect,
    queries: AtomicUsize,
    rows_read: AtomicUsize,
}

impl WalLedgerReader {
    /// Open `uri` read-only and prove `wal_segments` is there.
    ///
    /// The error is the operator's whole diagnosis, so it names the remedies:
    /// a WAL-journal catalog on a read-only mount needs SQLite to create a
    /// `-shm` beside it, which the mount refuses even though every statement
    /// is a SELECT. That failure arrives at the PROBE rather than at the
    /// connect — sqlx's pool connects lazily, so nothing touches the file
    /// until the first statement — which is why every step here is diagnosed
    /// through the same [`open_error`].
    pub async fn open(uri: &str) -> Result<Self> {
        sqlx::any::install_default_drivers();
        let dialect = Dialect::from_uri(uri);
        let read_only = read_only_uri(uri);
        // One connection: the Postgres read-only transaction lives on it, and
        // a pool that could hand out a second one would run SELECTs outside
        // the fence.
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&read_only)
            .await
            .map_err(|e| open_error(uri, dialect, Stage::Connect, &e))?;
        let mut conn = pool
            .acquire()
            .await
            .map_err(|e| open_error(uri, dialect, Stage::Connect, &e))?;
        if dialect == Dialect::Postgres {
            sqlx::query(PG_READ_ONLY_BEGIN)
                .execute(&mut *conn)
                .await
                .map_err(|e| open_error(uri, dialect, Stage::Fence, &e))?;
        }
        sqlx::query(PROBE_SQL)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| open_error(uri, dialect, Stage::Probe, &e))?;
        Ok(Self {
            pool,
            conn: Mutex::new(conn),
            dialect,
            queries: AtomicUsize::new(0),
            rows_read: AtomicUsize::new(0),
        })
    }

    /// Look the listed ids up, [`LOOKUP_CHUNK`] at a time. Duplicate ids cost
    /// nothing beyond their bind slot; the result is keyed by id.
    pub async fn lookup(&self, ids: &[String]) -> Result<HashMap<String, LedgerRow>> {
        let mut out = HashMap::new();
        let mut conn = self.conn.lock().await;
        for chunk in ids.chunks(LOOKUP_CHUNK) {
            let sql = self.dialect.rewrite(&lookup_sql(chunk.len()));
            let mut q = sqlx::query(&sql);
            for id in chunk {
                q = q.bind(id);
            }
            self.queries.fetch_add(1, Ordering::Relaxed);
            let rows = q
                .fetch_all(&mut **conn)
                .await
                .context("look wal_segments ids up")?;
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

    /// Queries issued since the probe — the cost measurement's own counter.
    pub fn queries(&self) -> usize {
        self.queries.load(Ordering::Relaxed)
    }

    /// Rows returned across every chunk.
    pub fn rows_read(&self) -> usize {
        self.rows_read.load(Ordering::Relaxed)
    }

    /// Release the connection and the pool. A Postgres reader's read-only
    /// transaction ends here; nothing was written in it either way.
    pub async fn close(self) {
        drop(self.conn.into_inner());
        self.pool.close().await;
    }
}

/// The filesystem path a SQLite URI names, for the "it is not there" check.
/// `None` for Postgres, for `:memory:`, and for any spelling this does not
/// recognise — in which case the diagnosis falls back to the driver's own
/// words rather than guessing.
fn sqlite_path(uri: &str) -> Option<std::path::PathBuf> {
    let rest = uri
        .strip_prefix("sqlite://")
        .or_else(|| uri.strip_prefix("sqlite:"))?;
    let path = rest.split('?').next().unwrap_or(rest);
    if path.is_empty() || path.contains(":memory:") {
        return None;
    }
    Some(std::path::PathBuf::from(path))
}

/// Which step of the open failed. Only used to word the diagnosis: a database
/// that is not a siglake catalog and a database that cannot be read at all are
/// different problems with different remedies.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Stage {
    Connect,
    Fence,
    Probe,
}

/// Turn a failed open into the line an operator reads during a restore.
fn open_error(uri: &str, dialect: Dialect, stage: Stage, e: &sqlx::Error) -> anyhow::Error {
    let text = e.to_string();
    // A file that is not there is the DR run's first failure domain — the
    // catalog did not survive the volume — and it reports `unable to open`,
    // the same string a read-only mount reports. Separated before the mount
    // advice, or an operator whose catalog is gone is told to copy its
    // sidecars somewhere writable.
    if let Some(path) = sqlite_path(uri) {
        if !path.exists() {
            return anyhow::anyhow!(
                "open {uri} read-only: no such database file ({}). A recovery reads the \
                 catalog and never creates one, so this is the catalog not having survived \
                 the volume.",
                path.display()
            );
        }
    }
    // Checked before the stage, because SQLite reports it at whichever step
    // first touches the file — the PROBE, with a lazily-connecting pool.
    let needs_immutable = dialect == Dialect::Sqlite
        && (text.contains("readonly database") || text.contains("unable to open"));
    if needs_immutable {
        return anyhow::anyhow!(
            "open {uri} read-only: {text}. A WAL-journal SQLite catalog needs a `-shm` file \
             created beside it, which a read-only mount refuses even though every statement \
             here is a SELECT. Copy the database and its sidecars somewhere writable and \
             point --catalog at the copy, or append `&immutable=1` to the URI — which reads \
             around the `-wal` sidecar, so it is exact only for a catalog nothing is still \
             writing."
        );
    }
    match stage {
        Stage::Probe => anyhow::anyhow!(
            "wal_segments is not readable in {uri}: this is a database, but not a siglake \
             catalog: {text}"
        ),
        Stage::Fence => anyhow::anyhow!("{PG_READ_ONLY_BEGIN} on {uri}: {text}"),
        Stage::Connect => anyhow::anyhow!("open {uri} read-only: {text}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_uri_carries_mode_ro_and_preserves_the_rest() {
        assert_eq!(
            read_only_uri("sqlite:///var/lib/siglake/catalog.db"),
            "sqlite:///var/lib/siglake/catalog.db?mode=ro"
        );
        // A `mode=` the operator passed is REPLACED, not appended to: the
        // whole point is that the engine refuses the write.
        assert_eq!(
            read_only_uri("sqlite:///c.db?mode=rwc"),
            "sqlite:///c.db?mode=ro"
        );
        // Everything else survives, `immutable=1` included — the only way to
        // read a WAL-journal catalog off a read-only mount.
        assert_eq!(
            read_only_uri("sqlite:///c.db?immutable=1&cache=private"),
            "sqlite:///c.db?immutable=1&cache=private&mode=ro"
        );
        // Postgres has no URI mode; its fence is the transaction.
        for uri in ["postgres://u@h/db", "postgresql://u@h/db?sslmode=require"] {
            assert_eq!(read_only_uri(uri), uri);
        }
    }

    /// Every statement this module runs, parsed in the dialect that would run
    /// it. There is no Postgres in a lane, and a Postgres-only syntax error
    /// here would surface at the worst possible moment: during a disaster
    /// recovery. The same cover the watermark statements get.
    #[test]
    fn the_read_only_statements_parse_as_postgres() {
        use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
        use datafusion::sql::sqlparser::parser::Parser;

        let rendered = Dialect::Postgres.rewrite(&lookup_sql(3));
        assert!(rendered.contains("$3"), "{rendered}");
        for sql in [PG_READ_ONLY_BEGIN, PROBE_SQL, rendered.as_str()] {
            let parsed = Parser::parse_sql(&PostgreSqlDialect {}, sql)
                .unwrap_or_else(|e| panic!("does not parse as Postgres: {e}\n{sql}"));
            assert_eq!(parsed.len(), 1, "one statement per execute(): {sql}");
        }
    }

    #[test]
    fn a_sqlite_uri_yields_the_file_the_missing_catalog_check_stats() {
        assert_eq!(
            sqlite_path("sqlite:///var/lib/siglake/catalog.db?mode=ro"),
            Some(std::path::PathBuf::from("/var/lib/siglake/catalog.db"))
        );
        assert_eq!(
            sqlite_path("sqlite:catalog.db"),
            Some(std::path::PathBuf::from("catalog.db"))
        );
        // No path to stat: the driver's own words are the diagnosis.
        for uri in ["postgres://u@h/db", "sqlite::memory:", "sqlite://"] {
            assert_eq!(sqlite_path(uri), None, "{uri}");
        }
    }

    #[test]
    fn the_lookup_binds_one_placeholder_per_id() {
        assert_eq!(lookup_sql(1).matches('?').count(), 1);
        assert_eq!(lookup_sql(LOOKUP_CHUNK).matches('?').count(), LOOKUP_CHUNK);
    }
}

/// The reader's Postgres-only properties, run against compose's live server.
///
/// `#[ignore]`d, and wired into the compose lifecycle that already runs the
/// catalog claim's Postgres cases (`.github/workflows/ci.yml` and
/// `scripts/ci-local.sh`):
///
/// ```text
/// SIGLAKE_TEST_JOBS_POSTGRES_URI=postgres://siglake:siglake@localhost:5433/siglake \
///   cargo test -p siglake-storage --lib wal_ledger_postgres -- --ignored --nocapture
/// ```
///
/// Every case gets its own schema. The schema is pinned through Postgres's
/// startup `search_path`, so neither the reader nor its setup can see the
/// compose stack's own `wal_segments` in `public`.
#[cfg(test)]
mod wal_ledger_postgres {
    use super::*;
    use anyhow::{ensure, Context};
    use sqlx::AnyPool;

    const URI_VAR: &str = "SIGLAKE_TEST_JOBS_POSTGRES_URI";

    struct Scratch {
        name: String,
        uri: String,
    }

    fn scratch_uri(base: &str, schema: &str) -> String {
        let sep = if base.contains('?') { '&' } else { '?' };
        format!("{base}{sep}options[search_path]={schema}")
    }

    impl Scratch {
        async fn create(admin: &AnyPool, base: &str, case: &str) -> Result<Self> {
            let name = format!(
                "siglake_wal_reader_{case}_{}",
                uuid::Uuid::new_v4().simple()
            );
            sqlx::query(&format!("CREATE SCHEMA {name}"))
                .execute(admin)
                .await
                .with_context(|| format!("create scratch schema {name}"))?;
            let uri = scratch_uri(base, &name);
            Ok(Self { name, uri })
        }

        async fn connect(&self) -> Result<AnyPool> {
            AnyPoolOptions::new()
                .max_connections(1)
                .connect(&self.uri)
                .await
                .with_context(|| format!("connect to scratch schema {}", self.name))
        }

        async fn drop_schema(self, admin: &AnyPool) {
            if let Err(e) = sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.name))
                .execute(admin)
                .await
            {
                eprintln!("warning: leaked scratch schema {}: {e}", self.name);
            }
        }
    }

    async fn create_ledger_table(pool: &AnyPool) -> Result<()> {
        sqlx::query(
            "CREATE TABLE wal_segments (
                id TEXT PRIMARY KEY,
                tenant TEXT NOT NULL,
                index_id TEXT NOT NULL,
                segment_url TEXT NOT NULL
            )",
        )
        .execute(pool)
        .await
        .context("create wal_segments")?;
        Ok(())
    }

    fn require_read_only_transaction(err: sqlx::Error) -> Result<()> {
        let text = err.to_string();
        let sqlx::Error::Database(database) = err else {
            anyhow::bail!("UPDATE failed outside the server: {text}");
        };
        ensure!(
            database.code().as_deref() == Some("25006"),
            "UPDATE was not refused as a read-only SQL transaction (SQLSTATE {:?}): {text}",
            database.code()
        );
        Ok(())
    }

    async fn chunk_and_fence_case(admin: &AnyPool, base: &str) -> Result<()> {
        let scratch = Scratch::create(admin, base, "lookup").await?;
        let outcome = async {
            let setup = scratch.connect().await?;
            create_ledger_table(&setup).await?;
            sqlx::query(
                "CREATE TABLE write_probe (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
            )
            .execute(&setup)
            .await
            .context("create the ordinary table used by the write probe")?;
            sqlx::query("INSERT INTO write_probe (id, value) VALUES (1, 7)")
                .execute(&setup)
                .await
                .context("seed the write probe")?;

            let count = LOOKUP_CHUNK + 3;
            for i in 0..count {
                let id = format!("seg-{i:03}");
                sqlx::query(
                    "INSERT INTO wal_segments (id, tenant, index_id, segment_url) \
                     VALUES ($1, $2, $3, $4)",
                )
                .bind(&id)
                .bind(format!("tenant-{i}"))
                .bind(if i % 2 == 0 { "" } else { "orders" })
                .bind(format!("wal-mirror/{id}.arrow"))
                .execute(&setup)
                .await
                .with_context(|| format!("seed {id}"))?;
            }
            setup.close().await;

            let listed: Vec<String> = (0..count).map(|i| format!("seg-{i:03}")).collect();
            let reader = WalLedgerReader::open(&scratch.uri).await?;
            let matched = reader.lookup(&listed).await?;
            ensure!(
                reader.queries() == 2,
                "259 ids took {} queries",
                reader.queries()
            );
            ensure!(
                matched.len() == count,
                "matched {} of {count}",
                matched.len()
            );
            for i in [0, LOOKUP_CHUNK - 1, LOOKUP_CHUNK, count - 1] {
                let id = format!("seg-{i:03}");
                let row = matched.get(&id).with_context(|| format!("missing {id}"))?;
                ensure!(
                    row.tenant == format!("tenant-{i}"),
                    "wrong row for {id}: {row:?}"
                );
                ensure!(
                    row.segment_url == format!("wal-mirror/{id}.arrow"),
                    "wrong URL for {id}: {row:?}"
                );
            }

            // This is the reader's fenced connection, not a second pool. The
            // target is an ordinary table that the setup connection just
            // created and updated, so SQLSTATE 25006 proves the transaction
            // is what refused the write.
            let write_error = {
                let mut conn = reader.conn.lock().await;
                sqlx::query("UPDATE write_probe SET value = value + 1 WHERE id = 1")
                    .execute(&mut **conn)
                    .await
                    .expect_err("the reader connection must be read-only")
            };
            require_read_only_transaction(write_error)?;
            reader.close().await;

            let verify = scratch.connect().await?;
            let value: i64 = sqlx::query_scalar("SELECT value FROM write_probe WHERE id = 1")
                .fetch_one(&verify)
                .await
                .context("read the write probe after the refused UPDATE")?;
            verify.close().await;
            ensure!(
                value == 7,
                "the refused UPDATE changed write_probe to {value}"
            );
            Ok(())
        }
        .await;
        scratch.drop_schema(admin).await;
        outcome
    }

    async fn absent_and_empty_case(admin: &AnyPool, base: &str) -> Result<()> {
        let scratch = Scratch::create(admin, base, "absence").await?;
        let outcome = async {
            let err = WalLedgerReader::open(&scratch.uri)
                .await
                .expect_err("an absent wal_segments table must be unavailable");
            let text = format!("{err:#}");
            ensure!(
                text.contains("not a siglake catalog"),
                "wrong absence error: {text}"
            );

            let setup = scratch.connect().await?;
            let tables: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name = 'wal_segments'",
            )
            .bind(&scratch.name)
            .fetch_one(&setup)
            .await
            .context("check that the reader did not create wal_segments")?;
            ensure!(tables == 0, "the failed reader created wal_segments");

            create_ledger_table(&setup).await?;
            setup.close().await;
            let reader = WalLedgerReader::open(&scratch.uri).await?;
            let rows = reader.lookup(&["absent-id".to_string()]).await?;
            ensure!(rows.is_empty(), "an empty ledger returned {rows:?}");
            reader.close().await;
            Ok(())
        }
        .await;
        scratch.drop_schema(admin).await;
        outcome
    }

    #[test]
    fn the_scratch_uri_pins_the_search_path() {
        use sqlx::postgres::PgConnectOptions;
        use std::str::FromStr;

        let uri = scratch_uri(
            "postgres://siglake:siglake@localhost:5433/siglake?sslmode=disable",
            "siglake_wal_reader_deadbeef",
        );
        let options = PgConnectOptions::from_str(&uri).expect(&uri);
        assert_eq!(
            options.get_options(),
            Some("-c search_path=siglake_wal_reader_deadbeef")
        );
        assert_eq!(Dialect::from_uri(&uri), Dialect::Postgres);
    }

    #[test]
    fn both_gates_run_this_module_by_name() {
        let module = module_path!().rsplit("::").next().expect("module name");
        for rel in [".github/workflows/ci.yml", "scripts/ci-local.sh"] {
            let path =
                std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../..")).join(rel);
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
            let filters: Vec<&str> = text
                .lines()
                .filter(|line| line.contains("cargo test -p siglake-storage --lib"))
                .filter_map(|line| {
                    let mut words = line.split_whitespace().skip_while(|w| *w != "--lib");
                    words.next();
                    words.next()
                })
                .collect();
            assert!(
                filters.contains(&module),
                "{rel} runs no `cargo test -p siglake-storage --lib {module}`; filters: {filters:?}"
            );
            assert!(text.contains(URI_VAR), "{rel} must give that run {URI_VAR}");
        }
    }

    #[tokio::test]
    #[ignore]
    async fn the_reader_behaves_read_only_against_postgres() {
        let base = match std::env::var(URI_VAR) {
            Ok(uri) if !uri.trim().is_empty() => uri,
            _ => {
                eprintln!("skipped: {URI_VAR} is unset; nothing was verified");
                return;
            }
        };
        assert!(
            Dialect::from_uri(&base) == Dialect::Postgres,
            "{URI_VAR} must name Postgres"
        );
        sqlx::any::install_default_drivers();
        let admin = AnyPool::connect(&base)
            .await
            .unwrap_or_else(|e| panic!("connect {URI_VAR}: {e}"));

        let outcomes = [
            (
                "a full 256-id chunk and its successor return the right rows, and writes fail",
                chunk_and_fence_case(&admin, &base).await,
            ),
            (
                "an absent wal_segments is unavailable while an empty one is readable",
                absent_and_empty_case(&admin, &base).await,
            ),
        ];
        admin.close().await;
        let failed: Vec<String> = outcomes
            .into_iter()
            .filter_map(|(case, outcome)| outcome.err().map(|e| format!("- {case}: {e:#}")))
            .collect();
        assert!(
            failed.is_empty(),
            "Postgres reader cases failed:\n{}",
            failed.join("\n")
        );
    }
}
