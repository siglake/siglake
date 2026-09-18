//! Catalog-tracked WAL segment claim.
//!
//! The local-FS [`siglake_wal::claim_segment`] primitive uses the file
//! system's atomic-rename guarantee to coordinate "who is allowed to
//! process this sealed segment." That works for a single-pod
//! compactor reading from local disk, but breaks down for the BYOC
//! multi-pod story:
//!
//! - Object stores (S3, GCS, …) don't offer atomic rename — list +
//!   copy + delete leaves a window where two compactors could
//!   double-process the same segment.
//! - Multiple compactor replicas pulling from a shared S3 WAL mirror
//!   need a coordination primitive that *doesn't* assume single-node
//!   locking.
//!
//! This module ships a Postgres/SQLite-backed alternative. The
//! `wal_segments` table records every sealed segment + its current
//! claim state; the SQL `UPDATE … RETURNING` pattern gives us
//! atomic-claim semantics that work across pods.
//!
//! v0 ships the primitive only — the compactor still uses the FS
//! path (single-pod is the default deployment). Multi-pod compactor
//! work (Phase 5+) will adopt this.

use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::AnyPool;

/// Snapshot of the sealed (unclaimed) WAL-segment queue, returned by
/// [`SqlSegmentClaim::peek_pending`]. Drives the compactor's
/// commit-accumulation gate (BIG-4 `#4b`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PendingStats {
    /// Number of sealed segments waiting to be claimed.
    pub segments: u64,
    /// Total on-mirror bytes across those segments.
    pub bytes: u64,
    /// Total event rows across those segments.
    pub rows: u64,
    /// Age of the oldest sealed segment (now − its `registered_at`).
    /// Zero when the queue is empty.
    pub oldest_age: Duration,
}

/// Durable progress for one mirror-to-catalog reconciliation rotation.
///
/// `last_key = None` is the start of the prefix. `rotation` increments only
/// after a listing reaches the end of the prefix and wraps back to the start.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MirrorSyncCursor {
    pub last_key: Option<String>,
    pub rotation: u64,
    /// Start of the rotation currently in progress. `None` at a rotation
    /// boundary, including before the first page has been attempted.
    pub rotation_started_at_ms: Option<i64>,
    /// Objects examined in the current rotation, or in the most recently
    /// completed rotation while [`Self::last_key`] is `None`.
    pub rotation_objects_examined: u64,
    /// Completion time of the most recent full rotation. This survives owner
    /// handoff so completion-age telemetry does not reset with a pod.
    pub last_completed_at_ms: Option<i64>,
}

/// Catalog-certified low watermark for one Iceberg target, with the table
/// incarnation that established it.
///
/// `table_uuid` is `None` for the events table — it has no recreate-under-one-
/// name path, so its name IS its identity, the same "no opinion" the WAL owner
/// markers and [`crate::iceberg::AppendIncarnationMismatch`] already take. For an index
/// it is `None` only on a row written before #2889, which is unproved: the
/// boundary may describe a dropped incarnation's segments and must not be
/// applied to whatever the name resolves to now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumedProofWatermark {
    pub acknowledged_through_ms: i64,
    pub table_uuid: Option<String>,
}

/// Which table incarnation each `(tenant, index_id)` group's commit was
/// verified against, carried from the drain into the terminal-claim
/// transaction that advances that group's watermark (#2889).
///
/// A watermark advance is the only place provenance can be recorded honestly:
/// it happens in the same transaction as the `committed` transition, for a
/// commit whose append was already fenced against this very uuid. Reading the
/// uuid a name resolves to later proves nothing about the boundary.
#[derive(Debug, Clone, Default)]
pub struct ProofProvenance(std::collections::HashMap<(String, String), String>);

impl ProofProvenance {
    /// Record that `(tenant, index_id)`'s commit this cycle was verified
    /// against `table_uuid`. Ignored for the events table, whose watermark
    /// deliberately carries no incarnation (see [`ConsumedProofWatermark`]) —
    /// stamping it would make an events row alternate between proved and
    /// unproved depending on which path advanced it.
    pub fn record(&mut self, tenant: &str, index_id: &str, table_uuid: &str) {
        if index_id.is_empty() {
            return;
        }
        self.0.insert(
            (tenant.to_string(), index_id.to_string()),
            table_uuid.to_string(),
        );
    }

    fn get(&self, tenant: &str, index_id: &str) -> Option<&str> {
        self.0
            .get(&(tenant.to_string(), index_id.to_string()))
            .map(String::as_str)
    }
}

/// The `consumed_proof_watermarks` statements, named so they can be parsed
/// under the Postgres dialect by a test: this store has no live Postgres in CI,
/// and a typo here is a silent runtime failure on the path that decides which
/// table an acknowledgement is applied to (#2889).
pub(crate) const WATERMARK_CREATE_SQL: &str = "\
    CREATE TABLE IF NOT EXISTS consumed_proof_watermarks ( \
        tenant                    TEXT NOT NULL, \
        index_id                  TEXT NOT NULL, \
        acknowledged_through_ms   BIGINT NOT NULL, \
        table_uuid                TEXT, \
        PRIMARY KEY (tenant, index_id) \
    )";
pub(crate) const WATERMARK_ADD_UUID_SQL: &str =
    "ALTER TABLE consumed_proof_watermarks ADD COLUMN table_uuid TEXT";
pub(crate) const WATERMARK_SELECT_SQL: &str =
    "SELECT acknowledged_through_ms, table_uuid FROM consumed_proof_watermarks \
     WHERE tenant = ? AND index_id = ?";
pub(crate) const WATERMARK_CANDIDATE_SQL: &str = "SELECT \
         MIN(CASE WHEN status <> 'committed' THEN registered_at_ms END), \
         MAX(CASE WHEN status = 'committed' THEN \
             COALESCE(claimed_at_ms, committed_at_ms, registered_at_ms) END) \
     FROM wal_segments WHERE tenant = ? AND index_id = ?";
/// Monotone advance within one incarnation. The `acknowledged_through_ms <`
/// guard makes a concurrent advance that got further win rather than lose.
pub(crate) const WATERMARK_ADVANCE_SQL: &str =
    "UPDATE consumed_proof_watermarks SET acknowledged_through_ms = ? \
     WHERE tenant = ? AND index_id = ? AND acknowledged_through_ms < ?";
/// A different incarnation REPLACES the boundary. Taking the max here would
/// carry a dropped table's acknowledgement forward under the replacement's
/// uuid, which is the relabelling the column exists to prevent.
pub(crate) const WATERMARK_REESTABLISH_SQL: &str =
    "UPDATE consumed_proof_watermarks SET acknowledged_through_ms = ?, table_uuid = ? \
     WHERE tenant = ? AND index_id = ?";
pub(crate) const WATERMARK_ESTABLISH_SQL: &str = "INSERT INTO consumed_proof_watermarks \
         (tenant, index_id, acknowledged_through_ms, table_uuid) VALUES (?, ?, ?, ?) \
     ON CONFLICT (tenant, index_id) DO UPDATE SET \
         acknowledged_through_ms = excluded.acknowledged_through_ms, \
         table_uuid = excluded.table_uuid";

/// The three statements of the local-drain mirror-reclamation mark (#4913),
/// named so they can be parse-gated against the Postgres dialect: this store
/// has no live Postgres in CI, and a Postgres-only syntax error here would be a
/// silent runtime failure on the path that decides which mirror objects may be
/// deleted.
///
/// `placeholders` is the `?, ?, …` list for one chunk of ids. The single `?`
/// inside `COALESCE` comes FIRST in the text, so it binds first.
fn local_mark_update_sql(placeholders: &str) -> String {
    format!(
        "UPDATE wal_segments \
             SET status = 'committed', \
                 committed_at_ms = COALESCE(committed_at_ms, ?) \
             WHERE id IN ({placeholders}) \
               AND status IN ('sealed', 'committed')"
    )
}

fn local_mark_readback_sql(placeholders: &str) -> String {
    format!(
        "SELECT id FROM wal_segments \
         WHERE id IN ({placeholders}) AND status = 'committed'"
    )
}

const LOCAL_MARK_INSERT_SQL: &str = "INSERT INTO wal_segments \
         (id, tenant, index_id, segment_url, bytes, rows, \
          status, committed_at_ms, registered_at_ms) \
         VALUES (?, ?, ?, ?, ?, 0, 'committed', ?, ?) \
     ON CONFLICT(id) DO NOTHING";

/// Unix-millis representation. We store timestamps as `BIGINT` to
/// dodge the cross-dialect (sqlite ↔ postgres) chrono ↔ timestamptz
/// type-mapping mismatch when binding through `sqlx::AnyPool`.
fn now_millis() -> i64 {
    Utc::now().timestamp_millis()
}

/// Placeholder style picker. sqlx's AnyPool doesn't auto-translate
/// `?` to Postgres-style `$N`; queries that use the wrong form get a
/// "syntax error at or near ?" or end up unbound. We detect the
/// underlying backend at connect time and rewrite the queries.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Dialect {
    Sqlite,
    Postgres,
}

impl Dialect {
    fn from_uri(uri: &str) -> Self {
        if uri.starts_with("postgres:") || uri.starts_with("postgresql:") {
            Self::Postgres
        } else {
            Self::Sqlite
        }
    }

    /// Replace `?` markers with the backend's parameter form. Order
    /// matters: `$1` first, `$2` second, ... The number of `?`s in
    /// the input must equal the number of bound parameters at call
    /// time.
    fn rewrite(self, sql: &str) -> String {
        match self {
            Self::Sqlite => sql.to_string(),
            Self::Postgres => {
                let mut out = String::with_capacity(sql.len() + 8);
                let mut i = 1usize;
                for c in sql.chars() {
                    if c == '?' {
                        out.push('$');
                        out.push_str(&i.to_string());
                        i += 1;
                    } else {
                        out.push(c);
                    }
                }
                out
            }
        }
    }
}

/// One row in the `wal_segments` catalog. The compactor uses this
/// after [`SqlSegmentClaim::try_claim`] to know which segment to pull
/// from object storage and process.
#[derive(Debug, Clone)]
pub struct ClaimedSegment {
    pub id: String,
    /// Tenant identifier (Iceberg namespace minus the `tenant_`
    /// prefix). `"default"` for single-tenant deployments.
    pub tenant: String,
    /// User-index id the segment belongs to; empty string = the built-in
    /// `events` table. Derived from the mirror key layout
    /// (`<prefix>/<tenant>/<index>/<id>.arrow`).
    pub index_id: String,
    pub segment_url: String,
    pub bytes: i64,
    pub rows: i64,
    pub claimed_at: DateTime<Utc>,
}

/// A segment the local filesystem drain committed, for
/// [`SqlSegmentClaim::mark_committed_local`].
///
/// `segment_url` must be the key the uploader wrote — root-relative
/// `<mirror prefix>/<tenant>/<index>/<id>.arrow`, the same string
/// [`SqlSegmentClaim::register`] records — because that is the key retention
/// deletes. It is only used when the row is absent; where a registered row
/// exists, its own `segment_url` is preserved.
#[derive(Debug, Clone)]
pub struct LocalCommittedSegment {
    /// Segment id: the file basename without `.arrow`.
    pub id: String,
    pub tenant: String,
    /// User-index id, or the empty string for the built-in `events` table.
    pub index_id: String,
    pub segment_url: String,
    pub bytes: i64,
}

/// Catalog-tracked claim coordinator. Cheap to clone — wraps a
/// connection pool.
/// Drain attempts before a segment is set aside. Generous, because most
/// release reasons are transient; the point is that "forever" is not an option.
const RELEASE_MAX_ATTEMPTS: i64 = 12;

#[derive(Clone)]
pub struct SqlSegmentClaim {
    pool: AnyPool,
    claimer: String,
    dialect: Dialect,
}

impl SqlSegmentClaim {
    /// Connect to the catalog DB and ensure the `wal_segments`
    /// schema exists. `uri` is a `postgres://…` or
    /// `sqlite://…?mode=rwc` URI; in tests prefer `sqlite::memory:`.
    pub async fn connect(uri: &str, claimer: impl Into<String>) -> Result<Self> {
        sqlx::any::install_default_drivers();
        let pool = AnyPool::connect(uri)
            .await
            .with_context(|| format!("connect {uri}"))?;
        let this = Self {
            pool,
            claimer: claimer.into(),
            dialect: Dialect::from_uri(uri),
        };
        this.ensure_schema().await?;
        Ok(this)
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        // Both postgres and sqlite accept this DDL. `TEXT` columns are
        // fine on both; we don't rely on JSONB / sequences / etc.
        // `status` is one of: 'sealed' (default), 'processing',
        // 'committed', 'released'.
        // Timestamps stored as unix-millis to keep the binding shape
        // identical across sqlite and postgres via `sqlx::AnyPool`.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS wal_segments (
                id              TEXT PRIMARY KEY,
                tenant          TEXT NOT NULL DEFAULT 'default',
                index_id        TEXT NOT NULL DEFAULT '',
                segment_url     TEXT NOT NULL,
                bytes           BIGINT NOT NULL,
                rows            BIGINT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'sealed',
                attempts        INTEGER NOT NULL DEFAULT 0,
                not_before_ms   BIGINT,
                claimer         TEXT,
                claimed_at_ms   BIGINT,
                committed_at_ms BIGINT,
                registered_at_ms BIGINT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create wal_segments table")?;

        // Per-table leases (remediation plan Phase 1 item 3 / Phase 3 item 4).
        //
        // Iceberg exposes ONE metadata pointer per table, so every
        // snapshot-changing operation for a table serializes there whether or not
        // we intend it to. Today that serialization is accidental — N drains and
        // N compactors race the CAS and the losers redo work. A lease makes it
        // deliberate: one holder owns a table's mutations, everyone else prepares
        // work in parallel and hands it over.
        //
        // The same primitive serves compaction (stop N replicas planning the same
        // table) and the Phase 3 publisher (one batched committer per table).
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS table_leases (
                table_id      TEXT PRIMARY KEY,
                holder        TEXT NOT NULL,
                purpose       TEXT NOT NULL DEFAULT 'commit',
                acquired_at_ms BIGINT NOT NULL,
                expires_at_ms BIGINT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create table_leases table")?;

        // Fleet-wide progress for the bounded mirror recovery sweep. This is
        // separate from `table_leases`: leases are short-lived ownership,
        // while this row must survive owner death and pod replacement.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS mirror_sync_cursors (
                cursor_id                   TEXT PRIMARY KEY,
                last_key                    TEXT,
                rotation                    BIGINT NOT NULL DEFAULT 0,
                rotation_started_at_ms      BIGINT,
                rotation_objects_examined  BIGINT NOT NULL DEFAULT 0,
                last_completed_at_ms        BIGINT,
                updated_at_ms               BIGINT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create mirror_sync_cursors table")?;
        // Additive telemetry state for catalogs created before bounded scans
        // exposed whole-rotation progress. A cursor already in flight starts
        // timing/counting from its first post-upgrade page; correctness state
        // (`last_key`, `rotation`) is unchanged.
        let _ =
            sqlx::query("ALTER TABLE mirror_sync_cursors ADD COLUMN rotation_started_at_ms BIGINT")
                .execute(&self.pool)
                .await;
        let _ = sqlx::query(
            "ALTER TABLE mirror_sync_cursors ADD COLUMN rotation_objects_examined BIGINT NOT NULL DEFAULT 0",
        )
        .execute(&self.pool)
        .await;
        let _ =
            sqlx::query("ALTER TABLE mirror_sync_cursors ADD COLUMN last_completed_at_ms BIGINT")
                .execute(&self.pool)
                .await;

        // Per-table low watermark for compacting the Iceberg consumed-proof
        // property. It advances only in the same SQL transaction that makes a
        // claim terminal; a missing wal_segments row is never treated as an
        // acknowledgement.
        //
        // `table_uuid` is the incarnation whose commit established the boundary
        // (#2889). The key is a NAME, and a `DELETE` + `POST` of an index id
        // gives that name a different table, so the value alone cannot say
        // which table the acknowledged segments went into. NULL means unproved.
        sqlx::query(WATERMARK_CREATE_SQL)
            .execute(&self.pool)
            .await
            .context("create consumed_proof_watermarks table")?;
        // Additive migration (#2889) for catalogs written before the column
        // existed. Their rows keep their value and read back as unproved, so a
        // pre-upgrade watermark is never applied to a table it cannot be shown
        // to belong to; the next terminal claim re-establishes it with its own
        // incarnation. Same duplicate-column tolerance as the columns below.
        let _ = sqlx::query(WATERMARK_ADD_UUID_SQL)
            .execute(&self.pool)
            .await;
        // Best-effort schema migration for catalogs created by 4.9h /
        // 4.10b before the `tenant` column existed. Postgres and
        // SQLite both accept `ALTER TABLE ADD COLUMN IF NOT EXISTS`
        // as of recent versions, but for older SQLite we fall back
        // to ignoring the duplicate-column error.
        let _ = sqlx::query(
            r#"
            ALTER TABLE wal_segments ADD COLUMN tenant TEXT NOT NULL DEFAULT 'default'
            "#,
        )
        .execute(&self.pool)
        .await;
        // Additive migration for pre-index-dimension catalogs (fleet prereq 2):
        // '' = the built-in events table, so existing rows keep their meaning.
        let _ = sqlx::query(
            r#"
            ALTER TABLE wal_segments ADD COLUMN index_id TEXT NOT NULL DEFAULT ''
            "#,
        )
        .execute(&self.pool)
        .await;
        // Poison-segment control. A segment that cannot be drained -- an index
        // that does not exist, an unreadable mirror object -- was released
        // straight back to 'sealed' with `registered_at_ms` untouched, and the
        // claim is OLDEST-FIRST, so it was permanently the oldest row in the
        // queue and every drain re-claimed it every cycle, forever. Once enough
        // accumulated to fill the claim batch, no legitimate segment was ever
        // claimed again: one client header could stall the whole cluster's
        // ingest.
        let _ = sqlx::query(
            r#"
            ALTER TABLE wal_segments ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0
            "#,
        )
        .execute(&self.pool)
        .await;
        let _ = sqlx::query(
            r#"
            ALTER TABLE wal_segments ADD COLUMN not_before_ms BIGINT
            "#,
        )
        .execute(&self.pool)
        .await;
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS wal_segments_status_idx
                ON wal_segments (status, registered_at_ms)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create wal_segments status index")?;
        Ok(())
    }

    /// Record a freshly-sealed segment. Idempotent — duplicate
    /// `INSERT` for the same `id` is a no-op so we can call this
    /// from the mirror upload path safely. `tenant` is the Iceberg
    /// namespace tail; pass `"default"` for single-tenant
    /// deployments. Returns `true` when this call inserted the row and `false`
    /// when the id was already registered.
    pub async fn register(
        &self,
        id: &str,
        tenant: &str,
        index_id: &str,
        segment_url: &str,
        bytes: i64,
        rows: i64,
    ) -> Result<bool> {
        let q = self.dialect.rewrite(
            r#"
            INSERT INTO wal_segments
                (id, tenant, index_id, segment_url, bytes, rows, status, registered_at_ms)
                VALUES (?, ?, ?, ?, ?, ?, 'sealed', ?)
            ON CONFLICT(id) DO NOTHING
            "#,
        );
        let result = sqlx::query(&q)
            .bind(id)
            .bind(tenant)
            .bind(index_id)
            .bind(segment_url)
            .bind(bytes)
            .bind(rows)
            .bind(now_millis())
            .execute(&self.pool)
            .await
            .with_context(|| format!("register wal segment {id}: {q:?}"))?;
        Ok(result.rows_affected() > 0)
    }

    /// Stable shard for an index id. FNV-1a so the mapping is identical across
    /// processes, releases and architectures — a drain that disagrees with its
    /// peers about ownership would either double-claim or strand segments.
    fn index_shard(index_id: &str, shard_count: usize) -> usize {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in index_id.as_bytes() {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x1000_0000_01b3);
        }
        (h % shard_count as u64) as usize
    }

    /// Atomically claim up to `batch` sealed segments. Each row is
    /// transitioned from `'sealed'` to `'processing'` with the
    /// caller's `claimer` id stamped.
    ///
    /// We use a per-row UPDATE rather than an `UPDATE … LIMIT … RETURNING`
    /// (which Postgres supports but sqlite doesn't) to keep the API
    /// portable across both backends. Concurrent claimers race the
    /// per-row UPDATE; the loser's UPDATE matches 0 rows and we
    /// continue past it.
    pub async fn try_claim(&self, batch: usize) -> Result<Vec<ClaimedSegment>> {
        self.try_claim_sharded(batch, 0, 1).await
    }

    /// [`Self::try_claim`] restricted to one shard of the index keyspace.
    ///
    /// **This exists because sharding WITHOUT routing is worse than not sharding
    /// at all.** `commit_claimed` groups a claim batch by `(tenant, index_id)`
    /// and commits each group separately, so an unrouted claim spanning N
    /// indexes becomes N commits instead of one. The 2026-08-12 fleet round
    /// measured exactly that: 8 index shards took the drain from 350,678 to
    /// 277,255 rows/s and tripled the backlog slope (+50,846 -> +169,645),
    /// because contention per table fell 8x while total commit count rose 8x and
    /// each commit pays a fixed `load_table` + metadata-read + CAS cost.
    ///
    /// Routing makes a batch one group again: each drain owns
    /// `hash(index_id) % shard_count == shard_index`, so it commits once per
    /// batch AND contends with no other drain.
    ///
    /// Filtering happens in Rust rather than SQL because a portable hash across
    /// Postgres and sqlite does not exist (`hashtext` is Postgres-only), and the
    /// candidate rows are small. `shard_count` over-fetch keeps each drain's
    /// yield at roughly `batch`.
    /// Claim a batch only if THAT BATCH is worth committing — the candidate-local
    /// replacement for the fleet-global `peek_pending()` gate.
    ///
    /// The global gate aggregates every sealed row and every drain runs the same
    /// query, so all of them reach the same defer/proceed decision at the same
    /// instant: a fleet-wide barrier that idles N machines together and then
    /// releases them into a race N-1 lose. It also costs one full-table aggregate
    /// per drain per cycle.
    ///
    /// Here the eligibility test is applied to the rows THIS claimer just locked.
    /// `FOR UPDATE SKIP LOCKED` gives each claimer a disjoint candidate set, so
    /// two drains see different batches and decide independently -- the barrier
    /// disappears while small-commit amortization is kept.
    ///
    /// Postgres only: it needs SKIP LOCKED and a CTE in one statement. Callers
    /// fall back to the peek gate elsewhere.
    pub async fn try_claim_eligible(
        &self,
        batch: usize,
        target_bytes: u64,
        max_age: std::time::Duration,
    ) -> Result<Vec<ClaimedSegment>> {
        if batch == 0 || !matches!(self.dialect, Dialect::Postgres) {
            return Ok(vec![]);
        }
        let now_ms = now_millis();
        let q = self.dialect.rewrite(
            r#"
            WITH cand AS (
                SELECT id FROM wal_segments
                    WHERE status = 'sealed'
                      AND (not_before_ms IS NULL OR not_before_ms <= ?)
                    ORDER BY registered_at_ms ASC
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
            ), agg AS (
                SELECT COALESCE(SUM(bytes), 0) AS total_bytes,
                       COALESCE(MIN(registered_at_ms), ?) AS oldest_ms
                    FROM wal_segments WHERE id IN (SELECT id FROM cand)
            )
            UPDATE wal_segments
                SET status = 'processing', claimer = ?, claimed_at_ms = ?
                FROM agg
                WHERE wal_segments.id IN (SELECT id FROM cand)
                  AND (agg.total_bytes >= ? OR (? - agg.oldest_ms) >= ?)
                RETURNING wal_segments.id, wal_segments.tenant, wal_segments.index_id,
                          wal_segments.segment_url, wal_segments.bytes, wal_segments.rows
            "#,
        );
        let claimed: Vec<(String, String, String, String, i64, i64)> = sqlx::query_as(&q)
            .bind(now_ms)
            .bind(batch as i64)
            .bind(now_ms)
            .bind(&self.claimer)
            .bind(now_ms)
            .bind(target_bytes as i64)
            .bind(now_ms)
            .bind(max_age.as_millis() as i64)
            .fetch_all(&self.pool)
            .await
            .context("claim wal segments (candidate-local eligibility)")?;
        if claimed.is_empty() {
            metrics::counter!("siglake_catalog_claim_ineligible_total").increment(1);
        }
        let claimed_at = DateTime::<Utc>::from_timestamp_millis(now_ms).unwrap_or_else(Utc::now);
        Ok(claimed
            .into_iter()
            .map(
                |(id, tenant, index_id, segment_url, bytes, rows)| ClaimedSegment {
                    id,
                    tenant,
                    index_id,
                    segment_url,
                    bytes,
                    rows,
                    claimed_at,
                },
            )
            .collect())
    }

    pub async fn try_claim_sharded(
        &self,
        batch: usize,
        shard_index: usize,
        shard_count: usize,
    ) -> Result<Vec<ClaimedSegment>> {
        if batch == 0 {
            return Ok(vec![]);
        }
        let shard_count = shard_count.max(1);
        let shard_index = if shard_count == 1 {
            0
        } else {
            shard_index % shard_count
        };
        // Over-fetch so a sharded drain still gets ~`batch` of its own rows.
        let fetch = batch
            .saturating_mul(shard_count)
            .min(batch.saturating_mul(16));
        let batch = fetch;
        // POSTGRES: one statement. The previous shape was 1 SELECT + N per-row
        // UPDATEs — at batch=512 that is **513 sequential round-trips per claim
        // cycle**, 0.5-2.5s of pure latency depending on RTT, which is the best
        // explanation yet for drains sitting ~88% idle while RDS runs at 22.8%
        // CPU: the database is not working hard, it is being asked one row at a
        // time.
        //
        // It also explains why bigger claims regressed past 512 MiB — the 2 GiB
        // round used 2048 segments, so 2049 round-trips — which I had wrongly
        // attributed to decode memory.
        //
        // `FOR UPDATE SKIP LOCKED` inside the subquery gives each claimer a
        // disjoint set with no racing, and `RETURNING` makes the whole claim
        // atomic in a single round-trip with no long-lived transaction. An
        // earlier attempt kept the N UPDATEs and merely wrapped them in a
        // transaction to make SKIP LOCKED effective; that held 512 row locks
        // across 513 round-trips and measured WORSE (drain 350,678 -> 322,261,
        // slope +50,846 -> +103,425).
        //
        // sqlite keeps the loop: it has neither SKIP LOCKED nor concurrent
        // claimers in practice.
        if matches!(self.dialect, Dialect::Postgres) {
            let now_ms = now_millis();
            let q = self.dialect.rewrite(
                r#"
                UPDATE wal_segments
                    SET status = 'processing', claimer = ?, claimed_at_ms = ?
                    WHERE id IN (
                        SELECT id FROM wal_segments
                            WHERE status = 'sealed'
                              AND (not_before_ms IS NULL OR not_before_ms <= ?)
                            ORDER BY registered_at_ms ASC
                            LIMIT ?
                            FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, tenant, index_id, segment_url, bytes, rows
                "#,
            );
            let claimed: Vec<(String, String, String, String, i64, i64)> = sqlx::query_as(&q)
                .bind(&self.claimer)
                .bind(now_ms)
                .bind(now_ms)
                .bind(batch as i64)
                .fetch_all(&self.pool)
                .await
                .context("claim wal segments (single-statement)")?;
            let claimed_at =
                DateTime::<Utc>::from_timestamp_millis(now_ms).unwrap_or_else(Utc::now);

            // RELEASE what this shard does not own.
            //
            // The UPDATE above has ALREADY transitioned every returned row to
            // 'processing'. Filtering in Rust and dropping the rest therefore
            // STRANDS those rows: nobody processes them, nobody commits them, and
            // nobody releases them — they sit in 'processing' forever. sqlite
            // filters before updating, so no test exposed this.
            //
            // Note the shard filter cannot move into the SQL: the mapping is
            // FNV-1a in Rust and no portable equivalent exists across Postgres
            // and sqlite. Releasing is the correct fix for the claim-then-discard
            // shape.
            //
            // (Routing on `index_id` is separately the wrong unit for the primary
            // single-table workload — every segment of one hot table maps to one
            // shard and the rest of the fleet idles. See the remediation plan.)
            let (mine, theirs): (Vec<_>, Vec<_>) = claimed.into_iter().partition(|c| {
                shard_count == 1 || Self::index_shard(&c.2, shard_count) == shard_index
            });
            if !theirs.is_empty() {
                metrics::counter!("siglake_catalog_claim_released_foreign_shard_total")
                    .increment(theirs.len() as u64);
                for c in &theirs {
                    if let Err(e) = self.release(&c.0).await {
                        tracing::warn!(id = %c.0, error = %e,
                            "failed to release foreign-shard claim; row may strand in processing");
                    }
                }
            }
            return Ok(mine
                .into_iter()
                .map(
                    |(id, tenant, index_id, segment_url, bytes, rows)| ClaimedSegment {
                        id,
                        tenant,
                        index_id,
                        segment_url,
                        bytes,
                        rows,
                        claimed_at,
                    },
                )
                .collect());
        }

        let q = self.dialect.rewrite(
            r#"
            SELECT id, tenant, index_id, segment_url, bytes, rows
                FROM wal_segments
                WHERE status = 'sealed'
                  AND (not_before_ms IS NULL OR not_before_ms <= ?)
                ORDER BY registered_at_ms ASC
                LIMIT ?
            "#,
        );
        let candidates: Vec<(String, String, String, String, i64, i64)> = sqlx::query_as(&q)
            .bind(now_millis())
            .bind(batch as i64)
            .fetch_all(&self.pool)
            .await
            .context("list sealed wal segments")?;
        let candidates: Vec<_> = candidates
            .into_iter()
            .filter(|c| shard_count == 1 || Self::index_shard(&c.2, shard_count) == shard_index)
            .collect();

        let mut out = Vec::with_capacity(candidates.len());
        let claim_q = self.dialect.rewrite(
            r#"
            UPDATE wal_segments
                SET status = 'processing', claimer = ?, claimed_at_ms = ?
                WHERE id = ? AND status = 'sealed'
            "#,
        );
        for (id, tenant, index_id, segment_url, bytes, rows) in candidates {
            let now_ms = now_millis();
            let res = sqlx::query(&claim_q)
                .bind(&self.claimer)
                .bind(now_ms)
                .bind(&id)
                .execute(&self.pool)
                .await
                .context("claim wal segment")?;
            if res.rows_affected() == 1 {
                out.push(ClaimedSegment {
                    id,
                    tenant,
                    index_id,
                    segment_url,
                    bytes,
                    rows,
                    claimed_at: DateTime::<Utc>::from_timestamp_millis(now_ms)
                        .unwrap_or_else(Utc::now),
                });
            }
        }
        Ok(out)
    }

    /// Cheap peek at the sealed (unclaimed) queue without claiming
    /// anything. Backs the compactor's commit-accumulation gate
    /// (BIG-4 `#4b`): the run loop uses this to decide whether enough
    /// work has piled up to be worth paying the fixed per-commit
    /// catalog cost, vs. waiting another poll. Uses the
    /// `(status, registered_at_ms)` index. Returns `(segments, bytes,
    /// rows, oldest_registered_at_ms)`; `oldest` is `None` when the
    /// queue is empty.
    pub async fn peek_pending(&self) -> Result<PendingStats> {
        let q = self.dialect.rewrite(
            r#"
            SELECT
                CAST(COUNT(*) AS BIGINT),
                CAST(COALESCE(SUM(bytes), 0) AS BIGINT),
                CAST(COALESCE(SUM(rows), 0) AS BIGINT),
                MIN(registered_at_ms)
                FROM wal_segments
                WHERE status = 'sealed'
            "#,
        );
        let (segments, bytes, rows, oldest): (i64, i64, i64, Option<i64>) = sqlx::query_as(&q)
            .fetch_one(&self.pool)
            .await
            .context("peek sealed wal segments")?;
        let oldest_age = oldest
            .map(|ms| {
                let age_ms = now_millis().saturating_sub(ms).max(0);
                Duration::from_millis(age_ms as u64)
            })
            .unwrap_or_default();
        Ok(PendingStats {
            segments: segments.max(0) as u64,
            bytes: bytes.max(0) as u64,
            rows: rows.max(0) as u64,
            oldest_age,
        })
    }

    pub async fn mark_committed(&self, id: &str) -> Result<()> {
        let q = self.dialect.rewrite(
            r#"
            UPDATE wal_segments
                SET status = 'committed', committed_at_ms = ?
                WHERE id = ? AND status = 'processing' AND claimer = ?
            "#,
        );
        let res = sqlx::query(&q)
            .bind(now_millis())
            .bind(id)
            .bind(&self.claimer)
            .execute(&self.pool)
            .await
            .context("mark_committed")?;
        if res.rows_affected() != 1 {
            anyhow::bail!(
                "mark_committed({id}): expected to update 1 row from this claimer, hit {}",
                res.rows_affected()
            );
        }
        Ok(())
    }

    /// Batch [`Self::mark_committed`]: one UPDATE per chunk instead of one
    /// round-trip per segment — a 256-segment claim batch was paying 256
    /// serial catalog round-trips per drain cycle. Same claimer guard; errors
    /// if any id in a chunk didn't transition (split into singles to attribute
    /// which — the caller treats that as retry-safe noise, same as before).
    ///
    /// `provenance` carries the table incarnation each group's commit was
    /// verified against (#2889); a target it does not name gets an unproved
    /// watermark, which maintenance refuses to apply rather than guess about.
    pub async fn mark_committed_batch(
        &self,
        ids: &[String],
        provenance: &ProofProvenance,
    ) -> Result<()> {
        for chunk in ids.chunks(256) {
            let mut tx = self.pool.begin().await.context("begin mark-committed")?;
            let placeholders = vec!["?"; chunk.len()].join(", ");
            let q = self.dialect.rewrite(&format!(
                "UPDATE wal_segments \
                     SET status = 'committed', committed_at_ms = ? \
                     WHERE id IN ({placeholders}) AND status = 'processing' AND claimer = ?"
            ));
            let mut query = sqlx::query(&q).bind(now_millis());
            for id in chunk {
                query = query.bind(id);
            }
            let res = query
                .bind(&self.claimer)
                .execute(&mut *tx)
                .await
                .context("mark_committed_batch")?;
            let targets_q = self.dialect.rewrite(&format!(
                "SELECT DISTINCT tenant, index_id FROM wal_segments WHERE id IN ({placeholders})"
            ));
            let mut targets_query = sqlx::query_as::<_, (String, String)>(&targets_q);
            for id in chunk {
                targets_query = targets_query.bind(id);
            }
            let targets = targets_query
                .fetch_all(&mut *tx)
                .await
                .context("load committed proof-watermark targets")?;
            for (tenant, index_id) in targets {
                let verified = provenance.get(&tenant, &index_id);
                self.advance_consumed_proof_watermark_in(&mut tx, &tenant, &index_id, verified)
                    .await?;
            }
            tx.commit().await.context("commit mark_committed_batch")?;
            if res.rows_affected() != chunk.len() as u64 {
                // Same tolerance as the caller's single-mark path: stragglers
                // stay 'processing' and re-enter via release/requeue; the
                // consumed-set dedup makes a re-commit a no-op. (A partial
                // batch UPDATE isn't reversible, so single-retries here would
                // false-error on the ids that DID transition.)
                tracing::warn!(
                    expected = chunk.len(),
                    updated = res.rows_affected(),
                    "mark_committed_batch: partial update; stragglers will retry"
                );
            }
        }
        Ok(())
    }

    /// Mark segments the LOCAL filesystem drain committed, for mirror
    /// reclamation under a drain that never claimed them (#4913).
    ///
    /// This is deliberately not [`Self::mark_committed`]: that one requires
    /// `status = 'processing' AND claimer = ?`, and there is no claim here —
    /// the evidence is the drain's own `committed/` rename, which is ordered
    /// after its Iceberg append. It also writes NO consumed-proof watermark:
    /// that boundary exists to let claim reclaim decide whether an abandoned
    /// claim was already committed, and this path leaves no claims to reclaim.
    ///
    /// An UPSERT, not an UPDATE. A late `catch_up_sweep` registration is
    /// `ON CONFLICT DO NOTHING` (see [`Self::register`]), so it cannot undo a
    /// `committed` row written first — but an update-only mark would lose the
    /// reverse race (mark before the upload registers) and leak the object,
    /// because retention only ever sees rows.
    ///
    /// `committed_at_ms` is stamped once and then preserved: the caller
    /// re-marks from `committed/` every cycle, and re-stamping would keep
    /// pushing the retention clock forward for as long as the local file
    /// lives. Only a `sealed` or already-`committed` row is touched; a
    /// `processing`, `released` or quarantined row belongs to a claim-mode
    /// drain and is left exactly as it is.
    ///
    /// Returns the ids whose `committed` row is durable after this call — the
    /// only ones whose local evidence the caller may then destroy.
    pub async fn mark_committed_local(
        &self,
        segments: &[LocalCommittedSegment],
    ) -> Result<Vec<String>> {
        let mut durable = Vec::new();
        for chunk in segments.chunks(256) {
            let ids: Vec<&str> = chunk.iter().map(|s| s.id.as_str()).collect();
            let placeholders = std::iter::repeat_n("?", ids.len())
                .collect::<Vec<_>>()
                .join(", ");
            let q = self.dialect.rewrite(&local_mark_update_sql(&placeholders));
            let mut query = sqlx::query(&q).bind(now_millis());
            for id in &ids {
                query = query.bind(*id);
            }
            query
                .execute(&self.pool)
                .await
                .context("mark locally-committed wal segments")?;
            // Read back rather than trusting `rows_affected`: it cannot say
            // WHICH ids transitioned, and a row left in another state must not
            // be reported as marked.
            let read_q = self
                .dialect
                .rewrite(&local_mark_readback_sql(&placeholders));
            let mut read = sqlx::query_scalar::<_, String>(&read_q);
            for id in &ids {
                read = read.bind(*id);
            }
            let marked: std::collections::BTreeSet<String> = read
                .fetch_all(&self.pool)
                .await
                .context("read back locally-committed wal segments")?
                .into_iter()
                .collect();
            for segment in chunk {
                if marked.contains(&segment.id) {
                    durable.push(segment.id.clone());
                    continue;
                }
                // Absent, or in a state this path must not touch. Insert-only:
                // a conflict means a row appeared between the UPDATE and here
                // (the registrar's `sealed`, or a claim), and the next cycle's
                // UPDATE picks that up rather than overwriting it now.
                let insert = self.dialect.rewrite(LOCAL_MARK_INSERT_SQL);
                let now = now_millis();
                let inserted = sqlx::query(&insert)
                    .bind(&segment.id)
                    .bind(&segment.tenant)
                    .bind(&segment.index_id)
                    .bind(&segment.segment_url)
                    .bind(segment.bytes)
                    .bind(now)
                    .bind(now)
                    .execute(&self.pool)
                    .await
                    .with_context(|| {
                        format!("insert locally-committed wal segment {}", segment.id)
                    })?
                    .rows_affected();
                if inserted > 0 {
                    durable.push(segment.id.clone());
                }
            }
        }
        Ok(durable)
    }

    /// Catalog-certified low watermark for one Iceberg target. Every segment
    /// whose proof timestamp is at or below this value has crossed a terminal
    /// `committed` transition. `None` means no terminal transition has yet
    /// established a boundary.
    ///
    /// The boundary comes back with the incarnation that established it, which
    /// the caller must check before applying it to a table (#2889): the key is
    /// a name, and both sides of `(tenant, index NAME)` still speak for a
    /// dropped incarnation after a `DELETE` + `POST` of that id.
    pub async fn consumed_proof_watermark(
        &self,
        tenant: &str,
        index_id: &str,
    ) -> Result<Option<ConsumedProofWatermark>> {
        let q = self.dialect.rewrite(WATERMARK_SELECT_SQL);
        let row = sqlx::query_as::<_, (i64, Option<String>)>(&q)
            .bind(tenant)
            .bind(index_id)
            .fetch_optional(&self.pool)
            .await
            .context("read consumed-proof watermark")?;
        Ok(row.map(
            |(acknowledged_through_ms, table_uuid)| ConsumedProofWatermark {
                acknowledged_through_ms,
                table_uuid,
            },
        ))
    }

    /// `table_uuid` is the incarnation this transaction's commit was verified
    /// against, or `None` for the events table and for any path with no
    /// verified identity to offer.
    async fn advance_consumed_proof_watermark_in(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Any>,
        tenant: &str,
        index_id: &str,
        table_uuid: Option<&str>,
    ) -> Result<()> {
        // `registered_at_ms` is never cleared, unlike `claimed_at_ms`. If a
        // non-terminal row exists at R, a proof entry for it cannot predate R;
        // therefore R-1 is a safe low watermark. With no non-terminal rows the
        // latest terminal claim itself is the boundary. This deliberately lets
        // sealed/released/quarantined rows block progress.
        let q = self.dialect.rewrite(WATERMARK_CANDIDATE_SQL);
        let (oldest_nonterminal, latest_terminal): (Option<i64>, Option<i64>) = sqlx::query_as(&q)
            .bind(tenant)
            .bind(index_id)
            .fetch_one(&mut **tx)
            .await
            .context("compute consumed-proof watermark")?;
        let candidate = oldest_nonterminal
            .map(|registered_at| registered_at.saturating_sub(1))
            .or(latest_terminal);
        let Some(candidate) = candidate else {
            return Ok(());
        };
        // #2889: the stored boundary belongs to the incarnation that wrote it.
        // Only a same-incarnation advance may take the MAX — carrying a dropped
        // table's higher boundary forward under the replacement's uuid would
        // relabel it, which is the mislabelling this column exists to stop. A
        // different (or newly known) incarnation REPLACES the row with the
        // boundary just computed, never keeping the old value.
        //
        // The null-safe comparison is done here rather than in SQL because
        // `IS NOT DISTINCT FROM` and sqlite's `IS` are not the same dialect.
        // Two drains advancing the same target can therefore interleave and
        // lose one advance or move the stored boundary backwards; both are safe
        // directions — the table-side `compact_through` takes its own max, so a
        // lower stored boundary only delays compaction.
        let current = self
            .consumed_proof_watermark_in(tx, tenant, index_id)
            .await?;
        match current {
            Some(current) if current.table_uuid.as_deref() == table_uuid => {
                if candidate > current.acknowledged_through_ms {
                    let q = self.dialect.rewrite(WATERMARK_ADVANCE_SQL);
                    sqlx::query(&q)
                        .bind(candidate)
                        .bind(tenant)
                        .bind(index_id)
                        .bind(candidate)
                        .execute(&mut **tx)
                        .await
                        .context("advance consumed-proof watermark")?;
                }
            }
            Some(_) => {
                let q = self.dialect.rewrite(WATERMARK_REESTABLISH_SQL);
                sqlx::query(&q)
                    .bind(candidate)
                    .bind(table_uuid)
                    .bind(tenant)
                    .bind(index_id)
                    .execute(&mut **tx)
                    .await
                    .context("re-establish consumed-proof watermark for a new incarnation")?;
            }
            None => {
                let q = self.dialect.rewrite(WATERMARK_ESTABLISH_SQL);
                sqlx::query(&q)
                    .bind(tenant)
                    .bind(index_id)
                    .bind(candidate)
                    .bind(table_uuid)
                    .execute(&mut **tx)
                    .await
                    .context("establish consumed-proof watermark")?;
            }
        }
        Ok(())
    }

    /// [`Self::consumed_proof_watermark`] read inside an open transaction, so
    /// the advance decides against the row its own UPDATE will hit.
    async fn consumed_proof_watermark_in(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Any>,
        tenant: &str,
        index_id: &str,
    ) -> Result<Option<ConsumedProofWatermark>> {
        let q = self.dialect.rewrite(WATERMARK_SELECT_SQL);
        let row = sqlx::query_as::<_, (i64, Option<String>)>(&q)
            .bind(tenant)
            .bind(index_id)
            .fetch_optional(&mut **tx)
            .await
            .context("read consumed-proof watermark in transaction")?;
        Ok(row.map(
            |(acknowledged_through_ms, table_uuid)| ConsumedProofWatermark {
                acknowledged_through_ms,
                table_uuid,
            },
        ))
    }

    /// Release a previously-claimed segment back to `'sealed'` so
    /// another compactor (or the same one on retry) can pick it up.
    /// Acquire or renew a lease on `table_id`, returning whether we hold it.
    ///
    /// One conditional statement, so it is safe from any number of contenders:
    /// insert if absent, otherwise take it only when the current lease has
    /// EXPIRED or we already hold it. A racing acquirer's `WHERE` matches zero
    /// rows and it simply reports false.
    ///
    /// Callers must renew well inside `ttl` — a holder that stalls past expiry
    /// loses the lease to someone else, which is the point (a wedged compactor
    /// must not own a table forever) but means the holder has to notice. Work
    /// performed after expiry is not protected; the Iceberg CAS remains the
    /// correctness backstop, and the lease exists to stop the *waste*, not to
    /// provide mutual exclusion the storage layer does not already guarantee.
    pub async fn acquire_table_lease(
        &self,
        table_id: &str,
        purpose: &str,
        ttl: std::time::Duration,
    ) -> Result<bool> {
        let now = now_millis();
        let expires = now + ttl.as_millis() as i64;
        let q = self.dialect.rewrite(
            r#"
            INSERT INTO table_leases (table_id, holder, purpose, acquired_at_ms, expires_at_ms)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (table_id) DO UPDATE
                    SET holder = ?, purpose = ?, acquired_at_ms = ?, expires_at_ms = ?
                    WHERE table_leases.expires_at_ms < ? OR table_leases.holder = ?
            "#,
        );
        let res = sqlx::query(&q)
            .bind(table_id)
            .bind(&self.claimer)
            .bind(purpose)
            .bind(now)
            .bind(expires)
            .bind(&self.claimer)
            .bind(purpose)
            .bind(now)
            .bind(expires)
            .bind(now)
            .bind(&self.claimer)
            .execute(&self.pool)
            .await
            .context("acquire table lease")?;
        let held = res.rows_affected() == 1;
        metrics::counter!(
            "siglake_catalog_table_lease_total",
            "outcome" => if held { "held" } else { "denied" }
        )
        .increment(1);
        Ok(held)
    }

    /// Release a lease we hold. Conditional on ownership so a late release from
    /// a superseded holder cannot free the new owner's lease.
    pub async fn release_table_lease(&self, table_id: &str) -> Result<()> {
        let q = self
            .dialect
            .rewrite("DELETE FROM table_leases WHERE table_id = ? AND holder = ?");
        sqlx::query(&q)
            .bind(table_id)
            .bind(&self.claimer)
            .execute(&self.pool)
            .await
            .context("release table lease")?;
        Ok(())
    }

    /// Load durable mirror reconciliation progress. A missing row is the
    /// beginning of rotation zero.
    pub async fn mirror_sync_cursor(&self, cursor_id: &str) -> Result<MirrorSyncCursor> {
        let q = self.dialect.rewrite(
            r#"
            SELECT last_key, rotation, rotation_started_at_ms,
                   rotation_objects_examined, last_completed_at_ms
            FROM mirror_sync_cursors
            WHERE cursor_id = ?
            "#,
        );
        let row = sqlx::query_as::<_, (Option<String>, i64, Option<i64>, i64, Option<i64>)>(&q)
            .bind(cursor_id)
            .fetch_optional(&self.pool)
            .await
            .context("load mirror sync cursor")?;
        let Some((
            last_key,
            rotation,
            rotation_started_at_ms,
            rotation_objects_examined,
            last_completed_at_ms,
        )) = row
        else {
            return Ok(MirrorSyncCursor::default());
        };
        let rotation =
            u64::try_from(rotation).context("mirror sync cursor has negative rotation")?;
        let rotation_objects_examined = u64::try_from(rotation_objects_examined)
            .context("mirror sync cursor has negative object count")?;
        Ok(MirrorSyncCursor {
            last_key,
            rotation,
            rotation_started_at_ms,
            rotation_objects_examined,
            last_completed_at_ms,
        })
    }

    /// Persist one successfully registered mirror page.
    ///
    /// The compare-and-set includes the rotation as well as `last_key`. That
    /// keeps a superseded owner from regressing a newer owner's progress, and
    /// prevents two stale owners at the start of a rotation from both counting
    /// the same wrap. Call this only after every object in the page has been
    /// registered; replay after a crash is then harmless because registration
    /// is idempotent.
    pub async fn compare_and_set_mirror_sync_cursor(
        &self,
        cursor_id: &str,
        expected: &MirrorSyncCursor,
        next_last_key: Option<&str>,
        completed_rotation: bool,
        page_objects_examined: usize,
    ) -> Result<bool> {
        anyhow::ensure!(
            !completed_rotation || next_last_key.is_none(),
            "a completed mirror rotation must clear last_key"
        );
        let expected_rotation = i64::try_from(expected.rotation)
            .context("mirror sync cursor rotation exceeds catalog range")?;
        let next_rotation = expected_rotation
            .checked_add(i64::from(completed_rotation))
            .context("mirror sync cursor rotation overflow")?;
        let previous_objects = if expected.last_key.is_some() {
            expected.rotation_objects_examined
        } else {
            0
        };
        let rotation_objects_examined = previous_objects
            .checked_add(
                u64::try_from(page_objects_examined)
                    .context("mirror sync page object count exceeds catalog range")?,
            )
            .context("mirror sync rotation object count overflow")?;
        let rotation_objects_examined = i64::try_from(rotation_objects_examined)
            .context("mirror sync rotation object count exceeds catalog range")?;
        let now_ms = now_millis();
        let rotation_started_at_ms = if completed_rotation {
            None
        } else {
            expected.rotation_started_at_ms.or(Some(now_ms))
        };
        let last_completed_at_ms = if completed_rotation {
            Some(now_ms)
        } else {
            expected.last_completed_at_ms
        };
        let expected_key_predicate = if expected.last_key.is_some() {
            "mirror_sync_cursors.last_key = ?"
        } else {
            "mirror_sync_cursors.last_key IS NULL"
        };
        let q = self.dialect.rewrite(&format!(
            r#"
            INSERT INTO mirror_sync_cursors
                (cursor_id, last_key, rotation, rotation_started_at_ms,
                 rotation_objects_examined, last_completed_at_ms, updated_at_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cursor_id) DO UPDATE SET
                last_key = excluded.last_key,
                rotation = excluded.rotation,
                rotation_started_at_ms = excluded.rotation_started_at_ms,
                rotation_objects_examined = excluded.rotation_objects_examined,
                last_completed_at_ms = excluded.last_completed_at_ms,
                updated_at_ms = excluded.updated_at_ms
            WHERE mirror_sync_cursors.rotation = ?
              AND {expected_key_predicate}
            "#
        ));
        let mut query = sqlx::query(&q)
            .bind(cursor_id)
            .bind(next_last_key)
            .bind(next_rotation)
            .bind(rotation_started_at_ms)
            .bind(rotation_objects_examined)
            .bind(last_completed_at_ms)
            .bind(now_ms)
            .bind(expected_rotation);
        if let Some(last_key) = expected.last_key.as_deref() {
            query = query.bind(last_key);
        }
        let updated = query
            .execute(&self.pool)
            .await
            .context("store mirror sync cursor")?
            .rows_affected()
            == 1;
        Ok(updated)
    }

    /// Purge `committed` rows older than `max_age` (remediation plan Phase 1
    /// item 8).
    ///
    /// Nothing pruned these. `wal_segments` grew monotonically with every
    /// segment ever drained, and the table is on the claim hot path — the claim
    /// SELECT filters `status = 'sealed'` and orders by `registered_at_ms`, so an
    /// unbounded tail of committed rows inflates the index the claim scans on
    /// every cycle of every drain.
    ///
    /// It also bounds recovery: the mirror sync's `ON CONFLICT DO NOTHING`
    /// registration is proportional to retained rows, so unbounded rows means
    /// unbounded recovery cost.
    ///
    /// **`max_age` must exceed the mirror-object retention**, or a purged row
    /// whose object still exists gets re-registered by the next mirror sync and
    /// re-drained — silent duplicate data. Purge catalog rows only for objects
    /// that are themselves gone or provably consumed.
    /// Committed rows eligible for purge, oldest first, with their mirror keys.
    ///
    /// Split out from [`Self::purge_committed`] because deleting the row is only
    /// safe AFTER the mirror object is gone. A committed row is the record that
    /// says "this segment has already been drained"; delete it while the object
    /// still exists and any registration path that lists the mirror -- the
    /// full-prefix recovery scan -- will see an unknown object, register it, and
    /// the segment gets drained a SECOND time. That is silent duplication of
    /// committed rows, which no error surfaces and no count reveals.
    ///
    /// So the protocol is: object first, row second. A crash between the two
    /// leaves a committed row with no object, which is inert (nothing re-reads
    /// it) and is cleaned up on the next pass.
    pub async fn purgeable_committed(
        &self,
        max_age: std::time::Duration,
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        let cutoff = now_millis() - max_age.as_millis() as i64;
        let q = self.dialect.rewrite(
            r#"
            SELECT id, segment_url FROM wal_segments
                WHERE status = 'committed' AND committed_at_ms IS NOT NULL
                  AND committed_at_ms < ?
                ORDER BY committed_at_ms ASC
                LIMIT ?
            "#,
        );
        let rows: Vec<(String, String)> = sqlx::query_as(&q)
            .bind(cutoff)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .context("select purgeable committed wal segments")?;
        Ok(rows)
    }

    /// Return the requested segment IDs whose rows are terminal `committed`
    /// for one Iceberg target. Callers use the exact IDs currently present in
    /// the table property, keeping this lookup bounded even when committed
    /// catalog retention is long or disabled.
    pub async fn committed_segment_ids(
        &self,
        tenant: &str,
        index_id: &str,
        ids: &[String],
    ) -> Result<Vec<String>> {
        let mut committed = Vec::new();
        for chunk in ids.chunks(256) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let q = self.dialect.rewrite(&format!(
                "SELECT id FROM wal_segments \
                 WHERE tenant = ? AND index_id = ? AND status = 'committed' \
                   AND id IN ({placeholders})"
            ));
            let mut query = sqlx::query_scalar::<_, String>(&q)
                .bind(tenant)
                .bind(index_id);
            for id in chunk {
                query = query.bind(id);
            }
            committed.extend(
                query
                    .fetch_all(&self.pool)
                    .await
                    .context("select committed consumed-proof segment IDs")?,
            );
        }
        Ok(committed)
    }

    /// Which of `ids` are committed and settled long enough to be dropped from
    /// an ingester's LOCAL disk?
    ///
    /// The ingester keeps every sealed segment on its own PVC. In catalog-claim
    /// mode nothing ever removed them: the drain reads the MIRROR, and the
    /// retention sweep that deletes local files
    /// (`sweep_committed_coordinated`) runs only on the filesystem path --
    /// `run_once` returns through `run_once_catalog` before reaching it. So the
    /// disk fills and ingest starts 500ing, on a timer, in the topology the
    /// chart recommends.
    ///
    /// `settled_for` is a FLOOR, not an optimisation. A query pod serving
    /// un-committed rows reads those same sealed files, and a commit is not
    /// instantly visible to it -- the standing invariant is that the
    /// committed-sweep floor must exceed the table-cache stale ceiling.
    /// Deleting the moment the catalog says `committed` would make rows vanish
    /// for the width of that window, which is the transition race already fixed
    /// once for the FS path.
    pub async fn committed_and_settled(
        &self,
        ids: &[String],
        settled_for: std::time::Duration,
    ) -> Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let cutoff = now_millis() - settled_for.as_millis() as i64;
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let q = self.dialect.rewrite(&format!(
            r#"
            SELECT id FROM wal_segments
                WHERE status = 'committed' AND committed_at_ms IS NOT NULL
                  AND committed_at_ms < ?
                  AND id IN ({placeholders})
            "#
        ));
        let mut query = sqlx::query_scalar::<_, String>(&q).bind(cutoff);
        for id in ids {
            query = query.bind(id);
        }
        query
            .fetch_all(&self.pool)
            .await
            .context("select committed settled wal segments")
    }

    /// Delete specific committed rows by id, after their objects are gone.
    pub async fn purge_committed_ids(&self, ids: &[String]) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let mut n = 0u64;
        // A retention run can remove thousands of objects. One DELETE per row
        // made the SQL round trips another throughput ceiling after the object
        // side was page-drained. Keep each IN list below conservative dialect
        // parameter limits while deleting only rows whose objects are gone.
        for chunk in ids.chunks(256) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let q = self.dialect.rewrite(&format!(
                "DELETE FROM wal_segments WHERE status = 'committed' AND id IN ({placeholders})"
            ));
            let mut query = sqlx::query(&q);
            for id in chunk {
                query = query.bind(id);
            }
            n += query
                .execute(&self.pool)
                .await
                .context("purge committed wal segment row")?
                .rows_affected();
        }
        if n > 0 {
            metrics::counter!("siglake_catalog_rows_purged_total").increment(n);
        }
        Ok(n)
    }

    /// Age-based bulk purge. UNSAFE to schedule on its own while a mirror
    /// registration path exists -- see [`Self::purgeable_committed`]. Retained
    /// for tests and for deployments with no mirror.
    pub async fn purge_committed(&self, max_age: std::time::Duration) -> Result<u64> {
        let cutoff = now_millis() - max_age.as_millis() as i64;
        let q = self.dialect.rewrite(
            r#"
            DELETE FROM wal_segments
                WHERE status = 'committed' AND committed_at_ms IS NOT NULL
                  AND committed_at_ms < ?
            "#,
        );
        let res = sqlx::query(&q)
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .context("purge committed wal segment rows")?;
        let n = res.rows_affected();
        if n > 0 {
            metrics::counter!("siglake_catalog_rows_purged_total").increment(n);
            tracing::info!(
                purged = n,
                max_age_secs = max_age.as_secs(),
                "purged committed WAL catalog rows"
            );
        }
        Ok(n)
    }

    /// Reclaim segments abandoned in `processing` by a dead or wedged worker.
    ///
    /// **Nothing reclaimed these before.** A drain that dies between claiming and
    /// committing leaves its rows in `processing` permanently: no worker will
    /// claim them (the claim query selects `status = 'sealed'`), and no timeout
    /// returns them. Those rows' data is accepted, durable in the mirror, and
    /// invisible forever.
    ///
    /// That is not hypothetical — the routed-claim bug fixed alongside this
    /// stranded rows the same way, and the fleet rounds killed drain processes
    /// mid-claim routinely.
    ///
    /// Idempotent and safe to run from every worker: the conditional `UPDATE`
    /// only matches rows still `processing` and older than `max_age`, so a
    /// racing reclaimer simply matches zero rows. `max_age` must exceed the
    /// longest legitimate claim-to-commit time or live work gets yanked out from
    /// under a healthy worker — the commit path is the slow part, so size this
    /// from observed append latency, not from the claim.
    /// Abandoned claims, oldest first, with the ids needed to prove disposition.
    ///
    /// Reclaim MUST NOT blindly requeue. A drain can die after its Iceberg commit
    /// lands but before `mark_committed_batch`, leaving the row in 'processing'
    /// with its rows already durable in the table. Flipping that row back to
    /// 'sealed' hands it to another drain, which commits the same segment a
    /// SECOND time -- silent duplication, no error, no counter.
    ///
    /// This is the same hazard #81 solved for the filesystem orphan path via the
    /// cumulative consumed-segment set. The catalog path needs the identical
    /// proof, which is why the caller gets the ids rather than a bare count.
    pub async fn abandoned_claims(
        &self,
        max_age: std::time::Duration,
        limit: usize,
    ) -> Result<Vec<ClaimedSegment>> {
        let cutoff = now_millis() - max_age.as_millis() as i64;
        let q = self.dialect.rewrite(
            r#"
            SELECT id, tenant, index_id, segment_url, bytes, rows, claimed_at_ms
                FROM wal_segments
                WHERE status = 'processing' AND claimed_at_ms IS NOT NULL
                  AND claimed_at_ms < ?
                ORDER BY claimed_at_ms ASC
                LIMIT ?
            "#,
        );
        // `claimed_at_ms` is SELECTed, not stamped as `now`. The caller needs
        // the real value: its proof-of-commit is the cumulative consumed set
        // across RETAINED snapshots, and whether that proof can be trusted
        // depends on whether a commit of this segment would still be inside
        // the retained history — which is a comparison against when it was
        // claimed. Stamping `now` threw that away at the SQL boundary and made
        // "absent because the proving snapshot expired" indistinguishable from
        // "absent because it was never committed".
        let rows: Vec<(String, String, String, String, i64, i64, i64)> = sqlx::query_as(&q)
            .bind(cutoff)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .context("select abandoned wal segment claims")?;
        Ok(rows
            .into_iter()
            .map(
                |(id, tenant, index_id, segment_url, bytes, rows, claimed_at_ms)| ClaimedSegment {
                    id,
                    tenant,
                    index_id,
                    segment_url,
                    bytes,
                    rows,
                    claimed_at: DateTime::<Utc>::from_timestamp_millis(claimed_at_ms)
                        .unwrap_or_else(Utc::now),
                },
            )
            .collect())
    }

    /// Mark an abandoned claim as already committed — its rows are provably in
    /// the table, so requeueing it would duplicate them.
    ///
    /// `provenance` names the table generation the proof was READ from, which
    /// is the incarnation that holds these entries (#2889).
    pub async fn mark_reclaimed_committed(
        &self,
        ids: &[String],
        provenance: &ProofProvenance,
    ) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let mut n = 0u64;
        for chunk in ids.chunks(256) {
            let mut tx = self
                .pool
                .begin()
                .await
                .context("begin mark reclaimed-committed")?;
            let placeholders = vec!["?"; chunk.len()].join(", ");
            let q = self.dialect.rewrite(&format!(
                "UPDATE wal_segments SET status = 'committed', committed_at_ms = ?, \
                     claimer = NULL WHERE id IN ({placeholders}) AND status = 'processing'"
            ));
            let mut query = sqlx::query(&q).bind(now_millis());
            for id in chunk {
                query = query.bind(id);
            }
            n += query
                .execute(&mut *tx)
                .await
                .context("mark reclaimed-committed")?
                .rows_affected();
            let targets_q = self.dialect.rewrite(&format!(
                "SELECT DISTINCT tenant, index_id FROM wal_segments WHERE id IN ({placeholders})"
            ));
            let mut targets_query = sqlx::query_as::<_, (String, String)>(&targets_q);
            for id in chunk {
                targets_query = targets_query.bind(id);
            }
            let targets = targets_query
                .fetch_all(&mut *tx)
                .await
                .context("load reclaimed proof-watermark targets")?;
            for (tenant, index_id) in targets {
                let verified = provenance.get(&tenant, &index_id);
                self.advance_consumed_proof_watermark_in(&mut tx, &tenant, &index_id, verified)
                    .await?;
            }
            tx.commit()
                .await
                .context("commit mark reclaimed-committed")?;
        }
        if n > 0 {
            metrics::counter!("siglake_catalog_claims_reclaimed_already_committed_total")
                .increment(n);
        }
        Ok(n)
    }

    /// Requeue specific abandoned claims that are PROVEN not to have committed.
    pub async fn requeue_claims(&self, ids: &[String]) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let mut n = 0u64;
        for id in ids {
            let q = self.dialect.rewrite(
                "UPDATE wal_segments SET status = 'sealed', claimer = NULL, \
                 claimed_at_ms = NULL WHERE id = ? AND status = 'processing'",
            );
            n += sqlx::query(&q)
                .bind(id)
                .execute(&self.pool)
                .await
                .context("requeue abandoned claim")?
                .rows_affected();
        }
        if n > 0 {
            metrics::counter!("siglake_catalog_claims_reclaimed_total").increment(n);
        }
        Ok(n)
    }

    /// Blind age-based requeue. UNSAFE when a commit may have landed without its
    /// mark — see [`Self::abandoned_claims`]. Retained for tests and for callers
    /// that have already proven disposition.
    pub async fn reclaim_abandoned(&self, max_age: std::time::Duration) -> Result<u64> {
        let cutoff = now_millis() - max_age.as_millis() as i64;
        let q = self.dialect.rewrite(
            r#"
            UPDATE wal_segments
                SET status = 'sealed', claimer = NULL, claimed_at_ms = NULL
                WHERE status = 'processing' AND claimed_at_ms IS NOT NULL
                  AND claimed_at_ms < ?
            "#,
        );
        let res = sqlx::query(&q)
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .context("reclaim abandoned wal segment claims")?;
        let n = res.rows_affected();
        if n > 0 {
            metrics::counter!("siglake_catalog_claims_reclaimed_total").increment(n);
            tracing::warn!(
                reclaimed = n,
                max_age_secs = max_age.as_secs(),
                "reclaimed WAL segment claims abandoned in processing"
            );
        }
        Ok(n)
    }

    /// Give a claimed segment back, with backoff — and quarantine it once it
    /// has failed too often.
    ///
    /// THE DEFECT THIS CLOSES. This used to reset `status` to 'sealed' and
    /// nothing else. `registered_at_ms` was untouched and the claim is
    /// OLDEST-FIRST, so a segment that cannot be drained -- an index that does
    /// not exist, an unreadable mirror object -- became permanently the oldest
    /// row in the queue and every drain re-claimed it every cycle, forever.
    /// There was no retry counter, no backoff, no dead letter. Once enough of
    /// them accumulated to fill the claim batch, no legitimate segment was ever
    /// claimed again: ingest for the whole cluster stalled, and ingest accepts
    /// any syntactically valid index id, so one client header could do it.
    ///
    /// Backoff first, quarantine last. Most release reasons are transient (a
    /// catalog blip, an S3 5xx) and should retry soon; only a segment that has
    /// failed `max_attempts` times is set aside, where it is visible and
    /// recoverable rather than silently blocking everyone else. A refusal that
    /// retrying cannot change goes to
    /// [`quarantine_terminal`](Self::quarantine_terminal) instead.
    pub async fn release(&self, id: &str) -> Result<()> {
        self.release_with_backoff(id, RELEASE_MAX_ATTEMPTS).await
    }

    pub(crate) async fn release_with_backoff(&self, id: &str, max_attempts: i64) -> Result<()> {
        // Exponential in the attempt count, capped: 1s, 2s, 4s … 5 min. The cap
        // matters more than the growth -- it bounds how long a transient
        // failure delays an otherwise healthy segment.
        let q = self.dialect.rewrite(
            r#"
            UPDATE wal_segments
                SET status = CASE WHEN attempts + 1 >= ? THEN 'quarantined' ELSE 'sealed' END,
                    attempts = attempts + 1,
                    not_before_ms = ? + CASE
                        WHEN attempts >= 8 THEN 300000
                        ELSE 1000 * (1 << attempts)
                    END,
                    claimer = NULL,
                    claimed_at_ms = NULL
                WHERE id = ? AND status = 'processing' AND claimer = ?
            "#,
        );
        sqlx::query(&q)
            .bind(max_attempts)
            .bind(now_millis())
            .bind(id)
            .bind(&self.claimer)
            .execute(&self.pool)
            .await
            .context("release")?;
        // Report the quarantine separately: it is the state an operator has to
        // act on, and it must not read as an ordinary release.
        let quarantined: Option<i64> =
            sqlx::query_scalar(&self.dialect.rewrite(
                "SELECT attempts FROM wal_segments WHERE id = ? AND status = 'quarantined'",
            ))
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .unwrap_or(None);
        if let Some(attempts) = quarantined {
            Self::report_quarantined(
                id,
                attempts,
                "repeated drain failures (commonly: its index does not exist)",
            );
        }
        Ok(())
    }

    /// Set a claimed segment aside immediately, without spending the retry
    /// budget on a refusal that cannot change.
    ///
    /// [`release`](Self::release) is retry-shaped: back off, and quarantine only
    /// after `RELEASE_MAX_ATTEMPTS`. That is right for a catalog blip or an S3
    /// 5xx. It is wrong for a segment whose frame header names a table uuid the
    /// index no longer resolves to (#2693) or that carries no identity at all
    /// under a prefix that has named a dropped incarnation (#2729): a table uuid
    /// is never reused, so every one of those attempts pays a GET of the
    /// object's bytes to reach the same answer, and the operator sees twelve
    /// `warn`s before the one `error` that says what to do.
    ///
    /// Same predicate as `release` — only the current claimer, and only while
    /// the row is still `'processing'` — so a segment another drain has since
    /// reclaimed is left alone. The mirror object is untouched: this marks
    /// nothing committed or consumed, and
    /// [`requeue_quarantined`](Self::requeue_quarantined) is still the way back,
    /// after which the identity check runs again from the top.
    ///
    /// Returns whether this call is the one that transitioned the row, so a
    /// repeat is silent rather than a second alert.
    pub async fn quarantine_terminal(&self, id: &str, reason: &str) -> Result<bool> {
        let q = self.dialect.rewrite(
            r#"
            UPDATE wal_segments
                SET status = 'quarantined',
                    attempts = attempts + 1,
                    not_before_ms = NULL,
                    claimer = NULL,
                    claimed_at_ms = NULL
                WHERE id = ? AND status = 'processing' AND claimer = ?
            "#,
        );
        let res = sqlx::query(&q)
            .bind(id)
            .bind(&self.claimer)
            .execute(&self.pool)
            .await
            .context("quarantine terminal")?;
        if res.rows_affected() == 0 {
            return Ok(false);
        }
        let attempts: i64 = sqlx::query_scalar(
            &self
                .dialect
                .rewrite("SELECT attempts FROM wal_segments WHERE id = ?"),
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .unwrap_or(None)
        .unwrap_or(0);
        Self::report_quarantined(id, attempts, reason);
        Ok(true)
    }

    /// The one line an operator has to act on. Emitted once per transition, by
    /// whichever path made it: the attempt budget running out, or a refusal that
    /// no number of attempts could change.
    fn report_quarantined(id: &str, attempts: i64, reason: &str) {
        metrics::counter!("siglake_catalog_claim_quarantined_total").increment(1);
        tracing::error!(
            id = %id,
            attempts,
            reason = %reason,
            "segment QUARANTINED — it is durable in the mirror but will not be drained or \
             queried until an operator requeues it"
        );
    }

    /// How many segments are set aside, and cannot drain without intervention.
    pub async fn quarantined_count(&self) -> Result<i64> {
        let q = self
            .dialect
            .rewrite("SELECT COUNT(*) FROM wal_segments WHERE status = 'quarantined'");
        sqlx::query_scalar(&q)
            .fetch_one(&self.pool)
            .await
            .context("count quarantined")
    }

    /// Return quarantined segments to the queue, e.g. once a missing index has
    /// been created. Clears the attempt count so they get a full set of retries.
    pub async fn requeue_quarantined(&self) -> Result<u64> {
        let q = self.dialect.rewrite(
            r#"
            UPDATE wal_segments
                SET status = 'sealed', attempts = 0, not_before_ms = NULL
                WHERE status = 'quarantined'
            "#,
        );
        Ok(sqlx::query(&q)
            .execute(&self.pool)
            .await
            .context("requeue quarantined")?
            .rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fresh() -> (SqlSegmentClaim, tempfile::TempDir) {
        // File-backed SQLite (a tempdir per test). `sqlite::memory:`
        // with an `AnyPool` creates a separate empty in-memory DB per
        // pooled connection, so CREATE TABLE on one connection is
        // invisible to the next bind — file-backed gives us the
        // expected single-database semantics for tests.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("claim.db");
        let uri = format!("sqlite://{}?mode=rwc", path.display());
        let claim = SqlSegmentClaim::connect(&uri, "test-compactor")
            .await
            .unwrap();
        (claim, tmp)
    }

    /// A segment that cannot be drained must not block every other segment.
    ///
    /// THE DEFECT THIS GUARDS. `release` reset `status` to 'sealed' and nothing
    /// else -- no attempt counter, no backoff, no dead letter -- while
    /// `registered_at_ms` stayed put and the claim is OLDEST-FIRST. A segment
    /// that could not be drained (commonly: an `x-siglake-index` header naming
    /// an index that does not exist, which ingest accepts without checking) was
    /// therefore permanently the oldest row in the queue, re-claimed by every
    /// drain on every cycle, forever. Once enough accumulated to fill the claim
    /// batch, no legitimate segment was ever claimed again -- cluster-wide
    /// ingest stall, reachable from one client header.
    ///
    /// Data loss, too, not just an outage: those rows are accepted, durable and
    /// permanently unqueryable, which is this project's recurring defect class.
    #[tokio::test]
    async fn a_repeatedly_failing_segment_stops_blocking_the_queue() {
        let (c, _tmp) = fresh().await;
        for id in ["poison", "healthy"] {
            c.register(id, "default", "", &format!("s3://b/{id}.arrow"), 1, 1)
                .await
                .unwrap();
        }

        // Claim both, then fail only the poison one, repeatedly. Two attempts
        // is enough here; production allows twelve.
        for _ in 0..2 {
            let claimed = c.try_claim(10).await.unwrap();
            for seg in &claimed {
                if seg.id == "poison" {
                    c.release_with_backoff(&seg.id, 2).await.unwrap();
                } else {
                    c.mark_committed_batch(
                        std::slice::from_ref(&seg.id),
                        &ProofProvenance::default(),
                    )
                    .await
                    .unwrap();
                }
            }
            // Past the first backoff (~1s) so the next claim is a genuine
            // retry rather than a deferral.
            tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
            if c.quarantined_count().await.unwrap() > 0 {
                break;
            }
        }

        assert_eq!(
            c.quarantined_count().await.unwrap(),
            1,
            "a segment that failed every attempt was never set aside"
        );
        // The queue is clear: the poison segment is no longer offered.
        let after = c.try_claim(10).await.unwrap();
        assert!(
            after.iter().all(|s| s.id != "poison"),
            "a quarantined segment was claimed again: {:?}",
            after.iter().map(|s| &s.id).collect::<Vec<_>>()
        );

        // And it is recoverable rather than lost -- the operator creates the
        // missing index, then requeues.
        assert_eq!(c.requeue_quarantined().await.unwrap(), 1);
        assert_eq!(c.quarantined_count().await.unwrap(), 0);
        let requeued = c.try_claim(10).await.unwrap();
        assert!(
            requeued.iter().any(|s| s.id == "poison"),
            "a requeued segment was not offered back to the drain"
        );
    }

    /// Backoff must actually defer: a released segment is not instantly the
    /// oldest candidate again.
    #[tokio::test]
    async fn a_released_segment_is_deferred_before_it_can_be_reclaimed() {
        let (c, _tmp) = fresh().await;
        c.register("seg", "default", "", "s3://b/seg.arrow", 1, 1)
            .await
            .unwrap();
        let claimed = c.try_claim(10).await.unwrap();
        assert_eq!(claimed.len(), 1);
        c.release("seg").await.unwrap();
        // The first backoff is ~1s, so an immediate re-claim must find nothing.
        let immediate = c.try_claim(10).await.unwrap();
        assert!(
            immediate.is_empty(),
            "a just-released segment was re-claimed with no backoff — the spin this fixes"
        );
    }

    /// #2746: a refusal that cannot change spends no retry budget.
    ///
    /// The drain refuses a mirrored object whose frame header names a table the
    /// index no longer resolves to (#2693), or that carries no identity under a
    /// prefix that has named a dropped incarnation (#2729). A table uuid is
    /// never reused, so those verdicts are terminal by construction: routing
    /// them through `release` bought twelve cycles of backoff, each paying a GET
    /// of the object's bytes to reach the same answer, and twelve `warn`s before
    /// the one `error` that tells an operator what to do.
    ///
    /// Everything else about the disposition is unchanged: the row is set aside,
    /// not committed, not consumed, the mirror object is untouched, and
    /// `requeue_quarantined` is the way back.
    #[tokio::test]
    async fn a_terminal_refusal_is_quarantined_on_the_cycle_that_refuses_it() {
        let (c, _tmp) = fresh().await;
        for id in ["stale", "healthy"] {
            c.register(id, "default", "", &format!("s3://b/{id}.arrow"), 1, 1)
                .await
                .unwrap();
        }
        let claimed = c.try_claim(10).await.unwrap();
        assert_eq!(claimed.len(), 2);

        assert!(
            c.quarantine_terminal("stale", "test refusal")
                .await
                .unwrap(),
            "the transitioning call must report that it transitioned"
        );
        assert_eq!(
            c.quarantined_count().await.unwrap(),
            1,
            "a terminal refusal still had to spend its attempt budget first"
        );
        // The rest of the batch is unaffected.
        c.mark_committed_batch(&["healthy".to_string()], &ProofProvenance::default())
            .await
            .unwrap();

        // Not offered again, at any point: no backoff window in which it comes
        // back for one more GET.
        for _ in 0..2 {
            assert!(
                c.try_claim(10).await.unwrap().is_empty(),
                "a quarantined segment was claimed again"
            );
        }

        // Idempotent: a second call finds no row in 'processing' and must not
        // raise a second alert.
        assert!(
            !c.quarantine_terminal("stale", "test refusal")
                .await
                .unwrap(),
            "a repeat call reported a transition that did not happen"
        );
        assert_eq!(c.quarantined_count().await.unwrap(), 1);

        // And the operator's way back is the same one, which re-runs the
        // identity check from the top rather than trusting the old verdict.
        assert_eq!(c.requeue_quarantined().await.unwrap(), 1);
        assert_eq!(c.quarantined_count().await.unwrap(), 0);
        let requeued = c.try_claim(10).await.unwrap();
        assert!(
            requeued.iter().any(|s| s.id == "stale"),
            "a requeued segment was not offered back to the drain"
        );
    }

    /// The predicate `release` uses is the predicate this uses: only the current
    /// claimer, only while the row is still 'processing'. A drain that lost its
    /// claim to the abandoned-claim reclaimer must not reach across and
    /// quarantine what another pod is now working on.
    #[tokio::test]
    async fn quarantine_terminal_will_not_touch_another_claimers_segment() {
        let (mine, tmp) = fresh().await;
        let theirs = SqlSegmentClaim::connect(
            &format!(
                "sqlite://{}?mode=rwc",
                tmp.path().join("claim.db").display()
            ),
            "pod-2",
        )
        .await
        .unwrap();
        mine.register("seg", "default", "", "s3://b/seg.arrow", 1, 1)
            .await
            .unwrap();
        assert_eq!(theirs.try_claim(10).await.unwrap().len(), 1);

        assert!(
            !mine
                .quarantine_terminal("seg", "test refusal")
                .await
                .unwrap(),
            "a non-claimer quarantined a segment out from under the pod holding it"
        );
        assert_eq!(mine.quarantined_count().await.unwrap(), 0);
        // Still theirs to finish.
        theirs
            .mark_committed_batch(&["seg".to_string()], &ProofProvenance::default())
            .await
            .unwrap();
        assert_eq!(mine.quarantined_count().await.unwrap(), 0);
    }

    /// The transient path keeps its full retry budget: adding the terminal one
    /// must not make an S3 5xx a dead letter.
    #[tokio::test]
    async fn a_transient_release_still_retries_before_it_quarantines() {
        let (c, _tmp) = fresh().await;
        c.register("seg", "default", "", "s3://b/seg.arrow", 1, 1)
            .await
            .unwrap();
        for i in 0..2 {
            let claimed = c.try_claim(10).await.unwrap();
            assert_eq!(claimed.len(), 1, "attempt {i} was not offered for retry");
            c.release("seg").await.unwrap();
            assert_eq!(
                c.quarantined_count().await.unwrap(),
                0,
                "a transient release quarantined inside its attempt budget"
            );
            // Past the first backoff (~1s) so the next claim is a genuine retry.
            tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        }
    }

    /// The ingester's local-disk reclaim must only name segments that are
    /// genuinely committed AND have settled.
    ///
    /// THE DEFECT THIS SUPPORTS. Nothing deleted an ingester's local sealed
    /// segments in catalog-claim mode -- the drain reads the mirror, and the
    /// sweep that deletes local files runs only on the filesystem path -- so the
    /// PVC filled and ingest 500'd on a timer. This is the query that decides
    /// what may go.
    ///
    /// Both halves matter. Returning an uncommitted segment would delete the
    /// only local copy of data that is not yet in Iceberg; returning one that
    /// committed a moment ago would race a query pod still serving it out of
    /// `sealed/`, which is the transition race already fixed once for the FS
    /// path.
    #[tokio::test]
    async fn committed_and_settled_names_only_what_is_safe_to_delete() {
        let (c, _tmp) = fresh().await;
        for id in ["sealed-1", "committed-fresh", "committed-old"] {
            c.register(
                id,
                "default",
                "",
                &format!("s3://b/wal-mirror/{id}.arrow"),
                1,
                1,
            )
            .await
            .unwrap();
        }
        c.try_claim(10).await.unwrap();
        c.mark_committed_batch(
            &["committed-fresh".to_string(), "committed-old".to_string()],
            &ProofProvenance::default(),
        )
        .await
        .unwrap();

        // Nothing has settled yet: a one-hour floor excludes both.
        let none = c
            .committed_and_settled(
                &[
                    "sealed-1".to_string(),
                    "committed-fresh".to_string(),
                    "committed-old".to_string(),
                ],
                std::time::Duration::from_secs(3600),
            )
            .await
            .unwrap();
        assert!(
            none.is_empty(),
            "segments committed moments ago were offered for deletion: {none:?}"
        );

        // A zero floor compares `committed_at_ms < now()`, which is FALSE
        // inside the millisecond the commit happened in -- the same
        // millisecond-granularity race that made the retention test flaky
        // (33a71f6). Wait past the tick rather than leaving a test that fails
        // one run in several.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        // With the floor elapsed the committed ones qualify -- and the one
        // still sealed never does, however long we wait.
        let ready = c
            .committed_and_settled(
                &[
                    "sealed-1".to_string(),
                    "committed-fresh".to_string(),
                    "committed-old".to_string(),
                ],
                std::time::Duration::ZERO,
            )
            .await
            .unwrap();
        let mut ready = ready;
        ready.sort();
        assert_eq!(
            ready,
            vec!["committed-fresh".to_string(), "committed-old".to_string()],
            "an uncommitted segment was offered for deletion, or a committed one was withheld"
        );

        // An id this ingester does not know about must not come back.
        let unknown = c
            .committed_and_settled(&["not-a-segment".to_string()], std::time::Duration::ZERO)
            .await
            .unwrap();
        assert!(unknown.is_empty(), "unknown id matched: {unknown:?}");
    }

    #[tokio::test]
    async fn register_then_claim_round_trip() {
        let (c, _tmp) = fresh().await;
        c.register(
            "seg-1",
            "default",
            "",
            "s3://bucket/wal-mirror/seg-1.arrow",
            1024,
            100,
        )
        .await
        .unwrap();
        let claimed = c.try_claim(10).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, "seg-1");
        assert_eq!(claimed[0].tenant, "default");
        assert_eq!(claimed[0].bytes, 1024);
        c.mark_committed("seg-1").await.unwrap();
        let again = c.try_claim(10).await.unwrap();
        assert!(again.is_empty());
    }

    /// The mirror-to-catalog reconciler is the only owner of the
    /// "object in the mirror, no catalog row" repair, and it used to run on
    /// EVERY drain with no lease — N workers each listing the whole mirror
    /// prefix and issuing one INSERT per object, on a prefix that grows
    /// monotonically. The lease reduces that to one worker per interval.
    ///
    /// It must also DEGRADE to everyone-runs-it rather than nobody, because
    /// losing the repair is worse than duplicating it: the loser's work is
    /// wasted, but a missing owner means accepted, durable, unqueryable rows.
    #[tokio::test]
    async fn one_worker_wins_the_mirror_sync_lease() {
        // TWO distinct claimers against one database — the actual fleet shape.
        // A single claimer re-acquiring is a RENEWAL and correctly succeeds, so
        // testing with one instance would prove nothing.
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!(
            "sqlite://{}?mode=rwc",
            tmp.path().join("claim.db").display()
        );
        let a = SqlSegmentClaim::connect(&uri, "drain-a").await.unwrap();
        let b = SqlSegmentClaim::connect(&uri, "drain-b").await.unwrap();
        let ttl = Duration::from_secs(60);

        assert!(
            a.acquire_table_lease("__maintenance__mirror_sync", "mirror_sync", ttl)
                .await
                .unwrap(),
            "the first worker must win"
        );
        assert!(
            !b.acquire_table_lease("__maintenance__mirror_sync", "mirror_sync", ttl)
                .await
                .unwrap(),
            "a SECOND worker inside the TTL must lose, or N drains each recursively list the \
             whole mirror prefix every cycle"
        );
        // The holder may renew: an owner that could not re-acquire would hand
        // the repair to nobody for a whole TTL.
        assert!(
            a.acquire_table_lease("__maintenance__mirror_sync", "mirror_sync", ttl)
                .await
                .unwrap(),
            "the holder must be able to renew"
        );
        // Retention keeps its own election, then both sweep owners contend on
        // one short-lived exclusion while they touch the mirror/catalog pair.
        // Keeping election separate avoids a 60s mirror-sync owner renewing a
        // 300s lease forever and starving retention in split-role fleets.
        assert!(
            b.acquire_table_lease("__maintenance__retention", "retention", ttl)
                .await
                .unwrap(),
            "retention must elect independently"
        );
        assert!(
            a.acquire_table_lease(
                "__maintenance__mirror_reconciliation",
                "mirror_reconciliation",
                ttl,
            )
            .await
            .unwrap(),
            "mirror sync must acquire the shared exclusion"
        );
        assert!(
            !b.acquire_table_lease(
                "__maintenance__mirror_reconciliation",
                "mirror_reconciliation",
                ttl,
            )
            .await
            .unwrap(),
            "retention must not overlap a mirror listing"
        );
        a.release_table_lease("__maintenance__mirror_reconciliation")
            .await
            .unwrap();
        assert!(
            b.acquire_table_lease(
                "__maintenance__mirror_reconciliation",
                "mirror_reconciliation",
                ttl,
            )
            .await
            .unwrap(),
            "retention must proceed as soon as mirror sync releases the exclusion"
        );
        // Per purpose: electing a mirror-sync owner must not stop another
        // maintenance task electing its own.
        assert!(
            b.acquire_table_lease("__maintenance__expire", "expire", ttl)
                .await
                .unwrap(),
            "leases must not collide across purposes"
        );
    }

    /// The backlog and the claim batch are different numbers, and the gauge an
    /// operator alerts on must come from the former.
    ///
    /// `siglake_compactor_sealed_pending` used to be set to `claimed.len()` on
    /// the catalog-claim path — bounded by the claim batch — so on any real
    /// backlog it reported a saturating constant, and reported the true depth
    /// only when the queue was too small to commit. The operator consumes it
    /// against a target of 1.0, so 256/1 slammed the compactor to max on any
    /// backlog and to min the moment a cycle claimed nothing.
    ///
    /// This pins the distinction: with a queue deeper than one batch, peek and
    /// claim disagree, and peek is the one that answers "how far behind am I".
    #[tokio::test]
    async fn the_backlog_is_deeper_than_a_claim_batch() {
        let (c, _t) = fresh().await;
        for i in 0..20 {
            c.register(
                &format!("s{i}"),
                "default",
                "",
                &format!("wal-mirror/s{i}.arrow"),
                100,
                5,
            )
            .await
            .unwrap();
        }
        let batch = 4;
        let claimed = c.try_claim(batch).await.unwrap();
        assert_eq!(claimed.len(), batch, "the claim is bounded by its batch");

        let pending = c.peek_pending().await.unwrap();
        assert_eq!(
            pending.segments, 16,
            "peek must report the segments STILL waiting, not the batch just taken"
        );
        assert!(
            pending.segments > claimed.len() as u64,
            "a backlog gauge taken from the claim size cannot exceed the batch, so it can \
             never show a backlog"
        );
        // And the growth signal: the oldest unclaimed segment has an age.
        assert!(
            pending.oldest_age <= Duration::from_secs(60),
            "oldest_age must be a real age, got {:?}",
            pending.oldest_age
        );
    }

    #[tokio::test]
    async fn peek_pending_sums_only_sealed() {
        let (c, _tmp) = fresh().await;
        // Empty queue.
        let p0 = c.peek_pending().await.unwrap();
        assert_eq!(p0, PendingStats::default());
        assert_eq!(p0.oldest_age, Duration::ZERO);

        c.register("seg-1", "default", "", "url-1", 1000, 10)
            .await
            .unwrap();
        c.register("seg-2", "default", "", "url-2", 2500, 25)
            .await
            .unwrap();
        let p1 = c.peek_pending().await.unwrap();
        assert_eq!(p1.segments, 2);
        assert_eq!(p1.bytes, 3500);
        assert_eq!(p1.rows, 35);

        // Claiming a segment moves it to 'processing' — it must drop
        // out of the sealed peek (it's no longer commit-pending).
        let claimed = c.try_claim(1).await.unwrap();
        assert_eq!(claimed.len(), 1);
        let p2 = c.peek_pending().await.unwrap();
        assert_eq!(p2.segments, 1, "claimed segment excluded from peek");
        assert!(p2.bytes == 1000 || p2.bytes == 2500);
    }

    #[tokio::test]
    async fn register_is_idempotent() {
        let (c, _tmp) = fresh().await;
        assert!(c
            .register("seg-1", "default", "", "url-1", 1, 1)
            .await
            .unwrap());
        assert!(!c
            .register("seg-1", "default", "", "url-2", 2, 2)
            .await
            .unwrap());
        let claimed = c.try_claim(10).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].segment_url, "url-1");
    }

    #[tokio::test]
    async fn release_reopens_for_claim() {
        let (c, _tmp) = fresh().await;
        c.register("seg-1", "default", "", "url", 1, 1)
            .await
            .unwrap();
        let _ = c.try_claim(10).await.unwrap();
        c.release("seg-1").await.unwrap();
        // Released segments now come back AFTER a backoff rather than
        // instantly — a segment that fails every time used to be re-claimed
        // every cycle forever, ahead of everything else, because the claim is
        // oldest-first and `registered_at_ms` never moved. Waiting past the
        // first (~1s) delay asserts the thing this test was always about: it
        // does come back.
        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        let claimed = c.try_claim(10).await.unwrap();
        assert_eq!(claimed.len(), 1, "released segment should re-claim");
    }

    #[tokio::test]
    async fn mark_committed_batch_commits_all_and_tolerates_stragglers() {
        let (c, _tmp) = fresh().await;
        for i in 0..5 {
            c.register(
                &format!("seg-{i}"),
                "default",
                "",
                &format!("wal-mirror/seg-{i}.arrow"),
                10,
                100,
            )
            .await
            .unwrap();
        }
        let claimed = c.try_claim(5).await.unwrap();
        assert_eq!(claimed.len(), 5);
        let ids: Vec<String> = claimed.iter().map(|c| c.id.clone()).collect();
        // One id released back mid-flight: the batch UPDATE hits 4 of 5 and
        // must WARN, not error (stragglers re-enter via requeue + dedup).
        c.release(&ids[0]).await.unwrap();
        c.mark_committed_batch(&ids, &ProofProvenance::default())
            .await
            .unwrap();
        // A release now carries a backoff, so the straggler re-enters the queue
        // after it rather than on the very next claim.
        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        let again = c.try_claim(5).await.unwrap();
        assert_eq!(again.len(), 1, "only the released segment is claimable");
        assert_eq!(again[0].id, ids[0]);
    }

    #[tokio::test]
    async fn committed_segment_ids_filters_by_terminal_state_and_target() {
        let (c, _tmp) = fresh().await;
        for (id, tenant, index) in [
            ("committed", "default", "logs"),
            ("processing", "default", "logs"),
            ("other-index", "default", "other"),
            ("other-tenant", "acme", "logs"),
        ] {
            c.register(id, tenant, index, "url", 1, 1).await.unwrap();
        }
        let claimed = c.try_claim(10).await.unwrap();
        let committed = claimed
            .iter()
            .find(|segment| segment.id == "committed")
            .unwrap();
        c.mark_committed(&committed.id).await.unwrap();

        let candidates = [
            "committed".to_string(),
            "processing".to_string(),
            "other-index".to_string(),
            "other-tenant".to_string(),
            "missing".to_string(),
        ];
        assert_eq!(
            c.committed_segment_ids("default", "logs", &candidates)
                .await
                .unwrap(),
            vec!["committed".to_string()]
        );
        assert!(c
            .committed_segment_ids("default", "logs", &[])
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn consumed_proof_watermark_stops_at_nonterminal_rows() {
        let (c, _tmp) = fresh().await;
        c.register("terminal", "default", "logs", "url-a", 1, 1)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        c.register("still-processing", "default", "logs", "url-b", 1, 1)
            .await
            .unwrap();
        let claimed = c.try_claim(10).await.unwrap();
        let by_id: std::collections::HashMap<_, _> = claimed
            .iter()
            .map(|claim| (claim.id.as_str(), claim.claimed_at.timestamp_millis()))
            .collect();

        c.mark_committed_batch(&["terminal".to_string()], &ProofProvenance::default())
            .await
            .unwrap();
        let blocked = c
            .consumed_proof_watermark("default", "logs")
            .await
            .unwrap()
            .expect("terminal transition creates watermark");
        assert!(
            blocked.acknowledged_through_ms < by_id["still-processing"],
            "processing row must block the watermark: {blocked:?} vs {:?}",
            by_id
        );

        c.mark_committed_batch(
            &["still-processing".to_string()],
            &ProofProvenance::default(),
        )
        .await
        .unwrap();
        let advanced = c
            .consumed_proof_watermark("default", "logs")
            .await
            .unwrap()
            .unwrap();
        assert!(advanced.acknowledged_through_ms >= by_id["still-processing"]);
        assert_eq!(
            advanced.table_uuid, None,
            "a caller with no verified identity leaves the boundary unproved"
        );
    }

    /// #2889: the key is a NAME, so the boundary has to say which table it was
    /// established by. An advance from the same incarnation is monotone; the
    /// events table (`index_id = ""`) deliberately stays unproved, because its
    /// name has no recreate path to straddle.
    #[tokio::test]
    async fn a_terminal_claim_records_the_incarnation_it_committed_into() {
        let (c, _tmp) = fresh().await;
        let mut provenance = ProofProvenance::default();
        provenance.record("default", "logs", "uuid-a");
        provenance.record("default", "", "uuid-events");

        c.register("seg-1", "default", "logs", "url-a", 1, 1)
            .await
            .unwrap();
        c.register("evt-1", "default", "", "url-e", 1, 1)
            .await
            .unwrap();
        assert_eq!(c.try_claim(10).await.unwrap().len(), 2);
        c.mark_committed_batch(&["seg-1".to_string(), "evt-1".to_string()], &provenance)
            .await
            .unwrap();

        let first = c
            .consumed_proof_watermark("default", "logs")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.table_uuid.as_deref(), Some("uuid-a"));
        assert_eq!(
            c.consumed_proof_watermark("default", "")
                .await
                .unwrap()
                .unwrap()
                .table_uuid,
            None,
            "the events table's watermark carries no incarnation"
        );

        tokio::time::sleep(Duration::from_millis(5)).await;
        c.register("seg-2", "default", "logs", "url-b", 1, 1)
            .await
            .unwrap();
        assert_eq!(c.try_claim(10).await.unwrap().len(), 1);
        c.mark_committed_batch(&["seg-2".to_string()], &provenance)
            .await
            .unwrap();
        let second = c
            .consumed_proof_watermark("default", "logs")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.table_uuid.as_deref(), Some("uuid-a"));
        assert!(
            second.acknowledged_through_ms > first.acknowledged_through_ms,
            "an advance within one incarnation still moves the boundary forward"
        );
    }

    /// A `DELETE` + `POST` of the same index id means the next advance speaks
    /// for a different table. Carrying the stored boundary forward under the
    /// replacement's uuid would relabel a dropped incarnation's acknowledgement
    /// as the replacement's own, which is exactly the claim the maintenance
    /// compaction then writes onto a table that never held those segments.
    ///
    /// The pre-seeded far-future boundary is what makes the two rules
    /// distinguishable: with a real clock a later candidate is never smaller,
    /// so `MAX` and "replace" agree on every value the drain can produce.
    #[tokio::test]
    async fn a_new_incarnation_replaces_the_boundary_rather_than_inheriting_it() {
        let (c, _tmp) = fresh().await;
        sqlx::query(
            "INSERT INTO consumed_proof_watermarks \
                 (tenant, index_id, acknowledged_through_ms, table_uuid) VALUES \
                 ('default', 'logs', 4102444800000, 'uuid-dropped')",
        )
        .execute(&c.pool)
        .await
        .unwrap();

        c.register("seg-new", "default", "logs", "url", 1, 1)
            .await
            .unwrap();
        let claimed = c.try_claim(1).await.unwrap().pop().unwrap();
        let mut provenance = ProofProvenance::default();
        provenance.record("default", "logs", "uuid-live");
        c.mark_committed_batch(&["seg-new".to_string()], &provenance)
            .await
            .unwrap();

        let watermark = c
            .consumed_proof_watermark("default", "logs")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(watermark.table_uuid.as_deref(), Some("uuid-live"));
        assert_eq!(
            watermark.acknowledged_through_ms,
            claimed.claimed_at.timestamp_millis(),
            "the replacement's boundary is the one THIS transition computed, not \
             the dropped incarnation's higher one"
        );
    }

    /// `ProofProvenance` refuses to stamp the events table, so an events
    /// watermark cannot alternate between proved and unproved depending on
    /// which path advanced it — which would move the boundary backwards on
    /// every flip.
    #[test]
    fn provenance_never_stamps_the_events_table() {
        let mut provenance = ProofProvenance::default();
        provenance.record("default", "", "uuid-events");
        provenance.record("default", "logs", "uuid-logs");
        assert_eq!(provenance.get("default", ""), None);
        assert_eq!(provenance.get("default", "logs"), Some("uuid-logs"));
    }

    /// Mixed-version safety: a catalog written before #2889 has no
    /// `table_uuid` column. Opening it adds the column, its rows keep their
    /// value and read back UNPROVED, and nothing backfills an identity from the
    /// current name — a name is precisely what proves nothing here. The next
    /// terminal claim re-establishes the boundary under its own incarnation.
    #[tokio::test]
    async fn a_pre_upgrade_watermark_row_reads_back_unproved() {
        sqlx::any::install_default_drivers();
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!(
            "sqlite://{}?mode=rwc",
            tmp.path().join("claim.db").display()
        );
        {
            let pool = AnyPool::connect(&uri).await.unwrap();
            sqlx::query(
                "CREATE TABLE consumed_proof_watermarks ( \
                     tenant                    TEXT NOT NULL, \
                     index_id                  TEXT NOT NULL, \
                     acknowledged_through_ms   BIGINT NOT NULL, \
                     PRIMARY KEY (tenant, index_id) )",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO consumed_proof_watermarks VALUES ('default', 'logs', 1234567)",
            )
            .execute(&pool)
            .await
            .unwrap();
            pool.close().await;
        }

        let c = SqlSegmentClaim::connect(&uri, "post-upgrade")
            .await
            .unwrap();
        assert_eq!(
            c.consumed_proof_watermark("default", "logs")
                .await
                .unwrap()
                .unwrap(),
            ConsumedProofWatermark {
                acknowledged_through_ms: 1_234_567,
                table_uuid: None,
            },
            "the stored boundary survives the upgrade and is unproved"
        );

        c.register("seg-1", "default", "logs", "url", 1, 1)
            .await
            .unwrap();
        let claimed = c.try_claim(1).await.unwrap().pop().unwrap();
        let mut provenance = ProofProvenance::default();
        provenance.record("default", "logs", "uuid-live");
        c.mark_committed_batch(&["seg-1".to_string()], &provenance)
            .await
            .unwrap();
        assert_eq!(
            c.consumed_proof_watermark("default", "logs")
                .await
                .unwrap()
                .unwrap(),
            ConsumedProofWatermark {
                acknowledged_through_ms: claimed.claimed_at.timestamp_millis(),
                table_uuid: Some("uuid-live".to_string()),
            },
            "the first post-upgrade terminal claim re-establishes it"
        );
    }

    /// This store has no live Postgres in CI, so a Postgres-only syntax error
    /// in the watermark statements would be a silent runtime failure on the
    /// path that decides which table an acknowledgement lands on. Parse each
    /// one, in the form the Postgres dialect actually sends.
    #[test]
    fn every_watermark_statement_parses_as_postgres() {
        use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
        use datafusion::sql::sqlparser::parser::Parser;

        for sql in [
            WATERMARK_CREATE_SQL,
            WATERMARK_ADD_UUID_SQL,
            WATERMARK_SELECT_SQL,
            WATERMARK_CANDIDATE_SQL,
            WATERMARK_ADVANCE_SQL,
            WATERMARK_REESTABLISH_SQL,
            WATERMARK_ESTABLISH_SQL,
        ] {
            let rendered = Dialect::Postgres.rewrite(sql);
            assert!(!rendered.contains('?'), "unrewritten marker in {rendered}");
            let parsed = Parser::parse_sql(&PostgreSqlDialect {}, &rendered)
                .unwrap_or_else(|e| panic!("does not parse as Postgres: {e}\n{rendered}"));
            assert_eq!(parsed.len(), 1, "one statement per execute(): {rendered}");
        }
    }

    #[tokio::test]
    async fn mark_committed_requires_matching_claimer() {
        let (c1, _tmp) = fresh().await;
        c1.register("seg-1", "default", "", "url", 1, 1)
            .await
            .unwrap();
        let claimed = c1.try_claim(10).await.unwrap();
        assert_eq!(claimed.len(), 1);
        let mut c2 = c1.clone();
        c2.claimer = "other-compactor".to_string();
        let res = c2.mark_committed("seg-1").await;
        assert!(res.is_err(), "cross-claimer commit must fail");
        c1.mark_committed("seg-1").await.unwrap();
    }

    #[tokio::test]
    async fn try_claim_returns_at_most_batch() {
        let (c, _tmp) = fresh().await;
        for i in 0..5 {
            c.register(&format!("seg-{i}"), "default", "", "url", 1, 1)
                .await
                .unwrap();
        }
        let claimed = c.try_claim(3).await.unwrap();
        assert_eq!(claimed.len(), 3);
        let claimed2 = c.try_claim(10).await.unwrap();
        assert_eq!(claimed2.len(), 2);
    }

    #[test]
    fn dialect_rewrite_replaces_question_marks_for_postgres() {
        let sql = "INSERT INTO t (a, b, c) VALUES (?, ?, ?)";
        assert_eq!(Dialect::Sqlite.rewrite(sql), sql);
        assert_eq!(
            Dialect::Postgres.rewrite(sql),
            "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)"
        );
    }

    #[test]
    fn dialect_detection() {
        assert_eq!(
            Dialect::from_uri("postgres://user:pass@h:5432/db"),
            Dialect::Postgres
        );
        assert_eq!(Dialect::from_uri("postgresql://h/db"), Dialect::Postgres);
        assert_eq!(Dialect::from_uri("sqlite::memory:"), Dialect::Sqlite);
    }

    #[tokio::test]
    async fn register_carries_tenant_into_claim() {
        let (c, _tmp) = fresh().await;
        c.register("seg-acme", "acme", "", "url-a", 1, 1)
            .await
            .unwrap();
        c.register("seg-w", "widgets", "", "url-w", 1, 1)
            .await
            .unwrap();
        let claimed = c.try_claim(10).await.unwrap();
        assert_eq!(claimed.len(), 2);
        let names: std::collections::HashSet<_> =
            claimed.iter().map(|c| c.tenant.as_str()).collect();
        assert!(names.contains("acme"));
        assert!(names.contains("widgets"));
    }
}

#[cfg(test)]
mod shard_routing_tests {
    use super::*;

    /// Routing must partition the index keyspace disjointly and completely: a
    /// key owned by two drains is double-claimed, a key owned by none is
    /// stranded forever. Both are silent failures.
    #[test]
    fn index_shards_partition_disjointly_and_completely() {
        for shard_count in [2usize, 4, 8, 16] {
            let mut seen = vec![0usize; shard_count];
            for i in 0..2000 {
                let id = format!("logs-bench-s{i:04}");
                let owners: Vec<usize> = (0..shard_count)
                    .filter(|s| SqlSegmentClaim::index_shard(&id, shard_count) == *s)
                    .collect();
                assert_eq!(
                    owners.len(),
                    1,
                    "index {id} must have exactly one owner at shard_count={shard_count}, got {owners:?}"
                );
                seen[owners[0]] += 1;
            }
            // Balance is not correctness, but a wildly skewed hash would leave
            // drains idle while one does all the work — the thing routing exists
            // to prevent.
            let min = *seen.iter().min().unwrap();
            let max = *seen.iter().max().unwrap();
            assert!(
                max <= min * 2,
                "shard_count={shard_count} badly skewed: min={min} max={max} ({seen:?})"
            );
        }
    }

    /// The mapping must not depend on process, build or architecture — peers
    /// that disagree about ownership double-claim or strand.
    #[test]
    fn index_shard_is_stable() {
        assert_eq!(
            SqlSegmentClaim::index_shard("logs-bench", 8),
            SqlSegmentClaim::index_shard("logs-bench", 8)
        );
        // Pinned values: a hash change silently repartitions a live fleet.
        assert_eq!(SqlSegmentClaim::index_shard("logs-bench-s00", 8), 1);
        assert_eq!(SqlSegmentClaim::index_shard("logs-bench-s01", 8), 6);
        assert_eq!(SqlSegmentClaim::index_shard("events", 8), 4);
    }

    /// shard_count == 1 must be the historical path exactly.
    #[test]
    fn unsharded_claims_everything() {
        for id in ["events", "logs-bench", "logs-bench-s07"] {
            assert_eq!(SqlSegmentClaim::index_shard(id, 1), 0);
        }
    }
}

#[cfg(test)]
mod reclaim_tests {
    use super::*;

    async fn claim_on(tmp: &std::path::Path, who: &str) -> SqlSegmentClaim {
        let uri = format!("sqlite://{}?mode=rwc", tmp.join("c.db").display());
        let c = SqlSegmentClaim::connect(&uri, who).await.unwrap();
        c.ensure_schema().await.unwrap();
        c
    }

    /// A worker that dies mid-claim must not strand its segments forever.
    ///
    /// Before `reclaim_abandoned` nothing returned them: the claim query selects
    /// `status = 'sealed'`, so rows left in `processing` were invisible to every
    /// worker permanently — data accepted, durable, and unqueryable.
    #[tokio::test]
    async fn abandoned_claims_are_reclaimed_and_reclaimable() {
        let tmp = tempfile::tempdir().unwrap();
        let a = claim_on(tmp.path(), "worker-a").await;
        a.register("seg-1", "default", "logs", "s3://b/seg-1.arrow", 10, 1)
            .await
            .unwrap();

        // worker-a claims, then "dies" without committing or releasing.
        let claimed = a.try_claim(10).await.unwrap();
        assert_eq!(claimed.len(), 1);
        // A second worker sees nothing — this is the strand.
        let b = claim_on(tmp.path(), "worker-b").await;
        assert!(
            b.try_claim(10).await.unwrap().is_empty(),
            "an in-flight claim must not be stealable"
        );

        // Not yet old enough: a healthy in-flight claim must survive.
        assert_eq!(
            b.reclaim_abandoned(std::time::Duration::from_secs(3600))
                .await
                .unwrap(),
            0,
            "must not yank work from a worker still within its deadline"
        );
        assert!(b.try_claim(10).await.unwrap().is_empty());

        // Past the deadline: reclaimed and claimable again.
        let n = b
            .reclaim_abandoned(std::time::Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(n, 1, "abandoned claim must be reclaimed");
        let recovered = b.try_claim(10).await.unwrap();
        assert_eq!(recovered.len(), 1, "reclaimed segment must be claimable");
        assert_eq!(recovered[0].segment_url, "s3://b/seg-1.arrow");

        // Idempotent: a second reclaim finds nothing (the row is in flight again).
        assert_eq!(
            b.reclaim_abandoned(std::time::Duration::from_secs(3600))
                .await
                .unwrap(),
            0
        );
    }
}

#[cfg(test)]
mod purge_tests {
    use super::*;

    /// Committed rows must be prunable, and only when old enough — a purge that
    /// races mirror retention re-registers the object and re-drains it, which is
    /// silent duplicate data rather than a visible failure.
    #[tokio::test]
    async fn purge_removes_only_old_committed_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("sqlite://{}?mode=rwc", tmp.path().join("p.db").display());
        let c = SqlSegmentClaim::connect(&uri, "w").await.unwrap();
        c.ensure_schema().await.unwrap();

        c.register("a", "default", "logs", "s3://b/a.arrow", 1, 1)
            .await
            .unwrap();
        c.register("b", "default", "logs", "s3://b/b.arrow", 1, 1)
            .await
            .unwrap();
        let claimed = c.try_claim(10).await.unwrap();
        assert_eq!(claimed.len(), 2);
        c.mark_committed("a").await.unwrap();

        // 'b' is still processing, 'a' is committed but fresh.
        assert_eq!(
            c.purge_committed(std::time::Duration::from_secs(3600))
                .await
                .unwrap(),
            0,
            "a freshly committed row must not be purged"
        );
        // Old enough now. The predicate is strict (`committed_at_ms < cutoff`), so
        // a row committed in the CURRENT millisecond is not yet older than a
        // zero max_age — advance the clock past it rather than loosening the
        // comparison, which would let a purge race a just-completed commit.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert_eq!(
            c.purge_committed(std::time::Duration::ZERO).await.unwrap(),
            1,
            "the committed row must be purged"
        );
        // The in-flight row is untouched — purging must never touch live work.
        assert_eq!(
            c.purge_committed(std::time::Duration::ZERO).await.unwrap(),
            0
        );
        c.release("b").await.unwrap();
        // Release carries a backoff now; wait past it so this still asserts
        // survival rather than accidentally asserting the deferral.
        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        assert_eq!(
            c.try_claim(10).await.unwrap().len(),
            1,
            "'b' survived the purge"
        );
    }
}

#[cfg(test)]
mod lease_tests {
    use super::*;

    async fn c(tmp: &std::path::Path, who: &str) -> SqlSegmentClaim {
        let uri = format!("sqlite://{}?mode=rwc", tmp.join("l.db").display());
        let c = SqlSegmentClaim::connect(&uri, who).await.unwrap();
        c.ensure_schema().await.unwrap();
        c
    }

    /// Exactly one holder at a time; expiry transfers ownership; a superseded
    /// holder cannot release the new owner's lease.
    #[tokio::test]
    async fn table_lease_is_exclusive_and_expires() {
        let tmp = tempfile::tempdir().unwrap();
        let a = c(tmp.path(), "a").await;
        let b = c(tmp.path(), "b").await;
        let ttl = std::time::Duration::from_secs(60);

        assert!(a
            .acquire_table_lease("events", "commit", ttl)
            .await
            .unwrap());
        assert!(
            !b.acquire_table_lease("events", "commit", ttl)
                .await
                .unwrap(),
            "a second holder must be denied while the lease is live"
        );
        // The holder renews freely.
        assert!(a
            .acquire_table_lease("events", "commit", ttl)
            .await
            .unwrap());

        // Expired: ownership transfers.
        assert!(a
            .acquire_table_lease("events", "commit", std::time::Duration::ZERO)
            .await
            .unwrap());
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(
            b.acquire_table_lease("events", "commit", ttl)
                .await
                .unwrap(),
            "an expired lease must be takeable"
        );
        assert!(
            !a.acquire_table_lease("events", "commit", ttl)
                .await
                .unwrap(),
            "the superseded holder must not reacquire while b's lease is live"
        );

        // A superseded holder's release must not free b's lease.
        a.release_table_lease("events").await.unwrap();
        assert!(
            !a.acquire_table_lease("events", "commit", ttl)
                .await
                .unwrap(),
            "a's stale release must not have freed b's lease"
        );

        // Different tables are independent.
        assert!(a.acquire_table_lease("other", "commit", ttl).await.unwrap());
    }
}

#[cfg(test)]
mod mirror_sync_cursor_tests {
    use super::*;

    async fn c(tmp: &std::path::Path, who: &str) -> SqlSegmentClaim {
        let uri = format!("sqlite://{}?mode=rwc", tmp.join("cursor.db").display());
        SqlSegmentClaim::connect(&uri, who).await.unwrap()
    }

    #[tokio::test]
    async fn cursor_survives_owner_handoff_and_rejects_stale_progress() {
        let tmp = tempfile::tempdir().unwrap();
        let owner_a = c(tmp.path(), "a").await;
        let owner_b = c(tmp.path(), "b").await;
        let cursor_id = "s3:bucket:/warehouse/:wal-mirror";

        let start = owner_a.mirror_sync_cursor(cursor_id).await.unwrap();
        assert_eq!(start, MirrorSyncCursor::default());
        assert!(owner_a
            .compare_and_set_mirror_sync_cursor(cursor_id, &start, Some("b.arrow"), false, 2)
            .await
            .unwrap());

        let handed_off = owner_b.mirror_sync_cursor(cursor_id).await.unwrap();
        assert_eq!(handed_off.last_key.as_deref(), Some("b.arrow"));
        assert_eq!(handed_off.rotation, 0);
        assert!(handed_off.rotation_started_at_ms.is_some());
        assert_eq!(handed_off.rotation_objects_examined, 2);
        assert_eq!(handed_off.last_completed_at_ms, None);
        assert!(owner_b
            .compare_and_set_mirror_sync_cursor(cursor_id, &handed_off, None, true, 1)
            .await
            .unwrap());

        assert!(
            !owner_a
                .compare_and_set_mirror_sync_cursor(
                    cursor_id,
                    &handed_off,
                    Some("c.arrow"),
                    false,
                    1,
                )
                .await
                .unwrap(),
            "an owner using pre-wrap state must not overwrite the next rotation"
        );
        let completed = owner_a.mirror_sync_cursor(cursor_id).await.unwrap();
        assert_eq!(completed.last_key, None);
        assert_eq!(completed.rotation, 1);
        assert_eq!(completed.rotation_started_at_ms, None);
        assert_eq!(completed.rotation_objects_examined, 3);
        assert!(completed.last_completed_at_ms.is_some());
    }

    #[tokio::test]
    async fn cursor_telemetry_migrates_without_resetting_progress() {
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!(
            "sqlite://{}?mode=rwc",
            tmp.path().join("cursor.db").display()
        );
        sqlx::any::install_default_drivers();
        let pool = AnyPool::connect(&uri).await.unwrap();
        sqlx::query(
            r#"
            CREATE TABLE mirror_sync_cursors (
                cursor_id TEXT PRIMARY KEY,
                last_key TEXT,
                rotation BIGINT NOT NULL DEFAULT 0,
                updated_at_ms BIGINT NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO mirror_sync_cursors (cursor_id, last_key, rotation, updated_at_ms) VALUES (?, ?, ?, ?)",
        )
        .bind("legacy")
        .bind("wal-mirror/b.arrow")
        .bind(7_i64)
        .bind(1_i64)
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let owner = c(tmp.path(), "upgrade").await;
        let mut cursor = owner.mirror_sync_cursor("legacy").await.unwrap();
        assert_eq!(cursor.last_key.as_deref(), Some("wal-mirror/b.arrow"));
        assert_eq!(cursor.rotation, 7);
        assert_eq!(cursor.rotation_started_at_ms, None);
        assert_eq!(cursor.rotation_objects_examined, 0);
        assert_eq!(cursor.last_completed_at_ms, None);

        cursor.rotation_started_at_ms = Some(1);
        assert!(owner
            .compare_and_set_mirror_sync_cursor("legacy", &cursor, None, true, 1)
            .await
            .unwrap());
        let completed = owner.mirror_sync_cursor("legacy").await.unwrap();
        assert_eq!(completed.rotation, 8);
        assert_eq!(completed.rotation_objects_examined, 1);
        assert!(completed.last_completed_at_ms.is_some());
    }
}

#[cfg(test)]
mod retention_order_tests {
    use super::*;

    async fn fresh_claim() -> (SqlSegmentClaim, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("sqlite://{}/c.db?mode=rwc", tmp.path().display());
        let c = SqlSegmentClaim::connect(&uri, "t".to_string())
            .await
            .unwrap();
        (c, tmp)
    }

    #[tokio::test]
    async fn purgeable_lists_only_old_committed_rows_with_keys() {
        let (c, _t) = fresh_claim().await;
        c.register("s1", "default", "", "wal-mirror/s1.arrow", 10, 1)
            .await
            .unwrap();
        c.register("s2", "default", "", "wal-mirror/s2.arrow", 10, 1)
            .await
            .unwrap();
        // Sealed rows are NOT purgeable at any age: they have not been drained,
        // and deleting them would discard un-ingested data.
        assert!(c
            .purgeable_committed(Duration::from_secs(0), 100)
            .await
            .unwrap()
            .is_empty());

        let claimed = c.try_claim(2).await.unwrap();
        assert_eq!(claimed.len(), 2);
        c.mark_committed_batch(&["s1".to_string()], &ProofProvenance::default())
            .await
            .unwrap();

        // The sweep is `committed_at_ms < now - max_age`, STRICTLY less, so a row
        // committed in the same millisecond as an age-0 sweep is correctly
        // excluded. Without this wait the test is a race against the clock
        // granularity: it passes only when a millisecond happens to elapse
        // between the commit above and the sweep below, which on a warm machine
        // it often does not. Observed failing and passing at the same commit on
        // 2026-08-27.
        //
        // Waiting is right rather than relaxing the query to `<=`: retention
        // deleting a row in the same millisecond it committed is exactly the
        // behaviour the `Duration::from_secs(3600)` assertion below exists to
        // forbid.
        tokio::time::sleep(Duration::from_millis(5)).await;

        let p = c
            .purgeable_committed(Duration::from_secs(0), 100)
            .await
            .unwrap();
        assert_eq!(p.len(), 1, "only the committed row is purgeable");
        assert_eq!(p[0].0, "s1");
        assert_eq!(
            p[0].1, "wal-mirror/s1.arrow",
            "the mirror key must come back so the object can be deleted FIRST"
        );

        // A future cutoff must exclude it, or retention would delete rows the
        // moment they commit.
        let none = c
            .purgeable_committed(Duration::from_secs(3600), 100)
            .await
            .unwrap();
        assert!(
            none.is_empty(),
            "a row younger than the retention age must be kept"
        );
    }

    #[tokio::test]
    async fn purge_by_id_only_touches_committed_rows() {
        let (c, _t) = fresh_claim().await;
        c.register("s1", "default", "", "u1", 10, 1).await.unwrap();
        c.try_claim(1).await.unwrap();
        // Still 'processing' -- purging it would delete a row for a segment that
        // is mid-drain, so the drain's mark_committed would find nothing and the
        // segment could be re-registered and drained twice.
        assert_eq!(c.purge_committed_ids(&["s1".to_string()]).await.unwrap(), 0);

        c.mark_committed_batch(&["s1".to_string()], &ProofProvenance::default())
            .await
            .unwrap();
        assert_eq!(c.purge_committed_ids(&["s1".to_string()]).await.unwrap(), 1);
        assert_eq!(
            c.purge_committed_ids(&["s1".to_string()]).await.unwrap(),
            0,
            "idempotent"
        );
    }

    #[tokio::test]
    async fn purge_by_id_batches_large_retention_sets() {
        let (c, _t) = fresh_claim().await;
        let ids: Vec<String> = (0..300).map(|n| format!("segment-{n}")).collect();
        for id in &ids {
            c.register(id, "default", "", &format!("mirror/{id}"), 10, 1)
                .await
                .unwrap();
        }
        assert_eq!(c.try_claim(ids.len()).await.unwrap().len(), ids.len());
        c.mark_committed_batch(&ids, &ProofProvenance::default())
            .await
            .unwrap();

        assert_eq!(
            c.purge_committed_ids(&ids).await.unwrap(),
            ids.len() as u64,
            "every chunk must be deleted, not only the first SQL batch"
        );
        assert_eq!(c.purge_committed_ids(&ids).await.unwrap(), 0);
    }
}

#[cfg(test)]
mod eligible_claim_tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_declines_rather_than_silently_claiming_everything() {
        // The statement needs SKIP LOCKED and a CTE-driven UPDATE ... FROM, which
        // sqlite has neither of. Returning an empty vec (caller falls back to the
        // peek gate) is correct; silently claiming without the eligibility test
        // would defeat commit amortization on every non-Postgres deployment.
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("sqlite://{}/c.db?mode=rwc", tmp.path().display());
        let c = SqlSegmentClaim::connect(&uri, "t".to_string())
            .await
            .unwrap();
        c.register("s1", "default", "", "u1", 1_000_000, 10)
            .await
            .unwrap();
        let got = c
            .try_claim_eligible(256, 1, Duration::from_secs(0))
            .await
            .unwrap();
        assert!(
            got.is_empty(),
            "sqlite must decline, not claim unconditionally"
        );
        // And the row must be untouched, still claimable by the normal path.
        assert_eq!(c.try_claim(1).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn zero_batch_is_a_no_op() {
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("sqlite://{}/c.db?mode=rwc", tmp.path().display());
        let c = SqlSegmentClaim::connect(&uri, "t".to_string())
            .await
            .unwrap();
        assert!(c
            .try_claim_eligible(0, 0, Duration::from_secs(0))
            .await
            .unwrap()
            .is_empty());
    }
}

#[cfg(test)]
mod crash_window_tests {
    use super::*;

    async fn fresh() -> (SqlSegmentClaim, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("sqlite://{}/c.db?mode=rwc", tmp.path().display());
        let c = SqlSegmentClaim::connect(&uri, "t".to_string())
            .await
            .unwrap();
        (c, tmp)
    }

    /// Death AFTER the Iceberg commit lands, BEFORE mark_committed_batch.
    ///
    /// The dangerous one. The row is stuck in 'processing' while its rows are
    /// already durable in the table, so requeueing it commits the same segment
    /// twice -- with no error and no counter to reveal the duplication.
    #[tokio::test]
    async fn abandoned_claim_is_reported_for_proof_not_blindly_requeued() {
        let (c, _t) = fresh().await;
        c.register("s1", "default", "idx", "wal-mirror/s1.arrow", 100, 5)
            .await
            .unwrap();
        c.try_claim(1).await.unwrap();
        // The cutoff is `now - max_age`, and the comparison is strict, so a claim
        // made in the SAME millisecond is not yet older than a zero max_age.
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Visible to the prover, with everything needed to identify the target
        // table and check it against the consumed set.
        let ab = c
            .abandoned_claims(Duration::from_secs(0), 10)
            .await
            .unwrap();
        assert_eq!(ab.len(), 1);
        assert_eq!(ab[0].id, "s1");
        assert_eq!(
            ab[0].index_id, "idx",
            "target table must survive, or the proof reads the wrong consumed set"
        );

        // Proven committed => marked committed, NOT requeued.
        assert_eq!(
            c.mark_reclaimed_committed(&["s1".to_string()], &ProofProvenance::default())
                .await
                .unwrap(),
            1
        );
        assert!(
            c.try_claim(10).await.unwrap().is_empty(),
            "a segment already in the table must never be claimable again"
        );
    }

    /// `claimed_at` must be the REAL claim time, because the caller uses it to
    /// decide whether its proof-of-commit can be trusted at all.
    ///
    /// The proof is the cumulative consumed-segment set over the snapshots
    /// still in the table's metadata, and snapshot expiry runs by default. A
    /// segment claimed before the retained history begins may have been
    /// committed and had its proving snapshot expired — indistinguishable, from
    /// the set alone, from a segment never committed. The comparison that tells
    /// them apart is the history floor against the claim time, and this method
    /// used to stamp `claimed_at: Utc::now()` on every returned row, discarding
    /// the one value that comparison needs.
    ///
    /// Against that code this test FAILS: every claim looks like it was made
    /// this instant, so every proof looks trustworthy.
    #[tokio::test]
    async fn an_abandoned_claim_reports_when_it_was_actually_claimed() {
        let (c, _t) = fresh().await;
        c.register("s9", "default", "", "wal-mirror/s9.arrow", 100, 5)
            .await
            .unwrap();
        let before = now_millis();
        c.try_claim(1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let after = now_millis();

        let ab = c
            .abandoned_claims(Duration::from_secs(0), 10)
            .await
            .unwrap();
        assert_eq!(ab.len(), 1);
        let claimed_at = ab[0].claimed_at.timestamp_millis();
        assert!(
            claimed_at >= before && claimed_at <= after,
            "claimed_at must be when the claim was taken ({before}..={after}), got {claimed_at}"
        );
        assert!(
            claimed_at < now_millis(),
            "claimed_at must not be restamped to the read time"
        );
    }

    /// Death BEFORE the commit: the row must come back, or the data is lost.
    #[tokio::test]
    async fn unproven_claim_is_requeued_and_redrained() {
        let (c, _t) = fresh().await;
        c.register("s2", "default", "", "wal-mirror/s2.arrow", 100, 5)
            .await
            .unwrap();
        c.try_claim(1).await.unwrap();
        assert_eq!(c.requeue_claims(&["s2".to_string()]).await.unwrap(), 1);
        let again = c.try_claim(10).await.unwrap();
        assert_eq!(again.len(), 1, "un-committed work must return to the queue");
        assert_eq!(again[0].id, "s2");
    }

    /// Neither disposition may touch a healthy in-flight claim.
    #[tokio::test]
    async fn a_young_claim_is_not_abandoned() {
        let (c, _t) = fresh().await;
        c.register("s3", "default", "", "u", 1, 1).await.unwrap();
        c.try_claim(1).await.unwrap();
        let ab = c
            .abandoned_claims(Duration::from_secs(3600), 10)
            .await
            .unwrap();
        assert!(
            ab.is_empty(),
            "a slow but healthy drain must not have its work stolen"
        );
    }

    /// Both dispositions are idempotent: a crashed reclaim pass must be safe to
    /// repeat, since that is exactly when it runs.
    #[tokio::test]
    async fn dispositions_are_idempotent() {
        let (c, _t) = fresh().await;
        c.register("s4", "default", "", "u", 1, 1).await.unwrap();
        c.try_claim(1).await.unwrap();
        assert_eq!(
            c.mark_reclaimed_committed(&["s4".to_string()], &ProofProvenance::default())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            c.mark_reclaimed_committed(&["s4".to_string()], &ProofProvenance::default())
                .await
                .unwrap(),
            0
        );
        // And a committed row can never be requeued by the other branch.
        assert_eq!(c.requeue_claims(&["s4".to_string()]).await.unwrap(), 0);
    }
}

#[cfg(test)]
mod local_commit_mark_tests {
    use super::*;

    async fn fresh() -> (SqlSegmentClaim, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("sqlite://{}/c.db?mode=rwc", tmp.path().display());
        let c = SqlSegmentClaim::connect(&uri, "t".to_string())
            .await
            .unwrap();
        (c, tmp)
    }

    pub(super) fn local(id: &str) -> LocalCommittedSegment {
        LocalCommittedSegment {
            id: id.to_string(),
            tenant: "default".to_string(),
            index_id: String::new(),
            segment_url: format!("wal-mirror/{id}.arrow"),
            bytes: 7,
        }
    }

    /// Readback through the store's own dialect, so the Postgres arm of these
    /// cases (`local_commit_mark_postgres`) gets `$1` rather than a `?` the
    /// backend rejects.
    pub(super) async fn row(
        c: &SqlSegmentClaim,
        id: &str,
    ) -> Option<(String, Option<i64>, String)> {
        let q = c
            .dialect
            .rewrite("SELECT status, committed_at_ms, segment_url FROM wal_segments WHERE id = ?");
        sqlx::query_as::<_, (String, Option<i64>, String)>(&q)
            .bind(id)
            .fetch_optional(&c.pool)
            .await
            .unwrap()
    }

    /// #4913: the local drain's mark transitions the row the INGESTER wrote,
    /// keeping the key the uploader used, and inserts one where the upload has
    /// not registered yet — so a late `ON CONFLICT DO NOTHING` registration
    /// cannot take the object back out of retention's reach.
    #[tokio::test]
    async fn the_local_mark_upserts_sealed_rows_and_absent_ones() {
        let (c, _t) = fresh().await;
        c.register("reg", "default", "", "wal-mirror/reg.arrow", 11, 3)
            .await
            .unwrap();
        let marked = c
            .mark_committed_local(&[local("reg"), local("absent")])
            .await
            .unwrap();
        assert_eq!(marked, vec!["reg".to_string(), "absent".to_string()]);
        let (status, committed_at, url) = row(&c, "reg").await.unwrap();
        assert_eq!(status, "committed");
        assert!(committed_at.is_some());
        assert_eq!(url, "wal-mirror/reg.arrow", "the uploader's key is the key");
        assert_eq!(row(&c, "absent").await.unwrap().0, "committed");
        // The late registration loses, as `register` is insert-ignore.
        assert!(!c
            .register("absent", "default", "", "wal-mirror/absent.arrow", 1, 1)
            .await
            .unwrap());
        assert_eq!(row(&c, "absent").await.unwrap().0, "committed");
        // Both are now purgeable by the unchanged retention pass.
        let purgeable = c.purgeable_committed(Duration::ZERO, 10).await.unwrap();
        let keys: Vec<&str> = purgeable.iter().map(|(_, k)| k.as_str()).collect();
        assert_eq!(
            keys,
            vec!["wal-mirror/reg.arrow", "wal-mirror/absent.arrow"]
        );
    }

    /// The mark runs every cycle for as long as the file is in `committed/`.
    /// Re-stamping `committed_at_ms` would push retention's clock forward each
    /// time and the object would never come due.
    #[tokio::test]
    async fn re_marking_preserves_the_first_committed_timestamp() {
        let (c, _t) = fresh().await;
        c.mark_committed_local(&[local("s")]).await.unwrap();
        let first = row(&c, "s").await.unwrap().1.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let marked = c.mark_committed_local(&[local("s")]).await.unwrap();
        assert_eq!(
            marked,
            vec!["s".to_string()],
            "idempotent, and still durable"
        );
        assert_eq!(row(&c, "s").await.unwrap().1.unwrap(), first);
    }

    /// A claim-mode drain owns its `processing` rows. The local mark must not
    /// take one over — that drain will mark it with its own claimer guard, and
    /// its consumed-proof watermark depends on the transition.
    #[tokio::test]
    async fn the_local_mark_leaves_claimed_rows_alone() {
        let (c, _t) = fresh().await;
        c.register("held", "default", "", "wal-mirror/held.arrow", 1, 1)
            .await
            .unwrap();
        c.try_claim(1).await.unwrap();
        let marked = c.mark_committed_local(&[local("held")]).await.unwrap();
        assert!(
            marked.is_empty(),
            "a claimed segment's local evidence must be held, not released"
        );
        assert_eq!(row(&c, "held").await.unwrap().0, "processing");
    }

    /// The deployed catalog is Postgres and this store has none in CI, so a
    /// Postgres-only syntax error in the mark would be a silent runtime failure
    /// on the path that decides which mirror objects may be deleted. Parse each
    /// statement in the form the Postgres dialect actually sends.
    #[test]
    fn every_local_mark_statement_parses_as_postgres() {
        use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
        use datafusion::sql::sqlparser::parser::Parser;

        let placeholders = "?, ?";
        for sql in [
            local_mark_update_sql(placeholders),
            local_mark_readback_sql(placeholders),
            LOCAL_MARK_INSERT_SQL.to_string(),
        ] {
            let rendered = Dialect::Postgres.rewrite(&sql);
            assert!(!rendered.contains('?'), "unrewritten marker in {rendered}");
            let parsed = Parser::parse_sql(&PostgreSqlDialect {}, &rendered)
                .unwrap_or_else(|e| panic!("does not parse as Postgres: {e}\n{rendered}"));
            assert_eq!(parsed.len(), 1, "one statement per execute(): {rendered}");
        }
    }

    /// The bind order is the text order, and getting it wrong binds a timestamp
    /// as an id (or an id as a timestamp) only on Postgres, where the markers
    /// are numbered. `$1` must be the COALESCE timestamp.
    #[test]
    fn the_mark_update_binds_its_timestamp_first() {
        let rendered = Dialect::Postgres.rewrite(&local_mark_update_sql("?, ?"));
        assert!(
            rendered.contains("COALESCE(committed_at_ms, $1)"),
            "{rendered}"
        );
        assert!(rendered.contains("id IN ($2, $3)"), "{rendered}");
    }

    /// There are no claims to reclaim on this path, so the mark writes no
    /// consumed-proof boundary: a watermark written without a terminal claim
    /// transition is exactly the unproved acknowledgement #2889 refuses.
    #[tokio::test]
    async fn the_local_mark_writes_no_consumed_proof_watermark() {
        let (c, _t) = fresh().await;
        c.mark_committed_local(&[local("s")]).await.unwrap();
        assert!(c
            .consumed_proof_watermark("default", "")
            .await
            .unwrap()
            .is_none());
    }
}

/// The four `local_commit_mark_tests` cases, run against a live Postgres.
///
/// [`SqlSegmentClaim::mark_committed_local`] decides which mirror objects
/// retention may delete, the deployed catalog is Postgres, and every hermetic
/// test of it runs on SQLite. Two of its properties are backend behaviour that
/// no parser check establishes: `ON CONFLICT(id) DO NOTHING` reporting zero
/// `rows_affected` for the row it skipped, and `COALESCE(committed_at_ms, $1)`
/// keeping the first stamp so retention's clock does not restart every cycle.
///
/// `#[ignore]`d, and wired into the compose lifecycle that already starts a
/// Postgres for the query server's job-ownership suite
/// (`.github/workflows/ci.yml`, `scripts/ci-local.sh`):
///
/// ```text
/// SIGLAKE_TEST_JOBS_POSTGRES_URI=postgres://siglake:siglake@localhost:5433/siglake \
///   cargo test -p siglake-storage --lib local_commit_mark_postgres -- --ignored --nocapture
/// ```
///
/// Each case gets its own Postgres schema. compose points its own ingest and
/// compactor at this database (`deploy/docker-compose.yml`
/// `SIGLAKE_CATALOG_URI`), so a claim run in the public schema would take live
/// rows and strand them in `processing`.
#[cfg(test)]
mod local_commit_mark_postgres {
    use super::local_commit_mark_tests::{local, row};
    use super::*;
    use anyhow::ensure;
    use std::collections::BTreeSet;

    const URI_VAR: &str = "SIGLAKE_TEST_JOBS_POSTGRES_URI";

    /// A disposable schema and the URI that makes it this store's whole world.
    /// sqlx passes `options[...]` through to the Postgres startup packet as
    /// `-c search_path=…`, so `ensure_schema`'s `CREATE TABLE IF NOT EXISTS`
    /// lands here rather than next to compose's own `wal_segments`.
    struct Scratch {
        name: String,
        uri: String,
    }

    /// `base` with the scratch schema pinned as the whole `search_path`.
    /// Pure, so the one part of the isolation that does not need a server is
    /// checked by an ordinary test (`the_scratch_uri_pins_the_search_path`).
    fn scratch_uri(base: &str, schema: &str) -> String {
        let sep = if base.contains('?') { '&' } else { '?' };
        format!("{base}{sep}options[search_path]={schema}")
    }

    impl Scratch {
        async fn create(admin: &AnyPool, base: &str) -> Result<Self> {
            let name = format!("siglake_local_mark_{}", uuid::Uuid::new_v4().simple());
            sqlx::query(&format!("CREATE SCHEMA {name}"))
                .execute(admin)
                .await
                .with_context(|| format!("create scratch schema {name}"))?;
            let uri = scratch_uri(base, &name);
            Ok(Self { name, uri })
        }

        async fn connect(&self) -> Result<SqlSegmentClaim> {
            let claim = SqlSegmentClaim::connect(&self.uri, "pg-local-mark".to_string()).await?;
            ensure!(
                claim.dialect == Dialect::Postgres,
                "{URI_VAR} must name a Postgres, not a {:?} URI",
                claim.dialect
            );
            // The isolation is the premise of the claim case below, so check it
            // instead of trusting it: the schema this store just created its
            // table in has to be the scratch one.
            let here: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name = 'wal_segments'",
            )
            .bind(&self.name)
            .fetch_one(&claim.pool)
            .await
            .context("look up the scratch schema's wal_segments")?;
            ensure!(
                here == 1,
                "search_path did not take: no wal_segments in schema {}",
                self.name
            );
            Ok(claim)
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

    /// Run one case in a fresh schema and drop the schema either way. The cases
    /// return `Result` rather than asserting so that a failure still cleans up
    /// and still names which of the four went wrong.
    async fn in_scratch<F, Fut>(admin: &AnyPool, base: &str, case: F) -> Result<()>
    where
        F: FnOnce(SqlSegmentClaim) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let scratch = Scratch::create(admin, base).await?;
        let outcome = async {
            let claim = scratch.connect().await?;
            case(claim).await
        }
        .await;
        scratch.drop_schema(admin).await;
        outcome
    }

    /// `purgeable_committed` takes rows strictly older than its cutoff, and the
    /// two stamps this case writes can share a millisecond with the read. Poll
    /// for the end state instead of asserting on the first attempt; the claim is
    /// that both rows come due, not that the clock ticked in between. Order is
    /// deliberately not asserted either — equal stamps leave `ORDER BY
    /// committed_at_ms` nothing to break the tie with.
    async fn purgeable_keys(claim: &SqlSegmentClaim, want: usize) -> Result<BTreeSet<String>> {
        let mut keys = BTreeSet::new();
        for _ in 0..50 {
            keys = claim
                .purgeable_committed(Duration::ZERO, 10)
                .await?
                .into_iter()
                .map(|(_, key)| key)
                .collect();
            if keys.len() >= want {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(keys)
    }

    /// The mark transitions the row the ingester wrote, keeping the key the
    /// uploader used, and inserts one where the upload has not registered yet.
    /// The late registration must then lose: on Postgres as on SQLite, a
    /// skipped `ON CONFLICT DO NOTHING` insert reports no rows affected.
    async fn upserts_sealed_rows_and_absent_ones(claim: SqlSegmentClaim) -> Result<()> {
        ensure!(
            claim
                .register("reg", "default", "", "wal-mirror/reg.arrow", 11, 3)
                .await?,
            "registering a fresh id inserts a row"
        );
        let marked = claim
            .mark_committed_local(&[local("reg"), local("absent")])
            .await?;
        ensure!(
            marked == vec!["reg".to_string(), "absent".to_string()],
            "both ids must come back durable, got {marked:?}"
        );
        let (status, committed_at, url) = row(&claim, "reg").await.context("reg row")?;
        ensure!(status == "committed", "reg is {status}, not committed");
        ensure!(committed_at.is_some(), "reg has no committed_at_ms");
        ensure!(
            url == "wal-mirror/reg.arrow",
            "the uploader's key is the key, got {url}"
        );
        let absent = row(&claim, "absent").await.context("absent row")?;
        ensure!(absent.0 == "committed", "absent is {}", absent.0);
        ensure!(
            !claim
                .register("absent", "default", "", "wal-mirror/absent.arrow", 1, 1)
                .await?,
            "a late registration must report zero rows affected, or the caller \
             would read it as a fresh insert"
        );
        let after = row(&claim, "absent").await.context("absent row")?;
        ensure!(
            after.0 == "committed",
            "the late registration took the object back out of retention's \
             reach: absent is {}",
            after.0
        );
        let keys = purgeable_keys(&claim, 2).await?;
        ensure!(
            keys == BTreeSet::from([
                "wal-mirror/absent.arrow".to_string(),
                "wal-mirror/reg.arrow".to_string(),
            ]),
            "retention must see both objects, got {keys:?}"
        );
        Ok(())
    }

    /// The mark runs every cycle for as long as the file is in `committed/`, so
    /// `COALESCE(committed_at_ms, $1)` has to preserve the first stamp. Getting
    /// the Postgres marker numbering wrong here re-stamps the row, and the
    /// object never comes due. The sleep makes the second stamp a different
    /// millisecond, so a re-stamp is visible.
    async fn re_marking_preserves_the_first_timestamp(claim: SqlSegmentClaim) -> Result<()> {
        claim.mark_committed_local(&[local("s")]).await?;
        let first = row(&claim, "s")
            .await
            .context("stamped row")?
            .1
            .context("first committed_at_ms")?;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let marked = claim.mark_committed_local(&[local("s")]).await?;
        ensure!(
            marked == vec!["s".to_string()],
            "idempotent, and still durable, got {marked:?}"
        );
        let again = row(&claim, "s")
            .await
            .context("re-marked row")?
            .1
            .context("second committed_at_ms")?;
        ensure!(
            again == first,
            "re-marking re-stamped the row: {first} -> {again}"
        );
        Ok(())
    }

    /// A claim-mode drain owns its `processing` rows. `status IN ('sealed',
    /// 'committed')` must hold one back here; the claiming drain marks it with
    /// its own claimer guard, and its consumed-proof watermark depends on that
    /// transition.
    async fn leaves_claimed_rows_alone(claim: SqlSegmentClaim) -> Result<()> {
        claim
            .register("held", "default", "", "wal-mirror/held.arrow", 1, 1)
            .await?;
        let claimed = claim.try_claim(1).await?;
        let ids: Vec<&str> = claimed.iter().map(|c| c.id.as_str()).collect();
        ensure!(
            ids == vec!["held"],
            "the claim must see this case's row and nothing else, got {ids:?}"
        );
        let marked = claim.mark_committed_local(&[local("held")]).await?;
        ensure!(
            marked.is_empty(),
            "a claimed segment's local evidence must be held, not released: {marked:?}"
        );
        let held = row(&claim, "held").await.context("held row")?;
        ensure!(held.0 == "processing", "held is {}", held.0);
        Ok(())
    }

    /// There are no claims to reclaim on this path, so the mark writes no
    /// consumed-proof boundary: a watermark written without a terminal claim
    /// transition is the unproved acknowledgement #2889 refuses.
    async fn writes_no_consumed_proof_watermark(claim: SqlSegmentClaim) -> Result<()> {
        claim.mark_committed_local(&[local("s")]).await?;
        let watermark = claim.consumed_proof_watermark("default", "").await?;
        ensure!(
            watermark.is_none(),
            "the local mark established a boundary: {watermark:?}"
        );
        Ok(())
    }

    /// Both gates run this module by NAME, and a filter that matches nothing
    /// runs zero tests and exits 0 — so a rename here would take the Postgres
    /// coverage out of the compose step in both files and still report green.
    /// Same argument as `scripts/check-shell-job-parity.py`, one level down.
    #[test]
    fn both_gates_run_this_module_by_name() {
        let module = module_path!().rsplit("::").next().expect("module name");
        for rel in [".github/workflows/ci.yml", "scripts/ci-local.sh"] {
            let path =
                std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../..")).join(rel);
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
            // The filter is compared as a whole token: a filter that is nearly
            // this module's name is exactly the case that matches zero tests.
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
                "{rel} runs no `cargo test -p siglake-storage --lib {module}`, so the \
                 compose step's Postgres coverage matches zero tests; its --lib filters \
                 are {filters:?}"
            );
            assert!(text.contains(URI_VAR), "{rel} must give that run {URI_VAR}");
        }
    }

    /// The isolation rests on sqlx turning `options[search_path]` into the
    /// startup parameter Postgres reads as `-c search_path=…`. That much needs
    /// no server, and getting it wrong silently puts the scratch tables — and
    /// this store's claim — in the public schema compose's own ingest and
    /// compactor are using.
    #[test]
    fn the_scratch_uri_pins_the_search_path() {
        use sqlx::postgres::PgConnectOptions;
        use std::str::FromStr;

        for base in [
            "postgres://siglake:siglake@localhost:5433/siglake",
            "postgres://siglake:siglake@localhost:5433/siglake?sslmode=disable",
        ] {
            let uri = scratch_uri(base, "siglake_local_mark_deadbeef");
            let opts = PgConnectOptions::from_str(&uri).expect(&uri);
            assert_eq!(
                opts.get_options(),
                Some("-c search_path=siglake_local_mark_deadbeef"),
                "{uri}"
            );
            assert_eq!(opts.get_database(), Some("siglake"), "{uri}");
            assert_eq!(opts.get_port(), 5433, "{uri}");
            assert_eq!(
                Dialect::from_uri(&uri),
                Dialect::Postgres,
                "the store must still rewrite markers as $N: {uri}"
            );
        }
    }

    #[tokio::test]
    #[ignore]
    async fn the_local_mark_behaves_the_same_against_postgres() {
        // Skipping when the variable is absent keeps `--ignored` runnable
        // wherever there is no Postgres. The compose step that sets it also
        // runs `jobs_postgres_ownership`, which panics on an unset variable, so
        // a dropped variable in CI still fails the gate there.
        let base = match std::env::var(URI_VAR) {
            Ok(uri) if !uri.trim().is_empty() => uri,
            _ => {
                eprintln!("skipped: {URI_VAR} is unset; nothing was verified");
                return;
            }
        };
        sqlx::any::install_default_drivers();
        let admin = AnyPool::connect(&base)
            .await
            .unwrap_or_else(|e| panic!("connect {URI_VAR}: {e}"));

        let outcomes = [
            (
                "sealed and absent rows upsert, and a late registration loses",
                in_scratch(&admin, &base, upserts_sealed_rows_and_absent_ones).await,
            ),
            (
                "re-marking preserves the first committed timestamp",
                in_scratch(&admin, &base, re_marking_preserves_the_first_timestamp).await,
            ),
            (
                "a claimed row is left to its claim-mode drain",
                in_scratch(&admin, &base, leaves_claimed_rows_alone).await,
            ),
            (
                "the mark writes no consumed-proof watermark",
                in_scratch(&admin, &base, writes_no_consumed_proof_watermark).await,
            ),
        ];
        let failed: Vec<String> = outcomes
            .into_iter()
            .filter_map(|(case, outcome)| outcome.err().map(|e| format!("- {case}: {e:#}")))
            .collect();
        assert!(
            failed.is_empty(),
            "the local mark behaves differently on Postgres:\n{}",
            failed.join("\n")
        );
    }
}
