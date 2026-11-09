use super::{owned::OwnedPool, *};
use sqlx::{Connection, Row};
use std::sync::Arc;

/// Dedicated bounded PRIMARY connections only. No cache (including absence),
/// replica fallback, history/catalog/job pool injection, or implicit activation.
pub struct PolicyReader {
    pub(super) pool: Arc<OwnedPool>,
}
/// Offline reference/test API, not an HTTP capability or authorization proof.
pub struct PolicyManager {
    pub(super) pool: Arc<OwnedPool>,
}
impl std::fmt::Debug for PolicyReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PolicyReader([redacted])")
    }
}
impl std::fmt::Debug for PolicyManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PolicyManager([redacted])")
    }
}
impl PolicyReader {
    pub async fn connect(config: PolicyConfig) -> Result<Self, PolicyError> {
        Ok(Self {
            pool: OwnedPool::connect(config, Role::Reader).await?,
        })
    }
    pub async fn lookup(&self, key: &RuntimeKey) -> Result<PolicyLookup, PolicyError> {
        lookup(&self.pool, key, None).await
    }
    /// Fresh configured-primary health for callers without a verified agent key.
    /// Checkout verifies role/schema/primary/closed state, without reading or
    /// inventing a policy tuple. No cached health latch or agent attribution.
    pub async fn preflight(&self) -> Result<(), PolicyError> {
        tokio::time::timeout(self.pool.budget, async {
            let mut lease = self.pool.acquire().await?;
            lease.complete().await
        })
        .await
        .map_err(|_| PolicyError::Unavailable)?
    }
    pub fn close(&self) {
        self.pool.close();
    }
}
impl Drop for PolicyReader {
    fn drop(&mut self) {
        self.close();
    }
}
impl PolicyManager {
    pub async fn connect(config: PolicyConfig) -> Result<Self, PolicyError> {
        Ok(Self {
            pool: OwnedPool::connect(config, Role::Manager).await?,
        })
    }
    pub async fn lookup(&self, binding: &ManagementBinding) -> Result<PolicyLookup, PolicyError> {
        lookup(&self.pool, &binding.key, Some(binding)).await
    }
    /// Bounded primary read of unexpired edits for one server-selected binding.
    /// The context constructor is NOT human/session authorization. The next
    /// Console layer must authorize the current same-org human on every page.
    pub async fn audit_page(
        &self,
        binding: &ManagementBinding,
        context: &AuthorizedHumanManagementContext,
        request: AuditPageRequest,
    ) -> Result<AuditPage, PolicyError> {
        if context.org != binding.org {
            return Err(PolicyError::Binding);
        }
        tokio::time::timeout(self.pool.budget, async {
            let mut lease = self.pool.acquire().await?;
            let mut tx = begin(&mut lease, &binding.key, true, self.pool.budget).await?;
            let result = sqlx::query("SELECT schema_version,revision,actor_sub,edited_at,expires_at,old_timeout_seconds,old_scan_bytes_warning,new_timeout_seconds,new_scan_bytes_warning FROM query_policy.read_edits($1,$2,$3,$4,$5,$6,$7)")
                .bind(&binding.key.tenant).bind(&binding.key.issuer).bind(&binding.key.client)
                .bind(binding.org).bind(binding.credential).bind(request.before_revision)
                .bind(i32::from(request.limit)).fetch_all(&mut *tx).await;
            let rows = match result {
                Ok(rows) => rows,
                Err(e) => {
                    let error = save_error(&e);
                    tx.rollback().await.map_err(|_| PolicyError::Unavailable)?;
                    lease.complete().await?;
                    return Err(error);
                }
            };
            if rows.len() > usize::from(request.limit) {
                return Err(PolicyError::Unavailable);
            }
            let mut edits = Vec::with_capacity(rows.len());
            let mut before = request.before_revision;
            for row in rows {
                let decode = |_: sqlx::Error| PolicyError::Unavailable;
                let edit = PolicyEdit {
                    schema_version: row.try_get("schema_version").map_err(decode)?,
                    revision: row.try_get("revision").map_err(decode)?,
                    actor_sub: row.try_get("actor_sub").map_err(decode)?,
                    edited_at: row.try_get("edited_at").map_err(decode)?,
                    expires_at: row.try_get("expires_at").map_err(decode)?,
                    old_settings: SavedSettings::new(row.try_get("old_timeout_seconds").map_err(decode)?, row.try_get("old_scan_bytes_warning").map_err(decode)?).map_err(|_| PolicyError::Unavailable)?,
                    new_settings: SavedSettings::new(row.try_get("new_timeout_seconds").map_err(decode)?, row.try_get("new_scan_bytes_warning").map_err(decode)?).map_err(|_| PolicyError::Unavailable)?,
                };
                edit.validate()?;
                if before.is_some_and(|revision| edit.revision >= revision) {
                    return Err(PolicyError::Unavailable);
                }
                before = Some(edit.revision);
                edits.push(edit);
            }
            tx.commit().await.map_err(|_| PolicyError::Unavailable)?;
            lease.complete().await?;
            let next_before_revision = if edits.len() == usize::from(request.limit) {
                edits.last().map(|edit| edit.revision)
            } else {
                None
            };
            Ok(AuditPage { edits, next_before_revision })
        }).await.map_err(|_| PolicyError::Unavailable)?
    }
    /// Caller must already authorize the human session, exact org/credential UUID,
    /// ready binding and Origin. Manager GUC selection is NOT compromised-app
    /// isolation. A returned Unavailable can mean COMMIT outcome is UNKNOWN:
    /// never claim rollback/success or automatically retry with another revision.
    pub async fn save(
        &self,
        binding: &ManagementBinding,
        actor: &VerifiedHumanActor,
        request: SaveRequest,
    ) -> Result<SavedPolicy, PolicyError> {
        tokio::time::timeout(self.pool.budget, async {
            let mut lease = self.pool.acquire().await?;
            let mut tx = begin(&mut lease, &binding.key, false, self.pool.budget).await?;
            let result: Result<i64, sqlx::Error> =
                sqlx::query_scalar("SELECT query_policy.save($1,$2,$3,$4,$5,$6,$7,$8,$9)")
                    .bind(&binding.key.tenant)
                    .bind(&binding.key.issuer)
                    .bind(&binding.key.client)
                    .bind(binding.org)
                    .bind(binding.credential)
                    .bind(&actor.0)
                    .bind(request.expected_revision())
                    .bind(request.settings().timeout_seconds())
                    .bind(request.settings().scan_bytes_warning())
                    .fetch_one(&mut *tx)
                    .await;
            let revision = match result {
                Ok(n) => n,
                Err(e) => {
                    let error = save_error(&e);
                    // Statement-level errors are known failures, but a failed
                    // cleanup is still unavailable; no upstream diagnostics.
                    tx.rollback().await.map_err(|_| PolicyError::Unavailable)?;
                    lease.complete().await?;
                    return Err(error);
                }
            };
            if revision != request.expected_revision() + 1 {
                return Err(PolicyError::Unavailable);
            }
            tx.commit().await.map_err(|_| PolicyError::Unavailable)?;
            lease.complete().await?;
            Ok(SavedPolicy {
                revision,
                settings: request.settings(),
            })
        })
        .await
        .map_err(|_| PolicyError::Unavailable)?
    }
    pub fn close(&self) {
        self.pool.close();
    }
}
impl Drop for PolicyManager {
    fn drop(&mut self) {
        self.close();
    }
}
async fn begin<'c>(
    conn: &'c mut sqlx::PgConnection,
    key: &RuntimeKey,
    readonly: bool,
    budget: std::time::Duration,
) -> Result<sqlx::Transaction<'c, sqlx::Postgres>, PolicyError> {
    // Raw SQL BEGIN would bypass SQLx's is_in_transaction depth check.
    // A borrowed transaction queues rollback only on this owned socket; on
    // cancellation its non-reusable lease discards it without detached IO.
    let mut tx = conn
        .begin_with(if readonly {
            "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY"
        } else {
            "BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE"
        })
        .await
        .map_err(|_| PolicyError::Unavailable)?;
    // Server-side cleanup backstop for a cancelled socket, not an agent limit.
    sqlx::query("SELECT pg_catalog.set_config('query_policy.tenant',$1,true), pg_catalog.set_config('statement_timeout',$2,true)")
        .bind(&key.tenant)
        .bind(budget.as_millis().max(1).to_string())
        .execute(&mut *tx)
        .await
        .map_err(|_| PolicyError::Unavailable)?;
    Ok(tx)
}
async fn lookup(
    pool: &Arc<OwnedPool>,
    key: &RuntimeKey,
    binding: Option<&ManagementBinding>,
) -> Result<PolicyLookup, PolicyError> {
    tokio::time::timeout(pool.budget, async {
        let mut lease = pool.acquire().await?;
        let mut tx = begin(&mut lease, key, true, pool.budget).await?;
        let row = sqlx::query("SELECT schema_version,revision,timeout_seconds,scan_bytes_warning,org_uuid,credential_uuid FROM query_policy.agent_limits WHERE tenant=$1 AND verified_issuer=$2 AND verified_client_id=$3")
            .bind(&key.tenant).bind(&key.issuer).bind(&key.client).fetch_optional(&mut *tx).await.map_err(|_| PolicyError::Unavailable)?;
        let found = match row {
            None => PolicyLookup::Absent,
            Some(row) => {
                let version: i16 = row.try_get("schema_version").map_err(|_| PolicyError::Unavailable)?;
                let revision: i64 = row.try_get("revision").map_err(|_| PolicyError::Unavailable)?;
                if version != 1 || revision <= 0 { return Err(PolicyError::Unavailable); }
                if let Some(b) = binding {
                    let org: Uuid = row.try_get("org_uuid").map_err(|_| PolicyError::Unavailable)?;
                    let credential: Uuid = row.try_get("credential_uuid").map_err(|_| PolicyError::Unavailable)?;
                    if org != b.org || credential != b.credential { return Err(PolicyError::Binding); }
                }
                let settings = SavedSettings::new(row.try_get("timeout_seconds").map_err(|_| PolicyError::Unavailable)?, row.try_get("scan_bytes_warning").map_err(|_| PolicyError::Unavailable)?).map_err(|_| PolicyError::Unavailable)?;
                PolicyLookup::Found(SavedPolicy { revision, settings })
            }
        };
        tx.commit().await.map_err(|_| PolicyError::Unavailable)?;
        lease.complete().await?;
        Ok(found)
    }).await.map_err(|_| PolicyError::Unavailable)?
}
pub(super) fn save_error(e: &sqlx::Error) -> PolicyError {
    match e.as_database_error().and_then(|e| e.code()).as_deref() {
        Some("QP001") => PolicyError::Conflict,
        Some("QP002") => PolicyError::Binding,
        Some("QP003") => PolicyError::Invalid,
        _ => PolicyError::Unavailable,
    }
}
