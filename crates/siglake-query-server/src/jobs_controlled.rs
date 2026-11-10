//! Atomic controlled submission and owner-only dequeue. Manual schema required.
use super::*;
use crate::query_controls::jobs::{JobContext, JobSnapshot};
impl JobStore {
    #[cfg(test)]
    pub(crate) fn controlled_pool_for_test(&self) -> &PgPool {
        match &self.backend {
            Backend::Postgres(pool) => pool,
            _ => panic!("requires owned PostgreSQL fixture"),
        }
    }

    /// Manual compatibility artifact must be installed; never auto-upgrade it.
    pub(crate) async fn controlled_ready(&self) -> Result<()> {
        if let Backend::Postgres(pool) = &self.backend {
            sqlx::query("SELECT controls_version FROM siglake_query_jobs LIMIT 0")
                .execute(pool)
                .await?;
            sqlx::query(
                "SELECT context_json,execution_json FROM siglake_query_job_controls LIMIT 0",
            )
            .execute(pool)
            .await?;
            let n:i64=sqlx::query_scalar("SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgenabled='O' AND ((tgrelid='public.siglake_query_jobs'::regclass AND tgname IN ('siglake_controlled_job_version','siglake_controlled_job_start')) OR (tgrelid='public.siglake_query_job_controls'::regclass AND tgname='siglake_controlled_context_immutable'))").fetch_one(pool).await?;
            anyhow::ensure!(n == 3, "controlled job compatibility fence missing");
        }
        Ok(())
    }
    pub(crate) async fn submit_controlled(
        &self,
        id: JobId,
        query: String,
        tenant: Option<String>,
        history_execution_id: Option<Uuid>,
        mut context: JobContext,
    ) -> Result<(JobContext, tokio::time::Instant)> {
        context.validate(&self.owner)?;
        let anchor;
        match &self.backend {
            Backend::InMemory(map) => {
                let mut map = map.write().await;
                anchor = tokio::time::Instant::now();
                context.accept_now()?;
                map.insert(
                    id,
                    InMemoryRow {
                        info: JobInfo {
                            tenant,
                            job_id: id.to_string(),
                            status: JobStatus::Pending,
                            priority: Priority::Batch,
                            submitted_at: context.controls.accepted_at,
                            started_at: None,
                            ended_at: None,
                            query,
                            cost: None,
                            error: None,
                            controls: Some(context.controls.clone()),
                        },
                        owner: self.owner.clone(),
                        history_execution_id,
                        controls: Some(context.clone()),
                        result: None,
                        expires_at: None,
                    },
                );
            }
            Backend::Postgres(pool) => {
                let mut tx = pool.begin().await?;
                sqlx::query("INSERT INTO siglake_query_jobs (job_id,status,priority,submitted_at,query,tenant,owner,history_execution_id,controls_version) VALUES ($1,'pending','batch',$2,$3,$4,$5,$6,1)")
                    .bind(id.to_string()).bind(context.controls.accepted_at).bind(query).bind(tenant).bind(&self.owner).bind(history_execution_id).execute(&mut *tx).await?;
                // Exclude pool/INSERT lock waits from queue+run. Final staging
                // is a conservative pre-COMMIT anchor, not a PG commit timestamp.
                anchor = tokio::time::Instant::now();
                context.accept_now()?;
                sqlx::query("UPDATE siglake_query_jobs SET submitted_at=$2 WHERE job_id=$1")
                    .bind(id.to_string())
                    .bind(context.controls.accepted_at)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query(
                    "INSERT INTO siglake_query_job_controls(job_id,context_json) VALUES ($1,$2)",
                )
                .bind(id.to_string())
                .bind(serde_json::to_value(&context)?)
                .execute(&mut *tx)
                .await?;
                #[cfg(test)]
                self.at_commit_hook(id, false).await?;
                tx.commit().await?;
                #[cfg(test)]
                self.at_commit_hook(id, true).await?;
            }
        }
        Ok((context, anchor))
    }
    /// Always read the durable context, never use a cached/missing row as inherit.
    pub(crate) async fn controlled_context(&self, id: JobId) -> Result<JobContext> {
        let context = match &self.backend {
            Backend::InMemory(map) => {
                let map = map.read().await;
                let row = map.get(&id).context("controlled job missing")?;
                anyhow::ensure!(row.owner == self.owner, "controlled owner mismatch");
                row.controls.clone().context("controlled context missing")?
            }
            Backend::Postgres(pool) => {
                let row=sqlx::query("SELECT c.context_json FROM siglake_query_jobs j JOIN siglake_query_job_controls c USING(job_id) WHERE j.job_id=$1 AND j.owner=$2 AND j.controls_version=1")
                    .bind(id.to_string()).bind(&self.owner).fetch_one(pool).await?;
                serde_json::from_value(row.try_get("context_json")?)?
            }
        };
        context.validate(&self.owner)?;
        Ok(context)
    }
    /// Validated execution snapshot and original expiry must be supplied by the
    /// owner after its fresh primary read, on the same guarded transaction.
    pub(crate) async fn start_controlled(
        &self,
        id: JobId,
        expected: &JobContext,
        execution: &JobSnapshot,
        expires: DateTime<Utc>,
    ) -> Result<CompletionOutcome> {
        expected.validate(&self.owner)?;
        anyhow::ensure!(
            expires <= expected.controls.original_expires_at && Utc::now() < expires,
            "controlled job expired"
        );
        match &self.backend {
            Backend::InMemory(map) => {
                let mut map = map.write().await;
                let row = map.get_mut(&id).context("controlled job missing")?;
                anyhow::ensure!(
                    row.owner == self.owner
                        && serde_json::to_value(&row.controls)? == serde_json::to_value(expected)?,
                    "controlled context mismatch"
                );
                if row.info.status.is_terminal() {
                    return Ok(CompletionOutcome::Superseded {
                        status: row.info.status,
                        owned_by_us: true,
                        recovered: false,
                    });
                }
                anyhow::ensure!(Utc::now() < expires, "controlled job expired");
                row.info.status = JobStatus::Running;
                row.info.started_at.get_or_insert_with(Utc::now);
                row.info
                    .controls
                    .as_mut()
                    .context("controlled context missing")?
                    .execution = Some(execution.clone());
                Ok(CompletionOutcome::Applied {
                    status: JobStatus::Running,
                })
            }
            Backend::Postgres(pool) => {
                let mut tx = pool.begin().await?;
                let row=sqlx::query("SELECT c.context_json FROM siglake_query_jobs j JOIN siglake_query_job_controls c USING(job_id) WHERE j.job_id=$1 AND j.owner=$2 AND j.controls_version=1 FOR UPDATE OF j,c")
                    .bind(id.to_string()).bind(&self.owner).fetch_one(&mut *tx).await?;
                let stored: serde_json::Value = row.try_get("context_json")?;
                anyhow::ensure!(
                    stored == serde_json::to_value(expected)?,
                    "controlled context mismatch"
                );
                sqlx::query("SELECT set_config('siglake.controlled_start',$1,true)")
                    .bind(format!("{id}|{}|1", self.owner))
                    .execute(&mut *tx)
                    .await?;
                let affected=sqlx::query("UPDATE siglake_query_jobs SET status='running',started_at=COALESCE(started_at,clock_timestamp()) WHERE job_id=$1 AND owner=$2 AND controls_version=1 AND status IN ('pending','running') AND clock_timestamp()<$3")
                    .bind(id.to_string()).bind(&self.owner).bind(expires).execute(&mut *tx).await?.rows_affected();
                if affected == 0 {
                    tx.rollback().await?;
                    return lost_conditional_write(pool, id, &self.owner, false).await;
                }
                sqlx::query(
                    "UPDATE siglake_query_job_controls SET execution_json=$2 WHERE job_id=$1",
                )
                .bind(id.to_string())
                .bind(serde_json::to_value(execution)?)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                Ok(CompletionOutcome::Applied {
                    status: JobStatus::Running,
                })
            }
        }
    }
}
/// Do not JSON-project the whole job row to discover an optional column: that
/// would materialize its result body on every status/ownership lookup.
pub(super) async fn optional_public_controls(
    pool: &PgPool,
    id: JobId,
) -> Result<Option<crate::query_controls::jobs::JobControls>> {
    let marker_exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.siglake_query_jobs'::regclass AND attname='controls_version' AND NOT attisdropped)").fetch_one(pool).await?;
    if !marker_exists {
        return Ok(None);
    }
    let version: Option<i16> =
        sqlx::query_scalar("SELECT controls_version FROM siglake_query_jobs WHERE job_id=$1")
            .bind(id.to_string())
            .fetch_optional(pool)
            .await?
            .flatten();
    match version {
        None => Ok(None),
        Some(1) => Ok(Some(public_controls(pool, id).await?)),
        _ => anyhow::bail!("unsupported controlled job version"),
    }
}
pub(super) async fn public_controls(
    pool: &PgPool,
    id: JobId,
) -> Result<crate::query_controls::jobs::JobControls> {
    let row = sqlx::query(
        "SELECT context_json,execution_json FROM siglake_query_job_controls WHERE job_id=$1",
    )
    .bind(id.to_string())
    .fetch_one(pool)
    .await?;
    let context: JobContext = serde_json::from_value(row.try_get("context_json")?)?;
    // Validate strict schema before exposing only the bounded public projection.
    context.validate(&context.owner)?;
    let mut public = context.controls;
    let execution: Option<serde_json::Value> = row.try_get("execution_json")?;
    public.execution = execution.map(serde_json::from_value).transpose()?;
    if let Some(execution) = &public.execution {
        execution.validate()?;
        anyhow::ensure!(
            execution.effective_timeout_ms <= public.accepted.effective_timeout_ms,
            "invalid execution snapshot"
        );
    }
    Ok(public)
}

/// SQL/body-free ledger; absence is NOT rollback proof for an uncertain commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CommitState {
    Reserved,
    Writing,
    Uncertain,
}
pub(crate) struct CommitReservation {
    id: JobId,
    ledger: Arc<Mutex<HashMap<JobId, CommitState>>>,
}
impl CommitReservation {
    pub(crate) fn id(&self) -> JobId {
        self.id
    }
    pub(crate) fn writing(&self) {
        self.ledger
            .lock()
            .unwrap()
            .insert(self.id, CommitState::Writing);
    }
    pub(crate) fn accepted(self) {
        self.ledger.lock().unwrap().remove(&self.id);
    }
}
impl Drop for CommitReservation {
    fn drop(&mut self) {
        let mut ledger = self.ledger.lock().unwrap();
        match ledger.get(&self.id) {
            Some(CommitState::Writing) => {
                ledger.insert(self.id, CommitState::Uncertain);
            }
            Some(CommitState::Reserved) => {
                ledger.remove(&self.id);
            }
            _ => {}
        }
    }
}
impl JobStore {
    pub(crate) fn reserve_controlled_commit(&self) -> Result<CommitReservation> {
        let mut ledger = self.uncertain_commits.lock().unwrap();
        anyhow::ensure!(
            ledger.len() < MAX_FINISHED_UNPERSISTED,
            "controlled handoff ledger full"
        );
        let id = JobId::new();
        ledger.insert(id, CommitState::Reserved);
        Ok(CommitReservation {
            id,
            ledger: self.uncertain_commits.clone(),
        })
    }
}
pub(super) async fn reconcile_uncertain(
    backend: &Backend,
    owner: &str,
    ttl: Duration,
    ledger: &Arc<Mutex<HashMap<JobId, CommitState>>>,
    history: &JobHistory,
) -> ReconcileOutcome {
    let ids: Vec<_> = ledger
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(id, s)| (*s == CommitState::Uncertain).then_some(*id))
        .collect();
    let mut outcome = ReconcileOutcome::default();
    for id in ids {
        let result=tokio::time::timeout(RECONCILE_WRITE_TIMEOUT,async {
            match backend {
                Backend::InMemory(map)=>{
                    let mut map=map.write().await;let Some(row)=map.get_mut(&id)else{return Ok(None)};
                    if row.owner!=owner{return Ok(None)}
                    if row.info.status.is_terminal(){return Ok(Some(CompletionOutcome::Superseded{status:row.info.status,owned_by_us:true,recovered:false}))}
                    row.info.status=JobStatus::Failed;row.info.ended_at=Some(Utc::now());row.info.error=Some("query_job_handoff_uncertain".into());row.expires_at=Some(Instant::now()+ttl);
                    Ok(Some(CompletionOutcome::Applied{status:JobStatus::Failed}))
                }
                Backend::Postgres(pool)=>{
                    let expires=Utc::now()+chrono::Duration::from_std(ttl).unwrap_or_default();
                    let affected=sqlx::query("UPDATE siglake_query_jobs SET status='failed',ended_at=NOW(),error='query_job_handoff_uncertain',expires_at=$3 WHERE job_id=$1 AND owner=$2 AND controls_version=1 AND status IN ('pending','running')").bind(id.to_string()).bind(owner).bind(expires).execute(pool).await?.rows_affected();
                    if affected>0{return Ok(Some(CompletionOutcome::Applied{status:JobStatus::Failed}))}
                    let row=sqlx::query("SELECT status FROM siglake_query_jobs WHERE job_id=$1 AND owner=$2 AND controls_version=1").bind(id.to_string()).bind(owner).fetch_optional(pool).await?;
                    Ok(row.and_then(|r|r.try_get::<String,_>("status").ok()).and_then(|s|JobStatus::parse(&s)).filter(JobStatus::is_terminal).map(|status|CompletionOutcome::Superseded{status,owned_by_us:true,recovered:false}))
                }
            }
        }).await;
        match result {
            Ok(Ok::<_, anyhow::Error>(Some(terminal))) => {
                ledger.lock().unwrap().remove(&id);
                history.observe(backend, id, terminal).await;
                if matches!(terminal, CompletionOutcome::Applied { .. }) {
                    outcome.installed += 1
                } else {
                    outcome.preserved += 1
                }
            }
            _ => outcome.retained += 1,
        }
    }
    outcome
}

#[cfg(test)]
pub(crate) struct CommitHook {
    pub(crate) after: bool,
    pub(crate) reached: tokio::sync::oneshot::Sender<JobId>,
    pub(crate) release: tokio::sync::oneshot::Receiver<bool>,
}
#[cfg(test)]
impl JobStore {
    pub(crate) fn controlled_commit_hook(&self, hook: CommitHook) {
        *self.commit_hook.lock().unwrap() = Some(hook);
    }
    async fn at_commit_hook(&self, id: JobId, after: bool) -> Result<()> {
        let hook = {
            let mut h = self.commit_hook.lock().unwrap();
            if h.as_ref().is_some_and(|h| h.after == after) {
                h.take()
            } else {
                None
            }
        };
        if let Some(hook) = hook {
            let _ = hook.reached.send(id);
            if hook.release.await.unwrap_or(true) {
                anyhow::bail!("injected controlled commit acknowledgement loss")
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn controlled_commit_ledger_reserves_before_write_retains_absence_and_refuses_capacity() {
        let store = JobStore::new(1, Duration::from_secs(60));
        let mut reservations = Vec::new();
        for _ in 0..MAX_FINISHED_UNPERSISTED {
            reservations.push(store.reserve_controlled_commit().unwrap());
        }
        assert!(store.reserve_controlled_commit().is_err());
        assert_eq!(store.active_count().await, 0);
        let r = reservations.pop().unwrap();
        let id = r.id();
        r.writing();
        drop(r);
        drop(reservations);
        for _ in 0..4 {
            assert_eq!(store.reconcile_finished_jobs().await.retained, 1);
        }
        assert_eq!(
            store.uncertain_commits.lock().unwrap().get(&id).copied(),
            Some(CommitState::Uncertain)
        );
        let context = JobContext::new(
            store.owner_id(),
            &crate::auth::CallerIdentity::default(),
            crate::query_controls::runtime::test_controls(Duration::from_secs(1)),
        )
        .unwrap();
        store
            .submit_controlled(id, "SELECT 1".into(), None, None, context)
            .await
            .unwrap();
        assert_eq!(store.reconcile_finished_jobs().await.installed, 1);
        assert_eq!(
            store.info(id).await.unwrap().unwrap().status,
            JobStatus::Failed
        );
        assert!(store.uncertain_commits.lock().unwrap().is_empty());
        store.shutdown().await.unwrap();
    }
}
