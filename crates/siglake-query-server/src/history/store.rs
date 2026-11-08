mod owned;
mod runtime;
use super::*;
use owned::{OwnedPool, WriterPool};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgRow},
    Connection, PgPool, Row,
};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteResult {
    AppliedOrIdentical,
}

/// No runtime migrations, scheduling, retries or auth decisions are performed.
pub struct HistoryWriter {
    pool: WriterPool,
}
pub struct HistoryReader {
    pool: PgPool,
}

/// Read-only safe DB projection, not an event constructor or arbitrary JSON bag.
#[derive(Serialize)]
pub struct StoredExecution {
    execution_id: Uuid,
    #[serde(flatten)]
    principal: Principal,
    received_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    owner_uuid: Uuid,
    admitted_at: Option<DateTime<Utc>>,
    admission_observed_at: Option<DateTime<Utc>>,
    finished_observed_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    observation_state: ObservationState,
    execution_admitted: Option<bool>,
    #[serde(flatten)]
    request: RequestMetadata,
    effective_format: Option<OutputFormat>,
    effective_dry_run: Option<bool>,
    outcome: Option<Outcome>,
    error_code: Option<ErrorCode>,
    redacted_sql: Option<String>,
    redaction_version: i16,
    redaction_status: redaction::RedactionStatus,
    #[serde(flatten)]
    counters: Counters,
    duration_ms: Option<Counter>,
    cache_state: CacheState,
    truncated: Option<bool>,
    job_id: Option<Uuid>,
}
impl StoredExecution {
    pub fn execution_id(&self) -> Uuid {
        self.execution_id
    }
}
fn count(row: &PgRow, name: &str) -> Result<Option<Counter>, HistoryError> {
    let count: Option<i64> = row.try_get(name).map_err(unavailable)?;
    count
        .map(|value| {
            u64::try_from(value)
                .map_err(|_| HistoryError::Unavailable)
                .and_then(Counter::new)
        })
        .transpose()
}
fn stored(row: PgRow) -> Result<StoredExecution, HistoryError> {
    Ok(StoredExecution {
        execution_id: row.try_get("execution_id").map_err(unavailable)?,
        principal: Principal {
            tenant: row.try_get("tenant").map_err(unavailable)?,
            issuer: row.try_get("issuer").map_err(unavailable)?,
            client_id: row.try_get("client_id").map_err(unavailable)?,
            principal_kind: PrincipalKind::parse(
                row.try_get("principal_kind").map_err(unavailable)?,
            )?,
        },
        received_at: row.try_get("received_at").map_err(unavailable)?,
        expires_at: row.try_get("expires_at").map_err(unavailable)?,
        owner_uuid: row.try_get("owner_uuid").map_err(unavailable)?,
        admitted_at: row.try_get("admitted_at").map_err(unavailable)?,
        admission_observed_at: row.try_get("admission_observed_at").map_err(unavailable)?,
        finished_observed_at: row.try_get("finished_observed_at").map_err(unavailable)?,
        finished_at: row.try_get("finished_at").map_err(unavailable)?,
        observation_state: ObservationState::parse(
            row.try_get("observation_state").map_err(unavailable)?,
        )?,
        execution_admitted: row.try_get("execution_admitted").map_err(unavailable)?,
        request: RequestMetadata {
            operation: Operation::parse(row.try_get("operation").map_err(unavailable)?)?,
            format: OutputFormat::parse(row.try_get("format").map_err(unavailable)?)?,
            priority: Priority::parse(row.try_get("priority").map_err(unavailable)?)?,
            dry_run: row.try_get("dry_run").map_err(unavailable)?,
        },
        effective_format: match row.try_get::<Option<&str>, _>("effective_format") {
            Ok(value) => value.map(OutputFormat::parse).transpose()?,
            Err(sqlx::Error::ColumnNotFound(_)) => None, // v1/v2 compatibility
            Err(error) => return Err(unavailable(error)),
        },
        effective_dry_run: match row.try_get("effective_dry_run") {
            Ok(value) => value,
            Err(sqlx::Error::ColumnNotFound(_)) => None,
            Err(error) => return Err(unavailable(error)),
        },
        outcome: row
            .try_get::<Option<&str>, _>("outcome")
            .map_err(unavailable)?
            .map(Outcome::parse)
            .transpose()?,
        error_code: row
            .try_get::<Option<&str>, _>("error_code")
            .map_err(unavailable)?
            .map(ErrorCode::parse)
            .transpose()?,
        redacted_sql: row.try_get("redacted_sql").map_err(unavailable)?,
        redaction_version: row.try_get("redaction_version").map_err(unavailable)?,
        redaction_status: redaction::RedactionStatus::parse(
            row.try_get("redaction_status").map_err(unavailable)?,
        )?,
        counters: Counters {
            estimated_bytes_scanned: count(&row, "estimated_bytes_scanned")?,
            estimated_rows_processed: count(&row, "estimated_rows_processed")?,
            measured_bytes_scanned: count(&row, "measured_bytes_scanned")?,
            measured_rows_scanned: count(&row, "measured_rows_scanned")?,
            returned_rows: count(&row, "returned_rows")?,
        },
        duration_ms: count(&row, "duration_ms")?,
        cache_state: CacheState::parse(row.try_get("cache_state").map_err(unavailable)?)?,
        truncated: row.try_get("truncated").map_err(unavailable)?,
        job_id: row.try_get("job_id").map_err(unavailable)?,
    })
}
fn unavailable(_: sqlx::Error) -> HistoryError {
    HistoryError::Unavailable
}

// Deliberately stricter than "role is not superuser": fixed logins may have NO
// memberships (including NOINHERIT/SET ROLE), own nothing, or reach other app
// tables/functions. Provisioning must also revoke PUBLIC CREATE/TEMP/EXECUTE.
async fn check_role(conn: &mut sqlx::PgConnection, expected: &str) -> Result<bool, sqlx::Error> {
    let safe: bool = sqlx::query_scalar(r#"
      SELECT current_user = $1 AND session_user = $1
       AND NOT (r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication)
       AND (SELECT count(*)=2 AND bool_and(c.relrowsecurity AND c.relforcerowsecurity)
         FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
         WHERE n.nspname='query_history' AND c.relname IN ('executions','coverage') AND c.relkind='r')
       AND NOT EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member = r.oid)
       AND NOT EXISTS (SELECT 1 FROM pg_shdepend d WHERE d.refclassid='pg_authid'::regclass AND d.refobjid=r.oid AND d.deptype='o')
       AND NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspowner = r.oid)
       AND NOT has_database_privilege(current_user, current_database(), 'CREATE,TEMP')
       AND NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE left(n.nspname,3) <> 'pg_' AND n.nspname <> 'information_schema' AND has_schema_privilege(current_user,n.oid,'CREATE'))
       AND NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
         WHERE left(n.nspname,3) <> 'pg_' AND n.nspname <> 'information_schema' AND c.relkind IN ('r','p','v','m','S','f')
          AND (n.nspname <> 'query_history' OR ($1='qh_reader' AND c.relname NOT IN ('executions','coverage')))
          AND (has_table_privilege(current_user,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
           OR has_any_column_privilege(current_user,c.oid,'SELECT,INSERT,UPDATE,REFERENCES')))
       AND NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
         WHERE left(n.nspname,3) <> 'pg_' AND n.nspname <> 'information_schema' AND has_function_privilege(current_user,p.oid,'EXECUTE'))
       AND ($1 <> 'qh_reader' OR NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
         WHERE n.nspname='query_history' AND
          (has_table_privilege(current_user,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
           OR has_any_column_privilege(current_user,c.oid,'INSERT,UPDATE,REFERENCES'))))
      FROM pg_roles r WHERE r.rolname=current_user
    "#).bind(expected).fetch_one(&mut *conn).await?;
    Ok(safe)
}
// Fail closed on an absent, unvalidated or altered v3 contract. These fixed
// definitions are from the separately applied owner migration, never runtime DDL.
async fn check_v3(conn: &mut sqlx::PgConnection) -> Result<bool, sqlx::Error> {
    const CONSTRAINT: &str = r#"CHECK ((((effective_format IS NULL) AND (effective_dry_run IS NULL)) OR ((effective_format IS NOT NULL) AND (effective_dry_run IS NOT NULL) AND (((operation = 'sql'::text) AND ((effective_format = 'records'::text) OR ((effective_format = 'ndjson'::text) AND (NOT effective_dry_run)))) OR ((operation = 'explain'::text) AND (effective_format = 'records'::text) AND effective_dry_run) OR ((operation = 'shard'::text) AND (effective_format = 'arrow'::text) AND (NOT effective_dry_run))))))"#;
    let migration = include_str!("schema-v3-effective.sql");
    let function = migration
        .split_once("AS $$")
        .expect("fixed migration function")
        .1
        .split_once("$$;")
        .expect("fixed migration function end")
        .0;
    let safe: bool = sqlx::query_scalar(
        r#"
      SELECT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class r ON r.oid=c.conrelid
        JOIN pg_namespace n ON n.oid=r.relnamespace
        JOIN pg_roles owner ON owner.oid=r.relowner
        WHERE n.nspname='query_history' AND r.relname='executions'
          AND owner.rolname='qh_owner' AND n.nspowner=owner.oid
          AND c.conname='executions_effective_v3' AND c.contype='c'
          AND c.convalidated AND NOT c.connoinherit AND pg_get_constraintdef(c.oid)=$1
      ) AND (SELECT count(*)=2 FROM pg_attribute a
        WHERE a.attrelid='query_history.executions'::regclass AND NOT a.attisdropped
          AND NOT a.attnotnull AND a.attgenerated=''
          AND ((a.attname='effective_format' AND a.atttypid='text'::regtype)
            OR (a.attname='effective_dry_run' AND a.atttypid='boolean'::regtype)))
      AND EXISTS (
        SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
        JOIN pg_namespace n ON n.oid=p.pronamespace
        JOIN pg_roles owner ON owner.oid=p.proowner
        JOIN pg_language l ON l.oid=p.prolang
        WHERE t.tgrelid='query_history.executions'::regclass
          AND t.tgname='immutable_effective_v3' AND t.tgtype=19
          AND t.tgenabled='O' AND NOT t.tgisinternal AND t.tgqual IS NULL
          AND t.tgnargs=0 AND t.tgattr=''::int2vector
          AND n.nspname='query_history' AND p.proname='immutable_effective_v3'
          AND owner.rolname='qh_owner' AND l.lanname='plpgsql'
          AND NOT p.prosecdef AND p.prosrc=$2
          AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]
      )
    "#,
    )
    .bind(CONSTRAINT)
    .bind(function)
    .fetch_one(conn)
    .await?;
    Ok(safe)
}
// Only a completed false contract is rejection. Transport/protocol/query-cancel
// failures while executing either check are operational unavailability.
async fn verify_collector(conn: &mut sqlx::PgConnection) -> Result<(), HistoryError> {
    if !check_role(conn, "qh_writer").await.map_err(unavailable)?
        || !check_v3(conn).await.map_err(unavailable)?
    {
        return Err(HistoryError::Privilege);
    }
    Ok(())
}
// Legacy injected writer/reader path: preserve its original SQLx pool behavior.
async fn pool(options: PgConnectOptions, expected: &'static str) -> Result<PgPool, HistoryError> {
    let mut preflight = sqlx::PgConnection::connect_with(&options)
        .await
        .map_err(unavailable)?;
    if !check_role(&mut preflight, expected)
        .await
        .map_err(|_| HistoryError::Privilege)?
    {
        return Err(HistoryError::Privilege);
    }
    drop(preflight);
    PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(2))
        .after_connect(move |conn, _| {
            Box::pin(async move {
                if !check_role(conn, expected).await? {
                    return Err(sqlx::Error::Protocol(
                        "history privilege contract rejected".into(),
                    ));
                }
                Ok(())
            })
        })
        .connect_with(options)
        .await
        .map_err(unavailable)
}

impl HistoryWriter {
    pub(super) fn close_guard(&self) -> impl Drop {
        self.pool.close_guard()
    }
    pub(super) fn close_owned(&self) {
        self.pool.close_owned();
    }
    /// Dedicated standalone path: every fresh connection checks v3 as well as
    /// the fixed writer privilege contract. No DDL or downgraded schema path.
    pub(super) async fn connect_collector(options: PgConnectOptions) -> Result<Self, HistoryError> {
        Ok(Self {
            pool: WriterPool::Owned(OwnedPool::connect(options).await?),
        })
    }

    pub async fn connect(options: PgConnectOptions) -> Result<Self, HistoryError> {
        Ok(Self {
            pool: WriterPool::Legacy(pool(options, "qh_writer").await?),
        })
    }

    /// Internal association, conditional on the original row and no conflicting
    /// job link. Existing schema/role contract already permits these columns.
    pub(super) async fn associate_job(
        &self,
        execution: Uuid,
        job: Uuid,
    ) -> Result<(), HistoryError> {
        let changed = sqlx::query("UPDATE query_history.executions SET job_id=$2 WHERE execution_id=$1 AND expires_at>clock_timestamp() AND observation_state<>'terminal' AND (job_id IS NULL OR job_id=$2) AND operation='sql' AND priority='batch' AND NOT dry_run")
            .bind(execution).bind(job).execute(&self.pool).await.map_err(unavailable)?.rows_affected();
        self.classify(execution, changed).await?;
        Ok(())
    }

    pub(super) async fn resume_job(
        &self,
        execution: Uuid,
        job: Uuid,
        tenant: &str,
    ) -> Result<Option<(PendingExecution, Option<bool>)>, HistoryError> {
        let row = sqlx::query("SELECT * FROM query_history.executions WHERE execution_id=$1 AND job_id=$2 AND tenant=$3 AND operation='sql' AND priority='batch' AND format='records' AND NOT dry_run AND observation_state<>'terminal' AND expires_at>clock_timestamp()")
            .bind(execution).bind(job).bind(tenant).fetch_optional(&self.pool).await.map_err(unavailable)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let original = stored(row)?;
        // Do not deserialize job payloads or raw SQL. Only this original safe
        // row can supply the immutable receipt, attribution and request class.
        if original.execution_id.is_nil()
            || original.owner_uuid.is_nil()
            || original.expires_at - original.received_at != chrono::Duration::hours(720)
            || original.redacted_sql.is_some()
            || original.redaction_version != 1
        {
            return Err(HistoryError::InvalidValue);
        }
        let event = PendingExecution::new(
            Receipt {
                execution_id: original.execution_id,
                received_at: original.received_at,
                expires_at: original.expires_at,
            },
            original.principal,
            original.owner_uuid,
            original.request,
            RedactedSql::restore_omitted(original.redaction_status),
        )?;
        Ok(Some((event, original.execution_admitted)))
    }

    /// Allocates a fresh UUID and trusted DB receipt BEFORE building the event.
    /// No caller-provided UUID/timestamp re-admission API exists.
    pub async fn receipt(&self) -> Result<Receipt, HistoryError> {
        let row = sqlx::query("WITH t AS MATERIALIZED (SELECT clock_timestamp() AS now) SELECT now, now + interval '720 hours' AS expiry FROM t")
            .fetch_one(&self.pool).await.map_err(unavailable)?;
        Ok(Receipt {
            execution_id: Uuid::new_v4(),
            received_at: row.try_get("now").map_err(unavailable)?,
            expires_at: row.try_get("expiry").map_err(unavailable)?,
        })
    }
    pub async fn persist_pending(
        &self,
        event: &PendingExecution,
    ) -> Result<WriteResult, HistoryError> {
        let json = serde_json::to_value(event).map_err(|_| HistoryError::InvalidValue)?;
        let changed = sqlx::query(r#"
        INSERT INTO query_history.executions (execution_id,tenant,issuer,client_id,principal_kind,received_at,expires_at,owner_uuid,operation,format,priority,dry_run,redacted_sql,redaction_version,redaction_status)
        SELECT ($1::jsonb->>'execution_id')::uuid,$1::jsonb->>'tenant',$1::jsonb->>'issuer',$1::jsonb->>'client_id',$1::jsonb->>'principal_kind',($1::jsonb->>'received_at')::timestamptz,($1::jsonb->>'expires_at')::timestamptz,($1::jsonb->>'owner_uuid')::uuid,$1::jsonb->>'operation',$1::jsonb->>'format',$1::jsonb->>'priority',($1::jsonb->>'dry_run')::boolean,$1::jsonb->'redaction'->>'sql',($1::jsonb->'redaction'->>'version')::smallint,$1::jsonb->'redaction'->>'status'
        WHERE ($1::jsonb->>'expires_at')::timestamptz > clock_timestamp()
        ON CONFLICT (execution_id) DO UPDATE SET execution_id=EXCLUDED.execution_id
        WHERE ROW(executions.tenant,executions.issuer,executions.client_id,executions.principal_kind,executions.received_at,executions.expires_at,executions.owner_uuid,executions.operation,executions.format,executions.priority,executions.dry_run,executions.redacted_sql,executions.redaction_version,executions.redaction_status)
        IS NOT DISTINCT FROM ROW(EXCLUDED.tenant,EXCLUDED.issuer,EXCLUDED.client_id,EXCLUDED.principal_kind,EXCLUDED.received_at,EXCLUDED.expires_at,EXCLUDED.owner_uuid,EXCLUDED.operation,EXCLUDED.format,EXCLUDED.priority,EXCLUDED.dry_run,EXCLUDED.redacted_sql,EXCLUDED.redaction_version,EXCLUDED.redaction_status)
        AND executions.expires_at > clock_timestamp()
        "#).bind(json).execute(&self.pool).await.map_err(unavailable)?.rows_affected();
        self.classify(event.receipt.execution_id, changed).await
    }
    async fn classify(&self, id: Uuid, changed: u64) -> Result<WriteResult, HistoryError> {
        if changed > 0 {
            return Ok(WriteResult::AppliedOrIdentical);
        }
        let alive: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM query_history.executions WHERE execution_id=$1 AND expires_at > clock_timestamp())")
            .bind(id).fetch_one(&self.pool).await.map_err(unavailable)?;
        Err(if alive {
            HistoryError::Conflict
        } else {
            HistoryError::ExpiredOrAbsent
        })
    }
    /// Record trusted lifecycle admission evidence. Timestamp is persistence
    /// observation time, NOT the original admission event time.
    pub async fn admit(&self, receipt: &Receipt) -> Result<WriteResult, HistoryError> {
        let changed = sqlx::query("UPDATE query_history.executions SET execution_admitted=true, admission_observed_at=COALESCE(admission_observed_at,clock_timestamp()) WHERE execution_id=$1 AND expires_at>clock_timestamp() AND observation_state<>'terminal'")
            .bind(receipt.execution_id).execute(&self.pool).await.map_err(unavailable)?.rows_affected();
        self.classify(receipt.execution_id, changed).await
    }
    /// v3 only. A missing migration loses telemetry, never changes execution.
    pub(crate) async fn select_effective(
        &self,
        receipt: &Receipt,
        effective: EffectiveExecution,
    ) -> Result<WriteResult, HistoryError> {
        let format =
            serde_json::to_value(effective.format).map_err(|_| HistoryError::InvalidValue)?;
        let changed = sqlx::query("UPDATE query_history.executions SET effective_format=$2, effective_dry_run=$3 WHERE execution_id=$1 AND expires_at>clock_timestamp() AND ((effective_format IS NULL AND observation_state<>'terminal') OR (effective_format=$2 AND effective_dry_run=$3))")
            .bind(receipt.execution_id).bind(format.as_str()).bind(effective.dry_run)
            .execute(&self.pool).await.map_err(unavailable)?.rows_affected();
        self.classify(receipt.execution_id, changed).await
    }
    /// Reconcile indeterminate with genuine terminal evidence; never overwrite a
    /// different terminal. Admission is not inferred from an HTTP status.
    pub async fn finish(
        &self,
        receipt: &Receipt,
        terminal: &TerminalExecution,
    ) -> Result<WriteResult, HistoryError> {
        let json = serde_json::to_value(terminal).map_err(|_| HistoryError::InvalidValue)?;
        let changed = sqlx::query(r#"
          UPDATE query_history.executions SET
            observation_state='terminal',finished_observed_at=COALESCE(finished_observed_at,clock_timestamp()),
            execution_admitted=($2::jsonb->>'admission_evidence'='admitted'),
            admission_observed_at=CASE WHEN $2::jsonb->>'admission_evidence'='admitted' THEN COALESCE(admission_observed_at,statement_timestamp()) ELSE NULL END,
            outcome=$2::jsonb->>'outcome',error_code=$2::jsonb->>'error_code',duration_ms=($2::jsonb->>'duration_ms')::bigint,
            cache_state=$2::jsonb->>'cache_state',truncated=($2::jsonb->>'truncated')::boolean,job_id=($2::jsonb->>'job_id')::uuid,
            estimated_bytes_scanned=($2::jsonb->>'estimated_bytes_scanned')::bigint,estimated_rows_processed=($2::jsonb->>'estimated_rows_processed')::bigint,
            measured_bytes_scanned=($2::jsonb->>'measured_bytes_scanned')::bigint,measured_rows_scanned=($2::jsonb->>'measured_rows_scanned')::bigint,returned_rows=($2::jsonb->>'returned_rows')::bigint
          WHERE execution_id=$1 AND expires_at>clock_timestamp()
           AND (execution_admitted IS NULL OR execution_admitted=($2::jsonb->>'admission_evidence'='admitted'))
           AND (observation_state<>'terminal' OR
            jsonb_build_object('admission_evidence',CASE WHEN execution_admitted THEN 'admitted' ELSE 'not_admitted' END,'outcome',outcome,'error_code',error_code,'duration_ms',duration_ms,'cache_state',cache_state,'truncated',truncated,'job_id',job_id,
             'estimated_bytes_scanned',estimated_bytes_scanned,'estimated_rows_processed',estimated_rows_processed,'measured_bytes_scanned',measured_bytes_scanned,'measured_rows_scanned',measured_rows_scanned,'returned_rows',returned_rows)=$2::jsonb)
        "#).bind(receipt.execution_id).bind(json).execute(&self.pool).await.map_err(unavailable)?.rows_affected();
        self.classify(receipt.execution_id, changed).await
    }
    /// Recover missing initial persistence using the ORIGINAL receipt and
    /// explicit trusted lifecycle evidence. Original event times stay unknown;
    /// only persistence observation clocks are filled. Never mint a new receipt.
    pub async fn finish_pending(
        &self,
        event: &PendingExecution,
        terminal: &TerminalExecution,
    ) -> Result<WriteResult, HistoryError> {
        self.persist_pending(event).await?;
        self.finish(&event.receipt, terminal).await
    }
    pub async fn heartbeat(&self, owner: Uuid) -> Result<(), HistoryError> {
        if owner.is_nil() {
            return Err(HistoryError::InvalidValue);
        }
        sqlx::query("INSERT INTO query_history.owners(owner_uuid,coverage_version) VALUES ($1,1) ON CONFLICT(owner_uuid) DO UPDATE SET heartbeat_at=clock_timestamp()")
            .bind(owner).execute(&self.pool).await.map_err(unavailable)?;
        Ok(())
    }
    /// Explicit recovery evidence only. Lease/scheduling decisions belong to the
    /// next lifecycle slice; this never manufactures a successful/failed outcome.
    pub async fn mark_indeterminate(&self, owner: Uuid) -> Result<u64, HistoryError> {
        Ok(sqlx::query("UPDATE query_history.executions SET observation_state='indeterminate' WHERE owner_uuid=$1 AND observation_state='pending' AND expires_at>clock_timestamp()")
            .bind(owner).execute(&self.pool).await.map_err(unavailable)?.rows_affected())
    }
    /// One bounded primary DELETE batch. SKIP LOCKED coordinates sweepers; the
    /// advisory transaction lock also bounds owner/coverage cleanup contention.
    /// No task is started automatically. Returns physically deleted executions.
    pub async fn purge_expired(&self, batch: u32) -> Result<u64, HistoryError> {
        if batch == 0 || batch > 1000 {
            return Err(HistoryError::InvalidValue);
        }
        let mut connection = self.pool.acquire().await.map_err(unavailable)?;
        let mut tx = connection.begin().await.map_err(unavailable)?;
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(724918220194)")
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
        if !locked {
            if matches!(&self.pool, WriterPool::Owned(_)) {
                tx.rollback().await.map_err(unavailable)?;
                connection.complete().await.map_err(unavailable)?;
            }
            // Legacy injection retains its original transaction-Drop rollback.
            return Ok(0);
        }
        let result = sqlx::query("DELETE FROM query_history.executions WHERE execution_id IN (SELECT execution_id FROM query_history.executions WHERE expires_at<=statement_timestamp() ORDER BY expires_at,execution_id LIMIT $1 FOR UPDATE SKIP LOCKED)")
            .bind(i64::from(batch)).execute(&mut *tx).await.map_err(unavailable)?.rows_affected();
        sqlx::query("DELETE FROM query_history.coverage WHERE gap_id IN (SELECT gap_id FROM query_history.coverage WHERE expires_at<=statement_timestamp() ORDER BY expires_at,gap_id LIMIT $1 FOR UPDATE SKIP LOCKED)")
            .bind(i64::from(batch)).execute(&mut *tx).await.map_err(unavailable)?;
        sqlx::query("DELETE FROM query_history.owners WHERE owner_uuid IN (SELECT o.owner_uuid FROM query_history.owners o WHERE heartbeat_at<=statement_timestamp()-interval '720 hours' AND NOT EXISTS (SELECT 1 FROM query_history.executions e WHERE e.owner_uuid=o.owner_uuid) ORDER BY heartbeat_at LIMIT $1 FOR UPDATE SKIP LOCKED)")
            .bind(i64::from(batch)).execute(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        connection.complete().await.map_err(unavailable)?;
        Ok(result)
    }
}

impl HistoryReader {
    pub async fn connect(options: PgConnectOptions) -> Result<Self, HistoryError> {
        Ok(Self {
            pool: pool(options, "qh_reader").await?,
        })
    }
    async fn tenant_tx(
        &self,
        tenant: &str,
    ) -> Result<sqlx::Transaction<'_, sqlx::Postgres>, HistoryError> {
        let tenant =
            siglake_core::tenant::sanitize_tenant(tenant).ok_or(HistoryError::InvalidValue)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        if !check_role(&mut tx, "qh_reader")
            .await
            .map_err(|_| HistoryError::Privilege)?
        {
            return Err(HistoryError::Privilege);
        }
        sqlx::query("SELECT set_config('query_history.tenant',$1,true)")
            .bind(tenant)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        Ok(tx)
    }
    /// Foundation read only, not the future cursor/API. RLS is defense in depth;
    /// the caller MUST authorize organization membership before choosing tenant.
    pub async fn list(
        &self,
        tenant: &str,
        limit: u32,
    ) -> Result<Vec<StoredExecution>, HistoryError> {
        if limit == 0 || limit > 100 {
            return Err(HistoryError::InvalidValue);
        }
        let mut tx = self.tenant_tx(tenant).await?;
        let rows = sqlx::query("SELECT * FROM query_history.executions WHERE tenant=$1 AND expires_at>clock_timestamp() ORDER BY received_at DESC,execution_id DESC LIMIT $2")
            .bind(tenant.trim()).bind(i64::from(limit)).fetch_all(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        rows.into_iter().map(stored).collect()
    }
    pub async fn detail(
        &self,
        tenant: &str,
        execution_id: Uuid,
    ) -> Result<Option<StoredExecution>, HistoryError> {
        let mut tx = self.tenant_tx(tenant).await?;
        let row = sqlx::query("SELECT * FROM query_history.executions WHERE tenant=$1 AND execution_id=$2 AND expires_at>clock_timestamp()")
            .bind(tenant.trim()).bind(execution_id).fetch_optional(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        row.map(stored).transpose()
    }
}
