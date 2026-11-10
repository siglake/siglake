//! Fixed runtime queries. No schema, role or reader-contract changes.
use super::*;

/// Primary-row backlog only, not physical secure-erasure evidence.
pub(in crate::history) struct CleanupBacklog {
    pub oldest_overdue_ms: Option<i64>,
}

impl HistoryWriter {
    /// Bounded lease recovery; a stale/missing heartbeat proves uncertainty,
    /// never that execution stopped. The current owner is deliberately excluded.
    pub(in crate::history) async fn recover_stale(
        &self,
        current_owner: Uuid,
    ) -> Result<u64, HistoryError> {
        Ok(sqlx::query(r#"
            UPDATE query_history.executions SET observation_state='indeterminate'
            WHERE execution_id IN (
              SELECT e.execution_id FROM query_history.executions e
              LEFT JOIN query_history.owners o ON o.owner_uuid=e.owner_uuid
              WHERE e.owner_uuid<>$1 AND e.observation_state='pending'
                AND e.expires_at>statement_timestamp()
                AND COALESCE(o.heartbeat_at,e.received_at)<statement_timestamp()-interval '120 seconds'
              ORDER BY e.received_at,e.execution_id LIMIT 1000 FOR UPDATE OF e SKIP LOCKED)
              AND observation_state='pending' AND expires_at>clock_timestamp()
        "#).bind(current_owner).execute(&self.pool).await.map_err(unavailable)?.rows_affected())
    }

    pub(in crate::history) async fn abandon_pending(
        &self,
        receipt: &Receipt,
    ) -> Result<(), HistoryError> {
        // A later genuine terminal is still allowed. A concurrent terminal wins.
        sqlx::query("UPDATE query_history.executions SET observation_state='indeterminate' WHERE execution_id=$1 AND observation_state='pending' AND expires_at>clock_timestamp()")
            .bind(receipt.execution_id).execute(&self.pool).await.map_err(unavailable)?;
        Ok(())
    }

    /// A recovery-time POINT observation of a known local loss signal. Neither
    /// outage start/end nor a crash window is inferred. UUID makes ambiguous
    /// acknowledgements idempotent; conflict never resets the original expiry.
    pub(in crate::history) async fn record_loss_observation(
        &self,
        gap_id: Uuid,
        tenant: &str,
        queue_full: bool,
    ) -> Result<(), HistoryError> {
        sqlx::query(r#"
            WITH t AS MATERIALIZED (SELECT clock_timestamp() AS now)
            INSERT INTO query_history.coverage(gap_id,tenant,received_at,expires_at,started_at,ended_at,reason)
            SELECT $1,$2,now,now+interval '720 hours',now,now,$3 FROM t
            ON CONFLICT(gap_id) DO NOTHING
        "#).bind(gap_id).bind(tenant)
            .bind(if queue_full { "queue_full" } else { "store_unavailable" })
            .execute(&self.pool).await.map_err(unavailable)?;
        Ok(())
    }

    /// Check all three purge domains, independently of a purge return count:
    /// zero may mean another replica owns the advisory lock, NOT caught up.
    pub(in crate::history) async fn cleanup_backlog(&self) -> Result<CleanupBacklog, HistoryError> {
        let oldest_overdue_ms: Option<i64> = sqlx::query_scalar(r#"
            SELECT (greatest(0,extract(epoch FROM statement_timestamp()-min(expiry)))*1000)::bigint
            FROM (
              (SELECT expires_at AS expiry FROM query_history.executions WHERE expires_at<=statement_timestamp() ORDER BY expires_at LIMIT 1)
              UNION ALL
              (SELECT expires_at FROM query_history.coverage WHERE expires_at<=statement_timestamp() ORDER BY expires_at LIMIT 1)
              UNION ALL
              (SELECT heartbeat_at+interval '720 hours' FROM query_history.owners o
               WHERE heartbeat_at<=statement_timestamp()-interval '720 hours'
                AND NOT EXISTS(SELECT 1 FROM query_history.executions e WHERE e.owner_uuid=o.owner_uuid)
               ORDER BY heartbeat_at LIMIT 1)
            ) overdue
            HAVING min(expiry) IS NOT NULL
        "#).fetch_optional(&self.pool).await.map_err(unavailable)?.flatten();
        Ok(CleanupBacklog { oldest_overdue_ms })
    }
}
