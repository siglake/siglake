//! Dedicated owned backend, not SQLx PgPool: SQLx0.8.6 pool release pings can
//! consume a late server ERROR and log its raw message in an unowned future.
//! Raw PgConnection/socket Drop has no protocol I/O/logging/task. Successful
//! completed operations reuse sockets; everything uncertain drops synchronously.
//!
//! This is NOT an arbitrary-server-message confidentiality boundary: SQLx
//! postgres connection/stream.rs NoticeResponse handling logs notice.message()
//! under sqlx::postgres::notice. The DB/admin and reviewed schema are trusted;
//! arbitrary NOTICE/INFO remains outside this local slice's guarantee.
use super::*;
use futures::{future::BoxFuture, stream::BoxStream, TryStreamExt};
use sqlx::{
    postgres::{PgQueryResult, PgStatement, PgTypeInfo},
    Describe, Either, Execute, Executor, PgConnection, Postgres,
};
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) enum WriterPool {
    Legacy(PgPool),
    Owned(Arc<OwnedPool>),
}
impl std::fmt::Debug for WriterPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HistoryWriterPool")
    }
}
struct Idle {
    closed: bool,
    connections: Vec<PgConnection>,
}
pub(super) struct OwnedPool {
    options: PgConnectOptions,
    idle: Mutex<Idle>,
    permits: Arc<Semaphore>,
}
impl OwnedPool {
    pub(super) async fn connect(options: PgConnectOptions) -> Result<Arc<Self>, HistoryError> {
        let pool = Arc::new(Self {
            options,
            idle: Mutex::new(Idle {
                closed: false,
                connections: Vec::new(),
            }),
            permits: Arc::new(Semaphore::new(4)),
        });
        // Startup budget includes this first connection/full preflight. Retain
        // that proven connection, not another pool's second connect attempt.
        let mut lease = pool.acquire().await?;
        lease.complete().await.map_err(unavailable)?;
        drop(lease);
        Ok(pool)
    }
    async fn acquire(self: &Arc<Self>) -> Result<OwnedLease, HistoryError> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| HistoryError::Unavailable)?;
        let connection = {
            let mut idle = self.idle.lock().unwrap();
            if idle.closed {
                return Err(HistoryError::Unavailable);
            }
            idle.connections.pop()
        };
        let connection = match connection {
            Some(connection) => connection,
            None => {
                let mut connection = PgConnection::connect_with(&self.options)
                    .await
                    .map_err(unavailable)?;
                if let Err(error) = verify_collector(&mut connection).await {
                    // Fixed diagnostic only; no upstream Error Display/Debug.
                    match error {
                        HistoryError::Privilege => tracing::warn!("history connection rejected"),
                        _ => tracing::warn!("history connection unavailable"),
                    }
                    return Err(error);
                }
                connection
            }
        };
        if self.idle.lock().unwrap().closed {
            return Err(HistoryError::Unavailable);
        }
        Ok(OwnedLease {
            pool: self.clone(),
            connection: Some(connection),
            reusable: false,
            _permit: permit,
        })
    }
    fn close(&self) {
        let connections = {
            let mut idle = self.idle.lock().unwrap();
            idle.closed = true;
            self.permits.close();
            std::mem::take(&mut idle.connections)
        };
        // No graceful close/ping/read, await, log or spawned return task.
        drop(connections);
    }
}
pub(super) struct CloseOwned(Option<Arc<OwnedPool>>);
impl Drop for CloseOwned {
    fn drop(&mut self) {
        if let Some(pool) = &self.0 {
            pool.close();
        }
    }
}
impl WriterPool {
    pub(super) fn close_guard(&self) -> CloseOwned {
        CloseOwned(match self {
            Self::Owned(pool) => Some(pool.clone()),
            Self::Legacy(_) => None,
        })
    }
    pub(super) fn close_owned(&self) {
        if let Self::Owned(pool) = self {
            pool.close();
        }
    }
    pub(super) async fn acquire(&self) -> Result<WriterConnection, sqlx::Error> {
        match self {
            Self::Legacy(pool) => pool.acquire().await.map(WriterConnection::Legacy),
            Self::Owned(pool) => pool
                .acquire()
                .await
                .map(WriterConnection::Owned)
                .map_err(|_| safe_error()),
        }
    }
}
pub(super) enum WriterConnection {
    Legacy(sqlx::pool::PoolConnection<Postgres>),
    Owned(OwnedLease),
}
impl WriterConnection {
    pub(super) async fn complete(&mut self) -> Result<(), sqlx::Error> {
        if let Self::Owned(lease) = self {
            lease.complete().await?;
        }
        Ok(())
    }
}
impl Deref for WriterConnection {
    type Target = PgConnection;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Legacy(c) => c,
            Self::Owned(c) => c,
        }
    }
}
impl DerefMut for WriterConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Legacy(c) => c,
            Self::Owned(c) => c,
        }
    }
}
pub(super) struct OwnedLease {
    pool: Arc<OwnedPool>,
    connection: Option<PgConnection>,
    reusable: bool,
    _permit: OwnedSemaphorePermit,
}
impl OwnedLease {
    async fn complete(&mut self) -> Result<(), sqlx::Error> {
        // flush/wait_until_ready consumes all remaining ReadyForQuery/error
        // responses INSIDE the caller's original budget and safe-error boundary.
        // Cancellation here leaves the lease non-reusable.
        self.connection
            .as_mut()
            .unwrap()
            .flush()
            .await
            .map_err(|_| safe_error())?;
        if self.connection.as_ref().unwrap().is_in_transaction() {
            return Err(safe_error());
        }
        self.reusable = true;
        Ok(())
    }
}
impl Deref for OwnedLease {
    type Target = PgConnection;
    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().unwrap()
    }
}
impl DerefMut for OwnedLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection.as_mut().unwrap()
    }
}
impl Drop for OwnedLease {
    fn drop(&mut self) {
        let mut connection = self.connection.take();
        if self.reusable {
            let mut idle = self.pool.idle.lock().unwrap();
            if !idle.closed {
                idle.connections.push(connection.take().unwrap());
            }
        }
        // Discard before releasing this checkout's permit. No detached work.
        drop(connection);
    }
}
fn safe_error() -> sqlx::Error {
    sqlx::Error::Protocol("history operation unavailable".into())
}

// Existing writer SQL keeps its concrete SQLx Executor seam. Legacy injection
// delegates unchanged; only the standalone backend owns the raw lease lifecycle.
impl<'c> Executor<'c> for &'c WriterPool {
    type Database = Postgres;
    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<PgQueryResult, PgRow>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Postgres>,
    {
        match self {
            WriterPool::Legacy(pool) => pool.fetch_many(query),
            WriterPool::Owned(pool) => Box::pin(async_stream::try_stream! {
                let mut lease = pool.acquire().await.map_err(|_| safe_error())?;
                {
                    let mut rows = (&mut *lease).fetch_many(query);
                    while let Some(row) = rows.try_next().await.map_err(|_| safe_error())? { yield row; }
                }
                lease.complete().await?;
            }),
        }
    }
    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<PgRow>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Postgres>,
    {
        match self {
            WriterPool::Legacy(pool) => pool.fetch_optional(query),
            WriterPool::Owned(pool) => Box::pin(async move {
                let mut lease = pool.acquire().await.map_err(|_| safe_error())?;
                let row = (&mut *lease)
                    .fetch_optional(query)
                    .await
                    .map_err(|_| safe_error())?;
                lease.complete().await?;
                Ok(row)
            }),
        }
    }
    fn prepare_with<'e, 'q: 'e>(
        self,
        query: &'q str,
        parameters: &'e [PgTypeInfo],
    ) -> BoxFuture<'e, Result<PgStatement<'q>, sqlx::Error>>
    where
        'c: 'e,
    {
        match self {
            WriterPool::Legacy(pool) => pool.prepare_with(query, parameters),
            WriterPool::Owned(pool) => Box::pin(async move {
                let mut lease = pool.acquire().await.map_err(|_| safe_error())?;
                let statement = (&mut *lease)
                    .prepare_with(query, parameters)
                    .await
                    .map_err(|_| safe_error())?;
                lease.complete().await?;
                Ok(statement)
            }),
        }
    }
    fn describe<'e, 'q: 'e>(
        self,
        query: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Postgres>, sqlx::Error>>
    where
        'c: 'e,
    {
        match self {
            WriterPool::Legacy(pool) => pool.describe(query),
            WriterPool::Owned(pool) => Box::pin(async move {
                let mut lease = pool.acquire().await.map_err(|_| safe_error())?;
                let description = (&mut *lease)
                    .describe(query)
                    .await
                    .map_err(|_| safe_error())?;
                lease.complete().await?;
                Ok(description)
            }),
        }
    }
}

#[cfg(test)]
#[path = "owned_tests.rs"]
mod tests;
