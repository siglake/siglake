//! Policy-scoped owned raw leases; intentionally not a shared history extraction.
//! Cancellation/uncertain operations discard sockets synchronously BEFORE permit
//! release. Completed operations flush and reuse. No detached close/release tasks.
//! Trusted DB/admin/schema required: SQLx arbitrary NOTICE/INFO is NOT sanitized.
use super::{PolicyConfig, PolicyError, Role};
use sqlx::{postgres::PgConnectOptions, Connection, PgConnection};
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
struct Idle {
    closed: bool,
    connections: Vec<PgConnection>,
}
pub(super) struct OwnedPool {
    options: PgConnectOptions,
    role: Role,
    idle: Mutex<Idle>,
    permits: Arc<Semaphore>,
    pub(super) budget: Duration,
}
impl OwnedPool {
    pub(super) async fn connect(
        config: PolicyConfig,
        role: Role,
    ) -> Result<Arc<Self>, PolicyError> {
        if config.role != role {
            return Err(PolicyError::Configuration);
        }
        let pool = Arc::new(Self {
            options: config.options,
            role,
            idle: Mutex::new(Idle {
                closed: false,
                connections: Vec::new(),
            }),
            permits: Arc::new(Semaphore::new(config.bounds.max_connections.into())),
            budget: config.bounds.operation_timeout,
        });
        tokio::time::timeout(pool.budget, async {
            let mut lease = pool.acquire().await?;
            lease.complete().await
        })
        .await
        .map_err(|_| PolicyError::Unavailable)??;
        Ok(pool)
    }
    pub(super) async fn acquire(self: &Arc<Self>) -> Result<Lease, PolicyError> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| PolicyError::Unavailable)?;
        let conn = {
            let mut idle = self.idle.lock().unwrap();
            if idle.closed {
                return Err(PolicyError::Unavailable);
            }
            idle.connections.pop()
        };
        let connection = match conn {
            Some(conn) => conn,
            None => PgConnection::connect_with(&self.options)
                .await
                .map_err(|_| PolicyError::Unavailable)?,
        };
        let mut lease = Lease {
            pool: self.clone(),
            connection: Some(connection),
            reusable: false,
            _permit: permit,
        };
        super::role::verify(&mut lease, self.role).await?;
        if self.idle.lock().unwrap().closed {
            return Err(PolicyError::Unavailable);
        }
        Ok(lease)
    }
    pub(super) fn close(&self) {
        let connections = {
            let mut idle = self.idle.lock().unwrap();
            idle.closed = true;
            self.permits.close();
            std::mem::take(&mut idle.connections)
        };
        drop(connections);
    }
}
pub(super) struct Lease {
    pool: Arc<OwnedPool>,
    connection: Option<PgConnection>,
    reusable: bool,
    _permit: OwnedSemaphorePermit,
}
impl Lease {
    pub(super) async fn complete(&mut self) -> Result<(), PolicyError> {
        let conn = self.connection.as_mut().unwrap();
        conn.flush().await.map_err(|_| PolicyError::Unavailable)?;
        if conn.is_in_transaction() {
            return Err(PolicyError::Unavailable);
        }
        self.reusable = true;
        Ok(())
    }
}
impl Deref for Lease {
    type Target = PgConnection;
    fn deref(&self) -> &PgConnection {
        self.connection.as_ref().unwrap()
    }
}
impl DerefMut for Lease {
    fn deref_mut(&mut self) -> &mut PgConnection {
        self.reusable = false;
        self.connection.as_mut().unwrap()
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut conn = self.connection.take();
        if self.reusable {
            let mut idle = self.pool.idle.lock().unwrap();
            if !idle.closed {
                idle.connections.push(conn.take().unwrap());
            }
        }
        drop(conn);
    }
}
#[cfg(test)]
impl OwnedPool {
    pub(super) fn idle_count(&self) -> usize {
        self.idle.lock().unwrap().connections.len()
    }
}
