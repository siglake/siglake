//! SQL catalog startup must serialize PostgreSQL DDL across independent pools.
//! Live cases use a disposable schema and the existing compose PostgreSQL URI.
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use iceberg::io::LocalFsStorageFactory;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_catalog_sql::{SqlBindStyle, SqlCatalog, SqlCatalogBuilder};
use sqlx::PgPool;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

// Same database-wide physical-schema key as the initializer, not a catalog name.
const INIT_LOCK: i64 = 0x4943_4542_4552_4701;

async fn load(uri: &str, name: &str) -> Result<SqlCatalog> {
    Ok(SqlCatalogBuilder::default()
        .uri(uri)
        .warehouse_location("file:///catalog-init-unused")
        // Deliberately also used by SQLite: bind style is not backend identity.
        .sql_bind_style(SqlBindStyle::DollarNumeric)
        .prop("pool.max-connections", "1")
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(name, HashMap::new())
        .await?)
}

#[tokio::test]
async fn sqlite_initialization_keeps_dollar_bind_style_compatibility() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let uri = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("catalog.db").display()
    );
    let namespace = NamespaceIdent::new("existing".to_string());
    let first = load(&uri, "sqlite").await?;
    first.create_namespace(&namespace, HashMap::new()).await?;
    drop(first);
    let reopened = load(&uri, "sqlite").await?;
    ensure!(reopened.namespace_exists(&namespace).await?);
    Ok(())
}

async fn in_postgres<F, Fut>(case: F) -> Result<()>
where
    F: FnOnce(PgPool, String, String) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let base = std::env::var("SIGLAKE_TEST_JOBS_POSTGRES_URI")
        .context("live catalog test requires SIGLAKE_TEST_JOBS_POSTGRES_URI")?;
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&base)
        .await?;
    let schema = format!("siglake_catalog_init_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut uri = url::Url::parse(&base)?;
    uri.query_pairs_mut()
        .append_pair("options[search_path]", &schema)
        .append_pair("application_name", &schema)
        // Prove the waiting initializer uses a fresh post-lock DDL snapshot.
        .append_pair(
            "options[default_transaction_isolation]",
            r"repeatable\ read",
        )
        .append_pair("options[statement_timeout]", "30000");
    let isolation_probe = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(uri.as_str())
        .await?;
    let actual_isolation: String = sqlx::query_scalar("SHOW default_transaction_isolation")
        .fetch_one(&isolation_probe)
        .await?;
    isolation_probe.close().await;
    ensure!(actual_isolation == "repeatable read");
    let outcome = timeout(
        Duration::from_secs(90),
        case(admin.clone(), uri.into(), schema.clone()),
    )
    .await
    .context("bounded PostgreSQL initialization case")
    .and_then(|x| x);
    let cleanup = timeout(
        Duration::from_secs(10),
        sqlx::query(&format!("DROP SCHEMA {schema} CASCADE")).execute(&admin),
    )
    .await;
    admin.close().await;
    cleanup.context("bounded scratch schema cleanup")??;
    outcome
}

async fn waiting(admin: &PgPool, app: &str, expected: i64) -> Result<()> {
    timeout(Duration::from_secs(20), async {
        loop {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
                 WHERE l.locktype = 'advisory' AND NOT l.granted AND a.application_name = $1",
            )
            .bind(app)
            .fetch_one(admin)
            .await?;
            if count == expected {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("initializer lock wait observation")?
}

async fn relation_count(admin: &PgPool, schema: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables \
         WHERE table_schema = $1 AND table_name IN ('iceberg_tables', 'iceberg_namespace_properties')",
    )
    .bind(schema)
    .fetch_one(admin)
    .await?)
}

#[tokio::test]
#[ignore = "requires live PostgreSQL; run by compose catalog-initialization gate"]
async fn postgres_concurrent_initializers_wait_then_share_both_tables() -> Result<()> {
    in_postgres(|admin, uri, schema| async move {
        let mut blocker = admin.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1::bigint)")
            .bind(INIT_LOCK)
            .execute(&mut *blocker)
            .await?;
        let mut tasks = JoinSet::new();
        for index in 0..8 {
            let uri = uri.clone();
            tasks.spawn(async move { load(&uri, &format!("catalog_{index}")).await });
        }
        waiting(&admin, &schema, 8).await?;
        ensure!(relation_count(&admin, &schema).await? == 0);
        blocker.commit().await?;
        let namespace = NamespaceIdent::new("usable".to_string());
        let mut finished = 0;
        while let Some(result) = tasks.join_next().await {
            let catalog = result??;
            catalog.create_namespace(&namespace, HashMap::new()).await?;
            ensure!(catalog.namespace_exists(&namespace).await?);
            finished += 1;
        }
        ensure!(finished == 8);
        ensure!(relation_count(&admin, &schema).await? == 2);
        // Reopening must preserve records, not replace either physical table.
        ensure!(
            load(&uri, "catalog_0")
                .await?
                .namespace_exists(&namespace)
                .await?
        );
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires live PostgreSQL; run by compose catalog-initialization gate"]
async fn postgres_failed_ddl_rolls_back_tables_and_releases_lock() -> Result<()> {
    in_postgres(|admin, uri, schema| async move {
        // A conflicting type makes the second DDL fail after the first succeeds.
        sqlx::query(&format!(
            "CREATE TYPE {schema}.iceberg_namespace_properties AS ENUM ('occupied')"
        ))
        .execute(&admin)
        .await?;
        let error = load(&uri, "failure").await.err().context("DDL must fail")?;
        let detail = format!("{error:?}");
        ensure!(
            detail.contains("iceberg_namespace_properties") && detail.contains("already exists")
        );
        ensure!(relation_count(&admin, &schema).await? == 0);
        // Pool/transaction drop queues rollback; observe its actual completion
        // rather than assuming the server has processed it synchronously.
        timeout(
            Duration::from_secs(20),
            sqlx::query("SELECT pg_advisory_xact_lock($1::bigint)")
                .bind(INIT_LOCK)
                .execute(&admin),
        )
        .await
        .context("failed initializer must release its transaction lock")??;
        sqlx::query(&format!("DROP TYPE {schema}.iceberg_namespace_properties"))
            .execute(&admin)
            .await?;
        load(&uri, "after_failure").await?;
        ensure!(relation_count(&admin, &schema).await? == 2);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires live PostgreSQL; run by compose catalog-initialization gate"]
async fn postgres_cancelled_initializer_does_not_retain_lock_or_create_tables() -> Result<()> {
    in_postgres(|admin, uri, schema| async move {
        let mut blocker = admin.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1::bigint)")
            .bind(INIT_LOCK)
            .execute(&mut *blocker)
            .await?;
        let mut tasks = JoinSet::new();
        let cancelled_uri = uri.clone();
        tasks.spawn(async move { load(&cancelled_uri, "cancelled").await });
        waiting(&admin, &schema, 1).await?;
        tasks.abort_all();
        while let Some(result) = tasks.join_next().await {
            ensure!(result.is_err_and(|error| error.is_cancelled()));
        }
        blocker.commit().await?;
        waiting(&admin, &schema, 0).await?;
        ensure!(relation_count(&admin, &schema).await? == 0);
        load(&uri, "after_cancel").await?;
        ensure!(relation_count(&admin, &schema).await? == 2);
        Ok(())
    })
    .await
}
