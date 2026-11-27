use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::queue_dispatch::{DispatchMode, QueueDispatchConfig};

use super::PostgresStorage;

fn row_to_config(
    row: &sqlx::postgres::PgRow,
    include_secret: bool,
) -> Result<QueueDispatchConfig, StorageError> {
    let mode: String = row.get("mode");
    Ok(QueueDispatchConfig {
        tenant_id: row.get("tenant_id"),
        queue_name: row.get("queue_name"),
        mode: match mode.as_str() {
            "push" => DispatchMode::Push,
            "poll" => DispatchMode::Poll,
            _ => {
                return Err(StorageError::Query(format!(
                    "unknown queue dispatch mode: {mode}"
                )));
            }
        },
        push_url: row.get("push_url"),
        secret: if include_secret {
            row.get("secret")
        } else {
            None
        },
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn mode_str(mode: DispatchMode) -> &'static str {
    match mode {
        DispatchMode::Poll => "poll",
        DispatchMode::Push => "push",
    }
}

pub(super) async fn upsert(
    store: &PostgresStorage,
    cfg: &QueueDispatchConfig,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO queue_dispatch (tenant_id, queue_name, mode, push_url, secret, created_at, updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7)
          ON CONFLICT (tenant_id, queue_name)
          DO UPDATE SET mode = EXCLUDED.mode, push_url = EXCLUDED.push_url, secret = EXCLUDED.secret, updated_at = EXCLUDED.updated_at",
    )
    .bind(&cfg.tenant_id)
    .bind(&cfg.queue_name)
    .bind(mode_str(cfg.mode))
    .bind(&cfg.push_url)
    .bind(&cfg.secret)
    .bind(cfg.created_at)
    .bind(cfg.updated_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn set(
    store: &PostgresStorage,
    cfg: &QueueDispatchConfig,
    preserve_secret: bool,
) -> Result<QueueDispatchConfig, StorageError> {
    let row = sqlx::query(
        r"INSERT INTO queue_dispatch (tenant_id, queue_name, mode, push_url, secret, created_at, updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7)
          ON CONFLICT (tenant_id, queue_name)
          DO UPDATE SET mode = EXCLUDED.mode, push_url = EXCLUDED.push_url,
                        secret = CASE WHEN $8 THEN queue_dispatch.secret ELSE EXCLUDED.secret END,
                        updated_at = EXCLUDED.updated_at
          RETURNING tenant_id, queue_name, mode, push_url, secret, created_at, updated_at",
    )
    .bind(&cfg.tenant_id)
    .bind(&cfg.queue_name)
    .bind(mode_str(cfg.mode))
    .bind(&cfg.push_url)
    .bind(&cfg.secret)
    .bind(cfg.created_at)
    .bind(cfg.updated_at)
    .bind(preserve_secret)
    .fetch_one(&store.pool)
    .await?;
    row_to_config(&row, true)
}

pub(super) async fn get(
    store: &PostgresStorage,
    tenant_id: &str,
    queue_name: &str,
) -> Result<Option<QueueDispatchConfig>, StorageError> {
    let row = sqlx::query(
        r"SELECT tenant_id, queue_name, mode, push_url, secret, created_at, updated_at
          FROM queue_dispatch WHERE tenant_id = $1 AND queue_name = $2",
    )
    .bind(tenant_id)
    .bind(queue_name)
    .fetch_optional(&store.pool)
    .await?;
    row.as_ref().map(|r| row_to_config(r, true)).transpose()
}

pub(super) async fn get_metadata(
    store: &PostgresStorage,
    tenant_id: &str,
    queue_name: &str,
) -> Result<Option<QueueDispatchConfig>, StorageError> {
    let row = sqlx::query(
        "SELECT tenant_id, queue_name, mode, push_url, created_at, updated_at
         FROM queue_dispatch WHERE tenant_id = $1 AND queue_name = $2",
    )
    .bind(tenant_id)
    .bind(queue_name)
    .fetch_optional(&store.pool)
    .await?;
    row.as_ref()
        .map(|row| row_to_config(row, false))
        .transpose()
}

pub(super) async fn review(
    store: &PostgresStorage,
    tenant_id: &str,
    queue_name: &str,
) -> Result<Option<(QueueDispatchConfig, String)>, StorageError> {
    let row = sqlx::query(
        "SELECT tenant_id, queue_name, mode, push_url, created_at, updated_at, revision
         FROM queue_dispatch WHERE tenant_id = $1 AND queue_name = $2",
    )
    .bind(tenant_id)
    .bind(queue_name)
    .fetch_optional(&store.pool)
    .await?;
    row.as_ref()
        .map(|row| Ok((row_to_config(row, false)?, row.try_get("revision")?)))
        .transpose()
}

pub(super) async fn set_reviewed(
    store: &PostgresStorage,
    cfg: &QueueDispatchConfig,
    preserve_secret: bool,
    revision: Option<&str>,
) -> Result<Option<QueueDispatchConfig>, StorageError> {
    let row = if let Some(revision) = revision {
        sqlx::query(
            "UPDATE queue_dispatch
             SET mode = $3, push_url = $4, secret = CASE WHEN $7 THEN secret ELSE $5 END,
                 updated_at = $6
             WHERE tenant_id = $1 AND queue_name = $2 AND revision = $8
             RETURNING tenant_id, queue_name, mode, push_url, created_at, updated_at",
        )
        .bind(&cfg.tenant_id)
        .bind(&cfg.queue_name)
        .bind(mode_str(cfg.mode))
        .bind(&cfg.push_url)
        .bind(&cfg.secret)
        .bind(cfg.updated_at)
        .bind(preserve_secret)
        .bind(revision)
        .fetch_optional(&store.pool)
        .await?
    } else {
        sqlx::query(
            "INSERT INTO queue_dispatch (tenant_id, queue_name, mode, push_url, secret, created_at, updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7)
             ON CONFLICT (tenant_id, queue_name) DO NOTHING
             RETURNING tenant_id, queue_name, mode, push_url, created_at, updated_at",
        )
        .bind(&cfg.tenant_id)
        .bind(&cfg.queue_name)
        .bind(mode_str(cfg.mode))
        .bind(&cfg.push_url)
        .bind(&cfg.secret)
        .bind(cfg.created_at)
        .bind(cfg.updated_at)
        .fetch_optional(&store.pool)
        .await?
    };
    row.as_ref()
        .map(|row| row_to_config(row, false))
        .transpose()
}

pub(super) async fn delete_reviewed(
    store: &PostgresStorage,
    tenant_id: &str,
    queue_name: &str,
    revision: &str,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "DELETE FROM queue_dispatch WHERE tenant_id = $1 AND queue_name = $2 AND revision = $3",
    )
    .bind(tenant_id)
    .bind(queue_name)
    .bind(revision)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected() != 0)
}

pub(super) async fn list(
    store: &PostgresStorage,
    tenant_id: Option<&str>,
) -> Result<Vec<QueueDispatchConfig>, StorageError> {
    let mut qb = sqlx::QueryBuilder::new(
        r"SELECT tenant_id, queue_name, mode, push_url, created_at, updated_at
          FROM queue_dispatch WHERE 1=1",
    );
    if let Some(t) = tenant_id {
        qb.push(" AND tenant_id=").push_bind(t.to_string());
    }
    qb.push(" ORDER BY tenant_id, queue_name");
    let rows = qb.build().fetch_all(&store.pool).await?;
    rows.iter().map(|r| row_to_config(r, false)).collect()
}

pub(super) async fn delete(
    store: &PostgresStorage,
    tenant_id: &str,
    queue_name: &str,
) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM queue_dispatch WHERE tenant_id = $1 AND queue_name = $2")
        .bind(tenant_id)
        .bind(queue_name)
        .execute(&store.pool)
        .await?;
    Ok(())
}
