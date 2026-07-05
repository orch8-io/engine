use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::queue_dispatch::{DispatchMode, QueueDispatchConfig};

use super::SqliteStorage;
use super::helpers::{parse_ts, ts};

/// `include_secret` is false for list (we never leak secrets in bulk reads).
fn row_to_config(
    row: &sqlx::sqlite::SqliteRow,
    include_secret: bool,
) -> Result<QueueDispatchConfig, StorageError> {
    let mode: String = row.get("mode");
    Ok(QueueDispatchConfig {
        tenant_id: row.get("tenant_id"),
        queue_name: row.get("queue_name"),
        mode: if mode == "push" {
            DispatchMode::Push
        } else {
            DispatchMode::Poll
        },
        push_url: row.get("push_url"),
        secret: if include_secret {
            row.get("secret")
        } else {
            None
        },
        created_at: parse_ts(row.get::<&str, _>("created_at"))?,
        updated_at: parse_ts(row.get::<&str, _>("updated_at"))?,
    })
}

fn mode_str(mode: DispatchMode) -> &'static str {
    match mode {
        DispatchMode::Poll => "poll",
        DispatchMode::Push => "push",
    }
}

pub(super) async fn upsert(
    storage: &SqliteStorage,
    cfg: &QueueDispatchConfig,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO queue_dispatch (tenant_id, queue_name, mode, push_url, secret, created_at, updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(tenant_id, queue_name) DO UPDATE SET mode=excluded.mode, push_url=excluded.push_url, secret=excluded.secret, updated_at=excluded.updated_at",
    )
    .bind(&cfg.tenant_id)
    .bind(&cfg.queue_name)
    .bind(mode_str(cfg.mode))
    .bind(&cfg.push_url)
    .bind(&cfg.secret)
    .bind(ts(cfg.created_at))
    .bind(ts(cfg.updated_at))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    tenant_id: &str,
    queue_name: &str,
) -> Result<Option<QueueDispatchConfig>, StorageError> {
    let row = sqlx::query(
        "SELECT tenant_id, queue_name, mode, push_url, secret, created_at, updated_at FROM queue_dispatch WHERE tenant_id = ?1 AND queue_name = ?2",
    )
    .bind(tenant_id)
    .bind(queue_name)
    .fetch_optional(&storage.pool)
    .await?;
    row.as_ref().map(|r| row_to_config(r, true)).transpose()
}

pub(super) async fn list(
    storage: &SqliteStorage,
    tenant_id: Option<&str>,
) -> Result<Vec<QueueDispatchConfig>, StorageError> {
    let mut qb = sqlx::QueryBuilder::new(
        "SELECT tenant_id, queue_name, mode, push_url, secret, created_at, updated_at FROM queue_dispatch WHERE 1=1",
    );
    if let Some(t) = tenant_id {
        qb.push(" AND tenant_id=").push_bind(t);
    }
    qb.push(" ORDER BY tenant_id, queue_name");
    let rows = qb.build().fetch_all(&storage.pool).await?;
    rows.iter().map(|r| row_to_config(r, false)).collect()
}

pub(super) async fn delete(
    storage: &SqliteStorage,
    tenant_id: &str,
    queue_name: &str,
) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM queue_dispatch WHERE tenant_id = ?1 AND queue_name = ?2")
        .bind(tenant_id)
        .bind(queue_name)
        .execute(&storage.pool)
        .await?;
    Ok(())
}
