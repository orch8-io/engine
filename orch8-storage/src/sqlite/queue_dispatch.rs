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

pub(super) async fn set(
    storage: &SqliteStorage,
    cfg: &QueueDispatchConfig,
    preserve_secret: bool,
) -> Result<QueueDispatchConfig, StorageError> {
    let row = sqlx::query(
        "INSERT INTO queue_dispatch (tenant_id, queue_name, mode, push_url, secret, created_at, updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(tenant_id, queue_name) DO UPDATE SET mode=excluded.mode, push_url=excluded.push_url, secret=CASE WHEN ?8 THEN queue_dispatch.secret ELSE excluded.secret END, updated_at=excluded.updated_at RETURNING tenant_id, queue_name, mode, push_url, secret, created_at, updated_at",
    )
    .bind(&cfg.tenant_id)
    .bind(&cfg.queue_name)
    .bind(mode_str(cfg.mode))
    .bind(&cfg.push_url)
    .bind(&cfg.secret)
    .bind(ts(cfg.created_at))
    .bind(ts(cfg.updated_at))
    .bind(preserve_secret)
    .fetch_one(&storage.pool)
    .await?;
    row_to_config(&row, true)
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

pub(super) async fn get_metadata(
    storage: &SqliteStorage,
    tenant_id: &str,
    queue_name: &str,
) -> Result<Option<QueueDispatchConfig>, StorageError> {
    let row = sqlx::query(
        "SELECT tenant_id, queue_name, mode, push_url, created_at, updated_at
         FROM queue_dispatch WHERE tenant_id = ?1 AND queue_name = ?2",
    )
    .bind(tenant_id)
    .bind(queue_name)
    .fetch_optional(&storage.pool)
    .await?;
    row.as_ref()
        .map(|row| row_to_config(row, false))
        .transpose()
}

// SQLite RETURNING exposes pre-trigger values. Guarded writes return metadata,
// and reviews read the new revision in a separate scoped query.
pub(super) async fn install_revision_triggers(storage: &SqliteStorage) -> Result<(), StorageError> {
    let mut tx = super::helpers::begin_immediate(&storage.pool).await?;
    sqlx::raw_sql(
        "CREATE TRIGGER IF NOT EXISTS queue_dispatch_revision_insert
         AFTER INSERT ON queue_dispatch
         BEGIN
             UPDATE queue_dispatch SET revision = lower(hex(randomblob(16)))
             WHERE tenant_id = NEW.tenant_id AND queue_name = NEW.queue_name;
         END;
         CREATE TRIGGER IF NOT EXISTS queue_dispatch_revision_update
         AFTER UPDATE OF tenant_id, queue_name, mode, push_url, secret, created_at, updated_at ON queue_dispatch
         BEGIN
             UPDATE queue_dispatch SET revision = lower(hex(randomblob(16)))
             WHERE tenant_id = NEW.tenant_id AND queue_name = NEW.queue_name;
         END;
         UPDATE queue_dispatch SET revision = lower(hex(randomblob(16))) WHERE revision = '';",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn review(
    storage: &SqliteStorage,
    tenant_id: &str,
    queue_name: &str,
) -> Result<Option<(QueueDispatchConfig, String)>, StorageError> {
    let row = sqlx::query(
        "SELECT tenant_id, queue_name, mode, push_url, created_at, updated_at, revision
         FROM queue_dispatch WHERE tenant_id = ?1 AND queue_name = ?2",
    )
    .bind(tenant_id)
    .bind(queue_name)
    .fetch_optional(&storage.pool)
    .await?;
    row.as_ref()
        .map(|row| Ok((row_to_config(row, false)?, row.try_get("revision")?)))
        .transpose()
}

pub(super) async fn set_reviewed(
    storage: &SqliteStorage,
    cfg: &QueueDispatchConfig,
    preserve_secret: bool,
    revision: Option<&str>,
) -> Result<Option<QueueDispatchConfig>, StorageError> {
    let row = if let Some(revision) = revision {
        sqlx::query(
            "UPDATE queue_dispatch
             SET mode = ?3, push_url = ?4, secret = CASE WHEN ?7 THEN secret ELSE ?5 END,
                 updated_at = ?6
             WHERE tenant_id = ?1 AND queue_name = ?2 AND revision = ?8
             RETURNING tenant_id, queue_name, mode, push_url, created_at, updated_at",
        )
        .bind(&cfg.tenant_id)
        .bind(&cfg.queue_name)
        .bind(mode_str(cfg.mode))
        .bind(&cfg.push_url)
        .bind(&cfg.secret)
        .bind(ts(cfg.updated_at))
        .bind(preserve_secret)
        .bind(revision)
        .fetch_optional(&storage.pool)
        .await?
    } else {
        sqlx::query(
            "INSERT INTO queue_dispatch (tenant_id, queue_name, mode, push_url, secret, created_at, updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7)
             ON CONFLICT (tenant_id, queue_name) DO NOTHING
             RETURNING tenant_id, queue_name, mode, push_url, created_at, updated_at",
        )
        .bind(&cfg.tenant_id)
        .bind(&cfg.queue_name)
        .bind(mode_str(cfg.mode))
        .bind(&cfg.push_url)
        .bind(&cfg.secret)
        .bind(ts(cfg.created_at))
        .bind(ts(cfg.updated_at))
        .fetch_optional(&storage.pool)
        .await?
    };
    row.as_ref()
        .map(|row| row_to_config(row, false))
        .transpose()
}

pub(super) async fn delete_reviewed(
    storage: &SqliteStorage,
    tenant_id: &str,
    queue_name: &str,
    revision: &str,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "DELETE FROM queue_dispatch WHERE tenant_id = ?1 AND queue_name = ?2 AND revision = ?3",
    )
    .bind(tenant_id)
    .bind(queue_name)
    .bind(revision)
    .execute(&storage.pool)
    .await?;
    Ok(result.rows_affected() != 0)
}

pub(super) async fn list(
    storage: &SqliteStorage,
    tenant_id: Option<&str>,
) -> Result<Vec<QueueDispatchConfig>, StorageError> {
    let mut qb = sqlx::QueryBuilder::new(
        "SELECT tenant_id, queue_name, mode, push_url, created_at, updated_at FROM queue_dispatch WHERE 1=1",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unknown_persisted_mode_is_not_treated_as_poll() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = chrono::Utc::now();
        upsert(
            &storage,
            &QueueDispatchConfig {
                tenant_id: "tenant".into(),
                queue_name: "jobs".into(),
                mode: DispatchMode::Push,
                push_url: Some("https://example.invalid/push".into()),
                secret: None,
                created_at: now,
                updated_at: now,
            },
        )
        .await
        .unwrap();
        sqlx::query("UPDATE queue_dispatch SET mode = 'future-mode' WHERE tenant_id = 'tenant'")
            .execute(storage.pool())
            .await
            .unwrap();

        for result in [
            get(&storage, "tenant", "jobs").await.map(|_| ()),
            list(&storage, Some("tenant")).await.map(|_| ()),
        ] {
            assert!(matches!(
                result,
                Err(StorageError::Query(message)) if message.contains("future-mode")
            ));
        }
    }
}
