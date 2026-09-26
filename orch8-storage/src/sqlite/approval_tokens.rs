//! Approval action tokens. Timestamps are written in a fixed-width,
//! lexicographically sortable RFC 3339 form (`sortable_ts`), but expiry is
//! always decided in Rust on the fetched row.

use chrono::{DateTime, Utc};
use sqlx::Row;

use orch8_types::approval_link::{ApprovalActionToken, ApprovalChannel};
use orch8_types::error::StorageError;
use orch8_types::ids::{BlockId, InstanceId, TenantId};

use super::SqliteStorage;
use super::helpers::{begin_immediate, parse_ts, parse_ts_opt};

/// Fixed-width UTC timestamp so string comparison matches time order.
pub(super) fn sortable_ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

fn row_to_token(row: &sqlx::sqlite::SqliteRow) -> Result<ApprovalActionToken, StorageError> {
    let channel: String = row.try_get("channel")?;
    let instance: String = row.try_get("instance_id")?;
    Ok(ApprovalActionToken {
        token_hash: row.try_get("token_hash")?,
        tenant_id: TenantId::unchecked(row.try_get::<String, _>("tenant_id")?),
        instance_id: InstanceId::from_uuid(
            uuid::Uuid::parse_str(&instance).map_err(|e| StorageError::Query(e.to_string()))?,
        ),
        block_id: BlockId::new(row.try_get::<String, _>("block_id")?),
        choice: row.try_get("choice")?,
        channel: ApprovalChannel::parse(&channel)
            .ok_or_else(|| StorageError::Query(format!("unknown approval channel '{channel}'")))?,
        recipient: row.try_get("recipient")?,
        verify_secret_ref: row.try_get("verify_secret_ref")?,
        created_at: parse_ts(&row.try_get::<String, _>("created_at")?)?,
        expires_at: parse_ts(&row.try_get::<String, _>("expires_at")?)?,
        used_at: parse_ts_opt(row.try_get("used_at")?)?,
    })
}

pub(super) async fn create(
    store: &SqliteStorage,
    tokens: &[ApprovalActionToken],
) -> Result<(), StorageError> {
    let mut tx = begin_immediate(&store.pool).await?;
    let cutoff = sortable_ts(Utc::now() - chrono::Duration::days(7));
    sqlx::query(
        r"DELETE FROM approval_action_tokens WHERE token_hash IN (
              SELECT token_hash FROM approval_action_tokens WHERE expires_at < ?1 LIMIT 100)",
    )
    .bind(cutoff)
    .execute(&mut *tx)
    .await?;
    for t in tokens {
        sqlx::query(
            r"INSERT INTO approval_action_tokens
              (token_hash, tenant_id, instance_id, block_id, choice, channel, recipient,
               verify_secret_ref, created_at, expires_at, used_at)
              VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        )
        .bind(&t.token_hash)
        .bind(t.tenant_id.as_str())
        .bind(t.instance_id.into_uuid().to_string())
        .bind(t.block_id.as_str())
        .bind(&t.choice)
        .bind(t.channel.as_str())
        .bind(&t.recipient)
        .bind(&t.verify_secret_ref)
        .bind(sortable_ts(t.created_at))
        .bind(sortable_ts(t.expires_at))
        .bind(t.used_at.map(sortable_ts))
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn get(
    store: &SqliteStorage,
    token_hash: &str,
) -> Result<Option<ApprovalActionToken>, StorageError> {
    let row = sqlx::query("SELECT * FROM approval_action_tokens WHERE token_hash = ?1")
        .bind(token_hash)
        .fetch_optional(&store.pool)
        .await?;
    row.as_ref().map(row_to_token).transpose()
}

pub(super) async fn consume(
    store: &SqliteStorage,
    token_hash: &str,
    now: DateTime<Utc>,
) -> Result<Option<ApprovalActionToken>, StorageError> {
    let mut tx = begin_immediate(&store.pool).await?;
    let row = sqlx::query("SELECT * FROM approval_action_tokens WHERE token_hash = ?1")
        .bind(token_hash)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(row) = row else {
        tx.rollback().await?;
        return Ok(None);
    };
    let mut token = row_to_token(&row)?;
    if token.used_at.is_some() || token.expires_at <= now {
        tx.rollback().await?;
        return Ok(None);
    }
    let used = sortable_ts(now);
    sqlx::query(
        r"UPDATE approval_action_tokens SET used_at = ?3
          WHERE instance_id = ?1 AND block_id = ?2 AND used_at IS NULL",
    )
    .bind(token.instance_id.into_uuid().to_string())
    .bind(token.block_id.as_str())
    .bind(&used)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    token.used_at = Some(now);
    Ok(Some(token))
}

pub(super) async fn has_live(
    store: &SqliteStorage,
    instance_id: InstanceId,
    block_id: &BlockId,
    channel: ApprovalChannel,
    now: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let rows = sqlx::query(
        r"SELECT expires_at FROM approval_action_tokens
          WHERE instance_id = ?1 AND block_id = ?2 AND channel = ?3 AND used_at IS NULL",
    )
    .bind(instance_id.into_uuid().to_string())
    .bind(block_id.as_str())
    .bind(channel.as_str())
    .fetch_all(&store.pool)
    .await?;
    for r in rows {
        if parse_ts(&r.try_get::<String, _>("expires_at")?)? > now {
            return Ok(true);
        }
    }
    Ok(false)
}
