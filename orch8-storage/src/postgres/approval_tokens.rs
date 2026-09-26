use chrono::{DateTime, Utc};
use sqlx::Row;

use orch8_types::approval_link::{ApprovalActionToken, ApprovalChannel};
use orch8_types::error::StorageError;
use orch8_types::ids::{BlockId, InstanceId, TenantId};

use super::PostgresStorage;

const COLUMNS: &str = "token_hash, tenant_id, instance_id, block_id, choice, channel, recipient, verify_secret_ref, created_at, expires_at, used_at";

fn row_to_token(row: &sqlx::postgres::PgRow) -> Result<ApprovalActionToken, StorageError> {
    let channel: String = row.try_get("channel")?;
    Ok(ApprovalActionToken {
        token_hash: row.try_get("token_hash")?,
        tenant_id: TenantId::unchecked(row.try_get::<String, _>("tenant_id")?),
        instance_id: InstanceId::from_uuid(row.try_get("instance_id")?),
        block_id: BlockId::new(row.try_get::<String, _>("block_id")?),
        choice: row.try_get("choice")?,
        channel: ApprovalChannel::parse(&channel)
            .ok_or_else(|| StorageError::Query(format!("unknown approval channel '{channel}'")))?,
        recipient: row.try_get("recipient")?,
        verify_secret_ref: row.try_get("verify_secret_ref")?,
        created_at: row.try_get("created_at")?,
        expires_at: row.try_get("expires_at")?,
        used_at: row.try_get("used_at")?,
    })
}

pub(super) async fn create(
    store: &PostgresStorage,
    tokens: &[ApprovalActionToken],
) -> Result<(), StorageError> {
    let mut tx = store.pool.begin().await?;
    // Bounded opportunistic cleanup of long-dead rows.
    sqlx::query(
        r"DELETE FROM approval_action_tokens WHERE token_hash IN (
              SELECT token_hash FROM approval_action_tokens
              WHERE expires_at < NOW() - INTERVAL '7 days' LIMIT 100
          )",
    )
    .execute(&mut *tx)
    .await?;
    for t in tokens {
        sqlx::query(&format!(
            "INSERT INTO approval_action_tokens ({COLUMNS}) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)"
        ))
        .bind(&t.token_hash)
        .bind(t.tenant_id.as_str())
        .bind(t.instance_id.into_uuid())
        .bind(t.block_id.as_str())
        .bind(&t.choice)
        .bind(t.channel.as_str())
        .bind(&t.recipient)
        .bind(&t.verify_secret_ref)
        .bind(t.created_at)
        .bind(t.expires_at)
        .bind(t.used_at)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    token_hash: &str,
) -> Result<Option<ApprovalActionToken>, StorageError> {
    let row = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM approval_action_tokens WHERE token_hash = $1"
    ))
    .bind(token_hash)
    .fetch_optional(&store.pool)
    .await?;
    row.as_ref().map(row_to_token).transpose()
}

pub(super) async fn consume(
    store: &PostgresStorage,
    token_hash: &str,
    now: DateTime<Utc>,
) -> Result<Option<ApprovalActionToken>, StorageError> {
    let mut tx = store.pool.begin().await?;
    let row = sqlx::query(&format!(
        "UPDATE approval_action_tokens SET used_at = $2
         WHERE token_hash = $1 AND used_at IS NULL AND expires_at > $2
         RETURNING {COLUMNS}"
    ))
    .bind(token_hash)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        tx.rollback().await?;
        return Ok(None);
    };
    let token = row_to_token(&row)?;
    // Burn every sibling action for the same gate (all channels/choices).
    sqlx::query(
        r"UPDATE approval_action_tokens SET used_at = $3
          WHERE instance_id = $1 AND block_id = $2 AND used_at IS NULL",
    )
    .bind(token.instance_id.into_uuid())
    .bind(token.block_id.as_str())
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(token))
}

pub(super) async fn has_live(
    store: &PostgresStorage,
    instance_id: InstanceId,
    block_id: &BlockId,
    channel: ApprovalChannel,
    now: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let n: i64 = sqlx::query_scalar(
        r"SELECT COUNT(*) FROM approval_action_tokens
          WHERE instance_id = $1 AND block_id = $2 AND channel = $3
            AND used_at IS NULL AND expires_at > $4",
    )
    .bind(instance_id.into_uuid())
    .bind(block_id.as_str())
    .bind(channel.as_str())
    .bind(now)
    .fetch_one(&store.pool)
    .await?;
    Ok(n > 0)
}
