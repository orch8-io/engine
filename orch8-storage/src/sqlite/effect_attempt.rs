//! Durable active-invocation identity selection.
use super::{
    SqliteStorage,
    continuity::{decode, encode},
};
use orch8_types::{
    continuity::{EffectId, EffectReceipt, EffectState},
    error::StorageError,
    ids::TenantId,
};
use sqlx::Row;

pub(super) async fn ensure(
    store: &SqliteStorage,
    candidate: &EffectReceipt,
    legacy: EffectId,
) -> Result<EffectReceipt, StorageError> {
    if candidate.state != EffectState::Planned {
        return Err(StorageError::Constraint(
            "effect attempt candidate must be planned".into(),
        ));
    }
    let epoch = i64::try_from(candidate.epoch.get())
        .map_err(|_| StorageError::Constraint("effect epoch exceeds SQL integer range".into()))?;
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    match on(&mut tx, candidate, legacy, epoch).await {
        Ok(receipt) => {
            tx.commit().await?;
            Ok(receipt)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

fn bind_key<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>,
    candidate: &'q EffectReceipt,
    epoch: i64,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments> {
    query
        .bind(candidate.tenant_id.as_str())
        .bind(candidate.continuity_id.to_string())
        .bind(epoch)
        .bind(candidate.instance_id.to_string())
        .bind(candidate.block_id.as_str())
        .bind(i64::from(candidate.attempt))
}

async fn load(
    conn: &mut sqlx::SqliteConnection,
    tenant: &TenantId,
    id: EffectId,
) -> Result<Option<EffectReceipt>, StorageError> {
    let row = sqlx::query("SELECT record FROM effect_receipts WHERE tenant_id=?1 AND id=?2")
        .bind(tenant.as_str())
        .bind(id.to_string())
        .fetch_optional(&mut *conn)
        .await?;
    row.map(|row| decode(&row.get::<String, _>("record")))
        .transpose()
}

async fn on(
    conn: &mut sqlx::SqliteConnection,
    candidate: &EffectReceipt,
    legacy: EffectId,
    epoch: i64,
) -> Result<EffectReceipt, StorageError> {
    // BEGIN IMMEDIATE serializes identity creation and iteration retirement.
    let owner = sqlx::query(
        "SELECT continuity_id FROM continuity_executions WHERE tenant_id=?1 AND continuity_id=?2",
    )
    .bind(candidate.tenant_id.as_str())
    .bind(candidate.continuity_id.to_string())
    .fetch_optional(&mut *conn)
    .await?;
    let instance = sqlx::query("SELECT id FROM task_instances WHERE tenant_id=?1 AND id=?2")
        .bind(candidate.tenant_id.as_str())
        .bind(candidate.instance_id.to_string())
        .fetch_optional(&mut *conn)
        .await?;
    if owner.is_none() || instance.is_none() {
        return Err(StorageError::Constraint(
            "effect attempt owner or instance is missing".into(),
        ));
    }
    let binding=bind_key(sqlx::query("SELECT effect_id FROM effect_attempt_bindings WHERE tenant_id=?1 AND continuity_id=?2 AND epoch=?3 AND instance_id=?4 AND block_id=?5 AND attempt=?6"),candidate,epoch).fetch_optional(&mut *conn).await?;
    if let Some(row) = binding {
        let id = EffectId::from_uuid(
            row.get::<String, _>("effect_id")
                .parse::<uuid::Uuid>()
                .map_err(|error| StorageError::Query(error.to_string()))?,
        );
        let existing = load(&mut *conn, &candidate.tenant_id, id)
            .await?
            .ok_or_else(|| StorageError::Query("bound effect receipt is missing".into()))?;
        crate::effect_attempt::validate(&existing, candidate)?;
        return Ok(existing);
    }
    let reset = sqlx::query(
        "SELECT 1 FROM effect_block_resets WHERE tenant_id=?1 AND instance_id=?2 AND block_id=?3",
    )
    .bind(candidate.tenant_id.as_str())
    .bind(candidate.instance_id.to_string())
    .bind(candidate.block_id.as_str())
    .fetch_optional(&mut *conn)
    .await?
    .is_some();
    let legacy_receipt = if reset {
        None
    } else {
        load(&mut *conn, &candidate.tenant_id, legacy).await?
    };
    // Preserve an active legacy receipt, including committed/unknown evidence.
    // This is a PK lookup, not a continuity-history scan.
    let receipt = if let Some(existing) = legacy_receipt {
        crate::effect_attempt::validate(&existing, candidate)?;
        existing
    } else {
        sqlx::query("INSERT INTO effect_receipts(id,continuity_id,tenant_id,instance_id,state,record,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)").bind(candidate.id.to_string()).bind(candidate.continuity_id.to_string()).bind(candidate.tenant_id.as_str()).bind(candidate.instance_id.to_string()).bind("planned").bind(encode(candidate)?).bind(candidate.created_at.to_rfc3339()).bind(candidate.updated_at.to_rfc3339()).execute(&mut *conn).await?;
        candidate.clone()
    };
    bind_key(sqlx::query("INSERT INTO effect_attempt_bindings(tenant_id,continuity_id,epoch,instance_id,block_id,attempt,effect_id) VALUES(?1,?2,?3,?4,?5,?6,?7)"),candidate,epoch).bind(receipt.id.to_string()).execute(&mut *conn).await?;
    Ok(receipt)
}

/// Atomically invalidate the active slot, retaining evidence independently of it.
pub(super) async fn retire_on(
    conn: &mut sqlx::SqliteConnection,
    tenant: &TenantId,
    instance_id: orch8_types::ids::InstanceId,
    blocks: &[orch8_types::ids::BlockId],
) -> Result<u64, StorageError> {
    if blocks.is_empty() {
        return Ok(0);
    }
    let retain = sqlx::query("SELECT 1 FROM workflow_invariants w JOIN task_instances i ON i.sequence_id=w.sequence_id AND i.tenant_id=w.tenant_id
 LEFT JOIN sequences s ON s.id=i.sequence_id AND s.tenant_id=i.tenant_id
 WHERE i.tenant_id=?1 AND i.id=?2 AND w.enabled=1
 AND (w.sequence_version IS NULL OR s.version IS NULL OR w.sequence_version=s.version) LIMIT 1").bind(tenant.as_str()).bind(instance_id.to_string()).fetch_optional(&mut *conn).await?.is_some();
    let mut deleted = 0;
    for block in blocks {
        sqlx::query("INSERT INTO effect_block_resets(tenant_id,instance_id,block_id) SELECT tenant_id,id,?3 FROM task_instances WHERE tenant_id=?1 AND id=?2 ON CONFLICT DO NOTHING")
   .bind(tenant.as_str()).bind(instance_id.to_string()).bind(block.as_str()).execute(&mut *conn).await?;
        sqlx::query("DELETE FROM effect_attempt_bindings WHERE tenant_id=?1 AND instance_id=?2 AND block_id=?3")
   .bind(tenant.as_str()).bind(instance_id.to_string()).bind(block.as_str()).execute(&mut *conn).await?;
        // Unstarted effects cannot have happened. Ambiguous effects survive even
        // without a guard; resolved history survives while invariants depend on it.
        deleted+=sqlx::query("DELETE FROM effect_receipts WHERE tenant_id=?1 AND instance_id=?2 AND json_extract(record, '$.block_id')=?3 AND (state IN ('planned','prepared') OR (?4=FALSE AND state NOT IN ('dispatched','unknown')))")
   .bind(tenant.as_str()).bind(instance_id.to_string()).bind(block.as_str()).bind(retain).execute(&mut *conn).await?.rows_affected();
    }
    Ok(deleted)
}
