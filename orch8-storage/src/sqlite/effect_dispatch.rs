//! Atomic ownership, claim and receipt admission.
use super::{
    SqliteStorage,
    continuity::{decode, encode, state_name},
};
use crate::{
    EffectDispatchFence,
    effect_dispatch::{live, owns, verdict},
};
use orch8_types::{
    continuity::{ContinuityExecution, EffectDispatchOutcome, EffectReceipt, EffectState},
    error::StorageError,
    ids::TenantId,
};
use sqlx::Row;

pub(super) async fn owned(
    store: &SqliteStorage,
    tenant: &TenantId,
    next: &EffectReceipt,
    fence: &EffectDispatchFence,
    guarded: bool,
) -> Result<EffectDispatchOutcome, StorageError> {
    if next.state != EffectState::Dispatched {
        return Err(StorageError::Constraint(
            "effect dispatch requires a dispatched receipt".into(),
        ));
    }
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    match on(&mut tx, tenant, next, fence, guarded).await {
        Ok(EffectDispatchOutcome::Dispatched) => {
            tx.commit().await?;
            Ok(EffectDispatchOutcome::Dispatched)
        }
        Ok(outcome) => {
            tx.rollback().await?;
            Ok(outcome)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

pub(super) async fn on(
    conn: &mut sqlx::SqliteConnection,
    tenant: &TenantId,
    next: &EffectReceipt,
    fence: &EffectDispatchFence,
    guarded: bool,
) -> Result<EffectDispatchOutcome, StorageError> {
    // Lock ownership before the instance and receipt rows, matching handoff/GC.
    let row = sqlx::query(
        "SELECT record FROM continuity_executions WHERE tenant_id=?1 AND continuity_id=?2",
    )
    .bind(tenant.as_str())
    .bind(next.continuity_id.to_string())
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(EffectDispatchOutcome::Stale);
    };
    let owner: ContinuityExecution = decode(&row.get::<String, _>("record"))?;
    if !owns(&owner, tenant, next, fence) {
        return Ok(EffectDispatchOutcome::Stale);
    }
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT state,claimed_at FROM task_instances WHERE id=?1 AND tenant_id=?2 AND {}",
        super::mobile_execution::ALLOWED
    )))
    .bind(fence.instance_id.to_string())
    .bind(tenant.as_str())
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(EffectDispatchOutcome::Stale);
    };
    if !live(
        row.get::<&str, _>("state"),
        super::helpers::parse_ts_opt(row.try_get("claimed_at")?)?,
        fence,
    ) {
        return Ok(EffectDispatchOutcome::Stale);
    }
    if next.instance_id != fence.instance_id || next.epoch != fence.epoch {
        let epoch = i64::try_from(fence.epoch.get())
            .map_err(|_| StorageError::Constraint("effect epoch exceeds SQL range".into()))?;
        let alias=sqlx::query("SELECT 1 FROM effect_attempt_bindings WHERE tenant_id=?1 AND continuity_id=?2 AND epoch=?3 AND instance_id=?4 AND block_id=?5 AND attempt=?6 AND effect_id=?7")
            .bind(tenant.as_str()).bind(next.continuity_id.to_string()).bind(epoch).bind(fence.instance_id.to_string()).bind(next.block_id.as_str()).bind(i64::from(next.attempt)).bind(next.id.to_string()).fetch_optional(&mut *conn).await?;
        if alias.is_none() {
            return Ok(EffectDispatchOutcome::Stale);
        }
    }
    let wrong_namespace=sqlx::query("SELECT 1 FROM capsule_effect_imports WHERE tenant_id=?1 AND instance_id=?2 AND continuity_id<>?3 LIMIT 1")
        .bind(tenant.as_str()).bind(fence.instance_id.to_string()).bind(next.continuity_id.to_string()).fetch_optional(&mut *conn).await?;
    if wrong_namespace.is_some() {
        return Ok(EffectDispatchOutcome::Stale);
    }
    let rows = if guarded {
        sqlx::query("SELECT record FROM effect_receipts WHERE tenant_id=?1 AND continuity_id=?2 AND (id=?3 OR (json_extract(record,'$.kind')=?4 AND json_extract(record,'$.destination_fingerprint')=?5 AND json_extract(record,'$.request_sha256')=?6)) ORDER BY created_at,id").bind(tenant.as_str()).bind(next.continuity_id.to_string()).bind(next.id.to_string()).bind(state_name(next.kind)?).bind(&next.destination_fingerprint).bind(&next.request_sha256).fetch_all(&mut *conn).await?
    } else {
        sqlx::query("SELECT record FROM effect_receipts WHERE tenant_id=?1 AND id=?2")
            .bind(tenant.as_str())
            .bind(next.id.to_string())
            .fetch_all(&mut *conn)
            .await?
    };
    let receipts = rows
        .into_iter()
        .map(|row| decode(&row.get::<String, _>("record")))
        .collect::<Result<Vec<EffectReceipt>, _>>()?;
    let outcome = verdict(&receipts, next, guarded);
    if outcome != EffectDispatchOutcome::Dispatched {
        return Ok(outcome);
    }
    let changed=sqlx::query("UPDATE effect_receipts SET state=?1,record=?2,updated_at=?3 WHERE tenant_id=?4 AND id=?5 AND state=?6").bind(state_name(next.state)?).bind(encode(next)?).bind(next.updated_at.to_rfc3339()).bind(tenant.as_str()).bind(next.id.to_string()).bind(state_name(EffectState::Prepared)?).execute(&mut *conn).await?.rows_affected();
    Ok(if changed == 1 {
        EffectDispatchOutcome::Dispatched
    } else {
        EffectDispatchOutcome::Stale
    })
}
