//! Atomic ownership, claim and receipt admission.
use super::{
    PostgresStorage,
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
    store: &PostgresStorage,
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
    let mut tx = store.pool.begin().await?;
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

async fn on(
    conn: &mut sqlx::PgConnection,
    tenant: &TenantId,
    next: &EffectReceipt,
    fence: &EffectDispatchFence,
    guarded: bool,
) -> Result<EffectDispatchOutcome, StorageError> {
    // Lock ownership before the instance and receipt rows, matching handoff/GC.
    let row=sqlx::query("SELECT record FROM continuity_executions WHERE tenant_id=$1 AND continuity_id=$2 FOR UPDATE").bind(tenant.as_str()).bind(next.continuity_id.into_uuid()).fetch_optional(&mut *conn).await?;
    let Some(row) = row else {
        return Ok(EffectDispatchOutcome::Stale);
    };
    let owner: ContinuityExecution = decode(row.get("record"))?;
    if !owns(&owner, tenant, next, fence) {
        return Ok(EffectDispatchOutcome::Stale);
    }
    let row=sqlx::query("SELECT state::text AS state, claimed_at FROM task_instances WHERE id=$1 AND tenant_id=$2 FOR UPDATE").bind(fence.instance_id.into_uuid()).bind(tenant.as_str()).fetch_optional(&mut *conn).await?;
    let Some(row) = row else {
        return Ok(EffectDispatchOutcome::Stale);
    };
    if !live(
        row.get::<&str, _>("state"),
        row.try_get("claimed_at")?,
        fence,
    ) {
        return Ok(EffectDispatchOutcome::Stale);
    }
    // Same key/order as the legacy guarded dispatch operation.
    if guarded {
        let key = format!(
            "effect|{}|{}|{}|{}|{}",
            tenant,
            next.continuity_id,
            state_name(next.kind)?,
            next.destination_fingerprint,
            next.request_sha256
        );
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(key)
            .execute(&mut *conn)
            .await?;
    }
    let rows = if guarded {
        sqlx::query("SELECT record FROM effect_receipts WHERE tenant_id=$1 AND continuity_id=$2 AND (id=$3 OR (record->>'kind'=$4 AND record->>'destination_fingerprint'=$5 AND record->>'request_sha256'=$6)) ORDER BY created_at,id FOR UPDATE").bind(tenant.as_str()).bind(next.continuity_id.into_uuid()).bind(next.id.into_uuid()).bind(state_name(next.kind)?).bind(&next.destination_fingerprint).bind(&next.request_sha256).fetch_all(&mut *conn).await?
    } else {
        sqlx::query("SELECT record FROM effect_receipts WHERE tenant_id=$1 AND id=$2 FOR UPDATE")
            .bind(tenant.as_str())
            .bind(next.id.into_uuid())
            .fetch_all(&mut *conn)
            .await?
    };
    let receipts = rows
        .into_iter()
        .map(|row| decode(row.get("record")))
        .collect::<Result<Vec<EffectReceipt>, _>>()?;
    let outcome = verdict(&receipts, next, guarded);
    if outcome != EffectDispatchOutcome::Dispatched {
        return Ok(outcome);
    }
    let changed=sqlx::query("UPDATE effect_receipts SET state=$1,record=$2,updated_at=$3 WHERE tenant_id=$4 AND id=$5 AND state=$6").bind(state_name(next.state)?).bind(encode(next)?).bind(next.updated_at).bind(tenant.as_str()).bind(next.id.into_uuid()).bind(state_name(EffectState::Prepared)?).execute(&mut *conn).await?.rows_affected();
    Ok(if changed == 1 {
        EffectDispatchOutcome::Dispatched
    } else {
        EffectDispatchOutcome::Stale
    })
}
