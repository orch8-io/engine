//! Receipt settlement and signed provenance are one durable transaction.
use super::{
    PostgresStorage,
    continuity::{decode, encode, state_name},
};
use crate::effect_resolution::{EffectResolutionCommit, validate};
use orch8_types::{
    continuity::{ContinuityExecution, EffectReceipt, ProvenanceEntry},
    error::StorageError,
};
use sqlx::Row;

pub(super) async fn resolve(
    store: &PostgresStorage,
    expected: &EffectReceipt,
    next: &EffectReceipt,
    entry: &ProvenanceEntry,
) -> Result<EffectResolutionCommit, StorageError> {
    validate(expected, next, entry)?;
    let mut tx = store.pool.begin().await?;
    match on(&mut tx, expected, next, entry).await {
        Ok(EffectResolutionCommit::Applied) => {
            tx.commit().await?;
            Ok(EffectResolutionCommit::Applied)
        }
        Ok(other) => {
            tx.rollback().await?;
            Ok(other)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

async fn on(
    conn: &mut sqlx::PgConnection,
    expected: &EffectReceipt,
    next: &EffectReceipt,
    entry: &ProvenanceEntry,
) -> Result<EffectResolutionCommit, StorageError> {
    // Lock ownership before receipt rows, matching dispatch and handoff.
    let owner = sqlx::query("SELECT record FROM continuity_executions WHERE tenant_id=$1 AND continuity_id=$2 FOR UPDATE")
        .bind(expected.tenant_id.as_str()).bind(expected.continuity_id.into_uuid()).fetch_optional(&mut *conn).await?;
    let Some(owner) = owner else {
        return Ok(EffectResolutionCommit::Stale);
    };
    let owner: ContinuityExecution = decode(owner.get("record"))?;
    if owner.tenant_id != entry.tenant_id
        || owner.continuity_id != entry.continuity_id
        || owner.epoch != entry.epoch
    {
        return Ok(EffectResolutionCommit::Stale);
    }
    let row = sqlx::query("SELECT record,state,continuity_id::text AS continuity_scope,instance_id::text AS instance_scope FROM effect_receipts WHERE tenant_id=$1 AND id=$2 FOR UPDATE")
        .bind(expected.tenant_id.as_str()).bind(expected.id.into_uuid()).fetch_optional(&mut *conn).await?;
    let Some(row) = row else {
        return Ok(EffectResolutionCommit::Stale);
    };
    let current: EffectReceipt = decode(row.get("record"))?;
    if current != *expected
        || row.get::<String, _>("state") != state_name(expected.state)?
        || row.get::<String, _>("continuity_scope") != expected.continuity_id.to_string()
        || row.get::<String, _>("instance_scope") != expected.instance_id.to_string()
    {
        return Ok(EffectResolutionCommit::Stale);
    }
    let heads = sqlx::query("SELECT head.entry_sha256 FROM provenance_entries head WHERE head.tenant_id=$1 AND head.continuity_id=$2 AND NOT EXISTS (SELECT 1 FROM provenance_entries child WHERE child.tenant_id=head.tenant_id AND child.continuity_id=head.continuity_id AND child.previous_sha256=head.entry_sha256) LIMIT 2")
        .bind(expected.tenant_id.as_str()).bind(expected.continuity_id.into_uuid()).fetch_all(&mut *conn).await?;
    if heads.len() > 1 {
        return Err(StorageError::Constraint(
            "effect resolution provenance chain has multiple heads".into(),
        ));
    }
    let previous = heads
        .first()
        .map(|row| row.get::<String, _>("entry_sha256"));
    if previous != entry.previous_sha256 {
        return Ok(EffectResolutionCommit::ProvenanceChanged);
    }
    let changed = sqlx::query(
        "UPDATE effect_receipts SET state=$1,record=$2,updated_at=$3 WHERE tenant_id=$4 AND id=$5",
    )
    .bind(state_name(next.state)?)
    .bind(encode(next)?)
    .bind(next.updated_at)
    .bind(expected.tenant_id.as_str())
    .bind(expected.id.into_uuid())
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if changed != 1 {
        return Ok(EffectResolutionCommit::Stale);
    }
    let inserted = sqlx::query("INSERT INTO provenance_entries(id,continuity_id,tenant_id,epoch,entry_sha256,previous_sha256,record,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING")
        .bind(entry.id).bind(entry.continuity_id.into_uuid()).bind(entry.tenant_id.as_str())
        .bind(i64::try_from(entry.epoch.get()).map_err(|_| StorageError::Constraint("provenance epoch exceeds database range".into()))?)
        .bind(&entry.entry_sha256).bind(&entry.previous_sha256).bind(encode(entry)?).bind(entry.created_at)
        .execute(&mut *conn).await?.rows_affected();
    // A racing append may consume the same predecessor without locking the
    // owner. Returning a conflict rolls the receipt update back as well.
    Ok(if inserted == 1 {
        EffectResolutionCommit::Applied
    } else {
        EffectResolutionCommit::ProvenanceChanged
    })
}
