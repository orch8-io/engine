//! Atomic compensation admission and forward-execution fence.
use super::{
    PostgresStorage,
    continuity::{decode, encode},
};
use crate::CompensationStart;
use orch8_types::{
    continuity::{ContinuityExecution, OwnershipState},
    error::StorageError,
};
use sqlx::Row;

pub(super) async fn begin(
    store: &PostgresStorage,
    start: CompensationStart<'_>,
) -> Result<bool, StorageError> {
    start.validate()?;
    let mut tx = store.pool.begin().await?;
    let created = on(&mut tx, start).await?;
    if created {
        tx.commit().await?;
    } else {
        tx.rollback().await?;
    }
    Ok(created)
}

async fn on(
    conn: &mut sqlx::PgConnection,
    start: CompensationStart<'_>,
) -> Result<bool, StorageError> {
    let run = start.run;
    let row = sqlx::query("SELECT record FROM continuity_executions WHERE tenant_id=$1 AND continuity_id=$2 FOR UPDATE")
        .bind(run.tenant_id.as_str()).bind(run.continuity_id.into_uuid()).fetch_optional(&mut *conn).await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let owner: ContinuityExecution = decode(row.get("record"))?;
    if owner != *start.execution {
        return Ok(false);
    }
    // Bind the snapshot timestamp so PostgreSQL applies the same microsecond
    // precision as it did on insert; comparing decoded values in Rust rejects
    // unchanged snapshots that originally carried nanoseconds.
    let row = sqlx::query("SELECT state::text AS state,sequence_id FROM task_instances WHERE tenant_id=$1 AND id=$2 AND updated_at=$3 FOR UPDATE")
        .bind(run.tenant_id.as_str()).bind(run.source_instance_id.into_uuid()).bind(start.instance.updated_at).fetch_optional(&mut *conn).await?;
    let Some(row) = row else {
        return Ok(false);
    };
    if row.get::<uuid::Uuid, _>("sequence_id") != start.instance.sequence_id.into_uuid()
        || row.get::<String, _>("state") != super::continuity::state_name(start.instance.state)?
    {
        return Ok(false);
    }
    let effects =
        super::effect_receipts::compensation_on(conn, &run.tenant_id, run.continuity_id).await?;
    if !start.matches_effects(&effects) {
        return Ok(false);
    }
    let inserted = sqlx::query("INSERT INTO compensation_runs (id,tenant_id,continuity_id,state,version,record,created_at,updated_at) VALUES($1,$2,$3,'planned',0,$4,$5,$6) ON CONFLICT DO NOTHING")
        .bind(run.id.into_uuid()).bind(run.tenant_id.as_str()).bind(run.continuity_id.into_uuid())
        .bind(encode(run)?).bind(run.created_at).bind(run.updated_at).execute(&mut *conn).await?.rows_affected()==1;
    if !inserted {
        return Err(StorageError::Conflict(
            "compensation run already exists or another run is active".into(),
        ));
    }
    let fenced = ContinuityExecution {
        state: OwnershipState::Compensating,
        updated_at: run.created_at,
        ..owner
    };
    sqlx::query("UPDATE continuity_executions SET state='compensating',record=$1,updated_at=$2 WHERE tenant_id=$3 AND continuity_id=$4")
        .bind(encode(&fenced)?).bind(fenced.updated_at).bind(run.tenant_id.as_str()).bind(run.continuity_id.into_uuid()).execute(&mut *conn).await?;
    sqlx::query("UPDATE task_instances SET state=CASE WHEN state IN ('completed','failed','cancelled') THEN state ELSE 'paused' END,claimed_at=NULL,next_fire_at=NULL,updated_at=$1 WHERE tenant_id=$2 AND id=$3")
        .bind(fenced.updated_at).bind(run.tenant_id.as_str()).bind(run.source_instance_id.into_uuid()).execute(&mut *conn).await?;
    Ok(true)
}
