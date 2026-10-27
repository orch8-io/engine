//! Atomic compensation admission and forward-execution fence.
use super::{
    SqliteStorage,
    continuity::{decode, encode},
};
use crate::CompensationStart;
use orch8_types::{
    continuity::{ContinuityExecution, OwnershipState},
    error::StorageError,
};
use sqlx::Row;

pub(super) async fn begin(
    store: &SqliteStorage,
    start: CompensationStart<'_>,
) -> Result<bool, StorageError> {
    start.validate()?;
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    let created = on(&mut tx, start).await?;
    if created {
        tx.commit().await?;
    } else {
        tx.rollback().await?;
    }
    Ok(created)
}

async fn on(
    conn: &mut sqlx::SqliteConnection,
    start: CompensationStart<'_>,
) -> Result<bool, StorageError> {
    let run = start.run;
    let row = sqlx::query(
        "SELECT record FROM continuity_executions WHERE tenant_id=?1 AND continuity_id=?2",
    )
    .bind(run.tenant_id.as_str())
    .bind(run.continuity_id.to_string())
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let owner: ContinuityExecution = decode(&row.get::<String, _>("record"))?;
    if owner != *start.execution {
        return Ok(false);
    }
    let row = sqlx::query(
        sqlx::AssertSqlSafe(format!("SELECT state,updated_at,sequence_id FROM task_instances WHERE tenant_id=?1 AND id=?2 AND {}",super::mobile_execution::ALLOWED)),
    )
    .bind(run.tenant_id.as_str())
    .bind(run.source_instance_id.to_string())
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let updated_at = chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("updated_at"))
        .map_err(|e| StorageError::Constraint(e.to_string()))?
        .with_timezone(&chrono::Utc);
    if updated_at != start.instance.updated_at
        || row.get::<String, _>("sequence_id") != start.instance.sequence_id.to_string()
        || row.get::<String, _>("state") != super::continuity::state_name(start.instance.state)?
    {
        return Ok(false);
    }
    let effects =
        super::effect_receipts::compensation_on(conn, &run.tenant_id, run.continuity_id).await?;
    if !start.matches_effects(&effects) {
        return Ok(false);
    }
    let inserted = sqlx::query("INSERT OR IGNORE INTO compensation_runs (id,tenant_id,continuity_id,state,version,record,created_at,updated_at) VALUES(?1,?2,?3,'planned',0,?4,?5,?6)")
        .bind(run.id.to_string()).bind(run.tenant_id.as_str()).bind(run.continuity_id.to_string())
        .bind(encode(run)?).bind(run.created_at.to_rfc3339()).bind(run.updated_at.to_rfc3339()).execute(&mut *conn).await?.rows_affected()==1;
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
    sqlx::query("UPDATE continuity_executions SET state='compensating',record=?1,updated_at=?2 WHERE tenant_id=?3 AND continuity_id=?4")
        .bind(encode(&fenced)?).bind(fenced.updated_at.to_rfc3339()).bind(run.tenant_id.as_str()).bind(run.continuity_id.to_string()).execute(&mut *conn).await?;
    sqlx::query("UPDATE task_instances SET state=CASE WHEN state IN ('completed','failed','cancelled') THEN state ELSE 'paused' END,claimed_at=NULL,next_fire_at=NULL,updated_at=?1 WHERE tenant_id=?2 AND id=?3")
        .bind(fenced.updated_at.to_rfc3339()).bind(run.tenant_id.as_str()).bind(run.source_instance_id.to_string()).execute(&mut *conn).await?;
    Ok(true)
}
