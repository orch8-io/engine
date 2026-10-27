//! SQLite reserves its writer before validating or publishing a dispatch.
use super::SqliteStorage;
use crate::worker_invocation::{WorkerDispatchFence, WorkerDispatchTarget, flat_stamp_matches};
use orch8_types::{continuity::ContinuityExecution, error::StorageError, worker::WorkerTask};
use sqlx::Row;
use uuid::Uuid;

pub(super) async fn create(
    store: &SqliteStorage,
    task: &WorkerTask,
    fence: &WorkerDispatchFence,
) -> Result<Option<Uuid>, StorageError> {
    fence.validate(task)?;
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    match publish(&mut tx, task, fence).await {
        Ok(Some(id)) => {
            tx.commit().await?;
            Ok(Some(id))
        }
        Ok(None) => {
            tx.rollback().await?;
            Ok(None)
        }
        Err(error) => {
            let _ = tx.rollback().await;
            Err(error)
        }
    }
}

async fn owner(
    conn: &mut sqlx::SqliteConnection,
    task: &WorkerTask,
    fence: &WorkerDispatchFence,
) -> Result<Option<ContinuityExecution>, StorageError> {
    let value: Option<String> = if let Some(scope) = &fence.continuity {
        sqlx::query_scalar("SELECT record FROM continuity_executions WHERE tenant_id=?1 AND continuity_id=?2 AND epoch=?3 AND owner_runtime_id=?4 AND state='owned'")
            .bind(fence.tenant_id.as_str()).bind(scope.continuity_id.to_string())
            .bind(i64::try_from(scope.epoch).map_err(|_|StorageError::Constraint("worker epoch out of range".into()))?).bind(scope.owner_runtime_id.to_string()).fetch_optional(conn).await?
    } else {
        sqlx::query_scalar("SELECT record FROM continuity_executions WHERE tenant_id=?1 AND json_extract(record,'$.current_instance_id')=?2")
            .bind(fence.tenant_id.as_str()).bind(task.instance_id.to_string()).fetch_optional(conn).await?
    };
    value
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(StorageError::Serialization)
}

async fn publish(
    conn: &mut sqlx::SqliteConnection,
    task: &WorkerTask,
    fence: &WorkerDispatchFence,
) -> Result<Option<Uuid>, StorageError> {
    let current = owner(conn, task, fence).await?;
    if !fence.owner_matches(task.instance_id, current.as_ref()) {
        return Ok(None);
    }
    let parent=sqlx::query("SELECT COALESCE(json_extract(context,'$.runtime.dry_run'),0) AS dry_run, json_extract(context,'$.runtime.current_step') AS current_step,json_extract(context,'$.runtime.current_step_started_at') AS step_started_at FROM task_instances WHERE id=?1 AND tenant_id=?2 AND sequence_id=?3 AND state='running' AND claimed_at=?4 AND json_extract(context,'$.runtime.run_id')=?5")
        .bind(task.instance_id.to_string()).bind(fence.tenant_id.as_str()).bind(fence.sequence_id.to_string()).bind(super::helpers::ts(fence.claim_token)).bind(&fence.run_id).fetch_optional(&mut *conn).await?;
    let Some(parent) = parent else {
        return Ok(None);
    };
    if parent.get::<bool, _>("dry_run") != fence.dry_run {
        return Ok(None);
    }
    let frozen:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=?1) OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=?1 AND state IN ('captured','sealed'))")
        .bind(task.instance_id.to_string()).fetch_one(&mut *conn).await?;
    if frozen {
        return Ok(None);
    }
    match &fence.target {
        WorkerDispatchTarget::Tree {
            node_id,
            activation_id,
        } => {
            let node:Option<String>=sqlx::query_scalar("SELECT id FROM execution_tree WHERE id=?1 AND instance_id=?2 AND block_id=?3 AND replace(lower(activation_id),'-','')=?4 AND state IN ('pending','running','waiting')")
                .bind(node_id.to_string()).bind(task.instance_id.to_string()).bind(task.block_id.as_str()).bind(activation_id.simple().to_string()).fetch_optional(&mut *conn).await?;
            if node.is_none() {
                return Ok(None);
            }
            sqlx::query("UPDATE execution_tree SET state='waiting' WHERE id=?1")
                .bind(node_id.to_string())
                .execute(&mut *conn)
                .await?;
        }
        WorkerDispatchTarget::Flat { step_started_at } => {
            let tree: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM execution_tree WHERE instance_id=?1)",
            )
            .bind(task.instance_id.to_string())
            .fetch_one(&mut *conn)
            .await?;
            let step: Option<String> = parent.get("current_step");
            let started: Option<String> = parent.get("step_started_at");
            if tree
                || !flat_stamp_matches(
                    step.as_deref(),
                    started.as_deref(),
                    &task.block_id,
                    *step_started_at,
                )
            {
                return Ok(None);
            }
            sqlx::query("UPDATE task_instances SET context=json_set(context,'$.runtime.current_step',?2,'$.runtime.current_step_started_at',?3) WHERE id=?1")
                .bind(task.instance_id.to_string()).bind(task.block_id.as_str()).bind(step_started_at.to_rfc3339()).execute(&mut *conn).await?;
        }
    }
    let Some(raw_id) = super::workers::insert_task(conn, task, false).await? else {
        return Ok(None);
    };
    let id = Uuid::parse_str(&raw_id)
        .map_err(|_| StorageError::Constraint("invalid queued worker identity".into()))?;
    if id != task.id
        && super::worker_invocations::get_on(conn, &fence.tenant_id, id)
            .await?
            .is_none()
    {
        return Ok(None);
    }
    super::worker_invocations::capture(conn, &raw_id, Some(fence.expected_target())).await?;
    let binding = super::worker_invocations::get_on(conn, &fence.tenant_id, id).await?;
    Ok(binding
        .filter(|binding| {
            fence.matches(binding)
                && binding.instance_id == task.instance_id
                && binding.block_id == task.block_id
                && binding.handler_name == task.handler_name
                && binding.attempt == task.attempt
        })
        .map(|_| id))
}

pub(super) async fn prepare(
    store: &SqliteStorage,
    instance: &orch8_types::instance::TaskInstance,
    block_id: &orch8_types::ids::BlockId,
    selection: &crate::worker_invocation::WorkerDispatchSelection,
) -> Result<Option<WorkerDispatchFence>, StorageError> {
    use crate::worker_invocation::{WorkerDispatchSelection, preparation_fence};
    let Some(claim) = instance.claimed_at else {
        return Ok(None);
    };
    let Some(run) = instance.context.runtime.run_id.as_deref() else {
        return Ok(None);
    };
    let node_id = match selection {
        WorkerDispatchSelection::Tree { node_id } => Some(node_id.to_string()),
        WorkerDispatchSelection::Flat { .. } => None,
    };
    let row = sqlx::query("SELECT node.activation_id, COALESCE(json_extract(parent.context,'$.runtime.dry_run'),0) AS dry_run, (SELECT ce.record FROM continuity_executions ce WHERE ce.tenant_id=parent.tenant_id AND json_extract(ce.record,'$.current_instance_id')=parent.id) AS owner FROM task_instances parent LEFT JOIN execution_tree node ON node.id=?6 AND node.instance_id=parent.id AND node.block_id=?7 AND node.state IN ('pending','running','waiting') WHERE parent.id=?1 AND parent.tenant_id=?2 AND parent.sequence_id=?3 AND parent.state='running' AND parent.claimed_at=?4 AND json_extract(parent.context,'$.runtime.run_id')=?5 AND ((?6 IS NOT NULL AND node.id IS NOT NULL) OR (?6 IS NULL AND NOT EXISTS (SELECT 1 FROM execution_tree tree WHERE tree.instance_id=parent.id))) AND NOT EXISTS (SELECT 1 FROM physical_instance_retirements retired WHERE retired.instance_id=parent.id) AND NOT EXISTS (SELECT 1 FROM capsule_source_cuts cut WHERE cut.source_instance_id=parent.id AND cut.state IN ('captured','sealed'))")
        .bind(instance.id.to_string()).bind(instance.tenant_id.as_str()).bind(instance.sequence_id.to_string()).bind(super::helpers::ts(claim)).bind(run).bind(node_id).bind(block_id.as_str()).fetch_optional(&store.pool).await?;
    let Some(row) = row else { return Ok(None) };
    if row.get::<bool, _>("dry_run") != instance.context.runtime.dry_run {
        return Ok(None);
    }
    let owner = row
        .get::<Option<String>, _>("owner")
        .map(|value| serde_json::from_str(&value))
        .transpose()?;
    let token = row
        .get::<Option<String>, _>("activation_id")
        .map(|value| {
            Uuid::parse_str(&value)
                .map_err(|_| StorageError::Constraint("invalid node activation identity".into()))
        })
        .transpose()?;
    Ok(preparation_fence(
        instance, block_id, selection, token, owner,
    ))
}
