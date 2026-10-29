use super::SqliteStorage;
use crate::worker_invocation::{WorkerInvocation, dispatch_target};
use orch8_types::{
    continuity::EffectId,
    error::StorageError,
    ids::{BlockId, ExecutionNodeId, InstanceId, SequenceId, TenantId},
};
use sqlx::Row;
use uuid::Uuid;

fn uuid(value: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(value)
        .map_err(|_| StorageError::Constraint("invalid worker invocation identity".into()))
}

pub(super) async fn read_on(
    conn: &mut sqlx::SqliteConnection,
    task_id: &str,
    target_override: Option<crate::worker_invocation::WorkerInvocationTarget>,
) -> Result<WorkerInvocation, StorageError> {
    let task = sqlx::query("SELECT instance_id,block_id,handler_name,attempt,continuity_epoch,effect_id,awaiting_dispatch FROM worker_tasks WHERE id=?1")
        .bind(task_id).fetch_one(&mut *conn).await?;
    let instance_id: String = task.get("instance_id");
    let parent = sqlx::query("SELECT COALESCE(json_extract(context,'$.runtime.dry_run'),0) AS dry_run, state AS native_state, EXISTS(SELECT 1 FROM physical_instance_retirements retired WHERE retired.instance_id=task_instances.id) OR EXISTS(SELECT 1 FROM capsule_source_cuts cut WHERE cut.source_instance_id=task_instances.id AND cut.state IN ('captured','sealed')) AS frozen, tenant_id,sequence_id,json_extract(context,'$.runtime.run_id') AS run_id,json_extract(context,'$.runtime.current_step') AS current_step,json_extract(context,'$.runtime.current_step_started_at') AS step_started_at FROM task_instances WHERE id=?1")
        .bind(&instance_id).fetch_one(&mut *conn).await?;
    let block: String = task.get("block_id");
    let has_tree: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_tree WHERE instance_id=?1)")
            .bind(&instance_id)
            .fetch_one(&mut *conn)
            .await?;
    let rows = sqlx::query("SELECT id,activation_id FROM execution_tree WHERE instance_id=?1 AND block_id=?2 AND state IN ('pending','running','waiting') ORDER BY id LIMIT 2")
        .bind(&instance_id).bind(&block).fetch_all(&mut *conn).await?;
    let nodes = rows
        .iter()
        .map(|row| {
            Ok((
                ExecutionNodeId::from_uuid(uuid(&row.get::<String, _>("id"))?),
                uuid(&row.get::<String, _>("activation_id"))?,
            ))
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    let run_id: Option<String> = parent.get("run_id");
    let current_step: Option<String> = parent.get("current_step");
    let started: Option<String> = parent.get("step_started_at");
    let flat_started = if current_step.as_deref() == Some(block.as_str()) {
        started
            .as_deref()
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
            .map(|time| time.with_timezone(&chrono::Utc))
    } else {
        None
    };
    let target = target_override
        .unwrap_or_else(|| dispatch_target(run_id.as_deref(), has_tree, &nodes, flat_started));
    let mut record = WorkerInvocation {
        task_id: uuid(task_id)?,
        instance_id: InstanceId::from_uuid(uuid(&instance_id)?),
        tenant_id: TenantId::unchecked(parent.get::<String, _>("tenant_id")),
        sequence_id: SequenceId::from_uuid(uuid(&parent.get::<String, _>("sequence_id"))?),
        block_id: BlockId::new(block),
        handler_name: task.get("handler_name"),
        attempt: u16::try_from(task.get::<i64, _>("attempt"))
            .map_err(|_| StorageError::Constraint("invalid worker attempt".into()))?,
        continuity: None,
        continuity_epoch: task
            .get::<Option<i64>, _>("continuity_epoch")
            .map(u64::try_from)
            .transpose()
            .map_err(|_| StorageError::Constraint("negative worker continuity epoch".into()))?,
        effect_id: task
            .get::<Option<String>, _>("effect_id")
            .map(|id| uuid(&id).map(EffectId::from_uuid))
            .transpose()?,
        run_id: run_id.filter(|id| id.len() <= 128),
        dry_run: parent.get("dry_run"),
        target,
    };
    bind_owner_on(conn, &mut record, task.get("awaiting_dispatch")).await?;
    if parent.get::<bool, _>("frozen")
        || !matches!(
            parent.get::<String, _>("native_state").as_str(),
            "scheduled" | "running" | "waiting" | "paused"
        )
    {
        record.target = crate::worker_invocation::WorkerInvocationTarget::Unbound {
            reason: "inactive_or_frozen_native_source".into(),
        };
    }

    Ok(record)
}

pub(super) async fn get(
    store: &SqliteStorage,
    tenant: &TenantId,
    task_id: Uuid,
) -> Result<Option<WorkerInvocation>, StorageError> {
    let mut conn = store.pool.acquire().await?;
    get_on(&mut conn, tenant, task_id).await
}

pub(super) async fn get_on(
    conn: &mut sqlx::SqliteConnection,
    tenant: &TenantId,
    task_id: Uuid,
) -> Result<Option<WorkerInvocation>, StorageError> {
    let row = sqlx::query(
        "SELECT instance_id,record FROM worker_task_invocations WHERE task_id=?1 AND tenant_id=?2",
    )
    .bind(task_id.to_string())
    .bind(tenant.as_str())
    .fetch_optional(conn)
    .await?;
    let Some(row) = row else { return Ok(None) };
    let record: WorkerInvocation = serde_json::from_str(&row.get::<String, _>("record"))?;
    record.validate_scope(
        tenant,
        task_id,
        InstanceId::from_uuid(uuid(&row.get::<String, _>("instance_id"))?),
    )?;
    Ok(Some(record))
}

pub(super) async fn insert_on(
    conn: &mut sqlx::SqliteConnection,
    record: &WorkerInvocation,
) -> Result<(), StorageError> {
    sqlx::query("INSERT OR IGNORE INTO worker_task_invocations(task_id,tenant_id,instance_id,record) VALUES(?1,?2,?3,?4)")
        .bind(record.task_id.to_string()).bind(record.tenant_id.as_str()).bind(record.instance_id.to_string()).bind(serde_json::to_string(record)?).execute(conn).await?;
    Ok(())
}

pub(super) async fn capture(
    conn: &mut sqlx::SqliteConnection,
    task_id: &str,
    target_override: Option<crate::worker_invocation::WorkerInvocationTarget>,
) -> Result<(), StorageError> {
    let record = read_on(conn, task_id, target_override).await?;
    insert_on(conn, &record).await
}

pub(super) async fn source_on(
    conn: &mut sqlx::SqliteConnection,
    task_id: &str,
) -> Result<Option<WorkerInvocation>, StorageError> {
    let current = read_on(conn, task_id, None).await?;
    let Some(original) = get_on(conn, &current.tenant_id, uuid(task_id)?).await? else {
        return Ok(None);
    };
    let expected = if let Some(admission) =
        super::worker_admission::get_on(conn, &current.tenant_id, uuid(task_id)?).await?
    {
        if !original.same_invocation_scope(&admission.invocation)
            || !super::worker_admission::matches_hot_on(conn, &admission).await?
        {
            return Ok(None);
        }
        admission.invocation
    } else {
        original
    };
    Ok(expected.matches_source(&current).then_some(expected))
}

pub(super) async fn reserve_retry_on(
    conn: &mut sqlx::SqliteConnection,
    source: &WorkerInvocation,
    task: &orch8_types::worker::WorkerTask,
    node: Option<ExecutionNodeId>,
) -> Result<(), StorageError> {
    if !source.accepts_node(node) {
        return Err(StorageError::Conflict(
            "retry node differs from source invocation".into(),
        ));
    }
    let activation = match source.target {
        crate::worker_invocation::WorkerInvocationTarget::Tree {
            node_id,
            activation_id,
        } => {
            let token:Option<String> = sqlx::query_scalar("UPDATE execution_tree SET state='pending' WHERE id=?1 AND instance_id=?2 AND replace(lower(activation_id),'-','')=?3 RETURNING activation_id")
                .bind(node_id.to_string()).bind(source.instance_id.to_string()).bind(activation_id.simple().to_string()).fetch_optional(&mut *conn).await?;
            // SQLite AFTER triggers rotate the token after RETURNING is formed.
            if token.is_none() {
                return Err(StorageError::Conflict("retry activation changed".into()));
            }
            let token: String =
                sqlx::query_scalar("SELECT activation_id FROM execution_tree WHERE id=?1")
                    .bind(node_id.to_string())
                    .fetch_one(&mut *conn)
                    .await?;
            Some(uuid(&token)?)
        }
        _ => None,
    };
    let queued = super::workers::insert_task(conn, task, true).await?;
    if queued.as_deref() != Some(task.id.to_string().as_str()) {
        return Err(StorageError::Conflict(
            "retry task identity was not reserved".into(),
        ));
    }
    let binding = source.retry_binding(task, activation)?;
    insert_on(conn, &binding).await?;
    if get_on(conn, &binding.tenant_id, task.id).await?.as_ref() != Some(&binding) {
        return Err(StorageError::Conflict(
            "retry binding identity collision".into(),
        ));
    }
    Ok(())
}

async fn bind_owner_on(
    conn: &mut sqlx::SqliteConnection,
    record: &mut WorkerInvocation,
    awaiting_dispatch: bool,
) -> Result<(), StorageError> {
    // A reserved retry's hot epoch is intentionally unset. Recover it only
    // from retained evidence, then compare the actual current owner below.
    if record.continuity_epoch.is_none()
        && awaiting_dispatch
        && let Some(saved) = get_on(conn, &record.tenant_id, record.task_id).await?
    {
        record.continuity_epoch = saved.continuity_epoch;
    }
    let owner: Option<String> = sqlx::query_scalar("SELECT record FROM continuity_executions WHERE tenant_id=?1 AND json_extract(record,'$.current_instance_id')=?2")
        .bind(record.tenant_id.as_str()).bind(record.instance_id.to_string()).fetch_optional(&mut *conn).await?;
    let owner = owner
        .map(|v| serde_json::from_str::<orch8_types::continuity::ContinuityExecution>(&v))
        .transpose()?;
    // Historical ownership survives relocation. Absence of a current owner
    // cannot turn a previously scoped physical instance into standalone work.
    if owner.is_none() {
        let has_history: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM continuity_locations WHERE tenant_id=?1 AND instance_id=?2)",
        )
        .bind(record.tenant_id.as_str())
        .bind(record.instance_id.to_string())
        .fetch_one(&mut *conn)
        .await?;
        if has_history {
            record.target = crate::worker_invocation::WorkerInvocationTarget::Unbound {
                reason: "missing_or_moved_native_owner".into(),
            };
            return Ok(());
        }
    }
    record.bind_continuity(owner.as_ref());
    Ok(())
}

pub(super) async fn recovery_source_on(
    conn: &mut sqlx::SqliteConnection,
    task_id: &str,
    effect: Option<&crate::worker_invocation::WorkerDispatchEffect>,
    epoch: Option<u64>,
) -> Result<Option<WorkerInvocation>, StorageError> {
    // Only recovery may preview a missing hot effect ID. It must still match
    // the retained invocation and the original receipt proof below. Ordinary
    // source mutations continue to use source_on's strict hot-field checks.
    let mut current = read_on(conn, task_id, None).await?;
    let Some(original) = get_on(conn, &current.tenant_id, uuid(task_id)?).await? else {
        return Ok(None);
    };
    let source = if let Some(admission) =
        super::worker_admission::get_on(conn, &current.tenant_id, uuid(task_id)?).await?
    {
        if !original.same_invocation_scope(&admission.invocation)
            || !super::worker_admission::matches_hot_on(conn, &admission).await?
        {
            return Ok(None);
        }
        admission.invocation
    } else {
        original
    };
    if current.effect_id.is_none() {
        current.effect_id = effect.map(|proof| proof.effect_id);
    }
    if !source.matches_source(&current) {
        return Ok(None);
    }
    if let crate::worker_invocation::WorkerInvocationTarget::Tree { node_id, .. } = source.target {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM execution_tree WHERE id=?1 AND state='waiting')",
        )
        .bind(node_id.to_string())
        .fetch_one(&mut *conn)
        .await?;
        if !waiting {
            return Ok(None);
        }
    }
    let receipt = if let Some(proof) = effect {
        let id = proof.effect_id;
        let Some(scope) = &source.continuity else {
            return Ok(None);
        };
        let value:Option<String>=sqlx::query_scalar("SELECT record FROM effect_receipts WHERE id=?1 AND tenant_id=?2 AND instance_id=?3 AND continuity_id=?4 AND state='dispatched'")
            .bind(id.to_string()).bind(source.tenant_id.as_str()).bind(source.instance_id.to_string()).bind(scope.continuity_id.to_string()).fetch_optional(&mut *conn).await?;
        value
            .map(|value| serde_json::from_str(&value))
            .transpose()?
    } else {
        None
    };
    Ok(source
        .accepts_recovery_effect(effect, epoch, receipt.as_ref())
        .then_some(source))
}
