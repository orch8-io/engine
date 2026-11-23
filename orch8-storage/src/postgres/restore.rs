//! Publish an imported instance only when every dependent row is durable.
use super::PostgresStorage;
use crate::restore::{EffectArchive, InstanceHistory, InstanceSnapshot, WorkerTaskArchive};
use crate::worker_invocation::WorkerInvocationTarget;
use orch8_types::{error::StorageError, execution::ExecutionNode, instance::TaskInstance};
use sqlx::{FromRow as _, Row as _};
use uuid::Uuid;

pub(super) async fn restore(
    store: &PostgresStorage,
    snapshot: &InstanceSnapshot,
    history: &InstanceHistory,
) -> Result<bool, StorageError> {
    let nodes = snapshot.checked_nodes()?;
    history.validate_snapshot(snapshot)?;
    let mut tx = store.pool.begin().await?;
    match write_on(&mut tx, snapshot, &nodes, history).await {
        Ok(true) => {
            tx.commit().await?;
            Ok(true)
        }
        Ok(false) => {
            tx.rollback().await?;
            Ok(false)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

async fn write_on(
    conn: &mut sqlx::PgConnection,
    snapshot: &InstanceSnapshot,
    nodes: &[&ExecutionNode],
    history: &InstanceHistory,
) -> Result<bool, StorageError> {
    // SQL combines two trusted constants; every snapshot value is bound.
    let insert = format!(
        "{} ON CONFLICT (id) DO NOTHING",
        super::instances::INSTANCE_INSERT_SQL
    );
    let context = super::json_encoding::encode_context(&snapshot.instance.context)?;
    let inserted = super::instances::bind_instance_insert(
        sqlx::query(sqlx::AssertSqlSafe(insert.as_str())),
        &snapshot.instance,
        &context,
    )
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if inserted == 0 {
        return Ok(false);
    }
    for node in nodes {
        super::execution_tree::create_node_on(&mut *conn, node).await?;
    }
    for entry in &snapshot.payloads {
        super::externalized_snapshot::restore_on(&mut *conn, entry).await?;
    }
    for output in &snapshot.outputs {
        super::outputs::save_on(&mut *conn, output).await?;
    }
    for checkpoint in &history.checkpoints {
        super::checkpoints::save_on(&mut *conn, checkpoint).await?;
    }
    for signal in &history.signals {
        super::signals::insert_on(&mut *conn, signal).await?;
    }
    for entry in &history.state {
        super::kv_state::restore_on(&mut *conn, entry).await?;
    }
    if let Some(effects) = &history.effects {
        restore_effects(&mut *conn, &snapshot.instance, effects).await?;
    }
    for task in &history.worker_tasks {
        restore_worker_task(conn, task).await?;
    }
    if let Some(migration) = history.read_only_migration {
        sqlx::query(
            "INSERT INTO readonly_instance_history(instance_id,migration_id) VALUES ($1,$2)",
        )
        .bind(snapshot.instance.id.into_uuid())
        .bind(migration)
        .execute(conn)
        .await?;
    }
    Ok(true)
}

async fn restore_effects(
    conn: &mut sqlx::PgConnection,
    instance: &orch8_types::instance::TaskInstance,
    effects: &EffectArchive,
) -> Result<(), StorageError> {
    let sequence = super::sequences::get_on(&mut *conn, instance.sequence_id).await?;
    if sequence.is_none_or(|sequence| sequence.version != effects.sequence_version) {
        return Err(StorageError::Constraint(
            "restored effect ledger requires its original sequence version".into(),
        ));
    }
    let owner = &effects.owner;
    super::continuity::create_execution_on(&mut *conn, owner).await?;
    for receipt in &effects.state.receipts {
        super::continuity::insert_receipt_on(&mut *conn, receipt).await?;
    }
    let epoch = i64::try_from(owner.epoch.get()).map_err(|_| {
        StorageError::Constraint("effect epoch is outside the storage range".into())
    })?;
    for binding in &effects.state.active_attempts {
        sqlx::query(
            "INSERT INTO effect_attempt_bindings (tenant_id,continuity_id,epoch,instance_id,block_id,attempt,effect_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(owner.tenant_id.as_str())
        .bind(owner.continuity_id.into_uuid())
        .bind(epoch)
        .bind(owner.current_instance_id.into_uuid())
        .bind(binding.block_id.as_str())
        .bind(i64::from(binding.attempt))
        .bind(binding.effect_id.into_uuid())
        .execute(&mut *conn)
        .await?;
    }
    for block in &effects.state.reset_blocks {
        sqlx::query(
            "INSERT INTO effect_block_resets (tenant_id,instance_id,block_id) VALUES ($1,$2,$3)",
        )
        .bind(owner.tenant_id.as_str())
        .bind(owner.current_instance_id.into_uuid())
        .bind(block.as_str())
        .execute(&mut *conn)
        .await?;
    }
    for guard in &effects.state.commit_guards {
        super::portable_effects::freeze_guard(&mut *conn, owner, guard).await?;
    }
    Ok(())
}

pub(super) async fn worker_task_archive_page(
    store: &PostgresStorage,
    instance: &TaskInstance,
    after: Option<Uuid>,
    limit: u32,
) -> Result<Vec<WorkerTaskArchive>, StorageError> {
    if instance.state.is_terminal() {
        return Ok(Vec::new());
    }
    let mut conn = store.pool.acquire().await?;
    let rows = sqlx::query(
        "SELECT wt.*, EXISTS(SELECT 1 FROM delegation_publications dp WHERE dp.task_id=wt.id) AS relay
         FROM worker_tasks wt WHERE wt.instance_id=$1 AND wt.state IN ('pending','claimed')
         AND ($2::uuid IS NULL OR wt.id>$2) ORDER BY wt.id LIMIT $3",
    )
    .bind(instance.id.into_uuid())
    .bind(after)
    .bind(i64::from(limit.min(1000)))
    .fetch_all(&mut *conn)
    .await?;
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let task = super::rows::WorkerTaskRow::from_row(&row)?.into_task()?;
        if row.try_get::<bool, _>("relay")? || task.handler_name == "orch8.delegation" {
            return Err(StorageError::Unsupported(
                "instance backup cannot preserve delegation relay authority; use a full database backup for relay state".into(),
            ));
        }
        let invocation =
            super::worker_invocations::get_on(&mut conn, &instance.tenant_id, task.id).await?;
        let admission =
            super::worker_admission::get_on(&mut conn, &instance.tenant_id, task.id).await?;
        if let Some(WorkerInvocationTarget::Tree {
            node_id,
            activation_id,
        }) = invocation.as_ref().map(|binding| &binding.target)
        {
            let actual: Option<Uuid> = sqlx::query_scalar(
                "SELECT activation_id FROM execution_tree WHERE id=$1 AND instance_id=$2 AND block_id=$3
                 AND state IN ('pending','running','waiting')",
            )
            .bind(node_id.into_uuid())
            .bind(instance.id.into_uuid())
            .bind(task.block_id.as_str())
            .fetch_optional(&mut *conn)
            .await?;
            if actual != Some(*activation_id) {
                return Err(StorageError::Conflict(
                    "queued worker invocation refers to a stale execution activation".into(),
                ));
            }
        }
        records.push(WorkerTaskArchive {
            task,
            awaiting_dispatch: row.try_get("awaiting_dispatch")?,
            invocation,
            admission,
        });
    }
    Ok(records)
}

async fn restore_worker_task(
    conn: &mut sqlx::PgConnection,
    archive: &WorkerTaskArchive,
) -> Result<(), StorageError> {
    let WorkerTaskArchive {
        task,
        awaiting_dispatch,
        invocation,
        admission,
    } = archive.requeue();
    if let Some(WorkerInvocationTarget::Tree {
        node_id,
        activation_id,
    }) = invocation.as_ref().map(|binding| &binding.target)
    {
        sqlx::query("UPDATE execution_tree SET activation_id=$1 WHERE id=$2")
            .bind(*activation_id)
            .bind(node_id.into_uuid())
            .execute(&mut *conn)
            .await?;
    }
    let inserted = super::workers::insert_task_query(&task, awaiting_dispatch)?
        .fetch_optional(&mut *conn)
        .await?
        .map(|row| row.get::<Uuid, _>("id"));
    if inserted != Some(task.id) {
        return Err(StorageError::Conflict(
            "restored worker task identity collision".into(),
        ));
    }
    if let Some(source) = invocation {
        super::worker_invocations::insert_on(conn, &source).await?;
        if super::worker_invocations::get_on(conn, &source.tenant_id, task.id)
            .await?
            .as_ref()
            != Some(&source)
        {
            return Err(StorageError::Conflict(
                "restored worker invocation identity collision".into(),
            ));
        }
    }
    if let Some(admission) = admission {
        super::worker_admission::insert_on(conn, &admission).await?;
    }
    Ok(())
}
