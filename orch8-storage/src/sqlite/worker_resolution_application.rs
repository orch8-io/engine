//! Native failure application and its durable receipt commit together.
use crate::worker_resolution_application::{
    WorkerResolutionApplication as A, WorkerResolutionIntegration, WorkerResolutionRecovery,
};
use orch8_types::{
    error::StorageError, ids::TenantId, instance::InstanceState, worker::WorkerTaskResolutionAction,
};
use sqlx::Row;
use uuid::Uuid;

pub(super) async fn recovery_page(
    store: &super::SqliteStorage,
    limit: u32,
) -> Result<Vec<WorkerResolutionRecovery>, StorageError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    let result = recovery_page_on(&mut tx, limit).await;
    super::finish_transaction(tx, result).await
}
async fn recovery_page_on(
    conn: &mut sqlx::SqliteConnection,
    limit: u32,
) -> Result<Vec<WorkerResolutionRecovery>, StorageError> {
    let turn:i64=sqlx::query_scalar("UPDATE worker_outcome_recovery_clock SET turn=turn+1 WHERE singleton=1 AND turn<9223372036854775807 RETURNING turn").fetch_optional(&mut *conn).await?.ok_or_else(invalid)?;
    let rows=sqlx::query("SELECT r.id,r.tenant_id FROM worker_resolution_recovery q JOIN worker_resolution_outcomes r ON r.id=q.outcome_id ORDER BY q.turn,q.outcome_id LIMIT ?1")
  .bind(i64::from(limit.min(256))).fetch_all(&mut *conn).await?;
    let mut page = Vec::with_capacity(rows.len());
    for row in rows {
        let id: Uuid = Uuid::parse_str(&row.get::<String, _>("id")).map_err(|_| invalid())?;
        sqlx::query("UPDATE worker_resolution_recovery SET turn=?2 WHERE outcome_id=?1")
            .bind(id.to_string())
            .bind(turn)
            .execute(&mut *conn)
            .await?;
        page.push(WorkerResolutionRecovery {
            id,
            tenant_id: TenantId::unchecked(row.get::<String, _>("tenant_id")),
        });
    }
    Ok(page)
}
pub(super) async fn apply(
    store: &super::SqliteStorage,
    plan: &WorkerResolutionIntegration,
) -> Result<A, StorageError> {
    plan.validate()?;
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    let result = apply_on(&mut tx, plan).await;
    super::finish_transaction(tx, result).await
}
async fn apply_on(
    conn: &mut sqlx::SqliteConnection,
    plan: &WorkerResolutionIntegration,
) -> Result<A, StorageError> {
    let report = &plan.report;
    let Some(mut raw) =
        super::worker_resolution::get_by_id_on(conn, &report.tenant_id, report.id).await?
    else {
        return Ok(A::ReconciliationRequired);
    };
    raw.reason = report.reason.clone();
    if raw != *report {
        return Ok(A::ReconciliationRequired);
    }
    let applied:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM worker_resolution_integrations WHERE outcome_id=?1 AND tenant_id=?2 AND instance_id=?3)")
  .bind(report.id.to_string()).bind(report.tenant_id.as_str()).bind(report.instance_id.to_string()).fetch_one(&mut *conn).await?;
    if applied {
        return Ok(A::AlreadyApplied);
    }
    let queued: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM worker_resolution_recovery WHERE outcome_id=?1)",
    )
    .bind(report.id.to_string())
    .fetch_one(&mut *conn)
    .await?;
    if !queued {
        return Ok(A::Deferred);
    }
    let Some(parent) = super::instances::get_on(conn, report.instance_id).await? else {
        return Ok(A::Deferred);
    };
    if parent.state.is_terminal() || parent.state == InstanceState::Running {
        return Ok(A::Deferred);
    }
    if parent.state == InstanceState::Paused
        && matches!(plan.action, WorkerTaskResolutionAction::FailInstance)
    {
        return Ok(A::Deferred);
    }
    let source = report.invocation.as_ref().ok_or_else(invalid)?;
    let frozen:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=?1) OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=?1 AND state IN ('captured','sealed'))")
  .bind(report.instance_id.to_string()).fetch_one(&mut *conn).await?;
    if frozen {
        return Ok(A::Deferred);
    }
    let owner:Option<String>=sqlx::query_scalar("SELECT record FROM continuity_executions WHERE tenant_id=?1 AND json_extract(record,'$.current_instance_id')=?2")
  .bind(report.tenant_id.as_str()).bind(report.instance_id.to_string()).fetch_optional(&mut *conn).await?;
    let owner: Option<orch8_types::continuity::ContinuityExecution> = owner
        .map(|value| serde_json::from_str(&value))
        .transpose()?;
    if !crate::worker_outcome::integration_source_matches(source, &parent, owner.as_ref()) {
        return Ok(A::ReconciliationRequired);
    }
    let success: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM worker_accepted_outcomes WHERE task_id=?1)",
    )
    .bind(report.task_id.to_string())
    .fetch_one(&mut *conn)
    .await?;
    if success {
        return Ok(A::ReconciliationRequired);
    }
    if !super::worker_outcome::target_is_current_on(conn, &parent, source).await? {
        return Ok(A::ReconciliationRequired);
    }
    if !attempt_is_current_on(conn, report, source).await? {
        return Ok(A::ReconciliationRequired);
    }
    if matches!(plan.action, WorkerTaskResolutionAction::Retry { .. }) {
        sqlx::query("DELETE FROM worker_tasks WHERE id=?1")
            .bind(report.task_id.to_string())
            .execute(&mut *conn)
            .await?;
    }
    super::workers::apply_resolution_action_on(
        conn,
        report.instance_id,
        &plan.action,
        Some(source),
    )
    .await?;
    sqlx::query("INSERT INTO worker_resolution_integrations(outcome_id,tenant_id,instance_id,task_id,action,applied_at) VALUES(?1,?2,?3,?4,?5,?6)")
  .bind(report.id.to_string()).bind(report.tenant_id.as_str()).bind(report.instance_id.to_string()).bind(report.task_id.to_string())
  .bind(serde_json::to_string(&plan.action_summary())?).bind(super::helpers::ts(chrono::Utc::now())).execute(&mut *conn).await?;
    sqlx::query("DELETE FROM worker_resolution_recovery WHERE outcome_id=?1")
        .bind(report.id.to_string())
        .execute(&mut *conn)
        .await?;
    Ok(A::Applied)
}
async fn attempt_is_current_on(
    conn: &mut sqlx::SqliteConnection,
    report: &crate::worker_resolution::WorkerResolutionOutcome,
    source: &crate::worker_invocation::WorkerInvocation,
) -> Result<bool, StorageError> {
    let other: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM worker_tasks WHERE instance_id=?1 AND block_id=?2 AND id<>?3)",
    )
    .bind(report.instance_id.to_string())
    .bind(source.block_id.as_str())
    .bind(report.task_id.to_string())
    .fetch_one(&mut *conn)
    .await?;
    if other {
        return Ok(false);
    }
    let hot = sqlx::query(
        "SELECT state,claim_epoch,worker_id,instance_id,effect_id FROM worker_tasks WHERE id=?1",
    )
    .bind(report.task_id.to_string())
    .fetch_optional(&mut *conn)
    .await?;
    if let Some(hot) = hot
        && (hot.get::<String, _>("state") != "failed"
            || u64::try_from(hot.get::<i64, _>("claim_epoch")).ok() != Some(report.claim_epoch)
            || hot.get::<Option<String>, _>("worker_id") != report.worker_id
            || hot.get::<String, _>("instance_id") != report.instance_id.to_string()
            || hot.get::<Option<String>, _>("effect_id")
                != report.effect_id.map(|id| id.to_string()))
    {
        return Ok(false);
    }
    if let Some(effect) = report.effect_id {
        let row =
            sqlx::query("SELECT state,record FROM effect_receipts WHERE tenant_id=?1 AND id=?2")
                .bind(report.tenant_id.as_str())
                .bind(effect.to_string())
                .fetch_optional(&mut *conn)
                .await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let receipt: orch8_types::continuity::EffectReceipt =
            serde_json::from_str(&row.get::<String, _>("record"))?;
        if row.get::<String, _>("state") != "unknown"
            || report.validate_retained_receipt(receipt).is_err()
        {
            return Ok(false);
        }
    } else {
        let alias:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM effect_attempt_bindings WHERE tenant_id=?1 AND instance_id=?2 AND block_id=?3 AND attempt=?4)")
   .bind(report.tenant_id.as_str()).bind(report.instance_id.to_string()).bind(source.block_id.as_str()).bind(i64::from(source.attempt)).fetch_one(&mut *conn).await?;
        if alias {
            return Ok(false);
        }
    }
    Ok(true)
}
fn invalid() -> StorageError {
    StorageError::Constraint("invalid retained failure application evidence".into())
}
