//! Accepted output and hot completion share a transaction; the journal has no hot FK.
use super::PostgresStorage;
use crate::worker_outcome::WorkerAcceptedOutcome;
use orch8_types::{
    error::StorageError,
    ids::{InstanceId, TenantId},
    worker::WorkerClaim,
};
use sqlx::Row;
use uuid::Uuid;

pub(super) async fn get(
    store: &PostgresStorage,
    tenant: Option<&TenantId>,
    task_id: Uuid,
    claim: &WorkerClaim,
) -> Result<Option<WorkerAcceptedOutcome>, StorageError> {
    let row=sqlx::query("SELECT tenant_id,instance_id,record FROM worker_accepted_outcomes WHERE task_id=$1 AND ($2 IS NULL OR tenant_id=$2)")
        .bind(task_id).bind(tenant.map(TenantId::as_str)).fetch_optional(&store.pool).await?;
    let Some(row) = row else { return Ok(None) };
    let outcome = crate::worker_outcome::decode_record(row.get("record"))?;
    outcome.validate(
        &TenantId::unchecked(row.get::<String, _>("tenant_id")),
        task_id,
        InstanceId::from_uuid(row.get("instance_id")),
    )?;
    Ok((outcome.claim == *claim).then_some(outcome))
}
pub(super) async fn insert_on(
    conn: &mut sqlx::PgConnection,
    task_id: Uuid,
    claim: &WorkerClaim,
    completion: &crate::worker_outcome::WorkerCompletion,
) -> Result<(), StorageError> {
    let row = sqlx::query("SELECT i.tenant_id,t.instance_id FROM worker_tasks t JOIN task_instances i ON i.id=t.instance_id WHERE t.id=$1")
        .bind(task_id)
        .fetch_one(&mut *conn)
        .await?;
    let raw: Option<serde_json::Value>=sqlx::query_scalar("SELECT COALESCE((SELECT record->'invocation' FROM worker_task_admissions WHERE task_id=$1),(SELECT record FROM worker_task_invocations WHERE task_id=$1))")
        .bind(task_id).fetch_one(&mut *conn).await?;
    let invocation = raw.map(serde_json::from_value).transpose()?;
    let outcome = WorkerAcceptedOutcome::new(
        TenantId::unchecked(row.get::<String, _>("tenant_id")),
        task_id,
        InstanceId::from_uuid(row.get("instance_id")),
        claim.clone(),
        invocation,
        completion,
    );
    outcome.validate(&outcome.tenant_id, task_id, outcome.instance_id)?;
    sqlx::query("INSERT INTO worker_accepted_outcomes(task_id,tenant_id,instance_id,record) VALUES($1,$2,$3,$4)")
        .bind(task_id).bind(outcome.tenant_id.as_str()).bind(outcome.instance_id.into_uuid()).bind(crate::worker_outcome::encode_record(&outcome)?)
        .execute(&mut *conn).await?;
    Ok(())
}

/// Runs only after the exact lease CAS has won, inside that same transaction.
pub(super) async fn settle_on(
    conn: &mut sqlx::PgConnection,
    task_id: Uuid,
    completion: &crate::worker_outcome::WorkerCompletion,
) -> Result<(), StorageError> {
    use orch8_types::continuity::EffectReceipt;
    let invalid = || {
        StorageError::Conflict("worker completion effect evidence requires reconciliation".into())
    };
    let row=sqlx::query("SELECT t.effect_id,t.block_id,t.attempt,i.tenant_id FROM worker_tasks t JOIN task_instances i ON i.id=t.instance_id WHERE t.id=$1")
        .bind(task_id).fetch_one(&mut *conn).await?;
    let tenant = TenantId::unchecked(row.get::<String, _>("tenant_id"));
    let effect_id = row.get::<Option<Uuid>, _>("effect_id");
    let Some(effect_id) = effect_id else {
        // An old unbound task cannot certify that an existing effect is pure.
        let has_effect_binding: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM effect_attempt_bindings b JOIN worker_tasks t ON b.instance_id=t.instance_id AND b.block_id=t.block_id AND b.attempt=t.attempt WHERE t.id=$1 AND b.tenant_id=$2)")
            .bind(task_id).bind(tenant.as_str()).fetch_one(&mut *conn).await?;
        return if has_effect_binding {
            Err(invalid())
        } else {
            Ok(())
        };
    };
    let admission = super::worker_admission::get_on(conn, &tenant, task_id).await?;
    let invocation = match &admission {
        Some(admission) => admission.invocation.clone(),
        None => super::worker_invocations::get_on(conn, &tenant, task_id)
            .await?
            .ok_or_else(invalid)?,
    };
    if invocation
        .effect_id
        .map(orch8_types::continuity::EffectId::into_uuid)
        != Some(effect_id)
        || invocation.block_id.as_str() != row.get::<String, _>("block_id")
        || i64::from(invocation.attempt) != i64::from(row.get::<i16, _>("attempt"))
    {
        return Err(invalid());
    }
    let scope = invocation.continuity.as_ref().ok_or_else(invalid)?;
    let epoch = i64::try_from(scope.epoch).map_err(|_| invalid())?;
    let bound:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM effect_attempt_bindings b JOIN worker_tasks t ON b.instance_id=t.instance_id AND b.block_id=t.block_id AND b.attempt=t.attempt AND b.effect_id=t.effect_id WHERE t.id=$1 AND b.tenant_id=$2 AND b.continuity_id=$3 AND b.epoch=$4)")
        .bind(task_id).bind(tenant.as_str()).bind(scope.continuity_id.into_uuid()).bind(epoch).fetch_one(&mut *conn).await?;
    if !bound {
        return Err(invalid());
    }
    let row = sqlx::query(
        "SELECT state,record FROM effect_receipts WHERE tenant_id=$1 AND id=$2 FOR UPDATE",
    )
    .bind(tenant.as_str())
    .bind(effect_id)
    .fetch_optional(&mut *conn)
    .await?
    .ok_or_else(invalid)?;
    let receipt: EffectReceipt = serde_json::from_value(row.get("record"))?;
    let previous = super::continuity::state_name(receipt.state)?;
    if previous != row.get::<String, _>("state") {
        return Err(invalid());
    }
    let next = crate::worker_outcome::settled_receipt(
        &invocation,
        admission.as_ref().and_then(|a| a.effect.as_ref()),
        receipt,
        completion,
    )?;
    let changed=sqlx::query("UPDATE effect_receipts SET state='committed',record=$1,updated_at=$2 WHERE tenant_id=$3 AND id=$4 AND state=$5")
        .bind(serde_json::to_value(&next)?).bind(next.updated_at).bind(tenant.as_str()).bind(effect_id).bind(previous).execute(&mut *conn).await?.rows_affected();
    if changed != 1 {
        return Err(invalid());
    }
    Ok(())
}

/// Preserve namespace/owner -> native -> worker -> receipt ordering. Acceptance
/// may record an old owner's factual result, but never advances native state.
pub(super) async fn lock_on(
    conn: &mut sqlx::PgConnection,
    task_id: Uuid,
) -> Result<Option<(InstanceId, Option<Uuid>)>, StorageError> {
    let row=sqlx::query("SELECT t.instance_id,t.effect_id,i.tenant_id FROM worker_tasks t JOIN task_instances i ON i.id=t.instance_id WHERE t.id=$1")
        .bind(task_id).fetch_optional(&mut *conn).await?;
    let Some(row) = row else { return Ok(None) };
    let instance = InstanceId::from_uuid(row.get("instance_id"));
    let effect: Option<Uuid> = row.get("effect_id");
    let tenant = TenantId::unchecked(row.get::<String, _>("tenant_id"));
    super::portable_effects::lock_instance_on(conn, &tenant, instance).await?;
    let owners:Vec<Uuid>=sqlx::query_scalar("SELECT continuity_id FROM continuity_executions WHERE tenant_id=$1 AND (record->>'current_instance_id'=$2 OR continuity_id=(SELECT continuity_id FROM effect_receipts WHERE tenant_id=$1 AND id=$3))")
        .bind(tenant.as_str()).bind(instance.to_string()).bind(effect).fetch_all(&mut *conn).await?;
    if owners.len() > 1 {
        return Err(StorageError::Conflict(
            "completion crosses continuity namespaces".into(),
        ));
    }
    if let Some(owner) = owners.first() {
        sqlx::query("SELECT continuity_id FROM continuity_executions WHERE tenant_id=$1 AND continuity_id=$2 FOR UPDATE")
            .bind(tenant.as_str()).bind(owner).fetch_optional(&mut *conn).await?;
    }
    let parent =
        sqlx::query("SELECT id FROM task_instances WHERE tenant_id=$1 AND id=$2 FOR UPDATE")
            .bind(tenant.as_str())
            .bind(instance.into_uuid())
            .fetch_optional(&mut *conn)
            .await?;
    Ok(parent.map(|_| (instance, effect)))
}

/// All native writes and the application receipt share this transaction.
pub(super) async fn apply(
    store: &super::PostgresStorage,
    integration: &crate::worker_outcome::WorkerOutcomeIntegration,
) -> Result<crate::worker_outcome::WorkerOutcomeApplication, StorageError> {
    let mut tx = store.pool.begin().await?;
    let result = apply_on(&mut tx, integration).await;
    super::finish_transaction(tx, result).await
}

async fn apply_on(
    conn: &mut sqlx::PgConnection,
    integration: &crate::worker_outcome::WorkerOutcomeIntegration,
) -> Result<crate::worker_outcome::WorkerOutcomeApplication, StorageError> {
    use crate::worker_invocation::WorkerInvocationTarget;
    use crate::worker_outcome::WorkerOutcomeApplication::{
        AlreadyApplied, Applied, ContextChanged, Deferred, ReconciliationRequired,
    };
    use orch8_types::instance::InstanceState;
    let expected = &integration.accepted;
    if !matches_accepted_on(conn, expected).await? {
        return Ok(ReconciliationRequired);
    }
    let applied: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM worker_outcome_integrations WHERE task_id=$1 AND tenant_id=$2 AND instance_id=$3)")
        .bind(expected.task_id).bind(expected.tenant_id.as_str()).bind(expected.instance_id.into_uuid())
        .fetch_one(&mut *conn).await?;
    if applied {
        return Ok(AlreadyApplied);
    }
    if !try_lock_native_on(conn, expected).await? {
        return Ok(Deferred);
    }
    let applied: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM worker_outcome_integrations WHERE task_id=$1)",
    )
    .bind(expected.task_id)
    .fetch_one(&mut *conn)
    .await?;
    if applied {
        return Ok(AlreadyApplied);
    }
    if !integration.within_context_limit {
        return Ok(Deferred);
    }
    let Some(instance) = super::instances::get_on(conn, expected.instance_id).await? else {
        return Ok(Deferred);
    };
    if instance.state.is_terminal() || instance.state == InstanceState::Running {
        return Ok(Deferred);
    }
    let frozen: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=$1) OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=$1 AND state IN ('captured','sealed'))")
        .bind(expected.instance_id.into_uuid()).fetch_one(&mut *conn).await?;
    if frozen {
        return Ok(Deferred);
    }
    let owner: Option<serde_json::Value> = sqlx::query_scalar("SELECT record FROM continuity_executions WHERE tenant_id=$1 AND record->>'current_instance_id'=$2")
        .bind(expected.tenant_id.as_str()).bind(expected.instance_id.to_string()).fetch_optional(&mut *conn).await?;
    let owner: Option<orch8_types::continuity::ContinuityExecution> =
        owner.map(serde_json::from_value).transpose()?;
    let Some(invocation) = expected.invocation.as_ref() else {
        return Ok(ReconciliationRequired);
    };
    if !crate::worker_outcome::integration_source_matches(invocation, &instance, owner.as_ref()) {
        return Ok(ReconciliationRequired);
    }
    if serde_json::to_value(&instance.context)?
        != serde_json::to_value(&integration.expected_context)?
    {
        return Ok(ContextChanged);
    }
    match target_is_current_on(conn, &instance, invocation).await? {
        Some(true) => {}
        Some(false) => return Ok(ReconciliationRequired),
        None => return Ok(Deferred),
    }
    super::outputs::save_on(conn, &integration.output).await?;
    if let WorkerInvocationTarget::Tree {
        node_id,
        activation_id,
    } = invocation.target
    {
        let changed = sqlx::query("UPDATE execution_tree SET state='completed',completed_at=$1 WHERE id=$2 AND instance_id=$3 AND activation_id=$4 AND state IN ('running','waiting')")
            .bind(chrono::Utc::now()).bind(node_id.into_uuid())
            .bind(expected.instance_id.into_uuid()).bind(activation_id).execute(&mut *conn).await?.rows_affected();
        if changed != 1 {
            return Err(StorageError::Conflict(
                "worker outcome activation changed".into(),
            ));
        }
    }
    let paused = instance.state == InstanceState::Paused;
    sqlx::query("UPDATE task_instances SET context=$1,state=$2,next_fire_at=$3,updated_at=$4,claimed_at=NULL WHERE id=$5 AND tenant_id=$6")
        .bind(serde_json::to_value(&integration.context)?)
        .bind(if paused { "paused" } else { "scheduled" })
        .bind((!paused).then(chrono::Utc::now))
        .bind(chrono::Utc::now())
        .bind(expected.instance_id.into_uuid()).bind(expected.tenant_id.as_str()).execute(&mut *conn).await?;
    sqlx::query("INSERT INTO worker_outcome_integrations(task_id,tenant_id,instance_id,output_id,applied_at) VALUES($1,$2,$3,$4,$5)")
        .bind(expected.task_id).bind(expected.tenant_id.as_str()).bind(expected.instance_id.into_uuid())
        .bind(integration.output.id).bind(chrono::Utc::now()).execute(&mut *conn).await?;
    sqlx::query("DELETE FROM worker_outcome_recovery WHERE task_id=$1")
        .bind(expected.task_id)
        .execute(&mut *conn)
        .await?;
    Ok(Applied)
}

pub(super) async fn recovery_page(
    store: &super::PostgresStorage,
    limit: u32,
) -> Result<Vec<crate::worker_outcome::WorkerOutcomeRecovery>, StorageError> {
    let mut tx = store.pool.begin().await?;
    let result = recovery_page_on(&mut tx, limit).await;
    super::finish_transaction(tx, result).await
}
async fn recovery_page_on(
    conn: &mut sqlx::PgConnection,
    limit: u32,
) -> Result<Vec<crate::worker_outcome::WorkerOutcomeRecovery>, StorageError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let clock: Option<i64> = sqlx::query_scalar(
        "SELECT turn FROM worker_outcome_recovery_clock WHERE singleton=1 FOR UPDATE SKIP LOCKED",
    )
    .fetch_optional(&mut *conn)
    .await?;
    if clock.is_none() {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM worker_outcome_recovery_clock WHERE singleton=1)",
        )
        .fetch_one(&mut *conn)
        .await?;
        if !exists {
            return Err(StorageError::Constraint(
                "worker recovery clock missing".into(),
            ));
        }
        return Ok(Vec::new());
    }
    let turn: i64 = sqlx::query_scalar("UPDATE worker_outcome_recovery_clock SET turn=turn+1 WHERE singleton=1 AND turn < 9223372036854775807 RETURNING turn")
        .fetch_optional(&mut *conn).await?.ok_or_else(|| StorageError::Constraint("worker recovery clock exhausted".into()))?;
    let rows = sqlx::query("SELECT a.task_id,a.tenant_id,a.record->'claim' AS claim FROM worker_outcome_recovery r JOIN worker_accepted_outcomes a ON a.task_id=r.task_id ORDER BY r.turn,r.task_id LIMIT $1 FOR UPDATE OF r SKIP LOCKED").bind(i64::from(limit.min(256))).fetch_all(&mut *conn).await?;
    let mut page = Vec::with_capacity(rows.len());
    for row in rows {
        let task_id = row.get::<Uuid, _>("task_id");
        sqlx::query("INSERT INTO worker_outcome_recovery(task_id,turn) VALUES($1,$2) ON CONFLICT(task_id) DO UPDATE SET turn=excluded.turn")
            .bind(task_id).bind(turn).execute(&mut *conn).await?;
        let Some(claim) = row
            .get::<Option<serde_json::Value>, _>("claim")
            .and_then(|value| serde_json::from_value(value).ok())
        else {
            tracing::warn!(%task_id, "accepted worker claim requires reconciliation");
            continue;
        };
        page.push(crate::worker_outcome::WorkerOutcomeRecovery {
            task_id,
            tenant_id: TenantId::unchecked(row.get::<String, _>("tenant_id")),
            claim,
        });
    }
    Ok(page)
}

/// Validate the original activation without consulting any reusable hot task alias.
/// None means a current node is busy, not invalid or superseded.
async fn target_is_current_on(
    conn: &mut sqlx::PgConnection,
    instance: &orch8_types::instance::TaskInstance,
    invocation: &crate::worker_invocation::WorkerInvocation,
) -> Result<Option<bool>, StorageError> {
    use crate::worker_invocation::WorkerInvocationTarget;
    match invocation.target {
        WorkerInvocationTarget::Tree {
            node_id,
            activation_id,
        } => {
            let locked: Option<Uuid> = sqlx::query_scalar("SELECT id FROM execution_tree WHERE id=$1 AND instance_id=$2 FOR UPDATE SKIP LOCKED")
                .bind(node_id.into_uuid()).bind(instance.id.into_uuid()).fetch_optional(&mut *conn).await?;
            if locked.is_none() {
                let exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM execution_tree WHERE id=$1 AND instance_id=$2)",
                )
                .bind(node_id.into_uuid())
                .bind(instance.id.into_uuid())
                .fetch_one(&mut *conn)
                .await?;
                return Ok(if exists { None } else { Some(false) });
            }
            let valid = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_tree WHERE id=$1 AND instance_id=$2 AND block_id=$3 AND activation_id=$4 AND state IN ('running','waiting'))")
                .bind(node_id.into_uuid()).bind(instance.id.into_uuid()).bind(invocation.block_id.as_str()).bind(activation_id)
                .fetch_one(&mut *conn).await?;
            Ok(Some(valid))
        }
        WorkerInvocationTarget::Flat { step_started_at } => {
            let has_tree: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM execution_tree WHERE instance_id=$1)",
            )
            .bind(instance.id.into_uuid())
            .fetch_one(&mut *conn)
            .await?;
            Ok(Some(
                !has_tree
                    && instance.context.runtime.current_step.as_ref() == Some(&invocation.block_id)
                    && instance.context.runtime.current_step_started_at == Some(step_started_at),
            ))
        }
        WorkerInvocationTarget::Unbound { .. } => Ok(Some(false)),
    }
}

/// Validate indexed immutable evidence before trusting a privately prepared plan.
async fn matches_accepted_on(
    conn: &mut sqlx::PgConnection,
    expected: &WorkerAcceptedOutcome,
) -> Result<bool, StorageError> {
    let row = sqlx::query("SELECT tenant_id,instance_id,record FROM worker_accepted_outcomes WHERE task_id=$1 AND tenant_id=$2")
        .bind(expected.task_id).bind(expected.tenant_id.as_str())
        .fetch_optional(&mut *conn).await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let mut accepted = crate::worker_outcome::decode_record(row.get("record"))?;
    accepted.validate(
        &TenantId::unchecked(row.get::<String, _>("tenant_id")),
        expected.task_id,
        InstanceId::from_uuid(row.get("instance_id")),
    )?;
    // Only the wrapper may transform the body; every immutable identity stays equal.
    accepted.output = expected.output.clone();
    Ok(accepted == *expected)
}

/// Namespace -> owner -> native, skipping busy authority without changing its claim.
async fn try_lock_native_on(
    conn: &mut sqlx::PgConnection,
    accepted: &WorkerAcceptedOutcome,
) -> Result<bool, StorageError> {
    let tenant = &accepted.tenant_id;
    let instance = accepted.instance_id;
    if !super::portable_effects::try_lock_instance_on(conn, tenant, instance).await? {
        return Ok(false);
    }
    let owner: Option<Uuid> = sqlx::query_scalar("SELECT continuity_id FROM continuity_executions WHERE tenant_id=$1 AND record->>'current_instance_id'=$2 FOR UPDATE SKIP LOCKED")
        .bind(tenant.as_str()).bind(instance.to_string()).fetch_optional(&mut *conn).await?;
    if owner.is_none() {
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM continuity_executions WHERE tenant_id=$1 AND record->>'current_instance_id'=$2)")
            .bind(tenant.as_str()).bind(instance.to_string()).fetch_one(&mut *conn).await?;
        if exists {
            return Ok(false);
        }
    }
    let native: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM task_instances WHERE id=$1 AND tenant_id=$2 FOR UPDATE SKIP LOCKED",
    )
    .bind(instance.into_uuid())
    .bind(tenant.as_str())
    .fetch_optional(&mut *conn)
    .await?;
    if native.is_none() {
        return Ok(false);
    }
    let queued: Option<Uuid> = sqlx::query_scalar(
        "SELECT task_id FROM worker_outcome_recovery WHERE task_id=$1 FOR UPDATE SKIP LOCKED",
    )
    .bind(accepted.task_id)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(queued.is_some())
}
