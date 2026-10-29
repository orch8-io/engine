//! Accepted output and hot completion share a transaction; the journal has no hot FK.
use super::SqliteStorage;
use crate::worker_outcome::WorkerAcceptedOutcome;
use orch8_types::{
    error::StorageError,
    ids::{InstanceId, TenantId},
    worker::WorkerClaim,
};
use sqlx::Row;
use uuid::Uuid;

pub(super) async fn get(
    store: &SqliteStorage,
    tenant: Option<&TenantId>,
    task_id: Uuid,
    claim: &WorkerClaim,
) -> Result<Option<WorkerAcceptedOutcome>, StorageError> {
    let row=sqlx::query("SELECT tenant_id,instance_id,record FROM worker_accepted_outcomes WHERE task_id=?1 AND (?2 IS NULL OR tenant_id=?2)")
        .bind(task_id.to_string()).bind(tenant.map(TenantId::as_str)).fetch_optional(&store.pool).await?;
    let Some(row) = row else { return Ok(None) };
    let outcome = crate::worker_outcome::decode_record(serde_json::from_str(
        &row.get::<String, _>("record"),
    )?)?;
    outcome.validate(
        &TenantId::unchecked(row.get::<String, _>("tenant_id")),
        task_id,
        InstanceId::from_uuid(
            Uuid::parse_str(&row.get::<String, _>("instance_id"))
                .map_err(|e| StorageError::Constraint(e.to_string()))?,
        ),
    )?;
    Ok((outcome.claim == *claim).then_some(outcome))
}
pub(super) async fn insert_on(
    conn: &mut sqlx::SqliteConnection,
    task_id: Uuid,
    claim: &WorkerClaim,
    completion: &crate::worker_outcome::WorkerCompletion,
) -> Result<(), StorageError> {
    let row = sqlx::query("SELECT i.tenant_id,t.instance_id FROM worker_tasks t JOIN task_instances i ON i.id=t.instance_id WHERE t.id=?1")
        .bind(task_id.to_string())
        .fetch_one(&mut *conn)
        .await?;
    let raw: Option<String>=sqlx::query_scalar("SELECT COALESCE((SELECT json_extract(record,'$.invocation') FROM worker_task_admissions WHERE task_id=?1),(SELECT record FROM worker_task_invocations WHERE task_id=?1))")
        .bind(task_id.to_string()).fetch_one(&mut *conn).await?;
    let invocation = raw.map(|value| serde_json::from_str(&value)).transpose()?;
    let mut outcome = WorkerAcceptedOutcome::new(
        TenantId::unchecked(row.get::<String, _>("tenant_id")),
        task_id,
        InstanceId::from_uuid(
            Uuid::parse_str(&row.get::<String, _>("instance_id"))
                .map_err(|e| StorageError::Constraint(e.to_string()))?,
        ),
        claim.clone(),
        invocation,
        completion,
    );
    if let Some(approval) = &completion.reconciliation {
        outcome.reconciliation =
            Some(super::worker_reconciliation::decision_on(conn, approval, &outcome).await?);
    }
    outcome.validate(&outcome.tenant_id, task_id, outcome.instance_id)?;
    sqlx::query("INSERT INTO worker_accepted_outcomes(task_id,tenant_id,instance_id,record) VALUES(?1,?2,?3,?4)")
        .bind(task_id.to_string()).bind(outcome.tenant_id.as_str()).bind(outcome.instance_id.to_string()).bind(serde_json::to_string(&crate::worker_outcome::encode_record(&outcome)?)?)
        .execute(&mut *conn).await.map_err(crate::worker_reconciliation::insert_error)?;
    Ok(())
}

/// Runs only after the exact lease CAS has won, inside that same transaction.
pub(super) async fn settle_on(
    conn: &mut sqlx::SqliteConnection,
    task_id: Uuid,
    completion: &crate::worker_outcome::WorkerCompletion,
) -> Result<(), StorageError> {
    use orch8_types::continuity::EffectReceipt;
    let invalid = || {
        StorageError::Conflict("worker completion effect evidence requires reconciliation".into())
    };
    let row=sqlx::query("SELECT t.effect_id,t.block_id,t.attempt,i.tenant_id FROM worker_tasks t JOIN task_instances i ON i.id=t.instance_id WHERE t.id=?1")
        .bind(task_id.to_string()).fetch_one(&mut *conn).await?;
    let tenant = TenantId::unchecked(row.get::<String, _>("tenant_id"));
    let effect_id = row
        .get::<Option<String>, _>("effect_id")
        .map(|id| Uuid::parse_str(&id).map_err(|e| StorageError::Constraint(e.to_string())))
        .transpose()?;
    let Some(effect_id) = effect_id else {
        // An old unbound task cannot certify that an existing effect is pure.
        let has_effect_binding: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM effect_attempt_bindings b JOIN worker_tasks t ON b.instance_id=t.instance_id AND b.block_id=t.block_id AND b.attempt=t.attempt WHERE t.id=?1 AND b.tenant_id=?2)")
            .bind(task_id.to_string()).bind(tenant.as_str()).fetch_one(&mut *conn).await?;
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
        || i64::from(invocation.attempt) != row.get::<i64, _>("attempt")
    {
        return Err(invalid());
    }
    let scope = invocation.continuity.as_ref().ok_or_else(invalid)?;
    let epoch = i64::try_from(scope.epoch).map_err(|_| invalid())?;
    let bound:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM effect_attempt_bindings b JOIN worker_tasks t ON b.instance_id=t.instance_id AND b.block_id=t.block_id AND b.attempt=t.attempt AND b.effect_id=t.effect_id WHERE t.id=?1 AND b.tenant_id=?2 AND b.continuity_id=?3 AND b.epoch=?4)")
        .bind(task_id.to_string()).bind(tenant.as_str()).bind(scope.continuity_id.to_string()).bind(epoch).fetch_one(&mut *conn).await?;
    if !bound {
        return Err(invalid());
    }
    let row = sqlx::query("SELECT state,record FROM effect_receipts WHERE tenant_id=?1 AND id=?2")
        .bind(tenant.as_str())
        .bind(effect_id.to_string())
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(invalid)?;
    let receipt: EffectReceipt = serde_json::from_str(&row.get::<String, _>("record"))?;
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
    let changed=sqlx::query("UPDATE effect_receipts SET state='committed',record=?1,updated_at=?2 WHERE tenant_id=?3 AND id=?4 AND state=?5")
        .bind(serde_json::to_string(&next)?).bind(next.updated_at.to_rfc3339()).bind(tenant.as_str()).bind(effect_id.to_string()).bind(previous).execute(&mut *conn).await?.rows_affected();
    if changed != 1 {
        return Err(invalid());
    }
    Ok(())
}

/// All native writes and the application receipt share this transaction.
pub(super) async fn apply(
    store: &super::SqliteStorage,
    integration: &crate::worker_outcome::WorkerOutcomeIntegration,
) -> Result<crate::worker_outcome::WorkerOutcomeApplication, StorageError> {
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    let result = apply_on(&mut tx, integration).await;
    super::finish_transaction(tx, result).await
}

async fn apply_on(
    conn: &mut sqlx::SqliteConnection,
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
    let applied: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM worker_outcome_integrations WHERE task_id=?1 AND tenant_id=?2 AND instance_id=?3)")
        .bind(expected.task_id.to_string()).bind(expected.tenant_id.as_str()).bind(expected.instance_id.to_string())
        .fetch_one(&mut *conn).await?;
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
    let frozen: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=?1) OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=?1 AND state IN ('captured','sealed'))")
        .bind(expected.instance_id.to_string()).fetch_one(&mut *conn).await?;
    if frozen {
        return Ok(Deferred);
    }
    let owner: Option<String> = sqlx::query_scalar("SELECT record FROM continuity_executions WHERE tenant_id=?1 AND json_extract(record,'$.current_instance_id')=?2")
        .bind(expected.tenant_id.as_str()).bind(expected.instance_id.to_string()).fetch_optional(&mut *conn).await?;
    let owner: Option<orch8_types::continuity::ContinuityExecution> = owner
        .map(|value| serde_json::from_str(&value))
        .transpose()?;
    let Some(invocation) = expected.invocation.as_ref() else {
        return Ok(ReconciliationRequired);
    };
    let mailbox = if let Some(expected_publication) = &integration.mailbox {
        let Some(publication) = super::delegation_publication::get_on(
            conn,
            &expected.tenant_id,
            expected_publication.delegation.id.into_uuid(),
        )
        .await?
        else {
            return Ok(ReconciliationRequired);
        };
        if publication != *expected_publication
            || !publication.matches_outcome(expected)?
            || !crate::worker_outcome::mailbox_source_matches(invocation, &instance, owner.as_ref())
        {
            return Ok(ReconciliationRequired);
        }
        Some(publication)
    } else {
        if !crate::worker_outcome::integration_source_matches(invocation, &instance, owner.as_ref())
        {
            return Ok(ReconciliationRequired);
        }
        None
    };
    if serde_json::to_value(&instance.context)?
        != serde_json::to_value(&integration.expected_context)?
    {
        return Ok(ContextChanged);
    }
    if mailbox.is_none() && !target_is_current_on(conn, &instance, invocation).await? {
        return Ok(ReconciliationRequired);
    }
    let proxy = mailbox
        .as_ref()
        .map(|publication| publication.is_proxy(&instance))
        .transpose()?
        .unwrap_or(false);
    // A paused proxy has no native step to resume; retain recovery until it
    // can atomically reach its terminal state without scheduling its sequence.
    if proxy && instance.state == InstanceState::Paused {
        return Ok(Deferred);
    }
    super::outputs::save_on(conn, &integration.output).await?;
    if let WorkerInvocationTarget::Tree {
        node_id,
        activation_id,
    } = invocation.target
    {
        let changed = sqlx::query("UPDATE execution_tree SET state='completed',completed_at=?1 WHERE id=?2 AND instance_id=?3 AND replace(lower(activation_id),'-','')=?4 AND state IN ('running','waiting')")
            .bind(super::helpers::ts(chrono::Utc::now())).bind(node_id.to_string())
            .bind(expected.instance_id.to_string()).bind(activation_id.simple().to_string()).execute(&mut *conn).await?.rows_affected();
        if changed != 1 {
            return Err(StorageError::Conflict(
                "worker outcome activation changed".into(),
            ));
        }
    }
    let paused = instance.state == InstanceState::Paused;
    let next_state = if paused {
        "paused"
    } else if proxy {
        "completed"
    } else {
        "scheduled"
    };
    let changed = sqlx::query("UPDATE task_instances SET context=?1,state=?2,next_fire_at=?3,updated_at=?4,claimed_at=NULL WHERE id=?5 AND tenant_id=?6")
        .bind(serde_json::to_string(&integration.context)?)
        .bind(next_state)
        .bind((!paused && !proxy).then(|| super::helpers::ts(chrono::Utc::now())))
        .bind(super::helpers::ts(chrono::Utc::now()))
        .bind(expected.instance_id.to_string()).bind(expected.tenant_id.as_str()).execute(&mut *conn).await?.rows_affected();
    if changed != 1 {
        return Err(StorageError::Conflict(
            "worker outcome native write suppressed".into(),
        ));
    }
    let changed = sqlx::query("INSERT INTO worker_outcome_integrations(task_id,tenant_id,instance_id,output_id,applied_at) VALUES(?1,?2,?3,?4,?5)")
        .bind(expected.task_id.to_string()).bind(expected.tenant_id.as_str()).bind(expected.instance_id.to_string())
        .bind(integration.output.id.to_string()).bind(super::helpers::ts(chrono::Utc::now())).execute(&mut *conn).await?.rows_affected();
    if changed != 1 {
        return Err(StorageError::Conflict(
            "worker outcome application receipt suppressed".into(),
        ));
    }
    sqlx::query("DELETE FROM worker_outcome_recovery WHERE task_id=?1")
        .bind(expected.task_id.to_string())
        .execute(&mut *conn)
        .await?;
    Ok(Applied)
}

pub(super) async fn recovery_page(
    store: &super::SqliteStorage,
    limit: u32,
) -> Result<Vec<crate::worker_outcome::WorkerOutcomeRecovery>, StorageError> {
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    let result = recovery_page_on(&mut tx, limit).await;
    super::finish_transaction(tx, result).await
}
async fn recovery_page_on(
    conn: &mut sqlx::SqliteConnection,
    limit: u32,
) -> Result<Vec<crate::worker_outcome::WorkerOutcomeRecovery>, StorageError> {
    let turn: i64 = sqlx::query_scalar("UPDATE worker_outcome_recovery_clock SET turn=turn+1 WHERE singleton=1 AND turn < 9223372036854775807 RETURNING turn")
        .fetch_optional(&mut *conn).await?.ok_or_else(|| StorageError::Constraint("worker recovery clock exhausted".into()))?;
    let rows = sqlx::query("SELECT a.task_id,a.tenant_id,json_extract(a.record,'$.claim') AS claim FROM worker_outcome_recovery r JOIN worker_accepted_outcomes a ON a.task_id=r.task_id ORDER BY r.turn,r.task_id LIMIT ?1").bind(i64::from(limit.min(256))).fetch_all(&mut *conn).await?;
    let mut page = Vec::with_capacity(rows.len());
    for row in rows {
        let task_id = Uuid::parse_str(&row.get::<String, _>("task_id"))
            .map_err(|e| StorageError::Constraint(e.to_string()))?;
        sqlx::query("INSERT INTO worker_outcome_recovery(task_id,turn) VALUES(?1,?2) ON CONFLICT(task_id) DO UPDATE SET turn=excluded.turn")
            .bind(task_id.to_string()).bind(turn).execute(&mut *conn).await?;
        let Some(claim) = row
            .get::<Option<String>, _>("claim")
            .and_then(|value| serde_json::from_str(&value).ok())
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
pub(super) async fn target_is_current_on(
    conn: &mut sqlx::SqliteConnection,
    instance: &orch8_types::instance::TaskInstance,
    invocation: &crate::worker_invocation::WorkerInvocation,
) -> Result<bool, StorageError> {
    use crate::worker_invocation::WorkerInvocationTarget;
    match invocation.target {
        WorkerInvocationTarget::Tree { node_id, activation_id } => {
            Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_tree WHERE id=?1 AND instance_id=?2 AND block_id=?3 AND replace(lower(activation_id),'-','')=?4 AND state IN ('running','waiting'))")
                .bind(node_id.to_string())
                .bind(instance.id.to_string())
                .bind(invocation.block_id.as_str())
                .bind(activation_id.simple().to_string())
                .fetch_one(&mut *conn).await?)
        }
        WorkerInvocationTarget::Flat { step_started_at } => {
            let has_tree: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_tree WHERE instance_id=?1)")
                .bind(instance.id.to_string()).fetch_one(&mut *conn).await?;
            Ok(!has_tree && instance.context.runtime.current_step.as_ref() == Some(&invocation.block_id)
                && instance.context.runtime.current_step_started_at == Some(step_started_at))
        }
        WorkerInvocationTarget::Unbound { .. } => Ok(false),
    }
}

/// Validate indexed immutable evidence before trusting a privately prepared plan.
async fn matches_accepted_on(
    conn: &mut sqlx::SqliteConnection,
    expected: &WorkerAcceptedOutcome,
) -> Result<bool, StorageError> {
    let row = sqlx::query("SELECT tenant_id,instance_id,record FROM worker_accepted_outcomes WHERE task_id=?1 AND tenant_id=?2")
        .bind(expected.task_id.to_string()).bind(expected.tenant_id.as_str())
        .fetch_optional(&mut *conn).await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let mut accepted = crate::worker_outcome::decode_record(serde_json::from_str(
        &row.get::<String, _>("record"),
    )?)?;
    accepted.validate(
        &TenantId::unchecked(row.get::<String, _>("tenant_id")),
        expected.task_id,
        InstanceId::from_uuid(
            Uuid::parse_str(&row.get::<String, _>("instance_id"))
                .map_err(|e| StorageError::Constraint(e.to_string()))?,
        ),
    )?;
    // Only the wrapper may transform the body; every immutable identity stays equal.
    accepted.output = expected.output.clone();
    if let (Some(stored), Some(logical)) = (&mut accepted.delegation, &expected.delegation) {
        stored.body = logical.body.clone();
    }
    Ok(accepted == *expected)
}
