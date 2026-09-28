//! Lease expiry, timeouts, and voluntary release for external worker tasks.
//!
//! A worker task leaves the server with an effect receipt in `dispatched`
//! (every non-builtin handler is side-effecting). When the server loses track
//! of the node running it — heartbeats stop, the step times out, or the node
//! gives the task back — it must not silently hand the same effect to a second
//! node. The rules implemented here:
//!
//! * **Pure / idempotent tasks** (no unresolved receipt) are requeued: back to
//!   `pending`, lease cleared, next node claims them.
//! * **Side-effecting tasks that may have started** move their receipt to
//!   `unknown` and follow the ambiguous-effect policy used for worker-reported
//!   failures: the attempt counts as a retryable failure, so the step's retry
//!   policy either schedules a *new* attempt (new effect id) or fails the node.
//! * **Timed-out tasks** always advance the instance (retry or fail the node);
//!   a task that was never claimed abandons its receipt, since the effect
//!   provably never happened.
//!
//! Every transition is a fenced compare-and-swap on `(state, claim_epoch)`
//! applied atomically with the instance/node change by
//! [`orch8_storage::WorkerStore::resolve_worker_task`], so a racing completion always
//! wins cleanly and two reapers never double-apply.

use std::time::Duration;

use chrono::Utc;
use orch8_storage::StorageBackend;
use orch8_types::execution::NodeState;
use orch8_types::instance::TaskInstance;
use orch8_types::worker::{
    WorkerAttemptEventKind, WorkerClaim, WorkerTask, WorkerTaskResolution,
    WorkerTaskResolutionAction, WorkerTaskState,
};

use crate::effect_guard::{
    WorkerEffectSettlement, settle_worker_task_effect, worker_task_effect_is_ambiguous,
};
use crate::error::EngineError;

/// Upper bound of tasks examined per reaper pass (per category).
const REAPER_BATCH: u32 = 500;

pub const LEASE_EXPIRED_REASON: &str = "heartbeat lease expired";
pub const LEASE_EXPIRED_RESUMABLE_REASON: &str =
    "heartbeat lease expired; checkpointed activity requeued to resume (effect receipt unknown)";
pub const LEASE_EXPIRED_AMBIGUOUS_REASON: &str =
    "heartbeat lease expired after the side effect may have started (effect receipt unknown)";
pub const TIMED_OUT_REASON: &str = "task timed out (timeout_ms exceeded)";
pub const RELEASED_REASON: &str = "released by runtime before starting";
pub const RELEASED_AFTER_START_REASON: &str =
    "released by runtime after starting (effect receipt unknown)";

/// Outcome counters of one reaper pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReapReport {
    /// Pure tasks returned to `pending`.
    pub requeued: u64,
    /// Side-effecting tasks resolved through the ambiguous-effect policy.
    pub ambiguous: u64,
    /// Timed-out tasks whose instance was advanced.
    pub timed_out: u64,
}

impl ReapReport {
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.requeued + self.ambiguous + self.timed_out
    }
}

/// One reaper pass: expired leases, then timed-out tasks.
pub async fn reap_worker_tasks(
    storage: &dyn StorageBackend,
    default_lease: Duration,
) -> Result<ReapReport, EngineError> {
    let mut report = ReapReport::default();
    for task in storage
        .list_expired_worker_leases(default_lease, REAPER_BATCH)
        .await?
    {
        match resolve_expired_lease(storage, &task).await {
            Ok(Some(true)) => report.ambiguous += 1,
            Ok(Some(false)) => report.requeued += 1,
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(task_id = %task.id, %error, "worker lease resolution failed");
            }
        }
    }
    for task in storage.list_timed_out_worker_tasks(REAPER_BATCH).await? {
        match resolve_timed_out(storage, &task).await {
            Ok(true) => report.timed_out += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(task_id = %task.id, %error, "worker timeout resolution failed");
            }
        }
    }
    Ok(report)
}

/// Resolve one expired lease. `Some(ambiguous)` when applied, `None` when the
/// task moved on concurrently.
async fn resolve_expired_lease(
    storage: &dyn StorageBackend,
    task: &WorkerTask,
) -> Result<Option<bool>, EngineError> {
    let Some(instance) = storage.get_instance(task.instance_id).await? else {
        return Ok(None);
    };
    let ambiguous = worker_task_effect_is_ambiguous(storage, &instance.tenant_id, task).await?;
    let resolution = if ambiguous {
        settle_worker_task_effect(
            storage,
            &instance.tenant_id,
            task,
            WorkerEffectSettlement::Unknown,
            None,
        )
        .await?;
        if task.checkpoint_seq > 0 {
            // A resumable activity that durably checkpointed opted into
            // resumption: the replacement claimant continues from
            // `resume_checkpoint` under a new claim epoch (the receipt stays
            // `unknown` until that attempt reports) instead of re-running the
            // effect from scratch as a new attempt.
            fence(
                task,
                None,
                WorkerAttemptEventKind::Reclaimed,
                LEASE_EXPIRED_RESUMABLE_REASON,
                true,
                WorkerTaskResolutionAction::Requeue,
            )
        } else {
            let action = plan_failure_action(
                storage,
                &instance,
                task,
                true,
                LEASE_EXPIRED_AMBIGUOUS_REASON,
            )
            .await?;
            fence(
                task,
                None,
                WorkerAttemptEventKind::Reclaimed,
                LEASE_EXPIRED_AMBIGUOUS_REASON,
                true,
                action,
            )
        }
    } else {
        fence(
            task,
            None,
            WorkerAttemptEventKind::Reclaimed,
            LEASE_EXPIRED_REASON,
            true,
            WorkerTaskResolutionAction::Requeue,
        )
    };
    let applied = storage.resolve_worker_task(&resolution).await?;
    if applied {
        integrate_if_delegation(storage, task, &resolution).await?;
        tracing::info!(
            task_id = %task.id,
            instance_id = %task.instance_id,
            ambiguous,
            "worker lease expired"
        );
    }
    Ok(applied.then_some(ambiguous))
}

async fn resolve_timed_out(
    storage: &dyn StorageBackend,
    task: &WorkerTask,
) -> Result<bool, EngineError> {
    let Some(instance) = storage.get_instance(task.instance_id).await? else {
        return Ok(false);
    };
    // A task nobody claimed never ran: its effect provably did not happen.
    let settlement = if task.state == WorkerTaskState::Pending {
        WorkerEffectSettlement::Abandoned
    } else {
        WorkerEffectSettlement::Unknown
    };
    settle_worker_task_effect(storage, &instance.tenant_id, task, settlement, None).await?;
    let action = plan_failure_action(storage, &instance, task, true, TIMED_OUT_REASON).await?;
    let resolution = fence(
        task,
        None,
        WorkerAttemptEventKind::TimedOut,
        TIMED_OUT_REASON,
        true,
        action,
    );
    let applied = storage.resolve_worker_task(&resolution).await?;
    if applied {
        integrate_if_delegation(storage, task, &resolution).await?;
        tracing::info!(task_id = %task.id, instance_id = %task.instance_id, "worker task timed out");
    }
    Ok(applied)
}

/// Voluntary give-back by the lease holder (tab closing, app backgrounding).
/// `started = false`: straight back to `pending`, receipt untouched.
/// `started = true`: identical to a lease expiry after start — requeue a pure
/// task, otherwise receipt → `unknown` and the ambiguous-effect policy.
/// Returns `false` when the caller no longer holds `claim`.
pub async fn release_worker_task(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    task: &WorkerTask,
    claim: &WorkerClaim,
    started: bool,
) -> Result<bool, EngineError> {
    if task.state != WorkerTaskState::Claimed
        || task.claim_epoch != claim.claim_epoch
        || task.worker_id.as_deref() != Some(claim.worker_id.as_str())
    {
        return Ok(false);
    }
    let ambiguous =
        started && worker_task_effect_is_ambiguous(storage, &instance.tenant_id, task).await?;
    let resolution = if ambiguous {
        settle_worker_task_effect(
            storage,
            &instance.tenant_id,
            task,
            WorkerEffectSettlement::Unknown,
            None,
        )
        .await?;
        let action =
            plan_failure_action(storage, instance, task, true, RELEASED_AFTER_START_REASON).await?;
        fence(
            task,
            Some(claim.worker_id.clone()),
            WorkerAttemptEventKind::Reclaimed,
            RELEASED_AFTER_START_REASON,
            true,
            action,
        )
    } else {
        fence(
            task,
            Some(claim.worker_id.clone()),
            WorkerAttemptEventKind::Reclaimed,
            RELEASED_REASON,
            true,
            WorkerTaskResolutionAction::Requeue,
        )
    };
    let applied = storage.resolve_worker_task(&resolution).await?;
    if applied {
        integrate_if_delegation(storage, task, &resolution).await?;
    }
    Ok(applied)
}

/// A failure reported by the lease holder (`POST /workers/tasks/{id}/fail`,
/// gRPC `FailTask`). The effect receipt becomes `unknown` (a reported
/// failure never proves the effect did not happen), then one fenced
/// transaction marks the task failed and advances the instance:
///
/// * retryable + the step's retry policy allows another attempt → the task is
///   replaced by the next attempt (bound to a fresh effect id at re-dispatch);
/// * otherwise → the tree node (or a flat instance) fails;
/// * a delegation mailbox task → only the task fails and the failed outcome
///   is integrated into the parent;
/// * a terminal or paused instance → only the task fails (a late report never
///   resurrects or advances it).
///
/// Returns `false` when the caller no longer holds `claim` (nothing changed).
pub async fn fail_worker_task(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    task: &WorkerTask,
    claim: &WorkerClaim,
    message: &str,
    retryable: bool,
) -> Result<bool, EngineError> {
    if task.state != WorkerTaskState::Claimed
        || task.claim_epoch != claim.claim_epoch
        || task.worker_id.as_deref() != Some(claim.worker_id.as_str())
    {
        return Ok(false);
    }
    settle_worker_task_effect(
        storage,
        &instance.tenant_id,
        task,
        WorkerEffectSettlement::Unknown,
        None,
    )
    .await?;
    let delegation = crate::delegation::is_delegation_task(task);
    let action = if delegation
        || instance.state.is_terminal()
        || instance.state == orch8_types::instance::InstanceState::Paused
    {
        WorkerTaskResolutionAction::FailTaskOnly
    } else {
        plan_failure_action(storage, instance, task, retryable, message).await?
    };
    let resolution = fence(
        task,
        Some(claim.worker_id.clone()),
        WorkerAttemptEventKind::Failed,
        message,
        retryable,
        action,
    );
    let applied = storage.resolve_worker_task(&resolution).await?;
    if applied && delegation {
        crate::delegation::integrate_delegation_outcome(storage, task, Err(message)).await?;
    }
    Ok(applied)
}

async fn integrate_if_delegation(
    storage: &dyn StorageBackend,
    task: &WorkerTask,
    resolution: &WorkerTaskResolution,
) -> Result<(), EngineError> {
    if matches!(resolution.action, WorkerTaskResolutionAction::FailTaskOnly) {
        crate::delegation::integrate_delegation_outcome(storage, task, Err(&resolution.reason))
            .await?;
    }
    Ok(())
}

fn fence(
    task: &WorkerTask,
    expected_worker_id: Option<String>,
    event: WorkerAttemptEventKind,
    reason: &str,
    retryable: bool,
    action: WorkerTaskResolutionAction,
) -> WorkerTaskResolution {
    WorkerTaskResolution {
        task_id: task.id,
        instance_id: task.instance_id,
        expected_state: task.state,
        expected_claim_epoch: task.claim_epoch,
        expected_worker_id,
        holder_worker_id: task.worker_id.clone(),
        event,
        reason: reason.to_owned(),
        retryable,
        action,
    }
}

/// Decide how a failed worker attempt advances its instance, mirroring the
/// worker-reported failure path: retry per the step's retry policy (writing
/// the `__retry__` marker `compute_attempt` needs so the next dispatch gets a
/// fresh attempt number and effect id), else fail the tree node (the
/// evaluator propagates), else fail a flat instance.
pub async fn plan_failure_action(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    task: &WorkerTask,
    retryable: bool,
    message: &str,
) -> Result<WorkerTaskResolutionAction, EngineError> {
    let now = Utc::now();
    // A delegation never fails or retries its parent: the failed outcome is
    // integrated into the parent's context instead.
    if crate::delegation::is_delegation_task(task) {
        return Ok(WorkerTaskResolutionAction::FailTaskOnly);
    }
    let tree = storage.get_execution_tree(task.instance_id).await?;
    let live_node = tree
        .iter()
        .find(|node| {
            node.block_id == task.block_id
                && matches!(node.state, NodeState::Running | NodeState::Waiting)
        })
        .map(|node| node.id);
    let can_retry = retryable
        && !instance.state.is_terminal()
        && match storage.get_sequence(instance.sequence_id).await? {
            Some(sequence) => matches!(
                crate::evaluator::find_block(&sequence.blocks, &task.block_id),
                Some(orch8_types::sequence::BlockDefinition::Step(step))
                    if step.retry.as_ref().is_some_and(|retry| u32::from(task.attempt) + 1 < retry.max_attempts)
            ),
            None => false,
        };
    if can_retry {
        let marker_output = serde_json::json!({"_retry_marker": true, "error": message});
        let marker_size = serde_json::to_vec(&marker_output)
            .map_or(0, |bytes| u32::try_from(bytes.len()).unwrap_or(u32::MAX));
        storage
            .save_block_output(&orch8_types::output::BlockOutput {
                id: uuid::Uuid::now_v7(),
                instance_id: task.instance_id,
                block_id: task.block_id.clone(),
                output: marker_output,
                output_ref: Some("__retry__".into()),
                output_size: marker_size,
                attempt: task.attempt,
                created_at: now,
            })
            .await?;
        let retry_task = WorkerTask {
            id: uuid::Uuid::now_v7(),
            attempt: task.attempt.saturating_add(1),
            state: WorkerTaskState::Pending,
            worker_id: None,
            claimed_at: None,
            heartbeat_at: None,
            claim_epoch: 0,
            completed_at: None,
            output: None,
            error_message: None,
            error_retryable: None,
            created_at: now,
            // The re-dispatch of the next attempt creates that attempt's
            // receipt and binds its id/epoch onto this pending row; storage
            // keeps the row unclaimable (`awaiting_dispatch`) until then.
            effect_id: None,
            continuity_epoch: None,
            lease_secs: None,
            claimed_runtime_kind: None,
            ..task.clone()
        };
        return Ok(WorkerTaskResolutionAction::Retry {
            retry_task: Box::new(retry_task),
            node_id: live_node,
            fire_at: now,
        });
    }
    if tree.is_empty() {
        Ok(WorkerTaskResolutionAction::FailInstance)
    } else {
        Ok(WorkerTaskResolutionAction::FailNode {
            node_id: live_node,
            fire_at: now,
        })
    }
}
