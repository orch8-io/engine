//! Race-safe instance lifecycle operations shared by every control surface
//! (HTTP, gRPC, MCP, batch actions, diagnosis remediation).
//!
//! These compose [`StorageBackend`] primitives with compare-and-swap state
//! transitions so a concurrent scheduler tick or second operator request can
//! never interleave with a check-then-write sequence.

use chrono::Utc;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;
use orch8_types::signal::{Signal, SignalType};

use crate::StorageBackend;

/// Outcome of [`retry_failed_instance`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    /// The instance was `Failed`, its run was reset and it is now `Scheduled`.
    Retried,
    /// The instance was not `Failed` when the claim CAS ran (never failed, or
    /// a concurrent retry/transition won the race). Nothing was touched.
    NotFailed,
    /// The run was reset but another actor moved the instance out of the
    /// intermediate `Paused` claim state before it could be rescheduled
    /// (e.g. an operator cancelled it mid-retry). Their transition is kept.
    Superseded,
}

/// Retry a `Failed` instance without a check-then-write race.
///
/// 1. CAS `Failed → Paused` claims the instance: only one retrier wins, and a
///    `Paused` instance is never claimed by the scheduler while its tree is
///    being wiped.
/// 2. Delete the stale execution tree, clear sentinel outputs (real outputs
///    are kept so completed side-effectful steps are skipped) and reset the
///    run identity.
/// 3. CAS `Paused → Scheduled` with an immediate fire time.
///
/// On a storage error during step 2 the claim is rolled back
/// (`Paused → Failed`) so the instance stays in the DLQ for another attempt.
pub async fn retry_failed_instance<S>(
    storage: &S,
    id: InstanceId,
) -> Result<RetryOutcome, StorageError>
where
    S: StorageBackend + ?Sized,
{
    if !storage
        .conditional_update_instance_state(id, InstanceState::Failed, InstanceState::Paused, None)
        .await?
    {
        return Ok(RetryOutcome::NotFailed);
    }

    let reset = async {
        storage.delete_execution_tree(id).await?;
        storage.delete_sentinel_block_outputs(id).await?;
        storage
            .reset_instance_run(id, &Uuid::now_v7().to_string())
            .await
    }
    .await;

    if let Err(e) = reset {
        if let Err(rollback) = storage
            .conditional_update_instance_state(
                id,
                InstanceState::Paused,
                InstanceState::Failed,
                None,
            )
            .await
        {
            tracing::warn!(
                instance_id = %id.into_uuid(),
                error = %rollback,
                "failed to roll back retry claim; instance left paused"
            );
        }
        return Err(e);
    }

    let scheduled = storage
        .conditional_update_instance_state(
            id,
            InstanceState::Paused,
            InstanceState::Scheduled,
            Some(Utc::now()),
        )
        .await?;
    Ok(if scheduled {
        RetryOutcome::Retried
    } else {
        RetryOutcome::Superseded
    })
}

/// Outcome of [`request_cancel`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelRequest {
    /// A durable `Cancel` signal was enqueued; the scheduler applies it.
    Enqueued,
    /// An undelivered `Cancel` signal was already queued: nothing to add
    /// (retried request, double click, overlapping bulk calls).
    AlreadyPending,
    /// The instance is already terminal (or gone): nothing to cancel.
    Terminal,
}

/// The one way an operator surface (REST, gRPC, bulk, jobs) cancels an
/// instance.
///
/// It must NOT flip `state` to `Cancelled` directly: that skips the
/// scheduler's cancel handling, so the instance's interrupted sagas are never
/// compensated, its `on_cancel` hooks never fire, its live worker tasks stay
/// claimable and a waiting sub-sequence parent is not woken. Instead a
/// durable `Cancel` signal is enqueued -- atomically with the target-state
/// check -- and the scheduler's existing cancel path (signal sweep for
/// paused/waiting/scheduled instances, per-instance signal pass for claimed
/// ones) performs the transition and runs compensation and hooks exactly once
/// off the request path. The signal survives restarts; repeating the request
/// is a no-op while one is pending, and a stale extra signal on an
/// already-cancelled instance is discarded by the signal processor.
///
/// Scheduled instances are re-armed so a far-future `next_fire_at` or retry
/// backoff does not delay the cancel; paused/waiting ones are picked up by
/// the scheduler's signal sweep.
pub async fn request_cancel(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
) -> Result<CancelRequest, StorageError> {
    let pending = storage.get_pending_signals(instance_id).await?;
    if pending
        .iter()
        .any(|s| s.signal_type == SignalType::Cancel && !s.delivered)
    {
        return Ok(CancelRequest::AlreadyPending);
    }
    let signal = Signal {
        id: Uuid::now_v7(),
        instance_id,
        signal_type: SignalType::Cancel,
        payload: serde_json::Value::Null,
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    match storage.enqueue_signal_if_active(&signal).await {
        Ok(()) => {}
        Err(StorageError::TerminalTarget { .. } | StorageError::NotFound { .. }) => {
            return Ok(CancelRequest::Terminal);
        }
        Err(e) => return Err(e),
    }
    // CAS on Scheduled: never resurrects an instance that moved on.
    let _ = storage
        .conditional_update_instance_state(
            instance_id,
            InstanceState::Scheduled,
            InstanceState::Scheduled,
            Some(Utc::now()),
        )
        .await;
    Ok(CancelRequest::Enqueued)
}

/// Cancel every active instance matching `filter` via [`request_cancel`].
/// Returns how many instances now have a cancel pending (newly enqueued or
/// already queued). Walks the matches with a keyset cursor, so instances the
/// scheduler cancels while this runs neither shift nor skip the page window.
pub async fn request_cancel_matching(
    storage: &dyn StorageBackend,
    filter: &orch8_types::filter::InstanceFilter,
) -> Result<u64, StorageError> {
    const PAGE: u32 = 500;
    let mut before: Option<InstanceId> = None;
    let mut count = 0u64;
    loop {
        let page = storage.list_instances_keyset(filter, before, PAGE).await?;
        let Some(last) = page.last() else { break };
        before = Some(last.id);
        let full = page.len() == PAGE as usize;
        for inst in &page {
            if request_cancel(storage, inst.id).await? != CancelRequest::Terminal {
                count += 1;
            }
        }
        if !full {
            break;
        }
    }
    Ok(count)
}

const ALL_STATES: [InstanceState; 7] = [
    InstanceState::Scheduled,
    InstanceState::Running,
    InstanceState::Waiting,
    InstanceState::Paused,
    InstanceState::Completed,
    InstanceState::Failed,
    InstanceState::Cancelled,
];

/// Source states a *bulk* state update to `target` may touch.
///
/// Bulk updates only flip the `state` column, so they are restricted to the
/// state machine's legal transitions AND exclude terminal sources: the
/// terminal → `Scheduled` edges exist for retry / resume-from-block, which
/// must wipe the execution tree first — a bare column flip would re-queue a
/// Completed/Cancelled instance or instantly re-fail a Failed one.
///
/// If `requested` is given, every requested state must be a legal source;
/// otherwise the returned list is every legal source. Returns `Err` with a
/// human-readable reason when the request can never match anything legal.
pub fn bulk_transition_sources(
    target: InstanceState,
    requested: Option<&[InstanceState]>,
) -> Result<Vec<InstanceState>, String> {
    let legal = |s: InstanceState| !s.is_terminal() && s.can_transition_to(target);
    match requested {
        Some(states) if !states.is_empty() => {
            let illegal: Vec<String> = states
                .iter()
                .filter(|s| !legal(**s))
                .map(ToString::to_string)
                .collect();
            if illegal.is_empty() {
                Ok(states.to_vec())
            } else {
                Err(format!(
                    "bulk update cannot transition [{}] to {target}",
                    illegal.join(", ")
                ))
            }
        }
        _ => {
            let sources: Vec<InstanceState> =
                ALL_STATES.into_iter().filter(|s| legal(*s)).collect();
            if sources.is_empty() {
                Err(format!("no state can be bulk-transitioned to {target}"))
            } else {
                Ok(sources)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bulk_sources_exclude_terminal_states() {
        let s = bulk_transition_sources(InstanceState::Scheduled, None).unwrap();
        assert!(s.contains(&InstanceState::Paused));
        assert!(!s.contains(&InstanceState::Completed));
        assert!(!s.contains(&InstanceState::Failed));
        assert!(!s.contains(&InstanceState::Cancelled));
    }

    #[test]
    fn bulk_sources_reject_illegal_requested_state() {
        let err = bulk_transition_sources(
            InstanceState::Scheduled,
            Some(&[InstanceState::Paused, InstanceState::Completed]),
        )
        .unwrap_err();
        assert!(err.contains("completed"), "{err}");
        assert_eq!(
            bulk_transition_sources(InstanceState::Scheduled, Some(&[InstanceState::Paused]))
                .unwrap(),
            vec![InstanceState::Paused]
        );
    }

    #[test]
    fn bulk_sources_for_cancelled_cover_active_states_only() {
        let s = bulk_transition_sources(InstanceState::Cancelled, None).unwrap();
        for st in &s {
            assert!(!st.is_terminal());
        }
        assert!(s.contains(&InstanceState::Scheduled));
    }

    async fn seed(storage: &crate::sqlite::SqliteStorage, state: InstanceState) -> InstanceId {
        use orch8_types::ids::{Namespace, SequenceId, TenantId};
        let now = Utc::now();
        let inst = orch8_types::instance::TaskInstance {
            sub_tenant: None,
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("default"),
            state,
            next_fire_at: None,
            priority: orch8_types::instance::Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({}),
            context: orch8_types::context::ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        };
        crate::InstanceStore::create_instance(storage, &inst)
            .await
            .unwrap();
        inst.id
    }

    #[tokio::test]
    async fn retry_only_reschedules_failed_instances() {
        let storage = crate::sqlite::SqliteStorage::in_memory().await.unwrap();
        let failed = seed(&storage, InstanceState::Failed).await;
        let running = seed(&storage, InstanceState::Running).await;

        assert_eq!(
            retry_failed_instance(&storage, failed).await.unwrap(),
            RetryOutcome::Retried
        );
        let inst = crate::InstanceStore::get_instance(&storage, failed)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inst.state, InstanceState::Scheduled);

        // A second (racing) retry must not touch the now-live instance.
        assert_eq!(
            retry_failed_instance(&storage, failed).await.unwrap(),
            RetryOutcome::NotFailed
        );
        assert_eq!(
            retry_failed_instance(&storage, running).await.unwrap(),
            RetryOutcome::NotFailed
        );
        let inst = crate::InstanceStore::get_instance(&storage, running)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inst.state, InstanceState::Running);
    }
}
