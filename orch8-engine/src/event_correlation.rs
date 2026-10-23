//! Durable event correlation: ingestion, matching, and wait
//! registration.
//!
//! `wait_for_event` blocks pair the built-in handler (which registers a
//! wait) with the step's `wait_for_input` gate (which parks the
//! instance). Satisfaction — whether events arrived before or after the
//! block — resumes the instance by delivering the block's
//! `human_input:<block>` signal carrying the matched event payloads,
//! which the engine stores under `context.data[<block>]`.
//!
//! Consumption is one-consumer and at-most-once: the storage layer's
//! conditional update from `pending` decides races; a matcher that loses
//! re-reads and moves on.

use std::sync::Arc;

use chrono::Utc;
use serde_json::{Value, json};
use tracing::warn;

use orch8_storage::StorageBackend;
use orch8_types::error::StorageError;
use orch8_types::event_correlation::{
    EventEnvelope, EventStatus, EventWait, IngestOutcome, WaitStatus,
};
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;
use orch8_types::signal::{Signal, SignalType};

/// Ingest one event: deduplicate by producer identity, then try to match
/// it into a waiting registration. Returns what happened.
///
/// # Errors
/// Propagates storage failures. A duplicate is NOT an error.
pub async fn ingest(
    storage: &Arc<dyn StorageBackend>,
    envelope: EventEnvelope,
) -> Result<IngestOutcome, StorageError> {
    let receipt_id = envelope.id;
    let inserted = storage.ingest_event(&envelope).await?;
    // Retry a persisted but unmatched event using its original envelope, never
    // the retry's transport UUID, correlation key or replacement payload.
    let envelope = if inserted {
        envelope
    } else {
        let canonical = storage
            .get_event_by_identity(
                &envelope.tenant_id,
                &envelope.event_name,
                &envelope.producer_event_id,
            )
            .await?
            .ok_or_else(|| {
                StorageError::Conflict("deduplicated event disappeared before lookup".into())
            })?;
        if canonical.status != EventStatus::Pending {
            return Ok(IngestOutcome {
                event_id: receipt_id,
                duplicate: true,
                matched_wait: None,
                satisfied: false,
            });
        }
        canonical
    };

    let waits = storage
        .find_waiting_event_waits(
            &envelope.tenant_id,
            &envelope.event_name,
            &envelope.correlation_key,
        )
        .await?;
    for wait in waits {
        let Some(current) = storage
            .match_event_wait(&wait, Some(envelope.id), &satisfaction_signal(&wait))
            .await?
        else {
            continue;
        };
        if !current.consumed {
            continue;
        }
        let current = current.wait;
        let satisfied = current.status == WaitStatus::Satisfied;
        if satisfied {
            wake_satisfied_wait(storage, &current).await;
        }
        return Ok(IngestOutcome {
            event_id: receipt_id,
            duplicate: !inserted,
            matched_wait: Some(current.id),
            satisfied,
        });
    }

    Ok(IngestOutcome {
        event_id: receipt_id,
        duplicate: !inserted,
        matched_wait: None,
        satisfied: false,
    })
}

/// Register (or refresh) a wait and immediately try to satisfy it from
/// events that arrived *before* the workflow reached the block. Returns
/// the wait in its post-registration state.
///
/// # Errors
/// Propagates storage failures.
pub async fn register_wait(
    storage: &Arc<dyn StorageBackend>,
    wait: EventWait,
) -> Result<EventWait, StorageError> {
    // Explicit callers retain the unclaimed storage-error API.
    register_wait_with_control(storage, storage.as_ref(), wait)
        .await
        .map_err(|error| match error {
            crate::error::EngineError::Storage(error) => error,
            other => StorageError::Query(other.to_string()),
        })
}

pub(crate) async fn register_wait_with_control<C: crate::control::ExecutionControl + ?Sized>(
    storage: &Arc<dyn StorageBackend>,
    control: &C,
    wait: EventWait,
) -> Result<EventWait, crate::error::EngineError> {
    // Native registration serializes with independent matching, adopting its
    // authoritative identity/progress before refreshing the requested params.
    let mut wait = crate::control::ExecutionControl::upsert_event_wait(control, &wait).await?;
    if wait.status == WaitStatus::Satisfied {
        return Ok(wait);
    }
    let pending = storage
        .find_pending_events(&wait.tenant_id, &wait.event_names, &wait.correlation_key)
        .await?;
    let candidates = if wait.is_satisfied() {
        vec![None]
    } else {
        pending.iter().map(|event| Some(event.id)).collect()
    };
    for event_id in candidates {
        let Some(current) = storage
            .match_event_wait(&wait, event_id, &satisfaction_signal(&wait))
            .await?
        else {
            // An independent reset replaced this activation. Observe the new
            // row without replaying this activation's candidates into it.
            return storage
                .get_event_wait(InstanceId::from_uuid(wait.instance_id), &wait.block_id)
                .await?
                .ok_or_else(|| {
                    StorageError::NotFound {
                        entity: "event_wait",
                        id: wait.block_id,
                    }
                    .into()
                });
        };
        wait = current.wait;
        if wait.status == WaitStatus::Satisfied {
            wake_satisfied_wait(storage, &wait).await;
            break;
        }
        if wait.status != WaitStatus::Waiting {
            break;
        }
    }
    Ok(wait)
}

fn satisfaction_signal(wait: &EventWait) -> Signal {
    Signal {
        // One durable notification for this exact registration generation.
        id: wait.id,
        instance_id: InstanceId::from_uuid(wait.instance_id),
        signal_type: SignalType::Custom(format!("human_input:{}", wait.block_id)),
        payload: json!({"value": "yes", "event_wait": wait.id}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    }
}

// The notification already committed with the match. A failed wake is harmless:
// the scheduler's indexed pending-signal sweep retries it on the next tick.
async fn wake_satisfied_wait(storage: &Arc<dyn StorageBackend>, wait: &EventWait) {
    let instance_id = InstanceId::from_uuid(wait.instance_id);
    if let Ok(Some(instance)) = storage.get_instance(instance_id).await
        && matches!(
            instance.state,
            InstanceState::Waiting | InstanceState::Scheduled
        )
    {
        let _ = wake_if_still_parked(storage, instance_id, instance.state).await;
    }
}

async fn wake_if_still_parked(
    storage: &Arc<dyn StorageBackend>,
    instance_id: InstanceId,
    observed_state: InstanceState,
) -> Result<bool, StorageError> {
    storage
        .conditional_update_instance_state(
            instance_id,
            observed_state,
            observed_state,
            Some(Utc::now()),
        )
        .await
}

/// Parse `wait_for_event` params into an [`EventWait`]. Shared by the
/// handler and the pre-park gate hook.
///
/// # Errors
/// Human-readable message on invalid params.
pub fn parse_wait_params(
    params: &Value,
    tenant_id: &str,
    instance_id: uuid::Uuid,
    block_id: &str,
) -> Result<EventWait, String> {
    let event_names: Vec<String> = match params.get("events") {
        Some(Value::String(s)) if !s.trim().is_empty() => vec![s.clone()],
        Some(Value::Array(items)) => {
            let names: Vec<String> = items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect();
            if names.is_empty() || names.len() != items.len() {
                return Err("'events' must be a non-empty array of strings".into());
            }
            names
        }
        _ => return Err("'events' (string or array) is required".into()),
    };
    let correlation_key = params
        .get("correlation_key")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "'correlation_key' is required and must be non-empty".to_string())?;
    let join_mode = match params.get("join").and_then(Value::as_str) {
        None | Some("all") => orch8_types::event_correlation::JoinMode::All,
        Some("any") => orch8_types::event_correlation::JoinMode::Any,
        Some("count") => {
            let count = params
                .get("count")
                .and_then(Value::as_u64)
                .and_then(|n| u32::try_from(n).ok())
                .filter(|n| *n > 0)
                .ok_or_else(|| "join 'count' requires a positive 'count'".to_string())?;
            orch8_types::event_correlation::JoinMode::Count { count }
        }
        Some(other) => return Err(format!("unknown join mode '{other}'")),
    };
    Ok(EventWait {
        id: uuid::Uuid::now_v7(),
        tenant_id: tenant_id.to_string(),
        instance_id,
        block_id: block_id.to_string(),
        event_names,
        correlation_key: correlation_key.to_string(),
        join_mode,
        status: WaitStatus::Waiting,
        matched_names: vec![],
        matched_event_ids: vec![],
        created_at: Utc::now(),
    })
}

/// Pre-park hook: the human-input gate parks a `wait_for_input` step
/// BEFORE its handler runs, so a `wait_for_event` step must register its
/// wait here or the instance would park with nothing listening.
/// Idempotent per `(instance, block)`; a satisfied registration enqueues
/// the resume signal, which the freshly-parked instance picks up on the
/// next tick. Errors are logged, never propagated — the gate must park
/// regardless, and the lint rule + stuck-instance doctor surface
/// misconfigurations.
pub async fn register_wait_before_park(
    storage: &Arc<dyn StorageBackend>,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
) {
    if let Err(error) =
        register_before_park_with_control(storage, storage.as_ref(), instance, step_def).await
    {
        warn!(instance_id = %instance.id, block_id = %step_def.id, %error,
            "pre-park registration failed");
    }
}

pub(crate) async fn register_before_park_with_control<
    C: crate::control::ExecutionControl + ?Sized,
>(
    storage: &Arc<dyn StorageBackend>,
    control: &C,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
) -> Result<(), crate::error::EngineError> {
    if step_def.handler == crate::federation::FEDERATE_HANDLER {
        return crate::federation::register_call_with_control(storage, control, instance, step_def)
            .await;
    }
    if step_def.handler != "wait_for_event" {
        return Ok(());
    }
    let resolved = crate::template::resolve(&step_def.params, &instance.context, &json!({}))
        .unwrap_or_else(|_| step_def.params.clone());
    let wait = parse_wait_params(
        &resolved,
        instance.tenant_id.as_str(),
        instance.id.into_uuid(),
        step_def.id.as_str(),
    )
    .map_err(|error| {
        crate::error::EngineError::InvalidConfig(format!("wait_for_event: {error}"))
    })?;
    register_wait_with_control(storage, control, wait).await?;
    Ok(())
}

/// Convenience constructor for a fresh envelope.
#[must_use]
pub fn envelope(
    tenant_id: &str,
    event_name: &str,
    producer_event_id: &str,
    correlation_key: &str,
    payload: Value,
) -> EventEnvelope {
    EventEnvelope {
        id: uuid::Uuid::now_v7(),
        tenant_id: tenant_id.to_string(),
        event_name: event_name.to_string(),
        producer_event_id: producer_event_id.to_string(),
        correlation_key: correlation_key.to_string(),
        payload,
        status: EventStatus::Pending,
        consumed_by: None,
        received_at: Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::ExecutionContext;
    use orch8_types::event_correlation::JoinMode;
    use orch8_types::ids::{Namespace, SequenceId, TenantId};
    use orch8_types::instance::{Priority, TaskInstance};

    async fn store() -> Arc<dyn StorageBackend> {
        Arc::new(SqliteStorage::in_memory().await.unwrap())
    }

    fn instance(state: InstanceState) -> TaskInstance {
        let now = Utc::now();
        TaskInstance {
            claimed_at: None,
            sub_tenant: None,
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t1"),
            namespace: Namespace::new("default"),
            state,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: String::new(),
            metadata: json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn wait(names: &[&str], mode: JoinMode) -> EventWait {
        EventWait {
            id: uuid::Uuid::now_v7(),
            tenant_id: "t1".into(),
            instance_id: uuid::Uuid::now_v7(),
            block_id: "wait_block".into(),
            event_names: names.iter().map(ToString::to_string).collect(),
            correlation_key: "order-1".into(),
            join_mode: mode,
            status: WaitStatus::Waiting,
            matched_names: vec![],
            matched_event_ids: vec![],
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn old_matcher_cannot_consume_for_a_new_activation() {
        let storage = store().await;
        let old = register_wait(&storage, wait(&["a"], JoinMode::Any))
            .await
            .unwrap();
        let id = InstanceId::from_uuid(old.instance_id);
        storage
            .delete_event_waits_for_blocks(id, std::slice::from_ref(&old.block_id))
            .await
            .unwrap();
        let mut fresh = wait(&["a"], JoinMode::Any);
        fresh.instance_id = old.instance_id;
        let fresh = register_wait(&storage, fresh).await.unwrap();
        let event = envelope("t1", "a", "old-producer", "order-1", json!({}));
        storage.ingest_event(&event).await.unwrap();
        assert!(
            storage
                .match_event_wait(&old, Some(event.id), &satisfaction_signal(&old))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            storage
                .get_event_wait(id, &old.block_id)
                .await
                .unwrap()
                .unwrap(),
            fresh
        );
    }

    /// T1-GAP-131: events `a` and `b` of an all-join arrive concurrently.
    /// `b`'s matcher read the wait before `a`'s match landed; persisting its
    /// stale copy used to overwrite `a`'s match (status-only CAS), leaving a
    /// consumed-but-uncounted event and a join that never completes.
    #[tokio::test]
    async fn concurrent_all_join_events_are_both_counted_and_satisfy_the_wait() {
        let s = store().await;
        let registered = register_wait(&s, wait(&["a", "b"], JoinMode::All))
            .await
            .unwrap();
        let stale = s
            .find_waiting_event_waits("t1", "b", "order-1")
            .await
            .unwrap()
            .remove(0);
        ingest(&s, envelope("t1", "a", "p-a", "order-1", json!({})))
            .await
            .unwrap();
        let b = envelope("t1", "b", "p-b", "order-1", json!({}));
        s.ingest_event(&b).await.unwrap();
        s.match_event_wait(&stale, Some(b.id), &satisfaction_signal(&stale))
            .await
            .unwrap();

        let stored = s
            .get_event_wait(InstanceId::from_uuid(registered.instance_id), "wait_block")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.matched_names, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(stored.status, WaitStatus::Satisfied);
    }

    #[tokio::test]
    async fn duplicate_producer_id_is_discarded() {
        let s = store().await;
        let first = ingest(
            &s,
            envelope("t1", "paid", "p-1", "order-1", json!({"n": 1})),
        )
        .await
        .unwrap();
        assert!(!first.duplicate);
        let second = ingest(
            &s,
            envelope("t1", "paid", "p-1", "order-1", json!({"n": 2})),
        )
        .await
        .unwrap();
        assert!(second.duplicate);
        // Only one row exists, with the first payload.
        let events = s.list_events("t1", None, 10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, json!({"n": 1}));
    }

    #[tokio::test]
    async fn stale_wake_does_not_resurrect_a_cancelled_instance() {
        let storage = store().await;
        let parked = instance(InstanceState::Waiting);
        storage.create_instance(&parked).await.unwrap();
        storage
            .update_instance_state(parked.id, InstanceState::Cancelled, None)
            .await
            .unwrap();

        assert!(
            !wake_if_still_parked(&storage, parked.id, InstanceState::Waiting)
                .await
                .unwrap()
        );
        let stored = storage.get_instance(parked.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Cancelled);
    }

    #[tokio::test]
    async fn same_producer_id_different_tenant_is_not_a_duplicate() {
        let s = store().await;
        assert!(
            !ingest(&s, envelope("t1", "paid", "p-1", "o", json!({})))
                .await
                .unwrap()
                .duplicate
        );
        assert!(
            !ingest(&s, envelope("t2", "paid", "p-1", "o", json!({})))
                .await
                .unwrap()
                .duplicate
        );
    }

    #[tokio::test]
    async fn wait_before_event_matches_and_satisfies() {
        let s = store().await;
        let w = register_wait(&s, wait(&["paid"], JoinMode::Any))
            .await
            .unwrap();
        assert_eq!(w.status, WaitStatus::Waiting);

        let outcome = ingest(
            &s,
            envelope("t1", "paid", "p-1", "order-1", json!({"amt": 5})),
        )
        .await
        .unwrap();
        assert_eq!(outcome.matched_wait, Some(w.id));
        assert!(outcome.satisfied);

        // The event is consumed, attributed to the instance.
        let events = s.list_events("t1", None, 10).await.unwrap();
        assert_eq!(events[0].status, EventStatus::Consumed);
        assert_eq!(events[0].consumed_by, Some(w.instance_id));

        // A satisfaction signal is enqueued... but the instance row does
        // not exist in this storage-only test, so enqueue-if-active
        // rejected it gracefully. The wait itself is satisfied:
        let stored = s
            .get_event_wait(InstanceId::from_uuid(w.instance_id), "wait_block")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, WaitStatus::Satisfied);
    }

    #[tokio::test]
    async fn event_before_wait_is_drained_at_registration() {
        let s = store().await;
        ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
            .await
            .unwrap();
        let w = register_wait(&s, wait(&["paid"], JoinMode::Any))
            .await
            .unwrap();
        assert_eq!(w.status, WaitStatus::Satisfied);
        let events = s.list_events("t1", None, 10).await.unwrap();
        assert_eq!(events[0].status, EventStatus::Consumed);
    }

    #[tokio::test]
    async fn all_join_resumes_only_after_both_in_either_order() {
        let s = store().await;
        // First event arrives before the wait...
        ingest(
            &s,
            envelope("t1", "inventory_reserved", "i-1", "order-1", json!({})),
        )
        .await
        .unwrap();
        let w = register_wait(
            &s,
            wait(&["payment_received", "inventory_reserved"], JoinMode::All),
        )
        .await
        .unwrap();
        // ...one of two matched: still waiting.
        assert_eq!(w.status, WaitStatus::Waiting);
        assert_eq!(w.matched_names, vec!["inventory_reserved"]);

        // ...second arrives after: satisfied.
        let outcome = ingest(
            &s,
            envelope("t1", "payment_received", "pay-1", "order-1", json!({})),
        )
        .await
        .unwrap();
        assert!(outcome.satisfied);
    }

    #[tokio::test]
    async fn count_join_needs_n_events() {
        let s = store().await;
        let w = register_wait(&s, wait(&["reading"], JoinMode::Count { count: 2 }))
            .await
            .unwrap();
        let first = ingest(&s, envelope("t1", "reading", "r-1", "order-1", json!({})))
            .await
            .unwrap();
        assert!(!first.satisfied);
        let second = ingest(&s, envelope("t1", "reading", "r-2", "order-1", json!({})))
            .await
            .unwrap();
        assert!(second.satisfied);
        let _ = w;
    }

    #[tokio::test]
    async fn correlation_key_isolates_orders() {
        let s = store().await;
        let w = register_wait(&s, wait(&["paid"], JoinMode::Any))
            .await
            .unwrap();
        // Same event name, different order: must not match.
        let outcome = ingest(&s, envelope("t1", "paid", "p-9", "order-OTHER", json!({})))
            .await
            .unwrap();
        assert!(outcome.matched_wait.is_none());
        let stored = s
            .get_event_wait(InstanceId::from_uuid(w.instance_id), "wait_block")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, WaitStatus::Waiting);
    }

    #[tokio::test]
    async fn tenant_isolation_never_crosses() {
        let s = store().await;
        register_wait(&s, wait(&["paid"], JoinMode::Any))
            .await
            .unwrap();
        // Same name + key, wrong tenant.
        let outcome = ingest(&s, envelope("t2", "paid", "p-1", "order-1", json!({})))
            .await
            .unwrap();
        assert!(outcome.matched_wait.is_none());
    }

    #[tokio::test]
    async fn one_event_consumed_by_at_most_one_wait() {
        let s = store().await;
        let w1 = register_wait(&s, wait(&["paid"], JoinMode::Any))
            .await
            .unwrap();
        let mut second = wait(&["paid"], JoinMode::Any);
        second.instance_id = uuid::Uuid::now_v7();
        second.block_id = "other_block".into();
        let w2 = register_wait(&s, second).await.unwrap();

        let outcome = ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
            .await
            .unwrap();
        // Exactly one wait matched (the oldest).
        assert_eq!(outcome.matched_wait, Some(w1.id));
        let stored2 = s
            .get_event_wait(InstanceId::from_uuid(w2.instance_id), "other_block")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored2.status, WaitStatus::Waiting);
    }

    #[tokio::test]
    async fn satisfied_wait_survives_re_registration() {
        let s = store().await;
        let w = wait(&["paid"], JoinMode::Any);
        ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
            .await
            .unwrap();
        let first = register_wait(&s, w.clone()).await.unwrap();
        assert_eq!(first.status, WaitStatus::Satisfied);
        // Handler re-execution (retry) must keep the satisfied result.
        let again = register_wait(&s, w).await.unwrap();
        assert_eq!(again.status, WaitStatus::Satisfied);
    }

    #[tokio::test]
    async fn expiry_only_touches_pending() {
        let s = store().await;
        ingest(&s, envelope("t1", "old", "o-1", "k", json!({})))
            .await
            .unwrap();
        register_wait(&s, wait(&["paid"], JoinMode::Any))
            .await
            .unwrap();
        ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
            .await
            .unwrap();

        let expired = s
            .expire_events_before(Utc::now() + chrono::Duration::hours(1))
            .await
            .unwrap();
        // The consumed event is untouched; only the pending one expires.
        assert_eq!(expired, 1);
        let events = s
            .list_events("t1", Some(EventStatus::Expired), 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_name, "old");
    }
}
