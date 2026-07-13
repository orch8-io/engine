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
use tracing::{debug, warn};

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
    let inserted = storage.ingest_event(&envelope).await?;
    if !inserted {
        return Ok(IngestOutcome {
            event_id: envelope.id,
            duplicate: true,
            matched_wait: None,
            satisfied: false,
        });
    }

    let waits = storage
        .find_waiting_event_waits(
            &envelope.tenant_id,
            &envelope.event_name,
            &envelope.correlation_key,
        )
        .await?;
    for mut wait in waits {
        if !wait.record_match(&envelope) {
            continue;
        }
        // At-most-once: claim the event for this wait's instance first.
        let consumed = storage
            .consume_events(&[envelope.id], InstanceId::from_uuid(wait.instance_id))
            .await?;
        if consumed == 0 {
            // A concurrent matcher won the event.
            return Ok(IngestOutcome {
                event_id: envelope.id,
                duplicate: false,
                matched_wait: None,
                satisfied: false,
            });
        }
        let satisfied = wait.is_satisfied();
        if satisfied {
            wait.status = WaitStatus::Satisfied;
        }
        if !storage
            .update_event_wait(&wait, WaitStatus::Waiting)
            .await?
        {
            // The wait moved concurrently (cancelled / satisfied by a
            // sibling event). The event stays consumed and attributed —
            // log rather than un-consume: replaying it elsewhere would
            // break at-most-once.
            warn!(wait_id = %wait.id, event = %envelope.event_name,
                "event consumed but wait moved concurrently");
            return Ok(IngestOutcome {
                event_id: envelope.id,
                duplicate: false,
                matched_wait: Some(wait.id),
                satisfied: false,
            });
        }
        if satisfied {
            resume_waiting_instance(storage, &wait).await?;
        }
        return Ok(IngestOutcome {
            event_id: envelope.id,
            duplicate: false,
            matched_wait: Some(wait.id),
            satisfied,
        });
    }

    Ok(IngestOutcome {
        event_id: envelope.id,
        duplicate: false,
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
    mut wait: EventWait,
) -> Result<EventWait, StorageError> {
    // Idempotent re-execution: registration fires on every gate re-park
    // (any signal can wake and re-park the instance) and again when the
    // handler finally runs, each time with a freshly generated id. The
    // stored row's identity and accumulated matches MUST survive that:
    // the upsert keeps the original row id on conflict, so a wait that
    // didn't adopt it would CAS against the wrong id, and resetting
    // matched progress would strand already-consumed events, leaving an
    // all-join that can never complete.
    if let Some(existing) = storage
        .get_event_wait(InstanceId::from_uuid(wait.instance_id), &wait.block_id)
        .await?
    {
        if existing.status == WaitStatus::Satisfied {
            return Ok(existing);
        }
        wait.id = existing.id;
        wait.matched_names = existing.matched_names;
        wait.matched_event_ids = existing.matched_event_ids;
    }
    storage.upsert_event_wait(&wait).await?;

    // Event-before-wait: drain matching pending events.
    let pending = storage
        .find_pending_events(&wait.tenant_id, &wait.event_names, &wait.correlation_key)
        .await?;
    for event in pending {
        if wait.is_satisfied() {
            break;
        }
        if !wait.record_match(&event) {
            continue;
        }
        let consumed = storage
            .consume_events(&[event.id], InstanceId::from_uuid(wait.instance_id))
            .await?;
        if consumed == 0 {
            // Lost the race for this event — roll the local match back.
            wait.matched_event_ids.retain(|id| *id != event.id);
            wait.matched_names.retain(|n| n != &event.event_name);
        }
    }
    if wait.is_satisfied() {
        wait.status = WaitStatus::Satisfied;
    }
    if !storage
        .update_event_wait(&wait, WaitStatus::Waiting)
        .await?
    {
        // Concurrent ingestion satisfied it between upsert and update —
        // re-read the authoritative row.
        if let Some(fresh) = storage
            .get_event_wait(InstanceId::from_uuid(wait.instance_id), &wait.block_id)
            .await?
        {
            return Ok(fresh);
        }
    }
    if wait.status == WaitStatus::Satisfied {
        resume_waiting_instance(storage, &wait).await?;
    }
    Ok(wait)
}

/// Deliver the satisfaction signal that opens the block's
/// `wait_for_input` gate. The gate strictly validates the payload
/// against the step's choice list, so the signal carries the default
/// `yes` choice — `wait_for_event` steps must not define custom
/// `choices` (linted). The matched event payloads are returned by the
/// handler as the block's *output* once the gate opens.
async fn resume_waiting_instance(
    storage: &Arc<dyn StorageBackend>,
    wait: &EventWait,
) -> Result<(), StorageError> {
    let signal = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: InstanceId::from_uuid(wait.instance_id),
        signal_type: SignalType::Custom(format!("human_input:{}", wait.block_id)),
        payload: json!({"value": "yes", "event_wait": wait.id}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    match storage.enqueue_signal_if_active(&signal).await {
        Ok(()) => {}
        Err(StorageError::TerminalTarget { .. } | StorageError::NotFound { .. }) => {
            // The instance finished or vanished — nothing to resume.
            debug!(instance_id = %wait.instance_id, "event wait satisfied for terminal instance");
            return Ok(());
        }
        Err(e) => return Err(e),
    }

    // Wake a parked instance so the signal is handled promptly.
    let instance_id = InstanceId::from_uuid(wait.instance_id);
    if let Ok(Some(instance)) = storage.get_instance(instance_id).await
        && matches!(
            instance.state,
            InstanceState::Waiting | InstanceState::Scheduled
        )
    {
        let _ = storage
            .update_instance_state(instance_id, instance.state, Some(Utc::now()))
            .await;
    }
    Ok(())
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
    if step_def.handler != "wait_for_event" {
        return;
    }
    // Resolve templates in params against the instance context (prior
    // step outputs are intentionally not available at the gate — use
    // context data for correlation keys).
    let resolved = crate::template::resolve(&step_def.params, &instance.context, &json!({}))
        .unwrap_or_else(|_| step_def.params.clone());
    match parse_wait_params(
        &resolved,
        instance.tenant_id.as_str(),
        instance.id.into_uuid(),
        step_def.id.as_str(),
    ) {
        Ok(wait) => {
            if let Err(e) = register_wait(storage, wait).await {
                warn!(
                    instance_id = %instance.id,
                    block_id = %step_def.id,
                    error = %e,
                    "wait_for_event: pre-park registration failed"
                );
            }
        }
        Err(e) => {
            warn!(
                instance_id = %instance.id,
                block_id = %step_def.id,
                error = %e,
                "wait_for_event: invalid params at gate — instance will park without a wait"
            );
        }
    }
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
    use orch8_types::event_correlation::JoinMode;

    async fn store() -> Arc<dyn StorageBackend> {
        Arc::new(SqliteStorage::in_memory().await.unwrap())
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
