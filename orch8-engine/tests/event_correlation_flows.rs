//! End-to-end tests for the durable event correlation service layer:
//! `ingest` / `register_wait` / `parse_wait_params` / `envelope` driven
//! against a real `SQLite` in-memory `StorageBackend`.
//!
//! This IS the full ingestion pipeline: producer-id dedup, wait matching,
//! at-most-once consumption, join satisfaction, and resume-signal
//! delivery to a parked instance.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use serde_json::json;
use uuid::Uuid;

use orch8_engine::event_correlation::{envelope, ingest, parse_wait_params, register_wait};
use orch8_storage::StorageBackend;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::context::ExecutionContext;
use orch8_types::event_correlation::{EventStatus, EventWait, JoinMode, WaitStatus};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, SequenceStatus, StepDef};
use orch8_types::signal::SignalType;

// ============================================================================
// Fixtures
// ============================================================================

async fn store() -> Arc<dyn StorageBackend> {
    Arc::new(SqliteStorage::in_memory().await.unwrap())
}

fn wait(names: &[&str], mode: JoinMode) -> EventWait {
    EventWait {
        id: Uuid::now_v7(),
        tenant_id: "t1".into(),
        instance_id: Uuid::now_v7(),
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

/// Create a sequence + a parked (Waiting) instance so satisfaction can
/// enqueue a real resume signal.
async fn create_parked_instance(
    storage: &Arc<dyn StorageBackend>,
    tenant: &str,
    state: InstanceState,
) -> InstanceId {
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked(tenant),
        namespace: Namespace::new("ns"),
        name: format!("evt-seq-{}", Uuid::now_v7()),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::Step(Box::new(StepDef {
            id: BlockId::new("wait_block"),
            handler: "wait_for_event".into(),
            params: json!({}),
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
            output_schema: None,
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&seq).await.unwrap();

    let inst = TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq.id,
        tenant_id: TenantId::unchecked(tenant),
        namespace: Namespace::new("ns"),
        state,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    storage.create_instance(&inst).await.unwrap();
    inst.id
}

// ============================================================================
// Producer-id deduplication
// ============================================================================

#[tokio::test]
async fn dedup_same_tenant_name_producer_is_duplicate() {
    let s = store().await;
    let first = ingest(&s, envelope("t1", "paid", "p-1", "k", json!({})))
        .await
        .unwrap();
    assert!(!first.duplicate);
    let second = ingest(&s, envelope("t1", "paid", "p-1", "k", json!({})))
        .await
        .unwrap();
    assert!(second.duplicate);
}

#[tokio::test]
async fn dedup_keeps_first_payload_and_single_row() {
    let s = store().await;
    ingest(&s, envelope("t1", "paid", "p-1", "k", json!({"n": 1})))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "paid", "p-1", "k", json!({"n": 2})))
        .await
        .unwrap();
    let events = s.list_events("t1", None, 10).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload, json!({"n": 1}));
}

#[tokio::test]
async fn dedup_duplicate_outcome_reports_nothing_matched() {
    let s = store().await;
    register_wait(&s, wait(&["paid"], JoinMode::Count { count: 2 }))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    let dup_env = envelope("t1", "paid", "p-1", "order-1", json!({}));
    let dup_id = dup_env.id;
    let outcome = ingest(&s, dup_env).await.unwrap();
    assert!(outcome.duplicate);
    assert_eq!(outcome.matched_wait, None);
    assert!(!outcome.satisfied);
    // The outcome carries the rejected envelope's id, not the stored one.
    assert_eq!(outcome.event_id, dup_id);
}

#[tokio::test]
async fn dedup_different_tenant_same_producer_id_is_not_duplicate() {
    let s = store().await;
    assert!(
        !ingest(&s, envelope("t1", "paid", "p-1", "k", json!({})))
            .await
            .unwrap()
            .duplicate
    );
    assert!(
        !ingest(&s, envelope("t2", "paid", "p-1", "k", json!({})))
            .await
            .unwrap()
            .duplicate
    );
}

#[tokio::test]
async fn dedup_different_event_name_same_producer_id_is_not_duplicate() {
    let s = store().await;
    assert!(
        !ingest(&s, envelope("t1", "paid", "p-1", "k", json!({})))
            .await
            .unwrap()
            .duplicate
    );
    assert!(
        !ingest(&s, envelope("t1", "shipped", "p-1", "k", json!({})))
            .await
            .unwrap()
            .duplicate
    );
    assert_eq!(s.list_events("t1", None, 10).await.unwrap().len(), 2);
}

#[tokio::test]
async fn dedup_different_producer_id_creates_second_row() {
    let s = store().await;
    ingest(&s, envelope("t1", "paid", "p-1", "k", json!({})))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "paid", "p-2", "k", json!({})))
        .await
        .unwrap();
    assert_eq!(s.list_events("t1", None, 10).await.unwrap().len(), 2);
}

#[tokio::test]
async fn dedup_replay_does_not_advance_count_join() {
    let s = store().await;
    let w = register_wait(&s, wait(&["paid"], JoinMode::Count { count: 2 }))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    // Producer retry: same producer id again.
    let dup = ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    assert!(dup.duplicate && !dup.satisfied);
    let stored = s
        .get_event_wait(InstanceId::from_uuid(w.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, WaitStatus::Waiting);
    assert_eq!(stored.matched_event_ids.len(), 1);
}

// ============================================================================
// Wait-before-event and event-before-wait, per join mode
// ============================================================================

#[tokio::test]
async fn any_wait_before_event_satisfies() {
    let s = store().await;
    let w = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    assert_eq!(w.status, WaitStatus::Waiting);
    let outcome = ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    assert_eq!(outcome.matched_wait, Some(w.id));
    assert!(outcome.satisfied);
    let events = s.list_events("t1", None, 10).await.unwrap();
    assert_eq!(events[0].status, EventStatus::Consumed);
    assert_eq!(events[0].consumed_by, Some(w.instance_id));
}

#[tokio::test]
async fn any_event_before_wait_satisfies_at_registration() {
    let s = store().await;
    ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    let w = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    assert_eq!(w.status, WaitStatus::Satisfied);
    assert_eq!(w.matched_names, vec!["paid".to_string()]);
    let events = s.list_events("t1", None, 10).await.unwrap();
    assert_eq!(events[0].status, EventStatus::Consumed);
}

#[tokio::test]
async fn any_multi_name_wait_satisfied_by_either_name() {
    for name in ["a", "b"] {
        let s = store().await;
        let w = register_wait(&s, wait(&["a", "b"], JoinMode::Any))
            .await
            .unwrap();
        let outcome = ingest(&s, envelope("t1", name, "p-1", "order-1", json!({})))
            .await
            .unwrap();
        assert_eq!(outcome.matched_wait, Some(w.id), "name {name}");
        assert!(outcome.satisfied, "name {name}");
    }
}

#[tokio::test]
async fn all_wait_before_events_order_a_then_b() {
    let s = store().await;
    register_wait(&s, wait(&["a", "b"], JoinMode::All))
        .await
        .unwrap();
    let first = ingest(&s, envelope("t1", "a", "p-a", "order-1", json!({})))
        .await
        .unwrap();
    assert!(!first.satisfied);
    assert!(first.matched_wait.is_some());
    let second = ingest(&s, envelope("t1", "b", "p-b", "order-1", json!({})))
        .await
        .unwrap();
    assert!(second.satisfied);
}

#[tokio::test]
async fn all_wait_before_events_order_b_then_a() {
    let s = store().await;
    register_wait(&s, wait(&["a", "b"], JoinMode::All))
        .await
        .unwrap();
    let first = ingest(&s, envelope("t1", "b", "p-b", "order-1", json!({})))
        .await
        .unwrap();
    assert!(!first.satisfied);
    let second = ingest(&s, envelope("t1", "a", "p-a", "order-1", json!({})))
        .await
        .unwrap();
    assert!(second.satisfied);
}

#[tokio::test]
async fn all_both_events_before_wait_satisfies_at_registration() {
    let s = store().await;
    ingest(&s, envelope("t1", "a", "p-a", "order-1", json!({})))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "b", "p-b", "order-1", json!({})))
        .await
        .unwrap();
    let w = register_wait(&s, wait(&["a", "b"], JoinMode::All))
        .await
        .unwrap();
    assert_eq!(w.status, WaitStatus::Satisfied);
    let consumed = s
        .list_events("t1", Some(EventStatus::Consumed), 10)
        .await
        .unwrap();
    assert_eq!(consumed.len(), 2);
}

#[tokio::test]
async fn all_mixed_a_pre_b_post() {
    let s = store().await;
    ingest(&s, envelope("t1", "a", "p-a", "order-1", json!({})))
        .await
        .unwrap();
    let w = register_wait(&s, wait(&["a", "b"], JoinMode::All))
        .await
        .unwrap();
    assert_eq!(w.status, WaitStatus::Waiting);
    assert_eq!(w.matched_names, vec!["a".to_string()]);
    let outcome = ingest(&s, envelope("t1", "b", "p-b", "order-1", json!({})))
        .await
        .unwrap();
    assert!(outcome.satisfied);
}

#[tokio::test]
async fn all_mixed_b_pre_a_post() {
    let s = store().await;
    ingest(&s, envelope("t1", "b", "p-b", "order-1", json!({})))
        .await
        .unwrap();
    let w = register_wait(&s, wait(&["a", "b"], JoinMode::All))
        .await
        .unwrap();
    assert_eq!(w.status, WaitStatus::Waiting);
    assert_eq!(w.matched_names, vec!["b".to_string()]);
    let outcome = ingest(&s, envelope("t1", "a", "p-a", "order-1", json!({})))
        .await
        .unwrap();
    assert!(outcome.satisfied);
}

#[tokio::test]
async fn all_duplicate_second_event_of_same_name_stays_pending() {
    let s = store().await;
    register_wait(&s, wait(&["a", "b"], JoinMode::All))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "a", "p-a1", "order-1", json!({})))
        .await
        .unwrap();
    // A second 'a' adds nothing to the all-join: it must stay pending.
    let outcome = ingest(&s, envelope("t1", "a", "p-a2", "order-1", json!({})))
        .await
        .unwrap();
    assert!(!outcome.satisfied);
    assert_eq!(outcome.matched_wait, None);
    let pending = s
        .list_events("t1", Some(EventStatus::Pending), 10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].producer_event_id, "p-a2");
}

#[tokio::test]
async fn count_pre_and_post_events_accumulate() {
    let s = store().await;
    ingest(&s, envelope("t1", "reading", "r-1", "order-1", json!({})))
        .await
        .unwrap();
    let w = register_wait(&s, wait(&["reading"], JoinMode::Count { count: 2 }))
        .await
        .unwrap();
    // One pre-registration event drained: still waiting.
    assert_eq!(w.status, WaitStatus::Waiting);
    assert_eq!(w.matched_event_ids.len(), 1);
    let outcome = ingest(&s, envelope("t1", "reading", "r-2", "order-1", json!({})))
        .await
        .unwrap();
    assert!(outcome.satisfied);
}

#[tokio::test]
async fn count_all_events_pre_registration() {
    let s = store().await;
    ingest(&s, envelope("t1", "reading", "r-1", "order-1", json!({})))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "reading", "r-2", "order-1", json!({})))
        .await
        .unwrap();
    let w = register_wait(&s, wait(&["reading"], JoinMode::Count { count: 2 }))
        .await
        .unwrap();
    assert_eq!(w.status, WaitStatus::Satisfied);
    assert_eq!(w.matched_event_ids.len(), 2);
}

#[tokio::test]
async fn count_drain_stops_at_satisfaction_leaving_excess_pending() {
    let s = store().await;
    for i in 0..3 {
        ingest(
            &s,
            envelope("t1", "reading", &format!("r-{i}"), "order-1", json!({})),
        )
        .await
        .unwrap();
    }
    let w = register_wait(&s, wait(&["reading"], JoinMode::Count { count: 2 }))
        .await
        .unwrap();
    assert_eq!(w.status, WaitStatus::Satisfied);
    // Only 2 of 3 consumed; the third stays available for others.
    let pending = s
        .list_events("t1", Some(EventStatus::Pending), 10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
}

#[tokio::test]
async fn count_one_behaves_like_any() {
    let s = store().await;
    register_wait(&s, wait(&["paid"], JoinMode::Count { count: 1 }))
        .await
        .unwrap();
    let outcome = ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    assert!(outcome.satisfied);
}

#[tokio::test]
async fn count_three_not_satisfied_by_two() {
    let s = store().await;
    let w = register_wait(&s, wait(&["r"], JoinMode::Count { count: 3 }))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "r", "r-1", "order-1", json!({})))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "r", "r-2", "order-1", json!({})))
        .await
        .unwrap();
    let stored = s
        .get_event_wait(InstanceId::from_uuid(w.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, WaitStatus::Waiting);
    assert_eq!(stored.matched_event_ids.len(), 2);
    let third = ingest(&s, envelope("t1", "r", "r-3", "order-1", json!({})))
        .await
        .unwrap();
    assert!(third.satisfied);
}

// ============================================================================
// Correlation-key and tenant isolation
// ============================================================================

#[tokio::test]
async fn correlation_key_isolation_wait_untouched() {
    let s = store().await;
    let w = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    let outcome = ingest(&s, envelope("t1", "paid", "p-9", "order-OTHER", json!({})))
        .await
        .unwrap();
    assert_eq!(outcome.matched_wait, None);
    assert!(!outcome.satisfied);
    let stored = s
        .get_event_wait(InstanceId::from_uuid(w.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, WaitStatus::Waiting);
}

#[tokio::test]
async fn correlation_key_isolation_event_stays_pending() {
    let s = store().await;
    register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "paid", "p-9", "order-OTHER", json!({})))
        .await
        .unwrap();
    let pending = s
        .list_events("t1", Some(EventStatus::Pending), 10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].correlation_key, "order-OTHER");
}

#[tokio::test]
async fn correlation_key_isolation_at_registration_drain() {
    let s = store().await;
    ingest(&s, envelope("t1", "paid", "p-9", "order-OTHER", json!({})))
        .await
        .unwrap();
    let w = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    assert_eq!(w.status, WaitStatus::Waiting);
}

#[tokio::test]
async fn tenant_isolation_on_ingest() {
    let s = store().await;
    let w = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    let outcome = ingest(&s, envelope("t2", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    assert_eq!(outcome.matched_wait, None);
    let stored = s
        .get_event_wait(InstanceId::from_uuid(w.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, WaitStatus::Waiting);
}

#[tokio::test]
async fn tenant_isolation_at_registration_drain() {
    let s = store().await;
    ingest(&s, envelope("t2", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    // Wait in t1 must not drain t2's event.
    let w = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    assert_eq!(w.status, WaitStatus::Waiting);
    let t2_pending = s
        .list_events("t2", Some(EventStatus::Pending), 10)
        .await
        .unwrap();
    assert_eq!(t2_pending.len(), 1);
}

#[tokio::test]
async fn same_key_different_tenants_both_satisfiable() {
    let s = store().await;
    let w1 = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    let mut other = wait(&["paid"], JoinMode::Any);
    other.tenant_id = "t2".into();
    let w2 = register_wait(&s, other).await.unwrap();

    let o1 = ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    let o2 = ingest(&s, envelope("t2", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    assert_eq!(o1.matched_wait, Some(w1.id));
    assert_eq!(o2.matched_wait, Some(w2.id));
    assert!(o1.satisfied && o2.satisfied);
}

// ============================================================================
// One-consumer competition
// ============================================================================

#[tokio::test]
async fn one_consumer_oldest_wait_wins() {
    let s = store().await;
    let mut first = wait(&["paid"], JoinMode::Any);
    first.created_at = Utc::now() - chrono::Duration::seconds(10);
    let w1 = register_wait(&s, first).await.unwrap();

    let mut second = wait(&["paid"], JoinMode::Any);
    second.block_id = "other_block".into();
    let w2 = register_wait(&s, second).await.unwrap();

    let outcome = ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    assert_eq!(outcome.matched_wait, Some(w1.id));
    let stored2 = s
        .get_event_wait(InstanceId::from_uuid(w2.instance_id), "other_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored2.status, WaitStatus::Waiting);
}

#[tokio::test]
async fn second_event_satisfies_second_wait() {
    let s = store().await;
    let mut first = wait(&["paid"], JoinMode::Any);
    first.created_at = Utc::now() - chrono::Duration::seconds(10);
    let w1 = register_wait(&s, first).await.unwrap();
    let mut second = wait(&["paid"], JoinMode::Any);
    second.block_id = "other_block".into();
    let w2 = register_wait(&s, second).await.unwrap();

    let o1 = ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    assert_eq!(o1.matched_wait, Some(w1.id));
    let o2 = ingest(&s, envelope("t1", "paid", "p-2", "order-1", json!({})))
        .await
        .unwrap();
    assert_eq!(o2.matched_wait, Some(w2.id));
    assert!(o2.satisfied);
    // Each event is attributed to its own consumer.
    let consumed = s
        .list_events("t1", Some(EventStatus::Consumed), 10)
        .await
        .unwrap();
    assert_eq!(consumed.len(), 2);
    let consumers: Vec<Option<Uuid>> = consumed.iter().map(|e| e.consumed_by).collect();
    assert!(consumers.contains(&Some(w1.instance_id)));
    assert!(consumers.contains(&Some(w2.instance_id)));
}

#[tokio::test]
async fn consumed_event_not_drained_by_later_registration() {
    let s = store().await;
    let w1 = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    // The event is consumed by w1; a new wait cannot see it.
    let mut second = wait(&["paid"], JoinMode::Any);
    second.block_id = "other_block".into();
    let w2 = register_wait(&s, second).await.unwrap();
    assert_eq!(w2.status, WaitStatus::Waiting);
    let _ = w1;
}

#[tokio::test]
async fn cancelled_wait_is_never_matched() {
    let s = store().await;
    let mut w = wait(&["paid"], JoinMode::Any);
    w.status = WaitStatus::Cancelled;
    s.upsert_event_wait(&w).await.unwrap();
    let outcome = ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    assert_eq!(outcome.matched_wait, None);
    let pending = s
        .list_events("t1", Some(EventStatus::Pending), 10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
}

// ============================================================================
// Registration semantics (idempotency, upsert refresh)
// ============================================================================

#[tokio::test]
async fn satisfied_wait_survives_idempotent_reregistration() {
    let s = store().await;
    let w = wait(&["paid"], JoinMode::Any);
    ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    let first = register_wait(&s, w.clone()).await.unwrap();
    assert_eq!(first.status, WaitStatus::Satisfied);
    let again = register_wait(&s, w).await.unwrap();
    assert_eq!(again.status, WaitStatus::Satisfied);
    assert_eq!(again.matched_event_ids, first.matched_event_ids);
}

#[tokio::test]
async fn satisfied_reregistration_with_new_wait_object_keeps_result() {
    let s = store().await;
    let original = wait(&["paid"], JoinMode::Any);
    ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    let first = register_wait(&s, original.clone()).await.unwrap();
    assert_eq!(first.status, WaitStatus::Satisfied);

    // Retry builds a brand-new wait (new id) for the same (instance, block).
    let mut retry = wait(&["paid"], JoinMode::Any);
    retry.instance_id = original.instance_id;
    retry.block_id = original.block_id.clone();
    let again = register_wait(&s, retry).await.unwrap();
    assert_eq!(again.status, WaitStatus::Satisfied);
    assert_eq!(again.id, first.id);
}

#[tokio::test]
async fn reregistration_while_waiting_refreshes_names() {
    let s = store().await;
    let original = wait(&["a"], JoinMode::Any);
    let w1 = register_wait(&s, original.clone()).await.unwrap();
    assert_eq!(w1.status, WaitStatus::Waiting);

    // Re-register the same (instance, block) listening for a different name.
    let mut refreshed = wait(&["b"], JoinMode::Any);
    refreshed.instance_id = original.instance_id;
    refreshed.block_id = original.block_id.clone();
    register_wait(&s, refreshed).await.unwrap();

    // Upsert semantics: the stored row keeps the ORIGINAL wait id but
    // carries the refreshed name list.
    let stored = s
        .get_event_wait(InstanceId::from_uuid(original.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.id, w1.id);
    assert_eq!(stored.event_names, vec!["b".to_string()]);

    // The old name no longer matches; the new one satisfies.
    let old = ingest(&s, envelope("t1", "a", "p-a", "order-1", json!({})))
        .await
        .unwrap();
    assert_eq!(old.matched_wait, None);
    let new = ingest(&s, envelope("t1", "b", "p-b", "order-1", json!({})))
        .await
        .unwrap();
    assert_eq!(new.matched_wait, Some(w1.id));
    assert!(new.satisfied);
}

#[tokio::test]
async fn reregistration_while_waiting_refreshes_join_mode() {
    let s = store().await;
    let original = wait(&["r"], JoinMode::Any);
    register_wait(&s, original.clone()).await.unwrap();

    let mut refreshed = wait(&["r"], JoinMode::Count { count: 2 });
    refreshed.instance_id = original.instance_id;
    refreshed.block_id = original.block_id.clone();
    register_wait(&s, refreshed).await.unwrap();

    // One event is no longer enough under the refreshed count join.
    let first = ingest(&s, envelope("t1", "r", "r-1", "order-1", json!({})))
        .await
        .unwrap();
    assert!(first.matched_wait.is_some());
    assert!(!first.satisfied);
    let second = ingest(&s, envelope("t1", "r", "r-2", "order-1", json!({})))
        .await
        .unwrap();
    assert!(second.satisfied);
}

#[tokio::test]
async fn reregistration_preserves_matched_progress() {
    // Re-registration happens on every gate re-park (any signal can wake
    // and re-park the instance) — accumulated matches must survive it,
    // otherwise already-consumed events would be stranded and an
    // all-join could never complete.
    let s = store().await;
    let original = wait(&["a", "b"], JoinMode::All);
    register_wait(&s, original.clone()).await.unwrap();
    ingest(&s, envelope("t1", "a", "p-a", "order-1", json!({})))
        .await
        .unwrap();
    let mid = s
        .get_event_wait(InstanceId::from_uuid(original.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mid.matched_names, vec!["a".to_string()]);

    // Re-register with a fresh wait object (new id, empty matches):
    // the stored identity AND the matched progress are preserved.
    let mut fresh = wait(&["a", "b"], JoinMode::All);
    fresh.instance_id = original.instance_id;
    fresh.block_id = original.block_id.clone();
    let returned = register_wait(&s, fresh).await.unwrap();
    assert_eq!(returned.id, mid.id, "row identity survives re-registration");
    assert_eq!(returned.matched_names, vec!["a".to_string()]);
    let stored = s
        .get_event_wait(InstanceId::from_uuid(original.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, WaitStatus::Waiting);
    assert_eq!(stored.matched_names, vec!["a".to_string()]);

    // Only the still-missing name is required to satisfy the join.
    let done = ingest(&s, envelope("t1", "b", "p-b", "order-1", json!({})))
        .await
        .unwrap();
    assert!(done.satisfied);
}

#[tokio::test]
async fn reregistration_with_pending_event_adopts_row_identity_and_satisfies() {
    // Regression test: re-registering with a NEW wait id used to consume a
    // matching pending event and then CAS against the wrong row id — the
    // stored wait stayed 'waiting' forever with the event stranded.
    // register_wait now adopts the existing row's identity, so the drain
    // completes the join.
    let s = store().await;
    let original = wait(&["a"], JoinMode::Any);
    let w1 = register_wait(&s, original.clone()).await.unwrap();
    assert_eq!(w1.status, WaitStatus::Waiting);

    // Event that the CURRENT registration ignores; stays pending.
    ingest(&s, envelope("t1", "b", "p-b", "order-1", json!({})))
        .await
        .unwrap();

    // Re-register (new id) now listening for 'b'.
    let mut refreshed = wait(&["b"], JoinMode::Any);
    refreshed.instance_id = original.instance_id;
    refreshed.block_id = original.block_id.clone();
    let returned = register_wait(&s, refreshed).await.unwrap();

    // The pending 'b' was consumed and attributed...
    let events = s.list_events("t1", None, 10).await.unwrap();
    assert_eq!(events[0].status, EventStatus::Consumed);
    assert_eq!(events[0].consumed_by, Some(original.instance_id));
    // ...and the wait keeps the original row id and is SATISFIED.
    assert_eq!(returned.id, w1.id);
    assert_eq!(returned.status, WaitStatus::Satisfied);
    let stored = s
        .get_event_wait(InstanceId::from_uuid(original.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, WaitStatus::Satisfied);
}

// ============================================================================
// consume_events at-most-once and update_event_wait CAS
// ============================================================================

#[tokio::test]
async fn consume_then_reconsume_returns_zero() {
    let s = store().await;
    let e = envelope("t1", "paid", "p-1", "k", json!({}));
    let event_id = e.id;
    ingest(&s, e).await.unwrap();
    let consumer = InstanceId::new();
    assert_eq!(s.consume_events(&[event_id], consumer).await.unwrap(), 1);
    assert_eq!(s.consume_events(&[event_id], consumer).await.unwrap(), 0);
    // A different would-be consumer also gets zero.
    assert_eq!(
        s.consume_events(&[event_id], InstanceId::new())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn consume_unknown_event_id_returns_zero() {
    let s = store().await;
    assert_eq!(
        s.consume_events(&[Uuid::now_v7()], InstanceId::new())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn consume_mixed_batch_counts_only_pending() {
    let s = store().await;
    let e1 = envelope("t1", "paid", "p-1", "k", json!({}));
    let e2 = envelope("t1", "paid", "p-2", "k", json!({}));
    let (id1, id2) = (e1.id, e2.id);
    ingest(&s, e1).await.unwrap();
    ingest(&s, e2).await.unwrap();
    let consumer = InstanceId::new();
    assert_eq!(s.consume_events(&[id1], consumer).await.unwrap(), 1);
    // Batch containing one already-consumed and one pending: only 1.
    assert_eq!(s.consume_events(&[id1, id2], consumer).await.unwrap(), 1);
}

#[tokio::test]
async fn consume_marks_consumer_attribution() {
    let s = store().await;
    let e = envelope("t1", "paid", "p-1", "k", json!({}));
    let event_id = e.id;
    ingest(&s, e).await.unwrap();
    let consumer = InstanceId::new();
    s.consume_events(&[event_id], consumer).await.unwrap();
    let stored = s.get_event(event_id).await.unwrap().unwrap();
    assert_eq!(stored.status, EventStatus::Consumed);
    assert_eq!(stored.consumed_by, Some(consumer.into_uuid()));
}

#[tokio::test]
async fn update_event_wait_cas_stale_expected_returns_false() {
    let s = store().await;
    let registered = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    // Stored status is Waiting; expecting Satisfied is stale.
    let mut updated = registered.clone();
    updated.status = WaitStatus::Cancelled;
    assert!(
        !s.update_event_wait(&updated, WaitStatus::Satisfied)
            .await
            .unwrap()
    );
    // The row is untouched.
    let stored = s
        .get_event_wait(InstanceId::from_uuid(registered.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, WaitStatus::Waiting);
}

#[tokio::test]
async fn update_event_wait_cas_matching_expected_returns_true() {
    let s = store().await;
    let registered = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    let mut updated = registered.clone();
    updated.status = WaitStatus::Cancelled;
    assert!(
        s.update_event_wait(&updated, WaitStatus::Waiting)
            .await
            .unwrap()
    );
    let stored = s
        .get_event_wait(InstanceId::from_uuid(registered.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, WaitStatus::Cancelled);
}

#[tokio::test]
async fn update_event_wait_cas_fails_after_satisfaction() {
    let s = store().await;
    let registered = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    // Now stored as Satisfied: a writer still expecting Waiting loses.
    let mut updated = registered.clone();
    updated.status = WaitStatus::Cancelled;
    assert!(
        !s.update_event_wait(&updated, WaitStatus::Waiting)
            .await
            .unwrap()
    );
    let stored = s
        .get_event_wait(InstanceId::from_uuid(registered.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, WaitStatus::Satisfied);
}

#[tokio::test]
async fn update_event_wait_unknown_id_returns_false() {
    let s = store().await;
    let unknown = wait(&["paid"], JoinMode::Any);
    assert!(
        !s.update_event_wait(&unknown, WaitStatus::Waiting)
            .await
            .unwrap()
    );
}

// ============================================================================
// Retention / expiry
// ============================================================================

#[tokio::test]
async fn expire_touches_only_pending_events() {
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
    assert_eq!(expired, 1);
    let expired_rows = s
        .list_events("t1", Some(EventStatus::Expired), 10)
        .await
        .unwrap();
    assert_eq!(expired_rows.len(), 1);
    assert_eq!(expired_rows[0].event_name, "old");
    let consumed = s
        .list_events("t1", Some(EventStatus::Consumed), 10)
        .await
        .unwrap();
    assert_eq!(consumed.len(), 1);
}

#[tokio::test]
async fn expire_boundary_is_strictly_before_cutoff() {
    let s = store().await;
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
    let mut e = envelope("t1", "paid", "p-1", "k", json!({}));
    e.received_at = t0;
    ingest(&s, e).await.unwrap();

    // Cutoff exactly equal to received_at: NOT expired (strict <).
    assert_eq!(s.expire_events_before(t0).await.unwrap(), 0);
    // One second later: expired.
    assert_eq!(
        s.expire_events_before(t0 + chrono::Duration::seconds(1))
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn expire_with_early_cutoff_touches_nothing() {
    let s = store().await;
    ingest(&s, envelope("t1", "paid", "p-1", "k", json!({})))
        .await
        .unwrap();
    let expired = s
        .expire_events_before(Utc::now() - chrono::Duration::hours(1))
        .await
        .unwrap();
    assert_eq!(expired, 0);
    let pending = s
        .list_events("t1", Some(EventStatus::Pending), 10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
}

#[tokio::test]
async fn expired_event_not_drained_by_new_wait() {
    let s = store().await;
    ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    s.expire_events_before(Utc::now() + chrono::Duration::hours(1))
        .await
        .unwrap();
    let w = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    assert_eq!(w.status, WaitStatus::Waiting);
}

#[tokio::test]
async fn expire_is_idempotent() {
    let s = store().await;
    ingest(&s, envelope("t1", "paid", "p-1", "k", json!({})))
        .await
        .unwrap();
    let cutoff = Utc::now() + chrono::Duration::hours(1);
    assert_eq!(s.expire_events_before(cutoff).await.unwrap(), 1);
    assert_eq!(s.expire_events_before(cutoff).await.unwrap(), 0);
}

// ============================================================================
// list_events / get_event / get_event_wait
// ============================================================================

#[tokio::test]
async fn list_events_filters_by_status() {
    let s = store().await;
    register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "loose", "l-1", "k", json!({})))
        .await
        .unwrap();

    let pending = s
        .list_events("t1", Some(EventStatus::Pending), 10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event_name, "loose");
    let consumed = s
        .list_events("t1", Some(EventStatus::Consumed), 10)
        .await
        .unwrap();
    assert_eq!(consumed.len(), 1);
    assert_eq!(consumed[0].event_name, "paid");
    let all = s.list_events("t1", None, 10).await.unwrap();
    assert_eq!(all.len(), 2);
    let expired = s
        .list_events("t1", Some(EventStatus::Expired), 10)
        .await
        .unwrap();
    assert!(expired.is_empty());
}

#[tokio::test]
async fn list_events_is_tenant_scoped() {
    let s = store().await;
    ingest(&s, envelope("t1", "paid", "p-1", "k", json!({})))
        .await
        .unwrap();
    ingest(&s, envelope("t2", "paid", "p-1", "k", json!({})))
        .await
        .unwrap();
    assert_eq!(s.list_events("t1", None, 10).await.unwrap().len(), 1);
    assert_eq!(s.list_events("t2", None, 10).await.unwrap().len(), 1);
    assert!(s.list_events("t3", None, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn list_events_respects_limit() {
    let s = store().await;
    for i in 0..5 {
        ingest(
            &s,
            envelope("t1", "paid", &format!("p-{i}"), "k", json!({})),
        )
        .await
        .unwrap();
    }
    assert_eq!(s.list_events("t1", None, 3).await.unwrap().len(), 3);
    assert_eq!(s.list_events("t1", None, 10).await.unwrap().len(), 5);
    assert!(s.list_events("t1", None, 0).await.unwrap().is_empty());
}

#[tokio::test]
async fn list_events_newest_first() {
    let s = store().await;
    let base = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    for i in 0..3 {
        let mut e = envelope("t1", &format!("e{i}"), &format!("p-{i}"), "k", json!({}));
        e.received_at = base + chrono::Duration::seconds(i);
        ingest(&s, e).await.unwrap();
    }
    let events = s.list_events("t1", None, 10).await.unwrap();
    let names: Vec<&str> = events.iter().map(|e| e.event_name.as_str()).collect();
    assert_eq!(names, vec!["e2", "e1", "e0"]);
}

#[tokio::test]
async fn get_event_returns_stored_fields() {
    let s = store().await;
    let e = envelope("t1", "paid", "p-1", "order-1", json!({"amt": 5}));
    let id = e.id;
    ingest(&s, e).await.unwrap();
    let stored = s.get_event(id).await.unwrap().unwrap();
    assert_eq!(stored.id, id);
    assert_eq!(stored.tenant_id, "t1");
    assert_eq!(stored.event_name, "paid");
    assert_eq!(stored.producer_event_id, "p-1");
    assert_eq!(stored.correlation_key, "order-1");
    assert_eq!(stored.payload, json!({"amt": 5}));
    assert_eq!(stored.status, EventStatus::Pending);
    assert_eq!(stored.consumed_by, None);
}

#[tokio::test]
async fn get_event_miss_returns_none() {
    let s = store().await;
    assert!(s.get_event(Uuid::now_v7()).await.unwrap().is_none());
}

#[tokio::test]
async fn get_event_wait_miss_unknown_block() {
    let s = store().await;
    let w = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    assert!(
        s.get_event_wait(InstanceId::from_uuid(w.instance_id), "no_such_block")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn get_event_wait_miss_unknown_instance() {
    let s = store().await;
    register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    assert!(
        s.get_event_wait(InstanceId::new(), "wait_block")
            .await
            .unwrap()
            .is_none()
    );
}

// ============================================================================
// find_pending_events / find_waiting_event_waits
// ============================================================================

#[tokio::test]
async fn find_pending_events_filters_names_oldest_first() {
    let s = store().await;
    let base = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    for (i, name) in ["a", "b", "c"].iter().enumerate() {
        let mut e = envelope("t1", name, &format!("p-{name}"), "k", json!({}));
        e.received_at = base + chrono::Duration::seconds(i64::try_from(i).unwrap());
        ingest(&s, e).await.unwrap();
    }
    let found = s
        .find_pending_events("t1", &["c".to_string(), "a".to_string()], "k")
        .await
        .unwrap();
    let names: Vec<&str> = found.iter().map(|e| e.event_name.as_str()).collect();
    assert_eq!(names, vec!["a", "c"]);
}

#[tokio::test]
async fn find_pending_events_excludes_consumed_and_other_keys() {
    let s = store().await;
    let e1 = envelope("t1", "a", "p-1", "k", json!({}));
    let id1 = e1.id;
    ingest(&s, e1).await.unwrap();
    ingest(&s, envelope("t1", "a", "p-2", "OTHER", json!({})))
        .await
        .unwrap();
    s.consume_events(&[id1], InstanceId::new()).await.unwrap();

    let found = s
        .find_pending_events("t1", &["a".to_string()], "k")
        .await
        .unwrap();
    assert!(found.is_empty());
    let other = s
        .find_pending_events("t1", &["a".to_string()], "OTHER")
        .await
        .unwrap();
    assert_eq!(other.len(), 1);
}

#[tokio::test]
async fn find_pending_events_empty_names_finds_nothing() {
    let s = store().await;
    ingest(&s, envelope("t1", "a", "p-1", "k", json!({})))
        .await
        .unwrap();
    assert!(
        s.find_pending_events("t1", &[], "k")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn find_waiting_event_waits_filters_by_name() {
    let s = store().await;
    register_wait(&s, wait(&["a"], JoinMode::Any))
        .await
        .unwrap();
    assert_eq!(
        s.find_waiting_event_waits("t1", "a", "order-1")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        s.find_waiting_event_waits("t1", "b", "order-1")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn find_waiting_event_waits_filters_by_key_and_tenant() {
    let s = store().await;
    register_wait(&s, wait(&["a"], JoinMode::Any))
        .await
        .unwrap();
    assert!(
        s.find_waiting_event_waits("t1", "a", "OTHER")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        s.find_waiting_event_waits("t2", "a", "order-1")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn find_waiting_event_waits_excludes_satisfied() {
    let s = store().await;
    register_wait(&s, wait(&["a"], JoinMode::Any))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "a", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    assert!(
        s.find_waiting_event_waits("t1", "a", "order-1")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn find_waiting_event_waits_oldest_first() {
    let s = store().await;
    let mut old = wait(&["a"], JoinMode::Any);
    old.created_at = Utc::now() - chrono::Duration::seconds(30);
    let old_id = old.id;
    register_wait(&s, old).await.unwrap();
    let mut new = wait(&["a"], JoinMode::Any);
    new.block_id = "newer_block".into();
    let new_id = new.id;
    register_wait(&s, new).await.unwrap();

    let waits = s
        .find_waiting_event_waits("t1", "a", "order-1")
        .await
        .unwrap();
    let ids: Vec<Uuid> = waits.iter().map(|w| w.id).collect();
    assert_eq!(ids, vec![old_id, new_id]);
}

// ============================================================================
// parse_wait_params
// ============================================================================

#[allow(clippy::needless_pass_by_value)]
fn parse(params: serde_json::Value) -> Result<EventWait, String> {
    parse_wait_params(&params, "t1", Uuid::now_v7(), "blk")
}

#[tokio::test]
async fn parse_string_events_single_name() {
    let w = parse(json!({"events": "paid", "correlation_key": "k"})).unwrap();
    assert_eq!(w.event_names, vec!["paid".to_string()]);
    assert_eq!(w.join_mode, JoinMode::All);
}

#[tokio::test]
async fn parse_array_events_multiple_names() {
    let w = parse(json!({"events": ["a", "b"], "correlation_key": "k"})).unwrap();
    assert_eq!(w.event_names, vec!["a".to_string(), "b".to_string()]);
}

#[tokio::test]
async fn parse_missing_events_errors() {
    let err = parse(json!({"correlation_key": "k"})).unwrap_err();
    assert_eq!(err, "'events' (string or array) is required");
}

#[tokio::test]
async fn parse_blank_string_events_errors() {
    for events in ["", "   ", "\t\n"] {
        let err = parse(json!({"events": events, "correlation_key": "k"})).unwrap_err();
        assert_eq!(
            err, "'events' (string or array) is required",
            "events={events:?}"
        );
    }
}

#[tokio::test]
async fn parse_string_events_not_trimmed() {
    // A non-blank string passes the trim gate but is stored verbatim.
    let w = parse(json!({"events": " paid ", "correlation_key": "k"})).unwrap();
    assert_eq!(w.event_names, vec![" paid ".to_string()]);
}

#[tokio::test]
async fn parse_empty_array_events_errors() {
    let err = parse(json!({"events": [], "correlation_key": "k"})).unwrap_err();
    assert_eq!(err, "'events' must be a non-empty array of strings");
}

#[tokio::test]
async fn parse_array_with_non_string_entry_errors() {
    for bad in [
        json!(["a", 1]),
        json!([null]),
        json!([{"n": "a"}]),
        json!([["a"]]),
    ] {
        let err = parse(json!({"events": bad, "correlation_key": "k"})).unwrap_err();
        assert_eq!(err, "'events' must be a non-empty array of strings");
    }
}

#[tokio::test]
async fn parse_array_keeps_empty_string_entries() {
    // Empty strings inside an array are NOT filtered — only non-strings
    // are rejected. Assert the actual behavior.
    let w = parse(json!({"events": ["", "x"], "correlation_key": "k"})).unwrap();
    assert_eq!(w.event_names, vec![String::new(), "x".to_string()]);
}

#[tokio::test]
async fn parse_events_non_string_non_array_errors() {
    for bad in [json!(5), json!(true), json!({"a": 1}), json!(null)] {
        let err = parse(json!({"events": bad, "correlation_key": "k"})).unwrap_err();
        assert_eq!(err, "'events' (string or array) is required");
    }
}

#[tokio::test]
async fn parse_missing_correlation_key_errors() {
    let err = parse(json!({"events": "paid"})).unwrap_err();
    assert_eq!(err, "'correlation_key' is required and must be non-empty");
}

#[tokio::test]
async fn parse_blank_correlation_key_errors() {
    for key in ["", "  ", "\t"] {
        let err = parse(json!({"events": "paid", "correlation_key": key})).unwrap_err();
        assert_eq!(err, "'correlation_key' is required and must be non-empty");
    }
}

#[tokio::test]
async fn parse_non_string_correlation_key_errors() {
    let err = parse(json!({"events": "paid", "correlation_key": 42})).unwrap_err();
    assert_eq!(err, "'correlation_key' is required and must be non-empty");
}

#[tokio::test]
async fn parse_correlation_key_is_trimmed() {
    let w = parse(json!({"events": "paid", "correlation_key": "  order-9  "})).unwrap();
    assert_eq!(w.correlation_key, "order-9");
}

#[tokio::test]
async fn parse_join_defaults_to_all() {
    let w = parse(json!({"events": "paid", "correlation_key": "k"})).unwrap();
    assert_eq!(w.join_mode, JoinMode::All);
    let w = parse(json!({"events": "paid", "correlation_key": "k", "join": "all"})).unwrap();
    assert_eq!(w.join_mode, JoinMode::All);
}

#[tokio::test]
async fn parse_join_any() {
    let w = parse(json!({"events": ["a", "b"], "correlation_key": "k", "join": "any"})).unwrap();
    assert_eq!(w.join_mode, JoinMode::Any);
}

#[tokio::test]
async fn parse_join_count_valid() {
    let w =
        parse(json!({"events": "r", "correlation_key": "k", "join": "count", "count": 3})).unwrap();
    assert_eq!(w.join_mode, JoinMode::Count { count: 3 });
}

#[tokio::test]
async fn parse_join_count_missing_count_errors() {
    let err = parse(json!({"events": "r", "correlation_key": "k", "join": "count"})).unwrap_err();
    assert_eq!(err, "join 'count' requires a positive 'count'");
}

#[tokio::test]
async fn parse_join_count_invalid_values_error() {
    for bad in [
        json!(0),
        json!(-1),
        json!(2.5),
        json!("3"),
        json!(4_294_967_296_u64),
    ] {
        let err =
            parse(json!({"events": "r", "correlation_key": "k", "join": "count", "count": bad}))
                .unwrap_err();
        assert_eq!(
            err, "join 'count' requires a positive 'count'",
            "count={bad}"
        );
    }
}

#[tokio::test]
async fn parse_join_count_u32_max_ok() {
    let w = parse(json!({
        "events": "r", "correlation_key": "k", "join": "count", "count": u32::MAX
    }))
    .unwrap();
    assert_eq!(w.join_mode, JoinMode::Count { count: u32::MAX });
}

#[tokio::test]
async fn parse_unknown_join_errors() {
    let err = parse(json!({"events": "r", "correlation_key": "k", "join": "quorum"})).unwrap_err();
    assert_eq!(err, "unknown join mode 'quorum'");
}

#[tokio::test]
async fn parse_non_string_join_treated_as_missing() {
    // A non-string join value fails as_str and silently falls back to All.
    let w = parse(json!({"events": "r", "correlation_key": "k", "join": 5})).unwrap();
    assert_eq!(w.join_mode, JoinMode::All);
}

#[tokio::test]
async fn parse_output_fields_populated() {
    let instance = Uuid::now_v7();
    let w = parse_wait_params(
        &json!({"events": "paid", "correlation_key": "k"}),
        "tenant-x",
        instance,
        "my_block",
    )
    .unwrap();
    assert_eq!(w.tenant_id, "tenant-x");
    assert_eq!(w.instance_id, instance);
    assert_eq!(w.block_id, "my_block");
    assert_eq!(w.status, WaitStatus::Waiting);
    assert!(w.matched_names.is_empty());
    assert!(w.matched_event_ids.is_empty());
}

// ============================================================================
// envelope() constructor
// ============================================================================

#[tokio::test]
async fn envelope_constructor_populates_fields() {
    let before = Utc::now();
    let e = envelope("t9", "paid", "p-77", "order-3", json!({"x": 1}));
    let after = Utc::now();
    assert_eq!(e.tenant_id, "t9");
    assert_eq!(e.event_name, "paid");
    assert_eq!(e.producer_event_id, "p-77");
    assert_eq!(e.correlation_key, "order-3");
    assert_eq!(e.payload, json!({"x": 1}));
    assert_eq!(e.status, EventStatus::Pending);
    assert_eq!(e.consumed_by, None);
    assert!(e.received_at >= before && e.received_at <= after);
}

#[tokio::test]
async fn envelope_constructor_generates_unique_ids() {
    let a = envelope("t1", "paid", "p-1", "k", json!({}));
    let b = envelope("t1", "paid", "p-1", "k", json!({}));
    assert_ne!(a.id, b.id);
}

// ============================================================================
// Resume-signal delivery to real instances
// ============================================================================

#[tokio::test]
async fn satisfaction_enqueues_resume_signal_for_waiting_instance() {
    let s = store().await;
    let instance_id = create_parked_instance(&s, "t1", InstanceState::Waiting).await;

    let mut w = wait(&["paid"], JoinMode::Any);
    w.instance_id = instance_id.into_uuid();
    let registered = register_wait(&s, w).await.unwrap();
    assert_eq!(registered.status, WaitStatus::Waiting);

    let outcome = ingest(
        &s,
        envelope("t1", "paid", "p-1", "order-1", json!({"amt": 9})),
    )
    .await
    .unwrap();
    assert!(outcome.satisfied);

    let signals = s.get_pending_signals(instance_id).await.unwrap();
    assert_eq!(signals.len(), 1);
    let signal = &signals[0];
    assert_eq!(
        signal.signal_type,
        SignalType::Custom("human_input:wait_block".to_string())
    );
    assert_eq!(signal.payload["value"], json!("yes"));
    assert_eq!(
        signal.payload["event_wait"],
        json!(registered.id.to_string())
    );
    assert!(!signal.delivered);
    assert_eq!(signal.instance_id, instance_id);
}

#[tokio::test]
async fn event_before_wait_registration_also_enqueues_signal() {
    let s = store().await;
    let instance_id = create_parked_instance(&s, "t1", InstanceState::Waiting).await;

    ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    let mut w = wait(&["paid"], JoinMode::Any);
    w.instance_id = instance_id.into_uuid();
    let registered = register_wait(&s, w).await.unwrap();
    assert_eq!(registered.status, WaitStatus::Satisfied);

    let signals = s.get_pending_signals(instance_id).await.unwrap();
    assert_eq!(signals.len(), 1);
    assert_eq!(
        signals[0].signal_type,
        SignalType::Custom("human_input:wait_block".to_string())
    );
    assert_eq!(signals[0].payload["value"], json!("yes"));
}

#[tokio::test]
async fn signal_uses_the_wait_block_id() {
    let s = store().await;
    let instance_id = create_parked_instance(&s, "t1", InstanceState::Waiting).await;

    let mut w = wait(&["paid"], JoinMode::Any);
    w.instance_id = instance_id.into_uuid();
    w.block_id = "custom_gate".into();
    register_wait(&s, w).await.unwrap();
    ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();

    let signals = s.get_pending_signals(instance_id).await.unwrap();
    assert_eq!(signals.len(), 1);
    assert_eq!(
        signals[0].signal_type,
        SignalType::Custom("human_input:custom_gate".to_string())
    );
}

#[tokio::test]
async fn terminal_instance_gets_no_signal_but_wait_still_satisfies() {
    let s = store().await;
    let instance_id = create_parked_instance(&s, "t1", InstanceState::Completed).await;

    let mut w = wait(&["paid"], JoinMode::Any);
    w.instance_id = instance_id.into_uuid();
    register_wait(&s, w).await.unwrap();
    let outcome = ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    assert!(outcome.satisfied);

    let signals = s.get_pending_signals(instance_id).await.unwrap();
    assert!(signals.is_empty());
    let stored = s
        .get_event_wait(instance_id, "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, WaitStatus::Satisfied);
}

#[tokio::test]
async fn missing_instance_row_is_tolerated() {
    // The storage-only path: no instance exists at all. Satisfaction must
    // still complete without error and persist the wait as satisfied.
    let s = store().await;
    let w = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();
    let outcome = ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    assert!(outcome.satisfied);
    let stored = s
        .get_event_wait(InstanceId::from_uuid(w.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, WaitStatus::Satisfied);
}

#[tokio::test]
async fn count_join_signals_only_on_final_event() {
    let s = store().await;
    let instance_id = create_parked_instance(&s, "t1", InstanceState::Waiting).await;

    let mut w = wait(&["reading"], JoinMode::Count { count: 2 });
    w.instance_id = instance_id.into_uuid();
    register_wait(&s, w).await.unwrap();

    ingest(&s, envelope("t1", "reading", "r-1", "order-1", json!({})))
        .await
        .unwrap();
    assert!(s.get_pending_signals(instance_id).await.unwrap().is_empty());

    ingest(&s, envelope("t1", "reading", "r-2", "order-1", json!({})))
        .await
        .unwrap();
    assert_eq!(s.get_pending_signals(instance_id).await.unwrap().len(), 1);
}

// ============================================================================
// End-to-end multi-wait pipeline
// ============================================================================

#[tokio::test]
async fn independent_orders_progress_independently() {
    let s = store().await;
    let mut order1 = wait(&["paid", "stocked"], JoinMode::All);
    order1.correlation_key = "order-1".into();
    let mut order2 = wait(&["paid", "stocked"], JoinMode::All);
    order2.correlation_key = "order-2".into();
    order2.block_id = "wait_block".into();
    register_wait(&s, order1.clone()).await.unwrap();
    register_wait(&s, order2.clone()).await.unwrap();

    ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    ingest(&s, envelope("t1", "stocked", "s-2", "order-2", json!({})))
        .await
        .unwrap();

    let s1 = s
        .get_event_wait(InstanceId::from_uuid(order1.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    let s2 = s
        .get_event_wait(InstanceId::from_uuid(order2.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s1.matched_names, vec!["paid".to_string()]);
    assert_eq!(s2.matched_names, vec!["stocked".to_string()]);

    let done1 = ingest(&s, envelope("t1", "stocked", "s-1", "order-1", json!({})))
        .await
        .unwrap();
    let done2 = ingest(&s, envelope("t1", "paid", "p-2", "order-2", json!({})))
        .await
        .unwrap();
    assert!(done1.satisfied && done2.satisfied);
}

#[tokio::test]
async fn full_pipeline_dedup_then_match_then_consume_exactly_once() {
    let s = store().await;
    let w = register_wait(&s, wait(&["paid"], JoinMode::Any))
        .await
        .unwrap();

    // Producer sends the same logical event three times.
    let o1 = ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    let o2 = ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    let o3 = ingest(&s, envelope("t1", "paid", "p-1", "order-1", json!({})))
        .await
        .unwrap();
    assert!(!o1.duplicate && o1.satisfied);
    assert!(o2.duplicate && !o2.satisfied);
    assert!(o3.duplicate && !o3.satisfied);

    // Exactly one row, consumed exactly once.
    let events = s.list_events("t1", None, 10).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].status, EventStatus::Consumed);
    assert_eq!(events[0].consumed_by, Some(w.instance_id));
    assert_eq!(
        s.consume_events(&[events[0].id], InstanceId::new())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn wait_progress_is_durable_across_reads() {
    let s = store().await;
    let w = register_wait(&s, wait(&["a", "b", "c"], JoinMode::All))
        .await
        .unwrap();
    for name in ["c", "a"] {
        ingest(
            &s,
            envelope("t1", name, &format!("p-{name}"), "order-1", json!({})),
        )
        .await
        .unwrap();
    }
    let stored = s
        .get_event_wait(InstanceId::from_uuid(w.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, WaitStatus::Waiting);
    assert_eq!(stored.matched_names, vec!["c".to_string(), "a".to_string()]);
    assert_eq!(stored.matched_event_ids.len(), 2);

    let last = ingest(&s, envelope("t1", "b", "p-b", "order-1", json!({})))
        .await
        .unwrap();
    assert!(last.satisfied);
    let done = s
        .get_event_wait(InstanceId::from_uuid(w.instance_id), "wait_block")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(done.status, WaitStatus::Satisfied);
    assert_eq!(done.matched_event_ids.len(), 3);
}
