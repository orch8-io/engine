//! Overlap-policy semantics for cron schedules, exercised through
//! `process_cron_tick`. A "previous run" is simulated by inserting an
//! instance stamped with `metadata.cron_schedule_id` in a non-terminal state
//! before the tick runs.

use std::sync::Arc;

use chrono::Utc;
use serde_json::json;
use uuid::Uuid;

use orch8_engine::clock::SharedClock;
use orch8_engine::cron::process_cron_tick;
use orch8_storage::StorageBackend;
use orch8_types::cron::{CronSchedule, OverlapPolicy};
use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, TaskInstance};

mod common;
use common::{mk_sequence, mk_step, storage};

async fn seed_sequence(s: &Arc<dyn StorageBackend>) -> SequenceId {
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    let id = seq.id;
    s.create_sequence(&seq).await.unwrap();
    id
}

fn mk_schedule(seq_id: SequenceId, policy: OverlapPolicy) -> CronSchedule {
    let now = Utc::now();
    CronSchedule {
        id: Uuid::now_v7(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        sequence_id: seq_id,
        cron_expr: "* * * * *".into(),
        timezone: "UTC".into(),
        enabled: true,
        metadata: json!({}),
        overlap_policy: policy,
        skipped_fires: 0,
        last_skipped_at: None,
        // Due now, and never triggered → claim_due will pick it up.
        last_triggered_at: None,
        next_fire_at: Some(now - chrono::Duration::seconds(5)),
        created_at: now,
        updated_at: now,
    }
}

/// Insert a non-terminal instance attributed to `cron_id`, as the cron loop
/// would have stamped it.
async fn seed_active_run(
    s: &Arc<dyn StorageBackend>,
    seq_id: SequenceId,
    cron_id: Uuid,
) -> InstanceId {
    let now = Utc::now();
    let inst = TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq_id,
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        state: InstanceState::Running,
        next_fire_at: Some(now),
        priority: orch8_types::instance::Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({ "cron_schedule_id": cron_id.to_string() }),
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
    let id = inst.id;
    s.create_instance(&inst).await.unwrap();
    id
}

async fn count_instances_for(s: &Arc<dyn StorageBackend>, seq_id: SequenceId) -> usize {
    let filter = orch8_types::filter::InstanceFilter {
        sequence_id: Some(seq_id),
        ..Default::default()
    };
    let page = orch8_types::filter::Pagination {
        limit: 1000,
        offset: 0,
        sort_ascending: true,
    };
    s.list_instances(&filter, &page).await.unwrap().len()
}

#[tokio::test]
async fn allow_policy_fires_despite_active_run() {
    let s = storage().await;
    let seq_id = seed_sequence(&s).await;
    let sched = mk_schedule(seq_id, OverlapPolicy::Allow);
    s.create_cron_schedule(&sched).await.unwrap();
    seed_active_run(&s, seq_id, sched.id).await;

    process_cron_tick(&s, &SharedClock::default())
        .await
        .unwrap();

    // 1 pre-existing active run + 1 newly fired = 2.
    assert_eq!(count_instances_for(&s, seq_id).await, 2);
    let after = s.get_cron_schedule(sched.id).await.unwrap().unwrap();
    assert_eq!(after.skipped_fires, 0);
}

#[tokio::test]
async fn skip_policy_skips_and_counts_when_run_active() {
    let s = storage().await;
    let seq_id = seed_sequence(&s).await;
    let sched = mk_schedule(seq_id, OverlapPolicy::Skip);
    s.create_cron_schedule(&sched).await.unwrap();
    seed_active_run(&s, seq_id, sched.id).await;

    process_cron_tick(&s, &SharedClock::default())
        .await
        .unwrap();

    // No new instance: still just the pre-existing active run.
    assert_eq!(count_instances_for(&s, seq_id).await, 1);
    let after = s.get_cron_schedule(sched.id).await.unwrap().unwrap();
    assert_eq!(after.skipped_fires, 1);
    assert!(after.last_skipped_at.is_some());
    // Fire time advanced so the schedule is no longer due.
    assert!(after.next_fire_at.unwrap() > Utc::now());
}

#[tokio::test]
async fn skip_policy_fires_when_no_active_run() {
    let s = storage().await;
    let seq_id = seed_sequence(&s).await;
    let sched = mk_schedule(seq_id, OverlapPolicy::Skip);
    s.create_cron_schedule(&sched).await.unwrap();
    // No active run seeded.

    process_cron_tick(&s, &SharedClock::default())
        .await
        .unwrap();

    assert_eq!(count_instances_for(&s, seq_id).await, 1);
    let after = s.get_cron_schedule(sched.id).await.unwrap().unwrap();
    assert_eq!(after.skipped_fires, 0);
}

#[tokio::test]
async fn cancel_previous_cancels_active_then_fires() {
    // M-10: `CancelPrevious` enqueues a `Cancel` signal rather than writing
    // `Cancelled` directly, so the previous run's on_cancel hooks / scoped
    // cancellation still apply (same path a user-initiated cancel takes).
    // The cancellation is therefore no longer synchronous within
    // `process_cron_tick` -- it lands once the signal is processed.
    let s = storage().await;
    let seq_id = seed_sequence(&s).await;
    let sched = mk_schedule(seq_id, OverlapPolicy::CancelPrevious);
    s.create_cron_schedule(&sched).await.unwrap();
    let prev = seed_active_run(&s, seq_id, sched.id).await;

    process_cron_tick(&s, &SharedClock::default())
        .await
        .unwrap();

    // A Cancel signal was enqueued for the previous run (not an immediate
    // direct state write).
    let pending = s.get_pending_signals(prev).await.unwrap();
    assert!(
        pending
            .iter()
            .any(|sig| sig.signal_type == orch8_types::signal::SignalType::Cancel),
        "expected a pending Cancel signal for the previous run, got: {pending:?}"
    );
    // Still Running until the signal is actually processed.
    let prev_inst = s.get_instance(prev).await.unwrap().unwrap();
    assert_eq!(prev_inst.state, InstanceState::Running);

    // Drive the signal through, as the scheduler's signal-processing pass
    // would on its next tick.
    orch8_engine::signals::process_signals(&*s, prev, InstanceState::Running)
        .await
        .unwrap();
    let prev_inst = s.get_instance(prev).await.unwrap().unwrap();
    assert_eq!(prev_inst.state, InstanceState::Cancelled);

    // New run created: old (now cancelled) + new = 2 rows.
    assert_eq!(count_instances_for(&s, seq_id).await, 2);
}

#[tokio::test]
async fn buffer_one_defers_without_firing_when_active() {
    let s = storage().await;
    let seq_id = seed_sequence(&s).await;
    let sched = mk_schedule(seq_id, OverlapPolicy::BufferOne);
    s.create_cron_schedule(&sched).await.unwrap();
    seed_active_run(&s, seq_id, sched.id).await;

    process_cron_tick(&s, &SharedClock::default())
        .await
        .unwrap();

    // No new instance — the occurrence is deferred, not fired or skipped.
    assert_eq!(count_instances_for(&s, seq_id).await, 1);
    let after = s.get_cron_schedule(sched.id).await.unwrap().unwrap();
    assert_eq!(after.skipped_fires, 0);
    // Re-armed for a near-future retry (not advanced a full interval).
    assert!(after.next_fire_at.unwrap() > Utc::now());
}

#[tokio::test]
async fn fired_instance_is_stamped_with_cron_id() {
    let s = storage().await;
    let seq_id = seed_sequence(&s).await;
    let sched = mk_schedule(seq_id, OverlapPolicy::Allow);
    s.create_cron_schedule(&sched).await.unwrap();

    process_cron_tick(&s, &SharedClock::default())
        .await
        .unwrap();

    // The single fired instance must carry the attribution stamp so future
    // overlap checks can find it.
    let active = s.active_instance_ids_for_cron(sched.id, 100).await.unwrap();
    assert_eq!(active.len(), 1);
}

/// H-8: `trigger_cron_schedule` writes the instance and advances
/// `next_fire_at` in two separate storage calls. This simulates a crash
/// between the two: the instance for this exact fire already exists (created
/// by the "first attempt"), but `next_fire_at` was never advanced, so
/// `process_cron_tick` claims the same schedule again on this "retry" tick.
/// The idempotency key must make the retry's `create_instance` a no-op
/// (Conflict, swallowed) rather than a duplicate — and the fire time must
/// still advance so the schedule doesn't spin on this tick forever.
#[tokio::test]
async fn crash_between_create_and_advance_does_not_duplicate_instance() {
    let s = storage().await;
    let seq_id = seed_sequence(&s).await;
    let sched = mk_schedule(seq_id, OverlapPolicy::Allow);
    s.create_cron_schedule(&sched).await.unwrap();

    // Simulate the "first attempt": an instance already exists for this
    // exact (schedule, fire_at), stamped with the same idempotency key
    // `trigger_cron_schedule` would generate, but the schedule's
    // `next_fire_at` was never advanced (the crash happened before that
    // second write).
    let fire_at = sched.next_fire_at.unwrap();
    let idempotency_key = format!("cron:{}:{}", sched.id, fire_at.to_rfc3339());
    let now = Utc::now();
    let pre_existing = TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq_id,
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        state: InstanceState::Scheduled,
        next_fire_at: Some(fire_at),
        priority: orch8_types::instance::Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({ "cron_schedule_id": sched.id.to_string() }),
        context: orch8_types::context::ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: Some(idempotency_key),
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    };
    s.create_instance(&pre_existing).await.unwrap();
    assert_eq!(count_instances_for(&s, seq_id).await, 1);

    // The "retry" tick: next_fire_at is still due, so claim_due_cron_schedules
    // picks the schedule up again.
    process_cron_tick(&s, &SharedClock::default())
        .await
        .unwrap();

    // No duplicate instance was created.
    assert_eq!(
        count_instances_for(&s, seq_id).await,
        1,
        "the idempotency key must prevent a duplicate instance on retry"
    );
    // The schedule still advanced past this fire — a Conflict on
    // create_instance must not get stuck retrying the same tick forever.
    let after = s.get_cron_schedule(sched.id).await.unwrap().unwrap();
    assert!(
        after.next_fire_at.unwrap() > fire_at,
        "next_fire_at must advance even when create_instance hit a conflict"
    );
}
