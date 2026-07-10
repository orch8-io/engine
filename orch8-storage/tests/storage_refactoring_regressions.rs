//! Regression tests for the 2026-07 storage refactoring review
//! (`STORAGE_REFACTORING_2026-07.md`). Each test reproduces one confirmed
//! bug from the review's High/Medium findings:
//!
//!   H1. `expire_timed_out_worker_tasks` (SQLite) must not fail a task whose
//!       timeout hasn't actually elapsed just because the computed expiry
//!       falls on the same calendar day as now (string-vs-datetime() collation
//!       trap).
//!   H3. `inject_blocks` (SQLite) must append to existing injected blocks
//!       across multiple calls, not overwrite them.
//!   H4. `signal_inbox` (SQLite) must persist `delivered` / `delivered_at`
//!       faithfully — an already-delivered `Signal` must not come back as
//!       pending, and marking delivery must stamp `delivered_at`.
//!   M5. `externalized_state` upsert (SQLite) must preserve the original
//!       `created_at` across a re-save, not regenerate it via `INSERT OR
//!       REPLACE` (which would defeat GC TTL aging for repeatedly-updated
//!       context fields).
//!   L1. `delete_terminal_instances` (SQLite) must delete old terminal
//!       instances and their non-cascaded history (step_logs, audit_log,
//!       usage_events), while leaving recent or non-terminal instances
//!       alone.
//!   R4. `rollback_policies`/`rollback_history` timestamp columns (SQLite)
//!       must round-trip the real stored value on read, not silently
//!       substitute `Utc::now()` because the stored SQLite-native format
//!       doesn't parse as RFC 3339.
//!
//! Running only this file:
//!
//! ```text
//! cargo test -p orch8-storage --test storage_refactoring_regressions
//! ```

use chrono::Utc;
use serde_json::json;
use uuid::Uuid;

use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{
    AdminStore, InstanceStore, ResourceStore, SignalStore, TelemetryStore, WorkerStore,
};
use orch8_types::context::ExecutionContext;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::signal::{Signal, SignalType};
use orch8_types::worker::{WorkerTask, WorkerTaskState};

async fn store() -> SqliteStorage {
    SqliteStorage::in_memory().await.unwrap()
}

fn make_instance(state: InstanceState) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("default"),
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
        created_at: now,
        updated_at: now,
    }
}

fn make_worker_task(instance_id: InstanceId, timeout_ms: Option<i64>) -> WorkerTask {
    WorkerTask {
        id: Uuid::now_v7(),
        instance_id,
        block_id: BlockId::new("s1"),
        handler_name: "h".into(),
        queue_name: None,
        params: json!({}),
        context: json!({}),
        attempt: 0,
        timeout_ms,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    }
}

// ---------------------------------------------------------------------------
// H1: expire_timed_out_worker_tasks compares timestamps consistently
// ---------------------------------------------------------------------------

#[tokio::test]
async fn expire_timed_out_does_not_fail_tasks_before_their_deadline() {
    let s = store().await;
    let inst = make_instance(InstanceState::Running);
    s.create_instance(&inst).await.unwrap();

    // A task created "now" with a 1-hour timeout must NOT be expired by a
    // sweep that also runs "now" — the old buggy string comparison treated
    // any same-day expiry as already elapsed regardless of time-of-day.
    let task = make_worker_task(inst.id, Some(3_600_000));
    s.create_worker_task(&task).await.unwrap();

    let expired = s.expire_timed_out_worker_tasks().await.unwrap();
    assert_eq!(
        expired, 0,
        "a task with a 1-hour timeout must not expire immediately"
    );

    let reread = s.get_worker_task(task.id).await.unwrap().unwrap();
    assert_eq!(reread.state, WorkerTaskState::Pending);
}

#[tokio::test]
async fn expire_timed_out_expires_tasks_past_their_deadline() {
    let s = store().await;
    let inst = make_instance(InstanceState::Running);
    s.create_instance(&inst).await.unwrap();

    // Backdate created_at well past its 1-hour timeout so the deadline has
    // unambiguously elapsed — `datetime()` truncates to whole-second
    // resolution, so a real clock's sub-second gap isn't a reliable signal.
    let mut task = make_worker_task(inst.id, Some(3_600_000));
    task.created_at = Utc::now() - chrono::Duration::hours(2);
    s.create_worker_task(&task).await.unwrap();

    let expired = s.expire_timed_out_worker_tasks().await.unwrap();
    assert_eq!(expired, 1, "a task past its deadline must be expired");

    let reread = s.get_worker_task(task.id).await.unwrap().unwrap();
    assert_eq!(reread.state, WorkerTaskState::Failed);
}

// ---------------------------------------------------------------------------
// H3: inject_blocks appends across multiple calls instead of replacing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn inject_blocks_accumulates_across_multiple_calls() {
    let s = store().await;
    let inst = make_instance(InstanceState::Running);
    s.create_instance(&inst).await.unwrap();

    s.inject_blocks(inst.id, &json!([{"id": "a"}]))
        .await
        .unwrap();
    s.inject_blocks(inst.id, &json!([{"id": "b"}, {"id": "c"}]))
        .await
        .unwrap();

    let blocks = s.get_injected_blocks(inst.id).await.unwrap().unwrap();
    let arr = blocks.as_array().unwrap();
    assert_eq!(
        arr.len(),
        3,
        "two inject_blocks calls (1 + 2 blocks) must yield 3 total blocks, not overwrite to 2"
    );
    assert_eq!(arr[0]["id"], "a");
    assert_eq!(arr[1]["id"], "b");
    assert_eq!(arr[2]["id"], "c");
}

// ---------------------------------------------------------------------------
// H4: signal_inbox persists delivered / delivered_at faithfully
// ---------------------------------------------------------------------------

#[tokio::test]
async fn enqueue_signal_persists_delivered_flag() {
    let s = store().await;
    let inst = make_instance(InstanceState::Waiting);
    s.create_instance(&inst).await.unwrap();

    let mut signal = Signal {
        id: Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::Resume,
        payload: json!({}),
        delivered: true,
        created_at: Utc::now(),
        delivered_at: Some(Utc::now()),
    };
    s.enqueue_signal(&signal).await.unwrap();

    // A signal enqueued as already-delivered must not show up as pending.
    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert!(
        pending.is_empty(),
        "a signal enqueued with delivered=true must not be re-delivered"
    );

    // Sanity: an undelivered signal for the same instance IS returned.
    signal.id = Uuid::now_v7();
    signal.delivered = false;
    signal.delivered_at = None;
    s.enqueue_signal(&signal).await.unwrap();
    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert!(!pending[0].delivered);
    assert!(pending[0].delivered_at.is_none());
}

#[tokio::test]
async fn mark_signal_delivered_stamps_delivered_at() {
    let s = store().await;
    let inst = make_instance(InstanceState::Waiting);
    s.create_instance(&inst).await.unwrap();

    let signal = Signal {
        id: Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::Pause,
        payload: json!({}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    s.enqueue_signal(&signal).await.unwrap();
    s.mark_signal_delivered(signal.id).await.unwrap();

    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert!(pending.is_empty());

    // Read the row directly to confirm delivered_at was actually stamped
    // (not just `delivered` flipped) — no trait getter surfaces a delivered
    // signal's fields once it's out of the pending set.
    let delivered_at: Option<String> =
        sqlx::query_scalar("SELECT delivered_at FROM signal_inbox WHERE id = ?1")
            .bind(signal.id.to_string())
            .fetch_one(s.pool())
            .await
            .unwrap();
    assert!(
        delivered_at.is_some(),
        "mark_signal_delivered must stamp delivered_at, not leave it NULL"
    );
}

#[tokio::test]
async fn mark_signals_delivered_batch_stamps_delivered_at() {
    let s = store().await;
    let inst = make_instance(InstanceState::Waiting);
    s.create_instance(&inst).await.unwrap();

    let signal = Signal {
        id: Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::Cancel,
        payload: json!({}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    s.enqueue_signal(&signal).await.unwrap();
    s.mark_signals_delivered(&[signal.id]).await.unwrap();

    let delivered_at: Option<String> =
        sqlx::query_scalar("SELECT delivered_at FROM signal_inbox WHERE id = ?1")
            .bind(signal.id.to_string())
            .fetch_one(s.pool())
            .await
            .unwrap();
    assert!(
        delivered_at.is_some(),
        "mark_signals_delivered (batch) must stamp delivered_at, not leave it NULL"
    );
}

// ---------------------------------------------------------------------------
// M5: externalized_state upsert preserves created_at across re-saves
// ---------------------------------------------------------------------------

#[tokio::test]
async fn save_externalized_state_preserves_created_at_on_resave() {
    let s = store().await;
    let inst = make_instance(InstanceState::Running);
    s.create_instance(&inst).await.unwrap();

    let ref_key = format!("ctx-{}", Uuid::now_v7());
    s.save_externalized_state(inst.id, &ref_key, &json!({"v": 1}))
        .await
        .unwrap();

    let created_at_1: String =
        sqlx::query_scalar("SELECT created_at FROM externalized_state WHERE ref_key = ?1")
            .bind(&ref_key)
            .fetch_one(s.pool())
            .await
            .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    s.save_externalized_state(inst.id, &ref_key, &json!({"v": 2}))
        .await
        .unwrap();

    let created_at_2: String =
        sqlx::query_scalar("SELECT created_at FROM externalized_state WHERE ref_key = ?1")
            .bind(&ref_key)
            .fetch_one(s.pool())
            .await
            .unwrap();

    assert_eq!(
        created_at_1, created_at_2,
        "re-saving an externalized_state row must preserve the original created_at"
    );

    // The payload itself must still update.
    let payload = s.get_externalized_state(&ref_key).await.unwrap().unwrap();
    assert_eq!(payload, json!({"v": 2}));
}

#[tokio::test]
async fn save_externalized_state_preserves_created_at_on_resave_large_payload() {
    let s = store().await;
    let inst = make_instance(InstanceState::Running);
    s.create_instance(&inst).await.unwrap();

    let ref_key = format!("ctx-large-{}", Uuid::now_v7());
    // Large enough to cross COMPRESSION_THRESHOLD_BYTES and take the zstd branch.
    let big = json!({"blob": "x".repeat(10_000)});
    s.save_externalized_state(inst.id, &ref_key, &big)
        .await
        .unwrap();

    let created_at_1: String =
        sqlx::query_scalar("SELECT created_at FROM externalized_state WHERE ref_key = ?1")
            .bind(&ref_key)
            .fetch_one(s.pool())
            .await
            .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let big2 = json!({"blob": "y".repeat(10_000)});
    s.save_externalized_state(inst.id, &ref_key, &big2)
        .await
        .unwrap();

    let created_at_2: String =
        sqlx::query_scalar("SELECT created_at FROM externalized_state WHERE ref_key = ?1")
            .bind(&ref_key)
            .fetch_one(s.pool())
            .await
            .unwrap();

    assert_eq!(
        created_at_1, created_at_2,
        "re-saving a compressed externalized_state row must preserve the original created_at"
    );

    let payload = s.get_externalized_state(&ref_key).await.unwrap().unwrap();
    assert_eq!(payload, big2);
}

// ---------------------------------------------------------------------------
// L1: delete_terminal_instances removes old terminal instances and their
// non-cascaded history (step_logs, audit_log, usage_events), but leaves
// recent or non-terminal instances alone.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_terminal_instances_removes_old_terminal_rows_and_history() {
    let s = store().await;

    let mut old = make_instance(InstanceState::Completed);
    old.updated_at = Utc::now() - chrono::Duration::days(1);
    s.create_instance(&old).await.unwrap();

    let fresh = make_instance(InstanceState::Completed);
    s.create_instance(&fresh).await.unwrap();

    let running = make_instance(InstanceState::Running);
    s.create_instance(&running).await.unwrap();

    // Non-cascaded history for the old instance -- must be cleaned up
    // explicitly since neither `step_logs` nor `audit_log` (on SQLite) nor
    // `usage_events` declare an FK to `task_instances`.
    s.append_step_logs(
        old.id,
        &BlockId::new("s1"),
        &[orch8_types::step_log::StepLogEntry {
            ts: old.updated_at,
            level: "info".into(),
            message: "did a thing".into(),
        }],
    )
    .await
    .unwrap();
    s.append_audit_log(&orch8_types::audit::AuditLogEntry {
        id: Uuid::now_v7(),
        instance_id: old.id,
        tenant_id: old.tenant_id.clone(),
        event_type: "state_transition".into(),
        from_state: Some("running".into()),
        to_state: Some("completed".into()),
        block_id: None,
        details: json!({}),
        created_at: old.updated_at,
    })
    .await
    .unwrap();
    s.record_usage_event(&orch8_storage::UsageEvent {
        tenant_id: old.tenant_id.as_str().to_string(),
        instance_id: Some(old.id),
        block_id: None,
        kind: "llm_tokens".into(),
        model: "test-model".into(),
        input_tokens: 10,
        output_tokens: 5,
        created_at: old.updated_at,
    })
    .await
    .unwrap();

    // Cutoff between `old`'s backdated timestamp and `fresh`/`running`'s
    // (created at "now" a moment ago) -- only `old` should be swept.
    let cutoff = Utc::now() - chrono::Duration::hours(1);
    let deleted = s.delete_terminal_instances(cutoff, 100).await.unwrap();
    assert_eq!(deleted, 1, "only the old terminal instance should be swept");

    assert!(
        s.get_instance(old.id).await.unwrap().is_none(),
        "old terminal instance should be deleted"
    );
    assert!(
        s.get_instance(fresh.id).await.unwrap().is_some(),
        "recent terminal instance must survive"
    );
    assert!(
        s.get_instance(running.id).await.unwrap().is_some(),
        "non-terminal instance must survive regardless of age"
    );

    assert!(
        s.list_step_logs(old.id).await.unwrap().is_empty(),
        "step_logs for the deleted instance must be cleaned up (no FK cascade)"
    );
    assert!(
        s.list_audit_log(old.id, 10).await.unwrap().is_empty(),
        "audit_log for the deleted instance must be cleaned up (no FK cascade on SQLite)"
    );
    let (input_tokens, output_tokens) = s.query_instance_usage_totals(old.id).await.unwrap();
    assert_eq!(
        (input_tokens, output_tokens),
        (0, 0),
        "usage_events for the deleted instance must be cleaned up (no FK cascade)"
    );

    // Re-running with the same cutoff must be a no-op (idempotent).
    let deleted_again = s.delete_terminal_instances(cutoff, 100).await.unwrap();
    assert_eq!(deleted_again, 0);
}

// ---------------------------------------------------------------------------
// R4: rollback_policies/rollback_history timestamps must round-trip the real
// stored value, not silently substitute `Utc::now()` at read time.
//
// `rollback_policies.created_at`/`updated_at` and `rollback_history.
// triggered_at` are populated exclusively via the SQL-level `DEFAULT
// (datetime('now'))` / `updated_at = datetime('now')` (SQLite-native
// "YYYY-MM-DD HH:MM:SS" format) -- no Rust code path ever binds them. The
// read paths used to parse them with `chrono::DateTime::parse_from_rfc3339`,
// which always fails against that format, and silently substituted
// `Utc::now()` via `.map_or_else(|_| Utc::now(), ...)` -- every read
// returned the *current* time instead of the *stored* time, on every backend
// call, unconditionally. Found during the R4 SQLite timestamp-discipline
// audit; a worse bug than H1 (H1 was a comparison edge case, this is
// unconditional wrong data on every read).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rollback_policy_created_at_survives_read_not_replaced_with_now() {
    let s = store().await;
    s.create_rollback_policy("t1", "seq-a", 0.5, 300, None, None, None)
        .await
        .unwrap();

    // SQLite's `datetime('now')` resolves to whole seconds -- sleep past the
    // second boundary so a buggy "return Utc::now() at read time" read is
    // unambiguously distinguishable from the true (earlier) creation time.
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

    let policy = s
        .get_rollback_policy("t1", "seq-a")
        .await
        .unwrap()
        .expect("policy should exist");
    let age = Utc::now().signed_duration_since(policy.created_at);
    assert!(
        age >= chrono::Duration::seconds(1),
        "created_at should reflect the real insert time (age={age}), not be \
         silently replaced with the current read time (age would be ~0)"
    );

    let listed = s
        .list_rollback_policies(Some("t1"), 10)
        .await
        .unwrap()
        .into_iter()
        .find(|p| p.sequence_name == "seq-a")
        .expect("policy should be listed");
    let listed_age = Utc::now().signed_duration_since(listed.created_at);
    assert!(
        listed_age >= chrono::Duration::seconds(1),
        "list_rollback_policies must also return the real created_at"
    );
}

#[tokio::test]
async fn rollback_history_triggered_at_survives_read_not_replaced_with_now() {
    let s = store().await;
    s.record_rollback("t1", "seq-b", 0.9, 0.5, "error rate exceeded")
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

    let history = s
        .list_rollback_history(Some("t1"), Some("seq-b"), 10)
        .await
        .unwrap();
    let entry = history.first().expect("history entry should exist");
    let age = Utc::now().signed_duration_since(entry.triggered_at);
    assert!(
        age >= chrono::Duration::seconds(1),
        "triggered_at should reflect the real record_rollback time (age={age}), \
         not be silently replaced with the current read time"
    );
}
