//! `worker_task_stats(window_secs)`: terminal rows are windowed by
//! `completed_at`, live gauges are not, `None` keeps lifetime counts.
//! Runs on `SQLite` and (when `DATABASE_URL` is set) Postgres.

use chrono::{Duration, Utc};
use serde_json::json;
use uuid::Uuid;

use orch8_storage::backend_test;
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::worker::{WorkerTask, WorkerTaskState};

fn task(
    instance_id: InstanceId,
    n: usize,
    handler: &str,
    state: WorkerTaskState,
    age_secs: i64,
) -> WorkerTask {
    let terminal = matches!(state, WorkerTaskState::Completed | WorkerTaskState::Failed);
    let at = Utc::now() - Duration::seconds(age_secs);
    WorkerTask {
        id: Uuid::now_v7(),
        instance_id,
        block_id: BlockId::new(format!("step_{n}")),
        handler_name: handler.into(),
        queue_name: None,
        requirements: orch8_types::continuity::CapsuleRequirements::default(),
        params: json!({}),
        context: json!({}),
        attempt: 1,
        timeout_ms: None,
        state,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        claim_epoch: 0,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: terminal.then_some(at),
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: at,
        effect_id: None,
        continuity_epoch: None,
        lease_secs: None,
        carries_credentials: false,
        claimed_runtime_kind: None,
    }
}

backend_test!(
    async fn stats_window_limits_terminal_rows_only(b) {
        let seq = SequenceBuilder::new("t").step("s", "noop").build();
        b.storage.create_sequence(&seq).await.unwrap();
        let inst = InstanceBuilder::for_sequence(&seq).build();
        b.storage.create_instance(&inst).await.unwrap();

        // 10 old successes, 3 fresh failures, 1 old failure, 1 old pending.
        let mut seeded = Vec::new();
        for _ in 0..10 {
            seeded.push((WorkerTaskState::Completed, 86_400));
        }
        for _ in 0..3 {
            seeded.push((WorkerTaskState::Failed, 5));
        }
        seeded.push((WorkerTaskState::Failed, 86_400));
        seeded.push((WorkerTaskState::Pending, 86_400));
        for (n, (state, age)) in seeded.into_iter().enumerate() {
            let t = task(inst.id, n, "h", state, age);
            b.storage.create_worker_task(&t).await.unwrap();
            if let Some(at) = t.completed_at {
                // Postgres does not insert `completed_at` on create (it is stamped
                // on resolution); set it directly.
                b.raw
                    .backdate("worker_tasks", "completed_at", "id", &t.id.to_string(), at)
                    .await;
            }
        }

        let lifetime = b.storage.worker_task_stats(None, None).await.unwrap();
        assert_eq!(lifetime.by_handler["h"]["completed"], 10, "{}", b.name());
        assert_eq!(lifetime.by_handler["h"]["failed"], 4, "{}", b.name());

        let recent = b.storage.worker_task_stats(None, Some(300)).await.unwrap();
        let h = &recent.by_handler["h"];
        assert_eq!(h.get("completed").copied().unwrap_or(0), 0, "{}", b.name());
        assert_eq!(h["failed"], 3, "{}", b.name());
        assert_eq!(h["pending"], 1, "live gauge must not be windowed ({})", b.name());
        assert_eq!(recent.by_state["failed"], 3);

        let wide = b.storage.worker_task_stats(None, Some(2 * 86_400)).await.unwrap();
        assert_eq!(wide.by_handler["h"]["completed"], 10);
        assert_eq!(wide.by_handler["h"]["failed"], 4);
    }
);
