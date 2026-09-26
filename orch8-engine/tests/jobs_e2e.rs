//! End-to-end tests for background jobs: a job is an instance of an
//! auto-managed single-step system sequence, so it must run through the
//! ordinary evaluator — in-process handlers, retry/DLQ semantics and
//! external-worker dispatch — without any job-specific engine code.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::Utc;
use common::*;
use orch8_engine::jobs::{JobSpec, NewJob, build_job_instance, ensure_job_sequence, load_job};
use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
use orch8_types::ids::{Namespace, TenantId};
use orch8_types::instance::{InstanceState, Priority};
use orch8_types::job::{JobRetry, JobStatus};
use serde_json::json;

async fn enqueue(
    storage: &Arc<dyn StorageBackend>,
    spec: JobSpec,
    payload: serde_json::Value,
) -> (
    orch8_types::instance::TaskInstance,
    orch8_types::sequence::SequenceDefinition,
) {
    let tenant = TenantId::unchecked("t1");
    let ns = Namespace::new("default");
    let seq = ensure_job_sequence(storage.as_ref(), &tenant, &ns, &spec)
        .await
        .unwrap();
    let inst = build_job_instance(
        NewJob {
            tenant_id: tenant,
            namespace: ns,
            spec,
            payload,
            priority: Priority::Normal,
            run_at: Utc::now(),
            idempotency_key: None,
            metadata: None,
        },
        seq.id,
    );
    storage.create_instance(&inst).await.unwrap();
    (inst, seq)
}

fn spec(handler: &str, retry: Option<u32>) -> JobSpec {
    JobSpec {
        handler: handler.into(),
        queue: None,
        retry: retry.map(|n| JobRetry {
            max_attempts: n,
            initial_backoff_ms: 1,
            max_backoff_ms: Some(1),
        }),
    }
}

#[tokio::test]
async fn job_runs_in_process_handler_with_payload_as_params() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let mut reg = registry();
    reg.register("echo_job", |ctx| {
        Box::pin(async move { Ok(json!({"echo": ctx.params})) })
    });

    let (inst, seq) = enqueue(
        &storage,
        spec("echo_job", None),
        json!({"to": "a@b.c", "n": 2}),
    )
    .await;
    let before = load_job(storage.as_ref(), &inst).await.unwrap().unwrap();
    assert_eq!(before.status, JobStatus::Scheduled);
    assert_eq!(before.attempts, Some(0));

    drive(&storage, &reg, inst.id, &seq).await;

    let done = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(done.state, InstanceState::Completed);
    let job = load_job(storage.as_ref(), &done).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Completed);
    assert_eq!(job.attempts, Some(1));
    assert_eq!(
        job.output,
        Some(json!({"echo": {"to": "a@b.c", "n": 2}})),
        "payload must reach the handler verbatim as params"
    );
    assert!(job.error.is_none());
}

#[tokio::test]
async fn job_sequence_is_created_once_per_spec() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (_, a) = enqueue(&storage, spec("h", None), json!({})).await;
    let (_, b) = enqueue(&storage, spec("h", None), json!({})).await;
    let (_, c) = enqueue(&storage, spec("h", Some(3)), json!({})).await;
    assert_eq!(a.id, b.id, "same spec must reuse the system sequence");
    assert_ne!(a.id, c.id, "a different retry policy gets its own sequence");
    assert_eq!(a.name, "_job.h");
}

#[tokio::test]
async fn job_with_retry_exhausts_attempts_then_dead_letters() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_clone = Arc::clone(&calls);
    let mut reg = registry();
    reg.register("flaky_job", move |_ctx| {
        let c = Arc::clone(&calls_clone);
        Box::pin(async move {
            c.fetch_add(1, Ordering::SeqCst);
            Err(orch8_types::error::StepError::Retryable {
                message: "smtp timeout".into(),
                details: None,
            })
        })
    });

    let (inst, seq) = enqueue(&storage, spec("flaky_job", Some(3)), json!({})).await;
    drive(&storage, &reg, inst.id, &seq).await;

    let failed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(failed.state, InstanceState::Failed);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "job max_attempts counts total executions"
    );
    let job = load_job(storage.as_ref(), &failed).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::DeadLettered);
    assert_eq!(job.attempts, Some(3));
    assert!(
        job.error
            .as_deref()
            .is_some_and(|e| e.contains("smtp timeout")),
        "error must surface the handler message, got {:?}",
        job.error
    );
}

#[tokio::test]
async fn job_without_retry_fails_after_one_attempt() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let reg = registry_with_retryable_fail();
    let (inst, seq) = enqueue(&storage, spec("retryable_fail", None), json!({})).await;
    drive(&storage, &reg, inst.id, &seq).await;
    let failed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(failed.state, InstanceState::Failed);
    let job = load_job(storage.as_ref(), &failed).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Failed);
    assert_eq!(job.attempts, Some(1));
}

#[tokio::test]
async fn job_for_unregistered_handler_is_dispatched_to_external_worker_queue() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let reg = registry();
    let mut s = spec("resize_image", Some(2));
    s.queue = Some("gpu".into());
    let (inst, seq) = enqueue(&storage, s, json!({"url": "s3://x"})).await;
    // The evaluator dispatches the step to the worker queue and keeps
    // yielding until a worker reports back; a few passes are enough.
    drive_n(&storage, &reg, inst.id, &seq, 3).await;

    let dispatched = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert!(!dispatched.state.is_terminal());
    let job = load_job(storage.as_ref(), &dispatched)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        job.status,
        JobStatus::from_instance_state(dispatched.state, true)
    );
    assert_eq!(job.queue.as_deref(), Some("gpu"));

    let tasks = storage
        .list_worker_tasks(
            &orch8_types::worker_filter::WorkerTaskFilter {
                handler_name: Some("resize_image".into()),
                ..Default::default()
            },
            &orch8_types::filter::Pagination::default(),
        )
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].queue_name.as_deref(), Some("gpu"));
    assert_eq!(tasks[0].params, json!({"url": "s3://x"}));
    assert_eq!(tasks[0].instance_id, inst.id);
}
