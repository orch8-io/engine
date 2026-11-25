use std::time::Duration;

use chrono::Utc;
use orch8_types::ids::{BlockId, TenantId};
use orch8_types::instance::InstanceState;
use orch8_types::worker::{WorkerClaim, WorkerTask, WorkerTaskState};
use serde_json::json;
use uuid::Uuid;

use super::{
    ConformanceError, ConformanceReport, backend, conformance_instance, conformance_sequence,
    require,
};
use crate::StorageBackend;

const ADMISSION: &str = "worker_admission_ready";
const CLAIM: &str = "worker_exclusive_claim";
const RENEW: &str = "worker_lease_renewal";
const CHECKPOINT: &str = "worker_checkpoint_monotonic";
const EXPIRE: &str = "worker_lease_expiry";
const FENCE: &str = "worker_previous_claim_fenced";

/// Verify worker ownership, expiry, and resumable checkpoints on an isolated
/// test backend. The fixture expires leases with a zero stale threshold.
pub async fn run_worker_lease_conformance(
    storage: &dyn StorageBackend,
) -> Result<ConformanceReport, ConformanceError> {
    backend(ADMISSION, storage.verify_worker_admission().await)?;
    let marker = Uuid::now_v7();
    let scope = format!("orch8-lease-conformance-{marker}");
    let tenant = TenantId::new(scope.clone()).map_err(|message| ConformanceError::Violation {
        check: "scope",
        message,
    })?;
    let sequence = conformance_sequence(&tenant, marker);
    let instance = conformance_instance(&tenant, sequence.id, marker);
    let task = fixture(instance.id, &scope);
    backend(CLAIM, storage.create_sequence(&sequence).await)?;
    backend(CLAIM, storage.create_instance(&instance).await)?;
    backend(CLAIM, storage.create_worker_task(&task).await)?;

    let worker = "conformance-worker";
    let leased = exclusive_claim(storage, &tenant, &task, worker).await?;
    renew_and_checkpoint(storage, &leased, worker).await?;
    expire_and_resume(storage, &tenant, &leased, worker).await?;
    backend(
        FENCE,
        storage
            .update_instance_state(instance.id, InstanceState::Completed, None)
            .await,
    )?;
    Ok(ConformanceReport {
        scope,
        instance_id: instance.id,
        sequence_id: sequence.id,
        checks: vec![ADMISSION, CLAIM, RENEW, CHECKPOINT, EXPIRE, FENCE],
    })
}

async fn exclusive_claim(
    storage: &dyn StorageBackend,
    tenant: &TenantId,
    task: &WorkerTask,
    worker: &str,
) -> Result<WorkerTask, ConformanceError> {
    let (a, b) = tokio::join!(
        storage.claim_worker_tasks_for_tenant(&task.handler_name, worker, tenant, 1),
        storage.claim_worker_tasks_for_tenant(&task.handler_name, worker, tenant, 1),
    );
    let mut claims = backend(CLAIM, a)?;
    claims.extend(backend(CLAIM, b)?);
    require(
        CLAIM,
        claims.len() == 1 && claims[0].id == task.id && claims[0].claim_epoch == 1,
        "concurrent pollers did not issue exactly one first-generation lease",
    )?;
    Ok(claims.remove(0))
}

async fn renew_and_checkpoint(
    storage: &dyn StorageBackend,
    leased: &WorkerTask,
    worker: &str,
) -> Result<(), ConformanceError> {
    let claim = WorkerClaim::new(worker, leased.claim_epoch);
    let renewed = backend(
        RENEW,
        storage.heartbeat_worker_task(leased.id, &claim).await,
    )?;
    let current = backend(RENEW, storage.get_worker_task(leased.id).await)?;
    require(
        RENEW,
        renewed
            && current.is_some_and(|task| {
                task.heartbeat_at.is_some()
                    && task.heartbeat_at >= leased.heartbeat_at
                    && task.claim_epoch == leased.claim_epoch
            }),
        "renewal did not retain and heartbeat the live lease",
    )?;
    let expired = backend(
        RENEW,
        storage
            .list_expired_worker_leases(Duration::from_secs(3600), 50)
            .await,
    )?;
    require(
        RENEW,
        expired.iter().all(|task| task.id != leased.id),
        "the renewed lease was already expired",
    )?;
    let saved = backend(
        CHECKPOINT,
        storage
            .checkpoint_worker_task(leased.id, &claim, 0, &json!({"offset": 20}))
            .await,
    )?;
    let stale = backend(
        CHECKPOINT,
        storage
            .checkpoint_worker_task(leased.id, &claim, 0, &json!({"offset": 99}))
            .await,
    )?;
    require(
        CHECKPOINT,
        saved == Some(1) && stale.is_none(),
        "checkpoint versions did not prevent a stale overwrite",
    )
}

async fn expire_and_resume(
    storage: &dyn StorageBackend,
    tenant: &TenantId,
    leased: &WorkerTask,
    worker: &str,
) -> Result<(), ConformanceError> {
    let candidates = backend(
        EXPIRE,
        storage.list_expired_worker_leases(Duration::ZERO, 50).await,
    )?;
    let reaped = backend(
        EXPIRE,
        storage.reap_stale_worker_tasks(Duration::ZERO).await,
    )?;
    require(
        EXPIRE,
        reaped == 1 && candidates.iter().any(|task| task.id == leased.id),
        "the expired test lease was not recovered",
    )?;
    let mut next = backend(
        EXPIRE,
        storage
            .claim_worker_tasks_for_tenant(&leased.handler_name, worker, tenant, 1)
            .await,
    )?;
    require(
        EXPIRE,
        next.len() == 1
            && next[0].id == leased.id
            && next[0].claim_epoch == 2
            && next[0].resume_checkpoint == Some(json!({"offset": 20}))
            && next[0].checkpoint_seq == 1,
        "reclaim did not preserve the saved progress under a fresh generation",
    )?;
    let fresh = WorkerClaim::new(worker, next.remove(0).claim_epoch);
    let old = WorkerClaim::new(worker, leased.claim_epoch);
    let stale_completion = backend(
        FENCE,
        storage
            .complete_worker_task(leased.id, &old, &json!({"stale": true}))
            .await,
    )?;
    let stale_checkpoint = backend(
        FENCE,
        storage
            .checkpoint_worker_task(leased.id, &old, 1, &json!({"offset": 99}))
            .await,
    )?;
    let checkpoint = backend(
        FENCE,
        storage
            .checkpoint_worker_task(leased.id, &fresh, 1, &json!({"offset": 40}))
            .await,
    )?;
    require(
        FENCE,
        !stale_completion && stale_checkpoint.is_none() && checkpoint == Some(2),
        "expired authority could mutate a task after reclaim by the same worker ID",
    )?;
    let complete = backend(
        FENCE,
        storage
            .complete_worker_task(leased.id, &fresh, &json!({"done": true}))
            .await,
    )?;
    let final_task = backend(FENCE, storage.get_worker_task(leased.id).await)?;
    require(
        FENCE,
        complete
            && final_task.is_some_and(|task| {
                task.state == WorkerTaskState::Completed
                    && task.output == Some(json!({"done": true}))
            }),
        "the fresh lease could not durably complete the task",
    )
}

fn fixture(instance_id: orch8_types::ids::InstanceId, handler: &str) -> WorkerTask {
    WorkerTask {
        id: Uuid::now_v7(),
        instance_id,
        block_id: BlockId::new("worker-conformance"),
        handler_name: handler.into(),
        queue_name: None,
        requirements: orch8_types::continuity::CapsuleRequirements::default(),
        params: json!({}),
        context: json!({}),
        attempt: 0,
        timeout_ms: None,
        heartbeat_timeout_secs: None,
        wait_for_callback: false,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        claim_epoch: 0,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
        effect_id: None,
        continuity_epoch: None,
        lease_secs: None,
        carries_credentials: false,
        claimed_runtime_kind: None,
    }
}
