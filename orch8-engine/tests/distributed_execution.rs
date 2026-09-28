//! Distributed execution (runtime nodes) — engine-level regression tests.
//!
//! Every scenario runs against `SQLite` and, when `DATABASE_URL` is set,
//! against Postgres too (skipped otherwise, like the storage PG suite).
//! Postgres rows are shared across parallel tests, so assertions look at the
//! scenario's own instance/task only — never at global reaper counters.
#![allow(clippy::too_many_lines)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use orch8_engine::effect_guard::commit_external_worker_effect;
use orch8_engine::worker_lease::reap_worker_tasks;
use orch8_storage::StorageBackend;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::continuity::{
    EffectState, OwnershipState, RuntimeCapabilities, RuntimeConnectivity, RuntimeId, RuntimeKind,
    RuntimeTrustLevel,
};
use orch8_types::execution::NodeState;
use orch8_types::filter::Pagination;
use orch8_types::instance::InstanceState;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};
use orch8_types::worker::{WorkerTask, WorkerTaskState};
use orch8_types::worker_filter::WorkerTaskFilter;

use common::{drive, mk_instance, mk_sequence, mk_step, mk_step_with_retry, registry};

async fn backends() -> Vec<(&'static str, Arc<dyn StorageBackend>)> {
    let mut out: Vec<(&'static str, Arc<dyn StorageBackend>)> = vec![(
        "sqlite",
        Arc::new(SqliteStorage::in_memory().await.unwrap()),
    )];
    if let Ok(url) = std::env::var("DATABASE_URL") {
        let pg = PostgresStorage::new(&url, 5, None)
            .await
            .expect("connect to DATABASE_URL");
        pg.run_migrations().await.expect("run migrations");
        out.push(("postgres", Arc::new(pg)));
    } else {
        eprintln!("DATABASE_URL not set: running the sqlite leg only");
    }
    out
}

async fn start(
    storage: &Arc<dyn StorageBackend>,
    blocks: Vec<BlockDefinition>,
) -> (SequenceDefinition, orch8_types::instance::TaskInstance) {
    let mut seq = mk_sequence(blocks);
    seq.name = format!("dist-{}", uuid::Uuid::now_v7());
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();
    dispatch(storage, inst.id, &seq).await;
    (seq, inst)
}

/// Evaluate until the step is handed to the worker queue, then park the
/// instance the way `process_instance_tree` does (`Waiting`).
async fn dispatch(
    storage: &Arc<dyn StorageBackend>,
    instance_id: orch8_types::ids::InstanceId,
    seq: &SequenceDefinition,
) {
    common::drive_n(storage, &registry(), instance_id, seq, 5).await;
    let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
    if inst.state == InstanceState::Running {
        storage
            .update_instance_state(instance_id, InstanceState::Waiting, None)
            .await
            .unwrap();
    }
}

async fn tasks_of(
    storage: &Arc<dyn StorageBackend>,
    instance_id: orch8_types::ids::InstanceId,
) -> Vec<WorkerTask> {
    storage
        .list_worker_tasks(
            &WorkerTaskFilter {
                instance_id: Some(instance_id),
                ..WorkerTaskFilter::default()
            },
            &Pagination::default(),
        )
        .await
        .unwrap()
}

async fn only_task(
    storage: &Arc<dyn StorageBackend>,
    instance_id: orch8_types::ids::InstanceId,
) -> WorkerTask {
    let tasks = tasks_of(storage, instance_id).await;
    assert_eq!(
        tasks.len(),
        1,
        "expected exactly one worker task: {tasks:?}"
    );
    tasks.into_iter().next().unwrap()
}

async fn receipt_state(storage: &Arc<dyn StorageBackend>, task: &WorkerTask) -> EffectState {
    let tenant = orch8_types::ids::TenantId::unchecked("t");
    storage
        .get_effect_receipt(&tenant, task.effect_id.expect("task carries an effect id"))
        .await
        .unwrap()
        .expect("receipt exists")
        .state
}

fn browser_caps(runtime_id: RuntimeId, handler: &str) -> RuntimeCapabilities {
    let now = Utc::now();
    RuntimeCapabilities {
        runtime_id,
        kind: RuntimeKind::Browser,
        trust: RuntimeTrustLevel::Registered,
        handlers: vec![handler.into()],
        plugins: Vec::new(),
        credentials: Vec::new(),
        regions: Vec::new(),
        hardware: Vec::new(),
        offline_capable: false,
        connectivity: Some(RuntimeConnectivity::Wifi),
        battery_percent: None,
        estimated_cost_microunits: None,
        estimated_latency_ms: None,
        draining: false,
        capsule_signing_public_key: None,
        observed_at: now,
        expires_at: now + chrono::Duration::minutes(4),
    }
}

async fn claim(storage: &Arc<dyn StorageBackend>, task: &WorkerTask) -> WorkerTask {
    // Claim this exact row: other parallel tests share the Postgres table, so
    // target it through a unique handler name per scenario.
    let claimed = storage
        .claim_worker_tasks(&task.handler_name, "worker-a", 10)
        .await
        .unwrap();
    claimed
        .into_iter()
        .find(|claimed| claimed.id == task.id)
        .expect("task claimed")
}

fn unique_handler(prefix: &str) -> String {
    format!("{prefix}.{}", uuid::Uuid::now_v7().simple())
}

#[tokio::test]
async fn dispatch_binds_effect_id_and_ownership_epoch() {
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.charge");
        let (_, inst) = start(&storage, vec![mk_step("charge", &handler)]).await;
        assert_eq!(
            storage.get_instance(inst.id).await.unwrap().unwrap().state,
            InstanceState::Waiting,
            "{backend}"
        );
        let task = only_task(&storage, inst.id).await;
        assert!(
            task.effect_id.is_some(),
            "{backend}: effect id stored at dispatch"
        );
        assert_eq!(task.continuity_epoch, Some(0), "{backend}");
        assert!(!task.carries_credentials, "{backend}");
        assert_eq!(
            receipt_state(&storage, &task).await,
            EffectState::Dispatched
        );

        // The poll/claim response carries the same id.
        let claimed = claim(&storage, &task).await;
        assert_eq!(claimed.effect_id, task.effect_id, "{backend}");
    }
}

#[tokio::test]
async fn lease_expiry_with_side_effect_goes_unknown_and_fails_node() {
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.charge");
        let (seq, inst) = start(&storage, vec![mk_step("charge", &handler)]).await;
        let task = only_task(&storage, inst.id).await;
        let claimed = claim(&storage, &task).await;

        reap_worker_tasks(storage.as_ref(), Duration::ZERO)
            .await
            .unwrap();

        assert_eq!(
            receipt_state(&storage, &task).await,
            EffectState::Unknown,
            "{backend}: never blindly requeued"
        );
        let after = storage.get_worker_task(task.id).await.unwrap().unwrap();
        assert_eq!(after.state, WorkerTaskState::Failed, "{backend}");
        // The stale holder can no longer complete.
        assert!(
            !storage
                .complete_worker_task(
                    task.id,
                    &orch8_types::worker::WorkerClaim::new("worker-a", claimed.claim_epoch),
                    &json!({"ok": true}),
                )
                .await
                .unwrap(),
            "{backend}"
        );
        let tree = storage.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(common::node_state(&tree, "charge"), NodeState::Failed);
        drive(&storage, &registry(), inst.id, &seq).await;
        assert_eq!(
            storage.get_instance(inst.id).await.unwrap().unwrap().state,
            InstanceState::Failed,
            "{backend}: instance advanced, not dangling"
        );
    }
}

#[tokio::test]
async fn lease_expiry_with_retry_policy_schedules_a_fresh_attempt() {
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.charge");
        let (seq, inst) = start(&storage, vec![mk_step_with_retry("charge", &handler, 3)]).await;
        let first = only_task(&storage, inst.id).await;
        claim(&storage, &first).await;

        reap_worker_tasks(storage.as_ref(), Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(receipt_state(&storage, &first).await, EffectState::Unknown);

        let retry = only_task(&storage, inst.id).await;
        assert_ne!(retry.id, first.id, "{backend}");
        assert_eq!(retry.attempt, first.attempt + 1, "{backend}");
        assert_eq!(retry.state, WorkerTaskState::Pending, "{backend}");

        // Re-dispatch binds the next attempt's own receipt onto the row.
        dispatch(&storage, inst.id, &seq).await;
        let rebound = only_task(&storage, inst.id).await;
        assert_eq!(rebound.id, retry.id, "{backend}");
        let new_effect = rebound.effect_id.expect("next attempt bound to a receipt");
        assert_ne!(
            Some(new_effect),
            first.effect_id,
            "{backend}: fresh effect id"
        );
        assert_eq!(
            receipt_state(&storage, &rebound).await,
            EffectState::Dispatched
        );
    }
}

#[tokio::test]
async fn pure_task_lease_expiry_requeues() {
    for (backend, storage) in backends().await {
        let (_, inst) = start(&storage, vec![mk_step("noop", "noop")]).await;
        let handler = unique_handler("pure");
        let now = Utc::now();
        let task = WorkerTask {
            id: uuid::Uuid::now_v7(),
            instance_id: inst.id,
            block_id: orch8_types::ids::BlockId::new("pure-step"),
            handler_name: handler.clone(),
            queue_name: None,
            requirements: orch8_types::continuity::CapsuleRequirements::default(),
            params: json!({}),
            context: json!({}),
            attempt: 0,
            timeout_ms: None,
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
            created_at: now,
            effect_id: None,
            continuity_epoch: None,
            lease_secs: None,
            carries_credentials: false,
            claimed_runtime_kind: None,
        };
        storage.create_worker_task(&task).await.unwrap();
        // `pure.*` is not a builtin, so it is side-effect classified; without a
        // stored receipt and no effect scope for it, it is still pure here.
        let claimed = claim(&storage, &task).await;
        reap_worker_tasks(storage.as_ref(), Duration::ZERO)
            .await
            .unwrap();
        let after = storage.get_worker_task(task.id).await.unwrap().unwrap();
        assert_eq!(after.state, WorkerTaskState::Pending, "{backend}");
        assert!(after.worker_id.is_none(), "{backend}");
        assert_eq!(after.claim_epoch, claimed.claim_epoch, "{backend}");
    }
}

#[tokio::test]
async fn timed_out_pending_task_abandons_receipt_and_advances() {
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.slow");
        let (seq, inst) = start(
            &storage,
            vec![common::mk_step_with_timeout("slow", &handler, 1)],
        )
        .await;
        let task = only_task(&storage, inst.id).await;
        tokio::time::sleep(Duration::from_millis(1_100)).await;

        reap_worker_tasks(storage.as_ref(), Duration::from_secs(3600))
            .await
            .unwrap();

        assert_eq!(
            receipt_state(&storage, &task).await,
            EffectState::Abandoned,
            "{backend}: never claimed, so the effect provably did not happen"
        );
        let after = storage.get_worker_task(task.id).await.unwrap().unwrap();
        assert_eq!(after.state, WorkerTaskState::Failed, "{backend}");
        drive(&storage, &registry(), inst.id, &seq).await;
        assert_eq!(
            storage.get_instance(inst.id).await.unwrap().unwrap().state,
            InstanceState::Failed,
            "{backend}"
        );
    }
}

#[tokio::test]
async fn settlement_uses_the_stored_effect_id_after_an_epoch_change() {
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.charge");
        let (_, inst) = start(&storage, vec![mk_step("charge", &handler)]).await;
        let task = only_task(&storage, inst.id).await;
        let tenant = inst.tenant_id.clone();

        // Advance the ownership epoch (as a handoff would) — recomputing the
        // id from the current epoch would now miss the receipt.
        let execution = storage
            .get_continuity_execution_by_instance(&tenant, inst.id)
            .await
            .unwrap()
            .unwrap();
        let mut next = execution.clone();
        next.epoch = execution.epoch.checked_next().unwrap();
        next.state = OwnershipState::Owned;
        assert!(
            storage
                .cas_continuity_owner(
                    &tenant,
                    execution.continuity_id,
                    execution.epoch,
                    execution.owner_runtime_id,
                    &next,
                )
                .await
                .unwrap(),
            "{backend}"
        );

        commit_external_worker_effect(storage.as_ref(), &tenant, &task, &json!({"id": "ch_1"}))
            .await
            .unwrap();
        assert_eq!(
            receipt_state(&storage, &task).await,
            EffectState::Committed,
            "{backend}"
        );
    }
}

#[tokio::test]
async fn claimant_kind_sets_the_per_task_lease() {
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.render");
        let (_, inst) = start(&storage, vec![mk_step("render", &handler)]).await;
        let task = only_task(&storage, inst.id).await;
        let runtime_id = RuntimeId::new();
        let claimed = storage
            .claim_worker_tasks_matching(
                &handler,
                &runtime_id.to_string(),
                None,
                None,
                &browser_caps(runtime_id, &handler),
                10,
            )
            .await
            .unwrap();
        let claimed = claimed.into_iter().find(|c| c.id == task.id).unwrap();
        assert_eq!(claimed.lease_secs, Some(30), "{backend}");
        assert_eq!(claimed.claimed_runtime_kind, Some(RuntimeKind::Browser));

        // A zero default lease does not expire a task holding its own 30s lease.
        let expired = storage
            .list_expired_worker_leases(Duration::ZERO, 1_000)
            .await
            .unwrap();
        assert!(
            expired.iter().all(|t| t.id != task.id),
            "{backend}: per-task lease wins over the default"
        );
    }
}

#[tokio::test]
async fn legacy_reaper_never_requeues_an_ambiguous_side_effect() {
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.charge");
        let (_, inst) = start(&storage, vec![mk_step("charge", &handler)]).await;
        let task = only_task(&storage, inst.id).await;
        claim(&storage, &task).await;
        storage
            .reap_stale_worker_tasks(Duration::ZERO)
            .await
            .unwrap();
        let after = storage.get_worker_task(task.id).await.unwrap().unwrap();
        assert_eq!(
            after.state,
            WorkerTaskState::Claimed,
            "{backend}: the storage-level reaper skips dispatched receipts"
        );
    }
}

async fn move_ownership(
    storage: &Arc<dyn StorageBackend>,
    instance_id: orch8_types::ids::InstanceId,
    next_state: OwnershipState,
    bump_epoch: bool,
) {
    let tenant = orch8_types::ids::TenantId::unchecked("t");
    let execution = storage
        .get_continuity_execution_by_instance(&tenant, instance_id)
        .await
        .unwrap()
        .unwrap();
    let mut next = execution.clone();
    next.state = next_state;
    if bump_epoch {
        next.epoch = execution.epoch.checked_next().unwrap();
    }
    assert!(
        storage
            .cas_continuity_owner(
                &tenant,
                execution.continuity_id,
                execution.epoch,
                execution.owner_runtime_id,
                &next,
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn worker_lease_mutations_are_fenced_on_ownership_epoch() {
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.charge");
        let (_, inst) = start(&storage, vec![mk_step("charge", &handler)]).await;
        let task = only_task(&storage, inst.id).await;
        let tenant = inst.tenant_id.clone();
        assert!(
            orch8_engine::ownership::worker_task_ownership_current(
                storage.as_ref(),
                &tenant,
                &task
            )
            .await
            .unwrap(),
            "{backend}"
        );

        move_ownership(&storage, inst.id, OwnershipState::Transferring, false).await;
        assert!(
            !orch8_engine::ownership::worker_task_ownership_current(
                storage.as_ref(),
                &tenant,
                &task
            )
            .await
            .unwrap(),
            "{backend}: no lease mutation while the execution is being exported"
        );

        move_ownership(&storage, inst.id, OwnershipState::Owned, true).await;
        assert!(
            !orch8_engine::ownership::worker_task_ownership_current(
                storage.as_ref(),
                &tenant,
                &task
            )
            .await
            .unwrap(),
            "{backend}: a task dispatched under an older owner epoch is stale"
        );
    }
}

#[tokio::test]
async fn export_is_refused_while_a_worker_task_is_in_flight() {
    use orch8_engine::capsule::{CapsuleExportRequest, CapsuleServiceError};
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.charge");
        let (_, inst) = start(&storage, vec![mk_step("charge", &handler)]).await;
        let continuity = storage
            .get_continuity_execution_by_instance(&inst.tenant_id, inst.id)
            .await
            .unwrap()
            .unwrap();
        let result = orch8_engine::capsule::export_paused_capsule(
            storage.as_ref(),
            CapsuleExportRequest {
                continuity,
                destination_runtime_id: Some(RuntimeId::new()),
                requirements: orch8_types::continuity::CapsuleRequirements::default(),
                expires_at: Utc::now() + chrono::Duration::minutes(5),
                signing_key_id: "k".into(),
                encryption_key_id: "e".into(),
            },
            &ed25519_dalek::SigningKey::from_bytes(&[7; 32]),
            &orch8_types::encryption::FieldEncryptor::from_bytes(&[9; 32]),
        )
        .await;
        assert!(
            matches!(result, Err(CapsuleServiceError::WorkerTasksInFlight(1))),
            "{backend}: {result:?}"
        );
    }
}
