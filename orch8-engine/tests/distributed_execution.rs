//! Distributed execution (runtime nodes) — engine-level regression tests.
//!
//! Every scenario runs against `SQLite` and, when `DATABASE_URL` is set,
//! against Postgres too (skipped otherwise, like the storage PG suite).
//! Postgres rows are shared across parallel tests, so assertions look at the
//! scenario's own instance/task only — never at global reaper counters.
// TaskInstance/SequenceDefinition grew (sub-tenant fields); these whole-engine
// test futures are intentionally large and run once each.
#![allow(clippy::large_futures)]
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
        assert!(retry.effect_id.is_none(), "{backend}: not dispatched yet");

        // The retry row is not claimable before its re-dispatch bound an
        // effect id — a worker racing the scheduler gets nothing, so no
        // settlement can ever run against a missing (recomputed) id.
        for claimed in [
            storage
                .claim_worker_tasks(&handler, "racer", 10)
                .await
                .unwrap(),
            storage
                .claim_worker_tasks_matching(
                    &handler,
                    "racer",
                    None,
                    None,
                    &caps_of(RuntimeKind::Server, &handler),
                    10,
                )
                .await
                .unwrap(),
        ] {
            assert!(
                claimed.iter().all(|task| task.id != retry.id),
                "{backend}: a retry row must not be claimable before re-dispatch"
            );
        }

        // Re-dispatch binds the next attempt's own receipt onto the row.
        dispatch(&storage, inst.id, &seq).await;
        let rebound = only_task(&storage, inst.id).await;
        assert_eq!(rebound.id, retry.id, "{backend}");
        assert_eq!(
            claim(&storage, &rebound).await.effect_id,
            rebound.effect_id,
            "{backend}: claimable once bound, with the bound effect id"
        );
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
async fn reported_failure_is_one_fenced_resolution() {
    use orch8_engine::worker_lease::fail_worker_task;
    use orch8_types::worker::WorkerClaim;
    for (backend, storage) in backends().await {
        // Retry policy: receipt unknown, next attempt replaces the task.
        let handler = unique_handler("ext.charge");
        let (_, inst) = start(&storage, vec![mk_step_with_retry("charge", &handler, 2)]).await;
        let first = only_task(&storage, inst.id).await;
        let claimed = claim(&storage, &first).await;
        let instance = storage.get_instance(inst.id).await.unwrap().unwrap();
        let stale = WorkerClaim::new("worker-a", claimed.claim_epoch + 1);
        assert!(
            !fail_worker_task(storage.as_ref(), &instance, &claimed, &stale, "boom", true)
                .await
                .unwrap(),
            "{backend}: a stale claim changes nothing"
        );
        assert_eq!(
            receipt_state(&storage, &first).await,
            EffectState::Dispatched
        );
        let holder = WorkerClaim::new("worker-a", claimed.claim_epoch);
        assert!(
            fail_worker_task(storage.as_ref(), &instance, &claimed, &holder, "boom", true)
                .await
                .unwrap(),
            "{backend}"
        );
        assert_eq!(receipt_state(&storage, &first).await, EffectState::Unknown);
        assert!(
            storage.get_worker_task(first.id).await.unwrap().is_none(),
            "{backend}: superseded by the retry"
        );
        let retry = only_task(&storage, inst.id).await;
        assert_eq!(retry.attempt, first.attempt + 1, "{backend}");
        let events = storage
            .list_worker_task_attempt_events(first.id, 10)
            .await
            .unwrap();
        assert!(
            events.iter().any(|event| event.event
                == orch8_types::worker::WorkerAttemptEventKind::Failed
                && event.reason.as_deref() == Some("boom")),
            "{backend}: {events:?}"
        );

        // No retry policy: the task is marked failed, the node fails.
        let handler = unique_handler("ext.charge");
        let (_, inst) = start(&storage, vec![mk_step("charge", &handler)]).await;
        let task = only_task(&storage, inst.id).await;
        let claimed = claim(&storage, &task).await;
        let instance = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert!(
            fail_worker_task(
                storage.as_ref(),
                &instance,
                &claimed,
                &WorkerClaim::new("worker-a", claimed.claim_epoch),
                "fatal",
                false,
            )
            .await
            .unwrap()
        );
        let after = storage.get_worker_task(task.id).await.unwrap().unwrap();
        assert_eq!(after.state, WorkerTaskState::Failed, "{backend}");
        assert_eq!(after.error_message.as_deref(), Some("fatal"), "{backend}");
        assert_eq!(receipt_state(&storage, &task).await, EffectState::Unknown);
        let tree = storage.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(common::node_state(&tree, "charge"), NodeState::Failed);

        // Paused instance: only the task fails; the instance is untouched.
        let handler = unique_handler("ext.charge");
        let (_, inst) = start(&storage, vec![mk_step_with_retry("charge", &handler, 3)]).await;
        let task = only_task(&storage, inst.id).await;
        let claimed = claim(&storage, &task).await;
        storage
            .update_instance_state(inst.id, InstanceState::Paused, None)
            .await
            .unwrap();
        let instance = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert!(
            fail_worker_task(
                storage.as_ref(),
                &instance,
                &claimed,
                &WorkerClaim::new("worker-a", claimed.claim_epoch),
                "late",
                true,
            )
            .await
            .unwrap()
        );
        assert_eq!(
            storage
                .get_worker_task(task.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            WorkerTaskState::Failed,
            "{backend}"
        );
        assert_eq!(
            storage.get_instance(inst.id).await.unwrap().unwrap().state,
            InstanceState::Paused,
            "{backend}"
        );
        assert_eq!(
            tasks_of(&storage, inst.id).await.len(),
            1,
            "{backend}: no retry"
        );
    }
}

#[tokio::test]
async fn handed_off_source_resolves_its_execution_through_location_history() {
    use orch8_engine::ownership::{LocalOwnership, local_ownership};
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.charge");
        let (seq, source) = start(&storage, vec![mk_step("charge", &handler)]).await;
        let tenant = source.tenant_id.clone();
        let execution = storage
            .get_continuity_execution_by_instance(&tenant, source.id)
            .await
            .unwrap()
            .unwrap();
        // Hand the execution to a new instance (epoch bump records the
        // destination's location; the source keeps its epoch-0 location).
        let destination = mk_instance(seq.id);
        storage.create_instance(&destination).await.unwrap();
        let mut next = execution.clone();
        next.current_instance_id = destination.id;
        next.epoch = execution.epoch.checked_next().unwrap();
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

        // The source is found through the location history (second lookup),
        // the destination through the current-owner index (first lookup).
        let via_history = storage
            .get_continuity_execution_touching_instance(&tenant, source.id)
            .await
            .unwrap()
            .expect("source resolves through its location");
        assert_eq!(
            via_history.continuity_id, execution.continuity_id,
            "{backend}"
        );
        assert_eq!(
            local_ownership(storage.as_ref(), &tenant, source.id)
                .await
                .unwrap(),
            LocalOwnership::Superseded,
            "{backend}"
        );
        assert_eq!(
            local_ownership(storage.as_ref(), &tenant, destination.id)
                .await
                .unwrap(),
            LocalOwnership::Owned,
            "{backend}"
        );
        // Never enrolled: no execution at all.
        let stranger = mk_instance(seq.id);
        storage.create_instance(&stranger).await.unwrap();
        assert!(
            storage
                .get_continuity_execution_touching_instance(&tenant, stranger.id)
                .await
                .unwrap()
                .is_none(),
            "{backend}"
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

async fn seed_credential(storage: &Arc<dyn StorageBackend>, id: &str) {
    let now = Utc::now();
    storage
        .create_credential(&orch8_types::credential::CredentialDef {
            id: id.into(),
            tenant_id: "t".into(),
            name: id.into(),
            kind: orch8_types::credential::CredentialKind::default(),
            value: orch8_types::config::SecretString::from("\"sk_live_secret\"".to_owned()),
            expires_at: None,
            refresh_url: None,
            refresh_token: None,
            enabled: true,
            description: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn browser_placed_step_with_credentials_fails_permanently_at_dispatch() {
    for (backend, storage) in backends().await {
        let credential = format!("cred-{}", uuid::Uuid::now_v7().simple());
        seed_credential(&storage, &credential).await;
        let handler = unique_handler("ext.form");
        let (_, inst) = start(
            &storage,
            vec![common::mk_step_with_params(
                "form",
                &handler,
                json!({
                    "$runtime": {"runtime_kinds": ["browser"]},
                    "api_key": format!("credentials://{credential}")
                }),
            )],
        )
        .await;
        assert!(
            tasks_of(&storage, inst.id).await.is_empty(),
            "{backend}: nothing was enqueued"
        );
        let tree = storage.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(
            common::node_state(&tree, "form"),
            NodeState::Failed,
            "{backend}"
        );
        let output = storage
            .get_block_output(inst.id, &orch8_types::ids::BlockId::new("form"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            output.output["message"], "steps placed on browser runtimes cannot receive credentials",
            "{backend}"
        );
    }
}

#[tokio::test]
async fn browser_claimants_never_receive_credential_bearing_tasks() {
    for (backend, storage) in backends().await {
        let credential = format!("cred-{}", uuid::Uuid::now_v7().simple());
        seed_credential(&storage, &credential).await;
        let handler = unique_handler("ext.sync");
        let (_, inst) = start(
            &storage,
            vec![common::mk_step_with_params(
                "sync",
                &handler,
                json!({"api_key": format!("credentials://{credential}")}),
            )],
        )
        .await;
        let task = only_task(&storage, inst.id).await;
        assert!(task.carries_credentials, "{backend}");

        let browser = RuntimeId::new();
        let claimed = storage
            .claim_worker_tasks_matching(
                &handler,
                &browser.to_string(),
                None,
                None,
                &browser_caps(browser, &handler),
                10,
            )
            .await
            .unwrap();
        assert!(claimed.is_empty(), "{backend}: browser must not claim it");

        let mut mobile = browser_caps(RuntimeId::new(), &handler);
        mobile.kind = RuntimeKind::Mobile;
        let claimed = storage
            .claim_worker_tasks_matching(
                &handler,
                &mobile.runtime_id.to_string(),
                None,
                None,
                &mobile,
                10,
            )
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "{backend}: other kinds still receive credentials"
        );
        assert_eq!(claimed[0].lease_secs, Some(120), "{backend}: mobile lease");
    }
}

fn caps_of(kind: RuntimeKind, handler: &str) -> RuntimeCapabilities {
    let mut caps = browser_caps(RuntimeId::new(), handler);
    caps.kind = kind;
    caps
}

#[tokio::test]
async fn placed_step_dispatches_remotely_even_when_handler_is_local() {
    for (backend, storage) in backends().await {
        // `noop` is registered in-process; placement on mobile wins.
        let (_, inst) = start(
            &storage,
            vec![common::mk_step_with_params(
                "on-phone",
                "noop",
                json!({"$runtime": {"runtime_kinds": ["mobile"]}, "note": "x"}),
            )],
        )
        .await;
        let task = only_task(&storage, inst.id).await;
        assert_eq!(
            task.requirements.runtime_kinds,
            [RuntimeKind::Mobile],
            "{backend}"
        );
        assert!(task.params.get("$runtime").is_none(), "{backend}: stripped");
        let tree = storage.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(
            common::node_state(&tree, "on-phone"),
            NodeState::Waiting,
            "{backend}"
        );

        // Only a mobile node may claim it; a server worker never sees it.
        let server = caps_of(RuntimeKind::Server, "noop");
        assert!(
            storage
                .claim_worker_tasks_matching(
                    "noop",
                    &server.runtime_id.to_string(),
                    None,
                    None,
                    &server,
                    10
                )
                .await
                .unwrap()
                .iter()
                .all(|t| t.id != task.id),
            "{backend}"
        );
        let phone = caps_of(RuntimeKind::Mobile, "noop");
        let claimed = storage
            .claim_worker_tasks_matching(
                "noop",
                &phone.runtime_id.to_string(),
                None,
                None,
                &phone,
                10,
            )
            .await
            .unwrap();
        assert!(claimed.iter().any(|t| t.id == task.id), "{backend}");
    }
}

#[tokio::test]
async fn targeted_task_is_a_per_node_mailbox() {
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.capture");
        let device = caps_of(RuntimeKind::Mobile, &handler);
        let (_, inst) = start(
            &storage,
            vec![common::mk_step_with_params(
                "capture",
                &handler,
                json!({"$runtime": {"runtime_id": device.runtime_id}}),
            )],
        )
        .await;
        let task = only_task(&storage, inst.id).await;
        assert_eq!(
            task.requirements.runtime_id,
            Some(device.runtime_id),
            "{backend}"
        );

        let other = caps_of(RuntimeKind::Mobile, &handler);
        assert!(
            storage
                .claim_worker_tasks_matching(
                    &handler,
                    &other.runtime_id.to_string(),
                    None,
                    None,
                    &other,
                    10
                )
                .await
                .unwrap()
                .is_empty(),
            "{backend}: another device cannot take the mailbox task"
        );
        // Pending mailbox tasks are never touched by the lease reaper.
        reap_worker_tasks(storage.as_ref(), Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            storage
                .get_worker_task(task.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            WorkerTaskState::Pending,
            "{backend}"
        );
        let claimed = storage
            .claim_worker_tasks_matching(
                &handler,
                &device.runtime_id.to_string(),
                None,
                None,
                &device,
                10,
            )
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1, "{backend}: the target device polls it");
    }
}

#[tokio::test]
async fn locality_denial_at_dispatch_fails_permanently_with_recorded_decision() {
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.pii");
        let (_, inst) = start(
            &storage,
            vec![common::mk_step_with_params(
                "pii",
                &handler,
                json!({"$runtime": {
                    "runtime_kinds": ["browser"],
                    "classification": "confidential",
                    "policy": {"version": 1, "rules": [{
                        "classification": "confidential",
                        "allowed_runtime_kinds": ["mobile"],
                        "minimum_trust": null, "require_offline": null,
                        "require_hardware": null, "minimum_battery_percent": null,
                        "maximum_cost_microunits": null, "maximum_latency_ms": null
                    }]}
                }}),
            )],
        )
        .await;
        assert!(tasks_of(&storage, inst.id).await.is_empty(), "{backend}");
        let output = storage
            .get_block_output(inst.id, &orch8_types::ids::BlockId::new("pii"))
            .await
            .unwrap()
            .unwrap();
        let message = output.output["message"].as_str().unwrap().to_owned();
        assert!(
            message.contains("RUNTIME_KIND_DENIED"),
            "{backend}: {message}"
        );
        let decision_id: uuid::Uuid = message.rsplit("decision ").next().unwrap().parse().unwrap();
        let decision = storage
            .get_placement_decision(
                &inst.tenant_id,
                orch8_types::continuity::PlacementDecisionId::from_uuid(decision_id),
            )
            .await
            .unwrap()
            .expect("placement decision recorded");
        assert!(decision.selected_runtime_id.is_none(), "{backend}");
        assert_eq!(
            decision.classification,
            orch8_types::continuity::DataClassification::Confidential
        );
    }
}

#[tokio::test]
async fn invalid_placement_is_rejected_at_dispatch() {
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.bad");
        let (_, inst) = start(
            &storage,
            vec![common::mk_step_with_params(
                "bad",
                &handler,
                json!({"$runtime": {"runtime_kinds": ["mobile"], "hardware": [""]}}),
            )],
        )
        .await;
        assert!(tasks_of(&storage, inst.id).await.is_empty(), "{backend}");
        let tree = storage.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(
            common::node_state(&tree, "bad"),
            NodeState::Failed,
            "{backend}"
        );
    }
}

#[tokio::test]
async fn release_before_start_requeues_and_after_start_goes_unknown() {
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.upload");
        let (_, inst) = start(&storage, vec![mk_step_with_retry("upload", &handler, 3)]).await;
        let task = only_task(&storage, inst.id).await;
        let claimed = claim(&storage, &task).await;
        let claim_proof = orch8_types::worker::WorkerClaim::new("worker-a", claimed.claim_epoch);

        // Not started: straight back to pending, receipt untouched.
        assert!(
            orch8_engine::worker_lease::release_worker_task(
                storage.as_ref(),
                &inst,
                &claimed,
                &claim_proof,
                false
            )
            .await
            .unwrap(),
            "{backend}"
        );
        let after = storage.get_worker_task(task.id).await.unwrap().unwrap();
        assert_eq!(after.state, WorkerTaskState::Pending, "{backend}");
        assert_eq!(
            receipt_state(&storage, &task).await,
            EffectState::Dispatched
        );
        // A stale release is refused.
        assert!(
            !orch8_engine::worker_lease::release_worker_task(
                storage.as_ref(),
                &inst,
                &claimed,
                &claim_proof,
                false
            )
            .await
            .unwrap(),
            "{backend}"
        );

        // Started: side effect may have happened → unknown + next attempt.
        let reclaimed = claim(&storage, &after).await;
        let proof = orch8_types::worker::WorkerClaim::new("worker-a", reclaimed.claim_epoch);
        assert!(
            orch8_engine::worker_lease::release_worker_task(
                storage.as_ref(),
                &inst,
                &reclaimed,
                &proof,
                true
            )
            .await
            .unwrap(),
            "{backend}"
        );
        assert_eq!(receipt_state(&storage, &task).await, EffectState::Unknown);
        let retry = only_task(&storage, inst.id).await;
        assert_eq!(retry.attempt, task.attempt + 1, "{backend}");
    }
}

fn delegation_for(
    parent: &orch8_types::instance::TaskInstance,
    execution: &orch8_types::continuity::ContinuityExecution,
    destination: RuntimeId,
    sub_sequence: orch8_types::ids::SequenceId,
    ttl: chrono::Duration,
) -> orch8_types::continuity_advanced::DeviceDelegation {
    orch8_types::continuity_advanced::DeviceDelegation {
        id: orch8_types::continuity_advanced::DelegationId::new(),
        tenant_id: parent.tenant_id.clone(),
        parent_continuity_id: execution.continuity_id,
        parent_epoch: execution.epoch,
        source_runtime_id: RuntimeId::new(),
        destination_runtime_id: destination,
        sub_sequence_id: sub_sequence,
        grant_id: orch8_types::continuity::ContinuationGrantId::new(),
        expires_at: Utc::now() + ttl,
    }
}

#[tokio::test]
async fn delegation_is_a_mailbox_task_whose_result_resumes_the_parent() {
    use orch8_engine::delegation::{
        DELEGATION_HANDLER, enqueue_delegation_task, integrate_delegation_outcome,
    };
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.parent");
        let (seq, parent) = start(&storage, vec![mk_step("wait", &handler)]).await;
        let execution = storage
            .get_continuity_execution_by_instance(&parent.tenant_id, parent.id)
            .await
            .unwrap()
            .unwrap();
        let destination = caps_of(RuntimeKind::Desktop, DELEGATION_HANDLER);
        let delegation = delegation_for(
            &parent,
            &execution,
            destination.runtime_id,
            seq.id,
            chrono::Duration::minutes(5),
        );
        let task = enqueue_delegation_task(
            storage.as_ref(),
            &parent,
            &delegation,
            &seq,
            json!({"photo": "p1"}),
        )
        .await
        .unwrap();
        assert_eq!(
            task.requirements.runtime_id,
            Some(destination.runtime_id),
            "{backend}"
        );
        assert!(task.effect_id.is_some(), "{backend}");
        assert_eq!(
            task.context,
            json!({}),
            "{backend}: no shared mutable state"
        );

        // Only the destination runtime can take it from the mailbox.
        let phone = caps_of(RuntimeKind::Mobile, DELEGATION_HANDLER);
        assert!(
            storage
                .claim_worker_tasks_matching(
                    DELEGATION_HANDLER,
                    &phone.runtime_id.to_string(),
                    None,
                    None,
                    &phone,
                    10
                )
                .await
                .unwrap()
                .iter()
                .all(|t| t.id != task.id),
            "{backend}"
        );
        let claimed = storage
            .claim_worker_tasks_matching(
                DELEGATION_HANDLER,
                &destination.runtime_id.to_string(),
                None,
                None,
                &destination,
                100,
            )
            .await
            .unwrap()
            .into_iter()
            .find(|t| t.id == task.id)
            .expect("destination claims its mailbox task");

        let output = json!({"labels": ["cat"]});
        commit_external_worker_effect(storage.as_ref(), &parent.tenant_id, &claimed, &output)
            .await
            .unwrap();
        assert!(
            storage
                .complete_worker_task(
                    claimed.id,
                    &orch8_types::worker::WorkerClaim::new(
                        destination.runtime_id.to_string(),
                        claimed.claim_epoch,
                    ),
                    &output,
                )
                .await
                .unwrap()
        );
        integrate_delegation_outcome(storage.as_ref(), &claimed, Ok(&output))
            .await
            .unwrap();

        assert_eq!(
            receipt_state(&storage, &claimed).await,
            EffectState::Committed
        );
        let after = storage.get_instance(parent.id).await.unwrap().unwrap();
        assert_eq!(
            after.state,
            InstanceState::Scheduled,
            "{backend}: parent woken"
        );
        let result = &after.context.data["delegations"][delegation.id.to_string()];
        assert_eq!(result["status"], "completed", "{backend}");
        assert_eq!(result["output"], output, "{backend}");
    }
}

#[tokio::test]
async fn expired_delegation_integrates_a_failure_without_failing_the_parent() {
    use orch8_engine::delegation::enqueue_delegation_task;
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.parent");
        let (seq, parent) = start(&storage, vec![mk_step("wait", &handler)]).await;
        let execution = storage
            .get_continuity_execution_by_instance(&parent.tenant_id, parent.id)
            .await
            .unwrap()
            .unwrap();
        let delegation = delegation_for(
            &parent,
            &execution,
            RuntimeId::new(),
            seq.id,
            chrono::Duration::milliseconds(500),
        );
        let task =
            enqueue_delegation_task(storage.as_ref(), &parent, &delegation, &seq, json!(null))
                .await
                .unwrap();
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        reap_worker_tasks(storage.as_ref(), Duration::from_secs(3600))
            .await
            .unwrap();

        assert_eq!(
            receipt_state(&storage, &task).await,
            EffectState::Abandoned,
            "{backend}: never claimed"
        );
        let after = storage.get_instance(parent.id).await.unwrap().unwrap();
        assert_ne!(after.state, InstanceState::Failed, "{backend}");
        assert_eq!(
            after.context.data["delegations"][delegation.id.to_string()]["status"],
            "failed",
            "{backend}"
        );
    }
}

#[tokio::test]
async fn checkpointed_activity_resumes_on_lease_expiry_with_unknown_receipt() {
    for (backend, storage) in backends().await {
        let handler = unique_handler("ext.batch");
        let (_, inst) = start(&storage, vec![mk_step("batch", &handler)]).await;
        let task = only_task(&storage, inst.id).await;
        let claimed = claim(&storage, &task).await;
        let proof = orch8_types::worker::WorkerClaim::new("worker-a", claimed.claim_epoch);
        assert_eq!(
            storage
                .checkpoint_worker_task(task.id, &proof, 0, &json!({"cursor": 7}))
                .await
                .unwrap(),
            Some(1)
        );
        reap_worker_tasks(storage.as_ref(), Duration::ZERO)
            .await
            .unwrap();
        let after = storage.get_worker_task(task.id).await.unwrap().unwrap();
        assert_eq!(after.state, WorkerTaskState::Pending, "{backend}: resumes");
        assert_eq!(
            after.resume_checkpoint,
            Some(json!({"cursor": 7})),
            "{backend}"
        );
        assert_eq!(receipt_state(&storage, &task).await, EffectState::Unknown);

        // The replacement attempt reports success: unknown -> committed.
        let resumed = claim(&storage, &after).await;
        commit_external_worker_effect(
            storage.as_ref(),
            &inst.tenant_id,
            &resumed,
            &json!({"ok": 1}),
        )
        .await
        .unwrap();
        assert_eq!(receipt_state(&storage, &task).await, EffectState::Committed);
    }
}
