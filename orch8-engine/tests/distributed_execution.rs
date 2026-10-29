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
use orch8_storage::testing::backends::TestBackend;
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

/// `SQLite` plus Postgres (fresh schema per call; skipped locally without
/// `DATABASE_URL`, required under CI).
async fn backends() -> Vec<TestBackend> {
    TestBackend::sqlite_and_postgres().await
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
        labels: std::collections::BTreeMap::new(),
        observed_at: now,
        expires_at: now + chrono::Duration::minutes(4),
    }
}

/// Serializes the tests that reap with a zero default lease: on the shared
/// Postgres table such a reaper expires *every* claimed task, so it could
/// resolve another scenario's task between its claim and its next step
/// (e.g. before a checkpoint lands).
static ZERO_LEASE_REAPER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    let _reaper = ZERO_LEASE_REAPER.lock().await;
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    let _reaper = ZERO_LEASE_REAPER.lock().await;
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    let _reaper = ZERO_LEASE_REAPER.lock().await;
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
        let (_, inst) = start(
            &storage,
            vec![common::mk_step_with_params(
                "pure-step",
                "noop",
                json!({"$runtime":{"runtime_kinds":["mobile"]}}),
            )],
        )
        .await;
        let task = only_task(&storage, inst.id).await;
        assert!(
            task.effect_id.is_none(),
            "pure built-in task must not have an effect receipt"
        );
        let phone = caps_of(RuntimeKind::Mobile, "noop");
        let mut delivered = storage
            .claim_worker_tasks_matching(
                "noop",
                &phone.runtime_id.to_string(),
                None,
                None,
                &phone,
                1,
            )
            .await
            .unwrap();
        let claimed = delivered.pop().expect("pure mobile task was delivered");
        assert_eq!(claimed.id, task.id);
        // Capability claims have a per-runtime lease that overrides the
        // zero default used by this reaper test.
        test_backend
            .raw
            .backdate(
                "worker_tasks",
                "heartbeat_at",
                "id",
                &claimed.id.to_string(),
                Utc::now()
                    - chrono::Duration::seconds(i64::from(claimed.lease_secs.unwrap_or(0)) + 1),
            )
            .await;
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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    let _reaper = ZERO_LEASE_REAPER.lock().await;
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    for test_backend in TestBackend::sqlite_and_postgres_with_artifacts().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    let _reaper = ZERO_LEASE_REAPER.lock().await;
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
        source_runtime_id: execution.owner_runtime_id,
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
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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

/// A parent hosted by a runtime has no instance on the server: its
/// delegation is anchored on a proxy (id = delegation id) that holds the
/// integrated outcome and turns terminal. On the hosting runtime the parked
/// step is resumed by `resume_local_parent`, exactly once.
#[tokio::test]
async fn runtime_hosted_parent_uses_a_proxy_and_resumes_the_local_step_once() {
    use orch8_engine::delegation::{
        DELEGATION_HANDLER, enqueue_delegation_task, ensure_delegation_proxy,
        integrate_delegation_outcome, is_delegation_proxy, resume_local_parent,
    };
    use orch8_types::worker::WorkerClaim;
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
        // Server side: the parent lives on a phone (never stored here).
        let (seq, _) = start(&storage, vec![mk_step("classify", &unique_handler("ext"))]).await;
        let phone_parent = mk_instance(seq.id);
        let execution = orch8_types::continuity::ContinuityExecution {
            continuity_id: orch8_types::continuity::ContinuityId::new(),
            tenant_id: phone_parent.tenant_id.clone(),
            current_instance_id: phone_parent.id,
            owner_runtime_id: RuntimeId::new(),
            epoch: orch8_types::continuity::ExecutionEpoch::initial(),
            state: OwnershipState::Owned,
            updated_at: Utc::now(),
        };
        storage
            .create_continuity_execution(&execution)
            .await
            .unwrap();
        let destination = caps_of(RuntimeKind::Desktop, DELEGATION_HANDLER);
        let delegation = delegation_for(
            &phone_parent,
            &execution,
            destination.runtime_id,
            seq.id,
            chrono::Duration::minutes(5),
        );
        let proxy = ensure_delegation_proxy(storage.as_ref(), &execution, &delegation, &seq)
            .await
            .unwrap();
        assert_eq!(proxy.id.into_uuid(), delegation.id.into_uuid(), "{backend}");
        assert!(is_delegation_proxy(&proxy), "{backend}");
        assert_eq!(proxy.state, InstanceState::Waiting, "{backend}");
        let again = ensure_delegation_proxy(storage.as_ref(), &execution, &delegation, &seq)
            .await
            .unwrap();
        assert_eq!(again.id, proxy.id, "{backend}: a retried claim reuses it");
        let task = enqueue_delegation_task(storage.as_ref(), &again, &delegation, &seq, json!({}))
            .await
            .unwrap();
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
            .expect("destination claims the proxy's mailbox task");
        let output = json!({"labels": ["cat"]});
        assert!(
            storage
                .complete_worker_task(
                    claimed.id,
                    &WorkerClaim::new(destination.runtime_id.to_string(), claimed.claim_epoch),
                    &output,
                )
                .await
                .unwrap()
        );
        integrate_delegation_outcome(storage.as_ref(), &claimed, Ok(&output))
            .await
            .unwrap();
        let after = storage.get_instance(proxy.id).await.unwrap().unwrap();
        assert_eq!(
            after.state,
            InstanceState::Completed,
            "{backend}: the proxy never runs; it turns terminal"
        );
        assert_eq!(
            receipt_state(&storage, &claimed).await,
            EffectState::Committed
        );

        // Runtime side: a parked step placed on the desktop resumes once —
        // with the output, or with a retryable failure (retry policy).
        let handler = unique_handler("ext.classify");
        let placed = json!({"$runtime": {"runtime_id": destination.runtime_id}});
        for (retryable_failure, block) in [(false, "classify"), (true, "flaky")] {
            let step = match common::mk_step_with_retry(block, &handler, 2) {
                BlockDefinition::Step(mut step) => {
                    step.params = placed.clone();
                    BlockDefinition::Step(step)
                }
                other => other,
            };
            let (_, local) = start(&storage, vec![step]).await;
            let parked = only_task(&storage, local.id).await;
            let mut caps = caps_of(RuntimeKind::Desktop, &handler);
            caps.runtime_id = destination.runtime_id;
            let held = storage
                .claim_worker_tasks_matching(&handler, "pump", None, None, &caps, 100)
                .await
                .unwrap()
                .into_iter()
                .find(|t| t.id == parked.id)
                .expect("the pump holds the parked step");
            let claim = WorkerClaim::new("pump".to_owned(), held.claim_epoch);
            let result = json!({"status": "completed", "output": output});
            let outcome = if retryable_failure {
                Err(("destination failed", true))
            } else {
                Ok(&output)
            };
            for (delivery, expected) in [(1, true), (2, false)] {
                assert_eq!(
                    resume_local_parent(storage.as_ref(), held.id, &claim, "d1", &result, outcome)
                        .await
                        .unwrap(),
                    expected,
                    "{backend}/{block}: delivery {delivery}"
                );
            }
            let resumed = storage.get_instance(local.id).await.unwrap().unwrap();
            assert_eq!(resumed.state, InstanceState::Scheduled, "{backend}/{block}");
            assert_eq!(
                resumed.context.data["delegations"]["d1"], result,
                "{backend}/{block}"
            );
            if retryable_failure {
                assert_eq!(
                    receipt_state(&storage, &held).await,
                    EffectState::Unknown,
                    "{backend}"
                );
                let retry = only_task(&storage, local.id).await;
                assert_eq!(retry.attempt, held.attempt + 1, "{backend}: retry policy");
            } else {
                assert_eq!(
                    receipt_state(&storage, &held).await,
                    EffectState::Committed,
                    "{backend}"
                );
                let outputs = storage
                    .get_all_outputs(local.id)
                    .await
                    .unwrap()
                    .into_iter()
                    .filter(|o| o.block_id.as_str() == block && o.output_ref.is_none())
                    .count();
                assert_eq!(outputs, 1, "{backend}: one output despite two deliveries");
                assert_eq!(resumed.context.data["labels"], json!(["cat"]), "{backend}");
            }
        }
    }
}

#[tokio::test]
async fn expired_delegation_integrates_a_failure_without_failing_the_parent() {
    use orch8_engine::delegation::enqueue_delegation_task;
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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

/// L3-15: a task that checkpoints, loses its worker, is requeued, checkpoints
/// again... used to cycle forever. Resumption is bounded; past the budget the
/// ambiguous-effect policy applies (no retry policy here, so the node fails).
#[tokio::test]
async fn checkpointed_activity_resumption_is_bounded() {
    let _reaper = ZERO_LEASE_REAPER.lock().await;
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
        let handler = unique_handler("ext.flapping");
        let (_, inst) = start(&storage, vec![mk_step("flapping", &handler)]).await;
        let task = only_task(&storage, inst.id).await;

        let mut resumes = 0u64;
        let final_task = loop {
            assert!(
                resumes <= orch8_engine::worker_lease::MAX_CHECKPOINT_RESUMES + 1,
                "{backend}: checkpointed task was requeued {resumes} times"
            );
            let current = storage.get_worker_task(task.id).await.unwrap().unwrap();
            if current.state != WorkerTaskState::Pending {
                break current;
            }
            let claimed = claim(&storage, &current).await;
            let proof = orch8_types::worker::WorkerClaim::new("worker-a", claimed.claim_epoch);
            assert!(
                storage
                    .checkpoint_worker_task(
                        task.id,
                        &proof,
                        claimed.checkpoint_seq,
                        &json!({"cursor": resumes})
                    )
                    .await
                    .unwrap()
                    .is_some(),
                "{backend}"
            );
            reap_worker_tasks(storage.as_ref(), Duration::ZERO)
                .await
                .unwrap();
            resumes += 1;
        };

        assert_eq!(final_task.state, WorkerTaskState::Failed, "{backend}");
        assert_eq!(
            resumes,
            orch8_engine::worker_lease::MAX_CHECKPOINT_RESUMES + 1,
            "{backend}: one initial claim plus the resume budget"
        );
        let tree = storage.get_execution_tree(inst.id).await.unwrap();
        assert!(
            tree.iter()
                .any(|n| n.block_id.as_str() == "flapping" && n.state == NodeState::Failed),
            "{backend}: the node fails once the budget is exhausted"
        );
    }
}

#[tokio::test]
async fn checkpointed_activity_resumes_on_lease_expiry_with_unknown_receipt() {
    let _reaper = ZERO_LEASE_REAPER.lock().await;
    for test_backend in backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
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
        let resumed_claim = orch8_types::worker::WorkerClaim::new("worker-a", resumed.claim_epoch);
        assert!(
            storage
                .complete_worker_task(resumed.id, &resumed_claim, &json!({"ok":1}))
                .await
                .unwrap()
        );
        let accepted = storage
            .get_worker_accepted_outcome(Some(&inst.tenant_id), resumed.id, &resumed_claim)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(accepted.output, json!({"ok":1}));
        assert!(
            storage
                .get_worker_accepted_outcome(Some(&inst.tenant_id), resumed.id, &proof)
                .await
                .unwrap()
                .is_none(),
            "reclaimed worker inherited the replacement's acknowledgement"
        );
        assert_eq!(receipt_state(&storage, &task).await, EffectState::Committed);
    }
}

#[tokio::test]
async fn direct_evaluator_persists_run_identity_before_remote_dispatch() {
    for backend in backends().await {
        let storage = Arc::clone(&backend.storage);
        let (_, instance) = start(
            &storage,
            vec![mk_step("work", &unique_handler("ext.identity"))],
        )
        .await;
        let current = storage.get_instance(instance.id).await.unwrap().unwrap();
        let task = only_task(&storage, instance.id).await;
        let binding = storage
            .get_worker_invocation(&current.tenant_id, task.id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            current.context.runtime.run_id.is_some(),
            "{}: missing durable native run",
            backend.name()
        );
        assert!(current.context.runtime.started_at.is_some());
        assert_eq!(binding.run_id, current.context.runtime.run_id);
        assert!(matches!(
            binding.target,
            orch8_storage::worker_invocation::WorkerInvocationTarget::Tree { .. }
        ));
    }
}

#[tokio::test]
async fn failure_planning_is_read_only_and_counter_exhaustion_is_terminal() {
    use orch8_engine::worker_lease::plan_failure_action;
    use orch8_types::worker::WorkerTaskResolutionAction;
    for backend in backends().await {
        let storage = Arc::clone(&backend.storage);
        let (_, instance) = start(
            &storage,
            vec![mk_step_with_retry(
                "work",
                &unique_handler("ext.plan"),
                u32::MAX,
            )],
        )
        .await;
        let current = storage.get_instance(instance.id).await.unwrap().unwrap();
        let task = only_task(&storage, instance.id).await;
        assert!(matches!(
            plan_failure_action(storage.as_ref(), &current, &task, true, "retry")
                .await
                .unwrap(),
            WorkerTaskResolutionAction::Retry {
                retry_output: Some(_),
                ..
            }
        ));
        assert!(
            storage
                .get_block_output(instance.id, &task.block_id)
                .await
                .unwrap()
                .is_none(),
            "planning wrote a retry marker before resolution"
        );
        let mut exhausted = task.clone();
        exhausted.attempt = u16::MAX;
        assert!(
            matches!(
                plan_failure_action(storage.as_ref(), &current, &exhausted, true, "exhausted")
                    .await
                    .unwrap(),
                WorkerTaskResolutionAction::FailNode { .. }
            ),
            "attempt counter saturation scheduled the same attempt again"
        );
    }
}

#[tokio::test]
async fn stranded_recovery_matches_the_original_request_with_or_without_a_hot_effect_link() {
    use orch8_engine::worker_lease::{fail_worker_task, finalize_stranded_dispatches};
    use orch8_storage::testing::backends::RawPool;
    use orch8_types::{continuity::EffectId, worker::WorkerClaim};
    for backend in backends().await {
        let storage = Arc::clone(&backend.storage);
        for scenario in 0..4 {
            let mismatch = scenario % 2 == 1;
            let (_, instance) = start(
                &storage,
                vec![mk_step_with_retry(
                    "work",
                    &unique_handler("ext.stranded"),
                    3,
                )],
            )
            .await;
            let first = only_task(&storage, instance.id).await;
            let claimed = claim(&storage, &first).await;
            let current = storage.get_instance(instance.id).await.unwrap().unwrap();
            let lease = WorkerClaim::new(claimed.worker_id.clone().unwrap(), claimed.claim_epoch);
            assert!(
                fail_worker_task(storage.as_ref(), &current, &claimed, &lease, "retry", true)
                    .await
                    .unwrap()
            );
            let task = only_task(&storage, instance.id).await;
            assert_eq!(task.params, first.params);
            let binding = storage
                .get_worker_invocation(&current.tenant_id, task.id)
                .await
                .unwrap()
                .unwrap();
            let orch8_storage::worker_invocation::WorkerInvocationTarget::Tree { node_id, .. } =
                binding.target
            else {
                panic!("tree source")
            };
            storage
                .update_node_state(node_id, NodeState::Waiting)
                .await
                .unwrap();
            storage
                .update_instance_state(instance.id, InstanceState::Waiting, None)
                .await
                .unwrap();
            let mut receipt = storage
                .get_effect_receipt(&current.tenant_id, first.effect_id.unwrap())
                .await
                .unwrap()
                .unwrap();
            receipt.id = EffectId::new();
            receipt.attempt = u32::from(task.attempt);
            receipt.state = EffectState::Dispatched;
            receipt.provider_receipt_id = None;
            if mismatch {
                receipt.request_sha256 = "a".repeat(64);
            }
            storage.create_effect_receipt(&receipt).await.unwrap();
            if scenario >= 2 {
                match &backend.raw {
                    RawPool::Sqlite(pool) => {
                        sqlx::query("UPDATE worker_tasks SET effect_id=?1 WHERE id=?2")
                            .bind(receipt.id.to_string())
                            .bind(task.id.to_string())
                            .execute(pool)
                            .await
                            .unwrap();
                    }
                    RawPool::Postgres(pool) => {
                        sqlx::query("UPDATE worker_tasks SET effect_id=$1 WHERE id=$2")
                            .bind(receipt.id.into_uuid())
                            .bind(task.id)
                            .execute(pool)
                            .await
                            .unwrap();
                    }
                }
            }
            backend
                .raw
                .backdate(
                    "worker_tasks",
                    "created_at",
                    "id",
                    &task.id.to_string(),
                    Utc::now() - chrono::Duration::minutes(5),
                )
                .await;
            let published = finalize_stranded_dispatches(storage.as_ref(), Duration::ZERO)
                .await
                .unwrap();
            assert_eq!(
                published,
                u64::from(!mismatch),
                "{} scenario {scenario}",
                backend.name()
            );
            let delivered = storage
                .claim_worker_tasks(&task.handler_name, "recovered", 1)
                .await
                .unwrap();
            assert_eq!(delivered.len(), usize::from(!mismatch));
            if let Some(delivered) = delivered.first() {
                assert_eq!(delivered.id, task.id);
                assert_eq!(delivered.effect_id, Some(receipt.id));
                assert_eq!(delivered.continuity_epoch, binding.continuity_epoch);
            }
        }
    }
}

#[tokio::test]
async fn pure_remote_dispatch_survives_later_effect_scope_enrollment() {
    use orch8_engine::{effect_guard::ensure_effect_scope, worker_lease::release_worker_task};
    use orch8_types::worker::WorkerClaim;
    for placed in [true, false] {
        for backend in backends().await {
            let storage = Arc::clone(&backend.storage);
            let mut sequence = mk_sequence(vec![common::mk_step_with_params(
                "pure",
                "noop",
                if placed {
                    json!({"$runtime":{"runtime_kinds":["mobile"]}})
                } else {
                    json!({})
                },
            )]);
            sequence.name = format!("pure-scope-{}", uuid::Uuid::now_v7());
            storage.create_sequence(&sequence).await.unwrap();
            let instance = mk_instance(sequence.id);
            storage.create_instance(&instance).await.unwrap();
            // An ordinary queue also runs a pure builtin when the coordinator
            // has no local implementation registered.
            common::drive_n(
                &storage,
                &orch8_engine::handlers::HandlerRegistry::new(),
                instance.id,
                &sequence,
                5,
            )
            .await;
            storage
                .update_instance_state(instance.id, InstanceState::Waiting, None)
                .await
                .unwrap();
            let task = only_task(&storage, instance.id).await;
            let original = storage
                .get_worker_invocation(&instance.tenant_id, task.id)
                .await
                .unwrap()
                .unwrap();
            // A sibling effect may initialize the shared namespace after a pure
            // activity is queued. The queued activity must already have its scope.
            let owner = ensure_effect_scope(storage.as_ref(), &instance.tenant_id, instance.id)
                .await
                .unwrap();
            let phone = caps_of(RuntimeKind::Mobile, "noop");
            let claimed = storage
                .claim_worker_tasks_matching(
                    "noop",
                    &phone.runtime_id.to_string(),
                    None,
                    None,
                    &phone,
                    1,
                )
                .await
                .unwrap()
                .pop()
                .unwrap();
            let current = storage.get_instance(instance.id).await.unwrap().unwrap();
            let lease = WorkerClaim::new(claimed.worker_id.clone().unwrap(), claimed.claim_epoch);
            assert!(
                release_worker_task(storage.as_ref(), &current, &claimed, &lease, false)
                    .await
                    .unwrap(),
                "{}: pure task lost authority when a sibling enrolled effect scope",
                backend.name()
            );
            assert_eq!(
                original.continuity.as_ref().unwrap().continuity_id,
                owner.continuity_id
            );
            assert_eq!(
                storage
                    .get_worker_invocation(&current.tenant_id, task.id)
                    .await
                    .unwrap()
                    .unwrap(),
                original
            );
        }
    }
}

#[tokio::test]
async fn retry_admission_uses_the_rendered_payload_for_the_actual_reserved_task() {
    use orch8_engine::worker_lease::fail_worker_task;
    use orch8_types::worker::WorkerClaim;
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    for backend in backends().await {
        let storage = Arc::clone(&backend.storage);
        let handler = unique_handler("ext.retry-payload");
        let mut block = mk_step_with_retry("work", &handler, 3);
        if let BlockDefinition::Step(step) = &mut block {
            step.params = json!({"url":"{{context.data.url}}"});
        }
        let mut sequence = mk_sequence(vec![block]);
        sequence.name = format!("retry-payload-{}", uuid::Uuid::now_v7());
        storage.create_sequence(&sequence).await.unwrap();
        let mut instance = mk_instance(sequence.id);
        instance.context.data = json!({"url":"https://example.invalid/first"});
        storage.create_instance(&instance).await.unwrap();
        dispatch(&storage, instance.id, &sequence).await;
        let first = only_task(&storage, instance.id).await;
        let held = claim(&storage, &first).await;
        let current = storage.get_instance(instance.id).await.unwrap().unwrap();
        let lease = WorkerClaim::new(held.worker_id.clone().unwrap(), held.claim_epoch);
        assert!(
            fail_worker_task(storage.as_ref(), &current, &held, &lease, "retry", true)
                .await
                .unwrap()
        );
        let reserved = only_task(&storage, instance.id).await;
        let mut context = storage
            .get_instance(instance.id)
            .await
            .unwrap()
            .unwrap()
            .context;
        context.data["url"] = json!("https://example.invalid/retry");
        storage
            .update_instance_context(instance.id, &context)
            .await
            .unwrap();
        dispatch(&storage, instance.id, &sequence).await;
        let retry = only_task(&storage, instance.id).await;
        assert_eq!(
            retry.id, reserved.id,
            "admission must retain the reserved task identity"
        );
        assert_eq!(
            retry.params,
            json!({"url":"https://example.invalid/retry"}),
            "{}: receipt admission must not leave the old placeholder payload on the actual task",
            backend.name()
        );
        let receipt = storage
            .get_effect_receipt(&instance.tenant_id, retry.effect_id.unwrap())
            .await
            .unwrap()
            .unwrap();
        let canonical = orch8_publisher::manifest::canonical_json(
            &json!({"handler":handler,"params":retry.params}),
        )
        .unwrap();
        let mut expected = String::with_capacity(64);
        for byte in Sha256::digest(canonical.as_bytes()) {
            write!(&mut expected, "{byte:02x}").expect("writing to String cannot fail");
        }
        assert_eq!(
            receipt.request_sha256, expected,
            "queued payload and receipt disagree"
        );
        assert_eq!(claim(&storage, &retry).await.params, retry.params);
    }
}

#[tokio::test]
async fn mobile_tick_recovers_cold_accepted_result_while_preserving_pause() {
    for test_backend in backends().await {
        let storage = Arc::clone(&test_backend.storage);
        let handler = unique_handler("ext.cold-result");
        let (_, instance) = start(&storage, vec![mk_step("remote", &handler)]).await;
        let task = only_task(&storage, instance.id).await;
        let held = claim(&storage, &task).await;
        let claim = orch8_types::worker::WorkerClaim::new("worker-a", held.claim_epoch);
        assert!(
            storage
                .complete_worker_task(held.id, &claim, &json!({"recovered":true}))
                .await
                .unwrap()
        );
        storage
            .update_instance_state(instance.id, InstanceState::Paused, None)
            .await
            .unwrap();
        storage.delete_worker_task(held.id).await.unwrap();
        let handlers = Arc::new(registry());
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let cache = Arc::new(orch8_engine::sequence_cache::SequenceCache::new(
            16,
            Duration::from_secs(60),
        ));
        let cancel = tokio_util::sync::CancellationToken::new();
        orch8_engine::scheduler::tick_once(
            &storage,
            &handlers,
            &semaphore,
            &common::default_config(),
            &cache,
            &cancel,
        )
        .await
        .unwrap();
        let parent = storage.get_instance(instance.id).await.unwrap().unwrap();
        assert_eq!(
            parent.state,
            InstanceState::Paused,
            "{}",
            test_backend.name()
        );
        assert_eq!(
            parent.context.data["recovered"],
            true,
            "{}",
            test_backend.name()
        );
        assert!(
            storage
                .claim_worker_outcome_recovery_page(64)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            storage
                .get_block_output(instance.id, &held.block_id)
                .await
                .unwrap()
                .unwrap()
                .output,
            json!({"recovered":true})
        );
        storage
            .update_instance_state(instance.id, InstanceState::Scheduled, Some(Utc::now()))
            .await
            .unwrap();
        orch8_engine::scheduler::tick_once(
            &storage,
            &handlers,
            &semaphore,
            &common::default_config(),
            &cache,
            &cancel,
        )
        .await
        .unwrap();
        assert_eq!(
            storage
                .get_instance(instance.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            InstanceState::Completed,
            "{}",
            test_backend.name()
        );
        assert!(
            tasks_of(&storage, instance.id).await.is_empty(),
            "resuming an integrated result dispatched provider work again"
        );
    }
}

#[tokio::test]
async fn stranded_recovery_never_replaces_a_missing_retained_receipt() {
    use orch8_engine::worker_lease::finalize_stranded_dispatches;
    use orch8_storage::testing::backends::RawPool;
    use orch8_types::{
        continuity::{ContinuityExecution, ContinuityId, EffectId, ExecutionEpoch},
        execution::{BlockType, ExecutionNode},
        ids::{BlockId, ExecutionNodeId},
    };
    for backend in backends().await {
        let storage = Arc::clone(&backend.storage);
        let handler = unique_handler("missing-original-receipt");
        let sequence = mk_sequence(vec![mk_step("work", &handler)]);
        storage.create_sequence(&sequence).await.unwrap();
        let mut instance = mk_instance(sequence.id);
        instance.state = InstanceState::Waiting;
        instance.context.runtime.run_id = Some(uuid::Uuid::now_v7().to_string());
        storage.create_instance(&instance).await.unwrap();
        let owner = ContinuityExecution {
            continuity_id: ContinuityId::new(),
            tenant_id: instance.tenant_id.clone(),
            current_instance_id: instance.id,
            owner_runtime_id: RuntimeId::new(),
            epoch: ExecutionEpoch::initial(),
            state: OwnershipState::Owned,
            updated_at: Utc::now(),
        };
        storage.create_continuity_execution(&owner).await.unwrap();
        storage
            .create_execution_node(&ExecutionNode {
                id: ExecutionNodeId::new(),
                instance_id: instance.id,
                block_id: BlockId::new("work"),
                parent_id: None,
                block_type: BlockType::Step,
                branch_index: None,
                state: NodeState::Waiting,
                started_at: Some(Utc::now()),
                completed_at: None,
            })
            .await
            .unwrap();
        let original = EffectId::new();
        let task:WorkerTask = serde_json::from_value(json!({
            "id":uuid::Uuid::now_v7(),"instance_id":instance.id,"block_id":"work","handler_name":handler,
            "attempt":0,"params":{"request":"cached receipt was lost"},"context":instance.context,
            "state":"pending","effect_id":original,"continuity_epoch":0,
            "created_at":Utc::now()-chrono::Duration::minutes(5),
        })).unwrap();
        storage.create_worker_task(&task).await.unwrap();
        let source = storage
            .get_worker_invocation(&instance.tenant_id, task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(source.effect_id, Some(original));
        match &backend.raw {
            RawPool::Sqlite(pool) => {
                sqlx::query("UPDATE worker_tasks SET effect_id=NULL,continuity_epoch=NULL,awaiting_dispatch=1 WHERE id=?1").bind(task.id.to_string()).execute(pool).await.unwrap();
            }
            RawPool::Postgres(pool) => {
                sqlx::query("UPDATE worker_tasks SET effect_id=NULL,continuity_epoch=NULL,awaiting_dispatch=TRUE WHERE id=$1").bind(task.id).execute(pool).await.unwrap();
            }
        }
        assert_eq!(
            finalize_stranded_dispatches(storage.as_ref(), Duration::ZERO)
                .await
                .unwrap(),
            0
        );
        assert!(
            storage
                .list_effect_receipts(&instance.tenant_id, owner.continuity_id, 100)
                .await
                .unwrap()
                .is_empty(),
            "{}: recovery admitted a replacement request instead of retaining missing-receipt uncertainty",
            backend.name()
        );
        assert!(
            storage
                .claim_worker_tasks(&task.handler_name, "must-not-replay", 1)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn failure_resolution_rejection_never_presettles_the_effect() {
    use orch8_storage::testing::faulty::FaultStorage;
    use orch8_types::{error::StorageError, worker::WorkerClaim};
    for backend in backends().await {
        let storage = Arc::clone(&backend.storage);
        let handler = unique_handler("atomic-failure");
        let (_, instance) = start(&storage, vec![mk_step_with_retry("work", &handler, 2)]).await;
        let task = only_task(&storage, instance.id).await;
        let held = claim(&storage, &task).await;
        let current = storage.get_instance(instance.id).await.unwrap().unwrap();
        let proof = WorkerClaim::new("worker-a", held.claim_epoch);
        let (injected, faults) = FaultStorage::wrap(Arc::clone(&storage));
        faults
            .on("resolve_worker_task")
            .times(1)
            .fail_with(StorageError::Query("injected resolution rejection".into()));
        assert!(
            orch8_engine::worker_lease::fail_worker_task(
                injected.as_ref(),
                &current,
                &held,
                &proof,
                "failure evidence",
                true
            )
            .await
            .is_err()
        );
        assert_eq!(
            receipt_state(&storage, &held).await,
            EffectState::Dispatched,
            "{}: rejected failure mutated the effect before its resolution transaction",
            backend.name()
        );
        assert_eq!(
            storage
                .get_worker_task(held.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            WorkerTaskState::Claimed
        );
        assert_eq!(only_task(&storage, instance.id).await.id, held.id);
    }
}

#[tokio::test]
async fn failure_resolution_timeout_of_a_resumed_pending_task_keeps_uncertainty() {
    let _reaper = ZERO_LEASE_REAPER.lock().await;
    for backend in backends().await {
        let storage = Arc::clone(&backend.storage);
        let handler = unique_handler("resumed-timeout");
        let (_, instance) = start(
            &storage,
            vec![common::mk_step_with_timeout("work", &handler, 60_000)],
        )
        .await;
        let task = only_task(&storage, instance.id).await;
        let held = claim(&storage, &task).await;
        let proof = orch8_types::worker::WorkerClaim::new("worker-a", held.claim_epoch);
        assert_eq!(
            storage
                .checkpoint_worker_task(held.id, &proof, 0, &json!({"cursor":1}))
                .await
                .unwrap(),
            Some(1)
        );
        reap_worker_tasks(storage.as_ref(), Duration::ZERO)
            .await
            .unwrap();
        let resumed = storage.get_worker_task(held.id).await.unwrap().unwrap();
        assert_eq!(resumed.state, WorkerTaskState::Pending);
        assert!(resumed.claim_epoch > 0);
        assert_eq!(receipt_state(&storage, &held).await, EffectState::Unknown);
        backend
            .raw
            .backdate(
                "worker_tasks",
                "created_at",
                "id",
                &held.id.to_string(),
                Utc::now() - chrono::Duration::days(1),
            )
            .await;
        let report = reap_worker_tasks(storage.as_ref(), Duration::from_secs(86_400))
            .await
            .unwrap();
        assert_eq!(report.timed_out, 1);
        assert_eq!(
            receipt_state(&storage, &held).await,
            EffectState::Unknown,
            "{}: pending state erased evidence that a previous claimant may have acted",
            backend.name()
        );
    }
}

#[tokio::test]
async fn failure_resolution_new_attempt_drops_the_previous_activity_checkpoint() {
    for backend in backends().await {
        let storage = Arc::clone(&backend.storage);
        let handler = unique_handler("checkpoint-new-attempt");
        let (_, instance) = start(&storage, vec![mk_step_with_retry("work", &handler, 3)]).await;
        let task = only_task(&storage, instance.id).await;
        let held = claim(&storage, &task).await;
        let proof = orch8_types::worker::WorkerClaim::new("worker-a", held.claim_epoch);
        storage
            .checkpoint_worker_task(held.id, &proof, 0, &json!({"old_attempt_cursor":7}))
            .await
            .unwrap();
        let held = storage.get_worker_task(held.id).await.unwrap().unwrap();
        let current = storage.get_instance(instance.id).await.unwrap().unwrap();
        let action = orch8_engine::worker_lease::plan_failure_action(
            storage.as_ref(),
            &current,
            &held,
            true,
            "retry",
        )
        .await
        .unwrap();
        let orch8_types::worker::WorkerTaskResolutionAction::Retry { retry_task, .. } = action
        else {
            panic!("retry policy")
        };
        assert!(retry_task.resume_checkpoint.is_none());
        assert_eq!(retry_task.checkpoint_seq, 0);
        assert!(
            orch8_engine::worker_lease::fail_worker_task(
                storage.as_ref(),
                &current,
                &held,
                &proof,
                "retry",
                true
            )
            .await
            .unwrap()
        );
        let next = only_task(&storage, instance.id).await;
        assert_ne!(next.id, held.id);
        assert_eq!(next.attempt, held.attempt + 1);
        assert!(next.resume_checkpoint.is_none());
        assert_eq!(next.checkpoint_seq, 0);
    }
}

async fn paused_failure_recovers_through_scheduler(retryable: bool, tree: bool, remove_hot: bool) {
    use orch8_engine::testing::TestEngine;
    use orch8_types::worker::WorkerClaim;
    for backend in TestBackend::all(&[
        orch8_storage::testing::backends::BackendKind::Sqlite,
        orch8_storage::testing::backends::BackendKind::Postgres,
        orch8_storage::testing::backends::BackendKind::EncryptedSqlite,
        orch8_storage::testing::backends::BackendKind::EncryptedPostgres,
    ])
    .await
    {
        let storage = Arc::clone(&backend.storage);
        let handler = unique_handler("paused-failure");
        let work = mk_step_with_retry("work", &handler, if retryable { 2 } else { 1 });
        let blocks = if tree {
            vec![BlockDefinition::Parallel(Box::new(
                orch8_types::sequence::ParallelDef {
                    id: orch8_types::ids::BlockId::new("parallel"),
                    branches: vec![vec![work]],
                },
            ))]
        } else {
            vec![work]
        };
        let mut sequence = mk_sequence(blocks);
        sequence.name = format!("paused-failure-{}", uuid::Uuid::now_v7());
        let engine = TestEngine::builder()
            .storage(Arc::clone(&storage))
            .build()
            .await;
        let id = engine.start(&sequence).await;
        for _ in 0..6 {
            engine.advance(Duration::from_secs(1));
            engine.tick().await;
            if !tasks_of(&storage, id).await.is_empty() {
                break;
            }
        }
        let task = only_task(&storage, id).await;
        let held = claim(&storage, &task).await;
        let proof = WorkerClaim::new("worker-a", held.claim_epoch);
        storage
            .update_instance_state(id, InstanceState::Paused, None)
            .await
            .unwrap();
        let paused = storage.get_instance(id).await.unwrap().unwrap();
        assert!(
            orch8_engine::worker_lease::fail_worker_task(
                storage.as_ref(),
                &paused,
                &held,
                &proof,
                "provider failure while paused",
                retryable
            )
            .await
            .unwrap()
        );
        if remove_hot {
            storage.delete_worker_task(held.id).await.unwrap();
        }
        engine.advance(Duration::from_secs(1));
        engine.tick().await;
        assert_eq!(
            storage.get_instance(id).await.unwrap().unwrap().state,
            InstanceState::Paused,
            "{}: reporting failure resumed the paused parent",
            backend.name()
        );
        assert_eq!(receipt_state(&storage, &held).await, EffectState::Unknown);
        assert!(
            storage
                .claim_worker_tasks(&handler, "while-paused", 1)
                .await
                .unwrap()
                .is_empty(),
            "paused recovery exposed runnable provider work"
        );
        storage
            .update_instance_state(id, InstanceState::Scheduled, Some(engine.now()))
            .await
            .unwrap();
        for _ in 0..8 {
            engine.advance(Duration::from_secs(1));
            engine.tick().await;
            let parent = storage.get_instance(id).await.unwrap().unwrap();
            if parent.state.is_terminal()
                || tasks_of(&storage, id)
                    .await
                    .iter()
                    .any(|task| task.attempt > held.attempt)
            {
                break;
            }
        }
        if retryable {
            let retry = only_task(&storage, id).await;
            assert_eq!(
                retry.attempt,
                held.attempt + 1,
                "{}: resuming did not apply the retained failure's retry policy",
                backend.name()
            );
            assert_ne!(retry.id, held.id);
            assert!(retry.effect_id.is_some());
            assert_eq!(retry.state, WorkerTaskState::Pending);
        } else {
            assert_eq!(
                storage.get_instance(id).await.unwrap().unwrap().state,
                InstanceState::Failed,
                "{}: resuming left a reported terminal failure stranded",
                backend.name()
            );
        }
        assert_eq!(
            receipt_state(&storage, &held).await,
            EffectState::Unknown,
            "recovery must not relabel an uncertain provider action"
        );
        let report = storage
            .get_worker_resolution_outcome(Some(&paused.tenant_id), held.id, Some(&proof))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.reason, json!("provider failure while paused"));
    }
}
#[tokio::test]
async fn paused_nonretryable_failure_is_applied_after_explicit_resume() {
    paused_failure_recovers_through_scheduler(false, false, false).await;
}
#[tokio::test]
async fn paused_retryable_failure_is_applied_after_explicit_resume() {
    paused_failure_recovers_through_scheduler(true, false, false).await;
}

#[tokio::test]
async fn paused_tree_nonretryable_failure_survives_hot_task_gc() {
    paused_failure_recovers_through_scheduler(false, true, true).await;
}
#[tokio::test]
async fn paused_tree_retryable_failure_survives_hot_task_gc() {
    paused_failure_recovers_through_scheduler(true, true, true).await;
}
#[tokio::test]
async fn paused_flat_nonretryable_failure_survives_hot_task_gc() {
    paused_failure_recovers_through_scheduler(false, false, true).await;
}
#[tokio::test]
async fn paused_flat_retryable_failure_survives_hot_task_gc() {
    paused_failure_recovers_through_scheduler(true, false, true).await;
}
