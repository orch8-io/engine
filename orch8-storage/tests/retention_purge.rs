//! Instance-retention purge and auxiliary retention sweep (L1-01, L1-02,
//! L1-04, L1-05, L1-14, L1-16, L3-08). Runs on `SQLite` always and on Postgres
//! when `DATABASE_URL` is set.
//!
//! The Postgres database is shared with other suites, so every fixture is
//! back-dated to the year 2000 and asserted by id, never by global counts.
//!
//! ```text
//! cargo test -p orch8-storage --test retention_purge
//! ```

use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use serde_json::json;
use uuid::Uuid;

use orch8_storage::testing::backends::{RawPool, TestBackend, TestPostgres};
use orch8_storage::{AuxiliaryRetention, StorageBackend};
use orch8_types::context::ExecutionContext;
use orch8_types::continuity::{
    ContinuityExecution, ContinuityId, EffectId, EffectKind, EffectReceipt, EffectState,
    ExecutionEpoch, OwnershipState, RuntimeId,
};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{SequenceDefinition, SequenceStatus};
use orch8_types::worker::{WorkerTask, WorkerTaskState};

/// `SQLite` plus Postgres (fresh schema; skipped locally without
/// `DATABASE_URL`, required under CI).
async fn backends() -> Vec<TestBackend> {
    TestBackend::sqlite_and_postgres().await
}

fn y2000() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap()
}

fn sequence(tenant: &TenantId) -> SequenceDefinition {
    let id = SequenceId::new();
    SequenceDefinition {
        embed: None,
        sub_tenant: None,
        schema: None,
        schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
        id,
        tenant_id: tenant.clone(),
        namespace: Namespace::new("default"),
        name: format!("seq-{id}"),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        placement: None,
        created_at: Utc::now(),
    }
}

fn instance(
    tenant: &TenantId,
    seq: SequenceId,
    state: InstanceState,
    parent: Option<InstanceId>,
) -> TaskInstance {
    TaskInstance {
        id: InstanceId::new(),
        sub_tenant: None,
        sequence_id: seq,
        tenant_id: tenant.clone(),
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
        parent_instance_id: parent,
        budget: None,
        created_at: y2000(),
        updated_at: y2000(),
    }
}

fn worker_task(instance_id: InstanceId, handler: &str) -> WorkerTask {
    WorkerTask {
        id: Uuid::new_v4(),
        instance_id,
        block_id: BlockId::new("step_1"),
        handler_name: handler.into(),
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
        created_at: Utc::now(),
        effect_id: None,
        continuity_epoch: None,
        lease_secs: None,
        carries_credentials: false,
        claimed_runtime_kind: None,
    }
}

async fn create(storage: &dyn StorageBackend, raw: &RawPool, inst: &TaskInstance) {
    storage.create_instance(inst).await.unwrap();
    // Some backends stamp updated_at themselves on insert.
    raw.backdate(
        "task_instances",
        "updated_at",
        "id",
        &inst.id.to_string(),
        y2000(),
    )
    .await;
}

/// The purge removes the instance and everything keyed by it — including
/// the continuity scope (receipts), worker-task attempt evidence and event
/// waits that have no FK to `task_instances` — and skips instances whose
/// removal would break a live parent (L3-08) or orphan a child.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one fixture graph, asserted per backend
async fn purge_removes_instance_keyed_history_and_respects_family() {
    for backend in backends().await {
        let (name, storage, raw) = (backend.name(), Arc::clone(&backend.storage), &backend.raw);
        let storage = storage.as_ref();
        let tenant = TenantId::unchecked(format!("t-ret-{}", Uuid::new_v4()));
        let seq = sequence(&tenant);
        storage.create_sequence(&seq).await.unwrap();
        let handler = format!("h-{}", Uuid::new_v4());

        // A plain old terminal instance with side-effect history (created
        // live so its worker task can be claimed, then completed).
        let done = instance(&tenant, seq.id, InstanceState::Running, None);
        storage.create_instance(&done).await.unwrap();
        let continuity_id = ContinuityId::new();
        storage
            .create_continuity_execution(&ContinuityExecution {
                continuity_id,
                tenant_id: tenant.clone(),
                current_instance_id: done.id,
                owner_runtime_id: RuntimeId::new(),
                epoch: ExecutionEpoch::initial(),
                state: OwnershipState::Owned,
                updated_at: y2000(),
            })
            .await
            .unwrap();
        storage
            .create_effect_receipt(&EffectReceipt {
                id: EffectId::new(),
                tenant_id: tenant.clone(),
                continuity_id,
                epoch: ExecutionEpoch::initial(),
                instance_id: done.id,
                block_id: BlockId::new("charge"),
                kind: EffectKind::Http,
                state: EffectState::Committed,
                destination_fingerprint: "payments.example".into(),
                idempotency_key: None,
                request_sha256: "a".repeat(64),
                provider_receipt_id: None,
                attempt: 0,
                created_at: y2000(),
                updated_at: y2000(),
            })
            .await
            .unwrap();
        let task = worker_task(done.id, &handler);
        storage.create_worker_task(&task).await.unwrap();
        let claimed = storage.claim_worker_tasks(&handler, "w1", 1).await.unwrap();
        assert_eq!(claimed.len(), 1, "{name}: claim writes an attempt event");
        storage
            .upsert_event_wait(&orch8_types::event_correlation::EventWait {
                id: Uuid::now_v7(),
                tenant_id: tenant.to_string(),
                instance_id: done.id.into_uuid(),
                block_id: "wait".into(),
                event_names: vec!["paid".into()],
                correlation_key: "order-1".into(),
                join_mode: orch8_types::event_correlation::JoinMode::Any,
                status: orch8_types::event_correlation::WaitStatus::Satisfied,
                matched_names: vec![],
                matched_event_ids: vec![],
                created_at: y2000(),
            })
            .await
            .unwrap();
        storage
            .update_instance_state(done.id, InstanceState::Completed, None)
            .await
            .unwrap();
        raw.backdate(
            "task_instances",
            "updated_at",
            "id",
            &done.id.to_string(),
            y2000(),
        )
        .await;

        // L3-08: a terminal child of a live (paused) parent must survive.
        let live_parent = instance(&tenant, seq.id, InstanceState::Paused, None);
        create(storage, raw, &live_parent).await;
        let kept_child = instance(
            &tenant,
            seq.id,
            InstanceState::Completed,
            Some(live_parent.id),
        );
        create(storage, raw, &kept_child).await;

        // A terminal parent whose child is still running cannot go either.
        let done_parent = instance(&tenant, seq.id, InstanceState::Completed, None);
        create(storage, raw, &done_parent).await;
        let running_child = instance(
            &tenant,
            seq.id,
            InstanceState::Running,
            Some(done_parent.id),
        );
        create(storage, raw, &running_child).await;

        // A terminal parent + terminal child go together.
        let family_parent = instance(&tenant, seq.id, InstanceState::Failed, None);
        create(storage, raw, &family_parent).await;
        let family_child = instance(
            &tenant,
            seq.id,
            InstanceState::Cancelled,
            Some(family_parent.id),
        );
        create(storage, raw, &family_child).await;

        let cutoff = Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap();
        let candidates = storage
            .list_instance_gc_candidates(cutoff, 10_000)
            .await
            .unwrap();
        assert!(
            !candidates.contains(&kept_child.id),
            "{name}: child of a live parent is not a candidate"
        );
        assert!(candidates.contains(&done.id), "{name}");

        let purged = storage
            .purge_terminal_instances(&[
                done.id,
                kept_child.id,
                done_parent.id,
                family_parent.id,
                family_child.id,
            ])
            .await
            .unwrap();
        assert_eq!(purged, 3, "{name}: done + family parent/child");

        for (id, gone) in [
            (done.id, true),
            (family_parent.id, true),
            (family_child.id, true),
            (kept_child.id, false),
            (done_parent.id, false),
            (running_child.id, false),
        ] {
            assert_eq!(
                storage.get_instance(id).await.unwrap().is_none(),
                gone,
                "{name}: instance {id} gone={gone}"
            );
        }
        let id = done.id.to_string();
        assert_eq!(
            raw.count("effect_receipts", "instance_id", &id).await,
            0,
            "{name}"
        );
        assert_eq!(
            raw.count(
                "continuity_executions",
                "continuity_id",
                &continuity_id.to_string()
            )
            .await,
            0,
            "{name}"
        );
        assert_eq!(
            raw.count(
                "worker_task_attempt_events",
                "task_id",
                &task.id.to_string()
            )
            .await,
            0,
            "{name}"
        );
        assert_eq!(
            raw.count("event_waits", "instance_id", &id).await,
            0,
            "{name}"
        );
    }
}

/// D-01: a terminal parent that still has any child row is not a candidate
/// (the purge could never delete it unless the whole family is in the batch),
/// so blocked parents cannot fill the oldest-first window; once the child is
/// gone the parent becomes a candidate.
#[tokio::test]
async fn gc_candidates_exclude_parents_with_children() {
    for backend in backends().await {
        let (name, storage, raw) = (backend.name(), Arc::clone(&backend.storage), &backend.raw);
        let storage = storage.as_ref();
        let tenant = TenantId::unchecked(format!("t-ret-{}", Uuid::new_v4()));
        let seq = sequence(&tenant);
        storage.create_sequence(&seq).await.unwrap();

        let blocked = instance(&tenant, seq.id, InstanceState::Completed, None);
        create(storage, raw, &blocked).await;
        let live_child = instance(&tenant, seq.id, InstanceState::Running, Some(blocked.id));
        create(storage, raw, &live_child).await;
        let free = instance(&tenant, seq.id, InstanceState::Completed, None);
        create(storage, raw, &free).await;

        let cutoff = Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap();
        let candidates = storage
            .list_instance_gc_candidates(cutoff, 10_000)
            .await
            .unwrap();
        assert!(candidates.contains(&free.id), "{name}");
        assert!(
            !candidates.contains(&blocked.id),
            "{name}: parent with a child must not be a candidate"
        );
    }
}

/// The auxiliary sweep deletes old rows of the append-only tables it covers
/// and leaves fresh ones alone.
#[tokio::test]
async fn auxiliary_retention_deletes_only_expired_rows() {
    for backend in backends().await {
        let (name, storage, raw) = (backend.name(), Arc::clone(&backend.storage), &backend.raw);
        let storage = storage.as_ref();
        // Stopped node that went silent long ago vs. one still heartbeating.
        let mut node_ids = Vec::new();
        for _ in 0..2 {
            let id = Uuid::now_v7();
            storage
                .register_node(&orch8_types::cluster::ClusterNode {
                    id,
                    name: format!("n-{id}"),
                    status: orch8_types::cluster::NodeStatus::Stopped,
                    registered_at: Utc::now(),
                    last_heartbeat_at: Utc::now(),
                    drain: false,
                    drain_started_at: None,
                    stopped_at: Some(Utc::now()),
                    capabilities_withdrawn: true,
                    execution_handoff_evidence: None,
                })
                .await
                .unwrap();
            node_ids.push(id);
        }
        raw.backdate(
            "cluster_nodes",
            "last_heartbeat_at",
            "id",
            &node_ids[0].to_string(),
            y2000(),
        )
        .await;

        // Stale vs. fresh worker registration.
        let workers = [
            format!("w-old-{}", Uuid::new_v4()),
            format!("w-new-{}", Uuid::new_v4()),
        ];
        for worker in &workers {
            storage
                .upsert_worker_registration(&orch8_types::worker::WorkerRegistration {
                    worker_id: worker.clone(),
                    handler_name: "h".into(),
                    queue_name: None,
                    version: None,
                    tenant_id: None,
                    last_seen_at: Utc::now(),
                })
                .await
                .unwrap();
        }
        raw.backdate(
            "worker_registrations",
            "last_seen_at",
            "worker_id",
            &workers[0],
            y2000(),
        )
        .await;

        let cutoff = Some(Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap());
        let retention = AuxiliaryRetention {
            stopped_nodes_before: cutoff,
            worker_registrations_before: cutoff,
            worker_attempt_events_before: cutoff,
            settled_events_before: cutoff,
            sub_tenant_executions_before: cutoff,
            mobile_errors_before: cutoff,
            mobile_terminal_status_before: cutoff,
            mobile_settled_approvals_before: cutoff,
        };
        storage
            .delete_expired_auxiliary(&retention, 1000)
            .await
            .unwrap();

        assert_eq!(
            raw.count("cluster_nodes", "id", &node_ids[0].to_string())
                .await,
            0,
            "{name}: silent stopped node pruned"
        );
        assert_eq!(
            raw.count("cluster_nodes", "id", &node_ids[1].to_string())
                .await,
            1,
            "{name}: recently-heartbeating node kept"
        );
        assert_eq!(
            raw.count("worker_registrations", "worker_id", &workers[0])
                .await,
            0,
            "{name}"
        );
        assert_eq!(
            raw.count("worker_registrations", "worker_id", &workers[1])
                .await,
            1,
            "{name}"
        );
    }
}

/// The purge deletes a continuity's rows table by table before the
/// `continuity_executions` row; a new table with an FK to it that the purge
/// does not know about would make every purge batch fail on the FK. Keep
/// this list in sync with `CONTINUITY_CHILD_TABLES` in both backends'
/// `instances.rs`.
#[tokio::test]
async fn purge_knows_every_continuity_child_table() {
    let Some(pg) = TestPostgres::fresh().await else {
        return;
    };
    let mut referencing: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT c.conrelid::regclass::text FROM pg_constraint c \
         WHERE c.contype = 'f' AND c.confrelid = 'continuity_executions'::regclass",
    )
    .fetch_all(pg.pool())
    .await
    .unwrap();
    referencing.sort();
    let mut known = vec![
        "attention_tasks",
        "budget_reservations",
        "compensation_runs",
        "continuation_grants",
        "continuity_locations",
        "continuity_stream_frames",
        "continuity_streams",
        "effect_receipts",
        "evaluation_scores",
        "execution_capsules",
        "execution_handoffs",
        "invariant_results",
        "live_migrations",
        "placement_decisions",
        "provenance_entries",
        "what_if_runs",
    ];
    known.sort_unstable();
    assert_eq!(referencing, known);
}
