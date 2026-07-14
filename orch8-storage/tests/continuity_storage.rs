use chrono::{Duration, Utc};
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{ContinuityStore, InstanceStore, InvariantStore, LiveMigrationTransition};
use orch8_types::checkpoint::Checkpoint;
use orch8_types::context::ExecutionContext;
use orch8_types::continuity::{
    ContinuationGrant, ContinuationGrantId, ContinuationGrantState, ContinuityExecution,
    ContinuityId, ContinuityStream, EffectDispatchOutcome, EffectId, EffectKind, EffectReceipt,
    EffectState, ExecutionEpoch, ExecutionHandoff, GrantAction, HandoffId, HandoffState,
    OwnershipState, PlacementDecision, PlacementDecisionId, PlacementEvidence, PolicyOutcome,
    ProvenanceEntry, RuntimeCapabilities, RuntimeId, RuntimeKind, RuntimeTrustLevel, StreamFrame,
    StreamFrameState, StreamId,
};
use orch8_types::continuity_advanced::IncidentCaseId;
use orch8_types::continuity_advanced::{
    CheckpointBoundary, CompensationExecutionStep, CompensationPlanStep, CompensationRunId,
    CompensationRunRecord, CompensationRunState, CompensationStepState, ForkEffectMode,
    LiveMigrationPlan, LiveMigrationRecord, LiveMigrationState, MigrationDisposition,
    MigrationPlanId, ScenarioId, WhatIfRunRecord, WhatIfScenario,
};
use orch8_types::dlq::{DlqIncidentReproduction, ReproductionStatus};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::CompensationVerificationPolicy;

fn tenant(value: &str) -> TenantId {
    TenantId::new(value).unwrap()
}

#[tokio::test]
async fn compensation_runs_are_durable_and_version_cas_protected() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant_id = tenant("tenant-compensation");
    let continuity_id = ContinuityId::new();
    let instance_id = InstanceId::new();
    let now = Utc::now();
    storage
        .create_continuity_execution(&ContinuityExecution {
            continuity_id,
            tenant_id: tenant_id.clone(),
            current_instance_id: instance_id,
            owner_runtime_id: RuntimeId::new(),
            epoch: ExecutionEpoch::initial(),
            state: OwnershipState::Owned,
            updated_at: now,
        })
        .await
        .unwrap();
    let run = CompensationRunRecord {
        id: CompensationRunId::new(),
        tenant_id: tenant_id.clone(),
        continuity_id,
        source_instance_id: instance_id,
        state: CompensationRunState::Planned,
        version: 0,
        steps: vec![CompensationExecutionStep {
            plan: CompensationPlanStep {
                effect_id: EffectId::new(),
                effect_block_id: BlockId::new("charge"),
                handler: "refund".into(),
                params: serde_json::json!({"secret": "sensitive"}),
                idempotency_key: "compensate:charge".into(),
                verification: CompensationVerificationPolicy::ProviderReceipt,
            },
            state: CompensationStepState::Pending,
            attempt: 0,
            lease_owner: None,
            lease_expires_at: None,
            provider_receipt_id: None,
            error: None,
            updated_at: now,
        }],
        hazards: Vec::new(),
        residual_effects: Vec::new(),
        created_at: now,
        updated_at: now,
        completed_at: None,
    };
    assert!(storage.create_compensation_run(&run).await.unwrap());
    assert_eq!(
        storage
            .get_compensation_run(&tenant_id, run.id)
            .await
            .unwrap()
            .unwrap()
            .steps[0]
            .plan
            .params,
        serde_json::json!({"secret": "sensitive"})
    );
    let mut duplicate_active = run.clone();
    duplicate_active.id = CompensationRunId::new();
    assert!(
        !storage
            .create_compensation_run(&duplicate_active)
            .await
            .unwrap(),
        "only one active compensation executor may own an execution"
    );
    let mut claimed = run.clone();
    claimed.version = 1;
    claimed.state = CompensationRunState::Running;
    claimed.steps[0].state = CompensationStepState::Claimed;
    claimed.steps[0].lease_owner = Some("worker-a".into());
    assert!(
        storage
            .cas_compensation_run(&tenant_id, 0, &claimed)
            .await
            .unwrap()
    );
    assert!(
        !storage
            .cas_compensation_run(&tenant_id, 0, &claimed)
            .await
            .unwrap(),
        "stale compensation executors must not overwrite a newer claim"
    );
}

#[tokio::test]
async fn stale_runtime_heartbeat_cannot_replace_newer_capabilities() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant = tenant("tenant-a");
    let runtime_id = RuntimeId::new();
    let now = Utc::now();
    let current = RuntimeCapabilities {
        runtime_id,
        kind: RuntimeKind::Edge,
        trust: RuntimeTrustLevel::Registered,
        handlers: vec!["current".into()],
        plugins: Vec::new(),
        credentials: Vec::new(),
        regions: vec!["br-south".into()],
        hardware: Vec::new(),
        offline_capable: true,
        connectivity: None,
        battery_percent: None,
        estimated_cost_microunits: None,
        estimated_latency_ms: None,
        draining: false,
        capsule_signing_public_key: None,
        observed_at: now,
        expires_at: now + Duration::minutes(2),
    };
    storage
        .upsert_runtime_capabilities(&tenant, &current)
        .await
        .unwrap();

    let mut stale = current.clone();
    stale.handlers = vec!["stale".into()];
    stale.observed_at = now - Duration::seconds(1);
    stale.expires_at = now + Duration::minutes(4);
    storage
        .upsert_runtime_capabilities(&tenant, &stale)
        .await
        .unwrap();

    assert_eq!(
        storage
            .list_runtime_capabilities(&tenant, now, 10)
            .await
            .unwrap(),
        [current]
    );
}

#[tokio::test]
async fn ownership_cas_allows_only_one_epoch_winner() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant = tenant("tenant-a");
    let continuity_id = ContinuityId::new();
    let source = RuntimeId::new();
    let now = Utc::now();
    let execution = ContinuityExecution {
        continuity_id,
        tenant_id: tenant.clone(),
        current_instance_id: InstanceId::new(),
        owner_runtime_id: source,
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: now,
    };
    storage
        .create_continuity_execution(&execution)
        .await
        .unwrap();
    assert_eq!(
        storage
            .get_continuity_execution_by_instance(&tenant, execution.current_instance_id)
            .await
            .unwrap(),
        Some(execution.clone())
    );
    let initial_locations = storage
        .list_continuity_locations(&tenant, continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(initial_locations.len(), 1);
    assert_eq!(initial_locations[0].epoch, ExecutionEpoch::initial());
    assert_eq!(initial_locations[0].runtime_id, source);

    let destination = RuntimeId::new();
    let mut next = execution.clone();
    next.owner_runtime_id = destination;
    next.epoch = execution.epoch.checked_next().unwrap();
    next.updated_at = now + Duration::seconds(1);

    assert!(
        storage
            .cas_continuity_owner(&tenant, continuity_id, execution.epoch, source, &next,)
            .await
            .unwrap()
    );
    assert!(
        !storage
            .cas_continuity_owner(&tenant, continuity_id, execution.epoch, source, &next,)
            .await
            .unwrap(),
        "stale owners must not win a second claim"
    );
    let locations = storage
        .list_continuity_locations(&tenant, continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(locations.len(), 2);
    assert_eq!(locations[1].epoch, next.epoch);
    assert_eq!(locations[1].runtime_id, destination);
    assert_eq!(locations[1].instance_id, next.current_instance_id);
    assert!(locations[1].handoff_id.is_none());
    assert!(
        storage
            .list_continuity_locations(&TenantId::new("tenant-b").unwrap(), continuity_id, 10,)
            .await
            .unwrap()
            .is_empty(),
        "location history must be tenant scoped"
    );
    assert!(
        storage
            .get_continuity_execution(&TenantId::new("tenant-b").unwrap(), continuity_id)
            .await
            .unwrap()
            .is_none(),
        "continuity reads must be tenant scoped"
    );
}

#[tokio::test]
async fn effect_state_cas_and_provenance_are_durable() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant = tenant("tenant-a");
    let continuity_id = ContinuityId::new();
    let runtime = RuntimeId::new();
    let instance = InstanceId::new();
    let now = Utc::now();
    storage
        .create_continuity_execution(&ContinuityExecution {
            continuity_id,
            tenant_id: tenant.clone(),
            current_instance_id: instance,
            owner_runtime_id: runtime,
            epoch: ExecutionEpoch::initial(),
            state: OwnershipState::Owned,
            updated_at: now,
        })
        .await
        .unwrap();

    let receipt = EffectReceipt {
        id: EffectId::new(),
        tenant_id: tenant.clone(),
        continuity_id,
        epoch: ExecutionEpoch::initial(),
        instance_id: instance,
        block_id: BlockId::new("charge"),
        kind: EffectKind::Http,
        state: EffectState::Dispatched,
        destination_fingerprint: "payments.example".into(),
        idempotency_key: Some("order-7".into()),
        request_sha256: "a".repeat(64),
        provider_receipt_id: None,
        attempt: 1,
        created_at: now,
        updated_at: now,
    };
    storage.create_effect_receipt(&receipt).await.unwrap();
    let mut duplicate = receipt.clone();
    duplicate.destination_fingerprint = "different".into();
    assert_eq!(
        storage.ensure_effect_receipt(&duplicate).await.unwrap(),
        receipt,
        "ensure must return the first durable receipt rather than overwrite evidence"
    );
    let mut unknown = receipt.clone();
    unknown
        .transition(EffectState::Unknown, now + Duration::seconds(1))
        .unwrap();
    assert!(
        storage
            .cas_effect_receipt(&tenant, receipt.id, EffectState::Dispatched, &unknown)
            .await
            .unwrap()
    );
    assert!(
        !storage
            .cas_effect_receipt(&tenant, receipt.id, EffectState::Dispatched, &unknown)
            .await
            .unwrap()
    );

    assert_provenance_chain_is_structural_and_fork_safe(&storage, &tenant, continuity_id, now)
        .await;
}

async fn assert_provenance_chain_is_structural_and_fork_safe(
    storage: &SqliteStorage,
    tenant: &TenantId,
    continuity_id: ContinuityId,
    now: chrono::DateTime<Utc>,
) {
    let provenance = ProvenanceEntry {
        id: uuid::Uuid::now_v7(),
        continuity_id,
        tenant_id: tenant.to_owned(),
        epoch: ExecutionEpoch::initial(),
        kind: "effect_unknown".into(),
        redacted_summary: None,
        payload_sha256: "b".repeat(64),
        previous_sha256: None,
        entry_sha256: "c".repeat(64),
        signing_key_id: None,
        signature: None,
        created_at: now,
    };
    storage.append_provenance(&provenance).await.unwrap();
    let mut fork = provenance.clone();
    fork.id = uuid::Uuid::now_v7();
    fork.entry_sha256 = "d".repeat(64);
    assert!(
        storage.append_provenance(&fork).await.is_err(),
        "one predecessor must not admit two chain children"
    );
    let follower = ProvenanceEntry {
        id: uuid::Uuid::now_v7(),
        previous_sha256: Some(provenance.entry_sha256.clone()),
        entry_sha256: "e".repeat(64),
        created_at: now - Duration::days(1),
        ..provenance.clone()
    };
    storage.append_provenance(&follower).await.unwrap();
    assert_eq!(
        storage
            .get_provenance_head(tenant, continuity_id)
            .await
            .unwrap(),
        Some(follower.clone())
    );
    assert_eq!(
        storage
            .list_provenance(tenant, continuity_id, 10)
            .await
            .unwrap(),
        [provenance, follower]
    );
}

#[tokio::test]
async fn incident_reproductions_are_durable_and_tenant_scoped() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant = tenant("tenant-incident");
    let record = DlqIncidentReproduction {
        id: IncidentCaseId::new(),
        tenant_id: tenant.clone(),
        fingerprint: "a".repeat(64),
        source_instance_id: InstanceId::new(),
        stable_failure_code: "UPSTREAM_TIMEOUT".into(),
        status: ReproductionStatus::InsufficientEvidence,
        scenario: None,
        fixture: None,
        missing_evidence: vec!["continuity_checkpoint".into()],
        attempts: 0,
        suggested_remediation: vec!["verify provider health".into()],
        created_at: Utc::now(),
    };
    storage.save_incident_reproduction(&record).await.unwrap();
    let listed = storage
        .list_incident_reproductions(&tenant, &record.fingerprint, 10)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, record.id);
    assert!(
        storage
            .list_incident_reproductions(
                &TenantId::new("tenant-incident-other").unwrap(),
                &record.fingerprint,
                10,
            )
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn guarded_effect_dispatch_has_one_concurrent_winner() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant = tenant("tenant-effect-race");
    let continuity_id = ContinuityId::new();
    let instance = InstanceId::new();
    let now = Utc::now();
    storage
        .create_continuity_execution(&ContinuityExecution {
            continuity_id,
            tenant_id: tenant.clone(),
            current_instance_id: instance,
            owner_runtime_id: RuntimeId::new(),
            epoch: ExecutionEpoch::initial(),
            state: OwnershipState::Owned,
            updated_at: now,
        })
        .await
        .unwrap();

    let prepared = |block: &str, created_at| EffectReceipt {
        id: EffectId::new(),
        tenant_id: tenant.clone(),
        continuity_id,
        epoch: ExecutionEpoch::initial(),
        instance_id: instance,
        block_id: BlockId::new(block),
        kind: EffectKind::Worker,
        state: EffectState::Prepared,
        destination_fingerprint: "worker:payments".into(),
        idempotency_key: None,
        request_sha256: "a".repeat(64),
        provider_receipt_id: None,
        attempt: 0,
        created_at,
        updated_at: created_at,
    };
    let first = prepared("charge-a", now);
    let second = prepared("charge-b", now + Duration::milliseconds(1));
    storage.create_effect_receipt(&first).await.unwrap();
    storage.create_effect_receipt(&second).await.unwrap();

    let mut first_dispatch = first.clone();
    first_dispatch
        .transition(EffectState::Dispatched, now + Duration::seconds(1))
        .unwrap();
    let mut second_dispatch = second.clone();
    second_dispatch
        .transition(EffectState::Dispatched, now + Duration::seconds(1))
        .unwrap();
    let (first_outcome, second_outcome) = tokio::join!(
        storage.dispatch_effect_receipt_at_most_once(&tenant, &first_dispatch),
        storage.dispatch_effect_receipt_at_most_once(&tenant, &second_dispatch),
    );

    assert_eq!(first_outcome.unwrap(), EffectDispatchOutcome::Dispatched);
    assert_eq!(second_outcome.unwrap(), EffectDispatchOutcome::Duplicate);
    let receipts = storage
        .list_effect_receipts(&tenant, continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt.state == EffectState::Dispatched)
            .count(),
        1
    );
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt.state == EffectState::Prepared)
            .count(),
        1
    );
}

#[tokio::test]
async fn what_if_summaries_are_durable_and_tenant_scoped() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant_id = tenant("tenant-what-if");
    let continuity_id = ContinuityId::new();
    let instance_id = InstanceId::new();
    let now = Utc::now();
    storage
        .create_continuity_execution(&ContinuityExecution {
            continuity_id,
            tenant_id: tenant_id.clone(),
            current_instance_id: instance_id,
            owner_runtime_id: RuntimeId::new(),
            epoch: ExecutionEpoch::initial(),
            state: OwnershipState::Owned,
            updated_at: now,
        })
        .await
        .unwrap();
    let run = WhatIfRunRecord {
        scenario: WhatIfScenario {
            id: ScenarioId::new(),
            tenant_id: tenant_id.clone(),
            source: CheckpointBoundary {
                checkpoint_id: uuid::Uuid::now_v7(),
                instance_id,
                continuity_id,
                epoch: ExecutionEpoch::initial(),
                sequence_id: SequenceId::new(),
                sequence_version: 1,
                block_id: BlockId::new("model"),
                checkpoint_sha256: "a".repeat(64),
                provenance_head: None,
                created_at: now,
            },
            context_patch: serde_json::json!({}),
            config_patch: serde_json::json!({}),
            output_overrides: serde_json::json!({}),
            handler_mocks: serde_json::json!({}),
            block_param_overrides: serde_json::json!({}),
            signals: Vec::new(),
            target_sequence_version: None,
            effect_mode: ForkEffectMode::Blocked,
            virtual_time: true,
            retain_full_evidence: false,
        },
        summary: serde_json::json!({"cost_delta": 42}),
        created_at: now,
    };
    storage.save_what_if_run(&run).await.unwrap();

    assert_eq!(
        storage
            .list_what_if_runs(&tenant_id, continuity_id, 10)
            .await
            .unwrap(),
        [run]
    );
    assert!(
        storage
            .list_what_if_runs(&tenant("tenant-other"), continuity_id, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one transaction scenario verifies apply and rollback atomically
async fn live_migration_transition_atomically_advances_and_rolls_back() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant_id = tenant("tenant-migration");
    let source_sequence = SequenceId::new();
    let target_sequence = SequenceId::new();
    let instance_id = InstanceId::new();
    let continuity_id = ContinuityId::new();
    let runtime_id = RuntimeId::new();
    let now = Utc::now();
    let source_context = ExecutionContext {
        data: serde_json::json!({"profile": {"name": "Ada"}}),
        ..ExecutionContext::default()
    };
    storage
        .create_instance(&TaskInstance {
            id: instance_id,
            sequence_id: source_sequence,
            tenant_id: tenant_id.clone(),
            namespace: Namespace::new("default"),
            state: InstanceState::Waiting,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({}),
            context: source_context.clone(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    let execution = ContinuityExecution {
        continuity_id,
        tenant_id: tenant_id.clone(),
        current_instance_id: instance_id,
        owner_runtime_id: runtime_id,
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: now,
    };
    storage
        .create_continuity_execution(&execution)
        .await
        .unwrap();
    let source_checkpoint = Checkpoint {
        id: uuid::Uuid::now_v7(),
        instance_id,
        checkpoint_data: serde_json::json!({"context_snapshot": source_context.data}),
        created_at: now,
    };
    let record = LiveMigrationRecord {
        plan: LiveMigrationPlan {
            id: MigrationPlanId::new(),
            tenant_id: tenant_id.clone(),
            continuity_id,
            from_sequence_id: source_sequence,
            from_version: 1,
            to_sequence_id: target_sequence,
            to_version: 2,
            disposition: MigrationDisposition::Automatic,
            transforms: Vec::new(),
            finding_codes: Vec::new(),
            rollback_capsule_required: true,
            created_at: now,
        },
        expected_epoch: execution.epoch,
        source_checkpoint: source_checkpoint.clone(),
        source_context: serde_json::to_value(&source_context).unwrap(),
        source_state: InstanceState::Waiting,
        rollback_capsule: None,
        state: LiveMigrationState::Planned,
        applied_epoch: None,
        rollback_expires_at: now + Duration::hours(1),
        applied_at: None,
        rolled_back_at: None,
    };
    storage.save_live_migration(&record).await.unwrap();
    let mut applied_execution = execution.clone();
    applied_execution.epoch = execution.epoch.checked_next().unwrap();
    applied_execution.updated_at = now + Duration::seconds(1);
    let mut applied_record = record.clone();
    applied_record.state = LiveMigrationState::Applied;
    applied_record.applied_epoch = Some(applied_execution.epoch);
    applied_record.applied_at = Some(applied_execution.updated_at);
    let mut target_context = source_context.clone();
    target_context.data["migrated"] = serde_json::json!(true);
    assert!(
        storage
            .commit_live_migration_transition(LiveMigrationTransition {
                tenant_id: &tenant_id,
                expected_record: &record,
                next_record: &applied_record,
                expected_execution: &execution,
                next_execution: &applied_execution,
                expected_instance_state: InstanceState::Waiting,
                next_instance_state: InstanceState::Paused,
                next_sequence_id: target_sequence,
                next_context: &target_context,
                checkpoint: &Checkpoint {
                    id: uuid::Uuid::now_v7(),
                    instance_id,
                    checkpoint_data: serde_json::json!({"context_snapshot": target_context.data}),
                    created_at: applied_execution.updated_at,
                },
                forbid_effects_epoch: None,
            })
            .await
            .unwrap()
    );
    let applied_instance = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(applied_instance.sequence_id, target_sequence);
    assert_eq!(applied_instance.state, InstanceState::Paused);
    assert_eq!(applied_instance.context.data["migrated"], true);

    let mut rolled_execution = applied_execution.clone();
    rolled_execution.epoch = applied_execution.epoch.checked_next().unwrap();
    rolled_execution.updated_at = now + Duration::seconds(2);
    let mut rolled_record = applied_record.clone();
    rolled_record.state = LiveMigrationState::RolledBack;
    rolled_record.rolled_back_at = Some(rolled_execution.updated_at);
    let mut target_effect = EffectReceipt {
        id: EffectId::new(),
        tenant_id: tenant_id.clone(),
        continuity_id,
        epoch: applied_execution.epoch,
        instance_id,
        block_id: BlockId::new("v2-only-effect"),
        kind: EffectKind::Http,
        state: EffectState::Dispatched,
        destination_fingerprint: "provider:v2".into(),
        idempotency_key: Some("migration-effect".into()),
        request_sha256: "b".repeat(64),
        provider_receipt_id: None,
        attempt: 1,
        created_at: applied_execution.updated_at,
        updated_at: applied_execution.updated_at,
    };
    storage.create_effect_receipt(&target_effect).await.unwrap();
    assert!(
        !storage
            .commit_live_migration_transition(LiveMigrationTransition {
                tenant_id: &tenant_id,
                expected_record: &applied_record,
                next_record: &rolled_record,
                expected_execution: &applied_execution,
                next_execution: &rolled_execution,
                expected_instance_state: InstanceState::Paused,
                next_instance_state: InstanceState::Waiting,
                next_sequence_id: source_sequence,
                next_context: &source_context,
                checkpoint: &Checkpoint {
                    id: uuid::Uuid::now_v7(),
                    instance_id,
                    checkpoint_data: source_checkpoint.checkpoint_data.clone(),
                    created_at: rolled_execution.updated_at,
                },
                forbid_effects_epoch: Some(applied_execution.epoch),
            })
            .await
            .unwrap(),
        "provider-visible target effects must block rollback"
    );
    let mut unknown = target_effect.clone();
    unknown
        .transition(EffectState::Unknown, now + Duration::seconds(3))
        .unwrap();
    assert!(
        storage
            .cas_effect_receipt(
                &tenant_id,
                target_effect.id,
                EffectState::Dispatched,
                &unknown,
            )
            .await
            .unwrap()
    );
    target_effect = unknown;
    target_effect
        .transition(EffectState::Compensated, now + Duration::seconds(4))
        .unwrap();
    assert!(
        storage
            .cas_effect_receipt(
                &tenant_id,
                target_effect.id,
                EffectState::Unknown,
                &target_effect,
            )
            .await
            .unwrap()
    );
    assert!(
        storage
            .commit_live_migration_transition(LiveMigrationTransition {
                tenant_id: &tenant_id,
                expected_record: &applied_record,
                next_record: &rolled_record,
                expected_execution: &applied_execution,
                next_execution: &rolled_execution,
                expected_instance_state: InstanceState::Paused,
                next_instance_state: InstanceState::Waiting,
                next_sequence_id: source_sequence,
                next_context: &source_context,
                checkpoint: &Checkpoint {
                    id: uuid::Uuid::now_v7(),
                    instance_id,
                    checkpoint_data: source_checkpoint.checkpoint_data,
                    created_at: rolled_execution.updated_at,
                },
                forbid_effects_epoch: Some(applied_execution.epoch),
            })
            .await
            .unwrap()
    );
    let restored = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(restored.sequence_id, source_sequence);
    assert_eq!(restored.state, InstanceState::Waiting);
    assert_eq!(restored.context.data, source_context.data);
    assert_eq!(
        storage
            .get_continuity_execution(&tenant_id, continuity_id)
            .await
            .unwrap()
            .unwrap()
            .epoch,
        rolled_execution.epoch
    );
    assert_eq!(
        storage
            .get_live_migration(&tenant_id, record.plan.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        LiveMigrationState::RolledBack
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one scenario verifies the grant and stream lifecycle
async fn grants_are_single_use_and_stream_retractions_are_resumable() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant = tenant("tenant-a");
    let continuity_id = ContinuityId::new();
    let runtime = RuntimeId::new();
    let now = Utc::now();
    storage
        .create_continuity_execution(&ContinuityExecution {
            continuity_id,
            tenant_id: tenant.clone(),
            current_instance_id: InstanceId::new(),
            owner_runtime_id: runtime,
            epoch: ExecutionEpoch::initial(),
            state: OwnershipState::Owned,
            updated_at: now,
        })
        .await
        .unwrap();

    let grant = ContinuationGrant {
        id: ContinuationGrantId::new(),
        tenant_id: tenant.clone(),
        continuity_id,
        expected_epoch: ExecutionEpoch::initial(),
        destination_runtime_id: runtime,
        subject: Some("device-a".into()),
        allowed_actions: vec![GrantAction::Accept],
        nonce_sha256: "a".repeat(64),
        state: ContinuationGrantState::Active,
        issued_at: now,
        expires_at: now + Duration::minutes(5),
        consumed_at: None,
        signing_key_id: "grant-v1".into(),
    };
    storage.create_continuation_grant(&grant).await.unwrap();
    assert!(
        storage
            .consume_continuation_grant(
                &tenant,
                grant.id,
                &grant.nonce_sha256,
                now + Duration::seconds(1),
            )
            .await
            .unwrap()
    );
    assert!(
        !storage
            .consume_continuation_grant(
                &tenant,
                grant.id,
                &grant.nonce_sha256,
                now + Duration::seconds(2),
            )
            .await
            .unwrap(),
        "a continuation nonce must not be replayable"
    );
    assert_eq!(
        storage
            .get_continuation_grant(&tenant, grant.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        ContinuationGrantState::Consumed
    );

    let stream_id = StreamId::new();
    storage
        .create_continuity_stream(&ContinuityStream {
            stream_id,
            tenant_id: tenant.clone(),
            continuity_id,
            epoch: ExecutionEpoch::initial(),
            created_at: now,
            expires_at: now + Duration::minutes(5),
        })
        .await
        .unwrap();
    for sequence in 0..=1 {
        assert!(
            storage
                .append_stream_frame(&StreamFrame {
                    stream_id,
                    tenant_id: tenant.clone(),
                    continuity_id,
                    epoch: ExecutionEpoch::initial(),
                    sequence,
                    checkpoint_sha256: "b".repeat(64),
                    state: StreamFrameState::Committed,
                    payload: serde_json::json!({"token": sequence}),
                    created_at: now + Duration::milliseconds(i64::try_from(sequence).unwrap()),
                    expires_at: now + Duration::minutes(5),
                })
                .await
                .unwrap()
        );
    }
    let mut skipped = storage
        .list_stream_frames(&tenant, stream_id, Some(0), now, 10)
        .await
        .unwrap()[0]
        .clone();
    skipped.sequence = 3;
    assert!(
        !storage.append_stream_frame(&skipped).await.unwrap(),
        "non-contiguous stream frames must be rejected atomically"
    );
    assert_eq!(
        storage
            .retract_stream_frames(&tenant, stream_id, ExecutionEpoch::initial(), 0,)
            .await
            .unwrap(),
        1
    );
    let resumed = storage
        .list_stream_frames(&tenant, stream_id, Some(0), now, 10)
        .await
        .unwrap();
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].state, StreamFrameState::Retracted);

    let decision = PlacementDecision {
        id: PlacementDecisionId::new(),
        tenant_id: tenant.clone(),
        continuity_id,
        epoch: ExecutionEpoch::initial(),
        selected_runtime_id: Some(runtime),
        requirements: orch8_types::continuity::CapsuleRequirements::default(),
        policy: None,
        classification: orch8_types::continuity::DataClassification::Internal,
        policy_version: Some(1),
        candidates: vec![PlacementEvidence {
            runtime_id: runtime,
            outcome: PolicyOutcome::Allow,
            score: 10,
            finding_codes: vec!["POLICY_ALLOW".into()],
        }],
        created_at: now,
    };
    storage.save_placement_decision(&decision).await.unwrap();
    assert_eq!(
        storage
            .get_placement_decision(&tenant, decision.id)
            .await
            .unwrap(),
        Some(decision)
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one scenario verifies all atomic handoff boundaries
async fn export_and_accept_commit_ownership_atomically() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant = tenant("tenant-a");
    let continuity_id = ContinuityId::new();
    let source = RuntimeId::new();
    let destination = RuntimeId::new();
    let now = Utc::now();
    let execution = ContinuityExecution {
        continuity_id,
        tenant_id: tenant.clone(),
        current_instance_id: InstanceId::new(),
        owner_runtime_id: source,
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: now,
    };
    storage
        .create_continuity_execution(&execution)
        .await
        .unwrap();
    let quiescing = ExecutionHandoff {
        id: HandoffId::new(),
        continuity_id,
        tenant_id: tenant.clone(),
        source_runtime_id: source,
        destination_runtime_id: destination,
        expected_epoch: ExecutionEpoch::initial(),
        state: HandoffState::Quiescing,
        capsule_id: None,
        placement_decision_id: None,
        preview_sha256: "a".repeat(64),
        version: 1,
        failure_code: None,
        created_at: now,
        updated_at: now,
    };
    storage.create_handoff(&quiescing).await.unwrap();
    let mut exported = quiescing.clone();
    exported.state = HandoffState::Exported;
    exported.capsule_id = Some(orch8_types::continuity::CapsuleId::new());
    exported.version = 2;
    let mut transferring = execution.clone();
    transferring.state = OwnershipState::Transferring;

    assert!(
        storage
            .commit_handoff_export(&tenant, &quiescing, &exported, &execution, &transferring,)
            .await
            .unwrap()
    );
    assert!(
        !storage
            .commit_handoff_export(&tenant, &quiescing, &exported, &execution, &transferring,)
            .await
            .unwrap()
    );
    let mut accepted_handoff = exported.clone();
    accepted_handoff.state = HandoffState::Accepted;
    accepted_handoff.version = 3;
    let mut accepted_execution = transferring.clone();
    accepted_execution.epoch = transferring.epoch.checked_next().unwrap();
    accepted_execution.owner_runtime_id = destination;
    let destination_instance = TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: tenant.clone(),
        namespace: Namespace::new("default"),
        state: InstanceState::Paused,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: serde_json::json!({}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    };
    storage
        .create_instance(&destination_instance)
        .await
        .unwrap();
    accepted_execution.current_instance_id = destination_instance.id;
    accepted_execution.state = OwnershipState::Owned;
    assert!(
        storage
            .accept_handoff(
                &tenant,
                &exported,
                &accepted_handoff,
                &transferring,
                &accepted_execution,
            )
            .await
            .unwrap()
    );
    let current = storage
        .get_continuity_execution(&tenant, continuity_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.epoch.get(), 1);
    assert_eq!(current.owner_runtime_id, destination);

    let mut resumed = accepted_handoff.clone();
    resumed.state = HandoffState::Resumed;
    resumed.version = 4;
    resumed.updated_at = now + Duration::seconds(1);
    assert!(
        storage
            .resume_handoff(
                &tenant,
                &accepted_handoff,
                &resumed,
                destination_instance.id,
            )
            .await
            .unwrap()
    );
    assert_eq!(
        storage
            .get_instance(destination_instance.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Scheduled
    );
    assert!(
        !storage
            .resume_handoff(
                &tenant,
                &accepted_handoff,
                &resumed,
                destination_instance.id,
            )
            .await
            .unwrap(),
        "stale resume attempts must change neither handoff nor instance"
    );
}
