use chrono::{Duration, Utc};
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{ContinuityStore, InstanceStore};
use orch8_types::context::ExecutionContext;
use orch8_types::continuity::{
    ContinuationGrant, ContinuationGrantId, ContinuationGrantState, ContinuityExecution,
    ContinuityId, ContinuityStream, EffectId, EffectKind, EffectReceipt, EffectState,
    ExecutionEpoch, ExecutionHandoff, GrantAction, HandoffId, HandoffState, OwnershipState,
    PlacementDecision, PlacementDecisionId, PlacementEvidence, PolicyOutcome, ProvenanceEntry,
    RuntimeCapabilities, RuntimeId, RuntimeKind, RuntimeTrustLevel, StreamFrame, StreamFrameState,
    StreamId,
};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};

fn tenant(value: &str) -> TenantId {
    TenantId::new(value).unwrap()
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
        regions: vec!["br-south".into()],
        hardware: Vec::new(),
        offline_capable: true,
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

    let provenance = ProvenanceEntry {
        id: uuid::Uuid::now_v7(),
        continuity_id,
        tenant_id: tenant.clone(),
        epoch: ExecutionEpoch::initial(),
        kind: "effect_unknown".into(),
        payload_sha256: "b".repeat(64),
        previous_sha256: None,
        entry_sha256: "c".repeat(64),
        signing_key_id: None,
        signature: None,
        created_at: now,
    };
    storage.append_provenance(&provenance).await.unwrap();
    assert_eq!(
        storage
            .list_provenance(&tenant, continuity_id, 10)
            .await
            .unwrap(),
        [provenance]
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
