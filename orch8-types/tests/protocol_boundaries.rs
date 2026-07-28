//! Public-contract tests for protocol boundaries shared across crates and SDKs.

use std::str::FromStr;

use chrono::{Duration, Utc};
use orch8_types::checkpoint::Checkpoint;
use orch8_types::context::ExecutionContext;
use orch8_types::continuity::{
    ArtifactReference, CapsuleId, CapsuleInstanceState, CapsuleManifest, CapsulePayload,
    CapsuleSchemaVersion, CheckpointIdentity, ContinuationGrant, ContinuationGrantId,
    ContinuationGrantState, ContinuityError, ContinuityId, EffectId, EffectKind, EffectReceipt,
    EffectState, ExecutionEpoch, GrantAction, RuntimeId, SequenceIdentity, StreamCursor, StreamId,
};
use orch8_types::continuity_advanced::{FaultKind, FaultProfile, InvariantId, StateTransform};
use orch8_types::cron::OverlapPolicy;
use orch8_types::execution::{BlockType, NodeState};
use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::Priority;
use orch8_types::output::BlockOutput;
use orch8_types::queue_routing::QueueRoutingRule;
use uuid::Uuid;

fn manifest(
    now: chrono::DateTime<Utc>,
    tenant_id: TenantId,
    destination: Option<RuntimeId>,
) -> CapsuleManifest {
    CapsuleManifest {
        schema: CapsuleSchemaVersion::V1,
        capsule_id: CapsuleId::new(),
        continuity_id: ContinuityId::new(),
        source_instance_id: InstanceId::new(),
        epoch: ExecutionEpoch::initial(),
        tenant_id,
        source_runtime_id: RuntimeId::new(),
        allowed_destination_runtime_id: destination,
        sequence: SequenceIdentity {
            id: SequenceId::new(),
            version: 1,
            content_sha256: "a".repeat(64),
        },
        checkpoint: CheckpointIdentity {
            block_id: BlockId::new("checkpoint"),
            sha256: "b".repeat(64),
        },
        requirements_sha256: "c".repeat(64),
        payload_artifact: ArtifactReference {
            key: "capsules/payload".into(),
            sha256: "d".repeat(64),
            bytes: 128,
        },
        provenance_head: None,
        issued_at: now,
        expires_at: now + Duration::minutes(5),
        signing_key_id: "signing-v1".into(),
        encryption_key_id: "encryption-v1".into(),
    }
}

fn grant(
    now: chrono::DateTime<Utc>,
    tenant_id: TenantId,
    destination: RuntimeId,
) -> ContinuationGrant {
    ContinuationGrant {
        id: ContinuationGrantId::new(),
        tenant_id,
        continuity_id: ContinuityId::new(),
        expected_epoch: ExecutionEpoch::from_u64(7),
        destination_runtime_id: destination,
        subject: Some("device:trusted".into()),
        allowed_actions: vec![GrantAction::Resume],
        nonce_sha256: "a".repeat(64),
        state: ContinuationGrantState::Active,
        issued_at: now,
        expires_at: now + Duration::minutes(5),
        consumed_at: None,
        signing_key_id: "signing-v1".into(),
    }
}

fn payload(now: chrono::DateTime<Utc>) -> CapsulePayload {
    let instance_id = InstanceId::new();
    CapsulePayload {
        instance: CapsuleInstanceState {
            sequence_id: SequenceId::new(),
            namespace: Namespace::new("production"),
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({}),
            context: ExecutionContext::default(),
            budget: None,
            parent_instance_id: None,
        },
        checkpoint: Checkpoint {
            id: Uuid::now_v7(),
            instance_id,
            checkpoint_data: serde_json::json!({}),
            created_at: now,
        },
        outputs: Vec::new(),
        pending_waits: Vec::new(),
        pending_signals: Vec::new(),
        effect_ids: Vec::new(),
        artifacts: Vec::new(),
        stream_cursors: Vec::new(),
        redacted_audit_context: serde_json::json!({}),
    }
}

#[test]
fn capsule_import_rejects_every_trust_boundary_violation() {
    let now = Utc::now();
    let tenant = TenantId::new("tenant-a").unwrap();
    let destination = RuntimeId::new();
    let mut value = manifest(now, tenant.clone(), Some(destination));

    assert_eq!(
        value.validate_for_import(now, &tenant, destination, CapsuleSchemaVersion::V1),
        Ok(())
    );

    value.schema = CapsuleSchemaVersion { major: 2, minor: 0 };
    assert_eq!(
        value.validate_for_import(now, &tenant, destination, CapsuleSchemaVersion::V1),
        Err(ContinuityError::UnsupportedSchema {
            offered_major: 2,
            offered_minor: 0,
        })
    );

    value.schema = CapsuleSchemaVersion::V1;
    assert_eq!(
        value.validate_for_import(
            now,
            &TenantId::new("tenant-b").unwrap(),
            destination,
            CapsuleSchemaVersion::V1,
        ),
        Err(ContinuityError::TenantMismatch)
    );

    value.expires_at = now;
    assert_eq!(
        value.validate_for_import(now, &tenant, destination, CapsuleSchemaVersion::V1),
        Err(ContinuityError::CapsuleExpired)
    );

    value.expires_at = now + Duration::minutes(5);
    assert_eq!(
        value.validate_for_import(now, &tenant, RuntimeId::new(), CapsuleSchemaVersion::V1),
        Err(ContinuityError::WrongDestination)
    );

    value.allowed_destination_runtime_id = None;
    assert_eq!(
        value.validate_for_import(now, &tenant, RuntimeId::new(), CapsuleSchemaVersion::V1),
        Ok(())
    );
}

fn validate_claim(
    grant: &ContinuationGrant,
    now: chrono::DateTime<Utc>,
    tenant_id: &TenantId,
    continuity_id: ContinuityId,
    epoch: ExecutionEpoch,
    destination_runtime_id: RuntimeId,
    action: GrantAction,
) -> Result<(), ContinuityError> {
    grant.validate_claim(
        now,
        tenant_id,
        continuity_id,
        epoch,
        destination_runtime_id,
        action,
    )
}

#[test]
fn continuation_grant_requires_active_unexpired_authorization() {
    let now = Utc::now();
    let tenant = TenantId::new("tenant-a").unwrap();
    let destination = RuntimeId::new();
    let value = grant(now, tenant.clone(), destination);

    assert_eq!(
        validate_claim(
            &value,
            now,
            &tenant,
            value.continuity_id,
            value.expected_epoch,
            destination,
            GrantAction::Resume,
        ),
        Ok(())
    );

    let mut changed = value.clone();
    changed.state = ContinuationGrantState::Consumed;
    assert_eq!(
        validate_claim(
            &changed,
            now,
            &tenant,
            value.continuity_id,
            value.expected_epoch,
            destination,
            GrantAction::Resume
        ),
        Err(ContinuityError::GrantUnavailable)
    );

    changed = value.clone();
    changed.expires_at = now;
    assert_eq!(
        validate_claim(
            &changed,
            now,
            &tenant,
            value.continuity_id,
            value.expected_epoch,
            destination,
            GrantAction::Resume
        ),
        Err(ContinuityError::GrantExpired)
    );
}

#[test]
fn continuation_grant_rejects_every_claim_scope_violation() {
    let now = Utc::now();
    let tenant = TenantId::new("tenant-a").unwrap();
    let destination = RuntimeId::new();
    let value = grant(now, tenant.clone(), destination);

    for (tenant_id, continuity_id, epoch) in [
        (
            TenantId::new("tenant-b").unwrap(),
            value.continuity_id,
            value.expected_epoch,
        ),
        (tenant.clone(), ContinuityId::new(), value.expected_epoch),
        (
            tenant.clone(),
            value.continuity_id,
            ExecutionEpoch::from_u64(8),
        ),
    ] {
        assert_eq!(
            validate_claim(
                &value,
                now,
                &tenant_id,
                continuity_id,
                epoch,
                destination,
                GrantAction::Resume
            ),
            Err(ContinuityError::GrantScopeMismatch)
        );
    }

    assert_eq!(
        validate_claim(
            &value,
            now,
            &tenant,
            value.continuity_id,
            value.expected_epoch,
            RuntimeId::new(),
            GrantAction::Resume
        ),
        Err(ContinuityError::WrongDestination)
    );
    assert_eq!(
        validate_claim(
            &value,
            now,
            &tenant,
            value.continuity_id,
            value.expected_epoch,
            destination,
            GrantAction::Accept
        ),
        Err(ContinuityError::GrantActionDenied)
    );
}

#[test]
fn capsule_payload_enforces_each_collection_limit() {
    let now = Utc::now();
    let mut value = payload(now);
    assert_eq!(value.validate_bounds(), Ok(()));

    let oversized_output = BlockOutput {
        id: Uuid::now_v7(),
        instance_id: value.checkpoint.instance_id,
        block_id: BlockId::new("step"),
        output: serde_json::Value::Null,
        output_ref: None,
        output_size: 0,
        attempt: 1,
        created_at: now,
    };
    value.outputs = vec![oversized_output; CapsulePayload::MAX_OUTPUTS + 1];
    assert_eq!(
        value.validate_bounds(),
        Err(ContinuityError::CapsulePayloadTooLarge)
    );
    value.outputs.clear();

    value.pending_waits = vec![serde_json::Value::Null; CapsulePayload::MAX_PENDING_ITEMS + 1];
    assert_eq!(
        value.validate_bounds(),
        Err(ContinuityError::CapsulePayloadTooLarge)
    );
    value.pending_waits.clear();
    value.pending_signals = vec![serde_json::Value::Null; CapsulePayload::MAX_PENDING_ITEMS + 1];
    assert_eq!(
        value.validate_bounds(),
        Err(ContinuityError::CapsulePayloadTooLarge)
    );
    value.pending_signals.clear();
    value.effect_ids = vec![EffectId::new(); CapsulePayload::MAX_PENDING_ITEMS + 1];
    assert_eq!(
        value.validate_bounds(),
        Err(ContinuityError::CapsulePayloadTooLarge)
    );
    value.effect_ids.clear();
    value.artifacts = vec![
        ArtifactReference {
            key: "k".into(),
            sha256: "a".repeat(64),
            bytes: 1
        };
        CapsulePayload::MAX_ARTIFACTS + 1
    ];
    assert_eq!(
        value.validate_bounds(),
        Err(ContinuityError::CapsulePayloadTooLarge)
    );
    value.artifacts.clear();
    value.stream_cursors = vec![
        StreamCursor {
            stream_id: StreamId::new(),
            last_committed_sequence: 1
        };
        CapsulePayload::MAX_STREAM_CURSORS + 1
    ];
    assert_eq!(
        value.validate_bounds(),
        Err(ContinuityError::CapsulePayloadTooLarge)
    );
}

#[test]
fn effect_transition_is_atomic_on_rejection() {
    let now = Utc::now();
    let mut receipt = EffectReceipt {
        id: EffectId::new(),
        tenant_id: TenantId::new("tenant-a").unwrap(),
        continuity_id: ContinuityId::new(),
        epoch: ExecutionEpoch::initial(),
        instance_id: InstanceId::new(),
        block_id: BlockId::new("charge"),
        kind: EffectKind::Http,
        state: EffectState::Planned,
        destination_fingerprint: "payments".into(),
        idempotency_key: Some("request-1".into()),
        request_sha256: "a".repeat(64),
        provider_receipt_id: None,
        attempt: 1,
        created_at: now,
        updated_at: now,
    };

    let prepared_at = now + Duration::seconds(1);
    assert_eq!(
        receipt.transition(EffectState::Prepared, prepared_at),
        Ok(())
    );
    assert_eq!(receipt.state, EffectState::Prepared);
    assert_eq!(receipt.updated_at, prepared_at);

    assert_eq!(
        receipt.transition(EffectState::Committed, now + Duration::seconds(2)),
        Err(ContinuityError::InvalidEffectTransition {
            from: EffectState::Prepared,
            to: EffectState::Committed,
        })
    );
    assert_eq!(receipt.state, EffectState::Prepared);
    assert_eq!(receipt.updated_at, prepared_at);
}

#[test]
fn protocol_uuid_ids_preserve_exact_wire_identity() {
    let uuid = Uuid::from_u128(42);
    let continuity_id = ContinuityId::from_uuid(uuid);
    assert_eq!(continuity_id.as_uuid(), &uuid);
    assert_eq!(continuity_id.to_string(), uuid.to_string());
    assert_eq!(continuity_id.into_uuid(), uuid);
    assert_eq!(ContinuityId::default().as_uuid().get_version_num(), 7);

    let invariant_id = InvariantId::from_uuid(uuid);
    assert_eq!(invariant_id.to_string(), uuid.to_string());
    assert_eq!(invariant_id.into_uuid(), uuid);
    assert_eq!(InvariantId::default().into_uuid().get_version_num(), 7);
}

#[test]
fn advanced_protocol_defaults_remain_backward_compatible() {
    let transform: StateTransform = serde_json::from_value(serde_json::json!({
        "from_path": "/old",
        "to_path": "/new",
        "transform": "move"
    }))
    .unwrap();
    assert_eq!(transform.version, 1);

    for (profile, kind) in [
        (FaultProfile::WorkerDeath, FaultKind::WorkerDeath),
        (FaultProfile::DatabaseTimeout, FaultKind::DatabaseTimeout),
        (
            FaultProfile::DuplicateDelivery,
            FaultKind::DuplicateDelivery,
        ),
        (FaultProfile::StaleOwner, FaultKind::StaleOwner),
        (FaultProfile::OfflineDevice, FaultKind::OfflineDevice),
        (FaultProfile::CorruptCapsule, FaultKind::CorruptCapsule),
        (FaultProfile::ExpiredGrant, FaultKind::ExpiredGrant),
        (FaultProfile::ProviderOutage, FaultKind::ProviderOutage),
        (FaultProfile::DelayedApproval, FaultKind::DelayedApproval),
    ] {
        assert_eq!(profile.kind(), kind);
    }
}

#[test]
fn queue_routing_defaults_and_matching_are_fail_safe() {
    let now = Utc::now();
    let legacy = serde_json::json!({
        "id": Uuid::now_v7(),
        "tenant_id": "tenant-a",
        "handler_name": "send_email",
        "queue_override": "email-priority",
        "created_at": now,
        "updated_at": now
    });
    let mut rule: QueueRoutingRule = serde_json::from_value(legacy).unwrap();
    assert!(rule.enabled);
    assert_eq!(rule.priority, 0);
    assert!(rule.match_queue.is_none());
    assert!(rule.matches(None));
    assert!(rule.matches(Some("any-queue")));

    rule.match_queue = Some("email-default".into());
    assert!(rule.matches(Some("email-default")));
    assert!(!rule.matches(Some("other")));
    assert!(!rule.matches(None));

    rule.enabled = false;
    assert!(!rule.matches(Some("email-default")));
}

#[test]
fn cron_overlap_policy_text_contract_is_bidirectional() {
    for (wire, policy) in [
        ("allow", OverlapPolicy::Allow),
        ("skip", OverlapPolicy::Skip),
        ("buffer_one", OverlapPolicy::BufferOne),
        ("cancel_previous", OverlapPolicy::CancelPrevious),
    ] {
        assert_eq!(OverlapPolicy::from_str(wire), Ok(policy));
        assert_eq!(policy.to_string(), wire);
    }
    assert_eq!(
        OverlapPolicy::from_str("queue"),
        Err("unknown overlap policy: queue".into())
    );
}

#[test]
fn execution_enum_text_contracts_cover_all_public_variants() {
    for (wire, state) in [
        ("pending", NodeState::Pending),
        ("running", NodeState::Running),
        ("waiting", NodeState::Waiting),
        ("completed", NodeState::Completed),
        ("failed", NodeState::Failed),
        ("cancelled", NodeState::Cancelled),
        ("skipped", NodeState::Skipped),
    ] {
        assert_eq!(NodeState::from_str(wire), Ok(state));
        assert_eq!(state.to_string(), wire);
    }
    assert_eq!(
        NodeState::from_str("paused"),
        Err("unknown node state: paused".into())
    );

    for (wire, block_type) in [
        ("step", BlockType::Step),
        ("parallel", BlockType::Parallel),
        ("race", BlockType::Race),
        ("loop", BlockType::Loop),
        ("for_each", BlockType::ForEach),
        ("router", BlockType::Router),
        ("try_catch", BlockType::TryCatch),
        ("sub_sequence", BlockType::SubSequence),
        ("ab_split", BlockType::ABSplit),
        ("cancellation_scope", BlockType::CancellationScope),
        ("saga", BlockType::Saga),
    ] {
        assert_eq!(BlockType::from_str(wire), Ok(block_type));
        assert_eq!(block_type.to_string(), wire);
    }
    assert_eq!(
        BlockType::from_str("map"),
        Err("unknown block type: map".into())
    );
}

#[test]
fn core_ids_preserve_values_and_tenant_deserialization_rejects_wrong_types() {
    let uuid = Uuid::from_u128(99);
    let instance_id = InstanceId::from_uuid(uuid);
    assert_eq!(instance_id.as_uuid(), &uuid);
    assert_eq!(instance_id.into_uuid(), uuid);
    let sequence_id = SequenceId::from_uuid(uuid);
    assert_eq!(sequence_id.as_uuid(), &uuid);
    assert_eq!(sequence_id.into_uuid(), uuid);
    let node_id = ExecutionNodeId::from_uuid(uuid);
    assert_eq!(node_id.as_uuid(), &uuid);
    assert_eq!(node_id.into_uuid(), uuid);

    let error = serde_json::from_str::<TenantId>("123").unwrap_err();
    assert!(error.to_string().contains("a non-empty tenant_id string"));
}
