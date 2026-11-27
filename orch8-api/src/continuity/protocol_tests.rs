use super::validate_runtime_registration;
use crate::browser_sessions::RuntimeBinding;
use crate::device_sessions::{clamp_advertisement, destination_view};
use chrono::{Duration, Utc};
use orch8_types::continuity::{RuntimeCapabilities, RuntimeId, RuntimeKind, RuntimeTrustLevel};
use orch8_types::delegation_definition::{
    DefinitionGraphLimits,
    protocol::{DelegationProtocolCapabilities, MAX_INPUT_EXPANDED_BYTES, MAX_INPUT_PAYLOAD_BYTES},
};
use orch8_types::ids::TenantId;
use serde_json::json;

fn supported() -> DelegationProtocolCapabilities {
    DelegationProtocolCapabilities::v1(
        DefinitionGraphLimits::default(),
        MAX_INPUT_PAYLOAD_BYTES,
        MAX_INPUT_EXPANDED_BYTES,
    )
    .unwrap()
}
fn capabilities() -> RuntimeCapabilities {
    let now = Utc::now();
    serde_json::from_value(json!({"runtime_id":RuntimeId::new(),"kind":"mobile","trust":"registered","handlers":["orch8.delegation","private-charge"],"observed_at":now,"expires_at":now+Duration::minutes(4)})).unwrap()
}

#[test]
fn legacy_advertisements_do_not_acquire_protocol_support_implicitly() {
    let caps = capabilities();
    validate_runtime_registration(&caps, Utc::now()).unwrap();
    assert!(caps.delegation_protocol.is_none());
    assert!(
        serde_json::to_value(caps)
            .unwrap()
            .get("delegation_protocol")
            .is_none()
    );
}

#[test]
fn registration_rejects_invalid_protocol_versions_and_resource_limits() {
    let mut caps = capabilities();
    caps.delegation_protocol = Some(supported());
    validate_runtime_registration(&caps, Utc::now()).unwrap();
    for variant in 0..5 {
        let mut protocol = supported();
        match variant {
            0 => protocol.version = 2,
            1 => protocol.input_wire_version = 2,
            2 => protocol.max_definition_payload_bytes = u32::MAX,
            3 => protocol.max_definitions = 0,
            _ => protocol.max_input_expanded_bytes = 0,
        }
        caps.delegation_protocol = Some(protocol);
        assert!(matches!(
            validate_runtime_registration(&caps, Utc::now()),
            Err(crate::error::ApiError::InvalidArgument(_))
        ));
    }
}

#[test]
fn destination_discovery_preserves_protocol_limits_without_disclosing_inventory() {
    let mut caps = capabilities();
    caps.delegation_protocol = Some(supported());
    caps.trust = RuntimeTrustLevel::Attested;
    caps.plugins = vec!["private-plugin".into()];
    caps.credentials = vec!["private-binding".into()];
    caps.hardware = vec!["private-hardware".into()];
    caps.labels
        .insert("private-label".into(), "private-value".into());
    let visible = destination_view(vec![caps], RuntimeId::new(), Utc::now());
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].delegation_protocol, Some(supported()));
    assert_eq!(visible[0].trust, RuntimeTrustLevel::Registered);
    assert!(
        visible[0].plugins.is_empty()
            && visible[0].credentials.is_empty()
            && visible[0].hardware.is_empty()
            && visible[0].labels.is_empty()
    );
}

#[test]
fn session_protocol_support_does_not_expand_handler_or_placement_authority() {
    let mut caps = capabilities();
    caps.delegation_protocol = Some(supported());
    caps.labels.insert("residency".into(), "eu".into());
    let binding = RuntimeBinding {
        tenant_id: TenantId::unchecked("tenant"),
        kind: RuntimeKind::Mobile,
        runtime_id: caps.runtime_id,
        device_id: Some("device".into()),
        handlers: vec!["orch8.delegation".into()],
        queues: vec![],
        expires_at: Utc::now() + Duration::minutes(1),
    };
    clamp_advertisement(&binding, &mut caps).unwrap();
    assert_eq!(caps.delegation_protocol, Some(supported()));
    assert_eq!(caps.handlers, vec!["orch8.delegation"]);
    assert!(caps.labels.is_empty());
    assert_eq!(caps.expires_at, binding.expires_at);
}

#[test]
fn destination_discovery_excludes_unready_and_unknown_decoders() {
    let mut caps = capabilities();
    caps.delegation_protocol = None;
    assert!(destination_view(vec![caps.clone()], RuntimeId::new(), Utc::now()).is_empty());
    let mut unsupported = supported();
    unsupported.version = 2;
    caps.delegation_protocol = Some(unsupported);
    assert!(destination_view(vec![caps], RuntimeId::new(), Utc::now()).is_empty());
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // exercises inspection, acceptance, replay, and receipt recovery together
async fn federation_inspection_leaves_the_envelope_available_for_one_acceptance() {
    use super::{VerifyFederationRequest, hex_sha256, inspect_federation, verify_federation};
    use axum::{Json, extract::State};
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
    use orch8_engine::continuity_advanced::{FederationSigningRequest, sign_federation_envelope};
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::{
        continuity::{ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState},
        continuity_advanced::{FederationPeer, FederationPeerId},
        ids::InstanceId,
    };
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let mut state =
        crate::test_harness::test_state(storage, CancellationToken::new(), false, 0, None);
    let now = Utc::now();
    let tenant = TenantId::new("federation-fixture").unwrap();
    let execution = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: tenant.clone(),
        current_instance_id: InstanceId::new(),
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: now,
    };
    state
        .storage
        .create_continuity_execution(&execution)
        .await
        .unwrap();
    let signer = ed25519_dalek::SigningKey::from_bytes(&[21; 32]);
    let key = signer.verifying_key().to_bytes();
    let peer_id = FederationPeerId::new();
    state.federation_peers = Arc::new(vec![FederationPeer {
        id: peer_id,
        name: "Test sender".into(),
        trust_root_sha256: hex_sha256(&key),
        public_key: BASE64.encode(key),
        endpoint: "https://peer.invalid".into(),
        allowed_tenants: vec![tenant.clone()],
        revoked_at: None,
    }]);
    let payload = b"federation fixture";
    let envelope = sign_federation_envelope(
        &signer,
        FederationSigningRequest {
            peer_id,
            tenant_id: tenant.clone(),
            continuity_id: execution.continuity_id,
            epoch: execution.epoch,
            destination_runtime_id: RuntimeId::new(),
            payload,
            issued_at: now,
            expires_at: now + Duration::minutes(2),
        },
    );
    let input = |payload: &[u8]| {
        Json(VerifyFederationRequest {
            tenant_id: tenant.clone(),
            envelope: envelope.clone(),
            payload_base64: BASE64.encode(payload),
        })
    };

    for _ in 0..2 {
        let Json(observed) = inspect_federation(State(state.clone()), None, input(payload))
            .await
            .unwrap();
        assert_eq!(observed.message_id, envelope.id);
        assert_eq!(
            observed.envelope_sha256,
            hex_sha256(&serde_json::to_vec(&envelope).unwrap())
        );
    }
    assert!(
        inspect_federation(State(state.clone()), None, input(b"changed"))
            .await
            .is_err()
    );
    let provenance = || {
        state
            .storage
            .list_provenance(&tenant, execution.continuity_id, 10)
    };
    assert!(provenance().await.unwrap().is_empty());

    assert!(
        verify_federation(State(state.clone()), None, input(payload))
            .await
            .unwrap()
            .0
            .valid
    );
    assert!(
        verify_federation(State(state.clone()), None, input(payload))
            .await
            .is_err()
    );
    let chain = provenance().await.unwrap();
    assert_eq!(chain.len(), 1);
    assert_eq!(chain[0].kind, "federation_received");

    let second = sign_federation_envelope(
        &signer,
        FederationSigningRequest {
            peer_id,
            tenant_id: tenant.clone(),
            continuity_id: execution.continuity_id,
            epoch: execution.epoch,
            destination_runtime_id: RuntimeId::new(),
            payload,
            issued_at: now,
            expires_at: now + Duration::minutes(2),
        },
    );
    let second_request = || {
        Json(VerifyFederationRequest {
            tenant_id: tenant.clone(),
            envelope: second.clone(),
            payload_base64: BASE64.encode(payload),
        })
    };
    let (left, right) = tokio::join!(
        super::federation_acceptance::accept(State(state.clone()), None, second_request()),
        super::federation_acceptance::accept(State(state.clone()), None, second_request()),
    );
    let receipt = left.unwrap().0;
    assert_eq!(right.unwrap().0, receipt);
    assert!(
        verify_federation(State(state.clone()), None, second_request())
            .await
            .is_err()
    );
    assert_eq!(provenance().await.unwrap().len(), 2);

    let query = |tenant: &str, digest: &str| {
        axum::extract::Query(
            serde_json::from_value(json!({
                "tenant_id": tenant, "continuity_id": execution.continuity_id, "peer_id": peer_id,
                "envelope_sha256": digest,
            }))
            .unwrap(),
        )
    };
    let digest = &receipt.federation_acceptance.envelope_sha256;
    for (tenant, digest) in [
        ("other", digest.as_str()),
        (tenant.as_str(), &"c".repeat(64)),
    ] {
        assert!(
            super::federation_acceptance::read(
                State(state.clone()),
                None,
                axum::extract::Path(second.id),
                query(tenant, digest),
            )
            .await
            .is_err()
        );
    }
    // Replay receipts expire independently of durable provenance.
    assert!(
        state
            .storage
            .accept_federation_message(&second, digest, now + Duration::days(1),)
            .await
            .unwrap()
    );
    let observed = super::federation_acceptance::read(
        State(state.clone()),
        None,
        axum::extract::Path(second.id),
        query(tenant.as_str(), digest),
    )
    .await
    .unwrap();
    assert_eq!(observed.0, receipt);
}

#[tokio::test]
async fn federation_signing_returns_one_envelope_for_concurrent_retries() {
    use axum::{
        Json,
        extract::{Path, Query, State},
    };
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::{
        continuity::{ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState},
        continuity_advanced::FederationPeerId,
        ids::InstanceId,
    };
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let state = crate::test_harness::test_state(storage, CancellationToken::new(), false, 0, None);
    let now = Utc::now();
    let tenant = TenantId::new("signing-fixture").unwrap();
    let owner = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: tenant.clone(),
        current_instance_id: InstanceId::new(),
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: now,
    };
    state
        .storage
        .create_continuity_execution(&owner)
        .await
        .unwrap();
    let runtime = capabilities();
    state
        .storage
        .upsert_runtime_capabilities(&tenant, &runtime)
        .await
        .unwrap();
    let request = uuid::Uuid::now_v7();
    let peer = FederationPeerId::new();
    let input =
        |payload: &str| {
            Json(serde_json::from_value(json!({
        "tenant_id": tenant, "destination_runtime_id": runtime.runtime_id, "peer_id": peer,
        "expected_epoch": owner.epoch, "payload_base64": payload, "ttl_seconds": 120,
    })).unwrap())
        };
    let path = || Path((owner.continuity_id, request));
    let (first, retry) = tokio::join!(
        super::federation_signing::sign(State(state.clone()), None, path(), input("YWJj")),
        super::federation_signing::sign(State(state.clone()), None, path(), input("YWJj")),
    );
    let record = first.unwrap().0;
    assert_eq!(retry.unwrap().0, record);
    assert!(
        super::federation_signing::sign(State(state.clone()), None, path(), input("YWJjZA=="),)
            .await
            .is_err()
    );
    let saved = super::federation_signing::read(
        State(state.clone()),
        None,
        path(),
        Query(super::TenantQuery {
            tenant_id: tenant.to_string(),
        }),
    )
    .await
    .unwrap()
    .0;
    assert_eq!(record, saved);
    let chain = state
        .storage
        .list_provenance(&tenant, owner.continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(chain, vec![record.provenance]);
}
