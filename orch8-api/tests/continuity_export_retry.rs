//! Real HTTP retries must preserve a sealed source cut after ambiguous I/O.
use base64::Engine as _;
use chrono::{Duration, Utc};
use orch8_api::test_harness::spawn_test_server_on;
use orch8_storage::{
    StorageBackend,
    artifacts::ObjectArtifactStore,
    sqlite::SqliteStorage,
    testing::{
        faulty::FaultStorage,
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    checkpoint::Checkpoint,
    continuity::{
        CapsuleRequirements, ContinuityExecution, ContinuityId, ExecutionEpoch, ExecutionHandoff,
        HandoffId, HandoffState, OwnershipState, PlacementDecision, PlacementDecisionId,
        RuntimeCapabilities, RuntimeId,
    },
    error::StorageError,
    instance::InstanceState,
};
use reqwest::StatusCode;
use serde_json::json;
use std::sync::Arc;

#[allow(clippy::too_many_lines)] // one black-box scenario asserts the full durable retry boundary
async fn interrupted_export_retries(method: &'static str, ambiguous: bool) {
    let storage: Arc<dyn StorageBackend> = Arc::new(
        SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
    );
    let sequence = SequenceBuilder::new("http-export-retry")
        .step("done", "noop")
        .build();
    storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Paused)
        .build();
    storage.create_instance(&instance).await.unwrap();
    storage
        .save_checkpoint(&Checkpoint {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            checkpoint_data: json!({"completed_blocks":[]}),
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    let now = Utc::now();
    let execution = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: instance.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: now,
    };
    storage
        .create_continuity_execution(&execution)
        .await
        .unwrap();
    let destination = RuntimeId::new();
    for runtime in [execution.owner_runtime_id, destination] {
        let capabilities: RuntimeCapabilities = serde_json::from_value(json!({
            "runtime_id":runtime, "kind":"server", "trust":"registered",
            "observed_at":now, "expires_at":now + Duration::hours(1)
        }))
        .unwrap();
        storage
            .upsert_runtime_capabilities(&instance.tenant_id, &capabilities)
            .await
            .unwrap();
    }
    let placement = PlacementDecision {
        id: PlacementDecisionId::new(),
        tenant_id: instance.tenant_id.clone(),
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
        selected_runtime_id: Some(destination),
        requirements: CapsuleRequirements::default(),
        policy: None,
        classification: orch8_types::continuity::DataClassification::default(),
        policy_version: None,
        candidates: vec![],
        created_at: now,
    };
    storage.save_placement_decision(&placement).await.unwrap();
    let handoff = ExecutionHandoff {
        id: HandoffId::new(),
        continuity_id: execution.continuity_id,
        tenant_id: instance.tenant_id.clone(),
        source_runtime_id: execution.owner_runtime_id,
        destination_runtime_id: destination,
        expected_epoch: execution.epoch,
        state: HandoffState::Requested,
        capsule_id: None,
        placement_decision_id: Some(placement.id),
        preview_sha256: "a".repeat(64),
        version: 0,
        failure_code: None,
        created_at: now,
        updated_at: now,
    };
    storage.create_handoff(&handoff).await.unwrap();
    let (wrapped, faults) = FaultStorage::wrap(storage.clone());
    if ambiguous {
        faults
            .on(method)
            .times(1)
            .fail_after_commit(StorageError::Connection("lost acknowledgement".into()));
    } else {
        faults.on(method).times(1).fail_transient();
    }
    let server = spawn_test_server_on(wrapped, None).await;
    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/v1/continuity/handoffs/{}/export",
        server.base_url, handoff.id
    );
    let body = json!({"tenant_id":instance.tenant_id, "expires_in_seconds":300});
    let interrupted = client.post(&url).json(&body).send().await.unwrap();
    assert!(!interrupted.status().is_success(), "fault was not observed");
    assert_eq!(faults.injected(method), 1);
    let owner = storage
        .get_continuity_execution(&instance.tenant_id, execution.continuity_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(owner.state, OwnershipState::Transferring);
    let stored = storage
        .get_handoff(&instance.tenant_id, handoff.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(
            stored.state,
            HandoffState::Quiescing | HandoffState::Exported
        ),
        "{stored:?}"
    );
    let retry = client.post(&url).json(&body).send().await.unwrap();
    let status = retry.status();
    let response: serde_json::Value = retry.json().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["handoff"]["state"], "exported");
    let replay = client.post(&url).json(&body).send().await.unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.json::<serde_json::Value>().await.unwrap(), response);
    assert_eq!(
        storage
            .get_instance(instance.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Paused
    );
    let provenance = storage
        .list_provenance(&instance.tenant_id, execution.continuity_id, 100)
        .await
        .unwrap();
    assert_eq!(
        provenance.len(),
        1,
        "one durable export boundary must survive lost commit acknowledgement"
    );
    assert_eq!(provenance[0].kind, "capsule_exported");
    let crypto = orch8_api::ContinuityCrypto::from_master_key(&"11".repeat(32)).unwrap();
    let trusted = std::collections::BTreeMap::from([(
        crypto.signing_key_id,
        base64::engine::general_purpose::STANDARD
            .encode(crypto.signing_key.verifying_key().as_bytes()),
    )]);
    assert!(provenance[0].signature.is_some());
    assert!(
        orch8_engine::continuity::verify_provenance_chain_with_keys(
            &provenance,
            Some(&provenance[0].entry_sha256),
            &trusted
        )
        .valid
    );

    assert!(
        orch8_engine::continuity::verify_provenance_chain(
            &provenance,
            Some(&provenance[0].entry_sha256)
        )
        .valid
    );
    // Retries cannot extend the expiry fixed by the first durable cut.
    let extended = client
        .post(&url)
        .json(&json!({"tenant_id":instance.tenant_id,
        "expires_in_seconds":600}))
        .send()
        .await
        .unwrap();
    assert_eq!(extended.status(), StatusCode::OK);
    assert_eq!(
        extended.json::<serde_json::Value>().await.unwrap(),
        response
    );
    // Replacing the transfer key cannot produce a different transport for this cut.
    let changed = client
        .post(&url)
        .json(&json!({"tenant_id":instance.tenant_id,
        "payload_key_base64":"IiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiI="}))
        .send()
        .await
        .unwrap();
    assert_eq!(changed.status(), StatusCode::CONFLICT);
    // A retry cannot revive a transfer whose coordinator epoch has advanced.
    let mut advanced = owner.clone();
    advanced.epoch = owner.epoch.checked_next().unwrap();
    advanced.owner_runtime_id = destination;
    advanced.state = OwnershipState::Owned;
    // A transferred run continues under a fresh physical ID. Even a newer
    // coordinator epoch cannot revive the sealed source's physical identity.
    assert!(
        !storage
            .cas_continuity_owner(
                &instance.tenant_id,
                owner.continuity_id,
                owner.epoch,
                owner.owner_runtime_id,
                &advanced
            )
            .await
            .unwrap()
    );
    let mut destination_instance = instance.clone();
    destination_instance.id = orch8_types::ids::InstanceId::new();
    storage
        .create_instance(&destination_instance)
        .await
        .unwrap();
    advanced.current_instance_id = destination_instance.id;
    assert!(
        storage
            .cas_continuity_owner(
                &instance.tenant_id,
                owner.continuity_id,
                owner.epoch,
                owner.owner_runtime_id,
                &advanced
            )
            .await
            .unwrap()
    );
    let stale = client.post(&url).json(&body).send().await.unwrap();
    assert_eq!(stale.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn upload_acknowledgement_loss_retries_the_same_capsule() {
    interrupted_export_retries("put_artifact_with_id", true).await;
}
#[tokio::test]
async fn payload_read_failure_keeps_handoff_retryable() {
    interrupted_export_retries("get_artifact", false).await;
}
#[tokio::test]
async fn handoff_commit_acknowledgement_loss_returns_the_same_response() {
    interrupted_export_retries("commit_handoff_export_with_provenance", true).await;
}
