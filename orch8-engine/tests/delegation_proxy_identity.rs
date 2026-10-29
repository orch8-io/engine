//! A delegation UUID must never alias another destination, grant, or parent.
use chrono::Utc;
use orch8_engine::delegation::ensure_delegation_proxy;
use orch8_storage::testing::fixtures::SequenceBuilder;
use orch8_types::continuity::{
    ContinuationGrantId, ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState,
    RuntimeId,
};
use orch8_types::continuity_advanced::{DelegationId, DeviceDelegation};
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;

async fn verify_proxy_identity(b: orch8_storage::testing::backends::TestBackend) {
    let storage = b.storage.as_ref();
    let sequence = SequenceBuilder::new("proxy-identity")
        .step("work", "noop")
        .build();
    storage.create_sequence(&sequence).await.unwrap();
    let execution = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: sequence.tenant_id.clone(),
        current_instance_id: InstanceId::new(),
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    let delegation = DeviceDelegation {
        id: DelegationId::new(),
        tenant_id: execution.tenant_id.clone(),
        parent_continuity_id: execution.continuity_id,
        parent_epoch: execution.epoch,
        source_runtime_id: execution.owner_runtime_id,
        destination_runtime_id: RuntimeId::new(),
        sub_sequence_id: sequence.id,
        grant_id: ContinuationGrantId::new(),
        expires_at: Utc::now() + chrono::Duration::minutes(5),
    };
    let proxy = ensure_delegation_proxy(storage, &execution, &delegation, &sequence)
        .await
        .unwrap();
    for field in [
        "destination_runtime_id",
        "source_runtime_id",
        "grant_id",
        "parent_continuity_id",
        "parent_epoch",
        "sub_sequence_id",
        "expires_at",
    ] {
        let mut changed = serde_json::to_value(&delegation).unwrap();
        changed[field] = if field == "expires_at" {
            serde_json::to_value(delegation.expires_at + chrono::Duration::seconds(1)).unwrap()
        } else if field == "parent_epoch" {
            serde_json::json!(1)
        } else {
            serde_json::to_value(uuid::Uuid::now_v7()).unwrap()
        };
        let changed = serde_json::from_value(changed).unwrap();
        assert!(
            ensure_delegation_proxy(storage, &execution, &changed, &sequence)
                .await
                .is_err(),
            "reused delegation UUID accepted a changed {field}"
        );
    }
    let mut moved = execution.clone();
    moved.current_instance_id = InstanceId::new();
    assert!(
        ensure_delegation_proxy(storage, &moved, &delegation, &sequence)
            .await
            .is_err(),
        "proxy rebound to another physical parent"
    );
    let mut renamed = sequence.clone();
    renamed.namespace = orch8_types::ids::Namespace::new("another-namespace");
    assert!(
        ensure_delegation_proxy(storage, &execution, &delegation, &renamed)
            .await
            .is_err(),
        "proxy moved namespaces"
    );
    assert_eq!(
        storage
            .get_instance(proxy.id)
            .await
            .unwrap()
            .unwrap()
            .metadata,
        proxy.metadata
    );
    // Completion changes mutable state, not publication identity. A legitimate
    // retry must still discover the original terminal proxy.
    storage
        .update_instance_state(proxy.id, InstanceState::Completed, None)
        .await
        .unwrap();
    let retry = ensure_delegation_proxy(storage, &execution, &delegation, &sequence)
        .await
        .unwrap();
    assert_eq!(retry.id, proxy.id);
    assert_eq!(retry.state, InstanceState::Completed);
}

orch8_storage::backend_test!(async fn delegation_proxy_retries_require_the_entire_publication_identity(b) {
    verify_proxy_identity(b).await;
});
