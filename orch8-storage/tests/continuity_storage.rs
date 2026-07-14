use chrono::{Duration, Utc};
use orch8_storage::ContinuityStore;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::continuity::{
    ContinuityExecution, ContinuityId, EffectId, EffectKind, EffectReceipt, EffectState,
    ExecutionEpoch, OwnershipState, ProvenanceEntry, RuntimeCapabilities, RuntimeId, RuntimeKind,
    RuntimeTrustLevel,
};
use orch8_types::ids::{BlockId, InstanceId, TenantId};

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
