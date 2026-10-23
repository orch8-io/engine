//! Gate registrations serialize with recovery under the original parent claim.
use chrono::Utc;
use orch8_storage::{
    StorageBackend, backend_test,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    continuity_advanced::FederationPeerId,
    error::StorageError,
    event_correlation::{EventWait, JoinMode, WaitStatus},
    federation::{FederationCall, FederationCallState},
    ids::{BlockId, TenantId},
    instance::{InstanceState, TaskInstance},
};
use serde_json::json;

async fn parent(storage: &dyn StorageBackend) -> TaskInstance {
    let sequence = SequenceBuilder::new("gate-claim")
        .step("work", "noop")
        .build();
    storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    storage.create_instance(&instance).await.unwrap();
    storage
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap()
}

fn registration(parent: &TaskInstance) -> EventWait {
    EventWait {
        id: uuid::Uuid::now_v7(),
        tenant_id: parent.tenant_id.to_string(),
        instance_id: parent.id.into_uuid(),
        block_id: "work".into(),
        event_names: vec!["ready".into()],
        correlation_key: "key".into(),
        join_mode: JoinMode::All,
        status: WaitStatus::Waiting,
        matched_names: vec![],
        matched_event_ids: vec![],
        created_at: Utc::now(),
    }
}

fn call(parent: &TaskInstance) -> FederationCall {
    let now = Utc::now();
    FederationCall {
        call_id: uuid::Uuid::now_v7(),
        tenant_id: parent.tenant_id.clone(),
        instance_id: parent.id,
        block_id: BlockId::new("work"),
        peer_id: FederationPeerId::from_uuid(uuid::Uuid::now_v7()),
        sequence: "remote".into(),
        sequence_version: None,
        input: json!({"secret":"gate-private-input"}),
        withheld_sha256: vec![],
        state: FederationCallState::Pending,
        remote_instance_id: None,
        result: None,
        error: None,
        attempts: 0,
        notified: false,
        next_poll_at: now,
        created_at: now,
        updated_at: now,
        version: 0,
    }
}

backend_test!(async fn registration_rejects_stale_claims_and_preserves_replacement_rows(b) {
    let p = parent(b.storage.as_ref()).await; let token = p.claimed_at.unwrap();
    let mut wait = registration(&p); let mut outbound = call(&p);
    assert!(b.storage.upsert_event_wait_claimed(&wait, token).await.unwrap().is_some());
    assert_eq!(b.storage.create_federation_call_claimed(&outbound, token).await.unwrap(), Some(true));
    assert_eq!(b.storage.create_federation_call_claimed(&outbound, token).await.unwrap(), Some(false));
    assert_eq!(b.storage.get_federation_call(&p.tenant_id, outbound.call_id).await.unwrap().unwrap().input, outbound.input);
    let next = Utc::now()+chrono::Duration::seconds(2);
    assert!(b.storage.conditional_update_claimed_instance_state(p.id, token, InstanceState::Running, InstanceState::Scheduled, Some(next)).await.unwrap());
    let replacement = b.storage.claim_due_instances(next, 1, 0).await.unwrap().pop().unwrap();
    let current = replacement.claimed_at.unwrap(); assert_ne!(token, current);
    let before = b.storage.get_event_wait(p.id, "work").await.unwrap().unwrap();
    wait.correlation_key = "stale".into(); outbound.call_id = uuid::Uuid::now_v7();
    assert!(b.storage.upsert_event_wait_claimed(&wait, token).await.unwrap().is_none());
    assert_eq!(b.storage.create_federation_call_claimed(&outbound, token).await.unwrap(), None);
    assert_eq!(b.storage.get_event_wait(p.id, "work").await.unwrap().unwrap(), before);
    assert!(b.storage.get_federation_call(&p.tenant_id, outbound.call_id).await.unwrap().is_none());
    assert!(b.storage.upsert_event_wait_claimed(&wait, current).await.unwrap().is_some());
    assert_eq!(b.storage.create_federation_call_claimed(&outbound, current).await.unwrap(), Some(true));
});

backend_test!(async fn registration_cannot_cross_the_parent_tenant(b) {
    let p = parent(b.storage.as_ref()).await; let token = p.claimed_at.unwrap();
    let mut wait = registration(&p); wait.tenant_id = "foreign".into();
    let mut outbound = call(&p); outbound.tenant_id = TenantId::unchecked("foreign");
    assert!(matches!(b.storage.upsert_event_wait_claimed(&wait, token).await, Err(StorageError::Constraint(_))));
    assert!(matches!(b.storage.create_federation_call_claimed(&outbound, token).await, Err(StorageError::Constraint(_))));
    assert!(b.storage.get_event_wait(p.id, "work").await.unwrap().is_none());
    assert!(b.storage.get_federation_call(&outbound.tenant_id, outbound.call_id).await.unwrap().is_none());
    // An error rolls back and releases the lock, allowing the same owner to retry.
    assert!(b.storage.upsert_event_wait_claimed(&registration(&p), token).await.unwrap().is_some());
    assert_eq!(b.storage.create_federation_call_claimed(&call(&p), token).await.unwrap(), Some(true));
});

backend_test!(async fn reset_deletes_only_requested_blocks_and_requires_the_original_claim(b) {
    let p = parent(b.storage.as_ref()).await; let token = p.claimed_at.unwrap();
    let mut keep = registration(&p); keep.block_id = "keep".into();
    b.storage.upsert_event_wait_claimed(&keep, token).await.unwrap();
    let mut blocks = Vec::new();
    for i in 0..405 {
        let mut wait = registration(&p); wait.block_id = format!("step-{i}");
        b.storage.upsert_event_wait_claimed(&wait, token).await.unwrap();
        blocks.push(wait.block_id);
    }
    assert_eq!(b.storage.delete_event_waits_for_blocks_claimed(p.id, &[], token).await.unwrap(), Some(0));
    let next = Utc::now()+chrono::Duration::seconds(2);
    assert!(b.storage.conditional_update_claimed_instance_state(p.id, token, InstanceState::Running, InstanceState::Scheduled, Some(next)).await.unwrap());
    let replacement = b.storage.claim_due_instances(next, 1, 0).await.unwrap().pop().unwrap();
    assert_eq!(b.storage.delete_event_waits_for_blocks_claimed(p.id, &blocks, token).await.unwrap(), None);
    assert!(b.storage.get_event_wait(p.id, &blocks[0]).await.unwrap().is_some());
    assert_eq!(b.storage.delete_event_waits_for_blocks_claimed(p.id, &blocks, replacement.claimed_at.unwrap()).await.unwrap(), Some(405));
    for block in &blocks { assert!(b.storage.get_event_wait(p.id, block).await.unwrap().is_none()); }
    assert_eq!(b.storage.get_event_wait(p.id, "keep").await.unwrap().unwrap(), keep);
});
