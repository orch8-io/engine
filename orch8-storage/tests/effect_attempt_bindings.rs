//! Durable identity is shared by preparations, retired only at explicit boundaries.
use chrono::Utc;
use orch8_storage::{
    ContinuityStore, StorageBackend,
    testing::{
        backends::{RawPool, TestBackend},
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    continuity::{
        ContinuityExecution, ContinuityId, EffectId, EffectKind, EffectReceipt, EffectState,
        ExecutionEpoch, OwnershipState, RuntimeId,
    },
    ids::BlockId,
};

async fn fixture(storage: &dyn StorageBackend) -> EffectReceipt {
    let sequence = SequenceBuilder::new("attempt-binding")
        .step("work", "charge")
        .build();
    storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
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
    EffectReceipt {
        id: EffectId::new(),
        tenant_id: instance.tenant_id,
        continuity_id: owner.continuity_id,
        epoch: owner.epoch,
        instance_id: instance.id,
        block_id: BlockId::new("work"),
        kind: EffectKind::Worker,
        state: EffectState::Planned,
        destination_fingerprint: "provider".into(),
        idempotency_key: None,
        request_sha256: "request".into(),
        provider_receipt_id: None,
        attempt: 0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

orch8_storage::backend_test!(async fn concurrent_preparations_select_one_durable_identity(b) {
    let left=fixture(b.storage.as_ref()).await;
    let mut right=left.clone();right.id=EffectId::new();let legacy=EffectId::new();
    let (a,c)=tokio::join!(b.storage.ensure_effect_attempt(&left,legacy),b.storage.ensure_effect_attempt(&right,legacy));
    let a=a.unwrap();let c=c.unwrap();assert_eq!(a.id,c.id);
    assert_eq!(b.storage.list_effect_receipts(&left.tenant_id,left.continuity_id,100).await.unwrap().len(),1);
    assert_eq!(b.storage.ensure_effect_attempt(&right,legacy).await.unwrap().id,a.id);
    // A new retry attempt is a different intent under the existing retry policy.
    right.attempt=1;right.id=EffectId::new();
    assert_ne!(b.storage.ensure_effect_attempt(&right,EffectId::new()).await.unwrap().id,a.id);
});

orch8_storage::backend_test!(async fn legacy_receipt_is_adopted_without_replaying_its_state(b) {
    for state in [EffectState::Planned,EffectState::Prepared,EffectState::Dispatched,EffectState::Unknown,EffectState::Committed,EffectState::Verified] {
        let candidate=fixture(b.storage.as_ref()).await;let mut legacy=candidate.clone();legacy.id=EffectId::new();legacy.state=state;
        b.storage.create_effect_receipt(&legacy).await.unwrap();
        let adopted=b.storage.ensure_effect_attempt(&candidate,legacy.id).await.unwrap();
        assert_eq!(adopted.id,legacy.id);assert_eq!(adopted.state,state);
        assert!(b.storage.get_effect_receipt(&candidate.tenant_id,candidate.id).await.unwrap().is_none());
        let mut other=candidate.clone();other.block_id=BlockId::new("foreign-logical-key");
        assert!(b.storage.ensure_effect_attempt(&other,legacy.id).await.is_err());
    }
});

orch8_storage::backend_test!(async fn retirement_preserves_ambiguity_but_creates_a_new_active_intent(b) {
    let candidate=fixture(b.storage.as_ref()).await;let mut legacy=candidate.clone();legacy.id=EffectId::new();legacy.state=EffectState::Unknown;
    b.storage.create_effect_receipt(&legacy).await.unwrap();
    assert_eq!(b.storage.ensure_effect_attempt(&candidate,legacy.id).await.unwrap().id,legacy.id);
    let blocks=[candidate.block_id.clone()];
    for _ in 0..2 { b.storage.retire_effect_attempts_for_blocks(&candidate.tenant_id,candidate.instance_id,&blocks).await.unwrap(); }
    assert_eq!(b.storage.get_effect_receipt(&candidate.tenant_id,legacy.id).await.unwrap().unwrap().state,EffectState::Unknown);
    assert!(b.storage.find_unresolved_effect_receipt(&candidate.tenant_id,candidate.continuity_id,candidate.instance_id,&candidate.block_id,0).await.unwrap().is_none());
    // The old deterministic ID must not be re-adopted after the reset marker.
    let next=b.storage.ensure_effect_attempt(&candidate,legacy.id).await.unwrap();assert_eq!(next.id,candidate.id);assert_ne!(next.id,legacy.id);
    let mut unknown=next.clone();unknown.state=EffectState::Unknown;
    assert!(b.storage.cas_effect_receipt(&candidate.tenant_id,next.id,EffectState::Planned,&unknown).await.unwrap());
    assert_eq!(b.storage.find_unresolved_effect_receipt(&candidate.tenant_id,candidate.continuity_id,candidate.instance_id,&candidate.block_id,0).await.unwrap().unwrap().id,next.id);
});

orch8_storage::backend_test!(async fn stale_claim_cannot_retire_a_current_attempt(b) {
    let candidate=fixture(b.storage.as_ref()).await;let legacy=EffectId::new();
    let first=b.storage.ensure_effect_attempt(&candidate,legacy).await.unwrap();
    let claimed=b.storage.claim_due_instances(Utc::now()+chrono::Duration::seconds(1),1,0).await.unwrap().pop().unwrap();
    let token=claimed.claimed_at.unwrap();let blocks=[candidate.block_id.clone()];
    assert!(b.storage.retire_effect_attempts_for_blocks_claimed(&candidate.tenant_id,candidate.instance_id,&blocks,token+chrono::Duration::seconds(1)).await.unwrap().is_none());
    assert_eq!(b.storage.ensure_effect_attempt(&candidate,legacy).await.unwrap().id,first.id);
    assert!(b.storage.retire_effect_attempts_for_blocks_claimed(&candidate.tenant_id,candidate.instance_id,&blocks,token).await.unwrap().is_some());
    let mut next=candidate.clone();next.id=EffectId::new();
    assert_eq!(b.storage.ensure_effect_attempt(&next,legacy).await.unwrap().id,next.id);
});

async fn reject_bindings(b: &TestBackend) {
    match &b.raw {
        RawPool::Sqlite(pool) => {
            sqlx::query("CREATE TRIGGER fail_binding BEFORE INSERT ON effect_attempt_bindings BEGIN SELECT RAISE(ABORT,'injected binding failure'); END").execute(pool).await.unwrap();
        }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => {
            sqlx::raw_sql("CREATE FUNCTION fail_binding() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected binding failure'; END $$; CREATE TRIGGER fail_binding BEFORE INSERT ON effect_attempt_bindings FOR EACH ROW EXECUTE FUNCTION fail_binding();").execute(pool).await.unwrap();
        }
    }
}
orch8_storage::backend_test!(async fn failed_binding_insert_rolls_back_its_new_receipt(b) {
    let candidate=fixture(b.storage.as_ref()).await;reject_bindings(&b).await;
    assert!(b.storage.ensure_effect_attempt(&candidate,EffectId::new()).await.is_err());
    assert!(b.storage.get_effect_receipt(&candidate.tenant_id,candidate.id).await.unwrap().is_none());
});

#[tokio::test]
async fn sqlite_reopen_preserves_active_identity_and_retired_legacy_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("identity.sqlite");
    let path = path.to_str().unwrap();
    let store = orch8_storage::sqlite::SqliteStorage::file(path)
        .await
        .unwrap();
    let candidate = fixture(&store).await;
    let legacy = EffectId::new();
    assert_eq!(
        store
            .ensure_effect_attempt(&candidate, legacy)
            .await
            .unwrap()
            .id,
        candidate.id
    );
    store.close().await;
    let store = orch8_storage::sqlite::SqliteStorage::file(path)
        .await
        .unwrap();
    let mut next = candidate.clone();
    next.id = EffectId::new();
    assert_eq!(
        store.ensure_effect_attempt(&next, legacy).await.unwrap().id,
        candidate.id
    );
    store
        .retire_effect_attempts_for_blocks(
            &candidate.tenant_id,
            candidate.instance_id,
            std::slice::from_ref(&candidate.block_id),
        )
        .await
        .unwrap();
    store.close().await;
    let store = orch8_storage::sqlite::SqliteStorage::file(path)
        .await
        .unwrap();
    assert_eq!(
        store.ensure_effect_attempt(&next, legacy).await.unwrap().id,
        next.id
    );
    store.close().await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_rollback_refuses_active_bindings_and_retired_markers() {
    let Some(b) = TestBackend::postgres().await else {
        return;
    };
    let candidate = fixture(b.storage.as_ref()).await;
    b.storage
        .ensure_effect_attempt(&candidate, EffectId::new())
        .await
        .unwrap();
    let down = include_str!("../../migrations/down/127_effect_attempt_bindings.sql");
    assert!(sqlx::raw_sql(down).execute(b.pg_pool()).await.is_err());
    b.storage
        .retire_effect_attempts_for_blocks(
            &candidate.tenant_id,
            candidate.instance_id,
            std::slice::from_ref(&candidate.block_id),
        )
        .await
        .unwrap();
    assert!(
        sqlx::raw_sql(down).execute(b.pg_pool()).await.is_err(),
        "reset markers also protect against unsafe downgrade"
    );
    sqlx::query("DELETE FROM effect_block_resets")
        .execute(b.pg_pool())
        .await
        .unwrap();
    sqlx::raw_sql(down).execute(b.pg_pool()).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../migrations/127_effect_attempt_bindings.sql"
    ))
    .execute(b.pg_pool())
    .await
    .unwrap();
    assert_eq!(
        b.storage
            .ensure_effect_attempt(&candidate, EffectId::new())
            .await
            .unwrap()
            .id,
        candidate.id
    );
}
