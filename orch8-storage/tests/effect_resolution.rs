//! Operator settlement must not outlive or overwrite its provenance/evidence.
use chrono::Utc;
use orch8_storage::{
    StorageBackend,
    effect_resolution::{EffectResolutionCommit as Commit, effect_resolution_digest},
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    continuity::{
        ContinuityExecution, ContinuityId, EffectId, EffectKind, EffectReceipt, EffectState,
        ExecutionEpoch, OwnershipState, ProvenanceEntry, RuntimeId,
    },
    ids::BlockId,
};
use uuid::Uuid;

async fn fixture(s: &dyn StorageBackend) -> (ContinuityExecution, EffectReceipt) {
    let sequence = SequenceBuilder::new("effect-resolution")
        .step("work", "noop")
        .build();
    s.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).build();
    s.create_instance(&instance).await.unwrap();
    let now = Utc::now();
    let owner = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: instance.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: now,
    };
    s.create_continuity_execution(&owner).await.unwrap();
    let receipt = EffectReceipt {
        id: EffectId::new(),
        tenant_id: owner.tenant_id.clone(),
        continuity_id: owner.continuity_id,
        epoch: owner.epoch,
        instance_id: instance.id,
        block_id: BlockId::new("work"),
        kind: EffectKind::Worker,
        state: EffectState::Unknown,
        destination_fingerprint: "provider".into(),
        idempotency_key: Some("request".into()),
        request_sha256: "a".repeat(64),
        provider_receipt_id: None,
        attempt: 1,
        created_at: now,
        updated_at: now,
    };
    s.create_effect_receipt(&receipt).await.unwrap();
    (owner, receipt)
}
fn proposal(expected: &EffectReceipt) -> (EffectReceipt, ProvenanceEntry) {
    let mut next = expected.clone();
    next.transition(EffectState::Committed, Utc::now()).unwrap();
    next.provider_receipt_id = Some("operator-confirmed".into());
    let entry = ProvenanceEntry {
        id: Uuid::now_v7(),
        tenant_id: expected.tenant_id.clone(),
        continuity_id: expected.continuity_id,
        epoch: expected.epoch,
        kind: "effect_resolved".into(),
        redacted_summary: Some("operator resolution".into()),
        payload_sha256: effect_resolution_digest(&next).unwrap(),
        previous_sha256: None,
        entry_sha256: Uuid::now_v7().simple().to_string().repeat(2),
        signing_key_id: None,
        signature: None,
        created_at: Utc::now(),
    };
    (next, entry)
}

orch8_storage::backend_test!(async fn failed_provenance_insert_rolls_back_resolution(b) {
    use orch8_storage::testing::backends::RawPool;
    let s=b.storage.as_ref();let (owner,expected)=fixture(s).await;
    let (next,entry)=proposal(&expected);
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("CREATE TRIGGER reject_operator_provenance BEFORE INSERT ON provenance_entries BEGIN SELECT RAISE(ABORT,'injected provenance failure'); END;").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION reject_operator_provenance() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected provenance failure'; END $$; CREATE TRIGGER reject_operator_provenance BEFORE INSERT ON provenance_entries FOR EACH ROW EXECUTE FUNCTION reject_operator_provenance();").execute(pool).await.unwrap();},
    }
    assert!(s.resolve_effect_with_provenance(&expected,&next,&entry).await.is_err());
    assert_eq!(s.get_effect_receipt(&expected.tenant_id,expected.id).await.unwrap(),Some(expected.clone()));
    assert!(s.list_provenance(&owner.tenant_id,owner.continuity_id,10).await.unwrap().is_empty());
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("DROP TRIGGER reject_operator_provenance").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("DROP TRIGGER reject_operator_provenance ON provenance_entries; DROP FUNCTION reject_operator_provenance();").execute(pool).await.unwrap();},
    }
    assert_eq!(s.resolve_effect_with_provenance(&expected,&next,&entry).await.unwrap(),Commit::Applied);
    assert_eq!(s.get_effect_receipt(&expected.tenant_id,expected.id).await.unwrap(),Some(next));
    assert_eq!(s.list_provenance(&owner.tenant_id,owner.continuity_id,10).await.unwrap(),vec![entry]);
});

orch8_storage::backend_test!(async fn resolution_preserves_scope_and_exact_expected_evidence(b) {
    let s=b.storage.as_ref();let (_,expected)=fixture(s).await;
    let (next,entry)=proposal(&expected);
    let mut changed=next.clone();changed.request_sha256="b".repeat(64);
    let mut proof=entry.clone();proof.payload_sha256=effect_resolution_digest(&changed).unwrap();
    assert!(s.resolve_effect_with_provenance(&expected,&changed,&proof).await.is_err());
    let mut foreign=entry.clone();foreign.tenant_id=orch8_types::ids::TenantId::unchecked("foreign");
    assert!(s.resolve_effect_with_provenance(&expected,&next,&foreign).await.is_err());
    let mut wrong_digest=entry.clone();wrong_digest.payload_sha256="c".repeat(64);
    assert!(s.resolve_effect_with_provenance(&expected,&next,&wrong_digest).await.is_err());
    let mut stale_epoch=entry.clone();stale_epoch.epoch=ExecutionEpoch::from_u64(1);
    assert_eq!(s.resolve_effect_with_provenance(&expected,&next,&stale_epoch).await.unwrap(),Commit::Stale);
    let mut enriched=expected.clone();enriched.provider_receipt_id=Some("concurrent-provider-evidence".into());
    assert!(s.cas_effect_receipt(&expected.tenant_id,expected.id,expected.state,&enriched).await.unwrap());
    assert_eq!(s.resolve_effect_with_provenance(&expected,&next,&entry).await.unwrap(),Commit::Stale,"same state is not the same evidence");
    assert_eq!(s.get_effect_receipt(&expected.tenant_id,expected.id).await.unwrap(),Some(enriched));
    assert!(s.list_provenance(&expected.tenant_id,expected.continuity_id,10).await.unwrap().is_empty());
});

orch8_storage::backend_test!(async fn chain_conflicts_do_not_partially_resolve_effects(b) {
    let s=b.storage.as_ref();let (_,first)=fixture(s).await;
    let mut second=first.clone();second.id=EffectId::new();second.block_id=BlockId::new("second");s.create_effect_receipt(&second).await.unwrap();
    let (next_a,entry_a)=proposal(&first);let (next_b,entry_b)=proposal(&second);
    let (a,c)=tokio::join!(s.resolve_effect_with_provenance(&first,&next_a,&entry_a),s.resolve_effect_with_provenance(&second,&next_b,&entry_b));
    let a=a.unwrap();let c=c.unwrap();
    assert!(matches!((a,c),(Commit::Applied,Commit::ProvenanceChanged)|(Commit::ProvenanceChanged,Commit::Applied)));
    let (expected,next,mut entry,head)=if a==Commit::Applied {(&second,&next_b,entry_b,entry_a)} else {(&first,&next_a,entry_a,entry_b)};
    assert_eq!(s.get_effect_receipt(&expected.tenant_id,expected.id).await.unwrap(),Some(expected.clone()));
    entry.previous_sha256=Some(head.entry_sha256.clone());
    // A primary-key collision after the receipt UPDATE must roll it back too.
    let original_id=entry.id;entry.id=head.id;
    assert_eq!(s.resolve_effect_with_provenance(expected,next,&entry).await.unwrap(),Commit::ProvenanceChanged);
    assert_eq!(s.get_effect_receipt(&expected.tenant_id,expected.id).await.unwrap(),Some(expected.clone()));
    entry.id=original_id;
    assert_eq!(s.resolve_effect_with_provenance(expected,next,&entry).await.unwrap(),Commit::Applied);
    assert_eq!(s.list_provenance(&first.tenant_id,first.continuity_id,10).await.unwrap(),vec![head,entry]);
});

orch8_storage::backend_test!(async fn suppressed_receipt_update_cannot_commit_provenance(b) {
    use orch8_storage::testing::backends::RawPool;
    let s=b.storage.as_ref();let (owner,expected)=fixture(s).await;
    let (next,entry)=proposal(&expected);
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("CREATE TRIGGER suppress_effect_resolution BEFORE UPDATE ON effect_receipts BEGIN SELECT RAISE(IGNORE); END;").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION suppress_effect_resolution() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$; CREATE TRIGGER suppress_effect_resolution BEFORE UPDATE ON effect_receipts FOR EACH ROW EXECUTE FUNCTION suppress_effect_resolution();").execute(pool).await.unwrap();},
    }
    assert_eq!(s.resolve_effect_with_provenance(&expected,&next,&entry).await.unwrap(),Commit::Stale);
    assert_eq!(s.get_effect_receipt(&expected.tenant_id,expected.id).await.unwrap(),Some(expected));
    assert!(s.list_provenance(&owner.tenant_id,owner.continuity_id,10).await.unwrap().is_empty());
});
