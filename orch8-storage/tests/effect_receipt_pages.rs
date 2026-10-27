//! Keyset ordering follows physical columns across timestamp precision differences.
use chrono::{Timelike, Utc};
use orch8_storage::{
    StorageBackend,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    continuity::{
        ContinuityExecution, ContinuityId, EffectId, EffectKind, EffectReceipt, EffectState,
        ExecutionEpoch, OwnershipState, RuntimeId,
    },
    ids::{BlockId, TenantId},
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

orch8_storage::backend_test!(async fn pages_use_sql_timestamps_and_id_tiebreakers_without_repeats_or_gaps(b) {
    let mut receipt=fixture(b.storage.as_ref()).await;
    let mut expected=Vec::new();
    for (group,nanos) in [123_456_111,123_456_789].into_iter().enumerate() {
        receipt.created_at=receipt.created_at.with_nanosecond(nanos).unwrap();
        for index in (0..7).rev() {
            receipt.id=EffectId::from_uuid(uuid::Uuid::from_u128((group*10+index+1) as u128));
            b.storage.create_effect_receipt(&receipt).await.unwrap();
        }
        for index in 0..7 {expected.push(EffectId::from_uuid(uuid::Uuid::from_u128((group*10+index+1) as u128)));}
    }
    for size in [1,2,3,7,1000] {
        let mut cursor=None;let mut seen=Vec::new();
        for _ in 0..30 {
            let page=b.storage.page_effect_receipts(&receipt.tenant_id,receipt.continuity_id,cursor.as_ref(),size).await.unwrap();
            seen.extend(page.receipts.iter().map(|receipt|receipt.id));
            if page.next.is_none() {break;}
            cursor=page.next;
        }
        assert_eq!(seen,expected,"SQL timestamp rounding must not change keyset progress");
    }
    let empty=b.storage.page_effect_receipts(&receipt.tenant_id,receipt.continuity_id,None,0).await.unwrap();
    assert!(empty.receipts.is_empty()&&empty.next.is_none());
    assert!(b.storage.page_effect_receipts(&TenantId::unchecked("foreign"),receipt.continuity_id,None,10).await.unwrap().receipts.is_empty());
    assert!(b.storage.page_effect_receipts(&receipt.tenant_id,ContinuityId::new(),None,10).await.unwrap().receipts.is_empty());
});

orch8_storage::backend_test!(async fn receipt_pages_cap_reads_and_continue_at_the_physical_boundary(b) {
    let mut receipt=fixture(b.storage.as_ref()).await;
    for _ in 0..1001 {receipt.id=EffectId::new();b.storage.create_effect_receipt(&receipt).await.unwrap();}
    let first=b.storage.page_effect_receipts(&receipt.tenant_id,receipt.continuity_id,None,u32::MAX).await.unwrap();
    assert_eq!(first.receipts.len(),1000);
    let last=b.storage.page_effect_receipts(&receipt.tenant_id,receipt.continuity_id,first.next.as_ref(),u32::MAX).await.unwrap();
    assert_eq!(last.receipts.len(),1);assert!(last.next.is_none());
    assert!(!first.receipts.iter().any(|row|row.id==last.receipts[0].id));
});

orch8_storage::backend_test!(async fn receipt_pages_reject_json_that_disagrees_with_indexed_identity(b) {
    for field in ["tenant_id","continuity_id","id"] {
        let receipt=fixture(b.storage.as_ref()).await;
        b.storage.create_effect_receipt(&receipt).await.unwrap();
        let mut record=serde_json::to_value(&receipt).unwrap();
        record[field]=if field=="tenant_id" {serde_json::json!("foreign")} else {serde_json::json!(uuid::Uuid::now_v7())};
        b.raw.set_json("effect_receipts","record","id",&receipt.id.to_string(),&record).await;
        assert!(b.storage.page_effect_receipts(&receipt.tenant_id,receipt.continuity_id,None,100).await.is_err());
    }
});
