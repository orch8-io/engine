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

orch8_storage::backend_test!(async fn complete_audits_bound_witnesses_without_truncating_the_verdict(b) {
    let mut r = fixture(b.storage.as_ref()).await;
    let empty = b.storage.audit_effect_at_most_once(&r.tenant_id, r.continuity_id, r.kind, None).await.unwrap();
    assert_eq!((empty.observed_receipts, empty.duplicate_groups, empty.witnesses.len()), (0, 0, 0));
    for group in 0..40 {
        r.request_sha256 = format!("request-{group:03}");
        for state in [EffectState::Dispatched, EffectState::Committed, EffectState::Unknown, EffectState::Verified,
                      EffectState::Planned, EffectState::Prepared, EffectState::Abandoned, EffectState::Compensated] {
            r.id = EffectId::new(); r.state = state;
            b.storage.create_effect_receipt(&r).await.unwrap();
        }
    }
    // Same request in a different destination is not a duplicate group.
    r.id = EffectId::new(); r.state = EffectState::Committed;
    r.destination_fingerprint = "different-provider".into();
    b.storage.create_effect_receipt(&r).await.unwrap();
    // Another effect kind must not leak into this kind's complete verdict.
    r.kind = EffectKind::Http;
    for _ in 0..2 { r.id = EffectId::new(); b.storage.create_effect_receipt(&r).await.unwrap(); }
    r.kind = EffectKind::Worker;
    let report = b.storage.audit_effect_at_most_once(&r.tenant_id, r.continuity_id, r.kind, None).await.unwrap();
    assert_eq!((report.observed_receipts, report.duplicate_groups, report.witnesses.len()), (161, 40, 16));
    assert_eq!(report.witnesses[0].request_sha256, "request-000");
    assert_eq!(report.witnesses[15].request_sha256, "request-015");
    assert_eq!(b.storage.audit_effect_at_most_once(&r.tenant_id, r.continuity_id, r.kind, None).await.unwrap(), report);
    for (tenant, continuity) in [(TenantId::unchecked("foreign"), r.continuity_id), (r.tenant_id.clone(), ContinuityId::new())] {
        let report = b.storage.audit_effect_at_most_once(&tenant, continuity, r.kind, None).await.unwrap();
        assert_eq!((report.observed_receipts, report.duplicate_groups), (0, 0));
    }
});

orch8_storage::backend_test!(async fn audit_proposals_replace_their_stored_identity_and_reject_scope_changes(b) {
    let mut r = fixture(b.storage.as_ref()).await;
    r.state = EffectState::Committed;
    b.storage.create_effect_receipt(&r).await.unwrap();
    r.state = EffectState::Dispatched;
    let report = b.storage.audit_effect_at_most_once(&r.tenant_id, r.continuity_id, r.kind, Some(&r)).await.unwrap();
    assert_eq!((report.observed_receipts, report.duplicate_groups), (1, 0));
    let mut other = r.clone(); other.id = EffectId::new();
    let report = b.storage.audit_effect_at_most_once(&r.tenant_id, r.continuity_id, r.kind, Some(&other)).await.unwrap();
    assert_eq!((report.observed_receipts, report.duplicate_groups), (2, 1));
    for changed in [
        EffectReceipt { tenant_id: TenantId::unchecked("foreign"), ..other.clone() },
        EffectReceipt { continuity_id: ContinuityId::new(), ..other.clone() },
        EffectReceipt { state: EffectState::Prepared, ..other.clone() },
        EffectReceipt { kind: EffectKind::Http, ..other.clone() },
    ] {
        assert!(b.storage.audit_effect_at_most_once(&r.tenant_id, r.continuity_id, r.kind, Some(&changed)).await.is_err());
    }
    // An attempted duplicate remains hypothetical and never creates a receipt.
    assert!(b.storage.get_effect_receipt(&other.tenant_id, other.id).await.unwrap().is_none());
    let report = b.storage.audit_effect_at_most_once(&r.tenant_id, r.continuity_id, r.kind, None).await.unwrap();
    assert_eq!((report.observed_receipts, report.duplicate_groups), (1, 0));
});

orch8_storage::backend_test!(async fn complete_state_summaries_classify_every_state_and_bound_evidence(b) {
    let mut receipt = fixture(b.storage.as_ref()).await;
    let empty = b.storage.summarize_effect_receipts(&receipt.tenant_id, receipt.continuity_id).await.unwrap();
    assert_eq!((empty.receipt_count, empty.unresolved_count, empty.unknown_count), (0, 0, 0));
    assert!(empty.unresolved_sample.is_empty() && empty.unknown_request_sha256.is_empty());
    for _ in 0..21 {
        for state in [EffectState::Planned, EffectState::Prepared, EffectState::Dispatched, EffectState::Unknown,
                      EffectState::Committed, EffectState::Verified, EffectState::Compensated, EffectState::Abandoned] {
            receipt.id = EffectId::new(); receipt.state = state;
            b.storage.create_effect_receipt(&receipt).await.unwrap();
        }
    }
    let status = b.storage.summarize_effect_receipts(&receipt.tenant_id, receipt.continuity_id).await.unwrap();
    assert_eq!((status.receipt_count, status.unresolved_count, status.unknown_count), (168, 84, 21));
    assert_eq!(status.unresolved_sample.len(), 16);
    assert_eq!(status.unknown_request_sha256.len(), 16);
    assert!(status.unresolved_sample.iter().all(|r| !r.state.is_resolved()));
    assert_eq!(b.storage.summarize_effect_receipts(&receipt.tenant_id, receipt.continuity_id).await.unwrap(), status);
    for (tenant, continuity) in [(TenantId::unchecked("foreign"), receipt.continuity_id), (receipt.tenant_id.clone(), ContinuityId::new())] {
        let absent = b.storage.summarize_effect_receipts(&tenant, continuity).await.unwrap();
        assert_eq!((absent.receipt_count, absent.unresolved_count, absent.unknown_count), (0, 0, 0));
    }
});

orch8_storage::backend_test!(async fn complete_safety_queries_refuse_unclassifiable_receipt_states(b) {
    use orch8_storage::testing::backends::RawPool;
    let mut receipt = fixture(b.storage.as_ref()).await;
    receipt.state = EffectState::Committed;
    b.storage.create_effect_receipt(&receipt).await.unwrap();
    match &b.raw {
        RawPool::Sqlite(pool) => { sqlx::query("UPDATE effect_receipts SET record=json_remove(record,'$.state') WHERE id=?1")
            .bind(receipt.id.to_string()).execute(pool).await.unwrap(); }
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool) => { sqlx::query("UPDATE effect_receipts SET record=record-'state' WHERE id=$1")
            .bind(receipt.id.into_uuid()).execute(pool).await.unwrap(); }
    }
    assert!(b.storage.summarize_effect_receipts(&receipt.tenant_id, receipt.continuity_id).await.is_err());
    assert!(b.storage.audit_effect_at_most_once(&receipt.tenant_id, receipt.continuity_id, receipt.kind, None).await.is_err());
    let unrelated = b.storage.summarize_effect_receipts(&TenantId::unchecked("foreign"), receipt.continuity_id).await.unwrap();
    assert_eq!(unrelated.receipt_count, 0);
});
