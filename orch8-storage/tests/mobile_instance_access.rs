//! Explicit mobile authority and decorator parity on every backend.
use orch8_storage::{
    MobileInstanceAccess, backend_test,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{error::StorageError, ids::InstanceId, instance::InstanceState};
use serde_json::json;

backend_test!(async fn mobile_instance_reads_keep_legacy_tenant_and_encryption_authority(b) {
    let mobile=SequenceBuilder::new("mobile").step("work","noop").build();
    let other=SequenceBuilder::new("other-owner").step("work","noop").build();
    b.storage.create_sequence(&mobile).await.unwrap();
    b.storage.create_sequence(&other).await.unwrap();
    let own=InstanceBuilder::for_sequence(&mobile).data(json!({"private":"mobile-canary"})).build();
    let foreign=InstanceBuilder::for_sequence(&other).state(InstanceState::Completed).build();
    b.storage.create_instance(&own).await.unwrap();
    b.storage.create_instance(&foreign).await.unwrap();
    assert_eq!(b.storage.get_mobile_instance(&MobileInstanceAccess::Legacy,own.id).await.unwrap().unwrap().context.data,own.context.data);
    assert!(b.storage.get_mobile_instance(&MobileInstanceAccess::Legacy,foreign.id).await.unwrap().is_none());
    assert!(b.storage.cancel_mobile_instance(&MobileInstanceAccess::Legacy,foreign.id).await.unwrap().is_none());
    assert!(b.storage.get_mobile_instance(&MobileInstanceAccess::Legacy,InstanceId::new()).await.unwrap().is_none());
    let catalog=MobileInstanceAccess::Catalog {account_id:"not-an-account".into(),catalog_id:"not-a-catalog".into(),session_epoch:1};
    if b.kind.is_postgres() {
        assert!(matches!(b.storage.get_mobile_instance(&catalog,own.id).await,Err(StorageError::Unsupported(_))));
        assert!(matches!(b.storage.cancel_mobile_instance(&catalog,own.id).await,Err(StorageError::Unsupported(_))));
    } else {
        assert!(b.storage.get_mobile_instance(&catalog,own.id).await.unwrap().is_none());
        assert!(b.storage.cancel_mobile_instance(&catalog,own.id).await.unwrap().is_none());
    }
});

backend_test!(async fn mobile_cancel_preserves_terminal_results_and_clears_the_original_claim(b) {
    let sequence=SequenceBuilder::new("mobile").step("work","noop").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let own=InstanceBuilder::for_sequence(&sequence).data(json!({"private":"unchanged"})).build();
    b.storage.create_instance(&own).await.unwrap();
    let claimed=b.storage.claim_due_instances(chrono::Utc::now()+chrono::Duration::seconds(1),1,0).await.unwrap().pop().unwrap();
    assert_eq!(claimed.id,own.id);
    assert!(claimed.claimed_at.is_some());
    assert_eq!(b.storage.cancel_mobile_instance(&MobileInstanceAccess::Legacy,own.id).await.unwrap(),Some(InstanceState::Cancelled));
    let after=b.storage.get_mobile_instance(&MobileInstanceAccess::Legacy,own.id).await.unwrap().unwrap();
    assert!(after.claimed_at.is_none());
    assert!(after.next_fire_at.is_none());
    assert_eq!(after.context.data,own.context.data);
    for terminal in [InstanceState::Completed,InstanceState::Failed,InstanceState::Cancelled] {
        let row=InstanceBuilder::for_sequence(&sequence).state(terminal).build();
        b.storage.create_instance(&row).await.unwrap();
        let before=b.storage.get_instance(row.id).await.unwrap().unwrap();
        assert_eq!(b.storage.cancel_mobile_instance(&MobileInstanceAccess::Legacy,row.id).await.unwrap(),Some(terminal));
        let after=b.storage.get_instance(row.id).await.unwrap().unwrap();
        assert_eq!(before.updated_at,after.updated_at);
        assert_eq!(before.state,after.state);
    }
});
