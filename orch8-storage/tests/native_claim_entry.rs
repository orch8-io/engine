//! Entry validation is stricter than lease renewal, on every native backend.
use chrono::Utc;
use orch8_storage::backend_test;
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
use orch8_types::{
    ids::{InstanceId, TenantId},
    instance::InstanceState,
};

backend_test!(async fn native_entry_requires_running_tenant_and_exact_claim(b) {
    let sequence=SequenceBuilder::new("native-entry").step("work","native").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let instance=InstanceBuilder::for_sequence(&sequence).due_now().build();
    b.storage.create_instance(&instance).await.unwrap();
    let now=Utc::now()+chrono::Duration::seconds(1);
    let first=b.storage.claim_due_instances(now,1,0).await.unwrap().pop().unwrap();
    let token=first.claimed_at.unwrap();
    for state in [InstanceState::Running,InstanceState::Waiting,InstanceState::Scheduled,
        InstanceState::Paused,InstanceState::Completed,InstanceState::Failed,InstanceState::Cancelled] {
        b.storage.update_instance_state(instance.id,state,None).await.unwrap();
        let before=b.storage.get_instance(instance.id).await.unwrap().unwrap().updated_at;
        assert_eq!(b.storage.verify_instance_claim(instance.id,&instance.tenant_id,token).await.unwrap(),state==InstanceState::Running);
        assert_eq!(b.storage.get_instance(instance.id).await.unwrap().unwrap().updated_at,before,"validation must not renew a lease");
    }
    b.storage.update_instance_state(instance.id,InstanceState::Scheduled,None).await.unwrap();
    let second=b.storage.claim_due_instances(now,1,0).await.unwrap().pop().unwrap();
    let replacement=second.claimed_at.unwrap();
    assert_ne!(token,replacement);
    assert!(!b.storage.verify_instance_claim(instance.id,&instance.tenant_id,token).await.unwrap());
    assert!(b.storage.verify_instance_claim(instance.id,&instance.tenant_id,replacement).await.unwrap());
    assert!(!b.storage.verify_instance_claim(instance.id,&TenantId::unchecked("foreign"),replacement).await.unwrap());
    assert!(!b.storage.verify_instance_claim(InstanceId::new(),&instance.tenant_id,replacement).await.unwrap());
});
