//! Caller ownership is checked in storage, including at the signal commit boundary.
use chrono::Utc;
use orch8_storage::backend_test;
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
use orch8_types::{
    error::StorageError,
    ids::{InstanceId, Namespace, TenantId},
    instance::InstanceState,
    signal::{Signal, SignalType},
};
use serde_json::json;
use uuid::Uuid;

fn signal(target: InstanceId) -> Signal {
    Signal {
        id: Uuid::now_v7(),
        instance_id: target,
        signal_type: SignalType::Resume,
        payload: json!({"private":"payload"}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    }
}

backend_test!(async fn caller_reads_preserve_tenant_subtenant_and_encryption_contract(b) {
    let sequence = SequenceBuilder::new("coordination").step("work", "noop").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let mut caller = InstanceBuilder::for_sequence(&sequence).build();
    caller.sub_tenant = Some("customer-a".into());
    b.storage.create_instance(&caller).await.unwrap();
    let mut target = InstanceBuilder::for_sequence(&sequence).data(json!({"secret":"expected cleartext"})).build();
    target.sub_tenant = caller.sub_tenant.clone();
    target.namespace = Namespace::new("different-legacy-namespace");
    b.storage.create_instance(&target).await.unwrap();
    let visible = b.storage.get_instance_for_caller(caller.id,&caller.tenant_id,target.id).await.unwrap().unwrap();
    assert_eq!(visible.context.data, target.context.data, "authorized encrypted reads must decrypt");
    assert!(b.storage.get_instance_for_caller(InstanceId::new(),&caller.tenant_id,target.id).await.unwrap().is_none());
    assert!(b.storage.get_instance_for_caller(caller.id,&TenantId::unchecked("foreign"),target.id).await.unwrap().is_none());
    assert!(b.storage.get_instance_for_caller(caller.id,&caller.tenant_id,InstanceId::new()).await.unwrap().is_none());
    for subtenant in [Some("customer-b".into()), None, Some(String::new())] {
        let mut denied = target.clone(); denied.id=InstanceId::new(); denied.sub_tenant=subtenant;
        b.storage.create_instance(&denied).await.unwrap();
        assert!(b.storage.get_instance_for_caller(caller.id,&caller.tenant_id,denied.id).await.unwrap().is_none());
        assert!(matches!(b.storage.enqueue_signal_for_caller(caller.id,&caller.tenant_id,&signal(denied.id), None).await,Err(StorageError::NotFound{..})));
        assert!(b.storage.get_pending_signals(denied.id).await.unwrap().is_empty());
    }
    let unrestricted = InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&unrestricted).await.unwrap();
    assert!(b.storage.get_instance_for_caller(unrestricted.id,&unrestricted.tenant_id,target.id).await.unwrap().is_some());
    b.storage.enqueue_signal_for_caller(caller.id,&caller.tenant_id,&signal(target.id), None).await.unwrap();
    assert_eq!(b.storage.get_pending_signals(target.id).await.unwrap()[0].payload,json!({"private":"payload"}));
});

backend_test!(async fn signal_commit_rechecks_caller_and_hides_unauthorized_terminal_targets(b) {
    let sequence = SequenceBuilder::new("coordination").step("work", "noop").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let caller = InstanceBuilder::for_sequence(&sequence).build();
    let target = InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&caller).await.unwrap();
    b.storage.create_instance(&target).await.unwrap();
    assert!(b.storage.get_instance_for_caller(caller.id,&caller.tenant_id,target.id).await.unwrap().is_some());
    b.storage.update_instance_state(caller.id,InstanceState::Completed,None).await.unwrap();
    assert_eq!(b.storage.purge_terminal_instances(&[caller.id]).await.unwrap(),1);
    assert!(matches!(b.storage.enqueue_signal_for_caller(caller.id,&caller.tenant_id,&signal(target.id), None).await,Err(StorageError::NotFound{..})));
    assert!(b.storage.get_pending_signals(target.id).await.unwrap().is_empty());
    b.storage.create_instance(&caller).await.unwrap();
    b.storage.update_instance_state(target.id,InstanceState::Completed,None).await.unwrap();
    assert!(matches!(b.storage.enqueue_signal_for_caller(caller.id,&TenantId::unchecked("foreign"),&signal(target.id), None).await,Err(StorageError::NotFound{..})));
    assert!(matches!(b.storage.enqueue_signal_for_caller(caller.id,&caller.tenant_id,&signal(target.id), None).await,Err(StorageError::TerminalTarget{..})));
    assert!(b.storage.get_pending_signals(target.id).await.unwrap().is_empty());
});

backend_test!(async fn reciprocal_and_self_signals_do_not_invert_owner_row_locks(b) {
    let sequence = SequenceBuilder::new("coordination").step("work", "noop").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let alice = InstanceBuilder::for_sequence(&sequence).build();
    let bob = InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&alice).await.unwrap(); b.storage.create_instance(&bob).await.unwrap();
    let forward=signal(bob.id); let backward=signal(alice.id); let own=signal(alice.id);
    let results=tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(b.storage.enqueue_signal_for_caller(alice.id,&alice.tenant_id,&forward, None),
            b.storage.enqueue_signal_for_caller(bob.id,&bob.tenant_id,&backward, None),
            b.storage.enqueue_signal_for_caller(alice.id,&alice.tenant_id,&own, None))
    }).await.unwrap();
    results.0.unwrap();results.1.unwrap();results.2.unwrap();
    assert_eq!(b.storage.get_pending_signals(alice.id).await.unwrap().len(),2);
    assert_eq!(b.storage.get_pending_signals(bob.id).await.unwrap().len(),1);
});

backend_test!(async fn caller_claim_loss_cannot_enqueue_after_ownership_preflight(b) {
    let sequence=SequenceBuilder::new("coordination-claim").step("work","noop").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let caller=InstanceBuilder::for_sequence(&sequence).due_now().build();
    let target=InstanceBuilder::for_sequence(&sequence).state(InstanceState::Waiting).build();
    b.storage.create_instance(&caller).await.unwrap();b.storage.create_instance(&target).await.unwrap();
    let now=Utc::now()+chrono::Duration::seconds(1);
    let claimed=b.storage.claim_due_instances(now,1,0).await.unwrap().pop().unwrap();
    assert_eq!(claimed.id,caller.id);
    let token=claimed.claimed_at.unwrap();
    assert!(b.storage.get_instance_for_caller(caller.id,&caller.tenant_id,target.id).await.unwrap().is_some());
    for state in [InstanceState::Running,InstanceState::Waiting,InstanceState::Scheduled,InstanceState::Paused,InstanceState::Completed,InstanceState::Failed,InstanceState::Cancelled] {
        b.storage.update_instance_state(caller.id,state,None).await.unwrap();
        let inserted=b.storage.enqueue_signal_for_caller(caller.id,&caller.tenant_id,&signal(target.id),Some(token)).await.unwrap();
        assert_eq!(inserted,state==InstanceState::Running,"only the original running claim can enqueue");
    }
    b.storage.update_instance_state(caller.id,InstanceState::Scheduled,Some(now)).await.unwrap();
    let replacement=b.storage.claim_due_instances(now+chrono::Duration::seconds(1),1,0).await.unwrap().pop().unwrap().claimed_at.unwrap();
    assert_ne!(token,replacement);
    assert!(!b.storage.enqueue_signal_for_caller(caller.id,&caller.tenant_id,&signal(target.id),Some(token)).await.unwrap());
    assert!(b.storage.enqueue_signal_for_caller(caller.id,&caller.tenant_id,&signal(target.id),Some(replacement)).await.unwrap());
    assert_eq!(b.storage.get_pending_signals(target.id).await.unwrap().len(),2);
});
