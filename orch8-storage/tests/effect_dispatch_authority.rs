//! Last-moment receipt admission must prove the live durable owner and claim.
use chrono::Utc;
use orch8_storage::{
    EffectDispatchFence, StorageBackend,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    continuity::{
        ContinuityExecution, ContinuityId, EffectDispatchOutcome, EffectId, EffectKind,
        EffectReceipt, EffectState, ExecutionEpoch, OwnershipState, RuntimeId,
    },
    ids::{BlockId, TenantId},
    instance::{InstanceState, TaskInstance},
};
use std::sync::Arc;

async fn fixture(
    storage: &Arc<dyn StorageBackend>,
) -> (
    TaskInstance,
    ContinuityExecution,
    EffectReceipt,
    EffectDispatchFence,
) {
    let sequence = SequenceBuilder::new("dispatch-authority")
        .step("work", "charge")
        .build();
    storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Running)
        .build();
    storage.create_instance(&instance).await.unwrap();
    let instance = storage.get_instance(instance.id).await.unwrap().unwrap();
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
    let receipt = EffectReceipt {
        id: EffectId::new(),
        tenant_id: instance.tenant_id.clone(),
        continuity_id: owner.continuity_id,
        epoch: owner.epoch,
        instance_id: instance.id,
        block_id: BlockId::new("work"),
        kind: EffectKind::Worker,
        state: EffectState::Prepared,
        destination_fingerprint: "destination".into(),
        idempotency_key: Some("stable-key".into()),
        request_sha256: "request".into(),
        provider_receipt_id: None,
        attempt: 0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    storage.create_effect_receipt(&receipt).await.unwrap();
    let fence = EffectDispatchFence {
        instance_id: instance.id,
        epoch: owner.epoch,
        owner_runtime_id: owner.owner_runtime_id,
        claim_token: instance.claimed_at,
    };
    (instance, owner, receipt, fence)
}

orch8_storage::backend_test!(async fn owned_dispatch_rejects_stale_authority_without_changing_receipt(b) {
    for guarded in [false,true] {
        let (instance,owner,mut next,fence)=fixture(&b.storage).await;
        next.state=EffectState::Dispatched;next.updated_at=Utc::now();
        let mut bad=fence;bad.epoch=bad.epoch.checked_next().unwrap();
        assert_eq!(b.storage.dispatch_effect_receipt_owned(&instance.tenant_id,&next,&bad,guarded).await.unwrap(),EffectDispatchOutcome::Stale);
        bad=fence;bad.owner_runtime_id=RuntimeId::new();
        assert_eq!(b.storage.dispatch_effect_receipt_owned(&instance.tenant_id,&next,&bad,guarded).await.unwrap(),EffectDispatchOutcome::Stale);
        bad=fence;bad.claim_token=Some(Utc::now()+chrono::Duration::days(1));
        assert_eq!(b.storage.dispatch_effect_receipt_owned(&instance.tenant_id,&next,&bad,guarded).await.unwrap(),EffectDispatchOutcome::Stale);
        for state in [InstanceState::Paused,InstanceState::Cancelled,InstanceState::Completed] {
            b.storage.update_instance_state(instance.id,state,None).await.unwrap();
            assert_eq!(b.storage.dispatch_effect_receipt_owned(&instance.tenant_id,&next,&fence,guarded).await.unwrap(),EffectDispatchOutcome::Stale);
        }
        // Restore a new live claim; the old one cannot be silently adopted.
        let now=Utc::now();
        b.storage.update_instance_state(instance.id,InstanceState::Scheduled,Some(now)).await.unwrap();
        let reclaimed=b.storage.claim_due_instances(now+chrono::Duration::seconds(1),1,0).await.unwrap().pop().unwrap();
        let mut fresh=fence;fresh.claim_token=reclaimed.claimed_at;
        assert_eq!(b.storage.dispatch_effect_receipt_owned(&instance.tenant_id,&next,&fence,guarded).await.unwrap(),EffectDispatchOutcome::Stale);
        let mut transferring=owner.clone();transferring.state=OwnershipState::Transferring;
        assert!(b.storage.cas_continuity_owner(&instance.tenant_id,owner.continuity_id,owner.epoch,owner.owner_runtime_id,&transferring).await.unwrap());
        assert_eq!(b.storage.dispatch_effect_receipt_owned(&instance.tenant_id,&next,&fresh,guarded).await.unwrap(),EffectDispatchOutcome::Stale);
        assert!(b.storage.cas_continuity_owner(&instance.tenant_id,owner.continuity_id,owner.epoch,owner.owner_runtime_id,&owner).await.unwrap());
        let mut changed=next.clone();changed.request_sha256="substituted".into();
        assert_eq!(b.storage.dispatch_effect_receipt_owned(&instance.tenant_id,&changed,&fresh,guarded).await.unwrap(),EffectDispatchOutcome::Stale);
        assert_eq!(b.storage.dispatch_effect_receipt_owned(&TenantId::new("foreign").unwrap(),&next,&fresh,guarded).await.unwrap(),EffectDispatchOutcome::Stale);
        assert_eq!(b.storage.get_effect_receipt(&instance.tenant_id,next.id).await.unwrap().unwrap().state,EffectState::Prepared);
        // Loop/reset recovery may reuse the logical key. An old admission
        // cannot adopt a newly created receipt row with that same ID.
        let mut reissued=b.storage.get_effect_receipt(&instance.tenant_id,next.id).await.unwrap().unwrap();
        b.storage.delete_effect_receipts_for_blocks(&instance.tenant_id,instance.id,std::slice::from_ref(&next.block_id)).await.unwrap();
        reissued.created_at+=chrono::Duration::seconds(1);reissued.updated_at=reissued.created_at;
        b.storage.create_effect_receipt(&reissued).await.unwrap();
        assert_eq!(b.storage.dispatch_effect_receipt_owned(&instance.tenant_id,&next,&fresh,guarded).await.unwrap(),EffectDispatchOutcome::Stale);
        next.created_at=reissued.created_at;next.updated_at=reissued.updated_at+chrono::Duration::milliseconds(1);
        assert_eq!(b.storage.dispatch_effect_receipt_owned(&instance.tenant_id,&next,&fresh,guarded).await.unwrap(),EffectDispatchOutcome::Dispatched);
        assert_eq!(b.storage.dispatch_effect_receipt_owned(&instance.tenant_id,&next,&fresh,guarded).await.unwrap(),EffectDispatchOutcome::Stale);
    }
});

orch8_storage::backend_test!(async fn owned_guarded_dispatch_serializes_matching_attempts(b) {
    let (instance,_owner,mut left,fence)=fixture(&b.storage).await;
    let mut right=left.clone();right.id=EffectId::new();right.block_id=BlockId::new("other");
    b.storage.create_effect_receipt(&right).await.unwrap();
    left.state=EffectState::Dispatched;left.updated_at=Utc::now();
    right.state=EffectState::Dispatched;right.updated_at=Utc::now();
    let (a,c)=tokio::join!(b.storage.dispatch_effect_receipt_owned(&instance.tenant_id,&left,&fence,true),b.storage.dispatch_effect_receipt_owned(&instance.tenant_id,&right,&fence,true));
    let outcomes=[a.unwrap(),c.unwrap()];
    assert_eq!(outcomes.iter().filter(|outcome|**outcome==EffectDispatchOutcome::Dispatched).count(),1);
    assert_eq!(outcomes.iter().filter(|outcome|**outcome==EffectDispatchOutcome::Duplicate).count(),1);
});

orch8_storage::backend_test!(kinds=[postgres,encrypted_postgres], async fn owned_dispatch_rechecks_authority_after_a_concurrent_writer_commits(b) {
    for change_owner in [false,true] {
        let (instance,owner,mut next,fence)=fixture(&b.storage).await;
        next.state=EffectState::Dispatched;next.updated_at=Utc::now();
        let mut writer=b.pg_pool().begin().await.unwrap();
        let writer_pid:i32=sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *writer).await.unwrap();
        if change_owner {
            let mut transferring=owner.clone();transferring.state=OwnershipState::Transferring;
            sqlx::query("UPDATE continuity_executions SET state='transferring',record=$1 WHERE continuity_id=$2").bind(serde_json::to_value(&transferring).unwrap()).bind(owner.continuity_id.into_uuid()).execute(&mut *writer).await.unwrap();
        } else {
            sqlx::query("UPDATE task_instances SET claimed_at=$1 WHERE id=$2").bind(fence.claim_token.unwrap()+chrono::Duration::seconds(1)).bind(instance.id.into_uuid()).execute(&mut *writer).await.unwrap();
        }
        let store=b.storage.clone();let tenant=instance.tenant_id.clone();
        let dispatch=tokio::spawn(async move {store.dispatch_effect_receipt_owned(&tenant,&next,&fence,false).await});
        tokio::time::timeout(std::time::Duration::from_secs(5),async {
            loop {
                let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))").bind(writer_pid).fetch_one(b.pg_pool()).await.unwrap();
                if blocked {break;}
                assert!(!dispatch.is_finished(),"dispatch must wait for the durable authority writer");
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }).await.expect("dispatch must reach the held owner/instance lock");
        writer.commit().await.unwrap();
        assert_eq!(dispatch.await.unwrap().unwrap(),EffectDispatchOutcome::Stale);
        assert_eq!(b.storage.list_effect_receipts(&instance.tenant_id,owner.continuity_id,10).await.unwrap()[0].state,EffectState::Prepared);
    }
});
