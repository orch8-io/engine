//! Retained tokens must not expose KV/cache/output after ownership is lost.
use chrono::{DateTime, Utc};
use orch8_engine::{
    error::EngineError,
    handlers::{
        HandlerRegistry, StepContext,
        step::{StepExecParams, execute_step_dry},
    },
};
use orch8_storage::{
    StorageBackend, backend_test,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    context::ExecutionContext,
    error::StepError,
    ids::BlockId,
    instance::{InstanceState, TaskInstance},
    output::BlockOutput,
};
use serde_json::json;
use std::sync::Arc;

async fn claimed(storage: &Arc<dyn StorageBackend>) -> (TaskInstance, DateTime<Utc>) {
    let seq = SequenceBuilder::new("claimed-read")
        .step("work", "noop")
        .build();
    storage.create_sequence(&seq).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&seq).due_now().build();
    storage.create_instance(&instance).await.unwrap();
    let token = storage
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap()
        .claimed_at
        .unwrap();
    (instance, token)
}
fn exec(instance: &TaskInstance, token: DateTime<Utc>) -> StepExecParams {
    StepExecParams {
        claim_token: Some(token),
        instance_id: instance.id,
        tenant_id: instance.tenant_id.clone(),
        block_id: BlockId::new("work"),
        handler_name: "noop".into(),
        params: json!({}),
        context: ExecutionContext::default(),
        attempt: 0,
        timeout: None,
        externalize_threshold: 0,
        wait_for_input: None,
        cache_key: None,
        output_schema: None,
    }
}
backend_test!(async fn stale_claim_cannot_read_memoized_output(b) {
    let (instance,token)=claimed(&b.storage).await;
    b.storage.save_block_output(&BlockOutput { id:uuid::Uuid::now_v7(),instance_id:instance.id,block_id:BlockId::new("work"),output:json!({"secret":"memoized"}),output_ref:None,output_size:21,attempt:1,created_at:Utc::now() }).await.unwrap();
    b.storage.update_instance_state(instance.id,InstanceState::Waiting,None).await.unwrap();
    let mut params=exec(&instance,token);params.attempt=1;
    assert!(matches!(execute_step_dry(&b.storage,&HandlerRegistry::new(),params).await,Err(EngineError::ClaimLost{..})));
});
backend_test!(async fn stale_claim_cannot_read_step_cache(b) {
    let (instance,token)=claimed(&b.storage).await;
    b.storage.set_instance_kv(instance.id,"_cache:private",&json!({"secret":"cached"})).await.unwrap();
    b.storage.update_instance_state(instance.id,InstanceState::Waiting,None).await.unwrap();
    let mut params=exec(&instance,token);params.cache_key=Some("private".into());
    assert!(matches!(execute_step_dry(&b.storage,&HandlerRegistry::new(),params).await,Err(EngineError::ClaimLost{..})));
});
backend_test!(async fn stale_claim_cannot_read_builtin_state(b) {
    let (instance,token)=claimed(&b.storage).await;
    b.storage.set_instance_kv(instance.id,"private",&json!({"secret":"state"})).await.unwrap();
    b.storage.update_instance_state(instance.id,InstanceState::Waiting,None).await.unwrap();
    let mut registry=HandlerRegistry::new();orch8_engine::handlers::builtin::register_builtins(&mut registry);
    let context=StepContext{effect_admission:None,claim_token:Some(token),instance_id:instance.id,tenant_id:instance.tenant_id,block_id:BlockId::new("work"),params:json!({"key":"private"}),context:Arc::new(ExecutionContext::default()),attempt:0,storage:b.storage.clone(),wait_for_input:None};
    assert!(matches!(registry.get("get_state").unwrap()(context).await,Err(StepError::ClaimLost)));
});

backend_test!(async fn claimed_reads_preserve_missing_null_latest_and_encrypted_values(b) {
    use orch8_storage::ClaimedRead;
    use orch8_types::ids::{InstanceId,TenantId};
    let (instance,token)=claimed(&b.storage).await;
    let block=BlockId::new("work");
    assert!(matches!(b.storage.get_instance_kv_claimed(instance.id,&instance.tenant_id,"missing",token).await.unwrap(),ClaimedRead::Value(None)));
    assert!(matches!(b.storage.get_block_output_claimed(instance.id,&instance.tenant_id,&block,token).await.unwrap(),ClaimedRead::Value(None)));
    b.storage.set_instance_kv(instance.id,"private",&serde_json::Value::Null).await.unwrap();
    assert!(matches!(b.storage.get_instance_kv_claimed(instance.id,&instance.tenant_id,"private",token).await.unwrap(),ClaimedRead::Value(Some(serde_json::Value::Null))));
    b.storage.set_instance_kv(instance.id,"_cache:private",&json!({"secret":"clear cache"})).await.unwrap();
    let created_at=Utc::now();
    for index in 1..=2 {
        b.storage.save_block_output(&BlockOutput { id:uuid::Uuid::from_u128(index),instance_id:instance.id,block_id:block.clone(),output:json!({"secret":index}),output_ref:None,output_size:12,attempt:1,created_at }).await.unwrap();
    }
    let mut params=exec(&instance,token);params.attempt=1;
    assert_eq!(execute_step_dry(&b.storage,&HandlerRegistry::new(),params).await.unwrap().output,json!({"secret":2}),"latest tied output must decrypt after authorization");
    let mut params=exec(&instance,token);params.cache_key=Some("private".into());
    assert_eq!(execute_step_dry(&b.storage,&HandlerRegistry::new(),params).await.unwrap().output,json!({"secret":"clear cache"}));
    for (id,tenant,claim) in [(InstanceId::new(),instance.tenant_id.clone(),token),
        (instance.id,TenantId::unchecked("foreign"),token),
        (instance.id,instance.tenant_id.clone(),token+chrono::Duration::seconds(1))] {
        assert!(matches!(b.storage.get_instance_kv_claimed(id,&tenant,"private",claim).await.unwrap(),ClaimedRead::Lost));
        assert!(matches!(b.storage.get_block_output_claimed(id,&tenant,&block,claim).await.unwrap(),ClaimedRead::Lost));
    }
    for state in [InstanceState::Waiting,InstanceState::Scheduled,InstanceState::Paused,InstanceState::Completed,InstanceState::Failed,InstanceState::Cancelled] {
        b.storage.update_instance_state(instance.id,state,None).await.unwrap();
        assert!(matches!(b.storage.get_instance_kv_claimed(instance.id,&instance.tenant_id,"private",token).await.unwrap(),ClaimedRead::Lost));
        assert!(matches!(b.storage.get_block_output_claimed(instance.id,&instance.tenant_id,&block,token).await.unwrap(),ClaimedRead::Lost));
    }
});

backend_test!(async fn failed_claimed_cache_read_cannot_fall_through_to_handler_dispatch(b) {
    use orch8_storage::testing::faulty::FaultStorage;
    use std::sync::atomic::{AtomicUsize,Ordering};
    let (instance,token)=claimed(&b.storage).await;
    let (storage,faults)=FaultStorage::wrap(b.storage.clone());
    faults.on("get_instance_kv_claimed").times(1).fail_transient();
    let called=Arc::new(AtomicUsize::new(0));let count=called.clone();
    let mut registry=HandlerRegistry::new();
    registry.register("noop",move |_| { let count=count.clone();async move {count.fetch_add(1,Ordering::SeqCst);Ok(json!({}))} });
    let mut params=exec(&instance,token);params.cache_key=Some("private".into());
    assert!(execute_step_dry(&storage,&registry,params).await.is_err());
    assert_eq!(called.load(Ordering::SeqCst),0);
    assert_eq!(faults.calls("get_instance_kv_claimed"),1);
});
