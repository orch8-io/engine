//! Agent recovery must not replace or erase a successor's progress.
use chrono::Utc;
use orch8_engine::handlers::{StepContext, agent::handle_agent};
use orch8_storage::{
    StorageBackend, backend_test,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    context::ExecutionContext, error::StepError, ids::BlockId, instance::InstanceState,
};
use serde_json::json;
use std::sync::Arc;

async fn context(storage: Arc<dyn StorageBackend>) -> StepContext {
    let sequence = SequenceBuilder::new("agent-owner")
        .step("work", "agent")
        .build();
    storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    storage.create_instance(&instance).await.unwrap();
    let token = storage
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap()
        .claimed_at;
    StepContext {
        effect_admission: None,
        claim_token: token,
        instance_id: instance.id,
        tenant_id: instance.tenant_id,
        block_id: BlockId::new("work"),
        params: json!({"goal":"finish","max_iterations":1,"tools":[]}),
        context: Arc::new(ExecutionContext::default()),
        attempt: 0,
        storage,
        wait_for_input: None,
    }
}
backend_test!(async fn stale_agent_cannot_clear_successor_checkpoint(b) {
    let ctx=context(b.storage.clone()).await;
    let old=ctx.claim_token.unwrap();
    b.storage.update_instance_state(ctx.instance_id,InstanceState::Scheduled,Some(Utc::now())).await.unwrap();
    let successor=b.storage.claim_due_instances(Utc::now()+chrono::Duration::seconds(1),1,0).await.unwrap().pop().unwrap().claimed_at.unwrap();
    assert_ne!(old,successor);
    let checkpoint=json!({"messages":[],"iteration":1,"tool_calls_made":0,"owner":"successor"});
    b.storage.set_instance_kv(ctx.instance_id,"__agent__:work",&checkpoint).await.unwrap();
    let id=ctx.instance_id;
    let result=handle_agent(ctx).await;
    assert_eq!(b.storage.get_instance_kv(id,"__agent__:work").await.unwrap(),Some(checkpoint),"stale agent must not erase successor progress");
    assert!(matches!(result,Err(StepError::ClaimLost)));
});
backend_test!(async fn failed_agent_checkpoint_load_is_not_a_fresh_start(b) {
    use orch8_storage::testing::faulty::FaultStorage;
    let mut ctx=context(b.storage.clone()).await;
    let (storage,faults)=FaultStorage::wrap(b.storage.clone());
    faults.on("get_instance_kv").times(1).fail_transient();
    faults.on("get_instance_kv_claimed").times(1).fail_transient();
    ctx.storage=storage;
    let result=handle_agent(ctx).await;
    assert_eq!(faults.calls("get_instance_kv_claimed"),1,"checkpoint load must retain its original claim");
    assert!(matches!(result,Err(StepError::Retryable{..})),"checkpoint failure must stop before starting a fresh provider turn");
});

backend_test!(async fn corrupt_agent_checkpoint_is_preserved_for_explicit_recovery(b) {
    let ctx=context(b.storage.clone()).await;
    let checkpoint=json!({"messages":[],"iteration":"broken","tool_calls_made":0});
    b.storage.set_instance_kv(ctx.instance_id,"__agent__:work",&checkpoint).await.unwrap();
    let id=ctx.instance_id;
    assert!(matches!(handle_agent(ctx).await,Err(StepError::Permanent{..})));
    assert_eq!(b.storage.get_instance_kv(id,"__agent__:work").await.unwrap(),Some(checkpoint));
});

backend_test!(async fn checkpoint_load_failure_before_agent_admission_can_retry_same_effect(b) {
    use orch8_engine::handlers::{HandlerRegistry,step::{StepExecParams,execute_step_dry}};
    use orch8_storage::testing::faulty::FaultStorage;
    use orch8_types::continuity::EffectState;
    let ctx=context(b.storage.clone()).await;
    let checkpoint=json!({"messages":[],"iteration":1,"tool_calls_made":0});
    b.storage.set_instance_kv(ctx.instance_id,"__agent__:work",&checkpoint).await.unwrap();
    let (storage,faults)=FaultStorage::wrap(b.storage.clone());
    faults.on("get_instance_kv_claimed").times(1).fail_transient();
    let exec=||StepExecParams {claim_token:ctx.claim_token,instance_id:ctx.instance_id,tenant_id:ctx.tenant_id.clone(),block_id:ctx.block_id.clone(),handler_name:"agent".into(),params:ctx.params.clone(),context:ctx.context.as_ref().clone(),attempt:0,timeout:None,externalize_threshold:0,wait_for_input:None,cache_key:None,output_schema:None};
    let mut registry=HandlerRegistry::new();orch8_engine::handlers::builtin::register_builtins(&mut registry);
    assert!(execute_step_dry(&storage,&registry,exec()).await.is_err());
    let ledger=b.storage.get_continuity_execution_by_instance(&ctx.tenant_id,ctx.instance_id).await.unwrap().unwrap();
    let receipts=b.storage.list_effect_receipts(&ctx.tenant_id,ledger.continuity_id,10).await.unwrap();
    assert_eq!(receipts.len(),1);assert_eq!(receipts[0].state,EffectState::Prepared);
    assert_eq!(b.storage.get_instance_kv(ctx.instance_id,"__agent__:work").await.unwrap(),Some(checkpoint));
    let output=execute_step_dry(&storage,&registry,exec()).await.unwrap();
    assert_eq!(output.output["stop_reason"],"max_iterations");
    assert!(b.storage.get_instance_kv(ctx.instance_id,"__agent__:work").await.unwrap().is_none());
    assert_eq!(b.storage.list_effect_receipts(&ctx.tenant_id,ledger.continuity_id,10).await.unwrap()[0].state,EffectState::Committed);
});
