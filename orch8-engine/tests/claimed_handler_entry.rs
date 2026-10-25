//! A claim must still authorize ordinary handler construction after storage waits.
use chrono::Utc;
use orch8_engine::{
    error::EngineError,
    handlers::{
        HandlerRegistry,
        step::{StepExecParams, execute_step_dry},
    },
};
use orch8_storage::{
    StorageBackend, backend_test,
    testing::{
        faulty::FaultStorage,
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    context::ExecutionContext,
    ids::{BlockId, TenantId},
    instance::InstanceState,
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

async fn running(storage: &Arc<dyn StorageBackend>) -> StepExecParams {
    let sequence = SequenceBuilder::new("claimed-handler")
        .step("work", "http_request")
        .build();
    storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    storage.create_instance(&instance).await.unwrap();
    let claim = storage
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    StepExecParams {
        claim_token: claim.claimed_at,
        instance_id: instance.id,
        tenant_id: instance.tenant_id,
        block_id: BlockId::new("work"),
        handler_name: "http_request".into(),
        params: json!({"url":"https://provider.example/action"}),
        context: ExecutionContext::default(),
        attempt: 0,
        timeout: None,
        externalize_threshold: 0,
        wait_for_input: None,
        cache_key: None,
        output_schema: None,
    }
}
fn same_request(params: &StepExecParams) -> StepExecParams {
    StepExecParams {
        claim_token: params.claim_token,
        instance_id: params.instance_id,
        tenant_id: params.tenant_id.clone(),
        block_id: params.block_id.clone(),
        handler_name: params.handler_name.clone(),
        params: params.params.clone(),
        context: params.context.clone(),
        attempt: params.attempt,
        timeout: params.timeout,
        externalize_threshold: params.externalize_threshold,
        wait_for_input: params.wait_for_input.clone(),
        cache_key: params.cache_key.clone(),
        output_schema: params.output_schema.clone(),
    }
}

fn counted(calls: Arc<AtomicUsize>) -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();
    registry.register("http_request", move |_| {
        // Deliberately count in the factory, before its future is returned.
        calls.fetch_add(1, Ordering::SeqCst);
        async { Ok(json!({"done":true})) }
    });
    registry
}
backend_test!(async fn stale_claim_cannot_construct_an_ordinary_handler(b) {
    let params=running(&b.storage).await;
    b.storage.update_instance_state(params.instance_id,InstanceState::Waiting,None).await.unwrap();
    let calls=Arc::new(AtomicUsize::new(0));let registry=counted(calls.clone());
    let result=execute_step_dry(&b.storage,&registry,params).await;
    assert_eq!(calls.load(Ordering::SeqCst),0,"a stale claim must not even enter the handler factory");
    assert!(matches!(result,Err(EngineError::ClaimLost{..})));
});
backend_test!(async fn tenant_mismatch_cannot_create_a_foreign_effect_scope(b) {
    let mut params=running(&b.storage).await;
    let id=params.instance_id;params.tenant_id=TenantId::unchecked("foreign");
    let calls=Arc::new(AtomicUsize::new(0));let registry=counted(calls.clone());
    let result=execute_step_dry(&b.storage,&registry,params).await;
    assert_eq!(calls.load(Ordering::SeqCst),0);
    assert!(matches!(result,Err(EngineError::ClaimLost{..})));
    assert!(b.storage.get_continuity_execution_by_instance(&TenantId::unchecked("foreign"),id).await.unwrap().is_none(),"refused entry must not create a foreign receipt namespace");
});
backend_test!(async fn claim_loss_during_dispatch_wait_prevents_ordinary_handler_entry(b) {
    let params=running(&b.storage).await;let id=params.instance_id;
    let (storage,faults)=FaultStorage::wrap(b.storage.clone());
    let pause=faults.on("list_effect_commit_guards").times(1).pause();
    let calls=Arc::new(AtomicUsize::new(0));let registry=counted(calls.clone());
    let task=tokio::spawn(async move {execute_step_dry(&storage,&registry,params).await});
    tokio::time::timeout(std::time::Duration::from_secs(5),pause.entered()).await.unwrap();
    b.storage.update_instance_state(id,InstanceState::Waiting,None).await.unwrap();
    pause.release();
    let result=task.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst),0,"dispatch-storage waits cannot authorize late factory entry");
    assert!(matches!(result,Err(EngineError::ClaimLost{..})));
});

backend_test!(async fn pure_handler_factory_also_requires_its_claim(b) {
    let mut params=running(&b.storage).await;params.handler_name="noop".into();
    b.storage.update_instance_state(params.instance_id,InstanceState::Waiting,None).await.unwrap();
    let calls=Arc::new(AtomicUsize::new(0));let counter=calls.clone();
    let mut registry=HandlerRegistry::new();
    registry.register("noop",move |_|{counter.fetch_add(1,Ordering::SeqCst);async{Ok(json!({}))}});
    assert!(matches!(execute_step_dry(&b.storage,&registry,params).await,Err(EngineError::ClaimLost{..})));
    assert_eq!(calls.load(Ordering::SeqCst),0);
});
backend_test!(async fn initial_claim_read_failure_has_no_effect_and_can_retry(b) {
    let params=running(&b.storage).await;let id=params.instance_id;let tenant=params.tenant_id.clone();
    let retry=same_request(&params);
    let (storage,faults)=FaultStorage::wrap(b.storage.clone());
    faults.on("verify_instance_claim").times(1).fail_transient();
    let calls=Arc::new(AtomicUsize::new(0));let registry=counted(calls.clone());
    assert!(matches!(execute_step_dry(&storage,&registry,params).await,Err(EngineError::Storage(_))));
    assert_eq!(calls.load(Ordering::SeqCst),0);
    assert!(b.storage.get_continuity_execution_by_instance(&tenant,id).await.unwrap().is_none());
    // Retry exactly the original request, without changing owner or instance.
    execute_step_dry(&storage,&registry,retry).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst),1);
});
