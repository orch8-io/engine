//! Prepared native work resumes safely on every native storage backend.
use chrono::Utc;
use orch8_engine::{
    error::EngineError,
    handlers::{
        HandlerRegistry,
        step::{StepExecParams, execute_step_dry},
    },
};
use orch8_storage::{
    backend_test,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    context::ExecutionContext, continuity::EffectState, error::StepError, ids::BlockId,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

backend_test!(async fn deferred_native_preparation_survives_adapter_replacement(b) {
    let seq=SequenceBuilder::new("native-effect").step("work","native").build();
    b.storage.create_sequence(&seq).await.unwrap();
    let instance=InstanceBuilder::for_sequence(&seq).due_now().build();
    b.storage.create_instance(&instance).await.unwrap();
    let claim=b.storage.claim_due_instances(Utc::now()+chrono::Duration::seconds(1),1,0).await.unwrap().pop().unwrap();
    let exec=|| StepExecParams {
        claim_token:claim.claimed_at,instance_id:instance.id,tenant_id:instance.tenant_id.clone(),
        block_id:BlockId::new("work"),handler_name:"native".into(),params:serde_json::json!({}),
        context:ExecutionContext::default(),attempt:0,timeout:None,externalize_threshold:0,
        wait_for_input:None,cache_key:None,output_schema:None,
    };
    let mut first=HandlerRegistry::new();
    first.register_with_deferred_effect_admission("native",|_ctx|async{Err(StepError::ClaimLost)});
    assert!(matches!(execute_step_dry(&b.storage,&first,exec()).await,Err(EngineError::ClaimLost{..})));
    drop(first);
    let ledger=b.storage.get_continuity_execution_by_instance(&instance.tenant_id,instance.id).await.unwrap().unwrap();
    assert_eq!(b.storage.list_effect_receipts(&instance.tenant_id,ledger.continuity_id,10).await.unwrap()[0].state,EffectState::Prepared);
    let calls=Arc::new(AtomicUsize::new(0));let counter=calls.clone();
    let mut replacement=HandlerRegistry::new();
    replacement.register_with_deferred_effect_admission("native",move|ctx|{
        let counter=counter.clone();async move{
            if !ctx.storage.verify_instance_claim(ctx.instance_id,&ctx.tenant_id,ctx.claim_token.unwrap()).await.unwrap(){return Err(StepError::ClaimLost);}
            ctx.effect_admission.as_ref().unwrap().admit().await?;
            counter.fetch_add(1,Ordering::SeqCst);Ok(serde_json::json!({"receipt_id":"native-provider"}))
        }
    });
    execute_step_dry(&b.storage,&replacement,exec()).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst),1);
    let receipts=b.storage.list_effect_receipts(&instance.tenant_id,ledger.continuity_id,10).await.unwrap();
    assert_eq!(receipts.len(),1);assert_eq!(receipts[0].state,EffectState::Committed);
    assert_eq!(receipts[0].provider_receipt_id.as_deref(),Some("native-provider"));
});
