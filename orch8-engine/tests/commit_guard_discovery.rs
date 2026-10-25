//! Unrelated invariant history cannot hide an explicit dispatch guard.
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
    testing::{
        backends::TestBackend,
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    context::ExecutionContext,
    continuity::EffectKind,
    continuity_advanced::{InvariantId, InvariantRule, WorkflowInvariant},
    error::StepError,
    ids::BlockId,
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

async fn unrelated_history(b: &TestBackend, invariant: &WorkflowInvariant) {
    // One set-based fixture transaction avoids 10,000 independent commits.
    if b.kind.is_postgres() {
        sqlx::query("INSERT INTO workflow_invariants (id,tenant_id,sequence_id,sequence_version,enabled,record,created_at)
            SELECT ('00000000-0000-0000-0000-' || lpad(to_hex(n),12,'0'))::uuid,$1,$2,$3,true,
              $4::jsonb || jsonb_build_object('id','00000000-0000-0000-0000-' || lpad(to_hex(n),12,'0')),$5
            FROM generate_series(1,10000) n")
            .bind(invariant.tenant_id.as_str()).bind(invariant.sequence_id.into_uuid()).bind(invariant.sequence_version)
            .bind(serde_json::to_value(invariant).unwrap()).bind(invariant.created_at)
            .execute(b.pg_pool()).await.unwrap();
    } else {
        sqlx::query("WITH RECURSIVE rows(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM rows WHERE n<10000)
            INSERT INTO workflow_invariants (id,tenant_id,sequence_id,sequence_version,enabled,record,created_at)
            SELECT printf('00000000-0000-0000-0000-%012d',n),?1,?2,?3,1,
              json_set(?4,'$.id',printf('00000000-0000-0000-0000-%012d',n)),?5 FROM rows")
            .bind(invariant.tenant_id.as_str()).bind(invariant.sequence_id.to_string()).bind(invariant.sequence_version)
            .bind(serde_json::to_string(invariant).unwrap()).bind(invariant.created_at.to_rfc3339())
            .execute(b.sqlite_pool()).await.unwrap();
    }
}
backend_test!(async fn unrelated_invariants_cannot_hide_a_later_commit_guard(b) {
    let sequence=SequenceBuilder::new("guard-discovery").step("work","http_request").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let instance=InstanceBuilder::for_sequence(&sequence).due_now().build();
    b.storage.create_instance(&instance).await.unwrap();
    let token=b.storage.claim_due_instances(Utc::now()+chrono::Duration::seconds(1),1,0).await.unwrap().pop().unwrap().claimed_at;
    let mut invariant=WorkflowInvariant {id:InvariantId::new(),tenant_id:sequence.tenant_id.clone(),sequence_id:sequence.id,sequence_version:Some(sequence.version),name:"observe".into(),rule:InvariantRule::NoUnknownEffects,commit_guard:false,enabled:true,created_at:Utc::now()-chrono::Duration::days(1)};
    unrelated_history(&b,&invariant).await;
    invariant.id=InvariantId::new();invariant.name="do not repeat".into();invariant.rule=InvariantRule::EffectAtMostOnce{kind:EffectKind::Http};invariant.commit_guard=true;invariant.created_at=Utc::now();
    b.storage.create_workflow_invariant(&invariant).await.unwrap();
    assert_eq!(b.storage.list_workflow_invariants(&sequence.tenant_id,sequence.id,sequence.version,10000).await.unwrap().len(),10000);
    let calls=Arc::new(AtomicUsize::new(0));let count=calls.clone();let mut registry=HandlerRegistry::new();
    registry.register("http_request",move |_| {let count=count.clone();async move {count.fetch_add(1,Ordering::SeqCst);Err(StepError::Retryable{message:"ambiguous provider result".into(),details:None})}});
    let exec=|attempt|StepExecParams{claim_token:token,instance_id:instance.id,tenant_id:instance.tenant_id.clone(),block_id:BlockId::new("work"),handler_name:"http_request".into(),params:json!({"url":"https://provider.example/action","body":{"operation":"same"}}),context:ExecutionContext::default(),attempt,timeout:None,externalize_threshold:0,wait_for_input:None,cache_key:None,output_schema:None};
    assert!(execute_step_dry(&b.storage,&registry,exec(0)).await.is_err());
    let retry=execute_step_dry(&b.storage,&registry,exec(1)).await;
    assert!(matches!(retry,Err(EngineError::InvariantViolation{invariant_id,..}) if invariant_id==invariant.id),"an explicit guard cannot be bypassed by unrelated history");
    assert_eq!(calls.load(Ordering::SeqCst),1);
});

backend_test!(async fn guard_lookup_filters_scope_policy_and_kind_before_its_limit(b) {
    let sequence=SequenceBuilder::new("guard-selection").step("work","http_request").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let other=SequenceBuilder::new("other-tenant").step("work","http_request").build();
    b.storage.create_sequence(&other).await.unwrap();
    let base=WorkflowInvariant{id:InvariantId::new(),tenant_id:sequence.tenant_id.clone(),sequence_id:sequence.id,sequence_version:Some(sequence.version),name:"guard".into(),rule:InvariantRule::EffectAtMostOnce{kind:EffectKind::Http},commit_guard:true,enabled:true,created_at:Utc::now()-chrono::Duration::days(1)};
    for index in 0..6 {
        let mut ignored=base.clone();ignored.id=InvariantId::new();
        match index {
            0=>ignored.enabled=false,1=>ignored.commit_guard=false,
            2=>ignored.rule=InvariantRule::NoUnknownEffects,
            3=>ignored.rule=InvariantRule::EffectAtMostOnce{kind:EffectKind::Model},
            4=>ignored.sequence_version=Some(sequence.version+1),
            _=>{ignored.sequence_id=other.id;ignored.tenant_id=other.tenant_id.clone();}
        }
        b.storage.create_workflow_invariant(&ignored).await.unwrap();
    }
    let mut global=base.clone();global.sequence_version=None;global.created_at=Utc::now();
    b.storage.create_workflow_invariant(&global).await.unwrap();
    let mut exact=base.clone();exact.id=InvariantId::new();exact.created_at=global.created_at+chrono::Duration::seconds(1);
    b.storage.create_workflow_invariant(&exact).await.unwrap();
    let selected=b.storage.list_effect_commit_guards(&sequence.tenant_id,sequence.id,sequence.version,EffectKind::Http,1).await.unwrap();
    assert_eq!(selected.len(),1);assert_eq!(selected[0].id,global.id);
    let selected=b.storage.list_effect_commit_guards(&sequence.tenant_id,sequence.id,sequence.version,EffectKind::Http,10).await.unwrap();
    assert_eq!(selected.iter().map(|item|item.id).collect::<Vec<_>>(),vec![global.id,exact.id]);
    assert!(b.storage.list_effect_commit_guards(&sequence.tenant_id,other.id,sequence.version,EffectKind::Http,10).await.unwrap().is_empty());
});

backend_test!(async fn ordinary_retry_policy_remains_per_attempt_without_a_commit_guard(b) {
    let sequence=SequenceBuilder::new("ordinary-retry").step("work","http_request").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let instance=InstanceBuilder::for_sequence(&sequence).due_now().build();
    b.storage.create_instance(&instance).await.unwrap();
    let token=b.storage.claim_due_instances(Utc::now()+chrono::Duration::seconds(1),1,0).await.unwrap().pop().unwrap().claimed_at;
    let calls=Arc::new(AtomicUsize::new(0));let count=calls.clone();let mut registry=HandlerRegistry::new();
    registry.register("http_request",move |_|{let count=count.clone();async move{count.fetch_add(1,Ordering::SeqCst);Err(StepError::Retryable{message:"retry requested".into(),details:None})}});
    for attempt in [0,1] {
        let exec=StepExecParams{claim_token:token,instance_id:instance.id,tenant_id:instance.tenant_id.clone(),block_id:BlockId::new("work"),handler_name:"http_request".into(),params:json!({"url":"https://provider.example/action"}),context:ExecutionContext::default(),attempt,timeout:None,externalize_threshold:0,wait_for_input:None,cache_key:None,output_schema:None};
        assert!(execute_step_dry(&b.storage,&registry,exec).await.is_err());
    }
    assert_eq!(calls.load(Ordering::SeqCst),2,"existing per-attempt retry semantics are explicit; cross-attempt exclusion is a commit-guard policy");
});

backend_test!(async fn unavailable_commit_policy_cannot_be_treated_as_no_guard(b) {
    use orch8_storage::testing::faulty::FaultStorage;
    use orch8_types::continuity::EffectState;
    let sequence=SequenceBuilder::new("guard-unavailable").step("work","http_request").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let instance=InstanceBuilder::for_sequence(&sequence).due_now().build();
    b.storage.create_instance(&instance).await.unwrap();
    let token=b.storage.claim_due_instances(Utc::now()+chrono::Duration::seconds(1),1,0).await.unwrap().pop().unwrap().claimed_at;
    let (storage,faults)=FaultStorage::wrap(b.storage.clone());
    faults.on("list_effect_commit_guards").times(1).fail_transient();
    let calls=Arc::new(AtomicUsize::new(0));let count=calls.clone();let mut registry=HandlerRegistry::new();
    registry.register("http_request",move |_|{let count=count.clone();async move{count.fetch_add(1,Ordering::SeqCst);Ok(json!({"done":true}))}});
    let exec=||StepExecParams{claim_token:token,instance_id:instance.id,tenant_id:instance.tenant_id.clone(),block_id:BlockId::new("work"),handler_name:"http_request".into(),params:json!({"url":"https://provider.example/action"}),context:ExecutionContext::default(),attempt:0,timeout:None,externalize_threshold:0,wait_for_input:None,cache_key:None,output_schema:None};
    assert!(execute_step_dry(&storage,&registry,exec()).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst),0);
    assert_eq!(faults.calls("list_effect_commit_guards"),1);
    let scope=b.storage.get_continuity_execution_by_instance(&instance.tenant_id,instance.id).await.unwrap().unwrap();
    assert_eq!(b.storage.list_effect_receipts(&instance.tenant_id,scope.continuity_id,10).await.unwrap()[0].state,EffectState::Prepared);
    execute_step_dry(&storage,&registry,exec()).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst),1);
});
