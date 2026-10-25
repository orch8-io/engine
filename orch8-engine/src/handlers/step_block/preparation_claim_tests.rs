use super::*;
use orch8_storage::{
    sqlite::SqliteStorage,
    testing::{
        faulty::FaultStorage,
        fixtures::{InstanceBuilder, SequenceBuilder, step_with_params},
    },
};
use orch8_types::instance::InstanceState;

async fn source() -> (Arc<dyn StorageBackend>, TaskInstance, StepDef) {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    source_on(storage).await
}

async fn source_on(
    storage: Arc<dyn StorageBackend>,
) -> (Arc<dyn StorageBackend>, TaskInstance, StepDef) {
    let seq = SequenceBuilder::new("preparation")
        .step("work", "http_request")
        .build();
    storage.create_sequence(&seq).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&seq)
        .data(serde_json::json!({"value":"ready"}))
        .due_now()
        .build();
    storage.create_instance(&instance).await.unwrap();
    let claimed = storage
        .claim_due_instances(chrono::Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let step = step_with_params(
        "work",
        "http_request",
        serde_json::json!({"credential":"credentials://missing"}),
    );
    (storage, claimed, step)
}
#[tokio::test]
async fn lost_claim_never_begins_credential_preparation() {
    let (storage, instance, step) = source().await;
    storage
        .update_instance_state(instance.id, InstanceState::Waiting, None)
        .await
        .unwrap();
    let (guarded, faults) = FaultStorage::wrap(storage);
    let result = prepare_step(
        guarded.as_ref(),
        &instance,
        &step,
        &OutputsSnapshot::new(),
        false,
        &crate::embed_sandbox::SandboxPolicy::default(),
        instance.claimed_at.unwrap(),
    )
    .await;
    assert_eq!(
        faults.calls("get_credential_for_claim"),
        0,
        "stale preparation must not ask for a secret"
    );
    assert!(matches!(
        result,
        Err(PrepareError::Infra(EngineError::ClaimLost { .. }))
    ));
}
#[tokio::test]
async fn claim_loss_while_loading_templates_prevents_credentials() {
    let (storage, instance, mut step) = source().await;
    step.params["input"] = serde_json::json!("{{context.data.value}}");
    let (guarded, faults) = FaultStorage::wrap(storage.clone());
    let pause = faults.on("get_all_outputs").times(1).pause();
    let id = instance.id;
    let task = tokio::spawn(async move {
        prepare_step(
            guarded.as_ref(),
            &instance,
            &step,
            &OutputsSnapshot::new(),
            false,
            &crate::embed_sandbox::SandboxPolicy::default(),
            instance.claimed_at.unwrap(),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered())
        .await
        .unwrap();
    storage
        .update_instance_state(id, InstanceState::Waiting, None)
        .await
        .unwrap();
    pause.release();
    let result = task.await.unwrap();
    assert_eq!(
        faults.calls("get_credential_for_claim"),
        0,
        "template I/O cannot authorize a later secret lookup"
    );
    assert!(matches!(
        result,
        Err(PrepareError::Infra(EngineError::ClaimLost { .. }))
    ));
}
#[tokio::test]
async fn deferred_credential_fallback_retains_the_original_claim() {
    let (storage, instance, _) = source().await;
    storage
        .update_instance_state(instance.id, InstanceState::Waiting, None)
        .await
        .unwrap();
    let (guarded, faults) = FaultStorage::wrap(storage);
    let mut prepared = PreparedStep {
        step_context: instance.context.clone(),
        resolved_params: serde_json::json!({"credential":"credentials://missing"}),
        resolved_cache_key: None,
        carries_credentials: true,
        credentials_deferred: true,
    };
    let result = prepared
        .resolve_deferred_credentials(guarded.as_ref(), &instance, instance.claimed_at.unwrap())
        .await;
    assert_eq!(
        faults.calls("get_credential_for_claim"),
        0,
        "fallback credential resolution cannot use a retired evaluation"
    );
    assert!(matches!(
        result,
        Err(PrepareError::Infra(EngineError::ClaimLost { .. }))
    ));
}

async fn credential(storage: &dyn StorageBackend, tenant: &orch8_types::ids::TenantId) {
    let now = chrono::Utc::now();
    let value: orch8_types::credential::CredentialDef = serde_json::from_value(serde_json::json!({
        "id":"one","name":"one","tenant_id":tenant.as_str(),"value":"CLAIMED_CREDENTIAL_CANARY",
        "created_at":now,"updated_at":now
    }))
    .unwrap();
    storage.create_credential(&value).await.unwrap();
}

orch8_storage::backend_test!(async fn claimed_credentials_commit_only_a_complete_resolution(b) {
    let (storage,instance,_)=source_on(b.storage.clone()).await;
    credential(storage.as_ref(),&instance.tenant_id).await;
    let claim=crate::control::ExecutionClaim{instance_id:instance.id,token:instance.claimed_at.unwrap()};
    let mut value=serde_json::json!({"nested":["credentials://one"],"plain":"value"});
    crate::credentials::resolve_claimed_in_value(storage.as_ref(),&instance.tenant_id,claim,&mut value).await.unwrap();
    assert_eq!(value["nested"][0],"CLAIMED_CREDENTIAL_CANARY");
    let mut value=serde_json::json!({"a":"credentials://one","b":"credentials://missing"});
    let original=value.clone();
    assert!(matches!(crate::credentials::resolve_claimed_in_value(storage.as_ref(),&instance.tenant_id,claim,&mut value).await,Err(crate::credentials::ClaimedResolutionError::Credential(_))));
    assert_eq!(value,original,"failed resolution must not leave partially expanded secret material");
});

orch8_storage::backend_test!(async fn claim_loss_inside_credential_lookup_stops_later_references(b) {
    let (storage,instance,_)=source_on(b.storage.clone()).await;
    credential(storage.as_ref(),&instance.tenant_id).await;
    let (guarded,faults)=FaultStorage::wrap(storage.clone());
    let pause=faults.on("get_credential_for_claim").times(1).pause();
    let claim=crate::control::ExecutionClaim{instance_id:instance.id,token:instance.claimed_at.unwrap()};
    let original=serde_json::json!({"a":"credentials://one","b":"credentials://one"});
    let mut value=original.clone();let tenant=instance.tenant_id.clone();
    let task=tokio::spawn(async move {
        let result=crate::credentials::resolve_claimed_in_value(guarded.as_ref(),&tenant,claim,&mut value).await;
        (result,value)
    });
    tokio::time::timeout(std::time::Duration::from_secs(5),pause.entered()).await.unwrap();
    storage.update_instance_state(instance.id,InstanceState::Waiting,None).await.unwrap();
    pause.release();
    let (result,value)=task.await.unwrap();
    assert!(matches!(result,Err(crate::credentials::ClaimedResolutionError::Infrastructure(EngineError::ClaimLost{..}))));
    assert_eq!(faults.calls("get_credential_for_claim"),1);assert_eq!(value,original);
});

orch8_storage::backend_test!(async fn failed_ownership_read_stays_infrastructural_and_can_recover(b) {
    let (storage,instance,_)=source_on(b.storage.clone()).await;
    credential(storage.as_ref(),&instance.tenant_id).await;
    let (guarded,faults)=FaultStorage::wrap(storage);
    faults.on("verify_instance_claim").times(1).fail_transient();
    let claim=crate::control::ExecutionClaim{instance_id:instance.id,token:instance.claimed_at.unwrap()};
    let mut value=serde_json::json!({"a":"credentials://one"});let original=value.clone();
    assert!(matches!(crate::credentials::resolve_claimed_in_value(guarded.as_ref(),&instance.tenant_id,claim,&mut value).await,Err(crate::credentials::ClaimedResolutionError::Infrastructure(EngineError::Storage(_)))));
    assert_eq!(value,original);assert_eq!(faults.calls("get_credential_for_claim"),0);
    crate::credentials::resolve_claimed_in_value(guarded.as_ref(),&instance.tenant_id,claim,&mut value).await.unwrap();
    assert_eq!(value["a"],"CLAIMED_CREDENTIAL_CANARY");
});

orch8_storage::backend_test!(async fn refreshed_instance_cannot_replace_the_preparing_evaluations_nonce(b) {
    let (storage,instance,step)=source_on(b.storage.clone()).await;
    let original=instance.claimed_at.unwrap();
    storage.update_instance_state(instance.id,InstanceState::Scheduled,Some(chrono::Utc::now())).await.unwrap();
    let fresh=storage.claim_due_instances(chrono::Utc::now()+chrono::Duration::seconds(1),1,0).await.unwrap().pop().unwrap();
    assert_ne!(fresh.claimed_at,Some(original));
    let (guarded,faults)=FaultStorage::wrap(storage);
    let result=prepare_step(guarded.as_ref(),&fresh,&step,&OutputsSnapshot::new(),false,&crate::embed_sandbox::SandboxPolicy::default(),original).await;
    assert!(matches!(result,Err(PrepareError::Infra(EngineError::ClaimLost{..}))));
    assert_eq!(faults.calls("get_credential_for_claim"),0);
});

async fn credential_storage_outage_recovers(storage: Arc<dyn StorageBackend>, tree: bool) {
    use crate::testing::TestEngine;
    use orch8_types::sequence::{BlockDefinition, ParallelDef};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (guarded, faults) = FaultStorage::wrap(storage.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::clone(&calls);
    let engine = TestEngine::builder()
        .storage(guarded)
        .register("noop", move |ctx| {
            let calls = Arc::clone(&entered);
            async move {
                assert_eq!(ctx.params["credential"], "CLAIMED_CREDENTIAL_CANARY");
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(serde_json::json!({"done":true}))
            }
        })
        .build()
        .await;
    let work = step_with_params(
        "work",
        "noop",
        serde_json::json!({"credential":"credentials://one"}),
    );
    let block = BlockDefinition::Step(Box::new(work));
    let blocks = if tree {
        vec![BlockDefinition::Parallel(Box::new(ParallelDef {
            id: orch8_types::ids::BlockId::new("root"),
            branches: vec![vec![block]],
        }))]
    } else {
        vec![block]
    };
    let sequence = SequenceBuilder::new("credential-recovery")
        .blocks(blocks)
        .build();
    let id = engine.start(&sequence).await;
    let instance = storage.get_instance(id).await.unwrap().unwrap();
    credential(storage.as_ref(), &instance.tenant_id).await;
    faults
        .on("get_credential_for_claim")
        .times(1)
        .fail_transient();
    engine.tick().await;
    assert_eq!(faults.injected("get_credential_for_claim"), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        storage.get_instance(id).await.unwrap().unwrap().state,
        InstanceState::Scheduled,
        "a credential storage outage must preserve the workflow for infrastructure recovery"
    );
    assert!(
        storage
            .get_all_outputs(id)
            .await
            .unwrap()
            .iter()
            .all(|o| o.output_ref.as_deref() != Some("__error__"))
    );
    assert!(
        storage
            .get_execution_tree(id)
            .await
            .unwrap()
            .iter()
            .all(|n| n.state != orch8_types::execution::NodeState::Failed)
    );
    engine.advance(std::time::Duration::from_secs(61));
    engine.tick().await;
    assert_eq!(
        storage.get_instance(id).await.unwrap().unwrap().state,
        InstanceState::Completed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

orch8_storage::backend_test!(async fn flat_credential_storage_outage_recovers_without_handler_retry_policy(b) {
    credential_storage_outage_recovers(b.storage.clone(), false).await;
});
orch8_storage::backend_test!(async fn tree_credential_storage_outage_recovers_without_handler_retry_policy(b) {
    credential_storage_outage_recovers(b.storage.clone(), true).await;
});

orch8_storage::backend_test!(async fn deferred_credential_lookup_preserves_storage_class_and_can_recover(b) {
    let (storage,instance,_)=source_on(b.storage.clone()).await;
    credential(storage.as_ref(),&instance.tenant_id).await;
    let (guarded,faults)=FaultStorage::wrap(storage);
    let original=serde_json::json!({"credential":"credentials://one"});
    let mut prepared=PreparedStep {
        step_context:instance.context.clone(), resolved_params:original.clone(),
        resolved_cache_key:None,carries_credentials:true,credentials_deferred:true,
    };
    faults.on("get_credential_for_claim").times(1).fail_transient();
    let error=prepared.resolve_deferred_credentials(guarded.as_ref(),&instance,instance.claimed_at.unwrap()).await;
    assert!(matches!(error,Err(PrepareError::Infra(ref error)) if error.is_transient_storage()));
    assert_eq!(prepared.resolved_params,original);assert!(prepared.credentials_deferred);
    prepared.resolve_deferred_credentials(guarded.as_ref(),&instance,instance.claimed_at.unwrap()).await.unwrap_or_else(|_|panic!("transient lookup should recover"));
    assert_eq!(prepared.resolved_params["credential"],"CLAIMED_CREDENTIAL_CANARY");
    assert!(!prepared.credentials_deferred);
    prepared.credentials_deferred=true;prepared.resolved_params=original.clone();
    faults.on("get_credential_for_claim").times(1).fail_permanent();
    let error=prepared.resolve_deferred_credentials(guarded.as_ref(),&instance,instance.claimed_at.unwrap()).await;
    assert!(matches!(error,Err(PrepareError::Infra(EngineError::Storage(ref error))) if !error.is_transient()));
    assert_eq!(prepared.resolved_params,original);assert!(prepared.credentials_deferred);
    prepared.resolved_params=serde_json::json!({"credential":"credentials://missing"});
    let error=prepared.resolve_deferred_credentials(guarded.as_ref(),&instance,instance.claimed_at.unwrap()).await;
    assert!(matches!(error,Err(PrepareError::Credential(orch8_types::error::StepError::Permanent{..}))));
});
