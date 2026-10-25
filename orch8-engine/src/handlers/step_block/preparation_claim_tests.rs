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
        faults.calls("get_credential"),
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
        faults.calls("get_credential"),
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
        faults.calls("get_credential"),
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
    let pause=faults.on("get_credential").times(1).pause();
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
    assert!(matches!(result,Err(crate::credentials::ClaimedResolutionError::Ownership(EngineError::ClaimLost{..}))));
    assert_eq!(faults.calls("get_credential"),1);assert_eq!(value,original);
});

orch8_storage::backend_test!(async fn failed_ownership_read_stays_infrastructural_and_can_recover(b) {
    let (storage,instance,_)=source_on(b.storage.clone()).await;
    credential(storage.as_ref(),&instance.tenant_id).await;
    let (guarded,faults)=FaultStorage::wrap(storage);
    faults.on("verify_instance_claim").times(1).fail_transient();
    let claim=crate::control::ExecutionClaim{instance_id:instance.id,token:instance.claimed_at.unwrap()};
    let mut value=serde_json::json!({"a":"credentials://one"});let original=value.clone();
    assert!(matches!(crate::credentials::resolve_claimed_in_value(guarded.as_ref(),&instance.tenant_id,claim,&mut value).await,Err(crate::credentials::ClaimedResolutionError::Ownership(EngineError::Storage(_)))));
    assert_eq!(value,original);assert_eq!(faults.calls("get_credential"),0);
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
    assert_eq!(faults.calls("get_credential"),0);
});
