//! Explicit operator recovery must retain a decision without inventing an issuer.
use chrono::Utc;
use orch8_api::test_harness::spawn_test_server_on;
use orch8_storage::{
    StorageBackend,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
    worker_admission::{WorkerDispatchIntent, WorkerDispatchPublication},
    worker_invocation::WorkerDispatchSelection,
};
use orch8_types::{
    ids::BlockId,
    instance::InstanceState,
    worker::{WorkerClaim, WorkerTask},
};
use serde_json::json;
use uuid::Uuid;
const ROOT: &str = "root-worker-reconciliation-tests";

async fn legacy_claim(s: &dyn StorageBackend) -> WorkerTask {
    claimed_task(s, None).await
}
async fn claimed_task(
    s: &dyn StorageBackend,
    principal: Option<orch8_storage::worker_claim::WorkerClaimPrincipal>,
) -> WorkerTask {
    dispatch_claim(s, principal, false).await
}
async fn dispatch_claim(
    s: &dyn StorageBackend,
    principal: Option<orch8_storage::worker_claim::WorkerClaimPrincipal>,
    with_effect: bool,
) -> WorkerTask {
    let handler = if with_effect { "provider.work" } else { "noop" };
    let seq = SequenceBuilder::new("reconciliation")
        .step("work", handler)
        .build();
    s.create_sequence(&seq).await.unwrap();
    let mut parent = InstanceBuilder::for_sequence(&seq).due_now().build();
    parent.context.runtime.run_id = Some(Uuid::now_v7().to_string());
    parent.context.runtime.current_step = Some(BlockId::new("work"));
    parent.context.runtime.current_step_started_at = Some(Utc::now());
    s.create_instance(&parent).await.unwrap();
    let task:WorkerTask=serde_json::from_value(json!({"id":Uuid::now_v7(),"instance_id":parent.id,"block_id":"work","handler_name":handler,"params":{},"context":parent.context,"attempt":0,"state":"pending","created_at":Utc::now()})).unwrap();
    let claimed = s
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let fence = s
        .prepare_worker_dispatch(
            &claimed,
            &task.block_id,
            &WorkerDispatchSelection::Flat {
                step_started_at: claimed.context.runtime.current_step_started_at.unwrap(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    let effect = with_effect.then(|| effect_plan(&task, &fence));
    assert!(matches!(
        s.admit_worker_dispatch(&WorkerDispatchIntent::new(task, fence, effect).unwrap())
            .await
            .unwrap(),
        WorkerDispatchPublication::Published { .. }
    ));
    s.update_instance_state(parent.id, InstanceState::Waiting, None)
        .await
        .unwrap();
    if let Some(principal) = principal {
        s.claim_worker_tasks_with_authority(
            &orch8_storage::worker_claim::WorkerClaimRequest::new(
                principal,
                handler.into(),
                "legacy".into(),
                None,
                None,
                None,
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
    } else {
        s.claim_worker_tasks(handler, "legacy", 1)
            .await
            .unwrap()
            .pop()
            .unwrap()
    }
}

orch8_storage::backend_test!(async fn operator_reconciles_a_legacy_completion_without_rewriting_issuance(b) {
    let task=legacy_claim(b.storage.as_ref()).await;let claim=WorkerClaim::new("legacy",task.claim_epoch);
    let server=spawn_test_server_on(b.storage.clone(),Some(ROOT)).await;let client=reqwest::Client::new();
    let url=format!("{}/workers/tasks/{}/reconcile-completion",server.v1_url(),task.id);
    let body=json!({"worker_id":"legacy","claim_epoch":task.claim_epoch,"approval_id":Uuid::now_v7(),"evidence_reference":"backend-ack-123","output":{"recovered":true}});
    let response=client.post(&url).header("x-api-key",ROOT).json(&body).send().await.unwrap();
    assert_eq!(response.status(),reqwest::StatusCode::ACCEPTED,"{}",response.text().await.unwrap());
    let accepted=b.storage.get_worker_accepted_outcome(None,task.id,&claim).await.unwrap().unwrap();
    assert_eq!(accepted.output,json!({"recovered":true}));
    let record=serde_json::to_value(&accepted).unwrap();
    assert_eq!(record["reconciliation"]["approval"]["approval_id"],body["approval_id"]);
    assert_eq!(record["reconciliation"]["approval"]["actor"]["kind"],"root");
    assert_eq!(record["reconciliation"]["history"]["kind"],"legacy");
    let issued=b.storage.get_worker_issued_claim(None,task.id,&claim).await.unwrap().unwrap();
    assert_eq!(issued.principal,orch8_storage::worker_claim::WorkerClaimPrincipal::Legacy{});
    let response=client.post(&url).header("x-api-key",ROOT).json(&body).send().await.unwrap();
    assert!(response.status().is_success(),"{}",response.text().await.unwrap());
    let tenant=orch8_types::ids::TenantId::unchecked("reconciliation");
    assert_eq!(orch8_storage::worker_outcome::integrate_worker_outcome(b.storage.as_ref(),&tenant,task.id,&claim,1_048_576).await.unwrap(),orch8_storage::worker_outcome::WorkerOutcomeApplication::Applied);
    assert_eq!(b.storage.get_instance(task.instance_id).await.unwrap().unwrap().context.data["recovered"],true);
    b.storage.delete_worker_task(task.id).await.unwrap();
    b.storage.update_instance_state(task.instance_id,InstanceState::Completed,None).await.unwrap();
    assert_eq!(b.storage.purge_terminal_instances(&[task.instance_id]).await.unwrap(),1);

    let response=client.post(&url).header("x-api-key",ROOT).json(&body).send().await.unwrap();
    assert!(response.status().is_success(),"{}",response.text().await.unwrap());
    for field in ["approval_id","evidence_reference"] {
        let mut changed=body.clone();changed[field]=if field=="approval_id" {json!(Uuid::now_v7())} else {json!("another-ack")};
        let response=client.post(&url).header("x-api-key",ROOT).json(&changed).send().await.unwrap();assert_eq!(response.status(),reqwest::StatusCode::CONFLICT);
    }
    let mut changed=body.clone();changed["output"]=json!({"recovered":false});
    let response=client.post(&url).header("x-api-key",ROOT).json(&changed).send().await.unwrap();
    assert_eq!(response.status(),reqwest::StatusCode::CONFLICT);
    assert_eq!(b.storage.get_worker_accepted_outcome(None,task.id,&claim).await.unwrap().unwrap(),accepted);
});

#[tokio::test]
async fn postgres_operator_expiry_is_checked_after_waiting_for_its_key_lock() {
    use orch8_storage::{
        testing::backends::TestBackend, worker_claim::WorkerClaimPrincipal,
        worker_outcome::WorkerCompletion, worker_reconciliation::WorkerReconciliationApproval,
    };
    use orch8_types::{
        api_key::{ApiCapability, mint_scoped},
        ids::TenantId,
    };
    let Some(b) = TestBackend::postgres().await else {
        return;
    };
    let task = legacy_claim(b.storage.as_ref()).await;
    let claim = WorkerClaim::new("legacy", task.claim_epoch);
    let tenant = TenantId::unchecked("reconciliation");
    let now: chrono::DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(b.pg_pool())
        .await
        .unwrap();
    let expires = now + chrono::Duration::seconds(3);
    let key = mint_scoped(
        tenant.as_str(),
        "expiring operator",
        Some(expires),
        vec![ApiCapability::Operator],
    );
    b.storage.create_api_key(&key.record).await.unwrap();
    let approval = WorkerReconciliationApproval::new(
        WorkerClaimPrincipal::ApiKey {
            tenant_id: tenant.clone(),
            key_id: key.record.id.clone(),
        },
        tenant.clone(),
        task.id,
        claim.clone(),
        Uuid::now_v7(),
        "backend-ack".into(),
    )
    .unwrap();
    let completion =
        WorkerCompletion::for_legacy_reconciliation(json!({"confirmed":true}), approval).unwrap();
    let mut lock = b.pg_pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM api_keys WHERE id=$1 FOR UPDATE")
        .bind(&key.record.id)
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let storage = b.storage.clone();
    let target = task.id;
    let report_claim = claim.clone();
    let report = tokio::spawn(async move {
        storage
            .accept_worker_completion(target, &report_claim, &completion)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2),async {
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%FROM api_keys%FOR SHARE%')").fetch_one(b.pg_pool()).await.unwrap();
            if waiting{break}tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("operator validation must be waiting on the key lock");
    loop {
        let deadline_passed: bool = sqlx::query_scalar("SELECT clock_timestamp()>=$1")
            .bind(expires)
            .fetch_one(b.pg_pool())
            .await
            .unwrap();
        if deadline_passed {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    lock.rollback().await.unwrap();
    assert!(
        report.await.unwrap().is_err(),
        "an operator key that expired while blocked must not approve a completion"
    );
    assert!(
        b.storage
            .get_worker_accepted_outcome(Some(&tenant), task.id, &claim)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        b.storage
            .get_worker_task(task.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        orch8_types::worker::WorkerTaskState::Claimed
    );
}

fn report(task: &WorkerTask) -> serde_json::Value {
    json!({"worker_id":task.worker_id,"claim_epoch":task.claim_epoch,"approval_id":Uuid::now_v7(),"evidence_reference":"backend-ack-123","output":{"recovered":true}})
}
async fn key(
    s: &dyn StorageBackend,
    tenant: &str,
    capabilities: Vec<orch8_types::api_key::ApiCapability>,
) -> orch8_types::api_key::NewApiKey {
    let key = orch8_types::api_key::mint_scoped(tenant, "reconciliation key", None, capabilities);
    s.create_api_key(&key.record).await.unwrap();
    key
}

orch8_storage::backend_test!(async fn only_a_same_tenant_operator_can_record_a_decision(b) {
    use orch8_types::api_key::ApiCapability;
    let task=legacy_claim(b.storage.as_ref()).await;
    let server=spawn_test_server_on(b.storage.clone(),Some(ROOT)).await;let client=reqwest::Client::new();
    let url=format!("{}/workers/tasks/{}/reconcile-completion",server.v1_url(),task.id);
    let body=report(&task);
    let worker=key(b.storage.as_ref(),"reconciliation",vec![ApiCapability::Worker]).await;
    let foreign=key(b.storage.as_ref(),"foreign",vec![ApiCapability::Operator]).await;
    for (secret,status) in [(&worker.secret,reqwest::StatusCode::FORBIDDEN),(&foreign.secret,reqwest::StatusCode::NOT_FOUND)] {
        let response=client.post(&url).header("x-api-key",secret).json(&body).send().await.unwrap();
        assert_eq!(response.status(),status,"{}",response.text().await.unwrap());
    }
    let operator=key(b.storage.as_ref(),"reconciliation",vec![ApiCapability::Operator]).await;
    let response=client.post(&url).header("x-api-key",&operator.secret).json(&body).send().await.unwrap();
    assert_eq!(response.status(),reqwest::StatusCode::ACCEPTED,"{}",response.text().await.unwrap());
    let receipt:serde_json::Value=response.json().await.unwrap();
    assert_eq!(receipt["decision"]["approval"]["actor"]["key_id"],operator.record.id);
    assert_eq!(receipt["decision"]["approval"]["actor"]["kind"],"api_key");
    assert!(!receipt.to_string().contains(&operator.secret));
});

orch8_storage::backend_test!(async fn authenticated_claims_cannot_be_reclassified_as_legacy(b) {
    let task=claimed_task(b.storage.as_ref(),Some(orch8_storage::worker_claim::WorkerClaimPrincipal::Root{})).await;
    let server=spawn_test_server_on(b.storage.clone(),Some(ROOT)).await;
    let response=reqwest::Client::new().post(format!("{}/workers/tasks/{}/reconcile-completion",server.v1_url(),task.id))
        .header("x-api-key",ROOT).json(&report(&task)).send().await.unwrap();
    assert_eq!(response.status(),reqwest::StatusCode::CONFLICT,"{}",response.text().await.unwrap());
    assert_eq!(b.storage.get_worker_task(task.id).await.unwrap().unwrap().state,orch8_types::worker::WorkerTaskState::Claimed);
    assert!(b.storage.get_worker_accepted_outcome(None,task.id,&WorkerClaim::new("legacy",task.claim_epoch)).await.unwrap().is_none());
});

orch8_storage::backend_test!(async fn reconciliation_rejects_wire_actor_forgery_and_insecure_approval(b) {
    let task=legacy_claim(b.storage.as_ref()).await;
    let server=spawn_test_server_on(b.storage.clone(),Some(ROOT)).await;let client=reqwest::Client::new();
    let url=format!("{}/workers/tasks/{}/reconcile-completion",server.v1_url(),task.id);
    let mut forged=report(&task);forged["actor"]=json!({"kind":"root"});
    let response=client.post(&url).header("x-api-key",ROOT).json(&forged).send().await.unwrap();assert!(response.status().is_client_error());
    for reference in [" ".to_owned(),"ack\0foreign".to_owned(),"x".repeat(257)] {
        let mut body=report(&task);body["evidence_reference"]=json!(reference);
        let response=client.post(&url).header("x-api-key",ROOT).json(&body).send().await.unwrap();assert_eq!(response.status(),reqwest::StatusCode::BAD_REQUEST);
    }
    let insecure=spawn_test_server_on(b.storage.clone(),None).await;
    let response=client.post(format!("{}/workers/tasks/{}/reconcile-completion",insecure.v1_url(),task.id)).json(&report(&task)).send().await.unwrap();
    assert_eq!(response.status(),reqwest::StatusCode::FORBIDDEN);
    assert_eq!(b.storage.get_worker_task(task.id).await.unwrap().unwrap().state,orch8_types::worker::WorkerTaskState::Claimed);
});

orch8_storage::backend_test!(async fn legacy_epoch_zero_is_retained_without_fabricating_a_positive_issued_claim(b) {
    let mut task=legacy_claim(b.storage.as_ref()).await;b.storage.delete_worker_task(task.id).await.unwrap();
    task.id=Uuid::now_v7();task.claim_epoch=0;
    b.storage.create_worker_task(&task).await.unwrap();
    // PostgreSQL's producer primitive intentionally omits lease identity;
    // simulate an existing pre-epoch writer without using a modern selector.
    if b.kind.is_postgres() {sqlx::query("UPDATE worker_tasks SET worker_id='legacy' WHERE id=$1").bind(task.id).execute(b.pg_pool()).await.unwrap();}
    let server=spawn_test_server_on(b.storage.clone(),Some(ROOT)).await;
    let response=reqwest::Client::new().post(format!("{}/workers/tasks/{}/reconcile-completion",server.v1_url(),task.id))
        .header("x-api-key",ROOT).json(&report(&task)).send().await.unwrap();
    assert_eq!(response.status(),reqwest::StatusCode::ACCEPTED,"{}",response.text().await.unwrap());
    let receipt:serde_json::Value=response.json().await.unwrap();assert_eq!(receipt["decision"]["history"]["kind"],"missing");
    assert_eq!(receipt["decision"]["approval"]["claim"]["claim_epoch"],0);
    let claim=WorkerClaim::new("legacy",0);
    assert!(b.storage.get_worker_issued_claim(None,task.id,&claim).await.unwrap().is_none());
    assert!(b.storage.get_worker_accepted_outcome(None,task.id,&claim).await.unwrap().unwrap().reconciliation.is_some());
});

orch8_storage::backend_test!(async fn revoked_or_unprivileged_approval_cannot_commit_even_through_storage(b) {
    use orch8_types::{api_key::ApiCapability,ids::TenantId};
    use orch8_storage::{worker_claim::WorkerClaimPrincipal,worker_outcome::WorkerCompletion,worker_reconciliation::WorkerReconciliationApproval};
    let task=dispatch_claim(b.storage.as_ref(),None,true).await;let tenant=TenantId::unchecked("reconciliation");let claim=WorkerClaim::new("legacy",task.claim_epoch);
    let effect=b.storage.get_effect_receipt(&tenant,task.effect_id.unwrap()).await.unwrap().unwrap();
    for role in [ApiCapability::Operator,ApiCapability::Worker] {
        let operator=key(b.storage.as_ref(),tenant.as_str(),vec![role]).await;
        if role==ApiCapability::Operator {assert!(b.storage.revoke_api_key(&operator.record.id).await.unwrap());}
        let approval=WorkerReconciliationApproval::new(WorkerClaimPrincipal::ApiKey{tenant_id:tenant.clone(),key_id:operator.record.id},tenant.clone(),task.id,claim.clone(),Uuid::now_v7(),"backend-ack".into()).unwrap();
        let completion=WorkerCompletion::for_legacy_reconciliation(json!({"recovered":true}),approval).unwrap();
        assert!(b.storage.accept_worker_completion(task.id,&claim,&completion).await.is_err());
    }
    assert_eq!(b.storage.get_worker_task(task.id).await.unwrap().unwrap().state,orch8_types::worker::WorkerTaskState::Claimed);
    assert!(b.storage.get_worker_accepted_outcome(Some(&tenant),task.id,&claim).await.unwrap().is_none());
    assert_eq!(b.storage.get_effect_receipt(&tenant,task.effect_id.unwrap()).await.unwrap().unwrap(),effect);
});

orch8_storage::backend_test!(async fn reconciliation_insertion_failure_and_codec_floor_preserve_the_original_lease(b) {
    use orch8_storage::testing::backends::RawPool;
    let task=legacy_claim(b.storage.as_ref()).await;let claim=WorkerClaim::new("legacy",task.claim_epoch);
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("CREATE TRIGGER reject_reconciliation BEFORE INSERT ON worker_accepted_outcomes BEGIN SELECT RAISE(ABORT,'injected reconciliation failure'); END;").execute(pool).await.unwrap();},
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION reject_reconciliation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected reconciliation failure'; END $$; CREATE TRIGGER reject_reconciliation BEFORE INSERT ON worker_accepted_outcomes FOR EACH ROW EXECUTE FUNCTION reject_reconciliation();").execute(pool).await.unwrap();},
    }
    let server=spawn_test_server_on(b.storage.clone(),Some(ROOT)).await;let client=reqwest::Client::new();
    let url=format!("{}/workers/tasks/{}/reconcile-completion",server.v1_url(),task.id);let mut body=report(&task);
    body["output"]=json!({"n":1e18,"negative_zero":-0.0,"nested":["nul\0preserved"]});
    let response=client.post(&url).header("x-api-key",ROOT).json(&body).send().await.unwrap();assert!(!response.status().is_success());
    assert_eq!(b.storage.get_worker_task(task.id).await.unwrap().unwrap().state,orch8_types::worker::WorkerTaskState::Claimed);
    assert!(b.storage.get_worker_accepted_outcome(None,task.id,&claim).await.unwrap().is_none());
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("DROP TRIGGER reject_reconciliation").execute(pool).await.unwrap();},
        RawPool::Postgres(pool)=>{sqlx::raw_sql("DROP TRIGGER reject_reconciliation ON worker_accepted_outcomes; DROP FUNCTION reject_reconciliation();").execute(pool).await.unwrap();},
    }
    let response=client.post(&url).header("x-api-key",ROOT).json(&body).send().await.unwrap();assert_eq!(response.status(),reqwest::StatusCode::ACCEPTED,"{}",response.text().await.unwrap());
    let accepted=b.storage.get_worker_accepted_outcome(None,task.id,&claim).await.unwrap().unwrap();assert_eq!(accepted.output,body["output"]);
    let raw:serde_json::Value=match &b.raw {
        RawPool::Sqlite(pool)=>{let text:String=sqlx::query_scalar("SELECT record FROM worker_accepted_outcomes WHERE task_id=?1").bind(task.id.to_string()).fetch_one(pool).await.unwrap();serde_json::from_str(&text).unwrap()},
        RawPool::Postgres(pool)=>sqlx::query_scalar("SELECT record FROM worker_accepted_outcomes WHERE task_id=$1").bind(task.id).fetch_one(pool).await.unwrap(),
    };
    assert_eq!(raw["output_encoding"],"json_text_reconciled_v1");assert!(raw["output"].is_string());
    for downgrade in [true,false] {
        let candidate=Uuid::now_v7();let mut invalid=raw.clone();invalid["task_id"]=json!(candidate);
        if downgrade {invalid["output_encoding"]=json!("json_text_v1");}else{invalid.as_object_mut().unwrap().remove("reconciliation");}
        let error=match &b.raw {
            RawPool::Sqlite(pool)=>sqlx::query("INSERT INTO worker_accepted_outcomes(task_id,tenant_id,instance_id,record) VALUES(?1,?2,?3,?4)").bind(candidate.to_string()).bind(accepted.tenant_id.as_str()).bind(task.instance_id.to_string()).bind(invalid.to_string()).execute(pool).await.unwrap_err(),
            RawPool::Postgres(pool)=>sqlx::query("INSERT INTO worker_accepted_outcomes(task_id,tenant_id,instance_id,record) VALUES($1,$2,$3,$4)").bind(candidate).bind(accepted.tenant_id.as_str()).bind(task.instance_id.into_uuid()).bind(invalid).execute(pool).await.unwrap_err(),
        };assert!(error.to_string().contains("explicit reconciled reader"),"{error}");
    }
    if b.kind.is_postgres() {
        let mut tx=b.pg_pool().begin().await.unwrap();let error=sqlx::raw_sql(include_str!("../../migrations/down/141_worker_reconciliation_encoding.sql")).execute(&mut *tx).await.unwrap_err();
        assert!(error.to_string().contains("require the current reader"));tx.rollback().await.unwrap();
    }
    assert_eq!(b.storage.get_worker_accepted_outcome(None,task.id,&claim).await.unwrap().unwrap(),accepted);
});

orch8_storage::backend_test!(async fn concurrent_operator_retries_share_one_immutable_decision(b) {
    let task=legacy_claim(b.storage.as_ref()).await;let body=report(&task);
    let server=spawn_test_server_on(b.storage.clone(),Some(ROOT)).await;let client=reqwest::Client::new();
    let url=format!("{}/workers/tasks/{}/reconcile-completion",server.v1_url(),task.id);
    let (a,c)=tokio::join!(client.post(&url).header("x-api-key",ROOT).json(&body).send(),client.post(&url).header("x-api-key",ROOT).json(&body).send());
    let a=a.unwrap();let c=c.unwrap();assert_eq!(a.status(),reqwest::StatusCode::ACCEPTED,"{}",a.text().await.unwrap());assert_eq!(c.status(),reqwest::StatusCode::ACCEPTED,"{}",c.text().await.unwrap());
    assert_eq!(a.json::<serde_json::Value>().await.unwrap(),c.json::<serde_json::Value>().await.unwrap());
    let events=b.storage.list_worker_task_attempt_events(task.id,100).await.unwrap();
    assert_eq!(events.iter().filter(|e|e.event==orch8_types::worker::WorkerAttemptEventKind::Completed).count(),1);
});

orch8_storage::backend_test!(async fn approval_id_cannot_be_rebound_to_another_task(b) {
    let first=legacy_claim(b.storage.as_ref()).await;let second=legacy_claim(b.storage.as_ref()).await;
    let server=spawn_test_server_on(b.storage.clone(),Some(ROOT)).await;let client=reqwest::Client::new();let body=report(&first);
    assert_eq!(first.claim_epoch,second.claim_epoch);
    let response=client.post(format!("{}/workers/tasks/{}/reconcile-completion",server.v1_url(),first.id)).header("x-api-key",ROOT).json(&body).send().await.unwrap();
    assert_eq!(response.status(),reqwest::StatusCode::ACCEPTED,"{}",response.text().await.unwrap());
    let response=client.post(format!("{}/workers/tasks/{}/reconcile-completion",server.v1_url(),second.id)).header("x-api-key",ROOT).json(&body).send().await.unwrap();
    assert_eq!(response.status(),reqwest::StatusCode::CONFLICT,"an approval for a different task was reused: {}",response.text().await.unwrap());
    assert_eq!(b.storage.get_worker_task(second.id).await.unwrap().unwrap().state,orch8_types::worker::WorkerTaskState::Claimed);
    assert!(b.storage.get_worker_accepted_outcome(None,second.id,&WorkerClaim::new("legacy",second.claim_epoch)).await.unwrap().is_none());
});

fn effect_plan(
    task: &WorkerTask,
    fence: &orch8_storage::worker_invocation::WorkerDispatchFence,
) -> orch8_storage::worker_admission::WorkerEffectPlan {
    use orch8_storage::worker_admission::{WorkerEffectPlan, dispatch_effect, dispatch_scope};
    use orch8_types::continuity::{EffectId, EffectReceipt, EffectState, ExecutionEpoch};
    let scope = dispatch_scope(fence);
    let proof = dispatch_effect(task, EffectId::new());
    let now = Utc::now();
    WorkerEffectPlan::new(
        EffectReceipt {
            id: proof.effect_id,
            tenant_id: fence.tenant_id.clone(),
            continuity_id: scope.continuity_id,
            epoch: ExecutionEpoch::from_u64(scope.epoch),
            instance_id: task.instance_id,
            block_id: task.block_id.clone(),
            kind: proof.kind,
            state: EffectState::Planned,
            destination_fingerprint: proof.destination_fingerprint,
            idempotency_key: None,
            request_sha256: proof.request_sha256,
            provider_receipt_id: None,
            attempt: u32::from(task.attempt),
            created_at: now,
            updated_at: now,
        },
        false,
    )
}

orch8_storage::backend_test!(async fn explicit_legacy_approval_settles_the_original_unknown_effect_and_recovers_native_result(b) {
    use orch8_types::{continuity::EffectState,ids::TenantId};
    let task=dispatch_claim(b.storage.as_ref(),None,true).await;let tenant=TenantId::unchecked("reconciliation");let claim=WorkerClaim::new("legacy",task.claim_epoch);
    let mut effect=b.storage.get_effect_receipt(&tenant,task.effect_id.unwrap()).await.unwrap().unwrap();
    effect.transition(EffectState::Unknown,Utc::now()).unwrap();assert!(b.storage.cas_effect_receipt(&tenant,effect.id,EffectState::Dispatched,&effect).await.unwrap());
    let server=spawn_test_server_on(b.storage.clone(),Some(ROOT)).await;let mut body=report(&task);body["output"]["provider_receipt_id"]=json!("provider-confirmed-123");
    let response=reqwest::Client::new().post(format!("{}/workers/tasks/{}/reconcile-completion",server.v1_url(),task.id)).header("x-api-key",ROOT).json(&body).send().await.unwrap();
    assert_eq!(response.status(),reqwest::StatusCode::ACCEPTED,"{}",response.text().await.unwrap());
    let settled=b.storage.get_effect_receipt(&tenant,effect.id).await.unwrap().unwrap();assert_eq!(settled.state,EffectState::Committed);assert_eq!(settled.provider_receipt_id.as_deref(),Some("provider-confirmed-123"));
    assert_eq!(orch8_storage::worker_outcome::integrate_worker_outcome(b.storage.as_ref(),&tenant,task.id,&claim,1_048_576).await.unwrap(),orch8_storage::worker_outcome::WorkerOutcomeApplication::Applied);
    assert_eq!(b.storage.get_instance(task.instance_id).await.unwrap().unwrap().context.data["recovered"],true);
    assert!(b.storage.get_worker_accepted_outcome(Some(&tenant),task.id,&claim).await.unwrap().unwrap().reconciliation.is_some());
});

orch8_storage::backend_test!(async fn unsecured_history_requires_an_explicit_decision_without_transferring_the_issuer(b) {
    use orch8_storage::worker_claim::WorkerClaimPrincipal;
    let task=claimed_task(b.storage.as_ref(),Some(WorkerClaimPrincipal::Unsecured{})).await;
    let server=spawn_test_server_on(b.storage.clone(),Some(ROOT)).await;let client=reqwest::Client::new();let body=report(&task);
    let url=format!("{}/workers/tasks/{}/reconcile-completion",server.v1_url(),task.id);
    let response=client.post(&url).header("x-api-key",ROOT).json(&body).send().await.unwrap();assert_eq!(response.status(),reqwest::StatusCode::ACCEPTED,"{}",response.text().await.unwrap());
    let receipt:serde_json::Value=response.json().await.unwrap();assert_eq!(receipt["decision"]["history"]["kind"],"unsecured");
    let claim=WorkerClaim::new("legacy",task.claim_epoch);
    assert_eq!(b.storage.get_worker_issued_claim(None,task.id,&claim).await.unwrap().unwrap().principal,WorkerClaimPrincipal::Unsecured{});
    let response=client.post(format!("{}/workers/tasks/{}/complete",server.v1_url(),task.id)).header("x-api-key",ROOT)
        .json(&json!({"worker_id":"legacy","claim_epoch":task.claim_epoch,"output":{"recovered":true}})).send().await.unwrap();
    assert_eq!(response.status(),reqwest::StatusCode::FORBIDDEN,"an operator decision must not turn root into the original unsecured reporter");
});
