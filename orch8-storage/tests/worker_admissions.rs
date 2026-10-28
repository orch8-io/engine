//! Atomic dispatch admission owns its payload, receipt and reusable native generation.
use chrono::Utc;
use orch8_storage::{
    StorageBackend,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
    worker_admission::{
        WorkerDispatchIntent, WorkerDispatchPublication, WorkerEffectPlan, dispatch_effect,
        dispatch_scope,
    },
    worker_invocation::{WorkerDispatchFence, WorkerDispatchSelection},
};
use orch8_types::{
    continuity::{EffectId, EffectReceipt, EffectState, ExecutionEpoch},
    execution::{BlockType, ExecutionNode, NodeState},
    ids::{BlockId, ExecutionNodeId},
    instance::InstanceState,
    worker::WorkerTask,
};
use serde_json::json;
use uuid::Uuid;

async fn fixture(
    storage: &dyn StorageBackend,
) -> (WorkerTask, WorkerDispatchFence, ExecutionNodeId) {
    let sequence = SequenceBuilder::new("worker-admission")
        .name(&format!("admission-{}", Uuid::now_v7()))
        .step("work", "remote.work")
        .build();
    storage.create_sequence(&sequence).await.unwrap();
    let mut parent = InstanceBuilder::for_sequence(&sequence).due_now().build();
    parent.context.runtime.run_id = Some(Uuid::now_v7().to_string());
    parent.context.data = json!({"private":"ADMISSION_CONTEXT_CANARY"});
    storage.create_instance(&parent).await.unwrap();
    let parent = storage
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let node = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: parent.id,
        block_id: BlockId::new("work"),
        parent_id: None,
        block_type: BlockType::Step,
        branch_index: None,
        state: NodeState::Running,
        started_at: Some(Utc::now()),
        completed_at: None,
    };
    storage.create_execution_node(&node).await.unwrap();
    let fence = storage
        .prepare_worker_dispatch(
            &parent,
            &node.block_id,
            &WorkerDispatchSelection::Tree { node_id: node.id },
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        fence.continuity.is_none(),
        "fixture starts with no durable owner"
    );
    let task = serde_json::from_value(
        json!({"id":Uuid::now_v7(),"instance_id":parent.id,"block_id":"work",
        "handler_name":"remote.work","attempt":0,"params":{"value":"ADMISSION_PARAMS_CANARY"},
        "context":parent.context,"state":"pending","created_at":Utc::now()}),
    )
    .unwrap();
    (task, fence, node.id)
}

fn plan(task: &WorkerTask, fence: &WorkerDispatchFence) -> WorkerEffectPlan {
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
            request_sha256: proof.request_sha256,
            destination_fingerprint: proof.destination_fingerprint,
            idempotency_key: None,
            provider_receipt_id: None,
            attempt: u32::from(task.attempt),
            created_at: now,
            updated_at: now,
        },
        false,
    )
}

fn intent(task: WorkerTask, fence: WorkerDispatchFence) -> WorkerDispatchIntent {
    let effect = plan(&task, &fence);
    WorkerDispatchIntent::new(task, fence, Some(effect)).unwrap()
}

fn published(result: &WorkerDispatchPublication) -> Uuid {
    let WorkerDispatchPublication::Published { task_id, .. } = result else {
        panic!("publication refused: {result:?}")
    };
    *task_id
}

orch8_storage::backend_test!(async fn concurrent_admission_keeps_one_payload_and_survives_hot_gc(b) {
    let storage=b.storage.as_ref();let (task,fence,_)=fixture(storage).await;
    let first=intent(task.clone(),fence.clone());
    let debug=format!("{first:?}");assert!(!debug.contains("ADMISSION_PARAMS_CANARY"));assert!(!debug.contains("ADMISSION_CONTEXT_CANARY"));
    let mut alternative=task.clone();alternative.id=Uuid::now_v7();
    alternative.params=json!({"value":"SECOND_ADMISSION_CANARY"});let second=intent(alternative.clone(),fence.clone());
    let(left,right)=tokio::join!(storage.admit_worker_dispatch(&first),storage.admit_worker_dispatch(&second));
    let actual_id=published(&left.unwrap());assert_eq!(published(&right.unwrap()),actual_id);
    let actual=storage.get_worker_task(actual_id).await.unwrap().unwrap();
    assert!(actual.params==task.params || actual.params==alternative.params);
    let receipt=storage.get_effect_receipt(&fence.tenant_id,actual.effect_id.unwrap()).await.unwrap().unwrap();
    assert_eq!(receipt.state,EffectState::Dispatched);
    let proof=dispatch_effect(&actual,receipt.id);assert_eq!(proof.request_sha256,receipt.request_sha256);
    let admission=storage.get_worker_dispatch_admission(&fence.tenant_id,actual_id).await.unwrap().unwrap();
    assert_eq!(admission.invocation.handler_name,actual.handler_name);
    let record=serde_json::to_string(&admission).unwrap();
    for secret in ["ADMISSION_CONTEXT_CANARY","ADMISSION_PARAMS_CANARY","SECOND_ADMISSION_CANARY"] {assert!(!record.contains(secret));}
    assert!(storage.get_worker_dispatch_admission(&orch8_types::ids::TenantId::unchecked("foreign"),actual_id).await.unwrap().is_none());
    storage.delete_worker_task(actual_id).await.unwrap();
    assert_eq!(published(&storage.admit_worker_dispatch(&second).await.unwrap()),actual_id);
    assert!(storage.get_worker_task(actual_id).await.unwrap().is_none(),"cold admission resurrected collected hot work");
    storage.update_instance_state(task.instance_id,InstanceState::Cancelled,None).await.unwrap();
    assert_eq!(storage.purge_terminal_instances(&[task.instance_id]).await.unwrap(),0,"unresolved effect evidence must retain native history");
    let mut settled=receipt.clone();settled.transition(EffectState::Committed,Utc::now()).unwrap();
    assert!(storage.cas_effect_receipt(&fence.tenant_id,receipt.id,EffectState::Dispatched,&settled).await.unwrap());
    assert_eq!(storage.purge_terminal_instances(&[task.instance_id]).await.unwrap(),1);
    assert!(storage.get_instance(task.instance_id).await.unwrap().is_none());
    assert_eq!(storage.get_worker_dispatch_admission(&fence.tenant_id,actual_id).await.unwrap().unwrap(),admission);
});

orch8_storage::backend_test!(async fn stale_activation_rolls_back_initial_owner_and_effect(b) {
    let storage=b.storage.as_ref();let(task,fence,node)=fixture(storage).await;let prepared=intent(task.clone(),fence.clone());
    storage.update_node_state(node,NodeState::Pending).await.unwrap();storage.update_node_state(node,NodeState::Running).await.unwrap();
    assert!(matches!(storage.admit_worker_dispatch(&prepared).await.unwrap(),WorkerDispatchPublication::Stale));
    assert!(storage.get_worker_task(task.id).await.unwrap().is_none());
    assert!(storage.get_continuity_execution_by_instance(&fence.tenant_id,task.instance_id).await.unwrap().is_none());
    assert!(storage.get_worker_dispatch_admission(&fence.tenant_id,task.id).await.unwrap().is_none());
    assert_eq!(storage.get_execution_tree(task.instance_id).await.unwrap()[0].state,NodeState::Running);
});

orch8_storage::backend_test!(async fn payload_changed_after_receipt_planning_is_refused_before_storage(b) {
    let(task,fence,_)=fixture(b.storage.as_ref()).await;let effect=plan(&task,&fence);
    let mut changed=task.clone();changed.params=json!({"value":"different request"});
    assert!(WorkerDispatchIntent::new(changed,fence.clone(),Some(effect.clone())).is_err());
    let mut changed=task.clone();changed.context["runtime"]["dry_run"]=json!(true);
    assert!(WorkerDispatchIntent::new(changed,fence.clone(),Some(effect.clone())).is_err());
    let mut changed=task.clone();changed.context["runtime"]["run_id"]=json!("different run");
    assert!(WorkerDispatchIntent::new(changed,fence.clone(),Some(effect.clone())).is_err());
    let mut changed=task.clone();changed.resume_checkpoint=Some(json!({"old":"checkpoint"}));
    assert!(WorkerDispatchIntent::new(changed,fence.clone(),Some(effect.clone())).is_err());
    assert!(WorkerDispatchIntent::new(task.clone(),fence.clone(),None).is_err(),"unknown remote handler acquired pure authority");
    let mut changed=effect;changed.receipt.destination_fingerprint="00".repeat(32);
    assert!(WorkerDispatchIntent::new(task,fence,Some(changed)).is_err());
});

orch8_storage::backend_test!(async fn admission_insert_failure_rolls_back_queue_effect_binding_and_initial_scope(b) {
    use orch8_storage::testing::backends::RawPool;
    let storage=b.storage.as_ref();let(task,fence,node)=fixture(storage).await;let prepared=intent(task.clone(),fence.clone());
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("CREATE TRIGGER reject_admission_test BEFORE INSERT ON worker_task_admissions BEGIN SELECT RAISE(ABORT,'injected admission failure'); END;").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION reject_admission_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected admission failure'; END $$; CREATE TRIGGER reject_admission_test BEFORE INSERT ON worker_task_admissions FOR EACH ROW EXECUTE FUNCTION reject_admission_test();").execute(pool).await.unwrap();},
    }
    assert!(storage.admit_worker_dispatch(&prepared).await.is_err());
    assert!(storage.get_worker_task(task.id).await.unwrap().is_none());
    assert!(storage.get_worker_invocation(&fence.tenant_id,task.id).await.unwrap().is_none());
    assert!(storage.get_continuity_execution_by_instance(&fence.tenant_id,task.instance_id).await.unwrap().is_none());
    assert_eq!(storage.get_execution_tree(task.instance_id).await.unwrap().iter().find(|n|n.id==node).unwrap().state,NodeState::Running);
    let count:i64=match &b.raw {
        RawPool::Sqlite(pool)=>{let count=sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM effect_receipts)+(SELECT COUNT(*) FROM effect_attempt_bindings)+(SELECT COUNT(*) FROM worker_task_admissions)").fetch_one(pool).await.unwrap();sqlx::raw_sql("DROP TRIGGER reject_admission_test").execute(pool).await.unwrap();count},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{let count=sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM effect_receipts)+(SELECT COUNT(*) FROM effect_attempt_bindings)+(SELECT COUNT(*) FROM worker_task_admissions)").fetch_one(pool).await.unwrap();sqlx::raw_sql("DROP TRIGGER reject_admission_test ON worker_task_admissions; DROP FUNCTION reject_admission_test();").execute(pool).await.unwrap();count},
    };
    assert_eq!(count,0,"failed publication left receipt/admission evidence");
    assert_eq!(published(&storage.admit_worker_dispatch(&prepared).await.unwrap()),task.id);
});

orch8_storage::backend_test!(async fn admitted_payload_and_cold_receipt_cannot_be_rewritten(b) {
    use orch8_storage::testing::backends::RawPool;
    let storage=b.storage.as_ref();let(task,fence,_)=fixture(storage).await;
    let id=published(&storage.admit_worker_dispatch(&intent(task.clone(),fence.clone())).await.unwrap());
    let before=storage.get_worker_task(id).await.unwrap().unwrap();
    match &b.raw {
        RawPool::Sqlite(pool)=>{
            assert!(sqlx::query("UPDATE worker_tasks SET params='{}' WHERE id=?1").bind(id.to_string()).execute(pool).await.is_err());
            assert!(sqlx::query("UPDATE worker_tasks SET handler_name='different' WHERE id=?1").bind(id.to_string()).execute(pool).await.is_err());
            assert!(sqlx::query("UPDATE worker_tasks SET effect_id=NULL WHERE id=?1").bind(id.to_string()).execute(pool).await.is_err());
            assert!(sqlx::query("UPDATE worker_tasks SET tenant_id='foreign' WHERE id=?1").bind(id.to_string()).execute(pool).await.is_err());
            assert!(sqlx::query("UPDATE worker_task_admissions SET record=record WHERE task_id=?1").bind(id.to_string()).execute(pool).await.is_err());
            assert!(sqlx::query("DELETE FROM worker_task_admissions WHERE task_id=?1").bind(id.to_string()).execute(pool).await.is_err());
            if b.name().starts_with("encrypted") {let raw:String=sqlx::query_scalar("SELECT params || context FROM worker_tasks WHERE id=?1").bind(id.to_string()).fetch_one(pool).await.unwrap();assert!(!raw.contains("ADMISSION_PARAMS_CANARY"));assert!(!raw.contains("ADMISSION_CONTEXT_CANARY"));}
        },
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{
            assert!(sqlx::query("UPDATE worker_tasks SET params='{}' WHERE id=$1").bind(id).execute(pool).await.is_err());
            assert!(sqlx::query("UPDATE worker_tasks SET handler_name='different' WHERE id=$1").bind(id).execute(pool).await.is_err());
            assert!(sqlx::query("UPDATE worker_tasks SET effect_id=NULL WHERE id=$1").bind(id).execute(pool).await.is_err());
            assert!(sqlx::query("UPDATE worker_tasks SET tenant_id='foreign' WHERE id=$1").bind(id).execute(pool).await.is_err());
            assert!(sqlx::query("UPDATE worker_task_admissions SET record=record WHERE task_id=$1").bind(id).execute(pool).await.is_err());
            assert!(sqlx::query("DELETE FROM worker_task_admissions WHERE task_id=$1").bind(id).execute(pool).await.is_err());
            if b.name().starts_with("encrypted") {let raw:String=sqlx::query_scalar("SELECT params::text || context::text FROM worker_tasks WHERE id=$1").bind(id).fetch_one(pool).await.unwrap();assert!(!raw.contains("ADMISSION_PARAMS_CANARY"));assert!(!raw.contains("ADMISSION_CONTEXT_CANARY"));}
        },
    }
    assert_eq!(storage.get_worker_task(id).await.unwrap().unwrap().params,before.params);
    storage.delete_worker_task(id).await.unwrap();
    assert!(storage.create_worker_task(&before).await.is_err(),"legacy writer recreated an admitted task id after hot GC");
});

orch8_storage::backend_test!(async fn late_foreign_owner_cannot_be_adopted_by_initial_admission(b) {
    use orch8_types::continuity::{ContinuityExecution,ContinuityId,OwnershipState,RuntimeId};
    let storage=b.storage.as_ref();let(task,fence,_)=fixture(storage).await;let prepared=intent(task.clone(),fence.clone());
    let foreign=ContinuityExecution {continuity_id:ContinuityId::new(),tenant_id:fence.tenant_id.clone(),
        current_instance_id:task.instance_id,owner_runtime_id:RuntimeId::new(),epoch:ExecutionEpoch::initial(),
        state:OwnershipState::Owned,updated_at:Utc::now()};
    let foreign=storage.ensure_continuity_execution(&foreign).await.unwrap();
    assert!(matches!(storage.admit_worker_dispatch(&prepared).await.unwrap(),WorkerDispatchPublication::Stale));
    assert!(storage.get_worker_task(task.id).await.unwrap().is_none());
    assert_eq!(storage.get_continuity_execution_by_instance(&fence.tenant_id,task.instance_id).await.unwrap().unwrap(),foreign);
});

orch8_storage::backend_test!(async fn fallback_admission_keeps_reservation_scope_and_resolves_actual_handler(b) {
    use orch8_types::worker::{WorkerTaskResolution,WorkerTaskResolutionAction,WorkerAttemptEventKind};
    let storage=b.storage.as_ref();let(task,fence,node)=fixture(storage).await;
    let id=published(&storage.admit_worker_dispatch(&intent(task.clone(),fence.clone())).await.unwrap());
    let first=storage.get_worker_task(id).await.unwrap().unwrap();
    let mut receipt=storage.get_effect_receipt(&fence.tenant_id,first.effect_id.unwrap()).await.unwrap().unwrap();
    receipt.transition(EffectState::Unknown,Utc::now()).unwrap();
    assert!(storage.cas_effect_receipt(&fence.tenant_id,receipt.id,EffectState::Dispatched,&receipt).await.unwrap());
    let mut replacement=first.clone();replacement.id=Uuid::now_v7();replacement.attempt+=1;
    replacement.effect_id=None;replacement.continuity_epoch=None;
    storage.retry_worker_task(id,&replacement,Some(node),task.instance_id,Utc::now()).await.unwrap();
    let reserved=storage.get_worker_invocation(&fence.tenant_id,replacement.id).await.unwrap().unwrap();
    let parent=storage.claim_due_instances(Utc::now()+chrono::Duration::seconds(1),1,0).await.unwrap().pop().unwrap();
    let next=storage.prepare_worker_dispatch(&parent,&task.block_id,&WorkerDispatchSelection::Tree{node_id:node}).await.unwrap().unwrap();
    replacement.handler_name="remote.fallback".into();replacement.params=json!({"value":"FALLBACK_REQUEST_CANARY"});
    replacement.context=serde_json::to_value(&parent.context).unwrap();replacement.continuity_epoch=next.continuity.as_ref().map(|scope|scope.epoch);
    let id=published(&storage.admit_worker_dispatch(&intent(replacement.clone(),next)).await.unwrap());
    assert_eq!(id,replacement.id);
    assert_eq!(storage.get_worker_invocation(&fence.tenant_id,id).await.unwrap().unwrap(),reserved,"admission rewrote the immutable reservation");
    let admission=storage.get_worker_dispatch_admission(&fence.tenant_id,id).await.unwrap().unwrap();
    assert_eq!(admission.invocation.handler_name,"remote.fallback");
    let held=storage.claim_worker_tasks("remote.fallback","fallback-worker",1).await.unwrap().pop().unwrap();
    assert_eq!(held.params,replacement.params);
    assert!(storage.resolve_worker_task(&WorkerTaskResolution {
        task_id:id,instance_id:task.instance_id,expected_state:held.state,expected_claim_epoch:held.claim_epoch,
        expected_worker_id:held.worker_id.clone(),holder_worker_id:held.worker_id.clone(),event:WorkerAttemptEventKind::Reclaimed,
        reason:"not started".into(),retryable:false,action:WorkerTaskResolutionAction::Requeue,
    }).await.unwrap(),"completion/release rejected the admitted fallback handler");
});

orch8_storage::backend_test!(async fn guarded_duplicate_admission_rolls_back_its_new_receipt_and_node(b) {
    let storage=b.storage.as_ref();let(task,fence,node)=fixture(storage).await;
    let mut effect=plan(&task,&fence);effect.guarded=true;
    let first=WorkerDispatchIntent::new(task.clone(),fence.clone(),Some(effect)).unwrap();
    let id=published(&storage.admit_worker_dispatch(&first).await.unwrap());
    let old=storage.get_worker_task(id).await.unwrap().unwrap();
    storage.delete_worker_task(id).await.unwrap();
    storage.retire_effect_attempts_for_blocks(&fence.tenant_id,task.instance_id,std::slice::from_ref(&task.block_id)).await.unwrap();
    storage.update_node_state(node,NodeState::Pending).await.unwrap();storage.update_node_state(node,NodeState::Running).await.unwrap();
    let parent=storage.get_instance(task.instance_id).await.unwrap().unwrap();
    let next=storage.prepare_worker_dispatch(&parent,&task.block_id,&WorkerDispatchSelection::Tree{node_id:node}).await.unwrap().unwrap();
    let mut retry=task.clone();retry.id=Uuid::now_v7();retry.continuity_epoch=next.continuity.as_ref().map(|scope|scope.epoch);
    let mut effect=plan(&retry,&next);let candidate=effect.receipt.id;effect.guarded=true;
    let prepared=WorkerDispatchIntent::new(retry.clone(),next,Some(effect)).unwrap();
    assert!(matches!(storage.admit_worker_dispatch(&prepared).await.unwrap(),WorkerDispatchPublication::Duplicate(_)));
    assert!(storage.get_worker_task(retry.id).await.unwrap().is_none());
    assert!(storage.get_effect_receipt(&fence.tenant_id,candidate).await.unwrap().is_none());
    assert_eq!(storage.get_effect_receipt(&fence.tenant_id,old.effect_id.unwrap()).await.unwrap().unwrap().state,EffectState::Dispatched);
    assert_eq!(storage.get_execution_tree(task.instance_id).await.unwrap()[0].state,NodeState::Running);
});

orch8_storage::backend_test!(async fn admission_never_saturates_a_worker_attempt(b) {
    use orch8_storage::testing::backends::RawPool;
    let storage=b.storage.as_ref();let(mut task,fence,_)=fixture(storage).await;
    task.attempt=u16::MAX;let prepared=intent(task.clone(),fence.clone());
    match &b.raw {
        RawPool::Sqlite(_)=>{let id=published(&storage.admit_worker_dispatch(&prepared).await.unwrap());assert_eq!(storage.get_worker_task(id).await.unwrap().unwrap().attempt,u16::MAX);},
        #[cfg(feature="postgres")]
        RawPool::Postgres(_)=>{assert!(storage.admit_worker_dispatch(&prepared).await.is_err());assert!(storage.get_worker_task(task.id).await.unwrap().is_none());assert!(storage.get_continuity_execution_by_instance(&fence.tenant_id,task.instance_id).await.unwrap().is_none());},
    }
});

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_admission_downgrade_preserves_evidence_and_empty_roundtrip_works() {
    let Some(b) = orch8_storage::testing::backends::TestBackend::postgres().await else {
        return;
    };
    let down = include_str!("../../migrations/down/133_worker_dispatch_admissions.sql");
    let up = include_str!("../../migrations/133_worker_dispatch_admissions.sql");
    let mut tx = b.pg_pool().begin().await.unwrap();
    sqlx::raw_sql(down).execute(&mut *tx).await.unwrap();
    sqlx::raw_sql(up).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let (task, fence, _) = fixture(b.storage.as_ref()).await;
    let id = published(
        &b.storage
            .admit_worker_dispatch(&intent(task, fence.clone()))
            .await
            .unwrap(),
    );
    let admission = b
        .storage
        .get_worker_dispatch_admission(&fence.tenant_id, id)
        .await
        .unwrap()
        .unwrap();
    let mut tx = b.pg_pool().begin().await.unwrap();
    let error = sqlx::raw_sql(down).execute(&mut *tx).await.unwrap_err();
    assert!(error.to_string().contains("admissions must be retained"));
    tx.rollback().await.unwrap();
    assert_eq!(
        b.storage
            .get_worker_dispatch_admission(&fence.tenant_id, id)
            .await
            .unwrap()
            .unwrap(),
        admission
    );
}

#[tokio::test]
async fn sqlite_reopen_keeps_admission_and_does_not_republish_work() {
    use orch8_storage::{WorkerStore, sqlite::SqliteStorage};
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("admission.sqlite");
    let file = file.to_str().unwrap();
    let storage = SqliteStorage::file(file).await.unwrap();
    let (task, fence, _) = fixture(&storage).await;
    let prepared = intent(task.clone(), fence.clone());
    let id = published(&storage.admit_worker_dispatch(&prepared).await.unwrap());
    let admission = storage
        .get_worker_dispatch_admission(&fence.tenant_id, id)
        .await
        .unwrap()
        .unwrap();
    let body = storage.get_worker_task(id).await.unwrap().unwrap();
    storage.pool().close().await;
    drop(storage);
    let storage = SqliteStorage::file(file).await.unwrap();
    assert_eq!(
        storage
            .get_worker_dispatch_admission(&fence.tenant_id, id)
            .await
            .unwrap()
            .unwrap(),
        admission
    );
    assert_eq!(
        published(&storage.admit_worker_dispatch(&prepared).await.unwrap()),
        id
    );
    assert_eq!(
        storage.get_worker_task(id).await.unwrap().unwrap().params,
        body.params
    );
    assert!(
        sqlx::query("UPDATE worker_tasks SET params='{}' WHERE id=?1")
            .bind(id.to_string())
            .execute(storage.pool())
            .await
            .is_err()
    );
    storage.pool().close().await;
}

orch8_storage::backend_test!(async fn accepted_outcome_survives_hot_gc_and_enforces_exact_claim_and_tenant(b) {
    use orch8_types::{ids::TenantId, worker::WorkerClaim};
    use orch8_storage::testing::backends::RawPool;
    let storage=b.storage.as_ref();let(task,fence,_)=fixture(storage).await;
    let id=published(&storage.admit_worker_dispatch(&intent(task,fence.clone())).await.unwrap());
    let held=storage.claim_worker_tasks("remote.work","original-worker",1).await.unwrap().pop().unwrap();
    let claim=WorkerClaim::new("original-worker",held.claim_epoch);
    let output=json!({"secret":"OUTCOME_PRIVATE_CANARY"});
    assert!(storage.complete_worker_task(id,&claim,&output).await.unwrap());
    assert!(!storage.complete_worker_task(id,&claim,&json!({"secret":"changed"})).await.unwrap());
    let accepted=storage.get_worker_accepted_outcome(Some(&fence.tenant_id),id,&claim).await.unwrap().unwrap();
    assert_eq!(accepted.output,output);
    assert_eq!(accepted.invocation.as_ref().unwrap(), &storage.get_worker_dispatch_admission(&fence.tenant_id,id).await.unwrap().unwrap().invocation);
    assert!(!format!("{accepted:?}").contains("OUTCOME_PRIVATE_CANARY"));
    match &b.raw {
        RawPool::Sqlite(pool)=>{
            assert!(sqlx::query("UPDATE worker_accepted_outcomes SET record=record WHERE task_id=?1").bind(id.to_string()).execute(pool).await.is_err());
            assert!(sqlx::query("DELETE FROM worker_accepted_outcomes WHERE task_id=?1").bind(id.to_string()).execute(pool).await.is_err());
            if b.name().starts_with("encrypted") {let raw:String=sqlx::query_scalar("SELECT record FROM worker_accepted_outcomes WHERE task_id=?1").bind(id.to_string()).fetch_one(pool).await.unwrap();assert!(!raw.contains("OUTCOME_PRIVATE_CANARY"));}
        },
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{
            assert!(sqlx::query("UPDATE worker_accepted_outcomes SET record=record WHERE task_id=$1").bind(id).execute(pool).await.is_err());
            assert!(sqlx::query("DELETE FROM worker_accepted_outcomes WHERE task_id=$1").bind(id).execute(pool).await.is_err());
            if b.name().starts_with("encrypted") {let raw:String=sqlx::query_scalar("SELECT record::text FROM worker_accepted_outcomes WHERE task_id=$1").bind(id).fetch_one(pool).await.unwrap();assert!(!raw.contains("OUTCOME_PRIVATE_CANARY"));}
        },
    }
    storage.delete_worker_task(id).await.unwrap();
    assert_eq!(storage.get_worker_accepted_outcome(Some(&fence.tenant_id),id,&claim).await.unwrap().unwrap(),accepted);
    for wrong in [WorkerClaim::new("other-worker",claim.claim_epoch),WorkerClaim::new("original-worker",claim.claim_epoch+1),WorkerClaim::new("original-worker",u64::MAX)] {
        assert!(storage.get_worker_accepted_outcome(Some(&fence.tenant_id),id,&wrong).await.unwrap().is_none());
    }
    assert!(storage.get_worker_accepted_outcome(Some(&TenantId::unchecked("other-tenant")),id,&claim).await.unwrap().is_none());
});

orch8_storage::backend_test!(async fn accepted_outcome_insert_failure_rolls_back_completion_and_attempt_event(b) {
    use orch8_types::worker::{WorkerClaim,WorkerTaskState,WorkerAttemptEventKind};
    use orch8_storage::testing::backends::RawPool;
    let storage=b.storage.as_ref();let(task,fence,_)=fixture(storage).await;
    let id=published(&storage.admit_worker_dispatch(&intent(task,fence.clone())).await.unwrap());
    let held=storage.claim_worker_tasks("remote.work","worker",1).await.unwrap().pop().unwrap();
    let claim=WorkerClaim::new("worker",held.claim_epoch);
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("CREATE TRIGGER reject_outcome_test BEFORE INSERT ON worker_accepted_outcomes BEGIN SELECT RAISE(ABORT,'injected outcome failure'); END;").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION reject_outcome_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outcome failure'; END $$; CREATE TRIGGER reject_outcome_test BEFORE INSERT ON worker_accepted_outcomes FOR EACH ROW EXECUTE FUNCTION reject_outcome_test();").execute(pool).await.unwrap();},
    }
    assert!(storage.complete_worker_task(id,&claim,&json!({"value":"original"})).await.is_err());
    let after=storage.get_worker_task(id).await.unwrap().unwrap();
    assert_eq!(after.state,WorkerTaskState::Claimed);assert!(after.output.is_none());
    let receipt=storage.get_effect_receipt(&fence.tenant_id,held.effect_id.unwrap()).await.unwrap().unwrap();
    assert_eq!(receipt.state,EffectState::Dispatched,"journal failure committed an orphan effect");
    assert!(receipt.provider_receipt_id.is_none());
    assert!(storage.get_worker_accepted_outcome(Some(&fence.tenant_id),id,&claim).await.unwrap().is_none());
    assert!(!storage.list_worker_task_attempt_events(id,100).await.unwrap().iter().any(|e|e.event==WorkerAttemptEventKind::Completed));
});

orch8_storage::backend_test!(async fn accepted_outcome_concurrent_payloads_have_one_durable_winner(b) {
    use orch8_types::worker::WorkerClaim;
    let storage=b.storage.as_ref();let(task,fence,_)=fixture(storage).await;
    let id=published(&storage.admit_worker_dispatch(&intent(task,fence.clone())).await.unwrap());
    let held=storage.claim_worker_tasks("remote.work","worker",1).await.unwrap().pop().unwrap();
    let claim=WorkerClaim::new("worker",held.claim_epoch);
    let left=json!({"value":"left","provider_receipt_id":"left"});let right=json!({"value":"right","provider_receipt_id":"right"});
    let (a,b)=tokio::join!(storage.complete_worker_task(id,&claim,&left),storage.complete_worker_task(id,&claim,&right));
    let a=a.unwrap();let b=b.unwrap();assert_ne!(a,b);
    let accepted=storage.get_worker_accepted_outcome(Some(&fence.tenant_id),id,&claim).await.unwrap().unwrap();
    assert_eq!(accepted.output,if a {left} else {right});
    let receipt=storage.get_effect_receipt(&fence.tenant_id,held.effect_id.unwrap()).await.unwrap().unwrap();
    assert_eq!(receipt.state,EffectState::Committed);
    assert_eq!(receipt.provider_receipt_id.as_deref(),Some(if a {"left"} else {"right"}));
    assert_eq!(storage.get_worker_task(id).await.unwrap().unwrap().output,Some(accepted.output));
});

#[tokio::test]
async fn sqlite_reopen_keeps_original_accepted_outcome_after_hot_gc() {
    use orch8_storage::{WorkerStore, sqlite::SqliteStorage};
    use orch8_types::worker::WorkerClaim;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("outcome.sqlite");
    let file = file.to_str().unwrap();
    let storage = SqliteStorage::file(file).await.unwrap();
    let (task, fence, _) = fixture(&storage).await;
    let id = published(
        &storage
            .admit_worker_dispatch(&intent(task, fence.clone()))
            .await
            .unwrap(),
    );
    let held = storage
        .claim_worker_tasks("remote.work", "worker", 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let claim = WorkerClaim::new("worker", held.claim_epoch);
    let original = json!({"result":"durable"});
    assert!(
        storage
            .complete_worker_task(id, &claim, &original)
            .await
            .unwrap()
    );
    let accepted = storage
        .get_worker_accepted_outcome(Some(&fence.tenant_id), id, &claim)
        .await
        .unwrap()
        .unwrap();
    storage.delete_worker_task(id).await.unwrap();
    storage.pool().close().await;
    drop(storage);
    let storage = SqliteStorage::file(file).await.unwrap();
    assert_eq!(
        storage
            .get_worker_accepted_outcome(Some(&fence.tenant_id), id, &claim)
            .await
            .unwrap()
            .unwrap(),
        accepted
    );
    storage.pool().close().await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_outcome_downgrade_preserves_accepted_evidence() {
    use orch8_types::worker::WorkerClaim;
    let Some(b) = orch8_storage::testing::backends::TestBackend::postgres().await else {
        return;
    };
    let down = include_str!("../../migrations/down/134_worker_accepted_outcomes.sql");
    let up = include_str!("../../migrations/134_worker_accepted_outcomes.sql");
    let mut tx = b.pg_pool().begin().await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../migrations/down/135_worker_outcome_encoding.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::raw_sql(down).execute(&mut *tx).await.unwrap();
    sqlx::raw_sql(up).execute(&mut *tx).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../migrations/135_worker_outcome_encoding.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let (task, fence, _) = fixture(b.storage.as_ref()).await;
    let id = published(
        &b.storage
            .admit_worker_dispatch(&intent(task, fence.clone()))
            .await
            .unwrap(),
    );
    let held = b
        .storage
        .claim_worker_tasks("remote.work", "worker", 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let claim = WorkerClaim::new("worker", held.claim_epoch);
    assert!(
        b.storage
            .complete_worker_task(id, &claim, &json!({"value":"original"}))
            .await
            .unwrap()
    );
    let mut tx = b.pg_pool().begin().await.unwrap();
    let error = sqlx::raw_sql(down).execute(&mut *tx).await.unwrap_err();
    assert!(error.to_string().contains("outcomes must be retained"));
    tx.rollback().await.unwrap();
    assert_eq!(
        b.storage
            .get_worker_accepted_outcome(Some(&fence.tenant_id), id, &claim)
            .await
            .unwrap()
            .unwrap()
            .output,
        json!({"value":"original"})
    );
}

orch8_storage::backend_test!(async fn accepted_outcome_uses_authoritative_parent_tenant_for_legacy_rows(b) {
    use orch8_storage::testing::backends::RawPool;
    use orch8_types::{ids::TenantId,worker::WorkerClaim};
    let storage=b.storage.as_ref();
    for mirror in [None,Some("foreign-mirror")] {
        let(task,fence,_)=fixture(storage).await;
        storage.create_worker_task(&task).await.unwrap();
        let held=storage.claim_worker_tasks("remote.work","worker",1).await.unwrap().pop().unwrap();
        let claim=WorkerClaim::new("worker",held.claim_epoch);
        match &b.raw {
            RawPool::Sqlite(pool)=>{sqlx::query("UPDATE worker_tasks SET tenant_id=?1 WHERE id=?2").bind(mirror).bind(task.id.to_string()).execute(pool).await.unwrap();},
            #[cfg(feature="postgres")]
            RawPool::Postgres(pool)=>{sqlx::query("UPDATE worker_tasks SET tenant_id=$1 WHERE id=$2").bind(mirror).bind(task.id).execute(pool).await.unwrap();},
        }
        assert!(storage.complete_worker_task(task.id,&claim,&json!({"result":"original"})).await.unwrap());
        assert_eq!(storage.get_worker_accepted_outcome(Some(&fence.tenant_id),task.id,&claim).await.unwrap().unwrap().tenant_id,fence.tenant_id);
        assert!(storage.get_worker_accepted_outcome(Some(&TenantId::unchecked("foreign-mirror")),task.id,&claim).await.unwrap().is_none());
    }
});

orch8_storage::backend_test!(async fn completion_never_saturates_an_out_of_range_claim_epoch(b) {
    use orch8_storage::testing::backends::RawPool;
    use orch8_types::worker::{WorkerClaim,WorkerTaskState};
    let storage=b.storage.as_ref();let(task,fence,_)=fixture(storage).await;
    let id=published(&storage.admit_worker_dispatch(&intent(task,fence.clone())).await.unwrap());
    storage.claim_worker_tasks("remote.work","worker",1).await.unwrap();
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::query("UPDATE worker_tasks SET claim_epoch=?1 WHERE id=?2").bind(i64::MAX).bind(id.to_string()).execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::query("UPDATE worker_tasks SET claim_epoch=$1 WHERE id=$2").bind(i64::MAX).bind(id).execute(pool).await.unwrap();},
    }
    let oversized=WorkerClaim::new("worker",u64::MAX);
    assert!(storage.complete_worker_task(id,&oversized,&json!({"value":"wrong"})).await.is_err());
    assert_eq!(storage.get_worker_task(id).await.unwrap().unwrap().state,WorkerTaskState::Claimed);
    assert!(storage.get_worker_accepted_outcome(Some(&fence.tenant_id),id,&oversized).await.unwrap().is_none());
});

orch8_storage::backend_test!(async fn completion_atomically_commits_bound_effect_with_original_result(b) {
    use orch8_types::worker::WorkerClaim;
    let storage=b.storage.as_ref();let(task,fence,_)=fixture(storage).await;
    let id=published(&storage.admit_worker_dispatch(&intent(task,fence.clone())).await.unwrap());
    let held=storage.claim_worker_tasks("remote.work","worker",1).await.unwrap().pop().unwrap();
    let claim=WorkerClaim::new("worker",held.claim_epoch);
    let output=json!({"result":"original","provider_receipt_id":"provider-original"});
    assert!(storage.complete_worker_task(id,&claim,&output).await.unwrap());
    let receipt=storage.get_effect_receipt(&fence.tenant_id,held.effect_id.unwrap()).await.unwrap().unwrap();
    assert_eq!(receipt.state,EffectState::Committed,"task completion left its receipt unsettled");
    assert_eq!(receipt.provider_receipt_id.as_deref(),Some("provider-original"));
    assert_eq!(storage.get_worker_accepted_outcome(Some(&fence.tenant_id),id,&claim).await.unwrap().unwrap().output,output);
});

orch8_storage::backend_test!(async fn completion_of_an_unknown_effect_keeps_original_binding(b) {
    use orch8_types::worker::WorkerClaim;
    let storage=b.storage.as_ref();let(task,fence,_)=fixture(storage).await;
    let id=published(&storage.admit_worker_dispatch(&intent(task,fence.clone())).await.unwrap());
    let held=storage.claim_worker_tasks("remote.work","worker",1).await.unwrap().pop().unwrap();
    let claim=WorkerClaim::new("worker",held.claim_epoch);
    let mut receipt=storage.get_effect_receipt(&fence.tenant_id,held.effect_id.unwrap()).await.unwrap().unwrap();
    receipt.transition(EffectState::Unknown,Utc::now()).unwrap();
    assert!(storage.cas_effect_receipt(&fence.tenant_id,receipt.id,EffectState::Dispatched,&receipt).await.unwrap());
    assert!(storage.complete_worker_task(id,&claim,&json!({"request_id":"original-request"})).await.unwrap());
    let accepted=storage.get_worker_accepted_outcome(Some(&fence.tenant_id),id,&claim).await.unwrap().unwrap();
    let committed=storage.get_effect_receipt(&fence.tenant_id,receipt.id).await.unwrap().unwrap();
    assert_eq!(committed.state,EffectState::Committed);
    assert_eq!(committed.provider_receipt_id.as_deref(),Some("original-request"));
    assert_eq!(accepted.invocation.unwrap().effect_id,Some(receipt.id));
    assert!(accepted.logical_output_sha256.is_some());
});

orch8_storage::backend_test!(async fn precommitted_effect_without_accepted_result_is_not_recertified(b) {
    use orch8_types::worker::{WorkerClaim,WorkerTaskState};
    let storage=b.storage.as_ref();let(task,fence,_)=fixture(storage).await;
    let id=published(&storage.admit_worker_dispatch(&intent(task,fence.clone())).await.unwrap());
    let held=storage.claim_worker_tasks("remote.work","worker",1).await.unwrap().pop().unwrap();
    let claim=WorkerClaim::new("worker",held.claim_epoch);
    let mut receipt=storage.get_effect_receipt(&fence.tenant_id,held.effect_id.unwrap()).await.unwrap().unwrap();
    receipt.provider_receipt_id=Some("original-provider".into());
    receipt.transition(EffectState::Committed,Utc::now()).unwrap();
    assert!(storage.cas_effect_receipt(&fence.tenant_id,receipt.id,EffectState::Dispatched,&receipt).await.unwrap());
    assert!(storage.complete_worker_task(id,&claim,&json!({"provider_receipt_id":"regenerated"})).await.is_err());
    let after=storage.get_worker_task(id).await.unwrap().unwrap();
    assert_eq!(after.state,WorkerTaskState::Claimed);assert!(after.output.is_none());
    assert!(storage.get_worker_accepted_outcome(Some(&fence.tenant_id),id,&claim).await.unwrap().is_none());
    assert_eq!(storage.get_effect_receipt(&fence.tenant_id,receipt.id).await.unwrap().unwrap(),receipt);
});

orch8_storage::backend_test!(async fn completion_refuses_effect_receipt_drift_without_accepting_output(b) {
    use orch8_types::worker::{WorkerClaim,WorkerTaskState};
    let storage=b.storage.as_ref();let(task,fence,_)=fixture(storage).await;
    let id=published(&storage.admit_worker_dispatch(&intent(task,fence.clone())).await.unwrap());
    let held=storage.claim_worker_tasks("remote.work","worker",1).await.unwrap().pop().unwrap();
    let claim=WorkerClaim::new("worker",held.claim_epoch);
    let mut receipt=storage.get_effect_receipt(&fence.tenant_id,held.effect_id.unwrap()).await.unwrap().unwrap();
    receipt.request_sha256="00".repeat(32);
    assert!(storage.cas_effect_receipt(&fence.tenant_id,receipt.id,EffectState::Dispatched,&receipt).await.unwrap());
    assert!(storage.complete_worker_task(id,&claim,&json!({"value":"new"})).await.is_err());
    assert_eq!(storage.get_worker_task(id).await.unwrap().unwrap().state,WorkerTaskState::Claimed);
    assert!(storage.get_worker_accepted_outcome(Some(&fence.tenant_id),id,&claim).await.unwrap().is_none());
    assert_eq!(storage.get_effect_receipt(&fence.tenant_id,receipt.id).await.unwrap().unwrap(),receipt);
});

orch8_storage::backend_test!(async fn application_ciphertext_is_preserved_as_an_opaque_worker_result(b) {
    use orch8_types::{encryption::FieldEncryptor,worker::WorkerClaim};
    let storage=b.storage.as_ref();let(task,fence,_)=fixture(storage).await;
    let id=published(&storage.admit_worker_dispatch(&intent(task,fence.clone())).await.unwrap());
    let held=storage.claim_worker_tasks("remote.work","worker",1).await.unwrap().pop().unwrap();
    let claim=WorkerClaim::new("worker",held.claim_epoch);
    let encryptor=FieldEncryptor::from_hex_key(orch8_storage::testing::backends::TEST_ENCRYPTION_KEY_HEX).unwrap();
    let application_output=encryptor.encrypt_value(&json!({"value":"APPLICATION_OPAQUE_VALUE"})).unwrap();
    assert!(storage.complete_worker_task(id,&claim,&application_output).await.unwrap());
    assert_eq!(storage.get_worker_accepted_outcome(Some(&fence.tenant_id),id,&claim).await.unwrap().unwrap().output,application_output,"storage interpreted an application's own ciphertext as its storage envelope");
    assert_eq!(storage.get_worker_task(id).await.unwrap().unwrap().output,Some(application_output));
});

orch8_storage::backend_test!(async fn accepted_result_fingerprint_survives_json_number_roundtrip(b) {
    use orch8_types::worker::WorkerClaim;
    let storage=b.storage.as_ref();let(task,fence,_)=fixture(storage).await;
    let id=published(&storage.admit_worker_dispatch(&intent(task,fence.clone())).await.unwrap());
    let held=storage.claim_worker_tasks("remote.work","worker",1).await.unwrap().pop().unwrap();
    let claim=WorkerClaim::new("worker",held.claim_epoch);
    let output=json!({"integer_float":1e18,"negative_zero":-0.0,"nested":[1e18,0.0]});
    assert!(storage.complete_worker_task(id,&claim,&output).await.unwrap());
    let accepted=storage.get_worker_accepted_outcome(Some(&fence.tenant_id),id,&claim).await.unwrap().unwrap();
    assert_eq!(accepted.output,output,"database numeric normalization changed the immutable accepted result");
});

orch8_storage::backend_test!(async fn outdated_worker_outcome_writer_is_rejected_by_storage(b) {
    use orch8_storage::testing::backends::RawPool;
    use orch8_types::worker::WorkerClaim;
    let storage=b.storage.as_ref();let(task,fence,_)=fixture(storage).await;
    let id=published(&storage.admit_worker_dispatch(&intent(task,fence.clone())).await.unwrap());
    let held=storage.claim_worker_tasks("remote.work","worker",1).await.unwrap().pop().unwrap();
    let claim=WorkerClaim::new("worker",held.claim_epoch);
    assert!(storage.complete_worker_task(id,&claim,&json!({"value":"original"})).await.unwrap());
    let receipt=storage.get_worker_accepted_outcome(Some(&fence.tenant_id),id,&claim).await.unwrap().unwrap();
    let mut legacy=serde_json::to_value(&receipt).unwrap();
    let candidate=Uuid::now_v7();legacy["task_id"]=json!(candidate);legacy["invocation"]["task_id"]=json!(candidate);
    let error=match &b.raw {
        RawPool::Sqlite(pool)=>sqlx::query("INSERT INTO worker_accepted_outcomes(task_id,tenant_id,instance_id,record) VALUES(?1,?2,?3,?4)").bind(candidate.to_string()).bind(fence.tenant_id.as_str()).bind(fence.instance_id.to_string()).bind(legacy.to_string()).execute(pool).await.unwrap_err(),
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>sqlx::query("INSERT INTO worker_accepted_outcomes(task_id,tenant_id,instance_id,record) VALUES($1,$2,$3,$4)").bind(candidate).bind(fence.tenant_id.as_str()).bind(fence.instance_id.into_uuid()).bind(legacy).execute(pool).await.unwrap_err(),
    };
    assert!(error.to_string().contains("requires json_text_v1 writer"));
    assert!(storage.get_worker_accepted_outcome(Some(&fence.tenant_id),candidate,&claim).await.unwrap().is_none());
});

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_codec_downgrade_refuses_records_requiring_the_new_reader() {
    use orch8_types::worker::WorkerClaim;
    let Some(b) = orch8_storage::testing::backends::TestBackend::postgres().await else {
        return;
    };
    let down = include_str!("../../migrations/down/135_worker_outcome_encoding.sql");
    let mut tx = b.pg_pool().begin().await.unwrap();
    sqlx::raw_sql(down).execute(&mut *tx).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../migrations/135_worker_outcome_encoding.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let (task, fence, _) = fixture(b.storage.as_ref()).await;
    let id = published(
        &b.storage
            .admit_worker_dispatch(&intent(task, fence.clone()))
            .await
            .unwrap(),
    );
    let held = b
        .storage
        .claim_worker_tasks("remote.work", "worker", 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let claim = WorkerClaim::new("worker", held.claim_epoch);
    assert!(
        b.storage
            .complete_worker_task(id, &claim, &json!({"value":1e18}))
            .await
            .unwrap()
    );
    let mut tx = b.pg_pool().begin().await.unwrap();
    let error = sqlx::raw_sql(down).execute(&mut *tx).await.unwrap_err();
    assert!(error.to_string().contains("require the current reader"));
    tx.rollback().await.unwrap();
    assert_eq!(
        b.storage
            .get_worker_accepted_outcome(Some(&fence.tenant_id), id, &claim)
            .await
            .unwrap()
            .unwrap()
            .output,
        json!({"value":1e18})
    );
}
