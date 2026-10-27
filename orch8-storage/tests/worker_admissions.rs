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
