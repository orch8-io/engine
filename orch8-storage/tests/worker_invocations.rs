//! Dispatch identity must outlive hot rows and distinguish loop activations.
use chrono::Utc;
use orch8_storage::{
    StorageBackend,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
    worker_invocation::{WorkerInvocation, WorkerInvocationTarget},
};
use orch8_types::{
    execution::{BlockType, ExecutionNode, NodeState},
    ids::{BlockId, ExecutionNodeId, TenantId},
    instance::InstanceState,
    worker::WorkerTask,
};
use serde_json::json;

async fn fixture(storage: &dyn StorageBackend) -> (WorkerTask, ExecutionNodeId) {
    let sequence = SequenceBuilder::new("worker-invocations")
        .name(&format!("worker-invocations-{}", uuid::Uuid::new_v4()))
        .step("work", "remote")
        .build();
    storage.create_sequence(&sequence).await.unwrap();
    let mut instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Waiting)
        .data(json!({"secret":"never-copy-into-binding"}))
        .build();
    instance.context.runtime.run_id = Some(uuid::Uuid::new_v4().to_string());
    instance.context.runtime.current_step = Some(BlockId::new("work"));
    instance.context.runtime.current_step_started_at = Some(Utc::now());
    storage.create_instance(&instance).await.unwrap();
    let node_id = ExecutionNodeId::new();
    storage
        .create_execution_node(&ExecutionNode {
            id: node_id,
            instance_id: instance.id,
            block_id: BlockId::new("work"),
            parent_id: None,
            block_type: BlockType::Step,
            branch_index: None,
            state: NodeState::Running,
            started_at: Some(Utc::now()),
            completed_at: None,
        })
        .await
        .unwrap();
    let task = serde_json::from_value(json!({"id":uuid::Uuid::new_v4(),"instance_id":instance.id,"block_id":"work","handler_name":"remote","attempt":0,"params":{"secret":"private-params"},"context":{"secret":"private-worker-context"},"created_at":Utc::now(),"state":"pending"})).unwrap();
    (task, node_id)
}
async fn binding(storage: &dyn StorageBackend, task: &WorkerTask) -> WorkerInvocation {
    storage
        .get_worker_invocation(&TenantId::unchecked("worker-invocations"), task.id)
        .await
        .unwrap()
        .expect("durable invocation binding")
}
fn activation(record: &WorkerInvocation) -> uuid::Uuid {
    let WorkerInvocationTarget::Tree { activation_id, .. } = record.target else {
        panic!("tree invocation was not bound")
    };
    activation_id
}

orch8_storage::backend_test!(async fn dispatch_binding_is_scoped_and_survives_native_and_task_gc(b) {
    let storage=b.storage.as_ref();let(task,node)=fixture(storage).await;storage.create_worker_task(&task).await.unwrap();
    let original=binding(storage,&task).await;assert_eq!(original.task_id,task.id);assert_eq!(original.instance_id,task.instance_id);assert_eq!(original.block_id,task.block_id);
    assert!(matches!(original.target,WorkerInvocationTarget::Tree{node_id,..} if node_id==node));
    let bytes=serde_json::to_string(&original).unwrap();for secret in ["never-copy-into-binding","private-params","private-worker-context"] {assert!(!bytes.contains(secret));}
    assert!(storage.get_worker_invocation(&TenantId::unchecked("foreign"),task.id).await.unwrap().is_none());
    storage.delete_worker_task(task.id).await.unwrap();storage.update_instance_state(task.instance_id,InstanceState::Cancelled,None).await.unwrap();assert_eq!(storage.purge_terminal_instances(&[task.instance_id]).await.unwrap(),1);
    assert!(storage.get_instance(task.instance_id).await.unwrap().is_none());assert_eq!(binding(storage,&task).await,original);
});

orch8_storage::backend_test!(async fn reused_node_rotates_activation_but_waiting_resume_keeps_it(b) {
    let storage=b.storage.as_ref();let(task,node)=fixture(storage).await;storage.create_worker_task(&task).await.unwrap();let first=binding(storage,&task).await;
    let started=storage.get_execution_tree(task.instance_id).await.unwrap()[0].started_at;
    storage.delete_worker_task(task.id).await.unwrap();storage.update_node_state(node,NodeState::Waiting).await.unwrap();storage.update_node_state(node,NodeState::Running).await.unwrap();
    let mut resumed=task.clone();resumed.id=uuid::Uuid::new_v4();storage.create_worker_task(&resumed).await.unwrap();assert_eq!(activation(&binding(storage,&resumed).await),activation(&first),"resuming the same activity must not change its identity");
    storage.delete_worker_task(resumed.id).await.unwrap();storage.update_node_state(node,NodeState::Pending).await.unwrap();storage.update_node_state(node,NodeState::Running).await.unwrap();
    let mut next=task.clone();next.id=uuid::Uuid::new_v4();storage.create_worker_task(&next).await.unwrap();let second=binding(storage,&next).await;
    assert_ne!(activation(&first),activation(&second),"loop iteration reused its previous activation token");assert_eq!(storage.get_execution_tree(task.instance_id).await.unwrap()[0].started_at,started,"timestamps deliberately survive reset and cannot be the invocation nonce");assert_eq!(binding(storage,&task).await,first);
});

orch8_storage::backend_test!(async fn pending_dispatch_upsert_binds_actual_row_without_rewriting_history(b) {
    let storage=b.storage.as_ref();let(task,_)=fixture(storage).await;storage.create_worker_task(&task).await.unwrap();let first=binding(storage,&task).await;
    let mut duplicate=task.clone();duplicate.id=uuid::Uuid::new_v4();storage.create_worker_task(&duplicate).await.unwrap();assert_eq!(binding(storage,&task).await,first);
    assert!(storage.get_worker_invocation(&first.tenant_id,duplicate.id).await.unwrap().is_none(),"upsert created evidence for an id that was never enqueued");
});

orch8_storage::backend_test!(async fn dispatch_invocation_evidence_cannot_be_rewritten_or_deleted(b) {
    use orch8_storage::testing::backends::RawPool;
    let(task,_)=fixture(b.storage.as_ref()).await;b.storage.create_worker_task(&task).await.unwrap();
    match &b.raw {
        RawPool::Sqlite(pool)=>{assert!(sqlx::query("UPDATE worker_task_invocations SET record=record WHERE task_id=?1").bind(task.id.to_string()).execute(pool).await.is_err());assert!(sqlx::query("DELETE FROM worker_task_invocations WHERE task_id=?1").bind(task.id.to_string()).execute(pool).await.is_err());},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{assert!(sqlx::query("UPDATE worker_task_invocations SET record=record WHERE task_id=$1").bind(task.id).execute(pool).await.is_err());assert!(sqlx::query("DELETE FROM worker_task_invocations WHERE task_id=$1").bind(task.id).execute(pool).await.is_err());},
    }
    binding(b.storage.as_ref(),&task).await;
});

orch8_storage::backend_test!(async fn reserved_pending_activation_is_stable_through_retry_and_first_start(b) {
    let storage=b.storage.as_ref();let(task,node)=fixture(storage).await;
    storage.update_node_state(node,NodeState::Pending).await.unwrap();storage.create_worker_task(&task).await.unwrap();let reserved=binding(storage,&task).await;
    assert!(matches!(reserved.target,WorkerInvocationTarget::Tree{..}));
    storage.delete_worker_task(task.id).await.unwrap();storage.update_node_state(node,NodeState::Pending).await.unwrap();storage.update_node_state(node,NodeState::Running).await.unwrap();
    let mut retry=task.clone();retry.id=uuid::Uuid::new_v4();storage.create_worker_task(&retry).await.unwrap();assert_eq!(activation(&binding(storage,&retry).await),activation(&reserved),"idempotent reset or activation replaced the already reserved invocation");
});

orch8_storage::backend_test!(async fn missing_run_or_ambiguous_node_is_retained_as_unbound_evidence(b) {
    let storage=b.storage.as_ref();let(task,_)=fixture(storage).await;
    let mut instance=storage.get_instance(task.instance_id).await.unwrap().unwrap();instance.context.runtime.run_id=None;storage.update_instance_context(instance.id,&instance.context).await.unwrap();
    storage.create_worker_task(&task).await.unwrap();assert!(matches!(binding(storage,&task).await.target,WorkerInvocationTarget::Unbound{..}));
    storage.delete_worker_task(task.id).await.unwrap();instance.context.runtime.run_id=Some(uuid::Uuid::new_v4().to_string());storage.update_instance_context(instance.id,&instance.context).await.unwrap();
    storage.create_execution_node(&ExecutionNode {id:ExecutionNodeId::new(),instance_id:instance.id,block_id:task.block_id.clone(),parent_id:None,block_type:BlockType::Step,branch_index:None,state:NodeState::Waiting,started_at:Some(Utc::now()),completed_at:None}).await.unwrap();
    let mut ambiguous=task.clone();ambiguous.id=uuid::Uuid::new_v4();storage.create_worker_task(&ambiguous).await.unwrap();assert!(matches!(binding(storage,&ambiguous).await.target,WorkerInvocationTarget::Unbound{..}),"a block id selected an arbitrary node");
});

orch8_storage::backend_test!(async fn continuity_binding_retains_exact_identity_and_runtime_owner(b) {
    use orch8_types::continuity::{ContinuityExecution,ContinuityId,ExecutionEpoch,OwnershipState,RuntimeId};
    let storage=b.storage.as_ref();let(mut task,_)=fixture(storage).await;
    let owner=ContinuityExecution {continuity_id:ContinuityId::new(),tenant_id:TenantId::unchecked("worker-invocations"),current_instance_id:task.instance_id,owner_runtime_id:RuntimeId::new(),epoch:ExecutionEpoch::initial(),state:OwnershipState::Owned,updated_at:Utc::now()};storage.create_continuity_execution(&owner).await.unwrap();task.continuity_epoch=Some(owner.epoch.get());
    storage.create_worker_task(&task).await.unwrap();let record=binding(storage,&task).await;let continuity=record.continuity.as_ref().unwrap();assert_eq!(continuity.continuity_id,owner.continuity_id);assert_eq!(continuity.epoch,owner.epoch.get());assert_eq!(continuity.owner_runtime_id,owner.owner_runtime_id);
    storage.delete_worker_task(task.id).await.unwrap();let mut next=owner.clone();next.epoch=owner.epoch.checked_next().unwrap();next.owner_runtime_id=RuntimeId::new();next.updated_at=Utc::now();assert!(storage.cas_continuity_owner(&owner.tenant_id,owner.continuity_id,owner.epoch,owner.owner_runtime_id,&next).await.unwrap());
    let mut stale=task.clone();stale.id=uuid::Uuid::new_v4();if storage.create_worker_task(&stale).await.is_ok(){let quarantined=binding(storage,&stale).await;assert!(matches!(quarantined.target,WorkerInvocationTarget::Unbound{..}));assert!(quarantined.continuity.is_none(),"dispatch invented an owner identity for the old epoch");}
    assert_eq!(binding(storage,&task).await,record);
});

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_invocation_downgrade_refuses_retained_evidence_but_empty_roundtrip_works() {
    let Some(b) = orch8_storage::testing::backends::TestBackend::postgres().await else {
        return;
    };
    let down = include_str!("../../migrations/down/132_worker_invocation_bindings.sql");
    let mut tx = b.pg_pool().begin().await.unwrap();
    sqlx::raw_sql(down).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../migrations/132_worker_invocation_bindings.sql"
    ))
    .execute(b.pg_pool())
    .await
    .unwrap();
    let (task, _) = fixture(b.storage.as_ref()).await;
    b.storage.create_worker_task(&task).await.unwrap();
    let original = binding(b.storage.as_ref(), &task).await;
    b.storage.delete_worker_task(task.id).await.unwrap();
    let mut tx = b.pg_pool().begin().await.unwrap();
    assert!(sqlx::raw_sql(down).execute(&mut *tx).await.is_err());
    tx.rollback().await.unwrap();
    assert_eq!(
        binding(b.storage.as_ref(), &task).await,
        original,
        "failed downgrade lost the retained dispatch fence"
    );
}

#[tokio::test]
async fn sqlite_activation_upgrade_is_atomic_and_completed_reopen_keeps_tokens() {
    use orch8_storage::{WorkerStore, sqlite::SqliteStorage};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("worker-activation.sqlite");
    let path = path.to_str().unwrap();
    let storage = SqliteStorage::file(path).await.unwrap();
    let (task, _) = fixture(&storage).await;
    sqlx::raw_sql("DROP TRIGGER execution_activation_not_null; DELETE FROM schema_versions WHERE version=63; UPDATE execution_tree SET activation_id=NULL;
        CREATE TRIGGER fail_activation_upgrade BEFORE UPDATE OF activation_id ON execution_tree WHEN OLD.activation_id IS NULL BEGIN SELECT RAISE(ABORT,'injected activation upgrade failure'); END;")
        .execute(storage.pool()).await.unwrap();
    storage.pool().close().await;
    drop(storage);
    assert!(
        SqliteStorage::file(path).await.is_err(),
        "injected backfill failure was ignored"
    );
    let raw = sqlx::SqlitePool::connect(&format!("sqlite://{path}"))
        .await
        .unwrap();
    let complete: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM schema_versions WHERE version=63)")
            .fetch_one(&raw)
            .await
            .unwrap();
    assert!(
        !complete,
        "failed token backfill published schema completion"
    );
    let missing: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM execution_tree WHERE activation_id IS NULL)",
    )
    .fetch_one(&raw)
    .await
    .unwrap();
    assert!(missing);
    sqlx::query("DROP TRIGGER fail_activation_upgrade")
        .execute(&raw)
        .await
        .unwrap();
    raw.close().await;
    let storage = SqliteStorage::file(path).await.unwrap();
    storage.create_worker_task(&task).await.unwrap();
    let original = binding(&storage, &task).await;
    assert!(matches!(
        original.target,
        WorkerInvocationTarget::Tree { .. }
    ));
    storage.pool().close().await;
    drop(storage);
    let reopened = SqliteStorage::file(path).await.unwrap();
    assert_eq!(binding(&reopened, &task).await, original);
    let live_token: String = sqlx::query_scalar(
        "SELECT activation_id FROM execution_tree WHERE instance_id=?1 AND block_id=?2",
    )
    .bind(task.instance_id.to_string())
    .bind(task.block_id.as_str())
    .fetch_one(reopened.pool())
    .await
    .unwrap();
    assert_eq!(
        uuid::Uuid::parse_str(&live_token).unwrap(),
        activation(&original),
        "reopen changed the live token behind its retained binding"
    );
    reopened.pool().close().await;
}

async fn native_activation(
    b: &orch8_storage::testing::backends::TestBackend,
    node: ExecutionNodeId,
) -> uuid::Uuid {
    use orch8_storage::testing::backends::RawPool;
    match &b.raw {
        RawPool::Sqlite(pool) => {
            let value: String =
                sqlx::query_scalar("SELECT activation_id FROM execution_tree WHERE id=?1")
                    .bind(node.to_string())
                    .fetch_one(pool)
                    .await
                    .unwrap();
            uuid::Uuid::parse_str(&value).unwrap()
        }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => {
            sqlx::query_scalar("SELECT activation_id FROM execution_tree WHERE id=$1")
                .bind(node.into_uuid())
                .fetch_one(pool)
                .await
                .unwrap()
        }
    }
}

async fn dispatch_fence(
    b: &orch8_storage::testing::backends::TestBackend,
    task: &WorkerTask,
    node: ExecutionNodeId,
) -> orch8_storage::worker_invocation::WorkerDispatchFence {
    b.storage
        .update_instance_state(
            task.instance_id,
            InstanceState::Scheduled,
            Some(Utc::now() - chrono::Duration::seconds(1)),
        )
        .await
        .unwrap();
    let claimed = b
        .storage
        .claim_due_instances(Utc::now(), 1, 0)
        .await
        .unwrap();
    assert_eq!(claimed[0].id, task.instance_id);
    let instance = b
        .storage
        .get_instance(task.instance_id)
        .await
        .unwrap()
        .unwrap();
    b.storage
        .prepare_worker_dispatch(
            &instance,
            &task.block_id,
            &orch8_storage::worker_invocation::WorkerDispatchSelection::Tree { node_id: node },
        )
        .await
        .unwrap()
        .expect("claimed dispatch source")
}

orch8_storage::backend_test!(async fn bound_dispatch_atomically_waits_node_and_recovers_actual_queued_id(b) {
    let(task,node)=fixture(b.storage.as_ref()).await;let fence=dispatch_fence(&b,&task,node).await;
    assert_eq!(b.storage.create_worker_task_bound(&task,&fence).await.unwrap(),Some(task.id));
    assert_eq!(b.storage.get_execution_tree(task.instance_id).await.unwrap()[0].state,NodeState::Waiting);
    let original=binding(b.storage.as_ref(),&task).await;assert_eq!(original.run_id.as_deref(),Some(fence.run_id.as_str()));
    let mut duplicate=task.clone();duplicate.id=uuid::Uuid::new_v4();assert_eq!(b.storage.create_worker_task_bound(&duplicate,&fence).await.unwrap(),Some(task.id));
    assert_eq!(binding(b.storage.as_ref(),&task).await,original);assert!(b.storage.get_worker_task(duplicate.id).await.unwrap().is_none());
    assert_eq!(b.storage.get_worker_task(task.id).await.unwrap().unwrap().params,task.params);
});

orch8_storage::backend_test!(async fn bound_dispatch_refuses_stale_claim_run_sequence_and_activation_without_mutation(b) {
    use orch8_storage::worker_invocation::WorkerDispatchTarget;
    let(task,node)=fixture(b.storage.as_ref()).await;let fence=dispatch_fence(&b,&task,node).await;
    let before=serde_json::to_value(b.storage.get_instance(task.instance_id).await.unwrap().unwrap()).unwrap();
    for mutation in 0..6 {
        let mut wrong=fence.clone();match mutation {0=>wrong.claim_token+=chrono::Duration::seconds(1),1=>wrong.run_id=uuid::Uuid::new_v4().to_string(),2=>wrong.sequence_id=orch8_types::ids::SequenceId::new(),3=>wrong.tenant_id=TenantId::unchecked("foreign"),4=>wrong.target=WorkerDispatchTarget::Tree{node_id:node,activation_id:uuid::Uuid::new_v4()},_=>wrong.target=WorkerDispatchTarget::Tree{node_id:ExecutionNodeId::new(),activation_id:native_activation(&b,node).await}}
        assert!(b.storage.create_worker_task_bound(&task,&wrong).await.unwrap().is_none(),"stale fence {mutation} was accepted");
        assert!(b.storage.get_worker_task(task.id).await.unwrap().is_none());assert!(b.storage.get_worker_invocation(&fence.tenant_id,task.id).await.unwrap().is_none());
        assert_eq!(serde_json::to_value(b.storage.get_instance(task.instance_id).await.unwrap().unwrap()).unwrap(),before);
        assert_eq!(b.storage.get_execution_tree(task.instance_id).await.unwrap()[0].state,NodeState::Running);
    }
});

orch8_storage::backend_test!(async fn bound_dispatch_cannot_attach_old_preparation_to_reused_node_in_same_claim(b) {
    use orch8_storage::worker_invocation::WorkerDispatchTarget;
    let(task,node)=fixture(b.storage.as_ref()).await;let original=dispatch_fence(&b,&task,node).await;
    b.storage.update_node_state(node,NodeState::Pending).await.unwrap();let next=native_activation(&b,node).await;
    assert!(b.storage.create_worker_task_bound(&task,&original).await.unwrap().is_none());
    assert_eq!(b.storage.get_instance(task.instance_id).await.unwrap().unwrap().claimed_at,Some(original.claim_token),"test must retain the same parent claim");
    let mut refreshed=original.clone();refreshed.target=WorkerDispatchTarget::Tree{node_id:node,activation_id:next};assert_eq!(b.storage.create_worker_task_bound(&task,&refreshed).await.unwrap(),Some(task.id));
    assert_eq!(activation(&binding(b.storage.as_ref(),&task).await),next);
});

orch8_storage::backend_test!(async fn bound_flat_dispatch_preserves_context_and_cannot_reset_established_timeout(b) {
    use orch8_storage::worker_invocation::WorkerDispatchTarget;
    let(task,node)=fixture(b.storage.as_ref()).await;let _=dispatch_fence(&b,&task,node).await;
    b.storage.delete_execution_tree(task.instance_id).await.unwrap();let mut instance=b.storage.get_instance(task.instance_id).await.unwrap().unwrap();
    let start=instance.context.runtime.current_step_started_at.unwrap();
    instance.context.runtime.current_step=None;instance.context.runtime.current_step_started_at=None;instance.context.data["concurrent"] = json!("preserve me");b.storage.update_instance_context(instance.id,&instance.context).await.unwrap();
    let mut fence=b.storage.prepare_worker_dispatch(&instance,&task.block_id,&orch8_storage::worker_invocation::WorkerDispatchSelection::Flat{step_started_at:start}).await.unwrap().expect("claimed flat dispatch source");
    assert_eq!(b.storage.create_worker_task_bound(&task,&fence).await.unwrap(),Some(task.id));let current=b.storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(current.context.data,instance.context.data);assert_eq!(current.context.runtime.current_step,Some(task.block_id.clone()));assert_eq!(current.context.runtime.current_step_started_at,Some(start));assert!(matches!(binding(b.storage.as_ref(),&task).await.target,WorkerInvocationTarget::Flat{step_started_at} if step_started_at==start));
    fence.target=WorkerDispatchTarget::Flat{step_started_at:start+chrono::Duration::seconds(1)};let mut replacement=task.clone();replacement.id=uuid::Uuid::new_v4();assert!(b.storage.create_worker_task_bound(&replacement,&fence).await.unwrap().is_none());assert_eq!(b.storage.get_instance(instance.id).await.unwrap().unwrap().context.runtime.current_step_started_at,Some(start));
});

orch8_storage::backend_test!(async fn bound_dispatch_binding_failure_rolls_back_queue_and_node_suspension(b) {
    use orch8_storage::testing::backends::RawPool;
    let(task,node)=fixture(b.storage.as_ref()).await;let fence=dispatch_fence(&b,&task,node).await;
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("CREATE TRIGGER fail_bound_binding BEFORE INSERT ON worker_task_invocations BEGIN SELECT RAISE(ABORT,'injected binding failure'); END;").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION fail_bound_binding() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected binding failure'; END $$; CREATE TRIGGER fail_bound_binding BEFORE INSERT ON worker_task_invocations FOR EACH ROW EXECUTE FUNCTION fail_bound_binding();").execute(pool).await.unwrap();},
    }
    assert!(b.storage.create_worker_task_bound(&task,&fence).await.is_err());assert!(b.storage.get_worker_task(task.id).await.unwrap().is_none());assert_eq!(b.storage.get_execution_tree(task.instance_id).await.unwrap()[0].state,NodeState::Running);
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::query("DROP TRIGGER fail_bound_binding").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("DROP TRIGGER fail_bound_binding ON worker_task_invocations; DROP FUNCTION fail_bound_binding();").execute(pool).await.unwrap();},
    }
    assert_eq!(b.storage.create_worker_task_bound(&task,&fence).await.unwrap(),Some(task.id));
});

orch8_storage::backend_test!(async fn bound_dispatch_refuses_conflicting_handler_without_publishing_candidate(b) {
    let (task,node)=fixture(b.storage.as_ref()).await;
    let fence=dispatch_fence(&b,&task,node).await;
    assert_eq!(b.storage.create_worker_task_bound(&task,&fence).await.unwrap(),Some(task.id));
    let original=binding(b.storage.as_ref(),&task).await;
    let mut conflicting=task.clone();conflicting.id=uuid::Uuid::new_v4();conflicting.handler_name="different-provider".into();
    assert!(b.storage.create_worker_task_bound(&conflicting,&fence).await.unwrap().is_none());
    assert!(b.storage.get_worker_task(conflicting.id).await.unwrap().is_none());
    assert_eq!(b.storage.get_worker_task(task.id).await.unwrap().unwrap().handler_name,task.handler_name);
    assert_eq!(binding(b.storage.as_ref(),&task).await,original);
});

orch8_storage::backend_test!(async fn bound_dispatch_requires_exact_continuity_identity_runtime_and_epoch(b) {
    use orch8_types::continuity::{ContinuityExecution,ContinuityId,ExecutionEpoch,OwnershipState,RuntimeId};
    let (mut task,node)=fixture(b.storage.as_ref()).await;
    let owner=ContinuityExecution {continuity_id:ContinuityId::new(),tenant_id:TenantId::unchecked("worker-invocations"),current_instance_id:task.instance_id,owner_runtime_id:RuntimeId::new(),epoch:ExecutionEpoch::initial(),state:OwnershipState::Owned,updated_at:Utc::now()};
    b.storage.create_continuity_execution(&owner).await.unwrap();task.continuity_epoch=Some(owner.epoch.get());
    let fence=dispatch_fence(&b,&task,node).await;
    for mutation in 0..3 {
        let mut wrong=fence.clone();let scope=wrong.continuity.as_mut().unwrap();let mut candidate=task.clone();
        match mutation {0=>scope.continuity_id=ContinuityId::new(),1=>scope.owner_runtime_id=RuntimeId::new(),_=>{scope.epoch+=1;candidate.continuity_epoch=Some(scope.epoch);}}
        assert!(b.storage.create_worker_task_bound(&candidate,&wrong).await.unwrap().is_none());
        assert!(b.storage.get_worker_task(task.id).await.unwrap().is_none());
        assert_eq!(b.storage.get_execution_tree(task.instance_id).await.unwrap()[0].state,NodeState::Running);
    }
    assert_eq!(b.storage.create_worker_task_bound(&task,&fence).await.unwrap(),Some(task.id));
    let original=binding(b.storage.as_ref(),&task).await;
    assert_eq!(original.continuity.as_ref().unwrap().owner_runtime_id,owner.owner_runtime_id);
    b.storage.delete_worker_task(task.id).await.unwrap();
    let mut next=owner.clone();next.epoch=owner.epoch.checked_next().unwrap();next.owner_runtime_id=RuntimeId::new();next.updated_at=Utc::now();
    assert!(b.storage.cas_continuity_owner(&owner.tenant_id,owner.continuity_id,owner.epoch,owner.owner_runtime_id,&next).await.unwrap());
    let mut stale=task.clone();stale.id=uuid::Uuid::new_v4();
    assert!(b.storage.create_worker_task_bound(&stale,&fence).await.unwrap().is_none());
    assert!(b.storage.get_worker_task(stale.id).await.unwrap().is_none());
    assert_eq!(binding(b.storage.as_ref(),&task).await,original);
});

orch8_storage::backend_test!(async fn preparation_snapshot_refuses_stale_or_foreign_source_and_terminal_node(b) {
    use orch8_storage::worker_invocation::WorkerDispatchSelection;
    let (task,node)=fixture(b.storage.as_ref()).await;
    let fence=dispatch_fence(&b,&task,node).await;
    let instance=b.storage.get_instance(task.instance_id).await.unwrap().unwrap();
    let selection=WorkerDispatchSelection::Tree { node_id: node };
    for mutation in 0..6 {
        let mut wrong=instance.clone();
        match mutation {
            0=>wrong.claimed_at=None,
            1=>wrong.claimed_at=Some(fence.claim_token+chrono::Duration::seconds(1)),
            2=>wrong.context.runtime.run_id=Some("different-run".into()),
            3=>wrong.sequence_id=orch8_types::ids::SequenceId::new(),
            4=>wrong.tenant_id=TenantId::unchecked("foreign"),
            _=>wrong.context.runtime.run_id=None,
        }
        assert!(b.storage.prepare_worker_dispatch(&wrong,&task.block_id,&selection).await.unwrap().is_none(),"stale preparation {mutation} was accepted");
    }
    assert!(b.storage.prepare_worker_dispatch(&instance,&BlockId::new("foreign-block"),&selection).await.unwrap().is_none());
    assert!(b.storage.prepare_worker_dispatch(&instance,&task.block_id,&WorkerDispatchSelection::Tree{node_id:ExecutionNodeId::new()}).await.unwrap().is_none());
    assert!(b.storage.prepare_worker_dispatch(&instance,&task.block_id,&WorkerDispatchSelection::Flat{step_started_at:Utc::now()}).await.unwrap().is_none(),"tree execution was treated as flat");
    b.storage.update_node_state(node,NodeState::Completed).await.unwrap();
    assert!(b.storage.prepare_worker_dispatch(&instance,&task.block_id,&selection).await.unwrap().is_none());
    assert!(b.storage.get_worker_task(task.id).await.unwrap().is_none());
});

orch8_storage::backend_test!(async fn prepared_flat_dispatch_cannot_be_retargeted_to_another_block(b) {
    use orch8_storage::worker_invocation::WorkerDispatchSelection;
    let(task,node)=fixture(b.storage.as_ref()).await;let _=dispatch_fence(&b,&task,node).await;
    b.storage.delete_execution_tree(task.instance_id).await.unwrap();
    let instance=b.storage.get_instance(task.instance_id).await.unwrap().unwrap();
    let fence=b.storage.prepare_worker_dispatch(&instance,&task.block_id,&WorkerDispatchSelection::Flat{step_started_at:instance.context.runtime.current_step_started_at.unwrap()}).await.unwrap().unwrap();
    let mut wrong=task.clone();wrong.block_id=BlockId::new("different-block");
    let outcome=b.storage.create_worker_task_bound(&wrong,&fence).await;
    assert!(matches!(outcome,Err(orch8_types::error::StorageError::Constraint(_))),"a preparation fence was reused for another block: {outcome:?}");
    assert!(b.storage.get_worker_task(task.id).await.unwrap().is_none());
    assert_eq!(b.storage.get_instance(task.instance_id).await.unwrap().unwrap().context.runtime.current_step,Some(task.block_id));
});

fn retry_resolution(
    task: &WorkerTask,
    node: ExecutionNodeId,
) -> orch8_types::worker::WorkerTaskResolution {
    use orch8_types::worker::{
        WorkerAttemptEventKind, WorkerTaskResolution, WorkerTaskResolutionAction,
    };
    let mut replacement = task.clone();
    replacement.id = uuid::Uuid::new_v4();
    replacement.attempt += 1;
    WorkerTaskResolution {
        task_id: task.id,
        instance_id: task.instance_id,
        expected_state: task.state,
        expected_claim_epoch: task.claim_epoch,
        expected_worker_id: None,
        holder_worker_id: None,
        event: WorkerAttemptEventKind::TimedOut,
        reason: "retry binding regression".into(),
        retryable: true,
        action: WorkerTaskResolutionAction::Retry {
            retry_task: Box::new(replacement),
            retry_output: Some(Box::new(orch8_types::output::BlockOutput {
                id: uuid::Uuid::now_v7(),
                instance_id: task.instance_id,
                block_id: task.block_id.clone(),
                output: json!({"_retry_marker":true,"error":"private retry detail"}),
                output_ref: Some("__retry__".into()),
                output_size: 64,
                attempt: task.attempt,
                created_at: Utc::now(),
            })),
            node_id: Some(node),
            fire_at: Utc::now(),
        },
    }
}

orch8_storage::backend_test!(async fn retry_cannot_reset_a_reused_node_from_an_old_worker_invocation(b) {
    use orch8_types::worker::WorkerTaskResolutionAction;
    let(task,node)=fixture(b.storage.as_ref()).await;b.storage.create_worker_task(&task).await.unwrap();
    let original=binding(b.storage.as_ref(),&task).await;
    b.storage.update_node_state(node,NodeState::Pending).await.unwrap();b.storage.update_node_state(node,NodeState::Running).await.unwrap();
    let current=native_activation(&b,node).await;assert_ne!(current,activation(&original));
    let request=retry_resolution(&task,node);
    assert!(!b.storage.resolve_worker_task(&request).await.unwrap(),"old retry reset the new invocation");
    let WorkerTaskResolutionAction::Retry{retry_task,..}=&request.action else {unreachable!()};
    assert!(b.storage.retry_worker_task(task.id,retry_task,Some(node),task.instance_id,Utc::now()).await.is_err());
    assert!(b.storage.get_worker_task(task.id).await.unwrap().is_some());assert!(b.storage.get_worker_task(retry_task.id).await.unwrap().is_none());
    assert_eq!(b.storage.get_execution_tree(task.instance_id).await.unwrap()[0].state,NodeState::Running);
    assert_eq!(native_activation(&b,node).await,current);assert_eq!(binding(b.storage.as_ref(),&task).await,original);
    assert!(b.storage.list_worker_task_attempt_events(task.id,10).await.unwrap().is_empty());
    assert!(b.storage.get_block_output(task.instance_id,&task.block_id).await.unwrap().is_none());
});

orch8_storage::backend_test!(async fn retry_refuses_changed_run_and_unknown_legacy_source_without_certifying_it(b) {
    let(task,node)=fixture(b.storage.as_ref()).await;b.storage.create_worker_task(&task).await.unwrap();
    let mut instance=b.storage.get_instance(task.instance_id).await.unwrap().unwrap();instance.context.runtime.run_id=Some(uuid::Uuid::new_v4().to_string());b.storage.update_instance_context(instance.id,&instance.context).await.unwrap();
    assert!(!b.storage.resolve_worker_task(&retry_resolution(&task,node)).await.unwrap(),"old retry attached to a new run");
    b.storage.delete_worker_task(task.id).await.unwrap();instance.context.runtime.run_id=None;b.storage.update_instance_context(instance.id,&instance.context).await.unwrap();
    let mut unknown=task.clone();unknown.id=uuid::Uuid::new_v4();b.storage.create_worker_task(&unknown).await.unwrap();
    assert!(matches!(binding(b.storage.as_ref(),&unknown).await.target,WorkerInvocationTarget::Unbound{..}));
    assert!(!b.storage.resolve_worker_task(&retry_resolution(&unknown,node)).await.unwrap(),"unknown legacy target was silently certified");
    assert!(b.storage.get_worker_task(unknown.id).await.unwrap().is_some());
    assert_eq!(b.storage.get_execution_tree(task.instance_id).await.unwrap()[0].state,NodeState::Running);
});

orch8_storage::backend_test!(async fn retry_reserves_the_new_activation_and_keeps_it_unclaimable_until_bound_dispatch(b) {
    use orch8_types::worker::WorkerTaskResolutionAction;
    let(task,node)=fixture(b.storage.as_ref()).await;b.storage.create_worker_task(&task).await.unwrap();let original=binding(b.storage.as_ref(),&task).await;
    let request=retry_resolution(&task,node);let WorkerTaskResolutionAction::Retry{retry_task,..}=&request.action else {unreachable!()};
    assert!(b.storage.resolve_worker_task(&request).await.unwrap());
    let reserved=binding(b.storage.as_ref(),retry_task).await;
    assert_ne!(activation(&reserved),activation(&original));assert_eq!(activation(&reserved),native_activation(&b,node).await);
    assert_eq!(reserved.run_id,original.run_id);assert_eq!(reserved.sequence_id,original.sequence_id);assert_eq!(reserved.continuity,original.continuity);
    assert!(b.storage.claim_worker_tasks("remote","too-early",1).await.unwrap().is_empty());
    let fence=dispatch_fence(&b,retry_task,node).await;let mut candidate=retry_task.as_ref().clone();candidate.id=uuid::Uuid::new_v4();
    assert_eq!(b.storage.create_worker_task_bound(&candidate,&fence).await.unwrap(),Some(retry_task.id));
    assert_eq!(binding(b.storage.as_ref(),retry_task).await,reserved);assert_eq!(binding(b.storage.as_ref(),&task).await,original);
    let claimed=b.storage.claim_worker_tasks("remote","ready",1).await.unwrap();assert_eq!(claimed.len(),1);assert_eq!(claimed[0].id,retry_task.id);
});

orch8_storage::backend_test!(async fn retry_output_failure_rolls_back_task_node_binding_and_attempt_evidence(b) {
    use orch8_storage::testing::backends::RawPool;
    use orch8_types::worker::WorkerTaskResolutionAction;
    let(task,node)=fixture(b.storage.as_ref()).await;b.storage.create_worker_task(&task).await.unwrap();let original=binding(b.storage.as_ref(),&task).await;
    let request=retry_resolution(&task,node);let WorkerTaskResolutionAction::Retry{retry_task,..}=&request.action else {unreachable!()};
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("CREATE TRIGGER fail_retry_output BEFORE INSERT ON block_outputs BEGIN SELECT RAISE(ABORT,'injected retry output failure'); END;").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION fail_retry_output() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected retry output failure'; END $$; CREATE TRIGGER fail_retry_output BEFORE INSERT ON block_outputs FOR EACH ROW EXECUTE FUNCTION fail_retry_output();").execute(pool).await.unwrap();},
    }
    assert!(b.storage.resolve_worker_task(&request).await.is_err());
    assert!(b.storage.get_worker_task(task.id).await.unwrap().is_some());assert!(b.storage.get_worker_task(retry_task.id).await.unwrap().is_none());
    assert!(b.storage.get_worker_invocation(&original.tenant_id,retry_task.id).await.unwrap().is_none());assert!(b.storage.get_block_output(task.instance_id,&task.block_id).await.unwrap().is_none());
    assert_eq!(native_activation(&b,node).await,activation(&original));assert_eq!(b.storage.get_execution_tree(task.instance_id).await.unwrap()[0].state,NodeState::Running);
    assert!(b.storage.list_worker_task_attempt_events(task.id,10).await.unwrap().is_empty());
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::query("DROP TRIGGER fail_retry_output").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("DROP TRIGGER fail_retry_output ON block_outputs; DROP FUNCTION fail_retry_output();").execute(pool).await.unwrap();},
    }
    assert!(b.storage.resolve_worker_task(&request).await.unwrap());
    let output=b.storage.get_block_output(task.instance_id,&task.block_id).await.unwrap().unwrap();assert_eq!(output.output["error"],json!("private retry detail"));
    if b.kind.is_encrypted() {
        let raw=match &b.raw {
            RawPool::Sqlite(pool)=>sqlx::query_scalar::<_,String>("SELECT output FROM block_outputs WHERE id=?1").bind(output.id.to_string()).fetch_one(pool).await.unwrap(),
            #[cfg(feature="postgres")]
            RawPool::Postgres(pool)=>sqlx::query_scalar::<_,serde_json::Value>("SELECT output FROM block_outputs WHERE id=$1").bind(output.id).fetch_one(pool).await.unwrap().to_string(),
        };
        assert!(!raw.contains("private retry detail"),"transactional retry marker was stored as plaintext");
    }
});

#[cfg(feature = "postgres")]
#[tokio::test]
async fn owner_relocation_waits_for_target_namespace_before_locking_the_owner() {
    use orch8_types::continuity::{
        ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState, RuntimeId,
    };
    let Some(b) = orch8_storage::testing::backends::TestBackend::postgres().await else {
        return;
    };
    let (task, _) = fixture(b.storage.as_ref()).await;
    let mut target = b
        .storage
        .get_instance(task.instance_id)
        .await
        .unwrap()
        .unwrap();
    target.id = orch8_types::ids::InstanceId::new();
    b.storage.create_instance(&target).await.unwrap();
    let owner = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: target.tenant_id.clone(),
        current_instance_id: task.instance_id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    b.storage.create_continuity_execution(&owner).await.unwrap();
    let mut next = owner.clone();
    next.current_instance_id = target.id;
    next.epoch = owner.epoch.checked_next().unwrap();
    next.updated_at = Utc::now();
    let mut held = b.pg_pool().begin().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *held)
        .await
        .unwrap();
    let key = serde_json::to_string(&(
        "orch8-continuity-instance",
        target.tenant_id.as_str(),
        target.id,
    ))
    .unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(key)
        .execute(&mut *held)
        .await
        .unwrap();
    let storage = b.storage.clone();
    let before = owner.clone();
    let after = next.clone();
    let mover = tokio::spawn(async move {
        storage
            .cas_continuity_owner(
                &before.tenant_id,
                before.continuity_id,
                before.epoch,
                before.owner_runtime_id,
                &after,
            )
            .await
    });
    let waiting=tokio::time::timeout(std::time::Duration::from_secs(5),async {
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks pending JOIN pg_locks held ON held.locktype=pending.locktype AND held.database=pending.database AND held.classid=pending.classid AND held.objid=pending.objid AND held.objsubid=pending.objsubid WHERE held.pid=$1 AND held.granted AND NOT pending.granted AND pending.locktype='advisory')").bind(pid).fetch_one(b.pg_pool()).await.unwrap();
            if waiting {break} tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.is_ok();
    let owner_unlocked = if waiting {
        sqlx::query("SELECT continuity_id FROM continuity_executions WHERE continuity_id=$1 FOR UPDATE NOWAIT").bind(owner.continuity_id.into_uuid()).fetch_one(&mut *held).await.is_ok()
    } else {
        false
    };
    held.rollback().await.unwrap();
    let applied = tokio::time::timeout(std::time::Duration::from_secs(5), mover)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(waiting, "relocation bypassed the target namespace lock");
    assert!(
        owner_unlocked,
        "relocation locked its owner before the target namespace"
    );
    assert!(applied);
    assert_eq!(
        b.storage
            .get_continuity_execution_by_instance(&target.tenant_id, target.id)
            .await
            .unwrap()
            .unwrap(),
        next
    );
}

async fn stranded_retry(
    b: &orch8_storage::testing::backends::TestBackend,
    scoped: bool,
    pure: bool,
    dry_run: bool,
) -> (WorkerTask, ExecutionNodeId, WorkerInvocation) {
    use orch8_types::continuity::{
        ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState, RuntimeId,
    };
    use orch8_types::worker::WorkerTaskResolutionAction;
    let (mut task, node) = fixture(b.storage.as_ref()).await;
    if pure {
        task.handler_name = "noop".into();
    }
    if scoped {
        let owner = ContinuityExecution {
            continuity_id: ContinuityId::new(),
            tenant_id: TenantId::unchecked("worker-invocations"),
            current_instance_id: task.instance_id,
            owner_runtime_id: RuntimeId::new(),
            epoch: ExecutionEpoch::initial(),
            state: OwnershipState::Owned,
            updated_at: Utc::now(),
        };
        b.storage.create_continuity_execution(&owner).await.unwrap();
        task.continuity_epoch = Some(owner.epoch.get());
    }
    if dry_run {
        let mut instance = b
            .storage
            .get_instance(task.instance_id)
            .await
            .unwrap()
            .unwrap();
        instance.context.runtime.dry_run = true;
        b.storage
            .update_instance_context(instance.id, &instance.context)
            .await
            .unwrap();
        task.context["runtime"] = json!({"dry_run":true});
    }
    b.storage.create_worker_task(&task).await.unwrap();
    let mut request = retry_resolution(&task, node);
    let WorkerTaskResolutionAction::Retry { retry_task, .. } = &mut request.action else {
        unreachable!()
    };
    retry_task.continuity_epoch = None;
    let replacement = retry_task.as_ref().clone();
    assert!(b.storage.resolve_worker_task(&request).await.unwrap());
    let original = binding(b.storage.as_ref(), &replacement).await;
    b.storage
        .update_node_state(node, NodeState::Waiting)
        .await
        .unwrap();
    b.storage
        .update_instance_state(task.instance_id, InstanceState::Waiting, None)
        .await
        .unwrap();
    (replacement, node, original)
}

orch8_storage::backend_test!(async fn stranded_recovery_refuses_reused_activation_changed_run_and_paused_source(b) {
    for mutation in 0..4 {
        let(task,node,original)=stranded_retry(&b,false,true,false).await;
        match mutation {
            0=>{b.storage.update_node_state(node,NodeState::Pending).await.unwrap();b.storage.update_node_state(node,NodeState::Waiting).await.unwrap();},
            1=>{let mut instance=b.storage.get_instance(task.instance_id).await.unwrap().unwrap();instance.context.runtime.run_id=Some(uuid::Uuid::new_v4().to_string());b.storage.update_instance_context(instance.id,&instance.context).await.unwrap();},
            2=>{let mut instance=b.storage.get_instance(task.instance_id).await.unwrap().unwrap();instance.context.runtime.dry_run=true;b.storage.update_instance_context(instance.id,&instance.context).await.unwrap();},
            _=>b.storage.update_instance_state(task.instance_id,InstanceState::Paused,None).await.unwrap(),
        }
        assert!(!b.storage.finalize_stranded_worker_dispatch(task.id,task.attempt,None,None).await.unwrap(),"stranded source mutation {mutation} was certified");
        assert_eq!(binding(b.storage.as_ref(),&task).await,original);
        assert!(b.storage.claim_worker_tasks("noop","must-wait",1).await.unwrap().is_empty());
    }
});

orch8_storage::backend_test!(async fn stranded_recovery_uses_retained_scope_before_filling_the_hot_epoch(b) {
    let(task,_,original)=stranded_retry(&b,true,true,false).await;
    assert!(task.continuity_epoch.is_none());assert_eq!(original.continuity_epoch,Some(0));
    assert!(!b.storage.finalize_stranded_worker_dispatch(task.id,task.attempt,None,Some(1)).await.unwrap(),"caller epoch replaced retained ownership proof");
    assert!(b.storage.finalize_stranded_worker_dispatch(task.id,task.attempt,None,original.continuity_epoch).await.unwrap());
    assert_eq!(binding(b.storage.as_ref(),&task).await,original);
    let claimed=b.storage.claim_worker_tasks("noop","ready",1).await.unwrap();assert_eq!(claimed.len(),1);assert_eq!(claimed[0].id,task.id);assert_eq!(claimed[0].continuity_epoch,original.continuity_epoch);
});

orch8_storage::backend_test!(async fn stranded_recovery_does_not_infer_purity_from_a_missing_effect_id(b) {
    let(task,_,original)=stranded_retry(&b,false,false,false).await;
    assert!(!b.storage.finalize_stranded_worker_dispatch(task.id,task.attempt,None,None).await.unwrap(),"missing effect id was treated as proof of pure execution");
    assert_eq!(binding(b.storage.as_ref(),&task).await,original);
    assert!(b.storage.claim_worker_tasks("remote","must-wait",1).await.unwrap().is_empty());
});

orch8_storage::backend_test!(async fn stranded_dry_run_recovery_preserves_simulation_without_admitting_an_effect(b) {
    let(task,_,original)=stranded_retry(&b,false,false,true).await;
    assert!(original.dry_run);
    assert!(b.storage.finalize_stranded_worker_dispatch(task.id,task.attempt,None,None).await.unwrap());
    let claimed=b.storage.claim_worker_tasks("remote","simulation",1).await.unwrap();assert_eq!(claimed.len(),1);assert_eq!(claimed[0].context["runtime"]["dry_run"],json!(true));
    assert!(b.storage.get_continuity_execution_by_instance(&original.tenant_id,task.instance_id).await.unwrap().is_none());
});

orch8_storage::backend_test!(async fn dispatch_preparation_cannot_cross_an_execution_mode_change(b) {
    let(task,node)=fixture(b.storage.as_ref()).await;let fence=dispatch_fence(&b,&task,node).await;
    let mut instance=b.storage.get_instance(task.instance_id).await.unwrap().unwrap();let original=instance.clone();
    instance.context.runtime.dry_run=true;b.storage.update_instance_context(instance.id,&instance.context).await.unwrap();
    assert!(b.storage.create_worker_task_bound(&task,&fence).await.unwrap().is_none());
    assert!(b.storage.prepare_worker_dispatch(&original,&task.block_id,&orch8_storage::worker_invocation::WorkerDispatchSelection::Tree{node_id:node}).await.unwrap().is_none());
    assert!(b.storage.get_worker_task(task.id).await.unwrap().is_none());
    assert_eq!(b.storage.get_execution_tree(task.instance_id).await.unwrap()[0].state,NodeState::Running);
});

orch8_storage::backend_test!(async fn stranded_effect_publication_checks_receipt_scope_state_kind_and_request_digest(b) {
    use orch8_types::continuity::{EffectReceipt,EffectId,EffectKind,EffectState};
    use orch8_storage::worker_invocation::WorkerDispatchEffect;
    for mutation in 0..7 {
        let(task,_,original)=stranded_retry(&b,true,false,false).await;let scope=original.continuity.as_ref().unwrap();
        let mut receipt=EffectReceipt {id:EffectId::new(),tenant_id:original.tenant_id.clone(),continuity_id:scope.continuity_id,epoch:orch8_types::continuity::ExecutionEpoch::initial(),instance_id:task.instance_id,block_id:task.block_id.clone(),kind:EffectKind::Worker,state:EffectState::Dispatched,destination_fingerprint:"b".repeat(64),idempotency_key:None,request_sha256:"a".repeat(64),provider_receipt_id:None,attempt:u32::from(task.attempt),created_at:Utc::now(),updated_at:Utc::now()};
        let proof=WorkerDispatchEffect{effect_id:receipt.id,request_sha256:receipt.request_sha256.clone(),destination_fingerprint:receipt.destination_fingerprint.clone(),kind:receipt.kind};
        match mutation {0=>receipt.block_id=BlockId::new("foreign-block"),1=>receipt.attempt+=1,2=>receipt.state=EffectState::Unknown,3=>receipt.request_sha256="c".repeat(64),4=>receipt.kind=EffectKind::Http,5=>receipt.destination_fingerprint="d".repeat(64),_=>{}}
        b.storage.create_effect_receipt(&receipt).await.unwrap();
        let published=b.storage.finalize_stranded_worker_dispatch(task.id,task.attempt,Some(&proof),original.continuity_epoch).await.unwrap();
        assert_eq!(published,mutation==6,"receipt mutation {mutation} was misclassified");
        assert_eq!(binding(b.storage.as_ref(),&task).await,original);
        if !published {assert!(b.storage.claim_worker_tasks("remote","must-wait",1).await.unwrap().is_empty());}
    }
});

orch8_storage::backend_test!(async fn stranded_unscoped_source_is_not_recertified_after_its_namespace_moves_away(b) {
    use orch8_types::continuity::{ContinuityExecution,ContinuityId,ExecutionEpoch,OwnershipState,RuntimeId};
    let(task,_,original)=stranded_retry(&b,false,true,false).await;
    let owner=ContinuityExecution{continuity_id:ContinuityId::new(),tenant_id:original.tenant_id.clone(),current_instance_id:task.instance_id,owner_runtime_id:RuntimeId::new(),epoch:ExecutionEpoch::initial(),state:OwnershipState::Owned,updated_at:Utc::now()};
    b.storage.create_continuity_execution(&owner).await.unwrap();
    let mut target=b.storage.get_instance(task.instance_id).await.unwrap().unwrap();target.id=orch8_types::ids::InstanceId::new();b.storage.create_instance(&target).await.unwrap();
    let mut next=owner.clone();next.current_instance_id=target.id;next.epoch=owner.epoch.checked_next().unwrap();next.owner_runtime_id=RuntimeId::new();next.updated_at=Utc::now();
    assert!(b.storage.cas_continuity_owner(&owner.tenant_id,owner.continuity_id,owner.epoch,owner.owner_runtime_id,&next).await.unwrap());
    assert!(b.storage.get_continuity_execution_by_instance(&owner.tenant_id,task.instance_id).await.unwrap().is_none());
    assert!(!b.storage.finalize_stranded_worker_dispatch(task.id,task.attempt,None,None).await.unwrap(),"absence of a current owner erased retained namespace history");
    assert!(b.storage.claim_worker_tasks("noop","must-wait",1).await.unwrap().is_empty());
});
