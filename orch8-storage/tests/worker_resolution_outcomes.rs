//! Durable uncertainty and native resolution must share one exact-claim transaction.
use chrono::Utc;
use orch8_storage::{
    StorageBackend,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
    worker_admission::{
        WorkerDispatchIntent, WorkerDispatchPublication, WorkerEffectPlan, dispatch_effect,
        dispatch_scope,
    },
    worker_invocation::WorkerDispatchSelection,
};
use orch8_types::{
    continuity::{EffectId, EffectReceipt, EffectState, ExecutionEpoch},
    execution::{BlockType, ExecutionNode, NodeState},
    ids::{BlockId, ExecutionNodeId, TenantId},
    instance::InstanceState,
    worker::{
        WorkerAttemptEventKind, WorkerClaim, WorkerTask, WorkerTaskEffectResolution,
        WorkerTaskResolution, WorkerTaskResolutionAction, WorkerTaskState,
    },
};
use serde_json::json;
use uuid::Uuid;

async fn fixture(s: &dyn StorageBackend) -> (WorkerTask, WorkerClaim, ExecutionNodeId, TenantId) {
    let handler = format!("remote.failure.{}", Uuid::now_v7());
    let sequence = SequenceBuilder::new("resolution-outcomes")
        .name(&Uuid::now_v7().to_string())
        .step("work", &handler)
        .build();
    s.create_sequence(&sequence).await.unwrap();
    let mut instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    instance.context.runtime.run_id = Some(Uuid::now_v7().to_string());
    s.create_instance(&instance).await.unwrap();
    let parent = s
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
    s.create_execution_node(&node).await.unwrap();
    let fence = s
        .prepare_worker_dispatch(
            &parent,
            &node.block_id,
            &WorkerDispatchSelection::Tree { node_id: node.id },
        )
        .await
        .unwrap()
        .unwrap();
    let task:WorkerTask=serde_json::from_value(json!({"id":Uuid::now_v7(),"instance_id":parent.id,"block_id":"work","handler_name":handler,"attempt":0,"params":{},"context":parent.context,"state":"pending","created_at":Utc::now()})).unwrap();
    let scope = dispatch_scope(&fence);
    let proof = dispatch_effect(&task, EffectId::new());
    let now = Utc::now();
    let receipt = EffectReceipt {
        id: proof.effect_id,
        tenant_id: parent.tenant_id.clone(),
        continuity_id: scope.continuity_id,
        epoch: ExecutionEpoch::from_u64(scope.epoch),
        instance_id: parent.id,
        block_id: task.block_id.clone(),
        kind: proof.kind,
        state: EffectState::Planned,
        request_sha256: proof.request_sha256,
        destination_fingerprint: proof.destination_fingerprint,
        idempotency_key: None,
        provider_receipt_id: None,
        attempt: 0,
        created_at: now,
        updated_at: now,
    };
    let intent =
        WorkerDispatchIntent::new(task, fence, Some(WorkerEffectPlan::new(receipt, false)))
            .unwrap();
    assert!(matches!(
        s.admit_worker_dispatch(&intent).await.unwrap(),
        WorkerDispatchPublication::Published { .. }
    ));
    s.update_instance_state(parent.id, InstanceState::Waiting, None)
        .await
        .unwrap();
    let held = s
        .claim_worker_tasks(&handler, "real-worker", 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let claim = WorkerClaim::new("real-worker", held.claim_epoch);
    (held, claim, node.id, parent.tenant_id)
}
fn retry(task: &WorkerTask, node: ExecutionNodeId) -> WorkerTaskResolution {
    let next = WorkerTask {
        id: Uuid::now_v7(),
        attempt: task.attempt + 1,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        claim_epoch: 0,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
        effect_id: None,
        continuity_epoch: None,
        lease_secs: None,
        claimed_runtime_kind: None,
        ..task.clone()
    };
    WorkerTaskResolution {
        task_id: task.id,
        instance_id: task.instance_id,
        expected_state: task.state,
        expected_claim_epoch: task.claim_epoch,
        expected_worker_id: task.worker_id.clone(),
        holder_worker_id: Some("forged-audit-label".into()),
        event: WorkerAttemptEventKind::Failed,
        effect_resolution: WorkerTaskEffectResolution::Unknown,
        reason: "FAILURE_PRIVATE_CANARY".into(),
        retryable: true,
        action: WorkerTaskResolutionAction::Retry {
            retry_task: Box::new(next),
            retry_output: None,
            node_id: Some(node),
            fire_at: Utc::now(),
        },
    }
}

orch8_storage::backend_test!(async fn failed_report_survives_retry_and_native_gc_with_exact_claim_scope(b){
    use orch8_storage::testing::backends::RawPool;
    let s=b.storage.as_ref();let(task,claim,node,tenant)=fixture(s).await;let resolution=retry(&task,node);
    assert!(s.resolve_worker_task(&resolution).await.unwrap());
    assert!(s.get_worker_task(task.id).await.unwrap().is_none());
    let report=s.get_worker_resolution_outcome(Some(&tenant),task.id,Some(&claim)).await.unwrap().unwrap();
    assert_eq!(report.reason,json!("FAILURE_PRIVATE_CANARY"));assert!(!format!("{report:?}").contains("FAILURE_PRIVATE_CANARY"));
    assert_eq!(report.worker_id.as_deref(),Some("real-worker"));assert_eq!(report.effect_state_before,Some(EffectState::Dispatched));assert_eq!(report.effect_state_after,Some(EffectState::Unknown));
    assert_eq!(report.action["kind"],"retry");assert_eq!(report.invocation.as_ref().unwrap().task_id,task.id);
    for wrong in [WorkerClaim::new("different",claim.claim_epoch),WorkerClaim::new("real-worker",claim.claim_epoch+1),WorkerClaim::new("real-worker",u64::MAX)]{
        assert!(s.get_worker_resolution_outcome(Some(&tenant),task.id,Some(&wrong)).await.unwrap().is_none());
    }
    assert!(s.get_worker_resolution_outcome(Some(&TenantId::unchecked("foreign")),task.id,Some(&claim)).await.unwrap().is_none());
    if b.name().starts_with("encrypted") {
        let raw:String=match &b.raw {
            RawPool::Sqlite(pool)=>sqlx::query_scalar("SELECT record FROM worker_resolution_outcomes WHERE task_id=?1").bind(task.id.to_string()).fetch_one(pool).await.unwrap(),
            #[cfg(feature="postgres")]
            RawPool::Postgres(pool)=>sqlx::query_scalar("SELECT record::text FROM worker_resolution_outcomes WHERE task_id=$1").bind(task.id).fetch_one(pool).await.unwrap(),
        };assert!(!raw.contains("FAILURE_PRIVATE_CANARY"));
    }
    let mut receipt=s.get_effect_receipt(&tenant,task.effect_id.unwrap()).await.unwrap().unwrap();
    receipt.transition(EffectState::Abandoned,Utc::now()).unwrap();
    assert!(s.cas_effect_receipt(&tenant,receipt.id,EffectState::Unknown,&receipt).await.unwrap());
    s.update_instance_state(task.instance_id,InstanceState::Cancelled,None).await.unwrap();
    assert_eq!(s.purge_terminal_instances(&[task.instance_id]).await.unwrap(),1);
    assert_eq!(s.get_worker_resolution_outcome(Some(&tenant),task.id,Some(&claim)).await.unwrap().unwrap(),report);
});

orch8_storage::backend_test!(async fn resolution_journal_failure_restores_receipt_claim_and_node(b){
    use orch8_storage::testing::backends::RawPool;
    let s=b.storage.as_ref();let(task,claim,node,tenant)=fixture(s).await;let resolution=retry(&task,node);
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("CREATE TRIGGER reject_resolution_test BEFORE INSERT ON worker_resolution_outcomes BEGIN SELECT RAISE(ABORT,'injected report failure'); END;").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION reject_resolution_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected report failure'; END $$; CREATE TRIGGER reject_resolution_test BEFORE INSERT ON worker_resolution_outcomes FOR EACH ROW EXECUTE FUNCTION reject_resolution_test();").execute(pool).await.unwrap();},
    }
    assert!(s.resolve_worker_task(&resolution).await.is_err());
    assert_eq!(s.get_worker_task(task.id).await.unwrap().unwrap().state,WorkerTaskState::Claimed);
    assert_eq!(s.get_effect_receipt(&tenant,task.effect_id.unwrap()).await.unwrap().unwrap().state,EffectState::Dispatched);
    assert_eq!(s.get_execution_tree(task.instance_id).await.unwrap().iter().find(|n|n.id==node).unwrap().state,NodeState::Waiting);
    assert!(s.get_worker_resolution_outcome(Some(&tenant),task.id,Some(&claim)).await.unwrap().is_none());
});

orch8_storage::backend_test!(async fn retry_failure_after_journaling_rolls_back_report_and_settlement(b){
    use orch8_storage::testing::backends::RawPool;
    let s=b.storage.as_ref();let(task,claim,node,tenant)=fixture(s).await;let resolution=retry(&task,node);
    match &b.raw {
        RawPool::Sqlite(pool)=>{sqlx::raw_sql("CREATE TRIGGER reject_retry_test BEFORE UPDATE OF state ON execution_tree WHEN NEW.state='pending' BEGIN SELECT RAISE(ABORT,'injected retry failure'); END;").execute(pool).await.unwrap();},
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{sqlx::raw_sql("CREATE FUNCTION reject_retry_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='pending' THEN RAISE EXCEPTION 'injected retry failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_retry_test BEFORE UPDATE OF state ON execution_tree FOR EACH ROW EXECUTE FUNCTION reject_retry_test();").execute(pool).await.unwrap();},
    }
    assert!(s.resolve_worker_task(&resolution).await.is_err());
    assert_eq!(s.get_worker_task(task.id).await.unwrap().unwrap().state,WorkerTaskState::Claimed);
    assert_eq!(s.get_effect_receipt(&tenant,task.effect_id.unwrap()).await.unwrap().unwrap().state,EffectState::Dispatched);
    assert!(s.get_worker_resolution_outcome(Some(&tenant),task.id,Some(&claim)).await.unwrap().is_none());
});

async fn legacy_fixture(s: &dyn StorageBackend) -> (WorkerTask, WorkerClaim, TenantId) {
    let sequence = SequenceBuilder::new("legacy-resolution")
        .name(&Uuid::now_v7().to_string())
        .step("work", "noop")
        .build();
    s.create_sequence(&sequence).await.unwrap();
    let mut instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Waiting)
        .build();
    instance.context.runtime.run_id = Some(Uuid::now_v7().to_string());
    instance.context.runtime.current_step = Some(BlockId::new("work"));
    instance.context.runtime.current_step_started_at = Some(Utc::now());
    s.create_instance(&instance).await.unwrap();
    let task:WorkerTask=serde_json::from_value(json!({"id":Uuid::now_v7(),"instance_id":instance.id,"block_id":"work","handler_name":"noop","params":{},"context":instance.context,"attempt":0,"state":"pending","created_at":Utc::now()})).unwrap();
    s.create_worker_task(&task).await.unwrap();
    let held = s
        .claim_worker_tasks("noop", "legacy-worker", 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let claim = WorkerClaim::new("legacy-worker", held.claim_epoch);
    (held, claim, instance.tenant_id)
}
fn task_only(task: &WorkerTask) -> WorkerTaskResolution {
    WorkerTaskResolution {
        task_id: task.id,
        instance_id: task.instance_id,
        expected_state: task.state,
        expected_claim_epoch: task.claim_epoch,
        expected_worker_id: task.worker_id.clone(),
        holder_worker_id: task.worker_id.clone(),
        event: WorkerAttemptEventKind::Failed,
        effect_resolution: WorkerTaskEffectResolution::Unknown,
        reason: "failed".into(),
        retryable: false,
        action: WorkerTaskResolutionAction::FailTaskOnly,
    }
}

orch8_storage::backend_test!(async fn claimed_work_cannot_be_certified_abandoned_even_without_an_effect_receipt(b){
    let s=b.storage.as_ref();
    for with_effect in [false,true] {
        let(task,claim,tenant)=if with_effect {
            let(task,claim,_,tenant)=fixture(s).await;(task,claim,tenant)
        } else { legacy_fixture(s).await };
        let mut resolution=task_only(&task);
        resolution.effect_resolution=WorkerTaskEffectResolution::Abandoned;
        assert!(s.resolve_worker_task(&resolution).await.is_err());
        assert_eq!(s.get_worker_task(task.id).await.unwrap().unwrap().state,WorkerTaskState::Claimed);
        assert!(s.get_worker_resolution_outcome(Some(&tenant),task.id,Some(&claim)).await.unwrap().is_none());
        if let Some(effect)=task.effect_id {
            assert_eq!(s.get_effect_receipt(&tenant,effect).await.unwrap().unwrap().state,EffectState::Dispatched);
        }
    }
});

#[cfg(feature = "postgres")]
#[tokio::test]
async fn resolution_migration_roundtrips_empty_and_refuses_to_discard_retained_reports() {
    use orch8_storage::testing::backends::TestBackend;
    let Some(b) = TestBackend::postgres().await else {
        return;
    };
    let down = include_str!("../../migrations/down/137_worker_resolution_outcomes.sql");
    let up = include_str!("../../migrations/137_worker_resolution_outcomes.sql");
    let mut tx = b.pg_pool().begin().await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../migrations/down/139_worker_resolution_application.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::raw_sql(down).execute(&mut *tx).await.unwrap();
    sqlx::raw_sql(up).execute(&mut *tx).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../migrations/139_worker_resolution_application.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let (task, claim, tenant) = legacy_fixture(b.storage.as_ref()).await;
    assert!(
        b.storage
            .resolve_worker_task(&task_only(&task))
            .await
            .unwrap()
    );
    let before = b
        .storage
        .get_worker_resolution_outcome(Some(&tenant), task.id, Some(&claim))
        .await
        .unwrap()
        .unwrap();
    let mut tx = b.pg_pool().begin().await.unwrap();
    let error = sqlx::raw_sql(down).execute(&mut *tx).await.unwrap_err();
    assert!(error.to_string().contains("require the current protocol"));
    tx.rollback().await.unwrap();
    let after = b
        .storage
        .get_worker_resolution_outcome(Some(&tenant), task.id, Some(&claim))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(after).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    assert!(
        sqlx::query("DELETE FROM worker_resolution_outcomes WHERE task_id=$1")
            .bind(task.id)
            .execute(b.pg_pool())
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE worker_resolution_outcomes SET record=record WHERE task_id=$1")
            .bind(task.id)
            .execute(b.pg_pool())
            .await
            .is_err()
    );
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn retained_identity_cannot_be_reinserted_while_its_hot_row_is_being_deleted() {
    use orch8_storage::testing::backends::TestBackend;
    let Some(b) = TestBackend::postgres().await else {
        return;
    };
    let (task, claim, tenant) = legacy_fixture(b.storage.as_ref()).await;
    assert!(
        b.storage
            .resolve_worker_task(&task_only(&task))
            .await
            .unwrap()
    );
    let mut deletion = b.pg_pool().begin().await.unwrap();
    sqlx::query("DELETE FROM worker_tasks WHERE id=$1")
        .bind(task.id)
        .execute(&mut *deletion)
        .await
        .unwrap();
    let mut insertion = b.pg_pool().acquire().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *insertion)
        .await
        .unwrap();
    // The old row remains visible to the BEFORE INSERT trigger until DELETE commits.
    let insert = tokio::spawn(async move {
        sqlx::query("INSERT INTO worker_tasks SELECT * FROM worker_tasks WHERE id=$1")
            .bind(task.id)
            .execute(&mut *insertion)
            .await
    });
    let blocked = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted)",
            )
            .bind(pid)
            .fetch_one(b.pg_pool())
            .await
            .unwrap();
            if waiting || insert.is_finished() {
                break waiting;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    deletion.commit().await.unwrap();
    let result = insert.await.unwrap();
    // A stricter BEFORE trigger may reject early; otherwise the AFTER trigger
    // must reject the insert after it has waited for the old primary key.
    assert!(
        result.is_err(),
        "retained task identity was recreated after a concurrent delete (blocked={blocked})"
    );
    assert!(b.storage.get_worker_task(task.id).await.unwrap().is_none());
    assert!(
        b.storage
            .get_worker_resolution_outcome(Some(&tenant), task.id, Some(&claim))
            .await
            .unwrap()
            .is_some()
    );
}

orch8_storage::backend_test!(async fn resolution_boundary_prevents_recreating_a_retained_task_identity(b){
    let s=b.storage.as_ref();let(task,claim,tenant)=legacy_fixture(s).await;
    assert!(s.resolve_worker_task(&task_only(&task)).await.unwrap());
    s.delete_worker_task(task.id).await.unwrap();
    let replacement=WorkerTask{state:WorkerTaskState::Pending,worker_id:None,claimed_at:None,heartbeat_at:None,claim_epoch:0,
        completed_at:None,error_message:None,error_retryable:None,params:json!({"different_request":true}),..task.clone()};
    assert!(s.create_worker_task(&replacement).await.is_err(),"{}: a new activity reused a task ID with retained acknowledgements",b.name());
    assert!(s.get_worker_resolution_outcome(Some(&tenant),task.id,Some(&claim)).await.unwrap().is_some());
});

orch8_storage::backend_test!(async fn resolution_boundary_prevents_claim_counter_and_holder_reuse(b){
    use orch8_storage::testing::backends::RawPool;
    let s=b.storage.as_ref();let(task,_,_)=legacy_fixture(s).await;
    match &b.raw {
        RawPool::Sqlite(pool)=>{
            assert!(sqlx::query("UPDATE worker_tasks SET claim_epoch=0 WHERE id=?1").bind(task.id.to_string()).execute(pool).await.is_err());
            assert!(sqlx::query("UPDATE worker_tasks SET worker_id='another' WHERE id=?1").bind(task.id.to_string()).execute(pool).await.is_err());
            assert!(sqlx::query("UPDATE worker_tasks SET attempt=attempt+1 WHERE id=?1").bind(task.id.to_string()).execute(pool).await.is_err());
        },
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool)=>{
            assert!(sqlx::query("UPDATE worker_tasks SET claim_epoch=0 WHERE id=$1").bind(task.id).execute(pool).await.is_err());
            assert!(sqlx::query("UPDATE worker_tasks SET worker_id='another' WHERE id=$1").bind(task.id).execute(pool).await.is_err());
            assert!(sqlx::query("UPDATE worker_tasks SET attempt=attempt+1 WHERE id=$1").bind(task.id).execute(pool).await.is_err());
        },
    }
});

orch8_storage::backend_test!(async fn resolution_boundary_preserves_utf8_reports_with_embedded_nul(b){
    let s=b.storage.as_ref();let(task,claim,_,tenant)=fixture(s).await;
    let mut resolution=task_only(&task);resolution.reason="provider returned invalid\0payload with unicode λ".into();
    assert!(s.resolve_worker_task(&resolution).await.unwrap(),"{}",b.name());
    let report=s.get_worker_resolution_outcome(Some(&tenant),task.id,Some(&claim)).await.unwrap().unwrap();
    assert_eq!(report.reason,json!(resolution.reason));
});

async fn pending_failure(
    s: &dyn StorageBackend,
) -> (
    WorkerTask,
    ExecutionNodeId,
    orch8_storage::worker_resolution::WorkerResolutionOutcome,
) {
    let (task, claim, node, tenant) = fixture(s).await;
    s.update_instance_state(task.instance_id, InstanceState::Paused, None)
        .await
        .unwrap();
    let mut resolution = task_only(&task);
    resolution.retryable = true;
    assert!(s.resolve_worker_task(&resolution).await.unwrap());
    let report = s
        .get_worker_resolution_outcome(Some(&tenant), task.id, Some(&claim))
        .await
        .unwrap()
        .unwrap();
    (task, node, report)
}

orch8_storage::backend_test!(async fn retained_failure_application_is_atomic_idempotent_and_unclaimable(b) {
    use orch8_storage::worker_resolution_application::{WorkerResolutionApplication as A, WorkerResolutionIntegration as Plan};
    use orch8_storage::testing::backends::RawPool;
    let s = b.storage.as_ref();
    let (task, node, report) = pending_failure(s).await;
    let shell = Plan::retry_source(&report).unwrap();
    assert!(shell.context["data"].is_null());
    assert!(shell.context["config"].is_null());
    assert!(shell.context["audit"].as_array().unwrap().is_empty());
    assert_eq!(shell.context["runtime"]["run_id"], task.context["runtime"]["run_id"]);
    let action = retry(&shell, node).action;
    let next_id = match &action { WorkerTaskResolutionAction::Retry {retry_task,..} => retry_task.id, _=> unreachable!() };
    let plan = Plan::new(report.clone(), action).unwrap();
    assert_eq!(s.get_worker_resolution_outcome_by_id(&TenantId::unchecked("foreign"),report.id).await.unwrap(),None);
    assert!(s.claim_worker_resolution_recovery_page(0).await.unwrap().is_empty());
    assert_eq!(s.claim_worker_resolution_recovery_page(1).await.unwrap()[0].id, report.id);
    match &b.raw {
        RawPool::Sqlite(pool) => { sqlx::raw_sql("CREATE TRIGGER reject_application_test BEFORE INSERT ON worker_resolution_integrations BEGIN SELECT RAISE(ABORT,'injected application failure'); END;").execute(pool).await.unwrap(); },
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool) => { sqlx::raw_sql("CREATE FUNCTION reject_application_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected application failure'; END $$; CREATE TRIGGER reject_application_test BEFORE INSERT ON worker_resolution_integrations FOR EACH ROW EXECUTE FUNCTION reject_application_test();").execute(pool).await.unwrap(); },
    }
    assert!(s.apply_worker_resolution_integration(&plan).await.is_err());
    assert!(s.get_worker_task(next_id).await.unwrap().is_none());
    assert_eq!(s.get_worker_task(task.id).await.unwrap().unwrap().state,WorkerTaskState::Failed);
    assert_eq!(s.get_execution_tree(task.instance_id).await.unwrap().iter().find(|n|n.id==node).unwrap().state,NodeState::Waiting);
    assert_eq!(s.get_instance(task.instance_id).await.unwrap().unwrap().state,InstanceState::Paused);
    assert_eq!(s.claim_worker_resolution_recovery_page(1).await.unwrap()[0].id, report.id);
    match &b.raw {
        RawPool::Sqlite(pool) => { sqlx::raw_sql("DROP TRIGGER reject_application_test").execute(pool).await.unwrap(); },
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool) => { sqlx::raw_sql("DROP TRIGGER reject_application_test ON worker_resolution_integrations; DROP FUNCTION reject_application_test();").execute(pool).await.unwrap(); },
    }
    s.delete_worker_task(task.id).await.unwrap();
    assert_eq!(s.apply_worker_resolution_integration(&plan).await.unwrap(), A::Applied);
    assert_eq!(s.apply_worker_resolution_integration(&plan).await.unwrap(), A::AlreadyApplied);
    assert!(s.claim_worker_resolution_recovery_page(1).await.unwrap().is_empty());
    assert_eq!(s.get_instance(task.instance_id).await.unwrap().unwrap().state,InstanceState::Paused);
    assert!(s.claim_worker_tasks(&task.handler_name,"other-worker",1).await.unwrap().is_empty());
    assert_eq!(s.get_effect_receipt(&report.tenant_id,task.effect_id.unwrap()).await.unwrap().unwrap().state,EffectState::Unknown);
    assert_eq!(s.get_worker_resolution_outcome_by_id(&report.tenant_id,report.id).await.unwrap().unwrap(),report);
    match &b.raw {
        RawPool::Sqlite(pool) => {
            assert!(sqlx::raw_sql("UPDATE worker_resolution_integrations SET action=action").execute(pool).await.is_err());
            assert!(sqlx::raw_sql("DELETE FROM worker_resolution_integrations").execute(pool).await.is_err());
        },
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool) => {
            assert!(sqlx::raw_sql("UPDATE worker_resolution_integrations SET action=action").execute(pool).await.is_err());
            assert!(sqlx::raw_sql("DELETE FROM worker_resolution_integrations").execute(pool).await.is_err());
            assert!(sqlx::raw_sql(include_str!("../../migrations/down/139_worker_resolution_application.sql")).execute(pool).await.is_err());
        },
    }
});

orch8_storage::backend_test!(async fn retained_failure_application_refuses_changed_run_and_settled_effect(b) {
    use orch8_storage::worker_resolution_application::{WorkerResolutionApplication as A, WorkerResolutionIntegration as Plan};
    let s=b.storage.as_ref();
    let (task,node,report)=pending_failure(s).await;
    let plan=Plan::new(report.clone(),WorkerTaskResolutionAction::FailNode { node_id:Some(node),fire_at:Utc::now() }).unwrap();
    let mut parent=s.get_instance(task.instance_id).await.unwrap().unwrap();
    let original=parent.context.runtime.run_id.clone();
    parent.context.runtime.run_id=Some(Uuid::now_v7().to_string());
    s.update_instance_context(task.instance_id,&parent.context).await.unwrap();
    assert_eq!(s.apply_worker_resolution_integration(&plan).await.unwrap(),A::ReconciliationRequired);
    parent.context.runtime.run_id=original;
    s.update_instance_context(task.instance_id,&parent.context).await.unwrap();
    let mut effect=s.get_effect_receipt(&report.tenant_id,task.effect_id.unwrap()).await.unwrap().unwrap();
    effect.transition(EffectState::Abandoned,Utc::now()).unwrap();
    assert!(s.cas_effect_receipt(&report.tenant_id,effect.id,EffectState::Unknown,&effect).await.unwrap());
    assert_eq!(s.apply_worker_resolution_integration(&plan).await.unwrap(),A::ReconciliationRequired);
    assert_eq!(s.get_instance(task.instance_id).await.unwrap().unwrap().state,InstanceState::Paused);
    assert_eq!(s.get_execution_tree(task.instance_id).await.unwrap().iter().find(|n|n.id==node).unwrap().state,NodeState::Waiting);
    assert_eq!(s.claim_worker_resolution_recovery_page(1).await.unwrap()[0].id,report.id);
});

orch8_storage::backend_test!(async fn retained_failure_recovery_rotates_and_concurrent_application_has_one_winner(b) {
    use orch8_storage::worker_resolution_application::{WorkerResolutionApplication as A, WorkerResolutionIntegration as Plan};
    let s=b.storage.as_ref();
    let (first,node,report)=pending_failure(s).await;
    let (_,_,second)=pending_failure(s).await;
    let a=s.claim_worker_resolution_recovery_page(1).await.unwrap().pop().unwrap();
    let b=s.claim_worker_resolution_recovery_page(1).await.unwrap().pop().unwrap();
    assert_ne!(a.id,b.id,"a deferred report starved a younger report");
    assert!([a.id,b.id].contains(&report.id));
    assert!([a.id,b.id].contains(&second.id));
    let plan=Plan::new(report.clone(),WorkerTaskResolutionAction::FailNode {node_id:Some(node),fire_at:Utc::now()}).unwrap();
    s.delete_worker_task(first.id).await.unwrap();
    let (a,b)=tokio::join!(s.apply_worker_resolution_integration(&plan),s.apply_worker_resolution_integration(&plan));
    let results=[a.unwrap(),b.unwrap()];
    assert_eq!(results.iter().filter(|&&r|r==A::Applied).count(),1);
    assert!(results.iter().all(|r|matches!(r,A::Applied|A::AlreadyApplied|A::Deferred)));
    assert_eq!(s.apply_worker_resolution_integration(&plan).await.unwrap(),A::AlreadyApplied);
    assert_eq!(s.claim_worker_resolution_recovery_page(2).await.unwrap().iter().map(|r|r.id).collect::<Vec<_>>(),vec![second.id]);
});

#[cfg(feature = "postgres")]
#[tokio::test]
async fn failure_recovery_scanner_skips_a_busy_clock() {
    let Some(b) = orch8_storage::testing::backends::TestBackend::postgres().await else {
        return;
    };
    pending_failure(b.storage.as_ref()).await;
    let mut held = b.pg_pool().begin().await.unwrap();
    sqlx::query("SELECT singleton FROM worker_outcome_recovery_clock WHERE singleton=1 FOR UPDATE")
        .execute(&mut *held)
        .await
        .unwrap();
    let scan = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        b.storage.claim_worker_resolution_recovery_page(1),
    )
    .await;
    assert!(scan.unwrap().unwrap().is_empty());
    held.rollback().await.unwrap();
    assert_eq!(
        b.storage
            .claim_worker_resolution_recovery_page(1)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[cfg(feature = "postgres")]
async fn assert_failure_application_skips_busy_attempt(effect: bool) {
    use orch8_storage::worker_resolution_application::{
        WorkerResolutionApplication as A, WorkerResolutionIntegration as Plan,
    };
    let Some(b) = orch8_storage::testing::backends::TestBackend::postgres().await else {
        return;
    };
    let s = b.storage.as_ref();
    let (task, node, report) = pending_failure(s).await;
    let plan = Plan::new(
        report,
        WorkerTaskResolutionAction::FailNode {
            node_id: Some(node),
            fire_at: Utc::now(),
        },
    )
    .unwrap();
    let mut held = b.pg_pool().begin().await.unwrap();
    if effect {
        sqlx::query("SELECT id FROM effect_receipts WHERE id=$1 FOR UPDATE")
            .bind(task.effect_id.unwrap().into_uuid())
            .execute(&mut *held)
            .await
            .unwrap();
    } else {
        sqlx::query("SELECT id FROM worker_tasks WHERE id=$1 FOR UPDATE")
            .bind(task.id)
            .execute(&mut *held)
            .await
            .unwrap();
    }
    let attempt = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        s.apply_worker_resolution_integration(&plan),
    )
    .await;
    held.rollback().await.unwrap();
    assert!(
        matches!(attempt, Ok(Ok(A::Deferred))),
        "a busy attempt stalled recovery or was misclassified: {attempt:?}"
    );
    assert_eq!(
        s.apply_worker_resolution_integration(&plan).await.unwrap(),
        A::Applied
    );
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn failure_application_skips_busy_effect_receipt() {
    assert_failure_application_skips_busy_attempt(true).await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn failure_application_skips_busy_hot_task() {
    assert_failure_application_skips_busy_attempt(false).await;
}

orch8_storage::backend_test!(async fn retained_legacy_failure_without_effect_defers_flat_termination_until_resume(b) {
    use orch8_storage::worker_resolution_application::{WorkerResolutionApplication as A, WorkerResolutionIntegration as Plan};
    let s=b.storage.as_ref();
    let (task,claim,tenant)=legacy_fixture(s).await;
    assert!(task.effect_id.is_none());
    s.update_instance_state(task.instance_id,InstanceState::Paused,None).await.unwrap();
    assert!(s.resolve_worker_task(&task_only(&task)).await.unwrap());
    let report=s.get_worker_resolution_outcome(Some(&tenant),task.id,Some(&claim)).await.unwrap().unwrap();
    assert!(!report.admitted);
    let plan=Plan::new(report,WorkerTaskResolutionAction::FailInstance).unwrap();
    s.delete_worker_task(task.id).await.unwrap();
    assert_eq!(s.apply_worker_resolution_integration(&plan).await.unwrap(),A::Deferred);
    assert_eq!(s.get_instance(task.instance_id).await.unwrap().unwrap().state,InstanceState::Paused);
    s.update_instance_state(task.instance_id,InstanceState::Scheduled,Some(Utc::now())).await.unwrap();
    assert_eq!(s.apply_worker_resolution_integration(&plan).await.unwrap(),A::Applied);
    assert_eq!(s.get_instance(task.instance_id).await.unwrap().unwrap().state,InstanceState::Failed);
    assert_eq!(s.apply_worker_resolution_integration(&plan).await.unwrap(),A::AlreadyApplied);
});
