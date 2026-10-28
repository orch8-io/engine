//! Compound worker transitions must bind every target to the original dispatch.
use chrono::Utc;
use orch8_storage::{
    StorageBackend,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    execution::{BlockType, ExecutionNode, NodeState},
    ids::{BlockId, ExecutionNodeId},
    worker::{
        WorkerAttemptEventKind, WorkerTask, WorkerTaskResolution, WorkerTaskResolutionAction,
        WorkerTaskState,
    },
};
use serde_json::json;

async fn fixture(s: &dyn StorageBackend) -> (WorkerTask, ExecutionNode) {
    let sequence = SequenceBuilder::new(&format!("worker-identity-{}", uuid::Uuid::new_v4()))
        .step("step", "noop")
        .build();
    s.create_sequence(&sequence).await.unwrap();
    let mut instance = InstanceBuilder::for_sequence(&sequence).build();
    instance.context.runtime.run_id = Some(uuid::Uuid::new_v4().to_string());
    instance.context.runtime.current_step = Some(BlockId::new("step"));
    instance.context.runtime.current_step_started_at = Some(Utc::now());
    s.create_instance(&instance).await.unwrap();
    let node = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: instance.id,
        block_id: BlockId::new("step"),
        parent_id: None,
        block_type: BlockType::Step,
        branch_index: None,
        state: NodeState::Running,
        started_at: Some(Utc::now()),
        completed_at: None,
    };
    s.create_execution_node(&node).await.unwrap();
    let task: WorkerTask = serde_json::from_value(json!({"id":uuid::Uuid::new_v4(),"instance_id":instance.id,
        "block_id":"step","handler_name":"noop","params":{},"context":{},"attempt":0,"state":"pending","created_at":Utc::now()})).unwrap();
    s.create_worker_task(&task).await.unwrap();
    (task, node)
}

fn resolution(task: &WorkerTask, action: WorkerTaskResolutionAction) -> WorkerTaskResolution {
    WorkerTaskResolution {
        task_id: task.id,
        instance_id: task.instance_id,
        expected_state: WorkerTaskState::Pending,
        expected_claim_epoch: 0,
        expected_worker_id: None,
        holder_worker_id: None,
        event: WorkerAttemptEventKind::TimedOut,
        effect_resolution: orch8_types::worker::WorkerTaskEffectResolution::Keep,
        reason: "identity regression".into(),
        retryable: true,
        action,
    }
}

orch8_storage::backend_test!(async fn resolution_rejects_foreign_instance_without_mutation(b) {
    let (task,_) = fixture(b.storage.as_ref()).await;
    let (foreign,_) = fixture(b.storage.as_ref()).await;
    let before=b.storage.get_instance(foreign.instance_id).await.unwrap().unwrap();
    let mut request=resolution(&task,WorkerTaskResolutionAction::FailInstance);
    request.instance_id=foreign.instance_id;
    assert!(!b.storage.resolve_worker_task(&request).await.unwrap());
    assert_eq!(b.storage.get_worker_task(task.id).await.unwrap().unwrap().state,WorkerTaskState::Pending);
    assert_eq!(b.storage.get_instance(foreign.instance_id).await.unwrap().unwrap().state,before.state);
    assert!(b.storage.list_worker_task_attempt_events(task.id,10).await.unwrap().is_empty());
});

orch8_storage::backend_test!(async fn resolution_rejects_foreign_retry_without_deleting_original(b) {
    let (task,_) = fixture(b.storage.as_ref()).await;
    let (foreign,_) = fixture(b.storage.as_ref()).await;
    let mut retry=task.clone(); retry.id=uuid::Uuid::new_v4(); retry.instance_id=foreign.instance_id;
    retry.block_id=BlockId::new("injected"); retry.attempt=1;
    let request=resolution(&task,WorkerTaskResolutionAction::Retry { retry_task:Box::new(retry.clone()), retry_output:None,node_id:None,fire_at:Utc::now() });
    assert!(!b.storage.resolve_worker_task(&request).await.unwrap());
    assert!(b.storage.get_worker_task(task.id).await.unwrap().is_some());
    assert!(b.storage.get_worker_task(retry.id).await.unwrap().is_none());
    assert!(b.storage.list_worker_task_attempt_events(task.id,10).await.unwrap().is_empty());
});

orch8_storage::backend_test!(async fn resolution_rejects_foreign_node_and_accepts_own_node(b) {
    let (task,node) = fixture(b.storage.as_ref()).await;
    let (_,foreign) = fixture(b.storage.as_ref()).await;
    let action=|id| WorkerTaskResolutionAction::FailNode {node_id:Some(id),fire_at:Utc::now()};
    assert!(!b.storage.resolve_worker_task(&resolution(&task,action(foreign.id))).await.unwrap());
    assert_eq!(b.storage.get_execution_tree(foreign.instance_id).await.unwrap()[0].state,NodeState::Running);
    let mut other_block=node.clone(); other_block.id=ExecutionNodeId::new(); other_block.block_id=BlockId::new("other");
    b.storage.create_execution_node(&other_block).await.unwrap();
    assert!(!b.storage.resolve_worker_task(&resolution(&task,action(other_block.id))).await.unwrap());
    assert!(b.storage.resolve_worker_task(&resolution(&task,action(node.id))).await.unwrap());
    let tree=b.storage.get_execution_tree(task.instance_id).await.unwrap();
    assert_eq!(tree.iter().find(|n|n.id==node.id).unwrap().state,NodeState::Failed);
    assert_eq!(tree.iter().find(|n|n.id==other_block.id).unwrap().state,NodeState::Running);
});

orch8_storage::backend_test!(async fn legacy_retry_rejects_mixed_targets_atomically(b) {
    let (task,node)=fixture(b.storage.as_ref()).await;
    let (foreign,foreign_node)=fixture(b.storage.as_ref()).await;
    let mut retry=task.clone(); retry.id=uuid::Uuid::new_v4(); retry.attempt=1;
    // Mismatched parent, replacement parent, replacement block, and tree node.
    for variant in 0..4 {
        let mut replacement=retry.clone();
        let parent=if variant==0 {foreign.instance_id} else {task.instance_id};
        if variant==1 { replacement.instance_id=foreign.instance_id; }
        if variant==2 { replacement.block_id=BlockId::new("other"); }
        let target=if variant==3 {foreign_node.id} else {node.id};
        assert!(b.storage.retry_worker_task(task.id,&replacement,Some(target),parent,Utc::now()).await.is_err());
        assert!(b.storage.get_worker_task(task.id).await.unwrap().is_some());
        assert!(b.storage.get_worker_task(retry.id).await.unwrap().is_none());
        assert_eq!(b.storage.get_execution_tree(node.instance_id).await.unwrap()[0].state,NodeState::Running);
        assert_eq!(b.storage.get_execution_tree(foreign_node.instance_id).await.unwrap()[0].state,NodeState::Running);
    }
    b.storage.retry_worker_task(task.id,&retry,Some(node.id),task.instance_id,Utc::now()).await.unwrap();
    assert!(b.storage.get_worker_task(task.id).await.unwrap().is_none());
    assert_eq!(b.storage.get_worker_task(retry.id).await.unwrap().unwrap().attempt,1);
    assert_eq!(b.storage.get_execution_tree(node.instance_id).await.unwrap()[0].state,NodeState::Pending);
});

orch8_storage::backend_test!(async fn resolution_retry_preserves_block_identity_and_valid_retry(b) {
    let (task,node)=fixture(b.storage.as_ref()).await;
    let mut retry=task.clone(); retry.id=uuid::Uuid::new_v4(); retry.attempt=1;
    let mut wrong=retry.clone(); wrong.block_id=BlockId::new("other");
    let action=|task,node_id|WorkerTaskResolutionAction::Retry {retry_task:Box::new(task), retry_output:None,node_id,fire_at:Utc::now()};
    assert!(!b.storage.resolve_worker_task(&resolution(&task,action(wrong,None))).await.unwrap());
    assert!(!b.storage.resolve_worker_task(&resolution(&task,action(retry.clone(),Some(ExecutionNodeId::new())))).await.unwrap());
    assert!(b.storage.resolve_worker_task(&resolution(&task,action(retry.clone(),Some(node.id)))).await.unwrap());
    assert!(b.storage.get_worker_task(task.id).await.unwrap().is_none());
    assert_eq!(b.storage.get_worker_task(retry.id).await.unwrap().unwrap().attempt,1);
    assert_eq!(b.storage.get_execution_tree(node.instance_id).await.unwrap()[0].state,NodeState::Pending);
});
