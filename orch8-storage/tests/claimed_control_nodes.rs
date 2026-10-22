//! Composite tree batches serialize with claim recovery and never cross owners.
use chrono::{DateTime, Utc};
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder, step};
use orch8_storage::{StorageBackend, backend_test};
use orch8_types::{
    execution::{BlockType, ExecutionNode, NodeState},
    ids::{BlockId, ExecutionNodeId, InstanceId},
    instance::InstanceState,
    sequence::{BlockDefinition, ParallelDef, SequenceDefinition},
};

async fn claimed(
    store: &dyn StorageBackend,
) -> (
    SequenceDefinition,
    InstanceId,
    DateTime<Utc>,
    Vec<ExecutionNode>,
) {
    let sequence = SequenceBuilder::new("claimed-tree")
        .block(BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("root"),
            branches: vec![vec![BlockDefinition::Step(Box::new(step("work", "noop")))]],
        })))
        .build();
    store.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    store.create_instance(&instance).await.unwrap();
    let current = store
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let parent = ExecutionNodeId::new();
    let nodes = vec![
        ExecutionNode {
            id: parent,
            instance_id: instance.id,
            block_id: BlockId::new("root"),
            parent_id: None,
            block_type: BlockType::Parallel,
            branch_index: None,
            state: NodeState::Pending,
            started_at: None,
            completed_at: None,
        },
        ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id: instance.id,
            block_id: BlockId::new("work"),
            parent_id: Some(parent),
            block_type: BlockType::Step,
            branch_index: Some(0),
            state: NodeState::Pending,
            started_at: None,
            completed_at: None,
        },
    ];
    (sequence, instance.id, current.claimed_at.unwrap(), nodes)
}

backend_test!(async fn tree_batches_follow_the_original_running_claim(b) {
    let (_, id, token, nodes) = claimed(b.storage.as_ref()).await;
    assert!(b.storage.create_execution_nodes_batch_claimed(id, &nodes, token).await.unwrap());
    let ids = vec![nodes[0].id, nodes[1].id, nodes[0].id];
    assert!(b.storage.batch_activate_nodes_claimed(id, &ids, token).await.unwrap());
    let active = b.storage.get_execution_tree(id).await.unwrap();
    assert!(active.iter().all(|node| node.state == NodeState::Running && node.started_at.is_some()));
    assert!(b.storage.batch_activate_nodes_claimed(id, &ids, token).await.unwrap());
    assert_eq!(serde_json::to_value(&active).unwrap(), serde_json::to_value(b.storage.get_execution_tree(id).await.unwrap()).unwrap());
    let now = Utc::now() + chrono::Duration::seconds(1);
    assert!(b.storage.conditional_update_claimed_instance_state(id, token, InstanceState::Running, InstanceState::Scheduled, Some(now)).await.unwrap());
    assert!(!b.storage.batch_activate_nodes_claimed(id, &[], token).await.unwrap());
    let replacement = b.storage.claim_due_instances(now, 1, 0).await.unwrap().pop().unwrap();
    let current = replacement.claimed_at.unwrap();
    assert_ne!(current, token);
    assert!(b.storage.update_nodes_state_claimed(id, &ids, NodeState::Completed, current).await.unwrap());
    let completed = serde_json::to_value(b.storage.get_execution_tree(id).await.unwrap()).unwrap();
    assert!(!b.storage.update_nodes_state_claimed(id, &ids, NodeState::Pending, token).await.unwrap());
    assert!(!b.storage.batch_activate_nodes_claimed(id, &ids, token).await.unwrap());
    let mut extra = nodes[0].clone(); extra.id = ExecutionNodeId::new();
    assert!(!b.storage.create_execution_nodes_batch_claimed(id, &[extra], token).await.unwrap());
    assert!(!b.storage.create_execution_nodes_batch_claimed(id, &[], token).await.unwrap());
    assert_eq!(serde_json::to_value(b.storage.get_execution_tree(id).await.unwrap()).unwrap(), completed);
    assert!(b.storage.create_execution_nodes_batch_claimed(id, &[], current).await.unwrap());
});

backend_test!(async fn mixed_owner_batches_and_foreign_parents_leave_every_row_unchanged(b) {
    let (sequence, id, token, nodes) = claimed(b.storage.as_ref()).await;
    assert!(b.storage.create_execution_nodes_batch_claimed(id, &nodes, token).await.unwrap());
    let other = InstanceBuilder::for_sequence(&sequence).state(InstanceState::Completed).build();
    b.storage.create_instance(&other).await.unwrap();
    let mut foreign = nodes[0].clone(); foreign.id = ExecutionNodeId::new(); foreign.instance_id = other.id;
    b.storage.create_execution_node(&foreign).await.unwrap();
    let before = serde_json::to_value(b.storage.get_execution_tree(id).await.unwrap()).unwrap();
    let ids = [nodes[0].id, foreign.id];
    assert!(!b.storage.update_nodes_state_claimed(id, &ids, NodeState::Failed, token).await.unwrap());
    assert!(!b.storage.batch_activate_nodes_claimed(id, &ids, token).await.unwrap());
    assert!(!b.storage.update_nodes_state_claimed(id, &[ExecutionNodeId::new()], NodeState::Failed, token).await.unwrap());
    let mut child = nodes[1].clone(); child.id = ExecutionNodeId::new(); child.parent_id = Some(foreign.id);
    assert!(!b.storage.create_execution_nodes_batch_claimed(id, &[child], token).await.unwrap());
    let mut foreign_input = foreign.clone(); foreign_input.id = ExecutionNodeId::new();
    assert!(!b.storage.create_execution_nodes_batch_claimed(id, &[foreign_input], token).await.unwrap());
    assert_eq!(serde_json::to_value(b.storage.get_execution_tree(id).await.unwrap()).unwrap(), before);
    assert_eq!(b.storage.get_execution_tree(other.id).await.unwrap()[0].state, NodeState::Pending);
});

backend_test!(async fn a_late_batch_insert_error_rolls_back_every_new_node(b) {
    let (_, id, token, nodes) = claimed(b.storage.as_ref()).await;
    assert!(b.storage.create_execution_nodes_batch_claimed(id, &nodes, token).await.unwrap());
    let before = serde_json::to_value(b.storage.get_execution_tree(id).await.unwrap()).unwrap();
    let mut extra = nodes[0].clone(); extra.id = ExecutionNodeId::new();
    // SQLite inserts the first node before the second hits an existing id.
    assert!(b.storage.create_execution_nodes_batch_claimed(id, &[extra, nodes[0].clone()], token).await.is_err());
    assert_eq!(serde_json::to_value(b.storage.get_execution_tree(id).await.unwrap()).unwrap(), before);
    assert_eq!(b.storage.get_instance(id).await.unwrap().unwrap().claimed_at, Some(token));
});

backend_test!(async fn context_writes_distinguish_contention_from_ownership_loss(b) {
    let (_, id, token, _) = claimed(b.storage.as_ref()).await;
    let first = b.storage.get_instance(id).await.unwrap().unwrap();
    let mut context = first.context.clone(); context.data = serde_json::json!({"owner":"first"});
    assert_eq!(b.storage.update_instance_context_cas_claimed(id, &context, first.updated_at - chrono::Duration::days(1), token).await.unwrap(), Some(false));
    assert_eq!(b.storage.update_instance_context_cas_claimed(id, &context, first.updated_at, token).await.unwrap(), Some(true));
    assert!(b.storage.merge_context_data_claimed(id, "joined", &serde_json::json!(true), token).await.unwrap());
    let now = Utc::now() + chrono::Duration::seconds(1);
    assert!(b.storage.conditional_update_claimed_instance_state(id, token, InstanceState::Running, InstanceState::Scheduled, Some(now)).await.unwrap());
    let replacement = b.storage.claim_due_instances(now, 1, 0).await.unwrap().pop().unwrap();
    let current = replacement.claimed_at.unwrap();
    assert!(b.storage.merge_context_data_claimed(id, "owner", &serde_json::json!("replacement"), current).await.unwrap());
    b.storage.merge_context_data(id, "manual", &serde_json::json!(true)).await.unwrap();
    let before = b.storage.get_instance(id).await.unwrap().unwrap();
    // Even a fresh context CAS timestamp does not confer the new owner's claim.
    assert_eq!(b.storage.update_instance_context_cas_claimed(id, &context, before.updated_at, token).await.unwrap(), None);
    assert!(!b.storage.merge_context_data_claimed(id, "owner", &serde_json::json!("stale"), token).await.unwrap());
    let after = b.storage.get_instance(id).await.unwrap().unwrap();
    assert_eq!(after.context.data, before.context.data);
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(after.claimed_at, Some(current));
    assert_eq!(after.context.data["owner"], "replacement");
    assert_eq!(after.context.data["joined"], true);
    assert_eq!(after.context.data["manual"], true);
});

backend_test!(async fn concurrent_claimed_context_merges_keep_every_key(b) {
    let (_, id, token, _) = claimed(b.storage.as_ref()).await;
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..8 {
        let store = std::sync::Arc::clone(&b.storage);
        tasks.spawn(async move { store.merge_context_data_claimed(id, &format!("key{index}"), &serde_json::json!(index), token).await });
    }
    while let Some(result) = tasks.join_next().await { assert!(result.unwrap().unwrap()); }
    let instance = b.storage.get_instance(id).await.unwrap().unwrap();
    for index in 0..8 { assert_eq!(instance.context.data[format!("key{index}")], index); }
});

fn worker_task(id: InstanceId) -> orch8_types::worker::WorkerTask {
    orch8_types::worker::WorkerTask {
        id: uuid::Uuid::now_v7(),
        instance_id: id,
        block_id: BlockId::new("work"),
        handler_name: "remote".into(),
        queue_name: None,
        requirements: orch8_types::continuity::CapsuleRequirements::default(),
        params: serde_json::json!({}),
        context: serde_json::json!({}),
        attempt: 1,
        timeout_ms: None,
        state: orch8_types::worker::WorkerTaskState::Pending,
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
        carries_credentials: false,
        claimed_runtime_kind: None,
    }
}

backend_test!(async fn worker_cleanup_and_its_events_follow_the_current_claim(b) {
    let (_, id, token, _) = claimed(b.storage.as_ref()).await;
    let first = worker_task(id);
    b.storage.create_worker_task(&first).await.unwrap();
    let blocks = vec!["work".to_string()];
    assert_eq!(b.storage.cancel_worker_tasks_for_blocks_claimed(id, &blocks, token).await.unwrap(), Some(1));
    assert!(b.storage.get_worker_task(first.id).await.unwrap().is_none());
    let events = b.storage.list_worker_task_attempt_events(first.id, 10).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event, orch8_types::worker::WorkerAttemptEventKind::Cancelled);
    let now = Utc::now() + chrono::Duration::seconds(1);
    assert!(b.storage.conditional_update_claimed_instance_state(id, token, InstanceState::Running, InstanceState::Scheduled, Some(now)).await.unwrap());
    let replacement = b.storage.claim_due_instances(now, 1, 0).await.unwrap().pop().unwrap();
    let current = replacement.claimed_at.unwrap();
    let fresh = worker_task(id);
    b.storage.create_worker_task(&fresh).await.unwrap();
    assert_eq!(b.storage.cancel_worker_tasks_for_blocks_claimed(id, &blocks, token).await.unwrap(), None);
    assert!(b.storage.get_worker_task(fresh.id).await.unwrap().is_some());
    assert!(b.storage.list_worker_task_attempt_events(fresh.id, 10).await.unwrap().is_empty());
    assert_eq!(b.storage.cancel_worker_tasks_for_blocks_claimed(id, &blocks, current).await.unwrap(), Some(1));
    assert_eq!(b.storage.list_worker_task_attempt_events(fresh.id, 10).await.unwrap().len(), 1);
});
