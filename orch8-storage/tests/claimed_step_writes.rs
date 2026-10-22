//! A stale step cannot append outputs or replace cached/externalized results.
use chrono::Utc;
use orch8_storage::backend_test;
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, ExecutionNodeId};
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;
use serde_json::json;

fn output(instance_id: orch8_types::ids::InstanceId, owner: &str) -> BlockOutput {
    BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id,
        block_id: BlockId::new("work"),
        output: json!({"owner":owner}),
        output_ref: None,
        output_size: 32,
        attempt: 0,
        created_at: Utc::now(),
    }
}

backend_test!(async fn step_writes_follow_the_running_claim(b) {
    let seq = SequenceBuilder::new("tenant").step("work", "noop").build();
    b.storage.create_sequence(&seq).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&seq).due_now().build();
    b.storage.create_instance(&instance).await.unwrap();
    let node = ExecutionNode {
        id: ExecutionNodeId::new(), instance_id: instance.id, block_id: BlockId::new("work"),
        parent_id: None, block_type: BlockType::Step, branch_index: None,
        state: NodeState::Pending, started_at: None, completed_at: None,
    };
    b.storage.create_execution_node(&node).await.unwrap();
    let now = Utc::now() + chrono::Duration::seconds(1);
    let first = b.storage.claim_due_instances(now, 1, 0).await.unwrap().pop().unwrap();
    let old = first.updated_at;
    assert_eq!(first.claimed_at, Some(old));
    let mut context = first.context.clone();
    context.data = json!({"refreshed": true});
    b.storage.update_instance_context(instance.id, &context).await.unwrap();
    let refreshed = b.storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(refreshed.claimed_at, Some(old));
    assert_eq!(refreshed.context.data, context.data);
    assert!(serde_json::to_value(&refreshed).unwrap().get("claimed_at").is_none());

    assert!(b.storage.update_node_state_claimed(instance.id, node.id, NodeState::Running, old).await.unwrap());
    let first_output = output(instance.id, "first");
    assert!(b.storage.save_block_output_claimed(&first_output, old).await.unwrap());
    assert!(b.storage.set_instance_kv_claimed(instance.id, "_cache:result", &json!("first"), old).await.unwrap());
    assert!(b.storage.save_externalized_state_claimed(instance.id, "work-result", &json!("first"), old).await.unwrap());
    assert!(b.storage.conditional_update_claimed_instance_state(instance.id, old,
        InstanceState::Running, InstanceState::Scheduled, Some(now)).await.unwrap());
    // Cleared/released claims may not write, even before replacement is claimed.
    assert!(!b.storage.save_block_output_claimed(&output(instance.id, "released"), old).await.unwrap());
    let second = b.storage.claim_due_instances(now, 1, 0).await.unwrap().pop().unwrap();
    let current = second.updated_at;
    assert!(b.storage.update_node_state_claimed(instance.id, node.id, NodeState::Completed, current).await.unwrap());
    let completed = b.storage.get_execution_tree(instance.id).await.unwrap().pop().unwrap();
    assert!(!b.storage.update_node_state_claimed(instance.id, node.id, NodeState::Failed, old).await.unwrap());
    let unchanged = b.storage.get_execution_tree(instance.id).await.unwrap().pop().unwrap();
    assert_eq!(unchanged.state, NodeState::Completed);
    assert_eq!(unchanged.started_at, completed.started_at);
    assert_eq!(unchanged.completed_at, completed.completed_at);
    let other = InstanceBuilder::for_sequence(&seq).due_now().build();
    b.storage.create_instance(&other).await.unwrap();
    let mut foreign_node = node.clone();
    foreign_node.id = ExecutionNodeId::new();
    foreign_node.instance_id = other.id;
    b.storage.create_execution_node(&foreign_node).await.unwrap();
    assert!(!b.storage.update_node_state_claimed(instance.id, foreign_node.id, NodeState::Failed, current).await.unwrap());
    assert_eq!(b.storage.get_execution_tree(other.id).await.unwrap()[0].state, NodeState::Pending);
    assert_ne!(old, current);
    let replacement = output(instance.id, "replacement");
    assert!(b.storage.save_block_output_claimed(&replacement, current).await.unwrap());
    assert!(b.storage.set_instance_kv_claimed(instance.id, "_cache:result", &json!("replacement"), current).await.unwrap());
    assert!(b.storage.save_externalized_state_claimed(instance.id, "work-result", &json!("replacement"), current).await.unwrap());
    assert!(!b.storage.save_block_output_claimed(&output(instance.id, "stale"), old).await.unwrap());
    assert!(!b.storage.set_instance_kv_claimed(instance.id, "_cache:result", &json!("stale"), old).await.unwrap());
    assert!(!b.storage.save_externalized_state_claimed(instance.id, "work-result", &json!("stale"), old).await.unwrap());
    assert_eq!(b.storage.get_block_output(instance.id, &BlockId::new("work")).await.unwrap().unwrap().output, replacement.output);
    assert_eq!(b.storage.get_instance_kv(instance.id, "_cache:result").await.unwrap(), Some(json!("replacement")));
    assert_eq!(b.storage.get_externalized_state(instance.id, "work-result").await.unwrap(), Some(json!("replacement")));
    // A failed insert releases its ownership lock and leaves earlier data intact.
    assert!(b.storage.save_block_output_claimed(&replacement, current).await.is_err());
    assert!(b.storage.set_instance_kv_claimed(instance.id, "after-error", &json!(true), current).await.unwrap());
    assert_eq!(b.storage.get_all_outputs(instance.id).await.unwrap().len(), 2);
    for state in [InstanceState::Paused, InstanceState::Completed] {
        // Administrative state changes do not grant an executing step a lease.
        b.storage.update_instance_state(instance.id, state, None).await.unwrap();
        assert!(!b.storage.update_node_state_claimed(instance.id, node.id, NodeState::Failed, current).await.unwrap());
        assert!(!b.storage.save_block_output_claimed(&output(instance.id, "terminal"), current).await.unwrap());
        assert!(!b.storage.set_instance_kv_claimed(instance.id, "_cache:result", &json!("terminal"), current).await.unwrap());
        assert!(!b.storage.save_externalized_state_claimed(instance.id, "work-result", &json!("terminal"), current).await.unwrap());
    }
});
