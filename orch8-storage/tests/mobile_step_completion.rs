//! Waiting answers commit owner authorization, data, signal and wake atomically.
use orch8_storage::{
    MobileInstanceAccess, backend_test,
    testing::{
        backends::TestBackend,
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    context::ExecutionContext,
    ids::BlockId,
    instance::{InstanceState, TaskInstance},
    signal::{Signal, SignalType},
};
use serde_json::json;

async fn waiting(b: &TestBackend, tenant: &str) -> TaskInstance {
    let sequence = SequenceBuilder::new(tenant).step("approve", "noop").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let mut row = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Waiting)
        .data(json!({"private":"ANSWER_CANARY"}))
        .build();
    row.context.runtime.current_step = Some(BlockId::new("approve"));
    row.claimed_at = Some(chrono::Utc::now());
    b.storage.create_instance(&row).await.unwrap();
    b.storage.get_instance(row.id).await.unwrap().unwrap()
}

fn answer(row: &TaskInstance) -> (ExecutionContext, Signal) {
    let mut context = row.context.clone();
    context.data["approved"] = json!(true);
    let signal = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: row.id,
        signal_type: SignalType::Custom("human_input:approve".into()),
        payload: json!({"private":"ANSWER_SIGNAL_CANARY","value":"yes"}),
        delivered: false,
        created_at: chrono::Utc::now(),
        delivered_at: None,
    };
    (context, signal)
}

backend_test!(async fn mobile_step_answer_commits_all_components_and_rejects_replay(b) {
    let row = waiting(&b, "mobile").await;
    let (context, signal) = answer(&row);
    assert!(b.storage.complete_mobile_step(
        &MobileInstanceAccess::Legacy, row.updated_at, &context, &signal,
    ).await.unwrap());
    let after = b.storage.get_instance(row.id).await.unwrap().unwrap();
    assert_eq!(after.context.data, context.data);
    assert_eq!(after.state, InstanceState::Scheduled);
    assert!(after.claimed_at.is_none());
    assert!(after.next_fire_at.is_some());
    let pending = b.storage.get_pending_signals(row.id).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].payload, signal.payload);
    assert!(!b.storage.complete_mobile_step(
        &MobileInstanceAccess::Legacy, row.updated_at, &context, &signal,
    ).await.unwrap());
    b.storage.update_instance_state(row.id, InstanceState::Waiting, None).await.unwrap();
    assert!(!b.storage.complete_mobile_step(
        &MobileInstanceAccess::Legacy, row.updated_at, &context, &signal,
    ).await.unwrap(), "old answer cannot cross a later waiting generation");
    assert_eq!(b.storage.get_pending_signals(row.id).await.unwrap().len(), 1);
});

backend_test!(async fn mobile_step_answer_refuses_foreign_terminal_and_wrong_step_without_mutation(b) {
    for tenant in ["mobile", "other-owner"] {
        let row = waiting(&b, tenant).await;
        let (context, mut signal) = answer(&row);
        if tenant == "mobile" {
            signal.signal_type = SignalType::Custom("human_input:other".into());
            assert!(!b.storage.complete_mobile_step(
                &MobileInstanceAccess::Legacy, row.updated_at, &context, &signal,
            ).await.unwrap());
            signal.signal_type = SignalType::Custom("human_input:approve".into());
            let mut wrong_context = context.clone();
            wrong_context.runtime.current_step = Some(BlockId::new("other"));
            signal.signal_type = SignalType::Custom("human_input:other".into());
            assert!(!b.storage.complete_mobile_step(
                &MobileInstanceAccess::Legacy, row.updated_at, &wrong_context, &signal,
            ).await.unwrap());
        } else {
            assert!(!b.storage.complete_mobile_step(
                &MobileInstanceAccess::Legacy, row.updated_at, &context, &signal,
            ).await.unwrap());
        }
        let after = b.storage.get_instance(row.id).await.unwrap().unwrap();
        assert_eq!(after.context.data, row.context.data);
        assert_eq!(after.updated_at, row.updated_at);
        assert_eq!(after.state, row.state);
        assert!(b.storage.get_pending_signals(row.id).await.unwrap().is_empty());
        b.storage.update_instance_state(row.id, InstanceState::Completed, None).await.unwrap();
        let terminal = b.storage.get_instance(row.id).await.unwrap().unwrap();
        let (context, signal) = answer(&terminal);
        assert!(!b.storage.complete_mobile_step(
            &MobileInstanceAccess::Legacy, terminal.updated_at, &context, &signal,
        ).await.unwrap());
        let after = b.storage.get_instance(row.id).await.unwrap().unwrap();
        assert_eq!(after.state, InstanceState::Completed);
        assert_eq!(after.updated_at, terminal.updated_at);
    }
});

backend_test!(async fn mobile_step_answer_insert_failure_rolls_back_context_and_wake(b) {
    let row = waiting(&b, "mobile").await;
    let (context, signal) = answer(&row);
    let mut collision = signal.clone();
    collision.signal_type = SignalType::Custom("unrelated".into());
    collision.payload = json!({"original":"UNCHANGED"});
    b.storage.enqueue_signal(&collision).await.unwrap();
    assert!(b.storage.complete_mobile_step(
        &MobileInstanceAccess::Legacy, row.updated_at, &context, &signal,
    ).await.is_err());
    let after = b.storage.get_instance(row.id).await.unwrap().unwrap();
    assert_eq!(after.context.data, row.context.data);
    assert_eq!(after.state, row.state);
    assert_eq!(after.claimed_at, row.claimed_at);
    assert_eq!(after.next_fire_at, row.next_fire_at);
    assert_eq!(after.updated_at, row.updated_at);
    let pending = b.storage.get_pending_signals(row.id).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].payload, collision.payload);
});

backend_test!(async fn mobile_step_answer_uses_waiting_tree_nodes_instead_of_shared_telemetry(b) {
    use orch8_types::{execution::{BlockType, ExecutionNode, NodeState}, ids::ExecutionNodeId};
    let row = waiting(&b, "mobile").await;
    // The shared stamp still names an already completed branch.
    let finished = ExecutionNode {
        id: ExecutionNodeId::new(), instance_id: row.id, block_id: BlockId::new("approve"),
        parent_id: None, block_type: BlockType::Step, branch_index: None,
        state: NodeState::Completed, started_at: Some(row.created_at), completed_at: Some(row.updated_at),
    };
    let pending = ExecutionNode {
        id: ExecutionNodeId::new(), instance_id: row.id, block_id: BlockId::new("other"),
        parent_id: None, block_type: BlockType::Step, branch_index: None,
        state: NodeState::Waiting, started_at: Some(row.created_at), completed_at: None,
    };
    b.storage.create_execution_node(&finished).await.unwrap();
    b.storage.create_execution_node(&pending).await.unwrap();
    let (context, mut signal) = answer(&row);
    assert!(!b.storage.complete_mobile_step(
        &MobileInstanceAccess::Legacy, row.updated_at, &context, &signal,
    ).await.unwrap(), "stale telemetry cannot authorize a completed branch");
    let after = b.storage.get_instance(row.id).await.unwrap().unwrap();
    assert_eq!(after.updated_at, row.updated_at);
    assert_eq!(after.context.data, row.context.data);
    signal.signal_type = SignalType::Custom("human_input:other".into());
    assert!(b.storage.complete_mobile_step(
        &MobileInstanceAccess::Legacy, row.updated_at, &context, &signal,
    ).await.unwrap(), "a different durable waiting branch remains answerable");
    assert_eq!(b.storage.get_instance(row.id).await.unwrap().unwrap().state, InstanceState::Scheduled);
    let signals = b.storage.get_pending_signals(row.id).await.unwrap();
    assert_eq!(signals.len(), 1);
    assert_eq!(signals[0].signal_type, signal.signal_type);
});
