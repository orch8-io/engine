//! Instance imports are atomic, owner-scoped, idempotent, and race-safe.
use chrono::{Duration, Timelike, Utc};
use orch8_storage::testing::backends::{RawPool, TestBackend};
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder, step};
use orch8_storage::{
    backend_test,
    externalizing::{ExternalizedEntry, marker},
    restore::InstanceSnapshot,
};
use orch8_types::{
    execution::{BlockType, ExecutionNode, NodeState},
    ids::{BlockId, ExecutionNodeId},
    instance::InstanceState,
    output::BlockOutput,
    sequence::{BlockDefinition, ParallelDef, SequenceDefinition},
};
use serde_json::json;

fn sequence() -> SequenceDefinition {
    SequenceBuilder::new("restore")
        .block(BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("root"),
            branches: vec![vec![BlockDefinition::Step(Box::new(step("work", "noop")))]],
        })))
        .build()
}

fn snapshot(sequence: &SequenceDefinition) -> InstanceSnapshot {
    let mut instance = InstanceBuilder::for_sequence(sequence)
        .data(json!({"source": "original"}))
        .build();
    instance.state = InstanceState::Completed;
    let at = (Utc::now() - Duration::days(1))
        .with_nanosecond(123_456_000)
        .unwrap();
    let parent_id = ExecutionNodeId::from_uuid(uuid::Uuid::new_v4());
    let root = ExecutionNode {
        id: parent_id,
        instance_id: instance.id,
        block_id: BlockId::new("root"),
        parent_id: None,
        block_type: BlockType::Parallel,
        branch_index: None,
        state: NodeState::Completed,
        started_at: Some(at),
        completed_at: Some(at + Duration::seconds(1)),
    };
    let child = ExecutionNode {
        id: ExecutionNodeId::from_uuid(uuid::Uuid::new_v4()),
        instance_id: instance.id,
        block_id: BlockId::new("work"),
        parent_id: Some(parent_id),
        block_type: BlockType::Step,
        branch_index: Some(0),
        state: NodeState::Completed,
        started_at: Some(at),
        completed_at: Some(at),
    };
    let output_id = uuid::Uuid::new_v4();
    let reference = format!("{}:work:{output_id}", instance.id);
    let payload = json!({"private": "original-payload".repeat(200)});
    let output = BlockOutput {
        id: output_id,
        instance_id: instance.id,
        block_id: BlockId::new("work"),
        output: marker(&reference),
        output_ref: Some(reference.clone()),
        output_size: u32::try_from(serde_json::to_vec(&payload).unwrap().len()).unwrap(),
        attempt: 0,
        created_at: at,
    };
    let entry = ExternalizedEntry {
        instance_id: instance.id,
        ref_key: reference,
        payload,
        created_at: at,
        expires_at: Some(at + Duration::days(30)),
    };
    // UUID order and archive order need not put parents before their children.
    InstanceSnapshot {
        instance,
        nodes: vec![child, root],
        outputs: vec![output],
        payloads: vec![entry],
    }
}

async fn reject_second_output(backend: &TestBackend) {
    match &backend.raw {
        RawPool::Sqlite(pool) => {
            sqlx::query("CREATE TRIGGER fail_output_import BEFORE INSERT ON block_outputs WHEN NEW.attempt=1 BEGIN SELECT RAISE(ABORT, 'simulated output write failure'); END")
                .execute(pool).await.unwrap();
        }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => {
            sqlx::query("CREATE FUNCTION reject_output_import() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.attempt=1 THEN RAISE EXCEPTION 'simulated output write failure'; END IF; RETURN NEW; END $$")
                .execute(pool).await.unwrap();
            sqlx::query("CREATE TRIGGER fail_output_import BEFORE INSERT ON block_outputs FOR EACH ROW EXECUTE FUNCTION reject_output_import()")
                .execute(pool).await.unwrap();
        }
    }
}

backend_test!(async fn concurrent_restore_publishes_one_complete_instance(b) {
    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let mut snapshot = snapshot(&sequence);
    let (left, right) = tokio::join!(b.storage.restore_instance_snapshot(&snapshot), b.storage.restore_instance_snapshot(&snapshot));
    assert_eq!(u8::from(left.unwrap()) + u8::from(right.unwrap()), 1);
    let id = snapshot.instance.id;
    let stored = b.storage.get_instance(id).await.unwrap().unwrap();
    assert_eq!(stored.context.data, snapshot.instance.context.data);
    let nodes = b.storage.get_execution_tree(id).await.unwrap();
    assert_eq!(nodes.len(), snapshot.nodes.len());
    for expected in &snapshot.nodes {
        let actual = nodes.iter().find(|node| node.id == expected.id).unwrap();
        assert_eq!(actual.parent_id, expected.parent_id);
        assert_eq!(actual.state, expected.state);
        assert_eq!(actual.started_at, expected.started_at);
        assert_eq!(actual.completed_at, expected.completed_at);
    }
    let outputs = b.storage.get_all_outputs(id).await.unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].id, snapshot.outputs[0].id);
    assert_eq!(outputs[0].output, snapshot.outputs[0].output);
    assert_eq!(outputs[0].created_at, snapshot.outputs[0].created_at);
    assert_eq!(b.storage.list_externalized_entries(id, None, 10).await.unwrap(), snapshot.payloads);
    snapshot.instance.context.data = json!({"overwrite": "forbidden"});
    snapshot.payloads[0].payload = json!("do not replace existing data");
    assert!(!b.storage.restore_instance_snapshot(&snapshot).await.unwrap());
    assert_eq!(b.storage.get_instance(id).await.unwrap().unwrap().context.data, stored.context.data);
    assert_eq!(b.storage.get_externalized_state(id, &snapshot.payloads[0].ref_key).await.unwrap(), Some(json!({"private": "original-payload".repeat(200)})));
});

backend_test!(async fn payload_collision_rolls_back_the_entire_import(b) {
    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let snapshot = snapshot(&sequence);
    let other = InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&other).await.unwrap();
    let mut original = snapshot.payloads[0].clone();
    original.instance_id = other.id;
    original.payload = json!({"owner": "existing"});
    b.storage.restore_externalized_entry(&original).await.unwrap();
    assert!(b.storage.restore_instance_snapshot(&snapshot).await.is_err());
    assert!(b.storage.get_instance(snapshot.instance.id).await.unwrap().is_none());
    assert!(b.storage.get_execution_tree(snapshot.instance.id).await.unwrap().is_empty());
    assert!(b.storage.get_all_outputs(snapshot.instance.id).await.unwrap().is_empty());
    assert!(b.storage.list_externalized_entries(snapshot.instance.id, None, 10).await.unwrap().is_empty());
    assert_eq!(b.storage.get_externalized_state(other.id, &original.ref_key).await.unwrap(), Some(original.payload));
});

backend_test!(async fn output_failure_rolls_back_instance_nodes_and_payloads(b) {
    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let mut snapshot = snapshot(&sequence);
    let mut second = snapshot.outputs[0].clone();
    second.id = uuid::Uuid::new_v4();
    second.attempt = 1;
    snapshot.outputs.push(second);
    reject_second_output(&b).await;
    let error = b.storage.restore_instance_snapshot(&snapshot).await.unwrap_err();
    assert!(error.to_string().contains("simulated output write failure"));
    let id = snapshot.instance.id;
    assert!(b.storage.get_instance(id).await.unwrap().is_none());
    assert!(b.storage.get_execution_tree(id).await.unwrap().is_empty());
    assert!(b.storage.get_all_outputs(id).await.unwrap().is_empty());
    assert!(b.storage.list_externalized_entries(id, None, 10).await.unwrap().is_empty());
});

backend_test!(async fn invalid_snapshot_rows_never_create_an_instance(b) {
    let sequence = sequence();
    b.storage.create_sequence(&sequence).await.unwrap();
    let source = snapshot(&sequence);
    let other = InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&other).await.unwrap();
    let mut invalid = Vec::new();
    let mut node_owner = source.clone(); node_owner.nodes[0].instance_id = other.id; invalid.push(node_owner);
    let mut output_owner = source.clone(); output_owner.outputs[0].instance_id = other.id; invalid.push(output_owner);
    let mut payload_owner = source.clone(); payload_owner.payloads[0].instance_id = other.id; invalid.push(payload_owner);
    let mut duplicate_node = source.clone(); duplicate_node.nodes.push(source.nodes[0].clone()); invalid.push(duplicate_node);
    let mut duplicate_output = source.clone(); duplicate_output.outputs.push(source.outputs[0].clone()); invalid.push(duplicate_output);
    let mut duplicate_ref = source.clone(); duplicate_ref.payloads.push(source.payloads[0].clone()); invalid.push(duplicate_ref);
    let mut missing_parent = source.clone(); missing_parent.nodes[0].parent_id = Some(ExecutionNodeId::new()); invalid.push(missing_parent);
    let mut cycle = source.clone(); cycle.nodes[1].parent_id = Some(cycle.nodes[0].id); invalid.push(cycle);
    for snapshot in invalid {
        assert!(b.storage.restore_instance_snapshot(&snapshot).await.is_err());
        assert!(b.storage.get_instance(source.instance.id).await.unwrap().is_none());
    }
});
