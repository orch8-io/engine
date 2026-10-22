//! Child adoption and retry commit with the parent's original claim.
use chrono::{DateTime, Utc};
use orch8_storage::testing::{
    backends::RawPool,
    fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_storage::{ChildAdoption, StorageBackend, backend_test};
use orch8_types::{
    execution::{BlockType, ExecutionNode, NodeState},
    ids::{BlockId, ExecutionNodeId, InstanceId},
    instance::{InstanceState, TaskInstance},
    output::BlockOutput,
    sequence::SequenceDefinition,
};
use serde_json::json;

struct AdoptionFixture {
    sequence: SequenceDefinition,
    parent: InstanceId,
    token: DateTime<Utc>,
    child: TaskInstance,
    real: BlockOutput,
}

async fn fixture(store: &dyn StorageBackend) -> AdoptionFixture {
    let sequence = SequenceBuilder::new("adopt-parent")
        .step("work", "noop")
        .build();
    store.create_sequence(&sequence).await.unwrap();
    let parent = InstanceBuilder::for_sequence(&sequence).due_now().build();
    store.create_instance(&parent).await.unwrap();
    let claimed = store
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let mut child = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Failed)
        .build();
    child.parent_instance_id = Some(parent.id);
    child.metadata =
        json!({"_parent_activation":"old", "_parent_block_id":"spawn", "keep":{"value":true}});
    child.context.data = json!({"private_child_data":"retained"});
    child.context.runtime.run_id = Some("old-run".into());
    child.context.runtime.total_steps_executed = 10;
    child.context.runtime.current_step = Some(BlockId::new("work"));
    child.context.runtime.started_at = Some(Utc::now());
    child.context.runtime.current_step_started_at = Some(Utc::now());
    store.create_instance(&child).await.unwrap();
    store
        .create_execution_node(&ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id: child.id,
            block_id: BlockId::new("work"),
            parent_id: None,
            block_type: BlockType::Step,
            branch_index: None,
            state: NodeState::Failed,
            started_at: Some(Utc::now()),
            completed_at: Some(Utc::now()),
        })
        .await
        .unwrap();
    let real = BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: child.id,
        block_id: BlockId::new("work"),
        output: json!({"retained":"real output"}),
        output_ref: None,
        output_size: 26,
        attempt: 0,
        created_at: Utc::now(),
    };
    store.save_block_output(&real).await.unwrap();
    for reference in ["__in_progress__", "__error__"] {
        let mut marker = real.clone();
        marker.id = uuid::Uuid::now_v7();
        marker.output_ref = Some(reference.into());
        marker.created_at = Utc::now();
        store.save_block_output(&marker).await.unwrap();
    }
    AdoptionFixture {
        sequence,
        parent: parent.id,
        token: claimed.claimed_at.unwrap(),
        child,
        real,
    }
}

fn adopted(result: Option<ChildAdoption>) -> TaskInstance {
    match result {
        Some(ChildAdoption::Adopted(child)) => *child,
        other => panic!("expected committed adoption: {other:?}"),
    }
}

backend_test!(async fn adoption_retries_failed_children_and_rejects_stale_parent_claims(b) {
    let f = fixture(b.storage.as_ref()).await;
    let child = adopted(b.storage.adopt_child_instance_claimed(f.parent, f.child.id, "old", "new", f.token).await.unwrap());
    assert_eq!(child.state, InstanceState::Scheduled);
    assert!(child.next_fire_at.is_some());
    assert_eq!(child.metadata["_parent_activation"], "new");
    assert_eq!(child.metadata["keep"], f.child.metadata["keep"]);
    assert_eq!(child.context.data, f.child.context.data);
    assert_ne!(child.context.runtime.run_id, f.child.context.runtime.run_id);
    assert_eq!(child.context.runtime.total_steps_executed, 0);
    assert!(child.context.runtime.current_step.is_none());
    assert!(child.context.runtime.current_step_started_at.is_none());
    assert!(child.context.runtime.started_at.is_none());
    assert!(b.storage.get_execution_tree(child.id).await.unwrap().is_empty());
    let outputs = b.storage.get_all_outputs(child.id).await.unwrap();
    assert_eq!(outputs.len(), 1); assert_eq!(outputs[0].id, f.real.id);
    assert_eq!(outputs[0].output, f.real.output);
    assert_eq!(serde_json::to_value(b.storage.get_instance(child.id).await.unwrap().unwrap()).unwrap(), serde_json::to_value(&child).unwrap());
    let replay = adopted(b.storage.adopt_child_instance_claimed(f.parent, child.id, "old", "new", f.token).await.unwrap());
    assert_eq!(serde_json::to_value(&replay).unwrap(), serde_json::to_value(&child).unwrap());
    // A response-loss replay after the child fails again must not launch a
    // second retry under the same adoption request.
    b.storage.update_instance_state(child.id, InstanceState::Failed, None).await.unwrap();
    let failed = b.storage.get_instance(child.id).await.unwrap().unwrap();
    let replay = adopted(b.storage.adopt_child_instance_claimed(f.parent, child.id, "old", "new", f.token).await.unwrap());
    assert_eq!(serde_json::to_value(&replay).unwrap(), serde_json::to_value(&failed).unwrap());
    b.storage.update_instance_state(child.id, InstanceState::Completed, None).await.unwrap();
    let before = serde_json::to_value(b.storage.get_instance(child.id).await.unwrap().unwrap()).unwrap();
    let next = Utc::now() + chrono::Duration::seconds(2);
    assert!(b.storage.conditional_update_claimed_instance_state(f.parent, f.token, InstanceState::Running, InstanceState::Scheduled, Some(next)).await.unwrap());
    let replacement = b.storage.claim_due_instances(next, 1, 0).await.unwrap().pop().unwrap();
    assert_eq!(replacement.id, f.parent); let current = replacement.claimed_at.unwrap();
    assert_ne!(current, f.token);
    assert!(b.storage.adopt_child_instance_claimed(f.parent, child.id, "new", "stale", f.token).await.unwrap().is_none());
    assert_eq!(serde_json::to_value(b.storage.get_instance(child.id).await.unwrap().unwrap()).unwrap(), before);
    let completed = adopted(b.storage.adopt_child_instance_claimed(f.parent, child.id, "new", "current", current).await.unwrap());
    assert_eq!(completed.state, InstanceState::Completed);
    assert_eq!(completed.context.runtime.run_id, child.context.runtime.run_id);
    assert_eq!(completed.context.data, f.child.context.data);
});

backend_test!(async fn changed_cancelled_superseded_or_foreign_children_are_ineligible(b) {
    let f = fixture(b.storage.as_ref()).await;
    let other = InstanceBuilder::for_sequence(&f.sequence).state(InstanceState::Completed).build();
    b.storage.create_instance(&other).await.unwrap();
    let mut cancelled = f.child.clone(); cancelled.id = InstanceId::new(); cancelled.state = InstanceState::Cancelled;
    let mut superseded = f.child.clone(); superseded.id = InstanceId::new(); superseded.metadata["_superseded"] = serde_json::Value::Null;
    let mut changed = f.child.clone(); changed.id = InstanceId::new(); changed.metadata["_parent_activation"] = json!("changed");
    let mut foreign = f.child.clone(); foreign.id = InstanceId::new(); foreign.parent_instance_id = Some(other.id);
    for child in [cancelled, superseded, changed, foreign] {
        b.storage.create_instance(&child).await.unwrap();
        let before = serde_json::to_value(b.storage.get_instance(child.id).await.unwrap().unwrap()).unwrap();
        assert!(matches!(b.storage.adopt_child_instance_claimed(f.parent, child.id, "old", "new", f.token).await.unwrap(), Some(ChildAdoption::Ineligible)));
        assert_eq!(serde_json::to_value(b.storage.get_instance(child.id).await.unwrap().unwrap()).unwrap(), before);
    }
    assert!(matches!(b.storage.adopt_child_instance_claimed(f.parent, InstanceId::new(), "old", "new", f.token).await.unwrap(), Some(ChildAdoption::Ineligible)));
    assert_eq!(b.storage.get_instance(f.parent).await.unwrap().unwrap().claimed_at, Some(f.token));
});

backend_test!(async fn a_late_retry_error_restores_child_tree_outputs_context_and_activation(b) {
    let f = fixture(b.storage.as_ref()).await;
    let before = serde_json::to_value(b.storage.get_instance(f.child.id).await.unwrap().unwrap()).unwrap();
    let tree = serde_json::to_value(b.storage.get_execution_tree(f.child.id).await.unwrap()).unwrap();
    let outputs = serde_json::to_value(b.storage.get_all_outputs(f.child.id).await.unwrap()).unwrap();
    match &b.raw {
        RawPool::Sqlite(pool) => {
            sqlx::query("CREATE TRIGGER reject_adoption BEFORE UPDATE OF state ON task_instances WHEN OLD.state='failed' AND NEW.state='scheduled' BEGIN SELECT RAISE(ABORT,'simulated adoption scheduling failure'); END").execute(pool).await.unwrap();
        }
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool) => {
            sqlx::query("CREATE FUNCTION reject_adoption() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'simulated adoption scheduling failure'; END $$").execute(pool).await.unwrap();
            sqlx::query("CREATE TRIGGER reject_adoption BEFORE UPDATE OF state ON task_instances FOR EACH ROW WHEN (OLD.state='failed' AND NEW.state='scheduled') EXECUTE FUNCTION reject_adoption()").execute(pool).await.unwrap();
        }
    }
    let error = b.storage.adopt_child_instance_claimed(f.parent, f.child.id, "old", "new", f.token).await.unwrap_err();
    assert!(error.to_string().contains("simulated adoption scheduling failure"));
    assert_eq!(serde_json::to_value(b.storage.get_instance(f.child.id).await.unwrap().unwrap()).unwrap(), before);
    assert_eq!(serde_json::to_value(b.storage.get_execution_tree(f.child.id).await.unwrap()).unwrap(), tree);
    assert_eq!(serde_json::to_value(b.storage.get_all_outputs(f.child.id).await.unwrap()).unwrap(), outputs);
    assert_eq!(b.storage.get_instance(f.parent).await.unwrap().unwrap().claimed_at, Some(f.token));
    match &b.raw {
        RawPool::Sqlite(pool) => { sqlx::query("DROP TRIGGER reject_adoption").execute(pool).await.unwrap(); }
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool) => { sqlx::query("DROP TRIGGER reject_adoption ON task_instances").execute(pool).await.unwrap(); }
    }
    assert_eq!(adopted(b.storage.adopt_child_instance_claimed(f.parent, f.child.id, "old", "new", f.token).await.unwrap()).state, InstanceState::Scheduled);
});

backend_test!(async fn adopting_a_live_child_preserves_its_execution_claim_and_work(b) {
    let f = fixture(b.storage.as_ref()).await;
    b.storage.update_instance_state(f.child.id, InstanceState::Scheduled, Some(Utc::now())).await.unwrap();
    let live = b.storage.claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0).await.unwrap().pop().unwrap();
    assert_eq!(live.id, f.child.id);
    let token = live.claimed_at.unwrap();
    let tree = serde_json::to_value(b.storage.get_execution_tree(live.id).await.unwrap()).unwrap();
    let outputs = serde_json::to_value(b.storage.get_all_outputs(live.id).await.unwrap()).unwrap();
    let rebound = adopted(b.storage.adopt_child_instance_claimed(f.parent, live.id, "old", "new", f.token).await.unwrap());
    assert_eq!(rebound.state, InstanceState::Running);
    assert_eq!(rebound.claimed_at, Some(token));
    assert_eq!(serde_json::to_value(&rebound.context).unwrap(), serde_json::to_value(&live.context).unwrap());
    assert_eq!(serde_json::to_value(b.storage.get_execution_tree(live.id).await.unwrap()).unwrap(), tree);
    assert_eq!(serde_json::to_value(b.storage.get_all_outputs(live.id).await.unwrap()).unwrap(), outputs);
    // The child worker's original result still belongs to that worker: parent
    // adoption must not silently revoke or impersonate the child's claim.
    let mut result = f.real.clone(); result.id = uuid::Uuid::now_v7(); result.created_at = Utc::now();
    assert!(b.storage.save_block_output_claimed(&result, token).await.unwrap());
});
