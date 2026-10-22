//! In-flight scheduler bookkeeping must retain the original running claim.
use chrono::Utc;
use orch8_engine::testing::TestEngine;
use orch8_storage::{
    StorageBackend,
    sqlite::SqliteStorage,
    testing::{
        faulty::FaultStorage,
        fixtures::{SequenceBuilder, step},
    },
};
use orch8_types::{
    ids::{BlockId, InstanceId},
    instance::{InstanceState, TaskInstance},
    output::BlockOutput,
    sequence::{BlockDefinition, ParallelDef},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[derive(Clone, Copy)]
enum Boundary {
    RunStart,
    FlatStepStamp,
    TreeGateStamp,
}

async fn replacement(
    store: &dyn StorageBackend,
    id: InstanceId,
    boundary: Boundary,
) -> TaskInstance {
    let old = store.get_instance(id).await.unwrap().unwrap();
    assert_eq!(old.state, InstanceState::Running);
    let token = old.claimed_at.unwrap();
    let next = Utc::now() + chrono::Duration::seconds(2);
    assert!(
        store
            .conditional_update_claimed_instance_state(
                id,
                token,
                InstanceState::Running,
                InstanceState::Scheduled,
                Some(next)
            )
            .await
            .unwrap()
    );
    let mut current = store
        .claim_due_instances(next, 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(current.id, id);
    let claim = current.claimed_at.unwrap();
    assert_ne!(claim, token);
    current.context.data = json!({"owner":"replacement"});
    match boundary {
        Boundary::RunStart => {
            assert!(current.context.runtime.run_id.is_none());
            assert!(current.context.runtime.started_at.is_none());
        }
        Boundary::FlatStepStamp | Boundary::TreeGateStamp => {
            current.context.runtime.current_step = Some(BlockId::new("next"));
            current.context.runtime.current_step_started_at = Some(next);
            assert!(
                store
                    .save_block_output_claimed(
                        &BlockOutput {
                            id: uuid::Uuid::now_v7(),
                            instance_id: id,
                            block_id: BlockId::new("work"),
                            output: json!({"owner":"replacement"}),
                            output_ref: None,
                            output_size: 23,
                            attempt: 0,
                            created_at: Utc::now(),
                        },
                        claim
                    )
                    .await
                    .unwrap()
            );
        }
    }
    if matches!(boundary, Boundary::TreeGateStamp) {
        for node in store.get_execution_tree(id).await.unwrap() {
            let state = match node.block_id.as_str() {
                "work" => Some(orch8_types::execution::NodeState::Completed),
                "next" => Some(orch8_types::execution::NodeState::Running),
                _ => None,
            };
            if let Some(state) = state {
                assert!(
                    store
                        .update_node_state_claimed(id, node.id, state, claim)
                        .await
                        .unwrap()
                );
            }
        }
    }
    assert_eq!(
        store
            .update_instance_context_cas_claimed(id, &current.context, current.updated_at, claim)
            .await
            .unwrap(),
        Some(true)
    );
    store.get_instance(id).await.unwrap().unwrap()
}

fn metadata_sequence(boundary: Boundary) -> orch8_types::sequence::SequenceDefinition {
    let mut work = step("work", "noop");
    // The run-start case skips the step before its context stamp, isolating
    // the earlier run identity write from the independently unsafe stamp.
    work.when = Some(
        match boundary {
            Boundary::RunStart => "false",
            Boundary::FlatStepStamp | Boundary::TreeGateStamp => "true",
        }
        .into(),
    );
    if matches!(boundary, Boundary::TreeGateStamp) {
        work.wait_for_input = Some(serde_json::from_value(json!({"prompt":"approve"})).unwrap());
    }
    let work = BlockDefinition::Step(Box::new(work));
    let mut sequence = SequenceBuilder::new("metadata-handoff");
    sequence = if matches!(boundary, Boundary::TreeGateStamp) {
        sequence.block(BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("root"),
            branches: vec![vec![
                work,
                BlockDefinition::Step(Box::new(step("next", "noop"))),
            ]],
        })))
    } else {
        let mut sequence = sequence.block(work);
        if matches!(boundary, Boundary::FlatStepStamp) {
            sequence = sequence.step("next", "noop");
        }
        sequence
    };
    sequence.build()
}

async fn metadata_handoff(boundary: Boundary) {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    let method = match boundary {
        Boundary::RunStart => "ensure_instance_run_started",
        Boundary::FlatStepStamp => "get_all_outputs",
        Boundary::TreeGateStamp => "update_instance_context_cas",
    };
    let pause = faults.on(method).times(1).pause();
    let guarded = match boundary {
        Boundary::RunStart => Some(
            faults
                .on("ensure_instance_run_started_claimed")
                .times(1)
                .pause(),
        ),
        Boundary::FlatStepStamp => None,
        Boundary::TreeGateStamp => Some(
            faults
                .on("update_instance_context_cas_claimed")
                .times(1)
                .pause(),
        ),
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let engine = Arc::new(
        TestEngine::builder()
            .storage(storage)
            .register("noop", move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                async { Ok(json!({})) }
            })
            .build()
            .await,
    );
    let sequence = metadata_sequence(boundary);
    let id = engine.start(&sequence).await;
    let old_engine = Arc::clone(&engine);
    let mut old_tick = tokio::spawn(async move { old_engine.try_tick().await });
    tokio::select! {
        () = pause.entered() => {},
        () = async {
            if let Some(guard) = &guarded { guard.entered().await; }
            else { std::future::pending::<()>().await; }
        } => {},
        result = &mut old_tick => panic!("tick ended before metadata rendezvous: {result:?}"),
        () = tokio::time::sleep(Duration::from_secs(5)) => panic!("tick did not reach metadata rendezvous"),
    }
    let before = replacement(inner.as_ref(), id, boundary).await;
    let outputs = serde_json::to_value(inner.get_all_outputs(id).await.unwrap()).unwrap();
    let tree = serde_json::to_value(inner.get_execution_tree(id).await.unwrap()).unwrap();
    pause.release();
    if let Some(guard) = guarded {
        guard.release();
    }
    tokio::time::timeout(Duration::from_secs(5), old_tick)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let after = inner.get_instance(id).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(&after.context).unwrap(),
        serde_json::to_value(&before.context).unwrap(),
        "stale scheduler overwrote replacement execution bookkeeping"
    );
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(after.state, before.state);
    assert_eq!(after.claimed_at, before.claimed_at);
    assert_eq!(
        serde_json::to_value(inner.get_all_outputs(id).await.unwrap()).unwrap(),
        outputs
    );
    assert_eq!(
        serde_json::to_value(inner.get_execution_tree(id).await.unwrap()).unwrap(),
        tree
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "stale preamble reached a handler"
    );
}

#[tokio::test]
async fn stale_scheduler_cannot_start_a_replacement_executions_run() {
    metadata_handoff(Boundary::RunStart).await;
}

#[tokio::test]
async fn stale_flat_step_cannot_replace_the_current_step_or_timeout_baseline() {
    metadata_handoff(Boundary::FlatStepStamp).await;
}

#[tokio::test]
async fn stale_tree_gate_cannot_replace_the_current_step_or_timeout_baseline() {
    metadata_handoff(Boundary::TreeGateStamp).await;
}
