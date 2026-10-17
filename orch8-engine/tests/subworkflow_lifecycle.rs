//! Sub-workflow lifecycle regressions from the long-run review (L3-05,
//! L3-07, L3-11), driven through the real scheduler on virtual time
//! ([`TestEngine`]) on SQLite and, when `DATABASE_URL` is set, Postgres.

use serde_json::json;

use orch8_engine::testing::TestEngine;
use orch8_storage::testing::backends::{BackendKind, TestBackend};
use orch8_storage::testing::fixtures::{SequenceBuilder, step};
use orch8_types::ids::InstanceId;
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, SubSequenceDef};

const KINDS: [BackendKind; 2] = [BackendKind::Sqlite, BackendKind::Postgres];

/// A step that parks the instance in `Waiting` until a human-input signal.
fn gate(id: &str) -> BlockDefinition {
    let mut s = step(id, "noop");
    s.wait_for_input = Some(serde_json::from_value(json!({"prompt": "go?"})).unwrap());
    BlockDefinition::Step(Box::new(s))
}

fn sub_sequence(id: &str, target: &SequenceDefinition) -> BlockDefinition {
    BlockDefinition::SubSequence(Box::new(SubSequenceDef {
        id: orch8_types::ids::BlockId::new(id),
        sequence_name: target.name.clone(),
        version: None,
        input: json!({}),
    }))
}

async fn engine_on(kind: BackendKind) -> Option<(TestBackend, TestEngine)> {
    let backend = TestBackend::open(kind).await?;
    let engine = TestEngine::builder()
        .storage(backend.storage.clone())
        .build()
        .await;
    Some((backend, engine))
}

async fn only_child(engine: &TestEngine, parent: InstanceId) -> TaskInstance {
    let mut kids = engine.storage.get_child_instances(parent).await.unwrap();
    assert_eq!(kids.len(), 1, "expected exactly one child of {parent}");
    kids.remove(0)
}

/// Tick until `parent` has a child that has parked in `Waiting`.
async fn tick_until_child_parked(engine: &TestEngine, parent: InstanceId) -> TaskInstance {
    for _ in 0..20 {
        engine.tick().await;
        let kids = engine.storage.get_child_instances(parent).await.unwrap();
        if let Some(kid) = kids.first()
            && kid.state == InstanceState::Waiting
        {
            return kid.clone();
        }
    }
    panic!("child of {parent} never parked in Waiting");
}

/// parent -> child -> grandchild, grandchild parked on a human-input gate.
async fn start_chain(engine: &TestEngine) -> (InstanceId, InstanceId, InstanceId) {
    let grandchild_seq = SequenceBuilder::new("t").block(gate("hold")).build();
    let child_seq = SequenceBuilder::new("t")
        .block(sub_sequence("to_gc", &grandchild_seq))
        .build();
    let parent_seq = SequenceBuilder::new("t")
        .block(sub_sequence("to_child", &child_seq))
        .build();
    engine.storage.create_sequence(&grandchild_seq).await.unwrap();
    engine.storage.create_sequence(&child_seq).await.unwrap();
    let parent = engine.start(&parent_seq).await;

    let child = loop_until_child(engine, parent).await;
    let grandchild = tick_until_child_parked(engine, child).await;
    (parent, child, grandchild.id)
}

async fn loop_until_child(engine: &TestEngine, parent: InstanceId) -> InstanceId {
    for _ in 0..20 {
        engine.tick().await;
        if let Some(kid) = engine
            .storage
            .get_child_instances(parent)
            .await
            .unwrap()
            .first()
        {
            return kid.id;
        }
    }
    panic!("parent {parent} never spawned a child");
}

async fn state_of(engine: &TestEngine, id: InstanceId) -> InstanceState {
    engine.instance(id).await.state
}

async fn send_cancel(engine: &TestEngine, id: InstanceId) {
    let sig = orch8_types::signal::Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: id,
        signal_type: orch8_types::signal::SignalType::Cancel,
        payload: json!({}),
        delivered: false,
        created_at: chrono::Utc::now(),
        delivered_at: None,
    };
    engine.storage.enqueue_signal(&sig).await.unwrap();
}

// === L3-07 ===

#[tokio::test]
async fn l3_07_cancelling_a_parent_cancels_the_whole_child_chain() {
    for kind in KINDS {
        let Some((b, engine)) = engine_on(kind).await else {
            continue;
        };
        let (parent, child, grandchild) = start_chain(&engine).await;
        assert_eq!(state_of(&engine, grandchild).await, InstanceState::Waiting);

        send_cancel(&engine, parent).await;
        engine.tick().await;

        assert_eq!(state_of(&engine, parent).await, InstanceState::Cancelled, "{}", b.name());
        assert_eq!(state_of(&engine, child).await, InstanceState::Cancelled, "{}", b.name());
        assert_eq!(
            state_of(&engine, grandchild).await,
            InstanceState::Cancelled,
            "{}: grandchild kept running after its ancestor was cancelled",
            b.name()
        );
    }
}

#[tokio::test]
async fn l3_07_failing_a_parent_cancels_its_children() {
    for kind in KINDS {
        let Some((b, engine)) = engine_on(kind).await else {
            continue;
        };
        let child_seq = SequenceBuilder::new("t").block(gate("hold")).build();
        let parent_seq = SequenceBuilder::new("t")
            .block(sub_sequence("to_child", &child_seq))
            .build();
        engine.storage.create_sequence(&child_seq).await.unwrap();
        let parent = engine.start(&parent_seq).await;
        let child = tick_until_child_parked(&engine, parent).await;

        // The parent dies without ever observing its child (operator /
        // API state change, reaper, ...).
        engine
            .storage
            .update_instance_state(parent, InstanceState::Failed, None)
            .await
            .unwrap();
        engine.tick().await;

        assert_eq!(
            state_of(&engine, child.id).await,
            InstanceState::Cancelled,
            "{}",
            b.name()
        );
        assert_eq!(only_child(&engine, parent).await.id, child.id);
    }
}

#[tokio::test]
async fn l3_07_live_parent_keeps_its_children() {
    for kind in KINDS {
        let Some((b, engine)) = engine_on(kind).await else {
            continue;
        };
        let (parent, child, grandchild) = start_chain(&engine).await;
        for _ in 0..3 {
            engine.tick().await;
        }
        assert_eq!(state_of(&engine, parent).await, InstanceState::Waiting, "{}", b.name());
        assert_eq!(state_of(&engine, child).await, InstanceState::Waiting, "{}", b.name());
        assert_eq!(state_of(&engine, grandchild).await, InstanceState::Waiting, "{}", b.name());
    }
}
