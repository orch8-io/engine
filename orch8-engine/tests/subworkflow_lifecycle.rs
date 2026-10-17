//! Sub-workflow lifecycle regressions from the long-run review (L3-05,
//! L3-07, L3-11), driven through the real scheduler on virtual time
//! ([`TestEngine`]) on `SQLite` and, when `DATABASE_URL` is set, Postgres.

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
    engine
        .storage
        .create_sequence(&grandchild_seq)
        .await
        .unwrap();
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

        assert_eq!(
            state_of(&engine, parent).await,
            InstanceState::Cancelled,
            "{}",
            b.name()
        );
        assert_eq!(
            state_of(&engine, child).await,
            InstanceState::Cancelled,
            "{}",
            b.name()
        );
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
        assert_eq!(
            state_of(&engine, parent).await,
            InstanceState::Waiting,
            "{}",
            b.name()
        );
        assert_eq!(
            state_of(&engine, child).await,
            InstanceState::Waiting,
            "{}",
            b.name()
        );
        assert_eq!(
            state_of(&engine, grandchild).await,
            InstanceState::Waiting,
            "{}",
            b.name()
        );
    }
}

// === L3-05 ===

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use orch8_types::error::StepError;
use orch8_types::sequence::ParallelDef;

/// Handlers `count` (counts calls) and `flaky` (permanent failure on its
/// first call only). Returns (builder, count calls, flaky calls).
fn counting_engine_builder(
    backend: &TestBackend,
) -> (
    orch8_engine::testing::TestEngineBuilder,
    Arc<AtomicU32>,
    Arc<AtomicU32>,
) {
    let counted = Arc::new(AtomicU32::new(0));
    let flaky_calls = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counted);
    let f = Arc::clone(&flaky_calls);
    let builder = TestEngine::builder()
        .storage(backend.storage.clone())
        .register("noop", move |_ctx| {
            let c = Arc::clone(&c);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(json!({"ok": true}))
            }
        })
        .register("log", move |_ctx| {
            let f = Arc::clone(&f);
            async move {
                if f.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(StepError::Permanent {
                        message: "first call fails".into(),
                        details: None,
                    })
                } else {
                    Ok(json!({"ok": true}))
                }
            }
        });
    (builder, counted, flaky_calls)
}

/// Tick until `id` is terminal (advancing virtual time over any wait).
async fn run_to_terminal(engine: &TestEngine, id: InstanceId) -> TaskInstance {
    for _ in 0..80 {
        engine.tick().await;
        // Wakes (parent wake, retry) stamp real `Utc::now()`; keep the
        // virtual clock at or past it so they are due on the next tick.
        let real_now = chrono::Utc::now();
        if real_now > engine.now() {
            engine.clock.set(real_now);
        }
        let inst = engine.instance(id).await;
        if inst.state.is_terminal() {
            return inst;
        }
        if inst.state == InstanceState::Scheduled
            && let Some(at) = inst.next_fire_at
            && at > engine.now()
        {
            engine.clock.set(at);
        }
    }
    let inst = engine.instance(id).await;
    panic!("{id} did not reach a terminal state: {:?}", inst.state);
}

async fn retry(engine: &TestEngine, id: InstanceId) {
    let outcome = orch8_storage::lifecycle::retry_failed_instance(engine.storage.as_ref(), id)
        .await
        .unwrap();
    assert_eq!(outcome, orch8_storage::lifecycle::RetryOutcome::Retried);
}

#[tokio::test]
async fn l3_05_dlq_retry_of_a_composite_reruns_only_the_failed_leaf() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, counted, flaky_calls) = counting_engine_builder(&backend);
        let engine = builder.build().await;
        let seq = SequenceBuilder::new("t")
            .block(BlockDefinition::Parallel(Box::new(ParallelDef {
                id: orch8_types::ids::BlockId::new("par"),
                branches: vec![
                    vec![BlockDefinition::Step(Box::new(step("a", "noop")))],
                    vec![BlockDefinition::Step(Box::new(step("b", "log")))],
                ],
            })))
            .build();
        let id = engine.start(&seq).await;

        let failed = run_to_terminal(&engine, id).await;
        assert_eq!(failed.state, InstanceState::Failed, "{}", backend.name());
        assert_eq!(counted.load(Ordering::SeqCst), 1);

        retry(&engine, id).await;
        let done = run_to_terminal(&engine, id).await;

        assert_eq!(done.state, InstanceState::Completed, "{}", backend.name());
        assert_eq!(flaky_calls.load(Ordering::SeqCst), 2, "failed leaf re-ran");
        assert_eq!(
            counted.load(Ordering::SeqCst),
            1,
            "{}: the nested step that already succeeded re-ran on DLQ retry",
            backend.name()
        );
    }
}

/// parent = `[sub_sequence -> child]`, child = `[noop "c1", flaky "c2"]`.
async fn failed_parent_with_failed_child(engine: &TestEngine) -> InstanceId {
    let child_seq = SequenceBuilder::new("t")
        .step("c1", "noop")
        .step("c2", "log")
        .build();
    let parent_seq = SequenceBuilder::new("t")
        .block(sub_sequence("to_child", &child_seq))
        .build();
    engine.storage.create_sequence(&child_seq).await.unwrap();
    let parent = engine.start(&parent_seq).await;
    let failed = run_to_terminal(engine, parent).await;
    assert_eq!(failed.state, InstanceState::Failed);
    parent
}

#[tokio::test]
async fn l3_05_dlq_retry_resumes_the_failed_child_instead_of_respawning() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, counted, flaky_calls) = counting_engine_builder(&backend);
        let engine = builder.build().await;
        let parent = failed_parent_with_failed_child(&engine).await;
        let first_child = only_child(&engine, parent).await;
        assert_eq!(first_child.state, InstanceState::Failed);
        assert_eq!(counted.load(Ordering::SeqCst), 1);

        retry(&engine, parent).await;
        let done = run_to_terminal(&engine, parent).await;

        assert_eq!(done.state, InstanceState::Completed, "{}", backend.name());
        let kids = engine.storage.get_child_instances(parent).await.unwrap();
        assert_eq!(
            kids.len(),
            1,
            "{}: DLQ retry spawned a second sub-workflow",
            backend.name()
        );
        assert_eq!(kids[0].id, first_child.id);
        assert_eq!(kids[0].state, InstanceState::Completed);
        assert_eq!(
            counted.load(Ordering::SeqCst),
            1,
            "child's finished step re-ran"
        );
        assert_eq!(flaky_calls.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn l3_05_a_superseded_child_is_not_adopted() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, _counted, _flaky) = counting_engine_builder(&backend);
        let engine = builder.build().await;
        let parent = failed_parent_with_failed_child(&engine).await;
        let old = only_child(&engine, parent).await;
        // What resume-from-block does to the children of the blocks it wipes.
        engine
            .storage
            .merge_instance_metadata(
                old.id,
                &json!({ orch8_engine::evaluator::SUPERSEDED_CHILD_KEY: true }),
            )
            .await
            .unwrap();

        retry(&engine, parent).await;
        let done = run_to_terminal(&engine, parent).await;

        assert_eq!(done.state, InstanceState::Completed, "{}", backend.name());
        let kids = engine.storage.get_child_instances(parent).await.unwrap();
        assert_eq!(kids.len(), 2, "{}", backend.name());
        assert_eq!(engine.instance(old.id).await.state, InstanceState::Failed);
    }
}

// === L3-11 ===

use std::sync::Mutex;

use orch8_types::sequence::{SagaDef, SagaStep};

/// Engine whose `undo` handler records the block id of each compensation it
/// runs, in call order.
fn saga_engine_builder(
    backend: &TestBackend,
) -> (
    orch8_engine::testing::TestEngineBuilder,
    Arc<Mutex<Vec<String>>>,
) {
    let undone = Arc::new(Mutex::new(Vec::<String>::new()));
    let u = Arc::clone(&undone);
    let builder = TestEngine::builder()
        .storage(backend.storage.clone())
        .register("undo", move |ctx| {
            let u = Arc::clone(&u);
            async move {
                u.lock().unwrap().push(ctx.block_id.as_str().to_owned());
                Ok(json!({"undone": ctx.block_id.as_str()}))
            }
        });
    (builder, undone)
}

fn saga_step(id: &str, action: BlockDefinition, comp: Option<&str>) -> SagaStep {
    SagaStep {
        id: orch8_types::ids::BlockId::new(id),
        action: Box::new(action),
        compensation: comp.map(|c| Box::new(BlockDefinition::Step(Box::new(step(c, "undo"))))),
    }
}

fn noop_block(id: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(step(id, "noop")))
}

/// A noop step that parks the instance for 1 s first, so each saga action
/// takes its own scheduler tick.
fn delayed_block(id: &str) -> BlockDefinition {
    let mut s = step(id, "noop");
    s.delay = Some(orch8_types::sequence::DelaySpec {
        duration: std::time::Duration::from_secs(1),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    });
    BlockDefinition::Step(Box::new(s))
}

fn saga_block(steps: Vec<SagaStep>) -> BlockDefinition {
    BlockDefinition::Saga(Box::new(SagaDef {
        id: orch8_types::ids::BlockId::new("saga"),
        steps,
    }))
}

fn undone(log: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    log.lock().unwrap().clone()
}

#[tokio::test]
async fn l3_11_cancelling_mid_saga_compensates_completed_steps_in_reverse() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, log) = saga_engine_builder(&backend);
        let engine = builder.build().await;
        // s0, s1 complete; s2 parks on a human-input gate.
        let seq = SequenceBuilder::new("t")
            .block(saga_block(vec![
                saga_step("s0", noop_block("a0"), Some("c0")),
                saga_step("s1", noop_block("a1"), Some("c1")),
                saga_step("s2", gate("a2"), Some("c2")),
            ]))
            .build();
        let id = engine.start(&seq).await;
        let parked = engine.drive(id).await;
        assert_eq!(parked.state, InstanceState::Waiting, "{}", backend.name());
        assert!(undone(&log).is_empty());

        send_cancel(&engine, id).await;
        engine.tick().await;

        assert_eq!(engine.instance(id).await.state, InstanceState::Cancelled);
        assert_eq!(
            undone(&log),
            vec!["c1".to_owned(), "c0".to_owned()],
            "{}: completed saga steps must be compensated LIFO; the step that never completed must not be",
            backend.name()
        );
        // Recorded like any compensation, and not repeated by later ticks.
        assert!(
            engine
                .storage
                .get_block_output(id, &orch8_types::ids::BlockId::new("c0"))
                .await
                .unwrap()
                .is_some()
        );
        engine.tick().await;
        assert_eq!(undone(&log).len(), 2, "compensation ran twice");
    }
}

#[tokio::test]
async fn l3_11_step_budget_kill_compensates_the_saga() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, log) = saga_engine_builder(&backend);
        let engine = builder
            .config(|c| c.max_steps_per_instance = 2)
            .build()
            .await;
        let seq = SequenceBuilder::new("t")
            .block(saga_block(
                (0..6)
                    .map(|i| {
                        saga_step(
                            &format!("s{i}"),
                            delayed_block(&format!("a{i}")),
                            Some(&format!("c{i}")),
                        )
                    })
                    .collect(),
            ))
            .build();
        let id = engine.start(&seq).await;

        let done = run_to_terminal(&engine, id).await;

        assert_eq!(done.state, InstanceState::Failed, "{}", backend.name());
        let ran = undone(&log);
        assert!(
            !ran.is_empty() && ran.len() < 6,
            "{}: expected the completed prefix to be compensated, got {ran:?}",
            backend.name()
        );
        // LIFO over a prefix: c(k-1), ..., c0.
        let expected: Vec<String> = (0..ran.len()).rev().map(|i| format!("c{i}")).collect();
        assert_eq!(ran, expected, "{}", backend.name());
    }
}

#[tokio::test]
async fn l3_11_evaluation_error_compensates_the_saga() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, log) = saga_engine_builder(&backend);
        let engine = builder.build().await;
        // s1's action invokes a sequence that does not exist: dispatch
        // returns a (non-retryable) evaluation error instead of failing the
        // saga node, so the saga never rolls itself back.
        let missing = SequenceBuilder::new("t").name("does-not-exist").build();
        let seq = SequenceBuilder::new("t")
            .block(saga_block(vec![
                saga_step("s0", noop_block("a0"), Some("c0")),
                saga_step("s1", sub_sequence("a1", &missing), Some("c1")),
            ]))
            .build();
        let id = engine.start(&seq).await;

        let done = run_to_terminal(&engine, id).await;

        assert_eq!(done.state, InstanceState::Failed, "{}", backend.name());
        assert_eq!(undone(&log), vec!["c0".to_owned()], "{}", backend.name());
    }
}

#[tokio::test]
async fn l3_11_a_saga_that_rolled_itself_back_is_not_compensated_twice() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, log) = saga_engine_builder(&backend);
        let engine = builder
            .register("boom", |_ctx| async {
                Err(StepError::Permanent {
                    message: "boom".into(),
                    details: None,
                })
            })
            .build()
            .await;
        let seq = SequenceBuilder::new("t")
            .block(saga_block(vec![
                saga_step("s0", noop_block("a0"), Some("c0")),
                saga_step(
                    "s1",
                    BlockDefinition::Step(Box::new(step("a1", "boom"))),
                    Some("c1"),
                ),
            ]))
            .build();
        let id = engine.start(&seq).await;

        let done = run_to_terminal(&engine, id).await;

        assert_eq!(done.state, InstanceState::Failed, "{}", backend.name());
        assert_eq!(
            undone(&log),
            vec!["c0".to_owned()],
            "{}: the saga's own rollback already compensated s0",
            backend.name()
        );
    }
}

#[tokio::test]
async fn l3_07_cancelling_a_parent_compensates_the_sagas_of_its_children() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, log) = saga_engine_builder(&backend);
        let engine = builder.build().await;
        let child_seq = SequenceBuilder::new("t")
            .block(saga_block(vec![
                saga_step("s0", noop_block("a0"), Some("c0")),
                saga_step("s1", gate("a1"), Some("c1")),
            ]))
            .build();
        let parent_seq = SequenceBuilder::new("t")
            .block(sub_sequence("to_child", &child_seq))
            .build();
        engine.storage.create_sequence(&child_seq).await.unwrap();
        let parent = engine.start(&parent_seq).await;
        let child = tick_until_child_parked(&engine, parent).await;

        send_cancel(&engine, parent).await;
        engine.tick().await;

        assert_eq!(
            state_of(&engine, child.id).await,
            InstanceState::Cancelled,
            "{}",
            backend.name()
        );
        assert_eq!(undone(&log), vec!["c0".to_owned()], "{}", backend.name());
    }
}
