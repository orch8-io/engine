//! Operator (API / gRPC / jobs) cancels go through
//! `orch8_storage::lifecycle::request_cancel`: a durable `Cancel` signal the
//! scheduler applies. Unlike a direct state flip, that runs the instance's
//! saga compensations (LIFO) and its `on_cancel` hook exactly once, for every
//! state an instance can be cancelled from, survives a restart, and wakes a
//! waiting sub-sequence parent. Driven through the real scheduler on virtual
//! time on `SQLite` and, when `DATABASE_URL` is set, Postgres.

use std::sync::{Arc, Mutex};

use serde_json::json;

use orch8_engine::testing::{TestEngine, TestEngineBuilder};
use orch8_storage::lifecycle::{CancelRequest, request_cancel, request_cancel_matching};
use orch8_storage::testing::backends::{BackendKind, TestBackend};
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder, step};
use orch8_types::filter::InstanceFilter;
use orch8_types::ids::{BlockId, InstanceId, TenantId};
use orch8_types::instance::InstanceState;
use orch8_types::sequence::{
    BlockDefinition, DelaySpec, SagaDef, SagaStep, SequenceDefinition, SubSequenceDef,
};

const KINDS: [BackendKind; 2] = [BackendKind::Sqlite, BackendKind::Postgres];

type Log = Arc<Mutex<Vec<String>>>;

/// Engine whose `undo` and `hook` handlers record the block id they run for.
fn engine_builder(backend: &TestBackend) -> (TestEngineBuilder, Log, Log) {
    let undone = Arc::new(Mutex::new(Vec::<String>::new()));
    let hooked = Arc::new(Mutex::new(Vec::<String>::new()));
    let (u, h) = (Arc::clone(&undone), Arc::clone(&hooked));
    let builder = TestEngine::builder()
        .storage(backend.storage.clone())
        .register("undo", move |ctx| {
            let u = Arc::clone(&u);
            async move {
                u.lock().unwrap().push(ctx.block_id.as_str().to_owned());
                Ok(json!({}))
            }
        })
        .register("hook", move |ctx| {
            let h = Arc::clone(&h);
            async move {
                h.lock().unwrap().push(ctx.block_id.as_str().to_owned());
                Ok(json!({}))
            }
        });
    (builder, undone, hooked)
}

fn noop_block(id: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(step(id, "noop")))
}

fn gate(id: &str) -> BlockDefinition {
    let mut s = step(id, "noop");
    s.wait_for_input = Some(serde_json::from_value(json!({"prompt": "go?"})).unwrap());
    BlockDefinition::Step(Box::new(s))
}

/// A noop step that parks the instance in `Scheduled` for `secs` first.
fn delayed_block(id: &str, secs: u64) -> BlockDefinition {
    let mut s = step(id, "noop");
    s.delay = Some(DelaySpec {
        duration: std::time::Duration::from_secs(secs),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    });
    BlockDefinition::Step(Box::new(s))
}

fn saga(steps: Vec<(&str, BlockDefinition)>) -> BlockDefinition {
    BlockDefinition::Saga(Box::new(SagaDef {
        id: BlockId::new("saga"),
        steps: steps
            .into_iter()
            .map(|(i, action)| SagaStep {
                id: BlockId::new(i),
                action: Box::new(action),
                compensation: Some(Box::new(BlockDefinition::Step(Box::new(step(
                    &format!("c{}", &i[1..]),
                    "undo",
                ))))),
            })
            .collect(),
    }))
}

fn with_on_cancel(mut seq: SequenceDefinition) -> SequenceDefinition {
    seq.on_cancel = Some(vec![BlockDefinition::Step(Box::new(step("bye", "hook")))]);
    seq
}

fn log(l: &Log) -> Vec<String> {
    l.lock().unwrap().clone()
}

/// Tick until the instance is terminal (the cancel needs a sweep, a wake-up,
/// and a claim at most), then a few more ticks to expose repeated effects.
async fn settle(engine: &TestEngine, id: InstanceId) {
    for _ in 0..8 {
        engine.tick().await;
        if engine.instance(id).await.state.is_terminal() {
            break;
        }
    }
    for _ in 0..3 {
        engine.tick().await;
    }
}

fn assert_cancelled_once(
    name: &str,
    state: InstanceState,
    undone: &Log,
    hooked: &Log,
    expect_undone: &[&str],
) {
    assert_eq!(state, InstanceState::Cancelled, "{name}");
    assert_eq!(
        log(undone),
        expect_undone.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>(),
        "{name}: completed saga steps must be compensated LIFO, exactly once"
    );
    assert_eq!(
        log(hooked),
        vec!["bye".to_owned()],
        "{name}: on_cancel must fire exactly once"
    );
}

#[tokio::test]
async fn api_cancel_of_a_waiting_instance_compensates_and_fires_on_cancel() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, undone, hooked) = engine_builder(&backend);
        let engine = builder.build().await;
        let seq = with_on_cancel(
            SequenceBuilder::new("t")
                .block(saga(vec![
                    ("s0", noop_block("a0")),
                    ("s1", noop_block("a1")),
                    ("s2", gate("a2")),
                ]))
                .build(),
        );
        let id = engine.start(&seq).await;
        assert_eq!(engine.drive(id).await.state, InstanceState::Waiting);

        let storage = engine.storage.as_ref();
        assert_eq!(
            request_cancel(storage, id).await.unwrap(),
            CancelRequest::Enqueued
        );
        // Retried request while the first is pending: still one signal.
        assert_eq!(
            request_cancel(storage, id).await.unwrap(),
            CancelRequest::AlreadyPending
        );
        assert_eq!(
            engine.instance(id).await.state,
            InstanceState::Waiting,
            "the API only requests; the scheduler transitions"
        );

        settle(&engine, id).await;
        assert_cancelled_once(
            backend.name(),
            engine.instance(id).await.state,
            &undone,
            &hooked,
            &["c1", "c0"],
        );
        // Cancelling again is a no-op and cannot re-run anything.
        assert_eq!(
            request_cancel(storage, id).await.unwrap(),
            CancelRequest::Terminal
        );
        settle(&engine, id).await;
        assert_eq!(log(&undone).len(), 2, "{}", backend.name());
        assert_eq!(log(&hooked).len(), 1, "{}", backend.name());
    }
}

#[tokio::test]
async fn api_cancel_of_a_far_future_scheduled_instance_does_not_wait_for_its_fire_time() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, undone, hooked) = engine_builder(&backend);
        let engine = builder.build().await;
        // s0 completes, then the instance sleeps for an hour before s1.
        let seq = with_on_cancel(
            SequenceBuilder::new("t")
                .block(saga(vec![
                    ("s0", noop_block("a0")),
                    ("s1", delayed_block("a1", 3600)),
                ]))
                .build(),
        );
        let id = engine.start(&seq).await;
        // Tick (without jumping the clock) until the delayed step parks it.
        for _ in 0..4 {
            engine.tick().await;
        }
        let parked = engine.instance(id).await;
        assert_eq!(parked.state, InstanceState::Scheduled, "{}", backend.name());
        assert!(parked.next_fire_at.unwrap() > engine.now() + chrono::Duration::minutes(30));

        request_cancel(engine.storage.as_ref(), id).await.unwrap();
        // No clock advance: the cancel must not wait out the hour.
        settle(&engine, id).await;
        assert_cancelled_once(
            backend.name(),
            engine.instance(id).await.state,
            &undone,
            &hooked,
            &["c0"],
        );
    }
}

#[tokio::test]
async fn api_cancel_of_a_paused_instance_compensates_and_fires_on_cancel() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, undone, hooked) = engine_builder(&backend);
        let engine = builder.build().await;
        let seq = with_on_cancel(
            SequenceBuilder::new("t")
                .block(saga(vec![
                    ("s0", noop_block("a0")),
                    ("s1", gate("a1")),
                ]))
                .build(),
        );
        let id = engine.start(&seq).await;
        assert_eq!(engine.drive(id).await.state, InstanceState::Waiting);
        engine
            .storage
            .conditional_update_instance_state(
                id,
                InstanceState::Waiting,
                InstanceState::Paused,
                None,
            )
            .await
            .unwrap();

        request_cancel(engine.storage.as_ref(), id).await.unwrap();
        settle(&engine, id).await;
        assert_cancelled_once(
            backend.name(),
            engine.instance(id).await.state,
            &undone,
            &hooked,
            &["c0"],
        );
    }
}

#[tokio::test]
async fn api_cancel_survives_a_scheduler_restart() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, undone, hooked) = engine_builder(&backend);
        let engine = builder.build().await;
        let seq = with_on_cancel(
            SequenceBuilder::new("t")
                .block(saga(vec![("s0", noop_block("a0")), ("s1", gate("a1"))]))
                .build(),
        );
        let id = engine.start(&seq).await;
        assert_eq!(engine.drive(id).await.state, InstanceState::Waiting);
        request_cancel(engine.storage.as_ref(), id).await.unwrap();
        drop(engine); // the process dies before any tick saw the request

        // A fresh engine on the same storage (same handlers/log) finishes it.
        let (builder, _, _) = engine_builder(&backend);
        let restarted = builder
            .register("undo", {
                let u = Arc::clone(&undone);
                move |ctx| {
                    let u = Arc::clone(&u);
                    async move {
                        u.lock().unwrap().push(ctx.block_id.as_str().to_owned());
                        Ok(json!({}))
                    }
                }
            })
            .register("hook", {
                let h = Arc::clone(&hooked);
                move |ctx| {
                    let h = Arc::clone(&h);
                    async move {
                        h.lock().unwrap().push(ctx.block_id.as_str().to_owned());
                        Ok(json!({}))
                    }
                }
            })
            .build()
            .await;
        settle(&restarted, id).await;
        assert_cancelled_once(
            backend.name(),
            restarted.instance(id).await.state,
            &undone,
            &hooked,
            &["c0"],
        );
    }
}

#[tokio::test]
async fn api_bulk_cancel_runs_every_matching_instances_hooks_once() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, undone, hooked) = engine_builder(&backend);
        let engine = builder.build().await;
        let seq = with_on_cancel(
            SequenceBuilder::new("bulk-t")
                .block(saga(vec![("s0", noop_block("a0")), ("s1", gate("a1"))]))
                .build(),
        );
        engine.storage.create_sequence(&seq).await.unwrap();
        let mut ids = Vec::new();
        for _ in 0..3 {
            let inst = InstanceBuilder::for_sequence(&seq).due_at(engine.now()).build();
            ids.push(engine.create(&inst).await);
        }
        for id in &ids {
            assert_eq!(engine.drive(*id).await.state, InstanceState::Waiting);
        }

        let filter = InstanceFilter {
            tenant_id: Some(TenantId::unchecked("bulk-t")),
            states: Some(vec![InstanceState::Waiting]),
            ..InstanceFilter::default()
        };
        let n = request_cancel_matching(engine.storage.as_ref(), &filter)
            .await
            .unwrap();
        assert_eq!(n, 3, "{}", backend.name());
        // Repeating the bulk request while pending is idempotent.
        assert_eq!(
            request_cancel_matching(engine.storage.as_ref(), &filter)
                .await
                .unwrap(),
            3
        );
        for _ in 0..6 {
            engine.tick().await;
        }
        for id in &ids {
            assert_eq!(
                engine.instance(*id).await.state,
                InstanceState::Cancelled,
                "{}",
                backend.name()
            );
        }
        assert_eq!(log(&undone).len(), 3, "{}: one compensation per instance", backend.name());
        assert_eq!(log(&hooked).len(), 3, "{}: one hook per instance", backend.name());
    }
}

#[tokio::test]
async fn api_cancel_of_a_child_wakes_its_waiting_parent() {
    for kind in KINDS {
        let Some(backend) = TestBackend::open(kind).await else {
            continue;
        };
        let (builder, _, _) = engine_builder(&backend);
        let engine = builder.build().await;
        let child_seq = SequenceBuilder::new("t").block(gate("g")).build();
        let parent_seq = SequenceBuilder::new("t")
            .block(BlockDefinition::SubSequence(Box::new(SubSequenceDef {
                id: BlockId::new("to_child"),
                sequence_name: child_seq.name.clone(),
                version: None,
                input: json!({}),
            })))
            .build();
        engine.storage.create_sequence(&child_seq).await.unwrap();
        let parent = engine.start(&parent_seq).await;
        let mut child = None;
        for _ in 0..20 {
            engine.tick().await;
            let kids = engine.storage.get_child_instances(parent).await.unwrap();
            if let Some(k) = kids.first()
                && k.state == InstanceState::Waiting
            {
                child = Some(k.id);
                break;
            }
        }
        let child = child.expect("child never parked");
        assert_eq!(engine.instance(parent).await.state, InstanceState::Waiting);

        request_cancel(engine.storage.as_ref(), child).await.unwrap();
        settle(&engine, child).await;

        assert_eq!(engine.instance(child).await.state, InstanceState::Cancelled);
        assert_ne!(
            engine.instance(parent).await.state,
            InstanceState::Waiting,
            "{}: parent stayed parked on a cancelled child",
            backend.name()
        );
    }
}
