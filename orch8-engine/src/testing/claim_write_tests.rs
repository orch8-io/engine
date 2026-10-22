//! Late handlers must not change a replacement execution's durable results.
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use orch8_storage::testing::fixtures::{SequenceBuilder, step};
use orch8_types::error::StepError;
use orch8_types::ids::BlockId;
use orch8_types::instance::InstanceState;
use orch8_types::sequence::{BlockDefinition, ParallelDef};
use serde_json::json;
use tokio::sync::Notify;

use super::TestEngine;

#[derive(Clone, Copy)]
enum LateResult {
    Success,
    Retryable,
    Permanent,
}

#[derive(Clone, Copy)]
enum DispatchPath {
    Flat,
    Tree,
}
#[derive(Clone, Copy)]
enum OutputMode {
    Inline,
    Cached,
    Externalized,
    CachedExternalized,
}

async fn barrier_engine(
    externalize: bool,
    late: LateResult,
    handler_entered: Arc<Notify>,
    handler_release: Arc<Notify>,
    handler_calls: Arc<AtomicUsize>,
) -> Arc<TestEngine> {
    Arc::new(
        TestEngine::builder()
            .config(|config| {
                config.max_concurrent_steps = 2;
                config.externalize_output_threshold = u32::from(externalize);
            })
            // A pure builtin avoids effect-receipt ambiguity: the test concerns
            // engine-owned writes after genuine recovery and a new handler run.
            .register("noop", move |_| {
                let entered = Arc::clone(&handler_entered);
                let release = Arc::clone(&handler_release);
                let calls = Arc::clone(&handler_calls);
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        entered.notify_one();
                        release.notified().await;
                        match late {
                            LateResult::Success => Ok(json!({"owner":"stale"})),
                            LateResult::Retryable => Err(StepError::Retryable {
                                message: "stale failure".into(),
                                details: None,
                            }),
                            LateResult::Permanent => Err(StepError::Permanent {
                                message: "stale failure".into(),
                                details: None,
                            }),
                        }
                    } else {
                        Ok(json!({"owner":"replacement"}))
                    }
                }
            })
            .build()
            .await,
    )
}

async fn preserves_replacement(path: DispatchPath, mode: OutputMode, late: LateResult) {
    let tree = matches!(path, DispatchPath::Tree);
    let cache = matches!(mode, OutputMode::Cached | OutputMode::CachedExternalized);
    let externalize = matches!(
        mode,
        OutputMode::Externalized | OutputMode::CachedExternalized
    );
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let engine = barrier_engine(
        externalize,
        late,
        Arc::clone(&entered),
        Arc::clone(&release),
        Arc::clone(&calls),
    )
    .await;
    let mut work = step("work", "noop");
    work.cache_key = cache.then(|| "result".to_owned());
    let block = BlockDefinition::Step(Box::new(work));
    let blocks = if tree {
        vec![BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("parallel"),
            branches: vec![vec![block]],
        }))]
    } else {
        vec![block]
    };
    let sequence = SequenceBuilder::new("claim-output").blocks(blocks).build();
    let id = engine.start(&sequence).await;
    let old_engine = Arc::clone(&engine);
    let old_tick = tokio::spawn(async move { old_engine.try_tick().await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert_eq!(
        engine
            .storage
            .recover_stale_instances(Duration::ZERO)
            .await
            .unwrap(),
        1
    );
    engine.advance(Duration::from_secs(1));
    engine.tick().await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed
    );
    let replacement = engine
        .storage
        .get_block_output(id, &BlockId::new("work"))
        .await
        .unwrap()
        .unwrap();
    let before: HashSet<_> = engine
        .storage
        .get_all_outputs(id)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect();
    let nodes_before: HashMap<_, _> = engine
        .storage
        .get_execution_tree(id)
        .await
        .unwrap()
        .into_iter()
        .map(|node| (node.id, serde_json::to_value(node).unwrap()))
        .collect();
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), old_tick)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_preserved(&engine, id, replacement.id, &before, nodes_before, cache).await;
}

async fn assert_preserved(
    engine: &TestEngine,
    id: orch8_types::ids::InstanceId,
    replacement_id: uuid::Uuid,
    before: &HashSet<uuid::Uuid>,
    nodes_before: HashMap<orch8_types::ids::ExecutionNodeId, serde_json::Value>,
    cache: bool,
) {
    let current = engine
        .storage
        .get_block_output(id, &BlockId::new("work"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        current.id, replacement_id,
        "late success/error/retry must not append a new result"
    );
    let value = if let Some(key) = current.output_ref {
        engine
            .storage
            .get_externalized_state(id, &key)
            .await
            .unwrap()
            .unwrap()
    } else {
        current.output
    };
    assert_eq!(value, json!({"owner":"replacement"}));
    if cache {
        assert_eq!(
            engine
                .storage
                .get_instance_kv(id, "_cache:result")
                .await
                .unwrap(),
            Some(json!({"owner":"replacement"}))
        );
    }
    let after = engine.storage.get_all_outputs(id).await.unwrap();
    assert!(
        after.iter().all(|row| before.contains(&row.id)),
        "stale writes must not leave hidden sentinel, error or retry rows"
    );
    assert_eq!(
        engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed
    );
    let nodes_after: HashMap<_, _> = engine
        .storage
        .get_execution_tree(id)
        .await
        .unwrap()
        .into_iter()
        .map(|node| (node.id, serde_json::to_value(node).unwrap()))
        .collect();
    assert_eq!(
        nodes_after, nodes_before,
        "late completion/failure must not change replacement node timestamps or state"
    );
}

#[tokio::test]
async fn flat_inline_result_keeps_replacement_owner() {
    preserves_replacement(DispatchPath::Flat, OutputMode::Inline, LateResult::Success).await;
}
#[tokio::test]
async fn tree_inline_result_keeps_replacement_owner() {
    preserves_replacement(DispatchPath::Tree, OutputMode::Inline, LateResult::Success).await;
}
#[tokio::test]
async fn flat_cached_result_keeps_replacement_owner() {
    preserves_replacement(DispatchPath::Flat, OutputMode::Cached, LateResult::Success).await;
}
#[tokio::test]
async fn tree_cached_result_keeps_replacement_owner() {
    preserves_replacement(DispatchPath::Tree, OutputMode::Cached, LateResult::Success).await;
}
#[tokio::test]
async fn externalized_result_keeps_replacement_payload() {
    preserves_replacement(
        DispatchPath::Flat,
        OutputMode::Externalized,
        LateResult::Success,
    )
    .await;
}
#[tokio::test]
async fn flat_late_retry_does_not_poison_replacement() {
    preserves_replacement(
        DispatchPath::Flat,
        OutputMode::Inline,
        LateResult::Retryable,
    )
    .await;
}
#[tokio::test]
async fn tree_late_retry_does_not_poison_replacement() {
    preserves_replacement(
        DispatchPath::Tree,
        OutputMode::Inline,
        LateResult::Retryable,
    )
    .await;
}
#[tokio::test]
async fn flat_late_failure_does_not_poison_replacement() {
    preserves_replacement(
        DispatchPath::Flat,
        OutputMode::Inline,
        LateResult::Permanent,
    )
    .await;
}
#[tokio::test]
async fn tree_late_failure_does_not_poison_replacement() {
    preserves_replacement(
        DispatchPath::Tree,
        OutputMode::Inline,
        LateResult::Permanent,
    )
    .await;
}

#[tokio::test]
async fn cached_externalized_result_keeps_both_replacement_values() {
    preserves_replacement(
        DispatchPath::Flat,
        OutputMode::CachedExternalized,
        LateResult::Success,
    )
    .await;
}

#[tokio::test]
async fn transient_externalized_write_failure_reschedules_instead_of_failing() {
    use orch8_storage::testing::faulty::FaultStorage;
    let inner: Arc<dyn orch8_storage::StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    let (storage, faults) = FaultStorage::wrap(inner);
    faults
        .on("save_externalized_state_claimed")
        .times(1)
        .fail_transient();
    let engine = TestEngine::builder()
        .storage(storage)
        .config(|config| config.externalize_output_threshold = 1)
        .build()
        .await;
    let sequence = SequenceBuilder::new("claim-fault")
        .step("work", "noop")
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    assert_eq!(faults.injected("save_externalized_state_claimed"), 1);
    assert_eq!(
        engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Scheduled
    );
    assert!(
        engine
            .storage
            .get_all_outputs(id)
            .await
            .unwrap()
            .iter()
            .all(|row| row.output_ref.as_deref() != Some("__error__"))
    );
    engine.advance(Duration::from_secs(61));
    engine.tick().await;
    assert_eq!(
        engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed
    );
}

async fn preserves_builtin_value(handler_name: &'static str) {
    use crate::handlers::{HandlerRegistry, builtin::register_builtins};

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut builtins = HandlerRegistry::new();
    register_builtins(&mut builtins);
    let builtins = Arc::new(builtins);
    let engine = Arc::new(
        TestEngine::builder()
            .config(|config| config.max_concurrent_steps = 2)
            .register(handler_name, {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                let calls = Arc::clone(&calls);
                move |mut ctx| {
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    let calls = Arc::clone(&calls);
                    let builtins = Arc::clone(&builtins);
                    async move {
                        let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                        if first {
                            entered.notify_one();
                            release.notified().await;
                        } else if handler_name == "merge_state" {
                            ctx.params["values"] = json!({"owner": "replacement"});
                        } else {
                            ctx.params["value"] = json!("replacement");
                        }
                        let saved_ctx = ctx.clone();
                        let result = builtins.get(handler_name).unwrap()(ctx).await?;
                        if !first && handler_name == "delete_state" {
                            saved_ctx.set_state("owner", &json!("replacement")).await?;
                        }
                        Ok(result)
                    }
                }
            })
            .build()
            .await,
    );
    let mut work = step("work", handler_name);
    work.params = if handler_name == "merge_state" {
        json!({"values": {"owner": "stale"}})
    } else {
        json!({"key": "owner", "value": "stale"})
    };
    let sequence = SequenceBuilder::new("builtin-claim")
        .blocks(vec![BlockDefinition::Step(Box::new(work))])
        .build();
    let id = engine.start(&sequence).await;
    let old_engine = Arc::clone(&engine);
    let old_tick = tokio::spawn(async move { old_engine.try_tick().await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert_eq!(
        engine
            .storage
            .recover_stale_instances(Duration::ZERO)
            .await
            .unwrap(),
        1
    );
    engine.advance(Duration::from_secs(1));
    engine.tick().await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        engine.storage.get_instance_kv(id, "owner").await.unwrap(),
        Some(json!("replacement"))
    );
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), old_tick)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed
    );
    assert_eq!(
        engine.storage.get_instance_kv(id, "owner").await.unwrap(),
        Some(json!("replacement")),
        "a late builtin must not overwrite a replacement execution's state"
    );
}

#[tokio::test]
async fn late_builtin_set_state_preserves_replacement_value() {
    preserves_builtin_value("set_state").await;
}

#[tokio::test]
async fn late_builtin_merge_state_preserves_replacement_value() {
    preserves_builtin_value("merge_state").await;
}

#[tokio::test]
async fn late_builtin_delete_state_preserves_replacement_value() {
    preserves_builtin_value("delete_state").await;
}
