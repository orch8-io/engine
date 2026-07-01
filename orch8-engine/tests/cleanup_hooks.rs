//! Integration tests for sequence-level best-effort cleanup hooks
//! (`on_failure` / `on_cancel`). These run as the instance reaches a terminal
//! state, dispatching their step blocks once, errors swallowed, so a dying run
//! can release resources or notify instead of "just disappearing".

use std::sync::Arc;

use serde_json::json;
use tokio_util::sync::CancellationToken;

use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::scheduler::tick_once;
use orch8_storage::StorageBackend;
use orch8_types::error::StepError;
use orch8_types::ids::BlockId;
use orch8_types::instance::InstanceState;
use orch8_types::signal::{Signal, SignalType};

mod common;
use common::*;

/// Registry: a permanently-failing `boom` handler plus a `cleanup` handler that
/// records that it ran (returns a marker output).
fn registry_with_cleanup() -> HandlerRegistry {
    let mut reg = registry();
    reg.register("boom", |_ctx| {
        Box::pin(async {
            Err(StepError::Permanent {
                message: "boom".into(),
                details: None,
            })
        })
    });
    reg.register("cleanup", |_ctx| {
        Box::pin(async { Ok(json!({ "cleaned": true })) })
    });
    reg
}

async fn run_until_terminal(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    inst_id: orch8_types::ids::InstanceId,
) -> InstanceState {
    let sem = semaphore(128);
    let config = default_config();
    let seq_cache = cache();
    let cancel = CancellationToken::new();
    for _ in 0..50 {
        tick_once(storage, handlers, &sem, &config, &seq_cache, &cancel)
            .await
            .unwrap();
        let inst = storage.get_instance(inst_id).await.unwrap().unwrap();
        if matches!(
            inst.state,
            InstanceState::Completed | InstanceState::Failed | InstanceState::Cancelled
        ) {
            return inst.state;
        }
    }
    panic!("instance did not reach a terminal state");
}

#[tokio::test]
async fn on_failure_cleanup_runs_when_instance_fails() {
    let storage = storage().await;
    let handlers = Arc::new(registry_with_cleanup());

    let mut seq = mk_sequence(vec![mk_step("work", "boom")]);
    seq.on_failure = Some(vec![mk_step("cleanup", "cleanup")]);
    storage.create_sequence(&seq).await.unwrap();

    let inst = mk_instance_scheduled(seq.id, json!({}));
    storage.create_instance(&inst).await.unwrap();

    let final_state = run_until_terminal(&storage, &handlers, inst.id).await;
    assert_eq!(final_state, InstanceState::Failed);

    // The cleanup step's output proves the hook ran.
    let out = storage
        .get_block_output(inst.id, &BlockId::new("cleanup"))
        .await
        .unwrap();
    assert!(out.is_some(), "on_failure cleanup step should have run");
    assert_eq!(out.unwrap().output["cleaned"], true);
}

#[tokio::test]
async fn no_cleanup_when_instance_succeeds() {
    let storage = storage().await;
    let handlers = Arc::new(registry_with_cleanup());

    // `noop` succeeds, so the instance completes and on_failure must NOT run.
    let mut seq = mk_sequence(vec![mk_step("work", "noop")]);
    seq.on_failure = Some(vec![mk_step("cleanup", "cleanup")]);
    storage.create_sequence(&seq).await.unwrap();

    let inst = mk_instance_scheduled(seq.id, json!({}));
    storage.create_instance(&inst).await.unwrap();

    let final_state = run_until_terminal(&storage, &handlers, inst.id).await;
    assert_eq!(final_state, InstanceState::Completed);

    let out = storage
        .get_block_output(inst.id, &BlockId::new("cleanup"))
        .await
        .unwrap();
    assert!(out.is_none(), "cleanup must not run on success");
}

#[tokio::test]
async fn on_cancel_cleanup_runs_on_signal_cancel() {
    let storage = storage().await;
    let handlers = Arc::new(registry_with_cleanup());

    let mut seq = mk_sequence(vec![mk_step("work", "noop")]);
    seq.on_cancel = Some(vec![mk_step("cleanup", "cleanup")]);
    storage.create_sequence(&seq).await.unwrap();

    // A Paused instance with a pending cancel signal — processed by the
    // scheduler's signalled-instance sweep (immediate, unscoped cancel).
    let inst = mk_instance_in_state(seq.id, InstanceState::Paused);
    storage.create_instance(&inst).await.unwrap();

    let sig = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::Cancel,
        payload: json!({}),
        delivered: false,
        created_at: chrono::Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&sig).await.unwrap();

    let sem = semaphore(128);
    let config = default_config();
    let seq_cache = cache();
    let cancel = CancellationToken::new();
    tick_once(&storage, &handlers, &sem, &config, &seq_cache, &cancel)
        .await
        .unwrap();

    let after = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(after.state, InstanceState::Cancelled);

    let out = storage
        .get_block_output(inst.id, &BlockId::new("cleanup"))
        .await
        .unwrap();
    assert!(out.is_some(), "on_cancel cleanup step should have run");
}

/// Regression test: `run_cleanup_hooks` used to resolve handlers via
/// `HandlerRegistry::get()` only, which never contains `wasm://`/`grpc://`/
/// `ap://` plugin handlers (those are dispatched via a separate path in
/// `handlers/step_block.rs`). A cleanup step using a real, registered plugin
/// handler would silently no-op instead of running. This drives an actual
/// WASM plugin through `on_failure` and asserts its output was persisted,
/// proving the cleanup hook didn't just log-and-skip it.
#[tokio::test]
async fn on_failure_cleanup_runs_for_registered_wasm_plugin_handler() {
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Minimal WASM module: always returns a fixed JSON output, proving the
    // module was actually invoked (as opposed to the handler being skipped).
    const ECHO_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (data (i32.const 1024) "{\"cleaned_by\":\"wasm\"}")
          (func (export "alloc") (param $size i32) (result i32)
            i32.const 0)
          (func (export "handle") (param $ptr i32) (param $len i32) (result i64)
            i64.const 4398046511125)  ;; (1024 << 32) | 21
        )
    "#;
    let bytes = wat::parse_str(ECHO_WAT).expect("valid WAT");
    let mut tmp = NamedTempFile::with_suffix(".wasm").expect("tempfile");
    tmp.write_all(&bytes).expect("write wasm bytes");
    tmp.flush().expect("flush");

    let storage = storage().await;
    storage
        .create_plugin(&orch8_types::plugin::PluginDef {
            name: "cleanup-echo".into(),
            plugin_type: orch8_types::plugin::PluginType::Wasm,
            source: tmp.path().to_str().unwrap().to_string(),
            tenant_id: String::new(),
            enabled: true,
            config: json!({}),
            description: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

    let handlers = Arc::new(registry_with_cleanup());

    let mut seq = mk_sequence(vec![mk_step("work", "boom")]);
    seq.on_failure = Some(vec![mk_step("cleanup", "wasm://cleanup-echo")]);
    storage.create_sequence(&seq).await.unwrap();

    let inst = mk_instance_scheduled(seq.id, json!({}));
    storage.create_instance(&inst).await.unwrap();

    let final_state = run_until_terminal(&storage, &handlers, inst.id).await;
    assert_eq!(final_state, InstanceState::Failed);

    let out = storage
        .get_block_output(inst.id, &BlockId::new("cleanup"))
        .await
        .unwrap();
    assert!(
        out.is_some(),
        "on_failure cleanup should have dispatched the registered wasm:// plugin handler, not skipped it"
    );
    assert_eq!(out.unwrap().output["cleaned_by"], "wasm");
}
