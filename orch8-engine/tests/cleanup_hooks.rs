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
#[cfg(feature = "wasm")]
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

#[cfg(feature = "wasm")]
#[tokio::test]
async fn tenant_wasm_output_limits_apply_to_concurrent_workflows() {
    const CHILD: &str = "ORCH8_TEST_TENANT_WASM_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tenant_wasm_output_limits_apply_to_concurrent_workflows",
            ])
            .env(CHILD, "1")
            .env(
                "ORCH8_WASM_TENANT_LIMITS_JSON",
                r#"{"north":{"max_output_bytes":4},"south":{"max_output_bytes":64}}"#,
            )
            .env("ORCH8_WASM_MAX_OUTPUT_BYTES", "128")
            .env_remove("ORCH8_WASM_PLUGIN_DIR")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        return;
    }

    let module = wat::parse_str(
        r#"(module
        (memory (export "memory") 1)
        (data (i32.const 1024) "{\22ok\22:true}")
        (func (export "alloc") (param i32) (result i32) i32.const 0)
        (func (export "handle") (param i32 i32) (result i64) i64.const 4398046511115)
    )"#,
    )
    .unwrap();
    let path = tempfile::NamedTempFile::with_suffix(".wasm").unwrap();
    std::fs::write(path.path(), module).unwrap();
    let storage = storage().await;
    storage
        .create_plugin(&orch8_types::plugin::PluginDef {
            name: "tenant-limits".into(),
            plugin_type: orch8_types::plugin::PluginType::Wasm,
            source: path.path().to_str().unwrap().to_owned(),
            tenant_id: String::new(),
            enabled: true,
            config: json!({}),
            description: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

    let mut instances = Vec::new();
    for name in ["north", "south"] {
        let tenant = orch8_types::ids::TenantId::new(name).unwrap();
        let mut sequence = mk_sequence(vec![mk_step("work", "wasm://tenant-limits")]);
        sequence.tenant_id = tenant.clone();
        storage.create_sequence(&sequence).await.unwrap();
        let mut instance = mk_instance_scheduled(sequence.id, json!({}));
        instance.tenant_id = tenant;
        storage.create_instance(&instance).await.unwrap();
        instances.push(instance.id);
    }
    let handlers = Arc::new(registry());
    assert_eq!(
        run_until_terminal(&storage, &handlers, instances[0]).await,
        InstanceState::Failed
    );
    assert_eq!(
        run_until_terminal(&storage, &handlers, instances[1]).await,
        InstanceState::Completed
    );
    let output = storage
        .get_block_output(instances[1], &BlockId::new("work"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.output, json!({"ok":true}));
    let error = storage
        .get_block_output(instances[0], &BlockId::new("work"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        error.output["message"]
            .as_str()
            .unwrap()
            .contains("limit of 4")
    );
}

#[cfg(feature = "wasm")]
#[tokio::test]
async fn independent_executors_load_the_same_shared_wasm_address() {
    use orch8_storage::artifacts::ObjectArtifactStore;
    use orch8_types::plugin::{
        PluginDef, PluginType, WASM_ARTIFACT_SOURCE_PREFIX, WasmArtifactDigest,
    };

    const ROOT: &str = "ORCH8_TEST_SHARED_WASM_ROOT";
    const DIGEST: &str = "ORCH8_TEST_SHARED_WASM_DIGEST";
    if let Some(root) = std::env::var_os(ROOT) {
        let store = ObjectArtifactStore::local(root.to_str().unwrap()).unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap()
                .with_artifact_store(Arc::new(store)),
        );
        let digest = std::env::var(DIGEST).unwrap();
        let mut sequence = mk_sequence(vec![mk_step("work", "wasm://shared-module")]);
        let tenant = orch8_types::ids::TenantId::new("shared-wasm").unwrap();
        sequence.tenant_id = tenant.clone();
        let plugin = PluginDef {
            name: "shared-module".into(),
            plugin_type: PluginType::Wasm,
            source: format!("{WASM_ARTIFACT_SOURCE_PREFIX}{digest}"),
            tenant_id: tenant.to_string(),
            enabled: true,
            config: json!({}),
            description: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        storage.create_plugin(&plugin).await.unwrap();
        storage.create_sequence(&sequence).await.unwrap();
        let mut run = mk_instance_scheduled(sequence.id, json!({}));
        run.tenant_id = tenant;
        storage.create_instance(&run).await.unwrap();
        assert_eq!(
            run_until_terminal(&storage, &Arc::new(registry()), run.id).await,
            InstanceState::Completed
        );
        let output = storage
            .get_block_output(run.id, &BlockId::new("work"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(output.output, json!({"ok":true}));
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let store = ObjectArtifactStore::local(root.path().to_str().unwrap()).unwrap();
    let module = wat::parse_str(
        r#"(module
          (memory (export "memory") 1)
          (data (i32.const 1024) "{\22ok\22:true}")
          (func (export "alloc") (param i32) (result i32) i32.const 0)
          (func (export "handle") (param i32 i32) (result i64) i64.const 4398046511115))"#,
    )
    .unwrap();
    let digest = WasmArtifactDigest::for_bytes(&module);
    store
        .put_wasm_module("shared-wasm", &digest, module.into())
        .await
        .unwrap();
    for _ in 0..2 {
        let sandbox = tempfile::tempdir().unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "independent_executors_load_the_same_shared_wasm_address",
            ])
            .env(ROOT, root.path())
            .env(DIGEST, digest.as_str())
            .env("ORCH8_WASM_TENANT_CACHE_SLOTS_JSON", r#"{"shared-wasm":2}"#)
            .env_remove("ORCH8_WASM_PLUGIN_DIR")
            .current_dir(sandbox.path())
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
    }
}

#[cfg(feature = "wasm")]
#[tokio::test]
#[allow(clippy::too_many_lines)] // Keep the suspension, collection, and recovery sequence together.
async fn wasm_collection_releases_orphans_and_a_suspended_run_keeps_its_old_pin() {
    use orch8_engine::testing::TestEngine;
    use orch8_storage::artifacts::ObjectArtifactStore;
    use orch8_types::plugin::{WASM_ARTIFACT_SOURCE_PREFIX, WasmArtifactDigest};
    use orch8_types::sequence::BlockDefinition;

    let directory = tempfile::tempdir().unwrap();
    let store = ObjectArtifactStore::local(directory.path().to_str().unwrap()).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(store)),
    );
    let module = wat::parse_str(
        r#"(module
        (memory (export "memory") 1)
        (data (i32.const 1024) "7")
        (func (export "alloc") (param i32) (result i32) i32.const 0)
        (func (export "handle") (param i32 i32) (result i64) i64.const 4398046511105))"#,
    )
    .unwrap();
    let orphan = bytes::Bytes::from(wat::parse_str("(module)").unwrap());
    let old = WasmArtifactDigest::for_bytes(&module);
    let unused = WasmArtifactDigest::for_bytes(&orphan);
    let mut gate = mk_step("gate", "noop");
    if let BlockDefinition::Step(step) = &mut gate {
        step.wait_for_input = Some(serde_json::from_value(json!({"prompt":"Approve"})).unwrap());
    }
    let handler = format!("wasm://{WASM_ARTIFACT_SOURCE_PREFIX}{}", old.as_str());
    let mut revision = mk_sequence(vec![gate, mk_step("plugin", &handler)]);
    revision.deprecated = true;
    let tenant = &revision.tenant_id;
    storage
        .put_shared_wasm_module(tenant.as_str(), &old, module.into())
        .await
        .unwrap();
    storage
        .put_shared_wasm_module(tenant.as_str(), &unused, orphan.clone())
        .await
        .unwrap();
    let engine = TestEngine::builder().storage(storage.clone()).build().await;
    let id = engine.start(&revision).await;
    for _ in 0..4 {
        engine.tick().await;
    }
    assert_eq!(
        storage.get_instance(id).await.unwrap().unwrap().state,
        InstanceState::Waiting
    );

    let early = chrono::Utc::now() - chrono::Duration::seconds(60);
    let expired = chrono::Utc::now() + chrono::Duration::seconds(60);
    assert!(
        storage
            .collect_unused_wasm_modules(tenant, early, 10, true)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        storage
            .collect_unused_wasm_modules(tenant, early, 10, true)
            .await
            .unwrap()
            .is_empty()
    );
    let selected = storage
        .collect_unused_wasm_modules(tenant, expired, 10, false)
        .await
        .unwrap();
    assert_eq!(selected, vec![unused.as_str()]);
    assert!(
        storage
            .get_shared_wasm_module(tenant.as_str(), &unused, 1024)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        storage
            .collect_unused_wasm_modules(tenant, expired, 10, true)
            .await
            .unwrap(),
        selected
    );
    assert!(
        storage
            .get_shared_wasm_module(tenant.as_str(), &unused, 1024)
            .await
            .unwrap()
            .is_none()
    );

    let never = format!("wasm://{WASM_ARTIFACT_SOURCE_PREFIX}{}", unused.as_str());
    let mut proposed = mk_sequence(vec![mk_step("use_orphan", &never)]);
    proposed.tenant_id = tenant.clone();
    proposed.name = "reinstalled-module".into();
    assert!(storage.create_sequence(&proposed).await.is_err());
    storage
        .put_shared_wasm_module(tenant.as_str(), &unused, orphan)
        .await
        .unwrap();
    storage.create_sequence(&proposed).await.unwrap();

    storage
        .enqueue_signal(&Signal {
            id: uuid::Uuid::now_v7(),
            instance_id: id,
            signal_type: SignalType::Custom("human_input:gate".into()),
            payload: json!({"value":"yes"}),
            delivered: false,
            created_at: engine.now(),
            delivered_at: None,
        })
        .await
        .unwrap();
    engine.advance(std::time::Duration::from_secs(1));
    for _ in 0..8 {
        engine.tick().await;
    }
    assert_eq!(
        storage.get_instance(id).await.unwrap().unwrap().state,
        InstanceState::Completed
    );
    assert_eq!(
        storage
            .get_block_output(id, &BlockId::new("plugin"))
            .await
            .unwrap()
            .unwrap()
            .output,
        json!(7)
    );
}

#[cfg(feature = "wasm")]
#[tokio::test]
async fn pinned_wasm_revision_waits_for_all_modules_before_any_step_runs() {
    use orch8_engine::testing::TestEngine;
    use orch8_storage::artifacts::ObjectArtifactStore;
    use orch8_types::plugin::{WASM_ARTIFACT_SOURCE_PREFIX, WasmArtifactDigest};

    let directory = tempfile::tempdir().unwrap();
    let store = ObjectArtifactStore::local(directory.path().to_str().unwrap()).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(store)),
    );
    let bytes = wat::parse_str(
        r#"(module
        (memory (export "memory") 1)
        (data (i32.const 1024) "4")
        (func (export "alloc") (param i32) (result i32) i32.const 0)
        (func (export "handle") (param i32 i32) (result i64) i64.const 4398046511105))"#,
    )
    .unwrap();
    let digest = WasmArtifactDigest::for_bytes(&bytes);
    let handler = format!("wasm://{WASM_ARTIFACT_SOURCE_PREFIX}{}", digest.as_str());
    let sequence = mk_sequence(vec![mk_step("before", "noop"), mk_step("plugin", &handler)]);
    let engine = TestEngine::builder().storage(storage.clone()).build().await;
    let id = engine.start(&sequence).await;
    engine.tick().await;
    assert_eq!(
        storage.get_instance(id).await.unwrap().unwrap().state,
        InstanceState::Scheduled
    );
    assert!(storage.get_all_outputs(id).await.unwrap().is_empty());

    storage
        .put_shared_wasm_module(sequence.tenant_id.as_str(), &digest, bytes.into())
        .await
        .unwrap();
    engine.advance(std::time::Duration::from_secs(3));
    for _ in 0..4 {
        engine.tick().await;
    }
    assert_eq!(
        storage.get_instance(id).await.unwrap().unwrap().state,
        InstanceState::Completed
    );
    assert_eq!(
        storage
            .get_block_output(id, &BlockId::new("plugin"))
            .await
            .unwrap()
            .unwrap()
            .output,
        json!(4)
    );
}

#[cfg(feature = "wasm")]
#[tokio::test]
async fn published_wasm_revision_keeps_running_the_original_module_after_alias_update() {
    use orch8_engine::handlers::wasm_plugin::pin_sequence_modules;
    use orch8_storage::artifacts::ObjectArtifactStore;
    use orch8_types::plugin::{
        PluginDef, PluginType, WASM_ARTIFACT_SOURCE_PREFIX, WasmArtifactDigest,
    };

    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(ObjectArtifactStore::local(root.path().to_str().unwrap()).unwrap());
    let storage: Arc<dyn StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(store),
    );
    let tenant = orch8_types::ids::TenantId::new("wasm-revisions").unwrap();
    let module = |number| {
        wat::parse_str(format!(
            r#"(module
                (memory (export "memory") 1)
                (data (i32.const 1024) "{number}")
                (func (export "alloc") (param i32) (result i32) i32.const 0)
                (func (export "handle") (param i32 i32) (result i64) i64.const 4398046511105))"#,
        ))
        .unwrap()
    };
    let original = module(1);
    let replacement = module(2);
    let old_digest = WasmArtifactDigest::for_bytes(&original);
    let new_digest = WasmArtifactDigest::for_bytes(&replacement);
    storage
        .put_shared_wasm_module(tenant.as_str(), &old_digest, original.into())
        .await
        .unwrap();
    storage
        .put_shared_wasm_module(tenant.as_str(), &new_digest, replacement.into())
        .await
        .unwrap();
    let mut plugin = PluginDef {
        name: "score".into(),
        plugin_type: PluginType::Wasm,
        source: format!("{WASM_ARTIFACT_SOURCE_PREFIX}{}", old_digest.as_str()),
        tenant_id: tenant.to_string(),
        enabled: true,
        config: json!({}),
        description: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    storage.create_plugin(&plugin).await.unwrap();
    let mut sequence = mk_sequence(vec![mk_step("work", "wasm://score")]);
    sequence.tenant_id = tenant.clone();
    let mut next = sequence.clone();
    next.id = orch8_types::ids::SequenceId::new();
    next.version += 1;
    pin_sequence_modules(storage.as_ref(), &mut sequence)
        .await
        .unwrap();
    storage.create_sequence(&sequence).await.unwrap();
    let mut original_run = mk_instance_scheduled(sequence.id, json!({}));
    original_run.tenant_id = tenant.clone();
    storage.create_instance(&original_run).await.unwrap();

    plugin.source = format!("{WASM_ARTIFACT_SOURCE_PREFIX}{}", new_digest.as_str());
    storage.update_plugin(&plugin).await.unwrap();
    pin_sequence_modules(storage.as_ref(), &mut next)
        .await
        .unwrap();
    storage.create_sequence(&next).await.unwrap();
    let mut new_run = mk_instance_scheduled(next.id, json!({}));
    new_run.tenant_id = tenant;
    storage.create_instance(&new_run).await.unwrap();
    let handlers = Arc::new(registry());
    for (run, expected) in [(original_run, json!(1)), (new_run, json!(2))] {
        assert_eq!(
            run_until_terminal(&storage, &handlers, run.id).await,
            InstanceState::Completed
        );
        let output = storage
            .get_block_output(run.id, &BlockId::new("work"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(output.output, expected);
    }
}
