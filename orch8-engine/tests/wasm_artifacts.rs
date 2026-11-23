#![cfg(feature = "wasm")]

use std::path::Path;
use std::sync::Arc;

use chrono::{TimeDelta, Utc};
use orch8_engine::{handlers::wasm_plugin::pin_sequence_modules, testing::TestEngine};
use orch8_storage::{
    AdminStore as _, OutputStore as _, ResourceStore as _, SequenceStore as _,
    artifacts::ObjectArtifactStore, sqlite::SqliteStorage,
};
use orch8_types::{
    ids::{BlockId, InstanceId, TenantId},
    instance::InstanceState,
    plugin::{PluginDef, PluginType, WASM_ARTIFACT_SOURCE_PREFIX, WasmArtifactDigest},
    sequence::SequenceDefinition,
};
use serde_json::json;

fn module(value: u8) -> Vec<u8> {
    assert!(value < 10);
    wat::parse_str(format!(
        r#"(module
          (memory (export "memory") 1)
          (data (i32.const 1024) "{value}")
          (func (export "alloc") (param i32) (result i32) i32.const 0)
          (func (export "handle") (param i32 i32) (result i64) i64.const 4398046511105)
        )"#
    ))
    .unwrap()
}

fn revision(tenant: &TenantId, version: i32, delay: u64) -> SequenceDefinition {
    serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(), "tenant_id": tenant, "namespace": "default",
        "name": "artifact-pin", "version": version, "created_at": Utc::now(),
        "blocks": [{"type": "step", "id": "score", "handler": "wasm://score",
                    "delay": {"duration": delay}}]
    }))
    .unwrap()
}

async fn open_executor(database: &Path, artifacts: &Path) -> (Arc<SqliteStorage>, TestEngine) {
    let artifact_store = ObjectArtifactStore::local(artifacts.to_str().unwrap()).unwrap();
    let storage = Arc::new(
        SqliteStorage::file_path(database.to_str().unwrap())
            .await
            .unwrap()
            .with_artifact_store(Arc::new(artifact_store)),
    );
    let executor = TestEngine::builder().storage(storage.clone()).build().await;
    (storage, executor)
}

async fn expect_score(executor: &TestEngine, run: InstanceId, value: u8) {
    assert_eq!(executor.drive(run).await.state, InstanceState::Completed);
    let score = executor
        .storage
        .get_block_output(run, &BlockId::new("score"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(score.output, json!(value));
}

#[tokio::test]
async fn mounted_artifacts_keep_a_suspended_revision_pinned_through_alias_update_and_collection() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runs.db");
    let artifacts = directory.path().join("shared");
    std::fs::create_dir(&artifacts).unwrap();
    let (first, first_executor) = open_executor(&database, &artifacts).await;
    let tenant = TenantId::new("plugin-executors").unwrap();
    let [old, new, orphan] = [1, 2, 3].map(|value| {
        let bytes = module(value);
        (WasmArtifactDigest::for_bytes(&bytes), bytes)
    });
    for (digest, bytes) in [&old, &new, &orphan] {
        first
            .put_shared_wasm_module(tenant.as_str(), digest, bytes.clone().into())
            .await
            .unwrap();
    }
    let now = Utc::now();
    let mut alias = PluginDef {
        name: "score".into(),
        plugin_type: PluginType::Wasm,
        source: format!("{WASM_ARTIFACT_SOURCE_PREFIX}{}", old.0.as_str()),
        tenant_id: tenant.to_string(),
        enabled: true,
        config: json!({}),
        description: None,
        created_at: now,
        updated_at: now,
    };
    first.create_plugin(&alias).await.unwrap();
    let mut original = revision(&tenant, 1, 60_000);
    pin_sequence_modules(first.as_ref(), &mut original)
        .await
        .unwrap();

    let waiting = first_executor.start(&original).await;
    let block = BlockId::new("score");
    first_executor.tick().await;
    let parked = first_executor.instance(waiting).await;
    assert_eq!(parked.state, InstanceState::Scheduled);
    assert!(parked.next_fire_at.unwrap() > first_executor.now());
    assert!(
        first
            .get_block_output(waiting, &block)
            .await
            .unwrap()
            .is_none()
    );

    alias.source = format!("{WASM_ARTIFACT_SOURCE_PREFIX}{}", new.0.as_str());
    first.update_plugin(&alias).await.unwrap();
    first
        .update_sequence_status(original.id, "unpublished")
        .await
        .unwrap();
    let mut replacement = revision(&tenant, 2, 0);
    pin_sequence_modules(first.as_ref(), &mut replacement)
        .await
        .unwrap();
    let current = first_executor.start(&replacement).await;
    expect_score(&first_executor, current, 2).await;

    let before_grace = Utc::now() - TimeDelta::minutes(1);
    assert!(
        first
            .collect_unused_wasm_modules(&tenant, before_grace, 8, true)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        first
            .collect_unused_wasm_modules(&tenant, before_grace, 8, true)
            .await
            .unwrap()
            .is_empty()
    );
    let after_grace = Utc::now() + TimeDelta::minutes(1);
    assert_eq!(
        first
            .collect_unused_wasm_modules(&tenant, after_grace, 8, true)
            .await
            .unwrap(),
        [orphan.0.as_str()]
    );
    assert!(
        first
            .get_shared_wasm_module(tenant.as_str(), &orphan.0, 1024)
            .await
            .unwrap()
            .is_none()
    );
    drop(first_executor);
    first.close().await;

    let (_, second_executor) = open_executor(&database, &artifacts).await;
    expect_score(&second_executor, waiting, 1).await;
}
