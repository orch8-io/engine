//! End-to-end coverage for the `blob_put` / `blob_get` handlers driven through
//! the full engine: a real sequence stores bytes, passes the artifact key
//! forward via a template, and reads it back.

use std::sync::Arc;

use serde_json::json;

use orch8_storage::artifacts::ObjectArtifactStore;
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::ids::BlockId;
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};

mod common;
use common::*;

/// Like `common::setup`, but wires an in-memory artifact backend so the blob
/// handlers have somewhere to put/get bytes.
async fn setup_with_artifacts(
    blocks: Vec<BlockDefinition>,
) -> (Arc<dyn StorageBackend>, SequenceDefinition, TaskInstance) {
    let storage: Arc<dyn StorageBackend> = Arc::new(
        SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
    );
    let seq = mk_sequence(blocks);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_with_ctx(seq.id, json!({}));
    storage.create_instance(&inst).await.unwrap();
    (storage, seq, inst)
}

#[tokio::test]
async fn put_then_get_roundtrip_via_templated_ref() {
    let (storage, seq, inst) = setup_with_artifacts(vec![
        mk_step_with_params("put", "blob_put", json!({ "text": "e2e payload" })),
        mk_step_with_params(
            "get",
            "blob_get",
            json!({ "ref": "{{outputs.put.artifact.key}}", "encoding": "utf8" }),
        ),
    ])
    .await;

    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    // Instance ran to completion.
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    // blob_get returned exactly what blob_put stored.
    let out = storage
        .get_block_output(inst.id, &BlockId::new("get"))
        .await
        .unwrap()
        .expect("blob_get produced output");
    assert_eq!(out.output["text"], "e2e payload");
    assert_eq!(out.output["encoding"], "utf8");
}

#[tokio::test]
async fn put_base64_then_get_base64_roundtrip() {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;

    let raw = b"\x00\x10\x20\xff bytes";
    let b64 = STANDARD.encode(raw);

    let (storage, seq, inst) = setup_with_artifacts(vec![
        mk_step_with_params(
            "put",
            "blob_put",
            json!({ "data": b64, "content_type": "application/octet-stream" }),
        ),
        mk_step_with_params(
            "get",
            "blob_get",
            json!({ "ref": "{{outputs.put.artifact.key}}" }),
        ),
    ])
    .await;

    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let out = storage
        .get_block_output(inst.id, &BlockId::new("get"))
        .await
        .unwrap()
        .expect("blob_get produced output");
    let decoded = STANDARD
        .decode(out.output["data"].as_str().unwrap())
        .unwrap();
    assert_eq!(decoded, raw);
}

#[tokio::test]
async fn blob_get_missing_ref_fails_instance() {
    let (storage, seq, inst) = setup_with_artifacts(vec![mk_step_with_params(
        "get",
        "blob_get",
        json!({ "ref": "does-not-exist/key" }),
    )])
    .await;

    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(
        final_inst.state,
        InstanceState::Failed,
        "a missing artifact is a permanent error that fails the instance"
    );
}

/// Drive a single-step sequence and return the terminal instance state — used
/// by the error-path cases below to exercise blob handler branches end-to-end.
async fn run_to_state(blocks: Vec<BlockDefinition>) -> InstanceState {
    let (storage, seq, inst) = setup_with_artifacts(blocks).await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;
    storage.get_instance(inst.id).await.unwrap().unwrap().state
}

#[tokio::test]
async fn put_with_no_input_fails_instance() {
    let state = run_to_state(vec![mk_step_with_params("put", "blob_put", json!({}))]).await;
    assert_eq!(state, InstanceState::Failed);
}

#[tokio::test]
async fn put_invalid_base64_fails_instance() {
    let state = run_to_state(vec![mk_step_with_params(
        "put",
        "blob_put",
        json!({ "data": "!!! not base64" }),
    )])
    .await;
    assert_eq!(state, InstanceState::Failed);
}

#[tokio::test]
async fn put_oversize_fails_instance() {
    let state = run_to_state(vec![mk_step_with_params(
        "put",
        "blob_put",
        json!({ "text": "way too long", "max_size_bytes": 2 }),
    )])
    .await;
    assert_eq!(state, InstanceState::Failed);
}

#[tokio::test]
async fn get_unknown_encoding_fails_instance() {
    let state = run_to_state(vec![
        mk_step_with_params("put", "blob_put", json!({ "text": "x" })),
        mk_step_with_params(
            "get",
            "blob_get",
            json!({ "ref": "{{outputs.put.artifact.key}}", "encoding": "xml" }),
        ),
    ])
    .await;
    assert_eq!(state, InstanceState::Failed);
}

#[tokio::test]
async fn get_non_utf8_as_utf8_fails_instance() {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    let state = run_to_state(vec![
        mk_step_with_params(
            "put",
            "blob_put",
            json!({ "data": STANDARD.encode([0xFF, 0xFE]) }),
        ),
        mk_step_with_params(
            "get",
            "blob_get",
            json!({ "ref": "{{outputs.put.artifact.key}}", "encoding": "utf8" }),
        ),
    ])
    .await;
    assert_eq!(state, InstanceState::Failed);
}

#[tokio::test]
async fn put_explicit_content_type_roundtrips() {
    let (storage, seq, inst) = setup_with_artifacts(vec![
        mk_step_with_params(
            "put",
            "blob_put",
            json!({ "text": "<h1>hi</h1>", "content_type": "text/html" }),
        ),
        mk_step_with_params(
            "get",
            "blob_get",
            json!({ "ref": "{{outputs.put.artifact.key}}", "encoding": "utf8" }),
        ),
    ])
    .await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let put = storage
        .get_block_output(inst.id, &BlockId::new("put"))
        .await
        .unwrap()
        .expect("put output");
    assert_eq!(put.output["artifact"]["content_type"], "text/html");

    let get = storage
        .get_block_output(inst.id, &BlockId::new("get"))
        .await
        .unwrap()
        .expect("get output");
    assert_eq!(get.output["text"], "<h1>hi</h1>");
}

#[tokio::test]
async fn put_without_artifact_backend_fails_instance() {
    // `common::setup` wires no artifact store → put_artifact returns
    // Unsupported → blob_put maps it to a permanent error → instance fails.
    let (storage, seq, inst) = setup(vec![mk_step_with_params(
        "put",
        "blob_put",
        json!({ "text": "x" }),
    )])
    .await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;
    assert_eq!(
        storage.get_instance(inst.id).await.unwrap().unwrap().state,
        InstanceState::Failed
    );
}

#[tokio::test]
async fn get_without_artifact_backend_fails_instance() {
    let (storage, seq, inst) = setup(vec![mk_step_with_params(
        "get",
        "blob_get",
        json!({ "ref": "some/key" }),
    )])
    .await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;
    assert_eq!(
        storage.get_instance(inst.id).await.unwrap().unwrap().state,
        InstanceState::Failed
    );
}

#[tokio::test]
async fn dry_run_put_skips_storage_e2e() {
    let storage: Arc<dyn StorageBackend> = Arc::new(
        SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
    );
    let seq = mk_sequence(vec![mk_step_with_params(
        "put",
        "blob_put",
        json!({ "text": "hello" }),
    )]);
    storage.create_sequence(&seq).await.unwrap();
    let mut inst = mk_instance_with_ctx(seq.id, json!({}));
    inst.context.runtime.dry_run = true;
    storage.create_instance(&inst).await.unwrap();

    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
    let out = storage
        .get_block_output(inst.id, &BlockId::new("put"))
        .await
        .unwrap()
        .expect("put output");
    assert_eq!(out.output["dry_run"], true);
}
