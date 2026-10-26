//! A transferred composite must resume its frozen collection and tree cursor.
use chrono::{Duration, Timelike, Utc};
use ed25519_dalek::SigningKey;
use orch8_engine::{
    capsule::{
        CapsuleExportRequest, CapsuleImportRequest, export_paused_capsule,
        verify_and_import_paused_capsule_bytes,
    },
    evaluator::ensure_execution_tree,
};
use orch8_storage::{
    StorageBackend, artifacts::ObjectArtifactStore, encrypting::EncryptingStorage,
    sqlite::SqliteStorage,
};
use orch8_types::{
    checkpoint::Checkpoint,
    continuity::{
        CapsuleRequirements, ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState,
        RuntimeId,
    },
    encryption::FieldEncryptor,
    execution::NodeState,
    ids::BlockId,
    instance::InstanceState,
    output::BlockOutput,
    sequence::{BlockDefinition, ForEachDef},
};
use serde_json::json;
use std::sync::{Arc, Mutex};
mod common;

#[allow(clippy::too_many_lines)] // complete paused composite transfer and scheduler continuation
async fn assert_foreach_transfer(destination: Option<Arc<dyn StorageBackend>>) {
    let source: Arc<dyn StorageBackend> = Arc::new(
        SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
    );
    let source: Arc<dyn StorageBackend> = Arc::new(EncryptingStorage::new(
        source,
        FieldEncryptor::from_bytes(&[24; 32]),
    ));
    let destination = destination.unwrap_or_else(|| source.clone());
    let mut sequence = common::mk_sequence(vec![BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("each"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![common::mk_step("body", "record")],
        max_iterations: 10,
        retain_iterations: None,
    }))]);
    // Linux clocks can produce nanoseconds that PostgreSQL cannot store.
    // Force that precision on every host so cross-backend identity is tested.
    sequence.created_at = sequence.created_at.with_nanosecond(123_456_789).unwrap();
    source.create_sequence(&sequence).await.unwrap();
    if !Arc::ptr_eq(&source, &destination) {
        destination.create_sequence(&sequence).await.unwrap();
    }
    let mut instance = common::mk_instance_with_ctx(sequence.id, json!({"items":[999],"item":10}));
    instance.state = InstanceState::Paused;
    instance.context.runtime.instance_id = Some(instance.id.to_string());
    let large_key = format!("{}:ctx:data:large", instance.id);
    instance.context.data["large"] = json!({"_externalized":true,"_ref":"custom-portable"});
    source.create_instance(&instance).await.unwrap();
    source
        .save_externalized_state(
            instance.id,
            "custom-portable",
            &json!({"_externalized":true,"_ref":large_key}),
        )
        .await
        .unwrap();
    source
        .save_externalized_state(instance.id, &large_key, &json!({"value":"preserved"}))
        .await
        .unwrap();
    // Exercise the catalog cursor beyond one backend page.
    for n in 0..1001 {
        source
            .save_externalized_state(instance.id, &format!("extra-{n:04}"), &json!(n))
            .await
            .unwrap();
    }

    let tree = ensure_execution_tree(source.as_ref(), &instance, &sequence.blocks)
        .await
        .unwrap();
    for node in &tree {
        source
            .update_node_state(
                node.id,
                if node.block_id.as_str() == "each" {
                    NodeState::Running
                } else {
                    NodeState::Completed
                },
            )
            .await
            .unwrap();
    }
    let snapshot_key = format!("inst:{}:foreach:each:items", instance.id);
    let shadow_key = format!("inst:{}:foreach:each:shadowed_item_var", instance.id);
    source
        .save_externalized_state(instance.id, &snapshot_key, &json!([10, 20, 30]))
        .await
        .unwrap();
    source
        .save_externalized_state(
            instance.id,
            &shadow_key,
            &json!({"present":true,"value":"original-item"}),
        )
        .await
        .unwrap();
    source
        .save_block_output(&BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            block_id: BlockId::new("each"),
            output: json!({"_index":0,"_total":3,"_item_var":"item","_snapshot_ref":snapshot_key}),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    source
        .save_checkpoint(&Checkpoint {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            checkpoint_data: json!({}),
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    let continuity = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: instance.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    source
        .create_continuity_execution(&continuity)
        .await
        .unwrap();
    let runtime = RuntimeId::new();
    let encryptor = FieldEncryptor::from_bytes(&[23; 32]);
    let signed = export_paused_capsule(
        source.as_ref(),
        CapsuleExportRequest {
            continuity,
            destination_runtime_id: Some(runtime),
            requirements: CapsuleRequirements::default(),
            expires_at: Utc::now() + Duration::minutes(5),
            signing_key_id: "source".into(),
            encryption_key_id: "destination".into(),
        },
        &SigningKey::from_bytes(&[22; 32]),
        &encryptor,
    )
    .await
    .unwrap();
    let sealed = source
        .get_artifact(&signed.manifest.payload_artifact.key)
        .await
        .unwrap()
        .unwrap();
    // Relabeling a 1.1 payload as legacy must not bypass required recovery.
    let mut downgraded = signed.manifest.clone();
    downgraded.schema = orch8_types::continuity::CapsuleSchemaVersion::V1;
    let downgraded = orch8_publisher::capsule::sign_capsule_manifest(
        downgraded,
        &SigningKey::from_bytes(&[22; 32]),
    )
    .unwrap();
    assert!(matches!(
        verify_and_import_paused_capsule_bytes(
            destination.as_ref(),
            &downgraded,
            &sealed,
            CapsuleImportRequest {
                tenant_id: &instance.tenant_id,
                destination_runtime_id: runtime,
                destination_instance_id: None,
                expected_epoch: ExecutionEpoch::initial(),
                trusted_public_keys: std::slice::from_ref(&signed.public_key),
                now: Utc::now()
            },
            &encryptor
        )
        .await,
        Err(orch8_engine::capsule::CapsuleServiceError::MissingExecutionState)
    ));
    let (imported, _) = verify_and_import_paused_capsule_bytes(
        destination.as_ref(),
        &signed,
        &sealed,
        CapsuleImportRequest {
            tenant_id: &instance.tenant_id,
            destination_runtime_id: runtime,
            destination_instance_id: None,
            expected_epoch: ExecutionEpoch::initial(),
            trusted_public_keys: std::slice::from_ref(&signed.public_key),
            now: Utc::now(),
        },
        &encryptor,
    )
    .await
    .unwrap();
    assert_eq!(
        destination
            .get_execution_tree(imported.id)
            .await
            .unwrap()
            .len(),
        tree.len(),
        "import discarded the composite execution tree"
    );
    assert_eq!(
        destination
            .get_externalized_state(
                imported.id,
                &format!("inst:{}:foreach:each:items", imported.id)
            )
            .await
            .unwrap(),
        Some(json!([10, 20, 30]))
    );
    let custom = format!("capsule:{}:custom-portable", imported.id);
    let large = format!("{}:ctx:data:large", imported.id);
    assert_eq!(
        imported.context.data["large"],
        json!({"_externalized":true,"_ref":custom})
    );
    assert_eq!(
        destination
            .get_externalized_state(imported.id, &custom)
            .await
            .unwrap(),
        Some(json!({"_externalized":true,"_ref":large}))
    );
    assert_eq!(
        destination
            .get_externalized_state(imported.id, &large)
            .await
            .unwrap(),
        Some(json!({"value":"preserved"}))
    );
    assert_eq!(
        destination
            .get_externalized_state(imported.id, &format!("capsule:{}:extra-1000", imported.id))
            .await
            .unwrap(),
        Some(json!(1000))
    );
    assert_eq!(
        imported.context.runtime.instance_id,
        Some(imported.id.to_string())
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    let mut registry = common::registry();
    registry.register("record", move |ctx| {
        let seen = captured.clone();
        Box::pin(async move {
            let item = ctx.context.data["item"].clone();
            seen.lock().unwrap().push(item.clone());
            Ok(item)
        })
    });
    destination
        .update_instance_state(imported.id, InstanceState::Scheduled, Some(Utc::now()))
        .await
        .unwrap();
    let registry = Arc::new(registry);
    let semaphore = common::semaphore(4);
    let cache = common::cache();
    let config = common::default_config();
    let cancel = tokio_util::sync::CancellationToken::new();
    for _ in 0..30 {
        orch8_engine::scheduler::tick_once(
            &destination,
            &registry,
            &semaphore,
            &config,
            &cache,
            &cancel,
        )
        .await
        .unwrap();
        if destination
            .get_instance(imported.id)
            .await
            .unwrap()
            .unwrap()
            .state
            == InstanceState::Completed
        {
            break;
        }
    }
    let finished = destination
        .get_instance(imported.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(finished.state, InstanceState::Completed);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![json!(20), json!(30)],
        "must use frozen source items and never replay item10"
    );
    assert_eq!(finished.context.data["item"], json!("original-item"));
}

orch8_storage::backend_test!(async fn capsule_resumes_frozen_foreach_without_replay_or_lost_items(b) {
    assert_foreach_transfer(Some(b.storage.clone())).await;
});

#[tokio::test]
async fn capsule_composite_import_can_share_the_source_store() {
    assert_foreach_transfer(None).await;
}
