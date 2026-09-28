use chrono::Utc;
use orch8_storage::{
    ContinuityStore as _, InstanceStore as _, OutputStore as _, SequenceStore as _,
    SignalStore as _,
};
use orch8_types::context::ExecutionContext;
use orch8_types::continuity::{OwnershipState, RuntimeId};
use orch8_types::ids::{BlockId, InstanceId, Namespace, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::sequence::SequenceDefinition;
use orch8_types::signal::{Signal, SignalType};

use super::*;

fn sequence(tenant: &str) -> SequenceDefinition {
    serde_json::from_value(serde_json::json!({
        "id": uuid::Uuid::now_v7(), "tenant_id": tenant, "namespace": "default",
        "name": "onboarding", "version": 1, "created_at": Utc::now(),
        "blocks": [
            {"type": "step", "id": "welcome", "handler": "noop", "params": {}},
            {"type": "step", "id": "wait_approval", "handler": "noop", "params": {}}
        ]
    }))
    .unwrap()
}

fn instance(seq: &SequenceDefinition, state: InstanceState) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq.id,
        tenant_id: seq.tenant_id.clone(),
        namespace: Namespace::new("default"),
        state,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: serde_json::json!({"customer": "c-1"}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    }
}

fn args(target: &str, source: &std::path::Path) -> MigrateToArgs {
    MigrateToArgs {
        to: Some(target.to_owned()),
        source: Some(source.display().to_string()),
        target_api_key: None,
        tenant: Some("acme".into()),
        wait_for_idle: false,
        idle_timeout_secs: 1,
        migration_id: None,
        dry_run: false,
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn migrates_in_flight_state_fences_source_and_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("embedded.db");
    let seq = sequence("acme");
    let waiting = instance(&seq, InstanceState::Waiting);
    let busy = instance(&seq, InstanceState::Running);
    {
        let source = orch8_storage::sqlite::SqliteStorage::file(db.to_str().unwrap())
            .await
            .unwrap();
        source.create_sequence(&seq).await.unwrap();
        source.create_instance(&waiting).await.unwrap();
        source.create_instance(&busy).await.unwrap();
        source
            .save_block_output(&BlockOutput {
                id: uuid::Uuid::now_v7(),
                instance_id: waiting.id,
                block_id: BlockId::new("welcome"),
                output: serde_json::json!({"sent": true}),
                output_ref: None,
                output_size: 13,
                attempt: 0,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
        source
            .enqueue_signal(&Signal {
                id: uuid::Uuid::now_v7(),
                instance_id: waiting.id,
                signal_type: SignalType::Custom("approve".into()),
                payload: serde_json::json!({"by": "ops"}),
                delivered: false,
                created_at: Utc::now(),
                delivered_at: None,
            })
            .await
            .unwrap();
    }

    let target = orch8_api::test_harness::spawn_test_server().await;
    let report = migrate(&args(&target.base_url, &db), None).await.unwrap();
    assert_eq!(report.sequences_sent, 1);
    assert_eq!(report.sequences_created, 1);
    assert_eq!(report.migrated, vec![waiting.id.to_string()]);
    assert_eq!(report.refused.len(), 1, "{report:?}");
    assert_eq!(report.refused[0].instance_id, busy.id.to_string());
    assert!(report.refused[0].reason.contains("running"));

    // Target has the run with its history and pending signal, owned at epoch 1.
    let moved = target
        .storage
        .get_instance(waiting.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(moved.state, InstanceState::Waiting);
    assert_eq!(moved.metadata["customer"], "c-1");
    assert_eq!(moved.metadata[MIGRATION_METADATA_KEY]["phase"], "imported");
    assert_eq!(
        target
            .storage
            .get_all_outputs(waiting.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        target
            .storage
            .get_pending_signals(waiting.id)
            .await
            .unwrap()
            .len(),
        1
    );
    let tenant = TenantId::new("acme").unwrap();
    let owned = target
        .storage
        .get_continuity_execution_by_instance(&tenant, waiting.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(owned.epoch.get(), 1);
    assert_eq!(owned.state, OwnershipState::Owned);

    // Source is fenced: paused, ownership handed to the target runtime.
    let source = orch8_storage::sqlite::SqliteStorage::file(db.to_str().unwrap())
        .await
        .unwrap();
    let local = source.get_instance(waiting.id).await.unwrap().unwrap();
    assert_eq!(local.state, InstanceState::Paused);
    assert_eq!(local.metadata[MIGRATION_METADATA_KEY]["phase"], "committed");
    let fence = source
        .get_continuity_execution_by_instance(&tenant, waiting.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fence.state, OwnershipState::Transferring);
    assert_eq!(fence.epoch.get(), 1);
    assert_eq!(fence.owner_runtime_id, owned.owner_runtime_id);
    assert_eq!(
        orch8_engine::ownership::local_ownership(&source, &tenant, waiting.id)
            .await
            .unwrap(),
        orch8_engine::ownership::LocalOwnership::Transferring
    );
    // The refused instance was never fenced.
    assert!(
        source
            .get_continuity_execution_by_instance(&tenant, busy.id)
            .await
            .unwrap()
            .is_none_or(|e| e.state == OwnershipState::Owned)
    );
    drop(source);

    // Re-running resumes: nothing moves twice.
    let again = migrate(&args(&target.base_url, &db), None).await.unwrap();
    assert!(again.migrated.is_empty());
    assert_eq!(again.already_migrated, vec![waiting.id.to_string()]);
    assert_eq!(again.sequences_created, 0);
    let _ = RuntimeId::new();
}

#[tokio::test]
async fn target_import_is_idempotent_for_retried_batches() {
    let target = orch8_api::test_harness::spawn_test_server().await;
    let seq = sequence("acme");
    let run = instance(&seq, InstanceState::Scheduled);
    let continuity = orch8_types::continuity::ContinuityExecution {
        continuity_id: orch8_types::continuity::ContinuityId::new(),
        tenant_id: run.tenant_id.clone(),
        current_instance_id: run.id,
        owner_runtime_id: RuntimeId::new(),
        epoch: orch8_types::continuity::ExecutionEpoch::initial(),
        state: OwnershipState::Transferring,
        updated_at: Utc::now(),
    };
    let request = MigrationImportRequest {
        migration_id: uuid::Uuid::now_v7(),
        source_engine_id: "test".into(),
        sequences: vec![seq],
        instances: vec![MigratedInstance {
            instance: run.clone(),
            execution_tree: Vec::new(),
            block_outputs: Vec::new(),
            effect_receipts: Vec::new(),
            pending_signals: Vec::new(),
            continuity,
        }],
    };
    let client = crate::build_client(None, Some("acme")).unwrap();
    let base = target_base(&target.base_url);
    let first = send(&client, &base, &request).await.unwrap();
    assert_eq!(
        first.instances[0].status,
        orch8_types::migration::ImportStatus::Imported
    );
    let second = send(&client, &base, &request).await.unwrap();
    assert_eq!(
        second.instances[0].status,
        orch8_types::migration::ImportStatus::AlreadyPresent
    );
    assert_eq!(second.sequences.existing, 1);
    let stored = target.storage.get_instance(run.id).await.unwrap().unwrap();
    assert_eq!(stored.state, InstanceState::Scheduled);

    // A different tenant header cannot smuggle records in.
    let other = crate::build_client(None, Some("other")).unwrap();
    assert!(send(&other, &base, &request).await.is_err());
}
