use chrono::Utc;
use orch8_storage::{
    ContinuityStore as _, InstanceStore as _, OutputStore as _, ResourceStore as _,
    SchedulingStore as _, SequenceStore as _, SignalStore as _,
};
use orch8_types::context::ExecutionContext;
use orch8_types::continuity::{
    EffectId, EffectKind, EffectReceipt, EffectState, OwnershipState, RuntimeId,
};
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
        claimed_at: None,
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
        sub_tenant: Some("acme".into()),
        created_at: now,
        updated_at: now,
    }
}

fn args(target: &str, source: &std::path::Path) -> MigrateToArgs {
    MigrateToArgs {
        to: Some(target.to_owned()),
        source: Some(source.display().to_string()),
        source_artifacts_dir: None,
        target_api_key: None,
        tenant: Some("acme".into()),
        wait_for_idle: false,
        idle_timeout_secs: 1,
        migration_id: None,
        dry_run: false,
        stage_only: false,
        include_terminal_history: false,
        include_cron_schedules: false,
    }
}

#[tokio::test]
async fn interrupted_cron_cutover_fires_the_due_occurrence_only_at_destination() {
    use orch8_engine::clock::{ManualClock, SharedClock};
    use orch8_engine::cron::process_cron_tick;
    use orch8_types::cron::CronSchedule;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.db");
    let source = Arc::new(SqliteStorage::file(path.to_str().unwrap()).await.unwrap());
    let seq = sequence("acme");
    source.create_sequence(&seq).await.unwrap();
    let now = Utc::now();
    let mut schedule: CronSchedule = serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(), "tenant_id": seq.tenant_id, "namespace": seq.namespace,
        "sequence_id": seq.id, "cron_expr": "* * * * *", "timezone": "UTC", "enabled": true,
        "created_at": now, "updated_at": now, "metadata": {},
    }))
    .unwrap();
    let due = orch8_engine::cron::calculate_next_fire_after(&schedule, now).unwrap();
    schedule.next_fire_at = Some(due);
    source.create_cron_schedule(&schedule).await.unwrap();
    let key = schedule.occurrence_key();

    let target = orch8_api::test_harness::spawn_test_server().await;
    let mut arguments = args(&target.base_url, &path);
    let migration_id = uuid::Uuid::now_v7();
    arguments.include_cron_schedules = true;
    arguments.migration_id = Some(migration_id);
    arguments.dry_run = true;
    assert_eq!(
        migrate(&arguments, None).await.unwrap().cron_imported,
        vec![schedule.id]
    );
    assert!(
        source
            .get_cron_schedule(schedule.id)
            .await
            .unwrap()
            .unwrap()
            .enabled
    );
    arguments.dry_run = false;
    let fenced = source
        .fence_cron_for_cloud_migration(&seq.tenant_id, schedule.id, migration_id, now)
        .await
        .unwrap();
    assert_eq!(fenced.last_fire_at, None);
    assert!(
        target
            .storage
            .get_cron_schedule(schedule.id)
            .await
            .unwrap()
            .is_none()
    );

    let clock = SharedClock::new(ManualClock::new(due + chrono::Duration::seconds(1)));
    let source_backend: Arc<dyn StorageBackend> = source.clone();
    process_cron_tick(&source_backend, &clock).await.unwrap();
    let mut stale_run = instance(&seq, InstanceState::Scheduled);
    stale_run.idempotency_key.clone_from(&key);
    assert!(source.create_instance(&stale_run).await.is_err());
    assert!(source.update_cron_schedule(&schedule).await.is_err());
    let filter = InstanceFilter {
        tenant_id: Some(seq.tenant_id.clone()),
        ..InstanceFilter::default()
    };
    assert!(
        source
            .list_instances(&filter, &Pagination::default())
            .await
            .unwrap()
            .is_empty()
    );

    let report = migrate(&arguments, None).await.unwrap();
    assert_eq!(report.cron_imported, vec![schedule.id]);
    let target_backend: Arc<dyn StorageBackend> = target.storage.clone();
    process_cron_tick(&target_backend, &clock).await.unwrap();
    assert_eq!(
        migrate(&arguments, None).await.unwrap().cron_imported,
        vec![schedule.id]
    );
    process_cron_tick(&target_backend, &clock).await.unwrap();
    let runs = target
        .storage
        .list_instances(&filter, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].idempotency_key, key);
}

#[tokio::test]
async fn terminal_history_keeps_original_records_without_dispatch_authority() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.db");
    let source = SqliteStorage::file(path.to_str().unwrap()).await.unwrap();
    let seq = sequence("acme");
    source.create_sequence(&seq).await.unwrap();
    let states = [
        InstanceState::Completed,
        InstanceState::Failed,
        InstanceState::Cancelled,
    ];
    let mut runs = states.map(|state| instance(&seq, state));
    runs[2].parent_instance_id = Some(runs[0].id);
    for run in &runs {
        source.create_instance(run).await.unwrap();
        source
            .set_instance_kv(run.id, "retained", &json!(true))
            .await
            .unwrap();
    }
    let target = orch8_api::test_harness::spawn_test_server().await;
    let mut arguments = args(&target.base_url, &path);
    assert!(
        migrate(&arguments, None)
            .await
            .unwrap()
            .history_imported
            .is_empty()
    );
    arguments.include_terminal_history = true;
    let report = migrate(&arguments, None).await.unwrap();
    assert_eq!(report.history_imported.len(), 3);
    assert!(report.migrated.is_empty());
    for run in &runs {
        let restored = target.storage.get_instance(run.id).await.unwrap().unwrap();
        assert_eq!(restored.state, run.state);
        assert_eq!(restored.metadata, run.metadata);
        assert_eq!(restored.parent_instance_id, run.parent_instance_id);
        assert_eq!(
            target
                .storage
                .get_instance_kv(run.id, "retained")
                .await
                .unwrap(),
            Some(json!(true))
        );
        assert!(
            target
                .storage
                .update_instance_state(run.id, InstanceState::Scheduled, Some(Utc::now()))
                .await
                .is_err()
        );
        assert!(
            source
                .read_only_history_migration(run.id)
                .await
                .unwrap()
                .is_none()
        );
    }
    let client = crate::build_client(None, Some("acme")).unwrap();
    let response = client
        .post(format!(
            "{}/instances/{}/retry",
            target_base(&target.base_url),
            runs[1].id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
    let repeated = migrate(&arguments, None).await.unwrap();
    assert_eq!(repeated.history_already_present.len(), 3);
    assert!(repeated.history_imported.is_empty());
}

#[tokio::test]
async fn target_domain_mismatch_never_commits_source_ownership() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.db");
    let storage = SqliteStorage::file(path.to_str().unwrap()).await.unwrap();
    let sequence = sequence("acme");
    let run = instance(&sequence, InstanceState::Waiting);
    storage.create_sequence(&sequence).await.unwrap();
    storage.create_instance(&run).await.unwrap();
    let output = BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: run.id,
        block_id: BlockId::new("welcome"),
        output: json!({"from":"source"}),
        output_ref: None,
        output_size: 17,
        attempt: 0,
        created_at: Utc::now(),
    };
    storage.save_block_output(&output).await.unwrap();
    let expected =
        MigrationDomainDigests::compute(&[], std::slice::from_ref(&output), &[], &[]).unwrap();
    let migration_id = uuid::Uuid::now_v7();
    let target = orch8_api::test_harness::spawn_test_server().await;
    target.storage.create_sequence(&sequence).await.unwrap();
    target.storage.create_instance(&TaskInstance {
        state: InstanceState::Paused,
        metadata: json!({MIGRATION_METADATA_KEY: {
            "id": migration_id, "phase": "importing", "domain_digests": expected, "domains_verified": false,
        }}),
        ..run.clone()
    }).await.unwrap();
    target
        .storage
        .save_block_output(&BlockOutput {
            output: json!({"from":"unexpected"}),
            ..output
        })
        .await
        .unwrap();
    let mut arguments = args(&target.base_url, &path);
    arguments.migration_id = Some(migration_id);
    let error = migrate(&arguments, None).await.unwrap_err().to_string();
    assert!(
        error.contains("stored migration domains do not match"),
        "{error}"
    );
    let source = storage.get_instance(run.id).await.unwrap().unwrap();
    assert_eq!(source.metadata[MIGRATION_METADATA_KEY]["phase"], "fenced");
    assert_eq!(source.state, InstanceState::Waiting);
    let target_owner = target
        .storage
        .get_continuity_execution_by_instance(&sequence.tenant_id, run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target_owner.state, OwnershipState::Transferring);
}

#[tokio::test]
async fn target_without_a_used_capability_leaves_source_unfenced() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.db");
    let source = orch8_storage::sqlite::SqliteStorage::file(path.to_str().unwrap())
        .await
        .unwrap();
    let seq = sequence("acme");
    let stateful = instance(&seq, InstanceState::Waiting);
    source.create_sequence(&seq).await.unwrap();
    source.create_instance(&stateful).await.unwrap();
    source
        .set_instance_kv(stateful.id, "balance", &json!(42))
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new()
        .route(
            "/api/v1/migrations/capabilities",
            axum::routing::get(|| async {
                let mut capabilities = MigrationCapabilities::current();
                capabilities.domains.remove(MIGRATION_KV_DOMAIN);
                axum::Json(capabilities)
            }),
        )
        .route(
            "/api/v1/migrations/import",
            axum::routing::post(
                |axum::Json(request): axum::Json<MigrationImportRequest>| async move {
                    assert!(request.instances.is_empty());
                    axum::Json(MigrationImportResponse {
                        migration_id: request.migration_id,
                        target_runtime_id: uuid::Uuid::now_v7(),
                        sequences: orch8_types::migration::SequenceImportCounts::default(),
                        instances: Vec::new(),
                    })
                },
            ),
        );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let report = migrate(&args(&url, &path), None).await.unwrap();
    server.abort();
    assert!(report.migrated.is_empty());
    assert_eq!(report.refused.len(), 1);
    assert_eq!(report.refused[0].instance_id, stateful.id.to_string());
    assert!(
        report.refused[0]
            .reason
            .contains("target cannot preserve migration domain instance_kv")
    );
    let untouched = source.get_instance(stateful.id).await.unwrap().unwrap();
    assert_eq!(untouched.state, InstanceState::Waiting);
    assert!(untouched.metadata.get(MIGRATION_METADATA_KEY).is_none());
    assert!(
        source
            .get_continuity_execution_by_instance(&seq.tenant_id, stateful.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn migrates_kv_versions_expiry_and_checkpoint_history_before_release() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.db");
    let source = orch8_storage::sqlite::SqliteStorage::file(path.to_str().unwrap())
        .await
        .unwrap();
    let seq = sequence("acme");
    let run = instance(&seq, InstanceState::Paused);
    source.create_sequence(&seq).await.unwrap();
    source.create_instance(&run).await.unwrap();
    let now = Utc::now();
    source
        .set_instance_kv_expiring(
            run.id,
            "balance",
            &json!(42),
            now + chrono::Duration::hours(1),
        )
        .await
        .unwrap();
    let rows = source
        .list_instance_kv_page(run.id, None, 10)
        .await
        .unwrap();
    let checkpoint_ids = [uuid::Uuid::now_v7(), uuid::Uuid::now_v7()];
    for (index, id) in checkpoint_ids.iter().enumerate() {
        source
            .save_checkpoint(&orch8_types::checkpoint::Checkpoint {
                id: *id,
                instance_id: run.id,
                checkpoint_data: json!({"sequence_number": index + 1}),
                created_at: now + chrono::Duration::seconds(i64::try_from(index).unwrap()),
            })
            .await
            .unwrap();
    }

    let target = orch8_api::test_harness::spawn_test_server().await;
    let arguments = args(&target.base_url, &path);
    let first = migrate(&arguments, None).await.unwrap();
    assert_eq!(first.migrated, vec![run.id.to_string()]);
    assert!(first.refused.is_empty());
    assert_eq!(
        target
            .storage
            .list_instance_kv_page(run.id, None, 10)
            .await
            .unwrap(),
        rows
    );
    assert_eq!(
        target
            .storage
            .get_instance_kv(run.id, "balance")
            .await
            .unwrap(),
        Some(json!(42))
    );
    let checkpoints = target.storage.list_checkpoints(run.id, 5).await.unwrap();
    assert_eq!(
        checkpoints.iter().map(|cp| cp.id).collect::<Vec<_>>(),
        [checkpoint_ids[1], checkpoint_ids[0]]
    );
    assert_eq!(checkpoints[0].checkpoint_data["sequence_number"], 2);
    assert_eq!(
        target
            .storage
            .get_latest_checkpoint(run.id)
            .await
            .unwrap()
            .unwrap()
            .id,
        checkpoint_ids[1]
    );
    assert_eq!(
        target
            .storage
            .get_continuity_execution_by_instance(&seq.tenant_id, run.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        OwnershipState::Owned,
    );
    let http = crate::build_client(None, Some("acme")).unwrap();
    let checkpoints_url = format!(
        "{}/instances/{}/checkpoints",
        target_base(&target.base_url),
        run.id
    );
    for (url, body) in [
        (
            checkpoints_url.clone(),
            json!({"checkpoint_data":{"sequence_number":3},"expected_epoch":0}),
        ),
        (
            format!("{checkpoints_url}/prune"),
            json!({"keep":0,"expected_epoch":0}),
        ),
    ] {
        let stale = http.post(url).json(&body).send().await.unwrap();
        assert_eq!(stale.status(), reqwest::StatusCode::CONFLICT);
    }
    assert_eq!(
        target
            .storage
            .list_checkpoints(run.id, 5)
            .await
            .unwrap()
            .len(),
        2
    );
    let accepted = http
        .post(checkpoints_url)
        .json(&json!({"checkpoint_data":{"sequence_number":3},"expected_epoch":1}))
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), reqwest::StatusCode::CREATED);

    let retry = migrate(&arguments, None).await.unwrap();
    assert_eq!(retry.already_migrated, vec![run.id.to_string()]);
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
    assert_eq!(moved.sub_tenant.as_deref(), Some("acme"));
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
async fn migrates_unknown_effect_beyond_legacy_receipt_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("embedded.db");
    let seq = sequence("acme");
    let waiting = instance(&seq, InstanceState::Waiting);
    let source = orch8_storage::sqlite::SqliteStorage::file(db.to_str().unwrap())
        .await
        .unwrap();
    source.create_sequence(&seq).await.unwrap();
    source.create_instance(&waiting).await.unwrap();
    let scope =
        orch8_engine::effect_guard::ensure_effect_scope(&source, &waiting.tenant_id, waiting.id)
            .await
            .unwrap();
    let now = Utc::now();
    let mut receipt = EffectReceipt {
        id: EffectId::new(),
        tenant_id: waiting.tenant_id.clone(),
        continuity_id: scope.continuity_id,
        epoch: scope.epoch,
        instance_id: waiting.id,
        block_id: BlockId::new("welcome"),
        kind: EffectKind::Http,
        state: EffectState::Committed,
        destination_fingerprint: "example".into(),
        idempotency_key: None,
        request_sha256: "a".repeat(64),
        provider_receipt_id: None,
        attempt: 0,
        created_at: now,
        updated_at: now,
    };
    for _ in 0..10_000 {
        receipt.id = EffectId::new();
        source.create_effect_receipt(&receipt).await.unwrap();
    }
    receipt.id = EffectId::new();
    receipt.state = EffectState::Unknown;
    receipt.created_at = now + chrono::Duration::seconds(1);
    receipt.updated_at = receipt.created_at;
    source.create_effect_receipt(&receipt).await.unwrap();
    drop(source);

    let target = orch8_api::test_harness::spawn_test_server().await;
    let report = migrate(&args(&target.base_url, &db), None).await.unwrap();
    assert_eq!(report.migrated, vec![waiting.id.to_string()]);
    let ledger = target
        .storage
        .summarize_effect_receipts(&waiting.tenant_id, scope.continuity_id)
        .await
        .unwrap();
    assert_eq!(ledger.unresolved_count, 1);
    assert_eq!(ledger.unknown_count, 1);
    assert_eq!(
        ledger.unresolved_sample.as_slice(),
        std::slice::from_ref(&receipt)
    );
    assert_eq!(ledger.receipt_count, 10_001);
    // The engine consults this lookup before allowing an external dispatch.
    assert_eq!(
        target
            .storage
            .find_unresolved_effect_receipt(
                &waiting.tenant_id,
                scope.continuity_id,
                waiting.id,
                &receipt.block_id,
                receipt.attempt,
            )
            .await
            .unwrap(),
        Some(receipt),
    );
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
            durable_state: MigratedInstanceState::default(),
            domain_digests: None,
            artifacts: Vec::new(),
            externalized_payloads: Vec::new(),
            continuity,
        }],
    };
    let client = crate::build_client(None, Some("acme")).unwrap();
    let base = target_base(&target.base_url);
    let tenant = TenantId::new("acme").unwrap();
    let destination = MigrationDestination {
        client: &client,
        base: &base,
        tenant: &tenant,
        chunked: true,
    };
    let first = destination.send(&request).await.unwrap();
    assert_eq!(
        first.instances[0].status,
        orch8_types::migration::ImportStatus::Imported
    );
    let second = destination.send(&request).await.unwrap();
    assert_eq!(
        second.instances[0].status,
        orch8_types::migration::ImportStatus::AlreadyPresent
    );
    assert_eq!(second.sequences.existing, 1);
    let stored = target.storage.get_instance(run.id).await.unwrap().unwrap();
    assert_eq!(stored.state, InstanceState::Scheduled);

    // A different tenant header cannot smuggle records in.
    let other = crate::build_client(None, Some("other")).unwrap();
    let other_destination = MigrationDestination {
        client: &other,
        ..destination
    };
    assert!(other_destination.send(&request).await.is_err());
}

#[tokio::test]
async fn chunked_migration_preserves_one_instance_larger_than_the_http_body_limit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.db");
    let source = SqliteStorage::file(path.to_str().unwrap()).await.unwrap();
    let seq = sequence("acme");
    source.create_sequence(&seq).await.unwrap();
    let run = instance(&seq, InstanceState::Waiting);
    source.create_instance(&run).await.unwrap();
    let payload = "x".repeat(17 * 1024 * 1024);
    source
        .save_block_output(&BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: run.id,
            block_id: BlockId::new("welcome"),
            output: json!({"payload":payload}),
            output_ref: None,
            output_size: 17 * 1024 * 1024,
            attempt: 0,
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    let target = orch8_api::test_harness::spawn_test_server().await;
    let report = migrate(&args(&target.base_url, &path), None).await.unwrap();
    assert_eq!(report.migrated, [run.id.to_string()]);
    let restored = target
        .storage
        .get_block_output(run.id, &BlockId::new("welcome"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.output["payload"], payload);
    let target_run = target.storage.get_instance(run.id).await.unwrap().unwrap();
    assert_eq!(target_run.state, InstanceState::Waiting);
    assert_eq!(
        source.get_instance(run.id).await.unwrap().unwrap().state,
        InstanceState::Paused
    );
}

#[tokio::test]
async fn staged_migration_checks_ordered_chunks_before_importing_and_isolates_tenants() {
    let target = orch8_api::test_harness::spawn_test_server().await;
    let client = reqwest::Client::new();
    let payload = serde_json::to_vec(&MigrationImportRequest {
        migration_id: uuid::Uuid::now_v7(),
        source_engine_id: "test".into(),
        sequences: vec![sequence("acme")],
        instances: Vec::new(),
    })
    .unwrap();
    let digest = orch8_types::plugin::WasmArtifactDigest::for_bytes(&payload);
    let path = format!(
        "{}/migrations/uploads/v1/{}",
        target.v1_url(),
        digest.as_str()
    );
    let commit = |tenant: &str| {
        client
            .post(format!("{path}/commit"))
            .header("X-Tenant-Id", tenant)
            .json(&json!({"tenant_id":tenant,"length":payload.len()}))
            .send()
    };
    let stage = |bytes: Vec<u8>| {
        client
            .put(format!("{path}/chunks/0?tenant_id=acme"))
            .header("X-Tenant-Id", "acme")
            .body(bytes)
            .send()
    };
    assert_eq!(
        commit("acme").await.unwrap().status(),
        reqwest::StatusCode::CONFLICT
    );
    for _ in 0..2 {
        assert_eq!(
            stage(payload.clone()).await.unwrap().status(),
            reqwest::StatusCode::NO_CONTENT
        );
    }
    assert_eq!(
        stage(b"invalid".to_vec()).await.unwrap().status(),
        reqwest::StatusCode::CONFLICT
    );
    assert_eq!(
        commit("other").await.unwrap().status(),
        reqwest::StatusCode::CONFLICT
    );
    let response = commit("acme").await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response
            .json::<MigrationImportResponse>()
            .await
            .unwrap()
            .sequences
            .created,
        1
    );
}

#[tokio::test]
async fn live_migration_keeps_artifact_references_in_externalized_outputs() {
    use orch8_storage::artifacts::ObjectArtifactStore;
    use orch8_storage::externalizing::ExternalizedEntry;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.db");
    let artifacts_dir = directory.path().join("artifacts");
    let store = Arc::new(ObjectArtifactStore::local(artifacts_dir.to_str().unwrap()).unwrap());
    let source = SqliteStorage::file(path.to_str().unwrap())
        .await
        .unwrap()
        .with_artifact_store(store);
    let seq = sequence("acme");
    source.create_sequence(&seq).await.unwrap();
    let run = instance(&seq, InstanceState::Waiting);
    source.create_instance(&run).await.unwrap();
    let bytes = vec![0x87; MIGRATION_CHUNK_BYTES + 13];
    let artifact = source
        .put_artifact(run.id, "application/octet-stream", bytes.clone().into())
        .await
        .unwrap();
    let output_id = uuid::Uuid::now_v7();
    let ref_key = format!("{}:welcome:{output_id}", run.id);
    let externalized = ExternalizedEntry {
        instance_id: run.id,
        ref_key: ref_key.clone(),
        payload: artifact.clone().into_output(),
        created_at: Utc::now(),
        expires_at: None,
    };
    source
        .restore_externalized_entry(&externalized)
        .await
        .unwrap();
    source
        .save_block_output(&BlockOutput {
            id: output_id,
            instance_id: run.id,
            block_id: BlockId::new("welcome"),
            output: orch8_storage::externalizing::marker(&ref_key),
            output_ref: Some(ref_key.clone()),
            output_size: 512,
            attempt: 0,
            created_at: Utc::now(),
        })
        .await
        .unwrap();

    let target = orch8_api::test_harness::spawn_test_server_with_artifacts().await;
    let mut arguments = args(&target.base_url, &path);
    let refusal = migrate(&arguments, None).await.unwrap();
    assert_eq!(refusal.refused.len(), 1);
    assert!(refusal.refused[0].reason.contains("--source-artifacts-dir"));
    assert!(target.storage.get_instance(run.id).await.unwrap().is_none());

    let hash = orch8_types::plugin::WasmArtifactDigest::for_bytes(&bytes);
    let client = crate::build_client(None, Some("acme")).unwrap();
    let base = target_base(&target.base_url);
    let tenant = TenantId::new("acme").unwrap();
    let destination = MigrationDestination {
        client: &client,
        base: &base,
        tenant: &tenant,
        chunked: true,
    };
    destination
        .stage_artifact(hash.as_str(), &bytes)
        .await
        .unwrap();
    let verification = client
        .get(format!(
            "{base}/migrations/uploads/v1/{}/verify",
            hash.as_str()
        ))
        .query(&MigrationUploadCommit {
            tenant_id: tenant.clone(),
            length: bytes.len(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(verification.status(), reqwest::StatusCode::NO_CONTENT);

    arguments.source_artifacts_dir = Some(artifacts_dir);
    let report = migrate(&arguments, None).await.unwrap();
    assert_eq!(report.migrated, [run.id.to_string()]);
    assert_eq!(
        target
            .storage
            .get_externalized_state(run.id, &ref_key)
            .await
            .unwrap(),
        Some(externalized.payload)
    );
    assert_eq!(
        target.storage.get_artifact(&artifact.key).await.unwrap(),
        Some(bytes)
    );
}

#[tokio::test]
async fn migration_delivers_pre_cutover_and_in_window_signals_once_and_moves_intake() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.db");
    let source = Arc::new(SqliteStorage::file(path.to_str().unwrap()).await.unwrap());
    let seq = sequence("acme");
    source.create_sequence(&seq).await.unwrap();
    let run = instance(&seq, InstanceState::Waiting);
    source.create_instance(&run).await.unwrap();
    let source_api = orch8_api::test_harness::spawn_test_server_on(source.clone(), None).await;
    let target = orch8_api::test_harness::spawn_test_server().await;
    let client = reqwest::Client::new();
    let signal = |base: &str, value| {
        client
            .post(format!("{base}/instances/{}/signals", run.id))
            .header("X-Tenant-Id", "acme")
            .json(&json!({"signal_type":SignalType::Custom("migration-check".into()), "payload":{"value":value}}))
            .send()
    };

    let before = signal(&source_api.v1_url(), "before").await.unwrap();
    assert!(
        before.status().is_success(),
        "{}",
        before.text().await.unwrap()
    );
    let owner =
        orch8_engine::effect_guard::ensure_effect_scope(source.as_ref(), &run.tenant_id, run.id)
            .await
            .unwrap();
    let mut arguments = args(&target.base_url, &path);
    let id = uuid::Uuid::now_v7();
    arguments.migration_id = Some(id);
    source
        .fence_for_cloud_migration(&owner, &id.to_string())
        .await
        .unwrap()
        .unwrap();
    let during = signal(&source_api.v1_url(), "during").await.unwrap();
    assert!(
        during.status().is_success(),
        "{}",
        during.text().await.unwrap()
    );
    let accepted_ids: Vec<_> = source
        .get_pending_signals(run.id)
        .await
        .unwrap()
        .into_iter()
        .map(|signal| signal.id)
        .collect();

    let report = migrate(&arguments, None).await.unwrap();
    assert_eq!(report.migrated, [run.id.to_string()]);
    assert_eq!(
        signal(&source_api.v1_url(), "wrong endpoint")
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    let after = signal(&target.v1_url(), "after").await.unwrap();
    assert!(
        after.status().is_success(),
        "{}",
        after.text().await.unwrap()
    );

    let repeated = migrate(&arguments, None).await.unwrap();
    assert_eq!(repeated.already_migrated, [run.id.to_string()]);
    let inbox = target.storage.get_pending_signals(run.id).await.unwrap();
    assert_eq!(inbox.len(), 3);
    assert_eq!(
        inbox
            .iter()
            .map(|entry| entry.payload["value"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["before", "during", "after"]
    );
    assert_eq!(
        inbox
            .iter()
            .take(2)
            .map(|entry| entry.id)
            .collect::<Vec<_>>(),
        accepted_ids
    );
    for entry in inbox {
        target
            .storage
            .mark_signal_delivered(entry.id)
            .await
            .unwrap();
    }
    assert!(
        target
            .storage
            .get_pending_signals(run.id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[test]
fn migration_batches_measure_the_encoded_wire_length() {
    let value = json!({"data":"a\n\\\"β"});
    assert_eq!(
        count_encoded_bytes(&value).unwrap(),
        serde_json::to_vec(&value).unwrap().len()
    );
    assert!(count_encoded_bytes(&value).unwrap() > value["data"].as_str().unwrap().len());
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Covers the upload, lost commit reply, and both receipt outcomes.
async fn chunked_import_recovers_only_a_matching_receipt_after_a_lost_response() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let sequence = sequence("acme");
    let run = instance(&sequence, InstanceState::Waiting);
    let owner = ContinuityExecution {
        continuity_id: orch8_types::continuity::ContinuityId::new(),
        current_instance_id: run.id,
        owner_runtime_id: RuntimeId::new(),
        tenant_id: run.tenant_id.clone(),
        epoch: orch8_types::continuity::ExecutionEpoch::initial(),
        state: OwnershipState::Transferring,
        updated_at: run.updated_at,
    };
    let request = MigrationImportRequest {
        migration_id: uuid::Uuid::new_v4(),
        source_engine_id: "fixture".into(),
        sequences: vec![sequence],
        instances: vec![
            serde_json::from_value(json!({ "instance": run, "continuity": owner })).unwrap(),
        ],
    };
    let payload = serde_json::to_vec(&request).unwrap();
    let tenant = TenantId::new("acme").unwrap();
    let receipt = StagedMigrationImportReceipt {
        tenant_id: tenant.clone(),
        digest: orch8_types::plugin::WasmArtifactDigest::for_bytes(&payload)
            .as_str()
            .to_owned(),
        length: payload.len(),
        completed_at: Utc::now(),
        result: MigrationImportResponse {
            migration_id: request.migration_id,
            target_runtime_id: target_runtime_id(request.migration_id),
            sequences: orch8_types::migration::SequenceImportCounts {
                created: 1,
                existing: 0,
            },
            instances: vec![orch8_types::migration::ImportedInstance {
                instance_id: run.id,
                status: ImportStatus::Imported,
                epoch: owner.epoch.checked_next().unwrap().get(),
                domain_digests: None,
            }],
        },
    };
    let commits = Arc::new(AtomicUsize::new(0));
    let commit_calls = commits.clone();
    let lookups = Arc::new(AtomicUsize::new(0));
    let receipt_calls = lookups.clone();
    let expected_epoch = receipt.result.instances[0].epoch;
    let digest = receipt.digest.clone();
    let expected_length = receipt.length;
    let router =
        axum::Router::new()
            .route(
                "/migrations/uploads/v1/{digest}/chunks/{index}",
                axum::routing::put(|| async { axum::http::StatusCode::NO_CONTENT }),
            )
            .route(
                "/migrations/uploads/v1/{digest}/commit",
                axum::routing::post(move || {
                    let calls = commit_calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        // Simulate a gateway losing the reply after the target already saved a receipt.
                        axum::http::StatusCode::BAD_GATEWAY
                    }
                }),
            )
            .route(
                "/migrations/uploads/v1/{digest}/receipt",
                axum::routing::get(
                    move |axum::extract::Path(path): axum::extract::Path<String>,
                          axum::extract::Query(upload): axum::extract::Query<
                        MigrationUploadCommit,
                    >| {
                        let mut result = receipt.clone();
                        let calls = receipt_calls.clone();
                        async move {
                            assert_eq!(path, result.digest);
                            assert_eq!(upload.tenant_id, result.tenant_id);
                            assert_eq!(upload.length, result.length);
                            if calls.fetch_add(1, Ordering::SeqCst) > 0 {
                                result.result.instances[0].epoch += 1;
                            }
                            axum::Json(result)
                        }
                    },
                ),
            );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = reqwest::Client::new();
    let destination = MigrationDestination {
        client: &client,
        base: &base,
        tenant: &tenant,
        chunked: true,
    };
    let first = destination.send_chunks(&request).await.unwrap();
    assert_eq!(first.instances[0].epoch, expected_epoch);
    assert_eq!(commits.load(Ordering::SeqCst), 1);
    assert_eq!(lookups.load(Ordering::SeqCst), 1);

    let error = destination
        .send_chunks(&request)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("saved import receipt does not match the submitted batch"));
    assert!(error.contains(&digest));
    assert!(error.contains(&format!("{expected_length} bytes")));
    assert_eq!(commits.load(Ordering::SeqCst), 2);
    assert_eq!(lookups.load(Ordering::SeqCst), 2);
    server.abort();
}

#[tokio::test]
async fn staged_only_migration_keeps_the_source_fenced_until_target_import() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.db");
    let storage = SqliteStorage::file(path.to_str().unwrap()).await.unwrap();
    let sequence = sequence("acme");
    storage.create_sequence(&sequence).await.unwrap();
    let run = instance(&sequence, InstanceState::Waiting);
    storage.create_instance(&run).await.unwrap();

    let target = orch8_api::test_harness::spawn_test_server().await;
    let mut arguments = args(&target.base_url, &path);
    arguments.stage_only = true;
    let staged = migrate(&arguments, None).await.unwrap();
    assert!(staged.staging_error.is_none(), "{:?}", staged.staging_error);
    assert!(staged.refused.is_empty());
    assert!(staged.migrated.is_empty());
    assert_eq!(staged.sequences_sent, 1);
    assert_eq!(staged.sequences_created, 0);
    assert_eq!(staged.staged_uploads.len(), 2);
    assert_eq!(staged.staged_uploads[0].sequences, 1);
    assert!(staged.staged_uploads[0].execution_ids.is_empty());
    assert_eq!(staged.staged_uploads[1].sequences, 0);
    assert_eq!(staged.staged_uploads[1].execution_ids, vec![run.id]);
    assert!(
        target
            .storage
            .get_sequence(sequence.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(target.storage.get_instance(run.id).await.unwrap().is_none());

    let fenced = storage.get_instance(run.id).await.unwrap().unwrap();
    assert_eq!(
        phase(&fenced, &staged.migration_id).as_deref(),
        Some("fenced")
    );
    assert_eq!(
        storage
            .get_continuity_execution_by_instance(&run.tenant_id, run.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        OwnershipState::Transferring
    );

    let repeated = migrate(&arguments, None).await.unwrap();
    assert!(repeated.staging_error.is_none());
    assert_eq!(
        repeated
            .staged_uploads
            .iter()
            .map(|upload| (&upload.digest, upload.length))
            .collect::<Vec<_>>(),
        staged
            .staged_uploads
            .iter()
            .map(|upload| (&upload.digest, upload.length))
            .collect::<Vec<_>>()
    );

    let client = crate::build_client(None, Some("acme")).unwrap();
    for upload in &staged.staged_uploads {
        let endpoint = format!("/migrations/uploads/v1/{}/commit", upload.digest);
        let _: MigrationImportResponse = send_json(
            &client,
            &target.v1_url(),
            &endpoint,
            &MigrationUploadCommit {
                tenant_id: run.tenant_id.clone(),
                length: upload.length,
            },
        )
        .await
        .unwrap();
    }
    assert!(target.storage.get_instance(run.id).await.unwrap().is_some());
    assert_eq!(
        phase(
            &storage.get_instance(run.id).await.unwrap().unwrap(),
            &staged.migration_id
        )
        .as_deref(),
        Some("fenced")
    );

    arguments.stage_only = false;
    let finalized = migrate(&arguments, None).await.unwrap();
    assert!(finalized.refused.is_empty());
    assert_eq!(finalized.migrated, vec![run.id.to_string()]);
    let source = storage.get_instance(run.id).await.unwrap().unwrap();
    assert_eq!(source.state, InstanceState::Paused);
    assert_eq!(
        phase(&source, &staged.migration_id).as_deref(),
        Some("committed")
    );
}

#[tokio::test]
async fn stage_only_keeps_verified_coordinates_when_a_later_batch_fails() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.db");
    let storage = SqliteStorage::file(path.to_str().unwrap()).await.unwrap();
    let sequence = sequence("acme");
    storage.create_sequence(&sequence).await.unwrap();
    let run = instance(&sequence, InstanceState::Waiting);
    storage.create_instance(&run).await.unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let uploads = count.clone();
    let app = axum::Router::new()
        .route(
            "/api/v1/migrations/capabilities",
            axum::routing::get(|| async { axum::Json(MigrationCapabilities::current()) }),
        )
        .route(
            "/api/v1/migrations/uploads/v1/{digest}/chunks/{index}",
            axum::routing::put(move || {
                let uploads = uploads.clone();
                async move {
                    if uploads.fetch_add(1, Ordering::SeqCst) == 0 {
                        axum::http::StatusCode::NO_CONTENT
                    } else {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        )
        .route(
            "/api/v1/migrations/uploads/v1/{digest}/verify",
            axum::routing::get(|| async { axum::http::StatusCode::NO_CONTENT }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut arguments = args(&base, &path);
    arguments.stage_only = true;
    let report = migrate(&arguments, None).await.unwrap();
    assert!(report.staging_error.unwrap().contains("503"));
    assert_eq!(report.staged_uploads.len(), 1);
    assert_eq!(report.staged_uploads[0].sequences, 1);
    assert_eq!(count.load(Ordering::SeqCst), 2);
    let source = storage.get_instance(run.id).await.unwrap().unwrap();
    assert_eq!(
        phase(&source, &report.migration_id).as_deref(),
        Some("fenced")
    );
    server.abort();
}
