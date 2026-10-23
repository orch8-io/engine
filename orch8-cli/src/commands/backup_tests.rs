//! Backup / restore round trips on `SQLite` (the `PostgreSQL` path shares
//! every function except `open_source` / `open_target`).
#![allow(clippy::too_many_lines)]

use super::*;
use chrono::Utc;
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{
    AdminStore as _, ExecutionTreeStore as _, InstanceStore as _, OutputStore as _,
    ResourceStore as _, SchedulingStore as _, SequenceStore as _, WorkerStore as _,
};
use orch8_types::config::SecretString;
use orch8_types::context::ExecutionContext;
use orch8_types::execution::{BlockType, NodeState};
use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority};
use std::path::Path;

async fn seeded_source(dir: &Path) -> (PathBuf, SequenceId, InstanceId) {
    let path = dir.join("source.db");
    let storage = SqliteStorage::file(path.to_str().unwrap()).await.unwrap();
    let now = Utc::now();
    let seq_id = SequenceId::new();
    let seq: SequenceDefinition = serde_json::from_value(serde_json::json!({
        "id": seq_id,
        "tenant_id": "acme",
        "namespace": "default",
        "name": "billing",
        "version": 1,
        "blocks": [{"type": "step", "id": "charge", "handler": "noop"}],
        "created_at": now,
    }))
    .unwrap();
    storage.create_sequence(&seq).await.unwrap();

    storage
        .create_credential(&CredentialDef {
            id: "stripe".into(),
            tenant_id: "acme".into(),
            name: "Stripe".into(),
            kind: orch8_types::credential::CredentialKind::default(),
            value: SecretString::from("enc:v1:c2VjcmV0".to_string()),
            expires_at: None,
            refresh_url: None,
            refresh_token: Some(SecretString::from("refresh-me".to_string())),
            enabled: true,
            description: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    storage
        .create_trigger(&TriggerDef {
            slug: "orders".into(),
            sequence_name: "billing".into(),
            version: None,
            tenant_id: TenantId::unchecked("acme"),
            namespace: "default".into(),
            enabled: true,
            secret: Some(SecretString::from("hmac-secret".to_string())),
            trigger_type: orch8_types::trigger::TriggerType::default(),
            config: serde_json::json!({}),
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    storage
        .create_cron_schedule(&CronSchedule {
            id: uuid::Uuid::now_v7(),
            tenant_id: TenantId::unchecked("acme"),
            namespace: Namespace::new("default"),
            sequence_id: seq_id,
            cron_expr: "0 9 * * 1-5".into(),
            timezone: "UTC".into(),
            enabled: true,
            metadata: serde_json::json!({}),
            overlap_policy: orch8_types::cron::OverlapPolicy::default(),
            skipped_fires: 0,
            last_skipped_at: None,
            last_triggered_at: None,
            next_fire_at: Some(now),
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    storage
        .create_queue_routing_rule(&QueueRoutingRule {
            id: uuid::Uuid::now_v7(),
            tenant_id: "acme".into(),
            handler_name: "charge_card".into(),
            match_queue: None,
            queue_override: "payments".into(),
            priority: 5,
            enabled: true,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();

    let instance_id = InstanceId::new();
    storage
        .create_instance(&TaskInstance {
            claimed_at: None,
            sub_tenant: None,
            id: instance_id,
            sequence_id: seq_id,
            tenant_id: TenantId::unchecked("acme"),
            namespace: Namespace::new("default"),
            state: InstanceState::Completed,
            next_fire_at: None,
            priority: Priority::default(),
            timezone: "UTC".into(),
            metadata: serde_json::json!({"order": 42}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    storage
        .create_execution_nodes_batch(&[ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id,
            block_id: BlockId::new("charge"),
            parent_id: None,
            block_type: BlockType::Step,
            branch_index: None,
            state: NodeState::Completed,
            started_at: Some(now),
            completed_at: Some(now),
        }])
        .await
        .unwrap();
    storage
        .save_block_output(&BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id,
            block_id: BlockId::new("charge"),
            output: serde_json::json!({"charged": true}),
            output_ref: None,
            output_size: 16,
            attempt: 1,
            created_at: now,
        })
        .await
        .unwrap();
    (path, seq_id, instance_id)
}

#[tokio::test]
async fn backup_restore_round_trip_is_complete_and_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let (source, seq_id, instance_id) = seeded_source(dir.path()).await;

    let out = dir.path().join("backup.tar.gz");
    run_backup(
        BackupCmd {
            database_url: format!("sqlite:{}", source.display()),
            out: out.clone(),
            include_instances: true,
        },
        OutputFormat::Json,
    )
    .await
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(
            std::fs::metadata(&out).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    let archive = read_archive(&std::fs::read(&out).unwrap()).unwrap();
    assert_eq!(archive.manifest.format, FORMAT);
    assert_eq!(archive.manifest.format_version, FORMAT_VERSION);
    assert!(archive.manifest.include_instances);
    let count = |name: &str| {
        archive
            .manifest
            .files
            .iter()
            .find(|f| f.name == name)
            .unwrap()
            .records
    };
    for name in [
        "sequences.jsonl",
        "credentials.jsonl",
        "triggers.jsonl",
        "cron.jsonl",
        "queue_routing.jsonl",
        "instances.jsonl",
        "execution_tree.jsonl",
        "block_outputs.jsonl",
    ] {
        assert_eq!(count(name), 1, "{name}");
    }
    assert!(
        archive
            .manifest
            .warnings
            .iter()
            .any(|w| w.contains("ORCH8_ENCRYPTION_KEY"))
    );
    // Secrets are exported as stored, not redacted.
    let credentials = String::from_utf8(archive.members["credentials.jsonl"].clone()).unwrap();
    assert!(credentials.contains("enc:v1:c2VjcmV0"), "{credentials}");
    assert!(credentials.contains("refresh-me"));
    let triggers = String::from_utf8(archive.members["triggers.jsonl"].clone()).unwrap();
    assert!(triggers.contains("hmac-secret"));

    // Dry run into a fresh database writes nothing.
    let target = dir.path().join("target.db");
    let storage = open_target(&DbTarget::Sqlite(target.clone()))
        .await
        .unwrap();
    let dry = restore(&archive, storage.as_ref(), true, true)
        .await
        .unwrap();
    assert_eq!(dry.counts["sequences"].created, 1);
    assert!(storage.get_sequence(seq_id).await.unwrap().is_none());

    // Real restore creates everything …
    let report = restore(&archive, storage.as_ref(), false, true)
        .await
        .unwrap();
    for kind in [
        "sequences",
        "credentials",
        "triggers",
        "cron",
        "queue_routing",
        "instances",
        "execution_tree",
        "block_outputs",
    ] {
        assert_eq!(
            report.counts[kind],
            RestoreCount {
                total: 1,
                created: 1,
                existing: 0
            },
            "{kind}"
        );
    }
    let tenant = TenantId::unchecked("acme");
    let credential = storage
        .get_credential(Some(&tenant), "stripe")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(credential.value.expose(), "enc:v1:c2VjcmV0");
    let trigger = storage
        .get_trigger(Some(&tenant), "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(trigger.secret.unwrap().expose(), "hmac-secret");
    assert_eq!(
        storage
            .get_instance(instance_id)
            .await
            .unwrap()
            .unwrap()
            .metadata,
        serde_json::json!({"order": 42})
    );
    assert_eq!(storage.get_all_outputs(instance_id).await.unwrap().len(), 1);
    assert_eq!(
        storage.get_execution_tree(instance_id).await.unwrap().len(),
        1
    );

    // … and a second restore is a no-op.
    let again = restore(&archive, storage.as_ref(), false, true)
        .await
        .unwrap();
    assert!(
        again
            .counts
            .values()
            .all(|c| c.created == 0 && c.existing == c.total)
    );
    assert_eq!(storage.get_all_outputs(instance_id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn instances_are_opt_in_on_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let (source, _, instance_id) = seeded_source(dir.path()).await;
    let storage = SqliteStorage::file(source.to_str().unwrap()).await.unwrap();
    let snap = snapshot(&storage, false).await.unwrap();
    assert!(snap.instances.is_none());
    let archive = read_archive(&write_archive(&snap, "sqlite").unwrap()).unwrap();
    assert!(!archive.members.contains_key("instances.jsonl"));

    let with =
        read_archive(&write_archive(&snapshot(&storage, true).await.unwrap(), "sqlite").unwrap())
            .unwrap();
    let target = open_target(&DbTarget::Sqlite(dir.path().join("t.db")))
        .await
        .unwrap();
    let report = restore(&with, target.as_ref(), false, false).await.unwrap();
    assert!(!report.counts.contains_key("instances"));
    assert!(
        report
            .warnings
            .iter()
            .any(|w| w.contains("--include-instances"))
    );
    assert!(target.get_instance(instance_id).await.unwrap().is_none());
}

fn repack_archive_members(members: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut builder = tar::Builder::new(encoder);
    for (name, data) in members {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o600);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_cksum();
        builder
            .append_data(&mut header, name, data.as_slice())
            .unwrap();
    }
    builder.into_inner().unwrap().finish().unwrap()
}

#[test]
fn tampered_or_foreign_archives_are_rejected() {
    let snap = Snapshot {
        sequences: vec![serde_json::json!({"a": 1})],
        ..Snapshot::default()
    };
    let good = write_archive(&snap, "sqlite").unwrap();
    assert!(read_archive(&good).is_ok());

    // Re-pack with a modified member but the original manifest.
    let archive = read_archive(&good).unwrap();
    let rebuild = repack_archive_members;
    let manifest = serde_json::to_vec(&archive.manifest).unwrap();
    let mut members: Vec<(&str, Vec<u8>)> = vec![("manifest.json", manifest.clone())];
    for (name, body) in &archive.members {
        members.push((name.as_str(), body.clone()));
    }
    let tampered: Vec<(&str, Vec<u8>)> = members
        .iter()
        .map(|(name, body)| {
            if *name == "sequences.jsonl" {
                (*name, b"{\"a\":2}\n".to_vec())
            } else {
                (*name, body.clone())
            }
        })
        .collect();
    let err = read_archive(&rebuild(&tampered)).unwrap_err().to_string();
    assert!(err.contains("checksum mismatch"), "{err}");

    let mut extra = members.clone();
    extra.push(("evil.jsonl", b"{}\n".to_vec()));
    let err = read_archive(&rebuild(&extra)).unwrap_err().to_string();
    assert!(err.contains("unexpected archive member"), "{err}");

    let mut newer = archive.manifest.clone();
    newer.format_version = FORMAT_VERSION + 1;
    let mut future = members.clone();
    future[0] = ("manifest.json", serde_json::to_vec(&newer).unwrap());
    let err = read_archive(&rebuild(&future)).unwrap_err().to_string();
    assert!(err.contains("newer than this binary"), "{err}");

    assert!(read_archive(b"not a tarball").is_err());
}

struct StopSnapshotWriter(Arc<std::sync::atomic::AtomicBool>);

impl Drop for StopSnapshotWriter {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

/// A backup must contain every transaction committed before it started, and
/// must not combine half of two transactions while the live WAL checkpoints.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_sqlite_snapshot_preserves_atomic_commits_during_checkpoints() {
    use sqlx::Connection as _;
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

    let dir = tempfile::tempdir().unwrap();
    let (path, sequence_id, _) = seeded_source(dir.path()).await;
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&path)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_millis(200));
    let mut writer = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    sqlx::query("PRAGMA wal_autocheckpoint=0")
        .execute(&mut writer)
        .await
        .unwrap();
    sqlx::query("UPDATE credentials SET description='1' WHERE id='stripe'")
        .execute(&mut writer)
        .await
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_on_drop = StopSnapshotWriter(Arc::clone(&stop));
    let committed = Arc::new(AtomicI32::new(1));
    let actor_committed = Arc::clone(&committed);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let actor = tokio::spawn(async move {
        let mut epoch = 2i32;
        let mut started = Some(started_tx);
        while !stop.load(Ordering::Acquire) {
            let mut tx = writer.begin().await?;
            sqlx::query("UPDATE sequences SET version=? WHERE id=?")
                .bind(epoch)
                .bind(sequence_id.to_string())
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE credentials SET description=? WHERE id='stripe'")
                .bind(epoch.to_string())
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            actor_committed.store(epoch, Ordering::Release);
            if let Some(started) = started.take() {
                let _ = started.send(());
            }
            let _: (i64, i64, i64) = sqlx::query_as("PRAGMA wal_checkpoint(TRUNCATE)")
                .fetch_one(&mut writer)
                .await?;
            epoch = epoch.saturating_add(1);
            tokio::task::yield_now().await;
        }
        Ok::<_, sqlx::Error>(())
    });
    started_rx.await.unwrap();
    let tenant = TenantId::unchecked("acme");
    for _ in 0..12 {
        let floor = committed.load(Ordering::Acquire);
        let (snapshot, _guard) = open_source(&DbTarget::Sqlite(path.clone())).await.unwrap();
        let sequence = snapshot.get_sequence(sequence_id).await.unwrap().unwrap();
        let credential = snapshot
            .get_credential(Some(&tenant), "stripe")
            .await
            .unwrap()
            .unwrap();
        let credential_epoch: i32 = credential.description.unwrap().parse().unwrap();
        assert_eq!(
            sequence.version, credential_epoch,
            "a snapshot must not tear a transaction"
        );
        assert!(
            sequence.version >= floor,
            "a snapshot lost a transaction committed before backup began"
        );
    }
    drop(stop_on_drop);
    actor.await.unwrap().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn sqlite_snapshot_can_read_a_readonly_source_without_changing_it() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::Builder::new()
        .prefix("backup's-")
        .tempdir()
        .unwrap();
    let (path, sequence_id, _) = seeded_source(dir.path()).await;
    // Wait for the source pool's asynchronous close and checkpoint before
    // recording the original bytes; this raw connection performs no migrations.
    let options = sqlx::sqlite::SqliteConnectOptions::new().filename(&path);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
    let before = std::fs::read(&path).unwrap();
    let (snapshot, _guard) = open_source(&DbTarget::Sqlite(path.clone())).await.unwrap();
    assert_eq!(
        snapshot
            .get_sequence(sequence_id)
            .await
            .unwrap()
            .unwrap()
            .name,
        "billing"
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "backup must not reconcile or rewrite the source"
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o444
    );
}

#[tokio::test]
async fn backup_catalog_keeps_credentials_for_tenants_without_sequences() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let now = Utc::now();
    for index in 0..1001 {
        storage
            .create_credential(&CredentialDef {
                id: format!("credential-{index:04}"),
                tenant_id: format!("tenant-{}", index % 2),
                name: format!("Credential {index}"),
                kind: orch8_types::credential::CredentialKind::default(),
                value: SecretString::from(format!("secret-{index}")),
                expires_at: None,
                refresh_url: None,
                refresh_token: Some(SecretString::from(format!("refresh-{index}"))),
                enabled: index % 2 == 0,
                description: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
    }
    let snap = snapshot(&storage, false).await.unwrap();
    assert!(snap.sequences.is_empty());
    assert_eq!(
        snap.credentials.len(),
        1001,
        "a public listing cap must not omit credentials from a successful backup"
    );
    let archive = read_archive(&write_archive(&snap, "sqlite").unwrap()).unwrap();
    let restored = SqliteStorage::in_memory().await.unwrap();
    restore(&archive, &restored, false, false).await.unwrap();
    let credential = restored
        .get_credential(Some(&TenantId::unchecked("tenant-0")), "credential-1000")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(credential.value.expose(), "secret-1000");
    assert_eq!(credential.refresh_token.unwrap().expose(), "refresh-1000");
}

#[tokio::test]
async fn backup_catalog_keeps_triggers_for_tenants_without_sequences() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let now = Utc::now();
    for index in 0..1001 {
        storage
            .create_trigger(&TriggerDef {
                slug: format!("trigger-{index:04}"),
                sequence_name: "future-sequence".into(),
                version: None,
                tenant_id: TenantId::unchecked(format!("tenant-{}", index % 2)),
                namespace: "default".into(),
                enabled: false,
                secret: Some(SecretString::from(format!("hmac-{index}"))),
                trigger_type: orch8_types::trigger::TriggerType::default(),
                config: serde_json::json!({}),
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
    }
    let snap = snapshot(&storage, false).await.unwrap();
    assert!(snap.sequences.is_empty());
    assert_eq!(
        snap.triggers.len(),
        1001,
        "disabled/preprovisioned triggers must survive a backup without their sequence"
    );
    let archive = read_archive(&write_archive(&snap, "sqlite").unwrap()).unwrap();
    let restored = SqliteStorage::in_memory().await.unwrap();
    restore(&archive, &restored, false, false).await.unwrap();
    let trigger = restored
        .get_trigger(Some(&TenantId::unchecked("tenant-0")), "trigger-1000")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(trigger.secret.unwrap().expose(), "hmac-1000");
}

#[tokio::test]
async fn backup_catalog_keeps_more_than_one_page_of_cron_in_one_tenant() {
    let dir = tempfile::tempdir().unwrap();
    let (source, _, _) = seeded_source(dir.path()).await;
    let storage = SqliteStorage::file(source.to_str().unwrap()).await.unwrap();
    let template = storage
        .list_cron_schedules(None, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    for _ in 0..1000 {
        let mut schedule = template.clone();
        schedule.id = uuid::Uuid::now_v7();
        storage.create_cron_schedule(&schedule).await.unwrap();
    }
    let snap = snapshot(&storage, false)
        .await
        .expect("a valid catalog larger than the API cap must remain exportable");
    assert_eq!(snap.cron.len(), 1001);
}

async fn restore_including_payloads(
    source: &Path,
    target: &Path,
) -> std::sync::Arc<dyn StorageBackend> {
    let archive_path = source.with_extension("tar.gz");
    run_backup(
        BackupCmd {
            database_url: format!("sqlite:{}", source.display()),
            out: archive_path.clone(),
            include_instances: true,
        },
        OutputFormat::Json,
    )
    .await
    .unwrap();
    let archive = read_archive(&std::fs::read(archive_path).unwrap()).unwrap();
    let storage = open_target(&DbTarget::Sqlite(target.to_owned()))
        .await
        .unwrap();
    restore(&archive, storage.as_ref(), false, true)
        .await
        .unwrap();
    storage
}

#[tokio::test]
async fn backup_restore_preserves_externalized_context_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let (source, _, id) = seeded_source(dir.path()).await;
    let storage = SqliteStorage::file(source.to_str().unwrap()).await.unwrap();
    let payload = serde_json::json!({"invoice": "archived content", "items": [1,2,3]});
    let context = ExecutionContext {
        data: serde_json::json!({"payload": payload}),
        ..ExecutionContext::default()
    };
    storage
        .update_instance_context_externalized(id, &context, 1)
        .await
        .unwrap();
    assert!(orch8_engine::externalized::is_ref_marker(
        &storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .context
            .data["payload"]
    ));
    let target = restore_including_payloads(&source, &dir.path().join("restored.db")).await;
    let restored = target.get_instance(id).await.unwrap().unwrap();
    let hydrated =
        orch8_engine::externalized::resolve_context_markers(target.as_ref(), id, restored.context)
            .await
            .unwrap();
    assert_eq!(
        hydrated.data["payload"], payload,
        "restored context must retain its payload, not a dangling marker"
    );
}

#[tokio::test]
async fn backup_restore_preserves_every_externalized_output_payload() {
    let dir = tempfile::tempdir().unwrap();
    let (source, _, id) = seeded_source(dir.path()).await;
    let storage = SqliteStorage::file(source.to_str().unwrap()).await.unwrap();
    let mut history = Vec::new();
    for value in ["first historical payload", "second historical payload"] {
        let output_id = uuid::Uuid::now_v7();
        let reference = format!("{id}:charge:{output_id}");
        let payload = serde_json::json!({"value": value});
        storage
            .save_externalized_state(id, &reference, &payload)
            .await
            .unwrap();
        storage
            .save_block_output(&BlockOutput {
                id: output_id,
                instance_id: id,
                block_id: BlockId::new("charge"),
                output: orch8_storage::externalizing::marker(&reference),
                output_ref: Some(reference),
                output_size: u32::try_from(serde_json::to_vec(&payload).unwrap().len()).unwrap(),
                attempt: 0,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
        history.push((output_id, payload));
    }
    let target = restore_including_payloads(&source, &dir.path().join("restored.db")).await;
    let outputs = target.get_all_outputs(id).await.unwrap();
    for (output_id, expected) in history {
        let output = outputs
            .iter()
            .find(|output| output.id == output_id)
            .unwrap();
        let actual = if let Some(reference) = &output.output_ref {
            target
                .get_externalized_state(id, reference)
                .await
                .unwrap()
                .expect("restored historical output must not refer to a missing payload")
        } else {
            output.output.clone()
        };
        assert_eq!(actual, expected);
    }
}

#[tokio::test]
async fn encrypted_payload_archive_preserves_ciphertexts_and_same_key_restore() {
    use orch8_storage::encrypting::EncryptingStorage;
    use orch8_types::encryption::FieldEncryptor;
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let (source, _, initial) = seeded_source(dir.path()).await;
    let raw: Arc<dyn StorageBackend> =
        Arc::new(SqliteStorage::file(source.to_str().unwrap()).await.unwrap());
    let key = "ab".repeat(32);
    let sealed = EncryptingStorage::new(
        Arc::clone(&raw),
        FieldEncryptor::from_hex_key(&key).unwrap(),
    );
    let mut instance = raw.get_instance(initial).await.unwrap().unwrap();
    instance.id = InstanceId::new();
    let secret = "private-context-that-must-stay-encrypted";
    instance.context.data = serde_json::json!({"payload": secret});
    sealed
        .create_instance_externalized(&instance, 1)
        .await
        .unwrap();
    let reference = format!("{}:charge:encrypted", instance.id);
    let output_secret = serde_json::json!({"value": "private-output-that-must-stay-encrypted"});
    sealed
        .save_externalized_state(instance.id, &reference, &output_secret)
        .await
        .unwrap();
    sealed
        .save_block_output(&BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            block_id: BlockId::new("charge"),
            output: orch8_storage::externalizing::marker(&reference),
            output_ref: Some(reference),
            output_size: 100,
            attempt: 0,
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    let snap = snapshot(raw.as_ref(), true).await.unwrap();
    let serialized = serde_json::to_string(&snap.externalized_state).unwrap();
    assert!(!serialized.contains(secret));
    assert!(!serialized.contains("private-output-that-must-stay-encrypted"));
    let target =
        restore_including_payloads(&source, &dir.path().join("encrypted-restore.db")).await;
    let restored = EncryptingStorage::new(target, FieldEncryptor::from_hex_key(&key).unwrap());
    let current = restored.get_instance(instance.id).await.unwrap().unwrap();
    let context = orch8_engine::externalized::resolve_context_markers(
        &restored,
        instance.id,
        current.context,
    )
    .await
    .unwrap();
    assert_eq!(context.data["payload"], secret);
    let output = restored
        .get_block_output(instance.id, &BlockId::new("charge"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        restored
            .get_externalized_state(instance.id, output.output_ref.as_deref().unwrap())
            .await
            .unwrap(),
        Some(output_secret)
    );
}

#[tokio::test]
async fn legacy_archives_remain_readable_and_v2_requires_payload_member() {
    let dir = tempfile::tempdir().unwrap();
    let (source, _, id) = seeded_source(dir.path()).await;
    let raw = SqliteStorage::file(source.to_str().unwrap()).await.unwrap();
    let mut archive =
        read_archive(&write_archive(&snapshot(&raw, true).await.unwrap(), "sqlite").unwrap())
            .unwrap();
    archive.members.remove("externalized_state.jsonl");
    archive
        .manifest
        .files
        .retain(|file| file.name != "externalized_state.jsonl");
    let encode = |archive: &Archive| {
        let mut members = vec![(
            "manifest.json",
            serde_json::to_vec(&archive.manifest).unwrap(),
        )];
        for (name, body) in &archive.members {
            members.push((name.as_str(), body.clone()));
        }
        repack_archive_members(&members)
    };
    assert!(
        read_archive(&encode(&archive))
            .unwrap_err()
            .to_string()
            .contains("missing externalized_state")
    );
    archive.manifest.format_version = 1;
    let legacy = read_archive(&encode(&archive)).unwrap();
    let target = open_target(&DbTarget::Sqlite(dir.path().join("legacy.db")))
        .await
        .unwrap();
    let report = restore(&legacy, target.as_ref(), false, true)
        .await
        .unwrap();
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.contains("version 1 backups omit"))
    );
    assert_eq!(
        target
            .get_block_output(id, &BlockId::new("charge"))
            .await
            .unwrap()
            .unwrap()
            .output,
        serde_json::json!({"charged": true})
    );
    archive.manifest.format_version = 0;
    assert!(read_archive(&encode(&archive)).is_err());
}

#[test]
fn partial_instance_snapshots_cannot_produce_unreadable_archives() {
    let partial = Snapshot {
        instances: Some(Vec::new()),
        ..Snapshot::default()
    };
    assert!(write_archive(&partial, "sqlite").is_err());
}

#[tokio::test]
async fn repeated_payload_restore_preserves_existing_target_values_and_metadata() {
    use chrono::Timelike as _;
    use orch8_storage::externalizing::ExternalizedEntry;
    let dir = tempfile::tempdir().unwrap();
    let (source, _, id) = seeded_source(dir.path()).await;
    let raw = SqliteStorage::file(source.to_str().unwrap()).await.unwrap();
    let entry = ExternalizedEntry {
        instance_id: id,
        ref_key: format!("{id}:payload"),
        payload: serde_json::json!({"source": true}),
        created_at: Utc::now().with_nanosecond(123_456_000).unwrap(),
        expires_at: Some(Utc::now() + chrono::Duration::days(7)),
    };
    raw.restore_externalized_entry(&entry).await.unwrap();
    let archive =
        read_archive(&write_archive(&snapshot(&raw, true).await.unwrap(), "sqlite").unwrap())
            .unwrap();
    let target = open_target(&DbTarget::Sqlite(dir.path().join("idempotent.db")))
        .await
        .unwrap();
    let dry = restore(&archive, target.as_ref(), true, true)
        .await
        .unwrap();
    assert_eq!(dry.counts["externalized_state"].created, 1);
    assert!(target.get_instance(id).await.unwrap().is_none());
    restore(&archive, target.as_ref(), false, true)
        .await
        .unwrap();
    assert_eq!(
        target
            .list_externalized_entries(id, None, 10)
            .await
            .unwrap(),
        vec![entry.clone()]
    );
    let mut replacement = entry;
    replacement.payload = serde_json::json!({"target": "preserve this"});
    replacement.expires_at = None;
    target
        .restore_externalized_entry(&replacement)
        .await
        .unwrap();
    let again = restore(&archive, target.as_ref(), false, true)
        .await
        .unwrap();
    assert_eq!(again.counts["externalized_state"].existing, 1);
    assert_eq!(
        target
            .list_externalized_entries(id, None, 10)
            .await
            .unwrap(),
        vec![replacement]
    );
}

#[tokio::test]
async fn retry_after_payload_write_failure_completes_the_instance_restore() {
    use sqlx::Connection as _;
    let dir = tempfile::tempdir().unwrap();
    let (source, _, id) = seeded_source(dir.path()).await;
    let raw = SqliteStorage::file(source.to_str().unwrap()).await.unwrap();
    for key in ["payload-a", "payload-b"] {
        raw.save_externalized_state(id, key, &serde_json::json!({"key": key}))
            .await
            .unwrap();
    }
    let archive =
        read_archive(&write_archive(&snapshot(&raw, true).await.unwrap(), "sqlite").unwrap())
            .unwrap();
    let target_path = dir.path().join("retry-restore.db");
    let target = open_target(&DbTarget::Sqlite(target_path.clone()))
        .await
        .unwrap();
    let mut writer = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&target_path)
            .busy_timeout(std::time::Duration::from_secs(5)),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER fail_payload_import BEFORE INSERT ON externalized_state WHEN NEW.ref_key='payload-b' BEGIN SELECT RAISE(ABORT, 'simulated payload write failure'); END")
        .execute(&mut writer).await.unwrap();
    let error = restore(&archive, target.as_ref(), false, true)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("simulated payload write failure"));
    sqlx::query("DROP TRIGGER fail_payload_import")
        .execute(&mut writer)
        .await
        .unwrap();
    restore(&archive, target.as_ref(), false, true)
        .await
        .unwrap();
    let payloads = target
        .list_externalized_entries(id, None, 10)
        .await
        .unwrap();
    assert_eq!(
        payloads.len(),
        2,
        "a successful retry must finish the partially imported instance"
    );
    assert_eq!(target.get_all_outputs(id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn malformed_instance_snapshots_fail_preflight_without_catalog_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (source, sequence_id, id) = seeded_source(dir.path()).await;
    let raw = SqliteStorage::file(source.to_str().unwrap()).await.unwrap();
    for kind in [
        "cycle",
        "orphan_node",
        "orphan_output",
        "duplicate_instance",
        "duplicate_output",
        "orphan_payload",
    ] {
        let mut snap = snapshot(&raw, true).await.unwrap();
        match kind {
            "cycle" => {
                let nodes = snap.execution_tree.as_mut().unwrap();
                nodes[0]["parent_id"] = nodes[0]["id"].clone();
            }
            "orphan_node" => {
                snap.execution_tree.as_mut().unwrap()[0]["instance_id"] =
                    serde_json::json!(InstanceId::new());
            }
            "orphan_output" => {
                snap.block_outputs.as_mut().unwrap()[0]["instance_id"] =
                    serde_json::json!(InstanceId::new());
            }
            "duplicate_instance" => {
                let instances = snap.instances.as_mut().unwrap();
                instances.push(instances[0].clone());
            }
            "duplicate_output" => {
                let outputs = snap.block_outputs.as_mut().unwrap();
                outputs.push(outputs[0].clone());
            }
            "orphan_payload" => snap.externalized_state.push(
                serde_json::to_value(orch8_storage::externalizing::ExternalizedEntry {
                    instance_id: InstanceId::new(),
                    ref_key: "orphan".into(),
                    payload: serde_json::json!(true),
                    created_at: Utc::now(),
                    expires_at: None,
                })
                .unwrap(),
            ),
            _ => unreachable!(),
        }
        let archive = read_archive(&write_archive(&snap, "sqlite").unwrap()).unwrap();
        for dry_run in [true, false] {
            let target = open_target(&DbTarget::Sqlite(
                dir.path().join(format!("{kind}-{dry_run}.db")),
            ))
            .await
            .unwrap();
            assert!(
                restore(&archive, target.as_ref(), dry_run, true)
                    .await
                    .is_err(),
                "{kind}"
            );
            assert!(
                target.get_sequence(sequence_id).await.unwrap().is_none(),
                "{kind} mutated configuration"
            );
            assert!(
                target.get_instance(id).await.unwrap().is_none(),
                "{kind} created an instance"
            );
        }
    }
}

#[tokio::test]
async fn postgres_backup_snapshot_does_not_mix_commits() {
    use orch8_storage::testing::{
        backends::{TestPostgres, postgres_url},
        faulty::FaultStorage,
        fixtures::SequenceBuilder,
    };
    let Some(pg) = TestPostgres::fresh().await else {
        return;
    };
    let sequence = SequenceBuilder::new("consistent-backup")
        .step("work", "noop")
        .build();
    pg.create_sequence(&sequence).await.unwrap();
    let now = Utc::now();
    let credential: CredentialDef = serde_json::from_value(serde_json::json!({
        "id": "snapshot-secret", "tenant_id": "acme", "name": "snapshot", "kind": "api_key",
        "value": "secret", "enabled": true, "description": "old", "created_at": now, "updated_at": now
    })).unwrap();
    pg.create_credential(&credential).await.unwrap();
    let mut url = reqwest::Url::parse(&postgres_url().unwrap()).unwrap();
    url.query_pairs_mut()
        .append_pair("options", &format!("-c search_path={}", pg.schema()));
    let (source, source_guard) = open_source(&DbTarget::Postgres(url.to_string()))
        .await
        .unwrap();
    let (source, faults) = FaultStorage::wrap(source);
    let pause = faults.on("list_all_credentials").times(1).pause();
    let mut backup = Box::pin(snapshot(source.as_ref(), false));
    tokio::select! {
        () = pause.entered() => {},
        result = &mut backup => panic!("backup completed before the write rendezvous: {result:?}"),
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => panic!("backup did not reach credential reads"),
    }
    let mut tx = pg.pool().begin().await.unwrap();
    sqlx::query("UPDATE sequences SET version=2 WHERE id=$1")
        .bind(sequence.id.into_uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE credentials SET description='new' WHERE id=$1")
        .bind(&credential.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    pause.release();
    let snap = backup.await.unwrap();
    assert_eq!(snap.sequences[0]["version"], 1);
    assert_eq!(
        snap.credentials[0]["description"], "old",
        "one backup must not mix rows from before and after a single writer commit"
    );
    source_guard.close().await;
}

#[tokio::test]
async fn postgres_backup_read_failure_closes_the_snapshot_before_returning() {
    use orch8_storage::testing::backends::{TestPostgres, postgres_url};
    let Some(pg) = TestPostgres::fresh().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("failed-backup.tar.gz");
    sqlx::query("DROP TABLE credentials")
        .execute(pg.pool())
        .await
        .unwrap();
    let mut url = reqwest::Url::parse(&postgres_url().unwrap()).unwrap();
    url.query_pairs_mut()
        .append_pair("options", &format!("-c search_path={}", pg.schema()))
        .append_pair("application_name", pg.schema());
    let error = run_backup(
        BackupCmd {
            database_url: url.to_string(),
            out: out.clone(),
            include_instances: false,
        },
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(format!("{error:#}").contains("credentials"));
    assert!(!out.exists());
    // Observe real backend teardown, allowing the server to process Terminate.
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut poll = tokio::time::interval(std::time::Duration::from_millis(10));
        loop {
            poll.tick().await;
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name=$1",
            )
            .bind(pg.schema())
            .fetch_one(pg.pool())
            .await
            .unwrap();
            if count == 0 {
                break;
            }
        }
    })
    .await
    .expect("a failed backup must release its PostgreSQL snapshot promptly");
}

async fn dry_restore_archive(dir: &Path) -> PathBuf {
    let (source, _, _) = seeded_source(dir).await;
    let archive = dir.join("dry-run.tar.gz");
    run_backup(
        BackupCmd {
            database_url: source.to_string_lossy().into_owned(),
            out: archive.clone(),
            include_instances: true,
        },
        OutputFormat::Json,
    )
    .await
    .unwrap();
    archive
}

#[tokio::test]
async fn restore_dry_run_does_not_create_a_missing_target_or_its_parent() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dry_restore_archive(dir.path()).await;
    let parent = dir.path().join("not-created");
    let target = parent.join("target.db");
    run_restore(
        RestoreCmd {
            archive,
            database_url: target.to_string_lossy().into_owned(),
            dry_run: true,
            include_instances: true,
        },
        OutputFormat::Json,
    )
    .await
    .unwrap();
    assert!(!parent.exists(), "dry-run initialized a database target");
}

#[tokio::test]
async fn restore_dry_run_does_not_reconcile_an_existing_target_schema() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dry_restore_archive(dir.path()).await;
    let target = dir.path().join("old-target.db");
    let mut connection = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&target)
            .create_if_missing(true),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TABLE operator_data (value TEXT NOT NULL)")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO operator_data VALUES ('keep me')")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
    let before = std::fs::read(&target).unwrap();
    run_restore(
        RestoreCmd {
            archive,
            database_url: target.to_string_lossy().into_owned(),
            dry_run: true,
            include_instances: true,
        },
        OutputFormat::Json,
    )
    .await
    .unwrap();
    assert!(
        std::fs::read(&target).unwrap() == before,
        "dry-run migrated the target database"
    );
    assert!(!target.with_extension("db-wal").exists());
    assert!(!target.with_extension("db-shm").exists());
}

#[tokio::test]
async fn postgres_restore_dry_run_releases_its_snapshot_on_success_and_read_error() {
    use orch8_storage::testing::backends::{TestPostgres, postgres_url};
    let dir = tempfile::tempdir().unwrap();
    let archive = dry_restore_archive(dir.path()).await;
    for fail_read in [false, true] {
        let Some(pg) = TestPostgres::fresh().await else {
            return;
        };
        if fail_read {
            sqlx::query("DROP TABLE credentials")
                .execute(pg.pool())
                .await
                .unwrap();
        }
        let mut url = reqwest::Url::parse(&postgres_url().unwrap()).unwrap();
        url.query_pairs_mut()
            .append_pair("options", &format!("-c search_path={}", pg.schema()))
            .append_pair("application_name", pg.schema());
        let result = run_restore(
            RestoreCmd {
                archive: archive.clone(),
                database_url: url.to_string(),
                dry_run: true,
                include_instances: true,
            },
            OutputFormat::Json,
        )
        .await;
        if fail_read {
            assert!(format!("{:#}", result.unwrap_err()).contains("credentials"));
        } else {
            result.unwrap();
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
            .fetch_one(pg.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "dry-run wrote instances to its PostgreSQL target");
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut poll = tokio::time::interval(std::time::Duration::from_millis(10));
            loop {
                poll.tick().await;
                let count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name=$1",
                )
                .bind(pg.schema())
                .fetch_one(pg.pool())
                .await
                .unwrap();
                if count == 0 {
                    break;
                }
            }
        })
        .await
        .expect("dry-run retained its target snapshot session after returning");
    }
}
