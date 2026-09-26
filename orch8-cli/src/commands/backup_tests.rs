//! Backup / restore round trips on `SQLite` (the `PostgreSQL` path shares
//! every function except `open_source` / `open_target`).
#![allow(clippy::too_many_lines)]

use super::*;
use chrono::Utc;
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{
    AdminStore as _, ExecutionTreeStore as _, InstanceStore as _, OutputStore as _,
    SchedulingStore as _, SequenceStore as _, WorkerStore as _,
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
    let rebuild = |members: &[(&str, Vec<u8>)]| {
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
    };
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
    assert!(err.contains("not listed"), "{err}");

    let mut newer = archive.manifest.clone();
    newer.format_version = FORMAT_VERSION + 1;
    let mut future = members.clone();
    future[0] = ("manifest.json", serde_json::to_vec(&newer).unwrap());
    let err = read_archive(&rebuild(&future)).unwrap_err().to_string();
    assert!(err.contains("newer than this binary"), "{err}");

    assert!(read_archive(b"not a tarball").is_err());
}
