//! Self-contained product demonstrations that exercise real engine protocols.

use std::sync::Arc;

use anyhow::{Context as _, Result, bail};
use chrono::{Duration, Utc};
use ed25519_dalek::SigningKey;
use orch8_engine::capsule::{
    CapsuleExportRequest, CapsuleImportRequest, export_paused_capsule,
    verify_and_import_paused_capsule_bytes,
};
use orch8_storage::artifacts::ObjectArtifactStore;
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{
    ContinuityStore as _, InstanceStore as _, ResourceStore as _, SequenceStore as _,
};
use orch8_types::checkpoint::Checkpoint;
use orch8_types::context::ExecutionContext;
use orch8_types::continuity::{
    CapsuleRequirements, ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState,
    RuntimeId,
};
use orch8_types::encryption::FieldEncryptor;
use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::SequenceDefinition;
use rand::Rng as _;
use serde::Serialize;
use serde_json::json;

use crate::OutputFormat;

#[derive(Debug, clap::Subcommand)]
pub enum DemoCmd {
    /// Run a real cloud -> private device -> cloud capsule round trip locally.
    PortableAgent,
    /// Simulate a process crash and prove stale work is reclaimed after restart.
    CrashRecovery,
}

#[derive(Debug, Serialize)]
struct CrashRecoveryReport {
    database: String,
    instance_id: String,
    before_restart: String,
    recovered: u64,
    after_restart: String,
}

#[derive(Debug, Serialize)]
pub struct PortableAgentReport {
    pub continuity_id: String,
    pub source_instance_id: String,
    pub device_instance_id: String,
    pub returned_instance_id: String,
    pub source_epoch: u64,
    pub device_epoch: u64,
    pub returned_epoch: u64,
    pub approval: String,
    pub private_tool_result_sha256: String,
    pub private_input_left_device: bool,
    pub tampered_or_untrusted_capsule_rejected: bool,
    pub redelivery_was_idempotent: bool,
    pub final_state: String,
}

pub async fn run(cmd: DemoCmd, format: OutputFormat) -> Result<()> {
    match cmd {
        DemoCmd::PortableAgent => {
            let report = run_portable_agent().await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
                OutputFormat::Table => print_portable_agent_report(&report),
            }
        }
        DemoCmd::CrashRecovery => {
            let report = run_crash_recovery().await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
                OutputFormat::Table => {
                    println!("Crash recovery completed");
                    println!("  instance: {}", report.instance_id);
                    println!(
                        "  state:    {} -> {}",
                        report.before_restart, report.after_restart
                    );
                    println!("  reclaimed: {}", report.recovered);
                }
            }
        }
    }
    Ok(())
}

async fn run_crash_recovery() -> Result<CrashRecoveryReport> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("crash-recovery.db");
    let path_string = path.display().to_string();
    let tenant = TenantId::new("crash-demo").map_err(anyhow::Error::msg)?;
    let sequence = demo_sequence(&tenant)?;
    let now = Utc::now();
    let instance = TaskInstance {
        id: InstanceId::new(),
        sequence_id: sequence.id,
        tenant_id: tenant,
        namespace: Namespace::new("default"),
        state: InstanceState::Running,
        next_fire_at: Some(now - Duration::minutes(10)),
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({"demo": "crash-recovery"}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now - Duration::minutes(10),
        updated_at: now - Duration::minutes(10),
    };
    {
        let storage = SqliteStorage::file(&path_string).await?;
        storage.create_sequence(&sequence).await?;
        storage.create_instance(&instance).await?;
        // Dropping the only pool simulates the engine process disappearing.
    }
    let restarted = SqliteStorage::file(&path_string).await?;
    let recovered = orch8_engine::recovery::recover_stale_instances(&restarted, 60).await?;
    let state = restarted
        .get_instance(instance.id)
        .await?
        .context("recovered instance disappeared")?
        .state;
    if recovered != 1 || state != InstanceState::Scheduled {
        bail!("recovery invariant failed: recovered={recovered}, state={state}");
    }
    Ok(CrashRecoveryReport {
        database: path_string,
        instance_id: instance.id.to_string(),
        before_restart: InstanceState::Running.to_string(),
        recovered,
        after_restart: state.to_string(),
    })
}

fn print_portable_agent_report(report: &PortableAgentReport) {
    println!("Portable agent round trip completed");
    println!("  continuity:       {}", report.continuity_id);
    println!(
        "  ownership epochs: cloud {} -> device {} -> cloud {}",
        report.source_epoch, report.device_epoch, report.returned_epoch
    );
    println!("  approval:         {}", report.approval);
    println!(
        "  private result:   sha256:{}",
        report.private_tool_result_sha256
    );
    println!(
        "  private input:    {}",
        if report.private_input_left_device {
            "left device (FAILED)"
        } else {
            "remained on device"
        }
    );
    println!(
        "  trust check:      {}",
        if report.tampered_or_untrusted_capsule_rejected {
            "untrusted capsule rejected"
        } else {
            "FAILED"
        }
    );
    println!(
        "  redelivery:       {}",
        if report.redelivery_was_idempotent {
            "idempotent"
        } else {
            "FAILED"
        }
    );
    println!("  final state:      {}", report.final_state);
}

/// Execute the category-defining protocol locally using three isolated
/// in-memory runtimes. The payload crosses each boundary encrypted and signed;
/// only a digest of the simulated private device input returns to the cloud.
pub async fn run_portable_agent() -> Result<PortableAgentReport> {
    let cloud = demo_storage().await?;
    let device = demo_storage().await?;
    let returned_cloud = demo_storage().await?;
    let tenant_id = TenantId::new("portable-agent-demo").map_err(anyhow::Error::msg)?;
    let sequence = demo_sequence(&tenant_id)?;
    for storage in [&cloud, &device, &returned_cloud] {
        storage.create_sequence(&sequence).await?;
    }

    let source_runtime = RuntimeId::new();
    let device_runtime = RuntimeId::new();
    let returned_runtime = RuntimeId::new();
    let source_instance = create_paused_instance(
        &cloud,
        &sequence,
        json!({"request": "approve a private device check"}),
        "cloud_ready",
    )
    .await?;
    let source_execution = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: tenant_id.clone(),
        current_instance_id: source_instance.id,
        owner_runtime_id: source_runtime,
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    cloud.create_continuity_execution(&source_execution).await?;

    let first_transfer = transfer_capsule(
        &cloud,
        &device,
        source_execution.clone(),
        device_runtime,
        InstanceId::new(),
        "cloud-demo-key",
    )
    .await?;

    // The private value is deliberately never written to execution context.
    // Only its digest becomes portable evidence for the returning agent.
    let private_input = b"device-secret:biometric-approved";
    let private_digest =
        perform_private_device_action(&device, &first_transfer.instance, private_input).await?;
    let second_transfer = transfer_capsule(
        &device,
        &returned_cloud,
        first_transfer.execution.clone(),
        returned_runtime,
        InstanceId::new(),
        "device-demo-key",
    )
    .await?;
    let returned_context_json = serde_json::to_vec(&second_transfer.instance.context)?;

    if second_transfer.execution.epoch.get() != 2
        || second_transfer.execution.owner_runtime_id != returned_runtime
        || second_transfer.instance.state != InstanceState::Scheduled
    {
        bail!("portable-agent ownership invariant failed");
    }

    Ok(PortableAgentReport {
        continuity_id: source_execution.continuity_id.to_string(),
        source_instance_id: source_instance.id.to_string(),
        device_instance_id: first_transfer.instance.id.to_string(),
        returned_instance_id: second_transfer.instance.id.to_string(),
        source_epoch: source_execution.epoch.get(),
        device_epoch: first_transfer.execution.epoch.get(),
        returned_epoch: second_transfer.execution.epoch.get(),
        approval: "approved".to_owned(),
        private_tool_result_sha256: private_digest,
        private_input_left_device: returned_context_json
            .windows(private_input.len())
            .any(|window| window == private_input),
        tampered_or_untrusted_capsule_rejected: first_transfer.untrusted_rejected
            && second_transfer.untrusted_rejected,
        redelivery_was_idempotent: first_transfer.redelivery_idempotent
            && second_transfer.redelivery_idempotent,
        final_state: "scheduled".to_owned(),
    })
}

struct TransferResult {
    instance: TaskInstance,
    execution: ContinuityExecution,
    untrusted_rejected: bool,
    redelivery_idempotent: bool,
}

async fn transfer_capsule(
    source: &SqliteStorage,
    destination: &SqliteStorage,
    source_execution: ContinuityExecution,
    destination_runtime: RuntimeId,
    destination_instance_id: InstanceId,
    signing_key_id: &str,
) -> Result<TransferResult> {
    let payload_key = random_key();
    let signing_key = SigningKey::from_bytes(&random_key());
    let capsule = export_capsule(
        source,
        source_execution.clone(),
        destination_runtime,
        &signing_key,
        &payload_key,
        signing_key_id,
    )
    .await?;
    let payload = source
        .get_artifact(&capsule.manifest.payload_artifact.key)
        .await?
        .context("capsule payload is missing")?;
    let untrusted_rejected = rejects_untrusted_capsule(
        destination,
        &capsule,
        &payload,
        &payload_key,
        destination_runtime,
        destination_instance_id,
    )
    .await;
    let instance = import_and_activate(
        destination,
        &capsule,
        &payload,
        &payload_key,
        destination_runtime,
        destination_instance_id,
    )
    .await?;
    let redelivered = import_capsule(
        destination,
        &capsule,
        &payload,
        &payload_key,
        destination_runtime,
        destination_instance_id,
    )
    .await?;
    let execution = destination
        .get_continuity_execution(&source_execution.tenant_id, source_execution.continuity_id)
        .await?
        .context("destination ownership record is missing")?;
    Ok(TransferResult {
        instance,
        execution,
        untrusted_rejected,
        redelivery_idempotent: redelivered.id == destination_instance_id,
    })
}

async fn rejects_untrusted_capsule(
    storage: &SqliteStorage,
    capsule: &orch8_publisher::capsule::SignedCapsuleManifest,
    payload: &[u8],
    payload_key: &[u8; 32],
    destination_runtime: RuntimeId,
    destination_instance_id: InstanceId,
) -> bool {
    verify_and_import_paused_capsule_bytes(
        storage,
        capsule,
        payload,
        CapsuleImportRequest {
            tenant_id: &capsule.manifest.tenant_id,
            destination_runtime_id: destination_runtime,
            destination_instance_id: Some(destination_instance_id),
            expected_epoch: capsule.manifest.epoch,
            trusted_public_keys: &["not-a-trusted-key".to_owned()],
            now: Utc::now(),
        },
        &FieldEncryptor::from_bytes(payload_key),
    )
    .await
    .is_err()
}

async fn perform_private_device_action(
    storage: &SqliteStorage,
    instance: &TaskInstance,
    private_input: &[u8],
) -> Result<String> {
    let digest = sha256_hex(private_input);
    let mut context = instance.context.clone();
    context.data = json!({
        "request": "approve a private device check",
        "approval": "approved",
        "private_tool_result_sha256": digest,
    });
    storage
        .update_instance_context(instance.id, &context)
        .await?;
    storage
        .update_instance_state(instance.id, InstanceState::Paused, None)
        .await?;
    storage
        .save_checkpoint(&Checkpoint {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            checkpoint_data: json!({"safe_boundary": "device_approved"}),
            created_at: Utc::now(),
        })
        .await?;
    Ok(digest)
}

async fn demo_storage() -> Result<SqliteStorage> {
    Ok(SqliteStorage::in_memory()
        .await?
        .with_artifact_store(Arc::new(ObjectArtifactStore::memory())))
}

fn demo_sequence(tenant_id: &TenantId) -> Result<SequenceDefinition> {
    Ok(serde_json::from_value(json!({
        "id": SequenceId::new(),
        "tenant_id": tenant_id,
        "namespace": "default",
        "name": "portable-private-approval",
        "version": 1,
        "blocks": [],
        "created_at": Utc::now(),
    }))?)
}

async fn create_paused_instance(
    storage: &SqliteStorage,
    sequence: &SequenceDefinition,
    data: serde_json::Value,
    boundary: &str,
) -> Result<TaskInstance> {
    let now = Utc::now();
    let instance = TaskInstance {
        id: InstanceId::new(),
        sequence_id: sequence.id,
        tenant_id: sequence.tenant_id.clone(),
        namespace: Namespace::new("default"),
        state: InstanceState::Paused,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".to_owned(),
        metadata: json!({"demo": "portable-agent"}),
        context: ExecutionContext {
            data,
            ..ExecutionContext::default()
        },
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    };
    storage.create_instance(&instance).await?;
    storage
        .save_checkpoint(&Checkpoint {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            checkpoint_data: json!({"safe_boundary": boundary}),
            created_at: now,
        })
        .await?;
    Ok(instance)
}

async fn export_capsule(
    storage: &SqliteStorage,
    execution: ContinuityExecution,
    destination_runtime_id: RuntimeId,
    signing_key: &SigningKey,
    payload_key: &[u8; 32],
    signing_key_id: &str,
) -> Result<orch8_publisher::capsule::SignedCapsuleManifest> {
    Ok(export_paused_capsule(
        storage,
        CapsuleExportRequest {
            continuity: execution,
            destination_runtime_id: Some(destination_runtime_id),
            requirements: CapsuleRequirements {
                requires_human_ui: true,
                ..CapsuleRequirements::default()
            },
            expires_at: Utc::now() + Duration::minutes(5),
            signing_key_id: signing_key_id.to_owned(),
            encryption_key_id: "one-time-transfer-key".to_owned(),
        },
        signing_key,
        &FieldEncryptor::from_bytes(payload_key),
    )
    .await?)
}

async fn import_capsule(
    storage: &SqliteStorage,
    signed: &orch8_publisher::capsule::SignedCapsuleManifest,
    sealed: &[u8],
    payload_key: &[u8; 32],
    destination_runtime_id: RuntimeId,
    destination_instance_id: InstanceId,
) -> Result<TaskInstance> {
    let trusted_keys = [signed.public_key.clone()];
    let (instance, _) = verify_and_import_paused_capsule_bytes(
        storage,
        signed,
        sealed,
        CapsuleImportRequest {
            tenant_id: &signed.manifest.tenant_id,
            destination_runtime_id,
            destination_instance_id: Some(destination_instance_id),
            expected_epoch: signed.manifest.epoch,
            trusted_public_keys: &trusted_keys,
            now: Utc::now(),
        },
        &FieldEncryptor::from_bytes(payload_key),
    )
    .await?;
    Ok(instance)
}

async fn import_and_activate(
    storage: &SqliteStorage,
    signed: &orch8_publisher::capsule::SignedCapsuleManifest,
    sealed: &[u8],
    payload_key: &[u8; 32],
    destination_runtime_id: RuntimeId,
    destination_instance_id: InstanceId,
) -> Result<TaskInstance> {
    let instance = import_capsule(
        storage,
        signed,
        sealed,
        payload_key,
        destination_runtime_id,
        destination_instance_id,
    )
    .await?;
    let pending = ContinuityExecution {
        continuity_id: signed.manifest.continuity_id,
        tenant_id: signed.manifest.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: signed.manifest.source_runtime_id,
        epoch: signed.manifest.epoch,
        state: OwnershipState::Transferring,
        updated_at: Utc::now(),
    };
    storage.create_continuity_execution(&pending).await?;
    storage.save_capsule_manifest(&signed.manifest).await?;
    let accepted = ContinuityExecution {
        continuity_id: pending.continuity_id,
        tenant_id: pending.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: destination_runtime_id,
        epoch: pending.epoch.checked_next()?,
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    let claimed = storage
        .cas_continuity_owner(
            &pending.tenant_id,
            pending.continuity_id,
            pending.epoch,
            pending.owner_runtime_id,
            &accepted,
        )
        .await?;
    if !claimed {
        bail!("destination ownership changed concurrently");
    }
    storage
        .update_instance_state(instance.id, InstanceState::Scheduled, Some(Utc::now()))
        .await?;
    storage
        .get_instance(instance.id)
        .await?
        .context("activated instance disappeared")
}

fn random_key() -> [u8; 32] {
    let mut key = [0_u8; 32];
    rand::rng().fill_bytes(&mut key);
    key
}

fn sha256_hex(input: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};
    use std::fmt::Write as _;

    Sha256::digest(input)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn portable_agent_round_trip_preserves_core_invariants() {
        let report = run_portable_agent().await.unwrap();
        assert_eq!(report.source_epoch, 0);
        assert_eq!(report.device_epoch, 1);
        assert_eq!(report.returned_epoch, 2);
        assert_eq!(report.approval, "approved");
        assert!(!report.private_input_left_device);
        assert!(report.tampered_or_untrusted_capsule_rejected);
        assert!(report.redelivery_was_idempotent);
        assert_eq!(report.final_state, "scheduled");
    }
}
