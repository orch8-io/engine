//! `orch8 backup` / `orch8 restore` — logical export/import of engine
//! configuration (and optionally instances) for `SQLite` and `PostgreSQL`.
//!
//! Archive formats (3 for configuration, 5 with instances): a `.tar.gz` containing
//! `manifest.json` plus one JSON-lines file per kind (`sequences.jsonl`,
//! `triggers.jsonl`, `cron.jsonl`, `queue_routing.jsonl`,
//! `credentials.jsonl`, and with `--include-instances` `instances.jsonl`,
//! `execution_tree.jsonl`, `block_outputs.jsonl`, `externalized_state.jsonl`, and
//! `checkpoints.jsonl`, `signals.jsonl`, `instance_kv.jsonl`,
//! `effect_scopes.jsonl`, `effect_receipts.jsonl`, `worker_tasks.jsonl`). The manifest records the
//! record count and SHA-256 of every file; restore verifies them before
//! writing anything.
//!
//! Backups read the storage layer directly (the API has no complete list
//! endpoints for every kind). A `SQLite` source is read through a read-only
//! connection into a consistent temporary snapshot; only the snapshot is
//! opened for schema reconciliation. Credentials are
//! exported exactly as stored: encrypted with the server's
//! `ORCH8_ENCRYPTION_KEY` when it has one — the target server needs the same
//! key — and plaintext if the source ran without a key.
//!
//! Restore is idempotent: every record is looked up by its identity first
//! and skipped when it already exists. `--dry-run` reports what would be
//! created without writing.

use std::collections::BTreeMap;
use std::io::{BufRead as _, BufReader, Read, Write as _};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use clap::Args;
#[cfg(test)]
use ed25519_dalek::Signer as _;
use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use orch8_storage::StorageBackend;
use orch8_storage::restore::{EffectArchive, InstanceHistory, InstanceKvEntry, WorkerTaskArchive};
use orch8_types::checkpoint::Checkpoint;
use orch8_types::continuity::{CapsuleEffectState, EffectReceipt, OwnershipState};
use orch8_types::credential::CredentialDef;
use orch8_types::cron::CronSchedule;
use orch8_types::execution::ExecutionNode;
#[cfg(test)]
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::instance::TaskInstance;
use orch8_types::output::BlockOutput;
use orch8_types::queue_routing::QueueRoutingRule;
use orch8_types::sequence::SequenceDefinition;
use orch8_types::signal::Signal;
use orch8_types::trigger::TriggerDef;

use super::upgrade::{DbTarget, parse_target};
use crate::OutputFormat;

#[path = "backup_integrity.rs"]
mod integrity;
pub use integrity::{VerifyExternalizedCmd, run_verify_externalized};

#[path = "backup_artifacts.rs"]
mod artifacts;

#[path = "backup_stream.rs"]
mod stream;

#[path = "backup_age.rs"]
mod age;

pub const FORMAT: &str = "orch8-backup";
pub const FORMAT_VERSION: u32 = 3;
const ARTIFACT_FORMAT_VERSION: u32 = 4;
const WORKER_TASK_FORMAT_VERSION: u32 = 5;
const WORKER_TASKS: &str = "worker_tasks.jsonl";
/// Storage list calls without offsets cap at this many rows.
const PAGE: u32 = 500;
/// Bound the compressed input and the expanded data staged on disk.
const MAX_ARCHIVE_BYTES: usize = 512 * 1024 * 1024;
const MAX_MEMBER_BYTES: u64 = 512 * 1024 * 1024;
const MAX_TOTAL_UNCOMPRESSED_BYTES: u64 = 512 * 1024 * 1024;
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MANIFEST_SIGNATURE: &str = "manifest.ed25519";
const KNOWN_ARCHIVE_MEMBERS: &[&str] = &[
    "manifest.json",
    MANIFEST_SIGNATURE,
    "sequences.jsonl",
    "credentials.jsonl",
    "triggers.jsonl",
    "cron.jsonl",
    "queue_routing.jsonl",
    "instances.jsonl",
    "execution_tree.jsonl",
    "block_outputs.jsonl",
    "externalized_state.jsonl",
    "checkpoints.jsonl",
    "signals.jsonl",
    "instance_kv.jsonl",
    "effect_scopes.jsonl",
    "effect_receipts.jsonl",
    WORKER_TASKS,
    artifacts::REFERENCES,
];

const CREDENTIAL_WARNING: &str = "credential values are exported exactly as stored: encrypted \
     with the source server's ORCH8_ENCRYPTION_KEY when one is configured (the target server \
     must use the same key to read them), or plaintext if the source ran without a key — \
     protect this archive accordingly";

#[derive(Debug, Args)]
pub struct BackupCmd {
    /// Source database: `postgres://…`, `sqlite:path`, or a `SQLite` file path.
    #[arg(long, env = "ORCH8_DATABASE_URL", hide_env_values = true)]
    pub database_url: String,
    /// Archive to write (`.tar.gz`, or `.tar.gz.age` when encrypted).
    #[arg(long)]
    pub out: PathBuf,
    /// Also export instances with their execution trees and block outputs.
    #[arg(long)]
    pub include_instances: bool,
    /// Include artifacts from this local object-store directory.
    #[arg(long, requires = "include_instances")]
    pub artifacts_dir: Option<PathBuf>,
    /// Sign the manifest with a file containing a base64 Ed25519 seed.
    #[arg(long)]
    pub signing_key_file: Option<PathBuf>,
    /// Encrypt the complete archive to this native age public key. Repeat for more recipients.
    #[arg(long = "encrypt-recipient", value_parser = age::parse_recipient)]
    pub encrypt_recipients: Vec<String>,
}

#[derive(Debug, Args)]
pub struct RestoreCmd {
    /// Archive produced by `orch8 backup`.
    pub archive: PathBuf,
    /// Target database: `postgres://…`, `sqlite:path`, or a `SQLite` file path.
    /// `PostgreSQL` targets must be fully migrated (`orch8 migrate`).
    #[arg(long, env = "ORCH8_DATABASE_URL", hide_env_values = true)]
    pub database_url: String,
    /// Verify the archive and report what would be restored, writing nothing.
    #[arg(long)]
    pub dry_run: bool,
    /// Also restore instances (execution trees, block outputs) when present.
    #[arg(long)]
    pub include_instances: bool,
    /// Restore archived artifacts to this local object-store directory.
    #[arg(long, requires = "include_instances")]
    pub artifacts_dir: Option<PathBuf>,
    /// Decrypt an age backup with this identity file. Repeat to try more identities.
    #[arg(long = "identity")]
    pub identities: Vec<PathBuf>,
    /// Require a signed manifest verified with this base64 Ed25519 public key.
    #[arg(long)]
    pub trusted_key_file: Option<PathBuf>,
}

/// One archive member in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestFile {
    pub name: String,
    pub records: u64,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub format: String,
    pub format_version: u32,
    pub created_at: String,
    pub orch8_version: String,
    pub source_backend: String,
    /// Bundled schema version of the binary that wrote the archive.
    pub storage_schema_version: u32,
    pub include_instances: bool,
    pub files: Vec<ManifestFile>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Records of one kind, as JSON values (secrets already exposed).
#[cfg(test)]
#[derive(Debug, Default)]
pub struct Snapshot {
    pub sequences: Vec<Value>,
    pub triggers: Vec<Value>,
    pub cron: Vec<Value>,
    pub queue_routing: Vec<Value>,
    pub credentials: Vec<Value>,
    pub instances: Option<Vec<Value>>,
    pub execution_tree: Option<Vec<Value>>,
    pub block_outputs: Option<Vec<Value>>,
    pub externalized_state: Vec<Value>,
    pub checkpoints: Vec<Value>,
    pub signals: Vec<Value>,
    pub instance_kv: Vec<Value>,
    pub effect_scopes: Vec<Value>,
    pub effect_receipts: Vec<Value>,
    pub worker_tasks: Vec<Value>,
    pub warnings: Vec<String>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_bytes(&Sha256::digest(bytes))
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// Serialize with secrets exposed (the types redact them on Serialize).
fn credential_json(credential: &CredentialDef) -> Result<Value> {
    let mut value = serde_json::to_value(credential)?;
    value["value"] = Value::String(credential.value.expose().to_string());
    if let Some(token) = &credential.refresh_token {
        value["refresh_token"] = Value::String(token.expose().to_string());
    }
    Ok(value)
}

fn trigger_json(trigger: &TriggerDef) -> Result<Value> {
    let mut value = serde_json::to_value(trigger)?;
    if let Some(secret) = &trigger.secret {
        value["secret"] = Value::String(secret.expose().to_string());
    }
    Ok(value)
}

// ---------------------------------------------------------------------------
// Storage access
// ---------------------------------------------------------------------------

/// Keep the `SQLite` copy alive and explicitly release the `PostgreSQL` read transaction.
#[derive(Default)]
struct SourceGuard {
    _sqlite_snapshot: Option<tempfile::TempDir>,
    sqlite_pool: Option<sqlx::SqlitePool>,
    postgres_pool: Option<sqlx::PgPool>,
}

impl SourceGuard {
    async fn close(&self) {
        if let Some(pool) = &self.sqlite_pool {
            pool.close().await;
        }
        if let Some(pool) = &self.postgres_pool {
            pool.close().await;
        }
    }
}

/// Open the source for reading. A read-only `SQLite` connection makes a
/// transactionally consistent snapshot, including committed WAL frames.
/// Independent file copies can lose commits when a live writer checkpoints.
/// Schema reconciliation is confined to the temporary snapshot. `PostgreSQL`
/// reads share a dedicated read-only repeatable-read transaction.
async fn open_source(target: &DbTarget) -> Result<(Arc<dyn StorageBackend>, SourceGuard)> {
    match target {
        DbTarget::Postgres(url) => {
            let storage = orch8_storage::postgres::PostgresStorage::read_snapshot(url, None)
                .await
                .context("connect to PostgreSQL")?;
            let guard = SourceGuard {
                _sqlite_snapshot: None,
                sqlite_pool: None,
                postgres_pool: Some(storage.pool().clone()),
            };
            Ok((Arc::new(storage), guard))
        }
        DbTarget::Sqlite(path) => {
            if !path.is_file() {
                bail!("SQLite database {} does not exist", path.display());
            }
            let dir = tempfile::tempdir().context("create snapshot directory")?;
            let copy = dir.path().join("snapshot.db");
            orch8_storage::sqlite::copy_snapshot(path, &copy)
                .await
                .with_context(|| format!("snapshot {} consistently", path.display()))?;
            let storage = orch8_storage::sqlite::SqliteStorage::file(
                copy.to_str().context("non-UTF-8 temp path")?,
            )
            .await
            .context("open SQLite snapshot")?;
            let guard = SourceGuard {
                _sqlite_snapshot: Some(dir),
                sqlite_pool: Some(storage.pool().clone()),
                postgres_pool: None,
            };
            Ok((Arc::new(storage), guard))
        }
    }
}

/// Open the restore target. `PostgreSQL` must be fully migrated; `SQLite` is
/// created/reconciled like on server boot.
async fn open_target(target: &DbTarget) -> Result<Arc<dyn StorageBackend>> {
    match target {
        DbTarget::Postgres(url) => {
            require_current_target_schema(target).await?;
            Ok(Arc::new(
                orch8_storage::postgres::PostgresStorage::new(url, 4, None)
                    .await
                    .context("connect to PostgreSQL")?,
            ))
        }
        DbTarget::Sqlite(path) => {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent)?;
            }
            Ok(Arc::new(
                orch8_storage::sqlite::SqliteStorage::file(
                    path.to_str().context("non-UTF-8 database path")?,
                )
                .await
                .context("open SQLite target")?,
            ))
        }
    }
}

/// A dry run reads the same catalogs real restore would inspect, while schema
/// reconciliation happens only in a disposable `SQLite` snapshot. A missing
/// `SQLite` target is an empty catalog and does not need a filesystem entry.
async fn open_restore_target(
    target: &DbTarget,
    dry_run: bool,
) -> Result<(Arc<dyn StorageBackend>, SourceGuard)> {
    if !dry_run {
        return Ok((open_target(target).await?, SourceGuard::default()));
    }
    match target {
        DbTarget::Sqlite(path)
            if !path
                .try_exists()
                .with_context(|| format!("inspect {}", path.display()))? =>
        {
            Ok((
                Arc::new(orch8_storage::sqlite::SqliteStorage::in_memory().await?),
                SourceGuard::default(),
            ))
        }
        DbTarget::Postgres(_) => {
            require_current_target_schema(target).await?;
            open_source(target).await
        }
        DbTarget::Sqlite(_) => open_source(target).await,
    }
}

async fn require_current_target_schema(target: &DbTarget) -> Result<()> {
    let report = super::upgrade::check(target).await?;
    if !report.is_current() {
        bail!(
            "the target database has {} pending migration(s); run `orch8 migrate` first \
                     (see `orch8 upgrade --check`)",
            report.pending.len()
        );
    }
    Ok(())
}

/// Read the complete payload catalog for one instance using bounded pages.
#[cfg(test)]
async fn snapshot_payloads(
    storage: &dyn StorageBackend,
    id: orch8_types::ids::InstanceId,
) -> Result<Vec<Value>> {
    let mut values = Vec::new();
    let mut after = None;
    loop {
        let page = storage
            .list_externalized_entries(id, after.as_deref(), PAGE)
            .await?;
        let count = page.len();
        after = page.last().map(|entry| entry.ref_key.clone());
        for entry in page {
            values.push(serde_json::to_value(entry)?);
        }
        if count < PAGE as usize {
            return Ok(values);
        }
    }
}

/// Read everything to back up.
#[cfg(test)]
pub async fn snapshot(storage: &dyn StorageBackend, include_instances: bool) -> Result<Snapshot> {
    let mut snap = Snapshot::default();

    let mut sequences: Vec<SequenceDefinition> = Vec::new();
    let mut offset = 0;
    loop {
        let page = storage
            .list_sequences(None, None, PAGE, offset)
            .await
            .context("list sequences")?;
        let n = page.len();
        sequences.extend(page);
        if n < PAGE as usize {
            break;
        }
        offset += PAGE;
    }
    for seq in &sequences {
        snap.sequences.push(serde_json::to_value(seq)?);
    }

    // Catalog reads are complete independently of sequence ownership and
    // public API pagination caps (disabled/preprovisioned rows count too).
    for trigger in storage.list_all_triggers(None).await? {
        snap.triggers.push(trigger_json(&trigger)?);
    }
    for schedule in storage.list_all_cron_schedules(None).await? {
        snap.cron.push(serde_json::to_value(&schedule)?);
    }

    for rule in storage.list_queue_routing_rules(None, None).await? {
        snap.queue_routing.push(serde_json::to_value(&rule)?);
    }

    for credential in storage.list_all_credentials(None).await? {
        snap.credentials.push(credential_json(&credential)?);
    }
    if !snap.credentials.is_empty() {
        snap.warnings.push(CREDENTIAL_WARNING.into());
    }

    if include_instances {
        let mut instances = Vec::new();
        let mut tree = Vec::new();
        let mut outputs = Vec::new();
        let mut offset = 0_u64;
        loop {
            let page: Vec<TaskInstance> = storage
                .list_instances(
                    &InstanceFilter::default(),
                    &Pagination {
                        offset,
                        limit: PAGE,
                        sort_ascending: true,
                    },
                )
                .await
                .context("list instances")?;
            let n = page.len();
            for instance in page {
                if orch8_storage::delegation_publication::is_proxy_record(storage, &instance)
                    .await?
                {
                    bail!(
                        "instance backup cannot preserve delegation relay authority; use a full database backup for relay state"
                    );
                }
                for node in storage.get_execution_tree(instance.id).await? {
                    tree.push(serde_json::to_value(&node)?);
                }
                for output in storage.get_all_outputs(instance.id).await? {
                    outputs.push(serde_json::to_value(&output)?);
                }
                snap.externalized_state
                    .extend(snapshot_payloads(storage, instance.id).await?);
                snapshot_history(storage, &instance, &mut snap).await?;
                instances.push(serde_json::to_value(&instance)?);
            }
            if n < PAGE as usize {
                break;
            }
            offset += u64::from(PAGE);
        }
        snap.instances = Some(instances);
        snap.execution_tree = Some(tree);
        snap.block_outputs = Some(outputs);
        snap.warnings
            .push("instance backups exclude issued worker claims and audit history".into());
    }
    Ok(snap)
}

pub(super) async fn effect_archive(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
) -> Result<Option<EffectArchive>> {
    if !instance.state.is_terminal() && storage.is_physical_instance_retired(instance.id).await? {
        bail!(
            "instance {} has retired execution authority that this logical backup cannot transfer",
            instance.id
        );
    }
    let Some(owner) = storage
        .get_continuity_execution_touching_instance(&instance.tenant_id, instance.id)
        .await?
    else {
        return Ok(None);
    };
    if owner.current_instance_id != instance.id && instance.state.is_terminal() {
        return Ok(None);
    }
    if owner.current_instance_id != instance.id
        || !matches!(
            owner.state,
            OwnershipState::Owned | OwnershipState::Completed
        )
    {
        bail!(
            "instance {} has an active continuity transfer or compensation; use a full database backup",
            instance.id
        );
    }
    let sequence = storage
        .get_sequence(instance.sequence_id)
        .await?
        .context("effect ledger's sequence is missing")?;
    let (active_attempts, reset_blocks) = storage.export_effect_attempts(&owner).await?;
    let commit_guards = storage
        .load_effect_guards(
            &owner.tenant_id,
            owner.continuity_id,
            sequence.id,
            sequence.version,
            None,
        )
        .await?;
    Ok(Some(EffectArchive {
        owner,
        sequence_version: sequence.version,
        state: CapsuleEffectState {
            receipts: Vec::new(),
            active_attempts,
            reset_blocks,
            commit_guards,
        },
    }))
}

#[cfg(test)]
async fn snapshot_history(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    snapshot: &mut Snapshot,
) -> Result<()> {
    let mut after = None;
    loop {
        let checkpoints = storage
            .list_checkpoint_page(instance.id, after, PAGE)
            .await?;
        if checkpoints.is_empty() {
            break;
        }
        after = checkpoints.last().map(|checkpoint| checkpoint.id);
        for checkpoint in checkpoints {
            snapshot.checkpoints.push(serde_json::to_value(checkpoint)?);
        }
    }
    let mut after = None;
    loop {
        let signals = storage
            .list_signal_history_page(instance.id, after, PAGE)
            .await?;
        if signals.is_empty() {
            break;
        }
        after = signals.last().map(|signal| signal.id);
        for signal in signals {
            snapshot.signals.push(serde_json::to_value(signal)?);
        }
    }
    let mut after = None;
    loop {
        let entries = storage
            .list_instance_kv_page(instance.id, after.as_deref(), PAGE)
            .await?;
        if entries.is_empty() {
            break;
        }
        after = entries.last().map(|entry| entry.key.clone());
        for entry in entries {
            snapshot.instance_kv.push(serde_json::to_value(entry)?);
        }
    }
    if let Some(scope) = effect_archive(storage, instance).await? {
        let mut after = None;
        loop {
            let page = storage
                .page_effect_receipts(
                    &scope.owner.tenant_id,
                    scope.owner.continuity_id,
                    after.as_ref(),
                    PAGE,
                )
                .await?;
            for receipt in page.receipts {
                snapshot
                    .effect_receipts
                    .push(serde_json::to_value(receipt)?);
            }
            after = page.next;
            if after.is_none() {
                break;
            }
        }
        snapshot_workers(storage, instance, Some(&scope), snapshot).await?;
        snapshot.effect_scopes.push(serde_json::to_value(scope)?);
    } else {
        snapshot_workers(storage, instance, None, snapshot).await?;
    }
    Ok(())
}

#[cfg(test)]
async fn snapshot_workers(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    scope: Option<&EffectArchive>,
    snapshot: &mut Snapshot,
) -> Result<()> {
    let mut after = None;
    loop {
        let page = worker_task_archive_page(storage, instance, scope, after, PAGE).await?;
        after = page.last().map(|record| record.task.id);
        if page.is_empty() {
            return Ok(());
        }
        for record in page {
            snapshot.worker_tasks.push(serde_json::to_value(record)?);
        }
    }
}

async fn worker_task_archive_page(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    scope: Option<&EffectArchive>,
    after: Option<uuid::Uuid>,
    limit: u32,
) -> Result<Vec<WorkerTaskArchive>> {
    let records = storage
        .list_worker_task_archive_page(instance, after, limit)
        .await?;
    for record in &records {
        let receipt = match record.task.effect_id {
            Some(id) => storage.get_effect_receipt(&instance.tenant_id, id).await?,
            None => None,
        };
        record
            .validate(instance, scope, receipt.as_ref())
            .with_context(|| format!("back up worker task {}", record.task.id))?;
    }
    Ok(records)
}

#[cfg(test)]
fn jsonl(records: &[Value]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for record in records {
        serde_json::to_writer(&mut out, record)?;
        out.push(b'\n');
    }
    Ok(out)
}

/// Build the archive bytes for a snapshot.
#[cfg(test)]
pub fn write_archive(snap: &Snapshot, source_backend: &str) -> Result<Vec<u8>> {
    write_archive_with_key(snap, source_backend, None)
}

#[cfg(test)]
fn write_archive_with_key(
    snap: &Snapshot,
    source_backend: &str,
    signing_key: Option<&SigningKey>,
) -> Result<Vec<u8>> {
    let includes_instances = snap.instances.is_some();
    if snap.execution_tree.is_some() != includes_instances
        || snap.block_outputs.is_some() != includes_instances
        || (!includes_instances
            && (!snap.externalized_state.is_empty()
                || !snap.checkpoints.is_empty()
                || !snap.signals.is_empty()
                || !snap.instance_kv.is_empty()
                || !snap.effect_scopes.is_empty()
                || !snap.effect_receipts.is_empty()
                || !snap.worker_tasks.is_empty()))
    {
        bail!(
            "instance snapshots require instances, execution trees, outputs and payloads together"
        );
    }
    let mut members: Vec<(String, &[Value])> = vec![
        ("sequences.jsonl".into(), &snap.sequences),
        ("triggers.jsonl".into(), &snap.triggers),
        ("cron.jsonl".into(), &snap.cron),
        ("queue_routing.jsonl".into(), &snap.queue_routing),
        ("credentials.jsonl".into(), &snap.credentials),
    ];
    if let (Some(i), Some(t), Some(o)) =
        (&snap.instances, &snap.execution_tree, &snap.block_outputs)
    {
        members.push(("instances.jsonl".into(), i));
        members.push(("execution_tree.jsonl".into(), t));
        members.push(("block_outputs.jsonl".into(), o));
        members.push(("externalized_state.jsonl".into(), &snap.externalized_state));
        members.push(("checkpoints.jsonl".into(), &snap.checkpoints));
        members.push(("signals.jsonl".into(), &snap.signals));
        members.push(("instance_kv.jsonl".into(), &snap.instance_kv));
        members.push(("effect_scopes.jsonl".into(), &snap.effect_scopes));
        members.push(("effect_receipts.jsonl".into(), &snap.effect_receipts));
        members.push((WORKER_TASKS.into(), &snap.worker_tasks));
    }
    let mut files = Vec::new();
    let mut bodies = Vec::new();
    for (name, records) in &members {
        let body = jsonl(records)?;
        files.push(ManifestFile {
            name: name.clone(),
            records: records.len() as u64,
            bytes: body.len() as u64,
            sha256: sha256_hex(&body),
        });
        bodies.push((name.clone(), body));
    }
    let manifest = Manifest {
        format: FORMAT.into(),
        format_version: if includes_instances {
            WORKER_TASK_FORMAT_VERSION
        } else {
            FORMAT_VERSION
        },
        created_at: chrono::Utc::now().to_rfc3339(),
        orch8_version: env!("CARGO_PKG_VERSION").into(),
        source_backend: source_backend.into(),
        storage_schema_version: orch8_storage::STORAGE_SCHEMA_VERSION,
        include_instances: snap.instances.is_some(),
        files,
        warnings: snap.warnings.clone(),
    };
    let manifest_bytes = format!("{}\n", serde_json::to_string_pretty(&manifest)?).into_bytes();
    if manifest_bytes.len() as u64 > MAX_MANIFEST_BYTES {
        bail!("backup manifest exceeds {MAX_MANIFEST_BYTES} bytes");
    }

    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut builder = tar::Builder::new(encoder);
    let mut append = |name: &str, data: &[u8]| -> Result<()> {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o600);
        header.set_mtime(u64::try_from(chrono::Utc::now().timestamp()).unwrap_or(0));
        header.set_entry_type(tar::EntryType::Regular);
        header.set_cksum();
        builder.append_data(&mut header, name, data)?;
        Ok(())
    };
    append("manifest.json", &manifest_bytes)?;
    if let Some(key) = signing_key {
        append(
            MANIFEST_SIGNATURE,
            &key.sign(&signature_message(&manifest_bytes)).to_bytes(),
        )?;
    }
    for (name, body) in &bodies {
        append(name, body)?;
    }
    let encoder = builder.into_inner()?;
    Ok(encoder.finish()?)
}

/// Verified archive members staged in a private, bounded temporary directory.
#[derive(Debug)]
pub struct Archive {
    pub manifest: Manifest,
    members: BTreeMap<String, StagedMember>,
    _staging: tempfile::TempDir,
}

#[derive(Debug)]
struct StagedMember {
    path: PathBuf,
    bytes: u64,
    sha256: String,
    records: u64,
}

impl Archive {
    fn records<T: serde::de::DeserializeOwned>(
        &self,
        name: &str,
    ) -> Result<impl Iterator<Item = Result<T>>> {
        let file = self
            .members
            .get(name)
            .map(|member| std::fs::File::open(&member.path))
            .transpose()
            .with_context(|| format!("open staged {name}"))?;
        let name = name.to_owned();
        Ok(file
            .map(BufReader::new)
            .into_iter()
            .flat_map(|reader| reader.split(b'\n'))
            .filter(|line| !line.as_ref().is_ok_and(Vec::is_empty))
            .enumerate()
            .map(move |(i, line)| {
                let line = line.with_context(|| format!("read staged {name}"))?;
                serde_json::from_slice(&line)
                    .with_context(|| format!("{name}: record {} is invalid", i + 1))
            }))
    }

    fn validate_record_types(&self) -> Result<()> {
        self.validate_records::<SequenceDefinition>("sequences.jsonl")?;
        self.validate_records::<CredentialDef>("credentials.jsonl")?;
        self.validate_records::<TriggerDef>("triggers.jsonl")?;
        self.validate_records::<CronSchedule>("cron.jsonl")?;
        self.validate_records::<QueueRoutingRule>("queue_routing.jsonl")?;
        self.validate_records::<TaskInstance>("instances.jsonl")?;
        self.validate_records::<ExecutionNode>("execution_tree.jsonl")?;
        self.validate_records::<BlockOutput>("block_outputs.jsonl")?;
        self.validate_records::<Checkpoint>("checkpoints.jsonl")?;
        self.validate_records::<Signal>("signals.jsonl")?;
        self.validate_records::<InstanceKvEntry>("instance_kv.jsonl")?;
        self.validate_records::<EffectArchive>("effect_scopes.jsonl")?;
        self.validate_records::<EffectReceipt>("effect_receipts.jsonl")?;
        self.validate_records::<WorkerTaskArchive>(WORKER_TASKS)?;
        self.validate_records::<orch8_storage::externalizing::ExternalizedEntry>(
            "externalized_state.jsonl",
        )
    }

    fn validate_records<T: serde::de::DeserializeOwned>(&self, name: &str) -> Result<()> {
        for record in self.records::<T>(name)? {
            record?;
        }
        Ok(())
    }
}

fn validate_manifest_layout(
    manifest: &Manifest,
    members: &BTreeMap<String, StagedMember>,
) -> Result<()> {
    if manifest.format != FORMAT {
        bail!("not an orch8 backup (format '{}')", manifest.format);
    }
    if manifest.format_version > WORKER_TASK_FORMAT_VERSION {
        bail!(
            "archive format version {} is newer than this binary supports ({WORKER_TASK_FORMAT_VERSION}); \
             upgrade orch8",
            manifest.format_version
        );
    }
    if manifest.format_version == 0 {
        bail!("unsupported archive format version 0");
    }
    if manifest.format_version >= 2 {
        let instance_members = [
            "instances.jsonl",
            "execution_tree.jsonl",
            "block_outputs.jsonl",
            "externalized_state.jsonl",
        ];
        for name in instance_members {
            if manifest.include_instances && !members.contains_key(name) {
                bail!("archive is missing {name}");
            }
            if !manifest.include_instances && members.contains_key(name) {
                bail!("archive contains {name} without include_instances");
            }
        }
    }
    let expects_history = manifest.format_version >= 3 && manifest.include_instances;
    for name in [
        "checkpoints.jsonl",
        "signals.jsonl",
        "instance_kv.jsonl",
        "effect_scopes.jsonl",
        "effect_receipts.jsonl",
    ] {
        match (expects_history, members.contains_key(name)) {
            (true, false) => bail!("archive is missing {name}"),
            (false, true) => bail!("archive contains {name} without format 3 instance history"),
            _ => {}
        }
    }
    let expects_worker_tasks =
        manifest.format_version >= WORKER_TASK_FORMAT_VERSION && manifest.include_instances;
    match (expects_worker_tasks, members.contains_key(WORKER_TASKS)) {
        (true, false) => bail!("archive is missing {WORKER_TASKS}"),
        (false, true) => bail!("archive contains {WORKER_TASKS} without format 5 instance history"),
        _ => {}
    }
    if members
        .keys()
        .any(|name| name == artifacts::REFERENCES || artifacts::member_digest(name).is_some())
        && (manifest.format_version < ARTIFACT_FORMAT_VERSION
            || !manifest.include_instances
            || !members.contains_key(artifacts::REFERENCES))
    {
        bail!(
            "archive artifacts require format 4 instance history and an artifact reference index"
        );
    }
    Ok(())
}

/// Read and verify an archive: known format/version, only expected regular
/// files, sizes bounded, every checksum and record count matching.
#[cfg(test)]
pub fn read_archive(bytes: &[u8]) -> Result<Archive> {
    read_archive_with_key(bytes, None)
}

#[cfg(test)]
fn read_archive_with_key(bytes: &[u8], trusted_key: Option<&VerifyingKey>) -> Result<Archive> {
    read_archive_from(bytes, trusted_key)
}

fn private_archive_staging() -> Result<tempfile::TempDir> {
    let mut builder = tempfile::Builder::new();
    builder.prefix("orch8-backup-");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        builder.permissions(std::fs::Permissions::from_mode(0o700));
    }
    builder
        .tempdir()
        .context("create private backup staging directory")
}

fn read_archive_from(reader: impl Read, trusted_key: Option<&VerifyingKey>) -> Result<Archive> {
    let staging = private_archive_staging()?;
    let input = BufReader::new(reader.take(MAX_ARCHIVE_BYTES as u64 + 1));
    let decoder = flate2::bufread::GzDecoder::new(input);
    let mut tar = tar::Archive::new(decoder);
    let mut members = BTreeMap::new();
    let mut total_uncompressed = 0_u64;
    for entry in tar.entries().context("not a tar.gz archive")?.raw(true) {
        let mut entry = entry.context("corrupt archive entry")?;
        if entry.header().entry_type() != tar::EntryType::Regular {
            bail!("unexpected non-file entry in archive");
        }
        let path = entry.path()?.to_string_lossy().into_owned();
        if path.contains('/') || path.contains('\\') || path.starts_with('.') {
            bail!("unexpected archive member '{path}'");
        }
        let blob = artifacts::member_digest(&path).is_some();
        if !KNOWN_ARCHIVE_MEMBERS.contains(&path.as_str()) && !blob {
            bail!("unexpected archive member '{path}'");
        }
        if members.contains_key(&path) {
            bail!("duplicate archive member '{path}'");
        }
        let size = entry.size();
        if path == MANIFEST_SIGNATURE && size != 64 {
            bail!("invalid backup manifest signature length");
        }
        if path == "manifest.json" && size > MAX_MANIFEST_BYTES {
            bail!("backup manifest exceeds {MAX_MANIFEST_BYTES} bytes");
        }
        if size > MAX_MEMBER_BYTES || (blob && size > artifacts::MAX_BLOB_BYTES) {
            bail!("archive member '{path}' is too large ({size} bytes)");
        }
        total_uncompressed = total_uncompressed
            .checked_add(size)
            .context("archive expanded size overflow")?;
        if total_uncompressed > MAX_TOTAL_UNCOMPRESSED_BYTES {
            bail!("archive expands beyond the {MAX_TOTAL_UNCOMPRESSED_BYTES} byte limit");
        }
        let member = stage_member(&mut entry, staging.path().join(&path), size, blob)
            .with_context(|| format!("stage {path}"))?;
        members.insert(path, member);
    }
    // Tar may stop at the first zero block. Consume the gzip trailer to
    // validate its checksum, rejecting unexpected data and excessive padding.
    let mut decoder = tar.into_inner();
    validate_archive_tail(&mut decoder)?;
    let mut input = decoder.into_inner();
    let has_trailing_data = !input.fill_buf()?.is_empty();
    if input.get_ref().limit() == 0 {
        bail!("compressed archive exceeds {MAX_ARCHIVE_BYTES} bytes");
    }
    if has_trailing_data {
        bail!("unexpected data after the backup gzip stream");
    }

    let manifest = members
        .remove("manifest.json")
        .context("archive has no manifest.json")?;
    let manifest_bytes = std::fs::read(manifest.path).context("read staged manifest")?;
    let signature = members
        .remove(MANIFEST_SIGNATURE)
        .map(|member| std::fs::read(member.path))
        .transpose()
        .context("read staged signature")?;
    verify_manifest_signature(&manifest_bytes, signature.as_deref(), trusted_key)?;
    let manifest: Manifest =
        serde_json::from_slice(&manifest_bytes).context("manifest.json is invalid")?;
    validate_manifest_layout(&manifest, &members)?;
    for file in &manifest.files {
        let member = members
            .get(&file.name)
            .with_context(|| format!("archive is missing {}", file.name))?;
        if member.sha256 != file.sha256 || member.bytes != file.bytes {
            bail!(
                "checksum mismatch for {} — the archive is corrupt or was modified",
                file.name
            );
        }
        if member.records != file.records {
            bail!(
                "{} has {} records; manifest says {}",
                file.name,
                member.records,
                file.records
            );
        }
    }
    for name in members.keys() {
        if !manifest.files.iter().any(|f| &f.name == name) {
            bail!("archive member '{name}' is not listed in the manifest");
        }
    }
    let archive = Archive {
        manifest,
        members,
        _staging: staging,
    };
    artifacts::verify(&archive)?;
    Ok(archive)
}

fn create_private_archive_member(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    options.open(path)
}

fn stage_member(
    mut reader: impl Read,
    path: PathBuf,
    bytes: u64,
    blob: bool,
) -> Result<StagedMember> {
    let mut file = create_private_archive_member(&path)?;
    let mut hash = Sha256::new();
    let mut records = 0;
    let mut open_record = false;
    let mut copied = 0;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let chunk = &buffer[..count];
        copied += count as u64;
        if copied > bytes {
            bail!("archive member exceeds its declared size");
        }
        file.write_all(chunk)?;
        hash.update(chunk);
        if !blob {
            for segment in chunk.split_inclusive(|byte| *byte == b'\n') {
                if segment.last() == Some(&b'\n') {
                    records += u64::from(open_record || segment.len() > 1);
                    open_record = false;
                } else {
                    open_record = true;
                }
            }
        }
    }
    if copied != bytes {
        bail!("archive member ended before its declared size");
    }
    file.flush()?;
    Ok(StagedMember {
        path,
        bytes,
        sha256: hex_bytes(&hash.finalize()),
        records: if blob {
            1
        } else {
            records + u64::from(open_record)
        },
    })
}

fn validate_archive_tail(mut reader: impl Read) -> Result<()> {
    const MAX_PADDING: usize = 10 * 1024;
    let mut buffer = [0; 1024];
    let mut total = 0;
    loop {
        let count = reader
            .read(&mut buffer)
            .context("verify backup gzip trailer")?;
        if count == 0 {
            return Ok(());
        }
        total += count;
        if total > MAX_PADDING || buffer[..count].iter().any(|byte| *byte != 0) {
            bail!("unexpected data after the backup tar entries");
        }
    }
}

fn signature_message(manifest: &[u8]) -> Vec<u8> {
    let mut message = b"orch8 backup manifest ed25519 v1\0".to_vec();
    message.extend_from_slice(manifest);
    message
}

fn verify_manifest_signature(
    manifest: &[u8],
    signature: Option<&[u8]>,
    trusted_key: Option<&VerifyingKey>,
) -> Result<()> {
    match (signature, trusted_key) {
        (Some(signature), Some(key)) => {
            let signature =
                Signature::from_slice(signature).context("invalid backup manifest signature")?;
            key.verify_strict(&signature_message(manifest), &signature)
                .context("backup manifest signature verification failed")
        }
        (Some(_), None) => bail!("signed backup requires --trusted-key-file"),
        (None, Some(_)) => bail!("trusted-key restore requires a signed backup"),
        (None, None) => Ok(()),
    }
}

fn read_backup_key(path: &std::path::Path) -> Result<[u8; 32]> {
    let mut raw = String::new();
    std::fs::File::open(path)
        .with_context(|| format!("open backup key file {}", path.display()))?
        .take(257)
        .read_to_string(&mut raw)
        .context("read backup key file")?;
    if raw.len() > 256 {
        bail!("backup key file exceeds 256 bytes");
    }
    BASE64
        .decode(raw.trim())
        .map_err(|_| anyhow::anyhow!("backup key must be base64"))?
        .try_into()
        .map_err(|_| anyhow::anyhow!("backup key must contain exactly 32 bytes"))
}

/// Per-kind restore counts.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct RestoreCount {
    pub total: u64,
    pub created: u64,
    pub existing: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RestoreReport {
    pub dry_run: bool,
    pub counts: BTreeMap<String, RestoreCount>,
    pub warnings: Vec<String>,
}

impl RestoreReport {
    fn tally(&mut self, kind: &str, exists: bool) {
        let c = self.counts.entry(kind.into()).or_default();
        c.total += 1;
        if exists {
            c.existing += 1;
        } else {
            c.created += 1;
        }
    }
}

/// Restore into `storage`. Idempotent: existing records are skipped.
#[allow(clippy::too_many_lines)]
pub async fn restore(
    archive: &Archive,
    storage: &dyn StorageBackend,
    dry_run: bool,
    include_instances: bool,
) -> Result<RestoreReport> {
    let snapshots = if include_instances && archive.members.contains_key("instances.jsonl") {
        instance_snapshots(archive)?
    } else {
        Vec::new()
    };
    let mut report = RestoreReport {
        dry_run,
        warnings: archive.manifest.warnings.clone(),
        ..RestoreReport::default()
    };

    for seq in archive.records::<SequenceDefinition>("sequences.jsonl")? {
        let seq = seq?;
        let exists = storage.get_sequence(seq.id).await?.is_some();
        if !exists && !dry_run {
            storage
                .create_sequence(&seq)
                .await
                .with_context(|| format!("restore sequence {} v{}", seq.name, seq.version))?;
        }
        report.tally("sequences", exists);
    }
    for credential in archive.records::<CredentialDef>("credentials.jsonl")? {
        let credential = credential?;
        let tenant = orch8_types::ids::TenantId::unchecked(credential.tenant_id.clone());
        let exists = storage
            .get_credential(Some(&tenant), &credential.id)
            .await?
            .is_some_and(|c| c.tenant_id == credential.tenant_id);
        if !exists && !dry_run {
            storage
                .create_credential(&credential)
                .await
                .with_context(|| format!("restore credential {}", credential.id))?;
        }
        report.tally("credentials", exists);
    }
    for trigger in archive.records::<TriggerDef>("triggers.jsonl")? {
        let trigger = trigger?;
        let exists = storage
            .get_trigger(Some(&trigger.tenant_id), &trigger.slug)
            .await?
            .is_some();
        if !exists && !dry_run {
            storage
                .create_trigger(&trigger)
                .await
                .with_context(|| format!("restore trigger {}", trigger.slug))?;
        }
        report.tally("triggers", exists);
    }
    for schedule in archive.records::<CronSchedule>("cron.jsonl")? {
        let schedule = schedule?;
        let exists = storage.get_cron_schedule(schedule.id).await?.is_some();
        if !exists && !dry_run {
            storage
                .create_cron_schedule(&schedule)
                .await
                .with_context(|| format!("restore cron schedule {}", schedule.id))?;
        }
        report.tally("cron", exists);
    }
    for rule in archive.records::<QueueRoutingRule>("queue_routing.jsonl")? {
        let rule = rule?;
        let exists = storage.get_queue_routing_rule(rule.id).await?.is_some();
        if !exists && !dry_run {
            storage
                .create_queue_routing_rule(&rule)
                .await
                .with_context(|| format!("restore queue routing rule {}", rule.id))?;
        }
        report.tally("queue_routing", exists);
    }

    let has_instances = archive.members.contains_key("instances.jsonl");
    if has_instances && !include_instances {
        report.warnings.push(
            "the archive contains instances; pass --include-instances to restore them".into(),
        );
    }
    if has_instances && include_instances {
        if !archive.members.contains_key("externalized_state.jsonl") {
            report.warnings.push("version 1 backups omit externalized payloads; referenced context or outputs may be incomplete".into());
        }
        if !archive.members.contains_key("checkpoints.jsonl") {
            report
                .warnings
                .push("older backups omit execution checkpoints".into());
        }
        if !archive.members.contains_key("signals.jsonl") {
            report
                .warnings
                .push("older backups omit the signal inbox".into());
        }
        if !archive.members.contains_key("instance_kv.jsonl") {
            report
                .warnings
                .push("older backups omit the instance KV store".into());
        }
        if !archive.members.contains_key("effect_receipts.jsonl") {
            report
                .warnings
                .push("older backups omit the effect ledger".into());
        }
        if !archive.members.contains_key(WORKER_TASKS) {
            report
                .warnings
                .push("older backups omit queued worker tasks".into());
        }
        for archived in snapshots {
            let snapshot = archived.snapshot;
            let exists = if dry_run {
                storage.get_instance(snapshot.instance.id).await?.is_some()
            } else if archived.history.is_empty() {
                !storage
                    .restore_instance_snapshot(&snapshot)
                    .await
                    .with_context(|| format!("restore instance {}", snapshot.instance.id))?
            } else {
                !storage
                    .restore_instance_snapshot_with_history(&snapshot, &archived.history)
                    .await
                    .with_context(|| {
                        format!("restore instance {} with history", snapshot.instance.id)
                    })?
            };
            report.tally("instances", exists);
            for _ in &archived.history.checkpoints {
                report.tally("checkpoints", exists);
            }
            for _ in &archived.history.signals {
                report.tally("signals", exists);
            }
            for _ in &archived.history.state {
                report.tally("instance_kv", exists);
            }
            for _ in &archived.history.worker_tasks {
                report.tally("worker_tasks", exists);
            }
            if let Some(effects) = &archived.history.effects {
                report.tally("effect_scopes", exists);
                for _ in &effects.state.receipts {
                    report.tally("effect_receipts", exists);
                }
            }
            for _ in &snapshot.nodes {
                report.tally("execution_tree", exists);
            }
            for _ in &snapshot.outputs {
                report.tally("block_outputs", exists);
            }
            for _ in &snapshot.payloads {
                report.tally("externalized_state", exists);
            }
        }
    }
    Ok(report)
}

struct ArchivedInstance {
    snapshot: orch8_storage::restore::InstanceSnapshot,
    history: InstanceHistory,
}

fn instance_snapshots(archive: &Archive) -> Result<Vec<ArchivedInstance>> {
    let mut snapshots = Vec::new();
    let mut positions = BTreeMap::new();
    for instance in archive.records::<TaskInstance>("instances.jsonl")? {
        let instance = instance?;
        if positions.insert(instance.id, snapshots.len()).is_some() {
            bail!("archive contains duplicate instance ids");
        }
        snapshots.push(ArchivedInstance {
            snapshot: orch8_storage::restore::InstanceSnapshot {
                instance,
                nodes: Vec::new(),
                outputs: Vec::new(),
                payloads: Vec::new(),
            },
            history: InstanceHistory::default(),
        });
    }
    for node in archive.records::<ExecutionNode>("execution_tree.jsonl")? {
        let node = node?;
        let i = positions
            .get(&node.instance_id)
            .context("execution node has no archived instance")?;
        snapshots[*i].snapshot.nodes.push(node);
    }
    for output in archive.records::<BlockOutput>("block_outputs.jsonl")? {
        let output = output?;
        let i = positions
            .get(&output.instance_id)
            .context("output has no archived instance")?;
        snapshots[*i].snapshot.outputs.push(output);
    }
    for entry in archive
        .records::<orch8_storage::externalizing::ExternalizedEntry>("externalized_state.jsonl")?
    {
        let entry = entry?;
        let i = positions
            .get(&entry.instance_id)
            .context("payload has no archived instance")?;
        snapshots[*i].snapshot.payloads.push(entry);
    }
    let mut checkpoint_ids = std::collections::HashSet::new();
    for checkpoint in archive.records::<Checkpoint>("checkpoints.jsonl")? {
        let checkpoint = checkpoint?;
        let i = positions
            .get(&checkpoint.instance_id)
            .context("checkpoint has no archived instance")?;
        if !checkpoint_ids.insert(checkpoint.id) {
            bail!("archive contains duplicate checkpoint ids");
        }
        snapshots[*i].history.checkpoints.push(checkpoint);
    }
    let mut signal_ids = std::collections::HashSet::new();
    for signal in archive.records::<Signal>("signals.jsonl")? {
        let signal = signal?;
        let i = positions
            .get(&signal.instance_id)
            .context("signal has no archived instance")?;
        if !signal_ids.insert(signal.id) {
            bail!("archive contains duplicate signal ids");
        }
        snapshots[*i].history.signals.push(signal);
    }
    for entry in archive.records::<InstanceKvEntry>("instance_kv.jsonl")? {
        let entry = entry?;
        let i = positions
            .get(&entry.instance_id)
            .context("state entry has no archived instance")?;
        snapshots[*i].history.state.push(entry);
    }
    attach_effects(archive, &positions, &mut snapshots)?;
    let mut task_ids = std::collections::HashSet::new();
    for record in archive.records::<WorkerTaskArchive>(WORKER_TASKS)? {
        let record = record?;
        let i = positions
            .get(&record.task.instance_id)
            .context("worker task has no archived instance")?;
        if !task_ids.insert(record.task.id) {
            bail!("archive contains duplicate worker task ids");
        }
        snapshots[*i].history.worker_tasks.push(record);
    }
    for archived in &snapshots {
        archived.snapshot.validate()?;
        archived.history.validate_snapshot(&archived.snapshot)?;
    }
    Ok(snapshots)
}

fn attach_effects(
    archive: &Archive,
    positions: &BTreeMap<orch8_types::ids::InstanceId, usize>,
    snapshots: &mut [ArchivedInstance],
) -> Result<()> {
    let mut scopes = BTreeMap::new();
    for scope in archive.records::<EffectArchive>("effect_scopes.jsonl")? {
        let scope = scope?;
        let i = *positions
            .get(&scope.owner.current_instance_id)
            .context("effect ledger has no archived instance")?;
        if !scope.state.receipts.is_empty() {
            bail!("scope member contains inline effect receipts");
        }
        if scopes.insert(scope.owner.continuity_id, i).is_some()
            || snapshots[i].history.effects.replace(scope).is_some()
        {
            bail!("archive contains duplicate effect ledger owners");
        }
    }
    let mut receipts = std::collections::HashSet::new();
    for receipt in archive.records::<EffectReceipt>("effect_receipts.jsonl")? {
        let receipt = receipt?;
        let i = *scopes
            .get(&receipt.continuity_id)
            .context("effect receipt has no archived scope")?;
        if !receipts.insert(receipt.id) {
            bail!("archive contains duplicate effect receipt ids");
        }
        let ledger = snapshots[i]
            .history
            .effects
            .as_mut()
            .context("effect receipt has no archived owner")?;
        ledger.state.receipts.push(receipt);
    }
    Ok(())
}

fn render_restore(report: &RestoreReport) -> String {
    let rows: Vec<Vec<String>> = report
        .counts
        .iter()
        .map(|(kind, c)| {
            vec![
                kind.clone(),
                c.total.to_string(),
                c.created.to_string(),
                c.existing.to_string(),
            ]
        })
        .collect();
    let created_header = if report.dry_run {
        "would create"
    } else {
        "created"
    };
    crate::format_table(&["kind", "total", created_header, "already present"], &rows)
}

pub async fn run_backup(cmd: BackupCmd, format: OutputFormat) -> Result<()> {
    age::validate_recipients(&cmd.encrypt_recipients)?;
    let signing_key = cmd
        .signing_key_file
        .as_deref()
        .map(read_backup_key)
        .transpose()?
        .map(|bytes| SigningKey::from_bytes(&bytes));
    let target = parse_target(&cmd.database_url)?;
    let backend = match target {
        DbTarget::Postgres(_) => "postgres",
        DbTarget::Sqlite(_) => "sqlite",
    };
    let artifact_store = cmd
        .artifacts_dir
        .as_deref()
        .map(|path| {
            if !path.is_dir() {
                bail!("artifact source directory does not exist");
            }
            artifacts::open(path)
        })
        .transpose()?;
    let mut spool = stream::BackupSpool::new(cmd.include_instances)?;
    if artifact_store.is_some() {
        spool.enable_artifacts()?;
    }
    let (storage, source_guard) = open_source(&target).await?;
    let result = spool
        .collect(storage.as_ref(), &source_guard, artifact_store.as_ref())
        .await;
    // Release the consistent source before compression and publication.
    source_guard.close().await;
    result?;
    let (manifest, compressed_size) = if cmd.encrypt_recipients.is_empty() {
        spool.publish(&cmd.out, backend, signing_key.as_ref())?
    } else {
        let staging = private_archive_staging()?;
        let unencrypted = staging.path().join("archive.tar.gz");
        let (manifest, _) = spool.publish(&unencrypted, backend, signing_key.as_ref())?;
        let encrypted_size = age::encrypt(&unencrypted, &cmd.out, &cmd.encrypt_recipients)?;
        (manifest, encrypted_size)
    };
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&manifest)?),
        OutputFormat::Table => {
            let rows: Vec<Vec<String>> = manifest
                .files
                .iter()
                .map(|f| {
                    vec![
                        f.name.clone(),
                        f.records.to_string(),
                        f.sha256[..16].to_string(),
                    ]
                })
                .collect();
            print!(
                "{}",
                crate::format_table(&["file", "records", "sha256"], &rows)
            );
            println!(
                "wrote {} ({} bytes, mode 0600)",
                cmd.out.display(),
                compressed_size
            );
        }
    }
    for warning in &manifest.warnings {
        eprintln!("warning: {warning}");
    }
    Ok(())
}

pub async fn run_restore(cmd: RestoreCmd, format: OutputFormat) -> Result<()> {
    let trusted_key = cmd
        .trusted_key_file
        .as_deref()
        .map(read_backup_key)
        .transpose()?
        .map(|bytes| VerifyingKey::from_bytes(&bytes))
        .transpose()
        .context("invalid trusted backup public key")?;
    let file = std::fs::File::open(&cmd.archive)
        .with_context(|| format!("open {}", cmd.archive.display()))?;
    let archive = age::read(file, &cmd.identities, trusted_key.as_ref())?;
    archive.validate_record_types()?;
    let target = parse_target(&cmd.database_url)?;
    if !cmd.dry_run {
        crate::confirm_destructive(&format!(
            "restore {} into {}",
            cmd.archive.display(),
            match &target {
                DbTarget::Postgres(_) => "the PostgreSQL database".to_string(),
                DbTarget::Sqlite(p) => p.display().to_string(),
            }
        ))?;
    }
    let restore_artifacts =
        cmd.include_instances && archive.members.contains_key(artifacts::REFERENCES);
    if restore_artifacts && !cmd.dry_run && cmd.artifacts_dir.is_none() {
        bail!("backup contains artifacts; pass --artifacts-dir for the target store");
    }
    let (storage, guard) = open_restore_target(&target, cmd.dry_run).await?;
    let artifact_counts = if restore_artifacts {
        let destination = cmd
            .artifacts_dir
            .as_deref()
            .filter(|path| !cmd.dry_run || path.is_dir())
            .map(artifacts::open)
            .transpose()?;
        artifacts::restore(&archive, destination.as_ref(), cmd.dry_run)
            .await?
            .counts
    } else {
        BTreeMap::new()
    };
    let result = restore(
        &archive,
        storage.as_ref(),
        cmd.dry_run,
        cmd.include_instances,
    )
    .await;
    guard.close().await;
    let mut report = result?;
    report.counts.extend(artifact_counts);
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        OutputFormat::Table => {
            print!("{}", render_restore(&report));
            if cmd.dry_run {
                println!("dry run: nothing was written");
            }
        }
    }
    for warning in &report.warnings {
        eprintln!("warning: {warning}");
    }
    Ok(())
}

#[cfg(test)]
#[path = "backup_tests.rs"]
mod tests;
