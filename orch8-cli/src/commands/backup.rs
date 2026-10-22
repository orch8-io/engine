//! `orch8 backup` / `orch8 restore` — logical export/import of engine
//! configuration (and optionally instances) for `SQLite` and `PostgreSQL`.
//!
//! Archive format (`format_version` 2; version 1 remains readable): a `.tar.gz` containing
//! `manifest.json` plus one JSON-lines file per kind (`sequences.jsonl`,
//! `triggers.jsonl`, `cron.jsonl`, `queue_routing.jsonl`,
//! `credentials.jsonl`, and with `--include-instances` `instances.jsonl`,
//! `execution_tree.jsonl`, `block_outputs.jsonl`, `externalized_state.jsonl`). The manifest records the
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
use std::io::Read as _;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use sqlx::Connection as _;

use orch8_storage::StorageBackend;
use orch8_types::credential::CredentialDef;
use orch8_types::cron::CronSchedule;
use orch8_types::execution::ExecutionNode;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::instance::TaskInstance;
use orch8_types::output::BlockOutput;
use orch8_types::queue_routing::QueueRoutingRule;
use orch8_types::sequence::SequenceDefinition;
use orch8_types::trigger::TriggerDef;

use super::upgrade::{DbTarget, parse_target};
use crate::OutputFormat;

pub const FORMAT: &str = "orch8-backup";
pub const FORMAT_VERSION: u32 = 2;
/// Storage list calls without offsets cap at this many rows.
const PAGE: u32 = 500;
/// Restore verifies archives in memory, so bound both the compressed input and
/// the total expanded data before allocating attacker-controlled sizes.
const MAX_ARCHIVE_BYTES: usize = 512 * 1024 * 1024;
const MAX_MEMBER_BYTES: u64 = 512 * 1024 * 1024;
const MAX_TOTAL_UNCOMPRESSED_BYTES: u64 = 512 * 1024 * 1024;
const KNOWN_ARCHIVE_MEMBERS: &[&str] = &[
    "manifest.json",
    "sequences.jsonl",
    "credentials.jsonl",
    "triggers.jsonl",
    "cron.jsonl",
    "queue_routing.jsonl",
    "instances.jsonl",
    "execution_tree.jsonl",
    "block_outputs.jsonl",
    "externalized_state.jsonl",
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
    /// Archive to write (`.tar.gz`).
    #[arg(long)]
    pub out: PathBuf,
    /// Also export instances with their execution trees and block outputs.
    #[arg(long)]
    pub include_instances: bool,
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
    pub warnings: Vec<String>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
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

/// Open the source for reading. A read-only `SQLite` connection makes a
/// transactionally consistent snapshot, including committed WAL frames.
/// Independent file copies can lose commits when a live writer checkpoints.
/// Schema reconciliation is confined to the temporary snapshot. `PostgreSQL`
/// reads share a dedicated read-only repeatable-read transaction.
async fn open_source(
    target: &DbTarget,
) -> Result<(Arc<dyn StorageBackend>, Option<tempfile::TempDir>)> {
    match target {
        DbTarget::Postgres(url) => Ok((
            Arc::new(
                orch8_storage::postgres::PostgresStorage::read_snapshot(url, None)
                    .await
                    .context("connect to PostgreSQL")?,
            ),
            None,
        )),
        DbTarget::Sqlite(path) => {
            if !path.is_file() {
                bail!("SQLite database {} does not exist", path.display());
            }
            let dir = tempfile::tempdir().context("create snapshot directory")?;
            let copy = dir.path().join("snapshot.db");
            let options = sqlx::sqlite::SqliteConnectOptions::new()
                .filename(path)
                .read_only(true);
            let mut source = sqlx::SqliteConnection::connect_with(&options)
                .await
                .with_context(|| format!("open {} read-only for backup", path.display()))?;
            sqlx::query("VACUUM main INTO ?1")
                .bind(copy.to_str().context("non-UTF-8 snapshot path")?)
                .execute(&mut source)
                .await
                .with_context(|| format!("snapshot {} consistently", path.display()))?;
            source.close().await.context("close SQLite backup source")?;
            let storage = orch8_storage::sqlite::SqliteStorage::file(
                copy.to_str().context("non-UTF-8 temp path")?,
            )
            .await
            .context("open SQLite snapshot")?;
            Ok((Arc::new(storage), Some(dir)))
        }
    }
}

/// Open the restore target. `PostgreSQL` must be fully migrated; `SQLite` is
/// created/reconciled like on server boot.
async fn open_target(target: &DbTarget) -> Result<Arc<dyn StorageBackend>> {
    match target {
        DbTarget::Postgres(url) => {
            let report = super::upgrade::check(target).await?;
            if !report.is_current() {
                bail!(
                    "the target database has {} pending migration(s); run `orch8 migrate` first \
                     (see `orch8 upgrade --check`)",
                    report.pending.len()
                );
            }
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

/// Read the complete payload catalog for one instance using bounded pages.
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
                for node in storage.get_execution_tree(instance.id).await? {
                    tree.push(serde_json::to_value(&node)?);
                }
                for output in storage.get_all_outputs(instance.id).await? {
                    outputs.push(serde_json::to_value(&output)?);
                }
                snap.externalized_state
                    .extend(snapshot_payloads(storage, instance.id).await?);
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
        snap.warnings.push(
            "instances are exported with their execution trees and block outputs; signals, \
             worker tasks, KV state, checkpoints, and audit history are not included"
                .into(),
        );
    }
    Ok(snap)
}

fn jsonl(records: &[Value]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for record in records {
        serde_json::to_writer(&mut out, record)?;
        out.push(b'\n');
    }
    Ok(out)
}

/// Build the archive bytes for a snapshot.
pub fn write_archive(snap: &Snapshot, source_backend: &str) -> Result<Vec<u8>> {
    let includes_instances = snap.instances.is_some();
    if snap.execution_tree.is_some() != includes_instances
        || snap.block_outputs.is_some() != includes_instances
        || (!includes_instances && !snap.externalized_state.is_empty())
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
        format_version: FORMAT_VERSION,
        created_at: chrono::Utc::now().to_rfc3339(),
        orch8_version: env!("CARGO_PKG_VERSION").into(),
        source_backend: source_backend.into(),
        storage_schema_version: orch8_storage::STORAGE_SCHEMA_VERSION,
        include_instances: snap.instances.is_some(),
        files,
        warnings: snap.warnings.clone(),
    };
    let manifest_bytes = format!("{}\n", serde_json::to_string_pretty(&manifest)?).into_bytes();

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
    for (name, body) in &bodies {
        append(name, body)?;
    }
    let encoder = builder.into_inner()?;
    Ok(encoder.finish()?)
}

/// A verified, in-memory archive.
#[derive(Debug)]
pub struct Archive {
    pub manifest: Manifest,
    pub members: BTreeMap<String, Vec<u8>>,
}

impl Archive {
    fn records<T: serde::de::DeserializeOwned>(&self, name: &str) -> Result<Vec<T>> {
        let Some(body) = self.members.get(name) else {
            return Ok(Vec::new());
        };
        body.split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .enumerate()
            .map(|(i, line)| {
                serde_json::from_slice(line)
                    .with_context(|| format!("{name}: record {} is invalid", i + 1))
            })
            .collect()
    }
}

fn validate_manifest_layout(
    manifest: &Manifest,
    members: &BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    if manifest.format != FORMAT {
        bail!("not an orch8 backup (format '{}')", manifest.format);
    }
    if manifest.format_version > FORMAT_VERSION {
        bail!(
            "archive format version {} is newer than this binary supports ({FORMAT_VERSION}); \
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
    Ok(())
}

/// Read and verify an archive: known format/version, only expected regular
/// files, sizes bounded, every checksum and record count matching.
pub fn read_archive(bytes: &[u8]) -> Result<Archive> {
    if bytes.len() > MAX_ARCHIVE_BYTES {
        bail!(
            "compressed archive is too large ({} bytes; maximum {MAX_ARCHIVE_BYTES})",
            bytes.len()
        );
    }
    let decoder = flate2::read::GzDecoder::new(bytes);
    let mut tar = tar::Archive::new(decoder);
    let mut members = BTreeMap::new();
    let mut total_uncompressed = 0_u64;
    for entry in tar.entries().context("not a tar.gz archive")? {
        let mut entry = entry.context("corrupt archive entry")?;
        if entry.header().entry_type() != tar::EntryType::Regular {
            bail!("unexpected non-file entry in archive");
        }
        let path = entry.path()?.to_string_lossy().into_owned();
        if path.contains('/') || path.contains('\\') || path.starts_with('.') {
            bail!("unexpected archive member '{path}'");
        }
        if !KNOWN_ARCHIVE_MEMBERS.contains(&path.as_str()) {
            bail!("unexpected archive member '{path}'");
        }
        if members.contains_key(&path) {
            bail!("duplicate archive member '{path}'");
        }
        let size = entry.size();
        if size > MAX_MEMBER_BYTES {
            bail!("archive member '{path}' is too large ({size} bytes)");
        }
        total_uncompressed = total_uncompressed
            .checked_add(size)
            .context("archive expanded size overflow")?;
        if total_uncompressed > MAX_TOTAL_UNCOMPRESSED_BYTES {
            bail!("archive expands beyond the {MAX_TOTAL_UNCOMPRESSED_BYTES} byte limit");
        }
        let mut body = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
        entry
            .by_ref()
            .take(MAX_MEMBER_BYTES + 1)
            .read_to_end(&mut body)?;
        members.insert(path, body);
    }
    let manifest_bytes = members
        .remove("manifest.json")
        .context("archive has no manifest.json")?;
    let manifest: Manifest =
        serde_json::from_slice(&manifest_bytes).context("manifest.json is invalid")?;
    validate_manifest_layout(&manifest, &members)?;
    for file in &manifest.files {
        let body = members
            .get(&file.name)
            .with_context(|| format!("archive is missing {}", file.name))?;
        if sha256_hex(body) != file.sha256 || body.len() as u64 != file.bytes {
            bail!(
                "checksum mismatch for {} — the archive is corrupt or was modified",
                file.name
            );
        }
        let records = body
            .split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .count() as u64;
        if records != file.records {
            bail!(
                "{} has {records} records; manifest says {}",
                file.name,
                file.records
            );
        }
    }
    for name in members.keys() {
        if !manifest.files.iter().any(|f| &f.name == name) {
            bail!("archive member '{name}' is not listed in the manifest");
        }
    }
    Ok(Archive { manifest, members })
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
        for snapshot in snapshots {
            let exists = if dry_run {
                storage.get_instance(snapshot.instance.id).await?.is_some()
            } else {
                !storage
                    .restore_instance_snapshot(&snapshot)
                    .await
                    .with_context(|| format!("restore instance {}", snapshot.instance.id))?
            };
            report.tally("instances", exists);
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

fn instance_snapshots(archive: &Archive) -> Result<Vec<orch8_storage::restore::InstanceSnapshot>> {
    let mut snapshots = Vec::new();
    let mut positions = BTreeMap::new();
    for instance in archive.records::<TaskInstance>("instances.jsonl")? {
        if positions.insert(instance.id, snapshots.len()).is_some() {
            bail!("archive contains duplicate instance ids");
        }
        snapshots.push(orch8_storage::restore::InstanceSnapshot {
            instance,
            nodes: Vec::new(),
            outputs: Vec::new(),
            payloads: Vec::new(),
        });
    }
    for node in archive.records::<ExecutionNode>("execution_tree.jsonl")? {
        let i = positions
            .get(&node.instance_id)
            .context("execution node has no archived instance")?;
        snapshots[*i].nodes.push(node);
    }
    for output in archive.records::<BlockOutput>("block_outputs.jsonl")? {
        let i = positions
            .get(&output.instance_id)
            .context("output has no archived instance")?;
        snapshots[*i].outputs.push(output);
    }
    for entry in archive
        .records::<orch8_storage::externalizing::ExternalizedEntry>("externalized_state.jsonl")?
    {
        let i = positions
            .get(&entry.instance_id)
            .context("payload has no archived instance")?;
        snapshots[*i].payloads.push(entry);
    }
    for snapshot in &snapshots {
        snapshot.validate()?;
    }
    Ok(snapshots)
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
    let target = parse_target(&cmd.database_url)?;
    let backend = match target {
        DbTarget::Postgres(_) => "postgres",
        DbTarget::Sqlite(_) => "sqlite",
    };
    let (storage, _snapshot_dir) = open_source(&target).await?;
    let snap = snapshot(storage.as_ref(), cmd.include_instances).await?;
    let bytes = write_archive(&snap, backend)?;
    let archive = read_archive(&bytes)?;
    crate::atomic_write_private(&cmd.out, &bytes)?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&archive.manifest)?),
        OutputFormat::Table => {
            let rows: Vec<Vec<String>> = archive
                .manifest
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
                bytes.len()
            );
        }
    }
    for warning in &archive.manifest.warnings {
        eprintln!("warning: {warning}");
    }
    Ok(())
}

pub async fn run_restore(cmd: RestoreCmd, format: OutputFormat) -> Result<()> {
    let file = std::fs::File::open(&cmd.archive)
        .with_context(|| format!("open {}", cmd.archive.display()))?;
    let mut bytes = Vec::new();
    file.take((MAX_ARCHIVE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read {}", cmd.archive.display()))?;
    if bytes.len() > MAX_ARCHIVE_BYTES {
        bail!(
            "compressed archive is too large ({} bytes; maximum {MAX_ARCHIVE_BYTES})",
            bytes.len()
        );
    }
    let archive = read_archive(&bytes)?;
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
    let storage = open_target(&target).await?;
    let report = restore(
        &archive,
        storage.as_ref(),
        cmd.dry_run,
        cmd.include_instances,
    )
    .await?;
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
