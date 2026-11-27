//! `orch8 migrate --to <url>`: move sequences and in-flight instances from an
//! embedded (`SQLite`) engine to a remote engine without restarting runs.
//!
//! Per instance:
//! 1. **Idle check.** Refuse (or with `--wait-for-idle`, wait for) instances
//!    that are mid-step (`running`) or hold pending/claimed worker tasks.
//! 2. **Fence.** Move the source ownership record to `transferring` (the
//!    existing continuity fence: the source scheduler defers the instance and
//!    worker leases are refused), then re-check idleness.
//! 3. **Export + import.** Snapshot instance, execution tree, block outputs,
//!    effect receipts, pending signals and ownership record; POST them to the
//!    target's `/migrations/import` (idempotent).
//! 4. **Commit.** Record the target as owner at `epoch + 1` (state stays
//!    `transferring`, so the local copy never advances again), pause the
//!    local instance, and stamp `orch8_migration.phase = "committed"`.
//!
//! Every phase is recorded in the source database, so re-running the same
//! command resumes: committed instances are skipped, fenced ones are
//! re-exported (the target reports `already_present` if it has them).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
use orch8_types::continuity::{ContinuityExecution, OwnershipState, RuntimeId};
use orch8_types::cron::{CronMigrationAcknowledgement, CronMigrationSnapshot};
use orch8_types::error::StorageError;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::TenantId;
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::migration::{
    ImportStatus, ImportedHistoryInstance, MAX_SEQUENCES_PER_IMPORT, MIGRATION_ARTIFACTS_DOMAIN,
    MIGRATION_BASE_DOMAINS, MIGRATION_CHECKPOINT_DOMAIN, MIGRATION_CHUNK_BYTES,
    MIGRATION_CHUNKS_DOMAIN, MIGRATION_CRON_DOMAIN, MIGRATION_DIGEST_DOMAIN,
    MIGRATION_EXTERNALIZED_DOMAIN, MIGRATION_KV_DOMAIN, MIGRATION_MANIFEST_VERSION,
    MIGRATION_MAX_UPLOAD_BYTES, MIGRATION_METADATA_KEY, MIGRATION_TERMINAL_HISTORY_DOMAIN,
    MigratedInstance, MigratedInstanceState, MigrationCapabilities, MigrationDomainDigests,
    MigrationImportRequest, MigrationImportResponse, MigrationUploadCommit,
    StagedMigrationImportReceipt, TerminalHistoryImport, TerminalInstanceHistory,
    cron_schedule_digest, target_runtime_id,
};
use serde::Serialize;
use serde_json::json;

use crate::OutputFormat;

#[path = "cloud_migrate_artifacts.rs"]
mod artifacts;

/// Instances sent per import request (the server accepts up to 100).
const IMPORT_BATCH: usize = 25;
const DIRECT_IMPORT_BYTES: usize = 8 * 1024 * 1024;
const PAGE: u32 = 500;

#[derive(Debug, clap::Args)]
#[allow(clippy::struct_excessive_bools)] // These command-line switches are independent.
pub struct MigrateToArgs {
    /// Target engine API base URL (e.g. `https://cloud.orch8.io/api/v1`).
    /// Switches `orch8 migrate` from Postgres schema migration to moving an
    /// embedded engine's work to a remote engine.
    #[arg(long, value_name = "URL", requires = "source")]
    pub to: Option<String>,
    /// Source `SQLite` database (path or `sqlite://` URL) of the embedded engine.
    #[arg(long, value_name = "SQLITE")]
    pub source: Option<String>,
    /// Local source artifact-store directory.
    #[arg(long, requires = "source")]
    pub source_artifacts_dir: Option<PathBuf>,
    /// API key for the target (sent as `x-api-key`).
    #[arg(long, env = "ORCH8_TARGET_API_KEY", hide_env_values = true)]
    pub target_api_key: Option<String>,
    /// Tenant to migrate (default: `--tenant-id` / `ORCH8_TENANT_ID`, else `default`).
    #[arg(long)]
    pub tenant: Option<String>,
    /// Wait for busy instances (running step / leased worker task) to go
    /// idle instead of refusing them.
    #[arg(long)]
    pub wait_for_idle: bool,
    /// Maximum wait per busy instance with `--wait-for-idle`.
    #[arg(long, default_value_t = 300)]
    pub idle_timeout_secs: u64,
    /// Explicit migration id. Default: derived from source path, target URL
    /// and tenant, so re-running resumes the same migration.
    #[arg(long)]
    pub migration_id: Option<uuid::Uuid>,
    /// Report what would move without fencing or sending anything.
    #[arg(long)]
    pub dry_run: bool,
    /// Fence source executions and upload batches for separate target import.
    /// Leaves the source fenced until the migration is resumed.
    #[arg(long, requires = "to", conflicts_with_all = ["dry_run", "include_terminal_history", "include_cron_schedules"])]
    pub stage_only: bool,
    /// Copy completed, failed and cancelled runs as read-only history.
    #[arg(long)]
    pub include_terminal_history: bool,
    /// Transfer recurring schedules and permanently retire their source copies.
    #[arg(long, requires = "source")]
    pub include_cron_schedules: bool,
}

#[derive(Debug, Default, Serialize)]
pub struct MigrationReport {
    pub migration_id: String,
    pub target: String,
    pub tenant: String,
    pub dry_run: bool,
    pub stage_only: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub staged_uploads: Vec<StagedUploadReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staging_error: Option<String>,
    pub sequences_sent: usize,
    pub sequences_created: u64,
    pub migrated: Vec<String>,
    pub already_migrated: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub history_imported: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub history_already_present: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub cron_imported: Vec<uuid::Uuid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub cron_refused: Vec<CronRefusal>,
    pub refused: Vec<Refusal>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Refusal {
    pub instance_id: String,
    pub reason: String,
}

#[derive(Debug, Serialize)]
pub struct StagedUploadReport {
    pub digest: String,
    pub length: usize,
    pub sequences: usize,
    pub execution_ids: Vec<orch8_types::ids::InstanceId>,
}

#[derive(Debug, Serialize)]
pub struct CronRefusal {
    pub schedule_id: uuid::Uuid,
    pub reason: String,
}

fn target_base(url: &str) -> String {
    let url = url.trim().trim_end_matches('/');
    if url.ends_with("/api/v1") {
        url.to_owned()
    } else {
        format!("{url}/api/v1")
    }
}

fn source_path(raw: &str) -> String {
    raw.strip_prefix("sqlite://")
        .or_else(|| raw.strip_prefix("sqlite:"))
        .unwrap_or(raw)
        .split('?')
        .next()
        .unwrap_or(raw)
        .to_owned()
}

fn derive_migration_id(source: &str, target: &str, tenant: &str) -> uuid::Uuid {
    use sha2::{Digest, Sha256};
    let canonical = std::fs::canonicalize(source)
        .map_or_else(|_| source.to_owned(), |p| p.display().to_string());
    let digest = Sha256::digest(format!("orch8-migrate-v1\0{canonical}\0{target}\0{tenant}"));
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

fn phase(instance: &TaskInstance, migration: &str) -> Option<String> {
    let marker = instance.metadata.get(MIGRATION_METADATA_KEY)?;
    (marker.get("id")?.as_str()? == migration)
        .then(|| marker.get("phase")?.as_str().map(ToOwned::to_owned))
        .flatten()
}

async fn busy_reason(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
) -> Result<Option<String>> {
    if instance.state == InstanceState::Running {
        return Ok(Some("a step is executing (state=running)".into()));
    }
    let open = orch8_engine::capsule::open_worker_task_count(storage, instance.id).await?;
    Ok((open > 0)
        .then(|| format!("{open} pending/claimed worker task(s) (in-flight leased activity)")))
}

pub async fn run(
    args: MigrateToArgs,
    global_tenant: Option<&str>,
    format: OutputFormat,
) -> Result<()> {
    let report = migrate(&args, global_tenant).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        OutputFormat::Table => print_report(&report),
    }
    if report.staging_error.is_some() {
        bail!("migration staging stopped; resume with the same source, target and migration ID");
    }
    if !report.refused.is_empty() || !report.cron_refused.is_empty() {
        bail!(
            "{} instance(s) and {} schedule(s) were refused. Re-run to resume",
            report.refused.len(),
            report.cron_refused.len()
        );
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub async fn migrate(args: &MigrateToArgs, global_tenant: Option<&str>) -> Result<MigrationReport> {
    let to = args.to.as_deref().context("--to is required")?;
    if args.stage_only
        && (args.dry_run || args.include_terminal_history || args.include_cron_schedules)
    {
        bail!("staging only supports in-flight execution and sequence batches");
    }
    let source = source_path(args.source.as_deref().context("--source is required")?);
    if !std::path::Path::new(&source).is_file() {
        bail!("source SQLite database {source} does not exist");
    }
    let tenant_raw = args
        .tenant
        .as_deref()
        .or(global_tenant)
        .filter(|t| !t.is_empty())
        .unwrap_or("default");
    let tenant = TenantId::new(tenant_raw).map_err(anyhow::Error::msg)?;
    let base = target_base(to);
    let migration_id = args
        .migration_id
        .unwrap_or_else(|| derive_migration_id(&source, &base, tenant.as_str()));
    let mid = migration_id.to_string();
    let source_engine_id = format!(
        "sqlite:{}",
        PathBuf::from(&source)
            .file_name()
            .map_or_else(|| source.clone(), |n| n.to_string_lossy().into_owned())
    );

    let mut native = SqliteStorage::file(&source)
        .await
        .with_context(|| format!("open {source}"))?;
    if let Some(path) = &args.source_artifacts_dir {
        if !path.is_dir() {
            bail!("source artifact directory does not exist");
        }
        let store = orch8_storage::artifacts::ObjectArtifactStore::local(
            path.to_str().context("source artifact path is not UTF-8")?,
        )?;
        native = native.with_artifact_store(Arc::new(store));
    }
    let native = Arc::new(native);
    let mut storage: Arc<dyn StorageBackend> = native.clone();
    if let Some(key) = std::env::var_os("ORCH8_ENCRYPTION_KEY")
        && !key.is_empty()
    {
        let key = key.to_str().context("invalid source encryption key")?;
        let cipher = orch8_types::encryption::FieldEncryptor::from_hex_key(key)
            .context("invalid source encryption key")?;
        storage = Arc::new(orch8_storage::encrypting::EncryptingStorage::new(
            storage, cipher,
        ));
    }
    let client = crate::build_client(args.target_api_key.as_deref(), Some(tenant.as_str()))?;
    let capabilities = if args.dry_run {
        MigrationCapabilities::current()
    } else {
        read_target_capabilities(&client, &base).await?
    };
    if args.include_terminal_history && !capabilities.supports(MIGRATION_TERMINAL_HISTORY_DOMAIN, 1)
    {
        bail!("target cannot preserve terminal history format 1");
    }
    if args.include_cron_schedules && !capabilities.supports(MIGRATION_CRON_DOMAIN, 1) {
        bail!("target cannot preserve cron ownership format 1");
    }

    let destination = MigrationDestination {
        client: &client,
        base: &base,
        tenant: &tenant,
        chunked: capabilities.supports(MIGRATION_CHUNKS_DOMAIN, 1),
    };
    if args.stage_only && !destination.chunked {
        bail!("target does not support migration chunk staging");
    }

    let mut report = MigrationReport {
        migration_id: mid.clone(),
        target: base.clone(),
        tenant: tenant.to_string(),
        dry_run: args.dry_run,
        stage_only: args.stage_only,
        ..MigrationReport::default()
    };
    report
        .warnings
        .push("Audit history and step logs stay on the source".into());
    if args.stage_only {
        report.warnings.push("Import staged uploads in report order, then resume this migration without --stage-only. The source keeps staged executions fenced and their signal windows closed until the migration completes.".into());
    }

    // 1. Sequences (all versions of the tenant's sequences).
    let mut sequences = Vec::new();
    let mut offset = 0_u32;
    loop {
        let page = storage
            .list_sequences(Some(&tenant), None, PAGE, offset)
            .await
            .context("list sequences")?;
        let n = page.len();
        sequences.extend(page);
        if n < PAGE as usize {
            break;
        }
        offset += PAGE;
    }
    if !args.stage_only {
        report.sequences_sent = sequences.len();
    }
    if !args.dry_run {
        for chunk in sequences.chunks(MAX_SEQUENCES_PER_IMPORT) {
            let request = MigrationImportRequest {
                migration_id,
                source_engine_id: source_engine_id.clone(),
                sequences: chunk.to_vec(),
                instances: Vec::new(),
            };
            if args.stage_only {
                if !stage_for_review(&destination, &request, &mut report).await {
                    return Ok(report);
                }
            } else {
                let response = destination.send(&request).await?;
                report.sequences_created += response.sequences.created;
            }
        }
    }
    let mut history = TerminalHistoryTransfer {
        storage: storage.as_ref(),
        client: &client,
        base: &base,
        migration_id,
        copied: std::collections::HashSet::new(),
    };
    let deferred_history = if args.include_terminal_history {
        history.copy(&tenant, &mut report).await?
    } else {
        Vec::new()
    };
    let schedules = if args.include_cron_schedules {
        prepare_cron_schedules(&native, &tenant, migration_id, &mut report).await?
    } else {
        Vec::new()
    };

    // 2. In-flight instances.
    let filter = InstanceFilter {
        tenant_id: Some(tenant.clone()),
        states: Some(vec![
            InstanceState::Scheduled,
            InstanceState::Running,
            InstanceState::Waiting,
            InstanceState::Paused,
        ]),
        ..InstanceFilter::default()
    };
    let mut candidates = Vec::new();
    let mut offset = 0_u64;
    loop {
        let page = storage
            .list_instances(
                &filter,
                &Pagination {
                    offset,
                    limit: PAGE,
                    sort_ascending: true,
                },
            )
            .await
            .context("list instances")?;
        let n = page.len();
        candidates.extend(page);
        if n < PAGE as usize {
            break;
        }
        offset += u64::from(PAGE);
    }

    let mut batch: Vec<MigratedInstance> = Vec::new();
    let mut batch_bytes: usize = 0;
    for instance in candidates {
        let id = instance.id;
        if phase(&instance, &mid).as_deref() == Some("committed") {
            report.already_migrated.push(id.to_string());
            continue;
        }
        if args.dry_run {
            let refusal = if let Some(reason) = busy_reason(storage.as_ref(), &instance).await? {
                Some(format!("busy: {reason}"))
            } else {
                unsupported_state(storage.as_ref(), id, &capabilities).await?
            };
            if let Some(reason) = refusal {
                report.refused.push(Refusal {
                    instance_id: id.to_string(),
                    reason,
                });
            } else {
                report.migrated.push(id.to_string());
            }
            continue;
        }
        let fenced = fence(
            native.as_ref(),
            storage.as_ref(),
            &instance,
            &mid,
            args,
            &capabilities,
            &destination,
        )
        .await;
        let fenced = match fenced {
            Err(error) if args.stage_only => {
                report.staging_error = Some(error.to_string());
                return Ok(report);
            }
            result => result?,
        };
        match fenced {
            Ok(migrated) => {
                let encoded_bytes = count_encoded_bytes(&migrated)?;
                if !batch.is_empty()
                    && batch_bytes.saturating_add(encoded_bytes) > DIRECT_IMPORT_BYTES
                {
                    flush(
                        &destination,
                        storage.as_ref(),
                        migration_id,
                        &source_engine_id,
                        &mut batch,
                        &mut report,
                    )
                    .await?;
                    if report.staging_error.is_some() {
                        return Ok(report);
                    }
                    batch_bytes = 0;
                }
                batch_bytes = batch_bytes.saturating_add(encoded_bytes);
                batch.push(migrated);
            }
            Err(reason) => report.refused.push(Refusal {
                instance_id: id.to_string(),
                reason,
            }),
        }
        if batch.len() >= IMPORT_BATCH || batch_bytes >= DIRECT_IMPORT_BYTES {
            flush(
                &destination,
                storage.as_ref(),
                migration_id,
                &source_engine_id,
                &mut batch,
                &mut report,
            )
            .await?;
            if report.staging_error.is_some() {
                return Ok(report);
            }
            batch_bytes = 0;
        }
    }
    flush(
        &destination,
        storage.as_ref(),
        migration_id,
        &source_engine_id,
        &mut batch,
        &mut report,
    )
    .await?;
    if args.stage_only {
        return Ok(report);
    }
    for instance in deferred_history {
        let _ = history.copy_lineage(instance, false, &mut report).await?;
    }
    if report.refused.is_empty() {
        for snapshot in schedules {
            let ack: CronMigrationAcknowledgement =
                send_json(&client, &base, "/migrations/cron/v1", &snapshot).await?;
            if ack.schedule_id != snapshot.schedule.id
                || ack.migration_id != migration_id
                || Some(ack.ownership_epoch) != snapshot.source_epoch.checked_add(1)
                || ack.snapshot_sha256 != cron_schedule_digest(&snapshot)?
            {
                bail!(
                    "target returned an invalid ownership acknowledgement for cron schedule {}",
                    snapshot.schedule.id
                );
            }
            report.cron_imported.push(ack.schedule_id);
        }
    } else {
        report
            .cron_refused
            .extend(schedules.into_iter().map(|snapshot| CronRefusal {
                schedule_id: snapshot.schedule.id,
                reason: "source fenced; destination activation awaits in-flight runs".into(),
            }));
    }
    Ok(report)
}

async fn prepare_cron_schedules(
    source: &SqliteStorage,
    tenant: &TenantId,
    migration_id: uuid::Uuid,
    report: &mut MigrationReport,
) -> Result<Vec<CronMigrationSnapshot>> {
    use orch8_storage::SchedulingStore as _;
    let schedules = source.list_all_cron_schedules(Some(tenant)).await?;
    if report.dry_run {
        report
            .cron_imported
            .extend(schedules.iter().map(|schedule| schedule.id));
        return Ok(Vec::new());
    }
    let mut snapshots = Vec::new();
    for schedule in schedules {
        match source
            .fence_cron_for_cloud_migration(tenant, schedule.id, migration_id, Utc::now())
            .await
        {
            Ok(snapshot) => snapshots.push(snapshot),
            Err(StorageError::Conflict(reason)) => report.cron_refused.push(CronRefusal {
                schedule_id: schedule.id,
                reason,
            }),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(snapshots)
}

struct TerminalHistoryTransfer<'a> {
    storage: &'a dyn StorageBackend,
    client: &'a reqwest::Client,
    base: &'a str,
    migration_id: uuid::Uuid,
    copied: std::collections::HashSet<orch8_types::ids::InstanceId>,
}

impl TerminalHistoryTransfer<'_> {
    async fn copy(
        &mut self,
        tenant: &TenantId,
        report: &mut MigrationReport,
    ) -> Result<Vec<TaskInstance>> {
        let filter = InstanceFilter {
            tenant_id: Some(tenant.clone()),
            states: Some(vec![
                InstanceState::Completed,
                InstanceState::Failed,
                InstanceState::Cancelled,
            ]),
            ..Default::default()
        };
        let mut before = None;
        let mut deferred = Vec::new();
        loop {
            let page = self
                .storage
                .list_instances_keyset(&filter, before, PAGE)
                .await?;
            let done = page.len() < PAGE as usize;
            before = page.last().map(|instance| instance.id);
            for instance in page {
                if report.dry_run {
                    report.history_imported.push(instance.id.to_string());
                } else if let Some(instance) = self.copy_lineage(instance, true, report).await? {
                    deferred.push(instance);
                }
            }
            if done {
                return Ok(deferred);
            }
        }
    }

    async fn copy_lineage(
        &mut self,
        instance: TaskInstance,
        defer_live_parent: bool,
        report: &mut MigrationReport,
    ) -> Result<Option<TaskInstance>> {
        if self.copied.contains(&instance.id) {
            return Ok(None);
        }
        let mut seen = std::collections::HashSet::from([instance.id]);
        let mut parent = instance.parent_instance_id;
        let mut lineage = vec![instance];
        while let Some(id) = parent {
            if self.copied.contains(&id) {
                break;
            }
            if !seen.insert(id) {
                bail!("terminal history contains a parent cycle");
            }
            let ancestor = self
                .storage
                .get_instance(id)
                .await?
                .context("terminal history parent is missing")?;
            if ancestor.tenant_id != lineage[0].tenant_id {
                bail!("terminal history parent belongs to another tenant");
            }
            if !ancestor.state.is_terminal() {
                if defer_live_parent {
                    return Ok(Some(lineage.swap_remove(0)));
                }
                break;
            }
            parent = ancestor.parent_instance_id;
            lineage.push(ancestor);
        }
        for instance in lineage.into_iter().rev() {
            let id = instance.id;
            let entry = collect_terminal_history(self.storage, instance).await?;
            let result: Vec<ImportedHistoryInstance> = send_json(
                self.client,
                self.base,
                "/migrations/history/v1",
                &TerminalHistoryImport {
                    migration_id: self.migration_id,
                    entries: vec![entry],
                },
            )
            .await?;
            let [result] = result.as_slice() else {
                bail!("target returned an unexpected terminal history acknowledgement");
            };
            if result.instance_id != id {
                bail!("target acknowledged the wrong terminal instance");
            }
            self.copied.insert(id);
            match result.status {
                ImportStatus::Imported => report.history_imported.push(id.to_string()),
                ImportStatus::AlreadyPresent => report.history_already_present.push(id.to_string()),
            }
        }
        Ok(None)
    }
}

async fn collect_terminal_history(
    storage: &dyn StorageBackend,
    instance: TaskInstance,
) -> Result<TerminalInstanceHistory> {
    let id = instance.id;
    let mut payloads = Vec::new();
    let mut after = None;
    loop {
        let page = storage
            .list_externalized_entries(id, after.as_deref(), PAGE)
            .await?;
        let done = page.len() < PAGE as usize;
        after = page.last().map(|entry| entry.ref_key.clone());
        payloads.extend(page);
        if done {
            break;
        }
    }
    let mut signals = Vec::new();
    let mut after = None;
    loop {
        let page = storage.list_signal_history_page(id, after, PAGE).await?;
        let done = page.len() < PAGE as usize;
        after = page.last().map(|entry| entry.id);
        signals.extend(page);
        if done {
            break;
        }
    }
    let mut effects = super::backup::effect_archive(storage, &instance).await?;
    if let Some(effects) = &mut effects {
        effects.state.receipts = collect_effect_receipts(storage, &effects.owner).await?;
    }
    Ok(TerminalInstanceHistory {
        execution_tree: storage.get_execution_tree(id).await?,
        block_outputs: storage.get_all_outputs(id).await?,
        history: collect_durable_state(storage, id).await?,
        payloads,
        signals,
        effects,
        instance,
    })
}

/// Fence one instance and snapshot it. `Ok(Err(reason))` = refused (the
/// fence is released again).
async fn fence(
    native: &SqliteStorage,
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    mid: &str,
    args: &MigrateToArgs,
    capabilities: &MigrationCapabilities,
    destination: &MigrationDestination<'_>,
) -> Result<Result<MigratedInstance, String>> {
    let id = instance.id;
    let tenant = &instance.tenant_id;
    let deadline = Instant::now() + Duration::from_secs(args.idle_timeout_secs);
    loop {
        let current = storage
            .get_instance(id)
            .await?
            .context("instance disappeared during migration")?;
        if current.state.is_terminal() {
            return Ok(Err(format!(
                "finished locally ({}) before it could move",
                current.state
            )));
        }
        if let Some(reason) = busy_reason(storage, &current).await? {
            if !args.wait_for_idle {
                return Ok(Err(format!(
                    "busy: {reason}; re-run with --wait-for-idle or when it settles"
                )));
            }
            if Instant::now() >= deadline {
                return Ok(Err(format!(
                    "still busy after --idle-timeout-secs: {reason}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }

        if let Some(reason) = unsupported_state(storage, id, capabilities).await? {
            return Ok(Err(reason));
        }
        let execution = orch8_engine::effect_guard::ensure_effect_scope(storage, tenant, id)
            .await
            .map_err(|e| anyhow::anyhow!("ownership scope: {e}"))?;
        let ours = phase(&current, mid).as_deref() == Some("fenced");
        let fenced = match execution.state {
            OwnershipState::Transferring if ours => execution.clone(),
            OwnershipState::Transferring => {
                return Ok(Err(
                    "a continuity handoff is already in flight for this instance".into(),
                ));
            }
            OwnershipState::Compensating => {
                return Ok(Err(
                    "compensation permanently stopped forward execution".into()
                ));
            }
            OwnershipState::Owned | OwnershipState::Completed => {
                if execution.current_instance_id != id {
                    return Ok(Err("instance is owned by another runtime".into()));
                }
                let Some(next) = native.fence_for_cloud_migration(&execution, mid).await? else {
                    // Raced with the engine; look again.
                    if Instant::now() >= deadline {
                        return Ok(Err("ownership did not settle while fencing".into()));
                    }
                    continue;
                };
                next
            }
        };

        // The scheduler may have claimed it between the check and the fence.
        let current = storage
            .get_instance(id)
            .await?
            .context("instance disappeared during migration")?;
        if let Some(reason) = busy_reason(storage, &current).await? {
            if !ours {
                release_fence(storage, &fenced).await?;
            }
            if !args.wait_for_idle || Instant::now() >= deadline {
                return Ok(Err(format!("busy while fencing: {reason}")));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }

        if let Some(reason) = unsupported_state(storage, id, capabilities).await? {
            if !ours {
                release_fence(storage, &fenced).await?;
            }
            return Ok(Err(reason));
        }
        let migrated = snapshot_fenced_instance(storage, current, fenced).await?;
        return finalize_fenced_snapshot(
            native,
            storage,
            mid,
            capabilities,
            destination,
            migrated,
            ours,
        )
        .await;
    }
}

async fn snapshot_fenced_instance(
    storage: &dyn StorageBackend,
    instance: TaskInstance,
    continuity: ContinuityExecution,
) -> Result<MigratedInstance> {
    let id = instance.id;
    let externalized_payloads = collect_externalized_payloads(storage, id).await?;
    Ok(MigratedInstance {
        execution_tree: storage.get_execution_tree(id).await?,
        block_outputs: storage.get_all_outputs(id).await?,
        effect_receipts: collect_effect_receipts(storage, &continuity).await?,
        pending_signals: storage.get_pending_signals(id).await?,
        durable_state: collect_durable_state(storage, id).await?,
        domain_digests: None,
        externalized_payloads,
        artifacts: Vec::new(),
        continuity,
        instance,
    })
}

async fn finalize_fenced_snapshot(
    native: &SqliteStorage,
    storage: &dyn StorageBackend,
    migration_id: &str,
    capabilities: &MigrationCapabilities,
    destination: &MigrationDestination<'_>,
    mut migrated: MigratedInstance,
    resuming: bool,
) -> Result<Result<MigratedInstance, String>> {
    let id = migrated.instance.id;
    let references = artifacts::reachable_artifacts(&migrated);
    let can_stage_artifacts =
        storage.artifacts_enabled() && capabilities.supports(MIGRATION_ARTIFACTS_DOMAIN, 1);
    if !references.is_empty() {
        if !can_stage_artifacts {
            if !resuming {
                release_fence(storage, &migrated.continuity).await?;
            }
            return Ok(Err(
                "artifact transfer requires --source-artifacts-dir and a target artifact store"
                    .into(),
            ));
        }
        migrated.artifacts =
            artifacts::transfer(storage, destination, id, references.clone()).await?;
    }
    native
        .close_cloud_migration_signal_window(&migrated.continuity, migration_id, destination.base)
        .await?;
    migrated.pending_signals = storage.get_pending_signals(id).await?;
    let late_artifacts = artifacts::reachable_artifacts(&migrated)
        .difference(&references)
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if !late_artifacts.is_empty() {
        if !can_stage_artifacts {
            return Ok(Err("a signal accepted during cutover references an artifact; re-run with an artifact store".into()));
        }
        migrated
            .artifacts
            .extend(artifacts::transfer(storage, destination, id, late_artifacts).await?);
    }
    migrated.domain_digests = Some(MigrationDomainDigests::compute(
        &migrated.execution_tree,
        &migrated.block_outputs,
        &migrated.effect_receipts,
        &migrated.pending_signals,
    )?);
    Ok(Ok(migrated))
}

async fn collect_externalized_payloads(
    storage: &dyn StorageBackend,
    id: orch8_types::ids::InstanceId,
) -> Result<Vec<orch8_storage::externalizing::ExternalizedEntry>> {
    let mut entries = Vec::new();
    loop {
        let after = entries
            .last()
            .map(|entry: &orch8_storage::externalizing::ExternalizedEntry| entry.ref_key.as_str());
        let page = storage.list_externalized_entries(id, after, PAGE).await?;
        let size = page.len();
        entries.extend(page);
        if size < PAGE as usize {
            return Ok(entries);
        }
    }
}

async fn unsupported_state(
    storage: &dyn StorageBackend,
    id: orch8_types::ids::InstanceId,
    capabilities: &MigrationCapabilities,
) -> Result<Option<String>> {
    let domain = if !capabilities.supports(MIGRATION_KV_DOMAIN, 1)
        && !storage.list_instance_kv_page(id, None, 1).await?.is_empty()
    {
        MIGRATION_KV_DOMAIN
    } else if !capabilities.supports(MIGRATION_CHECKPOINT_DOMAIN, 1)
        && !storage.list_checkpoint_page(id, None, 1).await?.is_empty()
    {
        MIGRATION_CHECKPOINT_DOMAIN
    } else if !capabilities.supports(MIGRATION_EXTERNALIZED_DOMAIN, 1)
        && !storage
            .list_externalized_entries(id, None, 1)
            .await?
            .is_empty()
    {
        MIGRATION_EXTERNALIZED_DOMAIN
    } else {
        return Ok(None);
    };
    Ok(Some(format!(
        "target cannot preserve migration domain {domain} format 1"
    )))
}

async fn collect_durable_state(
    storage: &dyn StorageBackend,
    id: orch8_types::ids::InstanceId,
) -> Result<MigratedInstanceState> {
    let mut state = MigratedInstanceState::default();
    let mut after = None;
    loop {
        let page = storage
            .list_instance_kv_page(id, after.as_deref(), PAGE)
            .await?;
        let done = page.len() < PAGE as usize;
        after = page.last().map(|entry| entry.key.clone());
        state.instance_kv.extend(page);
        if done {
            break;
        }
    }
    let mut after = None;
    loop {
        let page = storage.list_checkpoint_page(id, after, PAGE).await?;
        let done = page.len() < PAGE as usize;
        after = page.last().map(|checkpoint| checkpoint.id);
        state.checkpoints.extend(page);
        if done {
            break;
        }
    }
    Ok(state)
}

async fn read_target_capabilities(
    client: &reqwest::Client,
    base: &str,
) -> Result<MigrationCapabilities> {
    let url = format!("{base}/migrations/capabilities");
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = response.status();
    if !status.is_success() {
        bail!("target did not declare migration capabilities ({status})");
    }
    let capabilities: MigrationCapabilities = response
        .json()
        .await
        .context("decode target migration capabilities")?;
    if capabilities.manifest_version != MIGRATION_MANIFEST_VERSION {
        bail!(
            "unsupported migration capability manifest version {}",
            capabilities.manifest_version
        );
    }
    for domain in MIGRATION_BASE_DOMAINS {
        if !capabilities.supports(domain, 1) {
            bail!("target cannot preserve migration domain {domain} format 1");
        }
    }
    if !capabilities.supports(MIGRATION_DIGEST_DOMAIN, 1) {
        bail!("target cannot verify migration domain digests format 1");
    }
    Ok(capabilities)
}

async fn collect_effect_receipts(
    storage: &dyn StorageBackend,
    fenced: &ContinuityExecution,
) -> Result<Vec<orch8_types::continuity::EffectReceipt>> {
    let mut receipts = Vec::new();
    let mut cursor = None;
    loop {
        let page = storage
            .page_effect_receipts(
                &fenced.tenant_id,
                fenced.continuity_id,
                cursor.as_ref(),
                PAGE,
            )
            .await?;
        receipts.extend(page.receipts);
        let Some(next) = page.next else {
            return Ok(receipts);
        };
        cursor = Some(next);
    }
}

async fn release_fence(storage: &dyn StorageBackend, fenced: &ContinuityExecution) -> Result<()> {
    let next = ContinuityExecution {
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
        ..fenced.clone()
    };
    storage
        .cas_continuity_owner(
            &fenced.tenant_id,
            fenced.continuity_id,
            fenced.epoch,
            fenced.owner_runtime_id,
            &next,
        )
        .await?;
    Ok(())
}

#[derive(Default)]
struct ByteCounter(usize);
impl std::io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn count_encoded_bytes(value: &impl Serialize) -> Result<usize> {
    let mut count = ByteCounter::default();
    serde_json::to_writer(&mut count, value)?;
    Ok(count.0)
}

struct MigrationDestination<'a> {
    client: &'a reqwest::Client,
    base: &'a str,
    tenant: &'a TenantId,
    chunked: bool,
}

impl MigrationDestination<'_> {
    async fn send(&self, request: &MigrationImportRequest) -> Result<MigrationImportResponse> {
        let size = count_encoded_bytes(request)?;
        if size > MIGRATION_MAX_UPLOAD_BYTES {
            bail!("migration batch exceeds {MIGRATION_MAX_UPLOAD_BYTES} encoded bytes");
        }
        if size > DIRECT_IMPORT_BYTES && self.chunked {
            return self.send_chunks(request).await;
        }
        if size > 16 * 1024 * 1024 {
            bail!("target does not support chunked imports for a migration batch of {size} bytes");
        }
        let has_durable_state = request
            .instances
            .iter()
            .any(MigratedInstance::requires_v1_import);
        send_json(
            self.client,
            self.base,
            if has_durable_state {
                "/migrations/import/v1"
            } else {
                "/migrations/import"
            },
            request,
        )
        .await
    }

    async fn send_chunks(
        &self,
        request: &MigrationImportRequest,
    ) -> Result<MigrationImportResponse> {
        let (digest, upload) = self.upload_batch(request).await?;
        let committed = send_json(
            self.client,
            self.base,
            &format!("/migrations/uploads/v1/{}/commit", digest.as_str()),
            &upload,
        )
        .await;
        match committed {
            Ok(response) => Ok(response),
            Err(error) => self
                .read_chunk_receipt(digest.as_str(), &upload, request)
                .await
                .map_err(|receipt_error| {
                    error.context(format!(
                        "staged import remains unconfirmed (SHA-256 {}, {} bytes): {receipt_error}",
                        digest.as_str(),
                        upload.length
                    ))
                }),
        }
    }

    async fn upload_batch(
        &self,
        request: &MigrationImportRequest,
    ) -> Result<(
        orch8_types::plugin::WasmArtifactDigest,
        MigrationUploadCommit,
    )> {
        if count_encoded_bytes(request)? > MIGRATION_MAX_UPLOAD_BYTES {
            bail!("migration batch exceeds {MIGRATION_MAX_UPLOAD_BYTES} encoded bytes");
        }
        let payload = serde_json::to_vec(request)?;
        let digest = orch8_types::plugin::WasmArtifactDigest::for_bytes(&payload);
        self.stage_bytes(
            &format!("/migrations/uploads/v1/{}", digest.as_str()),
            &payload,
        )
        .await?;
        Ok((
            digest,
            MigrationUploadCommit {
                tenant_id: self.tenant.clone(),
                length: payload.len(),
            },
        ))
    }

    async fn stage_batch(&self, request: &MigrationImportRequest) -> Result<StagedUploadReport> {
        let (digest, upload) = self.upload_batch(request).await?;
        let response = self
            .client
            .get(format!(
                "{}/migrations/uploads/v1/{}/verify",
                self.base,
                digest.as_str()
            ))
            .query(&upload)
            .send()
            .await
            .context("verify the staged migration batch")?;
        if response.status() != reqwest::StatusCode::NO_CONTENT {
            bail!(
                "target did not verify the staged migration batch ({})",
                response.status()
            );
        }
        Ok(StagedUploadReport {
            digest: digest.as_str().to_owned(),
            length: upload.length,
            sequences: request.sequences.len(),
            execution_ids: request
                .instances
                .iter()
                .map(|entry| entry.instance.id)
                .collect(),
        })
    }

    async fn read_chunk_receipt(
        &self,
        digest: &str,
        upload: &MigrationUploadCommit,
        request: &MigrationImportRequest,
    ) -> Result<MigrationImportResponse> {
        const MAX_RECEIPT_BYTES: usize = 128 * 1024;
        let mut response = self
            .client
            .get(format!(
                "{}/migrations/uploads/v1/{digest}/receipt",
                self.base
            ))
            .query(upload)
            .send()
            .await
            .context("read the saved import receipt")?;
        if response.status() != reqwest::StatusCode::OK {
            bail!("saved receipt lookup returned HTTP {}", response.status());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.context("read receipt bytes")? {
            if chunk.len() > MAX_RECEIPT_BYTES - body.len() {
                bail!("saved import receipt exceeds {MAX_RECEIPT_BYTES} bytes");
            }
            body.extend_from_slice(&chunk);
        }
        let receipt: StagedMigrationImportReceipt =
            serde_json::from_slice(&body).context("decode saved import receipt")?;
        let result = &receipt.result;
        let entries_match = result.instances.len() == request.instances.len()
            && request
                .instances
                .iter()
                .zip(&result.instances)
                .all(|(sent, found)| {
                    sent.instance.id == found.instance_id
                        && sent.domain_digests == found.domain_digests
                        && sent
                            .continuity
                            .epoch
                            .checked_next()
                            .is_ok_and(|epoch| epoch.get() == found.epoch)
                });
        if receipt.tenant_id != upload.tenant_id
            || receipt.digest != digest
            || receipt.length != upload.length
            || result.migration_id != request.migration_id
            || result.target_runtime_id != target_runtime_id(request.migration_id)
            || result
                .sequences
                .created
                .checked_add(result.sequences.existing)
                != Some(request.sequences.len() as u64)
            || !entries_match
        {
            bail!("saved import receipt does not match the submitted batch");
        }
        Ok(receipt.result)
    }
    async fn stage_artifact(&self, digest: &str, bytes: &[u8]) -> Result<()> {
        let path = format!("/migrations/uploads/v1/{digest}");
        let status = self
            .client
            .get(format!("{}{path}/verify", self.base))
            .query(&MigrationUploadCommit {
                tenant_id: self.tenant.clone(),
                length: bytes.len(),
            })
            .send()
            .await
            .context("verify staged migration artifact")?
            .status();
        if status.is_success() {
            return Ok(());
        }
        if status != reqwest::StatusCode::NOT_FOUND {
            bail!("target refused verification of migration artifact ({status})");
        }
        self.stage_bytes(&path, bytes).await
    }

    async fn stage_bytes(&self, path: &str, payload: &[u8]) -> Result<()> {
        for (index, bytes) in payload.chunks(MIGRATION_CHUNK_BYTES).enumerate() {
            let response = self
                .client
                .put(format!("{}{path}/chunks/{index}", self.base))
                .query(&[("tenant_id", self.tenant.as_str())])
                .body(bytes.to_vec())
                .send()
                .await
                .context("upload migration chunk")?;
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await?;
                bail!("target rejected migration chunk {index} ({status}): {body}");
            }
        }
        Ok(())
    }
}

async fn send_json<R: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    request: &impl Serialize,
) -> Result<R> {
    let response = client
        .post(format!("{base}{path}"))
        .json(request)
        .send()
        .await
        .with_context(|| format!("POST {base}{path}"))?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        let body: serde_json::Value =
            serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
        bail!(
            "target rejected the import ({status}): {}",
            crate::describe_api_error(&body, "import failed")
        );
    }
    serde_json::from_str(&text).context("target returned an unexpected import response")
}

async fn stage_for_review(
    destination: &MigrationDestination<'_>,
    request: &MigrationImportRequest,
    report: &mut MigrationReport,
) -> bool {
    match destination.stage_batch(request).await {
        Ok(upload) => {
            report.sequences_sent += upload.sequences;
            report.staged_uploads.push(upload);
            true
        }
        Err(error) => {
            report.staging_error = Some(error.to_string());
            false
        }
    }
}

/// Stage the batch or import it and finalize acknowledged source executions.
async fn flush(
    destination: &MigrationDestination<'_>,
    storage: &dyn StorageBackend,
    migration_id: uuid::Uuid,
    source_engine_id: &str,
    batch: &mut Vec<MigratedInstance>,
    report: &mut MigrationReport,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let request = MigrationImportRequest {
        migration_id,
        source_engine_id: source_engine_id.to_owned(),
        sequences: Vec::new(),
        instances: std::mem::take(batch),
    };
    if report.stage_only {
        stage_for_review(destination, &request, report).await;
        return Ok(());
    }
    let response = destination.send(&request).await?;
    if response.migration_id != migration_id
        || response.target_runtime_id != orch8_types::migration::target_runtime_id(migration_id)
    {
        bail!("target acknowledged a different migration; the source remains fenced");
    }
    let owner = RuntimeId::from_uuid(response.target_runtime_id);
    for migrated in &request.instances {
        let id = migrated.instance.id;
        let Some(ack) = response.instances.iter().find(|r| r.instance_id == id) else {
            report.refused.push(Refusal {
                instance_id: id.to_string(),
                reason: "target did not acknowledge the instance; still fenced, re-run to resume"
                    .into(),
            });
            continue;
        };
        let fenced = &migrated.continuity;
        if ack.domain_digests != migrated.domain_digests
            || fenced
                .epoch
                .checked_next()
                .ok()
                .map(orch8_types::continuity::ExecutionEpoch::get)
                != Some(ack.epoch)
        {
            report.refused.push(Refusal {
                instance_id: id.to_string(),
                reason: "target acknowledgement did not verify the state domains and epoch".into(),
            });
            continue;
        }
        let committed = ContinuityExecution {
            owner_runtime_id: owner,
            epoch: orch8_types::continuity::ExecutionEpoch::from_u64(ack.epoch),
            state: OwnershipState::Transferring,
            updated_at: Utc::now(),
            ..fenced.clone()
        };
        let applied = storage
            .cas_continuity_owner(
                &fenced.tenant_id,
                fenced.continuity_id,
                fenced.epoch,
                fenced.owner_runtime_id,
                &committed,
            )
            .await?;
        if !applied {
            let current = storage
                .get_continuity_execution(&fenced.tenant_id, fenced.continuity_id)
                .await?;
            if current.is_none_or(|c| c.owner_runtime_id != owner) {
                report.refused.push(Refusal {
                    instance_id: id.to_string(),
                    reason: "local ownership changed during commit; inspect before re-running"
                        .into(),
                });
                continue;
            }
        }
        storage
            .update_instance_state(id, InstanceState::Paused, None)
            .await?;
        storage
            .merge_instance_metadata(
                id,
                &json!({MIGRATION_METADATA_KEY: {
                    "id": migration_id.to_string(),
                    "phase": "committed",
                    "target": destination.base,
                    "signal_target": destination.base,
                    "target_epoch": ack.epoch,
                    "at": Utc::now(),
                }}),
            )
            .await?;
        report.migrated.push(id.to_string());
    }
    Ok(())
}

fn print_report(report: &MigrationReport) {
    println!(
        "Migration {} -> {} (tenant {}){}",
        report.migration_id,
        report.target,
        report.tenant,
        if report.dry_run {
            " [dry run]"
        } else if report.stage_only {
            " [staged]"
        } else {
            ""
        }
    );
    println!(
        "  sequences:        {} sent, {} created on target",
        report.sequences_sent, report.sequences_created
    );
    println!(
        "  instances moved:  {}{}",
        report.migrated.len(),
        if report.dry_run { " (would move)" } else { "" }
    );
    println!("  already moved:    {}", report.already_migrated.len());
    for (index, upload) in report.staged_uploads.iter().enumerate() {
        println!(
            "  upload {}: SHA-256 {} · {} bytes · {} sequences · {} executions",
            index + 1,
            upload.digest,
            upload.length,
            upload.sequences,
            upload.execution_ids.len()
        );
    }
    if let Some(error) = &report.staging_error {
        println!("  staging interrupted: {error}");
    }
    if !report.history_imported.is_empty() || !report.history_already_present.is_empty() {
        println!(
            "  terminal history: {}{}, {} already present",
            report.history_imported.len(),
            if report.dry_run {
                " (would copy)"
            } else {
                " copied"
            },
            report.history_already_present.len()
        );
    }
    println!("  refused:          {}", report.refused.len());
    for refusal in &report.refused {
        println!("    {}  {}", refusal.instance_id, refusal.reason);
    }
    if !report.cron_imported.is_empty() || !report.cron_refused.is_empty() {
        let action = if report.dry_run {
            "would move"
        } else {
            "moved"
        };
        println!(
            "  cron schedules:   {} {action}, {} refused",
            report.cron_imported.len(),
            report.cron_refused.len()
        );
        for refusal in &report.cron_refused {
            println!("    {}  {}", refusal.schedule_id, refusal.reason);
        }
    }
    for warning in &report.warnings {
        println!("  note: {warning}");
    }
}

#[cfg(test)]
#[path = "cloud_migrate_tests.rs"]
mod tests;
