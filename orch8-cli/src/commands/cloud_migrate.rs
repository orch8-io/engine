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
use orch8_storage::StorageBackend;
use orch8_types::continuity::{ContinuityExecution, OwnershipState, RuntimeId};
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::TenantId;
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::migration::{
    MAX_SEQUENCES_PER_IMPORT, MIGRATION_METADATA_KEY, MigratedInstance, MigrationImportRequest,
    MigrationImportResponse,
};
use serde::Serialize;
use serde_json::json;

use crate::OutputFormat;

/// Instances sent per import request (the server accepts up to 100).
const IMPORT_BATCH: usize = 25;
const PAGE: u32 = 500;

#[derive(Debug, clap::Args)]
pub struct MigrateToArgs {
    /// Target engine API base URL (e.g. `https://cloud.orch8.io/api/v1`).
    /// Switches `orch8 migrate` from Postgres schema migration to moving an
    /// embedded engine's work to a remote engine.
    #[arg(long, value_name = "URL", requires = "source")]
    pub to: Option<String>,
    /// Source `SQLite` database (path or `sqlite://` URL) of the embedded engine.
    #[arg(long, value_name = "SQLITE")]
    pub source: Option<String>,
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
}

#[derive(Debug, Default, Serialize)]
pub struct MigrationReport {
    pub migration_id: String,
    pub target: String,
    pub tenant: String,
    pub dry_run: bool,
    pub sequences_sent: usize,
    pub sequences_created: u64,
    pub migrated: Vec<String>,
    pub already_migrated: Vec<String>,
    pub refused: Vec<Refusal>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Refusal {
    pub instance_id: String,
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
    if !report.refused.is_empty() {
        bail!(
            "{} instance(s) were not migrated (see above); re-run to resume",
            report.refused.len()
        );
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub async fn migrate(args: &MigrateToArgs, global_tenant: Option<&str>) -> Result<MigrationReport> {
    let to = args.to.as_deref().context("--to is required")?;
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

    let storage: Arc<dyn StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::file(&source)
            .await
            .with_context(|| format!("open {source}"))?,
    );
    let client = crate::build_client(args.target_api_key.as_deref(), Some(tenant.as_str()))?;

    let mut report = MigrationReport {
        migration_id: mid.clone(),
        target: base.clone(),
        tenant: tenant.to_string(),
        dry_run: args.dry_run,
        ..MigrationReport::default()
    };
    report.warnings.push(
        "KV state, checkpoints, audit history, step logs and artifact blobs stay on the source; \
         externalized payload references must be reachable from the target"
            .into(),
    );

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
    report.sequences_sent = sequences.len();
    if !args.dry_run {
        for chunk in sequences.chunks(MAX_SEQUENCES_PER_IMPORT) {
            let response = send(
                &client,
                &base,
                &MigrationImportRequest {
                    migration_id,
                    source_engine_id: source_engine_id.clone(),
                    sequences: chunk.to_vec(),
                    instances: Vec::new(),
                },
            )
            .await?;
            report.sequences_created += response.sequences.created;
        }
    }

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
    for instance in candidates {
        let id = instance.id;
        if phase(&instance, &mid).as_deref() == Some("committed") {
            report.already_migrated.push(id.to_string());
            continue;
        }
        if args.dry_run {
            if let Some(reason) = busy_reason(storage.as_ref(), &instance).await? {
                report.refused.push(Refusal {
                    instance_id: id.to_string(),
                    reason: format!("busy: {reason}"),
                });
            } else {
                report.migrated.push(id.to_string());
            }
            continue;
        }
        match fence(storage.as_ref(), &instance, &mid, args).await? {
            Ok(migrated) => batch.push(migrated),
            Err(reason) => report.refused.push(Refusal {
                instance_id: id.to_string(),
                reason,
            }),
        }
        if batch.len() >= IMPORT_BATCH {
            flush(
                &client,
                &base,
                storage.as_ref(),
                migration_id,
                &source_engine_id,
                &mut batch,
                &mut report,
            )
            .await?;
        }
    }
    flush(
        &client,
        &base,
        storage.as_ref(),
        migration_id,
        &source_engine_id,
        &mut batch,
        &mut report,
    )
    .await?;
    Ok(report)
}

/// Fence one instance and snapshot it. `Ok(Err(reason))` = refused (the
/// fence is released again).
async fn fence(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    mid: &str,
    args: &MigrateToArgs,
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

        let execution = orch8_engine::effect_guard::ensure_effect_scope(storage, tenant, id)
            .await
            .map_err(|e| anyhow::anyhow!("ownership scope: {e}"))?;
        let ours = phase(&current, mid).is_some();
        let fenced = match execution.state {
            OwnershipState::Transferring if ours => execution.clone(),
            OwnershipState::Transferring => {
                return Ok(Err(
                    "a continuity handoff is already in flight for this instance".into(),
                ));
            }
            OwnershipState::Owned | OwnershipState::Completed => {
                if execution.current_instance_id != id {
                    return Ok(Err("instance is owned by another runtime".into()));
                }
                let next = ContinuityExecution {
                    state: OwnershipState::Transferring,
                    updated_at: Utc::now(),
                    ..execution.clone()
                };
                if !storage
                    .cas_continuity_owner(
                        tenant,
                        execution.continuity_id,
                        execution.epoch,
                        execution.owner_runtime_id,
                        &next,
                    )
                    .await?
                {
                    // Raced with the engine; look again.
                    continue;
                }
                storage
                    .merge_instance_metadata(
                        id,
                        &json!({MIGRATION_METADATA_KEY: {"id": mid, "phase": "fenced", "at": Utc::now()}}),
                    )
                    .await?;
                next
            }
        };

        // The scheduler may have claimed it between the check and the fence.
        let current = storage
            .get_instance(id)
            .await?
            .context("instance disappeared during migration")?;
        if let Some(reason) = busy_reason(storage, &current).await? {
            release_fence(storage, &fenced).await?;
            if !args.wait_for_idle || Instant::now() >= deadline {
                return Ok(Err(format!("busy while fencing: {reason}")));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }

        return Ok(Ok(MigratedInstance {
            execution_tree: storage.get_execution_tree(id).await?,
            block_outputs: storage.get_all_outputs(id).await?,
            effect_receipts: storage
                .list_instance_effect_receipts(tenant, id, 10_000)
                .await?,
            pending_signals: storage.get_pending_signals(id).await?,
            continuity: fenced,
            instance: current,
        }));
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

async fn send(
    client: &reqwest::Client,
    base: &str,
    request: &MigrationImportRequest,
) -> Result<MigrationImportResponse> {
    let response = client
        .post(format!("{base}/migrations/import"))
        .json(request)
        .send()
        .await
        .with_context(|| format!("POST {base}/migrations/import"))?;
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

/// Import the batch, then commit (fence for good) every acknowledged instance.
async fn flush(
    client: &reqwest::Client,
    base: &str,
    storage: &dyn StorageBackend,
    migration_id: uuid::Uuid,
    source_engine_id: &str,
    batch: &mut Vec<MigratedInstance>,
    report: &mut MigrationReport,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let instances = std::mem::take(batch);
    let response = send(
        client,
        base,
        &MigrationImportRequest {
            migration_id,
            source_engine_id: source_engine_id.to_owned(),
            sequences: Vec::new(),
            instances: instances.clone(),
        },
    )
    .await?;
    let owner = RuntimeId::from_uuid(response.target_runtime_id);
    for migrated in &instances {
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
                    "target": base,
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
        if report.dry_run { " [dry run]" } else { "" }
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
    println!("  refused:          {}", report.refused.len());
    for refusal in &report.refused {
        println!("    {}  {}", refusal.instance_id, refusal.reason);
    }
    for warning in &report.warnings {
        println!("  note: {warning}");
    }
}

#[cfg(test)]
#[path = "cloud_migrate_tests.rs"]
mod tests;
