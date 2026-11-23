//! Target side of `orch8 migrate --to <url>`: import sequences and in-flight
//! instances exported from another engine without restarting runs.
//!
//! Import is idempotent and resumable. An instance is created `paused` with
//! a `orch8_migration.phase = "importing"` marker, its execution tree, block
//! outputs, effect receipts, pending signals and ownership record are written
//! (each skipped when already present), and only then is it moved to its
//! original state and marked `imported`. A retry after a crash finishes a
//! partial import; a retry after success reports `already_present`.
//!
//! The target takes continuity ownership at `source epoch + 1` under a
//! deterministic migration runtime id. The source fences its copy with the
//! same epoch/owner, so the two engines never both advance the run.

use axum::extract::{DefaultBodyLimit, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use orch8_types::continuity::{ContinuityExecution, OwnershipState, RuntimeId};
use orch8_types::cron::{CronMigrationAcknowledgement, CronMigrationSnapshot, CronSchedule};
use orch8_types::instance::InstanceState;
use orch8_types::migration::{
    ImportStatus, ImportedHistoryInstance, ImportedInstance, MAX_INSTANCES_PER_IMPORT,
    MAX_SEQUENCES_PER_IMPORT, MIGRATION_METADATA_KEY, MigratedInstance, MigrationCapabilities,
    MigrationDomainDigests, MigrationImportRequest, MigrationImportResponse, SequenceImportCounts,
    TerminalHistoryImport, cron_schedule_digest, target_runtime_id,
};
use serde_json::json;

use crate::AppState;
use crate::auth::OptionalTenant;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/migrations/capabilities", get(capabilities))
        .route("/migrations/cron/v1", post(import_cron_schedule))
        .merge(crate::migration_chunks::routes())
        .route(
            "/migrations/import",
            post(import_legacy).layer(DefaultBodyLimit::max(16 * 1024 * 1024)),
        )
        .route(
            "/migrations/import/v1",
            post(import).layer(DefaultBodyLimit::max(16 * 1024 * 1024)),
        )
        .route(
            "/migrations/history/v1",
            post(import_terminal_history).layer(DefaultBodyLimit::max(16 * 1024 * 1024)),
        )
}

#[utoipa::path(
    get, path = "/migrations/capabilities", tag = "migrations",
    responses((status = 200, description = "Supported migration state formats", body = MigrationCapabilities))
)]
pub async fn capabilities(State(state): State<AppState>) -> Json<MigrationCapabilities> {
    let mut manifest = MigrationCapabilities::current();
    if !state.storage.artifacts_enabled() {
        manifest
            .domains
            .remove(orch8_types::migration::MIGRATION_ARTIFACTS_DOMAIN);
    }
    Json(manifest)
}

#[utoipa::path(
    post, path = "/migrations/cron/v1", tag = "migrations",
    request_body(content = serde_json::Value),
    responses((status = 200, description = "Recurring schedule ownership imported", body = serde_json::Value))
)]
pub async fn import_cron_schedule(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Json(snapshot): Json<CronMigrationSnapshot>,
) -> Result<Json<CronMigrationAcknowledgement>, ApiError> {
    let schedule = resumed_cron_schedule(&snapshot);
    let tenant = crate::auth::enforce_tenant_create(&tenant_ctx, &schedule.tenant_id)?;
    if tenant != schedule.tenant_id
        || state
            .storage
            .get_sequence(schedule.sequence_id)
            .await
            .map_err(|error| storage_error(error, "sequence"))?
            .is_none_or(|seq| seq.tenant_id != tenant || seq.namespace != schedule.namespace)
    {
        return Err(ApiError::InvalidArgument(
            "cron sequence is not installed for its tenant and namespace".into(),
        ));
    }
    orch8_engine::cron::validate_cron_expr(&schedule.cron_expr)
        .map_err(ApiError::InvalidArgument)?;
    schedule
        .timezone
        .parse::<chrono_tz::Tz>()
        .map_err(|_| ApiError::InvalidArgument("invalid cron time zone".into()))?;
    let epoch = i64::try_from(snapshot.source_epoch)
        .ok()
        .and_then(|epoch| epoch.checked_add(1))
        .ok_or_else(|| ApiError::InvalidArgument("invalid cron ownership epoch".into()))?;
    let digest = cron_schedule_digest(&snapshot)
        .map_err(|_| ApiError::InvalidArgument("invalid cron migration record".into()))?;
    state
        .storage
        .import_cron_migration(&snapshot, &schedule, epoch, &digest)
        .await
        .map_err(|error| storage_error(error, "cron schedule"))?;
    Ok(Json(CronMigrationAcknowledgement {
        schedule_id: schedule.id,
        migration_id: snapshot.migration_id,
        ownership_epoch: snapshot.source_epoch + 1,
        snapshot_sha256: digest,
    }))
}

fn resumed_cron_schedule(snapshot: &CronMigrationSnapshot) -> CronSchedule {
    let mut schedule = snapshot.schedule.clone();
    if let Some(last_fire) = snapshot.last_fire_at {
        schedule.last_triggered_at = schedule.last_triggered_at.max(Some(last_fire));
        if schedule.next_fire_at.is_some_and(|next| next <= last_fire) {
            schedule.next_fire_at =
                orch8_engine::cron::calculate_next_fire_after(&schedule, last_fire);
        }
    }
    schedule
}

#[cfg(test)]
mod cron_migration_tests {
    use super::*;

    #[test]
    fn watermark_advances_a_committed_occurrence_and_preserves_an_unfired_one() {
        let due = "2026-11-22T12:00:00Z"
            .parse::<chrono::DateTime<Utc>>()
            .unwrap();
        let schedule = serde_json::from_value(json!({
            "id": uuid::Uuid::now_v7(), "tenant_id": "test", "namespace": "default",
            "sequence_id": uuid::Uuid::now_v7(), "cron_expr": "* * * * *",
            "timezone": "UTC", "enabled": true, "next_fire_at": due,
            "created_at": due, "updated_at": due,
        }))
        .unwrap();
        let mut snapshot = CronMigrationSnapshot {
            migration_id: uuid::Uuid::now_v7(),
            source_epoch: 1,
            last_fire_at: Some(due - chrono::Duration::minutes(1)),
            schedule,
        };
        assert_eq!(resumed_cron_schedule(&snapshot).next_fire_at, Some(due));
        snapshot.last_fire_at = Some(due);
        let resumed = resumed_cron_schedule(&snapshot);
        assert_eq!(resumed.last_triggered_at, Some(due));
        assert_eq!(
            resumed.next_fire_at,
            Some(due + chrono::Duration::minutes(1))
        );
    }
}

#[utoipa::path(
    post, path = "/migrations/history/v1", tag = "migrations",
    request_body(content = serde_json::Value),
    responses((status = 200, description = "Read-only terminal instances imported", body = serde_json::Value))
)]
pub async fn import_terminal_history(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Json(request): Json<TerminalHistoryImport>,
) -> Result<Json<Vec<ImportedHistoryInstance>>, ApiError> {
    use orch8_storage::restore::{InstanceHistory, InstanceSnapshot};
    if request.entries.len() > MAX_INSTANCES_PER_IMPORT {
        return Err(ApiError::InvalidArgument(
            "too many terminal history entries".into(),
        ));
    }
    let mut ids = std::collections::HashSet::new();
    for entry in &request.entries {
        let instance = &entry.instance;
        let tenant = crate::auth::enforce_tenant_create(&tenant_ctx, &instance.tenant_id)?;
        if tenant != instance.tenant_id {
            return Err(ApiError::InvalidArgument(
                "terminal history requires an explicit tenant".into(),
            ));
        }
        if !instance.state.is_terminal() || !ids.insert(instance.id) {
            return Err(ApiError::InvalidArgument(
                "history instances must be terminal and unique".into(),
            ));
        }
        if state
            .storage
            .get_sequence(instance.sequence_id)
            .await
            .map_err(|error| storage_error(error, "sequence"))?
            .is_none_or(|sequence| sequence.tenant_id != instance.tenant_id)
        {
            return Err(ApiError::InvalidArgument(
                "history instance sequence is not installed for the tenant".into(),
            ));
        }
    }
    let mut results = Vec::new();
    for entry in request.entries {
        let id = entry.instance.id;
        if let Some(parent) = entry.instance.parent_instance_id
            && state
                .storage
                .get_instance(parent)
                .await
                .map_err(|error| storage_error(error, "terminal history parent"))?
                .is_none_or(|parent| parent.tenant_id != entry.instance.tenant_id)
        {
            return Err(ApiError::InvalidArgument(format!(
                "history parent of instance {id} is not present for its tenant"
            )));
        }
        let snapshot = InstanceSnapshot {
            instance: entry.instance,
            nodes: entry.execution_tree,
            outputs: entry.block_outputs,
            payloads: entry.payloads,
        };
        let history = InstanceHistory {
            checkpoints: entry.history.checkpoints,
            signals: entry.signals,
            state: entry.history.instance_kv,
            effects: entry.effects,
            worker_tasks: Vec::new(),
            read_only_migration: Some(request.migration_id),
        };
        let inserted = state
            .storage
            .restore_instance_snapshot_with_history(&snapshot, &history)
            .await
            .map_err(|error| storage_error(error, "terminal instance history"))?;
        let status = if inserted {
            ImportStatus::Imported
        } else if state
            .storage
            .read_only_history_migration(id)
            .await
            .map_err(|error| storage_error(error, "terminal instance history"))?
            == Some(request.migration_id)
        {
            ImportStatus::AlreadyPresent
        } else {
            return Err(ApiError::Conflict(format!(
                "instance {id} already exists on the target"
            )));
        };
        results.push(ImportedHistoryInstance {
            instance_id: id,
            status,
        });
    }
    Ok(Json(results))
}

fn storage_error(error: orch8_types::error::StorageError, what: &str) -> ApiError {
    ApiError::from_storage(error, what)
}

/// Import a migration batch.
#[utoipa::path(
    post, path = "/migrations/import/v1", tag = "migrations",
    request_body(content = serde_json::Value, description = "MigrationImportRequest: { migration_id, source_engine_id, sequences: [SequenceDefinition], instances: [{ instance, execution_tree, block_outputs, effect_receipts, pending_signals, continuity }] } (at most 100 instances and 500 sequences)"),
    responses(
        (status = 200, description = "MigrationImportResponse: per-instance status (`imported` | `already_present`), owned epoch, and the target runtime id", body = serde_json::Value),
        (status = 400, description = "Invalid batch (limits, unknown sequence, inconsistent ids)"),
        (status = 403, description = "Batch contains another tenant's records"),
        (status = 409, description = "An instance id exists but was not created by this migration"),
    )
)]
pub async fn import(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Json(request): Json<MigrationImportRequest>,
) -> Result<Json<MigrationImportResponse>, ApiError> {
    if request.instances.len() > MAX_INSTANCES_PER_IMPORT
        || request.sequences.len() > MAX_SEQUENCES_PER_IMPORT
    {
        return Err(ApiError::InvalidArgument(format!(
            "at most {MAX_INSTANCES_PER_IMPORT} instances and {MAX_SEQUENCES_PER_IMPORT} sequences per import request"
        )));
    }
    if request.source_engine_id.len() > 256 {
        return Err(ApiError::InvalidArgument(
            "source_engine_id is too long".into(),
        ));
    }
    // Every record must belong to the caller's tenant.
    for sequence in &request.sequences {
        crate::auth::enforce_tenant_create(&tenant_ctx, &sequence.tenant_id)?;
    }
    for migrated in &request.instances {
        crate::auth::enforce_tenant_create(&tenant_ctx, &migrated.instance.tenant_id)?;
        validate_consistency(migrated)?;
    }

    let mut counts = SequenceImportCounts::default();
    for sequence in &request.sequences {
        let existing = state
            .storage
            .get_sequence(sequence.id)
            .await
            .map_err(|e| storage_error(e, "sequence"))?;
        match existing {
            Some(found) if found.tenant_id == sequence.tenant_id => counts.existing += 1,
            Some(_) => return Err(ApiError::NotFound("sequence".into())),
            None => {
                sequence.validate().map_err(|e| {
                    ApiError::InvalidArgument(format!("sequence {}: {e}", sequence.name))
                })?;
                state.storage.create_sequence(sequence).await.map_err(|e| {
                    ApiError::Conflict(format!(
                        "sequence {} v{} could not be created: {e}",
                        sequence.name, sequence.version
                    ))
                })?;
                counts.created += 1;
            }
        }
    }

    let runtime = target_runtime_id(request.migration_id);
    let mut results = Vec::with_capacity(request.instances.len());
    for migrated in &request.instances {
        results.push(import_instance(&state, &request, migrated, runtime).await?);
    }
    Ok(Json(MigrationImportResponse {
        migration_id: request.migration_id,
        target_runtime_id: runtime,
        sequences: counts,
        instances: results,
    }))
}

async fn import_legacy(
    state: State<AppState>,
    tenant: OptionalTenant,
    Json(request): Json<MigrationImportRequest>,
) -> Result<Json<MigrationImportResponse>, ApiError> {
    if request
        .instances
        .iter()
        .any(MigratedInstance::requires_v1_import)
    {
        return Err(ApiError::InvalidArgument(
            "durable migration state and digests require /migrations/import/v1".into(),
        ));
    }
    import(state, tenant, Json(request)).await
}

fn validate_consistency(migrated: &MigratedInstance) -> Result<(), ApiError> {
    let id = migrated.instance.id;
    let tenant = &migrated.instance.tenant_id;
    let bad = |what: &str| {
        Err(ApiError::InvalidArgument(format!(
            "instance {id}: {what} belongs to another instance or tenant"
        )))
    };
    if migrated.execution_tree.iter().any(|n| n.instance_id != id) {
        return bad("an execution node");
    }
    if migrated.block_outputs.iter().any(|o| o.instance_id != id) {
        return bad("a block output");
    }
    if migrated.effect_receipts.iter().any(|r| {
        r.instance_id != id
            || &r.tenant_id != tenant
            || r.continuity_id != migrated.continuity.continuity_id
    }) {
        return bad("an effect receipt");
    }
    if migrated.pending_signals.iter().any(|s| s.instance_id != id) {
        return bad("a signal");
    }
    if !migrated.durable_state.belongs_to(id) {
        return Err(ApiError::InvalidArgument(format!(
            "instance {id} contains a duplicate or foreign state key or checkpoint"
        )));
    }
    let mut refs = std::collections::HashSet::new();
    if migrated
        .externalized_payloads
        .iter()
        .any(|entry| entry.instance_id != id || !refs.insert(&entry.ref_key))
    {
        return bad("an externalized payload");
    }
    let mut artifact_ids = std::collections::HashSet::new();
    if migrated.artifacts.iter().any(|artifact| {
        !artifact_ids.insert(artifact.artifact_id)
            || artifact.length > orch8_types::migration::MAX_MIGRATION_ARTIFACT_BYTES
            || !crate::migration_chunks::valid_digest(&artifact.sha256)
    }) {
        return Err(ApiError::InvalidArgument(format!(
            "instance {id} has an invalid artifact manifest"
        )));
    }
    if migrated.continuity.current_instance_id != id || &migrated.continuity.tenant_id != tenant {
        return bad("the ownership record");
    }
    if migrated.instance.state.is_terminal() {
        return Err(ApiError::InvalidArgument(format!(
            "instance {id} is terminal; only in-flight instances are migrated"
        )));
    }
    if let Some(expected) = &migrated.domain_digests {
        let actual = MigrationDomainDigests::compute(
            &migrated.execution_tree,
            &migrated.block_outputs,
            &migrated.effect_receipts,
            &migrated.pending_signals,
        )
        .map_err(|error| ApiError::InvalidArgument(error.to_string()))?;
        if actual != *expected {
            return Err(ApiError::Conflict(format!(
                "instance {id} migration request failed domain verification"
            )));
        }
    }
    Ok(())
}

async fn install_migrated_artifacts(
    storage: &dyn orch8_storage::StorageBackend,
    migrated: &MigratedInstance,
) -> Result<(), ApiError> {
    let id = migrated.instance.id;
    for artifact in &migrated.artifacts {
        let key = format!("{id}/{}", artifact.artifact_id);
        if let Some(bytes) = storage
            .get_artifact(&key)
            .await
            .map_err(|error| storage_error(error, "artifact"))?
        {
            let digest = orch8_types::plugin::WasmArtifactDigest::for_bytes(&bytes);
            if bytes.len() != artifact.length || digest.as_str() != artifact.sha256 {
                return Err(ApiError::Conflict(format!(
                    "destination artifact {key} has different content"
                )));
            }
            continue;
        }
        let bytes = crate::migration_chunks::read_verified_artifact(
            storage,
            &migrated.instance.tenant_id,
            artifact,
        )
        .await?;
        storage
            .put_artifact_with_id(id, artifact.artifact_id, "application/octet-stream", bytes)
            .await
            .map_err(|error| storage_error(error, "artifact"))?;
    }
    Ok(())
}

async fn mark_imported(
    state: &AppState,
    id: orch8_types::ids::InstanceId,
    request: &MigrationImportRequest,
    domain_digests: Option<&MigrationDomainDigests>,
) -> Result<(), ApiError> {
    state
        .storage
        .merge_instance_metadata(
            id,
            &json!({MIGRATION_METADATA_KEY: {
                "id": request.migration_id.to_string(),
                "phase": "imported",
                "source_engine_id": request.source_engine_id,
                "imported_at": Utc::now(),
                "domain_digests": domain_digests,
                "domains_verified": domain_digests.is_some(),
            }}),
        )
        .await
        .map_err(|e| storage_error(e, "instance metadata"))
}

fn check_requested_digests(
    instance: &orch8_types::instance::TaskInstance,
    requested: Option<&MigrationDomainDigests>,
) -> Result<bool, ApiError> {
    let marker = &instance.metadata[MIGRATION_METADATA_KEY];
    let recorded: Option<MigrationDomainDigests> =
        serde_json::from_value(marker["domain_digests"].clone())
            .map_err(|_| ApiError::Conflict("stored migration digests are invalid".into()))?;
    if recorded.as_ref() != requested {
        return Err(ApiError::Conflict(format!(
            "instance {} was staged with different domain digests",
            instance.id
        )));
    }
    Ok(marker["domains_verified"].as_bool().unwrap_or(false))
}

async fn verify_imported_domains(
    storage: &dyn orch8_storage::StorageBackend,
    owner: &ContinuityExecution,
    expected: &MigrationDomainDigests,
) -> Result<(), ApiError> {
    let id = owner.current_instance_id;
    let tree = storage
        .get_execution_tree(id)
        .await
        .map_err(|error| storage_error(error, "execution tree"))?;
    let outputs = storage
        .get_all_outputs(id)
        .await
        .map_err(|error| storage_error(error, "outputs"))?;
    let signals = storage
        .get_pending_signals(id)
        .await
        .map_err(|error| storage_error(error, "pending signals"))?;
    let mut receipts = Vec::new();
    let mut cursor = None;
    loop {
        let page = storage
            .page_effect_receipts(&owner.tenant_id, owner.continuity_id, cursor.as_ref(), 500)
            .await
            .map_err(|error| storage_error(error, "effect receipts"))?;
        receipts.extend(page.receipts);
        let Some(next) = page.next else {
            break;
        };
        cursor = Some(next);
    }
    let actual = MigrationDomainDigests::compute(&tree, &outputs, &receipts, &signals)
        .map_err(|error| ApiError::InvalidArgument(error.to_string()))?;
    if actual != *expected {
        return Err(ApiError::Conflict(format!(
            "instance {id} stored migration domains do not match the source"
        )));
    }
    Ok(())
}

fn migration_phase(instance: &orch8_types::instance::TaskInstance) -> Option<(String, String)> {
    let marker = instance.metadata.get(MIGRATION_METADATA_KEY)?;
    Some((
        marker.get("id")?.as_str()?.to_owned(),
        marker.get("phase")?.as_str()?.to_owned(),
    ))
}

#[allow(clippy::too_many_lines)]
async fn import_instance(
    state: &AppState,
    request: &MigrationImportRequest,
    migrated: &MigratedInstance,
    runtime: uuid::Uuid,
) -> Result<ImportedInstance, ApiError> {
    let storage = &state.storage;
    let source = &migrated.instance;
    let id = source.id;
    let digests = migrated.domain_digests.as_ref();
    let migration = request.migration_id.to_string();
    let epoch = migrated
        .continuity
        .epoch
        .checked_next()
        .map_err(|e| ApiError::InvalidArgument(e.to_string()))?;

    if storage
        .get_sequence(source.sequence_id)
        .await
        .map_err(|e| storage_error(e, "sequence"))?
        .is_none_or(|s| s.tenant_id != source.tenant_id)
    {
        return Err(ApiError::InvalidArgument(format!(
            "instance {id} references sequence {} which is not on the target; include it in `sequences`",
            source.sequence_id
        )));
    }

    let existing = storage
        .get_instance(id)
        .await
        .map_err(|e| storage_error(e, "instance"))?;
    if let Some(existing) = existing {
        match migration_phase(&existing) {
            Some((mid, phase)) if mid == migration && phase == "imported" => {
                if !check_requested_digests(&existing, digests)? && digests.is_some() {
                    return Err(ApiError::Conflict(format!(
                        "instance {id} has no verified migration domains"
                    )));
                }
                return Ok(ImportedInstance {
                    instance_id: id,
                    status: ImportStatus::AlreadyPresent,
                    epoch: epoch.get(),
                    domain_digests: digests.cloned(),
                });
            }
            Some((mid, phase)) if mid == migration && phase == "importing" => {
                let verified = check_requested_digests(&existing, digests)?;
                if existing.state != InstanceState::Paused {
                    // Released by an earlier attempt that crashed before
                    // stamping `imported`; the run may already be advancing
                    // here, so never touch its state again.
                    if !verified && digests.is_some() {
                        return Err(ApiError::Conflict(format!(
                            "instance {id} has no verified migration domains"
                        )));
                    }
                    mark_imported(state, id, request, digests).await?;
                    return Ok(ImportedInstance {
                        instance_id: id,
                        status: ImportStatus::AlreadyPresent,
                        epoch: epoch.get(),
                        domain_digests: digests.cloned(),
                    });
                }
            }
            _ => {
                return Err(ApiError::Conflict(format!(
                    "instance {id} already exists on the target and was not created by migration {migration}"
                )));
            }
        }
    } else {
        let mut staged = source.clone();
        staged.state = InstanceState::Paused;
        staged.next_fire_at = None;
        staged.updated_at = Utc::now();
        if !staged.metadata.is_object() {
            staged.metadata = json!({});
        }
        staged.metadata[MIGRATION_METADATA_KEY] = json!({
            "id": migration, "phase": "importing", "source_engine_id": request.source_engine_id,
            "domain_digests": digests, "domains_verified": false,
        });
        storage
            .create_instance(&staged)
            .await
            .map_err(|e| storage_error(e, "instance"))?;
    }

    // Receipts reference the ownership record. Install it before importing
    // children, and keep dispatch fenced until their writes have completed.
    let staged_owner = ContinuityExecution {
        continuity_id: migrated.continuity.continuity_id,
        tenant_id: source.tenant_id.clone(),
        current_instance_id: id,
        owner_runtime_id: RuntimeId::from_uuid(runtime),
        epoch,
        state: OwnershipState::Transferring,
        updated_at: Utc::now(),
    };
    let owner = storage
        .ensure_continuity_execution(&staged_owner)
        .await
        .map_err(|e| storage_error(e, "ownership"))?;
    if owner.continuity_id != staged_owner.continuity_id
        || owner.current_instance_id != id
        || owner.owner_runtime_id != staged_owner.owner_runtime_id
        || owner.epoch != epoch
        || !matches!(
            owner.state,
            OwnershipState::Transferring | OwnershipState::Owned
        )
    {
        return Err(ApiError::Conflict(format!(
            "instance {id} has conflicting migration ownership"
        )));
    }

    for entry in &migrated.externalized_payloads {
        storage
            .restore_externalized_entry(entry)
            .await
            .map_err(|error| storage_error(error, "externalized payload"))?;
    }
    install_migrated_artifacts(storage.as_ref(), migrated).await?;

    // Children: each step skips what an earlier partial attempt already wrote.
    if !migrated.execution_tree.is_empty()
        && storage
            .get_execution_tree(id)
            .await
            .map_err(|e| storage_error(e, "execution tree"))?
            .is_empty()
    {
        storage
            .create_execution_nodes_batch(&migrated.execution_tree)
            .await
            .map_err(|e| storage_error(e, "execution tree"))?;
    }
    let existing_outputs = storage
        .get_all_outputs(id)
        .await
        .map_err(|e| storage_error(e, "block outputs"))?;
    for output in &migrated.block_outputs {
        if existing_outputs.iter().any(|o| o.id == output.id) {
            continue;
        }
        storage
            .save_block_output(output)
            .await
            .map_err(|e| storage_error(e, "block output"))?;
    }
    for receipt in &migrated.effect_receipts {
        let stored = storage
            .ensure_effect_receipt(receipt)
            .await
            .map_err(|e| storage_error(e, "effect receipt"))?;
        if stored != *receipt {
            return Err(ApiError::Conflict(format!(
                "instance {id} has conflicting effect receipt {}",
                receipt.id
            )));
        }
    }
    let existing_signals = storage
        .get_pending_signals(id)
        .await
        .map_err(|e| storage_error(e, "signals"))?;
    for signal in &migrated.pending_signals {
        if existing_signals.iter().any(|row| row.id == signal.id) {
            continue;
        }
        storage
            .enqueue_signal(signal)
            .await
            .map_err(|e| storage_error(e, "signal"))?;
    }
    if let Some(digests) = digests {
        verify_imported_domains(storage.as_ref(), &owner, digests).await?;
        storage.merge_instance_metadata(id, &json!({MIGRATION_METADATA_KEY: {
            "id": migration, "phase": "importing", "source_engine_id": request.source_engine_id,
            "domain_digests": digests, "domains_verified": true,
        }})).await.map_err(|error| storage_error(error, "instance metadata"))?;
    }
    if owner.state == OwnershipState::Transferring
        && !storage
            .release_migrated_instance(&owner, &migrated.durable_state)
            .await
            .map_err(|e| storage_error(e, "ownership"))?
    {
        return Err(ApiError::Conflict(format!(
            "instance {id} changed ownership during migration"
        )));
    }

    // Release: original state (a mid-step `running` resumes as `scheduled`).
    let (state_after, fire_at) = match source.state {
        InstanceState::Running | InstanceState::Scheduled => (
            InstanceState::Scheduled,
            Some(source.next_fire_at.unwrap_or_else(Utc::now)),
        ),
        other => (other, source.next_fire_at),
    };
    storage
        .update_instance_state(id, state_after, fire_at)
        .await
        .map_err(|e| storage_error(e, "instance state"))?;
    mark_imported(state, id, request, digests).await?;
    Ok(ImportedInstance {
        instance_id: id,
        status: ImportStatus::Imported,
        epoch: epoch.get(),
        domain_digests: digests.cloned(),
    })
}
