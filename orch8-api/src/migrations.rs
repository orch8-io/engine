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

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use orch8_types::continuity::{ContinuityExecution, OwnershipState, RuntimeId};
use orch8_types::instance::InstanceState;
use orch8_types::migration::{
    ImportStatus, ImportedInstance, MAX_INSTANCES_PER_IMPORT, MAX_SEQUENCES_PER_IMPORT,
    MIGRATION_METADATA_KEY, MigratedInstance, MigrationImportRequest, MigrationImportResponse,
    SequenceImportCounts, target_runtime_id,
};
use serde_json::json;

use crate::AppState;
use crate::auth::OptionalTenant;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new().route("/migrations/import", post(import))
}

fn storage_error(error: orch8_types::error::StorageError, what: &str) -> ApiError {
    ApiError::from_storage(error, what)
}

/// Import a migration batch.
#[utoipa::path(
    post, path = "/migrations/import", tag = "migrations",
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
    if migrated
        .effect_receipts
        .iter()
        .any(|r| r.instance_id != id || &r.tenant_id != tenant)
    {
        return bad("an effect receipt");
    }
    if migrated.pending_signals.iter().any(|s| s.instance_id != id) {
        return bad("a signal");
    }
    if migrated.continuity.current_instance_id != id || &migrated.continuity.tenant_id != tenant {
        return bad("the ownership record");
    }
    if migrated.instance.state.is_terminal() {
        return Err(ApiError::InvalidArgument(format!(
            "instance {id} is terminal; only in-flight instances are migrated"
        )));
    }
    Ok(())
}

async fn mark_imported(
    state: &AppState,
    id: orch8_types::ids::InstanceId,
    request: &MigrationImportRequest,
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
            }}),
        )
        .await
        .map_err(|e| storage_error(e, "instance metadata"))
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
                return Ok(ImportedInstance {
                    instance_id: id,
                    status: ImportStatus::AlreadyPresent,
                    epoch: epoch.get(),
                });
            }
            Some((mid, phase)) if mid == migration && phase == "importing" => {
                if existing.state != InstanceState::Paused {
                    // Released by an earlier attempt that crashed before
                    // stamping `imported`; the run may already be advancing
                    // here, so never touch its state again.
                    mark_imported(state, id, request).await?;
                    return Ok(ImportedInstance {
                        instance_id: id,
                        status: ImportStatus::AlreadyPresent,
                        epoch: epoch.get(),
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
        if let Some(object) = staged.metadata.as_object_mut() {
            object.insert(
                    MIGRATION_METADATA_KEY.into(),
                    json!({"id": migration, "phase": "importing", "source_engine_id": request.source_engine_id}),
                );
        } else {
            staged.metadata = json!({MIGRATION_METADATA_KEY: {"id": migration, "phase": "importing", "source_engine_id": request.source_engine_id}});
        }
        storage
            .create_instance(&staged)
            .await
            .map_err(|e| storage_error(e, "instance"))?;
    }

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
        if storage
            .get_effect_receipt(&receipt.tenant_id, receipt.id)
            .await
            .map_err(|e| storage_error(e, "effect receipt"))?
            .is_none()
        {
            storage
                .create_effect_receipt(receipt)
                .await
                .map_err(|e| storage_error(e, "effect receipt"))?;
        }
    }
    if storage
        .get_pending_signals(id)
        .await
        .map_err(|e| storage_error(e, "signals"))?
        .is_empty()
    {
        for signal in &migrated.pending_signals {
            storage
                .enqueue_signal(signal)
                .await
                .map_err(|e| storage_error(e, "signal"))?;
        }
    }
    let ownership = ContinuityExecution {
        continuity_id: migrated.continuity.continuity_id,
        tenant_id: source.tenant_id.clone(),
        current_instance_id: id,
        owner_runtime_id: RuntimeId::from_uuid(runtime),
        epoch,
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    storage
        .ensure_continuity_execution(&ownership)
        .await
        .map_err(|e| storage_error(e, "ownership"))?;

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
    mark_imported(state, id, request).await?;
    Ok(ImportedInstance {
        instance_id: id,
        status: ImportStatus::Imported,
        epoch: epoch.get(),
    })
}
