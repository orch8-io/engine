use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::filter::InstanceFilter;
use orch8_types::ids::{InstanceId, SequenceId};
use orch8_types::instance::InstanceState;
use orch8_types::sequence::{SequenceDefinition, SequenceStatus};

use crate::AppState;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/sequences", post(create_sequence).get(list_sequences))
        .route("/sequences.json", get(list_sequences_array))
        .route("/sequences/{id}", get(get_sequence).delete(delete_sequence))
        .route("/sequences/{id}/deprecate", post(deprecate_sequence))
        .route("/sequences/{id}/status", post(set_sequence_status))
        .route("/sequences/{name}/unpublish", post(unpublish_sequence))
        .route("/sequences/{name}/promote", post(promote_sequence))
        .route("/sequences/by-name", get(get_sequence_by_name))
        .route("/sequences/versions", get(list_sequence_versions))
        .route("/sequences/migrate-instance", post(migrate_instance))
}

#[utoipa::path(post, path = "/sequences", tag = "sequences",
    request_body = SequenceDefinition,
    responses(
        (status = 201, description = "Sequence created", body = serde_json::Value),
        (status = 409, description = "Sequence already exists"),
    )
)]
pub(crate) async fn create_sequence(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(mut seq): Json<SequenceDefinition>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &seq.tenant_id)?;
    seq.tenant_id = tenant_id;

    // Structural validation — reject duplicate block ids up-front so the
    // engine isn't forced to reconcile collisions in block_outputs /
    // execution_tree keyed on BlockId.
    seq.validate()
        .map_err(|e| ApiError::InvalidArgument(e.to_string()))?;

    // Reject a malformed `input_schema` at authoring time, not on the first
    // instance create.
    if let Some(schema) = &seq.input_schema {
        crate::input_schema::validate_schema_is_well_formed(schema)?;
    }

    // Reject malformed `output_schema` on any step at authoring time.
    validate_output_schemas_in_blocks(&seq.blocks)?;
    if let Some(on_failure) = &seq.on_failure {
        validate_output_schemas_in_blocks(on_failure)?;
    }
    if let Some(on_cancel) = &seq.on_cancel {
        validate_output_schemas_in_blocks(on_cancel)?;
    }

    let mut warnings = seq.unknown_handler_warnings();

    let template_warnings = orch8_engine::template::validate_sequence_templates(&seq);
    for tw in &template_warnings {
        warnings.push(tw.to_string());
    }

    let lint_warnings = orch8_engine::lint::lint_sequence(&seq);
    for lw in &lint_warnings {
        warnings.push(lw.to_string());
    }

    state
        .storage
        .create_sequence(&seq)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;

    let mut body = serde_json::json!({ "id": seq.id });
    if !warnings.is_empty() {
        body["warnings"] = serde_json::json!(warnings);
    }

    Ok((StatusCode::CREATED, Json(body)))
}

#[utoipa::path(get, path = "/sequences/{id}", tag = "sequences",
    params(("id" = Uuid, Path, description = "Sequence ID")),
    responses(
        (status = 200, description = "Sequence found", body = SequenceDefinition),
        (status = 404, description = "Sequence not found"),
    )
)]
pub(crate) async fn get_sequence(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let seq = state
        .storage
        .get_sequence(SequenceId::from_uuid(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {id}")))?;

    crate::auth::enforce_tenant_access(&tenant_ctx, &seq.tenant_id, &format!("sequence {id}"))?;

    Ok(Json(seq))
}

#[derive(Deserialize)]
pub(crate) struct ByNameQuery {
    tenant_id: String,
    namespace: String,
    name: String,
    version: Option<i32>,
}

#[utoipa::path(get, path = "/sequences/by-name", tag = "sequences",
    params(
        ("tenant_id" = String, Query, description = "Tenant ID"),
        ("namespace" = String, Query, description = "Namespace"),
        ("name" = String, Query, description = "Sequence name"),
        ("version" = Option<i32>, Query, description = "Optional version"),
    ),
    responses(
        (status = 200, description = "Sequence found", body = SequenceDefinition),
        (status = 404, description = "Sequence not found"),
    )
)]
pub(crate) async fn get_sequence_by_name(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ByNameQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::scoped_tenant_id(&tenant_ctx, Some(&q.tenant_id))
        .unwrap_or_else(|| orch8_types::ids::TenantId::unchecked(q.tenant_id.clone()));
    let namespace = orch8_types::ids::Namespace::new(q.namespace);

    let seq = state
        .storage
        .get_sequence_by_name(&tenant_id, &namespace, &q.name, q.version)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {}", q.name)))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &seq.tenant_id,
        &format!("sequence {}", q.name),
    )?;

    Ok(Json(seq))
}

#[utoipa::path(post, path = "/sequences/{id}/deprecate", tag = "sequences",
    params(("id" = Uuid, Path, description = "Sequence ID to deprecate")),
    responses(
        (status = 204, description = "Sequence deprecated"),
    )
)]
pub(crate) async fn deprecate_sequence(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let seq = state
        .storage
        .get_sequence(SequenceId::from_uuid(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {id}")))?;

    crate::auth::enforce_tenant_access(&tenant_ctx, &seq.tenant_id, &format!("sequence {id}"))?;

    state
        .storage
        .deprecate_sequence(SequenceId::from_uuid(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(delete, path = "/sequences/{id}", tag = "sequences",
    params(("id" = Uuid, Path, description = "Sequence ID to delete")),
    responses(
        (status = 204, description = "Sequence deleted"),
        (status = 404, description = "Sequence not found"),
        (status = 409, description = "Cannot delete: active instances exist"),
    )
)]
pub(crate) async fn delete_sequence(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let seq = state
        .storage
        .get_sequence(SequenceId::from_uuid(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {id}")))?;

    crate::auth::enforce_tenant_access(&tenant_ctx, &seq.tenant_id, &format!("sequence {id}"))?;

    // Reject delete if non-terminal instances reference this sequence.
    let active_filter = InstanceFilter {
        sequence_id: Some(SequenceId::from_uuid(id)),
        states: Some(vec![
            InstanceState::Scheduled,
            InstanceState::Running,
            InstanceState::Paused,
            InstanceState::Waiting,
        ]),
        ..Default::default()
    };
    let active_count = state
        .storage
        .count_instances(&active_filter)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?;
    if active_count > 0 {
        return Err(ApiError::Conflict(format!(
            "cannot delete sequence {id}: {active_count} active instance(s) still reference it"
        )));
    }

    state
        .storage
        .delete_sequence(SequenceId::from_uuid(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(get, path = "/sequences/versions", tag = "sequences",
    params(
        ("tenant_id" = String, Query, description = "Tenant ID"),
        ("namespace" = String, Query, description = "Namespace"),
        ("name" = String, Query, description = "Sequence name"),
    ),
    responses(
        (status = 200, description = "All versions", body = Vec<SequenceDefinition>),
    )
)]
pub(crate) async fn list_sequence_versions(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ByNameQuery>,
) -> Result<Json<Vec<SequenceDefinition>>, ApiError> {
    let tenant_id = crate::auth::scoped_tenant_id(&tenant_ctx, Some(&q.tenant_id))
        .unwrap_or_else(|| orch8_types::ids::TenantId::unchecked(q.tenant_id.clone()));
    let namespace = orch8_types::ids::Namespace::new(q.namespace);

    let versions = state
        .storage
        .list_sequence_versions(&tenant_id, &namespace, &q.name)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;

    Ok(Json(versions))
}

#[derive(Deserialize)]
pub(crate) struct ListSequencesQuery {
    tenant_id: Option<String>,
    namespace: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
}

#[utoipa::path(get, path = "/sequences", tag = "sequences",
    params(
        ("tenant_id" = Option<String>, Query, description = "Optional tenant filter"),
        ("namespace" = Option<String>, Query, description = "Optional namespace filter"),
        ("limit" = Option<u32>, Query, description = "Max rows (default 200, max 1000)"),
        ("offset" = Option<u32>, Query, description = "Pagination offset"),
    ),
    responses(
        (status = 200, description = "All sequences (filtered/paginated)", body = Vec<SequenceDefinition>),
    )
)]
pub(crate) async fn list_sequences(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ListSequencesQuery>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    // Tenant scoping: if caller is a tenant-scoped key, force their tenant as
    // the filter. Anonymous/global callers may pass tenant_id to filter, or
    // omit it to see all sequences across all tenants.
    let effective_tenant = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref())
        .or_else(|| {
            q.tenant_id
                .clone()
                .map(orch8_types::ids::TenantId::unchecked)
        });
    let effective_namespace = q.namespace.map(orch8_types::ids::Namespace::new);
    let limit = q.limit.unwrap_or(200).min(1000);
    let offset = q.offset.unwrap_or(0);

    let sequences = state
        .storage
        .list_sequences(
            effective_tenant.as_ref(),
            effective_namespace.as_ref(),
            limit.saturating_add(1),
            offset,
        )
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;

    Ok(Json(crate::PaginatedResponse::from_vec(sequences, limit)))
}

/// Return all sequences as a plain JSON array (no pagination wrapper).
/// Used by mobile SDKs via `loadSequencesFromUrl`.
pub(crate) async fn list_sequences_array(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    // Tenant scoping: a tenant-scoped key is pinned to its own tenant; only a
    // global/root caller (empty scope) sees across tenants. Without this a
    // per-tenant key could read every tenant's sequence definitions — block
    // logic, prompts, integration params — via this endpoint, which the
    // paginated `GET /sequences` already guards.
    let tenant = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    let sequences = state
        .storage
        .list_sequences(tenant.as_ref(), None, 1000, 0)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;

    Ok(Json(sequences))
}

/// Hot migration: rebind a running instance to a different sequence version.
/// The instance will pick up the new version's block definitions on its next tick.
/// Only non-terminal instances can be migrated.
#[derive(Deserialize, ToSchema)]
pub(crate) struct MigrateInstanceRequest {
    /// The instance to migrate.
    instance_id: Uuid,
    /// The new sequence version to bind the instance to.
    target_sequence_id: Uuid,
}

#[utoipa::path(post, path = "/sequences/migrate-instance", tag = "sequences",
    request_body = MigrateInstanceRequest,
    responses(
        (status = 200, description = "Instance migrated to new sequence version"),
        (status = 400, description = "Instance in terminal state"),
        (status = 404, description = "Instance or sequence not found"),
    )
)]
pub(crate) async fn migrate_instance(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<MigrateInstanceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Validate the instance exists and is not terminal.
    let instance = state
        .storage
        .get_instance(InstanceId::from_uuid(req.instance_id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {}", req.instance_id)))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {}", req.instance_id),
    )?;

    if instance.state.is_terminal() {
        return Err(ApiError::InvalidArgument(format!(
            "instance {} is in terminal state {}",
            req.instance_id, instance.state
        )));
    }

    // Validate the target sequence exists.
    let target_seq = state
        .storage
        .get_sequence(SequenceId::from_uuid(req.target_sequence_id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {}", req.target_sequence_id)))?;

    // Tenant isolation: forbid migrating an instance onto a sequence owned
    // by a different tenant. Without this a tenant could pivot their
    // instance onto another tenant's sequence definition — the scheduler
    // would then execute their instance against the foreign tenant's
    // blocks/handlers/params, crossing the isolation boundary.
    if target_seq.tenant_id != instance.tenant_id {
        return Err(ApiError::Forbidden(format!(
            "sequence {} belongs to a different tenant than instance {}",
            req.target_sequence_id, req.instance_id
        )));
    }

    // Rebind: update the instance's sequence_id to the new version.
    state
        .storage
        .update_instance_sequence(
            InstanceId::from_uuid(req.instance_id),
            SequenceId::from_uuid(req.target_sequence_id),
        )
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?;

    Ok(Json(serde_json::json!({
        "migrated": true,
        "instance_id": req.instance_id,
        "from_sequence_id": instance.sequence_id,
        "to_sequence_id": req.target_sequence_id,
    })))
}

/// Unpublish a sequence: mark all versions as deprecated and optionally delete.
#[derive(Deserialize)]
pub(crate) struct UnpublishRequest {
    #[serde(default)]
    delete: bool,
}

pub(crate) async fn unpublish_sequence(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(name): Path<String>,
    Json(req): Json<UnpublishRequest>,
) -> Result<StatusCode, ApiError> {
    let tenant_id = crate::auth::scoped_tenant_id(&tenant_ctx, None)
        .unwrap_or_else(|| orch8_types::ids::TenantId::unchecked("default".to_string()));
    let namespace = orch8_types::ids::Namespace::new("default");

    let versions = state
        .storage
        .list_sequence_versions(&tenant_id, &namespace, &name)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;

    for seq in versions {
        crate::auth::enforce_tenant_access(
            &tenant_ctx,
            &seq.tenant_id,
            &format!("sequence {}", seq.id),
        )?;

        state
            .storage
            .deprecate_sequence(seq.id)
            .await
            .map_err(|e| ApiError::from_storage(e, "sequence"))?;

        state
            .storage
            .update_sequence_status(seq.id, "unpublished")
            .await
            .map_err(|e| ApiError::from_storage(e, "sequence"))?;

        if req.delete {
            state
                .storage
                .delete_sequence(seq.id)
                .await
                .map_err(|e| ApiError::from_storage(e, "sequence"))?;
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

/// Promote a sequence from staging to production.
/// Copies the latest version to a new production version.
pub(crate) async fn promote_sequence(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::scoped_tenant_id(&tenant_ctx, None)
        .unwrap_or_else(|| orch8_types::ids::TenantId::unchecked("default".to_string()));
    let namespace = orch8_types::ids::Namespace::new("default");

    let versions = state
        .storage
        .list_sequence_versions(&tenant_id, &namespace, &name)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;

    let latest = versions
        .into_iter()
        .next()
        .ok_or_else(|| ApiError::NotFound(format!("sequence {name}")))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &latest.tenant_id,
        &format!("sequence {}", latest.id),
    )?;

    if !latest.status.can_transition_to(SequenceStatus::Production) {
        return Err(ApiError::InvalidArgument(format!(
            "cannot promote sequence in '{}' status (must be staging)",
            latest.status,
        )));
    }

    // Create a new version with bumped version number and production status.
    let mut promoted = latest.clone();
    promoted.id = orch8_types::ids::SequenceId::new();
    promoted.version += 1;
    promoted.status = SequenceStatus::Production;
    promoted.created_at = chrono::Utc::now();

    state
        .storage
        .create_sequence(&promoted)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "id": promoted.id, "version": promoted.version })),
    ))
}

/// Transition a sequence's lifecycle status.
#[derive(Deserialize)]
pub(crate) struct SetStatusRequest {
    pub status: SequenceStatus,
}

pub(crate) async fn set_sequence_status(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<SetStatusRequest>,
) -> Result<StatusCode, ApiError> {
    let seq = state
        .storage
        .get_sequence(SequenceId::from_uuid(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {id}")))?;

    crate::auth::enforce_tenant_access(&tenant_ctx, &seq.tenant_id, &format!("sequence {id}"))?;

    if !seq.status.can_transition_to(req.status) {
        return Err(ApiError::InvalidArgument(format!(
            "invalid transition: {} → {} (allowed: {:?})",
            seq.status,
            req.status,
            seq.status.valid_transitions(),
        )));
    }

    state
        .storage
        .update_sequence_status(SequenceId::from_uuid(id), &req.status.to_string())
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;

    if req.status == SequenceStatus::Unpublished {
        state
            .storage
            .deprecate_sequence(SequenceId::from_uuid(id))
            .await
            .map_err(|e| ApiError::from_storage(e, "sequence"))?;
    }

    Ok(StatusCode::NO_CONTENT)
}

use orch8_types::sequence::BlockDefinition;

fn validate_output_schemas_in_blocks(blocks: &[BlockDefinition]) -> Result<(), ApiError> {
    for block in blocks {
        match block {
            BlockDefinition::Step(step) => {
                if let Some(schema) = &step.output_schema {
                    crate::input_schema::validate_output_schema_is_well_formed(schema).map_err(
                        |e| ApiError::InvalidArgument(format!("step '{}': {e}", step.id)),
                    )?;
                }
            }
            BlockDefinition::Parallel(p) => {
                for branch in &p.branches {
                    validate_output_schemas_in_blocks(branch)?;
                }
            }
            BlockDefinition::Race(r) => {
                for branch in &r.branches {
                    validate_output_schemas_in_blocks(branch)?;
                }
            }
            BlockDefinition::Loop(l) => validate_output_schemas_in_blocks(&l.body)?,
            BlockDefinition::ForEach(f) => validate_output_schemas_in_blocks(&f.body)?,
            BlockDefinition::Router(r) => {
                for route in &r.routes {
                    validate_output_schemas_in_blocks(&route.blocks)?;
                }
                if let Some(default) = &r.default {
                    validate_output_schemas_in_blocks(default)?;
                }
            }
            BlockDefinition::TryCatch(tc) => {
                validate_output_schemas_in_blocks(&tc.try_block)?;
                validate_output_schemas_in_blocks(&tc.catch_block)?;
                if let Some(finally) = &tc.finally_block {
                    validate_output_schemas_in_blocks(finally)?;
                }
            }
            BlockDefinition::SubSequence(_) => {}
            BlockDefinition::ABSplit(ab) => {
                for variant in &ab.variants {
                    validate_output_schemas_in_blocks(&variant.blocks)?;
                }
            }
            BlockDefinition::CancellationScope(cs) => {
                validate_output_schemas_in_blocks(&cs.blocks)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ─── ByNameQuery ───

    #[test]
    fn by_name_query_deserializes_with_version() {
        let raw = json!({
            "tenant_id": "tenant-1",
            "namespace": "ns",
            "name": "seq",
            "version": 5
        });
        let q: ByNameQuery = serde_json::from_value(raw).unwrap();
        assert_eq!(q.tenant_id, "tenant-1");
        assert_eq!(q.namespace, "ns");
        assert_eq!(q.name, "seq");
        assert_eq!(q.version, Some(5));
    }

    #[test]
    fn by_name_query_deserializes_without_version() {
        let raw = json!({
            "tenant_id": "tenant-1",
            "namespace": "ns",
            "name": "seq"
        });
        let q: ByNameQuery = serde_json::from_value(raw).unwrap();
        assert_eq!(q.tenant_id, "tenant-1");
        assert_eq!(q.namespace, "ns");
        assert_eq!(q.name, "seq");
        assert_eq!(q.version, None);
    }

    #[test]
    fn by_name_query_deserializes_with_null_version() {
        let raw = json!({
            "tenant_id": "tenant-1",
            "namespace": "ns",
            "name": "seq",
            "version": null
        });
        let q: ByNameQuery = serde_json::from_value(raw).unwrap();
        assert_eq!(q.version, None);
    }

    #[test]
    fn by_name_query_empty_strings() {
        let raw = json!({
            "tenant_id": "",
            "namespace": "",
            "name": "",
            "version": 1
        });
        let q: ByNameQuery = serde_json::from_value(raw).unwrap();
        assert_eq!(q.tenant_id, "");
        assert_eq!(q.namespace, "");
        assert_eq!(q.name, "");
        assert_eq!(q.version, Some(1));
    }

    // ─── ListSequencesQuery ───

    #[test]
    fn list_sequences_query_defaults_all_none() {
        let raw = json!({});
        let q: ListSequencesQuery = serde_json::from_value(raw).unwrap();
        assert!(q.tenant_id.is_none());
        assert!(q.namespace.is_none());
        assert!(q.limit.is_none());
        assert!(q.offset.is_none());
        assert_eq!(q.limit.unwrap_or(200).min(1000), 200);
        assert_eq!(q.offset.unwrap_or(0), 0);
    }

    #[test]
    fn list_sequences_query_limit_caps_at_1000() {
        let raw = json!({ "limit": 5000 });
        let q: ListSequencesQuery = serde_json::from_value(raw).unwrap();
        assert_eq!(q.limit.unwrap_or(200).min(1000), 1000);
    }

    #[test]
    fn list_sequences_query_limit_exact_1000() {
        let raw = json!({ "limit": 1000 });
        let q: ListSequencesQuery = serde_json::from_value(raw).unwrap();
        assert_eq!(q.limit.unwrap_or(200).min(1000), 1000);
    }

    #[test]
    fn list_sequences_query_limit_below_cap() {
        let raw = json!({ "limit": 50 });
        let q: ListSequencesQuery = serde_json::from_value(raw).unwrap();
        assert_eq!(q.limit.unwrap_or(200).min(1000), 50);
    }

    #[test]
    fn list_sequences_query_limit_zero() {
        let raw = json!({ "limit": 0 });
        let q: ListSequencesQuery = serde_json::from_value(raw).unwrap();
        assert_eq!(q.limit.unwrap_or(200).min(1000), 0);
    }

    #[test]
    fn list_sequences_query_offset_explicit() {
        let raw = json!({ "offset": 42 });
        let q: ListSequencesQuery = serde_json::from_value(raw).unwrap();
        assert_eq!(q.offset.unwrap_or(0), 42);
    }

    #[test]
    fn list_sequences_query_with_all_fields() {
        let raw = json!({
            "tenant_id": "t",
            "namespace": "n",
            "limit": 100,
            "offset": 10
        });
        let q: ListSequencesQuery = serde_json::from_value(raw).unwrap();
        assert_eq!(q.tenant_id, Some("t".to_string()));
        assert_eq!(q.namespace, Some("n".to_string()));
        assert_eq!(q.limit, Some(100));
        assert_eq!(q.offset, Some(10));
    }

    // ─── MigrateInstanceRequest ───

    #[test]
    fn migrate_instance_request_deserializes() {
        let raw = json!({
            "instance_id": "550e8400-e29b-41d4-a716-446655440000",
            "target_sequence_id": "550e8400-e29b-41d4-a716-446655440001"
        });
        let req: MigrateInstanceRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(
            req.instance_id.to_string(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(
            req.target_sequence_id.to_string(),
            "550e8400-e29b-41d4-a716-446655440001"
        );
    }

    #[test]
    fn migrate_instance_request_missing_instance_id_fails() {
        let raw = json!({
            "target_sequence_id": "550e8400-e29b-41d4-a716-446655440001"
        });
        let res = serde_json::from_value::<MigrateInstanceRequest>(raw);
        assert!(res.is_err());
    }

    #[test]
    fn migrate_instance_request_missing_target_sequence_id_fails() {
        let raw = json!({
            "instance_id": "550e8400-e29b-41d4-a716-446655440000"
        });
        let res = serde_json::from_value::<MigrateInstanceRequest>(raw);
        assert!(res.is_err());
    }

    #[test]
    fn migrate_instance_request_invalid_uuid_fails() {
        let raw = json!({
            "instance_id": "not-a-uuid",
            "target_sequence_id": "550e8400-e29b-41d4-a716-446655440001"
        });
        let res = serde_json::from_value::<MigrateInstanceRequest>(raw);
        assert!(res.is_err());
    }

    // ─── SetStatusRequest ───

    #[test]
    fn set_status_request_deserializes() {
        let raw = json!({ "status": "staging" });
        let req: SetStatusRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.status, SequenceStatus::Staging);
    }

    #[test]
    fn set_status_request_rejects_unknown() {
        let raw = json!({ "status": "nonexistent" });
        assert!(serde_json::from_value::<SetStatusRequest>(raw).is_err());
    }

    #[test]
    fn set_status_request_all_variants() {
        for (s, expected) in [
            ("draft", SequenceStatus::Draft),
            ("staging", SequenceStatus::Staging),
            ("production", SequenceStatus::Production),
            ("unpublished", SequenceStatus::Unpublished),
        ] {
            let raw = json!({ "status": s });
            let req: SetStatusRequest = serde_json::from_value(raw).unwrap();
            assert_eq!(req.status, expected);
        }
    }

    // ─── UnpublishRequest ───

    #[test]
    fn unpublish_request_defaults_delete_false() {
        let raw = json!({});
        let req: UnpublishRequest = serde_json::from_value(raw).unwrap();
        assert!(!req.delete);
    }

    #[test]
    fn unpublish_request_delete_true() {
        let raw = json!({ "delete": true });
        let req: UnpublishRequest = serde_json::from_value(raw).unwrap();
        assert!(req.delete);
    }
}
