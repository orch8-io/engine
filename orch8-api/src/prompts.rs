//! Prompt registry API: push immutable prompt versions, read them, and move
//! labels (`production`, `canary`, …) — see `docs/PROMPTS.md`.
//!
//! Every route is tenant-scoped: a tenant-bound caller (header or per-tenant
//! key) always operates on its own tenant; an unscoped admin caller names the
//! tenant with `tenant_id` (body or query).

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, put};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use orch8_engine::prompt_registry::{self, PromptDraft, PromptRegistryError};
use orch8_types::ai::{PromptCanary, PromptLabel, PromptMessage, PromptTemplate};
use orch8_types::ids::TenantId;

use crate::AppState;
use crate::auth::OptionalTenant;
use crate::error::ApiError;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/prompts", get(list_prompts).post(push_prompt))
        .route("/prompts/{name}", get(get_prompt))
        .route("/prompts/{name}/resolve", get(resolve_prompt))
        .route(
            "/prompts/{name}/versions/{version}",
            get(get_prompt_version),
        )
        .route(
            "/prompts/{name}/labels/{label}",
            put(set_prompt_label).delete(delete_prompt_label),
        )
}

fn map_err(e: PromptRegistryError) -> ApiError {
    match e {
        PromptRegistryError::Invalid(m) => ApiError::InvalidArgument(m),
        PromptRegistryError::NotFound(m) => ApiError::NotFound(m),
        PromptRegistryError::Storage(e) => ApiError::from_storage(e, "prompt"),
    }
}

/// Tenant for a read/label route: the caller's own tenant, else `?tenant_id=`.
fn read_tenant(ctx: &OptionalTenant, query_tenant: Option<&str>) -> Result<String, ApiError> {
    crate::auth::scoped_tenant_id(ctx, query_tenant)
        .map(|t| t.as_str().to_string())
        .ok_or_else(|| {
            ApiError::InvalidArgument(
                "prompt routes require a tenant (X-Tenant-Id header or ?tenant_id=)".into(),
            )
        })
}

#[derive(Debug, Deserialize)]
pub(crate) struct TenantQuery {
    tenant_id: Option<String>,
}

/// Push body: the content of a new prompt version.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct PushPromptRequest {
    /// Tenant (ignored/must match when the caller is tenant-scoped).
    #[serde(default)]
    pub tenant_id: String,
    pub name: String,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub messages: Vec<PromptMessage>,
    #[serde(default)]
    #[schema(value_type = Option<Object>)]
    pub model_params: Option<serde_json::Value>,
    #[serde(default)]
    #[schema(value_type = Option<Object>)]
    pub response_schema: Option<serde_json::Value>,
    #[serde(default)]
    pub description: Option<String>,
    /// Optionally point this label at the pushed version in the same call.
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct PushPromptResponse {
    pub prompt: PromptTemplate,
    /// False when the content equals the latest version (returned as-is).
    pub created: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<PromptLabel>,
}

#[utoipa::path(
    post, path = "/prompts", tag = "prompts",
    request_body = PushPromptRequest,
    responses(
        (status = 201, description = "New version created", body = PushPromptResponse),
        (status = 200, description = "Identical to the latest version", body = PushPromptResponse),
        (status = 400, description = "Invalid prompt"),
    )
)]
pub(crate) async fn push_prompt(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Json(req): Json<PushPromptRequest>,
) -> Result<(StatusCode, Json<PushPromptResponse>), ApiError> {
    let tenant =
        crate::auth::enforce_tenant_create(&tenant_ctx, &TenantId::unchecked(req.tenant_id))?;
    let (prompt, created) = prompt_registry::push_prompt(
        state.storage.as_ref(),
        PromptDraft {
            tenant_id: tenant.as_str().to_string(),
            name: req.name,
            system: req.system,
            messages: req.messages,
            model_params: req.model_params,
            response_schema: req.response_schema,
            description: req.description,
        },
    )
    .await
    .map_err(map_err)?;
    let label = match req.label {
        Some(label) => Some(
            prompt_registry::set_label(
                state.storage.as_ref(),
                &prompt.tenant_id,
                &prompt.name,
                &label,
                prompt.version,
                None,
            )
            .await
            .map_err(map_err)?,
        ),
        None => None,
    };
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(PushPromptResponse {
            prompt,
            created,
            label,
        }),
    ))
}

/// One prompt in the registry listing.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct PromptSummary {
    pub name: String,
    pub latest_version: i32,
    pub versions: usize,
    pub description: Option<String>,
    pub labels: Vec<PromptLabel>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

const LIST_LIMIT: u32 = 5_000;

#[utoipa::path(
    get, path = "/prompts", tag = "prompts",
    params(("tenant_id" = Option<String>, Query, description = "Tenant (unscoped callers)")),
    responses((status = 200, body = Vec<PromptSummary>))
)]
pub(crate) async fn list_prompts(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Query(q): Query<TenantQuery>,
) -> Result<Json<Vec<PromptSummary>>, ApiError> {
    let tenant = read_tenant(&tenant_ctx, q.tenant_id.as_deref())?;
    let versions = state
        .storage
        .list_prompt_versions(&tenant, None, LIST_LIMIT)
        .await?;
    let labels = state.storage.list_prompt_labels(&tenant, None).await?;
    let mut out: Vec<PromptSummary> = Vec::new();
    // Rows are ordered by (name, version DESC): the first row per name is
    // the latest version.
    for v in versions {
        match out.last_mut() {
            Some(last) if last.name == v.name => last.versions += 1,
            _ => out.push(PromptSummary {
                labels: labels
                    .iter()
                    .filter(|l| l.name == v.name)
                    .cloned()
                    .collect(),
                name: v.name,
                latest_version: v.version,
                versions: 1,
                description: v.description,
                updated_at: v.created_at,
            }),
        }
    }
    Ok(Json(out))
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct PromptDetail {
    pub name: String,
    /// Newest first.
    pub versions: Vec<PromptTemplate>,
    pub labels: Vec<PromptLabel>,
}

#[utoipa::path(
    get, path = "/prompts/{name}", tag = "prompts",
    params(
        ("name" = String, Path, description = "Prompt name"),
        ("tenant_id" = Option<String>, Query, description = "Tenant (unscoped callers)"),
    ),
    responses((status = 200, body = PromptDetail), (status = 404))
)]
pub(crate) async fn get_prompt(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path(name): Path<String>,
    Query(q): Query<TenantQuery>,
) -> Result<Json<PromptDetail>, ApiError> {
    let tenant = read_tenant(&tenant_ctx, q.tenant_id.as_deref())?;
    let versions = state
        .storage
        .list_prompt_versions(&tenant, Some(&name), LIST_LIMIT)
        .await?;
    if versions.is_empty() {
        return Err(ApiError::NotFound(format!("prompt {name}")));
    }
    let labels = state
        .storage
        .list_prompt_labels(&tenant, Some(&name))
        .await?;
    Ok(Json(PromptDetail {
        name,
        versions,
        labels,
    }))
}

#[utoipa::path(
    get, path = "/prompts/{name}/versions/{version}", tag = "prompts",
    params(
        ("name" = String, Path, description = "Prompt name"),
        ("version" = i32, Path, description = "Version number"),
        ("tenant_id" = Option<String>, Query, description = "Tenant (unscoped callers)"),
    ),
    responses((status = 200, body = PromptTemplate), (status = 404))
)]
pub(crate) async fn get_prompt_version(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path((name, version)): Path<(String, i32)>,
    Query(q): Query<TenantQuery>,
) -> Result<Json<PromptTemplate>, ApiError> {
    let tenant = read_tenant(&tenant_ctx, q.tenant_id.as_deref())?;
    state
        .storage
        .get_prompt_version(&tenant, &name, version)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("prompt {name} version {version}")))
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResolveQuery {
    tenant_id: Option<String>,
    label: Option<String>,
    version: Option<i32>,
}

#[utoipa::path(
    get, path = "/prompts/{name}/resolve", tag = "prompts",
    params(
        ("name" = String, Path, description = "Prompt name"),
        ("label" = Option<String>, Query, description = "Resolve through this label (its stable version)"),
        ("version" = Option<i32>, Query, description = "Exact version"),
        ("tenant_id" = Option<String>, Query, description = "Tenant (unscoped callers)"),
    ),
    responses((status = 200, body = PromptTemplate), (status = 404))
)]
pub(crate) async fn resolve_prompt(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path(name): Path<String>,
    Query(q): Query<ResolveQuery>,
) -> Result<Json<PromptTemplate>, ApiError> {
    let tenant = read_tenant(&tenant_ctx, q.tenant_id.as_deref())?;
    let found = match (q.version, q.label.as_deref()) {
        (Some(_), Some(_)) => {
            return Err(ApiError::InvalidArgument(
                "pass `version` or `label`, not both".into(),
            ));
        }
        (Some(v), None) => state.storage.get_prompt_version(&tenant, &name, v).await?,
        (None, Some(label)) => {
            let l = state
                .storage
                .get_prompt_label(&tenant, &name, label)
                .await?
                .ok_or_else(|| ApiError::NotFound(format!("prompt {name} label {label}")))?;
            state
                .storage
                .get_prompt_version(&tenant, &name, l.version)
                .await?
        }
        (None, None) => {
            state
                .storage
                .get_latest_prompt_version(&tenant, &name)
                .await?
        }
    };
    found
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("prompt {name}")))
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct SetLabelRequest {
    #[serde(default)]
    pub tenant_id: String,
    /// Stable version the label points at.
    pub version: i32,
    /// Optional canary candidate version.
    #[serde(default)]
    pub canary_version: Option<i32>,
    /// Percent (0–100) of executions routed to `canary_version`.
    #[serde(default)]
    pub canary_percent: Option<u8>,
}

#[utoipa::path(
    put, path = "/prompts/{name}/labels/{label}", tag = "prompts",
    request_body = SetLabelRequest,
    params(
        ("name" = String, Path, description = "Prompt name"),
        ("label" = String, Path, description = "Label, e.g. production"),
    ),
    responses((status = 200, body = PromptLabel), (status = 400), (status = 404))
)]
pub(crate) async fn set_prompt_label(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path((name, label)): Path<(String, String)>,
    Json(req): Json<SetLabelRequest>,
) -> Result<Json<PromptLabel>, ApiError> {
    let tenant =
        crate::auth::enforce_tenant_create(&tenant_ctx, &TenantId::unchecked(req.tenant_id))?;
    let canary = match (req.canary_version, req.canary_percent) {
        (Some(version), percent) => Some(PromptCanary {
            version,
            percent: percent.unwrap_or(0),
        }),
        (None, Some(p)) if p > 0 => {
            return Err(ApiError::InvalidArgument(
                "canary_percent requires canary_version".into(),
            ));
        }
        (None, _) => None,
    };
    let record = prompt_registry::set_label(
        state.storage.as_ref(),
        tenant.as_str(),
        &name,
        &label,
        req.version,
        canary,
    )
    .await
    .map_err(map_err)?;
    Ok(Json(record))
}

#[utoipa::path(
    delete, path = "/prompts/{name}/labels/{label}", tag = "prompts",
    params(
        ("name" = String, Path, description = "Prompt name"),
        ("label" = String, Path, description = "Label"),
        ("tenant_id" = Option<String>, Query, description = "Tenant (unscoped callers)"),
    ),
    responses((status = 204), (status = 404))
)]
pub(crate) async fn delete_prompt_label(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path((name, label)): Path<(String, String)>,
    Query(q): Query<TenantQuery>,
) -> Result<StatusCode, ApiError> {
    let tenant = read_tenant(&tenant_ctx, q.tenant_id.as_deref())?;
    if state
        .storage
        .delete_prompt_label(&tenant, &name, &label)
        .await?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(format!("prompt {name} label {label}")))
    }
}
