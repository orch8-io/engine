//! Sub-tenants: the `X-Orch8-Sub-Tenant` header, per-sub-tenant limits and
//! per-sub-tenant metering (the number that drives Embedded billing).
//!
//! The header is a *scoping* input sent by the vendor's backend, which
//! already holds a tenant API key and can act on every sub-tenant of its
//! tenant. It is not an authorization boundary for end customers — that is
//! the job of scoped embed tokens (`crate::embed`).

use axum::extract::Request;
use axum::extract::{FromRequestParts, Path, Query, State};
use axum::http::request::Parts;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use orch8_types::ids::TenantId;
use orch8_types::sub_tenant::{
    SUB_TENANT_HEADER, SubTenantLimits, SubTenantUsage, validate_sub_tenant,
};

use crate::AppState;
use crate::auth::OptionalTenant;
use crate::error::ApiError;

/// The validated `X-Orch8-Sub-Tenant` header, if present. A present but
/// malformed header is a 400 — it is never silently ignored, because
/// ignoring it would create the instance at tenant level.
#[derive(Debug, Clone, Default)]
pub struct SubTenantHeader(pub Option<String>);

impl<S: Send + Sync> FromRequestParts<S> for SubTenantHeader {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let Some(value) = parts.headers.get(SUB_TENANT_HEADER) else {
            return Ok(Self(None));
        };
        let value = value.to_str().map_err(|_| {
            ApiError::InvalidArgument("X-Orch8-Sub-Tenant must be visible ASCII".into())
        })?;
        validate_sub_tenant(value)
            .map_err(|e| ApiError::InvalidArgument(format!("X-Orch8-Sub-Tenant: {e}")))?;
        Ok(Self(Some(value.to_string())))
    }
}

impl SubTenantHeader {
    #[must_use]
    pub fn as_deref(&self) -> Option<&str> {
        self.0.as_deref()
    }

    /// Resolve the effective sub-tenant for a create: the header is
    /// authoritative; a body value must agree with it (like
    /// [`crate::auth::enforce_tenant_create`]); without a header the body
    /// value (validated) is used.
    pub fn for_create(&self, body: Option<&str>) -> Result<Option<String>, ApiError> {
        match (&self.0, body.filter(|b| !b.is_empty())) {
            (Some(header), Some(body)) if header != body => Err(ApiError::Forbidden(
                "sub_tenant in body does not match X-Orch8-Sub-Tenant header".into(),
            )),
            (Some(header), _) => Ok(Some(header.clone())),
            (None, Some(body)) => {
                validate_sub_tenant(body)
                    .map_err(|e| ApiError::InvalidArgument(format!("sub_tenant: {e}")))?;
                Ok(Some(body.to_string()))
            }
            (None, None) => Ok(None),
        }
    }

    /// Resolve a list filter: the header scopes the listing and overrides
    /// `?sub_tenant=`; otherwise the (validated) query value filters.
    pub fn for_filter(&self, query: Option<&str>) -> Result<Option<String>, ApiError> {
        if let Some(header) = &self.0 {
            return Ok(Some(header.clone()));
        }
        match query.filter(|q| !q.is_empty()) {
            Some(q) => {
                validate_sub_tenant(q)
                    .map_err(|e| ApiError::InvalidArgument(format!("sub_tenant: {e}")))?;
                Ok(Some(q.to_string()))
            }
            None => Ok(None),
        }
    }

    /// Single-resource reads: with a header, a resource of another (or no)
    /// sub-tenant is reported as not found.
    pub fn enforce_access(&self, resource: Option<&str>, label: &str) -> Result<(), ApiError> {
        if let Some(header) = &self.0
            && resource != Some(header.as_str())
        {
            return Err(ApiError::NotFound(label.to_string()));
        }
        Ok(())
    }
}

/// The caller's tenant, required by sub-tenant management endpoints.
pub(crate) fn require_tenant(tenant_ctx: &OptionalTenant) -> Result<TenantId, ApiError> {
    tenant_ctx
        .as_ref()
        .map(|axum::Extension(ctx)| ctx.tenant_id.clone())
        .ok_or_else(|| {
            ApiError::InvalidArgument(
                "this endpoint requires a tenant (per-tenant API key or X-Tenant-Id)".into(),
            )
        })
}

fn path_sub_tenant(sub: &str) -> Result<(), ApiError> {
    validate_sub_tenant(sub).map_err(|e| ApiError::InvalidArgument(format!("sub_tenant: {e}")))
}

/// Routes that read `X-Orch8-Sub-Tenant`: instance create / batch / list /
/// get and sequence create. Matches the method and the path without the
/// `/api/v1` prefix.
#[must_use]
pub fn honours_sub_tenant_header(method: &Method, path: &str) -> bool {
    let path = path.strip_prefix(crate::API_V1_PREFIX).unwrap_or(path);
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    match (method, segments.as_slice()) {
        (&Method::POST, ["instances" | "sequences"] | ["instances", "batch"])
        | (&Method::GET, ["instances"]) => true,
        (&Method::GET, ["instances", id]) => uuid::Uuid::parse_str(id).is_ok(),
        _ => false,
    }
}

/// Reject `X-Orch8-Sub-Tenant` on routes that do not honour it (EM-11): a
/// vendor proxy that attaches the header everywhere must learn it is not
/// being scoped, instead of silently getting tenant-wide behaviour.
pub async fn reject_unhonoured_sub_tenant_header(request: Request, next: Next) -> Response {
    if request.headers().contains_key(SUB_TENANT_HEADER)
        && !honours_sub_tenant_header(request.method(), request.uri().path())
    {
        return axum::response::IntoResponse::into_response(ApiError::InvalidArgument(format!(
            "{SUB_TENANT_HEADER} is not supported on this route (it scopes instance create/list/get/batch and sequence create only); remove the header"
        )));
    }
    next.run(request).await
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/sub-tenants/{sub}/limits", get(get_limits).put(put_limits))
        .route("/sub-tenants/{sub}/suspend", post(suspend))
        .route("/sub-tenants/{sub}/resume", post(resume))
        .route("/usage/sub-tenants", get(usage))
}

/// Limits as stored/returned. Absent fields are uncapped.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct SubTenantLimitsResponse {
    pub sub_tenant: String,
    pub max_executions_per_month: Option<u64>,
    pub max_concurrent: Option<u32>,
}

fn limits_response(sub: String, limits: SubTenantLimits) -> SubTenantLimitsResponse {
    SubTenantLimitsResponse {
        sub_tenant: sub,
        max_executions_per_month: limits.max_executions_per_month,
        max_concurrent: limits.max_concurrent,
    }
}

#[utoipa::path(get, path = "/sub-tenants/{sub}/limits", tag = "sub-tenants", operation_id = "get_sub_tenant_limits",
    params(("sub" = String, Path, description = "Sub-tenant id ([A-Za-z0-9._:-]{1,128})")),
    responses(
        (status = 200, description = "Caps (null = uncapped; unset sub-tenants are uncapped)",
            body = SubTenantLimitsResponse),
        (status = 400, description = "Invalid sub-tenant id or no tenant"),
    )
)]
pub(crate) async fn get_limits(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path(sub): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant = require_tenant(&tenant_ctx)?;
    path_sub_tenant(&sub)?;
    let limits = state
        .storage
        .get_sub_tenant_limits(&tenant, &sub)
        .await
        .map_err(|e| ApiError::from_storage(e, "sub_tenant_limits"))?
        .unwrap_or_default();
    Ok(Json(limits_response(sub, limits)))
}

#[utoipa::path(put, path = "/sub-tenants/{sub}/limits", tag = "sub-tenants", operation_id = "put_sub_tenant_limits",
    params(("sub" = String, Path, description = "Sub-tenant id ([A-Za-z0-9._:-]{1,128})")),
    request_body = SubTenantLimits,
    responses(
        (status = 200, description = "Caps replaced", body = SubTenantLimitsResponse),
        (status = 400, description = "Invalid sub-tenant id, body or no tenant"),
    )
)]
pub(crate) async fn put_limits(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path(sub): Path<String>,
    Json(orch8_types::strict::Strict(limits)): Json<orch8_types::strict::Strict<SubTenantLimits>>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant = require_tenant(&tenant_ctx)?;
    path_sub_tenant(&sub)?;
    state
        .storage
        .put_sub_tenant_limits(&tenant, &sub, &limits)
        .await
        .map_err(|e| ApiError::from_storage(e, "sub_tenant_limits"))?;
    Ok(Json(limits_response(sub, limits)))
}

async fn set_suspended(
    state: &AppState,
    tenant_ctx: &OptionalTenant,
    principal: Option<&Extension<crate::auth::PrincipalContext>>,
    sub: &str,
    suspended: bool,
) -> Result<StatusCode, ApiError> {
    if !crate::auth::principal_is_operator(principal.map(|Extension(p)| p)) {
        return Err(ApiError::Forbidden(
            "suspending sub-tenants requires the operator capability".into(),
        ));
    }
    let tenant = require_tenant(tenant_ctx)?;
    path_sub_tenant(sub)?;
    state
        .storage
        .set_sub_tenant_suspended(&tenant, sub, suspended)
        .await
        .map_err(|e| ApiError::from_storage(e, "sub_tenant"))?;
    // Make the change visible to this node's embed guard immediately; other
    // nodes pick it up within the revocation-cache TTL (a few seconds).
    state.embedded.revocations.clear();
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(post, path = "/sub-tenants/{sub}/suspend", tag = "sub-tenants", operation_id = "suspend_sub_tenant",
    params(("sub" = String, Path, description = "Sub-tenant id")),
    responses(
        (status = 204, description = "Every embed token of the sub-tenant is refused (401) within a few seconds"),
        (status = 403, description = "Operator capability required"),
    )
)]
pub(crate) async fn suspend(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    principal: Option<Extension<crate::auth::PrincipalContext>>,
    Path(sub): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    set_suspended(&state, &tenant_ctx, principal.as_ref(), &sub, true).await
}

#[utoipa::path(post, path = "/sub-tenants/{sub}/resume", tag = "sub-tenants", operation_id = "resume_sub_tenant",
    params(("sub" = String, Path, description = "Sub-tenant id")),
    responses(
        (status = 204, description = "Embed tokens of the sub-tenant are accepted again"),
        (status = 403, description = "Operator capability required"),
    )
)]
pub(crate) async fn resume(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    principal: Option<Extension<crate::auth::PrincipalContext>>,
    Path(sub): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    set_suspended(&state, &tenant_ctx, principal.as_ref(), &sub, false).await
}

#[derive(Debug, Deserialize, IntoParams)]
pub(crate) struct UsageQuery {
    /// Window start (RFC 3339, inclusive). Defaults to the start of the
    /// current UTC month.
    #[serde(default)]
    pub from: Option<DateTime<Utc>>,
    /// Window end (RFC 3339, exclusive). Defaults to now.
    #[serde(default)]
    pub to: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct SubTenantUsageResponse {
    pub items: Vec<SubTenantUsage>,
    /// Sub-tenants that started at least one execution in the window.
    pub active_sub_tenants: u64,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

/// Longest metering window served in one request.
const MAX_USAGE_WINDOW_DAYS: i64 = 400;

#[utoipa::path(get, path = "/usage/sub-tenants", tag = "sub-tenants", operation_id = "get_sub_tenant_usage",
    params(UsageQuery),
    responses(
        (status = 200, description = "Per-sub-tenant activity in [from, to)",
            body = SubTenantUsageResponse),
        (status = 400, description = "Invalid window or no tenant"),
    )
)]
pub(crate) async fn usage(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Query(q): Query<UsageQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant = require_tenant(&tenant_ctx)?;
    let to = q.to.unwrap_or_else(Utc::now);
    let from = q
        .from
        .unwrap_or_else(|| orch8_types::sub_tenant::month_start(to));
    if from >= to {
        return Err(ApiError::InvalidArgument("from must be before to".into()));
    }
    if to - from > chrono::Duration::days(MAX_USAGE_WINDOW_DAYS) {
        return Err(ApiError::InvalidArgument(format!(
            "usage window must not exceed {MAX_USAGE_WINDOW_DAYS} days"
        )));
    }
    let items = state
        .storage
        .sub_tenant_usage(&tenant, from, to)
        .await
        .map_err(|e| ApiError::from_storage(e, "sub_tenant_usage"))?;
    let active_sub_tenants = items.iter().filter(|u| u.executions_started > 0).count() as u64;
    Ok(Json(SubTenantUsageResponse {
        items,
        active_sub_tenants,
        from,
        to,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_is_authoritative_for_create_and_filter() {
        let header = SubTenantHeader(Some("acme".into()));
        assert_eq!(header.for_create(None).unwrap().as_deref(), Some("acme"));
        assert_eq!(
            header.for_create(Some("acme")).unwrap().as_deref(),
            Some("acme")
        );
        assert!(matches!(
            header.for_create(Some("other")),
            Err(ApiError::Forbidden(_))
        ));
        assert_eq!(
            header.for_filter(Some("other")).unwrap().as_deref(),
            Some("acme")
        );
        assert!(header.enforce_access(Some("acme"), "x").is_ok());
        assert!(matches!(
            header.enforce_access(None, "x"),
            Err(ApiError::NotFound(_))
        ));
    }

    /// EM-11: the header is only accepted where it is honoured.
    #[test]
    fn header_is_honoured_only_on_scoping_routes() {
        let id = uuid::Uuid::new_v4();
        for (m, p) in [
            (Method::POST, "/api/v1/instances".to_string()),
            (Method::POST, "/instances/batch".to_string()),
            (Method::GET, "/api/v1/instances".to_string()),
            (Method::GET, format!("/api/v1/instances/{id}")),
            (Method::POST, "/api/v1/sequences".to_string()),
        ] {
            assert!(honours_sub_tenant_header(&m, &p), "{m} {p}");
        }
        for (m, p) in [
            (Method::GET, format!("/api/v1/instances/{id}/children")),
            (Method::GET, format!("/api/v1/instances/{id}/logs")),
            (Method::GET, format!("/api/v1/instances/{id}/outputs")),
            (Method::POST, format!("/api/v1/instances/{id}/signals")),
            (Method::GET, "/api/v1/instances/dlq".to_string()),
            (Method::GET, "/api/v1/sequences".to_string()),
            (Method::GET, "/api/v1/embed/runs".to_string()),
            (Method::GET, "/api/v1/changes".to_string()),
        ] {
            assert!(!honours_sub_tenant_header(&m, &p), "{m} {p}");
        }
    }

    #[test]
    fn without_header_body_and_query_are_validated() {
        let none = SubTenantHeader(None);
        assert_eq!(none.for_create(None).unwrap(), None);
        assert!(none.for_create(Some("bad id")).is_err());
        assert_eq!(none.for_filter(Some("")).unwrap(), None);
        assert!(none.for_filter(Some("a/b")).is_err());
        assert!(none.enforce_access(None, "x").is_ok());
    }
}
