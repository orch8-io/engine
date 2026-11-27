//! Per-queue dispatch mode management (poll vs push).

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Deserializer};
use utoipa::ToSchema;

use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::queue_dispatch::{DispatchMode, QueueDispatchConfig};

use crate::AppState;
use crate::auth::{OptionalTenant, enforce_tenant_access, enforce_tenant_create};
use crate::error::ApiError;
use crate::security::validate_public_url;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/queues/dispatch",
            post(set_dispatch)
                .get(list_dispatch)
                .put(set_guarded_dispatch),
        )
        .route(
            "/queues/dispatch/remove-reviewed",
            post(delete_guarded_dispatch),
        )
        .route(
            "/queues/dispatch/{tenant_id}/{queue_name}",
            axum::routing::delete(delete_dispatch),
        )
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct SetDispatchRequest {
    tenant_id: String,
    queue_name: String,
    mode: DispatchMode,
    #[serde(default)]
    push_url: Option<String>,
    /// Optional HMAC secret for signing pushed envelopes.
    /// Omission keeps the existing secret; explicit null clears it.
    #[serde(default, deserialize_with = "deserialize_secret_update")]
    #[schema(value_type = Option<String>)]
    secret: SecretUpdate,
}

#[derive(Default)]
enum SecretUpdate {
    #[default]
    Keep,
    Clear,
    Set(String),
}

const MAX_QUEUE_SECRET_BYTES: usize = 256 * 1024;

fn validate_secret_update(secret: &SecretUpdate) -> Result<(), ApiError> {
    if matches!(secret, SecretUpdate::Set(value) if value.len() > MAX_QUEUE_SECRET_BYTES) {
        return Err(ApiError::PayloadTooLarge(
            "queue dispatch secret exceeds 256 KiB".into(),
        ));
    }
    Ok(())
}

fn deserialize_secret_update<'de, D>(deserializer: D) -> Result<SecretUpdate, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(|secret| match secret {
        Some(value) => SecretUpdate::Set(value),
        None => SecretUpdate::Clear,
    })
}

#[derive(Deserialize)]
pub(crate) struct ListDispatchQuery {
    tenant_id: Option<String>,
    queue_name: Option<String>,
}

#[utoipa::path(post, path = "/queues/dispatch", tag = "routing",
    request_body = SetDispatchRequest,
    responses(
        (status = 200, description = "Dispatch config set", body = QueueDispatchConfig),
        (status = 400, description = "push mode requires push_url"),
    )
)]
pub(crate) async fn set_dispatch(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Json(req): Json<SetDispatchRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let (cfg, preserve_secret) = dispatch_config(&tenant_ctx, req)?;
    let mut out = state
        .storage
        .set_queue_dispatch(&cfg, preserve_secret)
        .await
        .map_err(|e| ApiError::from_storage(e, "queue_dispatch"))?;
    // Never echo the secret back.
    out.secret = None;
    Ok((StatusCode::OK, Json(out)))
}

fn dispatch_config(
    tenant_ctx: &OptionalTenant,
    req: SetDispatchRequest,
) -> Result<(QueueDispatchConfig, bool), ApiError> {
    let tenant_id = enforce_tenant_create(tenant_ctx, &TenantId::unchecked(req.tenant_id))?;
    validate_secret_update(&req.secret)?;

    if req.queue_name.trim().is_empty() {
        return Err(ApiError::InvalidArgument("queue_name is required".into()));
    }
    if req.mode == DispatchMode::Push {
        let url = req
            .push_url
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| ApiError::InvalidArgument("push mode requires a push_url".into()))?;
        validate_public_url(url).map_err(|e| ApiError::InvalidArgument(e.to_string()))?;
    }

    let now = Utc::now();
    let preserve_secret = matches!(req.secret, SecretUpdate::Keep);
    let config = QueueDispatchConfig {
        tenant_id: tenant_id.as_str().to_string(),
        queue_name: req.queue_name,
        mode: req.mode,
        push_url: req.push_url,
        secret: match req.secret {
            SecretUpdate::Keep | SecretUpdate::Clear => None,
            SecretUpdate::Set(value) => Some(value),
        },
        created_at: now,
        updated_at: now,
    };
    Ok((config, preserve_secret))
}

const DISPATCH_REVISION: &str = "x-orch8-queue-dispatch-revision";
const EXPECTED_DISPATCH_REVISION: &str = "x-orch8-queue-dispatch-if-revision";
const DISPATCH_GUARD: &str = "x-orch8-queue-dispatch-guard";
const ABSENT_REVISION: &str = "absent";

fn expected_revision(headers: &HeaderMap) -> Result<Option<&str>, ApiError> {
    let mut values = headers.get_all(EXPECTED_DISPATCH_REVISION).iter();
    let revision = values.next().and_then(|value| value.to_str().ok());
    match revision {
        Some(ABSENT_REVISION) if values.next().is_none() => Ok(None),
        Some(value)
            if values.next().is_none()
                && value.len() == 32
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) =>
        {
            Ok(Some(value))
        }
        _ => Err(ApiError::InvalidArgument(
            "one valid queue dispatch revision is required".into(),
        )),
    }
}

fn guard_headers(outcome: &'static str) -> [(axum::http::HeaderName, HeaderValue); 2] {
    [
        (header::CACHE_CONTROL, HeaderValue::from_static("no-store")),
        (
            axum::http::HeaderName::from_static(DISPATCH_GUARD),
            HeaderValue::from_static(outcome),
        ),
    ]
}

fn stale_revision() -> Response {
    let mut response =
        ApiError::Conflict("queue dispatch changed; review the current configuration".into())
            .into_response();
    response.headers_mut().extend(guard_headers("stale"));
    response
}

#[utoipa::path(put, path = "/queues/dispatch", tag = "routing",
    params(("X-Orch8-Queue-Dispatch-If-Revision" = String, Header, description = "Exact review revision or absent for conditional creation")),
    request_body = SetDispatchRequest,
    responses(
        (status = 200, description = "Reviewed dispatch config set", body = QueueDispatchConfig,
            headers(("X-Orch8-Queue-Dispatch-Guard" = String, description = "Applied when the revision comparison succeeded"))),
        (status = 400, description = "Invalid configuration or revision"),
        (status = 409, description = "The reviewed configuration changed")
    )
)]
pub(crate) async fn set_guarded_dispatch(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    headers: HeaderMap,
    Json(req): Json<SetDispatchRequest>,
) -> Result<Response, ApiError> {
    let revision = expected_revision(&headers)?;
    let (config, preserve_secret) = dispatch_config(&tenant_ctx, req)?;
    let updated = state
        .storage
        .set_reviewed_queue_dispatch(&config, preserve_secret, revision)
        .await
        .map_err(|error| ApiError::from_storage(error, "queue_dispatch"))?;
    Ok(match updated {
        Some(mut updated) => {
            updated.secret = None;
            (StatusCode::OK, guard_headers("applied"), Json(updated)).into_response()
        }
        None => stale_revision(),
    })
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoveReviewedDispatchRequest {
    tenant_id: String,
    queue_name: String,
}

#[utoipa::path(post, path = "/queues/dispatch/remove-reviewed", tag = "routing",
    params(("X-Orch8-Queue-Dispatch-If-Revision" = String, Header, description = "Exact review revision")),
    request_body = RemoveReviewedDispatchRequest,
    responses(
        (status = 204, description = "Reviewed dispatch config removed",
            headers(("X-Orch8-Queue-Dispatch-Guard" = String, description = "Applied when the revision comparison succeeded"))),
        (status = 400, description = "Invalid revision"),
        (status = 409, description = "The reviewed configuration changed")
    )
)]
pub(crate) async fn delete_guarded_dispatch(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    headers: HeaderMap,
    Json(RemoveReviewedDispatchRequest {
        tenant_id,
        queue_name,
    }): Json<RemoveReviewedDispatchRequest>,
) -> Result<Response, ApiError> {
    enforce_tenant_access(
        &tenant_ctx,
        &TenantId::unchecked(tenant_id.clone()),
        "queue_dispatch",
    )?;
    if queue_name.trim().is_empty() {
        return Err(ApiError::InvalidArgument("queue_name is required".into()));
    }
    let revision = expected_revision(&headers)?.ok_or_else(|| {
        ApiError::InvalidArgument("removal requires an existing queue dispatch revision".into())
    })?;
    let deleted = state
        .storage
        .delete_reviewed_queue_dispatch(&tenant_id, &queue_name, revision)
        .await
        .map_err(|error| ApiError::from_storage(error, "queue_dispatch"))?;
    Ok(if deleted {
        (StatusCode::NO_CONTENT, guard_headers("applied")).into_response()
    } else {
        stale_revision()
    })
}

#[utoipa::path(get, path = "/queues/dispatch", tag = "routing",
    params(
        ("tenant_id" = Option<String>, Query, description = "Filter by tenant"),
        ("queue_name" = Option<String>, Query, description = "Select one exact queue name"),
    ),
    responses((status = 200, description = "Dispatch configs", body = Vec<QueueDispatchConfig>,
        headers(
            ("X-Orch8-Queue-Dispatch-Scope" = String, description = "Exact when the queue selector was applied"),
            ("X-Orch8-Queue-Dispatch-Revision" = String, description = "Comparison revision or absent when an exact queue was reviewed")
        )))
)]
pub(crate) async fn list_dispatch(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Query(q): Query<ListDispatchQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref())
        .ok_or_else(|| ApiError::InvalidArgument("tenant_id is required".into()))?;
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    let mut configs = if let Some(queue_name) = q.queue_name {
        if queue_name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("queue_name is required".into()));
        }
        headers.insert(
            "x-orch8-queue-dispatch-scope",
            HeaderValue::from_static("exact"),
        );
        match state
            .storage
            .review_queue_dispatch(scoped.as_str(), &queue_name)
            .await
        {
            Ok(Some((configuration, revision))) => {
                let value = HeaderValue::from_str(&revision)
                    .map_err(|_| ApiError::Internal("invalid stored dispatch revision".into()))?;
                headers.insert(DISPATCH_REVISION, value);
                vec![configuration]
            }
            Ok(None) => {
                headers.insert(DISPATCH_REVISION, HeaderValue::from_static(ABSENT_REVISION));
                Vec::new()
            }
            Err(StorageError::Unsupported(_)) => state
                .storage
                .get_queue_dispatch_metadata(scoped.as_str(), &queue_name)
                .await
                .map_err(|error| ApiError::from_storage(error, "queue_dispatch"))?
                .into_iter()
                .collect(),
            Err(error) => return Err(ApiError::from_storage(error, "queue_dispatch")),
        }
    } else {
        state
            .storage
            .list_queue_dispatch(Some(scoped.as_str()))
            .await
            .map_err(|e| ApiError::from_storage(e, "queue_dispatch"))?
    };
    // Secrets are write-only: never return them in list responses.
    for cfg in &mut configs {
        cfg.secret = None;
    }
    Ok((headers, Json(configs)))
}

#[utoipa::path(delete, path = "/queues/dispatch/{tenant_id}/{queue_name}", tag = "routing",
    params(
        ("tenant_id" = String, Path, description = "Tenant id"),
        ("queue_name" = String, Path, description = "Queue name"),
    ),
    responses((status = 204, description = "Deleted"))
)]
pub(crate) async fn delete_dispatch(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path((tenant_id, queue_name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    enforce_tenant_access(
        &tenant_ctx,
        &TenantId::unchecked(tenant_id.clone()),
        "queue_dispatch",
    )?;
    state
        .storage
        .delete_queue_dispatch(&tenant_id, &queue_name)
        .await
        .map_err(|e| ApiError::from_storage(e, "queue_dispatch"))?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_secret_size_is_bounded() {
        assert!(validate_secret_update(&SecretUpdate::Set("x".into())).is_ok());
        assert!(matches!(
            validate_secret_update(&SecretUpdate::Set("x".repeat(MAX_QUEUE_SECRET_BYTES + 1))),
            Err(ApiError::PayloadTooLarge(_))
        ));
    }
}
