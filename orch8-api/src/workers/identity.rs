//! A fresh nonce proves only the caller's authenticated device-session binding.
use crate::{browser_sessions::OptionalBinding, error::ApiError};
use axum::{Json, extract::rejection::JsonRejection, http::header, response::IntoResponse};
use orch8_types::{
    continuity::RuntimeKind,
    worker_identity::{WorkerIdentityRequest, WorkerIdentityResponse},
};
#[utoipa::path(post,path="/workers/identity",tag="workers",request_body=WorkerIdentityRequest,
    responses((status=200,description="Fresh authenticated mobile runtime identity; not an execution grant",body=WorkerIdentityResponse),(status=403,description="A live device session is required")))]
pub(crate) async fn read(
    binding: OptionalBinding,
    request: Result<Json<WorkerIdentityRequest>, JsonRejection>,
) -> Result<impl IntoResponse, ApiError> {
    let axum::Extension(binding) =
        binding.ok_or_else(|| ApiError::Forbidden("a device session is required".into()))?;
    if binding.kind != RuntimeKind::Mobile {
        return Err(ApiError::Forbidden("a device session is required".into()));
    }
    let Json(request) = request
        .map_err(|_| ApiError::InvalidArgument("invalid worker identity challenge".into()))?;
    request
        .validate()
        .map_err(|_| ApiError::InvalidArgument("invalid worker identity challenge".into()))?;
    let now = chrono::Utc::now();
    let expires = std::cmp::min(binding.expires_at, now + chrono::Duration::seconds(60));
    if expires <= now {
        return Err(ApiError::Forbidden("device session expired".into()));
    }
    let device = binding
        .device_id
        .ok_or_else(|| ApiError::Forbidden("a bound device is required".into()))?;
    let response = WorkerIdentityResponse {
        version: 1,
        nonce: request.nonce,
        tenant_id: binding.tenant_id,
        runtime_id: binding.runtime_id,
        kind: binding.kind,
        device_id: device,
        issued_at: now,
        expires_at: expires,
    };
    let response = ([(header::CACHE_CONTROL, "no-store")], Json(response)).into_response();
    if chrono::Utc::now() >= expires {
        return Err(ApiError::Forbidden("device session expired".into()));
    }
    Ok(response)
}
