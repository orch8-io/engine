//! Claim-scoped reads of immutable input, separate from definition transport.
use super::definition::{DefinitionQuery, check_expiry, current, stale};
use crate::{AppState, error::ApiError};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::header,
    response::IntoResponse,
};
use orch8_types::{delegation_definition::input::DelegationInputEnvelope, worker::WorkerClaim};
use uuid::Uuid;

#[utoipa::path(get,path="/workers/tasks/{id}/input",tag="workers",
    params(("id"=Uuid,Path),("worker_id"=String,Query),("claim_epoch"=u64,Query)),
    responses((status=200,description="Exact-byte input bound to the current claim and publication request",body=DelegationInputEnvelope),
        (status=403,description="Different or unattributed reporter"),(status=409,description="Stale claim, owner, or unavailable historical snapshot")))]
pub(crate) async fn read(
    State(state): State<AppState>,
    caller: crate::auth::WorkerCaller,
    key: Option<axum::Extension<crate::auth::StoredKeyAuthority>>,
    binding: crate::browser_sessions::OptionalBinding,
    tenant: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Query(query): Query<DefinitionQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let claim = WorkerClaim::new(query.worker_id, query.claim_epoch);
    let (task, context) = current(&state, &tenant, &caller, id, &claim).await?;
    crate::auth::reauthorize_worker_read(
        &state.storage,
        &caller,
        key.as_ref().map(|k| &k.0),
        binding.as_ref().map(|b| &b.0),
        &task,
    )
    .await?;
    let frozen = state
        .storage
        .get_published_delegation_input(&context.scope.tenant_id, id)
        .await
        .map_err(|e| ApiError::from_storage(e, "delegation input"))?
        .ok_or_else(|| {
            ApiError::Conflict("no certified input is retained for this publication".into())
        })?;
    if frozen.request_sha256() != context.request_sha256
        || frozen.reference() != &context.input
        || frozen.usage() != context.usage
        || frozen.expires_at() != context.expires_at
    {
        return Err(stale());
    }
    let envelope = frozen
        .envelope(context.scope.clone())
        .map_err(|e| ApiError::from_storage(e, "delegation input"))?;
    drop(frozen);
    let response = ([(header::CACHE_CONTROL, "no-store")], Json(envelope)).into_response();
    // Recheck after storage and response encoding: ownership or reporter authority may have changed.
    let (task, latest) = current(&state, &tenant, &caller, id, &claim).await?;
    if latest != context {
        return Err(stale());
    }
    crate::auth::reauthorize_worker_read(
        &state.storage,
        &caller,
        key.as_ref().map(|k| &k.0),
        binding.as_ref().map(|b| &b.0),
        &task,
    )
    .await?;
    check_expiry(&context)?;
    Ok(response)
}
