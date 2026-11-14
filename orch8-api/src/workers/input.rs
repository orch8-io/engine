//! Claim-scoped reads of immutable input, separate from definition transport.
use super::definition::{DefinitionQuery, current};
use crate::{AppState, error::ApiError};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::header,
    response::IntoResponse,
};
use orch8_types::{
    delegation_definition::{input::DelegationInputEnvelope, wire::DefinitionReadScope},
    worker::WorkerClaim,
};
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
    let (task, scope) = current(&state, &tenant, &caller, id, &claim).await?;
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
        .get_published_delegation_input(&scope, id)
        .await
        .map_err(|e| ApiError::from_storage(e, "delegation input"))?
        .ok_or_else(|| {
            ApiError::Conflict("no certified input is retained for this publication".into())
        })?;
    let expires_at = frozen.expires_at();
    check_expiry(expires_at)?;
    let envelope = frozen
        .envelope(DefinitionReadScope {
            tenant_id: scope,
            task_id: id,
            worker_id: claim.worker_id.clone(),
            claim_epoch: claim.claim_epoch,
        })
        .map_err(|e| ApiError::from_storage(e, "delegation input"))?;
    drop(frozen);
    let response = ([(header::CACHE_CONTROL, "no-store")], Json(envelope)).into_response();
    // Recheck after storage and response encoding: ownership or reporter authority may have changed.
    let (task, _) = current(&state, &tenant, &caller, id, &claim).await?;
    crate::auth::reauthorize_worker_read(
        &state.storage,
        &caller,
        key.as_ref().map(|k| &k.0),
        binding.as_ref().map(|b| &b.0),
        &task,
    )
    .await?;
    check_expiry(expires_at)?;
    Ok(response)
}

fn check_expiry(expires_at: chrono::DateTime<chrono::Utc>) -> Result<(), ApiError> {
    if expires_at <= chrono::Utc::now() {
        return Err(ApiError::Conflict(
            "delegation input publication expired".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn publication_deadline_remains_a_read_fence() {
        let now = chrono::Utc::now();
        assert!(check_expiry(now - chrono::Duration::seconds(1)).is_err());
        assert!(check_expiry(now + chrono::Duration::minutes(1)).is_ok());
    }
}
