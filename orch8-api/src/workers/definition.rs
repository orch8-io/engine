//! Claim-scoped reads of immutable delegated definitions, never the live catalog.
use super::enforce_ownership_fence;
use crate::{AppState, error::ApiError};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::header,
    response::IntoResponse,
};
use orch8_storage::worker_claim::{RecordedReporter, recorded_reporter};
use orch8_types::delegation_definition::wire::{DefinitionReadScope, DefinitionWireEnvelope};
use orch8_types::worker::{WorkerClaim, WorkerTaskState};
use orch8_types::{continuity::OwnershipState, ids::TenantId, worker::WorkerTask};
use serde::Deserialize;
use uuid::Uuid;

#[derive(Deserialize)]
pub(crate) struct DefinitionQuery {
    pub(super) worker_id: String,
    pub(super) claim_epoch: u64,
}

#[utoipa::path(get,path="/workers/tasks/{id}/definition",tag="workers",
    params(("id"=Uuid,Path),("worker_id"=String,Query),("claim_epoch"=u64,Query)),
    responses((status=200,description="Exact-byte frozen definition bundle bound to the current claim",body=DefinitionWireEnvelope),
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
        .get_published_delegation_definition(&scope, id)
        .await
        .map_err(|e| ApiError::from_storage(e, "delegation definition"))?
        .ok_or_else(|| {
            ApiError::Conflict(
                "no certified frozen definition is retained for this publication".into(),
            )
        })?;
    let envelope = DefinitionWireEnvelope::new(
        DefinitionReadScope {
            tenant_id: scope,
            task_id: id,
            worker_id: claim.worker_id.clone(),
            claim_epoch: claim.claim_epoch,
        },
        frozen.wire(),
    )
    .map_err(|e| ApiError::from_storage(e, "delegation definition"))?;
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
    Ok(response)
}
pub(super) async fn current(
    state: &AppState,
    tenant: &crate::auth::OptionalTenant,
    caller: &crate::auth::WorkerCaller,
    id: Uuid,
    claim: &WorkerClaim,
) -> Result<(WorkerTask, TenantId), ApiError> {
    let task = state
        .storage
        .get_worker_task(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker task"))?
        .ok_or_else(|| ApiError::NotFound("worker task".into()))?;
    let instance = state
        .storage
        .get_instance(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound("worker task".into()))?;
    crate::auth::enforce_tenant_access(tenant, &instance.tenant_id, "worker task")?;
    if task.state != WorkerTaskState::Claimed
        || task.worker_id.as_deref() != Some(claim.worker_id.as_str())
        || task.claim_epoch != claim.claim_epoch
        || instance.state.is_terminal()
    {
        return Err(stale());
    }
    if recorded_reporter(
        state.storage.as_ref(),
        Some(&instance.tenant_id),
        id,
        claim,
        &caller.0,
    )
    .await
    .map_err(|e| ApiError::from_storage(e, "worker claim"))?
        != RecordedReporter::Matches
    {
        return Err(ApiError::Forbidden(
            "definition requires the recorded claim reporter".into(),
        ));
    }
    let issued = state
        .storage
        .get_worker_issued_claim(Some(&instance.tenant_id), id, claim)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker claim"))?
        .ok_or_else(stale)?;
    issued
        .validate_task_scope(&instance.tenant_id, &task)
        .map_err(|_| stale())?;
    let source = issued
        .invocation
        .as_ref()
        .filter(|s| s.is_published_mailbox())
        .ok_or_else(stale)?;
    if source.instance_id != instance.id
        || source.sequence_id != instance.sequence_id
        || source.tenant_id != instance.tenant_id
        || source.run_id != instance.context.runtime.run_id
        || source.dry_run != instance.context.runtime.dry_run
    {
        return Err(stale());
    }
    let binding = source.continuity.as_ref().ok_or_else(stale)?;
    let owner = state
        .storage
        .get_continuity_execution(&instance.tenant_id, binding.continuity_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "continuity"))?
        .ok_or_else(stale)?;
    if owner.state != OwnershipState::Owned
        || owner.current_instance_id != instance.id
        || owner.epoch.get() != binding.epoch
        || owner.owner_runtime_id != binding.owner_runtime_id
        || state
            .storage
            .is_delegation_proxy_quarantined(instance.id)
            .await
            .map_err(|e| ApiError::from_storage(e, "relay"))?
    {
        return Err(stale());
    }
    enforce_ownership_fence(state, &instance.tenant_id, &task, claim, "definition read").await?;
    Ok((task, instance.tenant_id))
}
fn stale() -> ApiError {
    ApiError::Conflict("worker definition claim or ownership changed".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn session_routes_grant_only_the_exact_definition_read() {
        for resource in ["definition", "input"] {
            let path = format!("/api/v1/workers/tasks/{}/{resource}", Uuid::now_v7());
            for check in [
                crate::browser_sessions::route_allowed,
                crate::device_sessions::route_allowed,
            ] {
                assert!(check(&axum::http::Method::GET, &path));
                assert!(!check(&axum::http::Method::POST, &path));
                assert!(!check(&axum::http::Method::GET, &format!("{path}/extra")));
                assert!(!check(&axum::http::Method::GET, "/api/v1/sequences"));
                assert!(!check(
                    &axum::http::Method::GET,
                    "/api/v1/artifacts/private/key"
                ));
            }
        }
    }
    #[tokio::test]
    async fn refreshed_runtime_sessions_keep_their_expiry_handler_and_queue_limits() {
        use orch8_types::continuity::{RuntimeId, RuntimeKind};
        let storage: std::sync::Arc<dyn orch8_storage::StorageBackend> = std::sync::Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let runtime = RuntimeId::new();
        let tenant = TenantId::new("session").unwrap();
        let caller =
            crate::auth::WorkerCaller(orch8_storage::worker_claim::WorkerClaimPrincipal::Runtime {
                tenant_id: tenant.clone(),
                runtime_id: runtime,
                runtime_kind: RuntimeKind::Browser,
                device_id: None,
            });
        let task:WorkerTask=serde_json::from_value(serde_json::json!({"id":Uuid::now_v7(),"instance_id":Uuid::now_v7(),"block_id":"work","handler_name":"orch8.delegation","queue_name":"private","params":{},"context":{},"attempt":0,"state":"claimed","worker_id":runtime.to_string(),"claim_epoch":1,"created_at":chrono::Utc::now()})).unwrap();
        let mut binding = crate::browser_sessions::RuntimeBinding {
            tenant_id: tenant,
            kind: RuntimeKind::Browser,
            runtime_id: runtime,
            device_id: None,
            handlers: vec![task.handler_name.clone()],
            queues: vec!["private".into()],
            expires_at: chrono::Utc::now() + chrono::Duration::minutes(1),
        };
        assert!(
            crate::auth::reauthorize_worker_read(&storage, &caller, None, Some(&binding), &task)
                .await
                .is_ok()
        );
        binding.handlers.clear();
        assert!(matches!(
            crate::auth::reauthorize_worker_read(&storage, &caller, None, Some(&binding), &task)
                .await,
            Err(ApiError::Forbidden(_))
        ));
        binding.handlers.push(task.handler_name.clone());
        binding.queues.clear();
        assert!(matches!(
            crate::auth::reauthorize_worker_read(&storage, &caller, None, Some(&binding), &task)
                .await,
            Err(ApiError::Forbidden(_))
        ));
        binding.queues.push("private".into());
        binding.expires_at = chrono::Utc::now() - chrono::Duration::seconds(1);
        assert!(matches!(
            crate::auth::reauthorize_worker_read(&storage, &caller, None, Some(&binding), &task)
                .await,
            Err(ApiError::Unauthorized)
        ));
    }
}
