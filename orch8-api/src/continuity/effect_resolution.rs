//! Explicit operator evidence settlement; never implicit worker reassignment.
use super::*;
use orch8_storage::effect_resolution::{EffectResolutionCommit, effect_resolution_digest};

#[derive(Debug, Deserialize)]
pub(super) struct ResolveEffectRequest {
    tenant_id: TenantId,
    state: EffectState,
    provider_receipt_id: Option<String>,
}

pub(super) async fn resolve(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<orch8_types::continuity::EffectId>,
    Json(body): Json<ResolveEffectRequest>,
) -> Result<Json<EffectReceipt>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let expected = state
        .storage
        .get_effect_receipt(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "effect receipt"))?
        .ok_or_else(|| ApiError::NotFound(format!("effect receipt {id}")))?;
    let mut next = expected.clone();
    next.provider_receipt_id = body.provider_receipt_id;
    // Replicas can disagree on wall time. Preserve the durable timestamp
    // floor without treating clock skew as invalid reconciliation evidence.
    next.transition(body.state, Utc::now().max(expected.updated_at))
        .map_err(|error| ApiError::Conflict(error.to_string()))?;
    let execution = state
        .storage
        .get_continuity_execution(&tenant_id, expected.continuity_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    let digest = effect_resolution_digest(&next)
        .map_err(|error| ApiError::from_storage(error, "resolution evidence"))?;
    for _ in 0..8 {
        let previous = state
            .storage
            .get_provenance_head(&tenant_id, execution.continuity_id)
            .await
            .map_err(|error| ApiError::from_storage(error, "provenance head"))?
            .map(|entry| entry.entry_sha256);
        let mut entry = orch8_engine::continuity::build_provenance_entry_with_summary(
            &execution,
            "effect_resolved",
            Some("effect receipt resolved".into()),
            &digest,
            previous,
            Utc::now(),
        );
        if let Some(signer) = state.provenance_signer() {
            entry = orch8_engine::continuity::sign_provenance_entry(
                entry,
                signer.key_id.to_owned(),
                signer.signing_key,
            );
        }
        match state
            .storage
            .resolve_effect_with_provenance(&expected, &next, &entry)
            .await
            .map_err(|error| ApiError::from_storage(error, "effect resolution"))?
        {
            EffectResolutionCommit::Applied => return Ok(Json(next)),
            EffectResolutionCommit::Stale => {
                return Err(ApiError::Conflict(
                    "effect receipt or continuity epoch changed; reload before resolving".into(),
                ));
            }
            EffectResolutionCommit::ProvenanceChanged => tokio::task::yield_now().await,
        }
    }
    Err(ApiError::Conflict(
        "provenance changed concurrently; retry resolution".into(),
    ))
}
