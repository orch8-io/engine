use axum::Json;
use axum::extract::{Path, Query, State};
use orch8_types::continuity::ContinuityId;
use orch8_types::continuity_advanced::{
    FederationAcceptance, FederationMessageId, FederationPeerId, RecordedFederationAcceptance,
};
use orch8_types::ids::TenantId;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{VerifyFederationRequest, inspect_federation_input, query_tenant};
use crate::{AppState, auth::OptionalTenant, error::ApiError};

const KIND: &str = "federation_received";

#[derive(Deserialize)]
pub(crate) struct FederationAcceptanceQuery {
    tenant_id: String,
    continuity_id: ContinuityId,
    peer_id: FederationPeerId,
    envelope_sha256: String,
}

/// Read exact acceptance evidence independently of the replay receipt's retention window.
#[utoipa::path(
    get,
    operation_id = "read_federation_acceptance",
    path = "/continuity/federation/acceptances/{id}",
    tag = "continuity",
    params(
        ("id" = FederationMessageId, Path, description = "Signed message ID"),
        ("tenant_id" = String, Query, description = "Tenant ID"),
        ("continuity_id" = ContinuityId, Query, description = "Continuity execution ID"),
        ("peer_id" = FederationPeerId, Query, description = "Configured peer ID"),
        ("envelope_sha256" = String, Query, description = "SHA-256 returned by inspection")
    ),
    responses(
        (status = 200, description = "Exact stored acceptance and provenance", body = RecordedFederationAcceptance),
        (status = 404, description = "No stored acceptance matches the selected envelope")
    )
)]
pub(crate) async fn read(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path(message): Path<FederationMessageId>,
    Query(query): Query<FederationAcceptanceQuery>,
) -> Result<Json<RecordedFederationAcceptance>, ApiError> {
    let tenant = query_tenant(&tenant_ctx, &query.tenant_id)?;
    if query.envelope_sha256.len() != 64
        || !query
            .envelope_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ApiError::InvalidArgument(
            "use a lowercase envelope SHA-256".into(),
        ));
    }
    stored(&state, &tenant, query.continuity_id, query.peer_id, message)
        .await?
        .filter(|record| record.federation_acceptance.envelope_sha256 == query.envelope_sha256)
        .map(Json)
        .ok_or_else(|| ApiError::NotFound("federation acceptance".into()))
}

/// Accept a verified message once and store replay protection and provenance atomically.
#[utoipa::path(
    post,
    operation_id = "accept_federation_envelope",
    path = "/continuity/federation/acceptances",
    tag = "continuity",
    request_body = VerifyFederationRequest,
    responses(
        (status = 200, description = "Exact acceptance stored or already recorded", body = RecordedFederationAcceptance),
        (status = 404, description = "Envelope or continuity execution not found"),
        (status = 409, description = "Verification failed or the message could not be accepted")
    )
)]
pub(crate) async fn accept(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Json(body): Json<VerifyFederationRequest>,
) -> Result<Json<RecordedFederationAcceptance>, ApiError> {
    let tenant = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let envelope = &body.envelope;
    let (execution, envelope_sha256, inspected_at) =
        inspect_federation_input(&state, &tenant, &body).await?;
    let evidence = FederationAcceptance {
        peer_id: envelope.peer_id,
        message_id: envelope.id,
        destination_runtime_id: envelope.destination_runtime_id,
        payload_sha256: envelope.payload_sha256.clone(),
        envelope_sha256,
    };
    let replay_until =
        orch8_engine::continuity_advanced::federation_replay_deadline(envelope.expires_at);
    let id = acceptance_id(&tenant, evidence.peer_id, evidence.message_id);
    for _ in 0..8 {
        if let Some(record) = stored(
            &state,
            &tenant,
            execution.continuity_id,
            evidence.peer_id,
            evidence.message_id,
        )
        .await?
        {
            if record.federation_acceptance == evidence {
                return Ok(Json(record));
            }
            return Err(ApiError::Conflict(
                "the message ID belongs to a different federation envelope".into(),
            ));
        }
        let previous = state
            .storage
            .get_provenance_head(&tenant, execution.continuity_id)
            .await
            .map_err(|error| ApiError::from_storage(error, "provenance head"))?
            .map(|entry| entry.entry_sha256);
        let mut provenance = orch8_engine::continuity::build_provenance_entry_with_summary(
            &execution,
            KIND,
            Some("configured-peer federation envelope accepted".into()),
            &evidence.envelope_sha256,
            previous,
            inspected_at,
        );
        provenance.id = id;
        if let Some(signer) = state.provenance_signer() {
            provenance = orch8_engine::continuity::sign_provenance_entry(
                provenance,
                signer.key_id,
                signer.signing_key,
            );
        }
        let record = RecordedFederationAcceptance {
            provenance,
            federation_acceptance: evidence.clone(),
        };
        if state
            .storage
            .commit_federation_acceptance(&record, replay_until)
            .await
            .map_err(|error| ApiError::from_storage(error, "federation acceptance"))?
        {
            return Ok(Json(record));
        }
        tokio::task::yield_now().await;
    }
    Err(ApiError::Conflict(
        "the envelope could not be accepted for this message and epoch; inspect its saved evidence"
            .into(),
    ))
}

async fn stored(
    state: &AppState,
    tenant: &TenantId,
    continuity: ContinuityId,
    peer: FederationPeerId,
    message: FederationMessageId,
) -> Result<Option<RecordedFederationAcceptance>, ApiError> {
    let id = acceptance_id(tenant, peer, message);
    let result = state
        .storage
        .get_federation_acceptance(tenant, continuity, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "federation acceptance"))?;
    if let Some(record) = &result
        && (record.provenance.id != id
            || record.provenance.tenant_id != *tenant
            || record.provenance.continuity_id != continuity
            || record.provenance.kind != KIND
            || record.provenance.payload_sha256 != record.federation_acceptance.envelope_sha256
            || record.federation_acceptance.peer_id != peer
            || record.federation_acceptance.message_id != message)
    {
        return Err(ApiError::Internal(
            "stored federation acceptance binding is invalid".into(),
        ));
    }
    Ok(result)
}

fn acceptance_id(tenant: &TenantId, peer: FederationPeerId, message: FederationMessageId) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"orch8-continuity-federation-acceptance-v1\0");
    hash.update(peer.into_uuid().as_bytes());
    hash.update(message.into_uuid().as_bytes());
    hash.update(tenant.as_str().as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}
