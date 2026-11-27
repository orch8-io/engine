use axum::Json;
use axum::extract::{Path, Query, State};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{Duration, Utc};
use orch8_engine::continuity_advanced::{FederationSigningRequest, sign_federation_envelope};
use orch8_types::continuity::{ContinuityId, ExecutionEpoch, RuntimeId};
use orch8_types::continuity_advanced::{
    FederationPeerId, FederationSignature, RecordedFederationSignature,
};
use orch8_types::ids::TenantId;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{TenantQuery, hex_sha256, query_tenant};
use crate::{AppState, auth::OptionalTenant, error::ApiError};

const KIND: &str = "federation_signed";

#[derive(Deserialize, utoipa::ToSchema)]
pub(crate) struct RecordFederationSignatureRequest {
    tenant_id: TenantId,
    peer_id: FederationPeerId,
    destination_runtime_id: RuntimeId,
    expected_epoch: ExecutionEpoch,
    payload_base64: String,
    ttl_seconds: u32,
}

/// Read the signed envelope and its evidence by its saved request ID.
#[utoipa::path(
    get,
    operation_id = "read_federation_signature",
    path = "/continuity/executions/{id}/federation-signatures/{request_id}",
    tag = "continuity",
    params(
        ("id" = ContinuityId, Path, description = "Continuity execution ID"),
        ("request_id" = Uuid, Path, description = "Signing request ID"),
        ("tenant_id" = String, Query, description = "Tenant ID")
    ),
    responses(
        (status = 200, description = "Exact signed envelope and provenance", body = RecordedFederationSignature),
        (status = 404, description = "No signature matches the request")
    )
)]
pub(crate) async fn read(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path((continuity, request_id)): Path<(ContinuityId, Uuid)>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<RecordedFederationSignature>, ApiError> {
    let tenant = query_tenant(&tenant_ctx, &query.tenant_id)?;
    stored(&state, &tenant, continuity, request_id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::NotFound("federation signature".into()))
}

/// Sign once per request ID and bind the returned envelope to its persisted provenance.
#[utoipa::path(
    put,
    operation_id = "record_federation_signature",
    path = "/continuity/executions/{id}/federation-signatures/{request_id}",
    tag = "continuity",
    params(
        ("id" = ContinuityId, Path, description = "Continuity execution ID"),
        ("request_id" = Uuid, Path, description = "Caller-generated signing request ID")
    ),
    request_body = RecordFederationSignatureRequest,
    responses(
        (status = 200, description = "Stored signature and provenance", body = RecordedFederationSignature),
        (status = 404, description = "Continuity execution not found"),
        (status = 409, description = "The signing request conflicts with current state or a stored signature")
    )
)]
#[allow(clippy::too_many_lines)] // review, signing, and durable replay follow the same protocol
pub(crate) async fn sign(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path((continuity, request_id)): Path<(ContinuityId, Uuid)>,
    Json(body): Json<RecordFederationSignatureRequest>,
) -> Result<Json<RecordedFederationSignature>, ApiError> {
    let tenant = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if request_id.is_nil() || !(1..=300).contains(&body.ttl_seconds) {
        return Err(ApiError::InvalidArgument(
            "choose a signing request ID and a TTL of 1–300 seconds".into(),
        ));
    }
    if body.payload_base64.len() > 16 * 1024 * 1024 {
        return Err(ApiError::PayloadTooLarge(
            "federation payload exceeds the encoded 16 MiB limit".into(),
        ));
    }
    let payload = BASE64
        .decode(&body.payload_base64)
        .map_err(|_| ApiError::InvalidArgument("payload_base64 is invalid".into()))?;
    let request_digest = digest(&(
        body.peer_id,
        body.destination_runtime_id,
        body.expected_epoch,
        body.ttl_seconds,
        hex_sha256(&payload),
    ))?;
    if let Some(record) = stored(&state, &tenant, continuity, request_id).await? {
        return replay(record, &request_digest);
    }
    let crypto = state.continuity_crypto.as_ref().ok_or_else(|| {
        ApiError::Conflict("federation signing is disabled without an Engine signing key".into())
    })?;
    let execution = state
        .storage
        .get_continuity_execution(&tenant, continuity)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    if execution.epoch != body.expected_epoch {
        return Err(ApiError::Conflict(
            "review the current execution epoch".into(),
        ));
    }
    if state
        .storage
        .get_runtime_capability(&tenant, body.destination_runtime_id, Utc::now())
        .await
        .map_err(|error| ApiError::from_storage(error, "runtime capabilities"))?
        .is_none_or(|runtime| runtime.draining)
    {
        return Err(ApiError::Conflict(
            "destination runtime is unavailable or draining".into(),
        ));
    }
    let issued_at = Utc::now();
    let envelope = sign_federation_envelope(
        &crypto.signing_key,
        FederationSigningRequest {
            peer_id: body.peer_id,
            tenant_id: tenant.clone(),
            continuity_id: continuity,
            epoch: body.expected_epoch,
            destination_runtime_id: body.destination_runtime_id,
            payload: &payload,
            issued_at,
            expires_at: issued_at + Duration::seconds(i64::from(body.ttl_seconds)),
        },
    );
    let envelope_digest = digest(&envelope)?;
    let evidence = FederationSignature {
        request_id,
        request_sha256: request_digest,
        envelope,
    };
    for _ in 0..8 {
        let previous = state
            .storage
            .get_provenance_head(&tenant, continuity)
            .await
            .map_err(|error| ApiError::from_storage(error, "provenance head"))?
            .map(|entry| entry.entry_sha256);
        let mut provenance = orch8_engine::continuity::build_provenance_entry_with_summary(
            &execution,
            KIND,
            Some("outbound federation envelope signed".into()),
            &envelope_digest,
            previous,
            issued_at,
        );
        provenance.id = signature_id(&tenant, continuity, request_id);
        if let Some(signer) = state.provenance_signer() {
            provenance = orch8_engine::continuity::sign_provenance_entry(
                provenance,
                signer.key_id,
                signer.signing_key,
            );
        }
        let record = RecordedFederationSignature {
            provenance,
            federation_signature: evidence.clone(),
        };
        if state
            .storage
            .commit_federation_signature(&record)
            .await
            .map_err(|error| ApiError::from_storage(error, "federation signature"))?
        {
            return Ok(Json(record));
        }
        if let Some(record) = stored(&state, &tenant, continuity, request_id).await? {
            return replay(record, &evidence.request_sha256);
        }
        tokio::task::yield_now().await;
    }
    Err(ApiError::Conflict(
        "the signature could not be stored for this request and epoch".into(),
    ))
}

async fn stored(
    state: &AppState,
    tenant: &TenantId,
    continuity: ContinuityId,
    request_id: Uuid,
) -> Result<Option<RecordedFederationSignature>, ApiError> {
    let id = signature_id(tenant, continuity, request_id);
    let result = state
        .storage
        .get_federation_signature(tenant, continuity, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "federation signature"))?;
    if let Some(record) = &result {
        let proof = &record.provenance;
        let signature = &record.federation_signature;
        if proof.id != id
            || proof.tenant_id != *tenant
            || proof.continuity_id != continuity
            || proof.kind != KIND
            || signature.request_id != request_id
            || signature.envelope.tenant_id != *tenant
            || signature.envelope.continuity_id != continuity
            || signature.envelope.epoch != proof.epoch
            || proof.payload_sha256 != digest(&signature.envelope)?
        {
            return Err(ApiError::Internal(
                "stored federation signature binding is invalid".into(),
            ));
        }
    }
    Ok(result)
}

fn replay(
    record: RecordedFederationSignature,
    request_digest: &str,
) -> Result<Json<RecordedFederationSignature>, ApiError> {
    if record.federation_signature.request_sha256 == request_digest {
        Ok(Json(record))
    } else {
        Err(ApiError::Conflict(
            "this request ID belongs to another signing request".into(),
        ))
    }
}

fn digest(value: &impl serde::Serialize) -> Result<String, ApiError> {
    serde_json::to_vec(value)
        .map(|data| hex_sha256(&data))
        .map_err(|error| ApiError::Internal(format!("serialize signature evidence: {error}")))
}

fn signature_id(tenant: &TenantId, continuity: ContinuityId, request: Uuid) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"orch8-continuity-federation-signature-v1\0");
    hash.update(continuity.into_uuid().as_bytes());
    hash.update(request.as_bytes());
    hash.update(tenant.as_str().as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}
