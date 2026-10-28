//! Grant redemption and mailbox exposure share a durable storage transaction.
use crate::{AppState, error::ApiError};
use axum::{Json, extract::State};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::Utc;
use orch8_publisher::grant::{SignedContinuationGrant, verify_signed_continuation_grant};
use orch8_storage::delegation_publication::{
    DelegatedStep, DelegationPlan, DelegationPublication, DelegationPublishIntent,
    DelegationPublishResult, DelegationRequest, sequence_sha256,
};
use orch8_types::{
    continuity::{ContinuityExecution, EffectId, EffectReceipt, EffectState},
    continuity_advanced::DeviceDelegation,
    ids::TenantId,
    sequence::SequenceDefinition,
};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

#[derive(Deserialize)]
pub(super) struct ClaimDelegationRequest {
    tenant_id: TenantId,
    delegation: DeviceDelegation,
    signed_grant: SignedContinuationGrant,
    token: String,
    #[serde(default)]
    input: serde_json::Value,
    #[serde(default)]
    step: Option<DelegatedStep>,
}
#[derive(Serialize)]
pub(super) struct ClaimDelegationResponse {
    #[serde(flatten)]
    delegation: DeviceDelegation,
    mailbox_task_id: Option<uuid::Uuid>,
}
fn storage_error(error: orch8_types::error::StorageError) -> ApiError {
    ApiError::from_storage(error, "delegation publication")
}
fn response(receipt: DelegationPublication) -> Json<ClaimDelegationResponse> {
    Json(ClaimDelegationResponse {
        delegation: receipt.delegation,
        mailbox_task_id: Some(receipt.task_id),
    })
}

pub(super) async fn claim_delegation(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    caller: crate::auth::WorkerCaller,
    binding: crate::browser_sessions::OptionalBinding,
    Json(body): Json<ClaimDelegationRequest>,
) -> Result<Json<ClaimDelegationResponse>, ApiError> {
    let tenant = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if body.delegation.tenant_id != tenant || body.signed_grant.grant.tenant_id != tenant {
        return Err(ApiError::NotFound("delegation".into()));
    }
    crate::device_sessions::enforce_bound_runtime(&binding, body.delegation.source_runtime_id)?;
    let token =
        Zeroizing::new(BASE64.decode(&body.token).map_err(|_| {
            ApiError::InvalidArgument("delegation token is not valid base64".into())
        })?);
    if token.len() != 32 {
        return Err(ApiError::InvalidArgument(
            "delegation token must decode to 32 bytes".into(),
        ));
    }
    let request = DelegationRequest {
        delegation: body.delegation,
        grant: body.signed_grant.grant.clone(),
        nonce_sha256: super::hex_sha256(&token),
        input: body.input,
        step: body.step,
        actor: caller.0,
        authority_expires_at: binding.as_ref().map(|binding| binding.0.expires_at),
    };
    request.validate().map_err(storage_error)?;
    if let Some(receipt) = state
        .storage
        .get_delegation_publication(&tenant, request.delegation.id.into_uuid())
        .await
        .map_err(storage_error)?
    {
        return if receipt.matches(&request).map_err(storage_error)? {
            Ok(response(receipt))
        } else {
            Err(ApiError::Conflict(
                "delegation id already names a different publication".into(),
            ))
        };
    }
    let crypto = state.continuity_crypto.as_ref().ok_or_else(|| {
        ApiError::Unavailable(
            "device delegation is disabled without a configured engine encryption key".into(),
        )
    })?;
    let trusted = BASE64.encode(crypto.signing_key.verifying_key().to_bytes());
    verify_signed_continuation_grant(&body.signed_grant, &[trusted])
        .map_err(|error| ApiError::Conflict(error.to_string()))?;
    orch8_engine::continuity_advanced::validate_device_delegation(
        &request.delegation,
        &request.grant,
        Utc::now(),
    )
    .map_err(|error| ApiError::Conflict(error.to_string()))?;
    let sequence = sequence(&state, &request).await?;
    let execution = execution(&state, &request).await?;
    let payload = request.request_sha256().map_err(storage_error)?;
    let entry = entry(&state, &execution, &payload).await?;
    let mut intent = prepare(&state, request, execution.clone(), sequence, entry).await?;
    for _ in 0..8 {
        match state
            .storage
            .publish_delegation(&intent)
            .await
            .map_err(storage_error)?
        {
            DelegationPublishResult::Published(receipt) => return Ok(response(*receipt)),
            DelegationPublishResult::ProvenanceChanged => {
                intent = intent
                    .with_entry(self::entry(&state, &execution, &payload).await?)
                    .map_err(storage_error)?;
            }
        }
    }
    Err(ApiError::Conflict(
        "delegation provenance changed concurrently; retry the identical request".into(),
    ))
}
async fn sequence(
    state: &AppState,
    request: &DelegationRequest,
) -> Result<SequenceDefinition, ApiError> {
    let d = &request.delegation;
    let actual = state
        .storage
        .get_sequence(d.sub_sequence_id)
        .await
        .map_err(storage_error)?;
    let Some(step) = &request.step else {
        return actual
            .filter(|sequence| sequence.tenant_id == d.tenant_id)
            .ok_or_else(|| ApiError::NotFound("delegated sub-sequence".into()));
    };
    let id = orch8_engine::delegation::step_sequence_id(
        d.tenant_id.as_str(),
        &step.handler,
        &step.block_id,
    );
    if id != d.sub_sequence_id.into_uuid() {
        return Err(ApiError::InvalidArgument(
            "delegation.sub_sequence_id is not the delegated step's sequence id".into(),
        ));
    }
    let candidate: SequenceDefinition =
        serde_json::from_value(orch8_engine::delegation::step_sequence_document(
            d.tenant_id.as_str(),
            &step.handler,
            &step.block_id,
        ))
        .map_err(|error| ApiError::InvalidArgument(format!("delegated step sequence: {error}")))?;
    candidate
        .validate()
        .map_err(|error| ApiError::validation(error.catalog_key(), error.to_string()))?;
    if let Some(actual) = actual {
        if sequence_sha256(&actual).map_err(storage_error)?
            != sequence_sha256(&candidate).map_err(storage_error)?
        {
            return Err(ApiError::Conflict(
                "delegated step id names a different sequence definition".into(),
            ));
        }
        return Ok(actual);
    }
    Ok(candidate)
}
async fn execution(
    state: &AppState,
    request: &DelegationRequest,
) -> Result<ContinuityExecution, ApiError> {
    let d = &request.delegation;
    state
        .storage
        .get_continuity_execution(&d.tenant_id, d.parent_continuity_id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| ApiError::NotFound("delegation execution".into()))
}
async fn entry(
    state: &AppState,
    execution: &ContinuityExecution,
    payload: &str,
) -> Result<orch8_types::continuity::ProvenanceEntry, ApiError> {
    let previous = state
        .storage
        .get_provenance_head(&execution.tenant_id, execution.continuity_id)
        .await
        .map_err(storage_error)?
        .map(|entry| entry.entry_sha256);
    let mut entry = orch8_engine::continuity::build_provenance_entry_with_summary(
        execution,
        "device_delegation",
        Some("destination-bound device delegation claimed".into()),
        payload,
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
    Ok(entry)
}
async fn prepare(
    state: &AppState,
    request: DelegationRequest,
    owner: ContinuityExecution,
    sequence: SequenceDefinition,
    entry: orch8_types::continuity::ProvenanceEntry,
) -> Result<DelegationPublishIntent, ApiError> {
    use orch8_storage::delegation_publication::{mailbox_task, proxy_instance, proxy_owner};
    let now = Utc::now();
    let hosted = state
        .storage
        .get_instance(owner.current_instance_id)
        .await
        .map_err(storage_error)?;
    let sub_tenant = hosted
        .as_ref()
        .filter(|parent| parent.tenant_id == owner.tenant_id)
        .and_then(|parent| parent.sub_tenant.clone());
    let (parent, proxy) = match hosted.filter(|parent| {
        parent.tenant_id == owner.tenant_id
            && !orch8_engine::delegation::is_delegation_proxy(parent)
    }) {
        Some(parent) => (parent, false),
        None => (
            proxy_instance(&owner, &request.delegation, &sequence, sub_tenant, now),
            true,
        ),
    };
    let task_owner = if proxy {
        proxy_owner(&owner.tenant_id, parent.id, now)
    } else {
        owner.clone()
    };
    let effect_id = EffectId::new();
    let task = mailbox_task(
        &parent,
        &request.delegation,
        &sequence,
        &request.input,
        effect_id,
        task_owner.epoch,
        now,
    );
    let proof = orch8_storage::worker_admission::dispatch_effect(&task, effect_id);
    let effect = EffectReceipt {
        id: effect_id,
        tenant_id: owner.tenant_id.clone(),
        continuity_id: task_owner.continuity_id,
        epoch: task_owner.epoch,
        instance_id: parent.id,
        block_id: task.block_id.clone(),
        kind: proof.kind,
        state: EffectState::Planned,
        destination_fingerprint: proof.destination_fingerprint,
        idempotency_key: None,
        request_sha256: proof.request_sha256,
        provider_receipt_id: None,
        attempt: 0,
        created_at: now,
        updated_at: now,
    };
    DelegationPublishIntent::new(DelegationPlan {
        request,
        parent_owner: owner,
        task_owner,
        parent,
        proxy,
        sequence,
        task,
        effect,
        entry,
    })
    .map_err(storage_error)
}
