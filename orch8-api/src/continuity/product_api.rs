//! Stateless public product surface for the Durable Agent Handoff Protocol.

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer as _, Verifier as _};
use orch8_types::continuity_product::{
    CURRENT_PROTOCOL, CommercialContinuityPlan, CompiledPlacementPolicy, ConformanceCertificate,
    ConformanceCheckResult, ExecutionReceipt, GatewayManifest, PortableWorkOffer,
    ProductContractError, ProfileContract, ProtocolVersion, TrustBoundaryProfile,
    compile_placement_policy, issue_conformance_certificate,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use orch8_types::continuity::{ContinuityId, ExecutionEpoch};
use orch8_types::ids::TenantId;

use crate::AppState;
use crate::error::ApiError;

pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route("/continuity/protocol", get(protocol_description))
        .route("/continuity/offers/validate", post(validate_offer))
        .route("/continuity/policies/compile", post(compile_policy))
        .route("/continuity/gateways/validate", post(validate_gateway))
        .route("/continuity/receipts/verify", post(verify_receipt))
        .route("/continuity/conformance/certificates", post(certify))
        .route("/continuity/conformance/badge", post(render_badge))
        .route("/continuity/profiles", get(list_profiles))
        .route(
            "/continuity/profiles/{profile}/offers",
            post(create_profile_offer),
        )
        .route(
            "/continuity/commercial/validate",
            post(validate_commercial_plan),
        )
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct ProtocolDescription {
    name: &'static str,
    version: ProtocolVersion,
    transports: [&'static str; 4],
    invariants: [&'static str; 7],
}

#[utoipa::path(get, path = "/continuity/protocol", tag = "continuity-product",
    responses((status = 200, description = "Versioned handoff protocol and invariants", body = ProtocolDescription)))]
pub(crate) async fn protocol_description() -> Json<ProtocolDescription> {
    Json(ProtocolDescription {
        name: "Durable Agent Handoff Protocol",
        version: CURRENT_PROTOCOL,
        transports: ["http", "mcp", "local_process", "mobile_worker"],
        invariants: [
            "atomic_ownership",
            "monotonic_epoch",
            "stale_owner_rejected",
            "duplicate_effect_fenced",
            "capability_policy_enforced",
            "tenant_isolation",
            "receipt_verifiable",
        ],
    })
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct ValidationResponse {
    valid: bool,
}

#[utoipa::path(post, path = "/continuity/offers/validate", tag = "continuity-product",
    request_body = PortableWorkOffer,
    responses((status = 200, body = ValidationResponse), (status = 400, description = "Offer violates the protocol contract")))]
pub(crate) async fn validate_offer(
    Json(offer): Json<PortableWorkOffer>,
) -> Result<Json<ValidationResponse>, ApiError> {
    offer
        .validate(Utc::now())
        .map_err(|error| contract_error(&error))?;
    Ok(Json(ValidationResponse { valid: true }))
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub(crate) struct CompilePolicyRequest {
    source: String,
}

#[utoipa::path(post, path = "/continuity/policies/compile", tag = "continuity-product",
    request_body = CompilePolicyRequest,
    responses((status = 200, body = CompiledPlacementPolicy), (status = 400, description = "Policy is invalid or contains unknown keys")))]
pub(crate) async fn compile_policy(
    Json(request): Json<CompilePolicyRequest>,
) -> Result<Json<CompiledPlacementPolicy>, ApiError> {
    compile_placement_policy(&request.source)
        .map(Json)
        .map_err(|error| contract_error(&error))
}

#[utoipa::path(post, path = "/continuity/gateways/validate", tag = "continuity-product",
    request_body = GatewayManifest,
    responses((status = 200, body = CompiledPlacementPolicy), (status = 400, description = "Gateway manifest is unsafe or invalid")))]
pub(crate) async fn validate_gateway(
    Json(manifest): Json<GatewayManifest>,
) -> Result<Json<CompiledPlacementPolicy>, ApiError> {
    manifest
        .validate()
        .map(Json)
        .map_err(|error| contract_error(&error))
}

#[utoipa::path(post, path = "/continuity/receipts/verify", tag = "continuity-product",
    request_body = ExecutionReceipt,
    responses((status = 200, body = ValidationResponse), (status = 400, description = "Receipt is incomplete or its digest does not match")))]
pub(crate) async fn verify_receipt(
    Json(receipt): Json<ExecutionReceipt>,
) -> Result<Json<ValidationResponse>, ApiError> {
    receipt.verify().map_err(|error| contract_error(&error))?;
    Ok(Json(ValidationResponse { valid: true }))
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub(crate) struct CertificationRequest {
    subject: String,
    results: Vec<ConformanceCheckResult>,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

#[utoipa::path(post, path = "/continuity/conformance/certificates", tag = "continuity-product",
    request_body = CertificationRequest,
    responses((status = 200, body = ConformanceCertificate), (status = 400, description = "Results or certificate window do not qualify"), (status = 503, description = "Signing is not configured")))]
pub(crate) async fn certify(
    State(state): State<AppState>,
    Json(request): Json<CertificationRequest>,
) -> Result<Json<ConformanceCertificate>, ApiError> {
    let now = Utc::now();
    if request.issued_at < now - chrono::Duration::minutes(5)
        || request.issued_at > now + chrono::Duration::minutes(5)
    {
        return Err(ApiError::InvalidArgument(
            "certificate issued_at must be within five minutes of server time".into(),
        ));
    }
    let crypto = state.continuity_crypto.as_ref().ok_or_else(|| {
        ApiError::Unavailable("continuity certificate signing is not configured".into())
    })?;
    let mut certificate = issue_conformance_certificate(
        request.subject,
        crypto.signing_key_id.clone(),
        &request.results,
        request.issued_at,
        request.expires_at,
    )
    .map_err(|error| contract_error(&error))?;
    sign_certificate(&mut certificate, crypto)?;
    Ok(Json(certificate))
}

fn sign_certificate(
    certificate: &mut ConformanceCertificate,
    crypto: &crate::ContinuityCrypto,
) -> Result<(), ApiError> {
    certificate.signature = None;
    let payload = serde_json::to_vec(certificate)
        .map_err(|error| ApiError::Internal(format!("serialize certificate: {error}")))?;
    certificate.signature = Some(BASE64.encode(crypto.signing_key.sign(&payload).to_bytes()));
    Ok(())
}

#[utoipa::path(get, path = "/continuity/profiles", tag = "continuity-product",
    responses((status = 200, body = [ProfileContract])))]
pub(crate) async fn list_profiles() -> Json<Vec<ProfileContract>> {
    Json(
        TrustBoundaryProfile::ALL
            .into_iter()
            .map(TrustBoundaryProfile::contract)
            .collect(),
    )
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub(crate) struct ProfileOfferRequest {
    tenant_id: TenantId,
    continuity_id: ContinuityId,
    expected_epoch: ExecutionEpoch,
    input: serde_json::Value,
    idempotency_key: String,
}

#[utoipa::path(post, path = "/continuity/profiles/{profile}/offers", tag = "continuity-product",
    params(("profile" = String, Path, description = "Snake-case trust-boundary profile name")),
    request_body = ProfileOfferRequest,
    responses((status = 200, body = PortableWorkOffer), (status = 400, description = "Unknown profile or invalid offer")))]
pub(crate) async fn create_profile_offer(
    Path(profile): Path<String>,
    Json(request): Json<ProfileOfferRequest>,
) -> Result<Json<PortableWorkOffer>, ApiError> {
    let profile: TrustBoundaryProfile = profile.parse().map_err(|error| contract_error(&error))?;
    profile
        .work_offer(
            request.tenant_id,
            request.continuity_id,
            request.expected_epoch,
            request.input,
            request.idempotency_key,
            Utc::now(),
        )
        .map(Json)
        .map_err(|error| contract_error(&error))
}

#[utoipa::path(post, path = "/continuity/conformance/badge", tag = "continuity-product",
    request_body = ConformanceCertificate,
    responses((status = 200, description = "Server-verified SVG badge", content_type = "image/svg+xml", body = String), (status = 400, description = "Certificate is invalid, expired, or tampered")))]
pub(crate) async fn render_badge(
    State(state): State<AppState>,
    Json(mut certificate): Json<ConformanceCertificate>,
) -> Result<Response, ApiError> {
    let crypto = state.continuity_crypto.as_ref().ok_or_else(|| {
        ApiError::Unavailable("continuity certificate signing is not configured".into())
    })?;
    if certificate.issuer != crypto.signing_key_id
        || certificate.expires_at <= Utc::now()
        || certificate.score.score_millipoints < 900
        || !certificate.score.mandatory_failures.is_empty()
    {
        return Err(ApiError::InvalidArgument(
            "certificate is expired, below threshold, or issued by another key".into(),
        ));
    }
    let encoded = certificate
        .signature
        .take()
        .ok_or_else(|| ApiError::InvalidArgument("certificate signature is missing".into()))?;
    let payload = serde_json::to_vec(&certificate)
        .map_err(|error| ApiError::Internal(format!("serialize certificate: {error}")))?;
    let signature_bytes = BASE64
        .decode(encoded)
        .map_err(|_| ApiError::InvalidArgument("certificate signature is invalid".into()))?;
    let signature = Signature::from_slice(&signature_bytes)
        .map_err(|_| ApiError::InvalidArgument("certificate signature is invalid".into()))?;
    crypto
        .signing_key
        .verifying_key()
        .verify(&payload, &signature)
        .map_err(|_| ApiError::InvalidArgument("certificate signature is invalid".into()))?;
    Ok(([(CONTENT_TYPE, "image/svg+xml")], certificate.badge_svg()).into_response())
}

#[utoipa::path(post, path = "/continuity/commercial/validate", tag = "continuity-product",
    request_body = CommercialContinuityPlan,
    responses((status = 200, body = ValidationResponse), (status = 400, description = "Deployment violates a mandatory commercial invariant")))]
pub(crate) async fn validate_commercial_plan(
    Json(plan): Json<CommercialContinuityPlan>,
) -> Result<Json<ValidationResponse>, ApiError> {
    plan.validate().map_err(|error| contract_error(&error))?;
    Ok(Json(ValidationResponse { valid: true }))
}

fn contract_error(error: &ProductContractError) -> ApiError {
    ApiError::InvalidArgument(error.to_string())
}

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse as _;
    use ed25519_dalek::{Signature, Verifier as _};
    use utoipa::OpenApi as _;

    use super::*;
    use orch8_types::continuity_product::{RelayDeployment, score_conformance};

    #[tokio::test]
    async fn protocol_endpoint_exposes_transport_and_safety_contract() {
        let Json(description) = protocol_description().await;
        assert_eq!(description.version, CURRENT_PROTOCOL);
        assert!(description.transports.contains(&"mcp"));
        assert!(description.invariants.contains(&"duplicate_effect_fenced"));
    }

    #[tokio::test]
    async fn policy_endpoint_returns_compiled_core_policy() {
        let Json(compiled) = compile_policy(Json(CompilePolicyRequest {
            source: "classification=restricted;runtime_kinds=mobile;min_trust=attested".into(),
        }))
        .await
        .unwrap();
        assert_eq!(compiled.policy.rules.len(), 1);
    }

    #[tokio::test]
    async fn profiles_endpoint_exposes_every_high_score_vertical_contract() {
        let Json(profiles) = list_profiles().await;
        assert_eq!(profiles.len(), 10);
        assert!(
            profiles
                .iter()
                .all(|profile| profile.requires_signed_receipt)
        );
    }

    #[tokio::test]
    async fn profile_offer_endpoint_materializes_policy_and_receipt_requirements() {
        let Json(offer) = create_profile_offer(
            Path("executive_airlock".into()),
            Json(ProfileOfferRequest {
                tenant_id: TenantId::new("acme").unwrap(),
                continuity_id: ContinuityId::new(),
                expected_epoch: ExecutionEpoch::initial(),
                input: serde_json::json!({"action": "deploy"}),
                idempotency_key: "deploy-once".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(offer.handler, "airlock.approve");
        assert!(offer.receipt_required);
        assert!(offer.requirements.requires_human_ui);
    }

    #[tokio::test]
    async fn commercial_endpoint_rejects_unsafe_relay() {
        let result = validate_commercial_plan(Json(CommercialContinuityPlan {
            deployment: RelayDeployment::Hosted,
            tenant_isolation: false,
            signed_receipts: true,
            conformance_required: true,
            evidence_retention_days: 30,
            allowed_protocol_majors: vec![CURRENT_PROTOCOL.major],
            oem_product_id: None,
            usage_meter: Some("handoff".into()),
        }))
        .await;
        assert_eq!(
            result.unwrap_err().into_response().status(),
            axum::http::StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn empty_conformance_run_scores_zero() {
        assert_eq!(score_conformance(&[]).score_millipoints, 0);
    }

    #[test]
    fn conformance_certificate_is_signed_by_server_continuity_key() {
        let crypto = crate::ContinuityCrypto::from_master_key(&"11".repeat(32)).unwrap();
        let now = Utc::now();
        let mut results = Vec::new();
        for check in orch8_types::continuity_product::ConformanceCheck::ALL {
            results.push(ConformanceCheckResult {
                check,
                passed: true,
                evidence_sha256: "0".repeat(64),
                duration_ms: 1,
                finding: None,
            });
        }
        let mut certificate = issue_conformance_certificate(
            "sdk".into(),
            crypto.signing_key_id.clone(),
            &results,
            now,
            now + chrono::Duration::days(30),
        )
        .unwrap();
        sign_certificate(&mut certificate, &crypto).unwrap();
        let encoded = certificate.signature.take().unwrap();
        let payload = serde_json::to_vec(&certificate).unwrap();
        let bytes = BASE64.decode(encoded).unwrap();
        let signature = Signature::from_slice(&bytes).unwrap();
        crypto
            .signing_key
            .verifying_key()
            .verify(&payload, &signature)
            .unwrap();
    }

    #[test]
    fn generated_openapi_contains_every_product_endpoint() {
        let document = serde_json::to_value(crate::openapi::ApiDoc::openapi()).unwrap();
        let paths = document["paths"].as_object().unwrap();
        for path in [
            "/continuity/protocol",
            "/continuity/offers/validate",
            "/continuity/policies/compile",
            "/continuity/gateways/validate",
            "/continuity/receipts/verify",
            "/continuity/conformance/certificates",
            "/continuity/conformance/badge",
            "/continuity/profiles",
            "/continuity/profiles/{profile}/offers",
            "/continuity/commercial/validate",
        ] {
            assert!(paths.contains_key(path), "OpenAPI is missing {path}");
        }
    }
}
