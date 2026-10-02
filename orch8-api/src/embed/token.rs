//! `o8e1` scoped embed tokens.
//!
//! `o8e1.<b64url(json payload)>.<b64url(HMAC-SHA256(secret, "o8e1." + b64payload))>`
//! where `secret` is the hex-decoded `[embed] token_secret`. Tokens are
//! stateless, bound to one tenant and one sub-tenant, carry an explicit scope
//! list and optional sequence allowlist, and live at most one hour.

use axum::extract::{FromRequestParts, State};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::IntoResponse;
use axum::{Extension, Json};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::ids::TenantId;
use orch8_types::sub_tenant::validate_sub_tenant;

use crate::AppState;
use crate::auth::OptionalTenant;
use crate::error::ApiError;

pub const TOKEN_PREFIX: &str = "o8e1";
/// Env equivalent of `[embed] token_secret`.
pub const SECRET_ENV: &str = "ORCH8_EMBED_TOKEN_SECRET";
/// Env equivalent of `[embed] allowed_origins`.
pub const ALLOWED_ORIGINS_ENV: &str = "ORCH8_EMBED_ALLOWED_ORIGINS";
pub const MIN_SECRET_BYTES: usize = 32;
pub const MAX_TTL_SECS: i64 = 3_600;
pub const DEFAULT_TTL_SECS: i64 = 900;
/// Tolerated clock skew for `iat` in the future (tokens minted elsewhere).
const MAX_IAT_SKEW_SECS: i64 = 60;
const MAX_TOKEN_BYTES: usize = 8 * 1024;
const MAX_SEQUENCES: usize = 256;
const MAX_SEQUENCE_NAME_BYTES: usize = 256;

/// What an embed token may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub enum EmbedScope {
    #[serde(rename = "runs:read")]
    RunsRead,
    #[serde(rename = "runs:start")]
    RunsStart,
    #[serde(rename = "approvals:resolve")]
    ApprovalsResolve,
    #[serde(rename = "sequences:read")]
    SequencesRead,
    #[serde(rename = "builder:edit")]
    BuilderEdit,
}

impl EmbedScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RunsRead => "runs:read",
            Self::RunsStart => "runs:start",
            Self::ApprovalsResolve => "approvals:resolve",
            Self::SequencesRead => "sequences:read",
            Self::BuilderEdit => "builder:edit",
        }
    }
}

/// Signed claim set (field names are the wire contract).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbedClaims {
    pub v: u8,
    pub tid: String,
    pub sub: String,
    pub scp: Vec<EmbedScope>,
    pub seq: Option<Vec<String>>,
    pub iat: i64,
    pub exp: i64,
    pub jti: Uuid,
}

/// Verified embed principal, bound to one tenant + sub-tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbedPrincipal {
    pub tenant_id: TenantId,
    pub sub_tenant: String,
    pub scopes: Vec<EmbedScope>,
    /// Sequence-name allowlist; `None` = every sequence visible to the
    /// sub-tenant.
    pub sequences: Option<Vec<String>>,
    pub token_id: Uuid,
    pub expires_at: DateTime<Utc>,
}

impl EmbedPrincipal {
    pub fn require(&self, scope: EmbedScope) -> Result<(), ApiError> {
        if self.scopes.contains(&scope) {
            Ok(())
        } else {
            Err(ApiError::EmbedScopeDenied(format!(
                "embed token lacks the {} scope",
                scope.as_str()
            )))
        }
    }

    pub fn require_any(&self, scopes: &[EmbedScope]) -> Result<(), ApiError> {
        if scopes.iter().any(|s| self.scopes.contains(s)) {
            return Ok(());
        }
        let names: Vec<&str> = scopes.iter().map(|s| s.as_str()).collect();
        Err(ApiError::EmbedScopeDenied(format!(
            "embed token lacks one of the scopes: {}",
            names.join(", ")
        )))
    }

    /// Whether the token's sequence allowlist admits `name`.
    #[must_use]
    pub fn allows_sequence(&self, name: &str) -> bool {
        self.sequences
            .as_ref()
            .is_none_or(|list| list.iter().any(|allowed| allowed == name))
    }

    /// How (if at all) this principal sees `seq`:
    /// * the sub-tenant's own sequences — always;
    /// * gallery templates (tenant-level, `embed-gallery`) — always, read-only;
    /// * other tenant-level sequences — when the token's allowlist admits them;
    /// * anything of another tenant or sub-tenant — never.
    #[must_use]
    pub fn visibility(
        &self,
        seq: &orch8_types::sequence::SequenceDefinition,
    ) -> Option<SequenceVisibility> {
        if seq.tenant_id != self.tenant_id {
            return None;
        }
        if let Some(owner) = seq.sub_tenant.as_deref() {
            return (owner == self.sub_tenant).then_some(SequenceVisibility::Owned);
        }
        if self.allows_sequence(&seq.name) {
            Some(SequenceVisibility::Tenant)
        } else if orch8_types::sub_tenant::SequenceEmbed::is_gallery_template(seq) {
            Some(SequenceVisibility::Gallery)
        } else {
            None
        }
    }

    /// Whether runs of `seq` may be started/viewed with this token: owned
    /// sequences, or tenant-level ones admitted by the allowlist.
    #[must_use]
    pub fn can_run(&self, seq: &orch8_types::sequence::SequenceDefinition) -> bool {
        matches!(
            self.visibility(seq),
            Some(SequenceVisibility::Owned | SequenceVisibility::Tenant)
        )
    }
}

/// How an embed principal sees a sequence (see [`EmbedPrincipal::visibility`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceVisibility {
    Owned,
    Tenant,
    /// Gallery template not otherwise admitted: listable/readable only.
    Gallery,
}

/// HMAC-SHA256 signer/verifier keyed by the hex-decoded embed secret.
#[derive(Clone)]
pub struct EmbedSigner {
    key: zeroize::Zeroizing<Vec<u8>>,
}

impl std::fmt::Debug for EmbedSigner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("EmbedSigner(<redacted>)")
    }
}

fn decode_hex(input: &str) -> Result<Vec<u8>, String> {
    let input = input.trim();
    if !input.len().is_multiple_of(2) {
        return Err("embed token secret must be an even-length hex string".into());
    }
    let nibble = |b: u8| -> Result<u8, String> {
        match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            b'A'..=b'F' => Ok(b - b'A' + 10),
            _ => Err("embed token secret must be hex".into()),
        }
    };
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok((nibble(pair[0])? << 4) | nibble(pair[1])?))
        .collect()
}

impl EmbedSigner {
    /// Build from the hex secret (at least [`MIN_SECRET_BYTES`] decoded).
    pub fn from_hex(secret: &str) -> Result<Self, String> {
        let key = decode_hex(secret)?;
        if key.len() < MIN_SECRET_BYTES {
            return Err(format!(
                "embed token secret must decode to at least {MIN_SECRET_BYTES} bytes (got {})",
                key.len()
            ));
        }
        Ok(Self {
            key: zeroize::Zeroizing::new(key),
        })
    }

    fn mac(&self, signed: &[u8]) -> Hmac<Sha256> {
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&self.key)
            .expect("HMAC-SHA256 accepts keys of any length");
        mac.update(signed);
        mac
    }

    pub fn mint(&self, claims: &EmbedClaims) -> Result<String, ApiError> {
        let payload = serde_json::to_vec(claims)
            .map_err(|error| ApiError::Internal(format!("encode embed token: {error}")))?;
        let payload_b64 = URL_SAFE_NO_PAD.encode(payload);
        let signed = format!("{TOKEN_PREFIX}.{payload_b64}");
        let signature = self.mac(signed.as_bytes()).finalize().into_bytes();
        Ok(format!("{signed}.{}", URL_SAFE_NO_PAD.encode(signature)))
    }

    /// Verify signature (constant time), structure and lifetime. `None` for
    /// anything invalid — callers answer 401 without detail.
    #[must_use]
    pub fn verify(&self, token: &str, now: DateTime<Utc>) -> Option<EmbedPrincipal> {
        if token.len() > MAX_TOKEN_BYTES {
            return None;
        }
        let mut parts = token.split('.');
        let (Some(prefix), Some(payload_b64), Some(signature_b64), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return None;
        };
        if prefix != TOKEN_PREFIX {
            return None;
        }
        let signature = URL_SAFE_NO_PAD.decode(signature_b64).ok()?;
        let signed = &token[..prefix.len() + 1 + payload_b64.len()];
        // `verify_slice` compares in constant time.
        self.mac(signed.as_bytes()).verify_slice(&signature).ok()?;
        let payload = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
        let claims: EmbedClaims = serde_json::from_slice(&payload).ok()?;
        let now = now.timestamp();
        if claims.v != 1
            || claims.exp <= now
            || claims.iat > now + MAX_IAT_SKEW_SECS
            || claims.exp <= claims.iat
            || claims.exp - claims.iat > MAX_TTL_SECS
            || claims.scp.is_empty()
            || validate_sub_tenant(&claims.sub).is_err()
        {
            return None;
        }
        if let Some(seq) = &claims.seq
            && (seq.len() > MAX_SEQUENCES
                || seq
                    .iter()
                    .any(|name| name.is_empty() || name.len() > MAX_SEQUENCE_NAME_BYTES))
        {
            return None;
        }
        Some(EmbedPrincipal {
            tenant_id: TenantId::new(claims.tid).ok()?,
            sub_tenant: claims.sub,
            scopes: claims.scp,
            sequences: claims.seq,
            token_id: claims.jti,
            expires_at: DateTime::from_timestamp(claims.exp, 0)?,
        })
    }
}

/// The raw `o8e1` bearer credential of a request, if any.
#[must_use]
pub fn bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| value.starts_with("o8e1."))
}

/// Extractor: embed routes are 404 when embedding is disabled, 401 without
/// a valid `Authorization: Bearer o8e1…` token.
impl FromRequestParts<AppState> for EmbedPrincipal {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let Some(signer) = state.embedded.signer.as_ref() else {
            return Err(ApiError::NotFound("embed routes are disabled".into()));
        };
        let token = bearer_token(&parts.headers).ok_or(ApiError::Unauthorized)?;
        signer
            .verify(token, Utc::now())
            .ok_or(ApiError::Unauthorized)
    }
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct IssueTokenRequest {
    pub sub_tenant: String,
    pub scopes: Vec<EmbedScope>,
    /// Sequence-name allowlist; null = every sequence visible to the
    /// sub-tenant.
    #[serde(default)]
    pub sequences: Option<Vec<String>>,
    /// Lifetime (default 900, max 3600).
    #[serde(default)]
    pub ttl_seconds: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct IssueTokenResponse {
    pub token: String,
    pub expires_at: DateTime<Utc>,
}

#[utoipa::path(post, path = "/embed/tokens", tag = "embed", operation_id = "embed_issue_token",
    request_body = IssueTokenRequest,
    responses(
        (status = 201, description = "Scoped embed token", body = IssueTokenResponse),
        (status = 400, description = "Invalid sub-tenant, scopes, sequences or TTL"),
        (status = 404, description = "Embedding is disabled (no token secret)"),
    )
)]
pub(crate) async fn issue_token(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    principal: Option<Extension<crate::auth::PrincipalContext>>,
    Json(req): Json<IssueTokenRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let Some(signer) = state.embedded.signer.as_ref() else {
        return Err(ApiError::NotFound("embed routes are disabled".into()));
    };
    // Minting delegates tenant authority to an end customer: Operator only.
    if !crate::auth::principal_is_operator(principal.as_ref().map(|Extension(p)| p)) {
        return Err(ApiError::Forbidden(
            "issuing embed tokens requires the operator capability".into(),
        ));
    }
    let tenant = crate::sub_tenants::require_tenant(&tenant_ctx)?;
    validate_sub_tenant(&req.sub_tenant)
        .map_err(|e| ApiError::InvalidArgument(format!("sub_tenant: {e}")))?;
    if req.scopes.is_empty() {
        return Err(ApiError::InvalidArgument("scopes must not be empty".into()));
    }
    let mut scopes = req.scopes;
    scopes.sort_by_key(|s| s.as_str());
    scopes.dedup();
    if let Some(seq) = &req.sequences
        && (seq.len() > MAX_SEQUENCES
            || seq
                .iter()
                .any(|name| name.is_empty() || name.len() > MAX_SEQUENCE_NAME_BYTES))
    {
        return Err(ApiError::InvalidArgument(format!(
            "sequences must list at most {MAX_SEQUENCES} non-empty names"
        )));
    }
    let ttl = req.ttl_seconds.unwrap_or(DEFAULT_TTL_SECS);
    if !(1..=MAX_TTL_SECS).contains(&ttl) {
        return Err(ApiError::InvalidArgument(format!(
            "ttl_seconds must be 1-{MAX_TTL_SECS}"
        )));
    }
    let now = Utc::now();
    let claims = EmbedClaims {
        v: 1,
        tid: tenant.as_str().to_string(),
        sub: req.sub_tenant,
        scp: scopes,
        seq: req.sequences,
        iat: now.timestamp(),
        exp: now.timestamp() + ttl,
        jti: Uuid::new_v4(),
    };
    let token = signer.mint(&claims)?;
    let expires_at = DateTime::from_timestamp(claims.exp, 0)
        .ok_or_else(|| ApiError::Internal("token expiry out of range".into()))?;
    Ok((
        StatusCode::CREATED,
        Json(IssueTokenResponse { token, expires_at }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

    fn claims(now: i64) -> EmbedClaims {
        EmbedClaims {
            v: 1,
            tid: "tenant-a".into(),
            sub: "acme".into(),
            scp: vec![EmbedScope::RunsRead],
            seq: None,
            iat: now,
            exp: now + 600,
            jti: Uuid::new_v4(),
        }
    }

    #[test]
    fn secret_must_be_hex_and_long_enough() {
        assert!(EmbedSigner::from_hex(SECRET).is_ok());
        assert!(EmbedSigner::from_hex("abcd").is_err());
        assert!(EmbedSigner::from_hex(&"zz".repeat(32)).is_err());
        assert!(EmbedSigner::from_hex(&"a".repeat(63)).is_err());
    }

    #[test]
    fn round_trip_and_wire_format() {
        let signer = EmbedSigner::from_hex(SECRET).unwrap();
        let now = Utc::now();
        let token = signer.mint(&claims(now.timestamp())).unwrap();
        assert!(token.starts_with("o8e1."));
        assert_eq!(token.split('.').count(), 3);
        let principal = signer.verify(&token, now).unwrap();
        assert_eq!(principal.tenant_id.as_str(), "tenant-a");
        assert_eq!(principal.sub_tenant, "acme");
        // Independent HMAC over the documented signing input.
        let (signing_input, signature) = token.rsplit_once('.').unwrap();
        let mut mac =
            <Hmac<Sha256> as KeyInit>::new_from_slice(&decode_hex(SECRET).unwrap()).unwrap();
        mac.update(signing_input.as_bytes());
        assert_eq!(
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()),
            signature,
            "signature must be HMAC-SHA256(secret, \"o8e1.\" + b64payload)"
        );
    }

    #[test]
    fn rejects_forgery_expiry_and_overlong_ttl() {
        let signer = EmbedSigner::from_hex(SECRET).unwrap();
        let other = EmbedSigner::from_hex(&SECRET.replace('0', "1")).unwrap();
        let now = Utc::now();
        let t = now.timestamp();
        let token = signer.mint(&claims(t)).unwrap();
        assert!(other.verify(&token, now).is_none(), "wrong secret");

        // Payload swapped under the original signature.
        let mut forged = claims(t);
        forged.sub = "victim".into();
        let forged_payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&forged).unwrap());
        let parts: Vec<&str> = token.split('.').collect();
        let tampered = format!("o8e1.{forged_payload}.{}", parts[2]);
        assert!(signer.verify(&tampered, now).is_none(), "tampered payload");

        let mut expired = claims(t - 4000);
        expired.exp = t - 1;
        assert!(
            signer
                .verify(&signer.mint(&expired).unwrap(), now)
                .is_none()
        );

        let mut long = claims(t);
        long.exp = t + MAX_TTL_SECS + 1;
        assert!(signer.verify(&signer.mint(&long).unwrap(), now).is_none());

        let mut future = claims(t + 3_000);
        future.exp = t + 3_100;
        assert!(signer.verify(&signer.mint(&future).unwrap(), now).is_none());

        let mut bad_sub = claims(t);
        bad_sub.sub = "a/b".into();
        assert!(
            signer
                .verify(&signer.mint(&bad_sub).unwrap(), now)
                .is_none()
        );

        let mut no_scope = claims(t);
        no_scope.scp.clear();
        assert!(
            signer
                .verify(&signer.mint(&no_scope).unwrap(), now)
                .is_none()
        );

        for garbage in ["", "o8e1", "o8e1.x", "o8e1.x.y.z", "bst_x.y", "o8e1.!!.??"] {
            assert!(signer.verify(garbage, now).is_none(), "{garbage}");
        }
    }

    #[test]
    fn unknown_claims_and_scopes_are_rejected() {
        let signer = EmbedSigner::from_hex(SECRET).unwrap();
        let now = Utc::now();
        let mut value = serde_json::to_value(claims(now.timestamp())).unwrap();
        value["admin"] = serde_json::json!(true);
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&value).unwrap());
        let signing_input = format!("o8e1.{payload}");
        let mac =
            URL_SAFE_NO_PAD.encode(signer.mac(signing_input.as_bytes()).finalize().into_bytes());
        assert!(
            signer
                .verify(&format!("{signing_input}.{mac}"), now)
                .is_none()
        );

        let mut value = serde_json::to_value(claims(now.timestamp())).unwrap();
        value["scp"] = serde_json::json!(["runs:delete"]);
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&value).unwrap());
        let signing_input = format!("o8e1.{payload}");
        let mac =
            URL_SAFE_NO_PAD.encode(signer.mac(signing_input.as_bytes()).finalize().into_bytes());
        assert!(
            signer
                .verify(&format!("{signing_input}.{mac}"), now)
                .is_none()
        );
    }

    #[test]
    fn scope_and_sequence_checks() {
        let principal = EmbedPrincipal {
            tenant_id: TenantId::unchecked("t"),
            sub_tenant: "s".into(),
            scopes: vec![EmbedScope::RunsRead],
            sequences: Some(vec!["onboarding".into()]),
            token_id: Uuid::new_v4(),
            expires_at: Utc::now(),
        };
        assert!(principal.require(EmbedScope::RunsRead).is_ok());
        assert!(principal.require(EmbedScope::RunsStart).is_err());
        assert!(principal.allows_sequence("onboarding"));
        assert!(!principal.allows_sequence("billing"));
    }
}
