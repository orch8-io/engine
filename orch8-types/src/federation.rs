//! Federation transport: peer trust registry, outbound call records, the
//! signed wire messages exchanged between two engines, and the region fence
//! used for active-passive failover.
//!
//! The cryptographic envelope itself is the pre-existing
//! [`crate::continuity_advanced::FederationEnvelope`]:
//! ed25519-signed, short-lived (≤ 300 s), bound to a payload SHA-256 and to
//! the receiving tenant. This module only adds what is needed to carry those
//! envelopes over an explicit, opt-in HTTPS transport.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::continuity::ContinuityId;
use crate::continuity_advanced::{FederationEnvelope, FederationPeer, FederationPeerId};
use crate::ids::{BlockId, InstanceId, TenantId};

/// Wire protocol version carried in every request/response payload.
pub const FEDERATION_PROTOCOL_VERSION: u32 = 1;
/// Canonical inbound path on a peer's gateway, relative to its endpoint.
pub const FEDERATION_INBOUND_PATH: &str = "/api/v1/federation/inbound";
/// Wildcard accepted in `disclosed_fields` / `returned_outputs` for
/// same-organization cluster peers only.
pub const DISCLOSE_ALL: &str = "*";
/// Hard cap on list sizes in a peer policy.
pub const MAX_POLICY_ENTRIES: usize = 256;

/// Trust relationship with a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum PeerRelationship {
    /// A different organization. Wildcard disclosure is rejected and the
    /// caller's instance identity is never sent.
    #[default]
    Organization,
    /// Another cluster of the same organization (cross-cluster child
    /// workflows). May use `"*"` disclosure and links the child to its parent.
    Cluster,
}

/// What this engine may send to the peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
pub struct PeerOutboundPolicy {
    /// Remote sequence names this tenant may start at the peer.
    #[serde(default)]
    pub sequences: Vec<String>,
    /// Top-level input fields allowed to leave. Everything else is withheld
    /// (only its SHA-256 is kept locally as evidence).
    #[serde(default)]
    pub disclosed_fields: Vec<String>,
}

/// What the peer may do here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
pub struct PeerInboundPolicy {
    /// Local sequence names the peer may start in this tenant.
    #[serde(default)]
    pub sequences: Vec<String>,
    /// When set, an inbound start is refused unless every step handler of
    /// the target sequence is in this list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handlers: Option<Vec<String>>,
    /// Block ids whose outputs are returned to the peer on completion. No
    /// other instance data (context, errors, logs) ever leaves.
    #[serde(default)]
    pub returned_outputs: Vec<String>,
}

/// One entry of the per-tenant federation trust registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct FederationPeerRecord {
    /// Local tenant that owns this trust relationship.
    pub tenant_id: TenantId,
    /// The peer's federation identity (derived from its public key; see
    /// [`peer_id_from_public_key`]).
    pub peer_id: FederationPeerId,
    pub name: String,
    #[serde(default)]
    pub relationship: PeerRelationship,
    /// HTTPS base URL of the peer's gateway (no trailing path).
    pub endpoint: String,
    /// Base64 raw 32-byte ed25519 verifying key of the peer.
    pub public_key: String,
    /// Tenant at the peer that this relationship is bound to: our calls
    /// target it, and its calls must come from it.
    pub remote_tenant_id: TenantId,
    #[serde(default)]
    pub outbound: PeerOutboundPolicy,
    #[serde(default)]
    pub inbound: PeerInboundPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl FederationPeerRecord {
    /// Whether the relationship is usable at `now` (not revoked/expired).
    #[must_use]
    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        self.revoked_at.is_none_or(|revoked| revoked > now)
            && self.expires_at.is_none_or(|expires| expires > now)
    }

    /// Build the verification trust root for envelopes *sent by this peer to
    /// us* (their envelope names our local tenant).
    #[must_use]
    pub fn inbound_trust_root(&self) -> FederationPeer {
        FederationPeer {
            id: self.peer_id,
            name: self.name.clone(),
            trust_root_sha256: public_key_fingerprint(&self.public_key).unwrap_or_default(),
            public_key: self.public_key.clone(),
            endpoint: self.endpoint.clone(),
            allowed_tenants: vec![self.tenant_id.clone()],
            revoked_at: self.revoked_at,
        }
    }

    /// `true` when the outbound policy allows `sequence`.
    #[must_use]
    pub fn outbound_allows_sequence(&self, sequence: &str) -> bool {
        self.outbound.sequences.iter().any(|s| s == sequence)
    }

    /// `true` when the inbound policy allows `sequence`.
    #[must_use]
    pub fn inbound_allows_sequence(&self, sequence: &str) -> bool {
        self.inbound.sequences.iter().any(|s| s == sequence)
    }

    /// Validate structural invariants. Returns a human-readable reason.
    ///
    /// # Errors
    /// Describes the first violated invariant.
    pub fn validate(&self, allow_http: bool) -> Result<(), String> {
        if self.name.trim().is_empty() || self.name.len() > 128 {
            return Err("name must be 1-128 characters".into());
        }
        let endpoint_ok = self.endpoint.len() <= 2048
            && (self.endpoint.starts_with("https://")
                || (allow_http && self.endpoint.starts_with("http://")))
            && !self.endpoint.contains(['?', '#', ' ']);
        if !endpoint_ok {
            return Err("endpoint must be an https:// base URL without query or fragment".into());
        }
        public_key_fingerprint(&self.public_key)
            .ok_or("public_key must be base64 of a 32-byte ed25519 key")?;
        if peer_id_from_public_key(&self.public_key) != Some(self.peer_id) {
            return Err(
                "peer_id does not match the public key (see GET /federation/identity)".into(),
            );
        }
        for list in [
            &self.outbound.sequences,
            &self.outbound.disclosed_fields,
            &self.inbound.sequences,
            &self.inbound.returned_outputs,
        ] {
            if list.len() > MAX_POLICY_ENTRIES || list.iter().any(|v| v.is_empty() || v.len() > 256)
            {
                return Err(format!(
                    "policy lists hold at most {MAX_POLICY_ENTRIES} non-empty entries"
                ));
            }
        }
        if let Some(handlers) = &self.inbound.handlers
            && handlers.len() > MAX_POLICY_ENTRIES
        {
            return Err("inbound.handlers is too long".into());
        }
        if self.relationship == PeerRelationship::Organization
            && (self
                .outbound
                .disclosed_fields
                .iter()
                .any(|f| f == DISCLOSE_ALL)
                || self
                    .inbound
                    .returned_outputs
                    .iter()
                    .any(|f| f == DISCLOSE_ALL))
        {
            return Err(
                "\"*\" disclosure is only allowed for relationship = \"cluster\" (same organization)"
                    .into(),
            );
        }
        Ok(())
    }
}

/// Lifecycle of an outbound federated call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FederationCallState {
    /// Registered locally, not yet acknowledged by the peer.
    Pending,
    /// The peer accepted it and created a remote instance.
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl FederationCallState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Durable record of one outbound call. `version` is the CAS token.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FederationCall {
    pub call_id: Uuid,
    pub tenant_id: TenantId,
    pub instance_id: InstanceId,
    pub block_id: BlockId,
    pub peer_id: FederationPeerId,
    pub sequence: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_version: Option<i32>,
    /// Minimized input: only the peer's `disclosed_fields`.
    pub input: serde_json::Value,
    /// SHA-256 of each withheld top-level field (evidence, never the value).
    #[serde(default)]
    pub withheld_sha256: Vec<String>,
    pub state: FederationCallState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_instance_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Consecutive transport failures (drives backoff).
    #[serde(default)]
    pub attempts: u32,
    /// Local instance was told about the terminal state (resume signal sent).
    #[serde(default)]
    pub notified: bool,
    pub next_poll_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: u64,
}

/// Kind of request carried in a signed envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FederationRequestKind {
    /// Start (idempotently, keyed by `call_id`) and return current status.
    Start,
    Status,
    Cancel,
}

/// Parent linkage disclosed only for same-organization cluster peers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct FederationParentRef {
    pub instance_id: InstanceId,
    pub block_id: BlockId,
}

/// Signed request payload (the bytes the envelope's `payload_sha256` covers).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FederationRequest {
    pub v: u32,
    pub kind: FederationRequestKind,
    pub call_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_version: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<FederationParentRef>,
}

/// Remote-side state reported back to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RemoteCallState {
    /// No instance exists for this call id at the peer.
    Unknown,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Signed response payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FederationResponse {
    pub v: u32,
    pub call_id: Uuid,
    pub state: RemoteCallState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_instance_id: Option<Uuid>,
    /// `{ "<block_id>": <output> }` restricted to the peer's
    /// `inbound.returned_outputs`. Present only when `state = completed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outputs: Option<serde_json::Value>,
}

/// HTTP body in both directions: an envelope plus the exact payload bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SignedFederationMessage {
    pub envelope: FederationEnvelope,
    pub payload_base64: String,
}

/// Public identity other engines register us under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct FederationIdentity {
    pub peer_id: FederationPeerId,
    pub public_key: String,
    pub trust_root_sha256: String,
}

fn hex_sha256(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest.as_slice() {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn uuid_from_digest(domain: &[u8], parts: &[&[u8]]) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_slice()[..16]);
    // RFC 9562 version 8 (custom) + variant bits.
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// SHA-256 hex fingerprint of a base64 raw 32-byte public key.
#[must_use]
pub fn public_key_fingerprint(public_key_b64: &str) -> Option<String> {
    let bytes = BASE64.decode(public_key_b64).ok()?;
    (bytes.len() == 32).then(|| hex_sha256(&bytes))
}

/// Federation identity of an engine: a UUID derived from its public key, so
/// the registry cannot bind one identity to two keys.
#[must_use]
pub fn peer_id_from_public_key(public_key_b64: &str) -> Option<FederationPeerId> {
    let bytes = BASE64.decode(public_key_b64).ok()?;
    (bytes.len() == 32).then(|| {
        FederationPeerId::from_uuid(uuid_from_digest(b"orch8-federation-peer-v1\0", &[&bytes]))
    })
}

/// Deterministic call id: step retries and crash recovery reuse it, which is
/// what makes a remote start idempotent end to end.
#[must_use]
pub fn derive_call_id(tenant_id: &TenantId, instance_id: InstanceId, block_id: &BlockId) -> Uuid {
    uuid_from_digest(
        b"orch8-federation-call-v1\0",
        &[
            tenant_id.as_str().as_bytes(),
            instance_id.into_uuid().as_bytes(),
            block_id.as_str().as_bytes(),
        ],
    )
}

/// Continuity scope carried in every envelope of a call. The receiver binds
/// the remote instance to exactly this continuity id, so envelope receipts
/// (replay evidence) attach to a real execution on both backends.
#[must_use]
pub fn call_continuity_id(call_id: Uuid) -> ContinuityId {
    ContinuityId::from_uuid(uuid_from_digest(
        b"orch8-federation-continuity-v1\0",
        &[call_id.as_bytes()],
    ))
}

/// Idempotency key of the remote instance created for `(peer, call)`.
#[must_use]
pub fn inbound_idempotency_key(peer_id: FederationPeerId, call_id: Uuid) -> String {
    format!("federation:{peer_id}:{call_id}")
}

/// Singleton active-region fence for active-passive failover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RegionFence {
    pub active_region: String,
    /// Monotonic; every promotion increments it by exactly one.
    pub epoch: u64,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub updated_by: String,
    #[serde(default)]
    pub reason: String,
}

/// Valid region names: 1-64 of `[a-z0-9-]`.
#[must_use]
pub fn is_valid_region(region: &str) -> bool {
    !region.is_empty()
        && region.len() <= 64
        && region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";

    fn record() -> FederationPeerRecord {
        let now = Utc::now();
        FederationPeerRecord {
            tenant_id: TenantId::new("acme").unwrap(),
            peer_id: peer_id_from_public_key(KEY).unwrap(),
            name: "globex".into(),
            relationship: PeerRelationship::Organization,
            endpoint: "https://gw.globex.example".into(),
            public_key: KEY.into(),
            remote_tenant_id: TenantId::new("globex").unwrap(),
            outbound: PeerOutboundPolicy {
                sequences: vec!["kyc".into()],
                disclosed_fields: vec!["customer_id".into()],
            },
            inbound: PeerInboundPolicy::default(),
            expires_at: None,
            revoked_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn identity_is_bound_to_the_key() {
        let rec = record();
        rec.validate(false).unwrap();
        let mut other = rec.clone();
        other.peer_id = FederationPeerId::new();
        assert!(other.validate(false).is_err());
    }

    #[test]
    fn organization_peers_cannot_disclose_everything() {
        let mut rec = record();
        rec.outbound.disclosed_fields = vec!["*".into()];
        assert!(rec.validate(false).is_err());
        rec.relationship = PeerRelationship::Cluster;
        rec.validate(false).unwrap();
    }

    #[test]
    fn plain_http_requires_explicit_opt_in() {
        let mut rec = record();
        rec.endpoint = "http://127.0.0.1:9000".into();
        assert!(rec.validate(false).is_err());
        rec.validate(true).unwrap();
    }

    #[test]
    fn activity_honours_expiry_and_revocation() {
        let now = Utc::now();
        let mut rec = record();
        assert!(rec.is_active(now));
        rec.expires_at = Some(now - chrono::Duration::seconds(1));
        assert!(!rec.is_active(now));
        rec.expires_at = None;
        rec.revoked_at = Some(now);
        assert!(!rec.is_active(now));
    }

    #[test]
    fn call_ids_are_deterministic_and_scoped() {
        let tenant = TenantId::new("acme").unwrap();
        let instance = InstanceId::new();
        let a = derive_call_id(&tenant, instance, &BlockId::new("call"));
        assert_eq!(a, derive_call_id(&tenant, instance, &BlockId::new("call")));
        assert_ne!(a, derive_call_id(&tenant, instance, &BlockId::new("other")));
        assert_ne!(call_continuity_id(a).into_uuid(), a);
    }

    #[test]
    fn region_names_are_restricted() {
        assert!(is_valid_region("eu-west-1"));
        assert!(!is_valid_region("EU"));
        assert!(!is_valid_region(""));
    }
}
