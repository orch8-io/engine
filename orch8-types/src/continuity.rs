//! Portable execution continuity protocol types.
//!
//! These types are the durable compatibility boundary shared by server,
//! storage, CLI, SDK, and mobile runtimes. They deliberately contain no I/O.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};

macro_rules! uuid_id {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
            sqlx::Type,
            ToSchema,
        )]
        #[sqlx(transparent)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            #[must_use]
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            #[must_use]
            pub const fn as_uuid(&self) -> &Uuid {
                &self.0
            }

            #[must_use]
            pub const fn into_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

uuid_id!(ContinuityId);
uuid_id!(RuntimeId);
uuid_id!(CapsuleId);
uuid_id!(HandoffId);
uuid_id!(EffectId);
uuid_id!(ContinuationGrantId);
uuid_id!(PlacementDecisionId);
uuid_id!(StreamId);

/// Monotonic ownership generation for one portable execution.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    ToSchema,
)]
#[serde(transparent)]
pub struct ExecutionEpoch(u64);

impl ExecutionEpoch {
    #[must_use]
    pub const fn initial() -> Self {
        Self(0)
    }

    #[must_use]
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> Result<Self, ContinuityError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(ContinuityError::EpochOverflow)
    }
}

/// Version of the stable capsule wire schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct CapsuleSchemaVersion {
    pub major: u16,
    pub minor: u16,
}

impl CapsuleSchemaVersion {
    pub const V1: Self = Self { major: 1, minor: 0 };

    #[must_use]
    pub const fn can_read(self, offered: Self) -> bool {
        self.major == offered.major && self.minor >= offered.minor
    }
}

/// Physical class of a runtime eligible to own or execute work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RuntimeKind {
    Server,
    Edge,
    Mobile,
    Desktop,
    Browser,
}

/// How strongly a runtime's identity and environment are verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeTrustLevel {
    Unverified,
    Registered,
    Signed,
    Attested,
}

/// Bounded facts used by compatibility and placement decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RuntimeCapabilities {
    pub runtime_id: RuntimeId,
    pub kind: RuntimeKind,
    pub trust: RuntimeTrustLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub handlers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plugins: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub regions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hardware: Vec<String>,
    #[serde(default)]
    pub offline_capable: bool,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// Requirements that must be satisfied before accepting a capsule.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CapsuleRequirements {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub handlers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plugins: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub credentials: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub regions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hardware: Vec<String>,
    #[serde(default)]
    pub requires_network: bool,
    #[serde(default)]
    pub requires_human_ui: bool,
    pub minimum_trust: Option<RuntimeTrustLevel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SequenceIdentity {
    pub id: SequenceId,
    pub version: i32,
    pub content_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CheckpointIdentity {
    pub block_id: BlockId,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ArtifactReference {
    pub key: String,
    pub sha256: String,
    pub bytes: u64,
}

/// Bounded, implementation-independent execution state stored in an encrypted
/// capsule artifact. Large binary values remain external artifact references.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CapsulePayload {
    pub instance: CapsuleInstanceState,
    pub checkpoint: crate::checkpoint::Checkpoint,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<crate::output::BlockOutput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_waits: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_signals: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effect_ids: Vec<EffectId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactReference>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stream_cursors: Vec<StreamCursor>,
    #[serde(default)]
    pub redacted_audit_context: serde_json::Value,
}

/// Explicit portable subset of a runtime-local task instance. This avoids
/// turning the engine's SQL/Rust representation into the capsule contract.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CapsuleInstanceState {
    pub sequence_id: SequenceId,
    pub namespace: Namespace,
    pub priority: crate::instance::Priority,
    pub timezone: String,
    pub metadata: serde_json::Value,
    pub context: crate::context::ExecutionContext,
    pub budget: Option<crate::instance::Budget>,
    pub parent_instance_id: Option<InstanceId>,
}

impl CapsulePayload {
    pub const MAX_OUTPUTS: usize = 10_000;
    pub const MAX_PENDING_ITEMS: usize = 10_000;
    pub const MAX_ARTIFACTS: usize = 10_000;
    pub const MAX_STREAM_CURSORS: usize = 1_000;
    pub const MAX_ENCODED_BYTES: usize = 64 * 1024 * 1024;

    pub fn validate_bounds(&self) -> Result<(), ContinuityError> {
        if self.outputs.len() > Self::MAX_OUTPUTS
            || self.pending_waits.len() > Self::MAX_PENDING_ITEMS
            || self.pending_signals.len() > Self::MAX_PENDING_ITEMS
            || self.effect_ids.len() > Self::MAX_PENDING_ITEMS
            || self.artifacts.len() > Self::MAX_ARTIFACTS
            || self.stream_cursors.len() > Self::MAX_STREAM_CURSORS
        {
            return Err(ContinuityError::CapsulePayloadTooLarge);
        }
        Ok(())
    }
}

/// Signed metadata envelope. The encrypted payload is always an artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CapsuleManifest {
    pub schema: CapsuleSchemaVersion,
    pub capsule_id: CapsuleId,
    pub continuity_id: ContinuityId,
    pub source_instance_id: InstanceId,
    pub epoch: ExecutionEpoch,
    pub tenant_id: TenantId,
    pub source_runtime_id: RuntimeId,
    pub allowed_destination_runtime_id: Option<RuntimeId>,
    pub sequence: SequenceIdentity,
    pub checkpoint: CheckpointIdentity,
    pub requirements_sha256: String,
    pub payload_artifact: ArtifactReference,
    pub provenance_head: Option<String>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub signing_key_id: String,
    pub encryption_key_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum OwnershipState {
    Owned,
    Transferring,
    Completed,
}

/// Authoritative owner and epoch for one portable execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ContinuityExecution {
    pub continuity_id: ContinuityId,
    pub tenant_id: TenantId,
    pub current_instance_id: InstanceId,
    pub owner_runtime_id: RuntimeId,
    pub epoch: ExecutionEpoch,
    pub state: OwnershipState,
    pub updated_at: DateTime<Utc>,
}

/// Persisted transfer operation. `version` is the compare-and-swap token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionHandoff {
    pub id: HandoffId,
    pub continuity_id: ContinuityId,
    pub tenant_id: TenantId,
    pub source_runtime_id: RuntimeId,
    pub destination_runtime_id: RuntimeId,
    pub expected_epoch: ExecutionEpoch,
    pub state: HandoffState,
    pub capsule_id: Option<CapsuleId>,
    pub preview_sha256: String,
    pub version: u64,
    pub failure_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum GrantAction {
    Inspect,
    Accept,
    Resume,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationGrantState {
    Active,
    Consumed,
    Revoked,
}

/// One-time, destination-bound authorization for an offline ownership claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ContinuationGrant {
    pub id: ContinuationGrantId,
    pub tenant_id: TenantId,
    pub continuity_id: ContinuityId,
    pub expected_epoch: ExecutionEpoch,
    pub destination_runtime_id: RuntimeId,
    pub subject: Option<String>,
    pub allowed_actions: Vec<GrantAction>,
    pub nonce_sha256: String,
    pub state: ContinuationGrantState,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub consumed_at: Option<DateTime<Utc>>,
    pub signing_key_id: String,
}

impl ContinuationGrant {
    pub fn validate_claim(
        &self,
        now: DateTime<Utc>,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        expected_epoch: ExecutionEpoch,
        destination_runtime_id: RuntimeId,
        action: GrantAction,
    ) -> Result<(), ContinuityError> {
        if self.state != ContinuationGrantState::Active {
            return Err(ContinuityError::GrantUnavailable);
        }
        if self.expires_at <= now {
            return Err(ContinuityError::GrantExpired);
        }
        if &self.tenant_id != tenant_id
            || self.continuity_id != continuity_id
            || self.expected_epoch != expected_epoch
        {
            return Err(ContinuityError::GrantScopeMismatch);
        }
        if self.destination_runtime_id != destination_runtime_id {
            return Err(ContinuityError::WrongDestination);
        }
        if !self.allowed_actions.contains(&action) {
            return Err(ContinuityError::GrantActionDenied);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DataClassification {
    Public,
    Internal,
    Confidential,
    Restricted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LocalityRule {
    pub classification: DataClassification,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_regions: Vec<String>,
    pub minimum_trust: Option<RuntimeTrustLevel>,
    pub require_offline: Option<bool>,
    pub require_hardware: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LocalityPolicy {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<LocalityRule>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PolicyOutcome {
    Allow,
    Deny,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PlacementEvidence {
    pub runtime_id: RuntimeId,
    pub outcome: PolicyOutcome,
    pub score: i64,
    pub finding_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PlacementDecision {
    pub id: PlacementDecisionId,
    pub tenant_id: TenantId,
    pub continuity_id: ContinuityId,
    pub epoch: ExecutionEpoch,
    pub selected_runtime_id: Option<RuntimeId>,
    pub policy_version: Option<u32>,
    pub candidates: Vec<PlacementEvidence>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamFrameState {
    Committed,
    Retracted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ContinuityStream {
    pub stream_id: StreamId,
    pub tenant_id: TenantId,
    pub continuity_id: ContinuityId,
    pub epoch: ExecutionEpoch,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StreamFrame {
    pub stream_id: StreamId,
    pub tenant_id: TenantId,
    pub continuity_id: ContinuityId,
    pub epoch: ExecutionEpoch,
    pub sequence: u64,
    pub checkpoint_sha256: String,
    pub state: StreamFrameState,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StreamCursor {
    pub stream_id: StreamId,
    pub last_committed_sequence: u64,
}

impl CapsuleManifest {
    pub fn validate_for_import(
        &self,
        now: DateTime<Utc>,
        tenant_id: &TenantId,
        destination: RuntimeId,
        reader: CapsuleSchemaVersion,
    ) -> Result<(), ContinuityError> {
        if !reader.can_read(self.schema) {
            return Err(ContinuityError::UnsupportedSchema {
                offered_major: self.schema.major,
                offered_minor: self.schema.minor,
            });
        }
        if &self.tenant_id != tenant_id {
            return Err(ContinuityError::TenantMismatch);
        }
        if self.expires_at <= now {
            return Err(ContinuityError::CapsuleExpired);
        }
        if self
            .allowed_destination_runtime_id
            .is_some_and(|allowed| allowed != destination)
        {
            return Err(ContinuityError::WrongDestination);
        }
        Ok(())
    }
}

/// Durable ownership-transfer lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HandoffState {
    Requested,
    Quiescing,
    Exported,
    Accepted,
    Resumed,
    Completed,
    Rejected,
    Failed,
    Expired,
    Revoked,
}

impl HandoffState {
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Requested,
                Self::Quiescing | Self::Rejected | Self::Failed
            ) | (Self::Quiescing, Self::Exported | Self::Failed)
                | (
                    Self::Exported,
                    Self::Accepted | Self::Expired | Self::Revoked | Self::Failed
                )
                | (Self::Accepted, Self::Resumed | Self::Failed)
                | (Self::Resumed, Self::Completed | Self::Failed)
        )
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Rejected | Self::Failed | Self::Expired | Self::Revoked
        )
    }
}

/// Classification of a potentially externally visible operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EffectKind {
    Http,
    Worker,
    Webhook,
    Message,
    Storage,
    Model,
    HumanDecision,
    Custom,
}

/// Evidence state for an external effect; this never claims exactly-once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EffectState {
    Planned,
    Prepared,
    Dispatched,
    Committed,
    Unknown,
    Verified,
    Compensated,
    Abandoned,
}

impl EffectState {
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Planned, Self::Prepared | Self::Abandoned)
                | (Self::Prepared, Self::Dispatched | Self::Abandoned)
                | (Self::Dispatched, Self::Committed | Self::Unknown)
                | (
                    Self::Unknown,
                    Self::Verified | Self::Committed | Self::Compensated | Self::Abandoned
                )
                | (Self::Committed | Self::Verified, Self::Compensated)
        )
    }

    #[must_use]
    pub const fn is_resolved(self) -> bool {
        matches!(
            self,
            Self::Committed | Self::Verified | Self::Compensated | Self::Abandoned
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EffectReceipt {
    pub id: EffectId,
    pub tenant_id: TenantId,
    pub continuity_id: ContinuityId,
    pub epoch: ExecutionEpoch,
    pub instance_id: InstanceId,
    pub block_id: BlockId,
    pub kind: EffectKind,
    pub state: EffectState,
    pub destination_fingerprint: String,
    pub idempotency_key: Option<String>,
    pub request_sha256: String,
    pub provider_receipt_id: Option<String>,
    pub attempt: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// One immutable link in the execution provenance hash chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ProvenanceEntry {
    pub id: Uuid,
    pub continuity_id: ContinuityId,
    pub tenant_id: TenantId,
    pub epoch: ExecutionEpoch,
    pub kind: String,
    pub payload_sha256: String,
    pub previous_sha256: Option<String>,
    pub entry_sha256: String,
    pub signing_key_id: Option<String>,
    pub signature: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl EffectReceipt {
    pub fn transition(
        &mut self,
        next: EffectState,
        now: DateTime<Utc>,
    ) -> Result<(), ContinuityError> {
        if !self.state.can_transition_to(next) {
            return Err(ContinuityError::InvalidEffectTransition {
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        self.updated_at = now;
        Ok(())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ContinuityError {
    #[error("execution epoch overflow")]
    EpochOverflow,
    #[error("unsupported capsule schema {offered_major}.{offered_minor}")]
    UnsupportedSchema {
        offered_major: u16,
        offered_minor: u16,
    },
    #[error("capsule tenant does not match the authenticated tenant")]
    TenantMismatch,
    #[error("capsule has expired")]
    CapsuleExpired,
    #[error("capsule is bound to another destination runtime")]
    WrongDestination,
    #[error("capsule payload exceeds protocol bounds")]
    CapsulePayloadTooLarge,
    #[error("continuation grant is consumed or revoked")]
    GrantUnavailable,
    #[error("continuation grant has expired")]
    GrantExpired,
    #[error("continuation grant scope does not match this claim")]
    GrantScopeMismatch,
    #[error("continuation grant does not authorize this action")]
    GrantActionDenied,
    #[error("invalid effect transition from {from:?} to {to:?}")]
    InvalidEffectTransition { from: EffectState, to: EffectState },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_is_monotonic_and_checked() {
        assert_eq!(ExecutionEpoch::initial().checked_next().unwrap().get(), 1);
        assert_eq!(
            ExecutionEpoch(u64::MAX).checked_next(),
            Err(ContinuityError::EpochOverflow)
        );
    }

    #[test]
    fn handoff_transition_table_rejects_split_brain_shortcuts() {
        assert!(HandoffState::Requested.can_transition_to(HandoffState::Quiescing));
        assert!(HandoffState::Exported.can_transition_to(HandoffState::Accepted));
        assert!(!HandoffState::Requested.can_transition_to(HandoffState::Resumed));
        assert!(!HandoffState::Completed.can_transition_to(HandoffState::Resumed));
    }

    #[test]
    fn unknown_effect_requires_explicit_resolution() {
        assert!(EffectState::Dispatched.can_transition_to(EffectState::Unknown));
        assert!(!EffectState::Unknown.is_resolved());
        assert!(EffectState::Unknown.can_transition_to(EffectState::Verified));
        assert!(!EffectState::Unknown.can_transition_to(EffectState::Dispatched));
    }

    #[test]
    fn capsule_import_fails_closed() {
        let now = Utc::now();
        let tenant = TenantId::new("tenant-a").unwrap();
        let destination = RuntimeId::new();
        let manifest = CapsuleManifest {
            schema: CapsuleSchemaVersion::V1,
            capsule_id: CapsuleId::new(),
            continuity_id: ContinuityId::new(),
            source_instance_id: InstanceId::new(),
            epoch: ExecutionEpoch::initial(),
            tenant_id: tenant.clone(),
            source_runtime_id: RuntimeId::new(),
            allowed_destination_runtime_id: Some(destination),
            sequence: SequenceIdentity {
                id: SequenceId::new(),
                version: 1,
                content_sha256: "a".repeat(64),
            },
            checkpoint: CheckpointIdentity {
                block_id: BlockId::new("safe"),
                sha256: "b".repeat(64),
            },
            requirements_sha256: "c".repeat(64),
            payload_artifact: ArtifactReference {
                key: "capsules/payload".into(),
                sha256: "d".repeat(64),
                bytes: 128,
            },
            provenance_head: None,
            issued_at: now,
            expires_at: now + chrono::Duration::minutes(5),
            signing_key_id: "signing-v1".into(),
            encryption_key_id: "encryption-v1".into(),
        };

        assert_eq!(
            manifest.validate_for_import(
                now,
                &TenantId::new("tenant-b").unwrap(),
                destination,
                CapsuleSchemaVersion::V1,
            ),
            Err(ContinuityError::TenantMismatch)
        );
        assert_eq!(
            manifest.validate_for_import(now, &tenant, RuntimeId::new(), CapsuleSchemaVersion::V1,),
            Err(ContinuityError::WrongDestination)
        );
    }

    #[test]
    fn protocol_types_round_trip_without_shape_drift() {
        let value = serde_json::to_value(CapsuleRequirements {
            handlers: vec!["http_request".into()],
            minimum_trust: Some(RuntimeTrustLevel::Signed),
            ..CapsuleRequirements::default()
        })
        .unwrap();
        let decoded: CapsuleRequirements = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(decoded.handlers, ["http_request"]);
        assert_eq!(serde_json::to_value(decoded).unwrap(), value);
    }
}
