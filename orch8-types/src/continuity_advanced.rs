//! Evidence, simulation, policy, and federation types built on continuity.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::continuity::{
    ContinuityId, DataClassification, EffectKind, ExecutionEpoch, RuntimeId, RuntimeTrustLevel,
};
use crate::ids::{BlockId, InstanceId, SequenceId, TenantId};
use crate::instance::BudgetUsage;

macro_rules! uuid_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
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
            pub const fn into_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

uuid_id!(InvariantId);
uuid_id!(InvariantResultId);
uuid_id!(MigrationPlanId);
uuid_id!(ScenarioId);
uuid_id!(IncidentCaseId);
uuid_id!(BudgetReservationId);
uuid_id!(ProviderDecisionId);
uuid_id!(EvaluationId);
uuid_id!(RecommendationId);
uuid_id!(AttentionTaskId);
uuid_id!(DelegationId);
uuid_id!(FederationPeerId);
uuid_id!(FederationMessageId);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CheckpointBoundary {
    #[serde(default = "Uuid::nil")]
    pub checkpoint_id: Uuid,
    pub instance_id: InstanceId,
    pub continuity_id: ContinuityId,
    pub epoch: ExecutionEpoch,
    pub sequence_id: SequenceId,
    pub sequence_version: i32,
    pub block_id: BlockId,
    pub checkpoint_sha256: String,
    pub provenance_head: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ForkEffectMode {
    Copied,
    Mocked,
    Reexecuted,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WhatIfScenario {
    pub id: ScenarioId,
    pub tenant_id: TenantId,
    pub source: CheckpointBoundary,
    #[serde(default)]
    pub context_patch: serde_json::Value,
    #[serde(default)]
    pub output_overrides: serde_json::Value,
    #[serde(default)]
    pub handler_mocks: serde_json::Value,
    pub target_sequence_version: Option<i32>,
    pub effect_mode: ForkEffectMode,
    pub virtual_time: bool,
    pub retain_full_evidence: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExtractedTestFixture {
    pub source: CheckpointBoundary,
    pub stable_id: String,
    pub sanitized_context: serde_json::Value,
    pub receipt_mocks: Vec<String>,
    pub effect_mocks: Vec<ExtractedEffectMock>,
    pub sequence: crate::sequence::SequenceDefinition,
    pub contract: crate::contract::ContractSuite,
    pub missing_evidence: Vec<String>,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExtractedEffectMock {
    pub block_id: BlockId,
    pub kind: EffectKind,
    pub state: crate::continuity::EffectState,
    pub request_sha256: String,
    pub destination_fingerprint: String,
    pub provider_receipt_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InvariantRule {
    EffectAtMostOnce { kind: EffectKind },
    NoUnknownEffects,
    TerminalStateIn { states: Vec<String> },
    BudgetWithinLimits,
    OutputPathPresent { block_id: BlockId, path: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowInvariant {
    pub id: InvariantId,
    pub tenant_id: TenantId,
    pub sequence_id: SequenceId,
    pub sequence_version: Option<i32>,
    pub name: String,
    pub rule: InvariantRule,
    pub commit_guard: bool,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceStatus {
    Pass,
    Fail,
    Unknown,
    Inconclusive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InvariantResult {
    pub id: InvariantResultId,
    pub invariant_id: InvariantId,
    pub continuity_id: ContinuityId,
    pub epoch: ExecutionEpoch,
    pub status: EvidenceStatus,
    pub dedupe_key: String,
    pub evidence_sha256: Vec<String>,
    pub summary: String,
    pub evaluated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MigrationDisposition {
    Pin,
    Automatic,
    ApprovalRequired,
    Incompatible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StateTransform {
    pub from_path: String,
    pub to_path: String,
    pub transform: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LiveMigrationPlan {
    pub id: MigrationPlanId,
    pub tenant_id: TenantId,
    pub continuity_id: ContinuityId,
    pub from_sequence_id: SequenceId,
    pub from_version: i32,
    pub to_sequence_id: SequenceId,
    pub to_version: i32,
    pub disposition: MigrationDisposition,
    pub transforms: Vec<StateTransform>,
    pub finding_codes: Vec<String>,
    pub rollback_capsule_required: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FaultPoint {
    StorageBeforeWrite,
    StorageAfterWrite,
    Dispatch,
    EffectReceipt,
    OwnershipClaim,
    DeviceSync,
    StreamAppend,
    ExternalCall,
    Approval,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FaultKind {
    WorkerDeath,
    DatabaseTimeout,
    DuplicateDelivery,
    StaleOwner,
    OfflineDevice,
    CorruptCapsule,
    ExpiredGrant,
    ProviderOutage,
    DelayedApproval,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct FaultInjection {
    pub point: FaultPoint,
    pub kind: FaultKind,
    pub occurrence: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GeneratedScenario {
    pub id: ScenarioId,
    pub event_order: Vec<String>,
    pub faults: Vec<FaultInjection>,
    pub max_steps: u32,
    pub seed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct IncidentReproduction {
    pub id: IncidentCaseId,
    pub stable_failure_code: String,
    pub scenario: Option<GeneratedScenario>,
    pub status: EvidenceStatus,
    pub missing_evidence: Vec<String>,
    pub attempts: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReservationState {
    Reserved,
    Reconciled,
    Released,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BudgetReservation {
    pub id: BudgetReservationId,
    pub tenant_id: TenantId,
    pub continuity_id: ContinuityId,
    pub epoch: ExecutionEpoch,
    pub requested: BudgetUsage,
    pub actual: Option<BudgetUsage>,
    pub estimation_version: String,
    pub state: ReservationState,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ProviderCandidate {
    pub provider: String,
    pub model: String,
    pub region: String,
    pub price_microunits: i64,
    pub expected_latency_ms: u64,
    pub quality_millipoints: i64,
    pub breaker_open: bool,
    pub supports_idempotency: bool,
    pub pricing_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ProviderDecision {
    pub id: ProviderDecisionId,
    pub selected: Option<ProviderCandidate>,
    pub finding_codes: Vec<String>,
    pub cohort: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EvaluationScore {
    pub id: EvaluationId,
    pub tenant_id: TenantId,
    pub continuity_id: ContinuityId,
    pub evaluator: String,
    pub score_millipoints: i64,
    pub sample_size: u64,
    pub deferred: bool,
    pub dedupe_key: String,
    pub evidence_sha256: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct OptimizationRecommendation {
    pub id: RecommendationId,
    pub kind: String,
    pub summary: String,
    pub evidence: Vec<String>,
    pub estimated_impact_millipoints: i64,
    pub confidence_millipoints: u16,
    pub risks: Vec<String>,
    pub what_if: WhatIfScenario,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AttentionState {
    Pending,
    Assigned,
    Decided,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AttentionTask {
    pub id: AttentionTaskId,
    pub tenant_id: TenantId,
    pub continuity_id: ContinuityId,
    pub required_skills: Vec<String>,
    pub classification: DataClassification,
    pub allowed_regions: Vec<String>,
    pub priority: u8,
    pub deadline: DateTime<Utc>,
    pub estimated_attention_units: i64,
    pub state: AttentionState,
    pub assignee: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ReviewerCapabilities {
    pub reviewer_id: String,
    pub tenant_ids: Vec<TenantId>,
    pub skills: Vec<String>,
    pub region: String,
    pub trust: RuntimeTrustLevel,
    pub available_attention_units: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ResidencyEvidence {
    pub classification: DataClassification,
    pub operation: String,
    pub source_region: Option<String>,
    pub destination_region: Option<String>,
    pub outcome: EvidenceStatus,
    pub finding_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DisclosureResult {
    pub disclosed: serde_json::Value,
    pub withheld_sha256: Vec<String>,
    pub classification: DataClassification,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DeviceDelegation {
    pub id: DelegationId,
    pub tenant_id: TenantId,
    pub parent_continuity_id: ContinuityId,
    pub parent_epoch: ExecutionEpoch,
    pub source_runtime_id: RuntimeId,
    pub destination_runtime_id: RuntimeId,
    pub sub_sequence_id: SequenceId,
    pub grant_id: crate::continuity::ContinuationGrantId,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct FederationPeer {
    pub id: FederationPeerId,
    pub name: String,
    pub trust_root_sha256: String,
    pub public_key: String,
    pub endpoint: String,
    pub allowed_tenants: Vec<TenantId>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct FederationEnvelope {
    pub id: FederationMessageId,
    pub peer_id: FederationPeerId,
    pub tenant_id: TenantId,
    pub continuity_id: ContinuityId,
    pub epoch: ExecutionEpoch,
    pub destination_runtime_id: RuntimeId,
    pub payload_sha256: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub signature: String,
}
