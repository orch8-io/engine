//! Public product contracts built on the portable execution continuity core.
//!
//! This module keeps adoption, conformance, evidence, and commercial contracts
//! independent from a particular transport. HTTP, MCP, local workers, and OEM
//! runtimes can therefore implement the same protocol and run the same tests.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::continuity::{
    CapsuleRequirements, ContinuityId, DataClassification, EffectReceipt, ExecutionEpoch,
    LocalityPolicy, LocalityRule, PolicyOutcome, RuntimeCapabilities, RuntimeConnectivity,
    RuntimeId, RuntimeKind, RuntimeTrustLevel,
};
use crate::ids::{InstanceId, TenantId};

fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    use std::fmt::Write as _;

    let digest = Sha256::digest(bytes.as_ref());
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

/// Current framework-neutral Durable Agent Handoff Protocol version.
pub const CURRENT_PROTOCOL: ProtocolVersion = ProtocolVersion { major: 1, minor: 0 };

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    #[must_use]
    pub const fn can_read(self, offered: Self) -> bool {
        self.major == offered.major && self.minor >= offered.minor
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProductContractError {
    #[error("unsupported protocol version {major}.{minor}")]
    UnsupportedProtocol { major: u16, minor: u16 },
    #[error("{0} must not be empty")]
    Empty(&'static str),
    #[error("work offer has expired")]
    OfferExpired,
    #[error("runtime passport has expired")]
    PassportExpired,
    #[error("runtime passport does not match its capability snapshot")]
    PassportCapabilityMismatch,
    #[error("invalid placement policy: {0}")]
    InvalidPolicy(String),
    #[error("execution receipt digest mismatch")]
    ReceiptDigestMismatch,
    #[error("execution receipt contains an unresolved effect outcome")]
    ReceiptIncomplete,
    #[error("signature verification failed")]
    InvalidSignature,
    #[error("conformance certificate requires all mandatory checks and score >= 900")]
    CertificationThresholdNotMet,
    #[error("commercial plan violates invariant: {0}")]
    InvalidCommercialPlan(String),
}

/// Framework-neutral unit of work offered to capability-aware runtimes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PortableWorkOffer {
    pub id: Uuid,
    pub protocol: ProtocolVersion,
    pub tenant_id: TenantId,
    pub continuity_id: ContinuityId,
    pub expected_epoch: ExecutionEpoch,
    pub handler: String,
    pub input: serde_json::Value,
    pub input_schema: serde_json::Value,
    pub requirements: CapsuleRequirements,
    pub policy: Option<LocalityPolicy>,
    pub classification: DataClassification,
    pub idempotency_key: String,
    pub receipt_required: bool,
    pub expires_at: DateTime<Utc>,
}

impl PortableWorkOffer {
    pub const MAX_INPUT_BYTES: usize = 1024 * 1024;

    pub fn validate(&self, now: DateTime<Utc>) -> Result<(), ProductContractError> {
        if !CURRENT_PROTOCOL.can_read(self.protocol) {
            return Err(ProductContractError::UnsupportedProtocol {
                major: self.protocol.major,
                minor: self.protocol.minor,
            });
        }
        for (name, value) in [
            ("handler", self.handler.as_str()),
            ("idempotency_key", self.idempotency_key.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(ProductContractError::Empty(name));
            }
        }
        if self.expires_at <= now {
            return Err(ProductContractError::OfferExpired);
        }
        if self.expires_at > now + chrono::Duration::hours(24) {
            return Err(ProductContractError::InvalidPolicy(
                "work offer lifetime exceeds 24 hours".into(),
            ));
        }
        if self.handler.len() > 256 || self.idempotency_key.len() > 256 {
            return Err(ProductContractError::InvalidPolicy(
                "handler and idempotency key are limited to 256 bytes".into(),
            ));
        }
        if !self.input_schema.is_object() {
            return Err(ProductContractError::InvalidPolicy(
                "input_schema must be a JSON object".into(),
            ));
        }
        if serde_json::to_vec(&self.input).is_ok_and(|bytes| bytes.len() > Self::MAX_INPUT_BYTES) {
            return Err(ProductContractError::InvalidPolicy(
                "work offer input exceeds 1 MiB".into(),
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn can_be_claimed_by(&self, runtime: &RuntimeCapabilities, now: DateTime<Utc>) -> bool {
        self.validate(now).is_ok()
            && self.requirements.is_satisfied_by(runtime, now)
            && self.policy.as_ref().is_none_or(|policy| {
                runtime_allowed_by_policy(policy, self.classification, runtime)
            })
    }
}

/// Signed identity document for a runtime and its bounded capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RuntimePassport {
    pub protocol: ProtocolVersion,
    pub runtime_id: RuntimeId,
    pub issuer: String,
    pub subject: String,
    pub capabilities: RuntimeCapabilities,
    pub public_key: String,
    pub attestation_sha256: Option<String>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub signature: String,
}

impl RuntimePassport {
    pub fn validate(&self, now: DateTime<Utc>) -> Result<(), ProductContractError> {
        if !CURRENT_PROTOCOL.can_read(self.protocol) {
            return Err(ProductContractError::UnsupportedProtocol {
                major: self.protocol.major,
                minor: self.protocol.minor,
            });
        }
        if self.expires_at <= now {
            return Err(ProductContractError::PassportExpired);
        }
        if self.issued_at > now + chrono::Duration::minutes(5)
            || self.expires_at <= self.issued_at
            || self.expires_at > self.issued_at + chrono::Duration::hours(24)
        {
            return Err(ProductContractError::PassportCapabilityMismatch);
        }
        if self.runtime_id != self.capabilities.runtime_id
            || self.capabilities.expires_at < self.expires_at
            || self.capabilities.observed_at > self.issued_at
        {
            return Err(ProductContractError::PassportCapabilityMismatch);
        }
        for (name, value) in [
            ("issuer", self.issuer.as_str()),
            ("subject", self.subject.as_str()),
            ("public_key", self.public_key.as_str()),
            ("signature", self.signature.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(ProductContractError::Empty(name));
            }
        }
        Ok(())
    }

    pub fn verify_with(
        &self,
        now: DateTime<Utc>,
        verifier: impl FnOnce(&[u8], &str, &str) -> bool,
    ) -> Result<(), ProductContractError> {
        self.validate(now)?;
        let mut unsigned = self.clone();
        unsigned.signature.clear();
        let payload = serde_json::to_vec(&unsigned).expect("runtime passport is serializable");
        if verifier(&payload, &self.public_key, &self.signature) {
            Ok(())
        } else {
            Err(ProductContractError::InvalidSignature)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionLocationReceipt {
    pub runtime_id: RuntimeId,
    pub kind: RuntimeKind,
    pub trust: RuntimeTrustLevel,
    pub region: Option<String>,
    pub entered_at: DateTime<Utc>,
    pub exited_at: Option<DateTime<Utc>>,
}

/// Portable, hash-verifiable bill of execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionReceipt {
    pub protocol: ProtocolVersion,
    pub receipt_id: Uuid,
    pub tenant_id: TenantId,
    pub continuity_id: ContinuityId,
    pub instance_id: InstanceId,
    pub final_epoch: ExecutionEpoch,
    pub sequence_sha256: String,
    pub model_ids: Vec<String>,
    pub tool_ids: Vec<String>,
    pub locations: Vec<ExecutionLocationReceipt>,
    pub effects: Vec<EffectReceipt>,
    pub policy_outcome: PolicyOutcome,
    pub consent_receipt_ids: Vec<String>,
    pub previous_receipt_sha256: Option<String>,
    pub created_at: DateTime<Utc>,
    pub digest_sha256: String,
    pub signing_key_id: Option<String>,
    pub signature: Option<String>,
}

impl ExecutionReceipt {
    #[must_use]
    pub fn calculate_digest(&self) -> String {
        let mut value = self.clone();
        value.digest_sha256.clear();
        value.signature = None;
        let bytes = serde_json::to_vec(&value).expect("execution receipt is serializable");
        sha256_hex(bytes)
    }

    pub fn seal(&mut self) {
        self.digest_sha256 = self.calculate_digest();
    }

    pub fn verify(&self) -> Result<(), ProductContractError> {
        if !CURRENT_PROTOCOL.can_read(self.protocol) {
            return Err(ProductContractError::UnsupportedProtocol {
                major: self.protocol.major,
                minor: self.protocol.minor,
            });
        }
        if self.digest_sha256 != self.calculate_digest() {
            return Err(ProductContractError::ReceiptDigestMismatch);
        }
        if self
            .effects
            .iter()
            .any(|effect| !effect.state.is_resolved())
        {
            return Err(ProductContractError::ReceiptIncomplete);
        }
        Ok(())
    }

    pub fn verify_signature_with(
        &self,
        verifier: impl FnOnce(&[u8], &str) -> bool,
    ) -> Result<(), ProductContractError> {
        self.verify()?;
        let signature = self
            .signature
            .as_deref()
            .ok_or(ProductContractError::InvalidSignature)?;
        if verifier(self.digest_sha256.as_bytes(), signature) {
            Ok(())
        } else {
            Err(ProductContractError::InvalidSignature)
        }
    }
}

fn runtime_allowed_by_policy(
    policy: &LocalityPolicy,
    classification: DataClassification,
    runtime: &RuntimeCapabilities,
) -> bool {
    policy
        .rules
        .iter()
        .filter(|rule| rule.classification == classification)
        .any(|rule| {
            (rule.allowed_runtime_ids.is_empty()
                || rule.allowed_runtime_ids.contains(&runtime.runtime_id))
                && (rule.allowed_runtime_kinds.is_empty()
                    || rule.allowed_runtime_kinds.contains(&runtime.kind))
                && (rule.allowed_regions.is_empty()
                    || rule
                        .allowed_regions
                        .iter()
                        .any(|region| runtime.regions.contains(region)))
                && rule
                    .minimum_trust
                    .is_none_or(|trust| runtime.trust >= trust)
                && rule
                    .require_offline
                    .is_none_or(|required| !required || runtime.offline_capable)
                && rule
                    .require_hardware
                    .as_ref()
                    .is_none_or(|hardware| runtime.hardware.contains(hardware))
                && (rule.allowed_connectivity.is_empty()
                    || runtime.connectivity.is_some_and(|connectivity| {
                        rule.allowed_connectivity.contains(&connectivity)
                    }))
                && rule.minimum_battery_percent.is_none_or(|minimum| {
                    runtime
                        .battery_percent
                        .is_some_and(|battery| battery >= minimum)
                })
                && rule.maximum_cost_microunits.is_none_or(|maximum| {
                    runtime
                        .estimated_cost_microunits
                        .is_some_and(|cost| cost <= maximum)
                })
                && rule.maximum_latency_ms.is_none_or(|maximum| {
                    runtime
                        .estimated_latency_ms
                        .is_some_and(|latency| latency <= maximum)
                })
        })
}

/// Human-editable placement policy compiled into the durable core vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CompiledPlacementPolicy {
    pub source: String,
    pub requirements: CapsuleRequirements,
    pub policy: LocalityPolicy,
    pub classification: DataClassification,
    pub source_sha256: String,
}

pub fn compile_placement_policy(
    source: &str,
) -> Result<CompiledPlacementPolicy, ProductContractError> {
    let mut requirements = CapsuleRequirements::default();
    let mut classification = DataClassification::Internal;
    let mut rule = LocalityRule {
        classification,
        allowed_runtime_ids: Vec::new(),
        allowed_runtime_kinds: Vec::new(),
        allowed_regions: Vec::new(),
        minimum_trust: None,
        require_offline: None,
        require_hardware: None,
        allowed_connectivity: Vec::new(),
        minimum_battery_percent: None,
        maximum_cost_microunits: None,
        maximum_latency_ms: None,
    };
    if source.trim().is_empty() {
        return Err(ProductContractError::Empty("policy"));
    }
    if source.len() > 16 * 1024 {
        return Err(ProductContractError::InvalidPolicy(
            "policy exceeds 16 KiB".into(),
        ));
    }
    let mut seen_keys = BTreeSet::new();
    for statement in source.split(';').filter(|item| !item.trim().is_empty()) {
        let (raw_key, raw_value) = statement
            .split_once('=')
            .ok_or_else(|| ProductContractError::InvalidPolicy(statement.trim().into()))?;
        let key = raw_key.trim();
        let value = raw_value.trim();
        if !seen_keys.insert(key) {
            return Err(ProductContractError::InvalidPolicy(format!(
                "duplicate key {key}"
            )));
        }
        let list = || {
            value
                .split(',')
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect::<Vec<_>>()
        };
        match key {
            "classification" => {
                classification = parse_classification(value)?;
                rule.classification = classification;
            }
            "regions" => rule.allowed_regions = list(),
            "handlers" => requirements.handlers = list(),
            "plugins" => requirements.plugins = list(),
            "credentials" => requirements.credentials = list(),
            "hardware" => requirements.hardware = list(),
            "require_hardware" => rule.require_hardware = Some(value.to_string()),
            "min_trust" => {
                let trust = parse_trust(value)?;
                rule.minimum_trust = Some(trust);
                requirements.minimum_trust = Some(trust);
            }
            "runtime_kinds" => {
                rule.allowed_runtime_kinds = list()
                    .iter()
                    .map(|item| parse_runtime_kind(item))
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "connectivity" => {
                rule.allowed_connectivity = list()
                    .iter()
                    .map(|item| parse_connectivity(item))
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "require_offline" => rule.require_offline = Some(parse_bool(value, key)?),
            "human_ui" => requirements.requires_human_ui = parse_bool(value, key)?,
            "network" => requirements.requires_network = parse_bool(value, key)?,
            "minimum_battery" => rule.minimum_battery_percent = Some(parse_number(value, key)?),
            "maximum_cost" => rule.maximum_cost_microunits = Some(parse_number(value, key)?),
            "maximum_latency_ms" => rule.maximum_latency_ms = Some(parse_number(value, key)?),
            _ => {
                return Err(ProductContractError::InvalidPolicy(format!(
                    "unknown key {key}"
                )));
            }
        }
    }
    let digest = sha256_hex(source);
    Ok(CompiledPlacementPolicy {
        source: source.into(),
        requirements,
        policy: LocalityPolicy {
            version: 1,
            rules: vec![rule],
        },
        classification,
        source_sha256: digest,
    })
}

fn parse_classification(value: &str) -> Result<DataClassification, ProductContractError> {
    match value {
        "public" => Ok(DataClassification::Public),
        "internal" => Ok(DataClassification::Internal),
        "confidential" => Ok(DataClassification::Confidential),
        "restricted" => Ok(DataClassification::Restricted),
        _ => Err(ProductContractError::InvalidPolicy(format!(
            "unknown classification {value}"
        ))),
    }
}

fn parse_trust(value: &str) -> Result<RuntimeTrustLevel, ProductContractError> {
    match value {
        "unverified" => Ok(RuntimeTrustLevel::Unverified),
        "registered" => Ok(RuntimeTrustLevel::Registered),
        "signed" => Ok(RuntimeTrustLevel::Signed),
        "attested" => Ok(RuntimeTrustLevel::Attested),
        _ => Err(ProductContractError::InvalidPolicy(format!(
            "unknown trust level {value}"
        ))),
    }
}

fn parse_runtime_kind(value: &str) -> Result<RuntimeKind, ProductContractError> {
    match value {
        "server" => Ok(RuntimeKind::Server),
        "edge" => Ok(RuntimeKind::Edge),
        "mobile" => Ok(RuntimeKind::Mobile),
        "desktop" => Ok(RuntimeKind::Desktop),
        "browser" => Ok(RuntimeKind::Browser),
        _ => Err(ProductContractError::InvalidPolicy(format!(
            "unknown runtime kind {value}"
        ))),
    }
}

fn parse_connectivity(value: &str) -> Result<RuntimeConnectivity, ProductContractError> {
    match value {
        "offline" => Ok(RuntimeConnectivity::Offline),
        "metered" => Ok(RuntimeConnectivity::Metered),
        "wifi" => Ok(RuntimeConnectivity::Wifi),
        "ethernet" => Ok(RuntimeConnectivity::Ethernet),
        _ => Err(ProductContractError::InvalidPolicy(format!(
            "unknown connectivity {value}"
        ))),
    }
}

fn parse_bool(value: &str, key: &str) -> Result<bool, ProductContractError> {
    value
        .parse()
        .map_err(|_| ProductContractError::InvalidPolicy(format!("{key} expects true or false")))
}

fn parse_number<T: std::str::FromStr>(value: &str, key: &str) -> Result<T, ProductContractError> {
    value
        .parse()
        .map_err(|_| ProductContractError::InvalidPolicy(format!("{key} expects a number")))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum GatewayAdapter {
    GenericHttp,
    Mcp,
    LocalProcess,
    MobileWorker,
}

/// Drop-in wrapper/gateway manifest used by `orch8 wrap`, MCP, and local workers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GatewayManifest {
    pub protocol: ProtocolVersion,
    pub name: String,
    pub adapter: GatewayAdapter,
    pub entrypoint: String,
    #[serde(default)]
    pub arguments: Vec<String>,
    pub handler: String,
    pub policy_source: String,
    pub environment_allowlist: Vec<String>,
    pub secret_references: Vec<String>,
    pub receipt_required: bool,
}

impl GatewayManifest {
    pub fn validate(&self) -> Result<CompiledPlacementPolicy, ProductContractError> {
        if !CURRENT_PROTOCOL.can_read(self.protocol) {
            return Err(ProductContractError::UnsupportedProtocol {
                major: self.protocol.major,
                minor: self.protocol.minor,
            });
        }
        for (name, value) in [
            ("name", self.name.as_str()),
            ("entrypoint", self.entrypoint.as_str()),
            ("handler", self.handler.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(ProductContractError::Empty(name));
            }
        }
        let unique = self.secret_references.iter().collect::<BTreeSet<_>>();
        if unique.len() != self.secret_references.len() {
            return Err(ProductContractError::InvalidPolicy(
                "duplicate secret reference".into(),
            ));
        }
        if self.environment_allowlist.len() > 256 || self.secret_references.len() > 256 {
            return Err(ProductContractError::InvalidPolicy(
                "gateway allowlists are limited to 256 entries".into(),
            ));
        }
        if self.arguments.len() > 256
            || self.arguments.iter().any(|argument| argument.len() > 4_096)
        {
            return Err(ProductContractError::InvalidPolicy(
                "gateway arguments are limited to 256 entries of 4 KiB".into(),
            ));
        }
        if self
            .secret_references
            .iter()
            .any(|reference| reference.len() > 1_024 || !reference.contains("://"))
        {
            return Err(ProductContractError::InvalidPolicy(
                "secret references must be bounded URI references".into(),
            ));
        }
        compile_placement_policy(&self.policy_source)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConformanceCheck {
    ProtocolCompatibility,
    AtomicOwnership,
    StaleOwnerRejected,
    DuplicateEffectFenced,
    OfflineResume,
    PolicyEnforced,
    ReceiptVerifiable,
    TenantIsolation,
}

impl ConformanceCheck {
    pub const ALL: [Self; 8] = [
        Self::ProtocolCompatibility,
        Self::AtomicOwnership,
        Self::StaleOwnerRejected,
        Self::DuplicateEffectFenced,
        Self::OfflineResume,
        Self::PolicyEnforced,
        Self::ReceiptVerifiable,
        Self::TenantIsolation,
    ];

    #[must_use]
    pub const fn mandatory(self) -> bool {
        !matches!(self, Self::OfflineResume)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ConformanceCheckResult {
    pub check: ConformanceCheck,
    pub passed: bool,
    pub evidence_sha256: String,
    pub duration_ms: u64,
    pub finding: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ContinuityScore {
    pub score_millipoints: u16,
    pub passed: usize,
    pub total: usize,
    pub mandatory_failures: Vec<ConformanceCheck>,
}

#[must_use]
pub fn score_conformance(results: &[ConformanceCheckResult]) -> ContinuityScore {
    let passed_once = |check| {
        let matches = results
            .iter()
            .filter(|result| result.check == check)
            .collect::<Vec<_>>();
        matches.len() == 1
            && matches[0].passed
            && matches[0].evidence_sha256.len() == 64
            && matches[0]
                .evidence_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
    };
    let passed = ConformanceCheck::ALL
        .iter()
        .filter(|check| passed_once(**check))
        .count();
    let total = ConformanceCheck::ALL.len();
    let score_millipoints = u16::try_from((passed * 1_000) / total).unwrap_or(1_000);
    let mandatory_failures = ConformanceCheck::ALL
        .iter()
        .copied()
        .filter(|check| check.mandatory() && !passed_once(*check))
        .collect();
    ContinuityScore {
        score_millipoints,
        passed,
        total,
        mandatory_failures,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ConformanceCertificate {
    pub protocol: ProtocolVersion,
    pub subject: String,
    pub score: ContinuityScore,
    pub results_sha256: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub issuer: String,
    pub signature: Option<String>,
}

impl ConformanceCertificate {
    /// Dependency-free SVG badge suitable for CI README and release pages.
    #[must_use]
    pub fn badge_svg(&self) -> String {
        let subject = xml_escape(&self.subject);
        let score = self.score.score_millipoints / 10;
        let color = if self.score.mandatory_failures.is_empty() && score >= 90 {
            "#2f855a"
        } else {
            "#c53030"
        };
        format!(
            r##"<svg xmlns="http://www.w3.org/2000/svg" width="360" height="20" role="img" aria-label="Orch8 continuity: {score}%"><rect width="210" height="20" fill="#2d3748"/><rect x="210" width="150" height="20" fill="{color}"/><g fill="#fff" font-family="Verdana,sans-serif" font-size="11"><text x="8" y="14">{subject} continuity</text><text x="225" y="14">certified {score}%</text></g></svg>"##
        )
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub fn issue_conformance_certificate(
    subject: String,
    issuer: String,
    results: &[ConformanceCheckResult],
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> Result<ConformanceCertificate, ProductContractError> {
    let score = score_conformance(results);
    if score.score_millipoints < 900 || !score.mandatory_failures.is_empty() {
        return Err(ProductContractError::CertificationThresholdNotMet);
    }
    if expires_at <= issued_at {
        return Err(ProductContractError::InvalidCommercialPlan(
            "certificate expiration must follow issuance".into(),
        ));
    }
    if expires_at > issued_at + chrono::Duration::days(366) {
        return Err(ProductContractError::InvalidCommercialPlan(
            "certificate lifetime exceeds 366 days".into(),
        ));
    }
    if subject.trim().is_empty() {
        return Err(ProductContractError::Empty("subject"));
    }
    if issuer.trim().is_empty() {
        return Err(ProductContractError::Empty("issuer"));
    }
    let results_sha256 =
        sha256_hex(serde_json::to_vec(results).expect("conformance results serialize"));
    Ok(ConformanceCertificate {
        protocol: CURRENT_PROTOCOL,
        subject,
        score,
        results_sha256,
        issued_at,
        expires_at,
        issuer,
        signature: None,
    })
}

/// Opinionated profiles make the highest-value trust-boundary use cases
/// deployable without inventing policy from scratch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TrustBoundaryProfile {
    PrivateRag,
    BiometricApproval,
    DataResidency,
    BillOfExecution,
    AuditEvidence,
    SecretSafeCoding,
    RegulatedOnboarding,
    FraudChallenge,
    ExecutiveAirlock,
    PersonalDataVault,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ProfileContract {
    pub profile: TrustBoundaryProfile,
    pub handler: String,
    pub policy_source: String,
    pub required_evidence: Vec<String>,
    pub forbidden_payload_classes: Vec<String>,
    pub requires_signed_receipt: bool,
}

impl TrustBoundaryProfile {
    pub const ALL: [Self; 10] = [
        Self::PrivateRag,
        Self::BiometricApproval,
        Self::DataResidency,
        Self::BillOfExecution,
        Self::AuditEvidence,
        Self::SecretSafeCoding,
        Self::RegulatedOnboarding,
        Self::FraudChallenge,
        Self::ExecutiveAirlock,
        Self::PersonalDataVault,
    ];

    #[must_use]
    pub fn contract(self) -> ProfileContract {
        use TrustBoundaryProfile as P;
        let (handler, policy, evidence, forbidden) = match self {
            P::PrivateRag => (
                "private_rag.query",
                "classification=restricted;runtime_kinds=mobile,desktop;min_trust=signed;handlers=private_rag.query",
                vec!["result_digest", "runtime_passport"],
                vec!["raw_document"],
            ),
            P::BiometricApproval => (
                "device.biometric.verify",
                "classification=confidential;runtime_kinds=mobile;min_trust=attested;human_ui=true;handlers=device.biometric.verify",
                vec!["biometric_assertion", "consent_receipt"],
                vec!["biometric_template"],
            ),
            P::DataResidency => (
                "residency.execute",
                "classification=restricted;min_trust=signed;regions=tenant_primary;handlers=residency.execute",
                vec!["placement_decision", "location_history"],
                vec!["unredacted_cross_region_payload"],
            ),
            P::BillOfExecution => (
                "receipt.emit",
                "classification=confidential;min_trust=signed;handlers=receipt.emit",
                vec!["execution_receipt", "effect_receipts"],
                vec!["raw_secret"],
            ),
            P::AuditEvidence => (
                "audit.export",
                "classification=confidential;min_trust=signed;handlers=audit.export",
                vec!["provenance_chain", "policy_outcome", "execution_receipt"],
                vec!["raw_prompt"],
            ),
            P::SecretSafeCoding => (
                "coding.local",
                "classification=restricted;runtime_kinds=desktop;min_trust=attested;handlers=coding.local;credentials=source_control;hardware=secure_enclave",
                vec!["patch_digest", "runtime_passport"],
                vec!["repository", "credential"],
            ),
            P::RegulatedOnboarding => (
                "onboarding.local",
                "classification=restricted;runtime_kinds=mobile,desktop;min_trust=attested;human_ui=true;handlers=onboarding.local",
                vec!["consent_receipt", "verification_digest"],
                vec!["identity_document"],
            ),
            P::FraudChallenge => (
                "fraud.challenge",
                "classification=restricted;runtime_kinds=mobile;min_trust=attested;human_ui=true;handlers=fraud.challenge",
                vec!["challenge_result", "runtime_passport"],
                vec!["authentication_secret"],
            ),
            P::ExecutiveAirlock => (
                "airlock.approve",
                "classification=restricted;runtime_kinds=mobile;min_trust=attested;human_ui=true;handlers=airlock.approve",
                vec!["biometric_assertion", "effect_receipt", "consent_receipt"],
                vec!["signing_key"],
            ),
            P::PersonalDataVault => (
                "vault.compute",
                "classification=restricted;runtime_kinds=mobile,desktop;min_trust=attested;handlers=vault.compute",
                vec!["result_digest", "disclosure_report"],
                vec!["vault_content"],
            ),
        };
        ProfileContract {
            profile: self,
            handler: handler.into(),
            policy_source: policy.into(),
            required_evidence: evidence.into_iter().map(str::to_string).collect(),
            forbidden_payload_classes: forbidden.into_iter().map(str::to_string).collect(),
            requires_signed_receipt: true,
        }
    }

    pub fn compile(self) -> Result<CompiledPlacementPolicy, ProductContractError> {
        compile_placement_policy(&self.contract().policy_source)
    }

    pub fn work_offer(
        self,
        tenant_id: TenantId,
        continuity_id: ContinuityId,
        expected_epoch: ExecutionEpoch,
        input: serde_json::Value,
        idempotency_key: String,
        now: DateTime<Utc>,
    ) -> Result<PortableWorkOffer, ProductContractError> {
        let contract = self.contract();
        let compiled = self.compile()?;
        let offer = PortableWorkOffer {
            id: Uuid::now_v7(),
            protocol: CURRENT_PROTOCOL,
            tenant_id,
            continuity_id,
            expected_epoch,
            handler: contract.handler,
            input,
            input_schema: serde_json::json!({"type": "object"}),
            requirements: compiled.requirements,
            policy: Some(compiled.policy),
            classification: compiled.classification,
            idempotency_key,
            receipt_required: contract.requires_signed_receipt,
            expires_at: now + chrono::Duration::minutes(15),
        };
        offer.validate(now)?;
        Ok(offer)
    }
}

impl std::str::FromStr for TrustBoundaryProfile {
    type Err = ProductContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "private_rag" => Ok(Self::PrivateRag),
            "biometric_approval" => Ok(Self::BiometricApproval),
            "data_residency" => Ok(Self::DataResidency),
            "bill_of_execution" => Ok(Self::BillOfExecution),
            "audit_evidence" => Ok(Self::AuditEvidence),
            "secret_safe_coding" => Ok(Self::SecretSafeCoding),
            "regulated_onboarding" => Ok(Self::RegulatedOnboarding),
            "fraud_challenge" => Ok(Self::FraudChallenge),
            "executive_airlock" => Ok(Self::ExecutiveAirlock),
            "personal_data_vault" => Ok(Self::PersonalDataVault),
            _ => Err(ProductContractError::InvalidPolicy(format!(
                "unknown trust-boundary profile {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RelayDeployment {
    Hosted,
    PrivateCloud,
    OemEmbedded,
}

/// Enforceable plan for hosted relay, Conformance Cloud, and OEM deployments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CommercialContinuityPlan {
    pub deployment: RelayDeployment,
    pub tenant_isolation: bool,
    pub signed_receipts: bool,
    pub conformance_required: bool,
    pub evidence_retention_days: u16,
    pub allowed_protocol_majors: Vec<u16>,
    pub oem_product_id: Option<String>,
    pub usage_meter: Option<String>,
}

impl CommercialContinuityPlan {
    pub fn validate(&self) -> Result<(), ProductContractError> {
        if !self.tenant_isolation {
            return Err(ProductContractError::InvalidCommercialPlan(
                "tenant isolation is mandatory".into(),
            ));
        }
        if !self.signed_receipts {
            return Err(ProductContractError::InvalidCommercialPlan(
                "signed receipts are mandatory".into(),
            ));
        }
        if !self.conformance_required {
            return Err(ProductContractError::InvalidCommercialPlan(
                "conformance is mandatory".into(),
            ));
        }
        if self.evidence_retention_days == 0 {
            return Err(ProductContractError::InvalidCommercialPlan(
                "evidence retention must be non-zero".into(),
            ));
        }
        if !self
            .allowed_protocol_majors
            .contains(&CURRENT_PROTOCOL.major)
        {
            return Err(ProductContractError::InvalidCommercialPlan(
                "current protocol is not allowed".into(),
            ));
        }
        if matches!(self.deployment, RelayDeployment::OemEmbedded)
            && self.oem_product_id.as_deref().is_none_or(str::is_empty)
        {
            return Err(ProductContractError::InvalidCommercialPlan(
                "OEM product id is required".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "continuity_product_tests.rs"]
mod tests;
