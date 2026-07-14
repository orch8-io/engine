//! Portable-continuity compatibility, ownership, and provenance services.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use orch8_storage::StorageBackend;
use orch8_types::continuity::{
    CapsuleRequirements, ContinuityExecution, ExecutionHandoff, HandoffState, ProvenanceEntry,
    RuntimeCapabilities,
};
use orch8_types::error::StorageError;
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityStatus {
    Pass,
    Fail,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CompatibilityFinding {
    pub code: &'static str,
    pub status: CompatibilityStatus,
    pub summary: String,
}

#[must_use]
pub fn assess_compatibility(
    requirements: &CapsuleRequirements,
    capabilities: &RuntimeCapabilities,
    now: DateTime<Utc>,
) -> Vec<CompatibilityFinding> {
    let mut findings = Vec::new();
    if capabilities.expires_at <= now {
        findings.push(CompatibilityFinding {
            code: "CAPABILITIES_EXPIRED",
            status: CompatibilityStatus::Fail,
            summary: "runtime capability advertisement has expired".into(),
        });
    }
    compare_set(
        "HANDLERS_MISSING",
        "handler",
        &requirements.handlers,
        &capabilities.handlers,
        &mut findings,
    );
    compare_set(
        "PLUGINS_MISSING",
        "plugin",
        &requirements.plugins,
        &capabilities.plugins,
        &mut findings,
    );
    compare_set(
        "HARDWARE_MISSING",
        "hardware capability",
        &requirements.hardware,
        &capabilities.hardware,
        &mut findings,
    );
    if !requirements.regions.is_empty()
        && !requirements
            .regions
            .iter()
            .any(|region| capabilities.regions.contains(region))
    {
        findings.push(CompatibilityFinding {
            code: "REGION_NOT_ALLOWED",
            status: CompatibilityStatus::Fail,
            summary: "runtime is not in any allowed region".into(),
        });
    }
    if let Some(minimum) = requirements.minimum_trust
        && capabilities.trust < minimum
    {
        findings.push(CompatibilityFinding {
            code: "TRUST_TOO_LOW",
            status: CompatibilityStatus::Fail,
            summary: format!(
                "runtime trust {:?} is below required {:?}",
                capabilities.trust, minimum
            ),
        });
    }
    if requirements.requires_human_ui
        && !matches!(
            capabilities.kind,
            orch8_types::continuity::RuntimeKind::Mobile
                | orch8_types::continuity::RuntimeKind::Desktop
                | orch8_types::continuity::RuntimeKind::Browser
        )
    {
        findings.push(CompatibilityFinding {
            code: "HUMAN_UI_MISSING",
            status: CompatibilityStatus::Unknown,
            summary: "runtime kind does not prove an interactive human UI is available".into(),
        });
    }
    if findings.is_empty() {
        findings.push(CompatibilityFinding {
            code: "COMPATIBLE",
            status: CompatibilityStatus::Pass,
            summary: "all declared requirements are satisfied".into(),
        });
    }
    findings
}

fn compare_set(
    code: &'static str,
    label: &str,
    required: &[String],
    offered: &[String],
    findings: &mut Vec<CompatibilityFinding>,
) {
    let offered: BTreeSet<&str> = offered.iter().map(String::as_str).collect();
    let missing: Vec<&str> = required
        .iter()
        .map(String::as_str)
        .filter(|candidate| !offered.contains(candidate))
        .collect();
    if !missing.is_empty() {
        findings.push(CompatibilityFinding {
            code,
            status: CompatibilityStatus::Fail,
            summary: format!("missing {label}(s): {}", missing.join(", ")),
        });
    }
}

#[derive(Debug, Error)]
pub enum ContinuityServiceError {
    #[error("handoff must be exported before it can be accepted")]
    HandoffNotExported,
    #[error("handoff identity, tenant, continuity id, or destination changed")]
    HandoffIdentityChanged,
    #[error("handoff version must increment by exactly one")]
    InvalidHandoffVersion,
    #[error("execution identity or expected ownership changed")]
    ExecutionIdentityChanged,
    #[error("accepted execution must increment the epoch and assign the destination owner")]
    InvalidOwnershipClaim,
    #[error("handoff or ownership compare-and-swap lost")]
    StaleClaim,
    #[error(transparent)]
    Storage(#[from] StorageError),
}

pub async fn accept_handoff(
    storage: &dyn StorageBackend,
    expected_handoff: &ExecutionHandoff,
    accepted_handoff: &ExecutionHandoff,
    expected_execution: &ContinuityExecution,
    accepted_execution: &ContinuityExecution,
) -> Result<(), ContinuityServiceError> {
    if expected_handoff.state != HandoffState::Exported
        || accepted_handoff.state != HandoffState::Accepted
    {
        return Err(ContinuityServiceError::HandoffNotExported);
    }
    if expected_handoff.id != accepted_handoff.id
        || expected_handoff.tenant_id != accepted_handoff.tenant_id
        || expected_handoff.continuity_id != accepted_handoff.continuity_id
        || expected_handoff.destination_runtime_id != accepted_handoff.destination_runtime_id
    {
        return Err(ContinuityServiceError::HandoffIdentityChanged);
    }
    if expected_handoff.version.checked_add(1) != Some(accepted_handoff.version) {
        return Err(ContinuityServiceError::InvalidHandoffVersion);
    }
    if expected_execution.continuity_id != accepted_execution.continuity_id
        || expected_execution.tenant_id != accepted_execution.tenant_id
        || expected_execution.continuity_id != expected_handoff.continuity_id
        || expected_execution.epoch != expected_handoff.expected_epoch
    {
        return Err(ContinuityServiceError::ExecutionIdentityChanged);
    }
    if expected_execution.epoch.checked_next().ok() != Some(accepted_execution.epoch)
        || accepted_execution.owner_runtime_id != expected_handoff.destination_runtime_id
    {
        return Err(ContinuityServiceError::InvalidOwnershipClaim);
    }
    let accepted = storage
        .accept_handoff(
            &expected_handoff.tenant_id,
            expected_handoff,
            accepted_handoff,
            expected_execution,
            accepted_execution,
        )
        .await?;
    if !accepted {
        return Err(ContinuityServiceError::StaleClaim);
    }
    Ok(())
}

#[must_use]
pub fn build_provenance_entry(
    execution: &ContinuityExecution,
    kind: impl Into<String>,
    payload_sha256: impl Into<String>,
    previous_sha256: Option<String>,
    now: DateTime<Utc>,
) -> ProvenanceEntry {
    let kind = kind.into();
    let payload_sha256 = payload_sha256.into();
    let mut hash = Sha256::new();
    hash.update(execution.continuity_id.to_string());
    hash.update(execution.epoch.get().to_be_bytes());
    hash.update(kind.as_bytes());
    hash.update(payload_sha256.as_bytes());
    if let Some(previous) = &previous_sha256 {
        hash.update(previous.as_bytes());
    }
    let mut entry_sha256 = String::with_capacity(64);
    for byte in hash.finalize() {
        write!(&mut entry_sha256, "{byte:02x}").expect("writing to String cannot fail");
    }
    ProvenanceEntry {
        id: Uuid::now_v7(),
        continuity_id: execution.continuity_id,
        tenant_id: execution.tenant_id.clone(),
        epoch: execution.epoch,
        kind,
        payload_sha256,
        previous_sha256,
        entry_sha256,
        signing_key_id: None,
        signature: None,
        created_at: now,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::continuity::{RuntimeId, RuntimeKind, RuntimeTrustLevel};

    #[test]
    fn compatibility_fails_closed_for_expired_or_missing_capabilities() {
        let now = Utc::now();
        let findings = assess_compatibility(
            &CapsuleRequirements {
                handlers: vec!["camera".into()],
                minimum_trust: Some(RuntimeTrustLevel::Signed),
                ..CapsuleRequirements::default()
            },
            &RuntimeCapabilities {
                runtime_id: RuntimeId::new(),
                kind: RuntimeKind::Mobile,
                trust: RuntimeTrustLevel::Registered,
                handlers: Vec::new(),
                plugins: Vec::new(),
                regions: vec!["br-south".into()],
                hardware: Vec::new(),
                offline_capable: true,
                observed_at: now - chrono::Duration::minutes(2),
                expires_at: now - chrono::Duration::minutes(1),
            },
            now,
        );
        assert_eq!(
            findings
                .iter()
                .filter(|finding| finding.status == CompatibilityStatus::Fail)
                .count(),
            3
        );
    }
}
