//! Portable-continuity compatibility, ownership, and provenance services.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use orch8_storage::StorageBackend;
use orch8_types::continuity::{
    CapsuleRequirements, ContinuityExecution, ExecutionHandoff, HandoffState, ProvenanceEntry,
    RuntimeCapabilities,
};
use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;
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
    #[error("handoff must be quiescing before export can commit")]
    HandoffNotQuiescing,
    #[error("handoff must be accepted before the destination can resume")]
    HandoffNotAccepted,
    #[error("handoff identity, tenant, continuity id, or destination changed")]
    HandoffIdentityChanged,
    #[error("handoff version must increment by exactly one")]
    InvalidHandoffVersion,
    #[error("execution identity or expected ownership changed")]
    ExecutionIdentityChanged,
    #[error("accepted execution must increment the epoch and assign the destination owner")]
    InvalidOwnershipClaim,
    #[error("destination instance was not imported from this handoff capsule")]
    UnboundDestinationInstance,
    #[error("handoff or ownership compare-and-swap lost")]
    StaleClaim,
    #[error(transparent)]
    Storage(#[from] StorageError),
}

pub async fn commit_handoff_export(
    storage: &dyn StorageBackend,
    expected_handoff: &ExecutionHandoff,
    exported_handoff: &ExecutionHandoff,
    expected_execution: &ContinuityExecution,
    transferring_execution: &ContinuityExecution,
) -> Result<(), ContinuityServiceError> {
    if expected_handoff.state != HandoffState::Quiescing
        || exported_handoff.state != HandoffState::Exported
    {
        return Err(ContinuityServiceError::HandoffNotQuiescing);
    }
    if expected_handoff.id != exported_handoff.id
        || expected_handoff.tenant_id != exported_handoff.tenant_id
        || expected_handoff.continuity_id != exported_handoff.continuity_id
        || expected_handoff.destination_runtime_id != exported_handoff.destination_runtime_id
        || exported_handoff.capsule_id.is_none()
    {
        return Err(ContinuityServiceError::HandoffIdentityChanged);
    }
    if expected_handoff.version.checked_add(1) != Some(exported_handoff.version) {
        return Err(ContinuityServiceError::InvalidHandoffVersion);
    }
    if expected_execution.continuity_id != transferring_execution.continuity_id
        || expected_execution.tenant_id != transferring_execution.tenant_id
        || expected_execution.owner_runtime_id != transferring_execution.owner_runtime_id
        || expected_execution.current_instance_id != transferring_execution.current_instance_id
        || expected_execution.epoch != transferring_execution.epoch
        || expected_execution.state != orch8_types::continuity::OwnershipState::Owned
        || transferring_execution.state != orch8_types::continuity::OwnershipState::Transferring
    {
        return Err(ContinuityServiceError::ExecutionIdentityChanged);
    }
    let committed = storage
        .commit_handoff_export(
            &expected_handoff.tenant_id,
            expected_handoff,
            exported_handoff,
            expected_execution,
            transferring_execution,
        )
        .await?;
    if !committed {
        return Err(ContinuityServiceError::StaleClaim);
    }
    Ok(())
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
        || expected_execution.owner_runtime_id != expected_handoff.source_runtime_id
        || expected_execution.state != orch8_types::continuity::OwnershipState::Transferring
        || accepted_execution.state != orch8_types::continuity::OwnershipState::Owned
    {
        return Err(ContinuityServiceError::ExecutionIdentityChanged);
    }
    if expected_execution.epoch.checked_next().ok() != Some(accepted_execution.epoch)
        || accepted_execution.owner_runtime_id != expected_handoff.destination_runtime_id
    {
        return Err(ContinuityServiceError::InvalidOwnershipClaim);
    }
    let Some(capsule_id) = expected_handoff.capsule_id else {
        return Err(ContinuityServiceError::UnboundDestinationInstance);
    };
    if !storage
        .is_capsule_import_instance(
            &expected_handoff.tenant_id,
            capsule_id,
            expected_handoff.destination_runtime_id,
            accepted_execution.current_instance_id,
        )
        .await?
    {
        return Err(ContinuityServiceError::UnboundDestinationInstance);
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

pub async fn resume_handoff(
    storage: &dyn StorageBackend,
    expected_handoff: &ExecutionHandoff,
    resumed_handoff: &ExecutionHandoff,
    destination_instance_id: InstanceId,
) -> Result<(), ContinuityServiceError> {
    if expected_handoff.state != HandoffState::Accepted
        || resumed_handoff.state != HandoffState::Resumed
    {
        return Err(ContinuityServiceError::HandoffNotAccepted);
    }
    if expected_handoff.id != resumed_handoff.id
        || expected_handoff.tenant_id != resumed_handoff.tenant_id
        || expected_handoff.continuity_id != resumed_handoff.continuity_id
        || expected_handoff.destination_runtime_id != resumed_handoff.destination_runtime_id
    {
        return Err(ContinuityServiceError::HandoffIdentityChanged);
    }
    if expected_handoff.version.checked_add(1) != Some(resumed_handoff.version) {
        return Err(ContinuityServiceError::InvalidHandoffVersion);
    }
    let resumed = storage
        .resume_handoff(
            &expected_handoff.tenant_id,
            expected_handoff,
            resumed_handoff,
            destination_instance_id,
        )
        .await?;
    if !resumed {
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
    let entry_sha256 = provenance_hash(
        execution.continuity_id,
        execution.epoch,
        &kind,
        &payload_sha256,
        previous_sha256.as_deref(),
    );
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

#[must_use]
pub fn sign_provenance_entry(
    mut entry: ProvenanceEntry,
    signing_key_id: impl Into<String>,
    signing_key: &SigningKey,
) -> ProvenanceEntry {
    entry.signing_key_id = Some(signing_key_id.into());
    entry.signature =
        Some(BASE64.encode(signing_key.sign(entry.entry_sha256.as_bytes()).to_bytes()));
    entry
}

fn provenance_hash(
    continuity_id: orch8_types::continuity::ContinuityId,
    epoch: orch8_types::continuity::ExecutionEpoch,
    kind: &str,
    payload_sha256: &str,
    previous_sha256: Option<&str>,
) -> String {
    let mut hash = Sha256::new();
    hash.update(continuity_id.to_string());
    hash.update(epoch.get().to_be_bytes());
    hash.update(kind.as_bytes());
    hash.update(payload_sha256.as_bytes());
    if let Some(previous) = previous_sha256 {
        hash.update(previous.as_bytes());
    }
    let mut output = String::with_capacity(64);
    for byte in hash.finalize() {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProvenanceVerification {
    pub valid: bool,
    pub entries_checked: usize,
    pub first_invalid_index: Option<usize>,
    pub code: Option<&'static str>,
    pub head_sha256: Option<String>,
}

#[must_use]
pub fn verify_provenance_chain(
    entries: &[ProvenanceEntry],
    expected_head: Option<&str>,
) -> ProvenanceVerification {
    let mut previous: Option<&str> = None;
    for (index, entry) in entries.iter().enumerate() {
        if entry.previous_sha256.as_deref() != previous {
            return ProvenanceVerification {
                valid: false,
                entries_checked: index,
                first_invalid_index: Some(index),
                code: Some("PROVENANCE_LINK_MISMATCH"),
                head_sha256: previous.map(ToOwned::to_owned),
            };
        }
        let actual = provenance_hash(
            entry.continuity_id,
            entry.epoch,
            &entry.kind,
            &entry.payload_sha256,
            entry.previous_sha256.as_deref(),
        );
        if actual != entry.entry_sha256 {
            return ProvenanceVerification {
                valid: false,
                entries_checked: index,
                first_invalid_index: Some(index),
                code: Some("PROVENANCE_HASH_MISMATCH"),
                head_sha256: previous.map(ToOwned::to_owned),
            };
        }
        previous = Some(&entry.entry_sha256);
    }
    if expected_head.is_some() && expected_head != previous {
        return ProvenanceVerification {
            valid: false,
            entries_checked: entries.len(),
            first_invalid_index: None,
            code: Some("PROVENANCE_HEAD_MISMATCH"),
            head_sha256: previous.map(ToOwned::to_owned),
        };
    }
    ProvenanceVerification {
        valid: true,
        entries_checked: entries.len(),
        first_invalid_index: None,
        code: None,
        head_sha256: previous.map(ToOwned::to_owned),
    }
}

#[must_use]
pub fn verify_provenance_chain_with_keys(
    entries: &[ProvenanceEntry],
    expected_head: Option<&str>,
    trusted_keys: &std::collections::BTreeMap<String, String>,
) -> ProvenanceVerification {
    let chain = verify_provenance_chain(entries, expected_head);
    if !chain.valid {
        return chain;
    }
    for (index, entry) in entries.iter().enumerate() {
        let (Some(key_id), Some(signature)) = (&entry.signing_key_id, &entry.signature) else {
            if entry.signing_key_id.is_some() || entry.signature.is_some() {
                return invalid_signature_report(entries, index, "PROVENANCE_SIGNATURE_INCOMPLETE");
            }
            continue;
        };
        let Some(public_key) = trusted_keys.get(key_id) else {
            return invalid_signature_report(entries, index, "PROVENANCE_SIGNING_KEY_UNTRUSTED");
        };
        let key_bytes: [u8; 32] = match BASE64
            .decode(public_key)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
        {
            Some(bytes) => bytes,
            None => {
                return invalid_signature_report(entries, index, "PROVENANCE_PUBLIC_KEY_INVALID");
            }
        };
        let signature_bytes: [u8; 64] = match BASE64
            .decode(signature)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
        {
            Some(bytes) => bytes,
            None => {
                return invalid_signature_report(entries, index, "PROVENANCE_SIGNATURE_INVALID");
            }
        };
        let verified = VerifyingKey::from_bytes(&key_bytes).is_ok_and(|key| {
            key.verify(
                entry.entry_sha256.as_bytes(),
                &Signature::from_bytes(&signature_bytes),
            )
            .is_ok()
        });
        if !verified {
            return invalid_signature_report(entries, index, "PROVENANCE_SIGNATURE_INVALID");
        }
    }
    chain
}

fn invalid_signature_report(
    entries: &[ProvenanceEntry],
    index: usize,
    code: &'static str,
) -> ProvenanceVerification {
    ProvenanceVerification {
        valid: false,
        entries_checked: index,
        first_invalid_index: Some(index),
        code: Some(code),
        head_sha256: index
            .checked_sub(1)
            .and_then(|previous| entries.get(previous))
            .map(|entry| entry.entry_sha256.clone()),
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
                capsule_signing_public_key: None,
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

    #[test]
    fn provenance_verification_finds_the_first_tampered_boundary() {
        let execution = ContinuityExecution {
            continuity_id: orch8_types::continuity::ContinuityId::new(),
            tenant_id: orch8_types::ids::TenantId::new("tenant-a").unwrap(),
            current_instance_id: orch8_types::ids::InstanceId::new(),
            owner_runtime_id: RuntimeId::new(),
            epoch: orch8_types::continuity::ExecutionEpoch::initial(),
            state: orch8_types::continuity::OwnershipState::Owned,
            updated_at: Utc::now(),
        };
        let first = build_provenance_entry(&execution, "created", "a".repeat(64), None, Utc::now());
        let mut second = build_provenance_entry(
            &execution,
            "handoff",
            "b".repeat(64),
            Some(first.entry_sha256.clone()),
            Utc::now(),
        );
        let valid =
            verify_provenance_chain(&[first.clone(), second.clone()], Some(&second.entry_sha256));
        assert!(valid.valid);

        second.kind = "tampered".into();
        let invalid = verify_provenance_chain(&[first, second], None);
        assert_eq!(invalid.first_invalid_index, Some(1));
        assert_eq!(invalid.code, Some("PROVENANCE_HASH_MISMATCH"));
    }

    #[test]
    fn provenance_boundary_signature_requires_the_trusted_key() {
        let execution = ContinuityExecution {
            continuity_id: orch8_types::continuity::ContinuityId::new(),
            tenant_id: orch8_types::ids::TenantId::new("tenant-a").unwrap(),
            current_instance_id: orch8_types::ids::InstanceId::new(),
            owner_runtime_id: RuntimeId::new(),
            epoch: orch8_types::continuity::ExecutionEpoch::initial(),
            state: orch8_types::continuity::OwnershipState::Owned,
            updated_at: Utc::now(),
        };
        let key = SigningKey::from_bytes(&[9; 32]);
        let entry = sign_provenance_entry(
            build_provenance_entry(
                &execution,
                "handoff_accept",
                "a".repeat(64),
                None,
                Utc::now(),
            ),
            "key-1",
            &key,
        );
        let trusted = std::collections::BTreeMap::from([(
            "key-1".into(),
            BASE64.encode(key.verifying_key().to_bytes()),
        )]);
        assert!(
            verify_provenance_chain_with_keys(std::slice::from_ref(&entry), None, &trusted).valid
        );
        assert_eq!(
            verify_provenance_chain_with_keys(&[entry], None, &std::collections::BTreeMap::new())
                .code,
            Some("PROVENANCE_SIGNING_KEY_UNTRUSTED")
        );
    }
}
