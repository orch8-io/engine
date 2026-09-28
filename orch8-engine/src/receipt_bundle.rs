//! Signed effect-receipt bundles: portable **at-most-once dispatch evidence**.
//!
//! A bundle is JSON Lines:
//!
//! 1. a header (`kind = "orch8.effect_receipts.header"`) naming the claim,
//!    scope, signing key id and the engine's Ed25519 public key;
//! 2. one line per [`EffectReceipt`] (`kind = "orch8.effect_receipt"`);
//! 3. a trailer (`kind = "orch8.effect_receipts.signature"`) holding the
//!    record count, the SHA-256 of every preceding byte, and an Ed25519
//!    signature over `"orch8-effect-receipts-v1\n" + <sha256 hex>`.
//!
//! What a valid bundle proves: the engine holding that key recorded exactly
//! these receipts, unmodified, at `generated_at`. The ledger is designed for
//! at-most-once *dispatch* per attempt — a receipt left `unknown` means the
//! outcome was ambiguous and was not retried automatically under the same
//! attempt. It is **not** an exactly-once delivery claim about the remote
//! system; providers dedupe on the recorded idempotency key.
//!
//! Verifying with only the embedded key proves integrity, not authorship:
//! pin the engine's key (`GET /receipts/signing-key`) for authenticity.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer as _, SigningKey, Verifier as _, VerifyingKey};
use orch8_types::continuity::{EffectReceipt, EffectState};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const BUNDLE_FORMAT: &str = "orch8-effect-receipts/v1";
pub const CLAIM: &str = "at-most-once dispatch evidence";
pub const CLAIM_NOTE: &str = "Receipts record that each side-effecting attempt was dispatched at \
    most once by this engine. Ambiguous outcomes stay `unknown` and are never re-dispatched under \
    the same attempt. This is not an exactly-once delivery guarantee for the remote system.";
const SIGNING_DOMAIN: &str = "orch8-effect-receipts-v1\n";
const HEADER_KIND: &str = "orch8.effect_receipts.header";
const RECEIPT_KIND: &str = "orch8.effect_receipt";
const TRAILER_KIND: &str = "orch8.effect_receipts.signature";

/// What the bundle covers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BundleScope {
    Instance {
        instance_id: String,
    },
    Window {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        /// True when the instance scan hit its cap; the bundle is then a
        /// correct but partial view of the window.
        truncated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleHeader {
    pub kind: String,
    pub format: String,
    pub claim: String,
    pub claim_note: String,
    pub engine_version: String,
    pub generated_at: DateTime<Utc>,
    pub tenant_id: String,
    pub scope: BundleScope,
    pub signing_key_id: String,
    pub algorithm: String,
    pub public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReceiptLine {
    kind: String,
    receipt: EffectReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Trailer {
    kind: String,
    algorithm: String,
    records: u64,
    sha256: String,
    signature: String,
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Build a signed bundle. Receipts are sorted by (`created_at`, id) so the
/// same ledger always yields the same body.
pub fn build_bundle(
    tenant_id: &str,
    scope: BundleScope,
    mut receipts: Vec<EffectReceipt>,
    signing_key: &SigningKey,
    signing_key_id: &str,
    generated_at: DateTime<Utc>,
) -> Result<String, serde_json::Error> {
    receipts.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.id.to_string().cmp(&b.id.to_string()))
    });
    let header = BundleHeader {
        kind: HEADER_KIND.into(),
        format: BUNDLE_FORMAT.into(),
        claim: CLAIM.into(),
        claim_note: CLAIM_NOTE.into(),
        engine_version: env!("CARGO_PKG_VERSION").into(),
        generated_at,
        tenant_id: tenant_id.into(),
        scope,
        signing_key_id: signing_key_id.into(),
        algorithm: "ed25519".into(),
        public_key: BASE64.encode(signing_key.verifying_key().to_bytes()),
    };
    let mut body = serde_json::to_string(&header)?;
    body.push('\n');
    let records = receipts.len() as u64;
    for receipt in receipts {
        body.push_str(&serde_json::to_string(&ReceiptLine {
            kind: RECEIPT_KIND.into(),
            receipt,
        })?);
        body.push('\n');
    }
    let digest = hex(&Sha256::digest(body.as_bytes()));
    let signature = signing_key.sign(format!("{SIGNING_DOMAIN}{digest}").as_bytes());
    body.push_str(&serde_json::to_string(&Trailer {
        kind: TRAILER_KIND.into(),
        algorithm: "ed25519".into(),
        records,
        sha256: digest,
        signature: BASE64.encode(signature.to_bytes()),
    })?);
    body.push('\n');
    Ok(body)
}

/// Result of a successful verification.
#[derive(Debug, Clone, Serialize)]
pub struct VerifyReport {
    pub valid: bool,
    pub claim: String,
    pub tenant_id: String,
    pub signing_key_id: String,
    pub public_key: String,
    /// Whether the signing key matched a caller-pinned key.
    pub key_pinned: bool,
    pub generated_at: DateTime<Utc>,
    pub scope: BundleScope,
    pub records: u64,
    pub instances: usize,
    pub by_state: BTreeMap<String, u64>,
    /// `dispatched` + `unknown`: outcomes that need a verifier or operator.
    pub unresolved: u64,
    /// Attempts with more than one receipt (must be 0).
    pub duplicate_attempts: u64,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VerifyError {
    #[error("bundle is empty")]
    Empty,
    #[error("line {0} is not valid bundle JSON: {1}")]
    Malformed(usize, String),
    #[error("first line is not a receipts header")]
    MissingHeader,
    #[error("last line is not a signature trailer")]
    MissingTrailer,
    #[error("unsupported bundle format `{0}`")]
    Format(String),
    #[error("record count mismatch: trailer says {expected}, bundle has {actual}")]
    Count { expected: u64, actual: u64 },
    #[error("content digest mismatch: the bundle was modified after signing")]
    Digest,
    #[error("signature is invalid")]
    Signature,
    #[error("bundle was signed by key {actual}, not the pinned key")]
    UntrustedKey { actual: String },
    #[error("invalid key encoding: {0}")]
    Key(String),
    #[error("receipt tenant `{0}` does not match the bundle tenant")]
    TenantMismatch(String),
}

fn decode_key(b64: &str) -> Result<VerifyingKey, VerifyError> {
    let bytes = BASE64
        .decode(b64.trim())
        .map_err(|e| VerifyError::Key(e.to_string()))?;
    let bytes: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| VerifyError::Key("expected 32 raw bytes".into()))?;
    VerifyingKey::from_bytes(&bytes).map_err(|e| VerifyError::Key(e.to_string()))
}

/// Verify integrity and signature. `pinned_public_key` (base64 raw 32 bytes)
/// additionally checks authorship.
pub fn verify_bundle(
    text: &str,
    pinned_public_key: Option<&str>,
) -> Result<VerifyReport, VerifyError> {
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let lines: Vec<&str> = lines.into_iter().filter(|l| !l.trim().is_empty()).collect();
    let (trailer_line, signed) = lines.split_last().ok_or(VerifyError::Empty)?;
    let header_line = signed.first().ok_or(VerifyError::MissingHeader)?;

    let header: BundleHeader =
        serde_json::from_str(header_line).map_err(|e| VerifyError::Malformed(1, e.to_string()))?;
    if header.kind != HEADER_KIND {
        return Err(VerifyError::MissingHeader);
    }
    if header.format != BUNDLE_FORMAT {
        return Err(VerifyError::Format(header.format));
    }
    let trailer: Trailer = serde_json::from_str(trailer_line)
        .map_err(|e| VerifyError::Malformed(lines.len(), e.to_string()))?;
    if trailer.kind != TRAILER_KIND {
        return Err(VerifyError::MissingTrailer);
    }

    let mut signed_bytes = String::new();
    for line in signed {
        signed_bytes.push_str(line);
        if !line.ends_with('\n') {
            signed_bytes.push('\n');
        }
    }
    if hex(&Sha256::digest(signed_bytes.as_bytes())) != trailer.sha256 {
        return Err(VerifyError::Digest);
    }
    let key = decode_key(&header.public_key)?;
    let signature_bytes = BASE64
        .decode(trailer.signature.trim())
        .map_err(|_| VerifyError::Signature)?;
    let signature = Signature::from_slice(&signature_bytes).map_err(|_| VerifyError::Signature)?;
    key.verify(
        format!("{SIGNING_DOMAIN}{}", trailer.sha256).as_bytes(),
        &signature,
    )
    .map_err(|_| VerifyError::Signature)?;
    let key_pinned = if let Some(pinned) = pinned_public_key {
        if decode_key(pinned)? != key {
            return Err(VerifyError::UntrustedKey {
                actual: header.signing_key_id,
            });
        }
        true
    } else {
        false
    };

    let mut by_state: BTreeMap<String, u64> = BTreeMap::new();
    let mut attempts: BTreeMap<(String, String, u32), u64> = BTreeMap::new();
    let mut instances = BTreeSet::new();
    let mut unresolved = 0;
    for (index, line) in signed.iter().enumerate().skip(1) {
        let record: ReceiptLine = serde_json::from_str(line)
            .map_err(|e| VerifyError::Malformed(index + 1, e.to_string()))?;
        let receipt = record.receipt;
        if receipt.tenant_id.as_str() != header.tenant_id {
            return Err(VerifyError::TenantMismatch(receipt.tenant_id.to_string()));
        }
        let state = serde_json::to_value(receipt.state)
            .ok()
            .and_then(|v| v.as_str().map(ToOwned::to_owned))
            .unwrap_or_default();
        *by_state.entry(state).or_default() += 1;
        if matches!(
            receipt.state,
            EffectState::Dispatched | EffectState::Unknown
        ) {
            unresolved += 1;
        }
        instances.insert(receipt.instance_id.to_string());
        *attempts
            .entry((
                receipt.instance_id.to_string(),
                receipt.block_id.to_string(),
                receipt.attempt,
            ))
            .or_default() += 1;
    }
    let records = (signed.len() - 1) as u64;
    if records != trailer.records {
        return Err(VerifyError::Count {
            expected: trailer.records,
            actual: records,
        });
    }
    Ok(VerifyReport {
        valid: true,
        claim: header.claim,
        tenant_id: header.tenant_id,
        signing_key_id: header.signing_key_id,
        public_key: header.public_key,
        key_pinned,
        generated_at: header.generated_at,
        scope: header.scope,
        records,
        instances: instances.len(),
        by_state,
        unresolved,
        duplicate_attempts: attempts.values().filter(|n| **n > 1).map(|n| n - 1).sum(),
    })
}

/// Parse the header only (e.g. to display a bundle before verifying).
pub fn read_header(text: &str) -> Option<BundleHeader> {
    let first = text.lines().next()?;
    let value: Value = serde_json::from_str(first).ok()?;
    serde_json::from_value(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::continuity::{ContinuityId, EffectId, EffectKind, ExecutionEpoch};
    use orch8_types::ids::{BlockId, InstanceId, TenantId};

    fn receipt(state: EffectState, attempt: u32) -> EffectReceipt {
        let now = Utc::now();
        EffectReceipt {
            id: EffectId::new(),
            tenant_id: TenantId::new("acme").unwrap(),
            continuity_id: ContinuityId::new(),
            epoch: ExecutionEpoch::initial(),
            instance_id: InstanceId::new(),
            block_id: BlockId::new("charge"),
            kind: EffectKind::Worker,
            state,
            destination_fingerprint: "d".into(),
            idempotency_key: Some("k".into()),
            request_sha256: "r".into(),
            provider_receipt_id: Some("pr_1".into()),
            attempt,
            created_at: now,
            updated_at: now,
        }
    }

    fn key() -> SigningKey {
        SigningKey::from_bytes(&[7; 32])
    }

    fn bundle() -> String {
        build_bundle(
            "acme",
            BundleScope::Instance {
                instance_id: "i".into(),
            },
            vec![
                receipt(EffectState::Committed, 0),
                receipt(EffectState::Unknown, 1),
            ],
            &key(),
            "continuity-signing-test",
            Utc::now(),
        )
        .unwrap()
    }

    #[test]
    fn valid_bundle_verifies_and_summarizes() {
        let text = bundle();
        assert_eq!(text.lines().count(), 4);
        let pinned = BASE64.encode(key().verifying_key().to_bytes());
        let report = verify_bundle(&text, Some(&pinned)).unwrap();
        assert!(report.valid && report.key_pinned);
        assert_eq!(report.records, 2);
        assert_eq!(report.unresolved, 1);
        assert_eq!(report.by_state.get("committed"), Some(&1));
        assert_eq!(report.claim, CLAIM);
        assert!(!text.contains("exactly-once\""));
    }

    #[test]
    fn tampering_is_detected() {
        let text = bundle().replace("\"committed\"", "\"abandoned\"");
        assert_eq!(verify_bundle(&text, None).unwrap_err(), VerifyError::Digest);

        let mut lines: Vec<&str> = bundle().leak().lines().collect();
        lines.remove(1);
        let dropped = lines.join("\n");
        assert_eq!(
            verify_bundle(&dropped, None).unwrap_err(),
            VerifyError::Digest
        );
    }

    #[test]
    fn foreign_key_is_rejected_when_pinned() {
        let other = BASE64.encode(SigningKey::from_bytes(&[9; 32]).verifying_key().to_bytes());
        assert!(matches!(
            verify_bundle(&bundle(), Some(&other)),
            Err(VerifyError::UntrustedKey { .. })
        ));
    }

    #[test]
    fn resigned_with_another_key_fails_signature_unless_header_matches() {
        // Swap only the public key in the header: digest changes -> rejected.
        let text = bundle();
        let header = read_header(&text).unwrap();
        let forged = text.replace(
            &header.public_key,
            &BASE64.encode(SigningKey::from_bytes(&[9; 32]).verifying_key().to_bytes()),
        );
        assert_eq!(
            verify_bundle(&forged, None).unwrap_err(),
            VerifyError::Digest
        );
    }

    #[test]
    fn empty_ledger_still_produces_a_signed_bundle() {
        let text = build_bundle(
            "acme",
            BundleScope::Window {
                from: Utc::now(),
                to: Utc::now(),
                truncated: false,
            },
            Vec::new(),
            &key(),
            "k",
            Utc::now(),
        )
        .unwrap();
        let report = verify_bundle(&text, None).unwrap();
        assert_eq!(report.records, 0);
        assert!(!report.key_pinned);
    }
}
