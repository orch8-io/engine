//! Signed workflow packages.
//!
//! A package bundles sequences, workflow contracts, and docs with a
//! manifest describing exactly what the package needs (handlers,
//! credentials, plugins). The archive is **deterministic** — files are
//! stored in a sorted map and serialized as canonical JSON — so the same
//! content always produces the same bytes, the same SHA-256 content
//! hash, and (for the same key) the same signature.
//!
//! Trust model: packages are Ed25519-signed by their publisher. A
//! consumer verifies integrity (hash) and authenticity (signature)
//! locally, then applies a trust policy: explicitly trusted publisher
//! keys install silently; anything else requires an explicit
//! untrusted-install opt-in. Packages never run install-time code.

use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::manifest::canonical_json;

/// Current package format version. Readers must reject unknown versions.
pub const PACKAGE_FORMAT_VERSION: u32 = 1;

/// Everything the package requires from the target environment. Declared
/// up front so preflight can prove readiness before anything is
/// installed — undeclared requirements are a support incident, not a
/// surprise.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageRequirements {
    /// External handlers workers must provide.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub handlers: Vec<String>,
    /// Credential ids referenced by the sequences.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub credentials: Vec<String>,
    /// Plugins that must be installed and enabled.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plugins: Vec<String>,
    /// Queues that need consumers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queues: Vec<String>,
    /// Minimum engine version (dotted numeric, e.g. "0.6.0").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_engine_version: Option<String>,
}

/// Package identity and contents description.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PackageManifest {
    /// Namespaced name: `publisher/package` (lowercase, `[a-z0-9-_/]`).
    pub name: String,
    /// Dotted numeric version (`1.2.0`). Installs enforce monotonicity.
    pub version: String,
    #[serde(default)]
    pub description: String,
    /// Publisher identity label (human-readable; trust comes from the
    /// signing key, never from this string).
    #[serde(default)]
    pub publisher: String,
    #[serde(default)]
    pub requirements: PackageRequirements,
    pub created_at: DateTime<Utc>,
}

/// The deterministic archive: manifest + sorted file map. File paths are
/// slash-separated and relative (`sequences/checkout.json`,
/// `contracts/checkout.contracts.json`, `README.md`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PackageArchive {
    pub format_version: u32,
    pub manifest: PackageManifest,
    pub files: BTreeMap<String, String>,
}

/// A signed package as written to disk (`.orch8pkg`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignedPackage {
    pub archive: PackageArchive,
    /// SHA-256 of the archive's canonical JSON, hex.
    pub content_hash: String,
    /// Ed25519 signature over the content hash bytes, base64.
    pub signature: String,
    /// Publisher's public key, base64.
    pub public_key: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PackageError {
    #[error("invalid package: {0}")]
    Invalid(String),
    #[error("unsupported package format version {0} (this tool supports {PACKAGE_FORMAT_VERSION})")]
    UnsupportedFormat(u32),
    #[error("content hash mismatch — the archive was modified after signing")]
    Tampered,
    #[error("signature verification failed — not signed by the embedded key")]
    BadSignature,
    #[error("publisher key is not trusted; pass the key explicitly or allow untrusted installs")]
    UntrustedPublisher,
    #[error("downgrade rejected: installed version {installed} >= incoming {incoming}")]
    Downgrade { installed: String, incoming: String },
}

/// Validate a package name: `publisher/package`, lowercase alphanumerics
/// plus `-`/`_`, both parts non-empty.
///
/// # Errors
/// Describes the violation.
pub fn validate_package_name(name: &str) -> Result<(), PackageError> {
    let mut parts = name.split('/');
    let (Some(publisher), Some(package), None) = (parts.next(), parts.next(), parts.next()) else {
        return Err(PackageError::Invalid(format!(
            "package name '{name}' must be 'publisher/package'"
        )));
    };
    for part in [publisher, package] {
        if part.is_empty()
            || !part
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
        {
            return Err(PackageError::Invalid(format!(
                "package name segment '{part}' must be non-empty lowercase [a-z0-9-_]"
            )));
        }
    }
    Ok(())
}

/// The tenant namespace a package installs into: `pkg.<publisher>.<name>`.
/// Installing under a dedicated namespace is what makes "never overwrite
/// local work" structural rather than a prompt.
#[must_use]
pub fn install_namespace(package_name: &str) -> String {
    format!("pkg.{}", package_name.replace('/', "."))
}

/// Compute the canonical content hash of an archive.
///
/// # Errors
/// Serialization failures only.
pub fn content_hash(archive: &PackageArchive) -> Result<String, PackageError> {
    let canonical = canonical_json(archive).map_err(|e| PackageError::Invalid(e.to_string()))?;
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}

/// Build and sign a package.
///
/// # Errors
/// Invalid name/version/files, or serialization failure.
pub fn build_package(
    manifest: PackageManifest,
    files: BTreeMap<String, String>,
    signing_key: &SigningKey,
) -> Result<SignedPackage, PackageError> {
    validate_package_name(&manifest.name)?;
    parse_version(&manifest.version)?;
    if files.is_empty() {
        return Err(PackageError::Invalid("package has no files".into()));
    }
    for path in files.keys() {
        // Reject anything that could escape the extraction root on any
        // platform: absolute paths, `..` segments, backslashes (a path
        // separator on Windows, so `..\evil` is traversal there), empty
        // segments, and NUL bytes.
        let invalid = path.is_empty()
            || path.starts_with('/')
            || path.contains(['\\', '\0'])
            || path.split('/').any(|seg| seg.is_empty() || seg == "..");
        if invalid {
            return Err(PackageError::Invalid(format!(
                "file path '{path}' must be relative without '..', '\\', NUL, or empty segments"
            )));
        }
    }
    let archive = PackageArchive {
        format_version: PACKAGE_FORMAT_VERSION,
        manifest,
        files,
    };
    let hash = content_hash(&archive)?;
    let signature = signing_key.sign(hash.as_bytes());
    Ok(SignedPackage {
        archive,
        content_hash: hash,
        signature: BASE64.encode(signature.to_bytes()),
        public_key: BASE64.encode(signing_key.verifying_key().to_bytes()),
    })
}

/// Verify integrity + authenticity of a package. Returns the archive on
/// success. This proves the archive matches its hash and the hash was
/// signed by the embedded key — trust in *that key* is a separate
/// decision (see [`check_trust`]).
///
/// # Errors
/// [`PackageError::Tampered`] / [`PackageError::BadSignature`] /
/// [`PackageError::UnsupportedFormat`].
pub fn verify_package(pkg: &SignedPackage) -> Result<(), PackageError> {
    if pkg.archive.format_version != PACKAGE_FORMAT_VERSION {
        return Err(PackageError::UnsupportedFormat(pkg.archive.format_version));
    }
    let actual = content_hash(&pkg.archive)?;
    if actual != pkg.content_hash {
        return Err(PackageError::Tampered);
    }
    let key_bytes: [u8; 32] = BASE64
        .decode(&pkg.public_key)
        .map_err(|e| PackageError::Invalid(format!("bad public key: {e}")))?
        .try_into()
        .map_err(|_| PackageError::Invalid("public key must be 32 bytes".into()))?;
    let key = VerifyingKey::from_bytes(&key_bytes)
        .map_err(|e| PackageError::Invalid(format!("bad public key: {e}")))?;
    let sig_bytes: [u8; 64] = BASE64
        .decode(&pkg.signature)
        .map_err(|e| PackageError::Invalid(format!("bad signature: {e}")))?
        .try_into()
        .map_err(|_| PackageError::Invalid("signature must be 64 bytes".into()))?;
    let signature = Signature::from_bytes(&sig_bytes);
    key.verify(pkg.content_hash.as_bytes(), &signature)
        .map_err(|_| PackageError::BadSignature)?;
    Ok(())
}

/// Trust policy for installs.
#[derive(Debug, Clone)]
pub struct TrustPolicy {
    /// Base64 public keys the operator trusts.
    pub trusted_keys: Vec<String>,
    /// Explicit opt-in for untrusted publishers.
    pub allow_untrusted: bool,
}

/// How the package's publisher was trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustLevel {
    Trusted,
    /// Explicitly allowed despite an unknown key.
    UntrustedAllowed,
}

/// Apply the trust policy to a (verified) package.
///
/// # Errors
/// [`PackageError::UntrustedPublisher`] when the key is unknown and
/// untrusted installs were not allowed.
pub fn check_trust(pkg: &SignedPackage, policy: &TrustPolicy) -> Result<TrustLevel, PackageError> {
    if policy.trusted_keys.iter().any(|k| k == &pkg.public_key) {
        return Ok(TrustLevel::Trusted);
    }
    if policy.allow_untrusted {
        return Ok(TrustLevel::UntrustedAllowed);
    }
    Err(PackageError::UntrustedPublisher)
}

/// Parse a dotted numeric version.
///
/// # Errors
/// Invalid version strings.
pub fn parse_version(version: &str) -> Result<Vec<u64>, PackageError> {
    let parts: Result<Vec<u64>, _> = version.split('.').map(str::parse::<u64>).collect();
    match parts {
        Ok(parts) if !parts.is_empty() => Ok(parts),
        _ => Err(PackageError::Invalid(format!(
            "version '{version}' must be dotted numeric (e.g. 1.2.0)"
        ))),
    }
}

/// Enforce monotonic upgrades: the incoming version must be strictly
/// greater than every already-installed version of the same package.
///
/// # Errors
/// [`PackageError::Downgrade`] when it is not.
pub fn check_upgrade(installed: &str, incoming: &str) -> Result<(), PackageError> {
    let a = parse_version(installed)?;
    let b = parse_version(incoming)?;
    if b > a {
        Ok(())
    } else {
        Err(PackageError::Downgrade {
            installed: installed.to_string(),
            incoming: incoming.to_string(),
        })
    }
}

/// Sequence files inside the archive (path, raw JSON). Package paths
/// are canonical lowercase (enforced by the deterministic build), so a
/// case-sensitive suffix check is exact, not an oversight.
#[must_use]
#[allow(clippy::case_sensitive_file_extension_comparisons)]
pub fn sequence_files(archive: &PackageArchive) -> Vec<(&String, &String)> {
    archive
        .files
        .iter()
        .filter(|(path, _)| path.starts_with("sequences/") && path.ends_with(".json"))
        .collect()
}

/// Contract files inside the archive (path, raw JSON).
#[must_use]
#[allow(clippy::case_sensitive_file_extension_comparisons)]
pub fn contract_files(archive: &PackageArchive) -> Vec<(&String, &String)> {
    archive
        .files
        .iter()
        .filter(|(path, _)| path.starts_with("contracts/") && path.ends_with(".contracts.json"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand_core::OsRng;

    fn key() -> SigningKey {
        SigningKey::generate(&mut OsRng)
    }

    fn manifest(name: &str, version: &str) -> PackageManifest {
        PackageManifest {
            name: name.into(),
            version: version.into(),
            description: "test package".into(),
            publisher: "Acme".into(),
            requirements: PackageRequirements {
                handlers: vec!["charge_card".into()],
                credentials: vec!["stripe_key".into()],
                ..Default::default()
            },
            created_at: chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 7, 1, 0, 0, 0).unwrap(),
        }
    }

    fn files() -> BTreeMap<String, String> {
        let mut f = BTreeMap::new();
        f.insert(
            "sequences/checkout.json".to_string(),
            r#"{"name":"checkout"}"#.to_string(),
        );
        f.insert(
            "contracts/checkout.contracts.json".to_string(),
            r#"{"cases":[]}"#.to_string(),
        );
        f.insert("README.md".to_string(), "# Checkout".to_string());
        f
    }

    #[test]
    fn build_verify_round_trip() {
        let pkg = build_package(manifest("acme/checkout", "1.0.0"), files(), &key()).unwrap();
        verify_package(&pkg).unwrap();
        assert_eq!(pkg.archive.manifest.name, "acme/checkout");
    }

    #[test]
    fn builds_are_reproducible_for_identical_content() {
        let k = key();
        let a = build_package(manifest("acme/checkout", "1.0.0"), files(), &k).unwrap();
        let b = build_package(manifest("acme/checkout", "1.0.0"), files(), &k).unwrap();
        assert_eq!(a.content_hash, b.content_hash);
        assert_eq!(a.signature, b.signature);
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap()
        );
    }

    #[test]
    fn different_content_changes_the_hash() {
        let k = key();
        let a = build_package(manifest("acme/checkout", "1.0.0"), files(), &k).unwrap();
        let mut f2 = files();
        f2.insert("README.md".into(), "# Tampered".into());
        let b = build_package(manifest("acme/checkout", "1.0.0"), f2, &k).unwrap();
        assert_ne!(a.content_hash, b.content_hash);
    }

    #[test]
    fn tampering_with_a_file_is_detected() {
        let mut pkg = build_package(manifest("acme/checkout", "1.0.0"), files(), &key()).unwrap();
        pkg.archive.files.insert(
            "sequences/checkout.json".into(),
            r#"{"name":"evil"}"#.into(),
        );
        assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
    }

    #[test]
    fn tampering_with_the_manifest_is_detected() {
        let mut pkg = build_package(manifest("acme/checkout", "1.0.0"), files(), &key()).unwrap();
        pkg.archive.manifest.requirements.credentials.clear(); // hide a requirement
        assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
    }

    #[test]
    fn forged_hash_fails_signature_check() {
        let mut pkg = build_package(manifest("acme/checkout", "1.0.0"), files(), &key()).unwrap();
        // Recompute a "valid" hash for tampered content but keep the old
        // signature: the signature check must catch it.
        pkg.archive
            .files
            .insert("README.md".into(), "# Evil".into());
        pkg.content_hash = content_hash(&pkg.archive).unwrap();
        assert_eq!(verify_package(&pkg), Err(PackageError::BadSignature));
    }

    #[test]
    fn signature_from_a_different_key_is_rejected() {
        let pkg = build_package(manifest("acme/checkout", "1.0.0"), files(), &key()).unwrap();
        let mut forged = pkg.clone();
        // Swap in another key's signature over the same hash.
        let other = key();
        forged.signature = BASE64.encode(other.sign(pkg.content_hash.as_bytes()).to_bytes());
        assert_eq!(verify_package(&forged), Err(PackageError::BadSignature));
    }

    #[test]
    fn unsupported_format_version_is_rejected() {
        let mut pkg = build_package(manifest("acme/checkout", "1.0.0"), files(), &key()).unwrap();
        pkg.archive.format_version = 99;
        assert_eq!(
            verify_package(&pkg),
            Err(PackageError::UnsupportedFormat(99))
        );
    }

    #[test]
    fn corrupted_encoding_is_invalid_not_panic() {
        let mut pkg = build_package(manifest("acme/checkout", "1.0.0"), files(), &key()).unwrap();
        pkg.public_key = "not base64!!!".into();
        assert!(matches!(
            verify_package(&pkg),
            Err(PackageError::Invalid(_))
        ));
        let mut pkg2 = build_package(manifest("acme/checkout", "1.0.0"), files(), &key()).unwrap();
        pkg2.signature = "AAAA".into(); // wrong length
        assert!(matches!(
            verify_package(&pkg2),
            Err(PackageError::Invalid(_))
        ));
    }

    #[test]
    fn trust_policy_gates_unknown_keys() {
        let pkg = build_package(manifest("acme/checkout", "1.0.0"), files(), &key()).unwrap();
        let strict = TrustPolicy {
            trusted_keys: vec![],
            allow_untrusted: false,
        };
        assert_eq!(
            check_trust(&pkg, &strict),
            Err(PackageError::UntrustedPublisher)
        );

        let trusting = TrustPolicy {
            trusted_keys: vec![pkg.public_key.clone()],
            allow_untrusted: false,
        };
        assert_eq!(check_trust(&pkg, &trusting), Ok(TrustLevel::Trusted));

        let permissive = TrustPolicy {
            trusted_keys: vec![],
            allow_untrusted: true,
        };
        assert_eq!(
            check_trust(&pkg, &permissive),
            Ok(TrustLevel::UntrustedAllowed)
        );
    }

    #[test]
    fn name_validation() {
        validate_package_name("acme/checkout").unwrap();
        validate_package_name("a-1/b_2").unwrap();
        for bad in ["checkout", "Acme/checkout", "acme/", "/x", "a/b/c", "a b/c"] {
            assert!(validate_package_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn install_namespace_is_stable_and_scoped() {
        assert_eq!(install_namespace("acme/checkout"), "pkg.acme.checkout");
    }

    #[test]
    fn upgrade_monotonicity() {
        check_upgrade("1.0.0", "1.0.1").unwrap();
        check_upgrade("1.9.0", "1.10.0").unwrap(); // numeric, not lexical
        assert!(matches!(
            check_upgrade("1.0.0", "1.0.0"),
            Err(PackageError::Downgrade { .. })
        ));
        assert!(matches!(
            check_upgrade("2.0.0", "1.9.9"),
            Err(PackageError::Downgrade { .. })
        ));
    }

    #[test]
    fn path_traversal_is_rejected() {
        let mut f = BTreeMap::new();
        f.insert("../etc/passwd".to_string(), "evil".to_string());
        assert!(build_package(manifest("acme/x", "1.0.0"), f, &key()).is_err());
        let mut f2 = BTreeMap::new();
        f2.insert("/abs/path".to_string(), "evil".to_string());
        assert!(build_package(manifest("acme/x", "1.0.0"), f2, &key()).is_err());
    }

    #[test]
    fn file_helpers_pick_the_right_entries() {
        let pkg = build_package(manifest("acme/checkout", "1.0.0"), files(), &key()).unwrap();
        let seqs = sequence_files(&pkg.archive);
        assert_eq!(seqs.len(), 1);
        assert_eq!(seqs[0].0, "sequences/checkout.json");
        let contracts = contract_files(&pkg.archive);
        assert_eq!(contracts.len(), 1);
    }
}
