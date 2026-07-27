//! Coverage tests for signed workflow package tooling.
//!
//! Count contract: 28 independently named unit tests.

use super::*;

use ed25519_dalek::SigningKey;

fn seed_bytes(seed: u8) -> [u8; 32] {
    [seed; 32]
}

fn seed_base64(seed: u8) -> String {
    BASE64.encode(seed_bytes(seed))
}

#[test]
fn coverage_package_001_valid_base64_seed_round_trips() {
    let key = load_signing_key(&seed_base64(7)).unwrap();
    assert_eq!(key.to_bytes(), seed_bytes(7));
}

#[test]
fn coverage_package_002_at_file_form_loads_seed_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("publisher.key");
    std::fs::write(&path, seed_base64(9)).unwrap();
    let key = load_signing_key(&format!("@{}", path.display())).unwrap();
    assert_eq!(key.to_bytes(), seed_bytes(9));
}

#[test]
fn coverage_package_003_at_file_form_trims_surrounding_whitespace() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("publisher.key");
    std::fs::write(&path, format!("  {}\n", seed_base64(11))).unwrap();
    let key = load_signing_key(&format!("@{}", path.display())).unwrap();
    assert_eq!(key.to_bytes(), seed_bytes(11));
}

#[test]
fn coverage_package_004_non_base64_seed_is_rejected() {
    let error = load_signing_key("not!base64!").unwrap_err();
    assert!(
        format!("{error:#}").contains("key must be base64"),
        "{error:#}"
    );
}

#[test]
fn coverage_package_005_wrong_length_seed_is_rejected() {
    let short = BASE64.encode([1u8; 16]);
    let error = load_signing_key(&short).unwrap_err();
    assert!(
        format!("{error:#}").contains("key seed must be exactly 32 bytes"),
        "{error:#}"
    );
}

#[test]
fn coverage_package_006_missing_key_file_is_a_read_error() {
    let error = load_signing_key("@/nonexistent/publisher.key").unwrap_err();
    assert!(
        format!("{error:#}").contains("reading key file"),
        "{error:#}"
    );
}

fn test_manifest(version: &str) -> PackageManifest {
    PackageManifest {
        name: "acme/billing".into(),
        version: version.into(),
        description: "billing workflows".into(),
        publisher: "acme".into(),
        requirements: PackageRequirements::default(),
        created_at: chrono::DateTime::parse_from_rfc3339("2026-07-25T00:00:00Z")
            .unwrap()
            .to_utc(),
    }
}

fn test_package(seed: u8) -> SignedPackage {
    build_package(
        test_manifest("1.2.0"),
        BTreeMap::from([("sequences/billing.json".to_string(), "{}".to_string())]),
        &SigningKey::from_bytes(&seed_bytes(seed)),
    )
    .unwrap()
}

#[test]
fn coverage_package_007_read_package_missing_file_errors() {
    let dir = tempfile::tempdir().unwrap();
    let error = read_package(&dir.path().join("absent.orch8pkg")).unwrap_err();
    assert!(format!("{error:#}").contains("reading"), "{error:#}");
}

#[test]
fn coverage_package_008_read_package_garbage_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.orch8pkg");
    std::fs::write(&path, b"{nope").unwrap();
    let error = read_package(&path).unwrap_err();
    assert!(
        format!("{error:#}").contains("file is not a signed orch8 package"),
        "{error:#}"
    );
}

#[test]
fn coverage_package_009_read_package_round_trips_a_signed_package() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ok.orch8pkg");
    let pkg = test_package(3);
    std::fs::write(&path, serde_json::to_string_pretty(&pkg).unwrap()).unwrap();
    let loaded = read_package(&path).unwrap();
    assert_eq!(loaded, pkg);
}

/// A minimal but complete sequence definition the builder accepts.
const VALID_SEQUENCE_JSON: &str = r#"{
  "id": "0191e4f2-a1b2-7c3d-8e4f-a5b6c7d8e9f0",
  "tenant_id": "demo",
  "namespace": "default",
  "name": "billing",
  "version": 1,
  "blocks": [
    { "type": "step", "id": "charge", "handler": "charge_card" }
  ],
  "created_at": "2026-07-25T00:00:00Z"
}"#;

fn package_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("package.json"),
        r#"{"name": "acme/billing", "version": "1.2.0", "description": "billing", "publisher": "acme"}"#,
    )
    .unwrap();
    dir
}

fn build_in(dir: &tempfile::TempDir) -> Result<PathBuf> {
    let out = dir.path().join("out.orch8pkg");
    build(dir.path(), &seed_base64(5), Some(&out)).map(|()| out)
}

#[test]
fn coverage_package_010_build_requires_package_json() {
    let dir = tempfile::tempdir().unwrap();
    let error = build_in(&dir).unwrap_err();
    assert!(format!("{error:#}").contains("package.json"), "{error:#}");
}

#[test]
fn coverage_package_011_build_rejects_invalid_manifest_json() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("package.json"), "{not json").unwrap();
    let error = build_in(&dir).unwrap_err();
    assert!(
        format!("{error:#}").contains("package.json is not valid JSON"),
        "{error:#}"
    );
}

#[test]
fn coverage_package_012_build_requires_a_manifest_name() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("package.json"), r#"{"version": "1.0.0"}"#).unwrap();
    let error = build_in(&dir).unwrap_err();
    assert!(
        format!("{error:#}").contains("'name' is required"),
        "{error:#}"
    );
}

#[test]
fn coverage_package_013_build_requires_a_manifest_version() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("package.json"),
        r#"{"name": "acme/billing"}"#,
    )
    .unwrap();
    let error = build_in(&dir).unwrap_err();
    assert!(
        format!("{error:#}").contains("'version' is required"),
        "{error:#}"
    );
}

#[test]
fn coverage_package_014_build_rejects_broken_sequence_json() {
    let dir = package_dir();
    let sequences = dir.path().join("sequences");
    std::fs::create_dir_all(&sequences).unwrap();
    std::fs::write(sequences.join("billing.json"), "{broken").unwrap();
    let error = build_in(&dir).unwrap_err();
    assert!(
        format!("{error:#}").contains("is not a valid sequence definition"),
        "{error:#}"
    );
}

#[test]
fn coverage_package_015_build_rejects_invalid_contract_suite() {
    let dir = package_dir();
    let contracts = dir.path().join("contracts");
    std::fs::create_dir_all(&contracts).unwrap();
    std::fs::write(contracts.join("billing.contracts.json"), "{broken").unwrap();
    let error = build_in(&dir).unwrap_err();
    assert!(
        format!("{error:#}").contains("is not a valid contract suite"),
        "{error:#}"
    );
}

#[test]
fn coverage_package_016_build_writes_a_verifiable_package() {
    let dir = package_dir();
    let sequences = dir.path().join("sequences");
    std::fs::create_dir_all(&sequences).unwrap();
    std::fs::write(sequences.join("billing.json"), VALID_SEQUENCE_JSON).unwrap();
    let out = build_in(&dir).unwrap();
    assert!(out.exists());
    verify(&out, &[]).unwrap();
}

#[test]
fn coverage_package_017_built_package_carries_manifest_and_files() {
    let dir = package_dir();
    let sequences = dir.path().join("sequences");
    std::fs::create_dir_all(&sequences).unwrap();
    std::fs::write(sequences.join("billing.json"), VALID_SEQUENCE_JSON).unwrap();
    std::fs::write(dir.path().join("README.md"), "# Billing").unwrap();
    let out = build_in(&dir).unwrap();
    let pkg = read_package(&out).unwrap();
    assert_eq!(pkg.archive.manifest.name, "acme/billing");
    assert_eq!(pkg.archive.manifest.version, "1.2.0");
    assert!(pkg.archive.files.contains_key("sequences/billing.json"));
    assert!(pkg.archive.files.contains_key("README.md"));
    verify_package(&pkg).unwrap();
}

#[test]
fn coverage_package_018_verify_accepts_a_trusted_publisher_key() {
    let dir = package_dir();
    let sequences = dir.path().join("sequences");
    std::fs::create_dir_all(&sequences).unwrap();
    std::fs::write(sequences.join("billing.json"), VALID_SEQUENCE_JSON).unwrap();
    let out = build_in(&dir).unwrap();
    let pkg = read_package(&out).unwrap();
    verify(&out, std::slice::from_ref(&pkg.public_key)).unwrap();
}

#[test]
fn coverage_package_019_install_namespace_is_pkg_dotted() {
    assert_eq!(install_namespace("acme/billing"), "pkg.acme.billing");
    assert_eq!(install_namespace("a/b_c-d"), "pkg.a.b_c-d");
}

#[test]
fn coverage_package_020_upgrade_allows_a_higher_version() {
    assert!(check_upgrade("1.2.0", "1.3.0").is_ok());
    assert!(check_upgrade("1.2.0", "2.0").is_ok());
}

#[test]
fn coverage_package_021_upgrade_rejects_the_same_version() {
    let error = check_upgrade("1.2.0", "1.2.0").unwrap_err();
    assert!(
        matches!(
            error,
            orch8_publisher::package::PackageError::Downgrade { .. }
        ),
        "{error}"
    );
}

#[test]
fn coverage_package_022_upgrade_rejects_a_lower_version() {
    assert!(check_upgrade("2.0.0", "1.9.9").is_err());
}

#[test]
fn coverage_package_023_upgrade_rejects_unparseable_versions() {
    assert!(check_upgrade("1.2.0", "latest").is_err());
    assert!(check_upgrade("", "1.0.0").is_err());
}

#[test]
fn coverage_package_024_trusted_key_yields_trusted_level() {
    let pkg = test_package(3);
    let policy = TrustPolicy {
        trusted_keys: vec![pkg.public_key.clone()],
        allow_untrusted: false,
    };
    assert_eq!(check_trust(&pkg, &policy).unwrap(), TrustLevel::Trusted);
}

#[test]
fn coverage_package_025_unknown_key_without_opt_in_is_rejected() {
    let pkg = test_package(3);
    let policy = TrustPolicy {
        trusted_keys: vec![BASE64.encode(seed_bytes(99))],
        allow_untrusted: false,
    };
    assert!(check_trust(&pkg, &policy).is_err());
}

#[test]
fn coverage_package_026_unknown_key_with_opt_in_is_untrusted_allowed() {
    let pkg = test_package(3);
    let policy = TrustPolicy {
        trusted_keys: vec![],
        allow_untrusted: true,
    };
    assert_eq!(
        check_trust(&pkg, &policy).unwrap(),
        TrustLevel::UntrustedAllowed
    );
}

#[test]
fn coverage_package_027_file_filters_split_sequences_and_contracts() {
    let archive = orch8_publisher::package::PackageArchive {
        format_version: 1,
        manifest: test_manifest("1.0.0"),
        files: BTreeMap::from([
            ("sequences/a.json".to_string(), "{}".to_string()),
            ("sequences/b.txt".to_string(), "x".to_string()),
            ("contracts/a.contracts.json".to_string(), "{}".to_string()),
            ("contracts/c.json".to_string(), "{}".to_string()),
            ("README.md".to_string(), "# docs".to_string()),
        ]),
    };
    let sequences: Vec<&str> = sequence_files(&archive)
        .iter()
        .map(|(path, _)| path.as_str())
        .collect();
    assert_eq!(sequences, ["sequences/a.json"]);
    let contracts: Vec<&str> = contract_files(&archive)
        .iter()
        .map(|(path, _)| path.as_str())
        .collect();
    assert_eq!(contracts, ["contracts/a.contracts.json"]);
}

#[test]
fn coverage_package_028_upgrade_comparison_is_numeric_not_lexical() {
    // Lexically "1.10.0" < "1.9.0"; numerically it is the newer version.
    assert!(check_upgrade("1.9.0", "1.10.0").is_ok());
    let error = check_upgrade("1.10.0", "1.9.0").unwrap_err();
    assert!(
        matches!(
            error,
            orch8_publisher::package::PackageError::Downgrade { .. }
        ),
        "{error}"
    );
}
