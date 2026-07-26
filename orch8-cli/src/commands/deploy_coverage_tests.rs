//! Coverage tests for golden-path signed package deployment.
//!
//! Count contract: 24 independently named unit tests.

use super::*;

use std::collections::BTreeMap;

use clap::Parser as _;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ed25519_dalek::{Signer as _, SigningKey};
use orch8_publisher::package::{PackageManifest, PackageRequirements, build_package};

fn signing_key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn manifest(version: &str) -> PackageManifest {
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

fn sample_files() -> BTreeMap<String, String> {
    BTreeMap::from([("sequences/billing.json".to_string(), "{}".to_string())])
}

fn signed_package(seed: u8) -> orch8_publisher::package::SignedPackage {
    build_package(manifest("1.2.0"), sample_files(), &signing_key(seed)).unwrap()
}

fn write_package(dir: &std::path::Path, pkg: &orch8_publisher::package::SignedPackage) -> PathBuf {
    let path = dir.join("package.orch8pkg");
    std::fs::write(&path, serde_json::to_vec_pretty(pkg).unwrap()).unwrap();
    path
}

fn deploy_cmd(package: PathBuf) -> DeployCmd {
    DeployCmd {
        package,
        release_id: Uuid::now_v7(),
        canary_percent: 5,
        observations: 1,
        promote: false,
    }
}

/// An unroutable base URL: any HTTP attempt fails fast with a connect
/// error, which is how we prove a package passed local verification and
/// the command moved on to the control plane.
const DEAD_BASE: &str = "http://127.0.0.1:1/api/v1";

#[tokio::test]
async fn coverage_deploy_001_valid_package_passes_verification_before_network() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_package(dir.path(), &signed_package(1));
    let error = run(
        &Client::new(),
        DEAD_BASE,
        deploy_cmd(path),
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    let rendered = format!("{error:#}");
    assert!(
        !rendered.contains("signed package verification failed"),
        "a valid package must not fail verification: {rendered}"
    );
    assert!(
        !rendered.contains("decode signed package"),
        "a valid package must decode: {rendered}"
    );
}

#[tokio::test]
async fn coverage_deploy_002_tampered_file_content_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut pkg = signed_package(1);
    pkg.archive
        .files
        .insert("sequences/evil.json".into(), "{}".into());
    let path = write_package(dir.path(), &pkg);
    let error = run(
        &Client::new(),
        DEAD_BASE,
        deploy_cmd(path),
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("content hash mismatch"),
        "{error:#}"
    );
}

#[tokio::test]
async fn coverage_deploy_003_tampered_manifest_version_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut pkg = signed_package(1);
    pkg.archive.manifest.version = "9.9.9".into();
    let path = write_package(dir.path(), &pkg);
    let error = run(
        &Client::new(),
        DEAD_BASE,
        deploy_cmd(path),
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("content hash mismatch"),
        "{error:#}"
    );
}

#[tokio::test]
async fn coverage_deploy_004_unknown_format_version_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut pkg = signed_package(1);
    pkg.archive.format_version = 99;
    let path = write_package(dir.path(), &pkg);
    let error = run(
        &Client::new(),
        DEAD_BASE,
        deploy_cmd(path),
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("unsupported package format version 99"),
        "{error:#}"
    );
}

#[tokio::test]
async fn coverage_deploy_005_signature_from_wrong_key_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut pkg = signed_package(1);
    // Re-sign the genuine content hash with an attacker's key while the
    // embedded public key stays the publisher's — verification must fail.
    let attacker = signing_key(2);
    pkg.signature = BASE64.encode(attacker.sign(pkg.content_hash.as_bytes()).to_bytes());
    let path = write_package(dir.path(), &pkg);
    let error = run(
        &Client::new(),
        DEAD_BASE,
        deploy_cmd(path),
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("signature verification failed"),
        "{error:#}"
    );
}

#[tokio::test]
async fn coverage_deploy_006_forged_content_hash_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut pkg = signed_package(1);
    pkg.content_hash = "0".repeat(64);
    let path = write_package(dir.path(), &pkg);
    let error = run(
        &Client::new(),
        DEAD_BASE,
        deploy_cmd(path),
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("content hash mismatch"),
        "{error:#}"
    );
}

#[tokio::test]
async fn coverage_deploy_007_garbage_file_is_not_a_package() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("package.orch8pkg");
    std::fs::write(&path, b"this is not json").unwrap();
    let error = run(
        &Client::new(),
        DEAD_BASE,
        deploy_cmd(path),
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("decode signed package"),
        "{error:#}"
    );
}

#[tokio::test]
async fn coverage_deploy_008_missing_package_file_is_a_read_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("absent.orch8pkg");
    let error = run(
        &Client::new(),
        DEAD_BASE,
        deploy_cmd(path),
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("read signed package"),
        "{error:#}"
    );
}

#[tokio::test]
async fn coverage_deploy_009_wrongly_shaped_json_is_not_a_package() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("package.orch8pkg");
    std::fs::write(&path, br#"{"archive": 42}"#).unwrap();
    let error = run(
        &Client::new(),
        DEAD_BASE,
        deploy_cmd(path),
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("decode signed package"),
        "{error:#}"
    );
}

#[tokio::test]
async fn coverage_deploy_010_truncated_package_json_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = serde_json::to_vec_pretty(&signed_package(1)).unwrap();
    let path = dir.path().join("package.orch8pkg");
    std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
    let error = run(
        &Client::new(),
        DEAD_BASE,
        deploy_cmd(path),
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("decode signed package"),
        "{error:#}"
    );
}

fn evidence(promote: bool) -> DeployEvidence {
    DeployEvidence {
        release_id: Uuid::nil(),
        package_hash: "abc123".into(),
        package_status: "verified",
        semantic_diff_status: "checked",
        historical_validation_status: "passed",
        canary_percent: 5,
        evaluations: 2,
        promotion_status: if promote { "promoted" } else { "canary" },
    }
}

#[test]
fn coverage_deploy_011_evidence_serializes_stable_field_names() {
    let value = serde_json::to_value(evidence(false)).unwrap();
    for key in [
        "release_id",
        "package_hash",
        "package_status",
        "semantic_diff_status",
        "historical_validation_status",
        "canary_percent",
        "evaluations",
        "promotion_status",
    ] {
        assert!(value.get(key).is_some(), "evidence is missing {key}");
    }
}

#[test]
fn coverage_deploy_012_evidence_records_nil_release_id_verbatim() {
    let value = serde_json::to_value(evidence(false)).unwrap();
    assert_eq!(
        value["release_id"],
        json!("00000000-0000-0000-0000-000000000000")
    );
    assert_eq!(value["package_hash"], json!("abc123"));
}

#[test]
fn coverage_deploy_013_evidence_marks_promoted_release() {
    let value = serde_json::to_value(evidence(true)).unwrap();
    assert_eq!(value["promotion_status"], json!("promoted"));
}

#[test]
fn coverage_deploy_014_evidence_marks_canary_only_release() {
    let value = serde_json::to_value(evidence(false)).unwrap();
    assert_eq!(value["promotion_status"], json!("canary"));
}

#[test]
fn coverage_deploy_015_evidence_fixed_status_strings_are_stable() {
    let value = serde_json::to_value(evidence(false)).unwrap();
    assert_eq!(value["package_status"], json!("verified"));
    assert_eq!(value["semantic_diff_status"], json!("checked"));
    assert_eq!(value["historical_validation_status"], json!("passed"));
}

#[test]
fn coverage_deploy_016_evidence_records_canary_and_evaluation_counts() {
    let value = serde_json::to_value(evidence(false)).unwrap();
    assert_eq!(value["canary_percent"], json!(5));
    assert_eq!(value["evaluations"], json!(2));
}

#[derive(clap::Parser)]
struct DeployArgs {
    #[command(flatten)]
    cmd: DeployCmd,
}

fn parse(args: &[&str]) -> Result<DeployCmd, clap::Error> {
    DeployArgs::try_parse_from(args).map(|parsed| parsed.cmd)
}

fn release_arg() -> String {
    Uuid::now_v7().to_string()
}

#[test]
fn coverage_deploy_017_defaults_are_canary_5_and_one_observation() {
    let release = release_arg();
    let cmd = parse(&[
        "orch8-deploy",
        "--package",
        "pkg.orch8pkg",
        "--release-id",
        &release,
    ])
    .unwrap();
    assert_eq!(cmd.canary_percent, 5);
    assert_eq!(cmd.observations, 1);
    assert!(!cmd.promote);
}

#[test]
fn coverage_deploy_018_canary_percent_zero_is_rejected() {
    let release = release_arg();
    assert!(
        parse(&[
            "orch8-deploy",
            "--package",
            "p",
            "--release-id",
            &release,
            "--canary-percent",
            "0",
        ])
        .is_err()
    );
}

#[test]
fn coverage_deploy_019_canary_percent_above_50_is_rejected() {
    let release = release_arg();
    assert!(
        parse(&[
            "orch8-deploy",
            "--package",
            "p",
            "--release-id",
            &release,
            "--canary-percent",
            "51",
        ])
        .is_err()
    );
}

#[test]
fn coverage_deploy_020_canary_percent_50_is_accepted() {
    let release = release_arg();
    let cmd = parse(&[
        "orch8-deploy",
        "--package",
        "p",
        "--release-id",
        &release,
        "--canary-percent",
        "50",
    ])
    .unwrap();
    assert_eq!(cmd.canary_percent, 50);
}

#[test]
fn coverage_deploy_021_observations_bounds_are_enforced() {
    let release = release_arg();
    for bad in ["0", "21"] {
        assert!(
            parse(&[
                "orch8-deploy",
                "--package",
                "p",
                "--release-id",
                &release,
                "--observations",
                bad,
            ])
            .is_err(),
            "--observations {bad} must be rejected"
        );
    }
    let cmd = parse(&[
        "orch8-deploy",
        "--package",
        "p",
        "--release-id",
        &release,
        "--observations",
        "20",
    ])
    .unwrap();
    assert_eq!(cmd.observations, 20);
}

#[test]
fn coverage_deploy_022_package_flag_is_required() {
    let release = release_arg();
    assert!(parse(&["orch8-deploy", "--release-id", &release]).is_err());
}

#[test]
fn coverage_deploy_023_release_id_must_be_a_uuid() {
    assert!(parse(&["orch8-deploy", "--package", "p"]).is_err());
    assert!(
        parse(&[
            "orch8-deploy",
            "--package",
            "p",
            "--release-id",
            "not-a-uuid"
        ])
        .is_err()
    );
}

#[test]
fn coverage_deploy_024_canary_percent_one_is_accepted() {
    let release = release_arg();
    let cmd = parse(&[
        "orch8-deploy",
        "--package",
        "p",
        "--release-id",
        &release,
        "--canary-percent",
        "1",
    ])
    .unwrap();
    assert_eq!(cmd.canary_percent, 1);
}
