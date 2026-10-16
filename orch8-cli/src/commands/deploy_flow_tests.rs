//! `orch8 deploy` verifies the signed package locally before any control-plane
//! request, then runs release gate → canary → N evaluations → (optional)
//! promote, and stops at the first failure: a non-2xx gate response, a
//! rolled-back/paused/auto-rolled-back evaluation, or any `Fail` gate verdict
//! means the release is never promoted.

use super::*;

use std::collections::BTreeMap;

use clap::Parser as _;
use orch8_publisher::package::{PackageManifest, PackageRequirements, build_package};
use reqwest::StatusCode;

use crate::commands::test_support::{RecordedRequest, mock_api, mock_api_with_responses};

fn signed_package() -> orch8_publisher::package::SignedPackage {
    let manifest = PackageManifest {
        name: "acme/billing".into(),
        version: "1.2.0".into(),
        description: "billing workflows".into(),
        publisher: "acme".into(),
        requirements: PackageRequirements::default(),
        created_at: chrono::DateTime::parse_from_rfc3339("2026-07-25T00:00:00Z")
            .unwrap()
            .to_utc(),
    };
    let files = BTreeMap::from([("sequences/billing.json".to_string(), "{}".to_string())]);
    build_package(
        manifest,
        files,
        &ed25519_dalek::SigningKey::from_bytes(&[1; 32]),
    )
    .unwrap()
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

fn ok(body: &Value) -> (StatusCode, String) {
    (StatusCode::OK, body.to_string())
}

/// Responses for `check_gate`: the release, then the concurrent diff and
/// preflight calls (one body satisfies both contracts in either order).
fn passing_release_gate() -> Vec<(StatusCode, String)> {
    let release = json!({"candidate_sequence_id": Uuid::new_v4(),
        "state": "ready", "validation_summary": {"divergences": [], "inconclusive": 0}});
    let proof = json!({"entries": [], "overall": "pass"});
    vec![ok(&release), ok(&proof), ok(&proof)]
}

fn healthy_evaluation() -> Value {
    json!({"release_state": "canary", "auto_rolled_back": false, "gates": []})
}

fn count(requests: &[RecordedRequest], suffix: &str) -> usize {
    requests
        .iter()
        .filter(|request| request.uri.ends_with(suffix))
        .count()
}

#[tokio::test]
async fn unverifiable_package_is_rejected_before_any_http_request() {
    let mut tampered = signed_package();
    tampered
        .archive
        .files
        .insert("sequences/evil.json".into(), "{}".into());
    let cases: [(&str, Option<Vec<u8>>, &str); 4] = [
        (
            "tampered content",
            Some(serde_json::to_vec(&tampered).unwrap()),
            "content hash mismatch",
        ),
        (
            "not json",
            Some(b"this is not json".to_vec()),
            "decode signed package",
        ),
        (
            "wrong shape",
            Some(br#"{"archive": 42}"#.to_vec()),
            "decode signed package",
        ),
        ("missing file", None, "read signed package"),
    ];
    for (case, bytes, expected) in cases {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("package.orch8pkg");
        if let Some(bytes) = bytes {
            std::fs::write(&path, bytes).unwrap();
        }
        let api = mock_api().await;

        let error = run(
            &Client::new(),
            &api.base,
            deploy_cmd(path),
            OutputFormat::Json,
        )
        .await
        .unwrap_err();

        assert!(format!("{error:#}").contains(expected), "{case}: {error:#}");
        assert!(api.log.snapshot().is_empty(), "{case}: contacted the API");
    }
}

#[tokio::test]
async fn deploy_obeys_evaluation_report_before_promotion() {
    for promote in [false, true] {
        for (evaluation, accepted) in [
            (healthy_evaluation(), true),
            (
                json!({"release_state": "rolled_back", "auto_rolled_back": true, "gates": []}),
                false,
            ),
            (
                json!({"release_state": "paused", "auto_rolled_back": false, "gates": []}),
                false,
            ),
            (
                json!({"release_state": "canary", "auto_rolled_back": true, "gates": []}),
                false,
            ),
            (json!({"state": "canary"}), false),
            (json!({}), false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut command = deploy_cmd(write_package(dir.path(), &signed_package()));
            command.promote = promote;
            let mut responses = passing_release_gate();
            responses.extend([ok(&json!({})), ok(&evaluation), ok(&json!({}))]);
            let api = mock_api_with_responses(responses).await;

            let result = run(&Client::new(), &api.base, command, OutputFormat::Json).await;

            assert_eq!(result.is_ok(), accepted, "{evaluation}: {result:?}");
            let requests = api.log.snapshot();
            assert_eq!(
                count(&requests, "/promote"),
                usize::from(accepted && promote),
                "{evaluation}"
            );
            assert_eq!(count(&requests, "/evaluate"), 1, "{evaluation}");
        }
    }
}

#[tokio::test]
async fn failing_gate_verdict_blocks_promotion() {
    let dir = tempfile::tempdir().unwrap();
    let mut command = deploy_cmd(write_package(dir.path(), &signed_package()));
    command.promote = true;
    let failing = json!({
        "release_state": "canary",
        "auto_rolled_back": false,
        "gates": [{
            "gate": {"metric": "error_rate", "max_regression": 0.05, "min_sample": 10},
            "baseline_rate": 0.01,
            "candidate_rate": 0.20,
            "verdict": "fail"
        }]
    });
    let mut responses = passing_release_gate();
    responses.extend([ok(&json!({})), ok(&failing), ok(&json!({}))]);
    let api = mock_api_with_responses(responses).await;

    let error = run(&Client::new(), &api.base, command, OutputFormat::Json)
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "canary evaluation failed; release was not promoted"
    );
    assert_eq!(count(&api.log.snapshot(), "/promote"), 0);
}

#[tokio::test]
async fn each_observation_is_one_evaluate_call_before_promotion() {
    let dir = tempfile::tempdir().unwrap();
    let mut command = deploy_cmd(write_package(dir.path(), &signed_package()));
    command.promote = true;
    command.observations = 3;
    let mut responses = passing_release_gate();
    responses.push(ok(&json!({})));
    responses.extend(std::iter::repeat_n(ok(&healthy_evaluation()), 3));
    responses.push(ok(&json!({})));
    let api = mock_api_with_responses(responses).await;

    run(&Client::new(), &api.base, command, OutputFormat::Json)
        .await
        .unwrap();

    let requests = api.log.snapshot();
    assert_eq!(count(&requests, "/evaluate"), 3);
    assert_eq!(count(&requests, "/promote"), 1);
    let promote = requests.last().unwrap();
    assert!(promote.uri.ends_with("/promote"), "{}", promote.uri);
    assert_eq!(promote.body, json!({"force": false}));
}

#[tokio::test]
async fn rejected_canary_request_aborts_before_evaluation() {
    let dir = tempfile::tempdir().unwrap();
    let mut command = deploy_cmd(write_package(dir.path(), &signed_package()));
    command.promote = true;
    command.canary_percent = 10;
    let mut responses = passing_release_gate();
    responses.push((
        StatusCode::CONFLICT,
        json!({"error": "release not ready"}).to_string(),
    ));
    let api = mock_api_with_responses(responses).await;

    let error = run(&Client::new(), &api.base, command, OutputFormat::Json)
        .await
        .unwrap_err();

    assert!(
        error.to_string().starts_with("deploy gate failed (409"),
        "{error:#}"
    );
    let requests = api.log.snapshot();
    let canary = requests
        .iter()
        .find(|request| request.uri.ends_with("/canary"))
        .expect("canary was requested");
    assert_eq!(canary.body, json!({"percent": 10}));
    assert_eq!(count(&requests, "/evaluate"), 0);
    assert_eq!(count(&requests, "/promote"), 0);
}

#[derive(clap::Parser)]
struct DeployArgs {
    #[command(flatten)]
    cmd: DeployCmd,
}

#[test]
fn canary_percent_is_bounded_to_1_through_50_and_observations_to_1_through_20() {
    let release = Uuid::now_v7().to_string();
    let cases = [
        ("--canary-percent", "0", false),
        ("--canary-percent", "1", true),
        ("--canary-percent", "50", true),
        ("--canary-percent", "51", false),
        ("--observations", "0", false),
        ("--observations", "20", true),
        ("--observations", "21", false),
    ];
    for (flag, value, accepted) in cases {
        let parsed = DeployArgs::try_parse_from([
            "orch8-deploy",
            "--package",
            "p",
            "--release-id",
            &release,
            flag,
            value,
        ]);
        assert_eq!(parsed.is_ok(), accepted, "{flag} {value}");
    }
}
