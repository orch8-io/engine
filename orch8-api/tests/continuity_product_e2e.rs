//! Black-box HTTP coverage for the public Durable Agent Handoff Protocol surface.
//!
//! These tests deliberately use the real Axum listener, versioned routes,
//! middleware, JSON serialization, and server signing key. They complement the
//! domain unit tests instead of calling handlers directly.

use chrono::{Duration, Utc};
use orch8_api::test_harness::spawn_test_server;
use orch8_types::continuity::{ContinuityId, ExecutionEpoch, PolicyOutcome};
use orch8_types::continuity_product::{
    CURRENT_PROTOCOL, CommercialContinuityPlan, ConformanceCertificate, ConformanceCheck,
    ConformanceCheckResult, ExecutionReceipt, GatewayAdapter, GatewayManifest, PortableWorkOffer,
    RelayDeployment,
};
use orch8_types::ids::{InstanceId, TenantId};
use reqwest::StatusCode;

fn passing_results() -> Vec<ConformanceCheckResult> {
    ConformanceCheck::ALL
        .into_iter()
        .map(|check| ConformanceCheckResult {
            check,
            passed: true,
            evidence_sha256: "ab".repeat(32),
            duration_ms: 1,
            finding: None,
        })
        .collect()
}

fn sealed_receipt() -> ExecutionReceipt {
    let mut receipt = ExecutionReceipt {
        protocol: CURRENT_PROTOCOL,
        receipt_id: uuid::Uuid::now_v7(),
        tenant_id: TenantId::new("example").unwrap(),
        continuity_id: ContinuityId::new(),
        instance_id: InstanceId::new(),
        final_epoch: ExecutionEpoch::initial(),
        sequence_sha256: "cd".repeat(32),
        model_ids: vec!["local/model-v1".into()],
        tool_ids: vec!["private_rag.query".into()],
        locations: vec![],
        effects: vec![],
        policy_outcome: PolicyOutcome::Allow,
        consent_receipt_ids: vec![],
        previous_receipt_sha256: None,
        created_at: Utc::now(),
        digest_sha256: String::new(),
        signing_key_id: None,
        signature: None,
    };
    receipt.seal();
    receipt
}

#[tokio::test]
async fn discovery_profile_and_offer_flow_uses_the_versioned_api() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = server.v1_url();

    let protocol: serde_json::Value = client
        .get(format!("{base}/continuity/protocol"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(protocol["version"]["major"], 1);
    assert_eq!(protocol["transports"].as_array().unwrap().len(), 4);

    let profiles: Vec<serde_json::Value> = client
        .get(format!("{base}/continuity/profiles"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(profiles.len(), 10);
    assert!(
        profiles
            .iter()
            .all(|profile| profile["requires_signed_receipt"] == true)
    );

    let offer: PortableWorkOffer = client
        .post(format!(
            "{base}/continuity/profiles/executive_airlock/offers"
        ))
        .json(&serde_json::json!({
            "tenant_id": "example",
            "continuity_id": ContinuityId::new(),
            "expected_epoch": ExecutionEpoch::initial(),
            "input": {"action": "approve_release"},
            "idempotency_key": "release-42"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(offer.handler, "airlock.approve");
    assert!(offer.receipt_required);

    let validation: serde_json::Value = client
        .post(format!("{base}/continuity/offers/validate"))
        .json(&offer)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(validation, serde_json::json!({"valid": true}));
}

#[tokio::test]
async fn policy_gateway_and_receipt_contracts_round_trip_over_http() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = server.v1_url();
    let policy = "classification=restricted;runtime_kinds=desktop;min_trust=attested;handlers=coding.local;credentials=source_control;hardware=secure_enclave";

    let compiled: serde_json::Value = client
        .post(format!("{base}/continuity/policies/compile"))
        .json(&serde_json::json!({"source": policy}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(compiled["classification"], "restricted");
    assert_eq!(compiled["requirements"]["minimum_trust"], "attested");

    let manifest = GatewayManifest {
        protocol: CURRENT_PROTOCOL,
        name: "local-coding-agent".into(),
        adapter: GatewayAdapter::LocalProcess,
        entrypoint: "/usr/bin/env".into(),
        arguments: vec!["python3".into(), "worker.py".into()],
        handler: "coding.local".into(),
        policy_source: policy.into(),
        environment_allowlist: vec!["PATH".into()],
        secret_references: vec!["vault://source-control/token".into()],
        receipt_required: true,
    };
    let gateway: serde_json::Value = client
        .post(format!("{base}/continuity/gateways/validate"))
        .json(&manifest)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(gateway["source"], policy);

    let validation: serde_json::Value = client
        .post(format!("{base}/continuity/receipts/verify"))
        .json(&sealed_receipt())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(validation["valid"], true);
}

#[tokio::test]
async fn conformance_certificate_can_render_a_server_verified_badge() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = server.v1_url();
    let issued_at = Utc::now();

    let certificate: ConformanceCertificate = client
        .post(format!("{base}/continuity/conformance/certificates"))
        .json(&serde_json::json!({
            "subject": "example-sdk",
            "results": passing_results(),
            "issued_at": issued_at,
            "expires_at": issued_at + Duration::days(30)
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(certificate.score.score_millipoints, 1_000);
    assert!(certificate.signature.is_some());

    let badge = client
        .post(format!("{base}/continuity/conformance/badge"))
        .json(&certificate)
        .send()
        .await
        .unwrap();
    assert_eq!(badge.status(), StatusCode::OK);
    assert_eq!(
        badge.headers()[reqwest::header::CONTENT_TYPE],
        "image/svg+xml"
    );
    assert!(badge.text().await.unwrap().contains("certified 100%"));

    let mut tampered = certificate;
    tampered.subject = "attacker".into();
    let rejected = client
        .post(format!("{base}/continuity/conformance/badge"))
        .json(&tampered)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn commercial_and_validation_endpoints_fail_closed() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = server.v1_url();

    let plan = CommercialContinuityPlan {
        deployment: RelayDeployment::OemEmbedded,
        tenant_isolation: true,
        signed_receipts: true,
        conformance_required: true,
        evidence_retention_days: 30,
        allowed_protocol_majors: vec![CURRENT_PROTOCOL.major],
        oem_product_id: Some("com.example.agent".into()),
        usage_meter: Some("verified_handoff".into()),
    };
    let accepted = client
        .post(format!("{base}/continuity/commercial/validate"))
        .json(&plan)
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);

    let unknown_policy = client
        .post(format!("{base}/continuity/policies/compile"))
        .json(&serde_json::json!({"source": "classification=internal;teleport=true"}))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown_policy.status(), StatusCode::BAD_REQUEST);

    let mut tampered_receipt = sealed_receipt();
    tampered_receipt.tool_ids.push("injected.tool".into());
    let tampered = client
        .post(format!("{base}/continuity/receipts/verify"))
        .json(&tampered_receipt)
        .send()
        .await
        .unwrap();
    assert_eq!(tampered.status(), StatusCode::BAD_REQUEST);

    let unknown_profile = client
        .post(format!("{base}/continuity/profiles/not_real/offers"))
        .json(&serde_json::json!({
            "tenant_id": "example",
            "continuity_id": ContinuityId::new(),
            "expected_epoch": ExecutionEpoch::initial(),
            "input": {},
            "idempotency_key": "once"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown_profile.status(), StatusCode::BAD_REQUEST);
}
