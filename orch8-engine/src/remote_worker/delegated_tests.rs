use super::*;
use orch8_types::delegation_definition::{
    DefinitionGraphLimits,
    claim::DelegationClaimContext,
    input::{EncodedDelegationInput, InputLimits},
    protocol::{DelegationProtocolCapabilities, DelegationResourceUsage},
    wire::DefinitionWireEnvelope,
};
use serde_json::json;

fn task() -> RemoteTask {
    let vector: Value = serde_json::from_str(include_str!(
        "../../../contracts/vectors/delegation-definition-v1.json"
    ))
    .unwrap();
    let mut envelope: DefinitionWireEnvelope =
        serde_json::from_value(vector["envelope"].clone()).unwrap();
    envelope.scope.worker_id = "worker".into();
    envelope.scope.claim_epoch = 9_007_199_254_740_993;
    let graph = envelope
        .verify(
            &envelope.scope,
            &envelope.reference,
            DefinitionGraphLimits::default(),
        )
        .unwrap();
    let input = EncodedDelegationInput::from_value(&json!(null), InputLimits::default()).unwrap();
    let context = DelegationClaimContext {
        version: 1,
        scope: envelope.scope.clone(),
        request_sha256: "a".repeat(64),
        expires_at: chrono::Utc::now() + chrono::Duration::minutes(1),
        definition: envelope.reference.clone(),
        input: input.reference().clone(),
        usage: DelegationResourceUsage {
            definition_expanded_bytes: graph.expanded_bytes(),
            input_wire_version: 1,
            input_payload_bytes: 4,
            input_expanded_bytes: 64,
        },
    };
    serde_json::from_value(json!({"id":context.scope.task_id,"instance_id":uuid::Uuid::new_v4(),"block_id":"mailbox","handler_name":"orch8.delegation","params":{},"context":{},"claim_epoch":context.scope.claim_epoch,"delegation":context})).unwrap()
}
#[test]
fn issued_context_survives_task_parsing_and_rejects_identity_or_payload_shadowing() {
    let original = task();
    original.validate_delegation("worker").unwrap();
    assert_eq!(
        original.delegation.as_ref().unwrap().scope.claim_epoch,
        9_007_199_254_740_993
    );
    let mut variants = Vec::new();
    let mut v = original.clone();
    v.id = uuid::Uuid::new_v4();
    variants.push(v);
    let mut v = original.clone();
    v.claim_epoch += 1;
    variants.push(v);
    let mut v = original.clone();
    v.handler_name = "scan".into();
    variants.push(v);
    let mut v = original.clone();
    v.params = json!({"input":"PRIVATE_SHADOW"});
    variants.push(v);
    let mut v = original.clone();
    v.context = json!({"private":"PRIVATE_SHADOW"});
    variants.push(v);
    let mut v = original.clone();
    v.delegation = None;
    variants.push(v);
    for value in variants {
        assert!(value.validate_delegation("worker").is_err());
    }
    assert!(original.validate_delegation("other").is_err());
}

#[test]
fn issued_mailbox_attempt_is_fixed_while_claim_epochs_can_advance() {
    let original = task();
    assert_eq!(original.attempt, 0);
    original.validate_delegation("worker").unwrap();
    for attempt in [1, 2, u32::MAX] {
        let mut changed = original.clone();
        changed.attempt = attempt;
        assert!(
            changed.validate_delegation("worker").is_err(),
            "nonzero mailbox business attempt was accepted"
        );
    }
    let mut renewed = original.clone();
    renewed.claim_epoch += 1;
    renewed.delegation.as_mut().unwrap().scope.claim_epoch = renewed.claim_epoch;
    renewed.validate_delegation("worker").unwrap();
    let mut ordinary = original;
    ordinary.handler_name = "scan".into();
    ordinary.delegation = None;
    ordinary.attempt = 2;
    ordinary.validate_delegation("worker").unwrap();
}
#[test]
fn remote_task_debug_omits_all_opaque_payloads() {
    let mut value = task();
    value.params = json!({"private":"PRIVATE_PARAMS"});
    value.context = json!({"private":"PRIVATE_CONTEXT"});
    value.resume_checkpoint = Some(json!({"private":"PRIVATE_CHECKPOINT"}));
    let debug = format!("{value:?}");
    for marker in ["PRIVATE_PARAMS", "PRIVATE_CONTEXT", "PRIVATE_CHECKPOINT"] {
        assert!(!debug.contains(marker));
    }
}
async fn poll(body: Value, supported: bool) -> Result<PollResponse, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/api/v1", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let app =
            axum::Router::new().fallback(move || std::future::ready(axum::Json(body.clone())));
        axum::serve(listener, app).await.unwrap();
    });
    let client = HttpLeaseClient::new(reqwest::Client::new(), &base, Vec::new(), "worker".into());
    let now = chrono::Utc::now();
    let mut capabilities:RuntimeCapabilities=serde_json::from_value(json!({"runtime_id":uuid::Uuid::new_v4(),"kind":"server","trust":"registered","handlers":["orch8.delegation"],"observed_at":now,"expires_at":now+chrono::Duration::minutes(1)})).unwrap();
    if supported {
        capabilities.delegation_protocol = Some(
            DelegationProtocolCapabilities::v1(
                DefinitionGraphLimits::default(),
                4 * 1024 * 1024,
                16 * 1024 * 1024,
            )
            .unwrap(),
        );
    }
    let result = client
        .poll("orch8.delegation", 1, None, &capabilities)
        .await;
    server.abort();
    result
}
fn wire() -> Value {
    let task = task();
    json!({"tasks":[{"id":task.id,"instance_id":task.instance_id,"block_id":task.block_id,"handler_name":task.handler_name,"params":{},"context":{},"claim_epoch":task.claim_epoch,"delegation":task.delegation}]})
}
#[tokio::test]
async fn poll_requires_explicit_supported_decoder_and_redacts_bad_response_values() {
    let response = poll(wire(), true).await.unwrap();
    assert!(response.tasks[0].delegation.is_some());
    assert!(poll(wire(), false).await.is_err());
    let mut invalid = wire();
    invalid["tasks"][0]["delegation"]["version"] = json!("PRIVATE_DECODE_VALUE");
    let error = poll(invalid, true).await.unwrap_err();
    assert!(!error.contains("PRIVATE_DECODE_VALUE"));
}
