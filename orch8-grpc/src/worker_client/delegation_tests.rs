use super::*;
use orch8_types::delegation_definition::{
    DefinitionGraphLimits,
    claim::DelegationClaimContext,
    protocol::{DelegationProtocolCapabilities, DelegationResourceUsage},
};
use serde_json::{Value, json};

fn delivery() -> (Value, DelegationProtocolCapabilities) {
    let vector: Value = serde_json::from_str(include_str!(
        "../../../contracts/vectors/delegation-definition-v1.json"
    ))
    .unwrap();
    let envelope: orch8_types::delegation_definition::wire::DefinitionWireEnvelope =
        serde_json::from_value(vector["envelope"].clone()).unwrap();
    let graph = envelope
        .verify(
            &envelope.scope,
            &envelope.reference,
            DefinitionGraphLimits::default(),
        )
        .unwrap();
    let input = orch8_types::delegation_definition::input::EncodedDelegationInput::from_value(
        &Value::Null,
        orch8_types::delegation_definition::input::InputLimits::default(),
    )
    .unwrap();
    let context = DelegationClaimContext {
        version: 1,
        scope: envelope.scope,
        request_sha256: "a".repeat(64),
        expires_at: chrono::Utc::now() + chrono::Duration::minutes(1),
        definition: envelope.reference,
        input: input.reference().clone(),
        usage: DelegationResourceUsage {
            definition_expanded_bytes: graph.expanded_bytes(),
            input_wire_version: 1,
            input_payload_bytes: input.reference().payload_bytes,
            input_expanded_bytes: input.reference().expanded_bytes,
        },
    };
    let protocol = DelegationProtocolCapabilities::v1(
        DefinitionGraphLimits::default(),
        4 * 1024 * 1024,
        16 * 1024 * 1024,
    )
    .unwrap();
    (
        json!({"id":context.scope.task_id,"instance_id":uuid::Uuid::new_v4(),"block_id":"mailbox","handler_name":"orch8.delegation","params":{},"context":{},"claim_epoch":context.scope.claim_epoch,"delegation":context}),
        protocol,
    )
}
#[test]
fn stream_decoder_requires_supported_context_and_matching_worker() {
    let (mut wire, protocol) = delivery();
    let worker = wire["delegation"]["scope"]["worker_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let decoded = decode_stream_task(&wire.to_string(), &worker, Some(&protocol)).unwrap();
    assert!(decoded.delegation.is_some());
    assert!(decode_stream_task(&wire.to_string(), &worker, None).is_err());
    assert!(decode_stream_task(&wire.to_string(), "other-worker", Some(&protocol)).is_err());
    wire["params"] = json!({"input":"PRIVATE_INLINE"});
    assert!(decode_stream_task(&wire.to_string(), &worker, Some(&protocol)).is_err());
    wire["params"] = json!({});
    wire.as_object_mut().unwrap().remove("delegation");
    assert!(decode_stream_task(&wire.to_string(), &worker, Some(&protocol)).is_err());
    wire["handler_name"] = json!("legacy");
    assert!(decode_stream_task(&wire.to_string(), &worker, None).is_ok());
}
