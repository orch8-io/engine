use super::*;
use chrono::{Duration, Utc};
use orch8_types::delegation_definition::{
    DefinitionGraphLimits,
    protocol::{DelegationProtocolCapabilities, MAX_INPUT_EXPANDED_BYTES, MAX_INPUT_PAYLOAD_BYTES},
};
use serde_json::json;

fn capabilities() -> RuntimeCapabilities {
    let now = Utc::now();
    serde_json::from_value(json!({"runtime_id":RuntimeId::new(),"kind":"server","trust":"registered","handlers":["noop"],"observed_at":now,"expires_at":now+Duration::minutes(4)})).unwrap()
}
fn request(caps: RuntimeCapabilities) -> Result<WorkerClaimRequest, StorageError> {
    WorkerClaimRequest::new(
        WorkerClaimPrincipal::Root {},
        "noop".into(),
        caps.runtime_id.to_string(),
        None,
        None,
        Some(caps),
        1,
    )
}
#[test]
fn direct_claim_requests_reject_unsupported_decoder_contracts() {
    let mut caps = capabilities();
    let supported = DelegationProtocolCapabilities::v1(
        DefinitionGraphLimits::default(),
        MAX_INPUT_PAYLOAD_BYTES,
        MAX_INPUT_EXPANDED_BYTES,
    )
    .unwrap();
    caps.delegation_protocol = Some(supported);
    assert!(request(caps.clone()).is_ok());
    for variant in 0..3 {
        let mut invalid = supported;
        match variant {
            0 => invalid.version = 2,
            1 => invalid.max_selectors = 1025,
            _ => invalid.max_input_payload_bytes = 0,
        }
        caps.delegation_protocol = Some(invalid);
        assert!(request(caps.clone()).is_err());
    }
}
#[test]
fn legacy_claim_requests_do_not_invent_protocol_support() {
    let request = request(capabilities()).unwrap();
    assert!(request.capabilities.unwrap().delegation_protocol.is_none());
}
