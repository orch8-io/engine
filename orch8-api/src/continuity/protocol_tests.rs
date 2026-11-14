use super::validate_runtime_registration;
use crate::browser_sessions::RuntimeBinding;
use crate::device_sessions::{clamp_advertisement, destination_view};
use chrono::{Duration, Utc};
use orch8_types::continuity::{RuntimeCapabilities, RuntimeId, RuntimeKind, RuntimeTrustLevel};
use orch8_types::delegation_definition::{
    DefinitionGraphLimits,
    protocol::{DelegationProtocolCapabilities, MAX_INPUT_EXPANDED_BYTES, MAX_INPUT_PAYLOAD_BYTES},
};
use orch8_types::ids::TenantId;
use serde_json::json;

fn supported() -> DelegationProtocolCapabilities {
    DelegationProtocolCapabilities::v1(
        DefinitionGraphLimits::default(),
        MAX_INPUT_PAYLOAD_BYTES,
        MAX_INPUT_EXPANDED_BYTES,
    )
    .unwrap()
}
fn capabilities() -> RuntimeCapabilities {
    let now = Utc::now();
    serde_json::from_value(json!({"runtime_id":RuntimeId::new(),"kind":"mobile","trust":"registered","handlers":["orch8.delegation","private-charge"],"observed_at":now,"expires_at":now+Duration::minutes(4)})).unwrap()
}

#[test]
fn legacy_advertisements_do_not_acquire_protocol_support_implicitly() {
    let caps = capabilities();
    validate_runtime_registration(&caps, Utc::now()).unwrap();
    assert!(caps.delegation_protocol.is_none());
    assert!(
        serde_json::to_value(caps)
            .unwrap()
            .get("delegation_protocol")
            .is_none()
    );
}

#[test]
fn registration_rejects_invalid_protocol_versions_and_resource_limits() {
    let mut caps = capabilities();
    caps.delegation_protocol = Some(supported());
    validate_runtime_registration(&caps, Utc::now()).unwrap();
    for variant in 0..5 {
        let mut protocol = supported();
        match variant {
            0 => protocol.version = 2,
            1 => protocol.input_wire_version = 2,
            2 => protocol.max_definition_payload_bytes = u32::MAX,
            3 => protocol.max_definitions = 0,
            _ => protocol.max_input_expanded_bytes = 0,
        }
        caps.delegation_protocol = Some(protocol);
        assert!(matches!(
            validate_runtime_registration(&caps, Utc::now()),
            Err(crate::error::ApiError::InvalidArgument(_))
        ));
    }
}

#[test]
fn destination_discovery_preserves_protocol_limits_without_disclosing_inventory() {
    let mut caps = capabilities();
    caps.delegation_protocol = Some(supported());
    caps.trust = RuntimeTrustLevel::Attested;
    caps.plugins = vec!["private-plugin".into()];
    caps.credentials = vec!["private-binding".into()];
    caps.hardware = vec!["private-hardware".into()];
    caps.labels
        .insert("private-label".into(), "private-value".into());
    let visible = destination_view(vec![caps], RuntimeId::new(), Utc::now());
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].delegation_protocol, Some(supported()));
    assert_eq!(visible[0].trust, RuntimeTrustLevel::Registered);
    assert!(
        visible[0].plugins.is_empty()
            && visible[0].credentials.is_empty()
            && visible[0].hardware.is_empty()
            && visible[0].labels.is_empty()
    );
}

#[test]
fn session_protocol_support_does_not_expand_handler_or_placement_authority() {
    let mut caps = capabilities();
    caps.delegation_protocol = Some(supported());
    caps.labels.insert("residency".into(), "eu".into());
    let binding = RuntimeBinding {
        tenant_id: TenantId::unchecked("tenant"),
        kind: RuntimeKind::Mobile,
        runtime_id: caps.runtime_id,
        device_id: Some("device".into()),
        handlers: vec!["orch8.delegation".into()],
        queues: vec![],
        expires_at: Utc::now() + Duration::minutes(1),
    };
    clamp_advertisement(&binding, &mut caps).unwrap();
    assert_eq!(caps.delegation_protocol, Some(supported()));
    assert_eq!(caps.handlers, vec!["orch8.delegation"]);
    assert!(caps.labels.is_empty());
    assert_eq!(caps.expires_at, binding.expires_at);
}

#[test]
fn destination_discovery_excludes_unready_and_unknown_decoders() {
    let mut caps = capabilities();
    caps.delegation_protocol = None;
    assert!(destination_view(vec![caps.clone()], RuntimeId::new(), Utc::now()).is_empty());
    let mut unsupported = supported();
    unsupported.version = 2;
    caps.delegation_protocol = Some(unsupported);
    assert!(destination_view(vec![caps], RuntimeId::new(), Utc::now()).is_empty());
}
