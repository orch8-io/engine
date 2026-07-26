//! Coverage tests for the private managed control tunnel and the graceful
//! drain evidence it persists before closing the outbound session.
//!
//! Count contract: 61 independently named unit tests.

use super::*;

fn config() -> ManagedControlConfig {
    ManagedControlConfig {
        endpoint: "https://control.orch8.example:443".into(),
        api_key: "super-secret-managed-key".into(),
        tenant_id: "tenant-acme".into(),
        worker_id: "worker-edge-7".into(),
        runtime_id: RuntimeId::new(),
        kind: RuntimeKind::Edge,
    }
}

fn config_with_kind(kind: RuntimeKind) -> ManagedControlConfig {
    ManagedControlConfig { kind, ..config() }
}

macro_rules! draining_flag_case {
    ($name:ident, $input:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let capabilities = safe_capabilities(&config(), $input);
            assert_eq!(capabilities.draining, $expected);
        }
    };
}

draining_flag_case!(
    coverage_tunnel_001_steady_state_capabilities_are_not_draining,
    false,
    false
);
draining_flag_case!(
    coverage_tunnel_002_drain_evidence_capabilities_are_draining,
    true,
    true
);

#[test]
fn coverage_tunnel_003_handlers_advertise_exactly_managed_control() {
    let capabilities = safe_capabilities(&config(), false);
    assert_eq!(capabilities.handlers, vec!["managed-control".to_string()]);
}

macro_rules! unverified_trust_case {
    ($name:ident, $draining:expr) => {
        #[test]
        fn $name() {
            let capabilities = safe_capabilities(&config(), $draining);
            assert_eq!(capabilities.trust, RuntimeTrustLevel::Unverified);
        }
    };
}

unverified_trust_case!(coverage_tunnel_004_steady_state_trust_is_unverified, false);
unverified_trust_case!(
    coverage_tunnel_005_drain_evidence_never_elevates_trust,
    true
);

#[test]
fn coverage_tunnel_006_runtime_is_not_offline_capable() {
    assert!(!safe_capabilities(&config(), false).offline_capable);
}

#[test]
fn coverage_tunnel_007_connectivity_reports_ethernet() {
    assert_eq!(
        safe_capabilities(&config(), false).connectivity,
        Some(RuntimeConnectivity::Ethernet)
    );
}

#[test]
fn coverage_tunnel_008_plugins_are_empty() {
    assert!(safe_capabilities(&config(), false).plugins.is_empty());
}

#[test]
fn coverage_tunnel_009_credentials_are_empty() {
    assert!(safe_capabilities(&config(), false).credentials.is_empty());
}

#[test]
fn coverage_tunnel_010_regions_are_empty() {
    assert!(safe_capabilities(&config(), false).regions.is_empty());
}

#[test]
fn coverage_tunnel_011_hardware_is_empty() {
    assert!(safe_capabilities(&config(), false).hardware.is_empty());
}

#[test]
fn coverage_tunnel_012_battery_percent_is_absent() {
    assert!(
        safe_capabilities(&config(), false)
            .battery_percent
            .is_none()
    );
}

#[test]
fn coverage_tunnel_013_estimated_cost_is_absent() {
    assert!(
        safe_capabilities(&config(), false)
            .estimated_cost_microunits
            .is_none()
    );
}

#[test]
fn coverage_tunnel_014_estimated_latency_is_absent() {
    assert!(
        safe_capabilities(&config(), false)
            .estimated_latency_ms
            .is_none()
    );
}

#[test]
fn coverage_tunnel_015_capsule_signing_key_is_absent() {
    assert!(
        safe_capabilities(&config(), false)
            .capsule_signing_public_key
            .is_none()
    );
}

#[test]
fn coverage_tunnel_016_runtime_id_matches_config_identity() {
    let config = config();
    assert_eq!(
        safe_capabilities(&config, false).runtime_id,
        config.runtime_id
    );
}

#[test]
fn coverage_tunnel_017_edge_kind_is_propagated() {
    let capabilities = safe_capabilities(&config_with_kind(RuntimeKind::Edge), false);
    assert_eq!(capabilities.kind, RuntimeKind::Edge);
}

#[test]
fn coverage_tunnel_018_server_kind_is_propagated() {
    let capabilities = safe_capabilities(&config_with_kind(RuntimeKind::Server), false);
    assert_eq!(capabilities.kind, RuntimeKind::Server);
}

macro_rules! expiry_window_case {
    ($name:ident, $draining:expr) => {
        #[test]
        fn $name() {
            let capabilities = safe_capabilities(&config(), $draining);
            assert_eq!(
                capabilities.expires_at - capabilities.observed_at,
                chrono::Duration::seconds(45)
            );
        }
    };
}

expiry_window_case!(
    coverage_tunnel_019_steady_state_expiry_window_is_45_seconds,
    false
);
expiry_window_case!(
    coverage_tunnel_020_drain_evidence_expiry_window_is_45_seconds,
    true
);

#[test]
fn coverage_tunnel_021_observed_at_is_stamped_within_call_window() {
    let before = chrono::Utc::now();
    let capabilities = safe_capabilities(&config(), false);
    let after = chrono::Utc::now();
    assert!(capabilities.observed_at >= before);
    assert!(capabilities.observed_at <= after);
}

#[test]
fn coverage_tunnel_022_expires_at_is_strictly_in_the_future() {
    let capabilities = safe_capabilities(&config(), true);
    assert!(capabilities.expires_at > chrono::Utc::now());
}

#[test]
fn coverage_tunnel_023_capabilities_json_is_a_json_object() {
    let value: serde_json::Value =
        serde_json::from_str(&capabilities_json(&config(), false).unwrap()).unwrap();
    assert!(value.is_object());
}

macro_rules! roundtrip_case {
    ($name:ident, $draining:expr) => {
        #[test]
        fn $name() {
            let config = config();
            let json = capabilities_json(&config, $draining).unwrap();
            let decoded: RuntimeCapabilities = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.draining, $draining);
            assert_eq!(decoded.runtime_id, config.runtime_id);
            assert_eq!(decoded.kind, config.kind);
            assert_eq!(decoded.handlers, vec!["managed-control".to_string()]);
        }
    };
}

roundtrip_case!(coverage_tunnel_024_drain_evidence_json_roundtrips, true);
roundtrip_case!(coverage_tunnel_025_steady_state_json_roundtrips, false);

#[test]
fn coverage_tunnel_026_drain_evidence_json_marks_draining_true() {
    let json = capabilities_json(&config(), true).unwrap();
    assert!(json.contains("\"draining\":true"), "json was: {json}");
}

#[test]
fn coverage_tunnel_027_steady_state_json_marks_draining_false() {
    let json = capabilities_json(&config(), false).unwrap();
    assert!(json.contains("\"draining\":false"), "json was: {json}");
}

/// The graceful-drain heartbeat must differ from the steady-state heartbeat
/// ONLY in the `draining` flag (and the fresh timestamps): the cloud records
/// the drain evidence against an otherwise identical capability snapshot.
#[test]
fn coverage_tunnel_028_drain_evidence_differs_only_in_draining_and_timestamps() {
    let config = config();
    let mut steady: serde_json::Value =
        serde_json::from_str(&capabilities_json(&config, false).unwrap()).unwrap();
    let mut drain: serde_json::Value =
        serde_json::from_str(&capabilities_json(&config, true).unwrap()).unwrap();
    for key in ["observed_at", "expires_at"] {
        steady.as_object_mut().unwrap().remove(key);
        drain.as_object_mut().unwrap().remove(key);
    }
    assert_eq!(drain["draining"], serde_json::json!(true));
    assert_eq!(steady["draining"], serde_json::json!(false));
    drain.as_object_mut().unwrap().remove("draining");
    steady.as_object_mut().unwrap().remove("draining");
    assert_eq!(steady, drain);
}

macro_rules! redaction_case {
    ($name:ident, $fragment:expr) => {
        #[test]
        fn $name() {
            let fragment: &str = $fragment;
            let json = capabilities_json(&config(), false).unwrap();
            assert!(
                !json.contains(fragment),
                "capabilities leaked {fragment:?}: {json}"
            );
        }
    };
}

redaction_case!(
    coverage_tunnel_029_api_key_material_never_leaks,
    "super-secret-managed-key"
);
redaction_case!(coverage_tunnel_030_tenant_id_never_leaks, "tenant-acme");
redaction_case!(coverage_tunnel_031_worker_id_never_leaks, "worker-edge-7");
redaction_case!(
    coverage_tunnel_032_endpoint_host_never_leaks,
    "control.orch8.example"
);

#[test]
fn coverage_tunnel_033_drain_evidence_redacts_api_key_too() {
    let json = capabilities_json(&config(), true).unwrap();
    assert!(!json.contains("super-secret-managed-key"));
    assert!(!json.contains("tenant-acme"));
}

macro_rules! absent_key_case {
    ($name:ident, $key:expr) => {
        #[test]
        fn $name() {
            let value: serde_json::Value =
                serde_json::from_str(&capabilities_json(&config(), true).unwrap()).unwrap();
            assert!(
                value.get($key).is_none(),
                "empty field {:?} must be skipped, got: {value}",
                $key
            );
        }
    };
}

absent_key_case!(
    coverage_tunnel_034_credentials_key_is_skipped,
    "credentials"
);
absent_key_case!(coverage_tunnel_035_plugins_key_is_skipped, "plugins");
absent_key_case!(coverage_tunnel_036_regions_key_is_skipped, "regions");
absent_key_case!(coverage_tunnel_037_hardware_key_is_skipped, "hardware");
absent_key_case!(
    coverage_tunnel_038_battery_key_is_skipped,
    "battery_percent"
);
absent_key_case!(
    coverage_tunnel_039_cost_key_is_skipped,
    "estimated_cost_microunits"
);
absent_key_case!(
    coverage_tunnel_040_latency_key_is_skipped,
    "estimated_latency_ms"
);
absent_key_case!(
    coverage_tunnel_041_signing_key_is_skipped,
    "capsule_signing_public_key"
);

macro_rules! json_field_case {
    ($name:ident, $pointer:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let value: serde_json::Value =
                serde_json::from_str(&capabilities_json(&config(), false).unwrap()).unwrap();
            assert_eq!(value.pointer($pointer).unwrap(), &$expected);
        }
    };
}

json_field_case!(
    coverage_tunnel_042_json_trust_is_unverified,
    "/trust",
    serde_json::json!("unverified")
);
json_field_case!(
    coverage_tunnel_043_json_kind_is_edge,
    "/kind",
    serde_json::json!("edge")
);
json_field_case!(
    coverage_tunnel_044_json_connectivity_is_ethernet,
    "/connectivity",
    serde_json::json!("ethernet")
);
json_field_case!(
    coverage_tunnel_045_json_first_handler_is_managed_control,
    "/handlers/0",
    serde_json::json!("managed-control")
);
json_field_case!(
    coverage_tunnel_046_json_offline_capable_is_false,
    "/offline_capable",
    serde_json::json!(false)
);

#[test]
fn coverage_tunnel_047_client_frame_preserves_open_payload() {
    let open = WorkerStreamOpen {
        worker_id: "worker-edge-7".into(),
        handler_names: vec!["managed-control".into()],
        supported_features: vec!["draining".into()],
        max_in_flight: 1,
        protocol_version: 1,
        runtime_capabilities_json: "{}".into(),
        tenant_id: "tenant-acme".into(),
    };
    let frame = client_frame(ClientPayload::Open(open));
    let Some(ClientPayload::Open(payload)) = frame.payload else {
        panic!("open payload must survive framing");
    };
    assert_eq!(payload.worker_id, "worker-edge-7");
    assert_eq!(payload.tenant_id, "tenant-acme");
    assert_eq!(payload.protocol_version, 1);
}

#[test]
fn coverage_tunnel_048_client_frame_carries_heartbeat_json_verbatim() {
    let json = capabilities_json(&config(), true).unwrap();
    let frame = client_frame(ClientPayload::RuntimeHeartbeat(RuntimeHeartbeat {
        runtime_capabilities_json: json.clone(),
    }));
    let Some(ClientPayload::RuntimeHeartbeat(payload)) = frame.payload else {
        panic!("heartbeat payload must survive framing");
    };
    assert_eq!(payload.runtime_capabilities_json, json);
}

#[test]
fn coverage_tunnel_049_client_frame_preserves_command_ack_id() {
    let frame = client_frame(ClientPayload::CommandAck(WorkerCommandAck {
        command_id: "cmd-42".into(),
    }));
    let Some(ClientPayload::CommandAck(payload)) = frame.payload else {
        panic!("ack payload must survive framing");
    };
    assert_eq!(payload.command_id, "cmd-42");
}

fn command_json(kind: &str) -> String {
    serde_json::json!({
        "id": uuid::Uuid::new_v4(),
        "worker_id": "worker-edge-7",
        "command": kind,
        "payload": null,
        "created_at": chrono::Utc::now(),
    })
    .to_string()
}

macro_rules! command_decode_case {
    ($name:ident, $kind:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let command: WorkerCommand = serde_json::from_str(&command_json($kind)).unwrap();
            assert_eq!(command.command, $expected);
            assert_eq!(command.worker_id, "worker-edge-7");
        }
    };
}

command_decode_case!(
    coverage_tunnel_050_drain_command_decodes,
    "drain",
    WorkerCommandKind::Drain
);
command_decode_case!(
    coverage_tunnel_051_ping_command_decodes,
    "ping",
    WorkerCommandKind::Ping
);
command_decode_case!(
    coverage_tunnel_052_reload_command_decodes,
    "reload",
    WorkerCommandKind::Reload
);
command_decode_case!(
    coverage_tunnel_053_place_command_decodes,
    "place",
    WorkerCommandKind::Place
);

/// Deny path mirrored from `run_session`: an unknown command kind must fail
/// decoding instead of being silently acknowledged.
#[test]
fn coverage_tunnel_054_unknown_command_kind_is_rejected() {
    let result = serde_json::from_str::<WorkerCommand>(&command_json("shutdown"));
    assert!(result.is_err());
}

#[test]
fn coverage_tunnel_055_malformed_command_json_is_rejected() {
    assert!(serde_json::from_str::<WorkerCommand>("{not json").is_err());
}

#[test]
fn coverage_tunnel_056_command_without_id_is_rejected() {
    let json = serde_json::json!({
        "worker_id": "worker-edge-7",
        "command": "drain",
        "created_at": chrono::Utc::now(),
    })
    .to_string();
    assert!(serde_json::from_str::<WorkerCommand>(&json).is_err());
}

#[test]
fn coverage_tunnel_057_config_clone_preserves_identity_fields() {
    let config = config();
    let cloned = config.clone();
    assert_eq!(cloned.endpoint, config.endpoint);
    assert_eq!(cloned.tenant_id, config.tenant_id);
    assert_eq!(cloned.worker_id, config.worker_id);
    assert_eq!(cloned.runtime_id, config.runtime_id);
    assert_eq!(cloned.kind, config.kind);
}

#[test]
fn coverage_tunnel_058_config_clone_preserves_api_key_material() {
    let config = config();
    assert_eq!(config.clone().api_key.expose(), "super-secret-managed-key");
}

#[test]
fn coverage_tunnel_059_consecutive_capabilities_share_runtime_identity() {
    let config = config();
    let first = safe_capabilities(&config, false);
    let second = safe_capabilities(&config, true);
    assert_eq!(first.runtime_id, second.runtime_id);
    assert_eq!(first.kind, second.kind);
}

#[test]
fn coverage_tunnel_060_command_without_kind_is_rejected() {
    let json = serde_json::json!({
        "id": uuid::Uuid::new_v4(),
        "worker_id": "worker-edge-7",
        "created_at": chrono::Utc::now(),
    })
    .to_string();
    assert!(serde_json::from_str::<WorkerCommand>(&json).is_err());
}

#[test]
fn coverage_tunnel_061_command_without_worker_id_is_rejected() {
    let json = serde_json::json!({
        "id": uuid::Uuid::new_v4(),
        "command": "ping",
        "created_at": chrono::Utc::now(),
    })
    .to_string();
    assert!(serde_json::from_str::<WorkerCommand>(&json).is_err());
}
