//! Coverage tests for the aggregate startup preflight, federation peer
//! parsing, auth-config validation, artifact store selection, config file
//! loading, and CLI flag parsing.
//!
//! Count contract: 86 independently named unit tests.
//!
//! Env hygiene: `automatic_startup_preflight`, `validate_auth_config`, and
//! `load_config` read process-global env vars, so every test that calls them
//! runs under `#[serial(otlp_env)]` (the same key the pre-existing env tests
//! in `main.rs` use) and clears the relevant variables through [`EnvGuard`].
//! That makes the `unsafe` env blocks below sound: no test in this binary can
//! observe a half-mutated environment.

use super::*;
use serial_test::serial;

const ENV_VARS: [&str; 3] = [
    "ORCH8_FEDERATION_PEERS",
    "ORCH8_MOBILE_SYNC_ENABLED",
    "ORCH8_ALLOW_NO_TENANT_ISOLATION",
];

/// Captures, clears, and restores the preflight-relevant environment.
struct EnvGuard {
    saved: [Option<String>; 3],
}

impl EnvGuard {
    fn cleared() -> Self {
        #[allow(unsafe_code)]
        // SAFETY: serialized via #[serial(otlp_env)] at every call site.
        unsafe {
            let saved = ENV_VARS.map(|name| std::env::var(name).ok());
            for name in ENV_VARS {
                std::env::remove_var(name);
            }
            Self { saved }
        }
    }

    // Keeping this as a method ties each mutation visibly to the live guard
    // that restores the process environment when the test scope exits.
    #[allow(clippy::unused_self)]
    fn set(&self, name: &str, value: &str) {
        #[allow(unsafe_code)]
        // SAFETY: serialized via #[serial(otlp_env)] at every call site.
        unsafe {
            std::env::set_var(name, value);
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        #[allow(unsafe_code)]
        // SAFETY: serialized via #[serial(otlp_env)] at every call site.
        unsafe {
            for (name, value) in ENV_VARS.iter().zip(self.saved.iter()) {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }
}

fn valid_config() -> EngineConfig {
    let mut config = EngineConfig::default();
    config.database.url = "postgres://orch8:orch8@localhost:5432/orch8".into();
    config
}

fn preflight(config: &EngineConfig, role: NodeRole) -> anyhow::Result<()> {
    automatic_startup_preflight(config, NodeAssembly::for_role(role))
}

macro_rules! preflight_err_case {
    ($name:ident, $role:expr, $mutate:expr, $expected:expr) => {
        #[test]
        #[serial(otlp_env)]
        fn $name() {
            let _env = EnvGuard::cleared();
            let mut config = valid_config();
            ($mutate)(&mut config);
            let Err(error) = preflight(&config, $role) else {
                panic!("preflight must fail");
            };
            let message = error.to_string();
            assert!(message.contains($expected), "error was: {message}");
        }
    };
}

macro_rules! preflight_ok_case {
    ($name:ident, $role:expr, $mutate:expr) => {
        #[test]
        #[serial(otlp_env)]
        fn $name() {
            let _env = EnvGuard::cleared();
            let mut config = valid_config();
            ($mutate)(&mut config);
            if let Err(error) = preflight(&config, $role) {
                panic!("preflight must pass, got: {error}");
            }
        }
    };
}

preflight_err_case!(
    coverage_preflight_001_empty_database_url_is_rejected,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| c.database.url = "".into(),
    "database.url must be configured for startup"
);
preflight_err_case!(
    coverage_preflight_002_garbage_http_addr_is_rejected,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| c.api.http_addr = "not-an-address".into(),
    "api.http_addr is invalid"
);
preflight_err_case!(
    coverage_preflight_003_invalid_grpc_addr_is_rejected_on_full_surface,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| c.api.grpc_addr = "999.999.999.999:1".into(),
    "api.grpc_addr is invalid"
);
preflight_err_case!(
    coverage_preflight_004_invalid_grpc_addr_is_rejected_on_executor,
    NodeRole::Executor,
    |c: &mut EngineConfig| c.api.grpc_addr = "localhost:nope".into(),
    "api.grpc_addr is invalid"
);
preflight_err_case!(
    coverage_preflight_005_shared_http_grpc_socket_is_rejected,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| c.api.grpc_addr = c.api.http_addr.clone(),
    "api.http_addr and api.grpc_addr must use different sockets"
);
preflight_err_case!(
    coverage_preflight_006_shared_socket_is_rejected_on_executor,
    NodeRole::Executor,
    |c: &mut EngineConfig| {
        c.api.http_addr = "0.0.0.0:9000".into();
        c.api.grpc_addr = "0.0.0.0:9000".into();
    },
    "different sockets"
);
preflight_err_case!(
    coverage_preflight_007_cert_without_key_and_ca_is_rejected,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| c.api.grpc_tls_cert_path = "/tmp/server.crt".into(),
    "gRPC TLS requires certificate, private key, and client CA paths"
);
preflight_err_case!(
    coverage_preflight_008_cert_and_key_without_ca_is_rejected,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| {
        c.api.grpc_tls_cert_path = "/tmp/server.crt".into();
        c.api.grpc_tls_key_path = "/tmp/server.key".into();
    },
    "certificate, private key, and client CA"
);
preflight_err_case!(
    coverage_preflight_009_partial_tls_is_validated_even_when_grpc_disabled,
    NodeRole::Edge,
    |c: &mut EngineConfig| c.api.grpc_tls_client_ca_path = "/tmp/ca.crt".into(),
    "certificate, private key, and client CA"
);
preflight_err_case!(
    coverage_preflight_010_missing_tls_files_are_rejected,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| {
        c.api.grpc_tls_cert_path = "/definitely/missing/server.crt".into();
        c.api.grpc_tls_key_path = "/definitely/missing/server.key".into();
        c.api.grpc_tls_client_ca_path = "/definitely/missing/ca.crt".into();
    },
    "cannot be read"
);
// The TLS file-check tests below keep their tempdir in a binding that
// outlives the preflight call, so the directory is removed on drop instead
// of being leaked with `mem::forget`.
#[test]
#[serial(otlp_env)]
fn coverage_preflight_011_empty_tls_files_are_rejected() {
    let _env = EnvGuard::cleared();
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, b"").unwrap();
    let path = empty.to_str().unwrap().to_string();
    let mut config = valid_config();
    config.api.grpc_tls_cert_path = path.clone();
    config.api.grpc_tls_key_path = path.clone();
    config.api.grpc_tls_client_ca_path = path;
    let Err(error) = preflight(&config, NodeRole::AllInOne) else {
        panic!("preflight must fail");
    };
    assert!(
        error
            .to_string()
            .contains("must reference a non-empty regular file"),
        "error was: {error}"
    );
}

#[test]
#[serial(otlp_env)]
fn coverage_preflight_012_tls_directories_are_rejected() {
    let _env = EnvGuard::cleared();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let mut config = valid_config();
    config.api.grpc_tls_cert_path = path.clone();
    config.api.grpc_tls_key_path = path.clone();
    config.api.grpc_tls_client_ca_path = path;
    let Err(error) = preflight(&config, NodeRole::AllInOne) else {
        panic!("preflight must fail");
    };
    assert!(
        error
            .to_string()
            .contains("must reference a non-empty regular file"),
        "error was: {error}"
    );
}

#[test]
#[serial(otlp_env)]
fn coverage_preflight_013_single_missing_tls_file_names_its_field() {
    let _env = EnvGuard::cleared();
    let dir = tempfile::tempdir().unwrap();
    let pem = dir.path().join("real.pem");
    std::fs::write(&pem, b"pem-bytes").unwrap();
    let pem = pem.to_str().unwrap().to_string();
    let mut config = valid_config();
    config.api.grpc_tls_cert_path = pem.clone();
    config.api.grpc_tls_key_path = "/definitely/missing/server.key".into();
    config.api.grpc_tls_client_ca_path = pem;
    let Err(error) = preflight(&config, NodeRole::AllInOne) else {
        panic!("preflight must fail");
    };
    assert!(
        error
            .to_string()
            .contains("api.grpc_tls_key_path cannot be read"),
        "error was: {error}"
    );
}
preflight_err_case!(
    coverage_preflight_014_garbage_mtls_identities_are_rejected,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| c.api.grpc_mtls_identities = "not json".into(),
    "api.grpc_mtls_identities is invalid"
);
preflight_err_case!(
    coverage_preflight_015_short_mtls_fingerprint_is_rejected,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| c.api.grpc_mtls_identities = format!(
        r#"{{"{}":{{"tenant_id":"t","identity":"i"}}}}"#,
        "a".repeat(63)
    ),
    "api.grpc_mtls_identities is invalid"
);
preflight_err_case!(
    coverage_preflight_016_empty_mtls_tenant_is_rejected,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| c.api.grpc_mtls_identities = format!(
        r#"{{"{}":{{"tenant_id":"  ","identity":"i"}}}}"#,
        "b".repeat(64)
    ),
    "api.grpc_mtls_identities is invalid"
);
// Fingerprints normalize case and colons, so these two keys collide.
preflight_err_case!(
    coverage_preflight_017_duplicate_mtls_fingerprint_after_normalization_is_rejected,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| {
        let plain = "ab".repeat(32);
        let colonated = plain
            .chars()
            .collect::<Vec<_>>()
            .chunks(2)
            .map(|pair| pair.iter().collect::<String>().to_uppercase())
            .collect::<Vec<_>>()
            .join(":");
        c.api.grpc_mtls_identities = format!(
            r#"{{"{plain}":{{"tenant_id":"t","identity":"i"}},"{colonated}":{{"tenant_id":"t","identity":"i"}}}}"#
        );
    },
    "api.grpc_mtls_identities is invalid"
);
preflight_err_case!(
    coverage_preflight_018_bare_otlp_endpoint_is_rejected,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| c.telemetry.otlp_endpoint = "collector:4317".into(),
    "telemetry.otlp_endpoint must be an absolute HTTP(S) URI"
);
preflight_err_case!(
    coverage_preflight_019_non_http_otlp_scheme_is_rejected,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| c.telemetry.otlp_endpoint = "ftp://collector:4317".into(),
    "absolute HTTP(S) URI"
);
preflight_err_case!(
    coverage_preflight_020_relative_otlp_endpoint_is_rejected,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| c.telemetry.otlp_endpoint = "/v1/traces".into(),
    "absolute HTTP(S) URI"
);
preflight_err_case!(
    coverage_preflight_021_gateway_role_surfaces_config_validate_errors,
    NodeRole::Gateway,
    |c: &mut EngineConfig| c.node.role = NodeRole::Gateway,
    "node.role=gateway requires api.api_key"
);
preflight_err_case!(
    coverage_preflight_022_managed_control_is_rejected_on_all_in_one,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| {
        c.node.managed_control_endpoint = "https://control.example.com".into();
        c.node.managed_control_api_key = "key".into();
        c.node.managed_control_tenant_id = "tenant".into();
        c.node.managed_control_worker_id = "worker".into();
        c.node.managed_control_runtime_id = uuid::Uuid::new_v4().to_string();
    },
    "node.managed_control_endpoint requires node.role=executor or edge"
);
preflight_err_case!(
    coverage_preflight_023_managed_control_requires_https,
    NodeRole::Executor,
    |c: &mut EngineConfig| {
        c.node.role = NodeRole::Executor;
        c.node.managed_control_endpoint = "http://control.example.com".into();
        c.node.managed_control_api_key = "key".into();
        c.node.managed_control_tenant_id = "tenant".into();
        c.node.managed_control_worker_id = "worker".into();
        c.node.managed_control_runtime_id = uuid::Uuid::new_v4().to_string();
    },
    "node.managed_control_endpoint must use HTTPS"
);
preflight_err_case!(
    coverage_preflight_024_managed_control_requires_full_credentials,
    NodeRole::Executor,
    |c: &mut EngineConfig| {
        c.node.role = NodeRole::Executor;
        c.node.managed_control_endpoint = "https://control.example.com".into();
    },
    "managed control requires API key, tenant, worker id, and UUID runtime id"
);
preflight_err_case!(
    coverage_preflight_025_managed_control_rejects_non_uuid_runtime_id,
    NodeRole::Executor,
    |c: &mut EngineConfig| {
        c.node.role = NodeRole::Executor;
        c.node.managed_control_endpoint = "https://control.example.com".into();
        c.node.managed_control_api_key = "key".into();
        c.node.managed_control_tenant_id = "tenant".into();
        c.node.managed_control_worker_id = "worker".into();
        c.node.managed_control_runtime_id = "not-a-uuid".into();
    },
    "UUID runtime id"
);

preflight_ok_case!(
    coverage_preflight_026_valid_all_in_one_config_passes,
    NodeRole::AllInOne,
    |_c: &mut EngineConfig| {}
);
preflight_ok_case!(
    coverage_preflight_027_valid_control_config_passes,
    NodeRole::Control,
    |_c: &mut EngineConfig| {}
);
// The gRPC address check is skipped entirely when the role disables gRPC.
preflight_ok_case!(
    coverage_preflight_028_edge_ignores_invalid_grpc_addr,
    NodeRole::Edge,
    |c: &mut EngineConfig| c.api.grpc_addr = "not-an-address".into()
);
preflight_ok_case!(
    coverage_preflight_029_edge_ignores_shared_http_grpc_socket,
    NodeRole::Edge,
    |c: &mut EngineConfig| c.api.grpc_addr = c.api.http_addr.clone()
);
#[test]
#[serial(otlp_env)]
fn coverage_preflight_030_complete_tls_bundle_with_real_files_passes() {
    let _env = EnvGuard::cleared();
    let dir = tempfile::tempdir().unwrap();
    let pem = dir.path().join("bundle.pem");
    std::fs::write(&pem, b"pem-bytes").unwrap();
    let pem = pem.to_str().unwrap().to_string();
    let mut config = valid_config();
    config.api.grpc_tls_cert_path = pem.clone();
    config.api.grpc_tls_key_path = pem.clone();
    config.api.grpc_tls_client_ca_path = pem;
    if let Err(error) = preflight(&config, NodeRole::AllInOne) {
        panic!("preflight must pass, got: {error}");
    }
}
preflight_ok_case!(
    coverage_preflight_031_empty_mtls_identities_pass,
    NodeRole::AllInOne,
    |_c: &mut EngineConfig| {}
);
preflight_ok_case!(
    coverage_preflight_032_mtls_fingerprint_case_and_colons_are_normalized,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| {
        // 32 colon-separated pairs = 64 hex chars after normalization.
        let colonated = "AB:CD".repeat(16);
        c.api.grpc_mtls_identities =
            format!(r#"{{"{colonated}":{{"tenant_id":"t","identity":"i"}}}}"#);
    }
);
preflight_ok_case!(
    coverage_preflight_033_absolute_http_otlp_endpoint_passes,
    NodeRole::AllInOne,
    |c: &mut EngineConfig| c.telemetry.otlp_endpoint = "http://collector:4317".into()
);

/// The aggregate report has a fixed shape: a header line, then one
/// `"\n  - "`-joined line per failure.
#[test]
#[serial(otlp_env)]
fn coverage_preflight_034_error_report_shape_is_header_plus_bullets() {
    let _env = EnvGuard::cleared();
    let mut config = valid_config();
    config.database.url = "".into();
    config.api.http_addr = "garbage".into();
    let Err(error) = preflight(&config, NodeRole::AllInOne) else {
        panic!("preflight must fail");
    };
    let message = error.to_string();
    assert!(
        message.starts_with("automatic startup preflight failed:\n  - "),
        "message was: {message}"
    );
    // Two errors => two bullets; the header itself ends with the first
    // "\n  - ", so the separator count equals the bullet count.
    assert_eq!(message.matches("\n  - ").count(), 2, "expected 2 bullets");
}

/// `config.validate()` errors are aggregated BEFORE the preflight's own
/// checks, so operators see role errors ahead of startup-specific ones.
#[test]
#[serial(otlp_env)]
fn coverage_preflight_035_validate_errors_precede_startup_errors() {
    let _env = EnvGuard::cleared();
    let mut config = valid_config();
    config.node.role = NodeRole::Gateway;
    config.database.url = "".into();
    let Err(error) = preflight(&config, NodeRole::Gateway) else {
        panic!("preflight must fail");
    };
    let message = error.to_string();
    let validate_index = message
        .find("node.role=gateway requires api.api_key")
        .unwrap();
    let startup_index = message.find("database.url must be configured").unwrap();
    assert!(validate_index < startup_index, "message was: {message}");
}

#[test]
#[serial(otlp_env)]
fn coverage_preflight_036_mobile_sync_is_rejected_on_executor() {
    let env = EnvGuard::cleared();
    env.set("ORCH8_MOBILE_SYNC_ENABLED", "1");
    let Err(error) = preflight(&valid_config(), NodeRole::Executor) else {
        panic!("preflight must fail");
    };
    assert!(
        error
            .to_string()
            .contains("ORCH8_MOBILE_SYNC_ENABLED requires an all_in_one or control node"),
        "error was: {error}"
    );
}

#[test]
#[serial(otlp_env)]
fn coverage_preflight_037_mobile_sync_true_is_rejected_on_edge() {
    let env = EnvGuard::cleared();
    env.set("ORCH8_MOBILE_SYNC_ENABLED", "true");
    let Err(error) = preflight(&valid_config(), NodeRole::Edge) else {
        panic!("preflight must fail");
    };
    assert!(
        error
            .to_string()
            .contains("ORCH8_MOBILE_SYNC_ENABLED requires an all_in_one or control node"),
        "error was: {error}"
    );
}

#[test]
#[serial(otlp_env)]
fn coverage_preflight_038_mobile_sync_is_rejected_on_gateway() {
    let env = EnvGuard::cleared();
    env.set("ORCH8_MOBILE_SYNC_ENABLED", "1");
    let Err(error) = preflight(&valid_config(), NodeRole::Gateway) else {
        panic!("preflight must fail");
    };
    assert!(
        error
            .to_string()
            .contains("ORCH8_MOBILE_SYNC_ENABLED requires an all_in_one or control node"),
        "error was: {error}"
    );
}

#[test]
#[serial(otlp_env)]
fn coverage_preflight_039_mobile_sync_is_allowed_on_all_in_one() {
    let env = EnvGuard::cleared();
    env.set("ORCH8_MOBILE_SYNC_ENABLED", "1");
    assert!(preflight(&valid_config(), NodeRole::AllInOne).is_ok());
}

#[test]
#[serial(otlp_env)]
fn coverage_preflight_040_mobile_sync_is_allowed_on_control() {
    let env = EnvGuard::cleared();
    env.set("ORCH8_MOBILE_SYNC_ENABLED", "true");
    assert!(preflight(&valid_config(), NodeRole::Control).is_ok());
}

/// Only "1"/"true" enable mobile sync; any other value leaves it off.
#[test]
#[serial(otlp_env)]
fn coverage_preflight_041_mobile_sync_unrecognized_value_stays_disabled() {
    let env = EnvGuard::cleared();
    env.set("ORCH8_MOBILE_SYNC_ENABLED", "yes");
    assert!(preflight(&valid_config(), NodeRole::Executor).is_ok());
}

#[test]
#[serial(otlp_env)]
fn coverage_preflight_042_invalid_federation_peers_env_fails_closed() {
    let env = EnvGuard::cleared();
    env.set("ORCH8_FEDERATION_PEERS", "not json");
    let Err(error) = preflight(&valid_config(), NodeRole::AllInOne) else {
        panic!("preflight must fail");
    };
    assert!(
        error
            .to_string()
            .contains("ORCH8_FEDERATION_PEERS is invalid"),
        "error was: {error}"
    );
}

#[test]
#[serial(otlp_env)]
fn coverage_preflight_043_valid_federation_peers_env_passes() {
    let env = EnvGuard::cleared();
    env.set("ORCH8_FEDERATION_PEERS", "[]");
    assert!(preflight(&valid_config(), NodeRole::AllInOne).is_ok());
}

/// A set-but-empty peers variable is invalid JSON, not "no peers" — startup
/// fails closed on the operator's mistake.
#[test]
#[serial(otlp_env)]
fn coverage_preflight_044_set_but_empty_federation_peers_env_is_rejected() {
    let env = EnvGuard::cleared();
    env.set("ORCH8_FEDERATION_PEERS", "");
    let Err(error) = preflight(&valid_config(), NodeRole::AllInOne) else {
        panic!("preflight must fail");
    };
    assert!(
        error
            .to_string()
            .contains("ORCH8_FEDERATION_PEERS is invalid"),
        "error was: {error}"
    );
}

// --- Federation peer parsing (pure) ---

fn peer_json() -> serde_json::Value {
    serde_json::json!({
        "id": "018f0000-0000-7000-8000-000000000001",
        "name": "peer-a",
        "trust_root_sha256": "a".repeat(64),
        "public_key": "unused-at-configuration-time",
        "endpoint": "https://peer.example",
        "allowed_tenants": ["tenant-a"],
        "revoked_at": null
    })
}

fn peers_result(peers: &serde_json::Value) -> Result<usize, String> {
    parse_federation_peers(&serde_json::to_string(peers).unwrap()).map(|peers| peers.len())
}

#[test]
fn coverage_federation_045_empty_peer_list_is_allowed() {
    assert_eq!(peers_result(&serde_json::json!([])).unwrap(), 0);
}

#[test]
fn coverage_federation_046_single_valid_peer_is_allowed() {
    assert_eq!(peers_result(&serde_json::json!([peer_json()])).unwrap(), 1);
}

#[test]
fn coverage_federation_047_malformed_json_is_rejected() {
    assert!(parse_federation_peers("{not json").is_err());
}

#[test]
fn coverage_federation_048_duplicate_peer_id_is_rejected() {
    let error = peers_result(&serde_json::json!([peer_json(), peer_json()])).unwrap_err();
    assert!(
        error.contains("duplicate federation peer id"),
        "error was: {error}"
    );
}

fn peers_with_count(count: usize) -> serde_json::Value {
    let peers: Vec<serde_json::Value> = (0..count)
        .map(|index| {
            let mut peer = peer_json();
            peer["id"] = serde_json::json!(format!("018f0000-0000-7000-8000-{index:012x}"));
            peer
        })
        .collect();
    serde_json::json!(peers)
}

#[test]
fn coverage_federation_049_128_peers_are_allowed() {
    assert_eq!(peers_result(&peers_with_count(128)).unwrap(), 128);
}

#[test]
fn coverage_federation_050_129_peers_are_rejected() {
    let error = peers_result(&peers_with_count(129)).unwrap_err();
    assert!(
        error.contains("at most 128 federation peers"),
        "error was: {error}"
    );
}

#[test]
fn coverage_federation_051_empty_peer_name_is_rejected() {
    let mut peer = peer_json();
    peer["name"] = serde_json::json!("");
    let error = peers_result(&serde_json::json!([peer])).unwrap_err();
    assert!(error.contains("invalid name"), "error was: {error}");
}

#[test]
fn coverage_federation_052_128_char_peer_name_is_allowed() {
    let mut peer = peer_json();
    peer["name"] = serde_json::json!("n".repeat(128));
    assert_eq!(peers_result(&serde_json::json!([peer])).unwrap(), 1);
}

#[test]
fn coverage_federation_053_129_char_peer_name_is_rejected() {
    let mut peer = peer_json();
    peer["name"] = serde_json::json!("n".repeat(129));
    let error = peers_result(&serde_json::json!([peer])).unwrap_err();
    assert!(error.contains("invalid name"), "error was: {error}");
}

#[test]
fn coverage_federation_054_plain_http_endpoint_is_rejected() {
    let mut peer = peer_json();
    peer["endpoint"] = serde_json::json!("http://peer.example");
    let error = peers_result(&serde_json::json!([peer])).unwrap_err();
    assert!(
        error.contains("bounded HTTPS endpoint"),
        "error was: {error}"
    );
}

#[test]
fn coverage_federation_055_2048_char_endpoint_is_allowed() {
    let mut peer = peer_json();
    peer["endpoint"] = serde_json::json!(format!("https://{}", "e".repeat(2040)));
    assert_eq!(peers_result(&serde_json::json!([peer])).unwrap(), 1);
}

#[test]
fn coverage_federation_056_2049_char_endpoint_is_rejected() {
    let mut peer = peer_json();
    peer["endpoint"] = serde_json::json!(format!("https://{}", "e".repeat(2041)));
    let error = peers_result(&serde_json::json!([peer])).unwrap_err();
    assert!(
        error.contains("bounded HTTPS endpoint"),
        "error was: {error}"
    );
}

#[test]
fn coverage_federation_057_empty_tenant_allowlist_is_rejected() {
    let mut peer = peer_json();
    peer["allowed_tenants"] = serde_json::json!([]);
    let error = peers_result(&serde_json::json!([peer])).unwrap_err();
    assert!(
        error.contains("invalid tenant allowlist"),
        "error was: {error}"
    );
}

#[test]
fn coverage_federation_058_256_tenants_are_allowed() {
    let mut peer = peer_json();
    peer["allowed_tenants"] =
        serde_json::json!((0..256).map(|i| format!("tenant-{i}")).collect::<Vec<_>>());
    assert_eq!(peers_result(&serde_json::json!([peer])).unwrap(), 1);
}

#[test]
fn coverage_federation_059_257_tenants_are_rejected() {
    let mut peer = peer_json();
    peer["allowed_tenants"] =
        serde_json::json!((0..257).map(|i| format!("tenant-{i}")).collect::<Vec<_>>());
    let error = peers_result(&serde_json::json!([peer])).unwrap_err();
    assert!(
        error.contains("invalid tenant allowlist"),
        "error was: {error}"
    );
}

#[test]
fn coverage_federation_060_short_trust_root_is_rejected() {
    let mut peer = peer_json();
    peer["trust_root_sha256"] = serde_json::json!("a".repeat(63));
    let error = peers_result(&serde_json::json!([peer])).unwrap_err();
    assert!(
        error.contains("invalid trust-root digest"),
        "error was: {error}"
    );
}

#[test]
fn coverage_federation_061_non_hex_trust_root_is_rejected() {
    let mut peer = peer_json();
    peer["trust_root_sha256"] = serde_json::json!(format!("{}{}", "g".repeat(63), "a"));
    let error = peers_result(&serde_json::json!([peer])).unwrap_err();
    assert!(
        error.contains("invalid trust-root digest"),
        "error was: {error}"
    );
}

#[test]
fn coverage_federation_062_uppercase_hex_trust_root_is_allowed() {
    let mut peer = peer_json();
    peer["trust_root_sha256"] = serde_json::json!("A1".repeat(32));
    assert_eq!(peers_result(&serde_json::json!([peer])).unwrap(), 1);
}

// --- Auth config validation ---

#[test]
fn coverage_auth_063_api_key_with_tenant_isolation_and_allowlist_passes() {
    assert!(validate_auth_config(true, true, false, "https://app.example.com").is_ok());
}

#[test]
fn coverage_auth_064_bare_cors_wildcard_with_api_key_is_rejected() {
    let Err(error) = validate_auth_config(true, true, false, "*") else {
        panic!("wildcard CORS with an API key must fail");
    };
    assert!(
        error
            .to_string()
            .contains("CORS origins cannot contain '*'")
    );
}

#[test]
fn coverage_auth_065_padded_cors_wildcard_with_api_key_is_rejected() {
    let Err(error) = validate_auth_config(true, true, false, "  *  ") else {
        panic!("padded wildcard CORS with an API key must fail");
    };
    assert!(
        error
            .to_string()
            .contains("CORS origins cannot contain '*'")
    );
}

#[test]
fn coverage_auth_066_wildcard_inside_cors_list_is_rejected() {
    let Err(error) =
        validate_auth_config(true, true, false, "https://a.example,*,https://b.example")
    else {
        panic!("wildcard inside the CORS list must fail");
    };
    assert!(
        error
            .to_string()
            .contains("CORS origins cannot contain '*'")
    );
}

/// The CORS check only applies when API-key auth is on; an unauthenticated
/// local-dev server may keep the wildcard.
#[test]
fn coverage_auth_067_cors_wildcard_without_api_key_is_allowed() {
    assert!(validate_auth_config(false, true, true, "*").is_ok());
}

#[test]
fn coverage_auth_068_no_key_and_no_insecure_flag_is_rejected() {
    let Err(error) = validate_auth_config(false, true, false, "") else {
        panic!("authless startup without --insecure-auth must fail");
    };
    assert!(error.to_string().contains("No API key configured"));
}

#[test]
fn coverage_auth_069_no_key_with_insecure_auth_flag_is_allowed() {
    assert!(validate_auth_config(false, true, true, "").is_ok());
}

/// `--insecure-auth` waives the API key requirement only — it must not also
/// waive tenant isolation when an API key IS configured.
#[test]
#[serial(otlp_env)]
fn coverage_auth_070_insecure_auth_does_not_waive_tenant_isolation() {
    let _env = EnvGuard::cleared();
    let Err(error) = validate_auth_config(true, false, true, "") else {
        panic!("disabled tenant isolation with an API key must fail");
    };
    assert!(error.to_string().contains("Tenant isolation is disabled"));
}

#[test]
#[serial(otlp_env)]
fn coverage_auth_071_no_tenant_isolation_requires_explicit_env_opt_in() {
    let env = EnvGuard::cleared();
    env.set("ORCH8_ALLOW_NO_TENANT_ISOLATION", "1");
    assert!(validate_auth_config(true, false, false, "").is_ok());
}

// --- Artifact store selection ---

#[test]
fn coverage_artifact_072_default_backend_is_disabled() {
    let config = EngineConfig::default();
    assert!(build_artifact_store(&config).unwrap().is_none());
}

#[test]
fn coverage_artifact_073_unknown_backend_is_rejected() {
    let mut config = EngineConfig::default();
    config.artifacts.backend = "bogus".into();
    let Err(error) = build_artifact_store(&config) else {
        panic!("unknown artifact backend must fail");
    };
    assert!(error.to_string().contains("unknown artifacts.backend"));
}

#[test]
fn coverage_artifact_074_local_backend_requires_absolute_path() {
    let mut config = EngineConfig::default();
    config.artifacts.backend = "local".into();
    config.artifacts.path = "relative/artifacts".into();
    let Err(error) = build_artifact_store(&config) else {
        panic!("relative artifact path must fail");
    };
    assert!(error.to_string().contains("must be absolute"));
}

#[test]
fn coverage_artifact_075_local_backend_accepts_absolute_path() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = EngineConfig::default();
    config.artifacts.backend = "local".into();
    config.artifacts.path = dir.path().join("artifacts").to_str().unwrap().to_string();
    assert!(build_artifact_store(&config).unwrap().is_some());
}

// --- Config file loading (reads the full env-override surface) ---

#[test]
#[serial(otlp_env)]
fn coverage_config_076_missing_file_falls_back_to_defaults() {
    let config = load_config("/definitely/missing/orch8.toml").unwrap();
    assert_eq!(config.api.grpc_addr, "127.0.0.1:50051");
    assert_eq!(config.api.http_addr, "127.0.0.1:8080");
}

#[test]
#[serial(otlp_env)]
fn coverage_config_077_toml_values_override_defaults() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    std::io::Write::write_all(
        &mut file,
        b"[api]\nhttp_addr = \"127.0.0.1:9999\"\ngrpc_addr = \"127.0.0.1:9998\"\n",
    )
    .unwrap();
    let config = load_config(file.path().to_str().unwrap()).unwrap();
    assert_eq!(config.api.http_addr, "127.0.0.1:9999");
    assert_eq!(config.api.grpc_addr, "127.0.0.1:9998");
}

#[test]
#[serial(otlp_env)]
fn coverage_config_078_malformed_toml_is_rejected() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    std::io::Write::write_all(&mut file, b"[api\nnot toml").unwrap();
    let Err(error) = load_config(file.path().to_str().unwrap()) else {
        panic!("malformed TOML must fail");
    };
    assert!(error.to_string().contains("Failed to parse config TOML"));
}

/// The size cap rejects files LARGER than 1 MiB; exactly 1 MiB still loads.
#[test]
#[serial(otlp_env)]
fn coverage_config_079_exactly_max_size_comment_toml_is_accepted() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let max = usize::try_from(MAX_CONFIG_FILE_BYTES).unwrap();
    let unit = b"# pad\n";
    let mut padding = unit.repeat(max / unit.len());
    padding.resize(max, b'#');
    std::io::Write::write_all(&mut file, &padding).unwrap();
    let config = load_config(file.path().to_str().unwrap()).unwrap();
    assert_eq!(config.api.http_addr, "127.0.0.1:8080");
}

// --- CLI parsing ---

#[test]
fn coverage_cli_080_default_config_path_is_orch8_toml() {
    let cli = Cli::parse_from(["orch8"]);
    assert_eq!(cli.config, "orch8.toml");
    assert!(!cli.insecure && !cli.insecure_auth && !cli.insecure_storage);
}

#[test]
fn coverage_cli_081_config_flag_overrides_path() {
    let cli = Cli::parse_from(["orch8", "--config", "/etc/orch8/prod.toml"]);
    assert_eq!(cli.config, "/etc/orch8/prod.toml");
}

#[test]
fn coverage_cli_082_insecure_flag_sets_only_the_shorthand() {
    let cli = Cli::parse_from(["orch8", "--insecure"]);
    assert!(cli.insecure);
    assert!(!cli.insecure_auth);
    assert!(!cli.insecure_storage);
}

#[test]
fn coverage_cli_083_specific_insecure_flags_combine() {
    let cli = Cli::parse_from(["orch8", "--insecure-auth", "--insecure-storage"]);
    assert!(cli.insecure_auth && cli.insecure_storage);
    assert!(!cli.insecure);
}

#[test]
fn coverage_cli_084_unknown_flag_is_rejected() {
    assert!(Cli::try_parse_from(["orch8", "--definitely-not-a-flag"]).is_err());
}

#[test]
fn coverage_cli_085_missing_config_value_is_rejected() {
    assert!(Cli::try_parse_from(["orch8", "--config"]).is_err());
}

/// The CORS wildcard check trims each comma-separated item, so a padded
/// wildcard list item is still caught (complements the bare and unspaced
/// list cases in 065/066).
#[test]
fn coverage_auth_086_spaced_wildcard_item_in_cors_list_is_rejected() {
    let Err(error) =
        validate_auth_config(true, true, false, "https://a.example, * ,https://b.example")
    else {
        panic!("spaced wildcard list item must fail");
    };
    assert!(
        error
            .to_string()
            .contains("CORS origins cannot contain '*'")
    );
}
