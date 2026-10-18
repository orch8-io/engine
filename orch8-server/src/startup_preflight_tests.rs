//! Startup must fail closed: the aggregate preflight, federation peer
//! parsing, auth-config validation, artifact store selection and config file
//! loading reject every misconfiguration with an error naming the offending
//! setting, before any listener or engine task is spawned.
//!
//! Env hygiene: `automatic_startup_preflight`, `validate_auth_config`, and
//! `load_config` read process-global env vars, so every test that calls them
//! runs under `#[serial(otlp_env)]` (the same key the env tests in `main.rs`
//! use) and clears the relevant variables through [`EnvGuard`]. That makes
//! the `unsafe` env blocks below sound: no test in this binary can observe a
//! half-mutated environment.

use super::*;
use serial_test::serial;

const ENV_VARS: [&str; 5] = [
    "ORCH8_FEDERATION_PEERS",
    "ORCH8_MOBILE_SYNC_ENABLED",
    "ORCH8_ALLOW_NO_TENANT_ISOLATION",
    "ORCH8_HTTP_ADDR",
    "ORCH8_GRPC_ADDR",
];

/// Captures, clears, and restores the startup-relevant environment.
struct EnvGuard {
    saved: [Option<String>; 5],
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

fn mtls_identities(entries: &[(&str, &str)]) -> String {
    let body = entries
        .iter()
        .map(|(fingerprint, tenant)| {
            format!(r#""{fingerprint}":{{"tenant_id":"{tenant}","identity":"i"}}"#)
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{body}}}")
}

fn managed_control(config: &mut EngineConfig, runtime_id: &str) {
    config.node.role = NodeRole::Executor;
    config.node.managed_control_endpoint = "https://control.example.com".into();
    config.node.managed_control_api_key = "key".into();
    config.node.managed_control_tenant_id = "tenant".into();
    config.node.managed_control_worker_id = "worker".into();
    config.node.managed_control_runtime_id = runtime_id.into();
}

type Mutation = fn(&mut EngineConfig);

#[test]
#[serial(otlp_env)]
fn preflight_rejects_each_misconfiguration_with_a_named_error() {
    let _env = EnvGuard::cleared();
    let cases: [(&str, NodeRole, Mutation, &str); 15] = [
        (
            "empty database url",
            NodeRole::AllInOne,
            |c| c.database.url = "".into(),
            "database.url must be configured for startup",
        ),
        (
            "garbage http addr",
            NodeRole::AllInOne,
            |c| c.api.http_addr = "not-an-address".into(),
            "api.http_addr is invalid",
        ),
        (
            "out-of-range grpc addr on the full surface",
            NodeRole::AllInOne,
            |c| c.api.grpc_addr = "999.999.999.999:1".into(),
            "api.grpc_addr is invalid",
        ),
        (
            "grpc addr is still checked on the executor surface",
            NodeRole::Executor,
            |c| c.api.grpc_addr = "localhost:nope".into(),
            "api.grpc_addr is invalid",
        ),
        (
            "cert and key without client CA",
            NodeRole::AllInOne,
            |c| {
                c.api.grpc_tls_cert_path = "/tmp/server.crt".into();
                c.api.grpc_tls_key_path = "/tmp/server.key".into();
            },
            "certificate, private key, and client CA",
        ),
        (
            "partial TLS is validated even when gRPC is disabled",
            NodeRole::Edge,
            |c| c.api.grpc_tls_client_ca_path = "/tmp/ca.crt".into(),
            "certificate, private key, and client CA",
        ),
        (
            "complete TLS bundle of missing files",
            NodeRole::AllInOne,
            |c| {
                c.api.grpc_tls_cert_path = "/definitely/missing/server.crt".into();
                c.api.grpc_tls_key_path = "/definitely/missing/server.key".into();
                c.api.grpc_tls_client_ca_path = "/definitely/missing/ca.crt".into();
            },
            "cannot be read",
        ),
        (
            "blank mTLS tenant after trimming",
            NodeRole::AllInOne,
            |c| c.api.grpc_mtls_identities = mtls_identities(&[(&"b".repeat(64), "  ")]),
            "api.grpc_mtls_identities is invalid: workload identity tenant_id and identity must contain 1..=128 bytes",
        ),
        (
            "mTLS fingerprints that collide after case and colon normalization",
            NodeRole::AllInOne,
            |c| {
                let plain = "ab".repeat(32);
                let colonized = vec!["AB"; 32].join(":");
                c.api.grpc_mtls_identities = mtls_identities(&[(&plain, "t"), (&colonized, "t")]);
            },
            "api.grpc_mtls_identities is invalid: duplicate workload certificate fingerprint",
        ),
        (
            "bare host:port otlp endpoint",
            NodeRole::AllInOne,
            |c| c.telemetry.otlp_endpoint = "collector:4317".into(),
            "telemetry.otlp_endpoint must be an absolute HTTP(S) URI",
        ),
        (
            "non-HTTP otlp scheme",
            NodeRole::AllInOne,
            |c| c.telemetry.otlp_endpoint = "ftp://collector:4317".into(),
            "telemetry.otlp_endpoint must be an absolute HTTP(S) URI",
        ),
        (
            "managed control without credentials",
            NodeRole::Executor,
            |c| {
                c.node.role = NodeRole::Executor;
                c.node.managed_control_endpoint = "https://control.example.com".into();
            },
            "managed control requires API key, tenant, worker id, and UUID runtime id",
        ),
        (
            "managed control with a non-UUID runtime id",
            NodeRole::Executor,
            |c| managed_control(c, "not-a-uuid"),
            "UUID runtime id",
        ),
        (
            "shared http and grpc socket",
            NodeRole::Executor,
            |c| {
                c.api.http_addr = "0.0.0.0:9000".into();
                c.api.grpc_addr = "0.0.0.0:9000".into();
            },
            "api.http_addr and api.grpc_addr must use different sockets",
        ),
        (
            "gateway without an api key surfaces config.validate errors",
            NodeRole::Gateway,
            |c| c.node.role = NodeRole::Gateway,
            "node.role=gateway requires api.api_key",
        ),
    ];
    for (case, role, mutate, expected) in cases {
        let mut config = valid_config();
        mutate(&mut config);

        let error = preflight(&config, role).expect_err(case).to_string();

        assert!(error.contains(expected), "{case}: error was {error}");
    }
}

#[test]
#[serial(otlp_env)]
fn preflight_accepts_valid_configurations_and_skips_grpc_checks_without_grpc() {
    let _env = EnvGuard::cleared();
    let cases: [(&str, NodeRole, Mutation); 6] = [
        ("defaults on all-in-one", NodeRole::AllInOne, |_| {}),
        ("defaults on control", NodeRole::Control, |_| {}),
        ("edge ignores an invalid grpc addr", NodeRole::Edge, |c| {
            c.api.grpc_addr = "not-an-address".into();
        }),
        (
            "edge ignores a shared http/grpc socket",
            NodeRole::Edge,
            |c| {
                c.api.grpc_addr = c.api.http_addr.clone();
            },
        ),
        ("absolute http otlp endpoint", NodeRole::AllInOne, |c| {
            c.telemetry.otlp_endpoint = "http://collector:4317".into();
        }),
        ("managed control on an executor", NodeRole::Executor, |c| {
            managed_control(c, &uuid::Uuid::new_v4().to_string());
        }),
    ];
    for (case, role, mutate) in cases {
        let mut config = valid_config();
        mutate(&mut config);

        let result = preflight(&config, role);

        assert!(result.is_ok(), "{case}: {result:?}");
    }
}

// The TLS file-check tests keep their tempdir in a binding that outlives
// the preflight call, so the directory is removed on drop.
fn tls_bundle(config: &mut EngineConfig, cert: &str, key: &str, ca: &str) {
    config.api.grpc_tls_cert_path = cert.into();
    config.api.grpc_tls_key_path = key.into();
    config.api.grpc_tls_client_ca_path = ca.into();
}

#[test]
#[serial(otlp_env)]
fn tls_paths_must_name_non_empty_regular_files() {
    let _env = EnvGuard::cleared();
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, b"").unwrap();
    let empty = empty.to_str().unwrap().to_string();
    let directory = dir.path().to_str().unwrap().to_string();
    for (case, path) in [("empty file", empty), ("directory", directory)] {
        let mut config = valid_config();
        tls_bundle(&mut config, &path, &path, &path);

        let error = preflight(&config, NodeRole::AllInOne)
            .expect_err(case)
            .to_string();

        assert!(
            error.contains("must reference a non-empty regular file"),
            "{case}: error was {error}"
        );
    }
}

#[test]
#[serial(otlp_env)]
fn a_single_unreadable_tls_file_is_reported_by_its_field_name() {
    let _env = EnvGuard::cleared();
    let dir = tempfile::tempdir().unwrap();
    let pem = dir.path().join("real.pem");
    std::fs::write(&pem, b"pem-bytes").unwrap();
    let pem = pem.to_str().unwrap().to_string();
    let mut config = valid_config();
    tls_bundle(&mut config, &pem, "/definitely/missing/server.key", &pem);

    let error = preflight(&config, NodeRole::AllInOne).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("api.grpc_tls_key_path cannot be read"),
        "error was: {error}"
    );
}

#[test]
#[serial(otlp_env)]
fn complete_tls_bundle_of_readable_files_passes_preflight() {
    let _env = EnvGuard::cleared();
    let dir = tempfile::tempdir().unwrap();
    let pem = dir.path().join("bundle.pem");
    std::fs::write(&pem, b"pem-bytes").unwrap();
    let pem = pem.to_str().unwrap().to_string();
    let mut config = valid_config();
    tls_bundle(&mut config, &pem, &pem, &pem);

    let result = preflight(&config, NodeRole::AllInOne);

    assert!(result.is_ok(), "{result:?}");
}

/// The aggregate report has a fixed shape: a header line, then one
/// `"\n  - "`-joined line per failure.
#[test]
#[serial(otlp_env)]
fn preflight_report_lists_one_bullet_per_failure_under_a_header() {
    let _env = EnvGuard::cleared();
    let mut config = valid_config();
    config.database.url = "".into();
    config.api.http_addr = "garbage".into();

    let message = preflight(&config, NodeRole::AllInOne)
        .unwrap_err()
        .to_string();

    assert!(
        message.starts_with("automatic startup preflight failed:\n  - "),
        "message was: {message}"
    );
    assert_eq!(message.matches("\n  - ").count(), 2, "expected 2 bullets");
}

/// `config.validate()` errors are aggregated BEFORE the preflight's own
/// checks, so operators see role errors ahead of startup-specific ones.
#[test]
#[serial(otlp_env)]
fn config_validation_errors_are_reported_before_startup_errors() {
    let _env = EnvGuard::cleared();
    let mut config = valid_config();
    config.node.role = NodeRole::Gateway;
    config.database.url = "".into();

    let message = preflight(&config, NodeRole::Gateway)
        .unwrap_err()
        .to_string();

    let validate_index = message
        .find("node.role=gateway requires api.api_key")
        .unwrap();
    let startup_index = message.find("database.url must be configured").unwrap();
    assert!(validate_index < startup_index, "message was: {message}");
}

/// Mobile sync needs the full API, so only all-in-one and control nodes may
/// enable it; only "1"/"true" enable it.
#[test]
#[serial(otlp_env)]
fn mobile_sync_is_only_allowed_on_api_serving_roles() {
    let env = EnvGuard::cleared();
    let cases = [
        (NodeRole::AllInOne, "1", true),
        (NodeRole::Control, "true", true),
        (NodeRole::Executor, "1", false),
        (NodeRole::Edge, "true", false),
        (NodeRole::Gateway, "1", false),
        (NodeRole::Executor, "yes", true),
    ];
    for (role, value, allowed) in cases {
        env.set("ORCH8_MOBILE_SYNC_ENABLED", value);

        let result = preflight(&valid_config(), role);

        assert_eq!(
            result.is_ok(),
            allowed,
            "{role:?} with {value:?}: {result:?}"
        );
        if let Err(error) = result {
            assert!(
                error
                    .to_string()
                    .contains("ORCH8_MOBILE_SYNC_ENABLED requires an all_in_one or control node"),
                "{role:?}: {error}"
            );
        }
    }
}

/// A set-but-empty peers variable is invalid JSON, not "no peers": startup
/// fails closed on the operator's mistake.
#[test]
#[serial(otlp_env)]
fn invalid_or_empty_federation_peers_env_fails_preflight() {
    let env = EnvGuard::cleared();
    for value in ["not json", ""] {
        env.set("ORCH8_FEDERATION_PEERS", value);

        let error = preflight(&valid_config(), NodeRole::AllInOne)
            .expect_err(value)
            .to_string();

        assert!(
            error.contains("ORCH8_FEDERATION_PEERS is invalid"),
            "{value:?}: error was {error}"
        );
    }
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

fn peer_with(field: &str, value: serde_json::Value) -> serde_json::Value {
    let mut peer = peer_json();
    peer[field] = value;
    serde_json::json!([peer])
}

fn tenants(count: usize) -> serde_json::Value {
    serde_json::json!(
        (0..count)
            .map(|i| format!("tenant-{i}"))
            .collect::<Vec<_>>()
    )
}

fn parse(peers: &serde_json::Value) -> Result<usize, String> {
    parse_federation_peers(&peers.to_string()).map(|peers| peers.len())
}

#[test]
fn federation_peers_at_each_limit_are_accepted() {
    let cases = [
        ("no peers", serde_json::json!([]), 0),
        ("128 peers", peers_with_count(128), 128),
        (
            "128-char name",
            peer_with("name", serde_json::json!("n".repeat(128))),
            1,
        ),
        (
            "2048-char endpoint",
            peer_with(
                "endpoint",
                serde_json::json!(format!("https://{}", "e".repeat(2040))),
            ),
            1,
        ),
        ("256 tenants", peer_with("allowed_tenants", tenants(256)), 1),
    ];
    for (case, peers, expected) in cases {
        assert_eq!(parse(&peers), Ok(expected), "{case}");
    }
}

#[test]
fn federation_peers_beyond_each_limit_are_rejected_with_the_rule_broken() {
    let cases = [
        (
            "duplicate id",
            serde_json::json!([peer_json(), peer_json()]),
            "duplicate federation peer id",
        ),
        (
            "129 peers",
            peers_with_count(129),
            "at most 128 federation peers",
        ),
        (
            "empty name",
            peer_with("name", serde_json::json!("")),
            "invalid name",
        ),
        (
            "129-char name",
            peer_with("name", serde_json::json!("n".repeat(129))),
            "invalid name",
        ),
        (
            "2049-char endpoint",
            peer_with(
                "endpoint",
                serde_json::json!(format!("https://{}", "e".repeat(2041))),
            ),
            "bounded HTTPS endpoint",
        ),
        (
            "empty tenant allowlist",
            peer_with("allowed_tenants", serde_json::json!([])),
            "invalid tenant allowlist",
        ),
        (
            "257 tenants",
            peer_with("allowed_tenants", tenants(257)),
            "invalid tenant allowlist",
        ),
        (
            "63-char trust root",
            peer_with("trust_root_sha256", serde_json::json!("a".repeat(63))),
            "invalid trust-root digest",
        ),
        (
            "non-hex trust root",
            peer_with(
                "trust_root_sha256",
                serde_json::json!(format!("{}a", "g".repeat(63))),
            ),
            "invalid trust-root digest",
        ),
    ];
    for (case, peers, expected) in cases {
        let error = parse(&peers).expect_err(case);
        assert!(error.contains(expected), "{case}: error was {error}");
    }
}

/// The envelope verifier compares the configured trust root against a
/// lowercase `hex_sha256`, so a peer that passes preflight must carry its
/// trust root in that form (or be rejected up front); an accepted
/// uppercase digest would make every envelope from that peer fail.
#[test]
fn accepted_federation_trust_roots_match_the_verifiers_lowercase_digest() {
    let peers = peer_with("trust_root_sha256", serde_json::json!("A1".repeat(32)));

    let accepted = parse_federation_peers(&peers.to_string()).unwrap_or_default();

    assert!(
        accepted
            .iter()
            .all(|peer| peer.trust_root_sha256 == peer.trust_root_sha256.to_ascii_lowercase()),
        "accepted a trust root the verifier can never match"
    );
}

// --- Auth config validation ---

/// The CORS wildcard check trims each comma-separated item, so padded
/// wildcards are still caught.
#[test]
fn padded_cors_wildcards_are_rejected_when_an_api_key_is_configured() {
    for origins in ["  *  ", "https://a.example, * ,https://b.example"] {
        let error = validate_auth_config(true, true, false, origins).expect_err(origins);
        assert!(
            error
                .to_string()
                .contains("CORS origins cannot contain '*'"),
            "{origins:?}: {error}"
        );
    }
}

/// The CORS check only applies when API-key auth is on; an unauthenticated
/// local-dev server may keep the wildcard.
#[test]
fn cors_wildcard_is_allowed_on_an_unauthenticated_dev_server() {
    assert!(validate_auth_config(false, true, true, "*").is_ok());
}

/// `--insecure-auth` waives the API key requirement only: it must not also
/// waive tenant isolation when an API key IS configured.
#[test]
#[serial(otlp_env)]
fn insecure_auth_does_not_waive_tenant_isolation() {
    let _env = EnvGuard::cleared();

    let error = validate_auth_config(true, false, true, "").unwrap_err();

    assert!(error.to_string().contains("Tenant isolation is disabled"));
}

#[test]
#[serial(otlp_env)]
fn disabling_tenant_isolation_requires_the_explicit_env_opt_in() {
    let env = EnvGuard::cleared();
    env.set("ORCH8_ALLOW_NO_TENANT_ISOLATION", "1");

    assert!(validate_auth_config(true, false, false, "").is_ok());
}

// --- Artifact store selection ---

#[test]
fn artifact_store_is_disabled_by_default() {
    let config = EngineConfig::default();

    assert!(build_artifact_store(&config).unwrap().is_none());
}

#[test]
fn local_artifact_store_requires_an_absolute_path() {
    let mut config = EngineConfig::default();
    config.artifacts.backend = "local".into();
    config.artifacts.path = "relative/artifacts".into();

    let Err(error) = build_artifact_store(&config) else {
        panic!("relative artifact path must fail");
    };

    assert!(error.to_string().contains("must be absolute"));
}

#[test]
fn local_artifact_store_is_built_for_an_absolute_path() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = EngineConfig::default();
    config.artifacts.backend = "local".into();
    config.artifacts.path = dir.path().join("artifacts").to_str().unwrap().to_string();

    assert!(build_artifact_store(&config).unwrap().is_some());
}

// --- Config file loading (reads the full env-override surface) ---

#[test]
#[serial(otlp_env)]
fn missing_config_file_falls_back_to_default_listen_addresses() {
    let _env = EnvGuard::cleared();

    let config = load_config("/definitely/missing/orch8.toml").unwrap();

    assert_eq!(config.api.grpc_addr, "127.0.0.1:50051");
    assert_eq!(config.api.http_addr, "127.0.0.1:8080");
}

#[test]
#[serial(otlp_env)]
fn config_file_values_override_defaults() {
    let _env = EnvGuard::cleared();
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
fn malformed_config_toml_is_rejected() {
    let _env = EnvGuard::cleared();
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
fn config_file_of_exactly_the_size_cap_is_accepted() {
    let _env = EnvGuard::cleared();
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let max = usize::try_from(MAX_CONFIG_FILE_BYTES).unwrap();
    let unit = b"# pad\n";
    let mut padding = unit.repeat(max / unit.len());
    padding.resize(max, b'#');
    std::io::Write::write_all(&mut file, &padding).unwrap();

    let config = load_config(file.path().to_str().unwrap()).unwrap();

    assert_eq!(config.api.http_addr, "127.0.0.1:8080");
}
