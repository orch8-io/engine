//! Coverage tests for the secure verified bootstrap command.
//!
//! Count contract: 18 independently named unit tests.

use super::*;

fn config_with_addr(addr: &str) -> orch8_types::config::EngineConfig {
    let mut config = orch8_types::config::EngineConfig::default();
    config.api.http_addr = addr.into();
    config
}

macro_rules! readiness_case {
    ($name:ident, $addr:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!(readiness_url(&config_with_addr($addr)).unwrap(), $expected);
        }
    };
}

readiness_case!(
    coverage_bootstrap_001_wildcard_v4_probes_loopback,
    "0.0.0.0:8080",
    "http://127.0.0.1:8080/health/ready"
);
readiness_case!(
    coverage_bootstrap_002_wildcard_v6_probes_loopback,
    "[::]:8080",
    "http://[::1]:8080/health/ready"
);
readiness_case!(
    coverage_bootstrap_003_explicit_loopback_v4_is_used_verbatim,
    "127.0.0.1:9000",
    "http://127.0.0.1:9000/health/ready"
);
readiness_case!(
    coverage_bootstrap_004_specific_v4_address_is_used_verbatim,
    "192.168.1.50:9000",
    "http://192.168.1.50:9000/health/ready"
);
readiness_case!(
    coverage_bootstrap_005_specific_v6_address_is_bracketed,
    "[2001:db8::1]:9090",
    "http://[2001:db8::1]:9090/health/ready"
);
readiness_case!(
    coverage_bootstrap_006_port_zero_is_preserved,
    "0.0.0.0:0",
    "http://127.0.0.1:0/health/ready"
);
readiness_case!(
    coverage_bootstrap_007_max_port_is_preserved,
    "0.0.0.0:65535",
    "http://127.0.0.1:65535/health/ready"
);
readiness_case!(
    coverage_bootstrap_008_unspecified_v6_nonstandard_port,
    "[::]:1234",
    "http://[::1]:1234/health/ready"
);

macro_rules! readiness_error_case {
    ($name:ident, $addr:expr) => {
        #[test]
        fn $name() {
            let error = readiness_url(&config_with_addr($addr))
                .err()
                .expect(concat!("address ", $addr, " must be rejected"));
            assert!(
                format!("{error:#}").contains("api.http_addr is not a socket address"),
                "{error:#}"
            );
        }
    };
}

readiness_error_case!(
    coverage_bootstrap_009_garbage_address_is_rejected,
    "not-an-addr"
);
readiness_error_case!(coverage_bootstrap_010_missing_port_is_rejected, "127.0.0.1");
readiness_error_case!(
    coverage_bootstrap_011_unbracketed_ipv6_is_rejected,
    "::1:8080"
);
readiness_error_case!(
    coverage_bootstrap_012_hostname_is_not_a_socket_address,
    "localhost:8080"
);

fn bootstrap_cmd(dir: &std::path::Path, server_bin: &str) -> BootstrapCmd {
    BootstrapCmd {
        dir: dir.to_path_buf(),
        template: "default".into(),
        server_bin: server_bin.into(),
        timeout_secs: 1,
    }
}

#[tokio::test]
async fn coverage_bootstrap_013_empty_keys_fail_before_spawning() {
    let dir = tempfile::tempdir().unwrap();
    // A default-shaped config: valid, but with no generated secrets.
    std::fs::write(dir.path().join("orch8.toml"), "").unwrap();
    let error = run(bootstrap_cmd(dir.path(), "definitely-missing-server"))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("bootstrap requires generated API and encryption keys"),
        "{error:#}"
    );
}

#[tokio::test]
async fn coverage_bootstrap_014_unparseable_config_is_a_read_error() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("orch8.toml"), "[[[not toml").unwrap();
    let error = run(bootstrap_cmd(dir.path(), "definitely-missing-server"))
        .await
        .unwrap_err();
    let rendered = format!("{error:#}");
    assert!(rendered.contains("parse"), "{rendered}");
}

#[tokio::test]
async fn coverage_bootstrap_015_gateway_role_without_secrets_fails_preflight() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("orch8.toml"),
        "[node]\nrole = \"gateway\"\n",
    )
    .unwrap();
    let error = run(bootstrap_cmd(dir.path(), "definitely-missing-server"))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("bootstrap preflight failed"),
        "{error:#}"
    );
}

#[tokio::test]
async fn coverage_bootstrap_016_malformed_encryption_key_fails_preflight() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        "[engine]\nencryption_key = \"abcd\"\n\n[api]\napi_key = \"{}\"\n",
        "a".repeat(64)
    );
    std::fs::write(dir.path().join("orch8.toml"), toml).unwrap();
    let error = run(bootstrap_cmd(dir.path(), "definitely-missing-server"))
        .await
        .unwrap_err();
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("bootstrap preflight failed"),
        "{rendered}"
    );
    assert!(rendered.contains("encryption_key"), "{rendered}");
}

#[tokio::test]
async fn coverage_bootstrap_017_valid_scaffold_fails_at_missing_server_binary() {
    let dir = tempfile::tempdir().unwrap();
    super::super::init::run(dir.path().to_str().unwrap(), "default").unwrap();
    let error = run(bootstrap_cmd(dir.path(), "definitely-missing-orch8-server"))
        .await
        .unwrap_err();
    let rendered = format!("{error:#}");
    // Getting here proves preflight + key checks passed for a real scaffold.
    assert!(
        rendered.contains("start definitely-missing-orch8-server"),
        "{rendered}"
    );
}

#[tokio::test]
async fn coverage_bootstrap_018_empty_directory_is_scaffolded_then_spawn_fails() {
    let base = tempfile::tempdir().unwrap();
    let project = base.path().join("new-project");
    let error = run(bootstrap_cmd(&project, "definitely-missing-orch8-server"))
        .await
        .unwrap_err();
    // The missing config was scaffolded by init before the spawn attempt.
    assert!(project.join("orch8.toml").exists());
    assert!(project.join("sequence.json").exists());
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("start definitely-missing-orch8-server"),
        "{rendered}"
    );
}
