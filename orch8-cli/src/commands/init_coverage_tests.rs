//! Coverage tests for secure project scaffolding (`orch8 init`).
//!
//! Count contract: 14 independently named unit tests.

use super::*;

#[test]
fn coverage_init_001_secret_hex_is_64_chars() {
    assert_eq!(generate_secret_hex().len(), 64);
}

#[test]
fn coverage_init_002_secret_hex_is_lowercase_hex_only() {
    let secret = generate_secret_hex();
    assert!(
        secret
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "unexpected character in {secret}"
    );
}

#[test]
fn coverage_init_003_consecutive_secrets_differ() {
    assert_ne!(generate_secret_hex(), generate_secret_hex());
}

#[test]
fn coverage_init_004_sixteen_generated_secrets_are_all_unique() {
    let secrets: std::collections::BTreeSet<String> =
        (0..16).map(|_| generate_secret_hex()).collect();
    assert_eq!(secrets.len(), 16);
}

#[test]
fn coverage_init_005_distinct_scaffolds_get_distinct_api_keys() {
    let one = tempfile::tempdir().unwrap();
    let two = tempfile::tempdir().unwrap();
    run(one.path().to_str().unwrap(), "default").unwrap();
    run(two.path().to_str().unwrap(), "default").unwrap();
    let a = fs::read_to_string(one.path().join("orch8.toml")).unwrap();
    let b = fs::read_to_string(two.path().join("orch8.toml")).unwrap();
    assert_ne!(a, b, "independent scaffolds must not share secrets");
}

#[test]
fn coverage_init_006_scaffold_leaves_no_template_placeholders() {
    let dir = tempfile::tempdir().unwrap();
    run(dir.path().to_str().unwrap(), "default").unwrap();
    let toml = fs::read_to_string(dir.path().join("orch8.toml")).unwrap();
    assert!(!toml.contains("{api_key}"), "{toml}");
    assert!(!toml.contains("{encryption_key}"), "{toml}");
}

#[test]
fn coverage_init_007_scaffolded_keys_appear_verbatim_in_toml() {
    let dir = tempfile::tempdir().unwrap();
    run(dir.path().to_str().unwrap(), "default").unwrap();
    let toml_text = fs::read_to_string(dir.path().join("orch8.toml")).unwrap();
    let cfg: orch8_types::config::EngineConfig = toml::from_str(&toml_text).unwrap();
    assert!(toml_text.contains(&format!("api_key = \"{}\"", cfg.api.api_key.expose())));
    assert!(toml_text.contains(&format!(
        "encryption_key = \"{}\"",
        cfg.engine.encryption_key.expose()
    )));
}

#[test]
fn coverage_init_008_reinit_never_rotates_existing_keys() {
    let dir = tempfile::tempdir().unwrap();
    run(dir.path().to_str().unwrap(), "default").unwrap();
    let first = fs::read_to_string(dir.path().join("orch8.toml")).unwrap();
    run(dir.path().to_str().unwrap(), "default").unwrap();
    let second = fs::read_to_string(dir.path().join("orch8.toml")).unwrap();
    assert_eq!(
        first, second,
        "re-running init must keep the original secrets"
    );
}

#[cfg(unix)]
#[test]
fn coverage_init_009_preexisting_config_permissions_are_not_tightened() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("orch8.toml");
    fs::write(&config, "# my existing config\n").unwrap();
    fs::set_permissions(&config, fs::Permissions::from_mode(0o644)).unwrap();
    run(dir.path().to_str().unwrap(), "default").unwrap();
    assert_eq!(
        fs::read_to_string(&config).unwrap(),
        "# my existing config\n",
        "existing config content must be preserved"
    );
    assert_eq!(
        fs::metadata(&config).unwrap().permissions().mode() & 0o777,
        0o644,
        "permissions of a pre-existing config are the operator's responsibility"
    );
}

#[cfg(unix)]
#[test]
fn coverage_init_010_fresh_config_is_owner_only() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    run(dir.path().to_str().unwrap(), "default").unwrap();
    let mode = fs::metadata(dir.path().join("orch8.toml"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
}

#[test]
fn coverage_init_011_docker_compose_scaffold_mentions_postgres() {
    let dir = tempfile::tempdir().unwrap();
    run(dir.path().to_str().unwrap(), "default").unwrap();
    let compose = fs::read_to_string(dir.path().join("docker-compose.yml")).unwrap();
    assert!(compose.contains("postgres:16-alpine"), "{compose}");
    assert!(compose.contains("ghcr.io/orch8-io/engine"), "{compose}");
}

#[test]
fn coverage_init_012_write_if_absent_creates_missing_file() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("fresh.txt");
    write_if_absent(&target, "hello").unwrap();
    assert_eq!(fs::read_to_string(&target).unwrap(), "hello");
}

#[test]
fn coverage_init_013_write_if_absent_preserves_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("kept.txt");
    fs::write(&target, "original").unwrap();
    write_if_absent(&target, "replacement").unwrap();
    assert_eq!(fs::read_to_string(&target).unwrap(), "original");
}

#[test]
fn coverage_init_014_every_registered_template_scaffolds_parseable_config() {
    for template in templates::TEMPLATES {
        let dir = tempfile::tempdir().unwrap();
        run(dir.path().to_str().unwrap(), template.name).unwrap();
        let toml_text = fs::read_to_string(dir.path().join("orch8.toml")).unwrap();
        let cfg: orch8_types::config::EngineConfig =
            toml::from_str(&toml_text).unwrap_or_else(|e| {
                panic!("template {} scaffolded invalid config: {e}", template.name)
            });
        assert_eq!(cfg.api.api_key.expose().len(), 64);
        assert_eq!(cfg.engine.encryption_key.expose().len(), 64);
        let sequence = fs::read_to_string(dir.path().join("sequence.json")).unwrap();
        assert_eq!(sequence, template.json);
    }
}
