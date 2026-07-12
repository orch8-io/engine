//! End-to-end lifecycle tests for signed workflow packages.
//!
//! Exercises the full in-process pipeline: build → serialize to JSON (the
//! wire format) → deserialize → verify → trust-check → upgrade-check →
//! extract sequences/contracts — plus a realistic install-decision fixture
//! mirroring `orch8 package install` ordering.

use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{TimeZone, Utc};
use ed25519_dalek::SigningKey;
use rand_core::OsRng;
use serde_json::json;

use orch8_publisher::package::{
    PACKAGE_FORMAT_VERSION, PackageError, PackageManifest, PackageRequirements, SignedPackage,
    TrustLevel, TrustPolicy, build_package, check_trust, check_upgrade, content_hash,
    contract_files, install_namespace, parse_version, sequence_files, validate_package_name,
    verify_package,
};
use orch8_types::contract::{
    CONTRACT_SCHEMA_VERSION, ContractCase, ContractSuite, Expectations, MockDef, MockPolicy,
    UnmockedHandlerPolicy,
};
use orch8_types::ids::{BlockId, Namespace, SequenceId, TenantId};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, SequenceStatus, StepDef};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn key() -> SigningKey {
    SigningKey::generate(&mut OsRng)
}

fn step(id: &str, handler: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.to_string(),
        params: json!({}),
        delay: None,
        retry: None,
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }))
}

/// Deterministic `SequenceId` derived from the sequence name so identical
/// package content always produces identical bytes (reproducible builds).
fn fixed_sequence_id(name: &str) -> SequenceId {
    let tag = u32::from(name.bytes().fold(0u8, u8::wrapping_add));
    serde_json::from_value(json!(format!("00000000-0000-7000-8000-0000{tag:08x}"))).unwrap()
}

fn sequence_definition(name: &str, handlers: &[&str]) -> SequenceDefinition {
    SequenceDefinition {
        id: fixed_sequence_id(name),
        tenant_id: TenantId::new("pkg_tenant").unwrap(),
        namespace: Namespace::new("default"),
        name: name.to_string(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: handlers
            .iter()
            .enumerate()
            .map(|(i, h)| step(&format!("step{i}"), h))
            .collect(),
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap(),
    }
}

fn sequence_json(name: &str, handlers: &[&str]) -> String {
    serde_json::to_string(&sequence_definition(name, handlers)).unwrap()
}

fn contract_suite(sequence_name: &str, handler: &str, case_names: &[&str]) -> ContractSuite {
    ContractSuite {
        schema_version: CONTRACT_SCHEMA_VERSION,
        sequence_name: Some(sequence_name.to_string()),
        sequence_version: Some(1),
        unmocked_handlers: UnmockedHandlerPolicy::default(),
        cases: case_names
            .iter()
            .map(|n| ContractCase {
                name: (*n).to_string(),
                description: None,
                input: json!({"amount": 100}),
                config: None,
                mocks: vec![MockDef {
                    handler: Some(handler.to_string()),
                    block: None,
                    policy: MockPolicy::Success { output: json!({"ok": true}) },
                }],
                signals: vec![],
                expect: Expectations::default(),
                max_logical_duration_ms: None,
                max_ticks: None,
            })
            .collect(),
    }
}

fn contract_json(sequence_name: &str, handler: &str, case_names: &[&str]) -> String {
    serde_json::to_string(&contract_suite(sequence_name, handler, case_names)).unwrap()
}

fn manifest(name: &str, version: &str) -> PackageManifest {
    PackageManifest {
        name: name.into(),
        version: version.into(),
        description: "checkout flows".into(),
        publisher: "Acme".into(),
        requirements: PackageRequirements {
            handlers: vec!["charge_card".into(), "send_email".into()],
            credentials: vec!["stripe_key".into()],
            plugins: vec![],
            queues: vec!["billing".into()],
            min_engine_version: Some("0.6.0".into()),
        },
        created_at: Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap(),
    }
}

/// A realistic multi-file package: 3 sequences, 2 contract suites, README.
fn package_files() -> BTreeMap<String, String> {
    let mut f = BTreeMap::new();
    f.insert(
        "sequences/checkout.json".to_string(),
        sequence_json("checkout", &["charge_card", "send_email"]),
    );
    f.insert(
        "sequences/refund.json".to_string(),
        sequence_json("refund", &["charge_card"]),
    );
    f.insert(
        "sequences/receipt.json".to_string(),
        sequence_json("receipt", &["send_email"]),
    );
    f.insert(
        "contracts/checkout.contracts.json".to_string(),
        contract_json("checkout", "charge_card", &["happy path", "declined card"]),
    );
    f.insert(
        "contracts/refund.contracts.json".to_string(),
        contract_json("refund", "charge_card", &["full refund"]),
    );
    f.insert("README.md".to_string(), "# Acme Checkout Package".to_string());
    f
}

fn build_signed(name: &str, version: &str, k: &SigningKey) -> SignedPackage {
    build_package(manifest(name, version), package_files(), k).unwrap()
}

/// Serialize to the wire format and read it back — every e2e path goes
/// through real bytes.
fn wire_round_trip(pkg: &SignedPackage) -> SignedPackage {
    let bytes = serde_json::to_vec(pkg).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn trusting(pkg: &SignedPackage) -> TrustPolicy {
    TrustPolicy {
        trusted_keys: vec![pkg.public_key.clone()],
        allow_untrusted: false,
    }
}

// ---------------------------------------------------------------------------
// Install pipeline simulation (mirrors `orch8 package install` order)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
struct InstallOutcome {
    trust: TrustLevel,
    namespace: String,
    installed_sequences: usize,
    installed_contracts: usize,
}

/// Decision order: verify → trust → upgrade-check against every existing
/// version → conflict simulation (namespace isolation) → namespace
/// assignment.
fn install_pipeline(
    wire: &[u8],
    policy: &TrustPolicy,
    existing_versions: &[&str],
    local_namespaces: &[&str],
) -> Result<InstallOutcome, PackageError> {
    let pkg: SignedPackage = serde_json::from_slice(wire)
        .map_err(|e| PackageError::Invalid(format!("unreadable package: {e}")))?;
    verify_package(&pkg)?;
    let trust = check_trust(&pkg, policy)?;
    validate_package_name(&pkg.archive.manifest.name)?;
    for installed in existing_versions {
        check_upgrade(installed, &pkg.archive.manifest.version)?;
    }
    let namespace = install_namespace(&pkg.archive.manifest.name);
    // Conflict simulation: the pkg.* namespace must never collide with
    // local (user) namespaces — that is what makes installs non-destructive.
    if local_namespaces.iter().any(|ns| *ns == namespace) {
        return Err(PackageError::Invalid(format!(
            "namespace '{namespace}' already exists locally"
        )));
    }
    Ok(InstallOutcome {
        trust,
        namespace,
        installed_sequences: sequence_files(&pkg.archive).len(),
        installed_contracts: contract_files(&pkg.archive).len(),
    })
}

// ---------------------------------------------------------------------------
// Wire round trips
// ---------------------------------------------------------------------------

#[test]
fn e2e_build_serialize_deserialize_verify() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let back = wire_round_trip(&pkg);
    verify_package(&back).unwrap();
}

#[test]
fn e2e_multi_file_package_survives_round_trip() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let back = wire_round_trip(&pkg);
    assert_eq!(back.archive.files.len(), 6);
    assert_eq!(back, pkg);
}

#[test]
fn e2e_round_trip_preserves_manifest() {
    let pkg = build_signed("acme/checkout", "1.2.3", &key());
    let back = wire_round_trip(&pkg);
    assert_eq!(back.archive.manifest, pkg.archive.manifest);
    assert_eq!(back.archive.manifest.version, "1.2.3");
}

#[test]
fn e2e_round_trip_preserves_content_hash() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let back = wire_round_trip(&pkg);
    assert_eq!(back.content_hash, pkg.content_hash);
    assert_eq!(content_hash(&back.archive).unwrap(), pkg.content_hash);
}

#[test]
fn e2e_wire_bytes_are_deterministic() {
    let k = key();
    let a = serde_json::to_vec(&build_signed("acme/checkout", "1.0.0", &k)).unwrap();
    let b = serde_json::to_vec(&build_signed("acme/checkout", "1.0.0", &k)).unwrap();
    assert_eq!(a, b);
}

#[test]
fn e2e_double_round_trip_is_stable() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let once = wire_round_trip(&pkg);
    let twice = wire_round_trip(&once);
    assert_eq!(once, twice);
    assert_eq!(
        serde_json::to_vec(&once).unwrap(),
        serde_json::to_vec(&twice).unwrap()
    );
}

#[test]
fn e2e_pretty_printed_wire_still_verifies() {
    // Whitespace in the transport encoding must not affect verification —
    // the hash is over the canonical form, not the wire bytes.
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let pretty = serde_json::to_string_pretty(&pkg).unwrap();
    let back: SignedPackage = serde_json::from_str(&pretty).unwrap();
    verify_package(&back).unwrap();
    assert_eq!(back, pkg);
}

#[test]
fn e2e_unknown_extra_json_fields_are_ignored() {
    // The wire format tolerates additive fields (no deny_unknown_fields):
    // future-proof for older readers of newer wires. Assert actual behavior.
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let mut value: serde_json::Value = serde_json::to_value(&pkg).unwrap();
    value["future_field"] = json!("ignored");
    let back: SignedPackage = serde_json::from_value(value).unwrap();
    verify_package(&back).unwrap();
}

#[test]
fn e2e_extraction_counts_after_round_trip() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let back = wire_round_trip(&pkg);
    assert_eq!(sequence_files(&back.archive).len(), 3);
    assert_eq!(contract_files(&back.archive).len(), 2);
}

#[test]
fn e2e_truncated_wire_fails_to_parse() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let bytes = serde_json::to_vec(&pkg).unwrap();
    let truncated = &bytes[..bytes.len() / 2];
    assert!(serde_json::from_slice::<SignedPackage>(truncated).is_err());
}

// ---------------------------------------------------------------------------
// Lifecycle chains
// ---------------------------------------------------------------------------

#[test]
fn e2e_lifecycle_install_upgrade_downgrade_upgrade() {
    let k = key();
    let mut installed: Vec<String> = vec![];

    // v1.0.0 fresh install.
    let v1 = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &k));
    verify_package(&v1).unwrap();
    assert!(installed.iter().all(|i| check_upgrade(i, "1.0.0").is_ok()));
    installed.push("1.0.0".into());

    // v1.1.0 upgrade OK.
    let v11 = wire_round_trip(&build_signed("acme/checkout", "1.1.0", &k));
    verify_package(&v11).unwrap();
    assert!(installed.iter().all(|i| check_upgrade(i, "1.1.0").is_ok()));
    installed.push("1.1.0".into());

    // v1.0.5 downgrade rejected (1.1.0 already installed).
    let v105 = wire_round_trip(&build_signed("acme/checkout", "1.0.5", &k));
    verify_package(&v105).unwrap();
    let blocked = installed.iter().any(|i| check_upgrade(i, "1.0.5").is_err());
    assert!(blocked, "1.0.5 must be rejected after 1.1.0");

    // v2.0.0 OK again.
    let v2 = wire_round_trip(&build_signed("acme/checkout", "2.0.0", &k));
    verify_package(&v2).unwrap();
    assert!(installed.iter().all(|i| check_upgrade(i, "2.0.0").is_ok()));
}

#[test]
fn e2e_reinstalling_same_version_is_rejected() {
    let pkg = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &key()));
    verify_package(&pkg).unwrap();
    assert!(matches!(
        check_upgrade("1.0.0", &pkg.archive.manifest.version),
        Err(PackageError::Downgrade { .. })
    ));
}

#[test]
fn e2e_lifecycle_across_segment_counts() {
    // 1.0 -> 1.0.1 -> 1.1 all monotonic under Vec ordering.
    check_upgrade("1.0", "1.0.1").unwrap();
    check_upgrade("1.0.1", "1.1").unwrap();
    let pkg = wire_round_trip(&build_signed("acme/checkout", "1.1", &key()));
    verify_package(&pkg).unwrap();
    assert_eq!(parse_version(&pkg.archive.manifest.version).unwrap(), vec![1, 1]);
}

#[test]
fn e2e_incoming_must_beat_every_installed_version() {
    let installed = ["1.0.0", "1.3.0"];
    // 1.2.0 beats 1.0.0 but not 1.3.0 — overall rejected.
    assert!(check_upgrade(installed[0], "1.2.0").is_ok());
    assert!(check_upgrade(installed[1], "1.2.0").is_err());
    let overall_ok = installed.iter().all(|i| check_upgrade(i, "1.2.0").is_ok());
    assert!(!overall_ok);
}

#[test]
fn e2e_numeric_upgrade_through_full_pipeline() {
    let pkg = build_signed("acme/checkout", "1.10.0", &key());
    let wire = serde_json::to_vec(&pkg).unwrap();
    let outcome = install_pipeline(&wire, &trusting(&pkg), &["1.9.0"], &[]).unwrap();
    assert_eq!(outcome.trust, TrustLevel::Trusted);
}

#[test]
fn e2e_downgrade_error_surfaces_both_versions() {
    let pkg = build_signed("acme/checkout", "1.0.5", &key());
    let wire = serde_json::to_vec(&pkg).unwrap();
    let err = install_pipeline(&wire, &trusting(&pkg), &["1.1.0"], &[]).unwrap_err();
    match err {
        PackageError::Downgrade { installed, incoming } => {
            assert_eq!(installed, "1.1.0");
            assert_eq!(incoming, "1.0.5");
        }
        other => panic!("expected Downgrade, got {other:?}"),
    }
}

#[test]
fn e2e_version_history_stays_parseable_over_wire() {
    for v in ["0.1.0", "0.2.0", "1.0.0", "1.0.1", "1.10.3", "2.0.0.1"] {
        let pkg = wire_round_trip(&build_signed("acme/checkout", v, &key()));
        assert_eq!(pkg.archive.manifest.version, v);
        parse_version(&pkg.archive.manifest.version).unwrap();
    }
}

#[test]
fn e2e_fresh_install_with_no_history_needs_no_upgrade_check() {
    let pkg = build_signed("acme/checkout", "0.0.1", &key());
    let wire = serde_json::to_vec(&pkg).unwrap();
    let outcome = install_pipeline(&wire, &trusting(&pkg), &[], &[]).unwrap();
    assert_eq!(outcome.namespace, "pkg.acme.checkout");
}

// ---------------------------------------------------------------------------
// Tamper-after-serialize
// ---------------------------------------------------------------------------

fn tamper_wire(pkg: &SignedPackage, mutate: impl FnOnce(&mut serde_json::Value)) -> SignedPackage {
    let text = serde_json::to_string(pkg).unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&text).unwrap();
    mutate(&mut value);
    serde_json::from_value(value).unwrap()
}

#[test]
fn e2e_tampered_description_in_wire_text() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let bad = tamper_wire(&pkg, |v| {
        v["archive"]["manifest"]["description"] = json!("changed in transit");
    });
    assert_eq!(verify_package(&bad), Err(PackageError::Tampered));
}

#[test]
fn e2e_tampered_file_content_in_wire_text() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let bad = tamper_wire(&pkg, |v| {
        v["archive"]["files"]["README.md"] = json!("# Malicious");
    });
    assert_eq!(verify_package(&bad), Err(PackageError::Tampered));
}

#[test]
fn e2e_tampered_version_in_wire_text() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let bad = tamper_wire(&pkg, |v| {
        v["archive"]["manifest"]["version"] = json!("99.0.0");
    });
    assert_eq!(verify_package(&bad), Err(PackageError::Tampered));
}

#[test]
fn e2e_tampered_content_hash_in_wire_text() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let bad = tamper_wire(&pkg, |v| {
        v["content_hash"] = json!("f".repeat(64));
    });
    assert_eq!(verify_package(&bad), Err(PackageError::Tampered));
}

#[test]
fn e2e_recomputed_hash_with_stale_signature_over_wire() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let mut back = wire_round_trip(&pkg);
    back.archive.files.insert("README.md".into(), "# Evil".into());
    back.content_hash = content_hash(&back.archive).unwrap();
    let bad = wire_round_trip(&back);
    assert_eq!(verify_package(&bad), Err(PackageError::BadSignature));
}

#[test]
fn e2e_swapped_signature_in_wire_text() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let bad = tamper_wire(&pkg, |v| {
        v["signature"] = json!(BASE64.encode([7u8; 64]));
    });
    assert_eq!(verify_package(&bad), Err(PackageError::BadSignature));
}

#[test]
fn e2e_corrupted_signature_base64_in_wire_text() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let bad = tamper_wire(&pkg, |v| {
        v["signature"] = json!("@@not@@base64@@");
    });
    assert!(matches!(verify_package(&bad), Err(PackageError::Invalid(_))));
}

#[test]
fn e2e_attacker_public_key_in_wire_text() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let attacker = key();
    let bad = tamper_wire(&pkg, |v| {
        v["public_key"] = json!(BASE64.encode(attacker.verifying_key().to_bytes()));
    });
    assert_eq!(verify_package(&bad), Err(PackageError::BadSignature));
}

#[test]
fn e2e_tampered_format_version_in_wire_text() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let bad = tamper_wire(&pkg, |v| {
        v["archive"]["format_version"] = json!(2);
    });
    assert_eq!(verify_package(&bad), Err(PackageError::UnsupportedFormat(2)));
}

#[test]
fn e2e_hidden_requirement_in_wire_text() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let bad = tamper_wire(&pkg, |v| {
        v["archive"]["manifest"]["requirements"]["handlers"] = json!(["charge_card"]);
    });
    assert_eq!(verify_package(&bad), Err(PackageError::Tampered));
}

#[test]
fn e2e_removed_file_entry_in_wire_text() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let bad = tamper_wire(&pkg, |v| {
        v["archive"]["files"]
            .as_object_mut()
            .unwrap()
            .remove("sequences/refund.json");
    });
    assert_eq!(verify_package(&bad), Err(PackageError::Tampered));
}

#[test]
fn e2e_tampered_wire_is_rejected_by_pipeline_before_trust() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let bad = tamper_wire(&pkg, |v| {
        v["archive"]["manifest"]["publisher"] = json!("Wolf in sheep's clothing");
    });
    let wire = serde_json::to_vec(&bad).unwrap();
    // Even a fully-trusting policy cannot save a tampered package.
    let policy = TrustPolicy {
        trusted_keys: vec![bad.public_key.clone()],
        allow_untrusted: true,
    };
    assert_eq!(
        install_pipeline(&wire, &policy, &[], &[]),
        Err(PackageError::Tampered)
    );
}

// ---------------------------------------------------------------------------
// Publisher rotation
// ---------------------------------------------------------------------------

#[test]
fn e2e_rotation_same_content_same_hash() {
    let old_pkg = build_signed("acme/checkout", "1.0.0", &key());
    let new_pkg = build_signed("acme/checkout", "1.0.0", &key());
    assert_eq!(old_pkg.content_hash, new_pkg.content_hash);
}

#[test]
fn e2e_rotation_changes_signature_and_key() {
    let old_pkg = build_signed("acme/checkout", "1.0.0", &key());
    let new_pkg = build_signed("acme/checkout", "1.0.0", &key());
    assert_ne!(old_pkg.signature, new_pkg.signature);
    assert_ne!(old_pkg.public_key, new_pkg.public_key);
}

#[test]
fn e2e_rotation_old_trust_rejects_new_package() {
    let old_key = key();
    let new_key = key();
    let old_pkg = build_signed("acme/checkout", "1.0.0", &old_key);
    let new_pkg = wire_round_trip(&build_signed("acme/checkout", "1.1.0", &new_key));
    verify_package(&new_pkg).unwrap(); // self-consistent
    let policy = trusting(&old_pkg); // operator still trusts only the old key
    assert_eq!(
        check_trust(&new_pkg, &policy),
        Err(PackageError::UntrustedPublisher)
    );
}

#[test]
fn e2e_rotation_trusting_both_keys_accepts_both() {
    let old_pkg = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &key()));
    let new_pkg = wire_round_trip(&build_signed("acme/checkout", "1.1.0", &key()));
    let policy = TrustPolicy {
        trusted_keys: vec![old_pkg.public_key.clone(), new_pkg.public_key.clone()],
        allow_untrusted: false,
    };
    assert_eq!(check_trust(&old_pkg, &policy), Ok(TrustLevel::Trusted));
    assert_eq!(check_trust(&new_pkg, &policy), Ok(TrustLevel::Trusted));
}

#[test]
fn e2e_rotated_package_full_pipeline_with_updated_policy() {
    let new_key = key();
    let new_pkg = build_signed("acme/checkout", "1.1.0", &new_key);
    let wire = serde_json::to_vec(&new_pkg).unwrap();
    let outcome = install_pipeline(&wire, &trusting(&new_pkg), &["1.0.0"], &[]).unwrap();
    assert_eq!(outcome.trust, TrustLevel::Trusted);
    assert_eq!(outcome.installed_sequences, 3);
}

// ---------------------------------------------------------------------------
// Requirements-driven flows
// ---------------------------------------------------------------------------

#[test]
fn e2e_requirements_survive_wire_round_trip() {
    let pkg = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &key()));
    let req = &pkg.archive.manifest.requirements;
    assert_eq!(req.handlers, vec!["charge_card".to_string(), "send_email".to_string()]);
    assert_eq!(req.credentials, vec!["stripe_key".to_string()]);
    assert!(req.plugins.is_empty());
    assert_eq!(req.queues, vec!["billing".to_string()]);
    assert_eq!(req.min_engine_version.as_deref(), Some("0.6.0"));
}

#[test]
fn e2e_preflight_detects_missing_handlers() {
    let pkg = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &key()));
    let available_handlers = ["send_email"]; // charge_card missing
    let missing: Vec<&String> = pkg
        .archive
        .manifest
        .requirements
        .handlers
        .iter()
        .filter(|h| !available_handlers.contains(&h.as_str()))
        .collect();
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0], "charge_card");
}

#[test]
fn e2e_preflight_passes_when_everything_available() {
    let pkg = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &key()));
    let handlers = ["charge_card", "send_email", "unrelated"];
    let creds = ["stripe_key"];
    let queues = ["billing", "default"];
    let req = &pkg.archive.manifest.requirements;
    assert!(req.handlers.iter().all(|h| handlers.contains(&h.as_str())));
    assert!(req.credentials.iter().all(|c| creds.contains(&c.as_str())));
    assert!(req.queues.iter().all(|q| queues.contains(&q.as_str())));
}

#[test]
fn e2e_min_engine_version_gate_via_parse_version() {
    let pkg = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &key()));
    let required = pkg.archive.manifest.requirements.min_engine_version.clone().unwrap();
    let required = parse_version(&required).unwrap();
    // Engine 0.7.1 satisfies min 0.6.0; engine 0.5.9 does not.
    assert!(parse_version("0.7.1").unwrap() >= required);
    assert!(parse_version("0.5.9").unwrap() < required);
}

#[test]
fn e2e_empty_requirement_lists_are_omitted_from_wire() {
    // plugins is empty in the fixture — skip_serializing_if drops it.
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let value: serde_json::Value = serde_json::to_value(&pkg).unwrap();
    let req = &value["archive"]["manifest"]["requirements"];
    assert!(req.get("plugins").is_none(), "empty plugins should be omitted: {req}");
    assert!(req.get("handlers").is_some());
}

#[test]
fn e2e_requirements_drive_rejection_decision() {
    // A consumer that refuses packages needing credentials it doesn't have.
    let pkg = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &key()));
    let vault: [&str; 0] = [];
    let unsatisfied = pkg
        .archive
        .manifest
        .requirements
        .credentials
        .iter()
        .any(|c| !vault.contains(&c.as_str()));
    assert!(unsatisfied, "install must be blocked on missing stripe_key");
}

// ---------------------------------------------------------------------------
// orch8-types integration: packaged content is real engine content
// ---------------------------------------------------------------------------

#[test]
fn e2e_packaged_sequences_parse_as_sequence_definitions() {
    let pkg = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &key()));
    let seqs = sequence_files(&pkg.archive);
    assert_eq!(seqs.len(), 3);
    for (path, raw) in seqs {
        let def: SequenceDefinition = serde_json::from_str(raw)
            .unwrap_or_else(|e| panic!("{path} failed to parse: {e}"));
        assert!(!def.blocks.is_empty(), "{path} has no blocks");
    }
}

#[test]
fn e2e_parsed_sequence_preserves_identity() {
    let pkg = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &key()));
    let raw = &pkg.archive.files["sequences/checkout.json"];
    let def: SequenceDefinition = serde_json::from_str(raw).unwrap();
    assert_eq!(def.name, "checkout");
    assert_eq!(def.version, 1);
    assert_eq!(def.blocks.len(), 2);
}

#[test]
fn e2e_parsed_sequence_handlers_match_declared_requirements() {
    let pkg = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &key()));
    let declared = &pkg.archive.manifest.requirements.handlers;
    for (_, raw) in sequence_files(&pkg.archive) {
        let def: SequenceDefinition = serde_json::from_str(raw).unwrap();
        for block in &def.blocks {
            if let BlockDefinition::Step(s) = block {
                assert!(
                    declared.contains(&s.handler),
                    "handler '{}' used but not declared",
                    s.handler
                );
            }
        }
    }
}

#[test]
fn e2e_packaged_contracts_parse_and_validate_as_contract_suites() {
    let pkg = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &key()));
    let contracts = contract_files(&pkg.archive);
    assert_eq!(contracts.len(), 2);
    for (path, raw) in contracts {
        let suite: ContractSuite = serde_json::from_str(raw)
            .unwrap_or_else(|e| panic!("{path} failed to parse: {e}"));
        suite.validate().unwrap_or_else(|e| panic!("{path} invalid: {e}"));
        assert!(!suite.cases.is_empty());
    }
}

#[test]
fn e2e_parsed_contract_matches_its_sequence() {
    let pkg = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &key()));
    let raw = &pkg.archive.files["contracts/checkout.contracts.json"];
    let suite: ContractSuite = serde_json::from_str(raw).unwrap();
    assert_eq!(suite.sequence_name.as_deref(), Some("checkout"));
    assert_eq!(suite.cases.len(), 2);
    assert_eq!(suite.cases[0].name, "happy path");
}

#[test]
fn e2e_bad_contract_still_verifies_but_fails_suite_validation() {
    // Package signing does not inspect content: a structurally-broken suite
    // (duplicate case names) still verifies at the package layer and fails
    // at the contract layer.
    let mut suite = contract_suite("checkout", "charge_card", &["dup"]);
    suite.cases.push(suite.cases[0].clone()); // duplicate name
    let mut f = BTreeMap::new();
    f.insert(
        "contracts/bad.contracts.json".to_string(),
        serde_json::to_string(&suite).unwrap(),
    );
    let pkg = wire_round_trip(&build_package(manifest("acme/bad", "1.0.0"), f, &key()).unwrap());
    verify_package(&pkg).unwrap();
    let raw = &pkg.archive.files["contracts/bad.contracts.json"];
    let parsed: ContractSuite = serde_json::from_str(raw).unwrap();
    let err = parsed.validate().unwrap_err();
    assert!(err.contains("duplicate case name"), "{err}");
}

#[test]
fn e2e_future_contract_schema_fails_validation_not_verification() {
    let mut suite = contract_suite("checkout", "charge_card", &["case"]);
    suite.schema_version = 99;
    let mut f = BTreeMap::new();
    f.insert(
        "contracts/future.contracts.json".to_string(),
        serde_json::to_string(&suite).unwrap(),
    );
    let pkg = wire_round_trip(&build_package(manifest("acme/future", "1.0.0"), f, &key()).unwrap());
    verify_package(&pkg).unwrap();
    let parsed: ContractSuite =
        serde_json::from_str(&pkg.archive.files["contracts/future.contracts.json"]).unwrap();
    assert!(parsed.validate().is_err());
}

#[test]
fn e2e_installed_sequence_can_be_renamespaced_and_reserialized() {
    // Install step: parse the packaged sequence, move it into the package
    // namespace, and re-serialize — the artifact must survive intact.
    let pkg = wire_round_trip(&build_signed("acme/checkout", "1.0.0", &key()));
    let ns = install_namespace(&pkg.archive.manifest.name);
    let mut def: SequenceDefinition =
        serde_json::from_str(&pkg.archive.files["sequences/checkout.json"]).unwrap();
    def.namespace = Namespace::new(ns.clone());
    let re = serde_json::to_string(&def).unwrap();
    let back: SequenceDefinition = serde_json::from_str(&re).unwrap();
    assert_eq!(back.namespace.as_str(), "pkg.acme.checkout");
    assert_eq!(back.name, "checkout");
}

// ---------------------------------------------------------------------------
// Namespace decisions
// ---------------------------------------------------------------------------

#[test]
fn e2e_namespace_decisions_for_many_names() {
    let cases = [
        ("acme/checkout", "pkg.acme.checkout"),
        ("acme/refunds", "pkg.acme.refunds"),
        ("globex/checkout", "pkg.globex.checkout"),
        ("a1/b2", "pkg.a1.b2"),
        ("my-org/my_flows", "pkg.my-org.my_flows"),
        ("_/_", "pkg._._"),
    ];
    for (name, expected) in cases {
        validate_package_name(name).unwrap();
        assert_eq!(install_namespace(name), expected);
    }
}

#[test]
fn e2e_namespaces_unique_across_publishers_and_packages() {
    let names = ["acme/checkout", "globex/checkout", "acme/refunds", "globex/refunds"];
    let mut namespaces: Vec<String> = names.iter().map(|n| install_namespace(n)).collect();
    namespaces.sort();
    namespaces.dedup();
    assert_eq!(namespaces.len(), names.len());
}

#[test]
fn e2e_namespace_never_collides_with_unprefixed_local_names() {
    for name in ["acme/checkout", "x/y"] {
        let ns = install_namespace(name);
        assert!(ns.starts_with("pkg."));
        assert_ne!(ns, "default");
        assert_ne!(ns, name);
    }
}

#[test]
fn e2e_same_package_reinstall_targets_same_namespace() {
    let a = install_namespace("acme/checkout");
    let b = install_namespace("acme/checkout");
    assert_eq!(a, b);
}

#[test]
fn e2e_namespace_from_wire_manifest() {
    let pkg = wire_round_trip(&build_signed("globex/billing", "1.0.0", &key()));
    assert_eq!(
        install_namespace(&pkg.archive.manifest.name),
        "pkg.globex.billing"
    );
}

// ---------------------------------------------------------------------------
// Full install pipeline fixture
// ---------------------------------------------------------------------------

#[test]
fn e2e_pipeline_happy_path_all_stages() {
    let k = key();
    let pkg = build_signed("acme/checkout", "1.2.0", &k);
    let wire = serde_json::to_vec(&pkg).unwrap();

    let outcome = install_pipeline(
        &wire,
        &trusting(&pkg),
        &["1.0.0", "1.1.0"],
        &["default", "team-a", "pkg.globex.other"],
    )
    .unwrap();

    assert_eq!(outcome.trust, TrustLevel::Trusted);
    assert_eq!(outcome.namespace, "pkg.acme.checkout");
    assert_eq!(outcome.installed_sequences, 3);
    assert_eq!(outcome.installed_contracts, 2);
}

#[test]
fn e2e_pipeline_rejects_unreadable_wire() {
    let policy = TrustPolicy { trusted_keys: vec![], allow_untrusted: true };
    let err = install_pipeline(b"not json at all", &policy, &[], &[]).unwrap_err();
    assert!(matches!(err, PackageError::Invalid(_)));
}

#[test]
fn e2e_pipeline_rejects_tampered_before_trust_and_upgrade() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let mut bad = wire_round_trip(&pkg);
    bad.archive.manifest.description = "tampered".into();
    let wire = serde_json::to_vec(&bad).unwrap();
    // Downgrade situation AND untrusted policy AND tampered: Tampered wins.
    let policy = TrustPolicy { trusted_keys: vec![], allow_untrusted: false };
    assert_eq!(
        install_pipeline(&wire, &policy, &["9.0.0"], &[]),
        Err(PackageError::Tampered)
    );
}

#[test]
fn e2e_pipeline_rejects_untrusted_before_upgrade_check() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let wire = serde_json::to_vec(&pkg).unwrap();
    // Also a downgrade — but trust is decided first.
    let policy = TrustPolicy { trusted_keys: vec![], allow_untrusted: false };
    assert_eq!(
        install_pipeline(&wire, &policy, &["9.0.0"], &[]),
        Err(PackageError::UntrustedPublisher)
    );
}

#[test]
fn e2e_pipeline_rejects_downgrade_at_upgrade_stage() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let wire = serde_json::to_vec(&pkg).unwrap();
    assert!(matches!(
        install_pipeline(&wire, &trusting(&pkg), &["2.0.0"], &[]),
        Err(PackageError::Downgrade { .. })
    ));
}

#[test]
fn e2e_pipeline_unsupported_format_halts_first() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let bad = tamper_wire(&pkg, |v| {
        v["archive"]["format_version"] = json!(42);
    });
    let wire = serde_json::to_vec(&bad).unwrap();
    let policy = TrustPolicy { trusted_keys: vec![], allow_untrusted: false };
    assert_eq!(
        install_pipeline(&wire, &policy, &["9.0.0"], &[]),
        Err(PackageError::UnsupportedFormat(42))
    );
}

#[test]
fn e2e_pipeline_namespace_conflict_detected() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let wire = serde_json::to_vec(&pkg).unwrap();
    let err = install_pipeline(&wire, &trusting(&pkg), &[], &["pkg.acme.checkout"]).unwrap_err();
    assert!(matches!(err, PackageError::Invalid(msg) if msg.contains("pkg.acme.checkout")));
}

#[test]
fn e2e_pipeline_local_namespaces_do_not_conflict() {
    // User workspaces named like the package (but unprefixed) never collide.
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let wire = serde_json::to_vec(&pkg).unwrap();
    let outcome = install_pipeline(
        &wire,
        &trusting(&pkg),
        &[],
        &["acme.checkout", "checkout", "default"],
    )
    .unwrap();
    assert_eq!(outcome.namespace, "pkg.acme.checkout");
}

#[test]
fn e2e_pipeline_untrusted_opt_in_completes_with_untrusted_level() {
    let pkg = build_signed("acme/checkout", "1.0.0", &key());
    let wire = serde_json::to_vec(&pkg).unwrap();
    let policy = TrustPolicy { trusted_keys: vec![], allow_untrusted: true };
    let outcome = install_pipeline(&wire, &policy, &[], &[]).unwrap();
    assert_eq!(outcome.trust, TrustLevel::UntrustedAllowed);
    assert_eq!(outcome.installed_sequences, 3);
    assert_eq!(outcome.installed_contracts, 2);
}

#[test]
fn e2e_pipeline_upgrade_replaces_into_same_namespace() {
    let k = key();
    let v1 = build_signed("acme/checkout", "1.0.0", &k);
    let v2 = build_signed("acme/checkout", "2.0.0", &k);
    let out1 = install_pipeline(&serde_json::to_vec(&v1).unwrap(), &trusting(&v1), &[], &[]).unwrap();
    let out2 = install_pipeline(
        &serde_json::to_vec(&v2).unwrap(),
        &trusting(&v2),
        &["1.0.0"],
        &[], // same pkg namespace is the package's own — not a local conflict
    )
    .unwrap();
    assert_eq!(out1.namespace, out2.namespace);
}

#[test]
fn e2e_pipeline_full_realistic_story() {
    // A realistic end-to-end story asserting at each stage.
    let publisher_key = key();

    // 1. Publisher builds and ships v1.0.0.
    let v1 = build_signed("acme/checkout", "1.0.0", &publisher_key);
    let wire_v1 = serde_json::to_vec(&v1).unwrap();

    // 2. Operator inspects before trusting: verify works standalone.
    let received: SignedPackage = serde_json::from_slice(&wire_v1).unwrap();
    verify_package(&received).unwrap();
    assert_eq!(received.archive.manifest.publisher, "Acme");

    // 3. First install requires the untrusted opt-in (key not yet pinned).
    let strict = TrustPolicy { trusted_keys: vec![], allow_untrusted: false };
    assert_eq!(
        install_pipeline(&wire_v1, &strict, &[], &["default"]),
        Err(PackageError::UntrustedPublisher)
    );

    // 4. Operator pins the key; install completes.
    let pinned = trusting(&received);
    let outcome = install_pipeline(&wire_v1, &pinned, &[], &["default"]).unwrap();
    assert_eq!(outcome.trust, TrustLevel::Trusted);
    assert_eq!(outcome.namespace, "pkg.acme.checkout");

    // 5. Installed sequences parse and their contracts validate.
    for (_, raw) in sequence_files(&received.archive) {
        let _: SequenceDefinition = serde_json::from_str(raw).unwrap();
    }
    for (_, raw) in contract_files(&received.archive) {
        serde_json::from_str::<ContractSuite>(raw).unwrap().validate().unwrap();
    }

    // 6. v1.1.0 upgrade sails through; a later v1.0.2 does not.
    let v11 = build_signed("acme/checkout", "1.1.0", &publisher_key);
    let out = install_pipeline(
        &serde_json::to_vec(&v11).unwrap(),
        &pinned,
        &["1.0.0"],
        &["default"],
    )
    .unwrap();
    assert_eq!(out.trust, TrustLevel::Trusted);
    let v102 = build_signed("acme/checkout", "1.0.2", &publisher_key);
    assert!(matches!(
        install_pipeline(
            &serde_json::to_vec(&v102).unwrap(),
            &pinned,
            &["1.0.0", "1.1.0"],
            &["default"],
        ),
        Err(PackageError::Downgrade { .. })
    ));
}
