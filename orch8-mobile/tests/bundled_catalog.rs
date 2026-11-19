use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ed25519_dalek::SigningKey;
use orch8_mobile::{MobileEngine, MobileEngineConfig};
use orch8_publisher::package::{PackageManifest, PackageRequirements, build_package};

fn signed_seed(signing_key: &SigningKey, version: i32) -> String {
    let workflow = serde_json::json!({
        "id": format!("ca2059f7-6839-43c2-a8eb-{version:012}"),
        "tenant_id": "mobile", "namespace": "default", "version": version,
        "name": "bundled-inspection", "created_at": "2026-01-01T00:00:00Z",
        "blocks": [{"type": "step", "id": "start", "handler": "noop"}],
    });
    let package = build_package(
        PackageManifest {
            name: "example/mobile-seed".into(),
            version: "1.0.0".into(),
            description: "Mobile bootstrap catalog".into(),
            publisher: "example".into(),
            requirements: PackageRequirements::default(),
            created_at: chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .to_utc(),
        },
        BTreeMap::from([("sequences/inspection.json".into(), workflow.to_string())]),
        signing_key,
    )
    .unwrap();
    serde_json::to_string(&package).unwrap()
}

#[test]
fn signed_bundle_seeds_an_empty_offline_catalog_without_overwriting_existing_definitions() {
    let home = tempfile::tempdir().unwrap();
    let signing_key = SigningKey::from_bytes(&[25; 32]);
    let engine = MobileEngine::new(
        home.path().join("mobile.db").to_string_lossy().into_owned(),
        MobileEngineConfig {
            root_public_key: STANDARD.encode(signing_key.verifying_key().to_bytes()),
            telemetry_enabled: false,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        engine
            .install_bundled_catalog(signed_seed(&SigningKey::from_bytes(&[26; 32]), 1))
            .is_err()
    );
    assert!(engine.loaded_sequences().unwrap().is_empty());
    assert_eq!(
        engine
            .install_bundled_catalog(signed_seed(&signing_key, 1))
            .unwrap(),
        1
    );
    let result = engine
        .install_bundled_catalog_with_result(signed_seed(&signing_key, 2))
        .unwrap();
    assert_eq!(
        result.disposition,
        orch8_mobile::BundledCatalogDisposition::AlreadyPresent
    );
    assert_eq!((result.installed, result.skipped), (0, 1));
    assert_eq!(engine.loaded_sequences().unwrap()[0].version, 1);
    assert!(
        engine
            .start("bundled-inspection".into(), "{}".into(), None)
            .is_ok()
    );
}
