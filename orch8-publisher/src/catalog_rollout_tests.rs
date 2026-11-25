use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ed25519_dalek::{Signature, Verifier};
use orch8_types::catalog::CatalogAccess;
use serde_json::json;

use super::*;
use crate::cdn::MemoryCdnBackend;

fn key() -> SigningKey {
    SigningKey::from_bytes(&[7; 32])
}

fn signer() -> ManifestGenerator {
    ManifestGenerator::new(key(), "catalog-key".into())
}

fn audience() -> CatalogAudience {
    CatalogAudience {
        source: "distribution".into(),
        catalog: "inspection".into(),
        access: CatalogAccess::Principal {
            issuer: "tenant".into(),
            subject: "inspector".into(),
        },
    }
}

fn entry(version: i32) -> ManifestSequence {
    ManifestSequence {
        name: "inspection".into(),
        version,
        url: format!("/tenant/sequences/inspection-{version}.json"),
        signing_key_id: "catalog-key".into(),
        sha256: hex::encode(Sha256::digest(version.to_be_bytes())),
        required_handlers: vec![],
        min_sdk_version: "0.7.1".into(),
    }
}

fn snapshot(version: i32, min_app_version: &str) -> CatalogSnapshot {
    CatalogSnapshot {
        min_app_version: min_app_version.into(),
        entries: vec![entry(version)],
        signing_keys: vec![],
    }
}

fn rollout(percentage: u16) -> CatalogRollout {
    CatalogRollout {
        generation: 1,
        stable: snapshot(1, "1.0"),
        canary: Some(CatalogCanary {
            cohort_key: "inspection-v2".into(),
            basis_points: percentage,
            snapshot: snapshot(2, "2.0"),
        }),
    }
}

#[test]
fn cohort_is_sticky_as_percentage_expands_and_gates_on_app_and_sdk() {
    let small = rollout(1000);
    let mut expanded = rollout(6000);
    let mut counts = [0, 0];
    for index in 0..2000 {
        let device = CatalogDeviceCursor::new(format!("device-{index}"), audience()).unwrap();
        let early = device.select(&small, "2.2", "0.7.1").unwrap().0;
        let later = device.select(&expanded, "2.2", "0.7.1").unwrap().0;
        if early == CatalogReleaseRing::Canary {
            counts[0] += 1;
            assert_eq!(later, CatalogReleaseRing::Canary);
        }
        if later == CatalogReleaseRing::Canary {
            counts[1] += 1;
        }
        assert_eq!(
            device.select(&expanded, "1.9", "1.0").unwrap().0,
            CatalogReleaseRing::Stable
        );
        assert!(matches!(
            device.select(&expanded, "2.x", "0.7.1"),
            Err(CatalogRolloutError::Invalid(_))
        ));
    }
    assert!((120..280).contains(&counts[0]));
    assert!((1050..1350).contains(&counts[1]));
    let device = CatalogDeviceCursor::new("device-1".into(), audience()).unwrap();
    assert_eq!(device.bucket("inspection-v2"), 2309);
    let canary = expanded.canary.as_mut().unwrap();
    canary.basis_points = 10_000;
    canary.snapshot.entries[0].min_sdk_version = "1.0".into();
    assert_eq!(
        device.select(&expanded, "2.0", "0.9").unwrap().0,
        CatalogReleaseRing::Stable
    );
    assert_eq!(
        device.select(&expanded, "2.0", "1.0").unwrap().0,
        CatalogReleaseRing::Canary
    );
    assert!(matches!(
        device.select(&expanded, "0.5", "0.9"),
        Err(CatalogRolloutError::Incompatible)
    ));
}

#[test]
fn signs_existing_catalog_format_and_scopes_ack_to_latest_exact_device_delivery() {
    let mut device = CatalogDeviceCursor::new("device-a".into(), audience()).unwrap();
    let mut other = CatalogDeviceCursor::new("device-b".into(), audience()).unwrap();
    let rollout = rollout(10_000);
    let signer = signer();
    let first = device.issue(&rollout, &signer, "3", "0.7.1").unwrap();
    let wire_bytes = first.wire_bytes();
    let divider = wire_bytes.iter().position(|byte| *byte == b'\n').unwrap();
    let signature = &wire_bytes[..divider];
    let body = &wire_bytes[divider + 1..];
    assert_eq!(body, first.manifest.canonical_json.as_bytes());
    let signature = Signature::from_slice(&BASE64.decode(signature).unwrap()).unwrap();
    key()
        .verifying_key()
        .verify(first.manifest.canonical_json.as_bytes(), &signature)
        .unwrap();
    assert_eq!(first.manifest.body.audience, audience());
    assert_eq!(first.receipt.ring, CatalogReleaseRing::Canary);
    assert_eq!(
        first.receipt.manifest_sha256,
        hex::encode(Sha256::digest(first.manifest.canonical_json.as_bytes()))
    );
    assert!(device.acknowledge("device-b", &first.receipt).is_err());

    let other_delivery = other.issue(&rollout, &signer, "3", "0.7.1").unwrap();
    assert!(other.acknowledge("device-b", &first.receipt).is_err());
    assert!(
        device
            .acknowledge("device-a", &other_delivery.receipt)
            .is_err()
    );
    device.acknowledge("device-a", &first.receipt).unwrap();
    device.acknowledge("device-a", &first.receipt).unwrap();
    assert_eq!(device.acknowledged(), Some(&first.receipt));

    let second = device.issue(&rollout, &signer, "3", "0.7.1").unwrap();
    assert!(device.acknowledge("device-a", &first.receipt).is_err());
    assert_eq!(device.acknowledged(), Some(&first.receipt));
    assert!(
        device
            .acknowledge(
                "device-a",
                &CatalogDeliveryReceipt {
                    manifest_sha256: "wrong".into(),
                    ..second.receipt.clone()
                }
            )
            .is_err()
    );
    device.acknowledge("device-a", &second.receipt).unwrap();
}

fn definition(name: &str, version: i32, blocks: &serde_json::Value) -> SequenceDefinition {
    serde_json::from_value(json!({
        "id": SequenceId::new(),
        "tenant_id": "tenant",
        "namespace": "prod",
        "name": name,
        "version": version,
        "blocks": blocks,
        "created_at": "2026-01-01T00:00:00Z"
    }))
    .unwrap()
}

#[tokio::test]
async fn republishes_known_behavior_as_new_signed_revisions_and_keeps_manifest_versions_increasing()
{
    let early = rollout(10_000);
    let prior_signer = signer();
    prior_signer.observe_published_version(7_000_000_000_000);
    let mut device = CatalogDeviceCursor::new("device".into(), audience()).unwrap();
    let first = device.issue(&early, &prior_signer, "3", "0.7.1").unwrap();
    let persisted = serde_json::to_string(&device).unwrap();
    let mut restarted: CatalogDeviceCursor = serde_json::from_str(&persisted).unwrap();

    let withdrawn = CatalogRollout {
        generation: 2,
        stable: CatalogSnapshot {
            entries: vec![],
            ..snapshot(1, "1")
        },
        canary: None,
    };
    let next_signer = signer();
    let removal = restarted
        .issue(&withdrawn, &next_signer, "3", "0.7.1")
        .unwrap();
    assert!(removal.receipt.manifest_version > first.receipt.manifest_version);

    let unsafe_rollback = CatalogRollout {
        generation: 3,
        stable: snapshot(1, "1"),
        canary: None,
    };
    assert!(matches!(
        restarted.issue(&unsafe_rollback, &next_signer, "3", "0.7.1"),
        Err(CatalogRolloutError::WorkflowRollback(_))
    ));
    let mut reused_version = CatalogRollout {
        stable: snapshot(2, "1"),
        ..unsafe_rollback
    };
    reused_version.stable.entries[0].sha256 = "a".repeat(64);
    assert!(matches!(
        restarted.issue(&reused_version, &next_signer, "3", "0.7.1"),
        Err(CatalogRolloutError::WorkflowRollback(_))
    ));
    assert_eq!(restarted.last_issued(), Some(&removal.receipt));

    let prior_parent = definition(
        "inspection",
        1,
        &json!([{ "type": "sub_sequence", "id": "audit", "sequence_name": "audit", "version": 1 }]),
    );
    let prior_child = definition(
        "audit",
        1,
        &json!([{ "type": "step", "id": "check", "handler": "noop" }]),
    );
    let cdn = Arc::new(MemoryCdnBackend::new());
    let publisher = SequencePublisher::new(
        Box::new(Arc::clone(&cdn)),
        signer(),
        "tenant".into(),
        "catalog-key".into(),
    )
    .unwrap();
    let entries = publish_catalog_rollback(
        &publisher,
        &key(),
        &[prior_parent.clone(), prior_child.clone()],
        &BTreeMap::from([("inspection".into(), 2), ("audit".into(), 3)]),
    )
    .await
    .unwrap();

    let store = cdn.store.lock().await;
    for entry in &entries {
        let path = entry.url.trim_start_matches('/');
        let (payload, _) = &store[path];
        assert_eq!(entry.sha256, hex::encode(Sha256::digest(payload)));
        let (signature, _) = &store[&path.replace(".json", ".sig")];
        let signature = Signature::from_slice(&BASE64.decode(signature).unwrap()).unwrap();
        key().verifying_key().verify(payload, &signature).unwrap();
    }
    let parent: SequenceDefinition =
        serde_json::from_slice(&store[entries[0].url.trim_start_matches('/')].0).unwrap();
    assert_eq!(parent.version, 3);
    assert_ne!(parent.id, prior_parent.id);
    let BlockDefinition::SubSequence(child) = &parent.blocks[0] else {
        panic!("subsequence expected");
    };
    assert_eq!(child.version, Some(4));
    let child: SequenceDefinition =
        serde_json::from_slice(&store[entries[1].url.trim_start_matches('/')].0).unwrap();
    assert_eq!(child.version, 4);
    assert_ne!(child.id, prior_child.id);

    let rollback = CatalogRollout {
        generation: 3,
        stable: CatalogSnapshot {
            entries,
            ..snapshot(3, "1")
        },
        canary: None,
    };
    let delivery = restarted
        .issue(&rollback, &next_signer, "3", env!("CARGO_PKG_VERSION"))
        .unwrap();
    assert!(delivery.receipt.manifest_version > removal.receipt.manifest_version);
    assert_eq!(delivery.manifest.body.manifest.sequences[0].version, 3);
    assert!(restarted.issue(&early, &next_signer, "3", "0.7.1").is_err());
}

#[test]
fn idempotent_issuance_replays_wire_bytes_after_cursor_reload() {
    let mut policy = rollout(0);
    let mut cursor = CatalogDeviceCursor::new("device-1".into(), audience()).unwrap();
    let first = cursor
        .issue_idempotent(&policy, &signer(), "2.0", "1.0", "request-1")
        .unwrap();
    let mut reloaded: CatalogDeviceCursor =
        serde_json::from_slice(&serde_json::to_vec(&cursor).unwrap()).unwrap();
    let retry = reloaded
        .issue_idempotent(&policy, &signer(), "2.0", "1.0", "request-1")
        .unwrap();
    assert_eq!(retry.receipt, first.receipt);
    assert_eq!(retry.wire_bytes(), first.wire_bytes());
    let overlapping = reloaded
        .issue_idempotent(&policy, &signer(), "2.0", "1.0", "concurrent-request")
        .unwrap();
    assert_eq!(overlapping.receipt, first.receipt);
    reloaded.acknowledge("device-1", &first.receipt).unwrap();

    policy.generation += 1;
    assert!(
        reloaded
            .issue_idempotent(&policy, &signer(), "2.0", "1.0", "request-1")
            .is_err()
    );
    let next = reloaded
        .issue_idempotent(&policy, &signer(), "2.0", "1.0", "request-2")
        .unwrap();
    assert!(next.receipt.manifest_version > first.receipt.manifest_version);
    assert!(reloaded.acknowledge("device-1", &first.receipt).is_err());
}
