use super::*;
use ed25519_dalek::{Signer, SigningKey};
use orch8_publisher::{ManifestGenerator, ManifestSequence};
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::catalog::{CatalogAccess, CatalogAudience};
use std::sync::Arc;

const SOURCE: &str = "https://catalog.example/stable";

fn signer() -> SigningKey {
    SigningKey::from_bytes(&[21; 32])
}

fn audience(subject: &str, catalog: &str) -> CatalogAudience {
    CatalogAudience {
        source: SOURCE.into(),
        catalog: catalog.into(),
        access: CatalogAccess::Principal {
            issuer: "https://identity.example".into(),
            subject: subject.into(),
        },
    }
}

async fn fixture() -> Arc<MobileStorage> {
    Arc::new(MobileStorage::new(Arc::new(
        SqliteStorage::in_memory().await.unwrap(),
    )))
}

async fn scope(storage: &MobileStorage, subject: &str, catalog: &str) -> CatalogScope {
    let account = storage
        .open_account_scope(subject, "https://identity.example", subject)
        .await
        .unwrap();
    storage
        .open_catalog_scope(
            &account,
            catalog,
            SOURCE,
            &format!(
                "ed25519:{}",
                BASE64.encode(signer().verifying_key().as_bytes())
            ),
        )
        .await
        .unwrap()
}

fn definition() -> String {
    serde_json::json!({
        "id":"11111111-1111-4111-8111-111111111111", "tenant_id":"remote",
        "namespace":"remote", "name":"flow", "version":1, "blocks":[],
        "created_at":"2026-01-01T00:00:00Z",
    })
    .to_string()
}

fn signed(audience: CatalogAudience) -> Vec<u8> {
    let entry = ManifestSequence {
        name: "flow".into(),
        version: 1,
        url: "flow.json".into(),
        signing_key_id: "key".into(),
        sha256: hex::encode(Sha256::digest(definition().as_bytes())),
        required_handlers: vec![],
        min_sdk_version: "0.7.1".into(),
    };
    let manifest = ManifestGenerator::new(signer(), "key".into())
        .generate_catalog(audience, vec![entry], vec![], vec![])
        .unwrap();
    format!("{}\n{}", manifest.signature_b64, manifest.canonical_json).into_bytes()
}

fn resign(body: &[u8], signer: &SigningKey) -> Vec<u8> {
    format!(
        "{}\n{}",
        BASE64.encode(signer.sign(body).to_bytes()),
        std::str::from_utf8(body).unwrap()
    )
    .into_bytes()
}

fn body(bytes: &[u8]) -> &[u8] {
    &bytes[bytes.iter().position(|byte| *byte == b'\n').unwrap() + 1..]
}

#[tokio::test]
async fn publisher_documents_install_into_four_independent_catalogs() {
    let storage = fixture().await;
    let mut ids = std::collections::HashSet::new();
    for subject in ["alice", "bob"] {
        for catalog in ["one", "two"] {
            let scope = scope(&storage, subject, catalog).await;
            let bytes = signed(audience(subject, catalog));
            let accepted = storage
                .accept_signed_catalog_manifest(&scope, &bytes)
                .await
                .unwrap();
            assert_eq!(accepted.digest(), hex::encode(Sha256::digest(body(&bytes))));
            let workflow = accepted
                .install_workflow(
                    "flow",
                    &definition(),
                    &signer().sign(definition().as_bytes()).to_bytes(),
                )
                .await
                .unwrap();
            assert!(
                ids.insert(workflow),
                "the same supplied UUID must not alias another scope"
            );
            assert_eq!(
                storage
                    .catalog_workflow(&scope, "flow", None)
                    .await
                    .unwrap()
                    .unwrap()
                    .id,
                workflow
            );
        }
    }
    assert!(
        storage
            .manifest_trust()
            .await
            .unwrap()
            .accepted_version
            .is_none()
    );
}

#[tokio::test]
async fn foreign_audiences_and_legacy_documents_do_not_change_trust() {
    let storage = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let good = signed(audience("alice", "one"));
    let mut candidates = vec![
        signed(audience("bob", "one")),
        signed(audience("alice", "two")),
    ];
    let mut foreign_source = audience("alice", "one");
    foreign_source.source.push_str("/other");
    candidates.push(signed(foreign_source));
    let mut foreign_issuer = audience("alice", "one");
    foreign_issuer.access = CatalogAccess::Principal {
        issuer: "other".into(),
        subject: "alice".into(),
    };
    candidates.push(signed(foreign_issuer));
    let mut public = audience("alice", "one");
    public.access = CatalogAccess::Public;
    candidates.push(signed(public));
    let parsed: serde_json::Value = serde_json::from_slice(body(&good)).unwrap();
    candidates.push(resign(
        &serde_json::to_vec(&parsed["manifest"]).unwrap(),
        &signer(),
    ));
    candidates.push(resign(body(&good), &SigningKey::from_bytes(&[22; 32])));
    let mut tampered = good.clone();
    *tampered.last_mut().unwrap() ^= 1;
    candidates.push(tampered);
    for bytes in candidates {
        assert!(
            storage
                .accept_signed_catalog_manifest(&scope, &bytes)
                .await
                .is_err()
        );
        assert!(
            storage
                .catalog_manifest_trust(&scope)
                .await
                .unwrap()
                .accepted_version
                .is_none()
        );
        assert!(
            storage
                .catalog_signing_keys(&scope)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn verified_install_checks_exact_entry_body_signature_and_current_acceptance() {
    let storage = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let bytes = signed(audience("alice", "one"));
    let accepted = storage
        .accept_signed_catalog_manifest(&scope, &bytes)
        .await
        .unwrap();
    let definition = definition();
    let signature = signer().sign(definition.as_bytes()).to_bytes();
    assert!(
        accepted
            .install_workflow("absent", &definition, &signature)
            .await
            .is_err()
    );
    assert!(
        accepted
            .install_workflow("flow", &format!("{definition} "), &signature)
            .await
            .is_err()
    );
    assert!(
        accepted
            .install_workflow("flow", &definition, &[0; 64])
            .await
            .is_err()
    );
    // A newer root-signed document revokes all keys. An older accepted handle
    // must not reinstall a body after that revocation.
    let mut newer: serde_json::Value = serde_json::from_slice(body(&bytes)).unwrap();
    newer["manifest"]["manifest_version"] = (accepted.manifest().manifest_version + 1).into();
    newer["manifest"]["signing_keys"] = serde_json::json!([]);
    newer["manifest"]["sequences"] = serde_json::json!([]);
    let newer = resign(&serde_json::to_vec(&newer).unwrap(), &signer());
    storage
        .accept_signed_catalog_manifest(&scope, &newer)
        .await
        .unwrap();
    assert!(
        accepted
            .install_workflow("flow", &definition, &signature)
            .await
            .is_err()
    );
    assert!(
        storage
            .catalog_workflow(&scope, "flow", None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .accept_signed_catalog_manifest(&scope, &bytes)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn retirement_fences_verified_handles_and_late_acceptance() {
    let storage = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let bytes = signed(audience("alice", "one"));
    let accepted = storage
        .accept_signed_catalog_manifest(&scope, &bytes)
        .await
        .unwrap();
    storage.retire_account_scope(&scope.account).await.unwrap();
    let reopened = storage
        .open_account_scope("alice", "https://identity.example", "alice")
        .await
        .unwrap();
    assert!(
        storage
            .accept_signed_catalog_manifest(&scope, &bytes)
            .await
            .is_err()
    );
    assert!(
        accepted
            .install_workflow(
                "flow",
                &definition(),
                &signer().sign(definition().as_bytes()).to_bytes()
            )
            .await
            .is_err()
    );
    let reopened = storage
        .open_catalog_scope(
            &reopened,
            "one",
            SOURCE,
            &format!(
                "ed25519:{}",
                BASE64.encode(signer().verifying_key().as_bytes())
            ),
        )
        .await
        .unwrap();
    storage
        .accept_signed_catalog_manifest(&reopened, &bytes)
        .await
        .unwrap();
}

#[tokio::test]
async fn ambiguous_protocol_fields_and_invalid_key_sets_fail_atomically() {
    let storage = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let bytes = signed(audience("alice", "one"));
    let text = std::str::from_utf8(body(&bytes)).unwrap();
    for prefix in ["\"format\":\"orch8.catalog.v1\",", "\"unknown\":true,"] {
        let body = format!("{{{prefix}{}", &text[1..]);
        assert!(
            storage
                .accept_signed_catalog_manifest(&scope, &resign(body.as_bytes(), &signer()))
                .await
                .is_err()
        );
    }
    for field in ["format", "audience"] {
        let mut document: serde_json::Value = serde_json::from_slice(body(&bytes)).unwrap();
        document[field] = serde_json::Value::Null;
        assert!(
            storage
                .accept_signed_catalog_manifest(
                    &scope,
                    &resign(&serde_json::to_vec(&document).unwrap(), &signer())
                )
                .await
                .is_err()
        );
    }
    for invalid in [
        serde_json::json!([{"key_id":"key","public_key":"bad"}]),
        serde_json::json!([
        {"key_id":"key","public_key":BASE64.encode(signer().verifying_key().as_bytes())},
        {"key_id":"key","public_key":BASE64.encode(signer().verifying_key().as_bytes())}]),
    ] {
        let mut document: serde_json::Value = serde_json::from_slice(body(&bytes)).unwrap();
        document["manifest"]["signing_keys"] = invalid;
        assert!(
            storage
                .accept_signed_catalog_manifest(
                    &scope,
                    &resign(&serde_json::to_vec(&document).unwrap(), &signer())
                )
                .await
                .is_err()
        );
    }
    assert!(
        storage
            .catalog_manifest_trust(&scope)
            .await
            .unwrap()
            .accepted_version
            .is_none()
    );
}

#[tokio::test]
async fn acceptance_write_failure_preserves_the_previous_complete_trust_set() {
    let storage = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let first = signed(audience("alice", "one"));
    let accepted = storage
        .accept_signed_catalog_manifest(&scope, &first)
        .await
        .unwrap();
    let previous_version = accepted.manifest().manifest_version;
    let previous_digest = accepted.digest().to_owned();
    let mut document: serde_json::Value = serde_json::from_slice(body(&first)).unwrap();
    document["manifest"]["manifest_version"] = (previous_version + 1).into();
    document["manifest"]["signing_keys"][0]["key_id"] = "replacement".into();
    document["manifest"]["sequences"][0]["signing_key_id"] = "replacement".into();
    document["manifest"]["sequences"][0]["version"] = 2.into();
    document["manifest"]["sequences"][0]["sha256"] = "f".repeat(64).into();
    let newer = resign(&serde_json::to_vec(&document).unwrap(), &signer());
    sqlx::query("CREATE TRIGGER reject_catalog_key BEFORE INSERT ON mobile_catalog_signing_keys WHEN NEW.key_id = 'replacement' BEGIN SELECT RAISE(ABORT, 'injected key write failure'); END")
        .execute(storage.pool()).await.unwrap();
    assert!(
        storage
            .accept_signed_catalog_manifest(&scope, &newer)
            .await
            .is_err()
    );
    let trust = storage.catalog_manifest_trust(&scope).await.unwrap();
    assert_eq!(trust.accepted_version, Some(previous_version));
    assert_eq!(
        storage
            .catalog_metadata(&scope, "accepted_manifest_digest")
            .await
            .unwrap()
            .as_deref(),
        Some(previous_digest.as_str())
    );
    assert_eq!(
        storage.catalog_signing_keys(&scope).await.unwrap()[0].0,
        "key"
    );
    let entries: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_catalog_entry_history")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(
        entries, 1,
        "failed key writes must roll back newly accepted entry identity too"
    );
    sqlx::query("DROP TRIGGER reject_catalog_key")
        .execute(storage.pool())
        .await
        .unwrap();
    storage
        .accept_signed_catalog_manifest(&scope, &newer)
        .await
        .unwrap();
    assert_eq!(
        storage.catalog_signing_keys(&scope).await.unwrap()[0].0,
        "replacement"
    );
}

#[tokio::test]
async fn audience_and_replay_binding_survive_database_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("catalog.db");
    let path = path.to_str().unwrap();
    let first = signed(audience("alice", "one"));
    let digest;
    {
        let sqlite = Arc::new(SqliteStorage::file_mobile(path).await.unwrap());
        let storage = Arc::new(MobileStorage::new(Arc::clone(&sqlite)));
        let scope = scope(&storage, "alice", "one").await;
        let accepted = storage
            .accept_signed_catalog_manifest(&scope, &first)
            .await
            .unwrap();
        digest = accepted.digest().to_owned();
        drop(accepted);
        drop(storage);
        sqlite.pool().close().await;
    }
    let storage = Arc::new(MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path).await.unwrap(),
    )));
    let scope = scope(&storage, "alice", "one").await;
    let accepted = storage
        .accept_signed_catalog_manifest(&scope, &first)
        .await
        .unwrap();
    assert_eq!(accepted.digest(), digest);
    let mut changed: serde_json::Value = serde_json::from_slice(body(&first)).unwrap();
    changed["manifest"]["removed"] =
        serde_json::json!([{"name":"another-flow", "removed_at":chrono::Utc::now()}]);
    assert!(
        storage
            .accept_signed_catalog_manifest(
                &scope,
                &resign(&serde_json::to_vec(&changed).unwrap(), &signer())
            )
            .await
            .is_err(),
        "same-version equivocation survives reopen"
    );
    assert!(
        storage
            .accept_signed_catalog_manifest(&scope, &signed(audience("bob", "one")))
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .catalog_metadata(&scope, "accepted_manifest_digest")
            .await
            .unwrap(),
        Some(digest)
    );
}

#[tokio::test]
async fn signed_manifest_entry_must_match_the_embedded_definition() {
    let storage = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let first = signed(audience("alice", "one"));
    let mut document: serde_json::Value = serde_json::from_slice(body(&first)).unwrap();
    document["manifest"]["sequences"][0]["version"] = 2.into();
    let bytes = resign(&serde_json::to_vec(&document).unwrap(), &signer());
    let accepted = storage
        .accept_signed_catalog_manifest(&scope, &bytes)
        .await
        .unwrap();
    assert!(
        accepted
            .install_workflow(
                "flow",
                &definition(),
                &signer().sign(definition().as_bytes()).to_bytes()
            )
            .await
            .is_err()
    );
    assert!(
        storage
            .catalog_workflow(&scope, "flow", None)
            .await
            .unwrap()
            .is_none()
    );
}
