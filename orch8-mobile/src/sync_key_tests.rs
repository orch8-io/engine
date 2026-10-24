use super::*;

fn signed_with_keys(fixture: &Fixture, keys: serde_json::Value) -> Vec<u8> {
    let bytes = fixture.manifest("/seq.json");
    let split = bytes.iter().position(|b| *b == b'\n').unwrap();
    let mut body: serde_json::Value = serde_json::from_slice(&bytes[split + 1..]).unwrap();
    body["signing_keys"] = keys;
    super::super::tests::signed_manifest_bytes(&fixture.key, &serde_json::to_vec(&body).unwrap())
}

#[tokio::test]
async fn failed_key_revocation_cannot_install_using_the_revoked_cache_entry() {
    let fixture = Fixture::new().await;
    fixture
        .orch
        .mobile_storage
        .upsert_trusted_key("k1", &BASE64.encode(fixture.key.verifying_key().as_bytes()))
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER refuse_key_revocation BEFORE DELETE ON trusted_keys WHEN OLD.key_id = 'k1' BEGIN SELECT RAISE(ABORT, 'injected revocation failure'); END")
        .execute(fixture.orch.mobile_storage.pool()).await.unwrap();
    fixture.origin.serve(
        "/manifest",
        signed_with_keys(&fixture, serde_json::json!([])),
    );
    fixture.serve_entry(&fixture.origin, "");
    let result = fixture
        .orch
        .sync(
            &format!("{}/manifest", fixture.origin.base),
            &SyncAuth::UrlToken,
            &HashSet::new(),
        )
        .await;
    assert!(
        result.is_err(),
        "a failed authoritative revocation must abort before workflow admission; got {result:?}"
    );
    assert!(
        fixture
            .origin
            .recorded()
            .iter()
            .all(|request| request.path == "/manifest")
    );
    assert!(
        fixture
            .orch
            .list_local_sequences()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture.orch.get_last_manifest_version().await.unwrap(),
        None
    );
}

#[tokio::test]
async fn invalid_later_key_does_not_partially_replace_the_trust_set() {
    let fixture = Fixture::new().await;
    fixture
        .orch
        .mobile_storage
        .upsert_trusted_key("old", "previous-trust-set")
        .await
        .unwrap();
    let manifest = Manifest {
        signing_keys: vec![
            SigningKeyEntry {
                key_id: "new".into(),
                public_key: BASE64.encode(fixture.key.verifying_key().as_bytes()),
            },
            SigningKeyEntry {
                key_id: "bad".into(),
                public_key: "invalid-base64".into(),
            },
        ],
        sequences: vec![],
        removed: vec![],
        manifest_version: 1,
        generated_at: Utc::now(),
    };
    assert!(
        fixture
            .orch
            .cache_test_signing_keys(&manifest)
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .orch
            .mobile_storage
            .list_trusted_keys()
            .await
            .unwrap(),
        vec![("old".into(), "previous-trust-set".into())]
    );
}

#[tokio::test]
async fn failed_key_insert_rolls_back_revocation_and_all_other_inserts() {
    let fixture = Fixture::new().await;
    fixture
        .orch
        .mobile_storage
        .upsert_trusted_key("old", "old-public-key")
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER refuse_key_insert BEFORE INSERT ON trusted_keys WHEN NEW.key_id = 'reject' BEGIN SELECT RAISE(ABORT, 'injected insert failure'); END")
        .execute(fixture.orch.mobile_storage.pool()).await.unwrap();
    let public_key = BASE64.encode(fixture.key.verifying_key().as_bytes());
    let manifest = Manifest {
        signing_keys: vec![
            SigningKeyEntry {
                key_id: "new".into(),
                public_key: public_key.clone(),
            },
            SigningKeyEntry {
                key_id: "reject".into(),
                public_key,
            },
        ],
        sequences: vec![],
        removed: vec![],
        manifest_version: 1,
        generated_at: Utc::now(),
    };
    assert!(
        fixture
            .orch
            .cache_test_signing_keys(&manifest)
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .orch
            .mobile_storage
            .list_trusted_keys()
            .await
            .unwrap(),
        vec![("old".into(), "old-public-key".into())]
    );
}

#[tokio::test]
async fn unlisted_cached_key_cannot_verify_a_sequence() {
    let fixture = Fixture::new().await;
    fixture
        .orch
        .mobile_storage
        .upsert_trusted_key("k1", &BASE64.encode(fixture.key.verifying_key().as_bytes()))
        .await
        .unwrap();
    fixture.serve_entry(&fixture.origin, "");
    let bytes = fixture.manifest("/seq.json");
    let split = bytes.iter().position(|b| *b == b'\n').unwrap();
    let manifest: Manifest = serde_json::from_slice(&bytes[split + 1..]).unwrap();
    // Exercise the verification boundary directly: a stale cache row is not
    // authorization, regardless of how/when the current set was persisted.
    let (added, _, _, failures, _) = fixture
        .orch
        .download_and_verify_sequences(
            &format!("{}/manifest", fixture.origin.base),
            &manifest,
            &SyncAuth::UrlToken
                .for_manifest(&format!("{}/manifest", fixture.origin.base))
                .unwrap(),
            &HashSet::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!((added, failures), (0, 1));
    assert!(
        fixture.origin.recorded().is_empty(),
        "unlisted keys must be refused before network admission"
    );
    assert!(
        fixture
            .orch
            .list_local_sequences()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn empty_or_duplicate_manifest_key_ids_are_rejected_before_mutation() {
    let fixture = Fixture::new().await;
    fixture
        .orch
        .mobile_storage
        .upsert_trusted_key("old", "old-public-key")
        .await
        .unwrap();
    for ids in [vec![""], vec!["  "], vec!["same", "same"]] {
        let manifest = Manifest {
            signing_keys: ids
                .into_iter()
                .map(|id| SigningKeyEntry {
                    key_id: id.into(),
                    public_key: BASE64.encode(fixture.key.verifying_key().as_bytes()),
                })
                .collect(),
            sequences: vec![],
            removed: vec![],
            manifest_version: 1,
            generated_at: Utc::now(),
        };
        assert!(
            fixture
                .orch
                .cache_test_signing_keys(&manifest)
                .await
                .is_err()
        );
        assert_eq!(
            fixture
                .orch
                .mobile_storage
                .list_trusted_keys()
                .await
                .unwrap(),
            vec![("old".into(), "old-public-key".into())]
        );
    }
}
