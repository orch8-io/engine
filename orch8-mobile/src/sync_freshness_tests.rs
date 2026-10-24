use super::*;

fn sign(fixture: &Fixture, body: serde_json::Value) -> Vec<u8> {
    crate::sync::tests::signed_manifest_bytes(&fixture.key, &serde_json::to_vec(&body).unwrap())
}

async fn sync_now(fixture: &Fixture) -> Result<SyncResult, MobileError> {
    fixture
        .orch
        .sync(
            &format!("{}/manifest", fixture.origin.base),
            &SyncAuth::UrlToken,
            &HashSet::new(),
        )
        .await
}

fn not_modified() -> Reply {
    Reply {
        status: "304 Not Modified",
        headers: vec![],
        body: vec![],
    }
}

#[tokio::test]
async fn interrupted_newer_manifest_cannot_be_rolled_back_after_database_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("freshness.db");
    let sqlite = Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    );
    let mut fixture = Fixture::with_storage(sqlite.clone()).await;
    fixture
        .origin
        .serve("/manifest", fixture.manifest("/seq.json"));
    fixture.serve_entry(&fixture.origin, "");
    assert_eq!(sync_now(&fixture).await.unwrap().added, 1);

    // Version 2 was signed/published previously, but this device never applied
    // it. It remains an authentic replay candidate above the applied cursor 1.
    let mut sequence: serde_json::Value = serde_json::from_slice(&fixture.sequence).unwrap();
    sequence["version"] = 2.into();
    fixture.sequence = serde_json::to_vec(&sequence).unwrap();
    let bytes = fixture.manifest("/seq.json");
    let split = bytes.iter().position(|b| *b == b'\n').unwrap();
    let mut old: serde_json::Value = serde_json::from_slice(&bytes[split + 1..]).unwrap();
    old["manifest_version"] = 2.into();
    old["sequences"][0]["version"] = 2.into();
    let old = sign(&fixture, old);
    let newer = sign(
        &fixture,
        serde_json::json!({
            "signing_keys": [{"key_id":"k2", "public_key":BASE64.encode(fixture.key.verifying_key().as_bytes())}],
            "sequences": [{"name":"replacement", "version":1, "url":"/missing.json", "signing_key_id":"k2", "sha256":"unused-until-download", "required_handlers":[], "min_sdk_version":"0.0.0"}],
            "removed": [{"name":"origin-flow", "removed_at":Utc::now()}],
            "manifest_version":3, "generated_at":Utc::now(),
        }),
    );
    fixture.origin.serve("/manifest", newer);
    let partial = sync_now(&fixture).await.unwrap();
    assert_eq!((partial.removed, partial.skipped), (1, 1));
    assert_eq!(
        fixture.orch.get_last_manifest_version().await.unwrap(),
        Some(1)
    );
    assert!(
        fixture
            .orch
            .mobile_storage
            .get_trusted_key("k1")
            .await
            .unwrap()
            .is_none()
    );
    sqlite.pool().close().await;
    drop(fixture);
    drop(sqlite);

    let sqlite = Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    );
    let mut fixture = Fixture::with_storage(sqlite).await;
    fixture.sequence = serde_json::to_vec(&sequence).unwrap();
    fixture.origin.serve("/manifest", old);
    fixture.serve_entry(&fixture.origin, "");
    let result = sync_now(&fixture).await.unwrap();
    assert_eq!(
        result.added, 0,
        "a manifest below the observed version 3 must not reinstall withdrawn workflows"
    );
    assert!(
        fixture
            .orch
            .list_local_sequences()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        fixture
            .orch
            .mobile_storage
            .get_trusted_key("k1")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture.origin.recorded().len(),
        1,
        "reject replay before sequence/signature requests"
    );
}

#[tokio::test]
async fn pending_document_bypasses_old_etag_and_retries_until_evaluated() {
    let fixture = Fixture::new().await;
    let storage = &fixture.orch.mobile_storage;
    storage
        .set_sync_metadata("last_manifest_version", "0")
        .await
        .unwrap();
    storage
        .set_sync_metadata("manifest_etag", "old-etag")
        .await
        .unwrap();
    storage
        .set_sync_metadata(
            ENV_FINGERPRINT_KEY,
            &fixture.orch.env_fingerprint(&HashSet::new()),
        )
        .await
        .unwrap();
    let bytes = fixture.manifest("/seq.json");
    let response = Reply {
        status: "200 OK",
        headers: vec![("ETag", "new-etag".into())],
        body: bytes,
    };
    fixture.origin.script("/manifest", vec![response.clone()]);
    let result = sync_now(&fixture).await.unwrap();
    assert_eq!(result.skipped, 1);
    assert_eq!(
        fixture.origin.recorded()[0].if_none_match.as_deref(),
        Some("old-etag")
    );
    assert!(storage.manifest_trust().await.unwrap().pending());
    fixture.origin.script("/manifest", vec![not_modified()]);
    assert!(sync_now(&fixture).await.is_err());
    assert!(
        fixture
            .origin
            .recorded()
            .last()
            .unwrap()
            .if_none_match
            .is_none()
    );
    assert_eq!(
        fixture.orch.get_last_manifest_version().await.unwrap(),
        Some(0)
    );
    fixture.origin.script("/manifest", vec![response]);
    fixture.serve_entry(&fixture.origin, "");
    let result = sync_now(&fixture).await.unwrap();
    assert_eq!(
        result.added, 1,
        "the identical accepted document must remain retryable"
    );
    assert!(!storage.manifest_trust().await.unwrap().pending());
    let roots: Vec<_> = fixture
        .origin
        .recorded()
        .into_iter()
        .filter(|request| request.path == "/manifest")
        .collect();
    assert!(roots[2].if_none_match.is_none());
    fixture.origin.script("/manifest", vec![not_modified()]);
    assert_eq!(sync_now(&fixture).await.unwrap().added, 0);
    assert_eq!(
        fixture
            .origin
            .recorded()
            .last()
            .unwrap()
            .if_none_match
            .as_deref(),
        Some("new-etag")
    );
}

#[tokio::test]
async fn same_version_content_changes_fail_even_after_environment_change() {
    for installed in [false, true] {
        let fixture = Fixture::new().await;
        let bytes = fixture.manifest("/seq.json");
        fixture.origin.serve("/manifest", bytes.clone());
        if installed {
            fixture.serve_entry(&fixture.origin, "");
        }
        sync_now(&fixture).await.unwrap();
        let before = fixture.origin.recorded().len();
        let split = bytes.iter().position(|b| *b == b'\n').unwrap();
        let mut body: serde_json::Value = serde_json::from_slice(&bytes[split + 1..]).unwrap();
        body["signing_keys"] = serde_json::json!([]);
        body["sequences"] = serde_json::json!([]);
        body["removed"] = serde_json::json!([{"name":"origin-flow", "removed_at":Utc::now()}]);
        fixture.origin.serve("/manifest", sign(&fixture, body));
        let changed_environment = HashSet::from(["new-handler".into()]);
        assert!(
            fixture
                .orch
                .sync(
                    &format!("{}/manifest", fixture.origin.base),
                    &SyncAuth::UrlToken,
                    &changed_environment
                )
                .await
                .is_err()
        );
        assert_eq!(fixture.origin.recorded().len(), before + 1);
        assert!(
            fixture
                .orch
                .mobile_storage
                .get_trusted_key("k1")
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            fixture
                .orch
                .list_local_sequences()
                .await
                .unwrap()
                .contains_key("origin-flow"),
            installed
        );
    }
}

#[tokio::test]
async fn unverified_or_invalid_documents_cannot_create_an_acceptance_fence() {
    for invalid_key in [false, true] {
        let fixture = Fixture::new().await;
        let mut bytes = fixture.manifest("/seq.json");
        if invalid_key {
            let split = bytes.iter().position(|b| *b == b'\n').unwrap();
            let mut body: serde_json::Value = serde_json::from_slice(&bytes[split + 1..]).unwrap();
            body["signing_keys"][0]["public_key"] = "invalid-base64".into();
            bytes = sign(&fixture, body);
        } else {
            bytes[0] = if bytes[0] == b'A' { b'B' } else { b'A' };
        }
        fixture.origin.serve("/manifest", bytes);
        assert!(sync_now(&fixture).await.is_err());
        assert_eq!(
            fixture
                .orch
                .mobile_storage
                .manifest_trust()
                .await
                .unwrap()
                .accepted_version,
            None
        );
        assert!(
            fixture
                .orch
                .mobile_storage
                .list_trusted_keys()
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(fixture.origin.recorded().len(), 1);
    }
}

#[tokio::test]
async fn failed_acceptance_digest_write_rolls_back_keys_and_prevents_removal() {
    let fixture = Fixture::new().await;
    fixture
        .origin
        .serve("/manifest", fixture.manifest("/seq.json"));
    fixture.serve_entry(&fixture.origin, "");
    sync_now(&fixture).await.unwrap();
    sqlx::query("CREATE TRIGGER refuse_manifest_digest BEFORE INSERT ON sync_metadata WHEN NEW.key = 'accepted_manifest_digest' BEGIN SELECT RAISE(ABORT, 'injected acceptance failure'); END")
        .execute(fixture.orch.mobile_storage.pool()).await.unwrap();
    fixture.origin.serve("/manifest", sign(&fixture, serde_json::json!({
        "signing_keys":[], "sequences":[], "removed":[{"name":"origin-flow","removed_at":Utc::now()}],
        "manifest_version":2,"generated_at":Utc::now(),
    })));
    assert!(sync_now(&fixture).await.is_err());
    let trust = fixture.orch.mobile_storage.manifest_trust().await.unwrap();
    assert_eq!(
        (trust.accepted_version, trust.applied_version),
        (Some(1), Some(1))
    );
    assert!(
        fixture
            .orch
            .mobile_storage
            .get_trusted_key("k1")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        fixture
            .orch
            .list_local_sequences()
            .await
            .unwrap()
            .contains_key("origin-flow")
    );
}

#[tokio::test]
async fn signature_line_reencoding_does_not_change_the_accepted_content() {
    let fixture = Fixture::new().await;
    let bytes = fixture.manifest("/seq.json");
    fixture.origin.serve("/manifest", bytes.clone());
    sync_now(&fixture).await.unwrap();
    let split = bytes.iter().position(|b| *b == b'\n').unwrap();
    let mut reencoded = bytes[..split].to_vec();
    reencoded.extend_from_slice(b"  \n");
    reencoded.extend_from_slice(&bytes[split + 1..]);
    fixture.origin.serve("/manifest", reencoded);
    fixture.serve_entry(&fixture.origin, "");
    assert_eq!(sync_now(&fixture).await.unwrap().added, 1);
}

#[tokio::test]
async fn orphan_etag_without_an_applied_document_cannot_shortcut_initial_sync() {
    let fixture = Fixture::new().await;
    fixture
        .orch
        .mobile_storage
        .set_sync_metadata("manifest_etag", "orphan")
        .await
        .unwrap();
    fixture
        .orch
        .mobile_storage
        .set_sync_metadata(
            ENV_FINGERPRINT_KEY,
            &fixture.orch.env_fingerprint(&HashSet::new()),
        )
        .await
        .unwrap();
    fixture.origin.script("/manifest", vec![not_modified()]);
    assert!(sync_now(&fixture).await.is_err());
    assert!(fixture.origin.recorded()[0].if_none_match.is_none());
    assert_eq!(
        fixture
            .orch
            .mobile_storage
            .manifest_trust()
            .await
            .unwrap()
            .floor(),
        None
    );
}
