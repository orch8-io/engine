use super::*;

fn serve_empty_manifest(fixture: &Fixture, version: i64, etag: &str) {
    let body = serde_json::to_vec(&serde_json::json!({
        "signing_keys": [], "sequences": [], "removed": [],
        "manifest_version": version, "generated_at": Utc::now(),
    }))
    .unwrap();
    fixture.origin.script(
        "/manifest",
        vec![Reply {
            status: "200 OK",
            headers: vec![("ETag", etag.to_owned())],
            body: crate::sync::tests::signed_manifest_bytes(&fixture.key, &body),
        }],
    );
}

#[tokio::test]
async fn slow_older_sync_cannot_resurrect_a_newer_manifest_removal() {
    let fixture = Fixture::new().await;
    let manifest_url = format!("{}/manifest", fixture.origin.base);
    fixture.origin.serve(
        "/manifest",
        fixture.manifest(&format!("{}/seq.json", fixture.entry.base)),
    );
    fixture.serve_entry(&fixture.entry, "");
    let gate = fixture.entry.hold("/seq.json");
    let orch = Arc::clone(&fixture.orch);
    let old_url = manifest_url.clone();
    let older = tokio::spawn(async move {
        orch.sync(
            &old_url,
            &SyncAuth::Bearer("manifest_secret".into()),
            &HashSet::new(),
        )
        .await
    });
    let entered = tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.entry.recorded().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let body = serde_json::to_vec(&serde_json::json!({
        "signing_keys": [], "sequences": [],
        "removed": [{"name": "origin-flow", "removed_at": Utc::now()}],
        "manifest_version": 2, "generated_at": Utc::now(),
    }))
    .unwrap();
    fixture.origin.serve(
        "/manifest",
        crate::sync::tests::signed_manifest_bytes(&fixture.key, &body),
    );
    let newer = fixture
        .orch
        .sync(
            &manifest_url,
            &SyncAuth::Bearer("manifest_secret".into()),
            &HashSet::new(),
        )
        .await;
    gate.add_permits(1);
    older.await.unwrap().unwrap();
    assert!(
        entered,
        "older sync must reach entry download before replacement"
    );
    if matches!(newer, Err(MobileError::ResourceLimit { .. })) {
        fixture
            .orch
            .sync(
                &manifest_url,
                &SyncAuth::Bearer("manifest_secret".into()),
                &HashSet::new(),
            )
            .await
            .unwrap();
    } else {
        newer.unwrap();
    }
    assert_eq!(
        fixture.orch.get_last_manifest_version().await.unwrap(),
        Some(2),
        "an older in-flight sync must not overwrite the committed replay fence"
    );
    assert!(
        !fixture
            .orch
            .list_local_sequences()
            .await
            .unwrap()
            .contains_key("origin-flow"),
        "a newer signed removal must not be resurrected by an older download"
    );
}

#[tokio::test]
async fn failed_metadata_commit_cannot_mix_etag_cursor_and_environment() {
    let fixture = Fixture::new().await;
    for (key, value) in [
        ("last_sync_ts", "2026-01-01T00:00:00Z"),
        ("manifest_etag", "old-etag"),
        ("last_manifest_version", "0"),
        (ENV_FINGERPRINT_KEY, "old-environment"),
    ] {
        fixture
            .orch
            .mobile_storage
            .set_sync_metadata(key, value)
            .await
            .unwrap();
    }
    sqlx::query("CREATE TRIGGER reject_manifest_cursor BEFORE INSERT ON sync_metadata WHEN NEW.key = 'last_manifest_version' BEGIN SELECT RAISE(FAIL, 'injected metadata failure'); END")
        .execute(fixture.orch.mobile_storage.pool()).await.unwrap();
    serve_empty_manifest(&fixture, 1, "new-etag");
    assert!(
        fixture
            .orch
            .sync(
                &format!("{}/manifest", fixture.origin.base),
                &SyncAuth::UrlToken,
                &HashSet::new()
            )
            .await
            .is_err()
    );
    for (key, value) in [
        ("last_sync_ts", "2026-01-01T00:00:00Z"),
        ("manifest_etag", "old-etag"),
        ("last_manifest_version", "0"),
        (ENV_FINGERPRINT_KEY, "old-environment"),
    ] {
        assert_eq!(
            fixture
                .orch
                .mobile_storage
                .get_sync_metadata(key)
                .await
                .unwrap()
                .as_deref(),
            Some(value),
            "failed sync metadata changed {key}"
        );
    }
}

async fn durable_fixture() -> (
    Fixture,
    tempfile::TempDir,
    sqlx::pool::PoolConnection<sqlx::Sqlite>,
) {
    let directory = tempfile::tempdir().unwrap();
    let sqlite = Arc::new(
        SqliteStorage::file_mobile(directory.path().join("owned-sync.db").to_str().unwrap())
            .await
            .unwrap(),
    );
    let fixture = Fixture::with_storage(sqlite).await;
    let storage = Arc::clone(&fixture.orch.mobile_storage);
    // Open the WAL reader before holding the writer. Opening a fresh
    // connection runs connection pragmas that themselves need a DB lock.
    let writer = storage.pool().acquire().await.unwrap();
    let reader = storage.pool().acquire().await.unwrap();
    drop(writer);
    (fixture, directory, reader)
}

#[tokio::test]
async fn cancelled_caller_retains_admission_until_sqlite_commit_settles() {
    let (fixture, _directory, mut reader) = durable_fixture().await;
    let storage = Arc::clone(&fixture.orch.mobile_storage);
    let previous = [
        ("last_sync_ts", "2026-01-01T00:00:00Z"),
        ("manifest_etag", "old-etag"),
        ("last_manifest_version", "0"),
        (ENV_FINGERPRINT_KEY, "old-env"),
    ];
    for (key, value) in previous {
        storage.set_sync_metadata(key, value).await.unwrap();
    }
    serve_empty_manifest(&fixture, 1, "new-etag");
    let (entered, release) = storage.pause_next_metadata_commit();
    let orch = Arc::clone(&fixture.orch);
    let url = format!("{}/manifest", fixture.origin.base);
    let caller_url = url.clone();
    let caller = tokio::spawn(async move {
        orch.sync(&caller_url, &SyncAuth::UrlToken, &HashSet::new())
            .await
    });
    let began = tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .is_ok();
    caller.abort();
    let _ = caller.await;
    if !began {
        release.add_permits(1);
        panic!("metadata transaction did not reach its write barrier");
    }
    let mut before = Vec::new();
    for (key, _) in previous {
        before.push(
            sqlx::query_scalar::<_, String>("SELECT value FROM sync_metadata WHERE key = ?1")
                .bind(key)
                .fetch_optional(&mut *reader)
                .await,
        );
    }
    let concurrent = tokio::time::timeout(
        Duration::from_secs(1),
        fixture
            .orch
            .sync(&url, &SyncAuth::UrlToken, &HashSet::new()),
    )
    .await;
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.orch.sync_in_progress() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        concurrent,
        Ok(Err(MobileError::ResourceLimit { .. }))
    ));
    for ((key, value), observed) in previous.into_iter().zip(before) {
        assert_eq!(
            observed.unwrap().as_deref(),
            Some(value),
            "uncommitted {key} leaked to readers"
        );
    }
    assert_eq!(
        storage
            .get_sync_metadata("last_manifest_version")
            .await
            .unwrap()
            .as_deref(),
        Some("1")
    );
    assert_eq!(
        storage
            .get_sync_metadata("manifest_etag")
            .await
            .unwrap()
            .as_deref(),
        Some("new-etag")
    );
    assert_eq!(
        storage
            .get_sync_metadata(ENV_FINGERPRINT_KEY)
            .await
            .unwrap()
            .as_deref(),
        Some(fixture.orch.env_fingerprint(&HashSet::new()).as_str())
    );
    fixture
        .orch
        .sync(&url, &SyncAuth::UrlToken, &HashSet::new())
        .await
        .unwrap();
}

#[tokio::test]
async fn cancelling_download_releases_admission_and_prevents_further_mutation() {
    let fixture = Fixture::new().await;
    let url = format!("{}/manifest", fixture.origin.base);
    fixture.origin.serve(
        "/manifest",
        fixture.manifest(&format!("{}/seq.json", fixture.entry.base)),
    );
    fixture.serve_entry(&fixture.entry, "");
    let gate = fixture.entry.hold("/seq.json");
    let orch = Arc::clone(&fixture.orch);
    let caller_url = url.clone();
    let caller = tokio::spawn(async move {
        orch.sync(&caller_url, &SyncAuth::UrlToken, &HashSet::new())
            .await
    });
    let began = tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.entry.recorded().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    caller.abort();
    let _ = caller.await;
    let settled = tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.orch.sync_in_progress() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    gate.add_permits(1);
    assert!(began && settled);
    assert_eq!(
        fixture.entry.recorded().len(),
        1,
        "cancelled download must not request a signature"
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
async fn successful_representation_without_etag_retires_an_old_validator() {
    let fixture = Fixture::new().await;
    fixture
        .orch
        .mobile_storage
        .set_sync_metadata("manifest_etag", "old-etag")
        .await
        .unwrap();
    fixture
        .orch
        .persist_sync_metadata(None, Some(2), Some("new-env"))
        .await
        .unwrap();
    assert_eq!(
        fixture
            .orch
            .mobile_storage
            .get_sync_metadata("manifest_etag")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        fixture.orch.get_last_manifest_version().await.unwrap(),
        Some(2)
    );
}

#[tokio::test]
async fn already_retired_run_does_not_invoke_authorization_factory() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let fixture = Fixture::new().await;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let result = fixture
        .orch
        .sync_with_auth(
            &format!("{}/manifest", fixture.origin.base),
            &HashSet::new(),
            cancelled,
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(SyncAuth::UrlToken))
            },
        )
        .await;
    assert!(matches!(result, Err(MobileError::Shutdown)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!fixture.orch.sync_in_progress());
    assert!(fixture.origin.recorded().is_empty());
}

#[tokio::test]
async fn busy_syncs_reject_burst_before_authorization_or_transport() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let fixture = Fixture::new().await;
    let url = format!("{}/manifest", fixture.origin.base);
    fixture.origin.serve(
        "/manifest",
        fixture.manifest(&format!("{}/seq.json", fixture.entry.base)),
    );
    fixture.serve_entry(&fixture.entry, "");
    let release = fixture.entry.hold("/seq.json");
    let orch = Arc::clone(&fixture.orch);
    let caller_url = url.clone();
    let caller = tokio::spawn(async move {
        orch.sync(&caller_url, &SyncAuth::UrlToken, &HashSet::new())
            .await
    });
    let began = tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.entry.recorded().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rejected = 0;
    for _ in 0..128 {
        let observed = Arc::clone(&calls);
        let result = fixture
            .orch
            .sync_with_auth(&url, &HashSet::new(), CancellationToken::new(), move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(SyncAuth::UrlToken))
            })
            .await;
        if matches!(result, Err(MobileError::ResourceLimit { .. })) {
            rejected += 1;
        }
    }
    caller.abort();
    let _ = caller.await;
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.orch.sync_in_progress() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(began);
    assert_eq!(rejected, 128);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.origin.recorded().len(), 1);
    assert_eq!(fixture.entry.recorded().len(), 1);
}
