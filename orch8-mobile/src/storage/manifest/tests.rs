use super::*;
use orch8_storage::sqlite::SqliteStorage;
use std::sync::Arc;

#[tokio::test]
async fn legacy_cursor_is_preserved_and_late_evaluation_cannot_overwrite_new_acceptance() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    storage
        .set_sync_metadata("last_manifest_version", "5")
        .await
        .unwrap();
    storage
        .set_sync_metadata("manifest_etag", "old")
        .await
        .unwrap();
    let five = "5".repeat(64);
    let six = "6".repeat(64);
    assert!(storage.accept_manifest_trust(&[], 4, &five).await.is_err());
    assert_eq!(storage.manifest_trust().await.unwrap().floor(), Some(5));
    storage
        .accept_manifest_trust(&[("old", "old-key")], 5, &five)
        .await
        .unwrap();
    assert!(!storage.manifest_trust().await.unwrap().pending());
    storage
        .accept_manifest_trust(&[("new", "new-key")], 6, &six)
        .await
        .unwrap();
    assert!(
        storage
            .commit_manifest_evaluation(
                &[
                    ("last_manifest_version", Some("5")),
                    ("manifest_etag", Some("stale"))
                ],
                5,
                &five
            )
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .get_sync_metadata("manifest_etag")
            .await
            .unwrap()
            .as_deref(),
        Some("old")
    );
    storage
        .commit_manifest_evaluation(
            &[
                ("last_manifest_version", Some("6")),
                ("manifest_etag", Some("fresh")),
            ],
            6,
            &six,
        )
        .await
        .unwrap();
    assert!(!storage.manifest_trust().await.unwrap().pending());
    assert!(storage.accept_manifest_trust(&[], 6, &five).await.is_err());
    assert_eq!(
        storage.list_trusted_keys().await.unwrap(),
        vec![("new".into(), "new-key".into())]
    );
}

#[tokio::test]
async fn malformed_trust_records_fail_closed() {
    let cases = [
        vec![(ACCEPTED_VERSION, "2")],
        vec![(ACCEPTED_DIGEST, "bad")],
        vec![(ACCEPTED_VERSION, "x"), (ACCEPTED_DIGEST, "bad")],
        vec![(ACCEPTED_VERSION, "-1"), (ACCEPTED_DIGEST, "bad")],
        vec![("last_manifest_version", "-1")],
        vec![("last_manifest_version", "9223372036854775808")],
        vec![(ACCEPTED_VERSION, "2"), (ACCEPTED_DIGEST, "bad")],
    ];
    for records in cases {
        let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
        for (key, value) in records {
            storage.set_sync_metadata(key, value).await.unwrap();
        }
        assert!(storage.manifest_trust().await.is_err());
        assert!(
            storage
                .accept_manifest_trust(&[("new", "new-key")], 3, &"3".repeat(64))
                .await
                .is_err()
        );
        assert!(storage.list_trusted_keys().await.unwrap().is_empty());
    }
    let digest = "f".repeat(64);
    assert!(
        ManifestTrust::from_rows(vec![
            (ACCEPTED_VERSION.into(), "2".into()),
            (ACCEPTED_DIGEST.into(), digest),
            ("last_manifest_version".into(), "3".into())
        ])
        .is_err()
    );
}

#[tokio::test]
async fn durable_acceptance_comparison_happens_after_the_writer_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("compare.db");
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let other = Arc::new(MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    )));
    storage
        .accept_manifest_trust(&[("one", "key-one")], 1, &"1".repeat(64))
        .await
        .unwrap();
    let mut newer = storage.pool().begin().await.unwrap();
    sqlx::query("UPDATE sync_metadata SET value = '3' WHERE key = 'accepted_manifest_version'")
        .execute(&mut *newer)
        .await
        .unwrap();
    let older = {
        let other = other.clone();
        tokio::spawn(async move {
            other
                .accept_manifest_trust(&[("two", "key-two")], 2, &"2".repeat(64))
                .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!older.is_finished());
    sqlx::query("UPDATE sync_metadata SET value = ?1 WHERE key = 'accepted_manifest_digest'")
        .bind("3".repeat(64))
        .execute(&mut *newer)
        .await
        .unwrap();
    sqlx::query("DELETE FROM trusted_keys; INSERT INTO trusted_keys (key_id,public_key) VALUES ('three','key-three')").execute(&mut *newer).await.unwrap();
    newer.commit().await.unwrap();
    assert!(older.await.unwrap().is_err());
    assert_eq!(
        storage.manifest_trust().await.unwrap().accepted_version,
        Some(3)
    );
    assert_eq!(
        storage.list_trusted_keys().await.unwrap(),
        vec![("three".into(), "key-three".into())]
    );
}

#[tokio::test]
async fn evaluation_cannot_rewrite_acceptance_or_mislabel_its_applied_version() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let digest = "1".repeat(64);
    storage
        .accept_manifest_trust(&[("one", "one-key")], 1, &digest)
        .await
        .unwrap();
    for (key, value) in [
        (ACCEPTED_VERSION, Some("0")),
        (ACCEPTED_DIGEST, Some("different")),
        ("last_manifest_version", Some("2")),
        ("last_manifest_version", None),
    ] {
        assert!(
            storage
                .commit_manifest_evaluation(&[(key, value)], 1, &digest)
                .await
                .is_err()
        );
    }
    storage
        .commit_manifest_evaluation(&[("last_sync_ts", Some("partial"))], 1, &digest)
        .await
        .unwrap();
    let trust = storage.manifest_trust().await.unwrap();
    assert_eq!(
        (trust.accepted_version, trust.applied_version),
        (Some(1), None)
    );
    trust.check_candidate(1, &digest).unwrap();
}
