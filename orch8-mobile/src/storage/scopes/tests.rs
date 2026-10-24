use super::*;
use orch8_storage::sqlite::SqliteStorage;
use std::sync::Arc;

async fn fixture() -> (Arc<MobileStorage>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let sqlite = SqliteStorage::file_mobile(dir.path().join("scopes.db").to_str().unwrap())
        .await
        .unwrap();
    (Arc::new(MobileStorage::new(Arc::new(sqlite))), dir)
}

async fn catalog(storage: &MobileStorage, account: &AccountScope, name: &str) -> CatalogScope {
    storage
        .open_catalog_scope(
            account,
            name,
            "https://catalog.example/stable",
            "root-key-1",
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn two_accounts_with_two_catalogs_have_independent_state() {
    let (storage, _dir) = fixture().await;
    storage
        .set_sync_metadata("version", "legacy")
        .await
        .unwrap();
    let a = storage
        .open_account_scope("a", "issuer", "subject-a")
        .await
        .unwrap();
    let b = storage
        .open_account_scope("b", "issuer", "subject-b")
        .await
        .unwrap();
    let mut scopes = Vec::new();
    for account in [&a, &b] {
        for key in ["one", "two"] {
            scopes.push(catalog(&storage, account, key).await);
        }
    }
    for (index, scope) in scopes.iter().enumerate() {
        assert_eq!(
            storage.catalog_metadata(scope, "version").await.unwrap(),
            None
        );
        storage
            .update_catalog_metadata(
                scope,
                &[
                    ("version", Some(&index.to_string())),
                    ("etag", Some("same-key")),
                ],
            )
            .await
            .unwrap();
    }
    storage
        .update_catalog_metadata(&scopes[0], &[("etag", None)])
        .await
        .unwrap();
    for (index, scope) in scopes.iter().enumerate() {
        assert_eq!(
            storage.catalog_metadata(scope, "version").await.unwrap(),
            Some(index.to_string())
        );
        assert_eq!(
            storage.catalog_metadata(scope, "etag").await.unwrap(),
            if index == 0 {
                None
            } else {
                Some("same-key".into())
            }
        );
    }
    assert_eq!(
        storage
            .get_sync_metadata("version")
            .await
            .unwrap()
            .as_deref(),
        Some("legacy")
    );
    let forged = CatalogScope {
        account: b,
        id: scopes[0].id.clone(),
    };
    assert!(storage.catalog_metadata(&forged, "version").await.is_err());
    assert!(
        storage
            .update_catalog_metadata(&forged, &[("version", Some("stolen"))])
            .await
            .is_err()
    );
    // The composite FK also rejects a mixed owner below the Rust API.
    assert!(
        sqlx::query("INSERT INTO mobile_catalog_metadata VALUES (?1, ?2, 'x', 'y')")
            .bind(&forged.account.id)
            .bind(&forged.id)
            .execute(storage.pool())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn bindings_are_immutable_and_do_not_echo_private_descriptors() {
    let (storage, _dir) = fixture().await;
    let a = storage
        .open_account_scope("a", "issuer", "subject")
        .await
        .unwrap();
    let c = catalog(&storage, &a, "one").await;
    storage
        .update_catalog_metadata(&c, &[("version", Some("9"))])
        .await
        .unwrap();
    for (issuer, subject) in [("other-issuer", "subject"), ("issuer", "secret-subject")] {
        let err = storage
            .open_account_scope("a", issuer, subject)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, StorageError::Conflict(_)));
        assert!(!err.to_string().contains("secret-subject"));
    }
    for (source, authority) in [
        ("https://other.example/?private-token", "root-key-1"),
        ("https://catalog.example/stable", "other-root"),
    ] {
        let err = storage
            .open_catalog_scope(&a, "one", source, authority)
            .await
            .err()
            .unwrap();
        assert!(!err.to_string().contains("private-token"));
    }
    assert_eq!(catalog(&storage, &a, "one").await.id, c.id);
    assert_eq!(
        storage
            .catalog_metadata(&c, "version")
            .await
            .unwrap()
            .as_deref(),
        Some("9")
    );
}

#[tokio::test]
async fn retirement_and_reopen_fence_handles_across_independent_pools() {
    let (storage, dir) = fixture().await;
    let a = storage
        .open_account_scope("a", "issuer", "subject")
        .await
        .unwrap();
    let c = catalog(&storage, &a, "one").await;
    storage
        .update_catalog_metadata(&c, &[("cursor", Some("42")), ("etag", Some("validator"))])
        .await
        .unwrap();
    let other = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(dir.path().join("scopes.db").to_str().unwrap())
            .await
            .unwrap(),
    ));
    other.retire_account_scope(&a).await.unwrap();
    assert!(storage.catalog_metadata(&c, "cursor").await.is_err());
    assert!(catalog_result(&storage, &a).await.is_err());
    let reopened = other
        .open_account_scope("a", "issuer", "subject")
        .await
        .unwrap();
    assert_eq!(reopened.id, a.id);
    assert_eq!(reopened.epoch, a.epoch + 1);
    assert!(storage.retire_account_scope(&a).await.is_err());
    assert!(
        storage
            .update_catalog_metadata(&c, &[("cursor", Some("1"))])
            .await
            .is_err()
    );
    let current = catalog(&other, &reopened, "one").await;
    assert_eq!(current.id, c.id);
    assert_eq!(
        other
            .catalog_metadata(&current, "cursor")
            .await
            .unwrap()
            .as_deref(),
        Some("42")
    );
    assert_eq!(
        other
            .catalog_metadata(&current, "etag")
            .await
            .unwrap()
            .as_deref(),
        Some("validator")
    );
}

async fn catalog_result(
    storage: &MobileStorage,
    account: &AccountScope,
) -> Result<CatalogScope, StorageError> {
    storage
        .open_catalog_scope(
            account,
            "one",
            "https://catalog.example/stable",
            "root-key-1",
        )
        .await
}

#[tokio::test]
async fn competing_pool_opens_converge_on_one_identity_and_catalog() {
    let (storage, dir) = fixture().await;
    let other = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(dir.path().join("scopes.db").to_str().unwrap())
            .await
            .unwrap(),
    ));
    let (a, b) = tokio::join!(
        storage.open_account_scope("a", "issuer", "subject"),
        other.open_account_scope("a", "issuer", "subject")
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!((a.id.clone(), a.epoch), (b.id.clone(), b.epoch));
    let (c, d) = tokio::join!(catalog(&storage, &a, "one"), catalog(&other, &b, "one"));
    assert_eq!(c.id, d.id);
}

#[tokio::test]
async fn failed_scoped_batch_rolls_back_every_key() {
    let (storage, _dir) = fixture().await;
    let a = storage
        .open_account_scope("a", "issuer", "subject")
        .await
        .unwrap();
    let c = catalog(&storage, &a, "one").await;
    storage
        .update_catalog_metadata(&c, &[("etag", Some("old")), ("cursor", Some("9"))])
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_scoped_cursor BEFORE INSERT ON mobile_catalog_metadata WHEN NEW.key = 'cursor' BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
        .execute(storage.pool()).await.unwrap();
    assert!(
        storage
            .update_catalog_metadata(&c, &[("etag", Some("new")), ("cursor", Some("10"))])
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .catalog_metadata(&c, "etag")
            .await
            .unwrap()
            .as_deref(),
        Some("old")
    );
    assert_eq!(
        storage
            .catalog_metadata(&c, "cursor")
            .await
            .unwrap()
            .as_deref(),
        Some("9")
    );
}

#[tokio::test]
async fn admitted_write_settles_before_retirement_and_new_epoch() {
    let (storage, dir) = fixture().await;
    let a = storage
        .open_account_scope("a", "issuer", "subject")
        .await
        .unwrap();
    let c = catalog(&storage, &a, "one").await;
    let other = Arc::new(MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(dir.path().join("scopes.db").to_str().unwrap())
            .await
            .unwrap(),
    )));
    let mut tx = storage.pool().begin().await.unwrap();
    fence(&mut tx, &a).await.unwrap();
    sqlx::query("INSERT INTO mobile_catalog_metadata VALUES (?1, ?2, 'cursor', '42')")
        .bind(&a.id)
        .bind(&c.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    let retire = {
        let other = other.clone();
        let a = a.clone();
        tokio::spawn(async move { other.retire_account_scope(&a).await })
    };
    // SQLite must hold retirement behind the admitted transaction.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!retire.is_finished());
    tx.commit().await.unwrap();
    retire.await.unwrap().unwrap();
    assert!(
        storage
            .update_catalog_metadata(&c, &[("cursor", Some("1"))])
            .await
            .is_err()
    );
    let next = other
        .open_account_scope("a", "issuer", "subject")
        .await
        .unwrap();
    let current = catalog(&other, &next, "one").await;
    assert_eq!(
        other
            .catalog_metadata(&current, "cursor")
            .await
            .unwrap()
            .as_deref(),
        Some("42")
    );
}

#[tokio::test]
async fn invalid_descriptors_and_epoch_exhaustion_fail_closed() {
    let (storage, _dir) = fixture().await;
    for value in [
        String::new(),
        "  ".to_owned(),
        "a\0b".to_owned(),
        "x".repeat(4097),
    ] {
        assert!(
            storage
                .open_account_scope(&value, "issuer", "subject")
                .await
                .is_err()
        );
        assert!(
            storage
                .open_account_scope("a", &value, "subject")
                .await
                .is_err()
        );
        assert!(
            storage
                .open_account_scope("a", "issuer", &value)
                .await
                .is_err()
        );
    }
    let mut a = storage
        .open_account_scope("a", "issuer", "subject")
        .await
        .unwrap();
    for value in ["", "\n", "a\0b"] {
        assert!(
            storage
                .open_catalog_scope(&a, value, "source", "root")
                .await
                .is_err()
        );
        assert!(
            storage
                .open_catalog_scope(&a, "one", value, "root")
                .await
                .is_err()
        );
        assert!(
            storage
                .open_catalog_scope(&a, "one", "source", value)
                .await
                .is_err()
        );
    }
    sqlx::query("UPDATE mobile_accounts SET epoch = ?1 WHERE id = ?2")
        .bind(i64::MAX)
        .bind(&a.id)
        .execute(storage.pool())
        .await
        .unwrap();
    a.epoch = i64::MAX;
    storage.retire_account_scope(&a).await.unwrap();
    assert!(
        storage
            .open_account_scope("a", "issuer", "subject")
            .await
            .is_err()
    );
    assert!(catalog_result(&storage, &a).await.is_err());
}

#[tokio::test]
async fn legacy_upgrade_and_full_reopen_preserve_ownership_without_adoption() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("upgrade.db");
    let raw = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true),
    )
    .await
    .unwrap();
    sqlx::raw_sql("CREATE TABLE sync_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT (datetime('now'))); INSERT INTO sync_metadata(key,value) VALUES ('cursor','legacy-99'); CREATE TABLE trusted_keys (key_id TEXT PRIMARY KEY, public_key TEXT NOT NULL, trusted_since TEXT NOT NULL DEFAULT (datetime('now'))); INSERT INTO trusted_keys(key_id,public_key) VALUES ('root','legacy-key');")
        .execute(&raw).await.unwrap();
    raw.close().await;
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let a = storage
        .open_account_scope("a", "issuer", "subject")
        .await
        .unwrap();
    let c = catalog(&storage, &a, "one").await;
    assert_eq!(storage.catalog_metadata(&c, "cursor").await.unwrap(), None);
    storage
        .update_catalog_metadata(&c, &[("cursor", Some("7"))])
        .await
        .unwrap();
    storage.retire_account_scope(&a).await.unwrap();
    storage.pool().close().await;
    drop(storage);
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    assert!(storage.catalog_metadata(&c, "cursor").await.is_err());
    let reopened = storage
        .open_account_scope("a", "issuer", "subject")
        .await
        .unwrap();
    assert_eq!(reopened.id, a.id);
    assert_eq!(reopened.epoch, a.epoch + 1);
    let current = catalog(&storage, &reopened, "one").await;
    assert_eq!(current.id, c.id);
    assert_eq!(
        storage
            .catalog_metadata(&current, "cursor")
            .await
            .unwrap()
            .as_deref(),
        Some("7")
    );
    assert_eq!(
        storage
            .get_sync_metadata("cursor")
            .await
            .unwrap()
            .as_deref(),
        Some("legacy-99")
    );
    assert_eq!(
        storage.get_trusted_key("root").await.unwrap().as_deref(),
        Some("legacy-key")
    );
}

#[tokio::test]
async fn catalog_key_rotation_is_isolated_and_old_sessions_cannot_revoke_keys() {
    let (storage, dir) = fixture().await;
    storage
        .upsert_trusted_key("same", "legacy-key")
        .await
        .unwrap();
    let a = storage
        .open_account_scope("a", "issuer", "a")
        .await
        .unwrap();
    let b = storage
        .open_account_scope("b", "issuer", "b")
        .await
        .unwrap();
    let scopes = [
        catalog(&storage, &a, "one").await,
        catalog(&storage, &a, "two").await,
        catalog(&storage, &b, "one").await,
        catalog(&storage, &b, "two").await,
    ];
    for (index, scope) in scopes.iter().enumerate() {
        assert!(
            storage
                .catalog_signing_keys(scope)
                .await
                .unwrap()
                .is_empty()
        );
        storage
            .replace_catalog_signing_keys(scope, &[("same", &format!("key-{index}"))])
            .await
            .unwrap();
    }
    storage
        .replace_catalog_signing_keys(&scopes[0], &[])
        .await
        .unwrap();
    for (index, scope) in scopes.iter().enumerate() {
        assert_eq!(
            storage.catalog_signing_keys(scope).await.unwrap(),
            if index == 0 {
                vec![]
            } else {
                vec![("same".into(), format!("key-{index}"))]
            }
        );
    }
    assert_eq!(
        storage.get_trusted_key("same").await.unwrap().as_deref(),
        Some("legacy-key")
    );
    let forged = CatalogScope {
        account: a.clone(),
        id: scopes[2].id.clone(),
    };
    assert!(storage.catalog_signing_keys(&forged).await.is_err());
    assert!(
        storage
            .replace_catalog_signing_keys(&forged, &[])
            .await
            .is_err()
    );
    storage.retire_account_scope(&a).await.unwrap();
    let other = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(dir.path().join("scopes.db").to_str().unwrap())
            .await
            .unwrap(),
    ));
    let reopened = other.open_account_scope("a", "issuer", "a").await.unwrap();
    let current = catalog(&other, &reopened, "two").await;
    assert!(storage.catalog_signing_keys(&scopes[1]).await.is_err());
    assert!(
        storage
            .replace_catalog_signing_keys(&scopes[1], &[])
            .await
            .is_err()
    );
    assert_eq!(
        other.catalog_signing_keys(&current).await.unwrap(),
        vec![("same".into(), "key-1".into())]
    );
    assert_eq!(
        other.catalog_signing_keys(&scopes[2]).await.unwrap(),
        vec![("same".into(), "key-2".into())]
    );
}

#[tokio::test]
async fn scoped_key_failure_preserves_the_complete_previous_set() {
    let (storage, _dir) = fixture().await;
    let a = storage
        .open_account_scope("a", "issuer", "subject")
        .await
        .unwrap();
    let c = catalog(&storage, &a, "one").await;
    storage
        .replace_catalog_signing_keys(&c, &[("old", "old-public-key")])
        .await
        .unwrap();
    assert!(
        storage
            .replace_catalog_signing_keys(&c, &[("new", "new-key"), ("new", "different-key")])
            .await
            .is_err()
    );
    sqlx::query("CREATE TRIGGER reject_scoped_key BEFORE INSERT ON mobile_catalog_signing_keys WHEN NEW.key_id = 'reject' BEGIN SELECT RAISE(ABORT, 'injected key failure'); END")
        .execute(storage.pool()).await.unwrap();
    assert!(
        storage
            .replace_catalog_signing_keys(&c, &[("new", "new-key"), ("reject", "bad-key")])
            .await
            .is_err()
    );
    assert_eq!(
        storage.catalog_signing_keys(&c).await.unwrap(),
        vec![("old".into(), "old-public-key".into())]
    );
    sqlx::query("CREATE TRIGGER reject_scoped_revoke BEFORE DELETE ON mobile_catalog_signing_keys BEGIN SELECT RAISE(ABORT, 'injected revoke failure'); END")
        .execute(storage.pool()).await.unwrap();
    assert!(storage.replace_catalog_signing_keys(&c, &[]).await.is_err());
    assert_eq!(
        storage.catalog_signing_keys(&c).await.unwrap(),
        vec![("old".into(), "old-public-key".into())]
    );
}

async fn four_accepted_catalogs(storage: &MobileStorage) -> (AccountScope, [CatalogScope; 4]) {
    let a = storage
        .open_account_scope("a", "issuer", "a")
        .await
        .unwrap();
    let b = storage
        .open_account_scope("b", "issuer", "b")
        .await
        .unwrap();
    let scopes = [
        catalog(storage, &a, "one").await,
        catalog(storage, &a, "two").await,
        catalog(storage, &b, "one").await,
        catalog(storage, &b, "two").await,
    ];
    for (index, scope) in scopes.iter().enumerate() {
        let version = i64::try_from(index).unwrap() + 1;
        storage
            .accept_catalog_manifest(
                scope,
                &[("same", &format!("key-{version}"))],
                version,
                &version.to_string().repeat(64),
            )
            .await
            .unwrap();
    }
    (a, scopes)
}

#[tokio::test]
async fn catalog_acceptance_and_evaluation_are_independent_and_fenced() {
    let (storage, _dir) = fixture().await;
    let (_account, scopes) = four_accepted_catalogs(&storage).await;
    let one = "1".repeat(64);
    let five = "5".repeat(64);
    storage
        .commit_catalog_evaluation(&scopes[0], 1, &one, Some("one"), "old-env")
        .await
        .unwrap();
    storage
        .accept_catalog_manifest(&scopes[0], &[], 5, &five)
        .await
        .unwrap();
    assert!(
        storage
            .commit_catalog_evaluation(&scopes[0], 1, &one, Some("stale"), "stale-env")
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .catalog_metadata(&scopes[0], "manifest_etag")
            .await
            .unwrap()
            .as_deref(),
        Some("one")
    );
    assert!(
        storage
            .accept_catalog_manifest(&scopes[0], &[("old", "revoked")], 4, &"4".repeat(64))
            .await
            .is_err()
    );
    assert!(
        storage
            .accept_catalog_manifest(&scopes[0], &[], 5, &"6".repeat(64))
            .await
            .is_err()
    );
    storage
        .accept_catalog_manifest(&scopes[0], &[], 5, &five)
        .await
        .unwrap();
    storage
        .commit_catalog_evaluation(&scopes[0], 5, &five, None, "new-env")
        .await
        .unwrap();
    assert!(
        !storage
            .catalog_manifest_trust(&scopes[0])
            .await
            .unwrap()
            .pending()
    );
    assert_eq!(
        storage
            .catalog_metadata(&scopes[0], "manifest_etag")
            .await
            .unwrap(),
        None
    );
    for (index, scope) in scopes.iter().enumerate().skip(1) {
        let trust = storage.catalog_manifest_trust(scope).await.unwrap();
        assert_eq!(
            (trust.accepted_version, trust.applied_version),
            (Some(i64::try_from(index).unwrap() + 1), None)
        );
        assert_eq!(
            storage.catalog_signing_keys(scope).await.unwrap(),
            vec![("same".into(), format!("key-{}", index + 1))]
        );
    }
}

#[tokio::test]
async fn catalog_sync_metadata_cannot_bypass_ownership_or_acceptance() {
    let (storage, _dir) = fixture().await;
    let (a, scopes) = four_accepted_catalogs(&storage).await;
    for key in [
        ACCEPTED_VERSION,
        ACCEPTED_DIGEST,
        "last_manifest_version",
        "manifest_etag",
        "sync_env_fingerprint",
        "last_sync_ts",
    ] {
        assert!(
            storage
                .update_catalog_metadata(&scopes[0], &[(key, Some("bypass"))])
                .await
                .is_err()
        );
    }
    let foreign = CatalogScope {
        account: a.clone(),
        id: scopes[2].id.clone(),
    };
    assert!(storage.catalog_manifest_trust(&foreign).await.is_err());
    assert!(
        storage
            .accept_catalog_manifest(&foreign, &[], 9, &"9".repeat(64))
            .await
            .is_err()
    );
    storage.retire_account_scope(&a).await.unwrap();
    assert!(storage.catalog_manifest_trust(&scopes[0]).await.is_err());
    assert!(
        storage
            .accept_catalog_manifest(&scopes[0], &[], 9, &"9".repeat(64))
            .await
            .is_err()
    );
    assert!(
        storage
            .commit_catalog_evaluation(&scopes[0], 1, &"1".repeat(64), None, "retired")
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .catalog_manifest_trust(&scopes[2])
            .await
            .unwrap()
            .floor(),
        Some(3)
    );
}

#[tokio::test]
async fn catalog_acceptance_failure_rolls_back_keys_and_both_fence_fields() {
    let (storage, _dir) = fixture().await;
    let a = storage
        .open_account_scope("a", "issuer", "a")
        .await
        .unwrap();
    let c = catalog(&storage, &a, "one").await;
    let one = "1".repeat(64);
    storage
        .accept_catalog_manifest(&c, &[("old", "old-key")], 1, &one)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_catalog_digest BEFORE INSERT ON mobile_catalog_metadata WHEN NEW.key = 'accepted_manifest_digest' BEGIN SELECT RAISE(ABORT, 'injected digest failure'); END")
        .execute(storage.pool()).await.unwrap();
    assert!(
        storage
            .accept_catalog_manifest(&c, &[("new", "new-key")], 2, &"2".repeat(64))
            .await
            .is_err()
    );
    let trust = storage.catalog_manifest_trust(&c).await.unwrap();
    assert_eq!(trust.accepted_version, Some(1));
    trust.check_candidate(1, &one).unwrap();
    assert_eq!(
        storage.catalog_signing_keys(&c).await.unwrap(),
        vec![("old".into(), "old-key".into())]
    );
}

#[tokio::test]
async fn catalog_pending_freshness_survives_pool_close_and_account_reopen() {
    let (storage, dir) = fixture().await;
    let a = storage
        .open_account_scope("a", "issuer", "a")
        .await
        .unwrap();
    let c = catalog(&storage, &a, "one").await;
    storage
        .accept_catalog_manifest(&c, &[("one", "one-key")], 1, &"1".repeat(64))
        .await
        .unwrap();
    storage
        .commit_catalog_evaluation(&c, 1, &"1".repeat(64), Some("old"), "env")
        .await
        .unwrap();
    storage
        .accept_catalog_manifest(&c, &[("two", "two-key")], 2, &"2".repeat(64))
        .await
        .unwrap();
    storage.retire_account_scope(&a).await.unwrap();
    storage.pool().close().await;
    let other = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(dir.path().join("scopes.db").to_str().unwrap())
            .await
            .unwrap(),
    ));
    let reopened = other.open_account_scope("a", "issuer", "a").await.unwrap();
    let current = catalog(&other, &reopened, "one").await;
    assert!(other.catalog_manifest_trust(&c).await.is_err());
    let trust = other.catalog_manifest_trust(&current).await.unwrap();
    assert_eq!(
        (trust.accepted_version, trust.applied_version),
        (Some(2), Some(1))
    );
    assert!(trust.pending());
    assert!(
        other
            .commit_catalog_evaluation(&current, 1, &"1".repeat(64), Some("stale"), "old")
            .await
            .is_err()
    );
    other
        .commit_catalog_evaluation(&current, 2, &"2".repeat(64), Some("new"), "new")
        .await
        .unwrap();
    assert!(
        !other
            .catalog_manifest_trust(&current)
            .await
            .unwrap()
            .pending()
    );
    assert_eq!(
        other.catalog_signing_keys(&current).await.unwrap(),
        vec![("two".into(), "two-key".into())]
    );
}
