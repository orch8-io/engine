use super::super::history::CatalogNativeState;
use super::super::tests::{fixture, limits as hot_limits, native_fixture, request, scope};
use super::*;
use orch8_storage::{InstanceStore, ResourceStore};

#[tokio::test]
async fn archive_releases_hot_quota_keeps_dedup_and_outcome_after_native_gc() {
    let (storage, sqlite) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let input = serde_json::json!({"secret":"ARCHIVE_PRIVATE_INPUT".repeat(200)}).to_string();
    let run = storage
        .prepare_catalog_run(&scope, request(Some("acknowledged"), &input), hot_limits())
        .await
        .unwrap();
    native_fixture(&sqlite, &run).await;
    for key in ["two", "three", "four"] {
        storage
            .prepare_catalog_run(&scope, request(Some(key), "{}"), hot_limits())
            .await
            .unwrap();
    }
    assert!(
        storage
            .prepare_catalog_run(&scope, request(Some("five"), "{}"), hot_limits())
            .await
            .is_err()
    );
    assert!(
        storage
            .archive_catalog_run(&scope, run.id, "ack", limits())
            .await
            .unwrap()
            .is_none(),
        "active execution cannot archive"
    );
    sqlite
        .update_instance_state(run.id, InstanceState::Completed, None)
        .await
        .unwrap();
    let receipt = storage
        .archive_catalog_run(&scope, run.id, "PRIVATE_ACK_REFERENCE", limits())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.outcome, Some(InstanceState::Completed));
    assert_eq!(
        storage
            .archive_catalog_run(&scope, run.id, "PRIVATE_ACK_REFERENCE", limits())
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert!(
        storage
            .archive_catalog_run(&scope, run.id, "different", limits())
            .await
            .is_err()
    );
    storage
        .prepare_catalog_run(&scope, request(Some("five"), "{}"), hot_limits())
        .await
        .unwrap();
    let retry = storage
        .prepare_catalog_run(&scope, request(Some("acknowledged"), &input), hot_limits())
        .await
        .unwrap();
    assert_eq!(retry.id, run.id);
    assert_eq!(retry.state, CatalogRunState::Archived);
    assert!(retry.input.is_null());
    assert!(
        storage
            .prepare_catalog_run(&scope, request(Some("acknowledged"), "{}"), hot_limits())
            .await
            .is_err()
    );
    assert!(
        storage
            .prepare_materialization(&scope, run.id)
            .await
            .is_err()
    );
    assert!(
        !storage
            .forget_cancelled_catalog_run(&scope, run.id)
            .await
            .unwrap()
    );
    assert_eq!(sqlite.purge_terminal_instances(&[run.id]).await.unwrap(), 1);
    assert!(
        storage
            .prepare_materialization(&scope, run.id)
            .await
            .is_err()
    );
    let page = storage
        .catalog_run_history(&scope, None, 128)
        .await
        .unwrap();
    let item = page.items.iter().find(|item| item.id == run.id).unwrap();
    assert_eq!(item.request_state, CatalogRunState::Archived);
    assert_eq!(
        item.native_state,
        CatalogNativeState::Recorded(InstanceState::Completed)
    );
    assert_compacted_payload(&storage, run.id).await;
}

#[tokio::test]
async fn purged_materialization_archives_unknown_without_inventing_outcome() {
    let (storage, sqlite) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&scope, request(Some("purged"), "{}"), hot_limits())
        .await
        .unwrap();
    native_fixture(&sqlite, &run).await;
    sqlite
        .update_instance_state(run.id, InstanceState::Failed, None)
        .await
        .unwrap();
    sqlite.purge_terminal_instances(&[run.id]).await.unwrap();
    let receipt = storage
        .archive_catalog_run(&scope, run.id, "ack", limits())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.outcome, None);
    let page = storage.catalog_run_history(&scope, None, 1).await.unwrap();
    assert_eq!(page.items[0].native_state, CatalogNativeState::Unavailable);
    assert_eq!(page.items[0].request_state, CatalogRunState::Archived);
}

#[tokio::test]
async fn four_scope_archive_ownership_and_retirement_fence() {
    let (storage, sqlite) = fixture().await;
    let mut runs = Vec::new();
    for account in ["alice", "bob"] {
        for catalog in ["one", "two"] {
            let scope = scope(&storage, account, catalog).await;
            let run = storage
                .prepare_catalog_run(&scope, request(Some("shared"), "{}"), hot_limits())
                .await
                .unwrap();
            native_fixture(&sqlite, &run).await;
            sqlite
                .update_instance_state(run.id, InstanceState::Completed, None)
                .await
                .unwrap();
            runs.push((scope, run));
        }
    }
    for (scope, run) in &runs {
        for (other, _) in &runs {
            if other != scope {
                assert!(
                    storage
                        .archive_catalog_run(other, run.id, "ack", limits())
                        .await
                        .unwrap()
                        .is_none()
                );
            }
        }
        assert!(
            storage
                .archive_catalog_run(scope, run.id, "ack", limits())
                .await
                .unwrap()
                .is_some()
        );
    }
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    storage.retire_account_scope(&account).await.unwrap();
    assert!(
        storage
            .archive_catalog_run(&runs[0].0, runs[0].1.id, "ack", limits())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn archive_refuses_unmaterialized_and_invalid_ack_without_changing_input() {
    let (storage, _) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(
            &scope,
            request(Some("queued"), r#"{"keep":true}"#),
            hot_limits(),
        )
        .await
        .unwrap();
    for ack in ["", "\n", " "] {
        assert!(
            storage
                .archive_catalog_run(&scope, run.id, ack, limits())
                .await
                .is_err()
        );
    }
    assert!(
        storage
            .archive_catalog_run(&scope, run.id, "ack", limits())
            .await
            .unwrap()
            .is_none()
    );
    assert!(storage.cancel_catalog_run(&scope, run.id).await.unwrap());
    assert!(
        storage
            .archive_catalog_run(&scope, run.id, "ack", limits())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        storage
            .catalog_run(&scope, run.id)
            .await
            .unwrap()
            .unwrap()
            .input,
        run.input
    );
}

#[tokio::test]
async fn cold_quota_failure_rolls_back_and_does_not_destroy_input_or_receipts() {
    let (storage, sqlite) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(
            &scope,
            request(Some("quota"), r#"{"keep":true}"#),
            hot_limits(),
        )
        .await
        .unwrap();
    native_fixture(&sqlite, &run).await;
    sqlite
        .update_instance_state(run.id, InstanceState::Cancelled, None)
        .await
        .unwrap();
    for constrained in [
        CatalogRunLimits {
            catalog_records: 0,
            ..limits()
        },
        CatalogRunLimits {
            account_records: 0,
            ..limits()
        },
        CatalogRunLimits {
            global_records: 0,
            ..limits()
        },
        CatalogRunLimits {
            catalog_input_bytes: 1,
            ..limits()
        },
        CatalogRunLimits {
            account_input_bytes: 1,
            ..limits()
        },
        CatalogRunLimits {
            global_input_bytes: 1,
            ..limits()
        },
    ] {
        assert!(matches!(
            storage
                .archive_catalog_run(&scope, run.id, "ack", constrained)
                .await,
            Err(StorageError::QuotaExceeded(_))
        ));
        let intact = storage.catalog_run(&scope, run.id).await.unwrap().unwrap();
        assert_eq!(intact.input, run.input);
        assert_eq!(intact.state, CatalogRunState::Prepared);
    }
    assert_eq!(
        storage
            .archive_catalog_run(&scope, run.id, "ack", limits())
            .await
            .unwrap()
            .unwrap()
            .outcome,
        Some(InstanceState::Cancelled)
    );
    assert!(
        storage
            .archive_catalog_run(
                &scope,
                run.id,
                "ack",
                CatalogRunLimits {
                    global_records: 0,
                    ..limits()
                }
            )
            .await
            .unwrap()
            .is_some(),
        "idempotent retry cannot consume more quota"
    );
}

#[tokio::test]
async fn corrupt_native_or_receipt_identity_never_releases_hot_quota() {
    let (storage, sqlite) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&scope, request(Some("corrupt"), "{}"), hot_limits())
        .await
        .unwrap();
    native_fixture(&sqlite, &run).await;
    sqlite
        .update_instance_state(run.id, InstanceState::Completed, None)
        .await
        .unwrap();
    sqlx::query("UPDATE task_instances SET namespace='foreign' WHERE id=?1")
        .bind(run.id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(
        storage
            .archive_catalog_run(&scope, run.id, "ack", limits())
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .catalog_run(&scope, run.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        CatalogRunState::Prepared
    );
    sqlx::query("UPDATE task_instances SET namespace=?2 WHERE id=?1")
        .bind(run.id.to_string())
        .bind(format!("catalog-{}", scope.catalog_id()))
        .execute(storage.pool())
        .await
        .unwrap();
    storage
        .archive_catalog_run(&scope, run.id, "ack", limits())
        .await
        .unwrap()
        .unwrap();
    // Both FK directions keep compact evidence and prevent an orphan marker.
    assert!(
        sqlx::query("DELETE FROM mobile_catalog_run_archives WHERE request_id=?1")
            .bind(run.id.to_string())
            .execute(storage.pool())
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM mobile_catalog_run_materializations WHERE request_id=?1")
            .bind(run.id.to_string())
            .execute(storage.pool())
            .await
            .is_err()
    );
    sqlx::query("UPDATE mobile_catalog_run_archives SET receipt_digest=?2 WHERE request_id=?1")
        .bind(run.id.to_string())
        .bind("0".repeat(64))
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(
        storage
            .archive_catalog_run(&scope, run.id, "ack", limits())
            .await
            .is_err()
    );
    assert!(
        storage
            .prepare_catalog_run(&scope, request(Some("corrupt"), "{}"), hot_limits())
            .await
            .is_err()
    );
    assert!(
        storage
            .catalog_run_history(&scope, None, 128)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn concurrent_acknowledgements_share_one_receipt_and_cold_quota_is_atomic() {
    let (storage, sqlite) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let mut ids = Vec::new();
    for key in ["one", "two"] {
        let run = storage
            .prepare_catalog_run(&scope, request(Some(key), "{}"), hot_limits())
            .await
            .unwrap();
        native_fixture(&sqlite, &run).await;
        sqlite
            .update_instance_state(run.id, InstanceState::Completed, None)
            .await
            .unwrap();
        ids.push(run.id);
    }
    let first = storage.archive_catalog_run(&scope, ids[0], "ack", limits());
    let retry = storage.archive_catalog_run(&scope, ids[0], "ack", limits());
    let (first, retry) = tokio::join!(first, retry);
    assert_eq!(first.unwrap(), retry.unwrap());
    // A pre-existing receipt consumes the only cold slot, including when the
    // second request is admitted concurrently through independent calls.
    let cap = CatalogRunLimits {
        catalog_records: 1,
        ..limits()
    };
    let (one, two) = tokio::join!(
        storage.archive_catalog_run(&scope, ids[1], "a", cap),
        storage.archive_catalog_run(&scope, ids[1], "b", cap)
    );
    assert!(matches!(one, Err(StorageError::QuotaExceeded(_))));
    assert!(matches!(two, Err(StorageError::QuotaExceeded(_))));
    assert_eq!(
        storage
            .catalog_run(&scope, ids[1])
            .await
            .unwrap()
            .unwrap()
            .state,
        CatalogRunState::Prepared
    );
}

#[tokio::test]
async fn populated_version_55_upgrade_preserves_input_and_archive_survives_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("archive-upgrade.sqlite");
    let (storage, sqlite) = open_file(&file).await;
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(
            &scope,
            request(Some("restart"), r#"{"durable":true}"#),
            hot_limits(),
        )
        .await
        .unwrap();
    native_fixture(&sqlite, &run).await;
    sqlite
        .update_instance_state(run.id, InstanceState::Completed, None)
        .await
        .unwrap();
    restore_version_55_request_table(&storage).await;
    storage.pool().close().await;
    drop(storage);
    drop(sqlite);
    let (storage, sqlite) = open_file(&file).await;
    assert_eq!(
        storage
            .catalog_run(&scope, run.id)
            .await
            .unwrap()
            .unwrap()
            .input,
        run.input
    );
    let fks = sqlx::query("PRAGMA foreign_key_list(mobile_catalog_run_requests)")
        .fetch_all(storage.pool())
        .await
        .unwrap();
    assert!(
        fks.iter()
            .any(|fk| fk.get::<String, _>("from") == "archive_receipt_id"
                && fk.get::<String, _>("table") == "mobile_catalog_run_archives"),
        "upgrade must preserve the marker FK"
    );
    assert!(
        sqlx::query(
            "UPDATE mobile_catalog_run_requests SET archive_receipt_id='missing' WHERE id=?1"
        )
        .bind(run.id.to_string())
        .execute(storage.pool())
        .await
        .is_err()
    );
    let receipt = storage
        .archive_catalog_run(&scope, run.id, "ack", limits())
        .await
        .unwrap()
        .unwrap();
    sqlite.purge_terminal_instances(&[run.id]).await.unwrap();
    storage.pool().close().await;
    drop(storage);
    drop(sqlite);
    let (storage, _sqlite) = open_file(&file).await;
    assert_eq!(
        storage
            .archive_catalog_run(&scope, run.id, "ack", limits())
            .await
            .unwrap(),
        Some(receipt)
    );
    assert_eq!(
        storage
            .catalog_run_history(&scope, None, 1)
            .await
            .unwrap()
            .items[0]
            .native_state,
        CatalogNativeState::Recorded(InstanceState::Completed)
    );
    assert_eq!(
        storage
            .prepare_catalog_run(
                &scope,
                request(Some("restart"), r#"{"durable":true}"#),
                hot_limits()
            )
            .await
            .unwrap()
            .id,
        run.id
    );
    assert!(
        storage
            .prepare_materialization(&scope, run.id)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn failed_compaction_rolls_back_the_inserted_receipt() {
    let (storage, sqlite) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(
            &scope,
            request(Some("rollback"), r#"{"keep":"payload"}"#),
            hot_limits(),
        )
        .await
        .unwrap();
    native_fixture(&sqlite, &run).await;
    sqlite
        .update_instance_state(run.id, InstanceState::Completed, None)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_archive BEFORE UPDATE OF archive_receipt_id ON mobile_catalog_run_requests BEGIN SELECT RAISE(ABORT,'injected compaction failure'); END").execute(storage.pool()).await.unwrap();
    assert!(
        storage
            .archive_catalog_run(&scope, run.id, "ack", limits())
            .await
            .is_err()
    );
    let retained = storage.catalog_run(&scope, run.id).await.unwrap().unwrap();
    assert_eq!(retained.input, run.input);
    assert_eq!(retained.state, CatalogRunState::Prepared);
    let receipts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_catalog_run_archives")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(receipts, 0);
    sqlx::query("DROP TRIGGER reject_archive")
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(
        storage
            .archive_catalog_run(&scope, run.id, "ack", limits())
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn subtenant_collision_is_not_the_original_catalog_execution() {
    let (storage, sqlite) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&scope, request(Some("subtenant"), "{}"), hot_limits())
        .await
        .unwrap();
    native_fixture(&sqlite, &run).await;
    sqlite
        .update_instance_state(run.id, InstanceState::Completed, None)
        .await
        .unwrap();
    sqlx::query("UPDATE task_instances SET sub_tenant='another-owner' WHERE id=?1")
        .bind(run.id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(
        storage
            .archive_catalog_run(&scope, run.id, "ack", limits())
            .await
            .is_err(),
        "a native row with a different subtenant must not supply an outcome receipt"
    );
    let access = orch8_storage::MobileInstanceAccess::Catalog {
        account_id: scope.account_id().into(),
        catalog_id: scope.catalog_id().into(),
        session_epoch: scope.epoch(),
    };
    assert!(
        sqlite
            .get_mobile_instance(&access, run.id)
            .await
            .unwrap()
            .is_none()
    );
}

async fn assert_compacted_payload(storage: &MobileStorage, id: InstanceId) {
    let (input, bytes, pin): (String, i64, Option<String>) = sqlx::query_as(
        "SELECT input,input_bytes,sequence_id FROM mobile_catalog_run_requests WHERE id=?1",
    )
    .bind(id.to_string())
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!((input.as_str(), bytes, pin), ("null", 4, None));
    let body: String = sqlx::query_scalar(
        "SELECT receipt_json FROM mobile_catalog_run_archives WHERE request_id=?1",
    )
    .bind(id.to_string())
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert!(!body.contains("ARCHIVE_PRIVATE_INPUT"));
    assert!(!body.contains("PRIVATE_ACK_REFERENCE"));
    assert!(body.len() < MAX_RECEIPT_BYTES);
}

async fn open_file(
    path: &std::path::Path,
) -> (
    MobileStorage,
    std::sync::Arc<orch8_storage::sqlite::SqliteStorage>,
) {
    let sqlite = std::sync::Arc::new(
        orch8_storage::sqlite::SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    );
    (MobileStorage::new(sqlite.clone()), sqlite)
}

async fn restore_version_55_request_table(storage: &MobileStorage) {
    // Model the actual v55 request table, with populated rows/materializations.
    sqlx::query("DROP INDEX idx_mobile_catalog_run_hot_usage")
        .execute(storage.pool())
        .await
        .unwrap();
    sqlx::query("ALTER TABLE mobile_catalog_run_requests DROP COLUMN archive_receipt_id")
        .execute(storage.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM schema_versions WHERE version>55")
        .execute(storage.pool())
        .await
        .unwrap();
}
