use super::*;
use orch8_storage::sqlite::SqliteStorage;
use std::sync::Arc;

async fn scope(storage: &MobileStorage, account: &str, catalog: &str) -> CatalogScope {
    let owner = storage
        .open_account_scope(account, "issuer", account)
        .await
        .unwrap();
    storage
        .open_catalog_scope(&owner, catalog, "source", "authority")
        .await
        .unwrap()
}
async fn bind(
    storage: &MobileStorage,
    scope: &CatalogScope,
    key: &str,
    id: RuntimeId,
) -> Result<CatalogWorkerPeer, StorageError> {
    storage
        .bind_worker_peer(
            scope,
            WorkerPeerDescriptor {
                peer_key: key,
                api_base: "https://engine.example/api/v1",
                remote_tenant: "remote",
                runtime_id: id,
            },
        )
        .await
}
#[tokio::test]
async fn peer_identity_is_explicit_stable_on_retry_and_database_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("peers.db");
    let inner = Arc::new(SqliteStorage::file(path.to_str().unwrap()).await.unwrap());
    let storage = MobileStorage::new(inner);
    let owner = scope(&storage, "alice", "work").await;
    let id = RuntimeId::new();
    let first = bind(&storage, &owner, "backend", id).await.unwrap();
    assert_eq!(first.runtime_id(), id);
    assert_eq!(first.api_base(), "https://engine.example/api/v1/");
    assert!(first.scope() == &owner);
    let retry = storage
        .bind_worker_peer(
            &owner,
            WorkerPeerDescriptor {
                peer_key: "backend",
                api_base: "https://engine.example/api/v1/",
                remote_tenant: "remote",
                runtime_id: id,
            },
        )
        .await
        .unwrap();
    assert_eq!(first.runtime_id(), retry.runtime_id());
    let reopened = MobileStorage::new(Arc::new(
        SqliteStorage::file(path.to_str().unwrap()).await.unwrap(),
    ));
    reopened.check_worker_peer(&first).await.unwrap();
    assert_eq!(
        bind(&reopened, &owner, "backend", id)
            .await
            .unwrap()
            .runtime_id(),
        id
    );
    assert!(
        bind(&storage, &owner, "backend", RuntimeId::new())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn remote_worker_identity_cannot_alias_another_catalog_account_or_peer_key() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let one = scope(&storage, "alice", "one").await;
    let two = scope(&storage, "alice", "two").await;
    let other = scope(&storage, "bob", "one").await;
    let id = RuntimeId::new();
    let original = bind(&storage, &one, "backend", id).await.unwrap();
    for target in [&one, &two, &other] {
        assert!(bind(&storage, target, "alias", id).await.is_err());
    }
    bind(&storage, &two, "backend", RuntimeId::new())
        .await
        .unwrap();
    bind(&storage, &other, "backend", RuntimeId::new())
        .await
        .unwrap();
    storage.check_worker_peer(&original).await.unwrap();
}
#[tokio::test]
async fn retirement_stops_old_peer_handles_without_reassigning_durable_identity() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let owner = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    let cat = storage
        .open_catalog_scope(&owner, "work", "source", "authority")
        .await
        .unwrap();
    let id = RuntimeId::new();
    let old = bind(&storage, &cat, "backend", id).await.unwrap();
    storage.retire_account_scope(&owner).await.unwrap();
    assert!(storage.check_worker_peer(&old).await.is_err());
    assert!(bind(&storage, &cat, "backend", id).await.is_err());
    let current = scope(&storage, "alice", "work").await;
    assert!(current.epoch() > cat.epoch());
    let fresh = bind(&storage, &current, "backend", id).await.unwrap();
    storage.check_worker_peer(&fresh).await.unwrap();
    assert!(storage.check_worker_peer(&old).await.is_err());
    let foreign = scope(&storage, "bob", "work").await;
    assert!(bind(&storage, &foreign, "backend", id).await.is_err());
}
#[tokio::test]
async fn scoped_and_legacy_worker_identities_cannot_be_reclassified() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let cat = scope(&storage, "alice", "work").await;
    let legacy = crate::node::load_or_create_runtime_id(storage.pool())
        .await
        .unwrap();
    assert!(bind(&storage, &cat, "legacy", legacy).await.is_err());
    sqlx::query("UPDATE mobile_node_identity SET value=?1 WHERE key='runtime_id'")
        .bind(legacy.to_string().to_uppercase())
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(
        bind(&storage, &cat, "legacy-uppercase", legacy)
            .await
            .is_err()
    );
    let id = RuntimeId::new();
    bind(&storage, &cat, "scoped", id).await.unwrap();
    assert!(
        sqlx::query("UPDATE mobile_node_identity SET value=?1 WHERE key='runtime_id'")
            .bind(id.to_string().to_uppercase())
            .execute(storage.pool())
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM mobile_node_identity WHERE key='runtime_id'")
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(
        sqlx::query("INSERT INTO mobile_node_identity VALUES('runtime_id',?1)")
            .bind(id.to_string().to_uppercase())
            .execute(storage.pool())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn corrupt_or_unknown_peer_records_are_not_restamped_and_debug_is_redacted() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let cat = scope(&storage, "alice", "work").await;
    let id = RuntimeId::new();
    let peer = bind(&storage, &cat, "PRIVATE_PEER_KEY", id).await.unwrap();
    assert!(!format!("{peer:?}").contains("PRIVATE"));
    assert!(
        sqlx::query("UPDATE mobile_scoped_worker_peers SET record=record")
            .execute(storage.pool())
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM mobile_scoped_worker_peers")
            .execute(storage.pool())
            .await
            .is_err()
    );
    sqlx::query("DROP TRIGGER mobile_worker_peer_update")
        .execute(storage.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE mobile_scoped_worker_peers SET record=?1")
        .bind(r#"{"version":2,"private":"PRIVATE_CORRUPT_PEER"}"#)
        .execute(storage.pool())
        .await
        .unwrap();
    let error = storage.check_worker_peer(&peer).await.unwrap_err();
    assert!(!format!("{error:?}").contains("PRIVATE_CORRUPT_PEER"));
    assert!(bind(&storage, &cat, "PRIVATE_PEER_KEY", id).await.is_err());
}
#[tokio::test]
async fn descriptors_reject_private_origins_credentials_nil_ids_and_identity_changes() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let cat = scope(&storage, "alice", "work").await;
    for api in [
        "http://engine.example/api/v1",
        "https://127.0.0.1/api/v1",
        "https://localhost/api/v1",
        "https://user:PRIVATE_TOKEN@engine.example/api/v1",
        "https://engine.example/api/v1?PRIVATE_TOKEN",
        "https://engine.example/api/v1#PRIVATE_TOKEN",
    ] {
        let error = storage
            .bind_worker_peer(
                &cat,
                WorkerPeerDescriptor {
                    peer_key: "bad",
                    api_base: api,
                    remote_tenant: "remote",
                    runtime_id: RuntimeId::new(),
                },
            )
            .await
            .unwrap_err();
        assert!(!format!("{error:?}").contains("PRIVATE_TOKEN"));
    }
    assert!(
        bind(
            &storage,
            &cat,
            "nil",
            RuntimeId::from_uuid(uuid::Uuid::nil())
        )
        .await
        .is_err()
    );
    let id = RuntimeId::new();
    bind(&storage, &cat, "backend", id).await.unwrap();
    assert!(
        storage
            .bind_worker_peer(
                &cat,
                WorkerPeerDescriptor {
                    peer_key: "backend",
                    api_base: "https://different.example/api/v1",
                    remote_tenant: "remote",
                    runtime_id: id
                }
            )
            .await
            .is_err()
    );
}
#[tokio::test]
async fn quotas_include_retired_owners_but_do_not_block_identical_retries() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let mut first = None;
    for account in ["alice", "bob"] {
        for c in 0..4 {
            let cat = scope(&storage, account, &format!("catalog-{c}")).await;
            for k in 0..16 {
                let id = RuntimeId::new();
                bind(&storage, &cat, &format!("peer-{k}"), id)
                    .await
                    .unwrap();
                if first.is_none() {
                    first = Some((cat.clone(), id));
                }
            }
            assert!(matches!(
                bind(&storage, &cat, "catalog-overflow", RuntimeId::new()).await,
                Err(StorageError::QuotaExceeded(_))
            ));
        }
        let cat = scope(&storage, account, "account-overflow").await;
        assert!(matches!(
            bind(&storage, &cat, "backend", RuntimeId::new()).await,
            Err(StorageError::QuotaExceeded(_))
        ));
        let owner = storage
            .open_account_scope(account, "issuer", account)
            .await
            .unwrap();
        storage.retire_account_scope(&owner).await.unwrap();
    }
    let (_, id) = first.unwrap();
    let cat = scope(&storage, "alice", "catalog-0").await;
    bind(&storage, &cat, "peer-0", id).await.unwrap();
    let third = scope(&storage, "charlie", "work").await;
    assert!(matches!(
        bind(&storage, &third, "backend", RuntimeId::new()).await,
        Err(StorageError::QuotaExceeded(_))
    ));
}

#[tokio::test]
async fn concurrent_pools_reuse_one_durable_binding() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("concurrent.db");
    let one = MobileStorage::new(Arc::new(
        SqliteStorage::file(path.to_str().unwrap()).await.unwrap(),
    ));
    let cat = scope(&one, "alice", "work").await;
    let two = MobileStorage::new(Arc::new(
        SqliteStorage::file(path.to_str().unwrap()).await.unwrap(),
    ));
    let id = RuntimeId::new();
    let (left, right) = tokio::join!(
        bind(&one, &cat, "backend", id),
        bind(&two, &cat, "backend", id)
    );
    one.check_worker_peer(&left.unwrap()).await.unwrap();
    two.check_worker_peer(&right.unwrap()).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_scoped_worker_peers")
        .fetch_one(one.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}
#[tokio::test]
async fn issued_context_must_match_the_host_selected_tenant_and_worker() {
    use serde_json::json;
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let cat = scope(&storage, "alice", "work").await;
    let runtime = RuntimeId::new();
    let peer = bind(&storage, &cat, "backend", runtime).await.unwrap();
    let mut context:DelegationClaimContext=serde_json::from_value(json!({"version":1,"scope":{"tenant_id":"remote","task_id":uuid::Uuid::new_v4(),"worker_id":runtime,"claim_epoch":u64::MAX.to_string()},"request_sha256":"a".repeat(64),"expires_at":chrono::Utc::now()+chrono::Duration::minutes(1),"definition":{"version":1,"sequence_sha256":"b".repeat(64),"bundle_sha256":"c".repeat(64),"payload_sha256":"d".repeat(64),"payload_bytes":4,"definition_count":1,"selector_count":0},"input":{"version":1,"payload_sha256":"e".repeat(64),"payload_bytes":4,"expanded_bytes":64},"usage":{"definition_expanded_bytes":128,"input_wire_version":1,"input_payload_bytes":4,"input_expanded_bytes":64}})).unwrap();
    storage.check_worker_peer(&peer).await.unwrap();
    peer.validate_claim_identity(&context).unwrap();
    assert_eq!(peer.remote_tenant().as_str(), "remote");
    context.scope.tenant_id = TenantId::new("foreign").unwrap();
    assert!(peer.validate_claim_identity(&context).is_err());
    context.scope.tenant_id = TenantId::new("remote").unwrap();
    context.scope.worker_id = RuntimeId::new().to_string();
    assert!(peer.validate_claim_identity(&context).is_err());
}

#[tokio::test]
async fn upgrade_adds_peer_storage_without_restamping_legacy_identity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("upgrade.db");
    let original = Arc::new(SqliteStorage::file(path.to_str().unwrap()).await.unwrap());
    let legacy = crate::node::load_or_create_runtime_id(original.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE mobile_node_identity SET value=?1 WHERE key='runtime_id'")
        .bind(legacy.to_string().to_uppercase())
        .execute(original.pool())
        .await
        .unwrap();
    // Model an earlier schema-74 development database lacking the peer extension.
    // This is a synthetic layout fixture, not a released SDK migration probe.
    sqlx::raw_sql("DROP TRIGGER mobile_worker_peer_legacy_identity_insert; DROP TRIGGER mobile_worker_peer_legacy_identity_update; DROP TABLE mobile_scoped_worker_peers;").execute(original.pool()).await.unwrap();
    original.close().await;
    let reopened = Arc::new(SqliteStorage::file(path.to_str().unwrap()).await.unwrap());
    let stored: String =
        sqlx::query_scalar("SELECT value FROM mobile_node_identity WHERE key='runtime_id'")
            .fetch_one(reopened.pool())
            .await
            .unwrap();
    assert_eq!(stored, legacy.to_string().to_uppercase());
    let storage = MobileStorage::new(reopened);
    let cat = scope(&storage, "alice", "work").await;
    assert!(bind(&storage, &cat, "legacy", legacy).await.is_err());
    let peer = bind(&storage, &cat, "scoped", RuntimeId::new())
        .await
        .unwrap();
    storage.check_worker_peer(&peer).await.unwrap();
}
