use super::*;
use orch8_storage::{InstanceStore, SequenceStore};
use orch8_types::ids::InstanceId;
use orch8_types::instance::TaskInstance;
use std::collections::HashSet;
use std::sync::Arc;

fn digest(version: i64) -> String {
    version.to_string().repeat(64)
}

fn definition(name: &str, version: i32) -> String {
    serde_json::to_string(&serde_json::json!({
        "id":"11111111-1111-4111-8111-111111111111", "tenant_id":"publisher-tenant",
        "namespace":"publisher-namespace", "name":name,"version":version,"blocks":[],
        "sub_tenant":"foreign-customer", "embed":{"visible_outputs":["private-step"],"gallery":true},
        "created_at":"2026-01-01T00:00:00Z",
    })).unwrap()
}

async fn fixture() -> (Arc<MobileStorage>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let sqlite = SqliteStorage::file_mobile(dir.path().join("workflows.db").to_str().unwrap())
        .await
        .unwrap();
    (Arc::new(MobileStorage::new(Arc::new(sqlite))), dir)
}

async fn scope(storage: &MobileStorage, account: &str, catalog: &str) -> CatalogScope {
    let account = storage
        .open_account_scope(account, "issuer", account)
        .await
        .unwrap();
    let scope = storage
        .open_catalog_scope(&account, catalog, "stable-source", "root-authority")
        .await
        .unwrap();
    storage
        .accept_catalog_manifest(&scope, &[], 1, &digest(1))
        .await
        .unwrap();
    scope
}

async fn install(storage: &MobileStorage, scope: &CatalogScope, version: i32) -> SequenceId {
    storage
        .install_catalog_workflow(scope, &definition("flow", version), 1, &digest(1))
        .await
        .unwrap()
}

async fn engine_definition(storage: &MobileStorage, id: SequenceId) -> Option<SequenceDefinition> {
    storage.inner.get_sequence(id).await.unwrap()
}

async fn instance(storage: &MobileStorage, id: SequenceId, state: &str) -> InstanceId {
    let seq = engine_definition(storage, id).await.unwrap();
    let id = InstanceId::new();
    let instance: TaskInstance = serde_json::from_value(serde_json::json!({
        "id":id,"sequence_id":seq.id,"tenant_id":seq.tenant_id,"namespace":seq.namespace,
        "state":state,"priority":"Normal","timezone":"UTC","metadata":{},
        "context":orch8_types::context::ExecutionContext::default(),
        "created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now(),
    }))
    .unwrap();
    storage.inner.create_instance(&instance).await.unwrap();
    id
}

async fn advance(storage: &MobileStorage, scope: &CatalogScope, version: i64) {
    storage
        .accept_catalog_manifest(scope, &[], version, &digest(version))
        .await
        .unwrap();
}

#[tokio::test]
async fn four_catalogs_share_names_and_supplied_ids_without_aliasing_engine_rows() {
    let (storage, _dir) = fixture().await;
    let body = definition("flow", 1);
    let mut legacy: SequenceDefinition = serde_json::from_str(&body).unwrap();
    legacy.tenant_id = crate::mobile_tenant_id();
    legacy.namespace = Namespace::new("default");
    storage.inner.create_sequence(&legacy).await.unwrap();
    let scopes = [
        scope(&storage, "a", "one").await,
        scope(&storage, "a", "two").await,
        scope(&storage, "b", "one").await,
        scope(&storage, "b", "two").await,
    ];
    let mut ids = HashSet::from([legacy.id]);
    for scope in &scopes {
        let id = install(&storage, scope, 1).await;
        assert!(ids.insert(id));
        let stored = storage
            .catalog_workflow(scope, "flow", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.id, id);
        assert_eq!(
            (stored.tenant_id.clone(), stored.namespace.clone()),
            owner(scope).unwrap()
        );
        assert!(stored.sub_tenant.is_none() && stored.embed.is_none());
        assert_eq!(
            storage
                .inner
                .get_sequence_by_name(&stored.tenant_id, &stored.namespace, "flow", None)
                .await
                .unwrap()
                .unwrap()
                .id,
            id
        );
        assert_eq!(
            storage
                .catalog_workflow_by_id(scope, id)
                .await
                .unwrap()
                .unwrap()
                .id,
            id
        );
        assert!(
            storage
                .catalog_workflow_by_id(scope, legacy.id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            storage
                .install_catalog_workflow(scope, &body, 1, &digest(1))
                .await
                .unwrap(),
            id
        );
    }
    for scope in &scopes {
        let own = storage
            .catalog_workflow(scope, "flow", None)
            .await
            .unwrap()
            .unwrap()
            .id;
        for id in &ids {
            assert_eq!(
                storage
                    .catalog_workflow_by_id(scope, *id)
                    .await
                    .unwrap()
                    .is_some(),
                *id == own
            );
        }
    }
    assert_eq!(
        engine_definition(&storage, legacy.id)
            .await
            .unwrap()
            .tenant_id,
        crate::mobile_tenant_id()
    );
}

#[tokio::test]
async fn eviction_is_local_and_preserves_immutable_published_version_identity() {
    let (storage, _dir) = fixture().await;
    let a = scope(&storage, "a", "one").await;
    let b = scope(&storage, "b", "one").await;
    let old = install(&storage, &a, 1).await;
    let other = install(&storage, &b, 1).await;
    assert!(
        storage
            .evict_catalog_workflow(&a, "flow", 1, 1, &digest(1))
            .await
            .unwrap()
    );
    assert!(
        storage
            .catalog_workflow(&a, "flow", None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(engine_definition(&storage, old).await.is_none());
    assert_eq!(
        storage
            .catalog_workflow(&b, "flow", None)
            .await
            .unwrap()
            .unwrap()
            .id,
        other
    );
    let mut changed: serde_json::Value = serde_json::from_str(&definition("flow", 1)).unwrap();
    changed["input_schema"] = serde_json::json!({"type":"string"});
    assert!(
        storage
            .install_catalog_workflow(&a, &changed.to_string(), 1, &digest(1))
            .await
            .is_err()
    );
    let restored = install(&storage, &a, 1).await;
    assert_ne!(restored, old);
    assert_ne!(restored, other);
    assert_eq!(
        storage
            .install_catalog_workflow(&a, &definition("flow", 1), 1, &digest(1))
            .await
            .unwrap(),
        restored
    );
    assert!(
        storage
            .evict_catalog_workflow(&a, "flow", 1, 1, &digest(1))
            .await
            .unwrap()
    );
    assert!(
        !storage
            .evict_catalog_workflow(&a, "flow", 1, 1, &digest(1))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn source_id_collision_cannot_create_an_orphan_or_replace_another_name() {
    let (storage, _dir) = fixture().await;
    let c = scope(&storage, "a", "one").await;
    let id = install(&storage, &c, 1).await;
    assert!(
        storage
            .install_catalog_workflow(&c, &definition("different-name", 1), 1, &digest(1))
            .await
            .is_err()
    );
    let mut changed: serde_json::Value = serde_json::from_str(&definition("flow", 1)).unwrap();
    changed["id"] = "22222222-2222-4222-8222-222222222222".into();
    assert!(
        storage
            .install_catalog_workflow(&c, &changed.to_string(), 1, &digest(1))
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .catalog_workflow(&c, "flow", None)
            .await
            .unwrap()
            .unwrap()
            .id,
        id
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sequences")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn failed_owner_mapping_insert_rolls_back_the_engine_definition() {
    let (storage, _dir) = fixture().await;
    let c = scope(&storage, "a", "one").await;
    sqlx::query("CREATE TRIGGER reject_workflow_mapping BEFORE INSERT ON mobile_catalog_workflows BEGIN SELECT RAISE(ABORT, 'injected mapping failure'); END")
        .execute(storage.pool()).await.unwrap();
    assert!(
        storage
            .install_catalog_workflow(&c, &definition("flow", 1), 1, &digest(1))
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sequences")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_catalog_workflows")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    sqlx::query("DROP TRIGGER reject_workflow_mapping")
        .execute(storage.pool())
        .await
        .unwrap();
    install(&storage, &c, 1).await;
}

#[tokio::test]
async fn withdrawal_hides_new_starts_but_pins_all_nonterminal_instances() {
    let (storage, _dir) = fixture().await;
    let a = scope(&storage, "a", "one").await;
    let b = scope(&storage, "b", "one").await;
    let id = install(&storage, &a, 1).await;
    let other = install(&storage, &b, 1).await;
    let mut instances = Vec::new();
    for state in ["scheduled", "running", "waiting", "paused"] {
        instances.push(instance(&storage, id, state).await);
    }
    advance(&storage, &a, 2).await;
    let removal = storage
        .remove_catalog_workflow(&a, "flow", 2, &digest(2))
        .await
        .unwrap();
    assert_eq!(
        removal,
        WorkflowRemoval {
            deleted: 0,
            retained: 1
        }
    );
    assert!(
        storage
            .catalog_workflow(&a, "flow", None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .catalog_workflow_by_id(&a, id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(engine_definition(&storage, id).await.unwrap().deprecated);
    for instance in &instances {
        assert!(
            storage
                .inner
                .get_instance(*instance)
                .await
                .unwrap()
                .is_some()
        );
    }
    assert!(
        storage
            .evict_catalog_workflow(&a, "flow", 1, 2, &digest(2))
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .catalog_workflow(&b, "flow", None)
            .await
            .unwrap()
            .unwrap()
            .id,
        other
    );
    sqlx::query("UPDATE task_instances SET state = 'completed' WHERE sequence_id = ?1")
        .bind(id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    assert_eq!(
        storage
            .remove_catalog_workflow(&a, "flow", 2, &digest(2))
            .await
            .unwrap(),
        WorkflowRemoval {
            deleted: 1,
            retained: 0
        }
    );
    assert!(engine_definition(&storage, id).await.is_none());
    for instance in instances {
        assert!(
            storage
                .inner
                .get_instance(instance)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(
        storage
            .install_catalog_workflow(&a, &definition("flow", 1), 2, &digest(2))
            .await
            .is_err(),
        "the same removing manifest cannot resurrect a purged version"
    );
    advance(&storage, &a, 3).await;
    let restored = storage
        .install_catalog_workflow(&a, &definition("flow", 1), 3, &digest(3))
        .await
        .unwrap();
    assert_ne!(restored, id);
}

#[tokio::test]
async fn newer_versions_and_explicit_readd_never_replace_live_definitions() {
    let (storage, _dir) = fixture().await;
    let c = scope(&storage, "a", "one").await;
    let one = install(&storage, &c, 1).await;
    let live = instance(&storage, one, "waiting").await;
    let two = install(&storage, &c, 2).await;
    assert_ne!(one, two);
    assert_eq!(
        storage
            .catalog_workflow(&c, "flow", None)
            .await
            .unwrap()
            .unwrap()
            .id,
        two
    );
    assert_eq!(
        storage
            .catalog_workflow(&c, "flow", Some(1))
            .await
            .unwrap()
            .unwrap()
            .id,
        one
    );
    assert_eq!(
        storage
            .inner
            .get_instance(live)
            .await
            .unwrap()
            .unwrap()
            .sequence_id,
        one
    );
    advance(&storage, &c, 2).await;
    assert_eq!(
        storage
            .remove_catalog_workflow(&c, "flow", 2, &digest(2))
            .await
            .unwrap(),
        WorkflowRemoval {
            deleted: 1,
            retained: 1
        }
    );
    advance(&storage, &c, 3).await;
    assert_eq!(
        storage
            .install_catalog_workflow(&c, &definition("flow", 1), 3, &digest(3))
            .await
            .unwrap(),
        one
    );
    assert!(!engine_definition(&storage, one).await.unwrap().deprecated);
    assert_eq!(
        storage
            .catalog_workflow(&c, "flow", None)
            .await
            .unwrap()
            .unwrap()
            .id,
        one
    );
    assert!(storage.inner.get_instance(live).await.unwrap().is_some());
}

#[tokio::test]
async fn failed_withdrawal_rolls_back_prior_version_purge_and_deprecation() {
    let (storage, _dir) = fixture().await;
    let c = scope(&storage, "a", "one").await;
    let one = install(&storage, &c, 1).await;
    let terminal = instance(&storage, one, "completed").await;
    let two = install(&storage, &c, 2).await;
    instance(&storage, two, "waiting").await;
    advance(&storage, &c, 2).await;
    sqlx::query("CREATE TRIGGER reject_withdrawal BEFORE UPDATE OF deprecated ON sequences WHEN OLD.version = 2 BEGIN SELECT RAISE(ABORT, 'injected withdrawal failure'); END")
        .execute(storage.pool()).await.unwrap();
    assert!(
        storage
            .remove_catalog_workflow(&c, "flow", 2, &digest(2))
            .await
            .is_err()
    );
    assert!(
        storage
            .inner
            .get_instance(terminal)
            .await
            .unwrap()
            .is_some()
    );
    for id in [one, two] {
        assert!(!engine_definition(&storage, id).await.unwrap().deprecated);
    }
    assert_eq!(
        storage
            .catalog_workflow(&c, "flow", Some(1))
            .await
            .unwrap()
            .unwrap()
            .id,
        one
    );
    assert_eq!(
        storage
            .catalog_workflow(&c, "flow", None)
            .await
            .unwrap()
            .unwrap()
            .id,
        two
    );
    sqlx::query("DROP TRIGGER reject_withdrawal")
        .execute(storage.pool())
        .await
        .unwrap();
    assert_eq!(
        storage
            .remove_catalog_workflow(&c, "flow", 2, &digest(2))
            .await
            .unwrap(),
        WorkflowRemoval {
            deleted: 1,
            retained: 1
        }
    );
}

#[tokio::test]
async fn stale_sessions_and_old_manifest_callbacks_cannot_mutate_reopened_workflows() {
    let (storage, dir) = fixture().await;
    let old = scope(&storage, "a", "one").await;
    let id = install(&storage, &old, 1).await;
    storage.retire_account_scope(&old.account).await.unwrap();
    storage.pool().close().await;
    let other = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(dir.path().join("workflows.db").to_str().unwrap())
            .await
            .unwrap(),
    ));
    let account = other.open_account_scope("a", "issuer", "a").await.unwrap();
    let c = other
        .open_catalog_scope(&account, "one", "stable-source", "root-authority")
        .await
        .unwrap();
    assert!(other.catalog_workflow(&old, "flow", None).await.is_err());
    assert!(
        other
            .install_catalog_workflow(&old, &definition("flow", 1), 1, &digest(1))
            .await
            .is_err()
    );
    assert!(
        other
            .remove_catalog_workflow(&old, "flow", 1, &digest(1))
            .await
            .is_err()
    );
    assert!(
        other
            .evict_catalog_workflow(&old, "flow", 1, 1, &digest(1))
            .await
            .is_err()
    );
    assert_eq!(
        other
            .catalog_workflow(&c, "flow", None)
            .await
            .unwrap()
            .unwrap()
            .id,
        id
    );
    assert_eq!(
        other
            .install_catalog_workflow(&c, &definition("flow", 1), 1, &digest(1))
            .await
            .unwrap(),
        id
    );
    advance(&other, &c, 2).await;
    assert!(
        other
            .install_catalog_workflow(&c, &definition("flow", 2), 1, &digest(1))
            .await
            .is_err()
    );
    assert!(
        other
            .remove_catalog_workflow(&c, "flow", 1, &digest(1))
            .await
            .is_err()
    );
    assert!(
        other
            .evict_catalog_workflow(&c, "flow", 1, 1, &digest(1))
            .await
            .is_err()
    );
    assert_eq!(
        other
            .catalog_workflow(&c, "flow", None)
            .await
            .unwrap()
            .unwrap()
            .id,
        id
    );
}

#[tokio::test]
async fn independent_pool_installs_converge_on_one_owner_qualified_id() {
    let (storage, dir) = fixture().await;
    let c = scope(&storage, "a", "one").await;
    let other = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(dir.path().join("workflows.db").to_str().unwrap())
            .await
            .unwrap(),
    ));
    let body = definition("flow", 1);
    let hash = digest(1);
    let (a, b) = tokio::join!(
        storage.install_catalog_workflow(&c, &body, 1, &hash),
        other.install_catalog_workflow(&c, &body, 1, &hash)
    );
    assert_eq!(a.unwrap(), b.unwrap());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sequences")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn corrupted_engine_owner_is_neither_disclosed_nor_deleted_by_catalog_lookup() {
    let (storage, _dir) = fixture().await;
    let c = scope(&storage, "a", "one").await;
    let id = install(&storage, &c, 1).await;
    sqlx::query("UPDATE sequences SET tenant_id = 'different-owner' WHERE id = ?1")
        .bind(id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(storage.catalog_workflow(&c, "flow", None).await.is_err());
    assert!(storage.catalog_workflow_by_id(&c, id).await.is_err());
    assert!(
        storage
            .remove_catalog_workflow(&c, "flow", 1, &digest(1))
            .await
            .is_err()
    );
    assert!(
        storage
            .evict_catalog_workflow(&c, "flow", 1, 1, &digest(1))
            .await
            .is_err()
    );
    assert!(engine_definition(&storage, id).await.is_some());
}

#[tokio::test]
async fn foreign_catalogs_and_invalid_bodies_cannot_create_workflows() {
    let (storage, _dir) = fixture().await;
    let a = scope(&storage, "a", "one").await;
    let b = scope(&storage, "b", "one").await;
    let forged = CatalogScope {
        account: a.account.clone(),
        id: b.id,
    };
    assert!(
        storage
            .install_catalog_workflow(&forged, &definition("flow", 1), 1, &digest(1))
            .await
            .is_err()
    );
    assert!(
        storage
            .catalog_workflow(&forged, "flow", None)
            .await
            .is_err()
    );
    for body in [
        "not-json".into(),
        definition(" ", 1),
        definition("flow", 0),
        definition("flow", -1),
    ] {
        assert!(
            storage
                .install_catalog_workflow(&a, &body, 1, &digest(1))
                .await
                .is_err()
        );
    }
    assert!(matches!(
        storage
            .install_catalog_workflow(
                &a,
                &"x".repeat(crate::MAX_SEQUENCES_RESPONSE_BYTES + 1),
                1,
                &digest(1)
            )
            .await,
        Err(StorageError::QuotaExceeded(_))
    ));
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sequences")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}
