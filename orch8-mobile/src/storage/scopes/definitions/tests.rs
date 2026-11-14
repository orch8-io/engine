use super::*;
use orch8_types::{
    delegation_definition::{
        DefinitionManifest, FrozenDelegationDefinition,
        wire::{EncodedDefinitionBundle, RetainedDefinitionDocument},
    },
    sequence::{BlockDefinition, SequenceDefinition},
};
use std::sync::Arc;

const SERVER: &str = "https://engine.example/";
pub(super) fn bundle(
    salt: &str,
) -> (
    DefinitionWireEnvelope,
    DefinitionReadScope,
    DefinitionBundleReference,
) {
    bundle_in_tenant(salt, None)
}
pub(super) fn bundle_in_tenant(
    salt: &str,
    tenant: Option<&str>,
) -> (
    DefinitionWireEnvelope,
    DefinitionReadScope,
    DefinitionBundleReference,
) {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../contracts/vectors/delegation-definition-v1.json"
    ))
    .unwrap();
    let mut definition: SequenceDefinition =
        serde_json::from_str(vector["retained_definition_json"].as_str().unwrap()).unwrap();
    if let Some(tenant) = tenant {
        definition.tenant_id = orch8_types::ids::TenantId::new(tenant).unwrap();
    }
    let tenant_id = definition.tenant_id.clone();
    definition.schema = Some("https://schemas.example/editor.json".into());
    let BlockDefinition::Step(step) = &mut definition.blocks[0] else {
        panic!("step")
    };
    step.params = serde_json::json!({"private":salt});
    let document = FrozenDelegationDefinition::new(definition).unwrap();
    let manifest = DefinitionManifest {
        version: 1,
        root_sha256: document.sequence_sha256.clone(),
        bindings: vec![],
    };
    let (encoded, _) = EncodedDefinitionBundle::from_retained(
        &serde_json::to_string(&manifest).unwrap(),
        vec![RetainedDefinitionDocument {
            sequence_sha256: document.sequence_sha256,
            definition_json: serde_json::to_string(&document.definition).unwrap(),
        }],
        DefinitionGraphLimits::default(),
    )
    .unwrap();
    let original: DefinitionWireEnvelope =
        serde_json::from_value(vector["envelope"].clone()).unwrap();
    let mut claim = original.scope;
    claim.tenant_id = tenant_id;
    let reference = encoded.reference().clone();
    (
        DefinitionWireEnvelope::new(claim.clone(), &encoded).unwrap(),
        claim,
        reference,
    )
}
pub(super) async fn catalog(storage: &MobileStorage, account: &str, catalog: &str) -> CatalogScope {
    let account = storage
        .open_account_scope(account, "issuer", account)
        .await
        .unwrap();
    storage
        .open_catalog_scope(&account, catalog, "source", "authority")
        .await
        .unwrap()
}
async fn count(storage: &MobileStorage, table: &str) -> i64 {
    // Trusted test-only table names.
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
        .fetch_one(storage.pool())
        .await
        .unwrap()
}
#[tokio::test]
async fn retry_and_database_reopen_reuse_ids_and_full_width_claim_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("claims.db");
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let scope = catalog(&storage, "alice", "catalog").await;
    let (wire, claim, reference) = bundle("PRIVATE_BODY");
    assert_eq!(claim.claim_epoch, u64::MAX);
    let first = storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    assert_eq!(
        first,
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .unwrap()
    );
    let epoch: String =
        sqlx::query_scalar("SELECT claim_epoch FROM mobile_claim_definition_receipts")
            .fetch_one(storage.pool())
            .await
            .unwrap();
    assert_eq!(epoch, u64::MAX.to_string());
    assert!(!format!("{first:?}").contains("PRIVATE_BODY"));
    storage.pool().close().await;
    drop(storage);
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    assert_eq!(
        first,
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .unwrap()
    );
    assert_eq!(count(&storage, "sequences").await, 1);
}
#[tokio::test]
async fn accounts_catalogs_servers_and_remote_claims_cannot_share_rows() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let (wire, claim, reference) = bundle("private");
    let mut roots = std::collections::HashSet::new();
    let mut namespaces = std::collections::HashSet::new();
    for account in ["alice", "bob"] {
        for catalog_id in ["one", "two"] {
            let scope = catalog(&storage, account, catalog_id).await;
            for server in [SERVER, "https://other.example/"] {
                let installed = storage
                    .install_claim_definitions(&scope, server, &claim, &reference, &wire)
                    .await
                    .unwrap();
                assert!(roots.insert(installed.root_id));
                assert!(namespaces.insert(installed.namespace.as_str().to_owned()));
            }
        }
    }
    assert_eq!(roots.len(), 8);
    let scope = catalog(&storage, "alice", "one").await;
    for key in ["worker", "epoch", "task", "tenant"] {
        let mut changed = claim.clone();
        match key {
            "worker" => changed.worker_id = "another-worker".into(),
            "epoch" => changed.claim_epoch = 1,
            "task" => changed.task_id = uuid::Uuid::new_v4(),
            _ => changed.tenant_id = orch8_types::ids::TenantId::new("other-tenant").unwrap(),
        }
        let mut envelope = wire.clone();
        envelope.scope = changed.clone();
        if key == "tenant" {
            assert!(
                storage
                    .install_claim_definitions(&scope, SERVER, &changed, &reference, &envelope)
                    .await
                    .is_err()
            );
            continue;
        }
        let installed = storage
            .install_claim_definitions(&scope, SERVER, &changed, &reference, &envelope)
            .await
            .unwrap();
        assert!(roots.insert(installed.root_id));
    }
}
#[tokio::test]
async fn changed_reference_corrupt_body_and_foreign_scope_fail_without_replacement() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (wire, claim, reference) = bundle("original");
    let first = storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let (changed, _, new_reference) = bundle("replacement");
    assert!(
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &new_reference, &changed)
            .await
            .is_err()
    );
    let mut wrong = claim.clone();
    wrong.worker_id = "wrong-worker".into();
    assert!(
        storage
            .install_claim_definitions(&scope, SERVER, &wrong, &reference, &wire)
            .await
            .is_err()
    );
    sqlx::query("UPDATE sequences SET namespace='public' WHERE id=?1")
        .bind(first.root_id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .is_err()
    );
    assert_eq!(count(&storage, "sequences").await, 1);
    assert_eq!(count(&storage, "mobile_claim_definition_receipts").await, 1);
}
#[tokio::test]
async fn failed_mapping_insert_rolls_back_graph_and_retry_reuses_claim() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (wire, claim, reference) = bundle("private");
    sqlx::query("CREATE TRIGGER fail_mapping BEFORE INSERT ON mobile_claim_definition_entries BEGIN SELECT RAISE(ABORT,'injected mapping failure'); END").execute(storage.pool()).await.unwrap();
    assert!(
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .is_err()
    );
    for table in [
        "sequences",
        "mobile_claim_definition_receipts",
        "mobile_claim_definition_entries",
    ] {
        assert_eq!(count(&storage, table).await, 0);
    }
    sqlx::query("DROP TRIGGER fail_mapping")
        .execute(storage.pool())
        .await
        .unwrap();
    storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
}
#[tokio::test]
async fn retirement_blocks_old_epoch_and_preserves_receipt_after_sign_in() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (wire, claim, reference) = bundle("private");
    let first = storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    storage.retire_account_scope(&account).await.unwrap();
    assert!(
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .is_err()
    );
    let reopened = catalog(&storage, "alice", "one").await;
    let second = storage
        .install_claim_definitions(&reopened, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    assert_ne!(first.root_id, second.root_id);
    assert_eq!(count(&storage, "mobile_claim_definition_receipts").await, 2);
    assert!(
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn catalog_capacity_rejects_new_claims_but_allows_existing_retry() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (wire, claim, reference) = bundle("private");
    let first = storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    for _ in 1..32 {
        let mut envelope = wire.clone();
        envelope.scope.task_id = uuid::Uuid::new_v4();
        storage
            .install_claim_definitions(&scope, SERVER, &envelope.scope, &reference, &envelope)
            .await
            .unwrap();
    }
    let mut excess = wire.clone();
    excess.scope.task_id = uuid::Uuid::new_v4();
    assert!(matches!(
        storage
            .install_claim_definitions(&scope, SERVER, &excess.scope, &reference, &excess)
            .await,
        Err(StorageError::QuotaExceeded(_))
    ));
    assert_eq!(count(&storage, "sequences").await, 32);
    assert_eq!(
        first,
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn independent_connections_install_one_physical_graph_for_same_claim() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("concurrent.db");
    let a = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let scope = catalog(&a, "alice", "one").await;
    let b = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let (wire, claim, reference) = bundle("private");
    let (first, second) = tokio::join!(
        a.install_claim_definitions(&scope, SERVER, &claim, &reference, &wire),
        b.install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
    );
    assert_eq!(first.unwrap(), second.unwrap());
    assert_eq!(count(&a, "sequences").await, 1);
    assert_eq!(count(&a, "mobile_claim_definition_entries").await, 1);
}
#[tokio::test]
async fn byte_capacity_rolls_back_the_excess_graph_before_committing_rows() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (wire, _, reference) = bundle(&"x".repeat(3_000_000));
    for _ in 0..5 {
        let mut envelope = wire.clone();
        envelope.scope.task_id = uuid::Uuid::new_v4();
        storage
            .install_claim_definitions(&scope, SERVER, &envelope.scope, &reference, &envelope)
            .await
            .unwrap();
    }
    let mut excess = wire.clone();
    excess.scope.task_id = uuid::Uuid::new_v4();
    assert!(matches!(
        storage
            .install_claim_definitions(&scope, SERVER, &excess.scope, &reference, &excess)
            .await,
        Err(StorageError::QuotaExceeded(_))
    ));
    for table in [
        "sequences",
        "mobile_claim_definition_receipts",
        "mobile_claim_definition_entries",
    ] {
        assert_eq!(count(&storage, table).await, 5);
    }
}
#[tokio::test]
async fn untrusted_server_endpoints_cannot_create_any_local_rows() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (wire, claim, reference) = bundle("private");
    for server in [
        "http://engine.example/",
        "https://localhost/",
        "https://user:secret@engine.example/",
        "https://engine.example/?secret=1",
        "https://ENGINE.example/",
    ] {
        assert!(
            storage
                .install_claim_definitions(&scope, server, &claim, &reference, &wire)
                .await
                .is_err()
        );
    }
    assert_eq!(count(&storage, "sequences").await, 0);
}

#[tokio::test]
async fn linked_graph_retains_pins_protects_rows_and_detects_changed_source_mapping() {
    use orch8_types::delegation_definition::{DefinitionBinding, DefinitionSelector};
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (wire, claim, reference) = bundle("private");
    let mut root = wire
        .verify(&claim, &reference, DefinitionGraphLimits::default())
        .unwrap()
        .root()
        .definition
        .clone();
    let mut child = root.clone();
    child.id = SequenceId::new();
    child.name = "child".into();
    child.version = 4;
    root.blocks.push(
        serde_json::from_value(
            serde_json::json!({"type":"sub_sequence","id":"call-child","sequence_name":"child"}),
        )
        .unwrap(),
    );
    let root = FrozenDelegationDefinition::new(root).unwrap();
    let child = FrozenDelegationDefinition::new(child).unwrap();
    let manifest = DefinitionManifest {
        version: 1,
        root_sha256: root.sequence_sha256.clone(),
        bindings: vec![DefinitionBinding {
            selector: DefinitionSelector {
                name: "child".into(),
                version: None,
            },
            sequence_sha256: child.sequence_sha256.clone(),
        }],
    };
    let (encoded, _) = EncodedDefinitionBundle::from_retained(
        &serde_json::to_string(&manifest).unwrap(),
        [root, child]
            .into_iter()
            .map(|d| RetainedDefinitionDocument {
                sequence_sha256: d.sequence_sha256,
                definition_json: serde_json::to_string(&d.definition).unwrap(),
            })
            .collect(),
        DefinitionGraphLimits::default(),
    )
    .unwrap();
    let reference = encoded.reference().clone();
    let wire = DefinitionWireEnvelope::new(claim.clone(), &encoded).unwrap();
    let installed = storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    assert_eq!(
        installed,
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .unwrap()
    );
    let mut tx = storage.pool().begin().await.unwrap();
    let definition = SqliteStorage::get_sequence_in_transaction(&mut tx, installed.root_id)
        .await
        .unwrap()
        .unwrap();
    let references =
        orch8_types::delegation_definition::definition_references(&definition).unwrap();
    assert_eq!(
        references,
        std::collections::BTreeSet::from([DefinitionSelector {
            name: "child".into(),
            version: Some(4)
        }])
    );
    tx.commit().await.unwrap();
    assert!(
        sqlx::query("DELETE FROM sequences WHERE namespace=?1")
            .bind(installed.namespace.as_str())
            .execute(storage.pool())
            .await
            .is_err()
    );
    sqlx::query("UPDATE mobile_claim_definition_entries SET source_sha256=?1 WHERE receipt_id=?2 AND sequence_id<>?3").bind("0".repeat(64)).bind(&installed.receipt_id).bind(installed.root_id.to_string()).execute(storage.pool()).await.unwrap();
    assert!(
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .is_err()
    );
    assert_eq!(count(&storage, "sequences").await, 2);
}

#[tokio::test]
async fn schema_73_reopen_adds_claim_cache_without_changing_account_identity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("upgrade.db");
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let scope = catalog(&storage, "alice", "one").await;
    // Synthesize the pre-foundation schema: version 73 plus the existing account.
    for table in [
        "mobile_remote_run_observations",
        "mobile_remote_run_bindings",
        "mobile_claim_definition_codecs",
        "mobile_claim_definition_manifests",
        "mobile_claim_definition_entries",
        "mobile_claim_definition_receipts",
        "worker_issued_delegation_contexts",
        "delegation_publication_inputs",
        "delegation_definition_manifests",
        "delegation_definitions",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!("DROP TABLE {table}")))
            .execute(storage.pool())
            .await
            .unwrap();
    }
    sqlx::query("DELETE FROM schema_versions WHERE version=74")
        .execute(storage.pool())
        .await
        .unwrap();
    storage.pool().close().await;
    drop(storage);
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let reopened = catalog(&storage, "alice", "one").await;
    assert_eq!(scope.account_id(), reopened.account_id());
    assert_eq!(scope.catalog_id(), reopened.catalog_id());
    let (wire, claim, reference) = bundle("private");
    storage
        .install_claim_definitions(&reopened, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    assert_eq!(count(&storage, "mobile_claim_definition_receipts").await, 1);
}

#[tokio::test]
async fn remote_identity_metadata_is_bounded_and_rejects_control_characters() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    for tenant in [&"t".repeat(4097), "tenant\nother"] {
        let (wire, claim, reference) = bundle_in_tenant("private", Some(tenant));
        assert!(
            storage
                .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
                .await
                .is_err()
        );
    }
    let (mut wire, mut claim, reference) = bundle("private");
    claim.worker_id = "worker\nother".into();
    wire.scope = claim.clone();
    assert!(
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .is_err()
    );
    assert_eq!(count(&storage, "sequences").await, 0);
}
