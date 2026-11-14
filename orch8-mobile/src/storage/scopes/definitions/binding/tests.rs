use super::*;
use crate::storage::scopes::definitions::tests::{bundle, bundle_in_tenant, catalog};
use orch8_storage::sqlite::SqliteStorage;
use std::sync::Arc;
pub(super) const REQUEST: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
pub(super) const SERVER: &str = "https://engine.example/";

#[tokio::test]
async fn a_new_claim_generation_reuses_the_first_graph_and_logical_run() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (mut wire, mut claim, reference) = bundle("private");
    claim.claim_epoch = 1;
    wire.scope = claim.clone();
    let first_graph = storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let first = storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    assert_eq!(first.definitions, first_graph);
    assert_eq!(
        first,
        storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
            .await
            .unwrap()
    );
    let old = claim.clone();
    claim.claim_epoch = u64::MAX;
    claim.worker_id = "replacement".into();
    wire.scope = claim.clone();
    let later_graph = storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    assert_ne!(first_graph.root_id, later_graph.root_id);
    let second = storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    assert_eq!(first.local_run_id, second.local_run_id);
    assert_eq!(first.definitions, second.definitions);
    assert_eq!(second.claim_epoch, u64::MAX);
    assert!(
        storage
            .bind_claim_run(&scope, SERVER, &old, &reference, REQUEST)
            .await
            .is_err()
    );
    let native: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(native, 0, "binding must not schedule native work");
}
#[tokio::test]
async fn cached_graph_and_binding_survive_reopen_without_the_envelope() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("binding.db");
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let scope = catalog(&storage, "alice", "one").await;
    let (wire, claim, reference) = bundle("private");
    let installed = storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let bound = storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    drop(wire);
    storage.pool().close().await;
    drop(storage);
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    assert_eq!(
        Some(installed),
        storage
            .load_claim_definitions(&scope, SERVER, &claim, &reference)
            .await
            .unwrap()
    );
    assert_eq!(
        bound,
        storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn sign_in_epoch_changes_do_not_mint_a_second_run_for_the_same_remote_task() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let old = catalog(&storage, "alice", "one").await;
    let (wire, claim, reference) = bundle("private");
    storage
        .install_claim_definitions(&old, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let first = storage
        .bind_claim_run(&old, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    storage.retire_account_scope(&account).await.unwrap();
    assert!(
        storage
            .bind_claim_run(&old, SERVER, &claim, &reference, REQUEST)
            .await
            .is_err()
    );
    assert!(
        storage
            .load_claim_definitions(&old, SERVER, &claim, &reference)
            .await
            .is_err()
    );
    let current = catalog(&storage, "alice", "one").await;
    let new_graph = storage
        .install_claim_definitions(&current, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    assert_ne!(first.definitions.root_id, new_graph.root_id);
    let second = storage
        .bind_claim_run(&current, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    assert_eq!(first.local_run_id, second.local_run_id);
    assert_eq!(first.definitions, second.definitions);
    assert!(
        storage
            .bind_claim_run(&old, SERVER, &claim, &reference, REQUEST)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn conflicting_worker_and_changed_bundle_cannot_rebind_a_logical_run() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (mut wire, mut claim, reference) = bundle("private");
    claim.claim_epoch = 1;
    wire.scope = claim.clone();
    storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let first = storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    let mut conflict = wire.clone();
    conflict.scope.worker_id = "conflicting-worker".into();
    storage
        .install_claim_definitions(&scope, SERVER, &conflict.scope, &reference, &conflict)
        .await
        .unwrap();
    assert!(
        storage
            .bind_claim_run(&scope, SERVER, &conflict.scope, &reference, REQUEST)
            .await
            .is_err()
    );
    let (mut changed, mut later, new_reference) = bundle("changed-definition");
    later.claim_epoch = 2;
    changed.scope = later.clone();
    storage
        .install_claim_definitions(&scope, SERVER, &later, &new_reference, &changed)
        .await
        .unwrap();
    assert!(
        storage
            .bind_claim_run(&scope, SERVER, &later, &new_reference, REQUEST)
            .await
            .is_err()
    );
    assert_eq!(
        first,
        storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
            .await
            .unwrap()
    );
}
#[tokio::test]
async fn unavailable_manifest_requires_verified_wire_repair_and_corrupt_manifest_fails_closed() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (wire, claim, reference) = bundle("private");
    let installed = storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let first = storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    sqlx::query("DELETE FROM mobile_claim_definition_manifests WHERE receipt_id=?1")
        .bind(&installed.receipt_id)
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(
        storage
            .load_claim_definitions(&scope, SERVER, &claim, &reference)
            .await
            .is_err()
    );
    assert!(
        storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
            .await
            .is_err()
    );
    assert_eq!(
        installed,
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .unwrap()
    );
    assert_eq!(
        first,
        storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
            .await
            .unwrap()
    );
    sqlx::query("UPDATE mobile_claim_definition_manifests SET manifest='{}' WHERE receipt_id=?1")
        .bind(&installed.receipt_id)
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(
        storage
            .load_claim_definitions(&scope, SERVER, &claim, &reference)
            .await
            .is_err()
    );
    assert!(
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .is_err()
    );
    let retained: String = sqlx::query_scalar(
        "SELECT manifest FROM mobile_claim_definition_manifests WHERE receipt_id=?1",
    )
    .bind(&installed.receipt_id)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(retained, "{}");
}
#[tokio::test]
async fn a_valid_new_generation_cannot_hide_a_corrupt_initial_graph() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (mut wire, mut claim, reference) = bundle("private");
    claim.claim_epoch = 1;
    wire.scope = claim.clone();
    storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let first = storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    claim.claim_epoch = 2;
    wire.scope = claim.clone();
    storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    sqlx::query("UPDATE sequences SET namespace='public' WHERE id=?1")
        .bind(first.definitions.root_id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(
        storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
            .await
            .is_err()
    );
    let current: String = sqlx::query_scalar("SELECT claim_epoch FROM mobile_remote_run_bindings")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(current, "1");
}
#[tokio::test]
async fn stable_identity_is_scoped_by_account_catalog_server_and_remote_task() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let another = uuid::Uuid::new_v4();
    let mut ids = std::collections::HashSet::new();
    for tenant in ["remote-one", "remote-two"] {
        let (wire, claim, reference) = bundle_in_tenant("private", Some(tenant));
        for account in ["alice", "bob"] {
            for key in ["one", "two"] {
                let scope = catalog(&storage, account, key).await;
                for server in [SERVER, "https://another.example/"] {
                    for task in [claim.task_id, another] {
                        let mut envelope = wire.clone();
                        envelope.scope.task_id = task;
                        storage
                            .install_claim_definitions(
                                &scope,
                                server,
                                &envelope.scope,
                                &reference,
                                &envelope,
                            )
                            .await
                            .unwrap();
                        let bound = storage
                            .bind_claim_run(&scope, server, &envelope.scope, &reference, REQUEST)
                            .await
                            .unwrap();
                        assert!(ids.insert(bound.local_run_id));
                    }
                }
            }
        }
    }
    assert_eq!(ids.len(), 32);
}

#[tokio::test]
async fn independent_connections_bind_the_same_claim_to_one_logical_run() {
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
    a.install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let (first, second) = tokio::join!(
        a.bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST),
        b.bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
    );
    assert_eq!(first.unwrap(), second.unwrap());
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_remote_run_bindings")
        .fetch_one(a.pool())
        .await
        .unwrap();
    assert_eq!(rows, 1);
}
#[tokio::test]
async fn failed_handoff_update_preserves_previous_claim_and_is_retryable() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (mut wire, mut claim, reference) = bundle("private");
    claim.claim_epoch = 1;
    wire.scope = claim.clone();
    storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let first = storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    claim.claim_epoch = 2;
    wire.scope = claim.clone();
    storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER fail_binding BEFORE UPDATE ON mobile_remote_run_bindings BEGIN SELECT RAISE(ABORT,'injected handoff failure'); END").execute(storage.pool()).await.unwrap();
    assert!(
        storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
            .await
            .is_err()
    );
    let current: String = sqlx::query_scalar("SELECT claim_epoch FROM mobile_remote_run_bindings")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(current, "1");
    sqlx::query("DROP TRIGGER fail_binding")
        .execute(storage.pool())
        .await
        .unwrap();
    let second = storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    assert_eq!(first.local_run_id, second.local_run_id);
    assert_eq!(second.claim_epoch, 2);
}
#[tokio::test]
async fn an_overflowed_retained_epoch_is_rejected_without_resetting_the_binding() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (wire, claim, reference) = bundle("private");
    storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    sqlx::query("UPDATE mobile_remote_run_bindings SET claim_epoch='18446744073709551616'")
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(
        storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
            .await
            .is_err()
    );
    let current: String = sqlx::query_scalar("SELECT claim_epoch FROM mobile_remote_run_bindings")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(current, "18446744073709551616");
}

#[tokio::test]
async fn changed_dynamic_request_fingerprint_cannot_reuse_a_run_even_with_identical_definitions() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (wire, claim, reference) = bundle("same-definition");
    storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let first = storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    assert!(
        storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, &"c".repeat(64))
            .await
            .is_err()
    );
    assert!(
        storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, "bad-digest")
            .await
            .is_err()
    );
    assert_eq!(
        first,
        storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
            .await
            .unwrap()
    );
}
#[tokio::test]
async fn a_foreign_current_receipt_cannot_be_adopted_despite_matching_hashes() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let alice = catalog(&storage, "alice", "one").await;
    let bob = catalog(&storage, "bob", "one").await;
    let (wire, claim, reference) = bundle("private");
    storage
        .install_claim_definitions(&alice, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let other = storage
        .install_claim_definitions(&bob, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let first = storage
        .bind_claim_run(&alice, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE mobile_remote_run_bindings SET current_receipt_id=?1 WHERE local_run_id=?2",
    )
    .bind(&other.receipt_id)
    .bind(first.local_run_id.to_string())
    .execute(storage.pool())
    .await
    .unwrap();
    assert!(
        storage
            .bind_claim_run(&alice, SERVER, &claim, &reference, REQUEST)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn manifest_repair_and_failed_graph_validation_are_one_transaction() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (wire, claim, reference) = bundle("private");
    let installed = storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    sqlx::query("DELETE FROM mobile_claim_definition_manifests WHERE receipt_id=?1")
        .bind(&installed.receipt_id)
        .execute(storage.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE sequences SET namespace='public' WHERE id=?1")
        .bind(installed.root_id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .is_err()
    );
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_claim_definition_manifests")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn reclaimed_run_reuses_verified_graph_at_capacity_without_new_installations() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (mut wire, mut claim, reference) = bundle("private");
    claim.claim_epoch = 1;
    wire.scope = claim.clone();
    storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let first = storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    for _ in 1..32 {
        let mut other = wire.clone();
        other.scope.task_id = uuid::Uuid::new_v4();
        storage
            .install_claim_definitions(&scope, SERVER, &other.scope, &reference, &other)
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
    for epoch in 2..=65 {
        claim.claim_epoch = epoch;
        claim.worker_id = format!("worker-{}", epoch % 3);
        let bound = storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
            .await
            .expect("a reclaimed run must reuse its verified graph at full capacity");
        assert_eq!(bound.local_run_id, first.local_run_id);
        assert_eq!(bound.definitions, first.definitions);
        assert_eq!(bound.claim_epoch, epoch);
    }
    for (table, expected) in [
        ("mobile_claim_definition_receipts", 32),
        ("sequences", 32),
        ("mobile_remote_run_bindings", 1),
        ("mobile_remote_run_observations", 1),
        ("task_instances", 0),
    ] {
        let count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(count, expected, "unexpected growth in {table}");
    }
}

#[tokio::test]
async fn sign_in_epoch_rebinds_cached_run_without_downloading_another_graph() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let mut scope = catalog(&storage, "alice", "one").await;
    let (wire, claim, reference) = bundle("private");
    storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    let first = storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    for _ in 0..4 {
        let account = storage
            .open_account_scope("alice", "issuer", "alice")
            .await
            .unwrap();
        storage.retire_account_scope(&account).await.unwrap();
        assert!(
            storage
                .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
                .await
                .is_err()
        );
        scope = catalog(&storage, "alice", "one").await;
        let rebound = storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
            .await
            .expect("a fresh local session must reuse its authenticated cached graph");
        assert_eq!(rebound, first);
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_claim_definition_receipts")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}
