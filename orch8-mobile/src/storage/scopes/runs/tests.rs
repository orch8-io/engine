use super::*;
use orch8_storage::{InstanceStore, SequenceStore, sqlite::SqliteStorage};
use std::sync::Arc;

pub(super) fn limits() -> CatalogRunLimits {
    CatalogRunLimits {
        global_records: 16,
        account_records: 8,
        catalog_records: 4,
        global_input_bytes: 1024 * 1024,
        account_input_bytes: 512 * 1024,
        catalog_input_bytes: 256 * 1024,
    }
}
pub(super) fn request<'a>(key: Option<&'a str>, input: &'a str) -> CatalogRunRequest<'a> {
    CatalogRunRequest {
        workflow: "flow",
        version: None,
        input_json: input,
        dedup_key: key,
    }
}
fn digest(version: i64) -> String {
    hex::encode(Sha256::digest(version.to_be_bytes()))
}
fn body(version: i32) -> String {
    serde_json::json!({"id":"11111111-1111-4111-8111-111111111111","tenant_id":"remote","namespace":"remote","name":"flow","version":version,"blocks":[],"created_at":"2026-01-01T00:00:00Z"}).to_string()
}

pub(super) async fn fixture() -> (Arc<MobileStorage>, Arc<SqliteStorage>) {
    let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
    (Arc::new(MobileStorage::new(Arc::clone(&sqlite))), sqlite)
}
pub(super) async fn scope(storage: &MobileStorage, account: &str, catalog: &str) -> CatalogScope {
    let owner = storage
        .open_account_scope(account, "issuer", account)
        .await
        .unwrap();
    let scope = storage
        .open_catalog_scope(&owner, catalog, "source", "authority")
        .await
        .unwrap();
    install(storage, &scope, 1, 1).await;
    scope
}
async fn install(
    storage: &MobileStorage,
    scope: &CatalogScope,
    version: i32,
    manifest: i64,
) -> SequenceId {
    storage
        .accept_catalog_manifest(scope, &[], manifest, &digest(manifest))
        .await
        .unwrap();
    let id = storage
        .install_catalog_workflow(scope, &body(version), manifest, &digest(manifest))
        .await
        .unwrap();
    storage
        .mark_catalog_reconciled(scope, manifest, &digest(manifest))
        .await
        .unwrap();
    id
}

#[tokio::test]
async fn four_scopes_share_operation_keys_without_cross_account_or_catalog_access() {
    let (storage, sqlite) = fixture().await;
    let mut runs = Vec::new();
    for account in ["alice", "bob"] {
        for catalog in ["one", "two"] {
            let scope = scope(&storage, account, catalog).await;
            let run = storage
                .prepare_catalog_run(&scope, request(Some("same"), r#"{"b":2,"a":1}"#), limits())
                .await
                .unwrap();
            assert_eq!(run.owner_epoch, scope.account.epoch);
            assert_eq!(run.account_id, scope.account.id);
            assert_eq!(run.catalog_id, scope.id);
            assert_eq!(run.state, CatalogRunState::Prepared);
            assert_eq!(run.workflow, "flow");
            assert_eq!(run.version, 1);
            assert_eq!(run.input, serde_json::json!({"a":1,"b":2}));
            let retry = storage
                .prepare_catalog_run(
                    &scope,
                    request(Some("same"), r#"{ "a": 1, "b": 2 }"#),
                    limits(),
                )
                .await
                .unwrap();
            assert_eq!(retry.id, run.id);
            assert!(
                sqlite.get_instance(run.id).await.unwrap().is_none(),
                "durable requests must not enter the legacy scheduler"
            );
            runs.push((scope, run));
        }
    }
    let ids: std::collections::HashSet<_> = runs.iter().map(|(_, run)| run.id).collect();
    assert_eq!(ids.len(), 4);
    for (index, (owner, _)) in runs.iter().enumerate() {
        for (other, (_, run)) in runs.iter().enumerate() {
            if index != other {
                assert!(storage.catalog_run(owner, run.id).await.unwrap().is_none());
                assert!(!storage.cancel_catalog_run(owner, run.id).await.unwrap());
                assert!(
                    !storage
                        .forget_cancelled_catalog_run(owner, run.id)
                        .await
                        .unwrap()
                );
            }
        }
    }
}

#[tokio::test]
async fn idempotency_binds_request_but_preserves_first_latest_version_and_full_capacity_retry() {
    let (storage, _) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let first = storage
        .prepare_catalog_run(&scope, request(Some("operation"), r#"{"x":1}"#), limits())
        .await
        .unwrap();
    let new = install(&storage, &scope, 2, 2).await;
    assert_ne!(first.sequence_id, new);
    let closed = CatalogRunLimits {
        global_records: 0,
        account_records: 0,
        catalog_records: 0,
        global_input_bytes: 0,
        account_input_bytes: 0,
        catalog_input_bytes: 0,
    };
    let retry = storage
        .prepare_catalog_run(&scope, request(Some("operation"), r#"{"x":1}"#), closed)
        .await
        .unwrap();
    assert_eq!(retry.id, first.id);
    assert_eq!(retry.version, 1);
    assert_eq!(retry.created_at, first.created_at);
    assert_eq!(retry.admitted_manifest_version, 1);
    assert_eq!(retry.admitted_manifest_digest, digest(1));
    assert!(
        storage
            .prepare_catalog_run(&scope, request(Some("operation"), r#"{"x":2}"#), limits())
            .await
            .is_err()
    );
    assert!(
        storage
            .prepare_catalog_run(
                &scope,
                CatalogRunRequest {
                    workflow: "other",
                    ..request(Some("operation"), r#"{"x":1}"#)
                },
                limits()
            )
            .await
            .is_err()
    );
    assert!(
        storage
            .prepare_catalog_run(
                &scope,
                CatalogRunRequest {
                    version: Some(1),
                    ..request(Some("operation"), r#"{"x":1}"#)
                },
                limits()
            )
            .await
            .is_err()
    );
    let latest = storage
        .prepare_catalog_run(&scope, request(None, "{}"), limits())
        .await
        .unwrap();
    assert_eq!(latest.version, 2);
}

#[tokio::test]
async fn retirement_preserves_requests_and_fences_old_handles_without_re_admitting_old_keys() {
    let (storage, _) = fixture().await;
    let old = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&old, request(Some("key"), "{}"), limits())
        .await
        .unwrap();
    storage.retire_account_scope(&old.account).await.unwrap();
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    let fresh = storage
        .open_catalog_scope(&account, "one", "source", "authority")
        .await
        .unwrap();
    assert!(storage.catalog_run(&old, run.id).await.is_err());
    assert!(storage.cancel_catalog_run(&old, run.id).await.is_err());
    assert!(
        storage
            .forget_cancelled_catalog_run(&old, run.id)
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .catalog_run(&fresh, run.id)
            .await
            .unwrap()
            .unwrap()
            .owner_epoch,
        old.account.epoch
    );
    assert!(
        storage
            .prepare_catalog_run(&fresh, request(Some("key"), "{}"), limits())
            .await
            .is_err()
    );
    let new = storage
        .prepare_catalog_run(&fresh, request(Some("new"), "{}"), limits())
        .await
        .unwrap();
    assert_eq!(new.owner_epoch, account.epoch);
    assert!(storage.cancel_catalog_run(&fresh, run.id).await.unwrap());
    assert!(
        storage
            .forget_cancelled_catalog_run(&fresh, run.id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn accepted_but_unreconciled_manifest_admits_no_new_work_or_marker_forgery() {
    let (storage, _) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&scope, request(Some("old"), "{}"), limits())
        .await
        .unwrap();
    storage
        .accept_catalog_manifest(&scope, &[], 2, &digest(2))
        .await
        .unwrap();
    assert!(
        storage
            .prepare_catalog_run(&scope, request(Some("new"), "{}"), limits())
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .prepare_catalog_run(&scope, request(Some("old"), "{}"), limits())
            .await
            .unwrap()
            .id,
        run.id
    );
    for key in [
        super::super::RECONCILED_VERSION,
        super::super::RECONCILED_DIGEST,
    ] {
        assert!(
            storage
                .update_catalog_metadata(&scope, &[(key, Some("forged"))])
                .await
                .is_err()
        );
    }
    assert!(
        storage
            .mark_catalog_reconciled(&scope, 1, &digest(1))
            .await
            .is_err()
    );
    storage
        .mark_catalog_reconciled(&scope, 2, &digest(2))
        .await
        .unwrap();
    storage
        .prepare_catalog_run(&scope, request(Some("new"), "{}"), limits())
        .await
        .unwrap();
}

#[tokio::test]
async fn prepared_work_pins_definition_and_only_explicit_cancelled_cleanup_releases_it() {
    let (storage, sqlite) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&scope, request(Some("key"), "{}"), limits())
        .await
        .unwrap();
    assert!(
        !storage
            .forget_cancelled_catalog_run(&scope, run.id)
            .await
            .unwrap()
    );
    assert!(sqlite.delete_sequence(run.sequence_id).await.is_err());
    assert!(
        sqlx::query("DELETE FROM sequences WHERE id = ?1")
            .bind(run.sequence_id.to_string())
            .execute(storage.pool())
            .await
            .is_err(),
        "schema must also fence clients unaware of scoped request pins"
    );
    assert!(
        storage
            .evict_catalog_workflow(&scope, "flow", 1, 1, &digest(1))
            .await
            .is_err()
    );
    let removed = storage
        .remove_catalog_workflow(&scope, "flow", 1, &digest(1))
        .await
        .unwrap();
    assert_eq!(removed.retained, 1);
    assert_eq!(removed.deleted, 0);
    assert!(
        storage
            .catalog_workflow(&scope, "flow", None)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        storage
            .prepare_catalog_run(&scope, request(Some("key"), "{}"), limits())
            .await
            .unwrap()
            .id,
        run.id
    );
    assert!(storage.cancel_catalog_run(&scope, run.id).await.unwrap());
    sqlite.delete_sequence(run.sequence_id).await.unwrap();
    assert_eq!(
        storage
            .catalog_run(&scope, run.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        CatalogRunState::Cancelled
    );
    assert!(
        storage
            .forget_cancelled_catalog_run(&scope, run.id)
            .await
            .unwrap()
    );
    assert!(storage.catalog_run(&scope, run.id).await.unwrap().is_none());
}

#[tokio::test]
async fn quotas_count_utf8_bytes_and_cancelled_records_until_explicit_cleanup() {
    let (storage, _) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let cap = CatalogRunLimits {
        global_input_bytes: 7,
        ..limits()
    };
    let first = storage
        .prepare_catalog_run(&scope, request(Some("1"), "\"é\""), cap)
        .await
        .unwrap();
    assert!(matches!(
        storage
            .prepare_catalog_run(&scope, request(Some("2"), "\"é\""), cap)
            .await,
        Err(StorageError::QuotaExceeded(_))
    ));
    storage.cancel_catalog_run(&scope, first.id).await.unwrap();
    assert!(
        storage
            .prepare_catalog_run(&scope, request(Some("2"), "\"é\""), cap)
            .await
            .is_err()
    );
    storage
        .forget_cancelled_catalog_run(&scope, first.id)
        .await
        .unwrap();
    storage
        .prepare_catalog_run(&scope, request(Some("2"), "\"é\""), cap)
        .await
        .unwrap();
}

#[tokio::test]
async fn global_account_and_catalog_record_caps_are_checked_under_one_writer() {
    let (storage, _) = fixture().await;
    let a = scope(&storage, "alice", "one").await;
    let aa = scope(&storage, "alice", "two").await;
    let b = scope(&storage, "bob", "one").await;
    let cap = CatalogRunLimits {
        global_records: 3,
        account_records: 2,
        catalog_records: 1,
        ..limits()
    };
    storage
        .prepare_catalog_run(&a, request(Some("1"), "{}"), cap)
        .await
        .unwrap();
    assert!(
        storage
            .prepare_catalog_run(&a, request(Some("2"), "{}"), cap)
            .await
            .is_err()
    );
    storage
        .prepare_catalog_run(&aa, request(Some("1"), "{}"), cap)
        .await
        .unwrap();
    let aaa = scope(&storage, "alice", "three").await;
    assert!(
        storage
            .prepare_catalog_run(&aaa, request(None, "{}"), cap)
            .await
            .is_err()
    );
    storage
        .prepare_catalog_run(&b, request(None, "{}"), cap)
        .await
        .unwrap();
    let c = scope(&storage, "charlie", "one").await;
    assert!(
        storage
            .prepare_catalog_run(&c, request(None, "{}"), cap)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn failed_request_insert_leaves_no_partial_admission_and_inputs_are_bounded() {
    let (storage, _) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    sqlx::query("CREATE TRIGGER reject_run BEFORE INSERT ON mobile_catalog_run_requests BEGIN SELECT RAISE(ABORT,'injected request failure'); END").execute(storage.pool()).await.unwrap();
    assert!(
        storage
            .prepare_catalog_run(&scope, request(Some("key"), "{}"), limits())
            .await
            .is_err()
    );
    sqlx::query("DROP TRIGGER reject_run")
        .execute(storage.pool())
        .await
        .unwrap();
    storage
        .prepare_catalog_run(&scope, request(Some("key"), "{}"), limits())
        .await
        .unwrap();
    let too_large = format!("\"{}\"", "x".repeat(MAX_INPUT_BYTES));
    assert!(matches!(
        storage
            .prepare_catalog_run(&scope, request(None, &too_large), limits())
            .await,
        Err(StorageError::QuotaExceeded(_))
    ));
    assert!(
        storage
            .prepare_catalog_run(&scope, request(None, "not-json"), limits())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn independent_pools_deduplicate_and_cannot_overbook_a_single_record_allowance() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runs.db");
    let path = path.to_str().unwrap();
    let first = Arc::new(MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path).await.unwrap(),
    )));
    let scope = scope(&first, "alice", "one").await;
    let second = Arc::new(MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path).await.unwrap(),
    )));
    let cap = CatalogRunLimits {
        global_records: 1,
        ..limits()
    };
    let (a, b) = tokio::join!(
        first.prepare_catalog_run(&scope, request(Some("same"), "{}"), cap),
        second.prepare_catalog_run(&scope, request(Some("same"), "{}"), cap)
    );
    let a = a.unwrap();
    assert_eq!(a.id, b.unwrap().id);
    first.cancel_catalog_run(&scope, a.id).await.unwrap();
    first
        .forget_cancelled_catalog_run(&scope, a.id)
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        first.prepare_catalog_run(&scope, request(Some("one"), "{}"), cap),
        second.prepare_catalog_run(&scope, request(Some("two"), "{}"), cap)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_catalog_run_requests")
        .fetch_one(first.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn full_database_reopen_preserves_owner_input_and_selected_version() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reopen.db");
    let path = path.to_str().unwrap();
    let sqlite = Arc::new(SqliteStorage::file_mobile(path).await.unwrap());
    let storage = MobileStorage::new(Arc::clone(&sqlite));
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&scope, request(Some("key"), "{\"private\":42}"), limits())
        .await
        .unwrap();
    sqlite.pool().close().await;
    let storage = MobileStorage::new(Arc::new(SqliteStorage::file_mobile(path).await.unwrap()));
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    let fresh = storage
        .open_catalog_scope(&account, "one", "source", "authority")
        .await
        .unwrap();
    let restored = storage
        .prepare_catalog_run(&fresh, request(Some("key"), "{\"private\":42}"), limits())
        .await
        .unwrap();
    assert_eq!(restored.id, run.id);
    assert_eq!(restored.sequence_id, run.sequence_id);
    assert_eq!(restored.input, run.input);
    assert_eq!(restored.created_at, run.created_at);
    assert_eq!(
        restored.admitted_manifest_digest,
        run.admitted_manifest_digest
    );
}

#[tokio::test]
async fn a_normal_engine_tick_cannot_dispatch_a_prepared_request() {
    let (storage, sqlite) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&scope, request(Some("key"), "{}"), limits())
        .await
        .unwrap();
    let backend: Arc<dyn orch8_storage::StorageBackend> = sqlite;
    let result = orch8_engine::scheduler::tick_once(
        &backend,
        &Arc::new(orch8_engine::handlers::HandlerRegistry::new()),
        &Arc::new(tokio::sync::Semaphore::new(1)),
        &orch8_types::config::SchedulerConfig::default(),
        &Arc::new(orch8_engine::sequence_cache::SequenceCache::new(
            16,
            std::time::Duration::from_secs(30),
        )),
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(result.instances_advanced, 0);
    assert_eq!(result.steps_executed, 0);
    assert!(backend.get_instance(run.id).await.unwrap().is_none());
    assert_eq!(
        storage
            .catalog_run(&scope, run.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        CatalogRunState::Prepared
    );
}

#[tokio::test]
async fn quota_queries_use_a_covering_metadata_index_not_input_payloads() {
    let (storage, _) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    storage
        .prepare_catalog_run(&scope, request(Some("key"), "\"é\""), limits())
        .await
        .unwrap();
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "EXPLAIN QUERY PLAN {USAGE_SQL}"
    )))
    .bind(&scope.account.id)
    .bind(&scope.id)
    .fetch_all(storage.pool())
    .await
    .unwrap();
    let plan = rows
        .iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("COVERING INDEX idx_mobile_catalog_run_usage"),
        "{plan}"
    );
    assert!(
        sqlx::query("UPDATE mobile_catalog_run_requests SET input_bytes = 0")
            .execute(storage.pool())
            .await
            .is_err(),
        "stored accounting cannot understate UTF-8 payload bytes"
    );
}

#[tokio::test]
async fn corrupted_input_cannot_be_relabelled_as_an_idempotent_retry() {
    let (storage, _) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&scope, request(Some("key"), "{\"x\":1}"), limits())
        .await
        .unwrap();
    sqlx::query("UPDATE mobile_catalog_run_requests SET input = '{\"x\":2}' WHERE id = ?1")
        .bind(run.id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(storage.catalog_run(&scope, run.id).await.is_err());
    assert!(
        storage
            .prepare_catalog_run(&scope, request(Some("key"), "{\"x\":1}"), limits())
            .await
            .is_err()
    );
    assert!(storage.cancel_catalog_run(&scope, run.id).await.unwrap());
    assert!(
        storage
            .forget_cancelled_catalog_run(&scope, run.id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn fully_evaluated_older_cache_can_admit_offline_without_forging_pending_readiness() {
    let (storage, _) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    storage
        .commit_catalog_evaluation(&scope, 1, &digest(1), None, "old-sdk")
        .await
        .unwrap();
    sqlx::query("DELETE FROM mobile_catalog_metadata WHERE key IN ('catalog_reconciled_version','catalog_reconciled_digest')").execute(storage.pool()).await.unwrap();
    storage
        .prepare_catalog_run(&scope, request(Some("offline"), "{}"), limits())
        .await
        .unwrap();
    storage
        .accept_catalog_manifest(&scope, &[], 2, &digest(2))
        .await
        .unwrap();
    assert!(
        storage
            .prepare_catalog_run(&scope, request(Some("new"), "{}"), limits())
            .await
            .is_err(),
        "an older applied cursor cannot authorize a newer pending document"
    );
}

#[tokio::test]
async fn staging_cleanup_does_not_erase_an_id_that_has_materialized_elsewhere() {
    let (storage, sqlite) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&scope, request(Some("key"), "{}"), limits())
        .await
        .unwrap();
    let definition = sqlite.get_sequence(run.sequence_id).await.unwrap().unwrap();
    let instance:orch8_types::instance::TaskInstance=serde_json::from_value(serde_json::json!({
        "id":run.id,"sequence_id":run.sequence_id,"tenant_id":definition.tenant_id,"namespace":definition.namespace,
        "state":orch8_types::instance::InstanceState::Cancelled,"priority":"Normal","timezone":"UTC","metadata":{},
        "context":orch8_types::context::ExecutionContext::default(),"created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now()
    })).unwrap();
    sqlite.create_instance(&instance).await.unwrap();
    assert!(!storage.cancel_catalog_run(&scope, run.id).await.unwrap());
    assert!(
        !storage
            .forget_cancelled_catalog_run(&scope, run.id)
            .await
            .unwrap()
    );
    assert!(storage.catalog_run(&scope, run.id).await.unwrap().is_some());
}

// Test-only stand-in for future materialization. No production scoped API emits
// native instances yet: handler/builtin ownership must be installed first.
pub(super) async fn native_fixture(
    sqlite: &SqliteStorage,
    run: &CatalogRun,
) -> orch8_types::instance::TaskInstance {
    let definition = sqlite.get_sequence(run.sequence_id).await.unwrap().unwrap();
    let instance = serde_json::from_value(serde_json::json!({
        "id":run.id,"sequence_id":run.sequence_id,"tenant_id":definition.tenant_id,"namespace":definition.namespace,
        "state":"scheduled","priority":"Normal","timezone":"UTC","metadata":{},
        "context":orch8_types::context::ExecutionContext::default(),"created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now()
    })).unwrap();
    sqlite.create_instance(&instance).await.unwrap();
    instance
}

#[tokio::test]
async fn execution_scope_retirement_fences_existing_claim_writes_and_reopen() {
    use orch8_storage::ResourceStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("epochs.sqlite");
    let sqlite = Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    );
    let storage = MobileStorage::new(sqlite.clone());
    let independent = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let scope = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&scope, request(Some("native"), "{}"), limits())
        .await
        .unwrap();
    native_fixture(&sqlite, &run).await;
    let claimed = sqlite
        .claim_due_instances(chrono::Utc::now(), 1, 0)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let token = claimed[0].claimed_at.unwrap();
    assert!(
        sqlite
            .set_instance_kv_claimed(run.id, "before", &serde_json::json!(1), token)
            .await
            .unwrap()
    );
    independent
        .retire_account_scope(&scope.account)
        .await
        .unwrap();
    for reopen in [false, true] {
        if reopen {
            storage
                .open_account_scope("alice", "issuer", "alice")
                .await
                .unwrap();
        }
        assert_claim_fenced(&sqlite, &run, token).await;
    }
    assert!(
        sqlite
            .get_instance_kv(run.id, "late")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        sqlite.get_instance(run.id).await.unwrap().unwrap().state,
        orch8_types::instance::InstanceState::Running
    );
    assert_successor_can_execute(&storage, &sqlite).await;
}

async fn assert_claim_fenced(
    sqlite: &SqliteStorage,
    run: &CatalogRun,
    token: chrono::DateTime<chrono::Utc>,
) {
    use orch8_storage::{OutputStore, ResourceStore};
    assert!(
        !sqlite
            .set_instance_kv_claimed(run.id, "late", &serde_json::json!(2), token)
            .await
            .unwrap()
    );
    let output = orch8_types::output::BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: run.id,
        block_id: orch8_types::ids::BlockId::new("late"),
        output: serde_json::json!({"secret":42}),
        output_ref: None,
        output_size: 13,
        attempt: 1,
        created_at: chrono::Utc::now(),
    };
    assert!(
        !sqlite
            .save_block_output_claimed(&output, token)
            .await
            .unwrap()
    );
    assert!(
        !sqlite
            .conditional_update_claimed_instance_state_with_outbox(
                run.id,
                token,
                orch8_types::instance::InstanceState::Running,
                orch8_types::instance::InstanceState::Completed,
                None,
                &[],
            )
            .await
            .unwrap()
    );
    assert_eq!(
        sqlite.heartbeat_instance(run.id, token).await.unwrap(),
        orch8_storage::InstanceLease::Lost
    );
    assert!(
        !sqlite
            .conditional_update_claimed_instance_state(
                run.id,
                token,
                orch8_types::instance::InstanceState::Running,
                orch8_types::instance::InstanceState::Completed,
                None
            )
            .await
            .unwrap()
    );
}

async fn assert_successor_can_execute(storage: &MobileStorage, sqlite: &SqliteStorage) {
    use orch8_storage::ResourceStore;
    let fresh = scope(storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&fresh, request(Some("successor"), "{}"), limits())
        .await
        .unwrap();
    native_fixture(sqlite, &run).await;
    let claimed = sqlite
        .claim_due_instances(chrono::Utc::now(), 1, 0)
        .await
        .unwrap();
    assert_eq!(
        claimed.iter().map(|i| i.id).collect::<Vec<_>>(),
        vec![run.id]
    );
    let token = claimed[0].claimed_at.unwrap();
    assert_eq!(
        sqlite.heartbeat_instance(run.id, token).await.unwrap(),
        orch8_storage::InstanceLease::Held
    );
    assert!(
        sqlite
            .set_instance_kv_claimed(run.id, "successor", &serde_json::json!(1), token)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn execution_scope_retired_candidates_cannot_starve_another_account() {
    let (storage, sqlite) = fixture().await;
    let a = scope(&storage, "alice", "one").await;
    let b = scope(&storage, "bob", "one").await;
    let first = storage
        .prepare_catalog_run(&a, request(Some("a"), "{}"), limits())
        .await
        .unwrap();
    let second = storage
        .prepare_catalog_run(&b, request(Some("b"), "{}"), limits())
        .await
        .unwrap();
    native_fixture(&sqlite, &first).await;
    native_fixture(&sqlite, &second).await;
    sqlx::query("UPDATE task_instances SET priority = 2 WHERE id = ?1")
        .bind(first.id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    storage.retire_account_scope(&a.account).await.unwrap();
    for per_tenant in [0, 1] {
        let claimed = sqlite
            .claim_due_instances(chrono::Utc::now(), 1, per_tenant)
            .await
            .unwrap();
        assert_eq!(
            claimed.iter().map(|i| i.id).collect::<Vec<_>>(),
            vec![second.id]
        );
        sqlite
            .update_instance_state(
                second.id,
                orch8_types::instance::InstanceState::Scheduled,
                None,
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn execution_scope_proof_cannot_be_relabelled_or_omitted() {
    let (storage, sqlite) = fixture().await;
    let scope = scope(&storage, "alice", "one").await;
    for mutation in ["tenant", "namespace", "definition", "missing"] {
        let run = storage
            .prepare_catalog_run(&scope, request(Some(mutation), "{}"), limits())
            .await
            .unwrap();
        let mut instance = native_fixture(&sqlite, &run).await;
        // Mutations are intentionally made by raw SQL: a future scoped
        // materializer must not accidentally turn malformed rows into legacy.
        match mutation {
            "tenant" => {
                sqlx::query("UPDATE task_instances SET tenant_id='legacy' WHERE id=?1")
                    .bind(run.id.to_string())
                    .execute(storage.pool())
                    .await
                    .unwrap();
            }
            "namespace" => {
                sqlx::query("UPDATE task_instances SET namespace='catalog-other' WHERE id=?1")
                    .bind(run.id.to_string())
                    .execute(storage.pool())
                    .await
                    .unwrap();
            }
            "definition" => {
                sqlx::query("UPDATE task_instances SET sequence_id=?2 WHERE id=?1")
                    .bind(run.id.to_string())
                    .bind(SequenceId::new().to_string())
                    .execute(storage.pool())
                    .await
                    .unwrap();
            }
            _ => {
                instance.id = InstanceId::new();
                sqlite.create_instance(&instance).await.unwrap();
                sqlite
                    .update_instance_state(
                        run.id,
                        orch8_types::instance::InstanceState::Cancelled,
                        None,
                    )
                    .await
                    .unwrap();
            }
        }
    }
    for cap in [0, 1] {
        assert!(
            sqlite
                .claim_due_instances(chrono::Utc::now(), 32, cap)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn execution_scope_retirement_while_native_callback_is_queued_prevents_entry() {
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
        task::Poll,
    };
    struct Counter(Arc<AtomicUsize>);
    impl crate::handlers::StepHandler for Counter {
        fn execute(
            &self,
            _name: String,
            _input: String,
        ) -> Result<String, crate::error::HandlerError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok("{}".into())
        }
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    for reopen in [false, true] {
        let (storage, sqlite) = runtime.block_on(fixture());
        let (scope, ctx) = runtime.block_on(async {
            let scope = scope(&storage, "alice", "one").await;
            let run = storage
                .prepare_catalog_run(&scope, request(Some("queued"), "{}"), limits())
                .await
                .unwrap();
            let native = native_fixture(&sqlite, &run).await;
            let claim = sqlite
                .claim_due_instances(chrono::Utc::now(), 1, 0)
                .await
                .unwrap()
                .pop()
                .unwrap();
            let ctx = orch8_engine::handlers::StepContext {
                effect_admission: None,
                claim_token: claim.claimed_at,
                instance_id: run.id,
                tenant_id: native.tenant_id,
                block_id: orch8_types::ids::BlockId::new("work"),
                params: serde_json::json!({}),
                context: Arc::new(orch8_types::context::ExecutionContext::default()),
                attempt: 1,
                storage: sqlite.clone(),
                wait_for_input: None,
            };
            (scope, ctx)
        });
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = runtime.spawn_blocking(move || {
            entered_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(10));
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = orch8_engine::handlers::HandlerRegistry::new();
        crate::handlers::register_foreign_handler(
            &mut registry,
            "queued",
            Arc::new(Counter(calls.clone())),
            std::time::Duration::from_secs(5),
            Arc::new(crate::stragglers::Stragglers::default()),
        );
        runtime.block_on(async {
            let mut pending = Box::pin(registry.get("queued").unwrap()(ctx));
            std::future::poll_fn(|cx| {
                assert!(pending.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            storage.retire_account_scope(&scope.account).await.unwrap();
            if reopen {
                storage
                    .open_account_scope("alice", "issuer", "alice")
                    .await
                    .unwrap();
            }
            release_tx.send(()).unwrap();
            blocker.await.unwrap();
            assert!(matches!(
                pending.await,
                Err(orch8_types::error::StepError::ClaimLost)
            ));
            tokio::task::spawn_blocking(|| ()).await.unwrap();
        });
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
