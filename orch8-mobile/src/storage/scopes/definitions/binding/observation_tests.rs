use super::tests::{REQUEST, SERVER};
use super::*;
use crate::storage::scopes::definitions::tests::{bundle, catalog};
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::delegation_definition::wire::DefinitionWireEnvelope;
use serde_json::{Value, json};
use std::sync::Arc;

struct Fixture {
    storage: MobileStorage,
    scope: CatalogScope,
    wire: DefinitionWireEnvelope,
    claim: DefinitionReadScope,
    reference: DefinitionBundleReference,
    bound: BoundClaimRun,
}
impl Fixture {
    async fn new() -> Self {
        Self::in_storage(MobileStorage::new(Arc::new(
            SqliteStorage::in_memory().await.unwrap(),
        )))
        .await
    }
    async fn in_storage(storage: MobileStorage) -> Self {
        let scope = catalog(&storage, "alice", "one").await;
        let (mut wire, mut claim, reference) = bundle("private");
        claim.claim_epoch = 1;
        wire.scope = claim.clone();
        storage
            .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
            .await
            .unwrap();
        let bound = storage
            .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
            .await
            .unwrap();
        Self {
            storage,
            scope,
            wire,
            claim,
            reference,
            bound,
        }
    }
    async fn advance(&mut self) -> BoundClaimRun {
        self.claim.claim_epoch += 1;
        self.storage
            .try_bind_claim_run(&self.scope, SERVER, &self.claim, &self.reference, REQUEST)
            .await
            .unwrap()
            .unwrap()
    }
    async fn record(&self) -> String {
        sqlx::query_scalar(
            "SELECT record FROM mobile_remote_run_observations WHERE local_run_id=?1",
        )
        .bind(self.bound.local_run_id.to_string())
        .fetch_one(self.storage.pool())
        .await
        .unwrap()
    }
    async fn replace_record(&self, record: &str) {
        sqlx::query("UPDATE mobile_remote_run_observations SET record=?2 WHERE local_run_id=?1")
            .bind(self.bound.local_run_id.to_string())
            .bind(record)
            .execute(self.storage.pool())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn absent_cache_is_distinct_from_invalid_retained_state() {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    let scope = catalog(&storage, "alice", "one").await;
    let (_, claim, reference) = bundle("private");
    assert!(
        storage
            .try_bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
            .await
            .unwrap()
            .is_none()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_remote_run_bindings")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn changed_observation_fields_and_unknown_versions_fail_without_restamping() {
    const PRIVATE: &str = "OBSERVATION_PRIVATE_CANARY";
    let mut f = Fixture::new().await;
    f.advance().await;
    let original = f.record().await;
    for variant in 0..12 {
        let mut changed: Value = serde_json::from_str(&original).unwrap();
        match variant {
            0 => changed["version"] = json!(2),
            1 => changed["owner_epoch"] = json!(99),
            2 => changed["claim"]["worker_id"] = json!("other-worker"),
            3 => changed["claim"]["claim_epoch"] = json!("3"),
            4 => changed["reference"]["payload_sha256"] = json!("0".repeat(64)),
            5 => changed["request_sha256"] = json!("0".repeat(64)),
            6 => changed["definition_receipt_id"] = json!(uuid::Uuid::new_v4()),
            7 => changed["claim"]["task_id"] = json!(uuid::Uuid::new_v4()),
            8 => changed["claim"]["tenant_id"] = json!("foreign"),
            9 => changed["claim"]["claim_epoch"] = json!("18446744073709551616"),
            10 => changed["future_field"] = json!(PRIVATE),
            _ => {}
        }
        let changed = if variant == 11 {
            format!("{{\"private\":\"{PRIVATE}\"")
        } else {
            serde_json::to_string(&changed).unwrap()
        };
        f.replace_record(&changed).await;
        let error = f
            .storage
            .try_bind_claim_run(&f.scope, SERVER, &f.claim, &f.reference, REQUEST)
            .await
            .unwrap_err();
        assert!(!format!("{error:?} {error}").contains(PRIVATE));
        assert_eq!(
            f.record().await,
            changed,
            "corrupt observation was restamped"
        );
        f.replace_record(&original).await;
    }
    assert!(
        f.storage
            .try_bind_claim_run(&f.scope, SERVER, &f.claim, &f.reference, REQUEST)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn missing_advanced_observation_cannot_be_recreated_from_a_fresh_download() {
    let mut f = Fixture::new().await;
    f.advance().await;
    sqlx::query("DELETE FROM mobile_remote_run_observations")
        .execute(f.storage.pool())
        .await
        .unwrap();
    f.wire.scope = f.claim.clone();
    f.storage
        .install_claim_definitions(&f.scope, SERVER, &f.claim, &f.reference, &f.wire)
        .await
        .unwrap();
    assert!(
        f.storage
            .try_bind_claim_run(&f.scope, SERVER, &f.claim, &f.reference, REQUEST)
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_remote_run_observations")
        .fetch_one(f.storage.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn oversized_stored_observation_is_rejected_before_json_decode() {
    let f = Fixture::new().await;
    let mut connection = f.storage.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints=ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    let oversized = "OBSERVATION_PRIVATE_CANARY".repeat(1024);
    let result = sqlx::query("UPDATE mobile_remote_run_observations SET record=?1")
        .bind(&oversized)
        .execute(&mut *connection)
        .await;
    sqlx::query("PRAGMA ignore_check_constraints=OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    result.unwrap();
    drop(connection);
    let error = f
        .storage
        .try_bind_claim_run(&f.scope, SERVER, &f.claim, &f.reference, REQUEST)
        .await
        .unwrap_err();
    assert!(!format!("{error:?} {error}").contains("OBSERVATION_PRIVATE_CANARY"));
    assert_eq!(f.record().await, oversized);
}

#[tokio::test]
async fn observation_update_failure_rolls_back_the_generation_and_retry_reuses_the_run() {
    let mut f = Fixture::new().await;
    let original = f.record().await;
    f.claim.claim_epoch = 2;
    sqlx::query("CREATE TRIGGER reject_observation BEFORE UPDATE ON mobile_remote_run_observations BEGIN SELECT RAISE(ABORT,'injected observation failure'); END").execute(f.storage.pool()).await.unwrap();
    assert!(
        f.storage
            .try_bind_claim_run(&f.scope, SERVER, &f.claim, &f.reference, REQUEST)
            .await
            .is_err()
    );
    let epoch: String = sqlx::query_scalar("SELECT claim_epoch FROM mobile_remote_run_bindings")
        .fetch_one(f.storage.pool())
        .await
        .unwrap();
    assert_eq!(epoch, "1");
    assert_eq!(f.record().await, original);
    sqlx::query("DROP TRIGGER reject_observation")
        .execute(f.storage.pool())
        .await
        .unwrap();
    let rebound = f
        .storage
        .try_bind_claim_run(&f.scope, SERVER, &f.claim, &f.reference, REQUEST)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rebound.local_run_id, f.bound.local_run_id);
    assert_eq!(rebound.definitions, f.bound.definitions);
    assert_eq!(rebound.claim_epoch, 2);
}

#[tokio::test]
async fn initial_observation_insert_failure_leaves_no_partial_binding() {
    let f = Fixture::new().await;
    // Return to the installed-but-unbound crash-recovery state.
    sqlx::query("DELETE FROM mobile_remote_run_observations")
        .execute(f.storage.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM mobile_remote_run_bindings")
        .execute(f.storage.pool())
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_observation BEFORE INSERT ON mobile_remote_run_observations BEGIN SELECT RAISE(ABORT,'injected observation failure'); END").execute(f.storage.pool()).await.unwrap();
    assert!(
        f.storage
            .try_bind_claim_run(&f.scope, SERVER, &f.claim, &f.reference, REQUEST)
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_remote_run_bindings")
        .fetch_one(f.storage.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    sqlx::query("DROP TRIGGER reject_observation")
        .execute(f.storage.pool())
        .await
        .unwrap();
    let rebound = f
        .storage
        .try_bind_claim_run(&f.scope, SERVER, &f.claim, &f.reference, REQUEST)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rebound.definitions, f.bound.definitions);
}

#[tokio::test]
async fn advanced_cached_run_reopens_without_a_claim_specific_graph_or_envelope() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rebind.db");
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let mut f = Fixture::in_storage(storage).await;
    let expected = f.advance().await;
    let scope = f.scope.clone();
    let claim = f.claim.clone();
    let reference = f.reference.clone();
    f.storage.pool().close().await;
    drop(f);
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    assert!(
        storage
            .load_claim_definitions(&scope, SERVER, &claim, &reference)
            .await
            .unwrap()
            .is_none()
    );
    let rebound = storage
        .try_bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rebound, expected);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_claim_definition_receipts")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn pre_observation_binding_requires_immutable_scope_proof_and_preserves_full_epoch() {
    let mut f = Fixture::new().await;
    let original = f.claim.clone();
    sqlx::query("DELETE FROM mobile_remote_run_observations")
        .execute(f.storage.pool())
        .await
        .unwrap();
    f.claim.claim_epoch = u64::MAX;
    f.claim.worker_id = "replacement".into();
    let rebound = f
        .storage
        .try_bind_claim_run(&f.scope, SERVER, &f.claim, &f.reference, REQUEST)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rebound.local_run_id, f.bound.local_run_id);
    assert_eq!(rebound.definitions, f.bound.definitions);
    let record: Value = serde_json::from_str(&f.record().await).unwrap();
    assert_eq!(record["claim"]["claim_epoch"], json!(u64::MAX.to_string()));
    assert!(
        f.storage
            .try_bind_claim_run(&f.scope, SERVER, &original, &f.reference, REQUEST)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn independent_connections_keep_one_graph_and_monotonic_claim_observation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("concurrent-rebind.db");
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let f = Fixture::in_storage(storage).await;
    let other = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let mut second = f.claim.clone();
    second.claim_epoch = 2;
    second.worker_id = "worker-two".into();
    let mut third = f.claim.clone();
    third.claim_epoch = 3;
    third.worker_id = "worker-three".into();
    let (a, b) = tokio::join!(
        f.storage
            .try_bind_claim_run(&f.scope, SERVER, &second, &f.reference, REQUEST),
        other.try_bind_claim_run(&f.scope, SERVER, &third, &f.reference, REQUEST),
    );
    assert!(
        matches!(a, Ok(Some(_)) | Err(StorageError::Conflict(_))),
        "unexpected lower-generation race result: {a:?}"
    );
    let newest = b.unwrap().unwrap();
    assert_eq!(newest.local_run_id, f.bound.local_run_id);
    assert_eq!(newest.definitions, f.bound.definitions);
    assert_eq!(newest.claim_epoch, 3);
    let epoch: String = sqlx::query_scalar("SELECT claim_epoch FROM mobile_remote_run_bindings")
        .fetch_one(f.storage.pool())
        .await
        .unwrap();
    assert_eq!(epoch, "3");
    let observation: Value = serde_json::from_str(&f.record().await).unwrap();
    assert_eq!(observation["claim"]["claim_epoch"], json!("3"));
    assert!(
        f.storage
            .try_bind_claim_run(&f.scope, SERVER, &second, &f.reference, REQUEST)
            .await
            .is_err()
    );
    for table in [
        "mobile_claim_definition_receipts",
        "mobile_remote_run_bindings",
        "mobile_remote_run_observations",
    ] {
        let count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(f.storage.pool())
                .await
                .unwrap();
        assert_eq!(count, 1, "unexpected growth in {table}");
    }
}
