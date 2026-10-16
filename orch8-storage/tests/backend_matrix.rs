//! The public core conformance suite on every backend, plus self-tests of
//! the `backend_test!` harness (`orch8_storage::testing::backends`).

use chrono::Utc;
use serde_json::json;

use orch8_storage::backend_test;
use orch8_storage::conformance::run_core_conformance;
use orch8_storage::testing::backends::{BackendKind, TestBackend};
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
use orch8_types::instance::InstanceState;

backend_test!(
    async fn public_core_conformance_passes(b) {
        let report = run_core_conformance(b.storage.as_ref())
            .await
            .unwrap_or_else(|e| panic!("{} violates the storage contract: {e}", b.name()));

        assert_eq!(report.checks.len(), 9);
        let evidence = b
            .storage
            .get_instance(report.instance_id)
            .await
            .unwrap()
            .expect("conformance leaves its instance as evidence");
        assert_eq!(evidence.state, InstanceState::Cancelled);
    }
);

/// A due instance in one test's backend is invisible to a global claim in
/// another's: the isolation global sweeps rely on.
async fn assert_claims_are_isolated(kind: BackendKind) {
    let Some(a) = TestBackend::open(kind).await else {
        return;
    };
    let Some(b) = TestBackend::open(kind).await else {
        return;
    };
    let seq = SequenceBuilder::new("t").step("s", "noop").build();
    a.storage.create_sequence(&seq).await.unwrap();
    let inst = InstanceBuilder::for_sequence(&seq).due_now().build();
    a.storage.create_instance(&inst).await.unwrap();

    let claimed_by_b = b
        .storage
        .claim_due_instances(Utc::now(), 100, 0)
        .await
        .unwrap();
    let claimed_by_a = a
        .storage
        .claim_due_instances(Utc::now(), 100, 0)
        .await
        .unwrap();

    assert!(
        claimed_by_b.is_empty(),
        "{}: leaked across backends",
        kind.name()
    );
    assert_eq!(claimed_by_a.len(), 1);
}

#[tokio::test]
async fn sqlite_backends_are_isolated_from_each_other() {
    assert_claims_are_isolated(BackendKind::Sqlite).await;
}

#[tokio::test]
async fn postgres_backends_get_isolated_schemas() {
    assert_claims_are_isolated(BackendKind::Postgres).await;
}

#[tokio::test]
async fn postgres_schema_is_dropped_with_the_backend() {
    let Some(b) = TestBackend::postgres().await else {
        return;
    };
    let schema = b.pg_schema().expect("postgres leg has a schema").to_owned();
    let url = orch8_storage::testing::backends::postgres_url().unwrap();

    drop(b);

    let admin = sqlx::PgPool::connect(&url).await.unwrap();
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
    )
    .bind(&schema)
    .fetch_one(&admin)
    .await
    .unwrap();
    assert!(!exists, "schema {schema} must be dropped");
}

#[tokio::test]
async fn postgres_raw_pool_sees_the_test_schema() {
    let Some(b) = TestBackend::postgres().await else {
        return;
    };
    let seq = SequenceBuilder::new("t").step("s", "noop").build();
    b.storage.create_sequence(&seq).await.unwrap();

    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sequences")
        .fetch_one(b.pg_pool())
        .await
        .unwrap();

    assert_eq!(rows, 1);
}

backend_test!(
    kinds = [encrypted_sqlite],
    async fn encrypted_leg_stores_context_data_as_ciphertext(b) {
        let seq = SequenceBuilder::new("t").step("s", "noop").build();
        b.storage.create_sequence(&seq).await.unwrap();
        let inst = InstanceBuilder::for_sequence(&seq)
            .data(json!({"secret": "plaintext-marker"}))
            .build();
        b.storage.create_instance(&inst).await.unwrap();

        let raw: String = sqlx::query_scalar("SELECT context FROM task_instances WHERE id = ?1")
            .bind(inst.id.to_string())
            .fetch_one(b.sqlite_pool())
            .await
            .unwrap();
        let read_back = b.storage.get_instance(inst.id).await.unwrap().unwrap();

        assert!(!raw.contains("plaintext-marker"), "row holds plaintext: {raw}");
        assert_eq!(read_back.context.data["secret"], "plaintext-marker");
    }
);
