use super::*;
use serde_json::Value;
use std::sync::Arc;
const SERVER: &str = "https://engine.example/";
const RECEIPT: &str = "30000000-0000-4000-8000-000000000001";
fn vector() -> Value {
    serde_json::from_str(include_str!(
        "../../../../../contracts/vectors/native-definition-v1.json"
    ))
    .unwrap()
}
struct Fixture {
    storage: MobileStorage,
    scope: CatalogScope,
    wire: DefinitionWireEnvelope,
    claim: DefinitionReadScope,
    reference: DefinitionBundleReference,
    root: SequenceId,
}
async fn fixture(path: Option<&str>) -> Fixture {
    let sqlite = match path {
        Some(path) => SqliteStorage::file_mobile(path).await.unwrap(),
        None => SqliteStorage::in_memory().await.unwrap(),
    };
    let storage = MobileStorage::new(Arc::new(sqlite));
    let v = vector();
    sqlx::query("INSERT INTO mobile_accounts(id,account_key,principal_issuer,principal_subject,epoch,active) VALUES (?1,'alice','issuer','alice',1,1)").bind(v["account_id"].as_str().unwrap()).execute(storage.pool()).await.unwrap();
    sqlx::query("INSERT INTO mobile_catalogs(id,account_id,catalog_key,source,signing_authority) VALUES (?1,?2,'catalog','source','authority')").bind(v["catalog_id"].as_str().unwrap()).bind(v["account_id"].as_str().unwrap()).execute(storage.pool()).await.unwrap();
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    let scope = storage
        .open_catalog_scope(&account, "catalog", "source", "authority")
        .await
        .unwrap();
    let mut query = sqlx::QueryBuilder::new(
        "INSERT INTO sequences(id,tenant_id,namespace,name,version,deprecated,status,blocks,interceptors,input_schema,sla,on_failure,on_cancel,placement,created_at,sub_tenant,embed) VALUES (",
    );
    let mut values = query.separated(",");
    for col in v["columns"].as_array().unwrap() {
        values.push_bind(col["value"].as_str());
    }
    values.push_unseparated(")");
    query.build().execute(storage.pool()).await.unwrap();
    let wire: DefinitionWireEnvelope = serde_json::from_value(v["envelope"].clone()).unwrap();
    let claim = wire.scope.clone();
    let reference = wire.reference.clone();
    let root = column(&v, "id")
        .parse::<uuid::Uuid>()
        .map(SequenceId::from_uuid)
        .unwrap();
    sqlx::query("INSERT INTO mobile_claim_definition_receipts(id,account_id,catalog_id,owner_epoch,server,remote_tenant,task_id,worker_id,claim_epoch,reference_json,namespace,root_id,definition_bytes) VALUES (?1,?2,?3,1,?4,?5,?6,?7,?8,?9,?10,?11,?12)")
        .bind(RECEIPT).bind(scope.account_id()).bind(scope.catalog_id()).bind(SERVER).bind(claim.tenant_id.as_str()).bind(claim.task_id.to_string()).bind(&claim.worker_id).bind(claim.claim_epoch.to_string()).bind(serde_json::to_string(&reference).unwrap()).bind(column(&v,"namespace")).bind(root.to_string()).bind(v["native_bytes"].as_i64().unwrap()).execute(storage.pool()).await.unwrap();
    sqlx::query(
        "INSERT INTO mobile_claim_definition_manifests(receipt_id,manifest) VALUES (?1,?2)",
    )
    .bind(RECEIPT)
    .bind(v["source_manifest_json"].as_str().unwrap())
    .execute(storage.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO mobile_claim_definition_entries(receipt_id,source_sha256,sequence_id,local_sha256) VALUES (?1,?2,?3,?4)").bind(RECEIPT).bind(&reference.sequence_sha256).bind(root.to_string()).bind(v["native_sha256"].as_str().unwrap()).execute(storage.pool()).await.unwrap();
    sqlx::query("INSERT INTO mobile_claim_definition_codecs(receipt_id,version) VALUES (?1,1)")
        .bind(RECEIPT)
        .execute(storage.pool())
        .await
        .unwrap();
    Fixture {
        storage,
        scope,
        wire,
        claim,
        reference,
        root,
    }
}
fn column<'a>(v: &'a Value, name: &str) -> &'a str {
    v["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == name)
        .unwrap()["value"]
        .as_str()
        .unwrap()
}
#[tokio::test]
async fn legacy_defaulted_shape_reopens_without_rehashing_todays_typed_model() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    let f = fixture(Some(path.to_str().unwrap())).await;
    let verified = f
        .wire
        .verify(&f.claim, &f.reference, DefinitionGraphLimits::default())
        .unwrap();
    let orch8_types::sequence::BlockDefinition::Step(step) = &verified.root().definition.blocks[0]
    else {
        panic!("step")
    };
    assert!(step.cancellable);
    let before: String = sqlx::query_scalar("SELECT blocks FROM sequences WHERE id=?1")
        .bind(f.root.to_string())
        .fetch_one(f.storage.pool())
        .await
        .unwrap();
    assert!(!before.contains("cancellable"));
    f.storage.pool().close().await;
    drop(f.storage);
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let installed = storage
        .load_claim_definitions(&f.scope, SERVER, &f.claim, &f.reference)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(installed.root_id, f.root);
    let after: String = sqlx::query_scalar("SELECT blocks FROM sequences WHERE id=?1")
        .bind(f.root.to_string())
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(before, after);
}

#[tokio::test]
async fn each_persisted_execution_field_is_checked_before_typed_json_parsing() {
    let f = fixture(None).await;
    let v = vector();
    for (name, value) in [
        ("tenant_id", "changed"),
        ("namespace", "changed"),
        ("name", "changed"),
        ("version", "2"),
        ("deprecated", "1"),
        ("status", "draft"),
        ("blocks", "MALFORMED_PRIVATE_CANARY"),
        ("interceptors", "MALFORMED_PRIVATE_CANARY"),
        ("input_schema", ""),
        ("sla", "MALFORMED_PRIVATE_CANARY"),
        ("on_failure", "MALFORMED_PRIVATE_CANARY"),
        ("on_cancel", "MALFORMED_PRIVATE_CANARY"),
        ("placement", "MALFORMED_PRIVATE_CANARY"),
        ("created_at", "2026-10-29 00:00:00"),
        ("sub_tenant", "foreign"),
        ("embed", "MALFORMED_PRIVATE_CANARY"),
    ] {
        // Only fixed test column names are interpolated.
        let statement = format!("UPDATE sequences SET {name}=?1 WHERE id=?2");
        sqlx::query(sqlx::AssertSqlSafe(statement.clone()))
            .bind(value)
            .bind(f.root.to_string())
            .execute(f.storage.pool())
            .await
            .unwrap();
        let error = f
            .storage
            .load_claim_definitions(&f.scope, SERVER, &f.claim, &f.reference)
            .await
            .unwrap_err();
        assert!(
            matches!(error, StorageError::Conflict(_)),
            "{name}: {error}"
        );
        assert!(!format!("{error:?}").contains("MALFORMED_PRIVATE_CANARY"));
        let original = v["columns"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == name)
            .unwrap()["value"]
            .as_str();
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .bind(original)
            .bind(f.root.to_string())
            .execute(f.storage.pool())
            .await
            .unwrap();
    }
    let installed = f
        .storage
        .load_claim_definitions(&f.scope, SERVER, &f.claim, &f.reference)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(installed.root_id, f.root);
    let digest: String = sqlx::query_scalar(
        "SELECT local_sha256 FROM mobile_claim_definition_entries WHERE receipt_id=?1",
    )
    .bind(RECEIPT)
    .fetch_one(f.storage.pool())
    .await
    .unwrap();
    assert_eq!(digest, v["native_sha256"].as_str().unwrap());
}
#[tokio::test]
async fn missing_or_future_native_codec_never_restamps_retained_rows() {
    let f = fixture(None).await;
    let v = vector();
    sqlx::query("UPDATE mobile_claim_definition_codecs SET version=2 WHERE receipt_id=?1")
        .bind(RECEIPT)
        .execute(f.storage.pool())
        .await
        .unwrap();
    assert!(
        f.storage
            .load_claim_definitions(&f.scope, SERVER, &f.claim, &f.reference)
            .await
            .is_err()
    );
    assert!(
        f.storage
            .install_claim_definitions(&f.scope, SERVER, &f.claim, &f.reference, &f.wire)
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM mobile_claim_definition_codecs WHERE receipt_id=?1")
        .bind(RECEIPT)
        .execute(f.storage.pool())
        .await
        .unwrap();
    assert!(
        f.storage
            .load_claim_definitions(&f.scope, SERVER, &f.claim, &f.reference)
            .await
            .is_err()
    );
    assert!(
        f.storage
            .install_claim_definitions(&f.scope, SERVER, &f.claim, &f.reference, &f.wire)
            .await
            .is_err()
    );
    let hash: String = sqlx::query_scalar(
        "SELECT local_sha256 FROM mobile_claim_definition_entries WHERE receipt_id=?1",
    )
    .bind(RECEIPT)
    .fetch_one(f.storage.pool())
    .await
    .unwrap();
    assert_eq!(hash, v["native_sha256"].as_str().unwrap());
    let bytes: i64 = sqlx::query_scalar(
        "SELECT definition_bytes FROM mobile_claim_definition_receipts WHERE id=?1",
    )
    .bind(RECEIPT)
    .fetch_one(f.storage.pool())
    .await
    .unwrap();
    assert_eq!(bytes, v["native_bytes"].as_i64().unwrap());
}
#[tokio::test]
async fn oversized_corrupt_native_json_is_rejected_before_fetch_and_decode() {
    let f = fixture(None).await;
    sqlx::query("UPDATE sequences SET blocks=zeroblob(8388609) WHERE id=?1")
        .bind(f.root.to_string())
        .execute(f.storage.pool())
        .await
        .unwrap();
    let error = f
        .storage
        .load_claim_definitions(&f.scope, SERVER, &f.claim, &f.reference)
        .await
        .unwrap_err();
    assert!(matches!(error, StorageError::Conflict(_)));
    assert!(error.to_string().contains("oversized"));
}
#[tokio::test]
async fn unconsumed_editor_columns_do_not_change_the_versioned_projection() {
    let f = fixture(None).await;
    sqlx::query(
        "ALTER TABLE sequences ADD COLUMN future_editor_note TEXT NOT NULL DEFAULT 'metadata'",
    )
    .execute(f.storage.pool())
    .await
    .unwrap();
    assert_eq!(
        f.storage
            .load_claim_definitions(&f.scope, SERVER, &f.claim, &f.reference)
            .await
            .unwrap()
            .unwrap()
            .root_id,
        f.root
    );
}
