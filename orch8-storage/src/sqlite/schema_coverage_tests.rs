//! Coverage tests for the SQLite schema additions behind tenant partition
//! routing (078) and the push wake outbox (076/079).
//!
//! Count contract: 17 independently named unit tests.

use super::SqliteStorage;
use super::schema::{SCHEMA, SCHEMA_VERSION};

async fn store() -> SqliteStorage {
    SqliteStorage::in_memory().await.unwrap()
}

async fn table_columns(storage: &SqliteStorage, table: &str) -> Vec<String> {
    let rows: Vec<(String,)> =
        sqlx::query_as(&format!("SELECT name FROM pragma_table_info('{table}')"))
            .fetch_all(&storage.pool)
            .await
            .unwrap();
    rows.into_iter().map(|row| row.0).collect()
}

async fn index_names(storage: &SqliteStorage, table: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?")
        .bind(table)
        .fetch_all(&storage.pool)
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// Schema text invariants (what fresh databases get)
// ---------------------------------------------------------------------------

#[test]
fn coverage_schema_001_schema_declares_tenant_storage_placements() {
    assert!(SCHEMA.contains("CREATE TABLE IF NOT EXISTS tenant_storage_placements"));
}

#[test]
fn coverage_schema_002_placement_epoch_is_check_constrained_positive() {
    assert!(
        SCHEMA.contains("epoch INTEGER NOT NULL CHECK(epoch > 0)"),
        "the fencing epoch must be DB-enforced positive, got schema:\n{}",
        &SCHEMA[SCHEMA.find("tenant_storage_placements").unwrap()..]
    );
}

#[test]
fn coverage_schema_003_placement_backend_index_declared() {
    assert!(SCHEMA.contains("idx_tenant_storage_placements_backend"));
}

#[test]
fn coverage_schema_004_schema_declares_push_wake_outbox() {
    assert!(SCHEMA.contains("CREATE TABLE IF NOT EXISTS push_wake_outbox"));
}

#[test]
fn coverage_schema_005_outbox_dedupes_on_tenant_device_command() {
    assert!(
        SCHEMA.contains("UNIQUE (tenant_id, device_id, command_id)"),
        "enqueue idempotency relies on this unique key"
    );
}

#[test]
fn coverage_schema_006_outbox_carries_governance_columns() {
    for column in ["execution_id", "topic", "collapse_key", "superseded_by"] {
        assert!(
            SCHEMA.contains(column),
            "push_wake_outbox is missing governance column {column}"
        );
    }
}

#[test]
fn coverage_schema_007_due_index_is_partial_on_pending() {
    assert!(
        SCHEMA.contains("ON push_wake_outbox(next_attempt_at) WHERE status = 'pending'"),
        "the due index must stay partial so terminal rows don't bloat it"
    );
}

#[test]
fn coverage_schema_008_collapse_index_covers_pending_lookup() {
    assert!(SCHEMA.contains("idx_push_wake_collapse_pending"));
    assert!(SCHEMA.contains("ON push_wake_outbox(tenant_id,device_id,collapse_key,created_at)"));
}

#[test]
fn coverage_schema_009_sqlite_schema_version_is_current() {
    assert_eq!(SCHEMA_VERSION, 39);
}

#[test]
fn coverage_schema_010_crate_storage_schema_version_is_current() {
    assert_eq!(crate::STORAGE_SCHEMA_VERSION, 78);
}

// ---------------------------------------------------------------------------
// Live in-memory database: the DDL actually does what the code assumes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coverage_schema_011_placements_table_has_expected_columns() {
    let storage = store().await;
    let columns = table_columns(&storage, "tenant_storage_placements").await;
    assert_eq!(
        columns,
        vec!["tenant_id", "backend_id", "epoch", "updated_at"]
    );
}

#[tokio::test]
async fn coverage_schema_012_placement_epoch_check_is_enforced() {
    let storage = store().await;
    let result = sqlx::query(
        "INSERT INTO tenant_storage_placements (tenant_id, backend_id, epoch, updated_at) VALUES ('t', 'b', 0, 'now')",
    )
    .execute(&storage.pool)
    .await;
    assert!(result.is_err(), "epoch 0 must violate the CHECK constraint");
}

#[tokio::test]
async fn coverage_schema_013_outbox_unique_key_is_enforced() {
    let storage = store().await;
    let insert = "INSERT INTO push_wake_outbox (id, tenant_id, device_id, command_id, created_at) VALUES ('id-1', 't', 'd', 'c', 'now')";
    sqlx::query(insert).execute(&storage.pool).await.unwrap();
    let duplicate = sqlx::query(
        "INSERT INTO push_wake_outbox (id, tenant_id, device_id, command_id, created_at) VALUES ('id-2', 't', 'd', 'c', 'now')",
    )
    .execute(&storage.pool)
    .await;
    assert!(
        duplicate.is_err(),
        "a second row for (tenant, device, command) must be rejected"
    );
}

#[tokio::test]
async fn coverage_schema_014_outbox_table_has_governance_columns() {
    let storage = store().await;
    let columns = table_columns(&storage, "push_wake_outbox").await;
    for column in [
        "id",
        "tenant_id",
        "device_id",
        "command_id",
        "attempts",
        "status",
        "next_attempt_at",
        "lease_until",
        "last_error",
        "terminal_reason",
        "delivered_at",
        "command_acked_at",
        "execution_id",
        "topic",
        "collapse_key",
        "superseded_by",
        "created_at",
    ] {
        assert!(
            columns.contains(&column.to_string()),
            "missing column {column}"
        );
    }
}

#[tokio::test]
async fn coverage_schema_015_outbox_indexes_exist() {
    let storage = store().await;
    let indexes = index_names(&storage, "push_wake_outbox").await;
    assert!(indexes.contains(&"idx_push_wake_due".to_string()));
    assert!(indexes.contains(&"idx_push_wake_collapse_pending".to_string()));
}

#[tokio::test]
async fn coverage_schema_016_placement_backend_index_exists() {
    let storage = store().await;
    let indexes = index_names(&storage, "tenant_storage_placements").await;
    assert!(indexes.contains(&"idx_tenant_storage_placements_backend".to_string()));
}

#[tokio::test]
async fn coverage_schema_017_outbox_status_defaults_to_pending() {
    // The drain loop claims rows WHERE status='pending'; a bare enqueue insert
    // (no explicit status) must land in that state.
    let storage = store().await;
    sqlx::query(
        "INSERT INTO push_wake_outbox (id, tenant_id, device_id, command_id, created_at) VALUES ('id-1', 't', 'd', 'c', 'now')",
    )
    .execute(&storage.pool)
    .await
    .unwrap();
    let status: String = sqlx::query_scalar("SELECT status FROM push_wake_outbox WHERE id='id-1'")
        .fetch_one(&storage.pool)
        .await
        .unwrap();
    assert_eq!(status, "pending");
}
