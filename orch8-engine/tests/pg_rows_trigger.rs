//! Postgres row-change trigger against a live database.
//!
//! Gated on `DATABASE_URL` (skipped with a message when absent), like the
//! storage crate's Postgres suite. Run with:
//!
//! ```text
//! DATABASE_URL=postgres://… cargo test -p orch8-engine --features postgres-rows --test pg_rows_trigger
//! ```
#![cfg(feature = "postgres-rows")]

use std::sync::Arc;
use std::time::Duration;

use orch8_engine::trigger_sources::pg_rows::{QualifiedName, RowEvent, install_sql, uninstall_sql};
use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use orch8_types::instance::TaskInstance;
use orch8_types::trigger::{TriggerDef, TriggerType};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;

async fn orch8_storage(config: Value) -> (Arc<dyn StorageBackend>, TriggerDef) {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    storage
        .create_sequence(&orch8_types::sequence::SequenceDefinition {
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
            id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t1"),
            namespace: Namespace::new("default"),
            name: "on-row".into(),
            version: 1,
            deprecated: false,
            status: orch8_types::sequence::SequenceStatus::default(),
            blocks: vec![],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();
    let now = chrono::Utc::now();
    let trigger = TriggerDef {
        slug: "orders-changed".into(),
        sequence_name: "on-row".into(),
        version: None,
        tenant_id: TenantId::unchecked("t1"),
        namespace: "default".into(),
        enabled: true,
        secret: None,
        trigger_type: TriggerType::PostgresRows,
        config,
        created_at: now,
        updated_at: now,
    };
    storage.create_trigger(&trigger).await.unwrap();
    (storage, trigger)
}

async fn instances(storage: &Arc<dyn StorageBackend>) -> Vec<TaskInstance> {
    storage
        .list_instances(
            &InstanceFilter::default(),
            &Pagination {
                offset: 0,
                limit: 1000,
                sort_ascending: true,
            },
        )
        .await
        .unwrap()
}

async fn wait_for(storage: &Arc<dyn StorageBackend>, n: usize) -> Vec<TaskInstance> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let found = instances(storage).await;
        if found.len() >= n || tokio::time::Instant::now() > deadline {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn start(
    storage: &Arc<dyn StorageBackend>,
    trigger: &TriggerDef,
) -> (
    CancellationToken,
    tokio::task::JoinHandle<Result<(), orch8_engine::error::EngineError>>,
) {
    let cancel = CancellationToken::new();
    let task = tokio::spawn(orch8_engine::trigger_sources::pg_rows::run(
        Arc::clone(storage),
        trigger.clone(),
        cancel.clone(),
    ));
    (cancel, task)
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one end-to-end scenario, step by step
async fn row_changes_start_workflows_without_loss_or_duplicates() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let schema = format!("pgrows_{}", uuid::Uuid::now_v7().simple());
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".orders (id SERIAL PRIMARY KEY, status TEXT NOT NULL)"
    ))
    .execute(&pool)
    .await
    .unwrap();

    let table = QualifiedName::parse(&format!("{schema}.orders")).unwrap();
    let outbox = QualifiedName::parse(&format!("{schema}.orch8_row_changes")).unwrap();
    let channel = format!("{schema}_chan");
    // Capture every event kind, but the trigger config only wants insert +
    // update: deletes must be filtered out by the listener.
    let sql = install_sql(
        &table,
        &[RowEvent::Insert, RowEvent::Update, RowEvent::Delete],
        &outbox,
        &channel,
    );
    sqlx::raw_sql(&sql).execute(&pool).await.unwrap();
    // Re-running the install script is idempotent.
    sqlx::raw_sql(&sql).execute(&pool).await.unwrap();

    let (storage, trigger) = orch8_storage(json!({
        "database_url": url,
        "table": format!("{schema}.orders"),
        "events": ["insert", "update"],
        "outbox_table": format!("{schema}.orch8_row_changes"),
        "channel": channel,
        "start_from": "beginning",
        "poll_interval_ms": 200,
    }))
    .await;

    // A change committed before the trigger ever ran is picked up
    // (start_from = beginning).
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".orders (status) VALUES ('new')"
    ))
    .execute(&pool)
    .await
    .unwrap();

    let (cancel, task) = start(&storage, &trigger);
    let got = wait_for(&storage, 1).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].context.data["op"], "insert");
    assert_eq!(got[0].context.data["new"]["status"], "new");
    assert_eq!(got[0].metadata["_trigger_type"], "postgres_rows");

    sqlx::query(&format!(
        "UPDATE \"{schema}\".orders SET status = 'paid' WHERE id = 1"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let got = wait_for(&storage, 2).await;
    assert_eq!(got.len(), 2);
    let update = got
        .iter()
        .find(|i| i.context.data["op"] == "update")
        .expect("update delivered");
    assert_eq!(update.context.data["old"]["status"], "new");
    assert_eq!(update.context.data["new"]["status"], "paid");

    // Engine down: changes keep accumulating in the outbox and are recovered
    // on restart (no NOTIFY is ever received for them).
    cancel.cancel();
    task.await.unwrap().unwrap();
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".orders (status) VALUES ('while-down-1'), ('while-down-2')"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "DELETE FROM \"{schema}\".orders WHERE status = 'while-down-2'"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let (cancel, task) = start(&storage, &trigger);
    let got = wait_for(&storage, 4).await;
    assert_eq!(got.len(), 4, "recovered both inserts; delete filtered out");

    // Out-of-order commit: A takes the lower outbox id but commits after B.
    // The (txid, id) cursor must not skip A.
    let mut tx_a = pool.begin().await.unwrap();
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".orders (status) VALUES ('slow-a')"
    ))
    .execute(&mut *tx_a)
    .await
    .unwrap();
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".orders (status) VALUES ('fast-b')"
    ))
    .execute(&pool)
    .await
    .unwrap();
    // B is committed but not yet eligible while A is in flight.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        instances(&storage).await.len(),
        4,
        "B must wait for A (xmin fence)"
    );
    tx_a.commit().await.unwrap();
    let got = wait_for(&storage, 6).await;
    assert_eq!(got.len(), 6, "both A and B delivered exactly once");
    let statuses: Vec<String> = got
        .iter()
        .filter_map(|i| {
            i.context.data["new"]["status"]
                .as_str()
                .map(ToString::to_string)
        })
        .collect();
    assert!(statuses.contains(&"slow-a".to_string()));
    assert!(statuses.contains(&"fast-b".to_string()));

    // No duplicates after another restart (cursor persisted).
    cancel.cancel();
    task.await.unwrap().unwrap();
    let (cancel, task) = start(&storage, &trigger);
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(instances(&storage).await.len(), 6);
    cancel.cancel();
    task.await.unwrap().unwrap();

    sqlx::raw_sql(&uninstall_sql(&table))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(&pool)
        .await
        .unwrap();
}
