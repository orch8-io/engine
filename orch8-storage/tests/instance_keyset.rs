//! `InstanceStore::list_instances_keyset` contract, run against SQLite and
//! (when `DATABASE_URL` is set) Postgres.

use chrono::Utc;
use orch8_storage::StorageBackend;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::context::ExecutionContext;
use orch8_types::filter::InstanceFilter;
use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{SequenceDefinition, SequenceStatus};
use serde_json::json;

fn instance(tenant: &str, seq: SequenceId, meta: serde_json::Value) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq,
        tenant_id: TenantId::unchecked(tenant),
        namespace: Namespace::new("default"),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now),
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: meta,
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    }
}

async fn exercise(storage: &dyn StorageBackend) {
    let tenant = format!("keyset-{}", uuid::Uuid::now_v7());
    let seq = SequenceDefinition {
        schema: None,
        schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked(tenant.clone()),
        namespace: Namespace::new("default"),
        name: "keyset".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&seq).await.unwrap();

    let mut ids = Vec::new();
    for i in 0..5 {
        let inst = instance(
            &tenant,
            seq.id,
            json!({"_job": {"v": 1, "handler": "a", "n": i}}),
        );
        ids.push(inst.id);
        storage.create_instance(&inst).await.unwrap();
    }
    // Non-matching rows in the same tenant and in another tenant.
    storage
        .create_instance(&instance(&tenant, seq.id, json!({"other": true})))
        .await
        .unwrap();

    let filter = InstanceFilter {
        tenant_id: Some(TenantId::unchecked(tenant.clone())),
        metadata_filter: Some(json!({"_job": {"handler": "a"}})),
        ..InstanceFilter::default()
    };
    let mut seen = Vec::new();
    let mut before = None;
    loop {
        let page = storage
            .list_instances_keyset(&filter, before, 2)
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        before = page.last().map(|i| i.id);
        seen.extend(page.into_iter().map(|i| i.id));
    }
    let mut expected = ids.clone();
    expected.reverse();
    assert_eq!(seen, expected, "newest first, no gaps, no duplicates");

    // State filter composes with the keyset predicate.
    storage
        .update_instance_state(ids[4], InstanceState::Completed, None)
        .await
        .unwrap();
    let completed = storage
        .list_instances_keyset(
            &InstanceFilter {
                states: Some(vec![InstanceState::Completed]),
                ..filter.clone()
            },
            None,
            10,
        )
        .await
        .unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].id, ids[4]);
}

#[tokio::test]
async fn sqlite_keyset_pagination() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    exercise(&storage).await;
}

#[tokio::test]
async fn postgres_keyset_pagination() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };
    let storage = orch8_storage::postgres::PostgresStorage::new(&url, 5, None)
        .await
        .expect("connect to DATABASE_URL");
    storage.run_migrations().await.expect("run migrations");
    exercise(&storage).await;
}
