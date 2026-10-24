use super::*;
use crate::credential::Credential;
use crate::node::{Advertisement, NodeCapabilities};
use orch8_storage::WorkerStore;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::continuity::RuntimeId;

async fn pump(storage: Arc<SqliteStorage>, runtime: RuntimeId) -> Arc<DelegationPump> {
    pump_at(storage, runtime, "https://unused.invalid".into()).await
}

async fn pump_at(
    storage: Arc<SqliteStorage>,
    runtime: RuntimeId,
    base: String,
) -> Arc<DelegationPump> {
    init_tables(storage.pool()).await.unwrap();
    let client = NodeClient::new_unchecked(
        base,
        Credential::new("test-key".into()),
        "device".into(),
        runtime,
        Advertisement {
            handlers: vec!["noop".into()],
            caps: NodeCapabilities::default(),
            draining: false,
        },
    );
    DelegationPump::new(
        client,
        storage.clone(),
        storage.pool().clone(),
        DelegationOptions {
            tenant_id: "tenant".into(),
            poll_interval_ms: 2000,
            ttl_secs: 600,
        },
        Arc::new(Notify::new()),
    )
    .unwrap()
}

async fn seed_open(pool: &SqlitePool, count: i64) {
    let now = chrono::Utc::now();
    for index in 0..count {
        let deadline = if index == PUMP_BATCH {
            now - chrono::Duration::seconds(1)
        } else {
            now + chrono::Duration::hours(1)
        };
        sqlx::query("INSERT INTO mobile_delegations (delegation_id, local_instance_id, kind, input, state, deadline_at, created_at, updated_at) VALUES (?1, 'instance', 'explicit', '{}', 'prepared', ?2, ?3, ?3)")
            .bind(format!("d-{index:04}")).bind(deadline.to_rfc3339()).bind(now.to_rfc3339())
            .execute(pool).await.unwrap();
    }
}

#[tokio::test]
async fn backed_off_prefix_cannot_starve_later_delegations() {
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let pump = pump(Arc::clone(&storage), RuntimeId::new()).await;
    seed_open(storage.pool(), PUMP_BATCH + 1).await;
    for index in 0..PUMP_BATCH {
        pump.gate.miss(
            &format!("d-{index:04}"),
            Instant::now(),
            Duration::from_secs(3600),
        );
    }
    pump.run_once().await;
    pump.run_once().await;
    let state: String =
        sqlx::query_scalar("SELECT state FROM mobile_delegations WHERE delegation_id = 'd-0050'")
            .fetch_one(storage.pool())
            .await
            .unwrap();
    assert_eq!(
        state, "abandoned",
        "later expired work must be examined despite a blocked oldest batch"
    );
    assert_eq!(
        pump.gate.attempts("d-0000"),
        1,
        "page rotation must retain another page's retry hint"
    );
}

#[tokio::test]
async fn open_scan_progress_survives_a_database_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("delegation-scan.sqlite");
    let path = path.to_str().unwrap();
    let runtime = RuntimeId::new();
    let storage = Arc::new(SqliteStorage::file(path).await.unwrap());
    let first = pump(Arc::clone(&storage), runtime).await;
    seed_open(storage.pool(), PUMP_BATCH + 1).await;
    assert_eq!(
        first.load_open().await.unwrap().len(),
        usize::try_from(PUMP_BATCH).unwrap()
    );
    drop(first);
    storage.pool().close().await;
    drop(storage);
    let reopened = Arc::new(SqliteStorage::file(path).await.unwrap());
    let second = pump(reopened, runtime).await;
    let page = second.load_open().await.unwrap();
    assert!(
        page.iter().any(|row| row.delegation_id == "d-0050"),
        "cold reopen must not reset the oldest-batch trap"
    );
}

async fn seed_discovery(storage: &SqliteStorage, own: RuntimeId) -> uuid::Uuid {
    let instance = uuid::Uuid::new_v4().to_string();
    let now = now_text();
    sqlx::query("INSERT INTO task_instances(id, sequence_id, tenant_id, namespace, state, created_at, updated_at) VALUES (?1, ?2, 'tenant', 'default', 'waiting', ?3, ?3)")
        .bind(&instance).bind(uuid::Uuid::new_v4().to_string()).bind(&now).execute(storage.pool()).await.unwrap();
    let elsewhere = RuntimeId::new();
    for index in 0..=PUMP_BATCH {
        let id = uuid::Uuid::from_u128(u128::try_from(index + 1).unwrap());
        let target = if index == PUMP_BATCH { elsewhere } else { own };
        sqlx::query("INSERT INTO worker_tasks(id, instance_id, block_id, handler_name, requirements, created_at) VALUES (?1, ?2, ?3, 'noop', ?4, ?5)")
            .bind(id.to_string()).bind(&instance).bind(format!("work-{index}"))
            .bind(json!({"runtime_id":target}).to_string()).bind(&now).execute(storage.pool()).await.unwrap();
    }
    uuid::Uuid::from_u128(u128::try_from(PUMP_BATCH + 1).unwrap())
}

#[tokio::test]
async fn discovery_rotates_past_work_for_this_phone() {
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let runtime = RuntimeId::new();
    let pump = pump(Arc::clone(&storage), runtime).await;
    let tail = seed_discovery(&storage, runtime).await;
    pump.discover().await.unwrap();
    assert!(
        storage
            .get_worker_task(tail)
            .await
            .unwrap()
            .unwrap()
            .worker_id
            .is_none()
    );
    pump.discover().await.unwrap();
    assert_eq!(
        storage
            .get_worker_task(tail)
            .await
            .unwrap()
            .unwrap()
            .worker_id
            .as_deref(),
        Some(PUMP_WORKER_ID)
    );
}

#[tokio::test]
async fn malformed_records_are_retained_and_do_not_block_valid_work() {
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let pump = pump(Arc::clone(&storage), RuntimeId::new()).await;
    seed_open(storage.pool(), 9).await;
    let corrupt = [
        ("input", "private-secret-invalid-json"),
        ("grant_json", "bad-json"),
        ("result", "bad-json"),
        ("runtime_kinds", "[\"unknown-runtime\"]"),
        ("kind", "unknown-kind"),
        ("claim_epoch", "-1"),
        ("parent_epoch", "-1"),
        ("deadline_at", "invalid-date"),
    ];
    for (index, (field, value)) in corrupt.into_iter().enumerate() {
        // Field names are this fixed test list; all data remains bound.
        let query = format!("UPDATE mobile_delegations SET {field} = ?1 WHERE delegation_id = ?2");
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(value)
            .bind(format!("d-{index:04}"))
            .execute(storage.pool())
            .await
            .unwrap();
    }
    let past = (chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339();
    sqlx::query("UPDATE mobile_delegations SET deadline_at = ?1 WHERE delegation_id = 'd-0008'")
        .bind(past)
        .execute(storage.pool())
        .await
        .unwrap();
    let page = pump.load_open().await.unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].delegation_id, "d-0008");
    pump.run_once().await;
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM mobile_delegations WHERE state = 'prepared'")
            .fetch_one(storage.pool())
            .await
            .unwrap();
    assert_eq!(
        pending, 8,
        "corrupt work must not be rewritten or executed with replacement defaults"
    );
    let state: String =
        sqlx::query_scalar("SELECT state FROM mobile_delegations WHERE delegation_id = 'd-0008'")
            .fetch_one(storage.pool())
            .await
            .unwrap();
    assert_eq!(state, "abandoned");
}

#[tokio::test]
async fn idle_scans_do_not_write_and_bookmark_failure_does_not_skip_work() {
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let pump = pump(Arc::clone(&storage), RuntimeId::new()).await;
    let before: i64 = sqlx::query_scalar("SELECT total_changes()")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    pump.load_open().await.unwrap();
    pump.discover().await.unwrap();
    let after: i64 = sqlx::query_scalar("SELECT total_changes()")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(before, after);
    seed_open(storage.pool(), PUMP_BATCH + 1).await;
    sqlx::query("CREATE TRIGGER fail_scan_bookmark BEFORE UPDATE ON mobile_delegation_scan_cursors BEGIN SELECT RAISE(ABORT, 'test bookmark failure'); END")
        .execute(storage.pool()).await.unwrap();
    assert!(pump.load_open().await.is_err());
    sqlx::query("DROP TRIGGER fail_scan_bookmark")
        .execute(storage.pool())
        .await
        .unwrap();
    assert_eq!(pump.load_open().await.unwrap()[0].delegation_id, "d-0000");
}

#[tokio::test]
async fn independent_pools_advance_bookmarks_without_losing_tied_keys() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("two-scanners.sqlite");
    let path = path.to_str().unwrap();
    let runtime = RuntimeId::new();
    let one = Arc::new(SqliteStorage::file(path).await.unwrap());
    let a = pump(Arc::clone(&one), runtime).await;
    seed_open(one.pool(), PUMP_BATCH * 2).await;
    let two = Arc::new(SqliteStorage::file(path).await.unwrap());
    let b = pump(two, runtime).await;
    let (first, second) = tokio::join!(a.load_open(), b.load_open());
    let mut ids: Vec<_> = first
        .unwrap()
        .into_iter()
        .chain(second.unwrap())
        .map(|row| row.delegation_id)
        .collect();
    assert_eq!(ids.len(), usize::try_from(PUMP_BATCH * 2).unwrap());
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), usize::try_from(PUMP_BATCH * 2).unwrap());
    let wrapped = a.load_open().await.unwrap();
    assert_eq!(wrapped[0].delegation_id, "d-0000");
}

#[tokio::test]
async fn scan_queries_use_ordered_partial_indexes() {
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let runtime = RuntimeId::new();
    let _pump = pump(Arc::clone(&storage), runtime).await;
    seed_open(storage.pool(), PUMP_BATCH + 1).await;
    seed_discovery(&storage, runtime).await;
    for (spec, index) in [
        (&scan::OPEN, "idx_mobile_delegation_open_scan"),
        (&scan::DISCOVERY, "idx_mobile_delegation_discovery"),
    ] {
        // Only fixed production SQL is interpolated; cursor values remain binds.
        let query = format!("EXPLAIN QUERY PLAN {}", spec.page);
        let rows = sqlx::query(sqlx::AssertSqlSafe(query))
            .bind("")
            .bind("")
            .bind(PUMP_BATCH)
            .fetch_all(storage.pool())
            .await
            .unwrap();
        let plan = rows
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(plan.contains(index), "{plan}");
        assert!(!plan.contains("USE TEMP B-TREE"), "{plan}");
    }
}

#[test]
fn retry_hints_are_bounded_and_extreme_intervals_do_not_overflow() {
    let gate = RetryGate::default();
    for index in 0..=MAX_RETRY_HINTS {
        gate.miss(
            &format!("r-{index}"),
            Instant::now(),
            Duration::from_secs(1),
        );
    }
    assert_eq!(gate.lock().len(), MAX_RETRY_HINTS);
    assert_eq!(
        retry_delay(u32::MAX, Duration::MAX, PLACEMENT_RETRY_CAP, 1.0),
        PLACEMENT_RETRY_CAP
    );
}

#[tokio::test]
async fn a_completed_poll_does_not_reinsert_a_closed_retry_hint() {
    let runtime = RuntimeId::new();
    let server = crate::test_support::spawn_control_plane(Arc::new(move |_, _, _| (200, json!({
        "status":"completed", "delegation":{"id":"d-0000", "parent_continuity_id":"continuity", "source_runtime_id":runtime, "parent_epoch":1},
        "parent_owner_runtime_id":runtime, "parent_epoch_now":1, "result":{"output":{"done":true}}
    })))).await;
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let pump = pump_at(Arc::clone(&storage), runtime, server.base.clone()).await;
    seed_open(storage.pool(), 1).await;
    sqlx::query("UPDATE mobile_delegations SET state = 'claimed', parent_epoch = 1, continuity_id = 'continuity' WHERE delegation_id = 'd-0000'")
        .execute(storage.pool()).await.unwrap();
    pump.gate.miss("d-0000", Instant::now(), Duration::ZERO);
    pump.run_once().await;
    let requests = server.requests.lock().unwrap().clone();
    let records = sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
        "SELECT state, outcome, last_error FROM mobile_delegations",
    )
    .fetch_all(storage.pool())
    .await
    .unwrap();
    assert_eq!(
        pump.stats().completed,
        1,
        "requests={requests:?}, records={records:?}"
    );
    assert_eq!(pump.gate.attempts("d-0000"), 0);
    assert_eq!(
        server.count("/continuity/delegations/d-0000?tenant_id=tenant"),
        1
    );
}

#[tokio::test]
async fn unrelated_delegation_response_does_not_resolve_local_work() {
    for wrong_parent in [false, true] {
        let runtime = RuntimeId::new();
        let server = crate::test_support::spawn_control_plane(Arc::new(move |_, _, _| (200, json!({
            "status":"completed",
            "delegation":{
                "id":if wrong_parent {"d-0000"} else {"another-delegation"},
                "parent_continuity_id":if wrong_parent {"another-parent"} else {"continuity"},
                "source_runtime_id":runtime, "parent_epoch":1
            },
            "parent_owner_runtime_id":runtime, "parent_epoch_now":1,
            "result":{"output":{"private":"unrelated-result"}}
        })))).await;
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let pump = pump_at(Arc::clone(&storage), runtime, server.base.clone()).await;
        seed_open(storage.pool(), 1).await;
        sqlx::query("UPDATE mobile_delegations SET state = 'claimed', parent_epoch = 1, continuity_id = 'continuity' WHERE delegation_id = 'd-0000'")
            .execute(storage.pool()).await.unwrap();
        pump.run_once().await;
        let (state, result): (String, Option<String>) = sqlx::query_as(
            "SELECT state, result FROM mobile_delegations WHERE delegation_id = 'd-0000'",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(
            state, "claimed",
            "unrelated output must leave the original delegation recoverable"
        );
        assert!(result.is_none());
        assert_eq!(pump.stats().completed, 0);
        assert_eq!(
            server.count("/continuity/delegations/d-0000?tenant_id=tenant"),
            1,
            "the unrelated response must actually be read"
        );
    }
}

#[tokio::test]
async fn a_claimed_record_without_an_epoch_is_not_polled_or_resolved() {
    let server = crate::test_support::spawn_control_plane(Arc::new(|_, _, _| {
        (200, json!({"status":"completed"}))
    }))
    .await;
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let pump = pump_at(Arc::clone(&storage), RuntimeId::new(), server.base.clone()).await;
    seed_open(storage.pool(), 1).await;
    sqlx::query("UPDATE mobile_delegations SET state = 'claimed', continuity_id = 'continuity' WHERE delegation_id = 'd-0000'")
        .execute(storage.pool()).await.unwrap();
    pump.run_once().await;
    assert!(server.requests.lock().unwrap().is_empty());
    let state: String =
        sqlx::query_scalar("SELECT state FROM mobile_delegations WHERE delegation_id = 'd-0000'")
            .fetch_one(storage.pool())
            .await
            .unwrap();
    assert_eq!(
        state, "claimed",
        "unknown ownership must preserve recovery evidence"
    );
}
