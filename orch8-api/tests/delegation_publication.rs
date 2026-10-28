//! Grant redemption, mailbox exposure and recovery evidence are one transaction.
use chrono::Utc;
use orch8_api::test_harness::{BackendTestServer, spawn_test_server_on};
use orch8_storage::{ContinuityStore, InstanceStore};
use orch8_storage::{
    StorageBackend,
    testing::{
        backends::{RawPool, TestBackend},
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    continuity::{
        ContinuationGrantId, ContinuationGrantState, ContinuityExecution, ContinuityId,
        ExecutionEpoch, OwnershipState, RuntimeCapabilities, RuntimeId,
    },
    filter::Pagination,
    ids::{InstanceId, SequenceId},
    instance::InstanceState,
    worker_filter::WorkerTaskFilter,
};
use serde_json::{Value, json};
use uuid::Uuid;
const ROOT: &str = "delegation-publication-root";
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Hosted,
    Remote,
    Inline,
}
struct Fixture {
    owner: ContinuityExecution,
    request: Value,
    grant_id: ContinuationGrantId,
    mode: Mode,
}
impl Fixture {
    fn delegation_id(&self) -> Uuid {
        serde_json::from_value(self.request["delegation"]["id"].clone()).unwrap()
    }
    fn anchor(&self) -> InstanceId {
        if self.mode == Mode::Hosted {
            self.owner.current_instance_id
        } else {
            InstanceId::from_uuid(self.delegation_id())
        }
    }
    fn sequence_id(&self) -> SequenceId {
        serde_json::from_value(self.request["delegation"]["sub_sequence_id"].clone()).unwrap()
    }
}
async fn fixture(s: &dyn StorageBackend, server: &BackendTestServer, mode: Mode) -> Fixture {
    let sequence = SequenceBuilder::new(&format!("delegate-publication-{}", Uuid::now_v7()))
        .step("gate", "noop")
        .build();
    s.create_sequence(&sequence).await.unwrap();
    let parent = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Waiting)
        .build();
    if mode == Mode::Hosted {
        s.create_instance(&parent).await.unwrap();
    }
    let now = Utc::now();
    let source = RuntimeId::new();
    let destination = RuntimeId::new();
    let owner = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: parent.tenant_id.clone(),
        current_instance_id: parent.id,
        owner_runtime_id: source,
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: now,
    };
    s.create_continuity_execution(&owner).await.unwrap();
    for runtime in [source, destination] {
        let caps:RuntimeCapabilities=serde_json::from_value(json!({"runtime_id":runtime,"kind":"server","trust":"registered","handlers":["noop","orch8.delegation"],"observed_at":now,"expires_at":now+chrono::Duration::minutes(2)})).unwrap();
        s.upsert_runtime_capabilities(&owner.tenant_id, &caps)
            .await
            .unwrap();
    }
    let sub = SequenceBuilder::new(owner.tenant_id.as_str())
        .step("work", "noop")
        .build();
    let sub_id = if mode == Mode::Inline {
        SequenceId::from_uuid(orch8_engine::delegation::step_sequence_id(
            owner.tenant_id.as_str(),
            "noop",
            "work",
        ))
    } else {
        s.create_sequence(&sub).await.unwrap();
        sub.id
    };
    let grant=reqwest::Client::new().post(format!("{}/continuity/grants",server.v1_url())).header("x-api-key",ROOT)
        .json(&json!({"tenant_id":owner.tenant_id,"continuity_id":owner.continuity_id,"destination_runtime_id":destination,"allowed_actions":["accept"],"ttl_seconds":300})).send().await.unwrap();
    assert!(
        grant.status().is_success(),
        "{}",
        grant.text().await.unwrap()
    );
    let grant: Value = grant.json().await.unwrap();
    let grant_id = serde_json::from_value(grant["signed_grant"]["grant"]["id"].clone()).unwrap();
    let mut request = json!({"tenant_id":owner.tenant_id,"delegation":{"id":Uuid::now_v7(),"tenant_id":owner.tenant_id,"parent_continuity_id":owner.continuity_id,"parent_epoch":owner.epoch,"source_runtime_id":source,"destination_runtime_id":destination,"sub_sequence_id":sub_id,"grant_id":grant_id,"expires_at":now+chrono::Duration::seconds(120)},"signed_grant":grant["signed_grant"],"token":grant["token"],"input":{"private":"DELEGATION_INPUT_CANARY"}});
    if mode == Mode::Inline {
        request["step"] = json!({"handler":"noop","block_id":"work"});
    }
    Fixture {
        owner,
        request,
        grant_id,
        mode,
    }
}
async fn claim(server: &BackendTestServer, body: &Value) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}/continuity/delegations/claim", server.v1_url()))
        .header("x-api-key", ROOT)
        .json(body)
        .send()
        .await
        .unwrap()
}
async fn accepted(server: &BackendTestServer, body: &Value) -> Value {
    let response = claim(server, body).await;
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    response.json().await.unwrap()
}
async fn sql(b: &TestBackend, statement: &str) {
    match &b.raw {
        RawPool::Sqlite(pool) => {
            sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
                .execute(pool)
                .await
                .unwrap();
        }
        RawPool::Postgres(pool) => {
            sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
                .execute(pool)
                .await
                .unwrap();
        }
    }
}
async fn fault(b: &TestBackend, table: &str, suppress: bool, install: bool) {
    assert!(
        [
            "worker_tasks",
            "provenance_entries",
            "effect_receipts",
            "delegation_publications",
            "task_instances",
            "continuity_executions",
            "continuity_locations",
            "sequences",
            "continuation_grants"
        ]
        .contains(&table)
    );
    let event = if table == "continuation_grants" {
        "UPDATE"
    } else {
        "INSERT"
    };
    let statement = match (&b.raw, install) {
        (RawPool::Sqlite(_), true) => format!(
            "CREATE TRIGGER reject_delegation_publish BEFORE {event} ON {table} BEGIN SELECT RAISE({}); END;",
            if suppress {
                "IGNORE"
            } else {
                "ABORT,'injected publication failure'"
            }
        ),
        (RawPool::Postgres(_), true) => format!(
            "CREATE FUNCTION reject_delegation_publish() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN {} END $$; CREATE TRIGGER reject_delegation_publish BEFORE {event} ON {table} FOR EACH ROW EXECUTE FUNCTION reject_delegation_publish();",
            if suppress {
                "RETURN NULL;"
            } else {
                "RAISE EXCEPTION 'injected publication failure';"
            }
        ),
        (RawPool::Sqlite(_), false) => "DROP TRIGGER reject_delegation_publish".into(),
        (RawPool::Postgres(_), false) => format!(
            "DROP TRIGGER reject_delegation_publish ON {table}; DROP FUNCTION reject_delegation_publish();"
        ),
    };
    sql(b, &statement).await;
}
async fn assert_empty(s: &dyn StorageBackend, f: &Fixture) {
    assert_eq!(
        s.get_continuation_grant(&f.owner.tenant_id, f.grant_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        ContinuationGrantState::Active,
        "publication failure spent the only grant"
    );
    assert!(
        s.list_worker_tasks(
            &WorkerTaskFilter {
                instance_id: Some(f.anchor()),
                ..Default::default()
            },
            &Pagination::default()
        )
        .await
        .unwrap()
        .is_empty()
    );
    assert!(
        s.list_effect_receipts(&f.owner.tenant_id, f.owner.continuity_id, 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        s.get_delegation_publication(&f.owner.tenant_id, f.delegation_id())
            .await
            .unwrap()
            .is_none()
    );
    if f.mode != Mode::Hosted {
        assert!(
            s.get_instance(f.anchor()).await.unwrap().is_none(),
            "failed publication leaked a proxy"
        );
        assert!(
            s.get_continuity_execution_by_instance(&f.owner.tenant_id, f.anchor())
                .await
                .unwrap()
                .is_none(),
            "failed publication leaked proxy ownership"
        );
    }
    if f.mode == Mode::Inline {
        assert!(
            s.get_sequence(f.sequence_id()).await.unwrap().is_none(),
            "failed publication leaked an inline definition"
        );
    }
}
async fn failure_matrix(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for mode in [Mode::Hosted, Mode::Remote, Mode::Inline] {
        let mut tables = vec![
            ("worker_tasks", false),
            ("effect_receipts", false),
            ("provenance_entries", false),
            ("provenance_entries", true),
            ("delegation_publications", false),
            ("continuation_grants", true),
        ];
        if mode != Mode::Hosted {
            tables.extend([
                ("task_instances", false),
                ("continuity_executions", false),
                ("continuity_locations", false),
            ]);
        }
        if mode == Mode::Inline {
            tables.push(("sequences", false));
        }
        for (table, suppress) in tables {
            let f = fixture(b.storage.as_ref(), &server, mode).await;
            let before = b
                .storage
                .list_provenance(&f.owner.tenant_id, f.owner.continuity_id, 100)
                .await
                .unwrap();
            fault(&b, table, suppress, true).await;
            let response = claim(&server, &f.request).await;
            fault(&b, table, false, false).await;
            assert!(
                !response.status().is_success(),
                "{mode:?}: failure at {table} was ignored"
            );
            assert_empty(b.storage.as_ref(), &f).await;
            assert_eq!(
                b.storage
                    .list_provenance(&f.owner.tenant_id, f.owner.continuity_id, 100)
                    .await
                    .unwrap(),
                before
            );
            let first = accepted(&server, &f.request).await;
            assert_eq!(
                accepted(&server, &f.request).await,
                first,
                "lost-response retry changed identity"
            );
            let mut changed = f.request.clone();
            changed["input"] = json!({"private":"DIFFERENT_INPUT"});
            assert_eq!(
                claim(&server, &changed).await.status(),
                reqwest::StatusCode::CONFLICT
            );
        }
    }
}
orch8_storage::backend_test!(async fn failed_publication_keeps_the_grant_and_retry_is_idempotent(b){failure_matrix(b).await;});

async fn concurrent_publication(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Inline).await;
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..12 {
        let url = format!("{}/continuity/delegations/claim", server.v1_url());
        let body = f.request.clone();
        jobs.spawn(async move {
            let response = reqwest::Client::new()
                .post(url)
                .header("x-api-key", ROOT)
                .json(&body)
                .send()
                .await
                .unwrap();
            assert!(
                response.status().is_success(),
                "{}",
                response.text().await.unwrap()
            );
            response.json::<Value>().await.unwrap()
        });
    }
    let mut replies = Vec::new();
    while let Some(reply) = jobs.join_next().await {
        replies.push(reply.unwrap());
    }
    assert!(replies.iter().all(|reply| reply == &replies[0]));
    let tasks = b
        .storage
        .list_worker_tasks(
            &WorkerTaskFilter {
                instance_id: Some(f.anchor()),
                ..Default::default()
            },
            &Pagination::default(),
        )
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    let scope = b
        .storage
        .get_continuity_execution_by_instance(&f.owner.tenant_id, f.anchor())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        b.storage
            .list_effect_receipts(&f.owner.tenant_id, scope.continuity_id, 100)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        b.storage
            .list_provenance(&f.owner.tenant_id, f.owner.continuity_id, 100)
            .await
            .unwrap()
            .iter()
            .filter(|entry| entry.kind == "device_delegation")
            .count(),
        1
    );
}
orch8_storage::backend_test!(async fn concurrent_identical_publications_share_one_receipt(b){concurrent_publication(b).await;});

async fn cold_retry(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for mode in [Mode::Hosted, Mode::Remote, Mode::Inline] {
        let f = fixture(b.storage.as_ref(), &server, mode).await;
        let first = accepted(&server, &f.request).await;
        match &b.raw {
            RawPool::Postgres(pool) => {
                sqlx::query("DELETE FROM task_instances WHERE id=$1")
                    .bind(f.anchor().into_uuid())
                    .execute(pool)
                    .await
                    .unwrap();
            }
            RawPool::Sqlite(pool) => {
                sqlx::query("DELETE FROM task_instances WHERE id=?1")
                    .bind(f.anchor().to_string())
                    .execute(pool)
                    .await
                    .unwrap();
            }
        }
        let mut next = f.owner.clone();
        next.epoch = next.epoch.checked_next().unwrap();
        next.owner_runtime_id = RuntimeId::new();
        next.current_instance_id = InstanceId::new();
        assert!(
            b.storage
                .cas_continuity_owner(
                    &f.owner.tenant_id,
                    f.owner.continuity_id,
                    f.owner.epoch,
                    f.owner.owner_runtime_id,
                    &next
                )
                .await
                .unwrap()
        );
        assert_eq!(
            accepted(&server, &f.request).await,
            first,
            "cold retry depended on current ownership/native rows"
        );
        assert!(
            b.storage.get_instance(f.anchor()).await.unwrap().is_none(),
            "cold retry recreated native work"
        );
        let task: Uuid = serde_json::from_value(first["mailbox_task_id"].clone()).unwrap();
        assert!(b.storage.get_worker_task(task).await.unwrap().is_none());
    }
}
orch8_storage::backend_test!(async fn publication_receipt_survives_hot_native_cleanup_and_owner_change(b){cold_retry(b).await;});

async fn inline_requires_live_grant(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Inline).await;
    let mut revoked = b
        .storage
        .get_continuation_grant(&f.owner.tenant_id, f.grant_id)
        .await
        .unwrap()
        .unwrap();
    revoked.state = ContinuationGrantState::Revoked;
    assert!(
        b.storage
            .cas_continuation_grant_state(
                &f.owner.tenant_id,
                f.grant_id,
                ContinuationGrantState::Active,
                &revoked
            )
            .await
            .unwrap()
    );
    assert_eq!(
        claim(&server, &f.request).await.status(),
        reqwest::StatusCode::CONFLICT
    );
    assert!(
        b.storage
            .get_sequence(f.sequence_id())
            .await
            .unwrap()
            .is_none(),
        "a revoked authentic grant authored an inline sequence"
    );
    assert!(b.storage.get_instance(f.anchor()).await.unwrap().is_none());
}
orch8_storage::backend_test!(async fn revoked_grants_cannot_publish_inline_steps(b){inline_requires_live_grant(b).await;});

async fn delivery(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for mode in [Mode::Hosted, Mode::Remote, Mode::Inline] {
        let mut f = fixture(b.storage.as_ref(), &server, mode).await;
        f.request["input"] = json!({"private":"DELEGATION_INPUT_CANARY","nul":"nul\0λ","tiny":f64::from_bits(1),"precise":0.100_000_000_000_000_02});
        let published = accepted(&server, &f.request).await;
        let destination: RuntimeId =
            serde_json::from_value(f.request["delegation"]["destination_runtime_id"].clone())
                .unwrap();
        for runtime in [f.owner.owner_runtime_id, destination] {
            let capabilities = b
                .storage
                .get_runtime_capability(&f.owner.tenant_id, runtime, Utc::now())
                .await
                .unwrap()
                .unwrap();
            let response = reqwest::Client::new().post(format!("{}/workers/tasks/poll", server.v1_url())).header("x-api-key",ROOT)
                .json(&json!({"tenant_id":f.owner.tenant_id,"handler_name":"orch8.delegation","worker_id":runtime,"capabilities":capabilities,"limit":100})).send().await.unwrap();
            assert!(
                response.status().is_success(),
                "{}",
                response.text().await.unwrap()
            );
            let response: Value = response.json().await.unwrap();
            let tasks = response["tasks"].as_array().unwrap();
            if runtime != destination {
                assert!(
                    tasks.is_empty(),
                    "another runtime received a destination-bound delegation"
                );
                continue;
            }
            assert_eq!(
                tasks.len(),
                1,
                "published delegation was not claimable: {response}"
            );
            let task = &tasks[0];
            assert_eq!(task["id"], published["mailbox_task_id"]);
            assert_eq!(task["params"]["input"], f.request["input"]);
            let response=reqwest::Client::new().post(format!("{}/workers/tasks/{}/complete",server.v1_url(),task["id"].as_str().unwrap())).header("x-api-key",ROOT)
                .json(&json!({"worker_id":runtime,"claim_epoch":task["claim_epoch"],"output":{"done":true}})).send().await.unwrap();
            assert!(
                response.status().is_success(),
                "{}",
                response.text().await.unwrap()
            );
        }
        let task_id: Uuid = serde_json::from_value(published["mailbox_task_id"].clone()).unwrap();
        let stored = match &b.raw {
            RawPool::Postgres(pool) => sqlx::query_scalar::<_, String>(
                "SELECT record::text FROM delegation_publications WHERE task_id=$1",
            )
            .bind(task_id)
            .fetch_one(pool)
            .await
            .unwrap(),
            RawPool::Sqlite(pool) => sqlx::query_scalar::<_, String>(
                "SELECT record FROM delegation_publications WHERE task_id=?1",
            )
            .bind(task_id.to_string())
            .fetch_one(pool)
            .await
            .unwrap(),
        };
        assert!(
            !stored.contains("DELEGATION_INPUT_CANARY"),
            "receipt retained raw input"
        );
        assert!(
            !stored.contains(f.request["token"].as_str().unwrap()),
            "receipt retained the grant token"
        );
        assert_eq!(accepted(&server, &f.request).await, published);
    }
}
orch8_storage::backend_test!(async fn published_mailbox_preserves_lossless_input_and_destination_delivery(b){delivery(b).await;});

#[path = "delegation_publication/outcome.rs"]
mod outcome;

#[tokio::test]
async fn postgres_rechecks_grant_and_operator_deadlines_after_a_real_lock_wait() {
    use orch8_storage::testing::backends::BackendKind;
    for kind in [BackendKind::Postgres, BackendKind::EncryptedPostgres] {
        let Some(b) = TestBackend::open(kind).await else {
            continue;
        };
        let RawPool::Postgres(pool) = &b.raw else {
            unreachable!()
        };
        let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
        for expire_key in [false, true] {
            let mut f = fixture(b.storage.as_ref(), &server, Mode::Inline).await;
            let deadline = Utc::now() + chrono::Duration::seconds(3);
            if !expire_key {
                f.request["delegation"]["expires_at"] = json!(deadline);
            }
            let key = orch8_types::api_key::mint(
                f.owner.tenant_id.as_str(),
                "delegation-deadline",
                if expire_key { Some(deadline) } else { None },
            );
            b.storage.create_api_key(&key.record).await.unwrap();
            let mut lock = pool.begin().await.unwrap();
            sqlx::query("SELECT id FROM continuation_grants WHERE id=$1 FOR UPDATE")
                .bind(f.grant_id.into_uuid())
                .execute(&mut *lock)
                .await
                .unwrap();
            let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *lock)
                .await
                .unwrap();
            let request = f.request.clone();
            let url = format!("{}/continuity/delegations/claim", server.v1_url());
            let waiting = tokio::spawn(async move {
                reqwest::Client::new()
                    .post(url)
                    .header("x-api-key", &key.secret)
                    .json(&request)
                    .send()
                    .await
                    .unwrap()
            });
            tokio::time::timeout(std::time::Duration::from_secs(2),async {
                loop {
                    let request_is_waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))").bind(blocker).fetch_one(pool).await.unwrap();
                    if request_is_waiting {break;}
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }).await.expect("request never reached the held grant lock");
            tokio::time::sleep(
                (deadline - Utc::now()).to_std().unwrap_or_default()
                    + std::time::Duration::from_millis(40),
            )
            .await;
            lock.commit().await.unwrap();
            assert_eq!(
                waiting.await.unwrap().status(),
                reqwest::StatusCode::CONFLICT
            );
            assert_empty(b.storage.as_ref(), &f).await;
        }
    }
}

async fn tampered_payload(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Hosted).await;
    let reply = accepted(&server, &f.request).await;
    let id: Uuid = serde_json::from_value(reply["mailbox_task_id"].clone()).unwrap();
    let mut task = b.storage.get_worker_task(id).await.unwrap().unwrap();
    task.params["input"] = json!({"private":"TAMPERED_AFTER_PUBLICATION"});
    b.raw
        .set_json(
            "worker_tasks",
            "params",
            "id",
            &id.to_string(),
            &task.params,
        )
        .await;
    let destination: RuntimeId =
        serde_json::from_value(f.request["delegation"]["destination_runtime_id"].clone()).unwrap();
    let capabilities = b
        .storage
        .get_runtime_capability(&f.owner.tenant_id, destination, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let response=reqwest::Client::new().post(format!("{}/workers/tasks/poll",server.v1_url())).header("x-api-key",ROOT)
        .json(&json!({"handler_name":"orch8.delegation","worker_id":destination,"capabilities":capabilities,"limit":10})).send().await.unwrap();
    if response.status().is_success() {
        let body: Value = response.json().await.unwrap();
        assert!(
            body["tasks"].as_array().unwrap().is_empty(),
            "tampered mailbox payload was delivered"
        );
    }
    let stored = b.storage.get_worker_task(id).await.unwrap().unwrap();
    assert_eq!(
        stored.state,
        orch8_types::worker::WorkerTaskState::Pending,
        "failed claim retained partial lease state"
    );
}
orch8_storage::backend_test!(async fn published_mailbox_payload_cannot_change_before_delivery(b){tampered_payload(b).await;});

async fn receipt_integrity(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    if let RawPool::Postgres(pool) = &b.raw {
        let mut tx = pool.begin().await.unwrap();
        sqlx::raw_sql(include_str!(
            "../../migrations/down/142_delegation_publications.sql"
        ))
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        sqlx::raw_sql(include_str!(
            "../../migrations/142_delegation_publications.sql"
        ))
        .execute(pool)
        .await
        .unwrap();
    }
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    accepted(&server, &f.request).await;
    let before = b
        .storage
        .get_delegation_publication(&f.owner.tenant_id, f.delegation_id())
        .await
        .unwrap()
        .unwrap();
    match &b.raw {
        RawPool::Postgres(pool) => {
            assert!(
                sqlx::query(
                    "UPDATE delegation_publications SET record=record WHERE delegation_id=$1"
                )
                .bind(f.delegation_id())
                .execute(pool)
                .await
                .is_err()
            );
            assert!(
                sqlx::query("DELETE FROM delegation_publications WHERE delegation_id=$1")
                    .bind(f.delegation_id())
                    .execute(pool)
                    .await
                    .is_err()
            );
            let mut tx = pool.begin().await.unwrap();
            assert!(
                sqlx::raw_sql(include_str!(
                    "../../migrations/down/142_delegation_publications.sql"
                ))
                .execute(&mut *tx)
                .await
                .is_err()
            );
            tx.rollback().await.unwrap();
        }
        RawPool::Sqlite(pool) => {
            assert!(
                sqlx::query(
                    "UPDATE delegation_publications SET record=record WHERE delegation_id=?1"
                )
                .bind(f.delegation_id().to_string())
                .execute(pool)
                .await
                .is_err()
            );
            assert!(
                sqlx::query("DELETE FROM delegation_publications WHERE delegation_id=?1")
                    .bind(f.delegation_id().to_string())
                    .execute(pool)
                    .await
                    .is_err()
            );
        }
    }
    for version in [json!(2), Value::Null, json!("1")] {
        let id = Uuid::now_v7();
        let grant = Uuid::now_v7();
        let task = Uuid::now_v7();
        let mut record = serde_json::to_value(&before).unwrap();
        record["version"] = version;
        record["delegation"]["id"] = json!(id);
        record["delegation"]["grant_id"] = json!(grant);
        record["task_id"] = json!(task);
        let refused=match &b.raw {
            RawPool::Postgres(pool)=>sqlx::query("INSERT INTO delegation_publications(delegation_id,tenant_id,grant_id,task_id,instance_id,record,created_at) VALUES($1,$2,$3,$4,$5,$6,$7)").bind(id).bind(f.owner.tenant_id.as_str()).bind(grant).bind(task).bind(before.instance_id.into_uuid()).bind(record).bind(Utc::now()).execute(pool).await.is_err(),
            RawPool::Sqlite(pool)=>sqlx::query("INSERT INTO delegation_publications(delegation_id,tenant_id,grant_id,task_id,instance_id,record,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)").bind(id.to_string()).bind(f.owner.tenant_id.as_str()).bind(grant.to_string()).bind(task.to_string()).bind(before.instance_id.to_string()).bind(record.to_string()).bind(Utc::now().to_rfc3339()).execute(pool).await.is_err(),
        };
        assert!(refused, "unknown/downgraded publication codec was accepted");
    }
    assert_eq!(
        b.storage
            .get_delegation_publication(&f.owner.tenant_id, f.delegation_id())
            .await
            .unwrap()
            .unwrap(),
        before
    );
}
orch8_storage::backend_test!(async fn publication_receipts_are_immutable_versioned_and_cannot_be_downgraded(b){receipt_integrity(b).await;});

#[tokio::test]
async fn sqlite_71_upgrade_and_reopen_retains_the_publication_receipt() {
    use orch8_storage::sqlite::SqliteStorage;
    use std::sync::Arc;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("publication.db");
    let path = path.to_str().unwrap();
    let old = SqliteStorage::file(path).await.unwrap();
    sqlx::raw_sql(
        "DROP TABLE delegation_publications; DELETE FROM schema_versions WHERE version=72;",
    )
    .execute(old.pool())
    .await
    .unwrap();
    old.pool().close().await;
    drop(old);
    let storage = Arc::new(SqliteStorage::file(path).await.unwrap());
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM schema_versions WHERE version=72)"
        )
        .fetch_one(storage.pool())
        .await
        .unwrap()
    );
    let server = spawn_test_server_on(storage.clone(), Some(ROOT)).await;
    let f = fixture(storage.as_ref(), &server, Mode::Inline).await;
    let first = accepted(&server, &f.request).await;
    let receipt = storage
        .get_delegation_publication(&f.owner.tenant_id, f.delegation_id())
        .await
        .unwrap()
        .unwrap();
    drop(server);
    storage.pool().close().await;
    drop(storage);
    let storage = Arc::new(SqliteStorage::file(path).await.unwrap());
    assert_eq!(
        storage
            .get_delegation_publication(&f.owner.tenant_id, f.delegation_id())
            .await
            .unwrap()
            .unwrap(),
        receipt
    );
    sqlx::query("DELETE FROM task_instances WHERE id=?1")
        .bind(f.anchor().to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    let server = spawn_test_server_on(storage.clone(), Some(ROOT)).await;
    assert_eq!(accepted(&server, &f.request).await, first);
    assert!(storage.get_instance(f.anchor()).await.unwrap().is_none());
    drop(server);
    storage.pool().close().await;
}

async fn cross_tenant_grant(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Inline).await;
    let mut crossed = f.request.clone();
    crossed["tenant_id"] = json!("another-tenant");
    crossed["delegation"]["tenant_id"] = json!("another-tenant");
    assert_eq!(
        claim(&server, &crossed).await.status(),
        reqwest::StatusCode::NOT_FOUND
    );
    assert_empty(b.storage.as_ref(), &f).await;
    let sequence = SequenceId::from_uuid(orch8_engine::delegation::step_sequence_id(
        "another-tenant",
        "noop",
        "work",
    ));
    assert!(b.storage.get_sequence(sequence).await.unwrap().is_none());
}
orch8_storage::backend_test!(async fn a_cross_tenant_grant_is_hidden_before_any_inline_publication(b){cross_tenant_grant(b).await;});
