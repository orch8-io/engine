//! A relay record must never become a native executable workflow.
use super::*;
use orch8_storage::SignalStore;

async fn resume_proxy(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    accepted(&server, &f.request).await;
    b.storage
        .update_instance_state(f.anchor(), InstanceState::Paused, None)
        .await
        .unwrap();
    b.storage
        .enqueue_signal(&orch8_types::signal::Signal {
            id: Uuid::now_v7(),
            instance_id: f.anchor(),
            signal_type: orch8_types::signal::SignalType::Resume,
            payload: json!({}),
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        })
        .await
        .unwrap();
    orch8_engine::signals::process_signals(b.storage.as_ref(), f.anchor(), InstanceState::Paused)
        .await
        .unwrap();
    let relay = b.storage.get_instance(f.anchor()).await.unwrap().unwrap();
    assert_eq!(
        relay.state,
        InstanceState::Waiting,
        "resuming a relay scheduled the delegated sequence for native execution"
    );
    assert!(relay.next_fire_at.is_none());
    assert!(
        b.storage
            .get_pending_signals(f.anchor())
            .await
            .unwrap()
            .is_empty()
    );
}
orch8_storage::backend_test!(async fn resuming_a_proxy_waits_for_its_destination_result(b){resume_proxy(b).await;});

async fn scheduled_proxy_cannot_take_native_slot(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for max_per_tenant in [0, 1] {
        let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
        accepted(&server, &f.request).await;
        b.storage
            .update_instance_state(
                f.anchor(),
                InstanceState::Scheduled,
                Some(Utc::now() - chrono::Duration::hours(1)),
            )
            .await
            .unwrap();
        let sequence = b
            .storage
            .get_sequence(f.sequence_id())
            .await
            .unwrap()
            .unwrap();
        let mut native = InstanceBuilder::for_sequence(&sequence)
            .state(InstanceState::Scheduled)
            .build();
        native.next_fire_at = Some(Utc::now());
        b.storage.create_instance(&native).await.unwrap();
        let claimed = b
            .storage
            .claim_due_instances(Utc::now(), 1, max_per_tenant)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(
            claimed[0].id, native.id,
            "a relay consumed the native page before its limit/fairness cap"
        );
        let relay = b.storage.get_instance(f.anchor()).await.unwrap().unwrap();
        assert!(relay.context.runtime.run_id.is_none());
        assert_eq!(relay.state, InstanceState::Scheduled);
    }
}
orch8_storage::backend_test!(async fn scheduled_proxies_never_consume_native_claim_slots(b){scheduled_proxy_cannot_take_native_slot(b).await;});

pub(super) const DROP_SQLITE_FENCE: &str =
    "DROP TRIGGER IF EXISTS delegation_proxy_quarantine_capture_insert;
 DROP TRIGGER IF EXISTS delegation_proxy_quarantine_capture_delete;
 DROP TRIGGER IF EXISTS delegation_proxy_quarantine_update;
 DROP TRIGGER IF EXISTS delegation_proxy_quarantine_delete;
 DROP TRIGGER IF EXISTS delegation_proxy_instance_insert;
 DROP TRIGGER IF EXISTS delegation_proxy_instance_update;
 DROP TRIGGER IF EXISTS delegation_proxy_metadata_running;
 DROP TRIGGER IF EXISTS delegation_proxy_identity_update;
 DROP TRIGGER IF EXISTS delegation_proxy_identity_add;
 DROP TRIGGER IF EXISTS delegation_proxy_runtime_update;
 DROP VIEW IF EXISTS delegation_proxy_native_history;
 DROP VIEW IF EXISTS delegation_proxy_instances;
 DROP INDEX IF EXISTS idx_delegation_publications_proxy_instances;
 DROP INDEX IF EXISTS idx_task_instances_delegation_proxy;
 DROP TABLE IF EXISTS delegation_proxy_quarantines;
 DELETE FROM schema_versions WHERE version=73;";

async fn hosted_identity_collision_is_native(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let mut f = fixture(b.storage.as_ref(), &server, Mode::Hosted).await;
    f.request["delegation"]["id"] = json!(f.anchor());
    accepted(&server, &f.request).await;
    assert!(
        !orch8_storage::delegation_publication::is_proxy_instance(b.storage.as_ref(), f.anchor())
            .await
            .unwrap()
    );
    b.storage
        .update_instance_state(f.anchor(), InstanceState::Scheduled, None)
        .await
        .unwrap();
    let claimed = b
        .storage
        .claim_due_instances(Utc::now(), 1, 0)
        .await
        .unwrap();
    assert_eq!(
        claimed[0].id,
        f.anchor(),
        "a matching delegation/instance UUID hid a real hosted workflow"
    );
    assert!(
        b.storage
            .ensure_instance_run_started_claimed(
                f.anchor(),
                "hosted-run",
                Utc::now(),
                claimed[0].updated_at
            )
            .await
            .unwrap()
    );
}
orch8_storage::backend_test!(async fn hosted_parent_with_a_matching_delegation_uuid_remains_runnable(b){hosted_identity_collision_is_native(b).await;});

async fn destination_result_after_resume(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let (f, task, claim) = super::outcome::held_mailbox(&b, &server, Mode::Remote).await;
    let error = b
        .storage
        .update_instance_state(f.anchor(), InstanceState::Running, None)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("delegation relay cannot enter native execution")
    );
    b.storage
        .update_instance_state(f.anchor(), InstanceState::Paused, None)
        .await
        .unwrap();
    b.storage
        .enqueue_signal(&orch8_types::signal::Signal {
            id: Uuid::now_v7(),
            instance_id: f.anchor(),
            signal_type: orch8_types::signal::SignalType::Resume,
            payload: json!({}),
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        })
        .await
        .unwrap();
    orch8_engine::signals::process_signals(b.storage.as_ref(), f.anchor(), InstanceState::Paused)
        .await
        .unwrap();
    let response = reqwest::Client::new().post(format!("{}/workers/tasks/{task}/complete", server.v1_url()))
        .header("x-api-key",ROOT).json(&json!({"worker_id":claim.worker_id,"claim_epoch":claim.claim_epoch,"output":{"remote":true}})).send().await.unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let relay = b.storage.get_instance(f.anchor()).await.unwrap().unwrap();
    assert_eq!(relay.state, InstanceState::Completed);
    assert!(relay.context.runtime.run_id.is_none());
    assert_eq!(
        relay.context.data["delegations"][f.delegation_id().to_string()]["output"],
        json!({"remote":true})
    );
}
orch8_storage::backend_test!(async fn relay_execution_fences_allow_the_destination_result_after_resume(b){destination_result_after_resume(b).await;});

async fn cancellation_never_runs_the_delegated_hooks(b: TestBackend) {
    use std::sync::{Arc, Mutex};
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture_with_hooks(b.storage.as_ref(), &server, Mode::Remote, true).await;
    accepted(&server, &f.request).await;
    let seq = b
        .storage
        .get_sequence(f.sequence_id())
        .await
        .unwrap()
        .unwrap();
    let native = InstanceBuilder::for_sequence(&seq)
        .state(InstanceState::Waiting)
        .build();
    b.storage.create_instance(&native).await.unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let record = seen.clone();
    let engine = orch8_engine::testing::TestEngine::builder()
        .storage(b.storage.clone())
        .register("noop", move |ctx| {
            let record = record.clone();
            async move {
                record.lock().unwrap().push(ctx.instance_id);
                Ok(json!({}))
            }
        })
        .build()
        .await;
    for id in [f.anchor(), native.id] {
        b.storage
            .enqueue_signal(&orch8_types::signal::Signal {
                id: Uuid::now_v7(),
                instance_id: id,
                signal_type: orch8_types::signal::SignalType::Cancel,
                payload: json!({}),
                delivered: false,
                created_at: Utc::now(),
                delivered_at: None,
            })
            .await
            .unwrap();
    }
    for _ in 0..3 {
        engine.tick().await;
    }
    assert_eq!(
        *seen.lock().unwrap(),
        vec![native.id],
        "relay cleanup ran on the server or real native cleanup was disabled"
    );
    assert_eq!(
        b.storage
            .get_instance(f.anchor())
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Cancelled
    );
    assert!(
        b.storage
            .get_all_outputs(f.anchor())
            .await
            .unwrap()
            .is_empty(),
        "relay recorded the delegated sequence's interceptors/hooks"
    );
    assert!(
        !b.storage
            .get_all_outputs(native.id)
            .await
            .unwrap()
            .is_empty()
    );
}
orch8_storage::backend_test!(async fn relay_cancellation_skips_workflow_hooks_but_native_cancellation_keeps_them(b){cancellation_never_runs_the_delegated_hooks(b).await;});

orch8_storage::backend_test!(kinds=[sqlite,encrypted_sqlite], async fn relay_classification_uses_point_indexes(b){
    use sqlx::Row;
    let RawPool::Sqlite(pool) = &b.raw else { unreachable!() };
    let rows=sqlx::query("EXPLAIN QUERY PLAN SELECT EXISTS(SELECT 1 FROM delegation_proxy_instances WHERE instance_id=?1)").bind(Uuid::now_v7().to_string()).fetch_all(pool).await.unwrap();
    let plan:Vec<String>=rows.into_iter().map(|row|row.get(3)).collect();
    for index in ["idx_task_instances_delegation_proxy","idx_delegation_publications_proxy_instances"] {
        assert!(plan.iter().any(|line|line.contains("SEARCH")&&line.contains(index)), "relay classification lost its point index: {plan:?}");
    }
});

async fn metadata_values_are_not_proxy_authority(b: TestBackend) {
    let seq = SequenceBuilder::new("metadata-is-data")
        .step("native", "noop")
        .build();
    b.storage.create_sequence(&seq).await.unwrap();
    for metadata in [
        json!(["orch8_delegation_proxy"]),
        json!("orch8_delegation_proxy"),
        json!(null),
        json!({"note":"orch8_delegation_proxy"}),
    ] {
        let native = InstanceBuilder::for_sequence(&seq)
            .state(InstanceState::Scheduled)
            .metadata(metadata)
            .build();
        b.storage.create_instance(&native).await.unwrap();
        assert!(
            !orch8_storage::delegation_publication::is_proxy_instance(
                b.storage.as_ref(),
                native.id
            )
            .await
            .unwrap()
        );
        let claimed = b
            .storage
            .claim_due_instances(Utc::now(), 1, 0)
            .await
            .unwrap();
        assert_eq!(
            claimed[0].id, native.id,
            "business metadata was treated as a relay marker"
        );
    }
}
orch8_storage::backend_test!(async fn ordinary_metadata_arrays_and_strings_cannot_claim_relay_identity(b){metadata_values_are_not_proxy_authority(b).await;});

#[tokio::test]
async fn sqlite_72_reopen_quarantines_native_history_without_erasing_it() {
    use orch8_storage::sqlite::SqliteStorage;
    use std::sync::Arc;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("relay-upgrade.db");
    let path = path.to_str().unwrap();
    let old = Arc::new(SqliteStorage::file(path).await.unwrap());
    let server = spawn_test_server_on(old.clone(), Some(ROOT)).await;
    let mut cases = Vec::new();
    // Missing legacy metadata must not hide immutable publication identity.
    for (field, value) in [
        ("run_id", json!("historical-wrong-runtime")),
        ("started_at", json!(Utc::now())),
        ("current_step", json!("work")),
        ("current_step_started_at", json!(Utc::now())),
        ("clean", Value::Null),
    ] {
        let f = fixture(old.as_ref(), &server, Mode::Remote).await;
        accepted(&server, &f.request).await;
        cases.push((f, field, value));
    }
    drop(server);
    sqlx::raw_sql(DROP_SQLITE_FENCE)
        .execute(old.pool())
        .await
        .unwrap();
    for (f, field, value) in &cases {
        let mut context =
            serde_json::to_value(old.get_instance(f.anchor()).await.unwrap().unwrap().context)
                .unwrap();
        if *field != "clean" {
            context["runtime"][*field] = value.clone();
        }
        sqlx::query("UPDATE task_instances SET state='scheduled', context=?1, metadata='{}',claimed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),next_fire_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?2")
            .bind(context.to_string()).bind(f.anchor().to_string()).execute(old.pool()).await.unwrap();
    }
    old.pool().close().await;
    drop(old);
    let storage = Arc::new(SqliteStorage::file(path).await.unwrap());
    let server = spawn_test_server_on(storage.clone(), Some(ROOT)).await;
    for (f, field, value) in &cases {
        assert_reopened_relay_quarantine(&storage, &server, f, field, value).await;
        if *field != "clean" {
            assert_quarantine_survives_restore(
                storage.as_ref(),
                &RawPool::Sqlite(storage.pool().clone()),
                &server,
                f,
            )
            .await;
        }
    }
    drop(server);
    storage.pool().close().await;
}

async fn identity_and_runtime_are_protected(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    accepted(&server, &f.request).await;
    let relay = b.storage.get_instance(f.anchor()).await.unwrap().unwrap();
    let mut context = serde_json::to_value(&relay.context).unwrap();
    context["runtime"]["run_id"] = json!("forged-native-run");
    let err = match &b.raw {
        RawPool::Sqlite(pool) => sqlx::query("UPDATE task_instances SET context=?1 WHERE id=?2")
            .bind(context.to_string())
            .bind(f.anchor().to_string())
            .execute(pool)
            .await
            .unwrap_err(),
        RawPool::Postgres(pool) => sqlx::query("UPDATE task_instances SET context=$1 WHERE id=$2")
            .bind(context)
            .bind(f.anchor().into_uuid())
            .execute(pool)
            .await
            .unwrap_err(),
    };
    assert!(
        err.to_string()
            .contains("native runtime history is immutable")
    );
    // An unrelated native record cannot acquire reserved relay identity later.
    let sequence = b
        .storage
        .get_sequence(f.sequence_id())
        .await
        .unwrap()
        .unwrap();
    let native = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Waiting)
        .build();
    b.storage.create_instance(&native).await.unwrap();
    let err = match &b.raw {
        RawPool::Sqlite(pool) => sqlx::query("UPDATE task_instances SET metadata=?1 WHERE id=?2")
            .bind(relay.metadata.to_string())
            .bind(native.id.to_string())
            .execute(pool)
            .await
            .unwrap_err(),
        RawPool::Postgres(pool) => sqlx::query("UPDATE task_instances SET metadata=$1 WHERE id=$2")
            .bind(&relay.metadata)
            .bind(native.id.into_uuid())
            .execute(pool)
            .await
            .unwrap_err(),
    };
    assert!(
        err.to_string()
            .contains("native instance cannot become a delegation relay")
    );
}
orch8_storage::backend_test!(async fn relay_identity_and_runtime_history_are_database_fenced(b){identity_and_runtime_are_protected(b).await;});

async fn postgres_quarantines_old_native_proxy(b: TestBackend) {
    let RawPool::Postgres(pool) = &b.raw else {
        unreachable!()
    };
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    accepted(&server, &f.request).await;
    // Model a pre-143 writer/database, retaining the immutable publication.
    sqlx::raw_sql("DROP TRIGGER delegation_proxy_quarantine_capture_insert ON task_instances; DROP TRIGGER delegation_proxy_quarantine_capture_delete ON task_instances; DROP FUNCTION capture_delegation_proxy_quarantine(); DROP TRIGGER delegation_proxy_quarantine_immutable ON delegation_proxy_quarantines; DROP FUNCTION protect_delegation_proxy_quarantine(); DROP TRIGGER delegation_proxy_instance_fence ON task_instances; DROP FUNCTION fence_delegation_proxy_instance(); DROP VIEW delegation_proxy_native_history; DROP VIEW delegation_proxy_instances; DROP INDEX idx_delegation_publications_proxy_instances; DROP INDEX idx_task_instances_delegation_proxy; DROP TABLE delegation_proxy_quarantines;").execute(pool).await.unwrap();
    sqlx::query("UPDATE task_instances SET state='running',metadata='{}',context=jsonb_set(context,'{runtime,run_id}','\"historical-native-run\"'),claimed_at=clock_timestamp(),next_fire_at=clock_timestamp() WHERE id=$1")
        .bind(f.anchor().into_uuid()).execute(pool).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../../migrations/143_delegation_proxy_execution_fence.sql"
    ))
    .execute(pool)
    .await
    .unwrap();
    let relay = b.storage.get_instance(f.anchor()).await.unwrap().unwrap();
    assert_eq!(relay.state, InstanceState::Paused);
    assert_eq!(
        relay.context.runtime.run_id.as_deref(),
        Some("historical-native-run")
    );
    assert!(relay.claimed_at.is_none());
    assert!(relay.next_fire_at.is_none());
    assert_quarantine_survives_restore(b.storage.as_ref(), &b.raw, &server, &f).await;
    // Hot-instance GC cannot make a retained publication safe to downgrade.
    sqlx::query("DELETE FROM task_instances WHERE id=$1")
        .bind(f.anchor().into_uuid())
        .execute(pool)
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    let err = sqlx::raw_sql(include_str!(
        "../../../migrations/down/143_delegation_proxy_execution_fence.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap_err();
    assert!(err.to_string().contains("retained delegation relays"));
    tx.rollback().await.unwrap();
}
orch8_storage::backend_test!(kinds=[postgres,encrypted_postgres],async fn postgres_upgrade_quarantines_old_proxy_and_retained_receipt_blocks_downgrade(b){postgres_quarantines_old_native_proxy(b).await;});

async fn assert_reopened_relay_quarantine(
    storage: &std::sync::Arc<orch8_storage::sqlite::SqliteStorage>,
    server: &BackendTestServer,
    f: &Fixture,
    field: &str,
    value: &Value,
) {
    let relay = storage.get_instance(f.anchor()).await.unwrap().unwrap();
    let expected = if field == "clean" {
        InstanceState::Waiting
    } else {
        InstanceState::Paused
    };
    assert_eq!(relay.state, expected, "upgrade state for {field}");
    assert!(relay.claimed_at.is_none());
    assert!(relay.next_fire_at.is_none());
    if field == "clean" {
        return;
    }
    assert_eq!(
        serde_json::to_value(&relay.context).unwrap()["runtime"][field],
        *value
    );
    // Neither raw old writers nor modern resume may erase quarantine evidence.
    let mut cleared = serde_json::to_value(&relay.context).unwrap();
    cleared["runtime"][field] = Value::Null;
    assert!(
        sqlx::query("UPDATE task_instances SET context=?1 WHERE id=?2")
            .bind(cleared.to_string())
            .bind(f.anchor().to_string())
            .execute(storage.pool())
            .await
            .is_err()
    );
    assert!(
        storage
            .update_instance_state(f.anchor(), InstanceState::Running, None)
            .await
            .is_err()
    );
    storage
        .enqueue_signal(&orch8_types::signal::Signal {
            id: Uuid::now_v7(),
            instance_id: f.anchor(),
            signal_type: orch8_types::signal::SignalType::Resume,
            payload: json!({}),
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        })
        .await
        .unwrap();
    orch8_engine::signals::process_signals(storage.as_ref(), f.anchor(), InstanceState::Paused)
        .await
        .unwrap();
    assert_eq!(
        storage
            .get_instance(f.anchor())
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Paused
    );
    assert!(
        storage
            .get_pending_signals(f.anchor())
            .await
            .unwrap()
            .is_empty()
    );
    let destination: RuntimeId =
        serde_json::from_value(f.request["delegation"]["destination_runtime_id"].clone()).unwrap();
    let capabilities = storage
        .get_runtime_capability(&f.owner.tenant_id, destination, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let response=reqwest::Client::new().post(format!("{}/workers/tasks/poll",server.v1_url())).header("x-api-key",ROOT)
            .json(&json!({"handler_name":"orch8.delegation","worker_id":destination,"capabilities":capabilities,"limit":10})).send().await.unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let body: Value = response.json().await.unwrap();
    assert!(
        body["tasks"].as_array().unwrap().is_empty(),
        "quarantined relay dispatched destination work for {field}"
    );
}

async fn assert_quarantine_survives_restore(
    storage: &dyn StorageBackend,
    raw: &RawPool,
    server: &BackendTestServer,
    f: &Fixture,
) {
    let mut restored = storage.get_instance(f.anchor()).await.unwrap().unwrap();
    match raw {
        RawPool::Sqlite(pool) => {
            sqlx::query("DELETE FROM task_instances WHERE id=?1")
                .bind(f.anchor().to_string())
                .execute(pool)
                .await
                .unwrap();
        }
        RawPool::Postgres(pool) => {
            sqlx::query("DELETE FROM task_instances WHERE id=$1")
                .bind(f.anchor().into_uuid())
                .execute(pool)
                .await
                .unwrap();
        }
    }
    assert!(
        orch8_storage::delegation_publication::proxy_has_native_history(storage, f.anchor())
            .await
            .unwrap(),
        "quarantine disappeared while hot instance was absent"
    );
    // A cold/native restore can legitimately omit hot runtime and tree history.
    restored.context.runtime = orch8_types::context::RuntimeContext::default();
    restored.state = InstanceState::Paused;
    storage.create_instance(&restored).await.unwrap();
    assert!(
        orch8_storage::delegation_publication::proxy_has_native_history(storage, f.anchor())
            .await
            .unwrap(),
        "hot cleanup and cold restore erased durable relay quarantine"
    );
    storage
        .enqueue_signal(&orch8_types::signal::Signal {
            id: Uuid::now_v7(),
            instance_id: f.anchor(),
            signal_type: orch8_types::signal::SignalType::Resume,
            payload: json!({}),
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        })
        .await
        .unwrap();
    orch8_engine::signals::process_signals(storage, f.anchor(), InstanceState::Paused)
        .await
        .unwrap();
    assert_eq!(
        storage
            .get_instance(f.anchor())
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Paused
    );
    assert_no_destination_dispatch(storage, server, f).await;
}

async fn assert_no_destination_dispatch(
    storage: &dyn StorageBackend,
    server: &BackendTestServer,
    f: &Fixture,
) {
    let destination: RuntimeId =
        serde_json::from_value(f.request["delegation"]["destination_runtime_id"].clone()).unwrap();
    let capabilities = storage
        .get_runtime_capability(&f.owner.tenant_id, destination, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let response=reqwest::Client::new().post(format!("{}/workers/tasks/poll",server.v1_url())).header("x-api-key",ROOT)
        .json(&json!({"handler_name":"orch8.delegation","worker_id":destination,"capabilities":capabilities,"limit":10})).send().await.unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let body: Value = response.json().await.unwrap();
    assert!(
        body["tasks"].as_array().unwrap().is_empty(),
        "restored quarantined relay dispatched destination work"
    );
}

async fn legacy_quarantine_is_permanent(b: TestBackend) {
    let sequence = SequenceBuilder::new("historical-relay")
        .step("work", "noop")
        .build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let mut relay = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Waiting)
        .build();
    relay.metadata = json!({"orch8_delegation_proxy":{"legacy":true}});
    relay.context.runtime.run_id = Some("pre-upgrade-native-run".into());
    b.storage.create_instance(&relay).await.unwrap();
    assert!(
        b.storage
            .is_delegation_proxy_quarantined(relay.id)
            .await
            .unwrap()
    );
    // No publication or continuity row exists for this legacy relay.
    match &b.raw {
        RawPool::Sqlite(pool) => {
            assert!(sqlx::query("UPDATE delegation_proxy_quarantines SET tenant_id=tenant_id WHERE instance_id=?1").bind(relay.id.to_string()).execute(pool).await.is_err());
            assert!(
                sqlx::query("DELETE FROM delegation_proxy_quarantines WHERE instance_id=?1")
                    .bind(relay.id.to_string())
                    .execute(pool)
                    .await
                    .is_err()
            );
            sqlx::query("DELETE FROM task_instances WHERE id=?1")
                .bind(relay.id.to_string())
                .execute(pool)
                .await
                .unwrap();
        }
        RawPool::Postgres(pool) => {
            assert!(sqlx::query("UPDATE delegation_proxy_quarantines SET tenant_id=tenant_id WHERE instance_id=$1").bind(relay.id.into_uuid()).execute(pool).await.is_err());
            assert!(
                sqlx::query("DELETE FROM delegation_proxy_quarantines WHERE instance_id=$1")
                    .bind(relay.id.into_uuid())
                    .execute(pool)
                    .await
                    .is_err()
            );
            sqlx::query("DELETE FROM task_instances WHERE id=$1")
                .bind(relay.id.into_uuid())
                .execute(pool)
                .await
                .unwrap();
        }
    }
    assert!(
        b.storage
            .is_delegation_proxy_quarantined(relay.id)
            .await
            .unwrap()
    );
    relay.metadata = json!({});
    relay.context.runtime = orch8_types::context::RuntimeContext::default();
    relay.state = InstanceState::Scheduled;
    relay.next_fire_at = Some(Utc::now());
    b.storage.create_instance(&relay).await.unwrap();
    assert!(
        orch8_storage::delegation_publication::is_proxy_instance(b.storage.as_ref(), relay.id)
            .await
            .unwrap()
    );
    assert!(
        b.storage
            .claim_due_instances(Utc::now(), 1, 0)
            .await
            .unwrap()
            .is_empty()
    );
}
orch8_storage::backend_test!(async fn legacy_relay_quarantine_survives_hot_cleanup_without_a_publication(b){legacy_quarantine_is_permanent(b).await;});
