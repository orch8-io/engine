//! Atomicity and ownership edges of real scoped materialization.
use super::{
    MobileStorage,
    materialize::{InstanceLimits, Preparation},
    materialize_tests::{Echo, builtins, credential, instance_limits, owner_with_work, tick},
    tests::{fixture, limits, request},
};
use crate::handlers::scoped::CatalogHandlers;
use orch8_storage::{InstanceStore, StorageBackend, sqlite::SqliteStorage};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[tokio::test]
async fn two_pools_cannot_overbook_global_materialization_capacity() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("admission.sqlite");
    let sqlite = Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    );
    let one = Arc::new(MobileStorage::new(sqlite.clone()));
    let two = Arc::new(MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    )));
    let alice = owner_with_work(&one, "alice", "one").await;
    let bob = owner_with_work(&two, "bob", "one").await;
    let a = one
        .prepare_catalog_run(&alice, request(Some("one"), "{}"), limits())
        .await
        .unwrap();
    let b = two
        .prepare_catalog_run(&bob, request(Some("two"), "{}"), limits())
        .await
        .unwrap();
    let Preparation::Pending(a_snapshot) = one.prepare_materialization(&alice, a.id).await.unwrap()
    else {
        panic!("pending");
    };
    let Preparation::Pending(b_snapshot) = two.prepare_materialization(&bob, b.id).await.unwrap()
    else {
        panic!("pending");
    };
    let handlers = std::collections::HashSet::from(["native".into()]);
    let a_checked = a_snapshot
        .check(&handlers, env!("CARGO_PKG_VERSION"))
        .unwrap();
    let b_checked = b_snapshot
        .check(&handlers, env!("CARGO_PKG_VERSION"))
        .unwrap();
    let caps = InstanceLimits {
        global: 1,
        account: 1,
        catalog: 1,
    };
    let stop = tokio_util::sync::CancellationToken::new();
    let (a_result, b_result) = tokio::join!(
        one.commit_materialization(&alice, a_checked, caps, &stop),
        two.commit_materialization(&bob, b_checked, caps, &stop)
    );
    assert_eq!(
        usize::from(a_result.is_ok()) + usize::from(b_result.is_ok()),
        1
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
        .fetch_one(sqlite.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_catalog_run_materializations")
        .fetch_one(sqlite.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn receipt_failure_rolls_back_the_instance_and_releases_its_admission() {
    let (storage, sqlite) = fixture().await;
    let owner = owner_with_work(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    hub.bind(
        &owner,
        "native",
        Arc::new(Echo(Arc::new(AtomicUsize::new(0)))),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let run = storage
        .prepare_catalog_run(&owner, request(Some("atomic"), "{}"), limits())
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER fail_materialization BEFORE INSERT ON mobile_catalog_run_materializations BEGIN SELECT RAISE(ABORT,'injected materialization failure'); END").execute(sqlite.pool()).await.unwrap();
    assert!(
        hub.materialize(owner.clone(), run.id, instance_limits(), builtins())
            .await
            .is_err()
    );
    for table in ["task_instances", "mobile_catalog_run_materializations"] {
        let count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(sqlite.pool())
                .await
                .unwrap();
        assert_eq!(count, 0);
    }
    sqlx::query("DROP TRIGGER fail_materialization")
        .execute(sqlite.pool())
        .await
        .unwrap();
    assert_eq!(
        hub.materialize(owner, run.id, instance_limits(), builtins())
            .await
            .unwrap()
            .id,
        run.id
    );
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn cancelled_observer_keeps_admission_until_its_owned_db_wait_settles() {
    let (storage, sqlite) = fixture().await;
    let owner = owner_with_work(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    hub.bind(
        &owner,
        "native",
        Arc::new(Echo(Arc::new(AtomicUsize::new(0)))),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let run = storage
        .prepare_catalog_run(&owner, request(Some("cancel"), "{}"), limits())
        .await
        .unwrap();
    let connection = sqlite.pool().acquire().await.unwrap();
    let gate = hub.account_gate(&owner).unwrap();
    let task_owner = hub.clone();
    let task_scope = owner.clone();
    let task = tokio::spawn(async move {
        task_owner
            .materialize(task_scope, run.id, instance_limits(), builtins())
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while gate.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        gate.available_permits(),
        0,
        "observer cancellation released physical admission before its DB wait settled"
    );
    drop(connection);
    tokio::time::timeout(Duration::from_secs(5), async {
        while gate.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(sqlite.get_instance(run.id).await.unwrap().is_none());
    assert_eq!(
        hub.materialize(owner, run.id, instance_limits(), builtins())
            .await
            .unwrap()
            .id,
        run.id
    );
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn a_scoped_instance_cannot_publish_unproven_worker_payloads() {
    let (storage, sqlite) = fixture().await;
    let owner = owner_with_work(&storage, "alice", "one").await;
    let backend: Arc<dyn StorageBackend> = sqlite.clone();
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    hub.bind(
        &owner,
        "native",
        Arc::new(Echo(Arc::new(AtomicUsize::new(0)))),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let run = storage
        .prepare_catalog_run(&owner, request(Some("worker"), "{}"), limits())
        .await
        .unwrap();
    hub.materialize(owner, run.id, instance_limits(), builtins())
        .await
        .unwrap();
    let task=serde_json::from_value(serde_json::json!({"id":uuid::Uuid::now_v7(),"instance_id":run.id,"block_id":"work","handler_name":"native","params":{"private":"WORKER_CANARY"},"context":{},"attempt":0,"state":"pending","created_at":chrono::Utc::now()})).unwrap();
    assert!(matches!(
        backend.create_worker_task(&task).await,
        Err(orch8_types::error::StorageError::Unsupported(_))
    ));
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_tasks")
        .fetch_one(sqlite.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn encrypted_credential_grants_resolve_without_copying_secret_material_into_grants() {
    use orch8_storage::encrypting::EncryptingStorage;
    use orch8_types::encryption::FieldEncryptor;
    let (storage, sqlite) = fixture().await;
    let backend: Arc<dyn StorageBackend> = Arc::new(EncryptingStorage::new(
        sqlite.clone(),
        FieldEncryptor::from_hex_key(&"ab".repeat(32)).unwrap(),
    ));
    let owner = owner_with_work(&storage, "alice", "one").await;
    let tenant = super::super::workflows::owner(&owner).unwrap().0;
    backend
        .create_credential(&credential(
            "encrypted",
            tenant.as_str(),
            "ENCRYPTED_CREDENTIAL_CANARY",
        ))
        .await
        .unwrap();
    storage
        .grant_catalog_credential(&owner, "shared", "encrypted")
        .await
        .unwrap();
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    hub.bind(
        &owner,
        "native",
        Arc::new(Echo(calls.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let run = storage
        .prepare_catalog_run(&owner, request(Some("encrypted"), "{}"), limits())
        .await
        .unwrap();
    hub.materialize(owner, run.id, instance_limits(), builtins())
        .await
        .unwrap();
    let mut registry = orch8_engine::handlers::HandlerRegistry::new();
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    tick(&backend, &Arc::new(registry)).await;
    let output = backend
        .get_block_output(run.id, &orch8_types::ids::BlockId::new("work"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.output["secret"], "ENCRYPTED_CREDENTIAL_CANARY");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let stored: String = sqlx::query_scalar("SELECT value FROM credentials WHERE id='encrypted'")
        .fetch_one(sqlite.pool())
        .await
        .unwrap();
    assert!(!stored.contains("ENCRYPTED_CREDENTIAL_CANARY"));
    let stored:String=sqlx::query_scalar("SELECT output FROM block_outputs WHERE instance_id=?1 AND block_id='work' ORDER BY created_at DESC LIMIT 1").bind(run.id.to_string()).fetch_one(sqlite.pool()).await.unwrap();
    assert!(!stored.contains("ENCRYPTED_CREDENTIAL_CANARY"));
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One complete restart/restore scenario for both causes.
async fn runtime_sdk_downgrade_and_missing_binding_defer_without_business_retries() {
    use orch8_types::{
        clock::{Clock, ManualClock, SharedClock},
        instance::InstanceState,
    };
    for cause in ["sdk", "binding"] {
        let (storage, sqlite) = fixture().await;
        let owner = owner_with_work(&storage, "alice", "one").await;
        let backend: Arc<dyn StorageBackend> = sqlite.clone();
        let first = CatalogHandlers::isolated(storage.clone(), backend.clone());
        let tenant = super::super::workflows::owner(&owner).unwrap().0;
        backend
            .create_credential(&credential("runtime", tenant.as_str(), "RUNTIME_CANARY"))
            .await
            .unwrap();
        storage
            .grant_catalog_credential(&owner, "shared", "runtime")
            .await
            .unwrap();
        if cause == "sdk" {
            sqlx::query("UPDATE mobile_catalog_execution_policy SET min_sdk_version='9.0.0'")
                .execute(sqlite.pool())
                .await
                .unwrap();
        }
        let calls = Arc::new(AtomicUsize::new(0));
        first
            .bind(
                &owner,
                "native",
                Arc::new(Echo(calls.clone())),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let run = storage
            .prepare_catalog_run(&owner, request(Some("runtime"), "{}"), limits())
            .await
            .unwrap();
        let mut environment = builtins();
        environment.sdk = "9.0.0".into();
        first
            .materialize(owner.clone(), run.id, instance_limits(), environment)
            .await
            .unwrap();
        for receipt in first.close() {
            receipt.await.unwrap();
        }
        let replacement = CatalogHandlers::isolated(storage.clone(), backend.clone());
        if cause == "sdk" {
            replacement
                .bind(
                    &owner,
                    "native",
                    Arc::new(Echo(calls.clone())),
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
        }
        let clock = Arc::new(ManualClock::new(
            chrono::Utc::now() + chrono::Duration::seconds(1),
        ));
        let config = orch8_types::config::SchedulerConfig {
            clock: SharedClock::from_arc(clock.clone()),
            ..Default::default()
        };
        let mut registry = orch8_engine::handlers::HandlerRegistry::new();
        replacement.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(4));
        let cache = Arc::new(orch8_engine::sequence_cache::SequenceCache::new(
            16,
            Duration::from_secs(30),
        ));
        let stop = tokio_util::sync::CancellationToken::new();
        orch8_engine::scheduler::tick_once(
            &backend,
            &Arc::new(registry),
            &semaphore,
            &config,
            &cache,
            &stop,
        )
        .await
        .unwrap();
        let waiting = backend.get_instance(run.id).await.unwrap().unwrap();
        assert_eq!(waiting.state, InstanceState::Scheduled);
        assert_eq!(
            waiting.next_fire_at,
            Some(clock.now() + chrono::Duration::seconds(60))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            backend
                .get_continuity_execution_by_instance(&tenant, run.id)
                .await
                .unwrap()
                .is_none()
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_tasks")
            .fetch_one(sqlite.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
        if cause == "binding" {
            replacement
                .bind(
                    &owner,
                    "native",
                    Arc::new(Echo(calls.clone())),
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
        }
        let mut registry = orch8_engine::handlers::HandlerRegistry::new();
        replacement.install_resolver_for_sdk(&mut registry, crate::DEFAULT_BUILTINS, "9.0.0");
        clock.advance(chrono::Duration::seconds(61));
        orch8_engine::scheduler::tick_once(
            &backend,
            &Arc::new(registry),
            &semaphore,
            &config,
            &cache,
            &stop,
        )
        .await
        .unwrap();
        assert_eq!(
            backend.get_instance(run.id).await.unwrap().unwrap().state,
            InstanceState::Completed
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        for receipt in replacement.close() {
            receipt.await.unwrap();
        }
    }
}

#[tokio::test]
async fn materialization_rechecks_signed_policy_and_input_schema_before_any_native_row() {
    use orch8_storage::SequenceStore;
    let (storage, sqlite) = fixture().await;
    let owner = owner_with_work(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    hub.bind(
        &owner,
        "native",
        Arc::new(Echo(Arc::new(AtomicUsize::new(0)))),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let run = storage
        .prepare_catalog_run(&owner, request(Some("schema"), "{}"), limits())
        .await
        .unwrap();
    sqlx::query("UPDATE sequences SET input_schema=?1 WHERE id=?2")
        .bind(r#"{"type":"object","required":["required"]}"#)
        .bind(run.sequence_id.to_string())
        .execute(sqlite.pool())
        .await
        .unwrap();
    assert!(
        hub.materialize(owner.clone(), run.id, instance_limits(), builtins())
            .await
            .is_err()
    );
    sqlx::query("UPDATE sequences SET input_schema=NULL WHERE id=?1")
        .bind(run.sequence_id.to_string())
        .execute(sqlite.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE mobile_catalog_execution_policy SET min_sdk_version='99.0'")
        .execute(sqlite.pool())
        .await
        .unwrap();
    assert!(
        hub.materialize(owner.clone(), run.id, instance_limits(), builtins())
            .await
            .is_err()
    );
    sqlx::query("UPDATE mobile_catalog_execution_policy SET min_sdk_version='0.0',required_handlers='[\"missing-capability\"]'").execute(sqlite.pool()).await.unwrap();
    assert!(
        hub.materialize(owner.clone(), run.id, instance_limits(), builtins())
            .await
            .is_err()
    );
    sqlx::query("UPDATE mobile_catalog_execution_policy SET required_handlers='[\"native\"]'")
        .execute(sqlite.pool())
        .await
        .unwrap();
    let Preparation::Pending(snapshot) = storage
        .prepare_materialization(&owner, run.id)
        .await
        .unwrap()
    else {
        panic!("pending");
    };
    let checked = snapshot
        .check(&std::collections::HashSet::from(["native".into()]), "0.7.1")
        .unwrap();
    sqlx::query("UPDATE mobile_catalog_execution_policy SET min_sdk_version='99.0'")
        .execute(sqlite.pool())
        .await
        .unwrap();
    assert!(
        storage
            .commit_materialization(
                &owner,
                checked,
                instance_limits(),
                &tokio_util::sync::CancellationToken::new()
            )
            .await
            .is_err()
    );
    assert!(
        sqlite
            .get_sequence(run.sequence_id)
            .await
            .unwrap()
            .is_some()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
        .fetch_one(sqlite.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn account_retirement_cancels_logical_instances_without_erasing_materialization_evidence() {
    use orch8_types::instance::InstanceState;
    let (storage, sqlite) = fixture().await;
    let owner = owner_with_work(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    hub.bind(
        &owner,
        "native",
        Arc::new(Echo(Arc::new(AtomicUsize::new(0)))),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let run = storage
        .prepare_catalog_run(&owner, request(Some("retire"), "{}"), limits())
        .await
        .unwrap();
    hub.materialize(owner.clone(), run.id, instance_limits(), builtins())
        .await
        .unwrap();
    storage.retire_account_scope(&owner.account).await.unwrap();
    assert_eq!(
        sqlite.get_instance(run.id).await.unwrap().unwrap().state,
        InstanceState::Cancelled
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM mobile_catalog_run_materializations WHERE request_id=?1",
    )
    .bind(run.id.to_string())
    .fetch_one(sqlite.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn templates_cannot_turn_owned_local_dispatch_into_a_remote_worker_payload() {
    use super::materialize_tests::owner_with_params;
    use orch8_types::instance::InstanceState;
    let (storage, sqlite) = fixture().await;
    let owner = owner_with_params(
        &storage,
        "alice",
        "one",
        serde_json::json!("{{ data.params }}"),
    )
    .await;
    let backend: Arc<dyn StorageBackend> = sqlite.clone();
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let tenant = super::super::workflows::owner(&owner).unwrap().0;
    backend
        .create_credential(&credential(
            "dynamic",
            tenant.as_str(),
            "DYNAMIC_PRIVATE_CANARY",
        ))
        .await
        .unwrap();
    storage
        .grant_catalog_credential(&owner, "shared", "dynamic")
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    hub.bind(
        &owner,
        "native",
        Arc::new(Echo(calls.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let input=serde_json::json!({"params":{"$runtime":{"runtime_id":"untrusted-worker"},"secret":"credentials://shared"}}).to_string();
    let run = storage
        .prepare_catalog_run(
            &owner,
            request(Some("template-placement"), &input),
            limits(),
        )
        .await
        .unwrap();
    hub.materialize(owner, run.id, instance_limits(), builtins())
        .await
        .unwrap();
    let mut registry = orch8_engine::handlers::HandlerRegistry::new();
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    tick(&backend, &Arc::new(registry)).await;
    assert_eq!(
        backend.get_instance(run.id).await.unwrap().unwrap().state,
        InstanceState::Failed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_tasks")
        .fetch_one(sqlite.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(
        backend
            .get_continuity_execution_by_instance(&tenant, run.id)
            .await
            .unwrap()
            .is_none()
    );
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn a_frozen_version_survives_upgrade_but_new_materialization_refuses_explicit_withdrawal() {
    let (storage, sqlite) = fixture().await;
    let owner = owner_with_work(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    hub.bind(
        &owner,
        "native",
        Arc::new(Echo(Arc::new(AtomicUsize::new(0)))),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let first = storage
        .prepare_catalog_run(&owner, request(Some("frozen"), "{}"), limits())
        .await
        .unwrap();
    let second = storage
        .prepare_catalog_run(&owner, request(Some("withdrawn"), "{}"), limits())
        .await
        .unwrap();
    let mut newer = orch8_storage::testing::fixtures::SequenceBuilder::new("publisher")
        .name("flow")
        .step("work", "native")
        .build();
    newer.version = 2;
    let digest = "d".repeat(64);
    storage
        .accept_catalog_manifest(&owner, &[], 2, &digest)
        .await
        .unwrap();
    storage
        .install_catalog_workflow(&owner, &serde_json::to_string(&newer).unwrap(), 2, &digest)
        .await
        .unwrap();
    storage
        .mark_catalog_reconciled(&owner, 2, &digest)
        .await
        .unwrap();
    let instance = hub
        .materialize(owner.clone(), first.id, instance_limits(), builtins())
        .await
        .unwrap();
    assert_eq!(instance.sequence_id, first.sequence_id);
    let digest = "e".repeat(64);
    storage
        .accept_catalog_manifest(&owner, &[], 3, &digest)
        .await
        .unwrap();
    storage
        .remove_catalog_workflow(&owner, "flow", 3, &digest)
        .await
        .unwrap();
    storage
        .mark_catalog_reconciled(&owner, 3, &digest)
        .await
        .unwrap();
    assert!(
        hub.materialize(owner.clone(), second.id, instance_limits(), builtins())
            .await
            .is_err()
    );
    assert_eq!(
        hub.materialize(owner, first.id, instance_limits(), builtins())
            .await
            .unwrap()
            .id,
        first.id
    );
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn account_retirement_preserves_dispatched_evidence_while_the_host_is_still_running() {
    use crate::{HandlerError, StepHandler};
    use orch8_types::{continuity::EffectState, instance::InstanceState};
    struct Held {
        entered: Arc<tokio::sync::Notify>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        calls: Arc<AtomicUsize>,
    }
    impl StepHandler for Held {
        fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(30))
                .unwrap();
            Ok("{}".into())
        }
    }
    let (storage, sqlite) = fixture().await;
    let owner = owner_with_work(&storage, "alice", "one").await;
    let backend: Arc<dyn StorageBackend> = sqlite.clone();
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let tenant = super::super::workflows::owner(&owner).unwrap().0;
    backend
        .create_credential(&credential("held", tenant.as_str(), "HELD_PRIVATE_CANARY"))
        .await
        .unwrap();
    storage
        .grant_catalog_credential(&owner, "shared", "held")
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release, receiver) = std::sync::mpsc::channel();
    let calls = Arc::new(AtomicUsize::new(0));
    hub.bind(
        &owner,
        "native",
        Arc::new(Held {
            entered: entered.clone(),
            release: std::sync::Mutex::new(receiver),
            calls: calls.clone(),
        }),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    let run = storage
        .prepare_catalog_run(&owner, request(Some("held-retirement"), "{}"), limits())
        .await
        .unwrap();
    hub.materialize(owner.clone(), run.id, instance_limits(), builtins())
        .await
        .unwrap();
    let mut registry = orch8_engine::handlers::HandlerRegistry::new();
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    let task_store = backend.clone();
    let task = tokio::spawn(async move {
        tick(&task_store, &Arc::new(registry)).await;
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let ledger = backend
        .get_continuity_execution_by_instance(&tenant, run.id)
        .await
        .unwrap()
        .unwrap();
    let before = backend
        .list_effect_receipts(&tenant, ledger.continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].state, EffectState::Dispatched);
    storage.retire_account_scope(&owner.account).await.unwrap();
    assert_eq!(
        backend.get_instance(run.id).await.unwrap().unwrap().state,
        InstanceState::Cancelled
    );
    assert_eq!(hub.account_gate(&owner).unwrap().available_permits(), 0);
    let after = backend
        .list_effect_receipts(&tenant, ledger.continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].id, before[0].id);
    assert_eq!(after[0].state, EffectState::Dispatched);
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        backend.get_instance(run.id).await.unwrap().unwrap().state,
        InstanceState::Cancelled
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}
