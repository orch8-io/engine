//! Public legacy lifecycle operations must not adopt catalog-owned records.
use super::tests::fixture;
use crate::{
    MobileError, lifecycle::InstanceLifecycleManager, storage::scopes::test_support::scoped_run,
};
use orch8_storage::InstanceStore;
use orch8_types::instance::InstanceState;

#[tokio::test]
async fn legacy_lifecycle_cannot_read_or_list_four_scoped_runs() {
    let (mobile, sqlite) = fixture().await;
    let lifecycle = InstanceLifecycleManager::new(sqlite.clone(), mobile.clone(), 100);
    for account in ["alice", "bob"] {
        for catalog in ["one", "two"] {
            let id = scoped_run(&mobile, account, catalog).await;
            assert!(
                matches!(
                    lifecycle.get_instance(&id.to_string()).await,
                    Err(MobileError::NotFound { .. })
                ),
                "legacy get exposed scoped input"
            );
        }
    }
    assert!(
        lifecycle.active_instances().await.unwrap().is_empty(),
        "legacy list adopted scoped runs"
    );
}

#[tokio::test]
async fn legacy_lifecycle_cannot_cancel_a_scoped_run() {
    let (mobile, sqlite) = fixture().await;
    let lifecycle = InstanceLifecycleManager::new(sqlite.clone(), mobile.clone(), 100);
    let id = scoped_run(&mobile, "alice", "one").await;
    assert!(
        matches!(
            lifecycle.cancel_instance(&id.to_string()).await,
            Err(MobileError::NotFound { .. })
        ),
        "legacy cancel adopted scoped authority"
    );
    assert_eq!(
        sqlite.get_instance(id).await.unwrap().unwrap().state,
        InstanceState::Scheduled
    );
}

#[tokio::test]
async fn legacy_lifecycle_cannot_merge_approval_output_into_a_scoped_wait() {
    let (mobile, sqlite) = fixture().await;
    let lifecycle = InstanceLifecycleManager::new(sqlite.clone(), mobile.clone(), 100);
    let id = scoped_run(&mobile, "alice", "one").await;
    assert!(
        sqlite
            .conditional_update_instance_state(
                id,
                InstanceState::Scheduled,
                InstanceState::Waiting,
                None
            )
            .await
            .unwrap()
    );
    assert!(
        matches!(
            lifecycle
                .complete_step(&id.to_string(), "work", r#"{"foreign":"INJECTED"}"#)
                .await,
            Err(MobileError::NotFound { .. })
        ),
        "legacy approval changed scoped state"
    );
    let instance = sqlite.get_instance(id).await.unwrap().unwrap();
    assert_eq!(instance.state, InstanceState::Waiting);
    assert!(instance.context.data.get("foreign").is_none());
}

async fn owner(
    mobile: &crate::storage::MobileStorage,
    account: &str,
    catalog: &str,
) -> super::CatalogScope {
    let account = mobile
        .open_account_scope(account, "issuer", account)
        .await
        .unwrap();
    mobile
        .open_catalog_scope(&account, catalog, "source", "authority")
        .await
        .unwrap()
}

#[tokio::test]
async fn scoped_run_views_read_and_cancel_only_their_original_account_and_catalog() {
    let (mobile, sqlite) = fixture().await;
    let hub = crate::handlers::scoped::CatalogHandlers::isolated(mobile.clone(), sqlite.clone());
    let mut runs = Vec::new();
    for account in ["alice", "bob"] {
        for catalog in ["one", "two"] {
            let id = scoped_run(&mobile, account, catalog).await;
            let scope = owner(&mobile, account, catalog).await;
            runs.push((scope, id));
        }
    }
    for (index, (scope, id)) in runs.iter().enumerate() {
        let own = hub
            .run_access(scope, *id)
            .snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(own.context.data["private"], "SCOPED_CANARY");
        for (other, (_, target)) in runs.iter().enumerate() {
            if index != other {
                let foreign = hub.run_access(scope, *target);
                assert!(foreign.snapshot().await.unwrap().is_none());
                assert!(foreign.cancel().await.unwrap().is_none());
            }
        }
    }
    assert_eq!(
        hub.run_access(&runs[0].0, runs[0].1)
            .cancel()
            .await
            .unwrap(),
        Some(InstanceState::Cancelled)
    );
    for (_, id) in runs.iter().skip(1) {
        assert_eq!(
            sqlite.get_instance(*id).await.unwrap().unwrap().state,
            InstanceState::Scheduled
        );
    }
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn scoped_run_history_survives_reopen_but_old_session_views_lose_authority() {
    let (mobile, sqlite) = fixture().await;
    let id = scoped_run(&mobile, "alice", "one").await;
    let scope = owner(&mobile, "alice", "one").await;
    let hub = crate::handlers::scoped::CatalogHandlers::isolated(mobile.clone(), sqlite.clone());
    let stale = hub.run_access(&scope, id);
    mobile.retire_account_scope(&scope.account).await.unwrap();
    let fresh = owner(&mobile, "alice", "one").await;
    assert!(fresh.epoch() > scope.epoch());
    assert!(stale.snapshot().await.unwrap().is_none());
    assert!(stale.cancel().await.unwrap().is_none());
    let history = hub
        .run_access(&fresh, id)
        .snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.state, InstanceState::Cancelled);
    assert_eq!(history.context.data["private"], "SCOPED_CANARY");
    assert_eq!(
        hub.run_access(&fresh, id).cancel().await.unwrap(),
        Some(InstanceState::Cancelled)
    );
    // Model an old execution row restored from external backup/admin repair.
    // History access must not grant the fresh session execution authority.
    sqlite
        .update_instance_state(id, InstanceState::Scheduled, None)
        .await
        .unwrap();
    let before = sqlite.get_instance(id).await.unwrap().unwrap();
    assert_eq!(
        hub.run_access(&fresh, id)
            .snapshot()
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Scheduled
    );
    assert!(hub.run_access(&fresh, id).cancel().await.unwrap().is_none());
    let after = sqlite.get_instance(id).await.unwrap().unwrap();
    assert_eq!(after.state, before.state);
    assert_eq!(after.updated_at, before.updated_at);
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn closing_a_scoped_view_or_runtime_does_not_retire_the_account_or_cancel_work() {
    let (mobile, sqlite) = fixture().await;
    let id = scoped_run(&mobile, "alice", "one").await;
    let scope = owner(&mobile, "alice", "one").await;
    let hub = crate::handlers::scoped::CatalogHandlers::isolated(mobile.clone(), sqlite.clone());
    let closed = hub.run_access(&scope, id);
    let live = hub.run_access(&scope, id);
    closed.close();
    assert!(matches!(
        closed.snapshot().await,
        Err(MobileError::Shutdown)
    ));
    assert!(matches!(closed.cancel().await, Err(MobileError::Shutdown)));
    assert_eq!(
        live.snapshot().await.unwrap().unwrap().state,
        InstanceState::Scheduled
    );
    hub.shutdown();
    assert!(matches!(live.snapshot().await, Err(MobileError::Shutdown)));
    let next = hub.sibling();
    assert_eq!(
        next.run_access(&scope, id)
            .snapshot()
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Scheduled
    );
    let detached = next.run_access(&scope, id);
    drop(next);
    assert!(
        matches!(detached.snapshot().await, Err(MobileError::Shutdown)),
        "a view must not retain its runtime through the storage pool"
    );
}

#[tokio::test]
async fn scoped_snapshot_uses_the_encryption_decorator_after_owner_authorization() {
    use orch8_storage::{StorageBackend, encrypting::EncryptingStorage};
    use orch8_types::encryption::FieldEncryptor;
    use std::sync::Arc;
    let (mobile, sqlite) = fixture().await;
    let id = scoped_run(&mobile, "alice", "one").await;
    let scope = owner(&mobile, "alice", "one").await;
    let backend: Arc<dyn StorageBackend> = Arc::new(EncryptingStorage::new(
        sqlite.clone(),
        FieldEncryptor::from_bytes(&[13; 32]),
    ));
    let mut context = sqlite.get_instance(id).await.unwrap().unwrap().context;
    context.data = serde_json::json!({"private":"ENCRYPTED_SCOPED_CANARY"});
    backend.update_instance_context(id, &context).await.unwrap();
    assert!(
        !sqlite
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .context
            .data
            .to_string()
            .contains("ENCRYPTED_SCOPED_CANARY")
    );
    let hub = crate::handlers::scoped::CatalogHandlers::isolated(mobile.clone(), backend);
    assert_eq!(
        hub.run_access(&scope, id)
            .snapshot()
            .await
            .unwrap()
            .unwrap()
            .context
            .data["private"],
        "ENCRYPTED_SCOPED_CANARY"
    );
    assert_eq!(
        hub.run_access(&scope, id).cancel().await.unwrap(),
        Some(InstanceState::Cancelled)
    );
    assert_eq!(
        hub.run_access(&scope, id)
            .snapshot()
            .await
            .unwrap()
            .unwrap()
            .context
            .data["private"],
        "ENCRYPTED_SCOPED_CANARY"
    );
    let foreign = owner(&mobile, "bob", "one").await;
    assert!(
        hub.run_access(&foreign, id)
            .snapshot()
            .await
            .unwrap()
            .is_none()
    );
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn scoped_authority_is_checked_after_a_pending_read_or_cancel_reaches_storage() {
    use orch8_storage::{StorageBackend, testing::faulty::FaultStorage};
    use std::{sync::Arc, time::Duration};
    for method in ["get_mobile_instance", "cancel_mobile_instance"] {
        let (mobile, sqlite) = fixture().await;
        let id = scoped_run(&mobile, "alice", "one").await;
        let scope = owner(&mobile, "alice", "one").await;
        let (faulted, faults) = FaultStorage::wrap(sqlite.clone());
        let backend: Arc<dyn StorageBackend> = faulted;
        let hub = crate::handlers::scoped::CatalogHandlers::isolated(mobile.clone(), backend);
        let view = hub.run_access(&scope, id);
        let pause = faults.on(method).nth(1).pause();
        let task = tokio::spawn(async move {
            if method == "get_mobile_instance" {
                view.snapshot().await.map(|row| row.map(|_| ()))
            } else {
                view.cancel().await.map(|row| row.map(|_| ()))
            }
        });
        tokio::time::timeout(Duration::from_secs(3), pause.entered())
            .await
            .unwrap();
        mobile.retire_account_scope(&scope.account).await.unwrap();
        let fresh = owner(&mobile, "alice", "one").await;
        assert!(fresh.epoch() > scope.epoch());
        let retired = sqlite.get_instance(id).await.unwrap().unwrap();
        pause.release();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert_eq!(
            sqlite.get_instance(id).await.unwrap().unwrap().updated_at,
            retired.updated_at,
            "stale action wrote after retirement"
        );
        for receipt in hub.close() {
            receipt.await.unwrap();
        }
    }
}

#[tokio::test]
async fn scoped_pending_read_does_not_publish_into_a_closed_view_or_runtime() {
    use orch8_storage::{StorageBackend, testing::faulty::FaultStorage};
    use std::{sync::Arc, time::Duration};
    for runtime_close in [false, true] {
        let (mobile, sqlite) = fixture().await;
        let id = scoped_run(&mobile, "alice", "one").await;
        let scope = owner(&mobile, "alice", "one").await;
        let (faulted, faults) = FaultStorage::wrap(sqlite.clone());
        let backend: Arc<dyn StorageBackend> = faulted;
        let hub = crate::handlers::scoped::CatalogHandlers::isolated(mobile.clone(), backend);
        let view = Arc::new(hub.run_access(&scope, id));
        let pause = faults.on("get_mobile_instance").nth(1).pause();
        let task = tokio::spawn({
            let view = view.clone();
            async move { view.snapshot().await }
        });
        tokio::time::timeout(Duration::from_secs(3), pause.entered())
            .await
            .unwrap();
        if runtime_close {
            hub.shutdown();
        } else {
            view.close();
        }
        pause.release();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap(),
            Err(MobileError::Shutdown)
        ));
        assert_eq!(
            sqlite.get_instance(id).await.unwrap().unwrap().state,
            InstanceState::Scheduled
        );
        assert_eq!(owner(&mobile, "alice", "one").await.epoch(), scope.epoch());
        for receipt in hub.close() {
            receipt.await.unwrap();
        }
    }
}

#[tokio::test]
async fn scoped_access_refuses_inconsistent_native_identity_and_untracked_materialization() {
    let (mobile, sqlite) = fixture().await;
    let id = scoped_run(&mobile, "alice", "one").await;
    let scope = owner(&mobile, "alice", "one").await;
    let hub = crate::handlers::scoped::CatalogHandlers::isolated(mobile.clone(), sqlite.clone());
    let original = sqlite.get_instance(id).await.unwrap().unwrap();
    for (column, invalid, restore) in [
        ("tenant_id", "mobile", original.tenant_id.to_string()),
        ("namespace", "foreign", original.namespace.to_string()),
        (
            "created_at",
            "invalid-date",
            original.created_at.to_rfc3339(),
        ),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE task_instances SET {column}=?2 WHERE id=?1"
        )))
        .bind(id.to_string())
        .bind(invalid)
        .execute(mobile.pool())
        .await
        .unwrap();
        assert!(
            hub.run_access(&scope, id)
                .snapshot()
                .await
                .unwrap()
                .is_none()
        );
        assert!(hub.run_access(&scope, id).cancel().await.unwrap().is_none());
        let lifecycle = InstanceLifecycleManager::new(sqlite.clone(), mobile.clone(), 100);
        assert!(matches!(
            lifecycle.get_instance(&id.to_string()).await,
            Err(MobileError::NotFound { .. })
        ));
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE task_instances SET {column}=?2 WHERE id=?1"
        )))
        .bind(id.to_string())
        .bind(restore)
        .execute(mobile.pool())
        .await
        .unwrap();
    }
    assert!(
        hub.run_access(&scope, id)
            .snapshot()
            .await
            .unwrap()
            .is_some()
    );
    sqlx::query("DELETE FROM mobile_catalog_run_materializations WHERE request_id=?1")
        .bind(id.to_string())
        .execute(mobile.pool())
        .await
        .unwrap();
    assert!(
        hub.run_access(&scope, id)
            .snapshot()
            .await
            .unwrap()
            .is_none()
    );
    assert!(hub.run_access(&scope, id).cancel().await.unwrap().is_none());
    assert_eq!(
        sqlite.get_instance(id).await.unwrap().unwrap().state,
        InstanceState::Scheduled
    );
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn legacy_listener_callbacks_do_not_deliver_catalog_run_data_or_pending_steps() {
    use crate::EngineListener;
    use orch8_engine::sequence_cache::SequenceCache;
    use orch8_storage::{
        SequenceStore, StorageBackend,
        testing::fixtures::{InstanceBuilder, SequenceBuilder},
    };
    use orch8_types::ids::BlockId;
    use std::{sync::Arc, time::Duration};
    struct Listener(tokio::sync::mpsc::UnboundedSender<(String, String)>);
    impl EngineListener for Listener {
        fn on_instance_completed(&self, id: String, output: String) {
            let _ = self.0.send((id, output));
        }
        fn on_instance_failed(&self, id: String, error: String) {
            let _ = self.0.send((id, error));
        }
        fn on_step_pending(&self, id: String, step: String, handler: String) {
            let _ = self.0.send((id, format!("{step}:{handler}")));
        }
    }
    for waiting in [false, true] {
        let (mobile, sqlite) = fixture().await;
        let backend: Arc<dyn StorageBackend> = sqlite.clone();
        let state = if waiting {
            InstanceState::Waiting
        } else {
            InstanceState::Completed
        };
        for account in ["alice", "bob"] {
            for catalog in ["one", "two"] {
                let id = scoped_run(&mobile, account, catalog).await;
                let mut context = sqlite.get_instance(id).await.unwrap().unwrap().context;
                context.runtime.current_step = Some(BlockId::new("work"));
                sqlite.update_instance_context(id, &context).await.unwrap();
                sqlite.update_instance_state(id, state, None).await.unwrap();
            }
        }
        let corrupt = scoped_run(&mobile, "corrupt", "one").await;
        sqlx::query("UPDATE task_instances SET context='invalid-json',state=?2 WHERE id=?1")
            .bind(corrupt.to_string())
            .bind(state.to_string())
            .execute(mobile.pool())
            .await
            .unwrap();
        let sequence = SequenceBuilder::new("mobile")
            .name("legacy")
            .step("work", "noop")
            .build();
        sqlite.create_sequence(&sequence).await.unwrap();
        let mut legacy = InstanceBuilder::for_sequence(&sequence)
            .state(state)
            .data(serde_json::json!({"legacy":"VISIBLE"}))
            .build();
        legacy.context.runtime.current_step = Some(BlockId::new("work"));
        sqlite.create_instance(&legacy).await.unwrap();
        let notifier = crate::notifier::MobileNotifier::new();
        let (send, mut received) = tokio::sync::mpsc::unbounded_channel();
        notifier.set_listener(Arc::new(Listener(send)));
        if waiting {
            notifier
                .fire_step_pending_events(
                    &backend,
                    &mobile,
                    &Arc::new(SequenceCache::new(16, Duration::from_secs(30))),
                )
                .await;
        } else {
            notifier.fire_terminal_events(&backend, &mobile).await;
        }
        let (id, payload) = tokio::time::timeout(Duration::from_secs(3), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            id,
            legacy.id.to_string(),
            "global legacy listener received a catalog-owned event"
        );
        assert!(!payload.contains("SCOPED_CANARY"));
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            received.try_recv().is_err(),
            "additional foreign events were delivered"
        );
        drop(notifier.close());
    }
}
