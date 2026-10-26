use super::*;
use crate::{
    MobileError,
    handlers::scoped::CatalogHandlers,
    storage::scopes::{
        runs::{
            CatalogRunLimits, CatalogRunRequest,
            materialize::{InstanceLimits, Preparation},
        },
        test_support::scoped_run,
    },
};
use orch8_storage::{InstanceStore, ResourceStore, sqlite::SqliteStorage};
use std::collections::{HashMap, HashSet};

fn limits() -> CatalogRunLimits {
    CatalogRunLimits {
        global_records: 128,
        account_records: 64,
        catalog_records: 32,
        global_input_bytes: 2_097_152,
        account_input_bytes: 1_048_576,
        catalog_input_bytes: 1_048_576,
    }
}
async fn owner(storage: &MobileStorage, account: &str, catalog: &str) -> CatalogScope {
    let account = storage
        .open_account_scope(account, "issuer", account)
        .await
        .unwrap();
    storage
        .open_catalog_scope(&account, catalog, "source", "authority")
        .await
        .unwrap()
}
async fn prepared(storage: &MobileStorage, scope: &CatalogScope, key: &str) -> InstanceId {
    storage
        .prepare_catalog_run(
            scope,
            CatalogRunRequest {
                workflow: "flow",
                version: None,
                input_json: r#"{"private":"HISTORY_INPUT_CANARY","account_id":"forged"}"#,
                dedup_key: Some(key),
            },
            limits(),
        )
        .await
        .unwrap()
        .id
}
async fn materialize(storage: &MobileStorage, scope: &CatalogScope, id: InstanceId) {
    let Preparation::Pending(snapshot) = storage.prepare_materialization(scope, id).await.unwrap()
    else {
        panic!("pending")
    };
    let checked = snapshot
        .check(
            &HashSet::from(["noop".to_owned()]),
            env!("CARGO_PKG_VERSION"),
        )
        .unwrap();
    storage
        .commit_materialization(
            scope,
            checked,
            InstanceLimits {
                global: 128,
                account: 64,
                catalog: 32,
            },
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
}
async fn populate(
    storage: &MobileStorage,
    sqlite: &SqliteStorage,
    account: &str,
    catalog: &str,
) -> (CatalogScope, HashMap<InstanceId, CatalogNativeState>) {
    let live = scoped_run(storage, account, catalog).await;
    let scope = owner(storage, account, catalog).await;
    sqlite
        .update_instance_state(live, InstanceState::Waiting, None)
        .await
        .unwrap();
    let queued = prepared(storage, &scope, "queued").await;
    let cancelled = prepared(storage, &scope, "cancelled").await;
    assert!(storage.cancel_catalog_run(&scope, cancelled).await.unwrap());
    let purged = prepared(storage, &scope, "purged").await;
    materialize(storage, &scope, purged).await;
    sqlite
        .update_instance_state(purged, InstanceState::Completed, None)
        .await
        .unwrap();
    assert_eq!(sqlite.purge_terminal_instances(&[purged]).await.unwrap(), 1);
    (
        scope,
        HashMap::from([
            (live, CatalogNativeState::Present(InstanceState::Waiting)),
            (queued, CatalogNativeState::NotMaterialized),
            (cancelled, CatalogNativeState::NotMaterialized),
            (purged, CatalogNativeState::Unavailable),
        ]),
    )
}

#[tokio::test]
async fn four_scopes_page_real_queued_live_cancelled_and_purged_history_without_private_data() {
    let (storage, sqlite) = super::super::tests::fixture().await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let mut scopes = Vec::new();
    for account in ["alice", "bob"] {
        for catalog in ["one", "two"] {
            scopes.push(populate(&storage, &sqlite, account, catalog).await);
        }
    }
    for (scope, expected) in &scopes {
        let view = hub.run_history(scope);
        let first = view.page(None, 2).await.unwrap();
        assert_eq!(first.items.len(), 2);
        let cursor = first.next.as_ref().unwrap();
        let second = view.page(Some(cursor), 2).await.unwrap();
        assert_eq!(second.items.len(), 2);
        assert!(second.next.is_none());
        let mut seen = HashSet::new();
        let mut previous = String::new();
        for item in first.items.iter().chain(&second.items) {
            assert!(item.id.to_string() > previous);
            previous = item.id.to_string();
            assert!(seen.insert(item.id));
            assert_eq!(item.account_id, scope.account_id());
            assert_eq!(item.catalog_id, scope.catalog_id());
            assert_eq!(item.owner_epoch, scope.epoch());
            assert_eq!(item.native_state, expected[&item.id]);
            assert_eq!(item.workflow, "flow");
        }
        assert_eq!(seen.len(), expected.len());
        assert_eq!(first, view.page(None, 2).await.unwrap());
        for (other, _) in &scopes {
            if other != scope {
                assert!(hub.run_history(other).page(Some(cursor), 2).await.is_err());
            }
        }
        for limit in [0, MAX_HISTORY_PAGE + 1] {
            assert!(view.page(None, limit).await.is_err());
        }
        let debug = format!("{first:?}{second:?}");
        assert!(!debug.contains("HISTORY_INPUT_CANARY"));
        assert!(!debug.contains("SCOPED_CANARY"));
        assert!(!debug.contains("forged"));
        assert_eq!(
            first
                .items
                .iter()
                .chain(&second.items)
                .filter(|item| item.request_state == CatalogRunState::Cancelled)
                .count(),
            1
        );
    }
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn history_reopen_keeps_original_execution_epochs_and_refuses_old_cursors() {
    let (storage, sqlite) = super::super::tests::fixture().await;
    let (scope, expected) = populate(&storage, &sqlite, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let old = hub.run_history(&scope);
    let first = old.page(None, 1).await.unwrap();
    let cursor = first.next.unwrap();
    storage.retire_account_scope(&scope.account).await.unwrap();
    let fresh = owner(&storage, "alice", "one").await;
    assert!(fresh.epoch() > scope.epoch());
    assert!(old.page(None, 8).await.is_err());
    let current = hub.run_history(&fresh);
    assert!(current.page(Some(&cursor), 8).await.is_err());
    let history = current.page(None, 8).await.unwrap();
    assert_eq!(history.items.len(), expected.len());
    for item in history.items {
        assert_eq!(item.owner_epoch, scope.epoch());
        if matches!(expected[&item.id], CatalogNativeState::Present(_)) {
            assert_eq!(
                item.native_state,
                CatalogNativeState::Present(InstanceState::Cancelled)
            );
        }
    }
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn history_never_returns_an_inconsistent_native_owners_state_or_reclassifies_it_as_queued() {
    let (storage, sqlite) = super::super::tests::fixture().await;
    let id = scoped_run(&storage, "alice", "one").await;
    let scope = owner(&storage, "alice", "one").await;
    let original = sqlite.get_instance(id).await.unwrap().unwrap();
    for (column, bad, restore) in [
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
        .bind(bad)
        .execute(storage.pool())
        .await
        .unwrap();
        assert_eq!(
            storage
                .catalog_run_history(&scope, None, 8)
                .await
                .unwrap()
                .items[0]
                .native_state,
            CatalogNativeState::Unavailable
        );
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE task_instances SET {column}=?2 WHERE id=?1"
        )))
        .bind(id.to_string())
        .bind(restore)
        .execute(storage.pool())
        .await
        .unwrap();
    }
    sqlx::query("DELETE FROM mobile_catalog_run_materializations WHERE request_id=?1")
        .bind(id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    assert_eq!(
        storage
            .catalog_run_history(&scope, None, 8)
            .await
            .unwrap()
            .items[0]
            .native_state,
        CatalogNativeState::Unavailable
    );
}

#[tokio::test]
async fn metadata_history_does_not_decode_private_input_or_native_context() {
    let (storage, _sqlite) = super::super::tests::fixture().await;
    let id = scoped_run(&storage, "alice", "one").await;
    let scope = owner(&storage, "alice", "one").await;
    let foreign = scoped_run(&storage, "bob", "one").await;
    for id in [id, foreign] {
        sqlx::query("UPDATE task_instances SET context='invalid-json' WHERE id=?1")
            .bind(id.to_string())
            .execute(storage.pool())
            .await
            .unwrap();
        sqlx::query(
            "UPDATE mobile_catalog_run_requests SET input='bad-json',input_bytes=8 WHERE id=?1",
        )
        .bind(id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    }
    let page = storage.catalog_run_history(&scope, None, 8).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, id);
    assert_eq!(
        page.items[0].native_state,
        CatalogNativeState::Present(InstanceState::Scheduled)
    );
}

#[tokio::test]
async fn history_view_close_and_runtime_close_do_not_retire_accounts_and_views_do_not_pin_runtime()
{
    let (storage, sqlite) = super::super::tests::fixture().await;
    scoped_run(&storage, "alice", "one").await;
    let scope = owner(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite);
    let closed = hub.run_history(&scope);
    let live = hub.run_history(&scope);
    closed.close();
    assert!(matches!(
        closed.page(None, 1).await,
        Err(MobileError::Shutdown { .. })
    ));
    assert_eq!(live.page(None, 1).await.unwrap().items.len(), 1);
    hub.shutdown();
    assert!(matches!(
        live.page(None, 1).await,
        Err(MobileError::Shutdown { .. })
    ));
    let sibling = hub.sibling();
    let detached = sibling.run_history(&scope);
    assert_eq!(detached.page(None, 1).await.unwrap().items.len(), 1);
    drop(sibling);
    assert!(matches!(
        detached.page(None, 1).await,
        Err(MobileError::Shutdown { .. })
    ));
    assert_eq!(owner(&storage, "alice", "one").await.epoch(), scope.epoch());
}

#[tokio::test]
async fn pending_history_page_releases_runtime_when_view_or_runtime_closes() {
    use std::{sync::Arc, time::Duration};
    for close_runtime in [false, true] {
        let (storage, sqlite) = super::super::tests::fixture().await;
        scoped_run(&storage, "alice", "one").await;
        let scope = owner(&storage, "alice", "one").await;
        let hub = CatalogHandlers::isolated(storage.clone(), sqlite);
        let view = Arc::new(hub.run_history(&scope));
        let mut held = Vec::new();
        for _ in 0..storage.pool().options().get_max_connections() {
            held.push(storage.pool().acquire().await.unwrap());
        }
        let (entered, pending) = tokio::sync::oneshot::channel();
        let page = tokio::spawn({
            let view = view.clone();
            async move {
                entered.send(()).unwrap();
                view.page(None, 1).await
            }
        });
        // On this current-thread runtime the task reaches pool acquisition
        // before the receiver resumes; every physical connection is held above.
        pending.await.unwrap();
        if close_runtime {
            hub.shutdown();
        } else {
            view.close();
        }
        let weak = Arc::downgrade(&hub);
        drop(hub);
        let result = tokio::time::timeout(Duration::from_secs(2), page)
            .await
            .expect("a closed view must not wait for a pooled connection")
            .unwrap();
        assert!(matches!(result, Err(MobileError::Shutdown { .. })));
        assert!(weak.upgrade().is_none());
        drop(held);
        assert_eq!(owner(&storage, "alice", "one").await.epoch(), scope.epoch());
    }
}

#[tokio::test]
async fn pending_history_read_checks_retirement_on_an_independent_file_connection() {
    use std::{sync::Arc, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.db");
    let path = path.to_str().unwrap();
    let sqlite = Arc::new(SqliteStorage::file_mobile(path).await.unwrap());
    let storage = Arc::new(MobileStorage::new(sqlite.clone()));
    scoped_run(&storage, "alice", "one").await;
    let scope = owner(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let view = hub.run_history(&scope);
    // A second administrative pool proves the session check occurs after pool
    // acquisition. This is not a second executing engine/recovery owner.
    let admin_sqlite = Arc::new(SqliteStorage::file_mobile(path).await.unwrap());
    let admin = MobileStorage::new(admin_sqlite.clone());
    let mut held = Vec::new();
    for _ in 0..storage.pool().options().get_max_connections() {
        held.push(storage.pool().acquire().await.unwrap());
    }
    let (entered, pending) = tokio::sync::oneshot::channel();
    let page = tokio::spawn(async move {
        entered.send(()).unwrap();
        view.page(None, 1).await
    });
    pending.await.unwrap();
    admin.retire_account_scope(&scope.account).await.unwrap();
    let fresh = owner(&admin, "alice", "one").await;
    assert!(fresh.epoch() > scope.epoch());
    drop(held);
    assert!(
        tokio::time::timeout(Duration::from_secs(3), page)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_eq!(
        hub.run_history(&fresh)
            .page(None, 8)
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
    sqlite.close().await;
    admin_sqlite.close().await;
}

#[tokio::test]
async fn history_empty_exact_boundary_and_future_epoch_rows_are_bounded() {
    let (storage, _sqlite) = super::super::tests::fixture().await;
    scoped_run(&storage, "alice", "one").await;
    let scope = owner(&storage, "alice", "one").await;
    let empty = owner(&storage, "alice", "empty").await;
    let page = storage.catalog_run_history(&empty, None, 1).await.unwrap();
    assert!(page.items.is_empty());
    assert!(page.next.is_none());
    let future = prepared(&storage, &scope, "future").await;
    sqlx::query("UPDATE mobile_catalog_run_requests SET owner_epoch=?2 WHERE id=?1")
        .bind(future.to_string())
        .bind(scope.epoch() + 1)
        .execute(storage.pool())
        .await
        .unwrap();
    let page = storage.catalog_run_history(&scope, None, 1).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_ne!(page.items[0].id, future);
    assert!(page.next.is_none());
}
