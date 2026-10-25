//! Cross-runtime registration limits must retain physical ownership.
use super::tests::{fixture, scope};
use crate::{
    HandlerError, StepHandler, handlers::scoped::CatalogHandlers, storage::scopes::CatalogScope,
};
use orch8_types::error::StepError;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

struct Idle;
impl StepHandler for Idle {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        Ok("{}".into())
    }
}
async fn bind(hub: &CatalogHandlers, owner: &CatalogScope, name: &str) -> Result<(), StepError> {
    hub.bind(owner, name, Arc::new(Idle), Duration::from_secs(5))
        .await
}

#[tokio::test]
async fn sibling_router_cannot_multiply_catalog_registration_allowance() {
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite);
    let successor = hub.sibling();
    for index in 0..32 {
        bind(&hub, &owner, &format!("native-{index}"))
            .await
            .unwrap();
    }
    let overlap = bind(&successor, &owner, "new-runtime").await;
    drop(hub);
    if overlap.is_err() {
        bind(&successor, &owner, "new-runtime").await.unwrap();
    }
    assert!(
        overlap.is_err(),
        "a replacement runtime multiplied one catalog's registration allowance"
    );
}

#[tokio::test]
async fn account_registration_allowance_survives_router_and_epoch_replacement() {
    let (storage, sqlite) = fixture().await;
    let one = scope(&storage, "alice", "one").await;
    let two = scope(&storage, "alice", "two").await;
    let three = scope(&storage, "alice", "three").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite);
    let successor = hub.sibling();
    for owner in [&one, &two] {
        for index in 0..32 {
            bind(&hub, owner, &format!("native-{index}")).await.unwrap();
        }
    }
    let live_overlap = bind(&successor, &three, "new-runtime").await;
    storage.retire_account_scope(&one.account).await.unwrap();
    let reopened = scope(&storage, "alice", "three").await;
    let epoch_overlap = bind(&successor, &reopened, "new-epoch").await;
    hub.release_retired(&one).await.unwrap().await.unwrap();
    if epoch_overlap.is_err() {
        bind(&successor, &reopened, "new-epoch").await.unwrap();
    }
    assert!(
        live_overlap.is_err(),
        "another router bypassed the stable account allowance"
    );
    assert!(
        epoch_overlap.is_err(),
        "a new login reclaimed still-owned old registration capacity"
    );
}

#[tokio::test]
async fn global_registration_allowance_is_shared_across_router_replacements() {
    let (storage, sqlite) = fixture().await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite);
    for index in 0..8 {
        let owner = scope(&storage, &format!("account-{index}"), "one").await;
        for handler in 0..32 {
            bind(&hub, &owner, &format!("native-{handler}"))
                .await
                .unwrap();
        }
    }
    let successor = hub.sibling();
    let owner = scope(&storage, "another-account", "one").await;
    let overlap = bind(&successor, &owner, "new-runtime").await;
    drop(hub);
    if overlap.is_err() {
        bind(&successor, &owner, "new-runtime").await.unwrap();
    }
    assert!(
        overlap.is_err(),
        "another router bypassed the process registration allowance"
    );
}

struct HeldDrop {
    entered: Arc<tokio::sync::Notify>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl StepHandler for HeldDrop {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        panic!("retired idle callback cannot execute")
    }
}
impl Drop for HeldDrop {
    fn drop(&mut self) {
        self.entered.notify_one();
        let _ = self
            .release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_router_cannot_reclaim_a_registration_during_foreign_destruction() {
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite);
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release, held) = std::sync::mpsc::channel();
    hub.bind(
        &owner,
        "held",
        Arc::new(HeldDrop {
            entered: entered.clone(),
            release: Mutex::new(held),
        }),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    storage.retire_account_scope(&owner.account).await.unwrap();
    let finished = hub.release_retired(&owner).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let successor = hub.sibling();
    let reopened = scope(&storage, "alice", "one").await;
    for index in 0..31 {
        bind(&successor, &reopened, &format!("new-{index}"))
            .await
            .unwrap();
    }
    let overlap = bind(&successor, &reopened, "last").await;
    let bob = scope(&storage, "bob", "one").await;
    bind(&successor, &bob, "other-account").await.unwrap();
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), finished)
        .await
        .unwrap()
        .unwrap();
    if overlap.is_err() {
        bind(&successor, &reopened, "last").await.unwrap();
    }
    assert!(
        overlap.is_err(),
        "a replacement runtime reclaimed a physically live destructor's registration"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_routers_cannot_overbook_the_last_catalog_slot() {
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite);
    for index in 0..31 {
        bind(&hub, &owner, &format!("native-{index}"))
            .await
            .unwrap();
    }
    let left = hub.sibling();
    let right = hub.sibling();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = Vec::new();
    for router in [left.clone(), right.clone()] {
        let owner = owner.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            bind(&router, &owner, "last").await
        }));
    }
    let mut results = Vec::new();
    for task in tasks {
        results.push(task.await.unwrap());
    }
    assert_eq!(
        results.iter().filter(|r| r.is_ok()).count(),
        1,
        "{results:?}"
    );
    if results[0].is_ok() {
        drop(left);
        bind(&right, &owner, "last").await.unwrap();
    } else {
        drop(right);
        bind(&left, &owner, "last").await.unwrap();
    }
}

#[tokio::test]
async fn refused_registration_rolls_back_other_scope_and_process_charges() {
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite);
    for index in 0..32 {
        bind(&hub, &owner, &format!("native-{index}"))
            .await
            .unwrap();
    }
    let successor = hub.sibling();
    for index in 0..260 {
        assert!(
            bind(&successor, &owner, &format!("refused-{index}"))
                .await
                .is_err()
        );
    }
    let another_catalog = scope(&storage, "alice", "two").await;
    for index in 0..32 {
        bind(&successor, &another_catalog, &format!("other-{index}"))
            .await
            .unwrap();
    }
    let another_account = scope(&storage, "bob", "one").await;
    bind(&successor, &another_account, "unrelated")
        .await
        .unwrap();
}

#[tokio::test]
async fn production_router_construction_shares_registration_ownership() {
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let hub = CatalogHandlers::new(
        storage.clone(),
        sqlite.clone(),
        tokio_util::sync::CancellationToken::new(),
    );
    let successor = CatalogHandlers::new(
        storage.clone(),
        sqlite,
        tokio_util::sync::CancellationToken::new(),
    );
    for index in 0..32 {
        bind(&hub, &owner, &format!("native-{index}"))
            .await
            .unwrap();
    }
    let overlap = bind(&successor, &owner, "another-router").await;
    drop(hub);
    if overlap.is_err() {
        bind(&successor, &owner, "another-router").await.unwrap();
    }
    assert!(
        overlap.is_err(),
        "production constructors selected independent registration budgets"
    );
}
