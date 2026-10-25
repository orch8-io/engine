//! Real scheduler callbacks carry frozen ownership independently of user JSON.
use super::{
    materialize_tests::{builtins, instance_limits, owner_with_params, tick},
    tests::{fixture, limits, request},
};
use crate::{
    HandlerError,
    handlers::scoped::{
        CatalogHandlers,
        context::{CatalogCallbackContext, CatalogStepHandler},
    },
};
use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::StorageBackend;
use orch8_types::{continuity::EffectState, instance::InstanceState};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct SharedCallback(Mutex<Vec<(CatalogCallbackContext, serde_json::Value)>>);
impl CatalogStepHandler for SharedCallback {
    fn execute(
        &self,
        context: CatalogCallbackContext,
        name: String,
        input: String,
    ) -> Result<String, HandlerError> {
        assert_eq!(name, "native");
        self.0
            .lock()
            .unwrap()
            .push((context, serde_json::from_str(&input).unwrap()));
        Ok("{\"completed\":true}".into())
    }
}

#[tokio::test]
async fn contextual_callback_uses_frozen_owner_and_real_effect_for_all_four_scopes() {
    let (storage, sqlite) = fixture().await;
    let backend: Arc<dyn StorageBackend> = sqlite;
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let shared = Arc::new(SharedCallback::default());
    let mut expected = Vec::new();
    for account in ["alice", "bob"] {
        for catalog in ["one", "two"] {
            let owner=owner_with_params(&storage,account,catalog,serde_json::json!({"account_id":"FORGED_ACCOUNT","catalog_id":"FORGED_CATALOG","run_id":"FORGED_RUN","owner_epoch":999,"effect_id":"FORGED_EFFECT","attempt":999})).await;
            hub.bind_contextual(&owner, "native", shared.clone(), Duration::from_secs(5))
                .await
                .unwrap();
            let run = storage
                .prepare_catalog_run(&owner, request(Some("same-operation"), "{}"), limits())
                .await
                .unwrap();
            let instance = hub
                .materialize(owner.clone(), run.id, instance_limits(), builtins())
                .await
                .unwrap();
            expected.push((owner, instance));
        }
    }
    let mut registry = HandlerRegistry::new();
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    let registry = Arc::new(registry);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tick(&backend, &registry).await;
            let mut complete = true;
            for (_, instance) in &expected {
                complete &= backend
                    .get_instance(instance.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .state
                    == InstanceState::Completed;
            }
            if complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let calls = shared.0.lock().unwrap().clone();
    assert_eq!(
        calls.len(),
        4,
        "one shared callback must receive each scoped run exactly once"
    );
    for (owner, instance) in expected {
        let (context, input) = calls
            .iter()
            .find(|(context, _)| context.run_id() == instance.id)
            .unwrap();
        assert_eq!(context.account_id(), owner.account_id());
        assert_eq!(context.catalog_id(), owner.catalog_id());
        assert_eq!(context.owner_epoch(), owner.epoch());
        assert_eq!(context.block_id().as_str(), "work");
        assert_eq!(context.attempt(), 0);
        assert_eq!(input["account_id"], "FORGED_ACCOUNT");
        assert_eq!(input["owner_epoch"], 999);
        let receipts = backend
            .list_instance_effect_receipts(&instance.tenant_id, instance.id, 8)
            .await
            .unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(context.effect_id(), receipts[0].id);
        assert_eq!(receipts[0].state, EffectState::Committed);
        assert_eq!(receipts[0].attempt, context.attempt());
    }
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

struct HeldCallback {
    entered: Arc<tokio::sync::Notify>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    seen: Mutex<Option<CatalogCallbackContext>>,
}
impl CatalogStepHandler for HeldCallback {
    fn execute(
        &self,
        context: CatalogCallbackContext,
        _: String,
        _: String,
    ) -> Result<String, HandlerError> {
        *self.seen.lock().unwrap() = Some(context);
        self.entered.notify_one();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        Ok("{\"late_host_output\":true}".into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contextual_callback_keeps_original_identity_after_account_retirement_and_reopen() {
    let (storage, sqlite) = fixture().await;
    let backend: Arc<dyn StorageBackend> = sqlite;
    let owner = owner_with_params(&storage, "alice", "one", serde_json::json!({})).await;
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release, receiver) = std::sync::mpsc::channel();
    let held = Arc::new(HeldCallback {
        entered: entered.clone(),
        release: Mutex::new(receiver),
        seen: Mutex::new(None),
    });
    hub.bind_contextual(&owner, "native", held.clone(), Duration::from_secs(10))
        .await
        .unwrap();
    let run = storage
        .prepare_catalog_run(&owner, request(Some("old"), "{}"), limits())
        .await
        .unwrap();
    let instance = hub
        .materialize(owner.clone(), run.id, instance_limits(), builtins())
        .await
        .unwrap();
    let mut registry = HandlerRegistry::new();
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    let registry = Arc::new(registry);
    let task = tokio::spawn({
        let backend = backend.clone();
        let registry = registry.clone();
        async move {
            tick(&backend, &registry).await;
        }
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    storage.retire_account_scope(&owner.account).await.unwrap();
    let retirement = hub.close_retired(&owner).await.unwrap();
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    let fresh = storage
        .open_catalog_scope(&account, "one", "source", "authority")
        .await
        .unwrap();
    assert!(fresh.epoch() > owner.epoch());
    let before = held.seen.lock().unwrap().clone().unwrap();
    assert_eq!(before.account_id(), fresh.account_id());
    assert_eq!(before.owner_epoch(), owner.epoch());
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), retirement)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.seen.lock().unwrap().as_ref().unwrap(), &before);
    let old = backend.get_instance(run.id).await.unwrap().unwrap();
    assert_eq!(old.state, InstanceState::Cancelled);
    assert!(old.context.data.get("late_host_output").is_none());
    let receipts = backend
        .list_instance_effect_receipts(&instance.tenant_id, run.id, 8)
        .await
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].id, before.effect_id());
    assert_ne!(
        receipts[0].state,
        EffectState::Committed,
        "late host result published after retirement"
    );
    let shared = Arc::new(SharedCallback::default());
    hub.bind_contextual(&fresh, "native", shared.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    let new = storage
        .prepare_catalog_run(&fresh, request(Some("new"), "{}"), limits())
        .await
        .unwrap();
    hub.materialize(fresh.clone(), new.id, instance_limits(), builtins())
        .await
        .unwrap();
    tick(&backend, &registry).await;
    let calls = shared.0.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0.owner_epoch(), fresh.epoch());
    assert_eq!(calls[0].0.run_id(), new.id);
    assert_ne!(calls[0].0.effect_id(), before.effect_id());
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn contextual_callback_requires_durable_effect_admission_not_just_a_running_claim() {
    let (storage, sqlite) = fixture().await;
    let backend: Arc<dyn StorageBackend> = sqlite.clone();
    let owner = owner_with_params(&storage, "alice", "one", serde_json::json!({})).await;
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let shared = Arc::new(SharedCallback::default());
    hub.bind_contextual(&owner, "native", shared.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    let ctx = super::callback_tests::running(&storage, &sqlite, &owner, "no-effect-proof").await;
    assert!(ctx.effect_admission.is_none());
    let mut registry = HandlerRegistry::new();
    hub.register(&mut registry, "native");
    assert!(matches!(
        registry.get("native").unwrap()(ctx.clone()).await,
        Err(orch8_types::error::StepError::ClaimLost)
    ));
    assert!(shared.0.lock().unwrap().is_empty());
    assert!(
        backend
            .list_instance_effect_receipts(&ctx.tenant_id, ctx.instance_id, 8)
            .await
            .unwrap()
            .is_empty()
    );
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}
