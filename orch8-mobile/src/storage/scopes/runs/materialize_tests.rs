//! Materialization and credential preparation must share the durable owner.
use super::{
    tests::{fixture, limits, native_fixture, request},
    *,
};
use crate::{HandlerError, StepHandler, handlers::scoped::CatalogHandlers};
use orch8_engine::{
    handlers::HandlerRegistry, scheduler::tick_once, sequence_cache::SequenceCache,
};
use orch8_storage::{
    AdminStore, ClaimedRead, InstanceStore, StorageBackend,
    testing::fixtures::{SequenceBuilder, step_with_params},
};
use orch8_types::{credential::CredentialDef, instance::InstanceState, sequence::BlockDefinition};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
pub(super) struct Echo(pub(super) Arc<AtomicUsize>);
impl StepHandler for Echo {
    fn execute(&self, _: String, input: String) -> Result<String, HandlerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(input)
    }
}
pub(super) async fn owner_with_work(
    storage: &MobileStorage,
    account: &str,
    catalog: &str,
) -> CatalogScope {
    owner_with_params(
        storage,
        account,
        catalog,
        serde_json::json!({"secret":"credentials://shared"}),
    )
    .await
}
pub(super) async fn owner_with_params(
    storage: &MobileStorage,
    account: &str,
    catalog: &str,
    params: serde_json::Value,
) -> CatalogScope {
    let account = storage
        .open_account_scope(account, "issuer", account)
        .await
        .unwrap();
    let owner = storage
        .open_catalog_scope(&account, catalog, "source", "authority")
        .await
        .unwrap();
    let seq = SequenceBuilder::new("publisher")
        .name("flow")
        .block(BlockDefinition::Step(Box::new(step_with_params(
            "work", "native", params,
        ))))
        .build();
    let digest = "b".repeat(64);
    storage
        .accept_catalog_manifest(&owner, &[], 1, &digest)
        .await
        .unwrap();
    storage
        .install_catalog_workflow(&owner, &serde_json::to_string(&seq).unwrap(), 1, &digest)
        .await
        .unwrap();
    sqlx::query("INSERT INTO mobile_catalog_execution_policy(account_id,catalog_id,name,version,manifest_version,manifest_digest,min_sdk_version,required_handlers) VALUES (?1,?2,'flow',1,1,?3,'0.0.0','[\"native\"]')")
        .bind(owner.account_id()).bind(owner.catalog_id()).bind(&digest).execute(storage.pool()).await.unwrap();
    storage
        .mark_catalog_reconciled(&owner, 1, &digest)
        .await
        .unwrap();
    owner
}
pub(super) fn credential(id: &str, tenant: &str, value: &str) -> CredentialDef {
    serde_json::from_value(serde_json::json!({"id":id,"tenant_id":tenant,"name":id,"value":value,"enabled":true,"created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now()})).unwrap()
}
#[tokio::test]
async fn scoped_preparation_does_not_inherit_a_global_credential() {
    let (storage, sqlite) = fixture().await;
    let backend: Arc<dyn StorageBackend> = sqlite.clone();
    sqlite
        .create_credential(&credential("shared", "", "GLOBAL_PRIVATE_CANARY"))
        .await
        .unwrap();
    let owner = owner_with_work(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&owner, request(Some("global"), "{}"), limits())
        .await
        .unwrap();
    native_fixture(&sqlite, &run).await;
    let hub = CatalogHandlers::isolated(storage, backend.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    hub.bind(
        &owner,
        "native",
        Arc::new(Echo(calls.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let mut registry = HandlerRegistry::new();
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    tick_once(
        &backend,
        &Arc::new(registry),
        &Arc::new(tokio::sync::Semaphore::new(4)),
        &orch8_types::config::SchedulerConfig::default(),
        &Arc::new(SequenceCache::new(16, Duration::from_secs(30))),
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        backend.get_instance(run.id).await.unwrap().unwrap().state,
        InstanceState::Failed,
        "scoped preparation inherited an ambient global credential"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

pub(super) fn instance_limits() -> super::materialize::InstanceLimits {
    super::materialize::InstanceLimits {
        global: 16,
        account: 8,
        catalog: 4,
    }
}
pub(super) fn builtins() -> crate::handlers::scoped::materialize::RuntimeEnvironment {
    crate::handlers::scoped::materialize::RuntimeEnvironment {
        sdk: env!("CARGO_PKG_VERSION").into(),
        builtins: crate::DEFAULT_BUILTINS
            .iter()
            .map(|name| (*name).into())
            .collect(),
    }
}

pub(super) async fn tick(backend: &Arc<dyn StorageBackend>, registry: &Arc<HandlerRegistry>) {
    tick_once(
        backend,
        registry,
        &Arc::new(tokio::sync::Semaphore::new(4)),
        &orch8_types::config::SchedulerConfig::default(),
        &Arc::new(SequenceCache::new(16, Duration::from_secs(30))),
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
}
#[tokio::test]
async fn four_scopes_materialize_same_request_and_credential_alias_without_ambient_fallback() {
    let (storage, sqlite) = fixture().await;
    let backend: Arc<dyn StorageBackend> = sqlite.clone();
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let mut registry = HandlerRegistry::new();
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    let registry = Arc::new(registry);
    let calls = Arc::new(AtomicUsize::new(0));
    for (account, catalog) in [
        ("alice", "one"),
        ("alice", "two"),
        ("bob", "one"),
        ("bob", "two"),
    ] {
        let owner = owner_with_work(&storage, account, catalog).await;
        let tenant = super::super::workflows::owner(&owner).unwrap().0;
        let id = format!("{account}-{catalog}");
        let value = format!("{account}/{catalog}/PRIVATE_CANARY");
        backend
            .create_credential(&credential(&id, tenant.as_str(), &value))
            .await
            .unwrap();
        storage
            .grant_catalog_credential(&owner, "shared", &id)
            .await
            .unwrap();
        hub.bind(
            &owner,
            "native",
            Arc::new(Echo(calls.clone())),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let run = storage
            .prepare_catalog_run(
                &owner,
                request(Some("same"), r#"{"admitted":"original"}"#),
                limits(),
            )
            .await
            .unwrap();
        let instance = hub
            .materialize(owner.clone(), run.id, instance_limits(), builtins())
            .await
            .unwrap();
        assert_eq!(instance.id, run.id);
        assert_eq!(instance.created_at, run.created_at);
        assert_eq!(instance.context.data, run.input);
        assert_eq!(instance.sequence_id, run.sequence_id);
        assert_eq!(instance.tenant_id, tenant);
        let retry = hub
            .materialize(owner.clone(), run.id, instance_limits(), builtins())
            .await
            .unwrap();
        assert_eq!(retry.id, instance.id);
        tick(&backend, &registry).await;
        assert_eq!(
            backend.get_instance(run.id).await.unwrap().unwrap().state,
            InstanceState::Completed
        );
        let output = backend
            .get_block_output(run.id, &orch8_types::ids::BlockId::new("work"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(output.output["secret"], value);
        assert!(matches!(
            hub.materialize(owner, run.id, instance_limits(), builtins())
                .await
                .unwrap()
                .state,
            InstanceState::Completed
        ));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[tokio::test]
async fn materialization_refuses_foreign_cancelled_retired_and_stopped_requests() {
    let (storage, sqlite) = fixture().await;
    let hub = CatalogHandlers::isolated(storage.clone(), sqlite.clone());
    let alice = owner_with_work(&storage, "alice", "one").await;
    let bob = owner_with_work(&storage, "bob", "one").await;
    let calls = Arc::new(AtomicUsize::new(0));
    hub.bind(
        &alice,
        "native",
        Arc::new(Echo(calls)),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let run = storage
        .prepare_catalog_run(&alice, request(Some("pending"), "{}"), limits())
        .await
        .unwrap();
    assert!(
        hub.materialize(bob, run.id, instance_limits(), builtins())
            .await
            .is_err()
    );
    storage.cancel_catalog_run(&alice, run.id).await.unwrap();
    assert!(
        hub.materialize(alice.clone(), run.id, instance_limits(), builtins())
            .await
            .is_err()
    );
    let run = storage
        .prepare_catalog_run(&alice, request(Some("retired"), "{}"), limits())
        .await
        .unwrap();
    storage.retire_account_scope(&alice.account).await.unwrap();
    assert!(
        hub.materialize(alice, run.id, instance_limits(), builtins())
            .await
            .is_err()
    );
    assert!(sqlite.get_instance(run.id).await.unwrap().is_none());
    hub.shutdown();
    assert!(matches!(
        hub.materialize(
            owner_with_work(&storage, "carol", "one").await,
            run.id,
            instance_limits(),
            builtins()
        )
        .await,
        Err(crate::MobileError::Shutdown)
    ));
}

#[tokio::test]
async fn a_validated_request_cannot_publish_after_retirement_withdrawal_or_cancellation() {
    use super::materialize::Preparation;
    for change in ["retire", "withdraw", "cancel"] {
        let (storage, sqlite) = fixture().await;
        let owner = owner_with_work(&storage, "alice", "one").await;
        let run = storage
            .prepare_catalog_run(&owner, request(Some("race"), "{}"), limits())
            .await
            .unwrap();
        let Preparation::Pending(snapshot) = storage
            .prepare_materialization(&owner, run.id)
            .await
            .unwrap()
        else {
            panic!("pending request");
        };
        let checked = snapshot
            .check(
                &std::collections::HashSet::from(["native".into()]),
                env!("CARGO_PKG_VERSION"),
            )
            .unwrap();
        match change {
            "retire" => {
                storage.retire_account_scope(&owner.account).await.unwrap();
            }
            "cancel" => {
                storage.cancel_catalog_run(&owner, run.id).await.unwrap();
            }
            _ => {
                storage
                    .accept_catalog_manifest(&owner, &[], 2, &"c".repeat(64))
                    .await
                    .unwrap();
            }
        }
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
        assert!(sqlite.get_instance(run.id).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn credential_grants_are_catalog_qualified_and_epoch_fenced() {
    let (storage, sqlite) = fixture().await;
    let one = owner_with_work(&storage, "alice", "one").await;
    let two = owner_with_work(&storage, "alice", "two").await;
    let bob = owner_with_work(&storage, "bob", "one").await;
    let tenant = super::super::workflows::owner(&one).unwrap().0;
    sqlite
        .create_credential(&credential("a", tenant.as_str(), "ACCOUNT_PRIVATE_CANARY"))
        .await
        .unwrap();
    sqlite
        .create_credential(&credential("global", "", "GLOBAL_PRIVATE_CANARY"))
        .await
        .unwrap();
    assert!(
        storage
            .grant_catalog_credential(&bob, "shared", "a")
            .await
            .is_err()
    );
    assert!(
        storage
            .grant_catalog_credential(&one, "shared", "global")
            .await
            .is_err()
    );
    storage
        .grant_catalog_credential(&one, "shared", "a")
        .await
        .unwrap();
    let ctx1 = super::callback_tests::running(&storage, &sqlite, &one, "one").await;
    let ctx2 = super::callback_tests::running(&storage, &sqlite, &two, "two").await;
    let read = |ctx: orch8_engine::handlers::StepContext| {
        let backend = sqlite.clone();
        async move {
            backend
                .get_credential_for_claim(
                    ctx.instance_id,
                    &ctx.tenant_id,
                    ctx.claim_token.unwrap(),
                    "shared",
                )
                .await
                .unwrap()
        }
    };
    assert!(matches!(
        read(ctx1.clone()).await,
        ClaimedRead::Value(Some(_))
    ));
    assert!(matches!(read(ctx2.clone()).await, ClaimedRead::Value(None)));
    storage
        .grant_catalog_credential(&two, "shared", "a")
        .await
        .unwrap();
    storage
        .revoke_catalog_credential(&one, "shared")
        .await
        .unwrap();
    assert!(matches!(read(ctx1.clone()).await, ClaimedRead::Value(None)));
    assert!(matches!(read(ctx2).await, ClaimedRead::Value(Some(_))));
    storage.retire_account_scope(&one.account).await.unwrap();
    assert!(matches!(read(ctx1).await, ClaimedRead::Lost));
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    let reopened = storage
        .open_catalog_scope(&account, "one", "source", "authority")
        .await
        .unwrap();
    let ctx = super::callback_tests::running(&storage, &sqlite, &reopened, "new-epoch").await;
    assert!(matches!(read(ctx.clone()).await, ClaimedRead::Value(None)));
    assert!(
        storage
            .grant_catalog_credential(&one, "shared", "a")
            .await
            .is_err()
    );
    storage
        .grant_catalog_credential(&reopened, "shared", "a")
        .await
        .unwrap();
    assert!(matches!(read(ctx).await, ClaimedRead::Value(Some(_))));
}

#[test]
fn production_engine_materializes_an_explicit_scope_without_legacy_start() {
    let directory = tempfile::tempdir().unwrap();
    let engine = crate::MobileEngine::new(
        directory
            .path()
            .join("materialize.sqlite")
            .to_string_lossy()
            .into_owned(),
        crate::MobileEngineConfig::default(),
    )
    .unwrap();
    let gate = engine
        .runtime
        .block_on(CatalogHandlers::shared_test_gate().lock_owned());
    let storage = engine.catalog_handlers.test_storage();
    let calls = Arc::new(AtomicUsize::new(0));
    let owner = engine
        .runtime
        .block_on(owner_with_work(&storage, "alice", "one"));
    let tenant = super::super::workflows::owner(&owner).unwrap().0;
    engine
        .runtime
        .block_on(engine.storage.create_credential(&credential(
            "production",
            tenant.as_str(),
            "PRODUCTION_CANARY",
        )))
        .unwrap();
    engine
        .runtime
        .block_on(storage.grant_catalog_credential(&owner, "shared", "production"))
        .unwrap();
    engine
        .runtime
        .block_on(engine.catalog_handlers.bind(
            &owner,
            "native",
            Arc::new(Echo(calls.clone())),
            Duration::from_secs(5),
        ))
        .unwrap();
    let run = engine
        .runtime
        .block_on(storage.prepare_catalog_run(&owner, request(Some("production"), "{}"), limits()))
        .unwrap();
    let instance = engine
        .runtime
        .block_on(engine.materialize_catalog_run(owner, run.id))
        .unwrap();
    assert_eq!(instance.id, run.id);
    engine.tick_once().unwrap();
    assert_eq!(
        engine
            .runtime
            .block_on(engine.storage.get_instance(run.id))
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    engine.shutdown();
    for receipt in engine.catalog_handlers.close() {
        engine.runtime.block_on(async { receipt.await }).unwrap();
    }
    drop(gate);
}

#[tokio::test]
async fn materialization_receipt_survives_native_history_gc_and_cannot_be_forgotten_as_cancelled() {
    let (storage, sqlite) = fixture().await;
    let backend: Arc<dyn StorageBackend> = sqlite.clone();
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let owner = owner_with_work(&storage, "alice", "one").await;
    let tenant = super::super::workflows::owner(&owner).unwrap().0;
    backend
        .create_credential(&credential("gc", tenant.as_str(), "GC_CANARY"))
        .await
        .unwrap();
    storage
        .grant_catalog_credential(&owner, "shared", "gc")
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
    let run = storage
        .prepare_catalog_run(&owner, request(Some("gc"), "{}"), limits())
        .await
        .unwrap();
    hub.materialize(owner.clone(), run.id, instance_limits(), builtins())
        .await
        .unwrap();
    let mut registry = HandlerRegistry::new();
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    tick(&backend, &Arc::new(registry)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        backend.purge_terminal_instances(&[run.id]).await.unwrap(),
        1
    );
    assert!(!storage.cancel_catalog_run(&owner, run.id).await.unwrap());
    assert!(
        !storage
            .forget_cancelled_catalog_run(&owner, run.id)
            .await
            .unwrap()
    );
    assert!(
        hub.materialize(owner, run.id, instance_limits(), builtins())
            .await
            .is_err()
    );
    assert!(backend.get_instance(run.id).await.unwrap().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}
