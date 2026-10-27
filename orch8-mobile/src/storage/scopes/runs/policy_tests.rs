//! Resolve handler identity and effect policy together for each frozen owner.
use super::{
    callback_tests::running,
    tests::{fixture, scope},
};
use crate::{HandlerError, StepHandler, handlers::scoped::CatalogHandlers};
use orch8_engine::handlers::{
    HandlerRegistry,
    builtin::register_builtins,
    step::{StepExecParams, execute_step_dry},
};
use orch8_storage::StorageBackend;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

struct Native(Arc<AtomicUsize>);
impl StepHandler for Native {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(r#"{"native":true}"#.into())
    }
}

#[tokio::test]
async fn native_builtin_override_and_pure_builtin_keep_separate_scoped_effect_policies() {
    let (storage, sqlite) = fixture().await;
    let backend: Arc<dyn StorageBackend> = sqlite.clone();
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = HandlerRegistry::new();
    register_builtins(&mut registry);
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    for (account, catalog, native) in [
        ("alice", "one", true),
        ("alice", "two", false),
        ("bob", "one", false),
        ("bob", "two", true),
    ] {
        let owner = scope(&storage, account, catalog).await;
        if native {
            hub.bind(
                &owner,
                "noop",
                Arc::new(Native(calls.clone())),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        }
        let ctx = running(&storage, &sqlite, &owner, "same-operation").await;
        let params = serde_json::json!({"owner": format!("{account}/{catalog}")});
        let result = execute_step_dry(
            &backend,
            &registry,
            StepExecParams {
                claim_token: ctx.claim_token,
                instance_id: ctx.instance_id,
                tenant_id: ctx.tenant_id.clone(),
                block_id: ctx.block_id,
                handler_name: "noop".into(),
                params: params.clone(),
                context: ctx.context.as_ref().clone(),
                attempt: 0,
                timeout: None,
                externalize_threshold: 0,
                wait_for_input: None,
                cache_key: None,
                output_schema: None,
            },
        )
        .await
        .expect("a catalog without the override must retain its pure builtin");
        assert_eq!(
            result.output,
            if native {
                serde_json::json!({"native":true})
            } else {
                serde_json::json!({})
            }
        );
        let continuity = backend
            .get_continuity_execution_by_instance(&ctx.tenant_id, ctx.instance_id)
            .await
            .unwrap();
        assert_eq!(
            continuity.is_some(),
            native,
            "pure builtin must not inherit native effect admission"
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

fn execution(ctx: &orch8_engine::handlers::StepContext, name: &str) -> StepExecParams {
    StepExecParams {
        claim_token: ctx.claim_token,
        instance_id: ctx.instance_id,
        tenant_id: ctx.tenant_id.clone(),
        block_id: ctx.block_id.clone(),
        handler_name: name.into(),
        params: serde_json::json!({}),
        context: ctx.context.as_ref().clone(),
        attempt: 0,
        timeout: None,
        externalize_threshold: 0,
        wait_for_input: None,
        cache_key: None,
        output_schema: None,
    }
}

#[tokio::test]
async fn scoped_lookup_never_inherits_ambient_native_or_disabled_builtin() {
    let (storage, sqlite) = fixture().await;
    let backend: Arc<dyn StorageBackend> = sqlite.clone();
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let owner = scope(&storage, "alice", "one").await;
    let ctx = running(&storage, &sqlite, &owner, "ambient").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = HandlerRegistry::new();
    // Deliberately populate more than the trusted scoped builtin snapshot.
    register_builtins(&mut registry);
    for name in ["noop", "ambient"] {
        let calls = calls.clone();
        registry.register_with_deferred_effect_admission(name, move |ctx| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                ctx.effect_admission.unwrap().admit().await?;
                Ok(serde_json::json!({"ambient":true}))
            }
        });
    }
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    assert_eq!(
        execute_step_dry(&backend, &registry, execution(&ctx, "noop"))
            .await
            .unwrap()
            .output,
        serde_json::json!({})
    );
    for name in ["ambient", "http_request", "send_signal"] {
        assert!(matches!(
            execute_step_dry(&backend, &registry, execution(&ctx, name)).await,
            Err(orch8_engine::error::EngineError::HandlerNotFound(_)
                | orch8_engine::error::EngineError::RuntimeUnavailable { .. })
        ));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        backend
            .get_continuity_execution_by_instance(&ctx.tenant_id, ctx.instance_id)
            .await
            .unwrap()
            .is_none()
    );
    hub.shutdown();
    assert!(matches!(
        execute_step_dry(&backend, &registry, execution(&ctx, "noop")).await,
        Err(orch8_engine::error::EngineError::ClaimLost { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selection_does_not_pin_foreign_reference_during_effect_preparation() {
    use orch8_storage::testing::faulty::FaultStorage;
    use std::sync::atomic::AtomicBool;
    struct Released(Arc<AtomicBool>, Arc<AtomicUsize>);
    impl StepHandler for Released {
        fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
            self.1.fetch_add(1, Ordering::SeqCst);
            Ok("{}".into())
        }
    }
    impl Drop for Released {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let (storage, sqlite) = fixture().await;
    let (backend, faults) = FaultStorage::wrap(sqlite.clone());
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let owner = scope(&storage, "alice", "one").await;
    let ctx = running(&storage, &sqlite, &owner, "preparation").await;
    let released = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    hub.bind(
        &owner,
        "noop",
        Arc::new(Released(released.clone(), calls.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let mut registry = HandlerRegistry::new();
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    let pause = faults.on("ensure_effect_attempt").times(1).pause();
    let exec = execution(&ctx, "noop");
    let run_store = backend.clone();
    let run = tokio::spawn(async move { execute_step_dry(&run_store, &registry, exec).await });
    tokio::time::timeout(Duration::from_secs(5), pause.entered())
        .await
        .unwrap();
    for receipt in hub.close() {
        tokio::time::timeout(Duration::from_secs(5), receipt)
            .await
            .unwrap()
            .unwrap();
    }
    assert!(
        released.load(Ordering::SeqCst),
        "selected closure pinned the foreign callback"
    );
    pause.release();
    assert!(matches!(
        run.await.unwrap(),
        Err(orch8_engine::error::EngineError::ClaimLost { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let ledger = backend
        .get_continuity_execution_by_instance(&ctx.tenant_id, ctx.instance_id)
        .await
        .unwrap()
        .unwrap();
    let receipts = backend
        .list_effect_receipts(&ctx.tenant_id, ledger.continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(
        receipts[0].state,
        orch8_types::continuity::EffectState::Prepared
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pure_selection_cannot_switch_to_native_after_policy_resolution() {
    use orch8_storage::testing::faulty::FaultStorage;
    let (storage, sqlite) = fixture().await;
    let (backend, faults) = FaultStorage::wrap(sqlite.clone());
    let hub = CatalogHandlers::isolated(storage.clone(), backend.clone());
    let owner = scope(&storage, "alice", "one").await;
    let ctx = running(&storage, &sqlite, &owner, "pure-selection").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = HandlerRegistry::new();
    hub.install_resolver(&mut registry, crate::DEFAULT_BUILTINS);
    let pause = faults.on("verify_instance_claim").times(1).pause();
    let exec = execution(&ctx, "noop");
    let run_store = backend.clone();
    let run = tokio::spawn(async move { execute_step_dry(&run_store, &registry, exec).await });
    tokio::time::timeout(Duration::from_secs(5), pause.entered())
        .await
        .unwrap();
    hub.bind(
        &owner,
        "noop",
        Arc::new(Native(calls.clone())),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    pause.release();
    assert_eq!(run.await.unwrap().unwrap().output, serde_json::json!({}));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        backend
            .get_continuity_execution_by_instance(&ctx.tenant_id, ctx.instance_id)
            .await
            .unwrap()
            .is_none()
    );
    for receipt in hub.close() {
        receipt.await.unwrap();
    }
}

#[test]
fn production_registry_keeps_scoped_selection_through_legacy_registration_and_builtin_enable() {
    let directory = tempfile::tempdir().unwrap();
    let engine = crate::MobileEngine::new(
        directory
            .path()
            .join("policy.sqlite")
            .to_string_lossy()
            .into_owned(),
        crate::MobileEngineConfig::default(),
    )
    .unwrap();
    let storage = engine.catalog_handlers.test_storage();
    let sqlite = storage.inner.clone();
    let owner = engine.runtime.block_on(scope(&storage, "alice", "one"));
    let ambient = Arc::new(AtomicUsize::new(0));
    for pass in 0..3 {
        if pass == 1 {
            engine
                .register_handler("noop".into(), Arc::new(Native(ambient.clone())))
                .unwrap();
        }
        if pass == 2 {
            engine.enable_builtin("http_request".into()).unwrap();
        }
        let ctx = engine.runtime.block_on(running(
            &storage,
            &sqlite,
            &owner,
            &format!("production-{pass}"),
        ));
        let registry = engine.handlers.read().unwrap().clone();
        let result = engine
            .runtime
            .block_on(execute_step_dry(
                &engine.storage,
                &registry,
                execution(&ctx, "noop"),
            ))
            .unwrap();
        assert_eq!(result.output, serde_json::json!({}));
        assert!(
            engine
                .runtime
                .block_on(
                    engine
                        .storage
                        .get_continuity_execution_by_instance(&ctx.tenant_id, ctx.instance_id)
                )
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(ambient.load(Ordering::SeqCst), 0);
    engine.shutdown();
}
