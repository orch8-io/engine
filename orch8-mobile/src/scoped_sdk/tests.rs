use super::*;
use crate::{MobileEngineConfig, storage::scopes::CatalogScope};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use std::{
    collections::HashSet,
    sync::Mutex,
    time::{Duration, Instant},
};

fn engine(memory: bool) -> (Arc<MobileEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = if memory {
        ":memory:".into()
    } else {
        dir.path().join("sdk.db").to_string_lossy().into_owned()
    };
    let engine = MobileEngine::new(
        path,
        MobileEngineConfig {
            telemetry_enabled: false,
            operation_timeout_ms: 3000,
            ..Default::default()
        },
    )
    .unwrap();
    (engine, dir)
}
fn identity(account: &str) -> MobileAccountIdentity {
    MobileAccountIdentity {
        account_key: account.into(),
        issuer: "issuer".into(),
        subject: account.into(),
    }
}
fn configuration(catalog: &str) -> MobileCatalogConfiguration {
    let key = ed25519_dalek::SigningKey::from_bytes(&[31; 32]);
    MobileCatalogConfiguration {
        catalog_key: catalog.into(),
        source: "source".into(),
        signing_authority: format!("ed25519:{}", BASE64.encode(key.verifying_key().as_bytes())),
        manifest_url: format!("https://catalog.example/{catalog}/manifest.json"),
        identity_url: "https://catalog.example/identity".into(),
    }
}
fn request(key: &str) -> MobileCatalogRunRequest {
    MobileCatalogRunRequest {
        workflow: "flow".into(),
        version: None,
        input_json:
            r#"{"account_id":"forged","catalog_id":"forged","private":"SDK_PRIVATE_CANARY"}"#.into(),
        operation_key: key.into(),
    }
}

// Seed a trusted accepted definition to isolate public dispatch/FFI authority
// from the separately tested signed transport. No unsigned install is exported.
fn install(engine: &MobileEngine, scope: &CatalogScope, parallel: bool) {
    let step = |id: &str| {
        serde_json::json!({"type":"step","id":id,"handler":"echo","params":{},
        "wait_for_input":{"prompt":"Approve?","timeout":60000,"choices":[{"label":"Yes","value":"yes"}],"store_as":id}})
    };
    let blocks = if parallel {
        serde_json::json!([{"type":"parallel","id":"both","branches":[[step("left")],[step("right")]]}])
    } else {
        serde_json::json!([{"type":"step","id":"work","handler":"echo","params":{}}])
    };
    let seq = serde_json::json!({"id":uuid::Uuid::new_v4(),"tenant_id":"publisher","namespace":"published",
        "name":"flow","version":1,"blocks":blocks,"created_at":chrono::Utc::now()});
    engine.runtime.block_on(
        crate::storage::scopes::test_support::install_sdk_definition(
            engine.lifecycle.mobile_storage(),
            scope,
            &seq.to_string(),
            &["echo"],
        ),
    );
}
struct Echo(Arc<Mutex<Vec<MobileCatalogCallbackContext>>>);
impl MobileCatalogStepHandler for Echo {
    fn execute(
        &self,
        context: MobileCatalogCallbackContext,
        _: String,
        _: String,
    ) -> Result<String, HandlerError> {
        self.0.lock().unwrap().push(context);
        Ok(r#"{"ok":true}"#.into())
    }
}
fn advance(
    engine: &MobileEngine,
    run: &MobileCatalogRun,
    state: InstanceStateKind,
) -> MobileCatalogRunSnapshot {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        engine.tick_once().unwrap();
        let snapshot = run.snapshot().unwrap().unwrap();
        if snapshot.state == state {
            return snapshot;
        }
        assert!(
            Instant::now() < until,
            "expected {state:?}, got {:?}",
            snapshot.state
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn public_four_scope_dispatch_uses_contextual_callbacks_and_never_exposes_foreign_or_legacy_runs() {
    let (engine, _dir) = engine(false);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let handler = Arc::new(Echo(calls.clone()));
    let mut owners = Vec::new();
    for account_name in ["alice", "bob"] {
        let account = engine.clone().open_account(identity(account_name)).unwrap();
        for catalog_name in ["one", "two"] {
            let catalog = account.open_catalog(configuration(catalog_name)).unwrap();
            install(&engine, &catalog.scope, false);
            catalog
                .register_handler("echo".into(), handler.clone(), 1000)
                .unwrap();
            let run = catalog.prepare_run(request("same-key")).unwrap();
            assert!(run.snapshot().unwrap().is_none());
            assert!(matches!(
                catalog.history(None, 8).unwrap().items[0].native_state,
                MobileCatalogNativeState::NotMaterialized
            ));
            let replay = catalog.prepare_run(request("same-key")).unwrap();
            assert_eq!(replay.instance_id(), run.instance_id());
            let mut changed = request("same-key");
            changed.input_json = "{}".into();
            assert!(catalog.prepare_run(changed).is_err());
            run.start().unwrap();
            let completed = advance(&engine, &run, InstanceStateKind::Completed);
            assert_eq!(completed.account_id, account.account_id());
            assert_eq!(completed.catalog_id, catalog.catalog_id());
            assert!(engine.get_instance(run.instance_id()).is_err());
            assert_eq!(run.cancel().unwrap(), Some(InstanceStateKind::Completed));
            owners.push((account.clone(), catalog, run));
        }
    }
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 4);
    let ids: HashSet<_> = calls.iter().map(|c| c.effect_id.clone()).collect();
    assert_eq!(ids.len(), 4);
    for (account, catalog, run) in &owners {
        let context = calls
            .iter()
            .find(|c| c.run_id == run.instance_id())
            .unwrap();
        assert_eq!(context.account_id, account.account_id());
        assert_eq!(context.catalog_id, catalog.catalog_id());
        assert_eq!(context.owner_epoch, account.session_epoch());
        assert_eq!(context.block_id, "work");
        assert_ne!(context.account_id, "forged");
        let history = catalog.history(None, 8).unwrap();
        assert_eq!(history.items.len(), 1);
        assert_eq!(history.items[0].instance_id, run.instance_id());
        for (_, other, foreign) in &owners {
            if catalog.catalog_id() == other.catalog_id() {
                continue;
            }
            let guessed = catalog.run(foreign.instance_id()).unwrap();
            assert!(guessed.snapshot().unwrap().is_none());
            assert!(guessed.cancel().unwrap().is_none());
        }
    }
    drop(calls);
    engine.shutdown();
}

#[test]
fn public_parallel_approvals_have_real_pending_steps_and_reject_stale_versions_in_both_orders() {
    for memory in [false, true] {
        for first in ["left", "right"] {
            let (engine, _dir) = engine(memory);
            let account = engine.clone().open_account(identity("alice")).unwrap();
            let catalog = account.open_catalog(configuration("one")).unwrap();
            install(&engine, &catalog.scope, true);
            catalog
                .register_handler(
                    "echo".into(),
                    Arc::new(Echo(Arc::new(Mutex::new(Vec::new())))),
                    1000,
                )
                .unwrap();
            let run = catalog.prepare_run(request("answers")).unwrap();
            run.start().unwrap();
            let waiting = advance(&engine, &run, InstanceStateKind::Waiting);
            assert_eq!(waiting.pending_steps, ["left", "right"]);
            assert!(
                !run.complete_step(waiting.created_at.clone(), first.into(), "{}".into())
                    .unwrap()
            );
            assert!(
                !run.complete_step(waiting.updated_at.clone(), "unknown".into(), "{}".into())
                    .unwrap()
            );
            let unchanged = run.snapshot().unwrap().unwrap();
            assert_eq!(unchanged.updated_at, waiting.updated_at);
            assert!(
                run.complete_step(
                    waiting.updated_at.clone(),
                    first.into(),
                    r#"{"value":"yes"}"#.into()
                )
                .unwrap()
            );
            assert!(
                !run.complete_step(
                    waiting.updated_at.clone(),
                    first.into(),
                    r#"{"value":"no"}"#.into()
                )
                .unwrap()
            );
            let second = if first == "left" { "right" } else { "left" };
            let waiting = advance(&engine, &run, InstanceStateKind::Waiting);
            assert_eq!(waiting.pending_steps, [second]);
            assert!(
                run.complete_step(
                    waiting.updated_at,
                    second.into(),
                    r#"{"value":"yes"}"#.into()
                )
                .unwrap()
            );
            let completed = advance(&engine, &run, InstanceStateKind::Completed);
            let data: serde_json::Value = serde_json::from_str(&completed.data_json).unwrap();
            assert_eq!(data["left"], "yes");
            assert_eq!(data["right"], "yes");
            engine.shutdown();
        }
    }
}

#[test]
fn public_close_propagates_locally_and_retirement_reopens_without_rebinding_principals_or_old_cursors()
 {
    let (engine, _dir) = engine(false);
    let account = engine.clone().open_account(identity("alice")).unwrap();
    let sibling = engine.clone().open_account(identity("alice")).unwrap();
    let catalog = account.open_catalog(configuration("one")).unwrap();
    install(&engine, &catalog.scope, false);
    let run = catalog.prepare_run(request("queued")).unwrap();
    let extra = catalog.prepare_run(request("second")).unwrap();
    let cursor = catalog.history(None, 1).unwrap().next.unwrap();
    let sibling_catalog = sibling.open_catalog(configuration("one")).unwrap();
    account.close_view();
    assert!(matches!(run.snapshot(), Err(MobileError::Shutdown)));
    assert!(matches!(extra.start(), Err(MobileError::Shutdown)));
    assert_eq!(sibling_catalog.history(None, 8).unwrap().items.len(), 2);
    let epoch = sibling.session_epoch();
    sibling.retire().unwrap();
    let fresh = engine.clone().open_account(identity("alice")).unwrap();
    assert!(fresh.session_epoch() > epoch);
    let current = fresh.open_catalog(configuration("one")).unwrap();
    assert!(current.history(Some(cursor), 1).is_err());
    let page = current.history(None, 8).unwrap();
    assert_eq!(page.items.len(), 2);
    assert!(page.items.iter().all(|i| i.owner_epoch == epoch));
    let historical = current.run(run.instance_id()).unwrap();
    assert!(historical.start().is_err());
    assert_eq!(
        historical.cancel().unwrap(),
        Some(InstanceStateKind::Cancelled)
    );
    assert!(historical.forget_cancelled_request().unwrap());
    let mut wrong = identity("alice");
    wrong.subject = "mallory".into();
    assert!(engine.clone().open_account(wrong).is_err());
    let weak = Arc::downgrade(&engine);
    drop(engine);
    assert!(weak.upgrade().is_none());
    assert!(matches!(
        current.history(None, 1),
        Err(MobileError::Shutdown)
    ));
}

struct HeldConnections {
    connections: Vec<sqlx::pool::PoolConnection<sqlx::Sqlite>>,
    runtime: tokio::runtime::Handle,
}
impl Drop for HeldConnections {
    fn drop(&mut self) {
        let _context = self.runtime.enter();
        self.connections.clear();
    }
}

#[test]
fn public_parent_close_cancels_pool_blocked_history_without_retiring_the_account() {
    let (engine, _dir) = engine(false);
    let account = engine.clone().open_account(identity("alice")).unwrap();
    let sibling = engine.clone().open_account(identity("alice")).unwrap();
    let catalog = account.open_catalog(configuration("one")).unwrap();
    let sibling_catalog = sibling.open_catalog(configuration("one")).unwrap();
    let held = HeldConnections {
        runtime: engine.runtime.handle(),
        connections: engine.runtime.block_on(async {
            let mut held = Vec::new();
            for _ in 0..engine.sqlite.pool().options().get_max_connections() {
                held.push(engine.sqlite.pool().acquire().await.unwrap());
            }
            held
        }),
    };
    let (done, result) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        done.send(catalog.history(None, 8)).unwrap();
    });
    let until = Instant::now() + Duration::from_secs(2);
    while Arc::strong_count(&engine) < 2 {
        assert!(Instant::now() < until);
        std::thread::yield_now();
    }
    account.close_view();
    assert!(matches!(
        result.recv_timeout(Duration::from_secs(2)).unwrap(),
        Err(MobileError::Shutdown)
    ));
    reader.join().unwrap();
    drop(held);
    assert!(sibling_catalog.history(None, 8).unwrap().items.is_empty());
    engine.shutdown();
}

#[test]
fn public_invalid_catalog_configuration_does_not_pin_a_broken_binding() {
    let (engine, _dir) = engine(false);
    let account = engine.clone().open_account(identity("alice")).unwrap();
    for url in [
        "http://catalog.example/manifest",
        "https://localhost/manifest",
        "https://user:secret@catalog.example/manifest",
        "https://catalog.example/manifest?key=secret",
    ] {
        let mut bad = configuration("one");
        bad.manifest_url = url.into();
        assert!(account.open_catalog(bad).is_err());
    }
    let mut bad = configuration("one");
    bad.signing_authority = "ed25519:invalid".into();
    assert!(account.open_catalog(bad).is_err());
    let catalog = account.open_catalog(configuration("one")).unwrap();
    assert!(catalog.history(None, 1).unwrap().items.is_empty());
    let calls = Arc::new(Mutex::new(Vec::new()));
    assert!(matches!(
        catalog.register_handler("echo".into(), Arc::new(Echo(calls)), 0),
        Err(MobileError::InvalidInput { .. })
    ));
    engine.shutdown();
}

#[test]
fn public_retirement_detaches_registered_hosts_from_every_catalog_in_the_epoch() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Dropped(Arc<AtomicUsize>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl MobileCatalogStepHandler for Dropped {
        fn execute(
            &self,
            _: MobileCatalogCallbackContext,
            _: String,
            _: String,
        ) -> Result<String, HandlerError> {
            Ok("{}".into())
        }
    }
    let (engine, _dir) = engine(false);
    let account = engine.clone().open_account(identity("alice")).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let catalogs: Vec<_> = ["one", "two", "three"]
        .into_iter()
        .map(|name| {
            let catalog = account.open_catalog(configuration(name)).unwrap();
            catalog
                .register_handler("echo".into(), Arc::new(Dropped(drops.clone())), 1000)
                .unwrap();
            catalog
        })
        .collect();
    account.retire().unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while drops.load(Ordering::SeqCst) != 3 {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(5));
    }
    for catalog in catalogs {
        assert!(catalog.history(None, 1).is_err());
    }
    engine.shutdown();
}

#[test]
fn public_issued_run_view_outlives_temporary_parent_views_without_pinning_the_engine() {
    let (engine, _dir) = engine(false);
    let run = {
        let account = engine.clone().open_account(identity("alice")).unwrap();
        let catalog = account.open_catalog(configuration("one")).unwrap();
        catalog.run(uuid::Uuid::new_v4().to_string()).unwrap()
    };
    assert!(run.snapshot().unwrap().is_none());
    let weak = Arc::downgrade(&engine);
    drop(engine);
    assert!(weak.upgrade().is_none());
    assert!(matches!(run.snapshot(), Err(MobileError::Shutdown)));
}

/// Portable, trusted test data for real foreign SDK execution tests. There is
/// deliberately no production unsigned-install or fixture-export API.
#[test]
fn export_scoped_sdk_fixture_for_wrappers() {
    let temporary = tempfile::tempdir().unwrap();
    let destination = std::env::var_os("ORCH8_SCOPED_FIXTURE_DIR")
        .map_or_else(|| temporary.path().to_owned(), std::path::PathBuf::from);
    std::fs::create_dir_all(&destination).unwrap();
    let database = destination.join("scoped.db");
    let metadata = destination.join("metadata.json");
    assert!(
        !database.exists() && !metadata.exists(),
        "fixture output must be fresh"
    );
    let config = MobileEngineConfig {
        telemetry_enabled: false,
        operation_timeout_ms: 10_000,
        ..Default::default()
    };
    let producer =
        MobileEngine::new(database.to_string_lossy().into_owned(), config.clone()).unwrap();
    let mut cases = Vec::new();
    for name in ["alice", "bob"] {
        let account = producer.clone().open_account(identity(name)).unwrap();
        for key in ["one", "two"] {
            let configuration = configuration(key);
            let catalog = account.open_catalog(configuration.clone()).unwrap();
            install(&producer, &catalog.scope, true);
            cases.push(serde_json::json!({
                "accountKey":name,"issuer":"issuer","subject":name,"accountId":account.account_id(),
                "catalogId":catalog.catalog_id(),"sessionEpoch":account.session_epoch(),
                "catalogKey":key,"source":configuration.source,"signingAuthority":configuration.signing_authority,
                "manifestUrl":configuration.manifest_url,"identityUrl":configuration.identity_url
            }));
        }
    }
    checkpoint_and_close(&producer);
    let reopened = MobileEngine::new(database.to_string_lossy().into_owned(), config).unwrap();
    for case in &cases {
        let account = reopened
            .clone()
            .open_account(identity(case["accountKey"].as_str().unwrap()))
            .unwrap();
        assert_eq!(account.account_id(), case["accountId"].as_str().unwrap());
        assert_eq!(
            account.session_epoch(),
            case["sessionEpoch"].as_i64().unwrap()
        );
        let catalog = account
            .open_catalog(configuration(case["catalogKey"].as_str().unwrap()))
            .unwrap();
        let probe = catalog.prepare_run(request("fixture-validation")).unwrap();
        assert_eq!(probe.cancel().unwrap(), Some(InstanceStateKind::Cancelled));
        assert!(probe.forget_cancelled_request().unwrap());
        assert!(catalog.history(None, 1).unwrap().items.is_empty());
    }
    checkpoint_and_close(&reopened);
    std::fs::write(
        metadata,
        serde_json::to_vec_pretty(&serde_json::json!({"cases":cases})).unwrap(),
    )
    .unwrap();
}

fn checkpoint_and_close(engine: &MobileEngine) {
    engine.runtime.block_on(async {
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(engine.sqlite.pool())
            .await
            .unwrap();
    });
    engine.shutdown();
    engine.runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), engine.sqlite.close())
            .await
            .unwrap();
    });
}
