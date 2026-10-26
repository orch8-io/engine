//! A real process death is stronger evidence than dropping an in-process engine:
//! no runtime, callback destructor or graceful `SQLite` close runs after SIGKILL.
use super::*;
use std::{fs::OpenOptions, io::Write, path::Path, process::Child};

const CHILD_DB: &str = "ORCH8_TEST_SCOPED_PROCESS_DATABASE";
const TEST_NAME: &str = "scoped_sdk::tests::process::sigkill_preserves_scoped_ownership_and_never_replays_ambiguous_effects";

struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct PersistAndHold {
    effects: std::path::PathBuf,
    write: Mutex<()>,
}
impl MobileCatalogStepHandler for PersistAndHold {
    fn execute(
        &self,
        context: MobileCatalogCallbackContext,
        _: String,
        _: String,
    ) -> Result<String, HandlerError> {
        {
            let _write = self.write.lock().unwrap();
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.effects)
                .unwrap();
            writeln!(
                file,
                "{}",
                serde_json::json!({
                    "account": context.account_id, "catalog": context.catalog_id,
                    "epoch": context.owner_epoch, "run": context.run_id,
                    "effect": context.effect_id,
                })
            )
            .unwrap();
            file.sync_all().unwrap();
        }
        // Only OS process death can finish this native call.
        loop {
            std::thread::park();
        }
    }
}

fn config() -> MobileEngineConfig {
    MobileEngineConfig {
        telemetry_enabled: false,
        operation_timeout_ms: 10_000,
        ..Default::default()
    }
}

fn child(database: &Path) {
    let engine = MobileEngine::new(database.to_string_lossy().into_owned(), config()).unwrap();
    let handler = Arc::new(PersistAndHold {
        effects: database.with_extension("effects"),
        write: Mutex::new(()),
    });
    for name in ["alice", "bob"] {
        let account = engine.clone().open_account(identity(name)).unwrap();
        for key in ["one", "two"] {
            account
                .open_catalog(configuration(key))
                .unwrap()
                .register_handler("echo".into(), handler.clone(), 60_000)
                .unwrap();
        }
    }
    engine.resume();
    loop {
        std::thread::park();
    }
}

fn effects(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

struct Owner {
    name: &'static str,
    key: &'static str,
    account: String,
    catalog: String,
    epoch: i64,
    run: String,
}

fn seed(database: &Path) -> Vec<Owner> {
    let producer = MobileEngine::new(database.to_string_lossy().into_owned(), config()).unwrap();
    let mut owners = Vec::new();
    for name in ["alice", "bob"] {
        let account = producer.clone().open_account(identity(name)).unwrap();
        for key in ["one", "two"] {
            let catalog = account.open_catalog(configuration(key)).unwrap();
            install(&producer, &catalog.scope, false);
            catalog
                .register_handler("echo".into(), Arc::new(Echo(Arc::default())), 60_000)
                .unwrap();
            let run = catalog.prepare_run(request("process-crash")).unwrap();
            run.start().unwrap();
            owners.push(Owner {
                name,
                key,
                account: account.account_id(),
                catalog: catalog.catalog_id(),
                epoch: account.session_epoch(),
                run: run.instance_id(),
            });
        }
    }
    checkpoint_and_close(&producer);
    owners
}

fn spawn_child(database: &Path, log_path: &Path) -> KillOnDrop {
    let log = std::fs::File::create(log_path).unwrap();
    KillOnDrop(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env(CHILD_DB, database)
            .stdin(std::process::Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap(),
    )
}

fn wait_for_effects(
    process: &mut KillOnDrop,
    effects_path: &Path,
    log_path: &Path,
) -> Vec<serde_json::Value> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let dispatched = effects(effects_path);
        if dispatched.len() == 2 {
            return dispatched;
        }
        assert!(
            process.0.try_wait().unwrap().is_none(),
            "child exited: {}",
            std::fs::read_to_string(log_path).unwrap()
        );
        assert!(
            Instant::now() < deadline,
            "callbacks did not enter: {}",
            std::fs::read_to_string(log_path).unwrap()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn sigkill_preserves_scoped_ownership_and_never_replays_ambiguous_effects() {
    if let Some(database) = std::env::var_os(CHILD_DB) {
        child(Path::new(&database));
        unreachable!();
    }
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("process.db");
    let path = database.to_string_lossy().into_owned();
    let owners = seed(&database);
    let log_path = directory.path().join("child.log");
    let mut process = spawn_child(&database, &log_path);
    let effects_path = database.with_extension("effects");
    let dispatched = wait_for_effects(&mut process, &effects_path, &log_path);
    assert_ne!(
        dispatched[0]["account"], dispatched[1]["account"],
        "physical fairness must admit both accounts"
    );
    assert!(
        MobileEngine::new(path.clone(), config()).is_err(),
        "a live process must retain exclusive recovery ownership"
    );
    process.0.kill().unwrap();
    assert!(!process.0.wait().unwrap().success());

    let reopened = MobileEngine::new(path, config()).unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut restored = Vec::new();
    for owner in &owners {
        let account = reopened.clone().open_account(identity(owner.name)).unwrap();
        assert_eq!(account.account_id(), owner.account);
        assert_eq!(account.session_epoch(), owner.epoch);
        let catalog = account.open_catalog(configuration(owner.key)).unwrap();
        assert_eq!(catalog.catalog_id(), owner.catalog);
        catalog
            .register_handler("echo".into(), Arc::new(Echo(calls.clone())), 1000)
            .unwrap();
        let run = catalog.prepare_run(request("process-crash")).unwrap();
        assert_eq!(
            run.instance_id(),
            owner.run,
            "durable operation keys must not rematerialize after process death"
        );
        restored.push((catalog, run));
    }
    // Restore every binding before scheduling any owner. Otherwise this test
    // itself introduces missing-handler deferrals in unrelated catalogs.
    for (owner, (catalog, run)) in owners.iter().zip(&restored) {
        let uncertain = dispatched
            .iter()
            .any(|effect| effect["run"].as_str() == Some(owner.run.as_str()));
        let expected = if uncertain {
            InstanceStateKind::Failed
        } else {
            InstanceStateKind::Completed
        };
        let snapshot = advance(&reopened, run, expected);
        assert_eq!(snapshot.account_id, owner.account);
        assert_eq!(snapshot.catalog_id, owner.catalog);
        assert!(reopened.get_instance(owner.run.clone()).is_err());
        for foreign in &owners {
            if foreign.catalog != owner.catalog {
                assert!(
                    catalog
                        .run(foreign.run.clone())
                        .unwrap()
                        .snapshot()
                        .unwrap()
                        .is_none()
                );
            }
        }
    }
    let calls = calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        2,
        "only the two undispatched runs may enter a new host callback"
    );
    for effect in &dispatched {
        let owner = owners
            .iter()
            .find(|owner| effect["run"].as_str() == Some(owner.run.as_str()))
            .unwrap();
        assert_eq!(effect["account"], owner.account);
        assert_eq!(effect["catalog"], owner.catalog);
        assert_eq!(effect["epoch"], owner.epoch);
        assert!(
            calls.iter().all(|call| call.run_id != owner.run
                && call.effect_id != effect["effect"].as_str().unwrap())
        );
    }
    assert_eq!(
        effects(&effects_path),
        dispatched,
        "killed native effects must not be replayed"
    );
    drop(calls);
    checkpoint_and_close(&reopened);
}
