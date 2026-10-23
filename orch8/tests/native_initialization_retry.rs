//! Include the shared Node/Python implementation in an ordinary Rust test.
#[allow(dead_code)]
#[path = "../../packages/native-common/durable.rs"]
mod durable;

use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn failed_native_initialization_does_not_discard_registered_host_handlers() {
    let root = std::env::temp_dir().join(format!("orch8-native-init-{}", uuid::Uuid::now_v7()));
    std::fs::write(&root, "block database directory creation").unwrap();
    let engine = durable::DurableEngine::new(root.join("engine.db").to_string_lossy());
    engine
        .register(
            "host",
            Arc::new(|_| Box::pin(async { Ok(json!({"ok":{"called":true}}).to_string()) })),
        )
        .unwrap();
    let sequence =
        json!({"name":"retry-init", "blocks":[{"type":"step","id":"work","handler":"host"}]})
            .to_string();
    assert!(engine.deploy(&sequence).await.is_err());
    std::fs::remove_file(&root).unwrap();
    std::fs::create_dir(&root).unwrap();
    engine.deploy(&sequence).await.unwrap();
    let id = engine.start("retry-init", None, None, None).await.unwrap();
    let snapshot = engine
        .run(&id, Some(std::time::Duration::from_secs(2)))
        .await
        .unwrap();
    engine.close().await;
    let _ = std::fs::remove_dir_all(&root);
    let snapshot: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
    assert_eq!(
        snapshot["state"], "completed",
        "registered host handler disappeared after a recoverable initial open failure: {snapshot}"
    );
    assert_eq!(snapshot["outputs"]["work"]["called"], true);
}

#[tokio::test]
async fn closing_an_unused_engine_rejects_later_handler_registration() {
    let engine = durable::DurableEngine::new(":memory:");
    engine.close().await;
    let result = engine.register(
        "late",
        Arc::new(|_| Box::pin(async { Ok(json!({"ok":true}).to_string()) })),
    );
    assert!(
        result.is_err(),
        "closed native engine accepted a callback it can never execute"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn close_releases_the_owned_sqlite_connections() {
    let root = std::env::temp_dir().join(format!("orch8-native-close-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("engine.db");
    let engine = durable::DurableEngine::new(path.to_string_lossy());
    engine
        .deploy(
            &json!({"name":"close-db", "blocks":[{"type":"step","id":"work","handler":"noop"}]})
                .to_string(),
        )
        .await
        .unwrap();
    let wal = root.join("engine.db-wal");
    assert!(wal.exists(), "fixture must open its database in WAL mode");
    engine.close().await;
    let released = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while wal.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    drop(engine);
    let _ = std::fs::remove_dir_all(&root);
    assert!(
        released,
        "close retained idle SQLite connections and their WAL files"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn wal_fixture_observes_graceful_pool_shutdown() {
    let root =
        std::env::temp_dir().join(format!("orch8-native-wal-control-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("engine.db");
    let storage = orch8_storage::sqlite::SqliteStorage::file(&path.to_string_lossy())
        .await
        .unwrap();
    let wal = root.join("engine.db-wal");
    assert!(wal.exists());
    storage.pool().close().await;
    assert!(
        !wal.exists(),
        "WAL disappearance must identify graceful connection release"
    );
    std::fs::remove_dir_all(root).unwrap();
}

async fn verify_close_releases_callbacks(initialize: bool, fail_initialization: bool) {
    let root = std::env::temp_dir().join(format!("orch8-native-callback-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&root).unwrap();
    let database = if fail_initialization {
        let blocker = root.join("blocker");
        std::fs::write(&blocker, "not a directory").unwrap();
        blocker.join("engine.db")
    } else {
        root.join("engine.db")
    };
    let engine = durable::DurableEngine::new(database.to_string_lossy());
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let invoke: durable::Invoke = Arc::new(move |_| {
        let resource = Arc::clone(&resource);
        Box::pin(async move {
            drop(resource);
            Ok(json!({"ok":true}).to_string())
        })
    });
    engine.register("host", invoke).unwrap();
    if initialize {
        let result = engine.deploy(&json!({"name":"callback-close","blocks":[{"type":"step","id":"work","handler":"noop"}]}).to_string()).await;
        if fail_initialization {
            assert!(result.is_err());
        } else {
            result.unwrap();
        }
    }
    engine.close().await;
    let released = weak.upgrade().is_none();
    drop(engine);
    assert!(
        weak.upgrade().is_none(),
        "fixture must release the last callback owner on drop"
    );
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        released,
        "close retained a registered callback even though the engine can never invoke it again"
    );
}

#[tokio::test]
async fn close_releases_unused_handler_registrations() {
    verify_close_releases_callbacks(false, false).await;
}

#[tokio::test]
async fn close_releases_frozen_handler_registrations() {
    verify_close_releases_callbacks(true, false).await;
}

#[tokio::test]
async fn close_releases_handlers_after_failed_initialization() {
    verify_close_releases_callbacks(true, true).await;
}
