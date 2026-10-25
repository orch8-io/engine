//! Immediate crash recovery requires exclusive native database ownership.
#[allow(dead_code)]
#[path = "../../packages/native-common/durable.rs"]
mod durable;

use durable::DurableEngine;
use serde_json::json;
use std::{path::PathBuf, time::Duration};

fn directory() -> PathBuf {
    let root = std::env::temp_dir().join(format!("orch8-native-owner-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&root).unwrap();
    root
}
async fn deploy(engine: &DurableEngine) -> Result<String, String> {
    engine
        .deploy(
            &json!({"name":"owner","blocks":[{"type":"step","id":"one","handler":"noop"}]})
                .to_string(),
        )
        .await
}
#[tokio::test]
async fn second_owner_is_refused_before_recovery_and_can_retry_after_close() {
    let root = directory();
    let path = root.join("engine.db");
    let first = DurableEngine::new(path.to_string_lossy());
    deploy(&first).await.unwrap();
    let second = DurableEngine::new(path.to_string_lossy());
    let overlap = deploy(&second).await;
    first.close().await;
    let retry = deploy(&second).await;
    second.close().await;
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        overlap.is_err(),
        "two native owners opened the same database"
    );
    retry.unwrap();
}
#[tokio::test]
async fn a_second_owner_cannot_recover_a_live_host_claim() {
    use std::sync::Arc;
    use tokio::sync::{Notify, watch};
    let root = directory();
    let path = root.join("engine.db");
    let first = Arc::new(DurableEngine::new(path.to_string_lossy()));
    let entered = Arc::new(Notify::new());
    let (release, held) = watch::channel(false);
    let started = entered.clone();
    first
        .register(
            "host",
            Arc::new(move |_| {
                let mut held = held.clone();
                started.notify_one();
                Box::pin(async move {
                    held.wait_for(|done| *done).await.unwrap();
                    Ok(json!({"ok":true}).to_string())
                })
            }),
        )
        .unwrap();
    first.deploy(&json!({"name":"held","blocks":[{"type":"step","id":"one","handler":"host","timeout":10000}]}).to_string()).await.unwrap();
    let id = first.start("held", None, None, None).await.unwrap();
    let caller = first.clone();
    let target = id.clone();
    let run = tokio::spawn(async move { caller.run(&target, Some(Duration::from_secs(5))).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let second = DurableEngine::new(path.to_string_lossy());
    let overlap = deploy(&second).await;
    release.send_replace(true);
    let result = run.await.unwrap().unwrap();
    first.close().await;
    second.close().await;
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        overlap.is_err(),
        "second engine admitted recovery of a healthy owner's claim"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&result).unwrap()["state"],
        "completed"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn symlinks_share_ownership_and_hard_links_are_refused() {
    let root = directory();
    let path = root.join("engine.db");
    let first = DurableEngine::new(path.to_string_lossy());
    deploy(&first).await.unwrap();
    let alias = root.join("alias.db");
    std::os::unix::fs::symlink(&path, &alias).unwrap();
    let second = DurableEngine::new(alias.to_string_lossy());
    assert!(deploy(&second).await.is_err());
    first.close().await;
    deploy(&second).await.unwrap();
    second.close().await;
    let hard = root.join("hard.db");
    std::fs::hard_link(&path, &hard).unwrap();
    let linked = DurableEngine::new(hard.to_string_lossy());
    assert!(deploy(&linked).await.unwrap_err().contains("hard links"));
    linked.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn database_owner_child() {
    let Some(path) = std::env::var_os("ORCH8_NATIVE_OWNER_CHILD_PATH") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let engine = DurableEngine::new(path.to_str().unwrap());
        let error = deploy(&engine).await.unwrap_err();
        assert!(error.contains("owned"), "unexpected refusal: {error}");
        engine.close().await;
    });
}

#[tokio::test]
async fn another_process_cannot_acquire_live_recovery_authority() {
    let root = directory();
    let path = root.join("engine.db");
    let first = DurableEngine::new(path.to_string_lossy());
    deploy(&first).await.unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "database_owner_child", "--nocapture"])
        .env("ORCH8_NATIVE_OWNER_CHILD_PATH", &path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let limit = std::time::Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break Some(status);
        }
        if std::time::Instant::now() >= limit {
            child.kill().unwrap();
            child.wait().unwrap();
            break None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    first.close().await;
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        status.is_some_and(|status| status.success()),
        "ownership child failed or hung"
    );
}

#[tokio::test]
async fn dropping_owner_keeps_authority_with_its_live_pass() {
    use std::sync::Arc;
    use tokio::sync::{Notify, watch};
    let root = directory();
    let path = root.join("engine.db");
    let first = Arc::new(DurableEngine::new(path.to_string_lossy()));
    let entered = Arc::new(Notify::new());
    let started = entered.clone();
    let (release, held) = watch::channel(false);
    first
        .register(
            "host",
            Arc::new(move |_| {
                let mut held = held.clone();
                started.notify_one();
                Box::pin(async move {
                    held.wait_for(|done| *done).await.unwrap();
                    Ok(json!({"ok":true}).to_string())
                })
            }),
        )
        .unwrap();
    first.deploy(&json!({"name":"held","blocks":[{"type":"step","id":"one","handler":"host","timeout":10000}]}).to_string()).await.unwrap();
    let id = first.start("held", None, None, None).await.unwrap();
    let caller = first.clone();
    let run = tokio::spawn(async move { caller.run(&id, Some(Duration::from_millis(100))).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    run.await.unwrap().unwrap();
    drop(first);
    let second = DurableEngine::new(path.to_string_lossy());
    let overlap = deploy(&second).await;
    release.send_replace(true);
    let retry = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match deploy(&second).await {
                Ok(id) => break id,
                Err(error) if error.contains("owned") => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("unexpected retry failure: {error}"),
            }
        }
    })
    .await;
    second.close().await;
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        overlap.is_err(),
        "drop surrendered authority while a pass remained live"
    );
    retry.unwrap();
}

#[tokio::test]
async fn independent_memory_databases_do_not_share_an_owner_lock() {
    let first = DurableEngine::new(":memory:");
    let second = DurableEngine::new(":memory:");
    deploy(&first).await.unwrap();
    deploy(&second).await.unwrap();
    first.close().await;
    second.close().await;
}

#[tokio::test]
async fn failed_initialization_releases_its_reservation() {
    let root = directory();
    let path = root.join("engine.db");
    std::fs::write(&path, b"invalid sqlite database").unwrap();
    let first = DurableEngine::new(path.to_string_lossy());
    assert!(deploy(&first).await.is_err());
    std::fs::remove_file(&path).unwrap();
    let second = DurableEngine::new(path.to_string_lossy());
    deploy(&second).await.unwrap();
    first.close().await;
    second.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

use durable::database as ownership;

#[tokio::test]
async fn cancelled_initializer_retains_authority_until_its_build_settles() {
    use std::sync::Arc;
    let root = directory();
    let path = root.join("engine.db");
    let owner = ownership::Owner::acquire(path.to_str().unwrap()).unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let started = entered.clone();
    let (release, held) = tokio::sync::oneshot::channel();
    let (finished, ended) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(ownership::initialize(owner, async move {
        started.notify_one();
        held.await.unwrap();
        let engine = orch8::Engine::builder()
            .storage(orch8::Storage::sqlite_in_memory())
            .build()
            .await?;
        let _ = finished.send(());
        Ok(engine)
    }));
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    let overlap = ownership::Owner::acquire(path.to_str().unwrap());
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), ended)
        .await
        .unwrap()
        .unwrap();
    let retry = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(owner) = ownership::Owner::acquire(path.to_str().unwrap()) {
                break owner;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    drop(retry.unwrap());
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        overlap.is_err(),
        "cancelled observer surrendered a live initialization lock"
    );
}
