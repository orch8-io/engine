#[path = "../../packages/native-common/runtime.rs"]
mod host_runtime;

use host_runtime::HostRuntime;
use std::{
    sync::{Arc, mpsc},
    time::Duration,
};

fn runtime() -> HostRuntime {
    HostRuntime::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    )
}

#[tokio::test]
async fn host_runtime_can_retire_from_an_async_task() {
    drop(runtime());
}

#[test]
fn retirement_preserves_physical_work_without_waiting_for_it() {
    let runtime = runtime();
    let (entered, entry) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let (finished, finish) = mpsc::channel();
    let resource = Arc::new(());
    let retained = Arc::downgrade(&resource);
    runtime.block_on(async move {
        let call = tokio::task::spawn_blocking(move || {
            entered.send(()).unwrap();
            wait.recv().unwrap();
            drop(resource);
            finished.send(()).unwrap();
        });
        // Model a logical timeout dropping its join observer while the
        // physical blocking callback still owns its resources.
        drop(call);
    });
    entry.recv_timeout(Duration::from_secs(5)).unwrap();
    let (dropping, started) = mpsc::channel();
    let (retired, retirement) = mpsc::channel();
    let owner = std::thread::spawn(move || {
        dropping.send(()).unwrap();
        drop(runtime);
        let _ = retired.send(());
    });
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    let returned = retirement.recv_timeout(Duration::from_secs(5)).is_ok();
    let physical_resource_retained = retained.upgrade().is_some();
    release.send(()).unwrap();
    finish.recv_timeout(Duration::from_secs(5)).unwrap();
    owner.join().unwrap();
    assert!(returned, "runtime retirement waited for blocked host work");
    assert!(
        physical_resource_retained,
        "retirement released a live physical capture"
    );
    assert!(
        retained.upgrade().is_none(),
        "completed callback retained its capture"
    );
}
