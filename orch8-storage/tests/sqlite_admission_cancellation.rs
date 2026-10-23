//! Cancellation inside a native `SQLite` write must not poison a pooled connection.
use orch8_storage::{
    InstanceStore as _, SequenceStore as _,
    sqlite::SqliteStorage,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::instance::InstanceState;
use std::{
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);
impl ReleaseOnDrop {
    fn release(&self) {
        let (released, wake) = self.0.as_ref();
        *released.lock().unwrap() = true;
        wake.notify_all();
    }
}
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.release();
    }
}

struct PausedWrite {
    entered: Arc<tokio::sync::Notify>,
    release: ReleaseOnDrop,
}
impl PausedWrite {
    async fn install(storage: &SqliteStorage) -> Self {
        let entered = Arc::new(tokio::sync::Notify::new());
        let worker_entered = Arc::clone(&entered);
        let release = ReleaseOnDrop(Arc::new((Mutex::new(false), Condvar::new())));
        let worker_release = Arc::clone(&release.0);
        let mut connection = storage.pool().acquire().await.unwrap();
        let mut handle = connection.lock_handle().await.unwrap();
        let mut paused = false;
        handle.set_update_hook(move |event| {
            if !paused && event.table == "task_instances" {
                paused = true;
                worker_entered.notify_one();
                let (released, wake) = worker_release.as_ref();
                let mut done = released.lock().unwrap();
                while !*done {
                    done = wake.wait(done).unwrap();
                }
            }
        });
        Self { entered, release }
    }

    async fn abort<T>(&self, operation: tokio::task::JoinHandle<T>) {
        tokio::time::timeout(Duration::from_secs(3), self.entered.notified())
            .await
            .expect("operation did not reach its native write");
        operation.abort();
        match operation.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("native operation completed before cancellation"),
        }
        self.release.release();
    }
}

async fn assert_pool_usable(storage: &SqliteStorage) {
    // A new real transaction detects raw BEGINs that SQLx's tracked depth
    // alone cannot identify. Also verify the sole connection is released.
    let tx = tokio::time::timeout(
        Duration::from_secs(3),
        storage.pool().begin_with("BEGIN IMMEDIATE"),
    )
    .await
    .expect("aborted operation retained SQLite's write lock")
    .expect("aborted operation poisoned the pooled transaction state");
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn aborting_admission_rolls_back_its_insert_and_releases_the_single_connection() {
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = SequenceBuilder::new("admission-abort")
        .step("work", "noop")
        .build();
    storage.create_sequence(&seq).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&seq).due_now().build();
    let id = instance.id;
    let pause = PausedWrite::install(&storage).await;
    let old_storage = Arc::clone(&storage);
    pause
        .abort(tokio::spawn(async move {
            old_storage.create_instance_admitted(&instance, 1).await
        }))
        .await;
    let found = tokio::time::timeout(Duration::from_secs(3), storage.get_instance(id))
        .await
        .expect("aborted admission retained the only pooled connection")
        .unwrap();
    assert!(
        found.is_none(),
        "aborted admission left an uncommitted instance visible on the pooled connection"
    );
    assert_pool_usable(&storage).await;
}

#[tokio::test]
async fn aborting_batch_admission_rolls_back_every_insert() {
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = SequenceBuilder::new("batch-admission-abort")
        .step("work", "noop")
        .build();
    storage.create_sequence(&seq).await.unwrap();
    let instances = vec![
        InstanceBuilder::for_sequence(&seq).due_now().build(),
        InstanceBuilder::for_sequence(&seq).due_now().build(),
    ];
    let ids: Vec<_> = instances.iter().map(|i| i.id).collect();
    let limits = std::collections::HashMap::from([(seq.tenant_id, 10)]);
    let pause = PausedWrite::install(&storage).await;
    let old_storage = Arc::clone(&storage);
    pause
        .abort(tokio::spawn(async move {
            old_storage
                .create_instances_batch_admitted(&instances, &limits)
                .await
        }))
        .await;
    for id in ids {
        assert!(
            storage.get_instance(id).await.unwrap().is_none(),
            "cancelled batch retained a partially inserted instance"
        );
    }
    assert_pool_usable(&storage).await;
}

#[tokio::test]
async fn aborting_due_claim_rolls_back_its_lease_and_keeps_the_pool_reusable() {
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = SequenceBuilder::new("claim-abort")
        .step("work", "noop")
        .build();
    storage.create_sequence(&seq).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&seq).due_now().build();
    storage.create_instance(&instance).await.unwrap();
    let pause = PausedWrite::install(&storage).await;
    let old_storage = Arc::clone(&storage);
    pause
        .abort(tokio::spawn(async move {
            old_storage
                .claim_due_instances(chrono::Utc::now(), 1, 0)
                .await
        }))
        .await;
    let after = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(
        after.state,
        InstanceState::Scheduled,
        "cancelled claim left an uncommitted Running instance"
    );
    assert_eq!(after.claimed_at, None);
    assert_eq!(after.updated_at, instance.updated_at);
    assert_pool_usable(&storage).await;
    let claimed = storage
        .claim_due_instances(chrono::Utc::now(), 1, 0)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, instance.id);
}
