use super::*;

#[tokio::test]
async fn independent_database_cannot_consume_a_deferred_parent_wake() {
    let original: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let unrelated: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (parent, _, child_id) =
        seed_sub_sequence_parent(&original, InstanceState::Running, InstanceState::Completed).await;
    let child = original.get_instance(child_id).await.unwrap().unwrap();
    wake_parent_if_child(&original, &child).await;
    tokio::time::sleep(DEFERRED_WAKE_RETRY + Duration::from_millis(100)).await;
    retry_deferred_parent_wakes(&unrelated).await;
    assert!(
        original
            .conditional_update_instance_state(
                parent.id,
                InstanceState::Running,
                InstanceState::Waiting,
                None
            )
            .await
            .unwrap()
    );
    retry_deferred_parent_wakes(&original).await;
    assert_eq!(
        original
            .get_instance(parent.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Scheduled,
        "another database's tick must not drop this backend's deferred wake"
    );
}

#[tokio::test]
async fn colliding_instance_id_in_another_database_cannot_receive_a_deferred_wake() {
    let original: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let unrelated: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (parent, _, child_id) =
        seed_sub_sequence_parent(&original, InstanceState::Running, InstanceState::Completed).await;
    seed_instance_in_state(unrelated.as_ref(), parent.id, None, InstanceState::Waiting).await;
    let child = original.get_instance(child_id).await.unwrap().unwrap();
    wake_parent_if_child(&original, &child).await;
    tokio::time::sleep(DEFERRED_WAKE_RETRY + Duration::from_millis(100)).await;
    retry_deferred_parent_wakes(&unrelated).await;
    assert_eq!(
        unrelated
            .get_instance(parent.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Waiting,
        "a deferred wake must never mutate a colliding instance in a different database"
    );
}

async fn wait_until_due() {
    tokio::time::sleep(DEFERRED_WAKE_RETRY + Duration::from_millis(100)).await;
}

#[tokio::test]
async fn arc_clones_deliver_the_same_backends_wake() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (parent, _, _) =
        seed_sub_sequence_parent(&storage, InstanceState::Waiting, InstanceState::Completed).await;
    defer_parent_wake(&storage, parent.id);
    wait_until_due().await;
    retry_deferred_parent_wakes(&Arc::clone(&storage)).await;
    assert_eq!(
        storage
            .get_instance(parent.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Scheduled
    );
}

#[tokio::test]
async fn retired_backend_is_not_retained_or_replaced_by_a_colliding_database() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (parent, _, _) =
        seed_sub_sequence_parent(&storage, InstanceState::Running, InstanceState::Completed).await;
    defer_parent_wake(&storage, parent.id);
    let retired = Arc::downgrade(&storage);
    drop(storage);
    assert!(
        retired.upgrade().is_none(),
        "queued retries must not retain closed database/runtime handles"
    );
    let replacement: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    seed_instance_in_state(
        replacement.as_ref(),
        parent.id,
        None,
        InstanceState::Waiting,
    )
    .await;
    wait_until_due().await;
    retry_deferred_parent_wakes(&replacement).await;
    assert_eq!(
        replacement
            .get_instance(parent.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Waiting
    );
}

#[tokio::test]
async fn backend_decorators_do_not_drain_another_adapters_wakes() {
    let raw: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (view, _) = orch8_storage::testing::faulty::FaultStorage::wrap(Arc::clone(&raw));
    let (parent, _, _) =
        seed_sub_sequence_parent(&view, InstanceState::Waiting, InstanceState::Completed).await;
    defer_parent_wake(&view, parent.id);
    wait_until_due().await;
    retry_deferred_parent_wakes(&raw).await;
    assert_eq!(
        raw.get_instance(parent.id).await.unwrap().unwrap().state,
        InstanceState::Waiting
    );
    retry_deferred_parent_wakes(&view).await;
    assert_eq!(
        raw.get_instance(parent.id).await.unwrap().unwrap().state,
        InstanceState::Scheduled
    );
}

#[tokio::test]
async fn failed_initial_parent_wake_is_retried_by_its_owner() {
    let raw: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = orch8_storage::testing::faulty::FaultStorage::wrap(raw);
    let (parent, _, child) =
        seed_sub_sequence_parent(&storage, InstanceState::Waiting, InstanceState::Completed).await;
    let child = storage.get_instance(child).await.unwrap().unwrap();
    faults
        .on("conditional_update_instance_state")
        .times(1)
        .fail_with(orch8_types::error::StorageError::PoolExhausted);
    wake_parent_if_child(&storage, &child).await;
    assert_eq!(
        storage
            .get_instance(parent.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Waiting
    );
    wait_until_due().await;
    retry_deferred_parent_wakes(&storage).await;
    assert_eq!(
        storage
            .get_instance(parent.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Scheduled
    );
}

#[tokio::test]
async fn failed_compare_and_set_keeps_the_pending_parent_wake() {
    let raw: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = orch8_storage::testing::faulty::FaultStorage::wrap(Arc::clone(&raw));
    let (parent, _, _) =
        seed_sub_sequence_parent(&storage, InstanceState::Waiting, InstanceState::Completed).await;
    defer_parent_wake(&storage, parent.id);
    wait_until_due().await;
    let paused = faults
        .on("conditional_update_instance_state")
        .times(1)
        .pause();
    let retry = {
        let storage = Arc::clone(&storage);
        tokio::spawn(async move { retry_deferred_parent_wakes(&storage).await })
    };
    tokio::time::timeout(Duration::from_secs(5), paused.entered())
        .await
        .unwrap();
    assert!(
        raw.conditional_update_instance_state(
            parent.id,
            InstanceState::Waiting,
            InstanceState::Running,
            None
        )
        .await
        .unwrap()
    );
    paused.release();
    retry.await.unwrap();
    assert!(
        raw.conditional_update_instance_state(
            parent.id,
            InstanceState::Running,
            InstanceState::Waiting,
            None
        )
        .await
        .unwrap()
    );
    wait_until_due().await;
    retry_deferred_parent_wakes(&storage).await;
    assert_eq!(
        raw.get_instance(parent.id).await.unwrap().unwrap().state,
        InstanceState::Scheduled
    );
}

#[tokio::test]
async fn a_new_deferral_survives_completion_of_an_older_retry() {
    let raw: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = orch8_storage::testing::faulty::FaultStorage::wrap(Arc::clone(&raw));
    let (parent, _, _) =
        seed_sub_sequence_parent(&storage, InstanceState::Waiting, InstanceState::Completed).await;
    defer_parent_wake(&storage, parent.id);
    wait_until_due().await;
    let paused = faults
        .on("conditional_update_instance_state")
        .times(1)
        .pause();
    let retry = {
        let storage = Arc::clone(&storage);
        tokio::spawn(async move { retry_deferred_parent_wakes(&storage).await })
    };
    tokio::time::timeout(Duration::from_secs(5), paused.entered())
        .await
        .unwrap();
    defer_parent_wake(&storage, parent.id);
    paused.release();
    retry.await.unwrap();
    assert!(
        raw.conditional_update_instance_state(
            parent.id,
            InstanceState::Scheduled,
            InstanceState::Waiting,
            None
        )
        .await
        .unwrap()
    );
    wait_until_due().await;
    retry_deferred_parent_wakes(&storage).await;
    assert_eq!(
        raw.get_instance(parent.id).await.unwrap().unwrap().state,
        InstanceState::Scheduled,
        "the older successful attempt must not remove a newly queued event"
    );
}

#[tokio::test]
async fn owned_cancel_signal_preserves_the_deferred_parent_wake() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (parent, _, child) =
        seed_sub_sequence_parent(&storage, InstanceState::Running, InstanceState::Running).await;
    storage
        .enqueue_signal(&Signal {
            id: Uuid::now_v7(),
            instance_id: child,
            signal_type: SignalType::Cancel,
            payload: serde_json::Value::Null,
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        })
        .await
        .unwrap();
    assert!(
        crate::signals::process_signals_owned(&storage, child, InstanceState::Running)
            .await
            .unwrap()
    );
    assert_eq!(
        storage.get_instance(child).await.unwrap().unwrap().state,
        InstanceState::Cancelled
    );
    assert!(
        storage
            .conditional_update_instance_state(
                parent.id,
                InstanceState::Running,
                InstanceState::Waiting,
                None
            )
            .await
            .unwrap()
    );
    wait_until_due().await;
    retry_deferred_parent_wakes(&storage).await;
    assert_eq!(
        storage
            .get_instance(parent.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Scheduled
    );
}
