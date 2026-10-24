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

#[tokio::test]
async fn reopened_database_recovers_parent_after_post_park_read_failure() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("parent-recovery.sqlite");
    let path = path.to_str().unwrap();
    let raw = Arc::new(SqliteStorage::file(path).await.unwrap());
    let original: Arc<dyn StorageBackend> = raw.clone();
    let (storage, faults) = orch8_storage::testing::faulty::FaultStorage::wrap(original);
    let (parent, mut sequence, _) =
        seed_sub_sequence_parent(&storage, InstanceState::Running, InstanceState::Completed).await;
    sequence.id = parent.sequence_id;
    storage.create_sequence(&sequence).await.unwrap();
    faults.on("get_execution_tree").times(1).fail_transient();
    run_more_work(&storage, &parent, &sequence).await;
    assert_eq!(
        storage
            .get_instance(parent.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Waiting
    );
    let retired = Arc::downgrade(&storage);
    drop(storage);
    assert!(retired.upgrade().is_none());
    raw.pool().close().await;
    drop(raw);
    // The old backend's volatile registry cannot deliver into this lifetime.
    let reopened: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::file(path).await.unwrap());
    sweep_waiting(&reopened, &mut WaitingSweepCursor::default(), 10, 1).await;
    assert_eq!(
        reopened
            .get_instance(parent.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Scheduled,
        "durable child state must recover a parent after volatile wake ownership ends"
    );
}

async fn persisted_waiting_parent(
    storage: &Arc<dyn StorageBackend>,
    child_state: InstanceState,
) -> (TaskInstance, InstanceId) {
    let mut sequence = mk_sequence(vec![BlockDefinition::SubSequence(Box::new(
        orch8_types::sequence::SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child".into(),
            version: None,
            input: serde_json::json!({}),
        },
    ))]);
    sequence.name = sequence.id.to_string();
    storage.create_sequence(&sequence).await.unwrap();
    let parent = mk_waiting_instance(sequence.id, chrono::Duration::zero(), Utc::now());
    storage.create_instance(&parent).await.unwrap();
    let mut child = parent.clone();
    child.id = InstanceId::new();
    child.parent_instance_id = Some(parent.id);
    child.state = child_state;
    child.metadata = serde_json::json!({"_parent_block_id": "sub"});
    storage.create_instance(&child).await.unwrap();
    storage
        .create_execution_node(&orch8_types::execution::ExecutionNode {
            id: orch8_types::ids::ExecutionNodeId::new(),
            instance_id: parent.id,
            block_id: BlockId::new("sub"),
            parent_id: None,
            block_type: orch8_types::execution::BlockType::SubSequence,
            branch_index: None,
            state: orch8_types::execution::NodeState::Waiting,
            started_at: Some(Utc::now()),
            completed_at: None,
        })
        .await
        .unwrap();
    (parent, child.id)
}

#[tokio::test]
async fn waiting_parent_recovery_only_wakes_for_terminal_children() {
    for state in [
        InstanceState::Scheduled,
        InstanceState::Running,
        InstanceState::Waiting,
        InstanceState::Paused,
        InstanceState::Completed,
        InstanceState::Failed,
        InstanceState::Cancelled,
    ] {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let (parent, _) = persisted_waiting_parent(&storage, state).await;
        sweep_waiting(&storage, &mut WaitingSweepCursor::default(), 10, 1).await;
        assert_eq!(
            storage
                .get_instance(parent.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            if state.is_terminal() {
                InstanceState::Scheduled
            } else {
                InstanceState::Waiting
            },
            "child state {state:?}"
        );
    }
}

#[tokio::test]
async fn waiting_parent_recovery_ignores_previous_activations_and_other_blocks() {
    for metadata in [
        serde_json::json!({"_parent_activation": "old-node/loop:0"}),
        serde_json::json!({"_parent_block_id": "different-block"}),
    ] {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let (parent, child) = persisted_waiting_parent(&storage, InstanceState::Completed).await;
        storage
            .merge_instance_metadata(child, &metadata)
            .await
            .unwrap();
        sweep_waiting(&storage, &mut WaitingSweepCursor::default(), 10, 1).await;
        assert_eq!(
            storage
                .get_instance(parent.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            InstanceState::Waiting
        );
    }
}

#[tokio::test]
async fn waiting_parent_recovery_retries_write_failures_without_volatile_events() {
    let raw: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = orch8_storage::testing::faulty::FaultStorage::wrap(raw);
    let (parent, _) = persisted_waiting_parent(&storage, InstanceState::Completed).await;
    faults
        .on("conditional_update_instance_state")
        .times(1)
        .fail_transient();
    let mut cursor = WaitingSweepCursor::default();
    sweep_waiting(&storage, &mut cursor, 10, 1).await;
    assert_eq!(
        storage
            .get_instance(parent.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Waiting
    );
    sweep_waiting(&storage, &mut cursor, 10, 1).await;
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
async fn waiting_parent_recovery_does_not_overwrite_a_concurrent_pause() {
    let raw: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = orch8_storage::testing::faulty::FaultStorage::wrap(Arc::clone(&raw));
    let (parent, _) = persisted_waiting_parent(&storage, InstanceState::Completed).await;
    let paused = faults
        .on("conditional_update_instance_state")
        .times(1)
        .pause();
    let sweep = {
        let storage = Arc::clone(&storage);
        tokio::spawn(async move {
            sweep_waiting(&storage, &mut WaitingSweepCursor::default(), 10, 1).await;
        })
    };
    tokio::time::timeout(Duration::from_secs(5), paused.entered())
        .await
        .unwrap();
    assert!(
        raw.conditional_update_instance_state(
            parent.id,
            InstanceState::Waiting,
            InstanceState::Paused,
            None
        )
        .await
        .unwrap()
    );
    paused.release();
    tokio::time::timeout(Duration::from_secs(5), sweep)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        raw.get_instance(parent.id).await.unwrap().unwrap().state,
        InstanceState::Paused
    );
}

#[tokio::test]
async fn waiting_parent_recovery_rotates_past_the_page_cap() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (one, _) = persisted_waiting_parent(&storage, InstanceState::Running).await;
    let (two, _) = persisted_waiting_parent(&storage, InstanceState::Running).await;
    let (three, _) = persisted_waiting_parent(&storage, InstanceState::Completed).await;
    let mut cursor = WaitingSweepCursor::default();
    sweep_waiting(&storage, &mut cursor, 1, 1).await;
    sweep_waiting(&storage, &mut cursor, 1, 1).await;
    assert_eq!(
        storage.get_instance(three.id).await.unwrap().unwrap().state,
        InstanceState::Waiting
    );
    sweep_waiting(&storage, &mut cursor, 1, 1).await;
    assert_eq!(
        storage.get_instance(three.id).await.unwrap().unwrap().state,
        InstanceState::Scheduled
    );
    for parent in [one, two] {
        assert_eq!(
            storage
                .get_instance(parent.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            InstanceState::Waiting
        );
    }
}

#[tokio::test]
async fn embedded_tick_recovers_waiting_parent_and_drains_completed_child() {
    for backend in fencing_backends().await {
        let storage = Arc::clone(&backend.storage);
        let (parent, _) = persisted_waiting_parent(&storage, InstanceState::Completed).await;
        let registry = Arc::new(HandlerRegistry::new());
        let semaphore = Arc::new(Semaphore::new(2));
        let cache = Arc::new(SequenceCache::new(16, Duration::from_secs(60)));
        let cancel = CancellationToken::new();
        let config = SchedulerConfig::default();
        let first = tick_once(&storage, &registry, &semaphore, &config, &cache, &cancel)
            .await
            .unwrap();
        assert!(
            first.has_pending_work,
            "embedded caller must keep draining recovered work"
        );
        assert_eq!(
            storage
                .get_instance(parent.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            InstanceState::Scheduled
        );
        let second = tick_once(&storage, &registry, &semaphore, &config, &cache, &cancel)
            .await
            .unwrap();
        assert_eq!(
            storage
                .get_instance(parent.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            InstanceState::Completed
        );
        assert!(!second.has_pending_work);
    }
}
