//! A legacy device credential cannot report or command catalog-owned work.
use super::{tests::*, *};
use crate::storage::scopes::test_support::scoped_run;

#[tokio::test]
async fn legacy_sync_scan_excludes_active_and_terminal_catalog_runs() {
    let (reporter, storage, lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
    let mobile = lifecycle.mobile_storage();
    for account in ["alice", "bob"] {
        for catalog in ["one", "two"] {
            let id = scoped_run(mobile, account, catalog).await;
            if catalog == "two" {
                storage
                    .conditional_update_instance_state(
                        id,
                        InstanceState::Scheduled,
                        InstanceState::Completed,
                        None,
                    )
                    .await
                    .unwrap();
            }
        }
    }
    let legacy = insert_raw_instance(mobile, "running", chrono::Utc::now()).await;
    let cache = Arc::new(SequenceCache::new(16, Duration::from_secs(30)));
    assert!(reporter.scan_and_queue(&storage, mobile, &cache).await);
    let queued_ids: Vec<String> = sqlx::query_scalar("SELECT instance_id FROM sync_outbox")
        .fetch_all(&reporter.pool)
        .await
        .unwrap();
    assert_eq!(
        queued_ids,
        vec![legacy],
        "legacy credential adopted scoped status"
    );
}

#[tokio::test]
async fn legacy_sync_commands_cannot_signal_or_replace_catalog_runs() {
    for (kind, policy) in [
        ("cancel_instance", ""),
        ("complete_step", ""),
        ("step_result", ""),
        ("update_sequence", "restart"),
        ("update_sequence", "skip_executed"),
        ("update_sequence", "cancel"),
        ("update_sequence", "fail"),
    ] {
        let (reporter, storage, lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
        seed_sequence(&storage, "wf-a").await;
        let id = scoped_run(lifecycle.mobile_storage(), "alice", "one").await;
        let command = cmd(
            "foreign-command",
            kind,
            serde_json::json!({"instance_id":id,"step_name":"work","block_id":"work","request_id":"foreign","success":true,"policy":policy,"sequence_name":"wf-a","output":{"foreign":true},"resolved_params":{"foreign":true}}),
        );
        assert_eq!(
            reporter
                .execute_command(&command, &storage, &lifecycle)
                .await,
            CommandOutcome::Done
        );
        assert_eq!(
            storage.get_instance(id).await.unwrap().unwrap().state,
            InstanceState::Scheduled,
            "{kind}/{policy} changed catalog work"
        );
        assert!(
            storage.get_pending_signals(id).await.unwrap().is_empty(),
            "{kind} injected a foreign signal"
        );
        assert_eq!(
            count_instances(&storage).await,
            1,
            "{kind}/{policy} created a legacy replacement"
        );
    }
}

#[tokio::test]
async fn legacy_sync_http_drops_previously_queued_scoped_status_and_approval() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let (start, length) = loop {
            let mut buf = [0; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buf[..n]);
            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                break (end + 4, length);
            }
        };
        while bytes.len() < start + length {
            let mut buf = [0; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buf[..n]);
        }
        let body: serde_json::Value =
            serde_json::from_slice(&bytes[start..start + length]).unwrap();
        let response = r#"{"commands":[]}"#;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                    response.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        body
    });
    let (reporter, storage, lifecycle) = setup(format!("http://{address}/sync")).await;
    let foreign = scoped_run(lifecycle.mobile_storage(), "alice", "one").await;
    let legacy =
        insert_raw_instance(lifecycle.mobile_storage(), "running", chrono::Utc::now()).await;
    for kind in ["status", "approval"] {
        for id in [foreign.to_string(), legacy.clone()] {
            let key = if kind == "approval" {
                format!("{id}:work")
            } else {
                id.clone()
            };
            sqlx::query("INSERT INTO sync_outbox(entry_type,instance_id,payload) VALUES (?1,?2,?3)")
                .bind(kind).bind(key).bind(serde_json::json!({"instance_id":id,"private":if id==legacy {"legacy"} else {"SCOPED_CANARY"}}).to_string())
                .execute(&reporter.pool).await.unwrap();
        }
    }
    // No new scan: the protection must apply to persisted pre-fix rows too.
    tokio::time::timeout(
        Duration::from_secs(10),
        reporter.sync_once(&storage, &lifecycle),
    )
    .await
    .unwrap();
    let body = tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap();
    for field in ["status_updates", "approval_requests"] {
        let entries = body[field].as_array().unwrap();
        assert_eq!(entries.len(), 1, "{field} sent a catalog-owned report");
        assert_eq!(entries[0]["instance_id"], legacy);
    }
    assert!(!body.to_string().contains("SCOPED_CANARY"));
}

#[tokio::test]
async fn legacy_sync_rejects_owner_corruption_before_and_after_retirement() {
    let (reporter, storage, lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
    let mobile = lifecycle.mobile_storage();
    let id = scoped_run(mobile, "alice", "one").await;
    // Even a corrupted native tenant cannot erase the independent durable owner.
    sqlx::query("UPDATE task_instances SET tenant_id='mobile' WHERE id=?1")
        .bind(id.to_string())
        .execute(mobile.pool())
        .await
        .unwrap();
    let account = mobile
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    let scope = mobile
        .open_catalog_scope(&account, "one", "source", "authority")
        .await
        .unwrap();
    assert!(!mobile.cancel_catalog_run(&scope, id).await.unwrap());
    assert!(
        !mobile
            .forget_cancelled_catalog_run(&scope, id)
            .await
            .unwrap()
    );
    for retired in [false, true] {
        if retired {
            mobile.retire_account_scope(&account).await.unwrap();
        }
        assert!(
            mobile
                .legacy_sync_instance_ids(&[id.to_string()])
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            !mobile
                .transition_legacy_sync_instance(id, InstanceState::Cancelled)
                .await
                .unwrap()
        );
        let command = cmd(
            "cannot-adopt",
            "complete_step",
            serde_json::json!({"instance_id":id,"step_name":"work","output":{"foreign":true}}),
        );
        assert_eq!(
            reporter
                .execute_command(&command, &storage, &lifecycle)
                .await,
            CommandOutcome::Done
        );
        assert!(storage.get_pending_signals(id).await.unwrap().is_empty());
        let cache = Arc::new(SequenceCache::new(16, Duration::from_secs(30)));
        assert!(reporter.scan_and_queue(&storage, mobile, &cache).await);
        assert_eq!(outbox_count(&reporter).await, 0);
    }
}

#[tokio::test]
async fn legacy_sync_outbox_requires_matching_key_payload_and_durable_owner() {
    let (reporter, _storage, lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
    let mobile = lifecycle.mobile_storage();
    let foreign = scoped_run(mobile, "alice", "one").await;
    let legacy = insert_raw_instance(mobile, "running", chrono::Utc::now()).await;
    let other_legacy = insert_raw_instance(mobile, "running", chrono::Utc::now()).await;
    for approvals in [false, true] {
        let mut rows = Vec::new();
        for (key, owner) in [
            (legacy.clone(), legacy.clone()),
            (other_legacy.clone(), foreign.to_string()),
            (foreign.to_string(), legacy.clone()),
            (
                uuid::Uuid::new_v4().to_string(),
                uuid::Uuid::new_v4().to_string(),
            ),
        ] {
            let key = if approvals {
                format!("{key}:work:with:colons")
            } else {
                key
            };
            let payload = serde_json::json!({"instance_id":owner}).to_string();
            let row = sqlx::query(
                "INSERT INTO sync_outbox(entry_type,instance_id,payload) VALUES (?1,?2,?3)",
            )
            .bind(if approvals { "approval" } else { "status" })
            .bind(&key)
            .bind(&payload)
            .execute(&reporter.pool)
            .await
            .unwrap()
            .last_insert_rowid();
            rows.push((row, key, payload, false));
        }
        let allowed = reporter
            .legacy_outbox_rows(rows, mobile, approvals)
            .await
            .unwrap();
        assert_eq!(allowed.len(), 1);
        let payload: serde_json::Value = serde_json::from_str(&allowed[0].1).unwrap();
        assert_eq!(payload["instance_id"], legacy);
        let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_outbox")
            .fetch_one(&reporter.pool)
            .await
            .unwrap();
        assert_eq!(remaining, 1, "rejected rows must not starve later batches");
        sqlx::query("DELETE FROM sync_outbox")
            .execute(&reporter.pool)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn legacy_sync_authorized_signals_keep_encryption_and_terminal_fences() {
    let (reporter, raw, lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
    let backend: Arc<dyn StorageBackend> =
        Arc::new(orch8_storage::encrypting::EncryptingStorage::new(
            raw.clone(),
            orch8_types::encryption::FieldEncryptor::from_bytes(&[9; 32]),
        ));
    seed_sequence(&raw, "wf-a").await;
    let id: InstanceId = InstanceId::from_uuid(
        uuid::Uuid::parse_str(&lifecycle.start("wf-a", "{}", None).await.unwrap()).unwrap(),
    );
    for kind in ["complete_step", "step_result"] {
        let command = cmd(
            kind,
            kind,
            serde_json::json!({"instance_id":id,"step_name":"s1","block_id":"s1","request_id":"request","success":true,"output":{"private":"SIGNAL_CANARY"},"resolved_params":{"private":"SIGNAL_CANARY"}}),
        );
        for _ in 0..2 {
            assert_eq!(
                reporter
                    .execute_command(&command, &backend, &lifecycle)
                    .await,
                CommandOutcome::Done
            );
        }
    }
    let signals = backend.get_pending_signals(id).await.unwrap();
    assert_eq!(signals.len(), 2);
    assert!(
        signals
            .iter()
            .all(|signal| signal.payload.to_string().contains("SIGNAL_CANARY"))
    );
    let raw_payloads: Vec<String> =
        sqlx::query_scalar("SELECT payload FROM signal_inbox WHERE instance_id=?1")
            .bind(id.to_string())
            .fetch_all(lifecycle.mobile_storage().pool())
            .await
            .unwrap();
    assert!(
        raw_payloads
            .iter()
            .all(|payload| !payload.contains("SIGNAL_CANARY"))
    );
    assert!(
        lifecycle
            .mobile_storage()
            .transition_legacy_sync_instance(id, InstanceState::Completed)
            .await
            .unwrap()
    );
    let command = cmd(
        "late-result",
        "step_result",
        serde_json::json!({"instance_id":id,"block_id":"s1","request_id":"request","success":true}),
    );
    assert_eq!(
        reporter
            .execute_command(&command, &backend, &lifecycle)
            .await,
        CommandOutcome::Done
    );
    assert_eq!(backend.get_pending_signals(id).await.unwrap().len(), 2);
    assert!(
        !lifecycle
            .mobile_storage()
            .transition_legacy_sync_instance(id, InstanceState::Cancelled)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn legacy_sync_transition_revalidates_owner_after_waiting_for_another_writer() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sync.sqlite");
    let one = Arc::new(
        orch8_storage::sqlite::SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    );
    let two = Arc::new(
        orch8_storage::sqlite::SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    );
    let first = MobileStorage::new(one);
    let second = MobileStorage::new(two);
    let id = insert_raw_instance(&first, "running", chrono::Utc::now()).await;
    let id = InstanceId::from_uuid(uuid::Uuid::parse_str(&id).unwrap());
    let mut writer = first.pool().begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE task_instances SET tenant_id='other-owner' WHERE id=?1")
        .bind(id.to_string())
        .execute(&mut *writer)
        .await
        .unwrap();
    let transition = second.transition_legacy_sync_instance(id, InstanceState::Cancelled);
    tokio::pin!(transition);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut transition)
            .await
            .is_err()
    );
    writer.commit().await.unwrap();
    assert!(
        !transition.await.unwrap(),
        "write used stale legacy authority"
    );
    let state: String = sqlx::query_scalar("SELECT state FROM task_instances WHERE id=?1")
        .bind(id.to_string())
        .fetch_one(first.pool())
        .await
        .unwrap();
    assert_eq!(state, "running");
}

#[tokio::test]
async fn legacy_sync_authority_read_failure_is_retryable_without_host_or_http_entry() {
    let (reporter, storage, lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
    let id = insert_raw_instance(lifecycle.mobile_storage(), "running", chrono::Utc::now()).await;
    sqlx::query("INSERT INTO sync_outbox(entry_type,instance_id,payload) VALUES ('status',?1,?2)")
        .bind(&id)
        .bind(serde_json::json!({"instance_id":id}).to_string())
        .execute(&reporter.pool)
        .await
        .unwrap();
    lifecycle.mobile_storage().pool().close().await;
    let command = cmd(
        "retry-authority",
        "cancel_instance",
        serde_json::json!({"instance_id":id}),
    );
    assert_eq!(
        reporter
            .execute_command(&command, &storage, &lifecycle)
            .await,
        CommandOutcome::Retryable
    );
    assert!(!reporter.sync_once(&storage, &lifecycle).await);
    assert!(reporter.http.get().is_none());
}

#[tokio::test]
async fn legacy_sync_verified_status_survives_native_gc_without_adopting_scoped_rows() {
    let (reporter, storage, lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
    let mobile = lifecycle.mobile_storage();
    let legacy = insert_raw_instance(mobile, "completed", chrono::Utc::now()).await;
    let foreign = scoped_run(mobile, "alice", "one").await;
    let cache = Arc::new(SequenceCache::new(16, Duration::from_secs(30)));
    assert!(reporter.scan_and_queue(&storage, mobile, &cache).await);
    let rows: Vec<(i64, String, String, bool)> =
        sqlx::query_as("SELECT id,instance_id,payload,legacy_verified FROM sync_outbox")
            .fetch_all(&reporter.pool)
            .await
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].3,
        "only the guarded scanner can establish durable legacy authority"
    );
    assert!(
        storage
            .conditional_update_instance_state(
                foreign,
                InstanceState::Scheduled,
                InstanceState::Completed,
                None
            )
            .await
            .unwrap()
    );
    let legacy_id = InstanceId::from_uuid(uuid::Uuid::parse_str(&legacy).unwrap());
    assert_eq!(
        storage
            .purge_terminal_instances(&[legacy_id, foreign])
            .await
            .unwrap(),
        2
    );
    let valid = reporter
        .legacy_outbox_rows(rows, mobile, false)
        .await
        .unwrap();
    assert_eq!(valid.len(), 1, "offline status was lost after native GC");
    let foreign_payload =
        serde_json::json!({"instance_id":foreign,"private":"SCOPED_CANARY"}).to_string();
    let rows = vec![(999, foreign.to_string(), foreign_payload, true)];
    assert!(
        reporter
            .legacy_outbox_rows(rows, mobile, false)
            .await
            .unwrap()
            .is_empty(),
        "legacy verification overrode retained catalog ownership"
    );
    // One verified status cannot lend authority to an unverified approval with
    // the same now-missing native instance id.
    let payload = serde_json::json!({"instance_id":legacy}).to_string();
    let rows = vec![
        (1000, format!("{legacy}:verified"), payload.clone(), true),
        (1001, format!("{legacy}:unverified"), payload, false),
    ];
    let valid = reporter
        .legacy_outbox_rows(rows, mobile, true)
        .await
        .unwrap();
    assert_eq!(
        valid.iter().map(|row| row.0).collect::<Vec<_>>(),
        vec![1000]
    );
}

#[tokio::test]
async fn legacy_sync_old_outbox_upgrade_is_idempotent_and_preserves_unverified_rows() {
    let (reporter, _storage, lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
    let legacy =
        insert_raw_instance(lifecycle.mobile_storage(), "running", chrono::Utc::now()).await;
    sqlx::query("DROP TABLE sync_outbox")
        .execute(&reporter.pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE sync_outbox(id INTEGER PRIMARY KEY AUTOINCREMENT,entry_type TEXT NOT NULL,instance_id TEXT NOT NULL,payload TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT (datetime('now')),UNIQUE(entry_type,instance_id) ON CONFLICT REPLACE)").execute(&reporter.pool).await.unwrap();
    sqlx::query("INSERT INTO sync_outbox(entry_type,instance_id,payload) VALUES ('status',?1,?2)")
        .bind(&legacy)
        .bind(serde_json::json!({"instance_id":legacy}).to_string())
        .execute(&reporter.pool)
        .await
        .unwrap();
    reporter.init_tables().await;
    reporter.init_tables().await;
    let rows: Vec<(i64, String, String, bool)> =
        sqlx::query_as("SELECT id,instance_id,payload,legacy_verified FROM sync_outbox")
            .fetch_all(&reporter.pool)
            .await
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(!rows[0].3, "upgrade must not invent an ownership proof");
    assert_eq!(
        reporter
            .legacy_outbox_rows(rows, lifecycle.mobile_storage(), false)
            .await
            .unwrap()
            .len(),
        1
    );
}
