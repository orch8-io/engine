//! Coverage tests for `MobileSyncStore` via `SQLite` in-memory.
//!
//! ```text
//! cargo test -p orch8-storage --test mobile_sync_coverage
//! ```

use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{
    MobileApprovalRequest, MobileCommand, MobileDevice, MobileInstanceStatus, MobileSyncStore,
};

async fn store() -> SqliteStorage {
    SqliteStorage::in_memory().await.unwrap()
}

fn make_device(device_id: &str, tenant_id: &str) -> MobileDevice {
    MobileDevice {
        device_id: device_id.into(),
        tenant_id: tenant_id.into(),
        push_token: Some("tok_abc".into()),
        platform: "ios".into(),
        app_version: Some("1.0.0".into()),
        active: true,
        last_sync_at: None,
        registered_at: String::new(),
    }
}

fn make_status(device_id: &str, instance_id: &str) -> MobileInstanceStatus {
    MobileInstanceStatus {
        device_id: device_id.into(),
        instance_id: instance_id.into(),
        sequence_name: Some("onboarding".into()),
        state: "running".into(),
        current_step: Some("step_1".into()),
        handler: Some("http".into()),
        context_summary: Some(r#"{"user":"alice"}"#.into()),
        steps: Some(r#"[{"block_id":"s1","state":"completed"}]"#.into()),
        updated_at: "2026-01-01T00:00:00Z".into(),
    }
}

fn make_approval(device_id: &str, instance_id: &str, block_id: &str) -> MobileApprovalRequest {
    MobileApprovalRequest {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: device_id.into(),
        tenant_id: "t1".into(),
        instance_id: instance_id.into(),
        block_id: block_id.into(),
        sequence_name: Some("onboarding".into()),
        prompt: Some("Approve?".into()),
        choices: Some(r#"["yes","no"]"#.into()),
        store_as: Some("user_decision".into()),
        timeout_secs: Some(300),
        metadata: None,
        state: "pending".into(),
        resolution: None,
        created_at: String::new(),
        resolved_at: None,
    }
}

fn make_command(device_id: &str, cmd_type: &str) -> MobileCommand {
    MobileCommand {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: device_id.into(),
        command_type: cmd_type.into(),
        payload: r#"{"action":"test"}"#.into(),
        created_at: String::new(),
        acked_at: None,
    }
}

// ===========================================================================
// Device registration & retrieval
// ===========================================================================

#[tokio::test]
async fn register_and_get_device() {
    let s = store().await;
    let device = make_device("dev-1", "t1");
    s.register_mobile_device(&device).await.unwrap();

    let fetched = s.get_mobile_device("dev-1").await.unwrap().unwrap();
    assert_eq!(fetched.device_id, "dev-1");
    assert_eq!(fetched.tenant_id, "t1");
    assert_eq!(fetched.platform, "ios");
    assert!(fetched.active);
    assert!(fetched.last_sync_at.is_none());
}

#[tokio::test]
async fn register_device_upserts_on_conflict() {
    let s = store().await;
    let mut device = make_device("dev-1", "t1");
    device.platform = "ios".into();
    s.register_mobile_device(&device).await.unwrap();

    device.platform = "android".into();
    device.push_token = Some("new_tok".into());
    s.register_mobile_device(&device).await.unwrap();

    let fetched = s.get_mobile_device("dev-1").await.unwrap().unwrap();
    assert_eq!(fetched.platform, "android");
    assert_eq!(fetched.push_token.as_deref(), Some("new_tok"));
}

#[tokio::test]
async fn get_nonexistent_device_returns_none() {
    let s = store().await;
    assert!(s.get_mobile_device("nope").await.unwrap().is_none());
}

#[tokio::test]
async fn update_device_last_sync() {
    let s = store().await;
    s.register_mobile_device(&make_device("dev-1", "t1"))
        .await
        .unwrap();
    s.update_device_last_sync("dev-1").await.unwrap();

    let fetched = s.get_mobile_device("dev-1").await.unwrap().unwrap();
    assert!(fetched.last_sync_at.is_some());
}

// ===========================================================================
// List devices + tenant filtering
// ===========================================================================

#[tokio::test]
async fn list_devices_all() {
    let s = store().await;
    s.register_mobile_device(&make_device("d1", "t1"))
        .await
        .unwrap();
    s.register_mobile_device(&make_device("d2", "t2"))
        .await
        .unwrap();

    let all = s.list_mobile_devices(None, 100).await.unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn list_devices_by_tenant() {
    let s = store().await;
    s.register_mobile_device(&make_device("d1", "t1"))
        .await
        .unwrap();
    s.register_mobile_device(&make_device("d2", "t2"))
        .await
        .unwrap();

    let t1 = s.list_mobile_devices(Some("t1"), 100).await.unwrap();
    assert_eq!(t1.len(), 1);
    assert_eq!(t1[0].device_id, "d1");
}

#[tokio::test]
async fn list_devices_respects_limit() {
    let s = store().await;
    for i in 0..5 {
        s.register_mobile_device(&make_device(&format!("d{i}"), "t1"))
            .await
            .unwrap();
    }
    let limited = s.list_mobile_devices(None, 2).await.unwrap();
    assert_eq!(limited.len(), 2);
}

// ===========================================================================
// Mark stale devices inactive
// ===========================================================================

#[tokio::test]
async fn mark_stale_devices_inactive() {
    let s = store().await;
    s.register_mobile_device(&make_device("d1", "t1"))
        .await
        .unwrap();
    s.update_device_last_sync("d1").await.unwrap();

    // Sleep so last_sync_at is strictly in the past relative to `datetime('now')`
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let count = s.mark_stale_devices_inactive(1).await.unwrap();
    assert_eq!(count, 1);

    let device = s.get_mobile_device("d1").await.unwrap().unwrap();
    assert!(!device.active);
}

#[tokio::test]
async fn mark_stale_skips_recently_synced() {
    let s = store().await;
    s.register_mobile_device(&make_device("d1", "t1"))
        .await
        .unwrap();
    s.update_device_last_sync("d1").await.unwrap();

    // Very large threshold — device should not be marked stale
    let count = s.mark_stale_devices_inactive(999_999).await.unwrap();
    assert_eq!(count, 0);

    let device = s.get_mobile_device("d1").await.unwrap().unwrap();
    assert!(device.active);
}

// ===========================================================================
// Instance status
// ===========================================================================

#[tokio::test]
async fn upsert_and_list_instance_status() {
    let s = store().await;
    s.register_mobile_device(&make_device("d1", "t1"))
        .await
        .unwrap();

    let status = make_status("d1", "inst-1");
    s.upsert_mobile_instance_status(&status).await.unwrap();

    let items = s
        .list_mobile_instance_status(None, Some("d1"), 100)
        .await
        .unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].instance_id, "inst-1");
    assert_eq!(items[0].state, "running");
    assert_eq!(
        items[0].steps.as_deref(),
        Some(r#"[{"block_id":"s1","state":"completed"}]"#)
    );
}

#[tokio::test]
async fn upsert_instance_status_updates_on_conflict() {
    let s = store().await;
    let mut status = make_status("d1", "inst-1");
    s.upsert_mobile_instance_status(&status).await.unwrap();

    status.state = "completed".into();
    status.current_step = None;
    s.upsert_mobile_instance_status(&status).await.unwrap();

    let items = s
        .list_mobile_instance_status(None, Some("d1"), 100)
        .await
        .unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].state, "completed");
    assert!(items[0].current_step.is_none());
}

#[tokio::test]
async fn list_instance_status_by_tenant() {
    let s = store().await;
    s.register_mobile_device(&make_device("d1", "t1"))
        .await
        .unwrap();
    s.register_mobile_device(&make_device("d2", "t2"))
        .await
        .unwrap();

    s.upsert_mobile_instance_status(&make_status("d1", "i1"))
        .await
        .unwrap();
    s.upsert_mobile_instance_status(&make_status("d2", "i2"))
        .await
        .unwrap();

    let t1 = s
        .list_mobile_instance_status(Some("t1"), None, 100)
        .await
        .unwrap();
    assert_eq!(t1.len(), 1);
    assert_eq!(t1[0].device_id, "d1");
}

#[tokio::test]
async fn upsert_instance_status_batch() {
    let s = store().await;
    let statuses = vec![
        make_status("d1", "i1"),
        make_status("d1", "i2"),
        make_status("d1", "i3"),
    ];
    s.upsert_mobile_instance_status_batch(&statuses)
        .await
        .unwrap();

    let items = s
        .list_mobile_instance_status(None, Some("d1"), 100)
        .await
        .unwrap();
    assert_eq!(items.len(), 3);
}

// ===========================================================================
// Approval requests
// ===========================================================================

#[tokio::test]
async fn insert_and_get_approval() {
    let s = store().await;
    let approval = make_approval("d1", "inst-1", "block-1");
    let id = approval.id.clone();

    let inserted = s.insert_mobile_approval(&approval).await.unwrap();
    assert!(inserted);

    let fetched = s.get_mobile_approval(&id).await.unwrap().unwrap();
    assert_eq!(fetched.id, id);
    assert_eq!(fetched.state, "pending");
    assert_eq!(fetched.device_id, "d1");
    assert_eq!(fetched.prompt.as_deref(), Some("Approve?"));
    assert!(fetched.resolution.is_none());
}

#[tokio::test]
async fn insert_approval_deduplicates() {
    let s = store().await;
    let a1 = make_approval("d1", "inst-1", "block-1");
    assert!(s.insert_mobile_approval(&a1).await.unwrap());

    let mut a2 = make_approval("d1", "inst-1", "block-1");
    a2.id = uuid::Uuid::new_v4().to_string();
    let inserted = s.insert_mobile_approval(&a2).await.unwrap();
    assert!(!inserted);
}

#[tokio::test]
async fn resolve_approval() {
    let s = store().await;
    let approval = make_approval("d1", "inst-1", "block-1");
    let id = approval.id.clone();
    s.insert_mobile_approval(&approval).await.unwrap();

    let resolved = s
        .resolve_mobile_approval(&id, r#"{"choice":"yes"}"#)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.state, "resolved");
    assert_eq!(resolved.resolution.as_deref(), Some(r#"{"choice":"yes"}"#));
    assert!(resolved.resolved_at.is_some());
}

#[tokio::test]
async fn resolve_already_resolved_returns_none() {
    let s = store().await;
    let approval = make_approval("d1", "inst-1", "block-1");
    let id = approval.id.clone();
    s.insert_mobile_approval(&approval).await.unwrap();

    s.resolve_mobile_approval(&id, r#""yes""#).await.unwrap();
    let second = s.resolve_mobile_approval(&id, r#""no""#).await.unwrap();
    assert!(second.is_none());
}

#[tokio::test]
async fn resolve_nonexistent_approval_returns_none() {
    let s = store().await;
    let result = s
        .resolve_mobile_approval("nonexistent", r#""yes""#)
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn list_approvals_all() {
    let s = store().await;
    s.insert_mobile_approval(&make_approval("d1", "i1", "b1"))
        .await
        .unwrap();
    s.insert_mobile_approval(&make_approval("d1", "i2", "b2"))
        .await
        .unwrap();

    let all = s.list_mobile_approvals(None, None, 100).await.unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn list_approvals_by_tenant() {
    let s = store().await;
    let mut a1 = make_approval("d1", "i1", "b1");
    a1.tenant_id = "t1".into();
    let mut a2 = make_approval("d1", "i2", "b2");
    a2.tenant_id = "t2".into();

    s.insert_mobile_approval(&a1).await.unwrap();
    s.insert_mobile_approval(&a2).await.unwrap();

    let t1 = s
        .list_mobile_approvals(Some("t1"), None, 100)
        .await
        .unwrap();
    assert_eq!(t1.len(), 1);
}

#[tokio::test]
async fn list_approvals_by_state() {
    let s = store().await;
    let a1 = make_approval("d1", "i1", "b1");
    let id = a1.id.clone();
    s.insert_mobile_approval(&a1).await.unwrap();
    s.insert_mobile_approval(&make_approval("d1", "i2", "b2"))
        .await
        .unwrap();

    s.resolve_mobile_approval(&id, r#""yes""#).await.unwrap();

    let pending = s
        .list_mobile_approvals(None, Some("pending"), 100)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);

    let resolved = s
        .list_mobile_approvals(None, Some("resolved"), 100)
        .await
        .unwrap();
    assert_eq!(resolved.len(), 1);
}

// ===========================================================================
// Approval expiry
// ===========================================================================

#[tokio::test]
async fn expire_approvals_with_zero_timeout() {
    let s = store().await;
    let mut approval = make_approval("d1", "i1", "b1");
    approval.timeout_secs = Some(0);
    s.insert_mobile_approval(&approval).await.unwrap();

    // SQLite uses second-level precision; sleep so expiry time is strictly past
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let expired = s.expire_mobile_approvals().await.unwrap();
    assert_eq!(expired, 1);

    let fetched = s.get_mobile_approval(&approval.id).await.unwrap().unwrap();
    assert_eq!(fetched.state, "expired");
}

#[tokio::test]
async fn expire_skips_approvals_without_timeout() {
    let s = store().await;
    let mut approval = make_approval("d1", "i1", "b1");
    approval.timeout_secs = None;
    s.insert_mobile_approval(&approval).await.unwrap();

    let expired = s.expire_mobile_approvals().await.unwrap();
    assert_eq!(expired, 0);
}

// ===========================================================================
// Commands
// ===========================================================================

#[tokio::test]
async fn create_and_fetch_pending_commands() {
    let s = store().await;
    s.create_mobile_command(&make_command("d1", "complete_step"))
        .await
        .unwrap();
    s.create_mobile_command(&make_command("d1", "start_workflow"))
        .await
        .unwrap();
    s.create_mobile_command(&make_command("d2", "other"))
        .await
        .unwrap();

    let d1_cmds = s.fetch_pending_commands("d1", 100).await.unwrap();
    assert_eq!(d1_cmds.len(), 2);

    let d2_cmds = s.fetch_pending_commands("d2", 100).await.unwrap();
    assert_eq!(d2_cmds.len(), 1);
}

#[tokio::test]
async fn fetch_pending_commands_respects_limit() {
    let s = store().await;
    for _ in 0..5 {
        s.create_mobile_command(&make_command("d1", "test"))
            .await
            .unwrap();
    }
    let limited = s.fetch_pending_commands("d1", 2).await.unwrap();
    assert_eq!(limited.len(), 2);
}

#[tokio::test]
async fn fetch_pending_returns_empty_for_unknown_device() {
    let s = store().await;
    let cmds = s.fetch_pending_commands("no-device", 100).await.unwrap();
    assert!(cmds.is_empty());
}

#[tokio::test]
async fn ack_commands() {
    let s = store().await;
    let cmd1 = make_command("d1", "test");
    let cmd2 = make_command("d1", "test");
    let id1 = cmd1.id.clone();
    let id2 = cmd2.id.clone();
    s.create_mobile_command(&cmd1).await.unwrap();
    s.create_mobile_command(&cmd2).await.unwrap();

    let acked = s
        .ack_mobile_commands("d1", std::slice::from_ref(&id1))
        .await
        .unwrap();
    assert_eq!(acked, 1);

    let pending = s.fetch_pending_commands("d1", 100).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, id2);
}

#[tokio::test]
async fn ack_empty_list_returns_zero() {
    let s = store().await;
    let count = s.ack_mobile_commands("d1", &[]).await.unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn ack_wrong_device_ignores() {
    let s = store().await;
    let cmd = make_command("d1", "test");
    let id = cmd.id.clone();
    s.create_mobile_command(&cmd).await.unwrap();

    let acked = s.ack_mobile_commands("d2", &[id]).await.unwrap();
    assert_eq!(acked, 0);

    let still_pending = s.fetch_pending_commands("d1", 100).await.unwrap();
    assert_eq!(still_pending.len(), 1);
}

// ===========================================================================
// Command cleanup
// ===========================================================================

#[tokio::test]
async fn cleanup_acked_commands() {
    let s = store().await;
    let cmd = make_command("d1", "test");
    let id = cmd.id.clone();
    s.create_mobile_command(&cmd).await.unwrap();
    s.ack_mobile_commands("d1", &[id]).await.unwrap();

    // acked_at was set to datetime('now'); sleep so it's strictly past
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let cleaned = s.cleanup_acked_commands(1).await.unwrap();
    assert_eq!(cleaned, 1);

    let pending = s.fetch_pending_commands("d1", 100).await.unwrap();
    assert!(pending.is_empty());
}

#[tokio::test]
async fn cleanup_acked_skips_unacked() {
    let s = store().await;
    s.create_mobile_command(&make_command("d1", "test"))
        .await
        .unwrap();

    let cleaned = s.cleanup_acked_commands(0).await.unwrap();
    assert_eq!(cleaned, 0);

    let pending = s.fetch_pending_commands("d1", 100).await.unwrap();
    assert_eq!(pending.len(), 1);
}

#[tokio::test]
async fn cleanup_expired_commands() {
    let s = store().await;
    s.create_mobile_command(&make_command("d1", "test"))
        .await
        .unwrap();

    // created_at was set to datetime('now'); sleep so it's strictly past
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let cleaned = s.cleanup_expired_commands(1).await.unwrap();
    assert_eq!(cleaned, 1);
}

#[tokio::test]
async fn cleanup_expired_skips_recent() {
    let s = store().await;
    s.create_mobile_command(&make_command("d1", "test"))
        .await
        .unwrap();

    // Very long TTL — nothing should be cleaned
    let cleaned = s.cleanup_expired_commands(999_999).await.unwrap();
    assert_eq!(cleaned, 0);
}

// ===========================================================================
// End-to-end flow: device → status → approval → resolve → command → ack
// ===========================================================================

#[tokio::test]
async fn full_sync_lifecycle() {
    let s = store().await;

    // 1. Register device
    s.register_mobile_device(&make_device("d1", "t1"))
        .await
        .unwrap();

    // 2. Upsert instance status
    s.upsert_mobile_instance_status(&make_status("d1", "inst-1"))
        .await
        .unwrap();

    // 3. Insert approval
    let approval = make_approval("d1", "inst-1", "wait_for_input");
    let approval_id = approval.id.clone();
    s.insert_mobile_approval(&approval).await.unwrap();

    // 4. Verify pending approval
    let pending = s
        .list_mobile_approvals(Some("t1"), Some("pending"), 100)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);

    // 5. Resolve approval
    let resolved = s
        .resolve_mobile_approval(&approval_id, r#"{"approved":true}"#)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.state, "resolved");

    // 6. Create command for device
    let cmd = MobileCommand {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: "d1".into(),
        command_type: "complete_step".into(),
        payload: serde_json::json!({
            "instance_id": "inst-1",
            "block_id": "wait_for_input",
            "output": {"approved": true}
        })
        .to_string(),
        created_at: String::new(),
        acked_at: None,
    };
    let cmd_id = cmd.id.clone();
    s.create_mobile_command(&cmd).await.unwrap();

    // 7. Fetch and ack
    let commands = s.fetch_pending_commands("d1", 50).await.unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].command_type, "complete_step");

    s.ack_mobile_commands("d1", &[cmd_id]).await.unwrap();

    let after_ack = s.fetch_pending_commands("d1", 50).await.unwrap();
    assert!(after_ack.is_empty());

    // 8. Update last sync
    s.update_device_last_sync("d1").await.unwrap();
    let device = s.get_mobile_device("d1").await.unwrap().unwrap();
    assert!(device.last_sync_at.is_some());
}

#[tokio::test]
async fn paged_approvals_keep_legacy_reads_and_stable_tied_ordering() {
    let s = store().await;
    for index in 0..5 {
        let mut approval = make_approval("device", &format!("run-{index}"), "gate");
        approval.id = format!("approval-{index}");
        approval.created_at = "2026-09-06T00:00:00Z".into();
        s.insert_mobile_approval(&approval).await.unwrap();
    }
    sqlx::query("UPDATE mobile_approval_requests SET created_at = '2026-09-06T00:00:00Z'")
        .execute(s.pool())
        .await
        .unwrap();
    let legacy = s
        .list_mobile_approvals(Some("t1"), Some("pending"), 2)
        .await
        .unwrap();
    let first = s
        .list_mobile_approvals_page(Some("t1"), Some("pending"), 2, 0)
        .await
        .unwrap();
    assert_eq!(
        legacy.iter().map(|item| &item.id).collect::<Vec<_>>(),
        first.iter().map(|item| &item.id).collect::<Vec<_>>()
    );
    let mut ids = Vec::new();
    for offset in [0, 2, 4] {
        ids.extend(
            s.list_mobile_approvals_page(Some("t1"), Some("pending"), 2, offset)
                .await
                .unwrap()
                .into_iter()
                .map(|item| item.id),
        );
    }
    assert_eq!(
        ids,
        (0..5)
            .rev()
            .map(|index| format!("approval-{index}"))
            .collect::<Vec<_>>()
    );
}
