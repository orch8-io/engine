//! Sync reporter coverage: wire payload shapes, steps payload building, and
//! wall-clock scheduling edges (prune cadence, push-generation marker).
//!
//! Count contract: 18 independently named unit tests.

use super::*;
use orch8_types::clock::{Clock, ManualClock};

fn projection(instance_id: InstanceId, block_id: &str, state: &str) -> SyncExecutionStepProjection {
    SyncExecutionStepProjection {
        instance_id,
        block_id: BlockId::new(block_id),
        block_type: "step".to_string(),
        state: state.to_string(),
        started_at: None,
        completed_at: None,
    }
}

fn sequence_with_blocks(blocks: serde_json::Value) -> SequenceDefinition {
    serde_json::from_value(serde_json::json!({
        "id": SequenceId::new(),
        "tenant_id": "mobile",
        "namespace": "default",
        "name": "seq",
        "version": 1,
        "deprecated": false,
        "blocks": blocks,
        "created_at": chrono::Utc::now(),
    }))
    .unwrap()
}

fn step_block(id: &str, handler: &str) -> serde_json::Value {
    serde_json::json!({"type": "step", "id": id, "handler": handler, "params": {}})
}

#[test]
fn coverage_reporter_001_default_interval_is_thirty_seconds() {
    assert_eq!(default_interval(), 30);
}

#[test]
fn coverage_reporter_002_interval_bounds_are_five_seconds_and_one_hour() {
    assert_eq!(MIN_SYNC_INTERVAL_SECS, 5);
    assert_eq!(MAX_SYNC_INTERVAL_SECS, 3600);
}

#[test]
fn coverage_reporter_003_sync_response_defaults_missing_fields() {
    let response: SyncResponse = serde_json::from_str("{}").unwrap();
    assert!(response.commands.is_empty());
    assert_eq!(response.sync_interval_secs, 30);
}

#[test]
fn coverage_reporter_004_sync_response_renames_command_type_field() {
    let response: SyncResponse = serde_json::from_str(
        r#"{"sync_interval_secs": 60, "commands": [{"id": "c1", "type": "cancel_instance", "payload": {}}]}"#,
    )
    .unwrap();
    assert_eq!(response.sync_interval_secs, 60);
    assert_eq!(response.commands.len(), 1);
    assert_eq!(response.commands[0].id, "c1");
    assert_eq!(response.commands[0].command_type, "cancel_instance");
}

#[test]
fn coverage_reporter_005_borrowed_payloads_empty_input_yields_empty() {
    assert!(borrow_valid_payloads(&[]).is_empty());
}

#[test]
fn coverage_reporter_006_borrowed_payloads_drop_invalid_keep_order() {
    let rows = vec![
        (1, r#"{"a":1}"#.to_string()),
        (2, "not-json".to_string()),
        (3, "   ".to_string()),
        (4, r#"[1,2]"#.to_string()),
    ];

    let payloads = borrow_valid_payloads(&rows);

    assert_eq!(payloads.len(), 2);
    assert_eq!(payloads[0].get(), r#"{"a":1}"#);
    assert_eq!(payloads[1].get(), r#"[1,2]"#);
}

#[test]
fn coverage_reporter_007_status_payload_serializes_nullable_fields() {
    let payload: serde_json::Value = serde_json::from_str(&status_payload(
        "i1", None, "Running", None, None, None, "ts",
    ))
    .unwrap();

    assert_eq!(payload["instance_id"], "i1");
    assert!(payload["sequence_name"].is_null());
    assert_eq!(payload["state"], "Running");
    assert!(payload["current_step"].is_null());
    assert!(payload["handler"].is_null());
    assert!(payload["steps"].is_null());
    assert_eq!(payload["timestamp"], "ts");
}

#[test]
fn coverage_reporter_008_status_payload_includes_present_values() {
    let payload: serde_json::Value = serde_json::from_str(&status_payload(
        "i1",
        Some("onboarding"),
        "Waiting",
        Some("review"),
        Some("human_review"),
        Some(serde_json::json!([{"block_id": "review"}])),
        "ts",
    ))
    .unwrap();

    assert_eq!(payload["sequence_name"], "onboarding");
    assert_eq!(payload["current_step"], "review");
    assert_eq!(payload["handler"], "human_review");
    assert_eq!(payload["steps"][0]["block_id"], "review");
}

#[test]
fn coverage_reporter_009_approval_payload_parses_valid_choices() {
    let payload: serde_json::Value = serde_json::from_str(&approval_payload(
        "i1",
        "gate",
        Some("seq"),
        Some("Approve?"),
        Some(r#"[{"label":"Yes","value":"yes"}]"#),
        Some("decision"),
        Some(300),
    ))
    .unwrap();

    assert_eq!(payload["block_id"], "gate");
    assert_eq!(payload["prompt"], "Approve?");
    assert_eq!(payload["choices"][0]["value"], "yes");
    assert_eq!(payload["store_as"], "decision");
    assert_eq!(payload["timeout_seconds"], 300);
}

#[test]
fn coverage_reporter_010_approval_payload_nulls_invalid_choices() {
    let payload: serde_json::Value = serde_json::from_str(&approval_payload(
        "i1",
        "gate",
        None,
        None,
        Some("not-json"),
        None,
        None,
    ))
    .unwrap();

    assert!(payload["choices"].is_null());
    assert!(payload["prompt"].is_null());
    assert!(payload["timeout_seconds"].is_null());
}

#[test]
fn coverage_reporter_011_steps_payload_empty_tree_is_none() {
    assert!(build_steps_payload(&[], None).is_none());
}

#[test]
fn coverage_reporter_012_steps_payload_tree_only_uses_projection_state() {
    let id = InstanceId::new();
    let mut node = projection(id, "review", "waiting");
    node.started_at = Some("2026-07-25T12:00:00Z".to_string());

    let payload = build_steps_payload(&[node], None).unwrap();

    assert_eq!(payload[0]["block_id"], "review");
    assert_eq!(payload[0]["state"], "waiting");
    assert!(payload[0]["handler"].is_null());
    assert_eq!(payload[0]["started_at"], "2026-07-25T12:00:00Z");
    assert!(payload[0]["completed_at"].is_null());
}

#[test]
fn coverage_reporter_013_steps_payload_defaults_missing_nodes_to_pending() {
    let id = InstanceId::new();
    let sequence = sequence_with_blocks(serde_json::json!([
        step_block("done", "emit"),
        step_block("review", "human_review"),
    ]));
    let tree = [projection(id, "done", "completed")];

    let payload = build_steps_payload(&tree, Some(&sequence)).unwrap();

    assert_eq!(payload[0]["block_id"], "done");
    assert_eq!(payload[0]["state"], "completed");
    assert_eq!(payload[0]["handler"], "emit");
    assert_eq!(payload[1]["block_id"], "review");
    assert_eq!(payload[1]["state"], "pending");
    assert!(payload[1]["started_at"].is_null());
    assert_eq!(payload[1]["handler"], "human_review");
}

#[test]
fn coverage_reporter_014_group_execution_steps_buckets_by_instance() {
    let first = InstanceId::new();
    let second = InstanceId::new();
    let steps = vec![
        projection(first, "a", "completed"),
        projection(second, "b", "running"),
        projection(first, "c", "pending"),
    ];

    let grouped = group_execution_steps(steps);

    assert_eq!(grouped.len(), 2);
    assert_eq!(grouped[&first].len(), 2);
    assert_eq!(grouped[&first][0].block_id.as_str(), "a");
    assert_eq!(grouped[&first][1].block_id.as_str(), "c");
    assert_eq!(grouped[&second].len(), 1);
}

#[test]
fn coverage_reporter_015_find_handler_locates_step_handler() {
    let sequence = sequence_with_blocks(serde_json::json!([
        step_block("first", "emit"),
        step_block("second", "human_review"),
    ]));

    assert_eq!(
        find_handler(&sequence.blocks, &BlockId::new("second")),
        Some("human_review".to_string())
    );
    assert_eq!(
        find_handler(&sequence.blocks, &BlockId::new("missing")),
        None
    );
}

#[test]
fn coverage_reporter_016_find_wait_info_extracts_human_input_metadata() {
    let sequence = sequence_with_blocks(serde_json::json!([
        {
            "type": "step",
            "id": "gate",
            "handler": "approval",
            "params": {},
            "wait_for_input": {
                "prompt": "Approve?",
                "timeout": 300000,
                "store_as": "decision",
                "choices": [{"label": "Yes", "value": "yes"}]
            }
        },
        step_block("plain", "emit"),
    ]));

    let (prompt, choices, store_as, timeout) =
        find_wait_info(&sequence.blocks, &BlockId::new("gate")).unwrap();
    assert_eq!(prompt.as_deref(), Some("Approve?"));
    let choices: serde_json::Value = serde_json::from_str(&choices.unwrap()).unwrap();
    assert_eq!(choices[0]["value"], "yes");
    assert_eq!(store_as.as_deref(), Some("decision"));
    assert_eq!(timeout, Some(300));

    assert!(find_wait_info(&sequence.blocks, &BlockId::new("plain")).is_none());
    assert!(find_wait_info(&sequence.blocks, &BlockId::new("missing")).is_none());
}

#[tokio::test]
async fn coverage_reporter_017_command_prune_runs_once_then_waits_a_day() {
    let start = chrono::Utc::now();
    let manual = Arc::new(ManualClock::new(start));
    let clock = SharedClock::from_arc(Arc::clone(&manual) as Arc<dyn Clock>);
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let reporter = SyncReporter::new_with_clock(
        pool,
        "http://127.0.0.1:1/sync".to_string(),
        "device-1".to_string(),
        "key".to_string(),
        clock,
    );

    assert!(reporter.command_prune_due(start), "first run must prune");
    reporter.mark_command_prune_completed(start);

    let within_window = start + chrono::Duration::hours(23);
    assert!(!reporter.command_prune_due(within_window));

    let next_day = start + chrono::Duration::days(1);
    assert!(reporter.command_prune_due(next_day));
}

#[tokio::test]
async fn coverage_reporter_018_completed_push_marker_never_regresses() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let reporter = SyncReporter::new(
        pool,
        "http://127.0.0.1:1/sync".to_string(),
        "device-1".to_string(),
        "key".to_string(),
    );

    reporter.on_push_received();
    reporter.on_push_received();
    assert_eq!(reporter.push_generation.load(Ordering::Acquire), 2);

    reporter.mark_pushes_completed(1);
    assert!(reporter.has_forced_sync(), "one push still outstanding");

    reporter.mark_pushes_completed(2);
    assert!(!reporter.has_forced_sync());

    reporter.mark_pushes_completed(1);
    assert!(
        !reporter.has_forced_sync(),
        "stale completion must not move the marker backward"
    );
    assert_eq!(
        reporter.completed_push_generation.load(Ordering::Acquire),
        2
    );
}
