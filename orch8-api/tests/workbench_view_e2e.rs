//! Extended E2E coverage for the execution workbench view, run comparison,
//! fork preview, and fork sandbox markers.
//!
//! Complements `tests/workbench.rs` with a much deeper matrix: stable event
//! ordering under equal timestamps, kind ranking, limit clamping and the
//! `events_truncated` flag, sentinel/error/retry output classification,
//! context redaction, missing-sequence tolerance, comparison edge cases,
//! fork-preview boundaries and side-effect flags, and the fork's
//! dry-run/sandbox metadata contract.

use chrono::{DateTime, Duration, Utc};
use orch8_api::test_harness::{TestServer, spawn_test_server};
use orch8_storage::{AdminStore, InstanceStore, OutputStore, SequenceStore, WorkerStore};
use orch8_types::audit::AuditLogEntry;
use orch8_types::ids::{BlockId, InstanceId, SequenceId, TenantId};
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;
use orch8_types::step_log::StepLogEntry;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A timestamp far in the past: seeded events sort strictly before anything
/// the API creates at request time (instance-creation audit rows etc.), so
/// ordering assertions on seeded rows are unambiguous.
fn past_ts(offset_secs: i64) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
        + Duration::seconds(offset_secs)
}

async fn publish_as(base: &str, client: &reqwest::Client, tenant: &str, blocks: Value) -> Uuid {
    let id = Uuid::now_v7();
    let resp = client
        .post(format!("{base}/sequences"))
        .header("X-Tenant-Id", tenant)
        .json(&json!({
            "id": id, "tenant_id": tenant, "namespace": "default",
            "name": format!("wbv-seq-{id}"), "version": 3,
            "blocks": blocks,
            "created_at": Utc::now().to_rfc3339(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    id
}

async fn publish(base: &str, client: &reqwest::Client, blocks: Value) -> Uuid {
    publish_as(base, client, "t1", blocks).await
}

/// Two plain noop steps — the default fixture sequence.
fn two_step_blocks() -> Value {
    json!([
        {"type": "step", "id": "a", "handler": "noop", "params": {}},
        {"type": "step", "id": "b", "handler": "noop", "params": {}}
    ])
}

async fn spawn_instance_as(
    base: &str,
    client: &reqwest::Client,
    tenant: &str,
    seq: Uuid,
    data: Value,
) -> Uuid {
    let resp = client
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", tenant)
        .json(&json!({
            "sequence_id": seq, "tenant_id": tenant, "namespace": "default",
            "context": {"data": data},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let v: Value = resp.json().await.unwrap();
    Uuid::parse_str(v["id"].as_str().unwrap()).unwrap()
}

async fn spawn_instance(base: &str, client: &reqwest::Client, seq: Uuid, data: Value) -> Uuid {
    spawn_instance_as(base, client, "t1", seq, data).await
}

/// Insert a `BlockOutput` row with full control over ref/attempt/timestamp.
async fn record_output_full(
    srv: &TestServer,
    instance: Uuid,
    block: &str,
    output: Value,
    output_ref: Option<&str>,
    attempt: u16,
    created_at: DateTime<Utc>,
) -> Uuid {
    let id = Uuid::now_v7();
    srv.storage
        .save_block_output(&BlockOutput {
            id,
            instance_id: InstanceId::from_uuid(instance),
            block_id: BlockId::new(block),
            output,
            output_ref: output_ref.map(ToString::to_string),
            output_size: 10,
            attempt,
            created_at,
        })
        .await
        .unwrap();
    id
}

async fn record_output(srv: &TestServer, instance: Uuid, block: &str, output: Value) {
    record_output_full(srv, instance, block, output, None, 1, Utc::now()).await;
}

async fn seed_audit(
    srv: &TestServer,
    instance: Uuid,
    event_type: &str,
    from: Option<&str>,
    to: Option<&str>,
    at: DateTime<Utc>,
) {
    srv.storage
        .append_audit_log(&AuditLogEntry {
            id: Uuid::now_v7(),
            instance_id: InstanceId::from_uuid(instance),
            tenant_id: TenantId::unchecked("t1"),
            event_type: event_type.to_string(),
            from_state: from.map(ToString::to_string),
            to_state: to.map(ToString::to_string),
            block_id: None,
            details: json!({}),
            created_at: at,
        })
        .await
        .unwrap();
}

async fn seed_log(srv: &TestServer, instance: Uuid, block: &str, message: &str, at: DateTime<Utc>) {
    srv.storage
        .append_step_logs(
            InstanceId::from_uuid(instance),
            &BlockId::new(block),
            &[StepLogEntry {
                ts: at,
                level: "info".to_string(),
                message: message.to_string(),
            }],
        )
        .await
        .unwrap();
}

async fn get_workbench(base: &str, client: &reqwest::Client, inst: Uuid, query: &str) -> Value {
    let resp = client
        .get(format!("{base}/instances/{inst}/workbench{query}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json().await.unwrap()
}

async fn get_compare(base: &str, client: &reqwest::Client, left: Uuid, right: Uuid) -> Value {
    let resp = client
        .get(format!("{base}/instances/{left}/compare/{right}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json().await.unwrap()
}

async fn get_fork_preview(base: &str, client: &reqwest::Client, inst: Uuid, block: &str) -> Value {
    let resp = client
        .get(format!(
            "{base}/instances/{inst}/fork-preview?from_block_id={block}"
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json().await.unwrap()
}

fn event_ids(view: &Value) -> Vec<String> {
    view["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap().to_string())
        .collect()
}

fn str_vec(v: &Value) -> Vec<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect()
}

/// Standard fixture: two-step sequence + one instance, tenant `t1`.
async fn fixture(srv: &TestServer, client: &reqwest::Client, data: Value) -> (Uuid, Uuid) {
    let base = srv.v1_url();
    let seq = publish(&base, client, two_step_blocks()).await;
    let inst = spawn_instance(&base, client, seq, data).await;
    (seq, inst)
}

// ===========================================================================
// Workbench basics: identity fields, share path, sequence metadata
// ===========================================================================

#[tokio::test]
async fn workbench_returns_instance_and_sequence_ids() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (seq, inst) = fixture(&srv, &client, json!({})).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    assert_eq!(view["instance_id"], inst.to_string());
    assert_eq!(view["sequence_id"], seq.to_string());
}

#[tokio::test]
async fn workbench_share_path_shape() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    assert_eq!(view["share_path"], format!("/instances/{inst}/workbench"));
}

#[tokio::test]
async fn workbench_sequence_name_and_version_populated() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (seq, inst) = fixture(&srv, &client, json!({})).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    assert_eq!(view["sequence_name"], format!("wbv-seq-{seq}"));
    assert_eq!(view["sequence_version"], 3);
}

#[tokio::test]
async fn workbench_state_scheduled_for_fresh_instance() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    assert_eq!(view["state"], "scheduled");
}

#[tokio::test]
async fn workbench_after_sequence_delete_is_404_for_the_cascaded_instance() {
    // `delete_sequence` explicitly deletes the sequence's instances
    // (sqlite/sequences.rs deletes matching task_instances rows), so the
    // instance itself vanishes and the workbench answers 404 — there is
    // no orphaned-instance rendering path to tolerate.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (seq, inst) = fixture(&srv, &client, json!({"k": "v"})).await;
    record_output(&srv, inst, "a", json!({"ok": 1})).await;
    srv.storage
        .delete_sequence(SequenceId::from_uuid(seq))
        .await
        .unwrap();

    let resp = client
        .get(format!("{}/instances/{inst}/workbench", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn workbench_unknown_instance_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/instances/{}/workbench", srv.v1_url(), Uuid::now_v7()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn workbench_tenant_isolation_404_for_other_tenant() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    let resp = client
        .get(format!("{}/instances/{inst}/workbench", srv.v1_url()))
        .header("X-Tenant-Id", "someone-else")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn workbench_accessible_without_tenant_header() {
    // The harness runs with require_tenant=false and admin auth: omitting
    // the header must not 404 (OptionalTenant is None -> access allowed).
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    let resp = client
        .get(format!("{}/instances/{inst}/workbench", srv.v1_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// Context redaction
// ===========================================================================

#[tokio::test]
async fn workbench_redacts_password_in_context() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({"password": "hunter2", "user": "ada"})).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    assert_eq!(view["context_data"]["password"], "[REDACTED]");
    assert_eq!(view["context_data"]["user"], "ada");
    assert!(!view.to_string().contains("hunter2"));
}

#[tokio::test]
async fn workbench_redacts_nested_context_secrets() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(
        &srv,
        &client,
        json!({"config": {"api_key": "sk_live_deep", "retries": 3}}),
    )
    .await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    assert_eq!(view["context_data"]["config"]["api_key"], "[REDACTED]");
    assert_eq!(view["context_data"]["config"]["retries"], 3);
    assert!(!view.to_string().contains("sk_live_deep"));
}

#[tokio::test]
async fn workbench_redacts_secret_shaped_value_under_innocent_key() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(
        &srv,
        &client,
        json!({"note": "ghp_leakedinnote0000", "count": 7}),
    )
    .await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    assert_eq!(view["context_data"]["note"], "[REDACTED]");
    assert_eq!(view["context_data"]["count"], 7);
    assert!(!view.to_string().contains("ghp_leakedinnote0000"));
}

#[tokio::test]
async fn workbench_redacts_whole_subtree_under_sensitive_context_key() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(
        &srv,
        &client,
        json!({"credentials": {"user": "u", "pass": "p"}, "plain": 1}),
    )
    .await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    assert_eq!(view["context_data"]["credentials"], "[REDACTED]");
    assert_eq!(view["context_data"]["plain"], 1);
}

#[tokio::test]
async fn workbench_preserves_benign_context_untouched() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let data = json!({"customer": "ada", "amount": 12.5, "tags": ["x", "y"], "meta": {"n": 1}});
    let (_, inst) = fixture(&srv, &client, data.clone()).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    assert_eq!(view["context_data"], data);
}

// ===========================================================================
// Event ordering: stability under equal timestamps
// ===========================================================================

#[tokio::test]
async fn workbench_equal_timestamp_outputs_stable_across_repeated_fetches() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    // Five outputs at the SAME instant — the classic flapping scenario.
    let ts = past_ts(0);
    for block in ["a", "b", "c", "d", "e"] {
        record_output_full(&srv, inst, block, json!({"b": block}), None, 1, ts).await;
    }
    let base = srv.v1_url();
    let first = event_ids(&get_workbench(&base, &client, inst, "").await);
    for _ in 0..3 {
        let again = event_ids(&get_workbench(&base, &client, inst, "").await);
        assert_eq!(first, again, "event order must never flap between fetches");
    }
}

#[tokio::test]
async fn workbench_equal_timestamp_same_kind_ordered_by_id() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    let ts = past_ts(0);
    let mut ids = Vec::new();
    for block in ["a", "b", "c", "d"] {
        ids.push(
            record_output_full(&srv, inst, block, json!({}), None, 1, ts)
                .await
                .to_string(),
        );
    }
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let output_event_ids: Vec<String> = view["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["kind"] == "block_output")
        .map(|e| e["id"].as_str().unwrap().to_string())
        .collect();
    let mut expected = ids.clone();
    expected.sort();
    assert_eq!(output_event_ids, expected, "same-ts same-kind ties break by id");
}

#[tokio::test]
async fn workbench_kind_rank_state_transition_before_output_at_equal_ts() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    let ts = past_ts(0);
    // Insert the output FIRST so ordering cannot come from insertion order.
    record_output_full(&srv, inst, "a", json!({}), None, 1, ts).await;
    seed_audit(&srv, inst, "state_transition", Some("scheduled"), Some("running"), ts).await;

    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let events = view["events"].as_array().unwrap();
    assert_eq!(events[0]["kind"], "state_transition");
    assert_eq!(events[1]["kind"], "block_output");
}

#[tokio::test]
async fn workbench_kind_rank_generic_audit_before_output_at_equal_ts() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    let ts = past_ts(0);
    record_output_full(&srv, inst, "a", json!({}), None, 1, ts).await;
    seed_audit(&srv, inst, "signal_received", None, None, ts).await;

    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let events = view["events"].as_array().unwrap();
    assert_eq!(events[0]["kind"], "audit");
    assert_eq!(events[1]["kind"], "block_output");
}

#[tokio::test]
async fn workbench_kind_rank_output_before_log_at_equal_ts() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    let ts = past_ts(0);
    seed_log(&srv, inst, "a", "handler said hi", ts).await;
    record_output_full(&srv, inst, "a", json!({}), None, 1, ts).await;

    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let events = view["events"].as_array().unwrap();
    assert_eq!(events[0]["kind"], "block_output");
    assert_eq!(events[1]["kind"], "log");
}

#[tokio::test]
async fn workbench_kind_rank_full_chain_at_equal_ts() {
    // All four kinds at one instant: state_transition < audit <
    // block_output < log — the documented ordering contract.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    let ts = past_ts(0);
    seed_log(&srv, inst, "a", "log line", ts).await;
    record_output_full(&srv, inst, "a", json!({}), None, 1, ts).await;
    seed_audit(&srv, inst, "signal_received", None, None, ts).await;
    seed_audit(&srv, inst, "state_transition", Some("scheduled"), Some("running"), ts).await;

    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let kinds: Vec<&str> = view["events"]
        .as_array()
        .unwrap()
        .iter()
        .take(4)
        .map(|e| e["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, vec!["state_transition", "audit", "block_output", "log"]);
}

#[tokio::test]
async fn workbench_events_sorted_by_timestamp_first() {
    // Kind rank only breaks ties: a LATER state transition still sorts
    // after an earlier log/output.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    seed_log(&srv, inst, "a", "early log", past_ts(0)).await;
    seed_audit(&srv, inst, "state_transition", Some("scheduled"), Some("running"), past_ts(10)).await;

    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let events = view["events"].as_array().unwrap();
    assert_eq!(events[0]["kind"], "log");
    assert_eq!(events[1]["kind"], "state_transition");
}

#[tokio::test]
async fn workbench_state_transition_summary_includes_arrow() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    seed_audit(&srv, inst, "state_transition", Some("scheduled"), Some("running"), past_ts(0)).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let summary = view["events"][0]["summary"].as_str().unwrap();
    assert_eq!(summary, "state_transition: scheduled → running");
}

#[tokio::test]
async fn workbench_log_event_summary_has_level_and_message() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    seed_log(&srv, inst, "a", "handler chatter", past_ts(0)).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let ev = &view["events"][0];
    assert_eq!(ev["kind"], "log");
    assert_eq!(ev["block_id"], "a");
    assert_eq!(ev["summary"], "[info] handler chatter");
}

#[tokio::test]
async fn workbench_long_log_message_truncated_in_summary() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    let long = "m".repeat(500);
    seed_log(&srv, inst, "a", &long, past_ts(0)).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let summary = view["events"][0]["summary"].as_str().unwrap();
    assert!(summary.ends_with('…'), "long messages get an ellipsis");
    assert!(summary.len() < 300, "summary is bounded, got {}", summary.len());
}

#[tokio::test]
async fn workbench_output_events_have_block_id_audit_events_do_not() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output_full(&srv, inst, "a", json!({}), None, 1, past_ts(1)).await;
    seed_audit(&srv, inst, "state_transition", Some("scheduled"), Some("running"), past_ts(0)).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let events = view["events"].as_array().unwrap();
    let audit_ev = events.iter().find(|e| e["kind"] == "state_transition").unwrap();
    let output_ev = events.iter().find(|e| e["kind"] == "block_output").unwrap();
    assert!(audit_ev.get("block_id").is_none(), "audit events omit block_id");
    assert_eq!(output_ev["block_id"], "a");
    assert!(
        output_ev["summary"].as_str().unwrap().contains("attempt 1"),
        "output summary carries the attempt"
    );
}

// ===========================================================================
// Limit clamping and events_truncated
// ===========================================================================

#[tokio::test]
async fn workbench_limit_1_returns_single_event_and_truncated_flag() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    for (i, block) in ["a", "b"].iter().enumerate() {
        record_output_full(&srv, inst, block, json!({}), None, 1, past_ts(i as i64)).await;
    }
    let view = get_workbench(&srv.v1_url(), &client, inst, "?limit=1").await;
    assert_eq!(view["events"].as_array().unwrap().len(), 1);
    assert_eq!(view["events_truncated"], true);
}

#[tokio::test]
async fn workbench_limit_0_clamps_to_1() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output_full(&srv, inst, "a", json!({}), None, 1, past_ts(0)).await;
    record_output_full(&srv, inst, "b", json!({}), None, 1, past_ts(1)).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "?limit=0").await;
    assert_eq!(view["events"].as_array().unwrap().len(), 1);
    assert_eq!(view["events_truncated"], true);
}

#[tokio::test]
async fn workbench_not_truncated_when_events_fit() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output_full(&srv, inst, "a", json!({}), None, 1, past_ts(0)).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    assert_eq!(view["events_truncated"], false);
}

#[tokio::test]
async fn workbench_limit_exactly_event_count_not_truncated() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output_full(&srv, inst, "a", json!({}), None, 1, past_ts(0)).await;
    // Discover the actual total first (creation may add audit events).
    let total = get_workbench(&srv.v1_url(), &client, inst, "")
        .await["events"]
        .as_array()
        .unwrap()
        .len();
    let view = get_workbench(&srv.v1_url(), &client, inst, &format!("?limit={total}")).await;
    assert_eq!(view["events"].as_array().unwrap().len(), total);
    assert_eq!(view["events_truncated"], false);
}

#[tokio::test]
async fn workbench_limit_one_below_count_truncates_in_order() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    for i in 0..4 {
        record_output_full(&srv, inst, &format!("blk{i}"), json!({}), None, 1, past_ts(i)).await;
    }
    let full = event_ids(&get_workbench(&srv.v1_url(), &client, inst, "").await);
    let limit = full.len() - 1;
    let view = get_workbench(&srv.v1_url(), &client, inst, &format!("?limit={limit}")).await;
    assert_eq!(view["events_truncated"], true);
    // The truncated view is a strict prefix of the full ordering.
    assert_eq!(event_ids(&view), full[..limit].to_vec());
}

#[tokio::test]
async fn workbench_default_limit_is_500() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    let entries: Vec<StepLogEntry> = (0..600)
        .map(|i| StepLogEntry {
            ts: past_ts(i),
            level: "info".into(),
            message: format!("line {i}"),
        })
        .collect();
    srv.storage
        .append_step_logs(InstanceId::from_uuid(inst), &BlockId::new("a"), &entries)
        .await
        .unwrap();
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    assert_eq!(view["events"].as_array().unwrap().len(), 500);
    assert_eq!(view["events_truncated"], true);
}

#[tokio::test]
async fn workbench_limit_above_2000_clamped_to_2000() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    let entries: Vec<StepLogEntry> = (0..2100)
        .map(|i| StepLogEntry {
            ts: past_ts(i),
            level: "info".into(),
            message: format!("line {i}"),
        })
        .collect();
    srv.storage
        .append_step_logs(InstanceId::from_uuid(inst), &BlockId::new("a"), &entries)
        .await
        .unwrap();
    let view = get_workbench(&srv.v1_url(), &client, inst, "?limit=999999").await;
    assert_eq!(view["events"].as_array().unwrap().len(), 2000);
    assert_eq!(view["events_truncated"], true);
}

// ===========================================================================
// Output summaries: sentinels, status classification, payload hiding
// ===========================================================================

#[tokio::test]
async fn workbench_sentinel_rows_excluded_from_outputs() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output(&srv, inst, "a", json!({"ok": 1})).await;
    record_output(&srv, inst, "_dedupe_marker", json!({"internal": true})).await;
    record_output(&srv, inst, "_claim", json!({})).await;

    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let outputs = view["outputs"].as_array().unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0]["block_id"], "a");
}

#[tokio::test]
async fn workbench_sentinel_rows_excluded_from_events_too() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output(&srv, inst, "_internal", json!({})).await;
    record_output(&srv, inst, "real", json!({})).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let block_events: Vec<&Value> = view["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["kind"] == "block_output")
        .collect();
    assert_eq!(block_events.len(), 1);
    assert_eq!(block_events[0]["block_id"], "real");
}

#[tokio::test]
async fn workbench_error_ref_classified_as_error() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output_full(&srv, inst, "a", json!({"err": "boom"}), Some("__error__"), 2, Utc::now())
        .await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let out = &view["outputs"][0];
    assert_eq!(out["status"], "error");
    assert_eq!(out["attempt"], 2);
    // The event summary reflects the status.
    let ev = view["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["kind"] == "block_output")
        .unwrap();
    assert!(ev["summary"].as_str().unwrap().contains("(error)"));
}

#[tokio::test]
async fn workbench_retry_ref_classified_as_retry() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output_full(&srv, inst, "a", json!({}), Some("__retry__"), 1, Utc::now()).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    assert_eq!(view["outputs"][0]["status"], "retry");
}

#[tokio::test]
async fn workbench_plain_and_externalized_outputs_classified_ok() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output_full(&srv, inst, "a", json!({"v": 1}), None, 1, past_ts(0)).await;
    // An externalized reference that is NOT a sentinel is still "ok".
    record_output_full(&srv, inst, "b", json!(null), Some("ext:blob:key1"), 1, past_ts(1)).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let outputs = view["outputs"].as_array().unwrap();
    assert_eq!(outputs.len(), 2);
    for o in outputs {
        assert_eq!(o["status"], "ok", "{o}");
    }
}

#[tokio::test]
async fn workbench_output_summaries_never_carry_payload() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output(&srv, inst, "a", json!({"very_unique_payload_marker": 42})).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let out = &view["outputs"][0];
    assert!(out.get("output").is_none(), "payload must stay behind the detail endpoint");
    assert!(!view.to_string().contains("very_unique_payload_marker"));
    // Sizes and timestamps ARE exposed.
    assert_eq!(out["output_size"], 10);
    assert!(out.get("recorded_at").is_some());
}

#[tokio::test]
async fn workbench_multiple_attempts_all_listed() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output_full(&srv, inst, "a", json!({}), Some("__retry__"), 1, past_ts(0)).await;
    record_output_full(&srv, inst, "a", json!({"v": 2}), None, 2, past_ts(1)).await;
    let view = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let outputs = view["outputs"].as_array().unwrap();
    assert_eq!(outputs.len(), 2, "every attempt row is summarised");
    let statuses: Vec<&str> = outputs.iter().map(|o| o["status"].as_str().unwrap()).collect();
    assert!(statuses.contains(&"retry"));
    assert!(statuses.contains(&"ok"));
}

// ===========================================================================
// Run comparison
// ===========================================================================

/// Fixture for comparisons: one sequence, two instances.
async fn compare_fixture(srv: &TestServer, client: &reqwest::Client) -> (Uuid, Uuid) {
    let base = srv.v1_url();
    let seq = publish(&base, client, two_step_blocks()).await;
    let left = spawn_instance(&base, client, seq, json!({})).await;
    let right = spawn_instance(&base, client, seq, json!({})).await;
    (left, right)
}

#[tokio::test]
async fn compare_disjoint_paths() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (left, right) = compare_fixture(&srv, &client).await;
    record_output_full(&srv, left, "l1", json!({}), None, 1, past_ts(0)).await;
    record_output_full(&srv, left, "l2", json!({}), None, 1, past_ts(1)).await;
    record_output_full(&srv, right, "r1", json!({}), None, 1, past_ts(0)).await;

    let cmp = get_compare(&srv.v1_url(), &client, left, right).await;
    assert_eq!(str_vec(&cmp["only_left"]), vec!["l1", "l2"]);
    assert_eq!(str_vec(&cmp["only_right"]), vec!["r1"]);
    assert_eq!(cmp["differing_outputs"], json!([]));
    assert_eq!(cmp["matching_blocks"], 0);
}

#[tokio::test]
async fn compare_identical_runs() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (left, right) = compare_fixture(&srv, &client).await;
    for inst in [left, right] {
        record_output_full(&srv, inst, "a", json!({"v": 1}), None, 1, past_ts(0)).await;
        record_output_full(&srv, inst, "b", json!({"v": 2}), None, 1, past_ts(1)).await;
    }
    let cmp = get_compare(&srv.v1_url(), &client, left, right).await;
    assert_eq!(cmp["only_left"], json!([]));
    assert_eq!(cmp["only_right"], json!([]));
    assert_eq!(cmp["differing_outputs"], json!([]));
    assert_eq!(cmp["matching_blocks"], 2);
    assert_eq!(cmp["left"], left.to_string());
    assert_eq!(cmp["right"], right.to_string());
}

#[tokio::test]
async fn compare_differing_outputs_on_shared_blocks() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (left, right) = compare_fixture(&srv, &client).await;
    record_output_full(&srv, left, "same", json!({"v": 1}), None, 1, past_ts(0)).await;
    record_output_full(&srv, right, "same", json!({"v": 1}), None, 1, past_ts(0)).await;
    record_output_full(&srv, left, "diff", json!({"v": "left"}), None, 1, past_ts(1)).await;
    record_output_full(&srv, right, "diff", json!({"v": "right"}), None, 1, past_ts(1)).await;

    let cmp = get_compare(&srv.v1_url(), &client, left, right).await;
    assert_eq!(str_vec(&cmp["differing_outputs"]), vec!["diff"]);
    assert_eq!(cmp["matching_blocks"], 1);
    assert_eq!(cmp["only_left"], json!([]));
    assert_eq!(cmp["only_right"], json!([]));
}

#[tokio::test]
async fn compare_retry_rows_ignored_entirely() {
    // A block whose ONLY row is a __retry__ marker never executed to
    // completion — it must not count as part of the run's path.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (left, right) = compare_fixture(&srv, &client).await;
    record_output_full(&srv, left, "flaky", json!({}), Some("__retry__"), 1, past_ts(0)).await;
    record_output_full(&srv, right, "flaky", json!({"v": 1}), None, 1, past_ts(0)).await;

    let cmp = get_compare(&srv.v1_url(), &client, left, right).await;
    assert_eq!(cmp["only_left"], json!([]));
    assert_eq!(str_vec(&cmp["only_right"]), vec!["flaky"]);
    assert_eq!(cmp["matching_blocks"], 0);
}

#[tokio::test]
async fn compare_retry_then_success_uses_final_output() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (left, right) = compare_fixture(&srv, &client).await;
    record_output_full(&srv, left, "a", json!({}), Some("__retry__"), 1, past_ts(0)).await;
    record_output_full(&srv, left, "a", json!({"v": 2}), None, 2, past_ts(1)).await;
    record_output_full(&srv, right, "a", json!({"v": 2}), None, 1, past_ts(0)).await;

    let cmp = get_compare(&srv.v1_url(), &client, left, right).await;
    assert_eq!(cmp["matching_blocks"], 1);
    assert_eq!(cmp["differing_outputs"], json!([]));
}

#[tokio::test]
async fn compare_last_attempt_wins_for_diffing() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (left, right) = compare_fixture(&srv, &client).await;
    // Left's first attempt differs from right, but the LAST attempt matches.
    record_output_full(&srv, left, "a", json!({"v": 1}), None, 1, past_ts(0)).await;
    record_output_full(&srv, left, "a", json!({"v": 2}), None, 2, past_ts(1)).await;
    record_output_full(&srv, right, "a", json!({"v": 2}), None, 1, past_ts(0)).await;

    let cmp = get_compare(&srv.v1_url(), &client, left, right).await;
    assert_eq!(cmp["differing_outputs"], json!([]));
    assert_eq!(cmp["matching_blocks"], 1);
}

#[tokio::test]
async fn compare_error_rows_count_as_executed() {
    // An __error__ row (unlike __retry__) is a terminal record: the block
    // is part of the executed path.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (left, right) = compare_fixture(&srv, &client).await;
    record_output_full(&srv, left, "boom", json!({"error": "x"}), Some("__error__"), 1, past_ts(0))
        .await;

    let cmp = get_compare(&srv.v1_url(), &client, left, right).await;
    assert_eq!(str_vec(&cmp["only_left"]), vec!["boom"]);
}

#[tokio::test]
async fn compare_sentinel_rows_excluded() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (left, right) = compare_fixture(&srv, &client).await;
    record_output_full(&srv, left, "_claim", json!({}), None, 1, past_ts(0)).await;
    record_output_full(&srv, right, "_other_sentinel", json!({}), None, 1, past_ts(0)).await;

    let cmp = get_compare(&srv.v1_url(), &client, left, right).await;
    assert_eq!(cmp["only_left"], json!([]));
    assert_eq!(cmp["only_right"], json!([]));
    assert_eq!(cmp["matching_blocks"], 0);
}

#[tokio::test]
async fn compare_reports_fresh_states() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (left, right) = compare_fixture(&srv, &client).await;
    let cmp = get_compare(&srv.v1_url(), &client, left, right).await;
    assert_eq!(cmp["left_state"], "scheduled");
    assert_eq!(cmp["right_state"], "scheduled");
}

#[tokio::test]
async fn compare_reports_terminal_states() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (left, right) = compare_fixture(&srv, &client).await;
    srv.storage
        .update_instance_state(InstanceId::from_uuid(left), InstanceState::Completed, None)
        .await
        .unwrap();
    srv.storage
        .update_instance_state(InstanceId::from_uuid(right), InstanceState::Failed, None)
        .await
        .unwrap();
    let cmp = get_compare(&srv.v1_url(), &client, left, right).await;
    assert_eq!(cmp["left_state"], "completed");
    assert_eq!(cmp["right_state"], "failed");
}

#[tokio::test]
async fn compare_instance_with_itself() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (left, _) = compare_fixture(&srv, &client).await;
    record_output_full(&srv, left, "a", json!({"v": 1}), None, 1, past_ts(0)).await;
    record_output_full(&srv, left, "b", json!({"v": 2}), None, 1, past_ts(1)).await;

    let cmp = get_compare(&srv.v1_url(), &client, left, left).await;
    assert_eq!(cmp["left"], cmp["right"]);
    assert_eq!(cmp["only_left"], json!([]));
    assert_eq!(cmp["only_right"], json!([]));
    assert_eq!(cmp["differing_outputs"], json!([]));
    assert_eq!(cmp["matching_blocks"], 2);
}

#[tokio::test]
async fn compare_unknown_left_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (_, right) = compare_fixture(&srv, &client).await;
    let resp = client
        .get(format!("{}/instances/{}/compare/{right}", srv.v1_url(), Uuid::now_v7()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn compare_unknown_right_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (left, _) = compare_fixture(&srv, &client).await;
    let resp = client
        .get(format!("{}/instances/{left}/compare/{}", srv.v1_url(), Uuid::now_v7()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn compare_tenant_isolation_left_side() {
    // Left instance belongs to t2: a t1 caller must get a 404, never a diff.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let seq_t2 = publish_as(&base, &client, "t2", two_step_blocks()).await;
    let left = spawn_instance_as(&base, &client, "t2", seq_t2, json!({})).await;
    let seq_t1 = publish(&base, &client, two_step_blocks()).await;
    let right = spawn_instance(&base, &client, seq_t1, json!({})).await;

    let resp = client
        .get(format!("{base}/instances/{left}/compare/{right}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn compare_tenant_isolation_right_side() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let seq_t1 = publish(&base, &client, two_step_blocks()).await;
    let left = spawn_instance(&base, &client, seq_t1, json!({})).await;
    let seq_t2 = publish_as(&base, &client, "t2", two_step_blocks()).await;
    let right = spawn_instance_as(&base, &client, "t2", seq_t2, json!({})).await;

    let resp = client
        .get(format!("{base}/instances/{left}/compare/{right}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn compare_execution_order_preserved_in_only_lists() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (left, right) = compare_fixture(&srv, &client).await;
    // Insert in z→a name order but ascending time: order must follow TIME.
    record_output_full(&srv, left, "zeta", json!({}), None, 1, past_ts(0)).await;
    record_output_full(&srv, left, "alpha", json!({}), None, 1, past_ts(1)).await;
    record_output_full(&srv, left, "mid", json!({}), None, 1, past_ts(2)).await;
    let _ = right;

    let cmp = get_compare(&srv.v1_url(), &client, left, right).await;
    assert_eq!(str_vec(&cmp["only_left"]), vec!["zeta", "alpha", "mid"]);
}

// ===========================================================================
// Fork preview
// ===========================================================================

/// Three-step fixture with one recorded output on "fetch":
/// fetch(noop, recorded) → charge(llm_call) → log_it(log).
async fn preview_fixture(srv: &TestServer, client: &reqwest::Client) -> Uuid {
    let base = srv.v1_url();
    let seq = publish(
        &base,
        client,
        json!([
            {"type": "step", "id": "fetch", "handler": "noop", "params": {}},
            {"type": "step", "id": "charge", "handler": "llm_call", "params": {}},
            {"type": "step", "id": "log_it", "handler": "log", "params": {}}
        ]),
    )
    .await;
    let inst = spawn_instance(&base, client, seq, json!({})).await;
    record_output(srv, inst, "fetch", json!({"data": 1})).await;
    inst
}

#[tokio::test]
async fn fork_preview_from_first_block_reexecutes_everything() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst = preview_fixture(&srv, &client).await;
    let preview = get_fork_preview(&srv.v1_url(), &client, inst, "fetch").await;
    assert_eq!(preview["copied_blocks"], json!([]));
    assert_eq!(str_vec(&preview["re_executed_blocks"]), vec!["fetch", "charge", "log_it"]);
    assert_eq!(preview["from_block_id"], "fetch");
}

#[tokio::test]
async fn fork_preview_from_middle_block_copies_recorded_prefix() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst = preview_fixture(&srv, &client).await;
    let preview = get_fork_preview(&srv.v1_url(), &client, inst, "charge").await;
    assert_eq!(str_vec(&preview["copied_blocks"]), vec!["fetch"]);
    assert_eq!(str_vec(&preview["re_executed_blocks"]), vec!["charge", "log_it"]);
}

#[tokio::test]
async fn fork_preview_from_last_block() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst = preview_fixture(&srv, &client).await;
    let preview = get_fork_preview(&srv.v1_url(), &client, inst, "log_it").await;
    // "fetch" recorded → copied; "charge" is pre-fork but never recorded →
    // it must land in the re-executed set, not silently vanish.
    assert_eq!(str_vec(&preview["copied_blocks"]), vec!["fetch"]);
    assert_eq!(str_vec(&preview["re_executed_blocks"]), vec!["charge", "log_it"]);
}

#[tokio::test]
async fn fork_preview_all_recorded_from_last_block() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst = preview_fixture(&srv, &client).await;
    record_output(&srv, inst, "charge", json!({"llm": "answer"})).await;
    let preview = get_fork_preview(&srv.v1_url(), &client, inst, "log_it").await;
    assert_eq!(str_vec(&preview["copied_blocks"]), vec!["fetch", "charge"]);
    assert_eq!(str_vec(&preview["re_executed_blocks"]), vec!["log_it"]);
    // Nothing re-executed has side effects (log is benign).
    assert_eq!(preview["side_effect_blocks"], json!([]));
}

#[tokio::test]
async fn fork_preview_recorded_post_fork_block_still_reexecuted() {
    // A recording AFTER the fork point never rescues the block from
    // re-execution — only pre-fork recordings are copied.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst = preview_fixture(&srv, &client).await;
    record_output(&srv, inst, "log_it", json!({"already": "ran"})).await;
    let preview = get_fork_preview(&srv.v1_url(), &client, inst, "charge").await;
    assert!(str_vec(&preview["re_executed_blocks"]).contains(&"log_it".to_string()));
    assert!(!str_vec(&preview["copied_blocks"]).contains(&"log_it".to_string()));
}

#[tokio::test]
async fn fork_preview_unknown_block_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst = preview_fixture(&srv, &client).await;
    let resp = client
        .get(format!(
            "{}/instances/{inst}/fork-preview?from_block_id=ghost",
            srv.v1_url()
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fork_preview_missing_query_param_is_client_error() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst = preview_fixture(&srv, &client).await;
    let resp = client
        .get(format!("{}/instances/{inst}/fork-preview", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn fork_preview_unknown_instance_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "{}/instances/{}/fork-preview?from_block_id=a",
            srv.v1_url(),
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fork_preview_missing_sequence_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let (seq, inst) = fixture(&srv, &client, json!({})).await;
    srv.storage
        .delete_sequence(SequenceId::from_uuid(seq))
        .await
        .unwrap();
    let resp = client
        .get(format!(
            "{}/instances/{inst}/fork-preview?from_block_id=a",
            srv.v1_url()
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fork_preview_tenant_isolation() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst = preview_fixture(&srv, &client).await;
    let resp = client
        .get(format!(
            "{}/instances/{inst}/fork-preview?from_block_id=charge",
            srv.v1_url()
        ))
        .header("X-Tenant-Id", "intruder")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fork_preview_flags_llm_call_as_side_effect() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst = preview_fixture(&srv, &client).await;
    let preview = get_fork_preview(&srv.v1_url(), &client, inst, "fetch").await;
    // "charge" (llm_call) re-executes → flagged. noop/log are not.
    assert_eq!(str_vec(&preview["side_effect_blocks"]), vec!["charge"]);
}

#[tokio::test]
async fn fork_preview_flags_emit_event_as_side_effect() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let seq = publish(
        &base,
        &client,
        json!([
            {"type": "step", "id": "s1", "handler": "noop", "params": {}},
            {"type": "step", "id": "notify", "handler": "emit_event", "params": {}}
        ]),
    )
    .await;
    let inst = spawn_instance(&base, &client, seq, json!({})).await;
    let preview = get_fork_preview(&base, &client, inst, "s1").await;
    assert_eq!(str_vec(&preview["side_effect_blocks"]), vec!["notify"]);
}

#[tokio::test]
async fn fork_preview_flags_external_handler_as_side_effect() {
    // A handler that is not a built-in is conservatively treated as
    // side-effecting: the engine cannot know what it does.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let seq = publish(
        &base,
        &client,
        json!([
            {"type": "step", "id": "s1", "handler": "noop", "params": {}},
            {"type": "step", "id": "custom", "handler": "my_partner_webhook", "params": {}}
        ]),
    )
    .await;
    let inst = spawn_instance(&base, &client, seq, json!({})).await;
    let preview = get_fork_preview(&base, &client, inst, "s1").await;
    assert_eq!(str_vec(&preview["side_effect_blocks"]), vec!["custom"]);
}

#[tokio::test]
async fn fork_preview_benign_builtins_not_flagged() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let seq = publish(
        &base,
        &client,
        json!([
            {"type": "step", "id": "n", "handler": "noop", "params": {}},
            {"type": "step", "id": "l", "handler": "log", "params": {}},
            {"type": "step", "id": "t", "handler": "transform", "params": {}},
            {"type": "step", "id": "z", "handler": "sleep", "params": {}}
        ]),
    )
    .await;
    let inst = spawn_instance(&base, &client, seq, json!({})).await;
    let preview = get_fork_preview(&base, &client, inst, "n").await;
    assert_eq!(str_vec(&preview["re_executed_blocks"]), vec!["n", "l", "t", "z"]);
    assert_eq!(preview["side_effect_blocks"], json!([]));
}

#[tokio::test]
async fn fork_preview_side_effects_only_for_reexecuted_blocks() {
    // A side-effecting block whose output is COPIED must not be flagged —
    // it will not run again.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst = preview_fixture(&srv, &client).await;
    record_output(&srv, inst, "charge", json!({"llm": "done"})).await;
    let preview = get_fork_preview(&srv.v1_url(), &client, inst, "log_it").await;
    assert!(str_vec(&preview["copied_blocks"]).contains(&"charge".to_string()));
    assert_eq!(preview["side_effect_blocks"], json!([]));
}

#[tokio::test]
async fn fork_preview_sandbox_default_true() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst = preview_fixture(&srv, &client).await;
    let preview = get_fork_preview(&srv.v1_url(), &client, inst, "charge").await;
    assert_eq!(preview["sandbox_default"], true);
    assert_eq!(preview["instance_id"], inst.to_string());
}

#[tokio::test]
async fn fork_preview_is_read_only() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst = preview_fixture(&srv, &client).await;
    let before = get_workbench(&srv.v1_url(), &client, inst, "").await;
    let _ = get_fork_preview(&srv.v1_url(), &client, inst, "charge").await;
    let _ = get_fork_preview(&srv.v1_url(), &client, inst, "fetch").await;
    let after = get_workbench(&srv.v1_url(), &client, inst, "").await;
    assert_eq!(
        before["outputs"], after["outputs"],
        "previews must never create or mutate outputs"
    );
    assert_eq!(before["state"], after["state"]);
}

#[tokio::test]
async fn fork_preview_repeated_calls_deterministic() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst = preview_fixture(&srv, &client).await;
    let first = get_fork_preview(&srv.v1_url(), &client, inst, "charge").await;
    let second = get_fork_preview(&srv.v1_url(), &client, inst, "charge").await;
    assert_eq!(first, second);
}

// ===========================================================================
// Fork sandbox marker
// ===========================================================================

async fn get_instance_json(base: &str, client: &reqwest::Client, id: &str) -> Value {
    let resp = client
        .get(format!("{base}/instances/{id}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json().await.unwrap()
}

#[tokio::test]
async fn fork_defaults_to_dry_run_and_sets_sandbox_marker() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output(&srv, inst, "a", json!({"ok": 1})).await;

    let resp = client
        .post(format!("{base}/instances/{inst}/fork"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"from_block_id": "b"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let fork: Value = resp.json().await.unwrap();
    assert_eq!(fork["dry_run"], true, "dry_run defaults to true");

    let fork_inst = get_instance_json(&base, &client, fork["id"].as_str().unwrap()).await;
    assert_eq!(fork_inst["metadata"]["sandbox"], true);
    assert_eq!(fork_inst["metadata"]["forked_from"], inst.to_string());
    assert_eq!(fork_inst["metadata"]["forked_at_block"], "b");
}

#[tokio::test]
async fn fork_explicit_dry_run_false_clears_sandbox_marker() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output(&srv, inst, "a", json!({"ok": 1})).await;

    let resp = client
        .post(format!("{base}/instances/{inst}/fork"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"from_block_id": "b", "dry_run": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let fork: Value = resp.json().await.unwrap();
    assert_eq!(fork["dry_run"], false);

    let fork_inst = get_instance_json(&base, &client, fork["id"].as_str().unwrap()).await;
    assert_eq!(
        fork_inst["metadata"]["sandbox"], false,
        "explicit dry_run=false must clear the sandbox marker: {fork_inst}"
    );
}

#[tokio::test]
async fn fork_explicit_dry_run_true_same_as_default() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output(&srv, inst, "a", json!({"ok": 1})).await;

    let resp = client
        .post(format!("{base}/instances/{inst}/fork"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"from_block_id": "b", "dry_run": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let fork: Value = resp.json().await.unwrap();
    let fork_inst = get_instance_json(&base, &client, fork["id"].as_str().unwrap()).await;
    assert_eq!(fork_inst["metadata"]["sandbox"], true);
}

#[tokio::test]
async fn fork_response_reports_copy_and_rerun_partition() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let (_, inst) = fixture(&srv, &client, json!({})).await;
    record_output(&srv, inst, "a", json!({"ok": 1})).await;

    let resp = client
        .post(format!("{base}/instances/{inst}/fork"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"from_block_id": "b"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let fork: Value = resp.json().await.unwrap();
    assert_eq!(fork["copied_blocks"], 1);
    assert_eq!(fork["rerun_blocks"], json!([]));
    assert_eq!(fork["forked_from"], inst.to_string());
    assert_eq!(fork["state"], "scheduled");
}

#[tokio::test]
async fn forked_instance_workbench_shows_copied_output_and_sandbox_context() {
    // End-to-end: the fork's own workbench view includes the copied output
    // summary (block "a") on a fresh instance id.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let (_, inst) = fixture(&srv, &client, json!({"customer": "ada"})).await;
    record_output(&srv, inst, "a", json!({"ok": 1})).await;

    let resp = client
        .post(format!("{base}/instances/{inst}/fork"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"from_block_id": "b"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let fork: Value = resp.json().await.unwrap();
    let fork_id = Uuid::parse_str(fork["id"].as_str().unwrap()).unwrap();

    let view = get_workbench(&base, &client, fork_id, "").await;
    assert_eq!(view["instance_id"], fork_id.to_string());
    let outputs = view["outputs"].as_array().unwrap();
    assert_eq!(outputs.len(), 1, "copied output visible on the fork: {view}");
    assert_eq!(outputs[0]["block_id"], "a");
    assert_eq!(view["context_data"]["customer"], "ada");
}
