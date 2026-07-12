//! Extensive e2e tests for DLQ root-cause fingerprinting and guided
//! recovery, exercised over HTTP against a fully-wired test server.
//!
//! Complements `tests/dlq_groups.rs` with deeper coverage: grouping
//! matrices across cause mixes, ordering, group field contents (incl.
//! secret redaction), `__retry__`/no-output evidence paths, filters and
//! tenant scoping, the full bulk-gating matrix, limits, and idempotency.

use std::collections::BTreeSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use orch8_api::test_harness::{TestServer, spawn_test_server};
use orch8_storage::{InstanceStore, OutputStore};
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

struct Ctx {
    srv: TestServer,
    client: reqwest::Client,
    base: String,
}

async fn ctx() -> Ctx {
    let srv = spawn_test_server().await;
    let base = srv.v1_url();
    Ctx {
        srv,
        client: reqwest::Client::new(),
        base,
    }
}

/// Small delay so consecutive plants get strictly increasing timestamps.
async fn tick() {
    tokio::time::sleep(Duration::from_millis(15)).await;
}

/// Create a sequence with the given `(block_id, handler)` steps.
async fn create_sequence(ctx: &Ctx, tenant: &str, blocks: &[(&str, &str)]) -> Uuid {
    let id = Uuid::now_v7();
    let steps: Vec<Value> = blocks
        .iter()
        .map(|(block, handler)| json!({"type": "step", "id": block, "handler": handler, "params": {}}))
        .collect();
    let body = json!({
        "id": id,
        "tenant_id": tenant,
        "namespace": "default",
        "name": format!("dlq-e2e-{id}"),
        "version": 1,
        "blocks": steps,
        "created_at": Utc::now().to_rfc3339(),
    });
    let resp = ctx
        .client
        .post(format!("{}/sequences", ctx.base))
        .header("X-Tenant-Id", tenant)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    id
}

async fn seq_one_block(ctx: &Ctx) -> Uuid {
    create_sequence(ctx, "t1", &[("charge", "charge_card")]).await
}

/// Create an instance over HTTP; leave it in its initial state.
async fn create_instance(ctx: &Ctx, tenant: &str, seq: Uuid) -> Uuid {
    let resp = ctx
        .client
        .post(format!("{}/instances", ctx.base))
        .header("X-Tenant-Id", tenant)
        .json(&json!({
            "sequence_id": seq,
            "tenant_id": tenant,
            "namespace": "default",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let inst: Value = resp.json().await.unwrap();
    Uuid::parse_str(inst["id"].as_str().unwrap()).unwrap()
}

/// Save an output row with the given sentinel ref and payload.
async fn save_output(
    ctx: &Ctx,
    instance: Uuid,
    block: &str,
    output_ref: &str,
    output: Value,
    created_at: DateTime<Utc>,
) {
    ctx.srv
        .storage
        .save_block_output(&BlockOutput {
            id: Uuid::now_v7(),
            instance_id: InstanceId::from_uuid(instance),
            block_id: BlockId::new(block),
            output,
            output_ref: Some(output_ref.into()),
            output_size: 0,
            attempt: 1,
            created_at,
        })
        .await
        .unwrap();
}

/// Force an instance into `Failed` (Running → Failed), as the scheduler
/// does on permanent failure.
async fn fail_instance(ctx: &Ctx, instance: Uuid) {
    let id = InstanceId::from_uuid(instance);
    ctx.srv
        .storage
        .update_instance_state(id, InstanceState::Running, None)
        .await
        .unwrap();
    ctx.srv
        .storage
        .update_instance_state(id, InstanceState::Failed, None)
        .await
        .unwrap();
}

/// Full plant: instance + `__error__` output on `block` + Failed state.
async fn plant(ctx: &Ctx, tenant: &str, seq: Uuid, block: &str, message: &str) -> Uuid {
    let id = create_instance(ctx, tenant, seq).await;
    save_output(
        ctx,
        id,
        block,
        "__error__",
        json!({"__error__": true, "retryable": false, "message": message}),
        Utc::now(),
    )
    .await;
    fail_instance(ctx, id).await;
    id
}

async fn plant_default(ctx: &Ctx, seq: Uuid, message: &str) -> Uuid {
    plant(ctx, "t1", seq, "charge", message).await
}

async fn groups_for(ctx: &Ctx, tenant: &str) -> Vec<Value> {
    let resp = ctx
        .client
        .get(format!("{}/instances/dlq/groups", ctx.base))
        .header("X-Tenant-Id", tenant)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json().await.unwrap()
}

async fn groups_t1(ctx: &Ctx) -> Vec<Value> {
    groups_for(ctx, "t1").await
}

async fn fingerprint_of_only_group(ctx: &Ctx) -> String {
    let groups = groups_t1(ctx).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    groups[0]["fingerprint"].as_str().unwrap().to_string()
}

async fn retry(ctx: &Ctx, tenant: &str, fp: &str, body: &Value) -> reqwest::Response {
    ctx.client
        .post(format!("{}/instances/dlq/groups/{fp}/retry", ctx.base))
        .header("X-Tenant-Id", tenant)
        .json(body)
        .send()
        .await
        .unwrap()
}

async fn instance_state(ctx: &Ctx, id: Uuid) -> InstanceState {
    ctx.srv
        .storage
        .get_instance(InstanceId::from_uuid(id))
        .await
        .unwrap()
        .unwrap()
        .state
}

async fn complete_instance(ctx: &Ctx, id: Uuid) {
    let iid = InstanceId::from_uuid(id);
    ctx.srv
        .storage
        .update_instance_state(iid, InstanceState::Running, None)
        .await
        .unwrap();
    ctx.srv
        .storage
        .update_instance_state(iid, InstanceState::Completed, None)
        .await
        .unwrap();
}

fn retried_ids(body: &Value) -> Vec<Uuid> {
    body["retried"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| Uuid::parse_str(v.as_str().unwrap()).unwrap())
        .collect()
}

// =====================================================================
// Grouping correctness
// =====================================================================

#[tokio::test]
async fn volatile_uuids_in_unknown_messages_group_together() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..3 {
        let msg = format!("widget frobnication failed for {}", Uuid::now_v7());
        plant_default(&c, seq, &msg).await;
    }
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["count"], 3);
    assert_eq!(groups[0]["error_code"], "unknown");
}

#[tokio::test]
async fn http_status_groups_ignore_request_ids() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503 from upstream (req-11111111)").await;
    plant_default(&c, seq, "http 503 from upstream (req-99999999)").await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["count"], 2);
    assert_eq!(groups[0]["error_code"], "HTTP_STATUS");
}

#[tokio::test]
async fn different_http_statuses_split_groups() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "upstream http 503 unavailable").await;
    plant_default(&c, seq, "upstream http 404 missing").await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 2, "{groups:#?}");
    let classes: BTreeSet<&str> = groups
        .iter()
        .map(|g| g["error_class"].as_str().unwrap())
        .collect();
    assert!(classes.contains("external_dependency"));
    assert!(classes.contains("application"));
}

#[tokio::test]
async fn different_error_codes_split_groups() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "request timed out after 30s").await;
    plant_default(&c, seq, "credential 'billing' expired").await;
    plant_default(&c, seq, "rate limit exhausted").await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 3, "{groups:#?}");
    let codes: BTreeSet<&str> = groups
        .iter()
        .map(|g| g["error_code"].as_str().unwrap())
        .collect();
    assert_eq!(
        codes,
        BTreeSet::from(["TIMEOUT", "CREDENTIAL", "RATE_LIMITED"])
    );
}

#[tokio::test]
async fn different_blocks_split_groups() {
    let c = ctx().await;
    let seq = create_sequence(
        &c,
        "t1",
        &[("charge", "charge_card"), ("refund", "refund_card")],
    )
    .await;
    plant(&c, "t1", seq, "charge", "http 503").await;
    plant(&c, "t1", seq, "refund", "http 503").await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 2, "{groups:#?}");
    let blocks: BTreeSet<&str> = groups
        .iter()
        .map(|g| g["blocks"][0].as_str().unwrap())
        .collect();
    assert_eq!(blocks, BTreeSet::from(["charge", "refund"]));
}

#[tokio::test]
async fn handlers_reported_per_failing_block() {
    let c = ctx().await;
    let seq = create_sequence(
        &c,
        "t1",
        &[("charge", "charge_card"), ("refund", "refund_card")],
    )
    .await;
    plant(&c, "t1", seq, "charge", "http 503").await;
    plant(&c, "t1", seq, "refund", "http 503").await;
    let groups = groups_t1(&c).await;
    let handlers: BTreeSet<&str> = groups
        .iter()
        .map(|g| g["handlers"][0].as_str().unwrap())
        .collect();
    assert_eq!(handlers, BTreeSet::from(["charge_card", "refund_card"]));
}

#[tokio::test]
async fn different_sequences_split_groups() {
    let c = ctx().await;
    let seq_a = seq_one_block(&c).await;
    let seq_b = seq_one_block(&c).await;
    plant_default(&c, seq_a, "http 503").await;
    plant_default(&c, seq_b, "http 503").await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 2, "{groups:#?}");
    let seqs: BTreeSet<&str> = groups
        .iter()
        .map(|g| g["sequences"][0].as_str().unwrap())
        .collect();
    assert_eq!(
        seqs,
        BTreeSet::from([seq_a.to_string().as_str(), seq_b.to_string().as_str()])
    );
}

#[tokio::test]
async fn distinct_unknown_messages_split_groups() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "the widget melted").await;
    plant_default(&c, seq, "the gasket blew").await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 2, "{groups:#?}");
    assert!(groups.iter().all(|g| g["error_code"] == "unknown"));
}

#[tokio::test]
async fn numeric_noise_in_unknown_messages_does_not_split() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "returned 12345 rows then failed").await;
    plant_default(&c, seq, "returned 99999 rows then failed").await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["count"], 2);
}

#[tokio::test]
async fn mixed_causes_produce_expected_counts() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..3 {
        plant_default(&c, seq, "http 503 from gateway").await;
    }
    for _ in 0..2 {
        plant_default(&c, seq, "request timed out").await;
    }
    plant_default(&c, seq, "budget exhausted").await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 3, "{groups:#?}");
    let counts: Vec<u64> = groups.iter().map(|g| g["count"].as_u64().unwrap()).collect();
    assert_eq!(counts, vec![3, 2, 1]);
}

// =====================================================================
// Ordering
// =====================================================================

#[tokio::test]
async fn groups_ordered_by_count_desc() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "solo cause here").await;
    for _ in 0..3 {
        plant_default(&c, seq, "http 503").await;
    }
    for _ in 0..2 {
        plant_default(&c, seq, "rate limit hit").await;
    }
    let groups = groups_t1(&c).await;
    assert_eq!(groups[0]["count"], 3);
    assert_eq!(groups[0]["error_code"], "HTTP_STATUS");
    assert_eq!(groups[1]["count"], 2);
    assert_eq!(groups[1]["error_code"], "RATE_LIMITED");
    assert_eq!(groups[2]["count"], 1);
}

#[tokio::test]
async fn equal_counts_ordered_by_recency() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "older cause alpha").await;
    tick().await;
    plant_default(&c, seq, "older cause alpha").await;
    tick().await;
    plant_default(&c, seq, "newer cause omega").await;
    tick().await;
    plant_default(&c, seq, "newer cause omega").await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 2, "{groups:#?}");
    assert_eq!(groups[0]["count"], 2);
    assert_eq!(groups[1]["count"], 2);
    assert!(
        groups[0]["sample_message"]
            .as_str()
            .unwrap()
            .contains("omega"),
        "{groups:#?}"
    );
}

// =====================================================================
// Group field contents
// =====================================================================

#[tokio::test]
async fn group_fingerprint_is_16_lowercase_hex() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;
    assert_eq!(fp.len(), 16);
    assert!(
        fp.chars()
            .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase()),
        "{fp}"
    );
}

#[tokio::test]
async fn components_explain_the_grouping() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    let groups = groups_t1(&c).await;
    let components: Vec<&str> = groups[0]["components"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(components.contains(&format!("seq:{seq}").as_str()), "{components:?}");
    assert!(components.contains(&"v:1"), "{components:?}");
    assert!(components.contains(&"block:charge"), "{components:?}");
    assert!(components.contains(&"handler:charge_card"), "{components:?}");
    assert!(components.contains(&"class:external_dependency"), "{components:?}");
    assert!(components.contains(&"code:HTTP_STATUS"), "{components:?}");
    assert!(components.contains(&"status:503"), "{components:?}");
}

#[tokio::test]
async fn error_class_and_code_populated() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "request timed out").await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups[0]["error_code"], "TIMEOUT");
    assert_eq!(groups[0]["error_class"], "timeout");
}

#[tokio::test]
async fn sample_message_redacts_stripe_secret() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(
        &c,
        seq,
        "payment declined sk_live_4242424242424242abcdef by gateway",
    )
    .await;
    let groups = groups_t1(&c).await;
    let msg = groups[0]["sample_message"].as_str().unwrap();
    assert!(!msg.contains("sk_live_"), "{msg}");
    assert!(msg.contains("[REDACTED]"), "{msg}");
    assert!(msg.contains("payment declined"), "{msg}");
}

#[tokio::test]
async fn sample_message_redacts_github_token() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "push rejected for ghp_16C7e42F292c6912E7710c8383 upstream").await;
    let groups = groups_t1(&c).await;
    let msg = groups[0]["sample_message"].as_str().unwrap();
    assert!(!msg.contains("ghp_"), "{msg}");
    assert!(msg.contains("[REDACTED]"), "{msg}");
}

#[tokio::test]
async fn occurrences_span_first_to_last_failure() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    tick().await;
    plant_default(&c, seq, "http 503").await;
    let groups = groups_t1(&c).await;
    let first: DateTime<Utc> = groups[0]["first_occurrence"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let last: DateTime<Utc> = groups[0]["last_occurrence"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(first < last, "first={first} last={last}");
}

#[tokio::test]
async fn blocks_handlers_sequences_are_deduped() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..5 {
        plant_default(&c, seq, "http 503").await;
    }
    let groups = groups_t1(&c).await;
    // All five members share block/handler/sequence — the bounded lists
    // hold one entry each, not five.
    assert_eq!(groups[0]["blocks"], json!(["charge"]));
    assert_eq!(groups[0]["handlers"], json!(["charge_card"]));
    assert_eq!(groups[0]["sequences"], json!([seq.to_string()]));
}

#[tokio::test]
async fn sample_instance_ids_capped_at_ten_but_count_full() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let mut planted = BTreeSet::new();
    for _ in 0..12 {
        planted.insert(plant_default(&c, seq, "http 503").await);
    }
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["count"], 12);
    let samples = groups[0]["sample_instance_ids"].as_array().unwrap();
    assert_eq!(samples.len(), 10);
    for s in samples {
        let id = Uuid::parse_str(s.as_str().unwrap()).unwrap();
        assert!(planted.contains(&id), "sample {id} not planted");
    }
}

// =====================================================================
// Evidence paths: __retry__ markers, missing outputs
// =====================================================================

#[tokio::test]
async fn retry_marker_only_failures_group_by_error_field() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..2 {
        let id = create_instance(&c, "t1", seq).await;
        save_output(
            &c,
            id,
            "charge",
            "__retry__",
            json!({"error": "http 503 from upstream"}),
            Utc::now(),
        )
        .await;
        fail_instance(&c, id).await;
    }
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["count"], 2);
    assert_eq!(groups[0]["error_code"], "HTTP_STATUS");
    assert_eq!(groups[0]["blocks"], json!(["charge"]));
}

#[tokio::test]
async fn error_marker_beats_retry_marker() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let id = create_instance(&c, "t1", seq).await;
    save_output(
        &c,
        id,
        "charge",
        "__retry__",
        json!({"error": "http 503 transient"}),
        Utc::now(),
    )
    .await;
    save_output(
        &c,
        id,
        "charge",
        "__error__",
        json!({"__error__": true, "message": "rate limit exhausted"}),
        Utc::now() + chrono::Duration::milliseconds(5),
    )
    .await;
    fail_instance(&c, id).await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["error_code"], "RATE_LIMITED");
}

#[tokio::test]
async fn later_error_row_wins() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let id = create_instance(&c, "t1", seq).await;
    let earlier = Utc::now() - chrono::Duration::seconds(60);
    save_output(
        &c,
        id,
        "charge",
        "__error__",
        json!({"__error__": true, "message": "http 503 first attempt"}),
        earlier,
    )
    .await;
    save_output(
        &c,
        id,
        "charge",
        "__error__",
        json!({"__error__": true, "message": "credential finally revoked"}),
        Utc::now(),
    )
    .await;
    fail_instance(&c, id).await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["error_code"], "CREDENTIAL");
}

#[tokio::test]
async fn no_output_failures_group_as_unknown() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..2 {
        let id = create_instance(&c, "t1", seq).await;
        fail_instance(&c, id).await;
    }
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["count"], 2);
    assert_eq!(groups[0]["error_code"], "unknown");
    assert_eq!(groups[0]["blocks"], json!([]));
    assert_eq!(groups[0]["handlers"], json!([]));
    assert_eq!(
        groups[0]["sample_message"],
        "instance failed with no recorded step error"
    );
}

#[tokio::test]
async fn error_output_without_message_field_uses_unknown_error() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let id = create_instance(&c, "t1", seq).await;
    save_output(&c, id, "charge", "__error__", json!({"__error__": true}), Utc::now()).await;
    fail_instance(&c, id).await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["error_code"], "unknown");
    assert_eq!(groups[0]["sample_message"], "unknown error");
}

#[tokio::test]
async fn retry_marker_without_error_field_uses_unknown_error() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let id = create_instance(&c, "t1", seq).await;
    save_output(&c, id, "charge", "__retry__", json!({"attempt": 3}), Utc::now()).await;
    fail_instance(&c, id).await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["sample_message"], "unknown error");
}

#[tokio::test]
async fn unresolvable_block_reports_no_handler() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant(&c, "t1", seq, "ghost", "http 503").await;
    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["blocks"], json!(["ghost"]));
    assert_eq!(groups[0]["handlers"], json!([]));
}

// =====================================================================
// Filters and tenant scoping
// =====================================================================

#[tokio::test]
async fn sequence_id_filter_selects_matching_groups() {
    let c = ctx().await;
    let seq_a = seq_one_block(&c).await;
    let seq_b = seq_one_block(&c).await;
    plant_default(&c, seq_a, "http 503").await;
    plant_default(&c, seq_b, "rate limit hit").await;

    let resp = c
        .client
        .get(format!(
            "{}/instances/dlq/groups?sequence_id={seq_a}",
            c.base
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let groups: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["sequences"], json!([seq_a.to_string()]));
    assert_eq!(groups[0]["error_code"], "HTTP_STATUS");
}

#[tokio::test]
async fn sequence_id_filter_with_unknown_sequence_is_empty() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    let resp = c
        .client
        .get(format!(
            "{}/instances/dlq/groups?sequence_id={}",
            c.base,
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let groups: Vec<Value> = resp.json().await.unwrap();
    assert!(groups.is_empty(), "{groups:#?}");
}

#[tokio::test]
async fn tenant_header_scopes_groups() {
    let c = ctx().await;
    let seq1 = seq_one_block(&c).await;
    let seq2 = create_sequence(&c, "t2", &[("charge", "charge_card")]).await;
    plant(&c, "t1", seq1, "charge", "http 503").await;
    plant(&c, "t2", seq2, "charge", "rate limit hit").await;

    let t1 = groups_for(&c, "t1").await;
    assert_eq!(t1.len(), 1, "{t1:#?}");
    assert_eq!(t1[0]["error_code"], "HTTP_STATUS");

    let t2 = groups_for(&c, "t2").await;
    assert_eq!(t2.len(), 1, "{t2:#?}");
    assert_eq!(t2[0]["error_code"], "RATE_LIMITED");
}

#[tokio::test]
async fn tenant_query_param_scopes_when_no_header() {
    let c = ctx().await;
    let seq1 = seq_one_block(&c).await;
    let seq2 = create_sequence(&c, "t2", &[("charge", "charge_card")]).await;
    plant(&c, "t1", seq1, "charge", "http 503").await;
    plant(&c, "t2", seq2, "charge", "rate limit hit").await;

    let resp = c
        .client
        .get(format!("{}/instances/dlq/groups?tenant_id=t2", c.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let groups: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["error_code"], "RATE_LIMITED");
}

#[tokio::test]
async fn tenant_header_overrides_query_param() {
    let c = ctx().await;
    let seq1 = seq_one_block(&c).await;
    let seq2 = create_sequence(&c, "t2", &[("charge", "charge_card")]).await;
    plant(&c, "t1", seq1, "charge", "http 503").await;
    plant(&c, "t2", seq2, "charge", "rate limit hit").await;

    let resp = c
        .client
        .get(format!("{}/instances/dlq/groups?tenant_id=t2", c.base))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let groups: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["error_code"], "HTTP_STATUS");
}

#[tokio::test]
async fn no_tenant_scope_sees_all_tenants() {
    let c = ctx().await;
    let seq1 = seq_one_block(&c).await;
    let seq2 = create_sequence(&c, "t2", &[("charge", "charge_card")]).await;
    plant(&c, "t1", seq1, "charge", "http 503").await;
    plant(&c, "t2", seq2, "charge", "rate limit hit").await;

    let resp = c
        .client
        .get(format!("{}/instances/dlq/groups", c.base))
        .send()
        .await
        .unwrap();
    let groups: Vec<Value> = resp.json().await.unwrap();
    // The harness runs with require_tenant=false: an unscoped admin call
    // sees every tenant's groups.
    assert_eq!(groups.len(), 2, "{groups:#?}");
}

#[tokio::test]
async fn non_failed_instances_are_not_grouped() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    // One genuinely failed, one still in its initial state, one completed.
    plant_default(&c, seq, "http 503").await;
    create_instance(&c, "t1", seq).await;
    let done = create_instance(&c, "t1", seq).await;
    complete_instance(&c, done).await;

    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["count"], 1);
}

#[tokio::test]
async fn empty_dlq_returns_empty_list() {
    let c = ctx().await;
    let groups = groups_t1(&c).await;
    assert!(groups.is_empty(), "{groups:#?}");
}

// =====================================================================
// Sample retry
// =====================================================================

#[tokio::test]
async fn sample_retry_reschedules_newest_member() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    tick().await;
    let newest = plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;

    let resp = retry(&c, "t1", &fp, &json!({"mode": "sample"})).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(retried_ids(&body), vec![newest]);
    assert_eq!(instance_state(&c, newest).await, InstanceState::Scheduled);
}

#[tokio::test]
async fn sample_retry_marks_metadata_with_fingerprint_and_time() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let id = plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;

    let resp = retry(&c, "t1", &fp, &json!({"mode": "sample"})).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let inst = c
        .srv
        .storage
        .get_instance(InstanceId::from_uuid(id))
        .await
        .unwrap()
        .unwrap();
    let marker = &inst.metadata["dlq_sample_retry"];
    assert_eq!(marker["fingerprint"], json!(fp));
    let at: DateTime<Utc> = marker["at"].as_str().unwrap().parse().unwrap();
    assert!(at <= Utc::now());
}

#[tokio::test]
async fn sample_retry_response_shape() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;

    let body: Value = retry(&c, "t1", &fp, &json!({"mode": "sample"}))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(body["fingerprint"], json!(fp));
    assert_eq!(body["mode"], "sample");
    assert_eq!(body["retried"].as_array().unwrap().len(), 1);
    assert_eq!(body["skipped"], 0);
}

#[tokio::test]
async fn sample_retry_shrinks_group_by_one() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..3 {
        plant_default(&c, seq, "http 503").await;
    }
    let fp = fingerprint_of_only_group(&c).await;
    retry(&c, "t1", &fp, &json!({"mode": "sample"})).await;

    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["count"], 2);
    assert_eq!(groups[0]["fingerprint"], json!(fp));
}

#[tokio::test]
async fn sample_retry_on_singleton_group_empties_dlq() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;
    let resp = retry(&c, "t1", &fp, &json!({"mode": "sample"})).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let groups = groups_t1(&c).await;
    assert!(groups.is_empty(), "{groups:#?}");
}

#[tokio::test]
async fn sample_retry_wipes_in_progress_sentinels() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let id = plant_default(&c, seq, "http 503").await;
    save_output(&c, id, "charge", "__in_progress__", json!({"tick": 1}), Utc::now()).await;
    let fp = fingerprint_of_only_group(&c).await;

    retry(&c, "t1", &fp, &json!({"mode": "sample"})).await;

    let outputs = c
        .srv
        .storage
        .get_all_outputs(InstanceId::from_uuid(id))
        .await
        .unwrap();
    assert!(
        !outputs
            .iter()
            .any(|o| o.output_ref.as_deref() == Some("__in_progress__")),
        "in-progress sentinel should be wiped"
    );
}

#[tokio::test]
async fn consecutive_sample_retries_walk_newest_first() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let oldest = plant_default(&c, seq, "http 503").await;
    tick().await;
    let middle = plant_default(&c, seq, "http 503").await;
    tick().await;
    let newest = plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;

    let first: Value = retry(&c, "t1", &fp, &json!({"mode": "sample"}))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(retried_ids(&first), vec![newest]);

    let second: Value = retry(&c, "t1", &fp, &json!({"mode": "sample"}))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(retried_ids(&second), vec![middle]);
    assert_eq!(instance_state(&c, oldest).await, InstanceState::Failed);
}

#[tokio::test]
async fn refailed_sample_rejoins_its_group() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    tick().await;
    let sample = plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;

    retry(&c, "t1", &fp, &json!({"mode": "sample"})).await;
    // The retried instance fails again; its old __error__ row still
    // carries the evidence, so it lands back in the same group.
    fail_instance(&c, sample).await;

    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 1, "{groups:#?}");
    assert_eq!(groups[0]["count"], 2);
    assert_eq!(groups[0]["fingerprint"], json!(fp));
}

// =====================================================================
// Bulk gating matrix
// =====================================================================

#[tokio::test]
async fn bulk_without_sample_or_force_conflicts() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;
    let resp = retry(&c, "t1", &fp, &json!({"mode": "bulk"})).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    // Nothing was touched.
    let groups = groups_t1(&c).await;
    assert_eq!(groups[0]["count"], 1);
}

#[tokio::test]
async fn bulk_with_unfinished_sample_conflicts() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..2 {
        plant_default(&c, seq, "http 503").await;
    }
    let fp = fingerprint_of_only_group(&c).await;
    let body: Value = retry(&c, "t1", &fp, &json!({"mode": "sample"}))
        .await
        .json()
        .await
        .unwrap();
    let sample = retried_ids(&body)[0];

    // Sample is Scheduled, not Completed → still locked.
    let resp = retry(
        &c,
        "t1",
        &fp,
        &json!({"mode": "bulk", "sample_verified_instance_id": sample}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn bulk_with_sample_that_failed_again_conflicts() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..2 {
        plant_default(&c, seq, "http 503").await;
    }
    let fp = fingerprint_of_only_group(&c).await;
    let body: Value = retry(&c, "t1", &fp, &json!({"mode": "sample"}))
        .await
        .json()
        .await
        .unwrap();
    let sample = retried_ids(&body)[0];
    // The sample run fails again — bulk must stay locked.
    fail_instance(&c, sample).await;

    let resp = retry(
        &c,
        "t1",
        &fp,
        &json!({"mode": "bulk", "sample_verified_instance_id": sample}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn bulk_citing_sample_from_another_group_conflicts() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    plant_default(&c, seq, "rate limit hit").await;
    plant_default(&c, seq, "rate limit hit").await;

    let groups = groups_t1(&c).await;
    assert_eq!(groups.len(), 2, "{groups:#?}");
    let fp_rate = groups[0]["fingerprint"].as_str().unwrap().to_string();
    let fp_http = groups[1]["fingerprint"].as_str().unwrap().to_string();
    assert_eq!(groups[0]["error_code"], "RATE_LIMITED");

    // Sample-retry the HTTP group, complete it...
    let body: Value = retry(&c, "t1", &fp_http, &json!({"mode": "sample"}))
        .await
        .json()
        .await
        .unwrap();
    let http_sample = retried_ids(&body)[0];
    complete_instance(&c, http_sample).await;

    // ...then try to unlock the RATE_LIMITED group with it.
    let resp = retry(
        &c,
        "t1",
        &fp_rate,
        &json!({"mode": "bulk", "sample_verified_instance_id": http_sample}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn bulk_with_nonexistent_sample_is_404() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;
    let resp = retry(
        &c,
        "t1",
        &fp,
        &json!({"mode": "bulk", "sample_verified_instance_id": Uuid::now_v7()}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn bulk_citing_member_never_sample_retried_conflicts() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let member = plant_default(&c, seq, "http 503").await;
    plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;
    // A real group member, but it never went through sample retry — no
    // marker, so it cannot unlock bulk.
    let resp = retry(
        &c,
        "t1",
        &fp,
        &json!({"mode": "bulk", "sample_verified_instance_id": member}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn bulk_with_completed_sample_unlocks_and_retries_rest() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let mut members = BTreeSet::new();
    for _ in 0..4 {
        members.insert(plant_default(&c, seq, "http 503").await);
    }
    let fp = fingerprint_of_only_group(&c).await;
    let body: Value = retry(&c, "t1", &fp, &json!({"mode": "sample"}))
        .await
        .json()
        .await
        .unwrap();
    let sample = retried_ids(&body)[0];
    complete_instance(&c, sample).await;

    let resp = retry(
        &c,
        "t1",
        &fp,
        &json!({"mode": "bulk", "sample_verified_instance_id": sample}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let bulk: Value = resp.json().await.unwrap();
    let retried: BTreeSet<Uuid> = retried_ids(&bulk).into_iter().collect();
    assert_eq!(retried.len(), 3);
    assert!(!retried.contains(&sample), "sample must not be re-retried");
    assert_eq!(bulk["skipped"], 0);
    for id in retried {
        assert_eq!(instance_state(&c, id).await, InstanceState::Scheduled);
    }
    // The completed sample stays completed.
    assert_eq!(instance_state(&c, sample).await, InstanceState::Completed);
}

#[tokio::test]
async fn bulk_force_unlocks_without_any_sample() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..3 {
        plant_default(&c, seq, "http 503").await;
    }
    let fp = fingerprint_of_only_group(&c).await;
    let resp = retry(&c, "t1", &fp, &json!({"mode": "bulk", "force": true})).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["retried"].as_array().unwrap().len(), 3);
    assert_eq!(body["mode"], "bulk");
}

#[tokio::test]
async fn bulk_force_ignores_bogus_sample_reference() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;
    // force=true short-circuits verification entirely — even a
    // nonexistent sample id does not matter.
    let resp = retry(
        &c,
        "t1",
        &fp,
        &json!({
            "mode": "bulk",
            "force": true,
            "sample_verified_instance_id": Uuid::now_v7(),
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

// =====================================================================
// Bulk limits and idempotency
// =====================================================================

#[tokio::test]
async fn bulk_limit_bounds_the_retry() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..3 {
        plant_default(&c, seq, "http 503").await;
    }
    let fp = fingerprint_of_only_group(&c).await;
    let body: Value = retry(&c, "t1", &fp, &json!({"mode": "bulk", "force": true, "limit": 1}))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(body["retried"].as_array().unwrap().len(), 1);
    // The other two stay failed and grouped.
    let groups = groups_t1(&c).await;
    assert_eq!(groups[0]["count"], 2);
}

#[tokio::test]
async fn bulk_limit_two_of_three() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..3 {
        plant_default(&c, seq, "http 503").await;
    }
    let fp = fingerprint_of_only_group(&c).await;
    let body: Value = retry(&c, "t1", &fp, &json!({"mode": "bulk", "force": true, "limit": 2}))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(body["retried"].as_array().unwrap().len(), 2);
    let groups = groups_t1(&c).await;
    assert_eq!(groups[0]["count"], 1);
}

#[tokio::test]
async fn bulk_limit_zero_retries_nothing() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..2 {
        plant_default(&c, seq, "http 503").await;
    }
    let fp = fingerprint_of_only_group(&c).await;
    let resp = retry(&c, "t1", &fp, &json!({"mode": "bulk", "force": true, "limit": 0})).await;
    // limit=0 is honored literally: a successful call that touches nothing.
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert!(body["retried"].as_array().unwrap().is_empty());
    assert_eq!(body["skipped"], 0);
    let groups = groups_t1(&c).await;
    assert_eq!(groups[0]["count"], 2);
}

#[tokio::test]
async fn bulk_without_limit_uses_default_covering_small_groups() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..4 {
        plant_default(&c, seq, "http 503").await;
    }
    let fp = fingerprint_of_only_group(&c).await;
    let body: Value = retry(&c, "t1", &fp, &json!({"mode": "bulk", "force": true}))
        .await
        .json()
        .await
        .unwrap();
    // Default limit is 100 — far above this group's size.
    assert_eq!(body["retried"].as_array().unwrap().len(), 4);
    let groups = groups_t1(&c).await;
    assert!(groups.is_empty(), "{groups:#?}");
}

#[tokio::test]
async fn bulk_limit_above_cap_is_accepted_and_bounded_by_group_size() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..2 {
        plant_default(&c, seq, "http 503").await;
    }
    let fp = fingerprint_of_only_group(&c).await;
    let resp = retry(
        &c,
        "t1",
        &fp,
        &json!({"mode": "bulk", "force": true, "limit": 600}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["retried"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn second_bulk_after_full_retry_is_404() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    for _ in 0..2 {
        plant_default(&c, seq, "http 503").await;
    }
    let fp = fingerprint_of_only_group(&c).await;
    let first = retry(&c, "t1", &fp, &json!({"mode": "bulk", "force": true})).await;
    assert_eq!(first.status(), StatusCode::OK);
    // Everything is rescheduled; the group no longer exists.
    let second = retry(&c, "t1", &fp, &json!({"mode": "bulk", "force": true})).await;
    assert_eq!(second.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn member_rescheduled_before_bulk_is_simply_not_a_member() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let mut members = Vec::new();
    for _ in 0..3 {
        members.push(plant_default(&c, seq, "http 503").await);
    }
    let fp = fingerprint_of_only_group(&c).await;

    // Someone reschedules one member out-of-band before the bulk call.
    c.srv
        .storage
        .update_instance_state(
            InstanceId::from_uuid(members[0]),
            InstanceState::Scheduled,
            None,
        )
        .await
        .unwrap();

    let body: Value = retry(&c, "t1", &fp, &json!({"mode": "bulk", "force": true}))
        .await
        .json()
        .await
        .unwrap();
    let retried: BTreeSet<Uuid> = retried_ids(&body).into_iter().collect();
    assert_eq!(retried.len(), 2);
    assert!(!retried.contains(&members[0]));
    assert_eq!(body["skipped"], 0);
}

#[tokio::test]
async fn concurrent_bulk_retries_reschedule_every_member_exactly_once() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let mut members = BTreeSet::new();
    for _ in 0..4 {
        members.insert(plant_default(&c, seq, "http 503").await);
    }
    let fp = fingerprint_of_only_group(&c).await;

    let body = json!({"mode": "bulk", "force": true});
    let (a, b) = tokio::join!(retry(&c, "t1", &fp, &body), retry(&c, "t1", &fp, &body));

    let mut retried_union: BTreeSet<Uuid> = BTreeSet::new();
    for resp in [a, b] {
        let status = resp.status();
        assert!(
            status == StatusCode::OK || status == StatusCode::NOT_FOUND,
            "unexpected status {status}"
        );
        if status == StatusCode::OK {
            let body: Value = resp.json().await.unwrap();
            retried_union.extend(retried_ids(&body));
        }
    }
    // Between the two calls every member was rescheduled, none invented.
    assert_eq!(retried_union, members);
    for id in &members {
        assert_eq!(instance_state(&c, *id).await, InstanceState::Scheduled);
    }
}

// =====================================================================
// Post-retry state, 404s, validation
// =====================================================================

#[tokio::test]
async fn bulk_retried_instances_are_scheduled_with_wiped_sentinels() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let mut members = Vec::new();
    for _ in 0..3 {
        let id = plant_default(&c, seq, "http 503").await;
        save_output(&c, id, "charge", "__in_progress__", json!({"t": 0}), Utc::now()).await;
        members.push(id);
    }
    let fp = fingerprint_of_only_group(&c).await;
    let resp = retry(&c, "t1", &fp, &json!({"mode": "bulk", "force": true})).await;
    assert_eq!(resp.status(), StatusCode::OK);

    for id in members {
        assert_eq!(instance_state(&c, id).await, InstanceState::Scheduled);
        let outputs = c
            .srv
            .storage
            .get_all_outputs(InstanceId::from_uuid(id))
            .await
            .unwrap();
        assert!(
            !outputs
                .iter()
                .any(|o| o.output_ref.as_deref() == Some("__in_progress__")),
            "stale in-progress sentinel survived retry"
        );
    }
}

#[tokio::test]
async fn sample_retry_unknown_fingerprint_is_404() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    let resp = retry(&c, "t1", "ffffffffffffffff", &json!({"mode": "sample"})).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn bulk_retry_unknown_fingerprint_is_404_even_with_force() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    let resp = retry(
        &c,
        "t1",
        "0000000000000000",
        &json!({"mode": "bulk", "force": true}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn cross_tenant_fingerprint_retry_is_404_and_leaves_instances_failed() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let victim = plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;

    // Another tenant cannot see (or retry) t1's group.
    let resp = retry(&c, "intruder", &fp, &json!({"mode": "sample"})).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(instance_state(&c, victim).await, InstanceState::Failed);
}

#[tokio::test]
async fn invalid_retry_mode_is_a_client_error() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;
    let resp = retry(&c, "t1", &fp, &json!({"mode": "nuke_it_all"})).await;
    assert!(
        resp.status().is_client_error(),
        "expected 4xx, got {}",
        resp.status()
    );
    // And nothing was retried.
    let groups = groups_t1(&c).await;
    assert_eq!(groups[0]["count"], 1);
}

#[tokio::test]
async fn negative_limit_is_a_client_error() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    plant_default(&c, seq, "http 503").await;
    let fp = fingerprint_of_only_group(&c).await;
    let resp = retry(
        &c,
        "t1",
        &fp,
        &json!({"mode": "bulk", "force": true, "limit": -5}),
    )
    .await;
    assert!(
        resp.status().is_client_error(),
        "expected 4xx, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn sample_then_bulk_full_recovery_flow() {
    let c = ctx().await;
    let seq = seq_one_block(&c).await;
    let mut members = BTreeSet::new();
    for _ in 0..5 {
        members.insert(plant_default(&c, seq, "http 503 from gateway (req-8f2a9b17)").await);
    }
    let fp = fingerprint_of_only_group(&c).await;

    // 1. Guided flow: sample first.
    let sample_body: Value = retry(&c, "t1", &fp, &json!({"mode": "sample"}))
        .await
        .json()
        .await
        .unwrap();
    let sample = retried_ids(&sample_body)[0];
    assert!(members.contains(&sample));

    // 2. Sample succeeds.
    complete_instance(&c, sample).await;

    // 3. Bulk unlocks and drains the group.
    let bulk: Value = retry(
        &c,
        "t1",
        &fp,
        &json!({"mode": "bulk", "sample_verified_instance_id": sample}),
    )
    .await
    .json()
    .await
    .unwrap();
    assert_eq!(bulk["retried"].as_array().unwrap().len(), 4);

    // 4. DLQ is clean.
    let groups = groups_t1(&c).await;
    assert!(groups.is_empty(), "{groups:#?}");
}
