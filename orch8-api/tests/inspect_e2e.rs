//! Extensive E2E tests for the Template Resolution Inspector endpoints:
//!
//! - `POST /sequences/inspect-template` (draft-by-value / stored-by-id)
//! - `GET  /instances/{id}/blocks/{block}/resolved-input` (`?at_block=`)
//!
//! The API test harness has no engine loop, so executed steps are simulated
//! by writing `block_outputs` rows directly through `srv.storage` (same
//! pattern as `resume_from.rs` / `timeline_fork.rs`). Output visibility is
//! governed by the historical boundary in `orch8-api/src/inspect.rs`:
//! rows are read in `created_at` order, `_`-prefixed block ids are skipped,
//! iteration breaks at the first row of the boundary block (default: the
//! inspected block), and the last surviving row per block wins.

use chrono::{Duration, Utc};
use orch8_api::test_harness::spawn_test_server;
use orch8_storage::OutputStore;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::output::BlockOutput;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------- helpers

#[allow(clippy::needless_pass_by_value)]
fn step(id: &str, params: Value) -> Value {
    json!({"type": "step", "id": id, "handler": "noop", "params": params})
}

#[allow(clippy::needless_pass_by_value)]
fn seq_with_blocks(id: Uuid, tenant: &str, blocks: Vec<Value>) -> Value {
    json!({
        "id": id,
        "tenant_id": tenant,
        "namespace": "default",
        "name": format!("inspect-e2e-{id}"),
        "version": 1,
        "blocks": blocks,
        "created_at": Utc::now().to_rfc3339(),
    })
}

/// The standard 4-step pipeline used by the instance-endpoint tests.
/// `notify` references outputs from every position relative to itself,
/// plus its own output and the instance context.
fn pipeline_seq(id: Uuid, tenant: &str) -> Value {
    seq_with_blocks(
        id,
        tenant,
        vec![
            step("fetch", json!({})),
            step("transform", json!({})),
            step(
                "notify",
                json!({
                    "from_fetch": "{{ outputs.fetch.result }}",
                    "from_transform": "{{ outputs.transform.result }}",
                    "from_audit": "{{ outputs.audit.result }}",
                    "self_ref": "{{ outputs.notify.result }}",
                    "email": "{{ context.data.customer.email }}"
                }),
            ),
            step("audit", json!({})),
        ],
    )
}

async fn create_sequence(client: &reqwest::Client, base: &str, tenant: &str, body: &Value) {
    let resp = client
        .post(format!("{base}/sequences"))
        .header("X-Tenant-Id", tenant)
        .json(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "sequence create failed");
}

async fn create_instance(
    client: &reqwest::Client,
    base: &str,
    seq_id: Uuid,
    tenant: &str,
    context: Value,
) -> Uuid {
    let resp = client
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", tenant)
        .json(&json!({
            "sequence_id": seq_id,
            "tenant_id": tenant,
            "namespace": "default",
            "context": context
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "instance create failed");
    let created: Value = resp.json().await.unwrap();
    created["id"].as_str().unwrap().parse().unwrap()
}

/// A block output row with a deterministic timestamp offset so the
/// `created_at` ordering used by the endpoint is stable.
fn out_at(inst: Uuid, block: &str, offset_secs: i64, output: Value) -> BlockOutput {
    BlockOutput {
        id: Uuid::now_v7(),
        instance_id: InstanceId::from_uuid(inst),
        block_id: BlockId::new(block),
        output,
        output_ref: None,
        output_size: 0,
        attempt: 0,
        created_at: Utc::now() - Duration::seconds(1000) + Duration::seconds(offset_secs),
    }
}

/// Same, with an `output_ref` bookkeeping marker (`__error__`/`__retry__`).
fn marker_at(
    inst: Uuid,
    block: &str,
    offset_secs: i64,
    output: Value,
    marker: &str,
) -> BlockOutput {
    let mut o = out_at(inst, block, offset_secs, output);
    o.output_ref = Some(marker.to_string());
    o
}

fn entry<'a>(trace: &'a Value, path: &str) -> &'a Value {
    trace["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["param_path"] == path)
        .unwrap_or_else(|| panic!("no entry {path}: {trace}"))
}

async fn post_inspect(
    client: &reqwest::Client,
    v1: &str,
    tenant: Option<&str>,
    body: &Value,
) -> reqwest::Response {
    let mut req = client.post(format!("{v1}/sequences/inspect-template"));
    if let Some(t) = tenant {
        req = req.header("X-Tenant-Id", t);
    }
    req.json(body).send().await.unwrap()
}

async fn get_resolved_input(
    client: &reqwest::Client,
    v1: &str,
    inst: Uuid,
    block: &str,
    at_block: Option<&str>,
    tenant: &str,
) -> reqwest::Response {
    let mut url = format!("{v1}/instances/{inst}/blocks/{block}/resolved-input");
    if let Some(at) = at_block {
        use std::fmt::Write as _;
        let _ = write!(url, "?at_block={at}");
    }
    client
        .get(url)
        .header("X-Tenant-Id", tenant)
        .send()
        .await
        .unwrap()
}

/// Draft-inspect one block of an ad-hoc sequence and return the trace.
async fn draft_trace(blocks: Vec<Value>, block_id: &str, ctx_data: Value, outputs: Value) -> Value {
    draft_trace_cfg(blocks, block_id, ctx_data, json!({}), outputs).await
}

async fn draft_trace_cfg(
    blocks: Vec<Value>,
    block_id: &str,
    ctx_data: Value,
    ctx_config: Value,
    outputs: Value,
) -> Value {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let body = json!({
        "sequence": seq_with_blocks(Uuid::now_v7(), "t1", blocks),
        "block_id": block_id,
        "context_data": ctx_data,
        "context_config": ctx_config,
        "outputs": outputs
    });
    let resp = post_inspect(&client, &srv.v1_url(), Some("t1"), &body).await;
    assert_eq!(resp.status(), StatusCode::OK, "inspect failed");
    resp.json().await.unwrap()
}

// =====================================================================
// Draft inspection: resolution statuses over HTTP
// =====================================================================

#[tokio::test]
async fn draft_resolves_context_data_ok_status() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({"email": "{{ context.data.customer.email }}"}),
        )],
        "s1",
        json!({"customer": {"email": "a@b.c"}}),
        json!({}),
    )
    .await;
    let e = entry(&trace, "email");
    assert_eq!(e["status"], "ok");
    assert_eq!(e["value"], "a@b.c");
    assert_eq!(e["source"], "context.data.customer.email");
    assert_eq!(e["fallback_used"], false);
}

#[tokio::test]
async fn draft_reports_missing_status() {
    let trace = draft_trace(
        vec![step("s1", json!({"v": "{{ context.data.absent }}"}))],
        "s1",
        json!({}),
        json!({}),
    )
    .await;
    let e = entry(&trace, "v");
    assert_eq!(e["status"], "missing");
    assert_eq!(e["value"], Value::Null);
}

#[tokio::test]
async fn draft_reports_explicit_null_status() {
    let trace = draft_trace(
        vec![step("s1", json!({"v": "{{ context.data.field }}"}))],
        "s1",
        json!({"field": null}),
        json!({}),
    )
    .await;
    assert_eq!(entry(&trace, "v")["status"], "null");
}

#[tokio::test]
async fn draft_null_and_missing_are_distinct_statuses() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({
                "explicit": "{{ context.data.here }}",
                "absent": "{{ context.data.gone }}"
            }),
        )],
        "s1",
        json!({"here": null}),
        json!({}),
    )
    .await;
    assert_eq!(entry(&trace, "explicit")["status"], "null");
    assert_eq!(entry(&trace, "absent")["status"], "missing");
}

#[tokio::test]
async fn draft_reports_error_status_with_message_and_no_value() {
    let trace = draft_trace(
        vec![step("s1", json!({"v": "{{ bogus.path }}"}))],
        "s1",
        json!({}),
        json!({}),
    )
    .await;
    let e = entry(&trace, "v");
    assert_eq!(e["status"], "error");
    assert!(
        e["error"]
            .as_str()
            .unwrap()
            .contains("unknown template root")
    );
    assert!(e.get("value").is_none());
    assert!(e.get("result_type").is_none());
}

#[tokio::test]
async fn draft_filter_error_surfaces_over_http() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({"v": "{{ context.data.x | round(nope) }}"}),
        )],
        "s1",
        json!({"x": 1.5}),
        json!({}),
    )
    .await;
    let e = entry(&trace, "v");
    assert_eq!(e["status"], "error");
    assert!(e["error"].as_str().unwrap().contains("round()"));
}

// =====================================================================
// Draft inspection: context/config/outputs fixtures
// =====================================================================

#[tokio::test]
async fn draft_context_config_resolution() {
    let trace = draft_trace_cfg(
        vec![step("s1", json!({"region": "{{ context.config.region }}"}))],
        "s1",
        json!({}),
        json!({"region": "eu-west-1"}),
        json!({}),
    )
    .await;
    let e = entry(&trace, "region");
    assert_eq!(e["status"], "ok");
    assert_eq!(e["value"], "eu-west-1");
    assert_eq!(e["source"], "context.config.region");
}

#[tokio::test]
async fn draft_mixes_data_and_config_sources() {
    let trace = draft_trace_cfg(
        vec![step(
            "s1",
            json!({
                "who": "{{ context.data.user }}",
                "mode": "{{ context.config.mode }}"
            }),
        )],
        "s1",
        json!({"user": "ada"}),
        json!({"mode": "fast"}),
        json!({}),
    )
    .await;
    assert_eq!(entry(&trace, "who")["value"], "ada");
    assert_eq!(entry(&trace, "mode")["value"], "fast");
}

#[tokio::test]
async fn draft_outputs_fixture_resolution() {
    let trace = draft_trace(
        vec![step("s1", json!({"amount": "{{ outputs.charge.amount }}"}))],
        "s1",
        json!({}),
        json!({"charge": {"amount": 42}}),
    )
    .await;
    let e = entry(&trace, "amount");
    assert_eq!(e["status"], "ok");
    assert_eq!(e["value"], 42);
    assert_eq!(e["result_type"], "number");
    assert_eq!(e["source"], "outputs.charge.amount");
}

#[tokio::test]
async fn draft_outputs_fixture_nested_field_and_array_index() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({
                "second_id": "{{ outputs.list.items.1.id }}",
                "deep": "{{ outputs.list.meta.page.size }}"
            }),
        )],
        "s1",
        json!({}),
        json!({"list": {
            "items": [{"id": "a"}, {"id": "b"}],
            "meta": {"page": {"size": 50}}
        }}),
    )
    .await;
    assert_eq!(entry(&trace, "second_id")["value"], "b");
    assert_eq!(entry(&trace, "deep")["value"], 50);
}

#[tokio::test]
async fn draft_array_index_navigation_in_context() {
    let trace = draft_trace(
        vec![step("s1", json!({"v": "{{ context.data.items.1 }}"}))],
        "s1",
        json!({"items": ["zero", "one"]}),
        json!({}),
    )
    .await;
    assert_eq!(entry(&trace, "v")["value"], "one");
}

#[tokio::test]
async fn draft_default_fixtures_are_empty() {
    // Omitting context_data/context_config/outputs entirely must behave
    // like empty fixtures, not error.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let body = json!({
        "sequence": seq_with_blocks(
            Uuid::now_v7(),
            "t1",
            vec![step("s1", json!({"v": "{{ context.data.x }}"}))]
        ),
        "block_id": "s1"
    });
    let resp = post_inspect(&client, &srv.v1_url(), Some("t1"), &body).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "v")["status"], "missing");
}

// =====================================================================
// Draft inspection: fallback provenance, coercion, shapes
// =====================================================================

#[tokio::test]
async fn draft_fallback_provenance_visible_over_http() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({"v": "{{ context.data.primary | context.data.secondary }}"}),
        )],
        "s1",
        json!({"secondary": "backup"}),
        json!({}),
    )
    .await;
    let e = entry(&trace, "v");
    assert_eq!(e["status"], "ok");
    assert_eq!(e["value"], "backup");
    assert_eq!(e["source"], "context.data.secondary");
    assert_eq!(e["fallback_used"], true);
}

#[tokio::test]
async fn draft_literal_fallback_source_is_literal() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({"v": "{{ context.data.absent | default-val }}"}),
        )],
        "s1",
        json!({}),
        json!({}),
    )
    .await;
    let e = entry(&trace, "v");
    assert_eq!(e["value"], "default-val");
    assert_eq!(e["source"], "literal");
    assert_eq!(e["fallback_used"], true);
}

#[tokio::test]
async fn draft_three_segment_chain_middle_segment_fires() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({"v": "{{ context.data.a | outputs.b.v | context.data.c }}"}),
        )],
        "s1",
        json!({"c": "never"}),
        json!({"b": {"v": "middle"}}),
    )
    .await;
    let e = entry(&trace, "v");
    assert_eq!(e["value"], "middle");
    assert_eq!(e["source"], "outputs.b.v");
    assert_eq!(e["fallback_used"], true);
}

#[tokio::test]
async fn draft_multi_expression_string_yields_entry_per_expression() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({"msg": "{{ context.data.a }} and {{ context.data.b }}"}),
        )],
        "s1",
        json!({"a": 1, "b": 2}),
        json!({}),
    )
    .await;
    let for_msg: Vec<&Value> = trace["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["param_path"] == "msg")
        .collect();
    assert_eq!(for_msg.len(), 2);
    assert_eq!(for_msg[0]["expression"], "context.data.a");
    assert_eq!(for_msg[1]["expression"], "context.data.b");
    assert_eq!(for_msg[0]["coerced_to_string"], true);
}

#[tokio::test]
async fn draft_nested_param_paths_in_response() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({
                "body": {"customer": {"email": "{{ context.data.email }}"}},
                "headers": [{"value": "{{ context.data.h }}"}]
            }),
        )],
        "s1",
        json!({"email": "x@y.z", "h": "v"}),
        json!({}),
    )
    .await;
    assert_eq!(entry(&trace, "body.customer.email")["value"], "x@y.z");
    assert_eq!(entry(&trace, "headers.0.value")["value"], "v");
}

#[tokio::test]
async fn draft_coerced_to_string_flag_for_inline_number() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({"msg": "total: {{ context.data.n }} cents"}),
        )],
        "s1",
        json!({"n": 42}),
        json!({}),
    )
    .await;
    let e = entry(&trace, "msg");
    assert_eq!(e["coerced_to_string"], true);
    assert_eq!(e["value"], 42);
    assert_eq!(e["result_type"], "number");
    assert_eq!(trace["resolved_params"]["msg"], "total: 42 cents");
}

#[tokio::test]
async fn draft_result_types_for_all_value_shapes() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({
                "s": "{{ context.data.s }}",
                "n": "{{ context.data.n }}",
                "b": "{{ context.data.b }}",
                "a": "{{ context.data.a }}",
                "o": "{{ context.data.o }}"
            }),
        )],
        "s1",
        json!({"s": "x", "n": 1, "b": true, "a": [1], "o": {"k": "v"}}),
        json!({}),
    )
    .await;
    assert_eq!(entry(&trace, "s")["result_type"], "string");
    assert_eq!(entry(&trace, "n")["result_type"], "number");
    assert_eq!(entry(&trace, "b")["result_type"], "boolean");
    assert_eq!(entry(&trace, "a")["result_type"], "array");
    assert_eq!(entry(&trace, "o")["result_type"], "object");
    assert_eq!(entry(&trace, "o")["value"], json!({"k": "v"}));
}

#[tokio::test]
async fn draft_pipe_filters_applied_in_traced_value() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({
                "up": "{{ context.data.name | upper }}",
                "def": "{{ context.data.absent | default('none') }}"
            }),
        )],
        "s1",
        json!({"name": "bob"}),
        json!({}),
    )
    .await;
    assert_eq!(entry(&trace, "up")["value"], "BOB");
    assert_eq!(entry(&trace, "def")["value"], "none");
}

#[tokio::test]
async fn draft_no_template_block_produces_zero_entries() {
    let trace = draft_trace(
        vec![step("s1", json!({"static": "hello", "n": 5}))],
        "s1",
        json!({}),
        json!({}),
    )
    .await;
    assert_eq!(trace["entries"].as_array().unwrap().len(), 0);
    assert_eq!(trace["resolved_params"], json!({"static": "hello", "n": 5}));
}

#[tokio::test]
async fn draft_empty_params_produce_zero_entries() {
    let trace = draft_trace(vec![step("s1", json!({}))], "s1", json!({}), json!({})).await;
    assert_eq!(trace["entries"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn draft_block_id_echoed_in_trace() {
    let trace = draft_trace(
        vec![step("first", json!({})), step("target", json!({}))],
        "target",
        json!({}),
        json!({}),
    )
    .await;
    assert_eq!(trace["block_id"], "target");
}

// =====================================================================
// Draft inspection: redaction end-to-end
// =====================================================================

#[tokio::test]
async fn draft_redacts_sensitive_param_names_end_to_end() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({
                "api_key": "{{ context.data.k }}",
                "password": "{{ context.data.p }}",
                "token": "{{ context.data.t }}",
                "authorization": "{{ context.data.a }}"
            }),
        )],
        "s1",
        json!({"k": "k-secret", "p": "p-secret", "t": "t-secret", "a": "a-secret"}),
        json!({}),
    )
    .await;
    for path in ["api_key", "password", "token", "authorization"] {
        let e = entry(&trace, path);
        assert_eq!(e["status"], "redacted", "{path} not redacted");
        assert_eq!(e["value"], "[REDACTED]");
    }
    let body = trace.to_string();
    for secret in ["k-secret", "p-secret", "t-secret", "a-secret"] {
        assert!(!body.contains(secret), "{secret} leaked: {body}");
    }
}

#[tokio::test]
async fn draft_redacts_secret_shaped_values_under_innocent_names() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({
                "note": "{{ context.data.stripe }}",
                "remark": "{{ context.data.github }}"
            }),
        )],
        "s1",
        json!({"stripe": "sk_live_abc123", "github": "ghp_tok456"}),
        json!({}),
    )
    .await;
    assert_eq!(entry(&trace, "note")["status"], "redacted");
    assert_eq!(entry(&trace, "remark")["status"], "redacted");
    let body = trace.to_string();
    assert!(!body.contains("sk_live_abc123"));
    assert!(!body.contains("ghp_tok456"));
}

#[tokio::test]
async fn draft_redacts_bearer_and_jwt_values() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({
                "hdr": "{{ context.data.bearer }}",
                "blob": "{{ context.data.jwt }}"
            }),
        )],
        "s1",
        json!({
            "bearer": "Bearer supersecrettoken",
            "jwt": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sigpart"
        }),
        json!({}),
    )
    .await;
    assert_eq!(entry(&trace, "hdr")["status"], "redacted");
    assert_eq!(entry(&trace, "blob")["status"], "redacted");
    let body = trace.to_string();
    assert!(!body.contains("supersecrettoken"));
    assert!(!body.contains("sigpart"));
}

#[tokio::test]
async fn draft_resolved_params_aggregate_is_redacted() {
    let trace = draft_trace(
        vec![step(
            "s1",
            json!({
                "api_key": "{{ context.data.k }}",
                "plain": "{{ context.data.v }}"
            }),
        )],
        "s1",
        json!({"k": "hush", "v": "visible"}),
        json!({}),
    )
    .await;
    assert_eq!(trace["resolved_params"]["api_key"], "[REDACTED]");
    assert_eq!(trace["resolved_params"]["plain"], "visible");
    assert!(!trace.to_string().contains("hush"));
}

#[tokio::test]
async fn draft_secret_in_outputs_fixture_never_leaves_in_clear() {
    let trace = draft_trace(
        vec![step("s1", json!({"cred": "{{ outputs.login.token }}"}))],
        "s1",
        json!({}),
        json!({"login": {"token": "xoxb-slack-secret"}}),
    )
    .await;
    assert_eq!(entry(&trace, "cred")["status"], "redacted");
    assert!(!trace.to_string().contains("xoxb-slack-secret"));
}

// =====================================================================
// Draft inspection: request validation, sources, tenancy
// =====================================================================

#[tokio::test]
async fn draft_unknown_block_is_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let body = json!({
        "sequence": seq_with_blocks(Uuid::now_v7(), "t1", vec![step("s1", json!({}))]),
        "block_id": "ghost"
    });
    let resp = post_inspect(&client, &srv.v1_url(), Some("t1"), &body).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn draft_both_sequence_and_sequence_id_rejected_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let body = json!({
        "sequence": seq_with_blocks(Uuid::now_v7(), "t1", vec![step("s1", json!({}))]),
        "sequence_id": Uuid::now_v7(),
        "block_id": "s1"
    });
    let resp = post_inspect(&client, &srv.v1_url(), Some("t1"), &body).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn draft_neither_source_rejected_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = post_inspect(
        &client,
        &srv.v1_url(),
        Some("t1"),
        &json!({"block_id": "s1"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn draft_unknown_sequence_id_is_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = post_inspect(
        &client,
        &srv.v1_url(),
        Some("t1"),
        &json!({"sequence_id": Uuid::now_v7(), "block_id": "s1"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn draft_stored_sequence_by_id_resolves() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    let seq = seq_with_blocks(
        seq_id,
        "t1",
        vec![step("s1", json!({"v": "{{ context.data.x }}"}))],
    );
    create_sequence(&client, &v1, "t1", &seq).await;

    let resp = post_inspect(
        &client,
        &v1,
        Some("t1"),
        &json!({
            "sequence_id": seq_id,
            "block_id": "s1",
            "context_data": {"x": "stored"}
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "v")["value"], "stored");
}

#[tokio::test]
async fn draft_stored_sequence_cross_tenant_is_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    let seq = seq_with_blocks(seq_id, "t1", vec![step("s1", json!({}))]);
    create_sequence(&client, &v1, "t1", &seq).await;

    let resp = post_inspect(
        &client,
        &v1,
        Some("intruder"),
        &json!({"sequence_id": seq_id, "block_id": "s1"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn draft_by_value_tenant_mismatch_is_403() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let body = json!({
        "sequence": seq_with_blocks(Uuid::now_v7(), "t1", vec![step("s1", json!({}))]),
        "block_id": "s1"
    });
    let resp = post_inspect(&client, &srv.v1_url(), Some("other-tenant"), &body).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn draft_without_tenant_header_succeeds() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let body = json!({
        "sequence": seq_with_blocks(
            Uuid::now_v7(),
            "t1",
            vec![step("s1", json!({"v": "{{ context.data.x }}"}))]
        ),
        "block_id": "s1",
        "context_data": {"x": 1}
    });
    let resp = post_inspect(&client, &srv.v1_url(), None, &body).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "v")["value"], 1);
}

// =====================================================================
// Instance resolved-input: snapshot + historical boundary
// =====================================================================

/// Spin up a server with the standard pipeline sequence and one instance.
async fn pipeline_instance(
    context: Value,
) -> (orch8_api::test_harness::TestServer, reqwest::Client, Uuid) {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    create_sequence(&client, &v1, "t1", &pipeline_seq(seq_id, "t1")).await;
    let inst = create_instance(&client, &v1, seq_id, "t1", context).await;
    (srv, client, inst)
}

fn fetch_row(inst: Uuid, offset: i64) -> BlockOutput {
    out_at(inst, "fetch", offset, json!({"result": "fetched"}))
}

fn transform_row(inst: Uuid, offset: i64) -> BlockOutput {
    out_at(inst, "transform", offset, json!({"result": "transformed"}))
}

#[tokio::test]
async fn instance_resolves_context_from_snapshot() {
    let (srv, client, inst) =
        pipeline_instance(json!({"data": {"customer": {"email": "real@customer.io"}}})).await;
    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "notify", None, "t1").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "email")["status"], "ok");
    assert_eq!(entry(&trace, "email")["value"], "real@customer.io");
}

#[tokio::test]
async fn instance_reports_missing_when_no_outputs_recorded() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "from_fetch")["status"], "missing");
    assert_eq!(entry(&trace, "from_transform")["status"], "missing");
    assert_eq!(entry(&trace, "self_ref")["status"], "missing");
}

#[tokio::test]
async fn instance_outputs_before_inspected_block_are_visible() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    srv.storage
        .save_block_output(&fetch_row(inst, 0))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&transform_row(inst, 1))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "from_fetch")["value"], "fetched");
    assert_eq!(
        entry(&trace, "from_fetch")["source"],
        "outputs.fetch.result"
    );
    assert_eq!(entry(&trace, "from_transform")["value"], "transformed");
}

#[tokio::test]
async fn instance_outputs_at_and_after_boundary_are_invisible() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    srv.storage
        .save_block_output(&fetch_row(inst, 0))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&out_at(inst, "notify", 1, json!({"result": "self"})))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&out_at(inst, "audit", 2, json!({"result": "audited"})))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    // Before the boundary: visible.
    assert_eq!(entry(&trace, "from_fetch")["status"], "ok");
    // The block's own output and anything after: invisible.
    assert_eq!(entry(&trace, "self_ref")["status"], "missing");
    assert_eq!(entry(&trace, "from_audit")["status"], "missing");
}

#[tokio::test]
async fn instance_own_output_excluded_by_default_boundary() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    srv.storage
        .save_block_output(&out_at(inst, "notify", 0, json!({"result": "me"})))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "self_ref")["status"], "missing");
}

#[tokio::test]
async fn instance_at_block_earlier_narrows_visibility() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    srv.storage
        .save_block_output(&fetch_row(inst, 0))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&transform_row(inst, 1))
        .await
        .unwrap();

    // Boundary moved back to `transform`: only fetch remains visible.
    let resp = get_resolved_input(
        &client,
        &srv.v1_url(),
        inst,
        "notify",
        Some("transform"),
        "t1",
    )
    .await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "from_fetch")["status"], "ok");
    assert_eq!(entry(&trace, "from_transform")["status"], "missing");
}

#[tokio::test]
async fn instance_at_block_later_reveals_own_output() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    srv.storage
        .save_block_output(&fetch_row(inst, 0))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&transform_row(inst, 1))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&out_at(inst, "notify", 2, json!({"result": "self-out"})))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&out_at(inst, "audit", 3, json!({"result": "audited"})))
        .await
        .unwrap();

    // Boundary moved forward to `audit`: notify's own output becomes
    // visible, audit's does not.
    let resp =
        get_resolved_input(&client, &srv.v1_url(), inst, "notify", Some("audit"), "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "self_ref")["value"], "self-out");
    assert_eq!(entry(&trace, "from_transform")["value"], "transformed");
    assert_eq!(entry(&trace, "from_audit")["status"], "missing");
}

#[tokio::test]
async fn instance_at_block_unknown_boundary_shows_everything() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    srv.storage
        .save_block_output(&fetch_row(inst, 0))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&out_at(inst, "notify", 1, json!({"result": "self-out"})))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&out_at(inst, "audit", 2, json!({"result": "audited"})))
        .await
        .unwrap();

    // A boundary block that never produced output never breaks the scan,
    // so every recorded output is visible.
    let resp = get_resolved_input(
        &client,
        &srv.v1_url(),
        inst,
        "notify",
        Some("zzz-never"),
        "t1",
    )
    .await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "from_fetch")["status"], "ok");
    assert_eq!(entry(&trace, "self_ref")["value"], "self-out");
    assert_eq!(entry(&trace, "from_audit")["value"], "audited");
}

#[tokio::test]
async fn instance_boundary_breaks_at_first_occurrence_of_boundary_block() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    // Storage order: fetch, notify, transform — the scan stops at the FIRST
    // notify row, so the later transform output is invisible even though it
    // exists.
    srv.storage
        .save_block_output(&fetch_row(inst, 0))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&out_at(inst, "notify", 1, json!({"result": "early"})))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&transform_row(inst, 2))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "from_fetch")["status"], "ok");
    assert_eq!(entry(&trace, "from_transform")["status"], "missing");
}

#[tokio::test]
async fn instance_last_attempt_wins_before_boundary() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    srv.storage
        .save_block_output(&out_at(inst, "fetch", 0, json!({"result": "attempt-1"})))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&out_at(inst, "fetch", 1, json!({"result": "attempt-2"})))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "from_fetch")["value"], "attempt-2");
}

#[tokio::test]
async fn instance_attempt_after_boundary_does_not_override_earlier_one() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    srv.storage
        .save_block_output(&out_at(inst, "fetch", 0, json!({"result": "before"})))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&out_at(inst, "notify", 1, json!({"result": "boundary"})))
        .await
        .unwrap();
    // A fetch retry recorded after the boundary must not be visible.
    srv.storage
        .save_block_output(&out_at(inst, "fetch", 2, json!({"result": "after"})))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "from_fetch")["value"], "before");
}

// =====================================================================
// Instance resolved-input: sentinel and marker rows
// =====================================================================

#[tokio::test]
async fn instance_underscore_sentinel_rows_are_excluded() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    let seq = seq_with_blocks(
        seq_id,
        "t1",
        vec![
            step("s1", json!({})),
            step("s2", json!({"probe": "{{ outputs._probe.result }}"})),
        ],
    );
    create_sequence(&client, &v1, "t1", &seq).await;
    let inst = create_instance(&client, &v1, seq_id, "t1", json!({"data": {}})).await;

    // An engine bookkeeping row (interceptor/SLA style `_`-prefixed id)
    // exists in storage but must be invisible to resolution.
    srv.storage
        .save_block_output(&out_at(inst, "_probe", 0, json!({"result": "internal"})))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &v1, inst, "s2", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "probe")["status"], "missing");
    assert!(!trace.to_string().contains("internal"));
}

#[tokio::test]
async fn instance_underscore_sentinel_never_acts_as_boundary() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    // Sentinel first, then a real output: the sentinel must be skipped
    // without breaking the scan.
    srv.storage
        .save_block_output(&out_at(inst, "_sla:runtime", 0, json!({"breach": true})))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&fetch_row(inst, 1))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "from_fetch")["value"], "fetched");
}

#[tokio::test]
async fn instance_error_marker_row_is_included_as_an_output() {
    // `__error__` bookkeeping rows keep the REAL block id (the marker
    // lives in output_ref), so the endpoint's `_`-prefix filter does NOT
    // exclude them: the error payload is visible as the block's output.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    let seq = seq_with_blocks(
        seq_id,
        "t1",
        vec![
            step("fetch", json!({})),
            step(
                "notify",
                json!({
                    "result": "{{ outputs.fetch.result }}",
                    "failed": "{{ outputs.fetch.__error__ }}",
                    "why": "{{ outputs.fetch.message }}"
                }),
            ),
        ],
    );
    create_sequence(&client, &v1, "t1", &seq).await;
    let inst = create_instance(&client, &v1, seq_id, "t1", json!({"data": {}})).await;

    srv.storage
        .save_block_output(&marker_at(
            inst,
            "fetch",
            0,
            json!({"__error__": true, "message": "boom"}),
            "__error__",
        ))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &v1, inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "failed")["value"], true);
    assert_eq!(entry(&trace, "why")["value"], "boom");
    assert_eq!(entry(&trace, "result")["status"], "missing");
}

#[tokio::test]
async fn instance_retry_marker_after_real_output_does_not_shadow() {
    // Contract change (deep review 2026-07): `__retry__` rows are internal
    // attempt-counter bookkeeping, NOT outputs — they are now filtered here
    // exactly as `build_outputs_shape` filters them engine-side, so a marker
    // written after a real output no longer shadows it. (Previously
    // last-attempt-wins applied to marker rows and `result` went missing.)
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    srv.storage
        .save_block_output(&fetch_row(inst, 0))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&marker_at(
            inst,
            "fetch",
            1,
            json!({"retrying": true}),
            "__retry__",
        ))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    // The marker is invisible; the real output is what `{{ outputs.* }}` sees.
    assert_eq!(entry(&trace, "from_fetch")["value"], "fetched");
}

#[tokio::test]
async fn instance_real_output_after_retry_marker_wins() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    srv.storage
        .save_block_output(&marker_at(
            inst,
            "fetch",
            0,
            json!({"retrying": true}),
            "__retry__",
        ))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&fetch_row(inst, 1))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "from_fetch")["value"], "fetched");
}

// =====================================================================
// Instance resolved-input: 404s and tenancy
// =====================================================================

#[tokio::test]
async fn instance_cross_tenant_read_is_404() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "notify", None, "intruder").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn instance_unknown_instance_is_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp =
        get_resolved_input(&client, &srv.v1_url(), Uuid::now_v7(), "notify", None, "t1").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn instance_unknown_block_is_404() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "ghost", None, "t1").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn instance_cross_tenant_hides_even_with_at_block() {
    let (srv, client, inst) = pipeline_instance(json!({"data": {}})).await;
    let resp = get_resolved_input(
        &client,
        &srv.v1_url(),
        inst,
        "notify",
        Some("audit"),
        "intruder",
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// =====================================================================
// Instance resolved-input: redaction, config, state, shapes
// =====================================================================

#[tokio::test]
async fn instance_redaction_end_to_end() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    let seq = seq_with_blocks(
        seq_id,
        "t1",
        vec![step(
            "s1",
            json!({
                "api_key": "{{ context.data.secret_value }}",
                "note": "{{ context.data.shaped }}"
            }),
        )],
    );
    create_sequence(&client, &v1, "t1", &seq).await;
    let inst = create_instance(
        &client,
        &v1,
        seq_id,
        "t1",
        json!({"data": {"secret_value": "hunter2", "shaped": "sk_live_instance"}}),
    )
    .await;

    let resp = get_resolved_input(&client, &v1, inst, "s1", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "api_key")["status"], "redacted");
    assert_eq!(entry(&trace, "note")["status"], "redacted");
    assert_eq!(trace["resolved_params"]["api_key"], "[REDACTED]");
    let body = trace.to_string();
    assert!(!body.contains("hunter2"));
    assert!(!body.contains("sk_live_instance"));
}

#[tokio::test]
async fn instance_secret_in_stored_output_is_redacted_over_http() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    let seq = seq_with_blocks(
        seq_id,
        "t1",
        vec![
            step("login", json!({})),
            step("call", json!({"hdr": "{{ outputs.login.token }}"})),
        ],
    );
    create_sequence(&client, &v1, "t1", &seq).await;
    let inst = create_instance(&client, &v1, seq_id, "t1", json!({"data": {}})).await;
    srv.storage
        .save_block_output(&out_at(
            inst,
            "login",
            0,
            json!({"token": "ghp_storedsecret"}),
        ))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &v1, inst, "call", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "hdr")["status"], "redacted");
    assert!(!trace.to_string().contains("ghp_storedsecret"));
}

#[tokio::test]
async fn instance_context_config_resolves_from_instance() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    let seq = seq_with_blocks(
        seq_id,
        "t1",
        vec![step("s1", json!({"region": "{{ context.config.region }}"}))],
    );
    create_sequence(&client, &v1, "t1", &seq).await;
    let inst = create_instance(
        &client,
        &v1,
        seq_id,
        "t1",
        json!({"data": {}, "config": {"region": "us-east-1"}}),
    )
    .await;

    let resp = get_resolved_input(&client, &v1, inst, "s1", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "region")["value"], "us-east-1");
    assert_eq!(entry(&trace, "region")["source"], "context.config.region");
}

#[tokio::test]
async fn instance_state_root_is_missing_without_state_map() {
    // resolved-input does not load the instance KV map: `state.*` paths
    // honestly report missing rather than guessing.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    let seq = seq_with_blocks(
        seq_id,
        "t1",
        vec![step("s1", json!({"cursor": "{{ state.cursor }}"}))],
    );
    create_sequence(&client, &v1, "t1", &seq).await;
    let inst = create_instance(&client, &v1, seq_id, "t1", json!({"data": {}})).await;

    let resp = get_resolved_input(&client, &v1, inst, "s1", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "cursor")["status"], "missing");
}

#[tokio::test]
async fn instance_fallback_to_context_when_output_missing() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    let seq = seq_with_blocks(
        seq_id,
        "t1",
        vec![
            step("fetch", json!({})),
            step(
                "notify",
                json!({"v": "{{ outputs.fetch.result | context.data.fallback }}"}),
            ),
        ],
    );
    create_sequence(&client, &v1, "t1", &seq).await;
    let inst = create_instance(
        &client,
        &v1,
        seq_id,
        "t1",
        json!({"data": {"fallback": "from-ctx"}}),
    )
    .await;

    let resp = get_resolved_input(&client, &v1, inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    let e = entry(&trace, "v");
    assert_eq!(e["value"], "from-ctx");
    assert_eq!(e["source"], "context.data.fallback");
    assert_eq!(e["fallback_used"], true);

    // Once fetch runs, the primary source takes over.
    srv.storage
        .save_block_output(&out_at(inst, "fetch", 0, json!({"result": "primary"})))
        .await
        .unwrap();
    let resp = get_resolved_input(&client, &v1, inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    let e = entry(&trace, "v");
    assert_eq!(e["value"], "primary");
    assert_eq!(e["fallback_used"], false);
}

#[tokio::test]
async fn instance_nested_param_paths_over_http() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    let seq = seq_with_blocks(
        seq_id,
        "t1",
        vec![step(
            "s1",
            json!({
                "req": {"body": {"who": "{{ context.data.name }}"}},
                "tags": ["static", "{{ context.data.tag }}"]
            }),
        )],
    );
    create_sequence(&client, &v1, "t1", &seq).await;
    let inst = create_instance(
        &client,
        &v1,
        seq_id,
        "t1",
        json!({"data": {"name": "ada", "tag": "vip"}}),
    )
    .await;

    let resp = get_resolved_input(&client, &v1, inst, "s1", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "req.body.who")["value"], "ada");
    assert_eq!(entry(&trace, "tags.1")["value"], "vip");
    assert_eq!(
        trace["resolved_params"],
        json!({"req": {"body": {"who": "ada"}}, "tags": ["static", "vip"]})
    );
}

#[tokio::test]
async fn instance_no_template_block_has_empty_entries() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    let seq = seq_with_blocks(
        seq_id,
        "t1",
        vec![step("s1", json!({"fixed": true, "n": 3}))],
    );
    create_sequence(&client, &v1, "t1", &seq).await;
    let inst = create_instance(&client, &v1, seq_id, "t1", json!({"data": {}})).await;

    let resp = get_resolved_input(&client, &v1, inst, "s1", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(trace["entries"].as_array().unwrap().len(), 0);
    assert_eq!(trace["resolved_params"], json!({"fixed": true, "n": 3}));
}

#[tokio::test]
async fn instance_multi_expression_and_filters_over_http() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    let seq = seq_with_blocks(
        seq_id,
        "t1",
        vec![
            step("fetch", json!({})),
            step(
                "notify",
                json!({"msg": "{{ context.data.who | upper }} owes {{ outputs.fetch.amount }}"}),
            ),
        ],
    );
    create_sequence(&client, &v1, "t1", &seq).await;
    let inst = create_instance(&client, &v1, seq_id, "t1", json!({"data": {"who": "bob"}})).await;
    srv.storage
        .save_block_output(&out_at(inst, "fetch", 0, json!({"amount": 12})))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &v1, inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    let es: Vec<&Value> = trace["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["param_path"] == "msg")
        .collect();
    assert_eq!(es.len(), 2);
    assert_eq!(es[0]["value"], "BOB");
    assert_eq!(es[1]["value"], 12);
    assert_eq!(es[1]["coerced_to_string"], true);
    assert_eq!(trace["resolved_params"]["msg"], "BOB owes 12");
}

#[tokio::test]
async fn instance_five_block_pipeline_middle_inspection() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = Uuid::now_v7();
    let params = json!({
        "a": "{{ outputs.b1.result }}",
        "b": "{{ outputs.b2.result }}",
        "d": "{{ outputs.b4.result }}",
        "e": "{{ outputs.b5.result }}"
    });
    let seq = seq_with_blocks(
        seq_id,
        "t1",
        vec![
            step("b1", json!({})),
            step("b2", json!({})),
            step("b3", params),
            step("b4", json!({})),
            step("b5", json!({})),
        ],
    );
    create_sequence(&client, &v1, "t1", &seq).await;
    let inst = create_instance(&client, &v1, seq_id, "t1", json!({"data": {}})).await;
    for (i, b) in ["b1", "b2", "b3", "b4", "b5"].iter().enumerate() {
        srv.storage
            .save_block_output(&out_at(
                inst,
                b,
                i64::try_from(i).unwrap(),
                json!({"result": format!("{b}-out")}),
            ))
            .await
            .unwrap();
    }

    // Default boundary (b3): b1/b2 visible, b4/b5 not.
    let resp = get_resolved_input(&client, &v1, inst, "b3", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "a")["value"], "b1-out");
    assert_eq!(entry(&trace, "b")["value"], "b2-out");
    assert_eq!(entry(&trace, "d")["status"], "missing");
    assert_eq!(entry(&trace, "e")["status"], "missing");

    // at_block=b5: everything before b5 visible.
    let resp = get_resolved_input(&client, &v1, inst, "b3", Some("b5"), "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "d")["value"], "b4-out");
    assert_eq!(entry(&trace, "e")["status"], "missing");

    // at_block=b2: only b1 visible.
    let resp = get_resolved_input(&client, &v1, inst, "b3", Some("b2"), "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "a")["value"], "b1-out");
    assert_eq!(entry(&trace, "b")["status"], "missing");
}

#[tokio::test]
async fn instance_block_id_echoed_and_statuses_serialized_snake_case() {
    let (srv, client, inst) =
        pipeline_instance(json!({"data": {"customer": {"email": "e@x.io"}}})).await;
    srv.storage
        .save_block_output(&fetch_row(inst, 0))
        .await
        .unwrap();

    let resp = get_resolved_input(&client, &srv.v1_url(), inst, "notify", None, "t1").await;
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(trace["block_id"], "notify");
    let statuses: Vec<&str> = trace["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["status"].as_str().unwrap())
        .collect();
    assert!(statuses.contains(&"ok"));
    assert!(statuses.contains(&"missing"));
}
