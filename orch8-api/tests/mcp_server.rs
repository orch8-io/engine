//! E2E tests for the MCP server endpoint (`POST /api/v1/mcp`).
//!
//! Drives the JSON-RPC 2.0 surface over plain reqwest: handshake, tool
//! catalog, tools/call round-trips, the domain-vs-protocol error split, and
//! tenant isolation.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::InstanceStore;
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

/// POST one JSON-RPC message; return (status, parsed body or Null when empty).
async fn rpc(
    client: &reqwest::Client,
    url: &str,
    tenant: &str,
    body: &Value,
) -> (StatusCode, Value) {
    let resp = client
        .post(format!("{url}/mcp"))
        .header("X-Tenant-Id", tenant)
        .json(body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.bytes().await.unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

/// Call an MCP tool, asserting the JSON-RPC layer succeeded. Returns the
/// `tools/call` result envelope (`{content, isError}`).
async fn call_tool(
    client: &reqwest::Client,
    url: &str,
    tenant: &str,
    tool: &str,
    arguments: Value,
) -> Value {
    let (status, body) = rpc(
        client,
        url,
        tenant,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": { "name": tool, "arguments": arguments },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.get("error").is_none(),
        "unexpected JSON-RPC error: {body}"
    );
    body["result"].clone()
}

/// Parse the text content of a tool result back into JSON.
fn tool_json(result: &Value) -> Value {
    assert_eq!(result["isError"], false, "tool errored: {result}");
    let text = result["content"][0]["text"].as_str().unwrap();
    serde_json::from_str(text).unwrap()
}

fn mk_sequence_body(id: Uuid, tenant: &str, name: &str) -> Value {
    json!({
        "id": id,
        "tenant_id": tenant,
        "namespace": "default",
        "name": name,
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "s1",
                "handler": "noop",
                "params": {},
                "cancellable": true
            }
        ],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
    })
}

/// Create a sequence via REST (the MCP tools are then exercised against it).
async fn create_sequence(client: &reqwest::Client, v1: &str, tenant: &str, name: &str) -> Uuid {
    let seq_id = Uuid::now_v7();
    let resp = client
        .post(format!("{v1}/sequences"))
        .header("X-Tenant-Id", tenant)
        .json(&mk_sequence_body(seq_id, tenant, name))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    seq_id
}

/// Create an instance through the MCP tool; returns its id.
async fn mcp_create_instance(
    client: &reqwest::Client,
    v1: &str,
    tenant: &str,
    args: Value,
) -> Uuid {
    let result = call_tool(client, v1, tenant, "create_instance", args).await;
    let created = tool_json(&result);
    created["id"].as_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn initialize_handshake_and_notification() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();

    let (status, body) = rpc(
        &client,
        &v1,
        "t1",
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "0" }
            }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], 1);
    assert_eq!(body["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(body["result"]["serverInfo"]["name"], "orch8");
    assert!(body["result"]["serverInfo"]["version"].is_string());
    assert!(body["result"]["capabilities"]["tools"].is_object());

    // An unknown protocol version falls back to the server default.
    let (_, body) = rpc(
        &client,
        &v1,
        "t1",
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "initialize",
            "params": { "protocolVersion": "1999-01-01" }
        }),
    )
    .await;
    assert_eq!(body["result"]["protocolVersion"], "2025-06-18");

    // Notifications (no id) get 202 with an empty body.
    let (status, body) = rpc(
        &client,
        &v1,
        "t1",
        &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body, Value::Null);
}

#[tokio::test]
async fn tools_list_returns_all_eight_tools_with_schemas() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let (status, body) = rpc(
        &client,
        &srv.v1_url(),
        "t1",
        &json!({ "jsonrpc": "2.0", "id": 7, "method": "tools/list" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let tools = body["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 8);

    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    for expected in [
        "list_sequences",
        "create_instance",
        "get_instance_status",
        "get_instance_outputs",
        "send_signal",
        "retry_instance",
        "list_dlq",
        "get_usage",
    ] {
        assert!(names.contains(&expected), "missing tool {expected}");
    }
    for tool in tools {
        let schema = &tool["inputSchema"];
        assert_eq!(schema["type"], "object", "bad schema for {}", tool["name"]);
        assert!(schema["properties"].is_object());
    }
}

#[tokio::test]
async fn create_instance_and_get_status_round_trip() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let seq_id = create_sequence(&client, &v1, "t1", "round-trip-seq").await;

    // list_sequences sees the REST-created sequence.
    let result = call_tool(&client, &v1, "t1", "list_sequences", json!({})).await;
    let listed = tool_json(&result);
    let items = listed["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "round-trip-seq");

    // Launch by id, then by name — both paths must resolve.
    let inst_id = mcp_create_instance(
        &client,
        &v1,
        "t1",
        json!({ "sequence_id": seq_id, "metadata": { "source": "mcp" } }),
    )
    .await;
    let by_name = mcp_create_instance(
        &client,
        &v1,
        "t1",
        json!({ "sequence_name": "round-trip-seq" }),
    )
    .await;
    assert_ne!(inst_id, by_name);

    // Status of the first instance via MCP.
    let result = call_tool(
        &client,
        &v1,
        "t1",
        "get_instance_status",
        json!({ "instance_id": inst_id }),
    )
    .await;
    let status = tool_json(&result);
    assert_eq!(status["id"], inst_id.to_string());
    assert_eq!(status["state"], "scheduled");
    assert_eq!(status["sequence_id"], seq_id.to_string());
    assert_eq!(status["metadata"]["source"], "mcp");
    assert!(status["created_at"].is_string());
    assert!(status["updated_at"].is_string());

    // Outputs exist (empty — nothing has executed) and don't error.
    let result = call_tool(
        &client,
        &v1,
        "t1",
        "get_instance_outputs",
        json!({ "instance_id": inst_id }),
    )
    .await;
    let outputs = tool_json(&result);
    assert_eq!(outputs, json!([]));
}

#[tokio::test]
async fn send_signal_and_retry_instance_happy_paths() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    create_sequence(&client, &v1, "t1", "signal-seq").await;
    let inst_id =
        mcp_create_instance(&client, &v1, "t1", json!({ "sequence_name": "signal-seq" })).await;

    // send_signal: pause the scheduled instance.
    let result = call_tool(
        &client,
        &v1,
        "t1",
        "send_signal",
        json!({ "instance_id": inst_id, "signal": "pause", "payload": { "by": "test" } }),
    )
    .await;
    let signal = tool_json(&result);
    assert!(signal["signal_id"].is_string());

    // retry_instance requires a Failed instance — force the state directly
    // through the storage handle, like sibling tests do.
    srv.storage
        .update_instance_state(InstanceId::from_uuid(inst_id), InstanceState::Failed, None)
        .await
        .unwrap();

    // The failed instance shows up in the DLQ via MCP.
    let result = call_tool(&client, &v1, "t1", "list_dlq", json!({ "limit": 10 })).await;
    let dlq = tool_json(&result);
    let rows = dlq.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], inst_id.to_string());

    // retry resets it to scheduled.
    let result = call_tool(
        &client,
        &v1,
        "t1",
        "retry_instance",
        json!({ "instance_id": inst_id }),
    )
    .await;
    let retried = tool_json(&result);
    assert_eq!(retried["state"], "scheduled");

    let result = call_tool(
        &client,
        &v1,
        "t1",
        "get_instance_status",
        json!({ "instance_id": inst_id }),
    )
    .await;
    assert_eq!(tool_json(&result)["state"], "scheduled");
}

#[tokio::test]
async fn retry_non_failed_instance_is_domain_error() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    create_sequence(&client, &v1, "t1", "retry-seq").await;
    let inst_id =
        mcp_create_instance(&client, &v1, "t1", json!({ "sequence_name": "retry-seq" })).await;

    // Scheduled (not Failed) → invalid-state domain error, isError: true.
    let result = call_tool(
        &client,
        &v1,
        "t1",
        "retry_instance",
        json!({ "instance_id": inst_id }),
    )
    .await;
    assert_eq!(result["isError"], true);
    let text = result["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("can only retry failed instances"), "{text}");
}

#[tokio::test]
async fn get_usage_via_mcp() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let result = call_tool(
        &client,
        &srv.v1_url(),
        "t1",
        "get_usage",
        json!({ "start": "2026-01-01T00:00:00Z", "end": "2026-02-01T00:00:00Z" }),
    )
    .await;
    let usage = tool_json(&result);
    assert_eq!(usage["tenant"], "t1");
    assert!(usage["usage"].as_array().unwrap().is_empty());
    assert_eq!(usage["start"], "2026-01-01T00:00:00Z");
}

#[tokio::test]
async fn domain_error_unknown_instance_is_is_error_true() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let result = call_tool(
        &client,
        &srv.v1_url(),
        "t1",
        "get_instance_status",
        json!({ "instance_id": Uuid::now_v7() }),
    )
    .await;
    assert_eq!(result["isError"], true);
    let text = result["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("not found"), "{text}");
}

#[tokio::test]
async fn unknown_method_returns_32601() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let (status, body) = rpc(
        &client,
        &srv.v1_url(),
        "t1",
        &json!({ "jsonrpc": "2.0", "id": 3, "method": "resources/list" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["error"]["code"], -32601);
    assert_eq!(body["id"], 3);
}

#[tokio::test]
async fn malformed_json_returns_32700() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/mcp", srv.v1_url()))
        .header("Content-Type", "application/json")
        .header("X-Tenant-Id", "t1")
        .body("{ not json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], -32700);
    assert_eq!(body["id"], Value::Null);
}

#[tokio::test]
async fn invalid_jsonrpc_envelope_returns_32600() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Batch arrays are not supported in v1.
    let (_, body) = rpc(&client, &srv.v1_url(), "t1", &json!([])).await;
    assert_eq!(body["error"]["code"], -32600);

    // Wrong jsonrpc version marker.
    let (_, body) = rpc(
        &client,
        &srv.v1_url(),
        "t1",
        &json!({ "jsonrpc": "1.0", "id": 1, "method": "tools/list" }),
    )
    .await;
    assert_eq!(body["error"]["code"], -32600);
}

#[tokio::test]
async fn unknown_tool_returns_32602() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Documented choice: an unknown tool name is a protocol error (-32602),
    // not an isError result — the catalog is static.
    let (status, body) = rpc(
        &client,
        &srv.v1_url(),
        "t1",
        &json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": { "name": "explode_workflow", "arguments": {} },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["error"]["code"], -32602);
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unknown tool")
    );
}

#[tokio::test]
async fn tenant_isolation_hides_foreign_instances() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    create_sequence(&client, &v1, "t1", "isolated-seq").await;
    let inst_id = mcp_create_instance(
        &client,
        &v1,
        "t1",
        json!({ "sequence_name": "isolated-seq" }),
    )
    .await;

    // Another tenant cannot see t1's instance (404 → domain error).
    let result = call_tool(
        &client,
        &v1,
        "t2",
        "get_instance_status",
        json!({ "instance_id": inst_id }),
    )
    .await;
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("not found")
    );

    // Nor signal it.
    let result = call_tool(
        &client,
        &v1,
        "t2",
        "send_signal",
        json!({ "instance_id": inst_id, "signal": "cancel" }),
    )
    .await;
    assert_eq!(result["isError"], true);

    // list_sequences for t2 is empty — sequences are tenant-scoped too.
    let result = call_tool(&client, &v1, "t2", "list_sequences", json!({})).await;
    let listed = tool_json(&result);
    assert!(listed["items"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn create_instance_passes_budget_first_class() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let v1 = srv.v1_url();
    let tenant = "tb";

    let seq_id = create_sequence(&client, &v1, tenant, "budgeted").await;
    let inst_id = mcp_create_instance(
        &client,
        &v1,
        tenant,
        json!({
            "sequence_id": seq_id,
            "budget": { "max_total_tokens": 5000, "max_steps": 10 }
        }),
    )
    .await;

    // The budget must land on the instance itself (scheduler-enforced),
    // not inside context.config.
    let resp = client
        .get(format!("{v1}/instances/{inst_id}"))
        .header("X-Tenant-Id", tenant)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let inst: Value = resp.json().await.unwrap();
    assert_eq!(inst["budget"]["max_total_tokens"], 5000);
    assert_eq!(inst["budget"]["max_steps"], 10);
    assert!(inst["context"]["config"].get("budget").is_none());
}
