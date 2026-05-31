//! Built-in `agent` handler — a native, durable `ReAct` loop.
//!
//! Where `react-loop.json` expresses Reason → Act → Observe as a hand-authored
//! `Loop` + `llm_call` + `tool_call` sequence, this handler ships the loop as a
//! single first-class primitive: give it a goal, a tool set, and a tool
//! dispatch target, and it drives the conversation until the model stops
//! requesting tools (or an iteration budget is hit).
//!
//! It reuses the engine's existing primitives rather than reimplementing them:
//! the model call goes through [`super::llm::handle_llm_call`] (10 providers +
//! failover), and each tool invocation goes through [`super::tool_call`] (HTTP)
//! or [`super::mcp`] (Model Context Protocol). Because those are the same
//! handlers used everywhere else, an agent inherits provider failover,
//! SSRF guards, and MCP durability for free.
//!
//! ## Durability
//!
//! After every completed iteration the loop checkpoints its progress
//! (conversation + iteration counter) to the instance KV, keyed by block id.
//! If the step crashes or is retried, the loop **resumes from the last
//! completed iteration** rather than re-running (and re-paying for) earlier
//! turns — only the in-flight iteration is redone. (Per-iteration engine-level
//! retry/circuit-breaker — i.e. desugaring into one durable step per turn — is
//! the block-level follow-on.)
//!
//! ## Params
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `goal` | string | — | Shorthand for a single initial user message. |
//! | `messages` | array | `[]` | Initial conversation (overrides `goal` if both given). |
//! | `system` | string | — | System prompt. |
//! | `tools` | array | — | LLM tool/function schema. Auto-discovered via MCP `tools/list` when omitted and `tool_dispatch.type == "mcp"`. |
//! | `max_iterations` | u64 | `6` | Hard cap on reason→act cycles (clamped to `MAX_ITERATIONS_CEILING`). |
//! | `tool_dispatch` | object | — | How to execute tool calls (see below). Required if `tools` is set. |
//! | `auto_memory` | object | — | Recall before / store after via the memory handlers. `{ recall_k?, store_outcome?, base_url?, api_key?, api_key_env?, model? }`. |
//! | `provider` / `providers` / `model` / `api_key` / `api_key_env` / `base_url` / `temperature` / `max_tokens` | — | Forwarded verbatim to `llm_call`. |
//!
//! `tool_dispatch` is `{ "type": "http", "url": ..., "headers": {...} }` (each
//! tool call becomes a `tool_call`) or `{ "type": "mcp", "url"|"server": ..., "headers": {...} }`
//! (each tool call becomes an `mcp_call`).
//!
//! ## Result
//!
//! `{ "final": <assistant text | null>, "iterations": N, "stop_reason":
//! "completed" | "max_iterations", "tool_calls_made": M, "messages": [...] }`

use serde_json::{json, Value};
use tracing::debug;

use orch8_types::error::StepError;

use super::StepContext;

/// Default reason→act cycle budget when the caller does not set one.
const DEFAULT_MAX_ITERATIONS: u64 = 6;
/// Absolute ceiling on iterations regardless of caller request — a runaway
/// agent must not spin forever inside a single step.
const MAX_ITERATIONS_CEILING: u64 = 50;

/// LLM-config keys forwarded verbatim from the agent params into each
/// `llm_call`. `messages`, `system`, and `tools` are handled separately.
const LLM_PASSTHROUGH_KEYS: &[&str] = &[
    "provider",
    "providers",
    "model",
    "api_key",
    "api_key_env",
    "base_url",
    "temperature",
    "max_tokens",
    "total_timeout_secs",
    "per_provider_timeout_secs",
];

/// A tool call the model requested, normalized from the `llm_call` output.
#[derive(Debug, Clone, PartialEq)]
struct ToolCall {
    id: String,
    name: String,
    arguments: Value,
}

pub async fn handle_agent(ctx: StepContext) -> Result<Value, StepError> {
    // Dry-run: do not run the reason→act→observe loop (which would call the
    // LLM and dispatch tools). Return the loop's terminal shape with no
    // iterations so downstream templates still resolve.
    if ctx.is_dry_run() {
        return Ok(super::util::dry_run_stub(
            "agent",
            Value::Null,
            json!({
                "final": Value::Null,
                "iterations": 0,
                "stop_reason": "dry_run",
                "tool_calls_made": 0,
                "messages": [],
            }),
        ));
    }

    let max_iterations = ctx
        .params
        .get("max_iterations")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_MAX_ITERATIONS)
        .clamp(1, MAX_ITERATIONS_CEILING);

    // Effective params start as the step params, then get (1) auto-discovered
    // tools and (2) recalled memories layered in.
    let mut effective_params = maybe_discover_tools(&ctx).await?;
    effective_params = maybe_recall_memory(&ctx, effective_params).await?;

    // Closures wire the real handlers. They clone the step context and swap in
    // freshly-built params, so sub-calls inherit tenant/instance identity and
    // the storage handle.
    let llm_ctx = ctx.clone();
    let call_llm = move |llm_params: Value| {
        let mut sub = llm_ctx.clone();
        sub.params = llm_params;
        async move { super::llm::handle_llm_call(sub).await }
    };

    let tool_ctx = ctx.clone();
    let dispatch_cfg = effective_params.get("tool_dispatch").cloned();
    let dispatch_tool = move |call: ToolCall| {
        let mut sub = tool_ctx.clone();
        let cfg = dispatch_cfg.clone();
        async move {
            let (kind, params) = build_tool_dispatch(cfg.as_ref(), &call)?;
            sub.params = params;
            match kind {
                ToolKind::Http => super::tool_call::handle_tool_call(sub).await,
                ToolKind::Mcp => super::mcp::handle_mcp_call(sub).await,
            }
        }
    };

    // Durable checkpoint: progress (messages + completed iterations) is
    // persisted to the instance KV after each iteration, keyed by block id.
    // On crash/retry the loop resumes from the last completed iteration
    // instead of re-running (and re-paying for) earlier turns.
    let cp_key = format!("__agent__:{}", ctx.block_id.as_str());
    let (load_storage, load_iid, load_key) = (ctx.storage.clone(), ctx.instance_id, cp_key.clone());
    let load_checkpoint = move || {
        let (s, k) = (load_storage.clone(), load_key.clone());
        async move { s.get_instance_kv(load_iid, &k).await.ok().flatten() }
    };
    let (save_storage, save_iid) = (ctx.storage.clone(), ctx.instance_id);
    let save_checkpoint = move |cp: Value| {
        let (s, k) = (save_storage.clone(), cp_key.clone());
        async move {
            let _ = s.set_instance_kv(save_iid, &k, &cp).await;
        }
    };

    let outcome = run_agent_loop(
        &effective_params,
        max_iterations,
        call_llm,
        dispatch_tool,
        load_checkpoint,
        save_checkpoint,
    )
    .await?;

    // Persist the final answer to memory if auto_memory.store_outcome is on.
    maybe_store_memory(&ctx, &effective_params, &outcome).await;

    Ok(outcome)
}

/// Which sub-handler executes a tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolKind {
    Http,
    Mcp,
}

/// The reason→act→observe driver, generic over how the model and tools are
/// invoked (and how progress is checkpointed) so it can be unit-tested with
/// deterministic closures and an in-memory checkpoint — no network or storage.
async fn run_agent_loop<L, LFut, T, TFut, LD, LDFut, SV, SVFut>(
    agent_params: &Value,
    max_iterations: u64,
    call_llm: L,
    dispatch_tool: T,
    load_checkpoint: LD,
    save_checkpoint: SV,
) -> Result<Value, StepError>
where
    L: Fn(Value) -> LFut,
    LFut: std::future::Future<Output = Result<Value, StepError>>,
    T: Fn(ToolCall) -> TFut,
    TFut: std::future::Future<Output = Result<Value, StepError>>,
    LD: Fn() -> LDFut,
    LDFut: std::future::Future<Output = Option<Value>>,
    SV: Fn(Value) -> SVFut,
    SVFut: std::future::Future<Output = ()>,
{
    let (mut messages, start_iteration, mut tool_calls_made) =
        restore_checkpoint(load_checkpoint().await, agent_params);

    for iteration in start_iteration..max_iterations {
        let llm_params = build_llm_params(agent_params, &messages);
        let output = call_llm(llm_params).await?;

        let assistant = assistant_message(&output);
        messages.push(assistant.clone());

        let calls = extract_tool_calls(&assistant);
        if calls.is_empty() {
            debug!(
                iteration,
                "agent: model produced no tool calls — completing"
            );
            return Ok(result(
                final_text(&assistant),
                iteration + 1,
                "completed",
                tool_calls_made,
                messages,
            ));
        }

        for call in calls {
            tool_calls_made += 1;
            let observation = match dispatch_tool(call.clone()).await {
                Ok(v) => v,
                // A tool error becomes an observation so the model can react
                // and self-correct rather than failing the whole agent.
                Err(e) => json!({ "error": step_error_message(&e) }),
            };
            messages.push(tool_result_message(&call.id, &observation));
        }

        // Iteration fully done → checkpoint so a retry resumes after it.
        save_checkpoint(make_checkpoint(&messages, iteration + 1, tool_calls_made)).await;
    }

    debug!(max_iterations, "agent: iteration budget exhausted");
    Ok(result(
        None,
        max_iterations,
        "max_iterations",
        tool_calls_made,
        messages,
    ))
}

/// Serialize loop progress into a checkpoint value.
fn make_checkpoint(messages: &[Value], iteration: u64, tool_calls_made: u64) -> Value {
    json!({
        "messages": messages,
        "iteration": iteration,
        "tool_calls_made": tool_calls_made,
    })
}

/// Restore `(messages, start_iteration, tool_calls_made)` from a checkpoint, or
/// start fresh from the agent params when there is no valid checkpoint.
fn restore_checkpoint(checkpoint: Option<Value>, agent_params: &Value) -> (Vec<Value>, u64, u64) {
    if let Some(cp) = checkpoint {
        if let Some(messages) = cp.get("messages").and_then(Value::as_array) {
            let iteration = cp.get("iteration").and_then(Value::as_u64).unwrap_or(0);
            let tool_calls_made = cp
                .get("tool_calls_made")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            return (messages.clone(), iteration, tool_calls_made);
        }
    }
    (initial_messages(agent_params), 0, 0)
}

// ---- Tool auto-discovery (MCP `tools/list`) --------------------------------

/// If the agent has no `tools` schema but dispatches via MCP, discover the
/// server's tools (`tools/list`) and inject them as the LLM tool schema.
async fn maybe_discover_tools(ctx: &StepContext) -> Result<Value, StepError> {
    if ctx.params.get("tools").is_some() {
        return Ok(ctx.params.clone());
    }
    let Some(cfg) = ctx.params.get("tool_dispatch") else {
        return Ok(ctx.params.clone());
    };
    if cfg.get("type").and_then(Value::as_str) != Some("mcp") {
        return Ok(ctx.params.clone());
    }

    let mut list_params = serde_json::Map::new();
    list_params.insert("action".into(), json!("list"));
    for key in ["url", "server", "headers"] {
        if let Some(v) = cfg.get(key) {
            list_params.insert(key.to_string(), v.clone());
        }
    }
    let mut sub = ctx.clone();
    sub.params = Value::Object(list_params);
    let listed = super::mcp::handle_mcp_call(sub).await?;

    let tools = mcp_tools_to_llm_schema(listed.get("tools").unwrap_or(&Value::Null));
    let mut params = ctx.params.clone();
    if let Value::Object(map) = &mut params {
        map.insert("tools".into(), tools);
    }
    Ok(params)
}

/// Convert MCP `tools/list` entries (`{name, description, inputSchema}`) into
/// the `OpenAI` tool schema (`{type:function, function:{name, description, parameters}}`).
fn mcp_tools_to_llm_schema(mcp_tools: &Value) -> Value {
    let Some(arr) = mcp_tools.as_array() else {
        return json!([]);
    };
    let converted: Vec<Value> = arr
        .iter()
        .filter_map(|t| {
            let name = t.get("name").and_then(Value::as_str)?;
            let mut function = serde_json::Map::new();
            function.insert("name".into(), json!(name));
            if let Some(desc) = t.get("description") {
                function.insert("description".into(), desc.clone());
            }
            function.insert(
                "parameters".into(),
                t.get("inputSchema").cloned().unwrap_or_else(|| json!({})),
            );
            Some(json!({ "type": "function", "function": function }))
        })
        .collect();
    json!(converted)
}

// ---- Auto-memory (recall before, store after) -----------------------------

/// If `auto_memory` is set, semantically recall prior memories for the goal and
/// prepend them as a system message to the conversation.
async fn maybe_recall_memory(ctx: &StepContext, params: Value) -> Result<Value, StepError> {
    let Some(auto) = params.get("auto_memory").cloned() else {
        return Ok(params);
    };
    let Some(query) = auto_memory_query(&params) else {
        return Ok(params);
    };

    let mut search_params = embed_config(&auto);
    search_params.insert("query".into(), json!(query));
    if let Some(k) = auto.get("recall_k") {
        search_params.insert("top_k".into(), k.clone());
    }
    let mut sub = ctx.clone();
    sub.params = Value::Object(search_params);
    let found = super::memory::handle_memory_search(sub).await?;

    let Some(system_msg) = memory_recall_system_message(found.get("results")) else {
        return Ok(params);
    };

    // Prepend the recalled-memory system message to the conversation.
    let mut params = params;
    let mut messages = vec![system_msg];
    messages.extend(initial_messages(&params));
    if let Value::Object(map) = &mut params {
        map.insert("messages".into(), json!(messages));
    }
    Ok(params)
}

/// Persist the agent's final answer to memory (best-effort) when
/// `auto_memory.store_outcome` is enabled (default true).
async fn maybe_store_memory(ctx: &StepContext, params: &Value, outcome: &Value) {
    let Some(auto) = params.get("auto_memory") else {
        return;
    };
    let store = auto
        .get("store_outcome")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    if !store {
        return;
    }
    let Some(final_text) = outcome.get("final").and_then(Value::as_str) else {
        return;
    };

    let mut store_params = embed_config(auto);
    store_params.insert("text".into(), json!(final_text));
    let mut sub = ctx.clone();
    sub.params = Value::Object(store_params);
    // Best-effort: a memory write failure must not fail the agent.
    let _ = super::memory::handle_memory_store(sub).await;
}

/// Extract the embedding-provider config (`base_url`/`api_key`/`api_key_env`/`model`)
/// from an `auto_memory` object into a fresh params map.
fn embed_config(auto: &Value) -> serde_json::Map<String, Value> {
    let mut out = serde_json::Map::new();
    for key in ["base_url", "api_key", "api_key_env", "model"] {
        if let Some(v) = auto.get(key) {
            out.insert(key.to_string(), v.clone());
        }
    }
    out
}

/// The text to recall against: the explicit `goal`, else the last user message.
fn auto_memory_query(params: &Value) -> Option<String> {
    if let Some(goal) = params.get("goal").and_then(Value::as_str) {
        return Some(goal.to_string());
    }
    params
        .get("messages")
        .and_then(Value::as_array)
        .and_then(|msgs| {
            msgs.iter()
                .rev()
                .find(|m| m.get("role").and_then(Value::as_str) == Some("user"))
        })
        .and_then(|m| m.get("content").and_then(Value::as_str))
        .map(str::to_string)
}

/// Build a system message summarizing recalled memories, or `None` if empty.
fn memory_recall_system_message(results: Option<&Value>) -> Option<Value> {
    let arr = results.and_then(Value::as_array)?;
    if arr.is_empty() {
        return None;
    }
    let lines: Vec<String> = arr
        .iter()
        .filter_map(|r| r.get("text").and_then(Value::as_str))
        .map(|t| format!("- {t}"))
        .collect();
    if lines.is_empty() {
        return None;
    }
    Some(json!({
        "role": "system",
        "content": format!("Relevant memory from earlier:\n{}", lines.join("\n")),
    }))
}

/// Seed the conversation from `messages` (preferred) or `goal` (shorthand).
fn initial_messages(agent_params: &Value) -> Vec<Value> {
    if let Some(arr) = agent_params.get("messages").and_then(Value::as_array) {
        if !arr.is_empty() {
            return arr.clone();
        }
    }
    if let Some(goal) = agent_params.get("goal").and_then(Value::as_str) {
        return vec![json!({ "role": "user", "content": goal })];
    }
    Vec::new()
}

/// Build the params for one `llm_call`: forward the LLM config, then set the
/// running conversation, system prompt, and tool schema.
fn build_llm_params(agent_params: &Value, messages: &[Value]) -> Value {
    let mut params = serde_json::Map::new();
    for key in LLM_PASSTHROUGH_KEYS {
        if let Some(v) = agent_params.get(*key) {
            params.insert((*key).to_string(), v.clone());
        }
    }
    params.insert("messages".into(), json!(messages));
    if let Some(system) = agent_params.get("system") {
        params.insert("system".into(), system.clone());
    }
    if let Some(tools) = agent_params.get("tools") {
        params.insert("tools".into(), tools.clone());
    }
    Value::Object(params)
}

/// Pull the assistant `message` object out of the normalized `llm_call`
/// output. Falls back to an empty assistant message if absent.
fn assistant_message(llm_output: &Value) -> Value {
    llm_output
        .get("message")
        .cloned()
        .unwrap_or_else(|| json!({ "role": "assistant", "content": "" }))
}

/// Normalize `message.tool_calls` (`OpenAI` shape, also produced for Anthropic)
/// into [`ToolCall`]s. `function.arguments` is a JSON string; a parse failure
/// yields `Value::Null` arguments so the tool can report the problem rather
/// than the loop crashing.
fn extract_tool_calls(assistant: &Value) -> Vec<ToolCall> {
    let Some(arr) = assistant.get("tool_calls").and_then(Value::as_array) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|tc| {
            let function = tc.get("function")?;
            let name = function.get("name").and_then(Value::as_str)?.to_string();
            let id = tc
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let arguments = match function.get("arguments") {
                Some(Value::String(s)) => serde_json::from_str(s).unwrap_or(Value::Null),
                Some(other) => other.clone(),
                None => json!({}),
            };
            Some(ToolCall {
                id,
                name,
                arguments,
            })
        })
        .collect()
}

/// Build the `(handler, params)` for executing one tool call, from the
/// `tool_dispatch` config.
fn build_tool_dispatch(
    cfg: Option<&Value>,
    call: &ToolCall,
) -> Result<(ToolKind, Value), StepError> {
    let cfg = cfg.ok_or_else(|| StepError::Permanent {
        message: format!(
            "agent: model requested tool {:?} but no `tool_dispatch` is configured",
            call.name
        ),
        details: None,
    })?;

    let kind = cfg.get("type").and_then(Value::as_str).unwrap_or("http");
    let url = cfg
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| StepError::Permanent {
            message: "agent: tool_dispatch is missing `url`".into(),
            details: None,
        })?;
    let headers = cfg.get("headers").cloned();

    match kind {
        "http" => {
            let mut params = json!({
                "url": url,
                "tool_name": call.name,
                "arguments": call.arguments,
            });
            if let Some(h) = headers {
                params["headers"] = h;
            }
            Ok((ToolKind::Http, params))
        }
        "mcp" => {
            let mut params = json!({
                "url": url,
                "action": "call",
                "tool_name": call.name,
                "arguments": call.arguments,
            });
            if let Some(h) = headers {
                params["headers"] = h;
            }
            Ok((ToolKind::Mcp, params))
        }
        other => Err(StepError::Permanent {
            message: format!("agent: unknown tool_dispatch type {other:?} (expected http or mcp)"),
            details: None,
        }),
    }
}

/// Build the `role:"tool"` observation message for the conversation.
fn tool_result_message(tool_call_id: &str, observation: &Value) -> Value {
    json!({
        "role": "tool",
        "tool_call_id": tool_call_id,
        "content": serde_json::to_string(observation).unwrap_or_else(|_| "null".into()),
    })
}

/// Extract the final assistant text, if any (`null` when the turn was purely
/// tool calls with no text content).
fn final_text(assistant: &Value) -> Option<String> {
    assistant
        .get("content")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Assemble the handler's output value.
fn result(
    final_text: Option<String>,
    iterations: u64,
    stop_reason: &str,
    tool_calls_made: u64,
    messages: Vec<Value>,
) -> Value {
    // Build via an owned map so `final_text` and `messages` are moved in
    // (the `json!` macro would only borrow them).
    let mut m = serde_json::Map::new();
    m.insert(
        "final".into(),
        final_text.map_or(Value::Null, Value::String),
    );
    m.insert("iterations".into(), json!(iterations));
    m.insert("stop_reason".into(), json!(stop_reason));
    m.insert("tool_calls_made".into(), json!(tool_calls_made));
    m.insert("messages".into(), Value::Array(messages));
    Value::Object(m)
}

fn step_error_message(e: &StepError) -> String {
    match e {
        StepError::Permanent { message, .. } | StepError::Retryable { message, .. } => {
            message.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn assistant_text(text: &str) -> Value {
        json!({ "message": { "role": "assistant", "content": text } })
    }

    fn assistant_tool_call(id: &str, name: &str, args_json: &str) -> Value {
        json!({
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [
                    { "id": id, "type": "function", "function": { "name": name, "arguments": args_json } }
                ]
            }
        })
    }

    #[test]
    fn initial_messages_from_goal() {
        let msgs = initial_messages(&json!({ "goal": "find the weather" }));
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["role"], "user");
        assert_eq!(msgs[0]["content"], "find the weather");
    }

    #[test]
    fn initial_messages_prefers_explicit_messages() {
        let msgs = initial_messages(&json!({
            "goal": "ignored",
            "messages": [{ "role": "user", "content": "real" }]
        }));
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["content"], "real");
    }

    #[test]
    fn initial_messages_empty_when_neither_present() {
        assert!(initial_messages(&json!({})).is_empty());
    }

    #[test]
    fn build_llm_params_forwards_config_and_sets_conversation() {
        let agent = json!({
            "provider": "openai",
            "model": "gpt-4o",
            "api_key": "k",
            "temperature": 0.2,
            "system": "you are helpful",
            "tools": [{ "type": "function", "function": { "name": "t" } }],
            "goal": "hi"
        });
        let messages = vec![json!({ "role": "user", "content": "hi" })];
        let params = build_llm_params(&agent, &messages);
        assert_eq!(params["provider"], "openai");
        assert_eq!(params["model"], "gpt-4o");
        assert_eq!(params["api_key"], "k");
        assert_eq!(params["temperature"], 0.2);
        assert_eq!(params["system"], "you are helpful");
        assert_eq!(params["messages"][0]["content"], "hi");
        assert_eq!(params["tools"][0]["function"]["name"], "t");
        // `goal` is not an LLM param and must not leak through.
        assert!(params.get("goal").is_none());
    }

    #[test]
    fn extract_tool_calls_parses_arguments_json_string() {
        let assistant = assistant_tool_call("tc1", "search", r#"{"q":"rust"}"#)["message"].clone();
        let calls = extract_tool_calls(&assistant);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "tc1");
        assert_eq!(calls[0].name, "search");
        assert_eq!(calls[0].arguments, json!({ "q": "rust" }));
    }

    #[test]
    fn extract_tool_calls_tolerates_bad_arguments_json() {
        let assistant = assistant_tool_call("tc1", "search", "not json")["message"].clone();
        let calls = extract_tool_calls(&assistant);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].arguments, Value::Null);
    }

    #[test]
    fn extract_tool_calls_empty_when_absent() {
        let assistant = assistant_text("done")["message"].clone();
        assert!(extract_tool_calls(&assistant).is_empty());
    }

    #[test]
    fn build_tool_dispatch_http() {
        let call = ToolCall {
            id: "1".into(),
            name: "search".into(),
            arguments: json!({ "q": "x" }),
        };
        let (kind, params) = build_tool_dispatch(
            Some(&json!({ "type": "http", "url": "https://tools.example/run" })),
            &call,
        )
        .unwrap();
        assert_eq!(kind, ToolKind::Http);
        assert_eq!(params["url"], "https://tools.example/run");
        assert_eq!(params["tool_name"], "search");
        assert_eq!(params["arguments"]["q"], "x");
    }

    #[test]
    fn build_tool_dispatch_mcp_with_headers() {
        let call = ToolCall {
            id: "1".into(),
            name: "echo".into(),
            arguments: json!({}),
        };
        let (kind, params) = build_tool_dispatch(
            Some(&json!({
                "type": "mcp",
                "url": "https://mcp.example/rpc",
                "headers": { "Authorization": "Bearer x" }
            })),
            &call,
        )
        .unwrap();
        assert_eq!(kind, ToolKind::Mcp);
        assert_eq!(params["action"], "call");
        assert_eq!(params["headers"]["Authorization"], "Bearer x");
    }

    #[test]
    fn build_tool_dispatch_defaults_to_http() {
        let call = ToolCall {
            id: "1".into(),
            name: "t".into(),
            arguments: json!({}),
        };
        let (kind, _) =
            build_tool_dispatch(Some(&json!({ "url": "https://e.example" })), &call).unwrap();
        assert_eq!(kind, ToolKind::Http);
    }

    #[test]
    fn build_tool_dispatch_missing_config_is_permanent() {
        let call = ToolCall {
            id: "1".into(),
            name: "search".into(),
            arguments: json!({}),
        };
        let err = build_tool_dispatch(None, &call).unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn build_tool_dispatch_missing_url_is_permanent() {
        let call = ToolCall {
            id: "1".into(),
            name: "search".into(),
            arguments: json!({}),
        };
        let err = build_tool_dispatch(Some(&json!({ "type": "http" })), &call).unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn build_tool_dispatch_unknown_type_is_permanent() {
        let call = ToolCall {
            id: "1".into(),
            name: "search".into(),
            arguments: json!({}),
        };
        let err =
            build_tool_dispatch(Some(&json!({ "type": "smtp", "url": "x" })), &call).unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn tool_result_message_shape() {
        let m = tool_result_message("tc1", &json!({ "result": 42 }));
        assert_eq!(m["role"], "tool");
        assert_eq!(m["tool_call_id"], "tc1");
        // content is a JSON string of the observation.
        let parsed: Value = serde_json::from_str(m["content"].as_str().unwrap()).unwrap();
        assert_eq!(parsed["result"], 42);
    }

    #[test]
    fn final_text_none_when_empty_or_null() {
        assert_eq!(final_text(&json!({ "content": "hi" })), Some("hi".into()));
        assert_eq!(final_text(&json!({ "content": "" })), None);
        assert_eq!(final_text(&json!({ "content": null })), None);
        assert_eq!(final_text(&json!({})), None);
    }

    // ---- loop integration tests with deterministic closures ----------------

    /// Run the loop with no-op checkpointing (most loop tests don't exercise
    /// resume).
    async fn run_loop_no_cp<L, LFut, T, TFut>(
        agent_params: &Value,
        max_iterations: u64,
        call_llm: L,
        dispatch_tool: T,
    ) -> Result<Value, StepError>
    where
        L: Fn(Value) -> LFut,
        LFut: std::future::Future<Output = Result<Value, StepError>>,
        T: Fn(ToolCall) -> TFut,
        TFut: std::future::Future<Output = Result<Value, StepError>>,
    {
        super::run_agent_loop(
            agent_params,
            max_iterations,
            call_llm,
            dispatch_tool,
            || async { Option::<Value>::None },
            |_cp: Value| async {},
        )
        .await
    }

    #[tokio::test]
    async fn loop_completes_immediately_when_no_tool_calls() {
        let out = run_loop_no_cp(
            &json!({ "goal": "say hi" }),
            6,
            |_params| async { Ok(assistant_text("hello!")) },
            |_call| async { Ok(json!({})) },
        )
        .await
        .unwrap();
        assert_eq!(out["stop_reason"], "completed");
        assert_eq!(out["iterations"], 1);
        assert_eq!(out["final"], "hello!");
        assert_eq!(out["tool_calls_made"], 0);
    }

    #[tokio::test]
    async fn loop_runs_tool_then_completes() {
        // Iteration 0: model asks for a tool. Iteration 1: model answers.
        let step = RefCell::new(0u32);
        let out = run_loop_no_cp(
            &json!({
                "goal": "weather?",
                "tool_dispatch": { "type": "http", "url": "https://x.example" }
            }),
            6,
            |_params| {
                let n = {
                    let mut s = step.borrow_mut();
                    let cur = *s;
                    *s += 1;
                    cur
                };
                async move {
                    if n == 0 {
                        Ok(assistant_tool_call(
                            "tc1",
                            "get_weather",
                            r#"{"city":"SF"}"#,
                        ))
                    } else {
                        Ok(assistant_text("It is sunny."))
                    }
                }
            },
            |call| async move {
                assert_eq!(call.name, "get_weather");
                assert_eq!(call.arguments["city"], "SF");
                Ok(json!({ "tool_name": "get_weather", "result": "sunny" }))
            },
        )
        .await
        .unwrap();

        assert_eq!(out["stop_reason"], "completed");
        assert_eq!(out["iterations"], 2);
        assert_eq!(out["final"], "It is sunny.");
        assert_eq!(out["tool_calls_made"], 1);
        // Conversation: user, assistant(tool_call), tool result, assistant(final)
        assert_eq!(out["messages"].as_array().unwrap().len(), 4);
        assert_eq!(out["messages"][2]["role"], "tool");
    }

    #[tokio::test]
    async fn loop_hits_max_iterations() {
        // Model asks for a tool forever.
        let out = run_loop_no_cp(
            &json!({
                "goal": "loop",
                "tool_dispatch": { "type": "http", "url": "https://x.example" }
            }),
            3,
            |_params| async { Ok(assistant_tool_call("tc", "spin", "{}")) },
            |_call| async { Ok(json!({ "ok": true })) },
        )
        .await
        .unwrap();
        assert_eq!(out["stop_reason"], "max_iterations");
        assert_eq!(out["iterations"], 3);
        assert_eq!(out["tool_calls_made"], 3);
        assert_eq!(out["final"], Value::Null);
    }

    #[tokio::test]
    async fn loop_surfaces_tool_error_as_observation_and_continues() {
        let step = RefCell::new(0u32);
        let out = run_loop_no_cp(
            &json!({
                "goal": "x",
                "tool_dispatch": { "type": "http", "url": "https://x.example" }
            }),
            6,
            |_params| {
                let n = {
                    let mut s = step.borrow_mut();
                    let cur = *s;
                    *s += 1;
                    cur
                };
                async move {
                    if n == 0 {
                        Ok(assistant_tool_call("tc1", "flaky", "{}"))
                    } else {
                        Ok(assistant_text("recovered"))
                    }
                }
            },
            |_call| async {
                Err(StepError::Permanent {
                    message: "tool exploded".into(),
                    details: None,
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(out["stop_reason"], "completed");
        assert_eq!(out["final"], "recovered");
        // The tool observation carries the error text so the model can react.
        let tool_msg = &out["messages"][2];
        assert_eq!(tool_msg["role"], "tool");
        assert!(tool_msg["content"]
            .as_str()
            .unwrap()
            .contains("tool exploded"));
    }

    #[tokio::test]
    async fn loop_propagates_llm_error() {
        let result = run_loop_no_cp(
            &json!({ "goal": "x" }),
            6,
            |_params| async {
                Err(StepError::Retryable {
                    message: "llm down".into(),
                    details: None,
                })
            },
            |_call| async { Ok(json!({})) },
        )
        .await;
        assert!(matches!(result, Err(StepError::Retryable { .. })));
    }

    #[tokio::test]
    async fn loop_multiple_tool_calls_in_one_turn() {
        let step = RefCell::new(0u32);
        let out = run_loop_no_cp(
            &json!({
                "goal": "x",
                "tool_dispatch": { "type": "http", "url": "https://x.example" }
            }),
            6,
            |_params| {
                let n = {
                    let mut s = step.borrow_mut();
                    let cur = *s;
                    *s += 1;
                    cur
                };
                async move {
                    if n == 0 {
                        Ok(json!({ "message": {
                            "role": "assistant",
                            "content": null,
                            "tool_calls": [
                                { "id": "a", "type": "function", "function": { "name": "t1", "arguments": "{}" } },
                                { "id": "b", "type": "function", "function": { "name": "t2", "arguments": "{}" } }
                            ]
                        }}))
                    } else {
                        Ok(assistant_text("both done"))
                    }
                }
            },
            |_call| async { Ok(json!({ "ok": true })) },
        )
        .await
        .unwrap();
        assert_eq!(out["tool_calls_made"], 2);
        assert_eq!(out["final"], "both done");
    }

    // ---- checkpoint / resume ----------------------------------------------

    #[test]
    fn checkpoint_round_trips() {
        let messages = vec![json!({ "role": "user", "content": "hi" })];
        let cp = make_checkpoint(&messages, 3, 2);
        let (restored, iter, tcm) = restore_checkpoint(Some(cp), &json!({ "goal": "x" }));
        assert_eq!(restored, messages);
        assert_eq!(iter, 3);
        assert_eq!(tcm, 2);
    }

    #[test]
    fn restore_checkpoint_none_starts_fresh() {
        let (msgs, iter, tcm) = restore_checkpoint(None, &json!({ "goal": "fresh" }));
        assert_eq!(msgs[0]["content"], "fresh");
        assert_eq!(iter, 0);
        assert_eq!(tcm, 0);
    }

    #[test]
    fn restore_checkpoint_invalid_starts_fresh() {
        // No `messages` array → treated as no checkpoint.
        let (msgs, iter, _) =
            restore_checkpoint(Some(json!({ "garbage": 1 })), &json!({ "goal": "g" }));
        assert_eq!(msgs[0]["content"], "g");
        assert_eq!(iter, 0);
    }

    #[tokio::test]
    async fn loop_resumes_from_checkpoint() {
        // Checkpoint says iteration 2 already done, one tool call made.
        let cp = json!({
            "messages": [{ "role": "user", "content": "resume me" }],
            "iteration": 2,
            "tool_calls_made": 1
        });
        let out = super::run_agent_loop(
            &json!({ "goal": "ignored-because-checkpoint-wins" }),
            6,
            |_p| async { Ok(assistant_text("resumed answer")) },
            |_c| async { Ok(json!({})) },
            move || {
                let c = cp.clone();
                async move { Some(c) }
            },
            |_cp: Value| async {},
        )
        .await
        .unwrap();
        assert_eq!(out["stop_reason"], "completed");
        // Started at iteration 2 → completes at iteration 2 (result = 2 + 1).
        assert_eq!(out["iterations"], 3);
        assert_eq!(out["final"], "resumed answer");
        // Prior tool_calls_made carried over.
        assert_eq!(out["tool_calls_made"], 1);
    }

    #[tokio::test]
    async fn loop_saves_checkpoint_after_each_iteration() {
        let saved = RefCell::new(Vec::<Value>::new());
        let step = RefCell::new(0u32);
        let out = super::run_agent_loop(
            &json!({
                "goal": "x",
                "tool_dispatch": { "type": "http", "url": "https://x.example" }
            }),
            6,
            |_p| {
                let n = {
                    let mut s = step.borrow_mut();
                    let cur = *s;
                    *s += 1;
                    cur
                };
                async move {
                    if n == 0 {
                        Ok(assistant_tool_call("tc", "t", "{}"))
                    } else {
                        Ok(assistant_text("done"))
                    }
                }
            },
            |_c| async { Ok(json!({ "ok": true })) },
            || async { Option::<Value>::None },
            |cp: Value| {
                saved.borrow_mut().push(cp);
                async {}
            },
        )
        .await
        .unwrap();
        assert_eq!(out["stop_reason"], "completed");
        // Exactly one checkpoint, written after the first (tool) iteration.
        let recorded = saved.borrow();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0]["iteration"], 1);
        assert_eq!(recorded[0]["tool_calls_made"], 1);
    }

    // ---- tool auto-discovery ----------------------------------------------

    #[test]
    fn mcp_tools_to_llm_schema_converts() {
        let mcp = json!([
            { "name": "echo", "description": "Echo it", "inputSchema": { "type": "object" } },
            { "name": "noschema" }
        ]);
        let schema = mcp_tools_to_llm_schema(&mcp);
        assert_eq!(schema[0]["type"], "function");
        assert_eq!(schema[0]["function"]["name"], "echo");
        assert_eq!(schema[0]["function"]["description"], "Echo it");
        assert_eq!(schema[0]["function"]["parameters"]["type"], "object");
        // Missing inputSchema → empty parameters object.
        assert_eq!(schema[1]["function"]["parameters"], json!({}));
    }

    #[test]
    fn mcp_tools_to_llm_schema_handles_non_array() {
        assert_eq!(mcp_tools_to_llm_schema(&Value::Null), json!([]));
    }

    // ---- auto-memory helpers ----------------------------------------------

    #[test]
    fn auto_memory_query_prefers_goal() {
        assert_eq!(
            auto_memory_query(&json!({ "goal": "find x" })),
            Some("find x".into())
        );
    }

    #[test]
    fn auto_memory_query_falls_back_to_last_user_message() {
        let q = auto_memory_query(&json!({
            "messages": [
                { "role": "user", "content": "first" },
                { "role": "assistant", "content": "reply" },
                { "role": "user", "content": "latest" }
            ]
        }));
        assert_eq!(q, Some("latest".into()));
    }

    #[test]
    fn auto_memory_query_none_when_absent() {
        assert_eq!(auto_memory_query(&json!({})), None);
    }

    #[test]
    fn memory_recall_system_message_builds_from_results() {
        let results = json!([
            { "key": "a", "text": "the sky is blue", "score": 0.9 },
            { "key": "b", "text": "water is wet", "score": 0.8 }
        ]);
        let msg = memory_recall_system_message(Some(&results)).unwrap();
        assert_eq!(msg["role"], "system");
        let content = msg["content"].as_str().unwrap();
        assert!(content.contains("the sky is blue"));
        assert!(content.contains("water is wet"));
    }

    #[test]
    fn memory_recall_system_message_none_for_empty() {
        assert!(memory_recall_system_message(Some(&json!([]))).is_none());
        assert!(memory_recall_system_message(None).is_none());
        // Results without text fields → no message.
        assert!(memory_recall_system_message(Some(&json!([{ "key": "a" }]))).is_none());
    }

    #[test]
    fn embed_config_extracts_provider_fields() {
        let cfg = embed_config(&json!({
            "base_url": "https://e.example",
            "api_key": "k",
            "model": "m",
            "recall_k": 3,
            "ignored": true
        }));
        assert_eq!(cfg.get("base_url").unwrap(), "https://e.example");
        assert_eq!(cfg.get("api_key").unwrap(), "k");
        assert_eq!(cfg.get("model").unwrap(), "m");
        assert!(!cfg.contains_key("recall_k"));
        assert!(!cfg.contains_key("ignored"));
    }
}

/// Tests that drive the async wiring in `handle_agent` (tool discovery,
/// auto-memory recall/store, checkpoint persistence) against in-process HTTP
/// mocks and in-memory `SQLite` — no e2e server.
#[cfg(test)]
mod net_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, TenantId};

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    async fn read_request(sock: &mut tokio::net::TcpStream) -> (String, String) {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            let n = sock.read(&mut tmp).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..pos]);
                let path = head
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("")
                    .to_string();
                let want = head
                    .to_lowercase()
                    .split("content-length:")
                    .nth(1)
                    .and_then(|s| s.trim().split([' ', '\r', '\n']).next())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                let start = pos + 4;
                if buf.len() >= start + want {
                    let body = String::from_utf8_lossy(&buf[start..start + want]).to_string();
                    return (path, body);
                }
            }
        }
        (String::new(), String::new())
    }

    /// Spawn a mock answering `count` requests via `handler(path, body) -> (status, body)`.
    async fn spawn<F>(count: usize, handler: F) -> String
    where
        F: Fn(String, String) -> (u16, String) + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for _ in 0..count {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let (path, body) = read_request(&mut sock).await;
                let (status, resp) = handler(path, body);
                let out = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp}",
                    resp.len()
                );
                let _ = sock.write_all(out.as_bytes()).await;
                let _ = sock.flush().await;
            }
        });
        format!("http://127.0.0.1:{}", addr.port())
    }

    async fn mark_safe(url: &str) {
        super::super::builtin::mark_url_safe_for_test(url).await;
    }

    async fn mk_ctx(params: Value) -> (StepContext, Arc<dyn StorageBackend>, InstanceId) {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let instance_id = InstanceId::new();
        let ctx = StepContext {
            instance_id,
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("b"),
            params,
            context: ExecutionContext::default(),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        (ctx, storage, instance_id)
    }

    fn llm_answer(text: &str) -> String {
        json!({
            "choices": [{ "index": 0, "message": { "role": "assistant", "content": text }, "finish_reason": "stop" }],
            "model": "m"
        })
        .to_string()
    }

    fn llm_tool_call(name: &str, args: &str) -> String {
        json!({
            "choices": [{ "index": 0, "message": {
                "role": "assistant", "content": null,
                "tool_calls": [{ "id": "tc1", "type": "function", "function": { "name": name, "arguments": args } }]
            }, "finish_reason": "tool_calls" }],
            "model": "m"
        })
        .to_string()
    }

    #[tokio::test]
    async fn dry_run_skips_loop() {
        // No LLM mock spawned — a real loop would fail. Ok proves the skip.
        let (mut ctx, _s, _i) =
            mk_ctx(json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
        ctx.context.runtime.dry_run = true;
        let out = handle_agent(ctx).await.unwrap();
        assert_eq!(out["dry_run"], true);
        assert_eq!(out["iterations"], 0);
        assert_eq!(out["stop_reason"], "dry_run");
        assert!(out["final"].is_null());
    }

    #[tokio::test]
    async fn handle_agent_no_tools_completes() {
        let url = spawn(1, |_p, _b| (200, llm_answer("hi there"))).await;
        mark_safe(&url).await; // llm_call checks the base URL
        let (ctx, _s, _i) = mk_ctx(json!({
            "provider": "openai", "base_url": url, "api_key": "k", "model": "m",
            "goal": "say hi"
        }))
        .await;
        let out = handle_agent(ctx).await.unwrap();
        assert_eq!(out["stop_reason"], "completed");
        assert_eq!(out["final"], "hi there");
    }

    #[tokio::test]
    async fn handle_agent_tool_iteration_writes_checkpoint() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&calls);
        // 2 LLM turns (tool, then final) + 1 tool dispatch = 3 connections.
        let url = spawn(3, move |path, _body| {
            if path.contains("/tool") {
                return (200, json!({ "ok": true }).to_string());
            }
            let n = c.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                (200, llm_tool_call("do_it", "{}"))
            } else {
                (200, llm_answer("finished"))
            }
        })
        .await;
        mark_safe(&url).await; // llm_call checks the base URL
        mark_safe(&format!("{url}/tool")).await; // tool_call checks the full URL

        let (ctx, storage, iid) = mk_ctx(json!({
            "provider": "openai", "base_url": url.clone(), "api_key": "k", "model": "m",
            "goal": "go",
            "tool_dispatch": { "type": "http", "url": format!("{url}/tool") }
        }))
        .await;
        let out = handle_agent(ctx).await.unwrap();
        assert_eq!(out["stop_reason"], "completed");
        assert_eq!(out["final"], "finished");

        // A checkpoint was persisted after the tool iteration.
        let cp = storage
            .get_instance_kv(iid, "__agent__:b")
            .await
            .unwrap()
            .expect("checkpoint should be written");
        assert_eq!(cp["iteration"], 1);
        assert_eq!(cp["tool_calls_made"], 1);
    }

    #[tokio::test]
    async fn maybe_discover_tools_injects_mcp_schema() {
        // MCP handshake (initialize, initialized, tools/list) = 3 connections.
        let url = spawn(3, |_path, body| {
            let msg: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
            let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
            let id = msg.get("id").and_then(Value::as_u64);
            match method {
                "initialize" => (
                    200,
                    json!({"jsonrpc":"2.0","id":id,"result":{}}).to_string(),
                ),
                "notifications/initialized" => (202, String::new()),
                _ => (
                    200,
                    json!({"jsonrpc":"2.0","id":id,"result":{"tools":[
                        {"name":"search","description":"Search","inputSchema":{"type":"object"}}
                    ]}})
                    .to_string(),
                ),
            }
        })
        .await;
        mark_safe(&url).await;

        let (ctx, _s, _i) = mk_ctx(json!({
            "goal": "x",
            "tool_dispatch": { "type": "mcp", "url": url }
        }))
        .await;
        let params = maybe_discover_tools(&ctx).await.unwrap();
        let tools = params.get("tools").and_then(Value::as_array).unwrap();
        assert_eq!(tools[0]["function"]["name"], "search");
    }

    #[tokio::test]
    async fn maybe_discover_tools_noop_when_tools_present() {
        let (ctx, _s, _i) = mk_ctx(json!({
            "tools": [{ "type": "function", "function": { "name": "t" } }],
            "tool_dispatch": { "type": "mcp", "url": "https://x.example" }
        }))
        .await;
        let params = maybe_discover_tools(&ctx).await.unwrap();
        // Unchanged — no discovery call made.
        assert_eq!(params["tools"][0]["function"]["name"], "t");
    }

    #[tokio::test]
    async fn maybe_recall_memory_prepends_system_message() {
        let url = spawn(1, |_p, _b| {
            (
                200,
                json!({ "data": [{ "index": 0, "embedding": [1.0, 0.0] }], "model": "m" })
                    .to_string(),
            )
        })
        .await;
        mark_safe(&format!("{url}/embeddings")).await;

        let (ctx, storage, iid) = mk_ctx(json!({
            "goal": "what colour is the sky",
            "auto_memory": { "base_url": url, "api_key": "k", "recall_k": 1 }
        }))
        .await;
        // Pre-store a memory whose embedding matches the query embedding.
        storage
            .set_instance_kv(
                iid,
                "__mem__:sky",
                &json!({ "text": "the sky is blue", "embedding": [1.0, 0.0], "metadata": {} }),
            )
            .await
            .unwrap();

        let params = handle_recall(&ctx).await;
        let messages = params.get("messages").and_then(Value::as_array).unwrap();
        assert_eq!(messages[0]["role"], "system");
        assert!(messages[0]["content"]
            .as_str()
            .unwrap()
            .contains("the sky is blue"));
    }

    // Small wrapper so the test reads clearly.
    async fn handle_recall(ctx: &StepContext) -> Value {
        maybe_recall_memory(ctx, ctx.params.clone()).await.unwrap()
    }

    #[tokio::test]
    async fn maybe_store_memory_persists_final_answer() {
        let url = spawn(1, |_p, _b| {
            (
                200,
                json!({ "data": [{ "index": 0, "embedding": [0.5, 0.5] }], "model": "m" })
                    .to_string(),
            )
        })
        .await;
        mark_safe(&format!("{url}/embeddings")).await;

        let (ctx, storage, iid) = mk_ctx(json!({
            "auto_memory": { "base_url": url, "api_key": "k" }
        }))
        .await;
        let outcome = json!({ "final": "the answer is 42" });
        maybe_store_memory(&ctx, &ctx.params, &outcome).await;

        let kv = storage.get_all_instance_kv(iid).await.unwrap();
        let mem = kv.keys().find(|k| k.starts_with("__mem__:"));
        assert!(mem.is_some(), "a memory should have been stored");
    }

    #[tokio::test]
    async fn maybe_store_memory_skips_when_disabled() {
        let (ctx, storage, iid) = mk_ctx(json!({
            "auto_memory": { "store_outcome": false, "api_key": "k" }
        }))
        .await;
        maybe_store_memory(&ctx, &ctx.params, &json!({ "final": "x" })).await;
        let kv = storage.get_all_instance_kv(iid).await.unwrap();
        assert!(kv.keys().all(|k| !k.starts_with("__mem__:")));
    }
}
