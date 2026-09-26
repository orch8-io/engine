//! Interactive approvals for `wait_for_input` gates, sent by `human_review`.
//!
//! The gate's handler only runs *after* a decision arrives, so interactive
//! approvals are requested by a `human_review` step placed **before** the
//! gate and pointed at it with `approvals.gate`:
//!
//! ```json
//! {"id": "ask_manager", "handler": "human_review", "params": {
//!    "instructions": "Approve the refund?",
//!    "approvals": {
//!      "gate": "manager_decision",
//!      "slack": {"url": "credentials://ops-slack/url", "signing_secret_credential": "slack-app/signing_secret"},
//!      "teams": {"url": "credentials://ops-teams/url"},
//!      "email": {"provider": "resend", "api_key": "credentials://resend/api_key",
//!                "from": "Approvals <approvals@acme.com>", "to": "manager@acme.com"}
//!    }}},
//! {"id": "manager_decision", "handler": "noop", "wait_for_input": {"prompt": "Refund $120?",
//!    "choices": [{"label": "Approve", "value": "approve"}, {"label": "Reject", "value": "reject"}]}}
//! ```
//!
//! Every choice on every channel gets its own 256-bit random token; only its
//! SHA-256 is stored ([`ApprovalActionToken`]). Tokens expire, and consuming
//! one burns every sibling of the gate, so each gate records exactly one
//! out-of-band decision. Raw tokens appear only inside the sent messages —
//! never in step outputs or logs.
//!
//! * **Slack** — Block Kit buttons whose `value` is the token. Slack posts
//!   clicks to `POST /approvals/slack/interactions`, which verifies the Slack
//!   signing secret (`v0` HMAC, 5-minute tolerance) from the credential named
//!   by `signing_secret_credential` before consuming the token.
//! * **Teams** — Adaptive Card `Action.OpenUrl` buttons to the magic-link
//!   confirm page (incoming webhooks cannot deliver `Action.Submit`), plus
//!   the same URL accepts an `Action.Http`/JSON `POST`.
//! * **Email** — magic links. `GET /approvals/act/{token}` renders a confirm
//!   page and changes nothing (link prefetchers and scanners are harmless);
//!   only the `POST` records the decision.
//!
//! Re-running the step (retry, crash recovery) never re-sends a channel that
//! still has live tokens for the gate.

use std::sync::OnceLock;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use serde_json::{Value, json};
use tracing::warn;

use orch8_types::approval_link::{ApprovalActionToken, ApprovalChannel, hash_token};
use orch8_types::error::StepError;
use orch8_types::ids::BlockId;
use orch8_types::sequence::{HumanChoice, HumanInputDef};

use super::StepContext;
use super::notify::Notification;

/// Default token lifetime when the gate has no timeout (7 days).
const DEFAULT_TTL_SECS: i64 = 7 * 86_400;
const MAX_TTL_SECS: i64 = 30 * 86_400;

static PUBLIC_BASE_URL: OnceLock<String> = OnceLock::new();

/// Set the externally reachable base URL (e.g. `https://orch8.acme.com`)
/// used to build magic links. Called once by the server from
/// `api.public_url` / `ORCH8_PUBLIC_URL`.
pub fn set_public_base_url(url: &str) {
    let trimmed = url.trim().trim_end_matches('/');
    if !trimmed.is_empty() {
        let _ = PUBLIC_BASE_URL.set(trimmed.to_string());
    }
}

#[must_use]
pub fn public_base_url() -> Option<&'static str> {
    PUBLIC_BASE_URL.get().map(String::as_str)
}

/// A fresh 256-bit URL-safe token.
#[must_use]
pub fn new_raw_token() -> String {
    let bytes: [u8; 32] = rand::random();
    URL_SAFE_NO_PAD.encode(bytes)
}

fn permanent(message: impl Into<String>) -> StepError {
    StepError::Permanent {
        message: message.into(),
        details: None,
    }
}

fn storage_err(context: &str, e: &orch8_types::error::StorageError) -> StepError {
    StepError::Retryable {
        message: format!("{context}: {e}"),
        details: None,
    }
}

/// Depth-first search for the step `gate` carrying `wait_for_input`.
fn find_gate(v: &Value, gate: &str) -> Option<Value> {
    match v {
        Value::Object(map) => {
            if map.get("id").and_then(Value::as_str) == Some(gate)
                && let Some(wfi) = map.get("wait_for_input").filter(|w| !w.is_null())
            {
                return Some(wfi.clone());
            }
            map.values().find_map(|child| find_gate(child, gate))
        }
        Value::Array(items) => items.iter().find_map(|child| find_gate(child, gate)),
        _ => None,
    }
}

async fn load_gate(ctx: &StepContext, gate: &str) -> Result<HumanInputDef, StepError> {
    let instance = ctx
        .storage
        .get_instance(ctx.instance_id)
        .await
        .map_err(|e| storage_err("human_review: instance lookup", &e))?
        .ok_or_else(|| permanent("human_review: instance not found"))?;
    let sequence = ctx
        .storage
        .get_sequence(instance.sequence_id)
        .await
        .map_err(|e| storage_err("human_review: sequence lookup", &e))?
        .ok_or_else(|| permanent("human_review: sequence not found"))?;
    let blocks = serde_json::to_value(&sequence.blocks)
        .map_err(|e| permanent(format!("human_review: {e}")))?;
    let wfi = find_gate(&blocks, gate).ok_or_else(|| {
        permanent(format!(
            "human_review: approvals.gate `{gate}` is not a step with wait_for_input in this sequence"
        ))
    })?;
    serde_json::from_value(wfi)
        .map_err(|e| permanent(format!("human_review: bad gate definition: {e}")))
}

struct Issued {
    choice: HumanChoice,
    raw: String,
}

fn action_url(base: &str, raw: &str) -> String {
    format!("{base}/approvals/act/{raw}")
}

fn base_url(cfg: &Value) -> Result<String, StepError> {
    let from_param = cfg
        .get("public_base_url")
        .and_then(Value::as_str)
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty());
    let base = from_param
        .or_else(|| public_base_url().map(str::to_string))
        .ok_or_else(|| {
            permanent("human_review: teams/email approvals need a public URL (server `api.public_url` / ORCH8_PUBLIC_URL, or approvals.public_base_url)")
        })?;
    if !(base.starts_with("https://") || base.starts_with("http://")) {
        return Err(permanent("human_review: public base URL must be http(s)"));
    }
    Ok(base)
}

/// Send interactive approval messages for `approvals.gate`. Returns a
/// per-channel delivery summary (never tokens or secrets).
#[allow(clippy::too_many_lines)]
pub(crate) async fn send_interactive_approvals(
    ctx: &StepContext,
    cfg: &Value,
    instructions: &str,
    review_data: &Value,
) -> Result<Value, StepError> {
    let gate = cfg
        .get("gate")
        .and_then(Value::as_str)
        .filter(|g| !g.is_empty())
        .ok_or_else(|| {
            permanent("human_review: approvals.gate (the id of the wait_for_input step to answer) is required")
        })?;
    if gate == ctx.block_id.as_str() {
        return Err(permanent(
            "human_review: approvals.gate must name a later step — a step's handler runs only after its own wait_for_input is answered",
        ));
    }
    let human = load_gate(ctx, gate).await?;
    let choices = human.effective_choices();
    let gate_id = BlockId::new(gate);

    let ttl_secs = cfg
        .get("ttl_secs")
        .and_then(Value::as_i64)
        .or_else(|| human.timeout.and_then(|t| i64::try_from(t.as_secs()).ok()))
        .unwrap_or(DEFAULT_TTL_SECS)
        .clamp(60, MAX_TTL_SECS);

    let prompt = if human.prompt.is_empty() {
        instructions.to_string()
    } else {
        human.prompt.clone()
    };
    let title = cfg
        .get("title")
        .and_then(Value::as_str)
        .map_or_else(|| "Approval needed".to_string(), str::to_string);
    let mut body_text = prompt.clone();
    if !instructions.is_empty() && instructions != prompt {
        body_text.push_str("\n\n");
        body_text.push_str(instructions);
    }
    let fields: Vec<(String, String)> = match review_data {
        Value::Object(m) => m
            .iter()
            .take(10)
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.as_str().map_or_else(|| v.to_string(), str::to_string),
                )
            })
            .collect(),
        Value::Null => Vec::new(),
        other => vec![(
            "Details".into(),
            other
                .as_str()
                .map_or_else(|| other.to_string(), str::to_string),
        )],
    };
    let notification = Notification {
        title: Some(title.clone()),
        text: body_text.clone(),
        fields,
        link: None,
        color: None,
    };

    // Validate every configured channel before any side effect.
    let slack_cfg = cfg.get("slack").filter(|v| !v.is_null());
    let teams_cfg = cfg.get("teams").filter(|v| !v.is_null());
    let email_cfg = cfg.get("email").filter(|v| !v.is_null());
    if slack_cfg.is_none() && teams_cfg.is_none() && email_cfg.is_none() {
        return Err(permanent(
            "human_review: approvals needs at least one of slack, teams, email",
        ));
    }
    let slack_secret_ref = match slack_cfg {
        Some(s) => {
            let r = s
                .get("signing_secret_credential")
                .and_then(Value::as_str)
                .filter(|r| !r.is_empty() && !r.starts_with("credentials://"))
                .ok_or_else(|| {
                    permanent("human_review: approvals.slack.signing_secret_credential must be a bare credential id (e.g. `slack-app/signing_secret`, not credentials://…)")
                })?;
            // It must name an existing credential in this tenant — this also
            // guarantees a secret value accidentally inlined here is never
            // persisted as a "reference".
            let id = r.split('/').next().unwrap_or(r);
            let exists = ctx
                .storage
                .get_credential(Some(&ctx.tenant_id), id)
                .await
                .map_err(|e| storage_err("human_review: credential lookup", &e))?
                .is_some();
            if !exists {
                return Err(permanent(
                    "human_review: approvals.slack.signing_secret_credential does not name an existing credential",
                ));
            }
            Some(r.to_string())
        }
        None => None,
    };
    let slack_url = slack_cfg
        .map(|s| super::notify::webhook_url(s, "url"))
        .transpose()?;
    let teams_url = teams_cfg
        .map(|t| super::notify::webhook_url(t, "url"))
        .transpose()?;
    let link_base = if teams_cfg.is_some() || email_cfg.is_some() {
        Some(base_url(cfg)?)
    } else {
        None
    };
    for url in slack_url.iter().chain(teams_url.iter()) {
        if !super::builtin::is_url_safe(url).await {
            return Err(permanent(
                "blocked: URL targets a private/internal network address",
            ));
        }
    }

    let mut summary = serde_json::Map::new();
    summary.insert("gate".into(), json!(gate));
    summary.insert("expires_in_secs".into(), json!(ttl_secs));
    if ctx.is_dry_run() {
        summary.insert("dry_run".into(), json!(true));
        return Ok(Value::Object(summary));
    }

    let now = Utc::now();
    let expires_at = now + chrono::Duration::seconds(ttl_secs);
    let timeout = std::time::Duration::from_secs(10);

    let channels: [(ApprovalChannel, bool); 3] = [
        (ApprovalChannel::Slack, slack_cfg.is_some()),
        (ApprovalChannel::Teams, teams_cfg.is_some()),
        (ApprovalChannel::Email, email_cfg.is_some()),
    ];
    for (channel, enabled) in channels {
        if !enabled {
            continue;
        }
        let already = ctx
            .storage
            .has_live_approval_tokens(ctx.instance_id, &gate_id, channel, now)
            .await
            .map_err(|e| storage_err("human_review: token lookup", &e))?;
        if already {
            summary.insert(channel.as_str().into(), json!({"status": "already_sent"}));
            continue;
        }
        let recipient = match channel {
            ApprovalChannel::Email => email_cfg.and_then(|e| e.get("to")).map(|v| match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            }),
            _ => None,
        };
        let issued: Vec<Issued> = choices
            .iter()
            .map(|c| Issued {
                choice: c.clone(),
                raw: new_raw_token(),
            })
            .collect();
        let rows: Vec<ApprovalActionToken> = issued
            .iter()
            .map(|i| ApprovalActionToken {
                token_hash: hash_token(&i.raw),
                tenant_id: ctx.tenant_id.clone(),
                instance_id: ctx.instance_id,
                block_id: gate_id.clone(),
                choice: i.choice.value.clone(),
                channel,
                recipient: recipient.clone(),
                verify_secret_ref: if channel == ApprovalChannel::Slack {
                    slack_secret_ref.clone()
                } else {
                    None
                },
                created_at: now,
                expires_at,
                used_at: None,
            })
            .collect();
        // Persist before sending: a crash after this point leaves live tokens,
        // so recovery skips the channel instead of double-sending.
        ctx.storage
            .create_approval_tokens(&rows)
            .await
            .map_err(|e| storage_err("human_review: store approval tokens", &e))?;

        let result = match channel {
            ApprovalChannel::Slack => {
                let buttons: Vec<Value> = issued
                    .iter()
                    .enumerate()
                    .map(|(i, is)| {
                        let mut b = json!({
                            "type": "button",
                            "action_id": format!("orch8_approval_{i}"),
                            "text": {"type": "plain_text", "text": is.choice.label},
                            "value": is.raw,
                        });
                        if i == 0 {
                            b["style"] = json!("primary");
                        }
                        b
                    })
                    .collect();
                let body = super::notify::render_slack(&notification, Some(buttons));
                super::notify::post_webhook(
                    "human_review(slack)",
                    slack_url.as_deref().unwrap_or_default(),
                    &body,
                    timeout,
                )
                .await
                .map(|_| ())
            }
            ApprovalChannel::Teams => {
                let base = link_base.as_deref().unwrap_or_default();
                let actions: Vec<Value> = issued
                    .iter()
                    .map(|is| json!({"type": "Action.OpenUrl", "title": is.choice.label, "url": action_url(base, &is.raw)}))
                    .collect();
                let body = super::notify::render_teams(&notification, &actions);
                super::notify::post_webhook(
                    "human_review(teams)",
                    teams_url.as_deref().unwrap_or_default(),
                    &body,
                    timeout,
                )
                .await
                .map(|_| ())
            }
            ApprovalChannel::Email => {
                let base = link_base.as_deref().unwrap_or_default();
                send_email(
                    ctx,
                    email_cfg.unwrap_or(&Value::Null),
                    &title,
                    &body_text,
                    &issued,
                    base,
                )
                .await
            }
        };
        match result {
            Ok(()) => {
                summary.insert(
                    channel.as_str().into(),
                    json!({"status": "sent", "choices": issued.len()}),
                );
            }
            Err(e) => {
                // Non-blocking like `notify_url`: the gate stays answerable via
                // the API/dashboard. The tokens stay live, so a retry does not
                // spam the channel.
                let (StepError::Permanent { message, .. } | StepError::Retryable { message, .. }) =
                    e;
                warn!(instance_id = %ctx.instance_id, gate = %gate, channel = channel.as_str(), "human_review: approval message failed");
                summary.insert(
                    channel.as_str().into(),
                    json!({"status": "failed", "error": message}),
                );
            }
        }
    }
    Ok(Value::Object(summary))
}

fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

async fn send_email(
    ctx: &StepContext,
    email_cfg: &Value,
    title: &str,
    body_text: &str,
    issued: &[Issued],
    base: &str,
) -> Result<(), StepError> {
    use std::fmt::Write as _;
    let Value::Object(map) = email_cfg else {
        return Err(permanent("human_review: approvals.email must be an object"));
    };
    let mut params = map.clone();
    params.entry("subject").or_insert_with(|| {
        json!(format!(
            "{title}: {}",
            body_text.lines().next().unwrap_or("")
        ))
    });
    let mut text = format!("{body_text}\n\n");
    let mut html = format!("<p>{}</p><p>", html_escape(body_text).replace('\n', "<br>"));
    for is in issued {
        let url = action_url(base, &is.raw);
        let _ = writeln!(text, "{}: {url}", is.choice.label);
        let _ = write!(
            html,
            "<a href=\"{}\" style=\"display:inline-block;padding:10px 18px;margin:4px;border-radius:6px;background:#1f2937;color:#fff;text-decoration:none\">{}</a> ",
            html_escape(&url),
            html_escape(&is.choice.label)
        );
    }
    text.push_str("\nEach link opens a confirmation page; the decision is recorded only after you confirm. Links are single-use and expire.\n");
    html.push_str("</p><p style=\"color:#6b7280;font-size:12px\">Each link opens a confirmation page; the decision is recorded only after you confirm. Links are single-use and expire.</p>");
    params.insert("text".into(), json!(text));
    params.insert("html".into(), json!(html));
    // No attachments / idempotency passthrough for approval mail.
    params.remove("attachments");
    let email_ctx = StepContext {
        params: Value::Object(params),
        ..ctx.clone()
    };
    super::email::handle_email(email_ctx).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_url_safe_and_unique() {
        let a = new_raw_token();
        let b = new_raw_token();
        assert_ne!(a, b);
        assert_eq!(a.len(), 43);
        assert!(orch8_types::approval_link::token_looks_valid(&a));
    }

    #[test]
    fn find_gate_searches_nested_blocks() {
        let blocks = json!([
            {"type": "step", "id": "a", "handler": "noop"},
            {"type": "parallel", "id": "p", "branches": [[
                {"type": "step", "id": "g", "handler": "noop", "wait_for_input": {"prompt": "ok?"}}
            ]]}
        ]);
        assert_eq!(find_gate(&blocks, "g").unwrap()["prompt"], "ok?");
        assert!(find_gate(&blocks, "a").is_none());
    }

    #[test]
    fn html_is_escaped() {
        assert_eq!(
            html_escape("<a href='x'>&"),
            "&lt;a href=&#39;x&#39;&gt;&amp;"
        );
    }

    use orch8_storage::StorageBackend;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{InstanceId, Namespace, TenantId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Accepts `n` requests, returning each body.
    async fn mock(n: usize) -> (String, tokio::sync::mpsc::UnboundedReceiver<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            for _ in 0..n {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let mut chunk = [0u8; 8192];
                loop {
                    let k = sock.read(&mut chunk).await.unwrap();
                    if k == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..k]);
                    let t = String::from_utf8_lossy(&buf);
                    if let Some(i) = t.find("\r\n\r\n") {
                        let len = t[..i]
                            .lines()
                            .find_map(|l| {
                                let (k, v) = l.split_once(':')?;
                                k.eq_ignore_ascii_case("content-length")
                                    .then(|| v.trim().parse::<usize>().ok())?
                            })
                            .unwrap_or(0);
                        if buf.len() >= i + 4 + len {
                            break;
                        }
                    }
                }
                let reply = r#"{"id":"m1"}"#;
                let _ = sock
                    .write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}", reply.len()).as_bytes())
                    .await;
                let t = String::from_utf8_lossy(&buf).to_string();
                let _ = tx.send(t[t.find("\r\n\r\n").unwrap() + 4..].to_string());
            }
        });
        (format!("http://127.0.0.1:{}", addr.port()), rx)
    }

    async fn setup() -> (Arc<dyn StorageBackend>, StepContext) {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let tenant = TenantId::unchecked("acme");
        let seq: orch8_types::sequence::SequenceDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::now_v7(), "tenant_id": "acme", "namespace": "default", "name": "refund",
            "version": 1, "created_at": Utc::now(),
            "blocks": [
                {"type": "step", "id": "ask", "handler": "human_review", "params": {}},
                {"type": "step", "id": "decide", "handler": "noop", "params": {},
                 "wait_for_input": {"prompt": "Refund $120?", "choices": [
                     {"label": "Approve", "value": "approve"}, {"label": "Reject", "value": "reject"}]}}
            ]
        }))
        .unwrap();
        storage.create_sequence(&seq).await.unwrap();
        let now = Utc::now();
        let instance = TaskInstance {
            id: InstanceId::new(),
            sequence_id: seq.id,
            tenant_id: tenant.clone(),
            namespace: Namespace::new("default"),
            state: InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        };
        storage.create_instance(&instance).await.unwrap();
        storage
            .create_credential(&orch8_types::credential::CredentialDef {
                id: "slack-app".into(),
                tenant_id: "acme".into(),
                name: "slack".into(),
                kind: orch8_types::credential::CredentialKind::ApiKey,
                value: orch8_types::config::SecretString::new(
                    r#"{"signing_secret":"s3cret"}"#.into(),
                ),
                expires_at: None,
                refresh_url: None,
                refresh_token: None,
                enabled: true,
                description: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        let ctx = StepContext {
            instance_id: instance.id,
            tenant_id: tenant,
            block_id: BlockId::new("ask"),
            params: json!({}),
            context: Arc::new(ExecutionContext::default()),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        (storage, ctx)
    }

    #[tokio::test]
    async fn slack_and_email_approvals_issue_hashed_single_channel_tokens() {
        let (storage, ctx) = setup().await;
        let (slack, mut slack_rx) = mock(1).await;
        let slack_url = format!("{slack}/hook");
        crate::handlers::builtin::mark_url_safe_for_test(&slack_url).await;
        let (resend, mut mail_rx) = mock(1).await;
        crate::handlers::builtin::mark_url_safe_for_test(&format!("{resend}/emails")).await;
        let cfg = json!({
            "gate": "decide",
            "public_base_url": "https://orch8.example.com/",
            "slack": {"url": slack_url, "signing_secret_credential": "slack-app/signing_secret"},
            "email": {"provider": "resend", "api_key": "k", "api_base_url": resend,
                      "from": "approvals@acme.test", "to": "boss@acme.test"},
        });
        let out = send_interactive_approvals(&ctx, &cfg, "check refund", &json!({"amount": 120}))
            .await
            .unwrap();
        assert_eq!(out["slack"]["status"], "sent");
        assert_eq!(out["email"]["status"], "sent");

        let slack_body: Value = serde_json::from_str(&slack_rx.recv().await.unwrap()).unwrap();
        let actions = slack_body["blocks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|b| b["type"] == "actions")
            .unwrap();
        let approve_raw = actions["elements"][0]["value"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(actions["elements"][0]["text"]["text"], "Approve");
        let row = storage
            .get_approval_token(&hash_token(&approve_raw))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.choice, "approve");
        assert_eq!(row.channel, ApprovalChannel::Slack);
        assert_eq!(
            row.verify_secret_ref.as_deref(),
            Some("slack-app/signing_secret")
        );
        assert!(
            !out.to_string().contains(&approve_raw),
            "raw tokens must not be in output"
        );

        let mail: Value = serde_json::from_str(&mail_rx.recv().await.unwrap()).unwrap();
        let text = mail["text"].as_str().unwrap();
        assert!(text.contains("https://orch8.example.com/approvals/act/"));
        let link_token = text
            .split("https://orch8.example.com/approvals/act/")
            .nth(1)
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap();
        assert_eq!(
            storage
                .get_approval_token(&hash_token(link_token))
                .await
                .unwrap()
                .unwrap()
                .channel,
            ApprovalChannel::Email
        );

        // Re-running (retry / recovery) does not re-send live channels.
        let again = send_interactive_approvals(&ctx, &cfg, "check refund", &Value::Null)
            .await
            .unwrap();
        assert_eq!(again["slack"]["status"], "already_sent");
        assert_eq!(again["email"]["status"], "already_sent");
    }

    #[tokio::test]
    async fn rejects_bad_gate_and_inline_secret_refs() {
        let (_s, ctx) = setup().await;
        let err = send_interactive_approvals(
            &ctx,
            &json!({"gate": "ask", "email": {}}),
            "",
            &Value::Null,
        )
        .await
        .unwrap_err();
        assert!(format!("{err:?}").contains("later step"));
        let err = send_interactive_approvals(
            &ctx,
            &json!({"gate": "nope", "email": {}}),
            "",
            &Value::Null,
        )
        .await
        .unwrap_err();
        assert!(format!("{err:?}").contains("not a step with wait_for_input"));
        // A resolved secret value in place of a credential id is refused and never stored.
        let err = send_interactive_approvals(
            &ctx,
            &json!({"gate": "decide", "slack": {"url": "https://hooks.slack.com/x", "signing_secret_credential": "8f742231b10e8888abcd99yyyzzz85a5"}}),
            "",
            &Value::Null,
        )
        .await
        .unwrap_err();
        assert!(format!("{err:?}").contains("existing credential"));
        let err = send_interactive_approvals(
            &ctx,
            &json!({"gate": "decide", "teams": {"url": "https://x.example/h"}}),
            "",
            &Value::Null,
        )
        .await
        .unwrap_err();
        assert!(format!("{err:?}").contains("public URL") || public_base_url().is_some());
    }
}
