//! Built-in `notify` handler — post a chat notification to Slack, Discord,
//! or Microsoft Teams incoming webhooks from one provider-neutral input.
//!
//! Incoming-webhook URLs are bearer secrets (anyone holding one can post to
//! the channel), so reference them from the credential store:
//! `"url": "credentials://ops-slack/url"`. The URL is SSRF-checked, never
//! logged, and never echoed into the step output.
//!
//! `notify` is a side-effecting builtin and goes through the effect ledger,
//! so crash recovery does not double-post an ambiguous delivery.
//!
//! ## Params
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `provider` | string | **required** | `slack`, `discord`, or `teams` |
//! | `url` | string | **required** | Incoming webhook URL (use `credentials://`) |
//! | `text` | string | **required** | Message body (Slack mrkdwn / Discord markdown / Teams text) |
//! | `title` | string | — | Bold heading |
//! | `fields` | object \| array | — | `{"Key": "value"}` or `[{"name","value"}]`, rendered as facts |
//! | `link` | string \| object | — | `"https://…"` or `{"url","label"}`, rendered as a button |
//! | `color` | string | — | Hex accent colour (`#36a64f`) for Discord embeds |
//! | `timeout_ms` | u64 | 10000 | Request timeout (clamped) |
//!
//! Output: `{provider, status, delivered: true}`.

use serde_json::{Map, Value, json};
use tracing::debug;

use orch8_types::error::StepError;

use super::StepContext;

const MAX_TEXT_CHARS: usize = 3_000;
const MAX_FIELDS: usize = 25;

fn permanent(message: impl Into<String>) -> StepError {
    StepError::Permanent {
        message: message.into(),
        details: None,
    }
}

/// Provider-neutral notification input.
#[derive(Debug, Clone, Default)]
pub(crate) struct Notification {
    pub title: Option<String>,
    pub text: String,
    pub fields: Vec<(String, String)>,
    pub link: Option<(String, String)>,
    pub color: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Provider {
    Slack,
    Discord,
    Teams,
}

impl Provider {
    pub(crate) fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "slack" => Some(Self::Slack),
            "discord" => Some(Self::Discord),
            "teams" | "msteams" | "microsoft_teams" => Some(Self::Teams),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Slack => "slack",
            Self::Discord => "discord",
            Self::Teams => "teams",
        }
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

fn value_to_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub(crate) fn parse_notification(params: &Value) -> Result<Notification, StepError> {
    let text = params
        .get("text")
        .map(value_to_text)
        .filter(|t| !t.trim().is_empty())
        .ok_or_else(|| permanent("notify: missing required param: text"))?;
    let title = params
        .get("title")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .map(|t| truncate(t, 150));
    let fields = match params.get("fields") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Object(map)) => map
            .iter()
            .map(|(k, v)| (k.clone(), value_to_text(v)))
            .collect(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                let name = item
                    .get("name")
                    .or_else(|| item.get("title"))
                    .map(value_to_text)
                    .ok_or_else(|| permanent("notify: each field needs `name` and `value`"))?;
                let value = item.get("value").map(value_to_text).unwrap_or_default();
                Ok((name, value))
            })
            .collect::<Result<Vec<_>, StepError>>()?,
        Some(_) => return Err(permanent("notify: `fields` must be an object or array")),
    };
    if fields.len() > MAX_FIELDS {
        return Err(permanent(format!("notify: at most {MAX_FIELDS} fields")));
    }
    let link = match params.get("link") {
        None | Some(Value::Null) => None,
        Some(Value::String(u)) => Some((u.clone(), "Open".to_string())),
        Some(obj @ Value::Object(_)) => {
            let url = obj
                .get("url")
                .and_then(Value::as_str)
                .ok_or_else(|| permanent("notify: `link.url` is required"))?;
            let label = obj.get("label").and_then(Value::as_str).unwrap_or("Open");
            Some((url.to_string(), label.to_string()))
        }
        Some(_) => {
            return Err(permanent(
                "notify: `link` must be a URL string or {url,label}",
            ));
        }
    };
    if let Some((u, _)) = &link {
        // Links are rendered as clickable buttons for humans: only http(s).
        let ok = url::Url::parse(u).is_ok_and(|p| matches!(p.scheme(), "http" | "https"));
        if !ok {
            return Err(permanent("notify: `link.url` must be an http(s) URL"));
        }
    }
    let color = params
        .get("color")
        .and_then(Value::as_str)
        .map(|c| c.trim_start_matches('#').to_string())
        .filter(|c| c.len() == 6 && c.chars().all(|ch| ch.is_ascii_hexdigit()));
    Ok(Notification {
        title,
        text: truncate(&text, MAX_TEXT_CHARS),
        fields,
        link,
        color,
    })
}

/// Build the provider-shaped JSON body.
pub(crate) fn render(provider: Provider, n: &Notification) -> Value {
    match provider {
        Provider::Slack => render_slack(n, None),
        Provider::Discord => {
            let mut embed = Map::new();
            if let Some(t) = &n.title {
                embed.insert("title".into(), json!(t));
            }
            embed.insert("description".into(), json!(n.text));
            if let Some((u, _)) = &n.link {
                embed.insert("url".into(), json!(u));
            }
            if !n.fields.is_empty() {
                embed.insert(
                    "fields".into(),
                    Value::Array(
                        n.fields
                            .iter()
                            .map(|(k, v)| json!({"name": truncate(k, 256), "value": truncate(if v.is_empty() { "-" } else { v }, 1024), "inline": true}))
                            .collect(),
                    ),
                );
            }
            if let Some(c) = &n.color
                && let Ok(num) = u32::from_str_radix(c, 16)
            {
                embed.insert("color".into(), json!(num));
            }
            // `allowed_mentions: {parse: []}` stops workflow-supplied text
            // from pinging @everyone / roles.
            json!({
                "embeds": [Value::Object(embed)],
                "allowed_mentions": {"parse": []},
            })
        }
        Provider::Teams => render_teams(n, &[]),
    }
}

/// Slack Block Kit message. `extra_actions` lets `human_review` append
/// interactive approve/reject buttons.
pub(crate) fn render_slack(n: &Notification, extra_actions: Option<Vec<Value>>) -> Value {
    let mut blocks = Vec::new();
    if let Some(t) = &n.title {
        blocks.push(
            json!({"type": "header", "text": {"type": "plain_text", "text": t, "emoji": true}}),
        );
    }
    blocks.push(json!({"type": "section", "text": {"type": "mrkdwn", "text": n.text}}));
    if !n.fields.is_empty() {
        // Slack caps a section at 10 fields; chunk the rest.
        for chunk in n.fields.chunks(10) {
            blocks.push(json!({
                "type": "section",
                "fields": chunk
                    .iter()
                    .map(|(k, v)| json!({"type": "mrkdwn", "text": format!("*{}*\n{}", k, v)}))
                    .collect::<Vec<_>>(),
            }));
        }
    }
    let mut actions = Vec::new();
    if let Some((u, label)) = &n.link {
        actions.push(
            json!({"type": "button", "text": {"type": "plain_text", "text": label}, "url": u}),
        );
    }
    if let Some(extra) = extra_actions {
        actions.extend(extra);
    }
    if !actions.is_empty() {
        blocks.push(json!({"type": "actions", "elements": actions}));
    }
    // Top-level `text` is the notification/accessibility fallback.
    let fallback = n
        .title
        .as_ref()
        .map_or_else(|| n.text.clone(), |t| format!("{t}: {}", n.text));
    json!({"text": truncate(&fallback, MAX_TEXT_CHARS), "blocks": blocks})
}

/// Teams Workflows / incoming-webhook Adaptive Card envelope. `actions` are
/// extra Adaptive Card actions (e.g. approve/reject `Action.OpenUrl`).
pub(crate) fn render_teams(n: &Notification, actions: &[Value]) -> Value {
    let mut body = Vec::new();
    if let Some(t) = &n.title {
        body.push(json!({"type": "TextBlock", "text": t, "weight": "Bolder", "size": "Medium", "wrap": true}));
    }
    body.push(json!({"type": "TextBlock", "text": n.text, "wrap": true}));
    if !n.fields.is_empty() {
        body.push(json!({
            "type": "FactSet",
            "facts": n.fields.iter().map(|(k, v)| json!({"title": k, "value": v})).collect::<Vec<_>>(),
        }));
    }
    let mut all_actions = Vec::new();
    if let Some((u, label)) = &n.link {
        all_actions.push(json!({"type": "Action.OpenUrl", "title": label, "url": u}));
    }
    all_actions.extend(actions.iter().cloned());
    let mut content = json!({
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": body,
    });
    if !all_actions.is_empty() {
        content["actions"] = Value::Array(all_actions);
    }
    json!({
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "contentUrl": null,
            "content": content,
        }],
    })
}

/// Resolve the webhook URL param: a string, or a credential object carrying
/// `url` / `webhook_url`.
pub(crate) fn webhook_url(params: &Value, key: &str) -> Result<String, StepError> {
    let v = params
        .get(key)
        .ok_or_else(|| permanent(format!("notify: missing required param: {key}")))?;
    let url = match v {
        Value::String(s) => s.clone(),
        Value::Object(o) => o
            .get("url")
            .or_else(|| o.get("webhook_url"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| permanent(format!("notify: `{key}` object needs a `url` field")))?,
        _ => return Err(permanent(format!("notify: `{key}` must be a URL string"))),
    };
    if url.starts_with("credentials://") {
        return Err(permanent(format!(
            "notify: `{key}` credential reference was not resolved"
        )));
    }
    Ok(url)
}

/// POST a JSON body to an SSRF-checked webhook URL. Errors never include the
/// URL (it is a secret).
pub(crate) async fn post_webhook(
    context: &str,
    url: &str,
    body: &Value,
    timeout: std::time::Duration,
) -> Result<u16, StepError> {
    if !super::builtin::is_url_safe(url).await {
        return Err(permanent(
            "blocked: URL targets a private/internal network address",
        ));
    }
    let resp = super::llm::http_client()
        .post(url)
        .timeout(timeout)
        .json(body)
        .send()
        .await
        .map_err(|e| StepError::Retryable {
            // `without_url` keeps the secret webhook URL out of the message.
            message: format!("{context}: request failed: {}", e.without_url()),
            details: None,
        })?;
    let status = resp.status().as_u16();
    if (200..300).contains(&status) {
        return Ok(status);
    }
    let bytes = crate::outbound::read_body_capped(resp, 4096)
        .await
        .unwrap_or_default();
    Err(super::email::status_error(context, status, &bytes))
}

pub async fn handle_notify(ctx: StepContext) -> Result<Value, StepError> {
    let provider_raw = ctx
        .params
        .get("provider")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            permanent("notify: missing required param: provider (slack|discord|teams)")
        })?;
    let provider = Provider::parse(provider_raw).ok_or_else(|| {
        permanent(format!(
            "notify: unsupported provider `{provider_raw}`; expected slack, discord, or teams"
        ))
    })?;
    let url = webhook_url(&ctx.params, "url")?;
    let notification = parse_notification(&ctx.params)?;
    let body = render(provider, &notification);
    let timeout = crate::outbound::clamp_timeout_ms(
        ctx.params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(10_000),
    );

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        provider = provider.as_str(),
        "notify step"
    );

    if ctx.is_dry_run() {
        if !super::builtin::is_url_safe(&url).await {
            return Err(permanent(
                "blocked: URL targets a private/internal network address",
            ));
        }
        return Ok(json!({"dry_run": true, "provider": provider.as_str(), "payload": body}));
    }
    let status = post_webhook("notify", &url, &body, timeout).await?;
    Ok(json!({"provider": provider.as_str(), "status": status, "delivered": true}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, TenantId};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn ctx_with(params: Value) -> StepContext {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        StepContext {
            instance_id: InstanceId::new(),
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("n"),
            params,
            context: Arc::new(ExecutionContext::default()),
            attempt: 0,
            storage,
            wait_for_input: None,
        }
    }

    async fn mock(status: u16) -> (String, tokio::sync::oneshot::Receiver<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 8192];
            loop {
                let n = sock.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
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
            let _ = sock
                .write_all(
                    format!(
                        "HTTP/1.1 {status} X\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
                    )
                    .as_bytes(),
                )
                .await;
            let t = String::from_utf8_lossy(&buf).to_string();
            let _ = tx.send(t[t.find("\r\n\r\n").unwrap() + 4..].to_string());
        });
        (format!("http://127.0.0.1:{}/hook", addr.port()), rx)
    }

    fn sample() -> Value {
        json!({
            "text": "Deploy finished",
            "title": "Deploy",
            "fields": {"env": "prod", "version": "1.2.3"},
            "link": {"url": "https://example.com/run/1", "label": "View run"},
            "color": "#36a64f",
        })
    }

    #[test]
    fn slack_payload_is_block_kit() {
        let n = parse_notification(&sample()).unwrap();
        let body = render(Provider::Slack, &n);
        assert_eq!(body["blocks"][0]["type"], "header");
        assert_eq!(body["blocks"][1]["text"]["text"], "Deploy finished");
        assert_eq!(body["blocks"][2]["fields"].as_array().unwrap().len(), 2);
        assert_eq!(
            body["blocks"][3]["elements"][0]["url"],
            "https://example.com/run/1"
        );
        assert!(body["text"].as_str().unwrap().contains("Deploy"));
    }

    #[test]
    fn discord_payload_is_embed_without_mentions() {
        let n = parse_notification(&sample()).unwrap();
        let body = render(Provider::Discord, &n);
        assert_eq!(body["embeds"][0]["title"], "Deploy");
        assert_eq!(body["embeds"][0]["url"], "https://example.com/run/1");
        assert_eq!(body["embeds"][0]["color"], 0x0036_a64f);
        assert_eq!(
            body["allowed_mentions"]["parse"].as_array().unwrap().len(),
            0
        );
    }

    #[test]
    fn teams_payload_is_adaptive_card() {
        let n = parse_notification(&sample()).unwrap();
        let body = render(Provider::Teams, &n);
        let card = &body["attachments"][0]["content"];
        assert_eq!(
            body["attachments"][0]["contentType"],
            "application/vnd.microsoft.card.adaptive"
        );
        assert_eq!(card["type"], "AdaptiveCard");
        assert_eq!(card["body"][2]["type"], "FactSet");
        assert_eq!(card["actions"][0]["type"], "Action.OpenUrl");
    }

    #[test]
    fn rejects_javascript_links() {
        let err =
            parse_notification(&json!({"text": "x", "link": "javascript:alert(1)"})).unwrap_err();
        assert!(format!("{err:?}").contains("http(s)"));
    }

    #[tokio::test]
    async fn posts_to_webhook_and_hides_url() {
        let (url, rx) = mock(200).await;
        crate::handlers::builtin::mark_url_safe_for_test(&url).await;
        let mut params = sample();
        params["provider"] = json!("slack");
        params["url"] = json!({"url": url});
        let out = handle_notify(ctx_with(params).await).await.unwrap();
        assert_eq!(out["delivered"], true);
        assert!(!out.to_string().contains("/hook"));
        let body: Value = serde_json::from_str(&rx.await.unwrap()).unwrap();
        assert_eq!(body["blocks"][0]["type"], "header");
    }

    #[tokio::test]
    async fn non_2xx_maps_to_step_error() {
        let (url, _rx) = mock(404).await;
        crate::handlers::builtin::mark_url_safe_for_test(&url).await;
        let err =
            handle_notify(ctx_with(json!({"provider": "discord", "url": url, "text": "x"})).await)
                .await
                .unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
        assert!(!format!("{err:?}").contains("/hook"));
    }

    #[tokio::test]
    async fn blocks_internal_webhook_urls() {
        let err = handle_notify(
            ctx_with(json!({"provider": "teams", "url": "http://169.254.169.254/x", "text": "x"}))
                .await,
        )
        .await
        .unwrap_err();
        assert!(format!("{err:?}").contains("blocked"));
    }
}
