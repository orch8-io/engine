//! Built-in `email` handler — send one email through SMTP, Resend, or AWS SES v2.
//!
//! Secrets (SMTP password, Resend API key, AWS keys) are never inlined in a
//! sequence: reference them with `credentials://<id>[/<field>]`, which the
//! dispatcher resolves from the tenant-scoped credential store before the
//! handler runs. Nothing secret is logged or echoed into the step output.
//!
//! `email` is registered as a side-effecting builtin, so every dispatch goes
//! through the effect ledger (`EffectGuard`): a crash after the provider may
//! have accepted the message leaves an *unknown* receipt that blocks an
//! automatic resend instead of silently mailing the recipient twice. Resend
//! additionally honours `idempotency_key` via its `Idempotency-Key` header.
//!
//! ## Params
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `provider` | string | **required** | `smtp`, `resend`, or `ses` |
//! | `from` | string | **required** | `"Name <addr@host>"` or bare address |
//! | `to` / `cc` / `bcc` | string \| string[] | — | Recipients (at least one across all three; max 50) |
//! | `reply_to` | string \| string[] | — | Reply-To address(es) |
//! | `subject` | string | **required** | Single line, no CR/LF |
//! | `text` / `html` | string | — | At least one body is required |
//! | `attachments` | array | `[]` | `{artifact, filename?, content_type?}` — `artifact` is a `blob_put` ref (key string, `{key}`, or `{artifact:{key}}`) owned by this instance |
//! | `idempotency_key` | string | — | Forwarded to providers that support it (Resend) |
//! | `timeout_ms` | u64 | 30000 | Per-request timeout (clamped to 1..=300000) |
//! | `smtp` | object | — | `{host, port?, username?, password?, tls?}`; `tls` is `starttls` (default, port 587), `implicit` (port 465) or `none` (no auth allowed) |
//! | `api_key` | string | — | Resend API key (`credentials://resend/api_key`) |
//! | `aws` | object | — | SES: `{access_key_id, secret_access_key, session_token?, region}` |
//! | `api_base_url` | string | provider default | Override the Resend/SES endpoint (SSRF-checked) |
//!
//! Output: `{provider, message_id, provider_receipt_id, recipients}` — the
//! recipient count only, never the addresses.

use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, KeyInit, Mac};
use lettre::message::header::ContentType;
use lettre::message::{Attachment, Mailbox, Message, MultiPart, SinglePart};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracing::debug;

use orch8_types::error::StepError;

use super::StepContext;

/// Upper bound on recipients across to/cc/bcc. Providers cap lower or equal
/// (SES: 50), and a runaway template shouldn't fan out to a mailing list.
const MAX_RECIPIENTS: usize = 50;
/// Total attachment bytes (before base64). Resend/SES cap raw messages at
/// ~40 MB; stay well below so base64 expansion never trips the provider.
const MAX_ATTACHMENT_BYTES: usize = 20 * 1024 * 1024;
const MAX_ATTACHMENTS: usize = 20;
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const MAX_ERROR_BODY_BYTES: usize = 512;
const RESEND_DEFAULT_BASE: &str = "https://api.resend.com";

fn permanent(message: impl Into<String>) -> StepError {
    StepError::Permanent {
        message: message.into(),
        details: None,
    }
}

fn retryable(message: impl Into<String>) -> StepError {
    StepError::Retryable {
        message: message.into(),
        details: None,
    }
}

/// Classify a non-2xx provider status: 408/429/5xx are transient, every
/// other 4xx is a permanent request error. Shared with `notify`.
pub(crate) fn status_error(context: &str, status: u16, body: &[u8]) -> StepError {
    let snippet_len = body.len().min(MAX_ERROR_BODY_BYTES);
    let snippet = String::from_utf8_lossy(&body[..snippet_len]);
    let message = format!("{context}: provider returned HTTP {status}: {snippet}");
    if status == 408 || status == 429 || status >= 500 {
        retryable(message)
    } else {
        permanent(message)
    }
}

/// Accept a single string or an array of strings.
fn string_list(params: &Value, key: &str) -> Result<Vec<String>, StepError> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::String(s)) if s.trim().is_empty() => Ok(Vec::new()),
        Some(Value::String(s)) => Ok(vec![s.clone()]),
        Some(Value::Array(items)) => items
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| permanent(format!("email: `{key}` entries must be strings")))
            })
            .collect(),
        Some(_) => Err(permanent(format!(
            "email: `{key}` must be a string or array of strings"
        ))),
    }
}

/// Parse and validate an address. `lettre`'s parser rejects CR/LF and other
/// header-injection payloads.
fn parse_mailbox(field: &str, raw: &str) -> Result<Mailbox, StepError> {
    raw.parse::<Mailbox>()
        .map_err(|e| permanent(format!("email: invalid `{field}` address: {e}")))
}

/// Everything a provider needs, validated once up front.
#[derive(Debug, Clone)]
struct EmailSpec {
    from: Mailbox,
    to: Vec<Mailbox>,
    cc: Vec<Mailbox>,
    bcc: Vec<Mailbox>,
    reply_to: Vec<Mailbox>,
    subject: String,
    text: Option<String>,
    html: Option<String>,
    attachments: Vec<ResolvedAttachment>,
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedAttachment {
    filename: String,
    content_type: String,
    bytes: Vec<u8>,
}

impl EmailSpec {
    fn recipient_count(&self) -> usize {
        self.to.len() + self.cc.len() + self.bcc.len()
    }

    /// Render the full RFC 5322 message (used by SMTP and SES raw).
    fn to_message(&self) -> Result<Message, StepError> {
        let mut builder = Message::builder()
            .from(self.from.clone())
            .subject(self.subject.clone());
        for m in &self.to {
            builder = builder.to(m.clone());
        }
        for m in &self.cc {
            builder = builder.cc(m.clone());
        }
        for m in &self.bcc {
            builder = builder.bcc(m.clone());
        }
        for m in &self.reply_to {
            builder = builder.reply_to(m.clone());
        }
        let body = match (&self.text, &self.html) {
            (Some(t), Some(h)) => MultiPart::alternative_plain_html(t.clone(), h.clone()),
            (Some(t), None) => MultiPart::mixed().singlepart(SinglePart::plain(t.clone())),
            (None, Some(h)) => MultiPart::mixed().singlepart(SinglePart::html(h.clone())),
            (None, None) => return Err(permanent("email: one of `text` or `html` is required")),
        };
        let body = if self.attachments.is_empty() {
            body
        } else {
            let mut mixed = MultiPart::mixed().multipart(body);
            for a in &self.attachments {
                let ct = ContentType::parse(&a.content_type).map_err(|e| {
                    permanent(format!("email: invalid attachment content_type: {e}"))
                })?;
                mixed = mixed.singlepart(Attachment::new(a.filename.clone()).body(a.bytes.clone(), ct));
            }
            mixed
        };
        builder
            .multipart(body)
            .map_err(|e| permanent(format!("email: failed to build message: {e}")))
    }
}

async fn resolve_attachments(ctx: &StepContext) -> Result<Vec<ResolvedAttachment>, StepError> {
    let Some(list) = ctx.params.get("attachments") else {
        return Ok(Vec::new());
    };
    let Some(items) = list.as_array() else {
        if list.is_null() {
            return Ok(Vec::new());
        }
        return Err(permanent("email: `attachments` must be an array"));
    };
    if items.len() > MAX_ATTACHMENTS {
        return Err(permanent(format!(
            "email: at most {MAX_ATTACHMENTS} attachments are allowed"
        )));
    }
    let mut out = Vec::with_capacity(items.len());
    let mut total = 0usize;
    for item in items {
        let refval = item.get("artifact").unwrap_or(item);
        let key = super::blob::extract_key(refval)
            .ok_or_else(|| permanent("email: attachment needs an `artifact` ref"))?;
        // IDOR guard: only this instance's own artifacts may be attached.
        if !super::tool_call::artifact_key_owned_by(&key, ctx.instance_id) {
            return Err(permanent(format!(
                "email: attachment '{key}' does not belong to this instance"
            )));
        }
        let bytes = ctx
            .storage
            .get_artifact(&key)
            .await
            .map_err(|e| super::blob::artifact_step_err("email attachment fetch", &e))?
            .ok_or_else(|| permanent(format!("email: attachment artifact not found: {key}")))?;
        total = total.saturating_add(bytes.len());
        if total > MAX_ATTACHMENT_BYTES {
            return Err(permanent(format!(
                "email: attachments exceed {MAX_ATTACHMENT_BYTES} bytes in total"
            )));
        }
        let content_type = item
            .get("content_type")
            .and_then(Value::as_str)
            .or_else(|| {
                refval
                    .get("artifact")
                    .unwrap_or(refval)
                    .get("content_type")
                    .and_then(Value::as_str)
            })
            .unwrap_or("application/octet-stream")
            .to_string();
        let filename = item
            .get("filename")
            .and_then(Value::as_str)
            .map_or_else(
                || key.rsplit('/').next().unwrap_or("attachment").to_string(),
                str::to_string,
            );
        if filename.chars().any(char::is_control) || filename.contains(['/', '\\']) {
            return Err(permanent("email: attachment filename contains invalid characters"));
        }
        out.push(ResolvedAttachment {
            filename,
            content_type,
            bytes,
        });
    }
    Ok(out)
}

async fn build_spec(ctx: &StepContext) -> Result<EmailSpec, StepError> {
    let p = &ctx.params;
    let from_raw = p
        .get("from")
        .and_then(Value::as_str)
        .ok_or_else(|| permanent("email: missing required param: from"))?;
    let from = parse_mailbox("from", from_raw)?;
    let parse_all = |field: &str| -> Result<Vec<Mailbox>, StepError> {
        string_list(p, field)?
            .iter()
            .map(|s| parse_mailbox(field, s))
            .collect()
    };
    let to = parse_all("to")?;
    let cc = parse_all("cc")?;
    let bcc = parse_all("bcc")?;
    let reply_to = parse_all("reply_to")?;
    let total = to.len() + cc.len() + bcc.len();
    if total == 0 {
        return Err(permanent("email: at least one of `to`, `cc`, `bcc` is required"));
    }
    if total > MAX_RECIPIENTS {
        return Err(permanent(format!(
            "email: {total} recipients exceeds the limit of {MAX_RECIPIENTS}"
        )));
    }
    let subject = p
        .get("subject")
        .and_then(Value::as_str)
        .ok_or_else(|| permanent("email: missing required param: subject"))?
        .to_string();
    if subject.contains(['\r', '\n']) {
        return Err(permanent("email: `subject` must be a single line"));
    }
    let text = p.get("text").and_then(Value::as_str).map(str::to_string);
    let html = p.get("html").and_then(Value::as_str).map(str::to_string);
    if text.is_none() && html.is_none() {
        return Err(permanent("email: one of `text` or `html` is required"));
    }
    let idempotency_key = p
        .get("idempotency_key")
        .and_then(Value::as_str)
        .filter(|k| !k.is_empty() && k.len() <= 256 && !k.chars().any(char::is_control))
        .map(str::to_string);
    let attachments = resolve_attachments(ctx).await?;
    Ok(EmailSpec {
        from,
        to,
        cc,
        bcc,
        reply_to,
        subject,
        text,
        html,
        attachments,
        idempotency_key,
    })
}

pub async fn handle_email(ctx: StepContext) -> Result<Value, StepError> {
    let provider = ctx
        .params
        .get("provider")
        .and_then(Value::as_str)
        .ok_or_else(|| permanent("email: missing required param: provider (smtp|resend|ses)"))?
        .to_ascii_lowercase();
    if !matches!(provider.as_str(), "smtp" | "resend" | "ses") {
        return Err(permanent(format!(
            "email: unsupported provider `{provider}`; expected smtp, resend, or ses"
        )));
    }
    let spec = build_spec(&ctx).await?;
    let timeout = crate::outbound::clamp_timeout_ms(
        ctx.params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_TIMEOUT_MS),
    );

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        provider = %provider,
        recipients = spec.recipient_count(),
        "email step"
    );

    // Validate the destination (SSRF) before the dry-run short-circuit so a
    // dry run flags a blocked endpoint instead of reporting a false green.
    let target = match provider.as_str() {
        "smtp" => Target::Smtp(SmtpTarget::from_params(&ctx.params)?),
        "resend" => {
            let base = api_base(&ctx.params, RESEND_DEFAULT_BASE)?;
            Target::Http(format!("{base}/emails"))
        }
        _ => {
            let aws = AwsCreds::from_params(&ctx.params)?;
            let default_base = format!("https://email.{}.amazonaws.com", aws.region);
            let base = api_base(&ctx.params, &default_base)?;
            Target::Ses(format!("{base}/v2/email/outbound-emails"), aws)
        }
    };
    match &target {
        Target::Smtp(s) => s.check_address().await?,
        Target::Http(url) | Target::Ses(url, _) => {
            if !super::builtin::is_url_safe(url).await {
                return Err(permanent(
                    "blocked: URL targets a private/internal network address",
                ));
            }
        }
    }

    if ctx.is_dry_run() {
        return Ok(json!({
            "dry_run": true,
            "provider": provider,
            "recipients": spec.recipient_count(),
        }));
    }

    let message_id = match target {
        Target::Smtp(s) => send_smtp(&s, &spec, timeout).await?,
        Target::Http(url) => send_resend(&ctx.params, &url, &spec, timeout).await?,
        Target::Ses(url, aws) => send_ses(&url, &aws, &spec, timeout).await?,
    };
    Ok(json!({
        "provider": provider,
        "message_id": message_id,
        "provider_receipt_id": message_id,
        "recipients": spec.recipient_count(),
    }))
}

enum Target {
    Smtp(SmtpTarget),
    Http(String),
    Ses(String, AwsCreds),
}

fn api_base(params: &Value, default: &str) -> Result<String, StepError> {
    let base = params
        .get("api_base_url")
        .and_then(Value::as_str)
        .unwrap_or(default)
        .trim_end_matches('/')
        .to_string();
    let parsed = url::Url::parse(&base)
        .map_err(|_| permanent("email: `api_base_url` is not a valid URL"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(permanent("email: `api_base_url` must be http(s)"));
    }
    Ok(base)
}

// ---------------------------------------------------------------------------
// Resend
// ---------------------------------------------------------------------------

async fn send_resend(
    params: &Value,
    url: &str,
    spec: &EmailSpec,
    timeout: Duration,
) -> Result<String, StepError> {
    let api_key = params
        .get("api_key")
        .and_then(Value::as_str)
        .filter(|k| !k.is_empty())
        .ok_or_else(|| permanent("email: resend requires `api_key` (use credentials://)"))?;
    let list = |v: &[Mailbox]| v.iter().map(ToString::to_string).collect::<Vec<_>>();
    let mut body = json!({
        "from": spec.from.to_string(),
        "to": list(&spec.to),
        "subject": spec.subject,
    });
    if !spec.cc.is_empty() {
        body["cc"] = json!(list(&spec.cc));
    }
    if !spec.bcc.is_empty() {
        body["bcc"] = json!(list(&spec.bcc));
    }
    if !spec.reply_to.is_empty() {
        body["reply_to"] = json!(list(&spec.reply_to));
    }
    if let Some(t) = &spec.text {
        body["text"] = json!(t);
    }
    if let Some(h) = &spec.html {
        body["html"] = json!(h);
    }
    if !spec.attachments.is_empty() {
        body["attachments"] = Value::Array(
            spec.attachments
                .iter()
                .map(|a| {
                    json!({
                        "filename": a.filename,
                        "content": STANDARD.encode(&a.bytes),
                        "content_type": a.content_type,
                    })
                })
                .collect(),
        );
    }
    let mut req = super::llm::http_client()
        .post(url)
        .bearer_auth(api_key)
        .timeout(timeout)
        .json(&body);
    if let Some(k) = &spec.idempotency_key {
        req = req.header("Idempotency-Key", k);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| retryable(format!("email: resend request failed: {}", crate::outbound::redact_error(&e))))?;
    let status = resp.status().as_u16();
    let bytes = crate::outbound::read_body_capped(resp, 64 * 1024)
        .await
        .map_err(|e| retryable(format!("email: resend response read failed: {e:?}")))?;
    if !(200..300).contains(&status) {
        return Err(status_error("email(resend)", status, &bytes));
    }
    let parsed: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    Ok(parsed
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string())
}

// ---------------------------------------------------------------------------
// AWS SES v2 (SigV4-signed HTTP API)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AwsCreds {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    region: String,
}

impl std::fmt::Debug for AwsCreds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsCreds")
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

impl AwsCreds {
    fn from_params(params: &Value) -> Result<Self, StepError> {
        let aws = params
            .get("aws")
            .ok_or_else(|| permanent("email: ses requires an `aws` object (use credentials://)"))?;
        let field = |k: &str| {
            aws.get(k)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        };
        let region = field("region").ok_or_else(|| permanent("email: ses requires `aws.region`"))?;
        // Region lands in the hostname — restrict to the AWS region alphabet.
        if !region
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(permanent("email: `aws.region` is not a valid AWS region"));
        }
        Ok(Self {
            access_key_id: field("access_key_id")
                .ok_or_else(|| permanent("email: ses requires `aws.access_key_id`"))?,
            secret_access_key: field("secret_access_key")
                .ok_or_else(|| permanent("email: ses requires `aws.secret_access_key`"))?,
            session_token: field("session_token"),
            region,
        })
    }
}

async fn send_ses(
    url: &str,
    aws: &AwsCreds,
    spec: &EmailSpec,
    timeout: Duration,
) -> Result<String, StepError> {
    let list = |v: &[Mailbox]| v.iter().map(ToString::to_string).collect::<Vec<_>>();
    // Raw content carries attachments and every header exactly as rendered;
    // Destination still lists every envelope recipient (Bcc is stripped from
    // the rendered headers).
    let raw = spec.to_message()?.formatted();
    let body = json!({
        "FromEmailAddress": spec.from.to_string(),
        "Destination": {
            "ToAddresses": list(&spec.to),
            "CcAddresses": list(&spec.cc),
            "BccAddresses": list(&spec.bcc),
        },
        "ReplyToAddresses": list(&spec.reply_to),
        "Content": { "Raw": { "Data": STANDARD.encode(&raw) } },
    });
    let payload = serde_json::to_vec(&body).map_err(|e| permanent(format!("email: {e}")))?;
    let parsed = url::Url::parse(url).map_err(|_| permanent("email: invalid SES endpoint"))?;
    let host = match (parsed.host_str(), parsed.port()) {
        (Some(h), Some(p)) => format!("{h}:{p}"),
        (Some(h), None) => h.to_string(),
        _ => return Err(permanent("email: SES endpoint has no host")),
    };
    let amz_date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let payload_hash = hex(&Sha256::digest(&payload));
    let mut headers: Vec<(String, String)> = vec![
        ("content-type".into(), "application/json".into()),
        ("host".into(), host),
        ("x-amz-content-sha256".into(), payload_hash.clone()),
        ("x-amz-date".into(), amz_date.clone()),
    ];
    if let Some(t) = &aws.session_token {
        headers.push(("x-amz-security-token".into(), t.clone()));
    }
    let authorization = sigv4_authorization(&SigV4Request {
        method: "POST",
        path: parsed.path(),
        query: "",
        headers: &headers,
        payload_hash: &payload_hash,
        region: &aws.region,
        service: "ses",
        access_key_id: &aws.access_key_id,
        secret_access_key: &aws.secret_access_key,
        amz_date: &amz_date,
    });
    let mut req = super::llm::http_client()
        .post(url)
        .timeout(timeout)
        .header("Authorization", authorization)
        .body(payload);
    for (k, v) in &headers {
        if k != "host" {
            req = req.header(k.as_str(), v.as_str());
        }
    }
    let resp = req
        .send()
        .await
        .map_err(|e| retryable(format!("email: ses request failed: {}", crate::outbound::redact_error(&e))))?;
    let status = resp.status().as_u16();
    let bytes = crate::outbound::read_body_capped(resp, 64 * 1024)
        .await
        .map_err(|e| retryable(format!("email: ses response read failed: {e:?}")))?;
    if !(200..300).contains(&status) {
        return Err(status_error("email(ses)", status, &bytes));
    }
    let parsed: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    Ok(parsed
        .get("MessageId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .unwrap_or_else(|_| unreachable!("HMAC-SHA256 accepts keys of any length"));
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

struct SigV4Request<'a> {
    method: &'a str,
    path: &'a str,
    query: &'a str,
    /// Lowercase header names; every header listed here is signed.
    headers: &'a [(String, String)],
    payload_hash: &'a str,
    region: &'a str,
    service: &'a str,
    access_key_id: &'a str,
    secret_access_key: &'a str,
    /// `YYYYMMDD'T'HHMMSS'Z'`.
    amz_date: &'a str,
}

/// AWS Signature Version 4 `Authorization` header value.
fn sigv4_authorization(r: &SigV4Request<'_>) -> String {
    let mut headers: Vec<(String, String)> = r
        .headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    headers.sort();
    let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let signed_headers = headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        r.method,
        if r.path.is_empty() { "/" } else { r.path },
        r.query,
        canonical_headers,
        signed_headers,
        r.payload_hash
    );
    let date = &r.amz_date[..8];
    let scope = format!("{date}/{}/{}/aws4_request", r.region, r.service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        r.amz_date,
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );
    let k_date = hmac_sha256(format!("AWS4{}", r.secret_access_key).as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, r.region.as_bytes());
    let k_service = hmac_sha256(&k_region, r.service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        r.access_key_id
    )
}

// ---------------------------------------------------------------------------
// SMTP (lettre + rustls)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SmtpTls {
    StartTls,
    Implicit,
    None,
}

struct SmtpTarget {
    host: String,
    port: u16,
    tls: SmtpTls,
    username: Option<String>,
    password: Option<String>,
    /// Address vetted by [`Self::check_address`]; the connection is made to
    /// this exact IP (TLS still verifies `host`) so DNS cannot be rebound to
    /// an internal address between the check and the connect.
    resolved: std::sync::Mutex<Option<std::net::SocketAddr>>,
}

impl SmtpTarget {
    fn from_params(params: &Value) -> Result<Self, StepError> {
        let smtp = params
            .get("smtp")
            .ok_or_else(|| permanent("email: smtp requires an `smtp` object (use credentials://)"))?;
        let host = smtp
            .get("host")
            .and_then(Value::as_str)
            .filter(|h| !h.is_empty())
            .ok_or_else(|| permanent("email: `smtp.host` is required"))?
            .to_string();
        if host.contains(['/', '@', ' ', ':']) && host.parse::<std::net::IpAddr>().is_err() {
            return Err(permanent("email: `smtp.host` must be a bare hostname or IP"));
        }
        let tls = match smtp.get("tls").and_then(Value::as_str).unwrap_or("starttls") {
            "starttls" => SmtpTls::StartTls,
            "implicit" | "tls" | "ssl" => SmtpTls::Implicit,
            "none" => SmtpTls::None,
            other => {
                return Err(permanent(format!(
                    "email: `smtp.tls` must be starttls, implicit, or none (got `{other}`)"
                )));
            }
        };
        let default_port = match tls {
            SmtpTls::Implicit => 465,
            SmtpTls::StartTls => 587,
            SmtpTls::None => 25,
        };
        let port = match smtp.get("port").and_then(Value::as_u64) {
            None => default_port,
            Some(p) => u16::try_from(p)
                .ok()
                .filter(|p| *p != 0)
                .ok_or_else(|| permanent("email: `smtp.port` out of range"))?,
        };
        let username = smtp
            .get("username")
            .and_then(Value::as_str)
            .map(str::to_string);
        let password = smtp
            .get("password")
            .and_then(Value::as_str)
            .map(str::to_string);
        if tls == SmtpTls::None && (username.is_some() || password.is_some()) {
            return Err(permanent(
                "email: refusing to send SMTP credentials over an unencrypted connection (smtp.tls = none)",
            ));
        }
        Ok(Self {
            host,
            port,
            tls,
            username,
            password,
            resolved: std::sync::Mutex::new(None),
        })
    }

    /// SSRF guard: resolve once, drop private/internal/metadata addresses,
    /// pin the first remaining address for the connection.
    async fn check_address(&self) -> Result<(), StepError> {
        let allow_internal = super::builtin::internal_urls_allowed();
        let addrs = tokio::net::lookup_host((self.host.as_str(), self.port))
            .await
            .map_err(|e| retryable(format!("email: cannot resolve SMTP host: {e}")))?;
        let mut chosen = None;
        for sa in addrs {
            let blocked = match sa.ip() {
                std::net::IpAddr::V4(v4) => orch8_types::net::is_non_public_ipv4(v4),
                std::net::IpAddr::V6(v6) => orch8_types::net::is_non_public_ipv6(v6),
            };
            if blocked && !allow_internal && !(smtp_test_allow_loopback() && sa.ip().is_loopback()) {
                return Err(permanent(
                    "blocked: SMTP host resolves to a private/internal network address",
                ));
            }
            chosen.get_or_insert(sa);
        }
        let chosen = chosen.ok_or_else(|| retryable("email: SMTP host resolved to no addresses"))?;
        if let Ok(mut slot) = self.resolved.lock() {
            *slot = Some(chosen);
        }
        Ok(())
    }
}

#[cfg(test)]
static SMTP_TEST_ALLOW_LOOPBACK: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
fn smtp_test_allow_loopback() -> bool {
    SMTP_TEST_ALLOW_LOOPBACK.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(not(test))]
const fn smtp_test_allow_loopback() -> bool {
    false
}

async fn send_smtp(
    target: &SmtpTarget,
    spec: &EmailSpec,
    timeout: Duration,
) -> Result<String, StepError> {
    use lettre::transport::smtp::authentication::Credentials;
    use lettre::transport::smtp::client::{Tls, TlsParameters};
    use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};

    let addr = target
        .resolved
        .lock()
        .ok()
        .and_then(|g| *g)
        .ok_or_else(|| permanent("email: SMTP address was not validated"))?;
    let tls = match target.tls {
        SmtpTls::None => Tls::None,
        mode => {
            let params = TlsParameters::new(target.host.clone())
                .map_err(|e| permanent(format!("email: invalid SMTP TLS parameters: {e}")))?;
            if mode == SmtpTls::Implicit {
                Tls::Wrapper(params)
            } else {
                Tls::Required(params)
            }
        }
    };
    let mut builder = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(addr.ip().to_string())
        .port(addr.port())
        .tls(tls)
        .timeout(Some(timeout));
    if let (Some(u), Some(p)) = (&target.username, &target.password) {
        builder = builder.credentials(Credentials::new(u.clone(), p.clone()));
    }
    let transport = builder.build();
    let message = spec.to_message()?;
    match transport.send(message).await {
        Ok(resp) => Ok(resp.message().collect::<Vec<_>>().join(" ")),
        Err(e) if e.is_permanent() => Err(permanent(format!("email: SMTP rejected message: {e}"))),
        Err(e) => Err(retryable(format!("email: SMTP send failed: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, TenantId};
    use std::sync::Arc;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    async fn ctx_with(params: Value) -> StepContext {
        let storage: Arc<dyn StorageBackend> = Arc::new(
            SqliteStorage::in_memory()
                .await
                .unwrap()
                .with_artifact_store(Arc::new(orch8_storage::artifacts::ObjectArtifactStore::memory())),
        );
        StepContext {
            instance_id: InstanceId::new(),
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("mail"),
            params,
            context: Arc::new(ExecutionContext::default()),
            attempt: 0,
            storage,
            wait_for_input: None,
        }
    }

    /// One-shot HTTP mock: captures the raw request, replies with `status` + `reply`.
    async fn mock_http(status: u16, reply: &'static str) -> (String, tokio::sync::oneshot::Receiver<String>) {
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
                let text = String::from_utf8_lossy(&buf);
                if let Some(idx) = text.find("\r\n\r\n") {
                    let len = text[..idx]
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    if buf.len() >= idx + 4 + len {
                        break;
                    }
                }
            }
            let out = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                reply.len()
            );
            let _ = sock.write_all(out.as_bytes()).await;
            let _ = tx.send(String::from_utf8_lossy(&buf).to_string());
        });
        (format!("http://127.0.0.1:{}", addr.port()), rx)
    }

    #[test]
    fn sigv4_matches_aws_get_vanilla_vector() {
        // AWS SigV4 test suite: get-vanilla.
        let headers = vec![
            ("host".to_string(), "example.amazonaws.com".to_string()),
            ("x-amz-date".to_string(), "20150830T123600Z".to_string()),
        ];
        let auth = sigv4_authorization(&SigV4Request {
            method: "GET",
            path: "/",
            query: "",
            headers: &headers,
            payload_hash: &hex(&Sha256::digest(b"")),
            region: "us-east-1",
            service: "service",
            access_key_id: "AKIDEXAMPLE",
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            amz_date: "20150830T123600Z",
        });
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-date, \
             Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    #[tokio::test]
    async fn rejects_header_injection_in_addresses_and_subject() {
        let ctx = ctx_with(json!({
            "provider": "resend", "api_key": "k",
            "from": "a@example.com", "to": "b@example.com\r\nBcc: evil@example.com",
            "subject": "hi", "text": "x",
        }))
        .await;
        assert!(matches!(handle_email(ctx).await, Err(StepError::Permanent { .. })));
        let ctx = ctx_with(json!({
            "provider": "resend", "api_key": "k",
            "from": "a@example.com", "to": "b@example.com",
            "subject": "hi\r\nBcc: evil@example.com", "text": "x",
        }))
        .await;
        assert!(matches!(handle_email(ctx).await, Err(StepError::Permanent { .. })));
    }

    #[tokio::test]
    async fn requires_body_and_recipient() {
        let ctx = ctx_with(json!({"provider":"resend","from":"a@example.com","subject":"s","text":"t"})).await;
        assert!(handle_email(ctx).await.is_err());
        let ctx = ctx_with(json!({"provider":"resend","from":"a@example.com","to":"b@example.com","subject":"s"})).await;
        assert!(handle_email(ctx).await.is_err());
    }

    #[tokio::test]
    async fn blocks_private_api_base() {
        let ctx = ctx_with(json!({
            "provider": "resend", "api_key": "k", "api_base_url": "http://169.254.169.254",
            "from": "a@example.com", "to": "b@example.com", "subject": "s", "text": "t",
        }))
        .await;
        let err = handle_email(ctx).await.unwrap_err();
        assert!(format!("{err:?}").contains("blocked"));
    }

    #[tokio::test]
    async fn resend_sends_expected_payload_with_attachment() {
        let (base, rx) = mock_http(200, r#"{"id":"re_123"}"#).await;
        crate::handlers::builtin::mark_url_safe_for_test(&format!("{base}/emails")).await;
        let mut ctx = ctx_with(json!({})).await;
        let aref = ctx
            .storage
            .put_artifact(ctx.instance_id, "text/plain", bytes::Bytes::from_static(b"hello-attachment"))
            .await
            .unwrap();
        ctx.params = json!({
            "provider": "resend", "api_key": "re_secret", "api_base_url": base,
            "from": "Acme <no-reply@acme.test>", "to": ["b@example.com"], "cc": "c@example.com",
            "reply_to": "support@acme.test", "subject": "Welcome", "text": "hi", "html": "<b>hi</b>",
            "attachments": [{"artifact": aref, "filename": "note.txt"}],
            "idempotency_key": "welcome-42",
        });
        let out = handle_email(ctx).await.unwrap();
        assert_eq!(out["message_id"], "re_123");
        assert_eq!(out["provider_receipt_id"], "re_123");
        assert_eq!(out["recipients"], 2);
        assert!(out.to_string().find("re_secret").is_none(), "secret must not leak");
        let raw = rx.await.unwrap();
        assert!(raw.contains("authorization: Bearer re_secret") || raw.contains("Authorization: Bearer re_secret"));
        assert!(raw.to_ascii_lowercase().contains("idempotency-key: welcome-42"));
        let body: Value = serde_json::from_str(&raw[raw.find("\r\n\r\n").unwrap() + 4..]).unwrap();
        assert_eq!(body["to"][0], "b@example.com");
        assert_eq!(body["cc"][0], "c@example.com");
        assert_eq!(body["attachments"][0]["filename"], "note.txt");
        assert_eq!(body["attachments"][0]["content"], STANDARD.encode("hello-attachment"));
    }

    #[tokio::test]
    async fn resend_4xx_is_permanent_5xx_retryable() {
        let (base, _rx) = mock_http(422, r#"{"message":"bad"}"#).await;
        crate::handlers::builtin::mark_url_safe_for_test(&format!("{base}/emails")).await;
        let ctx = ctx_with(json!({
            "provider": "resend", "api_key": "k", "api_base_url": base,
            "from": "a@example.com", "to": "b@example.com", "subject": "s", "text": "t",
        }))
        .await;
        assert!(matches!(handle_email(ctx).await, Err(StepError::Permanent { .. })));
        let (base, _rx) = mock_http(503, "{}").await;
        crate::handlers::builtin::mark_url_safe_for_test(&format!("{base}/emails")).await;
        let ctx = ctx_with(json!({
            "provider": "resend", "api_key": "k", "api_base_url": base,
            "from": "a@example.com", "to": "b@example.com", "subject": "s", "text": "t",
        }))
        .await;
        assert!(matches!(handle_email(ctx).await, Err(StepError::Retryable { .. })));
    }

    #[tokio::test]
    async fn attachment_from_other_instance_is_rejected() {
        let ctx = ctx_with(json!({
            "provider": "resend", "api_key": "k",
            "from": "a@example.com", "to": "b@example.com", "subject": "s", "text": "t",
            "attachments": [{"artifact": {"key": "00000000-0000-0000-0000-000000000000/x"}}],
        }))
        .await;
        let err = handle_email(ctx).await.unwrap_err();
        assert!(format!("{err:?}").contains("does not belong"));
    }

    #[tokio::test]
    async fn ses_request_is_sigv4_signed_raw_message() {
        let (base, rx) = mock_http(200, r#"{"MessageId":"ses-1"}"#).await;
        crate::handlers::builtin::mark_url_safe_for_test(&format!("{base}/v2/email/outbound-emails")).await;
        let ctx = ctx_with(json!({
            "provider": "ses", "api_base_url": base,
            "aws": {"access_key_id": "AKID", "secret_access_key": "sekret", "region": "eu-west-1"},
            "from": "a@example.com", "to": "b@example.com", "bcc": "hidden@example.com",
            "subject": "s", "text": "t",
        }))
        .await;
        let out = handle_email(ctx).await.unwrap();
        assert_eq!(out["message_id"], "ses-1");
        let raw = rx.await.unwrap();
        let lower = raw.to_ascii_lowercase();
        assert!(lower.contains("authorization: aws4-hmac-sha256 credential=akid/"));
        assert!(lower.contains("/eu-west-1/ses/aws4_request"));
        assert!(!raw.contains("sekret"));
        let body: Value = serde_json::from_str(&raw[raw.find("\r\n\r\n").unwrap() + 4..]).unwrap();
        assert_eq!(body["Destination"]["BccAddresses"][0], "hidden@example.com");
        let mime = String::from_utf8(STANDARD.decode(body["Content"]["Raw"]["Data"].as_str().unwrap()).unwrap()).unwrap();
        assert!(mime.contains("Subject: s"));
        assert!(!mime.contains("hidden@example.com"), "Bcc must not appear in headers");
    }

    #[tokio::test]
    async fn smtp_refuses_credentials_without_tls_and_blocks_private_hosts() {
        let ctx = ctx_with(json!({
            "provider": "smtp", "smtp": {"host": "mail.example.com", "tls": "none", "username": "u", "password": "p"},
            "from": "a@example.com", "to": "b@example.com", "subject": "s", "text": "t",
        }))
        .await;
        assert!(format!("{:?}", handle_email(ctx).await.unwrap_err()).contains("unencrypted"));
        let ctx = ctx_with(json!({
            "provider": "smtp", "smtp": {"host": "169.254.169.254", "port": 2525},
            "from": "a@example.com", "to": "b@example.com", "subject": "s", "text": "t",
        }))
        .await;
        assert!(format!("{:?}", handle_email(ctx).await.unwrap_err()).contains("blocked"));
    }

    #[tokio::test]
    async fn smtp_delivers_to_mock_server() {
        SMTP_TEST_ALLOW_LOOPBACK.store(true, std::sync::atomic::Ordering::Relaxed);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let (r, mut w) = sock.into_split();
            let mut r = BufReader::new(r);
            w.write_all(b"220 mock ESMTP\r\n").await.unwrap();
            let mut transcript = String::new();
            let mut in_data = false;
            loop {
                let mut line = String::new();
                if r.read_line(&mut line).await.unwrap() == 0 {
                    break;
                }
                transcript.push_str(&line);
                if in_data {
                    if line == ".\r\n" {
                        in_data = false;
                        w.write_all(b"250 2.0.0 Ok: queued as MOCK1\r\n").await.unwrap();
                    }
                    continue;
                }
                let upper = line.to_ascii_uppercase();
                if upper.starts_with("EHLO") {
                    w.write_all(b"250-mock\r\n250 8BITMIME\r\n").await.unwrap();
                } else if upper.starts_with("DATA") {
                    in_data = true;
                    w.write_all(b"354 go\r\n").await.unwrap();
                } else if upper.starts_with("QUIT") {
                    w.write_all(b"221 bye\r\n").await.unwrap();
                    break;
                } else {
                    w.write_all(b"250 ok\r\n").await.unwrap();
                }
            }
            let _ = tx.send(transcript);
        });
        let ctx = ctx_with(json!({
            "provider": "smtp", "smtp": {"host": "127.0.0.1", "port": port, "tls": "none"},
            "from": "a@example.com", "to": "b@example.com", "bcc": "c@example.com",
            "subject": "Hello", "text": "body text",
        }))
        .await;
        let out = handle_email(ctx).await.unwrap();
        assert!(out["message_id"].as_str().unwrap().contains("MOCK1"));
        let transcript = rx.await.unwrap();
        assert!(transcript.contains("RCPT TO:<b@example.com>"));
        assert!(transcript.contains("RCPT TO:<c@example.com>"));
        assert!(transcript.contains("Subject: Hello"));
    }

    #[tokio::test]
    async fn dry_run_validates_without_sending() {
        let mut ctx = ctx_with(json!({
            "provider": "resend", "api_key": "k", "api_base_url": "https://api.resend.com",
            "from": "a@example.com", "to": "b@example.com", "subject": "s", "text": "t",
        }))
        .await;
        crate::handlers::builtin::mark_url_safe_for_test("https://api.resend.com/emails").await;
        Arc::make_mut(&mut ctx.context).runtime.dry_run = true;
        let out = handle_email(ctx).await.unwrap();
        assert_eq!(out["dry_run"], true);
    }
}
