//! AWS SQS trigger (`trigger_type: "sqs"`, engine feature `sqs`).
//!
//! Talks to SQS over its JSON protocol (`X-Amz-Target: AmazonSQS.*`) with
//! a small built-in `SigV4` signer on the engine's existing `reqwest` client,
//! so the feature adds no new crates. Long-polls `ReceiveMessage`, creates one
//! instance per message (idempotency key = SQS `MessageId`) and deletes the
//! message only after the instance is durably created. A failed create leaves
//! the message in flight; SQS redelivers it after its visibility timeout
//! (and moves it to the queue's own redrive DLQ after `maxReceiveCount`).
//! SQS coordinates consumers itself, so every engine node may poll.
//!
//! ```json
//! {
//!   "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/orders",
//!   "region": "us-east-1",                        // optional, parsed from the URL
//!   "access_key_id": "credentials://aws/access_key_id",     // optional,
//!   "secret_access_key": "credentials://aws/secret_access_key", // falls back to
//!   "session_token": null,                        // AWS_* environment variables
//!   "wait_time_seconds": 20,
//!   "max_messages": 10,
//!   "visibility_timeout": 60                      // optional
//! }
//! ```

use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{opt_str, opt_u64, req_str, require_object};

/// Static AWS credentials.
#[derive(Clone, PartialEq, Eq)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for AwsCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsCredentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Validated `sqs` trigger config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqsConfig {
    pub queue_url: String,
    /// `scheme://host[:port]` the API calls are sent to.
    pub endpoint: String,
    pub host: String,
    pub region: String,
    pub credentials: Option<AwsCredentials>,
    pub wait_time_seconds: u64,
    pub max_messages: u64,
    pub visibility_timeout: Option<u64>,
}

impl SqsConfig {
    pub fn parse(config: &Value) -> Result<Self, String> {
        require_object(config, "sqs")?;
        let queue_url = req_str(config, "queue_url")?;
        let url =
            url::Url::parse(&queue_url).map_err(|e| format!("'queue_url' is invalid: {e}"))?;
        if !matches!(url.scheme(), "https" | "http") {
            return Err("'queue_url' must be http(s)".into());
        }
        let host_only = url
            .host_str()
            .ok_or_else(|| "'queue_url' has no host".to_string())?
            .to_string();
        let host = match url.port() {
            Some(p) => format!("{host_only}:{p}"),
            None => host_only.clone(),
        };
        let endpoint = format!("{}://{host}", url.scheme());
        let region = match opt_str(config, "region")? {
            Some(r) => r,
            None => region_from_host(&host_only).ok_or_else(|| {
                "'region' is required when it cannot be parsed from queue_url".to_string()
            })?,
        };
        let credentials = match (
            opt_str(config, "access_key_id")?,
            opt_str(config, "secret_access_key")?,
        ) {
            (Some(a), Some(s)) => Some(AwsCredentials {
                access_key_id: a,
                secret_access_key: s,
                session_token: opt_str(config, "session_token")?,
            }),
            (None, None) => None,
            _ => {
                return Err("'access_key_id' and 'secret_access_key' must be set together".into());
            }
        };
        Ok(Self {
            queue_url,
            endpoint,
            host,
            region,
            credentials,
            wait_time_seconds: opt_u64(config, "wait_time_seconds", 20, 0, 20)?,
            max_messages: opt_u64(config, "max_messages", 10, 1, 10)?,
            visibility_timeout: match config.get("visibility_timeout") {
                None | Some(Value::Null) => None,
                Some(_) => Some(opt_u64(config, "visibility_timeout", 30, 0, 43_200)?),
            },
        })
    }

    /// Credentials from the config, else from the standard AWS environment
    /// variables.
    #[must_use]
    pub fn effective_credentials(&self) -> Option<AwsCredentials> {
        self.credentials.clone().or_else(|| {
            let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").ok()?;
            let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok()?;
            Some(AwsCredentials {
                access_key_id,
                secret_access_key,
                session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
            })
        })
    }
}

/// `sqs.<region>.amazonaws.com` / `<region>.queue.amazonaws.com` → region.
fn region_from_host(host: &str) -> Option<String> {
    let parts: Vec<&str> = host.split('.').collect();
    match parts.as_slice() {
        ["sqs", region, "amazonaws", ..] | [region, "queue", "amazonaws", ..] => {
            Some((*region).to_string())
        }
        _ => None,
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .unwrap_or_else(|_| unreachable!("HMAC accepts keys of any length"));
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// A request to sign with AWS Signature Version 4.
pub struct SigV4Request<'a> {
    pub method: &'a str,
    pub path: &'a str,
    /// Already-canonical query string (sorted, URI-encoded), may be empty.
    pub query: &'a str,
    /// Headers to sign: lowercase names; `host` and `x-amz-date` must be present.
    pub headers: &'a [(&'a str, &'a str)],
    pub body: &'a [u8],
    pub region: &'a str,
    pub service: &'a str,
    pub time: DateTime<Utc>,
}

/// Compute the `Authorization` header value for a request.
#[must_use]
pub fn sigv4_authorization(req: &SigV4Request<'_>, creds: &AwsCredentials) -> String {
    let amz_date = req.time.format("%Y%m%dT%H%M%SZ").to_string();
    let date = req.time.format("%Y%m%d").to_string();
    let mut headers: Vec<(String, String)> = req
        .headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    headers.sort();
    let canonical_headers = headers.iter().fold(String::new(), |mut acc, (k, v)| {
        use std::fmt::Write as _;
        let _ = writeln!(acc, "{k}:{v}");
        acc
    });
    let signed_headers = headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        req.method,
        req.path,
        req.query,
        canonical_headers,
        signed_headers,
        hex(&Sha256::digest(req.body))
    );
    let scope = format!("{date}/{}/{}/aws4_request", req.region, req.service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );
    let k_date = hmac(
        format!("AWS4{}", creds.secret_access_key).as_bytes(),
        date.as_bytes(),
    );
    let k_region = hmac(&k_date, req.region.as_bytes());
    let k_service = hmac(&k_region, req.service.as_bytes());
    let k_signing = hmac(&k_service, b"aws4_request");
    let signature = hex(&hmac(&k_signing, string_to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        creds.access_key_id
    )
}

/// Map one SQS message (JSON protocol shape) to `(data, meta, source_id)`.
/// Returns `None` when the message lacks the id or receipt handle.
#[must_use]
pub fn map_message(queue_url: &str, message: &Value) -> Option<(Value, Value, String)> {
    let message_id = message.get("MessageId")?.as_str()?.to_string();
    message.get("ReceiptHandle")?.as_str()?;
    let body = message.get("Body").and_then(Value::as_str).unwrap_or("");
    let data = super::decode_payload(body.as_bytes());
    let meta = json!({
        "queue_url": queue_url,
        "message_id": message_id,
        "attributes": message.get("Attributes").cloned().unwrap_or(Value::Null),
        "message_attributes": message.get("MessageAttributes").cloned().unwrap_or(Value::Null),
    });
    Some((data, meta, message_id))
}

#[cfg(feature = "sqs")]
pub use listener::run;

#[cfg(feature = "sqs")]
mod listener {
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;
    use tracing::{error, info, warn};

    use orch8_storage::StorageBackend;
    use orch8_types::trigger::TriggerDef;

    use super::{AwsCredentials, SigV4Request, SqsConfig, sigv4_authorization};
    use crate::error::EngineError;
    use crate::trigger_sources::{deliver, failure_backoff, resolved_config, sleep_or_cancel};

    const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

    struct SqsClient {
        http: reqwest::Client,
        cfg: SqsConfig,
        creds: AwsCredentials,
    }

    impl SqsClient {
        async fn call(&self, target: &str, body: &Value) -> Result<Value, String> {
            let payload = serde_json::to_vec(body).map_err(|e| e.to_string())?;
            let now = chrono::Utc::now();
            let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
            let target_header = format!("AmazonSQS.{target}");
            let mut signed: Vec<(&str, &str)> = vec![
                ("content-type", "application/x-amz-json-1.0"),
                ("host", self.cfg.host.as_str()),
                ("x-amz-date", amz_date.as_str()),
                ("x-amz-target", target_header.as_str()),
            ];
            if let Some(token) = &self.creds.session_token {
                signed.push(("x-amz-security-token", token.as_str()));
            }
            let auth = sigv4_authorization(
                &SigV4Request {
                    method: "POST",
                    path: "/",
                    query: "",
                    headers: &signed,
                    body: &payload,
                    region: &self.cfg.region,
                    service: "sqs",
                    time: now,
                },
                &self.creds,
            );
            let mut req = self
                .http
                .post(format!("{}/", self.cfg.endpoint))
                .header("content-type", "application/x-amz-json-1.0")
                .header("x-amz-date", &amz_date)
                .header("x-amz-target", &target_header)
                .header("authorization", auth);
            if let Some(token) = &self.creds.session_token {
                req = req.header("x-amz-security-token", token);
            }
            let resp = req.body(payload).send().await.map_err(|e| e.to_string())?;
            let status = resp.status();
            let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
            if bytes.len() > MAX_RESPONSE_BYTES {
                return Err("SQS response too large".into());
            }
            if !status.is_success() {
                let text = String::from_utf8_lossy(&bytes);
                let preview: String = text.chars().take(300).collect();
                return Err(format!("SQS {target} returned {status}: {preview}"));
            }
            if bytes.is_empty() {
                return Ok(Value::Null);
            }
            serde_json::from_slice(&bytes).map_err(|e| format!("SQS {target} bad JSON: {e}"))
        }
    }

    /// Run the SQS listener until cancelled.
    pub async fn run(
        storage: Arc<dyn StorageBackend>,
        trigger: TriggerDef,
        cancel: CancellationToken,
    ) -> Result<(), EngineError> {
        let config = resolved_config(storage.as_ref(), &trigger).await?;
        let cfg = SqsConfig::parse(&config)
            .map_err(|e| EngineError::InvalidConfig(format!("sqs: {e}")))?;
        let creds = cfg.effective_credentials().ok_or_else(|| {
            EngineError::InvalidConfig(
                "sqs: no credentials (set access_key_id/secret_access_key or AWS_* env vars)"
                    .into(),
            )
        })?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(cfg.wait_time_seconds + 15))
            .build()
            .map_err(|e| EngineError::InvalidConfig(format!("sqs http client: {e}")))?;
        let client = SqsClient { http, cfg, creds };
        let slug = trigger.slug.clone();
        info!(slug, queue = %client.cfg.queue_url, "sqs trigger listener active");

        let mut failures = 0u32;
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let mut receive = json!({
                "QueueUrl": client.cfg.queue_url,
                "MaxNumberOfMessages": client.cfg.max_messages,
                "WaitTimeSeconds": client.cfg.wait_time_seconds,
                "MessageAttributeNames": ["All"],
                "MessageSystemAttributeNames": ["All"],
            });
            if let Some(v) = client.cfg.visibility_timeout {
                receive["VisibilityTimeout"] = json!(v);
            }
            let received = tokio::select! {
                () = cancel.cancelled() => return Ok(()),
                r = client.call("ReceiveMessage", &receive) => r,
            };
            let messages = match received {
                Ok(v) => {
                    failures = 0;
                    v.get("Messages")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default()
                }
                Err(e) => {
                    failures = failures.saturating_add(1);
                    warn!(slug, error = %e, "sqs receive failed");
                    if sleep_or_cancel(&cancel, failure_backoff(failures)).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            let mut to_delete = Vec::new();
            for (i, msg) in messages.iter().enumerate() {
                let Some((data, meta, message_id)) = super::map_message(&client.cfg.queue_url, msg)
                else {
                    warn!(
                        slug,
                        "sqs message without MessageId/ReceiptHandle, skipping"
                    );
                    continue;
                };
                match deliver(storage.as_ref(), &trigger, data, meta, &message_id).await {
                    Ok(_) => to_delete.push(json!({
                        "Id": i.to_string(),
                        "ReceiptHandle": msg["ReceiptHandle"],
                    })),
                    // Not deleted → SQS redelivers after the visibility timeout.
                    Err(e) => {
                        error!(slug, message_id, error = %e, "sqs message not delivered; left for redelivery");
                    }
                }
            }
            if !to_delete.is_empty() {
                let body = json!({"QueueUrl": client.cfg.queue_url, "Entries": to_delete});
                match client.call("DeleteMessageBatch", &body).await {
                    Ok(resp) => {
                        if let Some(failed) = resp.get("Failed").and_then(Value::as_array)
                            && !failed.is_empty()
                        {
                            // Redelivery of these is deduplicated by MessageId.
                            warn!(slug, failed = failed.len(), "sqs delete partially failed");
                        }
                    }
                    Err(e) => {
                        warn!(slug, error = %e, "sqs delete failed; redelivery will be deduplicated");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_config_and_derives_endpoint_and_region() {
        let c = SqsConfig::parse(&json!({
            "queue_url": "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"
        }))
        .unwrap();
        assert_eq!(c.region, "eu-west-1");
        assert_eq!(c.endpoint, "https://sqs.eu-west-1.amazonaws.com");
        assert_eq!(c.host, "sqs.eu-west-1.amazonaws.com");
        assert_eq!(c.wait_time_seconds, 20);
        assert_eq!(c.max_messages, 10);
        assert!(c.credentials.is_none());

        let legacy = SqsConfig::parse(&json!({
            "queue_url": "https://us-east-2.queue.amazonaws.com/1/q"
        }))
        .unwrap();
        assert_eq!(legacy.region, "us-east-2");

        let local = SqsConfig::parse(&json!({
            "queue_url": "http://localhost:4566/000000000000/q",
            "region": "us-east-1",
            "access_key_id": "test", "secret_access_key": "s3cr3t-value",
            "wait_time_seconds": 1, "max_messages": 5, "visibility_timeout": 30
        }))
        .unwrap();
        assert_eq!(local.endpoint, "http://localhost:4566");
        assert_eq!(local.host, "localhost:4566");
        assert_eq!(local.visibility_timeout, Some(30));
        assert!(local.credentials.is_some());
        assert!(!format!("{:?}", local.credentials).contains("s3cr3t-value"));
    }

    #[test]
    fn rejects_invalid_configs() {
        for bad in [
            json!({}),
            json!({"queue_url": "not a url"}),
            json!({"queue_url": "ftp://x/1/q"}),
            json!({"queue_url": "http://localhost:4566/0/q"}),
            json!({"queue_url": "https://sqs.us-east-1.amazonaws.com/1/q", "access_key_id": "a"}),
            json!({"queue_url": "https://sqs.us-east-1.amazonaws.com/1/q", "max_messages": 11}),
            json!({"queue_url": "https://sqs.us-east-1.amazonaws.com/1/q", "wait_time_seconds": 21}),
        ] {
            assert!(SqsConfig::parse(&bad).is_err(), "{bad}");
        }
    }

    /// AWS's published `SigV4` example (IAM `ListUsers`, 2015-08-30).
    #[test]
    fn sigv4_matches_aws_reference_vector() {
        let creds = AwsCredentials {
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        };
        let time = DateTime::parse_from_rfc3339("2015-08-30T12:36:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let auth = sigv4_authorization(
            &SigV4Request {
                method: "GET",
                path: "/",
                query: "Action=ListUsers&Version=2010-05-08",
                headers: &[
                    (
                        "content-type",
                        "application/x-www-form-urlencoded; charset=utf-8",
                    ),
                    ("host", "iam.amazonaws.com"),
                    ("x-amz-date", "20150830T123600Z"),
                ],
                body: b"",
                region: "us-east-1",
                service: "iam",
                time,
            },
            &creds,
        );
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date, \
             Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
    }

    #[test]
    fn maps_message_to_instance_input() {
        let msg = json!({
            "MessageId": "m-1",
            "ReceiptHandle": "rh",
            "Body": "{\"order\": 7}",
            "Attributes": {"ApproximateReceiveCount": "1"},
            "MessageAttributes": {"kind": {"DataType": "String", "StringValue": "x"}}
        });
        let (data, meta, id) = map_message("https://q", &msg).unwrap();
        assert_eq!(data, json!({"order": 7}));
        assert_eq!(id, "m-1");
        assert_eq!(meta["attributes"]["ApproximateReceiveCount"], "1");
        assert_eq!(meta["message_attributes"]["kind"]["StringValue"], "x");

        let text = json!({"MessageId": "m-2", "ReceiptHandle": "rh", "Body": "hello"});
        assert_eq!(map_message("q", &text).unwrap().0, json!("hello"));
        assert!(map_message("q", &json!({"Body": "x"})).is_none());
        assert!(map_message("q", &json!({"MessageId": "m"})).is_none());
    }
}
