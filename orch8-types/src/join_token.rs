//! Executor join tokens: `o8x1.<b64url(json)>`.
//!
//! A join token bundles everything an `executor` node needs to open its
//! outbound managed-control session (see `docs/NODE_ROLES.md`): endpoint,
//! dedicated API key, tenant, runtime identity, and placement labels. It is a
//! **secret** (it carries an API key) and is deliberately unsigned — the
//! managed control plane authenticates the key, the token is only transport.
//!
//! Consumers: `orch8 executor join <token>` (writes an `orch8.toml`) and
//! `orch8-server` (`ORCH8_JOIN_TOKEN`, for containers).
//!
//! Routing (optional, added without a version bump): `api_url` is the REST
//! base for the HTTP polling fallback when it is not `<endpoint>/api/v1`
//! (e.g. gRPC and REST on different ports), and `headers` are routing
//! headers sent on every gRPC call and REST request (e.g. a load balancer's
//! instance pin such as `fly-force-instance-id`). Older executors ignore
//! both fields; tokens without them encode exactly as before.

use std::collections::BTreeMap;
use std::fmt;

use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::config::{EngineConfig, NodeConfig, NodeRole};

/// Token prefix (format version 1).
pub const JOIN_TOKEN_PREFIX: &str = "o8x1.";

/// Upper bound on an encoded token; rejects garbage before decoding.
const MAX_TOKEN_BYTES: usize = 16 * 1024;
const MAX_LABELS: usize = 64;
const MAX_HEADERS: usize = 16;
const MAX_HEADER_VALUE_BYTES: usize = 1024;

/// Header names a token may never set: credentials and tenant identity come
/// from their own fields, and transport/framing headers belong to the client.
const RESERVED_HEADERS: &[&str] = &[
    "x-api-key",
    "x-tenant-id",
    "authorization",
    "proxy-authorization",
    "cookie",
    "host",
    "content-type",
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "upgrade",
    "te",
    "trailer",
    "user-agent",
];

/// Decoded join token payload (contract §5).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinToken {
    pub v: u32,
    pub endpoint: String,
    pub api_key: String,
    pub tenant_id: String,
    pub runtime_id: uuid::Uuid,
    pub worker_id_prefix: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub region: Option<String>,
    /// REST base (`https://…/api/v1`) for the HTTP polling fallback. `None`
    /// = `<endpoint>/api/v1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url: Option<String>,
    /// Routing headers sent on every gRPC call and REST request to the
    /// control plane (not secrets; never credentials).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
}

impl fmt::Debug for JoinToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinToken")
            .field("v", &self.v)
            .field("endpoint", &self.endpoint)
            .field("api_key", &"[REDACTED]")
            .field("tenant_id", &self.tenant_id)
            .field("runtime_id", &self.runtime_id)
            .field("worker_id_prefix", &self.worker_id_prefix)
            .field("labels", &self.labels)
            .field("region", &self.region)
            .field("api_url", &self.api_url)
            .field("headers", &self.headers)
            .finish()
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum JoinTokenError {
    #[error("join token must start with `o8x1.`")]
    Prefix,
    #[error("join token is larger than {MAX_TOKEN_BYTES} bytes")]
    TooLarge,
    #[error("join token payload is not valid base64url")]
    Encoding,
    #[error("join token payload is not valid JSON: {0}")]
    Json(String),
    #[error("unsupported join token version {0} (expected 1)")]
    Version(u32),
    #[error("invalid join token field `{field}`: {reason}")]
    Field {
        field: &'static str,
        reason: &'static str,
    },
}

fn field(field: &'static str, reason: &'static str) -> JoinTokenError {
    JoinTokenError::Field { field, reason }
}

/// `[A-Za-z0-9._:-]`, 1..=max chars.
fn is_ident(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-'))
}

/// Label key/value rules shared by the token and `--label k=v` overrides.
pub fn validate_label(key: &str, value: &str) -> Result<(), JoinTokenError> {
    if !is_ident(key, 63) {
        return Err(field(
            "labels",
            "keys must be 1-63 chars of [A-Za-z0-9._:-]",
        ));
    }
    if value.len() > 256 || value.chars().any(char::is_control) {
        return Err(field(
            "labels",
            "values must be at most 256 chars without control characters",
        ));
    }
    Ok(())
}

/// Rules for routing headers (token `headers`, `node.managed_control_headers`):
/// at most 16, lowercase HTTP token names (`[a-z0-9-]`, not `grpc-*` or a
/// reserved credential/transport header), visible-ASCII values of at most
/// 1024 bytes.
pub fn validate_routing_headers(headers: &BTreeMap<String, String>) -> Result<(), JoinTokenError> {
    if headers.len() > MAX_HEADERS {
        return Err(field("headers", "at most 16 headers"));
    }
    for (name, value) in headers {
        if name.is_empty()
            || name.len() > 64
            || !name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(field(
                "headers",
                "names must be 1-64 chars of lowercase [a-z0-9-]",
            ));
        }
        if name.starts_with("grpc-") || RESERVED_HEADERS.contains(&name.as_str()) {
            return Err(field(
                "headers",
                "must not set credential, tenant, or transport headers",
            ));
        }
        if value.is_empty()
            || value.len() > MAX_HEADER_VALUE_BYTES
            || !value.bytes().all(|b| (0x20..0x7f).contains(&b))
        {
            return Err(field(
                "headers",
                "values must be 1-1024 bytes of visible ASCII",
            ));
        }
    }
    Ok(())
}

fn validate_https_url(value: &str, name: &'static str) -> Result<(), JoinTokenError> {
    let value = value.trim();
    if !value.starts_with("https://") || value.len() <= "https://".len() {
        return Err(field(name, "must be an https:// URL"));
    }
    if value.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(field(name, "must not contain whitespace"));
    }
    Ok(())
}

impl JoinToken {
    /// Decode and validate a token string (surrounding whitespace ignored).
    pub fn parse(raw: &str) -> Result<Self, JoinTokenError> {
        let raw = raw.trim();
        if raw.len() > MAX_TOKEN_BYTES {
            return Err(JoinTokenError::TooLarge);
        }
        let body = raw
            .strip_prefix(JOIN_TOKEN_PREFIX)
            .ok_or(JoinTokenError::Prefix)?;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(body.trim_end_matches('='))
            .map_err(|_| JoinTokenError::Encoding)?;
        let token: Self =
            serde_json::from_slice(&bytes).map_err(|e| JoinTokenError::Json(e.to_string()))?;
        token.validate()?;
        Ok(token)
    }

    /// Encode to the wire form. Used by issuers and tests.
    #[must_use]
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self).unwrap_or_default();
        format!(
            "{JOIN_TOKEN_PREFIX}{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
        )
    }

    pub fn validate(&self) -> Result<(), JoinTokenError> {
        if self.v != 1 {
            return Err(JoinTokenError::Version(self.v));
        }
        validate_https_url(&self.endpoint, "endpoint")?;
        if let Some(api_url) = &self.api_url {
            validate_https_url(api_url, "api_url")?;
        }
        validate_routing_headers(&self.headers)?;
        if self.api_key.trim().is_empty() || self.api_key.len() > 1024 {
            return Err(field("api_key", "must be 1-1024 chars"));
        }
        if crate::ids::TenantId::new(self.tenant_id.clone()).is_err() {
            return Err(field("tenant_id", "is not a valid tenant id"));
        }
        if self.runtime_id.is_nil() {
            return Err(field("runtime_id", "must not be the nil UUID"));
        }
        if !is_ident(&self.worker_id_prefix, 64) {
            return Err(field(
                "worker_id_prefix",
                "must be 1-64 chars of [A-Za-z0-9._:-]",
            ));
        }
        if self.labels.len() > MAX_LABELS {
            return Err(field("labels", "at most 64 labels"));
        }
        for (key, value) in &self.labels {
            validate_label(key, value)?;
        }
        if let Some(region) = &self.region
            && !is_ident(region, 64)
        {
            return Err(field("region", "must be 1-64 chars of [A-Za-z0-9._:-]"));
        }
        Ok(())
    }

    /// Stable worker id: `<prefix>-<host>`, where `host` is sanitized to the
    /// identifier alphabet. In containers pass the pod/host name so each
    /// replica gets its own identity.
    #[must_use]
    pub fn worker_id(&self, host: &str) -> String {
        let host: String = host
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                    c
                } else {
                    '-'
                }
            })
            .take(63)
            .collect();
        let host = host.trim_matches('-');
        if host.is_empty() {
            self.worker_id_prefix.clone()
        } else {
            format!("{}-{host}", self.worker_id_prefix)
        }
    }

    /// Point a whole config at the managed control plane: [`Self::apply_to`]
    /// plus the REST base (`executor.api_url`) when the token carries one.
    pub fn apply_to_config(&self, config: &mut EngineConfig, host: &str) {
        self.apply_to(&mut config.node, host);
        if let Some(api_url) = &self.api_url {
            api_url.trim().clone_into(&mut config.executor.api_url);
        }
    }

    /// Point `node` at the managed control plane as an `executor`.
    /// Existing labels are kept; token labels win on key conflicts. Routing
    /// headers replace any configured ones (they belong to this endpoint).
    pub fn apply_to(&self, node: &mut NodeConfig, host: &str) {
        node.role = NodeRole::Executor;
        self.endpoint
            .trim()
            .clone_into(&mut node.managed_control_endpoint);
        node.managed_control_api_key = self.api_key.clone().into();
        node.managed_control_tenant_id.clone_from(&self.tenant_id);
        node.managed_control_worker_id = self.worker_id(host);
        node.managed_control_runtime_id = self.runtime_id.to_string();
        for (key, value) in &self.labels {
            node.labels.insert(key.clone(), value.clone());
        }
        if let Some(region) = &self.region {
            node.region.clone_from(region);
        }
        node.managed_control_headers.clone_from(&self.headers);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> JoinToken {
        JoinToken {
            v: 1,
            endpoint: "https://control.orch8.example".into(),
            api_key: "o8k_secret".into(),
            tenant_id: "acme".into(),
            runtime_id: uuid::Uuid::now_v7(),
            worker_id_prefix: "acme-dc1".into(),
            labels: BTreeMap::from([("gpu".into(), "a10".into())]),
            region: Some("eu-west-1".into()),
            api_url: None,
            headers: BTreeMap::new(),
        }
    }

    fn routed() -> JoinToken {
        JoinToken {
            endpoint: "https://engines.orch8.example:50051".into(),
            api_url: Some("https://engines.orch8.example/api/v1".into()),
            headers: BTreeMap::from([("fly-force-instance-id".into(), "148e21ea7d9389".into())]),
            ..sample()
        }
    }

    #[test]
    fn tokens_without_routing_fields_encode_as_before() {
        let token = sample();
        let json: serde_json::Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(token.encode().strip_prefix(JOIN_TOKEN_PREFIX).unwrap())
                .unwrap(),
        )
        .unwrap();
        let keys: Vec<&str> = json
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert!(
            !keys.contains(&"api_url") && !keys.contains(&"headers"),
            "{keys:?}"
        );

        // A v1 token minted before the routing fields existed still parses.
        let legacy = serde_json::json!({
            "v": 1, "endpoint": "https://control.orch8.example", "api_key": "o8k_secret",
            "tenant_id": "acme", "runtime_id": token.runtime_id, "worker_id_prefix": "acme-dc1",
            "labels": {}, "region": null
        });
        let raw = format!(
            "{JOIN_TOKEN_PREFIX}{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(&legacy).unwrap())
        );
        let parsed = JoinToken::parse(&raw).unwrap();
        assert_eq!(parsed.api_url, None);
        assert!(parsed.headers.is_empty());

        // Unknown future fields are ignored, as older executors ignore these.
        let mut future = legacy;
        future["something_new"] = serde_json::json!({"x": 1});
        let raw = format!(
            "{JOIN_TOKEN_PREFIX}{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(&future).unwrap())
        );
        assert!(JoinToken::parse(&raw).is_ok());
    }

    #[test]
    fn routing_fields_round_trip_and_apply() {
        let token = routed();
        assert_eq!(JoinToken::parse(&token.encode()).unwrap(), token);
        let mut config = EngineConfig::default();
        config
            .node
            .managed_control_headers
            .insert("x-stale".into(), "1".into());
        token.apply_to_config(&mut config, "host");
        assert_eq!(
            config.executor.api_url,
            "https://engines.orch8.example/api/v1"
        );
        assert_eq!(config.node.managed_control_headers, token.headers);
        assert_eq!(
            config.node.managed_control_endpoint,
            "https://engines.orch8.example:50051"
        );
    }

    #[test]
    fn rejects_bad_routing_fields() {
        let mut token = routed();
        token.api_url = Some("http://engines.orch8.example/api/v1".into());
        assert!(matches!(
            JoinToken::parse(&token.encode()),
            Err(JoinTokenError::Field {
                field: "api_url",
                ..
            })
        ));
        for (name, value) in [
            ("x-api-key", "k"),
            ("x-tenant-id", "other"),
            ("authorization", "Bearer x"),
            ("grpc-timeout", "1S"),
            ("Fly-Force-Instance-Id", "m"),
            ("bad name", "m"),
            ("fly-force-instance-id", ""),
            ("fly-force-instance-id", "a\nb"),
        ] {
            let mut token = routed();
            token.headers = BTreeMap::from([(name.into(), value.into())]);
            assert!(
                matches!(
                    JoinToken::parse(&token.encode()),
                    Err(JoinTokenError::Field {
                        field: "headers",
                        ..
                    })
                ),
                "{name}={value:?} must be rejected"
            );
        }
        let mut token = routed();
        token.headers = (0..17).map(|i| (format!("x-r{i}"), "1".into())).collect();
        assert!(JoinToken::parse(&token.encode()).is_err());
    }

    #[test]
    fn round_trips_and_redacts_debug() {
        let token = sample();
        let encoded = token.encode();
        assert!(encoded.starts_with("o8x1."));
        assert_eq!(JoinToken::parse(&format!("  {encoded}\n")).unwrap(), token);
        assert!(!format!("{token:?}").contains("o8k_secret"));
    }

    #[test]
    fn rejects_bad_tokens() {
        assert_eq!(JoinToken::parse("o8e1.abc"), Err(JoinTokenError::Prefix));
        assert_eq!(JoinToken::parse("o8x1.!!!"), Err(JoinTokenError::Encoding));
        let mut token = sample();
        token.endpoint = "http://control.example".into();
        assert!(matches!(
            JoinToken::parse(&token.encode()),
            Err(JoinTokenError::Field {
                field: "endpoint",
                ..
            })
        ));
        let mut token = sample();
        token.v = 2;
        assert_eq!(
            JoinToken::parse(&token.encode()),
            Err(JoinTokenError::Version(2))
        );
        let mut token = sample();
        token.worker_id_prefix = "bad prefix".into();
        assert!(JoinToken::parse(&token.encode()).is_err());
        let mut token = sample();
        token.labels.insert("bad key".into(), "x".into());
        assert!(JoinToken::parse(&token.encode()).is_err());
    }

    #[test]
    fn applies_executor_role_and_managed_control_fields() {
        let token = sample();
        let mut node = NodeConfig::default();
        node.labels.insert("zone".into(), "a".into());
        token.apply_to(&mut node, "pod/7 x");
        assert_eq!(node.role, NodeRole::Executor);
        assert_eq!(node.managed_control_endpoint, token.endpoint);
        assert_eq!(node.managed_control_api_key.expose(), "o8k_secret");
        assert_eq!(node.managed_control_tenant_id, "acme");
        assert_eq!(node.managed_control_worker_id, "acme-dc1-pod-7-x");
        assert_eq!(
            node.managed_control_runtime_id,
            token.runtime_id.to_string()
        );
        assert_eq!(node.labels.get("gpu").map(String::as_str), Some("a10"));
        assert_eq!(node.labels.get("zone").map(String::as_str), Some("a"));
        assert_eq!(node.region, "eu-west-1");
        assert_eq!(token.worker_id(""), "acme-dc1");
    }
}
