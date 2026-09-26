//! Provider signature presets for public webhook triggers.
//!
//! A webhook trigger normally authenticates Orch8's own
//! `x-orch8-signature` scheme (see [`crate::webhooks`]). Third-party senders
//! (Stripe, GitHub, Shopify, Svix-based senders such as Clerk and Resend)
//! sign with their own formats, so a trigger may instead carry
//!
//! ```json
//! "config": { "verify": { "preset": "stripe", "secret_ref": "credentials://stripe-webhook" } }
//! ```
//!
//! Every preset verifies an HMAC-SHA256 over the **raw request body bytes**
//! (never a re-serialised JSON value), compares in constant time
//! (`Mac::verify_slice`), enforces a timestamp tolerance when the provider
//! signs one, and returns a replay key the caller claims once in the nonce
//! store so a captured delivery cannot be replayed inside the window.
//!
//! | preset | signature header | signed payload | replay key |
//! |---|---|---|---|
//! | `stripe` | `Stripe-Signature: t=…,v1=<hex>` | `"{t}.{body}"` | `t` + signature |
//! | `github` | `X-Hub-Signature-256: sha256=<hex>` | `body` | `X-GitHub-Delivery` |
//! | `shopify` | `X-Shopify-Hmac-Sha256: <base64>` | `body` | `X-Shopify-Webhook-Id` |
//! | `svix` | `svix-signature: v1,<b64> …` (+ `svix-id`, `svix-timestamp`; `webhook-*` also accepted) | `"{id}.{ts}.{body}"` | `svix-id` |
//! | `hmac_sha256` | configurable `header` (default `X-Signature`), `encoding` hex/base64, optional `prefix` | `body`, or `"{ts}.{body}"` with `timestamp_header` | `id_header` value, else the signature |

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use axum::http::HeaderMap;

/// Default tolerance for providers that sign a timestamp (Stripe, Svix).
pub(crate) const DEFAULT_TOLERANCE_SECS: i64 = 300;
/// How long an id-keyed replay key is remembered for presets without a
/// signed timestamp (GitHub, Shopify). Providers retry for up to ~48h.
pub(crate) const UNTIMED_REPLAY_TTL_SECS: i64 = 72 * 3600;
const MAX_TOLERANCE_SECS: i64 = 3600;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Preset {
    Stripe,
    Github,
    Shopify,
    Svix,
    HmacSha256,
}

impl Preset {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "stripe" => Some(Self::Stripe),
            "github" => Some(Self::Github),
            "shopify" => Some(Self::Shopify),
            "svix" | "clerk" | "resend" | "standard_webhooks" => Some(Self::Svix),
            "hmac_sha256" => Some(Self::HmacSha256),
            _ => None,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Stripe => "stripe",
            Self::Github => "github",
            Self::Shopify => "shopify",
            Self::Svix => "svix",
            Self::HmacSha256 => "hmac_sha256",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Encoding {
    Hex,
    Base64,
}

/// Parsed `config.verify`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifyConfig {
    pub preset: Preset,
    /// `credentials://id[/field]` (or bare credential id). `None` falls back
    /// to the trigger's own `secret`.
    pub secret_ref: Option<String>,
    pub tolerance_secs: i64,
    // hmac_sha256 knobs.
    pub header: String,
    pub encoding: Encoding,
    pub prefix: String,
    pub timestamp_header: Option<String>,
    pub id_header: Option<String>,
}

impl VerifyConfig {
    /// `Ok(None)` when the trigger has no `verify` block.
    pub(crate) fn from_trigger_config(config: &serde_json::Value) -> Result<Option<Self>, String> {
        let Some(v) = config.get("verify") else {
            return Ok(None);
        };
        if v.is_null() {
            return Ok(None);
        }
        let obj = v.as_object().ok_or("`verify` must be an object")?;
        let preset_raw = obj
            .get("preset")
            .and_then(serde_json::Value::as_str)
            .ok_or("`verify.preset` is required")?;
        let preset = Preset::parse(preset_raw).ok_or_else(|| {
            format!("unknown verify preset `{preset_raw}` (stripe, github, shopify, svix, hmac_sha256)")
        })?;
        let str_field = |k: &str| {
            obj.get(k)
                .and_then(serde_json::Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        };
        let tolerance_secs = match obj.get("tolerance_secs") {
            None => DEFAULT_TOLERANCE_SECS,
            Some(t) => t
                .as_i64()
                .filter(|t| (1..=MAX_TOLERANCE_SECS).contains(t))
                .ok_or_else(|| format!("`verify.tolerance_secs` must be 1..={MAX_TOLERANCE_SECS}"))?,
        };
        let encoding = match str_field("encoding").as_deref() {
            None | Some("hex") => Encoding::Hex,
            Some("base64") => Encoding::Base64,
            Some(other) => return Err(format!("`verify.encoding` must be hex or base64 (got `{other}`)")),
        };
        let header = str_field("header").unwrap_or_else(|| "x-signature".into());
        for h in [Some(&header), str_field("timestamp_header").as_ref(), str_field("id_header").as_ref()]
            .into_iter()
            .flatten()
        {
            if axum::http::HeaderName::from_bytes(h.as_bytes()).is_err() {
                return Err(format!("invalid header name `{h}` in verify config"));
            }
        }
        Ok(Some(Self {
            preset,
            secret_ref: str_field("secret_ref"),
            tolerance_secs,
            header,
            encoding,
            prefix: str_field("prefix").unwrap_or_default(),
            timestamp_header: str_field("timestamp_header"),
            id_header: str_field("id_header"),
        }))
    }
}

/// Successful verification: the replay key to claim once and how long to
/// remember it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Verified {
    pub replay_key: String,
    pub replay_ttl_secs: i64,
    /// Provider event name when the provider sends it in a header
    /// (`X-GitHub-Event`, `X-Shopify-Topic`).
    pub event: Option<String>,
}

/// Why verification failed. Always surfaced to the caller as a bare 401.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VerifyError {
    MissingHeader(&'static str),
    Malformed(&'static str),
    StaleTimestamp,
    BadSignature,
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(str::trim)
}

fn mac(secret: &[u8]) -> Hmac<Sha256> {
    Hmac::<Sha256>::new_from_slice(secret)
        .unwrap_or_else(|_| unreachable!("HMAC-SHA256 accepts keys of any length"))
}

/// Constant-time check of `candidate` against HMAC(secret, parts…).
fn mac_matches(secret: &[u8], parts: &[&[u8]], candidate: &[u8]) -> bool {
    let mut m = mac(secret);
    for p in parts {
        m.update(p);
    }
    m.verify_slice(candidate).is_ok()
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 || s.len() > 256 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

fn timestamp_ok(now: i64, ts: i64, tolerance: i64) -> bool {
    (now - ts).abs() <= tolerance
}

fn short_hash(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    let d = Sha256::digest(bytes);
    d.iter().take(16).fold(String::new(), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Verify one delivery. `secret` is the raw secret material; for Svix a
/// `whsec_` prefix is stripped and the remainder base64-decoded.
pub(crate) fn verify(
    cfg: &VerifyConfig,
    secret: &str,
    headers: &HeaderMap,
    body: &[u8],
    now: i64,
) -> Result<Verified, VerifyError> {
    match cfg.preset {
        Preset::Stripe => verify_stripe(cfg, secret.as_bytes(), headers, body, now),
        Preset::Github => {
            let sig = header(headers, "x-hub-signature-256")
                .ok_or(VerifyError::MissingHeader("X-Hub-Signature-256"))?;
            let hex = sig.strip_prefix("sha256=").ok_or(VerifyError::Malformed("signature prefix"))?;
            let provided = decode_hex(hex).ok_or(VerifyError::Malformed("signature encoding"))?;
            if !mac_matches(secret.as_bytes(), &[body], &provided) {
                return Err(VerifyError::BadSignature);
            }
            let id = header(headers, "x-github-delivery")
                .filter(|s| !s.is_empty() && s.len() <= 128)
                .map_or_else(|| format!("sig:{hex}"), |d| format!("id:{d}"));
            Ok(Verified {
                replay_key: format!("github:{id}"),
                replay_ttl_secs: UNTIMED_REPLAY_TTL_SECS,
                event: header(headers, "x-github-event").map(str::to_string),
            })
        }
        Preset::Shopify => {
            let sig = header(headers, "x-shopify-hmac-sha256")
                .ok_or(VerifyError::MissingHeader("X-Shopify-Hmac-Sha256"))?;
            let provided = STANDARD
                .decode(sig)
                .map_err(|_| VerifyError::Malformed("signature encoding"))?;
            if !mac_matches(secret.as_bytes(), &[body], &provided) {
                return Err(VerifyError::BadSignature);
            }
            let id = header(headers, "x-shopify-webhook-id")
                .or_else(|| header(headers, "x-shopify-event-id"))
                .filter(|s| !s.is_empty() && s.len() <= 128)
                .map_or_else(|| format!("sig:{sig}"), |d| format!("id:{d}"));
            Ok(Verified {
                replay_key: format!("shopify:{id}"),
                replay_ttl_secs: UNTIMED_REPLAY_TTL_SECS,
                event: header(headers, "x-shopify-topic").map(str::to_string),
            })
        }
        Preset::Svix => verify_svix(cfg, secret, headers, body, now),
        Preset::HmacSha256 => verify_generic(cfg, secret.as_bytes(), headers, body, now),
    }
}

fn verify_stripe(
    cfg: &VerifyConfig,
    secret: &[u8],
    headers: &HeaderMap,
    body: &[u8],
    now: i64,
) -> Result<Verified, VerifyError> {
    let sig_header =
        header(headers, "stripe-signature").ok_or(VerifyError::MissingHeader("Stripe-Signature"))?;
    let mut ts: Option<&str> = None;
    let mut candidates = Vec::new();
    for part in sig_header.split(',') {
        match part.trim().split_once('=') {
            Some(("t", v)) => ts = Some(v),
            Some(("v1", v)) => candidates.push(v),
            _ => {}
        }
    }
    let ts_str = ts.ok_or(VerifyError::Malformed("missing t="))?;
    let ts_val: i64 = ts_str.parse().map_err(|_| VerifyError::Malformed("t= not an integer"))?;
    if candidates.is_empty() {
        return Err(VerifyError::Malformed("missing v1="));
    }
    if !timestamp_ok(now, ts_val, cfg.tolerance_secs) {
        return Err(VerifyError::StaleTimestamp);
    }
    let matched = candidates.iter().find(|c| {
        decode_hex(c).is_some_and(|bytes| mac_matches(secret, &[ts_str.as_bytes(), b".", body], &bytes))
    });
    let Some(sig) = matched else {
        return Err(VerifyError::BadSignature);
    };
    Ok(Verified {
        replay_key: format!("stripe:{ts_str}:{sig}"),
        replay_ttl_secs: cfg.tolerance_secs * 2,
        event: None,
    })
}

fn verify_svix(
    cfg: &VerifyConfig,
    secret: &str,
    headers: &HeaderMap,
    body: &[u8],
    now: i64,
) -> Result<Verified, VerifyError> {
    let pick = |a: &str, b: &str| header(headers, a).or_else(|| header(headers, b));
    let id = pick("svix-id", "webhook-id").ok_or(VerifyError::MissingHeader("svix-id"))?;
    let ts = pick("svix-timestamp", "webhook-timestamp")
        .ok_or(VerifyError::MissingHeader("svix-timestamp"))?;
    let sigs = pick("svix-signature", "webhook-signature")
        .ok_or(VerifyError::MissingHeader("svix-signature"))?;
    if id.is_empty() || id.len() > 128 {
        return Err(VerifyError::Malformed("svix-id"));
    }
    let ts_val: i64 = ts.parse().map_err(|_| VerifyError::Malformed("svix-timestamp"))?;
    if !timestamp_ok(now, ts_val, cfg.tolerance_secs) {
        return Err(VerifyError::StaleTimestamp);
    }
    let key = match secret.strip_prefix("whsec_") {
        Some(b64) => STANDARD
            .decode(b64)
            .map_err(|_| VerifyError::Malformed("whsec_ secret is not base64"))?,
        None => secret.as_bytes().to_vec(),
    };
    let ok = sigs.split(' ').any(|entry| {
        entry
            .strip_prefix("v1,")
            .and_then(|b64| STANDARD.decode(b64).ok())
            .is_some_and(|bytes| {
                mac_matches(&key, &[id.as_bytes(), b".", ts.as_bytes(), b".", body], &bytes)
            })
    });
    if !ok {
        return Err(VerifyError::BadSignature);
    }
    Ok(Verified {
        replay_key: format!("svix:{id}"),
        replay_ttl_secs: cfg.tolerance_secs * 2,
        event: None,
    })
}

fn verify_generic(
    cfg: &VerifyConfig,
    secret: &[u8],
    headers: &HeaderMap,
    body: &[u8],
    now: i64,
) -> Result<Verified, VerifyError> {
    let raw = header(headers, &cfg.header).ok_or(VerifyError::MissingHeader("signature header"))?;
    let encoded = raw
        .strip_prefix(cfg.prefix.as_str())
        .ok_or(VerifyError::Malformed("signature prefix"))?;
    let provided = match cfg.encoding {
        Encoding::Hex => decode_hex(&encoded.to_ascii_lowercase()),
        Encoding::Base64 => STANDARD.decode(encoded).ok(),
    }
    .ok_or(VerifyError::Malformed("signature encoding"))?;
    let (ok, ttl) = if let Some(th) = &cfg.timestamp_header {
        let ts = header(headers, th).ok_or(VerifyError::MissingHeader("timestamp header"))?;
        let ts_val: i64 = ts.parse().map_err(|_| VerifyError::Malformed("timestamp"))?;
        if !timestamp_ok(now, ts_val, cfg.tolerance_secs) {
            return Err(VerifyError::StaleTimestamp);
        }
        (
            mac_matches(secret, &[ts.as_bytes(), b".", body], &provided),
            cfg.tolerance_secs * 2,
        )
    } else {
        (mac_matches(secret, &[body], &provided), UNTIMED_REPLAY_TTL_SECS)
    };
    if !ok {
        return Err(VerifyError::BadSignature);
    }
    let id = cfg
        .id_header
        .as_deref()
        .and_then(|h| header(headers, h))
        .filter(|s| !s.is_empty() && s.len() <= 128)
        .map_or_else(|| format!("sig:{}", short_hash(&provided)), |d| format!("id:{d}"));
    Ok(Verified {
        replay_key: format!("hmac:{id}"),
        replay_ttl_secs: ttl,
        event: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use serde_json::json;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn sign(secret: &[u8], parts: &[&[u8]]) -> Vec<u8> {
        let mut m = mac(secret);
        for p in parts {
            m.update(p);
        }
        m.finalize().into_bytes().to_vec()
    }

    fn cfg(v: serde_json::Value) -> VerifyConfig {
        VerifyConfig::from_trigger_config(&json!({"verify": v})).unwrap().unwrap()
    }

    fn headers(pairs: &[(&'static str, String)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(*k, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    const NOW: i64 = 1_700_000_000;
    const BODY: &[u8] = br#"{"id":"evt_1","type":"payment_intent.succeeded"}"#;

    #[test]
    fn config_parsing_validates_preset_and_knobs() {
        assert_eq!(VerifyConfig::from_trigger_config(&json!({})).unwrap(), None);
        assert!(VerifyConfig::from_trigger_config(&json!({"verify": {"preset": "nope"}})).is_err());
        assert!(VerifyConfig::from_trigger_config(&json!({"verify": {"preset": "stripe", "tolerance_secs": 0}})).is_err());
        assert!(VerifyConfig::from_trigger_config(&json!({"verify": {"preset": "hmac_sha256", "encoding": "rot13"}})).is_err());
        assert!(VerifyConfig::from_trigger_config(&json!({"verify": {"preset": "hmac_sha256", "header": "bad header"}})).is_err());
        assert_eq!(cfg(json!({"preset": "clerk"})).preset, Preset::Svix);
    }

    #[test]
    fn stripe_accepts_valid_and_rejects_tampered_stale_or_wrong_secret() {
        let c = cfg(json!({"preset": "stripe"}));
        let secret = "whsec_stripe_test";
        let t = NOW.to_string();
        let sig = hex(&sign(secret.as_bytes(), &[t.as_bytes(), b".", BODY]));
        let h = headers(&[("stripe-signature", format!("t={t},v1=deadbeef,v1={sig}"))]);
        let ok = verify(&c, secret, &h, BODY, NOW).unwrap();
        assert!(ok.replay_key.starts_with("stripe:"));
        assert_eq!(verify(&c, secret, &h, b"{}", NOW), Err(VerifyError::BadSignature));
        assert_eq!(verify(&c, "other", &h, BODY, NOW), Err(VerifyError::BadSignature));
        assert_eq!(verify(&c, secret, &h, BODY, NOW + 301), Err(VerifyError::StaleTimestamp));
        assert_eq!(
            verify(&c, secret, &HeaderMap::new(), BODY, NOW),
            Err(VerifyError::MissingHeader("Stripe-Signature"))
        );
    }

    #[test]
    fn github_uses_sha256_header_and_delivery_id() {
        let c = cfg(json!({"preset": "github"}));
        let sig = hex(&sign(b"gh-secret", &[BODY]));
        let h = headers(&[
            ("x-hub-signature-256", format!("sha256={sig}")),
            ("x-github-delivery", "72d3162e-cc78-11e3-81ab-4c9367dc0958".into()),
            ("x-github-event", "pull_request".into()),
        ]);
        let ok = verify(&c, "gh-secret", &h, BODY, NOW).unwrap();
        assert_eq!(ok.replay_key, "github:id:72d3162e-cc78-11e3-81ab-4c9367dc0958");
        assert_eq!(ok.event.as_deref(), Some("pull_request"));
        let bad = headers(&[("x-hub-signature-256", format!("sha1={sig}"))]);
        assert!(verify(&c, "gh-secret", &bad, BODY, NOW).is_err());
        assert_eq!(verify(&c, "gh-secret", &h, b"tampered", NOW), Err(VerifyError::BadSignature));
    }

    #[test]
    fn shopify_uses_base64_hmac() {
        let c = cfg(json!({"preset": "shopify"}));
        let sig = STANDARD.encode(sign(b"shp", &[BODY]));
        let h = headers(&[
            ("x-shopify-hmac-sha256", sig),
            ("x-shopify-webhook-id", "b54557e4-bdd9-4b37-8a5f-bf7d70bcd043".into()),
            ("x-shopify-topic", "orders/create".into()),
        ]);
        let ok = verify(&c, "shp", &h, BODY, NOW).unwrap();
        assert_eq!(ok.event.as_deref(), Some("orders/create"));
        assert!(ok.replay_key.starts_with("shopify:id:"));
        assert_eq!(verify(&c, "nope", &h, BODY, NOW), Err(VerifyError::BadSignature));
    }

    #[test]
    fn svix_decodes_whsec_and_checks_timestamp() {
        let c = cfg(json!({"preset": "svix"}));
        let key = b"svix-signing-key-bytes";
        let secret = format!("whsec_{}", STANDARD.encode(key));
        let ts = NOW.to_string();
        let sig = STANDARD.encode(sign(key, &[b"msg_1", b".", ts.as_bytes(), b".", BODY]));
        let h = headers(&[
            ("svix-id", "msg_1".into()),
            ("svix-timestamp", ts.clone()),
            ("svix-signature", format!("v1,Zm9v v1,{sig}")),
        ]);
        assert_eq!(verify(&c, &secret, &h, BODY, NOW).unwrap().replay_key, "svix:msg_1");
        assert_eq!(verify(&c, &secret, &h, BODY, NOW - 400), Err(VerifyError::StaleTimestamp));
        assert_eq!(verify(&c, &secret, &h, b"x", NOW), Err(VerifyError::BadSignature));
        // Standard Webhooks header names are accepted too.
        let h2 = headers(&[
            ("webhook-id", "msg_1".into()),
            ("webhook-timestamp", ts),
            ("webhook-signature", format!("v1,{sig}")),
        ]);
        assert!(verify(&c, &secret, &h2, BODY, NOW).is_ok());
    }

    #[test]
    fn generic_hmac_supports_prefix_encoding_and_timestamp() {
        let c = cfg(json!({"preset": "hmac_sha256", "header": "X-Sig", "prefix": "sha256=", "id_header": "X-Id"}));
        let sig = hex(&sign(b"k", &[BODY]));
        let h = headers(&[("x-sig", format!("sha256={sig}")), ("x-id", "abc".into())]);
        assert_eq!(verify(&c, "k", &h, BODY, NOW).unwrap().replay_key, "hmac:id:abc");

        let c = cfg(json!({"preset": "hmac_sha256", "encoding": "base64", "timestamp_header": "X-Ts"}));
        let ts = NOW.to_string();
        let sig = STANDARD.encode(sign(b"k", &[ts.as_bytes(), b".", BODY]));
        let h = headers(&[("x-signature", sig), ("x-ts", ts)]);
        assert!(verify(&c, "k", &h, BODY, NOW).is_ok());
        assert_eq!(verify(&c, "k", &h, BODY, NOW + 1000), Err(VerifyError::StaleTimestamp));
    }
}
