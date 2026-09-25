//! Reusable redaction policy.
//!
//! Combined operator surfaces (workbench, delivery inspector, template
//! inspector, DLQ triage) expose more context than any single endpoint
//! did before. This module is the one place that decides what may leave
//! the engine in clear text.
//!
//! The policy is deny-by-name plus deny-by-shape:
//! - **By name:** any JSON key, header, or URL query parameter whose name
//!   matches a sensitive pattern is replaced with [`REDACTED`].
//! - **By shape:** string values that look like bearer tokens, JWTs, or
//!   well-known secret-key prefixes are replaced even under innocent keys.

use serde_json::Value;

/// Placeholder emitted for every redacted value.
pub const REDACTED: &str = "[REDACTED]";

/// Key-name fragments (lowercased) that mark a field as sensitive.
const SENSITIVE_KEY_FRAGMENTS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "api-key",
    "apikey",
    "authorization",
    "credential",
    "private_key",
    "privatekey",
    "access_key",
    "accesskey",
    "client_secret",
    "session_id",
    "cookie",
    "signature",
    "signing_key",
    "bearer",
    "refresh",
    "otp",
    "pin_code",
    "passphrase",
    "_auth", // basic_auth, proxy_auth, http_auth
    "-auth", // x-auth, proxy-auth
    "_dsn",
    "database_url",
    "db_url",
    "connection_string",
    "connectionstring",
    "conn_str",
];

/// Exact key names (lowercased) that are sensitive but whose fragments
/// would over-match as substrings ("auth" would hit "author").
const SENSITIVE_EXACT_KEYS: &[&str] = &["auth", "key", "pass", "pwd", "jwt", "sid", "dsn"];

/// HTTP auth schemes whose following token is the credential in free text
/// such as `Authorization: Bearer <tok>`.
const AUTH_SCHEMES: &[&str] = &["bearer", "basic", "token", "digest", "negotiate"];

/// Well-known secret value prefixes (checked case-sensitively, as issued).
const SECRET_VALUE_PREFIXES: &[&str] = &[
    "sk_live_",
    "sk_test_",
    "rk_live_",
    "rk_test_", // Stripe
    "ghp_",
    "gho_",
    "ghu_",
    "ghs_",
    "github_pat_", // GitHub
    "xoxb-",
    "xoxp-",
    "xoxa-",
    "xoxs-",    // Slack
    "AKIA",     // AWS access key id
    "sk-ant-",  // Anthropic
    "sk-proj-", // OpenAI
    "glpat-",   // GitLab
    "Bearer ",
];

/// A reusable redaction policy. `Default` gives the platform policy;
/// construct a custom one only in tests.
#[derive(Debug, Clone)]
pub struct RedactionPolicy {
    /// Additional protected context sections (top-level keys of context
    /// data that are wholesale redacted, e.g. tenant-declared protected
    /// sections).
    pub protected_sections: Vec<String>,
    /// Maximum length for response-body excerpts produced by
    /// [`Self::safe_excerpt`].
    pub max_excerpt_len: usize,
}

impl Default for RedactionPolicy {
    fn default() -> Self {
        Self {
            protected_sections: Vec::new(),
            max_excerpt_len: 2048,
        }
    }
}

impl RedactionPolicy {
    /// True when a JSON key / header / query-param name is sensitive.
    #[must_use]
    pub fn is_sensitive_key(&self, key: &str) -> bool {
        let lower = key.to_ascii_lowercase();
        if SENSITIVE_EXACT_KEYS.contains(&lower.as_str()) {
            return true;
        }
        if SENSITIVE_KEY_FRAGMENTS.iter().any(|f| lower.contains(f)) {
            return true;
        }
        self.protected_sections
            .iter()
            .any(|s| s.eq_ignore_ascii_case(&lower))
    }

    /// True when a string *value* looks like a secret regardless of its
    /// key: bearer tokens, JWTs, or well-known key prefixes.
    #[must_use]
    pub fn is_secret_shaped(&self, value: &str) -> bool {
        if has_secret_prefix(value) {
            return true;
        }
        // JWT: three dot-separated base64url segments, first one decoding
        // is overkill — the eyJ prefix ("{\"" base64) is the reliable tell.
        if value.starts_with("eyJ") && value.split('.').count() == 3 {
            return true;
        }
        // Connection strings / URLs with embedded credentials
        // (`postgres://user:pass@host/db`), anywhere in the value.
        contains_url_credentials(value)
    }

    /// Redact a JSON value in place: sensitive keys have their values
    /// replaced entirely (recursively for objects/arrays under them we
    /// replace the whole subtree), and secret-shaped strings are replaced
    /// wherever they appear.
    pub fn redact_value(&self, value: &mut Value) {
        match value {
            Value::Object(map) => {
                for (k, v) in map.iter_mut() {
                    if self.is_sensitive_key(k) {
                        *v = Value::String(REDACTED.to_string());
                    } else {
                        self.redact_value(v);
                    }
                }
            }
            Value::Array(items) => {
                for item in items.iter_mut() {
                    self.redact_value(item);
                }
            }
            Value::String(s) if self.is_secret_shaped(s) => {
                *s = REDACTED.to_string();
            }
            _ => {}
        }
    }

    /// Return a redacted clone, leaving the original untouched.
    #[must_use]
    pub fn redacted(&self, value: &Value) -> Value {
        let mut clone = value.clone();
        self.redact_value(&mut clone);
        clone
    }

    /// Redact a header list. Header names are matched with the same key
    /// policy; `Authorization`, `Proxy-Authorization`, `Cookie`, and
    /// `Set-Cookie` are always redacted.
    #[must_use]
    pub fn redact_headers<'a, I>(&self, headers: I) -> Vec<(String, String)>
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        headers
            .into_iter()
            .map(|(name, value)| {
                let lower = name.to_ascii_lowercase();
                let always = matches!(
                    lower.as_str(),
                    "authorization" | "proxy-authorization" | "cookie" | "set-cookie"
                );
                if always || self.is_sensitive_key(&lower) || self.is_secret_shaped(value) {
                    (name.to_string(), REDACTED.to_string())
                } else {
                    (name.to_string(), value.to_string())
                }
            })
            .collect()
    }

    /// Redact credentials in a URL: `user:pass@` userinfo and sensitive
    /// query-parameter values (keys are percent-decoded before matching),
    /// leaving the rest intact. Works textually so it never fails on
    /// partial URLs.
    #[must_use]
    pub fn redact_url(&self, url: &str) -> String {
        let url = redact_userinfo(url);
        let Some((base, query)) = url.split_once('?') else {
            return url;
        };
        // Preserve a fragment if present.
        let (query, fragment) = match query.split_once('#') {
            Some((q, f)) => (q, Some(f)),
            None => (query, None),
        };
        let mut out = format!("{base}?{}", self.redact_form(query));
        if let Some(f) = fragment {
            out.push('#');
            out.push_str(f);
        }
        out
    }

    /// Redact sensitive values in an `application/x-www-form-urlencoded`
    /// string (`a=1&password=x`). Keys are percent-decoded before matching
    /// so `pass%77ord` / `api%5Fkey` cannot slip through.
    #[must_use]
    pub fn redact_form(&self, form: &str) -> String {
        form.split('&')
            .map(|pair| match pair.split_once('=') {
                Some((k, v)) => {
                    let key = percent_decode_lossy(k);
                    let value = percent_decode_lossy(v);
                    if self.is_sensitive_key(&key) || self.is_secret_shaped(&value) {
                        format!("{k}={REDACTED}")
                    } else {
                        format!("{k}={v}")
                    }
                }
                None => pair.to_string(),
            })
            .collect::<Vec<_>>()
            .join("&")
    }

    /// Redact free text token by token; see [`Self::safe_excerpt`].
    fn redact_text(&self, body: &str) -> String {
        let mut out: Vec<String> = Vec::new();
        // Number of following tokens to redact unconditionally.
        let mut redact_next = 0usize;
        for tok in body.split_whitespace() {
            let lower = tok.to_ascii_lowercase();
            if redact_next > 0 {
                redact_next -= 1;
                // `Authorization: Bearer <tok>`: keep the scheme name,
                // redact the credential that follows it.
                if AUTH_SCHEMES.contains(&lower.as_str()) {
                    out.push(tok.to_string());
                    redact_next = 1;
                } else {
                    out.push(REDACTED.to_string());
                }
                continue;
            }
            if lower == "bearer" {
                out.push(tok.to_string());
                redact_next = 1;
                continue;
            }
            // `password:` / `api_key=` label with the value as the next token.
            if let Some(label) = tok.strip_suffix(':').or_else(|| tok.strip_suffix('='))
                && !label.is_empty()
                && self.is_sensitive_key(label.trim_matches('"'))
            {
                out.push(tok.to_string());
                redact_next = 1;
                continue;
            }
            // `password:hunter2` without a space.
            if let Some((label, value)) = tok.split_once(':')
                && !value.is_empty()
                && !value.starts_with("//")
                && self.is_sensitive_key(label.trim_matches('"'))
            {
                out.push(format!("{label}:{REDACTED}"));
                continue;
            }
            out.push(self.redact_text_token(tok));
        }
        out.join(" ")
    }

    /// Redact one whitespace-separated token of free text.
    fn redact_text_token(&self, tok: &str) -> String {
        if self.is_secret_shaped(tok) {
            return REDACTED.to_string();
        }
        if tok.contains("://") {
            return self.redact_url(tok);
        }
        if tok.contains('=') {
            return self.redact_form(tok);
        }
        tok.to_string()
    }

    /// Produce a bounded, redacted excerpt of a response body suitable
    /// for persistence. JSON bodies are redacted structurally first;
    /// non-JSON bodies are scanned token by token (whitespace-separated):
    /// secret-shaped tokens, URL credentials, form-encoded `key=value`
    /// pairs with sensitive keys, the value after a sensitive `key:` label
    /// (`Authorization: Bearer <tok>`), and the credential after an auth
    /// scheme (`Bearer <tok>`) are all replaced.
    #[must_use]
    pub fn safe_excerpt(&self, body: &str) -> String {
        let rendered = if let Ok(v) = serde_json::from_str::<Value>(body) {
            self.redacted(&v).to_string()
        } else {
            self.redact_text(body)
        };
        truncate_on_char_boundary(&rendered, self.max_excerpt_len)
    }
}

/// Byte range of the userinfo (`user:pass`) in the first `scheme://user:pass@host`
/// authority found at or after `from`.
fn find_userinfo(s: &str, from: usize) -> Option<(usize, usize, usize)> {
    let rel = s[from..].find("://")?;
    let auth_start = from + rel + 3;
    let rest = &s[auth_start..];
    let auth_len = rest
        .find(|c: char| matches!(c, '/' | '?' | '#') || c.is_whitespace() || c == '"')
        .unwrap_or(rest.len());
    let authority = &rest[..auth_len];
    let next = auth_start + auth_len;
    match authority.rfind('@') {
        Some(at) => Some((auth_start, auth_start + at, next)),
        None => Some((auth_start, auth_start, next)),
    }
}

/// True when `s` contains `scheme://user:password@...` (or a
/// secret-shaped bare username such as `https://ghp_x@github.com`).
fn contains_url_credentials(s: &str) -> bool {
    let mut from = 0;
    while let Some((start, at, next)) = find_userinfo(s, from) {
        if at > start {
            let userinfo = &s[start..at];
            if userinfo.contains(':') || has_secret_prefix(userinfo) {
                return true;
            }
        }
        from = next;
    }
    false
}

/// Replace every `user:pass@` / `user@` userinfo with `[REDACTED]@`.
fn redact_userinfo(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut from = 0;
    while let Some((start, at, next)) = find_userinfo(s, from) {
        if at > start {
            out.push_str(&s[from..start]);
            out.push_str(REDACTED);
            out.push_str(&s[at..next]);
        } else {
            out.push_str(&s[from..next]);
        }
        from = next;
    }
    out.push_str(&s[from..]);
    out
}

/// Percent-decode (`%XX` and `+` as space) without failing on malformed
/// input; invalid escapes are kept verbatim.
fn percent_decode_lossy(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                    .ok()
                    .and_then(|h| u8::from_str_radix(h, 16).ok());
                if let Some(b) = hex {
                    out.push(b);
                    i += 3;
                    continue;
                }
                out.push(b'%');
            }
            b'+' => out.push(b' '),
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn has_secret_prefix(value: &str) -> bool {
    SECRET_VALUE_PREFIXES.iter().any(|p| value.starts_with(p))
}

fn truncate_on_char_boundary(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn policy() -> RedactionPolicy {
        RedactionPolicy::default()
    }

    // --- key matching ---

    #[test]
    fn sensitive_key_fragments_match_case_insensitively() {
        let p = policy();
        for key in [
            "password",
            "PASSWORD",
            "user_password",
            "apiKey",
            "api_key",
            "Authorization",
            "x-api-key",
            "clientSecret",
            "private_key_pem",
            "sessionToken",
        ] {
            assert!(p.is_sensitive_key(key), "{key} should be sensitive");
        }
    }

    #[test]
    fn exact_keys_match_but_not_substrings() {
        let p = policy();
        assert!(p.is_sensitive_key("auth"));
        assert!(p.is_sensitive_key("key"));
        assert!(p.is_sensitive_key("jwt"));
        // These must NOT be redacted — over-matching destroys usefulness.
        assert!(!p.is_sensitive_key("author"));
        assert!(!p.is_sensitive_key("keyboard"));
        assert!(!p.is_sensitive_key("monkey")); // contains "key" only as substring
        assert!(!p.is_sensitive_key("passenger"));
    }

    #[test]
    fn ordinary_keys_are_not_sensitive() {
        let p = policy();
        for key in ["email", "name", "amount", "url", "customer_id", "status"] {
            assert!(!p.is_sensitive_key(key), "{key} should not be sensitive");
        }
    }

    #[test]
    fn protected_sections_extend_the_policy() {
        let mut p = policy();
        p.protected_sections.push("medical_record".into());
        assert!(p.is_sensitive_key("medical_record"));
        assert!(p.is_sensitive_key("MEDICAL_RECORD"));
        assert!(!policy().is_sensitive_key("medical_record"));
    }

    // --- value shape matching ---

    #[test]
    fn secret_shaped_values_detected() {
        let p = policy();
        for v in [
            "sk_live_abc123",
            "ghp_16charslong0000",
            "xoxb-1234-abcd",
            "AKIAIOSFODNN7EXAMPLE",
            "Bearer eyJtoken",
            "sk-ant-api03-xyz",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig",
        ] {
            assert!(p.is_secret_shaped(v), "{v} should be secret-shaped");
        }
    }

    #[test]
    fn ordinary_values_are_not_secret_shaped() {
        let p = policy();
        for v in [
            "hello world",
            "customer-42",
            "https://example.com",
            "eyJ but not a jwt",
        ] {
            assert!(!p.is_secret_shaped(v), "{v} should not be secret-shaped");
        }
    }

    // --- JSON redaction ---

    #[test]
    fn redacts_sensitive_keys_at_any_depth() {
        let mut v = json!({
            "name": "checkout",
            "config": {
                "api_key": "sk_live_secret",
                "nested": {"password": "hunter2", "safe": "ok"}
            },
            "items": [{"token": "t"}, {"amount": 5}]
        });
        policy().redact_value(&mut v);
        assert_eq!(v["config"]["api_key"], REDACTED);
        assert_eq!(v["config"]["nested"]["password"], REDACTED);
        assert_eq!(v["config"]["nested"]["safe"], "ok");
        assert_eq!(v["items"][0]["token"], REDACTED);
        assert_eq!(v["items"][1]["amount"], 5);
        assert_eq!(v["name"], "checkout");
    }

    #[test]
    fn redacts_whole_subtree_under_sensitive_key() {
        let mut v = json!({"credentials": {"user": "a", "pass": "b"}});
        policy().redact_value(&mut v);
        assert_eq!(v["credentials"], REDACTED);
    }

    #[test]
    fn redacts_secret_shaped_strings_under_innocent_keys() {
        let mut v = json!({"note": "sk_live_oops_in_a_note", "count": 3});
        policy().redact_value(&mut v);
        assert_eq!(v["note"], REDACTED);
        assert_eq!(v["count"], 3);
    }

    #[test]
    fn redacted_returns_clone_and_preserves_original() {
        let original = json!({"password": "x"});
        let out = policy().redacted(&original);
        assert_eq!(original["password"], "x");
        assert_eq!(out["password"], REDACTED);
    }

    #[test]
    fn non_string_sensitive_values_also_redacted() {
        let mut v = json!({"token": 12345, "secret": true, "pin_code": [1,2,3]});
        policy().redact_value(&mut v);
        assert_eq!(v["token"], REDACTED);
        assert_eq!(v["secret"], REDACTED);
        assert_eq!(v["pin_code"], REDACTED);
    }

    // --- headers ---

    #[test]
    fn authorization_and_cookie_headers_always_redacted() {
        let p = policy();
        let out = p.redact_headers(vec![
            ("Authorization", "Basic abc"),
            ("Cookie", "sid=1"),
            ("Set-Cookie", "sid=1"),
            ("Proxy-Authorization", "x"),
            ("Content-Type", "application/json"),
        ]);
        assert_eq!(out[0].1, REDACTED);
        assert_eq!(out[1].1, REDACTED);
        assert_eq!(out[2].1, REDACTED);
        assert_eq!(out[3].1, REDACTED);
        assert_eq!(out[4].1, "application/json");
    }

    #[test]
    fn custom_secret_headers_redacted_by_name_or_shape() {
        let p = policy();
        let out = p.redact_headers(vec![
            ("X-Api-Key", "k"),
            ("X-Request-Id", "req-1"),
            ("X-Debug", "ghp_leakedtoken000"),
        ]);
        assert_eq!(out[0].1, REDACTED);
        assert_eq!(out[1].1, "req-1");
        assert_eq!(out[2].1, REDACTED);
    }

    // --- URLs ---

    #[test]
    fn redacts_sensitive_query_params_only() {
        let p = policy();
        assert_eq!(
            p.redact_url("https://api.example.com/v1/charge?amount=5&api_key=sk_live_x&user=bob"),
            format!("https://api.example.com/v1/charge?amount=5&api_key={REDACTED}&user=bob")
        );
    }

    #[test]
    fn url_without_query_is_unchanged() {
        let p = policy();
        assert_eq!(
            p.redact_url("https://example.com/path"),
            "https://example.com/path"
        );
    }

    #[test]
    fn url_fragment_is_preserved() {
        let p = policy();
        assert_eq!(
            p.redact_url("https://x.com/p?token=abc#section"),
            format!("https://x.com/p?token={REDACTED}#section")
        );
    }

    #[test]
    fn url_secret_shaped_value_redacted_even_with_safe_name() {
        let p = policy();
        assert_eq!(
            p.redact_url("https://x.com/p?ref=ghp_secret123456"),
            format!("https://x.com/p?ref={REDACTED}")
        );
    }

    // --- excerpts ---

    #[test]
    fn json_excerpt_is_structurally_redacted() {
        let p = policy();
        let out = p.safe_excerpt(r#"{"error":"denied","api_key":"sk_live_x"}"#);
        assert!(out.contains("denied"));
        assert!(!out.contains("sk_live_x"));
        assert!(out.contains(REDACTED));
    }

    #[test]
    fn text_excerpt_redacts_secret_tokens() {
        let p = policy();
        let out = p.safe_excerpt("upstream said Bearer abc failed with ghp_tok123456789");
        assert!(!out.contains("ghp_tok123456789"));
        // The credential after a standalone `Bearer` scheme is redacted too.
        assert!(!out.contains("abc"), "{out}");
        assert!(out.contains("Bearer"));
    }

    #[test]
    fn text_excerpt_redacts_authorization_header_lines() {
        let p = policy();
        let out = p.safe_excerpt("GET / HTTP/1.1\nAuthorization: Bearer opaque-tok-1\nHost: x");
        assert!(!out.contains("opaque-tok-1"), "{out}");
        assert!(out.contains("Host: x"), "{out}");
        let out = p.safe_excerpt("Authorization: Basic dXNlcjpwYXNz");
        assert!(!out.contains("dXNlcjpwYXNz"), "{out}");
        let out = p.safe_excerpt("X-Api-Key: plainvalue password:hunter2");
        assert!(!out.contains("plainvalue"), "{out}");
        assert!(!out.contains("hunter2"), "{out}");
    }

    #[test]
    fn text_excerpt_redacts_form_encoded_bodies() {
        let p = policy();
        let out =
            p.safe_excerpt("grant_type=password&username=bob&password=hunter2&client_secret=s3");
        assert!(!out.contains("hunter2"), "{out}");
        assert!(!out.contains("s3"), "{out}");
        assert!(out.contains("username=bob"), "{out}");
        assert!(out.contains("grant_type=password"), "{out}");
    }

    #[test]
    fn text_excerpt_redacts_url_credentials() {
        let p = policy();
        let out = p.safe_excerpt("connect failed: postgres://app:s3cr3t@db:5432/prod timeout");
        assert!(!out.contains("s3cr3t"), "{out}");
        assert!(out.contains("timeout"));
    }

    #[test]
    fn new_sensitive_names_match() {
        let p = policy();
        for key in [
            "passphrase",
            "key_passphrase",
            "basic_auth",
            "proxy_auth",
            "x-auth-token",
            "dsn",
            "sentry_dsn",
            "DATABASE_URL",
            "connection_string",
            "ConnectionString",
        ] {
            assert!(p.is_sensitive_key(key), "{key} should be sensitive");
        }
        assert!(!p.is_sensitive_key("author_name"));
    }

    #[test]
    fn values_with_url_credentials_are_secret_shaped() {
        let p = policy();
        assert!(p.is_secret_shaped("postgres://user:pw@localhost/db"));
        assert!(p.is_secret_shaped("see redis://:pw@cache:6379 for details"));
        assert!(p.is_secret_shaped("https://ghp_abc123@github.com/o/r.git"));
        assert!(!p.is_secret_shaped("https://example.com/a@b"));
        assert!(!p.is_secret_shaped("ssh://git@github.com/o/r"));
        let mut v = json!({"upstream": "amqp://guest:guest@mq:5672"});
        p.redact_value(&mut v);
        assert_eq!(v["upstream"], REDACTED);
    }

    #[test]
    fn redact_url_strips_userinfo() {
        let p = policy();
        assert_eq!(
            p.redact_url("https://bob:hunter2@api.example.com/v1?x=1"),
            format!("https://{REDACTED}@api.example.com/v1?x=1")
        );
        assert_eq!(
            p.redact_url("postgres://u:p@db/prod"),
            format!("postgres://{REDACTED}@db/prod")
        );
        // `@` in the path is not userinfo.
        assert_eq!(p.redact_url("https://x.com/a@b"), "https://x.com/a@b");
    }

    #[test]
    fn redact_url_matches_percent_encoded_keys() {
        let p = policy();
        assert_eq!(
            p.redact_url("https://x.com/p?api%5Fkey=abc&pass%77ord=b&ok=1"),
            format!("https://x.com/p?api%5Fkey={REDACTED}&pass%77ord={REDACTED}&ok=1")
        );
        // Percent-encoded secret-shaped values are caught as well.
        assert_eq!(
            p.redact_url("https://x.com/p?next=Bearer%20abc"),
            format!("https://x.com/p?next={REDACTED}")
        );
    }

    #[test]
    fn excerpt_is_bounded() {
        let mut p = policy();
        p.max_excerpt_len = 32;
        let body = "x".repeat(1000);
        let out = p.safe_excerpt(&body);
        assert!(out.chars().count() <= 33); // 32 + ellipsis
        assert!(out.ends_with('…'));
    }

    #[test]
    fn excerpt_truncation_respects_utf8_boundaries() {
        let mut p = policy();
        p.max_excerpt_len = 5;
        // Multi-byte chars near the boundary must not panic.
        let out = p.safe_excerpt("héllö wörld");
        assert!(out.ends_with('…'));
    }
}
