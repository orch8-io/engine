use serde_json::Value;
use tracing::warn;

use orch8_types::error::StepError;

/// Truncate a string to at most `max_bytes` without splitting a multi-byte
/// UTF-8 code point. Returns the full string when it's already within bounds.
pub(super) fn safe_truncate(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

pub(super) fn is_json_object_format(params: &Value) -> bool {
    params
        .get("response_format")
        .and_then(|rf| rf.get("type"))
        .and_then(Value::as_str)
        == Some("json_object")
}

/// Parse `content_str` as JSON and merge top-level fields into `output`.
/// Existing keys in `output` are preserved (no overwrite). Logs a warning
/// when the content isn't valid JSON.
pub(super) fn merge_json_response_fields(content_str: &str, output: &mut Value) {
    match serde_json::from_str::<Value>(content_str) {
        Ok(parsed) => {
            if let (Some(out_obj), Some(parsed_obj)) = (output.as_object_mut(), parsed.as_object())
            {
                for (k, v) in parsed_obj {
                    out_obj.entry(k).or_insert_with(|| v.clone());
                }
            }
        }
        Err(e) => {
            warn!(
                error = %e,
                content_preview = %safe_truncate(content_str, 200),
                "llm_call: response_format=json_object but content is not valid JSON — fields will NOT be merged into step output"
            );
        }
    }
}

/// Extract the first `system` message from a messages array and return the
/// remaining messages (Anthropic requires system as a top-level field).
pub(super) fn extract_system_message(messages: &Value) -> (Option<String>, Value) {
    let Some(arr) = messages.as_array() else {
        return (None, messages.clone());
    };

    let mut system = None;
    let mut filtered = Vec::with_capacity(arr.len());

    for msg in arr {
        if msg.get("role").and_then(Value::as_str) == Some("system") {
            if system.is_none() {
                system = msg.get("content").and_then(Value::as_str).map(String::from);
            }
        } else {
            filtered.push(msg.clone());
        }
    }

    (system, serde_json::json!(filtered))
}

/// Well-known infrastructure / cloud / CI secrets that must never leave the
/// process as a Bearer token, regardless of vendor naming conventions.
const DENIED_API_KEY_ENV: &[&str] = &[
    "AWS_SECRET_ACCESS_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SESSION_TOKEN",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GCP_SERVICE_ACCOUNT_KEY",
    "AZURE_CLIENT_SECRET",
    "DATABASE_URL",
    "REDIS_URL",
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "NPM_TOKEN",
    "SSH_PRIVATE_KEY",
    "PRIVATE_KEY",
];

/// Substrings that mark an env var as a non-LLM secret. A workflow author has
/// no legitimate reason to dereference a database password, signing key, or
/// session token as an `api_key_env`, so any var whose name *contains* one of
/// these is refused. This catches the long tail of third-party secrets a host
/// may carry (`STRIPE_SECRET_KEY`, `TWILIO_AUTH_TOKEN`, `DB_PASSWORD`, …) that
/// an exact-match denylist can never fully enumerate.
///
/// LLM provider keys conventionally end in `_API_KEY` / `_KEY` / `_TOKEN`, none
/// of which appear here, so legitimate names (`OPENAI_API_KEY`, `MY_OPENAI_KEY`,
/// `CUSTOM_LLM_TOKEN`) are unaffected.
const DENIED_API_KEY_ENV_SUBSTRINGS: &[&str] = &[
    "SECRET",
    "PASSWORD",
    "PASSWD",
    "CREDENTIAL",
    "PRIVATE_KEY",
    "SESSION_TOKEN",
];

/// `true` if `name` is a process env var that a *workflow author* is allowed to
/// dereference via `api_key_env`.
///
/// `api_key_env` is workflow-controlled and the resolved value is injected into
/// the `Authorization` header of a request to a (also workflow-controlled)
/// `base_url`. Without this gate a workflow could name the engine's own secrets
/// — `ORCH8_ENCRYPTION_KEY` (the master AES key for the entire credential
/// store), `ORCH8_API_KEY`, `ORCH8_DATABASE_URL`, `ORCH8_ARTIFACT_S3_SECRET_*`
/// — or common cloud/CI secrets, and exfiltrate them to an attacker host.
///
/// Defence is layered: deny the engine's whole `ORCH8_` namespace, an exact
/// list of well-known infrastructure names ([`DENIED_API_KEY_ENV`]), and any
/// name *containing* a secret-shaped substring ([`DENIED_API_KEY_ENV_SUBSTRINGS`]).
/// The substring rule is the important one — it covers third-party secrets a
/// host may hold that no fixed list can anticipate. Legitimate provider keys
/// (`OPENAI_API_KEY`, `MY_OPENAI_KEY`, …) are unaffected.
///
/// This remains a *denylist*: it cannot prove an arbitrary var is safe. Operators
/// running untrusted workflows should still avoid placing unrelated secrets in
/// the engine's process environment, or move to an explicit allowlist.
pub(crate) fn is_allowed_api_key_env(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();

    // The engine reads all of its own configuration/secrets from this prefix.
    // None of them are ever a legitimate LLM provider key.
    if upper.starts_with("ORCH8_") {
        return false;
    }

    if DENIED_API_KEY_ENV.contains(&upper.as_str()) {
        return false;
    }

    !DENIED_API_KEY_ENV_SUBSTRINGS
        .iter()
        .any(|needle| upper.contains(needle))
}

/// Resolve API key: direct param → env var param → provider default env var.
pub(super) fn resolve_api_key(params: &Value, provider: &str) -> Result<String, StepError> {
    if let Some(key) = params.get("api_key").and_then(Value::as_str) {
        return Ok(key.to_string());
    }

    if let Some(env_var) = params.get("api_key_env").and_then(Value::as_str) {
        if !is_allowed_api_key_env(env_var) {
            return Err(permanent(format!(
                "api_key_env '{env_var}' is not permitted: reading engine or \
                 infrastructure secrets via api_key_env is blocked"
            )));
        }
        return std::env::var(env_var)
            .map_err(|_| permanent(format!("env var '{env_var}' not set")));
    }

    let env_var = default_env_var(provider);
    std::env::var(env_var).map_err(|_| {
        permanent(format!(
            "no API key: set 'api_key', 'api_key_env', or env var '{env_var}'"
        ))
    })
}

/// Default env var name per provider.
pub(super) fn default_env_var(provider: &str) -> &'static str {
    match provider {
        "anthropic" => "ANTHROPIC_API_KEY",
        "gemini" => "GEMINI_API_KEY",
        "deepseek" => "DEEPSEEK_API_KEY",
        "qwen" => "DASHSCOPE_API_KEY",
        "perplexity" => "PERPLEXITY_API_KEY",
        "groq" => "GROQ_API_KEY",
        "together" => "TOGETHER_API_KEY",
        "mistral" => "MISTRAL_API_KEY",
        "openrouter" => "OPENROUTER_API_KEY",
        _ => "OPENAI_API_KEY",
    }
}

/// Resolve base URL: explicit param → provider default.
pub(super) fn resolve_base_url(params: &Value, provider: &str) -> String {
    if let Some(url) = params.get("base_url").and_then(Value::as_str) {
        return url.trim_end_matches('/').to_string();
    }

    match provider {
        "anthropic" => "https://api.anthropic.com/v1",
        "gemini" => "https://generativelanguage.googleapis.com/v1beta/openai",
        "deepseek" => "https://api.deepseek.com",
        "qwen" => "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "perplexity" => "https://api.perplexity.ai",
        "groq" => "https://api.groq.com/openai/v1",
        "together" => "https://api.together.xyz/v1",
        "mistral" => "https://api.mistral.ai/v1",
        "openrouter" => "https://openrouter.ai/api/v1",
        _ => "https://api.openai.com/v1",
    }
    .to_string()
}

pub(super) fn classify_reqwest_error(e: &reqwest::Error) -> StepError {
    if e.is_timeout() || e.is_connect() {
        retryable(format!("network error: {e}"))
    } else {
        permanent(format!("request error: {e}"))
    }
}

pub(super) fn classify_api_error(status: u16, body: &Value) -> StepError {
    let msg = body
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("unknown error");

    match status {
        429 => {
            warn!(status, msg, "LLM rate limited");
            retryable(format!("rate limited: {msg}"))
        }
        500..=599 => retryable(format!("server error ({status}): {msg}")),
        401 | 403 => permanent(format!("auth error ({status}): {msg}")),
        _ => permanent(format!("API error ({status}): {msg}")),
    }
}

pub(super) const fn retryable(message: String) -> StepError {
    StepError::Retryable {
        message,
        details: None,
    }
}

pub(super) const fn permanent(message: String) -> StepError {
    StepError::Permanent {
        message,
        details: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn safe_truncate_ascii_within_limit() {
        assert_eq!(safe_truncate("hello", 10), "hello");
    }

    #[test]
    fn safe_truncate_ascii_at_limit() {
        assert_eq!(safe_truncate("hello", 5), "hello");
    }

    #[test]
    fn safe_truncate_ascii_over_limit() {
        assert_eq!(safe_truncate("hello world", 5), "hello");
    }

    #[test]
    fn safe_truncate_empty() {
        assert_eq!(safe_truncate("", 10), "");
    }

    #[test]
    fn safe_truncate_multibyte_does_not_split_codepoint() {
        let s = "€abc";
        assert_eq!(safe_truncate(s, 4), "€a");
        assert_eq!(safe_truncate(s, 2), "");
        assert_eq!(safe_truncate(s, 3), "€");
    }

    #[test]
    fn safe_truncate_emoji() {
        let s = "🦀rust";
        assert_eq!(safe_truncate(s, 3), "");
        assert_eq!(safe_truncate(s, 4), "🦀");
        assert_eq!(safe_truncate(s, 5), "🦀r");
    }

    #[test]
    fn safe_truncate_cjk() {
        let s = "你好世界";
        assert_eq!(safe_truncate(s, 6), "你好");
        assert_eq!(safe_truncate(s, 7), "你好");
        assert_eq!(safe_truncate(s, 9), "你好世");
    }

    #[test]
    fn is_json_object_format_detects_correctly() {
        assert!(is_json_object_format(
            &json!({"response_format": {"type": "json_object"}})
        ));
        assert!(!is_json_object_format(
            &json!({"response_format": {"type": "text"}})
        ));
        assert!(!is_json_object_format(&json!({})));
    }

    #[test]
    fn extract_system_from_messages() {
        let msgs = json!([
            {"role": "system", "content": "You are helpful."},
            {"role": "user", "content": "Hello"},
        ]);
        let (sys, filtered) = extract_system_message(&msgs);
        assert_eq!(sys, Some("You are helpful.".into()));
        assert_eq!(filtered.as_array().unwrap().len(), 1);
        assert_eq!(filtered[0]["role"], "user");
    }

    #[test]
    fn extract_system_no_system() {
        let msgs = json!([{"role": "user", "content": "Hi"}]);
        let (sys, filtered) = extract_system_message(&msgs);
        assert!(sys.is_none());
        assert_eq!(filtered.as_array().unwrap().len(), 1);
    }

    #[test]
    fn extract_system_first_system_wins_others_dropped() {
        let msgs = json!([
            {"role": "system", "content": "first"},
            {"role": "user", "content": "hello"},
            {"role": "system", "content": "second"},
        ]);
        let (sys, filtered) = extract_system_message(&msgs);
        assert_eq!(sys, Some("first".into()));
        let arr = filtered.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["role"], "user");
    }

    #[test]
    fn resolve_base_urls() {
        let empty = json!({});
        assert!(resolve_base_url(&empty, "openai").contains("openai.com"));
        assert!(resolve_base_url(&empty, "anthropic").contains("anthropic.com"));
        assert!(resolve_base_url(&empty, "deepseek").contains("deepseek.com"));
        assert!(resolve_base_url(&empty, "qwen").contains("dashscope"));
        assert!(resolve_base_url(&empty, "perplexity").contains("perplexity"));
        assert!(resolve_base_url(&empty, "gemini").contains("googleapis"));
        assert!(resolve_base_url(&empty, "groq").contains("groq.com"));
        assert!(resolve_base_url(&empty, "together").contains("together"));
        assert!(resolve_base_url(&empty, "mistral").contains("mistral"));
        assert!(resolve_base_url(&empty, "openrouter").contains("openrouter"));
    }

    #[test]
    fn custom_base_url_overrides() {
        let params = json!({"base_url": "http://localhost:11434/v1"});
        assert_eq!(
            resolve_base_url(&params, "openai"),
            "http://localhost:11434/v1"
        );
    }

    #[test]
    fn default_env_vars() {
        assert_eq!(default_env_var("openai"), "OPENAI_API_KEY");
        assert_eq!(default_env_var("anthropic"), "ANTHROPIC_API_KEY");
        assert_eq!(default_env_var("gemini"), "GEMINI_API_KEY");
        assert_eq!(default_env_var("deepseek"), "DEEPSEEK_API_KEY");
        assert_eq!(default_env_var("unknown"), "OPENAI_API_KEY");
    }

    #[test]
    fn classify_errors() {
        let body_429 = json!({"error": {"message": "too many requests"}});
        assert!(matches!(
            classify_api_error(429, &body_429),
            StepError::Retryable { .. }
        ));
        let body_500 = json!({"error": {"message": "internal"}});
        assert!(matches!(
            classify_api_error(500, &body_500),
            StepError::Retryable { .. }
        ));
        let body_401 = json!({"error": {"message": "invalid key"}});
        assert!(matches!(
            classify_api_error(401, &body_401),
            StepError::Permanent { .. }
        ));
    }

    #[test]
    fn classify_api_error_403_is_permanent() {
        let body = json!({"error": {"message": "forbidden"}});
        assert!(matches!(
            classify_api_error(403, &body),
            StepError::Permanent { .. }
        ));
    }

    #[test]
    fn classify_api_error_503_is_retryable() {
        let body = json!({"error": {"message": "unavailable"}});
        assert!(matches!(
            classify_api_error(503, &body),
            StepError::Retryable { .. }
        ));
    }

    #[test]
    fn classify_api_error_4xx_other_is_permanent() {
        let body = json!({"error": {"message": "bad request"}});
        assert!(matches!(
            classify_api_error(400, &body),
            StepError::Permanent { .. }
        ));
    }

    #[test]
    fn resolve_api_key_prefers_direct_param_over_env() {
        let params = json!({"api_key": "direct-param-key"});
        std::env::set_var("OPENAI_API_KEY", "env-sourced-key");
        let key = resolve_api_key(&params, "openai").expect("direct param must resolve");
        assert_eq!(key, "direct-param-key");
    }

    #[test]
    fn resolve_api_key_from_explicit_env_var_param() {
        // A legitimate (non-engine) provider key var name passes the guard.
        let var = "MY_TEST_LLM_API_KEY_EXPLICIT";
        std::env::set_var(var, "from-explicit-env");
        let params = json!({"api_key_env": var});
        let key = resolve_api_key(&params, "openai").unwrap();
        assert_eq!(key, "from-explicit-env");
        std::env::remove_var(var);
    }

    #[test]
    fn resolve_api_key_returns_permanent_error_when_nothing_set() {
        // An allowed-but-unset var name exercises the missing-env-var path.
        let params = json!({"api_key_env": "MY_TEST_LLM_KEY_NONE_UNSET_VAR"});
        let err = resolve_api_key(&params, "openai").expect_err("missing env var must error out");
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn merge_json_response_fields_valid_json() {
        let content_str = r#"{"markets": [{"id": "m1"}], "count": 1}"#;
        let mut output = json!({
            "provider": "openai",
            "model": "gpt-4o",
            "message": {"role": "assistant", "content": content_str},
        });
        merge_json_response_fields(content_str, &mut output);
        assert!(output.get("markets").is_some());
        assert_eq!(output["count"], json!(1));
        assert_eq!(output["provider"], "openai");
    }

    #[test]
    fn merge_json_response_fields_does_not_overwrite_existing_keys() {
        let content_str = r#"{"provider": "evil", "model": "hijacked", "score": 42}"#;
        let mut output = json!({"provider": "openai", "model": "gpt-4o"});
        merge_json_response_fields(content_str, &mut output);
        assert_eq!(output["provider"], "openai");
        assert_eq!(output["model"], "gpt-4o");
        assert_eq!(output["score"], 42);
    }

    #[test]
    fn merge_json_response_fields_non_object_is_noop() {
        let mut output = json!({"provider": "openai"});
        merge_json_response_fields("[1, 2, 3]", &mut output);
        assert_eq!(output.as_object().unwrap().len(), 1);
        merge_json_response_fields(r#""just a string""#, &mut output);
        assert_eq!(output.as_object().unwrap().len(), 1);
        merge_json_response_fields("42", &mut output);
        assert_eq!(output.as_object().unwrap().len(), 1);
    }

    #[test]
    fn merge_json_response_fields_invalid_json_is_noop() {
        let mut output = json!({"provider": "openai"});
        merge_json_response_fields("", &mut output);
        assert_eq!(output.as_object().unwrap().len(), 1);
        merge_json_response_fields(r#"{"probability": 0.65, "reas"#, &mut output);
        assert_eq!(output.as_object().unwrap().len(), 1);
        merge_json_response_fields("<html>Not JSON</html>", &mut output);
        assert_eq!(output.as_object().unwrap().len(), 1);
    }

    #[test]
    fn merge_json_response_fields_does_not_panic_on_multibyte() {
        let content_str = "{ invalid: 日本語テスト 🦀 }";
        let mut output = json!({"provider": "openai"});
        merge_json_response_fields(content_str, &mut output);
        assert_eq!(output.as_object().unwrap().len(), 1);
    }

    #[test]
    fn merge_json_response_fields_nested_objects_preserved() {
        let content_str = r#"{"data": {"nested": {"deep": true}}, "count": 1}"#;
        let mut output = json!({"provider": "openai"});
        merge_json_response_fields(content_str, &mut output);
        assert_eq!(output["data"]["nested"]["deep"], json!(true));
        assert_eq!(output["count"], json!(1));
    }

    #[test]
    fn merge_json_response_fields_markdown_wrapped_json_fails() {
        let content_str = "```json\n{\"probability\": 0.65}\n```";
        let mut output = json!({"provider": "perplexity"});
        merge_json_response_fields(content_str, &mut output);
        assert!(output.get("probability").is_none());
    }

    #[test]
    fn api_key_env_allows_legitimate_provider_keys() {
        for name in [
            "OPENAI_API_KEY",
            "MY_OPENAI_KEY",
            "ANTHROPIC_API_KEY",
            "AZURE_OPENAI_KEY",
            "CUSTOM_LLM_TOKEN",
            "PATH",
        ] {
            assert!(is_allowed_api_key_env(name), "{name} should be allowed");
        }
    }

    #[test]
    fn api_key_env_blocks_engine_secret_namespace() {
        // The whole ORCH8_ namespace — most critically the master encryption key
        // and the engine's own API key — must never be exfiltratable.
        for name in [
            "ORCH8_ENCRYPTION_KEY",
            "ORCH8_OLD_ENCRYPTION_KEY",
            "ORCH8_API_KEY",
            "ORCH8_DATABASE_URL",
            "ORCH8_ARTIFACT_S3_SECRET_ACCESS_KEY",
            "orch8_encryption_key", // case-insensitive
        ] {
            assert!(!is_allowed_api_key_env(name), "{name} must be blocked");
        }
    }

    #[test]
    fn api_key_env_blocks_known_infrastructure_secrets() {
        for name in [
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "DATABASE_URL",
            "GITHUB_TOKEN",
            "SSH_PRIVATE_KEY",
            "private_key", // case-insensitive
        ] {
            assert!(!is_allowed_api_key_env(name), "{name} must be blocked");
        }
    }

    #[test]
    fn api_key_env_blocks_secret_shaped_third_party_names() {
        // The long tail an exact denylist can't enumerate — caught by substring.
        for name in [
            "STRIPE_SECRET_KEY",
            "TWILIO_AUTH_TOKEN_SESSION_TOKEN", // contains SESSION_TOKEN
            "DB_PASSWORD",
            "MY_PASSWD",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "DEPLOY_PRIVATE_KEY",
            "AWS_SESSION_TOKEN",
            "stripe_secret_key", // case-insensitive
        ] {
            assert!(!is_allowed_api_key_env(name), "{name} must be blocked");
        }
    }

    #[test]
    fn api_key_env_still_allows_token_and_key_suffixed_provider_names() {
        // Provider keys end in _KEY / _TOKEN / _API_KEY — none are secret-shaped.
        for name in ["OPENAI_API_KEY", "CUSTOM_LLM_TOKEN", "MY_GROQ_KEY"] {
            assert!(is_allowed_api_key_env(name), "{name} should stay allowed");
        }
    }

    #[test]
    fn resolve_api_key_rejects_blocked_env_var() {
        let params = json!({ "api_key_env": "ORCH8_ENCRYPTION_KEY" });
        let err = resolve_api_key(&params, "openai")
            .expect_err("reading the engine encryption key must be refused");
        match err {
            StepError::Permanent { message, .. } => {
                assert!(message.contains("not permitted"), "got: {message}");
            }
            other @ StepError::Retryable { .. } => {
                panic!("expected Permanent error, got {other:?}")
            }
        }
    }
}
