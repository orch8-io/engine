//! `jev` handler: calibrated, typed decisions from the `TypeSafe` AI Jev model.
//!
//! Jev is a "System One" decision model: it never generates text. A request
//! carries a `state` (the content to judge) and a map of typed questions;
//! each answer comes back in the shape the question declared, with a
//! calibrated confidence:
//!
//! | question `type` | `criteria`                         | answer fields                              |
//! |-----------------|------------------------------------|--------------------------------------------|
//! | `choice`        | map option → description (≤ 255)   | `choice`, `probabilities`, `confidence`    |
//! | `score`         | ordered array of levels (2..=10)   | `score`, `legend`, `probabilities`, `confidence` |
//! | `noul`          | optional `{true, false}` rubric    | `noul` (probability of yes, 0..1)          |
//!
//! The step output is the provider response (`{model, answers, usage}`), so
//! any downstream decision point reads it directly:
//!
//! ```json
//! {"type": "step", "id": "triage", "handler": "jev", "params": {
//!    "state": "{{context.data.ticket}}",
//!    "questions": {"team": {"type": "choice", "instructions": "Which team handles this?",
//!                           "criteria": {"billing": "Payments", "technical": "Bugs"}}}}}
//! // router:  outputs.triage.answers.team.choice == "billing"
//! // guard:   outputs.triage.answers.team.confidence >= 0.8
//! // model routing: an `llm_call` with "model": "{{outputs.route.answers.tier.choice}}"
//! ```
//!
//! Params: `questions` (required), `state` (defaults to the step's view of
//! `context.data`), `model` (default `jev-latest`), `api_key` (literal or
//! `credentials://…`; defaults to the operator's `TYPESAFE_API_KEY`, which is
//! only ever sent to the default endpoint), `base_url` (requires an explicit
//! `api_key`), `timeout_ms`.
//!
//! Failure classes: auth / validation errors (401, 403, 404, 422, other 4xx)
//! are permanent; rate limiting (429), overload (529), 5xx and transport
//! errors are retryable — so a step `retry` policy, the circuit breaker and
//! `fallback_handler` all apply unchanged when the provider is unavailable.
//!
//! The call is read-only (no external side effect), so it is not gated by
//! the effect guard and runs normally in dry-run mode.

use serde_json::{Map, Value, json};
use tracing::debug;

use orch8_types::error::StepError;

use super::StepContext;

/// Default Jev endpoint origin.
pub(crate) const DEFAULT_BASE_URL: &str = "https://api.typesafe.ai";
/// Evaluation endpoint path.
const ENDPOINT_PATH: &str = "/v1/systemone";
/// Operator env var holding the default API key (same name as the SDKs).
pub(crate) const API_KEY_ENV: &str = "TYPESAFE_API_KEY";
/// Default model alias.
pub(crate) const DEFAULT_MODEL: &str = "jev-latest";
/// Answers are a few hundred bytes; cap the body well above that.
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
/// Jev answers in 70-500 ms; a generous default still bounds a hung call.
const DEFAULT_TIMEOUT_MS: u64 = 15_000;
/// Provider limit on options per `choice` question.
const MAX_CHOICE_OPTIONS: usize = 255;
/// Provider limit on levels per `score` question.
const MAX_SCORE_LEVELS: usize = 10;

/// Endpoint + credentials for one evaluation.
#[derive(Debug, Clone, Default)]
pub(crate) struct JevEndpoint {
    pub base_url: Option<String>,
    pub api_key: Option<String>,
    pub model: Option<String>,
    pub timeout_ms: Option<u64>,
}

impl JevEndpoint {
    /// Read endpoint fields from a params-like object.
    pub(crate) fn from_params(params: &Value) -> Self {
        let s = |k: &str| params.get(k).and_then(Value::as_str).map(str::to_owned);
        Self {
            base_url: s("base_url"),
            api_key: s("api_key"),
            model: s("model"),
            timeout_ms: params.get("timeout_ms").and_then(Value::as_u64),
        }
    }

    /// Explicit key, or the operator key — but only for the default
    /// endpoint, so a workflow-chosen `base_url` can never receive it.
    fn resolve_api_key(&self) -> Result<String, StepError> {
        if let Some(key) = self.api_key.as_deref().filter(|k| !k.is_empty()) {
            return Ok(key.to_owned());
        }
        if !crate::handlers::llm::common::env_key_allowed_for_base_url(
            self.base_url.as_deref(),
            DEFAULT_BASE_URL,
        ) {
            return Err(permanent(
                "jev: a custom base_url requires an explicit 'api_key'".into(),
            ));
        }
        std::env::var(API_KEY_ENV).map_err(|_| {
            permanent(format!(
                "jev: no API key — set 'api_key' (e.g. a credentials:// reference) or {API_KEY_ENV}"
            ))
        })
    }

    fn url(&self) -> String {
        let base = self
            .base_url
            .as_deref()
            .unwrap_or(DEFAULT_BASE_URL)
            .trim_end_matches('/');
        format!("{base}{ENDPOINT_PATH}")
    }
}

const fn permanent(message: String) -> StepError {
    StepError::Permanent {
        message,
        details: None,
    }
}

const fn retryable(message: String) -> StepError {
    StepError::Retryable {
        message,
        details: None,
    }
}

/// Validate the question map locally so authoring mistakes fail fast and
/// permanently instead of burning a request (and retries) on a 422.
pub(crate) fn validate_questions(questions: &Value) -> Result<&Map<String, Value>, String> {
    let map = questions
        .as_object()
        .filter(|m| !m.is_empty())
        .ok_or_else(|| "'questions' must be a non-empty object of named questions".to_string())?;
    for (id, q) in map {
        let kind = q
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("question '{id}': missing 'type'"))?;
        if q.get("instructions").is_none_or(Value::is_null) {
            return Err(format!("question '{id}': missing 'instructions'"));
        }
        match kind {
            "choice" => {
                let n = q
                    .get("criteria")
                    .and_then(Value::as_object)
                    .map_or(0, Map::len);
                if n == 0 || n > MAX_CHOICE_OPTIONS {
                    return Err(format!(
                        "question '{id}': choice 'criteria' must map 1..={MAX_CHOICE_OPTIONS} options"
                    ));
                }
            }
            "score" => {
                let n = q
                    .get("criteria")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len);
                if !(2..=MAX_SCORE_LEVELS).contains(&n) {
                    return Err(format!(
                        "question '{id}': score 'criteria' must list 2..={MAX_SCORE_LEVELS} levels"
                    ));
                }
            }
            "noul" => {}
            other => {
                return Err(format!(
                    "question '{id}': unknown type '{other}' (expected choice, score or noul)"
                ));
            }
        }
    }
    Ok(map)
}

/// Evaluate `state` against `questions`. Returns the provider response
/// (`{model, answers, usage}`) after checking that every question has an
/// answer of the declared type.
pub(crate) async fn evaluate(
    endpoint: &JevEndpoint,
    state: &Value,
    questions: &Value,
) -> Result<Value, StepError> {
    let map = validate_questions(questions).map_err(|e| permanent(format!("jev: {e}")))?;
    let api_key = endpoint.resolve_api_key()?;
    let url = endpoint.url();
    if !crate::handlers::builtin::is_url_safe(&url).await {
        return Err(permanent(format!(
            "jev: endpoint '{}' targets an internal or non-public address",
            crate::outbound::redact_url(&url)
        )));
    }
    let body = json!({
        "state": state,
        "model": endpoint.model.as_deref().unwrap_or(DEFAULT_MODEL),
        "questions": questions,
    });
    let timeout =
        crate::outbound::clamp_timeout_ms(endpoint.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));

    let resp = crate::handlers::llm::http_client()
        .post(&url)
        .bearer_auth(api_key)
        .timeout(timeout)
        .json(&body)
        .send()
        .await
        .map_err(|e| {
            retryable(format!(
                "jev: request failed: {}",
                crate::outbound::redact_error(&e)
            ))
        })?;
    let status = resp.status();
    let bytes = crate::outbound::read_body_capped(resp, MAX_RESPONSE_BYTES)
        .await
        .map_err(|e| match e {
            crate::outbound::BodyReadError::TooLarge(cap) => {
                permanent(format!("jev: response exceeded {cap} bytes"))
            }
            crate::outbound::BodyReadError::Io(msg) => {
                retryable(format!("jev: reading response failed: {msg}"))
            }
        })?;

    if !status.is_success() {
        let detail = crate::outbound::truncate_for_error(&bytes, 512);
        let message = format!("jev: HTTP {}: {detail}", status.as_u16());
        return Err(
            if status.as_u16() == 429 || status.as_u16() == 529 || status.is_server_error() {
                retryable(message)
            } else {
                permanent(message)
            },
        );
    }

    let out: Value = serde_json::from_slice(&bytes)
        .map_err(|e| retryable(format!("jev: malformed response: {e}")))?;
    let answers = out
        .get("answers")
        .and_then(Value::as_object)
        .ok_or_else(|| retryable("jev: response has no 'answers' object".into()))?;
    for (id, q) in map {
        let expected = q.get("type").and_then(Value::as_str).unwrap_or_default();
        let got = answers
            .get(id)
            .and_then(|a| a.get("type"))
            .and_then(Value::as_str);
        if got != Some(expected) {
            return Err(retryable(format!(
                "jev: answer for '{id}' missing or not of type '{expected}'"
            )));
        }
    }
    debug!(questions = map.len(), "jev evaluation completed");
    Ok(out)
}

/// Ask a single `choice` question. Returns `(choice, confidence,
/// probabilities, model)`.
pub(crate) async fn decide_choice(
    endpoint: &JevEndpoint,
    state: &Value,
    instructions: &str,
    criteria: Map<String, Value>,
) -> Result<ChoiceDecision, StepError> {
    let questions = json!({
        "decision": {"type": "choice", "instructions": instructions, "criteria": criteria}
    });
    let out = evaluate(endpoint, state, &questions).await?;
    let answer = &out["answers"]["decision"];
    let choice = answer
        .get("choice")
        .and_then(Value::as_str)
        .ok_or_else(|| retryable("jev: choice answer without 'choice'".into()))?
        .to_owned();
    let confidence = answer
        .get("confidence")
        .and_then(Value::as_f64)
        .ok_or_else(|| retryable("jev: choice answer without 'confidence'".into()))?;
    Ok(ChoiceDecision {
        choice,
        confidence,
        probabilities: answer.get("probabilities").cloned().unwrap_or(Value::Null),
        model: out.get("model").cloned().unwrap_or(Value::Null),
    })
}

/// A single calibrated choice.
#[derive(Debug, Clone)]
pub(crate) struct ChoiceDecision {
    pub choice: String,
    pub confidence: f64,
    pub probabilities: Value,
    pub model: Value,
}

/// `jev` step handler.
pub async fn handle_jev(ctx: StepContext) -> Result<Value, StepError> {
    let questions = ctx
        .params
        .get("questions")
        .ok_or_else(|| permanent("jev: missing required 'questions' param".into()))?;
    let state = ctx
        .params
        .get("state")
        .filter(|v| !v.is_null())
        .cloned()
        .unwrap_or_else(|| ctx.context.data.clone());
    let endpoint = JevEndpoint::from_params(&ctx.params);
    evaluate(&endpoint, &state, questions).await
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Spawn a mock Jev endpoint that answers every request with
    /// `(status, body)` and records request bodies. Returns the base URL
    /// (already marked SSRF-safe for tests).
    pub(crate) async fn mock_jev(
        status: u16,
        body: Value,
    ) -> (
        String,
        Arc<tokio::sync::Mutex<Vec<Value>>>,
        Arc<AtomicUsize>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let hits = Arc::new(AtomicUsize::new(0));
        let (seen_srv, hits_srv) = (Arc::clone(&seen), Arc::clone(&hits));
        let body = body.to_string();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                hits_srv.fetch_add(1, Ordering::SeqCst);
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let n = stream.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
                        let len = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        if buf.len() >= end + 4 + len {
                            if let Ok(v) =
                                serde_json::from_slice::<Value>(&buf[end + 4..end + 4 + len])
                            {
                                seen_srv.lock().await.push(v);
                            }
                            break;
                        }
                    }
                }
                let resp = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        crate::handlers::builtin::mark_url_safe_for_test(&format!("{base}{ENDPOINT_PATH}")).await;
        (base, seen, hits)
    }

    pub(crate) fn choice_response(choice: &str, confidence: f64) -> Value {
        json!({
            "model": "jev-1.13.0",
            "answers": {"decision": {
                "type": "choice", "choice": choice,
                "probabilities": {choice: confidence},
                "confidence": confidence
            }},
            "usage": {"input_tokens": 10, "output_tokens": 5}
        })
    }

    fn endpoint(base: &str) -> JevEndpoint {
        JevEndpoint {
            base_url: Some(base.to_owned()),
            api_key: Some("k".into()),
            ..Default::default()
        }
    }

    #[test]
    fn validation_rejects_malformed_questions() {
        assert!(validate_questions(&json!({})).is_err());
        assert!(
            validate_questions(&json!({"q": {"type": "choice", "instructions": "x"}})).is_err()
        );
        assert!(
            validate_questions(
                &json!({"q": {"type": "score", "instructions": "x", "criteria": ["a"]}})
            )
            .is_err()
        );
        assert!(validate_questions(&json!({"q": {"type": "essay", "instructions": "x"}})).is_err());
        assert!(validate_questions(&json!({"q": {"type": "noul"}})).is_err());
        assert!(
            validate_questions(&json!({
                "a": {"type": "noul", "instructions": "urgent?"},
                "b": {"type": "score", "instructions": "how angry", "criteria": ["calm", "angry"]},
                "c": {"type": "choice", "instructions": "team", "criteria": {"x": null}}
            }))
            .is_ok()
        );
    }

    #[test]
    fn custom_base_url_never_gets_the_operator_key() {
        let ep = JevEndpoint {
            base_url: Some("https://evil.example".into()),
            ..Default::default()
        };
        let err = ep.resolve_api_key().unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn evaluate_posts_typed_request_and_returns_answers() {
        let (base, seen, _) = mock_jev(200, choice_response("billing", 0.93)).await;
        let q = json!({"decision": {"type": "choice", "instructions": "team?",
                                    "criteria": {"billing": "pay", "technical": "bugs"}}});
        let out = evaluate(&endpoint(&base), &json!("payouts failing"), &q)
            .await
            .unwrap();
        assert_eq!(out["answers"]["decision"]["choice"], "billing");
        let sent = seen.lock().await;
        assert_eq!(sent[0]["model"], DEFAULT_MODEL);
        assert_eq!(sent[0]["state"], "payouts failing");
        assert_eq!(sent[0]["questions"], q);
    }

    #[tokio::test]
    async fn rate_limit_and_overload_are_retryable_auth_is_permanent() {
        let q = json!({"x": {"type": "noul", "instructions": "?"}});
        for (status, retry) in [
            (429, true),
            (529, true),
            (503, true),
            (401, false),
            (422, false),
        ] {
            let (base, _, _) = mock_jev(status, json!({"error": "e"})).await;
            let err = evaluate(&endpoint(&base), &json!("s"), &q)
                .await
                .unwrap_err();
            assert_eq!(
                matches!(err, StepError::Retryable { .. }),
                retry,
                "status {status}: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn answer_of_wrong_type_is_rejected() {
        let (base, _, _) = mock_jev(
            200,
            json!({"answers": {"x": {"type": "score", "score": 1.0}}}),
        )
        .await;
        let q = json!({"x": {"type": "noul", "instructions": "?"}});
        assert!(evaluate(&endpoint(&base), &json!("s"), &q).await.is_err());
    }

    #[tokio::test]
    async fn invalid_questions_fail_before_any_request() {
        let (base, _, hits) = mock_jev(200, json!({})).await;
        let err = evaluate(
            &endpoint(&base),
            &json!("s"),
            &json!({"x": {"type": "noul"}}),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }
}
