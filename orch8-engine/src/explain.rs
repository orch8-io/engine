//! Plain-language instance explanations.
//!
//! [`explain_instance`] turns the ranked stuck-instance diagnosis (see
//! [`crate::doctor`]) and, for failed instances, the structured
//! [`FailureEnvelope`] into "what happened / likely cause / evidence / what
//! to do" text. It is pure and deterministic: the same evidence always
//! renders the same explanation.
//!
//! [`narrate_with_llm`] optionally asks an LLM (through the `llm_call`
//! provider plumbing) to rephrase the explanation. Everything sent to the
//! provider is first passed through [`RedactionPolicy`]; the structured
//! template fields are never replaced by model output.

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use orch8_types::diagnosis::{Diagnosis, InstanceDiagnosisReport};
use orch8_types::error::StepError;
use orch8_types::explain::{ExplanationMode, InstanceExplanation, LlmNarrative};
use orch8_types::failure::{ErrorClass, FailureEnvelope};
use orch8_types::finding::Confidence;
use orch8_types::redaction::RedactionPolicy;

/// Max diagnoses beyond the primary one listed as alternatives.
const MAX_ALTERNATIVES: usize = 3;

/// Render a deterministic explanation. `failure` is the derived envelope for
/// a failed instance (ignored for other states).
#[must_use]
pub fn explain_instance(
    report: &InstanceDiagnosisReport,
    failure: Option<&FailureEnvelope>,
    now: DateTime<Utc>,
) -> InstanceExplanation {
    let id = report.instance_id;
    let failed = report.state == "failed";
    let mut explanation = match (failed, failure) {
        (true, Some(envelope)) => explain_failure(report, envelope, now),
        _ => explain_diagnosis(report, now),
    };
    // Other ranked diagnoses as alternatives (skip the one already used).
    explanation.alternatives = report
        .diagnoses
        .iter()
        .filter(|d| d.finding.code != explanation.code)
        .take(MAX_ALTERNATIVES)
        .map(|d| format!("{}: {}", d.finding.code, d.finding.summary))
        .collect();
    if explanation.commands.is_empty() {
        explanation
            .commands
            .push(format!("orch8 instance diagnose {id}"));
    }
    explanation.commands.dedup();
    explanation
}

fn base(report: &InstanceDiagnosisReport, now: DateTime<Utc>) -> InstanceExplanation {
    InstanceExplanation {
        instance_id: report.instance_id,
        state: report.state.clone(),
        headline: String::new(),
        likely_cause: String::new(),
        evidence: Vec::new(),
        suggested_fix: String::new(),
        commands: Vec::new(),
        code: String::new(),
        error_code: None,
        docs_url: None,
        confidence: Confidence::Low,
        alternatives: Vec::new(),
        failure: None,
        mode: ExplanationMode::Template,
        narrative: None,
        llm_error: None,
        generated_at: now,
    }
}

/// Explanation driven by the top-ranked diagnosis.
fn explain_diagnosis(report: &InstanceDiagnosisReport, now: DateTime<Utc>) -> InstanceExplanation {
    let mut out = base(report, now);
    let id = report.instance_id;
    let Some(primary) = report.diagnoses.first() else {
        out.headline = format!(
            "Instance {id} is {} — no diagnosis available.",
            report.state
        );
        out.likely_cause = "The doctor produced no findings for this instance.".into();
        out.suggested_fix = "Re-run the diagnosis; check that storage is healthy.".into();
        out.code = "NO_BLOCKER_FOUND".into();
        attach_catalog(&mut out);
        return out;
    };
    let finding = &primary.finding;
    out.code.clone_from(&finding.code);
    out.confidence = finding.confidence;
    out.error_code.clone_from(&finding.error_code);
    out.docs_url.clone_from(&finding.docs_url);
    out.headline = headline_for(primary, &report.state);
    let entry = orch8_types::error_catalog::lookup(&finding.code);
    out.likely_cause = match entry {
        Some(entry) => format!("{} {}", sentence(&finding.summary), entry.cause),
        None => sentence(&finding.summary),
    };
    out.evidence = finding
        .evidence
        .iter()
        .map(|e| format!("{}: {}", e.label, e.summary))
        .collect();
    let remediation_text: Vec<String> = finding
        .remediation
        .iter()
        .map(|r| {
            if r.side_effect_risk {
                format!("{} (may repeat external side effects)", r.summary)
            } else {
                r.summary.clone()
            }
        })
        .collect();
    out.suggested_fix = match (remediation_text.is_empty(), entry) {
        (false, _) => sentence(&remediation_text.join("; ")),
        (true, Some(entry)) => entry.fix.to_string(),
        (true, None) => "No automatic remediation is known; inspect the evidence.".into(),
    };
    out.commands = finding
        .remediation
        .iter()
        .filter_map(|r| r.command.clone())
        .collect();
    out.commands.push(format!("orch8 instance diagnose {id}"));
    if out.error_code.is_none() {
        attach_catalog(&mut out);
    }
    out
}

fn attach_catalog(out: &mut InstanceExplanation) {
    if let Some(entry) = orch8_types::error_catalog::lookup(&out.code) {
        out.error_code = Some(entry.code.to_string());
        out.docs_url = Some(entry.docs_url());
    }
}

fn headline_for(primary: &Diagnosis, state: &str) -> String {
    let code = primary.finding.code.as_str();
    let what = match code {
        "WAITING_UNTIL" => "is waiting for a scheduled time (this is expected)",
        "WAITING_EVENT" => "is waiting for correlated events that have not arrived",
        "PENDING_APPROVAL" => "is waiting for a human approval",
        "BUDGET_PAUSED" => "was paused because it exceeded its budget",
        "PAUSED" => "is paused",
        "WORKER_TASK_PENDING" | "WAITING_WORKER_PICKUP" => {
            "is waiting for an external worker to pick up a task"
        }
        "WORKER_NOT_CLAIMING" => "has a task that live workers are not claiming",
        "STALE_WORKER_CLAIM" => "has a task claimed by a worker that went silent",
        "NO_COMPATIBLE_WORKER" => "cannot progress: no compatible worker is running",
        "WORKER_BELOW_VERSION_PIN" => "cannot progress: workers are older than the version pin",
        "OPEN_CIRCUIT_BREAKER" => "is held back by an open circuit breaker",
        "WAITING_CHILD" => "is waiting for a child workflow",
        "CHILDREN_DONE_PARENT_WAITING" => "is stuck: its children finished but it did not resume",
        "SIGNALS_NOT_CONSUMED" => "has signals queued that it has not consumed",
        "STALE_RUNNING_STATE" => "claims to be running but has not moved in a long time",
        "SCHEDULER_LAG" => "is overdue: the scheduler has not picked it up",
        "WAITING_EXTERNAL_EVENT" => "is waiting for an external signal or input",
        "SEQUENCE_MISSING" => "can never progress: its sequence was deleted",
        "TERMINAL_STATE" => {
            return format!("The instance is {state}; terminal instances do not progress.");
        }
        "NO_BLOCKER_FOUND" => "appears to be progressing normally",
        _ => {
            return format!(
                "The instance is {state}: {}",
                sentence(&primary.finding.summary)
            );
        }
    };
    format!("The instance {what}.")
}

/// Explanation for a failed instance, driven by its failure envelope.
fn explain_failure(
    report: &InstanceDiagnosisReport,
    envelope: &FailureEnvelope,
    now: DateTime<Utc>,
) -> InstanceExplanation {
    let mut out = base(report, now);
    let id = report.instance_id;
    let redaction = RedactionPolicy::default();
    let message = redaction.safe_excerpt(&envelope.message);
    let location = match (&envelope.block_id, &envelope.handler) {
        (Some(block), Some(handler)) => format!("block '{block}' (handler '{handler}')"),
        (Some(block), None) => format!("block '{block}'"),
        (None, Some(handler)) => format!("handler '{handler}'"),
        (None, None) => "an unknown block".to_string(),
    };
    out.code.clone_from(&envelope.error_code);
    out.confidence = if envelope.block_id.is_some() {
        Confidence::High
    } else {
        Confidence::Medium
    };
    out.headline = format!("The instance failed in {location}: {message}");
    let status = envelope.external_status.as_deref();
    let (cause, fix): (String, String) = match (envelope.error_code.as_str(), status) {
        ("HTTP_STATUS", Some("401" | "403")) => (
            format!("The remote service rejected the request's credentials (HTTP {}).", status.unwrap_or_default()),
            "Check the credential the step uses (rotate or re-authorize it), then retry.".into(),
        ),
        ("HTTP_STATUS", Some("404")) => (
            "The step called a URL that does not exist (HTTP 404).".into(),
            "Fix the URL or path parameters in the step's params, then retry.".into(),
        ),
        ("HTTP_STATUS", Some("408" | "429")) => (
            format!("The remote service throttled or timed out the request (HTTP {}).", status.unwrap_or_default()),
            "Add or increase the step's retry backoff / rate limit, then retry.".into(),
        ),
        ("HTTP_STATUS", Some(s)) if s.starts_with('5') => (
            format!("The remote service had an outage or internal error (HTTP {s})."),
            "Check the provider's health; once it recovers, retry. Consider a retry policy with backoff.".into(),
        ),
        ("HTTP_STATUS", _) => (
            format!("The remote service rejected the request (HTTP {}).", status.unwrap_or("4xx")),
            "Inspect the request the step sends (params, body, headers) against the API's contract.".into(),
        ),
        _ => class_cause_and_fix(envelope.error_class),
    };
    out.likely_cause = cause;
    out.suggested_fix = fix;
    out.evidence
        .push(format!("error_code: {}", envelope.error_code));
    out.evidence
        .push(format!("error_class: {}", envelope.error_class.as_str()));
    if let Some(status) = &envelope.external_status {
        out.evidence.push(format!("external_status: {status}"));
    }
    out.evidence.push(format!("message: {message}"));
    out.evidence
        .push(format!("failed_at: {}", envelope.occurred_at.to_rfc3339()));
    out.commands = vec![
        format!("orch8 instance outputs {id}"),
        format!("orch8 instance retry {id}"),
    ];
    if !matches!(envelope.error_class, ErrorClass::Cancelled) {
        out.suggested_fix.push_str(
            " Retrying re-runs the failed block, which may repeat external side effects.",
        );
    }
    out.failure = Some(FailureEnvelope {
        message: message.clone(),
        details: envelope.details.as_ref().map(|d| redaction.redacted(d)),
        ..envelope.clone()
    });
    // Failures are not catalogued findings; link the terminal-state entry.
    out.error_code = None;
    out.docs_url = orch8_types::error_catalog::lookup("TERMINAL_STATE")
        .map(orch8_types::error_catalog::ErrorCodeEntry::docs_url);
    out
}

fn class_cause_and_fix(class: ErrorClass) -> (String, String) {
    let (cause, fix) = match class {
        ErrorClass::Timeout => (
            "The step (or an upstream call) exceeded its timeout.",
            "Raise the step timeout, speed up the dependency, or add retries with backoff.",
        ),
        ErrorClass::Credential => (
            "A credential was missing, expired, or rejected.",
            "Restore or rotate the referenced credential, then retry.",
        ),
        ErrorClass::Worker => (
            "No compatible worker handled the step (none running, wrong queue, or below the version pin).",
            "Start or upgrade a worker for the handler (`orch8 dev --worker` locally), then retry.",
        ),
        ErrorClass::Policy => (
            "An engine policy stopped execution (circuit breaker, budget, rate limit, or URL policy).",
            "Review the policy named in the message; adjust it or wait for it to clear, then retry.",
        ),
        ErrorClass::ExternalDependency => (
            "An external dependency failed (network, DNS, TLS, or an upstream error).",
            "Check connectivity and the dependency's health, then retry.",
        ),
        ErrorClass::Configuration => (
            "The workflow is misconfigured (unknown handler, bad template, invalid params or schema).",
            "Fix the sequence definition, publish a new version, run `orch8 sequence preflight`, then retry.",
        ),
        ErrorClass::Cancelled => (
            "The instance was cancelled by an operator or by control flow.",
            "No fix needed unless the cancellation was unintended.",
        ),
        ErrorClass::Internal => (
            "The engine hit an internal invariant failure.",
            "Capture a support bundle (`orch8 support-bundle`) and report it.",
        ),
        ErrorClass::Application => (
            "The handler itself returned an error for this input (business logic or bad data).",
            "Inspect the block's input and the handler's logs, fix the handler or the data, then retry.",
        ),
    };
    (cause.to_string(), fix.to_string())
}

/// Capitalize the first letter and ensure a trailing period.
fn sentence(text: &str) -> String {
    let trimmed = text.trim();
    let mut chars = trimmed.chars();
    let mut out = match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect::<String>(),
        None => return String::new(),
    };
    if !out.ends_with(['.', '!', '?']) {
        out.push('.');
    }
    out
}

/// Build the (system, user) prompt for the optional LLM narrative. Every
/// string in the payload is passed through the redaction policy first: keys
/// that look sensitive are replaced wholesale and free text is scrubbed of
/// secret-shaped tokens, URL credentials, and `key=value` secrets.
#[must_use]
pub fn llm_prompt(explanation: &InstanceExplanation, policy: &RedactionPolicy) -> (String, String) {
    let mut payload = json!({
        "state": explanation.state,
        "headline": explanation.headline,
        "likely_cause": explanation.likely_cause,
        "evidence": explanation.evidence,
        "suggested_fix": explanation.suggested_fix,
        "commands": explanation.commands,
        "code": explanation.code,
        "error_code": explanation.error_code,
        "alternatives": explanation.alternatives,
        "failure": explanation.failure,
    });
    redact_for_llm(&mut payload, policy);
    let system = "You explain why an Orch8 durable-workflow instance is stuck or failed, for an \
                  on-call engineer. Use only the facts provided. Write 3-6 short sentences: what \
                  happened, the most likely cause, and the next step. Mention commands verbatim \
                  from the `commands` list only. Never invent identifiers, URLs, or secrets."
        .to_string();
    let user = format!(
        "Explain this instance diagnosis in plain language:\n{}",
        serde_json::to_string_pretty(&payload).unwrap_or_default()
    );
    (system, user)
}

/// Structural + free-text redaction of an outbound payload.
pub fn redact_for_llm(value: &mut Value, policy: &RedactionPolicy) {
    policy.redact_value(value);
    scrub_strings(value, policy);
}

fn scrub_strings(value: &mut Value, policy: &RedactionPolicy) {
    match value {
        Value::String(s) => *s = policy.safe_excerpt(s),
        Value::Array(items) => items.iter_mut().for_each(|v| scrub_strings(v, policy)),
        Value::Object(map) => map.values_mut().for_each(|v| scrub_strings(v, policy)),
        _ => {}
    }
}

/// Where the LLM narrative comes from. Built by the caller (API layer) from
/// server configuration; `api_key` may be a `credentials://<id>` reference
/// already resolved by the caller.
#[derive(Debug, Clone)]
pub struct LlmExplainConfig {
    pub provider: String,
    pub model: Option<String>,
    /// Explicit key; `None` uses the provider's default env var, exactly
    /// like the `llm_call` handler.
    pub api_key: Option<String>,
}

/// Ask the configured provider for a narrative. The explanation's
/// structured fields are left untouched; on success `mode` becomes `Llm`.
pub async fn narrate_with_llm(
    explanation: &mut InstanceExplanation,
    config: &LlmExplainConfig,
) -> Result<(), StepError> {
    let (system, user) = llm_prompt(explanation, &RedactionPolicy::default());
    let (model, text) = crate::handlers::llm::complete_text(
        &config.provider,
        config.model.as_deref(),
        config.api_key.as_deref(),
        &system,
        &user,
    )
    .await?;
    explanation.narrative = Some(LlmNarrative {
        provider: config.provider.clone(),
        model,
        text,
    });
    explanation.mode = ExplanationMode::Llm;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use orch8_types::diagnosis::{DiagnosisCategory, DiagnosisHealth};
    use orch8_types::finding::{Evidence, Finding, FindingSeverity, Remediation};
    use uuid::Uuid;

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 1, 12, 0, 0).unwrap()
    }

    fn report(state: &str, diagnoses: Vec<Diagnosis>) -> InstanceDiagnosisReport {
        InstanceDiagnosisReport {
            instance_id: Uuid::nil(),
            state: state.into(),
            diagnoses,
            generated_at: t0(),
        }
    }

    fn diag(code: &str, summary: &str) -> Diagnosis {
        Diagnosis {
            category: DiagnosisCategory::DirectEvidence,
            health: DiagnosisHealth::Degraded,
            finding: Finding::new(
                code,
                FindingSeverity::Error,
                summary,
                Confidence::High,
                t0(),
            ),
        }
    }

    #[test]
    fn no_worker_diagnosis_explains_cause_fix_and_commands() {
        let mut d = diag(
            "NO_COMPATIBLE_WORKER",
            "handler 'charge_card' has no live worker",
        );
        d.finding = d
            .finding
            .with_evidence(Evidence::new(
                "worker_registrations",
                "0 live workers for charge_card",
            ))
            .with_remediation(
                Remediation::new("start a worker for charge_card")
                    .with_command("orch8 dev --worker 'node worker.js'"),
            );
        let r = report("waiting", vec![d, diag("SCHEDULER_LAG", "overdue by 6m")]);
        let e = explain_instance(&r, None, t0());
        assert_eq!(e.mode, ExplanationMode::Template);
        assert_eq!(e.code, "NO_COMPATIBLE_WORKER");
        assert_eq!(e.error_code.as_deref(), Some("ORCH8-P001"));
        assert_eq!(
            e.headline,
            "The instance cannot progress: no compatible worker is running."
        );
        assert!(
            e.likely_cause
                .starts_with("Handler 'charge_card' has no live worker.")
        );
        assert_eq!(
            e.evidence,
            vec!["worker_registrations: 0 live workers for charge_card"]
        );
        assert_eq!(e.suggested_fix, "Start a worker for charge_card.");
        assert_eq!(
            e.commands,
            vec![
                "orch8 dev --worker 'node worker.js'".to_string(),
                format!("orch8 instance diagnose {}", Uuid::nil()),
            ]
        );
        assert_eq!(e.alternatives, vec!["SCHEDULER_LAG: overdue by 6m"]);
        // Deterministic: the same evidence renders the same explanation.
        assert_eq!(explain_instance(&r, None, t0()), e);
    }

    #[test]
    fn failed_instance_uses_failure_envelope_and_redacts_message() {
        let envelope = FailureEnvelope::new(
            "HTTP_STATUS",
            ErrorClass::ExternalDependency,
            "POST https://user:hunter2@api.example.com/charge returned 503 (Bearer sk-ant-abc123)",
            false,
            t0(),
        )
        .with_block("charge")
        .with_handler("http_request")
        .with_external_status("503");
        let r = report("failed", vec![diag("TERMINAL_STATE", "instance is failed")]);
        let e = explain_instance(&r, Some(&envelope), t0());
        assert!(
            e.headline
                .starts_with("The instance failed in block 'charge' (handler 'http_request')")
        );
        assert!(!e.headline.contains("hunter2"), "{}", e.headline);
        assert!(!e.headline.contains("sk-ant-abc123"), "{}", e.headline);
        assert!(e.likely_cause.contains("HTTP 503"), "{}", e.likely_cause);
        assert!(
            e.commands
                .contains(&format!("orch8 instance retry {}", Uuid::nil()))
        );
        assert!(e.evidence.contains(&"external_status: 503".to_string()));
        let failure = e.failure.as_ref().unwrap();
        assert!(!failure.message.contains("hunter2"));
        assert_eq!(e.confidence, Confidence::High);
    }

    #[test]
    fn credential_rejection_gets_credential_advice() {
        let envelope = FailureEnvelope::new(
            "HTTP_STATUS",
            ErrorClass::Credential,
            "http 401 unauthorized",
            false,
            t0(),
        )
        .with_external_status("401");
        let e = explain_instance(&report("failed", vec![]), Some(&envelope), t0());
        assert!(e.likely_cause.contains("credentials"), "{}", e.likely_cause);
        assert!(
            e.suggested_fix.contains("credential"),
            "{}",
            e.suggested_fix
        );
    }

    #[test]
    fn llm_prompt_is_redacted_before_sending() {
        let mut d = diag("WAITING_EXTERNAL_EVENT", "waiting for signal");
        d.finding = d.finding.with_evidence(Evidence::new(
            "last_signal",
            "payload token=ghp_supersecretvalue and api_key=abc",
        ));
        let mut e = explain_instance(&report("waiting", vec![d]), None, t0());
        e.evidence
            .push("url postgres://admin:pw@db.internal/prod".into());
        let (system, user) = llm_prompt(&e, &RedactionPolicy::default());
        assert!(system.contains("Use only the facts provided"));
        assert!(!user.contains("ghp_supersecretvalue"), "{user}");
        assert!(!user.contains("admin:pw"), "{user}");
        assert!(user.contains("WAITING_EXTERNAL_EVENT"));
    }

    #[test]
    fn empty_report_still_explains() {
        let e = explain_instance(&report("running", vec![]), None, t0());
        assert_eq!(e.code, "NO_BLOCKER_FOUND");
        assert_eq!(e.error_code.as_deref(), Some("ORCH8-D020"));
        assert!(!e.commands.is_empty());
    }
}
