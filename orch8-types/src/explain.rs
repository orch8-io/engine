//! Plain-language instance explanations (`orch8 explain`,
//! `GET /instances/{id}/explain`).
//!
//! An explanation is a *rendering* of evidence the engine already has — the
//! ranked stuck-instance diagnosis and, for failed instances, the structured
//! failure envelope — into "what happened, why, and what to do". The default
//! mode is deterministic and template-based; an optional LLM narrative may be
//! attached, but the structured fields are always the template output.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::failure::FailureEnvelope;
use crate::finding::Confidence;

/// How the explanation text was produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExplanationMode {
    /// Deterministic templates over the diagnosis and failure envelope.
    Template,
    /// Template fields plus an LLM-written narrative (see `narrative`).
    Llm,
}

/// Optional LLM narrative. The prompt contained only redacted evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LlmNarrative {
    pub provider: String,
    pub model: String,
    pub text: String,
}

/// A plain-language explanation of an instance's current situation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InstanceExplanation {
    pub instance_id: Uuid,
    /// Engine state name at explanation time.
    pub state: String,
    /// One sentence: what is going on.
    pub headline: String,
    /// The most likely cause, in plain language.
    pub likely_cause: String,
    /// Facts backing the explanation (already redacted by their producers).
    #[serde(default)]
    pub evidence: Vec<String>,
    /// What to do about it.
    pub suggested_fix: String,
    /// Commands the operator can run verbatim. Describing a command never
    /// runs it.
    #[serde(default)]
    pub commands: Vec<String>,
    /// Machine key of the primary diagnosis or failure (e.g. `NO_COMPATIBLE_WORKER`, `HTTP_STATUS`).
    pub code: String,
    /// Stable public code when catalogued (`ORCH8-P001`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docs_url: Option<String>,
    pub confidence: Confidence,
    /// Other plausible explanations, ranked, as `CODE: summary` lines.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub alternatives: Vec<String>,
    /// Structured failure envelope for failed instances.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<FailureEnvelope>,
    pub mode: ExplanationMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub narrative: Option<LlmNarrative>,
    /// Why an LLM narrative was requested but not produced (the template
    /// explanation is still complete).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm_error: Option<String>,
    pub generated_at: DateTime<Utc>,
}
