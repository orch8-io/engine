use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Defines lifecycle hooks attached to a sequence.
/// Interceptors are evaluated at defined points in the instance lifecycle.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct InterceptorDef {
    /// Called before each step executes. Can modify context or skip the step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_step: Option<InterceptorAction>,
    /// Called after each step completes (success or failure).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_step: Option<InterceptorAction>,
    /// Called when a signal is received.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_signal: Option<InterceptorAction>,
    /// Called when the instance completes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_complete: Option<InterceptorAction>,
    /// Called when the instance fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<InterceptorAction>,
}

/// An interceptor action that fires at a lifecycle point.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InterceptorAction {
    /// Handler name to invoke (must be registered or dispatched to external worker).
    pub handler: String,
    /// Static params passed to the handler.
    #[serde(default)]
    pub params: serde_json::Value,
}
