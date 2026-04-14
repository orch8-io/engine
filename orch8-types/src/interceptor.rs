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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interceptor_def_default_all_none() {
        let def = InterceptorDef::default();
        assert!(def.before_step.is_none());
        assert!(def.after_step.is_none());
        assert!(def.on_signal.is_none());
        assert!(def.on_complete.is_none());
        assert!(def.on_failure.is_none());
    }

    #[test]
    fn interceptor_def_serde_roundtrip() {
        let def = InterceptorDef {
            before_step: Some(InterceptorAction {
                handler: "log_step".into(),
                params: serde_json::json!({"level": "debug"}),
            }),
            after_step: None,
            on_signal: None,
            on_complete: Some(InterceptorAction {
                handler: "notify".into(),
                params: serde_json::json!({}),
            }),
            on_failure: None,
        };
        let json = serde_json::to_string(&def).unwrap();
        let back: InterceptorDef = serde_json::from_str(&json).unwrap();
        assert_eq!(back.before_step.as_ref().unwrap().handler, "log_step");
        assert!(back.after_step.is_none());
        assert_eq!(back.on_complete.as_ref().unwrap().handler, "notify");
    }

    #[test]
    fn interceptor_def_none_fields_skipped_in_json() {
        let def = InterceptorDef::default();
        let json = serde_json::to_string(&def).unwrap();
        assert!(!json.contains("before_step"));
        assert!(!json.contains("after_step"));
        assert!(!json.contains("on_signal"));
        assert!(!json.contains("on_complete"));
        assert!(!json.contains("on_failure"));
    }

    #[test]
    fn interceptor_action_params_default_empty() {
        let json_str = r#"{"handler": "my_handler"}"#;
        let action: InterceptorAction = serde_json::from_str(json_str).unwrap();
        assert_eq!(action.handler, "my_handler");
        assert_eq!(action.params, serde_json::Value::Null);
    }
}
