use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::context::ExecutionContext;
use crate::ids::InstanceId;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Signal {
    pub id: Uuid,
    pub instance_id: InstanceId,
    pub signal_type: SignalType,
    pub payload: serde_json::Value,
    pub delivered: bool,
    pub created_at: DateTime<Utc>,
    pub delivered_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SignalType {
    Pause,
    Resume,
    Cancel,
    UpdateContext,
    Custom(String),
}

impl std::fmt::Display for SignalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pause => f.write_str("pause"),
            Self::Resume => f.write_str("resume"),
            Self::Cancel => f.write_str("cancel"),
            Self::UpdateContext => f.write_str("update_context"),
            Self::Custom(s) => write!(f, "custom:{s}"),
        }
    }
}

/// Typed projection of `Signal` for handler dispatch.
///
/// The wire shape (`signal_type` + loose `payload`) is preserved for backward
/// compatibility with the API, dashboard, and persisted rows. At the handler
/// boundary, [`Signal::action`] lifts that pair into a variant that carries
/// the payload in its validated, typed form — so downstream code no longer
/// has to hand-decode `payload` per arm. Payload schemas required by each
/// variant are documented below; all decode failures surface as
/// [`SignalActionError`] rather than runtime panics.
#[derive(Debug, Clone)]
pub enum SignalAction {
    Pause,
    Resume,
    Cancel,
    /// Payload is the full replacement [`ExecutionContext`].
    UpdateContext(Box<ExecutionContext>),
    /// User-defined signal; the name is the discriminator, payload is opaque.
    Custom {
        name: String,
        payload: serde_json::Value,
    },
}

/// Failure modes when projecting a [`Signal`] onto [`SignalAction`]. The
/// error carries the `signal_type` string so logs can correlate without
/// leaking full payload content.
#[derive(Debug, thiserror::Error)]
pub enum SignalActionError {
    #[error("signal '{signal_type}' payload did not decode: {source}")]
    InvalidPayload {
        signal_type: String,
        #[source]
        source: serde_json::Error,
    },
}

impl Signal {
    /// Lift the raw `(signal_type, payload)` pair into a typed
    /// [`SignalAction`]. Variants that do not carry a payload ignore the
    /// `payload` field entirely; variants that require one return
    /// [`SignalActionError::InvalidPayload`] on malformed input.
    ///
    /// This is the single point where the loose `Value` is decoded — all
    /// handlers should consume the result rather than re-decoding in place.
    pub fn action(&self) -> Result<SignalAction, SignalActionError> {
        match &self.signal_type {
            SignalType::Pause => Ok(SignalAction::Pause),
            SignalType::Resume => Ok(SignalAction::Resume),
            SignalType::Cancel => Ok(SignalAction::Cancel),
            SignalType::UpdateContext => {
                let ctx: ExecutionContext =
                    serde_json::from_value(self.payload.clone()).map_err(|e| {
                        SignalActionError::InvalidPayload {
                            signal_type: "update_context".to_string(),
                            source: e,
                        }
                    })?;
                Ok(SignalAction::UpdateContext(Box::new(ctx)))
            }
            SignalType::Custom(name) => Ok(SignalAction::Custom {
                name: name.clone(),
                payload: self.payload.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::RuntimeContext;
    use chrono::Utc;
    use serde_json::json;

    fn mk(st: SignalType, payload: serde_json::Value) -> Signal {
        Signal {
            id: Uuid::new_v4(),
            instance_id: InstanceId::new(),
            signal_type: st,
            payload,
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        }
    }

    #[test]
    fn action_payload_free_variants_ignore_payload() {
        // Pause / Resume / Cancel must not fail on arbitrary payload content —
        // the payload column may hold legacy junk and that must not poison
        // the queue.
        for st in [SignalType::Pause, SignalType::Resume, SignalType::Cancel] {
            let s = mk(st.clone(), json!({"garbage": true}));
            let action = s.action().expect("payload-free variants never error");
            match (st, action) {
                (SignalType::Pause, SignalAction::Pause)
                | (SignalType::Resume, SignalAction::Resume)
                | (SignalType::Cancel, SignalAction::Cancel) => {}
                (other_st, other_action) => {
                    panic!("mismatch: {other_st:?} → {other_action:?}")
                }
            }
        }
    }

    #[test]
    fn action_update_context_decodes_typed_payload() {
        let ctx = ExecutionContext {
            data: json!({"answer": 42}),
            config: json!({}),
            audit: vec![],
            runtime: RuntimeContext::default(),
        };
        let s = mk(
            SignalType::UpdateContext,
            serde_json::to_value(&ctx).unwrap(),
        );
        match s.action().unwrap() {
            SignalAction::UpdateContext(decoded) => {
                assert_eq!(decoded.data["answer"], 42);
            }
            other => panic!("expected UpdateContext, got {other:?}"),
        }
    }

    #[test]
    fn action_update_context_with_bad_payload_returns_invalid_payload() {
        let s = mk(SignalType::UpdateContext, json!("not an ExecutionContext"));
        let err = s.action().expect_err("bad payload must error");
        match err {
            SignalActionError::InvalidPayload { signal_type, .. } => {
                assert_eq!(signal_type, "update_context");
            }
        }
    }

    #[test]
    fn action_custom_carries_name_and_raw_payload() {
        let s = mk(SignalType::Custom("kick".into()), json!({"reason": "late"}));
        match s.action().unwrap() {
            SignalAction::Custom { name, payload } => {
                assert_eq!(name, "kick");
                assert_eq!(payload["reason"], "late");
            }
            other => panic!("expected Custom, got {other:?}"),
        }
    }
}
