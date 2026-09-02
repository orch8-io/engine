pub mod api_key;
pub mod artifact;
pub mod audit;
pub mod auth;
pub mod checkpoint;
pub mod circuit_breaker;
pub mod clock;
pub mod cluster;
pub mod config;
pub mod context;
pub mod continuity;
pub mod continuity_advanced;
pub mod continuity_product;
pub mod contract;
pub mod credential;
pub mod cron;
pub mod dedupe;
pub mod diagnosis;
pub mod dlq;
pub mod encryption;
pub mod error;
pub mod event_correlation;
pub mod execution;
pub mod failure;
pub mod filter;
pub mod finding;
pub mod ids;
pub mod instance;
pub mod interceptor;
pub mod output;
pub mod plugin;
pub mod pool;
pub mod preflight;
pub mod queue_dispatch;
pub mod queue_routing;
pub mod rate_limit;
pub mod redaction;
pub mod release;
pub mod rollback;
pub mod sequence;
pub mod session;
pub mod signal;
pub mod step_log;
pub mod suggest;
pub mod template_trace;
pub mod trigger;
pub mod webhook_delivery;
pub mod webhook_outbox;
pub mod worker;
pub mod worker_filter;

// Re-exports for convenience.
pub use config::{EngineConfig, SecretString};
pub use error::{StepError, StorageError};
pub use ids::*;
pub use instance::{Budget, BudgetBreach, InstanceState, Priority, TaskInstance};

/// Shared serde default value functions used across types.
pub mod serde_defaults {
    #[must_use]
    pub const fn yes() -> bool {
        true
    }

    #[must_use]
    pub fn default_namespace() -> String {
        "default".to_string()
    }
}

/// Serde helper: serialize/deserialize `std::time::Duration` as milliseconds (u64).
pub mod serde_duration {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(dur: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(u64::try_from(dur.as_millis()).unwrap_or(u64::MAX))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        // Ref#14: reject negative values up front with a precise error instead
        // of letting `u64::deserialize` emit a generic "invalid type: integer
        // `-1`" or letting a float path silently saturate `-1.0 as u64 = 0`.
        let n = serde_json::Number::deserialize(d)?;
        ms_from_number(&n)
            .map(Duration::from_millis)
            .map_err(D::Error::custom)
    }

    fn ms_from_number(n: &serde_json::Number) -> Result<u64, String> {
        if let Some(ms) = n.as_u64() {
            return Ok(ms);
        }
        if let Some(signed) = n.as_i64() {
            return Err(format!(
                "duration must be non-negative (got {signed} ms); durations are expressed as milliseconds"
            ));
        }
        if n.as_f64().is_some_and(|value| value < 0.0) {
            return Err(format!(
                "duration must be non-negative (got {n}); durations are expressed as integer milliseconds"
            ));
        }
        Err(format!(
            "duration must be an integer number of milliseconds (got {n})"
        ))
    }

    // Re-exported so `serde_duration_opt` can share the parsing/validation.
    pub(super) fn parse_ms(n: &serde_json::Number) -> Result<u64, String> {
        ms_from_number(n)
    }
}

/// Serde helper: serialize/deserialize `Option<Duration>` as optional milliseconds.
pub mod serde_duration_opt {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(dur: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match dur {
            Some(d) => s.serialize_some(&u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        // Only integer milliseconds are accepted. Float seconds made `30`
        // mean 30ms while `30.0` meant 30s, an unsafe authoring ambiguity.
        let val: Option<serde_json::Value> = Option::deserialize(d)?;
        match val {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(serde_json::Value::Number(n)) => super::serde_duration::parse_ms(&n)
                .map(|ms| Some(Duration::from_millis(ms)))
                .map_err(D::Error::custom),
            Some(other) => Err(D::Error::custom(format!(
                "expected number or null for duration, got {other}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    // --- serde_defaults ---

    #[test]
    fn serde_defaults_yes() {
        assert!(serde_defaults::yes());
    }

    #[test]
    fn serde_defaults_default_namespace() {
        assert_eq!(serde_defaults::default_namespace(), "default");
    }

    // --- serde_duration ---

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct DurationWrapper {
        #[serde(with = "serde_duration")]
        dur: Duration,
    }

    #[test]
    fn serde_duration_round_trip() {
        let w = DurationWrapper {
            dur: Duration::from_millis(1500),
        };
        let json = serde_json::to_string(&w).unwrap();
        assert_eq!(json, r#"{"dur":1500}"#);
        let back: DurationWrapper = serde_json::from_str(&json).unwrap();
        assert_eq!(back.dur, Duration::from_millis(1500));
    }

    #[test]
    fn serde_duration_zero() {
        let w = DurationWrapper {
            dur: Duration::ZERO,
        };
        let json = serde_json::to_string(&w).unwrap();
        assert_eq!(json, r#"{"dur":0}"#);
        let back: DurationWrapper = serde_json::from_str(&json).unwrap();
        assert_eq!(back.dur, Duration::ZERO);
    }

    // --- serde_duration_opt ---

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct OptDurationWrapper {
        #[serde(with = "serde_duration_opt")]
        dur: Option<Duration>,
    }

    #[test]
    fn serde_duration_opt_some_round_trip() {
        let w = OptDurationWrapper {
            dur: Some(Duration::from_secs(3)),
        };
        let json = serde_json::to_string(&w).unwrap();
        assert_eq!(json, r#"{"dur":3000}"#);
        let back: OptDurationWrapper = serde_json::from_str(&json).unwrap();
        assert_eq!(back, w);
    }

    #[test]
    fn serde_duration_opt_none_round_trip() {
        let w = OptDurationWrapper { dur: None };
        let json = serde_json::to_string(&w).unwrap();
        assert_eq!(json, r#"{"dur":null}"#);
        let back: OptDurationWrapper = serde_json::from_str(&json).unwrap();
        assert_eq!(back.dur, None);
    }

    // Ref#14 — negative / malformed durations must surface a clear error
    // rather than silently saturating to zero.

    #[test]
    fn serde_duration_rejects_negative_integer() {
        let err = serde_json::from_str::<DurationWrapper>(r#"{"dur":-1}"#).unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("non-negative"),
            "error should mention non-negative: {err}"
        );
    }

    #[test]
    fn serde_duration_rejects_negative_float() {
        let err = serde_json::from_str::<DurationWrapper>(r#"{"dur":-0.5}"#).unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("non-negative"),
            "error should mention non-negative: {err}"
        );
    }

    #[test]
    fn serde_duration_opt_rejects_negative_integer() {
        let err = serde_json::from_str::<OptDurationWrapper>(r#"{"dur":-5}"#).unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("non-negative"),
            "error should mention non-negative: {err}"
        );
    }

    #[test]
    fn serde_duration_opt_rejects_negative_float() {
        let err = serde_json::from_str::<OptDurationWrapper>(r#"{"dur":-0.25}"#).unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("non-negative"),
            "error should mention non-negative: {err}"
        );
    }

    #[test]
    fn serde_duration_opt_rejects_fractional_seconds() {
        let err = serde_json::from_str::<OptDurationWrapper>(r#"{"dur":0.2}"#).unwrap_err();
        assert!(err.to_string().contains("integer number of milliseconds"));
    }

    #[test]
    fn serde_duration_opt_rejects_string() {
        let err = serde_json::from_str::<OptDurationWrapper>(r#"{"dur":"5s"}"#).unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("expected number"),
            "error should mention expected number: {err}"
        );
    }
}
