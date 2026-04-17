pub mod audit;
pub mod checkpoint;
pub mod circuit_breaker;
pub mod cluster;
pub mod config;
pub mod context;
pub mod credential;
pub mod cron;
pub mod dedupe;
pub mod encryption;
pub mod error;
pub mod execution;
pub mod filter;
pub mod ids;
pub mod instance;
pub mod interceptor;
pub mod output;
pub mod plugin;
pub mod pool;
pub mod rate_limit;
pub mod sequence;
pub mod session;
pub mod signal;
pub mod trigger;
pub mod worker;
pub mod worker_filter;

// Re-exports for convenience.
pub use config::{EngineConfig, SecretString};
pub use error::{StepError, StorageError};
pub use ids::*;
pub use instance::{InstanceState, Priority, TaskInstance};

/// Shared serde default value functions used across types.
pub mod serde_defaults {
    pub fn yes() -> bool {
        true
    }

    pub fn default_namespace() -> String {
        "default".to_string()
    }
}

/// Serde helper: serialize/deserialize `std::time::Duration` as milliseconds (u64).
pub mod serde_duration {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(dur: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(u64::try_from(dur.as_millis()).unwrap_or(u64::MAX))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let ms = u64::deserialize(d)?;
        Ok(Duration::from_millis(ms))
    }
}

/// Serde helper: serialize/deserialize `Option<Duration>` as optional milliseconds.
pub mod serde_duration_opt {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(dur: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match dur {
            Some(d) => s.serialize_some(&u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        let ms: Option<u64> = Option::deserialize(d)?;
        Ok(ms.map(Duration::from_millis))
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
}
