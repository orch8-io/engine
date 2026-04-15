pub mod config;
pub mod context;
pub mod cron;
pub mod error;
pub mod execution;
pub mod filter;
pub mod ids;
pub mod instance;
pub mod output;
pub mod rate_limit;
pub mod sequence;
pub mod signal;

// Re-exports for convenience.
pub use config::EngineConfig;
pub use error::{StepError, StorageError};
pub use ids::*;
pub use instance::{InstanceState, Priority, TaskInstance};

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
