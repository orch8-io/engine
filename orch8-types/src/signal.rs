use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ids::InstanceId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub id: Uuid,
    pub instance_id: InstanceId,
    pub signal_type: SignalType,
    pub payload: serde_json::Value,
    pub delivered: bool,
    pub created_at: DateTime<Utc>,
    pub delivered_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
