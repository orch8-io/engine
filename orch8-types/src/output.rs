use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::{BlockId, InstanceId};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BlockOutput {
    pub id: Uuid,
    pub instance_id: InstanceId,
    pub block_id: BlockId,
    pub output: serde_json::Value,
    /// Reference key if output was externalized (exceeded size threshold).
    pub output_ref: Option<String>,
    pub output_size: u32,
    pub attempt: u16,
    pub created_at: DateTime<Utc>,
}

/// Logical externalized state, independent of backend compression/encryption.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExternalizedEntry {
    pub instance_id: InstanceId,
    pub ref_key: String,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_output_serde_roundtrip() {
        let bo = BlockOutput {
            id: Uuid::now_v7(),
            instance_id: InstanceId::new(),
            block_id: BlockId::new("step_1"),
            output: serde_json::json!({"result": "ok"}),
            output_ref: None,
            output_size: 42,
            attempt: 1,
            created_at: Utc::now(),
        };
        let json = serde_json::to_string(&bo).unwrap();
        let back: BlockOutput = serde_json::from_str(&json).unwrap();
        assert_eq!(back.block_id.as_str(), "step_1");
        assert_eq!(back.output["result"], "ok");
        assert_eq!(back.output_size, 42);
        assert_eq!(back.attempt, 1);
    }

    #[test]
    fn block_output_with_output_ref() {
        let bo = BlockOutput {
            id: Uuid::now_v7(),
            instance_id: InstanceId::new(),
            block_id: BlockId::new("s"),
            output: serde_json::json!(null),
            output_ref: Some("ext:ref:key".into()),
            output_size: 0,
            attempt: 0,
            created_at: Utc::now(),
        };
        let json = serde_json::to_string(&bo).unwrap();
        let back: BlockOutput = serde_json::from_str(&json).unwrap();
        assert_eq!(back.output_ref.as_deref(), Some("ext:ref:key"));
    }
}
