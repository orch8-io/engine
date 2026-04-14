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
    pub output_size: i32,
    pub attempt: i16,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_output_serde_roundtrip() {
        let bo = BlockOutput {
            id: Uuid::now_v7(),
            instance_id: InstanceId(Uuid::now_v7()),
            block_id: BlockId("step_1".into()),
            output: serde_json::json!({"result": "ok"}),
            output_ref: None,
            output_size: 42,
            attempt: 1,
            created_at: Utc::now(),
        };
        let json = serde_json::to_string(&bo).unwrap();
        let back: BlockOutput = serde_json::from_str(&json).unwrap();
        assert_eq!(back.block_id.0, "step_1");
        assert_eq!(back.output["result"], "ok");
        assert_eq!(back.output_size, 42);
        assert_eq!(back.attempt, 1);
    }

    #[test]
    fn block_output_with_output_ref() {
        let bo = BlockOutput {
            id: Uuid::now_v7(),
            instance_id: InstanceId(Uuid::now_v7()),
            block_id: BlockId("s".into()),
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
