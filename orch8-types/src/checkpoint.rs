use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::InstanceId;

/// A checkpoint captures execution state at a point in time for efficient recovery.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Checkpoint {
    pub id: Uuid,
    pub instance_id: InstanceId,
    /// Snapshot of execution state: completed block IDs, context, etc.
    pub checkpoint_data: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_serde_roundtrip() {
        let cp = Checkpoint {
            id: Uuid::now_v7(),
            instance_id: InstanceId(Uuid::now_v7()),
            checkpoint_data: serde_json::json!({"completed_blocks": ["a", "b"], "context_snapshot": {"x": 1}}),
            created_at: Utc::now(),
        };
        let json = serde_json::to_string(&cp).unwrap();
        let back: Checkpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(back.checkpoint_data["completed_blocks"][0], "a");
        assert_eq!(back.checkpoint_data["context_snapshot"]["x"], 1);
    }
}
