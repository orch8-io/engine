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
