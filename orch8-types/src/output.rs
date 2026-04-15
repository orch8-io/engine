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
