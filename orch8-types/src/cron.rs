use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::{Namespace, SequenceId, TenantId};

/// A cron schedule that periodically creates instances of a sequence.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CronSchedule {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub namespace: Namespace,
    pub sequence_id: SequenceId,
    /// Standard cron expression (e.g. "0 9 * * MON-FRI").
    pub cron_expr: String,
    pub timezone: String,
    pub enabled: bool,
    /// Extra metadata to inject into created instances.
    #[serde(default)]
    pub metadata: serde_json::Value,
    pub last_triggered_at: Option<DateTime<Utc>>,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
