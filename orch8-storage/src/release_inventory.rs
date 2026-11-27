use chrono::{DateTime, Utc};
use orch8_types::release::ReleaseState;
use uuid::Uuid;

pub struct ReleasePageFilter {
    pub before: Option<(DateTime<Utc>, Uuid)>,
    pub state: Option<ReleaseState>,
    pub terms: Vec<String>,
}
