use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::ids::TenantId;
use crate::worker::WorkerTaskState;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct WorkerTaskFilter {
    pub tenant_id: Option<TenantId>,
    pub states: Option<Vec<WorkerTaskState>>,
    pub handler_name: Option<String>,
    pub worker_id: Option<String>,
    pub queue_name: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WorkerTaskStats {
    pub by_state: HashMap<String, u64>,
    pub by_handler: HashMap<String, HashMap<String, u64>>,
    pub active_workers: Vec<String>,
}
