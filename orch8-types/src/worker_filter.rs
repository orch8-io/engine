use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

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

#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct WorkerTaskStats {
    pub by_state: HashMap<String, u64>,
    pub by_handler: HashMap<String, HashMap<String, u64>>,
    pub active_workers: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_task_filter_default_all_none() {
        let f = WorkerTaskFilter::default();
        assert!(f.tenant_id.is_none());
        assert!(f.states.is_none());
        assert!(f.handler_name.is_none());
        assert!(f.worker_id.is_none());
        assert!(f.queue_name.is_none());
    }

    #[test]
    fn worker_task_stats_default_empty() {
        let s = WorkerTaskStats::default();
        assert!(s.by_state.is_empty());
        assert!(s.by_handler.is_empty());
        assert!(s.active_workers.is_empty());
    }

    #[test]
    fn worker_task_stats_serialize() {
        let mut s = WorkerTaskStats::default();
        s.by_state.insert("pending".into(), 5);
        s.active_workers.push("w-1".into());
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("\"pending\":5"));
        assert!(json.contains("w-1"));
    }
}
