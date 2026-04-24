use serde::Deserialize;

use crate::ids::{Namespace, SequenceId, TenantId};
use crate::instance::{InstanceState, Priority};

#[derive(Debug, Clone, Default, Deserialize)]
pub struct InstanceFilter {
    pub tenant_id: Option<TenantId>,
    pub namespace: Option<Namespace>,
    pub sequence_id: Option<SequenceId>,
    pub states: Option<Vec<InstanceState>>,
    /// JSONB containment query for metadata filtering.
    pub metadata_filter: Option<serde_json::Value>,
    pub priority: Option<Priority>,
}

#[derive(Debug, Clone)]
pub struct Pagination {
    pub offset: u64,
    pub limit: u32,
    /// When `true`, order results by oldest `updated_at` first.
    /// Default is `false` (newest first) to preserve existing behavior.
    pub sort_ascending: bool,
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: 100,
            sort_ascending: false,
        }
    }
}

impl Pagination {
    /// Cap limit to a maximum of 1000 rows.
    #[must_use]
    pub fn capped(mut self) -> Self {
        self.limit = self.limit.min(1000);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pagination_default_values() {
        let p = Pagination::default();
        assert_eq!(p.offset, 0);
        assert_eq!(p.limit, 100);
    }

    #[test]
    fn pagination_capped_no_change_when_under_limit() {
        let p = Pagination {
            offset: 0,
            limit: 500,
            sort_ascending: false,
        }
        .capped();
        assert_eq!(p.limit, 500);
    }

    #[test]
    fn pagination_capped_reduces_when_over_limit() {
        let p = Pagination {
            offset: 0,
            limit: 5000,
            sort_ascending: false,
        }
        .capped();
        assert_eq!(p.limit, 1000);
    }

    #[test]
    fn pagination_capped_at_boundary() {
        let p = Pagination {
            offset: 0,
            limit: 1000,
            sort_ascending: false,
        }
        .capped();
        assert_eq!(p.limit, 1000);
    }

    #[test]
    fn pagination_capped_preserves_offset() {
        let p = Pagination {
            offset: 42,
            limit: 2000,
            sort_ascending: false,
        }
        .capped();
        assert_eq!(p.offset, 42);
        assert_eq!(p.limit, 1000);
    }

    #[test]
    fn instance_filter_default_all_none() {
        let f = InstanceFilter::default();
        assert!(f.tenant_id.is_none());
        assert!(f.namespace.is_none());
        assert!(f.sequence_id.is_none());
        assert!(f.states.is_none());
        assert!(f.metadata_filter.is_none());
        assert!(f.priority.is_none());
    }
}
