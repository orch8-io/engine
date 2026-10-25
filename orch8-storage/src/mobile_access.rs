//! Storage descriptors supplied by engine-owned mobile handles, not bearer auth.

/// The original unscoped mobile API and a current catalog session have distinct
/// authority. A catalog session may read its earlier execution history; writes
/// additionally prove that nonterminal work belongs to that execution epoch.
/// Foreign callers must not construct these descriptors from workflow JSON.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MobileInstanceAccess {
    Legacy,
    Catalog {
        account_id: String,
        catalog_id: String,
        session_epoch: i64,
    },
}
