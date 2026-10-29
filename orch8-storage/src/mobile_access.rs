//! Storage descriptors supplied by engine-owned mobile handles, not bearer auth.

/// The original unscoped mobile API and a current catalog session have distinct
/// authority. A catalog session may read its earlier execution history; writes
/// additionally prove that nonterminal work belongs to that execution epoch.
/// Foreign callers must not construct these descriptors from workflow JSON.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MobileInstanceAccess {
    Legacy,
    Catalog {
        account_id: String,
        catalog_id: String,
        session_epoch: i64,
    },
}

/// Validate the trusted command envelope independently of encrypted data.
pub(crate) fn completion_step(
    signal: &orch8_types::signal::Signal,
) -> Result<&str, orch8_types::error::StorageError> {
    use orch8_types::{error::StorageError, signal::SignalType};
    if let SignalType::Custom(name) = &signal.signal_type
        && let Some(step) = name
            .strip_prefix("human_input:")
            .filter(|step| !step.is_empty())
        && !signal.delivered
        && signal.delivered_at.is_none()
    {
        return Ok(step);
    }
    Err(StorageError::Query(
        "invalid mobile step completion envelope".into(),
    ))
}
