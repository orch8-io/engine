//! Outcomes of adopting a child while holding its parent's execution claim.
use orch8_types::instance::TaskInstance;

/// A held parent claim does not imply a previously selected child is eligible.
#[derive(Debug, Clone)]
pub enum ChildAdoption {
    /// The child disappeared, changed activation, was cancelled or superseded,
    /// or does not belong to this parent and owner scope. Nothing was changed.
    Ineligible,
    /// Fresh committed child, with a failed child's retry completed atomically.
    Adopted(Box<TaskInstance>),
}
