//! Write-time scope and revision checks for durable compensation progress.
use orch8_types::{continuity_advanced::CompensationRunRecord, error::StorageError, ids::TenantId};

pub(crate) fn validate(
    tenant_id: &TenantId,
    expected_version: u64,
    next: &CompensationRunRecord,
) -> Result<(), StorageError> {
    if next.tenant_id != *tenant_id
        || expected_version.checked_add(1) != Some(next.version)
        || i64::try_from(next.version).is_err()
    {
        return Err(StorageError::Constraint(
            "compensation tenant must match and version must advance by one within the storage range".into(),
        ));
    }
    Ok(())
}
