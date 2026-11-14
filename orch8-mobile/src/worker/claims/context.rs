//! Bounded immutable claim context; never refreshed from duplicate deliveries.
use super::ClaimKey;
use orch8_engine::remote_worker::RemoteTask;
use orch8_types::delegation_definition::claim::DelegationClaimContext;

pub(super) fn encode(task: &RemoteTask, worker: &str) -> Result<Option<String>, sqlx::Error> {
    task.validate_delegation(worker).map_err(|_| invalid())?;
    let record = task
        .delegation
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|_| invalid())?;
    if record.as_ref().is_some_and(|value| value.len() > 16_384) {
        return Err(invalid());
    }
    Ok(record)
}
pub(super) fn decode(
    record: Option<String>,
    key: &ClaimKey,
) -> Result<Option<DelegationClaimContext>, sqlx::Error> {
    let Some(record) = record else {
        return Ok(None);
    };
    if record.len() > 16_384 {
        return Err(invalid());
    }
    let context: DelegationClaimContext = serde_json::from_str(&record).map_err(|_| invalid())?;
    context.validate().map_err(|_| invalid())?;
    if context.scope.task_id != key.task_id
        || context.scope.worker_id != key.worker_id
        || context.scope.claim_epoch != key.claim_epoch
    {
        return Err(invalid());
    }
    Ok(Some(context))
}
pub(super) fn invalid() -> sqlx::Error {
    sqlx::Error::Protocol("invalid or changed mobile claim context".into())
}

#[cfg(test)]
mod tests;
