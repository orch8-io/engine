//! Compact publication transport references retained with an issued generation.
use crate::{delegation_publication::DelegationPublication, worker_claim::WorkerIssuedClaim};
use orch8_types::{
    delegation_definition::{
        claim::DelegationClaimContext,
        input::DelegationInputReference,
        protocol::DelegationResourceUsage,
        wire::{DefinitionBundleReference, DefinitionReadScope},
    },
    error::StorageError,
};

pub(crate) fn capture(
    issued: &WorkerIssuedClaim,
    publication: DelegationPublication,
    definition: DefinitionBundleReference,
    input: DelegationInputReference,
    usage: DelegationResourceUsage,
) -> Result<DelegationClaimContext, StorageError> {
    issued.validate()?;
    publication.validate()?;
    if issued.task_id != publication.task_id
        || issued.tenant_id != publication.delegation.tenant_id
        || issued.instance_id != publication.instance_id
        || !issued
            .invocation
            .as_ref()
            .is_some_and(crate::worker_invocation::WorkerInvocation::is_published_mailbox)
        || definition.sequence_sha256 != publication.sequence_sha256
    {
        return Err(invalid());
    }
    let context = DelegationClaimContext {
        version: 1,
        scope: DefinitionReadScope {
            tenant_id: issued.tenant_id.clone(),
            task_id: issued.task_id,
            worker_id: issued.claim.worker_id.clone(),
            claim_epoch: issued.claim.claim_epoch,
        },
        request_sha256: publication.request_sha256,
        expires_at: publication.delegation.expires_at,
        definition,
        input,
        usage,
    };
    context.validate()?;
    Ok(context)
}
pub(crate) fn invalid() -> StorageError {
    StorageError::Constraint("missing or corrupt issued delegation references".into())
}
