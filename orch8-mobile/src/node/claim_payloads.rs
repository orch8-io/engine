//! Fixed-origin, separately bounded input/definition reads. Content is not admission.
use super::{
    MobileError, NodeClient, RemoteTask, network_err, read_capped, urlencode_path_segment,
};
use orch8_types::delegation_definition::{
    DefinitionGraphLimits,
    claim::DelegationClaimContext,
    input::{DelegationInputEnvelope, InputLimits, VerifiedDelegationInput},
    wire::DefinitionWireEnvelope,
};

impl NodeClient {
    #[allow(dead_code)] // Scoped bridge remains opted out until native admission is complete.
    pub(crate) async fn claim_definition(
        &self,
        task: &RemoteTask,
    ) -> Result<DefinitionWireEnvelope, MobileError> {
        let context = self.claim_context(task)?;
        let envelope: DefinitionWireEnvelope = self.claim_envelope(context, "definition").await?;
        envelope
            .verify(
                &context.scope,
                &context.definition,
                DefinitionGraphLimits::default(),
            )
            .map_err(|_| invalid())?;
        Ok(envelope)
    }
    #[allow(dead_code)] // Separate input verification is used by the scoped bridge.
    pub(crate) async fn claim_input(
        &self,
        task: &RemoteTask,
    ) -> Result<VerifiedDelegationInput, MobileError> {
        let context = self.claim_context(task)?;
        let envelope: DelegationInputEnvelope = self.claim_envelope(context, "input").await?;
        envelope
            .verify(
                &context.scope,
                &context.request_sha256,
                &context.input,
                InputLimits::default(),
            )
            .map_err(|_| invalid())
    }
    fn claim_context<'a>(
        &self,
        task: &'a RemoteTask,
    ) -> Result<&'a DelegationClaimContext, MobileError> {
        task.validate_delegation(&self.worker_id())
            .map_err(|_| invalid())?;
        task.delegation.as_ref().ok_or_else(invalid)
    }
    async fn claim_envelope<T: serde::de::DeserializeOwned>(
        &self,
        context: &DelegationClaimContext,
        resource: &'static str,
    ) -> Result<T, MobileError> {
        let path = format!(
            "workers/tasks/{}/{resource}?worker_id={}&claim_epoch={}",
            context.scope.task_id,
            urlencode_path_segment(&context.scope.worker_id),
            context.scope.claim_epoch
        );
        let response = self
            .lease
            .get(&path)
            .await
            .map_err(|error| network_err("delegation payload", &error))?;
        if !response.status().is_success() {
            return Err(MobileError::Engine {
                detail: format!("delegation {resource}: HTTP {}", response.status().as_u16()),
            });
        }
        let bytes = read_capped(response).await?;
        serde_json::from_slice(&bytes).map_err(|_| invalid())
    }
}
fn invalid() -> MobileError {
    MobileError::Engine {
        detail: "invalid or mismatched delegation payload".into(),
    }
}

#[cfg(test)]
pub(super) mod tests;
