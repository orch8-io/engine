//! Immutable callback identity from an authorized frozen run, never input JSON.
//! Kept internal until the complete scoped SDK read/lifecycle surface is ready.
use super::{CatalogScope, StepContext, StepHandler};
use crate::HandlerError;
use orch8_types::{
    continuity::EffectId,
    error::StepError,
    ids::{BlockId, InstanceId},
};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CatalogCallbackContext {
    account_id: String,
    catalog_id: String,
    owner_epoch: i64,
    run_id: InstanceId,
    block_id: BlockId,
    attempt: u32,
    effect_id: EffectId,
}
impl CatalogCallbackContext {
    pub(crate) fn account_id(&self) -> &str {
        &self.account_id
    }
    pub(crate) fn catalog_id(&self) -> &str {
        &self.catalog_id
    }
    pub(crate) fn owner_epoch(&self) -> i64 {
        self.owner_epoch
    }
    pub(crate) fn run_id(&self) -> InstanceId {
        self.run_id
    }
    pub(crate) fn block_id(&self) -> &BlockId {
        &self.block_id
    }
    pub(crate) fn attempt(&self) -> u32 {
        self.attempt
    }
    pub(crate) fn effect_id(&self) -> EffectId {
        self.effect_id
    }
}

pub(crate) trait CatalogStepHandler: Send + Sync {
    fn execute(
        &self,
        context: CatalogCallbackContext,
        step_name: String,
        input: String,
    ) -> Result<String, HandlerError>;
}

pub(super) enum BoundHandler {
    #[cfg(test)]
    Legacy(Arc<dyn StepHandler>),
    Contextual(Arc<dyn CatalogStepHandler>),
}
impl BoundHandler {
    /// Called only after `catalog_callback_scope` proves this original claim.
    /// `NativeInvocation` independently revalidates it at physical host entry.
    pub(super) fn for_invocation(
        &self,
        scope: &CatalogScope,
        ctx: &StepContext,
    ) -> Result<Arc<dyn StepHandler>, StepError> {
        match self {
            #[cfg(test)]
            Self::Legacy(handler) => Ok(handler.clone()),
            Self::Contextual(handler) => {
                let admission = ctx.effect_admission.as_ref().ok_or(StepError::ClaimLost)?;
                let effect_id = admission
                    .effect_id_for(&ctx.tenant_id, ctx.instance_id, &ctx.block_id, ctx.attempt)
                    .ok_or(StepError::ClaimLost)?;
                Ok(Arc::new(Invocation {
                    handler: handler.clone(),
                    context: CatalogCallbackContext {
                        account_id: scope.account_id().to_owned(),
                        catalog_id: scope.catalog_id().to_owned(),
                        owner_epoch: scope.epoch(),
                        run_id: ctx.instance_id,
                        block_id: ctx.block_id.clone(),
                        attempt: ctx.attempt,
                        effect_id,
                    },
                }))
            }
        }
    }
}

// This adapter follows the existing physically owned foreign-call path. Its
// handler and frozen context survive observer cancellation together; final
// foreign destruction still runs before callback/registration capacity returns.
struct Invocation {
    handler: Arc<dyn CatalogStepHandler>,
    context: CatalogCallbackContext,
}
impl StepHandler for Invocation {
    fn execute(&self, step_name: String, input: String) -> Result<String, HandlerError> {
        self.handler.execute(self.context.clone(), step_name, input)
    }
}
