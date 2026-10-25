//! Optional runtime-owned selection before durable effect preparation.
use super::{HandlerRegistry, StepHandler};
use crate::error::EngineError;
use orch8_storage::StorageBackend;
use orch8_types::ids::{InstanceId, TenantId};
use std::sync::Arc;

/// Immutable execution identity supplied by the scheduler, never handler params.
pub struct HandlerLookup<'a> {
    pub storage: &'a Arc<dyn StorageBackend>,
    pub instance_id: InstanceId,
    pub tenant_id: &'a TenantId,
    pub claim_token: Option<chrono::DateTime<chrono::Utc>>,
    pub name: &'a str,
}

/// A selected function and its effect policy travel together. Deferred handlers
/// must admit the opaque effect capability immediately before external entry.
pub struct ResolvedHandler {
    pub handler: StepHandler,
    pub deferred_effect_admission: bool,
}

/// Trusted embedding policy. `None` explicitly delegates to the global registry.
/// A scoped resolver must return an error for an unauthorized owner rather than
/// delegate to an ambient handler. Selected closures should retain owner identity
/// without pinning foreign objects across asynchronous preparation.
#[async_trait::async_trait]
pub trait HandlerResolver: Send + Sync {
    fn contains(&self, name: &str) -> bool;
    async fn resolve(
        &self,
        lookup: HandlerLookup<'_>,
    ) -> Result<Option<ResolvedHandler>, EngineError>;
}

pub(super) enum Selection<'a> {
    Global(&'a StepHandler, bool),
    Contextual(ResolvedHandler),
}
impl Selection<'_> {
    pub(super) fn handler(&self) -> &StepHandler {
        match self {
            Self::Global(handler, _) => handler,
            Self::Contextual(entry) => &entry.handler,
        }
    }
    pub(super) fn deferred(&self) -> bool {
        match self {
            Self::Global(_, deferred) => *deferred,
            Self::Contextual(entry) => entry.deferred_effect_admission,
        }
    }
}
impl HandlerRegistry {
    /// Install trusted contextual selection. Contextual entries take precedence
    /// over real handlers and mocks; direct `get()` remains an unscoped API.
    pub fn set_resolver(&mut self, resolver: Arc<dyn HandlerResolver>) {
        self.resolver = Some(resolver);
    }

    pub(super) async fn resolve_contextual(
        &self,
        lookup: HandlerLookup<'_>,
    ) -> Result<Option<Selection<'_>>, EngineError> {
        match &self.resolver {
            Some(resolver) => Ok(resolver.resolve(lookup).await?.map(Selection::Contextual)),
            None => Ok(None),
        }
    }

    pub(super) fn resolve_global(&self, name: &str) -> Result<Selection<'_>, EngineError> {
        self.get(name)
            .map(|handler| Selection::Global(handler, self.defers_effect_admission(name)))
            .ok_or_else(|| {
                let names = self.handler_names();
                let message = match orch8_types::suggest::did_you_mean(name, &names) {
                    Some(s) => format!("{name} (did you mean: {s}?)"),
                    None => name.to_owned(),
                };
                EngineError::HandlerNotFound(message)
            })
    }
}
