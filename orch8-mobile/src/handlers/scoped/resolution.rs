//! Scoped native overrides never replace another catalog's builtin or inherit
//! a legacy runtime-global callback. Selection holds only weak foreign identity.
use super::{Binding, CatalogHandlers, CatalogScope, lock, storage_error, unavailable};
use orch8_engine::{
    error::EngineError,
    handlers::{HandlerLookup, HandlerRegistry, HandlerResolver, ResolvedHandler},
};
use orch8_types::error::StepError;
use std::sync::{Arc, Weak};

struct Resolver {
    owner: Weak<CatalogHandlers>,
    builtins: Arc<HandlerRegistry>,
    sdk: String,
}
impl CatalogHandlers {
    pub(crate) fn install_resolver(
        self: &Arc<Self>,
        registry: &mut HandlerRegistry,
        builtin_names: &[&str],
    ) {
        self.install_resolver_for_sdk(registry, builtin_names, env!("CARGO_PKG_VERSION"));
    }
    pub(crate) fn install_resolver_for_sdk(
        self: &Arc<Self>,
        registry: &mut HandlerRegistry,
        builtin_names: &[&str],
        sdk: &str,
    ) {
        registry.set_resolver(Arc::new(Resolver {
            owner: Arc::downgrade(self),
            builtins: Arc::new(crate::builtins::registry_with(builtin_names)),
            sdk: sdk.to_owned(),
        }));
    }
}
#[async_trait::async_trait]
impl HandlerResolver for Resolver {
    fn requires_local_dispatch(&self, tenant: &orch8_types::ids::TenantId) -> bool {
        tenant.as_str().starts_with("mobile-account-")
    }
    fn contains(&self, name: &str) -> bool {
        self.builtins.contains(name)
            || self.owner.upgrade().is_some_and(|owner| {
                lock(&owner.bindings)
                    .live
                    .keys()
                    .any(|(_, candidate)| candidate == name)
            })
    }
    async fn resolve(
        &self,
        lookup: HandlerLookup<'_>,
    ) -> Result<Option<ResolvedHandler>, EngineError> {
        if !lookup.tenant_id.as_str().starts_with("mobile-account-") {
            return Ok(None);
        }
        let lost = || EngineError::ClaimLost {
            instance_id: lookup.instance_id,
        };
        let owner = self.owner.upgrade().ok_or_else(lost)?;
        if owner.stop.is_cancelled() || !Arc::ptr_eq(lookup.storage, &owner.backend) {
            return Err(lost());
        }
        let token = lookup.claim_token.ok_or_else(lost)?;
        let scope = owner
            .storage
            .catalog_callback_scope(lookup.instance_id, lookup.tenant_id, token)
            .await
            .map_err(|error| {
                if error.is_transient() {
                    EngineError::Storage(error)
                } else {
                    lost()
                }
            })?;
        let policy = owner
            .storage
            .materialized_runtime_policy(lookup.instance_id)
            .await
            .map_err(|error| {
                if error.is_transient() {
                    EngineError::Storage(error)
                } else {
                    EngineError::InvalidConfig(
                        "catalog materialization policy is unavailable".into(),
                    )
                }
            })?;
        let available: std::collections::HashSet<String> = {
            let bindings = lock(&owner.bindings);
            bindings
                .live
                .keys()
                .filter(|(candidate, _)| candidate == &scope)
                .map(|(_, name)| name.clone())
                .chain(self.builtins.handler_names().into_iter().map(str::to_owned))
                .collect()
        };
        if !policy.permits(&self.sdk, &available) {
            return Err(EngineError::RuntimeUnavailable {
                instance_id: lookup.instance_id,
            });
        }
        let binding = {
            let bindings = lock(&owner.bindings);
            if owner.stop.is_cancelled() {
                return Err(lost());
            }
            bindings
                .live
                .get(&(scope.clone(), lookup.name.to_owned()))
                .map(Arc::downgrade)
        };
        let name = lookup.name.to_owned();
        if let Some(binding) = binding {
            return Ok(Some(native_entry(self.owner.clone(), binding, name)));
        }
        if !self.builtins.contains(&name) {
            return Err(
                if orch8_types::sequence::BUILTIN_HANDLER_NAMES.contains(&name.as_str()) {
                    EngineError::HandlerNotFound(name)
                } else {
                    EngineError::RuntimeUnavailable {
                        instance_id: lookup.instance_id,
                    }
                },
            );
        }
        Ok(Some(builtin_entry(
            self.owner.clone(),
            self.builtins.clone(),
            name,
            scope,
        )))
    }
}

fn native_entry(
    weak: Weak<CatalogHandlers>,
    binding: Weak<Binding>,
    name: String,
) -> ResolvedHandler {
    ResolvedHandler {
        deferred_effect_admission: true,
        handler: Box::new(move |ctx| {
            let (weak, binding, name) = (weak.clone(), binding.clone(), name.clone());
            Box::pin(async move {
                let owner = weak.upgrade().ok_or(StepError::ClaimLost)?;
                owner.invoke_selected(ctx, &name, Some(&binding)).await
            })
        }),
    }
}
fn builtin_entry(
    weak: Weak<CatalogHandlers>,
    builtins: Arc<HandlerRegistry>,
    name: String,
    scope: CatalogScope,
) -> ResolvedHandler {
    ResolvedHandler {
        deferred_effect_admission: false,
        handler: Box::new(move |ctx| {
            let (weak, expected, builtins, name) =
                (weak.clone(), scope.clone(), builtins.clone(), name.clone());
            Box::pin(async move {
                let owner = weak.upgrade().ok_or(StepError::ClaimLost)?;
                if owner.stop.is_cancelled() || !Arc::ptr_eq(&ctx.storage, &owner.backend) {
                    return Err(StepError::ClaimLost);
                }
                let actual = owner
                    .storage
                    .catalog_callback_scope(
                        ctx.instance_id,
                        &ctx.tenant_id,
                        ctx.claim_token.ok_or(StepError::ClaimLost)?,
                    )
                    .await
                    .map_err(storage_error)?;
                if actual != expected || owner.stop.is_cancelled() {
                    return Err(StepError::ClaimLost);
                }
                builtins.get(&name).ok_or_else(unavailable)?(ctx).await
            })
        }),
    }
}
