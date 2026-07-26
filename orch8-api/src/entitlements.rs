//! Provider-neutral plan entitlements and admission decisions.
//!
//! Billing systems may populate a catalog, but API admission depends only on
//! this stable contract. No payment-provider identifiers enter engine state.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use orch8_types::ids::{Namespace, TenantId};
use serde::{Deserialize, Serialize};

use crate::{AppState, error::ApiError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanEntitlements {
    pub plan_id: String,
    pub max_active_instances: u64,
    pub max_batch_instances: u32,
    pub max_context_bytes: u32,
    #[serde(default)]
    pub allowed_namespaces: BTreeSet<String>,
    #[serde(default)]
    pub features: BTreeSet<String>,
}

impl PlanEntitlements {
    #[must_use]
    pub fn unlimited() -> Self {
        Self {
            plan_id: "self_managed".into(),
            max_active_instances: u64::MAX,
            max_batch_instances: 10_000,
            max_context_bytes: u32::MAX,
            allowed_namespaces: BTreeSet::new(),
            features: BTreeSet::new(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.plan_id.is_empty()
            || self.max_active_instances == 0
            || self.max_batch_instances == 0
            || self.max_batch_instances > 10_000
            || self.max_context_bytes == 0
        {
            return Err("plan limits must be non-zero and within API bounds".into());
        }
        Ok(())
    }
}

pub trait EntitlementProvider: Send + Sync + 'static {
    fn entitlements_for(&self, tenant_id: &TenantId) -> PlanEntitlements;
}

#[derive(Default)]
pub struct UnlimitedEntitlements;

impl EntitlementProvider for UnlimitedEntitlements {
    fn entitlements_for(&self, _tenant_id: &TenantId) -> PlanEntitlements {
        PlanEntitlements::unlimited()
    }
}

pub struct StaticEntitlementCatalog {
    plans: HashMap<TenantId, PlanEntitlements>,
    fallback: PlanEntitlements,
}

impl StaticEntitlementCatalog {
    pub fn new(
        plans: HashMap<TenantId, PlanEntitlements>,
        fallback: PlanEntitlements,
    ) -> Result<Self, String> {
        fallback.validate()?;
        for plan in plans.values() {
            plan.validate()?;
        }
        Ok(Self { plans, fallback })
    }
}

impl EntitlementProvider for StaticEntitlementCatalog {
    fn entitlements_for(&self, tenant_id: &TenantId) -> PlanEntitlements {
        self.plans.get(tenant_id).unwrap_or(&self.fallback).clone()
    }
}

pub fn admit_instances(
    state: &AppState,
    tenant_id: &TenantId,
    namespaces: &[Namespace],
    requested: usize,
    largest_context_bytes: usize,
) -> Result<PlanEntitlements, ApiError> {
    let plan = state.entitlements.entitlements_for(tenant_id);
    plan.validate().map_err(|error| {
        ApiError::Unavailable(format!("invalid entitlement configuration: {error}"))
    })?;
    let requested = u64::try_from(requested)
        .map_err(|_| ApiError::RateLimited("requested instance count exceeds plan".into()))?;
    if requested > u64::from(plan.max_batch_instances) {
        return Err(ApiError::RateLimited(format!(
            "plan {} permits at most {} instances per request",
            plan.plan_id, plan.max_batch_instances
        )));
    }
    if largest_context_bytes > plan.max_context_bytes as usize {
        return Err(ApiError::PayloadTooLarge(format!(
            "plan {} limits context to {} bytes",
            plan.plan_id, plan.max_context_bytes
        )));
    }
    for namespace in namespaces {
        if !plan.allowed_namespaces.is_empty()
            && !plan.allowed_namespaces.contains(namespace.as_str())
        {
            return Err(ApiError::Forbidden(format!(
                "namespace is not entitled by plan {}",
                plan.plan_id
            )));
        }
    }
    Ok(plan)
}

#[must_use]
pub fn unlimited_provider() -> Arc<dyn EntitlementProvider> {
    Arc::new(UnlimitedEntitlements)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_is_tenant_scoped_and_provider_neutral() {
        let tenant = TenantId::new("tenant-a").unwrap();
        let plan = PlanEntitlements {
            plan_id: "team".into(),
            max_active_instances: 10,
            max_batch_instances: 5,
            max_context_bytes: 1024,
            allowed_namespaces: BTreeSet::from(["prod".into()]),
            features: BTreeSet::from(["continuity".into()]),
        };
        let catalog = StaticEntitlementCatalog::new(
            HashMap::from([(tenant.clone(), plan)]),
            PlanEntitlements::unlimited(),
        )
        .unwrap();
        assert_eq!(catalog.entitlements_for(&tenant).plan_id, "team");
        assert_eq!(
            catalog
                .entitlements_for(&TenantId::new("tenant-b").unwrap())
                .plan_id,
            "self_managed"
        );
    }

    #[test]
    fn invalid_zero_limit_fails_closed() {
        let mut plan = PlanEntitlements::unlimited();
        plan.max_active_instances = 0;
        assert!(plan.validate().is_err());
    }
}

#[cfg(test)]
#[path = "entitlements_boundary_tests.rs"]
mod boundary_tests;
