use std::future::Future;
use std::ops::Deref;
use std::time::Duration;

use orch8_types::clock::SharedClock;
use orch8_types::continuity::RuntimeId;
use orch8_types::error::StepError;

use crate::{EffectContext, Engine, EngineBuilder, Error, StepContext, Storage};

/// Opinionated embedded runtime for durable agents.
///
/// It enables Orch8's built-in `agent`, LLM, MCP/tool, memory, approval, and
/// continuity handlers on one local engine and gives that runtime a stable
/// identity for portable handoff. The wrapper dereferences to [`Engine`], so
/// the ordinary sequence, instance, signal, and effect-ledger APIs remain
/// available without a second client surface.
#[derive(Clone)]
pub struct AgentRuntime {
    engine: Engine,
    runtime_id: RuntimeId,
}

impl AgentRuntime {
    /// Start an agent runtime backed by `storage`.
    pub fn builder(storage: Storage) -> AgentRuntimeBuilder {
        AgentRuntimeBuilder {
            engine: Engine::builder().storage(storage),
            runtime_id: RuntimeId::new(),
        }
    }

    /// Stable identity used when this runtime exports or receives continuity.
    #[must_use]
    pub const fn runtime_id(&self) -> RuntimeId {
        self.runtime_id
    }

    /// Consume the preset wrapper and return the underlying engine.
    #[must_use]
    pub fn into_engine(self) -> Engine {
        self.engine
    }
}

impl Deref for AgentRuntime {
    type Target = Engine;

    fn deref(&self) -> &Self::Target {
        &self.engine
    }
}

/// Builder for [`AgentRuntime`].
#[must_use = "call .build().await to construct the agent runtime"]
pub struct AgentRuntimeBuilder {
    engine: EngineBuilder,
    runtime_id: RuntimeId,
}

impl AgentRuntimeBuilder {
    /// Override the generated portable runtime identity.
    pub fn runtime_id(mut self, runtime_id: RuntimeId) -> Self {
        self.runtime_id = runtime_id;
        self
    }

    /// Register a pure or internally durable custom handler.
    pub fn handler<F, Fut>(mut self, name: &str, handler: F) -> Self
    where
        F: Fn(StepContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<serde_json::Value, StepError>> + Send + 'static,
    {
        self.engine = self.engine.handler(name, handler);
        self
    }

    /// Register an externally visible tool/effect with durable evidence.
    pub fn effect_handler<F, Fut>(mut self, name: &str, handler: F) -> Self
    where
        F: Fn(EffectContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<serde_json::Value, StepError>> + Send + 'static,
    {
        self.engine = self.engine.effect_handler(name, handler);
        self
    }

    /// Scheduler cadence for the local agent runtime.
    pub fn tick_interval(mut self, interval: Duration) -> Self {
        self.engine = self.engine.tick_interval(interval);
        self
    }

    /// Tenant boundary for every local execution and portable capsule.
    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.engine = self.engine.tenant(tenant);
        self
    }

    /// Time source used by scheduling, retries, delays, and approvals.
    pub fn clock(mut self, clock: SharedClock) -> Self {
        self.engine = self.engine.clock(clock);
        self
    }

    /// Build the runtime and apply storage migrations/recovery.
    pub async fn build(self) -> Result<AgentRuntime, Error> {
        Ok(AgentRuntime {
            engine: self.engine.build().await?,
            runtime_id: self.runtime_id,
        })
    }
}

#[cfg(test)]
#[path = "agent_runtime_coverage_tests.rs"]
mod agent_runtime_coverage_tests;
