//! Shared test-data builders (T2 harness H6).
//!
//! One place for the `SequenceDefinition` / `TaskInstance` / `StepDef`
//! literals that test files used to re-declare (`create_sequence` 15 times,
//! `create_instance` 13 times, ...). When a type grows a field, only this
//! file changes.
//!
//! ```ignore
//! use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder, step};
//! let seq = SequenceBuilder::new("t").step("a", "noop").build();
//! let inst = InstanceBuilder::for_sequence(&seq).due_now().build();
//! storage.create_sequence(&seq).await?;
//! storage.create_instance(&inst).await?;
//! ```

use chrono::{DateTime, Duration, Utc};
use serde_json::{Value, json};

use orch8_types::context::ExecutionContext;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{
    BlockDefinition, RetryPolicy, SEQUENCE_SCHEMA_VERSION, SequenceDefinition, SequenceStatus,
    StepDef,
};

/// A step with every optional field at its serde default (so `cancellable`
/// is `true`, as in a user-submitted sequence). Mutate the returned value for
/// anything else.
#[must_use]
pub fn step(id: &str, handler: &str) -> StepDef {
    step_with_params(id, handler, json!({}))
}

/// [`step`] with `params`.
#[must_use]
pub fn step_with_params(id: &str, handler: &str, params: Value) -> StepDef {
    let mut step: StepDef = serde_json::from_value(json!({"id": id, "handler": handler}))
        .expect("minimal StepDef JSON must deserialize");
    step.params = params;
    step
}

/// [`step`] wrapped as a [`BlockDefinition`].
#[must_use]
pub fn step_block(id: &str, handler: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(step(id, handler)))
}

/// Exponential retry policy with `max_attempts` and `initial_backoff`,
/// doubling, capped at 60 s.
#[must_use]
pub fn retry_policy(max_attempts: u32, initial_backoff: std::time::Duration) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        initial_backoff,
        max_backoff: std::time::Duration::from_secs(60),
        backoff_multiplier: 2.0,
        retry_if: None,
        non_retryable_codes: None,
    }
}

/// Builder for a [`SequenceDefinition`] in `Production` status.
#[must_use]
pub struct SequenceBuilder {
    seq: SequenceDefinition,
}

impl SequenceBuilder {
    /// Empty sequence for `tenant`, namespace `default`, a unique name.
    pub fn new(tenant: &str) -> Self {
        let id = SequenceId::new();
        Self {
            seq: SequenceDefinition {
                embed: None,
                sub_tenant: None,
                schema: None,
                schema_version: SEQUENCE_SCHEMA_VERSION,
                id,
                tenant_id: TenantId::unchecked(tenant),
                namespace: Namespace::new("default"),
                name: format!("seq-{}", uuid::Uuid::now_v7().simple()),
                version: 1,
                deprecated: false,
                blocks: Vec::new(),
                interceptors: None,
                input_schema: None,
                sla: None,
                on_failure: None,
                on_cancel: None,
                placement: None,
                created_at: Utc::now(),
                status: SequenceStatus::Production,
            },
        }
    }

    pub fn namespace(mut self, namespace: &str) -> Self {
        self.seq.namespace = Namespace::new(namespace);
        self
    }

    pub fn name(mut self, name: &str) -> Self {
        self.seq.name = name.into();
        self
    }

    pub fn version(mut self, version: i32) -> Self {
        self.seq.version = version;
        self
    }

    pub fn status(mut self, status: SequenceStatus) -> Self {
        self.seq.status = status;
        self
    }

    /// Append several blocks.
    pub fn blocks(mut self, blocks: impl IntoIterator<Item = BlockDefinition>) -> Self {
        self.seq.blocks.extend(blocks);
        self
    }

    /// Append a block.
    pub fn block(mut self, block: BlockDefinition) -> Self {
        self.seq.blocks.push(block);
        self
    }

    /// Append a [`step`].
    pub fn step(self, id: &str, handler: &str) -> Self {
        self.block(step_block(id, handler))
    }

    /// Append a fully customised step.
    pub fn step_def(self, step: StepDef) -> Self {
        self.block(BlockDefinition::Step(Box::new(step)))
    }

    #[must_use]
    pub fn build(self) -> SequenceDefinition {
        self.seq
    }
}

/// Builder for a [`TaskInstance`] of a sequence. Defaults: `Scheduled`, not
/// due (`next_fire_at = None`), empty context, `Normal` priority, UTC.
#[must_use]
pub struct InstanceBuilder {
    inst: TaskInstance,
}

impl InstanceBuilder {
    /// Instance of `seq`, in the sequence's tenant and namespace.
    pub fn for_sequence(seq: &SequenceDefinition) -> Self {
        Self::new(seq.tenant_id.as_str(), seq.namespace.as_str(), seq.id)
    }

    /// Instance of an arbitrary sequence id (the sequence need not exist).
    pub fn new(tenant: &str, namespace: &str, sequence_id: SequenceId) -> Self {
        let now = Utc::now();
        Self {
            inst: TaskInstance {
                claimed_at: None,
                id: InstanceId::new(),
                sub_tenant: None,
                sequence_id,
                tenant_id: TenantId::unchecked(tenant),
                namespace: Namespace::new(namespace),
                state: InstanceState::Scheduled,
                next_fire_at: None,
                priority: Priority::Normal,
                timezone: "UTC".into(),
                metadata: json!({}),
                context: ExecutionContext::default(),
                concurrency_key: None,
                max_concurrency: None,
                idempotency_key: None,
                session_id: None,
                parent_instance_id: None,
                budget: None,
                created_at: now,
                updated_at: now,
            },
        }
    }

    pub fn state(mut self, state: InstanceState) -> Self {
        self.inst.state = state;
        self
    }

    pub fn next_fire_at(mut self, at: Option<DateTime<Utc>>) -> Self {
        self.inst.next_fire_at = at;
        self
    }

    /// `Scheduled` and due one second ago, so the next claim picks it up.
    pub fn due_now(self) -> Self {
        self.state(InstanceState::Scheduled)
            .next_fire_at(Some(Utc::now() - Duration::seconds(1)))
    }

    /// `Scheduled` and due at `at` (e.g. a virtual clock's `now`).
    pub fn due_at(self, at: DateTime<Utc>) -> Self {
        self.state(InstanceState::Scheduled).next_fire_at(Some(at))
    }

    pub fn data(mut self, data: Value) -> Self {
        self.inst.context.data = data;
        self
    }

    /// Set `context.config` (default `null`).
    pub fn config(mut self, config: Value) -> Self {
        self.inst.context.config = config;
        self
    }

    /// Replace the whole execution context.
    pub fn context(mut self, context: ExecutionContext) -> Self {
        self.inst.context = context;
        self
    }

    pub fn metadata(mut self, metadata: Value) -> Self {
        self.inst.metadata = metadata;
        self
    }

    pub fn priority(mut self, priority: Priority) -> Self {
        self.inst.priority = priority;
        self
    }

    pub fn idempotency_key(mut self, key: &str) -> Self {
        self.inst.idempotency_key = Some(key.into());
        self
    }

    pub fn concurrency(mut self, key: &str, max: u32) -> Self {
        self.inst.concurrency_key = Some(key.into());
        self.inst.max_concurrency = Some(max);
        self
    }

    pub fn parent(mut self, parent: InstanceId) -> Self {
        self.inst.parent_instance_id = Some(parent);
        self
    }

    pub fn id(mut self, id: InstanceId) -> Self {
        self.inst.id = id;
        self
    }

    pub fn created_at(mut self, at: DateTime<Utc>) -> Self {
        self.inst.created_at = at;
        self.inst.updated_at = at;
        self
    }

    #[must_use]
    pub fn build(self) -> TaskInstance {
        self.inst
    }
}

/// The id of the first block of `seq` (handy for output lookups).
#[must_use]
pub fn first_block_id(seq: &SequenceDefinition) -> BlockId {
    match seq.blocks.first().expect("sequence has no blocks") {
        BlockDefinition::Step(s) => s.id.clone(),
        other => panic!("first block is not a step: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_uses_serde_defaults_like_a_submitted_sequence() {
        let s = step("a", "noop");
        assert!(s.cancellable);
        assert_eq!(s.id.as_str(), "a");
        assert_eq!(s.handler, "noop");
        assert!(s.retry.is_none() && s.delay.is_none() && s.timeout.is_none());
    }

    #[test]
    fn sequence_builder_appends_blocks_in_order() {
        let seq = SequenceBuilder::new("t")
            .step("a", "noop")
            .step("b", "log")
            .build();
        let ids: Vec<_> = seq
            .blocks
            .iter()
            .map(|b| match b {
                BlockDefinition::Step(s) => s.id.as_str().to_owned(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(ids, ["a", "b"]);
        assert_eq!(seq.tenant_id.as_str(), "t");
        assert_eq!(seq.status, SequenceStatus::Production);
    }

    #[test]
    fn instance_builder_inherits_sequence_scope_and_is_not_due_by_default() {
        let seq = SequenceBuilder::new("t").namespace("ns").build();
        let inst = InstanceBuilder::for_sequence(&seq).build();
        assert_eq!(inst.sequence_id, seq.id);
        assert_eq!(inst.tenant_id.as_str(), "t");
        assert_eq!(inst.namespace.as_str(), "ns");
        assert_eq!(inst.state, InstanceState::Scheduled);
        assert!(inst.next_fire_at.is_none());
    }

    #[test]
    fn due_now_is_in_the_past() {
        let seq = SequenceBuilder::new("t").build();
        let inst = InstanceBuilder::for_sequence(&seq).due_now().build();
        assert!(inst.next_fire_at.expect("due") < Utc::now());
    }
}
