//! Durable retry boundary for imported composite states.
use chrono::{DateTime, Utc};
use orch8_types::{
    execution::{ExecutionNode, NodeState},
    ids::{BlockId, ExecutionNodeId},
    instance::TaskInstance,
    output::BlockOutput,
    sequence::RetryPolicy,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{control::ExecutionControl, error::EngineError, evaluator};

use super::{
    step::{RetryCounts, choose_retry},
    step_deferral::{PendingStepDeferrals, StepDeferral, defer_step},
};

mod children;

pub(crate) const CHILD_ATTEMPT_PREFIX: &str = "@orch8-retry:";

#[derive(Clone, Default, Deserialize, Serialize)]
struct State {
    attempts: u32,
    counts: RetryCounts,
    retry_at: Option<DateTime<Utc>>,
}

impl State {
    fn active_attempt(&self) -> u32 {
        self.attempts
            .saturating_sub(u32::from(self.retry_at.is_some()))
    }
}

/// Internal output slot isolated from the declared block's result/Map cursor.
pub(crate) fn state_block(node: ExecutionNodeId) -> BlockId {
    BlockId::new(format!("_orch8_composite_retry:{node}"))
}

pub(crate) async fn child_attempt_segment(
    storage: &dyn orch8_storage::StorageBackend,
    instance: &TaskInstance,
    node: ExecutionNodeId,
) -> Result<Option<String>, EngineError> {
    let marker = storage
        .get_block_output(instance.id, &state_block(node))
        .await?;
    let attempt = decode_state(marker)?.active_attempt();
    Ok((attempt > 0).then(|| children::attempt_segment(node, attempt)))
}

fn decode_state(marker: Option<BlockOutput>) -> Result<State, EngineError> {
    marker.map_or_else(
        || Ok(State::default()),
        |marker| {
            serde_json::from_value(marker.output)
                .map_err(|error| EngineError::Serialization(error.to_string()))
        },
    )
}

pub(crate) struct Boundary<'a> {
    pub(crate) policy: &'a RetryPolicy,
    pub(crate) storage: &'a dyn ExecutionControl,
    pub(crate) instance: &'a TaskInstance,
    pub(crate) node: &'a ExecutionNode,
    pub(crate) tree: &'a [ExecutionNode],
    pub(crate) now: DateTime<Utc>,
    pub(crate) claim_token: DateTime<Utc>,
    pub(crate) deferrals: Option<&'a PendingStepDeferrals>,
}

impl Boundary<'_> {
    async fn state(&self) -> Result<State, EngineError> {
        let marker = self
            .storage
            .get_block_output(self.instance.id, &state_block(self.node.id))
            .await?;
        decode_state(marker)
    }

    async fn save(&self, state: &State) -> Result<(), EngineError> {
        let output = serde_json::to_value(state)
            .map_err(|error| EngineError::Serialization(error.to_string()))?;
        self.storage
            .save_block_output(&BlockOutput {
                id: uuid::Uuid::now_v7(),
                instance_id: self.instance.id,
                block_id: state_block(self.node.id),
                output,
                output_ref: Some("__retry__".into()),
                output_size: 0,
                attempt: u16::try_from(state.attempts).unwrap_or(u16::MAX),
                created_at: Utc::now(),
            })
            .await
    }

    async fn defer(&self, fire_at: DateTime<Utc>) -> Result<(), EngineError> {
        defer_step(
            self.storage.backend(),
            self.instance,
            self.node.id,
            self.claim_token,
            StepDeferral::RetryAt(fire_at),
            self.deferrals,
        )
        .await
    }

    /// Recover the boundary before opening any branch in the next attempt.
    /// A crash before the final metadata write repeats the idempotent reset.
    pub(crate) async fn resume(&self, restart_map: bool) -> Result<bool, EngineError> {
        let mut state = self.state().await?;
        let Some(fire_at) = state.retry_at else {
            return Ok(false);
        };
        if !children::quiescent(self, state.active_attempt(), self.now >= fire_at).await? {
            let check_at = self.now + chrono::Duration::seconds(1);
            self.defer(if self.now < fire_at {
                check_at.min(fire_at)
            } else {
                check_at
            })
            .await?;
            return Ok(true);
        }
        if self.now < fire_at {
            self.defer(fire_at).await?;
            return Ok(true);
        }
        evaluator::reset_subtree_for_composite_retry(
            self.storage,
            self.tree,
            &self.instance.tenant_id,
            self.instance.id,
            self.node.id,
        )
        .await?;
        if restart_map {
            self.restart_map().await?;
        }
        state.retry_at = None;
        self.save(&state).await?;
        Ok(true)
    }

    async fn restart_map(&self) -> Result<(), EngineError> {
        // Reuse the original collection snapshot. Reset the item's cursor,
        // without reading a possibly changed ItemsPath or its current binding.
        let Some(mut marker) = self
            .storage
            .get_block_output(self.instance.id, &self.node.block_id)
            .await?
        else {
            return Err(EngineError::InvalidConfig(
                "composite retry is missing its inline Map snapshot".into(),
            ));
        };
        marker.id = uuid::Uuid::now_v7();
        marker.created_at = Utc::now();
        marker.output["_index"] = json!(0);
        marker.output["_reset_pending"] = json!(false);
        self.storage.save_block_output(&marker).await
    }

    /// Decide a retry from the first failed leaf's persisted error. Publish
    /// a pending phase before resetting any execution evidence.
    pub(crate) async fn failure(&self) -> Result<bool, EngineError> {
        let error = self.first_failure().await?;
        let message = error["error"].as_str().unwrap_or("workflow state failed");
        let mut state = self.state().await?;
        let context = crate::externalized::resolved_context_for_instance(
            self.storage.backend(),
            self.instance,
        )
        .await?;
        if let Some((_, fire_at)) = choose_retry(
            self.policy,
            state.attempts,
            message,
            Some(&error),
            context.as_ref(),
            self.now,
            &mut state.counts,
        ) {
            state.attempts += 1;
            state.retry_at = Some(fire_at);
            self.save(&state).await?;
            let at = if children::quiescent(self, state.active_attempt(), false).await? {
                fire_at
            } else {
                fire_at.min(self.now + chrono::Duration::seconds(1))
            };
            self.defer(at).await?;
            return Ok(true);
        }

        if !children::quiescent(self, state.active_attempt(), false).await? {
            self.defer(self.now + chrono::Duration::seconds(1)).await?;
            return Ok(true);
        }

        // Catch locates a failed state's diagnostic by its own logical id.
        self.storage
            .save_block_output(&BlockOutput {
                id: uuid::Uuid::now_v7(),
                instance_id: self.instance.id,
                block_id: self.node.block_id.clone(),
                output: error,
                output_ref: Some("__error__".into()),
                output_size: 0,
                attempt: u16::try_from(state.attempts).unwrap_or(u16::MAX),
                created_at: Utc::now(),
            })
            .await?;
        Ok(false)
    }

    async fn first_failure(&self) -> Result<Value, EngineError> {
        // A successful nested Catch can leave a failed leaf in its tree.
        // Consider only failures in a path of failed composites to this state.
        let mut failed_path = std::collections::HashSet::from([self.node.id]);
        let mut failed = Vec::new();
        for node in evaluator::strict_descendants(self.tree, self.node.id) {
            if node.state == NodeState::Failed
                && node
                    .parent_id
                    .is_some_and(|parent| failed_path.contains(&parent))
            {
                failed_path.insert(node.id);
                failed.push(node);
            }
        }
        failed.sort_unstable_by(|left, right| {
            left.completed_at
                .cmp(&right.completed_at)
                .then_with(|| left.block_id.cmp(&right.block_id))
        });
        for node in failed {
            if let Some(row) = self
                .storage
                .get_block_output(self.instance.id, &node.block_id)
                .await?
                && matches!(row.output_ref.as_deref(), Some("__retry__" | "__error__"))
            {
                return Ok(row.output);
            }
        }
        Ok(json!({"error":"workflow state failed", "error_code":"States.TaskFailed"}))
    }
}
