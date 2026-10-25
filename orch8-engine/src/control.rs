//! Persistence capability for an evaluation's original instance claim.
//!
//! Generic composite helpers keep this capability when calling one another.
//! A plain backend preserves explicit direct/unclaimed helper calls; scheduler
//! dispatch supplies `ClaimedControl`, whose token never follows refreshed data.
use crate::error::EngineError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orch8_storage::{
    ContinuityStore, ExecutionTreeStore, InstanceStore, OutputStore, ResourceStore, SignalStore,
    StorageBackend, WorkerStore,
};
use orch8_types::{
    error::StorageError,
    execution::{ExecutionNode, NodeState},
    ids::{BlockId, ExecutionNodeId, InstanceId, TenantId},
    output::BlockOutput,
};

#[derive(Debug, Clone, Copy)]
pub struct ExecutionClaim {
    pub instance_id: InstanceId,
    pub token: DateTime<Utc>,
}

impl ExecutionClaim {
    /// Read-only admission check using this evaluation's original nonce.
    pub(crate) async fn verify(
        self,
        backend: &dyn StorageBackend,
        tenant: &TenantId,
    ) -> Result<(), EngineError> {
        held(
            self,
            backend
                .verify_instance_claim(self.instance_id, tenant, self.token)
                .await?,
        )
    }
}

/// A backend viewed through the immutable claim that started this evaluation.
pub struct ClaimedControl<'a> {
    backend: &'a dyn StorageBackend,
    claim: ExecutionClaim,
}

impl<'a> ClaimedControl<'a> {
    #[must_use]
    pub const fn new(
        backend: &'a dyn StorageBackend,
        instance_id: InstanceId,
        token: DateTime<Utc>,
    ) -> Self {
        Self {
            backend,
            claim: ExecutionClaim { instance_id, token },
        }
    }
}

fn held(claim: ExecutionClaim, held: bool) -> Result<(), EngineError> {
    if held {
        Ok(())
    } else {
        Err(EngineError::ClaimLost {
            instance_id: claim.instance_id,
        })
    }
}

fn same_instance(claim: ExecutionClaim, id: InstanceId) -> Result<(), EngineError> {
    if claim.instance_id == id {
        Ok(())
    } else {
        Err(EngineError::InvalidConfig(
            "control write targets another instance".into(),
        ))
    }
}

/// Composite persistence operations. Claimed callers must retain this type
/// through nested helpers rather than coercing it to a plain backend.
#[async_trait]
pub trait ExecutionControl: Send + Sync {
    /// Underlying backend for read-only utilities; do not bypass control writes.
    fn backend(&self) -> &dyn StorageBackend;
    fn execution_claim(&self) -> Option<ExecutionClaim> {
        None
    }

    async fn delete_event_waits_for_blocks(
        &self,
        instance_id: InstanceId,
        blocks: &[BlockId],
    ) -> Result<u64, EngineError> {
        let blocks: Vec<String> = blocks.iter().map(ToString::to_string).collect();
        if let Some(claim) = self.execution_claim() {
            same_instance(claim, instance_id)?;
            SignalStore::delete_event_waits_for_blocks_claimed(
                self.backend(),
                instance_id,
                &blocks,
                claim.token,
            )
            .await?
            .ok_or(EngineError::ClaimLost { instance_id })
        } else {
            Ok(
                SignalStore::delete_event_waits_for_blocks(self.backend(), instance_id, &blocks)
                    .await?,
            )
        }
    }

    /// Keep the evaluation's claim through durable gate registration.
    async fn upsert_event_wait(
        &self,
        wait: &orch8_types::event_correlation::EventWait,
    ) -> Result<orch8_types::event_correlation::EventWait, EngineError> {
        if let Some(claim) = self.execution_claim() {
            same_instance(claim, InstanceId::from_uuid(wait.instance_id))?;
            SignalStore::upsert_event_wait_claimed(self.backend(), wait, claim.token)
                .await?
                .ok_or(EngineError::ClaimLost {
                    instance_id: claim.instance_id,
                })
        } else {
            Ok(SignalStore::register_event_wait(self.backend(), wait).await?)
        }
    }

    async fn create_federation_call(
        &self,
        call: &orch8_types::federation::FederationCall,
    ) -> Result<bool, EngineError> {
        if let Some(claim) = self.execution_claim() {
            same_instance(claim, call.instance_id)?;
            ContinuityStore::create_federation_call_claimed(self.backend(), call, claim.token)
                .await?
                .ok_or(EngineError::ClaimLost {
                    instance_id: claim.instance_id,
                })
        } else {
            Ok(ContinuityStore::create_federation_call(self.backend(), call).await?)
        }
    }

    /// Create a child under this evaluation's parent claim, never the child's.
    async fn create_child_instance(
        &self,
        child: &orch8_types::instance::TaskInstance,
    ) -> Result<(), EngineError> {
        if let Some(claim) = self.execution_claim() {
            held(
                claim,
                InstanceStore::create_child_instance_claimed(
                    self.backend(),
                    claim.instance_id,
                    child,
                    claim.token,
                )
                .await?,
            )
        } else {
            InstanceStore::create_instance(self.backend(), child)
                .await
                .map_err(EngineError::Storage)
        }
    }

    /// Rebind or retry a previous child under this evaluation's parent claim.
    async fn adopt_child_instance(
        &self,
        previous: &orch8_types::instance::TaskInstance,
        activation: &str,
    ) -> Result<Option<orch8_types::instance::TaskInstance>, EngineError> {
        if let Some(claim) = self.execution_claim() {
            let expected = previous
                .metadata
                .get("_parent_activation")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    EngineError::InvalidConfig(
                        "child adoption requires its previous activation".into(),
                    )
                })?;
            match InstanceStore::adopt_child_instance_claimed(
                self.backend(),
                claim.instance_id,
                previous.id,
                expected,
                activation,
                claim.token,
            )
            .await?
            {
                None => Err(EngineError::ClaimLost {
                    instance_id: claim.instance_id,
                }),
                Some(orch8_storage::ChildAdoption::Ineligible) => Ok(None),
                Some(orch8_storage::ChildAdoption::Adopted(child)) => Ok(Some(*child)),
            }
        } else {
            InstanceStore::merge_instance_metadata(
                self.backend(),
                previous.id,
                &serde_json::json!({"_parent_activation": activation}),
            )
            .await?;
            if previous.state == orch8_types::instance::InstanceState::Failed {
                orch8_storage::lifecycle::retry_failed_instance(self.backend(), previous.id)
                    .await?;
                return Ok(InstanceStore::get_instance(self.backend(), previous.id).await?);
            }
            Ok(Some(previous.clone()))
        }
    }

    async fn mark_signal_delivered(
        &self,
        instance_id: InstanceId,
        signal_id: uuid::Uuid,
    ) -> Result<(), EngineError> {
        if let Some(claim) = self.execution_claim() {
            same_instance(claim, instance_id)?;
            held(
                claim,
                SignalStore::mark_signal_delivered_claimed(
                    self.backend(),
                    instance_id,
                    signal_id,
                    claim.token,
                )
                .await?,
            )
        } else {
            SignalStore::mark_signal_delivered(self.backend(), signal_id)
                .await
                .map_err(EngineError::Storage)
        }
    }

    async fn get_block_output(
        &self,
        id: InstanceId,
        block: &BlockId,
    ) -> Result<Option<BlockOutput>, StorageError> {
        OutputStore::get_block_output(self.backend(), id, block).await
    }

    async fn get_execution_tree(&self, id: InstanceId) -> Result<Vec<ExecutionNode>, StorageError> {
        ExecutionTreeStore::get_execution_tree(self.backend(), id).await
    }
    async fn get_instance(
        &self,
        id: InstanceId,
    ) -> Result<Option<orch8_types::instance::TaskInstance>, StorageError> {
        InstanceStore::get_instance(self.backend(), id).await
    }
    async fn get_completed_block_ids(&self, id: InstanceId) -> Result<Vec<BlockId>, StorageError> {
        OutputStore::get_completed_block_ids(self.backend(), id).await
    }
    async fn get_externalized_state(
        &self,
        id: InstanceId,
        key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        ResourceStore::get_externalized_state(self.backend(), id, key).await
    }
    async fn merge_context_data(
        &self,
        id: InstanceId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), EngineError> {
        if let Some(claim) = self.execution_claim() {
            same_instance(claim, id)?;
            held(
                claim,
                InstanceStore::merge_context_data_claimed(
                    self.backend(),
                    id,
                    key,
                    value,
                    claim.token,
                )
                .await?,
            )
        } else {
            InstanceStore::merge_context_data(self.backend(), id, key, value)
                .await
                .map_err(EngineError::Storage)
        }
    }
    async fn update_instance_context_cas(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        expected: DateTime<Utc>,
    ) -> Result<bool, EngineError> {
        if let Some(claim) = self.execution_claim() {
            same_instance(claim, id)?;
            InstanceStore::update_instance_context_cas_claimed(
                self.backend(),
                id,
                context,
                expected,
                claim.token,
            )
            .await?
            .ok_or(EngineError::ClaimLost {
                instance_id: claim.instance_id,
            })
        } else {
            InstanceStore::update_instance_context_cas(self.backend(), id, context, expected)
                .await
                .map_err(EngineError::Storage)
        }
    }
    async fn save_externalized_state(
        &self,
        id: InstanceId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), EngineError> {
        if let Some(claim) = self.execution_claim() {
            same_instance(claim, id)?;
            held(
                claim,
                ResourceStore::save_externalized_state_claimed(
                    self.backend(),
                    id,
                    key,
                    value,
                    claim.token,
                )
                .await?,
            )
        } else {
            ResourceStore::save_externalized_state(self.backend(), id, key, value)
                .await
                .map_err(EngineError::Storage)
        }
    }
    async fn cancel_worker_tasks_for_block(
        &self,
        id: uuid::Uuid,
        block: &str,
    ) -> Result<u64, EngineError> {
        if self.execution_claim().is_some() {
            self.cancel_worker_tasks_for_blocks(id, &[block.to_string()])
                .await
        } else {
            WorkerStore::cancel_worker_tasks_for_block(self.backend(), id, block)
                .await
                .map_err(EngineError::Storage)
        }
    }

    async fn get_all_outputs(&self, id: InstanceId) -> Result<Vec<BlockOutput>, StorageError> {
        OutputStore::get_all_outputs(self.backend(), id).await
    }
    async fn merge_instance_metadata(
        &self,
        id: InstanceId,
        patch: &serde_json::Value,
    ) -> Result<(), EngineError> {
        if let Some(claim) = self.execution_claim() {
            same_instance(claim, id)?;
            held(
                claim,
                InstanceStore::merge_instance_metadata_claimed(
                    self.backend(),
                    id,
                    patch,
                    claim.token,
                )
                .await?,
            )
        } else {
            InstanceStore::merge_instance_metadata(self.backend(), id, patch)
                .await
                .map_err(EngineError::Storage)
        }
    }
    async fn delete_block_outputs_batch(
        &self,
        id: InstanceId,
        blocks: &[BlockId],
    ) -> Result<u64, EngineError> {
        if let Some(claim) = self.execution_claim() {
            same_instance(claim, id)?;
            OutputStore::delete_block_outputs_batch_claimed(self.backend(), id, blocks, claim.token)
                .await?
                .ok_or(EngineError::ClaimLost { instance_id: id })
        } else {
            OutputStore::delete_block_outputs_batch(self.backend(), id, blocks)
                .await
                .map_err(EngineError::Storage)
        }
    }
    async fn delete_block_output_by_id(&self, id: uuid::Uuid) -> Result<(), EngineError> {
        if let Some(claim) = self.execution_claim() {
            held(
                claim,
                OutputStore::delete_block_output_by_id_claimed(
                    self.backend(),
                    claim.instance_id,
                    id,
                    claim.token,
                )
                .await?,
            )
        } else {
            OutputStore::delete_block_output_by_id(self.backend(), id)
                .await
                .map_err(EngineError::Storage)
        }
    }
    async fn delete_effect_receipts_for_blocks(
        &self,
        tenant: &orch8_types::ids::TenantId,
        id: InstanceId,
        blocks: &[BlockId],
    ) -> Result<u64, EngineError> {
        if let Some(claim) = self.execution_claim() {
            same_instance(claim, id)?;
            ContinuityStore::delete_effect_receipts_for_blocks_claimed(
                self.backend(),
                tenant,
                id,
                blocks,
                claim.token,
            )
            .await?
            .ok_or(EngineError::ClaimLost { instance_id: id })
        } else {
            ContinuityStore::delete_effect_receipts_for_blocks(self.backend(), tenant, id, blocks)
                .await
                .map_err(EngineError::Storage)
        }
    }
    async fn conditional_update_claimed_instance_state(
        &self,
        id: InstanceId,
        token: DateTime<Utc>,
        from: orch8_types::instance::InstanceState,
        to: orch8_types::instance::InstanceState,
        next: Option<DateTime<Utc>>,
    ) -> Result<bool, EngineError> {
        if let Some(claim) = self.execution_claim() {
            same_instance(claim, id)?;
            // The capability retains authority; refreshed snapshot timestamps
            // and legacy helper arguments cannot replace its original token.
            let changed = InstanceStore::conditional_update_claimed_instance_state(
                self.backend(),
                id,
                claim.token,
                from,
                to,
                next,
            )
            .await?;
            held(claim, changed)?;
            Ok(changed)
        } else {
            InstanceStore::conditional_update_claimed_instance_state(
                self.backend(),
                id,
                token,
                from,
                to,
                next,
            )
            .await
            .map_err(EngineError::Storage)
        }
    }

    async fn save_block_output(&self, output: &BlockOutput) -> Result<(), EngineError> {
        if let Some(claim) = self.execution_claim() {
            same_instance(claim, output.instance_id)?;
            held(
                claim,
                OutputStore::save_block_output_claimed(self.backend(), output, claim.token).await?,
            )
        } else {
            OutputStore::save_block_output(self.backend(), output)
                .await
                .map_err(EngineError::Storage)
        }
    }

    async fn create_execution_nodes_batch(
        &self,
        nodes: &[ExecutionNode],
    ) -> Result<(), EngineError> {
        if let Some(claim) = self.execution_claim() {
            held(
                claim,
                ExecutionTreeStore::create_execution_nodes_batch_claimed(
                    self.backend(),
                    claim.instance_id,
                    nodes,
                    claim.token,
                )
                .await?,
            )
        } else {
            ExecutionTreeStore::create_execution_nodes_batch(self.backend(), nodes)
                .await
                .map_err(EngineError::Storage)
        }
    }

    async fn update_node_state(
        &self,
        id: ExecutionNodeId,
        state: NodeState,
    ) -> Result<(), EngineError> {
        if let Some(claim) = self.execution_claim() {
            held(
                claim,
                ExecutionTreeStore::update_node_state_claimed(
                    self.backend(),
                    claim.instance_id,
                    id,
                    state,
                    claim.token,
                )
                .await?,
            )
        } else {
            ExecutionTreeStore::update_node_state(self.backend(), id, state)
                .await
                .map_err(EngineError::Storage)
        }
    }

    async fn update_nodes_state(
        &self,
        ids: &[ExecutionNodeId],
        state: NodeState,
    ) -> Result<(), EngineError> {
        if let Some(claim) = self.execution_claim() {
            held(
                claim,
                ExecutionTreeStore::update_nodes_state_claimed(
                    self.backend(),
                    claim.instance_id,
                    ids,
                    state,
                    claim.token,
                )
                .await?,
            )
        } else {
            ExecutionTreeStore::update_nodes_state(self.backend(), ids, state)
                .await
                .map_err(EngineError::Storage)
        }
    }

    async fn batch_activate_nodes(&self, ids: &[ExecutionNodeId]) -> Result<(), EngineError> {
        if let Some(claim) = self.execution_claim() {
            held(
                claim,
                ExecutionTreeStore::batch_activate_nodes_claimed(
                    self.backend(),
                    claim.instance_id,
                    ids,
                    claim.token,
                )
                .await?,
            )
        } else {
            ExecutionTreeStore::batch_activate_nodes(self.backend(), ids)
                .await
                .map_err(EngineError::Storage)
        }
    }

    async fn cancel_worker_tasks_for_blocks(
        &self,
        id: uuid::Uuid,
        blocks: &[String],
    ) -> Result<u64, EngineError> {
        if let Some(claim) = self.execution_claim() {
            same_instance(claim, InstanceId::from_uuid(id))?;
            WorkerStore::cancel_worker_tasks_for_blocks_claimed(
                self.backend(),
                claim.instance_id,
                blocks,
                claim.token,
            )
            .await?
            .ok_or(EngineError::ClaimLost {
                instance_id: claim.instance_id,
            })
        } else {
            WorkerStore::cancel_worker_tasks_for_blocks(self.backend(), id, blocks)
                .await
                .map_err(EngineError::Storage)
        }
    }
}

#[async_trait]
impl<T: StorageBackend> ExecutionControl for T {
    fn backend(&self) -> &dyn StorageBackend {
        self
    }
}

#[async_trait]
impl ExecutionControl for dyn StorageBackend {
    fn backend(&self) -> &dyn StorageBackend {
        self
    }
}

#[async_trait]
impl ExecutionControl for ClaimedControl<'_> {
    fn backend(&self) -> &dyn StorageBackend {
        self.backend
    }
    fn execution_claim(&self) -> Option<ExecutionClaim> {
        Some(self.claim)
    }
}
