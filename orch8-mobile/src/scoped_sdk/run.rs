use super::{MobileCatalogRunSnapshot, View};
use crate::{InstanceStateKind, MobileError, storage::scopes::CatalogScope};
use orch8_types::{
    execution::{BlockType, NodeState},
    ids::InstanceId,
    instance::InstanceState,
};

#[derive(uniffi::Object)]
pub struct MobileCatalogRun {
    pub(super) view: View,
    pub(super) scope: CatalogScope,
    pub(super) id: InstanceId,
}

#[uniffi::export]
impl MobileCatalogRun {
    pub fn instance_id(&self) -> String {
        self.id.to_string()
    }
    pub fn account_id(&self) -> String {
        self.scope.account_id().into()
    }
    pub fn catalog_id(&self) -> String {
        self.scope.catalog_id().into()
    }
    pub fn session_epoch(&self) -> i64 {
        self.scope.epoch()
    }
    pub fn close_view(&self) {
        self.view.closed.cancel();
    }

    /// Materialize this prepared request under its original execution epoch.
    /// Idempotent while its native row survives. A GC receipt never rematerializes.
    pub fn start(&self) -> Result<(), MobileError> {
        let engine = self.view.engine()?;
        self.view.run(&engine, async {
            engine
                .materialize_catalog_run(self.scope.clone(), self.id)
                .await?;
            engine.tick_controller.wake();
            Ok(())
        })
    }

    /// An owned native snapshot, or None for an unmaterialized, foreign or purged
    /// run. Use catalog history to distinguish durable request metadata.
    pub fn snapshot(&self) -> Result<Option<MobileCatalogRunSnapshot>, MobileError> {
        let engine = self.view.engine()?;
        let access = engine.catalog_handlers.run_access(&self.scope, self.id);
        self.view.run(&engine, async {
            // The tree has its own engine writes. Validate the owned instance
            // again before publishing a projection, and bound churn retries.
            for _ in 0..3 {
                let Some(instance) = access.snapshot().await? else {
                    return Ok(None);
                };
                let nodes = engine.storage.get_execution_tree(self.id).await?;
                let Some(verified) = access.snapshot().await? else {
                    return Ok(None);
                };
                if verified.updated_at != instance.updated_at || verified.state != instance.state {
                    continue;
                }
                let mut pending_steps: Vec<_> = if instance.state != InstanceState::Waiting {
                    Vec::new()
                } else if nodes.is_empty() {
                    instance
                        .context
                        .runtime
                        .current_step
                        .iter()
                        .map(ToString::to_string)
                        .collect()
                } else {
                    nodes
                        .into_iter()
                        .filter(|node| {
                            node.state == NodeState::Waiting && node.block_type == BlockType::Step
                        })
                        .map(|node| node.block_id.to_string())
                        .collect()
                };
                pending_steps.sort();
                pending_steps.dedup();
                return Ok(Some(MobileCatalogRunSnapshot {
                    instance_id: self.id.to_string(),
                    account_id: self.account_id(),
                    catalog_id: self.catalog_id(),
                    session_epoch: self.scope.epoch(),
                    state: instance.state.into(),
                    data_json: serde_json::to_string(&instance.context.data)?,
                    pending_steps,
                    created_at: instance.created_at.to_rfc3339(),
                    updated_at: instance.updated_at.to_rfc3339(),
                }));
            }
            Err(MobileError::ResourceLimit {
                message: "run changed during snapshot; retry".into(),
            })
        })
    }

    /// Return the resulting native state, or None if the run is not owned or a
    /// concurrent materialization won. A terminal outcome is never overwritten.
    pub fn cancel(&self) -> Result<Option<InstanceStateKind>, MobileError> {
        let engine = self.view.engine()?;
        let access = engine.catalog_handlers.run_access(&self.scope, self.id);
        self.view.run(&engine, async {
            let state = match access.cancel().await? {
                Some(state) => Some(state.into()),
                None => engine
                    .lifecycle
                    .mobile_storage()
                    .cancel_catalog_run(&self.scope, self.id)
                    .await?
                    .then_some(InstanceStateKind::Cancelled),
            };
            if state.is_some() {
                engine.tick_controller.wake();
            }
            Ok(state)
        })
    }

    /// Commit an answer only if the snapshot version and pending step still
    /// match inside the owner-bound transaction. False means no answer committed.
    pub fn complete_step(
        &self,
        expected_updated_at: String,
        step_name: String,
        output_json: String,
    ) -> Result<bool, MobileError> {
        let engine = self.view.engine()?;
        let version = chrono::DateTime::parse_from_rfc3339(&expected_updated_at)
            .map_err(|_| MobileError::InvalidInput {
                message: "invalid run snapshot version".into(),
            })?
            .with_timezone(&chrono::Utc);
        let access = engine.catalog_handlers.run_access(&self.scope, self.id);
        self.view.run(&engine, async {
            let changed = access
                .complete_step(version, &step_name, &output_json)
                .await?;
            if changed {
                engine.tick_controller.wake();
            }
            Ok(changed)
        })
    }

    /// Release a cancelled, never-materialized request's quota. Materialized
    /// executions and unknown effects retain their durable evidence.
    pub fn forget_cancelled_request(&self) -> Result<bool, MobileError> {
        let engine = self.view.engine()?;
        self.view.run(&engine, async {
            Ok(engine
                .lifecycle
                .mobile_storage()
                .forget_cancelled_catalog_run(&self.scope, self.id)
                .await?)
        })
    }
}
