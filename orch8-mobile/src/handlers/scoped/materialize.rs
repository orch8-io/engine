//! Owned materialization observer: cancellation never frees physical CPU
//! admission ahead of an already dispatched validator or database transaction.
use super::{CatalogHandlers, NativePermit};
use crate::{
    MobileError,
    storage::scopes::{
        CatalogScope,
        runs::materialize::{InstanceLimits, Preparation},
    },
};
use orch8_types::{ids::InstanceId, instance::TaskInstance};
use std::{collections::HashSet, sync::Arc};
use tokio_util::sync::CancellationToken;
pub(crate) struct RuntimeEnvironment {
    pub(crate) sdk: String,
    pub(crate) builtins: HashSet<String>,
}
struct Wait {
    task: tokio::task::JoinHandle<Result<TaskInstance, MobileError>>,
    stop: CancellationToken,
}
impl Drop for Wait {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
impl CatalogHandlers {
    pub(crate) async fn materialize(
        self: &Arc<Self>,
        scope: CatalogScope,
        id: InstanceId,
        limits: InstanceLimits,
        environment: RuntimeEnvironment,
    ) -> Result<TaskInstance, MobileError> {
        if self.stop.is_cancelled() {
            return Err(MobileError::Shutdown);
        }
        let permit = self
            .capacity
            .reserve(&scope)
            .map_err(|_| MobileError::ResourceLimit {
                message: "catalog materialization capacity is occupied".into(),
            })?;
        let owner = self.clone();
        let stop = self.stop.child_token();
        let task_stop = stop.clone();
        let task = tokio::spawn(async move {
            owner
                .materialize_owned(scope, id, limits, environment, task_stop, permit)
                .await
        });
        let mut wait = Wait { task, stop };
        (&mut wait.task).await.map_err(|_| MobileError::Engine {
            message: "catalog materialization task failed".into(),
        })?
    }
    async fn materialize_owned(
        &self,
        scope: CatalogScope,
        id: InstanceId,
        limits: InstanceLimits,
        environment: RuntimeEnvironment,
        stop: CancellationToken,
        permit: NativePermit,
    ) -> Result<TaskInstance, MobileError> {
        if stop.is_cancelled() {
            return Err(MobileError::Shutdown);
        }
        let snapshot = match self.storage.prepare_materialization(&scope, id).await? {
            Preparation::Existing(instance) => return Ok(*instance),
            Preparation::Pending(snapshot) => *snapshot,
        };
        let mut handlers = environment.builtins;
        handlers.extend(self.names(&scope).await.map_err(|_| MobileError::Engine {
            message: "catalog handler bindings are unavailable".into(),
        })?);
        let validation_stop = stop.clone();
        // The physical permit follows the CPU closure even if its Tokio caller
        // is aborted at runtime teardown. No foreign Binding is captured here.
        let (permit, checked) = tokio::task::spawn_blocking(move || {
            let result = if validation_stop.is_cancelled() {
                Err(MobileError::Shutdown)
            } else {
                snapshot
                    .check(&handlers, &environment.sdk)
                    .map_err(MobileError::from)
            };
            (permit, result)
        })
        .await
        .map_err(|_| MobileError::Engine {
            message: "catalog validation task failed".into(),
        })?;
        let _permit = permit;
        self.storage
            .commit_materialization(&scope, checked?, limits, &stop)
            .await
    }
}
