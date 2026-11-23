//! Add retained issued context to compact task JSON without changing legacy frames.
use super::{Status, Uuid, WorkerTask, to_json_string};
use orch8_storage::worker_callback::WorkerCallbackBinding;

pub(super) fn encode(
    batch: orch8_storage::worker_claim::WorkerClaimBatch,
) -> Result<Vec<(Uuid, String)>, Status> {
    #[derive(serde::Serialize)]
    struct Delivery<'a> {
        #[serde(flatten)]
        task: &'a WorkerTask,
        #[serde(skip_serializing_if = "Option::is_none")]
        callback_token: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        delegation: Option<&'a orch8_types::delegation_definition::claim::DelegationClaimContext>,
    }
    let orch8_storage::worker_claim::WorkerClaimDelivery { tasks, contexts } = batch
        .into_delivery()
        .map_err(|error| Status::internal(error.to_string()))?;
    let contexts = contexts
        .iter()
        .map(|context| (context.scope.task_id, context))
        .collect::<std::collections::HashMap<_, _>>();
    tasks
        .into_iter()
        .map(|(mut task, callback)| {
            task.lease_secs = task.heartbeat_timeout_secs.or(task.lease_secs);
            let wire = Delivery {
                task: &task,
                callback_token: callback.as_ref().map(WorkerCallbackBinding::token),
                delegation: contexts.get(&task.id).copied(),
            };
            Ok((task.id, to_json_string(&wire)?))
        })
        .collect()
}
