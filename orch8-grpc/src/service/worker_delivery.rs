//! Add retained issued context to compact task JSON without changing legacy frames.
use super::{Status, Uuid, WorkerTask, to_json_string};

pub(super) fn encode(
    batch: orch8_storage::worker_claim::WorkerClaimBatch,
) -> Result<Vec<(Uuid, String)>, Status> {
    #[derive(serde::Serialize)]
    struct Delivery<'a> {
        #[serde(flatten)]
        task: &'a WorkerTask,
        #[serde(skip_serializing_if = "Option::is_none")]
        delegation: Option<&'a orch8_types::delegation_definition::claim::DelegationClaimContext>,
    }
    let (tasks, contexts) = batch.into_parts();
    let contexts = contexts
        .iter()
        .map(|context| (context.scope.task_id, context))
        .collect::<std::collections::HashMap<_, _>>();
    tasks
        .iter()
        .map(|task| {
            let wire = Delivery {
                task,
                delegation: contexts.get(&task.id).copied(),
            };
            Ok((task.id, to_json_string(&wire)?))
        })
        .collect()
}
