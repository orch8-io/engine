use std::future::Future;

use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::ExecutionNodeId;

use orch8_types::error::StorageError;
use orch8_types::ids::{BlockId, InstanceId, TenantId};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::output::BlockOutput;
use serde_json::json;
use uuid::Uuid;

use super::{
    ConformanceError, ConformanceReport, backend, conformance_instance, conformance_sequence,
    require,
};
use crate::StorageBackend;
use crate::externalizing::{ExternalizedEntry, marker};
use crate::restore::InstanceSnapshot;

/// Diagnostic the fault injector must return from the selected output write.
pub const ATOMIC_RESTORE_OUTPUT_FAULT: &str = "orch8 conformance output insertion fault";

const FAULT: &str = "restore_output_fault";
const ROLLBACK: &str = "restore_rolls_back_every_row";
const RETRY: &str = "restore_concurrent_retry";
const CONTENTS: &str = "restore_complete_contents";

/// Verify complete rollback after the tree is inserted and exactly one
/// complete publication when two callers retry concurrently.
///
/// Use an isolated database. The hook receives the selected output ID to arm
/// a failure just before that row is inserted. Return a storage error from the
/// backend containing [`ATOMIC_RESTORE_OUTPUT_FAULT`] to prove the output writer
/// was reached. The hook receives `None` to clear the failure before the
/// rollback checks and retries. The successful run leaves a terminal fixture.
pub async fn run_atomic_restore_conformance<F, Fut>(
    storage: &dyn StorageBackend,
    configure_output_fault: F,
) -> Result<ConformanceReport, ConformanceError>
where
    F: Fn(Option<Uuid>) -> Fut,
    Fut: Future<Output = Result<(), StorageError>>,
{
    let run_id = Uuid::now_v7();
    let scope = format!("orch8-restore-conformance-{run_id}");
    let tenant = TenantId::new(scope.clone()).map_err(|message| ConformanceError::Violation {
        check: "scope",
        message,
    })?;
    let sequence = conformance_sequence(&tenant, run_id);
    backend("sequence_create", storage.create_sequence(&sequence).await)?;
    let snapshot = fixture(conformance_instance(&tenant, sequence.id, run_id));
    let id = snapshot.instance.id;

    backend(
        FAULT,
        configure_output_fault(Some(snapshot.outputs[1].id)).await,
    )?;
    let failed_restore = storage.restore_instance_snapshot(&snapshot).await;
    backend(FAULT, configure_output_fault(None).await)?;
    require(
        FAULT,
        failed_restore.is_err_and(|error| error.to_string().contains(ATOMIC_RESTORE_OUTPUT_FAULT)),
        "restore must reach the selected output writer and return its injected failure",
    )?;
    check_rollback(storage, id).await?;

    let (left, right) = tokio::join!(
        storage.restore_instance_snapshot(&snapshot),
        storage.restore_instance_snapshot(&snapshot),
    );
    require(
        RETRY,
        backend(RETRY, left)? ^ backend(RETRY, right)?,
        "exactly one concurrent restore must publish the snapshot",
    )?;
    check_contents(storage, &snapshot).await?;
    Ok(ConformanceReport {
        scope,
        sequence_id: sequence.id,
        instance_id: id,
        checks: vec![FAULT, ROLLBACK, RETRY, CONTENTS],
    })
}

async fn check_rollback(
    storage: &dyn StorageBackend,
    id: InstanceId,
) -> Result<(), ConformanceError> {
    require(
        ROLLBACK,
        backend(ROLLBACK, storage.get_instance(id).await)?.is_none()
            && backend(ROLLBACK, storage.get_execution_tree(id).await)?.is_empty()
            && backend(ROLLBACK, storage.get_all_outputs(id).await)?.is_empty()
            && backend(
                ROLLBACK,
                storage.list_externalized_entries(id, None, 10).await,
            )?
            .is_empty(),
        "the failed restore left an instance or dependent rows behind",
    )
}

async fn check_contents(
    storage: &dyn StorageBackend,
    snapshot: &InstanceSnapshot,
) -> Result<(), ConformanceError> {
    let id = snapshot.instance.id;
    let instance = backend(CONTENTS, storage.get_instance(id).await)?;
    let tree = backend(CONTENTS, storage.get_execution_tree(id).await)?;
    let outputs = backend(CONTENTS, storage.get_all_outputs(id).await)?;
    let payloads = backend(
        CONTENTS,
        storage.list_externalized_entries(id, None, 10).await,
    )?;
    require(
        CONTENTS,
        instance.is_some_and(|stored| {
            stored.sequence_id == snapshot.instance.sequence_id
                && stored.state == InstanceState::Completed
                && stored.context.data == snapshot.instance.context.data
        }) && tree.len() == 1
            && tree[0].id == snapshot.nodes[0].id
            && tree[0].state == NodeState::Completed
            && outputs.len() == snapshot.outputs.len()
            && snapshot.outputs.iter().all(|expected| {
                outputs.iter().any(|actual| {
                    actual.id == expected.id
                        && actual.attempt == expected.attempt
                        && actual.output == expected.output
                })
            })
            && payloads.len() == 1
            && payloads[0].ref_key == snapshot.payloads[0].ref_key
            && payloads[0].payload == snapshot.payloads[0].payload,
        "successful retry did not publish exactly the expected instance, tree, outputs, and payload",
    )
}

fn fixture(mut instance: TaskInstance) -> InstanceSnapshot {
    let id = instance.id;
    let now = instance.created_at;
    let block_id = BlockId::new("restore-conformance");
    let reference = format!("{id}:ctx:data:value");
    instance.state = InstanceState::Completed;
    instance.context.data = json!({"value": marker(&reference)});
    InstanceSnapshot {
        instance,
        nodes: vec![ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id: id,
            block_id: block_id.clone(),
            parent_id: None,
            block_type: BlockType::Step,
            branch_index: None,
            state: NodeState::Completed,
            started_at: Some(now),
            completed_at: Some(now),
        }],
        outputs: (0..2)
            .map(|attempt| BlockOutput {
                id: Uuid::now_v7(),
                instance_id: id,
                block_id: block_id.clone(),
                output: json!(attempt),
                output_ref: None,
                output_size: 1,
                attempt,
                created_at: now,
            })
            .collect(),
        payloads: vec![ExternalizedEntry {
            ref_key: reference,
            instance_id: id,
            payload: json!({"from": "conformance"}),
            created_at: now,
            expires_at: None,
        }],
    }
}
