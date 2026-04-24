use std::str::FromStr;
use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing;
use uuid::Uuid;

use orch8_storage::StorageBackend;
use orch8_types::ids::{InstanceId, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::sequence::{BlockDefinition, StepDef};
use orch8_types::worker::{WorkerTask, WorkerTaskState};

use crate::auth::{caller_tenant, enforce_tenant_create, enforce_tenant_match, scoped_tenant_id};
use crate::proto::{self, orch8_service_server::Orch8Service};

pub struct Orch8GrpcService {
    storage: Arc<dyn StorageBackend>,
}

impl Orch8GrpcService {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
    }

    /// Atomically save worker output, mark the execution node Completed, and
    /// transition the instance state. The instance UPDATE is CAS-guarded: if
    /// the instance became terminal/paused after our read, the tx rolls back
    /// and we return `Ok(())` (the late completion is harmlessly dropped).
    async fn atomically_complete_node(
        &self,
        task: &orch8_types::worker::WorkerTask,
        block_output: &orch8_types::output::BlockOutput,
        node_id: orch8_types::ids::ExecutionNodeId,
        merged_context: bool,
        context: &orch8_types::context::ExecutionContext,
    ) -> Result<(), Status> {
        let result = if merged_context {
            self.storage
                .save_output_complete_node_merge_context_and_transition(
                    block_output,
                    node_id,
                    task.instance_id,
                    context,
                    InstanceState::Scheduled,
                    Some(chrono::Utc::now()),
                )
                .await
        } else {
            self.storage
                .save_output_complete_node_and_transition(
                    block_output,
                    node_id,
                    task.instance_id,
                    InstanceState::Scheduled,
                    Some(chrono::Utc::now()),
                )
                .await
        };
        match result {
            Ok(()) => Ok(()),
            Err(orch8_types::error::StorageError::TerminalTarget { .. }) => {
                tracing::info!(
                    instance_id = %task.instance_id,
                    block_id = %task.block_id.0,
                    "gRPC worker completion CAS failed — instance became terminal/paused after read"
                );
                Ok(())
            }
            Err(e) => Err(storage_err(e)),
        }
    }
}

fn parse_uuid(s: &str) -> Result<Uuid, Status> {
    s.parse::<Uuid>()
        .map_err(|_| Status::invalid_argument(format!("invalid UUID: {s}")))
}

fn to_json_string<T: serde::Serialize>(val: &T) -> Result<String, Status> {
    serde_json::to_string(val).map_err(|e| {
        tracing::error!(error = %e, "json serialization failed");
        Status::internal("internal error")
    })
}

fn from_json_str<T: serde::de::DeserializeOwned>(s: &str) -> Result<T, Status> {
    const MAX_JSON_SIZE: usize = 10 * 1024 * 1024; // 10 MB
    if s.len() > MAX_JSON_SIZE {
        return Err(Status::invalid_argument("JSON payload too large"));
    }
    serde_json::from_str(s)
        .map_err(|e| Status::invalid_argument(format!("invalid JSON payload: {e}")))
}

fn storage_err(e: orch8_types::error::StorageError) -> Status {
    use orch8_types::error::StorageError;
    match e {
        StorageError::NotFound { entity, id } => Status::not_found(format!("{entity} {id}")),
        StorageError::Conflict(msg) => Status::already_exists(msg),
        // Terminal-state target: the request is well-formed but the target's
        // current state forbids it. `FailedPrecondition` is the canonical gRPC
        // mapping per the Google API guide ("resource is in a state that
        // prevents the operation").
        StorageError::TerminalTarget { entity, id } => {
            Status::failed_precondition(format!("{entity} {id} is in a terminal state"))
        }
        StorageError::Connection(_) => Status::unavailable("storage unavailable"),
        StorageError::PoolExhausted => Status::unavailable("pool exhausted"),
        other => {
            tracing::error!(error = %other, "internal storage error");
            Status::internal("internal error")
        }
    }
}

async fn get_worker_task_checked(
    storage: &Arc<dyn StorageBackend>,
    caller_tenant: Option<TenantId>,
    task_id: Uuid,
) -> Result<(WorkerTask, TaskInstance), Status> {
    let task = storage
        .get_worker_task(task_id)
        .await
        .map_err(storage_err)?
        .ok_or_else(|| Status::not_found(format!("worker_task {task_id}")))?;
    let instance = storage
        .get_instance(task.instance_id)
        .await
        .map_err(storage_err)?
        .ok_or_else(|| Status::not_found("instance not found"))?;
    if let Some(ref caller) = caller_tenant {
        if caller != &instance.tenant_id {
            return Err(Status::not_found("worker_task"));
        }
    }
    Ok((task, instance))
}

async fn worker_task_can_retry(
    storage: &Arc<dyn StorageBackend>,
    task: &WorkerTask,
) -> Result<bool, Status> {
    let Some(instance) = storage
        .get_instance(task.instance_id)
        .await
        .map_err(storage_err)?
    else {
        return Ok(false);
    };
    let Some(seq) = storage
        .get_sequence(instance.sequence_id)
        .await
        .map_err(storage_err)?
    else {
        return Ok(false);
    };
    let Some(step_def) = find_step_block(&seq.blocks, &task.block_id) else {
        return Ok(false);
    };
    let Some(retry) = &step_def.retry else {
        return Ok(false);
    };
    let next_attempt_i32 = i32::from(task.attempt).saturating_add(1);
    let next_attempt = u32::try_from(next_attempt_i32).unwrap_or(u32::MAX);
    Ok(next_attempt < retry.max_attempts)
}

fn find_step_block<'a>(
    blocks: &'a [BlockDefinition],
    block_id: &orch8_types::ids::BlockId,
) -> Option<&'a StepDef> {
    for block in blocks {
        match block {
            BlockDefinition::Step(step) if step.id == *block_id => return Some(step.as_ref()),
            BlockDefinition::Parallel(def) => {
                for branch in &def.branches {
                    if let Some(step) = find_step_block(branch, block_id) {
                        return Some(step);
                    }
                }
            }
            BlockDefinition::Race(def) => {
                for branch in &def.branches {
                    if let Some(step) = find_step_block(branch, block_id) {
                        return Some(step);
                    }
                }
            }
            BlockDefinition::Loop(def) => {
                if let Some(step) = find_step_block(&def.body, block_id) {
                    return Some(step);
                }
            }
            BlockDefinition::ForEach(def) => {
                if let Some(step) = find_step_block(&def.body, block_id) {
                    return Some(step);
                }
            }
            BlockDefinition::Router(def) => {
                for route in &def.routes {
                    if let Some(step) = find_step_block(&route.blocks, block_id) {
                        return Some(step);
                    }
                }
                if let Some(default) = &def.default {
                    if let Some(step) = find_step_block(default, block_id) {
                        return Some(step);
                    }
                }
            }
            BlockDefinition::TryCatch(def) => {
                if let Some(step) = find_step_block(&def.try_block, block_id) {
                    return Some(step);
                }
                if let Some(step) = find_step_block(&def.catch_block, block_id) {
                    return Some(step);
                }
                if let Some(finally_block) = &def.finally_block {
                    if let Some(step) = find_step_block(finally_block, block_id) {
                        return Some(step);
                    }
                }
            }
            BlockDefinition::ABSplit(def) => {
                for variant in &def.variants {
                    if let Some(step) = find_step_block(&variant.blocks, block_id) {
                        return Some(step);
                    }
                }
            }
            BlockDefinition::CancellationScope(def) => {
                if let Some(step) = find_step_block(&def.blocks, block_id) {
                    return Some(step);
                }
            }
            BlockDefinition::Step(_) | BlockDefinition::SubSequence(_) => {}
        }
    }
    None
}

fn retry_worker_task(task: &WorkerTask) -> WorkerTask {
    WorkerTask {
        id: Uuid::now_v7(),
        instance_id: task.instance_id,
        block_id: task.block_id.clone(),
        handler_name: task.handler_name.clone(),
        queue_name: task.queue_name.clone(),
        params: task.params.clone(),
        context: task.context.clone(),
        attempt: task.attempt.saturating_add(1),
        timeout_ms: task.timeout_ms,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: chrono::Utc::now(),
    }
}

#[tonic::async_trait]
impl Orch8Service for Orch8GrpcService {
    // --- Health ---

    async fn health(
        &self,
        _req: Request<proto::HealthRequest>,
    ) -> Result<Response<proto::HealthResponse>, Status> {
        self.storage.ping().await.map_err(storage_err)?;
        Ok(Response::new(proto::HealthResponse {
            status: "ok".into(),
        }))
    }

    // --- Sequences ---

    async fn create_sequence(
        &self,
        req: Request<proto::CreateSequenceRequest>,
    ) -> Result<Response<proto::SequenceResponse>, Status> {
        let mut seq: orch8_types::sequence::SequenceDefinition =
            from_json_str(&req.get_ref().definition_json)?;
        // Force the sequence's tenant_id to the caller's tenant (when
        // tenant-scoped). Prevents the client from creating a sequence under
        // another tenant by stuffing a foreign tenant_id into the payload.
        seq.tenant_id = enforce_tenant_create(&req, &seq.tenant_id)?;
        self.storage
            .create_sequence(&seq)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::SequenceResponse {
            definition_json: to_json_string(&seq)?,
        }))
    }

    async fn get_sequence(
        &self,
        req: Request<proto::GetSequenceRequest>,
    ) -> Result<Response<proto::SequenceResponse>, Status> {
        let id = SequenceId(parse_uuid(&req.get_ref().id)?);
        let seq = self
            .storage
            .get_sequence(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("sequence not found"))?;
        enforce_tenant_match(&req, &seq.tenant_id, "sequence")?;
        Ok(Response::new(proto::SequenceResponse {
            definition_json: to_json_string(&seq)?,
        }))
    }

    async fn get_sequence_by_name(
        &self,
        req: Request<proto::GetSequenceByNameRequest>,
    ) -> Result<Response<proto::SequenceResponse>, Status> {
        // Resolve the tenant from the caller's metadata when present —
        // otherwise fall back to the body. Prevents cross-tenant sequence
        // lookup by name when tenant-scoped.
        let body_tenant = TenantId(req.get_ref().tenant_id.clone());
        let tenant_id =
            scoped_tenant_id(&req, Some(&body_tenant)).unwrap_or_else(|| TenantId(String::new()));
        let inner = req.into_inner();
        let namespace = orch8_types::ids::Namespace(inner.namespace);
        let seq = self
            .storage
            .get_sequence_by_name(&tenant_id, &namespace, &inner.name, inner.version)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("sequence not found"))?;
        Ok(Response::new(proto::SequenceResponse {
            definition_json: to_json_string(&seq)?,
        }))
    }

    // --- Instances ---

    async fn create_instance(
        &self,
        req: Request<proto::CreateInstanceRequest>,
    ) -> Result<Response<proto::InstanceResponse>, Status> {
        let mut instance: orch8_types::instance::TaskInstance =
            from_json_str(&req.get_ref().instance_json)?;
        // Force the instance's tenant_id to match the caller. Without this,
        // a tenant-scoped client could create instances in other tenants by
        // forging `tenant_id` in the payload.
        instance.tenant_id = enforce_tenant_create(&req, &instance.tenant_id)?;
        self.storage
            .create_instance(&instance)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::InstanceResponse {
            instance_json: to_json_string(&instance)?,
        }))
    }

    async fn create_instances_batch(
        &self,
        req: Request<proto::CreateInstancesBatchRequest>,
    ) -> Result<Response<proto::CreateInstancesBatchResponse>, Status> {
        const MAX_BATCH_SIZE: usize = 10_000;
        if req.get_ref().instances_json.len() > MAX_BATCH_SIZE {
            return Err(Status::invalid_argument(format!(
                "batch size {} exceeds maximum {MAX_BATCH_SIZE}",
                req.get_ref().instances_json.len()
            )));
        }
        let mut instances: Vec<orch8_types::instance::TaskInstance> = req
            .get_ref()
            .instances_json
            .iter()
            .map(|s| from_json_str(s))
            .collect::<Result<_, _>>()?;
        // Every instance in the batch must belong to the caller's tenant.
        for inst in &mut instances {
            inst.tenant_id = enforce_tenant_create(&req, &inst.tenant_id)?;
        }
        let created = self
            .storage
            .create_instances_batch(&instances)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::CreateInstancesBatchResponse {
            created,
        }))
    }

    async fn get_instance(
        &self,
        req: Request<proto::GetInstanceRequest>,
    ) -> Result<Response<proto::InstanceResponse>, Status> {
        let id = InstanceId(parse_uuid(&req.get_ref().id)?);
        let inst = self
            .storage
            .get_instance(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("instance not found"))?;
        enforce_tenant_match(&req, &inst.tenant_id, "instance")?;
        Ok(Response::new(proto::InstanceResponse {
            instance_json: to_json_string(&inst)?,
        }))
    }

    async fn list_instances(
        &self,
        req: Request<proto::ListInstancesRequest>,
    ) -> Result<Response<proto::ListInstancesResponse>, Status> {
        let mut filter: orch8_types::filter::InstanceFilter =
            from_json_str(&req.get_ref().filter_json)?;
        // Override any tenant filter with the caller's tenant to stop a
        // tenant-scoped client from listing instances outside their tenant.
        if let Some(caller) = crate::auth::caller_tenant(&req) {
            filter.tenant_id = Some(caller.clone());
        }
        let pagination = req
            .get_ref()
            .pagination
            .as_ref()
            .map(|p| orch8_types::filter::Pagination {
                limit: p.limit,
                offset: u64::from(p.offset),
                sort_ascending: false,
            })
            .unwrap_or_default();
        let instances = self
            .storage
            .list_instances(&filter, &pagination)
            .await
            .map_err(storage_err)?;
        let json: Result<Vec<_>, _> = instances.iter().map(|i| to_json_string(i)).collect();
        Ok(Response::new(proto::ListInstancesResponse {
            instances_json: json?,
        }))
    }

    async fn update_state(
        &self,
        req: Request<proto::UpdateStateRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let id = InstanceId(parse_uuid(&req.get_ref().id)?);
        let inst = self
            .storage
            .get_instance(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("instance not found"))?;
        enforce_tenant_match(&req, &inst.tenant_id, "instance")?;
        let new_state: orch8_types::instance::InstanceState =
            InstanceState::from_str(&req.get_ref().new_state)
                .map_err(|e| Status::invalid_argument(e))?;
        // Validate the transition — HTTP path checks `can_transition_to`;
        // without this the gRPC path allows invalid moves like completed→running.
        if !inst.state.can_transition_to(new_state) {
            return Err(Status::failed_precondition(format!(
                "invalid transition: {} -> {}",
                inst.state, new_state
            )));
        }
        self.storage
            .update_instance_state(id, new_state, None)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::Empty {}))
    }

    async fn update_context(
        &self,
        req: Request<proto::UpdateContextRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let id = InstanceId(parse_uuid(&req.get_ref().id)?);
        let inst = self
            .storage
            .get_instance(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("instance not found"))?;
        enforce_tenant_match(&req, &inst.tenant_id, "instance")?;
        let ctx = from_json_str(&req.get_ref().context_json)?;
        self.storage
            .update_instance_context(id, &ctx)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::Empty {}))
    }

    async fn send_signal(
        &self,
        req: Request<proto::SendSignalRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let signal: orch8_types::signal::Signal = from_json_str(&req.get_ref().signal_json)?;
        // Signals target a specific instance — look it up so we can refuse
        // cross-tenant signal delivery.
        let inst = self
            .storage
            .get_instance(signal.instance_id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("instance not found"))?;
        enforce_tenant_match(&req, &inst.tenant_id, "instance")?;
        self.storage
            .enqueue_signal(&signal)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::Empty {}))
    }

    async fn get_outputs(
        &self,
        req: Request<proto::GetOutputsRequest>,
    ) -> Result<Response<proto::GetOutputsResponse>, Status> {
        let id = InstanceId(parse_uuid(&req.get_ref().instance_id)?);
        let inst = self
            .storage
            .get_instance(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("instance not found"))?;
        enforce_tenant_match(&req, &inst.tenant_id, "instance")?;
        let outputs = self
            .storage
            .get_all_outputs(id)
            .await
            .map_err(storage_err)?;
        let json: Result<Vec<_>, _> = outputs.iter().map(|o| to_json_string(o)).collect();
        Ok(Response::new(proto::GetOutputsResponse {
            outputs_json: json?,
        }))
    }

    async fn get_execution_tree(
        &self,
        req: Request<proto::GetExecutionTreeRequest>,
    ) -> Result<Response<proto::GetExecutionTreeResponse>, Status> {
        let id = InstanceId(parse_uuid(&req.get_ref().instance_id)?);
        let inst = self
            .storage
            .get_instance(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("instance not found"))?;
        enforce_tenant_match(&req, &inst.tenant_id, "instance")?;
        let nodes = self
            .storage
            .get_execution_tree(id)
            .await
            .map_err(storage_err)?;
        let json: Result<Vec<_>, _> = nodes.iter().map(|n| to_json_string(n)).collect();
        Ok(Response::new(proto::GetExecutionTreeResponse {
            nodes_json: json?,
        }))
    }

    async fn retry_instance(
        &self,
        req: Request<proto::RetryInstanceRequest>,
    ) -> Result<Response<proto::InstanceResponse>, Status> {
        // Must mirror the HTTP retry contract in
        // `orch8-api/src/instances/lifecycle.rs::retry_instance`:
        //   1. instance must exist and be in `Failed` state
        //   2. delete the stale execution tree so the evaluator rebuilds it
        //      (otherwise the old Failed/Running nodes cause immediate
        //      re-fail or deadlock)
        //   3. clear sentinel block outputs so permanently-failed steps can
        //      re-run, but keep real outputs so successful steps are skipped
        //   4. move the instance back to `Scheduled` with a fresh fire time
        // Previously the gRPC path only did step 4 — retrying an instance
        // over gRPC silently left stale tree state, producing divergent
        // behaviour from the HTTP client for the same operation.
        let id = InstanceId(parse_uuid(&req.get_ref().id)?);

        let inst = self
            .storage
            .get_instance(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("instance not found"))?;
        enforce_tenant_match(&req, &inst.tenant_id, "instance")?;

        if inst.state != orch8_types::instance::InstanceState::Failed {
            return Err(Status::failed_precondition(format!(
                "can only retry failed instances, current state: {}",
                inst.state
            )));
        }

        self.storage
            .delete_execution_tree(id)
            .await
            .map_err(storage_err)?;
        self.storage
            .delete_sentinel_block_outputs(id)
            .await
            .map_err(storage_err)?;
        self.storage
            .update_instance_state(
                id,
                orch8_types::instance::InstanceState::Scheduled,
                Some(chrono::Utc::now()),
            )
            .await
            .map_err(storage_err)?;

        let inst = self
            .storage
            .get_instance(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("instance not found"))?;
        Ok(Response::new(proto::InstanceResponse {
            instance_json: to_json_string(&inst)?,
        }))
    }

    async fn bulk_update_state(
        &self,
        req: Request<proto::BulkUpdateStateRequest>,
    ) -> Result<Response<proto::BulkUpdateStateResponse>, Status> {
        let mut filter: orch8_types::filter::InstanceFilter =
            from_json_str(&req.get_ref().filter_json)?;
        // Caller's tenant wins so bulk mutations can't escape tenant scope.
        if let Some(caller) = crate::auth::caller_tenant(&req) {
            filter.tenant_id = Some(caller.clone());
        }
        let new_state: orch8_types::instance::InstanceState =
            InstanceState::from_str(&req.get_ref().new_state)
                .map_err(|e| Status::invalid_argument(e))?;
        let updated = self
            .storage
            .bulk_update_state(&filter, new_state)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::BulkUpdateStateResponse { updated }))
    }

    async fn bulk_reschedule(
        &self,
        req: Request<proto::BulkRescheduleRequest>,
    ) -> Result<Response<proto::BulkRescheduleResponse>, Status> {
        let mut filter: orch8_types::filter::InstanceFilter =
            from_json_str(&req.get_ref().filter_json)?;
        if let Some(caller) = crate::auth::caller_tenant(&req) {
            filter.tenant_id = Some(caller.clone());
        }
        let offset_secs = req.get_ref().offset_secs;
        let updated = self
            .storage
            .bulk_reschedule(&filter, offset_secs)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::BulkRescheduleResponse { updated }))
    }

    // --- Cron ---

    async fn create_cron(
        &self,
        req: Request<proto::CreateCronRequest>,
    ) -> Result<Response<proto::CronResponse>, Status> {
        let mut schedule: orch8_types::cron::CronSchedule =
            from_json_str(&req.get_ref().schedule_json)?;
        schedule.tenant_id = enforce_tenant_create(&req, &schedule.tenant_id)?;
        self.storage
            .create_cron_schedule(&schedule)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::CronResponse {
            schedule_json: to_json_string(&schedule)?,
        }))
    }

    async fn get_cron(
        &self,
        req: Request<proto::GetCronRequest>,
    ) -> Result<Response<proto::CronResponse>, Status> {
        let id = parse_uuid(&req.get_ref().id)?;
        let schedule = self
            .storage
            .get_cron_schedule(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("cron schedule not found"))?;
        enforce_tenant_match(&req, &schedule.tenant_id, "cron schedule")?;
        Ok(Response::new(proto::CronResponse {
            schedule_json: to_json_string(&schedule)?,
        }))
    }

    async fn list_cron(
        &self,
        req: Request<proto::ListCronRequest>,
    ) -> Result<Response<proto::ListCronResponse>, Status> {
        let body_tenant = req.get_ref().tenant_id.clone().map(TenantId);
        // Caller-tenant override so a tenant-scoped client can't list
        // schedules outside their tenant by omitting the tenant filter.
        let tenant_id = scoped_tenant_id(&req, body_tenant.as_ref());
        let schedules = self
            .storage
            .list_cron_schedules(tenant_id.as_ref())
            .await
            .map_err(storage_err)?;
        let json: Result<Vec<_>, _> = schedules.iter().map(|s| to_json_string(s)).collect();
        Ok(Response::new(proto::ListCronResponse {
            schedules_json: json?,
        }))
    }

    async fn update_cron(
        &self,
        req: Request<proto::UpdateCronRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let mut schedule: orch8_types::cron::CronSchedule =
            from_json_str(&req.get_ref().schedule_json)?;
        // Verify the existing row belongs to the caller's tenant before
        // allowing an update. Also pin the updated row's tenant_id to the
        // caller so a tenant-scoped client can't reparent a schedule.
        let existing = self
            .storage
            .get_cron_schedule(schedule.id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("cron schedule not found"))?;
        enforce_tenant_match(&req, &existing.tenant_id, "cron schedule")?;
        // Pin the tenant_id to the existing row's tenant so the update cannot
        // reparent the schedule to a different tenant.
        schedule.tenant_id = existing.tenant_id.clone();
        self.storage
            .update_cron_schedule(&schedule)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::Empty {}))
    }

    async fn delete_cron(
        &self,
        req: Request<proto::DeleteCronRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let id = parse_uuid(&req.get_ref().id)?;
        let schedule = self
            .storage
            .get_cron_schedule(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("cron schedule not found"))?;
        enforce_tenant_match(&req, &schedule.tenant_id, "cron schedule")?;
        self.storage
            .delete_cron_schedule(id)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::Empty {}))
    }

    // --- Workers ---

    async fn poll_tasks(
        &self,
        req: Request<proto::PollTasksRequest>,
    ) -> Result<Response<proto::PollTasksResponse>, Status> {
        let scoped = crate::auth::caller_tenant(&req).cloned();
        let inner = req.into_inner();
        let limit = inner.limit.min(1000);
        // Route through tenant-aware claim when caller is tenant-scoped,
        // matching the HTTP path's isolation semantics.
        let tasks = if let Some(ref tid) = scoped {
            self.storage
                .claim_worker_tasks_for_tenant(&inner.handler_name, &inner.worker_id, tid, limit)
                .await
                .map_err(storage_err)?
        } else {
            self.storage
                .claim_worker_tasks(&inner.handler_name, &inner.worker_id, limit)
                .await
                .map_err(storage_err)?
        };
        let json: Result<Vec<_>, _> = tasks.iter().map(|t| to_json_string(t)).collect();
        Ok(Response::new(proto::PollTasksResponse {
            tasks_json: json?,
        }))
    }

    async fn complete_task(
        &self,
        req: Request<proto::CompleteTaskRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let task_id = parse_uuid(&req.get_ref().task_id)?;
        let caller_tenant = caller_tenant(&req).cloned();
        let (_pre_task, _pre_instance) =
            get_worker_task_checked(&self.storage, caller_tenant, task_id).await?;
        let inner = req.into_inner();
        let output: serde_json::Value = from_json_str(&inner.output_json)?;

        let updated = self
            .storage
            .complete_worker_task(task_id, &inner.worker_id, &output)
            .await
            .map_err(storage_err)?;
        if !updated {
            return Err(Status::not_found(format!("worker_task {task_id}")));
        }

        let task = self
            .storage
            .get_worker_task(task_id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found(format!("worker_task {task_id}")))?;

        let Some(mut instance) = self
            .storage
            .get_instance(task.instance_id)
            .await
            .map_err(storage_err)?
        else {
            return Ok(Response::new(proto::Empty {}));
        };
        if instance.state.is_terminal() || instance.state == InstanceState::Paused {
            tracing::info!(
                instance_id = %task.instance_id,
                state = %instance.state,
                "gRPC worker completion for terminal/paused instance; task accepted, transition skipped"
            );
            return Ok(Response::new(proto::Empty {}));
        }

        let mut merged_context = false;
        if let Some(obj) = output.as_object() {
            if instance.context.data.is_null() || !instance.context.data.is_object() {
                instance.context.data = serde_json::Value::Object(serde_json::Map::new());
            }
            if let Some(data_obj) = instance.context.data.as_object_mut() {
                for (k, v) in obj {
                    data_obj.insert(k.clone(), v.clone());
                }
                merged_context = true;
            }
        }

        let output_json = serde_json::to_string(&output).unwrap_or_else(|_| "{}".to_string());
        let block_output = orch8_types::output::BlockOutput {
            id: Uuid::now_v7(),
            instance_id: task.instance_id,
            block_id: task.block_id.clone(),
            output,
            output_ref: None,
            output_size: i32::try_from(output_json.len()).unwrap_or(i32::MAX),
            attempt: task.attempt,
            created_at: chrono::Utc::now(),
        };

        // Atomic: save output + complete node + transition instance in one tx.
        let tree = self
            .storage
            .get_execution_tree(task.instance_id)
            .await
            .map_err(storage_err)?;
        if let Some(node) = tree.iter().find(|n| {
            n.block_id == task.block_id
                && matches!(
                    n.state,
                    orch8_types::execution::NodeState::Running
                        | orch8_types::execution::NodeState::Waiting
                )
        }) {
            self.atomically_complete_node(
                &task,
                &block_output,
                node.id,
                merged_context,
                &instance.context,
            )
            .await?;
        }

        Ok(Response::new(proto::Empty {}))
    }

    async fn fail_task(
        &self,
        req: Request<proto::FailTaskRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let task_id = parse_uuid(&req.get_ref().task_id)?;
        let caller_tenant = caller_tenant(&req).cloned();
        let (_pre_task, _pre_instance) =
            get_worker_task_checked(&self.storage, caller_tenant, task_id).await?;
        let inner = req.into_inner();
        let updated = self
            .storage
            .fail_worker_task(task_id, &inner.worker_id, &inner.message, inner.retryable)
            .await
            .map_err(storage_err)?;
        if !updated {
            return Err(Status::not_found(format!("worker_task {task_id}")));
        }

        let task = self
            .storage
            .get_worker_task(task_id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found(format!("worker_task {task_id}")))?;

        let Some(inst) = self
            .storage
            .get_instance(task.instance_id)
            .await
            .map_err(storage_err)?
        else {
            return Ok(Response::new(proto::Empty {}));
        };
        if inst.state.is_terminal() {
            tracing::info!(
                instance_id = %task.instance_id,
                state = %inst.state,
                "gRPC worker failure for terminal instance; task accepted, transition skipped"
            );
            return Ok(Response::new(proto::Empty {}));
        }

        let tree = self
            .storage
            .get_execution_tree(task.instance_id)
            .await
            .map_err(storage_err)?;
        let active_node_id = tree
            .iter()
            .find(|n| {
                n.block_id == task.block_id
                    && matches!(
                        n.state,
                        orch8_types::execution::NodeState::Running
                            | orch8_types::execution::NodeState::Waiting
                    )
            })
            .map(|n| n.id);
        let has_tree = !tree.is_empty();

        if inner.retryable && worker_task_can_retry(&self.storage, &task).await? {
            let retry_task = retry_worker_task(&task);
            self.storage
                .delete_worker_task(task_id)
                .await
                .map_err(storage_err)?;
            self.storage
                .create_worker_task(&retry_task)
                .await
                .map_err(storage_err)?;
            if let Some(node_id) = active_node_id {
                self.storage
                    .update_node_state(node_id, orch8_types::execution::NodeState::Pending)
                    .await
                    .map_err(storage_err)?;
            }
        } else if has_tree {
            if let Some(node_id) = active_node_id {
                self.storage
                    .update_node_state(node_id, orch8_types::execution::NodeState::Failed)
                    .await
                    .map_err(storage_err)?;
            }
        } else {
            if inner.retryable {
                self.storage
                    .delete_worker_task(task_id)
                    .await
                    .map_err(storage_err)?;
            }
            self.storage
                .update_instance_state(task.instance_id, InstanceState::Failed, None)
                .await
                .map_err(storage_err)?;
            return Ok(Response::new(proto::Empty {}));
        }

        self.storage
            .update_instance_state(
                task.instance_id,
                InstanceState::Scheduled,
                Some(chrono::Utc::now()),
            )
            .await
            .map_err(storage_err)?;

        Ok(Response::new(proto::Empty {}))
    }

    async fn heartbeat_task(
        &self,
        req: Request<proto::HeartbeatTaskRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let task_id = parse_uuid(&req.get_ref().task_id)?;
        let caller_tenant = caller_tenant(&req).cloned();
        let (_task, _instance) =
            get_worker_task_checked(&self.storage, caller_tenant, task_id).await?;
        let inner = req.into_inner();
        let updated = self
            .storage
            .heartbeat_worker_task(task_id, &inner.worker_id)
            .await
            .map_err(storage_err)?;
        if !updated {
            return Err(Status::not_found(format!("worker_task {task_id}")));
        }
        Ok(Response::new(proto::Empty {}))
    }

    // --- Pools ---

    async fn create_pool(
        &self,
        req: Request<proto::CreatePoolRequest>,
    ) -> Result<Response<proto::PoolResponse>, Status> {
        let body_tenant = TenantId(req.get_ref().tenant_id.clone());
        let tenant_id = enforce_tenant_create(&req, &body_tenant)?;
        let inner = req.into_inner();
        let strategy: orch8_types::pool::RotationStrategy =
            from_json_str(&format!("\"{}\"", inner.strategy))?;
        let now = chrono::Utc::now();
        let pool = orch8_types::pool::ResourcePool {
            id: Uuid::now_v7(),
            tenant_id,
            name: inner.name,
            strategy,
            round_robin_index: 0,
            created_at: now,
            updated_at: now,
        };
        self.storage
            .create_resource_pool(&pool)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::PoolResponse {
            pool_json: to_json_string(&pool)?,
        }))
    }

    async fn get_pool(
        &self,
        req: Request<proto::GetPoolRequest>,
    ) -> Result<Response<proto::PoolResponse>, Status> {
        let id = parse_uuid(&req.get_ref().id)?;
        let pool = self
            .storage
            .get_resource_pool(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("pool not found"))?;
        enforce_tenant_match(&req, &pool.tenant_id, "pool")?;
        Ok(Response::new(proto::PoolResponse {
            pool_json: to_json_string(&pool)?,
        }))
    }

    async fn list_pools(
        &self,
        req: Request<proto::ListPoolsRequest>,
    ) -> Result<Response<proto::ListPoolsResponse>, Status> {
        let body_tenant = TenantId(req.get_ref().tenant_id.clone());
        // Caller-tenant override: tenant-scoped clients see only their pools.
        let tenant_id = scoped_tenant_id(&req, Some(&body_tenant)).unwrap_or(body_tenant);
        let pools = self
            .storage
            .list_resource_pools(&tenant_id)
            .await
            .map_err(storage_err)?;
        let json: Result<Vec<_>, _> = pools.iter().map(|p| to_json_string(p)).collect();
        Ok(Response::new(proto::ListPoolsResponse {
            pools_json: json?,
        }))
    }

    async fn delete_pool(
        &self,
        req: Request<proto::DeletePoolRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let id = parse_uuid(&req.get_ref().id)?;
        let pool = self
            .storage
            .get_resource_pool(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("pool not found"))?;
        enforce_tenant_match(&req, &pool.tenant_id, "pool")?;
        self.storage
            .delete_resource_pool(id)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::Empty {}))
    }

    async fn add_resource(
        &self,
        req: Request<proto::AddResourceRequest>,
    ) -> Result<Response<proto::ResourceResponse>, Status> {
        let pool_id = parse_uuid(&req.get_ref().pool_id)?;
        // Resources inherit tenancy from the pool — tenant-check the pool
        // before allowing a write to it.
        let pool = self
            .storage
            .get_resource_pool(pool_id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("pool not found"))?;
        enforce_tenant_match(&req, &pool.tenant_id, "pool")?;
        let inner = req.into_inner();

        #[derive(serde::Deserialize)]
        struct AddReq {
            resource_key: String,
            name: String,
            #[serde(default = "default_weight")]
            weight: u32,
            #[serde(default)]
            daily_cap: u32,
            #[serde(default)]
            warmup_start: Option<String>,
            #[serde(default)]
            warmup_days: u32,
            #[serde(default)]
            warmup_start_cap: u32,
        }
        const fn default_weight() -> u32 {
            1
        }

        let r: AddReq = from_json_str(&inner.resource_json)?;
        let warmup_start = r
            .warmup_start
            .and_then(|s| chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok());

        let resource = orch8_types::pool::PoolResource {
            id: Uuid::now_v7(),
            pool_id,
            resource_key: orch8_types::ids::ResourceKey(r.resource_key),
            name: r.name,
            weight: r.weight,
            enabled: true,
            daily_cap: r.daily_cap,
            daily_usage: 0,
            daily_usage_date: None,
            warmup_start,
            warmup_days: r.warmup_days,
            warmup_start_cap: r.warmup_start_cap,
            created_at: chrono::Utc::now(),
        };
        self.storage
            .add_pool_resource(&resource)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::ResourceResponse {
            resource_json: to_json_string(&resource)?,
        }))
    }

    async fn list_resources(
        &self,
        req: Request<proto::ListResourcesRequest>,
    ) -> Result<Response<proto::ListResourcesResponse>, Status> {
        let pool_id = parse_uuid(&req.get_ref().pool_id)?;
        let pool = self
            .storage
            .get_resource_pool(pool_id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("pool not found"))?;
        enforce_tenant_match(&req, &pool.tenant_id, "pool")?;
        let resources = self
            .storage
            .list_pool_resources(pool_id)
            .await
            .map_err(storage_err)?;
        let json: Result<Vec<_>, _> = resources.iter().map(|r| to_json_string(r)).collect();
        Ok(Response::new(proto::ListResourcesResponse {
            resources_json: json?,
        }))
    }

    async fn update_resource(
        &self,
        req: Request<proto::UpdateResourceRequest>,
    ) -> Result<Response<proto::ResourceResponse>, Status> {
        let pool_id = parse_uuid(&req.get_ref().pool_id)?;
        let resource_id = parse_uuid(&req.get_ref().resource_id)?;
        // Gate pool-membership writes on tenant ownership of the pool.
        let pool = self
            .storage
            .get_resource_pool(pool_id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("pool not found"))?;
        enforce_tenant_match(&req, &pool.tenant_id, "pool")?;
        let inner = req.into_inner();

        #[derive(serde::Deserialize)]
        struct UpdateReq {
            name: Option<String>,
            weight: Option<u32>,
            enabled: Option<bool>,
            daily_cap: Option<u32>,
            warmup_start: Option<String>,
            warmup_days: Option<u32>,
            warmup_start_cap: Option<u32>,
        }

        let upd: UpdateReq = from_json_str(&inner.update_json)?;
        let resources = self
            .storage
            .list_pool_resources(pool_id)
            .await
            .map_err(storage_err)?;
        let mut resource = resources
            .into_iter()
            .find(|r| r.id == resource_id)
            .ok_or_else(|| Status::not_found("resource not found"))?;

        if let Some(name) = upd.name {
            resource.name = name;
        }
        if let Some(weight) = upd.weight {
            resource.weight = weight;
        }
        if let Some(enabled) = upd.enabled {
            resource.enabled = enabled;
        }
        if let Some(daily_cap) = upd.daily_cap {
            resource.daily_cap = daily_cap;
        }
        if let Some(warmup_start) = upd.warmup_start {
            resource.warmup_start =
                chrono::NaiveDate::parse_from_str(&warmup_start, "%Y-%m-%d").ok();
        }
        if let Some(warmup_days) = upd.warmup_days {
            resource.warmup_days = warmup_days;
        }
        if let Some(warmup_start_cap) = upd.warmup_start_cap {
            resource.warmup_start_cap = warmup_start_cap;
        }

        self.storage
            .update_pool_resource(&resource)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::ResourceResponse {
            resource_json: to_json_string(&resource)?,
        }))
    }

    async fn delete_resource(
        &self,
        req: Request<proto::DeleteResourceRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let pool_id = parse_uuid(&req.get_ref().pool_id)?;
        let resource_id = parse_uuid(&req.get_ref().resource_id)?;
        // Use the caller-provided pool_id to check tenancy, but also verify
        // the resource actually lives in that pool — otherwise a
        // tenant-scoped client could delete a foreign-tenant resource by
        // pointing a pool they own at it.
        let pool = self
            .storage
            .get_resource_pool(pool_id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("pool not found"))?;
        enforce_tenant_match(&req, &pool.tenant_id, "pool")?;
        let resources = self
            .storage
            .list_pool_resources(pool_id)
            .await
            .map_err(storage_err)?;
        if !resources.iter().any(|r| r.id == resource_id) {
            return Err(Status::not_found("resource not found"));
        }
        self.storage
            .delete_pool_resource(resource_id)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::Empty {}))
    }
}
