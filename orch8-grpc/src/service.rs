use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing;
use uuid::Uuid;

use orch8_storage::StorageBackend;
use orch8_types::ids::{InstanceId, SequenceId, TenantId};

use crate::proto::{self, orch8_service_server::Orch8Service};

pub struct Orch8GrpcService {
    storage: Arc<dyn StorageBackend>,
}

impl Orch8GrpcService {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
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
    serde_json::from_str(s).map_err(|_| Status::invalid_argument("invalid JSON payload"))
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
        let seq = from_json_str(&req.into_inner().definition_json)?;
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
        let id = SequenceId(parse_uuid(&req.into_inner().id)?);
        let seq = self
            .storage
            .get_sequence(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("sequence not found"))?;
        Ok(Response::new(proto::SequenceResponse {
            definition_json: to_json_string(&seq)?,
        }))
    }

    async fn get_sequence_by_name(
        &self,
        req: Request<proto::GetSequenceByNameRequest>,
    ) -> Result<Response<proto::SequenceResponse>, Status> {
        let inner = req.into_inner();
        let tenant_id = TenantId(inner.tenant_id);
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
        let instance = from_json_str(&req.into_inner().instance_json)?;
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
        let inner = req.into_inner();
        const MAX_BATCH_SIZE: usize = 10_000;
        if inner.instances_json.len() > MAX_BATCH_SIZE {
            return Err(Status::invalid_argument(format!(
                "batch size {} exceeds maximum {MAX_BATCH_SIZE}",
                inner.instances_json.len()
            )));
        }
        let instances: Vec<orch8_types::instance::TaskInstance> = inner
            .instances_json
            .iter()
            .map(|s| from_json_str(s))
            .collect::<Result<_, _>>()?;
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
        let id = InstanceId(parse_uuid(&req.into_inner().id)?);
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

    async fn list_instances(
        &self,
        req: Request<proto::ListInstancesRequest>,
    ) -> Result<Response<proto::ListInstancesResponse>, Status> {
        let inner = req.into_inner();
        let filter = from_json_str(&inner.filter_json)?;
        let pagination = inner
            .pagination
            .map(|p| orch8_types::filter::Pagination {
                limit: p.limit,
                offset: u64::from(p.offset),
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
        let inner = req.into_inner();
        let id = InstanceId(parse_uuid(&inner.id)?);
        let new_state: orch8_types::instance::InstanceState =
            from_json_str(&format!("\"{}\"", inner.new_state))?;
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
        let inner = req.into_inner();
        let id = InstanceId(parse_uuid(&inner.id)?);
        let ctx = from_json_str(&inner.context_json)?;
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
        let inner = req.into_inner();
        let signal = from_json_str(&inner.signal_json)?;
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
        let id = InstanceId(parse_uuid(&req.into_inner().instance_id)?);
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
        let id = InstanceId(parse_uuid(&req.into_inner().instance_id)?);
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
        let id = InstanceId(parse_uuid(&req.into_inner().id)?);
        self.storage
            .update_instance_state(id, orch8_types::instance::InstanceState::Scheduled, None)
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
        let inner = req.into_inner();
        let filter = from_json_str(&inner.filter_json)?;
        let new_state: orch8_types::instance::InstanceState =
            from_json_str(&format!("\"{}\"", inner.new_state))?;
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
        let inner = req.into_inner();
        let filter = from_json_str(&inner.filter_json)?;
        let updated = self
            .storage
            .bulk_reschedule(&filter, inner.offset_secs)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::BulkRescheduleResponse { updated }))
    }

    // --- Cron ---

    async fn create_cron(
        &self,
        req: Request<proto::CreateCronRequest>,
    ) -> Result<Response<proto::CronResponse>, Status> {
        let schedule = from_json_str(&req.into_inner().schedule_json)?;
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
        let id = parse_uuid(&req.into_inner().id)?;
        let schedule = self
            .storage
            .get_cron_schedule(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("cron schedule not found"))?;
        Ok(Response::new(proto::CronResponse {
            schedule_json: to_json_string(&schedule)?,
        }))
    }

    async fn list_cron(
        &self,
        req: Request<proto::ListCronRequest>,
    ) -> Result<Response<proto::ListCronResponse>, Status> {
        let inner = req.into_inner();
        let tenant_id = inner.tenant_id.map(TenantId);
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
        let schedule = from_json_str(&req.into_inner().schedule_json)?;
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
        let id = parse_uuid(&req.into_inner().id)?;
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
        let inner = req.into_inner();
        let tasks = self
            .storage
            .claim_worker_tasks(&inner.handler_name, &inner.worker_id, inner.limit)
            .await
            .map_err(storage_err)?;
        let json: Result<Vec<_>, _> = tasks.iter().map(|t| to_json_string(t)).collect();
        Ok(Response::new(proto::PollTasksResponse {
            tasks_json: json?,
        }))
    }

    async fn complete_task(
        &self,
        req: Request<proto::CompleteTaskRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let inner = req.into_inner();
        let task_id = parse_uuid(&inner.task_id)?;
        let output: serde_json::Value = from_json_str(&inner.output_json)?;
        self.storage
            .complete_worker_task(task_id, &inner.worker_id, &output)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::Empty {}))
    }

    async fn fail_task(
        &self,
        req: Request<proto::FailTaskRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let inner = req.into_inner();
        let task_id = parse_uuid(&inner.task_id)?;
        self.storage
            .fail_worker_task(task_id, &inner.worker_id, &inner.message, inner.retryable)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::Empty {}))
    }

    async fn heartbeat_task(
        &self,
        req: Request<proto::HeartbeatTaskRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let inner = req.into_inner();
        let task_id = parse_uuid(&inner.task_id)?;
        self.storage
            .heartbeat_worker_task(task_id, &inner.worker_id)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::Empty {}))
    }

    // --- Pools ---

    async fn create_pool(
        &self,
        req: Request<proto::CreatePoolRequest>,
    ) -> Result<Response<proto::PoolResponse>, Status> {
        let inner = req.into_inner();
        let strategy: orch8_types::pool::RotationStrategy =
            from_json_str(&format!("\"{}\"", inner.strategy))?;
        let now = chrono::Utc::now();
        let pool = orch8_types::pool::ResourcePool {
            id: Uuid::now_v7(),
            tenant_id: TenantId(inner.tenant_id),
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
        let id = parse_uuid(&req.into_inner().id)?;
        let pool = self
            .storage
            .get_resource_pool(id)
            .await
            .map_err(storage_err)?
            .ok_or_else(|| Status::not_found("pool not found"))?;
        Ok(Response::new(proto::PoolResponse {
            pool_json: to_json_string(&pool)?,
        }))
    }

    async fn list_pools(
        &self,
        req: Request<proto::ListPoolsRequest>,
    ) -> Result<Response<proto::ListPoolsResponse>, Status> {
        let tenant_id = TenantId(req.into_inner().tenant_id);
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
        let id = parse_uuid(&req.into_inner().id)?;
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
        let inner = req.into_inner();
        let pool_id = parse_uuid(&inner.pool_id)?;

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
        fn default_weight() -> u32 {
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
        let pool_id = parse_uuid(&req.into_inner().pool_id)?;
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
        let inner = req.into_inner();
        let pool_id = parse_uuid(&inner.pool_id)?;
        let resource_id = parse_uuid(&inner.resource_id)?;

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
        let inner = req.into_inner();
        let resource_id = parse_uuid(&inner.resource_id)?;
        self.storage
            .delete_pool_resource(resource_id)
            .await
            .map_err(storage_err)?;
        Ok(Response::new(proto::Empty {}))
    }
}
