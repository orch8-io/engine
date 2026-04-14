use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Orch8.io API",
        description = "Durable task sequencing engine — REST API",
        version = "0.1.0",
        license(name = "BUSL-1.1"),
    ),
    paths(
        // Health
        crate::health::liveness,
        crate::health::readiness,
        // Sequences
        crate::sequences::create_sequence,
        crate::sequences::get_sequence,
        crate::sequences::get_sequence_by_name,
        crate::sequences::deprecate_sequence,
        crate::sequences::list_sequence_versions,
        crate::sequences::list_sequences,
        crate::sequences::migrate_instance,
        // Instances
        crate::instances::create_instance,
        crate::instances::list_instances,
        crate::instances::create_instances_batch,
        crate::instances::get_instance,
        crate::instances::update_state,
        crate::instances::update_context,
        crate::instances::send_signal,
        crate::instances::get_outputs,
        crate::instances::get_execution_tree,
        crate::instances::retry_instance,
        crate::instances::bulk_update_state,
        crate::instances::bulk_reschedule,
        crate::instances::list_dlq,
        crate::instances::save_checkpoint,
        crate::instances::list_checkpoints,
        crate::instances::get_latest_checkpoint,
        crate::instances::prune_checkpoints,
        crate::instances::list_audit_log,
        crate::instances::inject_blocks,
        // Cron
        crate::cron::create_cron,
        crate::cron::get_cron,
        crate::cron::list_cron,
        crate::cron::update_cron,
        crate::cron::delete_cron,
        // Workers
        crate::workers::poll_tasks,
        crate::workers::poll_tasks_from_queue,
        crate::workers::complete_task,
        crate::workers::fail_task,
        crate::workers::heartbeat_task,
        // Cluster
        crate::cluster::list_nodes,
        crate::cluster::drain_node,
        // Sessions
        crate::sessions::create_session,
        crate::sessions::get_session,
        crate::sessions::get_session_by_key,
        crate::sessions::update_session_data,
        crate::sessions::update_session_state,
        crate::sessions::list_session_instances,
        // Circuit breakers
        crate::circuit_breakers::list_all_breakers,
        crate::circuit_breakers::list_breakers_for_tenant,
        crate::circuit_breakers::get_breaker,
        crate::circuit_breakers::reset_breaker,
        // Pools
        crate::pools::create_pool,
        crate::pools::list_pools,
        crate::pools::get_pool,
        crate::pools::delete_pool,
        crate::pools::list_resources,
        crate::pools::add_resource,
        crate::pools::update_resource,
        crate::pools::delete_resource,
    ),
    components(schemas(
        // IDs
        orch8_types::ids::InstanceId,
        orch8_types::ids::SequenceId,
        orch8_types::ids::ExecutionNodeId,
        orch8_types::ids::BlockId,
        orch8_types::ids::TenantId,
        orch8_types::ids::Namespace,
        orch8_types::ids::ResourceKey,
        // Instance
        orch8_types::instance::InstanceState,
        orch8_types::instance::Priority,
        orch8_types::instance::TaskInstance,
        // Context
        orch8_types::context::ExecutionContext,
        orch8_types::context::RuntimeContext,
        orch8_types::context::AuditEntry,
        // Execution tree
        orch8_types::execution::NodeState,
        orch8_types::execution::BlockType,
        orch8_types::execution::ExecutionNode,
        // Sequence
        orch8_types::sequence::SequenceDefinition,
        orch8_types::sequence::BlockDefinition,
        orch8_types::sequence::StepDef,
        orch8_types::sequence::SubSequenceDef,
        orch8_types::sequence::DelaySpec,
        orch8_types::sequence::SendWindow,
        orch8_types::sequence::ContextAccess,
        orch8_types::sequence::HumanInputDef,
        orch8_types::sequence::RetryPolicy,
        orch8_types::sequence::ParallelDef,
        orch8_types::sequence::RaceDef,
        orch8_types::sequence::RaceSemantics,
        orch8_types::sequence::TryCatchDef,
        orch8_types::sequence::LoopDef,
        orch8_types::sequence::ForEachDef,
        orch8_types::sequence::RouterDef,
        orch8_types::sequence::Route,
        orch8_types::sequence::ABSplitDef,
        orch8_types::sequence::ABVariant,
        orch8_types::sequence::EscalationDef,
        // Signal
        orch8_types::signal::Signal,
        orch8_types::signal::SignalType,
        // Output
        orch8_types::output::BlockOutput,
        // Cron
        orch8_types::cron::CronSchedule,
        // Worker
        orch8_types::worker::WorkerTask,
        orch8_types::worker::WorkerTaskState,
        // Audit
        orch8_types::audit::AuditLogEntry,
        // Session
        orch8_types::session::Session,
        orch8_types::session::SessionState,
        // Circuit breaker
        orch8_types::circuit_breaker::CircuitBreakerState,
        orch8_types::circuit_breaker::BreakerState,
        // Interceptor
        orch8_types::interceptor::InterceptorDef,
        orch8_types::interceptor::InterceptorAction,
        // Pools
        orch8_types::pool::ResourcePool,
        orch8_types::pool::PoolResource,
        orch8_types::pool::RotationStrategy,
        // Cluster
        orch8_types::cluster::ClusterNode,
        orch8_types::cluster::NodeStatus,
        orch8_types::checkpoint::Checkpoint,
        crate::instances::SaveCheckpointRequest,
        crate::instances::PruneCheckpointsRequest,
        crate::instances::InjectBlocksRequest,
        crate::sessions::CreateSessionRequest,
        crate::sessions::UpdateSessionDataRequest,
        crate::sessions::UpdateSessionStateRequest,
        crate::sequences::MigrateInstanceRequest,
        crate::workers::QueuePollRequest,
        crate::pools::CreatePoolRequest,
        crate::pools::AddResourceRequest,
        crate::pools::UpdateResourceRequest,
        // Credentials
        crate::credentials::CredentialResponse,
        orch8_types::credential::CredentialKind,
        crate::credentials::CreateCredentialRequest,
        crate::credentials::UpdateCredentialRequest,
    )),
    tags(
        (name = "health", description = "Health check endpoints"),
        (name = "sequences", description = "Sequence definition management"),
        (name = "instances", description = "Task instance lifecycle"),
        (name = "cron", description = "Cron schedule management"),
        (name = "workers", description = "External worker task polling"),
        (name = "sessions", description = "Cross-instance session management"),
        (name = "circuit_breakers", description = "Circuit breaker inspection and reset"),
        (name = "pools", description = "Resource pool management"),
        (name = "cluster", description = "Multi-node cluster management"),
        (name = "credentials", description = "Shared secrets referenced by step params via credentials://<id>"),
        (name = "webhooks", description = "Public, unauthenticated webhook ingestion (HMAC-protected via trigger secret)"),
    )
)]
pub struct ApiDoc;
