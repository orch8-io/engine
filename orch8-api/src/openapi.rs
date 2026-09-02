use utoipa::openapi::path::{HttpMethod, OperationBuilder};
use utoipa::openapi::response::{Response, ResponsesBuilder};
use utoipa::{Modify, OpenApi};

/// Continuity operations are registered centrally because their handlers are
/// intentionally split across a broad, evolving protocol surface. Keeping the
/// operation IDs here makes every route available to SDK generators while the
/// endpoint-specific schemas are migrated to `utoipa::path` annotations.
pub(crate) struct ContinuityOpenApi;

pub(crate) const CONTINUITY_OPERATIONS: &[(&str, HttpMethod, &str)] = &[
    (
        "/continuity/executions",
        HttpMethod::Post,
        "create_execution",
    ),
    (
        "/continuity/executions/{id}",
        HttpMethod::Get,
        "get_execution",
    ),
    (
        "/continuity/executions/{id}/locations",
        HttpMethod::Get,
        "list_locations",
    ),
    (
        "/continuity/executions/{id}/handoff-preview",
        HttpMethod::Post,
        "handoff_preview",
    ),
    ("/continuity/handoffs", HttpMethod::Post, "create_handoff"),
    ("/continuity/handoffs/{id}", HttpMethod::Get, "get_handoff"),
    (
        "/continuity/handoffs/{id}/export",
        HttpMethod::Post,
        "export_handoff",
    ),
    (
        "/continuity/handoffs/{id}/attach-device-capsule",
        HttpMethod::Post,
        "attach_device_capsule",
    ),
    (
        "/continuity/handoffs/{id}/accept",
        HttpMethod::Post,
        "accept_handoff",
    ),
    (
        "/continuity/handoffs/{id}/accept-external",
        HttpMethod::Post,
        "accept_external_handoff",
    ),
    (
        "/continuity/handoffs/{id}/reject",
        HttpMethod::Post,
        "reject_handoff",
    ),
    (
        "/continuity/handoffs/{id}/resume",
        HttpMethod::Post,
        "resume_handoff",
    ),
    (
        "/continuity/handoffs/{id}/resume-external",
        HttpMethod::Post,
        "resume_external_handoff",
    ),
    (
        "/continuity/handoffs/{id}/revoke",
        HttpMethod::Post,
        "revoke_handoff",
    ),
    (
        "/continuity/capsules/import",
        HttpMethod::Post,
        "import_capsule",
    ),
    (
        "/continuity/grants",
        HttpMethod::Post,
        "issue_continuation_grant",
    ),
    (
        "/continuity/grants/consume",
        HttpMethod::Post,
        "consume_continuation_grant",
    ),
    (
        "/continuity/executions/{id}/effects",
        HttpMethod::Get,
        "list_effects",
    ),
    (
        "/instances/{id}/effects",
        HttpMethod::Get,
        "list_instance_effects",
    ),
    (
        "/continuity/effects/{id}/resolve",
        HttpMethod::Post,
        "resolve_effect",
    ),
    (
        "/continuity/executions/{id}/provenance",
        HttpMethod::Get,
        "list_provenance",
    ),
    (
        "/continuity/executions/{id}/provenance",
        HttpMethod::Post,
        "record_provenance_boundary",
    ),
    (
        "/continuity/executions/{id}/provenance/verify",
        HttpMethod::Get,
        "verify_provenance",
    ),
    ("/runtimes/register", HttpMethod::Post, "register_runtime"),
    ("/runtimes", HttpMethod::Get, "list_runtimes"),
    (
        "/continuity/executions/{id}/placement",
        HttpMethod::Post,
        "choose_placement",
    ),
    ("/continuity/streams", HttpMethod::Post, "create_stream"),
    (
        "/continuity/streams/{id}/frames",
        HttpMethod::Get,
        "list_stream_frames",
    ),
    (
        "/continuity/streams/{id}/frames",
        HttpMethod::Post,
        "append_stream_frame",
    ),
    (
        "/continuity/streams/{id}/windows",
        HttpMethod::Get,
        "list_stream_windows",
    ),
    (
        "/continuity/streams/{id}/retract",
        HttpMethod::Post,
        "retract_stream_frames",
    ),
    ("/continuity/invariants", HttpMethod::Get, "list_invariants"),
    (
        "/continuity/invariants",
        HttpMethod::Post,
        "create_invariant",
    ),
    (
        "/continuity/executions/{id}/invariants/evaluate",
        HttpMethod::Post,
        "evaluate_invariants",
    ),
    (
        "/continuity/executions/{id}/invariants/results",
        HttpMethod::Get,
        "list_invariant_results",
    ),
    (
        "/continuity/executions/{id}/evaluations",
        HttpMethod::Get,
        "list_evaluations",
    ),
    (
        "/continuity/executions/{id}/evaluations",
        HttpMethod::Post,
        "append_evaluation",
    ),
    (
        "/continuity/executions/{id}/budget-reservations",
        HttpMethod::Get,
        "list_execution_budget_reservations",
    ),
    (
        "/continuity/executions/{id}/budget-reservations",
        HttpMethod::Post,
        "reserve_execution_budget",
    ),
    (
        "/continuity/executions/{id}/budget-reservations/{reservation_id}/reconcile",
        HttpMethod::Post,
        "reconcile_execution_budget",
    ),
    (
        "/continuity/executions/{id}/budget-reservations/{reservation_id}/release",
        HttpMethod::Post,
        "release_execution_budget",
    ),
    (
        "/continuity/attention",
        HttpMethod::Post,
        "create_attention_task",
    ),
    (
        "/continuity/attention/{id}/assign",
        HttpMethod::Post,
        "assign_attention_task",
    ),
    (
        "/continuity/attention/{id}/decide",
        HttpMethod::Post,
        "decide_attention_task",
    ),
    (
        "/continuity/executions/{id}/checkpoints",
        HttpMethod::Get,
        "list_continuity_checkpoints",
    ),
    (
        "/continuity/executions/{id}/checkpoints/{checkpoint_id}",
        HttpMethod::Get,
        "get_continuity_checkpoint",
    ),
    (
        "/continuity/executions/{id}/what-if",
        HttpMethod::Get,
        "list_what_if_runs",
    ),
    (
        "/continuity/executions/{id}/what-if",
        HttpMethod::Post,
        "run_what_if",
    ),
    (
        "/continuity/executions/{id}/test-fixture",
        HttpMethod::Post,
        "extract_test_fixture",
    ),
    (
        "/continuity/migrations/plan",
        HttpMethod::Post,
        "plan_live_migration",
    ),
    (
        "/continuity/migrations/{id}",
        HttpMethod::Get,
        "get_live_migration",
    ),
    (
        "/continuity/migrations/{id}/apply",
        HttpMethod::Post,
        "apply_live_migration",
    ),
    (
        "/continuity/migrations/{id}/rollback",
        HttpMethod::Post,
        "rollback_live_migration",
    ),
    (
        "/continuity/executions/{id}/compensations/preview",
        HttpMethod::Post,
        "preview_compensation",
    ),
    (
        "/continuity/executions/{id}/compensations",
        HttpMethod::Post,
        "create_compensation_run",
    ),
    (
        "/continuity/compensations/{id}",
        HttpMethod::Get,
        "get_compensation_run",
    ),
    (
        "/continuity/compensations/{id}/claim",
        HttpMethod::Post,
        "claim_compensation_step",
    ),
    (
        "/continuity/compensations/{id}/steps/{effect_id}/complete",
        HttpMethod::Post,
        "complete_compensation_step",
    ),
    (
        "/continuity/compensations/{id}/steps/{effect_id}/fail",
        HttpMethod::Post,
        "fail_compensation_step",
    ),
    (
        "/continuity/compensations/{id}/steps/{effect_id}/verify",
        HttpMethod::Post,
        "verify_compensation_step",
    ),
    (
        "/continuity/scenarios/generate",
        HttpMethod::Post,
        "generate_scenarios",
    ),
    (
        "/continuity/scenarios/reproduce",
        HttpMethod::Post,
        "reproduce_incident",
    ),
    (
        "/continuity/fault-lab/run",
        HttpMethod::Post,
        "run_fault_lab",
    ),
    (
        "/continuity/providers/choose",
        HttpMethod::Post,
        "choose_provider",
    ),
    (
        "/continuity/optimizations/recommend",
        HttpMethod::Post,
        "recommend_optimizations",
    ),
    (
        "/continuity/optimizations/{id}/accept",
        HttpMethod::Post,
        "accept_optimization",
    ),
    (
        "/continuity/evaluations/gate",
        HttpMethod::Post,
        "evaluate_gate",
    ),
    (
        "/continuity/evaluations/stored-gate",
        HttpMethod::Post,
        "evaluate_stored_gate",
    ),
    (
        "/continuity/residency/evaluate",
        HttpMethod::Post,
        "evaluate_residency",
    ),
    (
        "/continuity/disclosure/minimize",
        HttpMethod::Post,
        "minimize_disclosure",
    ),
    (
        "/continuity/federation/verify",
        HttpMethod::Post,
        "verify_federation",
    ),
    (
        "/continuity/federation/sign",
        HttpMethod::Post,
        "sign_federation",
    ),
    (
        "/continuity/delegations/claim",
        HttpMethod::Post,
        "claim_delegation",
    ),
];

impl Modify for ContinuityOpenApi {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        for (path, method, operation_id) in CONTINUITY_OPERATIONS {
            let responses = ResponsesBuilder::new()
                .response("default", Response::new("Continuity API response"))
                .build();
            let operation = OperationBuilder::new()
                .operation_id(Some(*operation_id))
                .tag("continuity")
                .responses(responses)
                .build();
            openapi
                .paths
                .add_path_operation(path, vec![method.clone()], operation);
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    modifiers(&ContinuityOpenApi),
    info(
        title = "Orch8.io API",
        description = "Durable task sequencing engine — REST API.\n\n\
            All versioned endpoints are available under the `/api/v1` prefix \
            (canonical) and also at the bare path for backward compatibility. \
            Bare-path responses carry Deprecation, Sunset, and successor-version +            Link headers. Health and metrics endpoints remain at the root +            (`/health/*`, `/metrics`).",
        version = "1.0.0",
        license(name = "BUSL-1.1"),
    ),
    servers(
        (url = "/api/v1", description = "Versioned API (canonical)"),
        (url = "/", description = "Unversioned (deprecated, backward-compat)"),
    ),
    paths(
        // Health
        crate::health::liveness,
        crate::health::readiness,
        crate::health::info,
        // Sequences
        crate::sequences::create_sequence,
        crate::sequences::get_sequence,
        crate::sequences::get_sequence_by_name,
        crate::sequences::deprecate_sequence,
        crate::sequences::list_sequence_versions,
        crate::sequences::list_sequences,
        crate::sequences::migrate_instance,
        // Preflight
        crate::preflight::preflight_draft,
        crate::preflight::preflight_stored,
        crate::dataflow::compile_draft,
        crate::dataflow::compile_stored,
        // Instances
        crate::instances::create_instance,
        crate::instances::list_instances,
        crate::instances::create_instances_batch,
        crate::instances::get_instance,
        crate::instances::get_instance_children,
        crate::instances::get_instance_logs,
        crate::instances::update_state,
        crate::instances::update_context,
        crate::instances::send_signal,
        crate::instances::get_outputs,
        crate::instances::list_instance_artifacts,
        crate::instances::get_artifact_bytes,
        crate::instances::get_execution_tree,
        crate::instances::get_timeline,
        crate::instances::retry_instance,
        crate::instances::resume_from_block,
        crate::instances::fork_instance,
        crate::instances::bulk_update_state,
        crate::instances::bulk_reschedule,
        crate::instances::batch_action,
        crate::instances::list_dlq,
        crate::instances::save_checkpoint,
        crate::instances::list_checkpoints,
        crate::instances::get_latest_checkpoint,
        crate::instances::prune_checkpoints,
        crate::instances::list_audit_log,
        crate::instances::inject_blocks,
        // Diagnosis
        crate::diagnosis::get_diagnosis,
        crate::diagnosis::preview_remediations,
        crate::diagnosis::apply_remediation,
        crate::changes::list_changes,
        crate::changes::stream_changes,
        // Template inspector
        crate::inspect::inspect_draft,
        crate::inspect::inspect_instance_block,
        // Template debugger
        crate::inspect::debug_template_endpoint,
        // Events
        crate::events::ingest_event,
        crate::events::ingest_event_batch,
        crate::events::list_events,
        crate::events::get_event,
        // Workbench
        crate::workbench::get_workbench,
        crate::workbench::compare_runs,
        crate::workbench::fork_preview,
        // DLQ root-cause groups
        crate::dlq_groups::list_groups,
        crate::dlq_groups::retry_group,
        // Releases
        crate::releases::create_release,
        crate::releases::list_releases,
        crate::releases::get_release,
        crate::releases::list_decisions,
        crate::releases::diff_release,
        crate::releases::diff_sequences,
        crate::releases::validate_release,
        crate::releases::start_canary,
        crate::releases::evaluate_release,
        crate::releases::promote_release,
        crate::releases::pause_release,
        crate::releases::rollback_release,
        // Cron
        crate::cron::create_cron,
        crate::cron::get_cron,
        crate::cron::list_cron,
        crate::cron::update_cron,
        crate::cron::delete_cron,
        crate::cron::next_fires,
        // Webhook outbox
        crate::webhook_outbox::list_outbox,
        crate::webhook_outbox::redeliver_outbox,
        crate::webhook_outbox::discard_outbox,
        crate::webhook_outbox::redeliver_preview,
        crate::webhook_outbox::list_deliveries,
        crate::webhook_outbox::get_delivery,
        // Queue routing
        crate::queue_routing::create_rule,
        crate::queue_routing::list_rules,
        crate::queue_routing::get_rule,
        crate::queue_routing::delete_rule,
        // Queue dispatch
        crate::queue_dispatch::set_dispatch,
        crate::queue_dispatch::list_dispatch,
        crate::queue_dispatch::delete_dispatch,
        // Workers
        crate::workers::poll_tasks,
        crate::workers::poll_tasks_from_queue,
        crate::workers::complete_task,
        crate::workers::upload_task_artifact,
        crate::workers::fail_task,
        crate::workers::heartbeat_task,
        crate::workers::list_tasks,
        crate::workers::task_stats,
        crate::workers::list_task_attempts,
        crate::workers::list_workers,
        crate::workers::list_handlers,
        crate::workers::enqueue_command,
        crate::workers::list_commands,
        crate::workers::ack_command,
        crate::workers::set_version_pin,
        crate::workers::list_version_pins,
        crate::workers::delete_version_pin,
        // Triggers
        crate::triggers::create_trigger,
        crate::triggers::list_triggers,
        crate::triggers::get_trigger,
        crate::triggers::delete_trigger,
        crate::triggers::fire_trigger,
        // Webhooks
        crate::webhooks::public_webhook,
        // Usage
        crate::usage::get_usage,
        // Streaming
        crate::streaming::stream_instance,
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
        // Framework-neutral continuity product
        crate::continuity::product_api::protocol_description,
        crate::continuity::product_api::validate_offer,
        crate::continuity::product_api::compile_policy,
        crate::continuity::product_api::validate_gateway,
        crate::continuity::product_api::verify_receipt,
        crate::continuity::product_api::certify,
        crate::continuity::product_api::render_badge,
        crate::continuity::product_api::list_profiles,
        crate::continuity::product_api::create_profile_offer,
        crate::continuity::product_api::validate_commercial_plan,
    ),
    components(schemas(
        // Findings & preflight
        orch8_types::finding::Finding,
        orch8_types::finding::FindingSeverity,
        orch8_types::finding::Confidence,
        orch8_types::finding::Evidence,
        orch8_types::finding::Remediation,
        orch8_types::finding::ResourceRef,
        orch8_types::preflight::PreflightReport,
        orch8_types::preflight::PreflightCheck,
        orch8_types::preflight::PreflightStatus,
        orch8_engine::dataflow::DataflowSeverity,
        orch8_engine::dataflow::DataflowFinding,
        orch8_engine::dataflow::DataflowReport,
        orch8_engine::dataflow::GeneratedDataflowTypes,
        crate::dataflow::DataflowResponse,
        orch8_types::diagnosis::InstanceDiagnosisReport,
        orch8_types::diagnosis::Diagnosis,
        orch8_types::diagnosis::DiagnosisCategory,
        orch8_types::diagnosis::DiagnosisHealth,
        orch8_types::diagnosis::RemediationAction,
        orch8_types::diagnosis::RemediationPreview,
        crate::diagnosis::ApplyRemediationRequest,
        crate::diagnosis::RemediationApplyEvidence,
        orch8_types::audit::ChangeCursor,
        crate::changes::ChangePage,
        orch8_types::api_key::ApiCapability,
        orch8_types::template_trace::ResolutionTrace,
        orch8_types::template_trace::ResolutionEntry,
        orch8_types::template_trace::ResolutionStatus,
        crate::inspect::InspectTemplateRequest,
        orch8_types::template_trace::DebugTemplateRequest,
        orch8_types::template_trace::DebugTemplateResponse,
        orch8_types::webhook_delivery::WebhookDeliveryAttempt,
        orch8_types::webhook_delivery::WebhookDeliverySummary,
        orch8_types::webhook_delivery::DeliveryErrorClass,
        crate::webhook_outbox::RedeliverPreview,
        orch8_types::dlq::DlqGroup,
        orch8_types::dlq::DlqGroupRetryRequest,
        orch8_types::dlq::DlqGroupRetryResponse,
        orch8_types::dlq::DlqRetryMode,
        orch8_types::failure::ErrorClass,
        orch8_types::failure::FailureEnvelope,
        orch8_types::failure::FailureFingerprint,
        orch8_types::release::WorkflowRelease,
        orch8_types::release::ReleaseState,
        orch8_types::release::ReleaseGate,
        orch8_types::release::GateMetric,
        orch8_types::release::GateEvaluation,
        orch8_types::release::GateVerdict,
        orch8_types::release::InFlightPolicy,
        orch8_types::release::ReleaseDecision,
        orch8_types::release::VariantStats,
        orch8_types::release::SemanticDiff,
        orch8_types::release::DiffEntry,
        orch8_types::release::DiffSeverity,
        crate::releases::CreateReleaseRequest,
        crate::releases::DiffRequest,
        orch8_types::event_correlation::EventEnvelope,
        orch8_types::event_correlation::EventStatus,
        orch8_types::event_correlation::EventWait,
        orch8_types::event_correlation::JoinMode,
        orch8_types::event_correlation::WaitStatus,
        orch8_types::event_correlation::IngestOutcome,
        crate::events::IngestEventRequest,
        crate::workbench::ExecutionWorkbenchView,
        crate::workbench::WorkbenchEvent,
        crate::workbench::BlockOutputSummary,
        crate::workbench::RunComparison,
        crate::workbench::ForkPreview,
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
        orch8_types::instance::Budget,
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
        orch8_types::sequence::SlaPolicy,
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
        orch8_types::cron::OverlapPolicy,
        orch8_types::webhook_outbox::WebhookOutboxEntry,
        crate::webhook_outbox::RedeliverResponse,
        orch8_types::queue_routing::QueueRoutingRule,
        crate::queue_routing::CreateRoutingRuleRequest,
        orch8_types::queue_dispatch::QueueDispatchConfig,
        orch8_types::queue_dispatch::DispatchMode,
        crate::queue_dispatch::SetDispatchRequest,
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
        // Timeline / fork (time-travel operations)
        crate::instances::TimelineResponse,
        crate::instances::TimelineInstance,
        crate::instances::TimelineEntry,
        crate::instances::TimelineStateTransition,
        crate::instances::ForkRequest,
        crate::instances::BatchAction,
        crate::instances::BatchActionRequest,
        crate::instances::BatchActionResponse,
        crate::instances::BulkFilter,
        crate::instances::ResumeFromRequest,
        crate::instances::InjectedSignal,
        crate::instances::ForkResponse,
        crate::instances::SaveCheckpointRequest,
        crate::instances::PruneCheckpointsRequest,
        crate::instances::InjectBlocksRequest,
        crate::sessions::CreateSessionRequest,
        crate::sessions::UpdateSessionDataRequest,
        crate::sessions::UpdateSessionStateRequest,
        crate::sequences::MigrateInstanceRequest,
        crate::workers::QueuePollRequest,
        crate::workers::WorkerInfo,
        crate::workers::HandlerCatalog,
        orch8_types::worker::WorkerRegistration,
        orch8_types::worker::WorkerCommand,
        orch8_types::worker::WorkerCommandKind,
        orch8_types::worker::WorkerVersionPin,
        orch8_types::step_log::StepLog,
        orch8_types::step_log::StepLogEntry,
        crate::workers::EnqueueCommandRequest,
        crate::workers::SetVersionPinRequest,
        crate::pools::CreatePoolRequest,
        crate::pools::AddResourceRequest,
        crate::pools::UpdateResourceRequest,
        // Credentials
        crate::credentials::CredentialResponse,
        orch8_types::credential::CredentialKind,
        crate::credentials::CreateCredentialRequest,
        crate::credentials::UpdateCredentialRequest,
        // Triggers
        orch8_types::trigger::TriggerDef,
        orch8_types::trigger::TriggerType,
        orch8_types::trigger::TriggerPollState,
        crate::triggers::CreateTriggerRequest,
        // Worker stats
        orch8_types::worker_filter::WorkerTaskStats,
        crate::continuity::product_api::ProtocolDescription,
        crate::continuity::product_api::ValidationResponse,
        crate::continuity::product_api::CompilePolicyRequest,
        crate::continuity::product_api::CertificationRequest,
        crate::continuity::product_api::ProfileOfferRequest,
        orch8_types::continuity_product::ProtocolVersion,
        orch8_types::continuity_product::PortableWorkOffer,
        orch8_types::continuity_product::CompiledPlacementPolicy,
        orch8_types::continuity_product::GatewayManifest,
        orch8_types::continuity_product::GatewayAdapter,
        orch8_types::continuity_product::ExecutionReceipt,
        orch8_types::continuity_product::ExecutionLocationReceipt,
        orch8_types::continuity_product::ConformanceCheck,
        orch8_types::continuity_product::ConformanceCheckResult,
        orch8_types::continuity_product::ContinuityScore,
        orch8_types::continuity_product::ConformanceCertificate,
        orch8_types::continuity_product::TrustBoundaryProfile,
        orch8_types::continuity_product::ProfileContract,
        orch8_types::continuity_product::RelayDeployment,
        orch8_types::continuity_product::CommercialContinuityPlan,
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
        (name = "triggers", description = "Trigger definitions that convert inbound events into instance creations"),
        (name = "webhooks", description = "Public, unauthenticated webhook ingestion (HMAC-protected via trigger secret)"),
        (name = "continuity-product", description = "Framework-neutral handoff protocol, profiles, receipts, conformance, and commercial deployment validation"),
    )
)]
pub struct ApiDoc;

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn continuity_router_paths_are_all_registered_in_openapi() {
        let source = include_str!("continuity.rs");
        let mut router_paths = BTreeSet::new();
        for route in source.split(".route(").skip(1) {
            let Some(start) = route.find('"') else {
                continue;
            };
            let tail = &route[start + 1..];
            let end = tail.find('"').expect("route path has a closing quote");
            router_paths.insert(&tail[..end]);
        }

        let documented_paths: BTreeSet<_> = CONTINUITY_OPERATIONS
            .iter()
            .map(|(path, _, _)| *path)
            .collect();
        assert_eq!(router_paths, documented_paths);

        let operation_ids: BTreeSet<_> = CONTINUITY_OPERATIONS
            .iter()
            .map(|(_, _, operation_id)| *operation_id)
            .collect();
        assert_eq!(operation_ids.len(), CONTINUITY_OPERATIONS.len());

        let document = ApiDoc::openapi();
        for (path, method, operation_id) in CONTINUITY_OPERATIONS {
            let operation = document
                .paths
                .get_path_operation(path, method.clone())
                .unwrap_or_else(|| panic!("missing continuity operation {operation_id} at {path}"));
            assert_eq!(operation.operation_id.as_deref(), Some(*operation_id));
        }
    }
}
