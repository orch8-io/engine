use std::collections::{BTreeMap, BTreeSet, HashMap};

use axum::Json;
use axum::extract::{Path, Query, State};
use chrono::{DateTime, Utc};
use orch8_storage::worker_inventory::{RouteObservation, WorkerRoute};
use orch8_types::continuity::{CapsuleRequirements, RuntimeCapabilities};
use orch8_types::ids::SequenceId;
use orch8_types::placement::{
    self, Placement, PlacementPolicies, PlacementPreference, PlacementQueueEvidence,
    PlacementTarget,
};
use orch8_types::queue_routing::QueueRoutingRule;
use orch8_types::sequence::{SequenceDefinition, StepDef, visit_steps};
use orch8_types::worker::{
    MAX_WORKER_VERSION_BYTES, WorkerVersionRollout, claim_allowed, version_admits,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{AppState, auth::OptionalTenant, error::ApiError};

const ROUTE_OBSERVATION_LIMIT: u32 = 1_000;
const WORKER_SAMPLE_LIMIT: usize = 50;
const WORKER_LIVENESS_SECS: i64 = 60;

#[derive(Debug, Default, Deserialize)]
pub(crate) struct PlanQuery {
    /// Comma-separated instance tags for evaluating tag-selective policies.
    tags: Option<String>,
    recipe: Option<String>,
    worker_version: Option<String>,
    staged_pins: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct PlacementPlan {
    sequence_id: Uuid,
    ready: bool,
    inventory_complete: bool,
    steps: Vec<StepPlan>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct StepPlan {
    block_id: String,
    handler: String,
    tree: &'static str,
    route: &'static str,
    matched_policies: Vec<String>,
    eligible_workers: Vec<String>,
    preferred_workers: Vec<String>,
    excluded_workers: Vec<WorkerExclusion>,
    worker_inventory: Option<RouteInventory>,
    minimum_worker_version: Option<String>,
    staged_worker_version: Option<String>,
    queue_name: Option<String>,
    pending_tasks: Option<u64>,
    oldest_pending_age_secs: Option<i64>,
    /// Sum of unused demand from the last successful HTTP polls, when known
    /// for all eligible workers on this route.
    available_poll_slots: Option<u64>,
    /// Bytes of the compact JSON handler params in the saved definition.
    estimated_params_bytes: Option<usize>,
    canary_version: Option<String>,
    next_action: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct RouteInventory {
    completeness: InventoryCompleteness,
    /// Distinct eligible worker IDs observed. This is a lower bound when the limit is reached.
    eligible_count: usize,
    preferred_count: usize,
    /// Workers excluded by every evaluated observation. Additional observations may change this count.
    excluded_count: usize,
    observed_rows: usize,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InventoryCompleteness {
    Complete,
    ObservationLimitReached,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct WorkerExclusion {
    worker_id: String,
    reasons: Vec<&'static str>,
}

#[utoipa::path(get, path = "/placement/plan/{id}", tag = "placement",
    params(("id" = Uuid, Path, description = "Immutable sequence id"),
        ("tags" = Option<String>, Query, description = "Comma-separated instance tags"),
        ("recipe" = Option<String>, Query, description = "Preview a named recipe as an additional policy"),
        ("worker_version" = Option<String>, Query, description = "Only include workers advertising this exact version"),
        ("staged_pins" = Option<bool>, Query, description = "Preview staged minimum worker versions where present")),
    responses((status = 200, description = "Read-only placement and fleet readiness preview", body = PlacementPlan))
)]
#[allow(clippy::too_many_lines)] // Check scope and build one consistent fleet view before traversing steps.
pub(crate) async fn get_plan(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path(id): Path<Uuid>,
    Query(query): Query<PlanQuery>,
) -> Result<Json<PlacementPlan>, ApiError> {
    let sequence = state
        .storage
        .get_sequence(SequenceId::from_uuid(id))
        .await
        .map_err(|error| ApiError::from_storage(error, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {id}")))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &sequence.tenant_id, "sequence")?;
    if query.tags.as_ref().is_some_and(|tags| tags.len() > 4096) {
        return Err(ApiError::InvalidArgument("tags exceed 4096 bytes".into()));
    }
    let tags: Vec<&str> = query
        .tags
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .collect();
    if query
        .worker_version
        .as_ref()
        .is_some_and(|version| version.is_empty() || version.len() > MAX_WORKER_VERSION_BYTES)
    {
        return Err(ApiError::InvalidArgument(format!(
            "worker_version must be 1 to {MAX_WORKER_VERSION_BYTES} bytes"
        )));
    }
    let now = Utc::now();
    let (mut policies, rollouts, shared_rollouts, queue_rules, queue_evidence) = tokio::try_join!(
        state.storage.get_placement_policies(&sequence.tenant_id),
        state
            .storage
            .list_worker_version_rollouts(Some(sequence.tenant_id.as_str())),
        state.storage.list_worker_version_rollouts(Some("")),
        state
            .storage
            .list_queue_routing_rules(Some(&sequence.tenant_id), None),
        state.storage.placement_queue_evidence(&sequence.tenant_id),
    )
    .map_err(|error| ApiError::from_storage(error, "placement inventory"))?;
    if let Some(id) = query.recipe {
        let recipe = recipe_catalog()
            .into_iter()
            .find(|recipe| recipe.id == id)
            .ok_or_else(|| ApiError::InvalidArgument(format!("unknown placement recipe {id}")))?;
        policies.items.push(placement::PlacementPolicy {
            name: format!("preview recipe {id}"),
            matcher: placement::PolicyMatch::default(),
            require: placement::PolicyRequirement {
                region: recipe.placement.region,
                labels: recipe.placement.labels,
                residency: recipe.placement.residency,
            },
            prefer: None,
        });
    }
    let fleet = Fleet {
        rollouts: index_rollouts(&rollouts),
        shared_rollouts: index_rollouts(&shared_rollouts),
        staged_pins: query.staged_pins.unwrap_or(false),
        queue_evidence: queue_evidence
            .iter()
            .map(|row| ((row.handler_name.as_str(), row.queue_name.as_deref()), row))
            .collect(),
        queue_rules: &queue_rules,
        builtin_handlers: &state.builtin_handlers,
        canary_version: query.worker_version.as_deref(),
        now,
    };
    let mut planned = Vec::new();
    for (tree, blocks) in [
        ("main", sequence.blocks.as_slice()),
        (
            "on_failure",
            sequence.on_failure.as_deref().unwrap_or_default(),
        ),
        (
            "on_cancel",
            sequence.on_cancel.as_deref().unwrap_or_default(),
        ),
    ] {
        for block in blocks {
            visit_steps(block, &mut |step| {
                planned.push(fleet.step(&sequence, &policies, &tags, tree, step));
            });
        }
    }
    let mut steps = Vec::with_capacity(planned.len());
    let mut work_by_route = HashMap::<_, Vec<_>>::new();
    for (plan, step_requirements) in planned {
        if let Some(requirements) = step_requirements {
            work_by_route
                .entry((plan.handler.clone(), plan.queue_name.clone()))
                .or_default()
                .push((steps.len(), requirements));
        }
        steps.push(plan);
    }

    let mut inventory_complete = true;
    for ((handler, queue), work) in work_by_route {
        let observations = state
            .storage
            .worker_route_observations(&WorkerRoute {
                tenant: &sequence.tenant_id,
                handler: &handler,
                queue: queue.as_deref(),
                seen_since: now - chrono::Duration::seconds(WORKER_LIVENESS_SECS),
                now,
                limit: ROUTE_OBSERVATION_LIMIT + 1,
            })
            .await
            .map_err(|error| ApiError::from_storage(error, "worker route observations"))?;
        inventory_complete &= observations.len() <= ROUTE_OBSERVATION_LIMIT as usize;
        for (index, requirements) in work {
            fleet.assess(&mut steps[index], &requirements, &observations);
        }
    }
    Ok(Json(PlacementPlan {
        sequence_id: id,
        ready: inventory_complete && steps.iter().all(|step| step.next_action.is_none()),
        inventory_complete,
        steps,
    }))
}

fn index_rollouts(rollouts: &[WorkerVersionRollout]) -> HashMap<&str, &WorkerVersionRollout> {
    rollouts
        .iter()
        .map(|rollout| (rollout.handler_name.as_str(), rollout))
        .collect()
}

struct StepRequirements {
    capabilities: CapsuleRequirements,
    carries_credentials: bool,
}

struct Fleet<'a> {
    rollouts: HashMap<&'a str, &'a WorkerVersionRollout>,
    shared_rollouts: HashMap<&'a str, &'a WorkerVersionRollout>,
    staged_pins: bool,
    queue_evidence: HashMap<(&'a str, Option<&'a str>), &'a PlacementQueueEvidence>,
    queue_rules: &'a [QueueRoutingRule],
    builtin_handlers: &'a [String],
    canary_version: Option<&'a str>,
    now: DateTime<Utc>,
}

impl Fleet<'_> {
    #[allow(clippy::too_many_lines)] // Classify the step and explain the claim decision from one fleet snapshot.
    fn step(
        &self,
        sequence: &SequenceDefinition,
        policies: &PlacementPolicies,
        tags: &[&str],
        tree: &'static str,
        step: &StepDef,
    ) -> (StepPlan, Option<StepRequirements>) {
        let mut plan = StepPlan {
            block_id: step.id.to_string(),
            handler: step.handler.clone(),
            tree,
            route: "engine",
            matched_policies: Vec::new(),
            eligible_workers: Vec::new(),
            preferred_workers: Vec::new(),
            excluded_workers: Vec::new(),
            worker_inventory: None,
            minimum_worker_version: None,
            staged_worker_version: None,
            queue_name: None,
            pending_tasks: None,
            oldest_pending_age_secs: None,
            available_poll_slots: None,
            estimated_params_bytes: None,
            canary_version: None,
            next_action: None,
        };
        if orch8_types::sequence::is_engine_only_builtin(&step.handler) {
            return (plan, None);
        }
        if [
            orch8_engine::handlers::activepieces::is_ap_handler,
            orch8_engine::handlers::grpc_plugin::is_grpc_handler,
            orch8_engine::handlers::wasm_plugin::is_wasm_handler,
        ]
        .iter()
        .any(|detect| detect(&step.handler))
        {
            plan.route = "plugin";
            return (plan, None);
        }
        let target = PlacementTarget {
            sequence_name: &sequence.name,
            handler: &step.handler,
            tags,
        };
        let requirements = resolve_requirements(sequence, policies, step, target, &mut plan);
        let (requirements, hard_placement) = match requirements {
            Ok(resolved) => resolved,
            Err(error) => {
                plan.route = "invalid";
                plan.next_action = Some(error);
                return (plan, None);
            }
        };
        if !hard_placement
            && !requirements.is_remote_placement()
            && self.builtin_handlers.contains(&step.handler)
        {
            return (plan, None);
        }

        plan.route = "worker";
        if let Some(rollout) = self.rollouts.get(step.handler.as_str()) {
            plan.staged_worker_version
                .clone_from(&rollout.staged_min_version);
            plan.minimum_worker_version = self.minimum(rollout).map(str::to_owned);
        }
        plan.canary_version = self.canary_version.map(str::to_owned);
        // The queue registry returns rules in the same priority order used
        // when the engine enqueues tasks.
        let declared_queue = step.queue_name.as_deref();
        let effective_queue = self
            .queue_rules
            .iter()
            .find(|rule| rule.handler_name == step.handler && rule.matches(declared_queue))
            .map(|rule| rule.queue_override.as_str())
            .or(declared_queue);
        let route = (step.handler.as_str(), effective_queue);
        plan.queue_name = effective_queue.map(str::to_owned);
        let evidence = self.queue_evidence.get(&route);
        plan.pending_tasks = Some(evidence.map_or(0, |evidence| evidence.pending_tasks));
        plan.oldest_pending_age_secs =
            evidence.map(|evidence| (self.now - evidence.oldest_pending_at).num_seconds().max(0));
        plan.estimated_params_bytes = estimate_params_bytes(&step.params);
        let carries_credentials = orch8_types::worker::contains_credential_reference(&step.params);
        (
            plan,
            Some(StepRequirements {
                capabilities: requirements,
                carries_credentials,
            }),
        )
    }

    fn minimum<'a>(&self, rollout: &'a WorkerVersionRollout) -> Option<&'a str> {
        if self.staged_pins {
            rollout
                .staged_min_version
                .as_deref()
                .or(rollout.active_min_version.as_deref())
        } else {
            rollout.active_min_version.as_deref()
        }
    }

    fn assess(
        &self,
        plan: &mut StepPlan,
        requirements: &StepRequirements,
        observations: &[RouteObservation],
    ) {
        let shared_minimum = self
            .shared_rollouts
            .get(plan.handler.as_str())
            .and_then(|rollout| self.minimum(rollout));
        let count = observations.len().min(ROUTE_OBSERVATION_LIMIT as usize);
        let complete = observations.len() == count;
        let mut eligible = BTreeMap::<&str, Option<u64>>::new();
        let mut preferred = BTreeSet::new();
        let mut excluded = BTreeMap::new();
        let mut any_compatible_version = false;
        for observation in &observations[..count] {
            let worker = observation.worker_id.as_str();
            let version = observation.version.as_deref();
            let minimum = if observation.shared {
                shared_minimum
            } else {
                plan.minimum_worker_version.as_deref()
            };
            if self
                .canary_version
                .is_some_and(|canary| version != Some(canary))
                || !version_admits(version, minimum)
            {
                excluded
                    .entry(worker)
                    .or_insert_with(|| vec!["VERSION_INCOMPATIBLE"]);
                continue;
            }
            any_compatible_version = true;
            match assess_observation(&plan.handler, observation, requirements, self.now) {
                Ok(is_preferred) => {
                    let slots = observation.available_poll_slots.map(u64::from);
                    eligible
                        .entry(worker)
                        .and_modify(|value| {
                            if slots.is_none() {
                                *value = None;
                            }
                        })
                        .or_insert(slots);
                    if is_preferred {
                        preferred.insert(worker);
                    }
                }
                Err(reasons) => {
                    excluded.entry(worker).or_insert(reasons);
                }
            }
        }
        excluded.retain(|worker, _| !eligible.contains_key(worker));
        plan.eligible_workers = eligible
            .keys()
            .take(WORKER_SAMPLE_LIMIT)
            .map(|worker| (*worker).to_owned())
            .collect();
        plan.preferred_workers = preferred
            .iter()
            .take(WORKER_SAMPLE_LIMIT)
            .map(|worker| (*worker).to_owned())
            .collect();
        plan.excluded_workers = excluded
            .iter()
            .take(WORKER_SAMPLE_LIMIT)
            .map(|(worker, reasons)| WorkerExclusion {
                worker_id: (*worker).to_owned(),
                reasons: reasons.clone(),
            })
            .collect();
        plan.available_poll_slots = complete.then(|| eligible.values().copied().sum()).flatten();
        plan.worker_inventory = Some(RouteInventory {
            completeness: if complete {
                InventoryCompleteness::Complete
            } else {
                InventoryCompleteness::ObservationLimitReached
            },
            eligible_count: eligible.len(),
            preferred_count: preferred.len(),
            excluded_count: excluded.len(),
            observed_rows: count,
        });
        if eligible.is_empty() {
            plan.next_action = Some(if !complete {
                "Worker observations exceeded the route inventory limit. Readiness is inconclusive."
                    .to_owned()
            } else if observations.is_empty() {
                format!(
                    "Start a live worker polling {} on the {} queue.",
                    plan.handler,
                    plan.queue_name.as_deref().unwrap_or("default")
                )
            } else if !any_compatible_version && self.canary_version.is_some() {
                "Start a live worker reporting the requested canary version and satisfying the version pin.".to_owned()
            } else if !any_compatible_version {
                "Deploy a live worker whose reported version satisfies its active version pin."
                    .to_owned()
            } else {
                "Register a worker that satisfies the required capabilities, trust, and placement policy.".to_owned()
            });
        }
    }
}

fn assess_observation(
    handler: &str,
    observation: &RouteObservation,
    requirements: &StepRequirements,
    now: DateTime<Utc>,
) -> Result<bool, Vec<&'static str>> {
    if observation.draining {
        return Err(vec!["WORKER_DRAINING"]);
    }
    if observation.capability_unavailable {
        return Err(vec!["CAPABILITY_ADVERTISEMENT_MISSING"]);
    }
    let capabilities = &requirements.capabilities;
    let Some(runtime) = observation.runtime.as_ref() else {
        return if orch8_types::worker::legacy_claim_allowed(
            capabilities,
            &observation.worker_id,
            now,
        ) && handler != "orch8.delegation"
        {
            Ok(capabilities.prefer.is_some())
        } else {
            Err(vec!["CAPABILITY_ADVERTISEMENT_MISSING"])
        };
    };
    if !runtime
        .handlers
        .iter()
        .any(|candidate| candidate == handler)
    {
        return Err(vec!["HANDLERS_MISSING"]);
    }
    if !claim_allowed(capabilities, requirements.carries_credentials, runtime, now) {
        return Err(exclusion_reasons(
            capabilities,
            requirements.carries_credentials,
            runtime,
            now,
        ));
    }
    Ok(capabilities.prefer.is_some())
}

fn estimate_params_bytes(params: &serde_json::Value) -> Option<usize> {
    let (_, handler_params) =
        orch8_types::worker::take_runtime_requirements(params.clone()).ok()?;
    serde_json::to_vec(&handler_params)
        .ok()
        .map(|json| json.len())
}

fn exclusion_reasons(
    requirements: &CapsuleRequirements,
    carries_credentials: bool,
    runtime: &RuntimeCapabilities,
    now: DateTime<Utc>,
) -> Vec<&'static str> {
    use orch8_engine::continuity::{CompatibilityStatus, assess_compatibility};
    use orch8_types::continuity::{PolicyOutcome, RuntimeKind};
    let mut reasons: Vec<_> = assess_compatibility(requirements, runtime, now)
        .into_iter()
        .filter(|finding| finding.status != CompatibilityStatus::Pass)
        .map(|finding| finding.code)
        .collect();
    if !placement::placement_facts_satisfied(requirements, runtime) {
        reasons.push("PLACEMENT_FACTS_MISSING");
    }
    if carries_credentials && runtime.kind == RuntimeKind::Browser {
        reasons.push("BROWSER_CREDENTIALS_FORBIDDEN");
    }
    if requirements.has_locality_policy()
        && orch8_types::locality::evaluate_locality(
            requirements.policy.as_ref(),
            requirements.classification.unwrap_or_default(),
            runtime,
        )
        .outcome
            != PolicyOutcome::Allow
    {
        reasons.push("LOCALITY_NOT_ALLOWED");
    }
    if reasons.is_empty() {
        reasons.push("CLAIM_REQUIREMENTS_UNSATISFIED");
    }
    reasons
}

fn resolve_requirements(
    sequence: &SequenceDefinition,
    policies: &PlacementPolicies,
    step: &StepDef,
    target: PlacementTarget<'_>,
    plan: &mut StepPlan,
) -> Result<(CapsuleRequirements, bool), String> {
    let resolved = placement::resolve(
        step.placement.as_ref(),
        sequence.placement.as_ref(),
        &policies.items,
        &target,
    )?;
    if !resolved.is_empty() && !step.params.is_null() && !step.params.is_object() {
        return Err("placement requires object step params (got a non-object value)".to_owned());
    }
    let mut requirements = orch8_types::worker::peek_runtime_requirements(&step.params)?;
    placement::apply_hard_constraints(&resolved, &mut requirements)?;
    if resolved.has_hard_constraints() {
        for id in orch8_types::worker::credential_reference_ids(&step.params) {
            if !requirements.credentials.contains(&id) {
                requirements.credentials.push(id);
            }
        }
    }
    orch8_engine::placement::validate_requirements(&requirements)
        .map_err(|error| error.to_string())?;
    if let Some(policy) = &requirements.policy {
        orch8_engine::placement::validate_policy(policy).map_err(|error| error.to_string())?;
    }
    let hard = resolved.has_hard_constraints();
    if !resolved.prefer_labels.is_empty() {
        requirements.prefer = Some(PlacementPreference {
            worker_id: None,
            labels: resolved.prefer_labels,
            until_ms: i64::MAX,
        });
    }
    plan.matched_policies = resolved.policies;
    Ok((requirements, hard))
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct PlacementRecipe {
    id: &'static str,
    label: &'static str,
    placement: Placement,
}

#[utoipa::path(get, path = "/placement/recipes", tag = "placement",
    responses((status = 200, description = "Placement recipes for preview", body = Vec<PlacementRecipe>))
)]
pub(crate) async fn recipes() -> Json<Vec<PlacementRecipe>> {
    Json(recipe_catalog())
}

fn recipe_catalog() -> Vec<PlacementRecipe> {
    vec![
        PlacementRecipe {
            id: "private_network",
            label: "Private network",
            placement: Placement {
                labels: [("site".into(), "private".into())].into(),
                ..Default::default()
            },
        },
        PlacementRecipe {
            id: "gpu",
            label: "GPU worker",
            placement: Placement {
                labels: [("gpu".into(), "true".into())].into(),
                ..Default::default()
            },
        },
        PlacementRecipe {
            id: "eu_residency",
            label: "EU residency",
            placement: Placement {
                residency: Some("eu".into()),
                ..Default::default()
            },
        },
    ]
}
