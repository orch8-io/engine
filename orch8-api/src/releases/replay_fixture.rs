use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderValue, header::CACHE_CONTROL};
use axum::response::IntoResponse;
use axum::{Extension, Json};
use orch8_types::contract::{
    CONTRACT_SCHEMA_VERSION, ContractCase, ContractSuite, Expectations, ExpectedTerminalState,
    MockDef, MockPolicy, PathExpectation, UnmockedHandlerPolicy,
};
use orch8_types::ids::InstanceId;
use orch8_types::redaction::RedactionPolicy;
use orch8_types::sequence::SequenceDefinition;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    AppState,
    auth::{OptionalTenant, PrincipalContext, principal_is_operator},
    error::ApiError,
    sub_tenants::SubTenantHeader,
};

const MAX_OUTPUTS: u32 = 1_000;
const MAX_SELECTED_FIELDS: usize = 128;
const MAX_FIXTURE_BYTES: usize = 512 * 1024;

#[derive(Default, Deserialize, ToSchema)]
pub(crate) struct ReplayFixtureRequest {
    /// Exact top-level context input keys to preserve. Defaults to none.
    #[serde(default)]
    input_keys: BTreeSet<String>,
    /// Exact top-level output keys to preserve for each named mock block.
    #[serde(default)]
    output_keys: HashMap<String, BTreeSet<String>>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ReplayFixture {
    sequence: SequenceDefinition,
    contract: ContractSuite,
    /// The exported files produced the saved candidate outcome and executed-block set.
    reproduces_divergence: bool,
}

#[derive(Deserialize)]
struct Divergence {
    instance_id: Uuid,
    recorded_state: ExpectedTerminalState,
    replayed_state: String,
    blocks_added: BTreeSet<String>,
    blocks_removed: BTreeSet<String>,
}

#[utoipa::path(post, path = "/releases/{id}/divergences/{instance_id}/fixture", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id"),
           ("instance_id" = Uuid, Path, description = "Recorded divergent execution")),
    request_body = ReplayFixtureRequest,
    responses(
        (status = 200, description = "Redacted offline regression fixture", body = ReplayFixture),
        (status = 400, description = "Selection or fixture exceeds export limits"),
        (status = 403, description = "Operator capability required"),
        (status = 404, description = "Divergence not found within the caller's scope"),
        (status = 409, description = "Recorded evidence changed during export"),
    )
)]
pub(crate) async fn export_replay_fixture(
    State(state): State<AppState>,
    tenant: OptionalTenant,
    sub_tenant: SubTenantHeader,
    principal: Option<Extension<PrincipalContext>>,
    Path((release_id, instance_id)): Path<(Uuid, Uuid)>,
    Json(request): Json<ReplayFixtureRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if !principal_is_operator(principal.as_ref().map(|principal| &principal.0)) {
        return Err(ApiError::Forbidden(
            "Replay fixture export requires Operator capability".into(),
        ));
    }
    let output_count = request
        .output_keys
        .values()
        .map(BTreeSet::len)
        .sum::<usize>();
    if request.output_keys.len() > MAX_SELECTED_FIELDS
        || request.input_keys.len() + output_count > MAX_SELECTED_FIELDS
    {
        return Err(ApiError::InvalidArgument(
            "Too many fixture fields selected".into(),
        ));
    }
    let not_found = || ApiError::NotFound(format!("replay divergence {instance_id}"));
    let release = super::load_release(&state, &tenant, release_id).await?;
    let divergence = release
        .validation_summary
        .as_ref()
        .and_then(|summary| summary["divergences"].as_array())
        .and_then(|samples| {
            samples.iter().find_map(|sample| {
                let divergence: Divergence = serde_json::from_value(sample.clone()).ok()?;
                (divergence.instance_id == instance_id).then_some(divergence)
            })
        })
        .ok_or_else(not_found)?;
    let instance_id = InstanceId::from_uuid(instance_id);
    let instance = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?
        .filter(|instance| {
            instance.tenant_id == release.tenant_id
                && instance.sequence_id == release.baseline_sequence_id
        })
        .ok_or_else(not_found)?;
    sub_tenant.enforce_access(instance.sub_tenant.as_deref(), "replay divergence")?;
    let candidate = super::fetch_sequence(
        &state,
        release.candidate_sequence_id.into_uuid(),
        &release.tenant_id,
    )
    .await?;
    sub_tenant.enforce_access(candidate.sub_tenant.as_deref(), "candidate sequence")?;

    let policy = RedactionPolicy::default();
    let definition = serde_json::to_value(&candidate)
        .map_err(|_| ApiError::Internal("Could not serialize candidate sequence".into()))?;
    let candidate: SequenceDefinition = serde_json::from_value(policy.redacted(&definition))
        .map_err(|_| {
            ApiError::InvalidArgument("Candidate cannot be exported after redaction".into())
        })?;
    let evidence =
        load_recorded_outputs(&state, instance_id, &request.output_keys, &policy).await?;
    let input = select_fields(&instance.context.data, &request.input_keys, &policy);
    let replayed_state = divergence.replayed_state.clone();
    let (mut fixture, expected_blocks) = build_fixture(candidate, input, evidence, divergence);
    fixture.reproduces_divergence =
        verify_fixture(&fixture, &replayed_state, &expected_blocks).await?;
    Ok((
        [(CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(fixture),
    ))
}

struct RecordedEvidence {
    blocks: BTreeSet<String>,
    outputs: HashMap<String, Value>,
}

async fn load_recorded_outputs(
    state: &AppState,
    instance_id: InstanceId,
    output_keys: &HashMap<String, BTreeSet<String>>,
    policy: &RedactionPolicy,
) -> Result<RecordedEvidence, ApiError> {
    let history = state
        .storage
        .get_replay_output_keys(instance_id, MAX_OUTPUTS)
        .await
        .map_err(|error| ApiError::from_storage(error, "block output history"))?
        .ok_or_else(|| {
            ApiError::InvalidArgument("Execution exceeds the fixture output limit".into())
        })?;

    let mut recorded_blocks = BTreeSet::new();
    let mut outputs = HashMap::new();
    let mut retained_bytes = 0;
    for row in history {
        let block = row.block_id;
        recorded_blocks.insert(block.to_string());
        let Some(keys) = output_keys.get(block.as_str()) else {
            continue;
        };
        let selected = if row.output_ref.is_none() {
            let row = state
                .storage
                .get_inline_output_by_id(instance_id, row.id)
                .await
                .map_err(|error| ApiError::from_storage(error, "block output"))?
                .ok_or_else(|| {
                    ApiError::Conflict("Replay evidence changed during export".into())
                })?;
            select_fields(&row.output, keys, policy)
        } else {
            json!({})
        };
        retained_bytes += serde_json::to_vec(&selected)
            .map_err(|_| ApiError::Internal("Could not serialize mock output".into()))?
            .len();
        if retained_bytes > MAX_FIXTURE_BYTES {
            return Err(ApiError::InvalidArgument(
                "Selected outputs exceed the fixture size limit".into(),
            ));
        }
        outputs.insert(block.to_string(), selected);
    }
    Ok(RecordedEvidence {
        blocks: recorded_blocks,
        outputs,
    })
}

fn build_fixture(
    candidate: SequenceDefinition,
    input: Value,
    evidence: RecordedEvidence,
    divergence: Divergence,
) -> (ReplayFixture, BTreeSet<String>) {
    let RecordedEvidence {
        blocks: recorded_blocks,
        mut outputs,
    } = evidence;
    let expected_candidate: BTreeSet<_> = recorded_blocks
        .difference(&divergence.blocks_removed)
        .chain(&divergence.blocks_added)
        .cloned()
        .collect();
    let contract = ContractSuite {
        schema_version: CONTRACT_SCHEMA_VERSION,
        sequence_name: Some(candidate.name.clone()),
        sequence_version: Some(i64::from(candidate.version)),
        unmocked_handlers: UnmockedHandlerPolicy::EmptySuccess,
        cases: vec![ContractCase {
            name: "replay divergence regression".into(),
            description: Some("Restores the recorded workflow state and path.".into()),
            input,
            initial_outputs: BTreeMap::new(),
            config: None,
            mocks: super::candidate_step_blocks(&candidate)
                .into_iter()
                .map(|block| MockDef {
                    definition: None,
                    handler: None,
                    policy: MockPolicy::Success {
                        output: outputs.remove(&block).unwrap_or_else(|| json!({})),
                    },
                    block: Some(block),
                })
                .collect(),
            signals: vec![],
            expect: Expectations {
                terminal_state: Some(divergence.recorded_state),
                path: (!recorded_blocks.is_empty()).then(|| PathExpectation {
                    traversed: recorded_blocks.into_iter().collect(),
                    ordered: false,
                }),
                skipped_blocks: divergence.blocks_added.into_iter().collect(),
                ..Default::default()
            },
            max_logical_duration_ms: None,
            max_ticks: Some(5_000),
        }],
    };

    let fixture = ReplayFixture {
        sequence: candidate,
        contract,
        reproduces_divergence: false,
    };
    (fixture, expected_candidate)
}

async fn verify_fixture(
    fixture: &ReplayFixture,
    replayed_state: &str,
    expected_blocks: &BTreeSet<String>,
) -> Result<bool, ApiError> {
    if serde_json::to_vec(fixture)
        .map_err(|_| ApiError::Internal("Could not serialize replay fixture".into()))?
        .len()
        > MAX_FIXTURE_BYTES
    {
        return Err(ApiError::InvalidArgument(
            "Replay fixture exceeds the export size limit".into(),
        ));
    }
    let options = orch8::contract::RunOptions::default();
    match tokio::time::timeout(
        Duration::from_secs(5),
        orch8::contract::run_suite(&fixture.sequence, &fixture.contract, &options),
    )
    .await
    {
        Ok(Ok(report)) => {
            let case = &report.cases[0];
            Ok(case.final_state == replayed_state
                && case
                    .executed_blocks
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    == *expected_blocks)
        }
        Ok(Err(_)) => Err(ApiError::InvalidArgument(
            "Redacted replay files could not run as an offline contract".into(),
        )),
        Err(_) => Err(ApiError::InvalidArgument(
            "Redacted replay reached the export verification time limit".into(),
        )),
    }
}

fn select_fields(value: &Value, keys: &BTreeSet<String>, policy: &RedactionPolicy) -> Value {
    let mut projected = Value::Object(
        keys.iter()
            .filter(|key| !policy.is_sensitive_key(key))
            .filter_map(|key| {
                value
                    .as_object()?
                    .get(key)
                    .map(|value| (key.clone(), value.clone()))
            })
            .collect(),
    );
    policy.redact_value(&mut projected);
    projected
}
