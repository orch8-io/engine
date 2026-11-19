use std::time::Duration;

use axum::Json;
use orch8_types::contract::{ContractSuite, SuiteReport};
use serde::Deserialize;
use serde_json::Value;
use utoipa::ToSchema;

use crate::{auth::OptionalTenant, error::ApiError, sub_tenants::SubTenantHeader};

#[derive(Deserialize, ToSchema)]
pub(crate) struct DraftContract {
    sequence: Value,
    #[schema(value_type = Object)]
    contract: ContractSuite,
}

#[utoipa::path(post, path = "/sequences/test-draft", tag = "sequences",
    request_body = DraftContract,
    responses(
        (status = 200, description = "Effect-free contract report from an isolated draft", body = SuiteReport),
        (status = 400, description = "Invalid draft or contract"),
    )
)]
pub(crate) async fn run(
    tenant_ctx: OptionalTenant,
    sub_header: SubTenantHeader,
    Json(mut request): Json<DraftContract>,
) -> Result<Json<SuiteReport>, ApiError> {
    let (mut sequence, _) = super::decode_draft_sequence(&request.sequence, true)?;
    sequence.tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &sequence.tenant_id)?;
    sequence.sub_tenant = sub_header.for_create(sequence.sub_tenant.as_deref())?;
    sequence
        .validate()
        .map_err(|error| ApiError::validation(error.catalog_key(), error.to_string()))?;
    if request.contract.cases.len() > 4 {
        return Err(ApiError::InvalidArgument(
            "draft tests support up to four cases".into(),
        ));
    }
    for case in &mut request.contract.cases {
        case.max_ticks = Some(case.max_ticks.unwrap_or(250).min(250));
        case.max_logical_duration_ms = Some(
            case.max_logical_duration_ms
                .unwrap_or(604_800_000)
                .min(604_800_000),
        );
    }
    let options = orch8::contract::RunOptions::default();
    let report = tokio::time::timeout(
        Duration::from_secs(5),
        orch8::contract::run_suite(&sequence, &request.contract, &options),
    )
    .await
    .map_err(|_| ApiError::Unavailable("draft contract exceeded its time budget".into()))?
    .map_err(|error| match error {
        orch8::Error::Config(message) => ApiError::InvalidArgument(message),
        error => {
            tracing::warn!(error = %error, "draft contract could not run");
            ApiError::InvalidArgument(
                "draft contract could not run with the supplied inputs".into(),
            )
        }
    })?;
    Ok(Json(report))
}
