use axum::Extension;
use axum::extract::{Json, Path, State};
use chrono::{DateTime, Utc};
use orch8_types::config::SecretString;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use utoipa::ToSchema;

use crate::{
    AppState,
    auth::{OptionalAdmin, OptionalTenant, PrincipalContext, principal_is_operator},
    error::ApiError,
};

use super::{MAX_TRIGGER_SECRET_BYTES, validate_trigger_config};

#[derive(Deserialize, ToSchema)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum TriggerSecretChange {
    Replace { value: String },
    Clear,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct TriggerSettingsRequest {
    expected_created_at: DateTime<Utc>,
    expected_updated_at: DateTime<Utc>,
    enabled: Option<bool>,
    config_patch: Option<Value>,
    secret_change: Option<TriggerSecretChange>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct TriggerSettingsReceipt {
    updated_at: DateTime<Utc>,
}

fn merge_config(config: &mut Value, patch: Map<String, Value>) {
    if !config.is_object() {
        *config = Value::Object(Map::new());
    }
    let Value::Object(config) = config else {
        return;
    };
    for (key, value) in patch {
        match value {
            Value::Null => {
                config.remove(&key);
            }
            Value::Object(patch) => {
                merge_config(config.entry(key).or_insert(Value::Null), patch);
            }
            value => {
                config.insert(key, value);
            }
        }
    }
}

#[utoipa::path(patch, path = "/triggers/{slug}/settings", tag = "triggers",
    params(("slug" = String, Path, description = "Exact trigger slug")),
    request_body = TriggerSettingsRequest,
    responses(
        (status = 200, description = "Stored trigger settings verified", body = TriggerSettingsReceipt),
        (status = 400, description = "Invalid trigger settings"),
        (status = 403, description = "Operator capability required"),
        (status = 404, description = "Trigger not found in the selected tenant"),
        (status = 409, description = "Trigger was edited or replaced"),
    )
)]
pub(crate) async fn update_trigger_settings(
    State(state): State<AppState>,
    tenant: OptionalTenant,
    admin: OptionalAdmin,
    principal: Option<Extension<PrincipalContext>>,
    Path(slug): Path<String>,
    Json(request): Json<TriggerSettingsRequest>,
) -> Result<Json<TriggerSettingsReceipt>, ApiError> {
    if admin.is_none() && !principal_is_operator(principal.as_ref().map(|principal| &principal.0)) {
        return Err(ApiError::Forbidden(
            "trigger settings require Operator access".into(),
        ));
    }
    let Some(Extension(tenant)) = tenant else {
        return Err(ApiError::InvalidArgument(
            "trigger settings require a tenant".into(),
        ));
    };
    let stale = || ApiError::Conflict("trigger changed; inspect it again before editing".into());
    let mut trigger = state
        .storage
        .get_trigger(Some(&tenant.tenant_id), &slug)
        .await
        .map_err(|error| ApiError::from_storage(error, "trigger"))?
        .filter(|trigger| trigger.tenant_id == tenant.tenant_id && trigger.slug == slug)
        .ok_or_else(|| ApiError::NotFound("trigger".into()))?;
    if trigger.created_at != request.expected_created_at
        || trigger.updated_at != request.expected_updated_at
    {
        return Err(stale());
    }
    if request.enabled.is_none()
        && request.config_patch.is_none()
        && request.secret_change.is_none()
    {
        return Err(ApiError::InvalidArgument(
            "choose a trigger setting to change".into(),
        ));
    }
    let validate = request.config_patch.is_some()
        || request.secret_change.is_some()
        || request.enabled == Some(true);
    if let Some(enabled) = request.enabled {
        trigger.enabled = enabled;
    }
    if let Some(patch) = request.config_patch {
        let Value::Object(patch) = patch else {
            return Err(ApiError::InvalidArgument(
                "trigger config patch must be an object".into(),
            ));
        };
        if patch.is_empty() {
            return Err(ApiError::InvalidArgument(
                "trigger config patch must contain a field".into(),
            ));
        }
        merge_config(&mut trigger.config, patch);
    }
    if let Some(change) = request.secret_change {
        trigger.secret = match change {
            TriggerSecretChange::Clear => None,
            TriggerSecretChange::Replace { value } => {
                if value.is_empty() {
                    return Err(ApiError::InvalidArgument(
                        "trigger secret cannot be empty".into(),
                    ));
                }
                if value.len() > MAX_TRIGGER_SECRET_BYTES {
                    return Err(ApiError::PayloadTooLarge(
                        "trigger secret exceeds 256 KiB".into(),
                    ));
                }
                Some(SecretString::new(value))
            }
        };
    }
    if validate {
        // Validation errors can include snippets from the merged private configuration.
        validate_trigger_config(
            &trigger.trigger_type,
            &trigger.config,
            trigger.secret.is_some(),
        )
        .map_err(|_| {
            ApiError::InvalidArgument("the resulting trigger configuration is invalid".into())
        })?;
    }
    if !state
        .storage
        .update_trigger_cas(&trigger, request.expected_updated_at)
        .await
        .map_err(|error| ApiError::from_storage(error, "trigger"))?
    {
        return Err(stale());
    }
    let uncertain =
        || ApiError::Internal("trigger settings may have changed; inspect again".into());
    let stored = state
        .storage
        .get_trigger(Some(&tenant.tenant_id), &slug)
        .await
        .map_err(|_| uncertain())?
        .filter(|stored| {
            stored.slug == trigger.slug
                && stored.tenant_id == trigger.tenant_id
                && stored.created_at == trigger.created_at
                && stored.updated_at > trigger.updated_at
                && stored.enabled == trigger.enabled
                && stored.sequence_name == trigger.sequence_name
                && stored.namespace == trigger.namespace
                && stored.version == trigger.version
                && stored.trigger_type == trigger.trigger_type
                && stored.config == trigger.config
                && stored.secret.as_ref().map(SecretString::expose)
                    == trigger.secret.as_ref().map(SecretString::expose)
        })
        .ok_or_else(uncertain)?;
    Ok(Json(TriggerSettingsReceipt {
        updated_at: stored.updated_at,
    }))
}
