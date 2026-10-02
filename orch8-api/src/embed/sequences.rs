//! Embed sequences: discovery for embedded viewers and the embedded builder.
//!
//! Visibility ([`EmbedPrincipal::visibility`]): the sub-tenant's own
//! sequences, gallery templates (tenant-level, namespace `embed-gallery`,
//! `embed.gallery = true`), and tenant-level sequences admitted by the
//! token's allowlist. Only owned sequences and gallery templates expose their
//! full definition; other tenant-level sequences are served with every step's
//! `params` emptied, so a vendor's configuration (URLs, prompts, credential
//! references) is never disclosed to its end customers. Builders write only sequences their
//! sub-tenant owns and never into the gallery namespace; copying a gallery
//! template means reading it and writing it under a new owned name.

use std::collections::BTreeMap;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use orch8_types::ids::{Namespace, SequenceId};
use orch8_types::sequence::SequenceDefinition;

use super::runs::resolve_visible_sequence;
use super::token::{EmbedPrincipal, EmbedScope, SequenceVisibility};
use crate::AppState;
use crate::error::ApiError;

/// Upper bound on sequences scanned for one listing.
const LIST_SCAN_LIMIT: u32 = 1_000;
const MAX_NAME_BYTES: usize = 256;

#[derive(Debug, Deserialize, IntoParams)]
pub(crate) struct NamespaceQuery {
    /// Namespace (default `default`).
    #[serde(default)]
    pub namespace: Option<String>,
}

fn namespace_of(q: &NamespaceQuery) -> Namespace {
    Namespace::new(q.namespace.clone().unwrap_or_else(|| "default".into()))
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct EmbedSequenceSummary {
    pub id: SequenceId,
    pub name: String,
    pub namespace: String,
    pub version: i32,
    /// Display description (`embed.description`).
    pub description: Option<String>,
    /// JSON Schema of the run input (`context.data`), if the sequence has one.
    pub input_schema: Option<serde_json::Value>,
    /// Whether the token's sub-tenant owns (and may edit) the sequence.
    pub owned: bool,
    /// Read-only gallery template (copy it to customise).
    pub gallery: bool,
    /// Display title (`embed.title`).
    pub title: Option<String>,
    /// Opaque gallery template descriptor (`embed.template`).
    pub template: Option<serde_json::Value>,
}

fn summary(visibility: SequenceVisibility, seq: &SequenceDefinition) -> EmbedSequenceSummary {
    let embed = seq.embed.as_ref();
    EmbedSequenceSummary {
        id: seq.id,
        name: seq.name.clone(),
        namespace: seq.namespace.as_str().to_string(),
        version: seq.version,
        description: embed.and_then(|e| e.description.clone()),
        input_schema: seq.input_schema.clone(),
        owned: visibility == SequenceVisibility::Owned,
        gallery: orch8_types::sub_tenant::SequenceEmbed::is_gallery_template(seq),
        title: embed.and_then(|e| e.title.clone()),
        template: embed.and_then(|e| e.template.clone()),
    }
}

/// A handler the embedded builder may place in a step.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct EmbedHandler {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_params: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct EmbedSequenceList {
    pub items: Vec<EmbedSequenceSummary>,
    /// Handlers available to the builder (empty without `builder:edit`).
    pub handlers: Vec<EmbedHandler>,
}

/// Replace every step `params` object with `{}` so a summary-only
/// definition shows structure but no configuration (URLs, prompts,
/// credential references).
fn redact_params(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if key == "params" {
                    *child = serde_json::json!({});
                } else {
                    redact_params(child);
                }
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(redact_params),
        _ => {}
    }
}

fn require_read(principal: &EmbedPrincipal) -> Result<(), ApiError> {
    principal.require_any(&[
        EmbedScope::SequencesRead,
        EmbedScope::BuilderEdit,
        EmbedScope::RunsStart,
    ])
}

#[utoipa::path(get, path = "/embed/sequences", tag = "embed", operation_id = "embed_list_sequences",
    responses(
        (status = 200, description = "Latest version of every sequence visible to the sub-tenant",
            body = EmbedSequenceList),
        (status = 403, description = "Token lacks sequences:read / builder:edit / runs:start"),
    ),
    security(("embed_token" = []))
)]
pub(crate) async fn list_sequences(
    State(state): State<AppState>,
    principal: EmbedPrincipal,
) -> Result<impl IntoResponse, ApiError> {
    require_read(&principal)?;
    let all = state
        .storage
        .list_sequences(Some(&principal.tenant_id), None, LIST_SCAN_LIMIT, 0)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;
    let mut latest: BTreeMap<(String, String), (SequenceVisibility, SequenceDefinition)> =
        BTreeMap::new();
    for seq in all {
        if seq.deprecated {
            continue;
        }
        let Some(visibility) = principal.visibility(&seq) else {
            continue;
        };
        let key = (seq.namespace.as_str().to_string(), seq.name.clone());
        match latest.get(&key) {
            Some((_, existing)) if existing.version >= seq.version => {}
            _ => {
                latest.insert(key, (visibility, seq));
            }
        }
    }
    let items = latest
        .values()
        .map(|(visibility, seq)| summary(*visibility, seq))
        .collect();
    let handlers = if principal.scopes.contains(&EmbedScope::BuilderEdit) {
        state
            .builtin_handlers
            .iter()
            .filter(|name| !name.starts_with('_'))
            .map(|name| EmbedHandler {
                name: name.clone(),
                label: None,
                description: None,
                default_params: None,
            })
            .collect()
    } else {
        Vec::new()
    };
    Ok(Json(EmbedSequenceList { items, handlers }))
}

#[utoipa::path(get, path = "/embed/sequences/{name}", tag = "embed", operation_id = "embed_get_sequence",
    params(("name" = String, Path, description = "Sequence name"), NamespaceQuery),
    responses(
        (status = 200, description = "Summary + `definition`. Owned sequences and gallery templates carry the full definition; other tenant-level sequences a redacted one (step `params` emptied, `redacted: true`)",
            body = serde_json::Value),
        (status = 404, description = "Unknown or not visible"),
    ),
    security(("embed_token" = []))
)]
pub(crate) async fn get_sequence(
    State(state): State<AppState>,
    principal: EmbedPrincipal,
    Path(name): Path<String>,
    Query(q): Query<NamespaceQuery>,
) -> Result<impl IntoResponse, ApiError> {
    require_read(&principal)?;
    let (seq, visibility) =
        resolve_visible_sequence(&state, &principal, &namespace_of(&q), &name).await?;
    let summary = summary(visibility, &seq);
    let mut body = serde_json::to_value(&summary)
        .map_err(|e| ApiError::Internal(format!("encode sequence summary: {e}")))?;
    let mut definition = serde_json::to_value(&seq)
        .map_err(|e| ApiError::Internal(format!("encode sequence: {e}")))?;
    let redacted = !(summary.owned || summary.gallery);
    if redacted {
        redact_params(&mut definition);
        if let Some(object) = definition.as_object_mut() {
            object.remove("interceptors");
        }
    }
    body["definition"] = definition;
    body["redacted"] = serde_json::json!(redacted);
    Ok(Json(body))
}

#[utoipa::path(put, path = "/embed/sequences/{name}", tag = "embed", operation_id = "embed_put_sequence",
    params(("name" = String, Path, description = "Sequence name"), NamespaceQuery),
    request_body(content = serde_json::Value,
        description = "Sequence document (blocks, input_schema, embed, …); identity fields are server-assigned"),
    responses(
        (status = 201, description = "New version created, owned by the token's sub-tenant", body = serde_json::Value),
        (status = 403, description = "Token lacks builder:edit or the sequence"),
        (status = 409, description = "The name belongs to the tenant or another sub-tenant"),
    ),
    security(("embed_token" = []))
)]
pub(crate) async fn put_sequence(
    State(state): State<AppState>,
    principal: EmbedPrincipal,
    Path(name): Path<String>,
    Query(q): Query<NamespaceQuery>,
    Json(value): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    principal.require(EmbedScope::BuilderEdit)?;
    if name.is_empty() || name.len() > MAX_NAME_BYTES {
        return Err(ApiError::InvalidArgument(format!(
            "sequence name must be 1-{MAX_NAME_BYTES} bytes"
        )));
    }
    let namespace = namespace_of(&q);
    if namespace.as_str() == orch8_types::sub_tenant::GALLERY_NAMESPACE {
        return Err(ApiError::Forbidden(
            "gallery templates are read-only; copy one under a new name".into(),
        ));
    }
    let versions = state
        .storage
        .list_sequence_versions(&principal.tenant_id, &namespace, &name)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;
    // Builders may only write sequences their sub-tenant owns. A name held by
    // the tenant or another sub-tenant is refused without saying whose.
    if versions
        .iter()
        .any(|v| v.sub_tenant.as_deref() != Some(principal.sub_tenant.as_str()))
    {
        return Err(ApiError::Conflict("sequence name is not available".into()));
    }
    let next_version = versions.iter().map(|v| v.version).max().unwrap_or(0) + 1;

    // Identity is server-assigned: strip client-supplied identity fields
    // before decoding so they cannot smuggle another tenant/owner in.
    let mut value = value;
    let Some(object) = value.as_object_mut() else {
        return Err(ApiError::InvalidArgument(
            "sequence must be a JSON object".into(),
        ));
    };
    for key in [
        "id",
        "tenant_id",
        "namespace",
        "name",
        "version",
        "sub_tenant",
        "created_at",
        "deprecated",
        "status",
    ] {
        object.remove(key);
    }
    object.insert("id".into(), serde_json::json!(SequenceId::new()));
    object.insert(
        "tenant_id".into(),
        serde_json::json!(principal.tenant_id.as_str()),
    );
    object.insert("namespace".into(), serde_json::json!(namespace.as_str()));
    object.insert("name".into(), serde_json::json!(name));
    object.insert("version".into(), serde_json::json!(next_version));
    object.insert("created_at".into(), serde_json::json!(Utc::now()));
    // A copied gallery template becomes an ordinary owned sequence.
    if let Some(embed) = object
        .get_mut("embed")
        .and_then(serde_json::Value::as_object_mut)
    {
        embed.remove("gallery");
    }
    let (mut seq, warnings) = crate::sequences::decode_draft_sequence(&value, false)?;
    seq.tenant_id = principal.tenant_id.clone();
    seq.sub_tenant = Some(principal.sub_tenant.clone());
    let mut body = crate::sequences::validate_and_persist_sequence(&state, &seq, warnings).await?;
    body["version"] = serde_json::json!(seq.version);
    Ok((StatusCode::CREATED, Json(body)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redaction_empties_every_params_object() {
        let mut value = serde_json::json!({
            "blocks": [
                {"type": "step", "id": "a", "handler": "http_request",
                 "params": {"url": "https://internal", "token": "{{credentials.x}}"}},
                {"type": "parallel", "id": "p", "branches": [[
                    {"type": "step", "id": "b", "handler": "llm_call", "params": {"prompt": "x"}}
                ]]}
            ]
        });
        redact_params(&mut value);
        let text = value.to_string();
        assert!(
            !text.contains("internal") && !text.contains("credentials") && !text.contains("prompt")
        );
        assert_eq!(value["blocks"][0]["handler"], "http_request");
        assert_eq!(
            value["blocks"][1]["branches"][0][0]["params"],
            serde_json::json!({})
        );
    }
}
