//! Embed sequences: discovery for embedded viewers and the embedded builder.
//!
//! Visibility ([`EmbedPrincipal::visibility`]): the sub-tenant's own
//! sequences, gallery templates (tenant-level, namespace `embed-gallery`,
//! `embed.gallery = true`), and tenant-level sequences admitted by the
//! token's allowlist. Only owned sequences and gallery templates expose their
//! full definition; other tenant-level sequences are served as an allow-list
//! projection (block types, ids, step handler names and nesting only), so a
//! vendor's configuration (URLs, prompts, conditions, cache and queue keys,
//! credential references) is never disclosed to its end customers. Builders
//! write only sequences their sub-tenant owns and never into the gallery
//! namespace; copying a gallery template means reading it and writing it
//! under a new owned name.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use orch8_types::ids::{Namespace, SequenceId};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};

use super::runs::resolve_visible_sequence;
use super::token::{EmbedPrincipal, EmbedScope, SequenceVisibility};
use crate::AppState;
use crate::error::ApiError;

/// Upper bound on distinct sequence names returned by one listing.
const LIST_LIMIT: u32 = 1_000;
const MAX_NAME_BYTES: usize = 256;
/// Hard cap on a `PUT /embed/sequences/{name}` body (the server-wide limit
/// is far larger). Definitions beyond this are refused with 413.
pub(crate) const PUT_BODY_LIMIT_BYTES: usize = 1024 * 1024;

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

/// Allow-list projection of a block for viewers who may see a sequence's
/// shape but not its configuration (EM-09): block type, id, step handler and
/// nesting only. Everything else (params, conditions, inputs, cache/queue/
/// rate-limit keys, placement, prompts, sub-sequence targets) is dropped
/// because it is simply never copied.
fn project_block(block: &BlockDefinition) -> serde_json::Value {
    use serde_json::json;
    let list = |blocks: &[BlockDefinition]| -> Vec<serde_json::Value> {
        blocks.iter().map(project_block).collect()
    };
    let branches = |branches: &[Vec<BlockDefinition>]| -> Vec<Vec<serde_json::Value>> {
        branches.iter().map(|b| list(b)).collect()
    };
    match block {
        BlockDefinition::Step(d) => json!({"type": "step", "id": d.id, "handler": d.handler}),
        BlockDefinition::Parallel(d) => {
            json!({"type": "parallel", "id": d.id, "branches": branches(&d.branches)})
        }
        BlockDefinition::Race(d) => {
            json!({"type": "race", "id": d.id, "branches": branches(&d.branches)})
        }
        BlockDefinition::Loop(d) => json!({"type": "loop", "id": d.id, "body": list(&d.body)}),
        BlockDefinition::ForEach(d) => {
            json!({"type": "for_each", "id": d.id, "body": list(&d.body)})
        }
        BlockDefinition::Router(d) => json!({
            "type": "router", "id": d.id,
            "routes": d.routes.iter().map(|r| json!({"blocks": list(&r.blocks)})).collect::<Vec<_>>(),
            "default": d.default.as_deref().map(list),
        }),
        BlockDefinition::TryCatch(d) => json!({
            "type": "try_catch", "id": d.id,
            "try_block": list(&d.try_block), "catch_block": list(&d.catch_block),
            "finally_block": d.finally_block.as_deref().map(list),
        }),
        BlockDefinition::SubSequence(d) => json!({"type": "sub_sequence", "id": d.id}),
        BlockDefinition::ABSplit(d) => json!({
            "type": "ab_split", "id": d.id,
            "variants": d.variants.iter().map(|v| json!({"blocks": list(&v.blocks)})).collect::<Vec<_>>(),
        }),
        BlockDefinition::CancellationScope(d) => {
            json!({"type": "cancellation_scope", "id": d.id, "blocks": list(&d.blocks)})
        }
        BlockDefinition::Saga(d) => json!({
            "type": "saga", "id": d.id,
            "steps": d.steps.iter().map(|st| json!({
                "id": st.id,
                "action": project_block(&st.action),
                "compensation": st.compensation.as_deref().map(project_block),
            })).collect::<Vec<_>>(),
        }),
    }
}

/// Definition served for tenant-level sequences that are not owned by the
/// sub-tenant and are not gallery templates.
fn project_definition(seq: &SequenceDefinition) -> serde_json::Value {
    serde_json::json!({
        "id": seq.id,
        "namespace": seq.namespace,
        "name": seq.name,
        "version": seq.version,
        "input_schema": seq.input_schema,
        "blocks": seq.blocks.iter().map(project_block).collect::<Vec<_>>(),
    })
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
    // Visibility is decided in SQL: latest version per name, only what this
    // principal may see (EM-02).
    let visible = state
        .storage
        .list_embed_visible_sequences(
            &principal.tenant_id,
            &principal.sub_tenant,
            &principal.tenant_level_filter(),
            LIST_LIMIT,
        )
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;
    // Defence in depth (and de-duplication of a name held at both levels).
    let mut latest: std::collections::BTreeMap<
        (String, String),
        (SequenceVisibility, SequenceDefinition),
    > = std::collections::BTreeMap::new();
    for seq in visible {
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
        (status = 200, description = "Summary + `definition`. Owned sequences and gallery templates carry the full definition; other tenant-level sequences an allow-list projection (block types, ids, handler names, nesting; `redacted: true`)",
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
    let redacted = !(summary.owned || summary.gallery);
    let definition = if redacted {
        project_definition(&seq)
    } else {
        serde_json::to_value(&seq)
            .map_err(|e| ApiError::Internal(format!("encode sequence: {e}")))?
    };
    body["definition"] = definition;
    body["redacted"] = serde_json::json!(redacted);
    Ok(Json(body))
}

/// Per-sub-tenant storage caps for the embedded builder (EM-02): versions
/// kept per name and distinct names owned. `existing_versions` is the number
/// of stored versions of the name being written (0 = a new name).
async fn enforce_write_caps(
    state: &AppState,
    principal: &EmbedPrincipal,
    existing_versions: usize,
) -> Result<(), ApiError> {
    let limits = &state.embedded.limits;
    let max_versions = limits.max_versions_per_sequence as usize;
    if max_versions > 0 && existing_versions >= max_versions {
        return Err(ApiError::SubTenantQuotaExceeded(format!(
            "sequence already has {max_versions} versions (the per-sequence cap)"
        )));
    }
    let max_sequences = u64::from(limits.max_sequences_per_sub_tenant);
    if max_sequences > 0 && existing_versions == 0 {
        let owned = state
            .storage
            .count_sub_tenant_sequence_names(&principal.tenant_id, &principal.sub_tenant)
            .await
            .map_err(|e| ApiError::from_storage(e, "sequence"))?;
        if owned >= max_sequences {
            return Err(ApiError::SubTenantQuotaExceeded(format!(
                "sub-tenant already owns {max_sequences} sequences (the per-sub-tenant cap)"
            )));
        }
    }
    Ok(())
}

#[utoipa::path(put, path = "/embed/sequences/{name}", tag = "embed", operation_id = "embed_put_sequence",
    params(("name" = String, Path, description = "Sequence name"), NamespaceQuery),
    request_body(content = serde_json::Value,
        description = "Sequence document (blocks, input_schema, embed, …); identity fields are server-assigned"),
    responses(
        (status = 201, description = "New version created, owned by the token's sub-tenant", body = serde_json::Value),
        (status = 403, description = "Token lacks builder:edit or the sequence"),
        (status = 409, description = "The name belongs to the tenant or another sub-tenant"),
        (status = 413, description = "Body exceeds 1 MiB"),
        (status = 429, description = "Per-sequence version cap or per-sub-tenant sequence cap reached (`sub_tenant_quota_exceeded`)"),
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
    enforce_write_caps(&state, &principal, versions.len()).await?;
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
    fn projection_keeps_shape_and_drops_all_configuration() {
        let seq: SequenceDefinition = serde_json::from_value(serde_json::json!({
            "id": SequenceId::new(), "tenant_id": "t", "namespace": "default", "name": "n",
            "version": 3, "created_at": Utc::now(),
            "interceptors": {"before_step": {"handler": "secret_hook", "params": {"k": "v"}}},
            "placement": {"queue_name": "internal-queue"},
            "blocks": [
                {"type": "step", "id": "a", "handler": "http_request", "cache_key": "SECRETCACHE",
                 "queue_name": "SECRETQ", "rate_limit_key": "SECRETRL",
                 "params": {"url": "https://internal", "token": "{{credentials.x}}"}},
                {"type": "router", "id": "r", "routes": [{"condition": "ctx.data.vip == SECRETCOND",
                    "blocks": [{"type": "step", "id": "b", "handler": "llm_call", "params": {"prompt": "x"}}]}]},
                {"type": "sub_sequence", "id": "s", "sequence_name": "internal-admin", "input": {"k": "SECRETIN"}},
                {"type": "parallel", "id": "p", "branches": [[
                    {"type": "step", "id": "c", "handler": "noop", "params": {}}
                ]]}
            ]
        }))
        .unwrap();
        let value = project_definition(&seq);
        let text = value.to_string();
        for secret in [
            "internal",
            "credentials",
            "prompt",
            "SECRET",
            "secret_hook",
            "ctx.data",
        ] {
            assert!(!text.contains(secret), "{secret} leaked: {text}");
        }
        assert_eq!(value["blocks"][0]["handler"], "http_request");
        assert_eq!(
            value["blocks"][1]["routes"][0]["blocks"][0]["handler"],
            "llm_call"
        );
        assert_eq!(value["blocks"][3]["branches"][0][0]["id"], "c");
        assert_eq!(value["version"], 3);
    }
}
