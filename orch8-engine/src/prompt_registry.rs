//! Prompt registry: versioned, tenant-scoped prompt templates referenced
//! from `llm_call` as `prompt: {name, version | label, variables}`.
//!
//! - **Push** is idempotent: content identical to the latest version returns
//!   that version; otherwise version `latest + 1` is appended. Versions are
//!   immutable and never deleted (a replayed or audited step can always
//!   re-read the exact text it ran).
//! - **Labels** (`production`, `canary`, …) are movable aliases; a label can
//!   carry a canary split `{version, percent}` resolved per execution by the
//!   same deterministic `(instance, block)` hashing `ab_split` uses.
//! - **Replay determinism**: the first resolution of a prompt in a step is
//!   pinned in instance state (`__prompt__:{block_id}:{name}`); retries,
//!   crash recovery and re-executions of that block within the instance
//!   reuse the pinned version even if the label has moved since. The
//!   resolution is also recorded on the step output (`prompt`), which is what
//!   release validation replays.

use serde_json::{Map, Value, json};
use tracing::info;

use orch8_storage::StorageBackend;
use orch8_types::ai::{
    MAX_PROMPT_BYTES, PromptCanary, PromptLabel, PromptMessage, PromptRef, PromptResolution,
    PromptTemplate, render_prompt_text, validate_prompt_identifier,
};
use orch8_types::error::{StepError, StorageError};
use orch8_types::ids::InstanceId;

/// Instance-KV key prefix under which step prompt resolutions are pinned.
pub const PROMPT_PIN_PREFIX: &str = "__prompt__:";

/// Registry operation failure.
#[derive(Debug, thiserror::Error)]
pub enum PromptRegistryError {
    #[error("invalid prompt: {0}")]
    Invalid(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// Content of a prompt version to push (identity is assigned by the registry).
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct PromptDraft {
    pub tenant_id: String,
    pub name: String,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub messages: Vec<PromptMessage>,
    #[serde(default)]
    pub model_params: Option<Value>,
    #[serde(default)]
    pub response_schema: Option<Value>,
    #[serde(default)]
    pub description: Option<String>,
}

const ALLOWED_ROLES: &[&str] = &["system", "user", "assistant", "developer"];
/// Model params a prompt may not carry: credentials and endpoints stay with
/// the step (and its tenant's credential policy), never in shared templates.
const FORBIDDEN_MODEL_PARAMS: &[&str] = &[
    "api_key",
    "api_key_env",
    "base_url",
    "providers",
    "messages",
    "system",
    "prompt",
    "cache",
    "response_schema",
];

fn validate_draft(draft: &PromptDraft) -> Result<(), PromptRegistryError> {
    validate_prompt_identifier("name", &draft.name).map_err(PromptRegistryError::Invalid)?;
    if draft.system.is_none() && draft.messages.is_empty() {
        return Err(PromptRegistryError::Invalid(
            "a prompt needs `system` and/or `messages`".into(),
        ));
    }
    if let Some(m) = draft
        .messages
        .iter()
        .find(|m| !ALLOWED_ROLES.contains(&m.role.as_str()))
    {
        return Err(PromptRegistryError::Invalid(format!(
            "message role '{}' must be one of {ALLOWED_ROLES:?}",
            m.role
        )));
    }
    if let Some(params) = &draft.model_params {
        let Some(obj) = params.as_object() else {
            return Err(PromptRegistryError::Invalid(
                "model_params must be an object".into(),
            ));
        };
        if let Some(bad) = FORBIDDEN_MODEL_PARAMS
            .iter()
            .find(|k| obj.contains_key(**k))
        {
            return Err(PromptRegistryError::Invalid(format!(
                "model_params may not set '{bad}'"
            )));
        }
    }
    if let Some(schema) = &draft.response_schema {
        jsonschema::validator_for(schema).map_err(|e| {
            PromptRegistryError::Invalid(format!("response_schema is not a valid JSON Schema: {e}"))
        })?;
    }
    let size = serde_json::to_vec(draft).map_or(usize::MAX, |b| b.len());
    if size > MAX_PROMPT_BYTES {
        return Err(PromptRegistryError::Invalid(format!(
            "prompt is {size} bytes; the limit is {MAX_PROMPT_BYTES}"
        )));
    }
    Ok(())
}

/// Push a prompt version. Returns the stored version and whether it was
/// newly created (`false` = identical to the latest version, which is
/// returned unchanged).
pub async fn push_prompt(
    storage: &dyn StorageBackend,
    draft: PromptDraft,
) -> Result<(PromptTemplate, bool), PromptRegistryError> {
    validate_draft(&draft)?;
    let mut candidate = PromptTemplate {
        tenant_id: draft.tenant_id,
        name: draft.name,
        version: 0,
        system: draft.system,
        messages: draft.messages,
        variables: Vec::new(),
        model_params: draft
            .model_params
            .unwrap_or_else(|| Value::Object(Map::new())),
        response_schema: draft.response_schema,
        description: draft.description,
        content_hash: String::new(),
        created_at: chrono::Utc::now(),
    };
    candidate.variables = candidate.referenced_variables();
    candidate.content_hash = candidate.compute_content_hash();

    // Version allocation races are resolved by the primary key: on a
    // conflict, re-read the latest and try again.
    for _ in 0..5 {
        let latest = storage
            .get_latest_prompt_version(&candidate.tenant_id, &candidate.name)
            .await?;
        if let Some(latest) = &latest
            && latest.content_hash == candidate.content_hash
        {
            return Ok((latest.clone(), false));
        }
        candidate.version = latest.map_or(1, |l| l.version.saturating_add(1));
        match storage.insert_prompt_version(&candidate).await {
            Ok(()) => return Ok((candidate, true)),
            Err(StorageError::Conflict(_)) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Err(StorageError::Conflict("prompt version allocation kept racing; retry".into()).into())
}

/// Point `label` at `version` (optionally with a canary split).
pub async fn set_label(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    name: &str,
    label: &str,
    version: i32,
    canary: Option<PromptCanary>,
) -> Result<PromptLabel, PromptRegistryError> {
    validate_prompt_identifier("label", label).map_err(PromptRegistryError::Invalid)?;
    let mut versions = vec![version];
    if let Some(c) = canary {
        if c.percent > 100 {
            return Err(PromptRegistryError::Invalid(
                "canary percent must be 0-100".into(),
            ));
        }
        if c.version == version {
            return Err(PromptRegistryError::Invalid(
                "canary version must differ from the label's version".into(),
            ));
        }
        versions.push(c.version);
    }
    for v in versions {
        if storage
            .get_prompt_version(tenant_id, name, v)
            .await?
            .is_none()
        {
            return Err(PromptRegistryError::NotFound(format!(
                "prompt '{name}' version {v}"
            )));
        }
    }
    let record = PromptLabel {
        tenant_id: tenant_id.to_string(),
        name: name.to_string(),
        label: label.to_string(),
        version,
        canary: canary.filter(|c| c.percent > 0),
        updated_at: chrono::Utc::now(),
    };
    storage.upsert_prompt_label(&record).await?;
    Ok(record)
}

fn permanent(message: impl Into<String>) -> StepError {
    StepError::Permanent {
        message: message.into(),
        details: None,
    }
}

fn storage_err(e: &StorageError) -> StepError {
    // A registry read that fails transiently must not fail the step forever.
    StepError::Retryable {
        message: format!("prompt registry unavailable: {e}"),
        details: None,
    }
}

/// Parse the `prompt` param of an `llm_call` step.
pub fn parse_prompt_ref(value: &Value) -> Result<PromptRef, StepError> {
    let r: PromptRef = serde_json::from_value(value.clone())
        .map_err(|e| permanent(format!("llm_call: invalid `prompt` param: {e}")))?;
    validate_prompt_identifier("prompt.name", &r.name).map_err(permanent)?;
    if r.version.is_some() && r.label.is_some() {
        return Err(permanent(
            "llm_call: `prompt` takes `version` or `label`, not both",
        ));
    }
    if let Some(v) = &r.variables
        && !v.is_object()
    {
        return Err(permanent("llm_call: `prompt.variables` must be an object"));
    }
    Ok(r)
}

fn pin_key(block_id: &str, name: &str) -> String {
    format!("{PROMPT_PIN_PREFIX}{block_id}:{name}")
}

/// Resolve a step's prompt reference to an immutable version.
///
/// With `pin = true` (real execution) the first resolution for
/// `(instance, block, name)` is stored and every later call returns it;
/// dry-runs pass `pin = false` and never write.
pub async fn resolve_for_step(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    instance_id: InstanceId,
    block_id: &str,
    prompt: &PromptRef,
    pin: bool,
) -> Result<(PromptTemplate, PromptResolution), StepError> {
    let key = pin_key(block_id, &prompt.name);
    if pin
        && let Some(pinned) = storage
            .get_instance_kv(instance_id, &key)
            .await
            .map_err(|e| storage_err(&e))?
        && let Ok(resolution) = serde_json::from_value::<PromptResolution>(pinned)
    {
        let template = storage
            .get_prompt_version(tenant_id, &resolution.name, resolution.version)
            .await
            .map_err(|e| storage_err(&e))?
            .ok_or_else(|| {
                permanent(format!(
                    "pinned prompt '{}' version {} no longer exists",
                    resolution.name, resolution.version
                ))
            })?;
        return Ok((template, resolution));
    }

    let (version, label, variant) = if let Some(v) = prompt.version {
        (Some(v), None, "pinned")
    } else if let Some(label_name) = &prompt.label {
        let label = storage
            .get_prompt_label(tenant_id, &prompt.name, label_name)
            .await
            .map_err(|e| storage_err(&e))?
            .ok_or_else(|| {
                permanent(format!(
                    "prompt '{}' has no label '{label_name}'",
                    prompt.name
                ))
            })?;
        let (v, variant) = label.select(&instance_id.to_string(), block_id);
        (Some(v), Some(label_name.clone()), variant)
    } else {
        (None, None, "latest")
    };

    let template = match version {
        Some(v) => storage.get_prompt_version(tenant_id, &prompt.name, v).await,
        None => {
            storage
                .get_latest_prompt_version(tenant_id, &prompt.name)
                .await
        }
    }
    .map_err(|e| storage_err(&e))?
    .ok_or_else(|| {
        permanent(match version {
            Some(v) => format!("prompt '{}' version {v} not found", prompt.name),
            None => format!("prompt '{}' not found", prompt.name),
        })
    })?;

    let resolution = PromptResolution {
        name: template.name.clone(),
        version: template.version,
        label,
        variant: variant.to_string(),
        content_hash: template.content_hash.clone(),
    };
    if pin {
        let value = serde_json::to_value(&resolution).unwrap_or(Value::Null);
        storage
            .set_instance_kv(instance_id, &key, &value)
            .await
            .map_err(|e| storage_err(&e))?;
    }
    info!(
        target: "orch8::prompt",
        prompt_name = %resolution.name,
        prompt_version = resolution.version,
        prompt_label = resolution.label.as_deref().unwrap_or(""),
        prompt_variant = %resolution.variant,
        "orch8.prompt.resolved"
    );
    Ok((template, resolution))
}

/// Render `template` with `variables` and merge it into `llm_call` params:
/// system (unless the step sets one), prompt messages before the step's own
/// messages, model params and `response_schema` only where the step leaves
/// them unset. The `prompt` param is removed.
pub fn apply_prompt(
    params: &mut Value,
    template: &PromptTemplate,
    variables: &Value,
) -> Result<(), StepError> {
    let render = |text: &str| {
        render_prompt_text(text, variables).map_err(|missing| StepError::Permanent {
            message: format!(
                "prompt '{}' v{}: missing variables {missing:?}",
                template.name, template.version
            ),
            details: Some(json!({"missing_variables": missing})),
        })
    };
    let Some(obj) = params.as_object_mut() else {
        return Err(permanent("llm_call params must be an object"));
    };
    obj.remove("prompt");

    if let Some(system) = &template.system
        && !obj.contains_key("system")
    {
        obj.insert("system".into(), Value::String(render(system)?));
    }
    let mut messages = Vec::with_capacity(template.messages.len());
    for m in &template.messages {
        messages.push(json!({"role": m.role, "content": render(&m.content)?}));
    }
    if let Some(existing) = obj.get("messages").and_then(Value::as_array) {
        messages.extend(existing.iter().cloned());
    }
    obj.insert("messages".into(), Value::Array(messages));

    if let Some(defaults) = template.model_params.as_object() {
        for (k, v) in defaults {
            obj.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
    if let Some(schema) = &template.response_schema {
        obj.entry("response_schema")
            .or_insert_with(|| schema.clone());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::ResourceStore;
    use orch8_storage::sqlite::SqliteStorage;

    fn draft(tenant: &str, text: &str) -> PromptDraft {
        PromptDraft {
            tenant_id: tenant.into(),
            name: "triage".into(),
            system: Some("You triage {{ product }} tickets.".into()),
            messages: vec![PromptMessage {
                role: "user".into(),
                content: text.into(),
            }],
            model_params: Some(json!({"model": "gpt-4o", "temperature": 0})),
            response_schema: None,
            description: None,
        }
    }

    #[tokio::test]
    async fn push_is_idempotent_and_versions_are_monotonic() {
        let s = SqliteStorage::in_memory().await.unwrap();
        let (v1, created) = push_prompt(&s, draft("t", "A: {{ ticket }}"))
            .await
            .unwrap();
        assert!(created);
        assert_eq!(v1.version, 1);
        assert_eq!(
            v1.variables,
            vec!["product".to_string(), "ticket".to_string()]
        );
        let (same, created) = push_prompt(&s, draft("t", "A: {{ ticket }}"))
            .await
            .unwrap();
        assert!(!created);
        assert_eq!(same.version, 1);
        let (v2, _) = push_prompt(&s, draft("t", "B: {{ ticket }}"))
            .await
            .unwrap();
        assert_eq!(v2.version, 2);
        let (other, _) = push_prompt(&s, draft("u", "B: {{ ticket }}"))
            .await
            .unwrap();
        assert_eq!(other.version, 1, "versions are per tenant");
    }

    #[tokio::test]
    async fn push_rejects_invalid_prompts() {
        let s = SqliteStorage::in_memory().await.unwrap();
        let mut d = draft("t", "x");
        d.model_params = Some(json!({"api_key": "sk-leak"}));
        assert!(matches!(
            push_prompt(&s, d).await,
            Err(PromptRegistryError::Invalid(_))
        ));
        let mut d = draft("t", "x");
        d.messages[0].role = "wizard".into();
        assert!(push_prompt(&s, d).await.is_err());
        let mut d = draft("t", "x");
        d.response_schema = Some(json!({"type": 12}));
        assert!(push_prompt(&s, d).await.is_err());
        let mut d = draft("t", "x");
        d.system = None;
        d.messages.clear();
        assert!(push_prompt(&s, d).await.is_err());
    }

    #[tokio::test]
    async fn resolution_pins_first_version_even_after_label_moves() {
        let s = SqliteStorage::in_memory().await.unwrap();
        push_prompt(&s, draft("t", "one")).await.unwrap();
        push_prompt(&s, draft("t", "two")).await.unwrap();
        set_label(&s, "t", "triage", "production", 1, None)
            .await
            .unwrap();
        let r = PromptRef {
            name: "triage".into(),
            version: None,
            label: Some("production".into()),
            variables: None,
        };
        let inst = InstanceId::new();
        let (tpl, res) = resolve_for_step(&s, "t", inst, "llm", &r, true)
            .await
            .unwrap();
        assert_eq!((tpl.version, res.variant.as_str()), (1, "stable"));

        set_label(&s, "t", "triage", "production", 2, None)
            .await
            .unwrap();
        let (tpl, _) = resolve_for_step(&s, "t", inst, "llm", &r, true)
            .await
            .unwrap();
        assert_eq!(
            tpl.version, 1,
            "retry/replay keeps the originally resolved version"
        );
        let (tpl, _) = resolve_for_step(&s, "t", InstanceId::new(), "llm", &r, true)
            .await
            .unwrap();
        assert_eq!(tpl.version, 2, "new executions follow the label");

        // Dry-run resolution never pins.
        let dry = InstanceId::new();
        resolve_for_step(&s, "t", dry, "llm", &r, false)
            .await
            .unwrap();
        assert!(
            s.get_instance_kv(dry, &pin_key("llm", "triage"))
                .await
                .unwrap()
                .is_none()
        );

        // Unknown label / version / tenant are permanent config errors.
        let mut bad = r.clone();
        bad.label = Some("nope".into());
        assert!(matches!(
            resolve_for_step(&s, "t", InstanceId::new(), "llm", &bad, true).await,
            Err(StepError::Permanent { .. })
        ));
        assert!(
            resolve_for_step(&s, "other", InstanceId::new(), "llm", &r, true)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn canary_label_splits_executions() {
        let s = SqliteStorage::in_memory().await.unwrap();
        push_prompt(&s, draft("t", "one")).await.unwrap();
        push_prompt(&s, draft("t", "two")).await.unwrap();
        let canary = Some(PromptCanary {
            version: 2,
            percent: 30,
        });
        set_label(&s, "t", "triage", "production", 1, canary)
            .await
            .unwrap();
        assert!(
            set_label(
                &s,
                "t",
                "triage",
                "production",
                1,
                Some(PromptCanary {
                    version: 9,
                    percent: 5
                })
            )
            .await
            .is_err()
        );
        let r = PromptRef {
            name: "triage".into(),
            version: None,
            label: Some("production".into()),
            variables: None,
        };
        let mut canary_hits = 0;
        for _ in 0..200 {
            let (tpl, res) = resolve_for_step(&s, "t", InstanceId::new(), "llm", &r, false)
                .await
                .unwrap();
            if res.variant == "canary" {
                assert_eq!(tpl.version, 2);
                canary_hits += 1;
            }
        }
        assert!(
            (30..90).contains(&canary_hits),
            "~30% canary, got {canary_hits}"
        );
    }

    #[test]
    fn apply_prompt_renders_and_lets_step_params_win() {
        let tpl = PromptTemplate {
            tenant_id: "t".into(),
            name: "p".into(),
            version: 3,
            system: Some("Sys {{ product }}".into()),
            messages: vec![PromptMessage {
                role: "user".into(),
                content: "Q: {{ q }}".into(),
            }],
            variables: vec![],
            model_params: json!({"model": "gpt-4o", "temperature": 0.2}),
            response_schema: Some(json!({"type": "object"})),
            description: None,
            content_hash: "h".into(),
            created_at: chrono::Utc::now(),
        };
        let mut params = json!({
            "prompt": {"name": "p"},
            "temperature": 0.9,
            "messages": [{"role": "user", "content": "follow-up"}]
        });
        apply_prompt(&mut params, &tpl, &json!({"product": "Orch8", "q": "why?"})).unwrap();
        assert!(params.get("prompt").is_none());
        assert_eq!(params["system"], "Sys Orch8");
        assert_eq!(params["messages"][0]["content"], "Q: why?");
        assert_eq!(params["messages"][1]["content"], "follow-up");
        assert_eq!(params["model"], "gpt-4o");
        assert_eq!(params["temperature"], 0.9, "step param wins");
        assert_eq!(params["response_schema"], json!({"type": "object"}));

        let mut params = json!({});
        let err = apply_prompt(&mut params, &tpl, &json!({})).unwrap_err();
        assert!(err.to_string().contains("missing variables"), "{err}");
    }

    #[test]
    fn prompt_ref_parsing() {
        assert!(parse_prompt_ref(&json!({"name": "p", "label": "production"})).is_ok());
        assert!(parse_prompt_ref(&json!({"name": "p", "version": 1, "label": "x"})).is_err());
        assert!(parse_prompt_ref(&json!({"name": "p", "variables": [1]})).is_err());
        assert!(parse_prompt_ref(&json!({"name": "p", "bogus": 1})).is_err());
        assert!(parse_prompt_ref(&json!({"name": "bad name"})).is_err());
    }
}
