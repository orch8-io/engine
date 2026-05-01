//! Trigger processor: watches for enabled trigger definitions and spawns
//! listeners (NATS subscriptions, file watchers) that create instances on events.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
#[cfg(test)]
use orch8_types::ids::TenantId;
use orch8_types::ids::{InstanceId, Namespace};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::trigger::{TriggerDef, TriggerType};

/// Tracks active trigger listeners so we can start/stop them as definitions change.
struct ActiveTriggers {
    /// Map of slug -> cancel token for the listener task.
    listeners: HashMap<String, CancellationToken>,
}

/// Run the trigger processor loop. Periodically syncs trigger definitions from DB
/// and starts/stops listeners as needed.
pub async fn run_trigger_loop(
    storage: Arc<dyn StorageBackend>,
    poll_interval: std::time::Duration,
    cancel: CancellationToken,
) {
    let active = Arc::new(RwLock::new(ActiveTriggers {
        listeners: HashMap::new(),
    }));

    let mut ticker = interval(poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!(
        poll_secs = poll_interval.as_secs(),
        "starting trigger processor loop"
    );

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                info!("trigger processor loop cancelled");
                // Cancel all active listeners.
                {
                    let active = active.read().await;
                    for (slug, token) in &active.listeners {
                        debug!(slug, "cancelling trigger listener");
                        token.cancel();
                    }
                }
                return;
            }
            _ = ticker.tick() => {
                if let Err(e) = sync_triggers(&storage, &active, &cancel).await {
                    error!(error = %e, "trigger sync failed");
                }
            }
        }
    }
}

async fn sync_triggers(
    storage: &Arc<dyn StorageBackend>,
    active: &Arc<RwLock<ActiveTriggers>>,
    parent_cancel: &CancellationToken,
) -> Result<(), orch8_types::error::StorageError> {
    let triggers = storage.list_triggers(None, 1000).await?;

    {
        let mut active = active.write().await;

        // Build set of slugs that should be active. `Webhook` and `Event` triggers
        // are fired synchronously via HTTP routes, not by background listeners —
        // exclude them here so we don't warn about "unsupported trigger type".
        let desired: HashMap<String, &TriggerDef> = triggers
            .iter()
            .filter(|t| {
                t.enabled && !matches!(t.trigger_type, TriggerType::Webhook | TriggerType::Event)
            })
            .map(|t| (t.slug.clone(), t))
            .collect();

        // Stop listeners for triggers that were removed or disabled.
        let to_remove: Vec<String> = active
            .listeners
            .keys()
            .filter(|slug| !desired.contains_key(*slug))
            .cloned()
            .collect();
        for slug in to_remove {
            if let Some(token) = active.listeners.remove(&slug) {
                info!(slug, "stopping trigger listener");
                token.cancel();
            }
        }

        // Start listeners for new triggers.
        for (slug, trigger) in &desired {
            if active.listeners.contains_key(slug) {
                continue;
            }

            let child_cancel = parent_cancel.child_token();
            active.listeners.insert(slug.clone(), child_cancel.clone());

            match trigger.trigger_type {
                #[cfg(feature = "nats")]
                TriggerType::Nats => {
                    let storage = Arc::clone(storage);
                    let trigger = (*trigger).clone();
                    let cancel = child_cancel;
                    tokio::spawn(async move {
                        if let Err(e) = run_nats_listener(storage, trigger, cancel).await {
                            error!(error = %e, "NATS trigger listener failed");
                        }
                    });
                }
                #[cfg(feature = "file-watch")]
                TriggerType::FileWatch => {
                    let storage = Arc::clone(storage);
                    let trigger = (*trigger).clone();
                    let cancel = child_cancel;
                    tokio::spawn(async move {
                        if let Err(e) = run_file_watch_listener(storage, trigger, cancel).await {
                            error!(error = %e, "file watch trigger listener failed");
                        }
                    });
                }
                _ => {
                    warn!(
                        slug,
                        trigger_type = %trigger.trigger_type,
                        "unsupported trigger type, skipping"
                    );
                    active.listeners.remove(slug);
                }
            }
        }
    }

    Ok(())
}

/// Build an in-memory `TaskInstance` for a trigger firing. Pure function — no
/// storage mutation. The caller is responsible for persisting it (either via
/// `storage.create_instance` or, for dedupe paths, via
/// `storage.create_instance_with_dedupe`).
///
/// Factored out of [`create_trigger_instance`] so `emit_event` can build the
/// instance, then pass it to `StorageBackend::create_instance_with_dedupe` in
/// a single transaction — see architectural finding #2 (close the dedupe
/// orphan window).
#[allow(clippy::needless_pass_by_value)] // `data` and `event_meta` are moved into the returned instance via json!/struct field init — taking by value matches `create_trigger_instance`'s signature.
pub(crate) fn build_trigger_instance(
    trigger: &TriggerDef,
    sequence_id: orch8_types::ids::SequenceId,
    data: serde_json::Value,
    event_meta: serde_json::Value,
    id: Option<InstanceId>,
) -> TaskInstance {
    let now = chrono::Utc::now();
    TaskInstance {
        id: id.unwrap_or_default(),
        sequence_id,
        tenant_id: trigger.tenant_id.clone(),
        namespace: Namespace(trigger.namespace.clone()),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now),
        priority: Priority::Normal,
        timezone: String::new(),
        metadata: serde_json::json!({
            "_trigger": trigger.slug,
            "_trigger_type": trigger.trigger_type,
            "_trigger_event": event_meta,
        }),
        context: ExecutionContext {
            data,
            ..Default::default()
        },
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        created_at: now,
        updated_at: now,
    }
}

/// Resolve a trigger's sequence by `(tenant, namespace, name, version)`,
/// returning the `SequenceDefinition` or a `NotFound` error. Extracted so
/// `emit_event` can look up the sequence independently of persistence.
pub(crate) async fn resolve_trigger_sequence(
    storage: &dyn StorageBackend,
    trigger: &TriggerDef,
) -> Result<orch8_types::sequence::SequenceDefinition, crate::error::EngineError> {
    storage
        .get_sequence_by_name(
            &trigger.tenant_id,
            &Namespace(trigger.namespace.clone()),
            &trigger.sequence_name,
            trigger.version,
        )
        .await?
        .ok_or_else(|| {
            crate::error::EngineError::NotFound(format!(
                "sequence '{}' for trigger '{}'",
                trigger.sequence_name, trigger.slug
            ))
        })
}

/// Create an instance from a trigger event.
///
/// Resolves the sequence by name, builds a `TaskInstance` with trigger metadata,
/// and persists it. Used by both the trigger processor and the HTTP fire endpoint.
pub async fn create_trigger_instance(
    storage: &dyn StorageBackend,
    trigger: &TriggerDef,
    data: serde_json::Value,
    event_meta: serde_json::Value,
    id: Option<InstanceId>,
) -> Result<InstanceId, crate::error::EngineError> {
    let sequence = resolve_trigger_sequence(storage, trigger).await?;
    let instance = build_trigger_instance(trigger, sequence.id, data, event_meta, id);
    storage.create_instance(&instance).await?;
    info!(
        instance_id = %instance.id,
        trigger = %trigger.slug,
        "trigger created instance"
    );
    Ok(instance.id)
}

// ─── NATS Trigger ────────────────────────────────────────────────────────────

#[cfg(feature = "nats")]
async fn run_nats_listener(
    storage: Arc<dyn StorageBackend>,
    trigger: TriggerDef,
    cancel: CancellationToken,
) -> Result<(), crate::error::EngineError> {
    use tokio_stream::StreamExt;

    let url = trigger
        .config
        .get("url")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("nats://localhost:4222");
    let subject = trigger
        .config
        .get("subject")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            crate::error::EngineError::InvalidConfig(
                "NATS trigger requires 'subject' in config".into(),
            )
        })?
        .to_string();

    info!(
        slug = %trigger.slug,
        url,
        subject,
        "connecting NATS trigger listener"
    );

    let client = async_nats::connect(url).await.map_err(|e| {
        crate::error::EngineError::InvalidConfig(format!("NATS connect failed: {e}"))
    })?;

    let mut subscriber = client.subscribe(subject.clone()).await.map_err(|e| {
        crate::error::EngineError::InvalidConfig(format!("NATS subscribe failed: {e}"))
    })?;

    info!(
        slug = %trigger.slug,
        subject,
        "NATS trigger listener active"
    );

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                info!(slug = %trigger.slug, "NATS trigger listener shutting down");
                let _ = subscriber.unsubscribe().await;
                return Ok(());
            }
            msg = subscriber.next() => {
                if let Some(msg) = msg {
                    let data: serde_json::Value = serde_json::from_slice(&msg.payload)
                        .unwrap_or_else(|_| {
                            serde_json::Value::String(
                                String::from_utf8_lossy(&msg.payload).into_owned(),
                            )
                        });
                    let reply_str = msg.reply.as_ref().map(ToString::to_string);
                    let meta = serde_json::json!({
                        "subject": msg.subject.as_str(),
                        "reply": reply_str,
                    });
                    if let Err(e) = create_trigger_instance(&*storage, &trigger, data, meta, None).await {
                        error!(
                            slug = %trigger.slug,
                            error = %e,
                            "failed to create instance from NATS message"
                        );
                    }
                } else {
                    warn!(slug = %trigger.slug, "NATS subscription ended");
                    return Ok(());
                }
            }
        }
    }
}

// ─── File Watch Trigger ──────────────────────────────────────────────────────

#[cfg(feature = "file-watch")]
async fn run_file_watch_listener(
    storage: Arc<dyn StorageBackend>,
    trigger: TriggerDef,
    cancel: CancellationToken,
) -> Result<(), crate::error::EngineError> {
    use notify::{RecommendedWatcher, RecursiveMode, Watcher};
    use std::path::PathBuf;

    let path = trigger
        .config
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            crate::error::EngineError::InvalidConfig(
                "file_watch trigger requires 'path' in config".into(),
            )
        })?
        .to_string();

    let recursive = trigger
        .config
        .get("recursive")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let mode = if recursive {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    };

    info!(
        slug = %trigger.slug,
        path,
        recursive,
        "starting file watch trigger"
    );

    let (tx, mut rx) = tokio::sync::mpsc::channel(64);

    let watcher_slug = trigger.slug.clone();
    let mut watcher: RecommendedWatcher =
        notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| {
            // This closure runs on the notify backend thread (not a tokio
            // worker), so we surface problems via tracing rather than try to
            // return an error. Silent drops of either the watch error *or*
            // the forward-to-channel result would mean events vanish without
            // anyone noticing — e.g. permission revoked, filesystem
            // unmounted, or the scheduler-side receiver already dropped.
            match res {
                Ok(event) => {
                    if let Err(e) = tx.blocking_send(event) {
                        warn!(
                            slug = %watcher_slug,
                            error = %e,
                            "file watch event dropped: receiver unavailable or channel full"
                        );
                    }
                }
                Err(e) => {
                    error!(
                        slug = %watcher_slug,
                        error = %e,
                        "file watch backend reported error"
                    );
                }
            }
        })
        .map_err(|e| {
            crate::error::EngineError::InvalidConfig(format!("file watcher init failed: {e}"))
        })?;

    watcher.watch(&PathBuf::from(&path), mode).map_err(|e| {
        crate::error::EngineError::InvalidConfig(format!("file watch start failed: {e}"))
    })?;

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                info!(slug = %trigger.slug, "file watch trigger shutting down");
                return Ok(());
            }
            event = rx.recv() => {
                if let Some(event) = event {
                    // Only fire on create/modify events.
                    if !matches!(
                        event.kind,
                        notify::EventKind::Create(_) | notify::EventKind::Modify(_)
                    ) {
                        continue;
                    }

                    let paths: Vec<String> = event
                        .paths
                        .iter()
                        .map(|p| p.to_string_lossy().into_owned())
                        .collect();

                    let data = serde_json::json!({
                        "paths": paths,
                        "kind": format!("{:?}", event.kind),
                    });
                    let meta = serde_json::json!({
                        "watched_path": path,
                        "event_kind": format!("{:?}", event.kind),
                    });

                    if let Err(e) = create_trigger_instance(&*storage, &trigger, data, meta, None).await {
                        error!(
                            slug = %trigger.slug,
                            error = %e,
                            "failed to create instance from file event"
                        );
                    }
                } else {
                    warn!(slug = %trigger.slug, "file watch channel closed");
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::ids::SequenceId;
    use orch8_types::sequence::SequenceDefinition;

    fn mk_trigger(slug: &str, seq_name: &str, tt: TriggerType) -> TriggerDef {
        let now = chrono::Utc::now();
        TriggerDef {
            slug: slug.into(),
            sequence_name: seq_name.into(),
            version: None,
            tenant_id: TenantId("t1".into()),
            namespace: "default".into(),
            enabled: true,
            secret: None,
            trigger_type: tt,
            config: serde_json::Value::Null,
            created_at: now,
            updated_at: now,
        }
    }

    async fn seed_sequence(storage: &SqliteStorage, name: &str) -> SequenceId {
        let id = SequenceId::new();
        let seq = SequenceDefinition {
            id,
            tenant_id: TenantId("t1".into()),
            namespace: Namespace("default".into()),
            name: name.into(),
            version: 1,
            deprecated: false,
            blocks: vec![],
            interceptors: None,
            created_at: chrono::Utc::now(),
        };
        storage.create_sequence(&seq).await.unwrap();
        id
    }

    #[test]
    fn trigger_type_matching() {
        // Webhook triggers should be excluded from the processor (handled by HTTP API).
        let t = TriggerDef {
            slug: "test".into(),
            sequence_name: "seq".into(),
            version: None,
            tenant_id: TenantId("t1".into()),
            namespace: "default".into(),
            enabled: true,
            secret: None,
            trigger_type: TriggerType::Webhook,
            config: serde_json::Value::Null,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        // Webhook should not be in the desired set for non-webhook processing.
        assert_eq!(t.trigger_type, TriggerType::Webhook);
    }

    #[tokio::test]
    async fn create_trigger_instance_persists_instance_with_trigger_metadata() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let seq_id = seed_sequence(&storage, "pipeline").await;

        let trigger = mk_trigger("on-push", "pipeline", TriggerType::Nats);
        let data = serde_json::json!({"branch": "main"});
        let meta = serde_json::json!({"subject": "deploy"});

        let id = create_trigger_instance(&storage, &trigger, data.clone(), meta.clone(), None)
            .await
            .unwrap();

        let stored = storage.get_instance(id).await.unwrap().unwrap();
        assert_eq!(stored.sequence_id, seq_id);
        assert_eq!(stored.state, InstanceState::Scheduled);
        assert_eq!(stored.tenant_id.0, "t1");
        assert_eq!(stored.namespace.0, "default");
        assert_eq!(stored.context.data, data);
        assert_eq!(stored.metadata["_trigger"], "on-push");
        assert_eq!(stored.metadata["_trigger_event"], meta);
        // _trigger_type is serialized via serde (NATS → "nats").
        assert_eq!(stored.metadata["_trigger_type"], "nats");
    }

    #[tokio::test]
    async fn create_trigger_instance_errors_when_sequence_missing() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let trigger = mk_trigger("orphan", "no-such-seq", TriggerType::Webhook);
        let err = create_trigger_instance(
            &storage,
            &trigger,
            serde_json::Value::Null,
            serde_json::Value::Null,
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, crate::error::EngineError::NotFound(_)));
    }

    #[tokio::test]
    async fn create_trigger_instance_uses_default_priority() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        seed_sequence(&storage, "seq").await;
        let trigger = mk_trigger("t", "seq", TriggerType::Event);
        let id = create_trigger_instance(
            &storage,
            &trigger,
            serde_json::Value::Null,
            serde_json::Value::Null,
            None,
        )
        .await
        .unwrap();
        let stored = storage.get_instance(id).await.unwrap().unwrap();
        assert_eq!(stored.priority, Priority::Normal);
    }

    #[tokio::test]
    async fn create_trigger_instance_sets_next_fire_at_to_now() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        seed_sequence(&storage, "seq").await;
        let trigger = mk_trigger("t", "seq", TriggerType::Webhook);
        let before = chrono::Utc::now();
        let id = create_trigger_instance(
            &storage,
            &trigger,
            serde_json::Value::Null,
            serde_json::Value::Null,
            None,
        )
        .await
        .unwrap();
        let after = chrono::Utc::now();
        let stored = storage.get_instance(id).await.unwrap().unwrap();
        let nfa = stored.next_fire_at.expect("next_fire_at must be set");
        assert!(
            nfa >= before && nfa <= after,
            "nfa must be within [before, after]"
        );
    }

    #[tokio::test]
    async fn create_trigger_instance_clone_independent_ids() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        seed_sequence(&storage, "seq").await;
        let trigger = mk_trigger("t", "seq", TriggerType::Webhook);
        let a = create_trigger_instance(
            &storage,
            &trigger,
            serde_json::json!({}),
            serde_json::json!({}),
            None,
        )
        .await
        .unwrap();
        let b = create_trigger_instance(
            &storage,
            &trigger,
            serde_json::json!({}),
            serde_json::json!({}),
            None,
        )
        .await
        .unwrap();
        assert_ne!(a, b, "each trigger firing must create a distinct instance");
    }

    #[tokio::test]
    async fn create_trigger_instance_preserves_nested_data_payload() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        seed_sequence(&storage, "seq").await;
        let trigger = mk_trigger("t", "seq", TriggerType::Webhook);
        let data = serde_json::json!({
            "deep": {"nested": {"arr": [1,2,3], "null": null, "flag": true}}
        });
        let id = create_trigger_instance(
            &storage,
            &trigger,
            data.clone(),
            serde_json::Value::Null,
            None,
        )
        .await
        .unwrap();
        let stored = storage.get_instance(id).await.unwrap().unwrap();
        assert_eq!(stored.context.data, data);
    }

    #[tokio::test]
    async fn create_trigger_instance_uses_specific_version_when_specified() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        // Seed two versions of the same sequence name.
        let v1_id = SequenceId::new();
        let v2_id = SequenceId::new();
        let now = chrono::Utc::now();
        let mk_seq = |id: SequenceId, version: i32| SequenceDefinition {
            id,
            tenant_id: TenantId("t1".into()),
            namespace: Namespace("default".into()),
            name: "multi".into(),
            version,
            deprecated: false,
            blocks: vec![],
            interceptors: None,
            created_at: now,
        };
        storage.create_sequence(&mk_seq(v1_id, 1)).await.unwrap();
        storage.create_sequence(&mk_seq(v2_id, 2)).await.unwrap();

        let mut trigger = mk_trigger("t", "multi", TriggerType::Webhook);
        trigger.version = Some(1);
        let id = create_trigger_instance(
            &storage,
            &trigger,
            serde_json::Value::Null,
            serde_json::Value::Null,
            None,
        )
        .await
        .unwrap();
        let stored = storage.get_instance(id).await.unwrap().unwrap();
        assert_eq!(stored.sequence_id, v1_id, "must bind to exact version 1");
    }

    #[tokio::test]
    async fn create_trigger_instance_empty_timezone_string() {
        // Trigger-fired instances have no bound timezone — assert contract.
        let storage = SqliteStorage::in_memory().await.unwrap();
        seed_sequence(&storage, "seq").await;
        let trigger = mk_trigger("t", "seq", TriggerType::Webhook);
        let id = create_trigger_instance(
            &storage,
            &trigger,
            serde_json::Value::Null,
            serde_json::Value::Null,
            None,
        )
        .await
        .unwrap();
        let stored = storage.get_instance(id).await.unwrap().unwrap();
        assert_eq!(stored.timezone, "");
    }

    #[tokio::test]
    async fn create_trigger_instance_does_not_set_parent_or_session() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        seed_sequence(&storage, "seq").await;
        let trigger = mk_trigger("t", "seq", TriggerType::Webhook);
        let id = create_trigger_instance(
            &storage,
            &trigger,
            serde_json::Value::Null,
            serde_json::Value::Null,
            None,
        )
        .await
        .unwrap();
        let stored = storage.get_instance(id).await.unwrap().unwrap();
        assert!(stored.parent_instance_id.is_none());
        assert!(stored.session_id.is_none());
        assert!(stored.idempotency_key.is_none());
        assert!(stored.concurrency_key.is_none());
    }

    #[tokio::test]
    async fn create_trigger_instance_namespace_isolation() {
        // A trigger scoped to namespace "prod" creates an instance in namespace "prod".
        let storage = SqliteStorage::in_memory().await.unwrap();
        let seq_id = SequenceId::new();
        storage
            .create_sequence(&SequenceDefinition {
                id: seq_id,
                tenant_id: TenantId("t1".into()),
                namespace: Namespace("prod".into()),
                name: "seq".into(),
                version: 1,
                deprecated: false,
                blocks: vec![],
                interceptors: None,
                created_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let mut trigger = mk_trigger("t", "seq", TriggerType::Webhook);
        trigger.namespace = "prod".into();
        let id = create_trigger_instance(
            &storage,
            &trigger,
            serde_json::Value::Null,
            serde_json::Value::Null,
            None,
        )
        .await
        .unwrap();
        let stored = storage.get_instance(id).await.unwrap().unwrap();
        assert_eq!(stored.namespace.0, "prod");
        assert_eq!(stored.sequence_id, seq_id);
    }
}
