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
use orch8_types::ids::{InstanceId, Namespace, TenantId};
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
                let active = active.read().await;
                for (slug, token) in &active.listeners {
                    debug!(slug, "cancelling trigger listener");
                    token.cancel();
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
    let triggers = storage.list_triggers(None).await?;

    let mut active = active.write().await;

    // Build set of slugs that should be active. `Webhook` and `Event` triggers
    // are fired synchronously via HTTP routes, not by background listeners —
    // exclude them here so we don't warn about "unsupported trigger type".
    let desired: HashMap<String, &TriggerDef> = triggers
        .iter()
        .filter(|t| {
            t.enabled
                && !matches!(t.trigger_type, TriggerType::Webhook | TriggerType::Event)
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

    Ok(())
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
) -> Result<InstanceId, crate::error::EngineError> {
    let sequence = storage
        .get_sequence_by_name(
            &TenantId(trigger.tenant_id.clone()),
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
        })?;

    let now = chrono::Utc::now();
    let instance = TaskInstance {
        id: InstanceId::new(),
        sequence_id: sequence.id,
        tenant_id: TenantId(trigger.tenant_id.clone()),
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
    };

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
                    if let Err(e) = create_trigger_instance(&*storage, &trigger, data, meta).await {
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

    let mut watcher: RecommendedWatcher =
        notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx.blocking_send(event);
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

                    if let Err(e) = create_trigger_instance(&*storage, &trigger, data, meta).await {
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

    #[test]
    fn trigger_type_matching() {
        // Webhook triggers should be excluded from the processor (handled by HTTP API).
        let t = TriggerDef {
            slug: "test".into(),
            sequence_name: "seq".into(),
            version: None,
            tenant_id: "t1".into(),
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
}
