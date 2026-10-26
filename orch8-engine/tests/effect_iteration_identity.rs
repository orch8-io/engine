//! Provider idempotency distinguishes loop intents; explicit guards retain history.
use chrono::Utc;
use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::testing::{
    backends::TestBackend,
    fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    continuity::{EffectId, EffectKind},
    continuity_advanced::{EvidenceStatus, InvariantId, InvariantRule, WorkflowInvariant},
    ids::BlockId,
    instance::InstanceState,
    sequence::{BlockDefinition, ForEachDef},
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};
mod common;

#[derive(Default)]
struct FakeProvider {
    seen: Vec<EffectId>,
    unique: HashSet<EffectId>,
    applied: Vec<Value>,
}

fn provider_registry() -> (HandlerRegistry, Arc<Mutex<FakeProvider>>) {
    let provider = Arc::new(Mutex::new(FakeProvider::default()));
    let state = provider.clone();
    let mut registry = HandlerRegistry::new();
    registry.register_with_deferred_effect_admission("native-charge", move |ctx| {
        let state = state.clone();
        async move {
            let admission = ctx.effect_admission.as_ref().unwrap();
            let id = admission
                .effect_id_for(&ctx.tenant_id, ctx.instance_id, &ctx.block_id, ctx.attempt)
                .unwrap();
            admission.admit().await?;
            let mut state = state.lock().unwrap();
            state.seen.push(id);
            if state.unique.insert(id) {
                state.applied.push(ctx.context.data["item"].clone());
            }
            Ok(json!({"receipt_id": id.to_string()}))
        }
    });
    (registry, provider)
}

async fn assert_guard_failure(
    storage: &dyn orch8_storage::StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
) {
    let owner = storage
        .get_continuity_execution_by_instance(&instance.tenant_id, instance.id)
        .await
        .unwrap()
        .unwrap();
    let evidence = storage
        .list_invariant_results(&instance.tenant_id, owner.continuity_id, 100)
        .await
        .unwrap();
    assert!(
        evidence
            .iter()
            .any(|result| result.status == EvidenceStatus::Fail),
        "duplicate must record durable invariant failure evidence"
    );
}

async fn run_loop(b: &TestBackend, items: Vec<Value>, guarded: bool) {
    let sequence = SequenceBuilder::new("effect-iteration-identity")
        .block(BlockDefinition::ForEach(Box::new(ForEachDef {
            id: BlockId::new("each"),
            collection: "items".into(),
            item_var: "item".into(),
            body: vec![common::mk_step("charge", "native-charge")],
            max_iterations: 10,
            retain_iterations: None,
        })))
        .build();
    b.storage.create_sequence(&sequence).await.unwrap();
    if guarded {
        b.storage
            .create_workflow_invariant(&WorkflowInvariant {
                id: InvariantId::new(),
                tenant_id: sequence.tenant_id.clone(),
                sequence_id: sequence.id,
                sequence_version: Some(sequence.version),
                name: "one charge".into(),
                rule: InvariantRule::EffectAtMostOnce {
                    kind: EffectKind::Worker,
                },
                commit_guard: true,
                enabled: true,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
    }
    let instance = InstanceBuilder::for_sequence(&sequence)
        .data(json!({"items": items}))
        .due_now()
        .build();
    b.storage.create_instance(&instance).await.unwrap();
    let (registry, provider) = provider_registry();
    let storage = b.storage.clone();
    let registry = Arc::new(registry);
    let sem = common::semaphore(4);
    let config = common::default_config();
    let cache = common::cache();
    let cancel = tokio_util::sync::CancellationToken::new();
    for _ in 0..40 {
        orch8_engine::scheduler::tick_once(&storage, &registry, &sem, &config, &cache, &cancel)
            .await
            .unwrap();
        if storage
            .get_instance(instance.id)
            .await
            .unwrap()
            .unwrap()
            .state
            .is_terminal()
        {
            break;
        }
    }
    let state = storage
        .get_instance(instance.id)
        .await
        .unwrap()
        .unwrap()
        .state;
    if guarded {
        assert_guard_failure(storage.as_ref(), &instance).await;
    }
    let state_of_provider = provider.lock().unwrap();
    let ids = &state_of_provider.seen;
    if guarded {
        assert_ne!(
            state,
            InstanceState::Completed,
            "loop reset erased the configured commit guard's evidence"
        );
        assert_eq!(
            ids.len(),
            1,
            "guard allowed a duplicate provider invocation"
        );
        assert_eq!(state_of_provider.applied, vec![items[0].clone()]);
    } else {
        assert_eq!(state, InstanceState::Completed);
        assert_eq!(ids.len(), items.len());
        assert_ne!(
            ids[0], ids[1],
            "new loop intent reused a prior provider idempotency identity"
        );
        assert_eq!(
            state_of_provider.applied, items,
            "provider suppressed legitimate loop work"
        );
    }
}

orch8_storage::backend_test!(async fn foreach_effects_have_distinct_provider_identities(b) {
    run_loop(&b, vec![json!(10), json!(20)], false).await;
});
orch8_storage::backend_test!(async fn identical_loop_items_are_separate_intents(b) {
    run_loop(&b, vec![json!(10), json!(10)], false).await;
});
orch8_storage::backend_test!(async fn loop_reset_preserves_explicit_at_most_once_guards(b) {
    run_loop(&b, vec![json!(10), json!(10)], true).await;
});
