//! A stale worker must not change a replacement worker's Running instance.

use chrono::Utc;
use orch8_storage::backend_test;
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
use orch8_types::instance::InstanceState;
use orch8_types::webhook_outbox::{WebhookOutboxEntry, WebhookOutboxStatus};

fn webhook(instance_id: orch8_types::ids::InstanceId) -> WebhookOutboxEntry {
    WebhookOutboxEntry {
        id: uuid::Uuid::now_v7(),
        url: "https://hooks.example.test/deliver".into(),
        event_type: "instance.failed".into(),
        instance_id: Some(instance_id.into_uuid()),
        payload: serde_json::json!({}),
        attempts: 0,
        last_error: None,
        created_at: Utc::now(),
        delivery_id: None,
        status: WebhookOutboxStatus::Pending,
        next_attempt_at: None,
        claimed_at: None,
    }
}

backend_test!(
    async fn stale_claim_cannot_change_new_owner(b) {
        let seq = SequenceBuilder::new("tenant").step("work", "noop").build();
        b.storage.create_sequence(&seq).await.unwrap();
        let instance = InstanceBuilder::for_sequence(&seq).due_now().build();
        b.storage.create_instance(&instance).await.unwrap();
        // A fixed future scheduling clock must neither reuse the claim token
        // nor push lease age into the future.
        let claim_time = Utc::now() + chrono::Duration::minutes(1) + chrono::Duration::nanoseconds(123);

        let first = b
            .storage
            .claim_due_instances(claim_time, 1, 0)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let first_token = first.updated_at;
        assert!(
            first_token <= Utc::now() + chrono::Duration::seconds(5),
            "{}: lease timestamp followed the virtual due clock instead of wall/database time",
            b.name()
        );
        assert!(
            b.storage
                .conditional_update_claimed_instance_state(
                    instance.id,
                    first_token,
                    InstanceState::Running,
                    InstanceState::Scheduled,
                    Some(claim_time),
                )
                .await
                .unwrap()
        );

        let second = b
            .storage
            .claim_due_instances(claim_time, 1, 0)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_ne!(first_token, second.updated_at);

        for next in [InstanceState::Failed, InstanceState::Scheduled] {
            assert!(
                !b.storage
                    .conditional_update_claimed_instance_state(
                        instance.id,
                        first_token,
                        InstanceState::Running,
                        next,
                        None,
                    )
                    .await
                    .unwrap(),
                "{}: stale claim changed new owner to {next:?}",
                b.name()
            );
        }
        assert_eq!(
            b.storage.get_instance(instance.id).await.unwrap().unwrap().state,
            InstanceState::Running
        );
        let stale_event = webhook(instance.id);
        assert!(
            !b.storage
                .conditional_update_claimed_instance_state_with_outbox(
                    instance.id,
                    first_token,
                    InstanceState::Running,
                    InstanceState::Failed,
                    None,
                    std::slice::from_ref(&stale_event),
                )
                .await
                .unwrap()
        );
        assert!(b.storage.get_webhook_outbox(stale_event.id).await.unwrap().is_none());
        let current_event = webhook(instance.id);
        assert!(
            b.storage
                .conditional_update_claimed_instance_state_with_outbox(
                    instance.id,
                    second.updated_at,
                    InstanceState::Running,
                    InstanceState::Failed,
                    None,
                    std::slice::from_ref(&current_event),
                )
                .await
                .unwrap()
        );
        assert!(b.storage.get_webhook_outbox(current_event.id).await.unwrap().is_some());
    }
);

backend_test!(
    async fn explicitly_created_running_row_has_a_claim_token(b) {
        let seq = SequenceBuilder::new("tenant").step("work", "noop").build();
        b.storage.create_sequence(&seq).await.unwrap();
        for batch in [false, true] {
            let instance = InstanceBuilder::for_sequence(&seq)
                .state(InstanceState::Running)
                .build();
            if batch {
                b.storage
                    .create_instances_batch(std::slice::from_ref(&instance))
                    .await
                    .unwrap();
            } else {
                b.storage.create_instance(&instance).await.unwrap();
            }
            let stored = b.storage.get_instance(instance.id).await.unwrap().unwrap();
            assert!(
                b.storage
                    .conditional_update_claimed_instance_state(
                        instance.id,
                        stored.updated_at,
                        InstanceState::Running,
                        InstanceState::Failed,
                        None,
                    )
                    .await
                    .unwrap(),
                "{}: explicitly created Running instance (batch={batch}) has no owner token",
                b.name()
            );
        }
    }
);
