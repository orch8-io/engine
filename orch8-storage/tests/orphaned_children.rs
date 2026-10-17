//! `list_orphaned_children` (L3-07): live children whose parent already failed
//! or was cancelled, on every backend.

use orch8_storage::backend_test;
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
use orch8_types::instance::InstanceState;

backend_test!(
    async fn lists_only_live_children_of_failed_or_cancelled_parents(b) {
        let seq = SequenceBuilder::new("t").step("s", "noop").build();
        b.storage.create_sequence(&seq).await.unwrap();

        let mk = |parent_state: InstanceState| {
            InstanceBuilder::for_sequence(&seq).state(parent_state).build()
        };
        let cancelled_parent = mk(InstanceState::Cancelled);
        let failed_parent = mk(InstanceState::Failed);
        let running_parent = mk(InstanceState::Running);
        let completed_parent = mk(InstanceState::Completed);
        for p in [&cancelled_parent, &failed_parent, &running_parent, &completed_parent] {
            b.storage.create_instance(p).await.unwrap();
        }

        let child = |parent: &orch8_types::instance::TaskInstance, state: InstanceState| {
            InstanceBuilder::for_sequence(&seq)
                .parent(parent.id)
                .state(state)
                .build()
        };
        // Orphans: every live state under a dead parent.
        let o1 = child(&cancelled_parent, InstanceState::Waiting);
        let o2 = child(&failed_parent, InstanceState::Scheduled);
        let o3 = child(&cancelled_parent, InstanceState::Running);
        let o4 = child(&failed_parent, InstanceState::Paused);
        // Not orphans.
        let done = child(&cancelled_parent, InstanceState::Completed);
        let already_cancelled = child(&failed_parent, InstanceState::Cancelled);
        let healthy = child(&running_parent, InstanceState::Waiting);
        let after_completion = child(&completed_parent, InstanceState::Waiting);
        let root = InstanceBuilder::for_sequence(&seq)
            .state(InstanceState::Waiting)
            .build();
        for i in [&o1, &o2, &o3, &o4, &done, &already_cancelled, &healthy, &after_completion, &root] {
            b.storage.create_instance(i).await.unwrap();
        }

        let mut got = b.storage.list_orphaned_children(100).await.unwrap();
        got.sort();
        let mut want = vec![o1.id, o2.id, o3.id, o4.id];
        want.sort();
        assert_eq!(got, want, "{}", b.name());

        // `limit` caps the page.
        let page = b.storage.list_orphaned_children(2).await.unwrap();
        assert_eq!(page.len(), 2, "{}", b.name());

        // Draining the set empties it.
        for id in want {
            b.storage
                .update_instance_state(id, InstanceState::Cancelled, None)
                .await
                .unwrap();
        }
        assert!(b.storage.list_orphaned_children(100).await.unwrap().is_empty());
    }
);
