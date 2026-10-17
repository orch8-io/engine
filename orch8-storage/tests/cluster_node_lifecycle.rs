//! Cluster node registry lifecycle on every backend.
//!
//! L2-16: a node the stale-node reaper flipped to `stopped` (stall or
//! partition longer than the threshold) re-registers itself on its next
//! heartbeat; a node that deregistered gracefully stays stopped.

use std::time::Duration;

use chrono::Utc;
use uuid::Uuid;

use orch8_storage::backend_test;
use orch8_types::cluster::{ClusterNode, NodeStatus};

fn node() -> ClusterNode {
    let now = Utc::now();
    ClusterNode {
        id: Uuid::new_v4(),
        name: "n".into(),
        status: NodeStatus::Active,
        registered_at: now,
        last_heartbeat_at: now,
        drain: false,
        drain_started_at: None,
        stopped_at: None,
        capabilities_withdrawn: false,
        execution_handoff_evidence: None,
    }
}

backend_test!(
    async fn reaped_node_comes_back_on_heartbeat(b) {
        let n = node();
        b.storage.register_node(&n).await.unwrap();
        b.raw
            .backdate(
                "cluster_nodes",
                "last_heartbeat_at",
                "id",
                &n.id.to_string(),
                Utc::now() - chrono::Duration::hours(1),
            )
            .await;
        assert_eq!(b.storage.reap_stale_nodes(Duration::from_secs(60)).await.unwrap(), 1);
        let listed = b.storage.list_nodes().await.unwrap();
        assert_eq!(listed.iter().find(|x| x.id == n.id).unwrap().status, NodeStatus::Stopped);

        b.storage.heartbeat_node(n.id).await.unwrap();

        let after = b.storage.list_nodes().await.unwrap();
        let after = after.iter().find(|x| x.id == n.id).unwrap();
        assert_eq!(after.status, NodeStatus::Active, "heartbeat re-registers a reaped node");
        assert!(after.stopped_at.is_none());
        // And it is no longer a reaper candidate.
        assert_eq!(b.storage.reap_stale_nodes(Duration::from_secs(60)).await.unwrap(), 0);
    }
);

backend_test!(
    async fn deregistered_node_stays_stopped_on_late_heartbeat(b) {
        let n = node();
        b.storage.register_node(&n).await.unwrap();
        b.storage.deregister_node(n.id).await.unwrap();
        b.storage.heartbeat_node(n.id).await.unwrap();
        let listed = b.storage.list_nodes().await.unwrap();
        assert_eq!(listed.iter().find(|x| x.id == n.id).unwrap().status, NodeStatus::Stopped);
    }
);
