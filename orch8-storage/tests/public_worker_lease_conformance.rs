use orch8_storage::{backend_test, conformance::run_worker_lease_conformance};

backend_test!(async fn public_worker_lease_conformance(b) {
    let report = run_worker_lease_conformance(b.storage.as_ref()).await.unwrap();
    assert_eq!(report.checks.len(), 6);
});
