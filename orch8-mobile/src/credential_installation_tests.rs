use super::*;
use std::sync::atomic::AtomicUsize;

struct Provider {
    getters: Arc<AtomicUsize>,
}
impl TokenProvider for Provider {
    fn current_token(&self) -> String {
        self.getters.fetch_add(1, Ordering::SeqCst);
        "dst_sync".into()
    }
    fn refresh_token(&self) -> Result<String, crate::MobileError> {
        Ok("dst_refreshed".into())
    }
}
fn provider() -> (Arc<dyn TokenProvider>, Arc<AtomicUsize>) {
    let getters = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(Provider {
            getters: Arc::clone(&getters),
        }),
        getters,
    )
}

#[test]
fn reservation_clears_old_authority_without_invoking_host_getters() {
    let credential = Credential::new("old_operator_key".into());
    let (provider, getters) = provider();
    let ticket = credential.begin_installation(provider).unwrap();
    assert_eq!(getters.load(Ordering::SeqCst), 0);
    assert!(credential.current().is_empty());
    assert!(credential.request_snapshot().is_none());
    assert!(!credential.complete_installation("invalid", "dst_wrong".into()));
    assert!(!credential.complete_installation(&ticket, " \t\n".into()));
    assert!(credential.complete_installation(&ticket, "dst_initial".into()));
    assert_eq!(credential.current(), "dst_initial");
}

#[test]
fn reservation_tickets_are_bound_to_one_engine_and_current_owner() {
    let credential = Credential::new(String::new());
    let other = Credential::new(String::new());
    let first = credential.begin_installation(provider().0).unwrap();
    let second = credential.begin_installation(provider().0).unwrap();
    assert!(!other.complete_installation(&second, "dst_cross_engine".into()));
    assert!(!other.cancel_installation(&second));
    assert!(!credential.complete_installation(&first, "dst_old".into()));
    assert!(!credential.cancel_installation(&first));
    assert!(credential.complete_installation(&second, "dst_new".into()));
    credential.set_provider(provider().0);
    assert!(!credential.complete_installation(&second, "dst_old".into()));
    assert!(!credential.cancel_installation(&second));
    assert_eq!(credential.current(), "dst_sync");
}

#[tokio::test]
async fn initial_completion_never_rolls_back_an_owned_refresh() {
    let credential = Credential::new(String::new());
    let ticket = credential.begin_installation(provider().0).unwrap();
    let initial_generation = credential.snapshot().generation;
    assert!(credential.refresh_generation(&initial_generation).await);
    let refreshed_generation = credential.snapshot().generation;
    assert_eq!(credential.current(), "dst_refreshed");
    assert!(credential.complete_installation(&ticket, "dst_older_initial".into()));
    assert_eq!(credential.current(), "dst_refreshed");
    assert_eq!(credential.snapshot().generation, refreshed_generation);
}

#[tokio::test]
async fn cancelled_reservation_never_downgrades_to_anonymous_or_invokes_payload_builder() {
    let credential = Credential::new(String::new());
    assert!(
        credential.request_snapshot().is_some(),
        "legacy anonymous mode is explicit"
    );
    let ticket = credential.begin_installation(provider().0).unwrap();
    assert!(credential.cancel_installation(&ticket));
    assert!(!credential.cancel_installation(&ticket));
    assert!(!credential.complete_installation(&ticket, "dst_late".into()));
    assert!(credential.request_snapshot().is_none());
    let built = AtomicBool::new(false);
    let result = credential
        .send(|_| {
            built.store(true, Ordering::SeqCst);
            reqwest::Client::new().get("https://example.com")
        })
        .await;
    assert!(matches!(
        result,
        Err(orch8_engine::remote_worker::LeaseRequestError::AuthorizationUnavailable)
    ));
    assert!(!built.load(Ordering::SeqCst));
    let next = credential.begin_installation(provider().0).unwrap();
    assert!(credential.complete_installation(&next, "dst_recovered".into()));
}

#[test]
fn cancellation_can_retire_a_just_completed_owner_but_not_its_successor() {
    let credential = Credential::new(String::new());
    let ticket = credential.begin_installation(provider().0).unwrap();
    assert!(credential.complete_installation(&ticket, "dst_initial".into()));
    assert!(credential.complete_installation(&ticket, "dst_replayed".into()));
    assert_eq!(credential.current(), "dst_initial");
    assert!(credential.cancel_installation(&ticket));
    assert!(credential.current().is_empty());
    assert!(credential.request_snapshot().is_none());
    let next = credential.begin_installation(provider().0).unwrap();
    assert!(credential.complete_installation(&next, "dst_next".into()));
    assert!(!credential.cancel_installation(&ticket));
    assert_eq!(credential.current(), "dst_next");
}

#[test]
fn shutdown_fences_reservations_completion_and_cancellation() {
    let credential = Credential::new("bootstrap".into());
    let ticket = credential.begin_installation(provider().0).unwrap();
    drop(credential.close());
    assert!(!credential.complete_installation(&ticket, "dst_late".into()));
    assert!(!credential.cancel_installation(&ticket));
    assert!(credential.begin_installation(provider().0).is_none());
    assert!(credential.request_snapshot().is_none());
}
