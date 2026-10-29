//! Same-task physical exclusion across independent engines in this process.
use super::claims::ClaimKey;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex, OnceLock},
};

#[derive(Clone, PartialEq, Eq, Hash)]
struct TaskScope {
    backend: String,
    task: uuid::Uuid,
}

type Owners = Mutex<HashSet<TaskScope>>;

pub(super) struct Reservation {
    scope: TaskScope,
    owners: &'static Owners,
}

impl Reservation {
    pub(super) fn reserve(key: &ClaimKey) -> Option<Arc<Self>> {
        static OWNERS: OnceLock<Owners> = OnceLock::new();
        let url = reqwest::Url::parse(&key.api_base).ok()?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return None;
        }
        let scope = TaskScope {
            backend: url.as_str().trim_end_matches('/').to_owned(),
            task: key.task_id,
        };
        let owners = OWNERS.get_or_init(Mutex::default);
        let inserted = owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(scope.clone());
        inserted.then(|| Arc::new(Self { scope, owners }))
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.scope);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ClaimKey {
        ClaimKey {
            api_base: "https://control.example/api/v1".into(),
            task_id: uuid::Uuid::new_v4(),
            claim_epoch: 7,
            worker_id: "first-worker".into(),
        }
    }

    #[test]
    fn canonical_backend_and_task_exclude_new_workers_and_epochs_until_last_owner_drops() {
        let mut key = key();
        let owner = Reservation::reserve(&key).unwrap();
        let copy = owner.clone();
        key.api_base = "https://CONTROL.example:443/api/v1/".into();
        key.worker_id = "another-worker".into();
        key.claim_epoch = u64::MAX;
        assert!(Reservation::reserve(&key).is_none());
        drop(owner);
        assert!(Reservation::reserve(&key).is_none());
        drop(copy);
        assert!(Reservation::reserve(&key).is_some());
    }

    #[test]
    fn other_backends_and_tasks_remain_independent() {
        let mut key = key();
        let _first = Reservation::reserve(&key).unwrap();
        key.api_base = "https://other.example/api/v1".into();
        let _second = Reservation::reserve(&key).unwrap();
        key.api_base = "https://control.example/api/v1".into();
        key.task_id = uuid::Uuid::new_v4();
        assert!(Reservation::reserve(&key).is_some());
    }

    #[test]
    fn malformed_or_credential_bearing_scope_never_creates_a_reservation() {
        let mut key = key();
        for url in [
            "not a URL",
            "file:///api/v1",
            "https://user:secret@control.example/api/v1",
            "https://control.example/api/v1?token=secret",
            "https://control.example/api/v1#fragment",
        ] {
            key.api_base = url.into();
            assert!(Reservation::reserve(&key).is_none());
        }
    }
}
