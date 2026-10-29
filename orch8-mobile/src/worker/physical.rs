//! Physical exclusion across engines, claim generations and business retries.
use super::claims::ClaimKey;
use orch8_engine::remote_worker::RemoteTask;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex, OnceLock},
};

#[derive(Clone, PartialEq, Eq, Hash)]
struct TaskScope {
    backend: String,
    task: uuid::Uuid,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct InvocationScope {
    backend: String,
    instance: uuid::Uuid,
    block: String,
}

#[derive(Default)]
struct State {
    tasks: HashSet<TaskScope>,
    invocations: HashSet<InvocationScope>,
}

type Owners = Mutex<State>;

pub(super) struct Reservation {
    task: TaskScope,
    invocation: InvocationScope,
    owners: &'static Owners,
}

impl Reservation {
    pub(super) fn reserve(key: &ClaimKey, task: &RemoteTask) -> Option<Arc<Self>> {
        static OWNERS: OnceLock<Owners> = OnceLock::new();
        if key.task_id != task.id || key.claim_epoch != task.claim_epoch {
            return None;
        }
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
        let invocation = InvocationScope {
            backend: scope.backend.clone(),
            instance: task.instance_id,
            block: task.block_id.clone(),
        };
        let owners = OWNERS.get_or_init(Mutex::default);
        let mut state = owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.tasks.contains(&scope) || state.invocations.contains(&invocation) {
            return None;
        }
        state.tasks.insert(scope.clone());
        state.invocations.insert(invocation.clone());
        drop(state);
        Some(Arc::new(Self {
            task: scope,
            invocation,
            owners,
        }))
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut state = self
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.tasks.remove(&self.task);
        state.invocations.remove(&self.invocation);
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

    fn task(key: &ClaimKey) -> RemoteTask {
        serde_json::from_value(serde_json::json!({
            "id":key.task_id,"instance_id":uuid::Uuid::new_v4(),
            "block_id":"step","handler_name":"scan","claim_epoch":key.claim_epoch
        }))
        .unwrap()
    }

    #[test]
    fn canonical_backend_and_task_exclude_new_workers_and_epochs_until_last_owner_drops() {
        let mut key = key();
        let mut task = task(&key);
        let owner = Reservation::reserve(&key, &task).unwrap();
        let copy = owner.clone();
        key.api_base = "https://CONTROL.example:443/api/v1/".into();
        key.worker_id = "another-worker".into();
        key.claim_epoch = u64::MAX;
        task.claim_epoch = key.claim_epoch;
        assert!(Reservation::reserve(&key, &task).is_none());
        drop(owner);
        assert!(Reservation::reserve(&key, &task).is_none());
        drop(copy);
        assert!(Reservation::reserve(&key, &task).is_some());
    }

    #[test]
    fn other_backends_and_tasks_remain_independent() {
        let mut key = key();
        let mut task = task(&key);
        let _first = Reservation::reserve(&key, &task).unwrap();
        key.api_base = "https://other.example/api/v1".into();
        let _second = Reservation::reserve(&key, &task).unwrap();
        key.api_base = "https://control.example/api/v1".into();
        key.task_id = uuid::Uuid::new_v4();
        task.id = key.task_id;
        assert!(Reservation::reserve(&key, &task).is_none());
        task.block_id = "independent-step".into();
        let _other_block = Reservation::reserve(&key, &task).unwrap();
        key.task_id = uuid::Uuid::new_v4();
        task.id = key.task_id;
        task.instance_id = uuid::Uuid::new_v4();
        assert!(Reservation::reserve(&key, &task).is_some());
    }

    #[test]
    fn malformed_or_credential_bearing_scope_never_creates_a_reservation() {
        let mut key = key();
        let task = task(&key);
        for url in [
            "not a URL",
            "file:///api/v1",
            "https://user:secret@control.example/api/v1",
            "https://control.example/api/v1?token=secret",
            "https://control.example/api/v1#fragment",
        ] {
            key.api_base = url.into();
            assert!(Reservation::reserve(&key, &task).is_none());
        }
    }

    #[test]
    fn duplicate_task_cannot_escape_by_changing_instance_or_block() {
        let key = key();
        let mut task = task(&key);
        let _owner = Reservation::reserve(&key, &task).unwrap();
        task.instance_id = uuid::Uuid::new_v4();
        task.block_id = "changed".into();
        assert!(Reservation::reserve(&key, &task).is_none());
    }

    #[test]
    fn mismatched_claim_identity_does_not_reserve_either_domain() {
        let key = key();
        let mut task = task(&key);
        task.claim_epoch += 1;
        assert!(Reservation::reserve(&key, &task).is_none());
        task.claim_epoch = key.claim_epoch;
        let original_id = task.id;
        task.id = uuid::Uuid::new_v4();
        assert!(Reservation::reserve(&key, &task).is_none());
        task.id = original_id;
        assert!(Reservation::reserve(&key, &task).is_some());
    }
}
