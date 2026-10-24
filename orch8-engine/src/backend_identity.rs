//! Process-local identity for backend-owned volatile state. A weak reference
//! keeps allocation identity distinct through retirement without retaining the
//! backend, its connections, or its runtime. Never persisted or used as auth.
use orch8_storage::StorageBackend;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Weak};

#[derive(Clone)]
pub(crate) struct BackendIdentity(Weak<dyn StorageBackend>);

impl BackendIdentity {
    pub(crate) fn new(storage: &Arc<dyn StorageBackend>) -> Self {
        Self(Arc::downgrade(storage))
    }
    pub(crate) fn is_live(&self) -> bool {
        self.0.strong_count() != 0
    }
}
impl PartialEq for BackendIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
}
impl Eq for BackendIdentity {}
impl Hash for BackendIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // The pointer is only an opaque hash key. Weak ownership prevents
        // allocation reuse while this identity exists; no pointer is read.
        std::ptr::hash(self.0.as_ptr().cast::<()>(), state);
    }
}
