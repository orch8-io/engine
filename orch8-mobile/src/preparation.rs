//! Shared physical ownership for bounded remote preparation, including detached CPU work.
use crate::MobileError;
use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
pub(crate) fn budget() -> Arc<Semaphore> {
    static BUDGET: OnceLock<Arc<Semaphore>> = OnceLock::new();
    BUDGET.get_or_init(|| Arc::new(Semaphore::new(2))).clone()
}
pub(crate) struct Permit {
    budget: Arc<Semaphore>,
    _physical: OwnedSemaphorePermit,
}
impl Permit {
    pub(crate) fn acquire(budget: &Arc<Semaphore>) -> Result<Arc<Self>, MobileError> {
        let physical =
            budget
                .clone()
                .try_acquire_owned()
                .map_err(|_| MobileError::ResourceLimit {
                    detail: "scoped preparation capacity is occupied".into(),
                })?;
        Ok(Arc::new(Self {
            budget: budget.clone(),
            _physical: physical,
        }))
    }
    pub(crate) fn belongs_to(&self, budget: &Arc<Semaphore>) -> bool {
        Arc::ptr_eq(&self.budget, budget)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_process_budget_charges_physical_clones_and_rejects_foreign_permits() {
        let first = budget();
        let second = budget();
        assert!(Arc::ptr_eq(&first, &second));
        let one = Permit::acquire(&first).unwrap();
        let held = one.clone();
        let two = Permit::acquire(&second).unwrap();
        assert!(Permit::acquire(&first).is_err());
        drop(one);
        assert!(Permit::acquire(&first).is_err());
        assert!(held.belongs_to(&first));
        assert!(!held.belongs_to(&Arc::new(Semaphore::new(2))));
        drop(held);
        drop(two);
        assert_eq!(first.available_permits(), 2);
    }
}
