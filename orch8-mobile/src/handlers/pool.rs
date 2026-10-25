//! Drain Apple host temporaries before retirement returns physical capacity.
pub(super) fn run<T>(operation: impl FnOnce() -> T) -> T {
    #[cfg(target_vendor = "apple")]
    let _pool = apple::Pool::new();
    operation()
}

#[cfg(target_vendor = "apple")]
#[allow(unsafe_code)]
mod apple {
    use std::{ffi::c_void, marker::PhantomData, rc::Rc};

    #[link(name = "objc")]
    unsafe extern "C" {
        #[link_name = "objc_autoreleasePoolPush"]
        fn push() -> *mut c_void;
        #[link_name = "objc_autoreleasePoolPop"]
        fn pop(token: *mut c_void);
    }

    // The opaque token never leaves its creating thread or crosses an await.
    // Rc's marker makes the guard neither Send nor Sync.
    pub(super) struct Pool {
        token: *mut c_void,
        _thread: PhantomData<Rc<()>>,
    }
    impl Pool {
        pub(super) fn new() -> Self {
            // SAFETY: The Objective-C runtime creates a thread-local pool and
            // returns an opaque token. No Rust pointer is passed to it.
            let token = unsafe { push() };
            Self {
                token,
                _thread: PhantomData,
            }
        }
    }
    impl Drop for Pool {
        fn drop(&mut self) {
            // SAFETY: This is the exact token from push, popped once on the
            // same thread in stack order, including Rust unwind paths.
            unsafe {
                pop(self.token);
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::{cell::UnsafeCell, ffi::c_char};

        #[link(name = "Foundation", kind = "framework")]
        unsafe extern "C" {
            fn objc_getClass(name: *const c_char) -> *mut c_void;
            fn sel_registerName(name: *const c_char) -> *mut c_void;
            #[link_name = "objc_msgSend"]
            fn send(receiver: *mut c_void, selector: *mut c_void) -> *mut c_void;
            fn objc_autorelease(object: *mut c_void) -> *mut c_void;
            fn objc_initWeak(slot: *mut *mut c_void, object: *mut c_void) -> *mut c_void;
            fn objc_destroyWeak(slot: *mut *mut c_void);
        }

        struct Canary {
            slot: Box<UnsafeCell<*mut c_void>>,
        }
        impl Canary {
            fn new() -> Self {
                Self {
                    slot: Box::new(UnsafeCell::new(std::ptr::null_mut())),
                }
            }
            fn dead(&self) -> bool {
                // SAFETY: Each canary is thread-local. Objective-C only writes
                // the stable interior slot during calls on this same thread.
                unsafe { (*self.slot.get()).is_null() }
            }
            fn create(&mut self) {
                // SAFETY: NSObject.new has the no-argument object-return ABI
                // on supported Apple targets. The weak slot has a stable heap
                // address through destroyWeak; autorelease balances new's +1.
                unsafe {
                    let object = send(
                        objc_getClass(c"NSObject".as_ptr()),
                        sel_registerName(c"new".as_ptr()),
                    );
                    assert!(!object.is_null());
                    objc_initWeak(self.slot.get(), object);
                    objc_autorelease(object);
                }
                assert!(!self.dead());
            }
        }
        impl Drop for Canary {
            fn drop(&mut self) {
                // SAFETY: The slot stays allocated through this call and was
                // initialized once by create, which each test calls once.
                unsafe {
                    objc_destroyWeak(self.slot.get());
                }
            }
        }

        #[test]
        fn retirement_pool_drains_real_autoreleased_objects_before_return() {
            let mut canary = Canary::new();
            crate::handlers::pool::run(|| canary.create());
            assert!(
                canary.dead(),
                "autoreleased host object survived the retirement pool"
            );
        }

        #[test]
        fn retirement_pool_drains_real_objects_during_rust_unwind() {
            let mut canary = Canary::new();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                crate::handlers::pool::run(|| {
                    canary.create();
                    panic!("host drop panic");
                });
            }));
            assert!(result.is_err());
            assert!(canary.dead(), "host object survived pool unwind");
        }
    }
}

pub(super) fn dispose_panic(payload: Box<dyn std::any::Any + Send>) {
    let mut outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)));
    while let Err(payload) = outcome {
        outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)));
    }
}

pub(super) fn release<T>(value: T) {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value))) {
        dispose_panic(payload);
    }
}
