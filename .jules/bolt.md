## 2025-02-09 - [Zero-Allocation Batch Query Binding]
**Learning:** `sqlx::QueryBuilder::push_values` accepts collections of references, meaning we don't need to clone `String` or deep-clone `serde_json::Value` objects simply to build a query chunk. In `batch_save` operations, deep cloning JSON payloads into `Vec` buffers was creating massive memory allocation overhead.
**Action:** Always map elements to references (`&str`, `&serde_json::Value`) when building temporary `Vec` buffers for `push_values`, rather than using owned objects.

## 2025-05-16 - [DashMap Lock Contention & Key Allocation]
**Learning:** In highly concurrent paths like circuit breakers, allocating a new String key and acquiring a full `DashMap` entry lock via `.entry(key)` simply to update an existing breaker is extremely expensive. The previous approach allocated a String for every single failure or initialization check and acquired a lock that blocked other updates to the same shard.
**Action:** Use `.get_mut(q)` with a zero-allocation reference key first to update existing entries, falling back to allocating a Key and using `.entry(key)` ONLY when initialization is required.

## 2025-05-18 - [QueryBuilder String Reference Binding Lifetimes]
**Learning:** `sqlx::QueryBuilder::push_values` allows mapping values from a chunk directly using references `&val` instead of cloning `val.clone()`, even for `String` fields, because the iterator yields elements whose lifetimes are correctly bound to the execution scope. However, for dynamically constructed strings (like formatting a JSON fallback payload in SQLite telemetry), if the generated string isn't statically owned by the mapped structure, it MUST be fully evaluated (e.g. `event.payload.to_string()`) because temporary references will not satisfy the required `'1` lifetime tied to the `QueryBuilder`.
**Action:** When using `push_bind` inside `push_values` to optimize away `.clone()`, ensure all strings passed as references are strictly owned by the iterated items (`chunk`). If a string needs to be dynamically generated during the iteration, you must allocate it as an owned string.

## 2025-05-18 - [Zero-Allocation Batch Query Parameters]
**Learning:** `sqlx` in both Postgres (via `ANY($1)`) and SQLite (via `QueryBuilder::separated`) natively supports binding array parameters as `&[&str]`. Using `&[String]` in trait definitions forces callers to allocate a new `Vec<String>` and clone every string simply to satisfy the signature. In `orch8-engine/src/scheduler.rs:enforce_concurrency_limits`, this resulted in unnecessary String allocations on the hot path for every distinct concurrency key processed.
**Action:** When designing trait methods in `orch8-storage` that accept a list of strings for batch operations (like `count_running_by_concurrency_keys`), always use `&[&str]` instead of `&[String]`. This allows callers to map `HashMap` keys using `.copied().collect::<Vec<&str>>()` without any heap allocations for the strings themselves.

## 2025-10-23 - [Zero-Allocation JSON Value Ownership Transfer]
**Learning:** Functions like `save_interceptor_output` were taking `serde_json::Value` by reference, causing an internal `output.clone()` when allocating the `BlockOutput` struct. Since several callers possessed an explicitly cloned object already (e.g. `emit_on_signal` mutated and then referenced an already cloned params object), they were paying a double-clone tax for a single insert.
**Action:** Always map functions that eventually own heavy objects like `serde_json::Value` to accept them by value (`output: serde_json::Value`). This pushes the clone decision to the caller, preventing deep-cloning entirely where the caller already has ownership or is generating the object dynamically.

## 2025-10-24 - [Avoid Deep Cloning Large Structs in Filter Pipelines]
**Learning:** In the `claim_due` hot path across all database backends, an earlier implementation built an index of elements to exclude and then filtered the candidate slice by allocating a new `Vec` and calling `.clone()` on every retained `TaskInstance`. Since `TaskInstance` holds deep `serde_json::Value` trees (like `context`), this created massive, unnecessary memory allocation and deep-copy overhead simply to trim a few excluded elements.
**Action:** When filtering temporary vectors containing deep structs on a hot path, pass ownership of the `Vec` into the filtering function and use `into_iter` to selectively retain elements, avoiding any `clone()` calls.
