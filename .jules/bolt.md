## 2025-02-09 - [Zero-Allocation Batch Query Binding]
**Learning:** `sqlx::QueryBuilder::push_values` accepts collections of references, meaning we don't need to clone `String` or deep-clone `serde_json::Value` objects simply to build a query chunk. In `batch_save` operations, deep cloning JSON payloads into `Vec` buffers was creating massive memory allocation overhead.
**Action:** Always map elements to references (`&str`, `&serde_json::Value`) when building temporary `Vec` buffers for `push_values`, rather than using owned objects.

## 2025-05-16 - [DashMap Lock Contention & Key Allocation]
**Learning:** In highly concurrent paths like circuit breakers, allocating a new String key and acquiring a full `DashMap` entry lock via `.entry(key)` simply to update an existing breaker is extremely expensive. The previous approach allocated a String for every single failure or initialization check and acquired a lock that blocked other updates to the same shard.
**Action:** Use `.get_mut(q)` with a zero-allocation reference key first to update existing entries, falling back to allocating a Key and using `.entry(key)` ONLY when initialization is required.

## 2025-05-18 - [QueryBuilder String Reference Binding Lifetimes]
**Learning:** `sqlx::QueryBuilder::push_values` allows mapping values from a chunk directly using references `&val` instead of cloning `val.clone()`, even for `String` fields, because the iterator yields elements whose lifetimes are correctly bound to the execution scope. However, for dynamically constructed strings (like formatting a JSON fallback payload in SQLite telemetry), if the generated string isn't statically owned by the mapped structure, it MUST be fully evaluated (e.g. `event.payload.to_string()`) because temporary references will not satisfy the required `'1` lifetime tied to the `QueryBuilder`.
**Action:** When using `push_bind` inside `push_values` to optimize away `.clone()`, ensure all strings passed as references are strictly owned by the iterated items (`chunk`). If a string needs to be dynamically generated during the iteration, you must allocate it as an owned string.
