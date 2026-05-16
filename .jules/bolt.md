## 2025-02-09 - [Zero-Allocation Batch Query Binding]
**Learning:** `sqlx::QueryBuilder::push_values` accepts collections of references, meaning we don't need to clone `String` or deep-clone `serde_json::Value` objects simply to build a query chunk. In `batch_save` operations, deep cloning JSON payloads into `Vec` buffers was creating massive memory allocation overhead.
**Action:** Always map elements to references (`&str`, `&serde_json::Value`) when building temporary `Vec` buffers for `push_values`, rather than using owned objects.

## 2025-05-16 - [DashMap Lock Contention & Key Allocation]
**Learning:** In highly concurrent paths like circuit breakers, allocating a new String key and acquiring a full `DashMap` entry lock via `.entry(key)` simply to update an existing breaker is extremely expensive. The previous approach allocated a String for every single failure or initialization check and acquired a lock that blocked other updates to the same shard.
**Action:** Use `.get_mut(q)` with a zero-allocation reference key first to update existing entries, falling back to allocating a Key and using `.entry(key)` ONLY when initialization is required.
