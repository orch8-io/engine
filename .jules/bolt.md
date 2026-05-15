## 2025-02-09 - [Zero-Allocation Batch Query Binding]
**Learning:** `sqlx::QueryBuilder::push_values` accepts collections of references, meaning we don't need to clone `String` or deep-clone `serde_json::Value` objects simply to build a query chunk. In `batch_save` operations, deep cloning JSON payloads into `Vec` buffers was creating massive memory allocation overhead.
**Action:** Always map elements to references (`&str`, `&serde_json::Value`) when building temporary `Vec` buffers for `push_values`, rather than using owned objects.
