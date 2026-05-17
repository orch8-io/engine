## 2025-02-09 - [Zero-Allocation Batch Query Binding]
**Learning:** `sqlx::QueryBuilder::push_values` accepts collections of references, meaning we don't need to clone `String` or deep-clone `serde_json::Value` objects simply to build a query chunk. In `batch_save` operations, deep cloning JSON payloads into `Vec` buffers was creating massive memory allocation overhead.
**Action:** Always map elements to references (`&str`, `&serde_json::Value`) when building temporary `Vec` buffers for `push_values`, rather than using owned objects.
## 2026-05-17 - [O(N²) HashMap Allocation in Tree Traversal]
**Learning:** Instantiating a  inside a loop that iterates over the same collection (e.g., building a parent map for an execution tree on every iteration) results in severe O(N²) time complexity and massive memory allocation overhead.
**Action:** When performing repeated lookups against a static collection (like an execution tree), sort the collection into a  upfront and use  for O(log N) zero-allocation lookups.
## 2025-02-09 - [O(N²) HashMap Allocation in Tree Traversal]
**Learning:** Instantiating a `HashMap` inside a loop that iterates over the same collection (e.g., building a parent map for an execution tree on every iteration) results in severe O(N²) time complexity and massive memory allocation overhead.
**Action:** When performing repeated lookups against a static collection (like an execution tree), sort the collection into a `Vec` upfront and use `.binary_search_by_key()` for O(log N) zero-allocation lookups.
