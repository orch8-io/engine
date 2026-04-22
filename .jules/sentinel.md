## 2025-02-15 - [Refactored format! out of SQLx query in Sequences Delete]
**Vulnerability:** A `format!` macro was used to dynamically construct SQL queries in `orch8-storage/src/postgres/sequences.rs` and `orch8-storage/src/sqlite/sequences.rs`.
**Learning:** Although the variable passed into `format!` was a static slice array and not user input, using `format!` or string concatenation with `sqlx` defeats defense-in-depth and will trigger SAST/linters.
**Prevention:** Construct parameterized queries using `sqlx::QueryBuilder` or use `match` for table-level SQL string generation. Never use string format/concat for SQL queries.
