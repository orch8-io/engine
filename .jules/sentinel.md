## 2025-02-15 - [Refactored format! out of SQLx query in Sequences Delete]
**Vulnerability:** A `format!` macro was used to dynamically construct SQL queries in `orch8-storage/src/postgres/sequences.rs` and `orch8-storage/src/sqlite/sequences.rs`.
**Learning:** Although the variable passed into `format!` was a static slice array and not user input, using `format!` or string concatenation with `sqlx` defeats defense-in-depth and will trigger SAST/linters.
**Prevention:** Construct parameterized queries using `sqlx::QueryBuilder` or use `match` for table-level SQL string generation. Never use string format/concat for SQL queries.
## 2025-02-15 - [Refactored format! out of SQLx query in Workers Stats]
**Vulnerability:** A `format!` macro was used to dynamically construct SQL queries in `orch8-storage/src/postgres/workers.rs`.
**Learning:** Although the variable passed into `format!` was a static slice array and not user input, using `format!` or string concatenation with `sqlx` defeats defense-in-depth and will trigger SAST/linters.
**Prevention:** Construct parameterized queries using `sqlx::QueryBuilder` or use `match` for table-level SQL string generation. Never use string format/concat for SQL queries.
## 2025-02-15 - [Refactored format! out of SQLx query in SQLite storage layer]
**Vulnerability:** A `format!` macro was used to dynamically construct SQL queries with `IN ({})` clauses in `orch8-storage/src/sqlite/signals.rs`, `orch8-storage/src/sqlite/outputs.rs`, and `orch8-storage/src/sqlite/externalized.rs`.
**Learning:** Although the variable passed into `format!` was a dynamically constructed string of placeholders (e.g. `?1, ?2`) and not user input, using `format!` or string concatenation with `sqlx` defeats defense-in-depth and will trigger SAST/linters.
**Prevention:** Construct parameterized queries for `IN (...)` clauses using `sqlx::QueryBuilder` with the `.separated(",")` and `.push_bind()` pattern. Never use string format/concat for SQL queries.
