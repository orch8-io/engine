## 2025-02-15 - [Refactored format! out of SQLx query in Sequences Delete]
**Vulnerability:** A `format!` macro was used to dynamically construct SQL queries in `orch8-storage/src/postgres/sequences.rs` and `orch8-storage/src/sqlite/sequences.rs`.
**Learning:** Although the variable passed into `format!` was a static slice array and not user input, using `format!` or string concatenation with `sqlx` defeats defense-in-depth and will trigger SAST/linters.
**Prevention:** Construct parameterized queries using `sqlx::QueryBuilder` or use `match` for table-level SQL string generation. Never use string format/concat for SQL queries.
## 2025-02-15 - [Refactored format! out of SQLx query in Workers Stats]
**Vulnerability:** A `format!` macro was used to dynamically construct SQL queries in `orch8-storage/src/postgres/workers.rs`.
**Learning:** Although the variable passed into `format!` was a static slice array and not user input, using `format!` or string concatenation with `sqlx` defeats defense-in-depth and will trigger SAST/linters.
**Prevention:** Construct parameterized queries using `sqlx::QueryBuilder` or use `match` for table-level SQL string generation. Never use string format/concat for SQL queries.
## 2025-02-15 - [Refactored format! out of SQLx query in SQLite Externalized]
**Vulnerability:** A `format!` macro was used to dynamically construct SQL queries with `IN` clauses in `orch8-storage/src/sqlite/externalized.rs`.
**Learning:** Using `format!` or string concatenation with `sqlx` defeats defense-in-depth and will trigger SAST/linters. The preferred method to build dynamic `IN` clauses is using `QueryBuilder` with `.separated()`.
**Prevention:** Construct parameterized queries using `sqlx::QueryBuilder` with `.separated()` and `.push_bind()`. Never use string format/concat for SQL queries.
## 2025-02-15 - [Refactored format! out of SQLx query in SQLite Outputs]
**Vulnerability:** A `format!` macro was used to dynamically construct SQL queries with `IN` clauses in `orch8-storage/src/sqlite/outputs.rs`.
**Learning:** Using `format!` or string concatenation with `sqlx` defeats defense-in-depth and will trigger SAST/linters. The preferred method to build dynamic `IN` clauses is using `QueryBuilder` with `.separated()`.
**Prevention:** Construct parameterized queries using `sqlx::QueryBuilder` with `.separated()` and `.push_bind()`. Never use string format/concat for SQL queries.
## 2025-02-15 - [Refactored format! out of SQLx query in SQLite Signals]
**Vulnerability:** A `format!` macro was used to dynamically construct SQL queries with `IN` clauses in `orch8-storage/src/sqlite/signals.rs`.
**Learning:** Using `format!` or string concatenation with `sqlx` defeats defense-in-depth and will trigger SAST/linters. The preferred method to build dynamic `IN` clauses is using `QueryBuilder` with `.separated()`.
**Prevention:** Construct parameterized queries using `sqlx::QueryBuilder` with `.separated()` and `.push_bind()`. Never use string format/concat for SQL queries.
