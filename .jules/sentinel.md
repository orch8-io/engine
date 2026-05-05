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
## 2025-02-15 - [Refactored format! out of SQLx query in SQLite Instances Bulk Reschedule]
**Vulnerability:** A `format!` macro was used to dynamically construct SQL queries with string concatenation for datetime modifier in `orch8-storage/src/sqlite/instances.rs`.
**Learning:** Using `format!` or string concatenation to build raw SQL query string parts defeats defense-in-depth and triggers SAST/linters, even if the value itself is an integer and safe from traditional SQL injection. Parameter binding should always be preferred over string concatenation.
**Prevention:** Construct parameterized queries using `sqlx::QueryBuilder` with `.push_bind()`, formatting values before binding instead of formatting the SQL string itself. Never use string format/concat for raw SQL string components.
## 2025-02-15 - [Refactored format! out of SQLx query in PostgreSQL search_path setup]
**Vulnerability:** A `format!` macro was used to dynamically construct SQL query strings (`SET search_path TO ...`) in `orch8-storage/src/postgres/mod.rs`.
**Learning:** Using string interpolation with `sqlx` defeats defense-in-depth and triggers SAST/linters. Although the schema identifier may have been validated prior, parameter binding provides robust SQL injection protection in case validation fails.
**Prevention:** Use the `set_config` PostgreSQL function with `sqlx::QueryBuilder` or `.bind()` for parameterized queries instead of string concatenation/interpolation for database settings. Never use string format/concat for SQL queries.
## 2025-02-15 - [Refactored format! out of SQLx query in SQLite Signals Batch Delivery]
**Vulnerability:** A `format!` macro and string joining was used to dynamically construct SQL queries with `IN` clauses in `orch8-storage/src/sqlite/signals.rs`.
**Learning:** Using `format!` or string concatenation with `sqlx` defeats defense-in-depth and will trigger SAST/linters, even when used only for placeholders. The preferred method to build dynamic `IN` clauses securely is using `QueryBuilder` with `.separated()`.
**Prevention:** Construct parameterized queries using `sqlx::QueryBuilder` with `.separated()` and `.push_bind()`. Never use string format/concat to build query structure.
## 2026-05-03 - [Refactored string concat out of SQLx query in SQLite Sequences list_all]
**Vulnerability:** String concatenation (, ) was used to dynamically construct a SQL query in `orch8-storage/src/sqlite/sequences.rs`.
**Learning:** Although the variable passed into string concat was a static placeholder `?`, and positional binding was used with `sqlx`, manually tracking the correct number and order of `?` placeholders versus `.bind()` calls is brittle and can lead to runtime crashes or unexpected behavior if future developers make a mistake. Refactoring to `QueryBuilder` prevents placeholder-mismatch bugs and trigger SAST/linters.
**Prevention:** Construct parameterized queries using `sqlx::QueryBuilder`. Never use string format/concat for SQL queries.
## 2025-02-15 - [Refactored string concat out of SQLx query in SQLite Sequences list_all]
**Vulnerability:** String concatenation (`String::from`, `.push_str`) was used to dynamically construct a SQL query in `orch8-storage/src/sqlite/sequences.rs`.
**Learning:** Although the variable passed into string concat was a static placeholder `?`, and positional binding was used with `sqlx`, manually tracking the correct number and order of `?` placeholders versus `.bind()` calls is brittle and can lead to runtime crashes or unexpected behavior if future developers make a mistake. Refactoring to `QueryBuilder` prevents placeholder-mismatch bugs and trigger SAST/linters.
**Prevention:** Construct parameterized queries using `sqlx::QueryBuilder`. Never use string format/concat for SQL queries.
