# File Splitting & Readability Refactor

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Split oversized files into logical modules, improving navigability and compile-time isolation without changing any behavior.

**Architecture:** Pure mechanical refactoring. Each split follows the `// === Section ===` markers already in the code. The `StorageBackend` trait stays in `lib.rs`. Each backend (`postgres`, `sqlite`) becomes a directory with submodules grouped by domain. The CLI gets a `commands/` directory. No API changes, no behavior changes.

**Tech Stack:** Rust module system (`mod`, `use`, `pub(crate)`), `cargo check` for verification.

**Constraints:**
- Zero behavior change. Every `cargo test` must pass identically before and after each task.
- Every task ends with `cargo check` passing.
- Commit after each task.

---

## Overview: What Gets Split

| File | Lines | Problem | Split Into |
|---|---|---|---|
| `orch8-storage/src/postgres.rs` | 2416 | Monolithic — 20+ sections, row types mixed with queries | `postgres/` dir with domain modules |
| `orch8-storage/src/sqlite.rs` | 1725 | Same structure, same problem | `sqlite/` dir with domain modules |
| `orch8-storage/src/lib.rs` | 563 | One giant trait | Keep as-is (trait must be in one place for object safety) |
| `orch8-cli/src/main.rs` | 495 | All subcommands in one file | `commands/` dir |
| `orch8-engine/src/scheduler.rs` | 983 | Large but coherent | Leave (single function + helpers, natural unit) |
| `orch8-engine/src/evaluator.rs` | 790 | Large but coherent | Leave (evaluation logic is one flow) |
| `orch8-api/src/instances.rs` | 792 | Large but coherent | Leave (all instance endpoints, natural grouping) |

**Not splitting:** Files under 800 lines with coherent single responsibilities stay as-is. The evaluator, scheduler, and instances API are large but each is one logical unit.

---

## Task 1: Split `postgres.rs` into `postgres/` directory

**Files:**
- Delete: `orch8-storage/src/postgres.rs`
- Create: `orch8-storage/src/postgres/mod.rs` (struct, new, migrations, re-exports)
- Create: `orch8-storage/src/postgres/sequences.rs`
- Create: `orch8-storage/src/postgres/instances.rs`
- Create: `orch8-storage/src/postgres/execution_tree.rs`
- Create: `orch8-storage/src/postgres/outputs.rs`
- Create: `orch8-storage/src/postgres/rate_limits.rs`
- Create: `orch8-storage/src/postgres/signals.rs`
- Create: `orch8-storage/src/postgres/cron.rs`
- Create: `orch8-storage/src/postgres/workers.rs`
- Create: `orch8-storage/src/postgres/pools.rs`
- Create: `orch8-storage/src/postgres/checkpoints.rs`
- Create: `orch8-storage/src/postgres/audit.rs`
- Create: `orch8-storage/src/postgres/sessions.rs`
- Create: `orch8-storage/src/postgres/cluster.rs`
- Create: `orch8-storage/src/postgres/rows.rs` (all `XxxRow` structs + `into_xxx` methods)

**Approach:**

The `StorageBackend` trait impl on `PostgresStorage` cannot be split across files in Rust (one `impl Trait for Type` block per crate). Instead, use **delegation methods**:

```rust
// postgres/mod.rs
mod sequences;
mod instances;
mod execution_tree;
mod outputs;
mod rate_limits;
mod signals;
mod cron;
mod workers;
mod pools;
mod checkpoints;
mod audit;
mod sessions;
mod cluster;
mod rows;

pub use rows::*;

pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub async fn new(...) -> Result<Self, StorageError> { ... }
    pub fn pool(&self) -> &PgPool { &self.pool }
    pub async fn run_migrations(&self) -> Result<(), StorageError> { ... }
}

#[async_trait]
impl StorageBackend for PostgresStorage {
    // === Sequences ===
    async fn create_sequence(&self, seq: &SequenceDefinition) -> Result<(), StorageError> {
        sequences::create(self, seq).await
    }
    async fn get_sequence(&self, id: SequenceId) -> Result<Option<SequenceDefinition>, StorageError> {
        sequences::get(self, id).await
    }
    // ... etc for all methods
}
```

```rust
// postgres/sequences.rs
use super::PostgresStorage;
// ...imports...

pub(super) async fn create(
    store: &PostgresStorage,
    seq: &SequenceDefinition,
) -> Result<(), StorageError> {
    // SQL impl moved here verbatim
}

pub(super) async fn get(
    store: &PostgresStorage,
    id: SequenceId,
) -> Result<Option<SequenceDefinition>, StorageError> {
    // SQL impl moved here verbatim
}
```

Each domain module contains the actual SQL logic. `mod.rs` contains only the struct, constructor, and thin delegation in the trait impl.

**Row types** (`InstanceRow`, `WorkerTaskRow`, `SessionRow`, etc.) move to `rows.rs` since they're shared helpers.

**Free functions** (`apply_instance_filter`, `apply_worker_task_filter`) move to `instances.rs` and `workers.rs` respectively.

**Step 1:** Create `orch8-storage/src/postgres/` directory.

**Step 2:** Create `rows.rs` — move all `XxxRow` structs and their `into_xxx()` impls from the bottom of `postgres.rs`.

**Step 3:** Create each domain module by extracting the corresponding `// === Section ===` block. Each function becomes `pub(super)` and takes `&PostgresStorage` as first arg instead of `&self`.

**Step 4:** Create `mod.rs` — struct definition, constructor, migrations, and trait impl that delegates to submodules.

**Step 5:** Delete old `postgres.rs`.

**Step 6:** Run `cargo check` — must pass.

**Step 7:** Run `cargo test` — must pass.

**Step 8:** Commit: `refactor: split postgres.rs into domain modules`

---

## Task 2: Split `sqlite.rs` into `sqlite/` directory

**Files:** Same structure as Task 1 but for SQLite.
- Delete: `orch8-storage/src/sqlite.rs`
- Create: `orch8-storage/src/sqlite/mod.rs`
- Create: `orch8-storage/src/sqlite/schema.rs` (the `SCHEMA` const + `create_tables`)
- Create: `orch8-storage/src/sqlite/sequences.rs`
- Create: `orch8-storage/src/sqlite/instances.rs`
- Create: `orch8-storage/src/sqlite/execution_tree.rs`
- Create: `orch8-storage/src/sqlite/outputs.rs`
- Create: `orch8-storage/src/sqlite/rate_limits.rs`
- Create: `orch8-storage/src/sqlite/signals.rs`
- Create: `orch8-storage/src/sqlite/cron.rs`
- Create: `orch8-storage/src/sqlite/workers.rs`
- Create: `orch8-storage/src/sqlite/pools.rs`
- Create: `orch8-storage/src/sqlite/checkpoints.rs`
- Create: `orch8-storage/src/sqlite/audit.rs`
- Create: `orch8-storage/src/sqlite/sessions.rs`
- Create: `orch8-storage/src/sqlite/cluster.rs`
- Create: `orch8-storage/src/sqlite/helpers.rs` (the `row_to_xxx` free functions + `ts()` helper)

**Approach:** Identical pattern to Task 1. SQLite has `row_to_instance`, `row_to_worker_task`, etc. as free functions instead of `FromRow` structs — these go into `helpers.rs`.

The `SCHEMA` const (large SQL DDL string) and `create_tables()` go into `schema.rs`.

Tests (`#[cfg(test)] mod tests`) stay in `mod.rs` or move to a `tests.rs` module.

**Step 1-7:** Same as Task 1 but for sqlite.

**Step 8:** Run `cargo check && cargo test`.

**Step 9:** Commit: `refactor: split sqlite.rs into domain modules`

---

## Task 3: Split `orch8-cli/src/main.rs` into commands

**Files:**
- Modify: `orch8-cli/src/main.rs` (keep Cli struct, arg parsing, dispatch)
- Create: `orch8-cli/src/commands/mod.rs`
- Create: `orch8-cli/src/commands/health.rs`
- Create: `orch8-cli/src/commands/instance.rs`
- Create: `orch8-cli/src/commands/sequence.rs`
- Create: `orch8-cli/src/commands/cron.rs`
- Create: `orch8-cli/src/commands/signal.rs`
- Create: `orch8-cli/src/commands/checkpoint.rs`

**Approach:**

```rust
// main.rs (after split)
mod commands;

#[derive(Parser)]
struct Cli { ... }

#[derive(Subcommand)]
enum Commands { ... }

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = Client::new();
    match cli.command {
        Commands::Health => commands::health::run(&client, &cli.url).await,
        Commands::Instance(cmd) => commands::instance::run(&client, &cli.url, cmd).await,
        // ...
    }
}
```

Each `commands/xxx.rs` contains the subcommand enum + handler functions. The `Tabled` display structs move with their respective commands.

**Step 1:** Create `commands/` directory and `mod.rs`.

**Step 2:** Extract each subcommand handler into its own file.

**Step 3:** Update `main.rs` to dispatch to submodules.

**Step 4:** Run `cargo check`.

**Step 5:** Commit: `refactor: split CLI into command modules`

---

## Task 4: Final verification

**Step 1:** Run full test suite: `cargo test --workspace`

**Step 2:** Run clippy: `cargo clippy --workspace -- -D warnings`

**Step 3:** Run fmt: `cargo fmt --all`

**Step 4:** Verify line counts improved:
```bash
wc -l orch8-storage/src/postgres/mod.rs  # target: <300 lines
wc -l orch8-storage/src/sqlite/mod.rs    # target: <300 lines
wc -l orch8-cli/src/main.rs              # target: <100 lines
```

**Step 5:** Commit any fmt/clippy fixes.

---

## What NOT to split (and why)

| File | Lines | Why Keep |
|---|---|---|
| `scheduler.rs` (983) | One `run_tick_loop` function + one helper — it's a single flow | Splitting would scatter the hot path across files |
| `evaluator.rs` (790) | Tree evaluation is one recursive algorithm | Splitting by node type would require passing state everywhere |
| `instances.rs` (792) | All instance REST endpoints | Axum route grouping is natural per-entity |
| `service.rs` (654) | gRPC service impl | Same pattern as REST — one impl block per service |
| `expression.rs` (496) | Parser + evaluator for `{{expressions}}` | Tightly coupled parser/evaluator pair |
| `lib.rs` (563) | StorageBackend trait | Must be one trait for object safety; `// ===` markers provide navigation |
