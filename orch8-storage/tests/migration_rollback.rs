//! Structural validation tests for down/rollback migration scripts.
//!
//! These tests verify structural correctness of the rollback SQL files in
//! `migrations/down/` via static analysis. They do NOT execute the SQL against
//! a live database (that requires a running Postgres instance); instead they
//! catch common authoring errors:
//!
//!   1. Every rollback file has a matching forward migration.
//!   2. Each rollback file is non-empty, terminates statements with `;`, and
//!      contains the expected DDL operations.
//!   3. Forward-rollback pairs are semantically consistent (e.g. a forward
//!      CREATE TABLE has a rollback DROP TABLE for the same table name).
//!   4. Drop ordering is correct (indexes before tables).
//!
//! Limitations: these are lint-level checks — they cannot catch syntax errors,
//! wrong casing, or runtime failures. For that, run rollbacks against an actual
//! Postgres instance in CI.
//!
//! Running only this file:
//!
//! ```text
//! cargo test -p orch8-storage --test migration_rollback
//! ```

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Root of the migrations directory (relative to the workspace root).
fn migrations_dir() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // orch8-storage/.. == workspace root
    manifest.parent().unwrap().join("migrations")
}

fn down_dir() -> PathBuf {
    migrations_dir().join("down")
}

/// Parse the numeric prefix from a migration filename like `026_emit_event_dedupe_scope.sql`.
fn migration_number(filename: &str) -> Option<u32> {
    filename.split('_').next()?.parse().ok()
}

/// Read all `.sql` files from a directory, keyed by numeric prefix.
fn read_sql_files(dir: &Path) -> HashMap<u32, (String, String)> {
    let mut map = HashMap::new();
    if !dir.exists() {
        return map;
    }
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if !std::path::Path::new(&name)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"))
        {
            continue;
        }
        if let Some(num) = migration_number(&name) {
            let content = fs::read_to_string(entry.path()).unwrap();
            map.insert(num, (name, content));
        }
    }
    map
}

/// Strip SQL single-line comments (`-- ...`) and return only executable SQL.
fn strip_comments(sql: &str) -> String {
    sql.lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn down_dir_exists() {
    let dir = down_dir();
    assert!(
        dir.exists(),
        "migrations/down/ directory must exist: {dir:?}"
    );
    assert!(dir.is_dir(), "migrations/down must be a directory");
}

#[test]
fn all_rollback_files_have_matching_forward_migration() {
    let forward = read_sql_files(&migrations_dir());
    let rollbacks = read_sql_files(&down_dir());

    assert!(
        !rollbacks.is_empty(),
        "Expected at least one rollback file in migrations/down/"
    );

    for (num, (name, _)) in &rollbacks {
        assert!(
            forward.contains_key(num),
            "Rollback file {name} (#{num:03}) has no matching forward migration"
        );
    }
}

#[test]
fn rollback_files_are_non_empty_and_contain_sql() {
    let rollbacks = read_sql_files(&down_dir());

    for (num, (name, content)) in &rollbacks {
        assert!(
            !content.trim().is_empty(),
            "Rollback file {name} (#{num:03}) is empty"
        );

        let sql_body = strip_comments(content);

        assert!(
            !sql_body.trim().is_empty(),
            "Rollback file {name} (#{num:03}) contains only comments, no SQL statements"
        );

        // Every rollback must contain at least one DDL keyword.
        let upper = sql_body.to_uppercase();
        let has_ddl = upper.contains("DROP")
            || upper.contains("CREATE")
            || upper.contains("ALTER")
            || upper.contains("DELETE");
        assert!(
            has_ddl,
            "Rollback file {name} (#{num:03}) contains no DDL keywords (DROP/CREATE/ALTER/DELETE)"
        );
    }
}

#[test]
fn rollback_026_restores_original_emit_event_dedupe() {
    let rollbacks = read_sql_files(&down_dir());
    let (name, content) = rollbacks
        .get(&26)
        .expect("Missing rollback for migration 026");

    let upper = content.to_uppercase();

    assert!(
        upper.contains("DROP TABLE"),
        "{name}: must DROP the v2 emit_event_dedupe table"
    );
    assert!(
        upper.contains("CREATE TABLE"),
        "{name}: must CREATE the v1 emit_event_dedupe table"
    );
    assert!(
        content.contains("parent_instance_id"),
        "{name}: v1 table must have parent_instance_id column"
    );
    assert!(
        content.contains("dedupe_key"),
        "{name}: v1 table must have dedupe_key column"
    );
    assert!(
        content.contains("child_instance_id"),
        "{name}: v1 table must have child_instance_id column"
    );
    assert!(
        content.contains("PRIMARY KEY"),
        "{name}: v1 table must declare a PRIMARY KEY"
    );
}

#[test]
fn rollback_027_restores_unique_constraint() {
    let rollbacks = read_sql_files(&down_dir());
    let (name, content) = rollbacks
        .get(&27)
        .expect("Missing rollback for migration 027");

    let upper = content.to_uppercase();

    assert!(
        upper.contains("DROP INDEX"),
        "{name}: must DROP the multirow composite index"
    );
    assert!(
        upper.contains("UNIQUE"),
        "{name}: must re-add the UNIQUE constraint on (instance_id, block_id)"
    );
    assert!(
        content.contains("instance_id"),
        "{name}: UNIQUE constraint must reference instance_id"
    );
    assert!(
        content.contains("block_id"),
        "{name}: UNIQUE constraint must reference block_id"
    );
}

#[test]
fn rollback_034_reverts_cascade_fk_constraints() {
    let rollbacks = read_sql_files(&down_dir());
    let (name, content) = rollbacks
        .get(&34)
        .expect("Missing rollback for migration 034");

    let upper = content.to_uppercase();

    // Must reference all five tables that 034 modified.
    let tables = [
        "block_outputs",
        "execution_tree",
        "signal_inbox",
        "worker_tasks",
        "task_instances",
    ];
    for table in &tables {
        assert!(
            content.contains(table),
            "{name}: must reference table '{table}'"
        );
    }

    // Must DROP constraints before re-adding.
    let drop_count = upper.matches("DROP CONSTRAINT").count();
    assert!(
        drop_count >= 5,
        "{name}: expected at least 5 DROP CONSTRAINT statements, found {drop_count}"
    );

    // Must ADD constraints back.
    let add_count = upper.matches("ADD CONSTRAINT").count();
    assert!(
        add_count >= 5,
        "{name}: expected at least 5 ADD CONSTRAINT statements, found {add_count}"
    );

    // Must use NOT VALID pattern for online safety.
    assert!(
        upper.contains("NOT VALID"),
        "{name}: must use NOT VALID for online-safe constraint addition"
    );
    assert!(
        upper.contains("VALIDATE CONSTRAINT"),
        "{name}: must VALIDATE constraints after NOT VALID addition"
    );

    // The SQL body (excluding comments) must NOT contain ON DELETE CASCADE —
    // that is what the forward migration added and the rollback undoes.
    let sql_body = strip_comments(content).to_uppercase();
    assert!(
        !sql_body.contains("ON DELETE CASCADE"),
        "{name}: rollback SQL body must NOT contain ON DELETE CASCADE — it should restore RESTRICT"
    );
    assert!(
        !sql_body.contains("ON DELETE SET NULL"),
        "{name}: rollback SQL body must NOT contain ON DELETE SET NULL — it should restore RESTRICT"
    );
}

#[test]
fn rollback_035_drops_telemetry_tables() {
    let rollbacks = read_sql_files(&down_dir());
    let (name, content) = rollbacks
        .get(&35)
        .expect("Missing rollback for migration 035");

    let upper = content.to_uppercase();

    assert!(
        upper.contains("DROP TABLE") && content.contains("telemetry_mobile_events"),
        "{name}: must DROP TABLE telemetry_mobile_events"
    );
    assert!(
        upper.contains("DROP TABLE") && content.contains("telemetry_mobile_errors"),
        "{name}: must DROP TABLE telemetry_mobile_errors"
    );

    // Must drop indexes too.
    let drop_index_count = upper.matches("DROP INDEX").count();
    assert!(
        drop_index_count >= 7,
        "{name}: expected at least 7 DROP INDEX statements (4 events + 3 errors), found {drop_index_count}"
    );
}

#[test]
fn rollback_036_drops_status_column() {
    let rollbacks = read_sql_files(&down_dir());
    let (name, content) = rollbacks
        .get(&36)
        .expect("Missing rollback for migration 036");

    let upper = content.to_uppercase();

    assert!(
        upper.contains("DROP INDEX"),
        "{name}: must DROP the idx_sequences_status index"
    );
    assert!(
        content.contains("idx_sequences_status"),
        "{name}: must reference idx_sequences_status by name"
    );
    assert!(
        upper.contains("DROP COLUMN"),
        "{name}: must DROP COLUMN status from sequences"
    );
    assert!(
        content.contains("status"),
        "{name}: must reference the 'status' column"
    );
}

#[test]
fn rollback_037_drops_rollback_tables() {
    let rollbacks = read_sql_files(&down_dir());
    let (name, content) = rollbacks
        .get(&37)
        .expect("Missing rollback for migration 037");

    let upper = content.to_uppercase();

    assert!(
        upper.contains("DROP TABLE") && content.contains("rollback_policies"),
        "{name}: must DROP TABLE rollback_policies"
    );
    assert!(
        upper.contains("DROP TABLE") && content.contains("rollback_history"),
        "{name}: must DROP TABLE rollback_history"
    );

    // Must drop indexes before tables.
    let drop_index_count = upper.matches("DROP INDEX").count();
    assert!(
        drop_index_count >= 4,
        "{name}: expected at least 4 DROP INDEX statements, found {drop_index_count}"
    );
}

#[test]
fn rollback_statements_end_with_semicolons() {
    let rollbacks = read_sql_files(&down_dir());

    for (num, (name, content)) in &rollbacks {
        let sql_body = strip_comments(content);
        let trimmed = sql_body.trim();
        assert!(
            trimmed.ends_with(';'),
            "Rollback file {name} (#{num:03}) does not end with a semicolon — \
             likely a truncated or incomplete SQL statement"
        );
    }
}

#[test]
fn rollback_filenames_match_forward_migration_names() {
    let forward = read_sql_files(&migrations_dir());
    let rollbacks = read_sql_files(&down_dir());

    for (num, (rollback_name, _)) in &rollbacks {
        if let Some((forward_name, _)) = forward.get(num) {
            assert_eq!(
                rollback_name, forward_name,
                "Rollback file #{num:03} name '{rollback_name}' does not match \
                 forward migration name '{forward_name}'"
            );
        }
    }
}

#[test]
fn every_rollback_has_a_comment_header() {
    let rollbacks = read_sql_files(&down_dir());

    for (num, (name, content)) in &rollbacks {
        let first_line = content.lines().next().unwrap_or("");
        assert!(
            first_line.starts_with("--"),
            "Rollback file {name} (#{num:03}) should start with a comment header \
             explaining what it undoes"
        );
    }
}

#[test]
fn rollback_drop_order_is_correct_for_035() {
    // Indexes must be dropped before their parent tables.
    let rollbacks = read_sql_files(&down_dir());
    let (name, content) = rollbacks
        .get(&35)
        .expect("Missing rollback for migration 035");

    // Strip comments.
    let sql_lines: Vec<&str> = content
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.trim_start().starts_with("--"))
        .collect();

    // Find position of DROP TABLE for each table and verify all DROP INDEX
    // for that table come before it.
    let events_table_pos = sql_lines
        .iter()
        .position(|l| {
            l.to_uppercase().contains("DROP TABLE") && l.contains("telemetry_mobile_events")
        })
        .unwrap_or_else(|| panic!("{name}: DROP TABLE telemetry_mobile_events not found"));

    let errors_table_pos = sql_lines
        .iter()
        .position(|l| {
            l.to_uppercase().contains("DROP TABLE") && l.contains("telemetry_mobile_errors")
        })
        .unwrap_or_else(|| panic!("{name}: DROP TABLE telemetry_mobile_errors not found"));

    // Check that idx_telemetry_errors_* indexes are dropped before the errors table.
    for (i, line) in sql_lines.iter().enumerate() {
        if line.contains("idx_telemetry_errors_") {
            assert!(
                i < errors_table_pos,
                "{name}: index drop at line {i} must come before DROP TABLE \
                 telemetry_mobile_errors at line {errors_table_pos}"
            );
        }
        if line.contains("idx_telemetry_events_") {
            assert!(
                i < events_table_pos,
                "{name}: index drop at line {i} must come before DROP TABLE \
                 telemetry_mobile_events at line {events_table_pos}"
            );
        }
    }
}

#[test]
fn rollback_drop_order_is_correct_for_037() {
    // Indexes must be dropped before their parent tables.
    let rollbacks = read_sql_files(&down_dir());
    let (name, content) = rollbacks
        .get(&37)
        .expect("Missing rollback for migration 037");

    let sql_lines: Vec<&str> = content
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.trim_start().starts_with("--"))
        .collect();

    let history_table_pos = sql_lines
        .iter()
        .position(|l| l.to_uppercase().contains("DROP TABLE") && l.contains("rollback_history"))
        .unwrap_or_else(|| panic!("{name}: DROP TABLE rollback_history not found"));

    let policies_table_pos = sql_lines
        .iter()
        .position(|l| l.to_uppercase().contains("DROP TABLE") && l.contains("rollback_policies"))
        .unwrap_or_else(|| panic!("{name}: DROP TABLE rollback_policies not found"));

    for (i, line) in sql_lines.iter().enumerate() {
        if line.contains("idx_rollback_history_") {
            assert!(
                i < history_table_pos,
                "{name}: index drop at line {i} must come before DROP TABLE \
                 rollback_history at line {history_table_pos}"
            );
        }
        if line.contains("idx_rollback_policies_") {
            assert!(
                i < policies_table_pos,
                "{name}: index drop at line {i} must come before DROP TABLE \
                 rollback_policies at line {policies_table_pos}"
            );
        }
    }
}
