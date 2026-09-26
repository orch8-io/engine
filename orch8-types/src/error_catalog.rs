//! Stable, documented error codes for validation, preflight, and diagnosis
//! findings.
//!
//! Every [`crate::finding::Finding`] keeps its machine key (e.g.
//! `NO_COMPATIBLE_WORKER`) and additionally carries a stable public code
//! (`ORCH8-P001`) plus a docs URL (`https://orch8.io/docs/errors#ORCH8-P001`)
//! looked up from [`CATALOG`]. The catalog is the single source for
//! `docs/ERRORS.md`; a test fails when the checked-in file drifts.
//!
//! Series:
//! - `ORCH8-V0xx` — definition validation: the document itself is wrong
//!   (syntax, decode, structure, schemas, static lint, typed dataflow).
//! - `ORCH8-P0xx` — preflight readiness: the definition is valid but the
//!   runtime cannot serve it (workers, credentials, plugins, queues,
//!   sub-sequences).
//! - `ORCH8-D0xx` — instance diagnosis: why a running instance is not
//!   progressing.
//!
//! **Stability rules:** entries are append-only. Never renumber, reuse, or
//! remove a code; deprecate by editing its text instead.

use std::fmt::Write as _;

/// Base URL of the public error reference; the code is the fragment.
pub const DOCS_BASE_URL: &str = "https://orch8.io/docs/errors";

/// Which surface raises a code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeries {
    Validation,
    Preflight,
    Diagnosis,
}

impl ErrorSeries {
    #[must_use]
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::Validation => "ORCH8-V",
            Self::Preflight => "ORCH8-P",
            Self::Diagnosis => "ORCH8-D",
        }
    }

    #[must_use]
    pub const fn title(self) -> &'static str {
        match self {
            Self::Validation => "Validation (ORCH8-V)",
            Self::Preflight => "Preflight readiness (ORCH8-P)",
            Self::Diagnosis => "Instance diagnosis (ORCH8-D)",
        }
    }
}

/// One documented error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorCodeEntry {
    /// Stable public code, e.g. `ORCH8-P001`.
    pub code: &'static str,
    /// Machine key used in `Finding::code` (`SCREAMING_SNAKE_CASE`). A key
    /// ending in `:` matches every key with that prefix
    /// (`INCOMPATIBLE:BLOCK_REMOVED`).
    pub key: &'static str,
    pub series: ErrorSeries,
    /// Short title.
    pub title: &'static str,
    /// What happened / the likely cause, in plain language.
    pub cause: &'static str,
    /// How to fix it.
    pub fix: &'static str,
}

impl ErrorCodeEntry {
    /// `https://orch8.io/docs/errors#<code>`.
    #[must_use]
    pub fn docs_url(&self) -> String {
        docs_url(self.code)
    }
}

/// Docs URL for a public code.
#[must_use]
pub fn docs_url(code: &str) -> String {
    format!("{DOCS_BASE_URL}#{code}")
}

macro_rules! entry {
    ($code:literal, $key:literal, $series:ident, $title:literal, $cause:literal, $fix:literal $(,)?) => {
        ErrorCodeEntry {
            code: $code,
            key: $key,
            series: ErrorSeries::$series,
            title: $title,
            cause: $cause,
            fix: $fix,
        }
    };
}

/// The append-only catalog. Order within a series is numeric.
pub const CATALOG: &[ErrorCodeEntry] = &[
    // ── Validation ────────────────────────────────────────────────────────
    entry!(
        "ORCH8-V001",
        "DOCUMENT_SYNTAX",
        Validation,
        "Document is not valid JSON/YAML",
        "The sequence file or request body could not be parsed. The message \
         names the line and column of the first syntax error.",
        "Fix the syntax at the reported line/column. For YAML, check \
         indentation (spaces, not tabs) and quote strings containing `: ` or `#`."
    ),
    entry!(
        "ORCH8-V002",
        "SEQUENCE_DECODE_FAILED",
        Validation,
        "Document is not a sequence definition",
        "The document parsed, but a field has the wrong type, a required \
         field is missing, or (with `?strict=true`) an unknown field is present.",
        "Compare the document with contracts/sequence.schema.json; the message \
         names the offending path. Run `orch8 sequence preflight --file <file>`."
    ),
    entry!(
        "ORCH8-V003",
        "UNKNOWN_SEQUENCE_FIELD",
        Validation,
        "Unknown field ignored",
        "Lenient decoding ignored a field the engine does not know — usually a \
         typo such as `wehn` for `when`, which silently disables the feature.",
        "Rename or remove the field. Use `?strict=true` (API) to reject unknown \
         fields outright."
    ),
    entry!(
        "ORCH8-V004",
        "INVALID_DEFINITION",
        Validation,
        "Definition failed structural validation",
        "The sequence decoded but violates a structural rule: no blocks, an \
         unsupported schema_version, nesting/size limits, or an invalid block.",
        "Read the message for the block id and rule, fix the definition, and \
         re-run preflight."
    ),
    entry!(
        "ORCH8-V005",
        "DUPLICATE_BLOCK_ID",
        Validation,
        "Duplicate block id",
        "Two blocks share an id. Block ids key progress, outputs, and \
         references, so they must be unique across the whole tree (including \
         nested branches).",
        "Rename one of the blocks and update any `outputs.<id>` references."
    ),
    entry!(
        "ORCH8-V006",
        "INVALID_HUMAN_INPUT",
        Validation,
        "Invalid human review / wait_for_input configuration",
        "A block's human-input gate is malformed (e.g. empty or duplicate \
         choices, invalid timeout).",
        "Fix the `wait_for_input` / `human_review` settings on the named block."
    ),
    entry!(
        "ORCH8-V007",
        "INVALID_BLOCK",
        Validation,
        "Invalid block",
        "A block violates a per-block rule (empty handler, too many branches, \
         iteration caps, invalid parameters).",
        "Fix the named block as described in the message."
    ),
    entry!(
        "ORCH8-V008",
        "INVALID_INPUT_SCHEMA",
        Validation,
        "Input schema is not a valid JSON Schema",
        "`input_schema` is not a JSON object or does not compile as JSON Schema, \
         so instance inputs could never be validated.",
        "Make `input_schema` a valid JSON Schema object (draft 2020-12)."
    ),
    entry!(
        "ORCH8-V009",
        "INVALID_OUTPUT_SCHEMA",
        Validation,
        "Step output schema is not a valid JSON Schema",
        "A step's `output_schema` is not an object or does not compile.",
        "Fix or remove the named step's `output_schema`."
    ),
    entry!(
        "ORCH8-V010",
        "EMPTY_WHEN_GUARD",
        Validation,
        "Empty `when` guard",
        "A block has a `when` guard that is blank, which is almost certainly an \
         unfinished edit.",
        "Write the condition or remove the `when` field."
    ),
    entry!(
        "ORCH8-V011",
        "LINT_WARNING",
        Validation,
        "Lint warning",
        "Static analysis found a likely mistake (dangling output reference, \
         unsafe for_each, step after an unconditional fail, event wait without \
         a gate, …).",
        "Read the message for the block and rule; `orch8 sequence preflight` \
         lists every warning."
    ),
    entry!(
        "ORCH8-V012",
        "MISSING_PRODUCER",
        Validation,
        "Reference to an output that no block produces",
        "A template references `outputs.<block>` for a block that does not \
         exist or cannot have run before the consumer.",
        "Point the reference at an existing upstream block, or add the producer."
    ),
    entry!(
        "ORCH8-V013",
        "SCHEMA_PATH_MISSING",
        Validation,
        "Referenced field is not in the producer's output schema",
        "The consumer reads a path the producer's `output_schema` does not \
         declare.",
        "Fix the path, or add the field to the producer's `output_schema`."
    ),
    entry!(
        "ORCH8-V014",
        "TYPE_UNKNOWN",
        Validation,
        "Referenced value has no declared type",
        "The producer has no `output_schema` (or the path is dynamic), so the \
         value's type cannot be checked.",
        "Declare an `output_schema` on the producer to enable typed dataflow."
    ),
    entry!(
        "ORCH8-V015",
        "VALUE_MAY_BE_ABSENT",
        Validation,
        "Referenced value may be absent",
        "The referenced field is optional in the producer's schema.",
        "Mark it required in the producer schema or handle the missing case \
         (default filter / router)."
    ),
    entry!(
        "ORCH8-V016",
        "VALUE_MAY_BE_NULL",
        Validation,
        "Referenced value may be null",
        "The referenced field is nullable in the producer's schema.",
        "Make it non-nullable or handle null in the consumer."
    ),
    entry!(
        "ORCH8-V017",
        "INCOMPATIBLE_COERCION",
        Validation,
        "Incompatible type coercion",
        "A producer/consumer pair has types that cannot be coerced safely.",
        "Align the producer's output schema with what the consumer expects."
    ),
    entry!(
        "ORCH8-V018",
        "INCOMPATIBLE:",
        Validation,
        "Incompatible change",
        "A release/migration diff found a change that breaks in-flight \
         instances (the suffix names the rule, e.g. `INCOMPATIBLE:BLOCK_REMOVED`).",
        "Ship the change as a new sequence version and migrate or drain \
         in-flight instances first."
    ),
    entry!(
        "ORCH8-V019",
        "SEQUENCE_SERIALIZATION_FAILED",
        Validation,
        "Sequence could not be serialized for analysis",
        "Internal: the typed-dataflow compiler could not serialize the definition.",
        "Report this with the definition attached; other checks still ran."
    ),
    // ── Preflight readiness ───────────────────────────────────────────────
    entry!(
        "ORCH8-P001",
        "NO_COMPATIBLE_WORKER",
        Preflight,
        "No compatible worker for a handler",
        "A step uses a handler that is not built in, not a plugin, and has no \
         live worker registration — its tasks would wait forever.",
        "Start a worker that registers the handler (`orch8 dev --worker \"<cmd>\"` \
         locally), fix a handler-name typo, or use a built-in handler."
    ),
    entry!(
        "ORCH8-P002",
        "WORKER_BELOW_VERSION_PIN",
        Preflight,
        "Workers are older than the version pin",
        "The handler is pinned to a minimum worker version, but every live \
         worker for it is older, so none may claim its tasks.",
        "Deploy an upgraded worker or relax the version pin."
    ),
    entry!(
        "ORCH8-P003",
        "PLUGIN_DISABLED",
        Preflight,
        "Plugin is disabled",
        "A step calls a WASM/gRPC plugin that exists but is disabled; the step \
         would fail immediately.",
        "Enable the plugin or change the step's handler."
    ),
    entry!(
        "ORCH8-P004",
        "CREDENTIAL_MISSING",
        Preflight,
        "Referenced credential does not exist",
        "A `credentials://<id>` reference points to a credential that is not \
         stored for this tenant.",
        "Create the credential (`POST /credentials`) or fix the id."
    ),
    entry!(
        "ORCH8-P005",
        "CREDENTIAL_DISABLED",
        Preflight,
        "Referenced credential is disabled",
        "The credential exists but is disabled; resolution would fail at runtime.",
        "Re-enable the credential or point the step at an active one."
    ),
    entry!(
        "ORCH8-P006",
        "CREDENTIAL_EXPIRED",
        Preflight,
        "Referenced credential has expired",
        "The credential's `expires_at` is in the past and no refresh succeeded.",
        "Rotate the credential or fix its refresh configuration."
    ),
    entry!(
        "ORCH8-P007",
        "QUEUE_HAS_NO_CONSUMER",
        Preflight,
        "Queue has no consumer",
        "A step targets an explicit queue with no live polling worker, no push \
         dispatch config, and no routing rule redirecting it.",
        "Start a worker on that queue, configure push dispatch, or add a \
         routing rule."
    ),
    entry!(
        "ORCH8-P008",
        "SUB_SEQUENCE_MISSING",
        Preflight,
        "Sub-sequence does not exist",
        "A `sub_sequence` block names a sequence (or version) that is not stored.",
        "Publish the child sequence first or fix its name/version."
    ),
    entry!(
        "ORCH8-P009",
        "SUB_SEQUENCE_UNPUBLISHED",
        Preflight,
        "Sub-sequence has no runnable version",
        "Every version of the referenced child sequence is deprecated or \
         unpublished.",
        "Publish a runnable version of the child sequence."
    ),
    entry!(
        "ORCH8-P010",
        "SUB_SEQUENCE_DRAFT_ONLY",
        Preflight,
        "Sub-sequence only exists as a draft",
        "The referenced child sequence has only draft versions.",
        "Promote the child sequence before relying on it in production."
    ),
    // ── Instance diagnosis ────────────────────────────────────────────────
    entry!(
        "ORCH8-D001",
        "TERMINAL_STATE",
        Diagnosis,
        "Instance is in a terminal state",
        "The instance completed, failed, or was cancelled; terminal instances \
         never progress again.",
        "For a failed instance, fix the cause and `orch8 instance retry <id>`."
    ),
    entry!(
        "ORCH8-D002",
        "SEQUENCE_MISSING",
        Diagnosis,
        "Instance's sequence was deleted",
        "The instance references a sequence version that no longer exists.",
        "Restore the sequence (e.g. `orch8 restore`) or cancel the instance."
    ),
    entry!(
        "ORCH8-D003",
        "WAITING_UNTIL",
        Diagnosis,
        "Waiting for a scheduled time",
        "The instance is deliberately waiting for a timer (delay, send window, \
         retry backoff).",
        "Nothing to fix; it resumes at `next_fire_at`."
    ),
    entry!(
        "ORCH8-D004",
        "WAITING_EVENT",
        Diagnosis,
        "Waiting for correlated events",
        "A `wait_for_event` block is waiting for events that have not arrived.",
        "Emit the missing events or check the correlation key."
    ),
    entry!(
        "ORCH8-D005",
        "PENDING_APPROVAL",
        Diagnosis,
        "Waiting for human approval",
        "A block with a human-input gate is waiting for a decision.",
        "Approve or reject it (`orch8 signal <id> ...` or the dashboard)."
    ),
    entry!(
        "ORCH8-D006",
        "BUDGET_PAUSED",
        Diagnosis,
        "Paused by budget",
        "The instance exceeded its cost/token budget and was paused.",
        "Raise the budget or resume deliberately."
    ),
    entry!(
        "ORCH8-D007",
        "PAUSED",
        Diagnosis,
        "Paused",
        "An operator or policy paused the instance.",
        "Resume it when ready: `orch8 signal <id> resume`."
    ),
    entry!(
        "ORCH8-D008",
        "WORKER_TASK_PENDING",
        Diagnosis,
        "Worker task is pending",
        "A step was dispatched to external workers and is waiting to be claimed.",
        "Make sure a worker polls the handler/queue."
    ),
    entry!(
        "ORCH8-D009",
        "WORKER_NOT_CLAIMING",
        Diagnosis,
        "Live workers are not claiming the task",
        "Workers for the handler are registered but the task has stayed pending.",
        "Check worker logs, queue names, and version pins."
    ),
    entry!(
        "ORCH8-D010",
        "WAITING_WORKER_PICKUP",
        Diagnosis,
        "Waiting for worker pickup",
        "The task was recently dispatched; a worker should claim it shortly.",
        "Usually nothing; if it persists, check workers."
    ),
    entry!(
        "ORCH8-D011",
        "STALE_WORKER_CLAIM",
        Diagnosis,
        "Worker claimed the task and went silent",
        "A worker claimed the task but its heartbeat is stale — it probably \
         crashed.",
        "The lease will expire and the task is re-dispatched; restart the worker."
    ),
    entry!(
        "ORCH8-D012",
        "OPEN_CIRCUIT_BREAKER",
        Diagnosis,
        "Circuit breaker is open",
        "Repeated failures opened the handler's circuit breaker; dispatch is \
         paused until it half-opens.",
        "Fix the failing dependency; the breaker closes after a successful probe."
    ),
    entry!(
        "ORCH8-D013",
        "WAITING_CHILD",
        Diagnosis,
        "Waiting for a child instance",
        "A sub-sequence child is still running.",
        "Diagnose the child instance."
    ),
    entry!(
        "ORCH8-D014",
        "CHILDREN_DONE_PARENT_WAITING",
        Diagnosis,
        "Children finished but the parent did not resume",
        "Every child is terminal, yet the parent is still waiting — a missed \
         completion signal.",
        "Resume the parent: `orch8 signal <id> resume`."
    ),
    entry!(
        "ORCH8-D015",
        "SIGNALS_NOT_CONSUMED",
        Diagnosis,
        "Signals are queued but not consumed",
        "Signals were delivered to the instance but it has not processed them.",
        "Check that the instance is schedulable (not paused) and the scheduler runs."
    ),
    entry!(
        "ORCH8-D016",
        "STALE_RUNNING_STATE",
        Diagnosis,
        "Running state is stale",
        "The instance says Running but has not been updated for a long time — \
         the node executing it probably crashed.",
        "Stale-instance recovery reschedules it; check node health."
    ),
    entry!(
        "ORCH8-D017",
        "SCHEDULER_LAG",
        Diagnosis,
        "Scheduler is behind",
        "The instance was due a while ago but has not been picked up.",
        "Check scheduler health, database load, and node count."
    ),
    entry!(
        "ORCH8-D018",
        "WAITING_EXTERNAL_EVENT",
        Diagnosis,
        "Waiting for an external event",
        "The instance is waiting for a signal or input from outside.",
        "Send the expected signal or input."
    ),
    entry!(
        "ORCH8-D019",
        "EVIDENCE_INCOMPLETE",
        Diagnosis,
        "Diagnosis evidence is incomplete",
        "Some evidence could not be collected, so the diagnosis may miss a cause.",
        "Retry the diagnosis; check storage health."
    ),
    entry!(
        "ORCH8-D020",
        "NO_BLOCKER_FOUND",
        Diagnosis,
        "No blocker found",
        "No rule identified a blocking condition; the instance appears healthy.",
        "Watch progress; re-run diagnosis if it stays idle."
    ),
];

/// Look up the catalog entry for a finding key (exact, or by `PREFIX:`).
#[must_use]
pub fn lookup(key: &str) -> Option<&'static ErrorCodeEntry> {
    CATALOG.iter().find(|entry| entry.key == key).or_else(|| {
        CATALOG
            .iter()
            .find(|entry| entry.key.ends_with(':') && key.starts_with(entry.key))
    })
}

/// Look up an entry by its public code (`ORCH8-P001`).
#[must_use]
pub fn by_code(code: &str) -> Option<&'static ErrorCodeEntry> {
    CATALOG.iter().find(|entry| entry.code == code)
}

/// Render the Markdown reference checked in as `docs/ERRORS.md`.
#[must_use]
pub fn render_markdown() -> String {
    let mut out = String::new();
    out.push_str(
        "<!-- GENERATED by orch8-types/src/error_catalog.rs — do not edit by hand.\n     \
         Regenerate: UPDATE_ERRORS_MD=1 cargo test -p orch8-types errors_md_is_in_sync -->\n\n",
    );
    out.push_str("# Orch8 error codes\n\n");
    out.push_str(
        "Every validation, preflight, and diagnosis finding carries a stable \
         `error_code` and a `docs_url` pointing here. Codes are never renumbered \
         or reused. The machine key (`code` on a finding) is kept for \
         compatibility.\n\n",
    );
    for series in [
        ErrorSeries::Validation,
        ErrorSeries::Preflight,
        ErrorSeries::Diagnosis,
    ] {
        let _ = writeln!(out, "## {}\n", series.title());
        out.push_str("| Code | Key | Title |\n|---|---|---|\n");
        for entry in CATALOG.iter().filter(|e| e.series == series) {
            let _ = writeln!(
                out,
                "| [{code}](#{code}) | `{key}` | {title} |",
                code = entry.code,
                key = entry.key,
                title = entry.title
            );
        }
        out.push('\n');
        for entry in CATALOG.iter().filter(|e| e.series == series) {
            let _ = writeln!(
                out,
                "<a id=\"{code}\"></a>\n### {code} — {title}\n\nKey: `{key}`\n\n\
                 **Cause.** {cause}\n\n**Fix.** {fix}\n",
                code = entry.code,
                title = entry.title,
                key = entry.key,
                cause = entry.cause,
                fix = entry.fix
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn codes_and_keys_are_unique_and_well_formed() {
        let mut codes = HashSet::new();
        let mut keys = HashSet::new();
        for entry in CATALOG {
            assert!(codes.insert(entry.code), "duplicate code {}", entry.code);
            assert!(keys.insert(entry.key), "duplicate key {}", entry.key);
            let digits = entry
                .code
                .strip_prefix(entry.series.prefix())
                .unwrap_or_else(|| panic!("{} has the wrong series prefix", entry.code));
            assert_eq!(digits.len(), 3, "{}", entry.code);
            assert!(digits.bytes().all(|b| b.is_ascii_digit()), "{}", entry.code);
            assert!(
                entry
                    .key
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b == b'_' || b == b':'),
                "{}",
                entry.key
            );
        }
    }

    #[test]
    fn numbering_is_contiguous_per_series() {
        for series in [
            ErrorSeries::Validation,
            ErrorSeries::Preflight,
            ErrorSeries::Diagnosis,
        ] {
            let numbers: Vec<u32> = CATALOG
                .iter()
                .filter(|e| e.series == series)
                .map(|e| e.code[series.prefix().len()..].parse().unwrap())
                .collect();
            let expected: Vec<u32> = (1..=u32::try_from(numbers.len()).unwrap()).collect();
            assert_eq!(numbers, expected, "{series:?} codes must be 001.. in order");
        }
    }

    #[test]
    fn lookup_matches_exact_and_prefix_keys() {
        assert_eq!(lookup("NO_COMPATIBLE_WORKER").unwrap().code, "ORCH8-P001");
        assert_eq!(
            lookup("INCOMPATIBLE:BLOCK_REMOVED").unwrap().code,
            "ORCH8-V018"
        );
        assert!(lookup("NOT_A_KNOWN_KEY").is_none());
        assert_eq!(by_code("ORCH8-D003").unwrap().key, "WAITING_UNTIL");
        assert_eq!(
            by_code("ORCH8-P001").unwrap().docs_url(),
            "https://orch8.io/docs/errors#ORCH8-P001"
        );
    }

    /// `docs/ERRORS.md` is generated from [`CATALOG`]. Regenerate with
    /// `UPDATE_ERRORS_MD=1 cargo test -p orch8-types errors_md_is_in_sync`.
    #[test]
    fn errors_md_is_in_sync() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/ERRORS.md");
        let rendered = render_markdown();
        if std::env::var_os("UPDATE_ERRORS_MD").is_some() {
            std::fs::write(&path, &rendered).unwrap();
            return;
        }
        let on_disk = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            on_disk == rendered,
            "docs/ERRORS.md is out of sync with the error catalog; regenerate with \
             `UPDATE_ERRORS_MD=1 cargo test -p orch8-types errors_md_is_in_sync`"
        );
    }
}
