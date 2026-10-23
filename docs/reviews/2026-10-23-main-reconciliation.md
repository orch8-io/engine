# Remaining-work reconciliation — 2026-10-23

This record explains how the remaining local work and closed PR branches were
reconciled with current `main`. It is a historical integration record, not a
current feature reference.

## Baseline and preservation

The baseline is `77c60aeb`, the CI and dependency-advisory fix from PR #261.
Documentation commit `a6a91831` is included in full.

The previously uncommitted `review-fixes` work was saved without changing any
of its 34 file contents in commit `444aea6c`. Its source snapshot remains in
the merge ancestry. It contains unfinished prototypes and a temporary Cargo
target-directory override and must not be used as a production tree by itself.

The reconciliation tree starts from the current baseline plus the documentation
refresh. Historical branches are merge parents; their stale full-file versions
are not copied over newer implementations. The additional source changes are
listed below so the merge strategy does not imply every draft became active.

## Active source changes

- The subtree reset collects borrowed descendant nodes, avoiding an eager
  block-ID clone for every descendant. It retains the existing sorted child
  index, router reset, event-wait cleanup, attempt bookkeeping, notification
  generations, worker cancellation, and claim-fenced execution control.
- Parallel and race execution share their branch-grouping helper. Branches
  remain ordered by branch index, nodes retain source order within a branch,
  and an untagged child still belongs to branch zero. Existing parallel and
  race regression suites remain the behavioral checks.

No throughput improvement is claimed without a representative measurement.

## Review snapshot disposition

| Area                                                              | Resolution                                                                                                                                                                                                                                           |
| ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Temporary `.cargo/config.toml` override                           | Historical snapshot only; retain current platform flags and target configuration.                                                                                                                                                                    |
| Old manifest and lockfile edits                                   | Retain the current audited dependency graph and all independent lockfiles.                                                                                                                                                                           |
| Node terminal state and plugin classification                     | Already implemented in current `NodeState::is_terminal` and `PluginKind`; retain their current visibility, variants, and callers.                                                                                                                    |
| Composite resets and step dispatch                                | Retain the newer reset, claim-fencing, cancellation, and effect-settlement implementations. Port only branch grouping and borrowed reset descendants.                                                                                                |
| Worker lease wrappers and protocol hints                          | Retain the current protocol and per-task lease semantics; do not add the unused draft wrapper or a second hints implementation.                                                                                                                      |
| API state, trusted keys, startup preflight, and test initializers | Retain the newer typed startup validation and current state construction. Do not restore a deleted, superseded preflight test module.                                                                                                                |
| Sequence parsing and schema derives                               | Retain current strict parsing, schema-version checks, authored/persisted schema distinction, and generated OpenAPI contracts. The unwired `sequence_parse.rs` prototype and parallel schemars graph remain historical only.                          |
| API extractor/pagination files                                    | These are one-line placeholders, not functioning implementations. They remain historical only.                                                                                                                                                       |
| CLI API client, confirmation, and style prototypes                | These modules have no callers. Preserve them in history; retain the current wired transport, confirmation, and output behavior.                                                                                                                      |
| Storage test rewrites and `tests/common` prototype                | Retain current regressions and the existing `orch8_storage::testing::backends` harness. The older helper auto-seeds missing foreign-key parents and sweeps schemas by age; installing it would weaken checks and risk concurrent long-running tests. |

## Previously closed PR branches

The two performance branches from PRs #222 and #226 had no representative
measurement establishing a benefit over the current implementation. Their
history is retained; their algorithm substitutions are not installed.

The security branches from PRs #223 and #227 include a redundant handler guard
and unused extractor definitions. Retain current authentication, tenant
principals, and route authorization instead of reintroducing older handler
versions or treating an unreferenced extractor as enforcement. Later empty
acknowledgment commits do not change that resolution.

## Validation boundary

Run the current workspace, engine regression, documentation, and CI checks on
the resolved integration tree. Passing checks on an archived draft or on a
historical branch do not validate the merge. Check the integration PR for the
actual check results; this document deliberately does not freeze test counts.
