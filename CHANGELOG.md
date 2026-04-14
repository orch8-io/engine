# Changelog

All notable changes to the Orch8 Engine will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Externalized state TTL garbage collector with configurable sweep interval.
- `externalized_state` foreign-key cascade and shutdown-drain semantics.
- Per-instance required-field context preload (`RequiredFieldTree`) to avoid fetching externalized payloads unused by the current step.
- Transactional `create_instance(s)_externalized` and `update_instance_context_externalized` paths.
- Batch preload of externalized markers in the scheduler claim cycle.
- zstd compression for externalized payloads ≥ 1 KiB.
- `ExternalizationMode` config enum (`never`, `threshold { bytes }`, `always_outputs`) with default `threshold { bytes: 65536 }`.
- ActivePieces sidecar integration via the `ap://` handler prefix.
- Swiss operator-console dashboard visual pass (proportional bars, live timestamps, click-to-copy IDs).
- Criterion benchmarks and preload metrics.

### Fixed
- Resolve externalization markers before step dispatch (§8.5).
- Resolve externalization markers in `GET /instances/:id/outputs` responses.
- Enforce size ceiling and apply field filter policy on all dispatch paths.
- CI: pass `--insecure` to the server in the E2E harness.
- CI: larger `Duration` units for clippy 1.95 compatibility.

### Changed
- Internal product docs (`STATUS.md`, `ROADMAP.md`, `PRODUCT.md`, `TOOLING.md`, `RELEASE_CHECKLIST.md`) moved to the private `orch8-io/docs` repo.

## [0.1.0] — unreleased

First public pre-release. Nothing has been tagged yet; this file will be updated on the first `v0.1.0` tag.

---

[Unreleased]: https://github.com/orch8-io/engine/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/orch8-io/engine/releases/tag/v0.1.0
