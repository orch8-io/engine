# Rust crate review ledger

> **Stability: n/a**, historical record.

Objective: review every Rust crate in `engine/`, including excluded native bindings
and the fuzz package, for edge cases, code smells, and optimization opportunities.
Started 2026-09-05. This is an active review, not a completed audit.

## Completion criteria

For each crate: inspect its manifest and production modules, trace consequential
error/lifecycle/concurrency boundaries into callers, inspect relevant tests,
record confirmed findings and disposition, and run applicable checks. Source file
counts include inline tests and coverage modules; they are inventory, not proof
of review. Optional feature and platform-specific build coverage must be recorded.
Performance changes require measured evidence; unmeasured ideas remain candidates.
No crate is complete merely because Clippy passes.

## Inventory

| Crate | Directory | Rust source files | Source lines | Review status |
|---|---|---:|---:|---|
| `orch8` | `orch8` | 13 | 3,138 | Source reviewed; host tests/Clippy passed |
| `orch8-types` | `orch8-types` | 55 | 20,735 | In progress |
| `orch8-storage` | `orch8-storage` | 91 | 42,553 | In progress |
| `orch8-engine` | `orch8-engine` | 100 | 76,224 | In progress |
| `orch8-mobile` | `orch8-mobile` | 27 | 13,022 | In progress |
| `orch8-publisher` | `orch8-publisher` | 13 | 6,170 | Source reviewed; host tests/Clippy passed |
| `orch8-push` | `orch8-push` | 10 | 4,401 | In progress |
| `orch8-api` | `orch8-api` | 64 | 29,078 | In progress |
| `orch8-grpc` | `orch8-grpc` | 10 | 6,664 | In progress |
| `orch8-server` | `orch8-server` | 7 | 4,577 | In progress |
| `orch8-cli` | `orch8-cli` | 40 | 13,883 | Production source reviewed; follow-ups/checks in progress |
| `orch8-node` | `packages/node-native` | 1 | 42 | Source reviewed; host checks passed |
| `orch8-python` | `packages/python-native` | 1 | 57 | Source reviewed; host checks passed |
| `orch8-fuzz` | `fuzz` | 2 | 19 | Source reviewed; locked build passed |

## Findings and changes

### Dependency refresh status

- `cargo update --dry-run --verbose` on 2026-09-23 found zero newer versions
  compatible with the current manifest constraints after the refresh. Nine
  locked packages have newer versions outside those constraints.
- A fresh `cargo outdated -w -R` scan on 2026-09-23 reported all direct workspace
  dependencies current. `cargo audit` refreshed the RustSec advisory database
  and found no vulnerabilities in the 659 packages in the current lockfile.
- Upgraded the shared `jsonschema` constraint from 0.54 to 0.57 while keeping
  `default-features = false` so remote `$ref` retrieval stays disabled. The
  lockfile updated `jsonschema`, `jsonschema-regex`, `jsonschema-value`, and
  `referencing` together. The locked workspace compiles; 1,901 engine library tests
  and 507 API library/sequence/preflight HTTP tests pass serially. The API
  regressions confirm an invalid field value returns 422 with its JSON pointer
  and multiple invalid fields retain both paths.
  Both crates pass all-target Clippy; a cached-advisory `cargo audit --no-fetch`
  scan found no vulnerabilities.
- SQLx 0.9 is a separate migration, not a patch refresh: the workspace still
  declares 0.8, including its removed combined runtime/TLS feature, and uses
  APIs affected by the 0.9 argument-lifetime and dynamic-query changes. The
  [upstream 0.9 changelog](https://github.com/transact-rs/sqlx/blob/main/CHANGELOG.md#090---2026-05-06)
  documents those breaks. Plan and verify it across types, storage, CLI, and
  mobile before changing the shared dependency.

### Engine: scheduler and retry paths (partial crate review)

- High, fixed: instance heartbeat floor could exceed short stale windows.
  `src/scheduler.rs` now derives cadence from one-third of the positive threshold.
- High, fixed: `tick_once` converted a failed pending-work count to an idle result.
  Storage errors now propagate to embedded callers.
- Medium, fixed: retry backoff ignored its cap on attempt zero and froze its
  exponent at 63. Full attempt range now works, including zero initial delay.
- Verified: 12 backoff tests, 31 scheduler unit tests, 105 scheduler integration
  tests; engine library Clippy with warnings denied, formatting and docs passed.
- Remaining: all other engine modules, broader cancellation and shutdown review,
  feature-specific paths. No full-crate completion claim.

### Engine: embedding and governed memory (partial crate review)

- Medium, fixed: `memory_store` accepted unbounded text when callers supplied
  a precomputed embedding, bypassing the provider-input ceiling. Supplied text
  now has the same 10 MiB limit on normal and dry-run paths; vector-only records
  remain valid. Unit and storage-backed tests cover the boundary and rejection
  before persistence.
- Medium, fixed: tenant policies could admit a retention value that fit `i64`
  but panicked in Chrono duration construction or timestamp addition. Both
  conversions now fail with a permanent handler error instead. A regression
  covers each overflow boundary.
- High, fixed: the shared outbound SSRF classifier did not inspect the IPv4
  payload inside IPv4-mapped IPv6 addresses. Redirect literals and connect-time
  DNS results now apply the existing IPv4 blocklist to mapped addresses;
  regressions cover loopback, metadata, and public mapped addresses.
- Medium, fixed: the explicit internal-URL opt-in let the redirect policy
  return before checking the URL scheme. HTTP(S) internal targets remain
  allowed under opt-in, but `file:` and other schemes are always denied.
- Medium, fixed: embedding-provider responses were body-read before HTTP status
  classification, so an oversized 429 or 5xx error body became a permanent
  size error instead of a retryable provider failure. Status is now classified
  before reading any error body; a regression covers oversized 429, 503, 400,
  and successful responses. The engine library suite passes 1,902 tests.
- Low, fixed: embedding responses with a non-2xx status and a valid vector body
  were accepted as success. Only 2xx statuses now reach response parsing;
  regressions cover informational and redirect statuses and oversized redirect
  bodies.
- Medium, fixed: a present non-string memory `residency` was treated as absent,
  silently selecting the instance default or bypassing the tenant policy's
  requested-residency check. Store, search, and delete now reject that input;
  regressions cover normal and dry-run handler paths. All 88 memory-handler
  tests, engine library Clippy, formatting, and diff checks pass.
- Medium, fixed: tenant search silently ranked only the newest 10,000 shared
  records, so a better older match could be absent without any indication.
  One lookahead record now distinguishes a complete corpus from a truncated
  scan, and the response exposes `corpus_truncated`. A storage-backed boundary
  test covers exactly 10,000 and 10,001 records; 89 memory-handler tests pass.
- Verified: all 1,902 engine library tests pass serially; engine all-target
  Clippy and crate documentation with warnings denied, workspace formatting,
  and diff whitespace checks pass. Full engine crate review remains open.

### Push: provider dispatch (in progress)

- High, fixed: factory-created dispatcher did not forward
  `send_signed_wake`, falling back to the trait's unsupported error even with
  a configured provider. Both delivery methods now share platform selection.
- Verified: 196 push tests pass (including existing localhost FCM protocol
  fixtures); library Clippy with warnings denied passes. Initial sandbox run
  failed 11 mock-server binds; rerun with local socket access passed all tests.
- Production files inspected: `lib.rs`, `apns.rs`, `fcm.rs`, `outbox.rs`,
  `governance.rs`; relevant provider, outbox, and governance tests inspected.
  Follow-up findings below keep the crate review open.
- High, fixed: the outbox now claims one wake immediately before each provider
  call instead of leasing the whole sequential batch at once. A 75-second
  provider-call timeout fits within a 90-second lease, covering the built-in
  FCM/APNs retry budgets. Due eligibility stays fixed at drain start so an
  earlier retry cannot re-enter the same batch. The store still fences stale
  outcomes after a lease is reclaimed. Delivery remains at-least-once: a
  timed-out request may have reached the provider before cancellation.
- Governance follow-up, fixed: the bounded nonce cache now rejects a fresh
  wake at capacity rather than evicting unexpired replay evidence. Existing
  nonces remain blocked; expiry frees capacity. The cache is still process-local,
  so cross-process replay protection requires a durable/shared nonce boundary.
- Medium, unresolved: collapse-key length prefixes use `usize::to_be_bytes`,
  giving different hashes on 32-bit and 64-bit builds. A fixed-width encoding
  needs a compatibility decision for existing 32-bit persisted keys.
- Optimization candidate: repeated APNs payload encoding inside retry attempts;
  no profile yet, no performance claim or change.

### Native bindings: source review and host compilation

- Medium, fixed: Node declarations omitted the Rust `runSequenceJson` export;
  Python's package root omitted `run_sequence_json`. Both public package
  surfaces now expose the existing bounded dry-run runner.
- Reviewed both manifests, complete Rust sources, loaders/declarations and
  READMEs; traced validation and bounded execution into `orch8::run_sequence_once`.
- Verified: locked offline `cargo check` and Clippy with warnings denied pass
  for both crates on aarch64 macOS. Node declarations type-check; Python package
  syntax parses. Packaged binary import/execution and other platforms remain
  unverified. No unsafe code added; PyO3 detaches the GIL around owned inputs.
- No performance changes proposed without host profiling.

### Fuzz harness: source review

- Medium: independent Cargo.lock was stale against current path dependencies;
  locked builds failed before compilation. Reconciled only dependencies Cargo
  required, preserving the remaining lock entries (31 package changes).
- Reviewed both harnesses and manifest: expression/template strings use fixed
  empty context and null outputs. This leaves nonempty context/output and
  nested JSON template cases outside coverage. `cargo check --locked
  --manifest-path fuzz/Cargo.toml --bins` passes after reconciliation;
  no fuzz campaigns have been run as part of this review.

### Publisher: production source review and host verification

- Inspected all eight production modules: manifest, registry, publish, CDN,
  package, distribution, capsule, and grant, plus exports/manifest and relevant
  package, registry, CDN and distribution tests.
- High, fixed: raw object-key delimiters were parsed as query/fragment syntax
  or existing percent escapes in S3 URLs. URL construction now encodes raw
  path segments, consistent with signing. Regression includes Unicode.
- Medium, fixed: channel selection disagreed with the canonical capsule/task
  capability contract. Allowed regions are alternatives; desktop runtimes
  qualify for human-UI requirements. Added contract-parity tests.
- Verified: 214 publisher unit tests including localhost S3 fixtures pass
  after both fixes; all-target Clippy with warnings denied passes. Another 352
  tests across `package_e2e`, `package_cases`, and `publisher_coverage` pass.
  Publisher/facade docs and workspace formatting pass. Live S3 is unverified.
- Follow-up: process-local manifest versions have no durable high-water mark
  across generator recreation; multi-writer manifest publication lacks the
  registry's conditional-head-write model. Requires publication ownership design.
- Follow-up: check complete one-to-one index/ledger correspondence, not just
  individual matches and equal counts, when improving registry verification.
- Package version comparisons use lexicographic numeric vectors (e.g. differing
  segment counts are significant). Keep that contract explicit; this is not a
  full SemVer parser. No silent SemVer migration applied.
- Optimization candidate: registry index verification scans the entire signed
  ledger for each version (quadratic lookup work). Benchmark before replacing
  lookups; retain complete verification and one-to-one correspondence checks.
- Canonical JSON performs intermediate tree allocations; no profile evidence
  yet and no change proposed solely from allocation counts.

### Embedded facade: production source review

- Inspected all production modules and facade docs/manifest: engine, builder,
  agent runtime, effect wrapper, portable capsules, storage, embedded runner,
  contract runner, errors and exports. Traced shared-clock defaults and
  shutdown behavior into the scheduler; inspected relevant integration tests.
- High, fixed: the default instance schedule used wall time despite an
  injected clock. Scheduling now uses the configured clock; creation and update
  audit timestamps retain wall time as required by `orch8-types::clock`.
  The public-API regression checks both contracts and passes after the audit
  timestamp correction (targeted facade integration rerun).
- Medium, fixed: manual ticks could still enter the scheduler after shutdown.
  They now return the existing ShuttingDown error before processing work.
- Medium, fixed: SQLite file paths were silently converted using lossy UTF-8.
  Invalid paths now fail explicitly instead of targeting a rewritten name.
- Safe cleanup: removed a redundant clone of the owned encrypted payload on
  capsule export. No benchmark speedup is claimed.
- Verified: 14 facade engine integration tests, 66 library tests (including
  the invalid-path regression), 74 contract-runner tests, and 3 event tests
  pass; all-target Clippy with warnings denied passes. Default features only;
  optional NATS/file-watch/WASM and remote PostgreSQL are not verified here.
- Follow-up: tenant is a creation default for several facade CRUD calls, while
  portable/effect APIs enforce a boundary. Clarify and consistently enforce
  the intended shared-backend tenant contract before presenting it as isolation.
- Follow-up: facade storage dependency enables compression by default even
  when facade default features are disabled. Evaluate feature forwarding and
  compatibility with previously persisted compressed data.
- Lifecycle limitation remains explicit: manual and background driving must
  not be mixed; dropping the handle does not replace explicit shutdown.

### Types: pool quotas and shared contracts (partial crate review)

- Medium, fixed: a computed zero warmup allowance was mistaken for an unlimited
  daily cap. Only an explicitly configured `daily_cap == 0` is unlimited.
- Medium, fixed: a warmup starting cap above the daily cap could exceed the
  daily quota. The starting allowance is now bounded by the daily cap.
- Verified: all 629 types library tests pass, including both new regressions.
  All-target types Clippy with warnings denied, workspace formatting and diff
  whitespace checks pass. Traced capacity selection through
  `orch8-engine/src/scheduling/pool.rs`.
- Production modules inspected: clock, filter, instance, pool, rate_limit,
  circuit_breaker, queue_dispatch, queue_routing, ids, webhook_outbox, rollback,
  webhook_delivery, worker, worker_filter, continuity, cron, dedupe, context,
  checkpoint, output, execution, redaction, artifact, failure, event_correlation,
  auth, session, signal, config, audit, cluster, interceptor, plugin, step_log,
  credential, api_key, trigger, dlq, suggest, finding, preflight, template_trace,
  diagnosis, release, contract, encryption, error, continuity_advanced,
  continuity_product, sequence, and lib; types manifest inspected.
  All production modules have now been inspected. Caller tracing, relevant
  test inspection and the unresolved findings below keep the review open.
- Medium, fixed: `BlockType::ABSplit` serialized as `a_b_split`, while
  workflow definitions and SQL writes use `ab_split`. It now emits `ab_split`
  and accepts the legacy alias, matching `BlockDefinition`. The new test
  failed on the old spelling before the fix; checks cover every block variant.
  After the fix all 630 types library tests pass.
- Corrected authentication helper docs: only the fixed-length digest comparison
  is constant time; hashing work depends on input length and digest equality
  relies on SHA-256 resistance to alternate inputs. No algorithm change.
- Medium, fixed: configuration validation accepted zero worker-reaper,
  node-reaper, and cron intervals, which reach Tokio interval constructors.
  Each now produces a configuration error. Unknown artifact backends also
  fail the common validation pass instead of waiting for backend assembly.
  Verified: all 632 types tests and all-target Clippy with warnings denied pass.
  Workspace formatting passes after rustfmt; diff whitespace checks pass.
- Config follow-up: direct embedded `SchedulerConfig` construction does not
  pass through `EngineConfig::validate`. Review the engine/facade start boundary
  and platform timer limits; these startup checks are not proof that every
  direct library entry point validates configuration.
- Corrected worker stale-threshold documentation to use heartbeat cadence plus
  delay, and clarified equal-length timing semantics of `SecretString` equality.
- DTO review: secret-bearing credential fields use `SecretString`; API-key
  expiry is exclusive and generated secrets use two UUIDv4 values. Remaining
  tenant scope, persistence, and producer-redaction enforcement belongs to the
  storage/API review, not to these serialization-only records.
- Medium, fixed: package verification returns trust failures to its caller
  rather than calling process::exit inside a helper. Regression checks a validly
  signed package with a nonmatching trust key produces a normal error. Package
  tests passed: 29 package tests and nine sequence tests; all-target Clippy
  passed with warnings denied. Formatting and diff checks passed.
- Package registry follow-up: registry index/ledger reads lack body caps and
  request deadlines; package downloads are streamed with a 64 MiB cap but lack
  a deadline. Downloaded package identity/hash/key is not compared to the selected
  registry record before install. Independent package trust verification does not
  establish the requested name/version binding. Registry verifies absolute package
  paths, so a suspected relative URL join issue is not supported by that contract.
- Package install follow-up: syntactically valid lock entries without a version
  skip downgrade checks; lock entries are not scoped to server/tenant and writes
  lack interprocess serialization. Conflict checks and uploads interleave, leaving
  partial installs on later failure; retries then conflict before provenance is
  recorded. Contract files without matching sequences are skipped. Preflight
  status and malformed reports are swallowed; created IDs default to empty.
  Rewritten namespaces are not reconciled with packaged sub-sequence references.
- Package build/key follow-up: file reads are unbounded and follow symlinks;
  sequences deserialize but are not fully validated. Key bytes and input strings
  are not explicitly zeroized, and CLI key arguments are visible in process args.
  Inspection intentionally reports failed integrity but returns success; distinguish
  this informational command from verification in documentation/tests.
- Pieces follow-up: npm failures propagate, but child execution lacks deadline or
  cancellation-on-drop; install uses an unpinned registry package with npm's normal
  lifecycle scripts. Catalog uses a separate credential-free client, but no
  timeout/body cap. These behaviors need explicit installer documentation.
- Medium, fixed: portable HTTP and MCP adapters share the engine streaming
  body reader, enforcing the 1 MiB limit during download as well as rejecting
  oversized declared lengths. Previously the full body was allocated before
  the post-read check. Local HTTP regression covers valid and oversized bodies
  without Content-Length. All six portable tests passed (348 filtered out);
  all-target Clippy passed after explicit match/test-read lint fixes. Diff checks
  passed. Native adapters and real gateway interoperability remain unverified.
- Portable lease follow-up: poll ignores lease_secs/heartbeat_interval_secs;
  adapters send no heartbeat or checkpoints and ignore resume_checkpoint.
  Complete/fail correctly include claim_epoch, but task execution may outlive
  authority. Transport/parse/spawn errors exit the worker without failure reporting.
  Polled batches are processed sequentially without validating the requested limit.
- Portable execution follow-up: local stdin write precedes timeout and output
  drain, allowing pipe stalls; stdout/stderr buffer fully before size checks.
  kill_on_drop covers the direct child, not a process tree. HTTP/MCP use client
  timeout instead of task timeout and the control-plane client (default auth
  headers) for gateway requests. Gateway redirects, credential scope and enrollment
  must be explicit. MCP response ID/version and tool-result isError are unchecked.
  Wrapper accepts MCP entrypoints that the HTTP executor does not implement.
- Portable capability follow-up: hardware/plugins/credentials/regions are copied
  from policy requirements rather than probed; connectivity is fixed Ethernet.
  Registered trust is correctly not elevated by a manifest, but factual capability
  advertisement needs a host registration/probe contract. Worker polling delay has
  a 100 ms floor but no maximum hint bound or retry/backoff on network failure.
- Doctor follow-up: sequential independent checks can consume several 60-second
  client timeouts. JSON readability is often treated as schema validity; missing
  version or diagnosis fields produce no warning. Major-only compatibility is
  weak for 0.x releases. Doctor emits raw evidence and errors; redaction needs
  tracing into sensitive endpoint responses. No broad compatibility claim.
- Medium, fixed: debugger open now sends the bounded requested timeline limit
  to the API; previously it always fetched the default 200 even for --limit 500.
  Regression verifies the outgoing query alongside the other two evidence calls.
- Fixed: support bundle transport errors strip the request URL before export,
  avoiding query-credential disclosure. Regression uses an unsupported transport
  scheme to verify the exported error contains no URL. Both debugger tests and
  all 54 support-bundle tests passed; all-target Clippy passed with warnings
  denied. Formatting and diff checks passed.
- Support bundle follow-up: key-based sanitization is not a strict disclosure
  guarantee for free-form error/diagnosis strings, generic URL fields or server
  response values. Typed config SecretString serialization already redacts database
  URL; no database-config secret leak is claimed. Workload summaries discard failed
  fetch status and can imply an empty fleet. Request bodies are buffered before
  sanitization, and no total bundle byte limit exists. Requests run sequentially.
- Debugger follow-up: timeline/checkpoint/effect bodies are buffered in full;
  limiting displayed rows is not a memory bound. Unknown envelope objects bypass
  array truncation, and table counts can silently become zero. Timeline paging
  metadata is discarded, so truncation is not explained. Default requests still
  include workflow outputs, although the API supports metadata-only responses.
  Live fork remains explicitly opted in by flag; no actual live fork was run.
- Medium, fixed: continuity request reads now inspect the opened file and cap
  the actual read at 16 MiB plus one detection byte. A changing file or reader
  without useful metadata cannot trigger an unbounded read. Tests cover exact
  limit, oversized stream, metadata rejection and normal JSON. Capsule export
  uses the existing atomic-write helper instead of truncating the destination.
  All three continuity tests passed; CLI all-target Clippy passed with warnings
  denied. Formatting passed.
- Continuity CLI follow-up: handoff checks compatible=true but does not validate
  preview_sha256 before posting; export indexes missing capsule as null. Offline
  verification checks signature/trust, not expiration or execution compatibility.
  Import relies on server verification as expected, but help implies verification
  already occurred locally. Generic proof endpoints print HTTP-200 responses
  without mapping negative semantic verdicts to exit failure; contracts need
  tracing before changing all such commands. Responses lack byte caps; file reads
  remain synchronous and special files can block despite byte bounds.
- Generation follow-up: prompt/provider bodies and repair conversation bytes are
  unbounded. Timeout is per attempt (up to eight), not an overall budget. Schema
  URL is a prompt reference, not a supplied schema. Provider/tool availability
  and existing output overwrite policy remain separate from strict DSL validity.
  Programmatic attempts=0 hits unreachable(), though Clap restricts CLI values.
- Bootstrap follow-up: readiness can exceed deadline by request duration and
  polling delay; it accepts any successful service on the configured port without
  binding readiness to the spawned PID. Environment overrides inherited by server
  may diverge from the file-derived readiness URL. Ctrl-C kills rather than asking
  the engine to drain; relative server executable resolution after changing cwd
  needs documented behavior. Readiness does not independently prove migrations ran.
- Template follow-up: catalog response has an 8 MiB streaming cap, but its
  independently constructed client has no overall/connect deadline. Downloads
  check UTF-8 only, despite claiming JSON; inline payloads are arbitrary JSON.
  Pull overwrites existing output through atomic_write without conflict handling.
  Built-ins take precedence over catalog names; URL errors can include credentials.
  Embedded-template tests check block deserialization, not full authoring validity.
- Init follow-up: rerunning generates fresh secrets even when the config is
  skipped, so printed credentials and newly created Compose files can disagree
  with existing configuration. exists-then-write can overwrite concurrent files;
  config permissions tighten only after creation, and Compose carries the same
  secrets without the Unix permission restriction. Default Postgres credentials
  and all-interface port publishing require an explicit local-development contract.
  Multi-file writes can leave partial scaffolds on failure.
- Release follow-up: rates remain visible below `min_sample` while the verdict
  is inconclusive; corrected the field documentation to match. Trace gate
  threshold validation and aggregate-count invariants in release callers before
  treating directly constructed `f64` gates and stats as validated input.
- Suggestion follow-up, fixed: the acceptance threshold now counts characters
  like Levenshtein distance, rather than UTF-8 bytes. Candidate case is
  normalized for comparison while preserving original spelling in suggestions;
  multibyte regression tests cover close and unrelated strings.
- Medium, fixed: contract range assertions accepted non-finite native bounds
  and lacked reversed-range authoring validation. One shared validator now
  rejects missing, non-finite, and reversed bounds at both suite validation and
  direct assertion evaluation. Inclusive equal endpoints and one-sided ranges
  remain valid. All 633 types tests passed after this change.
- Medium, fixed: SQLite `SQLITE_BUSY_TIMEOUT` (773, confirmed in bundled SQLite
  headers) was missing from transient storage error codes, unlike the other
  busy/locked variants. Added it to the mapper and existing classifier tests.
  Verified after both fixes: 633 types tests, all-target Clippy with warnings
  denied, workspace formatting and diff whitespace checks pass.
- Encryption review: nonce use tracking is shared across clones, but not across
  independent handles/processes/restarts using the same key. It is telemetry,
  not a durable global key budget. Plaintext JSON temporary buffers and partially
  decoded malformed-key buffers are not zeroized; consider `Zeroizing` ownership
  for these buffers during encryption hardening. No cryptographic algorithm change.
- Product-contract follow-up, fixed: lifetime validation now compares elapsed
  durations without adding to caller timestamps, and profile offer construction
  returns an error if its expiry is outside Chrono's range. Boundary tests cover
  offer, passport, and certificate behavior near the maximum timestamp.
- Product-contract follow-up: seven mandatory conformance checks score 875/1000
  when optional offline-resume is absent; the certification threshold of 900
  effectively requires all eight. Reconcile the optional-check contract and
  scoring policy before changing persisted certification semantics.
- Optimization candidate: conformance scoring repeatedly allocates matching
  result vectors to establish uniqueness. An iterator can establish zero/one/many
  matches without collecting, but no measured performance change is claimed.
- Product-policy follow-up, fixed: explicitly empty allow-lists and blank list
  items now fail validation, as do separator-only policies. Omitted allow-lists
  remain unrestricted; `classification=internal` explicitly expresses the
  default internal placement rule. Parser regressions cover both behaviors.
- Medium, fixed: sequence retry validation accepted native non-finite
  multipliers. It now requires a finite positive multiplier, retaining valid
  decreasing schedules below 1.0.
- High, fixed: sequence handler discovery omitted cleanup, fallback, escalation
  and interceptor handlers. Delegation eligibility and contract mock registration
  now receive the complete local handler set. Referenced sub-sequences remain a
  separate lookup. Added discovery and retry-boundary regressions; 635 types
  tests pass. All 74 facade contract-runner integration tests pass, as do
  all-target types Clippy with warnings denied and workspace formatting.
- Sequence follow-up: strict unknown-field detection combines a hand-maintained
  block field list and serde's ignored-field reporting; nested internally tagged
  adapters need coverage when new nested policy fields are added. Decode-error
  diagnostics clone and re-decode subtrees; profile before optimizing.
- Sequence follow-up: validation depth and total block caps do not independently
  bound empty A/B variants or router branches. Trace branch-index limits in the
  evaluator before claiming every validated definition is executable.
- Traced capsule limits into engine export/import and mobile transport: element
  counts and encoded plaintext limits are separate checks. Engine import checks
  plaintext size after decryption; mobile also bounds transported bytes first.
  Review consistent pre-decryption limits for other import surfaces later.
- Worker version comparison is intentionally numeric-with-zero-padding with
  lexical fallback; it differs from publisher package-version comparison.
  Existing tests codify both behaviors. Do not silently replace either with
  SemVer or assume their ordering rules are interchangeable.
- Context field selection on non-object data deliberately copies the complete
  value when any field is allowed. Documented behavior, but callers must not
  present that as field isolation for arrays/scalars.
- Redaction follow-up: generic text and textual URL filtering are heuristic,
  not complete secret removal. Audit producer sanitization and URL parsing
  boundaries before claiming all inspector metadata is redacted.
- Optimization candidates: context size checking already counts serialized bytes
  without allocating output; changing it to early termination would sacrifice
  its exact-size diagnostic. Failure normalization collects complete tokens
  before bounding output. Profile large diagnostics before altering either.
- Medium, fixed: nested approval reporting now preserves the handler, prompt,
  choices, storage key and timeout via the engine's recursive block lookup.
  Pending notifications use the same engine traversal. Regression uses the actual
  nested onboarding fixture, including absent/non-step lookup behavior. Oversized
  timeout conversion saturates at i64::MAX instead of wrapping negative.
- Reporter recovery follow-up: execution markers precede command effects without
  a shared transaction or recoverable in-progress state. Crash/cancellation in
  that window can acknowledge unapplied work on redelivery. Failed marker removal
  after a retryable result has the same consequence. Simply moving markers after
  effects introduces duplicate effects; a durable per-command recovery contract
  is needed. Concurrent sync calls also need command ownership fencing.
- Reporter command follow-up: graceful update only logs, while skip_executed
  starts fresh without copying the execution tree. Restart may cancel before
  discovering a missing sequence name; cancel/fail writes can replace terminal
  states. Failed delegation results are logged then acknowledged without waking
  the waiting instance. Signal commands bypass the active-instance enqueue path.
- Reporter bounds follow-up: outbound row counts are bounded (170 outbox rows,
  100 acks); response JSON and command count are not capped. Invalid stored JSON
  is silently filtered, then those rows are deleted as sent. Approval entries
  remain queued after local completion, and scans can starve older instances.
- Manifest recovery follow-up: skipped transient downloads still advance ETag and
  manifest version, so the same manifest cannot retry its missing entries. Missing
  handlers/SDK upgrades face the same retry issue. Metadata writes are separate;
  removals and eviction errors can be swallowed before recording sync success.
- Manifest retention follow-up: eviction and replacement use sequence deletion,
  which cascades into execution data; active sequence references need preservation.
  Local version listing stops at 1000 names. Removed entries can be counted twice
  during explicit plus full reconciliation, and deletion selects only the latest
  stored version. Downloaded definition identity/namespace/version should be
  checked against manifest metadata before persistence.
- Manifest transport/trust follow-up: Retry-After is unbounded; signature URL uses
  whole-string replacement, including query values. Bearer auth is forwarded to
  each manifest-provided download URL; origin policy needs a defined contract.
  Key-set updates are non-atomic and failed revocations are swallowed while cached
  key fallback remains enabled. Audit these defensive boundaries with publisher
  contracts before changing them. This pass used static inspection only.
- Storage follow-up: PostgreSQL pool writes convert unsigned counters and caps
  with `as i32`, unlike SQLite's lossless `as i64` conversion. Values above
  `i32::MAX` require a consistent validation or wider-schema contract; do not
  count this as resolved by the warmup fixes.
- Webhook follow-up: `set_error` bounds excerpts but does not itself redact
  them. Verify transport error sanitization and endpoint visibility before
  relying on the type documentation's redacted-metadata promise.

### Storage: boundary review started

- Inspected manifest, exports and initial sequence-store trait surface; complete
  production source of compression, artifact store, API-key cache and both
  sequence-store implementations, tenant partition routing, and conformance
  suite, both rate-limit implementations and both circuit-breaker persistence
  implementations reviewed. Both complete worker-task persistence modules and
  both signal persistence modules are now source-reviewed as well.
  Both cron, webhook-outbox and push-outbox persistence implementations are
  source-reviewed; relevant engine cron/webhook callers were traced selectively.
  Most traits, decorators and both backend implementations remain pending.
- Medium, fixed: SQLite and PostgreSQL cluster-node row decoders treated an
  unknown persisted status as `active`. Node listing now returns a storage
  error instead of misreporting the node; corrupt-row regressions and all 24
  live PostgreSQL integration tests pass.
- Sequence persistence follow-up: both backends omit `$schema` and schema version
  on write and reconstruct the current version on read. Trace immutable sequence
  identity/digests and future-version compatibility before treating this as a
  lossless definition round trip.
- Medium, fixed: SQLite sequence deletion expanded every dependent instance
  into a bind parameter. It now uses scoped subqueries with one parameter per
  statement, independent of instance count. Regression checks deletion scope
  and rollback of dependent history when replacement insertion fails. All 95
  storage coverage integration tests pass after the deletion and mapper fixes.
- Sequence storage follow-up: several SQLite bulk reads still build one
  parameter per id without chunking. Externalized-state batch reads and
  sequence batch lookups now use 500-key chunks, with >32,766-key regressions.
  Signal inbox batch reads and deliveries also use 500-id chunks; delivery
  retains one transaction, and multi-query reads use one snapshot. The
  regression covers duplicate IDs and
  >32,766 inputs. Deletion and
  replacement still rely on callers to prevent deleting active instances;
  verify atomic admission/transition fencing in API and mobile callers.
  The storage library and SQLite integration suites pass serially: 461 tests.
- Medium, fixed: SQLite instance-KV and shared-knowledge batch deletes bound
  every key in one statement, so memory retention cleanup could fail above
  SQLite's variable limit. Both paths now use 500-key chunks inside one
  transaction. Regressions cover 32,768-key deletes, scope isolation, and
  rollback when a later chunk fails. The sibling PostgreSQL deletes now bind
  one `text[]` array with `ANY`, removing their per-key placeholder limit;
  a live 65,536-key regression and all 19 PostgreSQL integration tests pass
  against a fresh PostgreSQL 14 database.
- PostgreSQL test-isolation follow-up: integration tests share database tables.
  Re-running the suite against a previously used database can make global
  scheduler/outbox assertions see old rows. The worker-row lock test now probes
  its own instance directly; the outbox recovery test still verifies it can
  reclaim its own row. The full suite is verified on a fresh database, not
  proven repeatable against a populated one.
- Compression follow-up, fixed: externalized-state single, bulk, and instance
  writers now reject serialized JSON above the reader's 16 MiB limit before
  persistence. Builds without compression write inline rows rather than labeling
  raw JSON as zstd; their reader retains legacy mislabeled-JSON compatibility
  and reports actual zstd frames as unsupported. Unit and SQLite integration
  tests cover the size boundary, codec marker, and bulk-write atomicity.
- Medium, fixed: artifact errors no longer uniformly become retryable `Backend`.
  Typed permission/configuration/unsupported errors are permanent; conditional
  conflicts and missing artifacts retain their distinct storage variants.
  Unclassified operational errors remain retryable. All 13 artifact tests pass.
  Object-store variants checked against the pinned dependency source.
- Artifact follow-up: audit generic error sources and read-size bounds; the
  typed mapper cannot infer every permanent cause wrapped in a generic error.
- Conformance follow-up: core sequence comparison checks identifiers and block
  count, not complete definition equality; its empty-block fixture is invalid
  under authoring validation. Successful conformance is not proof of lossless
  definition persistence or comprehensive backend isolation.
- Partition-router follow-up: placement changes select a backend for subsequent
  calls but do not fence already-returned backend handles. Epoch advancement
  alone is not proof that tenant migration has quiesced old writers.
- High, fixed: SQLite rate-limit checks used raw BEGIN/COMMIT and manual error
  rollback, bypassing SQLx transaction ownership on cancellation. They now use
  the existing `begin_immediate` helper and transaction guard; error/drop paths
  are handled by SQLx. All six rate-limit integration tests pass, including the
  error rollback/pool recovery regression. Storage all-target Clippy and workspace
  formatting pass after this change. The preceding artifact/deletion changes
  also pass all 322 storage library tests and 95 storage coverage tests.
- SQLite lifecycle follow-up: several other modules still use raw transaction
  statements. Audit each cancellation/commit path and migrate where appropriate.
- High, fixed: the three SQLite worker claim variants and stale-task reaper now
  use SQLx immediate transaction guards. Capability matching previously had
  unguarded row-decoding and event-write error exits inside a raw transaction.
  Conditional signal enqueue now also uses a guard. Removed obsolete comments
  claiming SQLx could only begin deferred transactions and manual rollback code.
  Worker SQL errors now retain the shared typed error conversion, including
  retryable lock/connection errors, rather than always becoming `Query`.
  All 126 storage integration/bugs-group-B tests pass, including an injected
  attempt-event persistence failure for all three claims and the reaper: task
  mutation rolls back, no event is retained, and a subsequent transaction works.
  All four conditional-signal unit tests pass. Final storage all-target Clippy
  with warnings denied, workspace formatting and diff whitespace checks pass.
  Cancellation ownership was checked against SQLx 0.8.6 Transaction::drop,
  which queues rollback; the regression injects persistence failure rather
  than attempting to time cancellation inside SQLite's worker thread.
- Worker persistence follow-up: SQLite reaping is unbounded and builds one bind
  per task plus seven per attempt event; PostgreSQL limits reaping to 1,000 rows.
  Both capability claimers reserve memory from the caller's full limit and scan
  pages while holding locks, including incompatible candidates. HTTP poll callers
  cap requested claims at 1,000, but storage/native callers and scan work need
  independent bounds. Measure contention before choosing scan-budget semantics.
- Worker backend parity follow-up: PostgreSQL creation omits ownership/result
  fields supplied to SQLite creation, casts attempts to i16, and does not reap
  claimed rows with NULL heartbeat; SQLite does. Both saturate oversized epochs
  into signed database integers. Trace accepted creation states, retry bounds,
  and epoch validation across callers before changing the persistence contract.
  SQLite timeout SQL rounds to seconds; PostgreSQL retains fractional seconds.
- Worker evidence follow-up: SQLite bulk transition code silently filters invalid
  task IDs/epochs from evidence, while PostgreSQL substitutes epoch zero for
  invalid signed epochs. Define explicit corruption handling rather than treating
  successful task mutation as proof of complete attempt evidence.
- Signal backend parity follow-up: SQLite batch pending reads include requested
  IDs with empty vectors; PostgreSQL omits IDs with no pending signals. SQLite
  batch binds are unchunked. Caller semantics and practical batch sizes remain
  to be traced; enqueue/terminal guards are separate from these issues.
- High, fixed: retrying a collapsible push enqueue superseded pending work before
  ignoring its duplicate insert. Both backends now insert first and supersede
  other rows only for a newly inserted command. Replays preserve both the
  original ID and the current pending replacement; PostgreSQL retains the
  per-collapse-scope transaction advisory lock. SQLite and live PostgreSQL
  regressions cover duplicate enqueue and replay after replacement.
  All 18 PostgreSQL integration tests pass against an isolated temporary local
  database, run serially with DATABASE_URL configured (none skipped). This adds
  live backend evidence for prior rate-limit, worker and sequence paths covered
  by that suite; it is not full PostgreSQL storage conformance coverage.
- High, fixed: SQLite cron, webhook-outbox and push-wake claims now use immediate
  transaction guards, including decoding and commit error/drop paths. Removed
  obsolete cron commentary describing a SELECT/per-ID update loop that no
  longer exists. All 323 storage library tests and 122 SQLite integration tests
  pass after these changes; storage all-target Clippy passes.
- High, unresolved: cron claiming writes `last_triggered_at >= next_fire_at`,
  preventing subsequent claims until fire times advance. Instance creation and
  advancement happen later in engine `trigger_cron_schedule`; a process loss or
  failed advancement can strand the schedule. The instance idempotency key does
  not rearm that schedule. No stale cron-claim recovery was found in the reviewed
  paths. Design durable claim recovery plus concurrency fencing before claiming
  the existing crash-retry comments describe an end-to-end guarantee.
- Webhook follow-up: failure updates and successful-delivery deletion match only
  row ID, without claim ownership fencing. Engine stale recovery includes a
  retry-duration margin, but that margin does not fence a stalled older sender.
  Missing delivery IDs are regenerated per attempt, and SQLite silently drops
  malformed optional UUIDs while parsing rows. Trace legacy rows and receiver
  idempotency before changing replay behavior.
- Push follow-up: PostgreSQL claims commit before per-device target reads, so a
  device change/read failure can leave a claimed batch without returned work;
  target reads are N+1 and can observe later registrations. Both outcome writers
  clear a device's current token on invalid-token response without comparing it
  with the token used for the attempt. Lease identity is a caller-supplied expiry
  timestamp; validate monotonicity and token refresh races in callers. Device IDs
  are globally unique in the current mobile migration; a missing tenant filter
  alone is not evidence of duplicate device IDs across tenants.
- Rate-limit follow-up: expiry arithmetic and invalid stored counters need
  cross-backend validation. PostgreSQL and SQLite differ when nonpositive limits
  coexist with negative counters; API/native entry validation must be traced.
- Circuit-breaker persistence follow-up: both backends cast unsigned cooldowns
  into signed BIGINT without checking the range, and cast signed stored values
  back to unsigned without rejecting negative values. PostgreSQL's migration
  uses INTEGER for failure counts/thresholds while the domain permits full u32
  values. Trace configuration/registry validation before choosing shared bounds
  or a schema migration. These modules use single-statement writes and do not
  repeat the manual transaction ownership problem.
- API-key cache follow-up: process-global entries are keyed only by key hash,
  independent of the supplied storage backend. Embedded multi-backend hosts need
  scoped cache ownership. Concurrent misses are not coalesced, so documented
  at-most-one read/touch per TTL is not guaranteed under concurrency.
- API-key cache follow-up: invalidation does not fence an in-flight lookup that
  later inserts its fetched record. The documented immediate local revocation
  guarantee needs a concurrency design; static review only, no exploitation run.

### gRPC review in progress

- Inspected manifest, build script, exports, complete production auth module,
  service construction/sanitization, telemetry validation, runtime helpers,
  artifact preparation/transfer, worker stream negotiation and frame handling,
  instance/sequence/cron/pool/resource RPCs, serialization and storage-error
  mapping, step lookup and retry-task construction. Worker completion/failure
  orchestration and the complete proto contract are now source-reviewed, completing
  production-source inspection. Broad test review, caller tracing and unresolved
  findings remain. Server TLS/auth wiring was traced selectively.
- High, fixed: workload-only authentication now fails closed before the
  permissive-mode branch. A configured workload identity registry remains an
  authentication requirement regardless of unrelated request metadata. Static
  defensive review; no authentication-bypass reproduction was performed.
- High, fixed: initial worker command delivery happened before returning the
  response stream. The hello can already fill a capacity-one channel, causing
  handshake deadlock when commands are queued. Initial command delivery now
  runs in the spawned stream task, with errors returned through that stream.
  The runtime-session integration test now uses capacity one and a bounded
  handshake deadline.
- Medium, fixed: gRPC signal delivery now uses `enqueue_signal_if_active`,
  matching HTTP's atomic terminal-state guard. The signal integration test
  verifies rejection leaves the pending signal count unchanged.
- Validation: all 251 gRPC library tests and nine transport integration tests
  pass on the final changes. Transport tests initially failed on sandbox-denied
  listener binds; rerun with localhost access passed. All-target Clippy with
  warnings denied, workspace formatting and diff whitespace checks pass. This
  does not establish full crate or mTLS transport conformance.
- Follow-up: worker stream capability advertisements are persisted but Demand
  still calls the legacy unconstrained claim methods. RuntimeHeartbeat sends
  control commands without checking placement-command negotiation; cancellation
  frames likewise need feature-contract review. Session worker IDs/control
  command scoping require caller tracing.
- Follow-up: artifact transfer reads and hashes the entire object before chunk
  streaming and retains it while waiting for acknowledgements. Receiver closure
  and idle acknowledgement deadlines are not explicitly selected while waiting
  on inbound frames. Measure memory/idle connection behavior and define limits.
- Follow-up: instance retry deletes tree, clears sentinel outputs, resets run
  identity and changes state in separate writes. Creation sanitization resets
  only selected lifecycle fields, context update accepts the complete runtime
  context, and cron creation lacks sequence ownership/authoring parity checks.
  Trace shared policy and atomic storage contracts before changing these APIs.
- Medium, fixed: malformed warmup dates now return InvalidArgument before create
  or update persists anything, matching HTTP validation. The regression verifies
  rejection creates no resource and changes neither existing date nor name.
  Pool strategy uses its existing FromStr parser rather than JSON encode/decode.
  All 252 gRPC library tests and all-target Clippy pass after these changes.
- Resource follow-up: update/delete fetch every pool resource to find one ID,
  with no measurement of large-pool cost. Trace a scoped single-resource lookup.
- High, unresolved: completion records a worker task Completed before merged
  context validation and before output/node/instance persistence. Validation or
  later storage errors can return failure after consuming the claim. Failure
  handling likewise records Failed before retry/reset work. Trace engine
  reconciliation and design a shared atomic acceptance contract, including
  concurrent context merge and transport retry semantics.
- Protocol follow-up: proto worker polling/streams expose no checkpoint RPC;
  capabilities are a JSON advertisement without a matching claim route in the
  stream implementation. Pagination offsets narrow storage u64 to proto u32.
  Message byte limits differ between raw JSON helpers, negotiated frames and
  tonic transport defaults; encode overhead and large output handling need
  explicit conformance coverage.

### Server review in progress

- Manifest and all production source inspected: telemetry (both feature variants),
  managed control, benchmark and main startup/shutdown/configuration. Broader
  preflight/node-role tests and cross-module lifecycle tracing remain to review.
- High, fixed: managed control advertised worker protocol version 1 while the
  gRPC server accepts version 2. Both now use an exported protocol constant.
  The handshake builder is shared by production and its existing test, replacing
  a hand-built test frame that could pass while production drifted.
- Medium, fixed: benchmark full-set reads no longer treat missing rows/errors
  as completion; engine failures are reported, and a timed-out engine join is
  explicitly aborted/awaited instead of detached into subsequent benchmarks.
  Benchmark tenant identifiers now use full UUIDs instead of the common leading
  timestamp prefix of UUIDv7. No benchmark workload was executed or speed claim
  made. All 70 managed-control tests and the full 213-test server binary suite
  pass. The full suite needed localhost access for one listener-conflict test;
  the sandbox run passed the other 212. Server all-target Clippy with warnings
  denied passes, including the benchmark binary. Workspace formatting and diff
  whitespace checks pass. Non-default-feature/platform builds remain unverified.
- Build verification note: utoipa-swagger-ui's build script downloads frontend
  assets despite Cargo offline mode. Downloaded its exact requested v5.17.14
  release to `/private/tmp/orch8-review-swagger-v5.17.14.zip`; checks use
  `SWAGGER_UI_DOWNLOAD_URL=file:///private/tmp/orch8-review-swagger-v5.17.14.zip`.
  No dependency versions changed. Reproducible offline packaging needs vendored
  or separately cached assets with integrity policy.
- Managed-control follow-up: shutdown awaits bounded-channel sends without a
  deadline before its documented short flush sleep; connect/handshake does not
  select shutdown and handshake lacks an explicit deadline. Command ACK enqueue
  is not proof the remote server processed the ACK. Reconnect backoff never
  resets after a healthy session and has no jitter.
- Telemetry follow-up: disabled/enabled exports share a guard interface, but
  provider shutdown still depends on upstream timeout behavior. Export-layer
  target-prefix filtering can exclude unrelated similarly named targets; claims
  of no runtime cost/retry behavior need dependency-level evidence or measurement.
- Benchmark follow-up: global claims are not tenant-scoped and cleanup ignores
  failures while retaining rows; use a dedicated database. Full completion polling
  performs N sequential reads and can exceed the outer deadline inside that loop.
- Startup follow-up: storage migrations and managed-control/push tasks precede
  final authentication/listener validation. Early errors rely on runtime teardown
  rather than a coordinated cleanup path. Auth banner prioritizes the insecure
  opt-in flag even when a configured API key still enables authentication.
- Shutdown follow-up: HTTP graceful shutdown is awaited before the 30-second
  drain timeout starts, so long-lived requests can prevent entering that budget.
  Drain timeout drops join handles without aborting their tasks, then unbounded
  breaker flush and OTLP shutdown follow. The configured readiness flag is shared
  by multiple subsystems and is not a complete readiness barrier for startup.
- Push assembly follow-up: build_app_state installs NoopPushProvider unconditionally
  while applicable roles start the durable outbox worker. Confirm intended hosted
  injection/configuration before describing standalone server push delivery as
  operational; this review has not changed provider selection.

### Mobile review in progress

- Inspected manifest; complete production config, error, runtime, memory,
  tick-controller, notifier, foreign-handler bridge, lifecycle, mobile storage,
  sync, sync-reporter, telemetry, privacy, capabilities and continuity modules,
  the remaining facade methods, and the bindgen entry point. Mobile production
  source inspection is complete; consequential caller/state-transition follow-ups,
  broader test review and native platform validation remain open.
- Medium, fixed: mobile construction rejects zero step/instance concurrency and
  step counts exceeding the platform semaphore limit before opening storage.
  Tests verify invalid construction has no database side effect and cover the
  platform-dependent upper bound. Zero steps otherwise leaves a semaphore with
  no permits; zero instance capacity prevents all admissions.
- Fixed: manual ticks release their scheduler guard before notifier and GC work,
  matching foreground lock scope. Notification callbacks themselves are spawned
  without awaiting them; this is lock-contention reduction, not proof of an
  unconditional callback deadlock. Shutdown is checked before memory sampling.
- Medium, fixed: lifetime GC uses checked clock subtraction and clamps unrepresentable
  horizons to the earliest timestamp. It no longer substitutes a 24-hour lifetime
  on conversion failure or panics on date subtraction overflow. Regression checks
  extreme horizons preserve two-day-old work while normal one-day GC expires it.
- Validation: all 321 mobile host library tests passed, and all-target Clippy
  passed with warnings denied. The first sandboxed test run passed 304 tests;
  16 localhost-server tests required a rerun with local socket permission.
  Cleared only generated Cargo incremental caches to recover build space; no
  source or test data removed. Native iOS/Android execution remains unverified.
- Medium, fixed: the RSS sampler cached an over-budget verdict instead of
  re-evaluating its cached RSS against the caller's current budget. Raising a
  budget could therefore continue rejecting work until the next probe. It now
  recalculates the verdict without another probe; a deterministic regression
  covers raising and lowering the budget within one sampling interval. The
  current 326-test mobile library suite passes with localhost access, and
  all-target Clippy passes with warnings denied.
- Medium, fixed: failed telemetry uploads no longer read and log the response
  body. Error mapping accepts only an HTTP status, removing an unbounded buffer
  and raw response disclosure from that path. All 12 targeted telemetry tests
  passed, including failed-upload retention; all-target Clippy passed with
  warnings denied. The preceding full mobile suite passed 321 tests.
- Telemetry follow-up: simultaneous flushes can send the same rows; no delivery
  IDs are serialized for server deduplication. `sent` reports deleted rows rather
  than rows uploaded, so concurrent pruning changes the metric. Event count is
  bounded but payload bytes are not; cached count can drift if cancellation
  occurs around a durable write. Device-context changes relabel existing rows.
  Auto-flush learns an endpoint only after a successful nonempty explicit flush.
  No production engine call to record() was found: integration needs verification.
- Privacy follow-up: the boundary is an opt-in exported utility, not automatically
  applied to engine notifications, context or sync. Key-labelled redaction is not
  data-flow tracking. AAD identity encoding uses delimiter concatenation; identity
  validation/versioned encoding needs a compatibility decision. Leakage assertion
  searches serialized JSON, which does not reliably match escaped raw strings.
- Capability follow-up: foreground is supplied by the request; hosts must verify
  actual OS state and permissions. Response size is checked after serialization
  and invocation; an oversized response can follow an already-applied effect.
  Protected-reference usage is advisory. Each invocation allocates all four
  descriptors before choosing one; static operation slices are an optimization
  candidate, with no measured performance claim.
- Continuity follow-up: repeated activation of already-owned work unconditionally
  schedules the instance, including terminal or waiting states. Owner CAS and
  scheduling are separate; use an atomic guarded activation transition with
  recovery before changing idempotency. Local activation relies on host assertion
  of control-plane acceptance. Import creates several records separately and
  needs crash reconciliation. Manifest JSON is parsed before a byte cap, and
  base64 size arithmetic precedes the declared-payload bound check. Host signer
  methods run synchronously inside async work, beyond cooperative timeout control.
- Facade follow-up: operation timeouts do not roll back multiple storage writes;
  methods other than ticks continue accepting work after shutdown. Handler
  registration depends on transient Arc ownership rather than an explicit frozen
  state. URL loading omits the direct loader's sequence-count check, and neither
  loader normalizes mobile tenant/namespace or runs full authoring validation.
  Listing silently limits to 100 entries. Direct loading's zero sequence limit
  differs from sync's unlimited interpretation. Sync concurrency is not serialized
  by the optional-orchestrator mutex, and successful changes do not invalidate the
  execution sequence cache. URL logs/errors can include query credentials.
- Runtime follow-up: shutdown cancels engine tokens but does not invoke the
  runtime's bounded shutdown helper. Foreign spawn_blocking callbacks cannot be
  forcibly cancelled; default runtime drop can wait for them. Handle clones do
  not own Arc<Runtime>, despite the shutdown helper's explanatory comment.
- Lifecycle follow-up: concurrent starts can exceed the count-before-insert cap;
  salted reruns of terminal dedup keys can create multiple fresh instances.
  Dedup persistence follows instance insertion, and hydration logs failures then
  returns success. Paused instances are omitted from the admission count.
- Completion follow-up: complete_step writes context before human-input validation
  and signals/reschedules in separate operations. Indexing a scalar/array context
  by a string can panic. Trace the engine's human-input contract before selecting
  rejection versus normalization and atomic signal acceptance.
- Background follow-up: run_until_idle checks its time budget only between manual
  ticks; operation timeout, callbacks and RSS work can exceed the requested window.
  Foreground pause synchronizes only the tick mutex, not notification/sync/GC work;
  repeated resume calls can race token replacement. Failed dirty recovery clears
  the dirty flag anyway. Review authoritative lifecycle state before redesign.
- Notification follow-up: fixed-size scans can repeatedly select already-seen
  rows and starve later notifications. Top-level-only pending handler lookup
  was fixed using the engine recursive lookup in notifier and reporter;
  dedup still marks pending entries seen before sequence lookup succeeds.
- Medium, fixed: nested approval reporting now preserves the handler, prompt,
  choices, storage key and timeout via the engine's recursive block lookup.
  Pending notifications use the same engine traversal. Regression uses the actual
  nested onboarding fixture, including absent/non-step lookup behavior. Oversized
  timeout conversion saturates at i64::MAX instead of wrapping negative.
- Reporter recovery follow-up: execution markers precede command effects without
  a shared transaction or recoverable in-progress state. Crash/cancellation in
  that window can acknowledge unapplied work on redelivery. Failed marker removal
  after a retryable result has the same consequence. Simply moving markers after
  effects introduces duplicate effects; a durable per-command recovery contract
  is needed. Concurrent sync calls also need command ownership fencing.
- Reporter command follow-up: graceful update only logs, while skip_executed
  starts fresh without copying the execution tree. Restart may cancel before
  discovering a missing sequence name; cancel/fail writes can replace terminal
  states. Failed delegation results are logged then acknowledged without waking
  the waiting instance. Signal commands bypass the active-instance enqueue path.
- Reporter bounds follow-up: outbound row counts are bounded (170 outbox rows,
  100 acks); response JSON and command count are not capped. Invalid stored JSON
  is silently filtered, then those rows are deleted as sent. Approval entries
  remain queued after local completion, and scans can starve older instances.
- Manifest recovery follow-up: skipped transient downloads still advance ETag and
  manifest version, so the same manifest cannot retry its missing entries. Missing
  handlers/SDK upgrades face the same retry issue. Metadata writes are separate;
  removals and eviction errors can be swallowed before recording sync success.
- Manifest retention follow-up: eviction and replacement use sequence deletion,
  which cascades into execution data; active sequence references need preservation.
  Local version listing stops at 1000 names. Removed entries can be counted twice
  during explicit plus full reconciliation, and deletion selects only the latest
  stored version. Downloaded definition identity/namespace/version should be
  checked against manifest metadata before persistence.
- Manifest transport/trust follow-up: Retry-After is unbounded; signature URL uses
  whole-string replacement, including query values. Bearer auth is forwarded to
  each manifest-provided download URL; origin policy needs a defined contract.
  Key-set updates are non-atomic and failed revocations are swallowed while cached
  key fallback remains enabled. Audit these defensive boundaries with publisher
  contracts before changing them. This pass used static inspection only.
- Storage follow-up: terminal and waiting notification queries select newest
  rows first with a fixed limit, confirming starvation without a delivery cursor.
  Sync projections omit paused instances; trace reporter semantics before changing
  that contract. Dynamic telemetry-deletion and execution-step ID lists have no
  internal bind-count cap; current callers need bounds verification. Sequence
  cache eviction chooses oldest rows, with caller handling of active references
  still pending. Metadata and trusted-key tables assume one local engine scope.
- Memory/config follow-up: Darwin RSS shells out to ps, including iOS where that
  command is generally unavailable; missing samples disable enforcement. RSS cache
  assumes a stable budget and conversion multiplies without overflow checking.
  MobileEngineConfig derives Debug while holding the sync API key as a String.

### CLI review started

- Inspected manifest and inventory, all main.rs production code, context,
  config, health, signal, checkpoint, cron, inspect, sequence, instance, release
  and deploy commands, both template modules, init/scaffold, pieces and complete
  package-command production. Package tests and registry index URL/integrity
  matching were selectively traced. Complete portable-worker and doctor
  production source now inspected; worker poll envelope traced into API source.
  Support-bundle and debugger production source reviewed; timeline query/response
  contract traced into its API module and support sanitizer tests inspected.
  Complete continuity CLI, generation and bootstrap production reviewed; remaining
  CLI production modules are now source-reviewed, including dev, demo and
  test_cmd. Broader tests and consequential caller follow-ups remain. Dev-server
  production source and startup/cleanup callers in dev were also reviewed.
  Read release gate coverage and traced evaluation/promotion
  response contracts into API handlers (not a full API module review).
  Remaining commands,
  embedded templates, caller tracing and broader checks are pending.
- Medium, fixed: shared response printing propagates body-read errors instead
  of replacing them with empty successful output. Health checks reject malformed
  success JSON. Cron list propagates HTTP failure in JSON and table formats.
  Added incomplete-body, malformed-health and failed-cron response regressions;
  all four response-filtered CLI tests passed (344 filtered out), and all-target
  Clippy passed with warnings denied. Removed only completed mobile incremental
  caches (about 541 MiB) to recover build space; no source/test data removed.
- Context follow-up: selected context replaces explicit URL/tenant/key flags;
  document/verify precedence before changing. Context resolution precedes local
  commands such as migration/config/init, so an unrelated invalid context can
  prevent them. Context saves lack interprocess locking; concurrent edits lose
  updates. Load treats inaccessible/missing paths via exists(), checks permissions
  before opening, and does not revalidate deserialized context fields. Non-Unix
  credential permissions are not enforced.
- Shared client follow-up: default redirect policy needs review with custom
  API-key headers and destination changes. Responses are not byte-capped. Raw
  server strings reach terminal output; table width counts UTF-8/ANSI bytes rather
  than display cells. Atomic file replacement syncs file content, not the parent
  directory, and replaces existing file mode with temp-file mode. Test builds
  bypass destructive confirmation, so unit tests do not validate actual prompts.
- Sequence fingerprint disposition: comparing its allowlist against the current
  SequenceDefinition confirms that blocks, interceptors, input schema, SLA and
  both cleanup block lists are covered. The earlier claim of omitted behavior
  fields was not substantiated and is withdrawn. Identity/status metadata is
  intentionally excluded; schema-version normalization remains a compatibility
  consideration, not a confirmed missing execution-field bug.
- Medium, fixed: sequence apply now propagates malformed lookup JSON and rejects
  missing/non-i32 server versions. Checked increment rejects changed content at
  i32::MAX instead of posting the same version; identical content at that limit
  remains unchanged. Unit boundaries and HTTP tests verify no POST on invalid
  lookup or exhausted version. All nine sequence tests passed (339 filtered
  out); all-target Clippy passed with warnings denied.
- Fixed: sequence apply and package build now propagate directory-entry errors
  instead of silently omitting files. No performance claim; this preserves failure
  visibility before producing a package or apply plan.
- Sequence apply follow-up: Format upgrade recursively
  rewrites arbitrary objects, including application data with a matching type key.
  Trace persisted sequence contract before fixing. Preflight uses process::exit
  inside async dispatch, bypassing destructors. Dataflow writes generated files
  individually before checking compatibility; interruption can leave mixed outputs.
- Medium, fixed: instance list now checks HTTP status before JSON/table output.
  All five instance request tests passed, including errors in both output formats.
- Instance follow-up: watch repaints ANSI escapes
  even in JSON/nonterminal output and has no overall deadline or schema validation
  for terminal-state detection. DLQ groups ignores the supplied limit; fingerprint
  path segments are not encoded. Broader instance request tests remain pending.
- Medium, fixed: release proof evaluation distinguishes invalid evidence from
  numeric replay counts. Missing/malformed diff evidence no longer implies an
  empty diff, and missing/invalid counts cannot pass at u32::MAX thresholds.
  Valid empty diff entries retain compatibility with the API's omitted optional
  severity. All 33 release tests passed (318 filtered out), and all-target
  Clippy passed with warnings denied. Formatting and diff checks passed.
- Medium, fixed: deploy parses required EvaluationReport fields (release_state,
  auto_rolled_back and typed gates). Non-canary state, rollback or failed gate
  stops execution. Missing/malformed fields are errors. HTTP regression covers
  both promotion modes, healthy progress and rejected observations, verifying no
  promotion request after failure. All 25 deployment tests passed (327 filtered
  out). All-target Clippy passed after correcting a documentation-format lint;
  formatting and diff checks passed.
- Deploy proof follow-up: locally verified package is not bound to the release's
  candidate sequence or a trusted publisher; evidence joins unrelated package
  hash and release ID. Observations run consecutively without a sampling window,
  with no wait for additional samples. Server gates enforce
  sample sufficiency on promotion, but CLI evidence needs stricter semantics.
- Medium, fixed: package verification returns trust failures to its caller
  rather than calling process::exit inside a helper. Regression checks a validly
  signed package with a nonmatching trust key produces a normal error. Package
  tests passed: 29 package tests and nine sequence tests; all-target Clippy
  passed with warnings denied. Formatting and diff checks passed.
- Package registry follow-up: registry index/ledger reads lack body caps and
  request deadlines; package downloads are streamed with a 64 MiB cap but lack
  a deadline. Downloaded package identity/hash/key is not compared to the selected
  registry record before install. Independent package trust verification does not
  establish the requested name/version binding. Registry verifies absolute package
  paths, so a suspected relative URL join issue is not supported by that contract.
- Package install follow-up: syntactically valid lock entries without a version
  skip downgrade checks; lock entries are not scoped to server/tenant and writes
  lack interprocess serialization. Conflict checks and uploads interleave, leaving
  partial installs on later failure; retries then conflict before provenance is
  recorded. Contract files without matching sequences are skipped. Preflight
  status and malformed reports are swallowed; created IDs default to empty.
  Rewritten namespaces are not reconciled with packaged sub-sequence references.
- Package build/key follow-up: file reads are unbounded and follow symlinks;
  sequences deserialize but are not fully validated. Key bytes and input strings
  are not explicitly zeroized, and CLI key arguments are visible in process args.
  Inspection intentionally reports failed integrity but returns success; distinguish
  this informational command from verification in documentation/tests.
- Pieces follow-up: npm failures propagate, but child execution lacks deadline or
  cancellation-on-drop; install uses an unpinned registry package with npm's normal
  lifecycle scripts. Catalog uses a separate credential-free client, but no
  timeout/body cap. These behaviors need explicit installer documentation.
- Medium, fixed: portable HTTP and MCP adapters share the engine streaming
  body reader, enforcing the 1 MiB limit during download as well as rejecting
  oversized declared lengths. Previously the full body was allocated before
  the post-read check. Local HTTP regression covers valid and oversized bodies
  without Content-Length. All six portable tests passed (348 filtered out);
  all-target Clippy passed after explicit match/test-read lint fixes. Diff checks
  passed. Native adapters and real gateway interoperability remain unverified.
- Portable lease follow-up: poll ignores lease_secs/heartbeat_interval_secs;
  adapters send no heartbeat or checkpoints and ignore resume_checkpoint.
  Complete/fail correctly include claim_epoch, but task execution may outlive
  authority. Transport/parse/spawn errors exit the worker without failure reporting.
  Polled batches are processed sequentially without validating the requested limit.
- Portable execution follow-up: local stdin write precedes timeout and output
  drain, allowing pipe stalls; stdout/stderr buffer fully before size checks.
  kill_on_drop covers the direct child, not a process tree. HTTP/MCP use client
  timeout instead of task timeout and the control-plane client (default auth
  headers) for gateway requests. Gateway redirects, credential scope and enrollment
  must be explicit. MCP response ID/version and tool-result isError are unchecked.
  Wrapper accepts MCP entrypoints that the HTTP executor does not implement.
- Portable capability follow-up: hardware/plugins/credentials/regions are copied
  from policy requirements rather than probed; connectivity is fixed Ethernet.
  Registered trust is correctly not elevated by a manifest, but factual capability
  advertisement needs a host registration/probe contract. Worker polling delay has
  a 100 ms floor but no maximum hint bound or retry/backoff on network failure.
- Doctor follow-up: sequential independent checks can consume several 60-second
  client timeouts. JSON readability is often treated as schema validity; missing
  version or diagnosis fields produce no warning. Major-only compatibility is
  weak for 0.x releases. Doctor emits raw evidence and errors; redaction needs
  tracing into sensitive endpoint responses. No broad compatibility claim.
- Medium, fixed: debugger open now sends the bounded requested timeline limit
  to the API; previously it always fetched the default 200 even for --limit 500.
  Regression verifies the outgoing query alongside the other two evidence calls.
- Fixed: support bundle transport errors strip the request URL before export,
  avoiding query-credential disclosure. Regression uses an unsupported transport
  scheme to verify the exported error contains no URL. Both debugger tests and
  all 54 support-bundle tests passed; all-target Clippy passed with warnings
  denied. Formatting and diff checks passed.
- Support bundle follow-up: key-based sanitization is not a strict disclosure
  guarantee for free-form error/diagnosis strings, generic URL fields or server
  response values. Typed config SecretString serialization already redacts database
  URL; no database-config secret leak is claimed. Workload summaries discard failed
  fetch status and can imply an empty fleet. Request bodies are buffered before
  sanitization, and no total bundle byte limit exists. Requests run sequentially.
- Debugger follow-up: timeline/checkpoint/effect bodies are buffered in full;
  limiting displayed rows is not a memory bound. Unknown envelope objects bypass
  array truncation, and table counts can silently become zero. Timeline paging
  metadata is discarded, so truncation is not explained. Default requests still
  include workflow outputs, although the API supports metadata-only responses.
  Live fork remains explicitly opted in by flag; no actual live fork was run.
- Medium, fixed: continuity request reads now inspect the opened file and cap
  the actual read at 16 MiB plus one detection byte. A changing file or reader
  without useful metadata cannot trigger an unbounded read. Tests cover exact
  limit, oversized stream, metadata rejection and normal JSON. Capsule export
  uses the existing atomic-write helper instead of truncating the destination.
  All three continuity tests passed; CLI all-target Clippy passed with warnings
  denied. Formatting passed.
- Continuity CLI follow-up: handoff checks compatible=true but does not validate
  preview_sha256 before posting; export indexes missing capsule as null. Offline
  verification checks signature/trust, not expiration or execution compatibility.
  Import relies on server verification as expected, but help implies verification
  already occurred locally. Generic proof endpoints print HTTP-200 responses
  without mapping negative semantic verdicts to exit failure; contracts need
  tracing before changing all such commands. Responses lack byte caps; file reads
  remain synchronous and special files can block despite byte bounds.
- Generation follow-up: prompt/provider bodies and repair conversation bytes are
  unbounded. Timeout is per attempt (up to eight), not an overall budget. Schema
  URL is a prompt reference, not a supplied schema. Provider/tool availability
  and existing output overwrite policy remain separate from strict DSL validity.
  Programmatic attempts=0 hits unreachable(), though Clap restricts CLI values.
- Bootstrap follow-up: readiness can exceed deadline by request duration and
  polling delay; it accepts any successful service on the configured port without
  binding readiness to the spawned PID. Environment overrides inherited by server
  may diverge from the file-derived readiness URL. Ctrl-C kills rather than asking
  the engine to drain; relative server executable resolution after changing cwd
  needs documented behavior. Readiness does not independently prove migrations ran.
- Template follow-up: catalog response has an 8 MiB streaming cap, but its
  independently constructed client has no overall/connect deadline. Downloads
  check UTF-8 only, despite claiming JSON; inline payloads are arbitrary JSON.
  Pull overwrites existing output through atomic_write without conflict handling.
  Built-ins take precedence over catalog names; URL errors can include credentials.
  Embedded-template tests check block deserialization, not full authoring validity.
- Init follow-up: rerunning generates fresh secrets even when the config is
  skipped, so printed credentials and newly created Compose files can disagree
  with existing configuration. exists-then-write can overwrite concurrent files;
  config permissions tighten only after creation, and Compose carries the same
  secrets without the Unix permission restriction. Default Postgres credentials
  and all-interface port publishing require an explicit local-development contract.
  Multi-file writes can leave partial scaffolds on failure.
- Release follow-up: gate reads diff/preflight concurrently then historical
  validation separately, without a snapshot identity tying evidence together.
  Validation summary field checks do not establish nonempty samples or provenance.
  Numeric CLI options need finite/range validation traced against API enforcement.
  Direct command responses use inconsistent structured error handling.
- Build-space maintenance: removed the unused generated mobile static archive
  (about 439 MiB), which is not a CLI dependency, after verifying its location.
- Inspect follow-up, fixed: instance block IDs are now appended as one encoded
  URL path segment; a mock-request regression covers `/`, `?`, and `#` in the
  ID, alongside the existing encoded `at_block` query test. Instance mode
  still accepts outputs but ignores it. CLI argument named context needs
  inspection with global context selection.

## Next review work

Next inspect the types and storage crates, then API, gRPC, mobile, server and
CLI. Return to remaining engine modules. Resolve or explicitly disposition the
push timing/replay/encoding findings, facade tenant/feature findings and publisher
concurrency/index correspondence findings. Optional-feature/platform verification
and measured optimization work remain; the overall goal is still active.

### CLI development server lifecycle follow-up

- Medium, fixed: dropping DevServer after startup errors or caller cancellation
  previously detached its HTTP task without signalling shutdown. Drop now cancels
  the existing token, preserving graceful shutdown. A local regression verifies
  the background task receives cancellation and exits after its owner is dropped.
  The regression passed; CLI all-target Clippy passed with warnings denied.
  Formatting and diff checks passed. This does not impose a deadline on draining
  active requests.
- Remaining: readiness is asserted before caller engine initialization; advertised
  port remains zero when an ephemeral port is requested. HTTP task failures are
  logged but not propagated to the dev loop. The existing health-named integration
  test silently accepts startup errors and never requests health; it does not
  establish HTTP readiness. Wildcard CORS on the unauthenticated loopback service
  needs a deliberate browser-origin policy review. No network probing performed.

- Test-command partial review: declarations, fixture extraction and contract-run
  dispatch through line 260 inspected; remaining module not yet reviewed. Extract
  still checks path metadata before an unbounded read, writes three files without
  group atomicity, and indexes absent response fields as null. Force=false uses
  an existence check before replacement, so concurrent output writers are not
  excluded. Contract-run failure exits the process instead of returning an error.

### CLI replay, contracts and demos

- Completed production/source test inspection of test_cmd.rs and demo.rs; traced
  outputs response into API get_outputs (declared and returned as an array).
- Medium, fixed: record and replay silently converted non-array output responses
  into empty execution history. A shared boundary now rejects malformed envelopes
  and consumes the owned array without cloning its complete contents. Regression
  exercises the recording command with null/object/wrapped-array responses and
  proves no fixture is written; a genuine empty array remains supported.
- Medium, fixed: failed contract reports now return an error through normal CLI
  handling rather than process::exit inside run_contracts. Tests exercise passing
  and failing real embedded runs for human, JSON and JUnit report formats.
  All 10 test-command tests passed, including both regressions; CLI all-target
  Clippy passed with warnings denied.
- Replay follow-up: current instance context is used as original input, although
  execution may have mutated it. A block-ID map loses iteration/attempt histories;
  missing mocks synthesize empty success, and reached_terminal does not distinguish
  failure from successful completion. Block-set comparison discards order/counts,
  truncated replay still returns success, and outputs may retain unresolved
  externalization markers from the API. Early errors and normal replay return
  lack explicit engine shutdown. Virtual-time +1 second addition needs checked
  boundary handling. Record chooses first output per block while replay chooses
  last, and malformed individual output rows are still skipped. Missing instance
  state defaults to completed. Output overwrite prevention has a TOCTOU race.
- JUnit follow-up: XML metacharacters are escaped, but forbidden XML control
  characters in names/failure messages are not sanitized. CLI global format is
  ignored for replay/record; contract reporting uses its own documented format.
- Demo scope: portable-agent demonstrates local storage/crypto handoff, not an
  executing distributed workflow. It uses an empty sequence, synthetic approval,
  and independently seeded destination ownership. Trust is bootstrapped from the
  capsule itself within this isolated demo; this is not an enrollment recipe.
  Redelivery evidence checks only the returned instance ID. Private-input evidence
  inspects returned context only. Several reported failed invariants do not change
  command exit status. Crash recovery seeds stale state and reopens SQLite rather
  than killing a process; the report's temporary database path is deleted on return.

### CLI development loop

- Completed dev.rs production and inline test inspection.
- High, fixed: DevSession stopped ticking its shared engine after the displayed
  instance finished. Work created through the dashboard or directory auto-run
  then remained scheduled indefinitely. Scheduler ticks now run independently
  of whether an instance is selected for progress display. Regression completes
  the displayed workflow, creates another instance through the engine facade,
  and verifies session stepping completes it and persists its block output.
  All 39 dev tests passed; final CLI all-target Clippy exited successfully with
  warnings denied. Formatting and diff checks passed.
- Follow-ups: virtual time advances only from the displayed instance's deferral,
  so secondary timers still lack a global virtual-time policy; adding one second
  can overflow the date bound. Secondary work runs at the no-display poll interval
  and lacks a per-instance timeline in terminal output. All outputs are fetched
  every tick; index-only progress assumes append order despite virtual/wall clock
  mixing. Preview serializes full outputs before truncation.
- Dev loading/watch follow-ups: generic JSON traversal can interpret application
  data containing id/handler as blocks. Mtime/size signatures miss same-size changes
  with unchanged timestamps. Scan/entry errors are suppressed, failures consume
  signatures without retry, and deletions are silently dropped despite poll docs.
  Startup workflow upserts are detached tasks and their existing test only checks
  watcher paths/empty instances, not successful publication. Session versions
  restart at 1 despite persistent SQLite; reload counter increments are unchecked
  and shared across sequences. Primary hot reload always starts a new instance
  regardless of auto_run, while previous live instances continue.
- Dev startup errors after building the engine bypass explicit engine shutdown;
  Ctrl-C signal errors are mapped to success, as is interruption in once mode.
  Once has no overall time/tick bound and unknown-handler early exit can race
  externally enrolled workers or handlers on unreached branches. Local HTTP
  and embedded engine have separate circuit-breaker registries.

### API review queue

- Manifest and source inventory inspected. Instance outputs handler traced for
  CLI response contracts; most API production remains to inspect. Begin shared
  lib/error/request handling and instance lifecycle next. No broad API completion
  or test-coverage claim.

### API shared contracts and instance lifecycle

- Source reviewed: lib.rs, error.rs, request_id.rs, health.rs, input_schema.rs,
  instances/types.rs and full lifecycle.rs production. Test harness read; selected
  resume integration tests inspected. SQLite update_state/conditional_update_state
  implementations traced; broader lifecycle storage protocol review remains.
- Medium, fixed: EngineError::Storage now uses the same mapper as direct storage
  errors. Wrapped connection/backend/pool failures retain HTTP 503 and wrapped
  quota failures retain HTTP 429, with existing response redaction. Regression
  checks status, machine code and public message. No claim of current endpoint
  reachability for every EngineError conversion; this fixes the public mapper.
- High, fixed: resume-from checked merged context size only after deleting the
  execution tree, block outputs, effect receipts and worker tasks. Merged context
  is now validated before those writes. Integration regression uses a configured
  limit and verifies HTTP 413 preserves state, context and saved block output.
  Shared test harness now supports an explicit context limit.
- Validation: initial API test/Clippy builds failed due to exhausted disk. After
  confirming both processes exited, removed only generated incremental caches.
  Retried with incremental output disabled: all 18 error-mapping unit tests and
  all 11 resume integration tests passed. API all-target Clippy passed with
  warnings denied (7m 53s including build-lock wait).
- Lifecycle follow-ups: state validation is a read followed by unconditional
  update (SQLite confirms no expected-state predicate). Parent wake is another
  read/write and errors can be ignored. Retry/resume are multiple independent
  destructive writes without a transaction or ownership guard; evidence reset,
  worker invalidation and scheduling need one coherent recovery contract. Resume
  deletes effect receipts, which warrants explicit redispatch authorization and
  retained audit evidence. This pass did not execute real external effects.
- Create follow-ups: single creation routes releases while batch bypasses routing;
  request namespace/timezone/concurrency validity needs domain parity review.
  Context size is checked before runtime mode flags are stamped. Admission/schema
  checks precede idempotency lookup, so policy/schema changes can reject retries of
  already-created work. Idempotency is tenant-wide and dryrun-prefix collision is
  explicitly accepted in existing code; changing its identity requires migration.
  Children/logs/outputs are unpaginated; list OpenAPI still declares a bare array.
- Shared follow-ups: terminal-target comment promises a distinct code but preserves
  already_exists compatibility. Request-ID middleware buffers error bodies, loses
  failed/oversized body reads into an empty fallback, and leaves arbitrary JSON
  errors outside the envelope untouched. It removes content-length but does not
  otherwise reconcile representation headers. Its truncation test duplicates the
  algorithm instead of driving middleware. Health lacks its own ping deadline
  and shutdown-token check. Schema compilation repeats per validation (including
  batch items) and all validation errors are rendered without count/byte limits;
  external-reference configuration needs separate review before any safety claim.
- lib follow-ups: pagination advertises has_more without next_cursor; callers must
  supply an extra row. Continuity key derivation hashes textual hex, so case changes
  alter signing identity despite identical decoded encryption bytes; migration
  compatibility must precede normalization. Legacy successor Link construction
  uses the full request URI and replaces existing Link headers.

- Additional API source coverage: instances route assembly, signals, checkpoints,
  audit and artifact read modules fully inspected. Signal enqueue uses the existing
  atomic active-state guard; best-effort wake uses CAS but suppresses errors.
  Checkpoint/audit lists silently cap at 100/200 without paging information;
  checkpoint save accepts arbitrary JSON and prune does not account for continuity
  references in this handler (storage semantics still need tracing). Artifact reads
  buffer full objects and validate content_type only after fetching bytes; list
  has no pagination. Attachment/nosniff protections already exist.

- Further source coverage: injection and fork modules fully inspected; PostgreSQL
  update_state also confirmed unconditional. Injection performs serde decoding,
  not complete authoring validation of the combined workflow, and repeats a
  block-ID variant match already shared elsewhere. Fork snapshot selection and
  actual output copying are separate reads of a potentially running source;
  context comes from another read. Finalization has best-effort cancellation,
  but process interruption can leave an unarmed row. Fork create uses ordinary
  create_instance rather than admitted creation. Partitioning scans output
  history per block and marks a composite executed when any descendant has an
  output; snapshot completeness needs deeper evaluator/storage tracing.
- API error-mapping unit suite passed: 18 tests. Resume integration suite and
  all-target Clippy are still running after the disk-space recovery.

- Bulk instance operations source reviewed. Batch retry repeats single-retry's
  non-atomic reset sequence, drops the underlying storage error, and writes audit
  separately on a best-effort basis. Batch signals use active-state enqueue but
  do not perform single-signal's best-effort early wake. Blank custom names are
  accepted because validation checks only Option presence. Bulk state/reschedule
  delegate to storage without endpoint-level transition validation or action cap;
  deeper backend semantics remain to review. DLQ retains bare-array pagination.

- Resume integration validation completed: 11 tests passed, including the new
  oversized-patch preservation regression. Formatting/diff checks passed.

- Final API all-target Clippy passed with warnings denied. No remaining live
  build sessions from this pass. Full API crate review is still in progress.

### API streams, timeline and event ingestion

- Fully read streaming.rs, changes.rs, events.rs and instances/timeline.rs,
  including inline tests and existing SSE integration tests.
- High, fixed: SSE producer cancellation previously surrounded only polling, not
  storage reads or backpressured sends. Added a shared lifetime guard that drops
  the producer future on shutdown or receiver closure. Both instance and change
  streams use it, releasing their owned stream semaphore permit. The change
  stream previously never noticed idle client disconnection and could retain its
  slot indefinitely. Tests cover a blocked event send on shutdown, pending work
  on disconnect, and dropping an actual idle change-stream response. Validation
  in progress. No claim that cancelling a storage future aborts backend work
  already dispatched, or that bytes buffered by HTTP transport disappear.
- Remaining stream follow-ups: instance stream has no Last-Event-ID resume cursor,
  only a timestamp-plus-boundary-ID in-memory cursor. Backdated output insertion
  can be skipped; arbitrarily many rows at one timestamp grow the set. Reads
  return unbounded output batches, channel capacity bounds events rather than
  bytes, and initial access lookup precedes concurrency admission. Live delta
  lag is only logged; final durable output remains the recovery mechanism.
- Change-feed follow-ups: JSON serialization failure skips without advancing
  the cursor and can repeat forever; storage error retries are fixed one second.
  Cursor size is not bounded before base64 decoding and malformed Last-Event-ID
  bytes are treated as absent. Timestamp ordering needs commit-order/retention
  semantics before claiming lossless resumability. Shared stream admission has
  different status codes (503 instance, 429 changes).
- Timeline follow-ups: payload omission happens after storage loads outputs and
  current context. Entry count does not bound bytes. Instance, outputs and audit
  are separate snapshots; latest 200 audit rows are filtered after fetching, so
  fewer than the latest 200 transitions may appear. Audit failures fail the whole
  response despite best-effort recording. Offset paging can shift during resets.
- Event ingestion: batch validates all items before intentionally serial writes;
  partial backend failures rely on producer-id deduplication. Identities are checked
  for blankness only; payload/key limits depend on outer request limits. List has
  a row limit without a continuation cursor. Redaction applies to payloads, not
  correlation/producer identifiers. Engine correlation and storage dedup behavior
  remain to trace before stronger ingestion guarantees.

- Additional API source coverage: metrics.rs, circuit_breakers.rs and
  model_pricing.rs fully inspected. Metrics render is synchronous and materializes
  the full scrape; exposure policy belongs to server wiring. Circuit breaker
  reset acknowledges an in-memory change before tracked asynchronous persistence
  completes (engine reset caller traced); resetting an unknown breaker is a no-op
  success. List endpoints have no pagination and unavailable registry means empty
  list for the global endpoint but 503 for tenant-specific endpoints.
- Pricing code follow-ups (no external price verification performed): overrides
  accept negative rates and case-normalized duplicate keys collide in HashMap
  iteration order. Prefix matching can assign a family price to an unknown model
  suffix; provider prefixes are discarded. Signed token counts permit negative
  estimated costs, and extreme finite rates can overflow to non-finite estimates.
  Tests pin static table values, not freshness against provider billing.
- Initial streaming unit suite passed: 10 tests. Final expanded stream-filtered
  unit suite, HTTP streaming integration suite and all-target Clippy pending.

- Cron API source fully reviewed. Timezone and expression validation already
  happen at create/update. List passes caller limit directly to storage and
  has no continuation cursor; namespace validation differs from instance create.
  Updates read/modify/write a full schedule, recalculate next_fire even for
  metadata-only updates, and return a struct with its old updated_at value;
  scheduler concurrency/counter preservation needs backend tracing.

- Cluster and telemetry API source fully inspected. Cluster routes explicitly
  require admin; drain acknowledges the storage operation rather than completed
  draining. Telemetry ingestion has a 500-row cap, but timestamps silently fall
  back to now and event payloads remain strings without schema validation.
  Retry identity is absent and arbitrary client-supplied device identifiers are
  accepted at this layer. Several DB errors use Internal instead of the shared
  retry-aware storage mapper.
- Telemetry rollback follow-ups: cooldown lookup and record are not atomic, so
  concurrent reports can trigger repeated rollbacks. History is recorded before
  deprecation/publication succeeds, potentially suppressing recovery during the
  cooldown. Sequence lookup is hard-coded to namespace default; failures are
  ignored while logs still claim successful deprecation. Webhook dispatch is
  detached, lacks durable retry, logs raw URLs and calls every HTTP status
  delivered. URL policy must be traced through rollback configuration before
  making a security claim. Client-construction failure falls back to a default
  client without the specified timeout. Dashboard range is not validated and
  summing i64 counts can overflow; query semantics need storage review.
- Both existing HTTP streaming tests passed. Clippy found default_trait_access
  in the new test fixture; corrected to explicit ExternalizationMode::default.
  Expanded stream unit tests and final Clippy remain running.

- Rollback policy API and security URL helper fully source-reviewed. Policy
  configuration does validate outbound URL syntax/address literals, so the earlier
  telemetry URL-policy question is partially resolved. The helper explicitly
  relies on a separate DNS-aware outbound client; telemetry's webhook client is
  plain reqwest. Apply the engine's outbound destination/redirect policy at send
  time as a defensive follow-up. No probes or bypass reproduction performed.
  Policy creation validates finite thresholds/windows but accepts blank-only
  names, allows confirmation windows that ingestion silently ignores, and
  creates then re-reads outside a transaction.
- Final validation: all 11 stream-filtered unit tests passed, including the
  idle change-stream response regression; both HTTP streaming tests passed.
  API all-target Clippy passed with warnings denied after the test-fixture style
  correction. Formatting and diff checks passed. All build sessions from this
  pass have finished. API/source-wide follow-ups remain open.

### API usage, queues, sessions and pools

- Fully reviewed usage.rs, queue_routing.rs, queue_dispatch.rs, sessions.rs and
  pools.rs; both backends' queue_routing/queue_dispatch modules read. Traced
  usage insertion/aggregation in both telemetry storage implementations (only
  those sections, not full telemetry modules).
- Medium, fixed: usage window calculation now rejects a default-start date
  underflow and inverted explicit ranges before querying storage. Explicit equal
  boundaries remain valid (empty half-open window). Unit tests exercise date
  extremes/default width and integration tests check 400 versus equal-bound 200.
  Usage HTTP tests and API all-target Clippy pass.
- Usage cost follow-up, fixed: the API now sums raw estimates before rounding
  the window total, so sub-micro-dollar rows do not disappear. It reports
  `total_cost_is_complete` when unknown or invalid rows are omitted. Negative
  token counts and invalid pricing overrides no longer produce negative cost;
  non-finite estimates are omitted, and large finite rounding cannot overflow.
  All 419 API library tests and four usage HTTP tests pass.
- Usage storage follow-ups: aggregates fetch all distinct models/kinds without
  paging or a window cap, and i64 sums permit overflow failures. Both storage
  backends now reject negative usage token counts before insertion (zero
  remains valid); SQLite and live PostgreSQL regressions check that invalid
  events leave no rows. SQLite's RFC3339-text and PostgreSQL's native timestamp
  windows now have matching half-open boundary tests across whole-second,
  millisecond, and microsecond inputs. A live regression found that SQLx rounded
  sub-microsecond PostgreSQL query bounds down, incorrectly including a row at
  the preceding microsecond. Both query bounds now round up to the next stored
  microsecond tick; the regression and 21 PostgreSQL integration tests pass
  against a fresh temporary cluster (stopped and removed after verification).
- Queue follow-ups: routing/dispatch lists fetch all matching rows. Routing order
  lacks an ID tie-breaker for identical priority/timestamp. Blankness checks do
  not bound string lengths or validate optional match_queue. Dispatch updates
  now retain an omitted secret atomically, distinguish explicit null, and return
  the persisted created_at; API regressions cover all three. Unknown persisted
  dispatch modes previously decoded as `poll`; both backends now return an
  explicit storage error, with corrupt-row regressions. All 22 live PostgreSQL
  integration tests pass. Secret fields are explicitly omitted from API
  responses, though list queries still fetch secret columns internally.
- Session fixes: create and update apply the same serialized-data size limit;
  creation rejects blank and over-512-byte keys. Unknown persisted session
  states no longer decode as `active` in either backend; corrupt-row tests cover
  ID and key lookups, and all 23 live PostgreSQL integration tests pass.
  Remaining: expiration and allowed state transitions are
  not validated here; whole-data updates have no version/CAS, and session
  instance listing is unbounded.
- Pool follow-ups: create name has no validation, update accepts zero weight or
  invalid names rejected by add, and warmup_start cannot be cleared with null.
  Update/delete fetch the entire pool resource list; missing-resource delete
  additionally scans every sibling pool serially. Update writes a snapshot of
  mutable usage fields, needing storage tracing for races with resource picks.
  Missing tenant on pool list becomes an empty tenant ID instead of an explicit
  scope error. Resource selection/credential ownership remains to trace.


### Pre-push regression verification (2026-09-05)

- Added mobile regressions for nested pending-step listener delivery, saturated
  approval timeouts, and absent approval deadlines.
- Updated two older retry-validation assertions to match the new finite and
  positive multiplier requirement. The full workspace run reached 9,130 passing
  tests before the first stale assertion stopped it; no production-code failure
  was found. After the corrections, all 1,679 types tests/doc-tests passed.
- All remaining workspace documentation tests passed (5). Workspace all-target
  Clippy with warnings denied passed; the changed types tests were rechecked
  with all-target Clippy after correction. Formatting and diff checks passed.
- All 18 PostgreSQL integration tests passed against an isolated temporary local
  database. The workspace run without DATABASE_URL skips those test bodies;
  the separate PostgreSQL run exercised them with a live database.
- The initial sandboxed test run could not bind API fixture sockets. The broader
  verification above used localhost socket access. Node declarations type-check
  and Python package syntax parses; packaged native imports, optional features,
  non-host platforms, live external services, and fuzz campaigns remain outside
  this verification. Earlier unresolved review findings remain open.

### Storage integer boundary follow-up (2026-09-23)

- `TaskInstance.max_concurrency` is `u32`, but both backends cast it through
  `i32`; values above `i32::MAX` wrapped negative on writes and back to large
  positive values on reads. PostgreSQL migration 082 widens the column to
  `BIGINT`; both backends now bind `i64` and reject negative or out-of-range
  persisted values while decoding. Single and batch inserts round-trip
  `u32::MAX` in regression tests.
- SQLite storage library: 340 tests passed. Live PostgreSQL integration: 25
  tests passed against a temporary local cluster, which was stopped and
  removed afterward. Workspace all-target Clippy with warnings denied,
  formatting, and diff checks passed.
