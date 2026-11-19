# Adoption roadmap implementation

This page follows the 50-feature mobile, distributed, and sequence roadmap. The verification column points to the test or exercise for each item. Partial work and release-dependent features are marked in the relevant row.

## Mobile execution

| # | Feature | Entry point and verification |
|---:|---|---|
| 1 | Offline inspection starter | [iOS and Android starter](../examples/mobile-inspection/README.md), SDK recovery test, and [worker and approval reconnect example](../examples/mobile-inspection/reconnect/README.md). The native starter queues allowlisted inspection JSON and retries while online in the foreground. Tests cover an accepted response that was lost, restart, and duplicate retries. |
| 2 | Mobile compatibility matrix | [Published compatibility matrix](MOBILE_COMPATIBILITY.md). Verified iOS version and Android Maven build. |
| 3 | Fresh-install release gate | [Public install workflow](../.github/workflows/mobile-install.yml) builds consumer apps from public packages. iOS simulator passed with the matching published binary. Android instrumentation runtime still needs a device or emulator. |
| 4 | Mobile integration doctor | [Starter and runtime doctor](../examples/mobile-inspection/DOCTOR.md) checks release pins, tooling, declared handlers, manifest and root-key format, credential readiness, and push configuration. Tests check diagnostics and credential redaction. Live manifest and push delivery are separate checks. |
| 5 | Offline-to-cloud status | React Native `Orch8SyncStatus`. Tests verify that acknowledgements match the current submission ID and revision. The host loads the receipt. |
| 6 | Lifecycle adapters | The [starter](../examples/mobile-inspection/README.md#native-lifecycle) bounds window duration, ignores expired view updates, and refreshes from SQLite on foreground entry. Native tests exercise iOS continuation expiration and Android Activity cancellation. |
| 7 | Durable media upload | React Native `DurableMediaUploader`. The restart fixture covers a dropped acknowledgement, a duplicate range, metered-network deferral, and completion. The host supplies durable storage and the range endpoint. |
| 8 | Offline conflict callbacks | React Native `resolveOfflineConflict`. Tests cover default review, server authority, merge, and conditional writes using the current revision. |
| 9 | Approval inbox | React Native `Orch8ApprovalInbox`. Tests cover duplicate activation, expiry, stale revisions, and stable retry keys. The host performs the durable approval write. |
| 10 | Local-only data inspector | React Native `previewDataVisibility`. Tests cover exact paths, inherited privacy, and metadata-only rendering. The host enforces the same policy at each disclosure boundary. |
| 11 | Mobile incident simulator | [Incident runner](../examples/mobile-inspection/incidents.py) exercises process death, expired identity, critical battery, missed wake, and a connection loss during a lease. |
| 12 | Execution components | React Native status, timeline, and approval components. Presentation tests and wrapper typecheck exercise the view models. |
| 13 | Signed app-bundled catalog | Development `installBundledCatalog`. Tests cover signature trust, duplicate installs, starting locally, and the accepted-remote-manifest fence. |
| 14 | Mobile catalog release rings | [Publisher integration](../orch8-publisher/CATALOG_ROLLOUT.md) uses signed manifests and per-device acknowledgements. Tests cover cohorts, eligibility, stale receipts, and monotonic rollback. The host provides authenticated device identity and persistence. |
| 15 | Supported account switching | [Development scoped API](SDK_SCOPED_NATIVE.md) and [native binding tests](../tests/react-native-scopes/main.swift) cover opening accounts, switching, restart, per-account pending work and cached definitions, retirement, and re-login. The coordinated public release remains pending. |
| 16 | Background diagnostics | React Native `getBackgroundDiagnostics` reports observed window status and last progress. Tests cover budget exhaustion and progress surviving later idle windows. |
| 17 | Device capability kits | [Reference capability kits](MOBILE_PRIVACY_AND_TOOLS.md) and the starter permission-gated camera and QR path. Simulator tests cover the data boundary and permission decision. Physical camera execution needs device verification. |

React Native runner and SDK details are in the [wrapper README](../packages/react-native/README.md).

## Distributed execution

| # | Feature | Entry point and verification |
|---:|---|---|
| 18 | Placement doctor | `orch8 doctor --placement` uses the saved version and live fleet. API and CLI exercises cover ineligible and missing workers. |
| 19 | Hybrid sandbox | [One-command sandbox](../examples/private-workers/README.md) runs real leased TypeScript and Python workers and verifies a resumed checkpoint. |
| 20 | Cross-runtime timeline | The execution page shows scoped claim and reconciliation evidence. Distributed API tests cover tenant boundaries and payload redaction. |
| 21 | Wait reason and next action | The execution page shows structured diagnosis from worker and runtime evidence. Test cases distinguish unavailable workers and recovery waits. |
| 22 | Worker enrollment | The Workers page issues a tenant-scoped expiring worker key and provides setup instructions. Live registration is covered by the starter exercises. |
| 23 | Worker starter repos | [TypeScript and Python workers](../examples/private-workers/README.md) use lease epochs and checkpoints. Verified in the hybrid sandbox. |
| 24 | Placement policy recipes | Placement preview and worker UI preview a named recipe before publishing. Tests cover effective placement and worker eligibility. |
| 25 | Explicit fallback branches | [Private fallback example](../examples/private-workers/README.md#explicit-regional-fallback) keeps policy and residency on both branches. Verified with a timed-out GPU branch and an eligible EU CPU worker. |
| 26 | Drain workflow | Workers page supports gRPC drain and active-task status. The REST starter handles graceful drain on process signal. |
| 27 | Unknown-effect review queue | Tenant-scoped oldest-first queue links to the tracked effect. API tests verify redaction and tenant isolation. |
| 28 | Runtime trace export | The execution page exports redacted runtime events, attempt timing, and wait reasons. API tests verify that unknown claims omit payloads. |
| 29 | CI fleet readiness | `orch8 placement fleet-readiness` returns a failing status when workers cannot run the saved version. CLI exercises cover absent, eligible, and incompatible worker versions. |
| 30 | Checkpoint SDK examples | Both starter workers implement heartbeat checkpoint compare-and-swap. The sandbox verifies interruption and resumption. |
| 31 | Placement planning | The [placement plan](PLACEMENT.md) shows effective queues, pending queue age, eligible workers, available HTTP poll slots, and serialized parameter size estimates. |
| 32 | Worker canaries | Operators can [stage and preview a minimum worker version](PLACEMENT.md), then promote it or roll back. The gate applies to tenant-scoped HTTP polls and new gRPC claims. Existing claims retain their normal completion and reconnect behavior. |
| 33 | Scoped mobile delegation preview | The [opt-in desktop echo receiver](scoped-remote-execution.md) tests negotiated capabilities, confirmed lease authority, frozen definition and input delivery, and backend acknowledgement. Scoped native execution of general workflow graphs remains staged. |
| 34 | Private worker installer | [Private pilot installer](../examples/private-workers/README.md#install-a-private-pilot-worker) requires HTTPS and private credentials. Verified setup and rejection of insecure file permissions. |

## Sequence authoring

| # | Feature | Entry point and verification |
|---:|---|---|
| 35 | Fixture runner | `orch8 test run` accepts authoring definitions and workflow contract cases. CLI tests exercise authoring-format inputs and assertions. |
| 36 | Per-target preflight | `orch8 sequence preflight-mobile` checks published and development profiles and app handlers. Fleet readiness checks the selected worker build. |
| 37 | Runtime overlays | The sequence editor displays step target and placement details. Browser and helper tests cover the runtime overlay. |
| 38 | Guided setup | The template wizard runs readiness and isolated fixtures before publication. A Chromium smoke exercised shared fixtures and exact POST behavior. An API test checks mocked execution and absence of a stored sequence. |
| 39 | Expression autocomplete | The editor suggests output and input paths from declared schemas. Helper and browser tests cover path insertion. |
| 40 | Sample-to-schema | Schema draft UI infers field types and lets the author choose required fields. Helper and browser tests cover inference and editing. |
| 41 | Branch coverage | Contract reports list covered and uncovered steps across passing cases. CLI tests cover branch selection, failed attempts, and compensation. |
| 42 | Timeline debugging | The editor checks the exact sequence revision before displaying historical state and reconstructed redacted inputs. Raw historical outputs stay hidden. |
| 43 | Pinned dependency view | The editor locates exact child versions and distinguishes floating dependencies. Helper and browser tests verify scoped matches. |
| 44 | Typed handler scaffolds | The CLI generates contracts and stubs for TypeScript, Swift, Kotlin, and Python. CLI tests exercise generation for each language and a runnable matching fixture. |
| 45 | Human input builder | The editor supports choices, defaults, and human input validation. Helper and browser tests cover the rendered editor behavior. |
| 46 | Failure-mode test packs | [Reusable recipes](../examples/verified-sequences/README.md) cover retry, gate timeout, cancellation, and compensation. The worker sandbox and incident suite cover lease loss and recovery separately. |
| 47 | Replay divergence | The editor overlays saved replay evidence and can export a [redacted regression fixture](RELEASES.md#replay-regression-fixtures) for an individual divergence. The API test checks replay reproduction, tenant isolation, and a failing contract that passes after the regression is fixed. |
| 48 | Environment promotion | `orch8 deploy --package-sequence` binds a signed package member to the actual stored candidate before canary mutation. Tests reject mismatches and allow environment-specific identities. |
| 49 | Import parity | `orch8 import --parity-from … --parity-out …` maps observed source steps into a contract. CLI tests verify conversion and reject unmatched evidence. |
| 50 | Verified sequence recipes | [Inspection, delivery, and maintenance recipes](../examples/verified-sequences/README.md) share source definitions and passing fixture cases between CLI and dashboard. |
