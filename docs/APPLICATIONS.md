# Orch8 — Embedding Use Cases

Orch8 is built as an embeddable workflow runtime. Because it compiles to a native library and uses SQLite as its default backend, it can run inside any process — not just as a standalone server.

The common thread across all embedding contexts: the engine replaces something teams are building themselves, badly, with no durability, no audit trail, and no ability to update logic without a release.

---

## Mobile Applications

Rust compiles to native libraries for iOS (`.a` via `aarch64-apple-ios`) and Android (`.so` via NDK). Expose the engine through a C FFI layer and wrap it with Swift or Kotlin bindings using [UniFFI](https://github.com/mozilla/uniffi-rs).

**Architecture**

```
Mobile App
  └── FFI / UniFFI bindings
        └── Orch8 engine (Rust, SQLite backend)
              └── No HTTP layer — direct function calls
```

**What the engine replaces:** ad-hoc state machines in UserDefaults / SharedPreferences — brittle, untestable, impossible to debug in production.

### Load sequences from the server

Sequence definitions are JSON. A device can fetch a sequence from a server at runtime, cache it in SQLite, and run it locally — including offline.

```
Server                          Mobile Device
  │                                   │
  │── GET /sequences/onboarding ──────▶│
  │◀── { "blocks": [...] } ───────────│
  │                                   │
  │                          engine.run(sequence)
  │                                   │
  │                          [runs locally, offline]
```

This enables deploying behavior changes to mobile apps without an App Store release.

### Killing features on mobile

**Version-safe mid-flow upgrades**
The engine already tracks sequence versions per instance. Users mid-flow on v1 finish on v1. New users get v2. No other tool handles this — everyone else clobbers the running flow.

**Cross-device handoff**
Instance state can live on a server. A user starts onboarding on iPhone, resumes on iPad, finishes on web. The audit log records exactly where each device handed off.

```
Instance state (server)
     │
     ├── iPhone  (runs steps 1-3)
     ├── iPad    (resumes at step 4)
     └── Web     (completes step 5)
```

**Step-level analytics**
Every step is a named block with state transitions recorded in the audit log. Drop-off, time-between-steps, branch conversion, retry rates — all free, no instrumentation required.

**Human-in-the-loop flows**
`wait_for_input` pauses a mobile flow until a real person acts — a sales rep approves a discount, a support agent resolves an issue, a parent approves a purchase. The instance sits idle and costs nothing while waiting.

**Conditional sequences based on device signals**
Handlers on mobile can read local device state — notification permission, connectivity, location, sensor data. The sequence branches against real device reality, not server-side user properties.

**Compliance-ready flows**
The audit log is append-only. Every step, signal, and branch is recorded with a timestamp. Provable record of what a user was shown and when they consented — GDPR, HIPAA, financial onboarding.

### Tuning for mobile constraints

| Config | Recommended value | Reason |
|---|---|---|
| `backend` | `sqlite` | Native to mobile, no server |
| `batch_size` | `8–16` | Mobile memory constraints |
| `max_concurrent_steps` | `4–8` | Limit background CPU |
| `tick_interval_ms` | `500–1000` | Reduce battery drain |

iOS suspends background processes aggressively. The scheduler tick loop will pause when the app is backgrounded — design sequences accordingly or use background task APIs to wake the engine.

---

## Browser (WebAssembly)

Rust compiles to WASM. The engine runs inside a browser tab with no server required per user session.

**Use cases:**
- Multi-step form state that survives page refresh
- Offline-capable checkout flows
- Long-running AI agent loops in the browser
- Complex wizard logic defined server-side, executed client-side

Same "load sequence from server, run locally" model as mobile.

---

## Desktop Applications

Creative tools, IDEs, data processing applications.

**Use cases:**
- Resumable export pipelines that survive app crashes
- Multi-step AI processing with human approval gates
- Collaborative review flows where one participant signals readiness before the next step fires
- Long-running background jobs with retry and audit

**Killer use case:** AI-assisted workflows where a human approves each generated step before the next one fires — `wait_for_input` maps directly to this.

---

## IoT and Edge Devices

Raspberry Pi, industrial controllers, smart home hubs, fleet management.

**Use cases:**
- Device automation sequences triggered by sensor readings
- Retry logic for flaky hardware operations
- Escalation when a device doesn't respond within `deadline`
- Push new behavior sequences to a device fleet without firmware updates

Industrial IoT currently implements all of this as hand-written state machines in C per manufacturer. The engine is a general runtime for device behavior.

`deadline` + `on_deadline_breach` maps directly to "escalate if the sensor doesn't report within N seconds."

---

## CLI Tools and Developer Tooling

Long-running CLI workflows, CI/CD pipelines, database migrations, data processing scripts.

**Use cases:**
- Resume a failed migration from the last successful step (checkpoint-based recovery)
- Retry flaky network steps automatically with backoff
- Human approval gate before a destructive operation (`wait_for_input`)
- Full audit trail of exactly what ran, when, and what the output was

Think of it as a local alternative to GitHub Actions for operations that run on the developer's machine, with resume-on-failure as a first-class feature.

---

## Game Engines

Quest systems, narrative branching, event-driven game logic.

**Use cases:**
- Timed events ("unlock after 24h real time") — `delay` with absolute duration
- Player decision signals resuming paused branches — `wait_for_input` + signals
- Multi-session quest state that persists across play sessions — SQLite-backed instances
- A/B testing different narrative branches server-side — load sequence variant by segment

Game studios currently build all of this from scratch per game. The engine gives them a tested runtime for free.

---

## AR / VR

Spatial computing applications (Apple Vision Pro, Meta Quest).

Multi-step spatial experiences — onboarding tutorials, guided spatial workflows, collaborative AR sessions. `wait_for_input` maps to "wait for all participants to be positioned correctly before the next step fires."

---

## Summary

| Embedding context | What the engine replaces |
|---|---|
| Mobile | Ad-hoc state in UserDefaults / SharedPreferences |
| Browser | Multi-page form state hacks, localStorage flags |
| Desktop | Custom pipeline managers, hand-rolled retry logic |
| IoT / Edge | Hand-written state machines in firmware |
| CLI | Bash scripts with no resume capability |
| Games | Per-game quest system implementations |
| AR / VR | Custom spatial experience state machines |

---

## The positioning

Every other workflow engine is built for servers talking to servers.

Orch8 is small enough and fast enough to run **inside** a process — anywhere a process runs. That's the category: **the embeddable workflow runtime**.

Server orchestration is a solved market. Embedded orchestration is not.
