# Offline-First Sync Workflows on iOS: Running a Durable Workflow Engine on the Phone

*The Orch8 engine — the same Rust scheduler, storage layer, and step executor that runs on servers — compiles into your iOS or Android app and executes workflows entirely on-device. No connection required. This post explains why that exists, how it works, and what it makes possible that a backend-only architecture can't.*

## The problem nobody's tooling covers

Every durable workflow engine on the market — Temporal, Inngest, Restate, and friends — assumes the workflow runs *in your infrastructure* and the phone is a dumb client. That assumption breaks for a whole class of products:

- **Field work**: inspections, deliveries, maintenance checklists that must advance through multi-step logic in a basement, on a ship, or on a construction site with no signal.
- **Onboarding and lifecycle journeys**: "show the paywall 2 days after install, but only if the user finished step 3" — logic that product teams want to change weekly without an App Store release, and that must fire even if the app is offline when the timer elapses.
- **Point-of-sale and kiosk flows**: payment retries, receipt chains, inventory updates that queue durably and reconcile when connectivity returns.

The usual answer is a hand-rolled state machine in Swift plus a sync queue plus retry timers plus crash-recovery edge cases — rewritten slightly differently in every app, and again on Android.

## What Orch8 does instead

The mobile SDK embeds the **full engine** as a native library behind UniFFI bindings (Swift Package for iOS 15+, AAR for Android 7+):

```
Host App (Swift / Kotlin / RN)
└── UniFFI bindings
    └── orch8-mobile (Rust)
        ├── Scheduler + step executor   ← the same code paths the server runs
        ├── Sync orchestrator            ← Ed25519-verified sequence sync
        └── SQLite storage               ← every step's state is a row
```

Three properties fall out of that design:

**1. Workflows are data, shipped without releases.** Sequences are JSON definitions synced from your server and verified with Ed25519 signatures before they're stored. Product changes a journey, the app syncs, the new flow runs — no binary update, no review cycle. Size and count are capped (`maxStoredSequences`, `maxSequenceSizeBytes`) so a misbehaving backend can't bloat the device.

**2. Execution is durable through anything the OS does to you.** State after every step is persisted to SQLite before the next step runs — the same snapshot-not-event-log architecture as the server engine. App killed mid-workflow? Phone rebooted? The next tick resumes from the last completed step. There is no "replay" concept to get wrong; the state is just *there*.

**3. The engine respects the platform.** A tick controller adapts cadence to power states (`pause(state:)`), background execution hooks into BGTaskScheduler on iOS and WorkManager on Android, and `on_push_received()` lets a silent push wake the engine for time-sensitive steps. Step handlers are plain Swift/Kotlin callbacks (`StepHandler` protocol), so "send a local notification," "read HealthKit," or "charge the card reader" are first-class workflow steps.

## What this looks like in practice

A delivery app's proof-of-delivery flow, defined once as JSON, synced to drivers' phones:

1. `capture_signature` (native handler → camera/canvas)
2. `compress_photos` (native handler)
3. `delay: { duration: … }` / retry loop: upload with exponential backoff while offline
4. `notify_recipient` — runs server-side when the sync reconciles

Steps 1–3 advance in a parking garage with zero bars. The engine persists each step, retries the upload on its own schedule, survives the driver force-quitting the app, and the workflow completes when connectivity returns. Nobody wrote a state machine.

## Why we think this is a category, not a feature

No other durable-execution engine runs on a phone — not as a port, not as a lite mode. The mobile runtime exists in Orch8 because the engine was built embeddable from day one (single-binary, SQLite-capable, no server dependency in the core). It's the same property that makes the engine embeddable in a Rust backend service.

If you ship an app where workflows must survive offline gaps, OS kills, or release-cycle latency, we'd genuinely like to hear what you're building — it shapes where this goes next.

- Quick start: [`docs/MOBILE_SDK.md`](../MOBILE_SDK.md)
- Engine architecture: [`docs/ARCHITECTURE.md`](../ARCHITECTURE.md)
- Discord: come tell us your offline use case.
