# Changelog

## Unreleased

- Fix the native plugins against the current Orch8Mobile surface: full
  `MobileEngineConfig` (sync, telemetry, sequences URL, memory budget), native
  implementations for every Dart method (`registerHandler`, `tickOnce`,
  `cancelInstance`, `completeStep`, `loadedSequences`, `flushTelemetry` were
  declared in Dart but missing natively), `sync` token and
  `signatureFailures`, unsigned-type conversions on Android.
- Dart step handlers run: the engine thread waits for `executeStep` on the
  platform thread; engine calls moved off the platform thread.
  `PermanentHandlerException` fails without retry.
- New: `runUntilIdle`, `reportPowerState`, `getInstance`, `activeInstances`,
  `loadSequenceFromJson`, `loadSequencesFromUrl`, `onPushReceived`.
- Runtime node / worker API (needs the engine release after 0.7.1):
  `registerNode`, `updateNodeStatus`, `unregisterNode`, `nodeRuntimeId`,
  `startWorker`, `stopWorker`, `runWorkerWindow`, `workerStats`, `onPushWake`,
  `enableBuiltin`, and `Orch8TaskContext.fromInput` (`effectId`).
- `flushTelemetry` now returns `FlushResult`.

## 0.7.1

- Fix Apple installation by aligning XCFramework slice names and requiring the
  native SDK's actual iOS 16 deployment target.

## 0.7.0

- Initial public Flutter SDK release aligned with Orch8 Engine 0.7.0.
- Adds Swift Package Manager support and pins both native platforms to 0.7.0.
- Supports AGP 9 and Flutter's built-in Kotlin toolchain.
