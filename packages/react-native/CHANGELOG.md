# Changelog

## Unreleased

- Fix the native modules against the current Orch8Mobile surface: full
  `MobileEngineConfig` (sync, telemetry, sequences URL, memory budget), every
  JS method now has a native implementation on both platforms, `sync` passes
  the token through a `TokenProvider` and returns `signatureFailures`,
  `flushTelemetry` returns `{ sent, dropped }`, instance state is a string
  union, and Kotlin unsigned types are converted at the bridge.
- JS step handlers actually run: the native handler emits the step to JS and
  waits for its promise (up to `handlerTimeoutMs`); `PermanentHandlerError`
  fails without retry. Blocking engine calls run off the bridge thread.
- Runtime node / worker API (needs the engine release after 0.7.1):
  `registerNode`, `updateNodeStatus`, `unregisterNode`, `nodeRuntimeId`,
  `startWorker`, `stopWorker`, `runWorkerWindow`, `workerStats`, `onPushWake`,
  `enableBuiltin`. Handlers receive `ctx.task.effectId` (from `__orch8`).
- Delegation from phone-local workflows (needs the engine release after
  0.7.1): `startDelegation`, `stopDelegation`, `delegate`, `delegationStatus`,
  `listDelegations`, `delegationStats`, with `DelegationOptions`,
  `DelegateRequest`, `DelegationStatus` and `DelegationStats` types. Options
  and requests are validated before crossing the bridge.
- Android adds Orch8's Maven repository to every project of the app build
  (opt out with `orch8.addMavenRepository=false`); the pod and AAR versions
  follow the package version.
- TypeScript types for all results, and a vitest suite.

## 0.7.1

- Fix Apple installation by aligning XCFramework slice names and requiring the
  native SDK's actual iOS 16 deployment target.

## 0.7.0

- Initial public React Native SDK release aligned with Orch8 Engine 0.7.0.
- Resolves the native Android and iOS SDKs at the same version.
