# orch8_flutter

Flutter bridge for the Orch8 `0.7.1` embedded durable workflow engine.

```yaml
dependencies:
  orch8_flutter: ^0.7.1
```

```dart
import 'package:orch8_flutter/orch8_flutter.dart';

final orch8 = Orch8();
await orch8.initialize();
```

The plugin supports Swift Package Manager on current Flutter releases and
CocoaPods as a compatibility fallback. Android resolves the native AAR from
Orch8's public, read-only Maven repository.

Requires Dart 3.12+, Flutter 3.44+, iOS 16+, Xcode 16+, and Android API 24+.

## Step handlers

`registerHandler` runs your Dart handler for each step: the native engine
thread waits for the returned future (up to `handlerTimeoutMs`). Throw
`PermanentHandlerException` to fail without retry; any other error is
retryable.

## Where the database lives, backups and lifecycle

When `dbPath` is omitted the database is created in a location that is **not
backed up**:

| Platform | Default                                | Notes                                                                                                                                                    |
| -------- | -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| iOS      | `<Application Support>/Orch8/orch8.db` | hidden from Files, `isExcludedFromBackup`, protection class `completeUntilFirstUserAuthentication` (readable by background tasks after the first unlock) |
| Android  | `<noBackupFilesDir>/orch8.db`          | excluded from Auto Backup and device transfer                                                                                                            |

A database that already exists at the previous default (iOS `Documents/orch8.db`,
Android `getDatabasePath("orch8.db")`) keeps being used, so upgrades do not lose
data; it stays subject to the OS backup rules of that location. Pass `dbPath`
to choose a location explicitly. If you want the workflow state to be restored
from a backup, set `dbPath` to a backed-up directory and add the matching
Android `dataExtractionRules` / `fullBackupContent` entry yourself.

The engine is shut down automatically when the Flutter engine is detached
(`onDetachedFromEngine` on Android, `detachFromEngine` on iOS). Step handlers and the token provider are Dart
functions: they need a live Flutter engine/isolate. When none is
available (headless/background launch without Dart, or after detach) the step
fails immediately as retryable instead of waiting for `handlerTimeoutMs`.

## Errors

Failures are `PlatformException`s with `code = "ORCH8_<KIND>"` (for example
`ORCH8_NOT_FOUND`) and `details['kind']`; use `orch8ErrorKindOf(error)` to
classify them as an `Orch8ErrorKind`. The previous code (`ORCH8_ERROR`,
`INIT_ERROR`) is available as `details['legacyCode']`.

## Account and catalog scopes (unpublished development API)

The scoped API requires bindings and native artifacts built from the same engine
commit. It is not available in the published `0.7.1` binaries.

```dart
final account = await orch8.openAccount(const AccountIdentity(
  accountKey: 'signed-in-account', issuer: 'trusted-issuer', subject: 'user-id',
));
final catalog = await account.openCatalog(const CatalogConfiguration(
  catalogKey: 'invoices', source: 'backend', signingAuthority: 'catalog-key',
  manifestUrl: 'https://backend.example/manifest',
  identityUrl: 'https://backend.example/identity',
));
await catalog.registerHandler('approve', (context, step, inputJson) async {
  // context includes account/catalog, run, attempt and stable effectId.
  return '{"approved":true}';
});
final run = await catalog.prepareRun(const CatalogRunRequest(
  workflow: 'invoice', operationKey: 'backend-invoice-id',
));
await run.start();
final snapshot = await run.snapshot();
```

Supply identity from the trusted sign-in flow. Catalog token sources belong to
individual `sync` calls. Session epochs remain strings so 64-bit ownership is
preserved. A cursor belongs to one catalog session; answers use the snapshot's
exact update version. Use `CatalogHandlerFailure.permanent` for terminal host
errors and `CatalogHandlerFailure.retryable` for transient failures.

`dispose()` releases a temporary view and preserves its children. `closeView()`
fences descendants; account `retire()` fences every sibling view from the old
session. Native retirement cannot cancel an already-entered Dart future: its
physical callback charge remains until that future settles. Keep host handlers
bounded and return their real completion futures.

After the app or backend acknowledges a terminal outcome, call
`run.archiveAfterAcknowledgement(receipt)`. This frees full local run state while
retaining compact outcome/deduplication receipts. Archiving is explicit and does
not authorize deleting those receipts. Scoped native event observers and
account-owned distributed workers are still separate work.

## Runtime node (engine release after 0.7.1)

The device can join the distributed-execution mesh as a runtime of kind
`mobile` and run server-placed steps with its Dart handlers. These calls need
the native engine release that follows `0.7.1`.

```dart
await orch8.initialize(Orch8Config(syncUrl: syncUrl, deviceId: deviceId));
final runtimeId = await orch8.nodeRuntimeId();
await orch8.setTokenProvider(() => myBackend.deviceSession(deviceId, runtimeId)); // see below
await orch8.registerHandler('scan_document', (step, input) async {
  final task = Orch8TaskContext.fromInput(input); // null for local steps
  return await scan(input, idempotencyKey: task?.effectId);
});
await orch8.registerNode(const NodeCapabilities(hardware: ['camera']));
await orch8.startWorker();

await orch8.onPushWake(message.data);                      // id-only wake hint
await orch8.runWorkerWindow(const Duration(seconds: 25)); // background window
```

## Authenticating the device: device sessions (engine release after 0.7.1)

**Never ship an operator key (or any long-lived stored API key) in an app**:
anyone can extract it from the binary. The recommended flow:

1. Your **app backend** holds the operator key. After authenticating the user
   its own way, it mints a short-lived device session for the device's
   `deviceId` and `await orch8.nodeRuntimeId()` with
   `POST /runtimes/device-sessions`.
2. The **app** fetches that `dst_…` token from its backend through
   `setTokenProvider`. The plugin awaits the first token, and the engine asks
   Dart for a fresh one (the same function) whenever the control plane
   answers `401`, then retries the request once. Call it after `initialize`
   and before `registerNode`.

Backend (Node, [`@orch8.io/sdk`](https://github.com/orch8-io/sdk-node)):

```typescript
import { Orch8Client } from "@orch8.io/sdk";

const orch8 = new Orch8Client({
  baseUrl: "https://api.example.com",
  tenantId: "acme",
  headers: { "x-api-key": process.env.ORCH8_OPERATOR_KEY! },
});

// POST /device-session  { deviceId, runtimeId }  (behind your own user auth)
app.post("/device-session", requireUser, async (req, res) => {
  const session = await orch8.createDeviceSession({
    deviceId: req.body.deviceId,
    runtimeId: req.body.runtimeId, // the app's nodeRuntimeId()
    handlers: ["scan_document"], // handler allowlist; [] = delegation-only
    ttlSecs: 3600, // default 3600, max 86400
  });
  res.json({ token: session.token, expiresAt: session.expiresAt });
});
```

App backends may include optional `queues` in the device-session mint request
to grant explicit `/workers/tasks/poll/queue` access (at most 16 named queues).
Normal polls use the default queue; a named poll must match a signed queue grant.
The SDK token provider continues returning the opaque session token.

App:

```dart
final runtimeId = await orch8.nodeRuntimeId();
await orch8.setTokenProvider(
  () async => (await api.post('/device-session', {'deviceId': deviceId, 'runtimeId': runtimeId}))['token'] as String,
  refreshTimeout: const Duration(seconds: 30), // how long the engine waits for a refresh
);
await orch8.registerNode(const NodeCapabilities(hardware: ['camera']));
```

A device session is bound to its tenant, device, runtime and handler
allowlist and reaches only this device's own mobile, worker-lease and
delegation calls. `Orch8Config.syncApiKey` is the **legacy** path and not for
production apps: it still works, and the native SDK logs a warning when the
server reports the key is operator-capable.

## Delegating from a phone-local workflow (engine release after 0.7.1)

A step of a workflow running on the device's own engine whose `$runtime`
places it on another runtime (`runtime_id` of another node, or
`runtime_kinds` without `mobile`) is handed to that runtime through the server
mailbox; the local instance parks and resumes exactly once with the result,
across disconnects and app kills. Handler `orch8.delegation` delegates the
server-side sequence `params.sequence_id` with `params.input`; any other
handler delegates just that step. Needs `registerNode` and a node credential
allowed to call the continuity API.

```dart
await orch8.registerNode();
await orch8.startDelegation(const DelegationOptions(tenantId: 'acme')); // every launch

// Explicit delegation from app code (no local step is parked):
final id = await orch8.delegate(DelegateRequest(
  instanceId: localInstanceId,
  destinationRuntimeId: desktopRuntimeId,
  subSequenceId: classifySequenceId,
  input: {'photo': {'id': photoId}},
));
final status = await orch8.delegationStatus(id);
// status.state: preparing | delegated | completed (status.output) | failed (status.error) | abandoned

await orch8.listDelegations();  // journal, oldest first
await orch8.delegationStats();  // running, delegated, completed, failed, abandoned, resumed
await orch8.stopDelegation();   // pause; journaled delegations resume on the next start
```

`onPushReceived()` / `onPushWake(...)` advance pending delegations at once.
