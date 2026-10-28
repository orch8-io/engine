# @orch8.io/react-native-orch8

React Native bridge for the Orch8 embedded durable workflow engine. The npm
version is the Orch8 engine release it embeds: `0.7.1` resolves the
`Orch8Mobile` 0.7.1 pod and `io.orch8:orch8-mobile:0.7.1` AAR.

```bash
npm install @orch8.io/react-native-orch8@0.7.1
cd ios && pod install
```

Requires React Native 0.71+, iOS 16+, Xcode 16+, Android API 24+ and JDK 17.

## Multiple accounts and catalogs (development sources)

The scoped API below requires generated bindings and a native library built from
the same engine commit. It is not present in the published 0.7.1 pod/AAR. Build
and pair the native artifacts before using these sources; the wrapper refuses
unavailable scoped operations rather than falling back to global legacy APIs.

```ts
const account = await orch8.openAccount({
  accountKey: "signed-in-account", issuer: "trusted-issuer", subject: "trusted-subject",
});
const catalog = await account.openCatalog({
  catalogKey: "inbox", source: "publisher-source", signingAuthority: "ed25519:<base64>",
  manifestUrl: "https://catalog.example/manifest", identityUrl: "https://catalog.example/identity",
});
await catalog.registerHandler("apply", async (context, stepName, inputJson) => {
  // Use context.effectId for host-side idempotency. Identity comes from the engine.
  return JSON.stringify({ applied: true });
});
await catalog.sync({ currentToken: () => cachedAccessToken, refreshToken: refreshAccessToken });
const run = await catalog.prepareRun({ workflow: "approval", operationKey: "stable-request-key" });
await run.start();
const snapshot = await run.snapshot();
if (snapshot?.state === "waiting" && snapshot.pendingSteps.includes("approval")) {
  await run.answer(snapshot, "approval", "yes");
}
// After a terminal run has been acknowledged by the app/backend:
// await run.archiveAfterAcknowledgement(acknowledgementReference);
```

Epochs are decimal strings, preserving signed 64-bit precision in JavaScript.
Handlers and token providers are routed by immutable native-issued ownership and
per-operation callback keys. Switching the UI's selected account does not change
an existing handle. A stale snapshot answer is refused by its exact native
`updatedAt` version. `catalog.history(cursor, limit)` returns metadata and an
opaque cursor bound to the original catalog/session, with page sizes 1–128.

Call `closeView()` to fence a view and its descendants without deleting durable
work. Call `dispose()` when a wrapper or history cursor is no longer needed;
disposing a temporary parent preserves already-issued children. `account.retire()`
durably fences the entire account session, including sibling views. Reopening the
same trusted identity creates a new epoch. Closing trees and retiring accounts
reclaim their native handle-table entries; the table permits at most 4,096 live
views per engine, 1,024 per stable account and 512 per catalog. Release unused
cursors and run views during long sessions.

Scoped JS handlers and token refreshes each have a ceiling of 32 per loaded SDK module
unsettled host calls and 2 per stable account ID. A native timeout or engine
replacement does not release this JS charge until the host promise settles.
Scoped native reply slots likewise remain pending through Rust's logical timeout
and root retirement; they are not force-completed by legacy bridge teardown.
The actual late host reply releases the physical foreign call. App callbacks
must settle their promises to restore that account's physical capacity.
Pending registrations are bounded at 1,024 and sync bridge requests at 32 per
client across replacements. Native execution, registration, storage and physical
callback bounds still apply independently. These JS limits do not coordinate
independent JavaScript runtimes. This does not cancel arbitrary app
promises or complete the scoped worker/delegation rollout.

Explicit `archiveAfterAcknowledgement(reference)` releases hot request admission
and stored input while retaining compact deduplication/outcome receipts. The
reference is trusted host input, not a backend credential verified by the engine.
There is no automatic archive or receipt eviction. See the
[scoped native contract](../../docs/SDK_SCOPED_NATIVE.md) for bounds and remaining
ADR-0002 readiness requirements.

## Android: Orch8's Maven repository

The AAR lives in Orch8's public, read-only Maven repository
(`https://raw.githubusercontent.com/orch8-io/maven/main`). Gradle resolves the
app's dependency graph with the **app's** repositories, so the library adds
that repository to every project of the build automatically. If your
`settings.gradle` sets `RepositoriesMode.FAIL_ON_PROJECT_REPOS` (or you prefer
to declare it yourself), set `orch8.addMavenRepository=false` in
`android/gradle.properties` and add it where your build declares repositories:

```groovy
// android/build.gradle (React Native template)
allprojects {
    repositories {
        maven { url "https://raw.githubusercontent.com/orch8-io/maven/main" }
    }
}
```

```kotlin
// or settings.gradle.kts
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven("https://raw.githubusercontent.com/orch8-io/maven/main")
    }
}
```

Check: `./gradlew :app:dependencies --configuration releaseRuntimeClasspath | grep orch8-mobile`.

## Usage

```ts
import { orch8, PermanentHandlerError } from "@orch8.io/react-native-orch8";

await orch8.initialize({ syncUrl, deviceId });
// Before registerNode/sync: device sessions from your backend (see below).
await orch8.setTokenProvider(fetchDeviceSession);
await orch8.registerHandler("scan_document", async (stepName, input, ctx) => {
  const params = JSON.parse(input);
  const result = await scan(params.document, {
    // Remote tasks carry the server's idempotency key; null for local steps.
    idempotencyKey: ctx.task?.effectId ?? undefined,
  });
  if (!result) throw new PermanentHandlerError("unsupported document");
  return { pages: result.pages }; // JSON string or serialisable value
});
await orch8.loadSequenceFromJson(sequence);
await orch8.resume();
const id = await orch8.start("onboarding", { user_id: "abc123" });
```

Handlers run in JS: the native engine thread emits the step to JS and waits
for the returned promise, up to `handlerTimeoutMs` (default 30 s). A thrown
error is retryable unless it is a `PermanentHandlerError`.

## Authenticating the phone: device sessions (engine release after 0.7.1)

**Never ship an operator key (or any long-lived API key) in the app** — anyone
can extract it from the bundle and act on your tenant. Instead:

1. Your app backend holds the operator key.
2. After authenticating the user its own way, it mints a short-lived device
   session (`dst_…`) for this device and its `nodeRuntimeId`, limited to the
   handlers the phone runs (`POST /api/v1/runtimes/device-sessions`).
3. The app fetches that token through `orch8.setTokenProvider`. The SDK sends it
   on every control-plane call (registration, worker leases, delegation,
   `/mobile/sync`) and, when the server answers `401` (expired session), calls
   your function again and retries the request once.

```ts
// App: call after initialize(), before registerNode()/sync().
const runtimeId = await orch8.nodeRuntimeId();
async function fetchDeviceSession(): Promise<string> {
  const res = await fetch("https://app.example.com/orch8/device-session", {
    method: "POST",
    headers: {
      authorization: `Bearer ${userSession}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ deviceId, runtimeId }),
  });
  if (!res.ok) throw new Error(`device session: ${res.status}`);
  return (await res.json()).token;
}
await orch8.setTokenProvider(fetchDeviceSession);
await orch8.registerNode({ hardware: ["camera"] });
```

```ts
// Your backend (Node), with @orch8.io/sdk. The operator key never leaves it.
import { Orch8Client } from "@orch8.io/sdk";

const orch8 = new Orch8Client({
  baseUrl: "https://orch8.example.com",
  tenantId: "my-tenant",
  headers: { "x-api-key": process.env.ORCH8_OPERATOR_KEY! },
});

app.post("/orch8/device-session", requireUser, async (req, res) => {
  // Check that req.body.deviceId belongs to the signed-in user first.
  const session = await orch8.createDeviceSession({
    deviceId: req.body.deviceId,
    runtimeId: req.body.runtimeId,
    handlers: ["scan_document"], // [] = delegation-only phone
    ttlSecs: 3600, // default 3600, max 86400
  });
  res.json({ token: session.token, expiresAt: session.expiresAt });
});
```

App backends may include optional `queues` in the device-session mint request
to grant explicit `/workers/tasks/poll/queue` access (at most 16 named queues).
Normal polls use the default queue; a named poll must match a signed queue grant.
The SDK token provider continues returning the opaque session token.

The raw call is `POST /api/v1/runtimes/device-sessions` with
`{"device_id", "runtime_id", "handlers", "queues", "ttl_secs"}` and the operator key in
`x-api-key`; it answers `{"token", "device_id", "runtime_id", "expires_at",
"handlers", "queues"}` (`queues` is omitted when empty). A rejected or empty refresh leaves the request failed (401); the
native side waits for your function up to `handlerTimeoutMs`.

`syncApiKey` in `initialize` is the **legacy** path and not for production
apps: a stored key in the app is extractable, and an operator-capable key makes
the SDK log a warning. See the engine's `docs/MOBILE_SDK.md` for which routes a
device session reaches.

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

The engine is shut down automatically when the native module is invalidated
(JS reload, app teardown). Step handlers and the token provider are JavaScript
functions: they need a live JS runtime with an attached listener. When none is
available (headless/background launch without JS, or after teardown) the step
fails immediately as retryable instead of waiting for `handlerTimeoutMs`.

## Errors

Rejections carry `code = "ORCH8_<KIND>"` (for example `ORCH8_NOT_FOUND`) and
`userInfo.kind`; use `orch8ErrorKind(error)` to classify them as an
`Orch8ErrorKind`. The previous code (`ORCH8_ERROR`, `INIT_ERROR`) is available
as `userInfo.legacyCode`.

## Runtime node (engine release after 0.7.1)

The phone can join the distributed-execution mesh as a runtime of kind
`mobile` and run server-placed steps with its JS handlers. These calls need
the native engine release that follows `0.7.1`; the `0.7.1` pod and AAR do not
contain them.

```ts
await orch8.registerHandler("scan_document", scanHandler);
await orch8.setTokenProvider(fetchDeviceSession); // device session allowing "scan_document"
await orch8.registerNode({ hardware: ["camera"], pushToken: fcmOrApnsToken });
await orch8.startWorker({ maxConcurrentTasks: 1 });

// Silent push (id-only wake hint) -> immediate leased poll.
await orch8.onPushWake(remoteMessage.data);

// Background window (BGTask / WorkManager / headless JS).
const { completed, budgetExhausted } = await orch8.runWorkerWindow(25_000);

await orch8.workerStats(); // { running, inFlight, claimed, completed, failed, released, lost }
await orch8.enableBuiltin("http_request");
await orch8.stopWorker();
await orch8.unregisterNode(); // advertise draining
```

For remote tasks the handler input includes a reserved `__orch8` member;
`ctx.task` exposes it in camelCase (`effectId`, `taskId`, `instanceId`,
`attempt`, `runtimeId`, `continuityEpoch`). Send `effectId` to downstream APIs
as the idempotency key. See the
[Mobile SDK guide](https://github.com/orch8-io/engine/blob/main/docs/MOBILE_SDK.md#the-phone-as-a-runtime-node).

## Delegating from a phone-local workflow (engine release after 0.7.1)

A workflow running on the phone's own engine can hand a step placed on
another runtime (`$runtime.runtime_id`, or `$runtime.runtime_kinds` without
`mobile`) to that runtime through the server mailbox. The local instance
parks and resumes exactly once with the result, across disconnects and app
kills. Handler `orch8.delegation` delegates the server-side sequence
`params.sequence_id` with `params.input`; any other handler delegates just
that step. Needs `registerNode` and a node credential allowed to call the
continuity API: a device session covers it for executions this runtime owns
(no operator key in the app).

```ts
await orch8.registerNode({ hardware: ["camera"] });
await orch8.startDelegation({ tenantId: "acme" }); // call again after every launch

// Explicit delegation from app code (no local step is parked):
const delegationId = await orch8.delegate({
  instanceId, // a local instance
  destinationRuntimeId: desktopRuntimeId,
  subSequenceId: classifySequenceId, // server-side sequence
  input: { photo: { id: photoId } },
});
const s = await orch8.delegationStatus(delegationId);
// s.state: "preparing" | "delegated" | "completed" | "failed" | "abandoned"
// s.outputJson once completed, s.error once failed

await orch8.listDelegations(); // journal, oldest first
await orch8.delegationStats(); // { running, delegated, completed, failed, abandoned, resumed }
await orch8.stopDelegation(); // pause; journaled delegations resume on the next start
```

`onPushReceived` / `onPushWake` advance pending delegations immediately. See
the [Mobile SDK guide](https://github.com/orch8-io/engine/blob/main/docs/MOBILE_SDK.md#delegating-from-a-phone-local-workflow).
