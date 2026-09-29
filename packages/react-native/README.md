# @orch8.io/react-native-orch8

React Native bridge for the Orch8 embedded durable workflow engine. The npm
version is the Orch8 engine release it embeds: `0.7.1` resolves the
`Orch8Mobile` 0.7.1 pod and `io.orch8:orch8-mobile:0.7.1` AAR.

```bash
npm install @orch8.io/react-native-orch8@0.7.1
cd ios && pod install
```

Requires React Native 0.71+, iOS 16+, Xcode 16+, Android API 24+ and JDK 17.

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

await orch8.initialize({ syncUrl, deviceId, syncApiKey });
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

## Runtime node (engine release after 0.7.1)

The phone can join the distributed-execution mesh as a runtime of kind
`mobile` and run server-placed steps with its JS handlers. These calls need
the native engine release that follows `0.7.1`; the `0.7.1` pod and AAR do not
contain them.

```ts
await orch8.registerHandler("scan_document", scanHandler);
await orch8.registerNode({ hardware: ["camera"], pushToken: fcmOrApnsToken });
await orch8.startWorker({ maxConcurrentTasks: 1 });

// Silent push (id-only wake hint) -> immediate leased poll.
await orch8.onPushWake(remoteMessage.data);

// Background window (BGTask / WorkManager / headless JS).
const { completed, budgetExhausted } = await orch8.runWorkerWindow(25_000);

await orch8.workerStats();      // { running, inFlight, claimed, completed, failed, released, lost }
await orch8.enableBuiltin("http_request");
await orch8.stopWorker();
await orch8.unregisterNode();   // advertise draining
```

For remote tasks the handler input includes a reserved `__orch8` member;
`ctx.task` exposes it in camelCase (`effectId`, `taskId`, `instanceId`,
`attempt`, `runtimeId`, `continuityEpoch`). Send `effectId` to downstream APIs
as the idempotency key. See the
[Mobile SDK guide](https://github.com/orch8-io/engine/blob/main/docs/MOBILE_SDK.md#the-phone-as-a-runtime-node).
