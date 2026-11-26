# Offline field inspection

Start an inspection for site A-12, close the app while it waits, then reopen it and record the result. The app bundles the workflow and stores the run in its private SQLite database. No account, credentials, server, or network access is required at runtime.

Both apps consume the published Orch8 **0.7.1** SDK. Build dependencies download on the build machine before you take the device offline. See the [mobile compatibility matrix](../../docs/MOBILE_COMPATIBILITY.md) for the supported install paths.

The integration doctor requires Python 3.10 or newer.

## iOS

With Xcode 16+, XcodeGen, and an iOS simulator runtime installed, run from this directory.

```sh
python3 doctor.py ios --test
xcodegen generate
open FieldInspection.xcodeproj
```

Select an iPhone simulator and run the **FieldInspection** scheme. Use Xcode's **Product → Test** to run the database reopen test against the published SwiftPM binary.

On a camera-equipped iPhone, **Scan site QR tag** opens the native camera only
after an explicit tap and permission grant. Point it at a QR code containing
`A-12`. The app decodes it locally and adds a `site_tag_verified` boolean to the
inspection answer. Photos and barcode text stay out of the workflow. You can
submit the inspection if the camera is unavailable or permission is denied.

## Android

With JDK 17, Gradle 8.14.8, Android SDK Platform 35, and a running emulator or authorized device, run from this directory.

```sh
python3 doctor.py android --test
cd android
gradle :app:installDebug
```

Open **Field Inspection**. Run `gradle :app:connectedDebugAndroidTest` to test the database reopen against the published Maven AAR.

The published Android upgrade test uses two consecutive Maven releases and
preserves the app's data across installation. It queues an offline inspection
on the predecessor, then withholds the first server acknowledgement after the
upgrade. The restarted app must recover the same submission and receive the
same receipt when it retries.

To run the test with a local published engine binary and a disposable
emulator, set `ORCH8_UPGRADE_ENGINE_BINARY` to that release's
`orch8-server` executable.

## Verify offline recovery

1. Launch the app and select **Start inspection**. Wait for **Waiting for the inspection result**.
2. Force-quit the app. Disable device network access and launch the app again. The same inspection should still be waiting.
3. Select **Mark passed** or **Mark repair needed**. The app shows the saved result. Reopen the app again to confirm the result is still there.
4. On iOS or Android, select **Copy inspection export** after completion if you need the JSON for a manual handoff. The export contains the durable instance ID, site ID, and result. iOS can also include the site tag verification boolean.

## Automatic reconnect

Both apps queue the completed inspection in app-private storage. To enable automatic upload, deploy the review workflow and configure the starter with an HTTPS endpoint and sequence ID. The same JSON goes to the server after connectivity returns while the app is open, or when you reopen it. The app shows the server execution ID after recording the receipt. [Setup and verification](reconnect/README.md)

If the server rejects an inspection for validation or size, its queue entry keeps
the rejection after restart and later inspections can still upload. After
addressing the server-side issue, use **Retry inspection upload** on that
inspection. The retry uses its original submission ID and destination.

The two builds use the same [app-bundled workflow](Sources/Inspection/Resources/field-inspection-offline.json). The local workflow runs with SDK telemetry and SDK sync disabled. For account-scoped catalogs, see the [development SDK guide](../../docs/SDK_SCOPED_NATIVE.md).

## Native lifecycle

iOS refreshes persisted state when the scene becomes active. An operation started in the foreground receives a `UIApplication` background task lease so it can finish if you switch apps. The adapter releases the lease on completion or expiration and ignores a late result from an expired window.

Android admits operations while the Activity is started. `onStop` invalidates the window and discards its eventual UI update. `onStart` opens a new window and reloads from SQLite. Run `gradle :app:testDebugUnitTest` to check Activity-window cancellation and budget handling.

Both adapters serialize database access and pass the remaining portion of a five-second budget to the 0.7.1 SDK, with a maximum of 20 ticks. An SDK call that has already entered native code may finish after a window closes. The next foreground refresh reads any result it saved. This sample performs foreground work and uses the iOS continuation lease only for work that has already started.
