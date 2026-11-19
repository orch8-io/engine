# Continue an offline inspection on the server

Start the engine and use the same tenant in each terminal. Set `ORCH8_URL` and `ORCH8_API_KEY` for that engine.

```sh
orch8 sequence apply examples/mobile-inspection/reconnect/sequence.json
orch8 sequence lookup demo default review-field-inspection --output json
python3 examples/private-workers/worker.py --handler private_compute
```

Copy the sequence ID from the lookup.

## Automatic upload on iOS and Android

Create the gitignored file `examples/mobile-inspection/Sources/Inspection/Resources/reconnect-config.json` with the endpoint reachable from the device, the tenant, and the deployed sequence ID.

```json
{
  "url": "https://inspection.example",
  "tenant": "demo",
  "sequence_id": "0c2b4115-0f35-4ab7-a9c7-81fe496b560d"
}
```

For a private test build against a disposable engine, you can add its temporary key under `api_key`. The app bundle contains that value, so use an authenticated gateway for builds that you distribute. Regenerate the iOS project with `xcodegen generate`, or rebuild and install the Android app. Both clients use `/api/v1` on the configured HTTPS endpoint. With no configuration file, completion and export work offline.

1. Disable device networking and finish an inspection. Keep the app open or force-quit it after the result is saved.
2. Restore connectivity and bring the app to the foreground. The starter retries while in the foreground, backing off to 30 seconds between attempts.
3. Read the execution ID under **Received by server**. Use that ID to review the execution in the dashboard.

The queue holds the inspection JSON and the execution receipt in app-private storage. The app saves a pending item before starting the request and retains it if the response is lost. A retry sends the same instance ID and evidence. The engine returns the original execution ID for a matching retry. The app checks the stored evidence before accepting a deduplicated receipt. A mismatched inspection remains in the queue as rejected and is shown in the app. Pending items are tied to the endpoint, tenant, and sequence after the first attempt. Responses with HTTP 400, 413, or 422 mark that inspection as needing attention while the app attempts later uploads. After addressing the rejection, use **Retry inspection upload** to requeue it for the same destination with the original submission ID.

The upload contains the local instance ID, site ID, result, and the optional iOS site verification boolean. The native camera decodes tags on-device.

## Manual submission

Finish an inspection offline, export it from the app, and submit it from a connected machine.

```sh
python3 examples/mobile-inspection/reconnect/submit.py inspection.json \
  --sequence-id "$SEQUENCE_ID"
```

The response contains the server execution ID and acknowledges receipt of the export.

## Supervisor decision

Inspect the execution ID in the dashboard, wait for the worker to complete, then submit a supervisor decision.

```sh
orch8 signal "$EXECUTION_ID" custom:human_input:approve_inspection \
  --payload '{"value":"approved"}'
```

The export accepts the local instance ID, site ID, inspection result and optional site-verification boolean. The same local instance may be submitted again after an interrupted connection. The server execution ID stays the same. A repeated submission carrying different evidence is rejected.

## Exercise mobile incidents

Build the mobile SDK test harness with `cargo test -p orch8-mobile --lib --no-run`.
Use the executable path printed by Cargo to run the supported simulated incidents:

```sh
python3 examples/mobile-inspection/incidents.py "$MOBILE_TEST_EXECUTABLE"
```

Pass individual scenario names to run a subset. The suite exercises process
death, session expiry, critical battery, a missed wake, and loss of connection
during a lease. Use the starter's native tests to verify package installation
on a simulator or device.
