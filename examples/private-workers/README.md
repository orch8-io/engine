# Private worker sandbox

Run from the repository with an `orch8` binary built from this checkout, Node.js with TypeScript type stripping, and Python 3.9 or later.

```sh
sh examples/private-workers/sandbox.sh
```

The sandbox starts a loopback control plane with temporary SQLite storage, a TypeScript worker for `private_prepare`, and a Python worker for `private_compute`. The sequence runs an in-process log step and then routes each private step to the worker advertising `site=private`.

The Python worker checkpoints its first chunk and releases the lease before performing any external effect. It claims the task again, reads `resume_checkpoint.next_chunk`, and continues using the new `claim_epoch` and returned `checkpoint_seq`. The final output reports `resumed_from: 1`. The same worker code handles an expired lease when another worker gets the durable checkpoint. Keep checkpoints small and free of secrets. Once a provider call can have started, use the worker protocol's `started: true` release semantics and check the effect receipt before acting again.

For an interactive run with the dashboard and a persistent local database:

```sh
cd examples/private-workers
orch8 dev sequence.json \
  --worker 'node --experimental-strip-types worker.ts' \
  --worker 'python3 worker.py'
```

Open the execution's **Runtimes** tab for queued tasks, claims, reclaims, completion, and continuity owner changes. **Download redacted trace** exports the same events with an allowlisted set of fields. This view omits arguments, outputs, checkpoints, logs, receipt evidence, and free-text failure reasons. Legacy worker identifiers are hashed. The wait panel exposes the engine's highest-ranked diagnosis and suggested next actions during a wait or pause.

## Plan placement and check fleet readiness

After saving the sequence, use its immutable version UUID.

```sh
orch8 doctor --placement "$SEQUENCE_ID"
orch8 placement recipes
orch8 -o json placement plan "$SEQUENCE_ID" --recipe private_network --tags pilot
orch8 placement fleet-readiness "$SEQUENCE_ID"
```

Placement combines the sequence, step, and matching tenant policies with current runtime advertisements, worker registrations, and version pins. Recipe previews add a placement requirement for the duration of the request. Use `fleet-readiness` against the target environment as a CI step. It exits nonzero when a route is blocked or the bounded runtime inventory is incomplete.

Preview an exact advertised build before starting a worker canary:

```sh
orch8 placement fleet-readiness "$SEQUENCE_ID" --worker-version 2.1.0
```

For a live canary, start the chosen worker with `ORCH8_WORKER_VERSION=2.1.0`. Both starters advertise `handler_version=2.1.0` as a placement label. In a separate pilot sequence version or a tenant policy matching your pilot tag, require that label on the chosen handler. Start pilot instances explicitly. Existing residency, region, minimum-version pins, and provider-effect protections still apply. Compare the workers' version, claims, and outcomes in the dashboard before expanding the cohort.

## Explicit regional fallback

```sh
ORCH8_RESIDENCY=eu sh examples/private-workers/sandbox.sh fallback.json
```

The primary branch in `fallback.json` waits up to five seconds for an EU private GPU worker. Its explicit catch branch can then use an EU private CPU worker. The sequence-level residency requirement covers both branches, and region or trust constraints from matching tenant policies are evaluated on both. This example performs pure computation so trying the alternate route after failure is safe. For a side effect with an uncertain outcome, use **Workers → Unknown effects** and reconcile provider evidence before any retry.

## Install a private pilot worker

1. On the control plane, select **Workers → Enroll a worker** using the root API key. Enter the target tenant and a name for this worker.
2. On the worker host, save the generated worker key in a private file with mode `0600`.
3. On that host, run the installer and launch the worker:

   ```sh
   sh install.sh "$HOME/orch8-private-worker" https://orch8.example.com/api/v1 my-tenant /path/to/worker.key
   "$HOME/orch8-private-worker/run.sh" --handler private_compute
   ```

The installer creates a new directory, records a stable worker UUID, and references the key file. The worker opens outbound HTTPS connections. Verify the registration in **Workers** or run fleet readiness against the workflow UUID. Keys minted by the wizard expire after 24 hours.

To drain the REST starter, send its process SIGTERM. It stops polling and completes the current claim. For a worker using the gRPC stream, the dashboard can queue a tenant-scoped drain command and show remaining active tasks. Check the worker's status before stopping it.

### Health and upgrades

The starter prints its stable worker UUID and handler at startup. Use that UUID
in the Workers view to confirm it is alive. Run fleet readiness with the pilot
sequence and expected worker version before sending it work. Startup errors
identify the failed API route and status without logging credentials or task
payloads.

For an upgrade, install the new starter in a separate directory with a fresh
worker key or the pilot's existing private key file. Start the new process
with `ORCH8_WORKER_VERSION` set to the release version. Keep the old worker
serving its existing traffic while the pilot sequence or tag policy targets
the new `handler_version` label. Remove or restore the pilot version-label
requirement if the canary fails, and restore the earlier minimum version if
it was raised. Allow in-flight claims to finish on the worker that owns them.
Inspect unknown effects before replaying side-effecting work.
