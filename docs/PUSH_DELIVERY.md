# Durable Push Delivery

Mobile commands and their silent APNs/FCM wakes use one durable delivery path.
The API inserts the command and its `push_wake_outbox` row in the same database
transaction. A process crash can therefore leave both records pending, but
cannot persist a command without a corresponding wake.

## Delivery lifecycle

Every server runs a bounded `PushOutboxWorker`. PostgreSQL claims due rows with
`FOR UPDATE SKIP LOCKED`; SQLite serializes claim-and-lease with
`BEGIN IMMEDIATE`. A 30-second lease makes abandoned in-flight work eligible
after a worker crash without allowing healthy workers to deliver the same row
concurrently.

Provider outcomes are persisted as:

- `delivered`: APNs or FCM accepted the wake;
- `pending`: a transient network, throttling, or provider failure, scheduled
  with capped exponential backoff;
- `terminal/invalid_token`: the provider rejected the device token;
- `terminal/permanent_failure`: a non-retryable provider response;
- `terminal/misconfigured`: provider credentials or platform routing are not
  configured correctly; or
- `terminal/retry_limit`: eight completed attempts were exhausted.

Each row retains its attempt count, last bounded error, delivery timestamp,
terminal reason, and the associated tenant, device, and command identifiers.
When a device acknowledges commands through `/mobile/sync`, the same
transactional record receives `command_acked_at`. This distinguishes provider
acceptance from confirmed device command processing.

## Guarantees and limits

- Command plus wake creation is atomic.
- Claiming is bounded and safe across multiple server nodes.
- A worker crash is recovered after lease expiry.
- Retry classification is typed; invalid and permanent errors do not loop.
- Duplicate enqueue requests for the same tenant/device/command are
  idempotent at the storage boundary.
- Push payloads contain no workflow state; the vendor wake only prompts the
  authenticated device to fetch its durable command mailbox.

APNs and FCM acceptance is not proof that an app processed a command. Only
`command_acked_at` provides that evidence. Delivery is therefore at-least-once
at the wake boundary and idempotent at the command identifier boundary.
When no real provider is configured, the worker leaves rows pending; a no-op
provider never manufactures successful delivery evidence.
