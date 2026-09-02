# ADR 0001: External runtime handoff claims

- Status: Accepted
- Date: 2026-09-01

## Context

A mobile `MobileEngine` imports a continuity capsule into its own SQLite
database. Its destination instance therefore does not exist in the control
plane's `instances` table. The original server-side `accept` and `resume`
operations require such an instance and can schedule it, so using those
operations for an iPhone would either fail or falsely imply that the server
owns the device scheduler.

Push delivery and mobile execution are also interruptible. A device may stop
after consuming a one-time grant, after importing, after claiming ownership,
or after local activation. The protocol has to recover by retrying without
allowing the capsule to be claimed by another instance.

## Decision

Add explicit external-runtime transitions:

- `accept-external` verifies the exported capsule, its destination-bound
  signed grant and bearer token, then records an idempotent
  capsule/runtime/instance import binding before atomically advancing the
  continuity epoch and owner.
- `resume-external` verifies the owner and recorded binding, then records the
  handoff as resumed without scheduling an instance in server storage.
- Replays return the existing result only when the capsule binding, runtime,
  and instance all match. A different instance receives a conflict.
- The Swift actor coordinator serializes import, claim, activation, and resume
  locally. The caller retries the complete operation after interruption.
- Returning to Cloud uses the existing device-capsule attach and server-side
  accept/resume path because the cloud destination is a server instance.

The transfer key and one-time grant token are fetched over an authenticated
channel and remain out of notification payloads and durable convenience
storage.

## Consequences

The control plane can prove who owns an execution without storing or
scheduling the mobile instance. Capsule import evidence becomes part of the
ownership invariant and supports safe retry after a consumed grant. The API
surface gains two purpose-specific operations, but keeps the normal
server-to-server semantics unchanged.

This does not make iOS an always-running worker. Applications must use bounded
OS-granted execution windows, and the workflow survives between them through
durable checkpoints.
