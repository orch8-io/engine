# Negotiated gRPC Worker Stream

`Orch8Service.WorkerStream` is a bidirectional, demand-driven alternative to
the unary worker polling RPCs. The unary methods remain compatible; workers
can adopt the stream without changing workflow definitions or task storage.

## Session handshake

The first client frame must be `WorkerStreamOpen` and declares:

- a stable worker identifier;
- one to 64 handler names;
- supported protocol features;
- desired maximum in-flight work; and
- protocol version `1`.

The server replies with `WorkerStreamHello`, containing the exact feature
intersection and authoritative bounds. Version mismatches fail with
`FAILED_PRECONDITION`; oversized or malformed opens fail before task claims.
Version 1 supports `task_delivery`, `completion`, `failure`, `heartbeat`, and
`cancellation`. `task_delivery` is mandatory. Operations not negotiated by the
client are rejected.

Current limits are 256 in-flight tasks, 1 MiB per protocol frame, 64 handlers,
and a recommended 15-second heartbeat interval. A client controls backpressure
with `WorkerStreamDemand.capacity`; the server never claims more than both the
requested capacity and the remaining session window.

## Task lifecycle

Claimed tasks arrive as canonical `WorkerTask` JSON in `WorkerStreamTask`.
Completion, failure, and heartbeat frames reuse the same durable operations as
the unary RPCs, including worker ownership checks, tenant scoping, retry
transactions, context bounds, and terminal-instance compare-and-set behavior.
Successful mutations receive a `WorkerStreamAck`.

The session tracks task identifiers it delivered. A worker cannot complete,
fail, or heartbeat work claimed by another session. When a heartbeat discovers
that a delivered task is no longer active, the server emits
`WorkerStreamCancellation` and removes it from the session window.

## Reliability boundary

Task claims and outcomes remain durable in the shared storage backend; the
stream itself is not a durable log. After disconnect, uncompleted claims are
recovered by the existing stale-worker reaper and can be claimed by a later
session. Clients must treat task identifiers as idempotency keys and reconnect
with a new open handshake.

## Resumable artifact and continuity transfer

`Orch8Service.ArtifactTransfer` uses the same bounded open/hello discipline
for instance-owned binary artifacts and continuity payload artifacts. The
client supplies an object key, transfer kind (`artifact` or `continuity`),
resume offset, desired chunk size, and optionally the expected whole-object
SHA-256 digest.

The server authorizes the instance prefix against the authenticated tenant,
loads the durable object-store payload, verifies the expected digest, and
returns its authoritative size, digest, accepted offset, and chunk bound.
Chunks are 4 KiB to 1 MiB, carry their own SHA-256 digest and exact byte
offset, and are sent one at a time. The next chunk is not emitted until the
client acknowledges the exact next offset. A mismatched acknowledgement fails
the stream; reconnecting with the last verified offset resumes without
retransmitting the prefix.

This protocol deliberately transfers object-store bytes rather than embedding
capsules, checkpoints, provenance, or large step artifacts into control-plane
messages. Those formats remain independently versioned and signed; the stream
provides bounded transport, checksum verification, tenant authorization, and
resume semantics without becoming another persistence layer.
