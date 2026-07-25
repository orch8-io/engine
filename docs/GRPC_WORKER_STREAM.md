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
