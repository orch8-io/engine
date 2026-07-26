# Tenant partition routing

Orch8 can route tenants to independently configured storage backends through
`TenantPartitionRouter`. Routing is authoritative and fail closed: every tenant
must have a durable placement record and that record must name a backend
registered in the current process. There is no default-backend fallback.

## Control-plane records

Migration 078 adds `tenant_storage_placements`:

- `tenant_id` is the unique routing key.
- `backend_id` is an operator-defined identifier, not a connection string.
- `epoch` is a positive, monotonically increasing fencing value.
- `updated_at` records when the control plane issued the placement.

`TenantPlacementStore::advance_tenant_placement` atomically inserts a first
placement or replaces it only when the proposed epoch is greater than the
stored epoch. Concurrent or replayed stale updates return a conflict.

Placement metadata belongs in a highly available control-plane store. It does
not contain credentials or customer payloads and is intentionally a narrow
interface separate from `StorageBackend`. Backend connection details remain in
the process's protected configuration.

## Request routing

At startup, register each permitted backend under a stable identifier. For
every tenant-scoped operation:

1. Parse and validate the caller's `TenantId` before routing.
2. Call `TenantPartitionRouter::route` with that authoritative tenant ID.
3. Perform the entire operation on the returned backend without switching
   backends mid-operation.
4. Retain the returned placement epoch in diagnostics for migration audits.

The router reads the authoritative placement for every route. An absent record
returns `NotFound`; an unknown backend identifier returns `Unsupported`.
Neither condition falls back to another tenant partition.

## Moving a tenant

Quiesce writes or use a replication/cutover protocol before advancing a tenant
to another backend. Copy and validate the data, register the destination on all
serving nodes, and then issue a placement with a higher epoch. Never reuse an
epoch. Old control-plane writers are fenced by the conditional upsert.

The router selects a partition; it does not copy data, coordinate dual writes,
or make a multi-backend transaction atomic. Those remain explicit migration
responsibilities.
