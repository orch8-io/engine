# PostgreSQL outbox intake

Engine exposes a provider-neutral relay boundary for application-owned
transactional outboxes:

```http
POST /api/v1/events/batch
X-Tenant-Id: tenant-a

{"events":[{
  "tenant_id":"tenant-a",
  "event_name":"order.created",
  "producer_event_id":"outbox-row-1042",
  "correlation_key":"order-88",
  "payload":{"order_id":"order-88"}
}]}
```

An intake plugin reads committed rows from the application's PostgreSQL
outbox, posts up to 100 items, then marks those rows delivered. Use the
outbox row's immutable primary key as `producer_event_id`. Engine atomically
deduplicates `(tenant_id, event_name, producer_event_id)`, so the relay may
replay a whole batch after a timeout or partial response without duplicating
workflow events.

The API validates every item and tenant scope before writing the first event.
A backend interruption may accept a prefix; replaying the unchanged batch is
the recovery procedure. This keeps database credentials and table assumptions
in a deployable connector instead of coupling them to the scheduler core.
