# Schedule recurring workflow runs

Use a cron schedule when Orch8, rather than an upstream service, should own
when a workflow starts. This guide publishes a schedule-safe sequence,
previews its next fires, observes a run, and then disables the schedule.

**Starting point:** the Level 3 server is running and `ORCH8_URL`,
`ORCH8_API_KEY`, and `ORCH8_TENANT_ID` are exported.

**Result:** a durable schedule fires every 10 seconds without overlapping a
still-active prior run.

## 1. Publish a schedule-safe sequence

Cron metadata is instance metadata; it is not workflow input. Create
`cron-sequence.json` with no required input fields:

```json
{
  "id": "019a0000-0000-7000-8000-000000000101",
  "tenant_id": "demo",
  "namespace": "default",
  "name": "scheduled-maintenance",
  "version": 1,
  "blocks": [
    {
      "type": "step",
      "id": "record_fire",
      "handler": "log",
      "params": { "message": "Scheduled maintenance tick" }
    }
  ],
  "created_at": "2026-01-01T00:00:00Z"
}
```

Publish it and resolve the immutable sequence ID:

```bash
orch8 sequence apply cron-sequence.json
SEQUENCE_ID=$(orch8 --output json sequence lookup \
  demo default scheduled-maintenance | jq -r '.id')
test -n "$SEQUENCE_ID" && test "$SEQUENCE_ID" != null
```

## 2. Create a short-lived schedule

Create a schedule that fires at seconds `0`, `10`, `20`, and so on:

```bash
CRON_ID=$(curl -fsS -X POST "$ORCH8_URL/cron" \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H "x-tenant-id: demo" \
  -H "content-type: application/json" \
  -d "{
    \"tenant_id\": \"demo\",
    \"namespace\": \"default\",
    \"sequence_id\": \"$SEQUENCE_ID\",
    \"cron_expr\": \"*/10 * * * * * *\",
    \"timezone\": \"UTC\",
    \"metadata\": {\"source\": \"cron-quick-start\", \"customer\": \"Cron User\"},
    \"enabled\": true,
    \"overlap_policy\": \"skip\"
  }" | jq -r '.id')
printf '%s\n' "$CRON_ID"
```

Orch8 accepts standard five-field cron expressions and its native seven-field
form:

```text
second minute hour day-of-month month day-of-week year
```

The seven-field form makes seconds explicit, which keeps this exercise short.

## 3. Preview without waiting

Preview the next five UTC instants using the same timezone and DST logic as the
scheduler:

```bash
curl -fsS \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H "x-tenant-id: demo" \
  "$ORCH8_URL/cron/$CRON_ID/next-fires?n=5" | jq
```

For production schedules, set an IANA timezone such as
`America/Sao_Paulo` and preview around DST boundaries. Keep schedule labels in
`metadata`; Orch8 also stamps `cron_schedule_id` there for attribution and
overlap checks.

## 4. Observe one scheduled instance

Wait no more than one interval, then list recent instances for this sequence:

```bash
sleep 22
orch8 instance list \
  --tenant-id demo \
  --sequence-id "$SEQUENCE_ID" \
  --limit 5
```

Inspect the schedule after its first fire:

```bash
orch8 --output json cron get "$CRON_ID" |
  jq '{last_triggered_at, next_fire_at, skipped_fires, overlap_policy}'
```

`skip` prevents a new run when a previous run from this schedule is still
active. Other policies are `allow`, `buffer_one`, and `cancel_previous`.

## 5. Disable before cleanup

Disable the schedule while preserving its history:

```bash
curl -fsS -X PUT "$ORCH8_URL/cron/$CRON_ID" \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H "x-tenant-id: demo" \
  -H "content-type: application/json" \
  -d '{"enabled":false}' | jq '{id, enabled, last_triggered_at}'
```

Delete it when it is no longer useful:

```bash
orch8 cron delete "$CRON_ID"
```

Deleting a schedule does not delete instances it already created.

## Checkpoint

You are done when:

- the preview returns five increasing timestamps;
- at least one instance was created with this sequence ID;
- `last_triggered_at` is set; and
- the disabled or deleted schedule can no longer create runs.

For timezone behavior, overlap semantics, and update fields, use the
[cron API reference](../../API.md#cron-schedules).

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| `404 sequence` | `SEQUENCE_ID` came from another database or tenant | Run the lookup against the current server |
| `400 invalid cron expression` | The field count or token is invalid | Copy the seven-field expression exactly |
| No instance appears | The cron loop has not reached the next fire yet | Check `next_fire_at`, wait one interval, and confirm the schedule is enabled |
| Fires overlap unexpectedly | The policy is `allow` | Update `overlap_policy` to `skip` or `buffer_one` |
