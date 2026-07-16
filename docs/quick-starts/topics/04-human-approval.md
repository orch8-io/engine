# Add a human approval gate

Use `wait_for_input` when a workflow must pause for a validated human decision.
The decision arrives as a custom signal, is stored in `context.data`, and then
drives the next route.

**Starting point:** the Level 3 server is running with the standard client
variables exported.

**Result:** an instance appears in the approvals inbox, waits without polling
an external service, accepts one of three choices, and resumes the correct
branch.

## 1. Author the approval sequence

Create `approval-sequence.json`:

```json
{
  "id": "019a0000-0000-7000-8000-000000000104",
  "tenant_id": "demo",
  "namespace": "default",
  "name": "discount-approval",
  "version": 1,
  "input_schema": {
    "type": "object",
    "required": ["request_id"],
    "properties": { "request_id": { "type": "string" } },
    "additionalProperties": false
  },
  "blocks": [
    {
      "type": "step",
      "id": "review",
      "handler": "human_review",
      "params": {
        "instructions": "Review the requested discount",
        "reviewer": "sales-manager"
      },
      "wait_for_input": {
        "prompt": "Approve this discount?",
        "timeout": 3600000,
        "choices": [
          { "label": "Approve", "value": "approved" },
          { "label": "Reject", "value": "rejected" },
          { "label": "Escalate", "value": "escalated" }
        ],
        "store_as": "decision"
      }
    },
    {
      "type": "router",
      "id": "route_decision",
      "routes": [
        {
          "condition": "data.decision == \"approved\"",
          "blocks": [
            {
              "type": "step",
              "id": "apply_discount",
              "handler": "log",
              "params": { "message": "Discount approved" }
            }
          ]
        },
        {
          "condition": "data.decision == \"escalated\"",
          "blocks": [
            {
              "type": "step",
              "id": "escalate_discount",
              "handler": "log",
              "params": { "message": "Discount escalated" }
            }
          ]
        }
      ],
      "default": [
        {
          "type": "step",
          "id": "reject_discount",
          "handler": "log",
          "params": { "message": "Discount rejected" }
        }
      ]
    }
  ],
  "created_at": "2026-01-01T00:00:00Z"
}
```

`store_as` determines the context key. Without it, Orch8 would store the
accepted value under the block ID, `review`.

## 2. Publish and start one request

```bash
orch8 sequence preflight --file approval-sequence.json
orch8 sequence apply approval-sequence.json

SEQUENCE_ID=$(orch8 --output json sequence lookup \
  demo default discount-approval | jq -r '.id')
INSTANCE_ID=$(orch8 --output json instance create \
  --sequence-id "$SEQUENCE_ID" \
  --tenant-id demo \
  --context '{"data":{"request_id":"discount-42"}}' | jq -r '.id')
printf '%s\n' "$INSTANCE_ID"
```

Give the scheduler a moment to reach the gate:

```bash
sleep 1
orch8 --output json instance get "$INSTANCE_ID" | jq '.state'
```

The state should be `waiting`.

## 3. Read the approvals inbox

```bash
curl -fsS \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H "x-tenant-id: demo" \
  "$ORCH8_URL/approvals?tenant_id=demo" |
  jq --arg id "$INSTANCE_ID" '.items[] | select(.instance_id == $id)'
```

The item exposes the prompt, choices, block ID, storage key, waiting time, and
deadline. An operator UI can render this contract without understanding the
sequence internals.

## 4. Resolve with a human-input signal

Signals for approval gates use the custom name
`human_input:<block-id>`. Approve this request:

```bash
curl -fsS -X POST "$ORCH8_URL/instances/$INSTANCE_ID/signals" \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H "x-tenant-id: demo" \
  -H "content-type: application/json" \
  -d '{
    "signal_type": { "custom": "human_input:review" },
    "payload": { "value": "approved" }
  }' | jq
```

The value must exactly match one of the declared choice values. Invalid values
are consumed safely but leave the instance waiting for a valid decision.

## 5. Verify storage and routing

```bash
orch8 instance get "$INSTANCE_ID" --watch
orch8 --output json instance get "$INSTANCE_ID" |
  jq '{state, decision: .context.data.decision}'
orch8 --output json instance outputs "$INSTANCE_ID" |
  jq 'map(.block_id)'
```

The final state is `completed`, the stored decision is `approved`, and the
outputs include `apply_discount` rather than the other branches.

## Checkpoint

You are done when the instance is visible in `/approvals`, a valid signal
removes it from the inbox, `context.data.decision` contains the accepted value,
and only the matching route executes.

For notification callbacks, comments, timeout escalation, and mobile approval
sync, continue with [Human Review](../../SEQUENCES.md#human-review) and the live
OpenAPI.

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| Approval is not listed | Scheduler has not reached the gate | Inspect the instance tree and wait until state is `waiting` |
| Signal returns success but instance stays waiting | Value is not a declared choice | Send `approved`, `rejected`, or `escalated` exactly |
| Decision stored under the wrong key | `store_as` differs from the router field | Keep `store_as: decision` and route on `data.decision` |
| Instance fails after the decision | A downstream handler or condition is invalid | Inspect the tree and block outputs before retrying |

