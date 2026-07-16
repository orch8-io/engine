# Level 2: Pass data and choose a route

This level turns a fixed workflow into a reusable one. You will validate input,
insert run data into handler parameters, read a prior block's output, and choose
one of two routes.

**Starting point:** you completed Level 1 and are in `quickstart-work/level-1`.

**Result:** the same sequence takes a `customer` and `plan`, then produces a
different path for trial and paid customers.

## 1. Create the Level 2 definition

Create a sibling workspace:

```bash
cd ..
mkdir -p level-2
cd level-2
```

Create `sequence.json`:

```json
{
  "id": "019a0000-0000-7000-8000-000000000002",
  "tenant_id": "demo",
  "namespace": "default",
  "name": "customer-welcome",
  "version": 1,
  "input_schema": {
    "type": "object",
    "required": ["customer", "plan"],
    "properties": {
      "customer": {
        "type": "string",
        "minLength": 1
      },
      "plan": {
        "type": "string",
        "enum": ["trial", "paid"]
      }
    },
    "additionalProperties": false
  },
  "blocks": [
    {
      "type": "step",
      "id": "prepare",
      "handler": "transform",
      "params": {
        "display_name": "{{data.customer}}",
        "selected_plan": "{{data.plan}}"
      },
      "output_schema": {
        "type": "object",
        "required": ["display_name", "selected_plan"],
        "properties": {
          "display_name": { "type": "string" },
          "selected_plan": { "type": "string" }
        },
        "additionalProperties": false
      }
    },
    {
      "type": "router",
      "id": "choose_plan_path",
      "routes": [
        {
          "condition": "data.plan == \"paid\"",
          "blocks": [
            {
              "type": "step",
              "id": "paid_welcome",
              "handler": "log",
              "params": {
                "message": "Welcome {{outputs.prepare.display_name}}. Premium onboarding is ready."
              }
            }
          ]
        },
        {
          "condition": "data.plan == \"trial\"",
          "blocks": [
            {
              "type": "step",
              "id": "trial_welcome",
              "handler": "log",
              "params": {
                "message": "Welcome {{outputs.prepare.display_name}}. Your trial has started."
              }
            }
          ]
        }
      ],
      "default": [
        {
          "type": "step",
          "id": "unexpected_plan",
          "handler": "fail",
          "params": {
            "message": "No route matched the supplied plan",
            "retryable": false
          }
        }
      ]
    },
    {
      "type": "step",
      "id": "done",
      "handler": "log",
      "params": {
        "message": "Finished onboarding {{outputs.prepare.display_name}}"
      }
    }
  ],
  "created_at": "2026-01-01T00:00:00Z"
}
```

The explicit `id` and `created_at` let the API-backed preflight and dataflow
tools deserialize this file later. `orch8 dev` replaces both values for its
local session, while `orch8 sequence apply` replaces them when publishing an
immutable server version.

## 2. Run the paid path

The `--context` value becomes `context.data` for the instance:

```bash
orch8 dev . --once \
  --context '{"customer":"Ada","plan":"paid"}'
```

The completed blocks should include `prepare`, `paid_welcome`, and `done`.
`trial_welcome` is not executed.

## 3. Run the trial path

```bash
orch8 dev . --once \
  --context '{"customer":"Linus","plan":"trial"}'
```

This time `trial_welcome` runs and `paid_welcome` does not.

## 4. Follow one value through the workflow

For the paid run, the value follows this path:

```text
--context.customer
    -> context.data.customer
    -> {{data.customer}}
    -> transform output display_name
    -> outputs.prepare.display_name
    -> paid_welcome and done messages
```

Orch8 template expressions use four roots:

| Root | Contains | Typical use |
|---|---|---|
| `data` | Per-instance input in `context.data` | Customer, order, or request data |
| `config` | Per-instance configuration in `context.config` | Region, feature flags, provider choice |
| `outputs` | Results indexed by completed block ID | Feed one step's result into another |
| `state` | Durable instance key/value state | Counters and values updated during a run |

The shorter `data.customer` spelling is canonical in templates and expressions.
Some older examples may show `context.data.customer`; both describe instance
input, but new definitions should use the concise roots consistently.

## 5. Understand the two schema boundaries

`input_schema` validates the data supplied when an instance is created. It
prevents a run from starting with missing or malformed input.

`output_schema` validates the result of one block before downstream blocks can
consume it. It catches a handler that returned the wrong contract.

Together they make the dataflow explicit:

```text
validated instance input -> handler -> validated block output -> consumer
```

## 6. Prove that bad input is rejected

Run with an unsupported plan:

```bash
orch8 dev . --once \
  --context '{"customer":"Grace","plan":"enterprise"}'
```

The instance must not execute normally because `enterprise` is outside the
input schema's enum. Also try omitting `customer`:

```bash
orch8 dev . --once --context '{"plan":"trial"}'
```

Both failures happen at the input boundary, before the workflow can send a
message or invoke an application handler.

## 7. Compile the typed dataflow report

The dataflow command uses the API in server mode, which arrives in Level 3. For
now, the important static relationship is visible directly: `prepare` declares
the fields later consumers reference. After starting the server in Level 3,
return here and run:

```bash
orch8 sequence dataflow --file sequence.json --out-dir generated
```

That writes deterministic TypeScript, Python, canonical schema, and report
artifacts. See [Typed dataflow](../TYPED_DATAFLOW.md) for the generated contract.

## Checkpoint

You are ready for Level 3 when you can explain:

- Why `input_schema` and `output_schema` protect different boundaries.
- Why `outputs.prepare.display_name` is safe only after `prepare` completed.
- Why the router takes exactly one matching route and retains a defensive
  default even though the input schema currently allows only two plans.
- Why changing context creates different behavior without changing the
  immutable sequence definition.

Next: [Run the durable API server](03-durable-api.md).

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| A template prints literally | The braces or root name are misspelled | Copy `{{outputs.prepare.display_name}}` exactly |
| `prepare` fails output validation | The transform shape and schema differ | Keep `display_name` and `selected_plan` in both places |
| No route matches valid input | The condition uses a different field or spelling | Use `data.plan == "paid"` or `data.plan == "trial"` |
| The dataflow command cannot connect | No API server is running | Complete Level 3 before using the API-backed command |
