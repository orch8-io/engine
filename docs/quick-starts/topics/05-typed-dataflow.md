# Generate typed dataflow bindings

Use typed dataflow before publishing when one step consumes structured output
from another. Orch8 checks direct references and generates deterministic
TypeScript, Python, JSON Schema, and machine-readable findings.

**Starting point:** the Level 3 server is running. The CLI dataflow command is
API-backed even when analyzing a local draft.

**Result:** compatible bindings are generated, then an intentionally broken
reference demonstrates the non-zero CI gate.

## 1. Create a typed producer and consumer

Create `typed-sequence.json`:

```json
{
  "id": "019a0000-0000-7000-8000-000000000105",
  "tenant_id": "demo",
  "namespace": "default",
  "name": "typed-customer-notice",
  "version": 1,
  "input_schema": {
    "type": "object",
    "required": ["email"],
    "properties": {
      "email": { "type": "string", "minLength": 3 }
    },
    "additionalProperties": false
  },
  "blocks": [
    {
      "type": "step",
      "id": "lookup",
      "handler": "transform",
      "params": {
        "email": "{{data.email}}",
        "segment": "trial"
      },
      "output_schema": {
        "type": "object",
        "required": ["email", "segment"],
        "properties": {
          "email": { "type": "string" },
          "segment": { "type": "string" }
        },
        "additionalProperties": false
      }
    },
    {
      "type": "step",
      "id": "notify",
      "handler": "log",
      "params": {
        "message": "Notify {{outputs.lookup.email}} in {{outputs.lookup.segment}}"
      }
    }
  ],
  "created_at": "2026-01-01T00:00:00Z"
}
```

The `lookup` schema is the producer contract. The two template paths in
`notify` are consumers.

## 2. Compile and generate files

```bash
rm -rf generated-dataflow
orch8 sequence dataflow \
  --file typed-sequence.json \
  --out-dir generated-dataflow
find generated-dataflow -maxdepth 1 -type f -print | sort
```

The directory contains:

```text
report.json  findings and checked references
schema.json  normalized sequence data contract
types.py     generated Python types
types.ts     generated TypeScript types
```

Inspect the result without depending on terminal formatting:

```bash
jq '{references_checked, findings}' \
  generated-dataflow/report.json
sed -n '1,120p' generated-dataflow/types.ts
sed -n '1,120p' generated-dataflow/types.py
```

## 3. Prove generation is deterministic

Hash the outputs, regenerate, and compare:

```bash
shasum generated-dataflow/* > before.sha256
orch8 sequence dataflow \
  --file typed-sequence.json \
  --out-dir generated-dataflow
shasum generated-dataflow/* > after.sha256
diff -u before.sha256 after.sha256
```

An unchanged definition produces byte-identical artifacts.

## 4. Demonstrate a failing reference

Create a broken copy whose consumer asks for an undeclared field:

```bash
jq '(.blocks[1].params.message) =
  "Notify {{outputs.lookup.missing}}"' \
  typed-sequence.json > typed-sequence-broken.json
```

Run the compiler and preserve its exit status:

```bash
if orch8 sequence dataflow \
  --file typed-sequence-broken.json \
  --out-dir generated-broken
then
  echo "unexpected: broken dataflow passed" >&2
  exit 1
else
  echo "expected: incompatible dataflow was rejected"
fi
```

The report identifies the missing producer path and consumer block. This is the
behavior to place in CI before `sequence apply` or release promotion.

## 5. Compile a stored immutable version

Publish the compatible draft, then compile by ID:

```bash
orch8 sequence apply typed-sequence.json
SEQUENCE_ID=$(orch8 --output json sequence lookup \
  demo default typed-customer-notice | jq -r '.id')
orch8 sequence dataflow --id "$SEQUENCE_ID" --out-dir generated-stored
```

## Checkpoint

You are done when the valid sequence generates four stable artifacts with no
error findings, the second generation has no diff, and the broken reference
exits non-zero.

Use [Typed Dataflow](../../TYPED_DATAFLOW.md) for warning semantics, fallbacks,
dynamic shapes, and compiler bounds.

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| CLI cannot connect | Dataflow draft compilation uses the server API | Start the Level 3 server and restore client variables |
| Valid field is reported optional | It is absent from the schema's `required` list | Declare it required or add an explicit template fallback |
| Report says `TYPE_UNKNOWN` | Producer has no closed output schema | Add `output_schema`, or review the warning as intentional dynamic data |
| Regenerated hashes differ | The source definition or generator version changed | Diff the definition and record the generator version from the report |
