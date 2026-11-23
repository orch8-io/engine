# Typed Dataflow

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

Orch8 compiles declared sequence schemas and direct data references before a
workflow runs. The compiler is deliberately conservative: contradictions are
errors, while evidence it cannot prove remains an explicit warning.

## What is checked

The compiler inspects direct `data.*`, `outputs.*`, `state.*`, and `config.*`
references in step parameters, inputs, guards, loop expressions, and router
conditions. It reports stable findings for:

- a referenced producer that does not exist;
- a path excluded by a closed JSON Schema;
- optional or nullable values used without an explicit template fallback; and
- undeclared or dynamic shapes whose safety cannot be proven.

`state.*`, `config.*`, open `additionalProperties`, and producers without an
output schema are reported as `TYPE_UNKNOWN`; they are never silently treated
as statically valid. Version 1 intentionally limits sound claims to explicit
JSON Schemas and direct references. Compilation is bounded to 32 schema levels
and 4,096 rendered schema nodes.

Direct local references such as `#/$defs/address` share a definition across
path analysis and all four generated bindings. Each input or step-output schema
has its own definition scope. A recursive edge, unresolved reference, or
reference with sibling constraints falls back to a dynamic shape. The generated
`schema.json` preserves the original references.

An object with a schema in `additionalProperties` supplies a type for
undeclared map keys. A key is still optional unless the schema lists it as
required. Preflight reports a string used directly as a numeric operand in a
workflow condition.

Preflight rejects a mandatory template or collection read from a later
sequential producer. A read from a conditional producer generates a warning
when the producer is guarded, may be skipped with a loop body, or is on a
route or concurrent branch that does not guarantee availability at the read.
Use a template fallback if the consumer can proceed without the value.

Inside `for_each`, an item's schema comes from the collection's `items`
schema. A nested loop can shadow the same variable and the outer type is
restored after its body. Computed collections and heterogeneous tuples leave
the item type dynamic.

## CI and API usage

Analyze a draft sequence with `POST /sequences/dataflow`, or a stored sequence
with `GET /sequences/{id}/dataflow`. Both return the compatibility report, a
canonical schema artifact, deterministic TypeScript and Python bindings, the
generator version, and a SHA-256 hash of normalized sequence content.

Those routes are relative to `/api/v1` and require the same API-key and tenant
headers as other product endpoints. The CLI reads `ORCH8_URL`,
`ORCH8_API_KEY`, and `ORCH8_TENANT_ID`.

The same findings are included in sequence preflight. Any dataflow error makes
preflight fail and names the producer/reference and consumer chain. Unknown
evidence remains a warning.

Reference findings include a `source` object in the dataflow report. The
`sequence_pointer` locates the string containing the reference. The
half-open `start_byte` and `end_byte` offsets apply to the decoded UTF-8
string. When a producer declares a schema, `producer_schema_pointer` locates
that declaration in the sequence. Preflight includes these fields in the
`dataflow_source` evidence entry.

Use the CLI in CI or to generate bindings locally:

```sh
orch8 sequence dataflow --file checkout.json --out-dir generated
orch8 sequence dataflow --id 019abcde-0000-7000-8000-000000000001 --out-dir generated
```

The output directory receives `types.ts`, `types.py`, `Types.swift`,
`Types.kt`, `schema.json`, and `report.json`. Files are written atomically. The
command exits non-zero when the report contains an error, so an unsafe
sequence cannot pass a CI gate.

Generated files carry `orch8-dataflow-v2` and the same normalized sequence
hash. Regenerating an unchanged sequence produces byte-identical TypeScript,
Python, Swift, Kotlin, and schema artifacts. Swift uses Foundation `Codable`;
Kotlin targets `kotlinx.serialization`.

## Schema authoring rules

Declare every consumed field in the sequence input schema or the producing
step's `output_schema`. Mark guaranteed object properties as `required`; an
unlisted property is optional even if it has a type. Include `null` only when a
consumer handles it. A template fallback, such as
`{{ outputs.lookup.email | default('unknown') }}`, documents that handling and
allows the static check.

Open or intentionally dynamic values are valid escape hatches, but their
`TYPE_UNKNOWN` findings should be reviewed at release and migration boundaries.

## Scaffold a typed worker handler

```sh
orch8 sequence scaffold-handler checkout.json --block charge \
  --example-output sample-charge.json --out-dir charge-handler
orch8 test run charge-handler/handler.contracts.json --sequence checkout.json
```

The scaffold uses the selected step's `output_schema` for the return type.
TypeScript is the default. Pass `--language python`, `--language swift`, or
`--language kotlin` for the other stubs. Pass `--input` with a representative
sequence input when the definition requires one. For sequences with multiple
steps, add mocks for the other handlers traversed by the fixture.

For contracts that exercise a pinned sub-sequence, pass each child definition
with `--child-sequence`. When the same block ID appears in reachable revisions,
qualify both block mocks and output assertions with the definition's exact
name and version.

```json
{
  "name": "child validates the request",
  "mocks": [
    {
      "block": "validate",
      "definition": {"name": "risk-review", "version": 2},
      "type": "success",
      "output": {"approved": true}
    }
  ],
  "expect": {
    "assertions": [
      {
        "output": {
          "block": "validate",
          "definition": {"name": "risk-review", "version": 2},
          "path": "approved"
        },
        "op": "equals",
        "value": true
      }
    ]
  }
}
```

An explicit child assertion applies to every invocation of that revision.
Handler-name mocks still apply across definitions. Embedded Rust callers can
supply child recordings through `RunOptions::definition_recorded_outputs`.

## Preflight an offline mobile sequence

```sh
orch8 sequence preflight-mobile checkout.json --sdk published-0-7-1 --handler charge
orch8 sequence preflight-mobile checkout.json --sdk development --handler charge
```

The offline check shows app-registered handlers, built-ins and steps that
need another runtime or a separately installed child sequence. It also runs
typed-dataflow checks. For the online portion, run
`orch8 placement fleet-readiness` against the stored sequence version and
the intended worker build.


## Configuration and state schemas

Add `x-orch8-config-schema` inside `input_schema` to describe
`context.config`. A closed schema flags unknown `config.*` paths during
preflight. The server and embedded runtime validate configuration values
before activating a run.

Add `x-orch8-state-schema` inside `input_schema` to describe workflow KV
keys and their values. Declare an object schema with `properties`,
`patternProperties`, `propertyNames`, or `additionalProperties`.
Each key is validated at write time, and references to absent keys remain
optional because state is populated during execution. The schema supports
local references in value definitions.

## Pinned child workflow contracts

A sub-sequence block with an explicit `version` resolves that exact child
revision in its tenant and namespace. Preflight checks its input mapping
against the child's `input_schema` and reports the locations of both the
parent mapping and child constraint. Publication rejects proven conflicts.
Dynamic inputs also undergo validation before the child starts.

For output history returned to the parent, declare
`input_schema.x-orch8-child-result-schema` on the child. The schema describes
the array of block output records and can constrain each record's `output`.
The parent compiler checks references to these records, and the engine
validates the result before advancing the parent. With
`child_result: reference`, the returned object has one field,
`child_instance_id`, containing a UUID string.
