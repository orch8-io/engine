# Typed Dataflow

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

## CI and API usage

Analyze a draft sequence with `POST /sequences/dataflow`, or a stored sequence
with `GET /sequences/{id}/dataflow`. Both return the compatibility report, a
canonical schema artifact, deterministic TypeScript and Python bindings, the
generator version, and a SHA-256 hash of normalized sequence content.

The same findings are included in sequence preflight. Any dataflow error makes
preflight fail and names the producer/reference and consumer chain. Unknown
evidence remains a warning.

Use the CLI in CI or to generate bindings locally:

```sh
orch8 sequence dataflow --file checkout.json --out-dir generated
orch8 sequence dataflow --id 019abcde-0000-7000-8000-000000000001 --out-dir generated
```

The output directory receives `types.ts`, `types.py`, `schema.json`, and
`report.json`. Files are written atomically. The command exits non-zero when
the report contains an error, so an unsafe sequence cannot pass a CI gate.

Generated files carry `orch8-dataflow-v1` and the normalized sequence hash.
Regenerating an unchanged sequence produces byte-identical TypeScript, Python,
and schema artifacts.

## Schema authoring rules

Declare every consumed field in the sequence input schema or the producing
step's `output_schema`. Mark guaranteed object properties as `required`; an
unlisted property is optional even if it has a type. Include `null` only when a
consumer handles it. A template fallback, such as
`{{ outputs.lookup.email | default('unknown') }}`, documents that handling and
allows the static check.

Open or intentionally dynamic values are valid escape hatches, but their
`TYPE_UNKNOWN` findings should be reviewed at release and migration boundaries.
