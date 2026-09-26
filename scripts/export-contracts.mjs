#!/usr/bin/env node
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const out = resolve(root, process.argv[2] ?? "contracts");
const openapi = JSON.parse(await readFile(resolve(out, "openapi.json"), "utf8"));
const schemas = openapi.components?.schemas;
if (!schemas?.SequenceDefinition) throw new Error("OpenAPI has no SequenceDefinition schema");

const rewriteRefs = (value) => {
  if (Array.isArray(value)) return value.map(rewriteRefs);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).map(([key, child]) => [
    key,
    key === "$ref" && typeof child === "string"
      ? child.replace("#/components/schemas/", "#/$defs/")
      : rewriteRefs(child),
  ]));
};

const requiredDefinitions = new Set();
const collectDefinitions = (value) => {
  if (Array.isArray(value)) {
    value.forEach(collectDefinitions);
    return;
  }
  if (!value || typeof value !== "object") return;
  const reference = value.$ref;
  if (typeof reference === "string" && reference.startsWith("#/components/schemas/")) {
    const name = reference.slice("#/components/schemas/".length);
    if (!requiredDefinitions.has(name)) {
      if (!schemas[name]) throw new Error(`OpenAPI reference has no schema: ${name}`);
      requiredDefinitions.add(name);
      collectDefinitions(schemas[name]);
    }
  }
  Object.values(value).forEach(collectDefinitions);
};

collectDefinitions(schemas.SequenceDefinition);
const definitions = Object.fromEntries(
  [...requiredDefinitions].sort().map((name) => [name, schemas[name]]),
);

const schema = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  $id: "https://orch8.io/contracts/sequence.schema.json",
  title: "Orch8 Sequence Definition",
  description:
    "A persisted Orch8 workflow sequence as stored and returned by the engine API " +
    "(POST/GET /api/v1/sequences). Includes server-assigned identity fields. " +
    "For hand-written sequence files, use sequence-file.schema.json.",
  ...rewriteRefs(schemas.SequenceDefinition),
  $defs: rewriteRefs(definitions),
};

// Authoring variant for files on disk. The persisted definition requires
// server-assigned identity (`id`, `version`, `created_at`) and scoping
// (`tenant_id`, `namespace`) that authoring tools fill in: `orch8 dev` stamps
// id/tenant_id/version/created_at and defaults namespace; `orch8 sequence
// apply` stamps id/version/created_at. Editors validating a hand-written file
// against the persisted schema would report false "missing property" errors,
// so this variant only requires what a file must actually provide.
const { required: _persistedRequired, ...sequenceBody } = rewriteRefs(schemas.SequenceDefinition);
const fileSchema = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  $id: "https://orch8.io/contracts/sequence-file.schema.json",
  title: "Orch8 Sequence File",
  description:
    "A hand-written Orch8 workflow sequence file (for example *.orch8.json). " +
    "Only `name` and `blocks` are required; `id`, `version`, and `created_at` are " +
    "assigned by `orch8 sequence apply` / the API, and `orch8 dev` also fills " +
    "`tenant_id` and defaults `namespace`. See https://orch8.io/docs.",
  ...sequenceBody,
  required: ["name", "blocks"],
  $defs: rewriteRefs(definitions),
};

await mkdir(out, { recursive: true });
for (const [file, value] of [
  ["sequence.schema.json", schema],
  ["sequence-file.schema.json", fileSchema],
]) {
  await writeFile(resolve(out, file), `${JSON.stringify(value, null, 2)}\n`);
  console.log(`wrote ${resolve(out, file)}`);
}
