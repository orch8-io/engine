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
  ...rewriteRefs(schemas.SequenceDefinition),
  $defs: rewriteRefs(definitions),
};
await mkdir(out, { recursive: true });
await writeFile(resolve(out, "sequence.schema.json"), `${JSON.stringify(schema, null, 2)}\n`);
console.log(`wrote ${resolve(out, "sequence.schema.json")}`);
