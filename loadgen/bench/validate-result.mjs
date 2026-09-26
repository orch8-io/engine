#!/usr/bin/env node
// Validate benchmark result files against results.schema.json.
// Dependency-free: implements the JSON Schema subset that schema uses
// (type, enum, const, required, properties, additionalProperties, items,
// minimum, pattern). `format` is checked for date-time only.
//
// Usage: node validate-result.mjs results/*.json
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

const schema = JSON.parse(readFileSync(resolve(dirname(new URL(import.meta.url).pathname), "results.schema.json"), "utf8"));

function typeOf(value) {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  if (Number.isInteger(value)) return "integer";
  return typeof value;
}

function typeMatches(value, expected) {
  const actual = typeOf(value);
  return actual === expected || (expected === "number" && actual === "integer");
}

function validate(value, node, path, errors) {
  if (node.type !== undefined) {
    const types = Array.isArray(node.type) ? node.type : [node.type];
    if (!types.some((t) => typeMatches(value, t))) {
      errors.push(`${path}: expected ${types.join("|")}, got ${typeOf(value)}`);
      return;
    }
  }
  if (node.const !== undefined && value !== node.const) errors.push(`${path}: must equal ${JSON.stringify(node.const)}`);
  if (node.enum && !node.enum.includes(value)) errors.push(`${path}: must be one of ${node.enum.join(", ")}`);
  if (typeof value === "number" && node.minimum !== undefined && value < node.minimum) {
    errors.push(`${path}: must be >= ${node.minimum}`);
  }
  if (typeof value === "string") {
    if (node.pattern && !new RegExp(node.pattern).test(value)) errors.push(`${path}: does not match ${node.pattern}`);
    if (node.format === "date-time" && Number.isNaN(Date.parse(value))) errors.push(`${path}: not a date-time`);
  }
  if (typeOf(value) === "object") {
    for (const key of node.required ?? []) {
      if (!(key in value)) errors.push(`${path}: missing required "${key}"`);
    }
    for (const [key, child] of Object.entries(value)) {
      if (node.properties?.[key]) validate(child, node.properties[key], `${path}.${key}`, errors);
      else if (node.additionalProperties === false) errors.push(`${path}: unexpected property "${key}"`);
      else if (typeof node.additionalProperties === "object") {
        validate(child, node.additionalProperties, `${path}.${key}`, errors);
      }
    }
  }
  if (Array.isArray(value) && node.items) {
    value.forEach((item, index) => validate(item, node.items, `${path}[${index}]`, errors));
  }
}

const files = process.argv.slice(2);
if (files.length === 0) {
  console.error("usage: node validate-result.mjs <result.json>...");
  process.exit(2);
}
let failed = 0;
for (const file of files) {
  const errors = [];
  let data;
  try {
    data = JSON.parse(readFileSync(file, "utf8"));
  } catch (error) {
    errors.push(`unreadable JSON: ${error.message}`);
  }
  if (data !== undefined) {
    validate(data, schema, "$", errors);
    if (data.scenario === "crash_recovery" && !data.crash_recovery) {
      errors.push("$: crash_recovery scenario requires a crash_recovery object");
    }
    if (data.scenario === "throughput" && data.crash_recovery) {
      errors.push("$: throughput scenario must not carry crash_recovery data");
    }
  }
  if (errors.length) {
    failed += 1;
    console.error(`FAIL ${file}\n  ${errors.join("\n  ")}`);
  } else {
    console.log(`ok   ${file}`);
  }
}
process.exit(failed ? 1 : 0);
