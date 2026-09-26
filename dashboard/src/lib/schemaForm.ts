/**
 * Minimal JSON Schema → form model.
 *
 * Supports the subset operators actually author: an object schema whose
 * properties are string / number / integer / boolean (optionally `enum`),
 * with nested objects and arrays edited as raw JSON. Used for:
 *   - block property panels (schemas below mirror orch8-types `*Def` structs)
 *   - sub-sequence `input`, driven by the target sequence's `input_schema`
 *
 * Pure module: imported by node:test.
 */

export type FieldKind = "string" | "number" | "integer" | "boolean" | "enum" | "json";

export interface FormField {
  key: string;
  kind: FieldKind;
  title: string;
  description?: string;
  required: boolean;
  options?: string[];
  placeholder?: string;
}

type Schema = Record<string, unknown>;

function isObj(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

function primaryType(p: Schema): string | undefined {
  const t = p.type;
  if (Array.isArray(t)) return t.find((x) => x !== "null") as string | undefined;
  return typeof t === "string" ? t : undefined;
}

/** Flatten an object schema into form fields (declaration order). */
export function schemaFields(schema: unknown): FormField[] {
  if (!isObj(schema) || !isObj(schema.properties)) return [];
  const required = new Set(Array.isArray(schema.required) ? (schema.required as string[]) : []);
  return Object.entries(schema.properties).map(([key, raw]) => {
    const p: Schema = isObj(raw) ? raw : {};
    const t = primaryType(p);
    let kind: FieldKind;
    if (Array.isArray(p.enum) && p.enum.every((e) => typeof e === "string")) kind = "enum";
    else if (t === "string" || t === "number" || t === "integer" || t === "boolean") kind = t;
    else kind = "json";
    return {
      key,
      kind,
      title: typeof p.title === "string" ? p.title : key,
      description: typeof p.description === "string" ? p.description : undefined,
      required: required.has(key),
      options: kind === "enum" ? (p.enum as string[]) : undefined,
      placeholder: typeof p.examples === "object" && Array.isArray(p.examples) ? String(p.examples[0]) : undefined,
    };
  });
}

export type CoerceResult = { ok: true; value: unknown; remove: boolean } | { ok: false; error: string };

/**
 * Convert the raw text of an input into the value stored in the JSON.
 * An empty optional field removes the key (so optional fields the author
 * never touched do not appear in the definition).
 */
export function coerceField(field: FormField, raw: string | boolean): CoerceResult {
  if (field.kind === "boolean") return { ok: true, value: Boolean(raw), remove: false };
  const text = String(raw);
  if (text.trim() === "") {
    if (field.required && field.kind === "string") return { ok: true, value: "", remove: false };
    return field.required ? { ok: false, error: `${field.title} is required` } : { ok: true, value: undefined, remove: true };
  }
  switch (field.kind) {
    case "string":
    case "enum":
      return { ok: true, value: text, remove: false };
    case "number": {
      const n = Number(text);
      return Number.isFinite(n) ? { ok: true, value: n, remove: false } : { ok: false, error: `${field.title} must be a number` };
    }
    case "integer": {
      const n = Number(text);
      return Number.isInteger(n) ? { ok: true, value: n, remove: false } : { ok: false, error: `${field.title} must be an integer` };
    }
    case "json":
      try {
        return { ok: true, value: JSON.parse(text), remove: false };
      } catch {
        return { ok: false, error: `${field.title} must be valid JSON` };
      }
  }
}

/** Text shown in an input for the current value. */
export function fieldText(field: FormField, value: unknown): string {
  if (value === undefined) return "";
  if (field.kind === "json") return JSON.stringify(value, null, 2);
  if (value === null) return "";
  return String(value);
}

// ─── Block property schemas (mirror orch8-types/src/sequence.rs) ────────────

const WHEN = {
  type: "string",
  title: "when",
  description: "Guard expression; the step is skipped when it evaluates false.",
};

export const BLOCK_SCHEMAS: Record<string, Schema> = {
  step: {
    type: "object",
    required: ["id", "handler"],
    properties: {
      id: { type: "string" },
      handler: { type: "string", description: "Handler name a worker (or built-in) registers." },
      when: WHEN,
      params: { type: "object", description: "Handler parameters (templated)." },
      queue_name: { type: "string" },
      timeout: { type: "integer", description: "Milliseconds." },
      deadline: { type: "integer", description: "Milliseconds; breach triggers on_deadline_breach." },
      fallback_handler: { type: "string" },
      cache_key: { type: "string" },
      rate_limit_key: { type: "string" },
      cancellable: { type: "boolean", description: "Default true." },
      retry: { type: "object", description: "{ max_attempts, initial_backoff, max_backoff, backoff_multiplier }" },
      delay: { type: "object", description: "{ duration, business_days_only, jitter, ... }" },
      wait_for_input: { type: "object", description: "Human input: { prompt, choices, store_as, timeout }" },
      compensation: { type: "object", description: "{ handler, params }" },
      output_schema: { type: "object" },
    },
  },
  parallel: { type: "object", required: ["id"], properties: { id: { type: "string" } } },
  race: {
    type: "object",
    required: ["id"],
    properties: {
      id: { type: "string" },
      semantics: { type: "string", enum: ["first_to_resolve", "first_to_succeed"] },
    },
  },
  loop: {
    type: "object",
    required: ["id", "condition"],
    properties: {
      id: { type: "string" },
      condition: { type: "string", description: "Loop continues while this is truthy." },
      max_iterations: { type: "integer", description: "Default 1000." },
      break_on: { type: "string" },
      continue_on_error: { type: "boolean" },
      poll_interval: { type: "integer", description: "Seconds between condition polls." },
      retain_iterations: { type: "integer" },
    },
  },
  for_each: {
    type: "object",
    required: ["id", "collection"],
    properties: {
      id: { type: "string" },
      collection: { type: "string", description: "Path to the array, e.g. data.items." },
      item_var: { type: "string", description: "Default item." },
      max_iterations: { type: "integer" },
      retain_iterations: { type: "integer" },
    },
  },
  router: { type: "object", required: ["id"], properties: { id: { type: "string" } } },
  try_catch: { type: "object", required: ["id"], properties: { id: { type: "string" } } },
  sub_sequence: {
    type: "object",
    required: ["id", "sequence_name"],
    properties: {
      id: { type: "string" },
      sequence_name: { type: "string" },
      version: { type: "integer", description: "Omit for the latest non-deprecated version." },
    },
  },
  ab_split: { type: "object", required: ["id"], properties: { id: { type: "string" } } },
  cancellation_scope: { type: "object", required: ["id"], properties: { id: { type: "string" } } },
  saga: { type: "object", required: ["id"], properties: { id: { type: "string" } } },
};
