/**
 * Sequence editing model for the visual editor.
 *
 * The model IS the sequence JSON: the editor never converts to a separate
 * typed tree. Every edit is a copy-on-write update along a key path, so any
 * field the dashboard does not know about (new engine fields, `$schema`,
 * vendor extensions) survives a visual edit untouched. The JSON tab stays
 * authoritative; the visual tab is a projection of it.
 *
 * Block shapes mirror `orch8-types/src/sequence.rs` (`BlockDefinition`,
 * internally tagged by `type`).
 *
 * Pure module: imported by node:test.
 */

export type Json = null | boolean | number | string | Json[] | { [k: string]: Json };
export type JsonObject = { [k: string]: Json };
export type Path = Array<string | number>;

export type BlockType =
  | "step"
  | "parallel"
  | "race"
  | "loop"
  | "for_each"
  | "router"
  | "try_catch"
  | "sub_sequence"
  | "ab_split"
  | "cancellation_scope"
  | "saga";

export const BLOCK_TYPES: Array<{ type: BlockType; label: string; hint: string }> = [
  { type: "step", label: "Step", hint: "Dispatch one handler" },
  { type: "parallel", label: "Parallel", hint: "Run branches concurrently, wait for all" },
  { type: "race", label: "Race", hint: "Run branches concurrently, first one wins" },
  { type: "try_catch", label: "Try / catch", hint: "Error handling with optional finally" },
  { type: "loop", label: "Loop", hint: "Repeat a body while a condition holds" },
  { type: "for_each", label: "For each", hint: "Run a body per item of a collection" },
  { type: "router", label: "Router", hint: "First matching route wins, else default" },
  { type: "sub_sequence", label: "Sub-sequence", hint: "Invoke another sequence as a child" },
  { type: "cancellation_scope", label: "Cancellation scope", hint: "Children ignore external cancel" },
  { type: "ab_split", label: "A/B split", hint: "Weighted, sticky variant choice" },
  { type: "saga", label: "Saga", hint: "Steps with compensations, rolled back LIFO" },
];

export function isObject(v: unknown): v is JsonObject {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

export function blockType(block: unknown): string {
  return isObject(block) && typeof block.type === "string" ? block.type : "unknown";
}

export function blockId(block: unknown): string {
  return isObject(block) && typeof block.id === "string" ? block.id : "";
}

// ─── Path utilities (copy-on-write) ─────────────────────────────────────────

export function getAt(root: unknown, path: Path): unknown {
  let cur: unknown = root;
  for (const key of path) {
    if (cur === null || typeof cur !== "object") return undefined;
    cur = (cur as Record<string | number, unknown>)[key];
  }
  return cur;
}

/** Return a copy of `root` with `value` at `path`; siblings are shared, not cloned. */
export function setAt<T>(root: T, path: Path, value: unknown): T {
  if (path.length === 0) return value as T;
  const [head, ...rest] = path;
  if (Array.isArray(root)) {
    const copy = root.slice();
    copy[head as number] = setAt(copy[head as number], rest, value);
    return copy as T;
  }
  const base = isObject(root) ? root : {};
  return { ...base, [head as string]: setAt((base as JsonObject)[head as string], rest, value) } as T;
}

/** Return a copy of `root` with the object key or array element at `path` removed. */
export function removeAt<T>(root: T, path: Path): T {
  if (path.length === 0) return root;
  const parentPath = path.slice(0, -1);
  const last = path[path.length - 1]!;
  const parent = getAt(root, parentPath);
  if (Array.isArray(parent)) {
    const copy = parent.slice();
    copy.splice(last as number, 1);
    return setAt(root, parentPath, copy);
  }
  if (isObject(parent)) {
    const copy = { ...parent };
    delete copy[last as string];
    return setAt(root, parentPath, copy);
  }
  return root;
}

// ─── Block slots (child containers) ─────────────────────────────────────────

/**
 * A child container of a composite block. `path` is relative to the block.
 * `list` slots hold an array of blocks; `single` slots hold one block
 * (saga action / compensation).
 */
export interface Slot {
  label: string;
  path: Path;
  kind: "list" | "single";
  optional?: boolean;
  /** Present when the slot itself can be deleted (branch, route, variant, saga step). */
  removePath?: Path;
}

function arr(v: unknown): unknown[] {
  return Array.isArray(v) ? v : [];
}

export function blockSlots(block: unknown): Slot[] {
  if (!isObject(block)) return [];
  switch (block.type) {
    case "parallel":
    case "race":
      return arr(block.branches).map((_, i) => ({
        label: `branch ${i + 1}`,
        path: ["branches", i],
        kind: "list" as const,
        removePath: ["branches", i],
      }));
    case "loop":
    case "for_each":
      return [{ label: "body", path: ["body"], kind: "list" }];
    case "try_catch": {
      const slots: Slot[] = [
        { label: "try", path: ["try_block"], kind: "list" },
        { label: "catch", path: ["catch_block"], kind: "list" },
      ];
      if (Array.isArray(block.finally_block)) {
        slots.push({ label: "finally", path: ["finally_block"], kind: "list", optional: true, removePath: ["finally_block"] });
      }
      return slots;
    }
    case "router": {
      const slots: Slot[] = arr(block.routes).map((r, i) => ({
        label: `when ${isObject(r) && typeof r.condition === "string" ? r.condition : "?"}`,
        path: ["routes", i, "blocks"],
        kind: "list" as const,
        removePath: ["routes", i],
      }));
      if (Array.isArray(block.default)) {
        slots.push({ label: "default", path: ["default"], kind: "list", optional: true, removePath: ["default"] });
      }
      return slots;
    }
    case "ab_split":
      return arr(block.variants).map((v, i) => ({
        label: isObject(v)
          ? `variant ${String(v.name ?? i)} (weight ${String(v.weight ?? "?")})`
          : `variant ${i}`,
        path: ["variants", i, "blocks"],
        kind: "list" as const,
        removePath: ["variants", i],
      }));
    case "cancellation_scope":
      return [{ label: "blocks", path: ["blocks"], kind: "list" }];
    case "saga": {
      const slots: Slot[] = [];
      arr(block.steps).forEach((s, i) => {
        const sid = isObject(s) && typeof s.id === "string" ? s.id : String(i);
        slots.push({ label: `saga step ${sid} · action`, path: ["steps", i, "action"], kind: "single", removePath: ["steps", i] });
        if (isObject(s) && isObject(s.compensation)) {
          slots.push({
            label: `saga step ${sid} · compensation`,
            path: ["steps", i, "compensation"],
            kind: "single",
            optional: true,
            removePath: ["steps", i, "compensation"],
          });
        }
      });
      return slots;
    }
    default:
      return [];
  }
}

/** Structural additions a composite supports ("add branch", "add route", ...). */
export interface SlotAddition {
  label: string;
  /** Path relative to the block where `value` is appended (array) or set (key). */
  path: Path;
  mode: "append" | "set";
  make: (ids: Set<string>) => Json;
}

export function slotAdditions(block: unknown): SlotAddition[] {
  if (!isObject(block)) return [];
  switch (block.type) {
    case "parallel":
    case "race":
      return [{ label: "Add branch", path: ["branches"], mode: "append", make: () => [] }];
    case "try_catch":
      return Array.isArray(block.finally_block)
        ? []
        : [{ label: "Add finally", path: ["finally_block"], mode: "set", make: () => [] }];
    case "router": {
      const out: SlotAddition[] = [
        { label: "Add route", path: ["routes"], mode: "append", make: () => ({ condition: "true", blocks: [] }) },
      ];
      if (!Array.isArray(block.default)) {
        out.push({ label: "Add default", path: ["default"], mode: "set", make: () => [] });
      }
      return out;
    }
    case "ab_split":
      return [
        {
          label: "Add variant",
          path: ["variants"],
          mode: "append",
          make: () => ({ name: `variant_${arr(block.variants).length + 1}`, weight: 50, blocks: [] }),
        },
      ];
    case "saga":
      return [
        {
          label: "Add saga step",
          path: ["steps"],
          mode: "append",
          make: (ids) => {
            const sid = uniqueId("saga_step", ids);
            ids.add(sid);
            return { id: sid, action: newBlock("step", ids) };
          },
        },
      ];
    default:
      return [];
  }
}

// ─── Walking ────────────────────────────────────────────────────────────────

export interface WalkedBlock {
  path: Path;
  block: JsonObject;
  depth: number;
}

/** Every block (depth-first, document order) under the root-level block lists. */
export function walkBlocks(root: unknown): WalkedBlock[] {
  const out: WalkedBlock[] = [];
  const visit = (block: unknown, path: Path, depth: number) => {
    if (!isObject(block)) return;
    out.push({ path, block, depth });
    for (const slot of blockSlots(block)) {
      const slotPath = [...path, ...slot.path];
      const content = getAt(root, slotPath);
      if (slot.kind === "single") visit(content, slotPath, depth + 1);
      else arr(content).forEach((b, i) => visit(b, [...slotPath, i], depth + 1));
    }
  };
  for (const key of ROOT_LISTS) {
    arr(getAt(root, [key])).forEach((b, i) => visit(b, [key, i], 0));
  }
  return out;
}

/** Root-level block lists of a sequence definition. */
export const ROOT_LISTS = ["blocks", "on_failure", "on_cancel"] as const;

/**
 * Top-level blocks of `blocks` with every id nested under each (including
 * saga step ids). Fork endpoints accept only top-level block ids.
 */
export function topLevelBlocks(root: unknown): Array<{ id: string; descendants: string[] }> {
  const walked = walkBlocks(root);
  return arr(getAt(root, ["blocks"])).map((b, i) => {
    const prefix = pathKey(["blocks", i]) + "/";
    const descendants: string[] = [];
    for (const w of walked) {
      if (!pathKey(w.path).startsWith(prefix)) continue;
      if (typeof w.block.id === "string") descendants.push(w.block.id);
    }
    if (isObject(b) && b.type === "saga") {
      for (const s of arr(b.steps)) if (isObject(s) && typeof s.id === "string") descendants.push(s.id);
    }
    return { id: blockId(b), descendants };
  });
}

export function collectIds(root: unknown): Set<string> {
  const ids = new Set<string>();
  for (const w of walkBlocks(root)) {
    if (typeof w.block.id === "string") ids.add(w.block.id);
  }
  // Saga step ids share the namespace.
  for (const w of walkBlocks(root)) {
    if (w.block.type === "saga") {
      for (const s of arr(w.block.steps)) if (isObject(s) && typeof s.id === "string") ids.add(s.id);
    }
  }
  return ids;
}

export function uniqueId(base: string, ids: Set<string>): string {
  const clean = base.replace(/[^A-Za-z0-9_-]/g, "_") || "block";
  if (!ids.has(clean)) return clean;
  for (let i = 2; ; i++) {
    const candidate = `${clean}_${i}`;
    if (!ids.has(candidate)) return candidate;
  }
}

/** A fresh block of `type` with a unique id and the engine's required fields. */
export function newBlock(type: BlockType, ids: Set<string>): JsonObject {
  const id = uniqueId(type, ids);
  ids.add(id);
  switch (type) {
    case "step":
      return { type, id, handler: "noop", params: {} };
    case "parallel":
      return { type, id, branches: [[], []] };
    case "race":
      return { type, id, branches: [[], []], semantics: "first_to_resolve" };
    case "loop":
      return { type, id, condition: "false", body: [], max_iterations: 10 };
    case "for_each":
      return { type, id, collection: "data.items", item_var: "item", body: [] };
    case "router":
      return { type, id, routes: [{ condition: "true", blocks: [] }] };
    case "try_catch":
      return { type, id, try_block: [], catch_block: [] };
    case "sub_sequence":
      return { type, id, sequence_name: "", input: {} };
    case "ab_split":
      return {
        type,
        id,
        variants: [
          { name: "control", weight: 50, blocks: [] },
          { name: "variant_a", weight: 50, blocks: [] },
        ],
      };
    case "cancellation_scope":
      return { type, id, blocks: [] };
    case "saga": {
      const sid = uniqueId("saga_step", ids);
      ids.add(sid);
      return { type, id, steps: [{ id: sid, action: newBlock("step", ids) }] };
    }
  }
}

/** Deep copy of a block with every id (blocks and saga steps) made unique. */
export function duplicateBlock(block: JsonObject, ids: Set<string>): JsonObject {
  const copy = JSON.parse(JSON.stringify(block)) as JsonObject;
  const renumber = (b: unknown) => {
    if (!isObject(b)) return;
    if (typeof b.id === "string") {
      b.id = uniqueId(b.id, ids);
      ids.add(b.id);
    }
    if (b.type === "saga") {
      for (const s of arr(b.steps)) {
        if (isObject(s) && typeof s.id === "string") {
          s.id = uniqueId(s.id, ids);
          ids.add(s.id);
        }
      }
    }
    // Walk children through the same slot definitions the editor uses.
    for (const slot of blockSlots(b)) {
      const content = getAt(b, slot.path);
      if (slot.kind === "single") renumber(content);
      else arr(content).forEach(renumber);
    }
  };
  renumber(copy);
  return copy;
}

// ─── List edits ─────────────────────────────────────────────────────────────

export function insertBlock<T>(root: T, listPath: Path, index: number, block: Json): T {
  const list = arr(getAt(root, listPath)).slice();
  list.splice(Math.max(0, Math.min(index, list.length)), 0, block);
  return setAt(root, listPath, list);
}

export function moveBlock<T>(root: T, listPath: Path, from: number, to: number): T {
  const list = arr(getAt(root, listPath)).slice();
  if (from < 0 || from >= list.length || to < 0 || to >= list.length || from === to) return root;
  const [item] = list.splice(from, 1);
  list.splice(to, 0, item);
  return setAt(root, listPath, list);
}

/** Remove a block. Removing a `single` slot's block (saga action) is refused. */
export function removeBlock<T>(root: T, blockPath: Path): T {
  const last = blockPath[blockPath.length - 1];
  if (typeof last !== "number") return root;
  return removeAt(root, blockPath);
}

/** Split a block path into its containing list path and index, when in a list. */
export function listPosition(blockPath: Path): { listPath: Path; index: number } | null {
  const last = blockPath[blockPath.length - 1];
  if (typeof last !== "number") return null;
  return { listPath: blockPath.slice(0, -1), index: last };
}

export function pathKey(path: Path): string {
  return path.join("/");
}

// ─── JSON text round trip ───────────────────────────────────────────────────

export type ParseResult = { ok: true; value: JsonObject } | { ok: false; error: string };

export function parseSequenceText(text: string): ParseResult {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  }
  if (!isObject(value)) return { ok: false, error: "A sequence definition must be a JSON object" };
  return { ok: true, value };
}

export function serializeSequence(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

// ─── Local structural checks (instant feedback; preflight is authoritative) ─

export interface LocalIssue {
  path: Path;
  message: string;
}

const REQUIRED_STRING: Record<string, string[]> = {
  step: ["handler"],
  loop: ["condition"],
  for_each: ["collection"],
  sub_sequence: ["sequence_name"],
};

export function localIssues(root: unknown): LocalIssue[] {
  const issues: LocalIssue[] = [];
  if (!isObject(root)) return [{ path: [], message: "definition must be an object" }];
  for (const key of ["name", "tenant_id", "namespace"]) {
    if (typeof root[key] !== "string" || root[key] === "") {
      issues.push({ path: [key], message: `${key} is required` });
    }
  }
  if (!Array.isArray(root.blocks)) issues.push({ path: ["blocks"], message: "blocks must be an array" });
  const seen = new Map<string, Path>();
  const known = new Set(BLOCK_TYPES.map((b) => b.type as string));
  for (const w of walkBlocks(root)) {
    const id = w.block.id;
    if (typeof id !== "string" || id === "") {
      issues.push({ path: w.path, message: "block is missing an id" });
    } else if (seen.has(id)) {
      issues.push({ path: w.path, message: `duplicate block id "${id}"` });
    } else {
      seen.set(id, w.path);
    }
    const t = blockType(w.block);
    if (!known.has(t)) {
      issues.push({ path: w.path, message: `unknown block type "${t}"` });
      continue;
    }
    for (const f of REQUIRED_STRING[t] ?? []) {
      if (typeof w.block[f] !== "string" || (w.block[f] as string).trim() === "") {
        issues.push({ path: w.path, message: `${t} "${String(id ?? "")}" needs ${f}` });
      }
    }
    if (t === "ab_split") {
      const total = arr(w.block.variants).reduce<number>(
        (s, v) => s + (isObject(v) && typeof v.weight === "number" ? v.weight : 0),
        0,
      );
      if (total <= 0) issues.push({ path: w.path, message: `ab_split "${String(id)}" needs a positive total weight` });
    }
  }
  return issues;
}

// ─── Versioning ─────────────────────────────────────────────────────────────

/**
 * Body for saving an edited definition as a NEW immutable version. Existing
 * versions are never modified (docs/RELEASES.md): the draft gets a fresh id,
 * `version = max(known versions) + 1`, a new `created_at`, and is not
 * deprecated. Every other field — including unknown ones — is kept.
 */
export function prepareNewVersion(
  draft: JsonObject,
  knownVersions: number[],
  newId: string,
  now: Date,
): JsonObject {
  const maxVersion = knownVersions.reduce((m, v) => (Number.isFinite(v) && v > m ? v : m), 0);
  return {
    ...draft,
    id: newId,
    version: maxVersion + 1,
    created_at: now.toISOString(),
    deprecated: false,
  };
}

/**
 * The draft as the preflight endpoint expects it: identity fields the author
 * may have left out are filled with placeholders (preflight never persists).
 */
export function preflightBody(draft: JsonObject, placeholderId: string, now: Date): JsonObject {
  return {
    ...draft,
    id: typeof draft.id === "string" && draft.id ? draft.id : placeholderId,
    version: typeof draft.version === "number" ? draft.version : 1,
    created_at: typeof draft.created_at === "string" ? draft.created_at : now.toISOString(),
  };
}
