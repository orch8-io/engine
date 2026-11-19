import { blockId, isObject, walkBlocks, type JsonObject, type Path } from "./sequenceModel.ts";

import type { TimelineEntryDto } from "./timeTravel.ts";

const PATH_SEGMENT = /^[A-Za-z0-9_-]+$/;
const EXPRESSION_PATH = /^[A-Za-z0-9_.]+$/;

function schemaPaths(schema: unknown, root: string, paths: Set<string>, depth = 0) {
  if (paths.size >= 200 || depth > 12) return;
  paths.add(root);
  if (!isObject(schema)) return;
  if (isObject(schema.properties)) {
    for (const [key, value] of Object.entries(schema.properties)) {
      if (PATH_SEGMENT.test(key)) schemaPaths(value, `${root}.${key}`, paths, depth + 1);
    }
  }
  if (isObject(schema.items)) schemaPaths(schema.items, `${root}.0`, paths, depth + 1);
}

/** Inputs and outputs of earlier siblings in the current or an enclosing block list. */
export function referencePaths(doc: JsonObject, selected: Path, kind: "template" | "expression" = "template"): string[] {
  const paths = new Set<string>();
  schemaPaths(doc.input_schema, "context.data", paths);
  for (const { block, path } of walkBlocks(doc)) {
    const index = path[path.length - 1];
    const selectedIndex = selected[path.length - 1];
    const isEarlierSibling = typeof index === "number" && typeof selectedIndex === "number" &&
      index < selectedIndex && path.slice(0, -1).every((segment, i) => selected[i] === segment);
    const id = blockId(block);
    if (isEarlierSibling && PATH_SEGMENT.test(id) && ["step", "sub_sequence"].includes(String(block.type))) {
      schemaPaths(block.output_schema, `outputs.${id}`, paths);
    }
  }
  return [...paths].filter((path) => kind === "template" || EXPRESSION_PATH.test(path));
}

function inferValue(value: unknown): JsonObject {
  if (value === null) return { type: "null" };
  if (Array.isArray(value)) {
    const items = [...new Map(value.map((item) => {
      const shape = inferValue(item);
      return [JSON.stringify(shape), shape] as const;
    })).values()];
    return {
      type: "array",
      ...(items.length > 0 && { items: items.length === 1 ? items[0] : { anyOf: items } }),
    };
  }
  if (isObject(value)) {
    return {
      type: "object",
      properties: Object.fromEntries(Object.entries(value).map(([key, field]) => [key, inferValue(field)])),
    };
  }
  if (typeof value === "number") return { type: Number.isInteger(value) ? "integer" : "number" };
  return { type: typeof value };
}

export function draftInputSchema(sample: unknown, required: ReadonlySet<string>): JsonObject {
  if (!isObject(sample)) throw new Error("The sample must be a JSON object.");
  const fields = Object.keys(sample).filter((key) => required.has(key));
  return { ...inferValue(sample), ...(fields.length > 0 && { required: fields }) };
}

export interface SequenceDependency {
  blockId: string;
  name: string;
  version: number | null;
}

export function sequenceDependencies(doc: unknown): SequenceDependency[] {
  return walkBlocks(doc).flatMap(({ block }) => {
    if (block.type !== "sub_sequence" || typeof block.sequence_name !== "string") return [];
    return [{ blockId: blockId(block), name: block.sequence_name, version: typeof block.version === "number" ? block.version : null }];
  });
}

export interface ReplayDivergence {
  instanceId: string;
  recordedState: string;
  replayedState: string;
  added: string[];
  removed: string[];
}

export function replayDivergences(summary: unknown): ReplayDivergence[] {
  if (!isObject(summary) || !Array.isArray(summary.divergences)) return [];
  const strings = (items: unknown) => Array.isArray(items) ? items.filter((item): item is string => typeof item === "string") : [];
  return summary.divergences.flatMap((row) => {
    if (!isObject(row) || typeof row.instance_id !== "string" || typeof row.recorded_state !== "string" ||
      typeof row.replayed_state !== "string") return [];
    return [{
      instanceId: row.instance_id,
      recordedState: row.recorded_state,
      replayedState: row.replayed_state,
      added: strings(row.blocks_added),
      removed: strings(row.blocks_removed),
    }];
  });
}

const SAFE_SENTINELS = new Set(["__in_progress__", "__retry__", "__error__"]);

/** Keep only execution metadata, including only recognized status markers. */
export function safeTimelineEntry(entry: TimelineEntryDto): TimelineEntryDto {
  return {
    block_id: entry.block_id,
    attempt: entry.attempt,
    completed_at: entry.completed_at,
    is_sentinel: entry.is_sentinel,
    output_ref: entry.is_sentinel && SAFE_SENTINELS.has(entry.output_ref ?? "") ? entry.output_ref : undefined,
  };
}
