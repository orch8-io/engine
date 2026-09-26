/**
 * Time-scrubber model over `GET /instances/{id}/timeline`.
 *
 * The engine is snapshot-based: every executed block persists one
 * `block_outputs` row, and the timeline returns them in execution order.
 * Scrubber position `p` means "after the first `p` entries" (0 = just
 * created). Everything here is derived from that ordered list plus the
 * instance's recorded state transitions and saved checkpoints.
 *
 * Pure module: imported by node:test.
 */

export interface TimelineEntryDto {
  block_id: string;
  attempt: number;
  completed_at: string;
  output?: unknown;
  output_ref?: string | null;
  is_sentinel: boolean;
}

export interface TimelineTransitionDto {
  from_state?: string | null;
  to_state?: string | null;
  at: string;
}

export interface CheckpointLike {
  id: string;
  created_at: string;
  checkpoint_data?: unknown;
  context?: unknown;
}

export type ScrubBlockState = "pending" | "completed" | "running" | "retrying" | "failed";

export interface BlockAtPoint {
  block_id: string;
  state: ScrubBlockState;
  attempt: number;
  /** Latest output recorded for this block up to the point (sentinels excluded). */
  output?: unknown;
  outputRef?: string | null;
  at: string;
}

const SENTINEL_STATE: Record<string, ScrubBlockState> = {
  __in_progress__: "running",
  __retry__: "retrying",
  __error__: "failed",
};

/** Clamp a scrubber position into [0, entries.length]. */
export function clampPosition(position: number, count: number): number {
  if (!Number.isFinite(position)) return count;
  return Math.max(0, Math.min(Math.trunc(position), count));
}

/**
 * Per-block state after the first `position` entries. Blocks are listed in
 * the order they first appeared; `knownBlockIds` (e.g. from the sequence
 * definition) are appended as `pending` when they have not run yet.
 */
export function blocksAt(
  entries: TimelineEntryDto[],
  position: number,
  knownBlockIds: string[] = [],
): BlockAtPoint[] {
  const upTo = clampPosition(position, entries.length);
  const byId = new Map<string, BlockAtPoint>();
  for (let i = 0; i < upTo; i++) {
    const e = entries[i]!;
    const prev = byId.get(e.block_id);
    if (e.is_sentinel) {
      byId.set(e.block_id, {
        block_id: e.block_id,
        state: SENTINEL_STATE[e.output_ref ?? ""] ?? "running",
        attempt: e.attempt,
        output: prev?.output,
        outputRef: prev?.outputRef,
        at: e.completed_at,
      });
    } else {
      byId.set(e.block_id, {
        block_id: e.block_id,
        state: "completed",
        attempt: e.attempt,
        output: e.output,
        outputRef: e.output_ref ?? null,
        at: e.completed_at,
      });
    }
  }
  const out = [...byId.values()];
  for (const id of knownBlockIds) {
    if (!byId.has(id)) out.push({ block_id: id, state: "pending", attempt: 0, at: "" });
  }
  return out;
}

/** `{block_id: output}` for every completed block up to the point. */
export function outputsAt(entries: TimelineEntryDto[], position: number): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const b of blocksAt(entries, position)) {
    if (b.state === "completed" || b.output !== undefined) out[b.block_id] = b.output ?? null;
  }
  return out;
}

/** Timestamp of the point: creation time at 0, else the last included entry. */
export function pointTime(entries: TimelineEntryDto[], position: number, createdAt: string): string {
  const p = clampPosition(position, entries.length);
  return p === 0 ? createdAt : entries[p - 1]!.completed_at;
}

function ts(s: string): number {
  const t = Date.parse(s);
  return Number.isNaN(t) ? 0 : t;
}

/** Instance state at a timestamp, from recorded transitions (`null` if unknown). */
export function instanceStateAt(transitions: TimelineTransitionDto[], at: string): string | null {
  let state: string | null = null;
  const t = ts(at);
  for (const tr of transitions) {
    if (ts(tr.at) <= t && tr.to_state) state = tr.to_state;
  }
  return state;
}

/**
 * Context as of a timestamp. The engine does not keep per-step context
 * history, so this is the latest saved checkpoint at or before `at`; at the
 * final position the live context is exact. Returns the source so the UI can
 * say where the snapshot came from.
 */
export function contextAt(
  checkpoints: CheckpointLike[],
  at: string,
  isLatestPoint: boolean,
  currentContext: unknown,
):
  | { source: "current"; context: unknown }
  | { source: "checkpoint"; context: unknown; checkpointId: string; checkpointAt: string }
  | { source: "none" } {
  if (isLatestPoint && currentContext !== undefined) return { source: "current", context: currentContext };
  const t = ts(at);
  let best: CheckpointLike | null = null;
  for (const c of checkpoints) {
    if (ts(c.created_at) <= t && (!best || ts(c.created_at) >= ts(best.created_at))) best = c;
  }
  if (!best) return { source: "none" };
  return {
    source: "checkpoint",
    context: best.context ?? best.checkpoint_data ?? null,
    checkpointId: best.id,
    checkpointAt: best.created_at,
  };
}

/**
 * Map each block id in a sequence definition to its top-level block id.
 * Forks (and fork previews) only accept top-level blocks, so the scrubber
 * forks from the top-level ancestor of the selected entry.
 */
export function topLevelIndex(
  topLevel: Array<{ id: string; descendants: string[] }>,
): Map<string, string> {
  const m = new Map<string, string>();
  for (const t of topLevel) {
    m.set(t.id, t.id);
    for (const d of t.descendants) if (!m.has(d)) m.set(d, t.id);
  }
  return m;
}

/**
 * Default fork point for scrubber position `p`: the top-level block of the
 * entry right AFTER the point (the first thing that has not happened yet),
 * so "fork from here" replays history up to the point and re-executes from
 * there. At the end of the timeline, the top-level block of the last entry.
 */
export function forkBlockForPosition(
  entries: TimelineEntryDto[],
  position: number,
  topOf: Map<string, string>,
  topLevelOrder: string[],
): string | null {
  const p = clampPosition(position, entries.length);
  const next = entries[p] ?? entries[p - 1];
  if (next) {
    const top = topOf.get(next.block_id);
    if (top) return top;
  }
  if (p === 0) return topLevelOrder[0] ?? null;
  return null;
}
