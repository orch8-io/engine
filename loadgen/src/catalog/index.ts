/**
 * Catalog of hybrid workflow templates.
 *
 * Each template has:
 *   - `name` — stable name; used on the Sequences dashboard page.
 *   - `handlers` — handler names it dispatches to external workers.
 *     The worker pool advertises for these.
 *   - `buildSequence(rng, tenant, namespace)` — returns a fresh
 *     `SequenceDef`. Called once at bootstrap per tenant/namespace combo.
 *   - `buildContext(rng)` — params/context injected at instance spawn
 *     time so repeated runs of the same sequence vary shape.
 *
 * The catalog is deliberately plain data: no classes, no inheritance. If
 * you want to add a template, add a new file, export a `Template`, and
 * push it to `ALL_TEMPLATES`.
 */

import type { Block, SequenceDef } from "../types.ts";
import type { Rng } from "../lib/rng.ts";

import { orderFulfillment } from "./orderFulfillment.ts";
import { approvalFlow } from "./approvalFlow.ts";
import { dataPipeline } from "./dataPipeline.ts";
import { etaEscalation } from "./etaEscalation.ts";
import { abSplitRollout } from "./abSplitRollout.ts";
import { loopAggregator } from "./loopAggregator.ts";
import { raceFetch } from "./raceFetch.ts";
import { nestedSubsequence } from "./nestedSubsequence.ts";
import { llmChain } from "./llmChain.ts";

export interface Template {
  name: string;
  handlers: readonly string[];
  /** Weight for the producer's weighted-random pick. */
  weight: number;
  /** Build the sequence definition once per tenant+namespace. */
  buildBlocks(rng: Rng): Block[];
  /** Build a fresh context for each instance spawn. */
  buildContext(rng: Rng): Record<string, unknown>;
}

export const ALL_TEMPLATES: readonly Template[] = [
  orderFulfillment,
  approvalFlow,
  dataPipeline,
  etaEscalation,
  abSplitRollout,
  loopAggregator,
  raceFetch,
  nestedSubsequence,
  llmChain,
];

/** Build the full set of sequence definitions for a tenant/namespace. */
export function buildSequencesFor(
  tenantId: string,
  namespace: string,
  rng: Rng,
  templates: readonly Template[],
): SequenceDef[] {
  return templates.map((t) => ({
    id: rng.uuid(),
    tenant_id: tenantId,
    namespace,
    name: t.name,
    version: 1,
    blocks: t.buildBlocks(rng),
    created_at: new Date().toISOString(),
  }));
}

/** Collect every unique handler name across the active catalog. */
export function uniqueHandlers(templates: readonly Template[]): string[] {
  const set = new Set<string>();
  for (const t of templates) for (const h of t.handlers) set.add(h);
  return [...set];
}
