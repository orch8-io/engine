/**
 * Shared types for the E2E test client.
 *
 * These are hand-written and intentionally loose — they type the fields
 * tests actually read (`id`, `state`, common query shapes) and leave the
 * rest open via index signatures. Fully-typed request/response shapes
 * will be generated from the server's OpenAPI spec and swapped in here
 * later, so the call sites in `client.ts` stay untouched.
 *
 * Design rules:
 *   - Request bodies: fully typed. Typos in payload keys are the
 *     single biggest class of bug this file exists to prevent.
 *   - Response bodies: type commonly-read fields, allow extras via
 *     `[k: string]: unknown` so tests can `instance.foo` without casts
 *     while still catching misspelled keys on hot paths.
 *   - State/enum-like fields: string literal unions. Tests compare
 *     against them with `===`, so the typechecker catches bad values.
 */

export type InstanceState =
  | "scheduled"
  | "running"
  | "waiting"
  | "paused"
  | "completed"
  | "failed"
  | "cancelled";

export type WorkerTaskState =
  | "pending"
  | "claimed"
  | "completed"
  | "failed"
  | "dead";

export type Json =
  | string
  | number
  | boolean
  | null
  | Json[]
  | { [k: string]: Json };

/** Block definitions — structural, validated server-side. */
export interface StepBlock {
  type: "step";
  id: string;
  handler: string;
  params?: Record<string, unknown>;
  [k: string]: unknown;
}

export type Block = StepBlock | ({ type: string; id: string } & Record<string, unknown>);

export interface SequenceDef {
  id: string;
  tenant_id: string;
  namespace: string;
  name: string;
  version: number;
  blocks: Block[];
  created_at: string;
  [k: string]: unknown;
}

export interface CreateInstanceRequest {
  sequence_id: string;
  tenant_id: string;
  namespace: string;
  context?: Record<string, unknown>;
  concurrency_key?: string;
  max_concurrency?: number;
  idempotency_key?: string;
  [k: string]: unknown;
}

export interface Instance {
  id: string;
  state: InstanceState;
  sequence_id: string;
  tenant_id: string;
  namespace: string;
  context?: Record<string, unknown>;
  next_fire_at?: string | null;
  [k: string]: unknown;
}

export interface CreateInstanceResponse {
  id: string;
  [k: string]: unknown;
}

export interface BlockOutput {
  block_id: string;
  output: unknown;
  [k: string]: unknown;
}

export interface WorkerTask {
  id: string;
  handler_name: string;
  worker_id?: string;
  state: WorkerTaskState;
  block_id: string;
  params: Record<string, unknown>;
  [k: string]: unknown;
}

export interface ListInstancesQuery {
  tenant_id?: string;
  namespace?: string;
  state?: InstanceState;
  limit?: number;
  [k: string]: unknown;
}

/** Options for `waitForState` / `waitForReady`. */
export interface WaitOptions {
  timeoutMs?: number;
  intervalMs?: number;
}
