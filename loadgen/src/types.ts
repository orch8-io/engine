/**
 * Request/response shapes for the orch8 engine REST API.
 *
 * Hand-written and intentionally narrow: we type what we send (request
 * bodies) and the specific fields we read off responses. Everything else
 * is allowed through via index signatures. When the engine ships
 * OpenAPI-generated types, swap this file out.
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

export type Priority = "Low" | "Normal" | "High" | "Critical";

export interface RetryPolicy {
  max_attempts: number;
  initial_backoff: number;
  max_backoff: number;
  backoff_multiplier?: number;
}

export interface StepBlock {
  type: "step";
  id: string;
  handler: string;
  params?: Record<string, unknown>;
  retry?: RetryPolicy;
  timeout?: number;
  queue_name?: string;
  deadline?: number;
  [k: string]: unknown;
}

export type Block =
  | StepBlock
  | ({ type: string; id: string } & Record<string, unknown>);

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
  priority?: Priority;
  context?: Record<string, unknown>;
  [k: string]: unknown;
}

export interface CreateInstanceResponse {
  id: string;
  [k: string]: unknown;
}

export interface Instance {
  id: string;
  state: InstanceState;
  sequence_id: string;
  tenant_id: string;
  namespace: string;
  priority?: Priority;
  context?: Record<string, unknown>;
  created_at?: string;
  [k: string]: unknown;
}

export interface WorkerTask {
  id: string;
  handler_name: string;
  worker_id?: string;
  state: WorkerTaskState;
  block_id: string;
  instance_id: string;
  params: Record<string, unknown>;
  attempt?: number;
  [k: string]: unknown;
}

export interface ListInstancesQuery {
  tenant_id?: string;
  namespace?: string;
  state?: InstanceState;
  limit?: number;
  offset?: number;
  [k: string]: unknown;
}
