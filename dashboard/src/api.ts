const DEFAULT_API_URL = import.meta.env.VITE_ORCH8_API_URL || "http://localhost:8080";

function getApiUrl(): string {
  return localStorage.getItem("orch8_api_url") || DEFAULT_API_URL;
}

function getApiKey(): string | null {
  return localStorage.getItem("orch8_api_key");
}

export function setApiUrl(url: string) {
  localStorage.setItem("orch8_api_url", url);
}

export function setApiKey(key: string) {
  localStorage.setItem("orch8_api_key", key);
}

export function clearApiKey() {
  localStorage.removeItem("orch8_api_key");
}

async function request<T>(path: string, params?: Record<string, string | undefined>): Promise<T> {
  const url = new URL(path, getApiUrl());
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v) url.searchParams.set(k, v);
    }
  }

  const headers: Record<string, string> = { "Content-Type": "application/json" };
  const apiKey = getApiKey();
  if (apiKey) headers["X-API-Key"] = apiKey;

  const res = await fetch(url.toString(), { headers });

  if (res.status === 401) {
    throw new AuthError();
  }
  if (!res.ok) {
    throw new ApiRequestError(res.status, await res.text());
  }
  return res.json();
}

export class AuthError extends Error {
  constructor() {
    super("Unauthorized");
    this.name = "AuthError";
  }
}

export class ApiRequestError extends Error {
  status: number;
  body: string;
  constructor(status: number, body: string) {
    super(`API error ${status}`);
    this.name = "ApiRequestError";
    this.status = status;
    this.body = body;
  }
}

export interface WorkerTask {
  id: string;
  instance_id: string;
  block_id: string;
  handler_name: string;
  queue_name: string | null;
  params: Record<string, unknown>;
  context: Record<string, unknown>;
  attempt: number;
  timeout_ms: number | null;
  state: "pending" | "claimed" | "completed" | "failed";
  worker_id: string | null;
  claimed_at: string | null;
  heartbeat_at: string | null;
  completed_at: string | null;
  output: Record<string, unknown> | null;
  error_message: string | null;
  error_retryable: boolean | null;
  created_at: string;
}

export interface WorkerTaskStats {
  by_state: Record<string, number>;
  by_handler: Record<string, Record<string, number>>;
  active_workers: string[];
}

export interface ListTasksParams {
  state?: string;
  handler_name?: string;
  worker_id?: string;
  queue_name?: string;
  limit?: string;
  offset?: string;
}

export function listWorkerTasks(params?: ListTasksParams): Promise<WorkerTask[]> {
  return request("/workers/tasks", params as Record<string, string | undefined>);
}

export function getWorkerTaskStats(): Promise<WorkerTaskStats> {
  return request("/workers/tasks/stats");
}

export function checkHealth(): Promise<void> {
  return request("/health/live");
}

// ─── Instances ───────────────────────────────────────────────────────────────

export type InstanceState =
  | "scheduled"
  | "running"
  | "waiting"
  | "paused"
  | "completed"
  | "failed"
  | "cancelled";

export type NodeState =
  | "pending"
  | "running"
  | "waiting"
  | "completed"
  | "failed"
  | "cancelled"
  | "skipped";

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
  | "cancellation_scope";

export interface TaskInstance {
  id: string;
  sequence_id: string;
  tenant_id: string;
  namespace: string;
  state: InstanceState;
  next_fire_at: string | null;
  priority: number;
  timezone: string;
  metadata: Record<string, unknown>;
  context: Record<string, unknown>;
  concurrency_key?: string;
  max_concurrency?: number;
  idempotency_key?: string;
  session_id?: string;
  parent_instance_id?: string;
  created_at: string;
  updated_at: string;
}

export interface ExecutionNode {
  id: string;
  instance_id: string;
  block_id: string;
  parent_id: string | null;
  block_type: BlockType;
  branch_index: number | null;
  state: NodeState;
  started_at: string | null;
  completed_at: string | null;
}

export interface BlockOutput {
  instance_id: string;
  block_id: string;
  output: unknown;
  created_at: string;
}

export interface ListInstancesParams {
  tenant_id?: string;
  namespace?: string;
  sequence_id?: string;
  state?: string;
  limit?: string;
  offset?: string;
}

export function listInstances(params?: ListInstancesParams): Promise<TaskInstance[]> {
  return request("/instances", params as Record<string, string | undefined>);
}

export function getInstance(id: string): Promise<TaskInstance> {
  return request(`/instances/${encodeURIComponent(id)}`);
}

export function getExecutionTree(id: string): Promise<ExecutionNode[]> {
  return request(`/instances/${encodeURIComponent(id)}/tree`);
}

export function getInstanceOutputs(id: string): Promise<BlockOutput[]> {
  return request(`/instances/${encodeURIComponent(id)}/outputs`);
}

export type SignalType = "pause" | "resume" | "cancel" | "update_context" | { Custom: string };

export async function sendSignal(
  instanceId: string,
  signal_type: SignalType,
  payload: unknown = {},
): Promise<{ signal_id: string }> {
  return mutate(`/instances/${encodeURIComponent(instanceId)}/signals`, "POST", {
    signal_type,
    payload,
  });
}

export async function retryInstance(id: string): Promise<{ id: string; state: string }> {
  return mutate(`/instances/${encodeURIComponent(id)}/retry`, "POST");
}

// ─── Sequences ───────────────────────────────────────────────────────────────

export interface SequenceDefinition {
  id: string;
  tenant_id: string;
  namespace: string;
  name: string;
  version: number;
  deprecated: boolean;
  blocks: unknown[];
  interceptors?: unknown;
  created_at: string;
}

export function getSequence(id: string): Promise<SequenceDefinition> {
  return request(`/sequences/${encodeURIComponent(id)}`);
}

export function listSequenceVersions(params: {
  tenant_id: string;
  namespace: string;
  name: string;
}): Promise<SequenceDefinition[]> {
  return request("/sequences/versions", params);
}

// ─── Mutations (non-GET) ─────────────────────────────────────────────────────

async function mutate<T>(path: string, method: string, body?: unknown): Promise<T> {
  const url = new URL(path, getApiUrl());
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  const apiKey = getApiKey();
  if (apiKey) headers["X-API-Key"] = apiKey;

  const res = await fetch(url.toString(), {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  if (res.status === 401) throw new AuthError();
  if (!res.ok) throw new ApiRequestError(res.status, await res.text());

  const text = await res.text();
  return (text ? JSON.parse(text) : null) as T;
}

// ─── SSE streaming ───────────────────────────────────────────────────────────

export type StreamEvent =
  | { kind: "state"; data: { state: InstanceState; updated_at: string } }
  | { kind: "output"; data: { block_id: string; output: unknown; created_at: string } }
  | { kind: "done"; data: { state: InstanceState } }
  | { kind: "error"; data: { message: string } };

/**
 * Subscribe to an instance's SSE stream. Returns a disposer.
 * Note: EventSource does not support custom headers — API key is passed via query param
 * if configured. Use only with orch8 deployments that accept `api_key` query auth.
 */
export function streamInstance(
  instanceId: string,
  onEvent: (e: StreamEvent) => void,
  opts: { pollMs?: number } = {},
): () => void {
  const url = new URL(
    `/instances/${encodeURIComponent(instanceId)}/stream`,
    getApiUrl(),
  );
  if (opts.pollMs) url.searchParams.set("poll_ms", String(opts.pollMs));
  const apiKey = getApiKey();
  if (apiKey) url.searchParams.set("api_key", apiKey);

  const es = new EventSource(url.toString());

  const handlers: Array<[string, (ev: MessageEvent) => void]> = [
    ["state", (ev) => onEvent({ kind: "state", data: JSON.parse(ev.data) })],
    ["output", (ev) => onEvent({ kind: "output", data: JSON.parse(ev.data) })],
    ["done", (ev) => onEvent({ kind: "done", data: JSON.parse(ev.data) })],
    ["error", (ev) => onEvent({ kind: "error", data: JSON.parse(ev.data) })],
  ];
  for (const [name, fn] of handlers) es.addEventListener(name, fn);

  return () => {
    for (const [name, fn] of handlers) es.removeEventListener(name, fn);
    es.close();
  };
}
