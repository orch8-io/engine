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
