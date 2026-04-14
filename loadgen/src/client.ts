/**
 * Orch8 HTTP client — loadgen's own copy (zero shared imports with e2e).
 *
 * Intentionally minimal: only the endpoints the loadgen exercises are
 * exposed as methods. Everything else goes through `rawJson` so you can
 * poke endpoints without adding a wrapper each time.
 */

import type {
  CreateInstanceRequest,
  CreateInstanceResponse,
  Instance,
  ListInstancesQuery,
  SequenceDef,
  WorkerTask,
} from "./types.ts";

type Json = Record<string, unknown>;

function toQuery(query: Record<string, unknown>): string {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(query)) {
    if (v != null) params.set(k, String(v));
  }
  const qs = params.toString();
  return qs ? `?${qs}` : "";
}

export class ApiError extends Error {
  readonly status: number;
  readonly body: string;
  readonly path: string;

  constructor(status: number, body: string, path: string) {
    super(`HTTP ${status} on ${path}:\n${body}`);
    this.status = status;
    this.body = body;
    this.path = path;
  }
}

export interface Orch8ClientOptions {
  baseUrl: string;
  /** Per-request timeout in ms. 0 disables. */
  timeoutMs?: number;
}

export class Orch8Client {
  readonly baseUrl: string;
  readonly timeoutMs: number;

  constructor({ baseUrl, timeoutMs = 10_000 }: Orch8ClientOptions) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.timeoutMs = timeoutMs;
  }

  // --- Health ---

  async healthReady(): Promise<boolean> {
    try {
      const res = await this.#raw("/health/ready", "GET");
      return res.ok;
    } catch {
      return false;
    }
  }

  // --- Sequences ---

  async createSequence(seq: SequenceDef): Promise<void> {
    await this.#post("/sequences", seq);
  }

  async getSequenceByName(
    tenantId: string,
    namespace: string,
    name: string,
  ): Promise<SequenceDef | null> {
    const params = new URLSearchParams({ tenant_id: tenantId, namespace, name });
    try {
      return await this.#get<SequenceDef>(`/sequences/by-name?${params}`);
    } catch (err) {
      if (err instanceof ApiError && err.status === 404) return null;
      throw err;
    }
  }

  // --- Instances ---

  async createInstance(req: CreateInstanceRequest): Promise<CreateInstanceResponse> {
    return this.#post<CreateInstanceResponse>("/instances", req);
  }

  async listInstances(q: ListInstancesQuery = {}): Promise<Instance[]> {
    return this.#get<Instance[]>(`/instances${toQuery(q)}`);
  }

  async getInstance(id: string): Promise<Instance> {
    return this.#get<Instance>(`/instances/${id}`);
  }

  async sendSignal(id: string, signalType: string, payload: Json = {}): Promise<void> {
    await this.#post(`/instances/${id}/signals`, {
      signal_type: signalType,
      payload,
    });
  }

  async updateContext(id: string, context: Json): Promise<void> {
    await this.#patch(`/instances/${id}/context`, { context });
  }

  // --- Workers ---

  async pollWorkerTasks(
    handlerName: string,
    workerId: string,
    limit: number = 1,
  ): Promise<WorkerTask[]> {
    return this.#post<WorkerTask[]>("/workers/tasks/poll", {
      handler_name: handlerName,
      worker_id: workerId,
      limit,
    });
  }

  async completeWorkerTask(
    taskId: string,
    workerId: string,
    output: Json = {},
  ): Promise<void> {
    await this.#post(`/workers/tasks/${taskId}/complete`, {
      worker_id: workerId,
      output,
    });
  }

  async failWorkerTask(
    taskId: string,
    workerId: string,
    message: string,
    retryable: boolean,
  ): Promise<void> {
    await this.#post(`/workers/tasks/${taskId}/fail`, {
      worker_id: workerId,
      message,
      retryable,
    });
  }

  async heartbeatWorkerTask(taskId: string, workerId: string): Promise<void> {
    await this.#post(`/workers/tasks/${taskId}/heartbeat`, {
      worker_id: workerId,
    });
  }

  // --- Triggers / Cron ---

  async createCron(body: Json): Promise<Json> {
    return this.#post<Json>("/cron", body);
  }

  async listCron(): Promise<Json[]> {
    return this.#get<Json[]>("/cron");
  }

  // --- Internal ---

  async #get<T>(path: string): Promise<T> {
    const res = await this.#raw(path, "GET");
    if (!res.ok) throw new ApiError(res.status, await res.text(), path);
    return (await res.json()) as T;
  }

  async #post<T>(path: string, body: unknown): Promise<T> {
    return this.#jsonMethod<T>("POST", path, body);
  }

  async #patch<T>(path: string, body: unknown): Promise<T> {
    return this.#jsonMethod<T>("PATCH", path, body);
  }

  async #jsonMethod<T>(method: string, path: string, body: unknown): Promise<T> {
    const res = await this.#raw(path, method, JSON.stringify(body), {
      "Content-Type": "application/json",
    });
    if (!res.ok) throw new ApiError(res.status, await res.text(), path);
    const text = await res.text();
    return (text ? JSON.parse(text) : {}) as T;
  }

  async #raw(
    path: string,
    method: string,
    body?: string,
    headers: Record<string, string> = {},
  ): Promise<Response> {
    const ctrl = new AbortController();
    const t =
      this.timeoutMs > 0
        ? setTimeout(() => ctrl.abort(), this.timeoutMs)
        : null;
    try {
      const init: RequestInit = {
        method,
        headers,
        signal: ctrl.signal,
      };
      if (body !== undefined) init.body = body;
      return await fetch(`${this.baseUrl}${path}`, init);
    } finally {
      if (t) clearTimeout(t);
    }
  }
}
