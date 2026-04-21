/**
 * Orch8 test client — thin REST wrapper.
 *
 * Zero runtime deps, uses Node's built-in `fetch`. Types live in
 * `types.ts` and are intentionally loose on responses: tests assert on
 * the fields they need, so exhaustive response typing would be make-work
 * until the OpenAPI-generated types land.
 */

import type {
  BlockOutput,
  Block,
  CreateInstanceRequest,
  CreateInstanceResponse,
  Instance,
  ListInstancesQuery,
  SequenceDef,
  StepBlock,
  WaitOptions,
  WorkerTask,
} from "./types.ts";

// Default base URL honours `ORCH8_E2E_BASE_URL` (full override) and
// `ORCH8_E2E_PORT` (just the port). `run-standalone.ts` uses `ORCH8_E2E_PORT`
// to point each parallel suite at its own server without per-suite edits.
const DEFAULT_BASE: string =
  process.env.ORCH8_E2E_BASE_URL ??
  `http://localhost:${process.env.ORCH8_E2E_PORT ?? "18080"}`;

/** A loose JSON response shape — individual endpoints narrow as needed. */
type ApiResponse = Record<string, unknown>;

/** Translate a plain object into a URLSearchParams, skipping null/undefined. */
function toQuery(query: Record<string, unknown>): string {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(query)) {
    if (v != null) params.set(k, String(v));
  }
  const qs = params.toString();
  return qs ? `?${qs}` : "";
}

export class Orch8Client {
  readonly baseUrl: string;

  constructor(baseUrl: string = DEFAULT_BASE) {
    this.baseUrl = baseUrl;
  }

  // --- Sequences ---

  async createSequence(seq: SequenceDef): Promise<ApiResponse> {
    return this.#post("/sequences", seq);
  }

  async getSequence(id: string): Promise<SequenceDef> {
    return this.#get<SequenceDef>(`/sequences/${id}`);
  }

  async getSequenceByName(
    tenantId: string,
    namespace: string,
    name: string,
    version?: number,
  ): Promise<SequenceDef> {
    const params = new URLSearchParams({ tenant_id: tenantId, namespace, name });
    if (version != null) params.set("version", String(version));
    return this.#get<SequenceDef>(`/sequences/by-name?${params}`);
  }

  // --- Instances ---

  async createInstance(req: CreateInstanceRequest): Promise<CreateInstanceResponse> {
    return this.#post<CreateInstanceResponse>("/instances", req);
  }

  async createInstancesBatch(
    instances: CreateInstanceRequest[],
  ): Promise<ApiResponse> {
    return this.#post("/instances/batch", { instances });
  }

  async getInstance(id: string): Promise<Instance> {
    return this.#get<Instance>(`/instances/${id}`);
  }

  async listInstances(query: ListInstancesQuery = {}): Promise<Instance[]> {
    const res = await this.#get<{ items: Instance[]; has_more: boolean }>(
      `/instances${toQuery(query)}`,
    );
    return res.items;
  }

  async updateState(
    id: string,
    state: string,
    nextFireAt?: string,
  ): Promise<ApiResponse> {
    const body: Record<string, unknown> = { state };
    if (nextFireAt) body.next_fire_at = nextFireAt;
    return this.#patch(`/instances/${id}/state`, body);
  }

  async updateContext(id: string, context: Record<string, unknown>): Promise<ApiResponse> {
    return this.#patch(`/instances/${id}/context`, { context });
  }

  async sendSignal(
    id: string,
    signalType: string,
    payload: Record<string, unknown> = {},
  ): Promise<ApiResponse> {
    return this.#post(`/instances/${id}/signals`, {
      signal_type: signalType,
      payload,
    });
  }

  async getOutputs(id: string): Promise<BlockOutput[]> {
    return this.#get<BlockOutput[]>(`/instances/${id}/outputs`);
  }

  async bulkUpdateState(
    filter: Record<string, unknown>,
    state: string,
  ): Promise<ApiResponse> {
    return this.#patch("/instances/bulk/state", { filter, state });
  }

  async bulkReschedule(
    filter: Record<string, unknown>,
    offsetSecs: number,
  ): Promise<ApiResponse> {
    return this.#patch("/instances/bulk/reschedule", {
      filter,
      offset_secs: offsetSecs,
    });
  }

  async getInstanceTree(id: string): Promise<ApiResponse> {
    return this.#get(`/instances/${id}/tree`);
  }

  async getAuditLog(id: string): Promise<ApiResponse[]> {
    return this.#get<ApiResponse[]>(`/instances/${id}/audit`);
  }

  async pruneCheckpoints(id: string, keep: number): Promise<ApiResponse> {
    return this.#post(`/instances/${id}/checkpoints/prune`, { keep });
  }

  async listCheckpoints(id: string): Promise<ApiResponse[]> {
    return this.#get<ApiResponse[]>(`/instances/${id}/checkpoints`);
  }

  async saveCheckpoint(
    id: string,
    checkpointData: Record<string, unknown>,
  ): Promise<ApiResponse> {
    return this.#post(`/instances/${id}/checkpoints`, {
      checkpoint_data: checkpointData,
    });
  }

  // --- Pools ---

  async createPool(req: Record<string, unknown>): Promise<ApiResponse> {
    return this.#post("/pools", req);
  }

  async listPools(query: Record<string, unknown> = {}): Promise<ApiResponse[]> {
    return this.#get<ApiResponse[]>(`/pools${toQuery(query)}`);
  }

  async getPool(id: string): Promise<ApiResponse> {
    return this.#get(`/pools/${id}`);
  }

  async deletePool(id: string): Promise<Record<string, never>> {
    return this.#delete(`/pools/${id}`);
  }

  async addPoolResource(
    poolId: string,
    req: Record<string, unknown>,
  ): Promise<ApiResponse> {
    return this.#post(`/pools/${poolId}/resources`, req);
  }

  async listPoolResources(poolId: string): Promise<ApiResponse[]> {
    return this.#get<ApiResponse[]>(`/pools/${poolId}/resources`);
  }

  async updatePoolResource(
    poolId: string,
    resourceId: string,
    body: Record<string, unknown>,
  ): Promise<ApiResponse> {
    return this.#put(`/pools/${poolId}/resources/${resourceId}`, body);
  }

  async deletePoolResource(
    poolId: string,
    resourceId: string,
  ): Promise<Record<string, never>> {
    return this.#delete(`/pools/${poolId}/resources/${resourceId}`);
  }

  async listDlq(query: Record<string, unknown> = {}): Promise<Instance[]> {
    return this.#get<Instance[]>(`/instances/dlq${toQuery(query)}`);
  }

  async retryInstance(id: string): Promise<Instance> {
    return this.#post<Instance>(`/instances/${id}/retry`, {});
  }

  async getLatestCheckpoint(id: string): Promise<ApiResponse> {
    return this.#get(`/instances/${id}/checkpoints/latest`);
  }

  // --- Approvals ---

  async listApprovals(query: Record<string, unknown> = {}): Promise<ApiResponse> {
    return this.#get(`/approvals${toQuery(query)}`);
  }

  // --- Workers (additional) ---

  async pollTasksFromQueue(
    queueName: string,
    workerId: string,
    limit: number = 1,
  ): Promise<WorkerTask[]> {
    return this.#post<WorkerTask[]>("/workers/tasks/poll/queue", {
      queue_name: queueName,
      worker_id: workerId,
      limit,
    });
  }

  // --- Cron ---

  async createCron(req: Record<string, unknown>): Promise<ApiResponse> {
    return this.#post("/cron", req);
  }

  async getCron(id: string): Promise<ApiResponse> {
    return this.#get(`/cron/${id}`);
  }

  async listCron(query: Record<string, unknown> = {}): Promise<ApiResponse[]> {
    return this.#get<ApiResponse[]>(`/cron${toQuery(query)}`);
  }

  async updateCron(id: string, body: Record<string, unknown>): Promise<ApiResponse> {
    return this.#put(`/cron/${id}`, body);
  }

  async deleteCron(id: string): Promise<Record<string, never>> {
    return this.#delete(`/cron/${id}`);
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
    output: Record<string, unknown> = {},
  ): Promise<ApiResponse> {
    return this.#post(`/workers/tasks/${taskId}/complete`, {
      worker_id: workerId,
      output,
    });
  }

  async failWorkerTask(
    taskId: string,
    workerId: string,
    message: string,
    retryable: boolean = false,
  ): Promise<ApiResponse> {
    return this.#post(`/workers/tasks/${taskId}/fail`, {
      worker_id: workerId,
      message,
      retryable,
    });
  }

  async heartbeatWorkerTask(taskId: string, workerId: string): Promise<ApiResponse> {
    return this.#post(`/workers/tasks/${taskId}/heartbeat`, {
      worker_id: workerId,
    });
  }

  async listWorkerTasks(
    query: Record<string, unknown> = {},
  ): Promise<WorkerTask[]> {
    return this.#get<WorkerTask[]>(`/workers/tasks${toQuery(query)}`);
  }

  async workerTaskStats(): Promise<ApiResponse> {
    return this.#get("/workers/tasks/stats");
  }

  // --- Triggers ---

  async createTrigger(body: Record<string, unknown>): Promise<ApiResponse> {
    return this.#post("/triggers", body);
  }

  async getTrigger(slug: string): Promise<ApiResponse> {
    return this.#get(`/triggers/${slug}`);
  }

  async listTriggers(query: Record<string, unknown> = {}): Promise<ApiResponse[]> {
    return this.#get<ApiResponse[]>(`/triggers${toQuery(query)}`);
  }

  async deleteTrigger(slug: string): Promise<Record<string, never>> {
    return this.#delete(`/triggers/${slug}`);
  }

  async fireTrigger(
    slug: string,
    body: Record<string, unknown> = {},
    headers: Record<string, string> = {},
  ): Promise<ApiResponse> {
    return this.#rawJson(`/triggers/${slug}/fire`, "POST", body, headers);
  }

  async fireWebhook(
    slug: string,
    body: Record<string, unknown> = {},
    headers: Record<string, string> = {},
  ): Promise<ApiResponse> {
    // Webhooks with secrets require replay-protection headers.
    // Auto-inject them when the caller hasn't supplied them.
    const h = { ...headers };
    if (!h["x-trigger-timestamp"]) {
      h["x-trigger-timestamp"] = String(Math.floor(Date.now() / 1000));
    }
    if (!h["x-trigger-nonce"]) {
      h["x-trigger-nonce"] = crypto.randomUUID();
    }
    return this.#rawJson(`/webhooks/${slug}`, "POST", body, h);
  }

  // --- Credentials ---

  async createCredential(body: Record<string, unknown>): Promise<ApiResponse> {
    return this.#post("/credentials", body);
  }

  async listCredentials(query: Record<string, unknown> = {}): Promise<ApiResponse[]> {
    return this.#get<ApiResponse[]>(`/credentials${toQuery(query)}`);
  }

  async getCredential(id: string): Promise<ApiResponse> {
    return this.#get(`/credentials/${id}`);
  }

  async updateCredential(id: string, body: Record<string, unknown>): Promise<ApiResponse> {
    return this.#patch(`/credentials/${id}`, body);
  }

  async deleteCredential(id: string): Promise<Record<string, never>> {
    return this.#delete(`/credentials/${id}`);
  }

  // --- Plugins ---

  async createPlugin(body: Record<string, unknown>): Promise<ApiResponse> {
    return this.#post("/plugins", body);
  }

  async listPlugins(query: Record<string, unknown> = {}): Promise<ApiResponse[]> {
    return this.#get<ApiResponse[]>(`/plugins${toQuery(query)}`);
  }

  async getPlugin(name: string): Promise<ApiResponse> {
    return this.#get(`/plugins/${encodeURIComponent(name)}`);
  }

  async updatePlugin(name: string, body: Record<string, unknown>): Promise<ApiResponse> {
    return this.#patch(`/plugins/${encodeURIComponent(name)}`, body);
  }

  async deletePlugin(name: string): Promise<Record<string, never>> {
    return this.#delete(`/plugins/${encodeURIComponent(name)}`);
  }

  // --- Cluster ---

  async listClusterNodes(): Promise<ApiResponse[]> {
    return this.#get<ApiResponse[]>("/cluster/nodes");
  }

  async drainClusterNode(id: string): Promise<ApiResponse> {
    return this.#post(`/cluster/nodes/${id}/drain`, {});
  }

  // --- Sequences (additional) ---

  async deleteSequence(id: string): Promise<Record<string, never>> {
    return this.#delete(`/sequences/${id}`);
  }

  // --- Instances (additional) ---

  async injectBlocks(
    id: string,
    blocks: Record<string, unknown>[],
    position?: number,
  ): Promise<ApiResponse> {
    const body: Record<string, unknown> = { blocks };
    if (position != null) body.position = position;
    return this.#post(`/instances/${id}/inject-blocks`, body);
  }

  // --- Circuit Breakers ---

  async listCircuitBreakers(tenantId?: string): Promise<ApiResponse[]> {
    if (tenantId == null) {
      return this.#get<ApiResponse[]>("/circuit-breakers");
    }
    return this.#get<ApiResponse[]>(
      `/tenants/${encodeURIComponent(tenantId)}/circuit-breakers`,
    );
  }

  async getCircuitBreaker(
    handler: string,
    tenantId: string = "test",
  ): Promise<ApiResponse> {
    return this.#get(
      `/tenants/${encodeURIComponent(tenantId)}/circuit-breakers/${encodeURIComponent(handler)}`,
    );
  }

  async resetCircuitBreaker(
    handler: string,
    tenantId: string = "test",
  ): Promise<ApiResponse> {
    return this.#post(
      `/tenants/${encodeURIComponent(tenantId)}/circuit-breakers/${encodeURIComponent(handler)}/reset`,
      {},
    );
  }

  // --- Sessions ---

  async createSession(body: Record<string, unknown>): Promise<ApiResponse> {
    return this.#post("/sessions", body);
  }

  async getSession(id: string): Promise<ApiResponse> {
    return this.#get(`/sessions/${id}`);
  }

  async getSessionByKey(tenantId: string, key: string): Promise<ApiResponse> {
    return this.#get(
      `/sessions/by-key/${encodeURIComponent(tenantId)}/${encodeURIComponent(key)}`,
    );
  }

  async updateSessionData(
    id: string,
    data: Record<string, unknown>,
  ): Promise<ApiResponse> {
    return this.#patch(`/sessions/${id}/data`, { data });
  }

  async updateSessionState(id: string, state: string): Promise<ApiResponse> {
    return this.#patch(`/sessions/${id}/state`, { state });
  }

  async listSessionInstances(id: string): Promise<Instance[]> {
    return this.#get<Instance[]>(`/sessions/${id}/instances`);
  }

  // --- Sequence versioning ---

  async listSequenceVersions(
    tenantId: string,
    namespace: string,
    name: string,
  ): Promise<SequenceDef[]> {
    const params = new URLSearchParams({ tenant_id: tenantId, namespace, name });
    return this.#get<SequenceDef[]>(`/sequences/versions?${params}`);
  }

  async deprecateSequence(id: string): Promise<ApiResponse> {
    return this.#post(`/sequences/${id}/deprecate`, {});
  }

  async migrateInstance(
    instanceId: string,
    targetSequenceId: string,
  ): Promise<ApiResponse> {
    return this.#post("/sequences/migrate-instance", {
      instance_id: instanceId,
      target_sequence_id: targetSequenceId,
    });
  }

  // --- Health ---

  async healthLive(): Promise<{ status: number }> {
    const res = await fetch(`${this.baseUrl}/health/live`);
    if (!res.ok) throw new ApiError(res.status, await res.text(), "/health/live");
    return { status: res.status };
  }

  async healthReady(): Promise<{ status: number }> {
    const res = await fetch(`${this.baseUrl}/health/ready`);
    if (!res.ok) throw new ApiError(res.status, await res.text(), "/health/ready");
    return { status: res.status };
  }

  // --- Metrics ---

  async metrics(): Promise<string> {
    const res = await fetch(`${this.baseUrl}/metrics`);
    return res.text();
  }

  // --- Helpers ---

  /**
   * Poll an instance until it reaches one of the target states.
   * Returns the instance when reached, throws on timeout.
   */
  async waitForState(
    id: string,
    targetStates: string | string[],
    { timeoutMs = 15_000, intervalMs = 50 }: WaitOptions = {},
  ): Promise<Instance> {
    const states = Array.isArray(targetStates) ? targetStates : [targetStates];
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
      const instance = await this.getInstance(id);
      if (states.includes(instance.state)) return instance;
      await sleep(intervalMs);
    }

    const instance = await this.getInstance(id);
    throw new Error(
      `Timeout waiting for instance ${id} to reach [${states}]. Current state: ${instance.state}`,
    );
  }

  /** Wait for server to be healthy. */
  async waitForReady({
    timeoutMs = 10_000,
    intervalMs = 50,
  }: WaitOptions = {}): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      try {
        await this.healthReady();
        return;
      } catch {
        await sleep(intervalMs);
      }
    }
    throw new Error("Server not ready within timeout");
  }

  // --- Internal ---

  async #get<T = ApiResponse>(path: string): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`);
    if (!res.ok) throw new ApiError(res.status, await res.text(), path);
    return (await res.json()) as T;
  }

  async #post<T = ApiResponse>(path: string, body: unknown): Promise<T> {
    return this.#jsonMethod<T>("POST", path, body);
  }

  async #put<T = ApiResponse>(path: string, body: unknown): Promise<T> {
    return this.#jsonMethod<T>("PUT", path, body);
  }

  async #patch<T = ApiResponse>(path: string, body: unknown): Promise<T> {
    return this.#jsonMethod<T>("PATCH", path, body);
  }

  async #delete<T = Record<string, never>>(path: string): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, { method: "DELETE" });
    if (!res.ok) throw new ApiError(res.status, await res.text(), path);
    return {} as T;
  }

  async #jsonMethod<T>(method: string, path: string, body: unknown): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new ApiError(res.status, await res.text(), path);
    const text = await res.text();
    return (text ? JSON.parse(text) : {}) as T;
  }

  /** For endpoints where the caller supplies custom headers. */
  async #rawJson<T = ApiResponse>(
    path: string,
    method: string,
    body: Record<string, unknown>,
    headers: Record<string, string>,
  ): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new ApiError(res.status, await res.text(), path);
    const text = await res.text();
    return (text ? JSON.parse(text) : {}) as T;
  }
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

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** Generate a random UUID (for test IDs). */
export function uuid(): string {
  return crypto.randomUUID();
}

export interface TestSequenceOptions {
  tenantId?: string;
  namespace?: string;
}

/** Build a minimal sequence definition for testing. */
export function testSequence(
  name: string,
  blocks: Block[],
  { tenantId = "test", namespace = "default" }: TestSequenceOptions = {},
): SequenceDef {
  const suffix = uuid().slice(0, 8);
  return {
    id: uuid(),
    tenant_id: tenantId,
    namespace,
    name: `${name}-${suffix}`,
    version: 1,
    blocks,
    created_at: new Date().toISOString(),
  };
}

/** Build a step block definition. */
export function step(
  id: string,
  handler: string,
  params: Record<string, unknown> = {},
  opts: Record<string, unknown> = {},
): StepBlock {
  return {
    type: "step",
    id,
    handler,
    params,
    ...opts,
  };
}

// Re-export types so call sites can `import { Instance, ... } from "./client.ts"`
// without needing to know `types.ts` exists.
export type {
  Block,
  BlockOutput,
  CreateInstanceRequest,
  CreateInstanceResponse,
  Instance,
  InstanceState,
  ListInstancesQuery,
  SequenceDef,
  StepBlock,
  WaitOptions,
  WorkerTask,
  WorkerTaskState,
} from "./types.ts";
