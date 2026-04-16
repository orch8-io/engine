import type {
  Orch8ClientConfig,
  SequenceDefinition,
  TaskInstance,
  StepOutput,
  ExecutionNode,
  Checkpoint,
  CronSchedule,
  TriggerDef,
  PluginDef,
  Session,
  WorkerTask,
  ClusterNode,
  CircuitBreaker,
  AuditEntry,
} from "./types.js";

export class Orch8Error extends Error {
  constructor(
    public readonly status: number,
    public readonly body: unknown,
    public readonly path: string,
  ) {
    super(`Orch8 API error ${status} on ${path}`);
    this.name = "Orch8Error";
  }
}

export class Orch8Client {
  private readonly baseUrl: string;
  private readonly tenantId?: string;
  private readonly namespace?: string;
  private readonly extraHeaders: Record<string, string>;

  constructor(config: Orch8ClientConfig) {
    this.baseUrl = config.baseUrl.replace(/\/$/, "");
    this.tenantId = config.tenantId;
    this.namespace = config.namespace;
    this.extraHeaders = config.headers ?? {};
  }

  // ---------------------------------------------------------------------------
  // Internal HTTP helpers
  // ---------------------------------------------------------------------------

  private async request<T>(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<T> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      ...this.extraHeaders,
    };
    if (this.tenantId) {
      headers["X-Tenant-Id"] = this.tenantId;
    }

    const res = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers,
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });

    if (!res.ok) {
      let resBody: unknown;
      try {
        resBody = await res.json();
      } catch {
        resBody = await res.text().catch(() => null);
      }
      throw new Orch8Error(res.status, resBody, path);
    }

    if (res.status === 204) {
      return undefined as T;
    }

    return (await res.json()) as T;
  }

  private get<T>(path: string): Promise<T> {
    return this.request<T>("GET", path);
  }

  private post<T>(path: string, body?: unknown): Promise<T> {
    return this.request<T>("POST", path, body);
  }

  private patch<T>(path: string, body?: unknown): Promise<T> {
    return this.request<T>("PATCH", path, body);
  }

  private put<T>(path: string, body?: unknown): Promise<T> {
    return this.request<T>("PUT", path, body);
  }

  private del<T>(path: string): Promise<T> {
    return this.request<T>("DELETE", path);
  }

  // ---------------------------------------------------------------------------
  // Sequences
  // ---------------------------------------------------------------------------

  createSequence(body: Record<string, unknown>): Promise<SequenceDefinition> {
    return this.post<SequenceDefinition>("/sequences", body);
  }

  getSequence(id: string): Promise<SequenceDefinition> {
    return this.get<SequenceDefinition>(`/sequences/${id}`);
  }

  getSequenceByName(
    tenantId: string,
    namespace: string,
    name: string,
    version?: number,
  ): Promise<SequenceDefinition> {
    const params = new URLSearchParams({ tenant_id: tenantId, namespace, name });
    if (version !== undefined) params.set("version", String(version));
    return this.get<SequenceDefinition>(`/sequences/by-name?${params}`);
  }

  deprecateSequence(id: string): Promise<void> {
    return this.post<void>(`/sequences/${id}/deprecate`);
  }

  listSequenceVersions(
    tenantId: string,
    namespace: string,
    name: string,
  ): Promise<SequenceDefinition[]> {
    const params = new URLSearchParams({ tenant_id: tenantId, namespace, name });
    return this.get<SequenceDefinition[]>(`/sequences/versions?${params}`);
  }

  // ---------------------------------------------------------------------------
  // Instances
  // ---------------------------------------------------------------------------

  createInstance(body: Record<string, unknown>): Promise<TaskInstance> {
    return this.post<TaskInstance>("/instances", body);
  }

  batchCreateInstances(
    body: Record<string, unknown>[],
  ): Promise<TaskInstance[]> {
    return this.post<TaskInstance[]>("/instances/batch", body);
  }

  getInstance(id: string): Promise<TaskInstance> {
    return this.get<TaskInstance>(`/instances/${id}`);
  }

  listInstances(filter?: Record<string, string>): Promise<TaskInstance[]> {
    const params = filter ? `?${new URLSearchParams(filter)}` : "";
    return this.get<TaskInstance[]>(`/instances${params}`);
  }

  updateInstanceState(
    id: string,
    body: Record<string, unknown>,
  ): Promise<TaskInstance> {
    return this.patch<TaskInstance>(`/instances/${id}/state`, body);
  }

  updateInstanceContext(
    id: string,
    body: Record<string, unknown>,
  ): Promise<TaskInstance> {
    return this.patch<TaskInstance>(`/instances/${id}/context`, body);
  }

  sendSignal(id: string, body: Record<string, unknown>): Promise<void> {
    return this.post<void>(`/instances/${id}/signals`, body);
  }

  getOutputs(id: string): Promise<StepOutput[]> {
    return this.get<StepOutput[]>(`/instances/${id}/outputs`);
  }

  getExecutionTree(id: string): Promise<ExecutionNode[]> {
    return this.get<ExecutionNode[]>(`/instances/${id}/tree`);
  }

  retryInstance(id: string): Promise<TaskInstance> {
    return this.post<TaskInstance>(`/instances/${id}/retry`);
  }

  listCheckpoints(id: string): Promise<Checkpoint[]> {
    return this.get<Checkpoint[]>(`/instances/${id}/checkpoints`);
  }

  saveCheckpoint(
    id: string,
    body: Record<string, unknown>,
  ): Promise<Checkpoint> {
    return this.post<Checkpoint>(`/instances/${id}/checkpoints`, body);
  }

  getLatestCheckpoint(id: string): Promise<Checkpoint> {
    return this.get<Checkpoint>(`/instances/${id}/checkpoints/latest`);
  }

  pruneCheckpoints(
    id: string,
    body?: Record<string, unknown>,
  ): Promise<void> {
    return this.post<void>(`/instances/${id}/checkpoints/prune`, body);
  }

  listAuditLog(id: string): Promise<AuditEntry[]> {
    return this.get<AuditEntry[]>(`/instances/${id}/audit`);
  }

  bulkUpdateState(body: Record<string, unknown>): Promise<unknown> {
    return this.patch("/instances/bulk/state", body);
  }

  bulkReschedule(body: Record<string, unknown>): Promise<unknown> {
    return this.patch("/instances/bulk/reschedule", body);
  }

  listDLQ(filter?: Record<string, string>): Promise<TaskInstance[]> {
    const params = filter ? `?${new URLSearchParams(filter)}` : "";
    return this.get<TaskInstance[]>(`/instances/dlq${params}`);
  }

  // ---------------------------------------------------------------------------
  // Cron
  // ---------------------------------------------------------------------------

  createCron(body: Record<string, unknown>): Promise<CronSchedule> {
    return this.post<CronSchedule>("/cron", body);
  }

  listCron(tenantId?: string): Promise<CronSchedule[]> {
    const params = tenantId
      ? `?${new URLSearchParams({ tenant_id: tenantId })}`
      : "";
    return this.get<CronSchedule[]>(`/cron${params}`);
  }

  getCron(id: string): Promise<CronSchedule> {
    return this.get<CronSchedule>(`/cron/${id}`);
  }

  updateCron(
    id: string,
    body: Record<string, unknown>,
  ): Promise<CronSchedule> {
    return this.put<CronSchedule>(`/cron/${id}`, body);
  }

  deleteCron(id: string): Promise<void> {
    return this.del<void>(`/cron/${id}`);
  }

  // ---------------------------------------------------------------------------
  // Triggers
  // ---------------------------------------------------------------------------

  createTrigger(body: Record<string, unknown>): Promise<TriggerDef> {
    return this.post<TriggerDef>("/triggers", body);
  }

  listTriggers(tenantId?: string): Promise<TriggerDef[]> {
    const params = tenantId
      ? `?${new URLSearchParams({ tenant_id: tenantId })}`
      : "";
    return this.get<TriggerDef[]>(`/triggers${params}`);
  }

  getTrigger(slug: string): Promise<TriggerDef> {
    return this.get<TriggerDef>(`/triggers/${slug}`);
  }

  deleteTrigger(slug: string): Promise<void> {
    return this.del<void>(`/triggers/${slug}`);
  }

  fireTrigger(
    slug: string,
    body?: Record<string, unknown>,
  ): Promise<TaskInstance> {
    return this.post<TaskInstance>(`/triggers/${slug}/fire`, body);
  }

  // ---------------------------------------------------------------------------
  // Plugins
  // ---------------------------------------------------------------------------

  createPlugin(body: Record<string, unknown>): Promise<PluginDef> {
    return this.post<PluginDef>("/plugins", body);
  }

  listPlugins(tenantId?: string): Promise<PluginDef[]> {
    const params = tenantId
      ? `?${new URLSearchParams({ tenant_id: tenantId })}`
      : "";
    return this.get<PluginDef[]>(`/plugins${params}`);
  }

  getPlugin(name: string): Promise<PluginDef> {
    return this.get<PluginDef>(`/plugins/${name}`);
  }

  updatePlugin(
    name: string,
    body: Record<string, unknown>,
  ): Promise<PluginDef> {
    return this.patch<PluginDef>(`/plugins/${name}`, body);
  }

  deletePlugin(name: string): Promise<void> {
    return this.del<void>(`/plugins/${name}`);
  }

  // ---------------------------------------------------------------------------
  // Sessions
  // ---------------------------------------------------------------------------

  createSession(body: Record<string, unknown>): Promise<Session> {
    return this.post<Session>("/sessions", body);
  }

  getSession(id: string): Promise<Session> {
    return this.get<Session>(`/sessions/${id}`);
  }

  getSessionByKey(tenantId: string, key: string): Promise<Session> {
    const params = new URLSearchParams({ tenant_id: tenantId, key });
    return this.get<Session>(`/sessions/by-key?${params}`);
  }

  updateSessionData(
    id: string,
    body: Record<string, unknown>,
  ): Promise<Session> {
    return this.patch<Session>(`/sessions/${id}/data`, body);
  }

  updateSessionState(
    id: string,
    body: Record<string, unknown>,
  ): Promise<Session> {
    return this.patch<Session>(`/sessions/${id}/state`, body);
  }

  listSessionInstances(id: string): Promise<TaskInstance[]> {
    return this.get<TaskInstance[]>(`/sessions/${id}/instances`);
  }

  // ---------------------------------------------------------------------------
  // Workers
  // ---------------------------------------------------------------------------

  pollTasks(body: Record<string, unknown>): Promise<WorkerTask[]> {
    return this.post<WorkerTask[]>("/workers/tasks/poll", body);
  }

  completeTask(
    id: string,
    body: Record<string, unknown>,
  ): Promise<void> {
    return this.post<void>(`/workers/tasks/${id}/complete`, body);
  }

  failTask(id: string, body: Record<string, unknown>): Promise<void> {
    return this.post<void>(`/workers/tasks/${id}/fail`, body);
  }

  heartbeatTask(
    id: string,
    body: Record<string, unknown>,
  ): Promise<void> {
    return this.post<void>(`/workers/tasks/${id}/heartbeat`, body);
  }

  // ---------------------------------------------------------------------------
  // Cluster
  // ---------------------------------------------------------------------------

  listClusterNodes(): Promise<ClusterNode[]> {
    return this.get<ClusterNode[]>("/cluster/nodes");
  }

  drainNode(id: string): Promise<void> {
    return this.post<void>(`/cluster/nodes/${id}/drain`);
  }

  // ---------------------------------------------------------------------------
  // Circuit Breakers
  // ---------------------------------------------------------------------------

  listCircuitBreakers(): Promise<CircuitBreaker[]> {
    return this.get<CircuitBreaker[]>("/circuit-breakers");
  }

  getCircuitBreaker(handler: string): Promise<CircuitBreaker> {
    return this.get<CircuitBreaker>(`/circuit-breakers/${handler}`);
  }

  resetCircuitBreaker(handler: string): Promise<void> {
    return this.post<void>(`/circuit-breakers/${handler}/reset`);
  }

  // ---------------------------------------------------------------------------
  // Health
  // ---------------------------------------------------------------------------

  health(): Promise<unknown> {
    return this.get("/health/ready");
  }
}
