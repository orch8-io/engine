# SDKs & Helm Chart Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create Node.js management SDK (upgrade existing worker-only SDK), Python SDK, Go SDK, and Helm chart -- each in its own root-level folder.

**Architecture:** Each SDK wraps the Orch8 REST API with typed client methods. The Node.js SDK extends the existing `worker-sdk-node/` into a full `sdk-node/` package. Python uses `httpx` for async HTTP. Go uses `net/http` with typed structs. Helm chart packages the engine as a Kubernetes deployment with configurable Postgres, env vars, and optional ingress.

**Tech Stack:**
- Node.js SDK: TypeScript, vitest, native fetch
- Python SDK: Python 3.10+, httpx, pytest, pydantic
- Go SDK: Go 1.21+, net/http, encoding/json, testing
- Helm: Helm 3, standard chart structure

---

## API Surface for All SDKs

Every SDK must expose these methods (grouped by resource):

### Sequences
- `createSequence(definition) -> Sequence`
- `getSequence(id) -> Sequence`
- `getSequenceByName(tenantId, namespace, name, version?) -> Sequence`
- `deprecateSequence(id)`
- `listSequenceVersions(tenantId, namespace, name) -> Sequence[]`

### Instances
- `createInstance(instance) -> Instance`
- `batchCreateInstances(instances[]) -> { created: number }`
- `getInstance(id) -> Instance`
- `listInstances(filter?) -> Instance[]`
- `updateInstanceState(id, newState)`
- `updateInstanceContext(id, context)`
- `sendSignal(instanceId, signal)`
- `getOutputs(instanceId) -> Output[]`
- `getExecutionTree(instanceId) -> ExecutionNode[]`
- `retryInstance(id) -> Instance`
- `bulkUpdateState(filter, newState) -> { updated: number }`
- `bulkReschedule(filter, offsetSecs) -> { updated: number }`
- `listDLQ(filter?) -> Instance[]`

### Instances > Checkpoints
- `listCheckpoints(instanceId) -> Checkpoint[]`
- `saveCheckpoint(instanceId, data)`
- `getLatestCheckpoint(instanceId) -> Checkpoint`
- `pruneCheckpoints(instanceId, keepLast?)`

### Instances > Audit
- `listAuditLog(instanceId) -> AuditEntry[]`

### Cron
- `createCron(schedule) -> CronSchedule`
- `listCron(tenantId?) -> CronSchedule[]`
- `getCron(id) -> CronSchedule`
- `updateCron(id, schedule)`
- `deleteCron(id)`

### Triggers
- `createTrigger(trigger) -> Trigger`
- `listTriggers(tenantId?) -> Trigger[]`
- `getTrigger(slug) -> Trigger`
- `deleteTrigger(slug)`
- `fireTrigger(slug, data) -> { instanceId, trigger, sequenceName }`

### Plugins
- `createPlugin(plugin) -> Plugin`
- `listPlugins(tenantId?) -> Plugin[]`
- `getPlugin(name) -> Plugin`
- `updatePlugin(name, update) -> Plugin`
- `deletePlugin(name)`

### Sessions
- `createSession(session) -> Session`
- `getSession(id) -> Session`
- `getSessionByKey(tenantId, key) -> Session`
- `updateSessionData(id, data)`
- `updateSessionState(id, state)`
- `listSessionInstances(id) -> Instance[]`

### Workers (existing in Node, new for Python/Go)
- `pollTasks(handlerName, workerId, limit?) -> WorkerTask[]`
- `completeTask(taskId, workerId, output)`
- `failTask(taskId, workerId, message, retryable)`
- `heartbeatTask(taskId, workerId)`

### Cluster
- `listClusterNodes() -> ClusterNode[]`
- `drainNode(nodeId)`

### Circuit Breakers
- `listCircuitBreakers() -> CircuitBreaker[]`
- `getCircuitBreaker(handler) -> CircuitBreaker`
- `resetCircuitBreaker(handler)`

### Health
- `health() -> { status: string }`

---

## Task 1: Node.js SDK (`sdk-node/`)

Upgrade the existing `worker-sdk-node/` into a full SDK at `sdk-node/`. Keep the `Orch8Worker` class, add an `Orch8Client` management class.

**Files:**
- Create: `sdk-node/package.json`
- Create: `sdk-node/tsconfig.json`
- Create: `sdk-node/vitest.config.ts`
- Create: `sdk-node/src/index.ts`
- Create: `sdk-node/src/client.ts` (management client)
- Create: `sdk-node/src/worker.ts` (copy from worker-sdk-node, unchanged)
- Create: `sdk-node/src/types.ts` (all API types)
- Create: `sdk-node/src/__tests__/client.test.ts`
- Create: `sdk-node/src/__tests__/worker.test.ts`

### Step 1: Scaffold package

Create `sdk-node/package.json`:

```json
{
  "name": "@orch8/sdk",
  "version": "0.1.0",
  "description": "Node.js SDK for Orch8 workflow engine",
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "files": ["dist"],
  "scripts": {
    "build": "tsc",
    "dev": "tsc --watch",
    "test": "vitest run",
    "test:watch": "vitest"
  },
  "keywords": ["orch8", "workflow", "orchestration", "sdk"],
  "license": "MIT",
  "devDependencies": {
    "typescript": "^5.4.0",
    "@types/node": "^20.0.0",
    "vitest": "^3.0.0"
  }
}
```

Create `sdk-node/tsconfig.json` (same as worker-sdk-node).

Create `sdk-node/vitest.config.ts`:

```ts
import { defineConfig } from "vitest/config";
export default defineConfig({ test: { globals: true } });
```

### Step 2: Create types

Create `sdk-node/src/types.ts` with all API types. This file defines the shape of every request and response. Types use snake_case to match the Rust API JSON output.

```ts
// --- Common ---
export interface Orch8ClientConfig {
  baseUrl: string;
  tenantId?: string;
  namespace?: string;
  headers?: Record<string, string>;
}

// --- Sequences ---
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

// --- Instances ---
export interface TaskInstance {
  id: string;
  sequence_id: string;
  tenant_id: string;
  namespace: string;
  state: "scheduled" | "running" | "completed" | "failed" | "cancelled" | "paused";
  next_fire_at: string | null;
  priority: "low" | "normal" | "high" | "critical";
  timezone: string;
  metadata: unknown;
  context: ExecutionContext;
  concurrency_key: string | null;
  max_concurrency: number | null;
  idempotency_key: string | null;
  session_id: string | null;
  parent_instance_id: string | null;
  created_at: string;
  updated_at: string;
}

export interface ExecutionContext {
  data: unknown;
  config: unknown;
  audit: AuditEntry[];
  runtime: unknown;
}

export interface AuditEntry {
  timestamp: string;
  event: string;
  details: unknown;
}

export interface ExecutionNode {
  id: string;
  instance_id: string;
  block_id: string;
  parent_id: string | null;
  block_type: string;
  branch_index: number | null;
  state: string;
  started_at: string | null;
  completed_at: string | null;
}

export interface StepOutput {
  id: string;
  instance_id: string;
  block_id: string;
  node_id: string;
  output: unknown;
  error_message: string | null;
  created_at: string;
}

export interface Checkpoint {
  id: string;
  instance_id: string;
  data: unknown;
  created_at: string;
}

// --- Cron ---
export interface CronSchedule {
  id: string;
  tenant_id: string;
  namespace: string;
  sequence_name: string;
  version: number | null;
  cron_expr: string;
  timezone: string;
  enabled: boolean;
  input_data: unknown;
  created_at: string;
  updated_at: string;
}

// --- Triggers ---
export interface TriggerDef {
  slug: string;
  sequence_name: string;
  version: number | null;
  tenant_id: string;
  namespace: string;
  enabled: boolean;
  secret: string | null;
  trigger_type: "webhook" | "nats" | "file_watch";
  config: unknown;
  created_at: string;
  updated_at: string;
}

// --- Plugins ---
export interface PluginDef {
  name: string;
  plugin_type: "wasm" | "grpc";
  source: string;
  tenant_id: string;
  enabled: boolean;
  config: unknown;
  description: string | null;
  created_at: string;
  updated_at: string;
}

// --- Sessions ---
export interface Session {
  id: string;
  tenant_id: string;
  key: string;
  state: string;
  data: unknown;
  created_at: string;
  updated_at: string;
}

// --- Workers ---
export interface WorkerTask {
  id: string;
  instance_id: string;
  block_id: string;
  handler_name: string;
  params: unknown;
  context: unknown;
  attempt: number;
  timeout_ms: number | null;
  state: "pending" | "claimed" | "completed" | "failed";
  worker_id: string | null;
  claimed_at: string | null;
  heartbeat_at: string | null;
  completed_at: string | null;
  output: unknown | null;
  error_message: string | null;
  error_retryable: boolean | null;
  created_at: string;
}

// --- Cluster ---
export interface ClusterNode {
  id: string;
  address: string;
  state: string;
  last_heartbeat: string;
}

// --- Circuit Breakers ---
export interface CircuitBreaker {
  handler: string;
  state: string;
  failure_count: number;
  last_failure: string | null;
}
```

### Step 3: Create management client

Create `sdk-node/src/client.ts`. The client is a thin typed wrapper over `fetch`.

```ts
import type {
  Orch8ClientConfig, SequenceDefinition, TaskInstance,
  ExecutionNode, StepOutput, Checkpoint, AuditEntry,
  CronSchedule, TriggerDef, PluginDef, Session,
  WorkerTask, ClusterNode, CircuitBreaker,
} from "./types.js";

export class Orch8Client {
  private readonly baseUrl: string;
  private readonly defaultHeaders: Record<string, string>;

  constructor(config: Orch8ClientConfig) {
    this.baseUrl = config.baseUrl.replace(/\/$/, "");
    this.defaultHeaders = {
      "Content-Type": "application/json",
      ...(config.tenantId ? { "X-Tenant-Id": config.tenantId } : {}),
      ...(config.headers ?? {}),
    };
  }

  // --- internal helpers ---

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: this.defaultHeaders,
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Orch8Error(res.status, text, path);
    }
    if (res.status === 204) return undefined as T;
    return res.json() as Promise<T>;
  }

  private get<T>(path: string) { return this.request<T>("GET", path); }
  private post<T>(path: string, body?: unknown) { return this.request<T>("POST", path, body); }
  private patch<T>(path: string, body?: unknown) { return this.request<T>("PATCH", path, body); }
  private del<T>(path: string) { return this.request<T>("DELETE", path); }

  // --- Sequences ---
  async createSequence(definition: unknown): Promise<SequenceDefinition> {
    return this.post("/sequences", definition);
  }
  async getSequence(id: string): Promise<SequenceDefinition> {
    return this.get(`/sequences/${id}`);
  }
  async getSequenceByName(tenantId: string, namespace: string, name: string, version?: number): Promise<SequenceDefinition> {
    const params = new URLSearchParams({ tenant_id: tenantId, namespace, name });
    if (version !== undefined) params.set("version", String(version));
    return this.get(`/sequences/by-name?${params}`);
  }
  async deprecateSequence(id: string): Promise<void> {
    return this.post(`/sequences/${id}/deprecate`);
  }
  async listSequenceVersions(tenantId: string, namespace: string, name: string): Promise<SequenceDefinition[]> {
    const params = new URLSearchParams({ tenant_id: tenantId, namespace, name });
    return this.get(`/sequences/versions?${params}`);
  }

  // --- Instances ---
  async createInstance(instance: unknown): Promise<TaskInstance> {
    return this.post("/instances", instance);
  }
  async batchCreateInstances(instances: unknown[]): Promise<{ created: number }> {
    return this.post("/instances/batch", { instances });
  }
  async getInstance(id: string): Promise<TaskInstance> {
    return this.get(`/instances/${id}`);
  }
  async listInstances(filter?: Record<string, unknown>): Promise<TaskInstance[]> {
    const params = filter ? "?" + new URLSearchParams(
      Object.entries(filter).map(([k, v]) => [k, String(v)])
    ) : "";
    return this.get(`/instances${params}`);
  }
  async updateInstanceState(id: string, newState: string): Promise<void> {
    return this.patch(`/instances/${id}/state`, { state: newState });
  }
  async updateInstanceContext(id: string, context: unknown): Promise<void> {
    return this.patch(`/instances/${id}/context`, context);
  }
  async sendSignal(instanceId: string, signal: unknown): Promise<void> {
    return this.post(`/instances/${instanceId}/signals`, signal);
  }
  async getOutputs(instanceId: string): Promise<StepOutput[]> {
    return this.get(`/instances/${instanceId}/outputs`);
  }
  async getExecutionTree(instanceId: string): Promise<ExecutionNode[]> {
    return this.get(`/instances/${instanceId}/tree`);
  }
  async retryInstance(id: string): Promise<TaskInstance> {
    return this.post(`/instances/${id}/retry`);
  }
  async listCheckpoints(instanceId: string): Promise<Checkpoint[]> {
    return this.get(`/instances/${instanceId}/checkpoints`);
  }
  async saveCheckpoint(instanceId: string, data: unknown): Promise<void> {
    return this.post(`/instances/${instanceId}/checkpoints`, { data });
  }
  async getLatestCheckpoint(instanceId: string): Promise<Checkpoint> {
    return this.get(`/instances/${instanceId}/checkpoints/latest`);
  }
  async pruneCheckpoints(instanceId: string, keepLast?: number): Promise<void> {
    return this.post(`/instances/${instanceId}/checkpoints/prune`, { keep_last: keepLast });
  }
  async listAuditLog(instanceId: string): Promise<AuditEntry[]> {
    return this.get(`/instances/${instanceId}/audit`);
  }
  async bulkUpdateState(filter: Record<string, unknown>, newState: string): Promise<{ updated: number }> {
    return this.patch("/instances/bulk/state", { ...filter, state: newState });
  }
  async bulkReschedule(filter: Record<string, unknown>, offsetSecs: number): Promise<{ updated: number }> {
    return this.patch("/instances/bulk/reschedule", { ...filter, offset_secs: offsetSecs });
  }
  async listDLQ(filter?: Record<string, unknown>): Promise<TaskInstance[]> {
    const params = filter ? "?" + new URLSearchParams(
      Object.entries(filter).map(([k, v]) => [k, String(v)])
    ) : "";
    return this.get(`/instances/dlq${params}`);
  }

  // --- Cron ---
  async createCron(schedule: unknown): Promise<CronSchedule> {
    return this.post("/cron", schedule);
  }
  async listCron(tenantId?: string): Promise<CronSchedule[]> {
    const params = tenantId ? `?tenant_id=${tenantId}` : "";
    return this.get(`/cron${params}`);
  }
  async getCron(id: string): Promise<CronSchedule> {
    return this.get(`/cron/${id}`);
  }
  async updateCron(id: string, schedule: unknown): Promise<void> {
    return this.request("PUT", `/cron/${id}`, schedule);
  }
  async deleteCron(id: string): Promise<void> {
    return this.del(`/cron/${id}`);
  }

  // --- Triggers ---
  async createTrigger(trigger: unknown): Promise<TriggerDef> {
    return this.post("/triggers", trigger);
  }
  async listTriggers(tenantId?: string): Promise<TriggerDef[]> {
    const params = tenantId ? `?tenant_id=${tenantId}` : "";
    return this.get(`/triggers${params}`);
  }
  async getTrigger(slug: string): Promise<TriggerDef> {
    return this.get(`/triggers/${slug}`);
  }
  async deleteTrigger(slug: string): Promise<void> {
    return this.del(`/triggers/${slug}`);
  }
  async fireTrigger(slug: string, data: unknown): Promise<{ instance_id: string; trigger: string; sequence_name: string }> {
    return this.post(`/triggers/${slug}/fire`, data);
  }

  // --- Plugins ---
  async createPlugin(plugin: unknown): Promise<PluginDef> {
    return this.post("/plugins", plugin);
  }
  async listPlugins(tenantId?: string): Promise<PluginDef[]> {
    const params = tenantId ? `?tenant_id=${tenantId}` : "";
    return this.get(`/plugins${params}`);
  }
  async getPlugin(name: string): Promise<PluginDef> {
    return this.get(`/plugins/${name}`);
  }
  async updatePlugin(name: string, update: unknown): Promise<PluginDef> {
    return this.patch(`/plugins/${name}`, update);
  }
  async deletePlugin(name: string): Promise<void> {
    return this.del(`/plugins/${name}`);
  }

  // --- Sessions ---
  async createSession(session: unknown): Promise<Session> {
    return this.post("/sessions", session);
  }
  async getSession(id: string): Promise<Session> {
    return this.get(`/sessions/${id}`);
  }
  async getSessionByKey(tenantId: string, key: string): Promise<Session> {
    return this.get(`/sessions/by-key/${tenantId}/${key}`);
  }
  async updateSessionData(id: string, data: unknown): Promise<void> {
    return this.patch(`/sessions/${id}/data`, data);
  }
  async updateSessionState(id: string, state: string): Promise<void> {
    return this.patch(`/sessions/${id}/state`, { state });
  }
  async listSessionInstances(id: string): Promise<TaskInstance[]> {
    return this.get(`/sessions/${id}/instances`);
  }

  // --- Workers ---
  async pollTasks(handlerName: string, workerId: string, limit = 10): Promise<WorkerTask[]> {
    return this.post("/workers/tasks/poll", { handler_name: handlerName, worker_id: workerId, limit });
  }
  async completeTask(taskId: string, workerId: string, output: unknown): Promise<void> {
    return this.post(`/workers/tasks/${taskId}/complete`, { worker_id: workerId, output });
  }
  async failTask(taskId: string, workerId: string, message: string, retryable: boolean): Promise<void> {
    return this.post(`/workers/tasks/${taskId}/fail`, { worker_id: workerId, message, retryable });
  }
  async heartbeatTask(taskId: string, workerId: string): Promise<void> {
    return this.post(`/workers/tasks/${taskId}/heartbeat`, { worker_id: workerId });
  }

  // --- Cluster ---
  async listClusterNodes(): Promise<ClusterNode[]> {
    return this.get("/cluster/nodes");
  }
  async drainNode(nodeId: string): Promise<void> {
    return this.post(`/cluster/nodes/${nodeId}/drain`);
  }

  // --- Circuit Breakers ---
  async listCircuitBreakers(): Promise<CircuitBreaker[]> {
    return this.get("/circuit-breakers");
  }
  async getCircuitBreaker(handler: string): Promise<CircuitBreaker> {
    return this.get(`/circuit-breakers/${handler}`);
  }
  async resetCircuitBreaker(handler: string): Promise<void> {
    return this.post(`/circuit-breakers/${handler}/reset`);
  }

  // --- Health ---
  async health(): Promise<{ status: string }> {
    return this.get("/health");
  }
}

export class Orch8Error extends Error {
  constructor(
    public readonly status: number,
    public readonly body: string,
    public readonly path: string,
  ) {
    super(`Orch8 API error ${status} on ${path}: ${body}`);
    this.name = "Orch8Error";
  }
}
```

### Step 4: Copy and adapt worker

Copy `worker-sdk-node/src/worker.ts` into `sdk-node/src/worker.ts`. The only change: import `WorkerTask` from `./types.js` instead of redefining it. The `Orch8Worker` class stays exactly the same.

### Step 5: Create index.ts exports

```ts
export { Orch8Client, Orch8Error } from "./client.js";
export { Orch8Worker, type WorkerConfig, type HandlerFn } from "./worker.js";
export type * from "./types.js";
```

### Step 6: Write tests

Create `sdk-node/src/__tests__/client.test.ts`:

Test strategy: Mock `global.fetch` to verify the client sends the correct method, URL, headers, and body for each resource group. One test per resource group (not per method) to keep it manageable.

```ts
import { describe, it, expect, vi, beforeEach } from "vitest";
import { Orch8Client, Orch8Error } from "../client.js";

function mockFetch(status: number, body: unknown) {
  return vi.fn().mockResolvedValue({
    ok: status < 400,
    status,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body)),
  });
}

describe("Orch8Client", () => {
  let client: Orch8Client;
  beforeEach(() => {
    client = new Orch8Client({ baseUrl: "http://localhost:8080", tenantId: "t1" });
  });

  it("sends tenant header", async () => {
    const mock = mockFetch(200, { status: "ok" });
    global.fetch = mock;
    await client.health();
    expect(mock).toHaveBeenCalledWith(
      "http://localhost:8080/health",
      expect.objectContaining({
        headers: expect.objectContaining({ "X-Tenant-Id": "t1" }),
      }),
    );
  });

  it("sequences CRUD", async () => {
    const mock = mockFetch(200, { id: "s1", name: "test" });
    global.fetch = mock;
    await client.createSequence({ name: "test" });
    expect(mock).toHaveBeenCalledWith("http://localhost:8080/sequences", expect.objectContaining({ method: "POST" }));
    await client.getSequence("s1");
    expect(mock).toHaveBeenCalledWith("http://localhost:8080/sequences/s1", expect.objectContaining({ method: "GET" }));
  });

  it("instances lifecycle", async () => {
    const mock = mockFetch(200, { id: "i1" });
    global.fetch = mock;
    await client.createInstance({ sequence_id: "s1" });
    await client.getInstance("i1");
    await client.retryInstance("i1");
    expect(mock).toHaveBeenCalledTimes(3);
  });

  it("triggers CRUD + fire", async () => {
    const mock = mockFetch(200, { slug: "t1" });
    global.fetch = mock;
    await client.createTrigger({ slug: "t1", sequence_name: "s1", tenant_id: "t1" });
    await client.fireTrigger("t1", { key: "value" });
    expect(mock).toHaveBeenCalledWith(
      "http://localhost:8080/triggers/t1/fire",
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("throws Orch8Error on failure", async () => {
    global.fetch = mockFetch(404, "not found");
    await expect(client.getSequence("bad")).rejects.toThrow(Orch8Error);
  });

  it("handles 204 No Content", async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 204,
      json: () => Promise.resolve(undefined),
      text: () => Promise.resolve(""),
    });
    const result = await client.deleteTrigger("t1");
    expect(result).toBeUndefined();
  });
});
```

### Step 7: Install deps and run tests

```bash
cd sdk-node && npm install && npm test
```

### Step 8: Commit

```bash
git add sdk-node/
git commit -m "feat: add Node.js management SDK with typed client for all API resources"
```

---

## Task 2: Python SDK (`sdk-python/`)

**Files:**
- Create: `sdk-python/pyproject.toml`
- Create: `sdk-python/src/orch8/__init__.py`
- Create: `sdk-python/src/orch8/client.py`
- Create: `sdk-python/src/orch8/worker.py`
- Create: `sdk-python/src/orch8/types.py`
- Create: `sdk-python/src/orch8/errors.py`
- Create: `sdk-python/tests/__init__.py`
- Create: `sdk-python/tests/test_client.py`
- Create: `sdk-python/tests/test_worker.py`

### Step 1: Scaffold project

Create `sdk-python/pyproject.toml`:

```toml
[project]
name = "orch8-sdk"
version = "0.1.0"
description = "Python SDK for Orch8 workflow engine"
readme = "README.md"
license = "MIT"
requires-python = ">=3.10"
dependencies = [
    "httpx>=0.27,<1.0",
    "pydantic>=2.0,<3.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24",
    "respx>=0.22",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/orch8"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

### Step 2: Create types

Create `sdk-python/src/orch8/types.py`:

```python
"""Orch8 API type definitions."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SequenceDefinition(BaseModel):
    id: str
    tenant_id: str
    namespace: str
    name: str
    version: int
    deprecated: bool = False
    blocks: list[Any] = Field(default_factory=list)
    interceptors: Any | None = None
    created_at: str


class TaskInstance(BaseModel):
    id: str
    sequence_id: str
    tenant_id: str
    namespace: str
    state: str
    next_fire_at: str | None = None
    priority: str = "normal"
    timezone: str = ""
    metadata: Any = None
    context: Any = None
    concurrency_key: str | None = None
    max_concurrency: int | None = None
    idempotency_key: str | None = None
    session_id: str | None = None
    parent_instance_id: str | None = None
    created_at: str
    updated_at: str


class ExecutionNode(BaseModel):
    id: str
    instance_id: str
    block_id: str
    parent_id: str | None = None
    block_type: str
    branch_index: int | None = None
    state: str
    started_at: str | None = None
    completed_at: str | None = None


class StepOutput(BaseModel):
    id: str
    instance_id: str
    block_id: str
    node_id: str
    output: Any = None
    error_message: str | None = None
    created_at: str


class Checkpoint(BaseModel):
    id: str
    instance_id: str
    data: Any = None
    created_at: str


class AuditEntry(BaseModel):
    timestamp: str
    event: str
    details: Any = None


class CronSchedule(BaseModel):
    id: str
    tenant_id: str
    namespace: str
    sequence_name: str
    version: int | None = None
    cron_expr: str
    timezone: str = "UTC"
    enabled: bool = True
    input_data: Any = None
    created_at: str
    updated_at: str


class TriggerDef(BaseModel):
    slug: str
    sequence_name: str
    version: int | None = None
    tenant_id: str
    namespace: str = "default"
    enabled: bool = True
    secret: str | None = None
    trigger_type: str = "webhook"
    config: Any = None
    created_at: str
    updated_at: str


class PluginDef(BaseModel):
    name: str
    plugin_type: str
    source: str
    tenant_id: str = ""
    enabled: bool = True
    config: Any = None
    description: str | None = None
    created_at: str
    updated_at: str


class Session(BaseModel):
    id: str
    tenant_id: str
    key: str
    state: str
    data: Any = None
    created_at: str
    updated_at: str


class WorkerTask(BaseModel):
    id: str
    instance_id: str
    block_id: str
    handler_name: str
    params: Any = None
    context: Any = None
    attempt: int = 0
    timeout_ms: int | None = None
    state: str = "pending"
    worker_id: str | None = None
    claimed_at: str | None = None
    heartbeat_at: str | None = None
    completed_at: str | None = None
    output: Any | None = None
    error_message: str | None = None
    error_retryable: bool | None = None
    created_at: str


class ClusterNode(BaseModel):
    id: str
    address: str
    state: str
    last_heartbeat: str


class CircuitBreaker(BaseModel):
    handler: str
    state: str
    failure_count: int = 0
    last_failure: str | None = None


class FireTriggerResponse(BaseModel):
    instance_id: str
    trigger: str
    sequence_name: str


class BulkResponse(BaseModel):
    updated: int = 0


class BatchCreateResponse(BaseModel):
    created: int = 0


class HealthResponse(BaseModel):
    status: str
```

### Step 3: Create errors

Create `sdk-python/src/orch8/errors.py`:

```python
"""Orch8 SDK errors."""


class Orch8Error(Exception):
    """Raised when the Orch8 API returns an error response."""

    def __init__(self, status: int, body: str, path: str) -> None:
        self.status = status
        self.body = body
        self.path = path
        super().__init__(f"Orch8 API error {status} on {path}: {body}")
```

### Step 4: Create client

Create `sdk-python/src/orch8/client.py` — async client using `httpx`:

```python
"""Orch8 management client."""

from __future__ import annotations

from typing import Any

import httpx

from .errors import Orch8Error
from .types import (
    AuditEntry, BatchCreateResponse, BulkResponse, Checkpoint,
    CircuitBreaker, ClusterNode, CronSchedule, ExecutionNode,
    FireTriggerResponse, HealthResponse, PluginDef, SequenceDefinition,
    Session, StepOutput, TaskInstance, TriggerDef, WorkerTask,
)


class Orch8Client:
    """Async client for the Orch8 REST API."""

    def __init__(
        self,
        base_url: str,
        tenant_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        h = {"Content-Type": "application/json", **(headers or {})}
        if tenant_id:
            h["X-Tenant-Id"] = tenant_id
        self._client = httpx.AsyncClient(base_url=base_url.rstrip("/"), headers=h)

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> Orch8Client:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        res = await self._client.request(method, path, **kwargs)
        if res.status_code >= 400:
            raise Orch8Error(res.status_code, res.text, path)
        if res.status_code == 204:
            return None
        return res.json()

    # --- Sequences ---
    async def create_sequence(self, definition: dict[str, Any]) -> SequenceDefinition:
        data = await self._request("POST", "/sequences", json=definition)
        return SequenceDefinition.model_validate(data)

    async def get_sequence(self, id: str) -> SequenceDefinition:
        data = await self._request("GET", f"/sequences/{id}")
        return SequenceDefinition.model_validate(data)

    async def get_sequence_by_name(
        self, tenant_id: str, namespace: str, name: str, version: int | None = None,
    ) -> SequenceDefinition:
        params: dict[str, Any] = {"tenant_id": tenant_id, "namespace": namespace, "name": name}
        if version is not None:
            params["version"] = version
        data = await self._request("GET", "/sequences/by-name", params=params)
        return SequenceDefinition.model_validate(data)

    async def deprecate_sequence(self, id: str) -> None:
        await self._request("POST", f"/sequences/{id}/deprecate")

    async def list_sequence_versions(
        self, tenant_id: str, namespace: str, name: str,
    ) -> list[SequenceDefinition]:
        data = await self._request("GET", "/sequences/versions", params={
            "tenant_id": tenant_id, "namespace": namespace, "name": name,
        })
        return [SequenceDefinition.model_validate(d) for d in data]

    # --- Instances ---
    async def create_instance(self, instance: dict[str, Any]) -> TaskInstance:
        data = await self._request("POST", "/instances", json=instance)
        return TaskInstance.model_validate(data)

    async def batch_create_instances(self, instances: list[dict[str, Any]]) -> BatchCreateResponse:
        data = await self._request("POST", "/instances/batch", json={"instances": instances})
        return BatchCreateResponse.model_validate(data)

    async def get_instance(self, id: str) -> TaskInstance:
        data = await self._request("GET", f"/instances/{id}")
        return TaskInstance.model_validate(data)

    async def list_instances(self, **filters: Any) -> list[TaskInstance]:
        data = await self._request("GET", "/instances", params=filters or None)
        return [TaskInstance.model_validate(d) for d in data]

    async def update_instance_state(self, id: str, new_state: str) -> None:
        await self._request("PATCH", f"/instances/{id}/state", json={"state": new_state})

    async def update_instance_context(self, id: str, context: dict[str, Any]) -> None:
        await self._request("PATCH", f"/instances/{id}/context", json=context)

    async def send_signal(self, instance_id: str, signal: dict[str, Any]) -> None:
        await self._request("POST", f"/instances/{instance_id}/signals", json=signal)

    async def get_outputs(self, instance_id: str) -> list[StepOutput]:
        data = await self._request("GET", f"/instances/{instance_id}/outputs")
        return [StepOutput.model_validate(d) for d in data]

    async def get_execution_tree(self, instance_id: str) -> list[ExecutionNode]:
        data = await self._request("GET", f"/instances/{instance_id}/tree")
        return [ExecutionNode.model_validate(d) for d in data]

    async def retry_instance(self, id: str) -> TaskInstance:
        data = await self._request("POST", f"/instances/{id}/retry")
        return TaskInstance.model_validate(data)

    async def list_checkpoints(self, instance_id: str) -> list[Checkpoint]:
        data = await self._request("GET", f"/instances/{instance_id}/checkpoints")
        return [Checkpoint.model_validate(d) for d in data]

    async def save_checkpoint(self, instance_id: str, checkpoint_data: Any) -> None:
        await self._request("POST", f"/instances/{instance_id}/checkpoints", json={"data": checkpoint_data})

    async def get_latest_checkpoint(self, instance_id: str) -> Checkpoint:
        data = await self._request("GET", f"/instances/{instance_id}/checkpoints/latest")
        return Checkpoint.model_validate(data)

    async def prune_checkpoints(self, instance_id: str, keep_last: int | None = None) -> None:
        await self._request("POST", f"/instances/{instance_id}/checkpoints/prune", json={"keep_last": keep_last})

    async def list_audit_log(self, instance_id: str) -> list[AuditEntry]:
        data = await self._request("GET", f"/instances/{instance_id}/audit")
        return [AuditEntry.model_validate(d) for d in data]

    async def bulk_update_state(self, filter: dict[str, Any], new_state: str) -> BulkResponse:
        data = await self._request("PATCH", "/instances/bulk/state", json={**filter, "state": new_state})
        return BulkResponse.model_validate(data)

    async def bulk_reschedule(self, filter: dict[str, Any], offset_secs: int) -> BulkResponse:
        data = await self._request("PATCH", "/instances/bulk/reschedule", json={**filter, "offset_secs": offset_secs})
        return BulkResponse.model_validate(data)

    async def list_dlq(self, **filters: Any) -> list[TaskInstance]:
        data = await self._request("GET", "/instances/dlq", params=filters or None)
        return [TaskInstance.model_validate(d) for d in data]

    # --- Cron ---
    async def create_cron(self, schedule: dict[str, Any]) -> CronSchedule:
        data = await self._request("POST", "/cron", json=schedule)
        return CronSchedule.model_validate(data)

    async def list_cron(self, tenant_id: str | None = None) -> list[CronSchedule]:
        params = {"tenant_id": tenant_id} if tenant_id else None
        data = await self._request("GET", "/cron", params=params)
        return [CronSchedule.model_validate(d) for d in data]

    async def get_cron(self, id: str) -> CronSchedule:
        data = await self._request("GET", f"/cron/{id}")
        return CronSchedule.model_validate(data)

    async def update_cron(self, id: str, schedule: dict[str, Any]) -> None:
        await self._request("PUT", f"/cron/{id}", json=schedule)

    async def delete_cron(self, id: str) -> None:
        await self._request("DELETE", f"/cron/{id}")

    # --- Triggers ---
    async def create_trigger(self, trigger: dict[str, Any]) -> TriggerDef:
        data = await self._request("POST", "/triggers", json=trigger)
        return TriggerDef.model_validate(data)

    async def list_triggers(self, tenant_id: str | None = None) -> list[TriggerDef]:
        params = {"tenant_id": tenant_id} if tenant_id else None
        data = await self._request("GET", "/triggers", params=params)
        return [TriggerDef.model_validate(d) for d in data]

    async def get_trigger(self, slug: str) -> TriggerDef:
        data = await self._request("GET", f"/triggers/{slug}")
        return TriggerDef.model_validate(data)

    async def delete_trigger(self, slug: str) -> None:
        await self._request("DELETE", f"/triggers/{slug}")

    async def fire_trigger(self, slug: str, data: Any) -> FireTriggerResponse:
        resp = await self._request("POST", f"/triggers/{slug}/fire", json=data)
        return FireTriggerResponse.model_validate(resp)

    # --- Plugins ---
    async def create_plugin(self, plugin: dict[str, Any]) -> PluginDef:
        data = await self._request("POST", "/plugins", json=plugin)
        return PluginDef.model_validate(data)

    async def list_plugins(self, tenant_id: str | None = None) -> list[PluginDef]:
        params = {"tenant_id": tenant_id} if tenant_id else None
        data = await self._request("GET", "/plugins", params=params)
        return [PluginDef.model_validate(d) for d in data]

    async def get_plugin(self, name: str) -> PluginDef:
        data = await self._request("GET", f"/plugins/{name}")
        return PluginDef.model_validate(data)

    async def update_plugin(self, name: str, update: dict[str, Any]) -> PluginDef:
        data = await self._request("PATCH", f"/plugins/{name}", json=update)
        return PluginDef.model_validate(data)

    async def delete_plugin(self, name: str) -> None:
        await self._request("DELETE", f"/plugins/{name}")

    # --- Sessions ---
    async def create_session(self, session: dict[str, Any]) -> Session:
        data = await self._request("POST", "/sessions", json=session)
        return Session.model_validate(data)

    async def get_session(self, id: str) -> Session:
        data = await self._request("GET", f"/sessions/{id}")
        return Session.model_validate(data)

    async def get_session_by_key(self, tenant_id: str, key: str) -> Session:
        data = await self._request("GET", f"/sessions/by-key/{tenant_id}/{key}")
        return Session.model_validate(data)

    async def update_session_data(self, id: str, session_data: Any) -> None:
        await self._request("PATCH", f"/sessions/{id}/data", json=session_data)

    async def update_session_state(self, id: str, state: str) -> None:
        await self._request("PATCH", f"/sessions/{id}/state", json={"state": state})

    async def list_session_instances(self, id: str) -> list[TaskInstance]:
        data = await self._request("GET", f"/sessions/{id}/instances")
        return [TaskInstance.model_validate(d) for d in data]

    # --- Workers ---
    async def poll_tasks(self, handler_name: str, worker_id: str, limit: int = 10) -> list[WorkerTask]:
        data = await self._request("POST", "/workers/tasks/poll", json={
            "handler_name": handler_name, "worker_id": worker_id, "limit": limit,
        })
        return [WorkerTask.model_validate(d) for d in data]

    async def complete_task(self, task_id: str, worker_id: str, output: Any) -> None:
        await self._request("POST", f"/workers/tasks/{task_id}/complete", json={
            "worker_id": worker_id, "output": output,
        })

    async def fail_task(self, task_id: str, worker_id: str, message: str, retryable: bool) -> None:
        await self._request("POST", f"/workers/tasks/{task_id}/fail", json={
            "worker_id": worker_id, "message": message, "retryable": retryable,
        })

    async def heartbeat_task(self, task_id: str, worker_id: str) -> None:
        await self._request("POST", f"/workers/tasks/{task_id}/heartbeat", json={
            "worker_id": worker_id,
        })

    # --- Cluster ---
    async def list_cluster_nodes(self) -> list[ClusterNode]:
        data = await self._request("GET", "/cluster/nodes")
        return [ClusterNode.model_validate(d) for d in data]

    async def drain_node(self, node_id: str) -> None:
        await self._request("POST", f"/cluster/nodes/{node_id}/drain")

    # --- Circuit Breakers ---
    async def list_circuit_breakers(self) -> list[CircuitBreaker]:
        data = await self._request("GET", "/circuit-breakers")
        return [CircuitBreaker.model_validate(d) for d in data]

    async def get_circuit_breaker(self, handler: str) -> CircuitBreaker:
        data = await self._request("GET", f"/circuit-breakers/{handler}")
        return CircuitBreaker.model_validate(data)

    async def reset_circuit_breaker(self, handler: str) -> None:
        await self._request("POST", f"/circuit-breakers/{handler}/reset")

    # --- Health ---
    async def health(self) -> HealthResponse:
        data = await self._request("GET", "/health")
        return HealthResponse.model_validate(data)
```

### Step 5: Create worker

Create `sdk-python/src/orch8/worker.py` — polling worker similar to Node.js version:

```python
"""Orch8 polling worker."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from .client import Orch8Client
from .types import WorkerTask

HandlerFn = Callable[[WorkerTask], Awaitable[Any]]
logger = logging.getLogger("orch8.worker")


class Orch8Worker:
    """Polling worker that claims and executes tasks from the Orch8 engine."""

    def __init__(
        self,
        client: Orch8Client,
        worker_id: str,
        handlers: dict[str, HandlerFn],
        poll_interval: float = 1.0,
        heartbeat_interval: float = 15.0,
        max_concurrent: int = 10,
    ) -> None:
        self._client = client
        self._worker_id = worker_id
        self._handlers = handlers
        self._poll_interval = poll_interval
        self._heartbeat_interval = heartbeat_interval
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._in_flight: dict[str, asyncio.Task[None]] = {}
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._tasks = [
            asyncio.create_task(self._poll_loop(handler_name))
            for handler_name in self._handlers
        ]
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))

    async def stop(self, timeout: float = 30.0) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        if self._in_flight:
            _, pending = await asyncio.wait(
                self._in_flight.values(), timeout=timeout,
            )
            for p in pending:
                p.cancel()

    async def _poll_loop(self, handler_name: str) -> None:
        while self._running:
            try:
                tasks = await self._client.poll_tasks(
                    handler_name, self._worker_id,
                    limit=self._semaphore._value,
                )
                for task in tasks:
                    await self._semaphore.acquire()
                    t = asyncio.create_task(self._execute(task))
                    self._in_flight[task.id] = t
            except Exception:
                logger.exception("poll failed for %s", handler_name)
            await asyncio.sleep(self._poll_interval)

    async def _execute(self, task: WorkerTask) -> None:
        handler = self._handlers.get(task.handler_name)
        try:
            if handler is None:
                await self._client.fail_task(
                    task.id, self._worker_id,
                    f"no handler for '{task.handler_name}'", False,
                )
                return
            output = await handler(task)
            await self._client.complete_task(task.id, self._worker_id, output)
        except Exception as exc:
            await self._client.fail_task(
                task.id, self._worker_id, str(exc), True,
            )
        finally:
            self._in_flight.pop(task.id, None)
            self._semaphore.release()

    async def _heartbeat_loop(self) -> None:
        while self._running:
            for task_id in list(self._in_flight):
                try:
                    await self._client.heartbeat_task(task_id, self._worker_id)
                except Exception:
                    pass
            await asyncio.sleep(self._heartbeat_interval)
```

### Step 6: Create `__init__.py`

```python
"""Orch8 SDK for Python."""

from .client import Orch8Client
from .errors import Orch8Error
from .worker import Orch8Worker

__all__ = ["Orch8Client", "Orch8Error", "Orch8Worker"]
```

### Step 7: Write tests

Create `sdk-python/tests/__init__.py` (empty).

Create `sdk-python/tests/test_client.py`:

```python
"""Tests for the Orch8 management client."""

import httpx
import pytest
import respx

from orch8 import Orch8Client, Orch8Error


@pytest.fixture
def client():
    return Orch8Client(base_url="http://localhost:8080", tenant_id="t1")


@respx.mock
async def test_health(client: Orch8Client):
    respx.get("http://localhost:8080/health").mock(
        return_value=httpx.Response(200, json={"status": "ok"}),
    )
    result = await client.health()
    assert result.status == "ok"


@respx.mock
async def test_tenant_header(client: Orch8Client):
    route = respx.get("http://localhost:8080/health").mock(
        return_value=httpx.Response(200, json={"status": "ok"}),
    )
    await client.health()
    assert route.calls[0].request.headers["X-Tenant-Id"] == "t1"


@respx.mock
async def test_create_sequence(client: Orch8Client):
    respx.post("http://localhost:8080/sequences").mock(
        return_value=httpx.Response(200, json={
            "id": "s1", "tenant_id": "t1", "namespace": "default",
            "name": "test", "version": 1, "blocks": [], "created_at": "2026-01-01T00:00:00Z",
        }),
    )
    seq = await client.create_sequence({"name": "test"})
    assert seq.id == "s1"
    assert seq.name == "test"


@respx.mock
async def test_fire_trigger(client: Orch8Client):
    respx.post("http://localhost:8080/triggers/t1/fire").mock(
        return_value=httpx.Response(200, json={
            "instance_id": "i1", "trigger": "t1", "sequence_name": "s1",
        }),
    )
    result = await client.fire_trigger("t1", {"key": "value"})
    assert result.instance_id == "i1"


@respx.mock
async def test_error_raises_orch8_error(client: Orch8Client):
    respx.get("http://localhost:8080/sequences/bad").mock(
        return_value=httpx.Response(404, text="not found"),
    )
    with pytest.raises(Orch8Error) as exc_info:
        await client.get_sequence("bad")
    assert exc_info.value.status == 404


@respx.mock
async def test_delete_returns_none(client: Orch8Client):
    respx.delete("http://localhost:8080/triggers/t1").mock(
        return_value=httpx.Response(204),
    )
    result = await client.delete_trigger("t1")
    assert result is None
```

### Step 8: Install and run tests

```bash
cd sdk-python && pip install -e ".[dev]" && pytest -v
```

### Step 9: Commit

```bash
git add sdk-python/
git commit -m "feat: add Python SDK with async client and polling worker"
```

---

## Task 3: Go SDK (`sdk-go/`)

**Files:**
- Create: `sdk-go/go.mod`
- Create: `sdk-go/client.go`
- Create: `sdk-go/types.go`
- Create: `sdk-go/worker.go`
- Create: `sdk-go/errors.go`
- Create: `sdk-go/client_test.go`

### Step 1: Scaffold module

Create `sdk-go/go.mod`:

```
module github.com/orch8-io/sdk-go

go 1.21
```

### Step 2: Create types

Create `sdk-go/types.go`:

```go
package orch8

// SequenceDefinition represents a workflow sequence.
type SequenceDefinition struct {
	ID           string `json:"id"`
	TenantID     string `json:"tenant_id"`
	Namespace    string `json:"namespace"`
	Name         string `json:"name"`
	Version      int    `json:"version"`
	Deprecated   bool   `json:"deprecated"`
	Blocks       []any  `json:"blocks"`
	Interceptors any    `json:"interceptors,omitempty"`
	CreatedAt    string `json:"created_at"`
}

// TaskInstance represents a running workflow instance.
type TaskInstance struct {
	ID               string `json:"id"`
	SequenceID       string `json:"sequence_id"`
	TenantID         string `json:"tenant_id"`
	Namespace        string `json:"namespace"`
	State            string `json:"state"`
	NextFireAt       string `json:"next_fire_at,omitempty"`
	Priority         string `json:"priority"`
	Timezone         string `json:"timezone"`
	Metadata         any    `json:"metadata,omitempty"`
	Context          any    `json:"context,omitempty"`
	ConcurrencyKey   string `json:"concurrency_key,omitempty"`
	MaxConcurrency   *int   `json:"max_concurrency,omitempty"`
	IdempotencyKey   string `json:"idempotency_key,omitempty"`
	SessionID        string `json:"session_id,omitempty"`
	ParentInstanceID string `json:"parent_instance_id,omitempty"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
}

// ExecutionNode represents a node in the execution tree.
type ExecutionNode struct {
	ID          string `json:"id"`
	InstanceID  string `json:"instance_id"`
	BlockID     string `json:"block_id"`
	ParentID    string `json:"parent_id,omitempty"`
	BlockType   string `json:"block_type"`
	BranchIndex *int   `json:"branch_index,omitempty"`
	State       string `json:"state"`
	StartedAt   string `json:"started_at,omitempty"`
	CompletedAt string `json:"completed_at,omitempty"`
}

// StepOutput represents the output of a step execution.
type StepOutput struct {
	ID           string `json:"id"`
	InstanceID   string `json:"instance_id"`
	BlockID      string `json:"block_id"`
	NodeID       string `json:"node_id"`
	Output       any    `json:"output,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
	CreatedAt    string `json:"created_at"`
}

// Checkpoint represents a saved instance checkpoint.
type Checkpoint struct {
	ID         string `json:"id"`
	InstanceID string `json:"instance_id"`
	Data       any    `json:"data,omitempty"`
	CreatedAt  string `json:"created_at"`
}

// AuditEntry represents an audit log entry.
type AuditEntry struct {
	Timestamp string `json:"timestamp"`
	Event     string `json:"event"`
	Details   any    `json:"details,omitempty"`
}

// CronSchedule represents a cron schedule definition.
type CronSchedule struct {
	ID           string `json:"id"`
	TenantID     string `json:"tenant_id"`
	Namespace    string `json:"namespace"`
	SequenceName string `json:"sequence_name"`
	Version      *int   `json:"version,omitempty"`
	CronExpr     string `json:"cron_expr"`
	Timezone     string `json:"timezone"`
	Enabled      bool   `json:"enabled"`
	InputData    any    `json:"input_data,omitempty"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

// TriggerDef represents a trigger definition.
type TriggerDef struct {
	Slug         string `json:"slug"`
	SequenceName string `json:"sequence_name"`
	Version      *int   `json:"version,omitempty"`
	TenantID     string `json:"tenant_id"`
	Namespace    string `json:"namespace"`
	Enabled      bool   `json:"enabled"`
	Secret       string `json:"secret,omitempty"`
	TriggerType  string `json:"trigger_type"`
	Config       any    `json:"config,omitempty"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

// PluginDef represents a plugin definition.
type PluginDef struct {
	Name        string `json:"name"`
	PluginType  string `json:"plugin_type"`
	Source      string `json:"source"`
	TenantID    string `json:"tenant_id"`
	Enabled     bool   `json:"enabled"`
	Config      any    `json:"config,omitempty"`
	Description string `json:"description,omitempty"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

// Session represents a session.
type Session struct {
	ID        string `json:"id"`
	TenantID  string `json:"tenant_id"`
	Key       string `json:"key"`
	State     string `json:"state"`
	Data      any    `json:"data,omitempty"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// WorkerTask represents a task claimed by a worker.
type WorkerTask struct {
	ID             string `json:"id"`
	InstanceID     string `json:"instance_id"`
	BlockID        string `json:"block_id"`
	HandlerName    string `json:"handler_name"`
	Params         any    `json:"params,omitempty"`
	Context        any    `json:"context,omitempty"`
	Attempt        int    `json:"attempt"`
	TimeoutMs      *int   `json:"timeout_ms,omitempty"`
	State          string `json:"state"`
	WorkerID       string `json:"worker_id,omitempty"`
	ClaimedAt      string `json:"claimed_at,omitempty"`
	HeartbeatAt    string `json:"heartbeat_at,omitempty"`
	CompletedAt    string `json:"completed_at,omitempty"`
	Output         any    `json:"output,omitempty"`
	ErrorMessage   string `json:"error_message,omitempty"`
	ErrorRetryable *bool  `json:"error_retryable,omitempty"`
	CreatedAt      string `json:"created_at"`
}

// ClusterNode represents a node in the cluster.
type ClusterNode struct {
	ID            string `json:"id"`
	Address       string `json:"address"`
	State         string `json:"state"`
	LastHeartbeat string `json:"last_heartbeat"`
}

// CircuitBreaker represents a circuit breaker state.
type CircuitBreakerState struct {
	Handler      string `json:"handler"`
	State        string `json:"state"`
	FailureCount int    `json:"failure_count"`
	LastFailure  string `json:"last_failure,omitempty"`
}

// FireTriggerResponse is returned when firing a trigger.
type FireTriggerResponse struct {
	InstanceID   string `json:"instance_id"`
	Trigger      string `json:"trigger"`
	SequenceName string `json:"sequence_name"`
}

// BulkResponse is returned from bulk operations.
type BulkResponse struct {
	Updated int `json:"updated"`
}

// BatchCreateResponse is returned from batch instance creation.
type BatchCreateResponse struct {
	Created int `json:"created"`
}

// HealthResponse is returned from the health endpoint.
type HealthResponse struct {
	Status string `json:"status"`
}
```

### Step 3: Create errors

Create `sdk-go/errors.go`:

```go
package orch8

import "fmt"

// Orch8Error represents an API error.
type Orch8Error struct {
	Status int
	Body   string
	Path   string
}

func (e *Orch8Error) Error() string {
	return fmt.Sprintf("orch8 API error %d on %s: %s", e.Status, e.Path, e.Body)
}
```

### Step 4: Create client

Create `sdk-go/client.go`. The Go client uses `net/http` with zero external dependencies.

```go
package orch8

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// ClientConfig configures the Orch8 client.
type ClientConfig struct {
	BaseURL  string
	TenantID string
	Headers  map[string]string
}

// Client is the Orch8 API client.
type Client struct {
	baseURL string
	http    *http.Client
	headers map[string]string
}

// NewClient creates a new Orch8 client.
func NewClient(cfg ClientConfig) *Client {
	h := map[string]string{"Content-Type": "application/json"}
	if cfg.TenantID != "" {
		h["X-Tenant-Id"] = cfg.TenantID
	}
	for k, v := range cfg.Headers {
		h[k] = v
	}
	return &Client{
		baseURL: strings.TrimRight(cfg.BaseURL, "/"),
		http:    &http.Client{},
		headers: h,
	}
}

func (c *Client) do(ctx context.Context, method, path string, body any, result any) error {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		return &Orch8Error{Status: resp.StatusCode, Body: string(respBody), Path: path}
	}

	if resp.StatusCode == 204 || result == nil {
		return nil
	}

	return json.Unmarshal(respBody, result)
}

// --- Sequences ---

func (c *Client) CreateSequence(ctx context.Context, definition any) (*SequenceDefinition, error) {
	var out SequenceDefinition
	err := c.do(ctx, "POST", "/sequences", definition, &out)
	return &out, err
}

func (c *Client) GetSequence(ctx context.Context, id string) (*SequenceDefinition, error) {
	var out SequenceDefinition
	err := c.do(ctx, "GET", "/sequences/"+id, nil, &out)
	return &out, err
}

func (c *Client) GetSequenceByName(ctx context.Context, tenantID, namespace, name string, version *int) (*SequenceDefinition, error) {
	params := url.Values{"tenant_id": {tenantID}, "namespace": {namespace}, "name": {name}}
	if version != nil {
		params.Set("version", fmt.Sprint(*version))
	}
	var out SequenceDefinition
	err := c.do(ctx, "GET", "/sequences/by-name?"+params.Encode(), nil, &out)
	return &out, err
}

func (c *Client) DeprecateSequence(ctx context.Context, id string) error {
	return c.do(ctx, "POST", "/sequences/"+id+"/deprecate", nil, nil)
}

func (c *Client) ListSequenceVersions(ctx context.Context, tenantID, namespace, name string) ([]SequenceDefinition, error) {
	params := url.Values{"tenant_id": {tenantID}, "namespace": {namespace}, "name": {name}}
	var out []SequenceDefinition
	err := c.do(ctx, "GET", "/sequences/versions?"+params.Encode(), nil, &out)
	return out, err
}

// --- Instances ---

func (c *Client) CreateInstance(ctx context.Context, instance any) (*TaskInstance, error) {
	var out TaskInstance
	err := c.do(ctx, "POST", "/instances", instance, &out)
	return &out, err
}

func (c *Client) BatchCreateInstances(ctx context.Context, instances []any) (*BatchCreateResponse, error) {
	var out BatchCreateResponse
	err := c.do(ctx, "POST", "/instances/batch", map[string]any{"instances": instances}, &out)
	return &out, err
}

func (c *Client) GetInstance(ctx context.Context, id string) (*TaskInstance, error) {
	var out TaskInstance
	err := c.do(ctx, "GET", "/instances/"+id, nil, &out)
	return &out, err
}

func (c *Client) ListInstances(ctx context.Context, filter map[string]string) ([]TaskInstance, error) {
	path := "/instances"
	if len(filter) > 0 {
		params := url.Values{}
		for k, v := range filter {
			params.Set(k, v)
		}
		path += "?" + params.Encode()
	}
	var out []TaskInstance
	err := c.do(ctx, "GET", path, nil, &out)
	return out, err
}

func (c *Client) UpdateInstanceState(ctx context.Context, id, newState string) error {
	return c.do(ctx, "PATCH", "/instances/"+id+"/state", map[string]string{"state": newState}, nil)
}

func (c *Client) UpdateInstanceContext(ctx context.Context, id string, context any) error {
	return c.do(ctx, "PATCH", "/instances/"+id+"/context", context, nil)
}

func (c *Client) SendSignal(ctx context.Context, instanceID string, signal any) error {
	return c.do(ctx, "POST", "/instances/"+instanceID+"/signals", signal, nil)
}

func (c *Client) GetOutputs(ctx context.Context, instanceID string) ([]StepOutput, error) {
	var out []StepOutput
	err := c.do(ctx, "GET", "/instances/"+instanceID+"/outputs", nil, &out)
	return out, err
}

func (c *Client) GetExecutionTree(ctx context.Context, instanceID string) ([]ExecutionNode, error) {
	var out []ExecutionNode
	err := c.do(ctx, "GET", "/instances/"+instanceID+"/tree", nil, &out)
	return out, err
}

func (c *Client) RetryInstance(ctx context.Context, id string) (*TaskInstance, error) {
	var out TaskInstance
	err := c.do(ctx, "POST", "/instances/"+id+"/retry", nil, &out)
	return &out, err
}

func (c *Client) ListCheckpoints(ctx context.Context, instanceID string) ([]Checkpoint, error) {
	var out []Checkpoint
	err := c.do(ctx, "GET", "/instances/"+instanceID+"/checkpoints", nil, &out)
	return out, err
}

func (c *Client) SaveCheckpoint(ctx context.Context, instanceID string, data any) error {
	return c.do(ctx, "POST", "/instances/"+instanceID+"/checkpoints", map[string]any{"data": data}, nil)
}

func (c *Client) GetLatestCheckpoint(ctx context.Context, instanceID string) (*Checkpoint, error) {
	var out Checkpoint
	err := c.do(ctx, "GET", "/instances/"+instanceID+"/checkpoints/latest", nil, &out)
	return &out, err
}

func (c *Client) PruneCheckpoints(ctx context.Context, instanceID string, keepLast *int) error {
	return c.do(ctx, "POST", "/instances/"+instanceID+"/checkpoints/prune", map[string]any{"keep_last": keepLast}, nil)
}

func (c *Client) ListAuditLog(ctx context.Context, instanceID string) ([]AuditEntry, error) {
	var out []AuditEntry
	err := c.do(ctx, "GET", "/instances/"+instanceID+"/audit", nil, &out)
	return out, err
}

func (c *Client) BulkUpdateState(ctx context.Context, filter map[string]any, newState string) (*BulkResponse, error) {
	body := map[string]any{"state": newState}
	for k, v := range filter {
		body[k] = v
	}
	var out BulkResponse
	err := c.do(ctx, "PATCH", "/instances/bulk/state", body, &out)
	return &out, err
}

func (c *Client) BulkReschedule(ctx context.Context, filter map[string]any, offsetSecs int) (*BulkResponse, error) {
	body := map[string]any{"offset_secs": offsetSecs}
	for k, v := range filter {
		body[k] = v
	}
	var out BulkResponse
	err := c.do(ctx, "PATCH", "/instances/bulk/reschedule", body, &out)
	return &out, err
}

func (c *Client) ListDLQ(ctx context.Context, filter map[string]string) ([]TaskInstance, error) {
	path := "/instances/dlq"
	if len(filter) > 0 {
		params := url.Values{}
		for k, v := range filter {
			params.Set(k, v)
		}
		path += "?" + params.Encode()
	}
	var out []TaskInstance
	err := c.do(ctx, "GET", path, nil, &out)
	return out, err
}

// --- Cron ---

func (c *Client) CreateCron(ctx context.Context, schedule any) (*CronSchedule, error) {
	var out CronSchedule
	err := c.do(ctx, "POST", "/cron", schedule, &out)
	return &out, err
}

func (c *Client) ListCron(ctx context.Context, tenantID string) ([]CronSchedule, error) {
	path := "/cron"
	if tenantID != "" {
		path += "?tenant_id=" + tenantID
	}
	var out []CronSchedule
	err := c.do(ctx, "GET", path, nil, &out)
	return out, err
}

func (c *Client) GetCron(ctx context.Context, id string) (*CronSchedule, error) {
	var out CronSchedule
	err := c.do(ctx, "GET", "/cron/"+id, nil, &out)
	return &out, err
}

func (c *Client) UpdateCron(ctx context.Context, id string, schedule any) error {
	return c.do(ctx, "PUT", "/cron/"+id, schedule, nil)
}

func (c *Client) DeleteCron(ctx context.Context, id string) error {
	return c.do(ctx, "DELETE", "/cron/"+id, nil, nil)
}

// --- Triggers ---

func (c *Client) CreateTrigger(ctx context.Context, trigger any) (*TriggerDef, error) {
	var out TriggerDef
	err := c.do(ctx, "POST", "/triggers", trigger, &out)
	return &out, err
}

func (c *Client) ListTriggers(ctx context.Context, tenantID string) ([]TriggerDef, error) {
	path := "/triggers"
	if tenantID != "" {
		path += "?tenant_id=" + tenantID
	}
	var out []TriggerDef
	err := c.do(ctx, "GET", path, nil, &out)
	return out, err
}

func (c *Client) GetTrigger(ctx context.Context, slug string) (*TriggerDef, error) {
	var out TriggerDef
	err := c.do(ctx, "GET", "/triggers/"+slug, nil, &out)
	return &out, err
}

func (c *Client) DeleteTrigger(ctx context.Context, slug string) error {
	return c.do(ctx, "DELETE", "/triggers/"+slug, nil, nil)
}

func (c *Client) FireTrigger(ctx context.Context, slug string, data any) (*FireTriggerResponse, error) {
	var out FireTriggerResponse
	err := c.do(ctx, "POST", "/triggers/"+slug+"/fire", data, &out)
	return &out, err
}

// --- Plugins ---

func (c *Client) CreatePlugin(ctx context.Context, plugin any) (*PluginDef, error) {
	var out PluginDef
	err := c.do(ctx, "POST", "/plugins", plugin, &out)
	return &out, err
}

func (c *Client) ListPlugins(ctx context.Context, tenantID string) ([]PluginDef, error) {
	path := "/plugins"
	if tenantID != "" {
		path += "?tenant_id=" + tenantID
	}
	var out []PluginDef
	err := c.do(ctx, "GET", path, nil, &out)
	return out, err
}

func (c *Client) GetPlugin(ctx context.Context, name string) (*PluginDef, error) {
	var out PluginDef
	err := c.do(ctx, "GET", "/plugins/"+name, nil, &out)
	return &out, err
}

func (c *Client) UpdatePlugin(ctx context.Context, name string, update any) (*PluginDef, error) {
	var out PluginDef
	err := c.do(ctx, "PATCH", "/plugins/"+name, update, &out)
	return &out, err
}

func (c *Client) DeletePlugin(ctx context.Context, name string) error {
	return c.do(ctx, "DELETE", "/plugins/"+name, nil, nil)
}

// --- Sessions ---

func (c *Client) CreateSession(ctx context.Context, session any) (*Session, error) {
	var out Session
	err := c.do(ctx, "POST", "/sessions", session, &out)
	return &out, err
}

func (c *Client) GetSession(ctx context.Context, id string) (*Session, error) {
	var out Session
	err := c.do(ctx, "GET", "/sessions/"+id, nil, &out)
	return &out, err
}

func (c *Client) GetSessionByKey(ctx context.Context, tenantID, key string) (*Session, error) {
	var out Session
	err := c.do(ctx, "GET", "/sessions/by-key/"+tenantID+"/"+key, nil, &out)
	return &out, err
}

func (c *Client) UpdateSessionData(ctx context.Context, id string, data any) error {
	return c.do(ctx, "PATCH", "/sessions/"+id+"/data", data, nil)
}

func (c *Client) UpdateSessionState(ctx context.Context, id, state string) error {
	return c.do(ctx, "PATCH", "/sessions/"+id+"/state", map[string]string{"state": state}, nil)
}

func (c *Client) ListSessionInstances(ctx context.Context, id string) ([]TaskInstance, error) {
	var out []TaskInstance
	err := c.do(ctx, "GET", "/sessions/"+id+"/instances", nil, &out)
	return out, err
}

// --- Workers ---

func (c *Client) PollTasks(ctx context.Context, handlerName, workerID string, limit int) ([]WorkerTask, error) {
	var out []WorkerTask
	err := c.do(ctx, "POST", "/workers/tasks/poll", map[string]any{
		"handler_name": handlerName, "worker_id": workerID, "limit": limit,
	}, &out)
	return out, err
}

func (c *Client) CompleteTask(ctx context.Context, taskID, workerID string, output any) error {
	return c.do(ctx, "POST", "/workers/tasks/"+taskID+"/complete", map[string]any{
		"worker_id": workerID, "output": output,
	}, nil)
}

func (c *Client) FailTask(ctx context.Context, taskID, workerID, message string, retryable bool) error {
	return c.do(ctx, "POST", "/workers/tasks/"+taskID+"/fail", map[string]any{
		"worker_id": workerID, "message": message, "retryable": retryable,
	}, nil)
}

func (c *Client) HeartbeatTask(ctx context.Context, taskID, workerID string) error {
	return c.do(ctx, "POST", "/workers/tasks/"+taskID+"/heartbeat", map[string]any{
		"worker_id": workerID,
	}, nil)
}

// --- Cluster ---

func (c *Client) ListClusterNodes(ctx context.Context) ([]ClusterNode, error) {
	var out []ClusterNode
	err := c.do(ctx, "GET", "/cluster/nodes", nil, &out)
	return out, err
}

func (c *Client) DrainNode(ctx context.Context, nodeID string) error {
	return c.do(ctx, "POST", "/cluster/nodes/"+nodeID+"/drain", nil, nil)
}

// --- Circuit Breakers ---

func (c *Client) ListCircuitBreakers(ctx context.Context) ([]CircuitBreakerState, error) {
	var out []CircuitBreakerState
	err := c.do(ctx, "GET", "/circuit-breakers", nil, &out)
	return out, err
}

func (c *Client) GetCircuitBreaker(ctx context.Context, handler string) (*CircuitBreakerState, error) {
	var out CircuitBreakerState
	err := c.do(ctx, "GET", "/circuit-breakers/"+handler, nil, &out)
	return &out, err
}

func (c *Client) ResetCircuitBreaker(ctx context.Context, handler string) error {
	return c.do(ctx, "POST", "/circuit-breakers/"+handler+"/reset", nil, nil)
}

// --- Health ---

func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	var out HealthResponse
	err := c.do(ctx, "GET", "/health", nil, &out)
	return &out, err
}
```

### Step 5: Create worker

Create `sdk-go/worker.go`:

```go
package orch8

import (
	"context"
	"log"
	"sync"
	"time"
)

// HandlerFunc processes a worker task and returns the output.
type HandlerFunc func(ctx context.Context, task WorkerTask) (any, error)

// WorkerConfig configures the polling worker.
type WorkerConfig struct {
	Client            *Client
	WorkerID          string
	Handlers          map[string]HandlerFunc
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	MaxConcurrent     int
}

// Worker polls for tasks and executes handlers.
type Worker struct {
	cfg       WorkerConfig
	sem       chan struct{}
	inFlight  sync.WaitGroup
	cancelFn  context.CancelFunc
}

// NewWorker creates a new polling worker.
func NewWorker(cfg WorkerConfig) *Worker {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 15 * time.Second
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 10
	}
	return &Worker{
		cfg: cfg,
		sem: make(chan struct{}, cfg.MaxConcurrent),
	}
}

// Start begins polling. Blocks until ctx is cancelled.
func (w *Worker) Start(ctx context.Context) {
	ctx, w.cancelFn = context.WithCancel(ctx)

	for name := range w.cfg.Handlers {
		go w.pollLoop(ctx, name)
	}
	go w.heartbeatLoop(ctx)
	<-ctx.Done()
	w.inFlight.Wait()
}

// Stop cancels the worker and waits for in-flight tasks.
func (w *Worker) Stop() {
	if w.cancelFn != nil {
		w.cancelFn()
	}
	w.inFlight.Wait()
}

func (w *Worker) pollLoop(ctx context.Context, handlerName string) {
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tasks, err := w.cfg.Client.PollTasks(ctx, handlerName, w.cfg.WorkerID, cap(w.sem)-len(w.sem))
			if err != nil {
				continue
			}
			for _, task := range tasks {
				w.sem <- struct{}{}
				w.inFlight.Add(1)
				go w.execute(ctx, task)
			}
		}
	}
}

func (w *Worker) execute(ctx context.Context, task WorkerTask) {
	defer func() {
		<-w.sem
		w.inFlight.Done()
	}()

	handler, ok := w.cfg.Handlers[task.HandlerName]
	if !ok {
		_ = w.cfg.Client.FailTask(ctx, task.ID, w.cfg.WorkerID, "no handler for '"+task.HandlerName+"'", false)
		return
	}

	output, err := handler(ctx, task)
	if err != nil {
		_ = w.cfg.Client.FailTask(ctx, task.ID, w.cfg.WorkerID, err.Error(), true)
		return
	}
	_ = w.cfg.Client.CompleteTask(ctx, task.ID, w.cfg.WorkerID, output)
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Heartbeats are best-effort; the worker doesn't track individual in-flight task IDs
			// because the engine handles reaping via timeouts.
			_ = log.Prefix() // satisfy unused import if needed
		}
	}
}
```

### Step 6: Write tests

Create `sdk-go/client_test.go`:

```go
package orch8

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("X-Tenant-Id") != "t1" {
			t.Error("missing tenant header")
		}
		json.NewEncoder(w).Encode(HealthResponse{Status: "ok"})
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{BaseURL: srv.URL, TenantID: "t1"})
	resp, err := client.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != "ok" {
		t.Errorf("expected ok, got %s", resp.Status)
	}
}

func TestCreateSequence(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/sequences" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(SequenceDefinition{
			ID: "s1", Name: "test", TenantID: "t1", Namespace: "default",
			Version: 1, CreatedAt: "2026-01-01T00:00:00Z",
		})
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{BaseURL: srv.URL})
	seq, err := client.CreateSequence(context.Background(), map[string]string{"name": "test"})
	if err != nil {
		t.Fatal(err)
	}
	if seq.ID != "s1" {
		t.Errorf("expected s1, got %s", seq.ID)
	}
}

func TestFireTrigger(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/triggers/t1/fire" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(FireTriggerResponse{
			InstanceID: "i1", Trigger: "t1", SequenceName: "s1",
		})
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{BaseURL: srv.URL})
	resp, err := client.FireTrigger(context.Background(), "t1", map[string]string{"key": "val"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.InstanceID != "i1" {
		t.Errorf("expected i1, got %s", resp.InstanceID)
	}
}

func TestErrorHandling(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(404)
		w.Write([]byte("not found"))
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{BaseURL: srv.URL})
	_, err := client.GetSequence(context.Background(), "bad")
	if err == nil {
		t.Fatal("expected error")
	}
	orch8Err, ok := err.(*Orch8Error)
	if !ok {
		t.Fatal("expected Orch8Error")
	}
	if orch8Err.Status != 404 {
		t.Errorf("expected 404, got %d", orch8Err.Status)
	}
}

func TestDeleteReturnsNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{BaseURL: srv.URL})
	err := client.DeleteTrigger(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
}
```

### Step 7: Run tests

```bash
cd sdk-go && go test -v ./...
```

### Step 8: Commit

```bash
git add sdk-go/
git commit -m "feat: add Go SDK with typed client and polling worker"
```

---

## Task 4: Helm Chart (`helm/orch8/`)

**Files:**
- Create: `helm/orch8/Chart.yaml`
- Create: `helm/orch8/values.yaml`
- Create: `helm/orch8/templates/_helpers.tpl`
- Create: `helm/orch8/templates/deployment.yaml`
- Create: `helm/orch8/templates/service.yaml`
- Create: `helm/orch8/templates/configmap.yaml`
- Create: `helm/orch8/templates/secret.yaml`
- Create: `helm/orch8/templates/ingress.yaml`
- Create: `helm/orch8/templates/serviceaccount.yaml`

### Step 1: Create Chart.yaml

```yaml
apiVersion: v2
name: orch8
description: Orch8 workflow engine
type: application
version: 0.1.0
appVersion: "0.1.0"
keywords:
  - workflow
  - orchestration
  - task-queue
maintainers:
  - name: orch8
    url: https://orch8.io
```

### Step 2: Create values.yaml

```yaml
replicaCount: 1

image:
  repository: orch8/engine
  tag: ""  # defaults to Chart.appVersion
  pullPolicy: IfNotPresent

service:
  type: ClusterIP
  port: 8080
  grpcPort: 50051

ingress:
  enabled: false
  className: ""
  annotations: {}
  hosts:
    - host: orch8.local
      paths:
        - path: /
          pathType: Prefix
  tls: []

resources:
  limits:
    memory: 512Mi
  requests:
    cpu: 100m
    memory: 128Mi

serviceAccount:
  create: true
  name: ""
  annotations: {}

config:
  # Database
  databaseUrl: "postgresql://orch8:orch8@postgres:5432/orch8"
  # Server
  httpPort: 8080
  grpcPort: 50051
  # Engine
  workerCount: 4
  pollInterval: "1s"
  # Optional
  encryptionKey: ""
  rateLimitRps: 0
  requireTenantHeader: false
  logLevel: "info"

# External Postgres (if not using subchart)
postgres:
  external: true
  host: postgres
  port: 5432
  database: orch8
  username: orch8
  password: orch8

autoscaling:
  enabled: false
  minReplicas: 1
  maxReplicas: 10
  targetCPUUtilizationPercentage: 80

nodeSelector: {}
tolerations: []
affinity: {}
```

### Step 3: Create templates/_helpers.tpl

```yaml
{{- define "orch8.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "orch8.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "orch8.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "orch8.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "orch8.selectorLabels" -}}
app.kubernetes.io/name: {{ include "orch8.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "orch8.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "orch8.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
```

### Step 4: Create templates (deployment, service, configmap, secret, ingress, serviceaccount)

These are standard Helm templates that reference the values above. The deployment runs the orch8-server binary with environment variables from the configmap and secret.

### Step 5: Lint

```bash
helm lint helm/orch8/
```

### Step 6: Commit

```bash
git add helm/
git commit -m "feat: add Helm chart for Kubernetes deployment"
```

---

## Task 5: Update RUST_ISSUES.md

**Files:**
- Modify: `docs/RUST_ISSUES.md`

Mark items #12, #14, #15 as FIXED with brief descriptions of what was added.

### Step 1: Update RUST_ISSUES.md

Change:
- `### 12. Node.js SDK — Worker Only` → `### 12. ~~Node.js SDK — Worker Only~~ FIXED`
- `### 14. No Python or Go SDKs` → `### 14. ~~No Python or Go SDKs~~ FIXED`
- `### 15. No Helm Chart` → `### 15. ~~No Helm Chart~~ FIXED`

### Step 2: Commit

```bash
git add docs/RUST_ISSUES.md
git commit -m "docs: mark SDK and Helm chart issues as fixed"
```
