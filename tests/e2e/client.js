/**
 * Orch8 JS test client — thin REST wrapper.
 * Zero dependencies, uses Node.js built-in fetch.
 */

const DEFAULT_BASE = "http://localhost:18080";

export class Orch8Client {
  constructor(baseUrl = DEFAULT_BASE) {
    this.baseUrl = baseUrl;
  }

  // --- Sequences ---

  async createSequence(seq) {
    return this.#post("/sequences", seq);
  }

  async getSequence(id) {
    return this.#get(`/sequences/${id}`);
  }

  async getSequenceByName(tenantId, namespace, name, version) {
    const params = new URLSearchParams({ tenant_id: tenantId, namespace, name });
    if (version != null) params.set("version", version);
    return this.#get(`/sequences/by-name?${params}`);
  }

  // --- Instances ---

  async createInstance(req) {
    return this.#post("/instances", req);
  }

  async createInstancesBatch(instances) {
    return this.#post("/instances/batch", { instances });
  }

  async getInstance(id) {
    return this.#get(`/instances/${id}`);
  }

  async listInstances(query = {}) {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(query)) {
      if (v != null) params.set(k, v);
    }
    const qs = params.toString();
    return this.#get(`/instances${qs ? `?${qs}` : ""}`);
  }

  async updateState(id, state, nextFireAt) {
    const body = { state };
    if (nextFireAt) body.next_fire_at = nextFireAt;
    return this.#patch(`/instances/${id}/state`, body);
  }

  async updateContext(id, context) {
    return this.#patch(`/instances/${id}/context`, { context });
  }

  async sendSignal(id, signalType, payload = {}) {
    return this.#post(`/instances/${id}/signals`, {
      signal_type: signalType,
      payload,
    });
  }

  async getOutputs(id) {
    return this.#get(`/instances/${id}/outputs`);
  }

  async bulkUpdateState(filter, state) {
    return this.#patch("/instances/bulk/state", { filter, state });
  }

  async listDlq(query = {}) {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(query)) {
      if (v != null) params.set(k, v);
    }
    const qs = params.toString();
    return this.#get(`/instances/dlq${qs ? `?${qs}` : ""}`);
  }

  async retryInstance(id) {
    return this.#post(`/instances/${id}/retry`, {});
  }

  // --- Cron ---

  async createCron(req) {
    return this.#post("/cron", req);
  }

  async getCron(id) {
    return this.#get(`/cron/${id}`);
  }

  async listCron(query = {}) {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(query)) {
      if (v != null) params.set(k, v);
    }
    const qs = params.toString();
    return this.#get(`/cron${qs ? `?${qs}` : ""}`);
  }

  async updateCron(id, body) {
    return this.#put(`/cron/${id}`, body);
  }

  async deleteCron(id) {
    const res = await fetch(`${this.baseUrl}/cron/${id}`, { method: "DELETE" });
    if (!res.ok) {
      const text = await res.text();
      throw new ApiError(res.status, text, `/cron/${id}`);
    }
    return {};
  }

  // --- Workers ---

  async pollWorkerTasks(handlerName, workerId, limit = 1) {
    return this.#post("/workers/tasks/poll", {
      handler_name: handlerName,
      worker_id: workerId,
      limit,
    });
  }

  async completeWorkerTask(taskId, workerId, output = {}) {
    return this.#post(`/workers/tasks/${taskId}/complete`, {
      worker_id: workerId,
      output,
    });
  }

  async failWorkerTask(taskId, workerId, message, retryable = false) {
    return this.#post(`/workers/tasks/${taskId}/fail`, {
      worker_id: workerId,
      message,
      retryable,
    });
  }

  async heartbeatWorkerTask(taskId, workerId) {
    return this.#post(`/workers/tasks/${taskId}/heartbeat`, {
      worker_id: workerId,
    });
  }

  async listWorkerTasks(query = {}) {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(query)) {
      if (v != null) params.set(k, v);
    }
    const qs = params.toString();
    return this.#get(`/workers/tasks${qs ? `?${qs}` : ""}`);
  }

  async workerTaskStats() {
    return this.#get("/workers/tasks/stats");
  }

  // --- Health ---

  async healthLive() {
    return this.#get("/health/live");
  }

  async healthReady() {
    return this.#get("/health/ready");
  }

  // --- Metrics ---

  async metrics() {
    const res = await fetch(`${this.baseUrl}/metrics`);
    return res.text();
  }

  // --- Helpers ---

  /**
   * Poll an instance until it reaches one of the target states.
   * Returns the instance when reached, throws on timeout.
   */
  async waitForState(id, targetStates, { timeoutMs = 15000, intervalMs = 200 } = {}) {
    const states = Array.isArray(targetStates) ? targetStates : [targetStates];
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
      const instance = await this.getInstance(id);
      if (states.includes(instance.state)) {
        return instance;
      }
      await sleep(intervalMs);
    }

    const instance = await this.getInstance(id);
    throw new Error(
      `Timeout waiting for instance ${id} to reach [${states}]. Current state: ${instance.state}`
    );
  }

  /**
   * Wait for server to be healthy.
   */
  async waitForReady({ timeoutMs = 10000, intervalMs = 200 } = {}) {
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

  async #get(path) {
    const res = await fetch(`${this.baseUrl}${path}`);
    if (!res.ok) {
      const body = await res.text();
      throw new ApiError(res.status, body, path);
    }
    return res.json();
  }

  async #post(path, body) {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text();
      throw new ApiError(res.status, text, path);
    }
    const text = await res.text();
    return text ? JSON.parse(text) : {};
  }

  async #put(path, body) {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text();
      throw new ApiError(res.status, text, path);
    }
    const text = await res.text();
    return text ? JSON.parse(text) : {};
  }

  async #patch(path, body) {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text();
      throw new ApiError(res.status, text, path);
    }
    // PATCH may return 200 with no body
    const text = await res.text();
    return text ? JSON.parse(text) : {};
  }
}

export class ApiError extends Error {
  constructor(status, body, path) {
    super(`HTTP ${status} on ${path}: ${body}`);
    this.status = status;
    this.body = body;
    this.path = path;
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Generate a random UUID v4 (for test IDs).
 */
export function uuid() {
  return crypto.randomUUID();
}

/**
 * Build a minimal sequence definition for testing.
 */
export function testSequence(name, blocks, { tenantId = "test", namespace = "default" } = {}) {
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

/**
 * Build a step block definition.
 */
export function step(id, handler, params = {}, opts = {}) {
  return {
    type: "step",
    id,
    handler,
    params,
    ...opts,
  };
}
