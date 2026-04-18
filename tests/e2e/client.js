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

  async bulkReschedule(filter, offsetSecs) {
    return this.#patch("/instances/bulk/reschedule", {
      filter,
      offset_secs: offsetSecs,
    });
  }

  async getInstanceTree(id) {
    return this.#get(`/instances/${id}/tree`);
  }

  async getAuditLog(id) {
    return this.#get(`/instances/${id}/audit`);
  }

  async pruneCheckpoints(id, keep) {
    return this.#post(`/instances/${id}/checkpoints/prune`, { keep });
  }

  async listCheckpoints(id) {
    return this.#get(`/instances/${id}/checkpoints`);
  }

  async saveCheckpoint(id, checkpointData) {
    return this.#post(`/instances/${id}/checkpoints`, {
      checkpoint_data: checkpointData,
    });
  }

  // --- Pools ---

  async createPool(req) {
    return this.#post("/pools", req);
  }

  async listPools(query = {}) {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(query)) {
      if (v != null) params.set(k, v);
    }
    const qs = params.toString();
    return this.#get(`/pools${qs ? `?${qs}` : ""}`);
  }

  async getPool(id) {
    return this.#get(`/pools/${id}`);
  }

  async deletePool(id) {
    const res = await fetch(`${this.baseUrl}/pools/${id}`, { method: "DELETE" });
    if (!res.ok) {
      const text = await res.text();
      throw new ApiError(res.status, text, `/pools/${id}`);
    }
    return {};
  }

  async addPoolResource(poolId, req) {
    return this.#post(`/pools/${poolId}/resources`, req);
  }

  async listPoolResources(poolId) {
    return this.#get(`/pools/${poolId}/resources`);
  }

  async updatePoolResource(poolId, resourceId, body) {
    return this.#put(`/pools/${poolId}/resources/${resourceId}`, body);
  }

  async deletePoolResource(poolId, resourceId) {
    const res = await fetch(
      `${this.baseUrl}/pools/${poolId}/resources/${resourceId}`,
      { method: "DELETE" }
    );
    if (!res.ok) {
      const text = await res.text();
      throw new ApiError(
        res.status,
        text,
        `/pools/${poolId}/resources/${resourceId}`
      );
    }
    return {};
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

  // --- Triggers ---

  async createTrigger(body) {
    return this.#post("/triggers", body);
  }

  async getTrigger(slug) {
    return this.#get(`/triggers/${slug}`);
  }

  async listTriggers(query = {}) {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(query)) {
      if (v != null) params.set(k, v);
    }
    const qs = params.toString();
    return this.#get(`/triggers${qs ? `?${qs}` : ""}`);
  }

  async deleteTrigger(slug) {
    const res = await fetch(`${this.baseUrl}/triggers/${slug}`, { method: "DELETE" });
    if (!res.ok) {
      const text = await res.text();
      throw new ApiError(res.status, text, `/triggers/${slug}`);
    }
    return {};
  }

  async fireTrigger(slug, body = {}, headers = {}) {
    const res = await fetch(`${this.baseUrl}/triggers/${slug}/fire`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text();
      throw new ApiError(res.status, text, `/triggers/${slug}/fire`);
    }
    const text = await res.text();
    return text ? JSON.parse(text) : {};
  }

  async fireWebhook(slug, body = {}, headers = {}) {
    const res = await fetch(`${this.baseUrl}/webhooks/${slug}`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text();
      throw new ApiError(res.status, text, `/webhooks/${slug}`);
    }
    const text = await res.text();
    return text ? JSON.parse(text) : {};
  }

  // --- Circuit Breakers ---

  async listCircuitBreakers() {
    return this.#get("/circuit-breakers");
  }

  async getCircuitBreaker(handler) {
    return this.#get(`/circuit-breakers/${handler}`);
  }

  async resetCircuitBreaker(handler) {
    return this.#post(`/circuit-breakers/${handler}/reset`, {});
  }

  // --- Sessions ---

  async createSession(body) {
    return this.#post("/sessions", body);
  }

  async getSession(id) {
    return this.#get(`/sessions/${id}`);
  }

  async getSessionByKey(tenantId, key) {
    return this.#get(`/sessions/by-key/${encodeURIComponent(tenantId)}/${encodeURIComponent(key)}`);
  }

  async updateSessionData(id, data) {
    return this.#patch(`/sessions/${id}/data`, { data });
  }

  async updateSessionState(id, state) {
    return this.#patch(`/sessions/${id}/state`, { state });
  }

  async listSessionInstances(id) {
    return this.#get(`/sessions/${id}/instances`);
  }

  // --- Sequence versioning ---

  async listSequenceVersions(tenantId, namespace, name) {
    const params = new URLSearchParams({ tenant_id: tenantId, namespace, name });
    return this.#get(`/sequences/versions?${params}`);
  }

  async deprecateSequence(id) {
    return this.#post(`/sequences/${id}/deprecate`, {});
  }

  async migrateInstance(instanceId, targetSequenceId) {
    return this.#post("/sequences/migrate-instance", {
      instance_id: instanceId,
      target_sequence_id: targetSequenceId,
    });
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
