/**
 * Thin HTTP client for the orch8 REST API.
 *
 * Surfaces the endpoints most callers need (sequences + instances). For the
 * full surface, prefer hand-rolled fetch calls with the generated OpenAPI
 * types — this client is deliberately minimal to keep the SDK lean.
 */
import type { SequenceCreate } from "./schema.js";
import type { WorkflowBuilder } from "./builder.js";

export interface Orch8ClientOptions {
  /** Base URL of the orch8 server, e.g. `https://orch8.example.com`. */
  baseUrl: string;
  /** Tenant identifier — sent as `x-tenant-id`. */
  tenantId: string;
  /** API key — sent as `authorization: Bearer <key>`. Omit for internal/dev. */
  apiKey?: string;
  /** Optional fetch implementation (for testing). Defaults to global `fetch`. */
  fetch?: typeof fetch;
}

export interface InstanceCreatePayload {
  sequence_id?: string;
  sequence_name?: string;
  namespace?: string;
  version?: number;
  context?: { data?: unknown; config?: unknown };
  priority?: "low" | "normal" | "high";
  idempotency_key?: string;
  timezone?: string;
}

export class Orch8Client {
  private readonly baseUrl: string;
  private readonly tenantId: string;
  private readonly apiKey?: string;
  private readonly fetchImpl: typeof fetch;

  constructor(opts: Orch8ClientOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/$/, "");
    this.tenantId = opts.tenantId;
    this.apiKey = opts.apiKey;
    this.fetchImpl = opts.fetch ?? globalThis.fetch;
    if (!this.fetchImpl) {
      throw new Error("No fetch implementation available — pass one via options or use Node 18+");
    }
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = {
      "content-type": "application/json",
      "x-tenant-id": this.tenantId,
    };
    if (this.apiKey) {
      h["authorization"] = `Bearer ${this.apiKey}`;
    }
    return h;
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const res = await this.fetchImpl(`${this.baseUrl}${path}`, {
      method,
      headers: this.headers(),
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Orch8Error(res.status, `${method} ${path} -> ${res.status}: ${text}`);
    }
    // 204 No Content
    if (res.status === 204) return undefined as T;
    return (await res.json()) as T;
  }

  /** Create or update a sequence. Accepts a builder or raw payload. */
  async createSequence(
    wf: WorkflowBuilder | SequenceCreate,
  ): Promise<{ id: string; version: number }> {
    const payload = "build" in wf ? wf.build() : wf;
    return this.request("POST", "/sequences", payload);
  }

  /** Start a new instance of a sequence. */
  async createInstance(payload: InstanceCreatePayload): Promise<{ id: string }> {
    return this.request("POST", "/instances", payload);
  }

  /** Fetch an instance by ID. */
  async getInstance(id: string): Promise<unknown> {
    return this.request("GET", `/instances/${encodeURIComponent(id)}`);
  }

  /** Send a signal to an instance. `name` is the signal key, `payload` is JSON. */
  async sendSignal(
    instanceId: string,
    name: string,
    payload: unknown = {},
  ): Promise<void> {
    await this.request("POST", `/instances/${encodeURIComponent(instanceId)}/signals`, {
      name,
      payload,
    });
  }
}

export class Orch8Error extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "Orch8Error";
  }
}
