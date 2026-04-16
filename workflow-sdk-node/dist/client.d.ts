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
    context?: {
        data?: unknown;
        config?: unknown;
    };
    priority?: "low" | "normal" | "high";
    idempotency_key?: string;
    timezone?: string;
}
export declare class Orch8Client {
    private readonly baseUrl;
    private readonly tenantId;
    private readonly apiKey?;
    private readonly fetchImpl;
    constructor(opts: Orch8ClientOptions);
    private headers;
    private request;
    /** Create or update a sequence. Accepts a builder or raw payload. */
    createSequence(wf: WorkflowBuilder | SequenceCreate): Promise<{
        id: string;
        version: number;
    }>;
    /** Start a new instance of a sequence. */
    createInstance(payload: InstanceCreatePayload): Promise<{
        id: string;
    }>;
    /** Fetch an instance by ID. */
    getInstance(id: string): Promise<unknown>;
    /** Send a signal to an instance. `name` is the signal key, `payload` is JSON. */
    sendSignal(instanceId: string, name: string, payload?: unknown): Promise<void>;
}
export declare class Orch8Error extends Error {
    readonly status: number;
    constructor(status: number, message: string);
}
