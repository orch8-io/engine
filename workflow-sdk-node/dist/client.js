"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Orch8Error = exports.Orch8Client = void 0;
class Orch8Client {
    baseUrl;
    tenantId;
    apiKey;
    fetchImpl;
    constructor(opts) {
        this.baseUrl = opts.baseUrl.replace(/\/$/, "");
        this.tenantId = opts.tenantId;
        this.apiKey = opts.apiKey;
        this.fetchImpl = opts.fetch ?? globalThis.fetch;
        if (!this.fetchImpl) {
            throw new Error("No fetch implementation available — pass one via options or use Node 18+");
        }
    }
    headers() {
        const h = {
            "content-type": "application/json",
            "x-tenant-id": this.tenantId,
        };
        if (this.apiKey) {
            h["authorization"] = `Bearer ${this.apiKey}`;
        }
        return h;
    }
    async request(method, path, body) {
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
        if (res.status === 204)
            return undefined;
        return (await res.json());
    }
    /** Create or update a sequence. Accepts a builder or raw payload. */
    async createSequence(wf) {
        const payload = "build" in wf ? wf.build() : wf;
        return this.request("POST", "/sequences", payload);
    }
    /** Start a new instance of a sequence. */
    async createInstance(payload) {
        return this.request("POST", "/instances", payload);
    }
    /** Fetch an instance by ID. */
    async getInstance(id) {
        return this.request("GET", `/instances/${encodeURIComponent(id)}`);
    }
    /** Send a signal to an instance. `name` is the signal key, `payload` is JSON. */
    async sendSignal(instanceId, name, payload = {}) {
        await this.request("POST", `/instances/${encodeURIComponent(instanceId)}/signals`, {
            name,
            payload,
        });
    }
}
exports.Orch8Client = Orch8Client;
class Orch8Error extends Error {
    status;
    constructor(status, message) {
        super(message);
        this.status = status;
        this.name = "Orch8Error";
    }
}
exports.Orch8Error = Orch8Error;
