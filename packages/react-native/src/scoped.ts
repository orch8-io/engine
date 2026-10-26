import type { EventSource, Subscription } from "./core";
import type { InstanceStateKind, SyncResult } from "./types";

/** Trusted host identity; tokens and the currently selected UI account are not identity keys. */
export interface AccountIdentity { accountKey: string; issuer: string; subject: string }
export interface CatalogConfiguration {
  catalogKey: string; source: string; signingAuthority: string; manifestUrl: string; identityUrl: string;
}
export interface CatalogRunRequest { workflow: string; operationKey: string; inputJson?: string; version?: number }
export interface CatalogCallbackContext {
  readonly accountId: string; readonly catalogId: string; readonly ownerEpoch: string;
  readonly runId: string; readonly blockId: string; readonly attempt: number; readonly effectId: string;
}
export type CatalogStepHandler = (context: CatalogCallbackContext, stepName: string, inputJson: string) => Promise<string> | string;
export interface CatalogTokenSource { currentToken(): string; refreshToken(): Promise<string> }
export interface CatalogRunSnapshot {
  readonly instanceId: string; readonly accountId: string; readonly catalogId: string;
  readonly sessionEpoch: string; readonly state: InstanceStateKind; readonly dataJson: string;
  readonly pendingSteps: readonly string[]; readonly createdAt: string; readonly updatedAt: string;
}
export interface CatalogArchiveReceipt {
  readonly instanceId: string; readonly accountId: string; readonly catalogId: string;
  readonly acknowledgementDigest: string; readonly archivedAt: string; readonly outcome: InstanceStateKind | null;
}
export type CatalogNativeState = { kind: "not_materialized" | "unavailable" } |
  { kind: "present" | "recorded"; state: InstanceStateKind };
export interface CatalogRunSummary {
  readonly instanceId: string; readonly accountId: string; readonly catalogId: string; readonly ownerEpoch: string;
  readonly workflow: string; readonly version: number; readonly sequenceId: string; readonly createdAt: string;
  readonly requestState: "prepared" | "cancelled" | "archived"; readonly nativeState: CatalogNativeState;
}
export interface CatalogRunPage { readonly items: readonly CatalogRunSummary[]; readonly next: CatalogHistoryCursor | null }
export type NativeScopedCall = (handle: string, method: string, argsJson: string) => Promise<string>;

interface Descriptor { handle: string; kind: string; accountId: string; catalogId?: string; sessionEpoch: string; instanceId?: string }
interface Fence { closed: boolean; parent?: Fence }
const shutdown = () => Object.assign(new Error("scope view retired"), { code: "ORCH8_SHUTDOWN" });
function check(fence: Fence): void { if (fence.closed) throw shutdown(); if (fence.parent) check(fence.parent); }
function object(value: unknown): Record<string, any> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid scoped native response");
  return value as Record<string, any>;
}
function text(value: unknown): string {
  if (typeof value !== "string" || !value.length) throw new Error("Invalid scoped native string");
  return value;
}
function epoch(value: unknown): string {
  const v = text(value);
  if (!/^[1-9][0-9]*$/.test(v) || v.length > 19 || (v.length === 19 && v > "9223372036854775807"))
    throw new Error("Invalid scoped native epoch");
  return v;
}
function state(value: unknown, terminal = false): InstanceStateKind {
  const all = ["scheduled", "running", "waiting", "paused", "completed", "failed", "cancelled"];
  if (typeof value !== "string" || !all.includes(value) || (terminal && !all.slice(4).includes(value)))
    throw new Error("Invalid scoped native state");
  return value as InstanceStateKind;
}
function boolean(value: unknown): boolean {
  if (typeof value !== "boolean") throw new Error("Invalid scoped native boolean");
  return value;
}
function syncResult(value: unknown): SyncResult {
  const v = object(value);
  for (const field of ["added", "updated", "removed", "skipped", "signatureFailures"]) {
    if (!Number.isInteger(v[field]) || v[field] < 0 || v[field] > 4294967295) throw new Error("Invalid scoped sync count");
  }
  return Object.freeze({ ...v }) as SyncResult;
}
function descriptor(value: unknown, kind: string, parent?: Descriptor): Descriptor {
  const v = object(value);
  text(v.handle); text(v.accountId); epoch(v.sessionEpoch);
  if (v.kind !== kind || (parent && (v.accountId !== parent.accountId || v.sessionEpoch !== parent.sessionEpoch ||
      (parent.catalogId && v.catalogId !== parent.catalogId)))) throw new Error("Scoped native owner mismatch");
  if (kind !== "account") text(v.catalogId);
  if (kind === "run") text(v.instanceId);
  return Object.freeze({ ...v }) as Descriptor;
}
function owned(value: unknown, owner: Descriptor, run = false): Record<string, any> {
  const v = object(value);
  if (v.accountId !== owner.accountId || v.catalogId !== owner.catalogId || (run && v.instanceId !== owner.instanceId))
    throw new Error("Scoped native owner mismatch");
  return v;
}
let nextCallback = 0;
const callbackKey = () => {
  if (nextCallback >= Number.MAX_SAFE_INTEGER) throw new Error("Scoped callback identity space exhausted");
  return `scope-${++nextCallback}`;
};
// Native callback timeouts cannot cancel an arbitrary JS promise. Keep its
// physical host charge until settlement, including across root replacement.
const hostBudgets = {
  handler: { active: 0, accounts: new Map<string, number>() },
  token: { active: 0, accounts: new Map<string, number>() },
};
function enterHost(role: keyof typeof hostBudgets, accountId: string): () => void {
  const budget = hostBudgets[role], used = budget.accounts.get(accountId) ?? 0;
  if (budget.active >= 32 || used >= 2) throw Object.assign(new Error(`JS scoped ${role} capacity exhausted`), { code: "ORCH8_RESOURCE_LIMIT" });
  budget.active++; budget.accounts.set(accountId, used + 1);
  return () => {
    budget.active--;
    const remaining = budget.accounts.get(accountId)! - 1;
    if (remaining) budget.accounts.set(accountId, remaining); else budget.accounts.delete(accountId);
  };
}
function hostFailure(error: unknown, fallback: string): { message: string; permanent: boolean } {
  try {
    if (!error || typeof error !== "object") return { message: fallback, permanent: false };
    // Do not invoke arbitrary Error.message/permanent accessors while handling
    // a host failure. SDK errors and ordinary Error instances use data fields.
    const message = Object.getOwnPropertyDescriptor(error, "message");
    const permanent = Object.getOwnPropertyDescriptor(error, "permanent");
    return { message: typeof message?.value === "string" ? message.value : fallback, permanent: permanent?.value === true };
  } catch { return { message: fallback, permanent: false }; }
}

/** @internal Shared bridge transport. Each native view remains bound to its issuing engine generation. */
export class ScopedClient {
  private root: Fence = { closed: false };
  private subscriptions: Subscription[] = [];
  private handlers = new Map<string, { fence: Fence; owner: Descriptor; handler: CatalogStepHandler }>();
  private handlerNames = new Map<string, string>();
  private registrationTails = new Map<string, Promise<void>>();
  private pendingRegistrations = 0;
  private tokens = new Map<string, { fence: Fence; owner: Descriptor; source: CatalogTokenSource }>();
  private pendingSyncs = 0;
  private accountSessions = new Map<string, { epoch: string; fence: Fence }>();
  constructor(private native: NativeScopedCall, private events: EventSource,
    private resolveStep: (id: string, output: string | null, error: string | null, permanent: boolean) => void,
    private resolveToken: (id: string, token: string | null, error: string | null) => void) {}
  reset(): void {
    this.root.closed = true;
    this.root = { closed: false };
    this.handlers.clear(); this.tokens.clear();
    this.handlerNames.clear();
    this.registrationTails.clear();
    this.accountSessions.clear();
    for (const sub of this.subscriptions) sub.remove();
    this.subscriptions = [];
  }
  private listen(): void {
    if (this.subscriptions.length) return;
    this.subscriptions = [
      this.events.addListener("orch8:scopedExecuteStep", event => { void this.execute(event); }),
      this.events.addListener("orch8:scopedRefreshToken", event => { void this.refresh(event); }),
    ];
  }
  private replyStep(id: string, output: string | null, error: string | null, permanent: boolean): void {
    try { this.resolveStep(id, output, error, permanent); } catch { /* Detached native bridge. */ }
  }
  private replyToken(id: string, token: string | null, error: string | null): void {
    try { this.resolveToken(id, token, error); } catch { /* Detached native bridge. */ }
  }
  private async execute(event: any): Promise<void> {
    const registered = this.handlers.get(event.callbackKey);
    let release: (() => void) | undefined;
    try {
      if (!registered) throw shutdown();
      check(registered.fence);
      const context = owned(event.context, registered.owner);
      if (epoch(context.ownerEpoch) !== registered.owner.sessionEpoch) throw new Error("Callback session mismatch");
      text(context.runId); text(context.blockId); text(context.effectId);
      if (!Number.isInteger(context.attempt) || context.attempt < 0 || context.attempt > 4294967295)
        throw new Error("Invalid callback attempt");
      release = enterHost("handler", registered.owner.accountId);
      const output = await registered.handler(Object.freeze({ ...context }) as CatalogCallbackContext, text(event.stepName), text(event.inputJson));
      check(registered.fence);
      if (typeof output !== "string") throw new TypeError("Catalog handlers must return JSON strings");
      this.replyStep(event.requestId, output, null, false);
    } catch (error) {
      const failure = hostFailure(error, "catalog handler failed");
      this.replyStep(event.requestId, null, failure.message, failure.permanent);
    } finally { release?.(); }
  }
  private async refresh(event: any): Promise<void> {
    const registered = this.tokens.get(event.providerKey);
    let release: (() => void) | undefined;
    try {
      if (!registered) throw shutdown();
      check(registered.fence);
      release = enterHost("token", registered.owner.accountId);
      const token = await registered.source.refreshToken();
      check(registered.fence);
      text(token);
      this.replyToken(event.requestId, token, null);
    } catch (error) { this.replyToken(event.requestId, null, hostFailure(error, "token refresh failed").message); }
    finally { release?.(); }
  }
  async call(fence: Fence, handle: string, method: string, args: object = {}): Promise<any> {
    check(fence);
    const encoded = JSON.stringify(args);
    check(fence); // A host toJSON/getter can reenter root retirement.
    const value: unknown = JSON.parse(await this.native(handle, method, encoded));
    try { check(fence); }
    catch (error) {
      // A close may win while native code is returning a newly issued child.
      // Release that unreachable view rather than leaking its table slot.
      if (value && typeof value === "object") {
        const reply = value as { handle?: unknown; next?: { handle?: unknown } };
        const orphan = reply.handle ?? reply.next?.handle;
        if (typeof orphan === "string") void this.native(orphan, "dispose", "{}").catch(() => {});
      }
      throw error;
    }
    return value;
  }
  async openAccount(identity: AccountIdentity): Promise<Orch8Account> {
    this.listen();
    const fence = this.root;
    const value = descriptor(await this.call(fence, "", "openAccount", identity), "account");
    let session = this.accountSessions.get(value.accountId);
    if (session && session.epoch !== value.sessionEpoch) {
      session.fence.closed = true; this.releaseHandlers(session.fence); session = undefined;
    }
    if (!session) {
      session = { epoch: value.sessionEpoch, fence: { closed: false, parent: fence } };
      this.accountSessions.set(value.accountId, session);
    }
    return new Orch8Account(this, value, { closed: false, parent: session.fence });
  }
  retireAccount(owner: Descriptor): void {
    const session = this.accountSessions.get(owner.accountId);
    if (session?.epoch === owner.sessionEpoch) {
      session.fence.closed = true; this.releaseHandlers(session.fence); this.accountSessions.delete(owner.accountId);
    }
  }
  register(owner: Descriptor, fence: Fence, name: string, handler: CatalogStepHandler, timeoutMs: number): Promise<void> {
    if (this.pendingRegistrations >= 1024) return Promise.reject(Object.assign(new Error("JS registration queue capacity exhausted"), { code: "ORCH8_RESOURCE_LIMIT" }));
    const binding = JSON.stringify([owner.accountId, owner.catalogId, owner.sessionEpoch, name]);
    const previous = this.registrationTails.get(binding) ?? Promise.resolve();
    this.pendingRegistrations++;
    const operation = previous.catch(() => {}).then(() => this.registerNow(owner, fence, name, handler, timeoutMs, binding));
    this.registrationTails.set(binding, operation);
    void operation.finally(() => {
      this.pendingRegistrations--;
      if (this.registrationTails.get(binding) === operation) this.registrationTails.delete(binding);
    }).catch(() => {});
    return operation;
  }
  private async registerNow(owner: Descriptor, fence: Fence, name: string, handler: CatalogStepHandler, timeoutMs: number, binding: string): Promise<void> {
    check(fence);
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) throw new RangeError("timeoutMs must be a positive safe integer");
    const key = callbackKey();
    if (this.handlers.size >= 1024) throw Object.assign(new Error("JS scoped handler capacity exhausted"), { code: "ORCH8_RESOURCE_LIMIT" });
    this.handlers.set(key, { fence, owner, handler });
    try {
      await this.call(fence, owner.handle, "registerHandler", { name, callbackKey: key, timeoutMs });
      const previous = this.handlerNames.get(binding);
      if (previous) this.handlers.delete(previous);
      this.handlerNames.set(binding, key);
    }
    catch (error) { this.handlers.delete(key); throw error; }
  }
  async sync(owner: Descriptor, fence: Fence, source: CatalogTokenSource): Promise<SyncResult> {
    check(fence);
    if (this.pendingSyncs >= 32) throw Object.assign(new Error("JS scoped sync capacity exhausted"), { code: "ORCH8_RESOURCE_LIMIT" });
    const initialToken = source.currentToken();
    if (typeof initialToken !== "string") throw new TypeError("currentToken must return a string");
    const key = callbackKey();
    this.pendingSyncs++;
    this.tokens.set(key, { fence, owner, source });
    try { return syncResult(await this.call(fence, owner.handle, "sync", { providerKey: key, initialToken })); }
    finally { this.tokens.delete(key); this.pendingSyncs--; }
  }
  releaseHandlers(fence: Fence): void {
    for (const [key, value] of this.handlers) {
      let current: Fence | undefined = value.fence;
      while (current) { if (current === fence) { this.handlers.delete(key); break; } current = current.parent; }
    }
    for (const [name, key] of this.handlerNames) { if (!this.handlers.has(key)) this.handlerNames.delete(name); }
  }
}

class ScopedView {
  private disposed = false;
  constructor(protected transport: ScopedClient, protected owner: Descriptor, protected fence: Fence) {}
  get accountId(): string { return this.owner.accountId; }
  get sessionEpoch(): string { return this.owner.sessionEpoch; }
  protected ensureUsable(): void { if (this.disposed) throw shutdown(); check(this.fence); }
  protected call(method: string, args: object = {}): Promise<any> {
    if (this.disposed) return Promise.reject(shutdown());
    return this.transport.call(this.fence, this.owner.handle, method, args);
  }
  /** Fence this view and descendants; keep durable work and effect receipts. */
  async closeView(): Promise<void> {
    if (this.fence.closed) return;
    try { await this.call("closeView"); }
    finally { this.fence.closed = true; this.transport.releaseHandlers(this.fence); }
  }
  /** Release this wrapper only. Existing child views remain usable. */
  async dispose(): Promise<void> {
    if (this.disposed) return;
    // Disposal also works after closeView; it must not use the closed view fence.
    await this.transport.call({ closed: false }, this.owner.handle, "dispose");
    this.disposed = true;
  }
}
export class Orch8Account extends ScopedView {
  async openCatalog(configuration: CatalogConfiguration): Promise<Orch8Catalog> {
    const value = descriptor(await this.call("openCatalog", configuration), "catalog", this.owner);
    return new Orch8Catalog(this.transport, value, { closed: false, parent: this.fence });
  }
  /** Durably retire this exact account session; reopening creates a new epoch. */
  async retire(): Promise<void> {
    try { await this.call("retire"); }
    finally { this.fence.closed = true; this.transport.retireAccount(this.owner); }
  }
}
export class Orch8Catalog extends ScopedView {
  get catalogId(): string { return this.owner.catalogId!; }
  async registerHandler(name: string, handler: CatalogStepHandler, timeoutMs = 30000): Promise<void> {
    this.ensureUsable();
    return this.transport.register(this.owner, this.fence, name, handler, timeoutMs);
  }
  async sync(tokens: CatalogTokenSource): Promise<SyncResult> {
    this.ensureUsable();
    return this.transport.sync(this.owner, this.fence, tokens);
  }
  async prepareRun(request: CatalogRunRequest): Promise<Orch8CatalogRun> {
    const value = descriptor(await this.call("prepareRun", { ...request, inputJson: request.inputJson ?? "{}" }), "run", this.owner);
    return new Orch8CatalogRun(this.transport, value, { closed: false, parent: this.fence });
  }
  async run(instanceId: string): Promise<Orch8CatalogRun> {
    const value = descriptor(await this.call("run", { instanceId }), "run", this.owner);
    return new Orch8CatalogRun(this.transport, value, { closed: false, parent: this.fence });
  }
  async history(cursor: CatalogHistoryCursor | null = null, limit = 64): Promise<CatalogRunPage> {
    if (!Number.isInteger(limit) || limit < 1 || limit > 128) throw new RangeError("limit must be 1..128");
    const value = object(await this.call("history", { cursor: cursor?.reference(this.transport, this.owner) ?? null, limit }));
    if (!Array.isArray(value.items)) throw new Error("Invalid scoped history");
    const items = value.items.map(item => {
      const v = owned(item, this.owner); epoch(v.ownerEpoch);
      const native = object(v.nativeState);
      if (!["prepared", "cancelled", "archived"].includes(v.requestState) ||
          !["not_materialized", "unavailable", "present", "recorded"].includes(native.kind)) throw new Error("Invalid scoped history state");
      if (native.kind === "present" || native.kind === "recorded") state(native.state, native.kind === "recorded");
      if ((v.requestState === "archived" && !["recorded", "unavailable"].includes(native.kind)) ||
          (v.requestState !== "archived" && native.kind === "recorded")) throw new Error("Inconsistent archive history");
      return Object.freeze({ ...v, nativeState: Object.freeze({ ...native }) }) as CatalogRunSummary;
    });
    const next = value.next == null ? null : new CatalogHistoryCursor(this.transport, descriptor(value.next, "cursor", this.owner), { closed: false, parent: this.fence });
    return Object.freeze({ items: Object.freeze(items), next });
  }
}
export class CatalogHistoryCursor extends ScopedView {
  /** @internal Cursors are native-owned and bound to the issuing catalog/session. */
  reference(transport: ScopedClient, owner: Descriptor): string {
    this.ensureUsable();
    if (transport !== this.transport || owner.accountId !== this.owner.accountId || owner.catalogId !== this.owner.catalogId || owner.sessionEpoch !== this.owner.sessionEpoch)
      throw new Error("Cursor belongs to another catalog session");
    return this.owner.handle;
  }
}
export class Orch8CatalogRun extends ScopedView {
  get catalogId(): string { return this.owner.catalogId!; }
  get instanceId(): string { return this.owner.instanceId!; }
  async start(): Promise<void> { await this.call("start"); }
  async snapshot(): Promise<CatalogRunSnapshot | null> {
    const value: unknown = await this.call("snapshot");
    if (value === null) return null;
    const v = owned(value, this.owner, true);
    if (v.sessionEpoch !== this.sessionEpoch) throw new Error("Snapshot session mismatch");
    state(v.state); text(v.updatedAt); text(v.createdAt); text(v.dataJson);
    if (!Array.isArray(v.pendingSteps) || v.pendingSteps.some((s: unknown) => typeof s !== "string")) throw new Error("Invalid pending steps");
    return Object.freeze({ ...v, pendingSteps: Object.freeze([...v.pendingSteps]) }) as CatalogRunSnapshot;
  }
  async cancel(): Promise<InstanceStateKind | null> {
    const value: unknown = await this.call("cancel"); return value === null ? null : state(value);
  }
  async completeStep(expectedUpdatedAt: string, stepName: string, outputJson: string): Promise<boolean> {
    return boolean(await this.call("completeStep", { expectedUpdatedAt, stepName, outputJson }));
  }
  async answer(snapshot: CatalogRunSnapshot, stepName: string, choice: string, data: Record<string, unknown> = {}): Promise<boolean> {
    owned(snapshot, this.owner, true);
    if (snapshot.sessionEpoch !== this.sessionEpoch || !choice.trim() || "value" in data) throw new Error("Invalid answer or snapshot session");
    return this.completeStep(snapshot.updatedAt, stepName, JSON.stringify({ ...data, value: choice }));
  }
  /** Explicit app/backend acknowledgement; the trusted host reference is hashed, not remotely authenticated. */
  async archiveAfterAcknowledgement(acknowledgement: string): Promise<CatalogArchiveReceipt | null> {
    const value: unknown = await this.call("archiveAfterAcknowledgement", { acknowledgement });
    if (value === null) return null;
    const receipt = owned(value, this.owner, true);
    if (typeof receipt.acknowledgementDigest !== "string" || !/^[0-9a-f]{64}$/.test(receipt.acknowledgementDigest)) throw new Error("Invalid acknowledgement digest");
    text(receipt.archivedAt);
    if (receipt.outcome !== null) state(receipt.outcome, true);
    return Object.freeze({ ...receipt }) as CatalogArchiveReceipt;
  }
  async forgetCancelledRequest(): Promise<boolean> { return boolean(await this.call("forgetCancelledRequest")); }
}
