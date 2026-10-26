import { describe, expect, it, vi } from "vitest";
import { ScopedClient, type CatalogCallbackContext } from "../scoped";
import type { EventSource } from "../core";

function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>(r => { resolve = r; }); return { promise, resolve }; }
const configuration = { catalogKey: "one", source: "source", signingAuthority: "key", manifestUrl: "https://example/manifest", identityUrl: "https://example/identity" };
function setup() {
  const listeners = new Map<string, (event: any) => void>();
  const events: EventSource = { addListener(name, callback) { listeners.set(name, callback); return { remove: () => { listeners.delete(name); } }; } };
  let id = 0, sessionEpoch = "9007199254740993";
  const views = new Map<string, any>();
  const registrations: any[] = [], providers: any[] = [];
  const resolveStep = vi.fn(), resolveToken = vi.fn();
  let intercept: ((method: string, args: any) => Promise<any> | undefined) | undefined;
  const native = vi.fn(async (handle: string, method: string, json: string): Promise<string> => {
    const args = JSON.parse(json), parent = views.get(handle);
    let value: any = null;
    const custom = intercept?.(method, args);
    if (custom) {
      const resolved = await custom;
      return JSON.stringify(method === "sync" && resolved == null ? { added: 0, updated: 0, removed: 0, skipped: 0, signatureFailures: 0 } : resolved);
    }
    if (["openAccount", "openCatalog", "run", "prepareRun"].includes(method)) {
      const kind = method === "openAccount" ? "account" : method === "openCatalog" ? "catalog" : "run";
      value = { handle: `h${++id}`, kind, accountId: parent?.accountId ?? args.accountKey, sessionEpoch: parent?.sessionEpoch ?? sessionEpoch };
      if (kind !== "account") value.catalogId = kind === "catalog" ? `${parent.accountId}:${args.catalogKey}` : parent.catalogId;
      if (kind === "run") value.instanceId = args.instanceId ?? `run${id}`;
      views.set(value.handle, value);
    } else if (method === "registerHandler") { registrations.push({ ...args, owner: parent }); }
    else if (method === "sync") { providers.push(args); value = { added: 0, updated: 0, removed: 0, skipped: 0, signatureFailures: 0 }; }
    else if (method === "history") value = { items: [], next: null };
    else if (method === "completeStep") value = true;
    return JSON.stringify(value);
  });
  const client = new ScopedClient(native, events, resolveStep, resolveToken);
  const account = (name = "alice") => client.openAccount({ accountKey: name, issuer: "issuer", subject: name });
  const emit = (name: string, value: any) => listeners.get(`orch8:${name}`)?.(value);
  const callback = (registration: any, overrides: object = {}) => ({ callbackKey: registration.callbackKey, requestId: "request", stepName: "step", inputJson: "{}", context: { accountId: registration.owner.accountId, catalogId: registration.owner.catalogId, ownerEpoch: registration.owner.sessionEpoch, runId: "run", blockId: "block", attempt: 0, effectId: "effect", ...overrides } });
  return { client, native, account, registrations, providers, resolveStep, resolveToken, emit, callback, intercept: (fn: typeof intercept) => { intercept = fn; }, epoch: (value: string) => { sessionEpoch = value; } };
}
const flush = async () => { for (let i = 0; i < 8; i++) await Promise.resolve(); };

describe("scoped RN views", () => {
  it("preserves 64-bit epochs and independent same-named catalog callbacks", async () => {
    const s = setup();
    const alice = await s.account(), bob = await s.account("bob");
    expect(alice.sessionEpoch).toBe("9007199254740993");
    const one = await alice.openCatalog(configuration), two = await bob.openCatalog(configuration);
    const calls: CatalogCallbackContext[] = [];
    await one.registerHandler("same", context => { calls.push(context); return "{}"; });
    await two.registerHandler("same", context => { calls.push(context); return "{}"; });
    s.emit("scopedExecuteStep", s.callback(s.registrations[0]));
    s.emit("scopedExecuteStep", s.callback(s.registrations[1]));
    await flush();
    expect(calls.map(c => c.accountId)).toEqual(["alice", "bob"]);
    expect(calls[0].ownerEpoch).toBe("9007199254740993");
    expect(Object.isFrozen(calls[0])).toBe(true);
  });
  it("rejects cross-owner and lossy numeric callback identity before host entry", async () => {
    const s = setup(), handler = vi.fn(() => "{}");
    const catalog = await (await s.account()).openCatalog(configuration);
    await catalog.registerHandler("handler", handler);
    s.emit("scopedExecuteStep", s.callback(s.registrations[0], { accountId: "bob" }));
    s.emit("scopedExecuteStep", s.callback(s.registrations[0], { ownerEpoch: 9007199254740992 }));
    await flush();
    expect(handler).not.toHaveBeenCalled();
    expect(s.resolveStep).toHaveBeenCalledTimes(2);
    expect(s.resolveStep.mock.calls.every(call => call[1] === null && typeof call[2] === "string")).toBe(true);
  });
  it("retiring one account view fences sibling views and late handler answers", async () => {
    const s = setup();
    const first = await s.account(), sibling = await s.account();
    const catalog = await sibling.openCatalog(configuration), waiting = deferred<string>();
    await catalog.registerHandler("handler", () => waiting.promise);
    s.emit("scopedExecuteStep", s.callback(s.registrations[0]));
    await first.retire();
    waiting.resolve("{\"secret\":true}"); await flush();
    expect(s.resolveStep.mock.calls[0][1]).toBeNull();
    await expect(catalog.run("run")).rejects.toMatchObject({ code: "ORCH8_SHUTDOWN" });
    s.epoch("9007199254740994");
    expect((await s.account()).sessionEpoch).toBe("9007199254740994");
  });
  it("disposes temporary parents without closing children; closeView fences descendants", async () => {
    const s = setup(), account = await s.account();
    const catalog = await account.openCatalog(configuration), run = await catalog.run("run");
    await account.dispose(); await catalog.dispose();
    await expect(run.start()).resolves.toBeUndefined();
    const other = await (await s.account()).openCatalog(configuration), child = await other.run("child");
    await other.closeView();
    await expect(child.start()).rejects.toMatchObject({ code: "ORCH8_SHUTDOWN" });
    const source = { currentToken: vi.fn(() => "token"), refreshToken: vi.fn() };
    await expect(catalog.sync(source)).rejects.toMatchObject({ code: "ORCH8_SHUTDOWN" }); expect(source.currentToken).not.toHaveBeenCalled();
  });
  it("serializes handler replacement and refuses queued deliveries to superseded handlers", async () => {
    const s = setup(), catalog = await (await s.account()).openCatalog(configuration);
    const first = vi.fn(() => "{}"), second = vi.fn(() => "{}");
    await Promise.all([catalog.registerHandler("same", first), catalog.registerHandler("same", second)]);
    s.emit("scopedExecuteStep", s.callback(s.registrations[0]));
    s.emit("scopedExecuteStep", s.callback(s.registrations[1])); await flush();
    expect(first).not.toHaveBeenCalled(); expect(second).toHaveBeenCalledOnce();
  });
  it("bounds concurrent registration requests before native admission", async () => {
    const s = setup(), catalog = await (await s.account()).openCatalog(configuration), gate = deferred<null>();
    s.intercept(method => method === "registerHandler" ? gate.promise : undefined);
    const pending = Array.from({ length: 1024 }, (_, i) => catalog.registerHandler(`h${i}`, () => "{}"));
    await expect(catalog.registerHandler("overflow", () => "{}")).rejects.toMatchObject({ code: "ORCH8_RESOURCE_LIMIT" });
    gate.resolve(null); await Promise.all(pending);
  });
  it("routes overlapping token refreshes per operation and fences late replies on reset", async () => {
    const s = setup(), catalog = await (await s.account()).openCatalog(configuration);
    const sync = deferred<null>(), token = deferred<string>();
    const sources = [{ currentToken: () => "alice", refreshToken: vi.fn(() => token.promise) }, { currentToken: () => "bob", refreshToken: vi.fn(async () => "bob-fresh") }];
    const keys: string[] = [];
    s.intercept((method, args) => { if (method !== "sync") return; keys.push(args.providerKey); return sync.promise; });
    const operations = sources.map(source => catalog.sync(source).catch(error => error));
    await flush();
    s.emit("scopedRefreshToken", { providerKey: keys[0], requestId: "a" });
    s.emit("scopedRefreshToken", { providerKey: keys[1], requestId: "b" }); await flush();
    expect(s.resolveToken).toHaveBeenCalledWith("b", "bob-fresh", null);
    s.client.reset(); token.resolve("alice-late"); sync.resolve(null); await Promise.all(operations); await flush();
    expect(s.resolveToken).toHaveBeenCalledWith("a", null, "scope view retired");
  });
  it("keeps hung JS handler admission through reset while another account recovers", async () => {
    const s = setup(), old = await (await s.account()).openCatalog(configuration), gate = deferred<string>();
    const blocked = vi.fn(() => gate.promise);
    await old.registerHandler("hung", blocked);
    const oldRegistration = s.registrations[0];
    s.emit("scopedExecuteStep", s.callback(oldRegistration));
    s.emit("scopedExecuteStep", { ...s.callback(oldRegistration), requestId: "second" });
    s.client.reset();
    const replacement = await (await s.account()).openCatalog(configuration), refused = vi.fn(() => "{}");
    await replacement.registerHandler("new", refused);
    s.emit("scopedExecuteStep", s.callback(s.registrations[1])); await flush();
    expect(blocked).toHaveBeenCalledTimes(2); expect(refused).not.toHaveBeenCalled();
    expect(s.resolveStep).toHaveBeenCalledWith("request", null, "JS scoped handler capacity exhausted", false);
    const bob = await (await s.account("bob")).openCatalog(configuration), healthy = vi.fn(() => "{}");
    await bob.registerHandler("healthy", healthy);
    s.emit("scopedExecuteStep", s.callback(s.registrations[2])); await flush();
    expect(healthy).toHaveBeenCalledOnce();
    gate.resolve("{}"); await flush();
    s.emit("scopedExecuteStep", s.callback(s.registrations[1])); await flush();
    expect(refused).toHaveBeenCalledOnce();
  });
  it("retains host token admission after the native sync has returned", async () => {
    const s = setup(), catalog = await (await s.account()).openCatalog(configuration);
    const operation = deferred<null>(), token = deferred<string>(), keys: string[] = [];
    s.intercept((method, args) => { if (method !== "sync") return; keys.push(args.providerKey); return operation.promise; });
    const source = { currentToken: () => "token", refreshToken: vi.fn(() => token.promise) };
    const first = catalog.sync(source), second = catalog.sync(source); await flush();
    s.emit("scopedRefreshToken", { providerKey: keys[0], requestId: "a" });
    s.emit("scopedRefreshToken", { providerKey: keys[1], requestId: "b" });
    operation.resolve(null); await Promise.all([first, second]);
    const freshOperation = deferred<null>();
    s.intercept((method, args) => { if (method !== "sync") return; keys.push(args.providerKey); return freshOperation.promise; });
    const third = catalog.sync(source); await flush();
    s.emit("scopedRefreshToken", { providerKey: keys[2], requestId: "c" }); await flush();
    expect(source.refreshToken).toHaveBeenCalledTimes(2);
    expect(s.resolveToken).toHaveBeenCalledWith("c", null, "JS scoped token capacity exhausted");
    token.resolve("fresh"); freshOperation.resolve(null); await third; await flush();
  });
  it("does not let a stranded old registration block the replacement generation", async () => {
    const s = setup(), catalog = await (await s.account()).openCatalog(configuration), old = deferred<null>();
    s.intercept(method => method === "registerHandler" ? old.promise : undefined);
    const obsolete = catalog.registerHandler("same", () => "{}").catch(error => error); await flush();
    s.client.reset(); s.intercept(undefined);
    const replacement = await (await s.account()).openCatalog(configuration);
    await replacement.registerHandler("same", () => "{}");
    old.resolve(null);
    expect(await obsolete).toMatchObject({ code: "ORCH8_SHUTDOWN" });
  });
  it("enforces the real 32-call module ceiling across distinct clients", async () => {
    const s = setup(), gate = deferred<string>();
    for (let index = 0; index < 16; index++) {
      const catalog = await (await s.account(`capacity-${index}`)).openCatalog(configuration);
      await catalog.registerHandler("hung", () => gate.promise);
      const registration = s.registrations[index];
      s.emit("scopedExecuteStep", s.callback(registration));
      s.emit("scopedExecuteStep", { ...s.callback(registration), requestId: `second-${index}` });
    }
    const other = setup(), catalog = await (await other.account("capacity-17")).openCatalog(configuration), host = vi.fn(() => "{}");
    await catalog.registerHandler("healthy", host);
    other.emit("scopedExecuteStep", other.callback(other.registrations[0])); await flush();
    expect(host).not.toHaveBeenCalled();
    expect(other.resolveStep).toHaveBeenCalledWith("request", null, "JS scoped handler capacity exhausted", false);
    gate.resolve("{}"); await flush();
    other.emit("scopedExecuteStep", other.callback(other.registrations[0])); await flush();
    expect(host).toHaveBeenCalledOnce();
  });
  it("releases a native child issued after its parent has closed", async () => {
    const s = setup(), account = await s.account(), reply = deferred<any>();
    s.intercept(method => method === "openCatalog" ? reply.promise : undefined);
    const child = account.openCatalog(configuration).catch(error => error); await flush();
    await account.closeView();
    reply.resolve({ handle: "orphan", kind: "catalog", accountId: "alice", catalogId: "one", sessionEpoch: account.sessionEpoch });
    expect(await child).toMatchObject({ code: "ORCH8_SHUTDOWN" }); await flush();
    expect(s.native).toHaveBeenCalledWith("orphan", "dispose", "{}");
  });
  it("does not call hostile error accessors while settling a failed host callback", async () => {
    const s = setup(), catalog = await (await s.account()).openCatalog(configuration);
    const bad = new Error("ignored"), getter = vi.fn(() => { throw new Error("must not run"); });
    Object.defineProperty(bad, "message", { get: getter });
    Object.defineProperty(bad, "permanent", { get: getter });
    await catalog.registerHandler("bad", () => { throw bad; });
    s.emit("scopedExecuteStep", s.callback(s.registrations[0])); await flush();
    expect(getter).not.toHaveBeenCalled();
    expect(s.resolveStep).toHaveBeenCalledWith("request", null, "catalog handler failed", false);
  });
  it("an old retirement completion cannot close the replacement account epoch", async () => {
    const s = setup(), old = await s.account(), reply = deferred<null>();
    s.intercept(method => method === "retire" ? reply.promise : undefined);
    const retired = old.retire().catch(error => error); await flush();
    s.epoch("9007199254740994");
    const fresh = await s.account();
    reply.resolve(null); await retired;
    await expect(fresh.openCatalog(configuration)).resolves.toMatchObject({ sessionEpoch: "9007199254740994" });
  });
  it("rechecks retirement after host JSON serialization before native dispatch", async () => {
    const s = setup();
    const identity = { accountKey: "alice", issuer: "issuer", subject: "alice", toJSON() {
      s.client.reset(); return { accountKey: "alice", issuer: "issuer", subject: "alice" };
    } };
    await expect(s.client.openAccount(identity)).rejects.toMatchObject({ code: "ORCH8_SHUTDOWN" });
    expect(s.native).not.toHaveBeenCalled();
  });
  it("refuses cursor reuse across accounts and returns exact answer/archive requests", async () => {
    const s = setup(), a = await (await s.account()).openCatalog(configuration), b = await (await s.account("bob")).openCatalog(configuration);
    s.intercept(method => method === "history" ? Promise.resolve({ items: [], next: { handle: "cursor", kind: "cursor", accountId: "alice", catalogId: "alice:one", sessionEpoch: "9007199254740993" } }) : undefined);
    const page = await a.history();
    await expect(b.history(page.next)).rejects.toThrow("another catalog session");
    const run = await a.run("run");
    const snapshot = { instanceId: "run", accountId: "alice", catalogId: "alice:one", sessionEpoch: "9007199254740993", state: "waiting" as const, dataJson: "{}", pendingSteps: ["approval"], createdAt: "created", updatedAt: "exact-version" };
    expect(await run.answer(snapshot, "approval", "yes", { note: "ok" })).toBe(true);
    expect(s.native.mock.calls.at(-1)?.slice(1)).toEqual(["completeStep", JSON.stringify({ expectedUpdatedAt: "exact-version", stepName: "approval", outputJson: '{"note":"ok","value":"yes"}' })]);
    await run.archiveAfterAcknowledgement("backend-receipt");
    expect(JSON.parse(s.native.mock.calls.at(-1)![2])).toEqual({ acknowledgement: "backend-receipt" });
  });
});
