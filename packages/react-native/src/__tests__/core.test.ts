import { describe, expect, it, vi } from "vitest";
import {
  EVENTS,
  Orch8Client,
  PermanentHandlerError,
  parseTaskContext,
  type EventSource,
  type NativeOrch8,
} from "../core";

function setup() {
  const listeners = new Map<string, Set<(e: any) => void>>();
  const events: EventSource = {
    addListener(name, fn) {
      if (!listeners.has(name)) listeners.set(name, new Set());
      listeners.get(name)!.add(fn);
      return { remove: () => listeners.get(name)!.delete(fn) };
    },
  };
  const emit = (name: string, payload: unknown) => listeners.get(name)?.forEach((fn) => fn(payload));
  const native = new Proxy({} as Record<string, any>, {
    get(target, prop: string) {
      if (!(prop in target)) target[prop] = vi.fn(async () => undefined);
      return target[prop];
    },
  }) as unknown as Record<keyof NativeOrch8, ReturnType<typeof vi.fn>>;
  const client = new Orch8Client(native as unknown as NativeOrch8, events);
  return { client, native, emit, listeners };
}

const flush = () => new Promise((r) => setTimeout(r, 0));

describe("parseTaskContext", () => {
  it("maps __orch8 snake_case fields to camelCase", () => {
    const input = JSON.stringify({
      document: "passport",
      __orch8: {
        effect_id: "eff-1",
        task_id: "t-1",
        instance_id: "i-1",
        block_id: "b-1",
        attempt: 2,
        runtime_id: "r-1",
        continuity_epoch: 3,
        resume_checkpoint: null,
      },
    });
    expect(parseTaskContext(input)).toEqual({
      effectId: "eff-1",
      taskId: "t-1",
      instanceId: "i-1",
      blockId: "b-1",
      attempt: 2,
      runtimeId: "r-1",
      continuityEpoch: 3,
      resumeCheckpoint: null,
    });
  });

  it("keeps a null effect_id from pre-contract servers", () => {
    expect(parseTaskContext('{"__orch8":{"effect_id":null,"task_id":"t"}}')?.effectId).toBeNull();
  });

  it("returns null for local steps, arrays and invalid JSON", () => {
    expect(parseTaskContext('{"a":1}')).toBeNull();
    expect(parseTaskContext("[1,2]")).toBeNull();
    expect(parseTaskContext("not json")).toBeNull();
    expect(parseTaskContext('{"__orch8":"x"}')).toBeNull();
  });
});

describe("handler dispatch", () => {
  it("passes the parsed task context and resolves JSON output", async () => {
    const { client, native, emit } = setup();
    await client.initialize();
    const handler = vi.fn(async (_s: string, _i: string, ctx: any) => ({ key: ctx.task.effectId }));
    await client.registerHandler("scan", handler);
    expect(native.registerHandler).toHaveBeenCalledWith("scan");

    emit(EVENTS.executeStep, {
      requestId: "req-1",
      stepName: "scan",
      input: '{"__orch8":{"effect_id":"eff-9"}}',
    });
    await flush();

    expect(handler.mock.calls[0]![2].task.effectId).toBe("eff-9");
    expect(native.resolveStep).toHaveBeenCalledWith("req-1", '{"key":"eff-9"}', null, false);
  });

  it("passes string output through and maps undefined to {}", async () => {
    const { client, native, emit } = setup();
    await client.initialize();
    await client.registerHandler("a", () => '{"x":1}');
    await client.registerHandler("b", () => undefined);
    emit(EVENTS.executeStep, { requestId: "1", stepName: "a", input: "{}" });
    emit(EVENTS.executeStep, { requestId: "2", stepName: "b", input: "{}" });
    await flush();
    expect(native.resolveStep).toHaveBeenCalledWith("1", '{"x":1}', null, false);
    expect(native.resolveStep).toHaveBeenCalledWith("2", "{}", null, false);
  });

  it("reports retryable and permanent failures", async () => {
    const { client, native, emit } = setup();
    await client.initialize();
    await client.registerHandler("flaky", () => {
      throw new Error("try again");
    });
    await client.registerHandler("bad", () => {
      throw new PermanentHandlerError("invalid document");
    });
    emit(EVENTS.executeStep, { requestId: "1", stepName: "flaky", input: "{}" });
    emit(EVENTS.executeStep, { requestId: "2", stepName: "bad", input: "{}" });
    await flush();
    expect(native.resolveStep).toHaveBeenCalledWith("1", null, "try again", false);
    expect(native.resolveStep).toHaveBeenCalledWith("2", null, "invalid document", true);
  });

  it("answers unknown handlers with a retryable error", async () => {
    const { client, native, emit } = setup();
    await client.initialize();
    emit(EVENTS.executeStep, { requestId: "1", stepName: "missing", input: "{}" });
    await flush();
    expect(native.resolveStep).toHaveBeenCalledWith(
      "1",
      null,
      "No handler registered for step 'missing'",
      false
    );
  });

  it("re-initialising does not double-dispatch; shutdown unsubscribes", async () => {
    const { client, native, emit, listeners } = setup();
    await client.initialize();
    await client.initialize();
    expect(listeners.get(EVENTS.executeStep)!.size).toBe(1);
    await client.shutdown();
    expect(listeners.get(EVENTS.executeStep)!.size).toBe(0);
    emit(EVENTS.executeStep, { requestId: "1", stepName: "x", input: "{}" });
    await flush();
    expect(native.resolveStep).not.toHaveBeenCalled();
  });
});

describe("runtime node / worker API", () => {
  it("forwards registerNode, worker and builtin calls", async () => {
    const { client, native } = setup();
    native.registerNode.mockResolvedValue({
      runtimeId: "r",
      deviceId: "d",
      handlers: ["scan"],
      expiresAt: "2026-01-01T00:00:00Z",
    });
    const reg = await client.registerNode({ hardware: ["camera"], connectivity: "wifi", batteryPercent: 80 });
    expect(reg.runtimeId).toBe("r");
    expect(native.registerNode).toHaveBeenCalledWith({ hardware: ["camera"], connectivity: "wifi", batteryPercent: 80 });

    await client.startWorker({ maxConcurrentTasks: 2 });
    expect(native.startWorker).toHaveBeenCalledWith({ maxConcurrentTasks: 2 });
    await client.startWorker();
    expect(native.startWorker).toHaveBeenLastCalledWith({});

    await client.runWorkerWindow(20_000);
    expect(native.runWorkerWindow).toHaveBeenCalledWith(20_000);

    await client.updateNodeStatus("metered");
    expect(native.updateNodeStatus).toHaveBeenCalledWith("metered", null);

    await client.stopWorker();
    await client.unregisterNode();
    await client.workerStats();
    await client.nodeRuntimeId();
    await client.enableBuiltin("http_request");
    expect(native.stopWorker).toHaveBeenCalled();
    expect(native.unregisterNode).toHaveBeenCalled();
    expect(native.workerStats).toHaveBeenCalled();
    expect(native.nodeRuntimeId).toHaveBeenCalled();
    expect(native.enableBuiltin).toHaveBeenCalledWith("http_request");
  });

  it("validates numeric arguments before crossing the bridge", async () => {
    const { client, native } = setup();
    await expect(client.registerNode({ batteryPercent: 101 })).rejects.toThrow(RangeError);
    await expect(client.updateNodeStatus(undefined, -1)).rejects.toThrow(RangeError);
    await expect(client.runWorkerWindow(0)).rejects.toThrow(RangeError);
    await expect(client.startWorker({ maxConcurrentTasks: 1.5 })).rejects.toThrow(RangeError);
    await expect(client.runUntilIdle(0, 1000)).rejects.toThrow(RangeError);
    expect(native.registerNode).not.toHaveBeenCalled();
    expect(native.runWorkerWindow).not.toHaveBeenCalled();
  });

  it("reduces push payloads to the id-only wake envelope", async () => {
    const { client, native } = setup();
    native.onPushWake.mockResolvedValue(true);
    await expect(
      client.onPushWake({ orch8: { task_id: "t", runtime_id: "r", params: { secret: 1 } }, aps: {} })
    ).resolves.toBe(true);
    expect(native.onPushWake).toHaveBeenCalledWith('{"task_id":"t","runtime_id":"r"}');

    await client.onPushWake({ reason: "work" });
    expect(native.onPushWake).toHaveBeenLastCalledWith('{"reason":"work"}');

    await expect(client.onPushWake({ aps: { alert: "hi" } })).resolves.toBe(false);
    expect(native.onPushWake).toHaveBeenCalledTimes(2);

    await client.onPushWake('{"task_id":"x"}');
    expect(native.onPushWake).toHaveBeenLastCalledWith('{"task_id":"x"}');
  });
});

describe("engine calls", () => {
  it("serialises object input and passes null for absent optionals", async () => {
    const { client, native } = setup();
    await client.start("seq", { a: 1 });
    expect(native.start).toHaveBeenCalledWith("seq", '{"a":1}', null);
    await client.start("seq", "{}", "dedup");
    expect(native.start).toHaveBeenLastCalledWith("seq", "{}", "dedup");
    await client.sync("https://x/manifest");
    expect(native.sync).toHaveBeenCalledWith("https://x/manifest", null);
    await client.sync("https://x/manifest", "tok");
    expect(native.sync).toHaveBeenLastCalledWith("https://x/manifest", "tok");
    await client.completeStep("i", "s", { value: "yes" });
    expect(native.completeStep).toHaveBeenCalledWith("i", "s", '{"value":"yes"}');
    await client.loadSequencesFromUrl();
    expect(native.loadSequencesFromUrl).toHaveBeenCalledWith("");
  });
});
