import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { Orch8Worker } from "../worker.js";
import type { WorkerTask } from "../types.js";

function makeTask(overrides: Partial<WorkerTask> = {}): WorkerTask {
  return {
    id: "task-1",
    instance_id: "inst-1",
    block_id: "b-1",
    handler_name: "greet",
    params: { name: "alice" },
    context: {},
    attempt: 0,
    timeout_ms: null,
    state: "claimed",
    worker_id: "w-1",
    claimed_at: "2025-01-01T00:00:00Z",
    heartbeat_at: null,
    completed_at: null,
    output: null,
    error_message: null,
    error_retryable: null,
    created_at: "2025-01-01T00:00:00Z",
    ...overrides,
  };
}

function jsonResponse(body: unknown): Response {
  return {
    ok: true,
    status: 200,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body)),
  } as unknown as Response;
}

/** Stop worker under fake timers by advancing time so the drain loop exits. */
async function stopWorker(worker: Orch8Worker): Promise<void> {
  const p = worker.stop();
  await vi.advanceTimersByTimeAsync(31_000);
  await p;
}

describe("Orch8Worker", () => {
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.useFakeTimers();
    mockFetch = vi.fn().mockResolvedValue(jsonResponse([]));
    vi.stubGlobal("fetch", mockFetch);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("polls for tasks on start and calls handler", async () => {
    const handler = vi.fn().mockResolvedValue({ greeting: "hello" });
    const worker = new Orch8Worker({
      engineUrl: "http://localhost:8080",
      workerId: "w-1",
      handlers: { greet: handler },
      pollIntervalMs: 100,
    });

    mockFetch.mockResolvedValueOnce(jsonResponse([makeTask()]));
    mockFetch.mockResolvedValue(jsonResponse([]));

    await worker.start();
    await vi.advanceTimersByTimeAsync(0);

    expect(handler).toHaveBeenCalledWith(expect.objectContaining({ id: "task-1" }));

    const completeCall = mockFetch.mock.calls.find(
      (c: unknown[]) =>
        typeof c[0] === "string" && c[0].includes("/complete"),
    );
    expect(completeCall).toBeDefined();

    await stopWorker(worker);
  });

  it("calls failTask when handler throws", async () => {
    const handler = vi.fn().mockRejectedValue(new Error("boom"));
    const worker = new Orch8Worker({
      engineUrl: "http://localhost:8080",
      workerId: "w-1",
      handlers: { greet: handler },
      pollIntervalMs: 100,
    });

    mockFetch.mockResolvedValueOnce(jsonResponse([makeTask()]));
    mockFetch.mockResolvedValue(jsonResponse([]));

    await worker.start();
    await vi.advanceTimersByTimeAsync(0);

    const failCall = mockFetch.mock.calls.find(
      (c: unknown[]) => typeof c[0] === "string" && c[0].includes("/fail"),
    );
    expect(failCall).toBeDefined();
    const body = JSON.parse((failCall![1] as RequestInit).body as string);
    expect(body.message).toBe("boom");
    expect(body.retryable).toBe(true);

    await stopWorker(worker);
  });

  it("calls failTask for unregistered handler", async () => {
    const worker = new Orch8Worker({
      engineUrl: "http://localhost:8080",
      workerId: "w-1",
      handlers: { greet: vi.fn() },
      pollIntervalMs: 100,
    });

    // Poll returns a task whose handler_name doesn't match any registered handler.
    mockFetch.mockResolvedValueOnce(
      jsonResponse([makeTask({ handler_name: "unknown" })]),
    );
    mockFetch.mockResolvedValue(jsonResponse([]));

    await worker.start();
    await vi.advanceTimersByTimeAsync(0);

    const failCall = mockFetch.mock.calls.find(
      (c: unknown[]) => typeof c[0] === "string" && c[0].includes("/fail"),
    );
    expect(failCall).toBeDefined();
    const body = JSON.parse((failCall![1] as RequestInit).body as string);
    expect(body.message).toContain("no handler registered");
    expect(body.retryable).toBe(false);

    await stopWorker(worker);
  });

  it("sends heartbeats for in-flight tasks", async () => {
    let resolveHandler!: (value: unknown) => void;
    const handlerPromise = new Promise((r) => {
      resolveHandler = r;
    });
    const handler = vi.fn().mockReturnValue(handlerPromise);
    const worker = new Orch8Worker({
      engineUrl: "http://localhost:8080",
      workerId: "w-1",
      handlers: { greet: handler },
      pollIntervalMs: 100,
      heartbeatIntervalMs: 200,
    });

    mockFetch.mockResolvedValueOnce(jsonResponse([makeTask()]));
    mockFetch.mockResolvedValue(jsonResponse([]));

    await worker.start();
    await vi.advanceTimersByTimeAsync(0);

    // Advance past heartbeat interval.
    await vi.advanceTimersByTimeAsync(250);

    const heartbeatCall = mockFetch.mock.calls.find(
      (c: unknown[]) =>
        typeof c[0] === "string" && c[0].includes("/heartbeat"),
    );
    expect(heartbeatCall).toBeDefined();

    // Resolve handler so drain completes quickly.
    resolveHandler({ done: true });
    await vi.advanceTimersByTimeAsync(0);
    await stopWorker(worker);
  });

  it("respects concurrency limit on subsequent polls", async () => {
    const resolvers: Array<(v: unknown) => void> = [];
    const handler = vi.fn().mockImplementation(
      () => new Promise((r) => resolvers.push(r)),
    );
    const worker = new Orch8Worker({
      engineUrl: "http://localhost:8080",
      workerId: "w-1",
      handlers: { greet: handler },
      pollIntervalMs: 100,
      maxConcurrent: 2,
    });

    mockFetch.mockResolvedValueOnce(
      jsonResponse([
        makeTask({ id: "t1" }),
        makeTask({ id: "t2" }),
        makeTask({ id: "t3" }),
      ]),
    );
    mockFetch.mockResolvedValue(jsonResponse([]));

    await worker.start();
    await vi.advanceTimersByTimeAsync(0);

    // All 3 tasks dispatched to handlers.
    expect(handler).toHaveBeenCalledTimes(3);

    // Semaphore exhausted — next poll should skip.
    await vi.advanceTimersByTimeAsync(150);
    const pollCalls = mockFetch.mock.calls.filter(
      (c: unknown[]) => typeof c[0] === "string" && c[0].includes("/poll"),
    );
    expect(pollCalls.length).toBe(1);

    resolvers.forEach((r) => r({}));
    await vi.advanceTimersByTimeAsync(0);
    await stopWorker(worker);
  });

  it("stop is idempotent and clears timers", async () => {
    const worker = new Orch8Worker({
      engineUrl: "http://localhost:8080",
      workerId: "w-1",
      handlers: { greet: vi.fn() },
      pollIntervalMs: 100,
    });

    await worker.start();
    await stopWorker(worker);
    await stopWorker(worker); // second stop should be safe

    // No further polls after stop.
    const callsBefore = mockFetch.mock.calls.length;
    await vi.advanceTimersByTimeAsync(500);
    expect(mockFetch.mock.calls.length).toBe(callsBefore);
  });
});
