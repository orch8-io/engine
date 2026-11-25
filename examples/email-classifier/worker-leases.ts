import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";

type Task = {
  id: string;
  claim_epoch: number;
  lease_secs?: number;
  timeout_ms?: number;
  params: unknown;
};

type PollReply = {
  tasks: Task[];
  lease_secs: number;
  poll_after_ms?: number;
};

type Handler = (params: unknown, signal: AbortSignal) => Promise<unknown>;

class LeaseLost extends Error {}

export async function runWorker(engineUrl: string, handlers: Record<string, Handler>) {
  const address = engineUrl.replace(/\/+$/, "");
  const baseUrl = address.endsWith("/api/v1") ? address : `${address}/api/v1`;
  const workerId = process.env.ORCH8_WORKER_ID || `email-classifier-${randomUUID()}`;
  const headers: Record<string, string> = { "content-type": "application/json", "x-tenant-id": "example" };
  if (process.env.ORCH8_API_KEY) headers["x-api-key"] = process.env.ORCH8_API_KEY;
  const draining = new AbortController();

  async function post(path: string, body: object, heartbeatStop?: AbortSignal): Promise<Response> {
    const timeout = AbortSignal.timeout(10_000);
    const response = await fetch(baseUrl + path, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
      redirect: "error",
      signal: heartbeatStop ? AbortSignal.any([timeout, heartbeatStop]) : timeout,
    });
    if (response.status === 409) throw new LeaseLost(`Lease changed for ${path}`);
    if (!response.ok) throw new Error(`${path} failed with HTTP ${response.status}`);
    return response;
  }

  async function execute(task: Task, handler: Handler, leaseSeconds: number) {
    const path = `/workers/tasks/${encodeURIComponent(task.id)}`;
    const claim = { worker_id: workerId, claim_epoch: task.claim_epoch };
    const stop = new AbortController();
    const lease = new AbortController();
    const deadline = task.timeout_ms ? AbortSignal.timeout(task.timeout_ms) : undefined;
    const signal = deadline ? AbortSignal.any([lease.signal, deadline]) : lease.signal;

    async function renew() {
      const cadence = Math.max(100, Math.min(15_000, (task.lease_secs ?? leaseSeconds) * 1000 / 3));
      try {
        while (!stop.signal.aborted) {
          await sleep(cadence, undefined, { signal: stop.signal });
          try {
            await post(path + "/heartbeat", claim, stop.signal);
          } catch (error) {
            if (error instanceof LeaseLost) {
              lease.abort(error);
              return;
            }
            if (!stop.signal.aborted) console.warn("Worker heartbeat failed:", error);
          }
        }
      } catch (error) {
        if (!stop.signal.aborted) throw error;
      }
    }

    const heartbeat = renew();
    try {
      let report: { path: string; body: object };
      try {
        const output = await handler(task.params, signal);
        signal.throwIfAborted();
        report = { path: path + "/complete", body: { ...claim, output } };
      } catch (error) {
        if (lease.signal.aborted) return;
        const message = error instanceof Error ? error.message : String(error);
        report = { path: path + "/fail", body: { ...claim, message, retryable: false } };
      }
      await post(report.path, report.body);
    } finally {
      stop.abort();
      await heartbeat;
    }
  }

  async function serve(name: string, handler: Handler) {
    while (!draining.signal.aborted) {
      let delay = 1000;
      try {
        const response = await post("/workers/tasks/poll", {
          worker_id: workerId,
          handler_name: name,
          limit: 1,
        });
        const reply = await response.json() as PollReply;
        for (const task of reply.tasks) {
          await execute(task, handler, reply.lease_secs);
        }
        delay = reply.tasks.length ? 0 : Math.max(250, reply.poll_after_ms ?? 0);
      } catch (error) {
        console.warn("Worker request failed:", error);
      }
      if (delay > 0 && !draining.signal.aborted) {
        try {
          await sleep(delay, undefined, { signal: draining.signal });
        } catch (error) {
          if (!draining.signal.aborted) throw error;
        }
      }
    }
  }

  function stop() {
    draining.abort();
  }

  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  try {
    console.log(`Worker ${workerId} polling ${baseUrl} for ${Object.keys(handlers).join(", ")}`);
    await Promise.all(Object.entries(handlers).map(([name, handler]) => serve(name, handler)));
  } finally {
    process.off("SIGINT", stop);
    process.off("SIGTERM", stop);
  }
}
