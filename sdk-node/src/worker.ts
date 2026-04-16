import type { WorkerTask } from "./types.js";

export type HandlerFn = (task: WorkerTask) => Promise<unknown>;

export interface WorkerConfig {
  /** Base URL of the Orch8 engine API (e.g. "http://localhost:8080"). */
  engineUrl: string;
  /** Unique identifier for this worker instance. */
  workerId: string;
  /** Map of handler names to async handler functions. */
  handlers: Record<string, HandlerFn>;
  /** How often to poll for new tasks (ms). Default: 1000. */
  pollIntervalMs?: number;
  /** How often to send heartbeats for in-flight tasks (ms). Default: 15000. */
  heartbeatIntervalMs?: number;
  /** Maximum concurrent tasks per handler. Default: 10. */
  maxConcurrent?: number;
}

export class Orch8Worker {
  private readonly config: Required<
    Pick<
      WorkerConfig,
      "engineUrl" | "workerId" | "pollIntervalMs" | "heartbeatIntervalMs" | "maxConcurrent"
    >
  > & { handlers: Record<string, HandlerFn> };

  private running = false;
  private pollTimers: NodeJS.Timeout[] = [];
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private inFlightTasks = new Map<string, WorkerTask>();
  private concurrencySemaphore: number;

  constructor(config: WorkerConfig) {
    this.config = {
      engineUrl: config.engineUrl.replace(/\/$/, ""),
      workerId: config.workerId,
      handlers: config.handlers,
      pollIntervalMs: config.pollIntervalMs ?? 1000,
      heartbeatIntervalMs: config.heartbeatIntervalMs ?? 15000,
      maxConcurrent: config.maxConcurrent ?? 10,
    };
    this.concurrencySemaphore = this.config.maxConcurrent;
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;

    // Start a poll loop per handler.
    for (const handlerName of Object.keys(this.config.handlers)) {
      const timer = setInterval(() => {
        void this.poll(handlerName);
      }, this.config.pollIntervalMs);
      this.pollTimers.push(timer);
      // Immediate first poll.
      void this.poll(handlerName);
    }

    // Start heartbeat loop.
    this.heartbeatTimer = setInterval(() => {
      void this.sendHeartbeats();
    }, this.config.heartbeatIntervalMs);
  }

  async stop(): Promise<void> {
    this.running = false;
    for (const timer of this.pollTimers) clearInterval(timer);
    this.pollTimers = [];
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    // Wait for in-flight tasks to drain (with timeout).
    const deadline = Date.now() + 30_000;
    while (this.inFlightTasks.size > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 200));
    }
  }

  private async poll(handlerName: string): Promise<void> {
    if (!this.running || this.concurrencySemaphore <= 0) return;

    try {
      const limit = Math.min(this.concurrencySemaphore, this.config.maxConcurrent);
      const res = await fetch(`${this.config.engineUrl}/workers/tasks/poll`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          handler_name: handlerName,
          worker_id: this.config.workerId,
          limit,
        }),
      });

      if (!res.ok) return;

      const tasks: WorkerTask[] = await res.json() as WorkerTask[];
      for (const task of tasks) {
        this.concurrencySemaphore--;
        this.inFlightTasks.set(task.id, task);
        void this.executeTask(task);
      }
    } catch {
      // Network error — retry on next poll.
    }
  }

  private async executeTask(task: WorkerTask): Promise<void> {
    const handler = this.config.handlers[task.handler_name];
    if (!handler) {
      await this.failTask(task.id, `no handler registered for "${task.handler_name}"`, false);
      return;
    }

    try {
      const output = await this.withTimeout(handler(task), task.timeout_ms);
      await this.completeTask(task.id, output);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.failTask(task.id, message, true);
    } finally {
      this.inFlightTasks.delete(task.id);
      this.concurrencySemaphore++;
    }
  }

  private async withTimeout<T>(promise: Promise<T>, timeoutMs: number | null): Promise<T> {
    if (!timeoutMs) return promise;
    let timer: NodeJS.Timeout;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("task timed out")), timeoutMs);
    });
    try {
      return await Promise.race([promise, timeout]);
    } finally {
      clearTimeout(timer!);
    }
  }

  private async completeTask(taskId: string, output: unknown): Promise<void> {
    try {
      await fetch(`${this.config.engineUrl}/workers/tasks/${taskId}/complete`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          worker_id: this.config.workerId,
          output: output ?? {},
        }),
      });
    } catch {
      // Will be reaped and retried.
    }
  }

  private async failTask(taskId: string, message: string, retryable: boolean): Promise<void> {
    try {
      await fetch(`${this.config.engineUrl}/workers/tasks/${taskId}/fail`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          worker_id: this.config.workerId,
          message,
          retryable,
        }),
      });
    } catch {
      // Will be reaped and retried.
    }
  }

  private async sendHeartbeats(): Promise<void> {
    for (const taskId of this.inFlightTasks.keys()) {
      try {
        await fetch(`${this.config.engineUrl}/workers/tasks/${taskId}/heartbeat`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ worker_id: this.config.workerId }),
        });
      } catch {
        // Best-effort.
      }
    }
  }
}
