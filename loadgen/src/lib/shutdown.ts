/**
 * Shared shutdown signal. Roles poll `isShuttingDown()` / await
 * `onShutdown()` to exit their loops.
 */

type Listener = () => void;

let shuttingDown = false;
const listeners: Listener[] = [];

export function requestShutdown(reason: string): void {
  if (shuttingDown) return;
  shuttingDown = true;
  process.stderr.write(`[loadgen] shutdown requested: ${reason}\n`);
  for (const l of listeners) {
    try {
      l();
    } catch {
      // listener errors must not block shutdown
    }
  }
}

export function isShuttingDown(): boolean {
  return shuttingDown;
}

export function onShutdown(listener: Listener): void {
  listeners.push(listener);
}

export function installSignalHandlers(): void {
  process.on("SIGINT", () => requestShutdown("SIGINT"));
  process.on("SIGTERM", () => requestShutdown("SIGTERM"));
}
