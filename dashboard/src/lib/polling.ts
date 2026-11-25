export function createPollingLoop(
  fetch: (signal: AbortSignal) => Promise<boolean>,
  intervalMs: number,
  initiallyPaused = false,
) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let controller: AbortController | undefined;
  let pending: Promise<void> | undefined;
  let failures = 0;
  let paused = initiallyPaused;
  let disposed = false;

  function stopTimer() {
    clearTimeout(timer);
    timer = undefined;
  }

  async function run(signal: AbortSignal) {
    failures = (await fetch(signal)) ? 0 : Math.min(failures + 1, 4);
  }

  function refresh(): Promise<void> {
    if (disposed) return Promise.resolve();
    if (pending) return pending;
    stopTimer();
    controller = new AbortController();
    pending = run(controller.signal).finally(() => {
      pending = undefined;
      controller = undefined;
      if (!paused && !disposed) {
        timer = setTimeout(() => { void refresh(); }, intervalMs * (1 + failures));
      }
    });
    return pending;
  }

  return {
    refresh,
    pause() {
      paused = true;
      stopTimer();
    },
    resume() {
      paused = false;
      return refresh();
    },
    dispose() {
      disposed = true;
      stopTimer();
      controller?.abort();
    },
  };
}
