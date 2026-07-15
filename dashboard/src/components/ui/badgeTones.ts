type BadgeTone = "neutral" | "signal" | "live" | "hold" | "ok" | "warn" | "dim";

export const INSTANCE_TONE: Record<string, BadgeTone> = {
  scheduled: "dim", running: "live", waiting: "hold", paused: "hold",
  completed: "ok", failed: "warn", cancelled: "dim",
};

export const NODE_TONE: Record<string, BadgeTone> = {
  pending: "dim", running: "live", waiting: "hold", completed: "ok",
  failed: "warn", cancelled: "dim", skipped: "dim",
};

export const TASK_TONE: Record<string, BadgeTone> = {
  pending: "hold", claimed: "live", completed: "ok", failed: "warn",
};
