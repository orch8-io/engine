import type { ReactNode } from "react";

type Tone = "neutral" | "signal" | "live" | "hold" | "ok" | "warn" | "dim";

const TONE: Record<Tone, string> = {
  neutral: "text-fg-dim border-hairline bg-surface",
  signal: "text-signal border-signal/30 bg-signal/10",
  live: "text-live border-live/30 bg-live/10",
  hold: "text-hold border-hold/30 bg-hold/10",
  ok: "text-ok border-ok/30 bg-ok/10",
  warn: "text-warn border-warn/30 bg-warn/10",
  dim: "text-muted border-hairline bg-surface",
};

const DOT: Record<Tone, string> = {
  neutral: "bg-muted",
  signal: "bg-signal",
  live: "bg-live",
  hold: "bg-hold",
  ok: "bg-ok",
  warn: "bg-warn",
  dim: "bg-muted",
};

export function Badge({
  tone = "neutral",
  children,
  dot = false,
  live = false,
  className = "",
}: {
  tone?: Tone;
  children: ReactNode;
  dot?: boolean;
  live?: boolean;
  className?: string;
}) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-sm border px-1.5 py-0.5 text-[11px] font-mono uppercase tracking-wider ${TONE[tone]} ${className}`}
    >
      {dot && (
        <span
          className={`inline-block w-1.5 h-1.5 rounded-full ${DOT[tone]} ${live ? "pulse-live" : ""}`}
        />
      )}
      {children}
    </span>
  );
}

export const INSTANCE_TONE: Record<string, Tone> = {
  scheduled: "dim",
  running: "live",
  waiting: "hold",
  paused: "hold",
  completed: "ok",
  failed: "warn",
  cancelled: "dim",
};

export const NODE_TONE: Record<string, Tone> = {
  pending: "dim",
  running: "live",
  waiting: "hold",
  completed: "ok",
  failed: "warn",
  cancelled: "dim",
  skipped: "dim",
};

export const TASK_TONE: Record<string, Tone> = {
  pending: "hold",
  claimed: "live",
  completed: "ok",
  failed: "warn",
};
