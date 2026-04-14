import type { ReactNode } from "react";
import { cn } from "../../lib/cn";

type Tone = "neutral" | "signal" | "live" | "hold" | "ok" | "warn" | "dim";

// Flat tokens — no fills, no tinted backgrounds. Just colored text inside
// a hairline border. Measurement-instrument feel, not software-ui feel.
const TONE: Record<Tone, string> = {
  neutral: "text-ink-dim border-rule",
  signal:  "text-signal  border-signal/40",
  live:    "text-live    border-live/40",
  hold:    "text-hold    border-hold/40",
  ok:      "text-ok      border-ok/40",
  warn:    "text-warn    border-warn/40",
  dim:     "text-muted   border-rule",
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
      className={cn("inline-flex items-center gap-1.5 border px-1.5 py-[1px] text-[10px] leading-[1.4] font-mono uppercase tracking-[0.12em]", TONE[tone], className)}
    >
      {dot && (
        <span
          className={cn("inline-block w-1.5 h-1.5 rounded-full", DOT[tone], live && "pulse-live")}
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
