import { useEffect, useState } from "react";

function relative(iso: string | null): string {
  if (!iso) return "—";
  const ms = Date.now() - new Date(iso).getTime();
  const abs = Math.abs(ms);
  const fwd = ms >= 0;
  const S = 1000, M = 60 * S, H = 60 * M, D = 24 * H;
  if (abs < 5 * S) return "just now";
  if (abs < M) return `${Math.round(abs / S)}s${fwd ? " ago" : ""}`;
  if (abs < H) return `${Math.round(abs / M)}m${fwd ? " ago" : ""}`;
  if (abs < D) return `${Math.round(abs / H)}h${fwd ? " ago" : ""}`;
  if (abs < 7 * D) return `${Math.round(abs / D)}d${fwd ? " ago" : ""}`;
  return new Date(iso).toLocaleDateString();
}

/**
 * Shared 1Hz ticker. A single interval drives every <Relative/> on screen
 * so they stay in lockstep and we don't pay N setInterval handlers. The
 * hook returns a number that bumps once per second → consumers re-render.
 */
let subscribers = 0;
let tickId: number | null = null;
const listeners = new Set<() => void>();

function subscribe(cb: () => void): () => void {
  listeners.add(cb);
  subscribers++;
  if (tickId === null) {
    tickId = window.setInterval(() => {
      listeners.forEach((fn) => fn());
    }, 1000);
  }
  return () => {
    listeners.delete(cb);
    subscribers--;
    if (subscribers === 0 && tickId !== null) {
      clearInterval(tickId);
      tickId = null;
    }
  };
}

function useNow(): number {
  const [, setN] = useState(0);
  useEffect(() => subscribe(() => setN((n) => n + 1)), []);
  return Date.now();
}

export function Relative({
  at,
  className = "",
}: {
  at: string | null;
  className?: string;
}) {
  useNow();
  return (
    <span
      title={at ? new Date(at).toLocaleString() : ""}
      className={`text-muted text-[12px] tabular ${className}`}
    >
      {relative(at)}
    </span>
  );
}

export function Absolute({ at, className = "" }: { at: string | null; className?: string }) {
  return (
    <span className={`text-muted text-[12px] tabular ${className}`}>
      {at ? new Date(at).toLocaleString() : "—"}
    </span>
  );
}
