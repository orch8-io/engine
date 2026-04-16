import type { ReactNode } from "react";

type Tone = "neutral" | "signal" | "live" | "hold" | "ok" | "warn";

const ACCENT: Record<Tone, string> = {
  neutral: "bg-muted",
  signal: "bg-signal",
  live: "bg-live",
  hold: "bg-hold",
  ok: "bg-ok",
  warn: "bg-warn",
};

const NUM: Record<Tone, string> = {
  neutral: "text-fg",
  signal: "text-signal",
  live: "text-live",
  hold: "text-hold",
  ok: "text-ok",
  warn: "text-warn",
};

interface Props {
  label: string;
  value: number | string;
  tone?: Tone;
  sub?: ReactNode;
  /** Deprecated legacy prop for old Tailwind class strings. */
  color?: string;
}

export default function StatCard({ label, value, tone = "neutral", sub, color }: Props) {
  // Back-compat: old pages pass `color="text-danger"` etc.
  const inferredTone: Tone = color
    ? color.includes("warning") || color.includes("hold")
      ? "hold"
      : color.includes("accent") || color.includes("primary")
      ? "signal"
      : color.includes("success") || color.includes("ok")
      ? "ok"
      : color.includes("danger") || color.includes("warn")
      ? "warn"
      : "neutral"
    : tone;

  return (
    <div className="relative border border-hairline rounded-md bg-surface overflow-hidden group hover:border-hairline-strong transition-colors">
      <div
        className={`absolute left-0 top-0 bottom-0 w-[2px] ${ACCENT[inferredTone]}`}
        aria-hidden
      />
      <div className="p-4 pl-5">
        <div className="eyebrow mb-2.5">{label}</div>
        <div className={`display-num ${NUM[inferredTone]}`}>{value}</div>
        {sub && <div className="mt-2 text-[12px] text-muted">{sub}</div>}
      </div>
    </div>
  );
}
