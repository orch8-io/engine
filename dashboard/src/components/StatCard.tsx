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
}

export default function StatCard({ label, value, tone = "neutral", sub }: Props) {
  return (
    <div className="group relative border border-hairline rounded-md bg-surface overflow-hidden hover:border-hairline-strong transition-colors">
      <div
        className={`absolute left-0 top-0 bottom-0 w-[3px] ${ACCENT[tone]}`}
        aria-hidden
      />
      <div className="p-4 pl-5">
        <div className="eyebrow mb-3">{label}</div>
        <div className={`display-num ${NUM[tone]}`}>{value}</div>
        {sub && <div className="mt-2.5 text-[11px] font-mono tracking-wider uppercase text-faint">{sub}</div>}
      </div>
    </div>
  );
}
