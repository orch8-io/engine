import { cn } from "../../lib/cn";

type Tone = "signal" | "live" | "hold" | "ok" | "warn" | "dim" | "neutral";

const BG: Record<Tone, string> = {
  neutral: "bg-muted",
  signal: "bg-signal",
  live: "bg-live",
  hold: "bg-hold",
  ok: "bg-ok",
  warn: "bg-warn",
  dim: "bg-muted",
};

const FG: Record<Tone, string> = {
  neutral: "text-fg",
  signal: "text-signal",
  live: "text-live",
  hold: "text-hold",
  ok: "text-ok",
  warn: "text-warn",
  dim: "text-muted",
};

export type StateSegment = {
  label: string;
  value: number;
  tone: Tone;
};

export function StateBar({
  segments,
  height = 8,
  className,
  id,
}: {
  segments: StateSegment[];
  height?: number;
  className?: string;
  id?: string;
}) {
  const total = segments.reduce((s, seg) => s + seg.value, 0);
  const nonZero = segments.filter((s) => s.value > 0);

  return (
    <div
      id={id}
      className={cn("flex w-full bg-sunken border border-rule overflow-hidden", className)}
      style={{ height }}
      role="img"
      aria-label={segments.map((s) => `${s.label} ${s.value}`).join(", ")}
    >
      {total === 0
        ? null
        : nonZero.map((seg, i) => {
            const pct = (seg.value / total) * 100;
            return (
              <div
                key={`${seg.label}-${i}`}
                className={cn(BG[seg.tone], i > 0 && "border-l border-bg")}
                style={{ width: `${pct}%` }}
                title={`${seg.label}: ${seg.value} (${pct.toFixed(pct < 10 ? 1 : 0)}%)`}
              />
            );
          })}
    </div>
  );
}

export function StateBarLegend({
  segments,
  className,
  labelledBy,
}: {
  segments: StateSegment[];
  className?: string;
  labelledBy?: string;
}) {
  return (
    <ul
      className={cn("flex flex-wrap items-center gap-x-5 gap-y-1.5 text-[11px] font-mono tabular", className)}
      aria-labelledby={labelledBy}
    >
      {segments.map((seg) => (
        <li key={seg.label} className="flex items-center gap-1.5">
          <span className={cn("inline-block w-2 h-2", BG[seg.tone])} aria-hidden />
          <span className="uppercase tracking-wider text-faint">{seg.label}</span>
          <span className={FG[seg.tone]}>{seg.value}</span>
        </li>
      ))}
    </ul>
  );
}
