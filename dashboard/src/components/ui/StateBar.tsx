/**
 * StateBar — proportional horizontal stacked bar.
 *
 * Tufte: one visual axis, one meaning, no 3D, no gradients. Hairline 1px
 * separator between non-zero segments so adjacent same-tone segments (rare)
 * still read as distinct. Zero-width segments omit entirely.
 *
 * Used for:
 *  - Global task-state distribution (above stat cards on Overview)
 *  - Per-handler micro-bars inside the handlers table
 *
 * Percentages are computed from the raw counts; if total === 0 the bar
 * renders as an empty sunken well so the operator sees "nothing to report"
 * rather than a missing element.
 */

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
  className = "",
}: {
  segments: StateSegment[];
  height?: number;
  className?: string;
}) {
  const total = segments.reduce((s, seg) => s + seg.value, 0);
  const nonZero = segments.filter((s) => s.value > 0);

  return (
    <div
      className={`flex w-full bg-sunken border border-hairline rounded-sm overflow-hidden ${className}`}
      style={{ height }}
      role="img"
      aria-label={segments
        .map((s) => `${s.label} ${s.value}`)
        .join(", ")}
    >
      {total === 0
        ? null
        : nonZero.map((seg, i) => {
            const pct = (seg.value / total) * 100;
            return (
              <div
                key={seg.label}
                className={`${BG[seg.tone]} ${
                  i > 0 ? "border-l border-bg" : ""
                }`}
                style={{ width: `${pct}%` }}
                title={`${seg.label}: ${seg.value} (${pct.toFixed(pct < 10 ? 1 : 0)}%)`}
              />
            );
          })}
    </div>
  );
}

/**
 * Legend row. Pairs with StateBar to give each tone a label + value.
 * Dense layout (flex-wrap) — never grid — so trailing segments hug the end.
 */
export function StateBarLegend({
  segments,
  className = "",
}: {
  segments: StateSegment[];
  className?: string;
}) {
  return (
    <ul
      className={`flex flex-wrap items-center gap-x-5 gap-y-1.5 text-[11px] font-mono tabular ${className}`}
    >
      {segments.map((seg) => (
        <li key={seg.label} className="flex items-center gap-1.5">
          <span
            className={`inline-block w-2 h-2 rounded-[1px] ${BG[seg.tone]}`}
            aria-hidden
          />
          <span className="uppercase tracking-wider text-faint">
            {seg.label}
          </span>
          <span className={FG[seg.tone]}>{seg.value}</span>
        </li>
      ))}
    </ul>
  );
}
