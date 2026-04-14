import type { MetricTone } from "./Metric";

/**
 * <InlineSpark> — a tiny, wordless time-series trace meant to live inside a
 * table cell or beside a number. 72×20px default; no axes, no fill, just a
 * single-pixel stroke. Tufte's sparkline done right.
 *
 * The last datapoint gets a single dot so your eye lands on "right now".
 */
export function InlineSpark({
  values,
  width = 72,
  height = 20,
  tone = "signal",
  fill = false,
}: {
  values: number[];
  width?: number;
  height?: number;
  tone?: MetricTone;
  fill?: boolean;
}) {
  if (!values || values.length < 2) {
    return <span className="inline-block text-faint font-mono text-[10px]">—</span>;
  }

  const stroke =
    tone === "warn"   ? "var(--warn)" :
    tone === "hold"   ? "var(--hold)" :
    tone === "live"   ? "var(--live)" :
    tone === "ok"     ? "var(--ok)"   :
    tone === "muted"  ? "var(--muted)" :
    tone === "ink"    ? "var(--ink-dim)" :
                        "var(--signal)";

  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const pad = 2;
  const w = width - pad * 2;
  const h = height - pad * 2;
  const n = values.length;

  const points = values.map((v, i) => {
    const x = pad + (i / (n - 1)) * w;
    const y = pad + h - ((v - min) / range) * h;
    return [x, y] as const;
  });

  const line = points.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)} ${y.toFixed(1)}`).join(" ");
  const area = fill
    ? `${line} L ${pad + w} ${pad + h} L ${pad} ${pad + h} Z`
    : null;
  const last = points[points.length - 1]!;

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className="shrink-0 overflow-visible"
      aria-hidden
    >
      {area && (
        <path d={area} fill={stroke} opacity={0.12} />
      )}
      <path
        d={line}
        fill="none"
        stroke={stroke}
        strokeWidth={1}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle cx={last[0]} cy={last[1]} r={1.8} fill={stroke} />
    </svg>
  );
}
