/**
 * Inline-SVG sparkline. Intentionally minimal — no chart lib, no deps.
 */

type Tone = "signal" | "live" | "hold" | "ok" | "warn" | "muted";

const STROKE: Record<Tone, string> = {
  signal: "var(--signal)",
  live: "var(--live)",
  hold: "var(--hold)",
  ok: "var(--ok)",
  warn: "var(--warn)",
  muted: "var(--muted)",
};

export function Sparkline({
  values,
  width = 120,
  height = 28,
  tone = "signal",
  fill = true,
  baseline = true,
  ariaLabel,
}: {
  values: number[];
  width?: number;
  height?: number;
  tone?: Tone;
  fill?: boolean;
  baseline?: boolean;
  ariaLabel?: string;
}) {
  const stroke = STROKE[tone];
  const pad = 1.5;
  const w = width - pad * 2;
  const h = height - pad * 2;

  if (values.length < 2) {
    return (
      <svg width={width} height={height} role="img" aria-label={ariaLabel ?? "No data"}>
        {baseline && (
          <line
            x1={pad}
            x2={width - pad}
            y1={height / 2}
            y2={height / 2}
            stroke="var(--hairline)"
            strokeWidth={1}
          />
        )}
      </svg>
    );
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const step = w / (values.length - 1);

  const points = values.map((v, i) => {
    const x = pad + i * step;
    const y = pad + h - ((v - min) / span) * h;
    return [x, y] as const;
  });

  const linePath = points
    .map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)} ${y.toFixed(1)}`)
    .join(" ");

  const areaPath =
    fill && values.length > 1
      ? `${linePath} L${(width - pad).toFixed(1)} ${height - pad} L${pad.toFixed(1)} ${height - pad} Z`
      : null;

  return (
    <svg width={width} height={height} role="img" aria-label={ariaLabel ?? `Sparkline showing ${values.length} data points`}>
      {areaPath && (
        <path d={areaPath} fill={stroke} opacity={0.12} />
      )}
      <path
        d={linePath}
        fill="none"
        stroke={stroke}
        strokeWidth={1.25}
        strokeLinejoin="round"
        strokeLinecap="round"
      />
    </svg>
  );
}
