import type { ReactNode } from "react";
import { Link } from "react-router-dom";

export type MetricTone = "ink" | "signal" | "live" | "hold" | "ok" | "warn" | "muted";

/**
 * <Metric> — a single instrument reading.
 *
 * No card chrome, no gradient, no shadow. Just:
 *
 *   EYEBROW · LABEL                          unit
 *   48
 *   ─────────
 *   one-line meaning / what it represents
 *
 * Four information slots:
 *   - label:       what is being measured (eyebrow)
 *   - value:       the reading itself (display number)
 *   - unit:        tiny annotation next to the value (/s, %, ms, count)
 *   - caption:     one line explaining what this means to the operator
 *   - trend:       optional child (inline sparkline, delta indicator)
 *
 * Click behavior: if `to` is set, the whole block becomes a nav link.
 */
export function Metric({
  label,
  value,
  unit,
  caption,
  tone = "ink",
  trend,
  to,
  annotation,
}: {
  label: string;
  value: string | number;
  unit?: string;
  caption?: string;
  tone?: MetricTone;
  trend?: ReactNode;
  to?: string;
  annotation?: string;
}) {
  const valueClass =
    tone === "warn"   ? "text-warn" :
    tone === "hold"   ? "text-hold" :
    tone === "live"   ? "text-live" :
    tone === "ok"     ? "text-ok"   :
    tone === "signal" ? "text-signal" :
    tone === "muted"  ? "text-muted" :
                        "text-ink";

  const body = (
    <div className="group relative py-4 px-4 h-full flex flex-col">
      {/* label row */}
      <div className="flex items-baseline justify-between gap-2 mb-3">
        <span className="eyebrow">{label}</span>
        {unit && (
          <span className="font-mono text-[10px] uppercase tracking-[0.12em] text-faint">
            {unit}
          </span>
        )}
      </div>

      {/* value + trend */}
      <div className="flex items-end justify-between gap-4 mb-3">
        <span className={`display-num ${valueClass}`}>{value}</span>
        {trend && <div className="pb-1 shrink-0">{trend}</div>}
      </div>

      {/* hairline + caption */}
      <div className="rule-faint mt-auto" />
      {caption && (
        <p className="mt-2.5 text-[12px] leading-snug text-muted">{caption}</p>
      )}
      {annotation && (
        <p className="mt-1 font-mono text-[10px] uppercase tracking-[0.12em] text-faint">
          {annotation}
        </p>
      )}
    </div>
  );

  if (to) {
    return (
      <Link
        to={to}
        className="block h-full transition-colors hover:bg-surface/40"
      >
        {body}
      </Link>
    );
  }
  return body;
}

/**
 * <MetricRow> — a row of <Metric> instruments separated by vertical rules.
 * Mirror the face of a measurement instrument: gauges side by side, no gaps,
 * thin dividers between them.
 */
export function MetricRow({
  children,
  cols = 4,
}: {
  children: ReactNode;
  cols?: 2 | 3 | 4;
}) {
  const colsClass =
    cols === 2 ? "grid-cols-1 md:grid-cols-2"
    : cols === 3 ? "grid-cols-1 md:grid-cols-3"
    : "grid-cols-2 md:grid-cols-4";
  return (
    <div
      className={`grid ${colsClass} divide-y divide-rule md:divide-y-0 md:divide-x md:divide-rule border-y border-rule`}
    >
      {children}
    </div>
  );
}
