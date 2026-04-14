import type { ReactNode } from "react";

/**
 * <NoticeList> — a stacked list with a single warning tick running down the
 * left edge and hairline separators between rows. Replaces the repeated
 * hand-rolled `border-l-2 border-warn` + `<ul>` pattern used for
 * "needs attention" surfaces (DLQ, stale nodes, failure reasons).
 *
 * Tone controls the left-edge colour. Layout is fixed: one horizontal row per
 * child, vertically padded, separated by `border-rule-faint` hairlines.
 */
type Tone = "warn" | "hold" | "signal";

const EDGE: Record<Tone, string> = {
  warn: "border-warn",
  hold: "border-hold",
  signal: "border-signal",
};

export function NoticeList({
  children,
  tone = "warn",
}: {
  children: ReactNode;
  tone?: Tone;
}) {
  return (
    <div className={`border-l-2 ${EDGE[tone]}`}>
      <ul>{children}</ul>
    </div>
  );
}

export function NoticeItem({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <li
      className={`flex items-center gap-3 px-4 py-3 border-b border-rule-faint last:border-b-0 text-[13px] ${className}`}
    >
      {children}
    </li>
  );
}
