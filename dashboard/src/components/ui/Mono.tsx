import type { ReactNode } from "react";

/**
 * Monospaced identifier. Truncates mid-string to keep prefix + suffix visible —
 * e.g. `a8f3c…9b21` so the eye can disambiguate without full UUID width.
 */
export function Id({
  value,
  short = true,
  className = "",
}: {
  value: string;
  short?: boolean;
  className?: string;
}) {
  const display = short && value.length > 14 ? `${value.slice(0, 8)}…` : value;
  return (
    <span
      title={value}
      className={`font-mono text-[12px] text-fg-dim ${className}`}
    >
      {display}
    </span>
  );
}

export function Code({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <code className={`font-mono text-[12px] text-fg-dim bg-sunken px-1 py-0.5 rounded-sm ${className}`}>
      {children}
    </code>
  );
}

export function Kbd({ children }: { children: ReactNode }) {
  return (
    <kbd className="font-mono text-[10px] text-muted bg-raised border border-hairline px-1.5 py-0.5 rounded-sm">
      {children}
    </kbd>
  );
}
