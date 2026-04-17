import { useState, type ReactNode } from "react";
import { IconCheck, IconCopy } from "./Icons";

/**
 * Monospaced identifier. Truncates to prefix + ellipsis.
 * If `copy` is enabled, the element becomes a button that copies the full
 * value to the clipboard on click and flashes a check glyph for 1.2s.
 * Truncation must always be recoverable — either by click-to-copy or tooltip.
 */
export function Id({
  value,
  short = true,
  copy = false,
  className = "",
}: {
  value: string;
  short?: boolean;
  copy?: boolean;
  className?: string;
}) {
  const [copied, setCopied] = useState(false);
  const display = short && value.length > 14 ? `${value.slice(0, 8)}…` : value;

  if (!copy) {
    return (
      <span
        title={value}
        className={`font-mono text-[12px] text-fg-dim ${className}`}
      >
        {display}
      </span>
    );
  }

  const onClick = async (e: React.MouseEvent<HTMLButtonElement>) => {
    e.stopPropagation();
    try {
      await navigator.clipboard.writeText(value);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch {
      /* clipboard blocked — title still reveals full value */
    }
  };

  return (
    <button
      type="button"
      onClick={onClick}
      title={copied ? "Copied" : `${value} — click to copy`}
      aria-label={copied ? "Copied" : `Copy ${value}`}
      className={`inline-flex items-center gap-1.5 font-mono text-[12px] text-fg-dim hover:text-fg transition-colors cursor-copy ${className}`}
    >
      <span>{display}</span>
      {copied ? (
        <IconCheck size={11} className="text-ok" />
      ) : (
        <IconCopy size={11} className="text-faint" />
      )}
    </button>
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
