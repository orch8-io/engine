import type { ReactNode } from "react";

/**
 * <Panel> — a bordered, hairline-ruled container. Used for legacy pages and
 * tabular data where a clear enclosure helps scan-ability. New code should
 * prefer <Section> where possible.
 *
 * No rounded corners, no background fill, no shadow. Just 1px hairlines.
 */
export function Panel({
  children,
  className = "",
  elevated = false,
}: {
  children: ReactNode;
  className?: string;
  elevated?: boolean;
}) {
  return (
    <div
      className={`border border-rule ${elevated ? "bg-raised" : ""} ${className}`}
    >
      {children}
    </div>
  );
}

export function PanelHeader({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={`px-4 h-10 border-b border-rule flex items-center gap-3 ${className}`}
    >
      {children}
    </div>
  );
}

export function PanelBody({
  children,
  className = "",
  padded = true,
}: {
  children: ReactNode;
  className?: string;
  padded?: boolean;
}) {
  return (
    <div className={`${padded ? "p-4" : ""} ${className}`}>{children}</div>
  );
}
