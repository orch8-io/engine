import type { ReactNode } from "react";

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
      className={`border border-hairline rounded-md ${
        elevated ? "bg-raised" : "bg-surface"
      } ${className}`}
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
    <div className={`px-4 h-10 border-b border-hairline flex items-center gap-3 bg-sunken/40 ${className}`}>
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
  return <div className={`${padded ? "p-4" : ""} ${className}`}>{children}</div>;
}
