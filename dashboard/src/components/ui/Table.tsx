import type { ReactNode, ThHTMLAttributes, TdHTMLAttributes } from "react";

export function Table({ children, className = "" }: { children: ReactNode; className?: string }) {
  return (
    <div className={`overflow-x-auto ${className}`}>
      <table className="w-full text-[13px] border-collapse">{children}</table>
    </div>
  );
}

export function THead({ children }: { children: ReactNode }) {
  return (
    <thead>
      <tr className="border-b border-hairline text-left">{children}</tr>
    </thead>
  );
}

export function TH({
  children,
  className = "",
  ...rest
}: ThHTMLAttributes<HTMLTableCellElement> & { children?: ReactNode }) {
  return (
    <th
      {...rest}
      className={`eyebrow py-2 pr-4 align-middle font-medium ${className}`}
    >
      {children}
    </th>
  );
}

export function TR({
  children,
  className = "",
  onClick,
  active = false,
}: {
  children: ReactNode;
  className?: string;
  onClick?: () => void;
  active?: boolean;
}) {
  const hover = onClick ? "cursor-pointer hover:bg-raised/60" : "";
  const activeC = active ? "bg-signal/5" : "";
  return (
    <tr
      onClick={onClick}
      className={`border-b border-hairline/60 transition-colors ${hover} ${activeC} ${className}`}
    >
      {children}
    </tr>
  );
}

export function TD({
  children,
  className = "",
  ...rest
}: TdHTMLAttributes<HTMLTableCellElement> & { children?: ReactNode }) {
  return (
    <td {...rest} className={`py-2.5 pr-4 align-middle ${className}`}>
      {children}
    </td>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return (
    <tr>
      <td colSpan={99} className="py-12 text-center text-muted text-[13px]">
        {children}
      </td>
    </tr>
  );
}
