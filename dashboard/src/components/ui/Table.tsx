import { type ReactNode, type ThHTMLAttributes, type TdHTMLAttributes, type HTMLAttributes, type TableHTMLAttributes } from "react";
import { cn } from "../../lib/cn";

export function Table({ children, className, ...rest }: TableHTMLAttributes<HTMLTableElement> & { children: ReactNode }) {
  return (
    <div className={cn("overflow-x-auto", className)}>
      <table {...rest} className="w-full text-[13px] border-collapse">{children}</table>
    </div>
  );
}

export function THead({ children, ...rest }: HTMLAttributes<HTMLTableSectionElement> & { children: ReactNode }) {
  return (
    <thead {...rest}>
      <tr className="border-b border-rule text-left">{children}</tr>
    </thead>
  );
}

export function TH({ children, className, ...rest }: ThHTMLAttributes<HTMLTableCellElement> & { children?: ReactNode }) {
  return (
    <th
      {...rest}
      className={cn("field-label py-2.5 pr-4 pl-4 first:pl-4 align-middle font-medium", className)}
    >
      {children}
    </th>
  );
}

export function TR({ children, className, onClick, active = false, ...rest }: HTMLAttributes<HTMLTableRowElement> & { children: ReactNode; onClick?: () => void; active?: boolean }) {
  return (
    <tr
      {...rest}
      onClick={onClick}
      className={cn(
        "border-b border-rule-faint transition-colors",
        onClick && "cursor-pointer hover:bg-surface",
        active && "bg-signal-weak",
        className,
      )}
    >
      {children}
    </tr>
  );
}

export function TD({ children, className, ...rest }: TdHTMLAttributes<HTMLTableCellElement> & { children?: ReactNode }) {
  return (
    <td {...rest} className={cn("py-2.5 pr-4 pl-4 first:pl-4 align-middle", className)}>
      {children}
    </td>
  );
}

export function Empty({ children, colSpan = 1 }: { children: ReactNode; colSpan?: number }) {
  return (
    <tr>
      <td colSpan={colSpan} className="py-12 text-center text-muted text-[13px]">
        {children}
      </td>
    </tr>
  );
}
