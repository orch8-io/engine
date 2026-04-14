import { forwardRef, type InputHTMLAttributes, type SelectHTMLAttributes, type ReactNode, type LabelHTMLAttributes } from "react";
import { cn } from "../../lib/cn";

const BASE =
  "h-8 bg-sunken border border-hairline rounded-sm px-2.5 text-[13px] text-fg placeholder:text-faint hover:border-hairline-strong focus:border-signal focus:outline-none transition-colors";

export const Input = forwardRef<HTMLInputElement, InputHTMLAttributes<HTMLInputElement>>(
  ({ className, ...rest }, ref) => (
    <input ref={ref} {...rest} className={cn(BASE, className)} />
  ),
);
Input.displayName = "Input";

export const Select = forwardRef<HTMLSelectElement, SelectHTMLAttributes<HTMLSelectElement> & { children: ReactNode }>(
  ({ className, children, ...rest }, ref) => (
    <select ref={ref} {...rest} className={cn(BASE, "pr-8 cursor-pointer", className)}>
      {children}
    </select>
  ),
);
Select.displayName = "Select";

export function FieldLabel({ children, ...rest }: LabelHTMLAttributes<HTMLLabelElement>) {
  return <label {...rest} className={cn("eyebrow block mb-1.5", rest.className)}>{children}</label>;
}
