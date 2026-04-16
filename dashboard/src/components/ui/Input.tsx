import type { InputHTMLAttributes, SelectHTMLAttributes, ReactNode } from "react";

const BASE =
  "h-8 bg-sunken border border-hairline rounded-sm px-2.5 text-[13px] text-fg placeholder:text-faint hover:border-hairline-strong focus:border-signal focus:outline-none transition-colors";

export function Input({ className = "", ...rest }: InputHTMLAttributes<HTMLInputElement>) {
  return <input {...rest} className={`${BASE} ${className}`} />;
}

export function Select({
  className = "",
  children,
  ...rest
}: SelectHTMLAttributes<HTMLSelectElement> & { children: ReactNode }) {
  return (
    <select {...rest} className={`${BASE} pr-8 cursor-pointer ${className}`}>
      {children}
    </select>
  );
}

export function FieldLabel({ children }: { children: ReactNode }) {
  return <label className="eyebrow block mb-1.5">{children}</label>;
}
