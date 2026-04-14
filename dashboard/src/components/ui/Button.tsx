import { forwardRef, type ButtonHTMLAttributes, type ReactNode } from "react";
import { cn } from "../../lib/cn";

type Variant = "default" | "primary" | "danger" | "ghost";

const VARIANT: Record<Variant, string> = {
  default:
    "bg-surface border border-hairline text-fg hover:bg-raised hover:border-hairline-strong",
  primary:
    "bg-signal/10 border border-signal/40 text-signal hover:bg-signal/20 hover:border-signal/60",
  danger:
    "bg-warn/5 border border-warn/30 text-warn hover:bg-warn/15 hover:border-warn/50",
  ghost:
    "bg-transparent border border-transparent text-muted hover:text-fg hover:bg-surface",
};

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  children: ReactNode;
  variant?: Variant;
  size?: "sm" | "md";
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ children, variant = "default", size = "md", className, ...rest }, ref) => {
    const sz =
      size === "sm" ? "h-7 px-2.5 text-[12px]" : "h-8 px-3 text-[13px]";
    return (
      <button
        ref={ref}
        {...rest}
        className={cn(
          "inline-flex items-center gap-1.5 rounded-sm font-medium transition-colors disabled:opacity-40 disabled:cursor-not-allowed",
          sz,
          VARIANT[variant],
          className,
        )}
      >
        {children}
      </button>
    );
  },
);
Button.displayName = "Button";
