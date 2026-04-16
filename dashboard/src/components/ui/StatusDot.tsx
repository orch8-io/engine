const TONE: Record<string, string> = {
  live: "bg-live",
  signal: "bg-signal",
  ok: "bg-ok",
  hold: "bg-hold",
  warn: "bg-warn",
  dim: "bg-muted",
  neutral: "bg-muted",
};

export function StatusDot({
  tone = "neutral",
  live = false,
  className = "",
}: {
  tone?: keyof typeof TONE;
  live?: boolean;
  className?: string;
}) {
  return (
    <span
      className={`inline-block w-1.5 h-1.5 rounded-full shrink-0 ${TONE[tone]} ${
        live ? "pulse-live" : ""
      } ${className}`}
      aria-hidden
    />
  );
}
