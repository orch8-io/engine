function relative(iso: string | null): string {
  if (!iso) return "—";
  const ms = Date.now() - new Date(iso).getTime();
  const abs = Math.abs(ms);
  const fwd = ms >= 0;
  const S = 1000, M = 60 * S, H = 60 * M, D = 24 * H;
  if (abs < 5 * S) return "just now";
  if (abs < M) return `${Math.round(abs / S)}s${fwd ? " ago" : ""}`;
  if (abs < H) return `${Math.round(abs / M)}m${fwd ? " ago" : ""}`;
  if (abs < D) return `${Math.round(abs / H)}h${fwd ? " ago" : ""}`;
  if (abs < 7 * D) return `${Math.round(abs / D)}d${fwd ? " ago" : ""}`;
  return new Date(iso).toLocaleDateString();
}

export function Relative({
  at,
  className = "",
}: {
  at: string | null;
  className?: string;
}) {
  return (
    <span
      title={at ? new Date(at).toLocaleString() : ""}
      className={`text-muted text-[12px] tabular ${className}`}
    >
      {relative(at)}
    </span>
  );
}

export function Absolute({ at, className = "" }: { at: string | null; className?: string }) {
  return (
    <span className={`text-muted text-[12px] tabular ${className}`}>
      {at ? new Date(at).toLocaleString() : "—"}
    </span>
  );
}
