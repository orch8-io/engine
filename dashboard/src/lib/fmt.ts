/**
 * Number/delta formatting helpers shared by the Overview KPIs.
 *
 * Rules (per dashboard playbook):
 *  - 0–999 → exact
 *  - 1_000–99_999 → "1,234" (grouped)
 *  - 100_000–999_999 → "142.3K"
 *  - 1_000_000+ → "1.4M"
 *
 * `fmtDelta` returns a signed number like "+3" / "−1" / "0" using a true
 * minus sign so columns line up tabularly.
 */

export function fmtCount(n: number): string {
  if (!Number.isFinite(n)) return "—";
  const abs = Math.abs(n);
  if (abs < 1_000) return Math.trunc(n).toString();
  if (abs < 100_000) return Math.trunc(n).toLocaleString();
  if (abs < 1_000_000) return `${(n / 1_000).toFixed(1)}K`;
  if (abs < 1_000_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  return `${(n / 1_000_000_000).toFixed(1)}B`;
}

export function fmtRate(n: number, digits = 1): string {
  if (!Number.isFinite(n)) return "—";
  if (n === 0) return "0";
  if (Math.abs(n) < 0.1) return n.toFixed(2);
  if (Math.abs(n) < 10) return n.toFixed(digits);
  return fmtCount(n);
}

export function fmtPct(n: number, digits = 1): string {
  if (!Number.isFinite(n)) return "—";
  if (n >= 99.95) return "100%";
  if (n < 10) return `${n.toFixed(digits)}%`;
  return `${n.toFixed(0)}%`;
}

/** Signed delta with true minus. Returns empty string when n === 0 to reduce chrome. */
export function fmtDelta(n: number): string {
  if (!Number.isFinite(n) || n === 0) return "";
  const sign = n > 0 ? "+" : "−";
  return `${sign}${fmtCount(Math.abs(n))}`;
}
