/**
 * orch8 brand mark — an inscribed circle (ring + half-fill) evoking:
 *  - orchestration: concentric coordination
 *  - state: half-filled = "executing"
 *  - a dial / instrument face — operator-console authority
 *
 * Purely geometric. No typography inside. Pairs with the Inter wordmark.
 */
export function BrandMark({
  size = 20,
  className = "",
}: {
  size?: number;
  className?: string;
}) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      aria-label="orch8"
    >
      <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="1.5" />
      <path
        d="M12 2a10 10 0 0 1 0 20"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
      />
      <circle cx="12" cy="12" r="2.25" fill="currentColor" />
    </svg>
  );
}

export function Wordmark({ className = "" }: { className?: string }) {
  return (
    <div className={`flex items-center gap-2.5 ${className}`}>
      <BrandMark className="text-signal" />
      <span className="font-mono text-[13px] font-semibold tracking-[0.18em] uppercase text-fg">
        orch<span className="text-signal">8</span>
      </span>
    </div>
  );
}
