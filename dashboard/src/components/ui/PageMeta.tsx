import { useEffect, useRef, useState } from "react";
import { Button } from "./Button";
import { IconRefresh } from "./Icons";
import { Relative } from "./Relative";

/**
 * Right-side meta for PageHeader. Shows:
 *   · last-updated relative timestamp (auto-ticks via Relative)
 *   · a manual refresh button wired to usePolling.refresh
 *
 * Spinning affordance: while a refresh is in-flight the icon rotates once
 * per second. We don't block the button — double-clicking is harmless.
 *
 * When `updatedAt` is null (initial mount) we render an em-dash so the
 * layout doesn't jump on first data arrival.
 */
export function PageMeta({
  updatedAt,
  onRefresh,
}: {
  updatedAt: string | null;
  onRefresh?: () => void;
}) {
  const [spinning, setSpinning] = useState(false);
  const prev = useRef(updatedAt);

  useEffect(() => {
    if (prev.current !== updatedAt) {
      prev.current = updatedAt;
      setSpinning(false);
    }
  }, [updatedAt]);

  const handleClick = () => {
    if (!onRefresh) return;
    setSpinning(true);
    onRefresh();
    // Safety net — if the refresh never lands (network stall), release the
    // spinner after 4s so it doesn't spin forever.
    window.setTimeout(() => setSpinning(false), 4000);
  };

  return (
    <div className="flex items-center gap-3">
      <span className="text-[11px] font-mono uppercase tracking-wider text-faint">
        Updated
      </span>
      <Relative at={updatedAt} className="text-fg-dim" />
      {onRefresh && (
        <Button
          variant="ghost"
          size="sm"
          onClick={handleClick}
          aria-label="Refresh"
          className="!px-2"
        >
          <IconRefresh
            size={14}
            className={spinning ? "animate-spin" : ""}
          />
        </Button>
      )}
    </div>
  );
}
