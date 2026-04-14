/**
 * Skeleton placeholders. No spinners — a spinner says "something is happening";
 * a skeleton says "here is the shape of what you will see." The latter is
 * honest about both layout and loading state, so the first paint of real data
 * doesn't reflow.
 *
 * Single hairline shimmer keyframe (see globals) keeps the animation load low.
 */

export function SkeletonLine({
  width = "100%",
  height = 10,
  className = "",
}: {
  width?: string | number;
  height?: number;
  className?: string;
}) {
  return (
    <span
      aria-hidden
      className={`inline-block bg-raised rounded-[2px] animate-pulse ${className}`}
      style={{ width, height }}
    />
  );
}

export function SkeletonStatCard() {
  return (
    <div className="relative border border-hairline rounded-md bg-surface overflow-hidden">
      <div className="absolute left-0 top-0 bottom-0 w-[3px] bg-hairline" aria-hidden />
      <div className="p-4 pl-5">
        <SkeletonLine width={64} height={8} className="mb-3" />
        <SkeletonLine width={96} height={28} className="mb-2.5" />
        <SkeletonLine width={88} height={8} />
      </div>
    </div>
  );
}

export function SkeletonTable({
  rows = 6,
  cols = 5,
}: {
  rows?: number;
  cols?: number;
}) {
  return (
    <div className="border border-hairline rounded-md bg-surface overflow-hidden">
      <div className="border-b border-hairline bg-sunken/60 px-4 py-2.5 flex gap-6">
        {Array.from({ length: cols }).map((_, i) => (
          <SkeletonLine
            key={i}
            width={i === 0 ? 72 : 56}
            height={8}
          />
        ))}
      </div>
      <ul className="divide-y divide-hairline">
        {Array.from({ length: rows }).map((_, r) => (
          <li key={r} className="px-4 py-3 flex gap-6 items-center">
            {Array.from({ length: cols }).map((_, c) => (
              <SkeletonLine
                key={c}
                width={c === 0 ? 80 : c === cols - 1 ? 120 : 96}
                height={10}
              />
            ))}
          </li>
        ))}
      </ul>
    </div>
  );
}
