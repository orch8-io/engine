import { useCallback, useEffect, useId, useMemo, useState } from "react";
import { getFullInstanceTimeline, type Checkpoint, type TimelineResponse } from "../api";
import {
  blocksAt,
  clampPosition,
  contextAt,
  forkBlockForPosition,
  instanceStateAt,
  outputsAt,
  pointTime,
  topLevelIndex,
  type ScrubBlockState,
} from "../lib/timeTravel";
import { Badge } from "./ui/Badge";
import { Button } from "./ui/Button";
import { INSTANCE_TONE } from "./ui/badgeTones";

const STATE_TONE: Record<ScrubBlockState, "dim" | "live" | "hold" | "ok" | "warn"> = {
  pending: "dim",
  running: "live",
  retrying: "hold",
  completed: "ok",
  failed: "warn",
};

const TICK: Record<string, string> = {
  __in_progress__: "bg-live",
  __retry__: "bg-hold",
  __error__: "bg-warn",
};

/**
 * Time-scrubber over `GET /instances/{id}/timeline`: a slider across the
 * executed-block history showing block states, outputs, and context as of
 * the selected point, with "fork from here".
 *
 * Keyboard: the slider takes ←/→ (one step), PageUp/PageDown, Home/End.
 */
export function TimeScrubber({
  instanceId,
  topLevel,
  checkpoints,
  onForkFrom,
}: {
  instanceId: string;
  topLevel: Array<{ id: string; descendants: string[] }>;
  checkpoints: Checkpoint[];
  onForkFrom: (blockId: string) => void;
}) {
  const [data, setData] = useState<(TimelineResponse & { truncated: boolean }) | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [position, setPosition] = useState<number | null>(null);
  const sliderId = useId();

  const load = useCallback(
    (signal?: AbortSignal) =>
      getFullInstanceTimeline(instanceId, 5000, signal)
        .then((d) => {
          setData(d);
          setError(null);
          setPosition((p) => (p === null ? d.entries.length : clampPosition(p, d.entries.length)));
        })
        .catch((e) => {
          if (!signal?.aborted) setError(e instanceof Error ? e.message : String(e));
        }),
    [instanceId],
  );

  useEffect(() => {
    const ac = new AbortController();
    load(ac.signal);
    return () => ac.abort();
  }, [load]);

  const entries = useMemo(() => data?.entries ?? [], [data]);
  const count = entries.length;
  const p = clampPosition(position ?? count, count);
  const knownIds = useMemo(() => topLevel.flatMap((t) => [t.id, ...t.descendants]), [topLevel]);
  const topOf = useMemo(() => topLevelIndex(topLevel), [topLevel]);
  const blocks = useMemo(() => blocksAt(entries, p, knownIds), [entries, p, knownIds]);
  const outputs = useMemo(() => outputsAt(entries, p), [entries, p]);

  if (error) return <div className="notice notice-warn">Timeline unavailable: {error}</div>;
  if (!data) return <div className="text-muted text-[13px] py-6 text-center font-mono">Loading timeline…</div>;

  const at = pointTime(entries, p, data.instance.created_at);
  const stateAt = instanceStateAt(data.state_transitions, at);
  const ctx = contextAt(checkpoints, at, p === count, data.instance.context);
  const current = p > 0 ? entries[p - 1]! : null;
  const forkBlock = forkBlockForPosition(entries, p, topOf, topLevel.map((t) => t.id));
  const valueText =
    p === 0
      ? `Position 0 of ${count}: instance created`
      : `Position ${p} of ${count}: after ${current!.block_id} attempt ${current!.attempt}${current!.is_sentinel ? ` (${current!.output_ref})` : ""}`;

  return (
    <div className="space-y-5">
      <div className="space-y-2">
        <div className="flex items-center gap-2 flex-wrap">
          <label htmlFor={sliderId} className="eyebrow">
            Time travel
          </label>
          <span className="font-mono text-[12px] text-ink-dim tabular">{new Date(at).toLocaleString()}</span>
          {stateAt && (
            <Badge tone={INSTANCE_TONE[stateAt] ?? "dim"} dot>
              {stateAt}
            </Badge>
          )}
          <span className="ml-auto text-[11px] text-muted font-mono tabular">
            step {p} / {count}
            {data.truncated && " (first 5000 entries)"}
          </span>
          <Button size="sm" variant="ghost" onClick={() => load()}>
            Reload
          </Button>
        </div>
        <div className="flex items-center gap-2">
          <Button size="sm" aria-label="Previous step" disabled={p === 0} onClick={() => setPosition(p - 1)}>
            ◀
          </Button>
          <div className="flex-1 min-w-0">
            <input
              id={sliderId}
              type="range"
              min={0}
              max={count}
              step={1}
              value={p}
              aria-valuetext={valueText}
              onChange={(e) => setPosition(Number(e.target.value))}
              className="w-full accent-signal"
            />
            <div aria-hidden className="flex h-1.5 gap-px mt-0.5">
              {entries.map((e, i) => (
                <span
                  key={i}
                  className={`flex-1 ${e.is_sentinel ? TICK[e.output_ref ?? ""] ?? "bg-live" : "bg-ok"} ${i < p ? "opacity-100" : "opacity-25"}`}
                />
              ))}
            </div>
          </div>
          <Button size="sm" aria-label="Next step" disabled={p === count} onClick={() => setPosition(p + 1)}>
            ▶
          </Button>
        </div>
        <p className="annotation text-[12px]" aria-live="polite">
          {valueText}.
        </p>
      </div>

      <div className="flex items-center gap-3 flex-wrap border border-hairline rounded-sm px-3 py-2">
        <span className="text-[12px] text-ink-dim">
          {forkBlock ? (
            <>
              Fork re-executes from top-level block <code className="font-mono text-ink">{forkBlock}</code>; earlier
              completed blocks are copied.
            </>
          ) : (
            "This point is not inside a top-level block of the sequence — pick a fork point in the fork panel."
          )}
        </span>
        <Button
          size="sm"
          variant="primary"
          className="ml-auto"
          disabled={!forkBlock}
          onClick={() => forkBlock && onForkFrom(forkBlock)}
        >
          Fork from here…
        </Button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div>
          <div className="eyebrow mb-2">Blocks at this point</div>
          <ul className="space-y-1 max-h-96 overflow-auto">
            {blocks.map((b) => (
              <li key={b.block_id} className="flex items-center gap-2 text-[12px]">
                <Badge tone={STATE_TONE[b.state]} dot live={b.state === "running"}>
                  {b.state}
                </Badge>
                <span className="font-mono text-ink">{b.block_id}</span>
                {b.attempt > 0 && <span className="text-faint font-mono">attempt {b.attempt}</span>}
                {topOf.get(b.block_id) === b.block_id && <span className="text-faint text-[10px] uppercase">top-level</span>}
              </li>
            ))}
            {blocks.length === 0 && <li className="text-muted text-[12px]">Nothing has run yet.</li>}
          </ul>
          {current && (
            <div className="mt-4">
              <div className="eyebrow mb-1">Entry {p}</div>
              <div className="text-[12px] font-mono text-ink-dim">
                {current.block_id} · attempt {current.attempt}
                {current.is_sentinel && <span className="text-hold"> · {current.output_ref}</span>}
              </div>
              {current.output !== undefined && !current.is_sentinel && (
                <pre className="well max-h-40 mt-1">{JSON.stringify(current.output, null, 2)}</pre>
              )}
            </div>
          )}
        </div>
        <div className="space-y-4 min-w-0">
          <div>
            <div className="eyebrow mb-1">Outputs as of this point</div>
            <pre className="well max-h-60">{JSON.stringify(outputs, null, 2)}</pre>
          </div>
          <div>
            <div className="eyebrow mb-1">Context</div>
            {ctx.source === "current" && (
              <p className="annotation text-[11px] mb-1">Live context (latest point).</p>
            )}
            {ctx.source === "checkpoint" && (
              <p className="annotation text-[11px] mb-1">
                From checkpoint saved {new Date(ctx.checkpointAt).toLocaleString()} — the engine keeps no per-step
                context history, so this is the nearest earlier snapshot.
              </p>
            )}
            {ctx.source === "none" ? (
              <p className="text-muted text-[12px]">
                No context snapshot at or before this point. Only the latest context and saved checkpoints are
                recorded.
              </p>
            ) : (
              <pre className="well max-h-60">{JSON.stringify(ctx.context, null, 2)}</pre>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
