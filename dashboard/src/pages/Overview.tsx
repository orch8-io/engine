import { useCallback, useMemo } from "react";
import { Link, useNavigate } from "react-router-dom";
import { usePolling } from "../hooks/usePolling";
import { useWindowedStats } from "../hooks/useWindowedStats";
import { useClock } from "../hooks/useClock";
import {
  getWorkerTaskStats,
  listCircuitBreakers,
  listClusterNodes,
  listDlq,
  type WorkerTaskStats,
  type CircuitBreakerState,
  type ClusterNode,
  type TaskInstance,
  AuthError,
} from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Metric, MetricRow } from "../components/ui/Metric";
import { InlineSpark } from "../components/ui/InlineSpark";
import { Glossary } from "../components/ui/Glossary";
import { Badge } from "../components/ui/Badge";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { StateBar, type StateSegment } from "../components/ui/StateBar";
import { SkeletonTable } from "../components/ui/Skeleton";
import { Relative } from "../components/ui/Relative";
import { fmtCount, fmtPct, fmtRate } from "../lib/fmt";

/* ────────────────────────────────────────────────────────────────────────
 * Overview — the operator's ground truth.
 *
 * Reading order:
 *   1. Masthead: engine identity + one-line purpose
 *   2. Glossary:  "how to read this page" (collapsed)
 *   3. Hero:      engine status · throughput · success rate
 *   4. Readings:  pending · claimed · failed(window) · open breakers
 *   5. Attention: ONE conditional block when something needs looking at
 *   6. Handlers:  sorted by failure rate (worst first)
 *   7. Cluster:   every node and its heartbeat
 *
 * All panels poll at 1 Hz (KPIs) or 2–5 Hz (structural data). The windowed
 * hook converts cumulative counters into per-second deltas client-side.
 * ──────────────────────────────────────────────────────────────────────── */

const PAGE_GLOSSARY = [
  {
    term: "Execution",
    definition:
      "One run of a sequence definition. Each execution has its own state, context, and trail of emitted signals. Used to be called 'instance'.",
  },
  {
    term: "Handler",
    definition:
      "A worker-side function that processes one step of a sequence. Identified by name (e.g. loop_accumulate). Workers poll for tasks targeting handlers they implement.",
  },
  {
    term: "Throughput",
    definition:
      "Completed tasks per second, averaged over the live window (up to 60s). Warms up over the first minute after the page loads.",
  },
  {
    term: "Success rate",
    definition:
      "completed / (completed + failed) within the current window. Excludes in-flight work. A handler that always retries succeeds — only permanent failures count.",
  },
  {
    term: "Pending",
    definition:
      "Tasks queued but not yet claimed by any worker. Healthy systems hold a small, steady number. Growing pending means workers can't keep up.",
  },
  {
    term: "Claimed",
    definition:
      "Tasks in-flight with a worker. Bounded by concurrency; each worker holds N at a time before heartbeating extensions or completing.",
  },
  {
    term: "Circuit breaker",
    definition:
      "A per-handler guard that stops dispatch after N consecutive failures. 'Open' means the handler is paused and fresh tasks pile up in Pending.",
  },
  {
    term: "DLQ",
    definition:
      "Dead-letter queue. Executions whose current task exhausted its retry policy. They stop progressing until an operator inspects and retries or cancels.",
  },
  {
    term: "Heartbeat",
    definition:
      "Periodic signal from each cluster node. Missing a heartbeat for more than 45 seconds marks the node 'stale' — its claimed tasks will re-enter Pending.",
  },
  {
    term: "Draining",
    definition:
      "A node explicitly asked to finish its in-flight work and accept no new claims. Used before a safe shutdown or rolling deploy.",
  },
];

export default function Overview() {
  const navigate = useNavigate();

  const statsFetcher = useCallback(() => getWorkerTaskStats(), []);
  const breakersFetcher = useCallback(() => listCircuitBreakers(), []);
  const nodesFetcher = useCallback(() => listClusterNodes(), []);
  const dlqFetcher = useCallback(() => listDlq({ limit: "50" }), []);

  const stats = usePolling<WorkerTaskStats>(statsFetcher);
  const breakers = usePolling<CircuitBreakerState[]>(breakersFetcher, 2000);
  const nodes = usePolling<ClusterNode[]>(nodesFetcher, 2000);
  const dlq = usePolling<TaskInstance[]>(dlqFetcher, 5000);

  const windowed = useWindowedStats(stats.data, 60);
  const handlerRows = useHandlerRows(stats.data);

  if (stats.error instanceof AuthError) {
    navigate("/settings");
    return null;
  }

  const meta = <PageMeta updatedAt={stats.updatedAt} onRefresh={stats.refresh} />;

  if (stats.loading && !stats.data) {
    return (
      <div className="space-y-12">
        <PageHeader
          eyebrow="Operator · Orch8"
          title="Overview"
          description="Real-time engine health, throughput, and failure signals across every tenant routed through this cluster."
          actions={meta}
        />
        <SkeletonTable rows={5} cols={5} />
      </div>
    );
  }

  if (stats.error && !stats.data) {
    return (
      <div className="space-y-12">
        <PageHeader eyebrow="Operator · Orch8" title="Overview" actions={meta} />
        <div className="border-l-2 border-warn pl-4 py-2 text-[13px] text-warn">
          {stats.error.message}
        </div>
      </div>
    );
  }

  if (!stats.data) return null;

  const data = stats.data;
  const pending = data.by_state["pending"] ?? 0;
  const claimed = data.by_state["claimed"] ?? 0;
  const completed = data.by_state["completed"] ?? 0;
  const failed = data.by_state["failed"] ?? 0;
  const total = pending + claimed + completed + failed;

  const openBreakers = (breakers.data ?? []).filter((b) => b.state !== "closed");
  const staleCutoffMs = 45_000;
  const staleNodes = (nodes.data ?? []).filter((n) => {
    const age = Date.now() - new Date(n.last_heartbeat_at).getTime();
    return n.status === "active" && age > staleCutoffMs;
  });
  const drainingNodes = (nodes.data ?? []).filter((n) => n.drain);
  const activeNodes = (nodes.data ?? []).filter((n) => n.status === "active");
  const dlqCount = dlq.data?.length ?? 0;

  let engineStatus: "healthy" | "degraded" | "down" = "healthy";
  const reasons: string[] = [];
  if (stats.error) {
    engineStatus = "down";
    reasons.push("stats feed unreachable");
  }
  if (openBreakers.length > 0) {
    engineStatus = "degraded";
    reasons.push(`${openBreakers.length} breaker${openBreakers.length === 1 ? "" : "s"} open`);
  }
  if (staleNodes.length > 0) {
    engineStatus = "degraded";
    reasons.push(`${staleNodes.length} stale node${staleNodes.length === 1 ? "" : "s"}`);
  }
  if (activeNodes.length === 0 && data.active_workers.length === 0) {
    engineStatus = engineStatus === "down" ? "down" : "degraded";
    reasons.push("no active workers");
  }

  const badHandlers = handlerRows.filter((h) => h.total >= 5 && h.failureRate >= 10);

  const showAttention =
    openBreakers.length > 0 ||
    dlqCount > 0 ||
    staleNodes.length > 0 ||
    drainingNodes.length > 0 ||
    badHandlers.length > 0;

  const isEmpty = total === 0 && data.active_workers.length === 0 && activeNodes.length === 0;

  // Status line: "All signals nominal." or "N breakers open · M stale nodes"
  const statusNarration =
    reasons.length === 0 ? "All signals nominal." : capitalize(reasons.join(" · "));

  return (
    <div className="space-y-12 pb-16">
      <PageHeader
        eyebrow="Operator · Orch8"
        title="Overview"
        description="Real-time engine health, throughput, and failure signals across every tenant routed through this cluster. This page auto-refreshes; a stopped clock at the top-right means the feed has stalled."
        actions={meta}
      />

      <Glossary items={PAGE_GLOSSARY} />

      {/* ── Hero: three instruments, no card chrome ───────────────────── */}
      <Section
        eyebrow="Primary Readings"
        title="Engine status"
        description={
          <>
            A three-panel composite: the engine's overall state (derived from breaker, node, and worker
            signals), the live throughput rate, and the resulting success rate. If any of these three
            drift out of nominal, the reason appears inline — no separate alert panel needed.
          </>
        }
        meta={<>{useClock().toLocaleTimeString()} · {activeNodes.length}/{(nodes.data ?? []).length} nodes</>}
      >
        <MetricRow cols={3}>
          <Metric
            label="Engine"
            value={capitalize(engineStatus)}
            unit="state"
            tone={
              engineStatus === "healthy"
                ? "ok"
                : engineStatus === "degraded"
                  ? "hold"
                  : "warn"
            }
            caption={statusNarration}
            trend={
              <Badge
                tone={
                  engineStatus === "healthy"
                    ? "ok"
                    : engineStatus === "degraded"
                      ? "hold"
                      : "warn"
                }
                dot
                live={engineStatus === "healthy"}
              >
                {engineStatus}
              </Badge>
            }
          />

          <Metric
            label="Throughput"
            value={`${fmtRate(windowed.throughputNow)}`}
            unit="tasks / sec"
            tone="signal"
            caption={
              windowed.windowSeconds > 0
                ? `${fmtCount(windowed.windowCompleted)} completed in the last ${windowed.windowSeconds}s.`
                : "Collecting baseline — throughput warms up over the first minute."
            }
            trend={
              <InlineSpark
                values={windowed.throughput}
                width={88}
                height={22}
                tone="signal"
              />
            }
          />

          <Metric
            label="Success rate"
            value={Number.isFinite(windowed.successRate) ? fmtPct(windowed.successRate) : "—"}
            unit="completed / (completed + failed)"
            tone={
              !Number.isFinite(windowed.successRate)
                ? "muted"
                : windowed.successRate < 95
                  ? "warn"
                  : windowed.successRate < 99
                    ? "hold"
                    : "ok"
            }
            caption={
              windowed.windowSeconds > 0
                ? `${fmtCount(windowed.windowFailed)} terminal failures in the last ${windowed.windowSeconds}s.`
                : "Rate stabilizes once enough tasks have finished."
            }
            trend={
              <InlineSpark
                values={windowed.throughput}
                width={88}
                height={22}
                tone={windowed.successRate < 99 ? "warn" : "ok"}
              />
            }
          />
        </MetricRow>
      </Section>

      {/* ── Secondary readings — queue, concurrency, failure, breakers ─ */}
      <Section
        eyebrow="Queue & Concurrency"
        title="Work-in-flight"
        description={
          <>
            Four counters drawn from the task queue. Pending is work waiting for a worker; claimed is
            work actively processing. Failed is a count over the live window only, not the full history.
            Open breakers pause dispatch for specific handlers.
          </>
        }
      >
        <MetricRow cols={4}>
          <Metric
            label="Pending"
            value={fmtCount(pending)}
            unit="tasks"
            tone={pending === 0 ? "muted" : "hold"}
            caption="Queued, no worker yet. Steady state is small; growth means workers are a bottleneck."
            trend={<InlineSpark values={windowed.pendingSeries} tone="hold" />}
          />
          <Metric
            label="Claimed"
            value={fmtCount(claimed)}
            unit="tasks"
            tone={claimed === 0 ? "muted" : "live"}
            caption="In-flight. Bounded by each worker's configured concurrency."
            trend={<InlineSpark values={windowed.claimedSeries} tone="live" />}
          />
          <Metric
            label="Failed (window)"
            value={fmtCount(windowed.windowFailed)}
            unit={`last ${windowed.windowSeconds || 0}s`}
            tone={windowed.windowFailed === 0 ? "muted" : "warn"}
            caption="Terminal failures (all retries exhausted) observed in the live window only."
            trend={<InlineSpark values={windowed.failedDelta} tone="warn" />}
          />
          <Metric
            label="Open breakers"
            value={fmtCount(openBreakers.length)}
            unit={`${breakers.data?.length ?? 0} tracked`}
            tone={openBreakers.length === 0 ? "muted" : "warn"}
            caption={
              openBreakers.length === 0
                ? "No handler is currently blocked by its circuit breaker."
                : "Tap to inspect which handlers are paused and why."
            }
            to="/operations"
          />
        </MetricRow>
      </Section>

      {/* ── Attention — only rendered when something actually needs it ── */}
      {showAttention && (
        <Section
          eyebrow="Needs Attention"
          title="Signals above threshold"
          description="Items here either page an on-call engineer or block a tenant's work from advancing. Each row explains what happened and links to the inspector view."
        >
          <div className="border-l-2 border-warn">
            <ul>
              {openBreakers.map((b) => (
                <li key={`cb-${b.handler}`} className="flex items-center gap-3 px-4 py-3 border-b border-rule-faint last:border-b-0 text-[13px]">
                  <Badge tone="warn" dot>{b.state}</Badge>
                  <span className="font-mono text-[12px] text-ink">{b.handler}</span>
                  <span className="text-muted">
                    {b.failure_count} of {b.failure_threshold} consecutive failures — dispatch paused
                  </span>
                  <Link to="/operations" className="ml-auto text-signal text-[11px] font-mono uppercase tracking-[0.14em] hover:underline">
                    Inspect →
                  </Link>
                </li>
              ))}
              {dlqCount > 0 && (
                <li className="flex items-center gap-3 px-4 py-3 border-b border-rule-faint last:border-b-0 text-[13px]">
                  <Badge tone="warn">DLQ</Badge>
                  <span className="text-ink">
                    {dlqCount} execution{dlqCount === 1 ? "" : "s"} waiting on operator review
                  </span>
                  <span className="text-muted">retries exhausted — no further progress</span>
                  <Link to="/operations" className="ml-auto text-signal text-[11px] font-mono uppercase tracking-[0.14em] hover:underline">
                    Open →
                  </Link>
                </li>
              )}
              {staleNodes.map((n) => (
                <li key={`sn-${n.id}`} className="flex items-center gap-3 px-4 py-3 border-b border-rule-faint last:border-b-0 text-[13px]">
                  <Badge tone="hold">stale</Badge>
                  <span className="font-mono text-[12px] text-ink">{n.name}</span>
                  <span className="text-muted">
                    no heartbeat in <Relative at={n.last_heartbeat_at} className="!inline !text-muted" /> — claimed tasks will requeue
                  </span>
                </li>
              ))}
              {drainingNodes.map((n) => (
                <li key={`dn-${n.id}`} className="flex items-center gap-3 px-4 py-3 border-b border-rule-faint last:border-b-0 text-[13px]">
                  <Badge tone="hold">draining</Badge>
                  <span className="font-mono text-[12px] text-ink">{n.name}</span>
                  <span className="text-muted">finishing in-flight work, accepting no new claims</span>
                </li>
              ))}
              {badHandlers.map((h) => (
                <li key={`bh-${h.name}`} className="flex items-center gap-3 px-4 py-3 border-b border-rule-faint last:border-b-0 text-[13px]">
                  <Badge tone="warn">handler</Badge>
                  <span className="font-mono text-[12px] text-ink">{h.name}</span>
                  <span className="text-warn tabular font-mono text-[12px]">
                    {h.failureRate.toFixed(h.failureRate < 10 ? 1 : 0)}% failure
                  </span>
                  <span className="text-muted">above the 10% alert threshold over the current window</span>
                </li>
              ))}
            </ul>
          </div>
        </Section>
      )}

      {/* ── Empty state ──────────────────────────────────────────────── */}
      {isEmpty && (
        <Section
          eyebrow="Getting Started"
          title="No work has flowed through yet"
          description="This cluster is online and waiting. Define a sequence, wire a trigger, or schedule a cron job — activity will appear on this page within a second of the first task."
        >
          <div className="py-6 grid grid-cols-1 md:grid-cols-3 gap-8">
            <StepCard
              n="01"
              title="Define a sequence"
              body="Describe the steps your workflow must take. Each step is a named handler and some retry policy."
              to="/sequences"
            />
            <StepCard
              n="02"
              title="Wire a trigger"
              body="Attach an external event (webhook, cron, manual API call) to your sequence so instances can start."
              to="/triggers"
            />
            <StepCard
              n="03"
              title="Start a worker"
              body="Run a worker process (any language) that polls for handler tasks. You'll see it show up under Nodes below."
              to="/operations"
            />
          </div>
        </Section>
      )}

      {/* ── Handlers table ───────────────────────────────────────────── */}
      {handlerRows.length > 0 && (
        <Section
          eyebrow="Handlers"
          title="Per-handler failure distribution"
          description={
            <>
              Every handler seen in the current task stats, sorted by failure rate (worst first), with
              volume as the tiebreaker. A steady 0% column is what you want; anything above 10% gets
              promoted to the Attention panel. Distribution is a stacked bar: pending · claimed ·
              completed · failed.
            </>
          }
          meta={<>{handlerRows.length} tracked · sorted by failure %</>}
        >
          <Table>
            <THead>
              <TH>Handler</TH>
              <TH className="w-[28%]">Distribution</TH>
              <TH className="text-right">Failure %</TH>
              <TH className="text-right">Pending</TH>
              <TH className="text-right">Claimed</TH>
              <TH className="text-right">Completed</TH>
              <TH className="text-right">Failed</TH>
            </THead>
            <tbody>
              {handlerRows.map((h) => (
                <TR key={h.name}>
                  <TD className="font-mono text-[12px] text-ink">{h.name}</TD>
                  <TD>
                    <StateBar segments={h.segments} height={6} />
                  </TD>
                  <TD className="text-right tabular font-mono text-[12px]">
                    {h.total === 0 ? (
                      <span className="text-faint">—</span>
                    ) : (
                      <span
                        className={
                          h.failureRate >= 10
                            ? "text-warn"
                            : h.failureRate > 0
                              ? "text-hold"
                              : "text-muted"
                        }
                      >
                        {h.failureRate.toFixed(h.failureRate < 10 ? 1 : 0)}%
                      </span>
                    )}
                  </TD>
                  <TD className="text-right text-hold tabular font-mono text-[12px]">{h.pending}</TD>
                  <TD className="text-right text-live tabular font-mono text-[12px]">{h.claimed}</TD>
                  <TD className="text-right text-ok tabular font-mono text-[12px]">{h.completed}</TD>
                  <TD className="text-right text-warn tabular font-mono text-[12px]">{h.failed}</TD>
                </TR>
              ))}
            </tbody>
          </Table>
        </Section>
      )}

      {/* ── Cluster / Nodes ──────────────────────────────────────────── */}
      <Section
        eyebrow="Cluster"
        title="Nodes & heartbeats"
        description="Every process registered against this engine. Active + recent heartbeat is nominal; stale or draining rows bubble up to Attention above."
        meta={<>{data.active_workers.length} claiming · {activeNodes.length} active</>}
      >
        <Table>
          <THead>
            <TH>Node</TH>
            <TH>Status</TH>
            <TH>Last heartbeat</TH>
            <TH>Registered</TH>
          </THead>
          <tbody>
            {(nodes.data ?? []).map((n) => {
              const age = Date.now() - new Date(n.last_heartbeat_at).getTime();
              const stale = n.status === "active" && age > staleCutoffMs;
              const tone = n.drain
                ? "warn"
                : stale
                  ? "hold"
                  : n.status === "active"
                    ? "ok"
                    : "dim";
              const label = n.drain ? "draining" : stale ? "stale" : n.status;
              return (
                <TR key={n.id}>
                  <TD className="font-mono text-[12px] text-ink">{n.name}</TD>
                  <TD>
                    <Badge tone={tone} dot live={n.status === "active" && !stale && !n.drain}>
                      {label}
                    </Badge>
                  </TD>
                  <TD className="text-ink-dim">
                    <Relative at={n.last_heartbeat_at} />
                  </TD>
                  <TD className="text-muted">
                    <Relative at={n.registered_at} />
                  </TD>
                </TR>
              );
            })}
            {nodes.loading && !nodes.data && (
              <Empty colSpan={4}>
                Loading nodes…
              </Empty>
            )}
            {nodes.data && nodes.data.length === 0 && (
              <Empty colSpan={4}>
                No cluster nodes registered. Start a worker to see it here.
              </Empty>
            )}
          </tbody>
        </Table>
      </Section>

      {/* ── Stale-feed warning (if we have last-good data + a live error) */}
      {stats.error && stats.data && (
        <div className="border-l-2 border-warn pl-4 text-[12px] font-mono uppercase tracking-[0.14em] text-warn">
          stats feed stale — {stats.error.message}
        </div>
      )}
    </div>
  );
}

/* ── Helpers ────────────────────────────────────────────────────────── */

function capitalize(s: string): string {
  return s ? s[0]!.toUpperCase() + s.slice(1) : s;
}

function StepCard({
  n,
  title,
  body,
  to,
}: {
  n: string;
  title: string;
  body: string;
  to: string;
}) {
  return (
    <Link to={to} className="block group">
      <div className="font-mono text-[11px] tracking-[0.16em] text-signal mb-3">{n}</div>
      <div className="section-title mb-2 group-hover:text-signal transition-colors">{title}</div>
      <p className="text-[13px] leading-relaxed text-muted">{body}</p>
      <div className="mt-4 font-mono text-[10px] uppercase tracking-[0.16em] text-muted group-hover:text-signal transition-colors">
        Open →
      </div>
    </Link>
  );
}

/* ── Handler aggregation ────────────────────────────────────────────── */

type HandlerRow = {
  name: string;
  pending: number;
  claimed: number;
  completed: number;
  failed: number;
  total: number;
  failureRate: number;
  segments: StateSegment[];
};

function useHandlerRows(data: WorkerTaskStats | null): HandlerRow[] {
  return useMemo(() => {
    if (!data) return [];
    const rows: HandlerRow[] = Object.entries(data.by_handler).map(([name, counts]) => {
      const p = counts["pending"] ?? 0;
      const c = counts["claimed"] ?? 0;
      const ok = counts["completed"] ?? 0;
      const f = counts["failed"] ?? 0;
      const totalFinished = ok + f;
      const failureRate = totalFinished > 0 ? (f / totalFinished) * 100 : 0;
      return {
        name,
        pending: p,
        claimed: c,
        completed: ok,
        failed: f,
        total: p + c + ok + f,
        failureRate,
        segments: [
          { label: "pending", value: p, tone: "hold" },
          { label: "claimed", value: c, tone: "live" },
          { label: "completed", value: ok, tone: "ok" },
          { label: "failed", value: f, tone: "warn" },
        ],
      };
    });
    rows.sort((a, b) => {
      if (b.failureRate !== a.failureRate) return b.failureRate - a.failureRate;
      return b.total - a.total;
    });
    return rows;
  }, [data]);
}
