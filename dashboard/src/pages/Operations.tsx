import { useCallback, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  listDlq,
  listCircuitBreakers,
  listClusterNodes,
  listWorkerTasks,
  resetCircuitBreaker,
  drainClusterNode,
  retryInstance,
  type TaskInstance,
  type CircuitBreakerState,
  type ClusterNode,
  type BreakerState,
  type WorkerTask,
} from "../api";
import { usePolling } from "../hooks/usePolling";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Button } from "../components/ui/Button";
import { Badge } from "../components/ui/Badge";
import { Relative } from "../components/ui/Relative";
import { IconRetry, IconRefresh, IconStop } from "../components/ui/Icons";
import { SkeletonTable } from "../components/ui/Skeleton";

const BREAKER_TONE: Record<BreakerState, "ok" | "warn" | "hold"> = {
  closed: "ok",
  open: "warn",
  half_open: "hold",
};

/**
 * Page glossary — every domain term used on this page gets a definition.
 * If an operator has to guess what a word means, this list has failed.
 */
const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "DLQ",
    definition:
      "Dead-letter queue. The parking lot for executions that exhausted all retry attempts. They are finished from the engine's point of view — nothing will run them again until you retry manually.",
  },
  {
    term: "Retry",
    definition:
      "Re-queues the execution from the failed step. Useful when the underlying failure was transient (network blip, upstream outage) and has since been resolved.",
  },
  {
    term: "Circuit breaker",
    definition: (
      <>
        A per-handler trip switch. When a handler fails too many times in a
        row, the breaker opens and the engine stops dispatching work to it —
        protecting your downstream system from being hammered while it's
        unhealthy.
      </>
    ),
  },
  {
    term: "closed",
    definition:
      "Breaker healthy. The handler is receiving work normally. Failure count resets on each success.",
  },
  {
    term: "open",
    definition:
      "Breaker tripped. The handler is quarantined — no new work is dispatched to it. After a cooldown the breaker moves to half-open to probe for recovery.",
  },
  {
    term: "half_open",
    definition:
      "Breaker probing. A small number of requests are allowed through to test whether the handler has recovered. A success closes the breaker, a failure re-opens it.",
  },
  {
    term: "Failures / threshold",
    definition:
      "Consecutive failures counted since the last success / the number at which the breaker will trip open. Example: 3 / 5 means two more failures will open the breaker.",
  },
  {
    term: "Node",
    definition:
      "A worker process that has registered with the engine. Multiple nodes share work; each owns a fraction of the task stream.",
  },
  {
    term: "active",
    definition:
      "Node is alive, heartbeating, and accepting new work.",
  },
  {
    term: "drain",
    definition:
      "Node has been asked to stop accepting new work but keeps running what it already claimed. Used for safe rolling deploys — when the queue is empty, you can shut the node down.",
  },
  {
    term: "Heartbeat",
    definition:
      "A ping the node sends every few seconds. If it stops, the engine considers the node stale and other nodes will steal its in-flight work.",
  },
];

export default function Operations() {
  const dlqFetcher = useCallback(() => listDlq({ limit: "50" }), []);
  const cbFetcher = useCallback(() => listCircuitBreakers(), []);
  const nodesFetcher = useCallback(() => listClusterNodes(), []);
  const failedTasksFetcher = useCallback(
    () => listWorkerTasks({ state: "failed", limit: "500" }),
    [],
  );

  const dlq = usePolling<TaskInstance[]>(dlqFetcher);
  const cbs = usePolling<CircuitBreakerState[]>(cbFetcher);
  const nodes = usePolling<ClusterNode[]>(nodesFetcher);
  const failedTasks = usePolling<WorkerTask[]>(failedTasksFetcher, 5000);

  const [toast, setToast] = useState<string | null>(null);

  const showToast = useCallback((message: string) => {
    setToast(message);
    setTimeout(() => setToast(null), 2500);
  }, []);

  // Latest failed task per instance → feeds the "reason" column in the DLQ table.
  const reasonByInstance = useMemo(() => {
    const m = new Map<string, WorkerTask>();
    for (const t of failedTasks.data ?? []) {
      if (!t.error_message) continue;
      const prev = m.get(t.instance_id);
      if (!prev || (t.completed_at ?? "") > (prev.completed_at ?? "")) {
        m.set(t.instance_id, t);
      }
    }
    return m;
  }, [failedTasks.data]);

  const refreshAll = () => {
    dlq.refresh();
    cbs.refresh();
    nodes.refresh();
    failedTasks.refresh();
  };

  const latest =
    [dlq.updatedAt, cbs.updatedAt, nodes.updatedAt, failedTasks.updatedAt]
      .filter((x): x is string => !!x)
      .sort()
      .at(-1) ?? null;

  const errors = [
    dlq.error,
    cbs.error,
    nodes.error,
    failedTasks.error,
  ].filter(Boolean) as Error[];

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Operations"
        description="Incident surface for the three failure modes of a durable engine: executions that gave up (DLQ), handlers that are sick (circuit breakers), and worker processes that may be gone (cluster nodes)."
        actions={<PageMeta updatedAt={latest} onRefresh={refreshAll} />}
      />

      <Glossary items={PAGE_GLOSSARY} />

      {errors.map((err, i) => (
        <div key={i} className="notice notice-warn">{err.message}</div>
      ))}
      {toast && <div className="notice notice-ok">{toast}</div>}

      <DlqSection
        data={dlq.data}
        loading={dlq.loading && !dlq.data}
        reasonByInstance={reasonByInstance}
        onChange={dlq.refresh}
        showToast={showToast}
      />
      <BreakersSection
        data={cbs.data}
        loading={cbs.loading && !cbs.data}
        onChange={cbs.refresh}
        showToast={showToast}
      />
      <NodesSection
        data={nodes.data}
        loading={nodes.loading && !nodes.data}
        onChange={nodes.refresh}
        showToast={showToast}
      />
    </div>
  );
}

function DlqSection({
  data,
  loading,
  reasonByInstance,
  onChange,
  showToast,
}: {
  data: TaskInstance[] | null;
  loading: boolean;
  reasonByInstance: Map<string, WorkerTask>;
  onChange: () => void;
  showToast: (message: string) => void;
}) {
  const [pending, setPending] = useState<Set<string>>(new Set());

  const retry = async (id: string) => {
    setPending((prev) => new Set(prev).add(id));
    try {
      await retryInstance(id);
      showToast("Retry scheduled");
      onChange();
    } catch (e) {
      showToast(`Retry failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setPending((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
    }
  };

  const visible = data?.filter((i) => !pending.has(i.id)) ?? [];
  const count = visible.length;

  return (
    <Section
      eyebrow="Dead letter queue"
      title="Exhausted executions"
      description={
        <>
          These executions failed their final retry and are no longer being
          scheduled. The <strong className="text-ink">Reason</strong> column
          shows the last error surfaced by the handler. Use{" "}
          <strong className="text-ink">Retry</strong> after you believe the
          underlying cause is resolved — the execution will resume from the
          failed step, not from the beginning.
        </>
      }
      meta={
        <span>
          <span className="text-faint">COUNT</span>{" "}
          <span className={count > 0 ? "text-warn" : "text-ink-dim"}>
            {data ? count : "…"}
          </span>
        </span>
      }
    >
      {loading ? (
        <SkeletonTable rows={6} cols={5} />
      ) : (
        <Table>
          <THead>
            <TH>Execution</TH>
            <TH>Tenant / Namespace</TH>
            <TH>Reason of last failure</TH>
            <TH>Updated</TH>
            <TH className="text-right">Actions</TH>
          </THead>
          <tbody>
            {visible.map((i) => {
              const reason = reasonByInstance.get(i.id);
              return (
                <TR key={i.id}>
                  <TD className="align-top">
                    <Link
                      to={`/instances/${i.id}`}
                      className="font-mono text-[12px] text-signal hover:underline"
                    >
                      {i.id.slice(0, 8)}…
                    </Link>
                  </TD>
                  <TD className="font-mono text-[12px] align-top">
                    {i.tenant_id}
                    <span className="text-faint"> / </span>
                    {i.namespace}
                  </TD>
                  <TD className="align-top max-w-[48ch]">
                    {reason ? (
                      <div className="space-y-0.5">
                        <div
                          className="text-[12px] font-mono text-warn line-clamp-2"
                          title={reason.error_message ?? undefined}
                        >
                          {reason.error_message}
                        </div>
                        <div className="text-[10px] font-mono uppercase tracking-wider text-faint">
                          {reason.handler_name} · {reason.block_id} · attempt{" "}
                          {reason.attempt}
                          {reason.error_retryable === false &&
                            " · permanent (non-retryable)"}
                        </div>
                      </div>
                    ) : (
                      <span
                        className="text-faint text-[12px]"
                        title="No failed task found for this instance — it may have failed at dispatch, not at handler execution."
                      >
                        —
                      </span>
                    )}
                  </TD>
                  <TD className="align-top">
                    <Relative at={i.updated_at} />
                  </TD>
                  <TD className="text-right align-top">
                    <Button
                      variant="primary"
                      size="sm"
                      onClick={() => retry(i.id)}
                      title="Re-queue this execution from the failed step"
                    >
                      <IconRetry size={13} /> Retry
                    </Button>
                  </TD>
                </TR>
              );
            })}
            {visible.length === 0 && (
              <Empty colSpan={99}>
                No failed executions. Every execution either completed or is
                still being retried.
              </Empty>
            )}
          </tbody>
        </Table>
      )}
    </Section>
  );
}

function BreakersSection({
  data,
  loading,
  onChange,
  showToast,
}: {
  data: CircuitBreakerState[] | null;
  loading: boolean;
  onChange: () => void;
  showToast: (message: string) => void;
}) {
  const [pending, setPending] = useState<Set<string>>(new Set());

  const reset = async (handler: string) => {
    setPending((prev) => new Set(prev).add(handler));
    try {
      await resetCircuitBreaker(handler);
      showToast("Breaker reset");
      onChange();
    } catch (e) {
      showToast(`Reset failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setPending((prev) => {
        const next = new Set(prev);
        next.delete(handler);
        return next;
      });
    }
  };

  const visible = data?.filter((b) => !pending.has(b.handler)) ?? [];
  const open = visible.filter((b) => b.state === "open").length;
  const halfOpen = visible.filter((b) => b.state === "half_open").length;

  return (
    <Section
      eyebrow="Circuit breakers"
      title="Handler health guards"
      description={
        <>
          Each handler has a breaker that opens after enough consecutive
          failures. An <em>open</em> breaker stops the engine from dispatching
          more work to that handler — protecting the downstream system. The
          breaker auto-probes (half-open) after a cooldown; use{" "}
          <strong className="text-ink">Reset</strong> only if you've confirmed
          the handler is healthy and want to force it back into service early.
        </>
      }
      meta={
        <>
          <span>
            <span className="text-faint">OPEN</span>{" "}
            <span className={open > 0 ? "text-warn" : "text-ink-dim"}>
              {open}
            </span>
          </span>
          <span>
            <span className="text-faint">HALF</span>{" "}
            <span className={halfOpen > 0 ? "text-hold" : "text-ink-dim"}>
              {halfOpen}
            </span>
          </span>
          <span>
            <span className="text-faint">TOTAL</span>{" "}
            <span className="text-ink-dim">{data?.length ?? "…"}</span>
          </span>
        </>
      }
    >
      {loading ? (
        <SkeletonTable rows={6} cols={6} />
      ) : (
        <Table>
          <THead>
            <TH>Handler</TH>
            <TH>State</TH>
            <TH className="text-right">Failures</TH>
            <TH className="text-right">Threshold</TH>
            <TH>Opened at</TH>
            <TH className="text-right">Actions</TH>
          </THead>
          <tbody>
            {visible.map((b) => (
              <TR key={b.handler}>
                <TD className="font-mono text-[12px]">{b.handler}</TD>
                <TD>
                  <Badge tone={BREAKER_TONE[b.state]} dot>
                    {b.state}
                  </Badge>
                </TD>
                <TD
                  className="text-right tabular"
                  title="Consecutive failures since last success"
                >
                  {b.failure_count}
                </TD>
                <TD
                  className="text-right tabular text-muted"
                  title="Number of consecutive failures that will trip the breaker open"
                >
                  {b.failure_threshold}
                </TD>
                <TD>
                  {b.opened_at ? (
                    <Relative at={b.opened_at} />
                  ) : (
                    <span className="text-faint" title="Breaker has not tripped">
                      —
                    </span>
                  )}
                </TD>
                <TD className="text-right">
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => reset(b.handler)}
                    disabled={b.state === "closed" && b.failure_count === 0}
                    title="Force the breaker back to closed — only use after verifying the handler is healthy"
                  >
                    <IconRefresh size={13} /> Reset
                  </Button>
                </TD>
              </TR>
            ))}
            {visible.length === 0 && (
              <Empty colSpan={99}>
                No circuit breakers tracked yet — no handler has failed.
              </Empty>
            )}
          </tbody>
        </Table>
      )}
    </Section>
  );
}

function NodesSection({
  data,
  loading,
  onChange,
  showToast,
}: {
  data: ClusterNode[] | null;
  loading: boolean;
  onChange: () => void;
  showToast: (message: string) => void;
}) {
  const drain = async (id: string, name: string) => {
    if (
      !confirm(
        `Drain node ${name}?\n\nIt will stop accepting new work but will finish what it has already claimed. Used for safe rolling deploys.`,
      )
    )
      return;
    try {
      await drainClusterNode(id);
      onChange();
    } catch (e) {
      showToast(`Drain failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const active = data?.filter((n) => n.status === "active").length ?? 0;
  const draining = data?.filter((n) => n.drain).length ?? 0;

  return (
    <Section
      eyebrow="Cluster nodes"
      title="Worker processes"
      description={
        <>
          Every running worker registers here and heartbeats every few
          seconds. If a node stops heartbeating the engine treats it as stale
          and reclaims its in-flight work. Use{" "}
          <strong className="text-ink">Drain</strong> before shutting a node
          down — it lets the node finish what it has already claimed without
          picking up new work.
        </>
      }
      meta={
        <>
          <span>
            <span className="text-faint">ACTIVE</span>{" "}
            <span className="text-ok">{active}</span>
          </span>
          <span>
            <span className="text-faint">DRAIN</span>{" "}
            <span className={draining > 0 ? "text-hold" : "text-ink-dim"}>
              {draining}
            </span>
          </span>
          <span>
            <span className="text-faint">TOTAL</span>{" "}
            <span className="text-ink-dim">{data?.length ?? "…"}</span>
          </span>
        </>
      }
    >
      {loading ? (
        <SkeletonTable rows={6} cols={6} />
      ) : (
        <Table>
          <THead>
            <TH>Name</TH>
            <TH>Status</TH>
            <TH>Drain</TH>
            <TH>Registered</TH>
            <TH>Last heartbeat</TH>
            <TH className="text-right">Actions</TH>
          </THead>
          <tbody>
            {data?.map((n) => (
              <TR key={n.id}>
                <TD className="font-mono text-[12px]">{n.name}</TD>
                <TD>
                  <Badge tone={n.status === "active" ? "ok" : "dim"} dot>
                    {n.status}
                  </Badge>
                </TD>
                <TD>
                  {n.drain ? (
                    <Badge tone="warn">draining</Badge>
                  ) : (
                    <span
                      className="text-faint text-[12px]"
                      title="Node is accepting new work"
                    >
                      —
                    </span>
                  )}
                </TD>
                <TD title="When the node first joined the cluster">
                  <Relative at={n.registered_at} />
                </TD>
                <TD title="Time since the node last reported in. Stale nodes are automatically reaped.">
                  <Relative at={n.last_heartbeat_at} />
                </TD>
                <TD className="text-right">
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => drain(n.id, n.name)}
                    disabled={n.drain}
                    title="Stop this node from accepting new work — safe for rolling deploys"
                  >
                    <IconStop size={13} /> Drain
                  </Button>
                </TD>
              </TR>
            ))}
            {data && data.length === 0 && (
              <Empty colSpan={99}>
                No nodes registered. Start at least one worker process for
                the engine to dispatch work.
              </Empty>
            )}
          </tbody>
        </Table>
      )}
    </Section>
  );
}
