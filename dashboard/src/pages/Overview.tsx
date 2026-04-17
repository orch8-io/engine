import { useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { usePolling } from "../hooks/usePolling";
import { getWorkerTaskStats, type WorkerTaskStats, AuthError } from "../api";
import StatCard from "../components/StatCard";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Panel, PanelBody, PanelHeader } from "../components/ui/Panel";
import { Badge } from "../components/ui/Badge";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { StateBar, StateBarLegend, type StateSegment } from "../components/ui/StateBar";
import { SkeletonStatCard, SkeletonTable } from "../components/ui/Skeleton";

export default function Overview() {
  const navigate = useNavigate();
  const fetcher = useCallback(() => getWorkerTaskStats(), []);
  const { data, error, loading, updatedAt, refresh } =
    usePolling<WorkerTaskStats>(fetcher);

  if (error instanceof AuthError) {
    navigate("/settings");
    return null;
  }

  const meta = <PageMeta updatedAt={updatedAt} onRefresh={refresh} />;

  if (loading && !data) {
    return (
      <div className="space-y-8">
        <PageHeader eyebrow="Operator" title="Overview" actions={meta} />
        <section className="grid grid-cols-2 gap-3 md:grid-cols-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <SkeletonStatCard key={i} />
          ))}
        </section>
        <SkeletonTable rows={5} cols={5} />
      </div>
    );
  }

  if (error) {
    return (
      <div className="space-y-6">
        <PageHeader eyebrow="Operator" title="Overview" actions={meta} />
        <div className="rounded-md border border-warn/40 bg-warn/10 text-warn p-3 text-[13px]">
          {error.message}
        </div>
      </div>
    );
  }

  if (!data) return null;

  const pending = data.by_state["pending"] ?? 0;
  const claimed = data.by_state["claimed"] ?? 0;
  const completed = data.by_state["completed"] ?? 0;
  const failed = data.by_state["failed"] ?? 0;
  const total = pending + claimed + completed + failed;

  const globalSegments: StateSegment[] = [
    { label: "pending", value: pending, tone: "hold" },
    { label: "claimed", value: claimed, tone: "live" },
    { label: "completed", value: completed, tone: "ok" },
    { label: "failed", value: failed, tone: "warn" },
  ];

  const handlers = Object.entries(data.by_handler).sort(
    (a, b) =>
      Object.values(b[1]).reduce((s, n) => s + n, 0) -
      Object.values(a[1]).reduce((s, n) => s + n, 0),
  );

  return (
    <div className="space-y-8">
      <PageHeader
        eyebrow="Operator"
        title="Overview"
        description="Worker throughput and queue health across all handlers."
        actions={meta}
      />

      {/* Global state distribution — one Tufte bar, then counts. */}
      <section className="space-y-3">
        <div className="flex items-baseline justify-between">
          <span className="eyebrow">Task state distribution</span>
          <span className="text-[11px] font-mono tabular text-faint">
            {total.toLocaleString()} TOTAL
          </span>
        </div>
        <StateBar segments={globalSegments} height={10} />
        <StateBarLegend segments={globalSegments} />
      </section>

      <section className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <StatCard label="Pending" value={pending} tone="hold" sub={subPct(pending, total)} />
        <StatCard label="Claimed" value={claimed} tone="live" sub={subPct(claimed, total)} />
        <StatCard label="Completed" value={completed} tone="ok" sub={subPct(completed, total)} />
        <StatCard label="Failed" value={failed} tone="warn" sub={subPct(failed, total)} />
      </section>

      {handlers.length > 0 && (
        <Panel>
          <PanelHeader>
            <span className="eyebrow">By handler</span>
            <span className="ml-auto text-[11px] text-faint font-mono tabular">
              {handlers.length} HANDLERS
            </span>
          </PanelHeader>
          <PanelBody padded={false}>
            <Table>
              <THead>
                <TH>Handler</TH>
                <TH className="w-[40%]">Distribution</TH>
                <TH className="text-right">Pending</TH>
                <TH className="text-right">Claimed</TH>
                <TH className="text-right">Completed</TH>
                <TH className="text-right">Failed</TH>
              </THead>
              <tbody>
                {handlers.map(([name, counts]) => {
                  const p = counts["pending"] ?? 0;
                  const c = counts["claimed"] ?? 0;
                  const ok = counts["completed"] ?? 0;
                  const f = counts["failed"] ?? 0;
                  const segs: StateSegment[] = [
                    { label: "pending", value: p, tone: "hold" },
                    { label: "claimed", value: c, tone: "live" },
                    { label: "completed", value: ok, tone: "ok" },
                    { label: "failed", value: f, tone: "warn" },
                  ];
                  return (
                    <TR key={name}>
                      <TD className="pl-4 font-mono text-[12px]">{name}</TD>
                      <TD>
                        <StateBar segments={segs} height={6} />
                      </TD>
                      <TD className="text-right text-hold tabular">{p}</TD>
                      <TD className="text-right text-live tabular">{c}</TD>
                      <TD className="text-right text-ok tabular">{ok}</TD>
                      <TD className="text-right text-warn tabular pr-4">{f}</TD>
                    </TR>
                  );
                })}
              </tbody>
            </Table>
          </PanelBody>
        </Panel>
      )}

      <section>
        <div className="flex items-baseline justify-between mb-3">
          <span className="eyebrow">Active workers</span>
          <span className="text-[11px] text-faint font-mono tabular">
            {data.active_workers.length} ONLINE
          </span>
        </div>
        {data.active_workers.length > 0 ? (
          <div className="flex flex-wrap gap-1.5">
            {data.active_workers.map((w) => (
              <Badge key={w} tone="live" dot live>
                {w}
              </Badge>
            ))}
          </div>
        ) : (
          <Panel>
            <PanelBody>
              <div className="py-6 text-center text-muted text-[13px]">
                No workers currently claimed tasks.
              </div>
            </PanelBody>
          </Panel>
        )}
        {data.active_workers.length === 0 && handlers.length === 0 && (
          <Table>
            <tbody>
              <Empty>No data yet.</Empty>
            </tbody>
          </Table>
        )}
      </section>
    </div>
  );
}

function subPct(n: number, total: number): string {
  if (total === 0) return "—";
  const pct = (n / total) * 100;
  return `${pct.toFixed(pct < 10 ? 1 : 0)}% of total`;
}
