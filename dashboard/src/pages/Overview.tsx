import { useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { usePolling } from "../hooks/usePolling";
import { getWorkerTaskStats, type WorkerTaskStats, AuthError } from "../api";
import StatCard from "../components/StatCard";
import { PageHeader } from "../components/ui/PageHeader";
import { Panel, PanelBody, PanelHeader } from "../components/ui/Panel";
import { Badge } from "../components/ui/Badge";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";

export default function Overview() {
  const navigate = useNavigate();
  const fetcher = useCallback(() => getWorkerTaskStats(), []);
  const { data, error, loading } = usePolling<WorkerTaskStats>(fetcher);

  if (error instanceof AuthError) {
    navigate("/settings");
    return null;
  }

  if (loading && !data) {
    return (
      <>
        <PageHeader eyebrow="Operator" title="Overview" />
        <div className="mt-6 text-muted text-sm">Loading stats…</div>
      </>
    );
  }

  if (error) {
    return (
      <>
        <PageHeader eyebrow="Operator" title="Overview" />
        <div className="mt-6 rounded-md border border-warn/40 bg-warn/10 text-warn p-3 text-[13px]">
          {error.message}
        </div>
      </>
    );
  }

  if (!data) return null;

  const pending = data.by_state["pending"] ?? 0;
  const claimed = data.by_state["claimed"] ?? 0;
  const completed = data.by_state["completed"] ?? 0;
  const failed = data.by_state["failed"] ?? 0;
  const total = pending + claimed + completed + failed;

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
      />

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
                <TH className="text-right">Pending</TH>
                <TH className="text-right">Claimed</TH>
                <TH className="text-right">Completed</TH>
                <TH className="text-right">Failed</TH>
              </THead>
              <tbody>
                {handlers.map(([name, counts]) => (
                  <TR key={name}>
                    <TD className="pl-4 font-mono text-[12px]">{name}</TD>
                    <TD className="text-right text-hold tabular">{counts["pending"] ?? 0}</TD>
                    <TD className="text-right text-live tabular">{counts["claimed"] ?? 0}</TD>
                    <TD className="text-right text-ok tabular">{counts["completed"] ?? 0}</TD>
                    <TD className="text-right text-warn tabular pr-4">
                      {counts["failed"] ?? 0}
                    </TD>
                  </TR>
                ))}
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
