import { useState, useCallback } from "react";
import { usePolling } from "../hooks/usePolling";
import { listWorkerTasks, type WorkerTask } from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Panel, PanelBody, PanelHeader } from "../components/ui/Panel";
import { Badge, TASK_TONE } from "../components/ui/Badge";
import { Button } from "../components/ui/Button";
import { Input, Select } from "../components/ui/Input";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Id } from "../components/ui/Mono";
import { Relative } from "../components/ui/Relative";
import { SkeletonTable } from "../components/ui/Skeleton";

export default function Tasks() {
  const [stateFilter, setStateFilter] = useState("");
  const [handlerFilter, setHandlerFilter] = useState("");
  const [selected, setSelected] = useState<WorkerTask | null>(null);

  const fetcher = useCallback(
    () =>
      listWorkerTasks({
        state: stateFilter || undefined,
        handler_name: handlerFilter || undefined,
        limit: "100",
      }),
    [stateFilter, handlerFilter],
  );
  const { data: tasks, loading, updatedAt, refresh } =
    usePolling<WorkerTask[]>(fetcher);

  const hasFilters = Boolean(stateFilter || handlerFilter);

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Operator"
        title="Worker Tasks"
        description="Every unit of work dispatched to a handler. Click a row for detail."
        actions={<PageMeta updatedAt={updatedAt} onRefresh={refresh} />}
      />

      {/* Flat filter strip */}
      <div className="grid grid-cols-1 md:grid-cols-[160px_1fr] gap-3">
        <Select
          value={stateFilter}
          onChange={(e) => setStateFilter(e.target.value)}
        >
          <option value="">All states</option>
          <option value="pending">Pending</option>
          <option value="claimed">Claimed</option>
          <option value="completed">Completed</option>
          <option value="failed">Failed</option>
        </Select>
        <Input
          type="text"
          placeholder="Filter by handler…"
          value={handlerFilter}
          onChange={(e) => setHandlerFilter(e.target.value)}
        />
      </div>

      {loading && !tasks && <SkeletonTable rows={8} cols={6} />}

      {tasks && (
        <Panel>
          <PanelBody padded={false}>
            <Table>
              <THead>
                <TH className="pl-4">State</TH>
                <TH>Handler</TH>
                <TH>Block</TH>
                <TH>Worker</TH>
                <TH className="text-right">Attempt</TH>
                <TH className="pr-4">Created</TH>
              </THead>
              <tbody>
                {tasks.map((t) => (
                  <TR
                    key={t.id}
                    onClick={() =>
                      setSelected(selected?.id === t.id ? null : t)
                    }
                    active={selected?.id === t.id}
                    className="cursor-pointer"
                  >
                    <TD className="pl-4">
                      <Badge
                        tone={TASK_TONE[t.state] ?? "dim"}
                        dot
                        live={t.state === "claimed"}
                      >
                        {t.state}
                      </Badge>
                    </TD>
                    <TD className="font-mono text-[12px]">{t.handler_name}</TD>
                    <TD className="font-mono text-[12px] text-muted">
                      {t.block_id}
                    </TD>
                    <TD className="font-mono text-[12px] text-muted">
                      {t.worker_id ?? "—"}
                    </TD>
                    <TD className="text-right tabular">{t.attempt}</TD>
                    <TD className="pr-4">
                      <Relative at={t.created_at} />
                    </TD>
                  </TR>
                ))}
                {tasks.length === 0 && (
                  <Empty>
                    {hasFilters
                      ? "No tasks match these filters — try clearing one."
                      : "No tasks yet."}
                  </Empty>
                )}
              </tbody>
            </Table>
          </PanelBody>
        </Panel>
      )}

      {selected && (
        <Panel elevated>
          <PanelHeader>
            <span className="eyebrow">Task detail</span>
            <span className="ml-auto">
              <Id value={selected.id} short={false} copy />
            </span>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setSelected(null)}
            >
              Close
            </Button>
          </PanelHeader>
          <PanelBody>
            <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
              <Field label="Instance" value={selected.instance_id} mono copy />
              <Field label="Handler" value={selected.handler_name} mono />
              <Field label="Block" value={selected.block_id} mono />
              <Field label="State" value={selected.state} />
              <Field label="Attempt" value={String(selected.attempt)} />
              <Field label="Worker" value={selected.worker_id ?? "—"} mono />
              <Field label="Queue" value={selected.queue_name ?? "—"} mono />
              <Field
                label="Timeout"
                value={selected.timeout_ms ? `${selected.timeout_ms}ms` : "—"}
              />
              <Field
                label="Created"
                value={new Date(selected.created_at).toLocaleString()}
              />
              <Field
                label="Claimed"
                value={
                  selected.claimed_at
                    ? new Date(selected.claimed_at).toLocaleString()
                    : "—"
                }
              />
              <Field
                label="Completed"
                value={
                  selected.completed_at
                    ? new Date(selected.completed_at).toLocaleString()
                    : "—"
                }
              />
            </div>

            {selected.error_message && (
              <div className="mt-5">
                <div className="eyebrow mb-1.5">Error</div>
                <div className="rounded-md border border-warn/40 bg-warn/10 text-warn p-3 text-[12px] font-mono">
                  {selected.error_message}
                  {selected.error_retryable !== null && (
                    <span className="ml-2 text-muted">
                      ({selected.error_retryable ? "retryable" : "permanent"})
                    </span>
                  )}
                </div>
              </div>
            )}

            <JsonSection label="Params" data={selected.params} />
            <JsonSection label="Context" data={selected.context} />
            {selected.output && (
              <JsonSection label="Output" data={selected.output} />
            )}
          </PanelBody>
        </Panel>
      )}
    </div>
  );
}

function Field({
  label,
  value,
  mono,
  copy,
}: {
  label: string;
  value: string;
  mono?: boolean;
  copy?: boolean;
}) {
  return (
    <div className="min-w-0">
      <div className="eyebrow mb-1">{label}</div>
      {copy && value !== "—" ? (
        <Id value={value} short={false} copy className="!text-[12px]" />
      ) : (
        <div
          className={`truncate text-[13px] ${mono ? "font-mono text-[12px]" : ""}`}
          title={value}
        >
          {value}
        </div>
      )}
    </div>
  );
}

function JsonSection({ label, data }: { label: string; data: unknown }) {
  return (
    <div className="mt-5">
      <div className="eyebrow mb-1.5">{label}</div>
      <pre className="bg-sunken border border-hairline rounded-md p-3 text-[12px] font-mono text-fg-dim overflow-x-auto max-h-48">
        {JSON.stringify(data, null, 2)}
      </pre>
    </div>
  );
}
