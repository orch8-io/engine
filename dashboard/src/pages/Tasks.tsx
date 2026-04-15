import { useState, useCallback } from "react";
import { usePolling } from "../hooks/usePolling";
import { listWorkerTasks, type WorkerTask } from "../api";

const STATE_COLORS: Record<string, string> = {
  pending: "text-warning",
  claimed: "text-accent",
  completed: "text-success",
  failed: "text-danger",
};

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
  const { data: tasks, loading } = usePolling<WorkerTask[]>(fetcher);

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Worker Tasks</h1>

      <div className="flex gap-4 flex-wrap">
        <select
          value={stateFilter}
          onChange={(e) => setStateFilter(e.target.value)}
          className="bg-card border border-border rounded px-3 py-1.5 text-sm text-foreground"
        >
          <option value="">All states</option>
          <option value="pending">Pending</option>
          <option value="claimed">Claimed</option>
          <option value="completed">Completed</option>
          <option value="failed">Failed</option>
        </select>
        <input
          type="text"
          placeholder="Filter by handler..."
          value={handlerFilter}
          onChange={(e) => setHandlerFilter(e.target.value)}
          className="bg-card border border-border rounded px-3 py-1.5 text-sm w-64 text-foreground"
        />
      </div>

      {loading && !tasks && <div className="text-muted">Loading...</div>}

      {tasks && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border text-left text-muted">
                <th className="pb-2 pr-3">State</th>
                <th className="pb-2 pr-3">Handler</th>
                <th className="pb-2 pr-3">Block</th>
                <th className="pb-2 pr-3">Worker</th>
                <th className="pb-2 pr-3">Attempt</th>
                <th className="pb-2">Created</th>
              </tr>
            </thead>
            <tbody>
              {tasks.map((t) => (
                <tr
                  key={t.id}
                  onClick={() => setSelected(selected?.id === t.id ? null : t)}
                  className={`border-b border-border/50 cursor-pointer hover:bg-card/80 transition-colors ${
                    selected?.id === t.id ? "bg-primary/5" : ""
                  }`}
                >
                  <td className={`py-2 pr-3 font-medium ${STATE_COLORS[t.state] ?? ""}`}>
                    {t.state}
                  </td>
                  <td className="py-2 pr-3 font-mono">{t.handler_name}</td>
                  <td className="py-2 pr-3 font-mono text-muted">{t.block_id}</td>
                  <td className="py-2 pr-3 font-mono text-muted">{t.worker_id ?? "-"}</td>
                  <td className="py-2 pr-3">{t.attempt}</td>
                  <td className="py-2 text-muted">{new Date(t.created_at).toLocaleString()}</td>
                </tr>
              ))}
              {tasks.length === 0 && (
                <tr>
                  <td colSpan={6} className="py-8 text-center text-muted">
                    No tasks found
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {selected && (
        <div className="border border-border rounded-lg bg-card p-5 space-y-4">
          <div className="flex justify-between items-start">
            <h2 className="text-lg font-semibold">Task Detail</h2>
            <button
              onClick={() => setSelected(null)}
              className="text-muted hover:text-foreground text-sm"
            >
              Close
            </button>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-4 text-sm">
            <Field label="ID" value={selected.id} mono />
            <Field label="Instance" value={selected.instance_id} mono />
            <Field label="Handler" value={selected.handler_name} mono />
            <Field label="Block" value={selected.block_id} mono />
            <Field label="State" value={selected.state} />
            <Field label="Attempt" value={String(selected.attempt)} />
            <Field label="Worker" value={selected.worker_id ?? "-"} mono />
            <Field label="Queue" value={selected.queue_name ?? "-"} mono />
            <Field
              label="Timeout"
              value={selected.timeout_ms ? `${selected.timeout_ms}ms` : "-"}
            />
            <Field label="Created" value={new Date(selected.created_at).toLocaleString()} />
            <Field
              label="Claimed"
              value={
                selected.claimed_at ? new Date(selected.claimed_at).toLocaleString() : "-"
              }
            />
            <Field
              label="Completed"
              value={
                selected.completed_at
                  ? new Date(selected.completed_at).toLocaleString()
                  : "-"
              }
            />
          </div>
          {selected.error_message && (
            <div>
              <div className="text-sm text-muted mb-1">Error</div>
              <div className="bg-danger/10 text-danger rounded p-3 text-sm font-mono">
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
          {selected.output && <JsonSection label="Output" data={selected.output} />}
        </div>
      )}
    </div>
  );
}

function Field({
  label,
  value,
  mono,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div>
      <div className="text-muted text-xs mb-0.5">{label}</div>
      <div className={`truncate ${mono ? "font-mono text-xs" : ""}`}>{value}</div>
    </div>
  );
}

function JsonSection({ label, data }: { label: string; data: unknown }) {
  return (
    <div>
      <div className="text-sm text-muted mb-1">{label}</div>
      <pre className="bg-secondary/50 rounded p-3 text-xs font-mono overflow-x-auto max-h-48">
        {JSON.stringify(data, null, 2)}
      </pre>
    </div>
  );
}
