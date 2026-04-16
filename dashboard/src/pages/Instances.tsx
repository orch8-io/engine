import { useState, useCallback } from "react";
import { Link } from "react-router-dom";
import { usePolling } from "../hooks/usePolling";
import { listInstances, type TaskInstance, type InstanceState } from "../api";

const STATE_COLORS: Record<InstanceState, string> = {
  scheduled: "text-muted",
  running: "text-accent",
  waiting: "text-warning",
  paused: "text-warning",
  completed: "text-success",
  failed: "text-danger",
  cancelled: "text-muted",
};

const STATES: InstanceState[] = [
  "scheduled",
  "running",
  "waiting",
  "paused",
  "completed",
  "failed",
  "cancelled",
];

export default function Instances() {
  const [stateFilter, setStateFilter] = useState("");
  const [namespaceFilter, setNamespaceFilter] = useState("");
  const [tenantFilter, setTenantFilter] = useState("");

  const fetcher = useCallback(
    () =>
      listInstances({
        state: stateFilter || undefined,
        namespace: namespaceFilter || undefined,
        tenant_id: tenantFilter || undefined,
        limit: "100",
      }),
    [stateFilter, namespaceFilter, tenantFilter],
  );
  const { data: instances, loading, error } = usePolling<TaskInstance[]>(fetcher);

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Instances</h1>

      <div className="flex gap-3 flex-wrap">
        <select
          value={stateFilter}
          onChange={(e) => setStateFilter(e.target.value)}
          className="bg-card border border-border rounded px-3 py-1.5 text-sm text-foreground"
        >
          <option value="">All states</option>
          {STATES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <input
          type="text"
          placeholder="Namespace..."
          value={namespaceFilter}
          onChange={(e) => setNamespaceFilter(e.target.value)}
          className="bg-card border border-border rounded px-3 py-1.5 text-sm w-48 text-foreground"
        />
        <input
          type="text"
          placeholder="Tenant ID..."
          value={tenantFilter}
          onChange={(e) => setTenantFilter(e.target.value)}
          className="bg-card border border-border rounded px-3 py-1.5 text-sm w-48 text-foreground"
        />
      </div>

      {error && (
        <div className="rounded border border-danger/40 bg-danger/10 text-danger p-3 text-sm">
          {error.message}
        </div>
      )}
      {loading && !instances && <div className="text-muted">Loading...</div>}

      {instances && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border text-left text-muted">
                <th className="pb-2 pr-3">State</th>
                <th className="pb-2 pr-3">ID</th>
                <th className="pb-2 pr-3">Namespace</th>
                <th className="pb-2 pr-3">Tenant</th>
                <th className="pb-2 pr-3">Next fire</th>
                <th className="pb-2">Updated</th>
              </tr>
            </thead>
            <tbody>
              {instances.map((i) => (
                <tr
                  key={i.id}
                  className="border-b border-border/50 hover:bg-card/80 transition-colors"
                >
                  <td className={`py-2 pr-3 font-medium ${STATE_COLORS[i.state] ?? ""}`}>
                    {i.state}
                  </td>
                  <td className="py-2 pr-3 font-mono text-xs">
                    <Link to={`/instances/${i.id}`} className="text-primary hover:underline">
                      {i.id.slice(0, 8)}…
                    </Link>
                  </td>
                  <td className="py-2 pr-3 font-mono">{i.namespace}</td>
                  <td className="py-2 pr-3 font-mono text-muted">{i.tenant_id}</td>
                  <td className="py-2 pr-3 text-muted">
                    {i.next_fire_at ? new Date(i.next_fire_at).toLocaleString() : "-"}
                  </td>
                  <td className="py-2 text-muted">{new Date(i.updated_at).toLocaleString()}</td>
                </tr>
              ))}
              {instances.length === 0 && (
                <tr>
                  <td colSpan={6} className="py-8 text-center text-muted">
                    No instances found
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
