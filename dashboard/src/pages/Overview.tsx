import { useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { usePolling } from "../hooks/usePolling";
import { getWorkerTaskStats, type WorkerTaskStats, AuthError } from "../api";
import StatCard from "../components/StatCard";

export default function Overview() {
  const navigate = useNavigate();
  const fetcher = useCallback(() => getWorkerTaskStats(), []);
  const { data, error, loading } = usePolling<WorkerTaskStats>(fetcher);

  if (error instanceof AuthError) {
    navigate("/settings");
    return null;
  }

  if (loading && !data) {
    return <div className="text-muted">Loading stats...</div>;
  }

  if (error) {
    return <div className="text-danger">Error: {error.message}</div>;
  }

  if (!data) return null;

  const pending = data.by_state["pending"] ?? 0;
  const claimed = data.by_state["claimed"] ?? 0;
  const completed = data.by_state["completed"] ?? 0;
  const failed = data.by_state["failed"] ?? 0;

  const handlers = Object.entries(data.by_handler);

  return (
    <div className="space-y-8">
      <h1 className="text-2xl font-bold">Worker Overview</h1>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard label="Pending" value={pending} color="text-warning" />
        <StatCard label="Claimed" value={claimed} color="text-accent" />
        <StatCard label="Completed" value={completed} color="text-success" />
        <StatCard label="Failed" value={failed} color="text-danger" />
      </div>

      {handlers.length > 0 && (
        <div>
          <h2 className="text-lg font-semibold mb-3">By Handler</h2>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-muted">
                  <th className="pb-2 pr-4">Handler</th>
                  <th className="pb-2 pr-4">Pending</th>
                  <th className="pb-2 pr-4">Claimed</th>
                  <th className="pb-2 pr-4">Completed</th>
                  <th className="pb-2">Failed</th>
                </tr>
              </thead>
              <tbody>
                {handlers.map(([name, counts]) => (
                  <tr key={name} className="border-b border-border/50">
                    <td className="py-2 pr-4 font-mono">{name}</td>
                    <td className="py-2 pr-4 text-warning">{counts["pending"] ?? 0}</td>
                    <td className="py-2 pr-4 text-accent">{counts["claimed"] ?? 0}</td>
                    <td className="py-2 pr-4 text-success">{counts["completed"] ?? 0}</td>
                    <td className="py-2 text-danger">{counts["failed"] ?? 0}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {data.active_workers.length > 0 && (
        <div>
          <h2 className="text-lg font-semibold mb-3">Active Workers</h2>
          <div className="flex flex-wrap gap-2">
            {data.active_workers.map((w) => (
              <span
                key={w}
                className="px-3 py-1 rounded-full bg-success/10 text-success text-sm font-mono"
              >
                {w}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
