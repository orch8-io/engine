import { useState } from "react";
import { Link } from "react-router-dom";
import { listSequenceVersions, type SequenceDefinition } from "../api";

export default function Sequences() {
  const [tenant, setTenant] = useState("");
  const [namespace, setNamespace] = useState("default");
  const [name, setName] = useState("");
  const [versions, setVersions] = useState<SequenceDefinition[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const search = async () => {
    if (!tenant || !name) {
      setError("tenant_id and name are required");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const v = await listSequenceVersions({
        tenant_id: tenant,
        namespace,
        name,
      });
      setVersions(v);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Sequences</h1>

      <div className="bg-card border border-border rounded-lg p-4 space-y-3">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <input
            type="text"
            placeholder="tenant_id"
            value={tenant}
            onChange={(e) => setTenant(e.target.value)}
            className="bg-background border border-border rounded px-3 py-1.5 text-sm text-foreground"
          />
          <input
            type="text"
            placeholder="namespace"
            value={namespace}
            onChange={(e) => setNamespace(e.target.value)}
            className="bg-background border border-border rounded px-3 py-1.5 text-sm text-foreground"
          />
          <input
            type="text"
            placeholder="sequence name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && search()}
            className="bg-background border border-border rounded px-3 py-1.5 text-sm text-foreground"
          />
        </div>
        <button
          onClick={search}
          disabled={loading}
          className="bg-primary/10 text-primary border border-primary/40 rounded px-4 py-1.5 text-sm hover:bg-primary/20 disabled:opacity-50"
        >
          {loading ? "Loading..." : "List versions"}
        </button>
        <p className="text-xs text-muted">
          The orch8 engine stores sequences keyed by (tenant, namespace, name, version). Pick a
          name to see all deployed versions and their block counts.
        </p>
      </div>

      {error && (
        <div className="rounded border border-danger/40 bg-danger/10 text-danger p-3 text-sm">
          {error}
        </div>
      )}

      {versions && versions.length === 0 && (
        <div className="text-muted text-sm">No versions found.</div>
      )}

      {versions && versions.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border text-left text-muted">
                <th className="pb-2 pr-3">Version</th>
                <th className="pb-2 pr-3">ID</th>
                <th className="pb-2 pr-3">Blocks</th>
                <th className="pb-2 pr-3">Deprecated</th>
                <th className="pb-2">Created</th>
              </tr>
            </thead>
            <tbody>
              {versions.map((v) => (
                <tr key={v.id} className="border-b border-border/50 hover:bg-card/80">
                  <td className="py-2 pr-3 font-medium">v{v.version}</td>
                  <td className="py-2 pr-3 font-mono text-xs">
                    <Link to={`/sequences/${v.id}`} className="text-primary hover:underline">
                      {v.id.slice(0, 8)}…
                    </Link>
                  </td>
                  <td className="py-2 pr-3">{v.blocks.length}</td>
                  <td className="py-2 pr-3">
                    {v.deprecated ? (
                      <span className="text-warning text-xs">deprecated</span>
                    ) : (
                      <span className="text-success text-xs">active</span>
                    )}
                  </td>
                  <td className="py-2 text-muted">
                    {new Date(v.created_at).toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
