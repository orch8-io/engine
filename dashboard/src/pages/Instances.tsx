import { useState, useCallback } from "react";
import { Link } from "react-router-dom";
import { usePolling } from "../hooks/usePolling";
import { listInstances, type TaskInstance, type InstanceState } from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { Panel, PanelBody } from "../components/ui/Panel";
import { Badge, INSTANCE_TONE } from "../components/ui/Badge";
import { Input, Select } from "../components/ui/Input";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";

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
      <PageHeader
        eyebrow="Operator"
        title="Instances"
        description="Scheduled and running sequence instances across tenants."
      />

      <Panel>
        <PanelBody>
          <div className="grid grid-cols-1 md:grid-cols-[160px_1fr_1fr] gap-3">
            <Select
              value={stateFilter}
              onChange={(e) => setStateFilter(e.target.value)}
            >
              <option value="">All states</option>
              {STATES.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </Select>
            <Input
              type="text"
              placeholder="Namespace…"
              value={namespaceFilter}
              onChange={(e) => setNamespaceFilter(e.target.value)}
            />
            <Input
              type="text"
              placeholder="Tenant ID…"
              value={tenantFilter}
              onChange={(e) => setTenantFilter(e.target.value)}
            />
          </div>
        </PanelBody>
      </Panel>

      {error && (
        <div className="rounded-md border border-warn/40 bg-warn/10 text-warn p-3 text-[13px]">
          {error.message}
        </div>
      )}
      {loading && !instances && (
        <div className="text-muted text-sm">Loading instances…</div>
      )}

      {instances && (
        <Panel>
          <PanelBody padded={false}>
            <Table>
              <THead>
                <TH className="pl-4">State</TH>
                <TH>ID</TH>
                <TH>Namespace</TH>
                <TH>Tenant</TH>
                <TH>Next fire</TH>
                <TH className="pr-4">Updated</TH>
              </THead>
              <tbody>
                {instances.map((i) => (
                  <TR key={i.id}>
                    <TD className="pl-4">
                      <Badge
                        tone={INSTANCE_TONE[i.state] ?? "dim"}
                        dot
                        live={i.state === "running"}
                      >
                        {i.state}
                      </Badge>
                    </TD>
                    <TD className="font-mono text-[12px]">
                      <Link
                        to={`/instances/${i.id}`}
                        className="text-signal hover:underline"
                      >
                        {i.id.slice(0, 8)}…
                      </Link>
                    </TD>
                    <TD className="font-mono text-[12px]">{i.namespace}</TD>
                    <TD className="font-mono text-[12px] text-muted">{i.tenant_id}</TD>
                    <TD className="text-muted tabular">
                      {i.next_fire_at ? new Date(i.next_fire_at).toLocaleString() : "—"}
                    </TD>
                    <TD className="text-muted tabular pr-4">
                      {new Date(i.updated_at).toLocaleString()}
                    </TD>
                  </TR>
                ))}
                {instances.length === 0 && <Empty>No instances found.</Empty>}
              </tbody>
            </Table>
          </PanelBody>
        </Panel>
      )}
    </div>
  );
}
