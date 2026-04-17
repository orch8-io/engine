import { useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { usePolling } from "../hooks/usePolling";
import { listInstances, type TaskInstance, type InstanceState } from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Panel, PanelBody } from "../components/ui/Panel";
import { Badge, INSTANCE_TONE } from "../components/ui/Badge";
import { Input, Select } from "../components/ui/Input";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Id } from "../components/ui/Mono";
import { Relative } from "../components/ui/Relative";
import { SkeletonTable } from "../components/ui/Skeleton";

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
  const navigate = useNavigate();
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
  const { data: instances, loading, error, updatedAt, refresh } =
    usePolling<TaskInstance[]>(fetcher);

  const hasFilters = Boolean(stateFilter || namespaceFilter || tenantFilter);

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Operator"
        title="Instances"
        description="Scheduled and running sequence instances across tenants."
        actions={<PageMeta updatedAt={updatedAt} onRefresh={refresh} />}
      />

      {/* Flat filter strip — no wrapping panel. Density over chrome. */}
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

      {error && (
        <div className="rounded-md border border-warn/40 bg-warn/10 text-warn p-3 text-[13px]">
          {error.message}
        </div>
      )}
      {loading && !instances && <SkeletonTable rows={8} cols={6} />}

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
                  <TR
                    key={i.id}
                    onClick={() => navigate(`/instances/${i.id}`)}
                    className="cursor-pointer"
                  >
                    <TD className="pl-4">
                      <Badge
                        tone={INSTANCE_TONE[i.state] ?? "dim"}
                        dot
                        live={i.state === "running"}
                      >
                        {i.state}
                      </Badge>
                    </TD>
                    <TD>
                      <Id value={i.id} copy />
                    </TD>
                    <TD className="font-mono text-[12px]">{i.namespace}</TD>
                    <TD>
                      <Id value={i.tenant_id} copy className="!text-muted" />
                    </TD>
                    <TD>
                      <Relative at={i.next_fire_at ?? null} />
                    </TD>
                    <TD className="pr-4">
                      <Relative at={i.updated_at} />
                    </TD>
                  </TR>
                ))}
                {instances.length === 0 && (
                  <Empty>
                    {hasFilters
                      ? "No instances match these filters — try clearing one."
                      : "No instances yet. They appear here once a sequence is scheduled."}
                  </Empty>
                )}
              </tbody>
            </Table>
          </PanelBody>
        </Panel>
      )}
    </div>
  );
}
