import { useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { usePolling } from "../hooks/usePolling";
import { useDebounce } from "../hooks/useDebounce";
import { listInstances, type TaskInstance, type InstanceState } from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Badge, INSTANCE_TONE } from "../components/ui/Badge";
import { Input, Select } from "../components/ui/Input";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Id } from "../components/ui/Mono";
import { Relative } from "../components/ui/Relative";
import { SkeletonTable } from "../components/ui/Skeleton";
import { StateBar, StateBarLegend, type StateSegment } from "../components/ui/StateBar";
import { useMemo } from "react";

const STATES: InstanceState[] = [
  "scheduled",
  "running",
  "waiting",
  "paused",
  "completed",
  "failed",
  "cancelled",
];

const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "Execution",
    definition:
      "A single run of a sequence. Has a lifecycle (scheduled → running → completed/failed) and a full audit trail of every step it performed.",
  },
  {
    term: "Sequence",
    definition:
      "The workflow blueprint — a graph of steps (blocks) with retry policies, concurrency limits, and dependencies. Executions are instances of sequences.",
  },
  {
    term: "Tenant / Namespace",
    definition:
      "Isolation boundaries. Tenant is the business-level isolation (never mixes with other tenants); namespace is the environment (prod, staging). Filters below are additive.",
  },
  {
    term: "scheduled",
    definition:
      "Created but not yet picked up. Waiting for its next_fire_at time or for a worker to claim it.",
  },
  {
    term: "running",
    definition:
      "Currently executing on a worker. Will stay here until it finishes, fails, or is paused.",
  },
  {
    term: "waiting",
    definition:
      "Paused by the sequence itself — waiting for a sleep, an external signal, or a dependency.",
  },
  {
    term: "paused",
    definition:
      "Manually paused by an operator. Will not progress until resumed. Different from waiting, which is automatic.",
  },
  {
    term: "completed",
    definition:
      "Terminal success. Every step finished without unrecoverable error.",
  },
  {
    term: "failed",
    definition:
      "Terminal failure. Exhausted its retry budget and moved to the DLQ. Visible in Operations → DLQ for retry.",
  },
  {
    term: "cancelled",
    definition:
      "Terminal — stopped by explicit operator action. No retry, no DLQ. Will not resume.",
  },
  {
    term: "Next fire",
    definition:
      "When the engine plans to wake this execution next. For running executions this is empty; for scheduled/waiting it shows the planned resume time.",
  },
];

export default function Instances() {
  const navigate = useNavigate();
  const [stateFilter, setStateFilter] = useState("");
  const [namespaceFilter, setNamespaceFilter] = useState("");
  const [tenantFilter, setTenantFilter] = useState("");

  const debouncedNamespace = useDebounce(namespaceFilter, 300);
  const debouncedTenant = useDebounce(tenantFilter, 300);

  const fetcher = useCallback(
    () =>
      listInstances({
        state: stateFilter || undefined,
        namespace: debouncedNamespace || undefined,
        tenant_id: debouncedTenant || undefined,
        limit: "100",
      }),
    [stateFilter, debouncedNamespace, debouncedTenant],
  );
  const { data: instances, loading, error, updatedAt, refresh } =
    usePolling<TaskInstance[]>(fetcher);

  const hasFilters = Boolean(stateFilter || namespaceFilter || tenantFilter);

  const stateSummary = useMemo<StateSegment[] | null>(() => {
    if (!instances) return null;
    const counts: Record<string, number> = {};
    for (const i of instances) counts[i.state] = (counts[i.state] ?? 0) + 1;
    return [
      { label: "scheduled", value: counts.scheduled ?? 0, tone: "dim" },
      { label: "running", value: counts.running ?? 0, tone: "live" },
      { label: "waiting", value: counts.waiting ?? 0, tone: "hold" },
      { label: "paused", value: counts.paused ?? 0, tone: "hold" },
      { label: "completed", value: counts.completed ?? 0, tone: "ok" },
      { label: "failed", value: counts.failed ?? 0, tone: "warn" },
      { label: "cancelled", value: counts.cancelled ?? 0, tone: "dim" },
    ];
  }, [instances]);

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Executions"
        description="Every sequence run across every tenant and namespace. Each row is one execution; click to open its step-by-step timeline."
        actions={<PageMeta updatedAt={updatedAt} onRefresh={refresh} />}
      />

      <Glossary items={PAGE_GLOSSARY} />

      {stateSummary && instances && instances.length > 0 && (
        <Section
          eyebrow="State distribution"
          title="Snapshot of currently visible executions"
          description={
            <>
              Proportional bar across the {instances.length} executions
              matching your filters. Width = share, colour = state. Use this
              to spot skew — e.g. a dominant red bar means many executions
              are in the DLQ and operations attention is due.
            </>
          }
          meta={
            <span>
              <span className="text-faint">VISIBLE</span>{" "}
              <span className="text-ink-dim">{instances.length}</span>
            </span>
          }
        >
          <div className="space-y-3">
            <StateBar segments={stateSummary} height={10} />
            <StateBarLegend segments={stateSummary} />
          </div>
        </Section>
      )}

      <Section
        eyebrow="Filter"
        title="Narrow the list"
        description={
          <>
            Filters are additive — combine them to zero in on a tenant,
            namespace, or specific state. Blank fields are wildcards. The
            table below refreshes automatically as you type.
          </>
        }
      >
        <div className="grid grid-cols-1 md:grid-cols-[200px_1fr_1fr] gap-3">
          <div>
            <label className="field-label block mb-1.5">State</label>
            <Select
              value={stateFilter}
              onChange={(e) => setStateFilter(e.target.value)}
              className="w-full"
            >
              <option value="">All states</option>
              {STATES.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </Select>
          </div>
          <div>
            <label className="field-label block mb-1.5">Namespace</label>
            <Input
              type="text"
              placeholder="e.g. prod, staging"
              value={namespaceFilter}
              onChange={(e) => setNamespaceFilter(e.target.value)}
            />
          </div>
          <div>
            <label className="field-label block mb-1.5">Tenant ID</label>
            <Input
              type="text"
              placeholder="tenant-a"
              value={tenantFilter}
              onChange={(e) => setTenantFilter(e.target.value)}
            />
          </div>
        </div>
      </Section>

      {error && <div className="notice notice-warn">{error.message}</div>}
      {loading && !instances && <SkeletonTable rows={8} cols={6} />}

      {instances && (
        <Section
          eyebrow="Executions"
          title="Results"
          description={
            <>
              Each row is one execution. Click anywhere to open its timeline
              — you'll see every block it ran, every attempt, every input
              and output payload. <strong className="text-ink">ID</strong>{" "}
              and <strong className="text-ink">Tenant</strong> are
              click-to-copy.
            </>
          }
          meta={
            <span>
              <span className="text-faint">ROWS</span>{" "}
              <span className="text-ink-dim">{instances.length}</span>
              <span className="text-faint"> / LIMIT 100</span>
            </span>
          }
        >
          <Table>
            <THead>
              <TH>State</TH>
              <TH>Execution ID</TH>
              <TH>Namespace</TH>
              <TH>Tenant</TH>
              <TH>Next fire</TH>
              <TH>Updated</TH>
            </THead>
              <tbody>
                {instances.map((i) => (
                  <TR
                    key={i.id}
                    onClick={() => navigate(`/instances/${i.id}`)}
                    className="cursor-pointer"
                  >
                    <TD>
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
                    <TD title="When this execution will next wake">
                      <Relative at={i.next_fire_at ?? null} />
                    </TD>
                    <TD title="Time of last state change">
                      <Relative at={i.updated_at} />
                    </TD>
                  </TR>
                ))}
                {instances.length === 0 && (
                  <Empty colSpan={99}>
                    {hasFilters
                      ? "No executions match these filters — try clearing one."
                      : "No executions yet. They appear here once a sequence is scheduled."}
                  </Empty>
                )}
              </tbody>
            </Table>
        </Section>
      )}
    </div>
  );
}
