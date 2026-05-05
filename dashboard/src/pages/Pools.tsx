import { useCallback, useState } from "react";
import { usePolling } from "../hooks/usePolling";
import { usePageTitle } from "../hooks/usePageTitle";
import {
  listPools,
  createPool,
  deletePool,
  type ResourcePool,
  type RotationStrategy,
} from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Button } from "../components/ui/Button";
import { Input, Select, FieldLabel } from "../components/ui/Input";
import { Badge } from "../components/ui/Badge";
import { Relative } from "../components/ui/Relative";
import { IconPlus, IconTrash } from "../components/ui/Icons";
import { SkeletonTable } from "../components/ui/Skeleton";

const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "Resource pool",
    definition:
      "A named collection of resources (API keys, proxies, endpoints) that sequences draw from at runtime. Pools abstract rotation and capacity limits away from sequence definitions.",
  },
  {
    term: "Rotation strategy",
    definition:
      "How the engine picks a resource from the pool: round_robin cycles in order, weighted respects resource weights, random is uniform random.",
  },
];

export default function Pools() {
  usePageTitle("Pools");
  const fetcher = useCallback((signal?: AbortSignal) => listPools(signal), []);
  const { data, loading, updatedAt, refresh } = usePolling<ResourcePool[]>(fetcher, 5000);
  const [showForm, setShowForm] = useState(false);
  const [toast, setToast] = useState<string | null>(null);

  const flash = (msg: string) => {
    setToast(msg);
    setTimeout(() => setToast(null), 2500);
  };

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Resource Pools"
        description="Named collections of resources with rotation strategies and daily caps. Sequences draw from pools instead of hard-coding credentials or endpoints."
        actions={
          <div className="flex items-center gap-2">
            <Button variant="primary" size="sm" onClick={() => setShowForm((v) => !v)}>
              <IconPlus size={13} /> {showForm ? "Close" : "New pool"}
            </Button>
            <PageMeta updatedAt={updatedAt} onRefresh={refresh} />
          </div>
        }
      />

      <Glossary items={PAGE_GLOSSARY} />

      {toast && <div className="notice notice-ok">{toast}</div>}

      {showForm && (
        <CreatePoolForm
          onCreated={() => {
            flash("Pool created");
            setShowForm(false);
            refresh();
          }}
          onError={(msg) => flash(msg)}
        />
      )}

      {loading && !data && <SkeletonTable rows={6} cols={5} />}

      {data && (
        <Section
          eyebrow="Registry"
          title="Resource pools"
          description="Every pool registered with the engine."
          meta={
            <span>
              <span className="text-faint">TOTAL</span>{" "}
              <span className="text-ink-dim">{data.length}</span>
            </span>
          }
        >
          <Table>
            <THead>
              <TH>Name</TH>
              <TH>Strategy</TH>
              <TH>Tenant</TH>
              <TH>Created</TH>
              <TH className="text-right">Actions</TH>
            </THead>
            <tbody>
              {data.map((p) => (
                <TR key={p.id}>
                  <TD className="font-mono text-[12px] text-ink">{p.name}</TD>
                  <TD>
                    <Badge tone="dim">{p.strategy}</Badge>
                  </TD>
                  <TD className="font-mono text-[12px] text-muted">
                    {p.tenant_id || "global"}
                  </TD>
                  <TD>
                    <Relative at={p.created_at} />
                  </TD>
                  <TD className="text-right">
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => {
                        if (!confirm(`Delete pool "${p.name}"?`)) return;
                        deletePool(p.id)
                          .then(() => {
                            flash("Deleted");
                            refresh();
                          })
                          .catch((e) => flash(String(e)));
                      }}
                      title="Delete pool"
                    >
                      <IconTrash size={13} />
                    </Button>
                  </TD>
                </TR>
              ))}
              {data.length === 0 && (
                <Empty colSpan={99}>
                  No pools yet. Create one to manage rotating resources.
                </Empty>
              )}
            </tbody>
          </Table>
        </Section>
      )}
    </div>
  );
}

function CreatePoolForm({
  onCreated,
  onError,
}: {
  onCreated: () => void;
  onError: (msg: string) => void;
}) {
  const [name, setName] = useState("");
  const [strategy, setStrategy] = useState<RotationStrategy>("round_robin");
  const [tenantId, setTenantId] = useState("tenant-a");
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    if (!name || !tenantId) {
      onError("name and tenant are required");
      return;
    }
    setBusy(true);
    try {
      await createPool({ tenant_id: tenantId, name, strategy });
      onCreated();
    } catch (e) {
      onError(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Section eyebrow="New pool" title="Create a resource pool">
      <div>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
          <div>
            <FieldLabel>Name</FieldLabel>
            <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="api-key-pool" className="w-full" />
          </div>
          <div>
            <FieldLabel>Strategy</FieldLabel>
            <Select value={strategy} onChange={(e) => setStrategy(e.target.value as RotationStrategy)} className="w-full">
              <option value="round_robin">round_robin</option>
              <option value="weighted">weighted</option>
              <option value="random">random</option>
            </Select>
          </div>
          <div>
            <FieldLabel>Tenant</FieldLabel>
            <Input value={tenantId} onChange={(e) => setTenantId(e.target.value)} className="w-full" />
          </div>
        </div>
        <div className="mt-6 flex justify-end">
          <Button variant="primary" size="sm" disabled={busy} onClick={submit}>
            Create pool
          </Button>
        </div>
      </div>
    </Section>
  );
}
