import { useCallback, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { listSequences, type SequenceDefinition } from "../api";
import { usePolling } from "../hooks/usePolling";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Badge } from "../components/ui/Badge";
import { Input } from "../components/ui/Input";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Id } from "../components/ui/Mono";
import { Relative } from "../components/ui/Relative";
import { SkeletonTable } from "../components/ui/Skeleton";

// A group of versions sharing (tenant, namespace, name). Versions are sorted
// newest-first so the first row of each group is the "head" version.
interface SequenceGroup {
  tenant_id: string;
  namespace: string;
  name: string;
  versions: SequenceDefinition[];
}

const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "Sequence",
    definition:
      "A workflow definition — a named graph of steps (blocks) with retry, concurrency, and dependency rules. Sequences are the blueprint; executions are the instances.",
  },
  {
    term: "Version",
    definition:
      "Each deployment of a sequence produces a new numbered version. Old versions stay on disk so in-flight executions that started against them can keep running.",
  },
  {
    term: "Head",
    definition:
      "The newest version of a sequence. New executions and cron triggers use the head unless pinned to a specific version.",
  },
  {
    term: "active / deprecated",
    definition:
      "A deprecated version is still usable by in-flight executions, but new executions won't start against it. Use deprecation for safe rolling retirement.",
  },
  {
    term: "Block",
    definition:
      "One step inside a sequence. A sequence is a DAG of blocks — the block count shown below is how many steps each version has.",
  },
  {
    term: "Tenant / Namespace",
    definition:
      "Isolation. Two sequences with the same name in different tenants are unrelated. Namespace separates prod/staging/dev within a tenant.",
  },
];

export default function Sequences() {
  const navigate = useNavigate();
  const [tenant, setTenant] = useState("");
  const [namespace, setNamespace] = useState("");
  const [nameFilter, setNameFilter] = useState("");

  const fetcher = useCallback(
    () =>
      listSequences({
        tenant_id: tenant || undefined,
        namespace: namespace || undefined,
        limit: "500",
      }),
    [tenant, namespace],
  );
  const { data: sequences, loading, error, updatedAt, refresh } =
    usePolling<SequenceDefinition[]>(fetcher, 5000);

  const groups = useMemo<SequenceGroup[]>(() => {
    if (!sequences) return [];
    const m = new Map<string, SequenceGroup>();
    for (const s of sequences) {
      const key = `${s.tenant_id}\0${s.namespace}\0${s.name}`;
      const g = m.get(key);
      if (g) {
        g.versions.push(s);
      } else {
        m.set(key, {
          tenant_id: s.tenant_id,
          namespace: s.namespace,
          name: s.name,
          versions: [s],
        });
      }
    }
    const out = [...m.values()];
    for (const g of out) {
      g.versions.sort((a, b) => b.version - a.version);
    }
    // Stable, operator-friendly sort: tenant, namespace, name.
    out.sort((a, b) => {
      const t = a.tenant_id.localeCompare(b.tenant_id);
      if (t !== 0) return t;
      const n = a.namespace.localeCompare(b.namespace);
      if (n !== 0) return n;
      return a.name.localeCompare(b.name);
    });
    return out;
  }, [sequences]);

  const filtered = useMemo(() => {
    if (!nameFilter.trim()) return groups;
    const q = nameFilter.trim().toLowerCase();
    return groups.filter((g) => g.name.toLowerCase().includes(q));
  }, [groups, nameFilter]);

  const hasFilters = Boolean(tenant || namespace || nameFilter);

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Sequences"
        description="Every workflow definition deployed to the engine. One row per unique (tenant, namespace, name) — click through to see all its versions and the block graph of the head."
        actions={<PageMeta updatedAt={updatedAt} onRefresh={refresh} />}
      />

      <Glossary items={PAGE_GLOSSARY} />

      <Section
        eyebrow="Filter"
        title="Narrow by tenant, namespace, or name"
        description="Tenant and namespace filters hit the API; name is a client-side substring match on the results. Leave blank for everything."
      >
        <div className="grid grid-cols-1 md:grid-cols-[1fr_1fr_1fr] gap-3">
          <div>
            <label className="field-label block mb-1.5">Tenant ID</label>
            <Input
              type="text"
              placeholder="tenant-a"
              value={tenant}
              onChange={(e) => setTenant(e.target.value)}
            />
          </div>
          <div>
            <label className="field-label block mb-1.5">Namespace</label>
            <Input
              type="text"
              placeholder="prod, staging"
              value={namespace}
              onChange={(e) => setNamespace(e.target.value)}
            />
          </div>
          <div>
            <label className="field-label block mb-1.5">Name contains</label>
            <Input
              type="text"
              placeholder="e.g. onboarding"
              value={nameFilter}
              onChange={(e) => setNameFilter(e.target.value)}
            />
          </div>
        </div>
      </Section>

      {error && <div className="notice notice-warn">{error.message}</div>}

      {loading && !sequences && <SkeletonTable rows={6} cols={6} />}

      {sequences && (
        <Section
          eyebrow="Registry"
          title="Deployed sequences"
          description={
            <>
              Grouped by name. <strong className="text-ink">Versions</strong>{" "}
              shows active / total — if they differ, some versions are
              deprecated. Clicking a row opens the <em>head</em> version;
              other versions are reachable from the detail page.
            </>
          }
          meta={
            <>
              <span>
                <span className="text-faint">NAMES</span>{" "}
                <span className="text-ink-dim">{filtered.length}</span>
              </span>
              <span>
                <span className="text-faint">VERSIONS</span>{" "}
                <span className="text-ink-dim">
                  {sequences.length}
                </span>
              </span>
            </>
          }
        >
          <Table>
            <THead>
              <TH>Name</TH>
              <TH>Namespace</TH>
              <TH>Tenant</TH>
              <TH className="text-right">Versions</TH>
              <TH>Head</TH>
              <TH>Latest deploy</TH>
            </THead>
              <tbody>
                {filtered.map((g) => {
                  const head = g.versions[0]!;
                  const activeVersions = g.versions.filter(
                    (v) => !v.deprecated,
                  );
                  return (
                    <TR
                      key={`${g.tenant_id}/${g.namespace}/${g.name}`}
                      onClick={() => navigate(`/sequences/${head.id}`)}
                      className="cursor-pointer"
                    >
                      <TD className="align-top">
                        <div className="font-mono text-[13px] text-ink">
                          {g.name}
                        </div>
                        <div className="text-[11px] font-mono text-faint mt-0.5">
                          {head.blocks.length} block
                          {head.blocks.length === 1 ? "" : "s"}
                        </div>
                      </TD>
                      <TD className="font-mono text-[12px] align-top">
                        {g.namespace}
                      </TD>
                      <TD className="align-top">
                        <Id value={g.tenant_id} copy className="!text-muted" />
                      </TD>
                      <TD
                        className="text-right tabular align-top"
                        title="Active versions / total versions"
                      >
                        <span className="text-ink">
                          {activeVersions.length}
                        </span>
                        {g.versions.length !== activeVersions.length && (
                          <span className="text-faint">
                            {" "}
                            / {g.versions.length}
                          </span>
                        )}
                      </TD>
                      <TD className="align-top">
                        <div className="flex items-center gap-2">
                          <span className="font-mono text-[12px] tabular">
                            v{head.version}
                          </span>
                          {head.deprecated ? (
                            <Badge tone="hold">deprecated</Badge>
                          ) : (
                            <Badge tone="ok">active</Badge>
                          )}
                        </div>
                      </TD>
                      <TD className="align-top">
                        <Relative at={head.created_at} />
                      </TD>
                    </TR>
                  );
                })}
                {filtered.length === 0 && (
                  <Empty colSpan={99}>
                    {hasFilters
                      ? "No sequences match these filters — try clearing one."
                      : "No sequences deployed yet. Register one via POST /sequences or your SDK."}
                  </Empty>
                )}
              </tbody>
            </Table>
        </Section>
      )}
    </div>
  );
}
